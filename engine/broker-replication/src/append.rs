//! Persistent-client, bounded binary replication of append-only log epochs.

use base64::{engine::general_purpose::STANDARD as B64, Engine};
use broker_raft_meta::ClusterConfig;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::Semaphore;
use tracing::{debug, warn};
use uuid::Uuid;

type SharedReplicaProgress = Arc<Mutex<HashMap<(String, u32, Uuid), ReplicaProgress>>>;

const EPOCH_MAGIC: [u8; 4] = *b"BMQE";
const EPOCH_VERSION: u16 = 1;
const FIXED_HEADER_LEN: usize = 64;
const DEFAULT_MAX_EPOCH_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_IN_FLIGHT_BYTES: usize = 64 * 1024 * 1024;
const MAX_DECODE_EPOCH_BYTES: usize = 64 * 1024 * 1024;
const MAX_EPOCH_RECORDS: usize = 65_536;
const CATCHUP_MAGIC: [u8; 4] = *b"BMQC";
const CATCHUP_VERSION: u16 = 1;
const CATCHUP_HEADER_LEN: usize = 32;
const MAX_CATCHUP_RECORDS: usize = 4096;

#[derive(Debug, Error)]
pub enum ReplicateError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("quorum not reached: {acked}/{quorum}")]
    QuorumNotReached { acked: usize, quorum: usize },
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("invalid replication epoch: {0}")]
    InvalidEpoch(String),
    #[error("replication epoch is too large: {bytes} bytes (limit {limit})")]
    EpochTooLarge { bytes: usize, limit: usize },
    #[error("peer {peer} rejected replication: {status} {detail}")]
    PeerRejected {
        peer: String,
        status: reqwest::StatusCode,
        detail: String,
    },
    #[error("local WAL is not durable through offset {required}: durable hwm is {actual}")]
    LocalNotDurable { required: u64, actual: u64 },
    #[error("replication in-flight byte semaphore closed")]
    Closed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateAppendRequest {
    pub tenant_id: String,
    pub topic: String,
    pub partition: u32,
    /// Leader-assigned log offset for this frame (quorum-atomic agreement).
    #[serde(default)]
    pub offset: u64,
    /// Base64-encoded partition log frame (magic + header + payload + crc).
    pub frame_b64: String,
    pub leader_generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicateBatchRequest {
    pub tenant_id: String,
    /// Logical topic for a V1/single-topic epoch. Empty for a V2 physical-shard
    /// epoch whose frames carry multiple logical topics.
    pub topic: String,
    pub partition: u32,
    pub leader_id: Uuid,
    pub leader_epoch: u64,
    pub first_offset: u64,
    pub record_count: u32,
    /// Raw broker-proto frames. The primary transport encodes these without base64.
    pub frames: Vec<Vec<u8>>,
    pub batch_crc: u32,
}

impl ReplicateBatchRequest {
    pub fn new(
        tenant_id: String,
        topic: String,
        partition: u32,
        leader_id: Uuid,
        leader_epoch: u64,
        first_offset: u64,
        frames: Vec<Vec<u8>>,
    ) -> Result<Self, ReplicateError> {
        let record_count = u32::try_from(frames.len())
            .map_err(|_| ReplicateError::InvalidEpoch("too many frames".into()))?;
        if record_count == 0 {
            return Err(ReplicateError::InvalidEpoch(
                "an epoch must contain at least one frame".into(),
            ));
        }
        let batch_crc = crc_for_frames(&frames);
        let request = Self {
            tenant_id,
            topic,
            partition,
            leader_id,
            leader_epoch,
            first_offset,
            record_count,
            frames,
            batch_crc,
        };
        request.validate()?;
        Ok(request)
    }

    pub fn last_offset(&self) -> Result<u64, ReplicateError> {
        self.first_offset
            .checked_add(u64::from(self.record_count) - 1)
            .ok_or_else(|| ReplicateError::InvalidEpoch("offset range overflow".into()))
    }

    pub fn end_offset(&self) -> Result<u64, ReplicateError> {
        self.last_offset()?
            .checked_add(1)
            .ok_or_else(|| ReplicateError::InvalidEpoch("offset range overflow".into()))
    }

    pub fn validate(&self) -> Result<(), ReplicateError> {
        if self.leader_epoch == u64::MAX {
            return Err(ReplicateError::InvalidEpoch(
                "leader epoch u64::MAX is reserved".into(),
            ));
        }
        if self.record_count == 0
            || self.record_count as usize != self.frames.len()
            || self.frames.len() > MAX_EPOCH_RECORDS
        {
            return Err(ReplicateError::InvalidEpoch(format!(
                "invalid record count {} for {} frames (limit {})",
                self.record_count,
                self.frames.len(),
                MAX_EPOCH_RECORDS
            )));
        }
        self.end_offset()?;
        let actual_crc = crc_for_frames(&self.frames);
        if actual_crc != self.batch_crc {
            return Err(ReplicateError::InvalidEpoch(format!(
                "batch crc mismatch: declared {}, computed {}",
                self.batch_crc, actual_crc
            )));
        }
        Ok(())
    }

    /// Compact epoch body:
    /// fixed header | tenant | topic | repeated(frame_len | frame).
    pub fn binary_len(&self) -> Result<usize, ReplicateError> {
        self.validate()?;
        let tenant = self.tenant_id.as_bytes();
        let topic = self.topic.as_bytes();
        u16::try_from(tenant.len())
            .map_err(|_| ReplicateError::InvalidEpoch("tenant id is too long".into()))?;
        u16::try_from(topic.len())
            .map_err(|_| ReplicateError::InvalidEpoch("topic is too long".into()))?;
        let payload_len = self.frames.iter().try_fold(0usize, |total, frame| {
            total
                .checked_add(4)
                .and_then(|n| n.checked_add(frame.len()))
                .ok_or_else(|| ReplicateError::InvalidEpoch("epoch length overflow".into()))
        })?;
        FIXED_HEADER_LEN
            .checked_add(tenant.len())
            .and_then(|n| n.checked_add(topic.len()))
            .and_then(|n| n.checked_add(payload_len))
            .ok_or_else(|| ReplicateError::InvalidEpoch("epoch length overflow".into()))
    }

    pub fn encode_binary(&self) -> Result<Vec<u8>, ReplicateError> {
        let total_len = self.binary_len()?;
        let tenant = self.tenant_id.as_bytes();
        let topic = self.topic.as_bytes();
        let tenant_len = tenant.len() as u16;
        let topic_len = topic.len() as u16;
        let payload_len = total_len - FIXED_HEADER_LEN - tenant.len() - topic.len();
        let mut out = Vec::with_capacity(total_len);
        out.extend_from_slice(&EPOCH_MAGIC);
        out.extend_from_slice(&EPOCH_VERSION.to_be_bytes());
        out.extend_from_slice(&0u16.to_be_bytes());
        out.extend_from_slice(self.leader_id.as_bytes());
        out.extend_from_slice(&self.partition.to_be_bytes());
        out.extend_from_slice(&self.leader_epoch.to_be_bytes());
        out.extend_from_slice(&self.first_offset.to_be_bytes());
        out.extend_from_slice(&self.record_count.to_be_bytes());
        out.extend_from_slice(&tenant_len.to_be_bytes());
        out.extend_from_slice(&topic_len.to_be_bytes());
        out.extend_from_slice(&(payload_len as u64).to_be_bytes());
        out.extend_from_slice(&self.batch_crc.to_be_bytes());
        out.extend_from_slice(tenant);
        out.extend_from_slice(topic);
        for frame in &self.frames {
            let len = u32::try_from(frame.len())
                .map_err(|_| ReplicateError::InvalidEpoch("frame is too large".into()))?;
            out.extend_from_slice(&len.to_be_bytes());
            out.extend_from_slice(frame);
        }
        debug_assert_eq!(out.len(), total_len);
        Ok(out)
    }

    pub fn decode_binary(body: &[u8]) -> Result<Self, ReplicateError> {
        if body.len() > MAX_DECODE_EPOCH_BYTES {
            return Err(ReplicateError::EpochTooLarge {
                bytes: body.len(),
                limit: MAX_DECODE_EPOCH_BYTES,
            });
        }
        if body.len() < FIXED_HEADER_LEN {
            return Err(ReplicateError::InvalidEpoch(
                "truncated epoch header".into(),
            ));
        }
        if body[..4] != EPOCH_MAGIC {
            return Err(ReplicateError::InvalidEpoch("invalid epoch magic".into()));
        }
        let version = read_u16(body, 4)?;
        if version != EPOCH_VERSION {
            return Err(ReplicateError::InvalidEpoch(format!(
                "unsupported epoch version {version}"
            )));
        }
        let leader_id = Uuid::from_slice(&body[8..24])
            .map_err(|e| ReplicateError::InvalidEpoch(format!("invalid leader id: {e}")))?;
        let partition = read_u32(body, 24)?;
        let leader_epoch = read_u64(body, 28)?;
        let first_offset = read_u64(body, 36)?;
        let record_count = read_u32(body, 44)?;
        if record_count == 0 || record_count as usize > MAX_EPOCH_RECORDS {
            return Err(ReplicateError::InvalidEpoch(format!(
                "record count {record_count} is outside 1..={MAX_EPOCH_RECORDS}"
            )));
        }
        let tenant_len = read_u16(body, 48)? as usize;
        let topic_len = read_u16(body, 50)? as usize;
        let payload_len = usize::try_from(read_u64(body, 52)?)
            .map_err(|_| ReplicateError::InvalidEpoch("payload length overflow".into()))?;
        let batch_crc = read_u32(body, 60)?;
        let strings_end = FIXED_HEADER_LEN
            .checked_add(tenant_len)
            .and_then(|n| n.checked_add(topic_len))
            .ok_or_else(|| ReplicateError::InvalidEpoch("epoch length overflow".into()))?;
        let expected_len = strings_end
            .checked_add(payload_len)
            .ok_or_else(|| ReplicateError::InvalidEpoch("epoch length overflow".into()))?;
        if expected_len != body.len() {
            return Err(ReplicateError::InvalidEpoch(format!(
                "declared epoch length {expected_len} does not match body length {}",
                body.len()
            )));
        }
        let tenant_id = std::str::from_utf8(&body[FIXED_HEADER_LEN..FIXED_HEADER_LEN + tenant_len])
            .map_err(|e| ReplicateError::InvalidEpoch(format!("tenant id is not utf-8: {e}")))?
            .to_owned();
        let topic_start = FIXED_HEADER_LEN + tenant_len;
        let topic = std::str::from_utf8(&body[topic_start..strings_end])
            .map_err(|e| ReplicateError::InvalidEpoch(format!("topic is not utf-8: {e}")))?
            .to_owned();
        let mut cursor = strings_end;
        let mut frames = Vec::with_capacity(record_count as usize);
        for _ in 0..record_count {
            let frame_len = read_u32(body, cursor)? as usize;
            cursor = cursor
                .checked_add(4)
                .ok_or_else(|| ReplicateError::InvalidEpoch("frame offset overflow".into()))?;
            let end = cursor
                .checked_add(frame_len)
                .ok_or_else(|| ReplicateError::InvalidEpoch("frame length overflow".into()))?;
            if end > body.len() {
                return Err(ReplicateError::InvalidEpoch("truncated frame".into()));
            }
            frames.push(body[cursor..end].to_vec());
            cursor = end;
        }
        if cursor != body.len() {
            return Err(ReplicateError::InvalidEpoch(
                "trailing bytes after declared frames".into(),
            ));
        }
        let request = Self {
            tenant_id,
            topic,
            partition,
            leader_id,
            leader_epoch,
            first_offset,
            record_count,
            frames,
            batch_crc,
        };
        request.validate()?;
        Ok(request)
    }
}

fn read_u16(body: &[u8], at: usize) -> Result<u16, ReplicateError> {
    let bytes = body
        .get(at..at + 2)
        .ok_or_else(|| ReplicateError::InvalidEpoch("truncated integer".into()))?;
    Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn read_u32(body: &[u8], at: usize) -> Result<u32, ReplicateError> {
    let bytes = body
        .get(at..at + 4)
        .ok_or_else(|| ReplicateError::InvalidEpoch("truncated integer".into()))?;
    Ok(u32::from_be_bytes(
        bytes.try_into().expect("four byte slice"),
    ))
}

fn read_u64(body: &[u8], at: usize) -> Result<u64, ReplicateError> {
    let bytes = body
        .get(at..at + 8)
        .ok_or_else(|| ReplicateError::InvalidEpoch("truncated integer".into()))?;
    Ok(u64::from_be_bytes(
        bytes.try_into().expect("eight byte slice"),
    ))
}

fn crc_for_frames(frames: &[Vec<u8>]) -> u32 {
    let mut crc = crc32fast::Hasher::new();
    for frame in frames {
        crc.update(frame);
    }
    crc.finalize()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicateBatchAck {
    pub node_id: Uuid,
    pub partition: u32,
    pub leader_epoch: u64,
    /// Exclusive durable high watermark.
    pub durable_hwm: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicateOutcome {
    pub durable_acks: usize,
    pub quorum: usize,
    pub committed_hwm: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReplicaProgress {
    pub peer_id: Uuid,
    pub topic: String,
    pub partition: u32,
    pub durable_hwm: u64,
    pub leader_hwm: u64,
    pub lag: u64,
    pub in_sync: bool,
    pub last_success_ms: Option<u64>,
    pub consecutive_failures: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateCatchUpRequest {
    pub tenant_id: String,
    pub topic: String,
    pub partition: u32,
    pub from_offset: u64,
    #[serde(default = "default_catchup_max")]
    pub max_records: usize,
}

fn default_catchup_max() -> usize {
    512
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateCatchUpResponse {
    pub frames: Vec<ReplicateAppendRequest>,
    pub committed_hwm: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatchUpFrame {
    pub offset: u64,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicateCatchUpRange {
    pub partition: u32,
    pub leader_generation: u64,
    pub committed_hwm: u64,
    pub frames: Vec<CatchUpFrame>,
}

impl ReplicateCatchUpRange {
    pub fn encode_binary(&self) -> Result<Vec<u8>, ReplicateError> {
        if self.frames.len() > MAX_CATCHUP_RECORDS {
            return Err(ReplicateError::InvalidEpoch(
                "catch-up range has too many records".into(),
            ));
        }
        let total = self
            .frames
            .iter()
            .try_fold(CATCHUP_HEADER_LEN, |size, frame| {
                size.checked_add(12)
                    .and_then(|value| value.checked_add(frame.bytes.len()))
                    .ok_or_else(|| ReplicateError::InvalidEpoch("catch-up length overflow".into()))
            })?;
        if total > MAX_DECODE_EPOCH_BYTES {
            return Err(ReplicateError::EpochTooLarge {
                bytes: total,
                limit: MAX_DECODE_EPOCH_BYTES,
            });
        }
        let mut output = Vec::with_capacity(total);
        output.extend_from_slice(&CATCHUP_MAGIC);
        output.extend_from_slice(&CATCHUP_VERSION.to_be_bytes());
        output.extend_from_slice(&0u16.to_be_bytes());
        output.extend_from_slice(&self.partition.to_be_bytes());
        output.extend_from_slice(&self.leader_generation.to_be_bytes());
        output.extend_from_slice(&self.committed_hwm.to_be_bytes());
        output.extend_from_slice(&(self.frames.len() as u32).to_be_bytes());
        for frame in &self.frames {
            let len = u32::try_from(frame.bytes.len())
                .map_err(|_| ReplicateError::InvalidEpoch("catch-up frame too large".into()))?;
            output.extend_from_slice(&frame.offset.to_be_bytes());
            output.extend_from_slice(&len.to_be_bytes());
            output.extend_from_slice(&frame.bytes);
        }
        Ok(output)
    }

    pub fn decode_binary(body: &[u8]) -> Result<Self, ReplicateError> {
        if body.len() > MAX_DECODE_EPOCH_BYTES {
            return Err(ReplicateError::EpochTooLarge {
                bytes: body.len(),
                limit: MAX_DECODE_EPOCH_BYTES,
            });
        }
        if body.len() < CATCHUP_HEADER_LEN || body[..4] != CATCHUP_MAGIC {
            return Err(ReplicateError::InvalidEpoch(
                "invalid catch-up range header".into(),
            ));
        }
        if read_u16(body, 4)? != CATCHUP_VERSION {
            return Err(ReplicateError::InvalidEpoch(
                "unsupported catch-up range version".into(),
            ));
        }
        let partition = read_u32(body, 8)?;
        let leader_generation = read_u64(body, 12)?;
        let committed_hwm = read_u64(body, 20)?;
        let count = read_u32(body, 28)? as usize;
        if count > MAX_CATCHUP_RECORDS {
            return Err(ReplicateError::InvalidEpoch(
                "catch-up record count exceeds limit".into(),
            ));
        }
        let mut cursor = CATCHUP_HEADER_LEN;
        let mut frames = Vec::with_capacity(count);
        for _ in 0..count {
            let offset = read_u64(body, cursor)?;
            let len = read_u32(body, cursor + 8)? as usize;
            cursor = cursor
                .checked_add(12)
                .ok_or_else(|| ReplicateError::InvalidEpoch("catch-up offset overflow".into()))?;
            let end = cursor
                .checked_add(len)
                .filter(|end| *end <= body.len())
                .ok_or_else(|| ReplicateError::InvalidEpoch("truncated catch-up frame".into()))?;
            frames.push(CatchUpFrame {
                offset,
                bytes: body[cursor..end].to_vec(),
            });
            cursor = end;
        }
        if cursor != body.len() {
            return Err(ReplicateError::InvalidEpoch(
                "trailing catch-up range bytes".into(),
            ));
        }
        Ok(Self {
            partition,
            leader_generation,
            committed_hwm,
            frames,
        })
    }

    fn from_legacy(response: ReplicateCatchUpResponse) -> Result<Self, ReplicateError> {
        let partition = response
            .frames
            .first()
            .map(|frame| frame.partition)
            .unwrap_or(0);
        let leader_generation = response
            .frames
            .first()
            .map(|frame| frame.leader_generation)
            .unwrap_or(0);
        let frames = response
            .frames
            .into_iter()
            .map(|frame| {
                B64.decode(frame.frame_b64)
                    .map(|bytes| CatchUpFrame {
                        offset: frame.offset,
                        bytes,
                    })
                    .map_err(|error| {
                        ReplicateError::InvalidEpoch(format!(
                            "invalid legacy catch-up base64: {error}"
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            partition,
            leader_generation,
            committed_hwm: response.committed_hwm,
            frames,
        })
    }
}

#[derive(Clone)]
pub struct ReplicationClient {
    http: reqwest::Client,
    cluster: ClusterConfig,
    in_flight: Arc<Semaphore>,
    max_epoch_bytes: usize,
    progress: SharedReplicaProgress,
    telemetry: Arc<crate::telemetry::ReplicationTelemetry>,
    runtime: Option<broker_raft_meta::ClusterRuntime>,
}

impl ReplicationClient {
    pub fn new(cluster: ClusterConfig) -> Self {
        let max_epoch_bytes = env_bytes(
            "BETTERMQ_MAX_REPLICATION_EPOCH_BYTES",
            DEFAULT_MAX_EPOCH_BYTES,
        )
        .min(Semaphore::MAX_PERMITS);
        let max_in_flight = env_bytes(
            "BETTERMQ_MAX_REPLICATION_IN_FLIGHT_BYTES",
            DEFAULT_MAX_IN_FLIGHT_BYTES,
        )
        .max(max_epoch_bytes)
        .min(Semaphore::MAX_PERMITS);
        Self {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .expect("reqwest client"),
            cluster,
            in_flight: Arc::new(Semaphore::new(max_in_flight)),
            max_epoch_bytes,
            progress: Arc::new(Mutex::new(HashMap::new())),
            telemetry: Arc::new(crate::telemetry::ReplicationTelemetry::default()),
            runtime: None,
        }
    }

    pub fn with_runtime(mut self, runtime: broker_raft_meta::ClusterRuntime) -> Self {
        self.runtime = Some(runtime);
        self
    }

    fn targets_for_shard(&self, shard: u32) -> (Vec<broker_raft_meta::NodeConfig>, usize) {
        if let Some(runtime) = &self.runtime {
            return runtime.replication_targets(shard);
        }
        (self.cluster.peer_nodes(), self.cluster.quorum_size())
    }

    pub fn with_config(cluster: ClusterConfig) -> Self {
        Self::new(cluster)
    }

    /// Leader: parallel fan-out frame to peers; require quorum acks (including self).
    pub async fn replicate_append(
        &self,
        tenant_id: &str,
        topic: &str,
        partition: u32,
        offset: u64,
        frame: &[u8],
        leader_generation: u64,
    ) -> Result<(), ReplicateError> {
        if self.cluster.node_count() <= 1 {
            return Ok(());
        }

        let req = ReplicateBatchRequest::new(
            tenant_id.to_string(),
            topic.to_string(),
            partition,
            self.cluster.node_id,
            leader_generation,
            offset,
            vec![frame.to_vec()],
        )?;
        self.replicate_batch(req, offset.saturating_add(1))
            .await
            .map(|_| ())
    }

    fn apply_cluster_secret(req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Ok(secret) = std::env::var("BETTERMQ_CLUSTER_SECRET") {
            if !secret.trim().is_empty() {
                return req.header("x-bettermq-cluster-secret", secret);
            }
        }
        req
    }

    /// Replicate a contiguous epoch batch. `local_durable_hwm` is an exclusive
    /// WAL high watermark obtained only after local fsync. Returns after a
    /// durable majority; remaining peer RPCs continue in the background.
    pub async fn replicate_batch(
        &self,
        req: ReplicateBatchRequest,
        local_durable_hwm: u64,
    ) -> Result<ReplicateOutcome, ReplicateError> {
        self.replicate_batch_with_local(req, async move { Ok(local_durable_hwm) })
            .await
    }

    /// Fan out replica RPCs while `local_durable` is still in flight. `202`
    /// still requires local fsync plus minISR (self counts after local
    /// durability). minISR stays 2 on RF=3 — do not wait for three copies.
    pub async fn replicate_batch_with_local<F>(
        &self,
        req: ReplicateBatchRequest,
        local_durable: F,
    ) -> Result<ReplicateOutcome, ReplicateError>
    where
        F: std::future::Future<Output = Result<u64, ReplicateError>>,
    {
        req.validate()?;
        let required_hwm = req.end_offset()?;
        let quorum_started = Instant::now();
        if self.cluster.node_count() <= 1 {
            let local_durable_hwm = local_durable.await?;
            if local_durable_hwm < required_hwm {
                return Err(ReplicateError::LocalNotDurable {
                    required: required_hwm,
                    actual: local_durable_hwm,
                });
            }
            self.telemetry
                .record_quorum(req.partition, quorum_started.elapsed(), 1, 1, true);
            return Ok(ReplicateOutcome {
                durable_acks: 1,
                quorum: 1,
                committed_hwm: required_hwm,
            });
        }
        let encoded_len = req.binary_len()?;
        if encoded_len > self.max_epoch_bytes {
            return Err(ReplicateError::EpochTooLarge {
                bytes: encoded_len,
                limit: self.max_epoch_bytes,
            });
        }
        let permits = u32::try_from(encoded_len).map_err(|_| ReplicateError::EpochTooLarge {
            bytes: encoded_len,
            limit: u32::MAX as usize,
        })?;
        let epoch_permit = Arc::new(
            Arc::clone(&self.in_flight)
                .acquire_many_owned(permits)
                .await
                .map_err(|_| ReplicateError::Closed)?,
        );
        let encoded = bytes::Bytes::from(req.encode_binary()?);
        let (peers, quorum) = self.targets_for_shard(req.partition);
        let (tx, mut rx) = tokio::sync::mpsc::channel::<bool>(peers.len().max(1));
        for peer in peers {
            let url = format!(
                "{}/internal/v1/replicate/batch",
                peer.addr.trim_end_matches('/')
            );
            let http = self.http.clone();
            let body = encoded.clone();
            let tx = tx.clone();
            let epoch_permit = Arc::clone(&epoch_permit);
            let progress = Arc::clone(&self.progress);
            let topic = req.topic.clone();
            let partition = req.partition;
            let leader_epoch = req.leader_epoch;
            let peer_id = peer.id;
            tokio::spawn(async move {
                let builder = Self::apply_cluster_secret(
                    http.post(&url)
                        .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
                        .body(body),
                );
                let ok = match builder.send().await {
                    Ok(resp) if resp.status().is_success() => {
                        match resp.json::<ReplicateBatchAck>().await {
                            Ok(response)
                                if response.node_id == peer_id
                                    && response.partition == partition
                                    && response.leader_epoch == leader_epoch
                                    && response.durable_hwm >= required_hwm =>
                            {
                                update_progress(
                                    &progress,
                                    &topic,
                                    partition,
                                    peer_id,
                                    required_hwm,
                                    response.durable_hwm,
                                    true,
                                );
                                true
                            }
                            Ok(response) => {
                                warn!(
                                    expected_peer = %peer_id,
                                    actual_peer = %response.node_id,
                                    expected_hwm = required_hwm,
                                    actual_hwm = response.durable_hwm,
                                    "replicate peer returned non-durable or mismatched ack"
                                );
                                update_progress(
                                    &progress,
                                    &topic,
                                    partition,
                                    peer_id,
                                    required_hwm,
                                    response.durable_hwm,
                                    false,
                                );
                                false
                            }
                            Err(e) => {
                                warn!(error = %e, url = %url, "replicate peer returned invalid ack");
                                update_progress(
                                    &progress,
                                    &topic,
                                    partition,
                                    peer_id,
                                    required_hwm,
                                    0,
                                    false,
                                );
                                false
                            }
                        }
                    }
                    Ok(resp) => {
                        warn!(status = %resp.status(), url = %url, "replicate batch peer rejected");
                        update_progress(
                            &progress,
                            &topic,
                            partition,
                            peer_id,
                            required_hwm,
                            0,
                            false,
                        );
                        false
                    }
                    Err(e) => {
                        warn!(error = %e, url = %url, "replicate batch peer failed");
                        update_progress(
                            &progress,
                            &topic,
                            partition,
                            peer_id,
                            required_hwm,
                            0,
                            false,
                        );
                        false
                    }
                };
                drop(epoch_permit);
                let _ = tx.send(ok).await;
            });
        }
        drop(tx);
        let local_durable_hwm = local_durable.await?;
        if local_durable_hwm < required_hwm {
            return Err(ReplicateError::LocalNotDurable {
                required: required_hwm,
                actual: local_durable_hwm,
            });
        }
        let mut acked = 1usize;
        while acked < quorum {
            match rx.recv().await {
                Some(true) => acked += 1,
                Some(false) => {}
                None => break,
            }
        }
        debug!(
            acked,
            quorum,
            topic = %req.topic,
            partition = req.partition,
            first_offset = req.first_offset,
            record_count = req.record_count,
            "replicate durable quorum"
        );
        if acked >= quorum {
            self.telemetry.record_quorum(
                req.partition,
                quorum_started.elapsed(),
                acked,
                quorum,
                true,
            );
            Ok(ReplicateOutcome {
                durable_acks: acked,
                quorum,
                committed_hwm: required_hwm,
            })
        } else {
            self.telemetry.record_quorum(
                req.partition,
                quorum_started.elapsed(),
                acked,
                quorum,
                false,
            );
            Err(ReplicateError::QuorumNotReached { acked, quorum })
        }
    }

    pub fn replica_progress(&self) -> Vec<ReplicaProgress> {
        let mut progress: Vec<_> = self
            .progress
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .cloned()
            .collect();
        progress.sort_by_key(|p| (p.partition, p.topic.clone(), p.peer_id));
        progress
    }

    pub fn telemetry_snapshot(&self) -> crate::telemetry::ReplicationTelemetrySnapshot {
        let progress = self.replica_progress();
        self.telemetry.snapshot(&progress)
    }

    pub async fn catch_up_from(
        &self,
        peer: &str,
        body: &ReplicateCatchUpRequest,
    ) -> Result<ReplicateCatchUpRange, ReplicateError> {
        let url = format!(
            "{}/internal/v1/replicate/catch-up",
            peer.trim_end_matches('/')
        );
        let resp = Self::apply_cluster_secret(self.http.post(&url).json(body))
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let detail = resp.text().await.unwrap_or_default();
            return Err(ReplicateError::PeerRejected {
                peer: peer.to_string(),
                status,
                detail,
            });
        }
        let binary = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/octet-stream"));
        if resp
            .content_length()
            .is_some_and(|length| length > MAX_DECODE_EPOCH_BYTES as u64)
        {
            return Err(ReplicateError::EpochTooLarge {
                bytes: resp.content_length().unwrap_or_default() as usize,
                limit: MAX_DECODE_EPOCH_BYTES,
            });
        }
        let mut stream = resp.bytes_stream();
        let mut bytes = bytes::BytesMut::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if bytes.len().saturating_add(chunk.len()) > MAX_DECODE_EPOCH_BYTES {
                return Err(ReplicateError::EpochTooLarge {
                    bytes: bytes.len().saturating_add(chunk.len()),
                    limit: MAX_DECODE_EPOCH_BYTES,
                });
            }
            bytes.extend_from_slice(&chunk);
        }
        if binary {
            ReplicateCatchUpRange::decode_binary(&bytes)
        } else {
            let legacy = serde_json::from_slice::<ReplicateCatchUpResponse>(&bytes)?;
            ReplicateCatchUpRange::from_legacy(legacy)
        }
    }
}

fn env_bytes(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value: &usize| *value > 0)
        .unwrap_or(default)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

fn update_progress(
    progress: &Mutex<HashMap<(String, u32, Uuid), ReplicaProgress>>,
    topic: &str,
    partition: u32,
    peer_id: Uuid,
    leader_hwm: u64,
    durable_hwm: u64,
    success: bool,
) {
    let key = (topic.to_string(), partition, peer_id);
    let mut all = progress
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let entry = all.entry(key).or_insert_with(|| ReplicaProgress {
        peer_id,
        topic: topic.to_string(),
        partition,
        durable_hwm: 0,
        leader_hwm,
        lag: leader_hwm,
        in_sync: false,
        last_success_ms: None,
        consecutive_failures: 0,
    });
    entry.leader_hwm = leader_hwm;
    if success {
        entry.durable_hwm = entry.durable_hwm.max(durable_hwm);
        entry.last_success_ms = Some(now_ms());
        entry.consecutive_failures = 0;
    } else {
        entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
    }
    entry.lag = leader_hwm.saturating_sub(entry.durable_hwm);
    entry.in_sync = success && entry.durable_hwm >= leader_hwm;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(frames: Vec<Vec<u8>>) -> ReplicateBatchRequest {
        ReplicateBatchRequest::new(
            "tenant".into(),
            "jobs".into(),
            7,
            Uuid::new_v4(),
            11,
            42,
            frames,
        )
        .unwrap()
    }

    #[test]
    fn binary_epoch_round_trip_preserves_raw_frames() {
        let req = request(vec![b"\0binary-one\xff".to_vec(), b"two".to_vec()]);
        let encoded = req.encode_binary().unwrap();
        assert!(!encoded.windows(4).any(|bytes| bytes == b"AA=="));
        assert_eq!(ReplicateBatchRequest::decode_binary(&encoded).unwrap(), req);
    }

    #[test]
    fn binary_epoch_rejects_crc_and_count_corruption() {
        let req = request(vec![b"one".to_vec(), b"two".to_vec()]);
        let mut crc_corrupt = req.encode_binary().unwrap();
        *crc_corrupt.last_mut().unwrap() ^= 0x40;
        assert!(matches!(
            ReplicateBatchRequest::decode_binary(&crc_corrupt),
            Err(ReplicateError::InvalidEpoch(_))
        ));

        let mut count_corrupt = req.encode_binary().unwrap();
        count_corrupt[44..48].copy_from_slice(&3u32.to_be_bytes());
        assert!(matches!(
            ReplicateBatchRequest::decode_binary(&count_corrupt),
            Err(ReplicateError::InvalidEpoch(_))
        ));
    }

    #[test]
    fn binary_catch_up_range_round_trips_without_base64() {
        let range = ReplicateCatchUpRange {
            partition: 7,
            leader_generation: 12,
            committed_hwm: 45,
            frames: vec![
                CatchUpFrame {
                    offset: 42,
                    bytes: b"\0raw\xff".to_vec(),
                },
                CatchUpFrame {
                    offset: 43,
                    bytes: b"next".to_vec(),
                },
            ],
        };
        let encoded = range.encode_binary().unwrap();
        assert_eq!(&encoded[..4], &CATCHUP_MAGIC);
        assert_eq!(
            ReplicateCatchUpRange::decode_binary(&encoded).unwrap(),
            range
        );
    }

    #[test]
    fn legacy_catch_up_json_remains_decodable() {
        let legacy = ReplicateCatchUpResponse {
            committed_hwm: 2,
            frames: vec![ReplicateAppendRequest {
                tenant_id: "tenant".into(),
                topic: "jobs".into(),
                partition: 3,
                offset: 1,
                frame_b64: B64.encode(b"frame"),
                leader_generation: 9,
            }],
        };
        let decoded = ReplicateCatchUpRange::from_legacy(legacy).unwrap();
        assert_eq!(decoded.partition, 3);
        assert_eq!(decoded.frames[0].bytes, b"frame");
    }

    #[test]
    fn binary_catch_up_rejects_record_count_over_limit() {
        let range = ReplicateCatchUpRange {
            partition: 1,
            leader_generation: 1,
            committed_hwm: 0,
            frames: Vec::new(),
        };
        let mut encoded = range.encode_binary().unwrap();
        encoded[28..32].copy_from_slice(&((MAX_CATCHUP_RECORDS + 1) as u32).to_be_bytes());
        assert!(matches!(
            ReplicateCatchUpRange::decode_binary(&encoded),
            Err(ReplicateError::InvalidEpoch(message)) if message.contains("count")
        ));
    }

    #[tokio::test]
    async fn single_node_quorum_updates_native_telemetry() {
        let node_id = Uuid::new_v4();
        let client = ReplicationClient::new(ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: vec![broker_raft_meta::NodeConfig {
                id: node_id,
                addr: "http://127.0.0.1:8080".into(),
            }],
            node_id,
            generation: 1,
            hash_version: 1,
        });
        let request = request(vec![b"frame".to_vec()]);
        let required_hwm = request.end_offset().unwrap();
        let outcome = client.replicate_batch(request, required_hwm).await.unwrap();
        assert_eq!(outcome.durable_acks, 1);
        let snapshot = client.telemetry_snapshot();
        let shard = snapshot.shards.iter().find(|item| item.shard == 7).unwrap();
        assert_eq!(shard.quorum_requests, 1);
        assert_eq!(shard.quorum_failures, 0);
        assert_eq!(shard.last_durable_acks, 1);
        assert_eq!(shard.isr_members, 1);
    }

    #[tokio::test]
    async fn local_fsync_overlaps_replica_fanout() {
        let leader = Uuid::new_v4();
        let follower = Uuid::new_v4();
        let client = ReplicationClient::new(ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: vec![
                broker_raft_meta::NodeConfig {
                    id: leader,
                    addr: "http://127.0.0.1:1".into(),
                },
                broker_raft_meta::NodeConfig {
                    id: follower,
                    addr: "http://127.0.0.1:1".into(),
                },
            ],
            node_id: leader,
            generation: 1,
            hash_version: 1,
        });
        let request = request(vec![b"frame".to_vec()]);
        let required_hwm = request.end_offset().unwrap();
        let started = Instant::now();
        let err = client
            .replicate_batch_with_local(request, async move {
                tokio::time::sleep(Duration::from_millis(40)).await;
                Ok(required_hwm)
            })
            .await
            .expect_err("unreachable follower cannot form minISR");
        assert!(matches!(
            err,
            ReplicateError::QuorumNotReached { acked: 1, .. }
        ));
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_millis(400),
            "fan-out must not wait for local fsync before connecting: {elapsed:?}"
        );
    }
}
