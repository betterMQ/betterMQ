//! Append-only partition log: active WAL + sealed segments.

use crate::manifest::{WalManifest, WAL_FORMAT_V1, WAL_FORMAT_V2};
use crate::meta::LogMeta;
use broker_proto::epoch::{
    decode_epoch, encode_epoch, EpochError, EpochHeader, EPOCH_HEADER_BYTES,
};
use broker_proto::record::{decode_frame, encode_frame, RecordError};
use broker_proto::{LogRecord, StoredMessage};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Cursor, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::info;

const WAL_FILE: &str = "active.wal";
const SEGMENTS_DIR: &str = "segments";
const DEFAULT_GROUP_INTERVAL_MS: u64 = 10;
const DEFAULT_SPARSE_INDEX_STRIDE: u64 = 128;
const DEFAULT_READER_CACHE_CAPACITY: usize = 64;

/// How often the local WAL is forced to stable storage.
///
/// Override with `BETTERMQ_FSYNC=always|group|os` (default `group`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncMode {
    /// `sync_all` + persist `meta.json` after every record.
    Always,
    /// Write without fsync; flush WAL + meta together on a timer, request end, roll, or drop.
    Group,
    /// No explicit fsync (OS page cache). Fastest; a crash can lose recent writes.
    Os,
}

impl FsyncMode {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "always" | "every" | "record" => Some(Self::Always),
            "group" | "batch" => Some(Self::Group),
            "os" | "never" | "none" => Some(Self::Os),
            _ => None,
        }
    }

    pub fn from_env() -> Self {
        match std::env::var("BETTERMQ_FSYNC") {
            Ok(raw) if !raw.trim().is_empty() => Self::parse(&raw).unwrap_or_else(|| {
                tracing::warn!(
                    value = %raw,
                    "invalid BETTERMQ_FSYNC (use always|group|os); defaulting to group"
                );
                Self::Group
            }),
            _ => Self::Group,
        }
    }
}

fn group_interval_from_env() -> Duration {
    let ms = std::env::var("BETTERMQ_FSYNC_INTERVAL_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_GROUP_INTERVAL_MS)
        .clamp(1, 60_000);
    Duration::from_millis(ms)
}

#[derive(Debug, Clone)]
pub struct PartitionLogConfig {
    /// Roll WAL into a segment when it exceeds this many bytes.
    pub segment_max_bytes: u64,
    pub fsync: FsyncMode,
    /// Max delay before a group flush (used when `fsync` is [`FsyncMode::Group`]).
    pub group_interval: Duration,
}

impl Default for PartitionLogConfig {
    fn default() -> Self {
        Self {
            segment_max_bytes: 8 * 1024 * 1024,
            fsync: FsyncMode::Group,
            group_interval: Duration::from_millis(DEFAULT_GROUP_INTERVAL_MS),
        }
    }
}

impl PartitionLogConfig {
    pub fn from_env() -> Self {
        Self {
            segment_max_bytes: 8 * 1024 * 1024,
            fsync: FsyncMode::from_env(),
            group_interval: group_interval_from_env(),
        }
    }
}

#[derive(Debug, Clone)]
struct LogPosition {
    path: PathBuf,
    byte_offset: u64,
}

#[derive(Debug, Clone)]
struct SparsePoint {
    offset: u64,
    byte_offset: u64,
}

#[derive(Debug, Clone)]
struct SegmentIndex {
    path: PathBuf,
    first_offset: u64,
    last_offset: u64,
    points: Vec<SparsePoint>,
}

struct ReaderCache {
    files: HashMap<PathBuf, File>,
    order: VecDeque<PathBuf>,
    capacity: usize,
}

impl ReaderCache {
    fn new() -> Self {
        let capacity = std::env::var("BETTERMQ_SEGMENT_READER_CACHE")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_READER_CACHE_CAPACITY)
            .clamp(1, 4096);
        Self {
            files: HashMap::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    fn clone_reader(&mut self, path: &Path) -> io::Result<File> {
        if !self.files.contains_key(path) {
            while self.files.len() >= self.capacity {
                if let Some(evicted) = self.order.pop_front() {
                    self.files.remove(&evicted);
                }
            }
            self.files.insert(path.to_path_buf(), File::open(path)?);
        }
        self.order.retain(|cached| cached != path);
        self.order.push_back(path.to_path_buf());
        self.files.get(path).expect("reader inserted").try_clone()
    }

    fn remove(&mut self, path: &Path) {
        self.files.remove(path);
        self.order.retain(|cached| cached != path);
    }
}

#[derive(Debug, Error)]
pub enum LogError {
    #[error("record error: {0}")]
    Record(#[from] RecordError),
    #[error("epoch error: {0}")]
    Epoch(#[from] EpochError),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("offset {0} not found")]
    OffsetNotFound(u64),
    #[error("offset mismatch: have next={have}, want={want}")]
    OffsetMismatch { have: u64, want: u64 },
    #[error("stale leader fence: our generation {ours} < stored {stored}")]
    StaleFence { ours: u64, stored: u64 },
    #[error("corrupt segment {path} at offset {offset}: {source}")]
    CorruptSegment {
        path: String,
        offset: u64,
        source: RecordError,
    },
    #[error("slate: {0}")]
    Slate(String),
}

/// Durable log for one partition directory.
pub struct PartitionLog {
    dir: PathBuf,
    config: PartitionLogConfig,
    meta: LogMeta,
    manifest: WalManifest,
    wal: File,
    wal_size: u64,
    /// Full index is bounded to the active WAL only.
    index: BTreeMap<u64, LogPosition>,
    sealed_indexes: Vec<SegmentIndex>,
    active_base_offset: u64,
    /// Open readers use a bounded LRU; clones have independent cursors.
    reader_cache: Mutex<ReaderCache>,
    dirty: bool,
    last_flush: Instant,
    /// Last offset exclusive that has been fsynced (commit high watermark).
    committed_hwm: u64,
    flush_count: u64,
    fail_next_sync: bool,
    telemetry: crate::telemetry::StorageTelemetryHandle,
}

impl PartitionLog {
    pub fn open(dir: impl AsRef<Path>, config: PartitionLogConfig) -> Result<Self, LogError> {
        Self::open_for_shard(dir, config, 0, WAL_FORMAT_V2)
    }

    pub fn open_for_shard(
        dir: impl AsRef<Path>,
        config: PartitionLogConfig,
        shard_id: u32,
        requested_format: u16,
    ) -> Result<Self, LogError> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        std::fs::create_dir_all(dir.join(SEGMENTS_DIR))?;
        let manifest = WalManifest::load_or_create(&dir, requested_format, shard_id)?;

        let meta = match LogMeta::load(&dir) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => LogMeta::default(),
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                if broker_proto::allow_empty_metadata_recovery() {
                    LogMeta::default()
                } else {
                    return Err(LogError::Io(e));
                }
            }
            Err(e) => return Err(e.into()),
        };
        let wal_path = dir.join(WAL_FILE);
        let wal = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&wal_path)?;

        let mut log = Self {
            dir: dir.clone(),
            config,
            meta,
            manifest,
            wal,
            wal_size: 0,
            index: BTreeMap::new(),
            sealed_indexes: Vec::new(),
            active_base_offset: 0,
            reader_cache: Mutex::new(ReaderCache::new()),
            dirty: false,
            last_flush: Instant::now(),
            committed_hwm: 0,
            flush_count: 0,
            fail_next_sync: false,
            telemetry: crate::telemetry::register_shard(shard_id),
        };

        log.rebuild_from_disk()?;
        log.committed_hwm = log.meta.next_offset;

        info!(
            dir = %dir.display(),
            next_offset = log.meta.next_offset,
            records = log.index.len(),
            wal_format = log.manifest.wal_format_version,
            "partition log opened"
        );

        Ok(log)
    }

    fn rebuild_from_disk(&mut self) -> Result<(), LogError> {
        self.index.clear();
        self.sealed_indexes.clear();
        let mut next = 0u64;

        let mut segment_files: Vec<PathBuf> = std::fs::read_dir(self.dir.join(SEGMENTS_DIR))?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.is_file())
            .collect();
        segment_files.sort();

        for path in &segment_files {
            let first = next;
            next = self.scan_file(path, next, false)?;
            if next > first {
                self.sealed_indexes
                    .push(self.build_sparse_index(path, first, next)?);
                self.index
                    .retain(|offset, _| *offset < first || *offset >= next);
            }
        }

        self.active_base_offset = next;
        let wal_path = self.dir.join(WAL_FILE);
        next = self.scan_file(&wal_path, next, true)?;

        self.wal_size = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        self.meta.next_offset = next;
        self.meta.save(&self.dir)?;
        Ok(())
    }

    fn build_sparse_index(
        &self,
        path: &Path,
        first_offset: u64,
        last_offset: u64,
    ) -> Result<SegmentIndex, LogError> {
        let mut points = Vec::new();
        let mut file = BufReader::new(File::open(path)?);
        match self.manifest.wal_format_version {
            WAL_FORMAT_V1 => {
                let mut offset = first_offset;
                while offset < last_offset {
                    let byte_offset = file.stream_position()?;
                    decode_frame(&mut file)?;
                    if (offset - first_offset) % DEFAULT_SPARSE_INDEX_STRIDE == 0 {
                        points.push(SparsePoint {
                            offset,
                            byte_offset,
                        });
                    }
                    offset += 1;
                }
            }
            WAL_FORMAT_V2 => {
                while file.stream_position()? < std::fs::metadata(path)?.len() {
                    let epoch_start = file.stream_position()?;
                    let (epoch, _) = decode_epoch(&mut file)?;
                    for offset in epoch.first_offset..epoch.committed_hwm {
                        if (offset - first_offset) % DEFAULT_SPARSE_INDEX_STRIDE == 0 {
                            points.push(SparsePoint {
                                offset,
                                byte_offset: epoch_start,
                            });
                        }
                    }
                }
            }
            version => {
                return Err(LogError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported WAL format {version}"),
                )))
            }
        }
        if points.is_empty() {
            points.push(SparsePoint {
                offset: first_offset,
                byte_offset: 0,
            });
        }
        Ok(SegmentIndex {
            path: path.to_path_buf(),
            first_offset,
            last_offset,
            points,
        })
    }

    fn scan_file(
        &mut self,
        path: &Path,
        next_offset: u64,
        truncate_torn_tail: bool,
    ) -> Result<u64, LogError> {
        match self.manifest.wal_format_version {
            WAL_FORMAT_V1 => self.scan_v1_file(path, next_offset, truncate_torn_tail),
            WAL_FORMAT_V2 => self.scan_v2_file(path, next_offset, truncate_torn_tail),
            version => Err(LogError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported WAL format {version}"),
            ))),
        }
    }

    fn scan_v1_file(
        &mut self,
        path: &Path,
        mut next_offset: u64,
        truncate_torn_tail: bool,
    ) -> Result<u64, LogError> {
        let len = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        if len == 0 {
            return Ok(next_offset);
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut byte_pos = 0u64;

        while byte_pos < len {
            let frame_start = byte_pos;
            match decode_frame(&mut reader) {
                Ok((_header, _payload)) => {
                    if !self.meta.purged_offsets.contains(&next_offset) {
                        self.index.insert(
                            next_offset,
                            LogPosition {
                                path: path.to_path_buf(),
                                byte_offset: frame_start,
                            },
                        );
                    }
                    next_offset += 1;
                    byte_pos = reader.stream_position()?;
                }
                Err(RecordError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    if truncate_torn_tail {
                        truncate_file(path, frame_start)?;
                    }
                    break;
                }
                Err(e) => {
                    let pos = reader.stream_position().unwrap_or(byte_pos);
                    if pos < len {
                        quarantine_corrupt_file(path);
                        return Err(LogError::CorruptSegment {
                            path: path.display().to_string(),
                            offset: next_offset,
                            source: e,
                        });
                    }
                    break;
                }
            }
        }
        Ok(next_offset)
    }

    fn scan_v2_file(
        &mut self,
        path: &Path,
        mut next_offset: u64,
        truncate_torn_tail: bool,
    ) -> Result<u64, LogError> {
        let len = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        if len == 0 {
            return Ok(next_offset);
        }
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut byte_pos = 0u64;

        while byte_pos < len {
            let epoch_start = byte_pos;
            let (epoch, body) = match decode_epoch(&mut reader) {
                Ok(decoded) => decoded,
                Err(EpochError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    if truncate_torn_tail {
                        truncate_file(path, epoch_start)?;
                        break;
                    }
                    return Err(LogError::Epoch(EpochError::Io(e)));
                }
                Err(e) => {
                    if truncate_torn_tail
                        && len.saturating_sub(epoch_start) < EPOCH_HEADER_BYTES as u64
                    {
                        truncate_file(path, epoch_start)?;
                        break;
                    }
                    quarantine_corrupt_file(path);
                    return Err(LogError::Epoch(e));
                }
            };
            if epoch.shard_id != self.manifest.shard_id
                || epoch.first_offset != next_offset
                || epoch.committed_hwm
                    != epoch.first_offset.saturating_add(epoch.record_count as u64)
            {
                quarantine_corrupt_file(path);
                return Err(LogError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "invalid epoch: shard={} first={} count={} hwm={}",
                        epoch.shard_id, epoch.first_offset, epoch.record_count, epoch.committed_hwm
                    ),
                )));
            }

            let mut frames = Cursor::new(body.as_slice());
            for _ in 0..epoch.record_count {
                let frame_start = frames.position();
                decode_frame(&mut frames).map_err(|source| LogError::CorruptSegment {
                    path: path.display().to_string(),
                    offset: next_offset,
                    source,
                })?;
                if !self.meta.purged_offsets.contains(&next_offset) {
                    self.index.insert(
                        next_offset,
                        LogPosition {
                            path: path.to_path_buf(),
                            byte_offset: epoch_start + EPOCH_HEADER_BYTES as u64 + frame_start,
                        },
                    );
                }
                next_offset += 1;
            }
            if frames.position() != body.len() as u64 || next_offset != epoch.committed_hwm {
                quarantine_corrupt_file(path);
                return Err(LogError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "epoch record count/length mismatch",
                )));
            }
            byte_pos = reader.stream_position()?;
        }
        Ok(next_offset)
    }

    fn apply_fence(&mut self, fence_generation: Option<u64>) -> Result<(), LogError> {
        let Some(gen) = fence_generation else {
            return Ok(());
        };
        if gen < self.meta.leader_generation {
            return Err(LogError::StaleFence {
                ours: gen,
                stored: self.meta.leader_generation,
            });
        }
        if gen > self.meta.leader_generation {
            self.meta.leader_generation = gen;
            self.dirty = true;
        }
        Ok(())
    }

    /// Append a pre-encoded broker-proto frame (follower replication).
    /// When `expected_offset` is set, require `meta.next_offset == expected` (or
    /// idempotent success if that offset is already present).
    pub fn append_raw_frame(
        &mut self,
        partition: u32,
        frame: &[u8],
        expected_offset: Option<u64>,
    ) -> Result<StoredMessage, LogError> {
        let (header, payload) = {
            let mut cursor = std::io::Cursor::new(frame);
            decode_frame(&mut cursor)?
        };

        if let Some(want) = expected_offset {
            let have = self.meta.next_offset;
            if have > want {
                // Idempotent retry: already applied this offset.
                if let Some(existing) = self.read_range(partition, want, 1)?.into_iter().next() {
                    if existing.offset == want && existing.id == header.id {
                        return Ok(existing);
                    }
                }
                return Err(LogError::OffsetMismatch { have, want });
            }
            if have < want {
                return Err(LogError::OffsetMismatch { have, want });
            }
        }

        if self.manifest.wal_format_version == WAL_FORMAT_V2 {
            let mut appended = self.append_batch(partition, vec![(header, payload)], None)?;
            return appended.pop().map(|(stored, _)| stored).ok_or_else(|| {
                LogError::Io(io::Error::other(
                    "empty result from single-record epoch append",
                ))
            });
        }

        let offset = self.meta.next_offset;
        let byte_offset = self.wal_size;
        let wal_path = self.dir.join(WAL_FILE);

        use std::io::Write;
        self.wal.write_all(frame)?;

        self.index.insert(
            offset,
            LogPosition {
                path: wal_path,
                byte_offset,
            },
        );
        self.wal_size += frame.len() as u64;
        self.meta.next_offset = offset + 1;
        self.after_write()?;

        if self.wal_size >= self.config.segment_max_bytes {
            self.roll_segment()?;
        }

        Ok(StoredMessage {
            id: header.id,
            tenant_id: header.tenant_id,
            topic: header.topic,
            partition,
            offset,
            routing_key: header.routing_key,
            payload,
            published_at_ms: header.published_at_ms,
            priority: header.priority,
            flow_parallelism: header.flow_parallelism,
            flow_key: header.flow_key.clone(),
            flow_rate: header.flow_rate,
            flow_period_secs: header.flow_period_secs,
            queue_id: header.queue_id,
            group_id: header.group_id,
            group_member_id: header.group_member_id,
            flow_profile_id: header.flow_profile_id,
            destination_url: header.destination_url.clone(),
            destination_secret: header.destination_secret.clone(),
            max_retries: header.max_retries,
            retry_backoff: header.retry_backoff.clone(),
            http_method: header.http_method.clone(),
            http_headers_json: header.http_headers_json.clone(),
            http_sign: header.http_sign,
            payload_ref_json: header.payload_ref_json.clone(),
        })
    }

    pub fn append(
        &mut self,
        partition: u32,
        header: LogRecord,
        payload: Vec<u8>,
    ) -> Result<(StoredMessage, Vec<u8>), LogError> {
        self.append_fenced(partition, header, payload, None)
    }

    pub fn append_fenced(
        &mut self,
        partition: u32,
        header: LogRecord,
        payload: Vec<u8>,
        fence_generation: Option<u64>,
    ) -> Result<(StoredMessage, Vec<u8>), LogError> {
        self.apply_fence(fence_generation)?;
        if self.manifest.wal_format_version == WAL_FORMAT_V2 {
            return self
                .append_batch(partition, vec![(header, payload)], fence_generation)?
                .pop()
                .ok_or_else(|| {
                    LogError::Io(io::Error::other(
                        "empty result from single-record epoch append",
                    ))
                });
        }
        let offset = self.meta.next_offset;
        let byte_offset = self.wal_size;
        let wal_path = self.dir.join(WAL_FILE);

        let mut frame = Vec::new();
        encode_frame(&header, &payload, &mut frame)?;
        use std::io::Write;
        self.wal.write_all(&frame)?;

        self.index.insert(
            offset,
            LogPosition {
                path: wal_path,
                byte_offset,
            },
        );
        self.wal_size += frame.len() as u64;
        self.meta.next_offset = offset + 1;
        self.after_write()?;

        if self.wal_size >= self.config.segment_max_bytes {
            self.roll_segment()?;
        }

        let stored = StoredMessage {
            id: header.id,
            tenant_id: header.tenant_id,
            topic: header.topic,
            partition,
            offset,
            routing_key: header.routing_key,
            payload,
            published_at_ms: header.published_at_ms,
            priority: header.priority,
            flow_parallelism: header.flow_parallelism,
            flow_key: header.flow_key.clone(),
            flow_rate: header.flow_rate,
            flow_period_secs: header.flow_period_secs,
            queue_id: header.queue_id,
            group_id: header.group_id,
            group_member_id: header.group_member_id,
            flow_profile_id: header.flow_profile_id,
            destination_url: header.destination_url.clone(),
            destination_secret: header.destination_secret.clone(),
            max_retries: header.max_retries,
            retry_backoff: header.retry_backoff.clone(),
            http_method: header.http_method.clone(),
            http_headers_json: header.http_headers_json.clone(),
            http_sign: header.http_sign,
            payload_ref_json: header.payload_ref_json.clone(),
        };
        Ok((stored, frame))
    }

    /// Append many records with one `after_write` (one dirty epoch).
    pub fn append_batch(
        &mut self,
        partition: u32,
        items: Vec<(LogRecord, Vec<u8>)>,
        fence_generation: Option<u64>,
    ) -> Result<Vec<(StoredMessage, Vec<u8>)>, LogError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        self.apply_fence(fence_generation)?;
        let mut out = Vec::with_capacity(items.len());
        let mut buf = Vec::new();
        let wal_path = self.dir.join(WAL_FILE);
        let first_offset = self.meta.next_offset;
        let epoch_prefix = if self.manifest.wal_format_version == WAL_FORMAT_V2 {
            EPOCH_HEADER_BYTES as u64
        } else {
            0
        };
        let mut byte_offset = self.wal_size + epoch_prefix;
        let mut offset = first_offset;
        for (header, payload) in items {
            let frame_start = buf.len();
            encode_frame(&header, &payload, &mut buf)?;
            let frame = buf[frame_start..].to_vec();
            self.index.insert(
                offset,
                LogPosition {
                    path: wal_path.clone(),
                    byte_offset,
                },
            );
            byte_offset += frame.len() as u64;
            let stored = StoredMessage {
                id: header.id,
                tenant_id: header.tenant_id,
                topic: header.topic,
                partition,
                offset,
                routing_key: header.routing_key,
                payload,
                published_at_ms: header.published_at_ms,
                priority: header.priority,
                flow_parallelism: header.flow_parallelism,
                flow_key: header.flow_key.clone(),
                flow_rate: header.flow_rate,
                flow_period_secs: header.flow_period_secs,
                queue_id: header.queue_id,
                group_id: header.group_id,
                group_member_id: header.group_member_id,
                flow_profile_id: header.flow_profile_id,
                destination_url: header.destination_url.clone(),
                destination_secret: header.destination_secret.clone(),
                max_retries: header.max_retries,
                retry_backoff: header.retry_backoff.clone(),
                http_method: header.http_method.clone(),
                http_headers_json: header.http_headers_json.clone(),
                http_sign: header.http_sign,
                payload_ref_json: header.payload_ref_json.clone(),
            };
            offset += 1;
            out.push((stored, frame));
        }
        use std::io::Write;
        let epoch_bytes = if self.manifest.wal_format_version == WAL_FORMAT_V2 {
            let epoch = EpochHeader::v2(
                self.manifest.shard_id,
                self.meta.leader_generation,
                first_offset,
                out.len() as u32,
                &buf,
            );
            let mut encoded = Vec::with_capacity(EPOCH_HEADER_BYTES + buf.len());
            encode_epoch(&epoch, &buf, &mut encoded)?;
            self.wal.write_all(&encoded)?;
            self.wal_size = self.wal_size.saturating_add(encoded.len() as u64);
            encoded.len() as u64
        } else {
            self.wal.write_all(&buf)?;
            self.wal_size = byte_offset;
            buf.len() as u64
        };
        self.meta.next_offset = offset;
        self.after_write()?;
        self.telemetry
            .record_epoch(self.meta.leader_generation, out.len() as u64, epoch_bytes);
        if self.wal_size >= self.config.segment_max_bytes {
            self.roll_segment()?;
        }
        Ok(out)
    }

    fn roll_segment(&mut self) -> Result<(), LogError> {
        if self.wal_size == 0 {
            return Ok(());
        }

        let roll = self.meta.segment_roll_count;
        let seg_path = self
            .dir
            .join(SEGMENTS_DIR)
            .join(format!("segment-{roll:06}.log"));
        let wal_path = self.dir.join(WAL_FILE);

        if self.config.fsync != FsyncMode::Os {
            let started = Instant::now();
            let pending_records = self.meta.next_offset.saturating_sub(self.committed_hwm);
            if let Err(error) = self.wal.sync_all() {
                self.telemetry
                    .record_fsync(pending_records, started.elapsed(), false);
                return Err(error.into());
            }
            self.telemetry
                .record_fsync(pending_records, started.elapsed(), true);
        }

        // Close the WAL handle before rename. Do not truncate active.wal first.
        let placeholder = self.dir.join(".wal_fd_placeholder");
        let _closed = std::mem::replace(
            &mut self.wal,
            OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&placeholder)?,
        );

        std::fs::rename(&wal_path, &seg_path)?;
        if let Ok(mut cache) = self.reader_cache.lock() {
            cache.remove(&wal_path);
            cache.remove(&seg_path);
        }
        if self.config.fsync != FsyncMode::Os {
            // Durability of the segment rename (parent dir entry).
            if let Ok(dir) = std::fs::File::open(self.dir.join(SEGMENTS_DIR)) {
                let _ = dir.sync_all();
            }
            if let Ok(dir) = std::fs::File::open(&self.dir) {
                let _ = dir.sync_all();
            }
        }
        let _ = std::fs::remove_file(&placeholder);

        self.meta.segment_roll_count += 1;
        self.meta
            .save_with(&self.dir, self.config.fsync != FsyncMode::Os)?;
        self.dirty = false;
        self.last_flush = Instant::now();
        self.committed_hwm = self.meta.next_offset;

        if self.meta.next_offset > self.active_base_offset {
            self.sealed_indexes.push(self.build_sparse_index(
                &seg_path,
                self.active_base_offset,
                self.meta.next_offset,
            )?);
        }
        self.index.retain(|_, pos| pos.path != wal_path);
        self.active_base_offset = self.meta.next_offset;

        self.wal = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&wal_path)?;
        self.wal_size = 0;
        crate::archive::enqueue_sealed_segment(&seg_path);
        Ok(())
    }

    pub fn read_range(
        &self,
        partition: u32,
        start_offset: u64,
        max_messages: usize,
    ) -> Result<Vec<StoredMessage>, LogError> {
        let mut out = Vec::with_capacity(max_messages.min(256));
        for segment in self
            .sealed_indexes
            .iter()
            .filter(|segment| segment.last_offset > start_offset)
        {
            self.read_sparse_segment(segment, partition, start_offset, max_messages, &mut out)?;
            if out.len() >= max_messages {
                return Ok(out);
            }
        }
        for (&offset, pos) in self.index.range(start_offset..) {
            if out.len() >= max_messages {
                break;
            }
            let (header, payload) = self.read_at(pos)?;
            out.push(stored_message(header, payload, partition, offset));
        }
        Ok(out)
    }

    fn read_sparse_segment(
        &self,
        segment: &SegmentIndex,
        partition: u32,
        start_offset: u64,
        max_messages: usize,
        out: &mut Vec<StoredMessage>,
    ) -> Result<(), LogError> {
        let point = segment
            .points
            .iter()
            .rev()
            .find(|point| point.offset <= start_offset)
            .unwrap_or(&segment.points[0]);
        let mut file = self.cached_reader(&segment.path)?;
        file.seek(SeekFrom::Start(point.byte_offset))?;
        match self.manifest.wal_format_version {
            WAL_FORMAT_V1 => {
                let mut offset = point.offset;
                while offset < segment.last_offset && out.len() < max_messages {
                    let (header, payload) = decode_frame(&mut file)?;
                    if offset >= start_offset && !self.meta.purged_offsets.contains(&offset) {
                        out.push(stored_message(header, payload, partition, offset));
                    }
                    offset += 1;
                }
            }
            WAL_FORMAT_V2 => {
                while out.len() < max_messages {
                    let position = file.stream_position()?;
                    if position >= std::fs::metadata(&segment.path)?.len() {
                        break;
                    }
                    let (epoch, body) = decode_epoch(&mut file)?;
                    if epoch.first_offset >= segment.last_offset {
                        break;
                    }
                    let mut frames = Cursor::new(body);
                    for offset in epoch.first_offset..epoch.committed_hwm {
                        let (header, payload) = decode_frame(&mut frames)?;
                        if offset >= start_offset
                            && offset < segment.last_offset
                            && !self.meta.purged_offsets.contains(&offset)
                        {
                            out.push(stored_message(header, payload, partition, offset));
                            if out.len() >= max_messages {
                                break;
                            }
                        }
                    }
                }
            }
            _ => unreachable!("manifest version validated at open"),
        }
        Ok(())
    }

    pub fn high_watermark(&self) -> u64 {
        self.meta.next_offset
    }

    /// Exclusive offset of the last durable (fsynced) record.
    pub fn committed_hwm(&self) -> u64 {
        self.committed_hwm
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub fn fsync_mode(&self) -> FsyncMode {
        self.config.fsync
    }

    pub fn flush_count(&self) -> u64 {
        self.flush_count
    }

    pub fn resident_index_entries(&self) -> usize {
        self.index.len()
            + self
                .sealed_indexes
                .iter()
                .map(|segment| segment.points.len())
                .sum::<usize>()
    }

    /// Fail the next durability barrier before advancing the committed HWM.
    pub fn inject_fsync_failure(&mut self) {
        self.fail_next_sync = true;
    }

    /// Delete sealed segments whose last offset is at or below `watermark`.
    /// The active WAL is never deleted. Watermark only moves forward.
    pub fn gc_sealed_below(&mut self, watermark: u64) -> Result<usize, LogError> {
        let current = self.meta.gc_watermark;
        if watermark < current {
            return Err(LogError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("gc watermark {watermark} is below durable {current}"),
            )));
        }
        let mut deleted = 0usize;
        let mut kept = Vec::new();
        for segment in self.sealed_indexes.drain(..) {
            if segment.last_offset <= watermark && segment.last_offset > 0 {
                if let Ok(mut cache) = self.reader_cache.lock() {
                    cache.remove(&segment.path);
                }
                match std::fs::remove_file(&segment.path) {
                    Ok(()) => deleted += 1,
                    Err(err) if err.kind() == io::ErrorKind::NotFound => deleted += 1,
                    Err(err) => {
                        kept.push(segment);
                        return Err(err.into());
                    }
                }
            } else {
                kept.push(segment);
            }
        }
        self.sealed_indexes = kept;
        self.meta.gc_watermark = watermark;
        self.meta
            .save_with(&self.dir, self.config.fsync != FsyncMode::Os)?;
        Ok(deleted)
    }

    /// Tombstone a record: drop from the index and persist the offset in [`LogMeta`].
    /// WAL/segment bytes are not rewritten; [`Self::rebuild_from_disk`] skips purged offsets.
    pub fn purge_offset(&mut self, offset: u64) -> bool {
        if self.meta.purged_offsets.contains(&offset) {
            return false;
        }
        if offset >= self.meta.next_offset {
            return false;
        }
        let removed = self.index.remove(&offset).is_some()
            || self
                .sealed_indexes
                .iter()
                .any(|segment| offset >= segment.first_offset && offset < segment.last_offset);
        if removed {
            self.meta.purged_offsets.insert(offset);
            self.dirty = true;
            if self.config.fsync == FsyncMode::Always {
                if let Err(e) = self.flush_durable() {
                    tracing::warn!(error = %e, offset, "failed to persist purged offset");
                }
            }
        }
        removed
    }

    fn read_at(&self, pos: &LogPosition) -> Result<(LogRecord, Vec<u8>), LogError> {
        let mut file = self.cached_reader(&pos.path)?;
        file.seek(SeekFrom::Start(pos.byte_offset))?;
        decode_frame(&mut file).map_err(LogError::from)
    }

    fn cached_reader(&self, path: &Path) -> Result<File, LogError> {
        let mut cache = self
            .reader_cache
            .lock()
            .map_err(|_| LogError::Io(io::Error::other("segment reader cache poisoned")))?;
        Ok(cache.clone_reader(path)?)
    }

    fn after_write(&mut self) -> Result<(), LogError> {
        self.dirty = true;
        match self.config.fsync {
            FsyncMode::Always => self.flush_durable(),
            // Group/os: persist at HTTP request end, the background timer, roll, or drop.
            // Flushing here when `group_interval` elapsed re-enters fsync on the
            // ingest path and collapses back to ~one fsync per message.
            FsyncMode::Group | FsyncMode::Os => Ok(()),
        }
    }

    /// Persist WAL if there are unflushed writes. `meta.json` is a checkpoint,
    /// not the commit path — recovery reconstructs `next_offset` from the WAL.
    pub fn flush_durable(&mut self) -> Result<(), LogError> {
        if !self.dirty {
            return Ok(());
        }
        let started = Instant::now();
        let pending_records = self.meta.next_offset.saturating_sub(self.committed_hwm);
        if self.fail_next_sync {
            self.fail_next_sync = false;
            self.telemetry
                .record_fsync(pending_records, started.elapsed(), false);
            return Err(LogError::Io(io::Error::other("injected fsync failure")));
        }
        let persist_sync = self.config.fsync != FsyncMode::Os;
        if persist_sync {
            if let Err(error) = self.wal.sync_data() {
                self.telemetry
                    .record_fsync(pending_records, started.elapsed(), false);
                return Err(error.into());
            }
        }
        self.committed_hwm = self.meta.next_offset;
        self.flush_count = self.flush_count.saturating_add(1);
        let checkpoint = self.config.fsync == FsyncMode::Always || self.flush_count % 32 == 0;
        if checkpoint {
            if let Err(error) = self.meta.save_with(&self.dir, persist_sync) {
                if persist_sync {
                    self.telemetry
                        .record_fsync(pending_records, started.elapsed(), false);
                }
                return Err(error.into());
            }
        }
        self.dirty = false;
        self.last_flush = Instant::now();
        if persist_sync {
            self.telemetry
                .record_fsync(pending_records, started.elapsed(), true);
        }
        Ok(())
    }

    pub fn flush_if_due(&mut self) -> Result<(), LogError> {
        if self.config.fsync != FsyncMode::Group || !self.dirty {
            return Ok(());
        }
        if self.last_flush.elapsed() >= self.config.group_interval {
            self.flush_durable()
        } else {
            Ok(())
        }
    }

    pub fn sync(&mut self) -> Result<(), LogError> {
        self.flush_durable()
    }
}

impl Drop for PartitionLog {
    fn drop(&mut self) {
        if let Err(e) = self.flush_durable() {
            tracing::warn!(error = %e, "wal flush on drop failed");
        }
        let persist_sync = self.config.fsync != FsyncMode::Os;
        if let Err(e) = self.meta.save_with(&self.dir, persist_sync) {
            tracing::warn!(error = %e, "meta checkpoint on drop failed");
        }
    }
}

fn quarantine_corrupt_file(path: &Path) {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let dest = path.with_extension(format!("corrupt.{ts}"));
    match std::fs::rename(path, &dest) {
        Ok(()) => tracing::error!(
            from = %path.display(),
            to = %dest.display(),
            "quarantined corrupt WAL/segment; refusing to skip later frames"
        ),
        Err(e) => tracing::error!(
            file = %path.display(),
            error = %e,
            "could not quarantine corrupt WAL/segment"
        ),
    }
}

fn truncate_file(path: &Path, len: u64) -> io::Result<()> {
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(len)?;
    file.sync_data()?;
    Ok(())
}

fn stored_message(
    header: LogRecord,
    payload: Vec<u8>,
    partition: u32,
    offset: u64,
) -> StoredMessage {
    StoredMessage {
        id: header.id,
        tenant_id: header.tenant_id,
        topic: header.topic,
        partition,
        offset,
        routing_key: header.routing_key,
        payload,
        published_at_ms: header.published_at_ms,
        priority: header.priority,
        flow_parallelism: header.flow_parallelism,
        flow_key: header.flow_key,
        flow_rate: header.flow_rate,
        flow_period_secs: header.flow_period_secs,
        queue_id: header.queue_id,
        group_id: header.group_id,
        group_member_id: header.group_member_id,
        flow_profile_id: header.flow_profile_id,
        destination_url: header.destination_url,
        destination_secret: header.destination_secret,
        max_retries: header.max_retries,
        retry_backoff: header.retry_backoff,
        http_method: header.http_method,
        http_headers_json: header.http_headers_json,
        http_sign: header.http_sign,
        payload_ref_json: header.payload_ref_json,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use uuid::Uuid;

    fn sample_record(topic: &str) -> LogRecord {
        LogRecord {
            id: Uuid::new_v4(),
            tenant_id: "default".into(),
            topic: topic.into(),
            routing_key: "rk".into(),
            idempotency_key: None,
            published_at_ms: 0,
            priority: 5,
            flow_parallelism: None,
            flow_key: None,
            flow_rate: None,
            flow_period_secs: None,
            queue_id: None,
            group_id: None,
            group_member_id: None,
            flow_profile_id: None,
            destination_url: None,
            destination_secret: None,
            max_retries: 0,
            retry_backoff: None,
            http_method: None,
            http_headers_json: None,
            http_sign: None,
            payload_ref_json: None,
        }
    }

    #[test]
    fn append_and_read_back() {
        let dir = tempdir().unwrap();
        let mut log = PartitionLog::open(dir.path(), PartitionLogConfig::default()).unwrap();
        log.append(0, sample_record("t"), b"one".to_vec()).unwrap();
        log.append(0, sample_record("t"), b"two".to_vec()).unwrap();

        let msgs = log.read_range(0, 0, 10).unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].offset, 0);
        assert_eq!(msgs[1].payload, b"two");
    }

    #[test]
    fn reopen_recovers() {
        let dir = tempdir().unwrap();
        {
            let mut log = PartitionLog::open(dir.path(), PartitionLogConfig::default()).unwrap();
            for i in 0..5u8 {
                log.append(0, sample_record("t"), vec![i]).unwrap();
            }
            log.sync().unwrap();
        }
        let log = PartitionLog::open(dir.path(), PartitionLogConfig::default()).unwrap();
        let msgs = log.read_range(0, 0, 10).unwrap();
        assert_eq!(msgs.len(), 5);
        assert_eq!(log.high_watermark(), 5);
    }

    #[test]
    fn purge_offset_removes_from_reads() {
        let dir = tempdir().unwrap();
        let mut log = PartitionLog::open(dir.path(), PartitionLogConfig::default()).unwrap();
        log.append(0, sample_record("t"), b"one".to_vec()).unwrap();
        assert!(log.purge_offset(0));
        let msgs = log.read_range(0, 0, 10).unwrap();
        assert!(msgs.is_empty());
    }

    #[test]
    fn purge_offset_survives_reopen() {
        let dir = tempdir().unwrap();
        {
            let mut log = PartitionLog::open(dir.path(), PartitionLogConfig::default()).unwrap();
            log.append(0, sample_record("t"), b"one".to_vec()).unwrap();
            log.append(0, sample_record("t"), b"two".to_vec()).unwrap();
            assert!(log.purge_offset(0));
            log.sync().unwrap();
        }
        let log = PartitionLog::open(dir.path(), PartitionLogConfig::default()).unwrap();
        let msgs = log.read_range(0, 0, 10).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].offset, 1);
        assert_eq!(msgs[0].payload, b"two");
    }

    #[test]
    fn segment_roll_preserves_records() {
        let dir = tempdir().unwrap();
        let cfg = PartitionLogConfig {
            segment_max_bytes: 256,
            fsync: FsyncMode::Always,
            group_interval: Duration::from_millis(10),
        };
        let mut log = PartitionLog::open(dir.path(), cfg.clone()).unwrap();
        for i in 0..20u8 {
            log.append(0, sample_record("t"), vec![i; 32]).unwrap();
        }
        let log = PartitionLog::open(dir.path(), cfg).unwrap();
        let msgs = log.read_range(0, 0, 50).unwrap();
        assert_eq!(msgs.len(), 20);
    }

    #[test]
    fn sealed_segment_index_is_sparse_and_readable() {
        let dir = tempdir().unwrap();
        let cfg = PartitionLogConfig {
            segment_max_bytes: 1024,
            fsync: FsyncMode::Group,
            group_interval: Duration::from_secs(60),
        };
        let mut log =
            PartitionLog::open_for_shard(dir.path(), cfg.clone(), 4, WAL_FORMAT_V2).unwrap();
        let items = (0..1000)
            .map(|index| (sample_record("sparse"), vec![(index % 251) as u8; 32]))
            .collect();
        log.append_batch(4, items, None).unwrap();
        assert!(
            log.resident_index_entries() < 20,
            "sealed segment should retain sparse anchors, not every record"
        );
        let messages = log.read_range(4, 777, 25).unwrap();
        assert_eq!(messages.len(), 25);
        assert_eq!(messages[0].offset, 777);
        assert_eq!(messages[24].offset, 801);
        log.sync().unwrap();
        drop(log);

        let recovered = PartitionLog::open_for_shard(dir.path(), cfg, 4, WAL_FORMAT_V2).unwrap();
        assert!(recovered.resident_index_entries() < 20);
        assert_eq!(recovered.read_range(4, 995, 10).unwrap().len(), 5);
    }

    #[test]
    fn stale_fence_is_rejected() {
        let dir = tempdir().unwrap();
        let mut log = PartitionLog::open(dir.path(), PartitionLogConfig::default()).unwrap();
        log.append_fenced(0, sample_record("t"), b"a".to_vec(), Some(3))
            .unwrap();
        let err = log
            .append_fenced(0, sample_record("t"), b"b".to_vec(), Some(2))
            .unwrap_err();
        assert!(matches!(err, LogError::StaleFence { ours: 2, stored: 3 }));
    }

    #[test]
    fn mid_file_checksum_does_not_skip_later_frames() {
        let dir = tempdir().unwrap();
        let mut log = PartitionLog::open(dir.path(), PartitionLogConfig::default()).unwrap();
        log.append(0, sample_record("t"), b"one".to_vec()).unwrap();
        log.append(0, sample_record("t"), b"two".to_vec()).unwrap();
        log.sync().unwrap();
        drop(log);

        let wal = dir.path().join("active.wal");
        let mut bytes = std::fs::read(&wal).unwrap();
        // Flip a byte in the first frame body so the enclosing epoch CRC fails
        // while a second record remains.
        if bytes.len() > EPOCH_HEADER_BYTES + 16 {
            bytes[EPOCH_HEADER_BYTES + 16] ^= 0xff;
        }
        std::fs::write(&wal, &bytes).unwrap();

        match PartitionLog::open(dir.path(), PartitionLogConfig::default()) {
            Err(LogError::Epoch(EpochError::ChecksumMismatch)) => {}
            Err(e) => panic!("expected epoch checksum failure, got {e}"),
            Ok(_) => panic!("expected epoch checksum failure, log opened"),
        }
    }

    fn group_cfg(interval: Duration) -> PartitionLogConfig {
        PartitionLogConfig {
            segment_max_bytes: 8 * 1024 * 1024,
            fsync: FsyncMode::Group,
            group_interval: interval,
        }
    }

    #[test]
    fn fsync_mode_parse() {
        assert_eq!(FsyncMode::parse("always"), Some(FsyncMode::Always));
        assert_eq!(FsyncMode::parse("GROUP"), Some(FsyncMode::Group));
        assert_eq!(FsyncMode::parse("os"), Some(FsyncMode::Os));
        assert_eq!(FsyncMode::parse("nope"), None);
    }

    #[test]
    fn group_commit_defers_meta_until_flush() {
        let dir = tempdir().unwrap();
        let mut log = PartitionLog::open(dir.path(), group_cfg(Duration::from_secs(60))).unwrap();
        let meta_path = dir.path().join("meta.json");
        let meta_after_open = std::fs::read(&meta_path).unwrap();
        for i in 0..50u8 {
            log.append(0, sample_record("t"), vec![i]).unwrap();
        }
        let meta_after_appends = std::fs::read(&meta_path).unwrap();
        assert_eq!(
            meta_after_open, meta_after_appends,
            "group commit must not rewrite meta.json per record"
        );
        assert_eq!(log.committed_hwm(), 0);
        log.sync().unwrap();
        assert_eq!(log.committed_hwm(), 50);
        assert_eq!(log.high_watermark(), 50);
        drop(log);
        let log = PartitionLog::open(dir.path(), group_cfg(Duration::from_secs(60))).unwrap();
        assert_eq!(log.high_watermark(), 50);
        assert_eq!(log.read_range(0, 0, 100).unwrap().len(), 50);
    }

    #[test]
    fn group_commit_unflushed_tail_may_disappear() {
        let dir = tempdir().unwrap();
        let mut log = PartitionLog::open(dir.path(), group_cfg(Duration::from_secs(60))).unwrap();
        log.append(0, sample_record("t"), b"lost".to_vec()).unwrap();
        std::mem::forget(log);
        let recovered = PartitionLog::open(dir.path(), group_cfg(Duration::from_secs(60))).unwrap();
        assert!(recovered.high_watermark() <= 1);
    }

    #[test]
    fn always_mode_persists_meta_per_record() {
        let dir = tempdir().unwrap();
        let cfg = PartitionLogConfig {
            segment_max_bytes: 8 * 1024 * 1024,
            fsync: FsyncMode::Always,
            group_interval: Duration::from_millis(10),
        };
        let mut log = PartitionLog::open(dir.path(), cfg).unwrap();
        log.append(0, sample_record("t"), b"one".to_vec()).unwrap();
        let meta = LogMeta::load(dir.path()).unwrap();
        assert_eq!(meta.next_offset, 1);
    }

    #[test]
    fn v2_batch_uses_epoch_frame_and_recovers() {
        let dir = tempdir().unwrap();
        {
            let mut log = PartitionLog::open_for_shard(
                dir.path(),
                group_cfg(Duration::from_secs(60)),
                9,
                WAL_FORMAT_V2,
            )
            .unwrap();
            log.append_batch(
                9,
                vec![
                    (sample_record("a"), b"one".to_vec()),
                    (sample_record("b"), b"two".to_vec()),
                ],
                Some(4),
            )
            .unwrap();
            log.sync().unwrap();
        }
        let bytes = std::fs::read(dir.path().join(WAL_FILE)).unwrap();
        assert_eq!(&bytes[..4], &broker_proto::EPOCH_MAGIC);

        let log = PartitionLog::open_for_shard(
            dir.path(),
            group_cfg(Duration::from_secs(60)),
            9,
            WAL_FORMAT_V2,
        )
        .unwrap();
        assert_eq!(log.high_watermark(), 2);
        assert_eq!(log.read_range(9, 0, 10).unwrap().len(), 2);
    }

    #[test]
    fn pre_manifest_v1_remains_readable() {
        let dir = tempdir().unwrap();
        {
            let mut log = PartitionLog::open_for_shard(
                dir.path(),
                PartitionLogConfig::default(),
                2,
                WAL_FORMAT_V1,
            )
            .unwrap();
            log.append(2, sample_record("legacy"), b"v1".to_vec())
                .unwrap();
            log.sync().unwrap();
        }
        std::fs::remove_file(WalManifest::path(dir.path())).unwrap();

        let log = PartitionLog::open_for_shard(
            dir.path(),
            PartitionLogConfig::default(),
            2,
            WAL_FORMAT_V2,
        )
        .unwrap();
        assert_eq!(log.manifest.wal_format_version, WAL_FORMAT_V1);
        assert_eq!(log.read_range(2, 0, 1).unwrap()[0].payload, b"v1");
    }

    #[test]
    fn recovery_truncates_incomplete_v2_epoch_tail() {
        let dir = tempdir().unwrap();
        {
            let mut log = PartitionLog::open_for_shard(
                dir.path(),
                PartitionLogConfig::default(),
                3,
                WAL_FORMAT_V2,
            )
            .unwrap();
            log.append(3, sample_record("t"), b"committed".to_vec())
                .unwrap();
            log.sync().unwrap();
        }
        let wal = dir.path().join(WAL_FILE);
        let valid_len = std::fs::metadata(&wal).unwrap().len();
        use std::io::Write;
        OpenOptions::new()
            .append(true)
            .open(&wal)
            .unwrap()
            .write_all(&broker_proto::EPOCH_MAGIC)
            .unwrap();

        let log = PartitionLog::open_for_shard(
            dir.path(),
            PartitionLogConfig::default(),
            3,
            WAL_FORMAT_V2,
        )
        .unwrap();
        assert_eq!(log.high_watermark(), 1);
        assert_eq!(std::fs::metadata(&wal).unwrap().len(), valid_len);
    }

    #[test]
    fn injected_fsync_failure_does_not_advance_hwm() {
        let dir = tempdir().unwrap();
        let mut log = PartitionLog::open(dir.path(), group_cfg(Duration::from_secs(60))).unwrap();
        log.append(0, sample_record("t"), b"value".to_vec())
            .unwrap();
        log.inject_fsync_failure();
        assert!(log.sync().unwrap_err().to_string().contains("injected"));
        assert_eq!(log.committed_hwm(), 0);
        assert!(log.is_dirty());
        log.sync().unwrap();
        assert_eq!(log.committed_hwm(), 1);
    }
}
