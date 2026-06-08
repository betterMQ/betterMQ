//! In-process broker coordinating partition logs and RocksDB metadata.

use crate::flow::FlowSpec;
use crate::flows::FlowProfileRegistry;
use crate::groups::GroupRegistry;
use crate::http_delivery::HttpDeliverySpec;
use crate::priority::normalize_priority;
use crate::subscriptions::{Subscription, SubscriptionRegistry};
use crate::topic::{group_topic, is_dlq_topic, partition_dir, partition_for, DIRECT_TOPIC};
use broker_payload::{hydrate_payload, prepare_for_log, BlobStore};
use broker_proto::{LogRecord, RetryBackoff, RetryDefaults};
#[cfg(feature = "slate")]
use broker_storage::{open_object_store_from_env, slate_db_path};
use broker_storage::{
    DedupEntry, MetadataStore, PartitionBackend, PartitionLogConfig, StorageMode, StoredMessage,
};
use chrono::Utc;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

pub const DEFAULT_TENANT: &str = "default";
pub const DEFAULT_PARTITIONS: u32 = 4;

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub data_dir: PathBuf,
    pub tenant_id: String,
    pub partitions: u32,
    pub log: PartitionLogConfig,
    pub storage: StorageMode,
    pub retry_defaults: RetryDefaults,
}

impl BrokerConfig {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            tenant_id: DEFAULT_TENANT.into(),
            partitions: DEFAULT_PARTITIONS,
            log: PartitionLogConfig::default(),
            storage: StorageMode::from_env(),
            retry_defaults: RetryDefaults::default(),
        }
    }
}

fn resolve_delivery_retry(
    req: &PublishRequest,
    queue: Option<&Subscription>,
    defaults: &RetryDefaults,
) -> (u32, RetryBackoff) {
    let max_retries = req
        .max_retries
        .or(queue.and_then(|q| q.default_max_retries))
        .unwrap_or(defaults.max_retries);
    let retry_backoff = req
        .retry_backoff
        .clone()
        .or_else(|| queue.and_then(|q| q.retry_backoff.clone()))
        .unwrap_or_else(|| defaults.backoff.clone());
    (max_retries, retry_backoff)
}

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("storage error: {0}")]
    Storage(#[from] broker_storage::LogError),
    #[error("index error: {0}")]
    Index(#[from] broker_storage::IndexError),
    #[error("subscription error: {0}")]
    Subscription(#[from] crate::subscriptions::SubscriptionError),
    #[error("queue not found: {0}")]
    QueueNotFound(String),
    #[error("flow profile not found: {0}")]
    FlowProfileNotFound(Uuid),
    #[error("flow profile error: {0}")]
    FlowProfile(#[from] crate::flows::FlowProfileError),
    #[error("group error: {0}")]
    Group(#[from] crate::groups::GroupError),
    #[error("payload error: {0}")]
    Payload(#[from] broker_payload::PayloadError),
    #[error("not shard leader for partition {0}")]
    NotShardLeader(u32),
}

type ShardLeaderFn = Arc<dyn Fn(u32) -> bool + Send + Sync>;

/// Frozen at schedule/enqueue time so later queue URL edits do not affect in-flight jobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DestinationSnapshot {
    pub queue_id: Option<Uuid>,
    pub url: String,
    pub secret: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishRequest {
    /// Queue name (omit when `queue_id` or direct `url` is set).
    #[serde(default)]
    pub topic: String,
    /// Resolve queue by id (preferred for `POST /v1/enqueue`).
    #[serde(default)]
    pub queue_id: Option<Uuid>,
    #[serde(default)]
    pub group_id: Option<Uuid>,
    #[serde(default)]
    pub group_member_id: Option<Uuid>,
    #[serde(default)]
    pub routing_key: String,
    #[serde(deserialize_with = "crate::payload::deserialize_flexible_payload")]
    pub payload: String,
    #[serde(default)]
    pub payload_encoding: Option<String>,
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub delay_ms: Option<u64>,
    #[serde(default)]
    pub priority: Option<u8>,
    /// Reference a flow profile created with `POST /v1/flows`.
    #[serde(default)]
    pub flow_id: Option<Uuid>,
    /// One-off publish (`POST /v1/publish`) — do not use with `queue` / `queue_id`.
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub secret: Option<String>,
    /// When set (delayed/cron fire), use this destination instead of re-reading the queue registry.
    #[serde(default)]
    pub destination: Option<DestinationSnapshot>,
    /// Legacy inline flow — prefer `flow_id`.
    #[serde(default)]
    pub flow: Option<FlowSpec>,
    #[serde(default)]
    pub parallelism: Option<u32>,
    /// Push retries after first failure (falls back to queue / broker defaults).
    #[serde(default)]
    pub max_retries: Option<u32>,
    #[serde(default)]
    pub retry_backoff: Option<RetryBackoff>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(
        default,
        deserialize_with = "crate::http_delivery::deserialize_optional_headers"
    )]
    pub headers: Option<std::collections::HashMap<String, String>>,
    #[serde(default)]
    pub sign: Option<bool>,
    #[serde(default)]
    pub request: Option<crate::http_delivery::HttpDeliveryInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<Uuid>,
    pub topic: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    pub duplicate: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduled: Option<ScheduledInfo>,
    /// Encoded log frame for CP7a replication (not serialized to API clients).
    #[serde(skip)]
    pub replication_frame: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledInfo {
    pub schedule_id: Uuid,
    pub deliver_at_ms: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateSubscriptionRequest {
    pub topic: String,
    pub url: String,
    pub secret: String,
    #[serde(default)]
    pub default_max_retries: Option<u32>,
    #[serde(default)]
    pub retry_backoff: Option<RetryBackoff>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CreateSubscriptionResponse {
    pub id: Uuid,
    pub topic: String,
    pub url: String,
}

struct TopicState {
    logs: HashMap<u32, PartitionBackend>,
}

#[cfg(feature = "slate")]
struct SlateEnv {
    object_store: Arc<dyn object_store::ObjectStore>,
    cache_root: PathBuf,
}

#[derive(Clone)]
pub struct Broker {
    inner: Arc<BrokerInner>,
}

struct BrokerInner {
    config: BrokerConfig,
    metadata: MetadataStore,
    subscriptions: SubscriptionRegistry,
    flows: FlowProfileRegistry,
    groups: GroupRegistry,
    blob_store: BlobStore,
    topics: Mutex<HashMap<String, TopicState>>,
    shard_leader_check: Mutex<Option<ShardLeaderFn>>,
    #[cfg(feature = "slate")]
    slate: Option<SlateEnv>,
}

impl Broker {
    pub fn open(config: BrokerConfig) -> Result<Self, BrokerError> {
        let rocks_path = config.data_dir.join("rocksdb");
        let metadata = MetadataStore::open(rocks_path)?;
        let subscriptions = SubscriptionRegistry::open(&config.data_dir)?;
        subscriptions.compact_duplicates()?;
        let flows = FlowProfileRegistry::open(&config.data_dir)?;
        let groups = GroupRegistry::open(&config.data_dir)?;
        let blob_store = crate::blob::open_blob_store(&config.data_dir, config.storage)?;

        #[cfg(feature = "slate")]
        let slate = if config.storage == StorageMode::Slate {
            let object_store = open_object_store_from_env().map_err(|e| {
                BrokerError::Storage(broker_storage::LogError::Slate(e.to_string()))
            })?;
            let cache_root = config.data_dir.join("slate-cache");
            std::fs::create_dir_all(&cache_root)
                .map_err(|e| BrokerError::Storage(broker_storage::LogError::Io(e)))?;
            tracing::info!(
                cache = %cache_root.display(),
                "SlateDB storage enabled (messages on S3/MinIO, indexes in RocksDB)"
            );
            Some(SlateEnv {
                object_store,
                cache_root,
            })
        } else {
            None
        };

        #[cfg(not(feature = "slate"))]
        if config.storage == StorageMode::Slate {
            return Err(BrokerError::Storage(broker_storage::LogError::Slate(
                "rebuild bettermq with --features slate (enabled in stack docker image)".into(),
            )));
        }

        Ok(Self {
            inner: Arc::new(BrokerInner {
                config,
                metadata,
                subscriptions,
                flows,
                groups,
                blob_store,
                topics: Mutex::new(HashMap::new()),
                shard_leader_check: Mutex::new(None),
                #[cfg(feature = "slate")]
                slate,
            }),
        })
    }

    /// Slate + cluster: only the elected shard leader may append to shared object storage.
    pub fn set_shard_leader_check(&self, check: ShardLeaderFn) {
        *self.inner.shard_leader_check.lock() = Some(check);
    }

    fn require_shard_leader(&self, partition: u32) -> Result<(), BrokerError> {
        if self.inner.config.storage != StorageMode::Slate {
            return Ok(());
        }
        if let Some(check) = self.inner.shard_leader_check.lock().as_ref() {
            if !check(partition) {
                return Err(BrokerError::NotShardLeader(partition));
            }
        }
        Ok(())
    }

    /// Drop in-memory Slate handles when this node loses shard leadership (CP6b.1).
    pub fn evict_slate_partitions(&self, partitions: &[u32]) {
        if self.inner.config.storage == StorageMode::Slate && !partitions.is_empty() {
            #[cfg(feature = "slate")]
            {
                let mut topics = self.inner.topics.lock();
                for topic_state in topics.values_mut() {
                    for &p in partitions {
                        topic_state.logs.remove(&p);
                    }
                }
            }
        }
    }

    pub fn create_flow_profile(
        &self,
        key: String,
        parallelism: u32,
        rate: u32,
        period_secs: u64,
    ) -> Result<crate::flows::FlowProfile, BrokerError> {
        Ok(self.inner.flows.create(
            &self.inner.config.tenant_id,
            key,
            parallelism,
            rate,
            period_secs,
        )?)
    }

    pub fn upsert_flow_profile_by_key(
        &self,
        key: String,
        parallelism: u32,
        rate: u32,
        period_secs: u64,
    ) -> Result<crate::flows::FlowProfile, BrokerError> {
        Ok(self.inner.flows.upsert_by_key(
            &self.inner.config.tenant_id,
            key,
            parallelism,
            rate,
            period_secs,
        )?)
    }

    pub fn get_flow_profile_by_key(
        &self,
        key: &str,
    ) -> Result<Option<crate::flows::FlowProfile>, BrokerError> {
        Ok(self
            .inner
            .flows
            .get_by_key(&self.inner.config.tenant_id, key)?)
    }

    pub fn list_flow_profiles(&self) -> Result<Vec<crate::flows::FlowProfile>, BrokerError> {
        Ok(self.inner.flows.list(&self.inner.config.tenant_id)?)
    }

    pub fn list_queues(&self) -> Result<Vec<Subscription>, BrokerError> {
        Ok(self
            .inner
            .subscriptions
            .list_all(&self.inner.config.tenant_id)?)
    }

    pub fn delete_flow_profile(&self, id: Uuid) -> Result<crate::flows::FlowProfile, BrokerError> {
        Ok(self.inner.flows.delete(&self.inner.config.tenant_id, id)?)
    }

    pub fn get_flow_profile(
        &self,
        id: Uuid,
    ) -> Result<Option<crate::flows::FlowProfile>, BrokerError> {
        Ok(self
            .inner
            .flows
            .get_by_id(&self.inner.config.tenant_id, id)?)
    }

    pub fn create_subscription(
        &self,
        req: CreateSubscriptionRequest,
    ) -> Result<CreateSubscriptionResponse, BrokerError> {
        let tenant_id = &self.inner.config.tenant_id;
        let sub = self.inner.subscriptions.create(
            tenant_id,
            req.topic.clone(),
            req.url.clone(),
            req.secret,
            req.default_max_retries,
            req.retry_backoff,
        )?;
        Ok(CreateSubscriptionResponse {
            id: sub.id,
            topic: sub.topic,
            url: sub.url,
        })
    }

    pub fn subscriptions_for_topic(&self, topic: &str) -> Result<Vec<Subscription>, BrokerError> {
        let tenant_id = &self.inner.config.tenant_id;
        Ok(self
            .inner
            .subscriptions
            .unique_for_topic(tenant_id, topic)?)
    }

    pub fn list_endpoints(&self) -> Result<Vec<Subscription>, BrokerError> {
        let tenant_id = &self.inner.config.tenant_id;
        Ok(self.inner.subscriptions.list_all(tenant_id)?)
    }

    pub fn delete_endpoint(&self, id: Uuid) -> Result<Subscription, BrokerError> {
        let tenant_id = &self.inner.config.tenant_id;
        Ok(self.inner.subscriptions.delete(tenant_id, id)?)
    }

    pub fn get_queue(&self, queue: &str) -> Result<Option<Subscription>, BrokerError> {
        let tenant_id = &self.inner.config.tenant_id;
        Ok(self.inner.subscriptions.get_by_name(tenant_id, queue)?)
    }

    pub fn get_queue_by_id(&self, id: Uuid) -> Result<Option<Subscription>, BrokerError> {
        let tenant_id = &self.inner.config.tenant_id;
        Ok(self.inner.subscriptions.get_by_id(tenant_id, id)?)
    }

    pub fn partition_high_watermark(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<u64, BrokerError> {
        self.with_partition_log(topic, partition, |log| Ok(log.high_watermark()))
    }

    pub fn read_message(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<StoredMessage, BrokerError> {
        let batch =
            self.with_partition_log(topic, partition, |log| log.read_range(partition, offset, 1))?;
        batch.into_iter().next().ok_or(BrokerError::Storage(
            broker_storage::LogError::OffsetNotFound(offset),
        ))
    }

    pub fn dispatch_offset(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
    ) -> Result<u64, BrokerError> {
        Ok(self
            .inner
            .metadata
            .dispatch_offset(tenant_id, subscription_id, partition)?)
    }

    pub fn set_dispatch_offset(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        next_offset: u64,
    ) -> Result<(), BrokerError> {
        self.inner.metadata.set_dispatch_offset(
            tenant_id,
            subscription_id,
            partition,
            next_offset,
        )?;
        Ok(())
    }

    pub fn config(&self) -> &BrokerConfig {
        &self.inner.config
    }

    /// Load external blob bytes into `msg.payload` when `payload_ref_json` is set.
    pub fn hydrate_message_payload(&self, msg: &mut StoredMessage) -> Result<(), BrokerError> {
        hydrate_payload(
            &self.inner.blob_store,
            &mut msg.payload,
            msg.payload_ref_json.as_deref(),
        )?;
        Ok(())
    }

    pub fn publish(&self, mut req: PublishRequest) -> Result<PublishResponse, BrokerError> {
        let tenant_id = &self.inner.config.tenant_id;
        let payload = decode_payload(&req)?;
        let message_id = Uuid::new_v4();
        let (log_payload, payload_ref_json) =
            prepare_for_log(&self.inner.blob_store, tenant_id, message_id, payload)?;

        if let Some(ref key) = req.idempotency_key {
            if let Some(entry) = self.inner.metadata.get_dedup(tenant_id, key)? {
                return Ok(PublishResponse {
                    message_id: Some(entry.message_id),
                    topic: req.topic.clone(),
                    partition: Some(entry.partition),
                    offset: Some(entry.offset),
                    duplicate: true,
                    scheduled: None,
                    replication_frame: None,
                });
            }
        }

        let _ = req.delay_ms.take();

        let flow_spec = self.resolve_flow_spec(&req)?;

        let mut queue_sub: Option<Subscription> = None;
        let (topic, queue_id, destination_url, destination_secret) = if is_dlq_topic(&req.topic) {
            (req.topic.clone(), None, None, None)
        } else if let Some(dest) = req.destination.take() {
            let topic = dest
                .queue_id
                .and_then(|id| self.get_queue_by_id(id).ok().flatten().map(|q| q.topic))
                .unwrap_or_else(|| DIRECT_TOPIC.to_string());
            if let Some(id) = dest.queue_id {
                queue_sub = self.get_queue_by_id(id)?;
            }
            (topic, dest.queue_id, Some(dest.url), Some(dest.secret))
        } else if let (Some(url), Some(secret)) = (req.url.take(), req.secret.take()) {
            (DIRECT_TOPIC.to_string(), None, Some(url), Some(secret))
        } else {
            let queue = if let Some(id) = req.queue_id {
                self.get_queue_by_id(id)?
                    .ok_or(BrokerError::QueueNotFound(id.to_string()))?
            } else {
                self.inner
                    .subscriptions
                    .get_by_name(tenant_id, &req.topic)?
                    .ok_or_else(|| BrokerError::QueueNotFound(req.topic.clone()))?
            };
            queue_sub = Some(queue.clone());
            (
                queue.topic.clone(),
                Some(queue.id),
                Some(queue.url),
                Some(queue.secret),
            )
        };

        req.topic = topic.clone();
        let (max_retries, retry_backoff) =
            resolve_delivery_retry(&req, queue_sub.as_ref(), &self.inner.config.retry_defaults);

        let partition = partition_for(
            tenant_id,
            &req.topic,
            &req.routing_key,
            self.inner.config.partitions,
        );
        self.require_shard_leader(partition)?;

        let http = HttpDeliverySpec::merge(
            req.method.clone(),
            req.headers.clone(),
            req.sign,
            req.request
                .clone()
                .map(|r| HttpDeliverySpec::merge(r.method, r.headers, r.sign, None)),
        );

        let (stored, frame) = self.with_partition_log(&req.topic, partition, |log| {
            let header = LogRecord {
                id: message_id,
                tenant_id: tenant_id.clone(),
                topic: req.topic.clone(),
                routing_key: req.routing_key.clone(),
                idempotency_key: req.idempotency_key.clone(),
                published_at_ms: Utc::now().timestamp_millis(),
                priority: normalize_priority(req.priority),
                flow_parallelism: flow_spec.as_ref().and_then(|f| f.parallelism),
                flow_key: flow_spec.as_ref().and_then(|f| f.key.clone()),
                flow_rate: flow_spec.as_ref().and_then(|f| f.rate),
                flow_period_secs: flow_spec.as_ref().and_then(|f| f.period_secs),
                queue_id,
                group_id: req.group_id,
                group_member_id: req.group_member_id,
                flow_profile_id: req.flow_id,
                destination_url,
                destination_secret,
                max_retries,
                retry_backoff: Some(retry_backoff),
                http_method: Some(http.method.clone()),
                http_headers_json: http.headers_json(),
                http_sign: Some(http.sign),
                payload_ref_json,
            };
            log.append(partition, header, log_payload)
        })?;

        if let Some(ref key) = req.idempotency_key {
            self.inner.metadata.put_dedup(
                tenant_id,
                key,
                &DedupEntry {
                    message_id: stored.id,
                    offset: stored.offset,
                    partition,
                },
            )?;
        }

        Ok(PublishResponse {
            message_id: Some(stored.id),
            topic: req.topic,
            partition: Some(partition),
            offset: Some(stored.offset),
            duplicate: false,
            scheduled: None,
            replication_frame: Some(frame),
        })
    }

    /// Apply a replicated log frame on a follower (CP7a). Disabled in slate mode (CP6b.1).
    pub fn append_replicated_frame(
        &self,
        topic: &str,
        partition: u32,
        frame: &[u8],
    ) -> Result<PublishResponse, BrokerError> {
        if self.inner.config.storage == StorageMode::Slate {
            return Err(BrokerError::Storage(broker_storage::LogError::Slate(
                "slate mode: frame replication disabled; durability is on shared object storage"
                    .into(),
            )));
        }
        let stored = self.with_partition_log(topic, partition, |log| {
            log.append_raw_frame(partition, frame)
        })?;
        Ok(PublishResponse {
            message_id: Some(stored.id),
            topic: topic.to_string(),
            partition: Some(partition),
            offset: Some(stored.offset),
            duplicate: false,
            scheduled: None,
            replication_frame: None,
        })
    }

    /// Publish without `delay_ms` (used by schedule worker and DLQ).
    pub fn publish_immediate(
        &self,
        mut req: PublishRequest,
    ) -> Result<PublishResponse, BrokerError> {
        req.delay_ms = None;
        self.publish(req)
    }

    /// DLQ partition directories present on disk (`jobs.__dlq`, `__direct.__dlq`, …).
    pub fn list_dlq_topics_on_disk(&self) -> Result<Vec<String>, BrokerError> {
        let tenant_dir = self
            .inner
            .config
            .data_dir
            .join("partitions")
            .join(&self.inner.config.tenant_id);
        let mut topics = Vec::new();
        let Ok(entries) = std::fs::read_dir(&tenant_dir) else {
            return Ok(topics);
        };
        for entry in entries.flatten() {
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                let name = entry.file_name().to_string_lossy().into_owned();
                if is_dlq_topic(&name) {
                    topics.push(name);
                }
            }
        }
        topics.sort();
        Ok(topics)
    }

    /// Scan a topic from offset 0 (used for DLQ inspection; push-only broker).
    pub fn list_topic_messages(
        &self,
        topic: &str,
        max_messages: usize,
    ) -> Result<Vec<StoredMessage>, BrokerError> {
        let mut messages = Vec::new();
        for partition in 0..self.inner.config.partitions {
            let remaining = max_messages.saturating_sub(messages.len());
            if remaining == 0 {
                break;
            }
            let batch = self.with_partition_log(topic, partition, |log| {
                log.read_range(partition, 0, remaining)
            })?;
            messages.extend(batch);
        }
        messages.sort_by_key(|m| (m.partition, m.offset));
        messages.truncate(max_messages);
        Ok(messages)
    }

    /// Drop a primary-queue record from the partition log (not used for `*. __dlq` topics).
    pub fn purge_message(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<bool, BrokerError> {
        if is_dlq_topic(topic) {
            return Ok(false);
        }
        let removed =
            self.with_partition_log(topic, partition, |log| Ok(log.purge_offset(offset)))?;
        Ok(removed)
    }

    /// Remove a dead-letter record (operator cleanup from the panel / API).
    pub fn purge_dlq_message(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<bool, BrokerError> {
        if !is_dlq_topic(topic) {
            return Ok(false);
        }
        let removed =
            self.with_partition_log(topic, partition, |log| Ok(log.purge_offset(offset)))?;
        Ok(removed)
    }

    /// Purge after successful push delivery once the dispatch cursor has advanced.
    pub fn try_purge_message(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<bool, BrokerError> {
        if is_dlq_topic(topic) {
            return Ok(false);
        }

        let msg = self.read_message(topic, partition, offset).ok();
        let tenant_id = &self.inner.config.tenant_id;

        if let Some(ref m) = msg {
            if let Some(url) = &m.destination_url {
                if !url.is_empty() {
                    if let Some(owner) = m.queue_id.or(m.flow_profile_id) {
                        let cursor =
                            self.dispatch_offset(tenant_id, &owner.to_string(), partition)?;
                        if cursor <= offset {
                            return Ok(false);
                        }
                    }
                }
            }
        }

        self.purge_message(topic, partition, offset)
    }

    /// Freeze flow limits and queue destination from this broker's catalog before forwarding to a shard leader.
    pub fn prepare_for_cluster_forward(&self, req: &mut PublishRequest) -> Result<(), BrokerError> {
        let tenant_id = &self.inner.config.tenant_id;
        if req.flow.is_none() {
            if let Some(id) = req.flow_id {
                if let Some(profile) = self.inner.flows.get_by_id(tenant_id, id)? {
                    let mut spec = profile.to_spec();
                    if spec.key.is_none() && !req.routing_key.is_empty() {
                        spec.key = Some(req.routing_key.clone());
                    }
                    req.flow = Some(spec);
                }
            }
        }
        if req.destination.is_none() && req.url.is_none() {
            let queue = if let Some(id) = req.queue_id {
                self.inner.subscriptions.get_by_id(tenant_id, id)?
            } else if !req.topic.is_empty() && req.topic != DIRECT_TOPIC {
                self.inner
                    .subscriptions
                    .get_by_name(tenant_id, &req.topic)?
            } else {
                None
            };
            if let Some(q) = queue {
                req.destination = Some(DestinationSnapshot {
                    queue_id: Some(q.id),
                    url: q.url,
                    secret: q.secret,
                });
            }
        }
        Ok(())
    }

    pub fn upsert_flow_profile(
        &self,
        profile: crate::flows::FlowProfile,
    ) -> Result<(), BrokerError> {
        Ok(self.inner.flows.upsert(profile)?)
    }

    pub fn upsert_subscription_catalog(&self, sub: Subscription) -> Result<(), BrokerError> {
        Ok(self.inner.subscriptions.upsert(sub)?)
    }

    pub fn create_group(&self, name: String) -> Result<crate::groups::DispatchGroup, BrokerError> {
        Ok(self
            .inner
            .groups
            .create_group(&self.inner.config.tenant_id, name)?)
    }

    pub fn list_groups(&self) -> Result<Vec<crate::groups::DispatchGroup>, BrokerError> {
        Ok(self
            .inner
            .groups
            .list_groups(&self.inner.config.tenant_id)?)
    }

    pub fn get_group(&self, id: Uuid) -> Result<Option<crate::groups::DispatchGroup>, BrokerError> {
        Ok(self
            .inner
            .groups
            .get_group(&self.inner.config.tenant_id, id)?)
    }

    pub fn delete_group(&self, id: Uuid) -> Result<crate::groups::DispatchGroup, BrokerError> {
        Ok(self
            .inner
            .groups
            .delete_group(&self.inner.config.tenant_id, id)?)
    }

    pub fn upsert_group_catalog(
        &self,
        group: crate::groups::DispatchGroup,
    ) -> Result<(), BrokerError> {
        Ok(self.inner.groups.upsert_group(group)?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_group_member(
        &self,
        group_id: Uuid,
        name: String,
        url: String,
        secret: String,
        parallelism: u32,
        rate: u32,
        period_secs: u64,
        flow_key: Option<String>,
    ) -> Result<crate::groups::GroupMember, BrokerError> {
        Ok(self.inner.groups.add_member(
            &self.inner.config.tenant_id,
            group_id,
            name,
            url,
            secret,
            parallelism,
            rate,
            period_secs,
            flow_key,
        )?)
    }

    pub fn list_group_members(
        &self,
        group_id: Uuid,
    ) -> Result<Vec<crate::groups::GroupMember>, BrokerError> {
        Ok(self
            .inner
            .groups
            .list_members(&self.inner.config.tenant_id, group_id)?)
    }

    pub fn get_group_member(
        &self,
        id: Uuid,
    ) -> Result<Option<crate::groups::GroupMember>, BrokerError> {
        Ok(self
            .inner
            .groups
            .get_member(&self.inner.config.tenant_id, id)?)
    }

    pub fn delete_group_member(&self, id: Uuid) -> Result<crate::groups::GroupMember, BrokerError> {
        Ok(self
            .inner
            .groups
            .delete_member(&self.inner.config.tenant_id, id)?)
    }

    pub fn upsert_group_member_catalog(
        &self,
        member: crate::groups::GroupMember,
    ) -> Result<(), BrokerError> {
        Ok(self.inner.groups.upsert_member(member)?)
    }

    pub fn active_group_members(
        &self,
        group_id: Uuid,
    ) -> Result<Vec<crate::groups::GroupMember>, BrokerError> {
        Ok(self
            .inner
            .groups
            .active_members(&self.inner.config.tenant_id, group_id)?)
    }

    /// Build a publish request for one group member (caller runs `publish`).
    pub fn group_member_publish_request(
        &self,
        group_id: Uuid,
        member: &crate::groups::GroupMember,
        base: &PublishRequest,
    ) -> PublishRequest {
        let routing_key = if base.routing_key.is_empty() {
            member.id.to_string()
        } else {
            format!("{}:{}", base.routing_key, member.id)
        };
        let flow_key = member.flow_key.clone().or_else(|| {
            if base.routing_key.is_empty() {
                None
            } else {
                Some(base.routing_key.clone())
            }
        });
        let idempotency_key = base
            .idempotency_key
            .as_ref()
            .map(|k| format!("{k}:{}", member.id));
        PublishRequest {
            topic: group_topic(group_id),
            queue_id: None,
            group_id: Some(group_id),
            group_member_id: Some(member.id),
            routing_key,
            payload: base.payload.clone(),
            payload_encoding: base.payload_encoding.clone(),
            idempotency_key,
            delay_ms: None,
            priority: base.priority,
            flow_id: None,
            url: None,
            secret: None,
            destination: Some(DestinationSnapshot {
                queue_id: None,
                url: member.url.clone(),
                secret: member.secret.clone(),
            }),
            flow: Some(FlowSpec {
                key: flow_key,
                parallelism: Some(member.parallelism),
                rate: Some(member.rate),
                period_secs: Some(member.period_secs),
            }),
            parallelism: None,
            max_retries: base.max_retries,
            retry_backoff: base.retry_backoff.clone(),
            method: base.method.clone(),
            headers: base.headers.clone(),
            sign: base.sign,
            request: base.request.clone(),
        }
    }

    fn resolve_flow_spec(&self, req: &PublishRequest) -> Result<Option<FlowSpec>, BrokerError> {
        if let Some(ref flow) = req.flow {
            return Ok(Some(flow.clone()));
        }
        let tenant_id = &self.inner.config.tenant_id;
        if let Some(id) = req.flow_id {
            let profile = self
                .inner
                .flows
                .get_by_id(tenant_id, id)?
                .ok_or(BrokerError::FlowProfileNotFound(id))?;
            let mut spec = profile.to_spec();
            if spec.key.is_none() && !req.routing_key.is_empty() {
                spec.key = Some(req.routing_key.clone());
            }
            return Ok(Some(spec));
        }
        Ok(req.flow.clone())
    }

    fn ensure_topic(&self, topic: &str) -> Result<(), BrokerError> {
        let mut topics = self.inner.topics.lock();
        if topics.contains_key(topic) {
            return Ok(());
        }

        let tenant_id = &self.inner.config.tenant_id;
        let mut logs = HashMap::new();
        for p in 0..self.inner.config.partitions {
            let backend = match self.inner.config.storage {
                StorageMode::Local => {
                    let pdir = partition_dir(&self.inner.config.data_dir, tenant_id, topic, p);
                    PartitionBackend::open_local(pdir, self.inner.config.log.clone())?
                }
                StorageMode::Slate => {
                    #[cfg(feature = "slate")]
                    {
                        let slate = self.inner.slate.as_ref().expect("slate env");
                        let db_path = slate_db_path(tenant_id, topic, p);
                        let local_cache = slate
                            .cache_root
                            .join("local")
                            .join(tenant_id)
                            .join(topic)
                            .join(format!("p{p}"));
                        PartitionBackend::open_slate(
                            db_path,
                            local_cache,
                            slate.object_store.clone(),
                            p,
                            self.inner.config.log.clone(),
                        )?
                    }
                    #[cfg(not(feature = "slate"))]
                    {
                        let _ = p;
                        return Err(BrokerError::Storage(broker_storage::LogError::Slate(
                            "slate feature not compiled".into(),
                        )));
                    }
                }
            };
            logs.insert(p, backend);
        }
        topics.insert(topic.to_string(), TopicState { logs });
        Ok(())
    }

    fn with_partition_log<T>(
        &self,
        topic: &str,
        partition: u32,
        f: impl FnOnce(&mut PartitionBackend) -> Result<T, broker_storage::LogError>,
    ) -> Result<T, BrokerError> {
        self.ensure_topic(topic)?;
        let mut topics = self.inner.topics.lock();
        let state = topics.get_mut(topic).expect("topic initialized");
        let log = state.logs.get_mut(&partition).expect("partition exists");
        Ok(f(log)?)
    }
}

fn decode_payload(req: &PublishRequest) -> Result<Vec<u8>, BrokerError> {
    match req.payload_encoding.as_deref() {
        Some("base64") => {
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &req.payload)
                .map_err(|e| {
                    BrokerError::Storage(broker_storage::LogError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        e,
                    )))
                })
        }
        _ => Ok(req.payload.as_bytes().to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn publish_stores_readable_message() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();

        broker
            .create_subscription(CreateSubscriptionRequest {
                topic: "orders".into(),
                url: "http://127.0.0.1:9/hook".into(),
                secret: "sec".into(),
                default_max_retries: None,
                retry_backoff: None,
            })
            .unwrap();

        let published = broker
            .publish(PublishRequest {
                topic: "orders".into(),
                queue_id: None,
                group_id: None,
                group_member_id: None,
                routing_key: "a".into(),
                payload: "hello".into(),
                payload_encoding: None,
                idempotency_key: None,
                delay_ms: None,
                priority: None,
                flow_id: None,
                url: None,
                secret: None,
                destination: None,
                flow: None,
                parallelism: None,
                max_retries: None,
                retry_backoff: None,
                method: None,
                headers: None,
                sign: None,
                request: None,
            })
            .unwrap();
        assert!(!published.duplicate);
        let msg = broker
            .read_message(
                "orders",
                published.partition.unwrap(),
                published.offset.unwrap(),
            )
            .unwrap();
        assert_eq!(msg.payload, b"hello");
    }

    #[test]
    fn large_payload_stored_as_blob() {
        std::env::set_var("BETTERMQ_INLINE_MAX_BYTES", "1024");
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        broker
            .create_subscription(CreateSubscriptionRequest {
                topic: "big".into(),
                url: "http://127.0.0.1:9/h".into(),
                secret: "s".into(),
                default_max_retries: None,
                retry_backoff: None,
            })
            .unwrap();

        let body = "x".repeat(2048);
        let published = broker
            .publish(PublishRequest {
                topic: "big".into(),
                queue_id: None,
                group_id: None,
                group_member_id: None,
                routing_key: "rk".into(),
                payload: body.clone(),
                payload_encoding: None,
                idempotency_key: None,
                delay_ms: None,
                priority: None,
                flow_id: None,
                url: None,
                secret: None,
                destination: None,
                flow: None,
                parallelism: None,
                max_retries: None,
                retry_backoff: None,
                method: None,
                headers: None,
                sign: None,
                request: None,
            })
            .unwrap();

        let mut msg = broker
            .read_message(
                "big",
                published.partition.unwrap(),
                published.offset.unwrap(),
            )
            .unwrap();
        assert!(msg.payload.is_empty());
        assert!(msg.payload_ref_json.is_some());
        broker.hydrate_message_payload(&mut msg).unwrap();
        assert_eq!(msg.payload, body.as_bytes());
        std::env::remove_var("BETTERMQ_INLINE_MAX_BYTES");
    }

    #[test]
    fn idempotency_returns_same_offset() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        broker
            .create_subscription(CreateSubscriptionRequest {
                topic: "t".into(),
                url: "http://127.0.0.1:9/h".into(),
                secret: "s".into(),
                default_max_retries: None,
                retry_backoff: None,
            })
            .unwrap();

        let req = PublishRequest {
            topic: "t".into(),
            queue_id: None,
            group_id: None,
            group_member_id: None,
            routing_key: "".into(),
            payload: "x".into(),
            payload_encoding: None,
            idempotency_key: Some("idem-1".into()),
            delay_ms: None,
            priority: None,
            flow_id: None,
            url: None,
            secret: None,
            destination: None,
            flow: None,
            parallelism: None,
            max_retries: None,
            retry_backoff: None,
            method: None,
            headers: None,
            sign: None,
            request: None,
        };
        let a = broker.publish(req.clone()).unwrap();
        let b = broker.publish(req).unwrap();
        assert!(!a.duplicate);
        assert!(b.duplicate);
        assert_eq!(a.offset, b.offset);
        assert_eq!(a.message_id, b.message_id);
    }
}
