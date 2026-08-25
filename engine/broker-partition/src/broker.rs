//! In-process broker coordinating partition logs and RocksDB metadata.

use crate::flow::FlowSpec;
use crate::flows::FlowProfileRegistry;
use crate::groups::GroupRegistry;
use crate::http_delivery::HttpDeliverySpec;
use crate::layout::ShardLayout;
use crate::priority::normalize_priority;
use crate::shard::ShardHandle;
use crate::subscriptions::{Subscription, SubscriptionRegistry};
use crate::topic::{group_topic, is_dlq_topic, partition_dir, DIRECT_TOPIC};
use broker_payload::{hydrate_payload, prepare_for_log, BlobStore};
use broker_proto::{LogRecord, RetryBackoff, RetryDefaults};
#[cfg(feature = "slate")]
use broker_storage::{open_object_store_from_env, slate_db_path};
use broker_storage::{
    DedupEntry, MetadataStore, MetadataWriteBatch, PartitionBackend, PartitionLogConfig,
    StorageMode, StoredMessage,
};
use chrono::Utc;
use parking_lot::{Condvar, Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
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
            log: PartitionLogConfig::from_env(),
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
    #[error("invalid name: {0}")]
    InvalidName(#[from] broker_proto::PathSegmentError),
    #[error("invalid config: {0}")]
    InvalidConfig(String),
}

/// Returns `Some(generation)` when this node is leader for the shard (Slate fence).
type ShardFenceFn = Arc<dyn Fn(u32) -> Option<u64> + Send + Sync>;

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
    /// Commit epoch / durable high watermark after the producing shard fsyncs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_epoch: Option<u64>,
    /// Encoded log frame for CP7a replication (not serialized to API clients).
    #[serde(skip)]
    pub replication_frame: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledInfo {
    pub schedule_id: Uuid,
    pub deliver_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ReservationKey {
    tenant_id: String,
    idempotency_key: String,
}

#[derive(Default)]
struct DedupReservations {
    pending: Mutex<HashSet<ReservationKey>>,
    changed: Condvar,
}

impl DedupReservations {
    fn release(&self, key: &ReservationKey) {
        if self.pending.lock().remove(key) {
            self.changed.notify_all();
        }
    }
}

struct ReservationToken {
    reservations: Arc<DedupReservations>,
    key: ReservationKey,
    release_on_drop: bool,
}

impl ReservationToken {
    fn release(mut self) {
        self.reservations.release(&self.key);
        self.release_on_drop = false;
    }
}

impl Drop for ReservationToken {
    fn drop(&mut self) {
        if self.release_on_drop {
            self.reservations.release(&self.key);
        }
    }
}

enum ReservationOutcome {
    Existing(DedupEntry),
    Acquired(ReservationToken),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PreparedShardKey {
    partition: u32,
    storage_namespace: String,
    fence_generation: Option<u64>,
}

impl PreparedShardKey {
    pub fn partition(&self) -> u32 {
        self.partition
    }

    pub fn fence_generation(&self) -> Option<u64> {
        self.fence_generation
    }
}

struct PreparedRecord {
    topic: String,
    partition: u32,
    header: LogRecord,
    payload: Vec<u8>,
    fence_generation: Option<u64>,
    reservation: Option<ReservationToken>,
}

pub struct PreparedShardBatch {
    key: PreparedShardKey,
    records: Vec<PreparedRecord>,
}

impl PreparedShardBatch {
    pub fn key(&self) -> &PreparedShardKey {
        &self.key
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

enum PreparedSlot {
    Existing(PublishResponse),
    Awaiting {
        message_id: Uuid,
        duplicate: bool,
        topic_override: Option<String>,
    },
    Final(PublishResponse),
}

pub struct PreparedPublishBatch {
    slots: Vec<PreparedSlot>,
    shard_batches: Vec<PreparedShardBatch>,
}

impl PreparedPublishBatch {
    pub fn take_shard_batches(&mut self) -> Vec<PreparedShardBatch> {
        std::mem::take(&mut self.shard_batches)
    }

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    pub fn finish(self) -> Result<Vec<PublishResponse>, BrokerError> {
        self.slots
            .into_iter()
            .map(|slot| match slot {
                PreparedSlot::Existing(response) | PreparedSlot::Final(response) => Ok(response),
                PreparedSlot::Awaiting { .. } => Err(BrokerError::InvalidConfig(
                    "prepared batch has unfinalized shard records".into(),
                )),
            })
            .collect()
    }
}

struct FinalizeRecord {
    message_id: Uuid,
    topic: String,
    partition: u32,
    idempotency_key: Option<String>,
    reservation: Option<ReservationToken>,
}

pub struct AppendedShardBatch {
    appended: Vec<(StoredMessage, Vec<u8>)>,
    records: Vec<FinalizeRecord>,
}

pub struct AppendedRecordRef<'a> {
    pub message_id: Uuid,
    pub topic: &'a str,
    pub partition: u32,
    pub offset: u64,
    pub replication_frame: &'a [u8],
}

impl AppendedShardBatch {
    pub fn len(&self) -> usize {
        self.appended.len()
    }

    pub fn is_empty(&self) -> bool {
        self.appended.is_empty()
    }

    pub fn partition(&self) -> Option<u32> {
        self.appended.first().map(|(stored, _)| stored.partition)
    }

    pub fn last_offset(&self) -> Option<u64> {
        self.appended.last().map(|(stored, _)| stored.offset)
    }

    /// Borrow exact record frames for replication before finalization.
    pub fn records(&self) -> impl ExactSizeIterator<Item = AppendedRecordRef<'_>> {
        self.appended
            .iter()
            .map(|(stored, frame)| AppendedRecordRef {
                message_id: stored.id,
                topic: &stored.topic,
                partition: stored.partition,
                offset: stored.offset,
                replication_frame: frame,
            })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateSubscriptionRequest {
    pub topic: String,
    pub url: String,
    pub secret: String,
    #[serde(default)]
    pub parallelism: Option<u32>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallelism: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_max_retries: Option<u32>,
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
    layout: ShardLayout,
    shards: RwLock<HashMap<String, HashMap<u32, Arc<ShardHandle>>>>,
    dedup_reservations: Arc<DedupReservations>,
    shard_leader_check: Mutex<Option<ShardFenceFn>>,
    #[cfg(feature = "slate")]
    slate: Option<SlateEnv>,
}

impl Broker {
    /// Active tenant for this call (request-scoped in cloud, else config default).
    pub fn tenant(&self) -> String {
        crate::tenant_scope::effective_tenant(&self.inner.config.tenant_id).into_owned()
    }

    pub fn layout(&self) -> &ShardLayout {
        &self.inner.layout
    }

    pub fn shard_count(&self) -> u32 {
        self.inner.layout.shard_count
    }

    /// Physical storage path for V2. V1 remains topic-scoped and returns `None`.
    pub fn physical_shard_path(&self, shard_id: u32) -> Option<PathBuf> {
        (self.inner.layout.is_physical() && shard_id < self.inner.layout.shard_count).then(|| {
            self.inner
                .layout
                .physical_shard_dir(&self.inner.config.data_dir, shard_id)
        })
    }

    pub fn open(config: BrokerConfig) -> Result<Self, BrokerError> {
        if config.partitions == 0 {
            return Err(BrokerError::InvalidConfig(
                "partitions must be greater than 0".into(),
            ));
        }
        if config.log.fsync == broker_storage::FsyncMode::Os
            && std::env::var("BETTERMQ_SAAS")
                .map(|s| matches!(s.trim(), "1" | "true" | "TRUE" | "yes"))
                .unwrap_or(false)
        {
            return Err(BrokerError::InvalidConfig(
                "BETTERMQ_FSYNC=os is not allowed for SaaS (RPO would not be 0)".into(),
            ));
        }
        let layout = ShardLayout::load_or_create(&config.data_dir, config.partitions)
            .map_err(|e| BrokerError::InvalidConfig(format!("shard layout: {e}")))?;
        let mut config = config;
        config.partitions = layout.shard_count;
        let rocks_path = config.data_dir.join("rocksdb");

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
                "SlateDB storage enabled (messages + indexes on object store)"
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

        let metadata = {
            #[cfg(feature = "slate")]
            {
                if let Some(ref env) = slate {
                    MetadataStore::open_slate(Arc::clone(&env.object_store), &env.cache_root)?
                } else {
                    MetadataStore::open(rocks_path)?
                }
            }
            #[cfg(not(feature = "slate"))]
            {
                MetadataStore::open(rocks_path)?
            }
        };

        let subscriptions = SubscriptionRegistry::open(&config.data_dir)?;
        subscriptions.compact_duplicates()?;
        let flows = FlowProfileRegistry::open(&config.data_dir)?;
        let groups = GroupRegistry::open(&config.data_dir)?;
        let blob_store = crate::blob::open_blob_store(&config.data_dir, config.storage)?;

        Ok(Self {
            inner: Arc::new(BrokerInner {
                config,
                metadata,
                subscriptions,
                flows,
                groups,
                blob_store,
                layout,
                shards: RwLock::new(HashMap::new()),
                dedup_reservations: Arc::new(DedupReservations::default()),
                shard_leader_check: Mutex::new(None),
                #[cfg(feature = "slate")]
                slate,
            }),
        })
    }

    /// Slate + cluster: only the elected shard leader may append; generation is the fence token.
    pub fn set_shard_leader_check(&self, check: ShardFenceFn) {
        *self.inner.shard_leader_check.lock() = Some(check);
    }

    /// Returns fence generation when this node may append (cluster).
    fn require_shard_fence(&self, partition: u32) -> Result<Option<u64>, BrokerError> {
        if let Some(check) = self.inner.shard_leader_check.lock().as_ref() {
            match check(partition) {
                Some(gen) => Ok(Some(gen)),
                None => Err(BrokerError::NotShardLeader(partition)),
            }
        } else {
            Ok(None)
        }
    }

    /// Drop in-memory Slate handles when this node loses shard leadership (CP6b.1).
    pub fn evict_slate_partitions(&self, partitions: &[u32]) {
        if self.inner.config.storage == StorageMode::Slate && !partitions.is_empty() {
            #[cfg(feature = "slate")]
            {
                let mut shards = self.inner.shards.write();
                for topic_shards in shards.values_mut() {
                    for &p in partitions {
                        topic_shards.remove(&p);
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
        broker_proto::validate_flow_key(&key)?;
        Ok(self
            .inner
            .flows
            .create(&self.tenant(), key, parallelism, rate, period_secs)?)
    }

    pub fn upsert_flow_profile_by_key(
        &self,
        key: String,
        parallelism: u32,
        rate: u32,
        period_secs: u64,
    ) -> Result<crate::flows::FlowProfile, BrokerError> {
        broker_proto::validate_flow_key(&key)?;
        Ok(self
            .inner
            .flows
            .upsert_by_key(&self.tenant(), key, parallelism, rate, period_secs)?)
    }

    /// Reuse matching profile by key+limits, else create/update.
    pub fn ensure_flow_profile_by_key(
        &self,
        key: String,
        parallelism: u32,
        rate: u32,
        period_secs: u64,
    ) -> Result<crate::flows::FlowProfile, BrokerError> {
        broker_proto::validate_flow_key(&key)?;
        Ok(self
            .inner
            .flows
            .ensure_by_key(&self.tenant(), key, parallelism, rate, period_secs)?)
    }

    pub fn get_flow_profile_by_key(
        &self,
        key: &str,
    ) -> Result<Option<crate::flows::FlowProfile>, BrokerError> {
        Ok(self.inner.flows.get_by_key(&self.tenant(), key)?)
    }

    pub fn list_flow_profiles(&self) -> Result<Vec<crate::flows::FlowProfile>, BrokerError> {
        Ok(self.inner.flows.list(&self.tenant())?)
    }

    pub fn list_queues(&self) -> Result<Vec<Subscription>, BrokerError> {
        Ok(self.inner.subscriptions.list_all(&self.tenant())?)
    }

    pub fn delete_flow_profile(&self, id: Uuid) -> Result<crate::flows::FlowProfile, BrokerError> {
        Ok(self.inner.flows.delete(&self.tenant(), id)?)
    }

    pub fn get_flow_profile(
        &self,
        id: Uuid,
    ) -> Result<Option<crate::flows::FlowProfile>, BrokerError> {
        Ok(self.inner.flows.get_by_id(&self.tenant(), id)?)
    }

    pub fn create_subscription(
        &self,
        req: CreateSubscriptionRequest,
    ) -> Result<CreateSubscriptionResponse, BrokerError> {
        let tenant_id = self.tenant();
        broker_proto::validate_queue_name(&req.topic)?;
        let sub = self.inner.subscriptions.create(
            &tenant_id,
            req.topic.clone(),
            req.url.clone(),
            req.secret,
            req.parallelism,
            req.default_max_retries,
            req.retry_backoff,
        )?;
        Ok(CreateSubscriptionResponse {
            id: sub.id,
            topic: sub.topic,
            url: sub.url,
            parallelism: sub.parallelism,
            default_max_retries: sub.default_max_retries,
        })
    }

    pub fn subscriptions_for_topic(&self, topic: &str) -> Result<Vec<Subscription>, BrokerError> {
        let tenant_id = self.tenant();
        Ok(self
            .inner
            .subscriptions
            .unique_for_topic(&tenant_id, topic)?)
    }

    pub fn list_endpoints(&self) -> Result<Vec<Subscription>, BrokerError> {
        let tenant_id = self.tenant();
        Ok(self.inner.subscriptions.list_all(&tenant_id)?)
    }

    pub fn delete_endpoint(&self, id: Uuid) -> Result<Subscription, BrokerError> {
        let tenant_id = self.tenant();
        Ok(self.inner.subscriptions.delete(&tenant_id, id)?)
    }

    pub fn get_queue(&self, queue: &str) -> Result<Option<Subscription>, BrokerError> {
        let tenant_id = self.tenant();
        Ok(self.inner.subscriptions.get_by_name(&tenant_id, queue)?)
    }

    pub fn get_queue_by_id(&self, id: Uuid) -> Result<Option<Subscription>, BrokerError> {
        let tenant_id = self.tenant();
        Ok(self.inner.subscriptions.get_by_id(&tenant_id, id)?)
    }

    pub fn partition_high_watermark(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<u64, BrokerError> {
        Ok(self.shard_handle(topic, partition)?.high_watermark()?)
    }

    pub fn committed_hwm(&self, topic: &str, partition: u32) -> Result<u64, BrokerError> {
        let handle = self.shard_handle(topic, partition)?;
        Ok(handle.committed_hwm())
    }

    /// Block until `offset` is on the shard's committed high watermark.
    pub async fn wait_committed(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<u64, BrokerError> {
        let handle = self.shard_handle(topic, partition)?;
        handle
            .wait_committed(offset)
            .await
            .map_err(BrokerError::Storage)?;
        Ok(handle.committed_hwm())
    }

    pub fn mark_dispatch_complete(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        offset: u64,
    ) -> Result<(), BrokerError> {
        self.inner.metadata.mark_dispatch_complete(
            tenant_id,
            subscription_id,
            partition,
            offset,
        )?;
        Ok(())
    }

    pub fn is_dispatch_complete(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        offset: u64,
    ) -> Result<bool, BrokerError> {
        Ok(self.inner.metadata.is_dispatch_complete(
            tenant_id,
            subscription_id,
            partition,
            offset,
        )?)
    }

    pub fn read_message(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<StoredMessage, BrokerError> {
        let batch = self
            .shard_handle(topic, partition)?
            .read_range(partition, offset, 1)?;
        batch
            .into_iter()
            .find(|message| message.offset == offset && message.topic == topic)
            .ok_or(BrokerError::Storage(
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

    /// Force a WAL group flush on every open shard (shutdown / tests).
    pub fn flush_wal(&self) -> Result<(), BrokerError> {
        let handles: Vec<Arc<ShardHandle>> = {
            let shards = self.inner.shards.read();
            shards.values().flat_map(|m| m.values().cloned()).collect()
        };
        for handle in handles {
            handle.flush_durable()?;
        }
        let _ = self.inner.metadata.flush_epoch();
        Ok(())
    }

    /// Physically delete sealed segments at or below the durable GC watermark.
    pub fn gc_sealed_below(&self, shard: u32, watermark: u64) -> Result<usize, BrokerError> {
        let topic = if self.inner.layout.is_physical() {
            "__physical"
        } else {
            return Ok(0);
        };
        let handle = self.shard_handle(topic, shard)?;
        handle
            .gc_sealed_below(watermark)
            .map_err(BrokerError::Storage)
    }

    /// Group-commit timer: flush shards whose interval has elapsed.
    pub fn flush_wal_if_due(&self) -> Result<(), BrokerError> {
        let handles: Vec<Arc<ShardHandle>> = {
            let shards = self.inner.shards.read();
            shards.values().flat_map(|m| m.values().cloned()).collect()
        };
        for handle in handles {
            handle.flush_if_due()?;
        }
        Ok(())
    }

    /// Normalize an engine-facing batch without appending. Existing and
    /// within-batch duplicates retain their input slots.
    pub fn prepare_publish_batch(
        &self,
        requests: Vec<PublishRequest>,
    ) -> Result<PreparedPublishBatch, BrokerError> {
        let tenant_id = self.tenant();
        let mut slots = Vec::with_capacity(requests.len());
        let mut groups: Vec<PreparedShardBatch> = Vec::new();
        let mut group_indexes: HashMap<PreparedShardKey, usize> = HashMap::new();
        let mut local_owners: HashMap<String, Uuid> = HashMap::new();

        for request in requests {
            let original_topic = request.topic.clone();
            let payload = decode_payload(&request)?;
            self.validate_publish_request(&request)?;
            let reservation = if let Some(key) = request.idempotency_key.as_ref() {
                if let Some(message_id) = local_owners.get(key) {
                    slots.push(PreparedSlot::Awaiting {
                        message_id: *message_id,
                        duplicate: true,
                        topic_override: Some(original_topic),
                    });
                    continue;
                }
                match self.reserve_dedup(&tenant_id, key)? {
                    ReservationOutcome::Existing(entry) => {
                        slots.push(PreparedSlot::Existing(PublishResponse {
                            message_id: Some(entry.message_id),
                            topic: original_topic,
                            partition: Some(entry.partition),
                            offset: Some(entry.offset),
                            duplicate: true,
                            scheduled: None,
                            commit_epoch: None,
                            replication_frame: None,
                        }));
                        continue;
                    }
                    ReservationOutcome::Acquired(token) => Some(token),
                }
            } else {
                None
            };

            let record = self.prepare_record(request, payload, reservation)?;
            let message_id = record.header.id;
            if let Some(key) = record.header.idempotency_key.as_ref() {
                local_owners.insert(key.clone(), message_id);
            }
            slots.push(PreparedSlot::Awaiting {
                message_id,
                duplicate: false,
                topic_override: None,
            });
            let key = PreparedShardKey {
                partition: record.partition,
                storage_namespace: self
                    .inner
                    .layout
                    .storage_namespace(&record.topic)
                    .to_string(),
                fence_generation: record.fence_generation,
            };
            let group_index = match group_indexes.get(&key) {
                Some(index) => *index,
                None => {
                    let index = groups.len();
                    group_indexes.insert(key.clone(), index);
                    groups.push(PreparedShardBatch {
                        key: key.clone(),
                        records: Vec::new(),
                    });
                    index
                }
            };
            groups[group_index].records.push(record);
        }

        Ok(PreparedPublishBatch {
            slots,
            shard_batches: groups,
        })
    }

    /// Append one prepared physical-shard group with one backend batch write.
    pub fn append_prepared_batch(
        &self,
        batch: PreparedShardBatch,
    ) -> Result<AppendedShardBatch, BrokerError> {
        if batch.records.is_empty() {
            return Ok(AppendedShardBatch {
                appended: Vec::new(),
                records: Vec::new(),
            });
        }
        let partition = batch.key.partition;
        let fence_generation = batch.key.fence_generation;
        let topic = batch.records[0].topic.clone();
        let mut finalize = Vec::with_capacity(batch.records.len());
        let mut items = Vec::with_capacity(batch.records.len());
        for record in batch.records {
            finalize.push(FinalizeRecord {
                message_id: record.header.id,
                topic: record.topic,
                partition: record.partition,
                idempotency_key: record.header.idempotency_key.clone(),
                reservation: record.reservation,
            });
            items.push((record.header, record.payload));
        }
        let appended = self.shard_handle(&topic, partition)?.append_batch(
            partition,
            items,
            fence_generation,
        )?;
        Ok(AppendedShardBatch {
            appended,
            records: finalize,
        })
    }

    /// Finalize one appended shard group and atomically publish all dedup
    /// mappings for that group in one RocksDB WriteBatch.
    pub fn finalize_prepared_batch(
        &self,
        prepared: &mut PreparedPublishBatch,
        mut batch: AppendedShardBatch,
        commit_dedup: bool,
    ) -> Result<(), BrokerError> {
        if batch.appended.len() != batch.records.len() {
            return Err(BrokerError::InvalidConfig(
                "appended batch result count mismatch".into(),
            ));
        }
        if batch
            .records
            .iter()
            .zip(&batch.appended)
            .any(|(record, (stored, _))| {
                record.message_id != stored.id || record.partition != stored.partition
            })
        {
            return Err(BrokerError::InvalidConfig(
                "appended batch result identity mismatch".into(),
            ));
        }

        if commit_dedup {
            let tenant_id = self.tenant();
            let mut metadata = MetadataWriteBatch::default();
            for (record, (stored, _)) in batch.records.iter().zip(&batch.appended) {
                if let Some(key) = record.idempotency_key.as_ref() {
                    metadata.put_dedup(
                        tenant_id.clone(),
                        key,
                        DedupEntry {
                            message_id: stored.id,
                            offset: stored.offset,
                            partition: stored.partition,
                        },
                    );
                }
            }
            self.inner.metadata.write_batch(metadata)?;
        }

        let mut responses = HashMap::with_capacity(batch.appended.len());
        for (record, (stored, frame)) in batch.records.iter_mut().zip(batch.appended) {
            if commit_dedup {
                if let Some(reservation) = record.reservation.take() {
                    reservation.release();
                }
            } else if let Some(reservation) = record.reservation.take() {
                reservation.release();
            }
            responses.insert(
                record.message_id,
                PublishResponse {
                    message_id: Some(stored.id),
                    topic: record.topic.clone(),
                    partition: Some(record.partition),
                    offset: Some(stored.offset),
                    duplicate: false,
                    scheduled: None,
                    commit_epoch: None,
                    replication_frame: Some(frame),
                },
            );
        }

        for slot in &mut prepared.slots {
            let PreparedSlot::Awaiting {
                message_id,
                duplicate,
                topic_override,
            } = slot
            else {
                continue;
            };
            let Some(owner) = responses.get(message_id) else {
                continue;
            };
            let mut response = owner.clone();
            response.duplicate = *duplicate;
            if let Some(topic) = topic_override.take() {
                response.topic = topic;
            }
            if response.duplicate {
                response.replication_frame = None;
            }
            *slot = PreparedSlot::Final(response);
        }
        Ok(())
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

    /// Write inline or external payload bytes directly to a delivery writer.
    /// This avoids hydrating large blob-backed messages into `StoredMessage`.
    pub fn stream_message_payload_to_writer<W: std::io::Write + Unpin>(
        &self,
        msg: &StoredMessage,
        writer: &mut W,
    ) -> Result<u64, BrokerError> {
        let Some(reference_json) = msg.payload_ref_json.as_deref() else {
            writer
                .write_all(&msg.payload)
                .map_err(|error| BrokerError::Storage(broker_storage::LogError::Io(error)))?;
            return Ok(msg.payload.len() as u64);
        };
        if !msg.payload.is_empty() {
            writer
                .write_all(&msg.payload)
                .map_err(|error| BrokerError::Storage(broker_storage::LogError::Io(error)))?;
            return Ok(msg.payload.len() as u64);
        }
        let reference: broker_payload::PayloadRef =
            serde_json::from_str(reference_json).map_err(|error| {
                BrokerError::Payload(broker_payload::PayloadError::Store(error.to_string()))
            })?;
        self.inner
            .blob_store
            .get_blob_to_writer(&reference, writer)
            .map_err(BrokerError::Payload)
    }

    pub fn publish(&self, req: PublishRequest) -> Result<PublishResponse, BrokerError> {
        self.publish_inner(req, true)
    }

    /// Append without recording idempotency until replication quorum succeeds.
    pub fn publish_defer_dedup(&self, req: PublishRequest) -> Result<PublishResponse, BrokerError> {
        self.publish_inner(req, false)
    }

    fn publish_inner(
        &self,
        req: PublishRequest,
        commit_dedup: bool,
    ) -> Result<PublishResponse, BrokerError> {
        let mut prepared = self.prepare_publish_batch(vec![req])?;
        for shard_batch in prepared.take_shard_batches() {
            let appended = self.append_prepared_batch(shard_batch)?;
            self.finalize_prepared_batch(&mut prepared, appended, commit_dedup)?;
        }
        prepared
            .finish()?
            .pop()
            .ok_or_else(|| BrokerError::InvalidConfig("empty single publish result".into()))
    }

    fn prepare_record(
        &self,
        mut req: PublishRequest,
        payload: Vec<u8>,
        reservation: Option<ReservationToken>,
    ) -> Result<PreparedRecord, BrokerError> {
        let tenant_id = self.tenant();
        let message_id = Uuid::new_v4();
        // Large payloads may write a blob before append; crash orphans are GC'd later.
        let (log_payload, payload_ref_json) =
            prepare_for_log(&self.inner.blob_store, &tenant_id, message_id, payload)?;

        let _ = req.delay_ms.take();

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
                    .get_by_name(&tenant_id, &req.topic)?
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
        let queue_job = queue_sub.is_some() && req.group_member_id.is_none();
        let flow_spec = if queue_job {
            let q = queue_sub.as_ref().unwrap();
            crate::flow::queue_delivery_flow(&req.routing_key, q.parallelism, &q.topic)
        } else {
            self.resolve_flow_spec(&req)?
        };
        let (max_retries, retry_backoff) =
            resolve_delivery_retry(&req, queue_sub.as_ref(), &self.inner.config.retry_defaults);

        let partition_key = {
            let rk = req.routing_key.trim();
            if !rk.is_empty() {
                rk.to_string()
            } else {
                message_id.to_string()
            }
        };
        let partition = self
            .inner
            .layout
            .assign(&tenant_id, &req.topic, &partition_key);
        let fence_gen = self.require_shard_fence(partition)?;

        let http = HttpDeliverySpec::merge(
            req.method.clone(),
            req.headers.clone(),
            req.sign,
            req.request
                .clone()
                .map(|r| HttpDeliverySpec::merge(r.method, r.headers, r.sign, None)),
        );
        http.validate().map_err(BrokerError::InvalidConfig)?;

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
            flow_profile_id: if queue_job { None } else { req.flow_id },
            destination_url,
            destination_secret,
            max_retries,
            retry_backoff: Some(retry_backoff),
            http_method: Some(http.method.clone()),
            http_headers_json: http.headers_json(),
            http_sign: Some(http.sign),
            payload_ref_json,
        };
        Ok(PreparedRecord {
            topic: req.topic,
            partition,
            header,
            payload: log_payload,
            fence_generation: fence_gen,
            reservation,
        })
    }

    fn validate_publish_request(&self, req: &PublishRequest) -> Result<(), BrokerError> {
        if !req.topic.is_empty()
            && !is_dlq_topic(&req.topic)
            && req.queue_id.is_none()
            && req.url.is_none()
            && req.destination.is_none()
        {
            broker_proto::validate_queue_name(&req.topic)?;
        }
        if let Some(ref flow) = req.flow {
            if let Some(ref key) = flow.key {
                broker_proto::validate_flow_key(key)?;
            }
        }
        Ok(())
    }

    fn reserve_dedup(
        &self,
        tenant_id: &str,
        idempotency_key: &str,
    ) -> Result<ReservationOutcome, BrokerError> {
        let key = ReservationKey {
            tenant_id: tenant_id.to_string(),
            idempotency_key: idempotency_key.to_string(),
        };
        loop {
            {
                let mut pending = self.inner.dedup_reservations.pending.lock();
                while pending.contains(&key) {
                    self.inner.dedup_reservations.changed.wait(&mut pending);
                }
            }
            if let Some(entry) = self.inner.metadata.get_dedup(tenant_id, idempotency_key)? {
                return Ok(ReservationOutcome::Existing(entry));
            }
            let mut pending = self.inner.dedup_reservations.pending.lock();
            if pending.insert(key.clone()) {
                return Ok(ReservationOutcome::Acquired(ReservationToken {
                    reservations: Arc::clone(&self.inner.dedup_reservations),
                    key,
                    release_on_drop: true,
                }));
            }
        }
    }

    /// Record idempotency after replication quorum (or immediately on a single node).
    pub fn commit_publish_dedup(
        &self,
        idempotency_key: &str,
        entry: DedupEntry,
    ) -> Result<(), BrokerError> {
        let tenant_id = self.tenant();
        let mut batch = MetadataWriteBatch::default();
        batch.put_dedup(tenant_id.clone(), idempotency_key, entry);
        self.inner.metadata.write_batch(batch)?;
        self.inner.dedup_reservations.release(&ReservationKey {
            tenant_id,
            idempotency_key: idempotency_key.to_string(),
        });
        Ok(())
    }

    pub fn commit_publish_dedup_batch(
        &self,
        entries: impl IntoIterator<Item = (String, DedupEntry)>,
    ) -> Result<(), BrokerError> {
        let tenant_id = self.tenant();
        let mut batch = MetadataWriteBatch::default();
        let mut keys = Vec::new();
        for (key, entry) in entries {
            batch.put_dedup(tenant_id.clone(), key.clone(), entry);
            keys.push(key);
        }
        self.inner.metadata.write_batch(batch)?;
        for key in keys {
            self.inner.dedup_reservations.release(&ReservationKey {
                tenant_id: tenant_id.clone(),
                idempotency_key: key,
            });
        }
        Ok(())
    }

    /// Remove an idempotency mapping (used when replication quorum fails after local append).
    pub fn clear_publish_dedup(&self, idempotency_key: &str) -> Result<(), BrokerError> {
        let tenant_id = self.tenant();
        self.inner
            .metadata
            .delete_dedup(&tenant_id, idempotency_key)?;
        self.inner.dedup_reservations.release(&ReservationKey {
            tenant_id,
            idempotency_key: idempotency_key.to_string(),
        });
        Ok(())
    }

    /// Apply a replicated log frame on a follower (CP7a). Disabled in slate mode (CP6b.1).
    pub fn append_replicated_frame(
        &self,
        topic: &str,
        partition: u32,
        frame: &[u8],
        expected_offset: Option<u64>,
    ) -> Result<PublishResponse, BrokerError> {
        if self.inner.config.storage == StorageMode::Slate {
            return Err(BrokerError::Storage(broker_storage::LogError::Slate(
                "slate mode: frame replication disabled; durability is on shared object storage"
                    .into(),
            )));
        }
        let stored = self.shard_handle(topic, partition)?.append_raw_frame(
            partition,
            frame,
            expected_offset,
        )?;
        Ok(PublishResponse {
            message_id: Some(stored.id),
            topic: topic.to_string(),
            partition: Some(partition),
            offset: Some(stored.offset),
            duplicate: false,
            scheduled: None,
            commit_epoch: None,
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
            .join(self.tenant());
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
            messages.extend(self.list_topic_messages_from(topic, partition, 0, remaining)?);
        }
        messages.sort_by_key(|m| (m.partition, m.offset));
        messages.truncate(max_messages);
        Ok(messages)
    }

    pub fn partition_count(&self, _topic: &str) -> Result<u32, BrokerError> {
        Ok(self.inner.config.partitions)
    }

    /// Page messages for one partition starting at `from_offset` (inclusive).
    pub fn list_topic_messages_from(
        &self,
        topic: &str,
        partition: u32,
        from_offset: u64,
        max_messages: usize,
    ) -> Result<Vec<StoredMessage>, BrokerError> {
        if max_messages == 0 {
            return Ok(Vec::new());
        }
        let handle = self.shard_handle(topic, partition)?;
        let mut cursor = from_offset;
        let mut out = Vec::with_capacity(max_messages.min(256));
        while out.len() < max_messages {
            let remaining = max_messages - out.len();
            let scan = remaining.saturating_mul(2).clamp(64, 1024);
            let batch = handle.read_range(partition, cursor, scan)?;
            let Some(last_offset) = batch.last().map(|message| message.offset) else {
                break;
            };
            cursor = last_offset.saturating_add(1);
            out.extend(
                batch
                    .into_iter()
                    .filter(|message| message.topic == topic)
                    .take(remaining),
            );
            if cursor >= handle.high_watermark()? {
                break;
            }
        }
        Ok(out)
    }

    /// Encoded frames from `from_offset` (inclusive) for follower catch-up.
    pub fn replication_frames(
        &self,
        topic: &str,
        partition: u32,
        from_offset: u64,
        max_messages: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>, BrokerError> {
        let msgs = self.list_topic_messages_from(topic, partition, from_offset, max_messages)?;
        let mut out = Vec::with_capacity(msgs.len());
        for msg in msgs {
            let header = LogRecord {
                id: msg.id,
                tenant_id: msg.tenant_id.clone(),
                topic: msg.topic.clone(),
                routing_key: msg.routing_key.clone(),
                idempotency_key: None,
                published_at_ms: msg.published_at_ms,
                priority: msg.priority,
                flow_parallelism: msg.flow_parallelism,
                flow_key: msg.flow_key.clone(),
                flow_rate: msg.flow_rate,
                flow_period_secs: msg.flow_period_secs,
                queue_id: msg.queue_id,
                group_id: msg.group_id,
                group_member_id: msg.group_member_id,
                flow_profile_id: msg.flow_profile_id,
                destination_url: msg.destination_url.clone(),
                destination_secret: msg.destination_secret.clone(),
                max_retries: msg.max_retries,
                retry_backoff: msg.retry_backoff.clone(),
                http_method: msg.http_method.clone(),
                http_headers_json: msg.http_headers_json.clone(),
                http_sign: msg.http_sign,
                payload_ref_json: msg.payload_ref_json.clone(),
            };
            let frame = broker_proto::encode_frame_vec(&header, &msg.payload)
                .map_err(|e| BrokerError::Storage(broker_storage::LogError::Record(e)))?;
            out.push((msg.offset, frame));
        }
        Ok(out)
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
        let removed = self.shard_handle(topic, partition)?.purge_offset(offset)?;
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
        let removed = self.shard_handle(topic, partition)?.purge_offset(offset)?;
        Ok(removed)
    }

    /// Tombstone DLQ records. `older_than_ms = None` deletes every message on the topic;
    /// `Some(cutoff)` keeps messages with `published_at_ms` after that instant.
    pub fn purge_dlq_topic(
        &self,
        topic: &str,
        older_than_ms: Option<i64>,
    ) -> Result<usize, BrokerError> {
        if !is_dlq_topic(topic) {
            return Ok(0);
        }
        let mut deleted = 0usize;
        let partitions = self.partition_count(topic)?;
        for partition in 0..partitions {
            let mut from = 0u64;
            loop {
                let batch = self.list_topic_messages_from(topic, partition, from, 256)?;
                let Some(last) = batch.last().map(|message| message.offset) else {
                    break;
                };
                from = last.saturating_add(1);
                for msg in batch {
                    let eligible = match older_than_ms {
                        None => true,
                        Some(cutoff) => msg.published_at_ms > 0 && msg.published_at_ms <= cutoff,
                    };
                    if eligible && self.purge_dlq_message(topic, msg.partition, msg.offset)? {
                        deleted += 1;
                    }
                }
            }
        }
        Ok(deleted)
    }

    /// Delivery no longer physically purges; GC is sealed-segment retention.
    /// Kept so callers compile; always returns false.
    pub fn try_purge_message(
        &self,
        _topic: &str,
        _partition: u32,
        _offset: u64,
    ) -> Result<bool, BrokerError> {
        Ok(false)
    }

    /// Freeze flow limits and queue destination from this broker's catalog before forwarding to a shard leader.
    pub fn prepare_for_cluster_forward(&self, req: &mut PublishRequest) -> Result<(), BrokerError> {
        let tenant_id = self.tenant();
        if req.flow.is_none() {
            if let Some(id) = req.flow_id {
                if let Some(profile) = self.inner.flows.get_by_id(&tenant_id, id)? {
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
                self.inner.subscriptions.get_by_id(&tenant_id, id)?
            } else if !req.topic.is_empty() && req.topic != DIRECT_TOPIC {
                self.inner
                    .subscriptions
                    .get_by_name(&tenant_id, &req.topic)?
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
        Ok(self.inner.groups.create_group(&self.tenant(), name)?)
    }

    pub fn list_groups(&self) -> Result<Vec<crate::groups::DispatchGroup>, BrokerError> {
        Ok(self.inner.groups.list_groups(&self.tenant())?)
    }

    pub fn get_group(&self, id: Uuid) -> Result<Option<crate::groups::DispatchGroup>, BrokerError> {
        Ok(self.inner.groups.get_group(&self.tenant(), id)?)
    }

    pub fn delete_group(&self, id: Uuid) -> Result<crate::groups::DispatchGroup, BrokerError> {
        Ok(self.inner.groups.delete_group(&self.tenant(), id)?)
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
            &self.tenant(),
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
        Ok(self.inner.groups.list_members(&self.tenant(), group_id)?)
    }

    pub fn get_group_member(
        &self,
        id: Uuid,
    ) -> Result<Option<crate::groups::GroupMember>, BrokerError> {
        Ok(self.inner.groups.get_member(&self.tenant(), id)?)
    }

    pub fn delete_group_member(&self, id: Uuid) -> Result<crate::groups::GroupMember, BrokerError> {
        Ok(self.inner.groups.delete_member(&self.tenant(), id)?)
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
        Ok(self.inner.groups.active_members(&self.tenant(), group_id)?)
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
            delay_ms: base.delay_ms,
            priority: base.priority,
            flow_id: None,
            url: Some(member.url.clone()),
            secret: Some(member.secret.clone()),
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
        let tenant_id = self.tenant();
        if let Some(id) = req.flow_id {
            let profile = self
                .inner
                .flows
                .get_by_id(&tenant_id, id)?
                .ok_or(BrokerError::FlowProfileNotFound(id))?;
            let mut spec = profile.to_spec();
            if spec.key.is_none() && !req.routing_key.is_empty() {
                spec.key = Some(req.routing_key.clone());
            }
            return Ok(Some(spec));
        }
        Ok(req.flow.clone())
    }

    fn shard_handle(&self, topic: &str, partition: u32) -> Result<Arc<ShardHandle>, BrokerError> {
        if partition >= self.inner.layout.shard_count {
            return Err(BrokerError::InvalidConfig(format!(
                "unknown shard {partition}; layout has {} shards",
                self.inner.layout.shard_count
            )));
        }
        let namespace = self.inner.layout.storage_namespace(topic).to_string();
        if let Some(handle) = self
            .inner
            .shards
            .read()
            .get(&namespace)
            .and_then(|partitions| partitions.get(&partition))
            .cloned()
        {
            return Ok(handle);
        }

        let mut shards = self.inner.shards.write();
        if let Some(handle) = shards
            .get(&namespace)
            .and_then(|partitions| partitions.get(&partition))
            .cloned()
        {
            return Ok(handle);
        }

        let tenant_id = self.tenant();
        let backend = match self.inner.config.storage {
            StorageMode::Local => {
                let (path, wal_format) = if self.inner.layout.is_physical() {
                    (
                        self.inner
                            .layout
                            .physical_shard_dir(&self.inner.config.data_dir, partition),
                        broker_storage::WAL_FORMAT_V2,
                    )
                } else {
                    (
                        partition_dir(&self.inner.config.data_dir, &tenant_id, topic, partition)?,
                        broker_storage::WAL_FORMAT_V1,
                    )
                };
                PartitionBackend::open_local_for_shard(
                    path,
                    self.inner.config.log.clone(),
                    partition,
                    wal_format,
                )?
            }
            StorageMode::Slate => {
                if self.inner.layout.is_physical() {
                    return Err(BrokerError::InvalidConfig(
                        "physical shard layout V2 is not implemented for Slate storage".into(),
                    ));
                }
                #[cfg(feature = "slate")]
                {
                    let slate = self.inner.slate.as_ref().expect("slate env");
                    let db_path = slate_db_path(&tenant_id, topic, partition)?;
                    let tenant_seg = broker_proto::sanitize_path_segment(&tenant_id)?;
                    let topic_seg = broker_proto::sanitize_path_segment(topic)?;
                    let local_cache = slate
                        .cache_root
                        .join("local")
                        .join(tenant_seg)
                        .join(topic_seg)
                        .join(format!("p{partition}"));
                    PartitionBackend::open_slate(
                        db_path,
                        local_cache,
                        slate.object_store.clone(),
                        partition,
                        self.inner.config.log.clone(),
                    )?
                }
                #[cfg(not(feature = "slate"))]
                {
                    return Err(BrokerError::Storage(broker_storage::LogError::Slate(
                        "slate feature not compiled".into(),
                    )));
                }
            }
        };
        let handle = ShardHandle::new(backend);
        shards
            .entry(namespace)
            .or_default()
            .insert(partition, Arc::clone(&handle));
        Ok(handle)
    }

    pub fn assign_shard(&self, topic: &str, routing_key: &str) -> u32 {
        self.inner.layout.assign(&self.tenant(), topic, routing_key)
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
    use crate::layout::LAYOUT_V2;
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
                parallelism: None,
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
    fn queue_key_is_fifo_plain_uses_parallelism() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        broker
            .create_subscription(CreateSubscriptionRequest {
                topic: "jobs".into(),
                url: "http://127.0.0.1:9/hook".into(),
                secret: "sec".into(),
                parallelism: Some(8),
                default_max_retries: None,
                retry_backoff: None,
            })
            .unwrap();

        let keyed = broker
            .publish(PublishRequest {
                topic: "jobs".into(),
                queue_id: None,
                group_id: None,
                group_member_id: None,
                routing_key: "user-42".into(),
                payload: "k".into(),
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
        let keyed_msg = broker
            .read_message("jobs", keyed.partition.unwrap(), keyed.offset.unwrap())
            .unwrap();
        assert_eq!(keyed_msg.flow_parallelism, Some(1));
        assert_eq!(keyed_msg.flow_key.as_deref(), Some("user-42"));
        assert!(keyed_msg.flow_profile_id.is_none());

        let plain = broker
            .publish(PublishRequest {
                topic: "jobs".into(),
                queue_id: None,
                group_id: None,
                group_member_id: None,
                routing_key: String::new(),
                payload: "p".into(),
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
        let plain_msg = broker
            .read_message("jobs", plain.partition.unwrap(), plain.offset.unwrap())
            .unwrap();
        assert_eq!(plain_msg.flow_parallelism, Some(8));
        assert_eq!(plain_msg.flow_key.as_deref(), Some("jobs"));
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
                parallelism: None,
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
        let mut streamed = Vec::new();
        let streamed_bytes = broker
            .stream_message_payload_to_writer(&msg, &mut streamed)
            .unwrap();
        assert_eq!(streamed_bytes, body.len() as u64);
        assert_eq!(streamed, body.as_bytes());
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
                parallelism: None,
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

    #[test]
    fn independent_shards_append_concurrently() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        broker
            .create_subscription(CreateSubscriptionRequest {
                topic: "orders".into(),
                url: "http://127.0.0.1:9/h".into(),
                secret: "s".into(),
                parallelism: None,
                default_max_retries: None,
                retry_backoff: None,
            })
            .unwrap();
        std::thread::scope(|s| {
            for i in 0..4 {
                let broker = broker.clone();
                s.spawn(move || {
                    for n in 0..25 {
                        broker
                            .publish(PublishRequest {
                                topic: "orders".into(),
                                queue_id: None,
                                group_id: None,
                                group_member_id: None,
                                routing_key: format!("lane-{i}"),
                                payload: format!("{n}"),
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
                    }
                });
            }
        });
        broker.flush_wal().unwrap();
        let mut total = 0usize;
        for p in 0..broker.config().partitions {
            total += broker
                .list_topic_messages_from("orders", p, 0, 10_000)
                .unwrap()
                .len();
        }
        assert_eq!(total, 100);
    }

    fn direct_request(
        payload: impl Into<String>,
        idempotency_key: Option<String>,
    ) -> PublishRequest {
        PublishRequest {
            topic: String::new(),
            queue_id: None,
            group_id: None,
            group_member_id: None,
            routing_key: "shared-lane".into(),
            payload: payload.into(),
            payload_encoding: None,
            idempotency_key,
            delay_ms: None,
            priority: None,
            flow_id: None,
            url: Some("http://127.0.0.1:9/h".into()),
            secret: Some("s".into()),
            destination: None,
            flow: None,
            parallelism: None,
            max_retries: None,
            retry_backoff: None,
            method: None,
            headers: None,
            sign: None,
            request: None,
        }
    }

    fn queue_request(topic: &str, payload: &str) -> PublishRequest {
        let mut request = direct_request(payload, None);
        request.topic = topic.into();
        request.url = None;
        request.secret = None;
        request
    }

    fn open_v2_broker(path: &std::path::Path, shard_count: u32) -> Broker {
        std::fs::create_dir_all(path).unwrap();
        let layout = ShardLayout {
            version: LAYOUT_V2,
            shard_count,
            route_hash_version: broker_proto::HASH_VERSION_V2,
        };
        broker_storage::atomic_write_file(
            &ShardLayout::path(path),
            &serde_json::to_vec(&layout).unwrap(),
        )
        .unwrap();
        Broker::open(BrokerConfig::new(path.to_path_buf())).unwrap()
    }

    fn execute_prepared(broker: &Broker, requests: Vec<PublishRequest>) -> Vec<PublishResponse> {
        let mut prepared = broker.prepare_publish_batch(requests).unwrap();
        for shard_batch in prepared.take_shard_batches() {
            let appended = broker.append_prepared_batch(shard_batch).unwrap();
            broker
                .finalize_prepared_batch(&mut prepared, appended, true)
                .unwrap();
        }
        prepared.finish().unwrap()
    }

    #[test]
    fn same_shard_hundred_records_use_one_v2_epoch() {
        let dir = tempdir().unwrap();
        let broker = open_v2_broker(dir.path(), 8);
        let requests = (0..100)
            .map(|index| direct_request(format!("body-{index}"), Some(format!("key-{index}"))))
            .collect();
        let mut prepared = broker.prepare_publish_batch(requests).unwrap();
        let groups = prepared.take_shard_batches();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 100);
        let partition = groups[0].key().partition();
        let appended = broker
            .append_prepared_batch(groups.into_iter().next().unwrap())
            .unwrap();
        broker
            .finalize_prepared_batch(&mut prepared, appended, true)
            .unwrap();
        let responses = prepared.finish().unwrap();
        let last_offset = responses.last().unwrap().offset.unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let hwm = runtime
            .block_on(broker.wait_committed(DIRECT_TOPIC, partition, last_offset))
            .unwrap();
        assert_eq!(hwm, 100);
        assert_eq!(
            broker
                .shard_handle(DIRECT_TOPIC, partition)
                .unwrap()
                .flush_count()
                .unwrap(),
            1
        );

        let wal = std::fs::read(
            broker
                .physical_shard_path(partition)
                .unwrap()
                .join("active.wal"),
        )
        .unwrap();
        let mut cursor = std::io::Cursor::new(wal.as_slice());
        let (epoch, _) = broker_proto::decode_epoch(&mut cursor).unwrap();
        assert_eq!(epoch.record_count, 100);
        assert_eq!(cursor.position(), wal.len() as u64);
    }

    #[test]
    fn v2_batch_can_mix_topics_on_one_physical_shard() {
        let dir = tempdir().unwrap();
        let broker = open_v2_broker(dir.path(), 8);
        for topic in ["orders", "invoices"] {
            broker
                .create_subscription(CreateSubscriptionRequest {
                    topic: topic.into(),
                    url: format!("http://127.0.0.1:9/{topic}"),
                    secret: "s".into(),
                    parallelism: None,
                    default_max_retries: None,
                    retry_backoff: None,
                })
                .unwrap();
        }
        let mut prepared = broker
            .prepare_publish_batch(vec![
                queue_request("orders", "one"),
                queue_request("invoices", "two"),
            ])
            .unwrap();
        let groups = prepared.take_shard_batches();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 2);
        let appended = broker
            .append_prepared_batch(groups.into_iter().next().unwrap())
            .unwrap();
        broker
            .finalize_prepared_batch(&mut prepared, appended, true)
            .unwrap();
        let responses = prepared.finish().unwrap();
        assert_eq!(responses[0].topic, "orders");
        assert_eq!(responses[1].topic, "invoices");
        assert_eq!(responses[0].partition, responses[1].partition);
        assert_ne!(responses[0].offset, responses[1].offset);
    }

    #[test]
    fn within_batch_duplicates_keep_input_order() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let responses = execute_prepared(
            &broker,
            vec![
                direct_request("a-owner", Some("a".into())),
                direct_request("b-owner", Some("b".into())),
                direct_request("a-duplicate", Some("a".into())),
                direct_request("b-duplicate", Some("b".into())),
            ],
        );
        assert_eq!(responses.len(), 4);
        assert_eq!(
            responses.iter().map(|r| r.duplicate).collect::<Vec<_>>(),
            vec![false, false, true, true]
        );
        assert_eq!(responses[0].message_id, responses[2].message_id);
        assert_eq!(responses[1].message_id, responses[3].message_id);
        assert_eq!(responses[0].offset, responses[2].offset);
        assert_eq!(responses[1].offset, responses[3].offset);
    }

    #[test]
    fn concurrent_same_key_reservation_allows_one_append() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(16));
        let responses = std::thread::scope(|scope| {
            let mut threads = Vec::new();
            for index in 0..16 {
                let broker = broker.clone();
                let barrier = Arc::clone(&barrier);
                threads.push(scope.spawn(move || {
                    barrier.wait();
                    broker
                        .publish(direct_request(
                            format!("candidate-{index}"),
                            Some("race-key".into()),
                        ))
                        .unwrap()
                }));
            }
            threads
                .into_iter()
                .map(|thread| thread.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(
            responses
                .iter()
                .filter(|response| !response.duplicate)
                .count(),
            1
        );
        let first = &responses[0];
        assert!(responses.iter().all(|response| {
            response.message_id == first.message_id && response.offset == first.offset
        }));
        let partition = first.partition.unwrap();
        assert_eq!(
            broker
                .list_topic_messages_from(DIRECT_TOPIC, partition, 0, 100)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn dropping_unappended_batch_releases_reservation() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let prepared = broker
            .prepare_publish_batch(vec![direct_request(
                "abandoned",
                Some("released-key".into()),
            )])
            .unwrap();
        drop(prepared);

        let response = broker
            .publish(direct_request("retry", Some("released-key".into())))
            .unwrap();
        assert!(!response.duplicate);
    }

    #[test]
    fn append_failure_releases_reservation() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let generation = Arc::new(AtomicU64::new(2));
        let check = Arc::clone(&generation);
        broker.set_shard_leader_check(Arc::new(move |_| Some(check.load(Ordering::Acquire))));

        let mut prepared = broker
            .prepare_publish_batch(vec![direct_request(
                "will-fail",
                Some("retry-after-failure".into()),
            )])
            .unwrap();
        let stale_batch = prepared.take_shard_batches().pop().unwrap();

        generation.store(3, Ordering::Release);
        broker.publish(direct_request("new-term", None)).unwrap();
        let error = match broker.append_prepared_batch(stale_batch) {
            Err(error) => error,
            Ok(_) => panic!("stale prepared batch unexpectedly appended"),
        };
        assert!(matches!(
            error,
            BrokerError::Storage(broker_storage::LogError::StaleFence { .. })
        ));

        let response = broker
            .publish(direct_request("retry", Some("retry-after-failure".into())))
            .unwrap();
        assert!(!response.duplicate);
    }

    #[test]
    fn purge_dlq_topic_deletes_all_or_only_older_than_cutoff() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let topic = "jobs.__dlq";
        for key in ["a", "b"] {
            broker
                .publish(PublishRequest {
                    topic: topic.into(),
                    queue_id: None,
                    group_id: None,
                    group_member_id: None,
                    routing_key: key.into(),
                    payload: "{}".into(),
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
        }
        assert_eq!(broker.list_topic_messages(topic, 10).unwrap().len(), 2);
        assert_eq!(broker.purge_dlq_topic(topic, Some(0)).unwrap(), 0);
        assert_eq!(broker.list_topic_messages(topic, 10).unwrap().len(), 2);
        assert_eq!(broker.purge_dlq_topic(topic, None).unwrap(), 2);
        assert!(broker.list_topic_messages(topic, 10).unwrap().is_empty());
    }
}
