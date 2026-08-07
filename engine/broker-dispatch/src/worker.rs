//! Webhook push workers: HMAC signing, retries, DLQ on exhaustion.
//! Each message carries the destination URL frozen at enqueue time.

use crate::flow_control::FlowController;
use crate::host_blocker::{HostBlocker, HostBlockerConfig};
use crate::memory_guard::{MemoryGuard, MemoryGuardConfig};
use crate::outbound::{apply_to_reqwest, build_outbound};
use broker_partition::{dlq_topic, group_member_dlq_topic};
use broker_partition::{Broker, PublishRequest, DIRECT_TOPIC};
use broker_proto::RetryDefaults;
use broker_storage::StoredMessage;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex, Semaphore};
use tracing::{info, warn};
use uuid::Uuid;

/// Cap concurrent in-flight delivery tasks (HA M1). Override with BETTERMQ_DISPATCH_MAX_IN_FLIGHT.
fn dispatch_max_in_flight() -> usize {
    std::env::var("BETTERMQ_DISPATCH_MAX_IN_FLIGHT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(256)
        .max(1)
}

#[derive(Debug, Clone)]
pub struct DispatchConfig {
    pub retry_defaults: RetryDefaults,
    /// Default outbound webhook timeout (CP6).
    pub http_timeout_secs: u64,
    /// Plan-tier long wait pool (up to 12h on Scale).
    pub long_http_timeout_secs: u64,
    /// Payload size above which the long client is used.
    pub long_payload_threshold_bytes: usize,
    pub host_blocker: HostBlockerConfig,
    pub memory_guard: MemoryGuardConfig,
    /// HTTP status codes that should not be retried (CP6a).
    pub non_retry_status_codes: Vec<u16>,
}

impl Default for DispatchConfig {
    fn default() -> Self {
        let http_timeout_secs = std::env::var("BETTERMQ_HTTP_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);
        let long_http_timeout_secs = std::env::var("BETTERMQ_LONG_HTTP_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(7200);
        let max_retries = std::env::var("BETTERMQ_DEFAULT_MAX_RETRIES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3);
        let retry_defaults = RetryDefaults {
            max_retries,
            ..RetryDefaults::default()
        };
        Self {
            retry_defaults,
            http_timeout_secs,
            long_http_timeout_secs,
            long_payload_threshold_bytes: 256 * 1024,
            host_blocker: HostBlockerConfig::default(),
            memory_guard: MemoryGuardConfig::default(),
            non_retry_status_codes: vec![400, 401, 403, 404, 422],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryPriority {
    High,
    Low,
}

#[derive(Debug, Clone)]
pub struct DeliveryJob {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub message_id: Uuid,
    pub priority: DeliveryPriority,
}

impl DeliveryJob {
    pub fn live(topic: impl Into<String>, partition: u32, offset: u64, message_id: Uuid) -> Self {
        Self {
            topic: topic.into(),
            partition,
            offset,
            message_id,
            priority: DeliveryPriority::High,
        }
    }
}

#[derive(Debug, Error)]
pub enum DispatchError {
    #[error("broker error: {0}")]
    Broker(#[from] broker_partition::BrokerError),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("message has no push destination")]
    NoDestination,
    #[error("{0}")]
    Failed(String),
}

type ShardLeaderFn = Arc<dyn Fn(u32) -> bool + Send + Sync>;
type DispatchGaps = Arc<Mutex<HashMap<(Uuid, u32), BTreeSet<u64>>>>;

struct LeaseDrop {
    leases: Option<crate::lease::LeaseTable>,
    lease_id: Option<Uuid>,
    holder: String,
}

impl Drop for LeaseDrop {
    fn drop(&mut self) {
        if let (Some(leases), Some(id)) = (&self.leases, self.lease_id) {
            let _ = leases.take(id, &self.holder);
        }
    }
}

#[derive(Clone)]
pub struct DispatchEngine {
    broker: Broker,
    config: DispatchConfig,
    client: reqwest::Client,
    long_client: reqwest::Client,
    high_tx: mpsc::UnboundedSender<DeliveryJob>,
    pub flow: FlowController,
    dispatch_gaps: DispatchGaps,
    is_shard_leader: Option<ShardLeaderFn>,
    host_blocker: Arc<HostBlocker>,
    memory_guard: Arc<MemoryGuard>,
    in_flight: Arc<Semaphore>,
    /// Shared lease table — same CAS path as fleet claim API.
    leases: Option<crate::lease::LeaseTable>,
    local_holder: String,
    workers_enabled: bool,
}

impl DispatchEngine {
    pub fn new(broker: Broker, config: DispatchConfig) -> Self {
        Self::new_with_mode(broker, config, true)
    }

    /// Broker-only: no local delivery workers (fleet claims via lease API).
    pub fn new_broker_only(broker: Broker, config: DispatchConfig) -> Self {
        Self::new_with_mode(broker, config, false)
    }

    fn new_with_mode(broker: Broker, config: DispatchConfig, workers_enabled: bool) -> Self {
        let (high_tx, high_rx) = mpsc::unbounded_channel();
        let config_clone = config.clone();
        let memory_guard = Arc::new(MemoryGuard::new(config_clone.memory_guard.clone()));
        // Apply fleet long-wait tier when set (Phase E).
        let long_secs =
            crate::lease::long_wait_tier_secs().unwrap_or(config_clone.long_http_timeout_secs);
        let global_max = std::env::var("BETTERMQ_DISPATCH_GLOBAL_MAX")
            .ok()
            .and_then(|s| s.parse().ok());
        let engine = Self {
            broker,
            config,
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(2))
                .timeout(Duration::from_secs(config_clone.http_timeout_secs))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("reqwest short client"),
            long_client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(2))
                .timeout(Duration::from_secs(long_secs))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("reqwest long client"),
            high_tx,
            flow: FlowController::new(global_max, memory_guard.clone()),
            dispatch_gaps: Arc::new(Mutex::new(HashMap::new())),
            is_shard_leader: None,
            host_blocker: Arc::new(HostBlocker::new(config_clone.host_blocker.clone())),
            memory_guard,
            in_flight: Arc::new(Semaphore::new(dispatch_max_in_flight())),
            leases: None,
            local_holder: format!("local-{}", Uuid::new_v4()),
            workers_enabled,
        };
        engine.memory_guard.spawn_monitor();
        if workers_enabled {
            engine.spawn_workers(high_rx);
        }
        engine
    }

    pub fn with_leases(mut self, leases: crate::lease::LeaseTable) -> Self {
        self.leases = Some(leases);
        self
    }

    pub fn workers_enabled(&self) -> bool {
        self.workers_enabled
    }

    /// Deliver a hydrated message (used by fleet workers).
    pub async fn deliver_stored_message(&self, msg: &StoredMessage) -> Result<(), DispatchError> {
        self.deliver_message(msg).await
    }

    pub fn host_blocker(&self) -> Arc<HostBlocker> {
        self.host_blocker.clone()
    }

    pub fn memory_guard(&self) -> Arc<MemoryGuard> {
        self.memory_guard.clone()
    }

    /// Only dispatch/backfill partitions where this node is shard leader (CP7a).
    pub fn with_shard_leader_check(mut self, check: ShardLeaderFn) -> Self {
        self.is_shard_leader = Some(check);
        self
    }

    fn shard_leader(&self, partition: u32) -> bool {
        self.is_shard_leader
            .as_ref()
            .map(|f| f(partition))
            .unwrap_or(true)
    }

    pub fn enqueue(&self, job: DeliveryJob) {
        if !self.workers_enabled {
            return;
        }
        if !self.shard_leader(job.partition) {
            return;
        }
        let _ = self.high_tx.send(job);
    }

    /// Re-enqueue undelivered messages after restart (CP2.5 / CP7a).
    /// Pages per partition from cursor → high-water mark (no global 50k cliff).
    pub fn backfill_pending(&self) {
        if !self.workers_enabled {
            return;
        }
        if self.memory_guard.is_critical() {
            info!("dispatch backfill skipped: memory critical");
            return;
        }
        let tenant_id = self.broker.config().tenant_id.clone();
        let mut topics: HashSet<String> = HashSet::new();
        topics.insert(DIRECT_TOPIC.to_string());
        if let Ok(queues) = self.broker.list_endpoints() {
            for q in queues {
                topics.insert(q.topic);
            }
        }

        let mut enqueued = 0u64;
        const PAGE: usize = 500;
        for topic in topics {
            if broker_partition::is_dlq_topic(&topic) {
                continue;
            }
            let Ok(partition_count) = self.broker.partition_count(&topic) else {
                continue;
            };
            for partition in 0..partition_count {
                if !self.shard_leader(partition) {
                    continue;
                }
                // Walk messages in pages; broker list API may still cap — use offset cursor.
                let mut from_offset = 0u64;
                loop {
                    let Ok(messages) =
                        self.broker
                            .list_topic_messages_from(&topic, partition, from_offset, PAGE)
                    else {
                        // Fallback to legacy list if paged API missing.
                        break;
                    };
                    if messages.is_empty() {
                        break;
                    }
                    let mut advanced = false;
                    for msg in &messages {
                        from_offset = from_offset.max(msg.offset.saturating_add(1));
                        advanced = true;
                        if msg
                            .destination_url
                            .as_ref()
                            .map(|u| u.is_empty())
                            .unwrap_or(true)
                        {
                            continue;
                        }
                        let lane_owner = msg
                            .group_member_id
                            .or(msg.queue_id)
                            .or(msg.flow_profile_id)
                            .unwrap_or(msg.id);
                        let cursor = self
                            .broker
                            .dispatch_offset(&tenant_id, &lane_owner.to_string(), msg.partition)
                            .unwrap_or(0);
                        if msg.offset < cursor {
                            continue;
                        }
                        self.enqueue(DeliveryJob::live(
                            msg.topic.clone(),
                            msg.partition,
                            msg.offset,
                            msg.id,
                        ));
                        enqueued += 1;
                    }
                    if !advanced || messages.len() < PAGE {
                        break;
                    }
                }
            }
        }
        if enqueued > 0 {
            info!(enqueued, "dispatch backfill enqueued pending messages");
        }
    }
}

impl DispatchEngine {
    fn spawn_workers(&self, mut high_rx: mpsc::UnboundedReceiver<DeliveryJob>) {
        let this = self.clone();
        tokio::spawn(async move {
            while let Some(job) = high_rx.recv().await {
                let engine = this.clone();
                let permit = match engine.in_flight.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => {
                        warn!("dispatch semaphore closed");
                        break;
                    }
                };
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(e) = engine.deliver_job(job).await {
                        warn!(error = %e, "delivery job failed");
                    }
                });
            }
        });
    }

    /// Re-enqueue after a delay without advancing the dispatch cursor (pause / host block).
    fn defer_job(&self, job: DeliveryJob, delay: Duration, reason: &'static str) {
        let engine = self.clone();
        info!(
            topic = %job.topic,
            partition = job.partition,
            offset = job.offset,
            delay_ms = delay.as_millis() as u64,
            reason,
            "delivery deferred; will retry"
        );
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            engine.enqueue(job);
        });
    }

    async fn deliver_job(&self, job: DeliveryJob) -> Result<(), DispatchError> {
        if !self.shard_leader(job.partition) {
            return Ok(());
        }
        // Lease seam: skip if already claimed by fleet/another worker.
        if let Some(leases) = &self.leases {
            if leases.is_offset_leased(&job.topic, job.partition, job.offset) {
                return Ok(());
            }
        }
        let msg = match self
            .broker
            .read_message(&job.topic, job.partition, job.offset)
        {
            Ok(m) => m,
            Err(e) => {
                warn!(
                    error = %e,
                    topic = %job.topic,
                    partition = job.partition,
                    offset = job.offset,
                    message_id = %job.message_id,
                    "delivery skipped: message not found in log"
                );
                return Ok(());
            }
        };

        let lease_id = if let Some(leases) = &self.leases {
            if leases.is_offset_leased(&job.topic, job.partition, job.offset) {
                return Ok(());
            }
            let lane = msg
                .group_member_id
                .or(msg.queue_id)
                .or(msg.flow_profile_id)
                .map(|id| id.to_string())
                .unwrap_or_else(|| msg.topic.clone());
            let id = Uuid::new_v4();
            leases.insert(
                self.local_holder.clone(),
                crate::lease::ClaimedJob {
                    lease_id: id,
                    topic: job.topic.clone(),
                    partition: job.partition,
                    offset: job.offset,
                    message_id: job.message_id,
                    expires_at_ms: chrono::Utc::now().timestamp_millis() + 60_000,
                    generation: 0,
                    cursor_key: lane,
                    message: None,
                },
            );
            Some(id)
        } else {
            None
        };
        let _lease_guard = LeaseDrop {
            leases: self.leases.clone(),
            lease_id,
            holder: self.local_holder.clone(),
        };

        if msg
            .destination_url
            .as_ref()
            .map(|u| u.is_empty())
            .unwrap_or(true)
        {
            let _ = self
                .move_to_dlq(&msg, "missing destination URL — moved to DLQ")
                .await;
            let _ = self
                .broker
                .try_purge_message(&job.topic, job.partition, job.offset);
            return Ok(());
        }

        if let Some(queue_id) = msg.queue_id {
            if let Some(queue) = self.broker.get_queue_by_id(queue_id)? {
                if queue.paused {
                    self.defer_job(job, Duration::from_secs(15), "queue_paused");
                    return Ok(());
                }
            }
        }
        if let Some(member_id) = msg.group_member_id {
            if let Some(member) = self.broker.get_group_member(member_id)? {
                if member.paused {
                    self.defer_job(job, Duration::from_secs(15), "member_paused");
                    return Ok(());
                }
            }
        }

        if !broker_partition::delivery_uses_flow_control(&msg) {
            return self.deliver_message(&msg).await;
        }

        let limits = broker_partition::ResolvedFlow::for_delivery(&msg.routing_key, &msg);
        let lane_owner = msg
            .group_member_id
            .or(msg.queue_id)
            .or(msg.flow_profile_id)
            .expect("flow control lane");
        self.flow
            .submit(self.clone(), lane_owner, msg.clone(), limits, false)
            .await;
        Ok(())
    }

    pub(crate) async fn deliver_message(&self, msg: &StoredMessage) -> Result<(), DispatchError> {
        self.memory_guard.wait_below_limit().await;

        let url = msg
            .destination_url
            .as_deref()
            .filter(|u| !u.is_empty())
            .ok_or(DispatchError::NoDestination)?;

        if let Err(e) = crate::egress::validate_destination_url_resolved(url).await {
            warn!(destination = %url, error = %e, "delivery blocked by egress policy — moving to DLQ");
            self.move_to_dlq(msg, format!("destination blocked by egress policy: {e}"))
                .await?;
            let lane_owner = msg
                .group_member_id
                .or(msg.queue_id)
                .or(msg.flow_profile_id)
                .unwrap_or(msg.id);
            let tenant_id = if !msg.tenant_id.is_empty() {
                msg.tenant_id.clone()
            } else {
                self.broker.tenant()
            };
            self.commit_dispatch_offset(&tenant_id, lane_owner, msg.partition, msg.offset)
                .await?;
            let _ = self
                .broker
                .try_purge_message(&msg.topic, msg.partition, msg.offset);
            return Ok(());
        }

        if self.host_blocker.is_blocked(url) {
            let job = DeliveryJob::live(msg.topic.clone(), msg.partition, msg.offset, msg.id);
            self.defer_job(job, Duration::from_secs(30), "host_blocked");
            return Ok(());
        }
        let _secret = msg
            .destination_secret
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or(DispatchError::NoDestination)?;
        let lane_owner = msg
            .group_member_id
            .or(msg.queue_id)
            .or(msg.flow_profile_id)
            .unwrap_or(msg.id);

        let tenant_id = if !msg.tenant_id.is_empty() {
            msg.tenant_id.clone()
        } else {
            self.broker.tenant()
        };
        let cursor =
            self.broker
                .dispatch_offset(&tenant_id, &lane_owner.to_string(), msg.partition)?;

        if msg.offset < cursor {
            return Ok(());
        }

        let mut delivery_msg = msg.clone();
        if let Err(e) = self.broker.hydrate_message_payload(&mut delivery_msg) {
            warn!(
                error = %e,
                message_id = %msg.id,
                "delivery deferred: could not load payload blob"
            );
            let job = DeliveryJob::live(msg.topic.clone(), msg.partition, msg.offset, msg.id);
            self.defer_job(job, Duration::from_secs(10), "hydrate_failed");
            return Ok(());
        }

        let mut attempt = 0u32;
        let started = Instant::now();
        let mut last_failure: Option<String>;

        loop {
            let outbound = build_outbound(&delivery_msg);
            let http = if delivery_msg.payload.len() >= self.config.long_payload_threshold_bytes {
                &self.long_client
            } else {
                &self.client
            };
            let response = apply_to_reqwest(http, url, outbound).send().await;

            match response {
                Ok(resp) if resp.status().is_success() => {
                    self.host_blocker.record_success(url);
                    self.commit_dispatch_offset(&tenant_id, lane_owner, msg.partition, msg.offset)
                        .await?;
                    let _ = self
                        .broker
                        .try_purge_message(&msg.topic, msg.partition, msg.offset);
                    info!(
                        lane_owner = %lane_owner,
                        queue = %msg.topic,
                        destination = %url,
                        partition = msg.partition,
                        offset = msg.offset,
                        routing_key = %msg.routing_key,
                        priority = msg.priority,
                        attempt,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "webhook delivered"
                    );
                    return Ok(());
                }
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    last_failure = Some(format!("HTTP {status} from destination"));
                    warn!(
                        status = %resp.status(),
                        lane_owner = %lane_owner,
                        destination = %url,
                        attempt,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "webhook non-success"
                    );
                    if self.config.non_retry_status_codes.contains(&status) {
                        self.move_to_dlq(
                            msg,
                            format!(
                                "HTTP {status} — non-retryable status (moved to DLQ without further retries)"
                            ),
                        )
                        .await?;
                        self.commit_dispatch_offset(
                            &tenant_id,
                            lane_owner,
                            msg.partition,
                            msg.offset,
                        )
                        .await?;
                        let _ =
                            self.broker
                                .try_purge_message(&msg.topic, msg.partition, msg.offset);
                        return Ok(());
                    }
                    if let Some(retry_after) = resp.headers().get("retry-after") {
                        if let Ok(secs) = retry_after.to_str().unwrap_or("0").parse::<u64>() {
                            let cap = self.config.retry_defaults.backoff.max_ms;
                            let wait = (secs * 1000).min(cap);
                            tokio::time::sleep(Duration::from_millis(wait)).await;
                        }
                    }
                }
                Err(e) => {
                    self.host_blocker.record_transport_failure(url);
                    last_failure = Some(format!("transport error: {e}"));
                    warn!(
                        error = %e,
                        lane_owner = %lane_owner,
                        destination = %url,
                        attempt,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "webhook request failed"
                    );
                }
            }

            attempt += 1;
            let max_retries = msg.max_retries;
            if attempt > max_retries {
                let attempts = max_retries.saturating_add(1);
                let detail = last_failure.as_deref().unwrap_or("delivery failed");
                self.move_to_dlq(
                    msg,
                    format!(
                        "{detail} — exhausted {attempts} delivery attempt(s) (max_retries={max_retries})"
                    ),
                )
                .await?;
                self.commit_dispatch_offset(&tenant_id, lane_owner, msg.partition, msg.offset)
                    .await?;
                let _ = self
                    .broker
                    .try_purge_message(&msg.topic, msg.partition, msg.offset);
                return Ok(());
            }

            let backoff_cfg = msg
                .retry_backoff
                .as_ref()
                .unwrap_or(&self.config.retry_defaults.backoff);
            let backoff = backoff_cfg.delay_ms(attempt);
            tokio::time::sleep(Duration::from_millis(backoff)).await;
        }
    }

    async fn commit_dispatch_offset(
        &self,
        tenant_id: &str,
        queue_id: Uuid,
        partition: u32,
        offset: u64,
    ) -> Result<(), DispatchError> {
        let key = queue_id.to_string();
        let cursor = self.broker.dispatch_offset(tenant_id, &key, partition)?;
        if offset < cursor {
            return Ok(());
        }
        let gap_key = (queue_id, partition);
        let mut gaps = self.dispatch_gaps.lock().await;
        let pending = gaps.entry(gap_key).or_default();
        if offset == cursor {
            let mut next = cursor + 1;
            while pending.remove(&next) {
                next += 1;
            }
            self.broker
                .set_dispatch_offset(tenant_id, &key, partition, next)?;
        } else {
            pending.insert(offset);
        }
        Ok(())
    }

    async fn move_to_dlq(
        &self,
        msg: &StoredMessage,
        reason: impl Into<String>,
    ) -> Result<(), DispatchError> {
        let mut msg = msg.clone();
        self.broker
            .hydrate_message_payload(&mut msg)
            .map_err(DispatchError::Broker)?;
        let dlq = match (msg.group_id, msg.group_member_id) {
            (Some(gid), Some(mid)) => group_member_dlq_topic(gid, mid),
            _ => dlq_topic(&msg.topic),
        };
        let reason = reason.into();
        let payload = serde_json::json!({
            "source_queue": msg.topic,
            "message_id": msg.id,
            "destination_url": msg.destination_url,
            "method": msg.http_method,
            "reason": reason,
            "body": String::from_utf8_lossy(&msg.payload),
        });
        self.broker.publish_immediate(PublishRequest {
            topic: dlq,
            routing_key: msg.id.to_string(),
            payload: payload.to_string(),
            payload_encoding: None,
            idempotency_key: None,
            delay_ms: None,
            priority: None,
            parallelism: None,
            flow: None,
            destination: None,
            flow_id: None,
            queue_id: None,
            group_id: None,
            group_member_id: None,
            url: None,
            secret: None,
            max_retries: None,
            retry_backoff: None,
            method: None,
            headers: None,
            sign: None,
            request: None,
        })?;
        warn!(
            queue = %msg.topic,
            message_id = %msg.id,
            "message moved to DLQ"
        );
        Ok(())
    }

    /// Fleet push: HTTP only (no local cursor). Caller completes/fails the lease on the broker.
    pub async fn push_http_only(&self, msg: &StoredMessage) -> Result<(), DispatchError> {
        self.memory_guard.wait_below_limit().await;
        let url = msg
            .destination_url
            .as_deref()
            .filter(|u| !u.is_empty())
            .ok_or(DispatchError::NoDestination)?;
        if let Err(e) = crate::egress::validate_destination_url_resolved(url).await {
            return Err(DispatchError::Failed(format!("egress: {e}")));
        }
        if self.host_blocker.is_blocked(url) {
            return Err(DispatchError::Failed("host blocked".into()));
        }
        let outbound = build_outbound(msg);
        let http = if msg.payload.len() >= self.config.long_payload_threshold_bytes {
            &self.long_client
        } else {
            &self.client
        };
        let response = apply_to_reqwest(http, url, outbound).send().await?;
        if response.status().is_success() {
            self.host_blocker.record_success(url);
            Ok(())
        } else {
            let status = response.status().as_u16();
            if !self.config.non_retry_status_codes.contains(&status) {
                self.host_blocker.record_transport_failure(url);
            }
            Err(DispatchError::Failed(format!("HTTP {status}")))
        }
    }
}
