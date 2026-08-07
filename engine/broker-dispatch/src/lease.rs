//! Job lease protocol (Phase B): claim / heartbeat / complete / fail.
//! Shard leader owns CAS pending→leased; fleet and in-process dispatch use the same path.

use broker_partition::Broker;
use broker_partition::DIRECT_TOPIC;
use broker_storage::StoredMessage;
use chrono::Utc;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

use crate::fairness::TenantFairQueue;

#[derive(Debug, Error)]
pub enum LeaseError {
    #[error("not shard leader for partition {0}")]
    NotShardLeader(u32),
    #[error("lease not found: {0}")]
    NotFound(Uuid),
    #[error("lease held by another worker")]
    Conflict,
    #[error("lease expired")]
    Expired,
    #[error("{0}")]
    Other(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimedJob {
    pub lease_id: Uuid,
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub message_id: Uuid,
    pub expires_at_ms: i64,
    pub generation: u64,
    /// Cursor key used by dispatch (lane owner UUID string, else topic).
    pub cursor_key: String,
    /// Delivery envelope — present on claim so fleet can push without a second fetch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<StoredMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimRequest {
    pub holder: String,
    #[serde(default = "default_max_jobs")]
    pub max_jobs: usize,
    #[serde(default = "default_ttl_ms")]
    pub lease_ttl_ms: i64,
    /// Optional partition filter (fleet may round-robin).
    pub partition: Option<u32>,
}

fn default_max_jobs() -> usize {
    8
}
fn default_ttl_ms() -> i64 {
    30_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimResponse {
    pub jobs: Vec<ClaimedJob>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub lease_id: Uuid,
    pub holder: String,
    #[serde(default = "default_ttl_ms")]
    pub lease_ttl_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteRequest {
    pub lease_id: Uuid,
    pub holder: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailRequest {
    pub lease_id: Uuid,
    pub holder: String,
    pub reason: String,
    /// When true, move to DLQ instead of retry delay.
    #[serde(default)]
    pub dead_letter: bool,
}

#[derive(Debug, Clone)]
struct LeaseEntry {
    job: ClaimedJob,
    holder: String,
}

type LeasedOffsetKey = (String, u32, u64);
type LeasedOffsetMap = HashMap<LeasedOffsetKey, Uuid>;

/// In-process lease table (HA: also durable via broker cursor; leases themselves are soft state).
#[derive(Clone, Default)]
pub struct LeaseTable {
    inner: Arc<Mutex<HashMap<Uuid, LeaseEntry>>>,
    /// offset keys currently leased: (topic, partition, offset)
    leased_offsets: Arc<Mutex<LeasedOffsetMap>>,
}

impl LeaseTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, lease_id: Uuid) -> Option<ClaimedJob> {
        self.purge_expired();
        self.inner.lock().get(&lease_id).map(|e| e.job.clone())
    }

    pub fn is_offset_leased(&self, topic: &str, partition: u32, offset: u64) -> bool {
        self.purge_expired();
        self.leased_offsets
            .lock()
            .contains_key(&(topic.to_string(), partition, offset))
    }

    pub fn insert(&self, holder: String, job: ClaimedJob) {
        let id = job.lease_id;
        let key = (job.topic.clone(), job.partition, job.offset);
        self.leased_offsets.lock().insert(key, id);
        self.inner.lock().insert(id, LeaseEntry { job, holder });
    }

    pub fn heartbeat(
        &self,
        lease_id: Uuid,
        holder: &str,
        ttl_ms: i64,
    ) -> Result<ClaimedJob, LeaseError> {
        self.purge_expired();
        let mut map = self.inner.lock();
        let entry = map
            .get_mut(&lease_id)
            .ok_or(LeaseError::NotFound(lease_id))?;
        if entry.holder != holder {
            return Err(LeaseError::Conflict);
        }
        let now = Utc::now().timestamp_millis();
        if entry.job.expires_at_ms <= now {
            return Err(LeaseError::Expired);
        }
        entry.job.expires_at_ms = now + ttl_ms;
        Ok(entry.job.clone())
    }

    pub fn take(&self, lease_id: Uuid, holder: &str) -> Result<ClaimedJob, LeaseError> {
        self.purge_expired();
        let mut map = self.inner.lock();
        let entry = map
            .remove(&lease_id)
            .ok_or(LeaseError::NotFound(lease_id))?;
        if entry.holder != holder {
            map.insert(lease_id, entry);
            return Err(LeaseError::Conflict);
        }
        let key = (
            entry.job.topic.clone(),
            entry.job.partition,
            entry.job.offset,
        );
        self.leased_offsets.lock().remove(&key);
        Ok(entry.job)
    }

    fn purge_expired(&self) {
        let now = Utc::now().timestamp_millis();
        let mut map = self.inner.lock();
        let mut offsets = self.leased_offsets.lock();
        let expired: Vec<Uuid> = map
            .iter()
            .filter(|(_, e)| e.job.expires_at_ms <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            if let Some(e) = map.remove(&id) {
                offsets.remove(&(e.job.topic, e.job.partition, e.job.offset));
            }
        }
    }

    pub fn active_count(&self) -> usize {
        self.purge_expired();
        self.inner.lock().len()
    }

    pub fn snapshot_for_status(&self) -> Vec<ClaimedJob> {
        self.purge_expired();
        self.inner
            .lock()
            .values()
            .map(|e| {
                let mut j = e.job.clone();
                j.message = None; // strip payload from status
                j
            })
            .collect()
    }
}

fn cursor_key_for(msg: &StoredMessage) -> String {
    msg.group_member_id
        .or(msg.queue_id)
        .or(msg.flow_profile_id)
        .map(|id| id.to_string())
        .unwrap_or_else(|| msg.topic.clone())
}

/// Shared claim selection used by HTTP lease API and in-process dispatch.
#[allow(clippy::too_many_arguments)]
pub fn claim_from_broker(
    broker: &Broker,
    leases: &LeaseTable,
    fair_queue: Option<&TenantFairQueue>,
    holder: &str,
    max_jobs: usize,
    lease_ttl_ms: i64,
    partition_filter: Option<u32>,
    is_leader: &dyn Fn(u32) -> bool,
    generation: &dyn Fn(u32) -> u64,
) -> ClaimResponse {
    let max = max_jobs.clamp(1, 64);
    let ttl = lease_ttl_ms.max(5_000);
    let now = Utc::now().timestamp_millis();
    let mut jobs = Vec::new();

    let mut topics: HashSet<String> = HashSet::new();
    topics.insert(DIRECT_TOPIC.to_string());
    if let Ok(queues) = broker.list_endpoints() {
        for q in queues {
            topics.insert(q.topic);
        }
    }

    // Fairness: order topics by tenant WFQ score (lower = prefer).
    let mut topic_list: Vec<String> = topics.into_iter().collect();
    if let Some(fq) = fair_queue {
        topic_list.sort_by_key(|t| {
            let tenant = broker.tenant();
            fq.schedule_score(&format!("{tenant}:{t}"))
        });
    } else {
        topic_list.sort();
    }

    'outer: for topic in topic_list {
        if broker_partition::is_dlq_topic(&topic) {
            continue;
        }
        let Ok(pc) = broker.partition_count(&topic) else {
            continue;
        };
        for partition in 0..pc {
            if let Some(want) = partition_filter {
                if want != partition {
                    continue;
                }
            }
            if !is_leader(partition) {
                continue;
            }
            let tenant = broker.tenant();
            // Prefer queue/lane cursor when we can peek the first message; fall back to topic.
            let peek = broker
                .list_topic_messages_from(&topic, partition, 0, 1)
                .ok()
                .and_then(|m| m.into_iter().next());
            let cursor_key = peek
                .as_ref()
                .map(cursor_key_for)
                .unwrap_or_else(|| topic.clone());
            let cursor = broker
                .dispatch_offset(&tenant, &cursor_key, partition)
                .unwrap_or(0);
            let Ok(batch) = broker.list_topic_messages_from(&topic, partition, cursor, 32) else {
                continue;
            };
            for msg in batch {
                if jobs.len() >= max {
                    break 'outer;
                }
                if leases.is_offset_leased(&topic, partition, msg.offset) {
                    continue;
                }
                if msg.offset < cursor {
                    continue;
                }
                let ck = cursor_key_for(&msg);
                let lease_id = Uuid::new_v4();
                let claimed = ClaimedJob {
                    lease_id,
                    topic: topic.clone(),
                    partition,
                    offset: msg.offset,
                    message_id: msg.id,
                    expires_at_ms: now + ttl,
                    generation: generation(partition),
                    cursor_key: ck,
                    message: Some(msg),
                };
                leases.insert(holder.to_string(), claimed.clone());
                jobs.push(claimed);
            }
        }
    }

    ClaimResponse { jobs }
}

/// HTTP client used by `--dispatch-fleet` to claim from brokers.
#[derive(Clone)]
pub struct LeaseClient {
    http: reqwest::Client,
    broker_urls: Vec<String>,
    holder: String,
}

impl LeaseClient {
    pub fn from_env() -> Option<Self> {
        let urls = std::env::var("BETTERMQ_BROKER_URLS").ok()?;
        let broker_urls: Vec<String> = urls
            .split(',')
            .map(|s| s.trim().trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if broker_urls.is_empty() {
            return None;
        }
        let holder = std::env::var("BETTERMQ_FLEET_HOLDER")
            .unwrap_or_else(|_| format!("fleet-{}", Uuid::new_v4()));
        let timeout_secs = long_wait_tier_secs()
            .or_else(|| {
                std::env::var("BETTERMQ_LONG_HTTP_TIMEOUT_SECS")
                    .ok()
                    .and_then(|s| s.parse().ok())
            })
            .unwrap_or(30);
        Some(Self {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(timeout_secs.max(30)))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("lease client"),
            broker_urls,
            holder,
        })
    }

    pub fn holder(&self) -> &str {
        &self.holder
    }

    pub fn broker_urls(&self) -> &[String] {
        &self.broker_urls
    }

    pub fn http(&self) -> &reqwest::Client {
        &self.http
    }

    fn with_secret(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Ok(secret) = std::env::var("BETTERMQ_CLUSTER_SECRET") {
            if !secret.trim().is_empty() {
                return req.header("x-bettermq-cluster-secret", secret);
            }
        }
        req
    }

    pub async fn claim(&self, broker: &str, max_jobs: usize) -> Result<ClaimResponse, String> {
        let url = format!("{broker}/internal/v1/lease/claim");
        let body = ClaimRequest {
            holder: self.holder.clone(),
            max_jobs,
            lease_ttl_ms: default_ttl_ms(),
            partition: None,
        };
        let resp = self
            .with_secret(self.http.post(&url).json(&body))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !resp.status().is_success() {
            return Err(format!(
                "claim {}: {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            ));
        }
        resp.json().await.map_err(|e| e.to_string())
    }

    pub async fn heartbeat(&self, broker: &str, lease_id: Uuid) -> Result<(), String> {
        let url = format!("{broker}/internal/v1/lease/heartbeat");
        let body = HeartbeatRequest {
            lease_id,
            holder: self.holder.clone(),
            lease_ttl_ms: default_ttl_ms(),
        };
        let resp = self
            .with_secret(self.http.post(&url).json(&body))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !resp.status().is_success() {
            return Err(format!("heartbeat {}", resp.status()));
        }
        Ok(())
    }

    pub async fn complete(&self, broker: &str, lease_id: Uuid) -> Result<(), String> {
        let url = format!("{broker}/internal/v1/lease/complete");
        let body = CompleteRequest {
            lease_id,
            holder: self.holder.clone(),
        };
        let resp = self
            .with_secret(self.http.post(&url).json(&body))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !resp.status().is_success() {
            return Err(format!("complete {}", resp.status()));
        }
        Ok(())
    }

    pub async fn fail(
        &self,
        broker: &str,
        lease_id: Uuid,
        reason: &str,
        dead_letter: bool,
    ) -> Result<(), String> {
        let url = format!("{broker}/internal/v1/lease/fail");
        let body = FailRequest {
            lease_id,
            holder: self.holder.clone(),
            reason: reason.to_string(),
            dead_letter,
        };
        let resp = self
            .with_secret(self.http.post(&url).json(&body))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !resp.status().is_success() {
            return Err(format!("fail {}", resp.status()));
        }
        Ok(())
    }
}

/// Fleet concurrency (jobs in flight per fleet process).
pub fn fleet_concurrency() -> usize {
    std::env::var("BETTERMQ_FLEET_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
        .max(1)
}

/// Long-wait timeout tiers for fleet (Phase E). Env BETTERMQ_LONG_WAIT_TIER=15m|1h|6h|12h
pub fn long_wait_tier_secs() -> Option<u64> {
    let tier = std::env::var("BETTERMQ_LONG_WAIT_TIER").ok()?;
    match tier.trim().to_lowercase().as_str() {
        "15m" | "15" => Some(15 * 60),
        "1h" | "60m" => Some(60 * 60),
        "6h" => Some(6 * 60 * 60),
        "12h" => Some(12 * 60 * 60),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_insert_heartbeat_take() {
        let table = LeaseTable::new();
        let id = Uuid::new_v4();
        let job = ClaimedJob {
            lease_id: id,
            topic: "t".into(),
            partition: 0,
            offset: 1,
            message_id: Uuid::new_v4(),
            expires_at_ms: Utc::now().timestamp_millis() + 60_000,
            generation: 1,
            cursor_key: "t".into(),
            message: None,
        };
        table.insert("w1".into(), job);
        assert!(table.is_offset_leased("t", 0, 1));
        assert!(table.heartbeat(id, "w1", 30_000).is_ok());
        assert!(table.heartbeat(id, "other", 30_000).is_err());
        assert!(table.take(id, "w1").is_ok());
        assert!(!table.is_offset_leased("t", 0, 1));
    }

    #[test]
    fn long_wait_tiers() {
        std::env::set_var("BETTERMQ_LONG_WAIT_TIER", "1h");
        assert_eq!(long_wait_tier_secs(), Some(3600));
        std::env::remove_var("BETTERMQ_LONG_WAIT_TIER");
    }
}
