//! Delayed enqueue (`delay`) and recurring cron schedules.

mod cron;
mod persist;

use chrono::Utc;
pub use cron::{normalize_cron, CronError, CronJob, CronRegistry, ScheduleKind};
use parking_lot::Mutex;
use persist::{load_json_with_recovery, persist_json_atomic, JsonLoadSource};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum ScheduleError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("delayed job not found: {0}")]
    NotFound(Uuid),
}

fn deserialize_flexible_payload<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    match Value::deserialize(deserializer)? {
        Value::String(s) => Ok(s),
        other => serde_json::to_string(&other).map_err(serde::de::Error::custom),
    }
}

/// Payload waiting for delayed ingest (mirrors publish body fields).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledPublishRequest {
    pub topic: String,
    #[serde(default)]
    pub routing_key: String,
    #[serde(deserialize_with = "deserialize_flexible_payload")]
    pub payload: String,
    #[serde(default)]
    pub payload_encoding: Option<String>,
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub priority: Option<u8>,
    #[serde(default)]
    pub parallelism: Option<u32>,
    #[serde(default)]
    pub flow_id: Option<uuid::Uuid>,
    #[serde(default)]
    pub queue_id: Option<uuid::Uuid>,
    #[serde(default)]
    pub destination: Option<broker_partition::DestinationSnapshot>,
    /// Legacy — prefer `flow_id`.
    #[serde(default)]
    pub flow: Option<broker_partition::FlowSpec>,
    #[serde(default)]
    pub max_retries: Option<u32>,
    #[serde(default)]
    pub retry_backoff: Option<broker_proto::RetryBackoff>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(
        default,
        deserialize_with = "broker_partition::http_delivery::deserialize_optional_headers"
    )]
    pub headers: Option<std::collections::HashMap<String, String>>,
    #[serde(default)]
    pub sign: Option<bool>,
    #[serde(default)]
    pub request: Option<broker_partition::HttpDeliveryInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledPublish {
    pub id: Uuid,
    pub deliver_at_ms: i64,
    pub request: ScheduledPublishRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HeapItem {
    deliver_at_ms: i64,
    id: Uuid,
    request: ScheduledPublishRequest,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.deliver_at_ms == other.deliver_at_ms && self.id == other.id
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .deliver_at_ms
            .cmp(&self.deliver_at_ms)
            .then_with(|| other.id.cmp(&self.id))
    }
}

#[derive(Clone)]
pub struct ScheduleQueue {
    path: PathBuf,
    heap: Arc<Mutex<BinaryHeap<HeapItem>>>,
}

fn meta_file_path(data_dir: &std::path::Path, name: &str) -> std::path::PathBuf {
    if let Ok(shared) = std::env::var("BETTERMQ_SHARED_META_DIR") {
        let dir = std::path::PathBuf::from(shared);
        let _ = std::fs::create_dir_all(&dir);
        return dir.join(name);
    }
    data_dir.join(name)
}

impl ScheduleQueue {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, ScheduleError> {
        let path = meta_file_path(data_dir.as_ref(), "schedule.json");
        let loaded = load_json_with_recovery(&path, Vec::<HeapItem>::new);
        if loaded.source != JsonLoadSource::Main && loaded.source != JsonLoadSource::Default {
            info!(
                file = %path.display(),
                source = ?loaded.source,
                count = loaded.value.len(),
                "schedule queue recovered after metadata read failure"
            );
        }
        let heap = loaded.value.into_iter().collect();

        Ok(Self {
            path,
            heap: Arc::new(Mutex::new(heap)),
        })
    }

    pub fn schedule(
        &self,
        request: ScheduledPublishRequest,
        delay_ms: u64,
    ) -> Result<ScheduledPublish, ScheduleError> {
        let deliver_at_ms = Utc::now().timestamp_millis() + delay_ms as i64;
        let id = Uuid::new_v4();
        let item = HeapItem {
            deliver_at_ms,
            id,
            request: request.clone(),
        };
        self.heap.lock().push(item);
        self.persist()?;
        Ok(ScheduledPublish {
            id,
            deliver_at_ms,
            request,
        })
    }

    pub fn list(&self) -> Vec<ScheduledPublish> {
        let heap = self.heap.lock();
        heap.iter()
            .map(|item| ScheduledPublish {
                id: item.id,
                deliver_at_ms: item.deliver_at_ms,
                request: item.request.clone(),
            })
            .collect()
    }

    pub fn cancel(&self, id: Uuid) -> Result<ScheduledPublish, ScheduleError> {
        let mut heap = self.heap.lock();
        let drained: Vec<HeapItem> = heap.drain().collect();
        let (mut hit, rest): (Vec<HeapItem>, Vec<HeapItem>) =
            drained.into_iter().partition(|i| i.id == id);
        for item in rest {
            heap.push(item);
        }
        let removed = hit.pop().ok_or(ScheduleError::NotFound(id))?;
        drop(heap);
        self.persist()?;
        Ok(ScheduledPublish {
            id: removed.id,
            deliver_at_ms: removed.deliver_at_ms,
            request: removed.request,
        })
    }

    pub fn pop_due(&self, now_ms: i64) -> Vec<ScheduledPublishRequest> {
        let mut heap = self.heap.lock();
        let mut popped = Vec::new();
        while let Some(top) = heap.peek() {
            if top.deliver_at_ms > now_ms {
                break;
            }
            let item = heap.pop().expect("peeked");
            popped.push(item);
        }
        drop(heap);

        if popped.is_empty() {
            return Vec::new();
        }

        if let Err(e) = self.persist() {
            warn!(
                error = %e,
                count = popped.len(),
                "failed to persist schedule after pop_due; re-queued in memory"
            );
            let mut heap = self.heap.lock();
            for item in popped {
                heap.push(item);
            }
            return Vec::new();
        }

        popped.into_iter().map(|item| item.request).collect()
    }

    fn persist(&self) -> Result<(), ScheduleError> {
        let heap = self.heap.lock();
        let items: Vec<_> = heap.iter().cloned().collect();
        drop(heap);
        let bytes = serde_json::to_vec(&items)?;
        persist_json_atomic(&self.path, &bytes).map_err(ScheduleError::from)
    }
}

#[cfg(test)]
mod schedule_tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn open_recovers_empty_corrupt_schedule_json() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("schedule.json");
        std::fs::write(&path, b"").unwrap();
        let queue = ScheduleQueue::open(dir.path()).unwrap();
        assert!(queue.list().is_empty());
    }
}
