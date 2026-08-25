//! Delayed enqueue (`delay`) and recurring cron schedules.

mod cron;
mod persist;

use chrono::Utc;
pub use cron::{normalize_cron, CronError, CronJob, CronRegistry, ScheduleKind};
use parking_lot::Mutex;
use persist::{load_json_with_recovery, persist_json_atomic, JsonLoadSource, MetadataLoadError};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use tracing::info;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum ScheduleError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("delayed job not found: {0}")]
    NotFound(Uuid),
    #[error("unsupported delayed schedule version: {0}")]
    UnsupportedVersion(u16),
    #[error(transparent)]
    MetadataLoad(#[from] MetadataLoadError),
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
    journal_path: PathBuf,
    state: Arc<Mutex<ScheduleState>>,
}

#[derive(Default)]
struct ScheduleState {
    heap: BinaryHeap<HeapItem>,
    in_flight: HashMap<Uuid, HeapItem>,
    sequence: u64,
    journal_ops: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)] // Persisted operations are infrequent and mirror heap records.
enum JournalOp {
    Add { item: HeapItem },
    Remove { id: Uuid },
}

#[derive(Debug, Serialize, Deserialize)]
struct JournalRecord {
    version: u16,
    sequence: u64,
    op: JournalOp,
}

#[derive(Debug, Serialize, Deserialize)]
struct ScheduleSnapshot {
    version: u16,
    sequence: u64,
    items: Vec<HeapItem>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum ScheduleSnapshotCompat {
    Current(ScheduleSnapshot),
    Legacy(Vec<HeapItem>),
}

const SCHEDULE_STATE_VERSION: u16 = 1;
const JOURNAL_COMPACT_OPS: usize = 1024;

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
        let journal_path = path.with_extension("journal");
        let loaded = load_json_with_recovery(&path, || ScheduleSnapshotCompat::Legacy(Vec::new()))?;
        let (items, mut sequence) = match loaded.value {
            ScheduleSnapshotCompat::Current(snapshot) => {
                if snapshot.version != SCHEDULE_STATE_VERSION {
                    return Err(ScheduleError::UnsupportedVersion(snapshot.version));
                }
                (snapshot.items, snapshot.sequence)
            }
            ScheduleSnapshotCompat::Legacy(items) => (items, 0),
        };
        if !matches!(
            loaded.source,
            JsonLoadSource::Main | JsonLoadSource::Missing
        ) {
            info!(
                file = %path.display(),
                source = ?loaded.source,
                count = items.len(),
                "schedule queue recovered after metadata read failure"
            );
        }
        let mut heap: BinaryHeap<HeapItem> = items.into_iter().collect();
        let mut journal_ops = 0usize;
        if journal_path.exists() {
            let bytes = std::fs::read(&journal_path)?;
            let lines: Vec<&[u8]> = bytes.split(|b| *b == b'\n').collect();
            for (index, line) in lines.iter().enumerate() {
                if line.iter().all(|b| b.is_ascii_whitespace()) {
                    continue;
                }
                let record = match serde_json::from_slice::<JournalRecord>(line) {
                    Ok(record) => record,
                    Err(record_error) => match serde_json::from_slice::<JournalOp>(line) {
                        Ok(op) => JournalRecord {
                            version: SCHEDULE_STATE_VERSION,
                            sequence: sequence.saturating_add(1),
                            op,
                        },
                        Err(_) if index + 1 == lines.len() => {
                            tracing::warn!(%record_error, "ignoring torn schedule journal tail");
                            break;
                        }
                        Err(_) => return Err(ScheduleError::Serde(record_error)),
                    },
                };
                if record.version != SCHEDULE_STATE_VERSION {
                    return Err(ScheduleError::UnsupportedVersion(record.version));
                }
                if record.sequence <= sequence {
                    continue;
                }
                sequence = record.sequence;
                journal_ops += 1;
                match record.op {
                    JournalOp::Add { item } => {
                        let mut items: Vec<_> = heap.drain().filter(|i| i.id != item.id).collect();
                        items.push(item);
                        heap.extend(items);
                    }
                    JournalOp::Remove { id } => {
                        let items: Vec<_> = heap.drain().filter(|i| i.id != id).collect();
                        heap.extend(items);
                    }
                }
            }
        }

        Ok(Self {
            path,
            journal_path,
            state: Arc::new(Mutex::new(ScheduleState {
                heap,
                in_flight: HashMap::new(),
                sequence,
                journal_ops,
            })),
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
        let mut state = self.state.lock();
        self.append_op(&mut state, JournalOp::Add { item: item.clone() })?;
        state.heap.push(item);
        state.journal_ops += 1;
        self.maybe_compact(&mut state);
        Ok(ScheduledPublish {
            id,
            deliver_at_ms,
            request,
        })
    }

    pub fn list(&self) -> Vec<ScheduledPublish> {
        let state = self.state.lock();
        state
            .heap
            .iter()
            .chain(state.in_flight.values())
            .map(|item| ScheduledPublish {
                id: item.id,
                deliver_at_ms: item.deliver_at_ms,
                request: item.request.clone(),
            })
            .collect()
    }

    pub fn cancel(&self, id: Uuid) -> Result<ScheduledPublish, ScheduleError> {
        let mut state = self.state.lock();
        let drained: Vec<HeapItem> = state.heap.drain().collect();
        let (mut hit, rest): (Vec<HeapItem>, Vec<HeapItem>) =
            drained.into_iter().partition(|i| i.id == id);
        for item in rest {
            state.heap.push(item);
        }
        let removed = hit
            .pop()
            .or_else(|| state.in_flight.remove(&id))
            .ok_or(ScheduleError::NotFound(id))?;
        if let Err(error) = self.append_op(&mut state, JournalOp::Remove { id }) {
            state.heap.push(removed);
            return Err(error);
        }
        state.journal_ops += 1;
        self.maybe_compact(&mut state);
        Ok(ScheduledPublish {
            id: removed.id,
            deliver_at_ms: removed.deliver_at_ms,
            request: removed.request,
        })
    }

    /// Pop due jobs from the in-memory heap **without** persisting.
    /// Persist only via [`Self::complete`] after a successful fire so a crash
    /// before publish still reloads the job from disk (at-least-once).
    pub fn pop_due(&self, now_ms: i64) -> Vec<ScheduledPublish> {
        let mut state = self.state.lock();
        let mut popped = Vec::new();
        while let Some(top) = state.heap.peek() {
            if top.deliver_at_ms > now_ms {
                break;
            }
            let item = state.heap.pop().expect("peeked");
            state.in_flight.insert(item.id, item.clone());
            popped.push(ScheduledPublish {
                id: item.id,
                deliver_at_ms: item.deliver_at_ms,
                request: item.request,
            });
        }
        popped
    }

    /// Remove a fired job from durable storage after successful publish.
    pub fn complete(&self, id: Uuid) -> Result<(), ScheduleError> {
        let mut state = self.state.lock();
        self.append_op(&mut state, JournalOp::Remove { id })?;
        state.in_flight.remove(&id);
        state.journal_ops += 1;
        self.maybe_compact(&mut state);
        Ok(())
    }

    /// Put a failed fire back onto the heap. Its original durable Add remains
    /// in the journal until a successful completion appends Remove.
    pub fn requeue(&self, job: ScheduledPublish) -> Result<(), ScheduleError> {
        let mut state = self.state.lock();
        state.in_flight.remove(&job.id);
        state.heap.push(HeapItem {
            deliver_at_ms: job.deliver_at_ms,
            id: job.id,
            request: job.request,
        });
        Ok(())
    }

    fn append_op(&self, state: &mut ScheduleState, op: JournalOp) -> Result<(), ScheduleError> {
        if let Some(parent) = self.journal_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.journal_path)?;
        let sequence = state.sequence.saturating_add(1);
        let record = JournalRecord {
            version: SCHEDULE_STATE_VERSION,
            sequence,
            op,
        };
        serde_json::to_writer(&mut file, &record)?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        broker_storage::set_secret_file_mode(&self.journal_path);
        state.sequence = sequence;
        Ok(())
    }

    fn maybe_compact(&self, state: &mut ScheduleState) {
        if state.journal_ops < JOURNAL_COMPACT_OPS {
            return;
        }
        let items: Vec<_> = state
            .heap
            .iter()
            .chain(state.in_flight.values())
            .cloned()
            .collect();
        let snapshot = ScheduleSnapshot {
            version: SCHEDULE_STATE_VERSION,
            sequence: state.sequence,
            items,
        };
        let bytes = match serde_json::to_vec(&snapshot) {
            Ok(bytes) => bytes,
            Err(error) => {
                tracing::warn!(%error, "schedule journal compaction encode failed");
                return;
            }
        };
        if let Err(error) = persist_json_atomic(&self.path, &bytes) {
            tracing::warn!(%error, "schedule journal compaction snapshot failed");
            return;
        }
        broker_storage::set_secret_file_mode(&self.path);
        match OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.journal_path)
            .and_then(|file| file.sync_data())
        {
            Ok(()) => state.journal_ops = 0,
            Err(error) => tracing::warn!(%error, "schedule journal compaction truncate failed"),
        }
    }
}

#[cfg(test)]
mod schedule_tests {
    use super::*;
    use crate::persist::env_test_lock::LOCK;
    use tempfile::tempdir;

    fn request() -> ScheduledPublishRequest {
        ScheduledPublishRequest {
            topic: "__direct".into(),
            routing_key: "rk".into(),
            payload: "body".into(),
            payload_encoding: None,
            idempotency_key: Some("scheduled-test".into()),
            priority: None,
            parallelism: None,
            flow_id: None,
            queue_id: None,
            destination: None,
            flow: None,
            max_retries: None,
            retry_backoff: None,
            method: None,
            headers: None,
            sign: None,
            request: None,
        }
    }

    #[test]
    fn open_fails_on_empty_corrupt_schedule_json_without_recovery_env() {
        let _guard = LOCK.lock().unwrap();
        unsafe { std::env::remove_var("BETTERMQ_METADATA_RECOVER") };
        let dir = tempdir().unwrap();
        let path = dir.path().join("schedule.json");
        std::fs::write(&path, b"").unwrap();
        assert!(ScheduleQueue::open(dir.path()).is_err());
    }

    #[test]
    fn open_recovers_empty_schedule_json_when_recovery_env_set() {
        let _guard = LOCK.lock().unwrap();
        unsafe { std::env::set_var("BETTERMQ_METADATA_RECOVER", "empty") };
        let dir = tempdir().unwrap();
        let path = dir.path().join("schedule.json");
        std::fs::write(&path, b"").unwrap();
        let queue = ScheduleQueue::open(dir.path()).unwrap();
        unsafe { std::env::remove_var("BETTERMQ_METADATA_RECOVER") };
        assert!(queue.list().is_empty());
    }

    #[test]
    fn append_journal_survives_requeue_and_completion() {
        let dir = tempdir().unwrap();
        let queue = ScheduleQueue::open(dir.path()).unwrap();
        let scheduled = queue.schedule(request(), 0).unwrap();
        assert!(dir.path().join("schedule.journal").exists());
        drop(queue);

        let queue = ScheduleQueue::open(dir.path()).unwrap();
        assert_eq!(queue.list().len(), 1);
        let pending = queue.pop_due(i64::MAX).pop().unwrap();
        queue.requeue(pending).unwrap();
        drop(queue);

        let queue = ScheduleQueue::open(dir.path()).unwrap();
        assert_eq!(queue.list().len(), 1);
        let pending = queue.pop_due(i64::MAX).pop().unwrap();
        assert_eq!(pending.id, scheduled.id);
        queue.complete(pending.id).unwrap();
        drop(queue);

        let queue = ScheduleQueue::open(dir.path()).unwrap();
        assert!(queue.list().is_empty());
    }

    #[test]
    fn unsupported_schedule_journal_version_fails_closed() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("schedule.json"), b"[]").unwrap();
        std::fs::write(
            dir.path().join("schedule.journal"),
            br#"{"version":99,"sequence":1,"op":{"op":"remove","id":"00000000-0000-0000-0000-000000000000"}}"#,
        )
        .unwrap();
        assert!(matches!(
            ScheduleQueue::open(dir.path()),
            Err(ScheduleError::UnsupportedVersion(99))
        ));
    }
}
