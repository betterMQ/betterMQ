use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

const RETRY_STATE_VERSION: u16 = 1;
const RETRY_COMPACT_OPS: usize = 1024;

#[derive(Debug, Error)]
pub(crate) enum RetryStateError {
    #[error("retry state io: {0}")]
    Io(#[from] std::io::Error),
    #[error("retry state serde: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("unsupported retry state version: {0}")]
    UnsupportedVersion(u16),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct RetryKey {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub message_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RetryRecord {
    pub key: RetryKey,
    pub cursor_key: String,
    pub attempts: u32,
    pub due_at_ms: i64,
    pub last_error: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct RetrySnapshot {
    #[serde(default)]
    sequence: u64,
    #[serde(default)]
    records: HashMap<RetryKey, RetryRecord>,
    #[serde(skip)]
    journal_ops: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum RetryEvent {
    Upsert { record: RetryRecord },
    Remove { key: RetryKey },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RetryJournalRecord {
    version: u16,
    sequence: u64,
    event: RetryEvent,
}

#[derive(Clone)]
pub(crate) struct RetryState {
    snapshot_path: PathBuf,
    journal_path: PathBuf,
    state: Arc<parking_lot::Mutex<RetrySnapshot>>,
    io_lock: Arc<parking_lot::Mutex<()>>,
}

impl RetryState {
    pub(crate) fn open(data_dir: &Path) -> Result<Self, RetryStateError> {
        let dir = data_dir.to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let snapshot_path = dir.join("dispatch-retries.json");
        let journal_path = dir.join("dispatch-retries.journal");
        let mut state = if snapshot_path.exists() {
            serde_json::from_slice(&std::fs::read(&snapshot_path)?)?
        } else {
            RetrySnapshot::default()
        };
        if journal_path.exists() {
            let bytes = std::fs::read(&journal_path)?;
            let lines: Vec<&[u8]> = bytes.split(|byte| *byte == b'\n').collect();
            for (index, line) in lines.iter().enumerate() {
                if line.iter().all(|byte| byte.is_ascii_whitespace()) {
                    continue;
                }
                let record = match serde_json::from_slice::<RetryJournalRecord>(line) {
                    Ok(record) => record,
                    Err(error) if index + 1 == lines.len() => {
                        tracing::warn!(%error, "ignoring torn retry journal tail");
                        break;
                    }
                    Err(error) => return Err(error.into()),
                };
                if record.version != RETRY_STATE_VERSION {
                    return Err(RetryStateError::UnsupportedVersion(record.version));
                }
                if record.sequence <= state.sequence {
                    continue;
                }
                state.sequence = record.sequence;
                apply_event(&mut state, record.event);
                state.journal_ops += 1;
            }
        }
        Ok(Self {
            snapshot_path,
            journal_path,
            state: Arc::new(parking_lot::Mutex::new(state)),
            io_lock: Arc::new(parking_lot::Mutex::new(())),
        })
    }

    pub(crate) fn get(&self, key: &RetryKey) -> Option<RetryRecord> {
        self.state.lock().records.get(key).cloned()
    }

    pub(crate) fn pending(&self) -> Vec<RetryRecord> {
        self.state.lock().records.values().cloned().collect()
    }

    pub(crate) fn blocking_retry(
        &self,
        cursor_key: &str,
        partition: u32,
        offset: u64,
    ) -> Option<RetryRecord> {
        self.state
            .lock()
            .records
            .values()
            .filter(|record| {
                record.cursor_key == cursor_key
                    && record.key.partition == partition
                    && record.key.offset <= offset
            })
            .min_by_key(|record| record.key.offset)
            .cloned()
    }

    pub(crate) fn schedule(&self, record: RetryRecord) -> Result<(), RetryStateError> {
        let _guard = self.io_lock.lock();
        self.append(RetryEvent::Upsert {
            record: record.clone(),
        })?;
        self.state.lock().records.insert(record.key.clone(), record);
        self.maybe_compact()
    }

    pub(crate) fn clear(&self, key: &RetryKey) -> Result<(), RetryStateError> {
        let _guard = self.io_lock.lock();
        if !self.state.lock().records.contains_key(key) {
            return Ok(());
        }
        self.append(RetryEvent::Remove { key: key.clone() })?;
        self.state.lock().records.remove(key);
        self.maybe_compact()
    }

    fn append(&self, event: RetryEvent) -> Result<(), RetryStateError> {
        let sequence = self.state.lock().sequence.saturating_add(1);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.journal_path)?;
        serde_json::to_writer(
            &mut file,
            &RetryJournalRecord {
                version: RETRY_STATE_VERSION,
                sequence,
                event,
            },
        )?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        broker_storage::set_secret_file_mode(&self.journal_path);
        let mut state = self.state.lock();
        state.sequence = sequence;
        state.journal_ops += 1;
        Ok(())
    }

    fn maybe_compact(&self) -> Result<(), RetryStateError> {
        let mut state = self.state.lock();
        if state.journal_ops < RETRY_COMPACT_OPS {
            return Ok(());
        }
        let bytes = serde_json::to_vec(&*state)?;
        broker_storage::atomic_write_file(&self.snapshot_path, &bytes)?;
        broker_storage::set_secret_file_mode(&self.snapshot_path);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.journal_path)?;
        file.sync_data()?;
        state.journal_ops = 0;
        Ok(())
    }
}

fn apply_event(state: &mut RetrySnapshot, event: RetryEvent) {
    match event {
        RetryEvent::Upsert { record } => {
            state.records.insert(record.key.clone(), record);
        }
        RetryEvent::Remove { key } => {
            state.records.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_deadline_and_attempt_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let key = RetryKey {
            topic: "orders".into(),
            partition: 2,
            offset: 9,
            message_id: Uuid::new_v4(),
        };
        let state = RetryState::open(dir.path()).unwrap();
        state
            .schedule(RetryRecord {
                key: key.clone(),
                cursor_key: "lane".into(),
                attempts: 2,
                due_at_ms: 42,
                last_error: "HTTP 503".into(),
            })
            .unwrap();
        drop(state);

        let reopened = RetryState::open(dir.path()).unwrap();
        let restored = reopened.get(&key).unwrap();
        assert_eq!(restored.attempts, 2);
        assert_eq!(restored.due_at_ms, 42);
        reopened.clear(&key).unwrap();
        drop(reopened);
        assert!(RetryState::open(dir.path()).unwrap().get(&key).is_none());
    }
}
