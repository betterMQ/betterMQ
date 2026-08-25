use crate::retry_state::RetryKey;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

const LIFECYCLE_VERSION: u16 = 1;
const COMPACT_OPS: usize = 1024;

#[derive(Debug, Error)]
pub(crate) enum LifecycleError {
    #[error("lifecycle io: {0}")]
    Io(#[from] std::io::Error),
    #[error("lifecycle serde: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("unsupported lifecycle version: {0}")]
    UnsupportedVersion(u16),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DlqPhase {
    Prepared,
    DlqCommitted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DlqTransition {
    pub source: RetryKey,
    pub cursor_key: String,
    pub reason: String,
    pub phase: DlqPhase,
    pub updated_at_ms: i64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Snapshot {
    #[serde(default)]
    sequence: u64,
    #[serde(default)]
    transitions: HashMap<RetryKey, DlqTransition>,
    #[serde(skip)]
    journal_ops: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum Event {
    Upsert { transition: DlqTransition },
    Remove { source: RetryKey },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record {
    version: u16,
    sequence: u64,
    event: Event,
}

#[derive(Clone)]
pub(crate) struct LifecycleStore {
    snapshot_path: PathBuf,
    journal_path: PathBuf,
    state: Arc<parking_lot::Mutex<Snapshot>>,
    io_lock: Arc<parking_lot::Mutex<()>>,
}

impl LifecycleStore {
    pub(crate) fn open(data_dir: &Path) -> Result<Self, LifecycleError> {
        let dir = data_dir.to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let snapshot_path = dir.join("dispatch-lifecycle.json");
        let journal_path = dir.join("dispatch-lifecycle.journal");
        let mut state = if snapshot_path.exists() {
            serde_json::from_slice(&std::fs::read(&snapshot_path)?)?
        } else {
            Snapshot::default()
        };
        if journal_path.exists() {
            let bytes = std::fs::read(&journal_path)?;
            let lines: Vec<&[u8]> = bytes.split(|byte| *byte == b'\n').collect();
            for (index, line) in lines.iter().enumerate() {
                if line.iter().all(|byte| byte.is_ascii_whitespace()) {
                    continue;
                }
                let record = match serde_json::from_slice::<Record>(line) {
                    Ok(record) => record,
                    Err(error) if index + 1 == lines.len() => {
                        tracing::warn!(%error, "ignoring torn lifecycle journal tail");
                        break;
                    }
                    Err(error) => return Err(error.into()),
                };
                if record.version != LIFECYCLE_VERSION {
                    return Err(LifecycleError::UnsupportedVersion(record.version));
                }
                if record.sequence <= state.sequence {
                    continue;
                }
                state.sequence = record.sequence;
                apply(&mut state, record.event);
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

    pub(crate) fn pending(&self) -> Vec<DlqTransition> {
        self.state.lock().transitions.values().cloned().collect()
    }

    pub(crate) fn prepare(&self, mut transition: DlqTransition) -> Result<(), LifecycleError> {
        if self
            .state
            .lock()
            .transitions
            .get(&transition.source)
            .is_some_and(|existing| existing.phase == DlqPhase::DlqCommitted)
        {
            transition.phase = DlqPhase::DlqCommitted;
        }
        self.write(Event::Upsert {
            transition: transition.clone(),
        })?;
        self.state
            .lock()
            .transitions
            .insert(transition.source.clone(), transition);
        self.maybe_compact()
    }

    pub(crate) fn mark_dlq_committed(&self, source: &RetryKey) -> Result<(), LifecycleError> {
        let Some(mut transition) = self.state.lock().transitions.get(source).cloned() else {
            return Ok(());
        };
        transition.phase = DlqPhase::DlqCommitted;
        transition.updated_at_ms = chrono::Utc::now().timestamp_millis();
        self.prepare(transition)
    }

    pub(crate) fn clear(&self, source: &RetryKey) -> Result<(), LifecycleError> {
        if !self.state.lock().transitions.contains_key(source) {
            return Ok(());
        }
        self.write(Event::Remove {
            source: source.clone(),
        })?;
        self.state.lock().transitions.remove(source);
        self.maybe_compact()
    }

    fn write(&self, event: Event) -> Result<(), LifecycleError> {
        let _guard = self.io_lock.lock();
        let sequence = self.state.lock().sequence.saturating_add(1);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.journal_path)?;
        serde_json::to_writer(
            &mut file,
            &Record {
                version: LIFECYCLE_VERSION,
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

    fn maybe_compact(&self) -> Result<(), LifecycleError> {
        let mut state = self.state.lock();
        if state.journal_ops < COMPACT_OPS {
            return Ok(());
        }
        broker_storage::atomic_write_file(&self.snapshot_path, &serde_json::to_vec(&*state)?)?;
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

fn apply(state: &mut Snapshot, event: Event) {
    match event {
        Event::Upsert { transition } => {
            state
                .transitions
                .insert(transition.source.clone(), transition);
        }
        Event::Remove { source } => {
            state.transitions.remove(&source);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn transition() -> DlqTransition {
        DlqTransition {
            source: RetryKey {
                topic: "orders".into(),
                partition: 0,
                offset: 7,
                message_id: Uuid::new_v4(),
            },
            cursor_key: "lane".into(),
            reason: "exhausted".into(),
            phase: DlqPhase::Prepared,
            updated_at_ms: 1,
        }
    }

    #[test]
    fn every_dlq_boundary_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let transition = transition();
        let source = transition.source.clone();
        let store = LifecycleStore::open(dir.path()).unwrap();
        store.prepare(transition).unwrap();
        drop(store);

        let store = LifecycleStore::open(dir.path()).unwrap();
        assert_eq!(store.pending()[0].phase, DlqPhase::Prepared);
        store.mark_dlq_committed(&source).unwrap();
        drop(store);

        let store = LifecycleStore::open(dir.path()).unwrap();
        assert_eq!(store.pending()[0].phase, DlqPhase::DlqCommitted);
        store.clear(&source).unwrap();
        drop(store);

        assert!(LifecycleStore::open(dir.path())
            .unwrap()
            .pending()
            .is_empty());
    }
}
