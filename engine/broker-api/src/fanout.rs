//! Durable, versioned fan-out acceptance and per-member outcomes.

use broker_partition::{PublishRequest, PublishResponse};
use broker_raft_meta::ControllerCommand;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use thiserror::Error;
use uuid::Uuid;

const FANOUT_STATE_VERSION: u16 = 1;
const FANOUT_COMPACT_OPS: usize = 1024;
const FANOUT_COMPACT_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum FanoutStoreError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("unsupported fanout state version: {0}")]
    UnsupportedVersion(u16),
    #[error("fanout command not found: {0}")]
    NotFound(Uuid),
    #[error("fanout authority: {0}")]
    Authority(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanoutMemberCommand {
    pub member_id: Uuid,
    pub request: PublishRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanoutMemberOutcome {
    #[serde(default)]
    pub response: Option<PublishResponse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub attempts: u32,
    #[serde(default)]
    pub next_attempt_at_ms: Option<i64>,
    #[serde(default)]
    pub terminal: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanoutCommand {
    pub id: Uuid,
    #[serde(default)]
    pub acceptance_hash: String,
    #[serde(default)]
    pub tenant_id: String,
    pub group_id: Uuid,
    pub created_at_ms: i64,
    #[serde(default)]
    pub updated_at_ms: i64,
    pub members: Vec<FanoutMemberCommand>,
    #[serde(default)]
    pub outcomes: HashMap<Uuid, FanoutMemberOutcome>,
    #[serde(default)]
    pub completed: bool,
    #[serde(default)]
    pub completed_at_ms: Option<i64>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct FanoutSnapshot {
    #[serde(default)]
    sequence: u64,
    #[serde(default)]
    commands: HashMap<Uuid, FanoutCommand>,
    #[serde(skip)]
    journal_ops: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum FanoutEvent {
    Accept {
        fanout: FanoutCommand,
    },
    RecordResult {
        fanout_id: Uuid,
        member_id: Uuid,
        outcome: FanoutMemberOutcome,
        #[serde(default)]
        updated_at_ms: i64,
    },
    Replace {
        fanout: FanoutCommand,
    },
    GarbageCollect {
        ids: Vec<Uuid>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FanoutJournalRecord {
    version: u16,
    sequence: u64,
    event: FanoutEvent,
}

#[derive(Debug, Clone)]
pub struct FanoutStore {
    snapshot_path: PathBuf,
    journal_path: PathBuf,
}

static ACTIVE_FANOUTS: OnceLock<parking_lot::Mutex<HashSet<Uuid>>> = OnceLock::new();

pub(crate) struct FanoutExecutionGuard {
    id: Uuid,
}

impl Drop for FanoutExecutionGuard {
    fn drop(&mut self) {
        ACTIVE_FANOUTS
            .get_or_init(Default::default)
            .lock()
            .remove(&self.id);
    }
}

pub(crate) fn try_begin_execution(id: Uuid) -> Option<FanoutExecutionGuard> {
    ACTIVE_FANOUTS
        .get_or_init(Default::default)
        .lock()
        .insert(id)
        .then_some(FanoutExecutionGuard { id })
}

impl FanoutStore {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, FanoutStoreError> {
        let dir = std::env::var("BETTERMQ_SHARED_META_DIR")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| data_dir.as_ref().to_path_buf());
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            snapshot_path: dir.join("fanout-state.json"),
            journal_path: dir.join("fanout-state.journal"),
        })
    }

    pub fn accept(&self, fanout: FanoutCommand) -> Result<(FanoutCommand, bool), FanoutStoreError> {
        self.mutate(|state| {
            if let Some(existing) = state.commands.get(&fanout.id) {
                return Ok((existing.clone(), None));
            }
            Ok((
                fanout.clone(),
                Some(FanoutEvent::Accept {
                    fanout: fanout.clone(),
                }),
            ))
        })
    }

    pub fn record_result(
        &self,
        fanout_id: Uuid,
        member_id: Uuid,
        outcome: FanoutMemberOutcome,
    ) -> Result<FanoutCommand, FanoutStoreError> {
        self.mutate(|state| {
            if !state.commands.contains_key(&fanout_id) {
                return Err(FanoutStoreError::NotFound(fanout_id));
            }
            Ok((
                (),
                Some(FanoutEvent::RecordResult {
                    fanout_id,
                    member_id,
                    outcome,
                    updated_at_ms: chrono::Utc::now().timestamp_millis(),
                }),
            ))
        })?;
        self.get(fanout_id)?
            .ok_or(FanoutStoreError::NotFound(fanout_id))
    }

    pub fn replace(&self, fanout: FanoutCommand) -> Result<FanoutCommand, FanoutStoreError> {
        self.mutate(|_| {
            Ok((
                fanout.clone(),
                Some(FanoutEvent::Replace {
                    fanout: fanout.clone(),
                }),
            ))
        })?;
        self.get(fanout.id)?
            .ok_or(FanoutStoreError::NotFound(fanout.id))
    }

    pub fn pending(&self, limit: usize) -> Result<Vec<FanoutCommand>, FanoutStoreError> {
        let _lock = broker_storage::FileLock::exclusive(&self.journal_path)?;
        let state = self.load_state()?;
        let mut commands: Vec<_> = state
            .commands
            .into_values()
            .filter(|command| !command.completed)
            .collect();
        commands.sort_by_key(|command| (command.updated_at_ms, command.created_at_ms, command.id));
        commands.truncate(limit);
        Ok(commands)
    }

    pub fn garbage_collect(
        &self,
        now_ms: i64,
        retention_ms: i64,
        max_completed: usize,
    ) -> Result<usize, FanoutStoreError> {
        let (removed, _) = self.mutate(|state| {
            let cutoff = now_ms.saturating_sub(retention_ms.max(0));
            let mut completed: Vec<_> = state
                .commands
                .values()
                .filter(|command| command.completed)
                .collect();
            completed.sort_by_key(|command| {
                Reverse((
                    command.completed_at_ms.unwrap_or(command.updated_at_ms),
                    command.id,
                ))
            });
            let ids: Vec<_> = completed
                .into_iter()
                .enumerate()
                .filter(|(index, command)| {
                    *index >= max_completed
                        || command.completed_at_ms.unwrap_or(command.updated_at_ms) <= cutoff
                })
                .map(|(_, command)| command.id)
                .collect();
            let count = ids.len();
            Ok((
                count,
                (!ids.is_empty()).then_some(FanoutEvent::GarbageCollect { ids }),
            ))
        })?;
        Ok(removed)
    }

    pub fn get(&self, fanout_id: Uuid) -> Result<Option<FanoutCommand>, FanoutStoreError> {
        let _lock = broker_storage::FileLock::exclusive(&self.journal_path)?;
        let state = self.load_state()?;
        Ok(state.commands.get(&fanout_id).cloned())
    }

    fn mutate<T>(
        &self,
        mutation: impl FnOnce(&FanoutSnapshot) -> Result<(T, Option<FanoutEvent>), FanoutStoreError>,
    ) -> Result<(T, bool), FanoutStoreError> {
        let _lock = broker_storage::FileLock::exclusive(&self.journal_path)?;
        let mut state = self.load_state()?;
        let (result, event) = mutation(&state)?;
        let Some(event) = event else {
            return Ok((result, false));
        };
        state.sequence = state.sequence.saturating_add(1);
        let record = FanoutJournalRecord {
            version: FANOUT_STATE_VERSION,
            sequence: state.sequence,
            event: event.clone(),
        };
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.journal_path)?;
        serde_json::to_writer(&mut file, &record)?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        broker_storage::set_secret_file_mode(&self.journal_path);
        apply_event(&mut state, event);
        state.journal_ops += 1;
        self.maybe_compact(&mut state)?;
        Ok((result, true))
    }

    fn load_state(&self) -> Result<FanoutSnapshot, FanoutStoreError> {
        let mut state = if self.snapshot_path.exists() {
            serde_json::from_slice(&std::fs::read(&self.snapshot_path)?)?
        } else {
            FanoutSnapshot::default()
        };
        if !self.journal_path.exists() {
            return Ok(state);
        }
        let bytes = std::fs::read(&self.journal_path)?;
        let lines: Vec<&[u8]> = bytes.split(|byte| *byte == b'\n').collect();
        for (index, line) in lines.iter().enumerate() {
            if line.iter().all(|byte| byte.is_ascii_whitespace()) {
                continue;
            }
            let record = match serde_json::from_slice::<FanoutJournalRecord>(line) {
                Ok(record) => record,
                Err(error) if index + 1 == lines.len() => {
                    tracing::warn!(%error, "ignoring torn fanout journal tail");
                    break;
                }
                Err(error) => return Err(FanoutStoreError::Serde(error)),
            };
            if record.version != FANOUT_STATE_VERSION {
                return Err(FanoutStoreError::UnsupportedVersion(record.version));
            }
            if record.sequence <= state.sequence {
                continue;
            }
            state.sequence = record.sequence;
            apply_event(&mut state, record.event);
            state.journal_ops += 1;
        }
        Ok(state)
    }

    fn maybe_compact(&self, state: &mut FanoutSnapshot) -> Result<(), FanoutStoreError> {
        let journal_bytes = self
            .journal_path
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        if state.journal_ops < FANOUT_COMPACT_OPS && journal_bytes < FANOUT_COMPACT_BYTES {
            return Ok(());
        }
        let bytes = serde_json::to_vec(state)?;
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

pub async fn accept_authoritative(
    state: &crate::AppState,
    store: &FanoutStore,
    command: FanoutCommand,
) -> Result<FanoutCommand, FanoutStoreError> {
    let Some(controller) = state
        .cluster
        .as_ref()
        .and_then(|cluster| cluster.runtime.controller())
    else {
        return store.accept(command).map(|accepted| accepted.0);
    };
    if let Some(existing) = controller.state().fanout_outbox.get(&command.id) {
        if existing.tenant_id != command.tenant_id {
            return Err(FanoutStoreError::Authority(
                "fanout id belongs to another tenant".into(),
            ));
        }
        let authoritative: FanoutCommand = serde_json::from_str(&existing.payload_json)?;
        store.replace(authoritative.clone())?;
        return Ok(authoritative);
    }
    let payload_json = serde_json::to_string(&command)?;
    let committed = controller
        .submit(ControllerCommand::AcceptFanout {
            id: command.id,
            tenant_id: command.tenant_id.clone(),
            acceptance_hash: command.acceptance_hash.clone(),
            payload_json,
            created_at_ms: command.created_at_ms,
            controller_term: 0,
        })
        .await
        .map_err(|error| FanoutStoreError::Authority(error.to_string()))?;
    let entry = committed
        .fanout_outbox
        .get(&command.id)
        .ok_or(FanoutStoreError::NotFound(command.id))?;
    let authoritative: FanoutCommand = serde_json::from_str(&entry.payload_json)?;
    store.replace(authoritative.clone())?;
    Ok(authoritative)
}

pub async fn claim_authority(
    state: &crate::AppState,
    store: &FanoutStore,
    fanout_id: Uuid,
) -> Result<Option<FanoutCommand>, FanoutStoreError> {
    let Some(cluster) = state.cluster.as_ref() else {
        return store.get(fanout_id);
    };
    let Some(controller) = cluster.runtime.controller() else {
        return store.get(fanout_id);
    };
    let snapshot = controller.state();
    let entry = snapshot
        .fanout_outbox
        .get(&fanout_id)
        .ok_or(FanoutStoreError::NotFound(fanout_id))?;
    if entry.completed {
        let command: FanoutCommand = serde_json::from_str(&entry.payload_json)?;
        store.replace(command.clone())?;
        return Ok(Some(command));
    }
    let owner_id = cluster.runtime.config().node_id;
    let committed = match controller
        .submit(ControllerCommand::ClaimFanout {
            id: fanout_id,
            owner_id,
            expected_version: entry.version,
            now_ms: 0,
            ttl_ms: 120_000,
            controller_term: 0,
        })
        .await
    {
        Ok(state) => state,
        Err(error) if error.to_string().contains("leased by another worker") => return Ok(None),
        Err(error) if error.to_string().contains("version fence failed") => return Ok(None),
        Err(error) => return Err(FanoutStoreError::Authority(error.to_string())),
    };
    let entry = committed
        .fanout_outbox
        .get(&fanout_id)
        .ok_or(FanoutStoreError::NotFound(fanout_id))?;
    if entry.owner_id != Some(owner_id) {
        return Ok(None);
    }
    let command: FanoutCommand = serde_json::from_str(&entry.payload_json)?;
    store.replace(command.clone())?;
    Ok(Some(command))
}

pub async fn update_authoritative(
    state: &crate::AppState,
    store: &FanoutStore,
    command: FanoutCommand,
) -> Result<FanoutCommand, FanoutStoreError> {
    let command = store.replace(command)?;
    let Some(cluster) = state.cluster.as_ref() else {
        return Ok(command);
    };
    let Some(controller) = cluster.runtime.controller() else {
        return Ok(command);
    };
    let owner_id = cluster.runtime.config().node_id;
    let entry = controller
        .state()
        .fanout_outbox
        .get(&command.id)
        .cloned()
        .ok_or(FanoutStoreError::NotFound(command.id))?;
    if entry.owner_id != Some(owner_id) {
        return Err(FanoutStoreError::Authority(
            "fanout ownership changed before update".into(),
        ));
    }
    let committed = controller
        .submit(ControllerCommand::UpdateFanout {
            id: command.id,
            owner_id,
            expected_version: entry.version,
            payload_json: serde_json::to_string(&command)?,
            completed: command.completed,
            now_ms: 0,
            controller_term: 0,
        })
        .await
        .map_err(|error| FanoutStoreError::Authority(error.to_string()))?;
    let entry = committed
        .fanout_outbox
        .get(&command.id)
        .ok_or(FanoutStoreError::NotFound(command.id))?;
    let authoritative: FanoutCommand = serde_json::from_str(&entry.payload_json)?;
    store.replace(authoritative.clone())?;
    Ok(authoritative)
}

pub fn pending_authoritative(
    state: &crate::AppState,
    store: &FanoutStore,
    limit: usize,
) -> Result<Vec<FanoutCommand>, FanoutStoreError> {
    let Some(controller) = state
        .cluster
        .as_ref()
        .and_then(|cluster| cluster.runtime.controller())
    else {
        return store.pending(limit);
    };
    let mut pending: Vec<FanoutCommand> = Vec::new();
    for entry in controller.state().fanout_outbox.into_values() {
        if entry.completed {
            continue;
        }
        pending.push(serde_json::from_str(&entry.payload_json)?);
    }
    pending.sort_by_key(|command| (command.updated_at_ms, command.created_at_ms, command.id));
    pending.truncate(limit);
    Ok(pending)
}

pub async fn garbage_collect_authoritative(
    state: &crate::AppState,
    store: &FanoutStore,
    now_ms: i64,
    retention_ms: i64,
    max_completed: usize,
) -> Result<usize, FanoutStoreError> {
    let local_removed = store.garbage_collect(now_ms, retention_ms, max_completed)?;
    let Some(controller) = state
        .cluster
        .as_ref()
        .and_then(|cluster| cluster.runtime.controller())
    else {
        return Ok(local_removed);
    };
    let cutoff = now_ms.saturating_sub(retention_ms.max(0));
    let mut completed: Vec<_> = controller
        .state()
        .fanout_outbox
        .into_values()
        .filter(|entry| entry.completed)
        .collect();
    completed.sort_by_key(|entry| Reverse((entry.updated_at_ms, entry.id)));
    let ids: Vec<_> = completed
        .into_iter()
        .enumerate()
        .filter(|(index, entry)| *index >= max_completed || entry.updated_at_ms <= cutoff)
        .map(|(_, entry)| entry.id)
        .collect();
    if ids.is_empty() {
        return Ok(local_removed);
    }
    controller
        .submit(ControllerCommand::GarbageCollectFanout {
            ids: ids.clone(),
            completed_before_ms: cutoff,
            controller_term: 0,
        })
        .await
        .map_err(|error| FanoutStoreError::Authority(error.to_string()))?;
    Ok(local_removed.saturating_add(ids.len()))
}

fn apply_event(state: &mut FanoutSnapshot, event: FanoutEvent) {
    match event {
        FanoutEvent::Accept { fanout } => {
            state.commands.entry(fanout.id).or_insert(fanout);
        }
        FanoutEvent::RecordResult {
            fanout_id,
            member_id,
            outcome,
            updated_at_ms,
        } => {
            if let Some(fanout) = state.commands.get_mut(&fanout_id) {
                fanout.outcomes.insert(member_id, outcome);
                fanout.updated_at_ms = updated_at_ms;
                fanout.completed = fanout.members.iter().all(|member| {
                    fanout
                        .outcomes
                        .get(&member.member_id)
                        .is_some_and(|outcome| outcome.response.is_some() || outcome.terminal)
                });
                if fanout.completed && fanout.completed_at_ms.is_none() {
                    fanout.completed_at_ms = Some(fanout.updated_at_ms);
                }
            }
        }
        FanoutEvent::Replace { fanout } => {
            state.commands.insert(fanout.id, fanout);
        }
        FanoutEvent::GarbageCollect { ids } => {
            state.commands.retain(|id, _| !ids.contains(id));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> PublishRequest {
        PublishRequest {
            topic: "__direct".into(),
            queue_id: None,
            group_id: None,
            group_member_id: Some(Uuid::new_v4()),
            routing_key: "rk".into(),
            payload: "body".into(),
            payload_encoding: None,
            idempotency_key: Some("fanout-member".into()),
            delay_ms: None,
            priority: None,
            flow_id: None,
            url: Some("https://example.com/hook".into()),
            secret: Some("secret".into()),
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

    #[test]
    fn accepted_command_and_member_result_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let store = FanoutStore::open(dir.path()).unwrap();
        let fanout_id = Uuid::new_v4();
        let member_id = Uuid::new_v4();
        let command = FanoutCommand {
            id: fanout_id,
            acceptance_hash: "test".into(),
            tenant_id: "tenant-a".into(),
            group_id: Uuid::new_v4(),
            created_at_ms: 1,
            updated_at_ms: 1,
            members: vec![FanoutMemberCommand {
                member_id,
                request: request(),
            }],
            outcomes: HashMap::new(),
            completed: false,
            completed_at_ms: None,
        };
        assert!(store.accept(command.clone()).unwrap().1);
        store
            .record_result(
                fanout_id,
                member_id,
                FanoutMemberOutcome {
                    response: Some(PublishResponse {
                        message_id: Some(Uuid::new_v4()),
                        topic: "__direct".into(),
                        partition: Some(0),
                        offset: Some(7),
                        duplicate: false,
                        scheduled: None,
                        commit_epoch: Some(8),
                        replication_frame: None,
                    }),
                    error: None,
                    attempts: 1,
                    next_attempt_at_ms: None,
                    terminal: false,
                },
            )
            .unwrap();
        drop(store);

        let reopened = FanoutStore::open(dir.path()).unwrap();
        let restored = reopened.get(fanout_id).unwrap().unwrap();
        assert!(restored.completed);
        assert_eq!(restored.outcomes[&member_id].attempts, 1);
        let (_, inserted) = reopened.accept(command).unwrap();
        assert!(!inserted);
    }

    #[test]
    fn unknown_journal_version_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("fanout-state.journal"),
            br#"{"version":99,"sequence":1,"event":{"command":"record_result","fanout_id":"00000000-0000-0000-0000-000000000000","member_id":"00000000-0000-0000-0000-000000000000","outcome":{"attempts":1}}}"#,
        )
        .unwrap();
        let store = FanoutStore::open(dir.path()).unwrap();
        assert!(matches!(
            store.get(Uuid::nil()),
            Err(FanoutStoreError::UnsupportedVersion(99))
        ));
    }

    #[test]
    fn pending_replay_and_completed_retention_are_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let store = FanoutStore::open(dir.path()).unwrap();
        let member_id = Uuid::new_v4();
        let fanout = FanoutCommand {
            id: Uuid::new_v4(),
            acceptance_hash: "test".into(),
            tenant_id: "tenant".into(),
            group_id: Uuid::new_v4(),
            created_at_ms: 1,
            updated_at_ms: 1,
            members: vec![FanoutMemberCommand {
                member_id,
                request: request(),
            }],
            outcomes: HashMap::new(),
            completed: false,
            completed_at_ms: None,
        };
        store.accept(fanout.clone()).unwrap();
        assert_eq!(store.pending(10).unwrap().len(), 1);
        store
            .record_result(
                fanout.id,
                member_id,
                FanoutMemberOutcome {
                    response: Some(PublishResponse {
                        message_id: Some(Uuid::new_v4()),
                        topic: "__direct".into(),
                        partition: Some(0),
                        offset: Some(1),
                        duplicate: false,
                        scheduled: None,
                        commit_epoch: Some(2),
                        replication_frame: None,
                    }),
                    error: None,
                    attempts: 1,
                    next_attempt_at_ms: None,
                    terminal: false,
                },
            )
            .unwrap();
        assert!(store.pending(10).unwrap().is_empty());
        assert_eq!(store.garbage_collect(i64::MAX, 0, 0).unwrap(), 1);
        assert!(store.get(fanout.id).unwrap().is_none());
    }
}
