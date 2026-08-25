//! Flow-control profiles (rate, parallelism, grouping key) — separate from queues.

use crate::catalog_journal::CatalogJournal;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum FlowProfileError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("flow profile not found: {0}")]
    NotFound(Uuid),
    #[error("duplicate flow {0}")]
    Duplicate(Uuid),
    #[error("unsupported flow catalog version: {0}")]
    UnsupportedVersion(u16),
    #[error("flow catalog journal: {0}")]
    Journal(String),
}

/// Named flow-control policy referenced by `flow_id` on publish / cron (not enqueue).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FlowProfile {
    pub id: Uuid,
    pub tenant_id: String,
    /// Grouping key for parallelism + rate.
    pub key: String,
    pub parallelism: u32,
    pub rate: u32,
    pub period_secs: u64,
    /// Last catalog mutation time (ms since epoch) for cluster LWW merge (CP6c).
    #[serde(default)]
    pub updated_at_ms: i64,
}

impl FlowProfile {
    pub fn to_spec(&self) -> crate::flow::FlowSpec {
        crate::flow::FlowSpec {
            key: Some(self.key.clone()),
            parallelism: Some(self.parallelism),
            rate: Some(self.rate),
            period_secs: Some(self.period_secs),
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct FlowFile {
    #[serde(default)]
    version: u16,
    #[serde(default)]
    revision: u64,
    profiles: Vec<FlowProfile>,
}

const FLOW_CATALOG_VERSION: u16 = 1;
const FLOW_COMPACT_OPS: usize = 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum FlowCommand {
    Patch {
        upserts: Vec<FlowProfile>,
        deletes: Vec<Uuid>,
    },
}

fn apply_command(file: &mut FlowFile, command: FlowCommand) {
    match command {
        FlowCommand::Patch { upserts, deletes } => {
            file.profiles
                .retain(|profile| !deletes.contains(&profile.id));
            for profile in upserts {
                if let Some(slot) = file
                    .profiles
                    .iter_mut()
                    .find(|existing| existing.id == profile.id)
                {
                    *slot = profile;
                } else {
                    file.profiles.push(profile);
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct FlowProfileRegistry {
    path: PathBuf,
    journal: CatalogJournal,
}

fn meta_file_path(data_dir: &Path, name: &str) -> PathBuf {
    if let Ok(shared) = std::env::var("BETTERMQ_SHARED_META_DIR") {
        let shared = shared.trim();
        if !shared.is_empty() {
            let dir = PathBuf::from(shared);
            let _ = std::fs::create_dir_all(&dir);
            return dir.join(name);
        }
    }
    data_dir.join(name)
}

impl FlowProfileRegistry {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, FlowProfileError> {
        let path = meta_file_path(data_dir.as_ref(), "flows.json");
        if !path.exists() {
            let file = FlowFile {
                version: FLOW_CATALOG_VERSION,
                ..FlowFile::default()
            };
            std::fs::write(&path, serde_json::to_vec_pretty(&file)?)?;
        }
        let journal = CatalogJournal::new(&path, FLOW_CATALOG_VERSION);
        Ok(Self { path, journal })
    }

    fn load(&self) -> Result<FlowFile, FlowProfileError> {
        self.load_state().map(|state| state.0)
    }

    fn load_state(&self) -> Result<(FlowFile, usize), FlowProfileError> {
        let bytes = std::fs::read(&self.path)?;
        let mut file: FlowFile = serde_json::from_slice(&bytes)?;
        if file.version != 0 && file.version != FLOW_CATALOG_VERSION {
            return Err(FlowProfileError::UnsupportedVersion(file.version));
        }
        file.version = FLOW_CATALOG_VERSION;
        let (revision, operations) = self
            .journal
            .replay(file.revision, |command| apply_command(&mut file, command))
            .map_err(|error| FlowProfileError::Journal(error.to_string()))?;
        file.revision = revision;
        Ok((file, operations))
    }

    fn mutate<T>(
        &self,
        f: impl FnOnce(&mut FlowFile) -> Result<T, FlowProfileError>,
    ) -> Result<T, FlowProfileError> {
        let _lock = broker_storage::FileLock::exclusive(&self.path)?;
        let (mut file, journal_ops) = self.load_state()?;
        let before = file.profiles.clone();
        let out = f(&mut file)?;
        let before_by_id: std::collections::HashMap<_, _> =
            before.into_iter().map(|item| (item.id, item)).collect();
        let after_by_id: std::collections::HashMap<_, _> = file
            .profiles
            .iter()
            .cloned()
            .map(|item| (item.id, item))
            .collect();
        let upserts: Vec<_> = after_by_id
            .iter()
            .filter(|(id, item)| before_by_id.get(id) != Some(*item))
            .map(|(_, item)| item.clone())
            .collect();
        let deletes: Vec<_> = before_by_id
            .keys()
            .filter(|id| !after_by_id.contains_key(id))
            .copied()
            .collect();
        if upserts.is_empty() && deletes.is_empty() {
            return Ok(out);
        }
        let revision = file
            .revision
            .checked_add(1)
            .ok_or_else(|| FlowProfileError::Journal("revision exhausted".into()))?;
        self.journal
            .append(revision, &FlowCommand::Patch { upserts, deletes })
            .map_err(|error| FlowProfileError::Journal(error.to_string()))?;
        file.revision = revision;
        if journal_ops.saturating_add(1) >= FLOW_COMPACT_OPS {
            self.journal
                .compact(&self.path, &file)
                .map_err(|error| FlowProfileError::Journal(error.to_string()))?;
        }
        Ok(out)
    }

    /// Insert or replace by `id` (cluster catalog sync, LWW).
    pub fn upsert(&self, mut profile: FlowProfile) -> Result<(), FlowProfileError> {
        if profile.updated_at_ms == 0 {
            profile.updated_at_ms = Utc::now().timestamp_millis();
        }
        self.mutate(|file| {
            if let Some(pos) = file
                .profiles
                .iter()
                .position(|p| p.tenant_id == profile.tenant_id && p.id == profile.id)
            {
                if file.profiles[pos].updated_at_ms > profile.updated_at_ms {
                    return Ok(());
                }
                file.profiles[pos] = profile;
            } else {
                file.profiles.push(profile);
            }
            Ok(())
        })
    }

    pub fn create(
        &self,
        tenant_id: &str,
        key: String,
        parallelism: u32,
        rate: u32,
        period_secs: u64,
    ) -> Result<FlowProfile, FlowProfileError> {
        self.mutate(|file| {
            let parallelism = parallelism.max(1);
            let period_secs = period_secs.max(1);
            if let Some(existing) = file.profiles.iter().find(|p| {
                p.tenant_id == tenant_id
                    && p.key == key
                    && p.parallelism == parallelism
                    && p.rate == rate
                    && p.period_secs == period_secs
            }) {
                return Err(FlowProfileError::Duplicate(existing.id));
            }
            let profile = FlowProfile {
                id: Uuid::new_v4(),
                tenant_id: tenant_id.to_string(),
                key,
                parallelism,
                rate,
                period_secs,
                updated_at_ms: Utc::now().timestamp_millis(),
            };
            file.profiles.push(profile.clone());
            Ok(profile)
        })
    }

    pub fn get_by_key(
        &self,
        tenant_id: &str,
        key: &str,
    ) -> Result<Option<FlowProfile>, FlowProfileError> {
        let file = self.load()?;
        Ok(file
            .profiles
            .into_iter()
            .find(|p| p.tenant_id == tenant_id && p.key == key))
    }

    /// Create or update a flow profile by grouping key.
    pub fn upsert_by_key(
        &self,
        tenant_id: &str,
        key: String,
        parallelism: u32,
        rate: u32,
        period_secs: u64,
    ) -> Result<FlowProfile, FlowProfileError> {
        self.mutate(|file| {
            if let Some(existing) = file
                .profiles
                .iter_mut()
                .find(|p| p.tenant_id == tenant_id && p.key == key)
            {
                existing.parallelism = parallelism.max(1);
                existing.rate = rate;
                existing.period_secs = period_secs.max(1);
                existing.updated_at_ms = Utc::now().timestamp_millis();
                return Ok(existing.clone());
            }
            let profile = FlowProfile {
                id: Uuid::new_v4(),
                tenant_id: tenant_id.to_string(),
                key,
                parallelism: parallelism.max(1),
                rate,
                period_secs: period_secs.max(1),
                updated_at_ms: Utc::now().timestamp_millis(),
            };
            file.profiles.push(profile.clone());
            Ok(profile)
        })
    }

    /// Reuse an existing profile when key + limits match; otherwise create
    /// (missing key) or update (same key, different limits).
    pub fn ensure_by_key(
        &self,
        tenant_id: &str,
        key: String,
        parallelism: u32,
        rate: u32,
        period_secs: u64,
    ) -> Result<FlowProfile, FlowProfileError> {
        let parallelism = parallelism.max(1);
        let period_secs = period_secs.max(1);
        if let Some(existing) = self.get_by_key(tenant_id, &key)? {
            if existing.parallelism == parallelism
                && existing.rate == rate
                && existing.period_secs == period_secs
            {
                return Ok(existing);
            }
        }
        self.upsert_by_key(tenant_id, key, parallelism, rate, period_secs)
    }

    pub fn get_by_id(
        &self,
        tenant_id: &str,
        id: Uuid,
    ) -> Result<Option<FlowProfile>, FlowProfileError> {
        let file = self.load()?;
        Ok(file
            .profiles
            .into_iter()
            .find(|p| p.tenant_id == tenant_id && p.id == id))
    }

    pub fn list(&self, tenant_id: &str) -> Result<Vec<FlowProfile>, FlowProfileError> {
        let file = self.load()?;
        Ok(file
            .profiles
            .into_iter()
            .filter(|p| p.tenant_id == tenant_id)
            .collect())
    }

    pub fn delete(&self, tenant_id: &str, id: Uuid) -> Result<FlowProfile, FlowProfileError> {
        self.mutate(|file| {
            let pos = file
                .profiles
                .iter()
                .position(|p| p.tenant_id == tenant_id && p.id == id)
                .ok_or(FlowProfileError::NotFound(id))?;
            Ok(file.profiles.remove(pos))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flow_revision_survives_restart_and_newer_version_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let registry = FlowProfileRegistry::open(dir.path()).unwrap();
        let flow = registry
            .create("tenant", "account".into(), 2, 10, 60)
            .unwrap();
        drop(registry);
        let reopened = FlowProfileRegistry::open(dir.path()).unwrap();
        assert_eq!(
            reopened
                .get_by_id("tenant", flow.id)
                .unwrap()
                .unwrap()
                .parallelism,
            2
        );

        std::fs::write(
            dir.path().join("flows.json"),
            br#"{"version":99,"revision":2,"profiles":[]}"#,
        )
        .unwrap();
        assert!(matches!(
            reopened.load(),
            Err(FlowProfileError::UnsupportedVersion(99))
        ));
    }

    #[test]
    fn create_rejects_exact_duplicate_and_keeps_existing_id() {
        let dir = tempfile::tempdir().unwrap();
        let registry = FlowProfileRegistry::open(dir.path()).unwrap();
        let first = registry
            .create("tenant", "user-1".into(), 1, 100, 60)
            .unwrap();
        let err = registry
            .create("tenant", "user-1".into(), 1, 100, 60)
            .unwrap_err();
        assert!(matches!(err, FlowProfileError::Duplicate(id) if id == first.id));
        let different_limits = registry
            .create("tenant", "user-1".into(), 2, 100, 60)
            .unwrap();
        assert_ne!(first.id, different_limits.id);
    }
}
