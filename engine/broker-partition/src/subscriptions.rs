//! Webhook subscription registry (file-backed until CP3 control plane).

use crate::catalog_journal::CatalogJournal;
use broker_proto::RetryBackoff;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tracing::info;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum SubscriptionError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("subscription not found: {0}")]
    NotFound(Uuid),
    #[error("unsupported subscription catalog version: {0}")]
    UnsupportedVersion(u16),
    #[error("subscription catalog journal: {0}")]
    Journal(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Subscription {
    pub id: Uuid,
    pub tenant_id: String,
    pub topic: String,
    pub url: String,
    pub secret: String,
    #[serde(default)]
    pub paused: bool,
    /// Max in-flight deliveries for jobs **without** a routing key. `1` = the whole queue is serial.
    /// Omit / `0` = unconstrained (standard, high throughput).
    #[serde(default)]
    pub parallelism: Option<u32>,
    /// Default retry count for new jobs on this queue (overrides broker default when set).
    #[serde(default)]
    pub default_max_retries: Option<u32>,
    /// Default backoff between retries for jobs on this queue.
    #[serde(default)]
    pub retry_backoff: Option<RetryBackoff>,
    /// Last catalog mutation time (ms since epoch) for cluster LWW merge (CP6c).
    #[serde(default)]
    pub updated_at_ms: i64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct SubscriptionFile {
    #[serde(default)]
    version: u16,
    #[serde(default)]
    revision: u64,
    subscriptions: Vec<Subscription>,
}

const SUBSCRIPTION_CATALOG_VERSION: u16 = 1;
const SUBSCRIPTION_COMPACT_OPS: usize = 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum SubscriptionCommand {
    Patch {
        upserts: Vec<Subscription>,
        deletes: Vec<Uuid>,
    },
}

fn apply_command(file: &mut SubscriptionFile, command: SubscriptionCommand) {
    match command {
        SubscriptionCommand::Patch { upserts, deletes } => {
            file.subscriptions
                .retain(|subscription| !deletes.contains(&subscription.id));
            for subscription in upserts {
                if let Some(slot) = file
                    .subscriptions
                    .iter_mut()
                    .find(|existing| existing.id == subscription.id)
                {
                    *slot = subscription;
                } else {
                    file.subscriptions.push(subscription);
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct SubscriptionRegistry {
    path: PathBuf,
    journal: CatalogJournal,
}

fn meta_file_path(data_dir: &std::path::Path, name: &str) -> std::path::PathBuf {
    if let Ok(shared) = std::env::var("BETTERMQ_SHARED_META_DIR") {
        let dir = std::path::PathBuf::from(shared);
        let _ = std::fs::create_dir_all(&dir);
        return dir.join(name);
    }
    data_dir.join(name)
}

impl SubscriptionRegistry {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, SubscriptionError> {
        let path = meta_file_path(data_dir.as_ref(), "subscriptions.json");
        if !path.exists() {
            let file = SubscriptionFile {
                version: SUBSCRIPTION_CATALOG_VERSION,
                ..SubscriptionFile::default()
            };
            std::fs::write(&path, serde_json::to_vec_pretty(&file)?)?;
        }
        let journal = CatalogJournal::new(&path, SUBSCRIPTION_CATALOG_VERSION);
        Ok(Self { path, journal })
    }

    fn load(&self) -> Result<SubscriptionFile, SubscriptionError> {
        self.load_state().map(|state| state.0)
    }

    fn load_state(&self) -> Result<(SubscriptionFile, usize), SubscriptionError> {
        let bytes = std::fs::read(&self.path)?;
        let mut file: SubscriptionFile = serde_json::from_slice(&bytes)?;
        if file.version != 0 && file.version != SUBSCRIPTION_CATALOG_VERSION {
            return Err(SubscriptionError::UnsupportedVersion(file.version));
        }
        file.version = SUBSCRIPTION_CATALOG_VERSION;
        let (revision, operations) = self
            .journal
            .replay(file.revision, |command| apply_command(&mut file, command))
            .map_err(|error| SubscriptionError::Journal(error.to_string()))?;
        file.revision = revision;
        Ok((file, operations))
    }

    fn mutate<T>(
        &self,
        f: impl FnOnce(&mut SubscriptionFile) -> Result<T, SubscriptionError>,
    ) -> Result<T, SubscriptionError> {
        let _lock = broker_storage::FileLock::exclusive(&self.path)?;
        let (mut file, journal_ops) = self.load_state()?;
        let before = file.subscriptions.clone();
        let out = f(&mut file)?;
        let before_by_id: std::collections::HashMap<_, _> =
            before.into_iter().map(|item| (item.id, item)).collect();
        let after_by_id: std::collections::HashMap<_, _> = file
            .subscriptions
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
            .ok_or_else(|| SubscriptionError::Journal("revision exhausted".into()))?;
        self.journal
            .append(revision, &SubscriptionCommand::Patch { upserts, deletes })
            .map_err(|error| SubscriptionError::Journal(error.to_string()))?;
        file.revision = revision;
        if journal_ops.saturating_add(1) >= SUBSCRIPTION_COMPACT_OPS {
            self.journal
                .compact(&self.path, &file)
                .map_err(|error| SubscriptionError::Journal(error.to_string()))?;
        }
        Ok(out)
    }

    /// Insert or replace by `id` (cluster catalog sync, LWW).
    pub fn upsert(&self, mut sub: Subscription) -> Result<(), SubscriptionError> {
        if sub.updated_at_ms == 0 {
            sub.updated_at_ms = Utc::now().timestamp_millis();
        }
        self.mutate(|file| {
            if let Some(pos) = file
                .subscriptions
                .iter()
                .position(|s| s.tenant_id == sub.tenant_id && s.id == sub.id)
            {
                if file.subscriptions[pos].updated_at_ms > sub.updated_at_ms {
                    return Ok(());
                }
                file.subscriptions[pos] = sub;
            } else if let Some(pos) = file
                .subscriptions
                .iter()
                .position(|s| s.tenant_id == sub.tenant_id && s.topic == sub.topic)
            {
                if file.subscriptions[pos].updated_at_ms > sub.updated_at_ms {
                    return Ok(());
                }
                file.subscriptions[pos] = sub;
            } else {
                file.subscriptions.push(sub);
            }
            Ok(())
        })
    }

    /// Create or update a queue (one row per tenant + queue name). Changing `url` only affects new enqueues.
    pub fn create(
        &self,
        tenant_id: &str,
        topic: String,
        url: String,
        secret: String,
        parallelism: Option<u32>,
        default_max_retries: Option<u32>,
        retry_backoff: Option<RetryBackoff>,
    ) -> Result<Subscription, SubscriptionError> {
        self.mutate(|file| {
            if let Some(existing) = file
                .subscriptions
                .iter_mut()
                .find(|s| s.tenant_id == tenant_id && s.topic == topic)
            {
                existing.url = url;
                existing.secret = secret;
                if let Some(p) = parallelism {
                    existing.parallelism = if p == 0 { None } else { Some(p) };
                }
                if default_max_retries.is_some() {
                    existing.default_max_retries = default_max_retries;
                }
                if retry_backoff.is_some() {
                    existing.retry_backoff = retry_backoff;
                }
                existing.updated_at_ms = Utc::now().timestamp_millis();
                return Ok(existing.clone());
            }

            let sub = Subscription {
                id: Uuid::new_v4(),
                tenant_id: tenant_id.to_string(),
                topic,
                url,
                secret,
                paused: false,
                parallelism: parallelism.filter(|p| *p > 0),
                default_max_retries,
                retry_backoff,
                updated_at_ms: Utc::now().timestamp_millis(),
            };
            file.subscriptions.push(sub.clone());
            Ok(sub)
        })
    }

    pub fn get_by_name(
        &self,
        tenant_id: &str,
        queue: &str,
    ) -> Result<Option<Subscription>, SubscriptionError> {
        let file = self.load()?;
        Ok(file
            .subscriptions
            .into_iter()
            .find(|s| s.tenant_id == tenant_id && s.topic == queue))
    }

    pub fn get_by_id(
        &self,
        tenant_id: &str,
        id: Uuid,
    ) -> Result<Option<Subscription>, SubscriptionError> {
        let file = self.load()?;
        Ok(file
            .subscriptions
            .into_iter()
            .find(|s| s.tenant_id == tenant_id && s.id == id))
    }

    /// Remove duplicate rows (same tenant, topic, URL). Returns number removed.
    pub fn compact_duplicates(&self) -> Result<usize, SubscriptionError> {
        self.mutate(|file| {
            let before = file.subscriptions.len();
            let mut seen = HashSet::new();
            file.subscriptions.retain(|s| {
                let key = (s.tenant_id.clone(), s.topic.clone(), s.url.clone());
                seen.insert(key)
            });
            let removed = before.saturating_sub(file.subscriptions.len());
            if removed > 0 {
                info!(removed, "compacted duplicate webhook subscriptions");
            }
            Ok(removed)
        })
    }

    /// One subscription per webhook URL for a topic (avoids N POSTs per message).
    pub fn unique_for_topic(
        &self,
        tenant_id: &str,
        topic: &str,
    ) -> Result<Vec<Subscription>, SubscriptionError> {
        let file = self.load()?;
        let mut out = Vec::new();
        let mut seen_urls = HashSet::new();
        for s in file
            .subscriptions
            .into_iter()
            .filter(|s| s.tenant_id == tenant_id && s.topic == topic && !s.paused)
        {
            if seen_urls.insert(s.url.clone()) {
                out.push(s);
            }
        }
        Ok(out)
    }

    pub fn list_for_topic(
        &self,
        tenant_id: &str,
        topic: &str,
    ) -> Result<Vec<Subscription>, SubscriptionError> {
        let file = self.load()?;
        Ok(file
            .subscriptions
            .into_iter()
            .filter(|s| s.tenant_id == tenant_id && s.topic == topic && !s.paused)
            .collect())
    }

    pub fn all_for_topic(
        &self,
        tenant_id: &str,
        topic: &str,
    ) -> Result<Vec<Subscription>, SubscriptionError> {
        let file = self.load()?;
        Ok(file
            .subscriptions
            .into_iter()
            .filter(|s| s.tenant_id == tenant_id && s.topic == topic)
            .collect())
    }

    pub fn list_all(&self, tenant_id: &str) -> Result<Vec<Subscription>, SubscriptionError> {
        let file = self.load()?;
        Ok(file
            .subscriptions
            .into_iter()
            .filter(|s| s.tenant_id == tenant_id)
            .collect())
    }

    pub fn delete(&self, tenant_id: &str, id: Uuid) -> Result<Subscription, SubscriptionError> {
        self.mutate(|file| {
            let pos = file
                .subscriptions
                .iter()
                .position(|s| s.tenant_id == tenant_id && s.id == id)
                .ok_or(SubscriptionError::NotFound(id))?;
            Ok(file.subscriptions.remove(pos))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_revision_survives_restart_and_newer_version_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let registry = SubscriptionRegistry::open(dir.path()).unwrap();
        let created = registry
            .create(
                "tenant",
                "orders".into(),
                "https://example.com/hook".into(),
                "secret".into(),
                None,
                Some(2),
                None,
            )
            .unwrap();
        drop(registry);
        let reopened = SubscriptionRegistry::open(dir.path()).unwrap();
        assert_eq!(
            reopened
                .get_by_id("tenant", created.id)
                .unwrap()
                .unwrap()
                .topic,
            "orders"
        );

        std::fs::write(
            dir.path().join("subscriptions.json"),
            br#"{"version":99,"revision":2,"subscriptions":[]}"#,
        )
        .unwrap();
        assert!(matches!(
            SubscriptionRegistry::open(dir.path()).unwrap().load(),
            Err(SubscriptionError::UnsupportedVersion(99))
        ));
    }
}
