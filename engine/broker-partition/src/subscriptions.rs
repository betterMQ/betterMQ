//! Webhook subscription registry (file-backed until CP3 control plane).

use crate::flow::FlowSpec;
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub id: Uuid,
    pub tenant_id: String,
    pub topic: String,
    pub url: String,
    pub secret: String,
    #[serde(default)]
    pub paused: bool,
    /// Legacy fields — ignored; use flow profiles (`flow_id` on enqueue).
    #[serde(default, skip_serializing)]
    pub parallelism: Option<u32>,
    #[serde(default, skip_serializing)]
    pub flow: Option<FlowSpec>,
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
    subscriptions: Vec<Subscription>,
}

#[derive(Clone)]
pub struct SubscriptionRegistry {
    path: PathBuf,
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
            let file = SubscriptionFile::default();
            std::fs::write(&path, serde_json::to_vec_pretty(&file)?)?;
        }
        Ok(Self { path })
    }

    fn load(&self) -> Result<SubscriptionFile, SubscriptionError> {
        let bytes = std::fs::read(&self.path)?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    fn save(&self, file: &SubscriptionFile) -> Result<(), SubscriptionError> {
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(file)?)?;
        std::fs::rename(tmp, &self.path)?;
        Ok(())
    }

    /// Insert or replace by `id` (cluster catalog sync, LWW).
    pub fn upsert(&self, mut sub: Subscription) -> Result<(), SubscriptionError> {
        if sub.updated_at_ms == 0 {
            sub.updated_at_ms = Utc::now().timestamp_millis();
        }
        let mut file = self.load()?;
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
        self.save(&file)?;
        Ok(())
    }

    /// Create or update a queue (one row per tenant + queue name). Changing `url` only affects new enqueues.
    pub fn create(
        &self,
        tenant_id: &str,
        topic: String,
        url: String,
        secret: String,
        default_max_retries: Option<u32>,
        retry_backoff: Option<RetryBackoff>,
    ) -> Result<Subscription, SubscriptionError> {
        let mut file = self.load()?;
        if let Some(existing) = file
            .subscriptions
            .iter_mut()
            .find(|s| s.tenant_id == tenant_id && s.topic == topic)
        {
            existing.url = url;
            existing.secret = secret;
            if default_max_retries.is_some() {
                existing.default_max_retries = default_max_retries;
            }
            if retry_backoff.is_some() {
                existing.retry_backoff = retry_backoff;
            }
            existing.updated_at_ms = Utc::now().timestamp_millis();
            let updated = existing.clone();
            self.save(&file)?;
            return Ok(updated);
        }

        let sub = Subscription {
            id: Uuid::new_v4(),
            tenant_id: tenant_id.to_string(),
            topic,
            url,
            secret,
            paused: false,
            parallelism: None,
            flow: None,
            default_max_retries,
            retry_backoff,
            updated_at_ms: Utc::now().timestamp_millis(),
        };
        file.subscriptions.push(sub.clone());
        self.save(&file)?;
        Ok(sub)
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
        let mut file = self.load()?;
        let before = file.subscriptions.len();
        let mut seen = HashSet::new();
        file.subscriptions.retain(|s| {
            let key = (s.tenant_id.clone(), s.topic.clone(), s.url.clone());
            seen.insert(key)
        });
        let removed = before.saturating_sub(file.subscriptions.len());
        if removed > 0 {
            self.save(&file)?;
            info!(removed, "compacted duplicate webhook subscriptions");
        }
        Ok(removed)
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
        let mut file = self.load()?;
        let pos = file
            .subscriptions
            .iter()
            .position(|s| s.tenant_id == tenant_id && s.id == id)
            .ok_or(SubscriptionError::NotFound(id))?;
        let removed = file.subscriptions.remove(pos);
        self.save(&file)?;
        Ok(removed)
    }
}
