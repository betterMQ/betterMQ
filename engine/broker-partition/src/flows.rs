//! Flow-control profiles (rate, parallelism, grouping key) — separate from queues.

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
}

/// Named flow-control policy referenced by `flow_id` on publish / enqueue / cron.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    profiles: Vec<FlowProfile>,
}

#[derive(Clone)]
pub struct FlowProfileRegistry {
    path: PathBuf,
}

impl FlowProfileRegistry {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, FlowProfileError> {
        let path = data_dir.as_ref().join("flows.json");
        if !path.exists() {
            std::fs::write(&path, serde_json::to_vec_pretty(&FlowFile::default())?)?;
        }
        Ok(Self { path })
    }

    fn load(&self) -> Result<FlowFile, FlowProfileError> {
        let bytes = std::fs::read(&self.path)?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    fn save(&self, file: &FlowFile) -> Result<(), FlowProfileError> {
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(file)?)?;
        std::fs::rename(tmp, &self.path)?;
        Ok(())
    }

    /// Insert or replace by `id` (cluster catalog sync, LWW).
    pub fn upsert(&self, mut profile: FlowProfile) -> Result<(), FlowProfileError> {
        if profile.updated_at_ms == 0 {
            profile.updated_at_ms = Utc::now().timestamp_millis();
        }
        let mut file = self.load()?;
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
        self.save(&file)?;
        Ok(())
    }

    pub fn create(
        &self,
        tenant_id: &str,
        key: String,
        parallelism: u32,
        rate: u32,
        period_secs: u64,
    ) -> Result<FlowProfile, FlowProfileError> {
        let mut file = self.load()?;
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
        self.save(&file)?;
        Ok(profile)
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
        let mut file = self.load()?;
        if let Some(existing) = file
            .profiles
            .iter_mut()
            .find(|p| p.tenant_id == tenant_id && p.key == key)
        {
            existing.parallelism = parallelism.max(1);
            existing.rate = rate;
            existing.period_secs = period_secs.max(1);
            existing.updated_at_ms = Utc::now().timestamp_millis();
            let updated = existing.clone();
            self.save(&file)?;
            return Ok(updated);
        }
        self.create(tenant_id, key, parallelism, rate, period_secs)
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
        let mut file = self.load()?;
        let pos = file
            .profiles
            .iter()
            .position(|p| p.tenant_id == tenant_id && p.id == id)
            .ok_or(FlowProfileError::NotFound(id))?;
        let removed = file.profiles.remove(pos);
        self.save(&file)?;
        Ok(removed)
    }
}
