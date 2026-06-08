//! Panel-managed config path (`data_dir/bettermq.json`) — users never edit by hand.

use crate::types::{BetterMqConfig, ConfigError, S3Config, StorageConfig};
use std::path::{Path, PathBuf};

pub const REDACTED_SECRET: &str = "••••••••";

pub fn managed_config_path(data_dir: &Path) -> PathBuf {
    data_dir.join("bettermq.json")
}

pub fn load_managed_config(data_dir: &Path) -> Result<Option<BetterMqConfig>, ConfigError> {
    let path = managed_config_path(data_dir);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(&path)?;
    let mut cfg: BetterMqConfig = serde_json::from_slice(&bytes)?;
    crate::file::apply_node_env_overrides(&mut cfg);
    cfg.validate()?;
    Ok(Some(cfg))
}

pub fn save_managed_config(data_dir: &Path, cfg: &BetterMqConfig) -> Result<PathBuf, ConfigError> {
    std::fs::create_dir_all(data_dir)?;
    let path = managed_config_path(data_dir);
    crate::file::write_config(&path, cfg)?;
    Ok(path)
}

/// API view — secrets redacted.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BetterMqConfigView {
    pub version: u32,
    pub node: crate::types::NodeSection,
    pub data_dir: PathBuf,
    pub storage: StorageConfigView,
    pub cluster: Option<crate::types::ClusterConfigSection>,
    pub config_path: PathBuf,
    pub managed: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "mode", rename_all = "lowercase")]
pub enum StorageConfigView {
    Local,
    Slate { s3: S3ConfigView },
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct S3ConfigView {
    pub endpoint: String,
    pub bucket: String,
    pub payload_bucket: Option<String>,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
}

pub fn config_to_view(cfg: &BetterMqConfig, config_path: &Path) -> BetterMqConfigView {
    BetterMqConfigView {
        version: cfg.version,
        node: cfg.node.clone(),
        data_dir: cfg.data_dir.clone(),
        storage: storage_to_view(&cfg.storage),
        cluster: cfg.cluster.clone(),
        config_path: config_path.to_path_buf(),
        managed: true,
    }
}

fn storage_to_view(storage: &StorageConfig) -> StorageConfigView {
    match storage {
        StorageConfig::Local => StorageConfigView::Local,
        StorageConfig::Slate { s3 } => StorageConfigView::Slate {
            s3: S3ConfigView {
                endpoint: s3.endpoint.clone(),
                bucket: s3.bucket.clone(),
                payload_bucket: s3.payload_bucket.clone(),
                access_key: s3.access_key.clone(),
                secret_key: REDACTED_SECRET.into(),
                region: s3.region.clone(),
            },
        },
    }
}

/// Apply storage update from panel; preserve secret when client sends redacted placeholder.
pub fn merge_s3_update(existing: Option<&S3Config>, incoming: S3Config) -> S3Config {
    let secret_key = if incoming.secret_key == REDACTED_SECRET {
        existing
            .map(|e| e.secret_key.clone())
            .unwrap_or(incoming.secret_key)
    } else {
        incoming.secret_key
    };
    S3Config {
        secret_key,
        ..incoming
    }
}
