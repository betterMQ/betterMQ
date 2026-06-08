use crate::types::{BetterMqConfig, ConfigError};
use std::path::Path;

/// Optional per-host overrides when the same cluster config file is mounted on every node.
pub fn apply_node_env_overrides(cfg: &mut BetterMqConfig) {
    if let Ok(name) = std::env::var("BETTERMQ_NODE_NAME") {
        let name = name.trim();
        if !name.is_empty() {
            cfg.node.name = name.to_string();
        }
    }
    if let Ok(url) = std::env::var("BETTERMQ_NODE_PUBLIC_URL") {
        let url = url.trim();
        if !url.is_empty() {
            cfg.node.public_url = url.to_string();
        }
    }
}

pub fn load_config(path: &Path) -> Result<BetterMqConfig, ConfigError> {
    let bytes = std::fs::read(path)?;
    let mut cfg: BetterMqConfig = serde_json::from_slice(&bytes)?;
    apply_node_env_overrides(&mut cfg);
    cfg.validate()?;
    Ok(cfg)
}

pub fn write_config(path: &Path, cfg: &BetterMqConfig) -> Result<(), ConfigError> {
    cfg.validate()?;
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    std::fs::write(path, serde_json::to_vec_pretty(cfg)?)?;
    Ok(())
}
