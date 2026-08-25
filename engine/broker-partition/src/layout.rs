//! Physical shard layout. Live cells never change shard count by modulo remapping.

use broker_proto::{HASH_VERSION, HASH_VERSION_V2};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;

pub const LAYOUT_V1: u32 = 1;
pub const LAYOUT_V2: u32 = 2;
pub const DEFAULT_V2_SHARDS: u32 = 256;

#[derive(Debug, Error)]
pub enum LayoutError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("invalid shard layout: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardLayout {
    pub version: u32,
    pub shard_count: u32,
    /// 1 = original siphash assignment; 2 = canonical flow-lane routing.
    #[serde(default = "default_route_hash_version")]
    pub route_hash_version: u32,
}

fn default_route_hash_version() -> u32 {
    HASH_VERSION
}

impl ShardLayout {
    pub fn v1(shard_count: u32) -> Self {
        Self {
            version: LAYOUT_V1,
            shard_count: shard_count.max(1),
            route_hash_version: HASH_VERSION,
        }
    }

    pub fn path(data_dir: &Path) -> std::path::PathBuf {
        data_dir.join("shard-layout.json")
    }

    pub fn is_physical(&self) -> bool {
        self.version >= LAYOUT_V2
    }

    pub fn storage_namespace<'a>(&self, topic: &'a str) -> &'a str {
        if self.is_physical() {
            "__physical"
        } else {
            topic
        }
    }

    pub fn physical_shard_dir(&self, data_dir: &Path, shard_id: u32) -> PathBuf {
        data_dir
            .join("physical-shards")
            .join(format!("shard-{shard_id:06}"))
    }

    /// Load an existing layout, or persist one from config / env for a new cell.
    /// Existing files win: never rewrite shard_count on a live data directory.
    pub fn load_or_create(
        data_dir: &Path,
        configured_partitions: u32,
    ) -> Result<Self, LayoutError> {
        let path = Self::path(data_dir);
        if path.exists() {
            let bytes = std::fs::read(&path)?;
            let layout: Self = serde_json::from_slice(&bytes)?;
            if layout.shard_count == 0 {
                return Err(LayoutError::Invalid("shard_count must be > 0".into()));
            }
            return Ok(layout);
        }
        std::fs::create_dir_all(data_dir)?;
        let version = match std::env::var("BETTERMQ_SHARD_LAYOUT")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str()
        {
            "v2" | "2" => LAYOUT_V2,
            _ => LAYOUT_V1,
        };
        let legacy_root = data_dir.join("partitions");
        let has_legacy_data = legacy_root
            .read_dir()
            .map(|mut entries| entries.next().is_some())
            .unwrap_or(false);
        if version == LAYOUT_V2 && has_legacy_data {
            return Err(LayoutError::Invalid(
                "cannot initialize layout V2 over an existing V1 partitions directory; migrate or retain V1"
                    .into(),
            ));
        }
        let shard_count = if version == LAYOUT_V2 {
            std::env::var("BETTERMQ_SHARD_COUNT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_V2_SHARDS)
                .max(1)
        } else {
            configured_partitions.max(1)
        };
        let layout = Self {
            version,
            shard_count,
            route_hash_version: if version == LAYOUT_V2 {
                HASH_VERSION_V2
            } else {
                HASH_VERSION
            },
        };
        let bytes = serde_json::to_vec_pretty(&layout)?;
        broker_storage::atomic_write_file(&path, &bytes)?;
        Ok(layout)
    }

    pub fn assign(&self, tenant_id: &str, topic: &str, routing_key: &str) -> u32 {
        if self.version >= LAYOUT_V2 {
            broker_proto::stable_physical_shard(tenant_id, routing_key, self.shard_count)
        } else {
            broker_proto::stable_partition(tenant_id, topic, routing_key, self.shard_count)
        }
    }

    pub fn uses_canonical_lanes(&self) -> bool {
        self.route_hash_version >= HASH_VERSION_V2 || self.version >= LAYOUT_V2
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn existing_layout_is_not_overwritten() {
        let dir = tempdir().unwrap();
        let first = ShardLayout::load_or_create(dir.path(), 4).unwrap();
        assert_eq!(first.shard_count, 4);
        let second = ShardLayout::load_or_create(dir.path(), 256).unwrap();
        assert_eq!(second, first);
    }

    #[test]
    fn v2_storage_path_is_topic_independent() {
        let layout = ShardLayout {
            version: LAYOUT_V2,
            shard_count: 256,
            route_hash_version: HASH_VERSION_V2,
        };
        assert_eq!(layout.storage_namespace("orders"), "__physical");
        assert_eq!(
            layout.physical_shard_dir(Path::new("/cell"), 7),
            Path::new("/cell/physical-shards/shard-000007")
        );
    }
}
