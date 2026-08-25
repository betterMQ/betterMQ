//! Durable per-shard WAL format metadata.

use crate::meta::atomic_write_file;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub const WAL_FORMAT_V1: u16 = 1;
pub const WAL_FORMAT_V2: u16 = 2;
const MANIFEST_FILE: &str = "wal-manifest.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalManifest {
    pub manifest_version: u16,
    pub wal_format_version: u16,
    pub record_format_version: u32,
    pub shard_id: u32,
}

impl WalManifest {
    pub fn path(dir: &Path) -> PathBuf {
        dir.join(MANIFEST_FILE)
    }

    pub fn load_or_create(
        dir: &Path,
        requested_format: u16,
        shard_id: u32,
    ) -> std::io::Result<Self> {
        let path = Self::path(dir);
        if path.exists() {
            let bytes = std::fs::read(&path)?;
            let manifest: Self = serde_json::from_slice(&bytes)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            if !matches!(manifest.wal_format_version, WAL_FORMAT_V1 | WAL_FORMAT_V2) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unsupported WAL format {}", manifest.wal_format_version),
                ));
            }
            if manifest.shard_id != shard_id {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "WAL manifest shard {} does not match requested shard {}",
                        manifest.shard_id, shard_id
                    ),
                ));
            }
            return Ok(manifest);
        }

        // A pre-manifest directory is V1. Never reinterpret existing bytes as V2.
        let has_existing_data = file_has_data(&dir.join("active.wal"))
            || dir
                .join("segments")
                .read_dir()
                .map(|mut entries| {
                    entries.any(|e| e.ok().map(|e| file_has_data(&e.path())).unwrap_or(false))
                })
                .unwrap_or(false);
        let wal_format_version = if has_existing_data {
            WAL_FORMAT_V1
        } else {
            requested_format
        };
        if !matches!(wal_format_version, WAL_FORMAT_V1 | WAL_FORMAT_V2) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("unsupported requested WAL format {wal_format_version}"),
            ));
        }
        let manifest = Self {
            manifest_version: 1,
            wal_format_version,
            record_format_version: broker_proto::PROTOCOL_VERSION,
            shard_id,
        };
        let bytes = serde_json::to_vec_pretty(&manifest)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        atomic_write_file(&path, &bytes)?;
        Ok(manifest)
    }
}

fn file_has_data(path: &Path) -> bool {
    path.metadata()
        .map(|m| m.is_file() && m.len() > 0)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn existing_pre_manifest_wal_stays_v1() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("segments")).unwrap();
        std::fs::write(dir.path().join("active.wal"), b"legacy").unwrap();
        let manifest = WalManifest::load_or_create(dir.path(), WAL_FORMAT_V2, 3).unwrap();
        assert_eq!(manifest.wal_format_version, WAL_FORMAT_V1);
    }
}
