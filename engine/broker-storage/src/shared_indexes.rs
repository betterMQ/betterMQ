//! Flock-backed shared meta indexes (cursors + dedup) for multi-node local WAL.
//!
//! `indexes.json` is loaded into memory on every operation. Very large
//! idempotency/cursor maps will grow RSS unbounded — compact or shard the file
//! if a tenant accumulates millions of keys.

use crate::flock::FileLock;
use crate::indexes::DedupEntry;
use crate::meta::atomic_write_file;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SharedIndexError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("corrupt indexes.json; set BETTERMQ_METADATA_RECOVER=empty to start fresh")]
    Corrupt,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct SharedIndexFile {
    #[serde(default)]
    dedup: HashMap<String, DedupEntry>,
    #[serde(default)]
    dispatch: HashMap<String, u64>,
    #[serde(default)]
    completed: HashMap<String, Vec<u64>>,
}

/// Shared-meta store for idempotency + dispatch cursors (HA M2).
pub struct SharedMetadataStore {
    path: PathBuf,
}

impl SharedMetadataStore {
    pub fn open(shared_meta_dir: impl AsRef<Path>) -> Result<Self, SharedIndexError> {
        let dir = shared_meta_dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let path = dir.join("indexes.json");
        if !path.exists() {
            atomic_write_file(&path, br#"{"dedup":{},"dispatch":{}}"#)?;
        }
        Ok(Self { path })
    }

    fn with_locked<R>(
        &self,
        f: impl FnOnce(&mut SharedIndexFile) -> Result<R, SharedIndexError>,
    ) -> Result<R, SharedIndexError> {
        let _lock = FileLock::exclusive(&self.path)?;
        let mut file = if self.path.exists() {
            let bytes = std::fs::read(&self.path)?;
            match serde_json::from_slice(&bytes) {
                Ok(v) => v,
                Err(_) if broker_proto::allow_empty_metadata_recovery() => {
                    SharedIndexFile::default()
                }
                Err(_) => return Err(SharedIndexError::Corrupt),
            }
        } else {
            SharedIndexFile::default()
        };
        let out = f(&mut file)?;
        let bytes = serde_json::to_vec_pretty(&file)?;
        atomic_write_file(&self.path, &bytes)?;
        Ok(out)
    }

    fn with_locked_read<R>(
        &self,
        f: impl FnOnce(&SharedIndexFile) -> Result<R, SharedIndexError>,
    ) -> Result<R, SharedIndexError> {
        let _lock = FileLock::exclusive(&self.path)?;
        let file = if self.path.exists() {
            let bytes = std::fs::read(&self.path)?;
            match serde_json::from_slice(&bytes) {
                Ok(v) => v,
                Err(_) if broker_proto::allow_empty_metadata_recovery() => {
                    SharedIndexFile::default()
                }
                Err(_) => return Err(SharedIndexError::Corrupt),
            }
        } else {
            SharedIndexFile::default()
        };
        f(&file)
    }

    fn dedup_key(tenant_id: &str, idempotency_key: &str) -> String {
        format!("{tenant_id}:{idempotency_key}")
    }

    fn dispatch_key(tenant_id: &str, subscription_id: &str, partition: u32) -> String {
        format!("{tenant_id}:{subscription_id}:p{partition}")
    }

    pub fn get_dedup(
        &self,
        tenant_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<DedupEntry>, SharedIndexError> {
        let key = Self::dedup_key(tenant_id, idempotency_key);
        self.with_locked_read(|file| Ok(file.dedup.get(&key).cloned()))
    }

    pub fn put_dedup(
        &self,
        tenant_id: &str,
        idempotency_key: &str,
        entry: &DedupEntry,
    ) -> Result<(), SharedIndexError> {
        let key = Self::dedup_key(tenant_id, idempotency_key);
        let entry = entry.clone();
        self.with_locked(|file| {
            file.dedup.insert(key, entry);
            Ok(())
        })
    }

    pub fn delete_dedup(
        &self,
        tenant_id: &str,
        idempotency_key: &str,
    ) -> Result<(), SharedIndexError> {
        let key = Self::dedup_key(tenant_id, idempotency_key);
        self.with_locked(|file| {
            file.dedup.remove(&key);
            Ok(())
        })
    }

    pub fn dispatch_offset(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
    ) -> Result<u64, SharedIndexError> {
        let key = Self::dispatch_key(tenant_id, subscription_id, partition);
        self.with_locked_read(|file| Ok(file.dispatch.get(&key).copied().unwrap_or(0)))
    }

    pub fn set_dispatch_offset(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        next_offset: u64,
    ) -> Result<(), SharedIndexError> {
        let key = Self::dispatch_key(tenant_id, subscription_id, partition);
        self.with_locked(|file| {
            file.dispatch.insert(key, next_offset);
            let completion_key = Self::complete_key(tenant_id, subscription_id, partition);
            if let Some(completed) = file.completed.get_mut(&completion_key) {
                completed.retain(|offset| *offset >= next_offset);
                if completed.is_empty() {
                    file.completed.remove(&completion_key);
                }
            }
            Ok(())
        })
    }

    fn complete_key(tenant_id: &str, subscription_id: &str, partition: u32) -> String {
        Self::dispatch_key(tenant_id, subscription_id, partition)
    }

    pub fn mark_dispatch_complete(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        offset: u64,
    ) -> Result<(), SharedIndexError> {
        let key = Self::complete_key(tenant_id, subscription_id, partition);
        self.with_locked(|file| {
            let entry = file.completed.entry(key).or_default();
            if !entry.contains(&offset) {
                entry.push(offset);
            }
            Ok(())
        })
    }

    pub fn is_dispatch_complete(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        offset: u64,
    ) -> Result<bool, SharedIndexError> {
        let key = Self::complete_key(tenant_id, subscription_id, partition);
        self.with_locked_read(|file| {
            Ok(file
                .completed
                .get(&key)
                .map(|v| v.contains(&offset))
                .unwrap_or(false))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_advance_compacts_shared_completion_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let store = SharedMetadataStore::open(dir.path()).unwrap();
        store
            .mark_dispatch_complete("tenant", "lane", 0, 1)
            .unwrap();
        store
            .mark_dispatch_complete("tenant", "lane", 0, 3)
            .unwrap();
        store.set_dispatch_offset("tenant", "lane", 0, 2).unwrap();
        assert!(!store.is_dispatch_complete("tenant", "lane", 0, 1).unwrap());
        assert!(store.is_dispatch_complete("tenant", "lane", 0, 3).unwrap());
    }
}
