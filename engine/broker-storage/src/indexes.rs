//! Idempotency dedup and push dispatch cursors (local RocksDB, shared-meta file, or Slate).

use crate::shared_indexes::{SharedIndexError, SharedMetadataStore};
#[cfg(feature = "slate")]
use crate::slate_indexes::{SlateIndexError, SlateMetadataStore};
use rocksdb::{Options, WriteOptions, DB};
use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("rocksdb error: {0}")]
    Rocks(#[from] rocksdb::Error),
    #[error("serde error: {0}")]
    Serde(#[from] bincode::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("shared index error: {0}")]
    Shared(#[from] SharedIndexError),
    #[cfg(feature = "slate")]
    #[error("slate index error: {0}")]
    Slate(#[from] SlateIndexError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupEntry {
    pub message_id: Uuid,
    pub offset: u64,
    pub partition: u32,
}

enum MetaBackend {
    Rocks {
        db: DB,
        sync_writes: WriteOptions,
    },
    Shared(SharedMetadataStore),
    #[cfg(feature = "slate")]
    Slate(SlateMetadataStore),
}

/// Metadata for a broker: local RocksDB, flock shared-meta, or SlateDB on object store.
pub struct MetadataStore {
    backend: MetaBackend,
}

impl MetadataStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, IndexError> {
        if let Ok(shared) = std::env::var("BETTERMQ_SHARED_META_DIR") {
            let shared = shared.trim();
            if !shared.is_empty() {
                let store = SharedMetadataStore::open(shared)?;
                return Ok(Self {
                    backend: MetaBackend::Shared(store),
                });
            }
        }
        let path = path.as_ref();
        std::fs::create_dir_all(path)?;
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path)?;
        let mut sync_writes = WriteOptions::default();
        sync_writes.set_sync(true);
        Ok(Self {
            backend: MetaBackend::Rocks { db, sync_writes },
        })
    }

    /// Dedup + dispatch cursors on the same object store as Slate messages (HA M3).
    #[cfg(feature = "slate")]
    pub fn open_slate(
        object_store: std::sync::Arc<dyn object_store::ObjectStore>,
        local_cache_dir: impl AsRef<Path>,
    ) -> Result<Self, IndexError> {
        let store = SlateMetadataStore::open(object_store, local_cache_dir)?;
        Ok(Self {
            backend: MetaBackend::Slate(store),
        })
    }

    fn dedup_key(tenant_id: &str, idempotency_key: &str) -> Vec<u8> {
        format!("dedup:{tenant_id}:{idempotency_key}").into_bytes()
    }

    pub fn get_dedup(
        &self,
        tenant_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<DedupEntry>, IndexError> {
        match &self.backend {
            MetaBackend::Shared(s) => Ok(s.get_dedup(tenant_id, idempotency_key)?),
            #[cfg(feature = "slate")]
            MetaBackend::Slate(s) => Ok(s.get_dedup(tenant_id, idempotency_key)?),
            MetaBackend::Rocks { db, .. } => {
                let key = Self::dedup_key(tenant_id, idempotency_key);
                match db.get(key)? {
                    Some(bytes) => Ok(Some(bincode::deserialize(&bytes)?)),
                    None => Ok(None),
                }
            }
        }
    }

    pub fn put_dedup(
        &self,
        tenant_id: &str,
        idempotency_key: &str,
        entry: &DedupEntry,
    ) -> Result<(), IndexError> {
        match &self.backend {
            MetaBackend::Shared(s) => {
                s.put_dedup(tenant_id, idempotency_key, entry)?;
                Ok(())
            }
            #[cfg(feature = "slate")]
            MetaBackend::Slate(s) => {
                s.put_dedup(tenant_id, idempotency_key, entry)?;
                Ok(())
            }
            MetaBackend::Rocks { db, sync_writes } => {
                let key = Self::dedup_key(tenant_id, idempotency_key);
                let value = bincode::serialize(entry)?;
                db.put_opt(key, value, sync_writes)?;
                Ok(())
            }
        }
    }

    pub fn delete_dedup(&self, tenant_id: &str, idempotency_key: &str) -> Result<(), IndexError> {
        match &self.backend {
            MetaBackend::Shared(s) => {
                s.delete_dedup(tenant_id, idempotency_key)?;
                Ok(())
            }
            #[cfg(feature = "slate")]
            MetaBackend::Slate(s) => {
                s.delete_dedup(tenant_id, idempotency_key)?;
                Ok(())
            }
            MetaBackend::Rocks { db, sync_writes } => {
                let key = Self::dedup_key(tenant_id, idempotency_key);
                db.delete_opt(key, sync_writes)?;
                Ok(())
            }
        }
    }

    fn dispatch_key(tenant_id: &str, subscription_id: &str, partition: u32) -> Vec<u8> {
        format!("disp:{tenant_id}:{subscription_id}:p{partition}").into_bytes()
    }

    pub fn dispatch_offset(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
    ) -> Result<u64, IndexError> {
        match &self.backend {
            MetaBackend::Shared(s) => {
                Ok(s.dispatch_offset(tenant_id, subscription_id, partition)?)
            }
            #[cfg(feature = "slate")]
            MetaBackend::Slate(s) => {
                Ok(s.dispatch_offset(tenant_id, subscription_id, partition)?)
            }
            MetaBackend::Rocks { db, .. } => {
                let key = Self::dispatch_key(tenant_id, subscription_id, partition);
                match db.get(key)? {
                    Some(bytes) => Ok(u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]))),
                    None => Ok(0),
                }
            }
        }
    }

    pub fn set_dispatch_offset(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        next_offset: u64,
    ) -> Result<(), IndexError> {
        match &self.backend {
            MetaBackend::Shared(s) => {
                s.set_dispatch_offset(tenant_id, subscription_id, partition, next_offset)?;
                Ok(())
            }
            #[cfg(feature = "slate")]
            MetaBackend::Slate(s) => {
                s.set_dispatch_offset(tenant_id, subscription_id, partition, next_offset)?;
                Ok(())
            }
            MetaBackend::Rocks { db, sync_writes } => {
                let key = Self::dispatch_key(tenant_id, subscription_id, partition);
                db.put_opt(key, next_offset.to_le_bytes(), sync_writes)?;
                Ok(())
            }
        }
    }
}
