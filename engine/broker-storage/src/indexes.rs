//! Idempotency dedup and push dispatch cursors (local RocksDB, shared-meta file, or Slate).

use crate::shared_indexes::{SharedIndexError, SharedMetadataStore};
#[cfg(feature = "slate")]
use crate::slate_indexes::{SlateIndexError, SlateMetadataStore};
use rocksdb::{Options, WriteBatch, WriteOptions, DB};
use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("rocksdb error: {0}")]
    Rocks(#[from] rocksdb::Error),
    #[error("encode error: {0}")]
    Encode(#[from] bincode_next::error::EncodeError),
    #[error("decode error: {0}")]
    Decode(#[from] bincode_next::error::DecodeError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("shared index error: {0}")]
    Shared(#[from] SharedIndexError),
    #[cfg(feature = "slate")]
    #[error("slate index error: {0}")]
    Slate(#[from] SlateIndexError),
}

#[derive(Debug, Default)]
pub struct MetadataWriteBatch {
    operations: Vec<MetadataOperation>,
}

#[derive(Debug)]
enum MetadataOperation {
    PutDedup {
        tenant_id: String,
        key: String,
        entry: DedupEntry,
    },
    DeleteDedup {
        tenant_id: String,
        key: String,
    },
    SetDispatchOffset {
        tenant_id: String,
        subscription_id: String,
        partition: u32,
        next_offset: u64,
    },
    MarkDispatchComplete {
        tenant_id: String,
        subscription_id: String,
        partition: u32,
        offset: u64,
    },
}

impl MetadataWriteBatch {
    pub fn put_dedup(
        &mut self,
        tenant_id: impl Into<String>,
        key: impl Into<String>,
        entry: DedupEntry,
    ) {
        self.operations.push(MetadataOperation::PutDedup {
            tenant_id: tenant_id.into(),
            key: key.into(),
            entry,
        });
    }

    pub fn delete_dedup(&mut self, tenant_id: impl Into<String>, key: impl Into<String>) {
        self.operations.push(MetadataOperation::DeleteDedup {
            tenant_id: tenant_id.into(),
            key: key.into(),
        });
    }

    pub fn set_dispatch_offset(
        &mut self,
        tenant_id: impl Into<String>,
        subscription_id: impl Into<String>,
        partition: u32,
        next_offset: u64,
    ) {
        self.operations.push(MetadataOperation::SetDispatchOffset {
            tenant_id: tenant_id.into(),
            subscription_id: subscription_id.into(),
            partition,
            next_offset,
        });
    }

    pub fn mark_dispatch_complete(
        &mut self,
        tenant_id: impl Into<String>,
        subscription_id: impl Into<String>,
        partition: u32,
        offset: u64,
    ) {
        self.operations
            .push(MetadataOperation::MarkDispatchComplete {
                tenant_id: tenant_id.into(),
                subscription_id: subscription_id.into(),
                partition,
                offset,
            });
    }
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
        // Shard metadata is a rebuildable WAL-derived index. The shard commit
        // coordinator owns durability; RocksDB is synced once per checkpoint.
        let mut sync_writes = WriteOptions::default();
        sync_writes.set_sync(false);
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

    /// Atomically apply one metadata epoch on RocksDB.
    ///
    /// Shared-file and Slate compatibility backends preserve the API but apply
    /// operations sequentially; they are not used by the Core V2 local shard path.
    pub fn write_batch(&self, batch: MetadataWriteBatch) -> Result<(), IndexError> {
        if batch.operations.is_empty() {
            return Ok(());
        }
        match &self.backend {
            MetaBackend::Rocks {
                db, sync_writes, ..
            } => {
                let mut rocks_batch = WriteBatch::default();
                for operation in batch.operations {
                    match operation {
                        MetadataOperation::PutDedup {
                            tenant_id,
                            key,
                            entry,
                        } => rocks_batch.put(
                            Self::dedup_key(&tenant_id, &key),
                            broker_proto::encode(&entry)?,
                        ),
                        MetadataOperation::DeleteDedup { tenant_id, key } => {
                            rocks_batch.delete(Self::dedup_key(&tenant_id, &key))
                        }
                        MetadataOperation::SetDispatchOffset {
                            tenant_id,
                            subscription_id,
                            partition,
                            next_offset,
                        } => rocks_batch.put(
                            Self::dispatch_key(&tenant_id, &subscription_id, partition),
                            next_offset.to_le_bytes(),
                        ),
                        MetadataOperation::MarkDispatchComplete {
                            tenant_id,
                            subscription_id,
                            partition,
                            offset,
                        } => rocks_batch.put(
                            Self::complete_key(&tenant_id, &subscription_id, partition, offset),
                            [1u8],
                        ),
                    }
                }
                db.write_opt(rocks_batch, sync_writes)?;
                Ok(())
            }
            _ => {
                for operation in batch.operations {
                    match operation {
                        MetadataOperation::PutDedup {
                            tenant_id,
                            key,
                            entry,
                        } => self.put_dedup(&tenant_id, &key, &entry)?,
                        MetadataOperation::DeleteDedup { tenant_id, key } => {
                            self.delete_dedup(&tenant_id, &key)?
                        }
                        MetadataOperation::SetDispatchOffset {
                            tenant_id,
                            subscription_id,
                            partition,
                            next_offset,
                        } => self.set_dispatch_offset(
                            &tenant_id,
                            &subscription_id,
                            partition,
                            next_offset,
                        )?,
                        MetadataOperation::MarkDispatchComplete {
                            tenant_id,
                            subscription_id,
                            partition,
                            offset,
                        } => self.mark_dispatch_complete(
                            &tenant_id,
                            &subscription_id,
                            partition,
                            offset,
                        )?,
                    }
                }
                Ok(())
            }
        }
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
                    Some(bytes) => Ok(Some(broker_proto::decode(&bytes)?)),
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
                let value = broker_proto::encode(entry)?;
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
                let current = match db.get(&key)? {
                    Some(bytes) => u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])),
                    None => 0,
                };
                let mut batch = WriteBatch::default();
                batch.put(key, next_offset.to_le_bytes());
                // Cursor and covered gap deletion become visible atomically. This
                // bounds completion metadata as contiguous work advances.
                let prefix = Self::complete_prefix(tenant_id, subscription_id, partition);
                for item in db.prefix_iterator(&prefix) {
                    let (key, _) = item?;
                    if !key.starts_with(&prefix) {
                        break;
                    }
                    let covered = std::str::from_utf8(&key[prefix.len()..])
                        .ok()
                        .and_then(|value| value.parse::<u64>().ok())
                        .map(|offset| offset >= current && offset < next_offset)
                        .unwrap_or(false);
                    if covered {
                        batch.delete(key);
                    }
                }
                db.write_opt(batch, sync_writes)?;
                Ok(())
            }
        }
    }

    fn complete_key(
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        offset: u64,
    ) -> Vec<u8> {
        format!("dcomp:{tenant_id}:{subscription_id}:p{partition}:{offset}").into_bytes()
    }

    fn complete_prefix(tenant_id: &str, subscription_id: &str, partition: u32) -> Vec<u8> {
        format!("dcomp:{tenant_id}:{subscription_id}:p{partition}:").into_bytes()
    }

    pub fn mark_dispatch_complete(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        offset: u64,
    ) -> Result<(), IndexError> {
        match &self.backend {
            MetaBackend::Shared(s) => {
                s.mark_dispatch_complete(tenant_id, subscription_id, partition, offset)?;
                Ok(())
            }
            #[cfg(feature = "slate")]
            MetaBackend::Slate(s) => {
                s.mark_dispatch_complete(tenant_id, subscription_id, partition, offset)?;
                Ok(())
            }
            MetaBackend::Rocks { db, sync_writes } => {
                let key = Self::complete_key(tenant_id, subscription_id, partition, offset);
                db.put_opt(key, [1u8], sync_writes)?;
                Ok(())
            }
        }
    }

    pub fn is_dispatch_complete(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        offset: u64,
    ) -> Result<bool, IndexError> {
        if offset < self.dispatch_offset(tenant_id, subscription_id, partition)? {
            return Ok(true);
        }
        match &self.backend {
            MetaBackend::Shared(s) => {
                Ok(s.is_dispatch_complete(tenant_id, subscription_id, partition, offset)?)
            }
            #[cfg(feature = "slate")]
            MetaBackend::Slate(s) => {
                Ok(s.is_dispatch_complete(tenant_id, subscription_id, partition, offset)?)
            }
            MetaBackend::Rocks { db, .. } => {
                let key = Self::complete_key(tenant_id, subscription_id, partition, offset);
                Ok(db.get(key)?.is_some())
            }
        }
    }

    /// Flush the RocksDB WAL (epoch barrier). No-op for shared-file / slate.
    pub fn flush_epoch(&self) -> Result<(), IndexError> {
        match &self.backend {
            MetaBackend::Rocks { db, .. } => {
                db.flush_wal(true)?;
                Ok(())
            }
            MetaBackend::Shared(_) => Ok(()),
            #[cfg(feature = "slate")]
            MetaBackend::Slate(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn entry(offset: u64) -> DedupEntry {
        DedupEntry {
            message_id: Uuid::new_v4(),
            offset,
            partition: 2,
        }
    }

    #[test]
    fn metadata_write_batch_commits_epoch_together() {
        let dir = tempdir().unwrap();
        let store = MetadataStore::open(dir.path()).unwrap();
        let mut batch = MetadataWriteBatch::default();
        batch.put_dedup("tenant", "a", entry(10));
        batch.put_dedup("tenant", "b", entry(11));
        batch.set_dispatch_offset("tenant", "queue", 2, 7);
        store.write_batch(batch).unwrap();

        assert_eq!(store.get_dedup("tenant", "a").unwrap().unwrap().offset, 10);
        assert_eq!(store.get_dedup("tenant", "b").unwrap().unwrap().offset, 11);
        assert_eq!(store.dispatch_offset("tenant", "queue", 2).unwrap(), 7);
    }

    #[test]
    fn advancing_cursor_compacts_covered_completion_gaps() {
        let dir = tempdir().unwrap();
        let store = MetadataStore::open(dir.path()).unwrap();
        store
            .mark_dispatch_complete("tenant", "queue", 0, 1)
            .unwrap();
        store
            .mark_dispatch_complete("tenant", "queue", 0, 3)
            .unwrap();
        store.set_dispatch_offset("tenant", "queue", 0, 2).unwrap();

        assert!(store.is_dispatch_complete("tenant", "queue", 0, 1).unwrap());
        assert!(store.is_dispatch_complete("tenant", "queue", 0, 3).unwrap());
        assert_eq!(store.dispatch_offset("tenant", "queue", 0).unwrap(), 2);
    }
}
