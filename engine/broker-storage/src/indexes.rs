//! RocksDB: idempotency dedup and push dispatch cursors.

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupEntry {
    pub message_id: Uuid,
    pub offset: u64,
    pub partition: u32,
}

/// RocksDB metadata for a broker data directory.
pub struct MetadataStore {
    db: DB,
    sync_writes: WriteOptions,
}

impl MetadataStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, IndexError> {
        let path = path.as_ref();
        std::fs::create_dir_all(path)?;
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path)?;
        let mut sync_writes = WriteOptions::default();
        sync_writes.set_sync(true);
        Ok(Self { db, sync_writes })
    }

    fn dedup_key(tenant_id: &str, idempotency_key: &str) -> Vec<u8> {
        format!("dedup:{tenant_id}:{idempotency_key}").into_bytes()
    }

    pub fn get_dedup(
        &self,
        tenant_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<DedupEntry>, IndexError> {
        let key = Self::dedup_key(tenant_id, idempotency_key);
        match self.db.get(key)? {
            Some(bytes) => Ok(Some(bincode::deserialize(&bytes)?)),
            None => Ok(None),
        }
    }

    pub fn put_dedup(
        &self,
        tenant_id: &str,
        idempotency_key: &str,
        entry: &DedupEntry,
    ) -> Result<(), IndexError> {
        let key = Self::dedup_key(tenant_id, idempotency_key);
        let value = bincode::serialize(entry)?;
        self.db.put_opt(key, value, &self.sync_writes)?;
        Ok(())
    }

    fn dispatch_key(tenant_id: &str, subscription_id: &str, partition: u32) -> Vec<u8> {
        format!("disp:{tenant_id}:{subscription_id}:p{partition}").into_bytes()
    }

    /// Next log offset to push for a webhook subscription on a partition.
    pub fn dispatch_offset(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
    ) -> Result<u64, IndexError> {
        let key = Self::dispatch_key(tenant_id, subscription_id, partition);
        match self.db.get(key)? {
            Some(bytes) => Ok(u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]))),
            None => Ok(0),
        }
    }

    pub fn set_dispatch_offset(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        next_offset: u64,
    ) -> Result<(), IndexError> {
        let key = Self::dispatch_key(tenant_id, subscription_id, partition);
        self.db
            .put_opt(key, next_offset.to_le_bytes(), &self.sync_writes)?;
        Ok(())
    }
}
