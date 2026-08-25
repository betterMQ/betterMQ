//! SlateDB-backed dedup + dispatch cursors (shared across brokers via object store).

use crate::indexes::DedupEntry;
use crate::slate_log::block_on_slate;
use bytes::Bytes;
use object_store::ObjectStore;
use slatedb::config::WriteOptions;
use slatedb::{Db, WriteBatch};
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SlateIndexError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("slate: {0}")]
    Slate(String),
    #[error("encode error: {0}")]
    Encode(#[from] bincode_next::error::EncodeError),
    #[error("decode error: {0}")]
    Decode(#[from] bincode_next::error::DecodeError),
}

const INDEX_DB_PATH: &str = "bettermq/_indexes";

pub struct SlateMetadataStore {
    db: Arc<Db>,
}

impl SlateMetadataStore {
    pub fn open(
        object_store: Arc<dyn ObjectStore>,
        local_cache_dir: impl AsRef<Path>,
    ) -> Result<Self, SlateIndexError> {
        let cache = local_cache_dir.as_ref().join("indexes");
        std::fs::create_dir_all(&cache)?;
        let db = block_on_slate(Db::open(INDEX_DB_PATH.to_string(), object_store))
            .map_err(|e| SlateIndexError::Slate(e.to_string()))?;
        Ok(Self { db: Arc::new(db) })
    }

    fn durable_opts() -> WriteOptions {
        WriteOptions {
            await_durable: true,
            seqnum: 0,
        }
    }

    fn dedup_key(tenant_id: &str, idempotency_key: &str) -> Vec<u8> {
        format!("dedup:{tenant_id}:{idempotency_key}").into_bytes()
    }

    fn dispatch_key(tenant_id: &str, subscription_id: &str, partition: u32) -> Vec<u8> {
        format!("disp:{tenant_id}:{subscription_id}:p{partition}").into_bytes()
    }

    pub fn get_dedup(
        &self,
        tenant_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<DedupEntry>, SlateIndexError> {
        let key = Self::dedup_key(tenant_id, idempotency_key);
        let db = Arc::clone(&self.db);
        let bytes = block_on_slate(async move {
            db.get(&key)
                .await
                .map_err(|e| SlateIndexError::Slate(e.to_string()))
        })?;
        match bytes {
            Some(b) => Ok(Some(broker_proto::decode(&b)?)),
            None => Ok(None),
        }
    }

    pub fn put_dedup(
        &self,
        tenant_id: &str,
        idempotency_key: &str,
        entry: &DedupEntry,
    ) -> Result<(), SlateIndexError> {
        let key = Self::dedup_key(tenant_id, idempotency_key);
        let value = broker_proto::encode(entry)?;
        let db = Arc::clone(&self.db);
        block_on_slate(async move {
            let mut batch = WriteBatch::new();
            batch.put_bytes(Bytes::from(key), Bytes::from(value));
            let opts = Self::durable_opts();
            db.write_with_options(batch, &opts)
                .await
                .map_err(|e| SlateIndexError::Slate(e.to_string()))?;
            db.flush()
                .await
                .map_err(|e| SlateIndexError::Slate(e.to_string()))?;
            Ok(())
        })
    }

    pub fn delete_dedup(
        &self,
        tenant_id: &str,
        idempotency_key: &str,
    ) -> Result<(), SlateIndexError> {
        let key = Self::dedup_key(tenant_id, idempotency_key);
        let db = Arc::clone(&self.db);
        block_on_slate(async move {
            let mut batch = WriteBatch::new();
            batch.delete(key);
            let opts = Self::durable_opts();
            db.write_with_options(batch, &opts)
                .await
                .map_err(|e| SlateIndexError::Slate(e.to_string()))?;
            db.flush()
                .await
                .map_err(|e| SlateIndexError::Slate(e.to_string()))?;
            Ok(())
        })
    }

    pub fn dispatch_offset(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
    ) -> Result<u64, SlateIndexError> {
        let key = Self::dispatch_key(tenant_id, subscription_id, partition);
        let db = Arc::clone(&self.db);
        let bytes = block_on_slate(async move {
            db.get(&key)
                .await
                .map_err(|e| SlateIndexError::Slate(e.to_string()))
        })?;
        match bytes {
            Some(b) if b.len() >= 8 => {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&b[..8]);
                Ok(u64::from_le_bytes(buf))
            }
            _ => Ok(0),
        }
    }

    pub fn set_dispatch_offset(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        next_offset: u64,
    ) -> Result<(), SlateIndexError> {
        let key = Self::dispatch_key(tenant_id, subscription_id, partition);
        let value = next_offset.to_le_bytes().to_vec();
        let tenant_id = tenant_id.to_string();
        let subscription_id = subscription_id.to_string();
        let db = Arc::clone(&self.db);
        block_on_slate(async move {
            let current = db
                .get(&key)
                .await
                .map_err(|e| SlateIndexError::Slate(e.to_string()))?
                .and_then(|bytes| bytes.as_ref().try_into().ok().map(u64::from_le_bytes))
                .unwrap_or(0);
            let mut batch = WriteBatch::new();
            batch.put_bytes(Bytes::from(key), Bytes::from(value));
            for offset in current..next_offset {
                batch.delete(Self::complete_key(
                    &tenant_id,
                    &subscription_id,
                    partition,
                    offset,
                ));
            }
            let opts = Self::durable_opts();
            db.write_with_options(batch, &opts)
                .await
                .map_err(|e| SlateIndexError::Slate(e.to_string()))?;
            db.flush()
                .await
                .map_err(|e| SlateIndexError::Slate(e.to_string()))?;
            Ok(())
        })
    }

    fn complete_key(
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        offset: u64,
    ) -> Vec<u8> {
        format!("dcomp:{tenant_id}:{subscription_id}:p{partition}:{offset}").into_bytes()
    }

    pub fn mark_dispatch_complete(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        offset: u64,
    ) -> Result<(), SlateIndexError> {
        let key = Self::complete_key(tenant_id, subscription_id, partition, offset);
        let db = Arc::clone(&self.db);
        block_on_slate(async move {
            let mut batch = WriteBatch::new();
            batch.put_bytes(Bytes::from(key), Bytes::from_static(&[1u8]));
            let opts = Self::durable_opts();
            db.write_with_options(batch, &opts)
                .await
                .map_err(|e| SlateIndexError::Slate(e.to_string()))?;
            Ok(())
        })
    }

    pub fn is_dispatch_complete(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        offset: u64,
    ) -> Result<bool, SlateIndexError> {
        let key = Self::complete_key(tenant_id, subscription_id, partition, offset);
        let db = Arc::clone(&self.db);
        let bytes = block_on_slate(async move {
            db.get(&key)
                .await
                .map_err(|e| SlateIndexError::Slate(e.to_string()))
        })?;
        Ok(bytes.is_some())
    }
}
