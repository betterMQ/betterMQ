//! Narrow transactional state-index abstraction used by shard metadata.

use rocksdb::{Options, WriteBatch, WriteOptions, DB};
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StateIndexError {
    #[error("rocksdb error: {0}")]
    Rocks(#[from] rocksdb::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteDurability {
    /// Rebuildable checkpoint/index state; the shard WAL is authoritative.
    Derived,
    /// Controller-authoritative state; sync RocksDB's WAL before returning.
    Sync,
}

pub type StateEntry = (Vec<u8>, Vec<u8>);

#[derive(Debug, Default)]
pub struct StateWriteBatch {
    operations: Vec<StateOperation>,
}

#[derive(Debug)]
enum StateOperation {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

impl StateWriteBatch {
    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.operations
            .push(StateOperation::Put(key.into(), value.into()));
    }

    pub fn delete(&mut self, key: impl Into<Vec<u8>>) {
        self.operations.push(StateOperation::Delete(key.into()));
    }

    pub fn len(&self) -> usize {
        self.operations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }
}

pub trait StateIndex: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StateIndexError>;
    fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Result<Vec<StateEntry>, StateIndexError>;
    fn write(
        &self,
        batch: StateWriteBatch,
        durability: WriteDurability,
    ) -> Result<(), StateIndexError>;
    fn flush_wal(&self) -> Result<(), StateIndexError>;
}

pub struct RocksStateIndex {
    db: DB,
}

impl RocksStateIndex {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StateIndexError> {
        std::fs::create_dir_all(path.as_ref())?;
        let mut options = Options::default();
        options.create_if_missing(true);
        Ok(Self {
            db: DB::open(&options, path)?,
        })
    }
}

impl StateIndex for RocksStateIndex {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StateIndexError> {
        Ok(self.db.get(key)?)
    }

    fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Result<Vec<StateEntry>, StateIndexError> {
        let mut out = Vec::with_capacity(limit.min(256));
        for item in self.db.prefix_iterator(prefix) {
            let (key, value) = item?;
            if !key.starts_with(prefix) || out.len() >= limit {
                break;
            }
            out.push((key.to_vec(), value.to_vec()));
        }
        Ok(out)
    }

    fn write(
        &self,
        batch: StateWriteBatch,
        durability: WriteDurability,
    ) -> Result<(), StateIndexError> {
        if batch.is_empty() {
            return Ok(());
        }
        let mut rocks_batch = WriteBatch::default();
        for operation in batch.operations {
            match operation {
                StateOperation::Put(key, value) => rocks_batch.put(key, value),
                StateOperation::Delete(key) => rocks_batch.delete(key),
            }
        }
        let mut options = WriteOptions::default();
        options.set_sync(durability == WriteDurability::Sync);
        self.db.write_opt(rocks_batch, &options)?;
        Ok(())
    }

    fn flush_wal(&self) -> Result<(), StateIndexError> {
        self.db.flush_wal(true)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn write_batch_is_atomic_api_surface() {
        let dir = tempdir().unwrap();
        let index = RocksStateIndex::open(dir.path()).unwrap();
        let mut batch = StateWriteBatch::default();
        batch.put(b"epoch".to_vec(), 7u64.to_le_bytes().to_vec());
        batch.put(b"cursor".to_vec(), 11u64.to_le_bytes().to_vec());
        index.write(batch, WriteDurability::Derived).unwrap();
        assert_eq!(index.get(b"epoch").unwrap().unwrap(), 7u64.to_le_bytes());
        assert_eq!(index.get(b"cursor").unwrap().unwrap(), 11u64.to_le_bytes());
    }
}
