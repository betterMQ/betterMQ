//! Pluggable message persistence (local WAL vs SlateDB) — CP3b.

use broker_proto::{LogRecord, StoredMessage};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("log error: {0}")]
    Log(#[from] crate::LogError),
    #[error("not implemented: {0}")]
    NotImplemented(String),
}

/// Abstraction over partition storage backends.
pub trait MessageStore: Send + Sync {
    fn append(
        &mut self,
        partition: u32,
        header: LogRecord,
        payload: Vec<u8>,
    ) -> Result<(StoredMessage, Vec<u8>), StoreError>;

    fn append_raw_frame(
        &mut self,
        partition: u32,
        frame: &[u8],
    ) -> Result<StoredMessage, StoreError>;

    fn read_range(
        &self,
        partition: u32,
        offset: u64,
        max: usize,
    ) -> Result<Vec<StoredMessage>, StoreError>;
}
