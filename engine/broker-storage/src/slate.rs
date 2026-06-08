//! SlateDB backend skeleton (CP3b). Enable with `BETTERMQ_STORAGE=slate`.

use crate::message_store::{MessageStore, StoreError};
use broker_proto::{LogRecord, StoredMessage};

/// Placeholder until SlateDB + S3 wiring lands; use local WAL in production CP7a.
pub struct SlateMessageStore;

impl MessageStore for SlateMessageStore {
    fn append(
        &mut self,
        _partition: u32,
        _header: LogRecord,
        _payload: Vec<u8>,
    ) -> Result<(StoredMessage, Vec<u8>), StoreError> {
        Err(StoreError::NotImplemented(
            "SlateDB store: set BETTERMQ_STORAGE=local or implement slatedb feature".into(),
        ))
    }

    fn append_raw_frame(
        &mut self,
        _partition: u32,
        _frame: &[u8],
    ) -> Result<StoredMessage, StoreError> {
        Err(StoreError::NotImplemented(
            "SlateDB append_raw_frame".into(),
        ))
    }

    fn read_range(
        &self,
        _partition: u32,
        _offset: u64,
        _max: usize,
    ) -> Result<Vec<StoredMessage>, StoreError> {
        Err(StoreError::NotImplemented("SlateDB read_range".into()))
    }
}

pub fn storage_backend_from_env() -> &'static str {
    std::env::var("BETTERMQ_STORAGE")
        .unwrap_or_else(|_| "local".into())
        .leak()
}
