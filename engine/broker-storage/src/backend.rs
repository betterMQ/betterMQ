//! Local WAL vs SlateDB partition backend.

use crate::log::{LogError, PartitionLog, PartitionLogConfig};
use broker_proto::{LogRecord, StoredMessage};
#[cfg(feature = "slate")]
use object_store::ObjectStore;
#[cfg(feature = "slate")]
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageMode {
    Local,
    Slate,
}

impl StorageMode {
    pub fn from_env() -> Self {
        match std::env::var("BETTERMQ_STORAGE")
            .unwrap_or_else(|_| "local".into())
            .to_lowercase()
            .as_str()
        {
            "slate" | "s3" => StorageMode::Slate,
            _ => StorageMode::Local,
        }
    }
}

pub enum PartitionBackend {
    Local(PartitionLog),
    #[cfg(feature = "slate")]
    Slate(crate::slate_log::SlatePartitionLog),
}

impl PartitionBackend {
    pub fn open_local(
        dir: impl AsRef<std::path::Path>,
        config: PartitionLogConfig,
    ) -> Result<Self, LogError> {
        Ok(Self::Local(PartitionLog::open(dir, config)?))
    }

    #[cfg(feature = "slate")]
    pub fn open_slate(
        db_path: String,
        local_cache_dir: impl AsRef<std::path::Path>,
        object_store: Arc<dyn ObjectStore>,
        partition: u32,
        config: PartitionLogConfig,
    ) -> Result<Self, LogError> {
        Ok(Self::Slate(crate::slate_log::SlatePartitionLog::open(
            db_path,
            local_cache_dir,
            object_store,
            partition,
            config,
        )?))
    }

    pub fn append(
        &mut self,
        partition: u32,
        header: LogRecord,
        payload: Vec<u8>,
    ) -> Result<(StoredMessage, Vec<u8>), LogError> {
        match self {
            Self::Local(log) => log.append(partition, header, payload),
            #[cfg(feature = "slate")]
            Self::Slate(log) => log.append(partition, header, payload),
        }
    }

    pub fn append_raw_frame(
        &mut self,
        partition: u32,
        frame: &[u8],
    ) -> Result<StoredMessage, LogError> {
        match self {
            Self::Local(log) => log.append_raw_frame(partition, frame),
            #[cfg(feature = "slate")]
            Self::Slate(log) => log.append_raw_frame(partition, frame),
        }
    }

    pub fn read_range(
        &self,
        partition: u32,
        offset: u64,
        max: usize,
    ) -> Result<Vec<StoredMessage>, LogError> {
        match self {
            Self::Local(log) => log.read_range(partition, offset, max),
            #[cfg(feature = "slate")]
            Self::Slate(log) => log.read_range(partition, offset, max),
        }
    }

    pub fn high_watermark(&self) -> u64 {
        match self {
            Self::Local(log) => log.high_watermark(),
            #[cfg(feature = "slate")]
            Self::Slate(log) => log.high_watermark(),
        }
    }

    pub fn purge_offset(&mut self, offset: u64) -> bool {
        match self {
            Self::Local(log) => log.purge_offset(offset),
            #[cfg(feature = "slate")]
            Self::Slate(log) => log.purge_offset(offset),
        }
    }

    pub fn sync(&mut self) -> Result<(), LogError> {
        match self {
            Self::Local(log) => log.sync(),
            #[cfg(feature = "slate")]
            Self::Slate(log) => log.sync(),
        }
    }
}
