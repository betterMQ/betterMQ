//! Partition log storage (WAL + segments) and RocksDB metadata.

mod backend;
mod indexes;
mod log;
mod message_store;
mod meta;
#[cfg(feature = "slate")]
mod s3_store;
mod slate;
#[cfg(feature = "slate")]
mod slate_log;
#[cfg(feature = "slate")]
pub use slate_log::slate_db_path;

pub use backend::{PartitionBackend, StorageMode};
pub use indexes::{DedupEntry, IndexError, MetadataStore};
pub use log::{LogError, PartitionLog, PartitionLogConfig};
pub use message_store::{MessageStore, StoreError};
pub use meta::LogMeta;
#[cfg(feature = "slate")]
pub use s3_store::{
    open_object_store_from_config, open_object_store_from_env, open_payload_object_store_from_env,
    test_s3_connection, S3ConnectionConfig, S3StoreError,
};
pub use slate::{storage_backend_from_env, SlateMessageStore};

pub use broker_proto::{LogRecord, StoredMessage};
