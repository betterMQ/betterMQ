//! Partition log storage (WAL + segments) and RocksDB / shared-meta indexes.

mod backend;
mod flock;
mod indexes;
mod log;
mod message_store;
mod meta;
#[cfg(feature = "slate")]
mod s3_store;
mod shared_indexes;
mod slate;
#[cfg(feature = "slate")]
mod slate_indexes;
#[cfg(feature = "slate")]
mod slate_log;
#[cfg(feature = "slate")]
pub use slate_log::slate_db_path;

pub use backend::{PartitionBackend, StorageMode};
pub use flock::FileLock;
pub use indexes::{DedupEntry, IndexError, MetadataStore};
pub use log::{LogError, PartitionLog, PartitionLogConfig};
pub use message_store::{MessageStore, StoreError};
pub use meta::{atomic_write_file, LogMeta};
#[cfg(feature = "slate")]
pub use s3_store::{
    open_object_store_from_config, open_object_store_from_env, open_payload_object_store_from_env,
    test_s3_connection, S3ConnectionConfig, S3StoreError,
};
pub use slate::{storage_backend_from_env, SlateMessageStore};

pub use broker_proto::{LogRecord, StoredMessage};
