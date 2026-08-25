//! Partition log storage (WAL + segments) and RocksDB / shared-meta indexes.

mod archive;
mod backend;
mod flock;
mod indexes;
mod log;
mod manifest;
mod message_store;
mod meta;
mod migration;
#[cfg(feature = "s3")]
mod s3_store;
mod shared_indexes;
mod slate;
#[cfg(feature = "slate")]
mod slate_indexes;
#[cfg(feature = "slate")]
mod slate_log;
mod state_index;
mod telemetry;
#[cfg(feature = "slate")]
pub use slate_log::slate_db_path;

pub use archive::{
    archive_admission_blocked, archive_lag_status, restore_local_archive,
    restore_local_archive_until, scrub_local_archive, start_archive_service, ArchiveLagStatus,
    ArchiveManifest, ArchiveToolReport,
};
#[cfg(feature = "s3")]
pub use archive::{restore_object_archive, restore_object_archive_until, scrub_object_archive};
pub use backend::{PartitionBackend, StorageMode};
pub use flock::FileLock;
pub use indexes::{DedupEntry, IndexError, MetadataStore, MetadataWriteBatch};
pub use log::{FsyncMode, LogError, PartitionLog, PartitionLogConfig};
pub use manifest::{WalManifest, WAL_FORMAT_V1, WAL_FORMAT_V2};
pub use message_store::{MessageStore, StoreError};
pub use meta::{atomic_write_file, set_secret_file_mode, LogMeta};
pub use migration::{
    inspect_wal, migrate_v1_to_v2, rollback_v1_to_v2, MigrationError, MigrationReport,
    WalInspection,
};
#[cfg(feature = "s3")]
pub use s3_store::{
    open_archive_object_store_from_env, open_object_store_from_config, open_object_store_from_env,
    open_payload_object_store_from_env, test_s3_connection, S3ConnectionConfig, S3StoreError,
};
pub use slate::{storage_backend_from_env, SlateMessageStore};
pub use state_index::{
    RocksStateIndex, StateIndex, StateIndexError, StateWriteBatch, WriteDurability,
};
pub use telemetry::{
    storage_telemetry_snapshot, StorageShardTelemetrySnapshot, StorageTelemetrySnapshot,
    FSYNC_BUCKET_US, MAX_TELEMETRY_SHARDS,
};

pub use broker_proto::{LogRecord, StoredMessage};
