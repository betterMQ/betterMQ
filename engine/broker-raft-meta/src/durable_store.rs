//! Durability wrapper around the local Tokio RocksDB Raft store.
//!
//! The example store exercises the complete storage suite but some mutating
//! calls return before an explicit WAL sync. Raft requires votes and log writes
//! to be stable before acknowledging them, so this wrapper adds that barrier.

use crate::rocks_store::{RocksResponse, RocksStore, TypeConfig};
use openraft::storage::{LogState, Snapshot};
use openraft::{
    Entry, LogId, OptionalSend, RaftLogReader, RaftSnapshotBuilder, RaftStorage, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership, Vote,
};
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DurableRocksStore {
    inner: Arc<RocksStore>,
}

impl DurableRocksStore {
    pub fn new(inner: Arc<RocksStore>) -> Self {
        Self { inner }
    }

    async fn sync_wal(&self) -> Result<(), StorageError<u64>> {
        self.inner
            .state_machine
            .read()
            .await
            .db
            .flush_wal(true)
            .map_err(|error| StorageIOError::write(&error).into())
    }
}

impl RaftLogReader<TypeConfig> for DurableRocksStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let mut inner = Arc::clone(&self.inner);
        RaftLogReader::try_get_log_entries(&mut inner, range).await
    }
}

impl RaftSnapshotBuilder<TypeConfig> for DurableRocksStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let mut inner = Arc::clone(&self.inner);
        let snapshot = RaftSnapshotBuilder::build_snapshot(&mut inner).await?;
        self.sync_wal().await?;
        Ok(snapshot)
    }
}

impl RaftStorage<TypeConfig> for DurableRocksStore {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let mut inner = Arc::clone(&self.inner);
        RaftStorage::get_log_state(&mut inner).await
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = Arc::clone(&self.inner);
        RaftStorage::save_vote(&mut inner, vote).await?;
        self.sync_wal().await
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let mut inner = Arc::clone(&self.inner);
        RaftStorage::read_vote(&mut inner).await
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let mut inner = Arc::clone(&self.inner);
        RaftStorage::append_to_log(&mut inner, entries).await?;
        self.sync_wal().await
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<u64>,
    ) -> Result<(), StorageError<u64>> {
        let mut inner = Arc::clone(&self.inner);
        RaftStorage::delete_conflict_logs_since(&mut inner, log_id).await?;
        self.sync_wal().await
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = Arc::clone(&self.inner);
        RaftStorage::purge_logs_upto(&mut inner, log_id).await?;
        self.sync_wal().await
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<u64>>,
            StoredMembership<u64, openraft::BasicNode>,
        ),
        StorageError<u64>,
    > {
        let mut inner = Arc::clone(&self.inner);
        RaftStorage::last_applied_state(&mut inner).await
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<RocksResponse>, StorageError<u64>> {
        let mut inner = Arc::clone(&self.inner);
        let responses = RaftStorage::apply_to_state_machine(&mut inner, entries).await?;
        self.sync_wal().await?;
        Ok(responses)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<TypeConfig as openraft::RaftTypeConfig>::SnapshotData>, StorageError<u64>>
    {
        let mut inner = Arc::clone(&self.inner);
        RaftStorage::begin_receiving_snapshot(&mut inner).await
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, openraft::BasicNode>,
        snapshot: Box<<TypeConfig as openraft::RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<u64>> {
        let mut inner = Arc::clone(&self.inner);
        RaftStorage::install_snapshot(&mut inner, meta, snapshot).await?;
        self.sync_wal().await
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        let mut inner = Arc::clone(&self.inner);
        RaftStorage::get_current_snapshot(&mut inner).await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}
