use broker_raft_meta::{DurableRocksStore, RocksStore, TypeConfig};
use openraft::storage::Adaptor;
use openraft::testing::{StoreBuilder, Suite};
use openraft::StorageError;
use std::sync::Arc;
use tempfile::TempDir;

type LogStore = Adaptor<TypeConfig, Arc<RocksStore>>;
type StateMachine = Adaptor<TypeConfig, Arc<RocksStore>>;

struct RocksBuilder;

impl StoreBuilder<TypeConfig, LogStore, StateMachine, TempDir> for RocksBuilder {
    async fn build(&self) -> Result<(TempDir, LogStore, StateMachine), StorageError<u64>> {
        let dir = TempDir::new().expect("temporary OpenRaft RocksDB directory");
        let store = RocksStore::new(dir.path()).await;
        let (log_store, state_machine) = Adaptor::new(store);
        Ok((dir, log_store, state_machine))
    }
}

#[test]
fn official_openraft_storage_suite() -> Result<(), Box<dyn std::error::Error>> {
    Ok(Suite::test_all(RocksBuilder)?)
}

type DurableLogStore = Adaptor<TypeConfig, DurableRocksStore>;
type DurableStateMachine = Adaptor<TypeConfig, DurableRocksStore>;

struct DurableRocksBuilder;

impl StoreBuilder<TypeConfig, DurableLogStore, DurableStateMachine, TempDir>
    for DurableRocksBuilder
{
    async fn build(
        &self,
    ) -> Result<(TempDir, DurableLogStore, DurableStateMachine), StorageError<u64>> {
        let dir = TempDir::new().expect("temporary durable OpenRaft RocksDB directory");
        let store = RocksStore::new(dir.path()).await;
        let (log_store, state_machine) = Adaptor::new(DurableRocksStore::new(store));
        Ok((dir, log_store, state_machine))
    }
}

#[test]
fn production_durable_store_passes_openraft_suite() -> Result<(), Box<dyn std::error::Error>> {
    Ok(Suite::test_all(DurableRocksBuilder)?)
}
