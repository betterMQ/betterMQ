//! Cluster membership, peer health, shard leader election, scheduler lease.
//!
//! Peer health still uses gossip. The controller boundary durably enforces one
//! vote per term and requires quorum proof before leadership, but does not yet
//! provide an OpenRaft-compatible replicated command log.

mod cluster;
mod controller;
mod durable_store;
mod election;
mod epoch;
mod network;
mod placement;
mod telemetry;

pub use cluster::{ClusterConfig, ClusterError, ClusterRuntime, NodeConfig, SchedulerLease};
pub use controller::{
    raft_id, ControllerAppendRequest, ControllerAppendResponse, ControllerCommand,
    ControllerCommandResponse, ControllerError, ControllerNode, ControllerRaft,
    ControllerSnapshotRequest, ControllerSnapshotResponse, ControllerState,
    ControllerVoteRequest as RaftVoteRequest, ControllerVoteResponse as RaftVoteResponse,
    DrainReport, FanoutOutboxEntry, OpenRaftController, ShardPlacement, TypeConfig,
};
pub use durable_store::DurableRocksStore;
pub use election::{elect_shard_leader, DEFAULT_PEER_TTL_MS};
pub use epoch::{
    ControllerEpoch, ControllerLeaderProof, ControllerVoteRequest, ControllerVoteResponse,
    EpochError,
};
pub use placement::{
    initial_controller_voters, place_replicas, replication_policy, CatalogRecord, DataNodeRecord,
    RebalanceOp, DEFAULT_MIN_ISR, DEFAULT_RF,
};
pub use telemetry::{controller_telemetry_snapshot, ControllerTelemetrySnapshot};
