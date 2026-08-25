//! Replicate encoded log frames to peer brokers (quorum ack).

mod append;
mod telemetry;

pub use append::{
    CatchUpFrame, ReplicaProgress, ReplicateAppendRequest, ReplicateBatchAck,
    ReplicateBatchRequest, ReplicateCatchUpRange, ReplicateCatchUpRequest,
    ReplicateCatchUpResponse, ReplicateError, ReplicateOutcome, ReplicationClient,
};
pub use telemetry::{
    ReplicationShardTelemetrySnapshot, ReplicationTelemetrySnapshot,
    MAX_REPLICATION_TELEMETRY_SHARDS, QUORUM_BUCKET_MS,
};
