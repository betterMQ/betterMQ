//! Replicate encoded log frames to peer brokers (quorum ack).

mod append;

pub use append::{ReplicateAppendRequest, ReplicateError, ReplicationClient};
