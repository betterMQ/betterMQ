//! Async webhook delivery for committed log records.

mod egress;
mod fairness;
mod flow_control;
mod hmac_sig;
mod host_blocker;
mod lease;
mod memory_guard;
mod outbound;
mod worker;

pub use egress::{validate_destination_url, validate_destination_url_resolved, EgressError};
pub use fairness::TenantFairQueue;

pub use flow_control::{FlowControlInfo, FlowController, FlowKey, GlobalParallelismInfo};
pub use host_blocker::{HostBlocker, HostBlockerConfig};
pub use lease::{
    claim_from_broker, fleet_concurrency, long_wait_tier_secs, ClaimRequest, ClaimResponse,
    ClaimedJob, CompleteRequest, FailRequest, HeartbeatRequest, LeaseClient, LeaseError,
    LeaseTable,
};
pub use memory_guard::{
    sample_process_resources, MemoryGuard, MemoryGuardConfig, ProcessResourceStats,
};
pub use worker::{DeliveryJob, DeliveryPriority, DispatchConfig, DispatchEngine, DispatchError};
