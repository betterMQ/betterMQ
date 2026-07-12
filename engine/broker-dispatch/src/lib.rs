//! Async webhook delivery for committed log records.

mod egress;
mod fairness;
mod flow_control;
mod hmac_sig;
mod host_blocker;
mod memory_guard;
mod outbound;
mod worker;

pub use egress::{validate_destination_url, EgressError};
pub use fairness::TenantFairQueue;

pub use flow_control::{FlowControlInfo, FlowController, FlowKey, GlobalParallelismInfo};
pub use host_blocker::{HostBlocker, HostBlockerConfig};
pub use memory_guard::{
    sample_process_resources, MemoryGuard, MemoryGuardConfig, ProcessResourceStats,
};
pub use worker::{DeliveryJob, DeliveryPriority, DispatchConfig, DispatchEngine};

pub use broker_partition::dlq_topic;
