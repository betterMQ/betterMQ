//! Single-node broker: topics, publish, push dispatch, purge.

mod blob;
mod broker;
mod catalog_journal;
mod flow;
mod flows;
mod groups;
pub mod http_delivery;
mod layout;
pub mod payload;
mod priority;
mod shard;
mod subscriptions;
mod telemetry;
mod tenant_scope;
mod topic;

pub use flow::{
    delivery_uses_flow_control, flow_lane_owner, queue_delivery_flow, FlowSpec, ResolvedFlow,
};
pub use http_delivery::{
    deserialize_optional_headers, parse_headers_value, HttpDeliveryInput, HttpDeliverySpec,
};
pub use priority::{
    clamp_priority, normalize_priority, DEFAULT_PRIORITY, MAX_PRIORITY, MIN_PRIORITY,
};

pub use broker::{
    AppendedRecordRef, AppendedShardBatch, Broker, BrokerConfig, BrokerError,
    CreateSubscriptionRequest, CreateSubscriptionResponse, DestinationSnapshot,
    PreparedPublishBatch, PreparedShardBatch, PreparedShardKey, PublishRequest, PublishResponse,
    ScheduledInfo, DEFAULT_PARTITIONS, DEFAULT_TENANT,
};
pub use flows::{FlowProfile, FlowProfileError, FlowProfileRegistry};
pub use groups::{DispatchGroup, GroupError, GroupMember, GroupRegistry};
pub use subscriptions::{Subscription, SubscriptionRegistry};
pub use tenant_scope::{effective_tenant, scope_tenant};
pub use topic::{
    dlq_topic, group_member_dlq_topic, group_topic, is_dlq_topic, is_group_topic, partition_for,
    DIRECT_TOPIC,
};

pub use broker_storage::{DedupEntry, StoredMessage};
pub use layout::{ShardLayout, DEFAULT_V2_SHARDS, LAYOUT_V1, LAYOUT_V2};
pub use shard::{CommitTicket, ShardHandle};
pub use telemetry::{
    partition_telemetry_snapshot, PartitionTelemetrySnapshot, ShardQueueTelemetrySnapshot,
    MAX_QUEUE_TELEMETRY_SHARDS,
};
