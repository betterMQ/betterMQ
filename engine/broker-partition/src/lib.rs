//! Single-node broker: topics, publish, push dispatch, purge.

mod blob;
mod broker;
mod flow;
mod flows;
mod groups;
pub mod http_delivery;
pub mod payload;
mod priority;
mod subscriptions;
mod topic;

pub use flow::{delivery_uses_flow_control, FlowSpec, ResolvedFlow};
pub use http_delivery::{
    deserialize_optional_headers, parse_headers_value, HttpDeliveryInput, HttpDeliverySpec,
};
pub use priority::{
    clamp_priority, normalize_priority, DEFAULT_PRIORITY, MAX_PRIORITY, MIN_PRIORITY,
};

pub use broker::{
    Broker, BrokerConfig, BrokerError, CreateSubscriptionRequest, CreateSubscriptionResponse,
    DestinationSnapshot, PublishRequest, PublishResponse, ScheduledInfo, DEFAULT_PARTITIONS,
    DEFAULT_TENANT,
};
pub use flows::{FlowProfile, FlowProfileError, FlowProfileRegistry};
pub use groups::{DispatchGroup, GroupError, GroupMember, GroupRegistry};
pub use subscriptions::{Subscription, SubscriptionRegistry};
pub use topic::{
    dlq_topic, group_member_dlq_topic, group_topic, is_dlq_topic, is_group_topic, partition_for,
    DIRECT_TOPIC,
};

pub use broker_storage::StoredMessage;
