//! Flow control settings (per message and endpoint defaults).

use serde::{Deserialize, Serialize};

/// Limits applied per flow-control key (defaults to message `key` / routing key).
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct FlowSpec {
    /// Grouping key for rate + parallelism.
    #[serde(default)]
    pub key: Option<String>,
    /// Max in-flight deliveries. `1` = strict FIFO (+ priority) for this key.
    #[serde(default)]
    pub parallelism: Option<u32>,
    /// Max deliveries started per period. `0` or omit = no rate limit.
    #[serde(default)]
    pub rate: Option<u32>,
    /// Rate window length in seconds (default 1).
    #[serde(default)]
    pub period_secs: Option<u64>,
}

impl FlowSpec {
    pub fn effective_key<'a>(&'a self, message_key: &'a str) -> &'a str {
        self.key.as_deref().unwrap_or(message_key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedFlow {
    pub key: String,
    pub parallelism: u32,
    pub rate: u32,
    pub period_secs: u64,
}

impl FlowSpec {
    pub fn from_stored(msg: &crate::StoredMessage) -> Self {
        Self {
            key: msg.flow_key.clone(),
            parallelism: msg.flow_parallelism,
            rate: msg.flow_rate,
            period_secs: msg.flow_period_secs,
        }
    }
}

/// Whether delivery should go through the flow controller (FIFO / rate / parallelism).
///
/// - **false** — `POST /v1/publish` 1:1 jobs (no queue, no `flow_id`, no inline flow limits)
/// - **true** — queue enqueue (FIFO by default), or any message with a flow profile / limits
pub fn delivery_uses_flow_control(msg: &crate::StoredMessage) -> bool {
    if msg.group_member_id.is_some() {
        return true;
    }
    if msg.queue_id.is_some() {
        return true;
    }
    if msg.flow_profile_id.is_some() {
        return true;
    }
    msg.flow_parallelism.is_some() || msg.flow_rate.is_some() || msg.flow_key.is_some()
}

impl ResolvedFlow {
    /// Limits frozen on the message at publish/enqueue time.
    pub fn for_delivery(message_key: &str, msg: &crate::StoredMessage) -> Self {
        if !delivery_uses_flow_control(msg) {
            return Self::resolve(message_key, None, None, None);
        }
        if msg.flow_parallelism.is_some() || msg.flow_rate.is_some() || msg.flow_key.is_some() {
            return Self::resolve(message_key, Some(&FlowSpec::from_stored(msg)), None, None);
        }
        Self::for_queue_default(message_key)
    }

    /// Queue default: strict ordering by priority when no flow profile is attached.
    pub fn for_queue_default(message_key: &str) -> Self {
        Self::resolve(
            message_key,
            Some(&FlowSpec {
                key: None,
                parallelism: Some(1),
                rate: None,
                period_secs: None,
            }),
            None,
            None,
        )
    }

    pub fn resolve(
        message_key: &str,
        msg: Option<&FlowSpec>,
        endpoint: Option<&FlowSpec>,
        pinned: Option<&ResolvedFlow>,
    ) -> Self {
        if let Some(p) = pinned {
            return p.clone();
        }
        let key = msg
            .and_then(|f| f.key.as_deref())
            .or(endpoint.and_then(|f| f.key.as_deref()))
            .unwrap_or(message_key)
            .to_string();
        let parallelism = msg
            .and_then(|f| f.parallelism)
            .or(endpoint.and_then(|f| f.parallelism))
            .unwrap_or(4)
            .max(1);
        let rate = msg
            .and_then(|f| f.rate)
            .or(endpoint.and_then(|f| f.rate))
            .unwrap_or(0);
        let period_secs = msg
            .and_then(|f| f.period_secs)
            .or(endpoint.and_then(|f| f.period_secs))
            .unwrap_or(1)
            .max(1);
        Self {
            key,
            parallelism,
            rate,
            period_secs,
        }
    }
}
