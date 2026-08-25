//! Flow control settings (per message and endpoint defaults).

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Limits applied per flow-control key (defaults to message `key` / routing key).
///
/// On **publish** this can be sent as `flow` or `flowControl`.
/// Queue jobs derive limits from the routing key / queue parallelism instead.
/// `period` is accepted as an alias for `period_secs`.
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
    /// Rate window length in seconds (default 1). Alias: `period`.
    #[serde(default, alias = "period")]
    pub period_secs: Option<u64>,
}

impl FlowSpec {
    pub fn effective_key<'a>(&'a self, message_key: &'a str) -> &'a str {
        self.key.as_deref().unwrap_or(message_key)
    }
}

/// Limits frozen onto a queue job at enqueue time.
///
/// - Non-empty routing key → FIFO (parallelism 1) for that key.
/// - Else queue `parallelism` → cap in-flight for the whole queue.
/// - Else unconstrained (standard / high throughput).
pub fn queue_delivery_flow(
    routing_key: &str,
    queue_parallelism: Option<u32>,
    queue_name: &str,
) -> Option<FlowSpec> {
    let key = routing_key.trim();
    if !key.is_empty() {
        return Some(FlowSpec {
            key: Some(key.to_string()),
            parallelism: Some(1),
            rate: None,
            period_secs: None,
        });
    }
    let n = queue_parallelism.filter(|p| *p > 0)?;
    Some(FlowSpec {
        key: Some(queue_name.to_string()),
        parallelism: Some(n),
        rate: None,
        period_secs: None,
    })
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
/// - **false** — unconstrained (SQS-style standard): direct publish or queue enqueue
///   with no `flow_id` / inline limits. High throughput, best-effort order.
/// - **true** — FIFO/rate-limited: group fan-out, queue job with a key or queue parallelism, or publish with a flow profile / limits
pub fn delivery_uses_flow_control(msg: &crate::StoredMessage) -> bool {
    if msg.group_member_id.is_some() {
        return true;
    }
    if msg.flow_profile_id.is_some() {
        return true;
    }
    msg.flow_parallelism.is_some() || msg.flow_rate.is_some() || msg.flow_key.is_some()
}

/// Lane owner UUID for flow control. Direct publish with only `flow` limits
/// has no queue/group/profile id — derive a stable v5 UUID from the flow key.
pub fn flow_lane_owner(msg: &crate::StoredMessage) -> Uuid {
    if let Some(id) = msg.group_member_id.or(msg.queue_id).or(msg.flow_profile_id) {
        return id;
    }
    let key = msg.flow_key.as_deref().unwrap_or(msg.topic.as_str());
    Uuid::new_v5(&Uuid::NAMESPACE_URL, key.as_bytes())
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

    /// FIFO fallback when flow control is on but the message has no frozen limits.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StoredMessage;
    use uuid::Uuid;

    fn msg_with_flow_key(key: &str) -> StoredMessage {
        StoredMessage {
            id: Uuid::nil(),
            tenant_id: "t".into(),
            topic: "orders".into(),
            partition: 0,
            offset: 0,
            routing_key: "rk".into(),
            payload: vec![],
            published_at_ms: 0,
            priority: 4,
            flow_parallelism: Some(1),
            flow_key: Some(key.into()),
            flow_rate: None,
            flow_period_secs: None,
            queue_id: None,
            group_id: None,
            group_member_id: None,
            flow_profile_id: None,
            destination_url: None,
            destination_secret: None,
            max_retries: 0,
            retry_backoff: None,
            http_method: None,
            http_headers_json: None,
            http_sign: None,
            payload_ref_json: None,
        }
    }

    #[test]
    fn flow_lane_owner_without_ids_is_stable() {
        let a = flow_lane_owner(&msg_with_flow_key("k"));
        let b = flow_lane_owner(&msg_with_flow_key("k"));
        assert_eq!(a, b);
        assert_ne!(a, Uuid::nil());
        assert!(delivery_uses_flow_control(&msg_with_flow_key("k")));
    }

    #[test]
    fn standard_queue_skips_flow_control() {
        let mut msg = msg_with_flow_key("k");
        msg.flow_parallelism = None;
        msg.flow_key = None;
        msg.queue_id = Some(Uuid::nil());
        assert!(!delivery_uses_flow_control(&msg));
    }

    #[test]
    fn queue_delivery_flow_key_is_fifo() {
        let spec = queue_delivery_flow("user-42", Some(8), "jobs").unwrap();
        assert_eq!(spec.key.as_deref(), Some("user-42"));
        assert_eq!(spec.parallelism, Some(1));
    }

    #[test]
    fn queue_delivery_flow_plain_uses_queue_parallelism() {
        let spec = queue_delivery_flow("", Some(8), "jobs").unwrap();
        assert_eq!(spec.key.as_deref(), Some("jobs"));
        assert_eq!(spec.parallelism, Some(8));
        assert!(queue_delivery_flow("  ", None, "jobs").is_none());
        assert!(queue_delivery_flow("", Some(0), "jobs").is_none());
    }
}
