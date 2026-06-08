use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

/// Internal topic for one-off `POST /v1/publish` jobs (URL on the message, not a named queue).
pub const DIRECT_TOPIC: &str = "__direct";

/// Dead-letter queue topic for a primary queue (`jobs` → `jobs.__dlq`).
pub fn dlq_topic(topic: &str) -> String {
    format!("{topic}.__dlq")
}

/// Log topic for all messages published to a fan-out group.
pub fn group_topic(group_id: uuid::Uuid) -> String {
    format!("__group.{group_id}")
}

/// DLQ for a single member inside a group.
pub fn group_member_dlq_topic(group_id: uuid::Uuid, member_id: uuid::Uuid) -> String {
    format!("__group.{group_id}.{member_id}.__dlq")
}

pub fn is_group_topic(topic: &str) -> bool {
    topic.starts_with("__group.")
}

/// DLQ topics are retained for monitoring; primary queues are ephemeral after delivery.
pub fn is_dlq_topic(topic: &str) -> bool {
    topic.ends_with(".__dlq")
}

pub fn partition_for(tenant_id: &str, topic: &str, routing_key: &str, partitions: u32) -> u32 {
    let mut hasher = DefaultHasher::new();
    tenant_id.hash(&mut hasher);
    topic.hash(&mut hasher);
    routing_key.hash(&mut hasher);
    (hasher.finish() % partitions as u64) as u32
}

pub fn partition_dir(data_dir: &Path, tenant_id: &str, topic: &str, partition: u32) -> PathBuf {
    data_dir
        .join("partitions")
        .join(tenant_id)
        .join(topic)
        .join(format!("p{partition}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_partition() {
        let p1 = partition_for("t", "orders", "a", 4);
        let p2 = partition_for("t", "orders", "a", 4);
        assert_eq!(p1, p2);
        assert!(p1 < 4);
    }
}
