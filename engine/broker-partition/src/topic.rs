use broker_proto::{join_under_root, sanitize_path_segment, stable_partition};
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
    stable_partition(tenant_id, topic, routing_key, partitions)
}

pub fn partition_dir(
    data_dir: &Path,
    tenant_id: &str,
    topic: &str,
    partition: u32,
) -> Result<PathBuf, broker_proto::PathSegmentError> {
    let tenant = sanitize_path_segment(tenant_id)?;
    let topic = sanitize_path_segment(topic)?;
    join_under_root(
        data_dir,
        &["partitions", tenant, topic, &format!("p{partition}")],
    )
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

    #[test]
    fn partition_dir_rejects_traversal() {
        let root = Path::new("/data");
        assert!(partition_dir(root, "default", "../etc", 0).is_err());
        assert!(partition_dir(root, "..", "orders", 0).is_err());
        let ok = partition_dir(root, "default", "jobs.__dlq", 0).unwrap();
        assert!(ok.starts_with(root));
    }
}
