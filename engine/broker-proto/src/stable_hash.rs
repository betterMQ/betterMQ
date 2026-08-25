//! Versioned SipHash-1-3 so node IDs and partition assignment stay stable
//! across Rust toolchain upgrades (`DefaultHasher` is not guaranteed stable).

use siphasher::sip::SipHasher13;
use std::hash::{Hash, Hasher};
use uuid::Uuid;

/// Hash algorithm version stored on `ClusterConfig`.
pub const HASH_VERSION: u32 = 1;
/// Canonical flow-lane routing used by layout V2 physical shards.
pub const HASH_VERSION_V2: u32 = 2;

/// "bettermq" as bytes interpreted as u64 BE.
const K0: u64 = 0x6265_7474_6572_6d71;
/// "hashv1\0\x01"
const K1: u64 = 0x6861_7368_7631_0001;

fn sip13() -> SipHasher13 {
    SipHasher13::new_with_keys(K0, K1)
}

pub fn stable_hash_u64(bytes: &[u8]) -> u64 {
    let mut h = sip13();
    h.write(bytes);
    h.finish()
}

/// Deterministic UUID from a name or address (cluster node identity).
pub fn stable_node_id(key: &str) -> Uuid {
    let a = stable_hash_u64(key.as_bytes());
    let b = stable_hash_u64(format!("bettermq:{key}").as_bytes());
    Uuid::from_u128((a as u128) | ((b as u128) << 64))
}

/// Partition assignment — same inputs always map to the same shard.
pub fn stable_partition(tenant_id: &str, topic: &str, routing_key: &str, partitions: u32) -> u32 {
    if partitions == 0 {
        return 0;
    }
    let mut h = sip13();
    tenant_id.hash(&mut h);
    topic.hash(&mut h);
    routing_key.hash(&mut h);
    (h.finish() % u64::from(partitions)) as u32
}

/// Layout v2 physical shard: lane stays ordered, topic is not part of placement.
pub fn stable_physical_shard(tenant_id: &str, lane_key: &str, shards: u32) -> u32 {
    stable_partition(tenant_id, "", lane_key, shards)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_is_deterministic() {
        let a = stable_node_id("broker1");
        let b = stable_node_id("broker1");
        assert_eq!(a, b);
        assert_ne!(a, stable_node_id("broker2"));
    }

    #[test]
    fn node_id_golden_vector() {
        // Pinned so a siphasher/key change is a visible test failure.
        assert_eq!(
            stable_node_id("broker1").to_string(),
            "f65058f7-24a3-6df6-b8f3-72e54655c3ff"
        );
    }

    #[test]
    fn partition_golden_vector() {
        let p = stable_partition("default", "orders", "rk-a", 4);
        assert_eq!(p, stable_partition("default", "orders", "rk-a", 4));
        assert!(p < 4);
        assert_eq!(p, 1);
    }

    #[test]
    fn zero_partitions_does_not_div0() {
        assert_eq!(stable_partition("t", "q", "k", 0), 0);
    }

    #[test]
    fn physical_shard_ignores_topic() {
        let a = stable_physical_shard("default", "rk-a", 256);
        let b = stable_physical_shard("default", "rk-a", 256);
        assert_eq!(a, b);
        assert!(a < 256);
    }
}
