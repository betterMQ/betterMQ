//! Deterministic shard leader election with ring failover (CP7b).
//!
//! Preferred leader is `shard % node_count`. If that node is unhealthy, walk the ring
//! to the next alive peer. All nodes use the same health view → same elected leader
//! (safe for single-node failure; not split-brain safe under network partition).

use crate::cluster::NodeConfig;
use uuid::Uuid;

pub const DEFAULT_PEER_TTL_MS: i64 = 10_000;

/// Walk the ring from the preferred index; return the first alive node id.
pub fn elect_shard_leader(
    nodes: &[NodeConfig],
    shard: u32,
    is_alive: impl Fn(Uuid) -> bool,
) -> Option<Uuid> {
    if nodes.is_empty() {
        return None;
    }
    let n = nodes.len();
    let start = (shard as usize) % n;
    for step in 0..n {
        let idx = (start + step) % n;
        let id = nodes[idx].id;
        if is_alive(id) {
            return Some(id);
        }
    }
    Some(nodes[start].id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn nodes(ids: &[Uuid]) -> Vec<NodeConfig> {
        ids.iter()
            .enumerate()
            .map(|(i, id)| NodeConfig {
                id: *id,
                addr: format!("http://n{i}:8080"),
            })
            .collect()
    }

    #[test]
    fn prefers_static_leader_when_healthy() {
        let ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
        let list = nodes(&ids);
        let alive = |_| true;
        assert_eq!(elect_shard_leader(&list, 0, alive), Some(ids[0]));
        assert_eq!(elect_shard_leader(&list, 1, alive), Some(ids[1]));
        assert_eq!(elect_shard_leader(&list, 2, alive), Some(ids[2]));
    }

    #[test]
    fn fails_over_to_next_alive() {
        let ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
        let list = nodes(&ids);
        let dead_middle = ids[1];
        let alive = |id: Uuid| id != dead_middle;
        // shard 1 prefers index 1 (dead) → should pick index 2
        assert_eq!(elect_shard_leader(&list, 1, alive), Some(ids[2]));
        // shard 0 prefers index 0 (alive)
        assert_eq!(elect_shard_leader(&list, 0, alive), Some(ids[0]));
    }

    #[test]
    fn fails_over_when_preferred_dead() {
        let ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
        let list = nodes(&ids);
        let dead = ids[0];
        let alive = |id: Uuid| id != dead;
        assert_eq!(elect_shard_leader(&list, 0, alive), Some(ids[1]));
        assert_eq!(elect_shard_leader(&list, 3, alive), Some(ids[1]));
    }
}
