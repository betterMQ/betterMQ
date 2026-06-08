//! CP7b: shard leadership fails over when the preferred broker is unhealthy.

use broker_raft_meta::{ClusterConfig, ClusterRuntime, NodeConfig};
use uuid::Uuid;

fn three_node_runtime(this: usize) -> (ClusterRuntime, Vec<Uuid>) {
    let ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
    let nodes: Vec<NodeConfig> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| NodeConfig {
            id: *id,
            addr: format!("http://broker{i}:8080"),
        })
        .collect();
    let cfg = ClusterConfig {
        cluster_id: Uuid::new_v4(),
        nodes,
        node_id: ids[this],
        generation: 1,
    };
    (ClusterRuntime::from_config_only(cfg), ids)
}

#[test]
fn all_healthy_keeps_static_shard_owners() {
    let (rt0, ids) = three_node_runtime(0);
    let (rt1, _) = three_node_runtime(1);
    let now = chrono::Utc::now().timestamp_millis();
    for rt in [&rt0, &rt1] {
        rt.record_self_alive(now);
        for id in &ids {
            rt.record_peer_alive(*id, now);
        }
    }
    assert!(rt0.is_leader_for_shard(0));
    assert!(!rt0.is_leader_for_shard(1));
    assert!(rt1.is_leader_for_shard(1));
}

#[test]
fn broker1_down_shard1_moves_to_broker2() {
    let (rt2, ids) = three_node_runtime(2);
    let now = chrono::Utc::now().timestamp_millis();
    rt2.record_self_alive(now);
    rt2.record_peer_alive(ids[0], now);
    // broker1 (ids[1]) not healthy — preferred leader for shard 1
    assert_eq!(rt2.elect_leader_for_shard(1), Some(ids[2]));
    assert!(rt2.is_leader_for_shard(1));
}

#[test]
fn sole_survivor_leads_all_shards() {
    let (rt, ids) = three_node_runtime(2);
    let now = chrono::Utc::now().timestamp_millis();
    rt.record_self_alive(now);
    for shard in 0..4 {
        assert_eq!(rt.elect_leader_for_shard(shard), Some(ids[2]));
    }
}
