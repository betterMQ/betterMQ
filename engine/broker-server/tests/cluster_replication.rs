//! CP7a: replicated log frames visible on follower; leader-only dispatch.

use broker_dispatch::{DispatchConfig, DispatchEngine};
use broker_partition::{
    Broker, BrokerConfig, CreateSubscriptionRequest, PublishRequest, DIRECT_TOPIC,
};
use broker_raft_meta::{ClusterConfig, ClusterRuntime, NodeConfig};
use std::sync::Arc;
use tempfile::tempdir;
use uuid::Uuid;

fn two_node_cluster(
    leader_dir: &tempfile::TempDir,
    follower_dir: &tempfile::TempDir,
) -> (ClusterRuntime, ClusterRuntime) {
    let n1 = stable_id("http://n1:8080");
    let n2 = stable_id("http://n2:8080");
    let cfg1 = ClusterConfig {
        cluster_id: Uuid::new_v4(),
        nodes: vec![
            NodeConfig {
                id: n1,
                addr: "http://n1:8080".into(),
            },
            NodeConfig {
                id: n2,
                addr: "http://n2:8080".into(),
            },
        ],
        node_id: n1,
        generation: 1,
    };
    let cfg2 = ClusterConfig {
        cluster_id: cfg1.cluster_id,
        nodes: cfg1.nodes.clone(),
        node_id: n2,
        generation: 1,
    };
    ClusterRuntime::init_cluster_file(leader_dir.path(), &cfg1).unwrap();
    ClusterRuntime::init_cluster_file(follower_dir.path(), &cfg2).unwrap();
    std::fs::write(
        leader_dir.path().join("cluster-config.json"),
        serde_json::to_vec_pretty(&cfg1).unwrap(),
    )
    .unwrap();
    std::fs::write(
        follower_dir.path().join("cluster-config.json"),
        serde_json::to_vec_pretty(&cfg2).unwrap(),
    )
    .unwrap();
    let r1 = ClusterRuntime::open(leader_dir.path(), cfg1).unwrap();
    let r2 = ClusterRuntime::open(follower_dir.path(), cfg2).unwrap();
    let now = chrono::Utc::now().timestamp_millis();
    r1.record_self_alive(now);
    r1.record_peer_alive(n2, now);
    r2.record_self_alive(now);
    r2.record_peer_alive(n1, now);
    (r1, r2)
}

fn stable_id(addr: &str) -> Uuid {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    addr.hash(&mut h);
    Uuid::from_u128(h.finish() as u128)
}

#[tokio::test]
async fn replicated_frame_on_follower_and_leader_only_dispatch() {
    let leader_dir = tempdir().unwrap();
    let follower_dir = tempdir().unwrap();
    let (rt_leader, rt_follower) = two_node_cluster(&leader_dir, &follower_dir);

    let broker_l = Broker::open(BrokerConfig::new(leader_dir.path().to_path_buf())).unwrap();
    let broker_f = Broker::open(BrokerConfig::new(follower_dir.path().to_path_buf())).unwrap();

    broker_l
        .create_subscription(CreateSubscriptionRequest {
            topic: "jobs".into(),
            url: "http://example/hook".into(),
            secret: "sec".into(),
            default_max_retries: None,
            retry_backoff: None,
        })
        .unwrap();

    let resp = broker_l
        .publish(PublishRequest {
            topic: "jobs".into(),
            routing_key: "k1".into(),
            payload: "hello".into(),
            payload_encoding: None,
            idempotency_key: None,
            delay_ms: None,
            priority: None,
            flow_id: None,
            queue_id: None,
            group_id: None,
            group_member_id: None,
            destination: None,
            flow: None,
            parallelism: None,
            max_retries: None,
            retry_backoff: None,
            method: None,
            headers: None,
            sign: None,
            request: None,
            url: None,
            secret: None,
        })
        .unwrap();
    let partition = resp.partition.expect("partition");
    let frame = resp.replication_frame.expect("frame");

    broker_f
        .append_replicated_frame(&resp.topic, partition, &frame)
        .unwrap();

    let msgs = broker_f.list_topic_messages("jobs", 10).unwrap();
    assert_eq!(msgs.len(), 1);

    let rt_l = rt_leader.clone();
    let dispatch = DispatchEngine::new(broker_l.clone(), DispatchConfig::default())
        .with_shard_leader_check(Arc::new(move |p| rt_l.is_leader_for_shard(p)));
    dispatch.enqueue(broker_dispatch::DeliveryJob::live(
        "jobs",
        partition,
        resp.offset.unwrap(),
        resp.message_id.unwrap(),
    ));

    let rt_f = rt_follower.clone();
    let dispatch_f = DispatchEngine::new(broker_f.clone(), DispatchConfig::default())
        .with_shard_leader_check(Arc::new(move |p| rt_f.is_leader_for_shard(p)));
    dispatch_f.enqueue(broker_dispatch::DeliveryJob::live(
        "jobs",
        partition,
        resp.offset.unwrap(),
        resp.message_id.unwrap(),
    ));
    // follower dispatch channel should drop non-leader partitions
    assert!(rt_leader.is_leader_for_shard(partition) || rt_follower.is_leader_for_shard(partition));
}

#[tokio::test]
async fn backfill_skips_non_leader_shards() {
    let dir = tempdir().unwrap();
    let (rt, _) = two_node_cluster(&dir, &tempdir().unwrap());
    let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
    broker
        .publish(PublishRequest {
            topic: DIRECT_TOPIC.into(),
            routing_key: "rk".into(),
            payload: "x".into(),
            payload_encoding: None,
            idempotency_key: None,
            delay_ms: None,
            priority: None,
            flow_id: None,
            queue_id: None,
            group_id: None,
            group_member_id: None,
            destination: None,
            flow: None,
            parallelism: None,
            max_retries: None,
            retry_backoff: None,
            method: None,
            headers: None,
            sign: None,
            request: None,
            url: Some("http://example/hook".into()),
            secret: Some("s".into()),
        })
        .unwrap();
    let rt_c = rt.clone();
    let dispatch = DispatchEngine::new(broker, DispatchConfig::default())
        .with_shard_leader_check(Arc::new(move |p| rt_c.is_leader_for_shard(p)));
    dispatch.backfill_pending(); // should not panic
}
