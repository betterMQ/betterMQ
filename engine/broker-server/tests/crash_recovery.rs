//! Simulates crash by closing broker and reopening on same data dir.

use broker_partition::{Broker, BrokerConfig, CreateSubscriptionRequest, PublishRequest};
use std::collections::HashSet;
use tempfile::tempdir;

#[test]
fn messages_survive_reopen() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    let mut expected = HashSet::new();
    {
        let broker = Broker::open(BrokerConfig::new(data_dir.clone())).unwrap();
        broker
            .create_subscription(CreateSubscriptionRequest {
                topic: "events".into(),
                url: "http://127.0.0.1:1/pull-only".into(),
                secret: "sec".into(),
                default_max_retries: None,
                retry_backoff: None,
            })
            .unwrap();
        for i in 0..100 {
            let resp = broker
                .publish(PublishRequest {
                    topic: "events".into(),
                    queue_id: None,
                    group_id: None,
                    group_member_id: None,
                    routing_key: format!("key-{i}"),
                    payload: format!("payload-{i}"),
                    payload_encoding: None,
                    idempotency_key: None,
                    delay_ms: None,
                    priority: None,
                    flow_id: None,
                    url: None,
                    secret: None,
                    destination: None,
                    flow: None,
                    parallelism: None,
                    max_retries: None,
                    retry_backoff: None,
                    method: None,
                    headers: None,
                    sign: None,
                    request: None,
                })
                .unwrap();
            expected.insert((resp.partition.unwrap(), resp.offset.unwrap()));
        }
    }

    let broker = Broker::open(BrokerConfig::new(data_dir)).unwrap();
    let batch = broker.list_topic_messages("events", 200).unwrap();

    assert_eq!(batch.len(), 100);
    let found: HashSet<_> = batch.iter().map(|m| (m.partition, m.offset)).collect();
    assert_eq!(found, expected);
}

#[test]
#[ignore = "run: cargo test -p broker-server --test crash_recovery ten_thousand -- --ignored"]
fn ten_thousand_messages_durable() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    {
        let broker = Broker::open(BrokerConfig::new(data_dir.clone())).unwrap();
        broker
            .create_subscription(CreateSubscriptionRequest {
                topic: "load".into(),
                url: "http://127.0.0.1:1/pull-only".into(),
                secret: "sec".into(),
                default_max_retries: None,
                retry_backoff: None,
            })
            .unwrap();
        for i in 0..10_000 {
            broker
                .publish(PublishRequest {
                    topic: "load".into(),
                    queue_id: None,
                    group_id: None,
                    group_member_id: None,
                    routing_key: format!("k{i}"),
                    payload: "x".repeat(64),
                    payload_encoding: None,
                    idempotency_key: None,
                    delay_ms: None,
                    priority: None,
                    flow_id: None,
                    url: None,
                    secret: None,
                    destination: None,
                    flow: None,
                    parallelism: None,
                    max_retries: None,
                    retry_backoff: None,
                    method: None,
                    headers: None,
                    sign: None,
                    request: None,
                })
                .unwrap();
        }
    }

    let broker = Broker::open(BrokerConfig::new(data_dir)).unwrap();
    let batch = broker.list_topic_messages("load", 10_000).unwrap();
    assert_eq!(batch.len(), 10_000);
}
