//! Retries exhaust → message lands in DLQ.

use broker_dispatch::{DispatchConfig, DispatchEngine};
use broker_partition::{Broker, BrokerConfig, CreateSubscriptionRequest, PublishRequest};
use broker_proto::{RetryBackoff, RetryBackoffKind, RetryDefaults};
use std::time::Duration;
use tempfile::tempdir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn failed_delivery_moves_to_dlq_after_retries() {
    let dir = tempdir().unwrap();
    let mut cfg = BrokerConfig::new(dir.path().to_path_buf());
    cfg.retry_defaults = RetryDefaults {
        max_retries: 0,
        ..RetryDefaults::default()
    };
    let broker = Broker::open(cfg).unwrap();
    let dispatch = DispatchEngine::new(
        broker.clone(),
        DispatchConfig {
            retry_defaults: RetryDefaults {
                max_retries: 0,
                backoff: RetryBackoff {
                    kind: RetryBackoffKind::Fixed,
                    initial_ms: 5,
                    max_ms: 5,
                    multiplier: 1.0,
                },
            },
            ..DispatchConfig::default()
        },
    );

    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/fail"))
        .respond_with(ResponseTemplate::new(500))
        .expect(2) // initial + 1 retry (max_retries=1 on message)
        .mount(&mock)
        .await;

    broker
        .create_subscription(CreateSubscriptionRequest {
            topic: "jobs".into(),
            url: format!("{}/fail", mock.uri()),
            secret: "whsec_test".into(),
            default_max_retries: None,
            retry_backoff: None,
        })
        .unwrap();

    let resp = broker
        .publish(PublishRequest {
            topic: "jobs".into(),
            queue_id: None,
            group_id: None,
            group_member_id: None,
            routing_key: "rk".into(),
            payload: "x".into(),
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
            max_retries: Some(1),
            retry_backoff: Some(RetryBackoff {
                kind: RetryBackoffKind::Fixed,
                initial_ms: 5,
                max_ms: 5,
                multiplier: 1.0,
            }),
            method: None,
            headers: None,
            sign: None,
            request: None,
        })
        .unwrap();

    dispatch.enqueue(broker_dispatch::DeliveryJob::live(
        "jobs",
        resp.partition.unwrap(),
        resp.offset.unwrap(),
        resp.message_id.unwrap(),
    ));

    tokio::time::sleep(Duration::from_secs(2)).await;
    mock.verify().await;

    let dlq = broker.list_topic_messages("jobs.__dlq", 10).unwrap();
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0].topic, "jobs.__dlq");
}
