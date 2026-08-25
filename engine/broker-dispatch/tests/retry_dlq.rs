//! Retries exhaust → message lands in DLQ.

use broker_dispatch::{DispatchConfig, DispatchEngine};
use broker_partition::{Broker, BrokerConfig, CreateSubscriptionRequest, PublishRequest};
use broker_proto::{RetryBackoff, RetryBackoffKind, RetryDefaults};
use std::sync::Once;
use std::time::Duration;
use tempfile::tempdir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn allow_wiremock_localhost() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        std::env::set_var("BETTERMQ_ALLOW_PRIVATE_DESTINATIONS", "1");
    });
}

#[tokio::test]
async fn failed_delivery_moves_to_dlq_after_retries() {
    allow_wiremock_localhost();
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
            parallelism: None,
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
    broker.flush_wal().unwrap();

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
    let telemetry = dispatch.telemetry_snapshot();
    assert_eq!(telemetry.retries_scheduled, 1);
    assert!(telemetry.retries_due >= 1);
    assert_eq!(telemetry.retries_exhausted, 1);
    assert_eq!(telemetry.dlq_prepared, 1);
    assert_eq!(telemetry.dlq_committed, 1);
    assert_eq!(telemetry.dlq_failures, 0);
}

#[tokio::test]
async fn fleet_delivery_honors_message_retries_before_dlq() {
    allow_wiremock_localhost();
    let dir = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
    let dispatch = DispatchEngine::new_broker_only(
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
        .and(path("/fleet-fail"))
        .respond_with(ResponseTemplate::new(503))
        .expect(3)
        .mount(&mock)
        .await;

    let response = broker
        .publish(PublishRequest {
            topic: String::new(),
            queue_id: None,
            group_id: None,
            group_member_id: None,
            routing_key: "fleet".into(),
            payload: "body".into(),
            payload_encoding: None,
            idempotency_key: None,
            delay_ms: None,
            priority: None,
            flow_id: None,
            url: Some(format!("{}/fleet-fail", mock.uri())),
            secret: Some("secret".into()),
            destination: None,
            flow: None,
            parallelism: None,
            max_retries: Some(2),
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
    broker.flush_wal().unwrap();
    let partition = response.partition.unwrap();
    let offset = response.offset.unwrap();
    let message = broker
        .read_message(&response.topic, partition, offset)
        .unwrap();

    let uncommitted = dispatch.push_http_only(&message, offset).await.unwrap_err();
    assert!(matches!(
        uncommitted,
        broker_dispatch::DispatchError::Uncommitted
    ));
    assert!(mock
        .received_requests()
        .await
        .unwrap_or_default()
        .is_empty());
    let first_retry = dispatch
        .push_http_only(&message, offset + 1)
        .await
        .unwrap_err();
    let broker_dispatch::DispatchError::RetryDeferred { retry_after_ms, .. } = first_retry else {
        panic!("first fleet failure must persist a deferred retry");
    };
    drop(dispatch);
    tokio::time::sleep(Duration::from_millis(retry_after_ms)).await;
    let dispatch = DispatchEngine::new_broker_only(broker.clone(), DispatchConfig::default());
    let second_retry = dispatch
        .push_http_only(&message, offset + 1)
        .await
        .unwrap_err();
    let broker_dispatch::DispatchError::RetryDeferred { retry_after_ms, .. } = second_retry else {
        panic!("second fleet failure must remain deferred");
    };
    tokio::time::sleep(Duration::from_millis(retry_after_ms)).await;
    let exhausted = dispatch
        .push_http_only(&message, offset + 1)
        .await
        .unwrap_err();
    assert!(matches!(
        exhausted,
        broker_dispatch::DispatchError::RetryExhausted(_)
    ));
    mock.verify().await;
    dispatch
        .dead_letter_offset(
            &response.topic,
            partition,
            offset,
            "fleet retries exhausted",
        )
        .await
        .unwrap();
    assert_eq!(
        broker
            .list_topic_messages(&broker_partition::dlq_topic(&response.topic), 10)
            .unwrap()
            .len(),
        1
    );
    let telemetry = dispatch.telemetry_snapshot();
    assert!(telemetry.retries_scheduled >= 1);
    assert!(telemetry.retries_due >= 2);
    assert_eq!(telemetry.retries_exhausted, 1);
    assert_eq!(telemetry.dlq_committed, 1);
}
