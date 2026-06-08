//! Webhook delivery with HMAC signature.

use broker_dispatch::{DispatchConfig, DispatchEngine};
use broker_partition::{Broker, BrokerConfig, CreateSubscriptionRequest, PublishRequest};
use std::time::Duration;
use tempfile::tempdir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn publish_triggers_webhook_with_signature() {
    let dir = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
    let dispatch = DispatchEngine::new(
        broker.clone(),
        DispatchConfig {
            retry_defaults: broker_proto::RetryDefaults {
                max_retries: 2,
                backoff: broker_proto::RetryBackoff {
                    initial_ms: 50,
                    max_ms: 200,
                    ..broker_proto::RetryBackoff::default()
                },
            },
            ..DispatchConfig::default()
        },
    );

    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock)
        .await;

    broker
        .create_subscription(CreateSubscriptionRequest {
            topic: "hooks".into(),
            url: format!("{}/hook", mock.uri()),
            secret: "whsec_test".into(),
            default_max_retries: None,
            retry_backoff: None,
        })
        .unwrap();

    let resp = broker
        .publish(PublishRequest {
            topic: "hooks".into(),
            queue_id: None,
            group_id: None,
            group_member_id: None,
            routing_key: "rk".into(),
            payload: "ping".into(),
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
            max_retries: Some(2),
            retry_backoff: None,
            method: None,
            headers: None,
            sign: Some(true),
            request: None,
        })
        .unwrap();

    dispatch.enqueue(broker_dispatch::DeliveryJob::live(
        "hooks",
        resp.partition.unwrap(),
        resp.offset.unwrap(),
        resp.message_id.unwrap(),
    ));

    tokio::time::sleep(Duration::from_secs(2)).await;
    mock.verify().await;
}
