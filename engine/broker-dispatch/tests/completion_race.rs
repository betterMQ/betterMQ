//! Dispatch completion must survive duplicate jobs without HTTP storms or log holes.

use broker_dispatch::{DispatchConfig, DispatchEngine};
use broker_partition::{Broker, BrokerConfig, PublishRequest, DIRECT_TOPIC};
use std::sync::Once;
use std::time::{Duration, Instant};
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
async fn duplicate_enqueue_does_not_double_deliver_or_lose_record() {
    allow_wiremock_localhost();
    let dir = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
    let dispatch = DispatchEngine::new(
        broker.clone(),
        DispatchConfig {
            retry_defaults: broker_proto::RetryDefaults {
                max_retries: 0,
                backoff: broker_proto::RetryBackoff {
                    initial_ms: 10,
                    max_ms: 50,
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
        .mount(&mock)
        .await;

    let url = format!("{}/hook", mock.uri());
    let resp = broker
        .publish(PublishRequest {
            topic: String::new(),
            queue_id: None,
            group_id: None,
            group_member_id: None,
            routing_key: "rk".into(),
            payload: "once".into(),
            payload_encoding: None,
            idempotency_key: None,
            delay_ms: None,
            priority: None,
            flow_id: None,
            url: Some(url),
            secret: Some("sec".into()),
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
    broker.flush_wal().unwrap();
    let partition = resp.partition.unwrap();
    let offset = resp.offset.unwrap();
    let id = resp.message_id.unwrap();
    let job = broker_dispatch::DeliveryJob::live(DIRECT_TOPIC.to_string(), partition, offset, id);
    dispatch.enqueue(job.clone());
    dispatch.enqueue(job.clone());
    dispatch.enqueue(job);

    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(4) {
        if !mock
            .received_requests()
            .await
            .unwrap_or_default()
            .is_empty()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(mock.received_requests().await.unwrap_or_default().len(), 1);
    assert!(broker.read_message(DIRECT_TOPIC, partition, offset).is_ok());
}
