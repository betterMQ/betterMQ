use broker_dispatch::{DeliveryJob, DispatchConfig, DispatchEngine};
use broker_partition::{Broker, BrokerConfig, PublishRequest};
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
async fn graceful_drain_waits_for_active_http() {
    allow_wiremock_localhost();
    let dir = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/slow"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(250)))
        .expect(1)
        .mount(&mock)
        .await;
    let response = broker
        .publish(PublishRequest {
            topic: String::new(),
            queue_id: None,
            group_id: None,
            group_member_id: None,
            routing_key: "drain".into(),
            payload: "body".into(),
            payload_encoding: None,
            idempotency_key: None,
            delay_ms: None,
            priority: None,
            flow_id: None,
            url: Some(format!("{}/slow", mock.uri())),
            secret: Some("secret".into()),
            destination: None,
            flow: None,
            parallelism: None,
            max_retries: Some(0),
            retry_backoff: None,
            method: None,
            headers: None,
            sign: None,
            request: None,
        })
        .unwrap();
    broker.flush_wal().unwrap();
    let dispatch = DispatchEngine::new(broker, DispatchConfig::default());
    dispatch.enqueue(DeliveryJob::live(
        response.topic,
        response.partition.unwrap(),
        response.offset.unwrap(),
        response.message_id.unwrap(),
    ));
    tokio::time::sleep(Duration::from_millis(25)).await;
    let started = Instant::now();
    assert!(dispatch.drain(Duration::from_secs(2)).await);
    assert!(started.elapsed() >= Duration::from_millis(150));
    assert!(dispatch.is_draining());
    let telemetry = dispatch.telemetry_snapshot();
    assert!(telemetry.draining);
    assert_eq!(telemetry.drain_started, 1);
    assert_eq!(telemetry.drain_completed, 1);
    assert_eq!(telemetry.drain_timeouts, 0);
    assert!(telemetry.last_drain_duration_ms >= 150);
    mock.verify().await;
}
