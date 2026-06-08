//! Direct 1:1 publish must not sit behind flow-control waitlists.

use broker_dispatch::{DispatchConfig, DispatchEngine};
use broker_partition::{Broker, BrokerConfig, PublishRequest, DIRECT_TOPIC};
use std::time::{Duration, Instant};
use tempfile::tempdir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn direct_publish_delivers_without_flow_delay() {
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
    let mut first_done = None;
    for i in 0..2u32 {
        let resp = broker
            .publish(PublishRequest {
                topic: String::new(),
                queue_id: None,
                group_id: None,
                group_member_id: None,
                routing_key: format!("rk-{i}"),
                payload: format!("msg-{i}"),
                payload_encoding: None,
                idempotency_key: None,
                delay_ms: None,
                priority: Some(5),
                flow_id: None,
                url: Some(url.clone()),
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
        dispatch.enqueue(broker_dispatch::DeliveryJob::live(
            DIRECT_TOPIC.to_string(),
            resp.partition.unwrap(),
            resp.offset.unwrap(),
            resp.message_id.unwrap(),
        ));

        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            let n = mock.received_requests().await.unwrap_or_default().len() as u32;
            if n > i {
                if i == 0 {
                    first_done = Some(start.elapsed());
                }
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    assert_eq!(mock.received_requests().await.unwrap_or_default().len(), 2);
    let first = first_done.expect("first message should deliver");
    assert!(
        first < Duration::from_secs(2),
        "first direct publish took {:?}; expected immediate delivery",
        first
    );
}
