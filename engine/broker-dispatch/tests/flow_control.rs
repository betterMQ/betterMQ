//! Flow control: parallelism=1 is strict FIFO; parallelism>1 allows overlap.

use broker_dispatch::{DispatchConfig, DispatchEngine};
use broker_partition::{Broker, BrokerConfig, CreateSubscriptionRequest, PublishRequest};
use std::sync::Once;
use std::sync::{Arc, Mutex};
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
async fn fifo_parallelism_one_ignores_priority() {
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
    let order = Arc::new(Mutex::new(Vec::<String>::new()));
    let order_cap = order.clone();

    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(move |req: &wiremock::Request| {
            let body = String::from_utf8_lossy(&req.body).into_owned();
            order_cap.lock().unwrap().push(body);
            ResponseTemplate::new(200)
        })
        .mount(&mock)
        .await;

    broker
        .create_subscription(CreateSubscriptionRequest {
            topic: "fc".into(),
            url: format!("{}/hook", mock.uri()),
            secret: "sec".into(),
            parallelism: None,
            default_max_retries: None,
            retry_backoff: None,
        })
        .unwrap();

    let rk = "user-42";
    let mut published = Vec::new();
    for (payload, priority) in [("a", Some(1u8)), ("b", Some(9)), ("c", Some(5))] {
        let resp = broker
            .publish(PublishRequest {
                topic: "fc".into(),
                queue_id: None,
                group_id: None,
                group_member_id: None,
                routing_key: rk.into(),
                payload: payload.into(),
                payload_encoding: None,
                idempotency_key: None,
                delay_ms: None,
                priority,
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
        published.push((
            resp.partition.unwrap(),
            resp.offset.unwrap(),
            resp.message_id.unwrap(),
        ));
    }
    broker.flush_wal().unwrap();

    for (partition, offset, message_id) in published {
        dispatch.enqueue(broker_dispatch::DeliveryJob::live(
            "fc", partition, offset, message_id,
        ));
    }

    tokio::time::sleep(Duration::from_secs(3)).await;
    let seen = order.lock().unwrap().clone();
    assert_eq!(
        seen,
        vec!["a", "b", "c"],
        "parallelism=1 must preserve publish order"
    );
}
