use broker_proto::{encode_frame_vec, LogRecord};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use uuid::Uuid;

fn sample() -> LogRecord {
    LogRecord {
        id: Uuid::nil(),
        tenant_id: "default".into(),
        topic: "orders".into(),
        routing_key: "rk".into(),
        idempotency_key: None,
        published_at_ms: 0,
        priority: 5,
        flow_parallelism: None,
        flow_key: None,
        flow_rate: None,
        flow_period_secs: None,
        queue_id: None,
        group_id: None,
        group_member_id: None,
        flow_profile_id: None,
        destination_url: None,
        destination_secret: None,
        max_retries: 0,
        retry_backoff: None,
        http_method: None,
        http_headers_json: None,
        http_sign: None,
        payload_ref_json: None,
    }
}

fn bench_encode(c: &mut Criterion) {
    let header = sample();
    let payload = vec![0u8; 1024];
    c.bench_function("encode_frame_1kib", |b| {
        b.iter(|| encode_frame_vec(black_box(&header), black_box(&payload)).unwrap())
    });
}

fn bench_hash(c: &mut Criterion) {
    c.bench_function("stable_partition", |b| {
        b.iter(|| {
            broker_proto::stable_partition(
                black_box("default"),
                black_box("orders"),
                black_box("rk-a"),
                black_box(256),
            )
        })
    });
}

criterion_group!(benches, bench_encode, bench_hash);
criterion_main!(benches);
