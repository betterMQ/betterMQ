use broker_proto::LogRecord;
use broker_storage::{FsyncMode, PartitionLog, PartitionLogConfig};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::time::Duration;
use tempfile::tempdir;
use uuid::Uuid;

fn sample() -> LogRecord {
    LogRecord {
        id: Uuid::new_v4(),
        tenant_id: "default".into(),
        topic: "t".into(),
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

fn bench_wal(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let cfg = PartitionLogConfig {
        segment_max_bytes: 64 * 1024 * 1024,
        fsync: FsyncMode::Group,
        group_interval: Duration::from_secs(60),
    };
    let mut log = PartitionLog::open(dir.path(), cfg).unwrap();
    let payload = vec![0u8; 1024];
    c.bench_function("wal_append_1kib_group", |b| {
        b.iter(|| {
            log.append(0, sample(), black_box(payload.clone())).unwrap();
        })
    });
    let _ = log.sync();
}

criterion_group!(benches, bench_wal);
criterion_main!(benches);
