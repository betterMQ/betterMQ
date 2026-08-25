use broker_partition::{partition_telemetry_snapshot, ShardHandle};
use broker_proto::LogRecord;
use broker_storage::{FsyncMode, PartitionBackend, PartitionLogConfig, WAL_FORMAT_V2};
use std::time::Duration;
use tempfile::tempdir;
use uuid::Uuid;

fn record(sequence: u64) -> LogRecord {
    LogRecord {
        id: Uuid::from_u128(u128::from(sequence) + 100),
        tenant_id: "queue-telemetry".into(),
        topic: "events".into(),
        routing_key: "lane".into(),
        idempotency_key: None,
        published_at_ms: sequence as i64,
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

#[test]
fn shard_actor_records_queue_records_and_bytes() {
    const SHARD: u32 = 3_902;
    let directory = tempdir().unwrap();
    let config = PartitionLogConfig {
        fsync: FsyncMode::Group,
        group_interval: Duration::from_secs(60),
        ..PartitionLogConfig::default()
    };
    let backend =
        PartitionBackend::open_local_for_shard(directory.path(), config, SHARD, WAL_FORMAT_V2)
            .unwrap();
    let handle = ShardHandle::new(backend);
    handle
        .append_batch(
            SHARD,
            vec![(record(0), vec![b'a'; 100]), (record(1), vec![b'b'; 200])],
            None,
        )
        .unwrap();

    let snapshot = partition_telemetry_snapshot();
    let shard = snapshot
        .shards
        .iter()
        .find(|item| item.shard == SHARD)
        .unwrap();
    assert_eq!(shard.queued_records, 0);
    assert_eq!(shard.queued_bytes, 0);
    assert!(shard.max_queued_records >= 2);
    assert!(shard.max_queued_bytes >= 300);
    assert_eq!(shard.rejected_commands, 0);
}
