use broker_proto::LogRecord;
use broker_storage::{
    storage_telemetry_snapshot, FsyncMode, PartitionLog, PartitionLogConfig, WAL_FORMAT_V2,
};
use std::time::Duration;
use tempfile::tempdir;
use uuid::Uuid;

fn record(sequence: u64) -> LogRecord {
    LogRecord {
        id: Uuid::from_u128(u128::from(sequence) + 1),
        tenant_id: "telemetry-test".into(),
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
fn wal_epochs_and_fsync_outcomes_update_native_snapshot() {
    const SHARD: u32 = 3_901;
    let directory = tempdir().unwrap();
    let config = PartitionLogConfig {
        fsync: FsyncMode::Group,
        group_interval: Duration::from_secs(60),
        ..PartitionLogConfig::default()
    };
    let mut log =
        PartitionLog::open_for_shard(directory.path(), config, SHARD, WAL_FORMAT_V2).unwrap();
    log.append_batch(
        SHARD,
        vec![(record(0), vec![b'a'; 128]), (record(1), vec![b'b'; 128])],
        Some(17),
    )
    .unwrap();
    log.sync().unwrap();
    log.append_batch(SHARD, vec![(record(2), vec![b'c'; 64])], Some(17))
        .unwrap();
    log.inject_fsync_failure();
    assert!(log.sync().is_err());

    let snapshot = storage_telemetry_snapshot();
    let shard = snapshot
        .shards
        .iter()
        .find(|item| item.shard == SHARD)
        .unwrap();
    assert_eq!(shard.leader_epoch, 17);
    assert_eq!(shard.epoch_count, 2);
    assert_eq!(shard.epoch_records_total, 3);
    assert!(shard.epoch_bytes_total > 320);
    assert_eq!(shard.fsync_count, 1);
    assert_eq!(shard.fsync_records_total, 2);
    assert_eq!(shard.fsync_failures, 1);
    assert_eq!(shard.fsync_latency_count, 2);
    assert_eq!(shard.fsync_buckets.iter().sum::<u64>(), 2);
}
