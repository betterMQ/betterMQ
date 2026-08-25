use broker_proto::LogRecord;
use broker_storage::{FsyncMode, PartitionLog, PartitionLogConfig};
use std::io::Write;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use uuid::Uuid;

fn record(sequence: u64) -> LogRecord {
    LogRecord {
        id: Uuid::from_u128(u128::from(sequence) + 1),
        tenant_id: "truth-gate".into(),
        topic: "durable".into(),
        routing_key: format!("lane-{}", sequence % 4),
        idempotency_key: Some(format!("truth-gate:{sequence}")),
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

fn config(fsync: FsyncMode) -> PartitionLogConfig {
    PartitionLogConfig {
        segment_max_bytes: 64 * 1024 * 1024,
        fsync,
        group_interval: Duration::from_secs(60),
    }
}

#[test]
fn durable_batch_sync_advances_hwm_and_survives_reopen() {
    const RECORDS: u64 = 64;
    let directory = tempdir().unwrap();
    let mut log = PartitionLog::open(directory.path(), config(FsyncMode::Group)).unwrap();
    let items = (0..RECORDS)
        .map(|sequence| (record(sequence), vec![b'x'; 1024]))
        .collect();

    log.append_batch(0, items, None).unwrap();
    assert_eq!(log.high_watermark(), RECORDS);
    assert_eq!(log.committed_hwm(), 0);
    let sync_started = Instant::now();
    log.sync().unwrap();
    let sync_elapsed = sync_started.elapsed();
    assert_eq!(log.committed_hwm(), RECORDS);
    drop(log);

    let reopened = PartitionLog::open(directory.path(), config(FsyncMode::Group)).unwrap();
    assert_eq!(reopened.committed_hwm(), RECORDS);
    let recovered = reopened.read_range(0, 0, RECORDS as usize + 1).unwrap();
    assert_eq!(recovered.len(), RECORDS as usize);
    assert_eq!(recovered.first().unwrap().payload.len(), 1024);
    eprintln!(
        "durable_wal_truth_gate records={RECORDS} bytes={} sync_us={}",
        RECORDS * 1024,
        sync_elapsed.as_micros()
    );
}

#[test]
fn always_mode_fsyncs_each_acknowledgeable_append() {
    let directory = tempdir().unwrap();
    let mut log = PartitionLog::open(directory.path(), config(FsyncMode::Always)).unwrap();
    for sequence in 0..4 {
        log.append(0, record(sequence), vec![b'x'; 256]).unwrap();
        assert_eq!(log.committed_hwm(), sequence + 1);
    }
}

#[test]
fn injected_fsync_failure_blocks_commit_hwm_until_retry_succeeds() {
    let directory = tempdir().unwrap();
    let mut log = PartitionLog::open(directory.path(), config(FsyncMode::Group)).unwrap();
    log.append(0, record(0), vec![b'x'; 256]).unwrap();
    assert_eq!(log.high_watermark(), 1);
    assert_eq!(log.committed_hwm(), 0);

    log.inject_fsync_failure();
    let error = log.sync().unwrap_err();
    assert!(error.to_string().contains("injected fsync failure"));
    assert_eq!(log.committed_hwm(), 0);
    assert!(log.is_dirty());

    log.sync().unwrap();
    assert_eq!(log.committed_hwm(), 1);
    assert!(!log.is_dirty());
}

#[test]
fn incomplete_power_loss_tail_is_not_recovered_as_acknowledged() {
    const COMMITTED: u64 = 4;
    let directory = tempdir().unwrap();
    {
        let mut log = PartitionLog::open(directory.path(), config(FsyncMode::Group)).unwrap();
        for sequence in 0..COMMITTED {
            log.append(0, record(sequence), vec![b'x'; 128]).unwrap();
        }
        log.sync().unwrap();
    }

    let mut wal = std::fs::OpenOptions::new()
        .append(true)
        .open(directory.path().join("active.wal"))
        .unwrap();
    wal.write_all(b"incomplete-frame").unwrap();
    wal.sync_data().unwrap();
    drop(wal);

    let reopened = PartitionLog::open(directory.path(), config(FsyncMode::Group)).unwrap();
    assert_eq!(reopened.high_watermark(), COMMITTED);
    assert_eq!(reopened.committed_hwm(), COMMITTED);
    assert_eq!(
        reopened.read_range(0, 0, 100).unwrap().len(),
        COMMITTED as usize
    );
}
