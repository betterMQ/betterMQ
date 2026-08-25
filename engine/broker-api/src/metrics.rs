//! Process-wide ingest metrics (Prometheus text + in-process histograms).

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static ACCEPTED: AtomicU64 = AtomicU64::new(0);
static DUPLICATE: AtomicU64 = AtomicU64::new(0);
static REJECTED: AtomicU64 = AtomicU64::new(0);
static ACK_COUNT: AtomicU64 = AtomicU64::new(0);
static ACK_SUM_US: AtomicU64 = AtomicU64::new(0);
/// Non-cumulative bucket storage for 1, 2, 5, 10, 20, 50, 100, 250, 500, 1000 ms.
///
/// Values above the last finite boundary are represented only by the
/// Prometheus `+Inf` bucket (`ACK_COUNT`). They must not be folded into the
/// 1000ms bucket.
static ACK_BUCKETS: [AtomicU64; 10] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];
const BUCKET_MS: [u64; 10] = [1, 2, 5, 10, 20, 50, 100, 250, 500, 1000];

pub fn record_accepted() {
    ACCEPTED.fetch_add(1, Ordering::Relaxed);
}

pub fn record_duplicate() {
    DUPLICATE.fetch_add(1, Ordering::Relaxed);
}

pub fn record_rejected() {
    REJECTED.fetch_add(1, Ordering::Relaxed);
}

pub fn record_ack_latency(d: Duration) {
    let us = d.as_micros().min(u128::from(u64::MAX)) as u64;
    ACK_COUNT.fetch_add(1, Ordering::Relaxed);
    ACK_SUM_US.fetch_add(us, Ordering::Relaxed);
    if let Some(index) = bucket_index(d) {
        ACK_BUCKETS[index].fetch_add(1, Ordering::Relaxed);
    }
}

fn bucket_index(duration: Duration) -> Option<usize> {
    let micros = duration.as_micros();
    BUCKET_MS
        .iter()
        .position(|edge_ms| micros <= u128::from(*edge_ms) * 1000)
}

#[derive(Debug, Clone, Copy)]
pub struct IngestSnapshot {
    pub accepted: u64,
    pub duplicate: u64,
    pub rejected: u64,
    pub durable_commits: u64,
    ack_observations: u64,
}

pub fn snapshot() -> IngestSnapshot {
    let accepted = ACCEPTED.load(Ordering::Relaxed);
    IngestSnapshot {
        accepted,
        duplicate: DUPLICATE.load(Ordering::Relaxed),
        rejected: REJECTED.load(Ordering::Relaxed),
        // `record_accepted` is emitted only after a non-duplicate, non-scheduled
        // publish has returned from its durable commit wait.
        durable_commits: accepted,
        ack_observations: ACK_COUNT.load(Ordering::Relaxed),
    }
}

pub fn prometheus_text() -> String {
    let snapshot = snapshot();
    let mut out = String::new();
    out.push_str("# HELP bettermq_ingest_accepted_total Accepted ingest records\n");
    out.push_str("# TYPE bettermq_ingest_accepted_total counter\n");
    out.push_str(&format!(
        "bettermq_ingest_accepted_total {}\n",
        snapshot.accepted
    ));
    out.push_str("# HELP bettermq_ingest_duplicate_total Duplicate ingest records\n");
    out.push_str("# TYPE bettermq_ingest_duplicate_total counter\n");
    out.push_str(&format!(
        "bettermq_ingest_duplicate_total {}\n",
        snapshot.duplicate
    ));
    out.push_str("# HELP bettermq_ingest_rejected_total Rejected ingest records\n");
    out.push_str("# TYPE bettermq_ingest_rejected_total counter\n");
    out.push_str(&format!(
        "bettermq_ingest_rejected_total {}\n",
        snapshot.rejected
    ));
    out.push_str("# HELP bettermq_ingest_stage_records_total Records observed at ingest stages\n");
    out.push_str("# TYPE bettermq_ingest_stage_records_total counter\n");
    out.push_str(&format!(
        "bettermq_ingest_stage_records_total{{stage=\"accepted\"}} {}\n",
        snapshot.accepted
    ));
    out.push_str(&format!(
        "bettermq_ingest_stage_records_total{{stage=\"duplicate\"}} {}\n",
        snapshot.duplicate
    ));
    out.push_str(&format!(
        "bettermq_ingest_stage_records_total{{stage=\"rejected\"}} {}\n",
        snapshot.rejected
    ));
    out.push_str("# HELP bettermq_ingest_durable_commits_total Successful durable commit waits\n");
    out.push_str("# TYPE bettermq_ingest_durable_commits_total counter\n");
    out.push_str(&format!(
        "bettermq_ingest_durable_commits_total {}\n",
        snapshot.durable_commits
    ));
    out.push_str("# HELP bettermq_ingest_ack_latency_ms Durable-accept latency\n");
    out.push_str("# TYPE bettermq_ingest_ack_latency_ms histogram\n");
    let mut cum = 0u64;
    for (i, edge) in BUCKET_MS.iter().enumerate() {
        cum += ACK_BUCKETS[i].load(Ordering::Relaxed);
        out.push_str(&format!(
            "bettermq_ingest_ack_latency_ms_bucket{{le=\"{edge}\"}} {cum}\n"
        ));
    }
    out.push_str(&format!(
        "bettermq_ingest_ack_latency_ms_bucket{{le=\"+Inf\"}} {}\n",
        snapshot.ack_observations
    ));
    out.push_str(&format!(
        "bettermq_ingest_ack_latency_ms_sum {}\n",
        ACK_SUM_US.load(Ordering::Relaxed) as f64 / 1000.0
    ));
    out.push_str(&format!(
        "bettermq_ingest_ack_latency_ms_count {}\n",
        snapshot.ack_observations
    ));
    out
}

pub struct NativeTelemetrySnapshot {
    pub storage: broker_storage::StorageTelemetrySnapshot,
    pub partition: broker_partition::PartitionTelemetrySnapshot,
    pub replication: Option<broker_replication::ReplicationTelemetrySnapshot>,
    pub controller: Option<broker_raft_meta::ControllerTelemetrySnapshot>,
    pub archive: broker_storage::ArchiveLagStatus,
    pub dispatch: broker_dispatch::DispatchTelemetrySnapshot,
    pub fairness: broker_dispatch::TenantFairnessSnapshot,
}

pub fn native_snapshot(state: &crate::AppState) -> NativeTelemetrySnapshot {
    NativeTelemetrySnapshot {
        storage: broker_storage::storage_telemetry_snapshot(),
        partition: broker_partition::partition_telemetry_snapshot(),
        replication: state
            .cluster
            .as_ref()
            .map(|cluster| cluster.replication.telemetry_snapshot()),
        controller: state
            .cluster
            .as_ref()
            .map(|cluster| broker_raft_meta::controller_telemetry_snapshot(&cluster.runtime)),
        archive: broker_storage::archive_lag_status(),
        dispatch: state.dispatch.telemetry_snapshot(),
        fairness: state.fair_queue.telemetry_snapshot(),
    }
}

pub fn prometheus_text_with_native(native: &NativeTelemetrySnapshot) -> String {
    let mut out = prometheus_text();
    append_storage_metrics(&mut out, &native.storage);
    append_partition_metrics(&mut out, &native.partition);
    if let Some(replication) = &native.replication {
        append_replication_metrics(&mut out, replication);
    }
    if let Some(controller) = native.controller {
        append_controller_metrics(&mut out, controller);
    }
    append_archive_metrics(&mut out, &native.archive);
    append_dispatch_metrics(&mut out, &native.dispatch, &native.fairness);
    out
}

fn append_storage_metrics(out: &mut String, snapshot: &broker_storage::StorageTelemetrySnapshot) {
    out.push_str("# HELP bettermq_wal_epoch_total WAL epochs appended\n");
    out.push_str("# TYPE bettermq_wal_epoch_total counter\n");
    out.push_str("# HELP bettermq_wal_epoch_records_total Records appended in WAL epochs\n");
    out.push_str("# TYPE bettermq_wal_epoch_records_total counter\n");
    out.push_str("# HELP bettermq_wal_epoch_bytes_total Encoded bytes appended in WAL epochs\n");
    out.push_str("# TYPE bettermq_wal_epoch_bytes_total counter\n");
    out.push_str("# HELP bettermq_wal_last_epoch_records Records in the latest WAL epoch\n");
    out.push_str("# TYPE bettermq_wal_last_epoch_records gauge\n");
    out.push_str("# HELP bettermq_wal_last_epoch_bytes Encoded bytes in the latest WAL epoch\n");
    out.push_str("# TYPE bettermq_wal_last_epoch_bytes gauge\n");
    out.push_str("# HELP bettermq_wal_leader_epoch Current observed WAL leader epoch\n");
    out.push_str("# TYPE bettermq_wal_leader_epoch gauge\n");
    out.push_str("# HELP bettermq_wal_fsync_total Successful WAL fsync operations\n");
    out.push_str("# TYPE bettermq_wal_fsync_total counter\n");
    out.push_str("# HELP bettermq_wal_fsync_failures_total Failed WAL fsync operations\n");
    out.push_str("# TYPE bettermq_wal_fsync_failures_total counter\n");
    out.push_str("# HELP bettermq_wal_fsync_records_total Records covered by successful fsyncs\n");
    out.push_str("# TYPE bettermq_wal_fsync_records_total counter\n");
    out.push_str("# HELP bettermq_wal_records_per_fsync Mean records per successful fsync\n");
    out.push_str("# TYPE bettermq_wal_records_per_fsync gauge\n");
    out.push_str("# HELP bettermq_wal_fsync_duration_seconds WAL fsync latency\n");
    out.push_str("# TYPE bettermq_wal_fsync_duration_seconds histogram\n");
    for shard in &snapshot.shards {
        let label = format!("shard=\"{}\"", shard.shard);
        out.push_str(&format!(
            "bettermq_wal_epoch_total{{{label}}} {}\n",
            shard.epoch_count
        ));
        out.push_str(&format!(
            "bettermq_wal_epoch_records_total{{{label}}} {}\n",
            shard.epoch_records_total
        ));
        out.push_str(&format!(
            "bettermq_wal_epoch_bytes_total{{{label}}} {}\n",
            shard.epoch_bytes_total
        ));
        out.push_str(&format!(
            "bettermq_wal_last_epoch_records{{{label}}} {}\n",
            shard.last_epoch_records
        ));
        out.push_str(&format!(
            "bettermq_wal_last_epoch_bytes{{{label}}} {}\n",
            shard.last_epoch_bytes
        ));
        out.push_str(&format!(
            "bettermq_wal_leader_epoch{{{label}}} {}\n",
            shard.leader_epoch
        ));
        out.push_str(&format!(
            "bettermq_wal_fsync_total{{{label}}} {}\n",
            shard.fsync_count
        ));
        out.push_str(&format!(
            "bettermq_wal_fsync_failures_total{{{label}}} {}\n",
            shard.fsync_failures
        ));
        out.push_str(&format!(
            "bettermq_wal_fsync_records_total{{{label}}} {}\n",
            shard.fsync_records_total
        ));
        let records_per_fsync = if shard.fsync_count == 0 {
            0.0
        } else {
            shard.fsync_records_total as f64 / shard.fsync_count as f64
        };
        out.push_str(&format!(
            "bettermq_wal_records_per_fsync{{{label}}} {records_per_fsync}\n"
        ));
        append_histogram(
            out,
            "bettermq_wal_fsync_duration_seconds",
            &label,
            &broker_storage::FSYNC_BUCKET_US.map(|edge| edge as f64 / 1_000_000.0),
            &shard.fsync_buckets,
            shard.fsync_latency_count,
            shard.fsync_latency_sum_us as f64 / 1_000_000.0,
        );
    }
    out.push_str(&format!(
        "bettermq_wal_telemetry_dropped_shards_total {}\n",
        snapshot.dropped_shards
    ));
}

fn append_partition_metrics(
    out: &mut String,
    snapshot: &broker_partition::PartitionTelemetrySnapshot,
) {
    out.push_str("# HELP bettermq_shard_queue_records Records queued for shard I/O\n");
    out.push_str("# TYPE bettermq_shard_queue_records gauge\n");
    out.push_str("# HELP bettermq_shard_queue_bytes Payload/frame bytes queued for shard I/O\n");
    out.push_str("# TYPE bettermq_shard_queue_bytes gauge\n");
    out.push_str("# HELP bettermq_shard_queue_max_records Maximum observed queued records\n");
    out.push_str("# TYPE bettermq_shard_queue_max_records gauge\n");
    out.push_str(
        "# HELP bettermq_shard_queue_max_bytes Maximum observed queued payload/frame bytes\n",
    );
    out.push_str("# TYPE bettermq_shard_queue_max_bytes gauge\n");
    out.push_str(
        "# HELP bettermq_shard_queue_rejected_total Commands rejected by a full/closed queue\n",
    );
    out.push_str("# TYPE bettermq_shard_queue_rejected_total counter\n");
    for shard in &snapshot.shards {
        let label = format!("shard=\"{}\"", shard.shard);
        out.push_str(&format!(
            "bettermq_shard_queue_records{{{label}}} {}\n",
            shard.queued_records
        ));
        out.push_str(&format!(
            "bettermq_shard_queue_bytes{{{label}}} {}\n",
            shard.queued_bytes
        ));
        out.push_str(&format!(
            "bettermq_shard_queue_max_records{{{label}}} {}\n",
            shard.max_queued_records
        ));
        out.push_str(&format!(
            "bettermq_shard_queue_max_bytes{{{label}}} {}\n",
            shard.max_queued_bytes
        ));
        out.push_str(&format!(
            "bettermq_shard_queue_rejected_total{{{label}}} {}\n",
            shard.rejected_commands
        ));
    }
    out.push_str(&format!(
        "bettermq_shard_queue_dropped_shard_events_total {}\n",
        snapshot.dropped_shard_events
    ));
}

fn append_replication_metrics(
    out: &mut String,
    snapshot: &broker_replication::ReplicationTelemetrySnapshot,
) {
    out.push_str("# HELP bettermq_replication_quorum_total Replication quorum attempts\n");
    out.push_str("# TYPE bettermq_replication_quorum_total counter\n");
    out.push_str("# HELP bettermq_replication_quorum_failures_total Failed quorum attempts\n");
    out.push_str("# TYPE bettermq_replication_quorum_failures_total counter\n");
    out.push_str("# HELP bettermq_replication_follower_durable_acks_total Durable follower ACKs\n");
    out.push_str("# TYPE bettermq_replication_follower_durable_acks_total counter\n");
    out.push_str("# HELP bettermq_replication_isr_members In-sync replicas including the leader\n");
    out.push_str("# TYPE bettermq_replication_isr_members gauge\n");
    out.push_str("# HELP bettermq_replication_max_lag_records Maximum follower lag\n");
    out.push_str("# TYPE bettermq_replication_max_lag_records gauge\n");
    out.push_str("# HELP bettermq_replication_quorum_duration_seconds Durable quorum latency\n");
    out.push_str("# TYPE bettermq_replication_quorum_duration_seconds histogram\n");
    for shard in &snapshot.shards {
        let label = format!("shard=\"{}\"", shard.shard);
        out.push_str(&format!(
            "bettermq_replication_quorum_total{{{label}}} {}\n",
            shard.quorum_requests
        ));
        out.push_str(&format!(
            "bettermq_replication_quorum_failures_total{{{label}}} {}\n",
            shard.quorum_failures
        ));
        out.push_str(&format!(
            "bettermq_replication_follower_durable_acks_total{{{label}}} {}\n",
            shard.follower_durable_acks_total
        ));
        out.push_str(&format!(
            "bettermq_replication_last_durable_acks{{{label}}} {}\n",
            shard.last_durable_acks
        ));
        out.push_str(&format!(
            "bettermq_replication_required_quorum{{{label}}} {}\n",
            shard.last_required_quorum
        ));
        out.push_str(&format!(
            "bettermq_replication_isr_members{{{label}}} {}\n",
            shard.isr_members
        ));
        out.push_str(&format!(
            "bettermq_replication_max_lag_records{{{label}}} {}\n",
            shard.max_replica_lag
        ));
        append_histogram(
            out,
            "bettermq_replication_quorum_duration_seconds",
            &label,
            &broker_replication::QUORUM_BUCKET_MS.map(|edge| edge as f64 / 1_000.0),
            &shard.quorum_buckets,
            shard.quorum_latency_count,
            shard.quorum_latency_sum_us as f64 / 1_000_000.0,
        );
    }
    out.push_str(&format!(
        "bettermq_replication_telemetry_dropped_shard_events_total {}\n",
        snapshot.dropped_shard_events
    ));
}

fn append_controller_metrics(
    out: &mut String,
    snapshot: broker_raft_meta::ControllerTelemetrySnapshot,
) {
    out.push_str("# HELP bettermq_controller_term Current durable controller term\n");
    out.push_str("# TYPE bettermq_controller_term gauge\n");
    out.push_str(&format!("bettermq_controller_term {}\n", snapshot.term));
    out.push_str("# HELP bettermq_controller_quorum_ready Controller has a valid quorum lease\n");
    out.push_str("# TYPE bettermq_controller_quorum_ready gauge\n");
    out.push_str(&format!(
        "bettermq_controller_quorum_ready {}\n",
        u8::from(snapshot.quorum_ready)
    ));
    out.push_str(&format!(
        "bettermq_controller_configured_nodes {}\n",
        snapshot.configured_nodes
    ));
    out.push_str(&format!(
        "bettermq_controller_required_quorum {}\n",
        snapshot.required_quorum
    ));
}

fn append_archive_metrics(out: &mut String, snapshot: &broker_storage::ArchiveLagStatus) {
    out.push_str("# HELP bettermq_archive_enabled Whether asynchronous archive is configured\n");
    out.push_str("# TYPE bettermq_archive_enabled gauge\n");
    out.push_str(&format!(
        "bettermq_archive_enabled {}\n",
        u8::from(snapshot.enabled)
    ));
    out.push_str("# HELP bettermq_archive_backlog_segments Sealed segments awaiting archive\n");
    out.push_str("# TYPE bettermq_archive_backlog_segments gauge\n");
    out.push_str(&format!(
        "bettermq_archive_backlog_segments {}\n",
        snapshot.queued_records
    ));
    out.push_str("# HELP bettermq_archive_backlog_bytes Bytes awaiting archive\n");
    out.push_str("# TYPE bettermq_archive_backlog_bytes gauge\n");
    out.push_str(&format!(
        "bettermq_archive_backlog_bytes {}\n",
        snapshot.queued_bytes
    ));
    out.push_str("# HELP bettermq_archive_oldest_pending_age_seconds Oldest pending segment age\n");
    out.push_str("# TYPE bettermq_archive_oldest_pending_age_seconds gauge\n");
    out.push_str(&format!(
        "bettermq_archive_oldest_pending_age_seconds {}\n",
        snapshot.oldest_age_ms as f64 / 1_000.0
    ));
    out.push_str("# HELP bettermq_archive_failures_total Failed segment archive attempts\n");
    out.push_str("# TYPE bettermq_archive_failures_total counter\n");
    out.push_str(&format!(
        "bettermq_archive_failures_total {}\n",
        snapshot.failed_records
    ));
    out.push_str(&format!(
        "bettermq_archive_failed_bytes_total {}\n",
        snapshot.failed_bytes
    ));
    out.push_str(&format!(
        "bettermq_archive_queue_capacity_segments {}\n",
        snapshot.capacity
    ));
    out.push_str(&format!(
        "bettermq_archive_queue_headroom_segments {}\n",
        snapshot
            .capacity
            .saturating_sub(snapshot.queued_records as usize)
    ));
    out.push_str(&format!(
        "bettermq_archive_admission_blocked {}\n",
        u8::from(snapshot.admission_blocked)
    ));
}

fn append_dispatch_metrics(
    out: &mut String,
    snapshot: &broker_dispatch::DispatchTelemetrySnapshot,
    fairness: &broker_dispatch::TenantFairnessSnapshot,
) {
    macro_rules! metric {
        ($help:literal, $kind:literal, $name:literal, $value:expr) => {
            out.push_str(concat!("# HELP ", $name, " ", $help, "\n"));
            out.push_str(concat!("# TYPE ", $name, " ", $kind, "\n"));
            out.push_str(&format!(concat!($name, " {}\n"), $value));
        };
    }
    metric!(
        "Committed ranges announced to dispatch",
        "counter",
        "bettermq_dispatch_committed_ready_ranges_total",
        snapshot.committed_ready_ranges
    );
    metric!(
        "Committed records announced to dispatch",
        "counter",
        "bettermq_dispatch_committed_ready_records_total",
        snapshot.committed_ready_records
    );
    metric!(
        "Committed ready delivery jobs currently queued",
        "gauge",
        "bettermq_dispatch_committed_ready_depth",
        snapshot.queue_depth
    );
    metric!(
        "Delivery jobs rejected by the bounded ready queue",
        "counter",
        "bettermq_dispatch_queue_rejected_total",
        snapshot.queue_rejected
    );
    metric!(
        "Queued or executing dispatch jobs",
        "gauge",
        "bettermq_dispatch_in_flight_deliveries",
        snapshot.in_flight_deliveries
    );
    metric!(
        "Dispatch HTTP requests currently active",
        "gauge",
        "bettermq_dispatch_active_http_deliveries",
        snapshot.active_http_deliveries
    );
    metric!(
        "Durable retries currently pending",
        "gauge",
        "bettermq_dispatch_retries_pending",
        snapshot.retries_pending
    );
    metric!(
        "Durable retries whose deadlines have passed",
        "gauge",
        "bettermq_dispatch_retries_pending_due",
        snapshot.retries_pending_due
    );
    metric!(
        "Durable retries scheduled",
        "counter",
        "bettermq_dispatch_retries_scheduled_total",
        snapshot.retries_scheduled
    );
    metric!(
        "Due retries started",
        "counter",
        "bettermq_dispatch_retries_due_total",
        snapshot.retries_due
    );
    metric!(
        "Deliveries that exhausted retry policy",
        "counter",
        "bettermq_dispatch_retries_exhausted_total",
        snapshot.retries_exhausted
    );
    metric!(
        "Prepared DLQ transitions pending durable DLQ commit",
        "gauge",
        "bettermq_dispatch_dlq_pending_prepared",
        snapshot.dlq_pending_prepared
    );
    metric!(
        "DLQ-committed transitions pending cursor repair",
        "gauge",
        "bettermq_dispatch_dlq_pending_committed",
        snapshot.dlq_pending_committed
    );
    metric!(
        "DLQ transitions durably prepared",
        "counter",
        "bettermq_dispatch_dlq_prepared_total",
        snapshot.dlq_prepared
    );
    metric!(
        "DLQ transitions durably committed",
        "counter",
        "bettermq_dispatch_dlq_committed_total",
        snapshot.dlq_committed
    );
    metric!(
        "DLQ publish or durable transition failures",
        "counter",
        "bettermq_dispatch_dlq_failures_total",
        snapshot.dlq_failures
    );
    metric!(
        "Whether dispatch is draining",
        "gauge",
        "bettermq_dispatch_draining",
        u8::from(snapshot.draining)
    );
    metric!(
        "Dispatch drains started",
        "counter",
        "bettermq_dispatch_drain_started_total",
        snapshot.drain_started
    );
    metric!(
        "Dispatch drains completed",
        "counter",
        "bettermq_dispatch_drain_completed_total",
        snapshot.drain_completed
    );
    metric!(
        "Dispatch drains that timed out",
        "counter",
        "bettermq_dispatch_drain_timeouts_total",
        snapshot.drain_timeouts
    );
    metric!(
        "Duration of the latest drain",
        "gauge",
        "bettermq_dispatch_last_drain_duration_seconds",
        snapshot.last_drain_duration_ms as f64 / 1000.0
    );
    metric!(
        "Destination hosts tracked by the circuit breaker",
        "gauge",
        "bettermq_dispatch_destination_hosts_tracked",
        snapshot.host_pressure.tracked_hosts
    );
    metric!(
        "Destination hosts currently blocked",
        "gauge",
        "bettermq_dispatch_destination_hosts_blocked",
        snapshot.host_pressure.blocked_hosts
    );
    metric!(
        "Destination hosts with recorded pressure",
        "gauge",
        "bettermq_dispatch_destination_hosts_with_failures",
        snapshot.host_pressure.hosts_with_failures
    );
    metric!(
        "Current aggregate destination transport failures",
        "gauge",
        "bettermq_dispatch_destination_current_failures",
        snapshot.host_pressure.current_failures
    );
    metric!(
        "Maximum current failures for one destination",
        "gauge",
        "bettermq_dispatch_destination_max_failures",
        snapshot.host_pressure.max_host_failures
    );
    metric!(
        "Tenants tracked by weighted fairness",
        "gauge",
        "bettermq_dispatch_fairness_tracked_tenants",
        fairness.tracked_tenants
    );
    metric!(
        "Configured tenant weights",
        "gauge",
        "bettermq_dispatch_fairness_configured_weights",
        fairness.configured_weights
    );
    metric!(
        "Fairness tenant soft limit",
        "gauge",
        "bettermq_dispatch_fairness_soft_limit",
        fairness.soft_limit
    );
    metric!(
        "Ratio of tracked tenants to fairness soft limit",
        "gauge",
        "bettermq_dispatch_fairness_saturation_ratio",
        fairness.saturation_ratio
    );
    metric!(
        "Whether fairness tracking reached its soft limit",
        "gauge",
        "bettermq_dispatch_fairness_saturated",
        u8::from(fairness.saturated)
    );
    out.push_str("# HELP bettermq_dispatch_completions_total Durable dispatch completions\n");
    out.push_str("# TYPE bettermq_dispatch_completions_total counter\n");
    out.push_str("# HELP bettermq_dispatch_cursor_advanced_records_total Records advanced by durable cursors\n");
    out.push_str("# TYPE bettermq_dispatch_cursor_advanced_records_total counter\n");
    out.push_str("# HELP bettermq_dispatch_cursor_min_offset Minimum tracked durable cursor\n");
    out.push_str("# TYPE bettermq_dispatch_cursor_min_offset gauge\n");
    out.push_str("# HELP bettermq_dispatch_cursor_max_offset Maximum tracked durable cursor\n");
    out.push_str("# TYPE bettermq_dispatch_cursor_max_offset gauge\n");
    out.push_str("# HELP bettermq_dispatch_completion_gap_records Committed records above tracked durable cursors\n");
    out.push_str("# TYPE bettermq_dispatch_completion_gap_records gauge\n");
    out.push_str("# HELP bettermq_dispatch_tracked_cursor_lanes Internally tracked cursor lanes\n");
    out.push_str("# TYPE bettermq_dispatch_tracked_cursor_lanes gauge\n");
    for shard in &snapshot.shards {
        let label = format!("shard=\"{}\"", shard.shard);
        out.push_str(&format!(
            "bettermq_dispatch_completions_total{{{label}}} {}\n",
            shard.completions
        ));
        out.push_str(&format!(
            "bettermq_dispatch_cursor_advanced_records_total{{{label}}} {}\n",
            shard.cursor_advanced_records
        ));
        out.push_str(&format!(
            "bettermq_dispatch_cursor_min_offset{{{label}}} {}\n",
            shard.cursor_min_offset
        ));
        out.push_str(&format!(
            "bettermq_dispatch_cursor_max_offset{{{label}}} {}\n",
            shard.cursor_max_offset
        ));
        out.push_str(&format!(
            "bettermq_dispatch_completion_gap_records{{{label}}} {}\n",
            shard.completion_gap_records
        ));
        out.push_str(&format!(
            "bettermq_dispatch_tracked_cursor_lanes{{{label}}} {}\n",
            shard.tracked_cursor_lanes
        ));
    }
    metric!(
        "Cursor lanes omitted after the bounded telemetry cap",
        "counter",
        "bettermq_dispatch_dropped_cursor_lanes_total",
        snapshot.dropped_cursor_lanes
    );
}

fn append_histogram<const N: usize>(
    out: &mut String,
    name: &str,
    labels: &str,
    boundaries: &[f64; N],
    buckets: &[u64; N],
    count: u64,
    sum: f64,
) {
    let mut cumulative = 0u64;
    for (boundary, bucket) in boundaries.iter().zip(buckets) {
        cumulative = cumulative.saturating_add(*bucket);
        out.push_str(&format!(
            "{name}_bucket{{{labels},le=\"{boundary}\"}} {cumulative}\n"
        ));
    }
    out.push_str(&format!("{name}_bucket{{{labels},le=\"+Inf\"}} {count}\n"));
    out.push_str(&format!("{name}_sum{{{labels}}} {sum}\n"));
    out.push_str(&format!("{name}_count{{{labels}}} {count}\n"));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finite_histogram_boundaries_use_microsecond_precision() {
        assert_eq!(bucket_index(Duration::from_micros(999)), Some(0));
        assert_eq!(bucket_index(Duration::from_micros(1_000)), Some(0));
        assert_eq!(bucket_index(Duration::from_micros(1_001)), Some(1));
    }

    #[test]
    fn histogram_overflow_is_not_added_to_last_finite_bucket() {
        assert_eq!(bucket_index(Duration::from_millis(1_000)), Some(9));
        assert_eq!(bucket_index(Duration::from_millis(1_001)), None);
        assert_eq!(bucket_index(Duration::from_secs(60)), None);
    }

    #[test]
    fn prometheus_renders_real_native_snapshots_with_bounded_labels() {
        let native = NativeTelemetrySnapshot {
            storage: broker_storage::StorageTelemetrySnapshot {
                shards: vec![broker_storage::StorageShardTelemetrySnapshot {
                    shard: 7,
                    leader_epoch: 11,
                    epoch_count: 3,
                    epoch_records_total: 12,
                    epoch_bytes_total: 4096,
                    last_epoch_records: 4,
                    last_epoch_bytes: 1024,
                    fsync_count: 2,
                    fsync_failures: 1,
                    fsync_records_total: 12,
                    fsync_latency_count: 3,
                    fsync_latency_sum_us: 21_000,
                    fsync_buckets: [1, 0, 1, 0, 0, 0, 0, 0, 0, 0],
                }],
                dropped_shards: 0,
            },
            partition: broker_partition::PartitionTelemetrySnapshot {
                shards: vec![broker_partition::ShardQueueTelemetrySnapshot {
                    shard: 7,
                    queued_records: 2,
                    queued_bytes: 512,
                    max_queued_records: 9,
                    max_queued_bytes: 2048,
                    rejected_commands: 1,
                }],
                dropped_shard_events: 0,
            },
            replication: Some(broker_replication::ReplicationTelemetrySnapshot {
                shards: vec![broker_replication::ReplicationShardTelemetrySnapshot {
                    shard: 7,
                    quorum_requests: 5,
                    quorum_failures: 1,
                    follower_durable_acks_total: 8,
                    last_durable_acks: 2,
                    last_required_quorum: 2,
                    quorum_latency_count: 5,
                    quorum_latency_sum_us: 40_000,
                    quorum_buckets: [1, 1, 1, 1, 0, 0, 0, 0, 0, 0],
                    isr_members: 2,
                    max_replica_lag: 17,
                }],
                dropped_shard_events: 0,
            }),
            controller: Some(broker_raft_meta::ControllerTelemetrySnapshot {
                term: 13,
                quorum_ready: true,
                configured_nodes: 3,
                required_quorum: 2,
            }),
            archive: broker_storage::ArchiveLagStatus {
                enabled: true,
                queued_records: 4,
                queued_bytes: 8192,
                failed_records: 1,
                failed_bytes: 1024,
                oldest_age_ms: 2500,
                capacity: 128,
                admission_blocked: false,
            },
            dispatch: broker_dispatch::DispatchTelemetrySnapshot {
                committed_ready_ranges: 4,
                committed_ready_records: 16,
                queue_depth: 3,
                queue_rejected: 1,
                in_flight_deliveries: 5,
                active_http_deliveries: 2,
                retries_pending: 6,
                retries_pending_due: 2,
                retries_scheduled: 9,
                retries_due: 7,
                retries_exhausted: 1,
                dlq_pending_prepared: 1,
                dlq_pending_committed: 1,
                dlq_prepared: 3,
                dlq_committed: 2,
                dlq_failures: 1,
                drain_started: 2,
                drain_completed: 1,
                drain_timeouts: 1,
                last_drain_duration_ms: 250,
                draining: true,
                shards: vec![broker_dispatch::DispatchShardTelemetrySnapshot {
                    shard: 7,
                    completions: 12,
                    cursor_advanced_records: 10,
                    cursor_min_offset: 40,
                    cursor_max_offset: 50,
                    completion_gap_records: 4,
                    tracked_cursor_lanes: 2,
                }],
                dropped_cursor_lanes: 0,
                host_pressure: broker_dispatch::HostPressureSnapshot {
                    tracked_hosts: 4,
                    blocked_hosts: 1,
                    hosts_with_failures: 2,
                    current_failures: 5,
                    max_host_failures: 3,
                },
            },
            fairness: broker_dispatch::TenantFairnessSnapshot {
                tracked_tenants: 8,
                configured_weights: 3,
                soft_limit: 1024,
                saturation_ratio: 8.0 / 1024.0,
                saturated: false,
            },
        };
        let output = prometheus_text_with_native(&native);
        assert!(output.contains("bettermq_wal_epoch_total{shard=\"7\"} 3"));
        assert!(output.contains("bettermq_wal_records_per_fsync{shard=\"7\"} 6"));
        assert!(output
            .contains("bettermq_wal_fsync_duration_seconds_bucket{shard=\"7\",le=\"+Inf\"} 3"));
        assert!(output.contains("bettermq_shard_queue_bytes{shard=\"7\"} 512"));
        assert!(output.contains("bettermq_replication_isr_members{shard=\"7\"} 2"));
        assert!(output.contains("bettermq_replication_max_lag_records{shard=\"7\"} 17"));
        assert!(output.contains("bettermq_controller_term 13"));
        assert!(output.contains("bettermq_controller_quorum_ready 1"));
        assert!(output.contains("bettermq_archive_queue_headroom_segments 124"));
        assert!(output.contains("bettermq_dispatch_committed_ready_depth 3"));
        assert!(output.contains("bettermq_dispatch_retries_scheduled_total 9"));
        assert!(output.contains("bettermq_dispatch_dlq_failures_total 1"));
        assert!(output.contains("bettermq_dispatch_completion_gap_records{shard=\"7\"} 4"));
        assert!(output.contains("bettermq_dispatch_destination_hosts_blocked 1"));
        assert!(output.contains("bettermq_dispatch_fairness_tracked_tenants 8"));
        assert!(!output.contains("topic="));
        assert!(!output.contains("peer="));
        assert!(!output.contains("node="));
        assert!(!output.contains("tenant="));
        assert!(!output.contains("url="));
        assert!(!output.contains("lane="));
    }
}
