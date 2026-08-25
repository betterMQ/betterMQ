//! Bounded native dispatch telemetry.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

pub const MAX_DISPATCH_TELEMETRY_SHARDS: usize = 4096;
pub const MAX_DISPATCH_CURSOR_LANES: usize = 4096;

#[derive(Default)]
struct ShardCounters {
    completions: AtomicU64,
    cursor_advanced_records: AtomicU64,
}

#[derive(Default)]
pub(crate) struct DispatchTelemetry {
    committed_ready_ranges: AtomicU64,
    committed_ready_records: AtomicU64,
    queue_depth: AtomicU64,
    queue_rejected: AtomicU64,
    retries_scheduled: AtomicU64,
    retries_due: AtomicU64,
    retries_exhausted: AtomicU64,
    dlq_prepared: AtomicU64,
    dlq_committed: AtomicU64,
    dlq_failures: AtomicU64,
    drain_started: AtomicU64,
    drain_completed: AtomicU64,
    drain_timeouts: AtomicU64,
    last_drain_duration_ms: AtomicU64,
    draining: AtomicBool,
    shards: Box<[ShardCounters]>,
    cursor_lanes: Mutex<BTreeMap<(u32, String), (u64, u64)>>,
    dropped_cursor_lanes: AtomicU64,
}

impl DispatchTelemetry {
    pub(crate) fn new() -> Self {
        Self {
            shards: (0..MAX_DISPATCH_TELEMETRY_SHARDS)
                .map(|_| ShardCounters::default())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            ..Self::default()
        }
    }

    pub(crate) fn record_committed_range(&self, records: u64) {
        self.committed_ready_ranges.fetch_add(1, Ordering::Relaxed);
        self.committed_ready_records
            .fetch_add(records, Ordering::Relaxed);
    }

    pub(crate) fn record_queue_enqueue(&self) {
        self.queue_depth.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_queue_dequeue(&self) {
        self.queue_depth
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(1))
            })
            .ok();
    }

    pub(crate) fn record_queue_rejected(&self) {
        self.queue_rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_retry_scheduled(&self) {
        self.retries_scheduled.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_retry_due(&self) {
        self.retries_due.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_retry_exhausted(&self) {
        self.retries_exhausted.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_dlq_prepared(&self) {
        self.dlq_prepared.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_dlq_committed(&self) {
        self.dlq_committed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_dlq_failure(&self) {
        self.dlq_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_drain_started(&self) {
        self.draining.store(true, Ordering::Release);
        self.drain_started.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_drain_finished(&self, elapsed_ms: u64, completed: bool) {
        self.last_drain_duration_ms
            .store(elapsed_ms, Ordering::Relaxed);
        if completed {
            self.drain_completed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.drain_timeouts.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_stopped(&self) {
        self.draining.store(true, Ordering::Release);
    }

    pub(crate) fn record_completion(
        &self,
        shard: u32,
        lane: &str,
        old_cursor: u64,
        new_cursor: u64,
        committed_hwm: u64,
        newly_completed: bool,
    ) {
        let Some(counters) = self.shards.get(shard as usize) else {
            self.dropped_cursor_lanes.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if newly_completed {
            counters.completions.fetch_add(1, Ordering::Relaxed);
        }
        counters
            .cursor_advanced_records
            .fetch_add(new_cursor.saturating_sub(old_cursor), Ordering::Relaxed);
        let mut lanes = self
            .cursor_lanes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let key = (shard, lane.to_string());
        if lanes.contains_key(&key) || lanes.len() < MAX_DISPATCH_CURSOR_LANES {
            lanes.insert(key, (new_cursor, committed_hwm));
        } else {
            self.dropped_cursor_lanes.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn snapshot(&self) -> DispatchCounterSnapshot {
        let lanes = self
            .cursor_lanes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut aggregate: BTreeMap<u32, (u64, u64, u64, u64)> = BTreeMap::new();
        for ((shard, _), (cursor, hwm)) in lanes.iter() {
            let value = aggregate.entry(*shard).or_insert((u64::MAX, 0, 0, 0));
            value.0 = value.0.min(*cursor);
            value.1 = value.1.max(*cursor);
            value.2 = value.2.saturating_add(hwm.saturating_sub(*cursor));
            value.3 = value.3.saturating_add(1);
        }
        for (shard, counters) in self.shards.iter().enumerate() {
            let completions = counters.completions.load(Ordering::Relaxed);
            let advanced = counters.cursor_advanced_records.load(Ordering::Relaxed);
            if completions > 0 || advanced > 0 {
                aggregate.entry(shard as u32).or_insert((0, 0, 0, 0));
            }
        }
        DispatchCounterSnapshot {
            committed_ready_ranges: self.committed_ready_ranges.load(Ordering::Relaxed),
            committed_ready_records: self.committed_ready_records.load(Ordering::Relaxed),
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            queue_rejected: self.queue_rejected.load(Ordering::Relaxed),
            retries_scheduled: self.retries_scheduled.load(Ordering::Relaxed),
            retries_due: self.retries_due.load(Ordering::Relaxed),
            retries_exhausted: self.retries_exhausted.load(Ordering::Relaxed),
            dlq_prepared: self.dlq_prepared.load(Ordering::Relaxed),
            dlq_committed: self.dlq_committed.load(Ordering::Relaxed),
            dlq_failures: self.dlq_failures.load(Ordering::Relaxed),
            drain_started: self.drain_started.load(Ordering::Relaxed),
            drain_completed: self.drain_completed.load(Ordering::Relaxed),
            drain_timeouts: self.drain_timeouts.load(Ordering::Relaxed),
            last_drain_duration_ms: self.last_drain_duration_ms.load(Ordering::Relaxed),
            draining: self.draining.load(Ordering::Acquire),
            shards: aggregate
                .into_iter()
                .map(
                    |(shard, (cursor_min, cursor_max, gap_records, tracked_lanes))| {
                        let counters = &self.shards[shard as usize];
                        DispatchShardTelemetrySnapshot {
                            shard,
                            completions: counters.completions.load(Ordering::Relaxed),
                            cursor_advanced_records: counters
                                .cursor_advanced_records
                                .load(Ordering::Relaxed),
                            cursor_min_offset: cursor_min,
                            cursor_max_offset: cursor_max,
                            completion_gap_records: gap_records,
                            tracked_cursor_lanes: tracked_lanes,
                        }
                    },
                )
                .collect(),
            dropped_cursor_lanes: self.dropped_cursor_lanes.load(Ordering::Relaxed),
        }
    }
}

pub(crate) struct DispatchCounterSnapshot {
    pub committed_ready_ranges: u64,
    pub committed_ready_records: u64,
    pub queue_depth: u64,
    pub queue_rejected: u64,
    pub retries_scheduled: u64,
    pub retries_due: u64,
    pub retries_exhausted: u64,
    pub dlq_prepared: u64,
    pub dlq_committed: u64,
    pub dlq_failures: u64,
    pub drain_started: u64,
    pub drain_completed: u64,
    pub drain_timeouts: u64,
    pub last_drain_duration_ms: u64,
    pub draining: bool,
    pub shards: Vec<DispatchShardTelemetrySnapshot>,
    pub dropped_cursor_lanes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchShardTelemetrySnapshot {
    pub shard: u32,
    pub completions: u64,
    pub cursor_advanced_records: u64,
    pub cursor_min_offset: u64,
    pub cursor_max_offset: u64,
    pub completion_gap_records: u64,
    pub tracked_cursor_lanes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostPressureSnapshot {
    pub tracked_hosts: u64,
    pub blocked_hosts: u64,
    pub hosts_with_failures: u64,
    pub current_failures: u64,
    pub max_host_failures: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TenantFairnessSnapshot {
    pub tracked_tenants: u64,
    pub configured_weights: u64,
    pub soft_limit: u64,
    pub saturation_ratio: f64,
    pub saturated: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DispatchTelemetrySnapshot {
    pub committed_ready_ranges: u64,
    pub committed_ready_records: u64,
    pub queue_depth: u64,
    pub queue_rejected: u64,
    pub in_flight_deliveries: u64,
    pub active_http_deliveries: u64,
    pub retries_pending: u64,
    pub retries_pending_due: u64,
    pub retries_scheduled: u64,
    pub retries_due: u64,
    pub retries_exhausted: u64,
    pub dlq_pending_prepared: u64,
    pub dlq_pending_committed: u64,
    pub dlq_prepared: u64,
    pub dlq_committed: u64,
    pub dlq_failures: u64,
    pub drain_started: u64,
    pub drain_completed: u64,
    pub drain_timeouts: u64,
    pub last_drain_duration_ms: u64,
    pub draining: bool,
    pub shards: Vec<DispatchShardTelemetrySnapshot>,
    pub dropped_cursor_lanes: u64,
    pub host_pressure: HostPressureSnapshot,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_and_cursor_gaps_are_aggregated_without_lane_labels() {
        let telemetry = DispatchTelemetry::new();
        telemetry.record_committed_range(4);
        telemetry.record_queue_enqueue();
        telemetry.record_queue_dequeue();
        telemetry.record_retry_scheduled();
        telemetry.record_retry_due();
        telemetry.record_completion(3, "private-lane-a", 10, 12, 20, true);
        telemetry.record_completion(3, "private-lane-b", 5, 7, 9, true);
        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.committed_ready_ranges, 1);
        assert_eq!(snapshot.committed_ready_records, 4);
        assert_eq!(snapshot.queue_depth, 0);
        assert_eq!(snapshot.retries_scheduled, 1);
        assert_eq!(snapshot.retries_due, 1);
        let shard = &snapshot.shards[0];
        assert_eq!(shard.shard, 3);
        assert_eq!(shard.completions, 2);
        assert_eq!(shard.cursor_advanced_records, 4);
        assert_eq!(shard.cursor_min_offset, 7);
        assert_eq!(shard.cursor_max_offset, 12);
        assert_eq!(shard.completion_gap_records, 10);
        assert_eq!(shard.tracked_cursor_lanes, 2);
    }
}
