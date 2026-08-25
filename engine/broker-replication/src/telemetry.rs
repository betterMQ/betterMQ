//! Bounded per-shard replication and quorum telemetry.

use crate::ReplicaProgress;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub const MAX_REPLICATION_TELEMETRY_SHARDS: usize = 4096;
pub const QUORUM_BUCKET_MS: [u64; 10] = [1, 2, 5, 10, 20, 50, 100, 250, 500, 1_000];

#[derive(Default)]
struct ShardCounters {
    quorum_requests: AtomicU64,
    quorum_failures: AtomicU64,
    follower_durable_acks_total: AtomicU64,
    last_durable_acks: AtomicU64,
    last_required_quorum: AtomicU64,
    quorum_latency_count: AtomicU64,
    quorum_latency_sum_us: AtomicU64,
    quorum_buckets: [AtomicU64; 10],
}

#[derive(Default)]
struct Registry {
    shards: BTreeMap<u32, Arc<ShardCounters>>,
    dropped_shard_events: u64,
}

#[derive(Default)]
pub(crate) struct ReplicationTelemetry {
    registry: Mutex<Registry>,
}

impl ReplicationTelemetry {
    pub(crate) fn record_quorum(
        &self,
        shard: u32,
        latency: Duration,
        durable_acks: usize,
        required_quorum: usize,
        success: bool,
    ) {
        let counters = {
            let mut registry = self
                .registry
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(existing) = registry.shards.get(&shard) {
                Arc::clone(existing)
            } else if registry.shards.len() < MAX_REPLICATION_TELEMETRY_SHARDS {
                let counters = Arc::new(ShardCounters::default());
                registry.shards.insert(shard, Arc::clone(&counters));
                counters
            } else {
                registry.dropped_shard_events = registry.dropped_shard_events.saturating_add(1);
                return;
            }
        };
        let micros = latency.as_micros().min(u128::from(u64::MAX)) as u64;
        counters.quorum_requests.fetch_add(1, Ordering::Relaxed);
        counters
            .follower_durable_acks_total
            .fetch_add(durable_acks.saturating_sub(1) as u64, Ordering::Relaxed);
        counters
            .last_durable_acks
            .store(durable_acks as u64, Ordering::Relaxed);
        counters
            .last_required_quorum
            .store(required_quorum as u64, Ordering::Relaxed);
        counters
            .quorum_latency_count
            .fetch_add(1, Ordering::Relaxed);
        counters
            .quorum_latency_sum_us
            .fetch_add(micros, Ordering::Relaxed);
        if let Some(index) = QUORUM_BUCKET_MS
            .iter()
            .position(|edge| micros <= edge.saturating_mul(1_000))
        {
            counters.quorum_buckets[index].fetch_add(1, Ordering::Relaxed);
        }
        if !success {
            counters.quorum_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn snapshot(&self, progress: &[ReplicaProgress]) -> ReplicationTelemetrySnapshot {
        let registry = self
            .registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut shard_ids: BTreeSet<u32> = registry.shards.keys().copied().collect();
        shard_ids.extend(progress.iter().map(|item| item.partition));
        let shards = shard_ids
            .into_iter()
            .take(MAX_REPLICATION_TELEMETRY_SHARDS)
            .map(|shard| {
                let counters = registry.shards.get(&shard);
                let mut isr_members = 1u64;
                let mut max_replica_lag = 0u64;
                for replica in progress.iter().filter(|item| item.partition == shard) {
                    isr_members = isr_members.saturating_add(u64::from(replica.in_sync));
                    max_replica_lag = max_replica_lag.max(replica.lag);
                }
                ReplicationShardTelemetrySnapshot {
                    shard,
                    quorum_requests: load(counters, |value| &value.quorum_requests),
                    quorum_failures: load(counters, |value| &value.quorum_failures),
                    follower_durable_acks_total: load(counters, |value| {
                        &value.follower_durable_acks_total
                    }),
                    last_durable_acks: load(counters, |value| &value.last_durable_acks),
                    last_required_quorum: load(counters, |value| &value.last_required_quorum),
                    quorum_latency_count: load(counters, |value| &value.quorum_latency_count),
                    quorum_latency_sum_us: load(counters, |value| &value.quorum_latency_sum_us),
                    quorum_buckets: std::array::from_fn(|index| {
                        counters
                            .map(|value| value.quorum_buckets[index].load(Ordering::Relaxed))
                            .unwrap_or(0)
                    }),
                    isr_members,
                    max_replica_lag,
                }
            })
            .collect();
        ReplicationTelemetrySnapshot {
            shards,
            dropped_shard_events: registry.dropped_shard_events,
        }
    }
}

fn load(
    counters: Option<&Arc<ShardCounters>>,
    field: impl FnOnce(&ShardCounters) -> &AtomicU64,
) -> u64 {
    counters
        .map(|value| field(value).load(Ordering::Relaxed))
        .unwrap_or(0)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationShardTelemetrySnapshot {
    pub shard: u32,
    pub quorum_requests: u64,
    pub quorum_failures: u64,
    pub follower_durable_acks_total: u64,
    pub last_durable_acks: u64,
    pub last_required_quorum: u64,
    pub quorum_latency_count: u64,
    pub quorum_latency_sum_us: u64,
    /// Non-cumulative counts matching [`QUORUM_BUCKET_MS`].
    pub quorum_buckets: [u64; 10],
    pub isr_members: u64,
    pub max_replica_lag: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationTelemetrySnapshot {
    pub shards: Vec<ReplicationShardTelemetrySnapshot>,
    pub dropped_shard_events: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_quorum_success_and_failure() {
        let telemetry = ReplicationTelemetry::default();
        telemetry.record_quorum(7, Duration::from_millis(3), 2, 2, true);
        telemetry.record_quorum(7, Duration::from_millis(30), 1, 2, false);
        let snapshot = telemetry.snapshot(&[]);
        let shard = &snapshot.shards[0];
        assert_eq!(shard.shard, 7);
        assert_eq!(shard.quorum_requests, 2);
        assert_eq!(shard.quorum_failures, 1);
        assert_eq!(shard.follower_durable_acks_total, 1);
        assert_eq!(shard.quorum_latency_count, 2);
        assert_eq!(shard.quorum_buckets.iter().sum::<u64>(), 2);
    }
}
