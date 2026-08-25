//! Bounded shard-actor queue telemetry.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

pub const MAX_QUEUE_TELEMETRY_SHARDS: usize = 4096;

#[derive(Default)]
struct QueueCounters {
    queued_records: AtomicU64,
    queued_bytes: AtomicU64,
    max_queued_records: AtomicU64,
    max_queued_bytes: AtomicU64,
    rejected_commands: AtomicU64,
}

static SHARDS: OnceLock<Box<[QueueCounters]>> = OnceLock::new();
static DROPPED_SHARD_EVENTS: AtomicU64 = AtomicU64::new(0);

fn shards() -> &'static [QueueCounters] {
    SHARDS.get_or_init(|| {
        (0..MAX_QUEUE_TELEMETRY_SHARDS)
            .map(|_| QueueCounters::default())
            .collect::<Vec<_>>()
            .into_boxed_slice()
    })
}

fn counters(shard: u32) -> Option<&'static QueueCounters> {
    shards().get(shard as usize).or_else(|| {
        DROPPED_SHARD_EVENTS.fetch_add(1, Ordering::Relaxed);
        None
    })
}

fn update_max(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

pub(crate) fn record_enqueue(shard: u32, records: u64, bytes: u64) {
    let Some(counters) = counters(shard) else {
        return;
    };
    let queued_records = counters
        .queued_records
        .fetch_add(records, Ordering::Relaxed)
        .saturating_add(records);
    let queued_bytes = counters
        .queued_bytes
        .fetch_add(bytes, Ordering::Relaxed)
        .saturating_add(bytes);
    update_max(&counters.max_queued_records, queued_records);
    update_max(&counters.max_queued_bytes, queued_bytes);
}

pub(crate) fn record_dequeue(shard: u32, records: u64, bytes: u64) {
    let Some(counters) = counters(shard) else {
        return;
    };
    counters
        .queued_records
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            Some(value.saturating_sub(records))
        })
        .ok();
    counters
        .queued_bytes
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            Some(value.saturating_sub(bytes))
        })
        .ok();
}

pub(crate) fn record_rejected(shard: u32) {
    if let Some(counters) = counters(shard) {
        counters.rejected_commands.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardQueueTelemetrySnapshot {
    pub shard: u32,
    pub queued_records: u64,
    pub queued_bytes: u64,
    pub max_queued_records: u64,
    pub max_queued_bytes: u64,
    pub rejected_commands: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionTelemetrySnapshot {
    pub shards: Vec<ShardQueueTelemetrySnapshot>,
    pub dropped_shard_events: u64,
}

pub fn partition_telemetry_snapshot() -> PartitionTelemetrySnapshot {
    let snapshots = shards()
        .iter()
        .enumerate()
        .filter_map(|(shard, counters)| {
            let queued_records = counters.queued_records.load(Ordering::Relaxed);
            let queued_bytes = counters.queued_bytes.load(Ordering::Relaxed);
            let max_queued_records = counters.max_queued_records.load(Ordering::Relaxed);
            let max_queued_bytes = counters.max_queued_bytes.load(Ordering::Relaxed);
            let rejected_commands = counters.rejected_commands.load(Ordering::Relaxed);
            (queued_records > 0
                || queued_bytes > 0
                || max_queued_records > 0
                || max_queued_bytes > 0
                || rejected_commands > 0)
                .then_some(ShardQueueTelemetrySnapshot {
                    shard: shard as u32,
                    queued_records,
                    queued_bytes,
                    max_queued_records,
                    max_queued_bytes,
                    rejected_commands,
                })
        })
        .collect();
    PartitionTelemetrySnapshot {
        shards: snapshots,
        dropped_shard_events: DROPPED_SHARD_EVENTS.load(Ordering::Relaxed),
    }
}
