//! Bounded, lock-free-per-shard WAL telemetry.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

pub const MAX_TELEMETRY_SHARDS: usize = 4096;
pub const FSYNC_BUCKET_US: [u64; 10] = [
    100, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000, 1_000_000,
];

#[derive(Default)]
struct ShardCounters {
    epoch_count: AtomicU64,
    epoch_records_total: AtomicU64,
    epoch_bytes_total: AtomicU64,
    last_epoch_records: AtomicU64,
    last_epoch_bytes: AtomicU64,
    leader_epoch: AtomicU64,
    fsync_count: AtomicU64,
    fsync_failures: AtomicU64,
    fsync_records_total: AtomicU64,
    fsync_latency_count: AtomicU64,
    fsync_latency_sum_us: AtomicU64,
    fsync_buckets: [AtomicU64; 10],
}

#[derive(Default)]
struct Registry {
    shards: BTreeMap<u32, Arc<ShardCounters>>,
    dropped_shards: u64,
}

static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();

fn registry() -> &'static Mutex<Registry> {
    REGISTRY.get_or_init(|| Mutex::new(Registry::default()))
}

#[derive(Clone)]
pub(crate) struct StorageTelemetryHandle {
    counters: Arc<ShardCounters>,
}

pub(crate) fn register_shard(shard: u32) -> StorageTelemetryHandle {
    let mut registry = registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let counters = if let Some(existing) = registry.shards.get(&shard) {
        Arc::clone(existing)
    } else if registry.shards.len() < MAX_TELEMETRY_SHARDS {
        let counters = Arc::new(ShardCounters::default());
        registry.shards.insert(shard, Arc::clone(&counters));
        counters
    } else {
        registry.dropped_shards = registry.dropped_shards.saturating_add(1);
        Arc::new(ShardCounters::default())
    };
    StorageTelemetryHandle { counters }
}

impl StorageTelemetryHandle {
    pub(crate) fn record_epoch(&self, leader_epoch: u64, records: u64, bytes: u64) {
        self.counters.epoch_count.fetch_add(1, Ordering::Relaxed);
        self.counters
            .epoch_records_total
            .fetch_add(records, Ordering::Relaxed);
        self.counters
            .epoch_bytes_total
            .fetch_add(bytes, Ordering::Relaxed);
        self.counters
            .last_epoch_records
            .store(records, Ordering::Relaxed);
        self.counters
            .last_epoch_bytes
            .store(bytes, Ordering::Relaxed);
        self.counters
            .leader_epoch
            .store(leader_epoch, Ordering::Relaxed);
    }

    pub(crate) fn record_fsync(&self, records: u64, latency: Duration, success: bool) {
        let micros = latency.as_micros().min(u128::from(u64::MAX)) as u64;
        self.counters
            .fsync_latency_count
            .fetch_add(1, Ordering::Relaxed);
        self.counters
            .fsync_latency_sum_us
            .fetch_add(micros, Ordering::Relaxed);
        if let Some(index) = FSYNC_BUCKET_US.iter().position(|edge| micros <= *edge) {
            self.counters.fsync_buckets[index].fetch_add(1, Ordering::Relaxed);
        }
        if success {
            self.counters.fsync_count.fetch_add(1, Ordering::Relaxed);
            self.counters
                .fsync_records_total
                .fetch_add(records, Ordering::Relaxed);
        } else {
            self.counters.fsync_failures.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageShardTelemetrySnapshot {
    pub shard: u32,
    pub leader_epoch: u64,
    pub epoch_count: u64,
    pub epoch_records_total: u64,
    pub epoch_bytes_total: u64,
    pub last_epoch_records: u64,
    pub last_epoch_bytes: u64,
    pub fsync_count: u64,
    pub fsync_failures: u64,
    pub fsync_records_total: u64,
    pub fsync_latency_count: u64,
    pub fsync_latency_sum_us: u64,
    /// Non-cumulative counts matching [`FSYNC_BUCKET_US`].
    pub fsync_buckets: [u64; 10],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageTelemetrySnapshot {
    pub shards: Vec<StorageShardTelemetrySnapshot>,
    pub dropped_shards: u64,
}

pub fn storage_telemetry_snapshot() -> StorageTelemetrySnapshot {
    let registry = registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let shards = registry
        .shards
        .iter()
        .map(|(shard, counters)| StorageShardTelemetrySnapshot {
            shard: *shard,
            leader_epoch: counters.leader_epoch.load(Ordering::Relaxed),
            epoch_count: counters.epoch_count.load(Ordering::Relaxed),
            epoch_records_total: counters.epoch_records_total.load(Ordering::Relaxed),
            epoch_bytes_total: counters.epoch_bytes_total.load(Ordering::Relaxed),
            last_epoch_records: counters.last_epoch_records.load(Ordering::Relaxed),
            last_epoch_bytes: counters.last_epoch_bytes.load(Ordering::Relaxed),
            fsync_count: counters.fsync_count.load(Ordering::Relaxed),
            fsync_failures: counters.fsync_failures.load(Ordering::Relaxed),
            fsync_records_total: counters.fsync_records_total.load(Ordering::Relaxed),
            fsync_latency_count: counters.fsync_latency_count.load(Ordering::Relaxed),
            fsync_latency_sum_us: counters.fsync_latency_sum_us.load(Ordering::Relaxed),
            fsync_buckets: std::array::from_fn(|index| {
                counters.fsync_buckets[index].load(Ordering::Relaxed)
            }),
        })
        .collect();
    StorageTelemetrySnapshot {
        shards,
        dropped_shards: registry.dropped_shards,
    }
}
