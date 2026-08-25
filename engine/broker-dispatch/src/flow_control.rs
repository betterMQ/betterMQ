//! Flow control: key, parallelism, rate/period, waitlist.

use broker_partition::ResolvedFlow;
use broker_storage::StoredMessage;
use chrono::Utc;
use serde::Serialize;
use std::cmp::Reverse;
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::memory_guard::MemoryGuard;
use crate::worker::DispatchEngine;

static ENQUEUE_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct FlowKey {
    pub endpoint_id: Uuid,
    pub flow_key: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FlowControlInfo {
    pub flow_key: String,
    pub endpoint_id: Uuid,
    pub wait_list_size: usize,
    pub parallelism_max: u32,
    pub parallelism_count: u32,
    pub rate_max: u32,
    pub rate_count: u32,
    pub rate_period_secs: u64,
    pub rate_period_start_ms: i64,
    pub paused: bool,
    pub pinned_parallelism: bool,
    pub pinned_rate: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct GlobalParallelismInfo {
    pub parallelism_max: Option<u32>,
    pub parallelism_count: u32,
}

#[derive(Debug, Clone)]
struct PinnedLimits {
    parallelism: Option<u32>,
    rate: Option<u32>,
    period_secs: Option<u64>,
}

#[derive(Clone)]
struct FlowWork {
    engine: DispatchEngine,
    msg: Arc<StoredMessage>,
    backfill: bool,
}

struct OrderedWork {
    backfill: bool,
    priority: u8,
    published_at_ms: i64,
    partition: u32,
    offset: u64,
    seq: u64,
    work: FlowWork,
}

type FifoOrder = (i64, u32, u64, u64);
type PriorityOrder = (bool, u8, Reverse<u64>, Reverse<u64>);

#[derive(Default)]
struct WorkQueue {
    items: HashMap<u64, OrderedWork>,
    fifo: BTreeSet<FifoOrder>,
    priority: BTreeSet<PriorityOrder>,
}

impl WorkQueue {
    fn len(&self) -> usize {
        self.items.len()
    }

    fn push(&mut self, item: OrderedWork) {
        self.fifo.insert(fifo_order(&item));
        self.priority.insert(priority_order(&item));
        self.items.insert(item.seq, item);
    }

    fn pop(&mut self, strict_fifo: bool) -> Option<OrderedWork> {
        let sequence = if strict_fifo {
            self.fifo.first().map(|key| key.3)
        } else {
            self.priority.last().map(|key| key.3 .0)
        }?;
        let item = self.items.remove(&sequence)?;
        self.fifo.remove(&fifo_order(&item));
        self.priority.remove(&priority_order(&item));
        Some(item)
    }
}

fn fifo_order(item: &OrderedWork) -> FifoOrder {
    (item.published_at_ms, item.partition, item.offset, item.seq)
}

fn priority_order(item: &OrderedWork) -> PriorityOrder {
    (
        !item.backfill,
        item.priority,
        Reverse(item.offset),
        Reverse(item.seq),
    )
}

struct RateWindow {
    max: u32,
    period_secs: u64,
    period_start_ms: i64,
    count: u32,
}

impl RateWindow {
    fn new(max: u32, period_secs: u64) -> Self {
        Self {
            max,
            period_secs,
            period_start_ms: Utc::now().timestamp_millis(),
            count: 0,
        }
    }

    fn try_acquire(&mut self, now_ms: i64) -> bool {
        if self.max == 0 {
            return true;
        }
        let period_ms = (self.period_secs * 1000) as i64;
        if now_ms.saturating_sub(self.period_start_ms) >= period_ms {
            self.period_start_ms = now_ms;
            self.count = 0;
        }
        if self.count < self.max {
            self.count += 1;
            true
        } else {
            false
        }
    }

    fn reset(&mut self, now_ms: i64) {
        self.period_start_ms = now_ms;
        self.count = 0;
    }
}

struct FlowLane {
    limits: Mutex<ResolvedFlow>,
    waitlist: Mutex<WorkQueue>,
    active: AtomicU32,
    rate: Mutex<RateWindow>,
    paused: Mutex<bool>,
    pinned: Mutex<PinnedLimits>,
    notify: Notify,
}

#[derive(Clone)]
pub struct FlowController {
    lanes: Arc<Mutex<HashMap<FlowKey, Arc<FlowLane>>>>,
    global_max_parallelism: Option<u32>,
    global_active: Arc<AtomicU32>,
    memory_guard: Arc<MemoryGuard>,
    stop: Arc<AtomicBool>,
    drainers: Arc<parking_lot::Mutex<Vec<JoinHandle<()>>>>,
}

impl FlowController {
    pub fn new(global_max_parallelism: Option<u32>, memory_guard: Arc<MemoryGuard>) -> Self {
        Self {
            lanes: Arc::new(Mutex::new(HashMap::new())),
            global_max_parallelism,
            global_active: Arc::new(AtomicU32::new(0)),
            memory_guard,
            stop: Arc::new(AtomicBool::new(false)),
            drainers: Arc::new(parking_lot::Mutex::new(Vec::new())),
        }
    }

    pub fn take_drainer_tasks(&self) -> Vec<JoinHandle<()>> {
        self.stop.store(true, AtomicOrdering::Release);
        std::mem::take(&mut *self.drainers.lock())
    }

    pub async fn submit(
        &self,
        engine: DispatchEngine,
        lane_owner: Uuid,
        msg: StoredMessage,
        limits: ResolvedFlow,
        backfill: bool,
    ) {
        let flow_key = limits.key.clone();
        let key = FlowKey {
            endpoint_id: lane_owner,
            flow_key: flow_key.clone(),
        };
        let lane = self.get_or_create_lane(key, limits).await;
        let work = FlowWork {
            engine,
            msg: Arc::new(msg),
            backfill,
        };
        let mut heap = lane.waitlist.lock().await;
        let cap = std::env::var("BETTERMQ_FLOW_WAITLIST_CAP")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(8192)
            .max(1);
        if heap.len() >= cap {
            tracing::warn!(
                endpoint_id = %lane_owner,
                flow_key = %flow_key,
                cap,
                "flow waitlist full; dropping work (backfill will retry)"
            );
            return;
        }
        heap.push(OrderedWork {
            backfill: work.backfill,
            priority: work.msg.priority,
            published_at_ms: work.msg.published_at_ms,
            partition: work.msg.partition,
            offset: work.msg.offset,
            seq: ENQUEUE_SEQ.fetch_add(1, AtomicOrdering::Relaxed),
            work,
        });
        lane.notify.notify_one();
    }

    pub async fn get(&self, endpoint_id: Uuid, flow_key: &str) -> Option<FlowControlInfo> {
        let lanes = self.lanes.lock().await;
        let lane = lanes.get(&FlowKey {
            endpoint_id,
            flow_key: flow_key.to_string(),
        })?;
        Some(self.info_from_lane(lane).await)
    }

    pub async fn list_keys(&self, endpoint_id: Uuid) -> Vec<FlowControlInfo> {
        let lanes = self.lanes.lock().await;
        let mut out = Vec::new();
        for (key, lane) in lanes.iter() {
            if key.endpoint_id == endpoint_id {
                out.push(self.info_from_lane(lane).await);
            }
        }
        out
    }

    pub async fn global_parallelism(&self) -> GlobalParallelismInfo {
        GlobalParallelismInfo {
            parallelism_max: self.global_max_parallelism,
            parallelism_count: self.global_active.load(AtomicOrdering::Relaxed),
        }
    }

    pub async fn pending_count(&self) -> usize {
        let lanes = self.lanes.lock().await;
        let mut pending = 0usize;
        for lane in lanes.values() {
            pending = pending
                .saturating_add(lane.waitlist.lock().await.len())
                .saturating_add(lane.active.load(AtomicOrdering::Acquire) as usize);
        }
        pending
    }

    /// Create the in-memory lane if missing (so pause/pin work before first delivery).
    pub async fn ensure_lane(&self, endpoint_id: Uuid, flow_key: &str, limits: ResolvedFlow) {
        let _ = self
            .get_or_create_lane(
                FlowKey {
                    endpoint_id,
                    flow_key: flow_key.to_string(),
                },
                limits,
            )
            .await;
    }

    pub async fn pause(&self, endpoint_id: Uuid, flow_key: &str) -> bool {
        if let Some(lane) = self.lane_ref(endpoint_id, flow_key).await {
            *lane.paused.lock().await = true;
            return true;
        }
        false
    }

    pub async fn resume(&self, endpoint_id: Uuid, flow_key: &str) -> bool {
        if let Some(lane) = self.lane_ref(endpoint_id, flow_key).await {
            *lane.paused.lock().await = false;
            lane.notify.notify_one();
            return true;
        }
        false
    }

    pub async fn pin(
        &self,
        endpoint_id: Uuid,
        flow_key: &str,
        parallelism: Option<u32>,
        rate: Option<u32>,
        period_secs: Option<u64>,
    ) -> bool {
        if let Some(lane) = self.lane_ref(endpoint_id, flow_key).await {
            let mut pin = lane.pinned.lock().await;
            if parallelism.is_some() {
                pin.parallelism = parallelism;
            }
            if rate.is_some() {
                pin.rate = rate;
            }
            if period_secs.is_some() {
                pin.period_secs = period_secs;
            }
            self.apply_pins(&lane).await;
            lane.notify.notify_one();
            return true;
        }
        false
    }

    pub async fn unpin(
        &self,
        endpoint_id: Uuid,
        flow_key: &str,
        parallelism: bool,
        rate: bool,
    ) -> bool {
        if let Some(lane) = self.lane_ref(endpoint_id, flow_key).await {
            let mut pin = lane.pinned.lock().await;
            if parallelism {
                pin.parallelism = None;
            }
            if rate {
                pin.rate = None;
            }
            drop(pin);
            lane.notify.notify_one();
            return true;
        }
        false
    }

    pub async fn reset_rate(&self, endpoint_id: Uuid, flow_key: &str) -> bool {
        if let Some(lane) = self.lane_ref(endpoint_id, flow_key).await {
            lane.rate.lock().await.reset(Utc::now().timestamp_millis());
            lane.notify.notify_one();
            return true;
        }
        false
    }

    async fn info_from_lane(&self, lane: &FlowLane) -> FlowControlInfo {
        let lim = lane.limits.lock().await;
        let rate = lane.rate.lock().await;
        let pin = lane.pinned.lock().await;
        FlowControlInfo {
            flow_key: lim.key.clone(),
            endpoint_id: Uuid::nil(), // filled by caller if needed
            wait_list_size: lane.waitlist.lock().await.len(),
            parallelism_max: lim.parallelism,
            parallelism_count: lane.active.load(AtomicOrdering::Relaxed),
            rate_max: lim.rate,
            rate_count: rate.count,
            rate_period_secs: lim.period_secs,
            rate_period_start_ms: rate.period_start_ms,
            paused: *lane.paused.lock().await,
            pinned_parallelism: pin.parallelism.is_some(),
            pinned_rate: pin.rate.is_some(),
        }
    }

    async fn lane_ref(&self, endpoint_id: Uuid, flow_key: &str) -> Option<Arc<FlowLane>> {
        let lanes = self.lanes.lock().await;
        lanes
            .get(&FlowKey {
                endpoint_id,
                flow_key: flow_key.to_string(),
            })
            .cloned()
    }

    async fn get_or_create_lane(&self, key: FlowKey, limits: ResolvedFlow) -> Arc<FlowLane> {
        let mut map = self.lanes.lock().await;
        if let Some(existing) = map.get(&key) {
            // Update limits in place — never replace the lane (that orphans the waitlist
            // and leaks a drainer task parked on the old Notify).
            {
                let mut el = existing.limits.lock().await;
                el.parallelism = limits.parallelism;
                el.rate = limits.rate;
                el.period_secs = limits.period_secs;
            }
            {
                let mut rate = existing.rate.lock().await;
                rate.max = limits.rate;
                rate.period_secs = limits.period_secs.max(1);
            }
            existing.notify.notify_one();
            return existing.clone();
        }
        let lane = Arc::new(FlowLane {
            limits: Mutex::new(limits.clone()),
            waitlist: Mutex::new(WorkQueue::default()),
            active: AtomicU32::new(0),
            rate: Mutex::new(RateWindow::new(limits.rate, limits.period_secs)),
            paused: Mutex::new(false),
            pinned: Mutex::new(PinnedLimits {
                parallelism: None,
                rate: None,
                period_secs: None,
            }),
            notify: Notify::new(),
        });
        if !self.stop.load(AtomicOrdering::Acquire) {
            spawn_lane_drainer(
                lane.clone(),
                self.global_max_parallelism,
                self.global_active.clone(),
                self.memory_guard.clone(),
                self.stop.clone(),
                self.drainers.clone(),
            );
        }
        map.insert(key, lane.clone());
        lane
    }

    async fn apply_pins(&self, lane: &FlowLane) {
        let pin = lane.pinned.lock().await;
        let mut lim = lane.limits.lock().await;
        if let Some(p) = pin.parallelism {
            lim.parallelism = p.max(1);
        }
        if let Some(r) = pin.rate {
            lim.rate = r;
            lane.rate.lock().await.max = r;
        }
        if let Some(period) = pin.period_secs {
            lim.period_secs = period.max(1);
            lane.rate.lock().await.period_secs = period.max(1);
        }
    }
}

fn spawn_lane_drainer(
    lane: Arc<FlowLane>,
    global_max: Option<u32>,
    global_active: Arc<AtomicU32>,
    memory_guard: Arc<MemoryGuard>,
    stop: Arc<AtomicBool>,
    drainers: Arc<parking_lot::Mutex<Vec<JoinHandle<()>>>>,
) {
    let handle = tokio::spawn(async move {
        loop {
            if stop.load(AtomicOrdering::Acquire) {
                break;
            }
            if memory_guard.is_critical() {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                if stop.load(AtomicOrdering::Acquire) {
                    break;
                }
                lane.notify.notify_one();
                continue;
            }

            if *lane.paused.lock().await {
                if stop.load(AtomicOrdering::Acquire) {
                    break;
                }
                lane.notify.notified().await;
                continue;
            }

            let parallelism = lane.limits.lock().await.parallelism;
            let active = lane.active.load(AtomicOrdering::Relaxed);
            if active >= parallelism {
                lane.notify.notified().await;
                continue;
            }

            if let Some(gmax) = global_max {
                if global_active.load(AtomicOrdering::Relaxed) >= gmax {
                    lane.notify.notified().await;
                    continue;
                }
            }

            let now_ms = Utc::now().timestamp_millis();
            if !lane.rate.lock().await.try_acquire(now_ms) {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                if stop.load(AtomicOrdering::Acquire) {
                    break;
                }
                lane.notify.notify_one();
                continue;
            }

            let next = {
                let mut heap = lane.waitlist.lock().await;
                heap.pop(parallelism == 1)
            };
            let Some(ordered) = next else {
                lane.notify.notified().await;
                continue;
            };

            lane.active.fetch_add(1, AtomicOrdering::Relaxed);
            global_active.fetch_add(1, AtomicOrdering::Relaxed);

            let lane2 = lane.clone();
            let global_active2 = global_active.clone();
            tokio::spawn(async move {
                ordered
                    .work
                    .engine
                    .deliver_message(&ordered.work.msg)
                    .await
                    .ok();
                lane2.active.fetch_sub(1, AtomicOrdering::Relaxed);
                global_active2.fetch_sub(1, AtomicOrdering::Relaxed);
                lane2.notify.notify_one();
            });
        }
    });
    drainers.lock().push(handle);
}
