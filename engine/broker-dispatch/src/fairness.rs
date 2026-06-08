//! Per-tenant weighted fair queuing (CP6).

use parking_lot::Mutex;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct TenantFairQueue {
    weights: Mutex<HashMap<String, u32>>,
    virtual_time: Mutex<HashMap<String, u64>>,
}

impl TenantFairQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_weight(&self, tenant_id: &str, weight: u32) {
        self.weights
            .lock()
            .insert(tenant_id.to_string(), weight.max(1));
    }

    /// Lower score = higher priority for WFQ.
    pub fn schedule_score(&self, tenant_id: &str) -> u64 {
        let w = self.weights.lock().get(tenant_id).copied().unwrap_or(1) as u64;
        let mut vt = self.virtual_time.lock();
        let t = vt.entry(tenant_id.to_string()).or_insert(0);
        let score = *t;
        *t += 1_000 / w;
        score
    }
}
