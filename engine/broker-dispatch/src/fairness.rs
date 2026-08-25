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

    pub fn telemetry_snapshot(&self) -> crate::telemetry::TenantFairnessSnapshot {
        let configured_weights = self.weights.lock().len() as u64;
        let tracked_tenants = self.virtual_time.lock().len() as u64;
        let soft_limit = std::env::var("BETTERMQ_FAIRNESS_TENANT_SOFT_LIMIT")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(1024)
            .max(1);
        crate::telemetry::TenantFairnessSnapshot {
            tracked_tenants,
            configured_weights,
            soft_limit,
            saturation_ratio: tracked_tenants as f64 / soft_limit as f64,
            saturated: tracked_tenants >= soft_limit,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn telemetry_aggregates_tenants_without_exposing_ids() {
        let queue = TenantFairQueue::new();
        queue.set_weight("tenant-secret-a", 2);
        queue.schedule_score("tenant-secret-a");
        queue.schedule_score("tenant-secret-b");
        let snapshot = queue.telemetry_snapshot();
        assert_eq!(snapshot.tracked_tenants, 2);
        assert_eq!(snapshot.configured_weights, 1);
        assert!(snapshot.saturation_ratio > 0.0);
    }
}
