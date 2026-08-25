//! Cell admission: reject quickly with 429/503 rather than unbounded tails.

use crate::routes::ApiError;
use crate::AppState;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::OnceLock;
use uuid::Uuid;

fn env_limit(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
        .max(1)
}

fn max_in_flight() -> u64 {
    env_limit("BETTERMQ_INGEST_MAX_IN_FLIGHT", 4096)
}

fn max_queued_bytes() -> u64 {
    env_limit("BETTERMQ_INGEST_MAX_QUEUED_BYTES", 64 * 1024 * 1024)
}

fn max_tenant_records() -> u64 {
    env_limit("BETTERMQ_INGEST_MAX_TENANT_RECORDS", max_in_flight())
}

fn max_tenant_bytes() -> u64 {
    env_limit("BETTERMQ_INGEST_MAX_TENANT_BYTES", max_queued_bytes())
}

#[derive(Default)]
struct TenantQueued {
    records: u64,
    bytes: u64,
}

static ACCEPTING: AtomicBool = AtomicBool::new(true);
static QUEUED_RECORDS: AtomicU64 = AtomicU64::new(0);
static QUEUED_BYTES: AtomicU64 = AtomicU64::new(0);
static TENANTS: OnceLock<Mutex<HashMap<Uuid, TenantQueued>>> = OnceLock::new();

pub struct InFlightGuard {
    tenant_id: Uuid,
    bytes: u64,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        QUEUED_RECORDS.fetch_sub(1, Ordering::Relaxed);
        QUEUED_BYTES.fetch_sub(self.bytes, Ordering::Relaxed);
        let mut tenants = TENANTS.get_or_init(Default::default).lock();
        if let Some(queued) = tenants.get_mut(&self.tenant_id) {
            queued.records = queued.records.saturating_sub(1);
            queued.bytes = queued.bytes.saturating_sub(self.bytes);
            if queued.records == 0 {
                tenants.remove(&self.tenant_id);
            }
        }
    }
}

/// Stop accepting new ingest before graceful HTTP drain begins.
pub fn begin_shutdown() {
    ACCEPTING.store(false, Ordering::Release);
}

pub(crate) fn is_shutting_down() -> bool {
    !ACCEPTING.load(Ordering::Acquire)
}

pub fn check_admission(
    state: &AppState,
    tenant_id: Option<Uuid>,
    body_bytes: usize,
) -> Result<InFlightGuard, ApiError> {
    if !ACCEPTING.load(Ordering::Acquire) {
        return Err(ApiError::Overloaded {
            retry_after_ms: 1_000,
            message: "broker is draining".into(),
        });
    }
    if broker_storage::archive_admission_blocked() {
        let lag = broker_storage::archive_lag_status();
        return Err(ApiError::Overloaded {
            retry_after_ms: 1_000,
            message: format!(
                "archive lag saturated: {} queued / {} failed segments, {} queued bytes (oldest {} ms)",
                lag.queued_records, lag.failed_records, lag.queued_bytes, lag.oldest_age_ms
            ),
        });
    }
    if state.dispatch.memory_guard().is_critical() {
        return Err(ApiError::Overloaded {
            retry_after_ms: 1_000,
            message: "memory critical; retry shortly".into(),
        });
    }
    reserve_admission(tenant_id, body_bytes)
}

pub(crate) fn check_gateway_admission(
    tenant_id: Option<Uuid>,
    body_bytes: usize,
) -> Result<InFlightGuard, ApiError> {
    if !ACCEPTING.load(Ordering::Acquire) {
        return Err(ApiError::Overloaded {
            retry_after_ms: 1_000,
            message: "gateway is draining".into(),
        });
    }
    reserve_admission(tenant_id, body_bytes)
}

fn reserve_admission(
    tenant_id: Option<Uuid>,
    body_bytes: usize,
) -> Result<InFlightGuard, ApiError> {
    let bytes = body_bytes as u64;
    let records = QUEUED_RECORDS.fetch_add(1, Ordering::AcqRel) + 1;
    let queued_bytes = QUEUED_BYTES.fetch_add(bytes, Ordering::AcqRel) + bytes;
    if records > max_in_flight() || queued_bytes > max_queued_bytes() {
        QUEUED_RECORDS.fetch_sub(1, Ordering::Relaxed);
        QUEUED_BYTES.fetch_sub(bytes, Ordering::Relaxed);
        return Err(ApiError::Overloaded {
            retry_after_ms: 1_000,
            message: "broker ingest queue saturated".into(),
        });
    }

    // Cloud callers get their authenticated tenant. Self-host uses one local
    // tenant, so its tenant limits default to the process limits.
    let tenant_id = tenant_id.unwrap_or_else(Uuid::nil);
    let mut tenants = TENANTS.get_or_init(Default::default).lock();
    let tenant = tenants.entry(tenant_id).or_default();
    if tenant.records + 1 > max_tenant_records() || tenant.bytes + bytes > max_tenant_bytes() {
        if tenant.records == 0 {
            tenants.remove(&tenant_id);
        }
        drop(tenants);
        QUEUED_RECORDS.fetch_sub(1, Ordering::Relaxed);
        QUEUED_BYTES.fetch_sub(bytes, Ordering::Relaxed);
        return Err(ApiError::TooManyRequests {
            retry_after_ms: 1_000,
            message: "tenant ingest queue saturated".into(),
        });
    }
    tenant.records += 1;
    tenant.bytes += bytes;
    drop(tenants);

    Ok(InFlightGuard { tenant_id, bytes })
}
