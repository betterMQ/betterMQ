//! Cloud ingest metering (CP3 / CP6).

use crate::AppState;
#[cfg(feature = "cloud")]
use chrono::Utc;
use std::sync::Arc;
use uuid::Uuid;

/// Set by cloud auth middleware; absent on self-host.
#[derive(Debug, Clone, Copy)]
pub struct IngestAuth {
    pub tenant_id: Uuid,
}

/// Per-request ingest accounting passed into publish paths.
#[derive(Debug, Clone, Copy)]
pub struct IngestMeter {
    pub tenant_id: Uuid,
    pub body_bytes: u64,
}

pub fn ingest_meter(auth: Option<IngestAuth>, body_bytes: usize) -> Option<IngestMeter> {
    auth.map(|a| IngestMeter {
        tenant_id: a.tenant_id,
        body_bytes: body_bytes as u64,
    })
}

/// Record accepted ingest after a non-duplicate publish (best-effort).
pub async fn record_ingest(state: &Arc<AppState>, tenant_id: Uuid, body_bytes: u64) {
    #[cfg(feature = "cloud")]
    {
        let Some(cp) = state.control_plane.as_ref() else {
            return;
        };
        let now = Utc::now();
        let hour = now
            .with_minute(0)
            .and_then(|t| t.with_second(0))
            .and_then(|t| t.with_nanosecond(0))
            .unwrap_or(now);
        let delta = broker_control_plane::UsageCounters {
            messages_accepted: 1,
            bytes_ingested: body_bytes as i64,
            ..Default::default()
        };
        if let Err(e) = broker_control_plane::record_usage(cp, tenant_id, hour, &delta).await {
            tracing::warn!(error = %e, %tenant_id, "record_usage failed");
        }
    }
    #[cfg(not(feature = "cloud"))]
    {
        let _ = (state, tenant_id, body_bytes);
    }
}

#[cfg(feature = "cloud")]
pub fn usage_error_to_api(e: broker_control_plane::UsageError) -> crate::routes::ApiError {
    match e {
        broker_control_plane::UsageError::BandwidthExceeded => {
            crate::routes::ApiError::BadRequest("bandwidth cap exceeded".into())
        }
        broker_control_plane::UsageError::MessagesExceeded => {
            crate::routes::ApiError::BadRequest("monthly message cap exceeded".into())
        }
        other => crate::routes::ApiError::Broker(broker_partition::BrokerError::Storage(
            broker_storage::LogError::Io(std::io::Error::other(other.to_string())),
        )),
    }
}

/// Cloud batch ingest: enforce per-plan monthly message cap for the whole batch.
#[cfg(feature = "cloud")]
pub async fn check_cloud_batch_messages_cap(
    state: &AppState,
    tenant_id: Uuid,
    plan: &broker_control_plane::PlanLimits,
    count: usize,
) -> Result<(), crate::routes::ApiError> {
    let Some(cp) = state.control_plane.as_ref() else {
        return Ok(());
    };
    broker_control_plane::check_messages_cap(cp, tenant_id, plan.messages_per_month, count as u64)
        .await
        .map_err(usage_error_to_api)
}
