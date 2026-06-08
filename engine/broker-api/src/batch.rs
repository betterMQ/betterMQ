//! CP12: batch enqueue API.

use crate::cluster::publish_with_cluster;
use crate::routes::ApiError;
use crate::AppState;
use axum::{
    extract::{Extension, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct BatchEnqueueRequest {
    pub messages: Vec<broker_partition::PublishRequest>,
}

#[derive(Debug, serde::Serialize)]
pub struct BatchEnqueueResponse {
    pub accepted: usize,
    pub message_ids: Vec<uuid::Uuid>,
}

/// Cloud SaaS batch size cap (self-host has no batch limit).
#[cfg(feature = "cloud")]
const CLOUD_MAX_BATCH_MESSAGES: usize = 100;

pub async fn batch_enqueue(
    State(state): State<Arc<AppState>>,
    ingest: Option<Extension<crate::metering::IngestAuth>>,
    #[cfg(feature = "cloud")] plan: Option<Extension<broker_control_plane::PlanLimits>>,
    Json(body): Json<BatchEnqueueRequest>,
) -> Result<(StatusCode, Json<BatchEnqueueResponse>), ApiError> {
    if state.uses_cloud_auth() {
        #[cfg(feature = "cloud")]
        {
            if body.messages.len() > CLOUD_MAX_BATCH_MESSAGES {
                return Err(ApiError::BadRequest(format!(
                    "batch exceeds max size of {CLOUD_MAX_BATCH_MESSAGES}"
                )));
            }
            if let (Some(auth), Some(plan)) =
                (ingest.as_ref().map(|e| e.0), plan.as_ref().map(|e| &e.0))
            {
                crate::metering::check_cloud_batch_messages_cap(
                    &state,
                    auth.tenant_id,
                    plan,
                    body.messages.len(),
                )
                .await?;
                for req in &body.messages {
                    if req.payload.len() as u64 > plan.max_message_bytes {
                        return Err(ApiError::BadRequest(format!(
                            "message exceeds plan limit of {} bytes",
                            plan.max_message_bytes
                        )));
                    }
                }
            }
        }
    }

    let mut ids = Vec::new();
    for req in body.messages {
        let meter = crate::metering::ingest_meter(ingest.as_ref().map(|e| e.0), req.payload.len());
        let resp = publish_with_cluster(&state, req, meter).await?;
        if let Some(message_id) = resp.message_id {
            ids.push(message_id);
            if !resp.duplicate {
                crate::cluster::enqueue_dispatch_after_publish(&state, &resp);
            }
        }
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(BatchEnqueueResponse {
            accepted: ids.len(),
            message_ids: ids,
        }),
    ))
}
