//! CP13: stateless ingest gateway — accepts batch and forwards to shard leaders via cluster layer.

use crate::batch::{batch_enqueue, BatchEnqueueRequest, BatchEnqueueResponse};
use crate::routes::ApiError;
use crate::AppState;
use axum::{extract::State, http::StatusCode, Json};
use std::sync::Arc;

/// Same as batch enqueue; intended for edge gateways without local WAL.
pub async fn gateway_enqueue(
    state: State<Arc<AppState>>,
    ingest: Option<axum::extract::Extension<crate::metering::IngestAuth>>,
    #[cfg(feature = "cloud")] plan: Option<
        axum::extract::Extension<broker_control_plane::PlanLimits>,
    >,
    body: Json<BatchEnqueueRequest>,
) -> Result<(StatusCode, Json<BatchEnqueueResponse>), ApiError> {
    #[cfg(feature = "cloud")]
    return batch_enqueue(state, ingest, plan, body).await;
    #[cfg(not(feature = "cloud"))]
    batch_enqueue(state, ingest, body).await
}
