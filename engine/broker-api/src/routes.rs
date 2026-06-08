use crate::AppState;
use axum::{
    extract::{Extension, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use broker_partition::{
    BrokerError, CreateSubscriptionRequest, CreateSubscriptionResponse, DestinationSnapshot,
    PublishRequest, PublishResponse, ScheduledInfo,
};
use broker_schedule::ScheduledPublishRequest;
use std::sync::Arc;
use tracing::info;

pub(crate) async fn publish(
    State(state): State<Arc<AppState>>,
    ingest: Option<Extension<crate::metering::IngestAuth>>,
    #[cfg(feature = "cloud")] plan: Option<Extension<broker_control_plane::PlanLimits>>,
    Json(mut req): Json<PublishRequest>,
) -> Result<(StatusCode, Json<PublishResponse>), ApiError> {
    #[cfg(feature = "cloud")]
    if state.uses_cloud_auth() {
        if let Some(plan) = plan.as_ref().map(|e| &e.0) {
            if req.payload.len() as u64 > plan.max_message_bytes {
                return Err(ApiError::BadRequest(format!(
                    "message exceeds plan limit of {} bytes",
                    plan.max_message_bytes
                )));
            }
        }
    }
    if let Some(delay_ms) = req.delay_ms.take() {
        let destination = snapshot_destination(&state, &req).await?;
        let scheduled = state.schedule.schedule(
            ScheduledPublishRequest {
                topic: req.topic.clone(),
                routing_key: req.routing_key.clone(),
                payload: req.payload.clone(),
                payload_encoding: req.payload_encoding.clone(),
                idempotency_key: req.idempotency_key.clone(),
                priority: req.priority,
                parallelism: req.parallelism,
                flow_id: req.flow_id,
                queue_id: req.queue_id,
                destination: Some(destination),
                flow: req.flow.clone(),
                max_retries: req.max_retries,
                retry_backoff: req.retry_backoff.clone(),
                method: req.method.clone(),
                headers: req.headers.clone(),
                sign: req.sign,
                request: req.request.clone(),
            },
            delay_ms,
        )?;
        return Ok((
            StatusCode::ACCEPTED,
            Json(PublishResponse {
                message_id: None,
                topic: req.topic,
                partition: None,
                offset: None,
                duplicate: false,
                scheduled: Some(ScheduledInfo {
                    schedule_id: scheduled.id,
                    deliver_at_ms: scheduled.deliver_at_ms,
                }),
                replication_frame: None,
            }),
        ));
    }

    let meter = crate::metering::ingest_meter(ingest.map(|e| e.0), req.payload.len());
    let resp = crate::cluster::publish_with_cluster(&state, req, meter).await?;
    crate::cluster::enqueue_dispatch_after_publish(&state, &resp);

    let status = if resp.duplicate {
        StatusCode::OK
    } else {
        StatusCode::ACCEPTED
    };
    Ok((status, Json(resp)))
}

pub(crate) async fn create_subscription(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateSubscriptionRequest>,
) -> Result<(StatusCode, Json<CreateSubscriptionResponse>), ApiError> {
    let resp = state.broker.create_subscription(req)?;
    info!(queue_id = %resp.id, queue = %resp.topic, url = %resp.url, "queue ready");
    Ok((StatusCode::CREATED, Json(resp)))
}

/// Frozen destination for delayed enqueue or publish (queue or inline URL).
pub(crate) async fn snapshot_destination(
    state: &Arc<AppState>,
    req: &PublishRequest,
) -> Result<DestinationSnapshot, ApiError> {
    if let (Some(url), Some(secret)) = (req.url.as_ref(), req.secret.as_ref()) {
        return Ok(DestinationSnapshot {
            queue_id: req.queue_id,
            url: url.clone(),
            secret: secret.clone(),
        });
    }
    resolve_destination_with_repair(state, req.queue_id, &req.topic).await
}

pub(crate) async fn resolve_destination_with_repair(
    state: &Arc<AppState>,
    queue_id: Option<uuid::Uuid>,
    queue_name: &str,
) -> Result<DestinationSnapshot, ApiError> {
    match resolve_destination(&state.broker, queue_id, queue_name) {
        Ok(d) => Ok(d),
        Err(ApiError::Broker(BrokerError::QueueNotFound(_))) if state.cluster.is_some() => {
            crate::cluster::sync_catalog_from_peers(state).await;
            resolve_destination(&state.broker, queue_id, queue_name)
        }
        Err(e) => Err(e),
    }
}

pub(crate) fn resolve_destination(
    broker: &broker_partition::Broker,
    queue_id: Option<uuid::Uuid>,
    queue_name: &str,
) -> Result<DestinationSnapshot, ApiError> {
    let q = if let Some(id) = queue_id {
        broker
            .get_queue_by_id(id)?
            .ok_or_else(|| ApiError::Broker(BrokerError::QueueNotFound(id.to_string())))?
    } else {
        broker
            .get_queue(queue_name)?
            .ok_or_else(|| ApiError::Broker(BrokerError::QueueNotFound(queue_name.to_string())))?
    };
    Ok(DestinationSnapshot {
        queue_id: Some(q.id),
        url: q.url,
        secret: q.secret,
    })
}

#[derive(Debug)]
pub enum ApiError {
    Broker(BrokerError),
    BadRequest(String),
    ReplicationFailed(String),
}

impl From<BrokerError> for ApiError {
    fn from(e: BrokerError) -> Self {
        Self::Broker(e)
    }
}

impl From<broker_schedule::ScheduleError> for ApiError {
    fn from(e: broker_schedule::ScheduleError) -> Self {
        Self::Broker(BrokerError::Storage(broker_storage::LogError::Io(
            std::io::Error::other(e.to_string()),
        )))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        match self {
            ApiError::BadRequest(msg) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": msg })),
            )
                .into_response(),
            ApiError::ReplicationFailed(msg) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({ "error": msg })),
            )
                .into_response(),
            ApiError::Broker(e) => {
                let status = match &e {
                    BrokerError::QueueNotFound(_) | BrokerError::FlowProfileNotFound(_) => {
                        StatusCode::NOT_FOUND
                    }
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                let body = serde_json::json!({ "error": e.to_string() });
                (status, Json(body)).into_response()
            }
        }
    }
}
