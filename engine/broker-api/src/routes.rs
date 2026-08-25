use crate::AppState;
use axum::{
    extract::{Extension, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use broker_partition::{
    BrokerError, CreateSubscriptionRequest, CreateSubscriptionResponse, DestinationSnapshot,
    FlowProfileError, PublishRequest, PublishResponse,
};
use std::sync::Arc;
use tracing::info;

pub(crate) async fn publish(
    State(state): State<Arc<AppState>>,
    ingest: Option<Extension<crate::metering::IngestAuth>>,
    #[cfg(feature = "cloud")] plan: Option<Extension<broker_control_plane::PlanLimits>>,
    Json(req): Json<PublishRequest>,
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
    let meter = crate::metering::ingest_meter(ingest.map(|e| e.0), req.payload.len());
    let resp = crate::ingest::submit_one(&state, req, meter).await?;
    if resp.scheduled.is_none() {
        crate::ingest::notify_dispatch(&state, &resp);
    }
    if resp.duplicate {
        crate::metrics::record_duplicate();
    } else if resp.scheduled.is_none() {
        crate::metrics::record_accepted();
    }
    let status = crate::ingest::status_for(&resp);
    Ok((status, Json(resp)))
}

pub(crate) async fn create_subscription(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateSubscriptionRequest>,
) -> Result<(StatusCode, Json<CreateSubscriptionResponse>), ApiError> {
    validate_destination_url_str(&req.url)?;
    let resp = state.broker.create_subscription(req)?;
    info!(queue_id = %resp.id, queue = %resp.topic, url = %resp.url, "queue ready");
    Ok((StatusCode::CREATED, Json(resp)))
}

fn validate_destination_url_str(url: &str) -> Result<(), ApiError> {
    broker_dispatch::validate_destination_url(url).map_err(|e| ApiError::BadRequest(e.to_string()))
}

/// Public for catalog apply / other ingest paths.
pub fn validate_destination_url_str_pub(url: &str) -> Result<(), ApiError> {
    validate_destination_url_str(url)
}

fn validate_publish_destinations(req: &PublishRequest) -> Result<(), ApiError> {
    if let Some(url) = req.url.as_deref().filter(|u| !u.trim().is_empty()) {
        validate_destination_url_str(url)?;
    }
    if let Some(dest) = req.destination.as_ref() {
        validate_destination_url_str(&dest.url)?;
    }
    Ok(())
}

/// Public for batch / gateway ingest paths.
pub fn validate_publish_destinations_pub(req: &PublishRequest) -> Result<(), ApiError> {
    validate_publish_destinations(req)
}

/// Frozen destination for delayed enqueue or publish (queue, inline URL, or snapshot).
pub(crate) async fn snapshot_destination(
    state: &Arc<AppState>,
    req: &PublishRequest,
) -> Result<DestinationSnapshot, ApiError> {
    if let Some(dest) = req.destination.as_ref() {
        return Ok(dest.clone());
    }
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
    Unauthorized(String),
    NotFound(String),
    Conflict(String),
    Unavailable(String),
    ReplicationFailed(String),
    Overloaded {
        retry_after_ms: u64,
        message: String,
    },
    TooManyRequests {
        retry_after_ms: u64,
        message: String,
    },
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Broker(error) => write!(formatter, "{error}"),
            Self::BadRequest(message)
            | Self::Unauthorized(message)
            | Self::NotFound(message)
            | Self::Conflict(message)
            | Self::Unavailable(message)
            | Self::ReplicationFailed(message) => formatter.write_str(message),
            Self::Overloaded { message, .. } | Self::TooManyRequests { message, .. } => {
                formatter.write_str(message)
            }
        }
    }
}

impl From<BrokerError> for ApiError {
    fn from(e: BrokerError) -> Self {
        match e {
            BrokerError::FlowProfile(FlowProfileError::Duplicate(id)) => {
                Self::Conflict(format!("duplicate flow {id}"))
            }
            other => Self::Broker(other),
        }
    }
}

impl From<broker_schedule::ScheduleError> for ApiError {
    fn from(e: broker_schedule::ScheduleError) -> Self {
        Self::Broker(BrokerError::Storage(broker_storage::LogError::Io(
            std::io::Error::other(e.to_string()),
        )))
    }
}

fn retry_after_seconds(retry_after_ms: u64) -> u64 {
    retry_after_ms
        .saturating_add(999)
        .saturating_div(1_000)
        .max(1)
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        match self {
            ApiError::BadRequest(msg) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": msg })),
            )
                .into_response(),
            ApiError::Unauthorized(msg) => (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": msg })),
            )
                .into_response(),
            ApiError::NotFound(msg) => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": msg })),
            )
                .into_response(),
            ApiError::Conflict(msg) => (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": msg })),
            )
                .into_response(),
            ApiError::Unavailable(msg) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({ "error": msg, "code": "not_shard_leader" })),
            )
                .into_response(),
            ApiError::ReplicationFailed(msg) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({ "error": msg })),
            )
                .into_response(),
            ApiError::Overloaded {
                retry_after_ms,
                message,
            } => {
                let mut resp = (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({ "error": message, "code": "saturated" })),
                )
                    .into_response();
                if let Ok(v) = axum::http::HeaderValue::from_str(
                    &retry_after_seconds(retry_after_ms).to_string(),
                ) {
                    resp.headers_mut().insert("retry-after", v);
                }
                resp
            }
            ApiError::TooManyRequests {
                retry_after_ms,
                message,
            } => {
                let mut resp = (
                    StatusCode::TOO_MANY_REQUESTS,
                    Json(serde_json::json!({ "error": message, "code": "quota" })),
                )
                    .into_response();
                if let Ok(v) = axum::http::HeaderValue::from_str(
                    &retry_after_seconds(retry_after_ms).to_string(),
                ) {
                    resp.headers_mut().insert("retry-after", v);
                }
                resp
            }
            ApiError::Broker(e) => {
                let (status, code, msg) = match &e {
                    BrokerError::QueueNotFound(_) | BrokerError::FlowProfileNotFound(_) => {
                        (StatusCode::NOT_FOUND, "not_found", e.to_string())
                    }
                    BrokerError::NotShardLeader(_) => (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "not_shard_leader",
                        "not shard leader".to_string(),
                    ),
                    BrokerError::InvalidName(_) | BrokerError::InvalidConfig(_) => {
                        (StatusCode::BAD_REQUEST, "invalid_request", e.to_string())
                    }
                    other => {
                        tracing::error!(error = %other, "broker error");
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "internal_error",
                            "internal error".to_string(),
                        )
                    }
                };
                let body = serde_json::json!({ "error": msg, "code": code });
                (status, Json(body)).into_response()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::header::RETRY_AFTER;

    #[test]
    fn retry_after_is_whole_seconds_not_milliseconds() {
        assert_eq!(retry_after_seconds(1), 1);
        assert_eq!(retry_after_seconds(1_000), 1);
        assert_eq!(retry_after_seconds(1_001), 2);

        let response = ApiError::Overloaded {
            retry_after_ms: 1_500,
            message: "busy".into(),
        }
        .into_response();
        assert_eq!(response.headers().get(RETRY_AFTER).unwrap(), "2");
    }

    #[test]
    fn tenant_saturation_is_429_and_system_saturation_is_503() {
        let tenant = ApiError::TooManyRequests {
            retry_after_ms: 1_000,
            message: "tenant".into(),
        }
        .into_response();
        let system = ApiError::Overloaded {
            retry_after_ms: 1_000,
            message: "system".into(),
        }
        .into_response();
        assert_eq!(tenant.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(system.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
