//! Publish with destination in the URL path.

use crate::bettermq::to_enqueue_response;
use crate::http_fields::OutboundHttpFields;
use crate::routes::{publish, ApiError};
use crate::AppState;
use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use broker_partition::PublishRequest;
use percent_encoding::percent_decode_str;
use std::sync::Arc;

/// `GET /v1/publish/{*destination}` — no body; destination URL in path.
#[derive(Debug, serde::Deserialize)]
pub struct PublishPathQuery {
    pub secret: String,
    #[serde(default)]
    pub key: String,
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub delay: Option<u64>,
    #[serde(default)]
    pub priority: Option<u8>,
    #[serde(default)]
    pub flow_id: Option<uuid::Uuid>,
    #[serde(default)]
    pub max_retries: Option<u32>,
    #[serde(flatten)]
    pub outbound: OutboundHttpFields,
}

pub fn decode_destination_path(raw: &str) -> Result<String, ApiError> {
    let decoded = percent_decode_str(raw).decode_utf8().map_err(|e| {
        ApiError::Broker(broker_partition::BrokerError::Storage(
            broker_storage::LogError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                e.to_string(),
            )),
        ))
    })?;
    let url = decoded.trim();
    if url.starts_with("http://") || url.starts_with("https://") {
        Ok(url.to_string())
    } else {
        Err(ApiError::Broker(broker_partition::BrokerError::Storage(
            broker_storage::LogError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "destination must start with http:// or https://",
            )),
        )))
    }
}

pub async fn publish_get(
    State(state): State<Arc<AppState>>,
    ingest: Option<axum::extract::Extension<crate::metering::IngestAuth>>,
    #[cfg(feature = "cloud")] plan: Option<
        axum::extract::Extension<broker_control_plane::PlanLimits>,
    >,
    Path(destination): Path<String>,
    Query(q): Query<PublishPathQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let url = decode_destination_path(&destination)?;
    let mut req = PublishRequest {
        topic: broker_partition::DIRECT_TOPIC.to_string(),
        queue_id: None,
        group_id: None,
        group_member_id: None,
        routing_key: q.key,
        payload: String::new(),
        payload_encoding: None,
        idempotency_key: q.idempotency_key,
        delay_ms: q.delay,
        priority: q.priority,
        flow_id: q.flow_id,
        url: Some(url),
        secret: Some(q.secret),
        destination: None,
        flow: None,
        parallelism: None,
        max_retries: q.max_retries,
        retry_backoff: None,
        method: None,
        headers: None,
        sign: None,
        request: None,
    };
    q.outbound.apply_to(&mut req);
    if req.method.is_none() {
        req.method = Some("GET".to_string());
    }
    #[cfg(feature = "cloud")]
    let (status, Json(inner)) = publish(State(state), ingest, plan, Json(req)).await?;
    #[cfg(not(feature = "cloud"))]
    let (status, Json(inner)) = publish(State(state), ingest, Json(req)).await?;
    Ok((status, Json(to_enqueue_response(inner))))
}
