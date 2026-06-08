//! API-key auth: local panel token for self-hosted brokers.

use crate::routes::ApiError;
use crate::AppState;
use axum::{
    extract::{Request, State},
    http::header::AUTHORIZATION,
    middleware::Next,
    response::Response,
};
#[cfg(feature = "cloud")]
use axum::{http::StatusCode, response::IntoResponse, Json};
#[cfg(feature = "cloud")]
use http::header::CONTENT_LENGTH;
use std::sync::Arc;

pub async fn require_api_key(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    #[cfg(feature = "cloud")]
    if let Some(validator) = &state.auth {
        let mut req = req;
        use broker_control_plane::AuthError;
        let token = bearer_token(&req)
            .ok_or(AuthError::Missing)
            .map_err(auth_to_api)?;
        let ctx = validator
            .validate_bearer(token)
            .await
            .map_err(auth_to_api)?;
        req.extensions_mut().insert(crate::metering::IngestAuth {
            tenant_id: ctx.tenant_id,
        });
        req.extensions_mut().insert(ctx);
        return Ok(next.run(req).await);
    }

    if let Some(local) = &state.local_auth {
        if !local.is_configured() {
            return Err(ApiError::BadRequest(
                "local auth not configured — open /panel/ to set a password and API token".into(),
            ));
        }
        let token = bearer_token(&req)
            .ok_or_else(|| ApiError::BadRequest("missing Authorization: Bearer token".into()))?;
        if !local.verify_token(token).map_err(local_auth_to_api)? {
            return Err(ApiError::BadRequest("invalid API token".into()));
        }
    }

    Ok(next.run(req).await)
}

fn bearer_token(req: &Request) -> Option<&str> {
    req.headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
}

#[cfg(feature = "cloud")]
fn content_length_hint(req: &Request) -> u64 {
    req.headers()
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

fn local_auth_to_api(e: broker_local_auth::LocalAuthError) -> ApiError {
    ApiError::BadRequest(e.to_string())
}

#[cfg(feature = "cloud")]
fn auth_to_api(e: broker_control_plane::AuthError) -> ApiError {
    match e {
        broker_control_plane::AuthError::Missing | broker_control_plane::AuthError::Invalid => {
            ApiError::BadRequest(e.to_string())
        }
        broker_control_plane::AuthError::Db(err) => {
            ApiError::Broker(broker_partition::BrokerError::Storage(
                broker_storage::LogError::Io(std::io::Error::other(err.to_string())),
            ))
        }
    }
}

/// Load plan limits, enforce message size + bandwidth caps before body parse (cloud).
#[cfg(not(feature = "cloud"))]
pub async fn check_ingest_limits(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let _ = &state;
    Ok(next.run(req).await)
}

#[cfg(feature = "cloud")]
pub async fn check_ingest_limits(
    State(state): State<Arc<AppState>>,
    mut req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let Some(cp) = state.control_plane.as_ref() else {
        return Ok(next.run(req).await);
    };
    let Some(ctx) = req
        .extensions()
        .get::<broker_control_plane::TenantContext>()
        .cloned()
    else {
        return Ok(next.run(req).await);
    };

    let plan = broker_control_plane::load_plan(cp, ctx.plan_id)
        .await
        .map_err(|e| {
            ApiError::Broker(broker_partition::BrokerError::Storage(
                broker_storage::LogError::Io(std::io::Error::other(e.to_string())),
            ))
        })?;
    req.extensions_mut().insert(plan.clone());

    let body_hint = content_length_hint(&req);
    if body_hint > plan.max_message_bytes {
        return Ok((
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(serde_json::json!({
                "error": format!(
                    "message exceeds plan limit of {} bytes",
                    plan.max_message_bytes
                )
            })),
        )
            .into_response());
    }

    broker_control_plane::check_bandwidth_cap(
        cp,
        ctx.tenant_id,
        plan.bandwidth_bytes_month as i64,
        body_hint as i64,
    )
    .await
    .map_err(crate::metering::usage_error_to_api)?;

    // Single-message ingest routes; batch checks count in `batch_enqueue`.
    if !req.uri().path().ends_with("/batch") && !req.uri().path().ends_with("/gateway/enqueue") {
        broker_control_plane::check_messages_cap(cp, ctx.tenant_id, plan.messages_per_month, 1)
            .await
            .map_err(crate::metering::usage_error_to_api)?;
    }

    Ok(next.run(req).await)
}
