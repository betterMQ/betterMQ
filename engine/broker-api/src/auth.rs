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
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

fn token_cache() -> &'static Mutex<HashMap<String, (Instant, bool)>> {
    static CACHE: OnceLock<Mutex<HashMap<String, (Instant, bool)>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn cached_local_token(
    local: &broker_local_auth::LocalAuthStore,
    token: &str,
) -> Result<bool, ApiError> {
    let now = Instant::now();
    {
        let cache = token_cache().lock();
        if let Some((until, ok)) = cache.get(token) {
            if *until > now {
                return Ok(*ok);
            }
        }
    }
    let ok = local.verify_token(token).map_err(local_auth_to_api)?;
    token_cache()
        .lock()
        .insert(token.to_string(), (now + Duration::from_secs(5), ok));
    Ok(ok)
}

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
        req.extensions_mut().insert(ctx.clone());
        // Scope all broker catalog/publish ops to this tenant for the request.
        let tenant = ctx.tenant_id.to_string();
        return Ok(broker_partition::scope_tenant(tenant, next.run(req)).await);
    }

    if insecure_no_auth() {
        return Ok(next.run(req).await);
    }

    if let Some(local) = &state.local_auth {
        if !local.is_configured() {
            return Err(ApiError::Unauthorized(
                "local auth not configured — open /panel/ to set a password and API token".into(),
            ));
        }
        let token = bearer_token(&req)
            .ok_or_else(|| ApiError::Unauthorized("missing Authorization: Bearer token".into()))?;
        if !cached_local_token(local, token)? {
            return Err(ApiError::Unauthorized("invalid API token".into()));
        }
        return Ok(next.run(req).await);
    }

    if state.uses_cloud_auth() {
        return Ok(next.run(req).await);
    }

    Err(ApiError::Unauthorized(
        "authentication required (set BETTERMQ_INSECURE_NO_AUTH=1 only for local development)"
            .into(),
    ))
}

pub(crate) fn insecure_no_auth() -> bool {
    matches!(
        std::env::var("BETTERMQ_INSECURE_NO_AUTH")
            .ok()
            .as_deref()
            .map(str::trim),
        Some("1") | Some("true") | Some("TRUE") | Some("yes")
    )
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
            ApiError::Unauthorized(e.to_string())
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
