//! Standalone broker: panel-driven password + one-time API token.

use crate::routes::ApiError;
use crate::AppState;
use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use broker_local_auth::LocalAuthError;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/v1/auth/config", get(auth_config))
        .route("/v1/local-auth/status", get(local_status))
        .route("/v1/local-auth/setup", post(local_setup))
        .route("/v1/local-auth/regenerate", post(local_regenerate))
}

#[derive(Serialize)]
pub struct AuthConfigResponse {
    /// `local` (panel password + API token) or `control_plane` (external API keys).
    pub mode: &'static str,
    pub configured: bool,
}

#[derive(Serialize)]
struct StatusResponse {
    configured: bool,
}

#[derive(Deserialize)]
struct PasswordBody {
    password: String,
}

#[derive(Serialize)]
struct TokenResponse {
    token: String,
    #[serde(rename = "show_once")]
    show_once: bool,
}

async fn auth_config(State(state): State<Arc<AppState>>) -> Json<AuthConfigResponse> {
    if state.uses_cloud_auth() {
        return Json(AuthConfigResponse {
            mode: "control_plane",
            configured: true,
        });
    }
    let configured = state
        .local_auth
        .as_ref()
        .map(|s| s.is_configured())
        .unwrap_or(false);
    Json(AuthConfigResponse {
        mode: "local",
        configured,
    })
}

async fn local_status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<StatusResponse>, ApiError> {
    let store = local_store(&state)?;
    Ok(Json(StatusResponse {
        configured: store.is_configured(),
    }))
}

async fn propagate_local_auth(state: &AppState) {
    let Some(local) = &state.local_auth else {
        return;
    };
    if crate::cluster::catalog_peer_urls(state).is_empty() {
        return;
    }
    if let Ok(Some(creds)) = local.export_credentials() {
        crate::cluster::replicate_auth_credentials(state, creds).await;
    }
}

async fn local_setup(
    State(state): State<Arc<AppState>>,
    Json(body): Json<PasswordBody>,
) -> Result<Json<TokenResponse>, ApiError> {
    let store = local_store(&state)?;
    let token = store.setup(&body.password).map_err(map_local_err)?;
    propagate_local_auth(&state).await;
    Ok(Json(TokenResponse {
        token,
        show_once: true,
    }))
}

async fn local_regenerate(
    State(state): State<Arc<AppState>>,
    Json(body): Json<PasswordBody>,
) -> Result<Json<TokenResponse>, ApiError> {
    let store = local_store(&state)?;
    let token = store.regenerate(&body.password).map_err(map_local_err)?;
    propagate_local_auth(&state).await;
    Ok(Json(TokenResponse {
        token,
        show_once: true,
    }))
}

fn local_store(state: &AppState) -> Result<&Arc<broker_local_auth::LocalAuthStore>, ApiError> {
    if state.uses_cloud_auth() {
        return Err(ApiError::BadRequest(
            "local auth endpoints are disabled when control plane is enabled".into(),
        ));
    }
    state
        .local_auth
        .as_ref()
        .ok_or_else(|| ApiError::BadRequest("local auth not available".into()))
}

fn map_local_err(e: LocalAuthError) -> ApiError {
    match e {
        LocalAuthError::AlreadyConfigured => {
            ApiError::BadRequest("local auth already configured".into())
        }
        LocalAuthError::NotConfigured => ApiError::BadRequest("local auth not configured".into()),
        LocalAuthError::InvalidPassword => ApiError::BadRequest("invalid password".into()),
        LocalAuthError::InvalidToken => ApiError::BadRequest("invalid API token".into()),
        LocalAuthError::Io(err) => ApiError::Broker(broker_partition::BrokerError::Storage(
            broker_storage::LogError::Io(err),
        )),
        LocalAuthError::Json(err) => ApiError::Broker(broker_partition::BrokerError::Storage(
            broker_storage::LogError::Io(std::io::Error::other(err.to_string())),
        )),
        LocalAuthError::Hash => ApiError::BadRequest("password processing failed".into()),
    }
}
