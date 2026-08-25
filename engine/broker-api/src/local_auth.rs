//! Standalone broker: panel-driven password + one-time API token.

use crate::rate_limit::{client_ip_key, RateLimiter};
use crate::routes::ApiError;
use crate::AppState;
use axum::{
    extract::State,
    http::HeaderMap,
    routing::{get, post},
    Json, Router,
};
use broker_local_auth::LocalAuthError;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime};

fn auth_rate_limiter() -> &'static RateLimiter {
    static LIM: OnceLock<RateLimiter> = OnceLock::new();
    LIM.get_or_init(|| RateLimiter::new(5, Duration::from_secs(60)))
}

/// First-boot claim window. `None` means locked (unless env overrides).
static SETUP_WINDOW_UNTIL: Mutex<Option<SystemTime>> = Mutex::new(None);

/// Open panel password setup until `now + duration` (no token). Restarting the
/// process starts a new window. Used instead of writing a secret into the data dir.
pub fn open_setup_window(duration: Duration) {
    *SETUP_WINDOW_UNTIL.lock().unwrap() = Some(SystemTime::now() + duration);
}

/// Lock setup unless `BETTERMQ_ALLOW_OPEN_SETUP` is set.
pub fn close_setup_window() {
    *SETUP_WINDOW_UNTIL.lock().unwrap() = None;
}

fn env_flag(name: &str) -> bool {
    matches!(
        std::env::var(name).ok().as_deref().map(str::trim),
        Some("1") | Some("true") | Some("TRUE") | Some("yes")
    )
}

struct SetupAccess {
    open: bool,
    closes_in_secs: Option<u64>,
}

fn setup_access() -> SetupAccess {
    if env_flag("BETTERMQ_ALLOW_OPEN_SETUP") {
        return SetupAccess {
            open: true,
            closes_in_secs: None,
        };
    }
    let until = SETUP_WINDOW_UNTIL
        .lock()
        .unwrap()
        .filter(|t| SystemTime::now() <= *t);
    match until {
        Some(t) => SetupAccess {
            open: true,
            closes_in_secs: Some(
                t.duration_since(SystemTime::now())
                    .unwrap_or(Duration::ZERO)
                    .as_secs(),
            ),
        },
        None => SetupAccess {
            open: false,
            closes_in_secs: None,
        },
    }
}

fn setup_unlocked() -> Result<(), ApiError> {
    if setup_access().open {
        return Ok(());
    }
    Err(ApiError::Unauthorized(
        "setup window closed — restart BetterMQ, then set a password".into(),
    ))
}

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
    /// First-boot password can be set (15-minute window after start).
    pub setup_open: bool,
    /// Seconds left on the first-boot window. Omitted when not applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub setup_closes_in_secs: Option<u64>,
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

fn auth_config_body(mode: &'static str, configured: bool) -> AuthConfigResponse {
    if configured {
        return AuthConfigResponse {
            mode,
            configured: true,
            setup_open: false,
            setup_closes_in_secs: None,
        };
    }
    let access = setup_access();
    AuthConfigResponse {
        mode,
        configured: false,
        setup_open: access.open,
        setup_closes_in_secs: access.closes_in_secs,
    }
}

async fn auth_config(State(state): State<Arc<AppState>>) -> Json<AuthConfigResponse> {
    if state.uses_cloud_auth() {
        return Json(auth_config_body("control_plane", true));
    }
    let configured = state
        .local_auth
        .as_ref()
        .map(|s| s.is_configured())
        .unwrap_or(false);
    Json(auth_config_body("local", configured))
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

fn enforce_auth_rate(headers: &HeaderMap) -> Result<(), ApiError> {
    let key = client_ip_key(headers, None);
    if !auth_rate_limiter().check(&format!("local-auth:{key}")) {
        return Err(ApiError::BadRequest(
            "too many auth attempts — try again later".into(),
        ));
    }
    Ok(())
}

async fn local_setup(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<PasswordBody>,
) -> Result<Json<TokenResponse>, ApiError> {
    enforce_auth_rate(&headers)?;
    setup_unlocked()?;
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
    headers: HeaderMap,
    Json(body): Json<PasswordBody>,
) -> Result<Json<TokenResponse>, ApiError> {
    enforce_auth_rate(&headers)?;
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
