//! Ops endpoints: readiness, metrics, blocked destinations (CP6b.4 / CP6a).

use crate::routes::ApiError;
use crate::AppState;
use axum::{extract::State, http::StatusCode, Json};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Serialize)]
pub struct ReadyResponse {
    pub ready: bool,
    pub cluster_healthy: bool,
    pub auth_configured: bool,
}

#[derive(Debug, Serialize)]
pub struct MetricsResponse {
    pub blocked_hosts: usize,
    pub memory_critical: bool,
    pub cluster_enabled: bool,
    pub healthy_peers: usize,
}

#[derive(Debug, Serialize)]
pub struct BlockedHostEntry {
    pub host: String,
    pub remaining_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct BlockedHostsResponse {
    pub hosts: Vec<BlockedHostEntry>,
}

#[derive(Debug, Deserialize)]
pub struct UnblockHostRequest {
    pub host: String,
}

pub async fn readyz(State(state): State<Arc<AppState>>) -> (StatusCode, Json<ReadyResponse>) {
    let auth_configured =
        state.local_auth.as_ref().is_some_and(|a| a.is_configured()) || state.uses_cloud_auth();
    let cluster_healthy = match &state.cluster {
        None => true,
        Some(c) => {
            let now = Utc::now().timestamp_millis();
            let cfg = c.runtime.config();
            let alive = cfg
                .nodes
                .iter()
                .filter(|n| c.runtime.is_peer_alive(n.id, now))
                .count();
            alive >= cfg.quorum_size()
        }
    };
    let ready = auth_configured && cluster_healthy;
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(ReadyResponse {
            ready,
            cluster_healthy,
            auth_configured,
        }),
    )
}

pub async fn metrics(State(state): State<Arc<AppState>>) -> Json<MetricsResponse> {
    let blocked = state.dispatch.host_blocker().blocked_hosts();
    let cluster_enabled = state.cluster.is_some();
    let healthy_peers = state
        .cluster
        .as_ref()
        .map(|c| {
            let now = Utc::now().timestamp_millis();
            c.runtime
                .config()
                .nodes
                .iter()
                .filter(|n| c.runtime.is_peer_alive(n.id, now))
                .count()
        })
        .unwrap_or(1);
    Json(MetricsResponse {
        blocked_hosts: blocked.len(),
        memory_critical: false,
        cluster_enabled,
        healthy_peers,
    })
}

pub async fn list_blocked_hosts(State(state): State<Arc<AppState>>) -> Json<BlockedHostsResponse> {
    let hosts = state
        .dispatch
        .host_blocker()
        .blocked_hosts()
        .into_iter()
        .map(|(host, remaining_ms)| BlockedHostEntry { host, remaining_ms })
        .collect();
    Json(BlockedHostsResponse { hosts })
}

pub async fn unblock_host(
    State(state): State<Arc<AppState>>,
    Json(body): Json<UnblockHostRequest>,
) -> Result<StatusCode, ApiError> {
    if body.host.trim().is_empty() {
        return Err(ApiError::BadRequest("host required".into()));
    }
    state.dispatch.host_blocker().unblock(body.host.trim());
    Ok(StatusCode::OK)
}
