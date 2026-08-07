//! Ops endpoints: readiness, metrics, blocked destinations (CP6b.4 / CP6a).

use crate::routes::ApiError;
use crate::AppState;
use axum::{extract::State, http::StatusCode, Json};
use broker_dispatch::sample_process_resources;
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rss_mb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_limit_mb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_percent: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_percent: Option<f32>,
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

#[derive(Debug, Deserialize)]
pub struct BlockHostRequest {
    pub host: String,
    /// Block duration in milliseconds (default: 30 minutes).
    #[serde(default)]
    pub duration_ms: Option<u64>,
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

pub async fn metrics(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<MetricsResponse>, StatusCode> {
    if let Some(expected) = std::env::var("BETTERMQ_METRICS_TOKEN")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        let ok = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "))
            .is_some_and(|got| got == expected)
            || headers
                .get("x-bettermq-metrics-token")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|got| got == expected);
        if !ok {
            return Err(StatusCode::UNAUTHORIZED);
        }
    }
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
    let guard = state.dispatch.memory_guard();
    let resources = sample_process_resources();
    let memory_limit_mb = guard.limit_mb();
    let memory_percent = match (resources.rss_mb, memory_limit_mb) {
        (Some(rss), Some(limit)) if limit > 0 => Some(((rss * 100) / limit).min(100) as u8),
        _ => None,
    };
    Ok(Json(MetricsResponse {
        blocked_hosts: blocked.len(),
        memory_critical: guard.is_critical(),
        cluster_enabled,
        healthy_peers,
        rss_mb: resources.rss_mb,
        memory_limit_mb,
        memory_percent,
        cpu_percent: resources.cpu_percent,
    }))
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

pub async fn block_host(
    State(state): State<Arc<AppState>>,
    Json(body): Json<BlockHostRequest>,
) -> Result<Json<BlockedHostEntry>, ApiError> {
    if body.host.trim().is_empty() {
        return Err(ApiError::BadRequest("host required".into()));
    }
    let duration_ms = body.duration_ms.unwrap_or(30 * 60 * 1000);
    let key = state
        .dispatch
        .host_blocker()
        .block_manual(body.host.trim(), duration_ms);
    Ok(Json(BlockedHostEntry {
        host: key,
        remaining_ms: duration_ms,
    }))
}

/// Aggregate DLQ/delayed/metrics/fleet across local + peer brokers (Phase D).
pub async fn aggregate_ops_status(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let local_leases = state.leases.active_count();
    let blocked = state.dispatch.host_blocker().blocked_hosts().len();
    let delayed = state.schedule.list().len();
    let mut peers = Vec::new();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
        .ok();
    if let Some(http) = client {
        for (_id, addr) in crate::cluster::catalog_peer_targets(&state) {
            let lease_url = format!("{addr}/internal/v1/lease/status");
            let mut lease_req = http.get(&lease_url);
            if let Ok(secret) = std::env::var("BETTERMQ_CLUSTER_SECRET") {
                if !secret.trim().is_empty() {
                    lease_req = lease_req.header("x-bettermq-cluster-secret", secret);
                }
            }
            let peer_leases = match lease_req.send().await {
                Ok(r) if r.status().is_success() => r
                    .json::<serde_json::Value>()
                    .await
                    .ok()
                    .and_then(|v| v.get("active_leases").and_then(|n| n.as_u64()))
                    .unwrap_or(0),
                _ => 0,
            };
            peers.push(serde_json::json!({
                "addr": addr,
                "active_leases": peer_leases,
            }));
        }
    }
    Json(serde_json::json!({
        "local": {
            "active_leases": local_leases,
            "blocked_hosts": blocked,
            "delayed": delayed,
            "broker_only": state.broker_only,
            "dispatch_fleet": state.dispatch_fleet,
        },
        "peers": peers,
        "fleet": {
            "active_leases_total": local_leases
                + peers
                    .iter()
                    .filter_map(|p| p.get("active_leases").and_then(|n| n.as_u64()))
                    .sum::<u64>() as usize,
        }
    }))
}
