//! Versioned `/admin/v1` management API shared by embedded and standalone panel.

use crate::auth;
use crate::ops::{self, MetricsResponse, ReadyResponse};
use crate::AppState;
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use broker_config::stable_node_id;
use broker_raft_meta::{replication_policy, ControllerCommand, DataNodeRecord};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

const ADMIN_API_VERSION: &str = "v1";

#[derive(Clone)]
pub struct AdminState {
    pub local: Option<Arc<AppState>>,
    pub remote_controller: Option<String>,
    pub cell_registry: Arc<parking_lot::RwLock<CellRegistry>>,
    pub cluster_secret: Option<String>,
    pub registry_path: Option<PathBuf>,
}

impl AdminState {
    pub fn local(state: Arc<AppState>) -> Self {
        Self::local_with_registry(state, None, None)
    }

    pub fn local_with_registry(
        state: Arc<AppState>,
        registry_path: Option<PathBuf>,
        controller_url: Option<String>,
    ) -> Self {
        let mut registry = registry_path
            .as_ref()
            .map(|path| CellRegistry::load(path))
            .unwrap_or_default();
        if let Some(url) = controller_url.as_deref() {
            ensure_local_cell(&mut registry, &state, url);
        }
        if let Some(path) = registry_path.as_ref() {
            let _ = registry.save(path);
        }
        Self {
            local: Some(state),
            remote_controller: None,
            cell_registry: Arc::new(parking_lot::RwLock::new(registry)),
            cluster_secret: std::env::var("BETTERMQ_CLUSTER_SECRET").ok(),
            registry_path,
        }
    }

    pub fn remote(controller: String, registry: CellRegistry) -> Self {
        Self::remote_with_path(controller, registry, None)
    }

    pub fn remote_with_path(
        controller: String,
        registry: CellRegistry,
        registry_path: Option<PathBuf>,
    ) -> Self {
        if let Some(path) = registry_path.as_ref() {
            let _ = registry.save(path);
        }
        Self {
            local: None,
            remote_controller: Some(controller),
            cell_registry: Arc::new(parking_lot::RwLock::new(registry)),
            cluster_secret: std::env::var("BETTERMQ_CLUSTER_SECRET").ok(),
            registry_path,
        }
    }

    fn persist_registry(&self, registry: &CellRegistry) {
        if let Some(path) = &self.registry_path {
            if let Err(error) = registry.save(path) {
                tracing::warn!(%error, "cell registry save failed");
            }
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CellRegistry {
    pub cells: Vec<CellRecord>,
    #[serde(default)]
    pub members: Vec<CellMember>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CellRecord {
    pub id: String,
    pub region: String,
    pub controller_url: String,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CellMember {
    pub profile: String,
    pub reach_url: String,
    pub advertise_url: String,
    pub name: String,
    pub cell_id: String,
}

impl CellRegistry {
    pub fn load(path: &std::path::Path) -> Self {
        std::fs::read(path)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default()
    }

    pub fn save(&self, path: &std::path::Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, serde_json::to_vec_pretty(self)?)
    }

    pub fn upsert_member(&mut self, member: CellMember) {
        if let Some(existing) = self.members.iter_mut().find(|m| {
            m.cell_id == member.cell_id
                && (m.name == member.name
                    || m.reach_url == member.reach_url
                    || m.advertise_url == member.advertise_url)
        }) {
            *existing = member;
        } else {
            self.members.push(member);
        }
    }
}

fn ensure_local_cell(registry: &mut CellRegistry, local: &AppState, controller_url: &str) {
    let id = local
        .cluster
        .as_ref()
        .map(|c| c.runtime.config().cluster_id.to_string())
        .unwrap_or_else(|| "local".into());
    if registry
        .cells
        .iter()
        .any(|c| c.id == id || c.id == "local" || c.controller_url == controller_url)
    {
        return;
    }
    registry.cells.push(CellRecord {
        id,
        region: std::env::var("BETTERMQ_CELL_REGION").unwrap_or_else(|_| "local".into()),
        controller_url: controller_url.trim_end_matches('/').to_string(),
        label: Some(std::env::var("BETTERMQ_CELL_LABEL").unwrap_or_else(|_| "local".into())),
    });
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminErrorBody {
    pub error: String,
    pub code: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterOverview {
    pub api_version: &'static str,
    pub ready: bool,
    pub cluster_healthy: bool,
    pub node_count: usize,
    pub replication_factor: u32,
    pub min_isr: u32,
    pub catalog_epoch: u64,
    pub local: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PartialResult<T> {
    pub ok: Vec<CellOk<T>>,
    pub failed: Vec<CellFailure>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CellOk<T> {
    pub cell: String,
    pub data: T,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CellFailure {
    pub cell: String,
    pub error: String,
}

#[derive(Debug, Deserialize)]
pub struct AuditQuery {
    #[serde(default)]
    #[allow(dead_code)]
    pub limit: Option<usize>,
}

pub fn router(state: AdminState) -> Router {
    Router::new()
        .route("/admin/v1/health", get(admin_health))
        .route("/admin/v1/cluster", get(cluster_overview))
        .route("/admin/v1/nodes", get(list_nodes))
        .route("/admin/v1/shards", get(list_shards))
        .route("/admin/v1/catalog", get(catalog_snapshot))
        .route("/admin/v1/metrics", get(admin_metrics))
        .route("/admin/v1/readyz", get(admin_readyz))
        .route("/admin/v1/audit", get(audit_log))
        .route("/admin/v1/cells", get(list_cells).post(upsert_cell))
        .route("/admin/v1/cells/{id}/members", get(list_cell_members))
        .route("/admin/v1/cells/query/{kind}", get(federated_query))
        .route("/admin/v1/attach", post(attach_node))
        .route("/admin/v1/probe", post(probe_node))
        .route("/admin/v1/expand-rf", post(expand_rf))
        .route("/admin/v1/rebalance", post(start_rebalance))
        .route("/admin/v1/nodes/drain", post(drain_node))
        .route("/admin/v1/controller/voters", post(controller_voter))
        .route("/admin/v1/gc/{shard}", post(advance_gc))
        .with_state(state)
}

async fn require_admin(
    headers: &HeaderMap,
    state: &AdminState,
) -> Result<(), (StatusCode, Json<AdminErrorBody>)> {
    if let Some(local) = &state.local {
        if auth::insecure_no_auth() {
            return Ok(());
        }
        if local.uses_cloud_auth() {
            return Ok(());
        }
        if let Some(store) = &local.local_auth {
            if !store.is_configured() {
                return Ok(());
            }
            let token = bearer(headers);
            if token.is_some_and(|t| store.authorize_admin(t)) {
                return Ok(());
            }
            return Err(deny("admin authentication required", "unauthorized"));
        }
        return Ok(());
    }
    if auth::insecure_no_auth() {
        return Ok(());
    }
    let expected = state.cluster_secret.as_deref().unwrap_or("");
    if expected.is_empty() {
        return Ok(());
    }
    let token = bearer(headers).unwrap_or("");
    if subtle_eq(token, expected) {
        return Ok(());
    }
    Err(deny("admin authentication required", "unauthorized"))
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

fn subtle_eq(a: &str, b: &str) -> bool {
    use subtle::ConstantTimeEq;
    a.as_bytes().ct_eq(b.as_bytes()).into()
}

fn deny(error: &str, code: &'static str) -> (StatusCode, Json<AdminErrorBody>) {
    (
        StatusCode::UNAUTHORIZED,
        Json(AdminErrorBody {
            error: error.into(),
            code,
        }),
    )
}

async fn admin_health(State(state): State<AdminState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "apiVersion": ADMIN_API_VERSION,
        "mode": if state.local.is_some() { "embedded" } else { "standalone" },
        "cells": state.cell_registry.read().cells.len(),
    }))
}

async fn cluster_overview(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Json<ClusterOverview>, (StatusCode, Json<AdminErrorBody>)> {
    require_admin(&headers, &state).await?;
    if let Some(local) = &state.local {
        let ready = ops::ready_snapshot(local);
        let (node_count, catalog_epoch, rf, min_isr) = match &local.cluster {
            Some(cluster) => {
                let cfg = cluster.runtime.config();
                let snapshot = cluster.runtime.controller().map(|c| c.state());
                (
                    cfg.node_count(),
                    snapshot.as_ref().map(|s| s.catalog_epoch).unwrap_or(0),
                    snapshot
                        .as_ref()
                        .map(|s| s.replication_factor)
                        .filter(|v| *v > 0)
                        .unwrap_or_else(|| replication_policy(cfg.node_count()).0),
                    snapshot
                        .as_ref()
                        .map(|s| s.min_isr)
                        .filter(|v| *v > 0)
                        .unwrap_or_else(|| replication_policy(cfg.node_count()).1),
                )
            }
            None => (1, 0, 1, 1),
        };
        return Ok(Json(ClusterOverview {
            api_version: ADMIN_API_VERSION,
            ready: ready.ready,
            cluster_healthy: ready.cluster_healthy,
            node_count,
            replication_factor: rf,
            min_isr,
            catalog_epoch,
            local: true,
        }));
    }
    Ok(Json(ClusterOverview {
        api_version: ADMIN_API_VERSION,
        ready: true,
        cluster_healthy: true,
        node_count: 0,
        replication_factor: 3,
        min_isr: 2,
        catalog_epoch: 0,
        local: false,
    }))
}

async fn list_nodes(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    require_admin(&headers, &state).await?;
    if let Some(local) = &state.local {
        return Ok(Json(nodes_from_local(local)));
    }
    Ok(Json(serde_json::json!({ "nodes": [] })))
}

async fn list_shards(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    require_admin(&headers, &state).await?;
    if let Some(local) = &state.local {
        return Ok(Json(shards_from_local(local)));
    }
    Ok(Json(serde_json::json!({ "shards": [] })))
}

async fn catalog_snapshot(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    require_admin(&headers, &state).await?;
    if let Some(local) = &state.local {
        if let Some(cluster) = &local.cluster {
            if let Some(controller) = cluster.runtime.controller() {
                let snap = controller.state();
                return Ok(Json(serde_json::json!({
                    "epoch": snap.catalog_epoch,
                    "records": snap.catalog_records,
                    "authoritative": true,
                })));
            }
        }
        return Ok(Json(serde_json::json!({
            "epoch": 0,
            "records": {},
            "authoritative": false,
        })));
    }
    Ok(Json(serde_json::json!({
        "epoch": 0,
        "records": {},
        "authoritative": false,
    })))
}

async fn admin_metrics(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Json<MetricsResponse>, (StatusCode, Json<AdminErrorBody>)> {
    require_admin(&headers, &state).await?;
    if let Some(local) = &state.local {
        return Ok(Json(ops::metrics_snapshot(local)));
    }
    Ok(Json(MetricsResponse {
        blocked_hosts: 0,
        memory_critical: false,
        cluster_enabled: false,
        healthy_peers: 0,
        ingest_accepted: 0,
        ingest_duplicate: 0,
        ingest_rejected: 0,
        durable_commits: 0,
        rss_mb: None,
        memory_limit_mb: None,
        memory_percent: None,
        cpu_percent: None,
    }))
}

async fn admin_readyz(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Json<ReadyResponse>, (StatusCode, Json<AdminErrorBody>)> {
    require_admin(&headers, &state).await?;
    if let Some(local) = &state.local {
        return Ok(Json(ops::ready_snapshot(local)));
    }
    Ok(Json(ReadyResponse {
        ready: true,
        cluster_healthy: true,
        auth_configured: true,
    }))
}

async fn audit_log(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Query(_q): Query<AuditQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    require_admin(&headers, &state).await?;
    Ok(Json(serde_json::json!({ "events": [] })))
}

async fn list_cells(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Json<CellRegistry>, (StatusCode, Json<AdminErrorBody>)> {
    require_admin(&headers, &state).await?;
    Ok(Json(state.cell_registry.read().clone()))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpsertCell {
    id: String,
    region: String,
    controller_url: String,
    label: Option<String>,
}

async fn upsert_cell(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(body): Json<UpsertCell>,
) -> Result<Json<CellRegistry>, (StatusCode, Json<AdminErrorBody>)> {
    require_admin(&headers, &state).await?;
    let mut registry = state.cell_registry.write();
    if let Some(existing) = registry.cells.iter_mut().find(|c| c.id == body.id) {
        existing.region = body.region;
        existing.controller_url = body.controller_url;
        existing.label = body.label;
    } else {
        registry.cells.push(CellRecord {
            id: body.id,
            region: body.region,
            controller_url: body.controller_url,
            label: body.label,
        });
    }
    state.persist_registry(&registry);
    Ok(Json(registry.clone()))
}

async fn federated_query(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Path(kind): Path<String>,
) -> Result<Json<PartialResult<serde_json::Value>>, (StatusCode, Json<AdminErrorBody>)> {
    require_admin(&headers, &state).await?;
    let cells = state.cell_registry.read().cells.clone();
    if cells.is_empty() {
        if let Some(local) = &state.local {
            return Ok(Json(PartialResult {
                ok: vec![CellOk {
                    cell: "local".into(),
                    data: local_kind_data(local, &kind),
                }],
                failed: vec![],
            }));
        }
    }
    let client = reqwest::Client::new();
    let mut ok = Vec::new();
    let mut failed = Vec::new();
    let auth_token = bearer(&headers).map(str::to_string).or_else(|| {
        state
            .cluster_secret
            .clone()
            .filter(|secret| !secret.is_empty())
    });
    for cell in cells {
        if let Some(local) = &state.local {
            if cell.id == "local"
                || local
                    .cluster
                    .as_ref()
                    .is_some_and(|c| c.runtime.config().cluster_id.to_string() == cell.id)
            {
                ok.push(CellOk {
                    cell: cell.id,
                    data: local_kind_data(local, &kind),
                });
                continue;
            }
        }
        let url = format!(
            "{}/admin/v1/{}",
            cell.controller_url.trim_end_matches('/'),
            match kind.as_str() {
                "shards" => "shards",
                "nodes" => "nodes",
                "catalog" => "catalog",
                _ => "cluster",
            }
        );
        let mut req = client.get(url);
        if let Some(token) = auth_token.as_deref() {
            req = req.bearer_auth(token);
        }
        match req.send().await {
            Ok(resp) if resp.status().is_success() => match resp.json().await {
                Ok(data) => ok.push(CellOk {
                    cell: cell.id,
                    data,
                }),
                Err(err) => failed.push(CellFailure {
                    cell: cell.id,
                    error: err.to_string(),
                }),
            },
            Ok(resp) => failed.push(CellFailure {
                cell: cell.id,
                error: format!("status {}", resp.status()),
            }),
            Err(err) => failed.push(CellFailure {
                cell: cell.id,
                error: err.to_string(),
            }),
        }
    }
    Ok(Json(PartialResult { ok, failed }))
}

fn cluster_from_local(local: &AppState) -> serde_json::Value {
    let (node_count, rf, min_isr) = match &local.cluster {
        Some(cluster) => {
            let n = cluster.runtime.config().node_count();
            let snap = cluster.runtime.controller().map(|c| c.state());
            (
                n,
                snap.as_ref()
                    .map(|s| s.replication_factor)
                    .filter(|v| *v > 0)
                    .unwrap_or_else(|| replication_policy(n).0),
                snap.as_ref()
                    .map(|s| s.min_isr)
                    .filter(|v| *v > 0)
                    .unwrap_or_else(|| replication_policy(n).1),
            )
        }
        None => (1, 1, 1),
    };
    serde_json::json!({
        "nodeCount": node_count,
        "replicationFactor": rf,
        "minIsr": min_isr,
        "ready": ops::ready_snapshot(local).ready,
    })
}

fn nodes_from_local(local: &AppState) -> serde_json::Value {
    let Some(cluster) = &local.cluster else {
        return serde_json::json!({ "nodes": [] });
    };
    let cfg = cluster.runtime.config();
    let now = chrono::Utc::now().timestamp_millis();
    let nodes: Vec<_> = cfg
        .nodes
        .iter()
        .map(|n| {
            let voter = cluster
                .runtime
                .controller()
                .map(|c| c.state().controller_voters.contains(&n.id))
                .unwrap_or(false);
            serde_json::json!({
                "id": n.id,
                "addr": n.addr,
                "alive": cluster.runtime.is_peer_alive(n.id, now),
                "self": n.id == cfg.node_id,
                "controllerVoter": voter,
            })
        })
        .collect();
    serde_json::json!({ "nodes": nodes })
}

fn shards_from_local(local: &AppState) -> serde_json::Value {
    let Some(controller) = local
        .cluster
        .as_ref()
        .and_then(|cluster| cluster.runtime.controller())
    else {
        return serde_json::json!({ "shards": [] });
    };
    let shards: Vec<_> = controller
        .state()
        .shards
        .values()
        .map(|p| {
            serde_json::json!({
                "shard": p.shard,
                "leaderId": p.leader_id,
                "leaderTerm": p.leader_term,
                "replicas": p.replicas,
                "learners": p.learners,
                "isr": p.isr,
            })
        })
        .collect();
    serde_json::json!({ "shards": shards })
}

fn catalog_from_local(local: &AppState) -> serde_json::Value {
    if let Some(controller) = local
        .cluster
        .as_ref()
        .and_then(|cluster| cluster.runtime.controller())
    {
        let snap = controller.state();
        return serde_json::json!({
            "epoch": snap.catalog_epoch,
            "records": snap.catalog_records,
            "authoritative": true,
        });
    }
    serde_json::json!({
        "epoch": 0,
        "records": {},
        "authoritative": false,
    })
}

fn local_kind_data(local: &AppState, kind: &str) -> serde_json::Value {
    match kind {
        "nodes" => nodes_from_local(local),
        "shards" => shards_from_local(local),
        "catalog" => catalog_from_local(local),
        _ => cluster_from_local(local),
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RebalanceRequest {
    shard: u32,
    add: Option<Uuid>,
    remove: Option<Uuid>,
}

async fn start_rebalance(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(body): Json<RebalanceRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    require_admin(&headers, &state).await?;
    if let Some(local) = &state.local {
        if let Some(cluster) = &local.cluster {
            if let Some(controller) = cluster.runtime.controller() {
                let cmd = broker_raft_meta::ControllerCommand::BeginRebalance {
                    shard: body.shard,
                    add: body.add,
                    remove: body.remove,
                };
                match controller.submit(cmd).await {
                    Ok(snap) => {
                        return Ok(Json(serde_json::json!({
                            "ok": true,
                            "shard": snap.shards.get(&body.shard),
                        })))
                    }
                    Err(err) => {
                        return Err((
                            StatusCode::CONFLICT,
                            Json(AdminErrorBody {
                                error: err.to_string(),
                                code: "rebalance_rejected",
                            }),
                        ))
                    }
                }
            }
        }
    }
    Err((
        StatusCode::BAD_REQUEST,
        Json(AdminErrorBody {
            error: "rebalance requires a local controller".into(),
            code: "no_controller",
        }),
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GcRequest {
    watermark: u64,
}

async fn advance_gc(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Path(shard): Path<u32>,
    Json(body): Json<GcRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    require_admin(&headers, &state).await?;
    if let Some(local) = &state.local {
        if let Some(cluster) = &local.cluster {
            if let Some(controller) = cluster.runtime.controller() {
                let _ = controller
                    .submit(broker_raft_meta::ControllerCommand::SetGcWatermark {
                        shard,
                        watermark: body.watermark,
                    })
                    .await
                    .map_err(|err| {
                        (
                            StatusCode::CONFLICT,
                            Json(AdminErrorBody {
                                error: err.to_string(),
                                code: "gc_rejected",
                            }),
                        )
                    })?;
            }
        }
        let deleted = local
            .broker
            .gc_sealed_below(shard, body.watermark)
            .unwrap_or(0);
        return Ok(Json(serde_json::json!({
            "shard": shard,
            "watermark": body.watermark,
            "deletedSegments": deleted,
        })));
    }
    Ok(Json(serde_json::json!({
        "shard": shard,
        "watermark": body.watermark,
        "deletedSegments": 0,
    })))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DrainNodeRequest {
    node_id: Uuid,
}

async fn drain_node(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(body): Json<DrainNodeRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    require_admin(&headers, &state).await?;
    let Some(local) = &state.local else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(AdminErrorBody {
                error: "drain requires a local controller".into(),
                code: "no_controller",
            }),
        ));
    };
    let Some(cluster) = &local.cluster else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(AdminErrorBody {
                error: "cluster mode not enabled".into(),
                code: "no_cluster",
            }),
        ));
    };
    let Some(controller) = cluster.runtime.controller() else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(AdminErrorBody {
                error: "drain requires a local controller".into(),
                code: "no_controller",
            }),
        ));
    };
    match controller.drain_data_node(body.node_id).await {
        Ok(report) => Ok(Json(serde_json::json!({
            "ok": true,
            "nodeId": report.node_id,
            "shards": report.shards,
            "leadershipTransfers": report.leadership_transfers,
        }))),
        Err(err) => Err((
            StatusCode::CONFLICT,
            Json(AdminErrorBody {
                error: err.to_string(),
                code: "drain_rejected",
            }),
        )),
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ControllerVoterRequest {
    node_id: Uuid,
    action: String,
}

async fn controller_voter(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(body): Json<ControllerVoterRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    require_admin(&headers, &state).await?;
    let Some(local) = &state.local else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(AdminErrorBody {
                error: "controller voter changes require a local controller".into(),
                code: "no_controller",
            }),
        ));
    };
    let Some(controller) = local
        .cluster
        .as_ref()
        .and_then(|cluster| cluster.runtime.controller())
    else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(AdminErrorBody {
                error: "controller voter changes require a local controller".into(),
                code: "no_controller",
            }),
        ));
    };
    let command = match body.action.as_str() {
        "add" => broker_raft_meta::ControllerCommand::AddControllerVoter {
            node_id: body.node_id,
        },
        "remove" => broker_raft_meta::ControllerCommand::RemoveControllerVoter {
            node_id: body.node_id,
        },
        other => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(AdminErrorBody {
                    error: format!("action must be add or remove, got {other}"),
                    code: "bad_action",
                }),
            ))
        }
    };
    match controller.submit(command).await {
        Ok(snap) => Ok(Json(serde_json::json!({
            "ok": true,
            "controllerVoters": snap.controller_voters,
        }))),
        Err(err) => Err((
            StatusCode::CONFLICT,
            Json(AdminErrorBody {
                error: err.to_string(),
                code: "voter_rejected",
            }),
        )),
    }
}

const DATA_PROFILES: &[&str] = &["all", "broker"];
const FLEET_PROFILES: &[&str] = &["controller", "dispatch", "gateway", "panel"];

fn is_data_profile(profile: &str) -> bool {
    DATA_PROFILES.contains(&profile)
}

fn is_known_profile(profile: &str) -> bool {
    is_data_profile(profile) || FLEET_PROFILES.contains(&profile)
}

pub fn validate_attach_url(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim().trim_end_matches('/');
    let parsed = reqwest::Url::parse(trimmed).map_err(|e| format!("invalid url: {e}"))?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err("url must be http or https".into());
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err("url must not contain credentials".into());
    }
    if let Some(host) = parsed.host_str() {
        if let Ok(ip) = host.parse::<std::net::IpAddr>() {
            match ip {
                std::net::IpAddr::V4(v4) if v4.is_unspecified() || v4.is_link_local() => {
                    return Err("url host is not allowed".into());
                }
                std::net::IpAddr::V6(v6)
                    if v6.is_unspecified() || (v6.segments()[0] & 0xffc0) == 0xfe80 =>
                {
                    return Err("url host is not allowed".into());
                }
                _ => {}
            }
        }
    } else {
        return Err("url host is required".into());
    }
    Ok(trimmed.to_string())
}

async fn list_cell_members(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    require_admin(&headers, &state).await?;
    let registry = state.cell_registry.read().clone();
    let members: Vec<_> = registry
        .members
        .into_iter()
        .filter(|m| m.cell_id == id)
        .collect();
    let brokers = if let Some(local) = &state.local {
        nodes_from_local(local)
    } else {
        serde_json::json!({ "nodes": [] })
    };
    Ok(Json(serde_json::json!({
        "cellId": id,
        "brokers": brokers.get("nodes").cloned().unwrap_or(serde_json::json!([])),
        "members": members,
    })))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProbeRequest {
    reach_url: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AttachRequest {
    profile: String,
    reach_url: String,
    advertise_url: String,
    node_name: String,
    #[serde(default)]
    join_token: Option<String>,
    #[serde(default)]
    seed_url: Option<String>,
    #[serde(default)]
    cell_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExpandRfRequest {
    #[serde(default)]
    node_name: Option<String>,
    #[serde(default)]
    advertise_url: Option<String>,
}

async fn probe_node(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(body): Json<ProbeRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    require_admin(&headers, &state).await?;
    let reach = validate_attach_url(&body.reach_url).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            Json(AdminErrorBody {
                error,
                code: "bad_url",
            }),
        )
    })?;
    Ok(Json(probe_reach(&reach).await))
}

async fn probe_reach(reach: &str) -> serde_json::Value {
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
    {
        Ok(c) => c,
        Err(err) => {
            return serde_json::json!({
                "ok": false,
                "reachUrl": reach,
                "error": err.to_string(),
                "code": "client",
            })
        }
    };
    match client.get(format!("{reach}/healthz")).send().await {
        Ok(resp) if resp.status().is_success() => {
            let ready = client
                .get(format!("{reach}/readyz"))
                .send()
                .await
                .ok()
                .and_then(|r| r.status().is_success().then_some(true))
                .unwrap_or(false);
            let admin = match client.get(format!("{reach}/admin/v1/health")).send().await {
                Ok(resp) if resp.status().is_success() => {
                    resp.json::<serde_json::Value>().await.ok()
                }
                _ => None,
            };
            let gateway = client
                .get(format!("{reach}/v1/gateway/status"))
                .send()
                .await
                .ok()
                .and_then(|r| r.status().is_success().then_some(true))
                .unwrap_or(false);
            let profile_hint = if gateway {
                "gateway"
            } else if admin
                .as_ref()
                .and_then(|v| v.get("mode"))
                .and_then(|v| v.as_str())
                == Some("standalone")
            {
                "panel"
            } else {
                "broker"
            };
            serde_json::json!({
                "ok": true,
                "reachUrl": reach,
                "ready": ready,
                "profileHint": profile_hint,
                "admin": admin,
            })
        }
        Ok(resp) => serde_json::json!({
            "ok": false,
            "reachUrl": reach,
            "error": format!("HTTP {}", resp.status()),
            "code": if resp.status() == StatusCode::NOT_FOUND { "not_found" } else { "http" },
        }),
        Err(err) => {
            let code = if err.is_connect() {
                "connection_refused"
            } else if err.is_timeout() {
                "timeout"
            } else if err.to_string().contains("dns") {
                "dns"
            } else {
                "network"
            };
            serde_json::json!({
                "ok": false,
                "reachUrl": reach,
                "error": err.to_string(),
                "code": code,
            })
        }
    }
}

async fn attach_node(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(body): Json<AttachRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    require_admin(&headers, &state).await?;
    let profile = body.profile.trim().to_ascii_lowercase();
    if !is_known_profile(&profile) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(AdminErrorBody {
                error: format!("unknown profile '{profile}'"),
                code: "bad_profile",
            }),
        ));
    }
    let reach = validate_attach_url(&body.reach_url).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            Json(AdminErrorBody {
                error,
                code: "bad_url",
            }),
        )
    })?;
    let advertise = validate_attach_url(&body.advertise_url).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            Json(AdminErrorBody {
                error,
                code: "bad_url",
            }),
        )
    })?;
    let name = body.node_name.trim();
    if name.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(AdminErrorBody {
                error: "node name is required".into(),
                code: "bad_name",
            }),
        ));
    }
    let probe = probe_reach(&reach).await;
    if probe.get("ok") != Some(&serde_json::Value::Bool(true)) {
        let error = probe
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("target not reachable")
            .to_string();
        let code = probe
            .get("code")
            .and_then(|v| v.as_str())
            .unwrap_or("unreachable");
        return Err((
            StatusCode::BAD_GATEWAY,
            Json(AdminErrorBody {
                error,
                code: match code {
                    "connection_refused" => "connection_refused",
                    "timeout" => "timeout",
                    "dns" => "dns",
                    "not_found" => "not_found",
                    "http" => "http",
                    _ => "unreachable",
                },
            }),
        ));
    }

    let cell_id = body
        .cell_id
        .clone()
        .or_else(|| {
            state.local.as_ref().and_then(|local| {
                local
                    .cluster
                    .as_ref()
                    .map(|c| c.runtime.config().cluster_id.to_string())
            })
        })
        .unwrap_or_else(|| "local".into());

    if is_data_profile(&profile) {
        let token = body.join_token.as_deref().unwrap_or("").trim();
        if token.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(AdminErrorBody {
                    error: "join token is required for all/broker".into(),
                    code: "join_token_required",
                }),
            ));
        }
        let seed = body
            .seed_url
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.trim_end_matches('/').to_string())
            .or_else(|| {
                state.local.as_ref().and_then(|local| {
                    local.cluster.as_ref().and_then(|c| {
                        c.runtime
                            .config()
                            .nodes
                            .first()
                            .map(|n| n.addr.trim_end_matches('/').to_string())
                    })
                })
            })
            .ok_or_else(|| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(AdminErrorBody {
                        error: "seed url is required".into(),
                        code: "seed_required",
                    }),
                )
            })?;
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .map_err(|err| {
                (
                    StatusCode::BAD_GATEWAY,
                    Json(AdminErrorBody {
                        error: err.to_string(),
                        code: "client",
                    }),
                )
            })?;
        let enroll_url = format!("{reach}/v1/infra/cluster/enroll");
        let resp = client
            .post(&enroll_url)
            .json(&serde_json::json!({
                "seed_url": seed,
                "join_token": token,
                "public_url": advertise,
                "node_name": name,
            }))
            .send()
            .await
            .map_err(|err| {
                let code = if err.is_connect() {
                    "connection_refused"
                } else {
                    "network"
                };
                (
                    StatusCode::BAD_GATEWAY,
                    Json(AdminErrorBody {
                        error: format!("enroll failed: {err}"),
                        code,
                    }),
                )
            })?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            let code = if status == StatusCode::NOT_FOUND {
                "not_found"
            } else if status == StatusCode::UNAUTHORIZED {
                "unauthorized"
            } else {
                "enroll_rejected"
            };
            return Err((
                StatusCode::BAD_GATEWAY,
                Json(AdminErrorBody {
                    error: if body.is_empty() {
                        format!("enroll rejected: HTTP {status}")
                    } else {
                        body
                    },
                    code,
                }),
            ));
        }
        let enroll: serde_json::Value = resp.json().await.unwrap_or_default();
        let expand = if let Some(local) = &state.local {
            match expand_replication(local, name, &advertise).await {
                Ok(v) => v,
                Err(err) => serde_json::json!({ "ok": false, "error": err }),
            }
        } else {
            serde_json::json!({ "ok": false, "error": "expand requires a local controller" })
        };
        return Ok(Json(serde_json::json!({
            "ok": true,
            "profile": profile,
            "joined": true,
            "enroll": enroll,
            "expand": expand,
            "probe": probe,
        })));
    }

    let member = CellMember {
        profile: profile.clone(),
        reach_url: reach,
        advertise_url: advertise,
        name: name.to_string(),
        cell_id,
    };
    {
        let mut registry = state.cell_registry.write();
        registry.upsert_member(member.clone());
        state.persist_registry(&registry);
    }
    Ok(Json(serde_json::json!({
        "ok": true,
        "profile": profile,
        "joined": false,
        "registered": true,
        "member": member,
        "probe": probe,
        "warning": if profile == "controller" {
            Some("controller Raft voter membership is not automated; this node is registered for health only")
        } else {
            None
        },
    })))
}

async fn expand_rf(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(body): Json<ExpandRfRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<AdminErrorBody>)> {
    require_admin(&headers, &state).await?;
    let Some(local) = &state.local else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(AdminErrorBody {
                error: "expand requires a local controller".into(),
                code: "no_controller",
            }),
        ));
    };
    let name = body.node_name.as_deref().unwrap_or("");
    let addr = body.advertise_url.as_deref().unwrap_or("");
    expand_replication(local, name, addr)
        .await
        .map(Json)
        .map_err(|error| {
            (
                StatusCode::CONFLICT,
                Json(AdminErrorBody {
                    error,
                    code: "expand_rejected",
                }),
            )
        })
}

async fn expand_replication(
    local: &AppState,
    node_name: &str,
    advertise_url: &str,
) -> Result<serde_json::Value, String> {
    let Some(controller) = local
        .cluster
        .as_ref()
        .and_then(|cluster| cluster.runtime.controller())
    else {
        return Err("no local controller".into());
    };
    let addr = if advertise_url.is_empty() {
        None
    } else {
        Some(validate_attach_url(advertise_url)?)
    };
    if !node_name.is_empty() {
        if let Some(addr) = addr.clone() {
            let id = stable_node_id(node_name);
            controller
                .submit(ControllerCommand::RegisterDataNode {
                    node: DataNodeRecord {
                        id,
                        addr,
                        rack: None,
                        region: None,
                        broker: true,
                        controller_voter: false,
                    },
                })
                .await
                .map_err(|e| e.to_string())?;
        }
    }
    let snap = controller.state();
    let data_count = snap.data_nodes.values().filter(|n| n.broker).count();
    let mut actions = Vec::new();
    if data_count >= 3 && snap.replication_factor < 3 {
        controller
            .submit(ControllerCommand::SetReplicationPolicy {
                factor: 3,
                min_isr: 2,
            })
            .await
            .map_err(|e| e.to_string())?;
        actions.push("set_rf_3".to_string());
    }
    let snap = controller.state();
    let factor = snap.replication_factor.max(1) as usize;
    if data_count >= 3 && factor >= 3 {
        if let Some(new_id) = (!node_name.is_empty()).then(|| stable_node_id(node_name)) {
            let shards: Vec<u32> = snap.shards.keys().copied().collect();
            for shard in shards {
                let Some(placement) = controller.state().shards.get(&shard).cloned() else {
                    continue;
                };
                if placement.replicas.len() >= factor {
                    continue;
                }
                if placement.replicas.contains(&new_id) {
                    continue;
                }
                if !placement.learners.contains(&new_id) {
                    controller
                        .submit(ControllerCommand::BeginRebalance {
                            shard,
                            add: Some(new_id),
                            remove: None,
                        })
                        .await
                        .map_err(|e| e.to_string())?;
                }
                controller
                    .submit(ControllerCommand::PromoteLearner {
                        shard,
                        node_id: new_id,
                    })
                    .await
                    .map_err(|e| e.to_string())?;
                controller
                    .submit(ControllerCommand::CompleteRebalance { shard })
                    .await
                    .map_err(|e| e.to_string())?;
                actions.push(format!("shard_{shard}_add"));
            }
        }
    }
    let snap = controller.state();
    let ha_ready = snap.replication_factor >= 3
        && snap
            .shards
            .values()
            .all(|s| s.isr.len() >= snap.min_isr.max(2) as usize);
    Ok(serde_json::json!({
        "ok": true,
        "dataNodes": data_count,
        "replicationFactor": snap.replication_factor,
        "minIsr": snap.min_isr,
        "haReady": ha_ready,
        "actions": actions,
        "warning": if data_count == 2 {
            Some("RF=2 / minISR=2 — both nodes must be up. Add a third broker for HA.")
        } else if data_count >= 4 && snap.replication_factor == 3 {
            Some("RF stays 3. Extra brokers add shard capacity, not a fourth copy.")
        } else if data_count >= 3 && !ha_ready {
            Some("Expanding replicas to RF=3…")
        } else {
            None
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cell_registry_survives_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cell-registry.json");
        let mut registry = CellRegistry::default();
        registry.cells.push(CellRecord {
            id: "us-east".into(),
            region: "us-east".into(),
            controller_url: "http://broker1:8080".into(),
            label: Some("prod".into()),
        });
        registry.upsert_member(CellMember {
            profile: "dispatch".into(),
            reach_url: "http://dispatch1:8080".into(),
            advertise_url: "http://dispatch1:8080".into(),
            name: "dispatch1".into(),
            cell_id: "us-east".into(),
        });
        registry.save(&path).unwrap();
        let loaded = CellRegistry::load(&path);
        assert_eq!(loaded.cells.len(), 1);
        assert_eq!(loaded.cells[0].id, "us-east");
        assert_eq!(loaded.members.len(), 1);
        assert_eq!(loaded.members[0].profile, "dispatch");
    }

    #[test]
    fn attach_url_allows_loopback_and_rfc1918() {
        assert!(validate_attach_url("http://127.0.0.1:8082").is_ok());
        assert!(validate_attach_url("http://broker2:8080").is_ok());
        assert!(validate_attach_url("http://10.0.0.5:8090").is_ok());
        assert!(validate_attach_url("http://169.254.169.254/").is_err());
        assert!(validate_attach_url("http://user:pass@broker2:8080").is_err());
        assert!(validate_attach_url("ftp://broker2:8080").is_err());
    }

    #[test]
    fn fleet_profiles_are_not_data_nodes() {
        assert!(is_data_profile("all"));
        assert!(is_data_profile("broker"));
        assert!(!is_data_profile("dispatch"));
        assert!(!is_data_profile("gateway"));
        assert!(!is_data_profile("panel"));
        assert!(!is_data_profile("controller"));
    }
}
