//! CP7b cluster: shard leadership, replication quorum, status API, internal endpoints.

use crate::routes::ApiError;
use crate::AppState;
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Response,
    Json,
};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use broker_config::{load_managed_config, stable_node_id};
use broker_partition::{
    is_dlq_topic, DispatchGroup, FlowProfile, GroupMember, PublishRequest, PublishResponse,
    Subscription,
};
use broker_raft_meta::ClusterRuntime;
use broker_replication::{
    CatchUpFrame, ReplicaProgress, ReplicateAppendRequest, ReplicateBatchAck,
    ReplicateBatchRequest, ReplicateCatchUpRange, ReplicateCatchUpRequest, ReplicationClient,
};
use broker_schedule::CronJob;
use broker_storage::StorageMode;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;
use uuid::Uuid;

#[derive(Debug, Serialize)]
pub struct ClusterStatusResponse {
    pub enabled: bool,
    pub cluster_id: Option<Uuid>,
    pub generation: Option<u64>,
    pub this_node_id: Option<Uuid>,
    pub node_count: usize,
    pub healthy_count: usize,
    pub scheduler_leader_id: Option<Uuid>,
    pub this_node_scheduler_leader: bool,
    pub controller_term: Option<u64>,
    pub controller_quorum_lease: bool,
    pub nodes: Vec<ClusterNodeStatus>,
    pub shards: Vec<ShardLeaderStatus>,
    pub replicas: Vec<ReplicaProgress>,
}

#[derive(Debug, Serialize)]
pub struct ClusterNodeStatus {
    pub id: Uuid,
    pub name: String,
    pub addr: String,
    pub healthy: bool,
    pub is_self: bool,
    pub led_shards: Vec<u32>,
    pub preferred_shards: Vec<u32>,
}

#[derive(Debug, Serialize)]
pub struct ShardLeaderStatus {
    pub shard: u32,
    pub leader_id: Uuid,
    pub leader_addr: Option<String>,
    pub preferred_leader_id: Uuid,
    pub failover: bool,
    pub ready: bool,
}

pub fn build_cluster_status(
    cluster: Option<&ClusterHandle>,
    partition_count: u32,
) -> ClusterStatusResponse {
    let Some(cluster) = cluster else {
        return ClusterStatusResponse {
            enabled: false,
            cluster_id: None,
            generation: None,
            this_node_id: None,
            node_count: 1,
            healthy_count: 1,
            scheduler_leader_id: None,
            this_node_scheduler_leader: true,
            controller_term: None,
            controller_quorum_lease: true,
            nodes: vec![],
            shards: vec![],
            replicas: vec![],
        };
    };

    let rt = &cluster.runtime;
    let cfg = rt.config();
    let now = Utc::now().timestamp_millis();
    let partitions = partition_count.max(1);

    let mut nodes = Vec::new();
    let mut healthy_count = 0usize;
    for node in &cfg.nodes {
        let healthy = rt.is_peer_alive(node.id, now);
        if healthy {
            healthy_count += 1;
        }
        let mut led_shards = Vec::new();
        let mut preferred_shards = Vec::new();
        for shard in 0..partitions {
            if cfg.preferred_leader_for_shard(shard) == node.id {
                preferred_shards.push(shard);
            }
            if rt.elect_leader_for_shard(shard) == Some(node.id) {
                led_shards.push(shard);
            }
        }
        nodes.push(ClusterNodeStatus {
            id: node.id,
            name: node.addr.clone(),
            addr: node.addr.clone(),
            healthy,
            is_self: node.id == cfg.node_id,
            led_shards,
            preferred_shards,
        });
    }

    let mut shards = Vec::new();
    for shard in 0..partitions {
        let preferred = cfg.preferred_leader_for_shard(shard);
        let leader = rt.elect_leader_for_shard(shard).unwrap_or(preferred);
        shards.push(ShardLeaderStatus {
            shard,
            leader_id: leader,
            leader_addr: cfg.node_addr(leader),
            preferred_leader_id: preferred,
            failover: leader != preferred,
            ready: leader != cfg.node_id || rt.shard_ready(shard),
        });
    }

    let scheduler_leader_id = rt.scheduler_holder();
    ClusterStatusResponse {
        enabled: true,
        cluster_id: Some(cfg.cluster_id),
        generation: Some(cfg.generation),
        this_node_id: Some(cfg.node_id),
        node_count: cfg.node_count(),
        healthy_count,
        scheduler_leader_id,
        this_node_scheduler_leader: rt.is_scheduler_leader(),
        controller_term: Some(rt.controller_term()),
        controller_quorum_lease: rt.has_controller_lease(),
        nodes,
        shards,
        replicas: cluster.replication.replica_progress(),
    }
}

pub async fn get_cluster_status(State(state): State<Arc<AppState>>) -> Json<ClusterStatusResponse> {
    Json(build_cluster_status(
        state.cluster.as_ref(),
        state.broker.config().partitions,
    ))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterGossipRequest {
    pub observer: Uuid,
    pub seen: HashMap<Uuid, i64>,
    #[serde(default)]
    pub controller_vote: Option<broker_raft_meta::ControllerVoteRequest>,
    #[serde(default)]
    pub controller_leader: Option<broker_raft_meta::ControllerLeaderProof>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterGossipResponse {
    pub observer: Uuid,
    pub controller_term: u64,
    pub vote: Option<broker_raft_meta::ControllerVoteResponse>,
}

#[derive(Clone)]
pub struct ClusterHandle {
    pub runtime: ClusterRuntime,
    pub replication: ReplicationClient,
    http: reqwest::Client,
}

impl ClusterHandle {
    pub fn new(runtime: ClusterRuntime) -> Self {
        let replication =
            ReplicationClient::new(runtime.config().clone()).with_runtime(runtime.clone());
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("cluster HTTP client");
        Self {
            runtime,
            replication,
            http,
        }
    }
}

fn shard_for_request(state: &AppState, req: &PublishRequest) -> u32 {
    state.broker.assign_shard(&req.topic, &req.routing_key)
}

/// Enqueue push dispatch when this broker leads the message shard.
/// Callers must invoke this only after the shard commit high watermark covers `offset`.
pub fn enqueue_dispatch_after_publish(state: &AppState, resp: &PublishResponse) {
    if resp.duplicate {
        return;
    }
    let Some(partition) = resp.partition else {
        return;
    };
    let Some(offset) = resp.offset else {
        return;
    };
    let Some(message_id) = resp.message_id else {
        return;
    };
    if is_dlq_topic(&resp.topic) || !is_dispatch_leader(state, partition) {
        return;
    }
    state.dispatch.notify_committed_range(
        &resp.topic,
        partition,
        offset,
        offset.saturating_add(1),
        message_id,
    );
}

async fn replicate_if_needed(
    state: &Arc<AppState>,
    resp: &mut PublishResponse,
    local_durable: impl std::future::Future<Output = Result<u64, ApiError>>,
) -> Result<u64, ApiError> {
    if state.broker.config().storage == StorageMode::Slate {
        return local_durable.await;
    }
    let Some(cluster) = &state.cluster else {
        return local_durable.await;
    };
    if cluster.runtime.config().node_count() <= 1 {
        return local_durable.await;
    }
    let Some(partition) = resp.partition else {
        return local_durable.await;
    };
    let Some(offset) = resp.offset else {
        return local_durable.await;
    };
    let Some(frame) = resp.replication_frame.take() else {
        return local_durable.await;
    };
    let leader_generation = cluster.runtime.shard_generation(partition);
    let batch = ReplicateBatchRequest::new(
        state.broker.tenant(),
        resp.topic.clone(),
        partition,
        cluster.runtime.config().node_id,
        leader_generation,
        offset,
        vec![frame],
    )
    .map_err(|e| ApiError::ReplicationFailed(e.to_string()))?;
    match cluster
        .replication
        .replicate_batch_with_local(batch, async move {
            local_durable.await.map_err(|error| {
                broker_replication::ReplicateError::InvalidEpoch(error.to_string())
            })
        })
        .await
    {
        Ok(outcome) => Ok(outcome.committed_hwm),
        Err(e) => {
            warn!(error = %e, "replication durable quorum failed");
            Err(ApiError::ReplicationFailed(e.to_string()))
        }
    }
}

async fn publish_on_leader(
    state: &Arc<AppState>,
    req: PublishRequest,
    meter: Option<crate::metering::IngestMeter>,
) -> Result<PublishResponse, ApiError> {
    let body_bytes = meter
        .map(|m| m.body_bytes)
        .unwrap_or_else(|| req.payload.len() as u64);
    let idempotency_key = req.idempotency_key.clone();
    let mut resp = if state
        .cluster
        .as_ref()
        .is_some_and(|c| c.runtime.config().node_count() > 1)
        && state.broker.config().storage != StorageMode::Slate
    {
        state.broker.publish_defer_dedup(req)?
    } else {
        state.broker.publish(req)?
    };
    let topic = resp.topic.clone();
    let wait = {
        let broker = state.broker.clone();
        let partition = resp.partition;
        let offset = resp.offset;
        async move {
            if let (Some(partition), Some(offset)) = (partition, offset) {
                broker
                    .wait_committed(&topic, partition, offset)
                    .await
                    .map_err(ApiError::from)
            } else {
                Ok(0)
            }
        }
    };
    let epoch = replicate_if_needed(state, &mut resp, wait).await?;
    resp.commit_epoch = Some(epoch);
    if let (Some(key), false) = (idempotency_key.as_deref(), resp.duplicate) {
        if let (Some(message_id), Some(offset), Some(partition)) =
            (resp.message_id, resp.offset, resp.partition)
        {
            state.broker.commit_publish_dedup(
                key,
                broker_partition::DedupEntry {
                    message_id,
                    offset,
                    partition,
                },
            )?;
        }
    }
    if let Some(m) = meter {
        if !resp.duplicate {
            crate::metering::record_ingest(state, m.tenant_id, body_bytes).await;
        }
    }
    Ok(resp)
}

async fn forward_publish_to_leader(
    cluster: &ClusterHandle,
    shard: u32,
    req: &PublishRequest,
) -> Result<PublishResponse, ApiError> {
    let leader = cluster
        .runtime
        .leader_http_base_for_shard(shard)
        .ok_or_else(|| ApiError::BadRequest("no leader for shard".into()))?;
    let url = format!(
        "{}/internal/v1/cluster/publish",
        leader.trim_end_matches('/')
    );
    let resp = crate::cluster_auth::apply_cluster_secret(cluster.http.post(&url))
        .json(req)
        .send()
        .await
        .map_err(|e| ApiError::BadRequest(format!("forward to shard leader failed: {e}")))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(ApiError::BadRequest(format!(
            "shard leader returned {status}: {body}"
        )));
    }
    resp.json()
        .await
        .map_err(|e| ApiError::BadRequest(format!("invalid leader response: {e}")))
}

pub async fn publish_with_cluster(
    state: &Arc<AppState>,
    mut req: PublishRequest,
    meter: Option<crate::metering::IngestMeter>,
) -> Result<PublishResponse, ApiError> {
    if let Some(cluster) = &state.cluster {
        if cluster.runtime.config().node_count() > 1 {
            let shard = shard_for_request(state, &req);
            if !cluster.runtime.is_leader_for_shard(shard) {
                state
                    .broker
                    .prepare_for_cluster_forward(&mut req)
                    .map_err(ApiError::Broker)?;
                if req.flow.is_none() {
                    if let Some(flow_id) = req.flow_id {
                        return Err(ApiError::Broker(
                            broker_partition::BrokerError::FlowProfileNotFound(flow_id),
                        ));
                    }
                }
                if req.destination.is_none()
                    && req.url.is_none()
                    && (req.queue_id.is_some()
                        || (!req.topic.is_empty() && req.topic != broker_partition::DIRECT_TOPIC))
                {
                    return Err(ApiError::Broker(
                        broker_partition::BrokerError::QueueNotFound(
                            req.queue_id
                                .map(|id| id.to_string())
                                .unwrap_or_else(|| req.topic.clone()),
                        ),
                    ));
                }
                return forward_publish_to_leader(cluster, shard, &req).await;
            }
        }
    }
    publish_on_leader(state, req, meter).await
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogSnapshot {
    pub flows: Vec<FlowProfile>,
    pub queues: Vec<Subscription>,
    #[serde(default)]
    pub groups: Vec<DispatchGroup>,
    #[serde(default)]
    pub group_members: Vec<GroupMember>,
    #[serde(default)]
    pub crons: Vec<CronJob>,
    #[serde(default)]
    pub tombstones: HashMap<Uuid, i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogFlowUpsert {
    pub profile: FlowProfile,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogQueueUpsert {
    pub subscription: Subscription,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogFlowDelete {
    pub flow_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogQueueDelete {
    pub queue_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogGroupUpsert {
    pub group: DispatchGroup,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogGroupDelete {
    pub group_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogGroupMemberUpsert {
    pub member: GroupMember,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogGroupMemberDelete {
    pub member_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogCronUpsert {
    pub job: CronJob,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogCronDelete {
    pub cron_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterAuthSnapshot {
    pub credentials: broker_local_auth::AuthCredentials,
}

fn normalize_peer_url(url: &str) -> String {
    url.trim()
        .trim_end_matches('/')
        .replace("http://", "")
        .replace("https://", "")
        .to_lowercase()
}

fn loopback_host(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}

/// Base URLs to try when calling another broker. `http://localhost:PORT` from inside a
/// container reaches the caller, not the peer on the host — rewrite to host.docker.internal.
pub fn peer_fanout_urls(public_url: &str) -> Vec<String> {
    let base = public_url.trim().trim_end_matches('/');
    let scheme_end = base.find("://").map(|i| i + 3).unwrap_or(0);
    let authority = base[scheme_end..].split('/').next().unwrap_or("");
    let (host, port) = if let Some((h, p)) = authority.rsplit_once(':') {
        (h, p.parse::<u16>().ok())
    } else {
        (authority, None)
    };

    if !loopback_host(host) {
        return vec![base.to_string()];
    }

    let scheme = base.get(..scheme_end).unwrap_or("http://");
    let port = port.or_else(|| {
        if scheme.starts_with("https") {
            Some(443)
        } else {
            Some(80)
        }
    });
    let Some(port) = port else {
        return vec![base.to_string()];
    };

    let docker = format!("{scheme}host.docker.internal:{port}");
    if docker == base {
        vec![base.to_string()]
    } else {
        vec![docker, base.to_string()]
    }
}

/// Peer broker base URLs for catalog fan-out. Uses saved `bettermq.json` so joins and
/// registrations propagate before restart (in-memory cluster runtime can be stale).
pub fn catalog_peer_urls(state: &AppState) -> Vec<String> {
    let dir = state.broker.config().data_dir.clone();
    if let Ok(Some(cfg)) = load_managed_config(&dir) {
        if cfg.cluster_enabled() {
            if let Some(cluster) = &cfg.cluster {
                let self_norm = normalize_peer_url(&cfg.node.public_url);
                return cluster
                    .nodes
                    .iter()
                    .map(|n| n.public_url.trim().trim_end_matches('/').to_string())
                    .filter(|u| normalize_peer_url(u) != self_norm)
                    .collect();
            }
        }
    }
    if let Some(cluster) = &state.cluster {
        let self_id = cluster.runtime.config().node_id;
        return cluster
            .runtime
            .config()
            .nodes
            .iter()
            .filter(|n| n.id != self_id)
            .map(|n| n.addr.trim().trim_end_matches('/').to_string())
            .collect();
    }
    Vec::new()
}

/// `(node_id, public_url)` for every peer in the saved cluster config.
pub fn catalog_peer_targets(state: &AppState) -> Vec<(Uuid, String)> {
    let dir = state.broker.config().data_dir.clone();
    if let Ok(Some(cfg)) = load_managed_config(&dir) {
        if cfg.cluster_enabled() {
            if let Some(cluster) = &cfg.cluster {
                let self_norm = normalize_peer_url(&cfg.node.public_url);
                return cluster
                    .nodes
                    .iter()
                    .filter(|n| normalize_peer_url(&n.public_url) != self_norm)
                    .map(|n| {
                        (
                            stable_node_id(&n.name),
                            n.public_url.trim().trim_end_matches('/').to_string(),
                        )
                    })
                    .collect();
            }
        }
    }
    if let Some(cluster) = &state.cluster {
        let self_id = cluster.runtime.config().node_id;
        return cluster
            .runtime
            .config()
            .nodes
            .iter()
            .filter(|n| n.id != self_id)
            .map(|n| (n.id, n.addr.trim().trim_end_matches('/').to_string()))
            .collect();
    }
    Vec::new()
}

/// Union-merge catalog from all peers into this node (best-effort).
pub async fn sync_catalog_from_peers(state: &Arc<AppState>) {
    let peers = catalog_peer_urls(state);
    if peers.is_empty() {
        return;
    }
    match merge_catalog_from_peers(state, &peers).await {
        Ok(snap) => tracing::info!(
            flows = snap.flows.len(),
            queues = snap.queues.len(),
            crons = snap.crons.len(),
            "catalog merged from peers"
        ),
        Err(e) => warn!(error = ?e, "catalog merge from peers failed"),
    }
}

/// Pull catalog from peers after boot (covers creates that happened while this node was down).
pub fn spawn_cluster_catalog_sync(state: Arc<AppState>) -> Option<tokio::task::JoinHandle<()>> {
    if catalog_peer_urls(&state).is_empty() {
        return None;
    }
    Some(tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        sync_catalog_from_peers(&state).await;
    }))
}

/// Push local catalog to a peer that just came back online.
pub async fn push_catalog_to_recovered_peer(peer_base: &str, state: &AppState) -> bool {
    let snap = catalog_snapshot(state);
    if snap.flows.is_empty() && snap.queues.is_empty() && snap.crons.is_empty() {
        return true;
    }
    push_catalog_to_peer(peer_base, &snap).await
}

async fn propagate_catalog_to_peers<T: Serialize>(
    peer_bases: &[String],
    path: &str,
    body: &T,
) -> usize {
    if peer_bases.is_empty() {
        return 0;
    }
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "catalog fan-out: http client build failed");
            return 0;
        }
    };
    let mut notified = 0usize;
    for peer in peer_bases {
        let mut delivered = false;
        for base in peer_fanout_urls(peer) {
            let url = format!("{}{}", base.trim_end_matches('/'), path);
            match crate::cluster_auth::apply_cluster_secret(client.post(&url))
                .json(body)
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    notified += 1;
                    delivered = true;
                    break;
                }
                Ok(resp) => {
                    let status = resp.status();
                    let detail = resp.text().await.unwrap_or_default();
                    warn!(peer = %peer, base = %base, %status, body = %detail, "catalog fan-out rejected");
                }
                Err(e) => warn!(peer = %peer, base = %base, error = %e, "catalog fan-out failed"),
            }
        }
        if !delivered {
            warn!(peer = %peer, "catalog fan-out exhausted peer URLs");
        }
    }
    notified
}

fn persist_catalog_tombstone(
    state: &AppState,
    id: Uuid,
    kind: crate::catalog_tombstones::CatalogKind,
) {
    if let Err(e) = state.catalog_tombstones.record(id, kind) {
        warn!(error = %e, %id, "catalog tombstone persist failed");
    }
}

async fn fanout_catalog<T: Serialize>(state: &AppState, path: &str, body: &T, what: &'static str) {
    let peers = catalog_peer_urls(state);
    let notified = propagate_catalog_to_peers(&peers, path, body).await;
    if !peers.is_empty() && notified == 0 {
        warn!(
            peers = peers.len(),
            what, "catalog propagate did not reach any peer"
        );
    }
}

async fn advance_catalog_controller(state: &AppState) {
    if let Some(cluster) = &state.cluster {
        if let Err(error) = cluster.runtime.advance_catalog_epoch().await {
            warn!(%error, "catalog mutation rejected by OpenRaft controller");
        }
    }
}

async fn put_authoritative_catalog(
    state: &AppState,
    kind: &str,
    key: &str,
    payload: &impl serde::Serialize,
    tombstone: bool,
) {
    let Some(cluster) = &state.cluster else {
        return;
    };
    let payload_json = match serde_json::to_string(payload) {
        Ok(json) => json,
        Err(error) => {
            warn!(%error, kind, key, "catalog record serialize failed");
            return;
        }
    };
    if let Err(error) = cluster
        .runtime
        .put_catalog_record(kind.into(), key.into(), payload_json, tombstone)
        .await
    {
        warn!(%error, kind, key, "authoritative catalog write rejected");
    }
}

pub fn catalog_snapshot(state: &AppState) -> CatalogSnapshot {
    let members = state
        .broker
        .list_groups()
        .ok()
        .map(|groups| {
            groups
                .iter()
                .flat_map(|g| state.broker.list_group_members(g.id).unwrap_or_default())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    // Cluster catalog sync still needs secrets for followers to deliver webhooks.
    // Exposure is gated by BETTERMQ_CLUSTER_SECRET (fail-closed). Operators must
    // keep /internal/v1/* off the public internet.
    CatalogSnapshot {
        flows: state.broker.list_flow_profiles().unwrap_or_default(),
        queues: state.broker.list_queues().unwrap_or_default(),
        groups: state.broker.list_groups().unwrap_or_default(),
        group_members: members,
        crons: state.crons.list(),
        tombstones: state.catalog_tombstones.snapshot(),
    }
}

fn catalog_item_alive(id: Uuid, updated_at_ms: i64, tombstones: &HashMap<Uuid, i64>) -> bool {
    tombstones
        .get(&id)
        .map(|deleted_at| updated_at_ms > *deleted_at)
        .unwrap_or(true)
}

pub fn apply_catalog_snapshot(state: &AppState, snap: &CatalogSnapshot) -> Result<(), ApiError> {
    state
        .catalog_tombstones
        .merge_remote(&snap.tombstones)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let tombstones = state.catalog_tombstones.snapshot();
    for profile in &snap.flows {
        if !catalog_item_alive(profile.id, profile.updated_at_ms, &tombstones) {
            continue;
        }
        state
            .broker
            .upsert_flow_profile(profile.clone())
            .map_err(ApiError::Broker)?;
    }
    for sub in &snap.queues {
        if !catalog_item_alive(sub.id, sub.updated_at_ms, &tombstones) {
            continue;
        }
        crate::routes::validate_destination_url_str_pub(&sub.url)?;
        state
            .broker
            .upsert_subscription_catalog(sub.clone())
            .map_err(ApiError::Broker)?;
    }
    for group in &snap.groups {
        if !catalog_item_alive(group.id, group.updated_at_ms, &tombstones) {
            continue;
        }
        state
            .broker
            .upsert_group_catalog(group.clone())
            .map_err(ApiError::Broker)?;
    }
    for member in &snap.group_members {
        if !catalog_item_alive(member.id, member.updated_at_ms, &tombstones) {
            continue;
        }
        crate::routes::validate_destination_url_str_pub(&member.url)?;
        state
            .broker
            .upsert_group_member_catalog(member.clone())
            .map_err(ApiError::Broker)?;
    }
    for job in &snap.crons {
        if !catalog_item_alive(job.id, job.updated_at_ms, &tombstones) {
            continue;
        }
        if let Some(dest) = job.request.destination.as_ref() {
            crate::routes::validate_destination_url_str_pub(&dest.url)?;
        }
        state
            .crons
            .upsert(job.clone())
            .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    }
    Ok(())
}

pub async fn fetch_catalog_from_seed(seed_base: &str) -> Result<CatalogSnapshot, ApiError> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let mut last_err = String::from("no peer URL attempted");
    for base in peer_fanout_urls(seed_base) {
        let url = format!("{}/internal/v1/cluster/catalog", base.trim_end_matches('/'));
        let resp = match crate::cluster_auth::apply_cluster_secret(client.get(&url))
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                last_err = format!("cannot fetch catalog from {base}: {e}");
                continue;
            }
        };
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            last_err = format!("catalog at {base} returned {status}: {body}");
            continue;
        }
        return resp.json().await.map_err(|e| {
            ApiError::BadRequest(format!("invalid catalog snapshot from {base}: {e}"))
        });
    }
    Err(ApiError::BadRequest(last_err))
}

fn merge_flow_lww(target: &mut HashMap<Uuid, FlowProfile>, item: FlowProfile) {
    target
        .entry(item.id)
        .and_modify(|existing| {
            if item.updated_at_ms >= existing.updated_at_ms {
                *existing = item.clone();
            }
        })
        .or_insert(item);
}

fn merge_queue_lww(target: &mut HashMap<Uuid, Subscription>, item: Subscription) {
    target
        .entry(item.id)
        .and_modify(|existing| {
            if item.updated_at_ms >= existing.updated_at_ms {
                *existing = item.clone();
            }
        })
        .or_insert(item);
}

fn merge_cron_lww(target: &mut HashMap<Uuid, CronJob>, item: CronJob) {
    target
        .entry(item.id)
        .and_modify(|existing| {
            if item.updated_at_ms >= existing.updated_at_ms {
                *existing = item.clone();
            }
        })
        .or_insert(item);
}

fn merge_group_lww(target: &mut HashMap<Uuid, DispatchGroup>, item: DispatchGroup) {
    target
        .entry(item.id)
        .and_modify(|existing| {
            if item.updated_at_ms >= existing.updated_at_ms {
                *existing = item.clone();
            }
        })
        .or_insert(item);
}

fn merge_group_member_lww(target: &mut HashMap<Uuid, GroupMember>, item: GroupMember) {
    target
        .entry(item.id)
        .and_modify(|existing| {
            if item.updated_at_ms >= existing.updated_at_ms {
                *existing = item.clone();
            }
        })
        .or_insert(item);
}

fn merge_tombstones_lww(target: &mut HashMap<Uuid, i64>, remote: &HashMap<Uuid, i64>) {
    for (id, deleted_at) in remote {
        target
            .entry(*id)
            .and_modify(|existing| {
                if *deleted_at > *existing {
                    *existing = *deleted_at;
                }
            })
            .or_insert(*deleted_at);
    }
}

fn finalize_catalog_merge(
    flows: HashMap<Uuid, FlowProfile>,
    queues: HashMap<Uuid, Subscription>,
    groups: HashMap<Uuid, DispatchGroup>,
    group_members: HashMap<Uuid, GroupMember>,
    crons: HashMap<Uuid, CronJob>,
    tombstones: &HashMap<Uuid, i64>,
) -> CatalogSnapshot {
    CatalogSnapshot {
        flows: flows
            .into_values()
            .filter(|f| catalog_item_alive(f.id, f.updated_at_ms, tombstones))
            .collect(),
        queues: queues
            .into_values()
            .filter(|q| catalog_item_alive(q.id, q.updated_at_ms, tombstones))
            .collect(),
        groups: groups
            .into_values()
            .filter(|g| catalog_item_alive(g.id, g.updated_at_ms, tombstones))
            .collect(),
        group_members: group_members
            .into_values()
            .filter(|m| catalog_item_alive(m.id, m.updated_at_ms, tombstones))
            .collect(),
        crons: crons
            .into_values()
            .filter(|j| catalog_item_alive(j.id, j.updated_at_ms, tombstones))
            .collect(),
        tombstones: tombstones.clone(),
    }
}

/// Merge catalogs from local disk and every cluster peer (LWW + tombstones, CP6c).
pub async fn merge_catalog_from_peers(
    state: &Arc<AppState>,
    peer_bases: &[String],
) -> Result<CatalogSnapshot, ApiError> {
    let mut flows: HashMap<Uuid, FlowProfile> = HashMap::new();
    let mut queues: HashMap<Uuid, Subscription> = HashMap::new();
    let mut groups: HashMap<Uuid, DispatchGroup> = HashMap::new();
    let mut group_members: HashMap<Uuid, GroupMember> = HashMap::new();
    let mut crons: HashMap<Uuid, CronJob> = HashMap::new();
    let mut tombstones: HashMap<Uuid, i64> = state.catalog_tombstones.snapshot();
    let mut fetched = 0usize;

    let local = catalog_snapshot(state);
    merge_tombstones_lww(&mut tombstones, &local.tombstones);
    for f in local.flows {
        merge_flow_lww(&mut flows, f);
    }
    for q in local.queues {
        merge_queue_lww(&mut queues, q);
    }
    for g in local.groups {
        merge_group_lww(&mut groups, g);
    }
    for m in local.group_members {
        merge_group_member_lww(&mut group_members, m);
    }
    for job in local.crons {
        merge_cron_lww(&mut crons, job);
    }

    for peer in peer_bases {
        let base = peer.trim().trim_end_matches('/');
        if base.is_empty() {
            continue;
        }
        match fetch_catalog_from_seed(base).await {
            Ok(snap) => {
                fetched += 1;
                merge_tombstones_lww(&mut tombstones, &snap.tombstones);
                for f in snap.flows {
                    merge_flow_lww(&mut flows, f);
                }
                for q in snap.queues {
                    merge_queue_lww(&mut queues, q);
                }
                for g in snap.groups {
                    merge_group_lww(&mut groups, g);
                }
                for m in snap.group_members {
                    merge_group_member_lww(&mut group_members, m);
                }
                for job in snap.crons {
                    merge_cron_lww(&mut crons, job);
                }
            }
            Err(e) => {
                tracing::warn!(peer = %base, error = ?e, "catalog fetch from peer failed");
            }
        }
    }

    let merged = finalize_catalog_merge(flows, queues, groups, group_members, crons, &tombstones);

    if fetched == 0
        && merged.flows.is_empty()
        && merged.queues.is_empty()
        && merged.groups.is_empty()
        && merged.crons.is_empty()
    {
        return Err(ApiError::BadRequest(
            "could not fetch catalog from any cluster peer".into(),
        ));
    }

    apply_catalog_snapshot(state, &merged)?;
    tracing::info!(
        flows = merged.flows.len(),
        queues = merged.queues.len(),
        groups = merged.groups.len(),
        group_members = merged.group_members.len(),
        crons = merged.crons.len(),
        peers = fetched,
        "cluster catalog merged from peers"
    );
    Ok(merged)
}

pub async fn push_catalog_to_peer(peer_base: &str, snap: &CatalogSnapshot) -> bool {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
    {
        Ok(c) => c,
        Err(_) => return false,
    };
    for base in peer_fanout_urls(peer_base) {
        let url = format!(
            "{}/internal/v1/cluster/catalog/apply",
            base.trim_end_matches('/')
        );
        if crate::cluster_auth::apply_cluster_secret(client.post(&url))
            .json(snap)
            .send()
            .await
            .ok()
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            return true;
        }
    }
    false
}

pub async fn internal_catalog_snapshot(
    State(state): State<Arc<AppState>>,
) -> Json<CatalogSnapshot> {
    Json(catalog_snapshot(&state))
}

pub async fn internal_catalog_apply(
    State(state): State<Arc<AppState>>,
    Json(snap): Json<CatalogSnapshot>,
) -> Result<StatusCode, ApiError> {
    apply_catalog_snapshot(&state, &snap)?;
    tracing::info!(
        flows = snap.flows.len(),
        queues = snap.queues.len(),
        "cluster catalog applied"
    );
    Ok(StatusCode::OK)
}

pub async fn replicate_flow_catalog(state: &AppState, profile: FlowProfile) {
    advance_catalog_controller(state).await;
    put_authoritative_catalog(state, "flow", &profile.id.to_string(), &profile, false).await;
    let peers = catalog_peer_urls(state);
    let notified = propagate_catalog_to_peers(
        &peers,
        "/internal/v1/cluster/catalog/flow",
        &CatalogFlowUpsert { profile },
    )
    .await;
    if !peers.is_empty() && notified == 0 {
        warn!(
            peers = peers.len(),
            "flow catalog create did not reach any peer"
        );
    }
}

pub async fn replicate_queue_catalog(state: &AppState, subscription: Subscription) {
    advance_catalog_controller(state).await;
    put_authoritative_catalog(
        state,
        "queue",
        &subscription.id.to_string(),
        &subscription,
        false,
    )
    .await;
    let peers = catalog_peer_urls(state);
    let notified = propagate_catalog_to_peers(
        &peers,
        "/internal/v1/cluster/catalog/queue",
        &CatalogQueueUpsert { subscription },
    )
    .await;
    if !peers.is_empty() && notified == 0 {
        warn!(
            peers = peers.len(),
            "queue catalog create did not reach any peer"
        );
    }
}

pub async fn replicate_flow_delete(state: &AppState, flow_id: Uuid) {
    advance_catalog_controller(state).await;
    persist_catalog_tombstone(state, flow_id, crate::catalog_tombstones::CatalogKind::Flow);
    fanout_catalog(
        state,
        "/internal/v1/cluster/catalog/flow/delete",
        &CatalogFlowDelete { flow_id },
        "flow delete",
    )
    .await;
}

pub async fn replicate_queue_delete(state: &AppState, queue_id: Uuid) {
    advance_catalog_controller(state).await;
    persist_catalog_tombstone(
        state,
        queue_id,
        crate::catalog_tombstones::CatalogKind::Queue,
    );
    fanout_catalog(
        state,
        "/internal/v1/cluster/catalog/queue/delete",
        &CatalogQueueDelete { queue_id },
        "queue delete",
    )
    .await;
}

pub async fn replicate_group_catalog(state: &AppState, group: DispatchGroup) {
    advance_catalog_controller(state).await;
    let peers = catalog_peer_urls(state);
    let notified = propagate_catalog_to_peers(
        &peers,
        "/internal/v1/cluster/catalog/group",
        &CatalogGroupUpsert { group },
    )
    .await;
    if !peers.is_empty() && notified == 0 {
        warn!(
            peers = peers.len(),
            "group catalog create did not reach any peer"
        );
    }
}

pub async fn replicate_group_member_catalog(state: &AppState, member: GroupMember) {
    advance_catalog_controller(state).await;
    let peers = catalog_peer_urls(state);
    let notified = propagate_catalog_to_peers(
        &peers,
        "/internal/v1/cluster/catalog/group-member",
        &CatalogGroupMemberUpsert { member },
    )
    .await;
    if !peers.is_empty() && notified == 0 {
        warn!(
            peers = peers.len(),
            "group member catalog create did not reach any peer"
        );
    }
}

pub async fn replicate_group_delete(state: &AppState, group_id: Uuid) {
    advance_catalog_controller(state).await;
    persist_catalog_tombstone(
        state,
        group_id,
        crate::catalog_tombstones::CatalogKind::Group,
    );
    fanout_catalog(
        state,
        "/internal/v1/cluster/catalog/group/delete",
        &CatalogGroupDelete { group_id },
        "group delete",
    )
    .await;
}

pub async fn replicate_group_member_delete(state: &AppState, member_id: Uuid) {
    advance_catalog_controller(state).await;
    persist_catalog_tombstone(
        state,
        member_id,
        crate::catalog_tombstones::CatalogKind::GroupMember,
    );
    fanout_catalog(
        state,
        "/internal/v1/cluster/catalog/group-member/delete",
        &CatalogGroupMemberDelete { member_id },
        "group member delete",
    )
    .await;
}

pub async fn replicate_cron_catalog(state: &AppState, job: CronJob) {
    advance_catalog_controller(state).await;
    let peers = catalog_peer_urls(state);
    let notified = propagate_catalog_to_peers(
        &peers,
        "/internal/v1/cluster/catalog/cron",
        &CatalogCronUpsert { job },
    )
    .await;
    if !peers.is_empty() && notified == 0 {
        warn!(
            peers = peers.len(),
            "cron catalog create did not reach any peer"
        );
    }
}

pub async fn replicate_auth_credentials(
    state: &AppState,
    creds: broker_local_auth::AuthCredentials,
) {
    fanout_catalog(
        state,
        "/internal/v1/cluster/auth/apply",
        &ClusterAuthSnapshot { credentials: creds },
        "auth credentials",
    )
    .await;
}

pub async fn pull_auth_from_seed(state: &Arc<AppState>, seed_base: &str) -> Result<(), ApiError> {
    let Some(local) = &state.local_auth else {
        return Ok(());
    };
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let mut last_err = String::from("no peer URL attempted");
    for base in peer_fanout_urls(seed_base) {
        let url = format!("{}/internal/v1/cluster/auth", base.trim_end_matches('/'));
        let resp = match crate::cluster_auth::apply_cluster_secret(client.get(&url))
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                last_err = format!("cannot fetch auth from {base}: {e}");
                continue;
            }
        };
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }
        if !resp.status().is_success() {
            last_err = format!("auth at {base} returned {}", resp.status());
            continue;
        }
        let snap: ClusterAuthSnapshot = resp
            .json()
            .await
            .map_err(|e| ApiError::BadRequest(format!("invalid auth snapshot from {base}: {e}")))?;
        local
            .apply_credentials(&snap.credentials)
            .map_err(|e| ApiError::BadRequest(e.to_string()))?;
        tracing::info!(peer = %base, "cluster API token credentials synced from seed");
        return Ok(());
    }
    Err(ApiError::BadRequest(last_err))
}

pub async fn internal_cluster_auth(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ClusterAuthSnapshot>, StatusCode> {
    let local = state.local_auth.as_ref().ok_or(StatusCode::NOT_FOUND)?;
    let creds = local
        .export_credentials()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let creds = creds.ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(ClusterAuthSnapshot { credentials: creds }))
}

pub async fn internal_cluster_auth_apply(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ClusterAuthSnapshot>,
) -> Result<StatusCode, ApiError> {
    let Some(local) = &state.local_auth else {
        return Ok(StatusCode::NO_CONTENT);
    };
    local
        .apply_credentials(&body.credentials)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    Ok(StatusCode::OK)
}

pub async fn replicate_cron_delete(state: &AppState, cron_id: Uuid) {
    advance_catalog_controller(state).await;
    persist_catalog_tombstone(state, cron_id, crate::catalog_tombstones::CatalogKind::Cron);
    fanout_catalog(
        state,
        "/internal/v1/cluster/catalog/cron/delete",
        &CatalogCronDelete { cron_id },
        "cron delete",
    )
    .await;
}

pub async fn internal_catalog_flow(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CatalogFlowUpsert>,
) -> Result<StatusCode, ApiError> {
    state
        .broker
        .upsert_flow_profile(body.profile)
        .map_err(ApiError::Broker)?;
    Ok(StatusCode::OK)
}

pub async fn internal_catalog_queue(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CatalogQueueUpsert>,
) -> Result<StatusCode, ApiError> {
    state
        .broker
        .upsert_subscription_catalog(body.subscription)
        .map_err(ApiError::Broker)?;
    Ok(StatusCode::OK)
}

pub async fn internal_catalog_delete_flow(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CatalogFlowDelete>,
) -> Result<StatusCode, ApiError> {
    persist_catalog_tombstone(
        &state,
        body.flow_id,
        crate::catalog_tombstones::CatalogKind::Flow,
    );
    if state
        .broker
        .get_flow_profile(body.flow_id)
        .map_err(ApiError::Broker)?
        .is_some()
    {
        state
            .broker
            .delete_flow_profile(body.flow_id)
            .map_err(ApiError::Broker)?;
    }
    Ok(StatusCode::OK)
}

pub async fn internal_catalog_cron(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CatalogCronUpsert>,
) -> Result<StatusCode, ApiError> {
    state
        .crons
        .upsert(body.job)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    Ok(StatusCode::OK)
}

pub async fn internal_catalog_delete_cron(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CatalogCronDelete>,
) -> Result<StatusCode, ApiError> {
    persist_catalog_tombstone(
        &state,
        body.cron_id,
        crate::catalog_tombstones::CatalogKind::Cron,
    );
    if state.crons.get(body.cron_id).is_ok() {
        state
            .crons
            .delete(body.cron_id)
            .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    }
    Ok(StatusCode::OK)
}

pub async fn internal_catalog_delete_queue(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CatalogQueueDelete>,
) -> Result<StatusCode, ApiError> {
    persist_catalog_tombstone(
        &state,
        body.queue_id,
        crate::catalog_tombstones::CatalogKind::Queue,
    );
    if state
        .broker
        .get_queue_by_id(body.queue_id)
        .map_err(ApiError::Broker)?
        .is_some()
    {
        state
            .broker
            .delete_endpoint(body.queue_id)
            .map_err(ApiError::Broker)?;
    }
    Ok(StatusCode::OK)
}

pub async fn internal_catalog_group(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CatalogGroupUpsert>,
) -> Result<StatusCode, ApiError> {
    state
        .broker
        .upsert_group_catalog(body.group)
        .map_err(ApiError::Broker)?;
    Ok(StatusCode::OK)
}

pub async fn internal_catalog_delete_group(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CatalogGroupDelete>,
) -> Result<StatusCode, ApiError> {
    persist_catalog_tombstone(
        &state,
        body.group_id,
        crate::catalog_tombstones::CatalogKind::Group,
    );
    if state
        .broker
        .get_group(body.group_id)
        .map_err(ApiError::Broker)?
        .is_some()
    {
        state
            .broker
            .delete_group(body.group_id)
            .map_err(ApiError::Broker)?;
    }
    Ok(StatusCode::OK)
}

pub async fn internal_catalog_group_member(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CatalogGroupMemberUpsert>,
) -> Result<StatusCode, ApiError> {
    state
        .broker
        .upsert_group_member_catalog(body.member)
        .map_err(ApiError::Broker)?;
    Ok(StatusCode::OK)
}

pub async fn internal_catalog_delete_group_member(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CatalogGroupMemberDelete>,
) -> Result<StatusCode, ApiError> {
    persist_catalog_tombstone(
        &state,
        body.member_id,
        crate::catalog_tombstones::CatalogKind::GroupMember,
    );
    if state
        .broker
        .get_group_member(body.member_id)
        .map_err(ApiError::Broker)?
        .is_some()
    {
        state
            .broker
            .delete_group_member(body.member_id)
            .map_err(ApiError::Broker)?;
    }
    Ok(StatusCode::OK)
}

/// Shard leader ingest (cluster peers forward here instead of HTTP 307 to clients).
pub async fn internal_cluster_publish(
    State(state): State<Arc<AppState>>,
    Json(req): Json<PublishRequest>,
) -> Result<Json<PublishResponse>, ApiError> {
    let cluster = state
        .cluster
        .as_ref()
        .ok_or_else(|| ApiError::BadRequest("cluster mode not enabled".into()))?;
    let shard = shard_for_request(&state, &req);
    if !cluster.runtime.is_leader_for_shard(shard) {
        return Err(ApiError::BadRequest(format!(
            "this node is not leader for shard {shard}"
        )));
    }
    let resp = publish_on_leader(&state, req, None).await?;
    enqueue_dispatch_after_publish(&state, &resp);
    Ok(Json(resp))
}

pub async fn internal_replicate(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ReplicateAppendRequest>,
) -> Result<StatusCode, ApiError> {
    if state.broker.config().storage == StorageMode::Slate {
        return Err(ApiError::BadRequest(
            "slate mode: frame replication disabled".into(),
        ));
    }
    if body.leader_generation == u64::MAX {
        return Err(ApiError::BadRequest(
            "replicate leader_generation is invalid".into(),
        ));
    }
    if let Some(cluster) = &state.cluster {
        let expected = cluster.runtime.shard_generation(body.partition);
        if body.leader_generation < expected {
            return Err(ApiError::BadRequest(format!(
                "stale leader_generation {} < {}",
                body.leader_generation, expected
            )));
        }
        // Adopt higher generation from a fenced leader (shared meta catch-up).
        if body.leader_generation > expected {
            cluster
                .runtime
                .try_observe_shard_generation(body.partition, body.leader_generation)
                .map_err(|e| ApiError::BadRequest(e.to_string()))?;
        }
    }
    let frame = B64
        .decode(&body.frame_b64)
        .map_err(|e| ApiError::BadRequest(format!("invalid frame_b64: {e}")))?;
    let (header, _) = {
        let mut cursor = std::io::Cursor::new(&frame);
        broker_proto::decode_frame(&mut cursor)
            .map_err(|e| ApiError::BadRequest(format!("invalid frame: {e}")))?
    };
    if header.topic != body.topic {
        return Err(ApiError::BadRequest(
            "replicate frame topic does not match request topic".into(),
        ));
    }
    if !body.tenant_id.is_empty() && header.tenant_id != body.tenant_id {
        return Err(ApiError::BadRequest(
            "replicate frame tenant does not match request tenant".into(),
        ));
    }
    state
        .broker
        .append_replicated_frame(&body.topic, body.partition, &frame, Some(body.offset))?;
    state
        .broker
        .wait_committed(&body.topic, body.partition, body.offset)
        .await?;
    Ok(StatusCode::OK)
}

fn validate_replication_epoch(
    runtime: &ClusterRuntime,
    body: &ReplicateBatchRequest,
) -> Result<(), ApiError> {
    let elected = runtime.elect_leader_for_shard(body.partition);
    if elected != Some(body.leader_id) {
        return Err(ApiError::BadRequest(format!(
            "replication leader {} is not elected for shard {}",
            body.leader_id, body.partition
        )));
    }
    let expected = runtime.shard_generation(body.partition);
    if body.leader_epoch < expected {
        return Err(ApiError::BadRequest(format!(
            "stale leader_epoch {} < {}",
            body.leader_epoch, expected
        )));
    }
    if body.leader_epoch > expected {
        if body.leader_epoch != expected.saturating_add(1) {
            return Err(ApiError::BadRequest(format!(
                "leader_epoch jump {} -> {} is not allowed",
                expected, body.leader_epoch
            )));
        }
        let observed = runtime
            .try_observe_shard_generation(body.partition, body.leader_epoch)
            .map_err(|e| ApiError::BadRequest(e.to_string()))?;
        if observed != body.leader_epoch {
            return Err(ApiError::BadRequest(format!(
                "failed to durably adopt leader_epoch {}",
                body.leader_epoch
            )));
        }
    }
    Ok(())
}

pub async fn internal_replicate_batch(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<ReplicateBatchAck>, ApiError> {
    if state.broker.config().storage == StorageMode::Slate {
        return Err(ApiError::BadRequest(
            "slate mode: frame replication disabled".into(),
        ));
    }
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if content_type
        .split(';')
        .next()
        .is_none_or(|value| value.trim() != "application/octet-stream")
    {
        return Err(ApiError::BadRequest(
            "replicate batch requires Content-Type: application/octet-stream".into(),
        ));
    }
    let body = ReplicateBatchRequest::decode_binary(&body)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    if let Some(cluster) = &state.cluster {
        validate_replication_epoch(&cluster.runtime, &body)?;
    }

    // Validate every frame before mutating the follower WAL. This prevents a
    // bad CRC/topic/count near the end of an epoch from causing a partial append.
    // An empty epoch topic denotes a V2 physical-shard epoch containing several
    // logical topics; each validated frame then carries its authoritative topic.
    let mut frame_topics = Vec::with_capacity(body.frames.len());
    for frame in &body.frames {
        let (header, _) = broker_proto::decode_frame(std::io::Cursor::new(frame))
            .map_err(|e| ApiError::BadRequest(format!("invalid frame: {e}")))?;
        if !body.topic.is_empty() && header.topic != body.topic {
            return Err(ApiError::BadRequest(
                "replicate frame topic does not match epoch topic".into(),
            ));
        }
        if !body.tenant_id.is_empty() && header.tenant_id != body.tenant_id {
            return Err(ApiError::BadRequest(
                "replicate frame tenant does not match epoch tenant".into(),
            ));
        }
        frame_topics.push(header.topic);
    }

    let mut last_offset = body.first_offset;
    for (i, (frame, topic)) in body.frames.iter().zip(&frame_topics).enumerate() {
        let offset = body.first_offset + i as u64;
        last_offset = offset;
        state
            .broker
            .append_replicated_frame(topic, body.partition, frame, Some(offset))?;
    }
    let commit_topic = frame_topics.first().cloned().unwrap_or_default();
    let durable_hwm = state
        .broker
        .wait_committed(&commit_topic, body.partition, last_offset)
        .await?;
    let node_id = state
        .cluster
        .as_ref()
        .map(|cluster| cluster.runtime.config().node_id)
        .ok_or_else(|| ApiError::BadRequest("cluster mode not enabled".into()))?;
    Ok(Json(ReplicateBatchAck {
        node_id,
        partition: body.partition,
        leader_epoch: body.leader_epoch,
        durable_hwm,
    }))
}

pub async fn internal_replicate_catch_up(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ReplicateCatchUpRequest>,
) -> Result<Response, ApiError> {
    let committed_hwm = state.broker.committed_hwm(&body.topic, body.partition)?;
    let committed_remaining = committed_hwm.saturating_sub(body.from_offset) as usize;
    let max_records = body.max_records.clamp(1, 4096).min(committed_remaining);
    let frames = if max_records == 0 {
        Vec::new()
    } else {
        state.broker.replication_frames(
            &body.topic,
            body.partition,
            body.from_offset,
            max_records,
        )?
    };
    let leader_generation = state
        .cluster
        .as_ref()
        .map(|c| c.runtime.shard_generation(body.partition))
        .unwrap_or(0);
    let frames = frames
        .into_iter()
        .map(|(offset, bytes)| CatchUpFrame { offset, bytes })
        .collect();
    let encoded = ReplicateCatchUpRange {
        partition: body.partition,
        leader_generation,
        frames,
        committed_hwm,
    }
    .encode_binary()
    .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    Response::builder()
        .header(axum::http::header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(encoded))
        .map_err(|error| ApiError::BadRequest(error.to_string()))
}

pub fn is_dispatch_leader(state: &AppState, partition: u32) -> bool {
    match &state.cluster {
        None => true,
        Some(c) => c.runtime.is_leader_for_shard(partition),
    }
}

pub async fn internal_cluster_config(
    State(state): State<Arc<AppState>>,
) -> Result<Json<broker_raft_meta::ClusterConfig>, ApiError> {
    let cluster = state
        .cluster
        .as_ref()
        .ok_or_else(|| ApiError::BadRequest("cluster mode not enabled".into()))?;
    Ok(Json(cluster.runtime.config().clone()))
}

pub async fn internal_controller_vote(
    State(state): State<Arc<AppState>>,
    Json(request): Json<broker_raft_meta::RaftVoteRequest>,
) -> Result<Json<broker_raft_meta::RaftVoteResponse>, ApiError> {
    let controller = state
        .cluster
        .as_ref()
        .and_then(|cluster| cluster.runtime.controller())
        .ok_or_else(|| ApiError::BadRequest("OpenRaft controller is not enabled".into()))?;
    controller
        .raft()
        .vote(request)
        .await
        .map(Json)
        .map_err(|error| ApiError::BadRequest(error.to_string()))
}

pub async fn internal_controller_append(
    State(state): State<Arc<AppState>>,
    Json(request): Json<broker_raft_meta::ControllerAppendRequest>,
) -> Result<Json<broker_raft_meta::ControllerAppendResponse>, ApiError> {
    let controller = state
        .cluster
        .as_ref()
        .and_then(|cluster| cluster.runtime.controller())
        .ok_or_else(|| ApiError::BadRequest("OpenRaft controller is not enabled".into()))?;
    let response = controller
        .raft()
        .append_entries(request)
        .await
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    controller.note_leader_contact();
    Ok(Json(response))
}

pub async fn internal_controller_snapshot(
    State(state): State<Arc<AppState>>,
    Json(request): Json<broker_raft_meta::ControllerSnapshotRequest>,
) -> Result<Json<broker_raft_meta::ControllerSnapshotResponse>, ApiError> {
    let controller = state
        .cluster
        .as_ref()
        .and_then(|cluster| cluster.runtime.controller())
        .ok_or_else(|| ApiError::BadRequest("OpenRaft controller is not enabled".into()))?;
    controller
        .raft()
        .install_snapshot(request)
        .await
        .map(Json)
        .map_err(|error| ApiError::BadRequest(error.to_string()))
}

pub async fn internal_controller_command(
    State(state): State<Arc<AppState>>,
    Json(command): Json<broker_raft_meta::ControllerCommand>,
) -> Result<Json<broker_raft_meta::ControllerCommandResponse>, ApiError> {
    let controller = state
        .cluster
        .as_ref()
        .and_then(|cluster| cluster.runtime.controller())
        .ok_or_else(|| ApiError::BadRequest("OpenRaft controller is not enabled".into()))?;
    let state = controller
        .submit(command)
        .await
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    Ok(Json(broker_raft_meta::ControllerCommandResponse { state }))
}

/// Merge peer health observations from another cluster member (CP7b gossip).
pub async fn internal_cluster_gossip(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ClusterGossipRequest>,
) -> Result<Json<ClusterGossipResponse>, ApiError> {
    let cluster = state
        .cluster
        .as_ref()
        .ok_or_else(|| ApiError::BadRequest("cluster mode not enabled".into()))?;
    if !cluster
        .runtime
        .config()
        .nodes
        .iter()
        .any(|n| n.id == body.observer)
    {
        return Err(ApiError::BadRequest("unknown gossip observer".into()));
    }
    cluster.runtime.merge_peer_health(&body.seen);
    if let Some(proof) = &body.controller_leader {
        cluster
            .runtime
            .observe_controller_leader(proof)
            .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    }
    let vote = body
        .controller_vote
        .as_ref()
        .map(|request| cluster.runtime.vote_controller(request))
        .transpose()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    Ok(Json(ClusterGossipResponse {
        observer: cluster.runtime.config().node_id,
        controller_term: cluster.runtime.controller_term(),
        vote,
    }))
}

#[cfg(test)]
mod tests {
    use super::{peer_fanout_urls, validate_replication_epoch};
    use broker_raft_meta::{ClusterConfig, ClusterRuntime, NodeConfig};
    use broker_replication::ReplicateBatchRequest;
    use chrono::Utc;
    use uuid::Uuid;

    #[test]
    fn rewrites_localhost_peer_for_container_fanout() {
        let urls = peer_fanout_urls("http://localhost:8080");
        assert_eq!(urls[0], "http://host.docker.internal:8080");
        assert_eq!(urls[1], "http://localhost:8080");
    }

    #[test]
    fn leaves_docker_internal_url_unchanged() {
        let url = "http://host.docker.internal:8081";
        assert_eq!(peer_fanout_urls(url), vec![url.to_string()]);
    }

    #[test]
    fn stale_and_skipped_replication_terms_are_fenced() {
        let leader = Uuid::new_v4();
        let follower = Uuid::new_v4();
        let runtime = ClusterRuntime::from_config_only(ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: vec![
                NodeConfig {
                    id: leader,
                    addr: "http://leader".into(),
                },
                NodeConfig {
                    id: follower,
                    addr: "http://follower".into(),
                },
            ],
            node_id: follower,
            generation: 1,
            hash_version: 1,
        });
        let now = Utc::now().timestamp_millis();
        runtime.record_self_alive(now);
        runtime.record_peer_alive(leader, now);
        assert_eq!(runtime.observe_shard_generation(0, 5), 5);

        let request = |term| {
            ReplicateBatchRequest::new(
                "default".into(),
                "jobs".into(),
                0,
                leader,
                term,
                0,
                vec![b"frame".to_vec()],
            )
            .unwrap()
        };
        assert!(validate_replication_epoch(&runtime, &request(4)).is_err());
        assert!(validate_replication_epoch(&runtime, &request(7)).is_err());
        assert!(validate_replication_epoch(&runtime, &request(6)).is_ok());
        assert_eq!(runtime.shard_generation(0), 6);
    }
}
