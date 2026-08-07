//! Internal job lease endpoints (Phase B).

use crate::routes::ApiError;
use crate::AppState;
use axum::{extract::State, http::StatusCode, Json};
use broker_dispatch::{
    claim_from_broker, ClaimRequest, ClaimResponse, ClaimedJob, CompleteRequest, FailRequest,
    HeartbeatRequest, LeaseError,
};
use std::sync::Arc;

fn map_lease(e: LeaseError) -> ApiError {
    match e {
        LeaseError::NotShardLeader(p) => ApiError::BadRequest(format!("not shard leader for {p}")),
        LeaseError::NotFound(id) => ApiError::BadRequest(format!("lease not found: {id}")),
        LeaseError::Conflict => ApiError::BadRequest("lease held by another worker".into()),
        LeaseError::Expired => ApiError::BadRequest("lease expired".into()),
        LeaseError::Other(m) => ApiError::BadRequest(m),
    }
}

fn is_dispatch_leader(state: &AppState, partition: u32) -> bool {
    match &state.cluster {
        None => true,
        Some(c) => c.runtime.is_leader_for_shard(partition),
    }
}

fn generation(state: &AppState, partition: u32) -> u64 {
    state
        .cluster
        .as_ref()
        .map(|c| c.runtime.shard_generation(partition))
        .unwrap_or(0)
}

/// Claim pending messages on shards this node leads (CAS into lease table).
pub async fn lease_claim(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ClaimRequest>,
) -> Result<Json<ClaimResponse>, ApiError> {
    let state_ref = state.as_ref();
    let resp = claim_from_broker(
        &state_ref.broker,
        &state_ref.leases,
        Some(state_ref.fair_queue.as_ref()),
        &req.holder,
        req.max_jobs,
        req.lease_ttl_ms,
        req.partition,
        &|p| is_dispatch_leader(state_ref, p),
        &|p| generation(state_ref, p),
    );
    Ok(Json(resp))
}

pub async fn lease_heartbeat(
    State(state): State<Arc<AppState>>,
    Json(req): Json<HeartbeatRequest>,
) -> Result<Json<ClaimedJob>, ApiError> {
    let job = state
        .leases
        .heartbeat(req.lease_id, &req.holder, req.lease_ttl_ms)
        .map_err(map_lease)?;
    Ok(Json(job))
}

pub async fn lease_complete(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CompleteRequest>,
) -> Result<StatusCode, ApiError> {
    let job = state
        .leases
        .take(req.lease_id, &req.holder)
        .map_err(map_lease)?;
    if !is_dispatch_leader(&state, job.partition) {
        return Err(map_lease(LeaseError::NotShardLeader(job.partition)));
    }
    let tenant = state.broker.tenant();
    let cur = state
        .broker
        .dispatch_offset(&tenant, &job.cursor_key, job.partition)
        .unwrap_or(0);
    if job.offset >= cur {
        let next = job.offset + 1;
        let _ = state
            .broker
            .set_dispatch_offset(&tenant, &job.cursor_key, job.partition, next);
    }
    let _ = state
        .broker
        .try_purge_message(&job.topic, job.partition, job.offset);
    Ok(StatusCode::NO_CONTENT)
}

pub async fn lease_fail(
    State(state): State<Arc<AppState>>,
    Json(req): Json<FailRequest>,
) -> Result<StatusCode, ApiError> {
    let job = state
        .leases
        .take(req.lease_id, &req.holder)
        .map_err(map_lease)?;
    tracing::warn!(
        lease_id = %job.lease_id,
        topic = %job.topic,
        offset = job.offset,
        reason = %req.reason,
        dead_letter = req.dead_letter,
        "lease fail"
    );
    if req.dead_letter {
        state.dispatch.enqueue(broker_dispatch::DeliveryJob::live(
            job.topic,
            job.partition,
            job.offset,
            job.message_id,
        ));
    }
    Ok(StatusCode::NO_CONTENT)
}

pub async fn lease_status(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let jobs = state.leases.snapshot_for_status();
    Json(serde_json::json!({
        "active_leases": jobs.len(),
        "leases": jobs,
        "fleet_mode": state.dispatch_fleet,
        "broker_only": state.broker_only,
    }))
}
