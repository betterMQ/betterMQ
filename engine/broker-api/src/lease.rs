//! Internal job lease endpoints (Phase B).

use crate::routes::ApiError;
use crate::AppState;
use axum::{extract::State, http::StatusCode, Json};
use broker_dispatch::{
    claim_from_broker, ClaimRequest, ClaimResponse, ClaimedJob, CompleteRequest, FailRequest,
    HeartbeatRequest, LeaseError,
};
use std::sync::Arc;
use uuid::Uuid;

fn map_lease(e: LeaseError) -> ApiError {
    match e {
        LeaseError::NotShardLeader(p) => ApiError::Unavailable(format!("not shard leader for {p}")),
        LeaseError::NotFound(id) => ApiError::NotFound(format!("lease not found: {id}")),
        LeaseError::Conflict => ApiError::Conflict("lease held by another worker".into()),
        LeaseError::Expired => ApiError::Conflict("lease expired".into()),
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

fn validate_token(
    state: &AppState,
    generation_token: u64,
    topic: &str,
    partition: u32,
    offset: u64,
    message_id: Uuid,
    cursor_key: &str,
) -> Result<broker_storage::StoredMessage, ApiError> {
    if !is_dispatch_leader(state, partition) {
        return Err(map_lease(LeaseError::NotShardLeader(partition)));
    }
    let current_generation = generation(state, partition);
    if generation_token != current_generation {
        return Err(map_lease(LeaseError::Conflict));
    }
    let hwm = state.broker.committed_hwm(topic, partition)?;
    if offset >= hwm {
        return Err(map_lease(LeaseError::Other(
            "lease offset is not committed".into(),
        )));
    }
    let msg = state.broker.read_message(topic, partition, offset)?;
    if msg.id != message_id || broker_partition::flow_lane_owner(&msg).to_string() != cursor_key {
        return Err(map_lease(LeaseError::Conflict));
    }
    Ok(msg)
}

struct CompletionIdentity<'a> {
    generation: u64,
    topic: &'a str,
    partition: u32,
    offset: u64,
    message_id: Uuid,
    cursor_key: &'a str,
}

fn resolve_completion_job(
    state: &AppState,
    lease_id: Uuid,
    holder: &str,
    identity: CompletionIdentity<'_>,
) -> Result<ClaimedJob, ApiError> {
    let _ = validate_token(
        state,
        identity.generation,
        identity.topic,
        identity.partition,
        identity.offset,
        identity.message_id,
        identity.cursor_key,
    )?;
    if let Some(existing) = state.leases.get(lease_id) {
        if existing.generation != identity.generation
            || existing.topic != identity.topic
            || existing.partition != identity.partition
            || existing.offset != identity.offset
            || existing.message_id != identity.message_id
            || existing.cursor_key != identity.cursor_key
        {
            return Err(map_lease(LeaseError::Conflict));
        }
        return state.leases.take(lease_id, holder).map_err(map_lease);
    }
    if state
        .leases
        .is_offset_leased(identity.topic, identity.partition, identity.offset)
    {
        return Err(map_lease(LeaseError::Conflict));
    }
    // The process may have restarted after delivery. The generation-fenced,
    // committed record identity is sufficient to finish idempotently.
    Ok(ClaimedJob {
        lease_id,
        topic: identity.topic.to_string(),
        partition: identity.partition,
        offset: identity.offset,
        message_id: identity.message_id,
        expires_at_ms: chrono::Utc::now().timestamp_millis(),
        generation: identity.generation,
        committed_hwm: state
            .broker
            .committed_hwm(identity.topic, identity.partition)?,
        cursor_key: identity.cursor_key.to_string(),
        message: None,
    })
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
    if job.generation != generation(&state, job.partition)
        || !is_dispatch_leader(&state, job.partition)
    {
        let _ = state.leases.take(req.lease_id, &req.holder);
        return Err(map_lease(LeaseError::Conflict));
    }
    Ok(Json(job))
}

pub async fn lease_complete(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CompleteRequest>,
) -> Result<StatusCode, ApiError> {
    let job = resolve_completion_job(
        &state,
        req.lease_id,
        &req.holder,
        CompletionIdentity {
            generation: req.generation,
            topic: &req.topic,
            partition: req.partition,
            offset: req.offset,
            message_id: req.message_id,
            cursor_key: &req.cursor_key,
        },
    )?;
    let tenant = state.broker.tenant();
    if let Err(e) = state
        .dispatch
        .commit_cursor(
            &tenant,
            &job.topic,
            &job.cursor_key,
            job.partition,
            job.offset,
        )
        .await
    {
        let mut retry = job.clone();
        retry.expires_at_ms = chrono::Utc::now().timestamp_millis() + 5_000;
        let _ = state.leases.try_insert(req.holder.clone(), retry);
        return Err(ApiError::Broker(broker_partition::BrokerError::Storage(
            broker_storage::LogError::Io(std::io::Error::other(e.to_string())),
        )));
    }
    if let Err(e) = state
        .broker
        .try_purge_message(&job.topic, job.partition, job.offset)
    {
        tracing::warn!(error = %e, "lease complete: purge failed");
    }
    Ok(StatusCode::NO_CONTENT)
}

pub async fn lease_fail(
    State(state): State<Arc<AppState>>,
    Json(req): Json<FailRequest>,
) -> Result<StatusCode, ApiError> {
    let mut job = resolve_completion_job(
        &state,
        req.lease_id,
        &req.holder,
        CompletionIdentity {
            generation: req.generation,
            topic: &req.topic,
            partition: req.partition,
            offset: req.offset,
            message_id: req.message_id,
            cursor_key: &req.cursor_key,
        },
    )?;
    tracing::warn!(
        lease_id = %job.lease_id,
        topic = %job.topic,
        offset = job.offset,
        reason = %req.reason,
        dead_letter = req.dead_letter,
        "lease fail"
    );
    if req.dead_letter {
        if let Err(e) = state
            .dispatch
            .dead_letter_offset(&job.topic, job.partition, job.offset, &req.reason)
            .await
        {
            job.expires_at_ms = chrono::Utc::now().timestamp_millis() + 5_000;
            let _ = state.leases.try_insert(req.holder.clone(), job.clone());
            return Err(ApiError::Broker(broker_partition::BrokerError::Storage(
                broker_storage::LogError::Io(std::io::Error::other(e.to_string())),
            )));
        }
    } else {
        job.expires_at_ms = chrono::Utc::now().timestamp_millis()
            + i64::try_from(req.retry_after_ms.max(250)).unwrap_or(i64::MAX);
        if !state.leases.try_insert(req.holder, job) {
            return Err(map_lease(LeaseError::Conflict));
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use broker_dispatch::{DispatchConfig, DispatchEngine, LeaseTable};
    use broker_partition::{Broker, BrokerConfig, PublishRequest};
    use broker_schedule::{CronRegistry, ScheduleQueue};

    #[tokio::test]
    async fn completion_is_generation_fenced_and_survives_empty_lease_table() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let response = broker
            .publish(PublishRequest {
                topic: String::new(),
                queue_id: None,
                group_id: None,
                group_member_id: None,
                routing_key: "restart".into(),
                payload: "body".into(),
                payload_encoding: None,
                idempotency_key: None,
                delay_ms: None,
                priority: None,
                flow_id: None,
                url: Some("https://example.com/hook".into()),
                secret: Some("secret".into()),
                destination: None,
                flow: None,
                parallelism: None,
                max_retries: None,
                retry_backoff: None,
                method: None,
                headers: None,
                sign: None,
                request: None,
            })
            .unwrap();
        broker.flush_wal().unwrap();
        let partition = response.partition.unwrap();
        let offset = response.offset.unwrap();
        let message_id = response.message_id.unwrap();
        let cursor_key = broker_partition::flow_lane_owner(
            &broker
                .read_message(&response.topic, partition, offset)
                .unwrap(),
        )
        .to_string();
        let state = Arc::new(AppState {
            broker: broker.clone(),
            schedule: ScheduleQueue::open(dir.path()).unwrap(),
            crons: CronRegistry::open(dir.path()).unwrap(),
            dispatch: DispatchEngine::new_broker_only(broker.clone(), DispatchConfig::default()),
            leases: LeaseTable::new(),
            cluster: None,
            local_auth: None,
            fair_queue: Arc::new(broker_dispatch::TenantFairQueue::new()),
            catalog_tombstones: crate::catalog_tombstones::CatalogTombstones::open(dir.path())
                .unwrap(),
            dispatch_fleet: false,
            broker_only: false,
            #[cfg(feature = "cloud")]
            auth: None,
            #[cfg(feature = "cloud")]
            control_plane: None,
        });
        let request = |generation| CompleteRequest {
            lease_id: Uuid::new_v4(),
            holder: "worker-after-restart".into(),
            generation,
            topic: response.topic.clone(),
            partition,
            offset,
            message_id,
            cursor_key: cursor_key.clone(),
        };

        assert!(matches!(
            lease_complete(State(state.clone()), Json(request(1))).await,
            Err(ApiError::Conflict(_))
        ));
        assert_eq!(
            lease_complete(State(state), Json(request(0)))
                .await
                .unwrap(),
            StatusCode::NO_CONTENT
        );
        assert!(broker
            .is_dispatch_complete(&broker.tenant(), &cursor_key, partition, offset)
            .unwrap());
    }
}
