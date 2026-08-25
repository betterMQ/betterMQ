//! Canonical ingest pipeline shared by publish, enqueue, batch, gateway, and delay.

use crate::cluster::publish_with_cluster;
use crate::metrics::{record_ack_latency, record_rejected};
use crate::routes::{snapshot_destination, validate_publish_destinations_pub, ApiError};
use crate::AppState;
use axum::http::StatusCode;
use broker_partition::{PublishRequest, PublishResponse};
use broker_replication::ReplicateBatchRequest;
use broker_schedule::ScheduledPublishRequest;
use broker_storage::StorageMode;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinSet;

pub const MAX_BATCH_MESSAGES: usize = 100;
pub const MAX_HT_BATCH_MESSAGES: usize = 1000;
pub const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct IngestEnvelope {
    pub req: PublishRequest,
    pub body_bytes: usize,
}

#[derive(Debug)]
pub struct IngestBatch {
    pub records: Vec<IngestEnvelope>,
}

impl IngestBatch {
    pub fn from_requests(reqs: Vec<PublishRequest>) -> Result<Self, ApiError> {
        if reqs.is_empty() {
            return Err(ApiError::BadRequest(
                "batch must contain at least one message".into(),
            ));
        }
        let total_bytes: usize = reqs.iter().map(|r| r.payload.len()).sum();
        if total_bytes > MAX_BATCH_BYTES {
            return Err(ApiError::BadRequest(format!(
                "batch exceeds max bytes of {MAX_BATCH_BYTES}"
            )));
        }
        for req in &reqs {
            validate_publish_destinations_pub(req)?;
        }
        Ok(Self {
            records: reqs
                .into_iter()
                .map(|req| IngestEnvelope {
                    body_bytes: req.payload.len(),
                    req,
                })
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_dispatch::{DispatchConfig, DispatchEngine, LeaseTable};
    use broker_partition::{Broker, BrokerConfig, PublishRequest};
    use broker_schedule::{CronRegistry, ScheduleQueue};

    fn sample(delay: Option<u64>) -> PublishRequest {
        PublishRequest {
            topic: String::new(),
            queue_id: None,
            group_id: None,
            group_member_id: None,
            routing_key: "rk".into(),
            payload: "x".into(),
            payload_encoding: None,
            idempotency_key: None,
            delay_ms: delay,
            priority: None,
            flow_id: None,
            url: Some("http://127.0.0.1:9/h".into()),
            secret: Some("s".into()),
            destination: None,
            flow: None,
            parallelism: None,
            max_retries: None,
            retry_backoff: None,
            method: None,
            headers: None,
            sign: None,
            request: None,
        }
    }

    #[test]
    fn batch_preserves_delay_ms() {
        std::env::set_var("BETTERMQ_ALLOW_PRIVATE_DESTINATIONS", "1");
        let batch = IngestBatch::from_requests(vec![sample(Some(1500)), sample(None)]).unwrap();
        assert_eq!(batch.records[0].req.delay_ms, Some(1500));
        assert_eq!(batch.records[1].req.delay_ms, None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn local_api_batch_uses_one_shard_commit_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
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
            broker_only: true,
            #[cfg(feature = "cloud")]
            auth: None,
            #[cfg(feature = "cloud")]
            control_plane: None,
        });
        let requests = (0..100)
            .map(|index| {
                let mut request = sample(None);
                request.url = Some("https://example.com/hook".into());
                request.payload = format!("body-{index}");
                request.idempotency_key = Some(format!("batch-{index}"));
                request
            })
            .collect();

        let outcomes =
            submit_batch(&state, None, IngestBatch::from_requests(requests).unwrap()).await;
        assert_eq!(outcomes.len(), 100);
        assert!(outcomes.iter().all(|outcome| outcome.result.is_ok()));
        let first = outcomes[0].result.as_ref().unwrap();
        let epoch = first.commit_epoch;
        assert!(epoch.is_some());
        assert!(outcomes.iter().all(|outcome| {
            let response = outcome.result.as_ref().unwrap();
            response.partition == first.partition && response.commit_epoch == epoch
        }));
        let offsets: Vec<_> = outcomes
            .iter()
            .map(|outcome| outcome.result.as_ref().unwrap().offset.unwrap())
            .collect();
        assert!(offsets.windows(2).all(|window| window[1] == window[0] + 1));
    }
}

#[derive(Debug, serde::Serialize)]
pub struct IngestItemResult {
    pub index: usize,
    pub accepted: bool,
    pub message_id: Option<uuid::Uuid>,
    pub topic: String,
    pub partition: Option<u32>,
    pub offset: Option<u64>,
    pub duplicate: bool,
    pub commit_epoch: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduled: Option<broker_partition::ScheduledInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub(crate) async fn ingest_one_validated(
    state: &Arc<AppState>,
    mut req: PublishRequest,
    meter: Option<crate::metering::IngestMeter>,
) -> Result<PublishResponse, ApiError> {
    let _permit =
        crate::admission::check_admission(state, meter.map(|m| m.tenant_id), req.payload.len())?;
    if let Some(delay_ms) = req.delay_ms.take() {
        if req.idempotency_key.is_none() {
            req.idempotency_key = Some(format!("delay:{}", uuid::Uuid::new_v4()));
        }
        let destination = snapshot_destination(state, &req).await?;
        let scheduled = state.schedule.schedule(
            ScheduledPublishRequest {
                topic: req.topic.clone(),
                routing_key: req.routing_key.clone(),
                payload: req.payload.clone(),
                payload_encoding: req.payload_encoding.clone(),
                idempotency_key: req.idempotency_key.clone(),
                priority: req.priority,
                parallelism: req.parallelism,
                flow_id: req.flow_id,
                queue_id: req.queue_id,
                destination: Some(destination),
                flow: req.flow.clone(),
                max_retries: req.max_retries,
                retry_backoff: req.retry_backoff.clone(),
                method: req.method.clone(),
                headers: req.headers.clone(),
                sign: req.sign,
                request: req.request.clone(),
            },
            delay_ms,
        )?;
        return Ok(PublishResponse {
            message_id: None,
            topic: req.topic,
            partition: None,
            offset: None,
            duplicate: false,
            scheduled: Some(broker_partition::ScheduledInfo {
                schedule_id: scheduled.id,
                deliver_at_ms: scheduled.deliver_at_ms,
            }),
            commit_epoch: None,
            replication_frame: None,
        });
    }

    let started = Instant::now();
    let resp = publish_with_cluster(state, req, meter).await?;
    record_ack_latency(started.elapsed());
    Ok(resp)
}

/// Single-record ingest used by publish/enqueue/GET compatibility.
pub async fn submit_one(
    state: &Arc<AppState>,
    req: PublishRequest,
    meter: Option<crate::metering::IngestMeter>,
) -> Result<PublishResponse, ApiError> {
    validate_publish_destinations_pub(&req)?;
    ingest_one_validated(state, req, meter).await
}

#[derive(Debug)]
pub struct IngestOutcome {
    pub index: usize,
    pub topic: String,
    pub result: Result<PublishResponse, ApiError>,
}

fn failed_outcomes(
    records: &[(usize, String, usize)],
    error: impl Into<String>,
) -> Vec<IngestOutcome> {
    let error = error.into();
    records
        .iter()
        .map(|(index, topic, _)| {
            record_rejected();
            IngestOutcome {
                index: *index,
                topic: topic.clone(),
                result: Err(ApiError::BadRequest(error.clone())),
            }
        })
        .collect()
}

/// Local-leader fast path: prepare once, append once per physical shard, request
/// all shard commit barriers concurrently, then replicate complete epochs before
/// publishing dedup results. One shard sub-batch therefore uses one WAL epoch,
/// one local fsync, and one RF=3 quorum operation.
async fn submit_local_leader_batch(
    state: &Arc<AppState>,
    ingest: Option<crate::metering::IngestAuth>,
    records: Vec<(usize, IngestEnvelope)>,
) -> Vec<IngestOutcome> {
    if records.is_empty() {
        return Vec::new();
    }

    let started = Instant::now();
    let mut admitted = Vec::with_capacity(records.len());
    let mut requests = Vec::with_capacity(records.len());
    let mut rejected = Vec::new();
    let mut guards = Vec::with_capacity(records.len());
    for (index, env) in records {
        match crate::admission::check_admission(
            state,
            ingest.map(|auth| auth.tenant_id),
            env.body_bytes,
        ) {
            Ok(guard) => {
                guards.push(guard);
                admitted.push((index, env.req.topic.clone(), env.body_bytes));
                requests.push(env.req);
            }
            Err(error) => {
                record_rejected();
                rejected.push(IngestOutcome {
                    index,
                    topic: env.req.topic,
                    result: Err(error),
                });
            }
        }
    }
    if admitted.is_empty() {
        return rejected;
    }

    let mut prepared = match state.broker.prepare_publish_batch(requests) {
        Ok(prepared) => prepared,
        Err(error) => {
            rejected.extend(failed_outcomes(
                &admitted,
                format!("batch preparation failed: {error}"),
            ));
            return rejected;
        }
    };

    let mut append_tasks = JoinSet::new();
    for shard_batch in prepared.take_shard_batches() {
        let broker = state.broker.clone();
        append_tasks.spawn_blocking(move || broker.append_prepared_batch(shard_batch));
    }
    let mut appended_batches = Vec::new();
    let mut append_error = None;
    while let Some(joined) = append_tasks.join_next().await {
        match joined {
            Ok(Ok(appended)) => appended_batches.push(appended),
            Ok(Err(error)) => {
                append_error.get_or_insert_with(|| format!("batch append failed: {error}"));
            }
            Err(error) => {
                append_error.get_or_insert_with(|| format!("batch append task failed: {error}"));
            }
        }
    }
    if let Some(error) = append_error {
        rejected.extend(failed_outcomes(&admitted, error));
        return rejected;
    }

    let replicate = state
        .cluster
        .as_ref()
        .is_some_and(|cluster| cluster.runtime.config().node_count() > 1)
        && state.broker.config().storage != StorageMode::Slate;
    let mut commit_tasks = JoinSet::new();
    for appended in appended_batches {
        let Some(last_offset) = appended.last_offset() else {
            continue;
        };
        let Some(first) = appended.records().next() else {
            continue;
        };
        let topic = first.topic.to_string();
        let partition = first.partition;
        let response_keys: Vec<_> = appended
            .records()
            .map(|record| (record.topic.to_string(), record.partition))
            .collect();
        let broker = state.broker.clone();
        let local = async move {
            broker
                .wait_committed(&topic, partition, last_offset)
                .await
                .map_err(|error| {
                    broker_replication::ReplicateError::InvalidEpoch(error.to_string())
                })
        };
        if replicate {
            let cluster = state.cluster.as_ref().expect("cluster checked above");
            let records: Vec<_> = appended.records().collect();
            let Some(first_rec) = records.first() else {
                continue;
            };
            let epoch_topic = if records.iter().all(|record| record.topic == first_rec.topic) {
                first_rec.topic.to_string()
            } else {
                String::new()
            };
            let frames = records
                .iter()
                .map(|record| record.replication_frame.to_vec())
                .collect();
            let request = match ReplicateBatchRequest::new(
                state.broker.tenant(),
                epoch_topic,
                first_rec.partition,
                cluster.runtime.config().node_id,
                cluster.runtime.shard_generation(first_rec.partition),
                first_rec.offset,
                frames,
            ) {
                Ok(request) => request,
                Err(error) => {
                    rejected.extend(failed_outcomes(
                        &admitted,
                        format!("batch replication encoding failed: {error}"),
                    ));
                    return rejected;
                }
            };
            let replication = cluster.replication.clone();
            commit_tasks.spawn(async move {
                let outcome = replication
                    .replicate_batch_with_local(request, local)
                    .await?;
                Ok::<_, broker_replication::ReplicateError>((
                    appended,
                    response_keys,
                    outcome.committed_hwm,
                ))
            });
        } else {
            commit_tasks.spawn(async move {
                let epoch = local.await?;
                Ok::<_, broker_replication::ReplicateError>((appended, response_keys, epoch))
            });
        }
    }

    let mut committed = Vec::new();
    let mut commit_error = None;
    while let Some(joined) = commit_tasks.join_next().await {
        match joined {
            Ok(Ok(item)) => committed.push(item),
            Ok(Err(error)) => {
                commit_error.get_or_insert_with(|| format!("batch commit failed: {error}"));
            }
            Err(error) => {
                commit_error.get_or_insert_with(|| format!("batch commit task failed: {error}"));
            }
        }
    }
    if let Some(error) = commit_error {
        rejected.extend(failed_outcomes(&admitted, error));
        return rejected;
    }

    let mut epochs = HashMap::new();
    for (appended, response_keys, epoch) in committed {
        if let Err(error) = state
            .broker
            .finalize_prepared_batch(&mut prepared, appended, true)
        {
            rejected.extend(failed_outcomes(
                &admitted,
                format!("batch finalize failed: {error}"),
            ));
            return rejected;
        }
        for key in response_keys {
            epochs.insert(key, epoch);
        }
    }

    let responses = match prepared.finish() {
        Ok(responses) => responses,
        Err(error) => {
            rejected.extend(failed_outcomes(
                &admitted,
                format!("batch response finalization failed: {error}"),
            ));
            return rejected;
        }
    };

    let latency = started.elapsed();
    for ((index, topic, body_bytes), mut response) in admitted.into_iter().zip(responses) {
        if let Some(partition) = response.partition {
            response.commit_epoch = epochs.get(&(response.topic.clone(), partition)).copied();
        }
        record_ack_latency(latency);
        if let Some(auth) = ingest {
            if !response.duplicate {
                crate::metering::record_ingest(state, auth.tenant_id, body_bytes as u64).await;
            }
        }
        rejected.push(IngestOutcome {
            index,
            topic,
            result: Ok(response),
        });
    }
    drop(guards);
    rejected.sort_by_key(|outcome| outcome.index);
    rejected
}

/// Submit a batch after validating the whole envelope. Immediate records owned
/// by this node share physical-shard append, fsync, and RF=3 replication epochs.
/// Delayed records and records forwarded to another leader retain the canonical
/// single-record path and explicit indexed outcomes.
pub async fn submit_batch(
    state: &Arc<AppState>,
    ingest: Option<crate::metering::IngestAuth>,
    batch: IngestBatch,
) -> Vec<IngestOutcome> {
    let mut immediate = Vec::new();
    let mut out = Vec::with_capacity(batch.records.len());
    for (index, env) in batch.records.into_iter().enumerate() {
        if env.req.delay_ms.is_none() {
            immediate.push((index, env));
            continue;
        }
        let meter = crate::metering::ingest_meter(ingest, env.body_bytes);
        let topic = env.req.topic.clone();
        let result = ingest_one_validated(state, env.req, meter).await;
        if result.is_err() {
            record_rejected();
        }
        out.push(IngestOutcome {
            index,
            topic,
            result,
        });
    }
    let mut locally_led = Vec::new();
    let mut forwarded = Vec::new();
    for (index, env) in immediate {
        let is_local = state.cluster.as_ref().is_none_or(|cluster| {
            cluster.runtime.config().node_count() <= 1
                || cluster.runtime.is_leader_for_shard(
                    state
                        .broker
                        .assign_shard(&env.req.topic, &env.req.routing_key),
                )
        });
        if is_local {
            locally_led.push((index, env));
        } else {
            forwarded.push((index, env));
        }
    }
    out.extend(submit_local_leader_batch(state, ingest, locally_led).await);
    for (index, env) in forwarded {
        let meter = crate::metering::ingest_meter(ingest, env.body_bytes);
        let topic = env.req.topic.clone();
        let result = ingest_one_validated(state, env.req, meter).await;
        if result.is_err() {
            record_rejected();
        }
        out.push(IngestOutcome {
            index,
            topic,
            result,
        });
    }
    out.sort_by_key(|outcome| outcome.index);
    out
}

pub fn status_for(resp: &PublishResponse) -> StatusCode {
    if resp.duplicate {
        StatusCode::OK
    } else {
        StatusCode::ACCEPTED
    }
}

/// After commit, notify dispatch (never before HWM).
pub fn notify_dispatch(state: &AppState, resp: &PublishResponse) {
    crate::cluster::enqueue_dispatch_after_publish(state, resp);
}
