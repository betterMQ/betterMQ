//! BetterMQ HTTP webhook API.

use crate::http_fields::OutboundHttpFields;
use crate::routes::{create_subscription, publish, ApiError};
use crate::AppState;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use broker_dispatch::{FlowControlInfo, GlobalParallelismInfo};
use broker_partition::{
    dlq_topic, group_member_dlq_topic, is_dlq_topic, BrokerError, CreateSubscriptionRequest,
    CreateSubscriptionResponse, DestinationSnapshot, FlowSpec, PublishRequest, PublishResponse,
    ResolvedFlow, ScheduledInfo, StoredMessage, Subscription, DIRECT_TOPIC,
};
use broker_schedule::{
    CronError, CronJob, ScheduleError, ScheduledPublish, ScheduledPublishRequest,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

pub fn bettermq_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/v1/publish/{*destination}",
            get(crate::publish_path::publish_get),
        )
        .route("/v1/publish", post(publish_job))
        .route("/v1/enqueue", post(enqueue))
        .route("/v1/queues/{queue_id}/enqueue", post(enqueue_to_queue))
        .route("/v1/dlq", get(list_dlq).delete(delete_dlq_message))
        .route("/v1/dlq/sources", get(list_dlq_sources))
        .route("/v1/flows", get(list_flows).post(create_flow))
        .route("/v1/flows/{flow_id}", delete(delete_flow))
        .route("/v1/queues", get(list_queues).post(create_queue))
        .route("/v1/queues/{queue_id}", delete(delete_queue))
        .route("/v1/delayed", get(list_delayed))
        .route("/v1/delayed/{schedule_id}", delete(cancel_delayed))
        .route("/v1/flow/{key}", get(get_flow).put(upsert_flow_control))
        .route("/v1/flow/{key}/pause", post(pause_flow))
        .route("/v1/flow/{key}/resume", post(resume_flow))
        .route("/v1/flow/{key}/reset-rate", post(reset_flow_rate))
        .route("/v1/flow/{key}/pin", post(pin_flow))
        .route("/v1/flow/{key}/unpin", post(unpin_flow))
        .route("/v1/flow/global", get(global_flow))
        .route("/v1/crons", get(list_crons).post(create_cron))
        .route("/v1/crons/{cron_id}", get(get_cron).delete(delete_cron))
        .route("/v1/crons/{cron_id}/pause", post(pause_cron))
        .route("/v1/crons/{cron_id}/resume", post(resume_cron))
        .route("/v1/cluster", get(crate::cluster::get_cluster_status))
}

// --- Request types ---

/// Optional per-request retry overrides (`max_retries` + `retry_backoff`).
#[derive(Debug, Deserialize, Default, Clone)]
pub struct RetryInput {
    #[serde(default)]
    pub max_retries: Option<u32>,
    #[serde(default)]
    pub retry_backoff: Option<broker_proto::RetryBackoff>,
}

/// Enqueue into a named queue (`queue_id` preferred over `queue` name).
#[derive(Debug, Deserialize)]
pub struct EnqueueRequest {
    #[serde(default)]
    pub queue_id: Option<Uuid>,
    #[serde(default)]
    pub queue: String,
    #[serde(default)]
    pub key: String,
    #[serde(deserialize_with = "broker_partition::payload::deserialize_flexible_payload")]
    pub body: String,
    #[serde(default)]
    pub body_encoding: Option<String>,
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub delay: Option<u64>,
    #[serde(default)]
    pub priority: Option<u8>,
    #[serde(default)]
    pub flow_id: Option<Uuid>,
    /// Inline flow limits — upserts a profile by `flow.key`.
    #[serde(default)]
    pub flow: Option<FlowSpec>,
    #[serde(flatten)]
    pub retry: RetryInput,
    #[serde(flatten)]
    pub outbound: OutboundHttpFields,
}

/// One-off delivery to a URL (`POST /v1/publish`).
#[derive(Debug, Deserialize)]
pub struct PublishJobRequest {
    pub url: String,
    pub secret: String,
    #[serde(default)]
    pub key: String,
    #[serde(deserialize_with = "broker_partition::payload::deserialize_flexible_payload")]
    pub body: String,
    #[serde(default)]
    pub body_encoding: Option<String>,
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub delay: Option<u64>,
    #[serde(default)]
    pub priority: Option<u8>,
    #[serde(default)]
    pub flow_id: Option<Uuid>,
    /// Inline flow limits — upserts a profile by `flow.key`.
    #[serde(default)]
    pub flow: Option<FlowSpec>,
    #[serde(flatten)]
    pub retry: RetryInput,
    #[serde(flatten)]
    pub outbound: OutboundHttpFields,
}

#[derive(Debug, Deserialize)]
pub struct UpsertFlowControlRequest {
    #[serde(default = "default_flow_parallelism")]
    pub parallelism: u32,
    #[serde(default)]
    pub rate: u32,
    #[serde(default = "default_flow_period")]
    pub period_secs: u64,
}

#[derive(Debug, Deserialize)]
pub struct CreateFlowRequest {
    pub key: String,
    #[serde(default = "default_flow_parallelism")]
    pub parallelism: u32,
    #[serde(default)]
    pub rate: u32,
    #[serde(default = "default_flow_period")]
    pub period_secs: u64,
}

fn default_flow_parallelism() -> u32 {
    1
}

fn default_flow_period() -> u64 {
    60
}

#[derive(Debug, Deserialize)]
pub struct DlqQuery {
    /// Primary queue name (`jobs` → `jobs.__dlq`). Legacy param; use `dlq_topic` for direct/group DLQs.
    #[serde(default)]
    pub queue: Option<String>,
    /// Full DLQ topic (`__direct.__dlq`, `__group.{id}.{member}.__dlq`, …).
    #[serde(default)]
    pub dlq_topic: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
pub struct DeliveryRef {
    pub shard: u32,
    pub seq: u64,
}

#[derive(Debug, Deserialize)]
pub struct CreateQueueRequest {
    pub queue: String,
    pub url: String,
    pub secret: String,
    #[serde(flatten)]
    pub retry: RetryInput,
}

#[derive(Debug, Deserialize)]
pub struct FlowKeyQuery {
    #[serde(alias = "endpoint_id")]
    pub queue_id: Option<Uuid>,
    #[serde(alias = "flow_id")]
    pub flow_profile_id: Option<Uuid>,
    pub group_member_id: Option<Uuid>,
}

#[derive(Debug, Deserialize, Default)]
pub struct PinFlowRequest {
    pub parallelism: Option<u32>,
    pub rate: Option<u32>,
    pub period_secs: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
pub struct UnpinFlowRequest {
    #[serde(default)]
    pub parallelism: bool,
    #[serde(default)]
    pub rate: bool,
}

// --- Response types ---

#[derive(Debug, Serialize)]
pub struct EnqueueResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<Uuid>,
    pub queue: String,
    pub duplicate: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery: Option<DeliveryRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduled: Option<ScheduledInfo>,
}

#[derive(Debug, Serialize)]
pub struct DlqListResponse {
    pub queue: String,
    pub dlq_topic: String,
    pub messages: Vec<DlqMessage>,
}

#[derive(Debug, Serialize)]
pub struct DlqMessage {
    pub message_id: Uuid,
    pub key: String,
    pub body: String,
    pub published_at_ms: i64,
    pub partition: u32,
    pub offset: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_queue: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination_url: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct DeleteDlqQuery {
    pub dlq_topic: String,
    pub partition: u32,
    pub offset: u64,
}

#[derive(Debug, Serialize)]
pub struct DlqSourceResponse {
    pub dlq_topic: String,
    /// `queue` | `direct` | `group_member`
    pub kind: String,
    pub label: String,
    pub count: usize,
    /// Set when `kind` is `queue` — pass as `?queue=` to list messages.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct QueueResponse {
    pub queue_id: Uuid,
    pub queue: String,
    pub url: String,
}

#[derive(Debug, Serialize)]
pub struct FlowProfileResponse {
    pub flow_id: Uuid,
    pub key: String,
    pub parallelism: u32,
    pub rate: u32,
    pub period_secs: u64,
}

#[derive(Debug, Serialize)]
pub struct FlowListResponse {
    pub flows: Vec<FlowProfileResponse>,
}

fn default_limit() -> usize {
    10
}

fn to_publish_enqueue(req: EnqueueRequest) -> Result<PublishRequest, ApiError> {
    if req.queue_id.is_none() && req.queue.is_empty() {
        return Err(ApiError::Broker(BrokerError::QueueNotFound(
            "queue_id or queue name required".into(),
        )));
    }
    let mut pr = PublishRequest {
        topic: req.queue,
        queue_id: req.queue_id,
        group_id: None,
        group_member_id: None,
        routing_key: req.key,
        payload: req.body,
        payload_encoding: req.body_encoding,
        idempotency_key: req.idempotency_key,
        delay_ms: req.delay,
        priority: req.priority,
        flow_id: req.flow_id,
        url: None,
        secret: None,
        destination: None,
        flow: None,
        parallelism: None,
        max_retries: req.retry.max_retries,
        retry_backoff: req.retry.retry_backoff.clone(),
        method: None,
        headers: None,
        sign: None,
        request: None,
    };
    req.outbound.apply_to(&mut pr);
    Ok(pr)
}

fn to_publish_job(req: PublishJobRequest) -> PublishRequest {
    let mut pr = PublishRequest {
        topic: broker_partition::DIRECT_TOPIC.to_string(),
        queue_id: None,
        group_id: None,
        group_member_id: None,
        routing_key: req.key,
        payload: req.body,
        payload_encoding: req.body_encoding,
        idempotency_key: req.idempotency_key,
        delay_ms: req.delay,
        priority: req.priority,
        flow_id: req.flow_id,
        url: Some(req.url),
        secret: Some(req.secret),
        destination: None,
        flow: None,
        parallelism: None,
        max_retries: req.retry.max_retries,
        retry_backoff: req.retry.retry_backoff.clone(),
        method: None,
        headers: None,
        sign: None,
        request: None,
    };
    req.outbound.apply_to(&mut pr);
    pr
}

pub(crate) fn to_enqueue_response(inner: PublishResponse) -> EnqueueResponse {
    let delivery = match (inner.partition, inner.offset) {
        (Some(shard), Some(seq)) => Some(DeliveryRef { shard, seq }),
        _ => None,
    };
    EnqueueResponse {
        message_id: inner.message_id,
        queue: inner.topic,
        duplicate: inner.duplicate,
        delivery,
        scheduled: inner.scheduled,
    }
}

fn parse_dlq_payload(body: &str) -> (Option<String>, Option<String>) {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(body) else {
        return (None, None);
    };
    let source_queue = v
        .get("source_queue")
        .and_then(|x| x.as_str())
        .map(str::to_string);
    let destination_url = v
        .get("destination_url")
        .and_then(|x| x.as_str())
        .map(str::to_string);
    (source_queue, destination_url)
}

fn to_dlq_message(msg: StoredMessage) -> DlqMessage {
    let body = String::from_utf8_lossy(&msg.payload).into_owned();
    let (source_queue, destination_url) = parse_dlq_payload(&body);
    DlqMessage {
        message_id: msg.id,
        key: msg.routing_key,
        body,
        published_at_ms: msg.published_at_ms,
        partition: msg.partition,
        offset: msg.offset,
        source_queue,
        destination_url,
    }
}

fn resolve_dlq_topic(q: &DlqQuery) -> Result<String, ApiError> {
    if let Some(topic) = &q.dlq_topic {
        if !broker_partition::is_dlq_topic(topic) {
            return Err(ApiError::BadRequest(format!(
                "dlq_topic must end with .__dlq (got {topic})"
            )));
        }
        return Ok(topic.clone());
    }
    if let Some(queue) = &q.queue {
        if queue.is_empty() {
            return Err(ApiError::BadRequest("queue must not be empty".into()));
        }
        return Ok(dlq_topic(queue));
    }
    Err(ApiError::BadRequest(
        "provide queue or dlq_topic query parameter".into(),
    ))
}

fn dlq_source_label(state: &AppState, dlq_topic_name: &str) -> (String, String, Option<String>) {
    let direct = dlq_topic(DIRECT_TOPIC);
    if dlq_topic_name == direct {
        return (
            "direct".into(),
            "Direct publish".into(),
            Some(DIRECT_TOPIC.into()),
        );
    }
    if let Some(rest) = dlq_topic_name.strip_prefix("__group.") {
        if let Some((ids, _)) = rest.split_once(".__dlq") {
            let parts: Vec<&str> = ids.splitn(2, '.').collect();
            if parts.len() == 2 {
                if let (Ok(gid), Ok(mid)) = (Uuid::parse_str(parts[0]), Uuid::parse_str(parts[1])) {
                    let group_name = state
                        .broker
                        .get_group(gid)
                        .ok()
                        .flatten()
                        .map(|g| g.name)
                        .unwrap_or_else(|| gid.to_string());
                    let member_name = state
                        .broker
                        .get_group_member(mid)
                        .ok()
                        .flatten()
                        .map(|m| m.name)
                        .unwrap_or_else(|| mid.to_string());
                    return (
                        "group_member".into(),
                        format!("Group {group_name} → {member_name}"),
                        None,
                    );
                }
            }
        }
    }
    if let Some(queue) = dlq_topic_name.strip_suffix(".__dlq") {
        return (
            "queue".into(),
            format!("Queue: {queue}"),
            Some(queue.to_string()),
        );
    }
    ("other".into(), dlq_topic_name.to_string(), None)
}

fn collect_dlq_topic_candidates(state: &AppState) -> Result<Vec<String>, ApiError> {
    let mut topics: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    topics.insert(dlq_topic(DIRECT_TOPIC));
    for q in state.broker.list_endpoints()? {
        topics.insert(dlq_topic(&q.topic));
    }
    for g in state.broker.list_groups()? {
        for m in state.broker.list_group_members(g.id)? {
            topics.insert(group_member_dlq_topic(g.id, m.id));
        }
    }
    for t in state.broker.list_dlq_topics_on_disk()? {
        topics.insert(t);
    }
    Ok(topics.into_iter().collect())
}

fn to_queue_response(inner: CreateSubscriptionResponse) -> QueueResponse {
    QueueResponse {
        queue_id: inner.id,
        queue: inner.topic,
        url: inner.url,
    }
}

fn to_flow_response(p: broker_partition::FlowProfile) -> FlowProfileResponse {
    FlowProfileResponse {
        flow_id: p.id,
        key: p.key,
        parallelism: p.parallelism,
        rate: p.rate,
        period_secs: p.period_secs,
    }
}

fn flow_lane_owner(q: &FlowKeyQuery) -> Result<Uuid, FlowApiError> {
    q.group_member_id
        .or(q.flow_profile_id)
        .or(q.queue_id)
        .ok_or(FlowApiError::BadRequest(
            "queue_id, flow_id, or group_member_id query param required".into(),
        ))
}

fn resolved_flow_limits(key: &str, spec: &FlowSpec) -> ResolvedFlow {
    ResolvedFlow::resolve(key, Some(spec), None, None)
}

fn resolved_flow_for_profile(key: &str, profile: &broker_partition::FlowProfile) -> ResolvedFlow {
    resolved_flow_limits(key, &profile.to_spec())
}

async fn flow_limits_for_lane(state: &AppState, key: &str) -> Result<ResolvedFlow, FlowApiError> {
    if let Some(profile) = state
        .broker
        .get_flow_profile_by_key(key)
        .map_err(ApiError::Broker)?
    {
        return Ok(resolved_flow_for_profile(key, &profile));
    }
    Ok(ResolvedFlow::for_queue_default(key))
}

async fn ensure_flow_lane(state: &AppState, owner: Uuid, key: &str) -> Result<(), FlowApiError> {
    let limits = flow_limits_for_lane(state, key).await?;
    state.dispatch.flow.ensure_lane(owner, key, limits).await;
    Ok(())
}

/// Upsert flow profile by key and attach to the publish request.
async fn apply_inline_flow(
    state: &AppState,
    req: &mut PublishRequest,
    flow: &FlowSpec,
) -> Result<(), ApiError> {
    let key = flow.effective_key(&req.routing_key).to_string();
    let parallelism = flow.parallelism.unwrap_or(1).max(1);
    let rate = flow.rate.unwrap_or(0);
    let period_secs = flow.period_secs.unwrap_or(1).max(1);
    let profile =
        state
            .broker
            .upsert_flow_profile_by_key(key.clone(), parallelism, rate, period_secs)?;
    crate::cluster::replicate_flow_catalog(state, profile.clone()).await;
    req.flow_id = Some(profile.id);
    let mut spec = profile.to_spec();
    if spec.key.is_none() {
        spec.key = Some(key.clone());
    }
    req.flow = Some(spec);
    Ok(())
}

async fn publish_job(
    State(state): State<Arc<AppState>>,
    ingest: Option<axum::extract::Extension<crate::metering::IngestAuth>>,
    #[cfg(feature = "cloud")] plan: Option<
        axum::extract::Extension<broker_control_plane::PlanLimits>,
    >,
    Json(req): Json<PublishJobRequest>,
) -> Result<(StatusCode, Json<EnqueueResponse>), ApiError> {
    let inline_flow = req.flow.clone();
    let mut pr = to_publish_job(req);
    if let Some(flow) = inline_flow {
        apply_inline_flow(&state, &mut pr, &flow).await?;
    }
    #[cfg(feature = "cloud")]
    let (status, Json(inner)) = publish(State(state), ingest, plan, Json(pr)).await?;
    #[cfg(not(feature = "cloud"))]
    let (status, Json(inner)) = publish(State(state), ingest, Json(pr)).await?;
    Ok((status, Json(to_enqueue_response(inner))))
}

async fn enqueue(
    State(state): State<Arc<AppState>>,
    ingest: Option<axum::extract::Extension<crate::metering::IngestAuth>>,
    #[cfg(feature = "cloud")] plan: Option<
        axum::extract::Extension<broker_control_plane::PlanLimits>,
    >,
    Json(req): Json<EnqueueRequest>,
) -> Result<(StatusCode, Json<EnqueueResponse>), ApiError> {
    let inline_flow = req.flow.clone();
    let mut pr = to_publish_enqueue(req)?;
    if let Some(flow) = inline_flow {
        apply_inline_flow(&state, &mut pr, &flow).await?;
    }
    #[cfg(feature = "cloud")]
    let (status, Json(inner)) = publish(State(state), ingest, plan, Json(pr)).await?;
    #[cfg(not(feature = "cloud"))]
    let (status, Json(inner)) = publish(State(state), ingest, Json(pr)).await?;
    Ok((status, Json(to_enqueue_response(inner))))
}

async fn enqueue_to_queue(
    State(state): State<Arc<AppState>>,
    ingest: Option<axum::extract::Extension<crate::metering::IngestAuth>>,
    #[cfg(feature = "cloud")] plan: Option<
        axum::extract::Extension<broker_control_plane::PlanLimits>,
    >,
    Path(queue_id): Path<Uuid>,
    Json(mut req): Json<EnqueueRequest>,
) -> Result<(StatusCode, Json<EnqueueResponse>), ApiError> {
    req.queue_id = Some(queue_id);
    #[cfg(feature = "cloud")]
    return enqueue(State(state), ingest, plan, Json(req)).await;
    #[cfg(not(feature = "cloud"))]
    enqueue(State(state), ingest, Json(req)).await
}

async fn list_dlq(
    State(state): State<Arc<AppState>>,
    Query(q): Query<DlqQuery>,
) -> Result<Json<DlqListResponse>, ApiError> {
    let topic = resolve_dlq_topic(&q)?;
    let messages = state
        .broker
        .list_topic_messages(&topic, q.limit)?
        .into_iter()
        .map(to_dlq_message)
        .collect();
    let queue_label = q
        .queue
        .clone()
        .or_else(|| {
            topic
                .strip_suffix(".__dlq")
                .filter(|s| !s.starts_with("__group."))
                .map(str::to_string)
        })
        .unwrap_or_else(|| topic.clone());
    Ok(Json(DlqListResponse {
        queue: queue_label,
        dlq_topic: topic,
        messages,
    }))
}

async fn delete_dlq_message(
    State(state): State<Arc<AppState>>,
    Query(q): Query<DeleteDlqQuery>,
) -> Result<StatusCode, ApiError> {
    if !is_dlq_topic(&q.dlq_topic) {
        return Err(ApiError::BadRequest(
            "dlq_topic must end with .__dlq".into(),
        ));
    }
    let removed = state
        .broker
        .purge_dlq_message(&q.dlq_topic, q.partition, q.offset)
        .map_err(ApiError::Broker)?;
    if removed {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::BadRequest("DLQ message not found".into()))
    }
}

async fn list_dlq_sources(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<DlqSourceResponse>>, ApiError> {
    let topics = collect_dlq_topic_candidates(&state)?;
    let mut sources = Vec::new();
    for topic in topics {
        let count = state.broker.list_topic_messages(&topic, 10_000)?.len();
        let (kind, label, queue) = dlq_source_label(&state, &topic);
        sources.push(DlqSourceResponse {
            dlq_topic: topic,
            kind,
            label,
            count,
            queue,
        });
    }
    sources.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.label.cmp(&b.label)));
    Ok(Json(sources))
}

async fn create_queue(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateQueueRequest>,
) -> Result<(StatusCode, Json<QueueResponse>), ApiError> {
    let (status, Json(inner)) = create_subscription(
        State(state.clone()),
        Json(CreateSubscriptionRequest {
            topic: req.queue,
            url: req.url.clone(),
            secret: req.secret.clone(),
            default_max_retries: req.retry.max_retries,
            retry_backoff: req.retry.retry_backoff.clone(),
        }),
    )
    .await?;
    crate::cluster::replicate_queue_catalog(
        &state,
        Subscription {
            id: inner.id,
            tenant_id: state.broker.config().tenant_id.clone(),
            topic: inner.topic.clone(),
            url: inner.url.clone(),
            secret: req.secret,
            paused: false,
            parallelism: None,
            flow: None,
            default_max_retries: req.retry.max_retries,
            retry_backoff: req.retry.retry_backoff.clone(),
            updated_at_ms: chrono::Utc::now().timestamp_millis(),
        },
    )
    .await;
    Ok((status, Json(to_queue_response(inner))))
}

async fn create_flow(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateFlowRequest>,
) -> Result<(StatusCode, Json<FlowProfileResponse>), ApiError> {
    let p =
        state
            .broker
            .create_flow_profile(req.key, req.parallelism, req.rate, req.period_secs)?;
    crate::cluster::replicate_flow_catalog(&state, p.clone()).await;
    Ok((StatusCode::CREATED, Json(to_flow_response(p))))
}

async fn list_flows(
    State(state): State<Arc<AppState>>,
) -> Result<Json<FlowListResponse>, ApiError> {
    Ok(Json(FlowListResponse {
        flows: state
            .broker
            .list_flow_profiles()?
            .into_iter()
            .map(to_flow_response)
            .collect(),
    }))
}

async fn delete_flow(
    State(state): State<Arc<AppState>>,
    Path(flow_id): Path<Uuid>,
) -> Result<(StatusCode, Json<FlowProfileResponse>), ApiError> {
    let p = state.broker.delete_flow_profile(flow_id)?;
    crate::cluster::replicate_flow_delete(&state, flow_id).await;
    Ok((StatusCode::OK, Json(to_flow_response(p))))
}

async fn list_queues(
    State(state): State<Arc<AppState>>,
) -> Result<Json<QueueListResponse>, ApiError> {
    let queues = state
        .broker
        .list_endpoints()?
        .into_iter()
        .map(|s| QueueResponse {
            queue_id: s.id,
            queue: s.topic,
            url: s.url,
        })
        .collect();
    Ok(Json(QueueListResponse { queues }))
}

async fn delete_queue(
    State(state): State<Arc<AppState>>,
    Path(queue_id): Path<Uuid>,
) -> Result<(StatusCode, Json<QueueResponse>), ApiError> {
    let s = state.broker.delete_endpoint(queue_id)?;
    crate::cluster::replicate_queue_delete(&state, queue_id).await;
    Ok((
        StatusCode::OK,
        Json(QueueResponse {
            queue_id: s.id,
            queue: s.topic,
            url: s.url,
        }),
    ))
}

#[derive(Debug, Serialize)]
pub struct QueueListResponse {
    pub queues: Vec<QueueResponse>,
}

#[derive(Debug, Serialize)]
pub struct DelayedJobResponse {
    pub schedule_id: Uuid,
    pub queue: String,
    pub key: String,
    pub deliver_at_ms: i64,
    pub body: String,
}

#[derive(Debug, Serialize)]
pub struct DelayedListResponse {
    pub delayed: Vec<DelayedJobResponse>,
}

fn to_delayed_response(item: ScheduledPublish) -> DelayedJobResponse {
    DelayedJobResponse {
        schedule_id: item.id,
        queue: item.request.topic,
        key: item.request.routing_key,
        deliver_at_ms: item.deliver_at_ms,
        body: item.request.payload,
    }
}

async fn list_delayed(State(state): State<Arc<AppState>>) -> Json<DelayedListResponse> {
    Json(DelayedListResponse {
        delayed: state
            .schedule
            .list()
            .into_iter()
            .map(to_delayed_response)
            .collect(),
    })
}

async fn cancel_delayed(
    State(state): State<Arc<AppState>>,
    Path(schedule_id): Path<Uuid>,
) -> Result<(StatusCode, Json<DelayedJobResponse>), DelayedApiError> {
    let removed = state
        .schedule
        .cancel(schedule_id)
        .map_err(DelayedApiError::from)?;
    Ok((StatusCode::OK, Json(to_delayed_response(removed))))
}

#[derive(Debug)]
enum DelayedApiError {
    NotFound(Uuid),
    Internal(String),
}

impl From<ScheduleError> for DelayedApiError {
    fn from(e: ScheduleError) -> Self {
        match e {
            ScheduleError::NotFound(id) => DelayedApiError::NotFound(id),
            ScheduleError::Io(e) => DelayedApiError::Internal(e.to_string()),
            ScheduleError::Serde(e) => DelayedApiError::Internal(e.to_string()),
            ScheduleError::MetadataLoad(e) => DelayedApiError::Internal(e.to_string()),
        }
    }
}

impl IntoResponse for DelayedApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, msg) = match self {
            DelayedApiError::NotFound(id) => (
                StatusCode::NOT_FOUND,
                format!("delayed job not found: {id}"),
            ),
            DelayedApiError::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
        };
        (status, Json(serde_json::json!({ "error": msg }))).into_response()
    }
}

async fn get_flow(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    Query(q): Query<FlowKeyQuery>,
) -> Result<Json<FlowControlInfo>, FlowApiError> {
    let owner = flow_lane_owner(&q)?;
    if state.dispatch.flow.get(owner, &key).await.is_none() {
        ensure_flow_lane(&state, owner, &key).await?;
    }
    let mut info = state
        .dispatch
        .flow
        .get(owner, &key)
        .await
        .ok_or(FlowApiError::NotFound)?;
    info.endpoint_id = owner;
    Ok(Json(info))
}

async fn upsert_flow_control(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    Query(q): Query<FlowKeyQuery>,
    Json(body): Json<UpsertFlowControlRequest>,
) -> Result<(StatusCode, Json<FlowProfileResponse>), FlowApiError> {
    let owner = flow_lane_owner(&q)?;
    let profile = state
        .broker
        .upsert_flow_profile_by_key(key.clone(), body.parallelism, body.rate, body.period_secs)
        .map_err(ApiError::Broker)?;
    crate::cluster::replicate_flow_catalog(&state, profile.clone()).await;
    let limits = resolved_flow_for_profile(&key, &profile);
    state.dispatch.flow.ensure_lane(owner, &key, limits).await;
    Ok((StatusCode::OK, Json(to_flow_response(profile))))
}

async fn pause_flow(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    Query(q): Query<FlowKeyQuery>,
) -> Result<StatusCode, FlowApiError> {
    let owner = flow_lane_owner(&q)?;
    ensure_flow_lane(&state, owner, &key).await?;
    if state.dispatch.flow.pause(owner, &key).await {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(FlowApiError::NotFound)
    }
}

async fn resume_flow(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    Query(q): Query<FlowKeyQuery>,
) -> Result<StatusCode, FlowApiError> {
    let owner = flow_lane_owner(&q)?;
    ensure_flow_lane(&state, owner, &key).await?;
    if state.dispatch.flow.resume(owner, &key).await {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(FlowApiError::NotFound)
    }
}

async fn reset_flow_rate(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    Query(q): Query<FlowKeyQuery>,
) -> Result<StatusCode, FlowApiError> {
    let owner = flow_lane_owner(&q)?;
    ensure_flow_lane(&state, owner, &key).await?;
    if state.dispatch.flow.reset_rate(owner, &key).await {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(FlowApiError::NotFound)
    }
}

async fn pin_flow(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    Query(q): Query<FlowKeyQuery>,
    Json(body): Json<PinFlowRequest>,
) -> Result<StatusCode, FlowApiError> {
    let owner = flow_lane_owner(&q)?;
    ensure_flow_lane(&state, owner, &key).await?;
    if state
        .dispatch
        .flow
        .pin(owner, &key, body.parallelism, body.rate, body.period_secs)
        .await
    {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(FlowApiError::NotFound)
    }
}

async fn unpin_flow(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    Query(q): Query<FlowKeyQuery>,
    Json(body): Json<UnpinFlowRequest>,
) -> Result<StatusCode, FlowApiError> {
    let owner = flow_lane_owner(&q)?;
    ensure_flow_lane(&state, owner, &key).await?;
    if state
        .dispatch
        .flow
        .unpin(owner, &key, body.parallelism, body.rate)
        .await
    {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(FlowApiError::NotFound)
    }
}

async fn global_flow(State(state): State<Arc<AppState>>) -> Json<GlobalParallelismInfo> {
    Json(state.dispatch.flow.global_parallelism().await)
}

// --- Cron schedules ---

#[derive(Debug, Deserialize)]
pub struct CreateCronRequest {
    /// Cron expression (5 fields: `min hour dom month dow`, or 6 with leading seconds). UTC.
    /// Provide **either** `cron` or `every_seconds`, not both.
    #[serde(default)]
    pub cron: Option<String>,
    /// Fixed interval between runs (seconds). Provide **either** this or `cron`.
    #[serde(default)]
    pub every_seconds: Option<u64>,
    /// Direct webhook URL (schedule job — like publish). Preferred over queue.
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub secret: Option<String>,
    /// Optional: enqueue to a named queue instead of `url`.
    #[serde(default)]
    pub queue: Option<String>,
    #[serde(default)]
    pub key: String,
    #[serde(deserialize_with = "broker_partition::payload::deserialize_flexible_payload")]
    pub body: String,
    #[serde(default)]
    pub body_encoding: Option<String>,
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub priority: Option<u8>,
    #[serde(default)]
    pub queue_id: Option<Uuid>,
    #[serde(default)]
    pub flow_id: Option<Uuid>,
    #[serde(flatten)]
    pub retry: RetryInput,
    #[serde(flatten)]
    pub outbound: OutboundHttpFields,
}

#[derive(Debug, Serialize)]
pub struct CronResponse {
    pub cron_id: Uuid,
    /// `cron` or `interval`
    pub schedule_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub every_seconds: Option<u64>,
    pub queue: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination_url: Option<String>,
    pub paused: bool,
    pub next_run_at_ms: i64,
    pub created_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run_at_ms: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct CronListResponse {
    pub crons: Vec<CronResponse>,
}

fn to_cron_response(job: CronJob) -> CronResponse {
    let (schedule_type, cron, every_seconds) = if let Some(secs) = job.every_seconds {
        ("interval".into(), None, Some(secs))
    } else {
        ("cron".into(), Some(job.cron.clone()), None)
    };
    CronResponse {
        cron_id: job.id,
        schedule_type,
        cron,
        every_seconds,
        queue: job.request.topic,
        destination_url: job.request.destination.as_ref().map(|d| d.url.clone()),
        paused: job.paused,
        next_run_at_ms: job.next_run_at_ms,
        created_at_ms: job.created_at_ms,
        last_run_at_ms: job.last_run_at_ms,
    }
}

fn parse_schedule_kind(
    req: &CreateCronRequest,
) -> Result<broker_schedule::ScheduleKind, CronApiError> {
    let has_cron = req
        .cron
        .as_ref()
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false);
    let has_interval = req.every_seconds.is_some();

    match (has_cron, has_interval) {
        (true, true) => Err(CronApiError::BadRequest(
            "provide either cron or every_seconds, not both".into(),
        )),
        (false, false) => Err(CronApiError::BadRequest(
            "provide cron or every_seconds".into(),
        )),
        (true, false) => Ok(broker_schedule::ScheduleKind::from_cron(
            req.cron.as_ref().unwrap().trim(),
        )),
        (false, true) => broker_schedule::ScheduleKind::from_interval(req.every_seconds.unwrap())
            .map_err(|e| CronApiError::InvalidCron(e.to_string())),
    }
}

async fn build_cron_scheduled_request(
    state: &Arc<AppState>,
    req: &CreateCronRequest,
) -> Result<ScheduledPublishRequest, CronApiError> {
    let url = req.url.as_deref().unwrap_or("").trim();
    let secret = req.secret.as_deref().unwrap_or("").trim();
    if !url.is_empty() && !secret.is_empty() {
        let mut scheduled = ScheduledPublishRequest {
            topic: DIRECT_TOPIC.to_string(),
            routing_key: req.key.clone(),
            payload: req.body.clone(),
            payload_encoding: req.body_encoding.clone(),
            idempotency_key: req.idempotency_key.clone(),
            priority: req.priority,
            flow_id: req.flow_id,
            queue_id: None,
            destination: Some(DestinationSnapshot {
                queue_id: None,
                url: url.to_string(),
                secret: secret.to_string(),
            }),
            flow: None,
            parallelism: None,
            max_retries: req.retry.max_retries,
            retry_backoff: req.retry.retry_backoff.clone(),
            method: None,
            headers: None,
            sign: None,
            request: None,
        };
        req.outbound.apply_to_scheduled(&mut scheduled);
        return Ok(scheduled);
    }

    let queue_name = req.queue.as_deref().unwrap_or("").trim();
    if req.queue_id.is_none() && queue_name.is_empty() {
        return Err(CronApiError::BadRequest(
            "provide url and secret for a schedule job (or queue_id / queue for enqueue-style)"
                .into(),
        ));
    }
    let destination =
        crate::routes::resolve_destination_with_repair(state, req.queue_id, queue_name)
            .await
            .map_err(|e| match e {
                ApiError::Broker(be) => CronApiError::BadRequest(be.to_string()),
                ApiError::BadRequest(m) => CronApiError::BadRequest(m),
                ApiError::ReplicationFailed(m) => CronApiError::BadRequest(m),
            })?;
    let mut scheduled = ScheduledPublishRequest {
        topic: destination
            .queue_id
            .and_then(|id| state.broker.get_queue_by_id(id).ok().flatten())
            .map(|q| q.topic)
            .unwrap_or_else(|| queue_name.to_string()),
        routing_key: req.key.clone(),
        payload: req.body.clone(),
        payload_encoding: req.body_encoding.clone(),
        idempotency_key: req.idempotency_key.clone(),
        priority: req.priority,
        flow_id: req.flow_id,
        queue_id: req.queue_id,
        destination: Some(destination),
        flow: None,
        parallelism: None,
        max_retries: req.retry.max_retries,
        retry_backoff: req.retry.retry_backoff.clone(),
        method: None,
        headers: None,
        sign: None,
        request: None,
    };
    req.outbound.apply_to_scheduled(&mut scheduled);
    Ok(scheduled)
}

async fn create_cron(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateCronRequest>,
) -> Result<(StatusCode, Json<CronResponse>), CronApiError> {
    let schedule = parse_schedule_kind(&req)?;
    let scheduled = build_cron_scheduled_request(&state, &req).await?;
    let job = state.crons.create_with_kind(schedule, scheduled)?;
    crate::cluster::replicate_cron_catalog(&state, job.clone()).await;
    Ok((StatusCode::CREATED, Json(to_cron_response(job))))
}

async fn list_crons(State(state): State<Arc<AppState>>) -> Json<CronListResponse> {
    Json(CronListResponse {
        crons: state
            .crons
            .list()
            .into_iter()
            .map(to_cron_response)
            .collect(),
    })
}

async fn get_cron(
    State(state): State<Arc<AppState>>,
    Path(cron_id): Path<Uuid>,
) -> Result<Json<CronResponse>, CronApiError> {
    Ok(Json(to_cron_response(state.crons.get(cron_id)?)))
}

async fn pause_cron(
    State(state): State<Arc<AppState>>,
    Path(cron_id): Path<Uuid>,
) -> Result<Json<CronResponse>, CronApiError> {
    let job = state.crons.pause(cron_id)?;
    crate::cluster::replicate_cron_catalog(&state, job.clone()).await;
    Ok(Json(to_cron_response(job)))
}

async fn resume_cron(
    State(state): State<Arc<AppState>>,
    Path(cron_id): Path<Uuid>,
) -> Result<Json<CronResponse>, CronApiError> {
    let job = state.crons.resume(cron_id)?;
    crate::cluster::replicate_cron_catalog(&state, job.clone()).await;
    Ok(Json(to_cron_response(job)))
}

async fn delete_cron(
    State(state): State<Arc<AppState>>,
    Path(cron_id): Path<Uuid>,
) -> Result<(StatusCode, Json<CronResponse>), CronApiError> {
    let removed = state.crons.delete(cron_id)?;
    crate::cluster::replicate_cron_delete(&state, cron_id).await;
    Ok((StatusCode::OK, Json(to_cron_response(removed))))
}

#[derive(Debug)]
enum CronApiError {
    InvalidCron(String),
    BadRequest(String),
    NotFound(Uuid),
    Internal(String),
}

impl From<CronError> for CronApiError {
    fn from(e: CronError) -> Self {
        match e {
            CronError::NotFound(id) => CronApiError::NotFound(id),
            CronError::InvalidExpression(msg) | CronError::InvalidSchedule(msg) => {
                CronApiError::InvalidCron(msg)
            }
            CronError::Io(e) => CronApiError::Internal(e.to_string()),
            CronError::Serde(e) => CronApiError::Internal(e.to_string()),
            CronError::MetadataLoad(e) => CronApiError::Internal(e.to_string()),
        }
    }
}

impl IntoResponse for CronApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, msg) = match self {
            CronApiError::InvalidCron(m) | CronApiError::BadRequest(m) => {
                (StatusCode::BAD_REQUEST, m)
            }
            CronApiError::NotFound(id) => (StatusCode::NOT_FOUND, format!("cron not found: {id}")),
            CronApiError::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
        };
        let body = serde_json::json!({ "error": msg });
        (status, Json(body)).into_response()
    }
}

#[derive(Debug)]
enum FlowApiError {
    NotFound,
    BadRequest(String),
    Broker(ApiError),
}

impl From<ApiError> for FlowApiError {
    fn from(e: ApiError) -> Self {
        FlowApiError::Broker(e)
    }
}

impl IntoResponse for FlowApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, msg) = match self {
            FlowApiError::NotFound => (
                StatusCode::NOT_FOUND,
                "flow key not found — upsert with PUT /v1/flow/{key} or enqueue with inline flow"
                    .to_string(),
            ),
            FlowApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            FlowApiError::Broker(e) => return e.into_response(),
        };
        let body = serde_json::json!({ "error": msg });
        (status, Json(body)).into_response()
    }
}
