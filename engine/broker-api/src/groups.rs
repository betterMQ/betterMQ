//! Fan-out groups API — one publish delivers to many webhook destinations.

use crate::http_fields::OutboundHttpFields;
use crate::routes::ApiError;
use crate::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post, put},
    Json, Router,
};
use broker_partition::{BrokerError, DispatchGroup, GroupMember, PublishRequest, PublishResponse};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

pub fn group_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/v1/groups", get(list_groups).post(create_group))
        .route(
            "/v1/groups/{group_id}",
            get(get_group).delete(delete_group_handler),
        )
        .route(
            "/v1/groups/{group_id}/members",
            get(list_members).post(add_member),
        )
        .route(
            "/v1/groups/{group_id}/members/{member_id}",
            put(update_member).delete(delete_member_handler),
        )
        .route("/v1/groups/{group_id}/publish", post(publish_group))
}

#[derive(Debug, Deserialize, Default, Clone)]
pub(crate) struct RetryInput {
    #[serde(default)]
    max_retries: Option<u32>,
    #[serde(default)]
    retry_backoff: Option<broker_proto::RetryBackoff>,
}

#[derive(Debug, Deserialize)]
pub struct CreateGroupRequest {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct AddMemberRequest {
    pub name: String,
    pub url: String,
    pub secret: String,
    #[serde(default = "default_parallelism")]
    pub parallelism: u32,
    #[serde(default)]
    pub rate: u32,
    #[serde(default = "default_period")]
    pub period_secs: u64,
    #[serde(default)]
    pub flow_key: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateMemberRequest {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub secret: Option<String>,
    #[serde(default)]
    pub parallelism: Option<u32>,
    #[serde(default)]
    pub rate: Option<u32>,
    #[serde(default)]
    pub period_secs: Option<u64>,
    #[serde(default)]
    pub flow_key: Option<Option<String>>,
    #[serde(default)]
    pub paused: Option<bool>,
}

fn default_parallelism() -> u32 {
    1
}

fn default_period() -> u64 {
    60
}

#[derive(Debug, Deserialize)]
pub struct GroupPublishRequest {
    #[serde(default)]
    pub key: String,
    #[serde(deserialize_with = "broker_partition::payload::deserialize_flexible_payload")]
    pub body: String,
    #[serde(default)]
    pub body_encoding: Option<String>,
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub priority: Option<u8>,
    #[serde(flatten)]
    pub retry: RetryInput,
    #[serde(flatten)]
    pub outbound: OutboundHttpFields,
}

#[derive(Debug, Serialize)]
pub struct GroupResponse {
    pub group_id: Uuid,
    pub name: String,
    pub paused: bool,
}

#[derive(Debug, Serialize)]
pub struct GroupListResponse {
    pub groups: Vec<GroupResponse>,
}

#[derive(Debug, Serialize)]
pub struct GroupDetailResponse {
    pub group: GroupResponse,
    pub members: Vec<MemberResponse>,
}

#[derive(Debug, Serialize)]
pub struct MemberResponse {
    pub member_id: Uuid,
    pub group_id: Uuid,
    pub name: String,
    pub url: String,
    pub paused: bool,
    pub parallelism: u32,
    pub rate: u32,
    pub period_secs: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flow_key: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct MemberListResponse {
    pub members: Vec<MemberResponse>,
}

#[derive(Debug, Serialize)]
pub struct GroupPublishResponse {
    pub group_id: Uuid,
    pub accepted: usize,
    pub deliveries: Vec<GroupDeliveryRef>,
}

#[derive(Debug, Serialize)]
pub struct GroupDeliveryRef {
    pub member_id: Uuid,
    pub message_id: Option<Uuid>,
    pub duplicate: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
}

fn to_group_response(g: DispatchGroup) -> GroupResponse {
    GroupResponse {
        group_id: g.id,
        name: g.name,
        paused: g.paused,
    }
}

fn to_member_response(m: GroupMember) -> MemberResponse {
    MemberResponse {
        member_id: m.id,
        group_id: m.group_id,
        name: m.name,
        url: m.url,
        paused: m.paused,
        parallelism: m.parallelism,
        rate: m.rate,
        period_secs: m.period_secs,
        flow_key: m.flow_key,
    }
}

async fn list_groups(
    State(state): State<Arc<AppState>>,
) -> Result<Json<GroupListResponse>, ApiError> {
    Ok(Json(GroupListResponse {
        groups: state
            .broker
            .list_groups()?
            .into_iter()
            .map(to_group_response)
            .collect(),
    }))
}

async fn create_group(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateGroupRequest>,
) -> Result<(StatusCode, Json<GroupResponse>), GroupApiError> {
    let group = state.broker.create_group(req.name)?;
    crate::cluster::replicate_group_catalog(&state, group.clone()).await;
    Ok((StatusCode::CREATED, Json(to_group_response(group))))
}

async fn get_group(
    State(state): State<Arc<AppState>>,
    Path(group_id): Path<Uuid>,
) -> Result<Json<GroupDetailResponse>, GroupApiError> {
    let group = state
        .broker
        .get_group(group_id)?
        .ok_or(GroupApiError::GroupNotFound(group_id))?;
    let members = state.broker.list_group_members(group_id)?;
    Ok(Json(GroupDetailResponse {
        group: to_group_response(group),
        members: members.into_iter().map(to_member_response).collect(),
    }))
}

async fn delete_group_handler(
    State(state): State<Arc<AppState>>,
    Path(group_id): Path<Uuid>,
) -> Result<(StatusCode, Json<GroupResponse>), GroupApiError> {
    let members = state.broker.list_group_members(group_id)?;
    let removed = state.broker.delete_group(group_id)?;
    crate::cluster::replicate_group_delete(&state, group_id).await;
    for member in members {
        crate::cluster::replicate_group_member_delete(&state, member.id).await;
    }
    Ok((StatusCode::OK, Json(to_group_response(removed))))
}

async fn list_members(
    State(state): State<Arc<AppState>>,
    Path(group_id): Path<Uuid>,
) -> Result<Json<MemberListResponse>, GroupApiError> {
    if state.broker.get_group(group_id)?.is_none() {
        return Err(GroupApiError::GroupNotFound(group_id));
    }
    Ok(Json(MemberListResponse {
        members: state
            .broker
            .list_group_members(group_id)?
            .into_iter()
            .map(to_member_response)
            .collect(),
    }))
}

async fn add_member(
    State(state): State<Arc<AppState>>,
    Path(group_id): Path<Uuid>,
    Json(req): Json<AddMemberRequest>,
) -> Result<(StatusCode, Json<MemberResponse>), GroupApiError> {
    let member = state.broker.add_group_member(
        group_id,
        req.name,
        req.url,
        req.secret,
        req.parallelism,
        req.rate,
        req.period_secs,
        req.flow_key,
    )?;
    crate::cluster::replicate_group_member_catalog(&state, member.clone()).await;
    Ok((StatusCode::CREATED, Json(to_member_response(member))))
}

async fn update_member(
    State(state): State<Arc<AppState>>,
    Path((group_id, member_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<UpdateMemberRequest>,
) -> Result<Json<MemberResponse>, GroupApiError> {
    let mut member = state
        .broker
        .get_group_member(member_id)?
        .ok_or(GroupApiError::MemberNotFound(member_id))?;
    if member.group_id != group_id {
        return Err(GroupApiError::MemberNotFound(member_id));
    }
    if let Some(name) = req.name {
        member.name = name;
    }
    if let Some(url) = req.url {
        member.url = url;
    }
    if let Some(secret) = req.secret {
        member.secret = secret;
    }
    if let Some(p) = req.parallelism {
        member.parallelism = p.max(1);
    }
    if let Some(rate) = req.rate {
        member.rate = rate;
    }
    if let Some(period) = req.period_secs {
        member.period_secs = period.max(1);
    }
    if let Some(flow_key) = req.flow_key {
        member.flow_key = flow_key;
    }
    if let Some(paused) = req.paused {
        member.paused = paused;
    }
    member.updated_at_ms = chrono::Utc::now().timestamp_millis();
    state.broker.upsert_group_member_catalog(member.clone())?;
    crate::cluster::replicate_group_member_catalog(&state, member.clone()).await;
    Ok(Json(to_member_response(member)))
}

async fn delete_member_handler(
    State(state): State<Arc<AppState>>,
    Path((group_id, member_id)): Path<(Uuid, Uuid)>,
) -> Result<(StatusCode, Json<MemberResponse>), GroupApiError> {
    let member = state
        .broker
        .get_group_member(member_id)?
        .ok_or(GroupApiError::MemberNotFound(member_id))?;
    if member.group_id != group_id {
        return Err(GroupApiError::MemberNotFound(member_id));
    }
    let removed = state.broker.delete_group_member(member_id)?;
    crate::cluster::replicate_group_member_delete(&state, member_id).await;
    Ok((StatusCode::OK, Json(to_member_response(removed))))
}

async fn publish_group(
    State(state): State<Arc<AppState>>,
    ingest: Option<axum::extract::Extension<crate::metering::IngestAuth>>,
    #[cfg(feature = "cloud")] plan: Option<
        axum::extract::Extension<broker_control_plane::PlanLimits>,
    >,
    Path(group_id): Path<Uuid>,
    Json(req): Json<GroupPublishRequest>,
) -> Result<(StatusCode, Json<GroupPublishResponse>), GroupApiError> {
    if state.broker.get_group(group_id)?.is_none() {
        return Err(GroupApiError::GroupNotFound(group_id));
    }
    let members = state.broker.active_group_members(group_id)?;
    if members.is_empty() {
        return Err(GroupApiError::NoActiveMembers);
    }

    let mut base = PublishRequest {
        topic: String::new(),
        queue_id: None,
        group_id: None,
        group_member_id: None,
        routing_key: req.key,
        payload: req.body,
        payload_encoding: req.body_encoding,
        idempotency_key: req.idempotency_key,
        delay_ms: None,
        priority: req.priority,
        flow_id: None,
        url: None,
        secret: None,
        destination: None,
        flow: None,
        parallelism: None,
        max_retries: req.retry.max_retries,
        retry_backoff: req.retry.retry_backoff,
        method: None,
        headers: None,
        sign: None,
        request: None,
    };
    req.outbound.apply_to(&mut base);

    #[cfg(feature = "cloud")]
    if state.uses_cloud_auth() {
        if let (Some(auth), Some(plan)) =
            (ingest.as_ref().map(|e| e.0), plan.as_ref().map(|e| &e.0))
        {
            let fanout = members.len();
            crate::metering::check_cloud_batch_messages_cap(&state, auth.tenant_id, plan, fanout)
                .await
                .map_err(GroupApiError::Publish)?;
            if req.body.len() as u64 > plan.max_message_bytes {
                return Err(GroupApiError::Publish(crate::routes::ApiError::BadRequest(
                    format!(
                        "message exceeds plan limit of {} bytes",
                        plan.max_message_bytes
                    ),
                )));
            }
        }
    }

    let mut deliveries = Vec::new();
    for member in members {
        let member_req = state
            .broker
            .group_member_publish_request(group_id, &member, &base);
        let meter = crate::metering::ingest_meter(ingest.map(|e| e.0), member_req.payload.len());
        let resp = crate::cluster::publish_with_cluster(&state, member_req, meter)
            .await
            .map_err(GroupApiError::Publish)?;
        crate::cluster::enqueue_dispatch_after_publish(&state, &resp);
        deliveries.push(to_delivery_ref(member.id, &resp));
    }

    let accepted = deliveries.iter().filter(|d| !d.duplicate).count();
    Ok((
        StatusCode::ACCEPTED,
        Json(GroupPublishResponse {
            group_id,
            accepted,
            deliveries,
        }),
    ))
}

fn to_delivery_ref(member_id: Uuid, resp: &PublishResponse) -> GroupDeliveryRef {
    GroupDeliveryRef {
        member_id,
        message_id: resp.message_id,
        duplicate: resp.duplicate,
        shard: resp.partition,
        seq: resp.offset,
    }
}

#[derive(Debug)]
enum GroupApiError {
    GroupNotFound(Uuid),
    MemberNotFound(Uuid),
    NoActiveMembers,
    DuplicateName(String),
    Publish(ApiError),
}

impl From<BrokerError> for GroupApiError {
    fn from(e: BrokerError) -> Self {
        match e {
            BrokerError::Group(broker_partition::GroupError::DuplicateName(n)) => {
                GroupApiError::DuplicateName(n)
            }
            BrokerError::Group(broker_partition::GroupError::GroupNotFound(id)) => {
                GroupApiError::GroupNotFound(id)
            }
            BrokerError::Group(broker_partition::GroupError::MemberNotFound(id)) => {
                GroupApiError::MemberNotFound(id)
            }
            BrokerError::Group(broker_partition::GroupError::NoActiveMembers) => {
                GroupApiError::NoActiveMembers
            }
            other => GroupApiError::Publish(ApiError::Broker(other)),
        }
    }
}

impl IntoResponse for GroupApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, msg) = match self {
            GroupApiError::GroupNotFound(id) => {
                (StatusCode::NOT_FOUND, format!("group not found: {id}"))
            }
            GroupApiError::MemberNotFound(id) => {
                (StatusCode::NOT_FOUND, format!("member not found: {id}"))
            }
            GroupApiError::NoActiveMembers => (
                StatusCode::BAD_REQUEST,
                "group has no active members".to_string(),
            ),
            GroupApiError::DuplicateName(n) => {
                (StatusCode::CONFLICT, format!("duplicate group name: {n}"))
            }
            GroupApiError::Publish(e) => return e.into_response(),
        };
        let body = serde_json::json!({ "error": msg });
        (status, Json(body)).into_response()
    }
}
