//! Fan-out groups API — manage groups and members (publish via `POST /v1/publish` with `group_id`).

use crate::routes::ApiError;
use crate::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, put},
    Json, Router,
};
use broker_partition::{BrokerError, DispatchGroup, GroupMember};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

pub fn group_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/v1/groups", get(list_groups).post(create_group))
        .route(
            "/v1/groups/{group_id}",
            get(get_group)
                .put(update_group)
                .delete(delete_group_handler),
        )
        .route(
            "/v1/groups/{group_id}/members",
            get(list_members).post(add_member),
        )
        .route(
            "/v1/groups/{group_id}/members/{member_id}",
            put(update_member).delete(delete_member_handler),
        )
}

#[derive(Debug, Deserialize)]
pub struct CreateGroupRequest {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateGroupRequest {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub paused: Option<bool>,
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

async fn update_group(
    State(state): State<Arc<AppState>>,
    Path(group_id): Path<Uuid>,
    Json(req): Json<UpdateGroupRequest>,
) -> Result<Json<GroupResponse>, GroupApiError> {
    let mut group = state
        .broker
        .get_group(group_id)?
        .ok_or(GroupApiError::GroupNotFound(group_id))?;
    if let Some(name) = req.name {
        let name = name.trim().to_string();
        if name.is_empty() {
            return Err(GroupApiError::Invalid("name must not be empty".into()));
        }
        group.name = name;
    }
    if let Some(paused) = req.paused {
        group.paused = paused;
    }
    group.updated_at_ms = chrono::Utc::now().timestamp_millis();
    state.broker.upsert_group_catalog(group.clone())?;
    crate::cluster::replicate_group_catalog(&state, group.clone()).await;
    Ok(Json(to_group_response(group)))
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
    broker_dispatch::validate_destination_url(&req.url)
        .map_err(|e| GroupApiError::Invalid(e.to_string()))?;
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
        broker_dispatch::validate_destination_url(&url)
            .map_err(|e| GroupApiError::Invalid(e.to_string()))?;
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

#[derive(Debug)]
enum GroupApiError {
    GroupNotFound(Uuid),
    MemberNotFound(Uuid),
    NoActiveMembers,
    DuplicateName(String),
    Invalid(String),
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
            GroupApiError::Invalid(msg) => (StatusCode::BAD_REQUEST, msg),
            GroupApiError::Publish(e) => return e.into_response(),
        };
        let body = serde_json::json!({ "error": msg });
        (status, Json(body)).into_response()
    }
}
