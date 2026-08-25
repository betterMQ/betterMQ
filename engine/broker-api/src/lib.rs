//! Public HTTP surface for the BetterMQ data plane.

mod admin;
mod admission;
mod auth;
mod batch;
mod bettermq;
mod catalog_tombstones;
mod cluster;
pub mod cluster_auth;
mod fanout;
mod gateway;
mod groups;
mod http_fields;
mod infra;
mod ingest;
mod lease;
mod local_auth;
mod metering;
mod metrics;
mod ops;
mod publish_path;
mod rate_limit;
mod routes;

use axum::{extract::DefaultBodyLimit, routing::get, Json, Router};
use broker_dispatch::{DispatchEngine, LeaseTable};
use broker_partition::Broker;
use broker_schedule::{CronRegistry, ScheduleQueue};
use cluster::ClusterHandle;
use serde::Serialize;
use std::sync::Arc;

/// HTTP body cap (cloud plan ceiling or self-host default). Override with BETTERMQ_MAX_HTTP_BODY_BYTES.
fn http_body_limit_bytes() -> usize {
    if let Some(n) = std::env::var("BETTERMQ_MAX_HTTP_BODY_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        return n;
    }
    #[cfg(feature = "cloud")]
    if let Some(n) = std::env::var("BETTERMQ_MAX_MESSAGE_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        return n;
    }
    // Self-host / default: 64 MiB (was unbounded — DoS risk).
    64 * 1024 * 1024
}

pub use admin::{
    router as admin_router, validate_attach_url, AdminState, CellMember, CellRecord, CellRegistry,
};
pub use catalog_tombstones::CatalogTombstones;
pub use cluster::{
    build_cluster_status, catalog_peer_targets, enqueue_dispatch_after_publish,
    publish_with_cluster, push_catalog_to_recovered_peer, spawn_cluster_catalog_sync,
    sync_catalog_from_peers, ClusterGossipRequest, ClusterGossipResponse, ClusterHandle as Cluster,
    ClusterStatusResponse,
};
pub use gateway::GatewayOnlyState;
pub use local_auth::{close_setup_window, open_setup_window};

/// Reject new ingest while allowing already-admitted requests to drain.
pub fn begin_shutdown() {
    admission::begin_shutdown();
}

#[derive(Clone)]
pub struct AppState {
    pub broker: Broker,
    pub schedule: ScheduleQueue,
    pub crons: CronRegistry,
    pub dispatch: DispatchEngine,
    pub leases: LeaseTable,
    pub cluster: Option<ClusterHandle>,
    pub local_auth: Option<Arc<broker_local_auth::LocalAuthStore>>,
    pub fair_queue: Arc<broker_dispatch::TenantFairQueue>,
    pub catalog_tombstones: CatalogTombstones,
    /// When true, public ingest routes are not mounted (fleet mode).
    pub dispatch_fleet: bool,
    /// When true, local delivery workers are not the primary path (broker-only).
    pub broker_only: bool,
    #[cfg(feature = "cloud")]
    pub auth: Option<broker_control_plane::ApiKeyValidator>,
    #[cfg(feature = "cloud")]
    pub control_plane: Option<broker_control_plane::ControlPlanePool>,
}

impl AppState {
    /// Postgres API-key auth (BetterMQ Cloud edition only).
    pub fn uses_cloud_auth(&self) -> bool {
        #[cfg(feature = "cloud")]
        {
            return self.auth.is_some();
        }
        #[cfg(not(feature = "cloud"))]
        {
            false
        }
    }
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub version: &'static str,
    pub protocol: u32,
}

/// Builds the data-plane HTTP router.
pub fn router(state: AppState) -> Router {
    let dispatch_fleet = state.dispatch_fleet;
    let shared = Arc::new(state);
    bettermq::spawn_fanout_replay(shared.clone());
    bettermq::spawn_dlq_retention(shared.clone());

    let protected = if dispatch_fleet {
        Router::new()
    } else {
        Router::new()
            .route(
                "/v1/enqueue/batch",
                axum::routing::post(batch::batch_enqueue),
            )
            .route(
                "/v1/ingest/batch",
                axum::routing::post(batch::batch_enqueue_ht),
            )
            .route(
                "/v1/ingest/ndjson",
                axum::routing::post(batch::batch_enqueue_ndjson),
            )
            .route(
                "/v1/gateway/enqueue",
                axum::routing::post(gateway::gateway_enqueue),
            )
            .merge(bettermq::bettermq_routes())
            .merge(groups::group_routes())
            .route("/v1/destinations/blocked", get(ops::list_blocked_hosts))
            .route(
                "/v1/destinations/block",
                axum::routing::post(ops::block_host),
            )
            .route(
                "/v1/destinations/unblock",
                axum::routing::post(ops::unblock_host),
            )
            .merge(infra::protected_infra_routes())
            .route("/v1/ops/aggregate", get(ops::aggregate_ops_status))
            .route_layer(axum::middleware::from_fn_with_state(
                shared.clone(),
                auth::check_ingest_limits,
            ))
            .route_layer(axum::middleware::from_fn_with_state(
                shared.clone(),
                auth::require_api_key,
            ))
    };

    let internal = internal_cluster_routes();

    let public = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(ops::readyz))
        .route("/metrics", get(ops::metrics))
        .route("/metrics/prometheus", get(ops::metrics_prometheus))
        .merge(internal)
        .merge(local_auth::routes())
        .merge(infra::public_infra_routes());

    let app = public
        .merge(protected)
        .layer(DefaultBodyLimit::max(http_body_limit_bytes()));
    app.with_state(shared)
}

/// Public data-plane listener. Combined with internal on collapsed binds.
pub fn public_router(state: AppState) -> Router {
    router(state)
}

/// Internal cluster/replication/controller listener.
pub fn internal_router(state: AppState) -> Router {
    router(state)
}

fn internal_cluster_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/internal/v1/lease/claim",
            axum::routing::post(lease::lease_claim),
        )
        .route(
            "/internal/v1/lease/heartbeat",
            axum::routing::post(lease::lease_heartbeat),
        )
        .route(
            "/internal/v1/lease/complete",
            axum::routing::post(lease::lease_complete),
        )
        .route(
            "/internal/v1/lease/fail",
            axum::routing::post(lease::lease_fail),
        )
        .route(
            "/internal/v1/lease/status",
            axum::routing::get(lease::lease_status),
        )
        .route(
            "/internal/v1/replicate",
            axum::routing::post(cluster::internal_replicate),
        )
        .route(
            "/internal/v1/replicate/batch",
            axum::routing::post(cluster::internal_replicate_batch),
        )
        .route(
            "/internal/v1/replicate/catch-up",
            axum::routing::post(cluster::internal_replicate_catch_up),
        )
        .route(
            "/internal/v1/cluster",
            axum::routing::get(cluster::internal_cluster_config),
        )
        .route(
            "/internal/v1/cluster/gossip",
            axum::routing::post(cluster::internal_cluster_gossip),
        )
        .route(
            "/internal/v1/controller/raft/vote",
            axum::routing::post(cluster::internal_controller_vote),
        )
        .route(
            "/internal/v1/controller/raft/append",
            axum::routing::post(cluster::internal_controller_append),
        )
        .route(
            "/internal/v1/controller/raft/snapshot",
            axum::routing::post(cluster::internal_controller_snapshot),
        )
        .route(
            "/internal/v1/controller/command",
            axum::routing::post(cluster::internal_controller_command),
        )
        .route(
            "/internal/v1/cluster/membership",
            axum::routing::get(infra::internal_cluster_membership),
        )
        .route(
            "/internal/v1/cluster/update-node",
            axum::routing::post(infra::internal_cluster_update_node),
        )
        .route(
            "/internal/v1/cluster/apply-membership",
            axum::routing::post(infra::internal_cluster_apply_membership),
        )
        .route(
            "/internal/v1/cluster/publish",
            axum::routing::post(cluster::internal_cluster_publish),
        )
        .route(
            "/internal/v1/gateway/ingest",
            axum::routing::post(gateway::internal_gateway_ingest),
        )
        .route(
            "/internal/v1/cluster/catalog",
            axum::routing::get(cluster::internal_catalog_snapshot),
        )
        .route(
            "/internal/v1/cluster/catalog/apply",
            axum::routing::post(cluster::internal_catalog_apply),
        )
        .route(
            "/internal/v1/cluster/catalog/flow",
            axum::routing::post(cluster::internal_catalog_flow),
        )
        .route(
            "/internal/v1/cluster/catalog/queue",
            axum::routing::post(cluster::internal_catalog_queue),
        )
        .route(
            "/internal/v1/cluster/catalog/flow/delete",
            axum::routing::post(cluster::internal_catalog_delete_flow),
        )
        .route(
            "/internal/v1/cluster/catalog/queue/delete",
            axum::routing::post(cluster::internal_catalog_delete_queue),
        )
        .route(
            "/internal/v1/cluster/catalog/cron",
            axum::routing::post(cluster::internal_catalog_cron),
        )
        .route(
            "/internal/v1/cluster/catalog/cron/delete",
            axum::routing::post(cluster::internal_catalog_delete_cron),
        )
        .route(
            "/internal/v1/cluster/catalog/group",
            axum::routing::post(cluster::internal_catalog_group),
        )
        .route(
            "/internal/v1/cluster/catalog/group/delete",
            axum::routing::post(cluster::internal_catalog_delete_group),
        )
        .route(
            "/internal/v1/cluster/catalog/group-member",
            axum::routing::post(cluster::internal_catalog_group_member),
        )
        .route(
            "/internal/v1/cluster/catalog/group-member/delete",
            axum::routing::post(cluster::internal_catalog_delete_group_member),
        )
        .route(
            "/internal/v1/cluster/auth",
            axum::routing::get(cluster::internal_cluster_auth),
        )
        .route(
            "/internal/v1/cluster/auth/apply",
            axum::routing::post(cluster::internal_cluster_auth_apply),
        )
        .route_layer(axum::middleware::from_fn(
            cluster_auth::require_cluster_secret,
        ))
}

/// Stateless gateway router. This state contains only routing/auth clients and
/// never opens broker storage, WAL, indexes, schedules, dispatch, or archives.
pub fn gateway_only_router(state: GatewayOnlyState) -> Router {
    let shared = Arc::new(state);
    let protected = Router::new()
        .route(
            "/v1/gateway/enqueue",
            axum::routing::post(gateway::gateway_only_enqueue),
        )
        .route(
            "/v1/ingest/batch",
            axum::routing::post(gateway::gateway_only_enqueue),
        )
        .route(
            "/v1/ingest/ndjson",
            axum::routing::post(gateway::gateway_only_ndjson),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            shared.clone(),
            gateway::gateway_only_auth,
        ));
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(gateway::gateway_only_ready))
        .route("/v1/gateway/status", get(gateway::gateway_only_status))
        .merge(protected)
        .layer(DefaultBodyLimit::max(ingest::MAX_BATCH_BYTES))
        .with_state(shared)
}

async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
        protocol: broker_proto::PROTOCOL_VERSION,
    })
}

#[cfg(test)]
#[allow(clippy::await_holding_lock)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use base64::Engine;
    use broker_dispatch::DispatchConfig;
    use broker_partition::BrokerConfig;
    use std::sync::Mutex;
    use tempfile::tempdir;
    use tower::ServiceExt;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn test_state(dir: &tempfile::TempDir) -> AppState {
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let schedule = ScheduleQueue::open(dir.path()).unwrap();
        let crons = CronRegistry::open(dir.path()).unwrap();
        let dispatch = DispatchEngine::new_broker_only(broker.clone(), DispatchConfig::default());
        let catalog_tombstones = CatalogTombstones::open(dir.path()).expect("catalog tombstones");
        AppState {
            broker,
            schedule,
            crons,
            dispatch,
            leases: LeaseTable::new(),
            cluster: None,
            local_auth: None,
            fair_queue: Arc::new(broker_dispatch::TenantFairQueue::new()),
            catalog_tombstones,
            dispatch_fleet: false,
            broker_only: false,
            #[cfg(feature = "cloud")]
            auth: None,
            #[cfg(feature = "cloud")]
            control_plane: None,
        }
    }

    #[tokio::test]
    async fn healthz_returns_ok() {
        let dir = tempdir().unwrap();
        let app = router(test_state(&dir));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn missing_bearer_is_401_when_auth_configured() {
        let dir = tempdir().unwrap();
        let store = broker_local_auth::LocalAuthStore::open(dir.path()).unwrap();
        store.setup("password1234").unwrap();
        let mut state = test_state(&dir);
        state.local_auth = Some(Arc::new(store));
        let app = router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/queues")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn missing_local_auth_is_401_fail_closed() {
        let dir = tempdir().unwrap();
        let app = router(test_state(&dir));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/publish")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn setup_outside_window_is_401() {
        let _guard = ENV_LOCK.lock().unwrap();
        close_setup_window();
        std::env::remove_var("BETTERMQ_ALLOW_OPEN_SETUP");
        let dir = tempdir().unwrap();
        let store = broker_local_auth::LocalAuthStore::open(dir.path()).unwrap();
        let mut state = test_state(&dir);
        state.local_auth = Some(Arc::new(store));
        let app = router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/local-auth/setup")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"password":"password1234"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn setup_during_open_window_succeeds() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("BETTERMQ_ALLOW_OPEN_SETUP");
        open_setup_window(std::time::Duration::from_secs(60));
        let dir = tempdir().unwrap();
        let store = broker_local_auth::LocalAuthStore::open(dir.path()).unwrap();
        let mut state = test_state(&dir);
        state.local_auth = Some(Arc::new(store));
        let app = router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/local-auth/setup")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"password":"password1234"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        close_setup_window();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn internal_replicate_without_cluster_secret_is_401() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("BETTERMQ_CLUSTER_SECRET");
        let dir = tempdir().unwrap();
        let app = router(test_state(&dir));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/internal/v1/replicate")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn replicate_topic_mismatch_is_400() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("BETTERMQ_CLUSTER_SECRET", "test-cluster-secret");
        let dir = tempdir().unwrap();
        let app = router(test_state(&dir));
        let header = broker_proto::LogRecord {
            id: uuid::Uuid::new_v4(),
            tenant_id: "default".into(),
            topic: "orders".into(),
            routing_key: "rk".into(),
            idempotency_key: None,
            published_at_ms: 1,
            priority: 5,
            flow_parallelism: None,
            flow_key: None,
            flow_rate: None,
            flow_period_secs: None,
            queue_id: None,
            group_id: None,
            group_member_id: None,
            flow_profile_id: None,
            destination_url: None,
            destination_secret: None,
            max_retries: 0,
            retry_backoff: None,
            http_method: None,
            http_headers_json: None,
            http_sign: None,
            payload_ref_json: None,
        };
        let mut frame = Vec::new();
        broker_proto::encode_frame(&header, b"hi", &mut frame).unwrap();
        let body = serde_json::json!({
            "tenant_id": "default",
            "topic": "other-topic",
            "partition": 0,
            "offset": 0,
            "frame_b64": base64::engine::general_purpose::STANDARD.encode(&frame),
            "leader_generation": 1
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/internal/v1/replicate")
                    .header("content-type", "application/json")
                    .header("x-bettermq-cluster-secret", "test-cluster-secret")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        std::env::remove_var("BETTERMQ_CLUSTER_SECRET");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn publish_with_flow_and_no_flow_id_does_not_panic() {
        let dir = tempdir().unwrap();
        let store = broker_local_auth::LocalAuthStore::open(dir.path()).unwrap();
        let token = store.setup("password1234").unwrap();
        let mut state = test_state(&dir);
        state.local_auth = Some(Arc::new(store));
        let app = router(state);
        let body = serde_json::json!({
            "url": "https://example.com/hook",
            "secret": "whsec_test",
            "body": "hi",
            "flow": { "key": "k", "parallelism": 1 }
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/publish")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }
}
