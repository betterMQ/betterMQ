//! Public HTTP surface for the BetterMQ data plane.

mod auth;
mod batch;
mod bettermq;
mod catalog_tombstones;
mod cluster;
pub mod cluster_auth;
mod gateway;
mod groups;
mod http_fields;
mod infra;
mod local_auth;
mod metering;
mod ops;
mod publish_path;
mod routes;

use axum::{extract::DefaultBodyLimit, routing::get, Json, Router};
use broker_dispatch::DispatchEngine;
use broker_partition::Broker;
use broker_schedule::{CronRegistry, ScheduleQueue};
use cluster::ClusterHandle;
use serde::Serialize;
use std::sync::Arc;

/// Largest single-message body allowed on cloud (matches Scale plan unless overridden).
#[cfg(feature = "cloud")]
fn cloud_body_limit_bytes() -> usize {
    std::env::var("BETTERMQ_MAX_MESSAGE_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(52 * 1024 * 1024)
}

pub use catalog_tombstones::CatalogTombstones;
pub use cluster::{
    build_cluster_status, catalog_peer_targets, enqueue_dispatch_after_publish,
    publish_with_cluster, push_catalog_to_recovered_peer, spawn_cluster_catalog_sync,
    sync_catalog_from_peers, ClusterGossipRequest, ClusterHandle as Cluster, ClusterStatusResponse,
};

#[derive(Clone)]
pub struct AppState {
    pub broker: Broker,
    pub schedule: ScheduleQueue,
    pub crons: CronRegistry,
    pub dispatch: DispatchEngine,
    pub cluster: Option<ClusterHandle>,
    pub local_auth: Option<Arc<broker_local_auth::LocalAuthStore>>,
    pub fair_queue: Arc<broker_dispatch::TenantFairQueue>,
    pub catalog_tombstones: CatalogTombstones,
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
    let shared = Arc::new(state);
    let protected = Router::new()
        .route(
            "/v1/enqueue/batch",
            axum::routing::post(batch::batch_enqueue),
        )
        .route(
            "/v1/gateway/enqueue",
            axum::routing::post(gateway::gateway_enqueue),
        )
        .merge(bettermq::bettermq_routes())
        .merge(groups::group_routes())
        .route("/v1/destinations/blocked", get(ops::list_blocked_hosts))
        .route(
            "/v1/destinations/unblock",
            axum::routing::post(ops::unblock_host),
        )
        .merge(infra::protected_infra_routes())
        .route_layer(axum::middleware::from_fn_with_state(
            shared.clone(),
            auth::check_ingest_limits,
        ))
        .route_layer(axum::middleware::from_fn_with_state(
            shared.clone(),
            auth::require_api_key,
        ));

    let internal = Router::new()
        .route(
            "/internal/v1/replicate",
            axum::routing::post(cluster::internal_replicate),
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
        ));

    let public = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(ops::readyz))
        .route("/metrics", get(ops::metrics))
        .merge(internal)
        .merge(local_auth::routes())
        .merge(infra::public_infra_routes());

    let app = public.merge(protected);
    // Self-host: no HTTP body cap. Cloud: cap at operator/plan ceiling (per-tenant limits in middleware).
    let app = if shared.uses_cloud_auth() {
        #[cfg(feature = "cloud")]
        {
            app.layer(DefaultBodyLimit::max(cloud_body_limit_bytes()))
        }
        #[cfg(not(feature = "cloud"))]
        {
            app
        }
    } else {
        app.layer(DefaultBodyLimit::disable())
    };
    app.with_state(shared)
}

async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
        protocol: broker_proto::PROTOCOL_VERSION,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use broker_dispatch::DispatchConfig;
    use broker_partition::BrokerConfig;
    use tempfile::tempdir;
    use tower::ServiceExt;

    fn test_state(dir: &tempfile::TempDir) -> AppState {
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let schedule = ScheduleQueue::open(dir.path()).unwrap();
        let crons = CronRegistry::open(dir.path()).unwrap();
        let dispatch = DispatchEngine::new(broker.clone(), DispatchConfig::default());
        let catalog_tombstones = CatalogTombstones::open(dir.path()).expect("catalog tombstones");
        AppState {
            broker,
            schedule,
            crons,
            dispatch,
            cluster: None,
            local_auth: None,
            fair_queue: Arc::new(broker_dispatch::TenantFairQueue::new()),
            catalog_tombstones,
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
}
