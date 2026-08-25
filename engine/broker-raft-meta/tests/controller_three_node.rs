use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use broker_raft_meta::{
    ClusterConfig, ControllerAppendRequest, ControllerAppendResponse, ControllerCommand,
    ControllerSnapshotRequest, ControllerSnapshotResponse, ControllerState, NodeConfig,
    OpenRaftController, RaftVoteRequest, RaftVoteResponse,
};
use std::collections::HashMap;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use uuid::Uuid;

type ApiError = (StatusCode, String);

async fn vote(
    State(controller): State<OpenRaftController>,
    Json(request): Json<RaftVoteRequest>,
) -> Result<Json<RaftVoteResponse>, ApiError> {
    controller
        .raft()
        .vote(request)
        .await
        .map(Json)
        .map_err(|error| (StatusCode::CONFLICT, error.to_string()))
}

async fn append(
    State(controller): State<OpenRaftController>,
    Json(request): Json<ControllerAppendRequest>,
) -> Result<Json<ControllerAppendResponse>, ApiError> {
    controller
        .raft()
        .append_entries(request)
        .await
        .map(Json)
        .map_err(|error| (StatusCode::CONFLICT, error.to_string()))
}

async fn snapshot(
    State(controller): State<OpenRaftController>,
    Json(request): Json<ControllerSnapshotRequest>,
) -> Result<Json<ControllerSnapshotResponse>, ApiError> {
    controller
        .raft()
        .install_snapshot(request)
        .await
        .map(Json)
        .map_err(|error| (StatusCode::CONFLICT, error.to_string()))
}

fn app(controller: OpenRaftController) -> Router {
    Router::new()
        .route("/internal/v1/controller/raft/vote", post(vote))
        .route("/internal/v1/controller/raft/append", post(append))
        .route("/internal/v1/controller/raft/snapshot", post(snapshot))
        .with_state(controller)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_http_cluster_commits_and_snapshots_controller_state() {
    let mut listeners = Vec::new();
    let mut addresses = Vec::new();
    for _ in 0..3 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addresses.push(format!("http://{}", listener.local_addr().unwrap()));
        listeners.push(listener);
    }
    let ids: Vec<_> = (0..3).map(|_| Uuid::new_v4()).collect();
    let nodes: Vec<_> = ids
        .iter()
        .enumerate()
        .map(|(index, id)| NodeConfig {
            id: *id,
            addr: addresses[index].clone(),
        })
        .collect();
    let cluster_id = Uuid::new_v4();
    let dirs: Vec<_> = (0..3).map(|_| TempDir::new().unwrap()).collect();
    let mut controllers = Vec::new();
    for index in 0..3 {
        let config = ClusterConfig {
            cluster_id,
            nodes: nodes.clone(),
            node_id: ids[index],
            generation: 1,
            hash_version: 1,
        };
        let initial = ControllerState::from_v1(&config, 8, &HashMap::new(), None).unwrap();
        controllers.push(
            OpenRaftController::open(dirs[index].path(), config, initial)
                .await
                .unwrap(),
        );
    }

    let servers: Vec<_> = listeners
        .into_iter()
        .zip(controllers.iter().cloned())
        .map(|(listener, controller)| {
            tokio::spawn(async move {
                axum::serve(listener, app(controller)).await.unwrap();
            })
        })
        .collect();

    let leader = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(controller) = controllers
                .iter()
                .find(|controller| controller.is_leader_with_quorum())
            {
                break controller.clone();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("three-node OpenRaft election");

    let committed = leader
        .submit(ControllerCommand::AdvanceCatalogEpoch)
        .await
        .unwrap();
    assert_eq!(committed.catalog_epoch, 1);

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if controllers
                .iter()
                .all(|controller| controller.state().catalog_epoch == 1)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("controller state replicated to all three nodes");

    leader.trigger_snapshot().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if leader.raft().metrics().borrow().snapshot.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("OpenRaft snapshot");

    let leader_id = leader.raft().metrics().borrow().id;
    for (index, server) in servers.iter().enumerate() {
        if controllers[index].raft().metrics().borrow().id != leader_id {
            server.abort();
        }
    }
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if !leader.is_ready() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("isolated controller leader loses quorum readiness");

    for server in servers {
        server.abort();
    }
}
