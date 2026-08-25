//! OpenRaft-backed durable cluster controller.
//!
//! OpenRaft owns terms, votes, membership, replication, and snapshots. The
//! application controller state is encoded as one versioned value in the
//! official RocksDB state machine so every mutation is committed by Raft.

use crate::cluster::{ClusterConfig, SchedulerLease};
use crate::durable_store::DurableRocksStore;
use crate::network::HttpNetworkFactory;
use crate::placement::{
    initial_controller_voters, min_isr_for, pick_leader, place_replicas, replication_policy,
    upsert_catalog, CatalogRecord, DataNodeRecord, RebalanceOp,
};
use openraft::storage::Adaptor;
use openraft::{BasicNode, Config, Raft, ServerState, SnapshotPolicy};
pub use openraft_rocksstore::TypeConfig;
use openraft_rocksstore::{RocksRequest, RocksStore};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Mutex;
use uuid::Uuid;

pub type NodeId = u64;
pub type ControllerRaft = Raft<TypeConfig>;
pub type ControllerAppendRequest = openraft::raft::AppendEntriesRequest<TypeConfig>;
pub type ControllerAppendResponse = openraft::raft::AppendEntriesResponse<NodeId>;
pub type ControllerVoteRequest = openraft::raft::VoteRequest<NodeId>;
pub type ControllerVoteResponse = openraft::raft::VoteResponse<NodeId>;
pub type ControllerSnapshotRequest = openraft::raft::InstallSnapshotRequest<TypeConfig>;
pub type ControllerSnapshotResponse = openraft::raft::InstallSnapshotResponse<NodeId>;

const STATE_KEY: &str = "bettermq/controller-state/v1";
const QUORUM_ACK_MAX_AGE_MS: u64 = 2_000;

#[derive(Debug, Error)]
pub enum ControllerError {
    #[error("openraft: {0}")]
    Raft(String),
    #[error("controller storage: {0}")]
    Storage(String),
    #[error("controller transport: {0}")]
    Transport(String),
    #[error("controller has no known leader")]
    NoLeader,
    #[error("controller node id collision between {first} and {second}")]
    NodeIdCollision { first: Uuid, second: Uuid },
    #[error("unknown controller node {0}")]
    UnknownNode(Uuid),
    #[error("invalid controller command: {0}")]
    InvalidCommand(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ControllerNode {
    pub id: Uuid,
    pub raft_id: NodeId,
    pub addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardPlacement {
    pub shard: u32,
    pub leader_id: Uuid,
    pub leader_term: u64,
    pub replicas: Vec<Uuid>,
    #[serde(default)]
    pub learners: Vec<Uuid>,
    #[serde(default)]
    pub isr: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FanoutOutboxEntry {
    pub id: Uuid,
    pub tenant_id: String,
    #[serde(default)]
    pub acceptance_hash: String,
    pub version: u64,
    pub authority_term: u64,
    pub owner_id: Option<Uuid>,
    pub lease_expires_at_ms: i64,
    pub payload_json: String,
    pub completed: bool,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ControllerState {
    pub version: u32,
    pub cluster_id: Uuid,
    pub membership_generation: u64,
    pub nodes: BTreeMap<NodeId, ControllerNode>,
    pub shards: HashMap<u32, ShardPlacement>,
    pub catalog_epoch: u64,
    pub scheduler: Option<SchedulerLease>,
    #[serde(default)]
    pub fanout_outbox: BTreeMap<Uuid, FanoutOutboxEntry>,
    #[serde(default)]
    pub data_nodes: BTreeMap<Uuid, DataNodeRecord>,
    #[serde(default)]
    pub controller_voters: Vec<Uuid>,
    #[serde(default)]
    pub replication_factor: u32,
    #[serde(default)]
    pub min_isr: u32,
    #[serde(default)]
    pub catalog_records: BTreeMap<String, CatalogRecord>,
    #[serde(default)]
    pub gc_watermarks: HashMap<u32, u64>,
    #[serde(default)]
    pub rebalance: BTreeMap<u32, RebalanceOp>,
}

impl ControllerState {
    pub fn from_v1(
        config: &ClusterConfig,
        shard_count: u32,
        shard_terms: &HashMap<u32, u64>,
        scheduler: Option<SchedulerLease>,
    ) -> Result<Self, ControllerError> {
        let mut nodes: BTreeMap<NodeId, ControllerNode> = BTreeMap::new();
        for node in &config.nodes {
            let raft_id = raft_id(node.id);
            if let Some(existing) = nodes.get(&raft_id) {
                if existing.id != node.id {
                    return Err(ControllerError::NodeIdCollision {
                        first: existing.id,
                        second: node.id,
                    });
                }
            }
            nodes.insert(
                raft_id,
                ControllerNode {
                    id: node.id,
                    raft_id,
                    addr: node.addr.clone(),
                },
            );
        }
        let provisional: Vec<_> = config
            .nodes
            .iter()
            .map(|node| DataNodeRecord {
                id: node.id,
                addr: node.addr.clone(),
                rack: None,
                region: None,
                broker: true,
                controller_voter: false,
            })
            .collect();
        let voters = initial_controller_voters(&provisional);
        let data_nodes: BTreeMap<_, _> = provisional
            .into_iter()
            .map(|mut node| {
                node.controller_voter = voters.contains(&node.id);
                (node.id, node)
            })
            .collect();
        let node_list: Vec<_> = data_nodes.values().cloned().collect();
        let (replication_factor, min_isr) = replication_policy(node_list.len());
        let mut shards = HashMap::new();
        for shard in 0..shard_count.max(1) {
            let replicas = place_replicas(&node_list, shard, replication_factor as usize);
            let leader_id = pick_leader(&replicas, shard)
                .or_else(|| node_list.first().map(|n| n.id))
                .unwrap_or(config.node_id);
            shards.insert(
                shard,
                ShardPlacement {
                    shard,
                    leader_id,
                    leader_term: shard_terms.get(&shard).copied().unwrap_or(0),
                    isr: replicas.clone(),
                    learners: Vec::new(),
                    replicas,
                },
            );
        }
        Ok(Self {
            version: 1,
            cluster_id: config.cluster_id,
            membership_generation: config.generation,
            nodes,
            shards,
            catalog_epoch: 0,
            scheduler,
            fanout_outbox: BTreeMap::new(),
            controller_voters: initial_controller_voters(&node_list),
            data_nodes,
            replication_factor,
            min_isr,
            catalog_records: BTreeMap::new(),
            gc_watermarks: HashMap::new(),
            rebalance: BTreeMap::new(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ControllerCommand {
    AcquireShardLeadership {
        shard: u32,
        leader_id: Uuid,
    },
    ObserveShardTerm {
        shard: u32,
        leader_id: Uuid,
        term: u64,
    },
    AcquireScheduler {
        holder: Uuid,
        now_ms: i64,
        ttl_ms: i64,
    },
    AdvanceCatalogEpoch,
    AcceptFanout {
        id: Uuid,
        tenant_id: String,
        acceptance_hash: String,
        payload_json: String,
        created_at_ms: i64,
        controller_term: u64,
    },
    ClaimFanout {
        id: Uuid,
        owner_id: Uuid,
        expected_version: u64,
        now_ms: i64,
        ttl_ms: i64,
        controller_term: u64,
    },
    UpdateFanout {
        id: Uuid,
        owner_id: Uuid,
        expected_version: u64,
        payload_json: String,
        completed: bool,
        now_ms: i64,
        controller_term: u64,
    },
    GarbageCollectFanout {
        ids: Vec<Uuid>,
        completed_before_ms: i64,
        controller_term: u64,
    },
    RegisterDataNode {
        node: DataNodeRecord,
    },
    PutCatalogRecord {
        kind: String,
        key: String,
        payload_json: String,
        tombstone: bool,
    },
    BeginRebalance {
        shard: u32,
        add: Option<Uuid>,
        remove: Option<Uuid>,
    },
    PromoteLearner {
        shard: u32,
        node_id: Uuid,
    },
    CompleteRebalance {
        shard: u32,
    },
    SetGcWatermark {
        shard: u32,
        watermark: u64,
    },
    SetReplicationPolicy {
        factor: u32,
        min_isr: u32,
    },
    AddControllerVoter {
        node_id: Uuid,
    },
    RemoveControllerVoter {
        node_id: Uuid,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DrainReport {
    pub node_id: Uuid,
    pub shards: Vec<u32>,
    pub leadership_transfers: Vec<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControllerCommandResponse {
    pub state: ControllerState,
}

#[derive(Clone)]
pub struct OpenRaftController {
    raft: ControllerRaft,
    store: Arc<RocksStore>,
    node_id: NodeId,
    config: ClusterConfig,
    state: Arc<RwLock<ControllerState>>,
    state_committed: Arc<AtomicBool>,
    last_leader_contact_ms: Arc<AtomicI64>,
    command_lock: Arc<Mutex<()>>,
    http: reqwest::Client,
}

impl OpenRaftController {
    pub async fn open(
        data_dir: &Path,
        config: ClusterConfig,
        bootstrap_state: ControllerState,
    ) -> Result<Self, ControllerError> {
        let node_id = raft_id(config.node_id);
        let store = RocksStore::new(data_dir.join("controller-rocksdb")).await;
        let durable_store = DurableRocksStore::new(Arc::clone(&store));
        let (log_store, state_machine) = Adaptor::new(durable_store);
        let network = HttpNetworkFactory::new()
            .map_err(|error| ControllerError::Transport(error.to_string()))?;
        let raft_config = Config {
            cluster_name: config.cluster_id.to_string(),
            heartbeat_interval: 250,
            election_timeout_min: 750,
            election_timeout_max: 1_500,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(1_000),
            replication_lag_threshold: 2_000,
            max_in_snapshot_log_to_keep: 256,
            ..Default::default()
        }
        .validate()
        .map_err(|error| ControllerError::Raft(error.to_string()))?;
        let raft = Raft::new(
            node_id,
            Arc::new(raft_config),
            network,
            log_store,
            state_machine,
        )
        .await
        .map_err(|error| ControllerError::Raft(error.to_string()))?;
        let http = reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(90))
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|error| ControllerError::Transport(error.to_string()))?;
        let controller = Self {
            raft,
            store,
            node_id,
            config,
            state: Arc::new(RwLock::new(bootstrap_state)),
            state_committed: Arc::new(AtomicBool::new(false)),
            last_leader_contact_ms: Arc::new(AtomicI64::new(0)),
            command_lock: Arc::new(Mutex::new(())),
            http,
        };
        controller.initialize_membership().await?;
        controller.spawn_state_sync();
        Ok(controller)
    }

    async fn initialize_membership(&self) -> Result<(), ControllerError> {
        if self
            .raft
            .is_initialized()
            .await
            .map_err(|error| ControllerError::Raft(error.to_string()))?
        {
            return Ok(());
        }
        let state = self.state.read().clone();
        let voter_ids: BTreeSet<_> = if state.controller_voters.is_empty() {
            self.config.nodes.iter().map(|node| node.id).collect()
        } else {
            state.controller_voters.iter().copied().collect()
        };
        let members: BTreeMap<_, _> = self
            .config
            .nodes
            .iter()
            .filter(|node| voter_ids.contains(&node.id))
            .map(|node| (raft_id(node.id), BasicNode::new(&node.addr)))
            .collect();
        self.raft
            .initialize(members)
            .await
            .map_err(|error| ControllerError::Raft(error.to_string()))
    }

    fn spawn_state_sync(&self) {
        let controller = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                interval.tick().await;
                if let Ok(Some(state)) = controller.read_store_state().await {
                    *controller.state.write() = state;
                    controller.state_committed.store(true, Ordering::Release);
                } else if controller.is_leader_with_quorum() {
                    let bootstrap = controller.state.read().clone();
                    if let Err(error) = controller.write_state_local(bootstrap).await {
                        tracing::warn!(%error, "failed to commit migrated controller state");
                    }
                }
                if controller.is_leader_with_quorum() {
                    if let Err(error) = controller.reconcile_membership().await {
                        tracing::warn!(%error, "failed to reconcile OpenRaft membership");
                    }
                }
            }
        });
    }

    async fn read_store_state(&self) -> Result<Option<ControllerState>, ControllerError> {
        let state_machine = self.store.state_machine.read().await;
        let encoded = state_machine
            .get(STATE_KEY)
            .map_err(|error| ControllerError::Storage(error.to_string()))?;
        encoded
            .map(|json| {
                serde_json::from_str(&json)
                    .map_err(|error| ControllerError::Storage(error.to_string()))
            })
            .transpose()
    }

    async fn write_state_local(
        &self,
        state: ControllerState,
    ) -> Result<ControllerState, ControllerError> {
        self.raft
            .ensure_linearizable()
            .await
            .map_err(|error| ControllerError::Raft(error.to_string()))?;
        let value = serde_json::to_string(&state)
            .map_err(|error| ControllerError::Storage(error.to_string()))?;
        self.raft
            .client_write(RocksRequest::Set {
                key: STATE_KEY.to_string(),
                value,
            })
            .await
            .map_err(|error| ControllerError::Raft(error.to_string()))?;
        *self.state.write() = state.clone();
        self.state_committed.store(true, Ordering::Release);
        Ok(state)
    }

    pub async fn submit(
        &self,
        command: ControllerCommand,
    ) -> Result<ControllerState, ControllerError> {
        if self.is_leader_with_quorum() {
            return self.submit_local(command).await;
        }
        let leader = self
            .raft
            .metrics()
            .borrow()
            .current_leader
            .ok_or(ControllerError::NoLeader)?;
        let addr = self
            .state
            .read()
            .nodes
            .get(&leader)
            .map(|node| node.addr.clone())
            .ok_or(ControllerError::NoLeader)?;
        let url = format!(
            "{}/internal/v1/controller/command",
            addr.trim_end_matches('/')
        );
        let mut request = self.http.post(url).json(&command);
        if let Ok(secret) = std::env::var("BETTERMQ_CLUSTER_SECRET") {
            if !secret.trim().is_empty() {
                request = request.header("x-bettermq-cluster-secret", secret);
            }
        }
        let response = request
            .send()
            .await
            .map_err(|error| ControllerError::Transport(error.to_string()))?;
        if !response.status().is_success() {
            return Err(ControllerError::Transport(format!(
                "controller leader returned {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }
        let response: ControllerCommandResponse = response
            .json()
            .await
            .map_err(|error| ControllerError::Transport(error.to_string()))?;
        *self.state.write() = response.state.clone();
        Ok(response.state)
    }

    pub async fn submit_local(
        &self,
        mut command: ControllerCommand,
    ) -> Result<ControllerState, ControllerError> {
        let _guard = self.command_lock.lock().await;
        let now = chrono::Utc::now().timestamp_millis();
        let term = self.term();
        match &mut command {
            ControllerCommand::AcquireScheduler { now_ms, .. } => *now_ms = now,
            ControllerCommand::AcceptFanout {
                controller_term,
                created_at_ms,
                ..
            } => {
                *controller_term = term;
                if *created_at_ms <= 0 {
                    *created_at_ms = now;
                }
            }
            ControllerCommand::ClaimFanout {
                now_ms,
                controller_term,
                ..
            }
            | ControllerCommand::UpdateFanout {
                now_ms,
                controller_term,
                ..
            } => {
                *now_ms = now;
                *controller_term = term;
            }
            ControllerCommand::GarbageCollectFanout {
                controller_term, ..
            } => *controller_term = term,
            _ => {}
        }
        let mut state = self
            .read_store_state()
            .await?
            .unwrap_or_else(|| self.state.read().clone());
        apply_command(&mut state, command)?;
        self.write_state_local(state).await
    }

    async fn reconcile_membership(&self) -> Result<(), ControllerError> {
        let state = self.state.read().clone();
        let desired: BTreeSet<_> = if state.controller_voters.is_empty() {
            self.config
                .nodes
                .iter()
                .map(|node| raft_id(node.id))
                .collect()
        } else {
            state
                .controller_voters
                .iter()
                .map(|id| raft_id(*id))
                .collect()
        };
        let current: BTreeSet<_> = self
            .raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .voter_ids()
            .collect();
        if desired == current {
            return Ok(());
        }
        let mut node_by_raft: BTreeMap<NodeId, String> = BTreeMap::new();
        for node in &self.config.nodes {
            node_by_raft.insert(raft_id(node.id), node.addr.clone());
        }
        for node in state.data_nodes.values() {
            node_by_raft
                .entry(raft_id(node.id))
                .or_insert_with(|| node.addr.clone());
        }
        for id in &desired {
            if !current.contains(id) {
                let addr = node_by_raft.get(id).cloned().unwrap_or_default();
                self.raft
                    .add_learner(*id, BasicNode::new(&addr), true)
                    .await
                    .map_err(|error| ControllerError::Raft(error.to_string()))?;
            }
        }
        self.raft
            .change_membership(desired, false)
            .await
            .map_err(|error| ControllerError::Raft(error.to_string()))?;
        Ok(())
    }

    pub fn raft(&self) -> &ControllerRaft {
        &self.raft
    }

    pub fn state(&self) -> ControllerState {
        self.state.read().clone()
    }

    pub fn term(&self) -> u64 {
        self.raft.metrics().borrow().current_term
    }

    pub fn leader_uuid(&self) -> Option<Uuid> {
        let metrics = self.raft.metrics();
        let leader = metrics.borrow().current_leader?;
        self.state.read().nodes.get(&leader).map(|node| node.id)
    }

    /// Move shard replicas off `node_id` before dropping membership.
    /// Refuses if the node is the last ISR copy of any shard.
    pub async fn drain_data_node(&self, node_id: Uuid) -> Result<DrainReport, ControllerError> {
        let state = self.state();
        let min_isr = state.min_isr.max(1) as usize;
        let mut last_copy = Vec::new();
        let mut shards = Vec::new();
        for placement in state.shards.values() {
            let holds = placement.replicas.contains(&node_id)
                || placement.isr.contains(&node_id)
                || placement.leader_id == node_id
                || placement.learners.contains(&node_id);
            if !holds {
                continue;
            }
            shards.push(placement.shard);
            let remaining_isr = placement.isr.iter().filter(|id| **id != node_id).count();
            if placement.isr.contains(&node_id) && remaining_isr < min_isr {
                last_copy.push(placement.shard);
            }
        }
        if !last_copy.is_empty() {
            return Err(ControllerError::InvalidCommand(format!(
                "refuse drain: node is last ISR copy for shards {last_copy:?} (minISR={min_isr})"
            )));
        }
        shards.sort_unstable();
        let mut leadership_transfers = Vec::new();
        for shard in &shards {
            let placement =
                self.state().shards.get(shard).cloned().ok_or_else(|| {
                    ControllerError::InvalidCommand(format!("unknown shard {shard}"))
                })?;
            if placement.leader_id == node_id {
                if let Some(next) = placement
                    .isr
                    .iter()
                    .copied()
                    .find(|id| *id != node_id)
                    .or_else(|| placement.replicas.iter().copied().find(|id| *id != node_id))
                {
                    self.submit(ControllerCommand::AcquireShardLeadership {
                        shard: *shard,
                        leader_id: next,
                    })
                    .await?;
                    leadership_transfers.push(*shard);
                }
            }
            self.submit(ControllerCommand::BeginRebalance {
                shard: *shard,
                add: None,
                remove: Some(node_id),
            })
            .await?;
            self.submit(ControllerCommand::CompleteRebalance { shard: *shard })
                .await?;
        }
        Ok(DrainReport {
            node_id,
            shards,
            leadership_transfers,
        })
    }

    pub fn is_leader_with_quorum(&self) -> bool {
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow();
        metrics.running_state.is_ok()
            && metrics.state == ServerState::Leader
            && metrics.current_leader == Some(self.node_id)
            && metrics
                .millis_since_quorum_ack
                .is_some_and(|age| age <= QUORUM_ACK_MAX_AGE_MS)
    }

    pub fn is_ready(&self) -> bool {
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow();
        self.state_committed.load(Ordering::Acquire)
            && metrics.running_state.is_ok()
            && metrics.current_leader.is_some()
            && (if matches!(metrics.state, ServerState::Leader) {
                metrics
                    .millis_since_quorum_ack
                    .is_some_and(|age| age <= QUORUM_ACK_MAX_AGE_MS)
            } else {
                let contact = self.last_leader_contact_ms.load(Ordering::Acquire);
                contact > 0
                    && chrono::Utc::now()
                        .timestamp_millis()
                        .saturating_sub(contact)
                        <= QUORUM_ACK_MAX_AGE_MS as i64
            })
    }

    pub fn note_leader_contact(&self) {
        self.last_leader_contact_ms
            .store(chrono::Utc::now().timestamp_millis(), Ordering::Release);
    }

    pub async fn trigger_snapshot(&self) -> Result<(), ControllerError> {
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|error| ControllerError::Raft(error.to_string()))
    }
}

fn apply_command(
    state: &mut ControllerState,
    command: ControllerCommand,
) -> Result<(), ControllerError> {
    match command {
        ControllerCommand::AcquireShardLeadership { shard, leader_id } => {
            if !state.nodes.values().any(|node| node.id == leader_id) {
                return Err(ControllerError::UnknownNode(leader_id));
            }
            let placement = state
                .shards
                .get_mut(&shard)
                .ok_or_else(|| ControllerError::InvalidCommand(format!("unknown shard {shard}")))?;
            placement.leader_term = placement
                .leader_term
                .checked_add(1)
                .filter(|term| *term != u64::MAX)
                .ok_or_else(|| ControllerError::InvalidCommand("shard term exhausted".into()))?;
            placement.leader_id = leader_id;
        }
        ControllerCommand::ObserveShardTerm {
            shard,
            leader_id,
            term,
        } => {
            let placement = state
                .shards
                .get_mut(&shard)
                .ok_or_else(|| ControllerError::InvalidCommand(format!("unknown shard {shard}")))?;
            if placement.leader_id != leader_id || placement.leader_term != term {
                return Err(ControllerError::InvalidCommand(format!(
                    "term {term} for {leader_id} is not the committed placement {}@{}",
                    placement.leader_id, placement.leader_term
                )));
            }
        }
        ControllerCommand::AcquireScheduler {
            holder,
            now_ms,
            ttl_ms,
        } => {
            if !state.nodes.values().any(|node| node.id == holder) {
                return Err(ControllerError::UnknownNode(holder));
            }
            let can_acquire = state
                .scheduler
                .as_ref()
                .is_none_or(|lease| lease.expires_at_ms <= now_ms || lease.holder == holder);
            if !can_acquire {
                return Err(ControllerError::InvalidCommand(
                    "scheduler lease is held by another node".into(),
                ));
            }
            state.scheduler = Some(SchedulerLease {
                holder,
                expires_at_ms: now_ms.saturating_add(ttl_ms.max(1)),
            });
        }
        ControllerCommand::AdvanceCatalogEpoch => {
            state.catalog_epoch = state
                .catalog_epoch
                .checked_add(1)
                .ok_or_else(|| ControllerError::InvalidCommand("catalog epoch exhausted".into()))?;
        }
        ControllerCommand::AcceptFanout {
            id,
            tenant_id,
            acceptance_hash,
            payload_json,
            created_at_ms,
            controller_term,
        } => {
            if let Some(existing) = state.fanout_outbox.get(&id) {
                if existing.tenant_id == tenant_id
                    && ((!acceptance_hash.is_empty()
                        && existing.acceptance_hash == acceptance_hash)
                        || (acceptance_hash.is_empty() && existing.payload_json == payload_json))
                {
                    return Ok(());
                }
                return Err(ControllerError::InvalidCommand(format!(
                    "fanout {id} already exists with different content"
                )));
            }
            state.fanout_outbox.insert(
                id,
                FanoutOutboxEntry {
                    id,
                    tenant_id,
                    acceptance_hash,
                    version: 1,
                    authority_term: controller_term,
                    owner_id: None,
                    lease_expires_at_ms: 0,
                    payload_json,
                    completed: false,
                    created_at_ms,
                    updated_at_ms: created_at_ms,
                },
            );
        }
        ControllerCommand::ClaimFanout {
            id,
            owner_id,
            expected_version,
            now_ms,
            ttl_ms,
            controller_term,
        } => {
            let entry = state
                .fanout_outbox
                .get_mut(&id)
                .ok_or_else(|| ControllerError::InvalidCommand(format!("unknown fanout {id}")))?;
            if entry.completed {
                return Ok(());
            }
            if entry.owner_id == Some(owner_id)
                && entry.authority_term == controller_term
                && entry.lease_expires_at_ms > now_ms
            {
                return Ok(());
            }
            if expected_version != entry.version {
                return Err(ControllerError::InvalidCommand(format!(
                    "fanout {id} version fence failed: expected {expected_version}, have {}",
                    entry.version
                )));
            }
            if entry.authority_term > controller_term {
                return Err(ControllerError::InvalidCommand(format!(
                    "fanout {id} term fence failed: stale {controller_term}, have {}",
                    entry.authority_term
                )));
            }
            if entry.owner_id.is_some()
                && entry.lease_expires_at_ms > now_ms
                && entry.authority_term == controller_term
            {
                return Err(ControllerError::InvalidCommand(format!(
                    "fanout {id} is leased by another worker"
                )));
            }
            entry.version = entry.version.checked_add(1).ok_or_else(|| {
                ControllerError::InvalidCommand(format!("fanout {id} version exhausted"))
            })?;
            entry.authority_term = controller_term;
            entry.owner_id = Some(owner_id);
            entry.lease_expires_at_ms = now_ms.saturating_add(ttl_ms.max(1));
            entry.updated_at_ms = now_ms;
        }
        ControllerCommand::UpdateFanout {
            id,
            owner_id,
            expected_version,
            payload_json,
            completed,
            now_ms,
            controller_term,
        } => {
            let entry = state
                .fanout_outbox
                .get_mut(&id)
                .ok_or_else(|| ControllerError::InvalidCommand(format!("unknown fanout {id}")))?;
            if entry.payload_json == payload_json && entry.completed == completed {
                return Ok(());
            }
            if expected_version != entry.version
                || entry.authority_term != controller_term
                || entry.owner_id != Some(owner_id)
            {
                return Err(ControllerError::InvalidCommand(format!(
                    "fanout {id} ownership fence failed"
                )));
            }
            if entry.lease_expires_at_ms <= now_ms {
                return Err(ControllerError::InvalidCommand(format!(
                    "fanout {id} lease expired"
                )));
            }
            entry.version = entry.version.checked_add(1).ok_or_else(|| {
                ControllerError::InvalidCommand(format!("fanout {id} version exhausted"))
            })?;
            entry.payload_json = payload_json;
            entry.completed = completed;
            entry.updated_at_ms = now_ms;
            if completed {
                entry.owner_id = None;
                entry.lease_expires_at_ms = 0;
            }
        }
        ControllerCommand::GarbageCollectFanout {
            ids,
            completed_before_ms: _,
            controller_term,
        } => {
            state.fanout_outbox.retain(|id, entry| {
                !ids.contains(id) || !entry.completed || entry.authority_term > controller_term
            });
        }
        ControllerCommand::RegisterDataNode { mut node } => {
            node.controller_voter = state.controller_voters.contains(&node.id);
            state.data_nodes.insert(node.id, node.clone());
            if !state.nodes.values().any(|existing| existing.id == node.id) {
                let raft_id = raft_id(node.id);
                state.nodes.insert(
                    raft_id,
                    ControllerNode {
                        id: node.id,
                        raft_id,
                        addr: node.addr.clone(),
                    },
                );
            }
            state.membership_generation = state.membership_generation.saturating_add(1);
        }
        ControllerCommand::PutCatalogRecord {
            kind,
            key,
            payload_json,
            tombstone,
        } => {
            upsert_catalog(
                &mut state.catalog_records,
                kind,
                key,
                payload_json,
                tombstone,
            );
            state.catalog_epoch = state.catalog_epoch.saturating_add(1);
        }
        ControllerCommand::BeginRebalance { shard, add, remove } => {
            let placement = state
                .shards
                .get_mut(&shard)
                .ok_or_else(|| ControllerError::InvalidCommand(format!("unknown shard {shard}")))?;
            if let Some(add) = add {
                if !placement.replicas.contains(&add) && !placement.learners.contains(&add) {
                    placement.learners.push(add);
                }
            }
            let generation = state
                .rebalance
                .get(&shard)
                .map(|op| op.generation.saturating_add(1))
                .unwrap_or(1);
            state.rebalance.insert(
                shard,
                RebalanceOp {
                    shard,
                    adding: add.into_iter().collect(),
                    removing: remove.into_iter().collect(),
                    generation,
                },
            );
        }
        ControllerCommand::PromoteLearner { shard, node_id } => {
            let placement = state
                .shards
                .get_mut(&shard)
                .ok_or_else(|| ControllerError::InvalidCommand(format!("unknown shard {shard}")))?;
            placement.learners.retain(|id| *id != node_id);
            if !placement.replicas.contains(&node_id) {
                placement.replicas.push(node_id);
            }
            if !placement.isr.contains(&node_id) {
                placement.isr.push(node_id);
            }
        }
        ControllerCommand::CompleteRebalance { shard } => {
            if let Some(op) = state.rebalance.remove(&shard) {
                if let Some(placement) = state.shards.get_mut(&shard) {
                    for id in op.removing {
                        placement.replicas.retain(|r| *r != id);
                        placement.isr.retain(|r| *r != id);
                        placement.learners.retain(|r| *r != id);
                        if placement.leader_id == id {
                            if let Some(next) = pick_leader(&placement.replicas, shard) {
                                placement.leader_id = next;
                                placement.leader_term = placement.leader_term.saturating_add(1);
                            }
                        }
                    }
                }
            }
        }
        ControllerCommand::SetGcWatermark { shard, watermark } => {
            let current = state.gc_watermarks.get(&shard).copied().unwrap_or(0);
            if watermark < current {
                return Err(ControllerError::InvalidCommand(format!(
                    "gc watermark for shard {shard} cannot move backwards ({watermark} < {current})"
                )));
            }
            state.gc_watermarks.insert(shard, watermark);
        }
        ControllerCommand::SetReplicationPolicy { factor, min_isr } => {
            let factor = factor.max(1);
            state.replication_factor = factor;
            state.min_isr = min_isr_for(factor, min_isr);
        }
        ControllerCommand::AddControllerVoter { node_id } => {
            if !state.data_nodes.contains_key(&node_id)
                && !state.nodes.values().any(|node| node.id == node_id)
            {
                return Err(ControllerError::InvalidCommand(format!(
                    "unknown node {node_id}"
                )));
            }
            if !state.controller_voters.contains(&node_id) {
                if state.controller_voters.len() >= 5 {
                    return Err(ControllerError::InvalidCommand(
                        "controller voter set is already at 5".into(),
                    ));
                }
                state.controller_voters.push(node_id);
            }
            if let Some(node) = state.data_nodes.get_mut(&node_id) {
                node.controller_voter = true;
            }
        }
        ControllerCommand::RemoveControllerVoter { node_id } => {
            if !state.controller_voters.contains(&node_id) {
                return Err(ControllerError::InvalidCommand(format!(
                    "{node_id} is not a controller voter"
                )));
            }
            if state.controller_voters.len() <= 1 {
                return Err(ControllerError::InvalidCommand(
                    "cannot remove the last controller voter".into(),
                ));
            }
            state.controller_voters.retain(|id| *id != node_id);
            if let Some(node) = state.data_nodes.get_mut(&node_id) {
                node.controller_voter = false;
            }
        }
    }
    Ok(())
}

pub fn raft_id(id: Uuid) -> NodeId {
    u64::from_be_bytes(id.as_bytes()[..8].try_into().expect("UUID prefix"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controller_commands_update_versioned_state() {
        let ids = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let config = ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: ids
                .iter()
                .enumerate()
                .map(|(index, id)| crate::cluster::NodeConfig {
                    id: *id,
                    addr: format!("http://n{index}"),
                })
                .collect(),
            node_id: ids[0],
            generation: 1,
            hash_version: 1,
        };
        let mut state = ControllerState::from_v1(&config, 4, &HashMap::new(), None).unwrap();
        apply_command(
            &mut state,
            ControllerCommand::AcquireShardLeadership {
                shard: 1,
                leader_id: ids[2],
            },
        )
        .unwrap();
        assert_eq!(state.shards[&1].leader_id, ids[2]);
        assert_eq!(state.shards[&1].leader_term, 1);
        apply_command(&mut state, ControllerCommand::AdvanceCatalogEpoch).unwrap();
        assert_eq!(state.catalog_epoch, 1);
    }

    #[test]
    fn fanout_outbox_is_version_and_term_fenced() {
        let ids = [Uuid::new_v4(), Uuid::new_v4()];
        let config = ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: ids
                .iter()
                .enumerate()
                .map(|(index, id)| crate::cluster::NodeConfig {
                    id: *id,
                    addr: format!("http://n{index}"),
                })
                .collect(),
            node_id: ids[0],
            generation: 1,
            hash_version: 1,
        };
        let mut state = ControllerState::from_v1(&config, 1, &HashMap::new(), None).unwrap();
        let fanout_id = Uuid::new_v4();
        apply_command(
            &mut state,
            ControllerCommand::AcceptFanout {
                id: fanout_id,
                tenant_id: "tenant".into(),
                acceptance_hash: "hash".into(),
                payload_json: r#"{"completed":false}"#.into(),
                created_at_ms: 1,
                controller_term: 5,
            },
        )
        .unwrap();
        apply_command(
            &mut state,
            ControllerCommand::ClaimFanout {
                id: fanout_id,
                owner_id: ids[0],
                expected_version: 1,
                now_ms: 10,
                ttl_ms: 100,
                controller_term: 5,
            },
        )
        .unwrap();
        assert!(apply_command(
            &mut state,
            ControllerCommand::UpdateFanout {
                id: fanout_id,
                owner_id: ids[0],
                expected_version: 2,
                payload_json: r#"{"completed":true}"#.into(),
                completed: true,
                now_ms: 11,
                controller_term: 4,
            },
        )
        .is_err());
        apply_command(
            &mut state,
            ControllerCommand::ClaimFanout {
                id: fanout_id,
                owner_id: ids[1],
                expected_version: 2,
                now_ms: 12,
                ttl_ms: 100,
                controller_term: 6,
            },
        )
        .unwrap();
        assert_eq!(state.fanout_outbox[&fanout_id].owner_id, Some(ids[1]));
        assert_eq!(state.fanout_outbox[&fanout_id].authority_term, 6);
    }

    #[test]
    fn nine_node_cell_uses_rf3_not_all_peers() {
        let ids: Vec<_> = (0..9).map(|_| Uuid::new_v4()).collect();
        let config = ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: ids
                .iter()
                .enumerate()
                .map(|(index, id)| crate::cluster::NodeConfig {
                    id: *id,
                    addr: format!("http://n{index}"),
                })
                .collect(),
            node_id: ids[0],
            generation: 1,
            hash_version: 1,
        };
        let state = ControllerState::from_v1(&config, 8, &HashMap::new(), None).unwrap();
        assert_eq!(state.replication_factor, 3);
        assert_eq!(state.min_isr, 2);
        assert_eq!(state.controller_voters.len(), 5);
        for placement in state.shards.values() {
            assert_eq!(placement.replicas.len(), 3);
            assert_eq!(placement.isr.len(), 3);
        }
        let a = &state.shards[&0].replicas;
        let b = &state.shards[&1].replicas;
        assert_ne!(a, b);
    }

    #[test]
    fn catalog_and_gc_commands_are_monotonic() {
        let id = Uuid::new_v4();
        let config = ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: vec![crate::cluster::NodeConfig {
                id,
                addr: "http://n0".into(),
            }],
            node_id: id,
            generation: 1,
            hash_version: 1,
        };
        let mut state = ControllerState::from_v1(&config, 1, &HashMap::new(), None).unwrap();
        apply_command(
            &mut state,
            ControllerCommand::PutCatalogRecord {
                kind: "queue".into(),
                key: "orders".into(),
                payload_json: "{}".into(),
                tombstone: false,
            },
        )
        .unwrap();
        assert_eq!(state.catalog_epoch, 1);
        apply_command(
            &mut state,
            ControllerCommand::SetGcWatermark {
                shard: 0,
                watermark: 10,
            },
        )
        .unwrap();
        assert!(apply_command(
            &mut state,
            ControllerCommand::SetGcWatermark {
                shard: 0,
                watermark: 9,
            },
        )
        .is_err());
    }

    #[test]
    fn fourth_data_node_is_not_an_automatic_voter() {
        let ids: Vec<_> = (0..3).map(|_| Uuid::new_v4()).collect();
        let config = ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: ids
                .iter()
                .enumerate()
                .map(|(index, id)| crate::cluster::NodeConfig {
                    id: *id,
                    addr: format!("http://n{index}"),
                })
                .collect(),
            node_id: ids[0],
            generation: 1,
            hash_version: 1,
        };
        let mut state = ControllerState::from_v1(&config, 2, &HashMap::new(), None).unwrap();
        assert_eq!(state.controller_voters.len(), 3);
        let extra = Uuid::new_v4();
        apply_command(
            &mut state,
            ControllerCommand::RegisterDataNode {
                node: DataNodeRecord {
                    id: extra,
                    addr: "http://n3".into(),
                    rack: None,
                    region: None,
                    broker: true,
                    controller_voter: true,
                },
            },
        )
        .unwrap();
        assert_eq!(state.controller_voters.len(), 3);
        assert!(!state.controller_voters.contains(&extra));
        apply_command(
            &mut state,
            ControllerCommand::AddControllerVoter { node_id: extra },
        )
        .unwrap();
        assert!(state.controller_voters.contains(&extra));
        apply_command(
            &mut state,
            ControllerCommand::RemoveControllerVoter { node_id: extra },
        )
        .unwrap();
        assert!(!state.controller_voters.contains(&extra));
    }

    #[test]
    fn two_node_cell_grows_to_rf3_without_becoming_rf4() {
        let ids = [
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        ];
        let config = ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: ids[..2]
                .iter()
                .enumerate()
                .map(|(index, id)| crate::cluster::NodeConfig {
                    id: *id,
                    addr: format!("http://n{index}"),
                })
                .collect(),
            node_id: ids[0],
            generation: 1,
            hash_version: 1,
        };
        let mut state = ControllerState::from_v1(&config, 4, &HashMap::new(), None).unwrap();
        assert_eq!(state.replication_factor, 2);
        assert_eq!(state.min_isr, 2);
        assert!(state.shards.values().all(|s| s.replicas.len() == 2));

        apply_command(
            &mut state,
            ControllerCommand::RegisterDataNode {
                node: DataNodeRecord {
                    id: ids[2],
                    addr: "http://n2".into(),
                    rack: None,
                    region: None,
                    broker: true,
                    controller_voter: false,
                },
            },
        )
        .unwrap();
        apply_command(
            &mut state,
            ControllerCommand::SetReplicationPolicy {
                factor: 3,
                min_isr: 2,
            },
        )
        .unwrap();
        assert_eq!(state.replication_factor, 3);
        for shard in 0..4 {
            apply_command(
                &mut state,
                ControllerCommand::BeginRebalance {
                    shard,
                    add: Some(ids[2]),
                    remove: None,
                },
            )
            .unwrap();
            apply_command(
                &mut state,
                ControllerCommand::PromoteLearner {
                    shard,
                    node_id: ids[2],
                },
            )
            .unwrap();
            apply_command(&mut state, ControllerCommand::CompleteRebalance { shard }).unwrap();
            assert_eq!(state.shards[&shard].replicas.len(), 3);
            assert!(state.shards[&shard].isr.contains(&ids[2]));
        }

        apply_command(
            &mut state,
            ControllerCommand::RegisterDataNode {
                node: DataNodeRecord {
                    id: ids[3],
                    addr: "http://n3".into(),
                    rack: None,
                    region: None,
                    broker: true,
                    controller_voter: false,
                },
            },
        )
        .unwrap();
        assert_eq!(state.replication_factor, 3);
        assert_eq!(state.data_nodes.len(), 4);
        assert!(state.shards.values().all(|s| s.replicas.len() == 3));
        assert!(state.shards.values().all(|s| !s.replicas.contains(&ids[3])));
    }
}
