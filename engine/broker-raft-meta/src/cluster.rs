//! Cluster membership, peer health, and shard leader election (CP7b / HA M2).

use crate::election::{elect_shard_leader, DEFAULT_PEER_TTL_MS};
use crate::epoch::{ControllerLeaderProof, ControllerVoteRequest, ControllerVoteResponse};
use crate::{ControllerCommand, ControllerState, OpenRaftController};
use broker_storage::FileLock;
use chrono::Utc;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum ClusterError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("cluster not configured")]
    NotConfigured,
    #[error("unknown node: {0}")]
    UnknownNode(Uuid),
    #[error("corrupt cluster metadata {path}: {source}")]
    Corrupt {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("controller epoch: {0}")]
    Epoch(#[from] crate::epoch::EpochError),
    #[error("controller campaigning requires quorum votes in a {nodes}-node cluster")]
    QuorumVotesRequired { nodes: usize },
    #[error("controller quorum not reached: {votes}/{quorum} valid votes")]
    ControllerQuorum { votes: usize, quorum: usize },
    #[error("controller vote used cluster generation {saw}, expected {expected}")]
    StaleClusterGeneration { expected: u64, saw: u64 },
    #[error("controller candidate is not a configured member: {0}")]
    InvalidCandidate(Uuid),
    #[error("shard {0} fence term space exhausted")]
    ShardTermExhausted(u32),
    #[error("raft controller: {0}")]
    Controller(#[from] crate::ControllerError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub id: Uuid,
    pub addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub cluster_id: Uuid,
    pub nodes: Vec<NodeConfig>,
    /// This process's node id.
    pub node_id: Uuid,
    /// Cluster membership generation (bumped on join).
    pub generation: u64,
    /// 0 = legacy DefaultHasher IDs; 1 = siphash-1-3 (`broker_proto::HASH_VERSION`).
    #[serde(default)]
    pub hash_version: u32,
}

impl ClusterConfig {
    pub fn node_count(&self) -> usize {
        self.nodes.len().max(1)
    }

    pub fn this_node(&self) -> Result<&NodeConfig, ClusterError> {
        self.nodes
            .iter()
            .find(|n| n.id == self.node_id)
            .ok_or(ClusterError::UnknownNode(self.node_id))
    }

    /// Static preferred leader (before failover).
    pub fn preferred_leader_for_shard(&self, shard: u32) -> Uuid {
        let idx = (shard as usize) % self.node_count();
        self.nodes[idx].id
    }

    pub fn peer_addrs(&self) -> Vec<String> {
        self.nodes
            .iter()
            .filter(|n| n.id != self.node_id)
            .map(|n| n.addr.clone())
            .collect()
    }

    pub fn peer_nodes(&self) -> Vec<NodeConfig> {
        self.nodes
            .iter()
            .filter(|n| n.id != self.node_id)
            .cloned()
            .collect()
    }

    pub fn quorum_size(&self) -> usize {
        self.node_count() / 2 + 1
    }

    pub fn node_addr(&self, id: Uuid) -> Option<String> {
        self.nodes
            .iter()
            .find(|n| n.id == id)
            .map(|n| n.addr.clone())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchedulerLease {
    pub holder: Uuid,
    pub expires_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ClusterStateFile {
    scheduler: Option<SchedulerLease>,
    /// Last time we observed a peer healthy (ms since epoch).
    #[serde(default)]
    peer_last_seen_ms: HashMap<Uuid, i64>,
    #[serde(default)]
    shard_generations: HashMap<u32, u64>,
}

fn cluster_state_path(data_dir: &Path) -> PathBuf {
    if let Ok(shared) = std::env::var("BETTERMQ_SHARED_META_DIR") {
        let shared = shared.trim();
        if !shared.is_empty() {
            let dir = PathBuf::from(shared).join("cluster");
            let _ = fs::create_dir_all(&dir);
            return dir.join("cluster.json");
        }
    }
    data_dir.join("cluster.json")
}

fn load_state_file(path: &Path) -> Result<ClusterStateFile, ClusterError> {
    if !path.exists() {
        return Ok(ClusterStateFile::default());
    }
    let bytes = fs::read(path)?;
    match serde_json::from_slice(&bytes) {
        Ok(v) => Ok(v),
        Err(source) => {
            if broker_proto::allow_empty_metadata_recovery() {
                Ok(ClusterStateFile::default())
            } else {
                Err(ClusterError::Corrupt {
                    path: path.display().to_string(),
                    source,
                })
            }
        }
    }
}

fn write_state_file(path: &Path, state: &ClusterStateFile) -> Result<(), ClusterError> {
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let tmp = PathBuf::from(format!("{}.tmp", path.display()));
    let bytes = serde_json::to_vec_pretty(state)?;
    {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        if let Ok(dir) = fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

#[derive(Clone)]
pub struct ClusterRuntime {
    config: ClusterConfig,
    path: PathBuf,
    data_dir: PathBuf,
    state: Arc<Mutex<ClusterStateFile>>,
    epoch: Arc<Mutex<crate::epoch::ControllerEpoch>>,
    controller_confirmed_at_ms: Arc<AtomicI64>,
    blocked_shards: Arc<Mutex<HashSet<u32>>>,
    controller: Option<OpenRaftController>,
    peer_ttl_ms: i64,
    /// When false (unit tests via `from_config_only`), skip disk/flock.
    durable: bool,
}

impl ClusterRuntime {
    pub fn open(data_dir: impl AsRef<Path>, config: ClusterConfig) -> Result<Self, ClusterError> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let path = cluster_state_path(&data_dir);
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let state = load_state_file(&path)?;
        let epoch = crate::epoch::ControllerEpoch::load(&data_dir)?;
        Ok(Self {
            config,
            path,
            data_dir,
            state: Arc::new(Mutex::new(state)),
            epoch: Arc::new(Mutex::new(epoch)),
            controller_confirmed_at_ms: Arc::new(AtomicI64::new(0)),
            blocked_shards: Arc::new(Mutex::new(HashSet::new())),
            controller: None,
            peer_ttl_ms: DEFAULT_PEER_TTL_MS,
            durable: true,
        })
    }

    pub fn from_config_only(config: ClusterConfig) -> Self {
        Self {
            config,
            path: PathBuf::from("/dev/null"),
            data_dir: PathBuf::from("/dev/null"),
            state: Arc::new(Mutex::new(ClusterStateFile::default())),
            epoch: Arc::new(Mutex::new(crate::epoch::ControllerEpoch::default())),
            controller_confirmed_at_ms: Arc::new(AtomicI64::new(Utc::now().timestamp_millis())),
            blocked_shards: Arc::new(Mutex::new(HashSet::new())),
            controller: None,
            peer_ttl_ms: DEFAULT_PEER_TTL_MS,
            durable: false,
        }
    }

    /// Open the production OpenRaft controller and migrate V1 metadata as the
    /// initial state-machine value. V1 files remain readable for rollback but
    /// are no longer an authority after this returns.
    pub async fn open_with_raft(
        data_dir: impl AsRef<Path>,
        config: ClusterConfig,
        shard_count: u32,
    ) -> Result<Self, ClusterError> {
        let mut runtime = Self::open(data_dir, config.clone())?;
        let legacy = runtime.state.lock().clone();
        let bootstrap = ControllerState::from_v1(
            &config,
            shard_count,
            &legacy.shard_generations,
            legacy.scheduler,
        )?;
        runtime.controller =
            Some(OpenRaftController::open(&runtime.data_dir, config, bootstrap).await?);
        Ok(runtime)
    }

    pub fn controller(&self) -> Option<&OpenRaftController> {
        self.controller.as_ref()
    }

    /// True when cluster.json lives under `BETTERMQ_SHARED_META_DIR` (required for HA leases).
    pub fn uses_shared_meta(&self) -> bool {
        self.durable
            && std::env::var("BETTERMQ_SHARED_META_DIR")
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false)
    }

    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// Flock → reload disk → mutate → persist → refresh memory.
    fn with_locked_mutate<R>(
        &self,
        f: impl FnOnce(&mut ClusterStateFile) -> R,
    ) -> Result<R, ClusterError> {
        if !self.durable {
            let mut state = self.state.lock();
            return Ok(f(&mut state));
        }
        let _lock = FileLock::exclusive(&self.path)?;
        let mut disk = load_state_file(&self.path)?;
        let out = f(&mut disk);
        write_state_file(&self.path, &disk)?;
        *self.state.lock() = disk;
        Ok(out)
    }

    fn refresh_from_disk(&self) {
        if !self.durable {
            return;
        }
        if let Ok(_lock) = FileLock::exclusive(&self.path) {
            if let Ok(disk) = load_state_file(&self.path) {
                *self.state.lock() = disk;
            }
        }
    }

    pub fn record_self_alive(&self, now_ms: i64) {
        let _ = self.with_locked_mutate(|state| {
            state.peer_last_seen_ms.insert(self.config.node_id, now_ms);
        });
    }

    pub fn record_peer_alive(&self, peer_id: Uuid, now_ms: i64) {
        let _ = self.with_locked_mutate(|state| {
            state.peer_last_seen_ms.insert(peer_id, now_ms);
        });
    }

    /// Merge peer health observations (gossip). Keeps the newest timestamp per node.
    pub fn merge_peer_health(&self, seen: &HashMap<Uuid, i64>) {
        if seen.is_empty() {
            return;
        }
        let _ = self.with_locked_mutate(|state| {
            for (id, ts) in seen {
                let entry = state.peer_last_seen_ms.entry(*id).or_insert(*ts);
                if *ts > *entry {
                    *entry = *ts;
                }
            }
        });
    }

    pub fn peer_health_snapshot(&self) -> HashMap<Uuid, i64> {
        self.refresh_from_disk();
        self.state.lock().peer_last_seen_ms.clone()
    }

    fn peer_alive_in_state(
        state: &ClusterStateFile,
        peer_id: Uuid,
        now_ms: i64,
        ttl_ms: i64,
    ) -> bool {
        state
            .peer_last_seen_ms
            .get(&peer_id)
            .map(|t| now_ms - *t <= ttl_ms)
            .unwrap_or(false)
    }

    /// Isolated nodes must not keep leading: require a quorum that includes
    /// at least one *other* peer (solo clusters of 1 node still lead).
    fn has_election_quorum(
        state: &ClusterStateFile,
        config: &ClusterConfig,
        now_ms: i64,
        ttl_ms: i64,
    ) -> bool {
        let n = config.node_count();
        if n <= 1 {
            return true;
        }
        let others_alive = config
            .nodes
            .iter()
            .filter(|node| {
                node.id != config.node_id
                    && Self::peer_alive_in_state(state, node.id, now_ms, ttl_ms)
            })
            .count();
        others_alive + 1 >= config.quorum_size()
    }

    pub fn is_peer_alive(&self, peer_id: Uuid, now_ms: i64) -> bool {
        if self
            .controller
            .as_ref()
            .is_some_and(|controller| !controller.is_ready())
        {
            return false;
        }
        self.refresh_from_disk();
        let state = self.state.lock();
        Self::peer_alive_in_state(&state, peer_id, now_ms, self.peer_ttl_ms)
    }

    /// Elected leader for a shard (CP7b ring failover). `None` if no alive peers.
    pub fn elect_leader_for_shard(&self, shard: u32) -> Option<Uuid> {
        if let Some(controller) = &self.controller {
            if !controller.is_ready() {
                return None;
            }
            return controller
                .state()
                .shards
                .get(&shard)
                .map(|placement| placement.leader_id);
        }
        self.refresh_from_disk();
        let now = Utc::now().timestamp_millis();
        let config = self.config.clone();
        let state = self.state.lock();
        let ttl = self.peer_ttl_ms;
        let node_id = config.node_id;
        let quorum = Self::has_election_quorum(&state, &config, now, ttl);
        let is_alive = |id: Uuid| {
            if id == node_id {
                return quorum;
            }
            Self::peer_alive_in_state(&state, id, now, ttl)
        };
        elect_shard_leader(&config.nodes, shard, is_alive)
    }

    /// Health-based placement recommendation. Only the current OpenRaft
    /// leader may commit this recommendation as authoritative placement.
    pub fn desired_leader_for_shard(&self, shard: u32) -> Option<Uuid> {
        self.refresh_from_disk();
        let now = Utc::now().timestamp_millis();
        let config = self.config.clone();
        let state = self.state.lock();
        let ttl = self.peer_ttl_ms;
        let node_id = config.node_id;
        let quorum = Self::has_election_quorum(&state, &config, now, ttl);
        let is_alive = |id: Uuid| {
            if id == node_id {
                return quorum;
            }
            Self::peer_alive_in_state(&state, id, now, ttl)
        };
        elect_shard_leader(&config.nodes, shard, is_alive)
    }

    pub fn is_controller_leader(&self) -> bool {
        self.controller
            .as_ref()
            .is_some_and(|controller| controller.is_leader_with_quorum())
    }

    pub fn is_leader_for_shard(&self, shard: u32) -> bool {
        (!self.durable || self.has_controller_lease())
            && !self.blocked_shards.lock().contains(&shard)
            && self.elect_leader_for_shard(shard) == Some(self.config.node_id)
    }

    /// Replica set for a shard. Falls back to the full node list when no
    /// controller snapshot is available (single-node / pre-RF clusters).
    pub fn replica_ids_for_shard(&self, shard: u32) -> Vec<Uuid> {
        if let Some(controller) = &self.controller {
            if let Some(placement) = controller.state().shards.get(&shard) {
                if !placement.replicas.is_empty() {
                    return placement.replicas.clone();
                }
            }
        }
        self.config.nodes.iter().map(|n| n.id).collect()
    }

    pub fn replication_targets(&self, shard: u32) -> (Vec<NodeConfig>, usize) {
        let replicas = self.replica_ids_for_shard(shard);
        let peers = self
            .config
            .peer_nodes()
            .into_iter()
            .filter(|peer| replicas.contains(&peer.id))
            .collect::<Vec<_>>();
        let rf = replicas.len().max(1);
        let min_isr = self
            .controller
            .as_ref()
            .map(|c| c.state().min_isr as usize)
            .filter(|v| *v > 0)
            .unwrap_or(if rf >= 3 { 2 } else { 1 })
            .clamp(1, rf);
        (peers, min_isr)
    }

    pub fn is_replica_for_shard(&self, shard: u32) -> bool {
        self.replica_ids_for_shard(shard)
            .contains(&self.config.node_id)
    }

    /// Temporarily fence writes/dispatch while a newly acquired shard catches up.
    pub fn set_shard_ready(&self, shard: u32, ready: bool) {
        let mut blocked = self.blocked_shards.lock();
        if ready {
            blocked.remove(&shard);
        } else {
            blocked.insert(shard);
        }
    }

    pub fn shard_ready(&self, shard: u32) -> bool {
        !self.blocked_shards.lock().contains(&shard)
    }

    pub fn leader_http_base_for_shard(&self, shard: u32) -> Option<String> {
        let leader = self.elect_leader_for_shard(shard)?;
        self.config.node_addr(leader)
    }

    /// Shards this node currently leads (for logging / tests).
    pub fn led_shards(&self, max_shard: u32) -> Vec<u32> {
        (0..max_shard)
            .filter(|s| self.is_leader_for_shard(*s))
            .collect()
    }

    /// Monotonic fence token for shard leadership (shared under flock).
    pub fn shard_generation(&self, shard: u32) -> u64 {
        if let Some(controller) = &self.controller {
            return controller
                .state()
                .shards
                .get(&shard)
                .map(|placement| placement.leader_term)
                .unwrap_or(0);
        }
        self.refresh_from_disk();
        self.state
            .lock()
            .shard_generations
            .get(&shard)
            .copied()
            .unwrap_or(0)
    }

    /// Observe a peer's generation; adopt if higher (follower fence catch-up).
    /// Rejects pathological jumps (e.g. `u64::MAX`) from a compromised peer.
    pub fn try_observe_shard_generation(
        &self,
        shard: u32,
        generation: u64,
    ) -> Result<u64, ClusterError> {
        if let Some(controller) = &self.controller {
            let state = controller.state();
            let placement = state.shards.get(&shard).ok_or_else(|| {
                ClusterError::Controller(crate::ControllerError::InvalidCommand(format!(
                    "unknown shard {shard}"
                )))
            })?;
            if placement.leader_term != generation {
                return Err(ClusterError::Controller(
                    crate::ControllerError::InvalidCommand(format!(
                        "term {generation} is not committed controller term {}",
                        placement.leader_term
                    )),
                ));
            }
            return Ok(generation);
        }
        const MAX_JUMP: u64 = 1024;
        self.with_locked_mutate(|state| {
            let cur = state.shard_generations.get(&shard).copied().unwrap_or(0);
            if generation > cur && generation <= cur.saturating_add(MAX_JUMP) {
                state.shard_generations.insert(shard, generation);
            }
        })?;
        Ok(self.shard_generation(shard))
    }

    pub fn observe_shard_generation(&self, shard: u32, generation: u64) -> u64 {
        self.try_observe_shard_generation(shard, generation)
            .unwrap_or_else(|_| self.shard_generation(shard))
    }

    pub fn try_bump_shard_generation(&self, shard: u32) -> Result<u64, ClusterError> {
        if self.controller.is_some() {
            return Err(ClusterError::Controller(
                crate::ControllerError::InvalidCommand(
                    "use async OpenRaft shard leadership acquisition".into(),
                ),
            ));
        }
        self.with_locked_mutate(|state| -> Result<u64, ClusterError> {
            let next = state
                .shard_generations
                .get(&shard)
                .copied()
                .unwrap_or(0)
                .checked_add(1)
                .filter(|term| *term != u64::MAX)
                .ok_or(ClusterError::ShardTermExhausted(shard))?;
            state.shard_generations.insert(shard, next);
            Ok(next)
        })?
    }

    pub fn bump_shard_generation(&self, shard: u32) -> u64 {
        self.try_bump_shard_generation(shard).unwrap_or(0)
    }

    pub async fn acquire_shard_leadership(&self, shard: u32) -> Result<u64, ClusterError> {
        self.assign_shard_leadership(shard, self.config.node_id)
            .await
    }

    pub async fn assign_shard_leadership(
        &self,
        shard: u32,
        leader_id: Uuid,
    ) -> Result<u64, ClusterError> {
        if let Some(controller) = &self.controller {
            let state = controller
                .submit(ControllerCommand::AcquireShardLeadership { shard, leader_id })
                .await?;
            return state
                .shards
                .get(&shard)
                .map(|placement| placement.leader_term)
                .ok_or_else(|| {
                    ClusterError::Controller(crate::ControllerError::InvalidCommand(format!(
                        "unknown shard {shard}"
                    )))
                });
        }
        if leader_id != self.config.node_id {
            return Err(ClusterError::Controller(
                crate::ControllerError::InvalidCommand(
                    "V1 controller can only acquire local shard leadership".into(),
                ),
            ));
        }
        self.try_bump_shard_generation(shard)
    }

    pub fn validate_shard_term(&self, shard: u32, leader_id: Uuid, term: u64) -> bool {
        if let Some(controller) = &self.controller {
            return controller
                .state()
                .shards
                .get(&shard)
                .is_some_and(|placement| {
                    placement.leader_id == leader_id && placement.leader_term == term
                });
        }
        self.elect_leader_for_shard(shard) == Some(leader_id)
            && self.shard_generation(shard) == term
    }

    /// Durable controller term used to fence stale leaders.
    pub fn controller_term(&self) -> u64 {
        if let Some(controller) = &self.controller {
            return controller.term();
        }
        self.epoch.lock().current_term
    }

    /// Compatibility entry point. A one-node cluster can campaign locally.
    /// Multi-node callers must use `begin_controller_campaign`, gather votes
    /// from peers, then call `confirm_controller_leader`.
    pub fn campaign_controller(&self) -> Result<u64, ClusterError> {
        if self.config.node_count() > 1 {
            return Err(ClusterError::QuorumVotesRequired {
                nodes: self.config.node_count(),
            });
        }
        let mut epoch = self.epoch.lock();
        if self.durable {
            let term = epoch.become_leader(&self.data_dir, self.config.node_id)?;
            self.controller_confirmed_at_ms
                .store(Utc::now().timestamp_millis(), Ordering::Release);
            Ok(term)
        } else {
            epoch.current_term = epoch
                .current_term
                .checked_add(1)
                .filter(|term| *term != u64::MAX)
                .ok_or(crate::epoch::EpochError::TermExhausted)?;
            epoch.leader_id = Some(self.config.node_id);
            epoch.voted_for = Some(self.config.node_id);
            self.controller_confirmed_at_ms
                .store(Utc::now().timestamp_millis(), Ordering::Release);
            Ok(epoch.current_term)
        }
    }

    pub fn begin_controller_campaign(&self) -> Result<ControllerVoteRequest, ClusterError> {
        let mut epoch = self.epoch.lock();
        let term = if self.durable {
            epoch.begin_campaign(&self.data_dir, self.config.node_id)?
        } else {
            epoch.current_term = epoch
                .current_term
                .checked_add(1)
                .filter(|term| *term != u64::MAX)
                .ok_or(crate::epoch::EpochError::TermExhausted)?;
            epoch.voted_for = Some(self.config.node_id);
            epoch.leader_id = None;
            epoch.current_term
        };
        Ok(ControllerVoteRequest {
            term,
            candidate_id: self.config.node_id,
            cluster_generation: self.config.generation,
        })
    }

    pub fn vote_controller(
        &self,
        request: &ControllerVoteRequest,
    ) -> Result<ControllerVoteResponse, ClusterError> {
        if request.cluster_generation != self.config.generation {
            return Err(ClusterError::StaleClusterGeneration {
                expected: self.config.generation,
                saw: request.cluster_generation,
            });
        }
        if !self
            .config
            .nodes
            .iter()
            .any(|node| node.id == request.candidate_id)
        {
            return Err(ClusterError::InvalidCandidate(request.candidate_id));
        }
        let mut epoch = self.epoch.lock();
        let granted = if self.durable {
            epoch.grant_vote(&self.data_dir, request.term, request.candidate_id)?
        } else {
            if request.term < epoch.current_term {
                return Err(crate::epoch::EpochError::StaleTerm {
                    have: epoch.current_term,
                    saw: request.term,
                }
                .into());
            }
            if request.term > epoch.current_term {
                epoch.current_term = request.term;
                epoch.voted_for = None;
                epoch.leader_id = None;
            }
            match epoch.voted_for {
                Some(id) if id != request.candidate_id => false,
                _ => {
                    epoch.voted_for = Some(request.candidate_id);
                    true
                }
            }
        };
        Ok(ControllerVoteResponse {
            term: epoch.current_term,
            voter_id: self.config.node_id,
            candidate_id: request.candidate_id,
            granted,
        })
    }

    pub fn confirm_controller_leader(
        &self,
        request: &ControllerVoteRequest,
        responses: &[ControllerVoteResponse],
    ) -> Result<ControllerLeaderProof, ClusterError> {
        if request.candidate_id != self.config.node_id {
            return Err(ClusterError::InvalidCandidate(request.candidate_id));
        }
        let members: HashSet<_> = self.config.nodes.iter().map(|node| node.id).collect();
        let mut voters = HashSet::from([self.config.node_id]);
        for response in responses {
            if response.granted
                && response.term == request.term
                && response.candidate_id == request.candidate_id
                && members.contains(&response.voter_id)
            {
                voters.insert(response.voter_id);
            }
        }
        let quorum = self.config.quorum_size();
        if voters.len() < quorum {
            return Err(ClusterError::ControllerQuorum {
                votes: voters.len(),
                quorum,
            });
        }
        let mut epoch = self.epoch.lock();
        if self.durable {
            epoch.confirm_leader(&self.data_dir, request.term, request.candidate_id)?;
        } else {
            epoch.current_term = request.term;
            epoch.voted_for = Some(request.candidate_id);
            epoch.leader_id = Some(request.candidate_id);
        }
        self.controller_confirmed_at_ms
            .store(Utc::now().timestamp_millis(), Ordering::Release);
        Ok(ControllerLeaderProof {
            term: request.term,
            leader_id: request.candidate_id,
            cluster_generation: request.cluster_generation,
        })
    }

    pub fn observe_controller_leader(
        &self,
        proof: &ControllerLeaderProof,
    ) -> Result<(), ClusterError> {
        if proof.cluster_generation != self.config.generation {
            return Err(ClusterError::StaleClusterGeneration {
                expected: self.config.generation,
                saw: proof.cluster_generation,
            });
        }
        self.observe_controller_term(proof.term, proof.leader_id)?;
        self.controller_confirmed_at_ms
            .store(Utc::now().timestamp_millis(), Ordering::Release);
        Ok(())
    }

    pub fn controller_leader_proof(&self) -> Option<ControllerLeaderProof> {
        let epoch = self.epoch.lock();
        Some(ControllerLeaderProof {
            term: epoch.current_term,
            leader_id: epoch.leader_id?,
            cluster_generation: self.config.generation,
        })
    }

    pub fn has_controller_lease(&self) -> bool {
        if let Some(controller) = &self.controller {
            return controller.is_ready();
        }
        if self.config.node_count() <= 1 {
            return true;
        }
        let confirmed = self.controller_confirmed_at_ms.load(Ordering::Acquire);
        confirmed > 0 && Utc::now().timestamp_millis().saturating_sub(confirmed) <= self.peer_ttl_ms
    }

    /// Deterministic live candidate. This is only candidate selection; votes
    /// are still required before it receives a controller lease.
    pub fn controller_candidate(&self) -> Option<Uuid> {
        self.refresh_from_disk();
        let now = Utc::now().timestamp_millis();
        let state = self.state.lock();
        if !Self::has_election_quorum(&state, &self.config, now, self.peer_ttl_ms) {
            return None;
        }
        self.config.nodes.iter().find_map(|node| {
            let alive = node.id == self.config.node_id
                || Self::peer_alive_in_state(&state, node.id, now, self.peer_ttl_ms);
            alive.then_some(node.id)
        })
    }

    pub fn observe_controller_term(&self, term: u64, leader: Uuid) -> Result<u64, ClusterError> {
        let mut epoch = self.epoch.lock();
        if self.durable {
            Ok(epoch.observe_term(&self.data_dir, term, leader)?)
        } else if term < epoch.current_term {
            Err(crate::epoch::EpochError::StaleTerm {
                have: epoch.current_term,
                saw: term,
            }
            .into())
        } else {
            epoch.current_term = term;
            epoch.leader_id = Some(leader);
            Ok(term)
        }
    }

    /// Try to become scheduler leader via CAS under flock (delay/cron tick owner).
    pub fn try_acquire_scheduler_leader(&self, ttl_ms: i64) -> bool {
        if self.controller.is_some() {
            return false;
        }
        let now = Utc::now().timestamp_millis();
        let node_id = self.config.node_id;
        let ttl = self.peer_ttl_ms;
        self.with_locked_mutate(|state| {
            let holder_dead = state.scheduler.as_ref().is_none_or(|lease| {
                lease.expires_at_ms <= now
                    || !Self::peer_alive_in_state(state, lease.holder, now, ttl)
            });
            let we_hold = state
                .scheduler
                .as_ref()
                .is_some_and(|lease| lease.holder == node_id && lease.expires_at_ms > now);
            if holder_dead || we_hold {
                state.scheduler = Some(SchedulerLease {
                    holder: node_id,
                    expires_at_ms: now + ttl_ms,
                });
                true
            } else {
                false
            }
        })
        .unwrap_or(false)
    }

    pub fn scheduler_holder(&self) -> Option<Uuid> {
        if let Some(controller) = &self.controller {
            let now = Utc::now().timestamp_millis();
            return controller
                .state()
                .scheduler
                .filter(|lease| lease.expires_at_ms > now)
                .map(|lease| lease.holder);
        }
        self.refresh_from_disk();
        let now = Utc::now().timestamp_millis();
        let state = self.state.lock();
        state.scheduler.as_ref().and_then(|l| {
            if l.expires_at_ms > now
                && Self::peer_alive_in_state(&state, l.holder, now, self.peer_ttl_ms)
            {
                Some(l.holder)
            } else {
                None
            }
        })
    }

    pub fn is_scheduler_leader(&self) -> bool {
        if self.controller.is_some() {
            return self.scheduler_holder() == Some(self.config.node_id)
                && self.has_controller_lease();
        }
        self.refresh_from_disk();
        let now = Utc::now().timestamp_millis();
        let state = self.state.lock();
        match &state.scheduler {
            Some(l) if l.holder == self.config.node_id && l.expires_at_ms > now => true,
            Some(l) if !Self::peer_alive_in_state(&state, l.holder, now, self.peer_ttl_ms) => {
                drop(state);
                self.try_acquire_scheduler_leader(5_000)
            }
            _ => false,
        }
    }

    pub async fn acquire_scheduler_leader(&self, ttl_ms: i64) -> Result<bool, ClusterError> {
        if let Some(controller) = &self.controller {
            let now_ms = Utc::now().timestamp_millis();
            if controller.state().scheduler.is_some_and(|lease| {
                lease.holder != self.config.node_id && lease.expires_at_ms > now_ms
            }) {
                return Ok(false);
            }
            let state = controller
                .submit(ControllerCommand::AcquireScheduler {
                    holder: self.config.node_id,
                    now_ms,
                    ttl_ms,
                })
                .await?;
            return Ok(state
                .scheduler
                .is_some_and(|lease| lease.holder == self.config.node_id));
        }
        Ok(self.try_acquire_scheduler_leader(ttl_ms))
    }

    pub async fn advance_catalog_epoch(&self) -> Result<u64, ClusterError> {
        let Some(controller) = &self.controller else {
            return Ok(0);
        };
        let state = controller
            .submit(ControllerCommand::AdvanceCatalogEpoch)
            .await?;
        Ok(state.catalog_epoch)
    }

    pub async fn put_catalog_record(
        &self,
        kind: String,
        key: String,
        payload_json: String,
        tombstone: bool,
    ) -> Result<u64, ClusterError> {
        let Some(controller) = &self.controller else {
            return Ok(0);
        };
        let state = controller
            .submit(ControllerCommand::PutCatalogRecord {
                kind,
                key,
                payload_json,
                tombstone,
            })
            .await?;
        Ok(state.catalog_epoch)
    }

    pub fn catalog_epoch(&self) -> u64 {
        self.controller
            .as_ref()
            .map(|controller| controller.state().catalog_epoch)
            .unwrap_or(0)
    }

    pub fn init_cluster_file(
        data_dir: impl AsRef<Path>,
        config: &ClusterConfig,
    ) -> Result<(), ClusterError> {
        let path = cluster_state_path(data_dir.as_ref());
        if path.exists() {
            return Ok(());
        }
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let state = ClusterStateFile::default();
        write_state_file(&path, &state)?;
        let cfg_path = data_dir.as_ref().join("cluster-config.json");
        fs::write(cfg_path, serde_json::to_vec_pretty(config)?)?;
        Ok(())
    }

    pub fn load_config(data_dir: impl AsRef<Path>) -> Result<ClusterConfig, ClusterError> {
        let cfg_path = data_dir.as_ref().join("cluster-config.json");
        if !cfg_path.exists() {
            return Err(ClusterError::NotConfigured);
        }
        let bytes = fs::read(cfg_path)?;
        Ok(serde_json::from_slice(&bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::election::elect_shard_leader;

    #[test]
    fn shard_leader_round_robin_when_all_healthy() {
        let n1 = Uuid::new_v4();
        let n2 = Uuid::new_v4();
        let cfg = ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: vec![
                NodeConfig {
                    id: n1,
                    addr: "http://n1:8080".into(),
                },
                NodeConfig {
                    id: n2,
                    addr: "http://n2:8080".into(),
                },
            ],
            node_id: n1,
            generation: 1,
            hash_version: 1,
        };
        let rt = ClusterRuntime::from_config_only(cfg);
        let now = Utc::now().timestamp_millis();
        rt.record_self_alive(now);
        rt.record_peer_alive(n2, now);
        assert!(rt.is_leader_for_shard(0));
        assert!(!rt.is_leader_for_shard(1));
        assert_eq!(rt.elect_leader_for_shard(1), Some(n2));
    }

    #[test]
    fn failover_when_peer_marked_dead() {
        let n1 = Uuid::new_v4();
        let n2 = Uuid::new_v4();
        let n3 = Uuid::new_v4();
        let nodes = vec![
            NodeConfig {
                id: n1,
                addr: "http://n1:8080".into(),
            },
            NodeConfig {
                id: n2,
                addr: "http://n2:8080".into(),
            },
            NodeConfig {
                id: n3,
                addr: "http://n3:8080".into(),
            },
        ];
        let cfg = ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: nodes.clone(),
            node_id: n3,
            generation: 1,
            hash_version: 1,
        };
        let rt = ClusterRuntime::from_config_only(cfg);
        let now = Utc::now().timestamp_millis();
        rt.record_self_alive(now);
        // n1 preferred for shard 0 is stale → n2 then n3
        rt.record_peer_alive(n2, now);
        assert_eq!(
            elect_shard_leader(&nodes, 0, |id| id == n2 || id == n3),
            Some(n2)
        );
        assert_eq!(rt.elect_leader_for_shard(0), Some(n2));
    }

    #[test]
    fn scheduler_lease_cas_under_shared_file() {
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("BETTERMQ_SHARED_META_DIR", dir.path());
        let n1 = Uuid::new_v4();
        let n2 = Uuid::new_v4();
        let cfg1 = ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: vec![
                NodeConfig {
                    id: n1,
                    addr: "http://n1:8080".into(),
                },
                NodeConfig {
                    id: n2,
                    addr: "http://n2:8080".into(),
                },
            ],
            node_id: n1,
            generation: 1,
            hash_version: 1,
        };
        let cfg2 = ClusterConfig {
            cluster_id: cfg1.cluster_id,
            nodes: cfg1.nodes.clone(),
            node_id: n2,
            generation: 1,
            hash_version: 1,
        };
        let r1 = ClusterRuntime::open(dir.path(), cfg1).unwrap();
        let r2 = ClusterRuntime::open(dir.path(), cfg2).unwrap();
        let now = Utc::now().timestamp_millis();
        r1.record_self_alive(now);
        r1.record_peer_alive(n2, now);
        r2.record_self_alive(now);
        r2.record_peer_alive(n1, now);
        assert!(r1.try_acquire_scheduler_leader(5_000));
        assert!(r1.is_scheduler_leader());
        assert!(!r2.try_acquire_scheduler_leader(5_000));
        assert!(!r2.is_scheduler_leader());
        std::env::remove_var("BETTERMQ_SHARED_META_DIR");
    }

    #[test]
    fn multi_node_controller_requires_distinct_quorum_votes() {
        let ids: Vec<_> = (0..3).map(|_| Uuid::new_v4()).collect();
        let nodes: Vec<_> = ids
            .iter()
            .enumerate()
            .map(|(index, id)| NodeConfig {
                id: *id,
                addr: format!("http://n{index}:8080"),
            })
            .collect();
        let cluster_id = Uuid::new_v4();
        let runtimes: Vec<_> = ids
            .iter()
            .map(|id| {
                ClusterRuntime::from_config_only(ClusterConfig {
                    cluster_id,
                    nodes: nodes.clone(),
                    node_id: *id,
                    generation: 9,
                    hash_version: 1,
                })
            })
            .collect();

        assert!(matches!(
            runtimes[0].campaign_controller(),
            Err(ClusterError::QuorumVotesRequired { nodes: 3 })
        ));
        let request = runtimes[0].begin_controller_campaign().unwrap();
        assert!(matches!(
            runtimes[0].confirm_controller_leader(&request, &[]),
            Err(ClusterError::ControllerQuorum {
                votes: 1,
                quorum: 2
            })
        ));
        let vote = runtimes[1].vote_controller(&request).unwrap();
        let proof = runtimes[0]
            .confirm_controller_leader(&request, &[vote.clone(), vote])
            .unwrap();
        assert_eq!(proof.leader_id, ids[0]);
        assert!(runtimes[0].has_controller_lease());
    }

    #[test]
    fn conflicting_controller_vote_is_rejected() {
        let (first, second, voter) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let nodes = vec![
            NodeConfig {
                id: first,
                addr: "http://n0".into(),
            },
            NodeConfig {
                id: second,
                addr: "http://n1".into(),
            },
            NodeConfig {
                id: voter,
                addr: "http://n2".into(),
            },
        ];
        let rt = ClusterRuntime::from_config_only(ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes,
            node_id: voter,
            generation: 1,
            hash_version: 1,
        });
        let a = ControllerVoteRequest {
            term: 7,
            candidate_id: first,
            cluster_generation: 1,
        };
        let b = ControllerVoteRequest {
            candidate_id: second,
            ..a.clone()
        };
        assert!(rt.vote_controller(&a).unwrap().granted);
        assert!(!rt.vote_controller(&b).unwrap().granted);
    }
}
