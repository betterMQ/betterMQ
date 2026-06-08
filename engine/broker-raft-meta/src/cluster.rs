//! Cluster membership, peer health, and shard leader election (CP7b).

use crate::election::{elect_shard_leader, DEFAULT_PEER_TTL_MS};
use chrono::Utc;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
        let dir = PathBuf::from(shared).join("cluster");
        let _ = fs::create_dir_all(&dir);
        return dir.join("cluster.json");
    }
    data_dir.join("cluster.json")
}

#[derive(Clone)]
pub struct ClusterRuntime {
    config: ClusterConfig,
    path: PathBuf,
    state: Arc<Mutex<ClusterStateFile>>,
    peer_ttl_ms: i64,
}

impl ClusterRuntime {
    pub fn open(data_dir: impl AsRef<Path>, config: ClusterConfig) -> Result<Self, ClusterError> {
        let path = cluster_state_path(data_dir.as_ref());
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let state = if path.exists() {
            let bytes = fs::read(&path)?;
            serde_json::from_slice(&bytes).unwrap_or_default()
        } else {
            ClusterStateFile::default()
        };
        Ok(Self {
            config,
            path,
            state: Arc::new(Mutex::new(state)),
            peer_ttl_ms: DEFAULT_PEER_TTL_MS,
        })
    }

    pub fn from_config_only(config: ClusterConfig) -> Self {
        Self {
            config,
            path: PathBuf::from("/dev/null"),
            state: Arc::new(Mutex::new(ClusterStateFile::default())),
            peer_ttl_ms: DEFAULT_PEER_TTL_MS,
        }
    }

    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    pub fn record_self_alive(&self, now_ms: i64) {
        let mut state = self.state.lock();
        state.peer_last_seen_ms.insert(self.config.node_id, now_ms);
        drop(state);
        let _ = self.persist();
    }

    pub fn record_peer_alive(&self, peer_id: Uuid, now_ms: i64) {
        let mut state = self.state.lock();
        state.peer_last_seen_ms.insert(peer_id, now_ms);
        drop(state);
        let _ = self.persist();
    }

    /// Merge peer health observations (gossip). Keeps the newest timestamp per node.
    pub fn merge_peer_health(&self, seen: &HashMap<Uuid, i64>) {
        if seen.is_empty() {
            return;
        }
        let mut state = self.state.lock();
        let mut changed = false;
        for (id, ts) in seen {
            let entry = state.peer_last_seen_ms.entry(*id).or_insert(*ts);
            if *ts > *entry {
                *entry = *ts;
                changed = true;
            }
        }
        drop(state);
        if changed {
            let _ = self.persist();
        }
    }

    pub fn peer_health_snapshot(&self) -> HashMap<Uuid, i64> {
        self.state.lock().peer_last_seen_ms.clone()
    }

    fn peer_alive_in_state(
        state: &ClusterStateFile,
        peer_id: Uuid,
        self_id: Uuid,
        now_ms: i64,
        ttl_ms: i64,
    ) -> bool {
        if peer_id == self_id {
            return true;
        }
        state
            .peer_last_seen_ms
            .get(&peer_id)
            .map(|t| now_ms - *t <= ttl_ms)
            .unwrap_or(false)
    }

    pub fn is_peer_alive(&self, peer_id: Uuid, now_ms: i64) -> bool {
        let state = self.state.lock();
        Self::peer_alive_in_state(
            &state,
            peer_id,
            self.config.node_id,
            now_ms,
            self.peer_ttl_ms,
        )
    }

    /// Elected leader for a shard (CP7b ring failover).
    pub fn elect_leader_for_shard(&self, shard: u32) -> Option<Uuid> {
        let now = Utc::now().timestamp_millis();
        let config = self.config.clone();
        let state = self.state.lock();
        let ttl = self.peer_ttl_ms;
        let node_id = config.node_id;
        let is_alive = |id: Uuid| {
            if id == node_id {
                return true;
            }
            state
                .peer_last_seen_ms
                .get(&id)
                .map(|t| now - *t <= ttl)
                .unwrap_or(false)
        };
        elect_shard_leader(&config.nodes, shard, is_alive)
    }

    pub fn is_leader_for_shard(&self, shard: u32) -> bool {
        self.elect_leader_for_shard(shard) == Some(self.config.node_id)
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

    /// Monotonic fence token for shard leadership (CP6b.2).
    pub fn shard_generation(&self, shard: u32) -> u64 {
        self.state
            .lock()
            .shard_generations
            .get(&shard)
            .copied()
            .unwrap_or(0)
    }

    pub fn bump_shard_generation(&self, shard: u32) -> u64 {
        let mut state = self.state.lock();
        let next = state.shard_generations.get(&shard).copied().unwrap_or(0) + 1;
        state.shard_generations.insert(shard, next);
        drop(state);
        let _ = self.persist();
        next
    }

    pub fn persist(&self) -> Result<(), ClusterError> {
        let state = self.state.lock();
        let tmp = self.path.with_extension("json.tmp");
        fs::write(&tmp, serde_json::to_vec_pretty(&*state)?)?;
        fs::rename(tmp, &self.path)?;
        Ok(())
    }

    /// Try to become scheduler leader (delay/cron tick owner).
    pub fn try_acquire_scheduler_leader(&self, ttl_ms: i64) -> bool {
        let now = Utc::now().timestamp_millis();
        let mut state = self.state.lock();
        let holder_dead = state.scheduler.as_ref().is_none_or(|lease| {
            lease.expires_at_ms <= now
                || !Self::peer_alive_in_state(
                    &state,
                    lease.holder,
                    self.config.node_id,
                    now,
                    self.peer_ttl_ms,
                )
        });
        let we_hold = state
            .scheduler
            .as_ref()
            .is_some_and(|lease| lease.holder == self.config.node_id && lease.expires_at_ms > now);
        if holder_dead || we_hold {
            state.scheduler = Some(SchedulerLease {
                holder: self.config.node_id,
                expires_at_ms: now + ttl_ms,
            });
            drop(state);
            let _ = self.persist();
            true
        } else {
            false
        }
    }

    pub fn scheduler_holder(&self) -> Option<Uuid> {
        let now = Utc::now().timestamp_millis();
        let state = self.state.lock();
        state.scheduler.as_ref().and_then(|l| {
            if l.expires_at_ms > now
                && Self::peer_alive_in_state(
                    &state,
                    l.holder,
                    self.config.node_id,
                    now,
                    self.peer_ttl_ms,
                )
            {
                Some(l.holder)
            } else {
                None
            }
        })
    }

    pub fn is_scheduler_leader(&self) -> bool {
        let now = Utc::now().timestamp_millis();
        let state = self.state.lock();
        match &state.scheduler {
            Some(l) if l.holder == self.config.node_id && l.expires_at_ms > now => true,
            Some(l)
                if !Self::peer_alive_in_state(
                    &state,
                    l.holder,
                    self.config.node_id,
                    now,
                    self.peer_ttl_ms,
                ) =>
            {
                drop(state);
                self.try_acquire_scheduler_leader(5_000)
            }
            _ => false,
        }
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
        fs::write(&path, serde_json::to_vec_pretty(&state)?)?;
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
        };
        let rt = ClusterRuntime::from_config_only(cfg);
        let now = Utc::now().timestamp_millis();
        rt.record_self_alive(now);
        rt.record_peer_alive(n1, now);
        // n2 not recorded → dead
        // shard 1 prefers n2 → should failover to n3
        assert_eq!(elect_shard_leader(&nodes, 1, |id| id != n2), Some(n3));
        assert!(rt.is_leader_for_shard(1));
    }
}
