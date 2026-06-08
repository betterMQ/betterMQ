use crate::types::{BetterMqConfig, ClusterConfigSection, ConfigError};
use broker_raft_meta::{ClusterConfig, ClusterRuntime, NodeConfig};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Path;
use uuid::Uuid;

pub fn stable_node_id(key: &str) -> Uuid {
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    let a = h.finish();
    let mut h2 = DefaultHasher::new();
    format!("bettermq:{key}").hash(&mut h2);
    let b = h2.finish();
    Uuid::from_u128((a as u128) | ((b as u128) << 64))
}

fn cluster_id_from_section(section: &ClusterConfigSection) -> Uuid {
    if let Some(id) = &section.id {
        return stable_node_id(id);
    }
    let mut names: Vec<_> = section.nodes.iter().map(|n| n.name.as_str()).collect();
    names.sort_unstable();
    stable_node_id(&names.join("|"))
}

pub fn build_cluster_config(cfg: &BetterMqConfig) -> Result<ClusterConfig, ConfigError> {
    let section = cfg
        .cluster
        .as_ref()
        .filter(|c| c.enabled)
        .ok_or_else(|| ConfigError::Invalid("cluster not enabled".into()))?;

    let node_id = stable_node_id(&cfg.node.name);
    let nodes: Vec<NodeConfig> = section
        .nodes
        .iter()
        .map(|n| NodeConfig {
            id: stable_node_id(&n.name),
            addr: n.public_url.trim_end_matches('/').to_string(),
        })
        .collect();

    if !nodes.iter().any(|n| n.id == node_id) {
        return Err(ConfigError::Invalid(format!(
            "node.name '{}' not found in cluster.nodes",
            cfg.node.name
        )));
    }

    Ok(ClusterConfig {
        cluster_id: cluster_id_from_section(section),
        nodes,
        node_id,
        generation: 1,
    })
}

/// Write `cluster-config.json` from `bettermq.json` when missing or membership changed.
pub fn ensure_cluster_config(cfg: &BetterMqConfig, data_dir: &Path) -> Result<bool, ConfigError> {
    if !cfg.cluster_enabled() {
        return Ok(false);
    }

    std::fs::create_dir_all(data_dir)?;
    let desired = build_cluster_config(cfg)?;
    let cfg_path = data_dir.join("cluster-config.json");

    let write = if cfg_path.exists() {
        let existing = ClusterRuntime::load_config(data_dir)
            .map_err(|e| ConfigError::Invalid(e.to_string()))?;
        existing.cluster_id != desired.cluster_id
            || existing.nodes.len() != desired.nodes.len()
            || existing.node_id != desired.node_id
            || nodes_differ(&existing.nodes, &desired.nodes)
    } else {
        true
    };

    if write {
        ClusterRuntime::init_cluster_file(data_dir, &desired)
            .map_err(|e| ConfigError::Invalid(e.to_string()))?;
        std::fs::write(cfg_path, serde_json::to_vec_pretty(&desired)?)?;
    }

    Ok(true)
}

fn nodes_differ(a: &[NodeConfig], b: &[NodeConfig]) -> bool {
    if a.len() != b.len() {
        return true;
    }
    for (x, y) in a.iter().zip(b.iter()) {
        if x.id != y.id || x.addr != y.addr {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::BetterMqConfig;

    #[test]
    fn stable_ids_are_deterministic() {
        let a = stable_node_id("broker1");
        let b = stable_node_id("broker1");
        assert_eq!(a, b);
        assert_ne!(a, stable_node_id("broker2"));
    }

    #[test]
    fn builds_three_node_config() {
        let cfg = BetterMqConfig::template_cluster_local();
        let cluster = build_cluster_config(&cfg).expect("build");
        assert_eq!(cluster.nodes.len(), 3);
        assert!(!cluster.preferred_leader_for_shard(0).is_nil());
    }
}
