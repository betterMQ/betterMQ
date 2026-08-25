use crate::types::{BetterMqConfig, ClusterConfigSection, ConfigError};
use broker_proto::HASH_VERSION;
use broker_raft_meta::{ClusterConfig, ClusterRuntime, NodeConfig};
use std::path::Path;
use uuid::Uuid;

pub fn stable_node_id(key: &str) -> Uuid {
    broker_proto::stable_node_id(key)
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
        hash_version: HASH_VERSION,
    })
}

/// Write `cluster-config.json` from `bettermq.json` when missing or membership changed.
/// Existing node IDs are preserved when peer addresses are unchanged so a hasher
/// upgrade does not reshuffle identity.
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
        if existing.hash_version != 0 && existing.hash_version != HASH_VERSION {
            return Err(ConfigError::Invalid(format!(
                "cluster hash_version {} is unsupported (expected {HASH_VERSION} or 0/legacy)",
                existing.hash_version
            )));
        }
        !addrs_match(&existing.nodes, &desired.nodes)
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

fn addrs_match(a: &[NodeConfig], b: &[NodeConfig]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut aa: Vec<_> = a.iter().map(|n| n.addr.as_str()).collect();
    let mut bb: Vec<_> = b.iter().map(|n| n.addr.as_str()).collect();
    aa.sort_unstable();
    bb.sort_unstable();
    aa == bb
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
        assert_eq!(a.to_string(), "f65058f7-24a3-6df6-b8f3-72e54655c3ff");
    }

    #[test]
    fn builds_three_node_config() {
        let cfg = BetterMqConfig::template_cluster_local();
        let cluster = build_cluster_config(&cfg).expect("build");
        assert_eq!(cluster.nodes.len(), 3);
        assert!(!cluster.preferred_leader_for_shard(0).is_nil());
        assert_eq!(cluster.hash_version, HASH_VERSION);
    }
}
