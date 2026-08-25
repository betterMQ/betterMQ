//! Fixed-RF replica placement and failure-domain aware assignment.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

pub const DEFAULT_RF: u32 = 3;
pub const DEFAULT_MIN_ISR: u32 = 2;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DataNodeRecord {
    pub id: Uuid,
    pub addr: String,
    #[serde(default)]
    pub rack: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default = "default_true")]
    pub broker: bool,
    #[serde(default)]
    pub controller_voter: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct CatalogRecord {
    pub kind: String,
    pub key: String,
    pub version: u64,
    pub payload_json: String,
    #[serde(default)]
    pub tombstone: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct RebalanceOp {
    pub shard: u32,
    #[serde(default)]
    pub adding: Vec<Uuid>,
    #[serde(default)]
    pub removing: Vec<Uuid>,
    pub generation: u64,
}

pub fn replication_policy(node_count: usize) -> (u32, u32) {
    if node_count <= 1 {
        (1, 1)
    } else {
        let rf = DEFAULT_RF.min(node_count as u32).max(1);
        let min_isr = DEFAULT_MIN_ISR.min(rf).max(1);
        (rf, min_isr)
    }
}

/// Place `rf` replicas for `shard` across nodes, preferring distinct racks.
pub fn place_replicas(nodes: &[DataNodeRecord], shard: u32, rf: usize) -> Vec<Uuid> {
    let brokers: Vec<_> = nodes.iter().filter(|n| n.broker).collect();
    if brokers.is_empty() {
        return Vec::new();
    }
    let rf = rf.clamp(1, brokers.len());
    let start = (shard as usize) % brokers.len();
    let mut chosen = Vec::with_capacity(rf);
    let mut used_racks = BTreeSet::new();
    let mut used_ids = BTreeSet::new();

    let mut idx = start;
    for _ in 0..brokers.len() {
        if chosen.len() >= rf {
            break;
        }
        let node = brokers[idx];
        idx = (idx + 1) % brokers.len();
        let rack = node.rack.as_deref().unwrap_or("");
        if !rack.is_empty() && used_racks.contains(rack) {
            continue;
        }
        chosen.push(node.id);
        used_ids.insert(node.id);
        if !rack.is_empty() {
            used_racks.insert(rack.to_string());
        }
    }
    idx = start;
    while chosen.len() < rf {
        let node = brokers[idx];
        idx = (idx + 1) % brokers.len();
        if used_ids.insert(node.id) {
            chosen.push(node.id);
        }
        if idx == start {
            break;
        }
    }
    chosen
}

pub fn pick_leader(replicas: &[Uuid], shard: u32) -> Option<Uuid> {
    if replicas.is_empty() {
        return None;
    }
    Some(replicas[(shard as usize) % replicas.len()])
}

pub fn min_isr_for(rf: u32, configured: u32) -> u32 {
    configured.clamp(1, rf.max(1))
}

/// Fixed controller quorum: 1, 2, 3, or 5 voters. Extra data nodes are not voters.
pub fn initial_controller_voters(nodes: &[DataNodeRecord]) -> Vec<Uuid> {
    let count = match nodes.len() {
        0 => 0,
        1 => 1,
        2 => 2,
        3 | 4 => 3,
        _ => 5,
    };
    nodes.iter().take(count).map(|node| node.id).collect()
}

pub fn catalog_key(kind: &str, key: &str) -> String {
    format!("{kind}:{key}")
}

pub fn upsert_catalog(
    records: &mut BTreeMap<String, CatalogRecord>,
    kind: String,
    key: String,
    payload_json: String,
    tombstone: bool,
) -> u64 {
    let map_key = catalog_key(&kind, &key);
    let version = records
        .get(&map_key)
        .map(|r| r.version.saturating_add(1))
        .unwrap_or(1);
    records.insert(
        map_key,
        CatalogRecord {
            kind,
            key,
            version,
            payload_json,
            tombstone,
        },
    );
    version
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str, rack: Option<&str>) -> DataNodeRecord {
        DataNodeRecord {
            id: Uuid::new_v4(),
            addr: format!("http://{name}"),
            rack: rack.map(str::to_string),
            region: None,
            broker: true,
            controller_voter: true,
        }
    }

    #[test]
    fn rf3_uses_three_replicas_not_all_nodes() {
        let nodes: Vec<_> = (0..9).map(|i| node(&format!("n{i}"), None)).collect();
        let ids = place_replicas(&nodes, 0, 3);
        assert_eq!(ids.len(), 3);
        let other = place_replicas(&nodes, 1, 3);
        assert_ne!(ids, other);
    }

    #[test]
    fn prefers_distinct_racks() {
        let nodes = vec![
            node("a", Some("r1")),
            node("b", Some("r1")),
            node("c", Some("r2")),
            node("d", Some("r3")),
        ];
        let ids = place_replicas(&nodes, 0, 3);
        assert_eq!(ids.len(), 3);
        let racks: BTreeSet<_> = ids
            .iter()
            .filter_map(|id| {
                nodes
                    .iter()
                    .find(|n| n.id == *id)
                    .and_then(|n| n.rack.clone())
            })
            .collect();
        assert_eq!(racks.len(), 3);
    }

    #[test]
    fn controller_voter_count_stays_small() {
        let four: Vec<_> = (0..4).map(|i| node(&format!("n{i}"), None)).collect();
        assert_eq!(initial_controller_voters(&four).len(), 3);
        let nine: Vec<_> = (0..9).map(|i| node(&format!("n{i}"), None)).collect();
        assert_eq!(initial_controller_voters(&nine).len(), 5);
    }
}
