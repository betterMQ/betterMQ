//! Peer health probes, dispatch backfill on shard leadership failover, and catalog sync (CP7b).

use broker_api::{
    catalog_peer_targets, push_catalog_to_recovered_peer, sync_catalog_from_peers, AppState,
    Cluster, ClusterGossipRequest,
};
use broker_dispatch::DispatchEngine;
use broker_partition::DEFAULT_PARTITIONS;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use uuid::Uuid;

pub fn spawn_cluster_health_monitor(
    cluster: Cluster,
    dispatch: DispatchEngine,
    state: Arc<AppState>,
) {
    let runtime = cluster.runtime.clone();
    let node_id = runtime.config().node_id;

    tokio::spawn(async move {
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                warn!(error = %e, "cluster health monitor: failed to build HTTP client");
                return;
            }
        };

        let mut prev_leaders: HashMap<u32, Uuid> = HashMap::new();
        let mut prev_peer_alive: HashMap<Uuid, bool> = HashMap::new();
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        loop {
            interval.tick().await;
            let now = Utc::now().timestamp_millis();
            runtime.record_self_alive(now);

            for peer in runtime.config().nodes.iter() {
                if peer.id == node_id {
                    continue;
                }
                let url = format!("{}/healthz", peer.addr.trim_end_matches('/'));
                match client.get(&url).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        runtime.record_peer_alive(peer.id, now);
                    }
                    Ok(resp) => {
                        warn!(peer = %peer.addr, status = %resp.status(), "peer health check failed");
                    }
                    Err(e) => {
                        warn!(peer = %peer.addr, error = %e, "peer health check unreachable");
                    }
                }
            }

            for (peer_id, peer_url) in catalog_peer_targets(&state) {
                let alive = runtime.is_peer_alive(peer_id, now);
                if alive && prev_peer_alive.get(&peer_id) == Some(&false) {
                    if push_catalog_to_recovered_peer(&peer_url, &state).await {
                        info!(peer = %peer_url, "pushed catalog to recovered peer");
                    } else {
                        warn!(peer = %peer_url, "failed to push catalog to recovered peer");
                    }
                }
                prev_peer_alive.insert(peer_id, alive);
            }

            let gossip = ClusterGossipRequest {
                observer: node_id,
                seen: runtime.peer_health_snapshot(),
            };
            for peer in runtime.config().nodes.iter() {
                if peer.id == node_id {
                    continue;
                }
                let url = format!(
                    "{}/internal/v1/cluster/gossip",
                    peer.addr.trim_end_matches('/')
                );
                let req =
                    broker_api::cluster_auth::apply_cluster_secret(client.post(&url)).json(&gossip);
                if let Err(e) = req.send().await {
                    warn!(peer = %peer.addr, error = %e, "cluster gossip failed");
                }
            }

            let mut gained_shards = Vec::new();
            let mut lost_shards = Vec::new();
            for shard in 0..DEFAULT_PARTITIONS {
                let leader = match runtime.elect_leader_for_shard(shard) {
                    Some(id) => id,
                    None => continue,
                };
                let was = prev_leaders.insert(shard, leader);
                if leader == node_id && was != Some(node_id) {
                    gained_shards.push(shard);
                    runtime.bump_shard_generation(shard);
                }
                if was == Some(node_id) && leader != node_id {
                    lost_shards.push(shard);
                }
            }

            if !lost_shards.is_empty() {
                info!(shards = ?lost_shards, "failover: evicting slate handles for lost shards");
                state.broker.evict_slate_partitions(&lost_shards);
            }

            if !gained_shards.is_empty() {
                info!(
                    shards = ?gained_shards,
                    "failover: this node gained shard leadership; backfilling dispatch"
                );
                dispatch.backfill_pending();
                sync_catalog_from_peers(&state).await;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_raft_meta::{ClusterConfig, ClusterRuntime, NodeConfig};

    #[test]
    fn leadership_gained_when_preferred_peer_stale() {
        let ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
        let nodes: Vec<NodeConfig> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| NodeConfig {
                id: *id,
                addr: format!("http://b{i}:8080"),
            })
            .collect();
        let cfg = ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: nodes.clone(),
            node_id: ids[2],
            generation: 1,
        };
        let rt = ClusterRuntime::from_config_only(cfg);
        let now = Utc::now().timestamp_millis();
        rt.record_self_alive(now);
        rt.record_peer_alive(ids[0], now);
        // ids[1] stale → shard 1 fails over from preferred ids[1] to ids[2]
        assert!(rt.is_leader_for_shard(1));
    }
}
