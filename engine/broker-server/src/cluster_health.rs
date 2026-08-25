//! Peer health probes, dispatch backfill on shard leadership failover, and catalog sync (CP7b).

use broker_api::{
    catalog_peer_targets, push_catalog_to_recovered_peer, sync_catalog_from_peers, AppState,
    Cluster, ClusterGossipRequest,
};
use broker_dispatch::DispatchEngine;
use chrono::Utc;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use uuid::Uuid;

fn discover_topics(state: &AppState) -> HashSet<String> {
    let mut topics = HashSet::from([broker_partition::DIRECT_TOPIC.to_string()]);
    if let Ok(queues) = state.broker.list_queues() {
        topics.extend(queues.into_iter().map(|queue| queue.topic));
    }
    if let Ok(groups) = state.broker.list_groups() {
        topics.extend(
            groups
                .into_iter()
                .map(|group| broker_partition::group_topic(group.id)),
        );
    }
    let root = state
        .broker
        .config()
        .data_dir
        .join("partitions")
        .join(state.broker.tenant());
    if let Ok(entries) = std::fs::read_dir(root) {
        for entry in entries.flatten() {
            if entry.file_type().map(|kind| kind.is_dir()).unwrap_or(false) {
                topics.insert(entry.file_name().to_string_lossy().into_owned());
            }
        }
    }
    topics
}

async fn catch_up_shard(cluster: &Cluster, state: &AppState, shard: u32) -> Result<(), String> {
    let now = Utc::now().timestamp_millis();
    let peers: Vec<_> = cluster
        .runtime
        .config()
        .peer_nodes()
        .into_iter()
        .filter(|peer| cluster.runtime.is_peer_alive(peer.id, now))
        .collect();
    if peers.is_empty() {
        return Ok(());
    }
    for topic in discover_topics(state) {
        let mut local_leo = state
            .broker
            .partition_high_watermark(&topic, shard)
            .map_err(|e| format!("{topic}/{shard}: read local LEO: {e}"))?;
        let local_committed = state
            .broker
            .committed_hwm(&topic, shard)
            .map_err(|e| format!("{topic}/{shard}: read local durable HWM: {e}"))?;
        let probe_from = local_committed.saturating_sub(1);

        let mut best: Option<(String, broker_replication::ReplicateCatchUpRange)> = None;
        for peer in &peers {
            let request = broker_replication::ReplicateCatchUpRequest {
                tenant_id: state.broker.tenant(),
                topic: topic.clone(),
                partition: shard,
                from_offset: probe_from,
                max_records: 4096,
            };
            match cluster
                .replication
                .catch_up_from(&peer.addr, &request)
                .await
            {
                Ok(response)
                    if best.as_ref().is_none_or(|(_, current)| {
                        response.committed_hwm > current.committed_hwm
                    }) =>
                {
                    best = Some((peer.addr.clone(), response));
                }
                Ok(_) => {}
                Err(error) => {
                    warn!(
                        peer = %peer.addr,
                        %topic,
                        shard,
                        error = %error,
                        "leadership catch-up probe failed"
                    );
                }
            }
        }
        let Some((source, mut page)) = best else {
            return Err(format!(
                "{topic}/{shard}: no replica answered catch-up probe"
            ));
        };
        if local_leo > page.committed_hwm {
            return Err(format!(
                "{topic}/{shard}: local LEO {local_leo} exceeds replica committed HWM {}; refusing to overwrite a divergent/uncommitted tail",
                page.committed_hwm
            ));
        }

        loop {
            let mut last_appended = None;
            for frame in &page.frames {
                state
                    .broker
                    .append_replicated_frame(&topic, shard, &frame.bytes, Some(frame.offset))
                    .map_err(|e| {
                        format!(
                            "{topic}/{shard}: divergent frame at offset {}: {e}",
                            frame.offset
                        )
                    })?;
                last_appended = Some(frame.offset);
            }
            if let Some(offset) = last_appended {
                state
                    .broker
                    .wait_committed(&topic, shard, offset)
                    .await
                    .map_err(|e| format!("{topic}/{shard}: fsync catch-up page: {e}"))?;
            }
            local_leo = state
                .broker
                .partition_high_watermark(&topic, shard)
                .map_err(|e| format!("{topic}/{shard}: refresh local LEO: {e}"))?;
            if local_leo >= page.committed_hwm {
                break;
            }
            let request = broker_replication::ReplicateCatchUpRequest {
                tenant_id: state.broker.tenant(),
                topic: topic.clone(),
                partition: shard,
                from_offset: local_leo,
                max_records: 4096,
            };
            page = cluster
                .replication
                .catch_up_from(&source, &request)
                .await
                .map_err(|e| format!("{topic}/{shard}: continue catch-up: {e}"))?;
            if page.frames.is_empty() && local_leo < page.committed_hwm {
                return Err(format!(
                    "{topic}/{shard}: source reported HWM {} but returned no frame at {local_leo}",
                    page.committed_hwm
                ));
            }
        }
    }
    Ok(())
}

pub fn spawn_cluster_health_monitor(
    cluster: Cluster,
    dispatch: DispatchEngine,
    state: Arc<AppState>,
) -> tokio::task::JoinHandle<()> {
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
                controller_vote: None,
                controller_leader: None,
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
                match req.send().await {
                    Ok(response) if response.status().is_success() => {}
                    Ok(response) => {
                        warn!(peer = %peer.addr, status = %response.status(), "cluster gossip rejected");
                    }
                    Err(error) => {
                        warn!(peer = %peer.addr, %error, "cluster gossip failed");
                    }
                }
            }
            if !runtime.has_controller_lease() {
                continue;
            }

            let mut gained_shards = Vec::new();
            let mut lost_shards = Vec::new();
            for shard in 0..state.broker.layout().shard_count {
                if runtime.is_controller_leader() {
                    let current = runtime.elect_leader_for_shard(shard);
                    let desired = runtime.desired_leader_for_shard(shard);
                    if let Some(desired) = desired.filter(|desired| Some(*desired) != current) {
                        if let Err(error) = runtime.assign_shard_leadership(shard, desired).await {
                            warn!(shard, %error, "failed to commit OpenRaft shard placement");
                        }
                    }
                }
                let leader = match runtime.elect_leader_for_shard(shard) {
                    Some(id) => id,
                    None => continue,
                };
                let was = prev_leaders.insert(shard, leader);
                if leader == node_id && was != Some(node_id) {
                    runtime.set_shard_ready(shard, false);
                    gained_shards.push(shard);
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
                    "failover: this node gained shard leadership; catching up durable prefixes"
                );
                sync_catalog_from_peers(&state).await;
                let mut caught_up = Vec::new();
                for shard in gained_shards {
                    match catch_up_shard(&cluster, &state, shard).await {
                        Ok(()) => {
                            runtime.set_shard_ready(shard, true);
                            caught_up.push(shard);
                        }
                        Err(error) => {
                            runtime.set_shard_ready(shard, false);
                            prev_leaders.remove(&shard);
                            warn!(shard, %error, "failover catch-up failed; shard remains fenced");
                        }
                    }
                }
                if !caught_up.is_empty() {
                    info!(shards = ?caught_up, "failover catch-up complete; shards unfenced");
                    dispatch.backfill_pending();
                }
            }
        }
    })
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
            hash_version: 1,
        };
        let rt = ClusterRuntime::from_config_only(cfg);
        let now = Utc::now().timestamp_millis();
        rt.record_self_alive(now);
        rt.record_peer_alive(ids[0], now);
        // ids[1] stale → shard 1 fails over from preferred ids[1] to ids[2]
        assert!(rt.is_leader_for_shard(1));
    }
}
