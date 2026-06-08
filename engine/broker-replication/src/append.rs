//! HTTP replication of append-only log frames between brokers.

use base64::{engine::general_purpose::STANDARD as B64, Engine};
use broker_raft_meta::ClusterConfig;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, warn};

async fn join_all<I>(futs: Vec<I>) -> Vec<bool>
where
    I: std::future::Future<Output = bool>,
{
    let mut out = Vec::with_capacity(futs.len());
    for f in futs {
        out.push(f.await);
    }
    out
}

#[derive(Debug, Error)]
pub enum ReplicateError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("quorum not reached: {acked}/{quorum}")]
    QuorumNotReached { acked: usize, quorum: usize },
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateAppendRequest {
    pub tenant_id: String,
    pub topic: String,
    pub partition: u32,
    /// Base64-encoded partition log frame (magic + header + payload + crc).
    pub frame_b64: String,
    pub leader_generation: u64,
}

#[derive(Clone)]
pub struct ReplicationClient {
    http: reqwest::Client,
    cluster: ClusterConfig,
}

impl ReplicationClient {
    pub fn new(cluster: ClusterConfig) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .expect("reqwest client"),
            cluster,
        }
    }

    pub fn with_config(cluster: ClusterConfig) -> Self {
        Self::new(cluster)
    }

    /// Leader: replicate frame to all peers; require quorum acks (including self).
    pub async fn replicate_append(
        &self,
        tenant_id: &str,
        topic: &str,
        partition: u32,
        frame: &[u8],
        leader_generation: u64,
    ) -> Result<(), ReplicateError> {
        if self.cluster.node_count() <= 1 {
            return Ok(());
        }

        let req = ReplicateAppendRequest {
            tenant_id: tenant_id.to_string(),
            topic: topic.to_string(),
            partition,
            frame_b64: B64.encode(frame),
            leader_generation,
        };

        let quorum = self.cluster.quorum_size();
        let mut acked = 1usize; // leader local write assumed done by caller

        let peers = self.cluster.peer_addrs();
        let mut futs = Vec::new();
        for peer in peers {
            let url = format!("{peer}/internal/v1/replicate");
            let http = self.http.clone();
            let body = req.clone();
            futs.push(async move {
                let req_builder = http.post(&url).json(&body);
                let req_builder = if let Ok(secret) = std::env::var("BETTERMQ_CLUSTER_SECRET") {
                    if secret.trim().is_empty() {
                        req_builder
                    } else {
                        req_builder.header("x-bettermq-cluster-secret", secret)
                    }
                } else {
                    req_builder
                };
                match req_builder.send().await {
                    Ok(resp) if resp.status().is_success() => true,
                    Ok(resp) => {
                        warn!(status = %resp.status(), url = %url, "replicate peer rejected");
                        false
                    }
                    Err(e) => {
                        warn!(error = %e, url = %url, "replicate peer failed");
                        false
                    }
                }
            });
        }

        for ok in join_all(futs).await {
            if ok {
                acked += 1;
            }
        }

        debug!(acked, quorum, topic, partition, "replicate quorum");
        if acked >= quorum {
            Ok(())
        } else {
            Err(ReplicateError::QuorumNotReached { acked, quorum })
        }
    }
}
