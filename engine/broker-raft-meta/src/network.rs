//! Persistent HTTP transport for OpenRaft protocol RPCs.

use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::error::Error;
use std::io;
use std::time::Duration;

use crate::controller::{NodeId, TypeConfig};

#[derive(Clone)]
pub struct HttpNetworkFactory {
    client: reqwest::Client,
}

impl HttpNetworkFactory {
    pub fn new() -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: reqwest::Client::builder()
                .pool_idle_timeout(Duration::from_secs(90))
                .timeout(Duration::from_secs(5))
                .build()?,
        })
    }
}

pub struct HttpNetworkConnection {
    target: NodeId,
    addr: String,
    client: reqwest::Client,
}

impl RaftNetworkFactory<TypeConfig> for HttpNetworkFactory {
    type Network = HttpNetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        HttpNetworkConnection {
            target,
            addr: node.addr.trim_end_matches('/').to_string(),
            client: self.client.clone(),
        }
    }
}

impl HttpNetworkConnection {
    fn authenticate(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match std::env::var("BETTERMQ_CLUSTER_SECRET") {
            Ok(secret) if !secret.trim().is_empty() => {
                request.header("x-bettermq-cluster-secret", secret)
            }
            _ => request,
        }
    }

    async fn post<T, R, E>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<R, RPCError<NodeId, BasicNode, RaftError<NodeId, E>>>
    where
        T: Serialize + ?Sized,
        R: DeserializeOwned,
        E: Error,
    {
        let url = format!("{}{}", self.addr, path);
        let response = self
            .authenticate(self.client.post(&url))
            .json(body)
            .send()
            .await
            .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
        if !response.status().is_success() {
            let status = response.status();
            let detail = response.text().await.unwrap_or_default();
            let error = io::Error::other(format!(
                "controller peer {} returned {status}: {detail}",
                self.target
            ));
            return Err(RPCError::Network(NetworkError::new(&error)));
        }
        response
            .json()
            .await
            .map_err(|error| RPCError::Network(NetworkError::new(&error)))
    }
}

impl RaftNetwork<TypeConfig> for HttpNetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.post("/internal/v1/controller/raft/append", &rpc).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.post("/internal/v1/controller/raft/snapshot", &rpc)
            .await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.post("/internal/v1/controller/raft/vote", &rpc).await
    }
}
