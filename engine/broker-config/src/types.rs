use broker_proto::RetryDefaults;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use thiserror::Error;

pub const CONFIG_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("config: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BetterMqConfig {
    #[serde(default = "default_version")]
    pub version: u32,
    pub node: NodeSection,
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub dispatch: DispatchConfigSection,
    #[serde(default)]
    pub cluster: Option<ClusterConfigSection>,
}

fn default_version() -> u32 {
    CONFIG_VERSION
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("./data")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeSection {
    pub name: String,
    #[serde(default = "default_listen")]
    pub listen: String,
    pub public_url: String,
}

fn default_listen() -> String {
    "0.0.0.0:8080".into()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "lowercase")]
pub enum StorageConfig {
    #[serde(alias = "rocksdb", alias = "wal")]
    #[default]
    Local,
    Slate {
        s3: S3Config,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    #[serde(default)]
    pub payload_bucket: Option<String>,
    pub access_key: String,
    pub secret_key: String,
    #[serde(default = "default_region")]
    pub region: String,
}

fn default_region() -> String {
    "auto".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "lowercase")]
pub enum AuthConfig {
    #[serde(alias = "panel")]
    Local {
        #[serde(default, rename = "authFile")]
        auth_file: Option<PathBuf>,
    },
    Cloud {
        #[serde(rename = "databaseUrl")]
        database_url: String,
    },
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self::Local { auth_file: None }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DispatchConfigSection {
    #[serde(default = "default_http_timeout")]
    pub http_timeout_secs: u64,
    #[serde(default = "default_long_http_timeout")]
    pub long_http_timeout_secs: u64,
    #[serde(default)]
    pub retry: RetryDefaults,
}

impl Default for DispatchConfigSection {
    fn default() -> Self {
        Self {
            http_timeout_secs: default_http_timeout(),
            long_http_timeout_secs: default_long_http_timeout(),
            retry: RetryDefaults::default(),
        }
    }
}

fn default_http_timeout() -> u64 {
    300
}

fn default_long_http_timeout() -> u64 {
    7200
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterConfigSection {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub id: Option<String>,
    pub nodes: Vec<ClusterNode>,
    #[serde(default)]
    pub shared_meta_dir: Option<PathBuf>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterNode {
    pub name: String,
    pub public_url: String,
}

impl BetterMqConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.version != CONFIG_VERSION {
            return Err(ConfigError::Invalid(format!(
                "unsupported config version {} (expected {CONFIG_VERSION})",
                self.version
            )));
        }
        if self.node.name.trim().is_empty() {
            return Err(ConfigError::Invalid("node.name is required".into()));
        }
        if self.node.public_url.trim().is_empty() {
            return Err(ConfigError::Invalid("node.public_url is required".into()));
        }
        parse_listen(&self.node.listen)?;

        match &self.storage {
            StorageConfig::Local => {}
            StorageConfig::Slate { s3 } => validate_s3(s3)?,
        }

        match &self.auth {
            AuthConfig::Local { .. } => {}
            AuthConfig::Cloud { database_url } => {
                if database_url.trim().is_empty() {
                    return Err(ConfigError::Invalid(
                        "auth.database_url is required for cloud mode".into(),
                    ));
                }
            }
        }

        if let Some(cluster) = &self.cluster {
            if cluster.enabled {
                if cluster.nodes.is_empty() {
                    return Err(ConfigError::Invalid(
                        "cluster.nodes must list at least one broker when cluster.enabled is true"
                            .into(),
                    ));
                }
                let mut names = std::collections::HashSet::new();
                for n in &cluster.nodes {
                    if n.name.trim().is_empty() {
                        return Err(ConfigError::Invalid(
                            "cluster node name must not be empty".into(),
                        ));
                    }
                    if n.public_url.trim().is_empty() {
                        return Err(ConfigError::Invalid(format!(
                            "cluster node {} needs public_url",
                            n.name
                        )));
                    }
                    if !names.insert(n.name.clone()) {
                        return Err(ConfigError::Invalid(format!(
                            "duplicate cluster node name: {}",
                            n.name
                        )));
                    }
                }
                if !cluster.nodes.iter().any(|n| n.name == self.node.name) {
                    return Err(ConfigError::Invalid(format!(
                        "node.name '{}' must appear in cluster.nodes",
                        self.node.name
                    )));
                }
            }
        }

        Ok(())
    }

    pub fn cluster_enabled(&self) -> bool {
        self.cluster
            .as_ref()
            .map(|c| c.enabled && c.nodes.len() >= 2)
            .unwrap_or(false)
    }
}

fn validate_s3(s3: &S3Config) -> Result<(), ConfigError> {
    if s3.endpoint.trim().is_empty() {
        return Err(ConfigError::Invalid(
            "storage.s3.endpoint is required".into(),
        ));
    }
    if s3.bucket.trim().is_empty() {
        return Err(ConfigError::Invalid("storage.s3.bucket is required".into()));
    }
    if s3.access_key.trim().is_empty() || s3.secret_key.trim().is_empty() {
        return Err(ConfigError::Invalid(
            "storage.s3.access_key and secret_key are required".into(),
        ));
    }
    Ok(())
}

pub fn parse_listen(listen: &str) -> Result<std::net::SocketAddr, ConfigError> {
    listen
        .parse()
        .map_err(|e| ConfigError::Invalid(format!("invalid node.listen '{listen}': {e}")))
}

impl BetterMqConfig {
    /// Single-node self-host with local RocksDB/WAL storage.
    pub fn template_single_local() -> Self {
        Self {
            version: CONFIG_VERSION,
            node: NodeSection {
                name: "default".into(),
                listen: default_listen(),
                public_url: "http://localhost:8080".into(),
            },
            data_dir: default_data_dir(),
            storage: StorageConfig::Local,
            auth: AuthConfig::Local { auth_file: None },
            dispatch: DispatchConfigSection::default(),
            cluster: None,
        }
    }

    /// Single-node self-host with SlateDB + MinIO (edit endpoint for R2).
    pub fn template_single_slate() -> Self {
        Self {
            version: CONFIG_VERSION,
            node: NodeSection {
                name: "default".into(),
                listen: default_listen(),
                public_url: "http://localhost:8080".into(),
            },
            data_dir: default_data_dir(),
            storage: StorageConfig::Slate {
                s3: S3Config {
                    endpoint: "http://minio:9000".into(),
                    bucket: "bettermq".into(),
                    payload_bucket: Some("bettermq-payloads".into()),
                    access_key: "minio".into(),
                    secret_key: "minio12345".into(),
                    region: default_region(),
                },
            },
            auth: AuthConfig::Local { auth_file: None },
            dispatch: DispatchConfigSection::default(),
            cluster: None,
        }
    }

    /// Three-node cluster (local storage). Set `node.name` per host.
    pub fn template_cluster_local() -> Self {
        Self {
            version: CONFIG_VERSION,
            node: NodeSection {
                name: "broker1".into(),
                listen: default_listen(),
                public_url: "http://broker1:8080".into(),
            },
            data_dir: PathBuf::from("/data"),
            storage: StorageConfig::Local,
            auth: AuthConfig::Local {
                auth_file: Some(PathBuf::from("/cluster-shared/local-auth.json")),
            },
            dispatch: DispatchConfigSection::default(),
            cluster: Some(ClusterConfigSection {
                enabled: true,
                id: Some("selfhost-cluster".into()),
                nodes: vec![
                    ClusterNode {
                        name: "broker1".into(),
                        public_url: "http://broker1:8080".into(),
                    },
                    ClusterNode {
                        name: "broker2".into(),
                        public_url: "http://broker2:8080".into(),
                    },
                    ClusterNode {
                        name: "broker3".into(),
                        public_url: "http://broker3:8080".into(),
                    },
                ],
                shared_meta_dir: Some(PathBuf::from("/cluster-shared/meta")),
            }),
        }
    }

    /// BetterMQ Cloud cell (Postgres + Slate on R2/MinIO). Cloud build only.
    #[cfg(feature = "cloud")]
    pub fn template_cloud() -> Self {
        Self {
            version: CONFIG_VERSION,
            node: NodeSection {
                name: "broker1".into(),
                listen: default_listen(),
                public_url: "http://broker:8080".into(),
            },
            data_dir: PathBuf::from("/data"),
            storage: StorageConfig::Slate {
                s3: S3Config {
                    endpoint: "http://minio:9000".into(),
                    bucket: "bettermq".into(),
                    payload_bucket: Some("bettermq-payloads".into()),
                    access_key: "minio".into(),
                    secret_key: "minio12345".into(),
                    region: default_region(),
                },
            },
            auth: AuthConfig::Cloud {
                database_url: "postgres://bettermq:bettermq@postgres:5432/bettermq".into(),
            },
            dispatch: DispatchConfigSection::default(),
            cluster: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_single_local() {
        BetterMqConfig::template_single_local()
            .validate()
            .expect("valid");
    }

    #[test]
    fn rejects_cluster_without_self_in_nodes() {
        let mut cfg = BetterMqConfig::template_cluster_local();
        cfg.node.name = "broker9".into();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validates_cluster_seed_with_one_node() {
        let mut cfg = BetterMqConfig::template_single_local();
        cfg.cluster = Some(ClusterConfigSection {
            enabled: true,
            id: Some("seed".into()),
            nodes: vec![ClusterNode {
                name: cfg.node.name.clone(),
                public_url: cfg.node.public_url.clone(),
            }],
            shared_meta_dir: None,
        });
        cfg.validate().expect("seed with one node is valid");
        assert!(!cfg.cluster_enabled());
    }

    #[test]
    fn bundled_config_templates_validate() {
        for cfg in [
            BetterMqConfig::template_single_local(),
            BetterMqConfig::template_single_slate(),
            BetterMqConfig::template_cluster_local(),
        ] {
            cfg.validate().expect("template config valid");
        }
    }
}
