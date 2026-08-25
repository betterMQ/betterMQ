//! CLI argument parsing and subcommands.

use clap::{Parser, Subcommand, ValueEnum};
use std::net::SocketAddr;
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "bettermq", about = "BetterMQ message broker")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Run the HTTP broker (data plane).
    Serve(ServeArgs),
    /// Standalone management gateway + control panel (no broker storage).
    Panel(PanelArgs),
    /// Cluster management (legacy; prefer `bettermq.json` cluster section).
    Cluster {
        #[command(subcommand)]
        cmd: ClusterCommands,
    },
    /// Create or validate `bettermq.json`.
    Config {
        #[command(subcommand)]
        cmd: ConfigCommands,
    },
    /// Validate config, data directory, WAL frames, and archive manifests.
    Doctor(DoctorArgs),
    /// Export a redacted JSON diagnostic bundle for support.
    SupportBundle(SupportBundleArgs),
}

#[derive(Debug, Subcommand)]
pub enum ClusterCommands {
    /// Initialize a new cluster config in the data directory.
    Init(ClusterInitArgs),
    /// Join an existing cluster (writes local cluster-config.json).
    Join(ClusterJoinArgs),
}

#[derive(Debug, Subcommand)]
pub enum ConfigCommands {
    /// Write a starter bettermq.json to disk.
    Init(ConfigInitArgs),
    /// Validate a bettermq.json file.
    Validate(ConfigValidateArgs),
    /// Print JSON Schema for bettermq.json (editor autocomplete).
    Schema,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ConfigTemplate {
    /// Single node, local WAL + RocksDB.
    Local,
    /// Single node, SlateDB + S3 (MinIO/R2).
    Slate,
    /// Three-node HA cluster (local storage).
    Cluster,
    /// Cloud cell: Postgres + SlateDB (`bettermq` built with `--features cloud`).
    #[cfg(feature = "cloud")]
    Cloud,
}

#[derive(Debug, Parser)]
pub struct ConfigInitArgs {
    /// Output path (default: ./bettermq.json).
    #[arg(short, long, default_value = "bettermq.json")]
    pub output: PathBuf,
    #[arg(long, value_enum, default_value_t = ConfigTemplate::Local)]
    pub template: ConfigTemplate,
}

#[derive(Debug, Parser)]
pub struct ConfigValidateArgs {
    #[arg(short, long, default_value = "bettermq.json")]
    pub config: PathBuf,
}

#[derive(Debug, Parser)]
pub struct DoctorArgs {
    /// Persistent BetterMQ data directory to inspect.
    #[arg(long, env = "BETTERMQ_DATA_DIR", default_value = "./data")]
    pub data_dir: PathBuf,
    /// Optional explicit bettermq.json path.
    #[arg(short, long, env = "BETTERMQ_CONFIG")]
    pub config: Option<PathBuf>,
    /// Emit the full machine-readable report.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Parser)]
pub struct SupportBundleArgs {
    /// Persistent BetterMQ data directory to inspect.
    #[arg(long, env = "BETTERMQ_DATA_DIR", default_value = "./data")]
    pub data_dir: PathBuf,
    /// Optional explicit bettermq.json path.
    #[arg(short, long, env = "BETTERMQ_CONFIG")]
    pub config: Option<PathBuf>,
    /// Destination JSON file. Existing files are replaced.
    #[arg(short, long, default_value = "bettermq-support-bundle.json")]
    pub output: PathBuf,
}

#[derive(Debug, Parser)]
pub struct ClusterInitArgs {
    #[arg(long, env = "BETTERMQ_DATA_DIR", default_value = "./data")]
    pub data_dir: std::path::PathBuf,
    /// This node's public HTTP base URL (e.g. http://broker1:8080).
    #[arg(long)]
    pub addr: String,
    /// Comma-separated peer base URLs (including this node).
    #[arg(long, value_delimiter = ',')]
    pub peers: Vec<String>,
    /// Optional fixed node id (defaults to random v4).
    #[arg(long)]
    pub node_id: Option<Uuid>,
}

#[derive(Debug, Parser)]
pub struct ClusterJoinArgs {
    #[arg(long, env = "BETTERMQ_DATA_DIR", default_value = "./data")]
    pub data_dir: std::path::PathBuf,
    #[arg(long)]
    pub addr: String,
    /// Seed broker URL to fetch cluster membership from.
    #[arg(long)]
    pub seed: String,
    #[arg(long)]
    pub node_id: Option<Uuid>,
}

/// Shared `bettermq serve` flags (self-host build).
#[derive(Debug, Parser)]
#[cfg(not(feature = "cloud"))]
pub struct ServeArgs {
    /// Path to bettermq.json.
    #[arg(short, long, env = "BETTERMQ_CONFIG")]
    pub config: Option<PathBuf>,

    /// Listen address (host:port). Overrides `-p` and config file.
    #[arg(long, env = "BETTERMQ_LISTEN")]
    pub listen: Option<SocketAddr>,

    /// HTTP listen port (default 8080 when no config `node.listen`). Binds `0.0.0.0:{port}`.
    #[arg(short = 'p', long, env = "BETTERMQ_PORT")]
    pub port: Option<u16>,

    /// Persistent data directory (WAL, segments, RocksDB). Overrides config file.
    #[arg(long, env = "BETTERMQ_DATA_DIR")]
    pub data_dir: Option<std::path::PathBuf>,

    /// Enable cluster mode (overrides config file).
    #[arg(long, env = "BETTERMQ_CLUSTER")]
    pub cluster: Option<bool>,

    /// Broker-only: accept ingest + lease API; do not run local delivery workers.
    #[arg(long, env = "BETTERMQ_BROKER_ONLY", default_value_t = false)]
    pub broker_only: bool,

    /// Gateway-only: route ingest to brokers without opening any local data stores.
    #[arg(long, env = "BETTERMQ_GATEWAY_ONLY", default_value_t = false)]
    pub gateway_only: bool,

    /// Dispatch fleet: claim jobs from brokers via BETTERMQ_BROKER_URLS (no ingest).
    #[arg(long, env = "BETTERMQ_DISPATCH_FLEET", default_value_t = false)]
    pub dispatch_fleet: bool,

    /// Optional second listen address for the embedded panel only (e.g. 127.0.0.1:8090).
    #[arg(long, env = "BETTERMQ_PANEL_LISTEN")]
    pub panel_listen: Option<SocketAddr>,

    /// Named component profile: all, broker, dispatch, gateway, controller, panel.
    #[arg(long, env = "BETTERMQ_PROFILE")]
    pub profile: Option<String>,

    /// Extra components to enable (comma-separated).
    #[arg(long, env = "BETTERMQ_COMPONENTS", value_delimiter = ',')]
    pub components: Vec<String>,

    /// Disable the embedded panel.
    #[arg(long, env = "BETTERMQ_NO_PANEL", default_value_t = false)]
    pub no_panel: bool,

    /// Private admin listener (panel + /admin/v1).
    #[arg(long, env = "BETTERMQ_ADMIN_LISTEN")]
    pub admin_listen: Option<SocketAddr>,

    /// Internal cluster/replication listener.
    #[arg(long, env = "BETTERMQ_INTERNAL_LISTEN")]
    pub internal_listen: Option<SocketAddr>,
}

/// Shared `bettermq serve` flags (cloud build).
#[derive(Debug, Parser)]
#[cfg(feature = "cloud")]
pub struct ServeArgs {
    /// Path to bettermq.json.
    #[arg(short, long, env = "BETTERMQ_CONFIG")]
    pub config: Option<PathBuf>,

    /// Listen address (host:port). Overrides `-p` and config file.
    #[arg(long, env = "BETTERMQ_LISTEN")]
    pub listen: Option<SocketAddr>,

    /// HTTP listen port (default 8080 when no config `node.listen`). Binds `0.0.0.0:{port}`.
    #[arg(short = 'p', long, env = "BETTERMQ_PORT")]
    pub port: Option<u16>,

    /// Persistent data directory (WAL, segments, RocksDB). Overrides config file.
    #[arg(long, env = "BETTERMQ_DATA_DIR")]
    pub data_dir: Option<std::path::PathBuf>,

    /// Enable cluster mode (overrides config file).
    #[arg(long, env = "BETTERMQ_CLUSTER")]
    pub cluster: Option<bool>,

    /// Postgres URL for cloud control plane. Overrides config file.
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: Option<String>,

    /// Broker-only: accept ingest + lease API; do not run local delivery workers.
    #[arg(long, env = "BETTERMQ_BROKER_ONLY", default_value_t = false)]
    pub broker_only: bool,

    /// Gateway-only: route ingest to brokers without opening any local data stores.
    #[arg(long, env = "BETTERMQ_GATEWAY_ONLY", default_value_t = false)]
    pub gateway_only: bool,

    /// Dispatch fleet: claim jobs from brokers via BETTERMQ_BROKER_URLS (no ingest).
    #[arg(long, env = "BETTERMQ_DISPATCH_FLEET", default_value_t = false)]
    pub dispatch_fleet: bool,

    /// Optional second listen address for the embedded panel only.
    #[arg(long, env = "BETTERMQ_PANEL_LISTEN")]
    pub panel_listen: Option<SocketAddr>,

    /// Named component profile: all, broker, dispatch, gateway, controller, panel.
    #[arg(long, env = "BETTERMQ_PROFILE")]
    pub profile: Option<String>,

    /// Extra components to enable (comma-separated).
    #[arg(long, env = "BETTERMQ_COMPONENTS", value_delimiter = ',')]
    pub components: Vec<String>,

    /// Disable the embedded panel.
    #[arg(long, env = "BETTERMQ_NO_PANEL", default_value_t = false)]
    pub no_panel: bool,

    /// Private admin listener (panel + /admin/v1).
    #[arg(long, env = "BETTERMQ_ADMIN_LISTEN")]
    pub admin_listen: Option<SocketAddr>,

    /// Internal cluster/replication listener.
    #[arg(long, env = "BETTERMQ_INTERNAL_LISTEN")]
    pub internal_listen: Option<SocketAddr>,
}

#[derive(Debug, Parser)]
pub struct PanelArgs {
    /// Path to bettermq.json (optional cell registry / listener config).
    #[arg(short, long, env = "BETTERMQ_CONFIG")]
    pub config: Option<PathBuf>,

    /// Panel listen address.
    #[arg(long, env = "BETTERMQ_LISTEN", default_value = "127.0.0.1:8090")]
    pub listen: SocketAddr,

    /// Regional cell controller or broker admin base URL.
    #[arg(long, env = "BETTERMQ_CONTROLLER")]
    pub controller: Option<String>,

    /// Optional cell-registry directory. Must not be used as a broker WAL root.
    #[arg(long, env = "BETTERMQ_PANEL_DATA_DIR")]
    pub data_dir: Option<PathBuf>,
}

impl Cli {
    pub fn parse_args() -> Self {
        Self::parse()
    }
}

pub fn is_sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase().replace('-', "_");
    [
        "secret",
        "token",
        "password",
        "api_key",
        "apikey",
        "access_key",
        "accesskey",
        "private_key",
        "signature",
        "authorization",
        "credential",
        "database_url",
    ]
    .iter()
    .any(|needle| key.contains(needle))
}

/// Recursively redact secrets while retaining diagnostic structure.
pub fn redact_json(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, value) in map {
                if is_sensitive_key(key) {
                    *value = serde_json::Value::String("[REDACTED]".into());
                } else {
                    redact_json(value);
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                redact_json(value);
            }
        }
        serde_json::Value::String(text) => {
            if let Some(redacted) = redact_text(text) {
                *text = redacted;
            }
        }
        _ => {}
    }
}

fn redact_text(text: &str) -> Option<String> {
    if let Some((base, query)) = text.split_once('?') {
        let has_secret_query = query.split('&').any(|pair| {
            pair.split_once('=')
                .map(|(key, _)| is_sensitive_key(key))
                .unwrap_or(false)
        });
        if has_secret_query {
            return Some(format!("{base}?[REDACTED]"));
        }
    }
    let (scheme, rest) = text.split_once("://")?;
    let (userinfo, suffix) = rest.split_once('@')?;
    userinfo
        .contains(':')
        .then(|| format!("{scheme}://[REDACTED]@{suffix}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn support_redaction_is_recursive() {
        let mut value = serde_json::json!({
            "auth": {"apiKey": "keep? no", "password": "no"},
            "cluster-secret": "no",
            "listen": "127.0.0.1:8080",
            "peer": "https://user:password@example.com/api",
            "callback": "https://example.com/hook?token=secret&mode=test",
            "nested": [{"database_url": "postgres://user:pass@host/db"}]
        });
        redact_json(&mut value);
        assert_eq!(value["auth"]["apiKey"], "[REDACTED]");
        assert_eq!(value["auth"]["password"], "[REDACTED]");
        assert_eq!(value["cluster-secret"], "[REDACTED]");
        assert_eq!(value["nested"][0]["database_url"], "[REDACTED]");
        assert_eq!(value["peer"], "https://[REDACTED]@example.com/api");
        assert_eq!(value["callback"], "https://example.com/hook?[REDACTED]");
        assert_eq!(value["listen"], "127.0.0.1:8080");
    }
}
