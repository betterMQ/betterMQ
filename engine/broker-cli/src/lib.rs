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

    /// CP10: run as stateless dispatch-only fleet (no enqueue routes).
    #[arg(long, env = "BETTERMQ_DISPATCH_FLEET", default_value_t = false)]
    pub dispatch_fleet: bool,
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

    /// CP10: run as stateless dispatch-only fleet (no enqueue routes).
    #[arg(long, env = "BETTERMQ_DISPATCH_FLEET", default_value_t = false)]
    pub dispatch_fleet: bool,
}

impl Cli {
    pub fn parse_args() -> Self {
        Self::parse()
    }
}
