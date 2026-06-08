//! Typed `bettermq.json` configuration for self-hosted deployments.

mod cluster;
mod file;
mod managed;
mod resolve;
mod types;

pub use cluster::{ensure_cluster_config, stable_node_id};
pub use file::{apply_node_env_overrides, load_config, write_config};
pub use managed::{
    config_to_view, load_managed_config, managed_config_path, merge_s3_update, save_managed_config,
    BetterMqConfigView, REDACTED_SECRET,
};
pub use resolve::{
    resolve_from_path, resolve_serve, ResolvedAuth, ResolvedServeSettings, ServeOverrides,
    StorageMode,
};
pub use types::{
    AuthConfig, BetterMqConfig, ClusterConfigSection, ClusterNode, ConfigError,
    DispatchConfigSection, NodeSection, S3Config, StorageConfig, CONFIG_VERSION,
};
