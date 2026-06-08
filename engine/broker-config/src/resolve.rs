use crate::types::{
    parse_listen, AuthConfig, BetterMqConfig, ConfigError, S3Config, StorageConfig,
};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

/// CLI / env overrides applied on top of `bettermq.json`.
#[derive(Debug, Clone, Default)]
pub struct ServeOverrides {
    pub config_path: Option<PathBuf>,
    pub listen: Option<SocketAddr>,
    /// Shorthand for `0.0.0.0:{port}` when `--listen` is not set.
    pub port: Option<u16>,
    pub data_dir: Option<PathBuf>,
    pub cluster: Option<bool>,
    pub database_url: Option<String>,
    pub dispatch_fleet: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct ResolvedServeSettings {
    pub listen: SocketAddr,
    pub data_dir: PathBuf,
    pub storage: StorageMode,
    pub s3: Option<S3Config>,
    pub auth: ResolvedAuth,
    pub cluster_enabled: bool,
    pub cluster_shared_meta_dir: Option<PathBuf>,
    pub dispatch_http_timeout_secs: u64,
    pub dispatch_long_http_timeout_secs: u64,
    pub dispatch_retry: broker_proto::RetryDefaults,
    pub dispatch_fleet: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageMode {
    Local,
    Slate,
}

#[derive(Debug, Clone)]
pub enum ResolvedAuth {
    Local { auth_file: Option<PathBuf> },
    Cloud { database_url: String },
}

impl ResolvedServeSettings {
    /// Set process env vars consumed by existing broker crates (backward compatible).
    pub fn apply_env(&self) {
        let storage = match self.storage {
            StorageMode::Local => "local",
            StorageMode::Slate => "slate",
        };
        std::env::set_var("BETTERMQ_STORAGE", storage);

        if let Some(s3) = &self.s3 {
            std::env::set_var("S3_ENDPOINT", &s3.endpoint);
            std::env::set_var("S3_BUCKET", &s3.bucket);
            std::env::set_var("S3_ACCESS_KEY", &s3.access_key);
            std::env::set_var("S3_SECRET_KEY", &s3.secret_key);
            std::env::set_var("S3_REGION", &s3.region);
            if let Some(pb) = &s3.payload_bucket {
                std::env::set_var("S3_PAYLOAD_BUCKET", pb);
            }
        }

        std::env::set_var(
            "BETTERMQ_HTTP_TIMEOUT_SECS",
            self.dispatch_http_timeout_secs.to_string(),
        );
        std::env::set_var(
            "BETTERMQ_LONG_HTTP_TIMEOUT_SECS",
            self.dispatch_long_http_timeout_secs.to_string(),
        );

        if let Some(dir) = &self.cluster_shared_meta_dir {
            std::env::set_var("BETTERMQ_SHARED_META_DIR", dir);
        }

        if let ResolvedAuth::Local {
            auth_file: Some(path),
        } = &self.auth
        {
            std::env::set_var("BETTERMQ_LOCAL_AUTH_FILE", path);
        }
    }
}

pub fn resolve_serve(
    file_cfg: Option<&BetterMqConfig>,
    overrides: &ServeOverrides,
) -> Result<ResolvedServeSettings, ConfigError> {
    let base = file_cfg
        .cloned()
        .unwrap_or_else(BetterMqConfig::template_single_local);

    let listen = overrides
        .listen
        .or_else(|| {
            overrides
                .port
                .map(|port| SocketAddr::from(([0, 0, 0, 0], port)))
        })
        .or_else(|| parse_listen(&base.node.listen).ok())
        .unwrap_or_else(|| "0.0.0.0:8080".parse().expect("default listen"));

    let data_dir = overrides
        .data_dir
        .clone()
        .unwrap_or_else(|| base.data_dir.clone());

    let cluster_enabled = overrides.cluster.unwrap_or_else(|| base.cluster_enabled());

    let (storage, s3) = match &base.storage {
        StorageConfig::Local => (StorageMode::Local, None),
        StorageConfig::Slate { s3 } => (StorageMode::Slate, Some(s3.clone())),
    };

    let auth = match (&base.auth, &overrides.database_url) {
        (_, Some(url)) if !url.is_empty() => ResolvedAuth::Cloud {
            database_url: url.clone(),
        },
        (AuthConfig::Cloud { database_url }, _) => ResolvedAuth::Cloud {
            database_url: database_url.clone(),
        },
        (AuthConfig::Local { auth_file }, _) => ResolvedAuth::Local {
            auth_file: auth_file.clone(),
        },
    };

    let cluster_shared_meta_dir = base
        .cluster
        .as_ref()
        .and_then(|c| c.shared_meta_dir.clone());

    Ok(ResolvedServeSettings {
        listen,
        data_dir,
        storage,
        s3,
        auth,
        cluster_enabled,
        cluster_shared_meta_dir,
        dispatch_http_timeout_secs: base.dispatch.http_timeout_secs,
        dispatch_long_http_timeout_secs: base.dispatch.long_http_timeout_secs,
        dispatch_retry: base.dispatch.retry.clone(),
        dispatch_fleet: overrides.dispatch_fleet.unwrap_or(false),
    })
}

pub fn resolve_from_path(
    path: &Path,
    overrides: &ServeOverrides,
) -> Result<ResolvedServeSettings, ConfigError> {
    let cfg = crate::file::load_config(path)?;
    resolve_serve(Some(&cfg), overrides)
}
