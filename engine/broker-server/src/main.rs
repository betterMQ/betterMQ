use anyhow::Context;
use axum::{response::Redirect, routing::get};
use broker_api::{
    admin_router, begin_shutdown, enqueue_dispatch_after_publish, gateway_only_router,
    internal_router, open_setup_window, public_router, publish_with_cluster, router,
    spawn_cluster_catalog_sync, AdminState, AppState, CatalogTombstones, Cluster, GatewayOnlyState,
};
use broker_cli::{
    Cli, ClusterCommands, ClusterInitArgs, ClusterJoinArgs, Commands, ConfigCommands,
    ConfigInitArgs, ConfigTemplate, ConfigValidateArgs, DoctorArgs, PanelArgs, ServeArgs,
    SupportBundleArgs,
};
use broker_config::{
    ensure_cluster_config, load_config, load_managed_config, managed_config_path,
    resolve_from_path, resolve_serve, write_config, BetterMqConfig, Component, PanelMode,
    ResolvedAuth, ResolvedServeSettings, ServeOverrides,
};
use broker_dispatch::{DispatchConfig, DispatchEngine};
use broker_partition::{Broker, BrokerConfig, PublishRequest};
use broker_raft_meta::{ClusterConfig, ClusterRuntime, NodeConfig};
use broker_schedule::{CronRegistry, ScheduleQueue};
use broker_storage::StorageMode;
use chrono::Utc;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tower_http::{
    cors::{AllowOrigin, Any, CorsLayer},
    trace::TraceLayer,
};

mod cluster_health;
mod docs;
mod panel;
mod security;
mod startup_banner;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    match Cli::parse_args().command {
        Commands::Serve(args) => serve(args).await,
        Commands::Cluster { cmd } => match cmd {
            ClusterCommands::Init(a) => cluster_init(a),
            ClusterCommands::Join(a) => cluster_join(a).await,
        },
        Commands::Config { cmd } => match cmd {
            ConfigCommands::Init(a) => config_init(a),
            ConfigCommands::Validate(a) => config_validate(a),
            ConfigCommands::Schema => config_schema(),
        },
        Commands::Doctor(args) => doctor(args),
        Commands::SupportBundle(args) => support_bundle(args),
        Commands::Panel(args) => serve_panel(args).await,
    }
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();
}

fn config_init(args: ConfigInitArgs) -> anyhow::Result<()> {
    let cfg = match args.template {
        ConfigTemplate::Local => BetterMqConfig::template_single_local(),
        ConfigTemplate::Slate => BetterMqConfig::template_single_slate(),
        ConfigTemplate::Cluster => BetterMqConfig::template_cluster_local(),
        #[cfg(feature = "cloud")]
        ConfigTemplate::Cloud => BetterMqConfig::template_cloud(),
    };
    write_config(&args.output, &cfg).context("write bettermq.json")?;
    info!(path = %args.output.display(), template = ?args.template, "wrote config");
    Ok(())
}

fn config_validate(args: ConfigValidateArgs) -> anyhow::Result<()> {
    load_config(&args.config).context("invalid bettermq.json")?;
    info!(path = %args.config.display(), "config valid");
    Ok(())
}

fn config_schema() -> anyhow::Result<()> {
    #[cfg(feature = "cloud")]
    let examples = serde_json::json!({
        "version": broker_config::CONFIG_VERSION,
        "description": "BetterMQ configuration. Run: bettermq config init --template <local|slate|cluster|cloud>",
        "templates": {
            "local": BetterMqConfig::template_single_local(),
            "slate": BetterMqConfig::template_single_slate(),
            "cluster": BetterMqConfig::template_cluster_local(),
            "cloud": BetterMqConfig::template_cloud(),
        }
    });
    #[cfg(not(feature = "cloud"))]
    let examples = serde_json::json!({
        "version": broker_config::CONFIG_VERSION,
        "description": "BetterMQ configuration. Run: bettermq config init --template <local|slate|cluster>",
        "templates": {
            "local": BetterMqConfig::template_single_local(),
            "slate": BetterMqConfig::template_single_slate(),
            "cluster": BetterMqConfig::template_cluster_local(),
        }
    });
    println!("{}", serde_json::to_string_pretty(&examples)?);
    Ok(())
}

fn doctor(args: DoctorArgs) -> anyhow::Result<()> {
    let report = build_doctor_report(&args.data_dir, args.config.as_deref());
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        for check in report["checks"].as_array().into_iter().flatten() {
            println!(
                "{:<5} {:<18} {}",
                check["status"].as_str().unwrap_or("ERROR"),
                check["name"].as_str().unwrap_or("unknown"),
                check["detail"].as_str().unwrap_or("")
            );
        }
    }
    if report["ok"].as_bool() == Some(true) {
        Ok(())
    } else {
        anyhow::bail!("doctor found one or more errors")
    }
}

fn support_bundle(args: SupportBundleArgs) -> anyhow::Result<()> {
    let config_path = args.config.clone().or_else(|| {
        managed_config_path(&args.data_dir)
            .exists()
            .then(|| managed_config_path(&args.data_dir))
    });
    let mut config = config_path
        .as_deref()
        .and_then(|path| std::fs::read(path).ok())
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
        .unwrap_or(serde_json::Value::Null);
    broker_cli::redact_json(&mut config);

    let mut environment = serde_json::Map::new();
    for (key, value) in std::env::vars() {
        if key.starts_with("BETTERMQ_") || key.starts_with("AWS_") || key == "DATABASE_URL" {
            let value = if broker_cli::is_sensitive_key(&key) {
                "[REDACTED]".to_string()
            } else {
                value
            };
            environment.insert(key, serde_json::Value::String(value));
        }
    }
    let mut environment = serde_json::Value::Object(environment);
    broker_cli::redact_json(&mut environment);
    let bundle = serde_json::json!({
        "format_version": 1,
        "generated_at": Utc::now().to_rfc3339(),
        "bettermq_version": env!("CARGO_PKG_VERSION"),
        "protocol_version": broker_proto::PROTOCOL_VERSION,
        "platform": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH
        },
        "doctor": build_doctor_report(&args.data_dir, config_path.as_deref()),
        "config_path": config_path,
        "config": config,
        "environment": environment,
        "files": inventory_files(&args.data_dir, 10_000),
    });
    if let Some(parent) = args.output.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    let part = args.output.with_extension("json.part");
    std::fs::write(&part, serde_json::to_vec_pretty(&bundle)?)?;
    broker_storage::set_secret_file_mode(&part);
    std::fs::rename(&part, &args.output)?;
    broker_storage::set_secret_file_mode(&args.output);
    println!("wrote redacted support bundle {}", args.output.display());
    Ok(())
}

fn build_doctor_report(
    data_dir: &std::path::Path,
    config: Option<&std::path::Path>,
) -> serde_json::Value {
    let mut checks = Vec::new();
    let mut ok = true;
    let mut add = |name: &str, status: &str, detail: String| {
        if status == "ERROR" {
            ok = false;
        }
        checks.push(serde_json::json!({"name": name, "status": status, "detail": detail}));
    };

    if !data_dir.is_dir() {
        add(
            "data-directory",
            "ERROR",
            format!(
                "{} does not exist or is not a directory",
                data_dir.display()
            ),
        );
    } else {
        let probe = data_dir.join(format!(".doctor-write-test-{}", std::process::id()));
        match std::fs::write(&probe, b"ok").and_then(|_| std::fs::remove_file(&probe)) {
            Ok(()) => add(
                "data-directory",
                "OK",
                format!("{} is readable and writable", data_dir.display()),
            ),
            Err(error) => add("data-directory", "ERROR", error.to_string()),
        }
    }

    let config_path = config.map(std::path::Path::to_path_buf).or_else(|| {
        managed_config_path(data_dir)
            .exists()
            .then(|| managed_config_path(data_dir))
    });
    match config_path {
        Some(path) => match load_config(&path) {
            Ok(cfg) => {
                add("config", "OK", format!("{} is compatible", path.display()));
                let profile = cfg
                    .components
                    .as_ref()
                    .and_then(|s| s.profile.clone())
                    .unwrap_or_else(|| "all".into());
                add(
                    "components",
                    "OK",
                    format!(
                        "schema v{} profile {profile} (V1 files resolve in memory, not rewritten)",
                        cfg.version
                    ),
                );
            }
            Err(error) => add("config", "ERROR", format!("{}: {error:#}", path.display())),
        },
        None => add(
            "config",
            "WARN",
            "no config file found; serve defaults or CLI flags may be intentional".into(),
        ),
    }

    let files = inventory_paths(data_dir, 10_000);
    let wal_files: Vec<_> = files
        .iter()
        .filter(|path| {
            path.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|name| {
                    name == "active.wal" || (name.starts_with("segment-") && name.ends_with(".log"))
                })
        })
        .collect();
    let mut frames = 0usize;
    let mut wal_error = None;
    for path in &wal_files {
        match verify_wal_frames(path) {
            Ok(count) => frames += count,
            Err(error) => {
                wal_error = Some(format!("{}: {error}", path.display()));
                break;
            }
        }
    }
    if let Some(error) = wal_error {
        add("wal-checksums", "ERROR", error);
    } else {
        add(
            "wal-checksums",
            "OK",
            format!(
                "{} WAL/segment files, {frames} valid frames",
                wal_files.len()
            ),
        );
    }

    let archive_files = std::env::var_os("BETTERMQ_ARCHIVE_DIR")
        .map(std::path::PathBuf::from)
        .map(|root| inventory_paths(&root, 10_000))
        .unwrap_or_default();
    let manifests: Vec<_> = archive_files
        .iter()
        .filter(|path| {
            path.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|name| name.ends_with(".manifest.json"))
        })
        .collect();
    let invalid_manifest = manifests.iter().find_map(|path| match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice::<serde_json::Value>(&bytes)
            .err()
            .map(|error| format!("{}: {error}", path.display())),
        Err(error) => Some(format!("{}: {error}", path.display())),
    });
    if let Some(error) = invalid_manifest {
        add("archive-manifests", "ERROR", error);
    } else {
        add(
            "archive-manifests",
            "OK",
            format!("{} manifests parsed", manifests.len()),
        );
    }

    serde_json::json!({"ok": ok, "checks": checks})
}

fn verify_wal_frames(path: &std::path::Path) -> Result<usize, String> {
    use std::io::BufRead;
    let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
    let mut reader = std::io::BufReader::new(file);
    let manifest = path
        .parent()
        .into_iter()
        .flat_map(|parent| {
            [
                parent.to_path_buf(),
                parent.parent().unwrap_or(parent).to_path_buf(),
            ]
        })
        .map(|dir| broker_storage::WalManifest::path(&dir))
        .find(|candidate| candidate.exists())
        .and_then(|manifest| std::fs::read(manifest).ok())
        .and_then(|bytes| serde_json::from_slice::<broker_storage::WalManifest>(&bytes).ok());
    let mut count = 0usize;
    loop {
        if reader.fill_buf().map_err(|e| e.to_string())?.is_empty() {
            return Ok(count);
        }
        if manifest
            .as_ref()
            .is_some_and(|manifest| manifest.wal_format_version == broker_storage::WAL_FORMAT_V2)
        {
            let (epoch, body) =
                broker_proto::decode_epoch(&mut reader).map_err(|e| e.to_string())?;
            let mut body = std::io::Cursor::new(body);
            let mut epoch_records = 0u32;
            while body.position() < body.get_ref().len() as u64 {
                broker_proto::decode_frame(&mut body).map_err(|e| e.to_string())?;
                epoch_records += 1;
            }
            if epoch_records != epoch.record_count {
                return Err(format!(
                    "epoch declared {} records but contained {epoch_records}",
                    epoch.record_count
                ));
            }
            count += epoch_records as usize;
        } else {
            broker_proto::decode_frame(&mut reader).map_err(|e| e.to_string())?;
            count += 1;
        }
    }
}

fn inventory_paths(root: &std::path::Path, limit: usize) -> Vec<std::path::PathBuf> {
    fn visit(
        dir: &std::path::Path,
        depth: usize,
        limit: usize,
        output: &mut Vec<std::path::PathBuf>,
    ) {
        if depth > 16 || output.len() >= limit {
            return;
        }
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            if output.len() >= limit {
                return;
            }
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                visit(&path, depth + 1, limit, output);
            } else if file_type.is_file() {
                output.push(path);
            }
        }
    }
    let mut output = Vec::new();
    if root.is_dir() {
        visit(root, 0, limit, &mut output);
    }
    output
}

fn inventory_files(root: &std::path::Path, limit: usize) -> Vec<serde_json::Value> {
    inventory_paths(root, limit)
        .into_iter()
        .map(|path| {
            let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            let relative = path.strip_prefix(root).unwrap_or(&path);
            serde_json::json!({"path": relative, "bytes": bytes})
        })
        .collect()
}

fn cluster_init(args: ClusterInitArgs) -> anyhow::Result<()> {
    std::fs::create_dir_all(&args.data_dir)?;
    let node_id = args.node_id.unwrap_or_else(|| stable_node_id(&args.addr));
    let mut nodes = Vec::new();
    for peer in &args.peers {
        nodes.push(NodeConfig {
            id: stable_node_id(peer),
            addr: peer.clone(),
        });
    }
    if !args.peers.iter().any(|p| p == &args.addr) {
        nodes.push(NodeConfig {
            id: node_id,
            addr: args.addr.clone(),
        });
    }
    let config = ClusterConfig {
        cluster_id: Uuid::new_v4(),
        nodes,
        node_id,
        generation: 1,
        hash_version: 1,
    };
    ClusterRuntime::init_cluster_file(&args.data_dir, &config)?;
    let cfg_path = args.data_dir.join("cluster-config.json");
    std::fs::write(cfg_path, serde_json::to_vec_pretty(&config)?)?;
    info!(node_id = %node_id, peers = args.peers.len(), "cluster initialized");
    Ok(())
}

async fn cluster_join(args: ClusterJoinArgs) -> anyhow::Result<()> {
    std::fs::create_dir_all(&args.data_dir)?;
    let client = reqwest::Client::new();
    let url = format!("{}/internal/v1/cluster", args.seed.trim_end_matches('/'));
    let req = broker_api::cluster_auth::apply_cluster_secret(client.get(&url));
    let remote: ClusterConfig = req
        .send()
        .await
        .context("fetch cluster from seed")?
        .error_for_status()
        .context("seed cluster endpoint")?
        .json()
        .await
        .context("decode cluster config")?;

    let node_id = args.node_id.unwrap_or_else(Uuid::new_v4);
    let mut nodes = remote.nodes;
    nodes.push(NodeConfig {
        id: node_id,
        addr: args.addr.clone(),
    });
    let config = ClusterConfig {
        cluster_id: remote.cluster_id,
        nodes,
        node_id,
        generation: remote.generation + 1,
        hash_version: remote.hash_version.max(broker_config::HASH_VERSION),
    };
    let cfg_path = args.data_dir.join("cluster-config.json");
    std::fs::write(cfg_path, serde_json::to_vec_pretty(&config)?)?;
    ClusterRuntime::init_cluster_file(&args.data_dir, &config)?;
    info!(node_id = %node_id, "joined cluster");
    Ok(())
}

fn stable_node_id(addr: &str) -> Uuid {
    broker_config::stable_node_id(addr)
}

#[cfg(feature = "cloud")]
fn serve_database_url(args: &ServeArgs) -> Option<String> {
    args.database_url.clone()
}

#[cfg(not(feature = "cloud"))]
fn serve_database_url(_args: &ServeArgs) -> Option<String> {
    None
}

fn serve_overrides(args: &ServeArgs) -> ServeOverrides {
    ServeOverrides {
        config_path: args.config.clone(),
        listen: args.listen,
        port: args.port,
        data_dir: args.data_dir.clone(),
        cluster: args.cluster,
        database_url: serve_database_url(args),
        dispatch_fleet: if args.dispatch_fleet {
            Some(true)
        } else {
            None
        },
        broker_only: if args.broker_only { Some(true) } else { None },
        gateway_only: if args.gateway_only { Some(true) } else { None },
        panel_listen: args.panel_listen,
        admin_listen: args.admin_listen,
        internal_listen: args.internal_listen,
        profile: args.profile.clone(),
        components: args.components.clone(),
        no_panel: args.no_panel,
        standalone_panel: false,
        controller_url: None,
    }
}

async fn serve(args: ServeArgs) -> anyhow::Result<()> {
    let overrides = serve_overrides(&args);
    let data_dir = overrides
        .data_dir
        .clone()
        .or_else(|| args.data_dir.clone())
        .unwrap_or_else(|| std::path::PathBuf::from("./data"));
    let managed_path = managed_config_path(&data_dir);
    let config_path = args
        .config
        .clone()
        .or_else(|| managed_path.exists().then_some(managed_path));

    let file_cfg = if let Some(ref path) = config_path {
        Some(load_config(path).with_context(|| format!("load {}", path.display()))?)
    } else {
        None
    };

    let settings = if let Some(ref path) = config_path {
        resolve_from_path(path, &overrides)?
    } else {
        resolve_serve(file_cfg.as_ref(), &overrides)?
    };

    settings.apply_env();
    std::env::set_var(
        "BETTERMQ_PANEL_MODE",
        match settings.panel_mode {
            PanelMode::Embedded => "embedded",
            PanelMode::SeparateListener => "separate",
            PanelMode::Disabled => "disabled",
            PanelMode::Standalone => "standalone",
        },
    );

    if settings.gateway_only || (settings.has(Component::Gateway) && !settings.opens_local_storage)
    {
        if settings.broker_only || settings.dispatch_fleet {
            anyhow::bail!(
                "--gateway-only cannot be combined with --broker-only or --dispatch-fleet"
            );
        }
        return serve_gateway_only(&settings).await;
    }

    if !settings.opens_local_storage {
        return serve_panel_from_settings(&settings).await;
    }

    std::fs::create_dir_all(&settings.data_dir)
        .with_context(|| format!("create data dir {}", settings.data_dir.display()))?;
    broker_storage::start_archive_service();

    if settings.cluster_enabled {
        let cluster_cfg = if let Some(c) = &file_cfg {
            c.clone()
        } else {
            load_managed_config(&settings.data_dir)
                .context("load managed config")?
                .context(
                    "cluster mode requires saved infrastructure config — use Panel → Infrastructure",
                )?
        };
        ensure_cluster_config(&cluster_cfg, &settings.data_dir)
            .context("write cluster-config.json")?;
    }

    let postgres = matches!(&settings.auth, ResolvedAuth::Cloud { .. });
    let mut first_boot_task = None;

    #[cfg(feature = "cloud")]
    let (auth, local_auth, control_plane) = match &settings.auth {
        ResolvedAuth::Cloud { database_url } => {
            let cp = broker_control_plane::ControlPlanePool::connect(database_url.as_str())
                .await
                .context("connect control plane postgres")?;
            cp.migrate().await.context("migrate control plane")?;
            let auth = broker_control_plane::ApiKeyValidator::new(cp.clone());
            (Some(auth), None, Some(cp))
        }
        ResolvedAuth::Local { .. } => {
            let local = broker_local_auth::LocalAuthStore::open(&settings.data_dir)
                .context("open local auth")?;
            if local.is_configured() {
                info!("local API token auth enabled");
            } else {
                first_boot_task = start_first_boot_setup(&settings.data_dir);
            }
            (None, Some(Arc::new(local)), None)
        }
    };

    #[cfg(not(feature = "cloud"))]
    let local_auth = match &settings.auth {
        ResolvedAuth::Cloud { .. } => {
            anyhow::bail!("auth.mode \"cloud\" is not supported in the self-host build (use auth.mode \"local\")")
        }
        ResolvedAuth::Local { .. } => {
            let local = broker_local_auth::LocalAuthStore::open(&settings.data_dir)
                .context("open local auth")?;
            if local.is_configured() {
                info!("local API token auth enabled");
            } else {
                first_boot_task = start_first_boot_setup(&settings.data_dir);
            }
            Some(Arc::new(local))
        }
    };

    let mut broker_cfg = BrokerConfig::new(settings.data_dir.clone());
    broker_cfg.storage = match settings.storage {
        broker_config::StorageMode::Local => StorageMode::Local,
        broker_config::StorageMode::Slate => StorageMode::Slate,
    };
    broker_cfg.retry_defaults = settings.dispatch_retry.clone();
    let storage = broker_cfg.storage;
    let broker = Broker::open(broker_cfg).context("open broker storage")?;

    let cluster = if settings.cluster_enabled {
        let config =
            ClusterRuntime::load_config(&settings.data_dir).context("load cluster-config.json")?;
        let runtime =
            ClusterRuntime::open_with_raft(&settings.data_dir, config, broker.layout().shard_count)
                .await
                .context("open OpenRaft controller")?;
        Some(Cluster::new(runtime))
    } else {
        None
    };
    if let Some(ref c) = cluster {
        let rt = c.runtime.clone();
        broker.set_shard_leader_check(Arc::new(move |p| {
            if rt.is_leader_for_shard(p) {
                Some(rt.shard_generation(p))
            } else {
                None
            }
        }));
    }
    let schedule = ScheduleQueue::open(&settings.data_dir).context("open schedule queue")?;
    let crons = CronRegistry::open(&settings.data_dir).context("open cron registry")?;
    let catalog_tombstones =
        CatalogTombstones::open(&settings.data_dir).context("open catalog tombstones")?;

    let dispatch_cfg = DispatchConfig {
        retry_defaults: settings.dispatch_retry.clone(),
        http_timeout_secs: settings.dispatch_http_timeout_secs,
        long_http_timeout_secs: settings.dispatch_long_http_timeout_secs,
        long_payload_threshold_bytes: 256 * 1024,
        ..DispatchConfig::default()
    };
    let leases = broker_dispatch::LeaseTable::new();
    let broker_only = settings.broker_only || settings.dispatch_fleet;
    let mut dispatch = if broker_only {
        DispatchEngine::new_broker_only(broker.clone(), dispatch_cfg)
    } else {
        DispatchEngine::new(broker.clone(), dispatch_cfg)
    };
    dispatch = dispatch.with_leases(leases.clone());
    if let Some(ref c) = cluster {
        let rt = c.runtime.clone();
        dispatch = dispatch.with_shard_leader_check(Arc::new(move |p| rt.is_leader_for_shard(p)));
    }
    if !broker_only {
        dispatch.backfill_pending();
    }

    let app_state = Arc::new(AppState {
        broker,
        schedule,
        crons,
        dispatch,
        leases,
        cluster,
        local_auth,
        fair_queue: Arc::new(broker_dispatch::TenantFairQueue::new()),
        catalog_tombstones,
        dispatch_fleet: settings.dispatch_fleet,
        broker_only: settings.broker_only,
        #[cfg(feature = "cloud")]
        auth,
        #[cfg(feature = "cloud")]
        control_plane,
    });

    let mut bg_tasks = Vec::new();
    if let Some(task) = first_boot_task {
        bg_tasks.push(task);
    }
    if settings.cluster_enabled {
        if let Some(h) = spawn_cluster_catalog_sync(app_state.clone()) {
            bg_tasks.push(h);
        }
    }
    if let Some(ref c) = app_state.cluster {
        bg_tasks.push(cluster_health::spawn_cluster_health_monitor(
            c.clone(),
            app_state.dispatch.clone(),
            app_state.clone(),
        ));
    }

    if !settings.dispatch_fleet {
        bg_tasks.push(spawn_schedule_worker(app_state.clone()));
    }
    if app_state.broker.config().log.fsync == broker_storage::FsyncMode::Group {
        bg_tasks.push(spawn_wal_group_flusher(app_state.broker.clone()));
    }
    if !broker_only {
        bg_tasks.push(spawn_dispatch_backfill_loop(app_state.clone()));
    }
    if settings.dispatch_fleet {
        if let Some(h) = spawn_fleet_workers(app_state.clone()) {
            bg_tasks.push(h);
        }
    }

    let cors = build_cors_layer();

    if settings.dispatch_fleet {
        info!("dispatch fleet mode: claiming from BETTERMQ_BROKER_URLS");
    }
    if settings.broker_only {
        info!("broker-only mode: lease API enabled, local delivery workers off");
    }

    let include_panel =
        settings.panel_mode != PanelMode::Disabled && settings.has(Component::Panel);
    let registry_path = settings.data_dir.join("cell-registry.json");
    let controller_url = settings
        .controller_url
        .clone()
        .unwrap_or_else(|| format!("http://{}", settings.listeners.admin));
    let admin_state = AdminState::local_with_registry(
        app_state.clone(),
        Some(registry_path),
        Some(controller_url),
    );
    let panel_and_admin = {
        let mut r = admin_router(admin_state);
        if include_panel {
            r = r
                .route("/panel", get(|| async { Redirect::permanent("/panel/") }))
                .nest("/panel/", panel::resolve_router());
        }
        r.merge(docs::router())
            .layer(axum::middleware::from_fn(security::api_security_headers))
            .layer(cors.clone())
            .layer(TraceLayer::new_for_http())
    };

    let collapsed = settings.listeners.collapsed
        || settings.panel_mode == PanelMode::Embedded
            && settings.listeners.public == settings.listeners.internal;
    let app = if collapsed {
        router((*app_state).clone())
            .merge(panel_and_admin.clone())
            .layer(axum::middleware::from_fn(security::api_security_headers))
            .layer(cors.clone())
            .layer(TraceLayer::new_for_http())
    } else {
        public_router((*app_state).clone())
            .merge(docs::router())
            .layer(axum::middleware::from_fn(security::api_security_headers))
            .layer(cors.clone())
            .layer(TraceLayer::new_for_http())
    };

    let listener = tokio::net::TcpListener::bind(settings.listeners.public)
        .await
        .with_context(|| format!("bind {}", settings.listeners.public))?;

    if !collapsed {
        let internal_app = internal_router((*app_state).clone())
            .layer(axum::middleware::from_fn(security::api_security_headers))
            .layer(cors.clone())
            .layer(TraceLayer::new_for_http());
        let internal_listener = tokio::net::TcpListener::bind(settings.listeners.internal)
            .await
            .with_context(|| format!("bind internal {}", settings.listeners.internal))?;
        info!(addr = %settings.listeners.internal, "internal listen");
        bg_tasks.push(tokio::spawn(async move {
            let _ = axum::serve(internal_listener, internal_app).await;
        }));
        if settings.listeners.admin != settings.listeners.public {
            let admin_listener = tokio::net::TcpListener::bind(settings.listeners.admin)
                .await
                .with_context(|| format!("bind admin {}", settings.listeners.admin))?;
            info!(addr = %settings.listeners.admin, "admin listen");
            let admin_app = panel_and_admin;
            bg_tasks.push(tokio::spawn(async move {
                let _ = axum::serve(admin_listener, admin_app).await;
            }));
        }
    } else if settings.panel_mode == PanelMode::SeparateListener {
        let panel_addr = settings.listeners.admin;
        let panel_listener = tokio::net::TcpListener::bind(panel_addr)
            .await
            .with_context(|| format!("bind panel {}", panel_addr))?;
        info!(%panel_addr, "panel+admin listen");
        bg_tasks.push(tokio::spawn(async move {
            let _ = axum::serve(panel_listener, panel_and_admin).await;
        }));
    }

    startup_banner::print(&settings, storage);
    if postgres {
        info!("cloud auth enabled (postgres)");
    }

    let shutdown_deadline = std::env::var("BETTERMQ_SHUTDOWN_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(30u64)
        .clamp(1, 300);
    let drain_notice = Arc::new(tokio::sync::Notify::new());
    let signal_notice = drain_notice.clone();
    let shutdown = async move {
        shutdown_signal().await;
        begin_shutdown();
        info!(
            timeout_secs = shutdown_deadline,
            "shutdown signal received; admission stopped, draining HTTP"
        );
        signal_notice.notify_one();
    };

    let server = axum::serve(listener, app).with_graceful_shutdown(shutdown);
    let mut server = Box::pin(std::future::IntoFuture::into_future(server));
    tokio::select! {
        result = &mut server => {
            result.context("HTTP server exited with error")?;
        }
        _ = drain_notice.notified() => {
            if tokio::time::timeout(Duration::from_secs(shutdown_deadline), &mut server)
                .await
                .is_err()
            {
                tracing::warn!("HTTP drain deadline exceeded; closing remaining connections");
            }
        }
    }

    if app_state
        .dispatch
        .drain(Duration::from_secs(shutdown_deadline))
        .await
    {
        info!("dispatch drain complete");
    } else {
        tracing::warn!("dispatch drain deadline exceeded; retry state remains durable");
    }

    let broker = app_state.broker.clone();
    match tokio::time::timeout(
        Duration::from_secs(shutdown_deadline),
        tokio::task::spawn_blocking(move || broker.flush_wal()),
    )
    .await
    {
        Ok(Ok(Ok(()))) => info!("WAL drain complete"),
        Ok(Ok(Err(error))) => tracing::warn!(%error, "WAL flush on shutdown failed"),
        Ok(Err(error)) => tracing::warn!(%error, "WAL flush task failed"),
        Err(_) => tracing::warn!("WAL flush deadline exceeded"),
    }
    abort_and_join(bg_tasks, Duration::from_secs(shutdown_deadline.min(5))).await;
    app_state.dispatch.shutdown_background().await;
    drop(app_state);
    // Let reqwest/hyper drop timers before #[tokio::main] tears down the runtime.
    tokio::task::yield_now().await;
    info!("dispatch, scheduler, and panel tasks stopped");

    Ok(())
}

async fn serve_panel(args: PanelArgs) -> anyhow::Result<()> {
    let mut overrides = ServeOverrides {
        config_path: args.config.clone(),
        listen: Some(args.listen),
        standalone_panel: true,
        controller_url: args.controller.clone(),
        data_dir: args.data_dir.clone(),
        ..ServeOverrides::default()
    };
    overrides.panel_listen = Some(args.listen);
    let file_cfg = args
        .config
        .as_ref()
        .map(|path| load_config(path))
        .transpose()
        .context("load panel config")?;
    let mut settings = resolve_serve(file_cfg.as_ref(), &overrides)?;
    settings.listen = args.listen;
    settings.listeners.public = args.listen;
    settings.listeners.admin = args.listen;
    settings.listeners.internal = args.listen;
    settings.panel_mode = PanelMode::Standalone;
    settings.controller_url = args.controller.or(settings.controller_url);
    std::env::set_var("BETTERMQ_PANEL_MODE", "standalone");
    serve_panel_from_settings(&settings).await
}

async fn serve_panel_from_settings(settings: &ResolvedServeSettings) -> anyhow::Result<()> {
    let registry_path = settings.data_dir.join("cell-registry.json");
    let registry = broker_api::CellRegistry::load(&registry_path);
    let controller = settings
        .controller_url
        .clone()
        .unwrap_or_else(|| format!("http://{}", settings.listen));
    let state = AdminState::remote_with_path(controller, registry, Some(registry_path));
    let app = admin_router(state)
        .route("/panel", get(|| async { Redirect::permanent("/panel/") }))
        .nest("/panel/", panel::resolve_router())
        .merge(docs::router())
        .route("/healthz", get(|| async { "ok" }))
        .layer(axum::middleware::from_fn(security::api_security_headers))
        .layer(build_cors_layer())
        .layer(TraceLayer::new_for_http());
    let addr = settings.listeners.admin;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind panel {}", addr))?;
    info!(
        listen = %addr,
        "standalone panel: no broker WAL, RocksDB, archive, or dispatch opened"
    );
    let shutdown = async {
        shutdown_signal().await;
        begin_shutdown();
        info!("panel shutdown signal received; draining HTTP");
    };
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .context("panel HTTP server exited")
}

async fn serve_gateway_only(settings: &ResolvedServeSettings) -> anyhow::Result<()> {
    let state = GatewayOnlyState::from_env().map_err(anyhow::Error::msg)?;
    #[cfg(feature = "cloud")]
    let state = match &settings.auth {
        ResolvedAuth::Cloud { database_url } => {
            let control_plane = broker_control_plane::ControlPlanePool::connect(database_url)
                .await
                .context("connect gateway control plane")?;
            let auth = broker_control_plane::ApiKeyValidator::new(control_plane.clone());
            state.with_cloud_auth(auth, control_plane)
        }
        ResolvedAuth::Local { .. } => state,
    };
    #[cfg(not(feature = "cloud"))]
    if matches!(&settings.auth, ResolvedAuth::Cloud { .. }) {
        anyhow::bail!("cloud auth is unavailable in this build");
    }

    let app = gateway_only_router(state)
        .merge(docs::router())
        .layer(axum::middleware::from_fn(security::api_security_headers))
        .layer(build_cors_layer())
        .layer(TraceLayer::new_for_http());
    let listener = tokio::net::TcpListener::bind(settings.listen)
        .await
        .with_context(|| format!("bind {}", settings.listen))?;
    info!(
        listen = %settings.listen,
        "gateway-only mode: no local broker, WAL, index, scheduler, dispatch, or archive opened"
    );
    let shutdown = async {
        shutdown_signal().await;
        begin_shutdown();
        info!("gateway shutdown signal received; draining HTTP");
    };
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .context("gateway HTTP server exited")
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    result = tokio::signal::ctrl_c() => {
                        if let Err(error) = result {
                            tracing::warn!(%error, "SIGINT handler failed");
                        }
                    }
                    _ = terminate.recv() => {}
                }
            }
            Err(error) => {
                tracing::warn!(%error, "SIGTERM handler failed; waiting for SIGINT");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::warn!(%error, "shutdown signal handler failed");
    }
}

fn build_cors_layer() -> CorsLayer {
    // Default: same-origin panel needs no CORS. Set BETTERMQ_CORS_ORIGINS to a
    // comma-separated allowlist, or `*` only when you intentionally need it.
    let raw = std::env::var("BETTERMQ_CORS_ORIGINS").unwrap_or_default();
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return CorsLayer::new();
    }
    if trimmed == "*" {
        return CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any);
    }
    let origins: Vec<_> = trimmed
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.parse().ok())
        .collect();
    if origins.is_empty() {
        return CorsLayer::new();
    }
    CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_methods(Any)
        .allow_headers(Any)
}

fn start_first_boot_setup(data_dir: &std::path::Path) -> Option<JoinHandle<()>> {
    // Older builds wrote a one-time secret into the volume. That is unused now.
    let leftover = data_dir.join("setup-token.txt");
    if leftover.exists() {
        if let Err(e) = std::fs::remove_file(&leftover) {
            tracing::warn!(error = %e, "could not remove leftover setup-token.txt");
        }
    }

    if matches!(
        std::env::var("BETTERMQ_ALLOW_OPEN_SETUP")
            .ok()
            .as_deref()
            .map(str::trim),
        Some("1") | Some("true") | Some("TRUE") | Some("yes")
    ) {
        info!("local auth not configured — open setup allowed (BETTERMQ_ALLOW_OPEN_SETUP)");
        eprintln!(
            "Open /panel/ and set a password. Anyone who can reach this port can claim admin."
        );
        return None;
    }

    let secs: u64 = std::env::var("BETTERMQ_SETUP_WINDOW_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(15 * 60);
    if secs == 0 {
        info!("local auth not configured — setup locked (BETTERMQ_SETUP_WINDOW_SECS=0)");
        eprintln!(
            "Setup is locked. Restart BetterMQ to open a setup window, or set BETTERMQ_ALLOW_OPEN_SETUP=1."
        );
        return None;
    }

    open_setup_window(Duration::from_secs(secs));
    let mins = secs.div_ceil(60);
    info!(
        minutes = mins,
        "local auth not configured — setup window open"
    );
    eprintln!("Set a panel password at /panel/ (open for {mins} minutes).");
    eprintln!("After that, restart this process to open setup again.");
    Some(tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(secs)).await;
        eprintln!("Setup window closed. Restart BetterMQ to set a password.");
    }))
}

async fn abort_and_join(tasks: Vec<JoinHandle<()>>, timeout: Duration) {
    for task in &tasks {
        task.abort();
    }
    let joining = async {
        for task in tasks {
            let _ = task.await;
        }
    };
    if tokio::time::timeout(timeout, joining).await.is_err() {
        tracing::warn!("background task join deadline exceeded");
    }
}

fn spawn_wal_group_flusher(broker: Broker) -> tokio::task::JoinHandle<()> {
    let interval = broker.config().log.group_interval;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            if let Err(e) = broker.flush_wal_if_due() {
                tracing::warn!(error = %e, "wal group flush failed");
            }
        }
    })
}

fn spawn_dispatch_backfill_loop(state: Arc<AppState>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        interval.tick().await; // skip immediate tick (boot already backfills)
        loop {
            interval.tick().await;
            state.dispatch.backfill_pending();
        }
    })
}

fn spawn_schedule_worker(state: Arc<AppState>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(200));
        loop {
            interval.tick().await;
            let scheduler_leader = match &state.cluster {
                None => true,
                Some(c) => {
                    if let Err(error) = c.runtime.acquire_scheduler_leader(5_000).await {
                        tracing::warn!(%error, "OpenRaft scheduler lease acquisition failed");
                    }
                    c.runtime.is_scheduler_leader()
                }
            };
            if !scheduler_leader {
                continue;
            }

            let now = Utc::now().timestamp_millis();

            for pending in state.schedule.pop_due(now) {
                let id = pending.id;
                let deliver_at_ms = pending.deliver_at_ms;
                let ok = fire_scheduled_publish(&state, pending.request.clone()).await;
                if ok {
                    if let Err(e) = state.schedule.complete(id) {
                        tracing::warn!(error = %e, "schedule complete persist failed");
                    }
                } else if let Err(e) = state.schedule.requeue(broker_schedule::ScheduledPublish {
                    id,
                    deliver_at_ms,
                    request: pending.request,
                }) {
                    tracing::warn!(error = %e, "schedule requeue after failed fire failed");
                }
            }

            for job in state.crons.pop_due(now) {
                tracing::info!(cron_id = %job.id, queue = %job.request.topic, "cron tick");
                let mut req = job.request.clone();
                // Idempotent publish key absorbs brief dual-hold / retry.
                if req.idempotency_key.is_none() {
                    req.idempotency_key = Some(format!(
                        "cron:{}:{}",
                        job.id,
                        job.last_run_at_ms.unwrap_or(now)
                    ));
                }
                let ok = fire_scheduled_publish(&state, req).await;
                if ok {
                    if let Err(e) = state.crons.commit_fire(job.id) {
                        tracing::warn!(error = %e, cron_id = %job.id, "cron commit_fire failed");
                    }
                } else if let Err(e) = state.crons.revert_fire(&job) {
                    tracing::warn!(error = %e, cron_id = %job.id, "cron revert_fire failed");
                }
            }
        }
    })
}

/// Returns true when the publish was accepted (including duplicates).
async fn fire_scheduled_publish(
    state: &Arc<AppState>,
    pending: broker_schedule::ScheduledPublishRequest,
) -> bool {
    let req = PublishRequest {
        topic: pending.topic,
        routing_key: pending.routing_key,
        payload: pending.payload,
        payload_encoding: pending.payload_encoding,
        idempotency_key: pending.idempotency_key,
        delay_ms: None,
        priority: pending.priority,
        flow_id: pending.flow_id,
        queue_id: pending.queue_id,
        group_id: None,
        group_member_id: None,
        destination: pending.destination,
        flow: pending.flow,
        parallelism: pending.parallelism,
        max_retries: pending.max_retries,
        retry_backoff: pending.retry_backoff.clone(),
        method: pending.method.clone(),
        headers: pending.headers.clone(),
        sign: pending.sign,
        request: pending.request.clone(),
        url: None,
        secret: None,
    };
    if state.cluster.is_some() {
        match publish_with_cluster(state, req, None).await {
            Ok(resp) => {
                if !resp.duplicate {
                    enqueue_dispatch_after_publish(state, &resp);
                }
                true
            }
            Err(e) => {
                tracing::warn!(error = ?e, "scheduled cluster publish failed");
                false
            }
        }
    } else {
        match state.broker.publish_immediate(req) {
            Ok(resp) => {
                if let (Some(partition), Some(offset)) = (resp.partition, resp.offset) {
                    if let Err(error) = state
                        .broker
                        .wait_committed(&resp.topic, partition, offset)
                        .await
                    {
                        tracing::warn!(
                            %error,
                            topic = %resp.topic,
                            partition,
                            offset,
                            "scheduled enqueue commit failed"
                        );
                        return false;
                    }
                }
                if !resp.duplicate {
                    if let (Some(partition), Some(offset), Some(message_id)) =
                        (resp.partition, resp.offset, resp.message_id)
                    {
                        if !broker_partition::is_dlq_topic(&resp.topic) {
                            state.dispatch.enqueue(broker_dispatch::DeliveryJob::live(
                                resp.topic, partition, offset, message_id,
                            ));
                        }
                    }
                }
                true
            }
            Err(e) => {
                tracing::warn!(error = %e, "scheduled enqueue failed");
                false
            }
        }
    }
}

fn spawn_fleet_workers(state: Arc<AppState>) -> Option<tokio::task::JoinHandle<()>> {
    let Some(client) = broker_dispatch::LeaseClient::from_env() else {
        tracing::error!(
            "dispatch fleet requires BETTERMQ_BROKER_URLS (comma-separated broker base URLs)"
        );
        return None;
    };
    let concurrency = broker_dispatch::fleet_concurrency();
    info!(
        holder = %client.holder(),
        brokers = ?client.broker_urls(),
        concurrency,
        "starting dispatch fleet workers"
    );
    let sem = Arc::new(tokio::sync::Semaphore::new(concurrency));
    Some(tokio::spawn(async move {
        let mut rr = 0usize;
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        loop {
            interval.tick().await;
            let urls = client.broker_urls();
            if urls.is_empty() {
                continue;
            }
            rr = (rr + 1) % urls.len();
            let broker = urls[rr].clone();
            let permit = match sem.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => break,
            };
            let client = client.clone();
            let dispatch = state.dispatch.clone();
            tokio::spawn(async move {
                let _permit = permit;
                if dispatch.is_draining() {
                    return;
                }
                let claimed = match client.claim(&broker, 4).await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::debug!(error = %e, %broker, "fleet claim");
                        return;
                    }
                };
                for job in claimed.jobs {
                    let Some(msg) = job.message.clone() else {
                        let _ = client
                            .fail(&broker, &job, "missing message envelope", true, 0)
                            .await;
                        continue;
                    };
                    let hb = client.clone();
                    let broker_hb = broker.clone();
                    let lease_id = job.lease_id;
                    let heartbeat = tokio::spawn(async move {
                        let mut tick = tokio::time::interval(Duration::from_secs(10));
                        loop {
                            tick.tick().await;
                            if hb.heartbeat(&broker_hb, lease_id).await.is_err() {
                                break;
                            }
                        }
                    });
                    match dispatch.push_http_only(&msg, job.committed_hwm).await {
                        Ok(()) => {
                            heartbeat.abort();
                            if let Err(e) = client.complete(&broker, &job).await {
                                tracing::warn!(error = %e, "fleet complete failed");
                            }
                        }
                        Err(e) => {
                            heartbeat.abort();
                            let dead = matches!(
                                &e,
                                broker_dispatch::DispatchError::NoDestination
                                    | broker_dispatch::DispatchError::Egress(_)
                                    | broker_dispatch::DispatchError::NonRetryable(_)
                                    | broker_dispatch::DispatchError::RetryExhausted(_)
                            );
                            let retry_after_ms =
                                if let broker_dispatch::DispatchError::RetryDeferred {
                                    retry_after_ms,
                                    ..
                                } = &e
                                {
                                    *retry_after_ms
                                } else if matches!(&e, broker_dispatch::DispatchError::HostBlocked)
                                {
                                    30_000
                                } else if matches!(&e, broker_dispatch::DispatchError::Uncommitted)
                                {
                                    250
                                } else {
                                    1_000
                                };
                            let _ = client
                                .fail(&broker, &job, &e.to_string(), dead, retry_after_ms)
                                .await;
                        }
                    }
                }
            });
        }
    }))
}
