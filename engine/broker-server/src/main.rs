use anyhow::Context;
use axum::{response::Redirect, routing::get};
use broker_api::{
    enqueue_dispatch_after_publish, publish_with_cluster, router, spawn_cluster_catalog_sync,
    AppState, CatalogTombstones, Cluster,
};
use broker_cli::{
    Cli, ClusterCommands, ClusterInitArgs, ClusterJoinArgs, Commands, ConfigCommands,
    ConfigInitArgs, ConfigTemplate, ConfigValidateArgs, ServeArgs,
};
use broker_config::{
    ensure_cluster_config, load_config, load_managed_config, managed_config_path,
    resolve_from_path, resolve_serve, write_config, BetterMqConfig, ResolvedAuth, ServeOverrides,
};
use broker_dispatch::{DispatchConfig, DispatchEngine};
use broker_partition::{Broker, BrokerConfig, PublishRequest};
use broker_raft_meta::{ClusterConfig, ClusterRuntime, NodeConfig};
use broker_schedule::{CronRegistry, ScheduleQueue};
use broker_storage::StorageMode;
use chrono::Utc;
use std::sync::Arc;
use std::time::Duration;
use tower_http::{
    cors::{AllowOrigin, Any, CorsLayer},
    trace::TraceLayer,
};

mod cluster_health;
mod docs;
mod panel;
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
        panel_listen: args.panel_listen,
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

    std::fs::create_dir_all(&settings.data_dir)
        .with_context(|| format!("create data dir {}", settings.data_dir.display()))?;

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
                ensure_setup_token(&settings.data_dir);
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
                ensure_setup_token(&settings.data_dir);
            }
            Some(Arc::new(local))
        }
    };

    let cluster = if settings.cluster_enabled {
        let config =
            ClusterRuntime::load_config(&settings.data_dir).context("load cluster-config.json")?;
        let runtime = ClusterRuntime::open(&settings.data_dir, config.clone())?;
        if config.node_count() >= 2
            && matches!(settings.storage, broker_config::StorageMode::Local)
            && !runtime.uses_shared_meta()
        {
            anyhow::bail!(
                "HA cluster (2+ nodes, local storage) requires BETTERMQ_SHARED_META_DIR / cluster.sharedMetaDir for fencing and scheduler lease"
            );
        }
        Some(Cluster::new(runtime))
    } else {
        None
    };

    let mut broker_cfg = BrokerConfig::new(settings.data_dir.clone());
    broker_cfg.storage = match settings.storage {
        broker_config::StorageMode::Local => StorageMode::Local,
        broker_config::StorageMode::Slate => StorageMode::Slate,
    };
    broker_cfg.retry_defaults = settings.dispatch_retry.clone();
    let storage = broker_cfg.storage;
    let broker = Broker::open(broker_cfg).context("open broker storage")?;
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

    if settings.cluster_enabled {
        spawn_cluster_catalog_sync(app_state.clone());
    }
    if let Some(ref c) = app_state.cluster {
        cluster_health::spawn_cluster_health_monitor(
            c.clone(),
            app_state.dispatch.clone(),
            app_state.clone(),
        );
    }

    if !settings.dispatch_fleet {
        spawn_schedule_worker(app_state.clone());
    }
    if !broker_only {
        spawn_dispatch_backfill_loop(app_state.clone());
    }
    if settings.dispatch_fleet {
        spawn_fleet_workers(app_state.clone());
    }

    let cors = build_cors_layer();

    let app = router((*app_state).clone());
    if settings.dispatch_fleet {
        info!("dispatch fleet mode: claiming from BETTERMQ_BROKER_URLS");
    }
    if settings.broker_only {
        info!("broker-only mode: lease API enabled, local delivery workers off");
    }
    let app = app
        .merge(docs::router())
        .route("/panel", get(|| async { Redirect::permanent("/panel/") }))
        .nest("/panel/", panel::resolve_router())
        .layer(cors.clone())
        .layer(TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind(settings.listen)
        .await
        .with_context(|| format!("bind {}", settings.listen))?;

    if let Some(panel_addr) = settings.panel_listen {
        let panel_app = axum::Router::new()
            .route("/panel", get(|| async { Redirect::permanent("/panel/") }))
            .nest("/panel/", panel::resolve_router())
            .route("/healthz", get(|| async { "ok" }))
            .layer(cors)
            .layer(TraceLayer::new_for_http());
        let panel_listener = tokio::net::TcpListener::bind(panel_addr)
            .await
            .with_context(|| format!("bind panel {}", panel_addr))?;
        info!(%panel_addr, "panel listen");
        tokio::spawn(async move {
            let _ = axum::serve(panel_listener, panel_app).await;
        });
    }

    startup_banner::print(&settings, storage);
    if postgres {
        info!("cloud auth enabled (postgres)");
    }

    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
        info!("shutdown signal received, draining HTTP connections");
    };
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .context("HTTP server exited with error")?;

    Ok(())
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

fn ensure_setup_token(data_dir: &std::path::Path) {
    if std::env::var("BETTERMQ_SETUP_TOKEN")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .is_some()
    {
        info!("local auth not configured — use BETTERMQ_SETUP_TOKEN for /panel setup");
        return;
    }
    if matches!(
        std::env::var("BETTERMQ_ALLOW_OPEN_SETUP")
            .ok()
            .as_deref()
            .map(str::trim),
        Some("1") | Some("true") | Some("TRUE") | Some("yes")
    ) {
        info!("local auth not configured — open setup allowed (BETTERMQ_ALLOW_OPEN_SETUP)");
        return;
    }
    let token = uuid::Uuid::new_v4().to_string();
    unsafe { std::env::set_var("BETTERMQ_SETUP_TOKEN", &token) };
    let token_path = data_dir.join("setup-token.txt");
    if let Err(e) = std::fs::write(&token_path, format!("{token}\n")) {
        tracing::warn!(error = %e, "could not write setup-token.txt");
    } else {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&token_path, std::fs::Permissions::from_mode(0o600));
        }
    }
    info!(
        token_file = %token_path.display(),
        "local auth not configured — setup token written"
    );
    eprintln!("BetterMQ setup token: {token}");
    eprintln!("  header: x-bettermq-setup-token: {token}");
    eprintln!("  or set BETTERMQ_ALLOW_OPEN_SETUP=1 for open local setup");
}

fn spawn_dispatch_backfill_loop(state: Arc<AppState>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        interval.tick().await; // skip immediate tick (boot already backfills)
        loop {
            interval.tick().await;
            state.dispatch.backfill_pending();
        }
    });
}

fn spawn_schedule_worker(state: Arc<AppState>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(200));
        loop {
            interval.tick().await;
            let scheduler_leader = match &state.cluster {
                None => true,
                Some(c) => {
                    let _ = c.runtime.try_acquire_scheduler_leader(5_000);
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
    });
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

fn spawn_fleet_workers(state: Arc<AppState>) {
    let Some(client) = broker_dispatch::LeaseClient::from_env() else {
        tracing::error!(
            "dispatch fleet requires BETTERMQ_BROKER_URLS (comma-separated broker base URLs)"
        );
        return;
    };
    let concurrency = broker_dispatch::fleet_concurrency();
    info!(
        holder = %client.holder(),
        brokers = ?client.broker_urls(),
        concurrency,
        "starting dispatch fleet workers"
    );
    let sem = Arc::new(tokio::sync::Semaphore::new(concurrency));
    tokio::spawn(async move {
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
                            .fail(&broker, job.lease_id, "missing message envelope", true)
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
                    match dispatch.push_http_only(&msg).await {
                        Ok(()) => {
                            heartbeat.abort();
                            if let Err(e) = client.complete(&broker, job.lease_id).await {
                                tracing::warn!(error = %e, "fleet complete failed");
                            }
                        }
                        Err(e) => {
                            heartbeat.abort();
                            let dead = matches!(&e, broker_dispatch::DispatchError::NoDestination)
                                || matches!(
                                    &e,
                                    broker_dispatch::DispatchError::Failed(s) if s.starts_with("egress")
                                );
                            let _ = client
                                .fail(&broker, job.lease_id, &e.to_string(), dead)
                                .await;
                        }
                    }
                }
            });
        }
    });
}
