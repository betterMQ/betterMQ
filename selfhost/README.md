# BetterMQ self-host

Free self-hosted BetterMQ. **No config files required** — install the CLI or run Docker, then use the panel.

Shared engine code lives in [`../engine/`](../engine/). This folder is the self-host product (Docker + docs only).

## Install CLI (recommended)

Downloads the latest release binary from GitHub — no Rust toolchain or Docker required.

### macOS, Linux, WSL, Git Bash (Windows)

```bash
curl -fsSL https://bettermq.com/install | bash
```

Installs to `~/.bettermq/bin` and links `bettermq` into `~/.local/bin` (add that directory to your `PATH` if prompted).

Then start the server:

```bash
bettermq serve
open http://localhost:8080/panel/    # macOS
# xdg-open http://localhost:8080/panel/   # Linux
```

The installer can offer to run `bettermq serve` immediately when run in an interactive terminal.

### Windows (PowerShell or CMD)

Stock Windows does not ship with `bash`. Use the PowerShell installer instead:

```powershell
powershell -ExecutionPolicy Bypass -c "irm https://bettermq.com/install.ps1 | iex"
```

Then open a **new** terminal and run:

```powershell
bettermq serve
```

Panel: http://localhost:8080/panel/

### Options

| Variable / arg | Effect |
|----------------|--------|
| `BETTERMQ_FORCE=1` | Reinstall even if the same version is already present |
| `BETTERMQ_NO_START=1` | Install only; do not prompt to start the server |
| `BETTERMQ_INSTALL_DIR` | Base install dir (default `~/.bettermq`) |
| `BETTERMQ_BIN_DIR` | Directory for the `bettermq` symlink (default `~/.local/bin`) |
| `BETTERMQ_VERSION` | Windows installer: pin a release (e.g. `0.3.1`). Unix: pass the version as the first argument instead |
| First argument `0.3.1` | Unix installer: install a specific version instead of latest |

### First-time setup

Open **http://localhost:8080/panel/** and set a panel password. Copy the `sk_local_…` API token shown once.

Setup is open for **15 minutes** after the process starts. If the window closes, restart the process. `BETTERMQ_ALLOW_OPEN_SETUP=1` keeps it open until a password is set. `BETTERMQ_SETUP_WINDOW_SECS` changes the duration (`0` locks setup).

Data (queues, WAL, schedules) is stored under `./data` in the current directory unless you pass `--data-dir`.

```bash
bettermq serve --data-dir /var/lib/bettermq
```

---

## CLI reference

Verified from `bettermq --help` / `bettermq <cmd> --help` on the self-host binary. For the live list on your install: `bettermq --help`.

### Commands

| Command | Purpose |
|---------|---------|
| `bettermq serve` | Run the HTTP broker (data plane) |
| `bettermq panel` | Standalone management gateway + panel (no broker storage) |
| `bettermq config init` | Write a starter `bettermq.json` |
| `bettermq config validate` | Validate a `bettermq.json` |
| `bettermq config schema` | Print JSON Schema for editors |
| `bettermq doctor` | Check config compatibility, WAL frames, and data-dir access |
| `bettermq support-bundle` | Write a redacted JSON diagnostic bundle |
| `bettermq cluster init` | Legacy: initialize cluster files in the data dir |
| `bettermq cluster join` | Legacy: join via seed URL |

Prefer creating/joining a cluster from the **panel → Infrastructure**. `cluster` subcommands remain for scripting.

### `bettermq serve`

```text
bettermq serve [OPTIONS]
```

| Flag | Env | Meaning |
|------|-----|---------|
| `-c`, `--config <PATH>` | `BETTERMQ_CONFIG` | Path to `bettermq.json` |
| `--listen <HOST:PORT>` | `BETTERMQ_LISTEN` | Listen address (overrides `-p` and config) |
| `-p`, `--port <PORT>` | `BETTERMQ_PORT` | Bind `0.0.0.0:{port}` (default 8080 if unset) |
| `--data-dir <DIR>` | `BETTERMQ_DATA_DIR` | Persistent data (WAL, RocksDB, auth, managed config) |
| `--cluster <true\|false>` | `BETTERMQ_CLUSTER` | Enable/disable cluster mode (overrides config) |
| `--broker-only` | `BETTERMQ_BROKER_ONLY` | Compatibility alias for `--profile broker` |
| `--gateway-only` | `BETTERMQ_GATEWAY_ONLY` | Compatibility alias for `--profile gateway` (no WAL) |
| `--dispatch-fleet` | `BETTERMQ_DISPATCH_FLEET` | Compatibility alias for `--profile dispatch` |
| `--profile <NAME>` | `BETTERMQ_PROFILE` | `all` (default), `broker`, `dispatch`, `gateway`, `controller`, `panel` |
| `--components <A,B>` | `BETTERMQ_COMPONENTS` | Extra components to enable (comma-separated) |
| `--no-panel` | `BETTERMQ_NO_PANEL` | Disable the embedded panel |
| `--admin-listen <HOST:PORT>` | `BETTERMQ_ADMIN_LISTEN` | Private admin + panel listener |
| `--internal-listen <HOST:PORT>` | `BETTERMQ_INTERNAL_LISTEN` | Replication/controller listener |
| `--panel-listen <HOST:PORT>` | `BETTERMQ_PANEL_LISTEN` | Alias: separate panel+admin listener |

Examples:

```bash
bettermq serve
bettermq serve -p 9000
bettermq serve --listen 0.0.0.0:8080 --panel-listen 127.0.0.1:8090
bettermq serve --data-dir /var/lib/bettermq --broker-only
bettermq serve --profile gateway
bettermq panel --listen 127.0.0.1:8090 --controller http://broker1:8080
BETTERMQ_BROKER_URLS=http://broker1:8080,http://broker2:8080 bettermq serve --dispatch-fleet
```

Multi-node cells use **fixed RF=3 / minISR=2**. Additional nodes host other shards rather than extra copies of every message. ACKs never cross regions; a global `bettermq panel` federates cells.

### `bettermq config`

```bash
bettermq config init [-o bettermq.json] [--template local|slate|cluster]
bettermq config validate [-c bettermq.json]
bettermq config schema
```

| `--template` | Meaning |
|--------------|---------|
| `local` (default) | Single node, local WAL + RocksDB |
| `slate` | Single node, SlateDB + S3-compatible store |
| `cluster` | Three-node HA cluster (local storage) template |

### `bettermq cluster` (legacy)

```bash
bettermq cluster init --addr <URL> [--data-dir ./data] [--peers <URL,URL,...>] [--node-id <UUID>]
bettermq cluster join --addr <URL> --seed <URL> [--data-dir ./data] [--node-id <UUID>]
```

- `--addr` — this node’s public HTTP base URL (e.g. `http://broker1:8080`)
- `--peers` — comma-separated peer base URLs (including this node) for `init`
- `--seed` — seed broker URL for `join`

### `bettermq panel`

Standalone management gateway (no broker storage). Same `--config` / `--listen` as `serve`, plus:

| Flag | Env | Meaning |
|------|-----|---------|
| `--controller <URL>` | `BETTERMQ_CONTROLLER` | Broker/controller base URL this panel talks to |
| `--panel-data-dir <DIR>` | `BETTERMQ_PANEL_DATA_DIR` | Panel-only data directory |

### Related environment variables

The public repo reads **92** `BETTERMQ_*` names used by self-host. **16** are CLI flags for `serve`/`panel` (tables above). **5** are installer-only (`BETTERMQ_FORCE`, `BETTERMQ_NO_START`, `BETTERMQ_INSTALL_DIR`, `BETTERMQ_BIN_DIR`, `BETTERMQ_VERSION`). The rest are process env (some also written from `bettermq.json` at startup).

#### Auth, setup, metrics

| Env | Used for |
|-----|----------|
| `BETTERMQ_ALLOW_OPEN_SETUP` | Keep setup open until a password is set (no time window) |
| `BETTERMQ_SETUP_WINDOW_SECS` | First-boot open-setup duration (default `900`; `0` locks until restart or allow-open) |
| `BETTERMQ_INSECURE_NO_AUTH` | Skip auth (`1`/`true`/`yes`). Local development only |
| `BETTERMQ_LOCAL_AUTH_FILE` | Path to local auth file (also set from config) |
| `BETTERMQ_ALLOW_QUERY_SECRET` | Deprecated: accept GET publish `?secret=` (`1`) |
| `BETTERMQ_METRICS_TOKEN` | Bearer / header gate for `/metrics` (required in HA compose files) |
| `BETTERMQ_JOIN_TOKEN_QUERY` | Allow join token in query string (`1`/`true`/`yes`) |
| `BETTERMQ_TRUST_PROXY` | Trust `X-Forwarded-For` for rate-limit client IP |

#### Cluster, fleet, identity

| Env | Used for |
|-----|----------|
| `BETTERMQ_CLUSTER_SECRET` | Internal `/internal/v1/*` (replication, lease, catalog) |
| `BETTERMQ_SHARED_META_DIR` | Local-WAL HA only: one shared filesystem for leases, cursors, catalogs, host breaker. Each node still has its own `--data-dir`. Skip this when nodes have no shared disk — use Slate + S3 instead |
| `BETTERMQ_BROKER_URLS` | Fleet/gateway: comma-separated broker base URLs |
| `BETTERMQ_FLEET_CONCURRENCY` | Fleet in-flight jobs (default `1`) |
| `BETTERMQ_FLEET_HOLDER` | Optional fleet worker id |
| `BETTERMQ_LONG_WAIT_TIER` | Fleet long HTTP wait: `15m` \| `1h` \| `6h` \| `12h` |
| `BETTERMQ_NODE_NAME` | Node display name (also set from config) |
| `BETTERMQ_NODE_PUBLIC_URL` | Node public HTTP URL (also set from config) |
| `BETTERMQ_CELL_LABEL` | Cell label in panel bootstrap and admin (default `local`) |
| `BETTERMQ_CELL_REGION` | Cell region in admin (default `local`) |

#### Ingest, HTTP, payloads

| Env | Used for |
|-----|----------|
| `BETTERMQ_INGEST_MAX_IN_FLIGHT` | Process in-flight ingest records (default `4096`) |
| `BETTERMQ_INGEST_MAX_QUEUED_BYTES` | Process queued-byte cap (default 64 MiB) |
| `BETTERMQ_INGEST_MAX_TENANT_RECORDS` | Per-tenant queued-record cap; excess gets `429` |
| `BETTERMQ_INGEST_MAX_TENANT_BYTES` | Per-tenant queued-byte cap; excess gets `429` |
| `BETTERMQ_MAX_HTTP_BODY_BYTES` | HTTP body cap (default 64 MiB) |
| `BETTERMQ_INLINE_MAX_BYTES` | Inline payload threshold before blob store (default 256 KiB) |
| `BETTERMQ_MAX_PAYLOAD_BLOB_BYTES` | Hard cap for blob payloads (default 256 MiB) |
| `BETTERMQ_CORS_ORIGINS` | CORS allowlist (`*` or comma-separated origins) |
| `BETTERMQ_SHUTDOWN_TIMEOUT_SECS` | HTTP/WAL graceful drain bound (default 30 seconds) |

#### Dispatch / delivery

| Env | Used for |
|-----|----------|
| `BETTERMQ_ALLOW_PRIVATE_DESTINATIONS` | Allow loopback/LAN webhook destinations |
| `BETTERMQ_DISPATCH_MAX_IN_FLIGHT` | Concurrent delivery tasks (default `256`) |
| `BETTERMQ_DISPATCH_QUEUE_CAP` | Dispatch work-queue capacity (default `4096`) |
| `BETTERMQ_DISPATCH_GLOBAL_MAX` | Optional global in-flight delivery cap |
| `BETTERMQ_DEFAULT_MAX_RETRIES` | Default webhook retries (default `3`) |
| `BETTERMQ_HTTP_TIMEOUT_SECS` | Webhook HTTP timeout (default `30`; also set from config) |
| `BETTERMQ_LONG_HTTP_TIMEOUT_SECS` | Long-payload webhook timeout (default `7200`; also set from config) |
| `BETTERMQ_FLOW_WAITLIST_CAP` | Per-lane flow waitlist (default `8192`) |
| `BETTERMQ_MEMORY_LIMIT_MB` | Pause dispatch fetches above this RSS (optional) |
| `BETTERMQ_FAIRNESS_TENANT_SOFT_LIMIT` | Tenant fairness tracking soft cap (default `1024`) |

#### DLQ and group fan-out

Fan-out vars are **group bookkeeping**, not DLQ TTL.

| Env | Used for |
|-----|----------|
| `BETTERMQ_DLQ_RETENTION_DAYS` | Auto-delete DLQ messages older than N days. Unset, `0`, or `never` keeps them forever (default) |
| `BETTERMQ_DLQ_RETENTION_INTERVAL_MS` | DLQ sweeper interval (default `900000` = 15m; clamp 5s–1d) |
| `BETTERMQ_FANOUT_MAX_ATTEMPTS` | Group fan-out delivery attempts (default `20`) |
| `BETTERMQ_FANOUT_REPLAY_INTERVAL_MS` | Fan-out replay tick (default `2000`; clamp 100–60000) |
| `BETTERMQ_FANOUT_RETENTION_MS` | Completed fan-out retention (default 24h) |
| `BETTERMQ_FANOUT_MAX_COMPLETED` | Completed fan-out entries kept (default `10000`) |

#### Storage, WAL, archive, replication

| Env | Used for |
|-----|----------|
| `BETTERMQ_STORAGE` | `local` or `slate` (also set from config) |
| `BETTERMQ_FSYNC` | WAL durability: `group` (default, shard commit epoch), `always` (every message), `os` (no explicit fsync) |
| `BETTERMQ_FSYNC_INTERVAL_MS` | Background group-commit timer in milliseconds (default `10`, clamp 1–60000) |
| `BETTERMQ_COMMIT_LINGER_MS` | Ingest waiter linger before shard fsync (default `1`, max 20) |
| `BETTERMQ_SHARD_LAYOUT` | New cell only: `v1` (default) or `v2` |
| `BETTERMQ_SHARD_COUNT` | New v2 cells: physical shard count (default 256). Never remaps an existing `shard-layout.json` |
| `BETTERMQ_SHARD_QUEUE_CAPACITY` | Per-shard I/O queue (default `1024`, clamp 1–65536) |
| `BETTERMQ_SEGMENT_READER_CACHE` | Open WAL segment file cache (default `64`, clamp 1–4096) |
| `BETTERMQ_METADATA_RECOVER` | `empty` allows replacing corrupt JSON meta with defaults |
| `BETTERMQ_MAX_REPLICATION_EPOCH_BYTES` | Replication epoch size (default 16 MiB) |
| `BETTERMQ_MAX_REPLICATION_IN_FLIGHT_BYTES` | Replication in-flight bytes (default 64 MiB) |
| `BETTERMQ_ARCHIVE_DIR` | Directory for async copies of sealed WAL segments |
| `BETTERMQ_ARCHIVE_QUEUE_CAPACITY` | Bounded sealed-segment archive queue (default 128) |
| `BETTERMQ_ARCHIVE_BUCKET` | S3/R2 bucket for sealed-segment archive (aliases `S3_ARCHIVE_BUCKET` / `R2_ARCHIVE_BUCKET`) |
| `BETTERMQ_ARCHIVE_PREFIX` | Object-key prefix inside the archive bucket |
| `BETTERMQ_ARCHIVE_MAX_RETRIES` | Archive upload retries (default `5`) |
| `BETTERMQ_ARCHIVE_MAX_QUEUED_BYTES` | Archive queue byte cap (default 4 GiB) |
| `BETTERMQ_ARCHIVE_MAX_LAG_MS` | Archive lag before health degrades (default 5 minutes) |

S3/R2 credentials for Slate and archive are **not** `BETTERMQ_*`: `S3_ENDPOINT` or `R2_ENDPOINT`, plus `S3_ACCESS_KEY` / `R2_ACCESS_KEY` / `AWS_ACCESS_KEY_ID`, matching secret and region vars, and `S3_BUCKET` / `R2_BUCKET` (payload blobs: `S3_PAYLOAD_BUCKET` / `R2_PAYLOAD_BUCKET`). See [`engine/broker-storage/ARCHIVE.md`](../engine/broker-storage/ARCHIVE.md).

#### Gateway-only profile

| Env | Used for |
|-----|----------|
| `BETTERMQ_GATEWAY_SHARD_COUNT` | Gateway shard count (default 256) |
| `BETTERMQ_GATEWAY_LAYOUT_VERSION` | Gateway layout version (default `2`) |
| `BETTERMQ_GATEWAY_TENANT` | Tenant id when forwarding (default `default`) |
| `BETTERMQ_GATEWAY_TOKEN` | Token the gateway presents to brokers |
| `BETTERMQ_GATEWAY_IDLE_CONNECTIONS_PER_HOST` | HTTP pool idle per host (default `16`) |

#### Panel bootstrap

| Env | Used for |
|-----|----------|
| `BETTERMQ_PANEL_DIR` | Serve panel assets from a directory instead of embedded (`engine/panel/dist` after `npm run build`) |
| `BETTERMQ_PANEL_MODE` | `embedded` (default) or `standalone` (set automatically by `bettermq panel`) |
| `BETTERMQ_ADMIN_API_BASE` | Admin API prefix injected into the panel (default `/admin/v1`) |
| `BETTERMQ_PANEL_FLAGS` | Comma-separated feature flags injected into the panel |

#### Tools

| Env | Used for |
|-----|----------|
| `BETTERMQ_TOKEN` | Bearer token for `tools/bettermq-loadgen` |

Operational runbooks: [backup/restore and migration](BACKUP.md) and
[production launch/soak gates](PRODUCTION.md). Compose health checks use the
deep `/readyz` endpoint; `/healthz` is liveness only.

---

## Deploy on Railway

One-click deploy with HTTPS and a persistent `/data` volume:

[![Deploy on Railway](https://railway.com/button.svg)](https://railway.com/deploy/bettermq?referralCode=O5l32o&utm_medium=integration&utm_source=template&utm_campaign=generic)

Panel: `https://<your-railway-domain>/panel/`

---

## Quick start (Docker)

Published image (recommended — no Rust build):

```bash
docker pull ghcr.io/bettermq/bettermq:latest
docker run -d --name bettermq -p 8080:8080 -v bettermq-data:/data \
  ghcr.io/bettermq/bettermq:latest serve --data-dir /data
open http://localhost:8080/panel/
```

Or Compose (pulls the same image; use `--build` only if you want to compile locally):

```bash
git clone https://github.com/betterMQ/betterMQ.git
cd betterMQ/selfhost
docker compose up -d
open http://localhost:8080/panel/
```

| Image | Notes |
|-------|--------|
| `ghcr.io/bettermq/bettermq:latest` | Latest release |
| `ghcr.io/bettermq/bettermq:0.4.0` | Pin a version |

1. Set a **panel password** and copy your API token.
2. **Infrastructure** — storage (local or Slate + S3), optional cluster.
3. **Create cluster** on the first broker, then **Add node** from that same panel (or **Join cluster** on the new broker’s panel).

Settings persist in the Docker volume at `/data/bettermq.json`.

## Compose files

| File | Use case |
|------|----------|
| `docker-compose.yml` | Solo — local WAL |
| `docker-compose.slate.yml` | Solo — Slate + MinIO (dev) |
| `docker-compose.ha.yml` | HA LAN — 3 brokers + shared meta volume |
| `docker-compose.ha-slate.yml` | HA multi-VPS — 3 brokers + MinIO/S3 |
| `docker-compose.ha-fleet.yml` | HA + `--broker-only` + `--dispatch-fleet` (scale fleet) |
| `docker-compose.ha-slate-fleet.yml` | Slate HA + fleet |

### Scale path

| Stage | Command |
|-------|---------|
| Solo | `docker compose up -d` |
| HA LAN | `BETTERMQ_CLUSTER_SECRET=… docker compose -f docker-compose.ha.yml up -d` |
| HA + Slate | `… -f docker-compose.ha-slate.yml up -d` |
| Egress fleet | `… -f docker-compose.ha-fleet.yml up -d --scale fleet=3` |

Roles (one binary):

- `bettermq serve` — broker + local dispatch
- `bettermq serve --broker-only` — ingest + lease API (no local push workers)
- `bettermq serve --dispatch-fleet` — claim/push via `BETTERMQ_BROKER_URLS`

### Panel on its own port

By default the panel is on the same listen address as the API (`http://localhost:8080/panel/`).

To bind the panel on a **separate** address (e.g. loopback-only while the API stays on `0.0.0.0:8080`):

```bash
bettermq serve --listen 0.0.0.0:8080 --panel-listen 127.0.0.1:8090
# or
BETTERMQ_PANEL_LISTEN=127.0.0.1:8090 bettermq serve
```

Then open **http://127.0.0.1:8090/panel/**. The API remains at `http://<host>:8080`.

Useful when you expose the API publicly but keep the panel on localhost / VPN only. Day-to-day ops use the seed (or standalone `bettermq panel`).

Fleet long waits (fleet workers only): `BETTERMQ_LONG_WAIT_TIER=15m|1h|6h|12h`.

### Slate + MinIO

```bash
docker compose -f docker-compose.slate.yml up -d --build
```

In **Infrastructure → Storage**, choose SlateDB and set:

- Endpoint: `http://minio:9000`
- Buckets: `bettermq`, `bettermq-payloads` (payload bucket is **required**)
- Access key / secret: set `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` (no compose defaults)

**Note:** the MinIO in `docker-compose.slate.yml` is **one node**. Good for local/dev. Not real object-store HA. For production HA use a durable S3-compatible store (or multi-node MinIO) plus 3 brokers.

Any S3-compatible endpoint works the same way in the panel (endpoint, buckets, access key / secret).

### Multi-node HA (same cell)

Prefer compose profiles above, or run **one broker per server**. Two `all` nodes are a valid cluster but **not** failure-tolerant (RF=2 / minISR=2 — both must be up). A third broker is HA (RF=3 / minISR=2). A fourth adds shard capacity; RF stays 3.

**From one panel:**

1. Open broker 1 → **Infrastructure** → **Create cluster**. Copy the join token.
2. Start broker 2 on another port and data dir, e.g. `bettermq serve --listen 127.0.0.1:8082 --data-dir ./data2`.
3. On broker 1, **Add node**, paste **This panel connects here**. Two local processes: `http://127.0.0.1:8082`. Panel on the laptop and brokers in Docker: that published port, then tick **Other brokers cannot use that URL** and enter `http://broker2:8080`.
4. Restart every cluster node when the join response says so.
5. Later add broker 3 the same way for HA. Optional 4th = capacity, not RF=4.
6. Dispatch / gateway / panel: same form, other profile. They are **not** replicas.

`docker-compose.ha.yml` maps broker1→8081, broker2→8082, broker3→8083. Use the URL this panel process can call (compose DNS if the panel is in the compose network; `127.0.0.1` plus the published port if the panel is on the host).

Standalone panel: `docker compose -f docker-compose.panel.yml up -d` then register each cell’s admin URL.

Local WAL HA still requires `BETTERMQ_SHARED_META_DIR` (shared volume/NFS). Separate servers without shared FS → Slate + S3.

See [BACKUP.md](BACKUP.md) for backup/restore and host-breaker cold-start notes.

### Honesty

| Topic | Reality |
|-------|---------|
| Delivery | **At-least-once** — use message / idempotency keys in handlers |
| HA model | Fenced shard leaders + quorum/Slate + shared meta — **not** full Raft |
| MinIO single-node | Dev only; not object-store HA |
| Host breaker | Shared via `BETTERMQ_SHARED_META_DIR/host_breaker.json` when set; otherwise in-memory (lost on restart) |
| Fleet | Not in `cluster.nodes`; claims via internal lease API |

## Build from source

If you prefer to compile yourself (or no prebuilt binary exists for your platform):

```bash
git clone https://github.com/betterMQ/betterMQ.git
cd BetterMQ
cargo build --release -p broker-server
./target/release/bettermq serve
```

Panel still writes `./data/bettermq.json` when using `--data-dir ./data`.

## Contributions

Pull requests are disabled. Coding agents make it too easy to send a large, low-context change that costs maintainers more time than it saves.
