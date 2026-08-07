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
| First argument `0.3.1` | Install a specific version instead of latest |

### First-time setup

1. Open **http://localhost:8080/panel/**
2. Set a **panel password** and copy your `sk_local_…` API token.
3. Create queues and test enqueue from the panel or curl.

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
| `bettermq config init` | Write a starter `bettermq.json` |
| `bettermq config validate` | Validate a `bettermq.json` |
| `bettermq config schema` | Print JSON Schema for editors |
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
| `--broker-only` | `BETTERMQ_BROKER_ONLY` | Ingest + lease API; no local delivery workers |
| `--dispatch-fleet` | `BETTERMQ_DISPATCH_FLEET` | Fleet worker: claim/push (needs `BETTERMQ_BROKER_URLS`); no ingest |
| `--panel-listen <HOST:PORT>` | `BETTERMQ_PANEL_LISTEN` | Second bind for the embedded panel only |

Examples:

```bash
bettermq serve
bettermq serve -p 9000
bettermq serve --listen 0.0.0.0:8080 --panel-listen 127.0.0.1:8090
bettermq serve --data-dir /var/lib/bettermq --broker-only
BETTERMQ_BROKER_URLS=http://broker1:8080,http://broker2:8080 bettermq serve --dispatch-fleet
```

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

### Related environment variables

Not CLI flags, but used by `serve` / fleet / HA (read from process env):

| Env | Used for |
|-----|----------|
| `BETTERMQ_CLUSTER_SECRET` | Internal `/internal/v1/*` (replication, lease, catalog) |
| `BETTERMQ_SHARED_META_DIR` | Shared meta for HA (cursors, fencing, host breaker file, …) |
| `BETTERMQ_BROKER_URLS` | Fleet: comma-separated broker base URLs to claim from |
| `BETTERMQ_FLEET_CONCURRENCY` | Fleet in-flight jobs (default `1`) |
| `BETTERMQ_FLEET_HOLDER` | Optional fleet worker id |
| `BETTERMQ_LONG_WAIT_TIER` | Fleet long HTTP wait: `15m` \| `1h` \| `6h` \| `12h` |
| `BETTERMQ_SETUP_TOKEN` | First-time panel setup when auth is not configured |
| `BETTERMQ_ALLOW_OPEN_SETUP` | Allow open local setup without a setup token |
| `BETTERMQ_ALLOW_PRIVATE_DESTINATIONS` | Allow loopback/LAN webhook destinations |
| `BETTERMQ_METRICS_TOKEN` | Optional Bearer / header gate for `/metrics` |
| `BETTERMQ_CORS_ORIGINS` | CORS allowlist (`*` or comma-separated origins) |
| `BETTERMQ_PANEL_DIR` | Serve panel assets from a directory instead of embedded |

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
3. **Create cluster** on the first broker, **Join cluster** on each additional broker (join token from seed).

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

Useful when you expose the API publicly but keep the panel on localhost / VPN only. Every broker can take `--panel-listen`; for day-to-day ops use the seed node’s panel, and open a new node’s panel (SSH/VPN) when joining the cluster.

Fleet long waits (fleet workers only): `BETTERMQ_LONG_WAIT_TIER=15m|1h|6h|12h`.

### Slate + MinIO

```bash
docker compose -f docker-compose.slate.yml up -d --build
```

In **Infrastructure → Storage**, choose SlateDB and set:

- Endpoint: `http://minio:9000`
- Buckets: `bettermq`, `bettermq-payloads` (payload bucket is **required**)
- Access key / secret: `minio` / `minio12345`

**Note:** the MinIO in `docker-compose.slate.yml` is **one node**. Good for local/dev. Not real object-store HA. For production HA use a durable S3-compatible store (or multi-node MinIO) plus 3 brokers.

Any S3-compatible endpoint works the same way in the panel (endpoint, buckets, access key / secret).

### Multi-node HA

Prefer compose profiles above, or run **one broker per server**:

1. **Broker 1** — set public URL, **Create cluster**, copy join token.
2. **Broker 2+** — set public URL, **Join cluster** (seed URL + token), restart.
3. Local WAL HA requires `BETTERMQ_SHARED_META_DIR` (shared volume/NFS). Separate servers without shared FS → Slate + S3.

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
