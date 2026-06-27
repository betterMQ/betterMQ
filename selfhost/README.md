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

## Quick start (Docker)

```bash
git clone https://github.com/betterMQ/betterMQ.git
cd BetterMQ/selfhost
docker compose up -d --build
open http://localhost:8080/panel/
```

1. Set a **panel password** and copy your API token.
2. **Infrastructure** — storage (local or Slate + S3), optional cluster.
3. **Create cluster** on the first broker, **Join cluster** on each additional broker (join token from seed).

Settings persist in the Docker volume at `/data/bettermq.json`.

## Compose files

| File | Use case |
|------|----------|
| `docker-compose.yml` | Single node, local WAL + RocksDB |
| `docker-compose.slate.yml` | Single node + MinIO (configure Slate in panel) |

### Slate + MinIO

```bash
docker compose -f docker-compose.slate.yml up -d --build
```

In **Infrastructure → Storage**, choose SlateDB and set:

- Endpoint: `http://minio:9000`
- Buckets: `bettermq`, `bettermq-payloads`
- Access key / secret: `minio` / `minio12345`

For **Cloudflare R2**, skip MinIO and use your R2 endpoint and credentials in the panel.

### Multi-node HA (panel-managed)

Run **one broker per server** (same `docker compose up` on each). No special cluster compose file.

1. **Broker 1** — set public URL, **Create cluster**, copy join token.
2. **Broker 2+** — set public URL, **Join cluster** (seed URL + token), restart.
3. On all nodes — **Sync from seed**, restart when membership changes.

Use URLs each broker can reach (not `localhost` from other containers). On one Mac with multiple containers, use `http://host.docker.internal:8080` as the seed URL.

Optional production: shared metadata volume (`BETTERMQ_SHARED_META_DIR`) for crons/queues across nodes.

**Failover:** survives one broker loss; not partition-safe (at-least-once delivery).

## Build from source

If you prefer to compile yourself (or no prebuilt binary exists for your platform):

```bash
git clone https://github.com/betterMQ/betterMQ.git
cd BetterMQ
cargo build --release -p broker-server
./target/release/bettermq serve
```

Panel still writes `./data/bettermq.json` when using `--data-dir ./data`.
