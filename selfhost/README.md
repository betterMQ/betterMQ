# BetterMQ self-host

Free self-hosted BetterMQ. **No config files** — run Docker and use the panel.

Shared engine code lives in [`../engine/`](../engine/). This folder is the self-host product (Docker + docs only).

## Quick start (local disk)

```bash
cd selfhost
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

## Run without Docker

```bash
bettermq config init --template local
bettermq serve --data-dir ./data
```

Panel still writes `./data/bettermq.json`.
