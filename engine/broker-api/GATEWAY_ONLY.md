# Gateway-only mode

Gateway-only mode runs the public high-throughput ingest routes without opening
Broker, WAL, RocksDB, schedules, dispatch workers, or the archive service.

```bash
BETTERMQ_BROKER_URLS=http://broker-1:8080,http://broker-2:8080 \
BETTERMQ_GATEWAY_TOKEN=replace-me \
BETTERMQ_CLUSTER_SECRET=shared-internal-secret \
bettermq serve --gateway-only --listen 0.0.0.0:8080
```

Routing defaults to layout V2 with 256 physical shards. Override this only to
match an existing cell:

- `BETTERMQ_GATEWAY_LAYOUT_VERSION=1|2`
- `BETTERMQ_GATEWAY_SHARD_COUNT=<count>`
- `BETTERMQ_GATEWAY_TENANT=default` for self-hosted routing

The gateway keeps pooled connections and learns authoritative leader addresses
and terms from stale-leader responses. `/readyz` requires configured auth and
at least one healthy broker. `/v1/gateway/status` reports routing readiness.

Supported ingest routes:

- `POST /v1/gateway/enqueue`
- `POST /v1/ingest/batch`
- `POST /v1/ingest/ndjson`

NDJSON is parsed incrementally with decoded-byte and record limits. Compressed
NDJSON is rejected so limits cannot be bypassed through decompression.
