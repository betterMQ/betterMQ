# betterMQ

**Self-hosted HTTP message broker** — enqueue durable jobs, deliver them with signed webhook push. No workers to poll; your app receives HTTP callbacks.

<img width="1200" height="630" alt="betterMQ — self-hosted HTTP message broker" src="./docs/assets/gh-banner.png" />

[betterMQ.com](https://betterMQ.com) · [Interactive API docs](https://github.com/betterMQ/betterMQ) (`/docs` when running) · [LLM docs](https://betterMQ.com/llms.txt) (full guide: `llm.txt` / `llms.txt`)

---

## What is betterMQ?

betterMQ is an **open-source, push-only message broker**. You send messages over HTTP; betterMQ stores them durably and **pushes** them to your webhook URLs (like a background job queue with HTTP delivery instead of pull-based workers).

Typical uses:

- **Async jobs** — enqueue work from your API, deliver to an internal webhook handler
- **Scheduled tasks** — cron or fixed-interval jobs into a queue
- **Delayed execution** — `delay` on publish or enqueue
- **Fan-out** — one event delivered to multiple destinations (groups)
- **Rate limiting** — per-key parallelism and delivery rate (flow control)

betterMQ is a push queue: messages leave after successful delivery or DLQ placement — not an append-only event log or a long-polling worker queue.

---

## How it works

```mermaid
flowchart LR
  Client[Your app / curl]
  API[betterMQ API]
  WAL[(Durable log)]
  Flow[Flow control]
  Webhook[Your HTTPS endpoint]

  Client -->|POST enqueue or publish| API
  API --> WAL
  WAL --> Flow
  Flow -->|HTTP push| Webhook
  Webhook -->|2xx| Flow
  Flow -->|fail + retries exhausted| DLQ[(DLQ topic)]
```

1. **Accept** — `POST /v1/enqueue` or `POST /v1/publish` returns `202` after the message is durably stored.
2. **Snapshot** — For queues, the destination URL and secret are frozen at enqueue time (later queue updates do not affect in-flight jobs).
3. **Flow control** — Optional rate limits and per-key parallelism before delivery starts.
4. **Push** — betterMQ sends your `body` to the destination (optional custom method, headers, HMAC signature).
5. **Retry** — Configurable retries with fixed or exponential backoff.
6. **DLQ** — After retries exhaust, a copy lands on `{queue}.__dlq` for inspection.

Delivery is **at-least-once**. Use `idempotency_key` on publish/enqueue to dedupe accepts.

### Honesty

| Topic | Reality |
|-------|---------|
| Delivery | At-least-once; handlers must be idempotent |
| HA | Fenced leaders + quorum/Slate + shared meta — not full Raft |
| MinIO single-node | Dev only |
| Roles | `serve` \| `--broker-only` \| `--dispatch-fleet` (same binary) |

See [`selfhost/README.md`](selfhost/README.md) for compose profiles (solo → HA → Slate → fleet), including **panel on its own port** (`--panel-listen` / `BETTERMQ_PANEL_LISTEN`).

---

## Features

### Messaging

- **Named queues** — register a queue name → http(s) destination URL + signing secret
- **Enqueue** — add jobs to a queue by `queue_id` (preferred) or name
- **1:1 publish** — one-off delivery to any URL without creating a queue
- **Groups (fan-out)** — `POST /v1/publish` with `group_id` delivers to every member webhook
- **Batch ingest** — `POST /v1/enqueue/batch`
- **Gateway enqueue** — same body as batch; for stateless edge gateways

### Scheduling

- **Delayed jobs** — `delay` (milliseconds) on publish or enqueue
- **Cron** — 5-field UTC cron expressions (`0 9 * * *`)
- **Intervals** — `every_seconds` recurring schedules
- **Pause / resume** — per cron schedule

### Flow control

Set rate and parallelism on publish — no need to pre-create a key.

- **Inline on publish** — `flowControl: { key, parallelism, rate, period }` (also `flow` / `period_secs`); reuses a matching profile or creates one
- **Flow profiles UI / API** — list, create, delete via `/v1/flows` (profiles appear after inline publish too)
- **`flow_id`** — optional; attach a pre-created profile instead of inline limits
- **Runtime admin** — pause, resume, pin, unpin, reset-rate on live lanes
- **Priority** — `0`–`9` (default `5`); with `parallelism: 1`, higher priority runs first

### Reliability

- **Retries** — per message, per queue, or broker default; fixed or exponential backoff
- **Dead letter queue (DLQ)** — `GET /v1/dlq?queue=…` after push failures
- **Idempotency keys** — safe retries from clients
- **Circuit breaker** — auto-block repeatedly failing destination hosts; manual unblock API
- **Durable storage** — local WAL or SlateDB + S3-compatible object storage (MinIO, R2)

### Delivery

- **Push-only** — no pull/worker API; your endpoint receives HTTP callbacks
- **curl-style outbound** — optional `method`, `headers`, raw `body`
- **Webhook signing** — `betterMQ-Signature` + `betterMQ-Timestamp` HMAC headers

### Operations

- **Embedded control panel** — `/panel/` (setup, queues, infra, docs)
- **OpenAPI 3.1 + Scalar UI** — `/docs`, `/openapi.json`
- **Health & metrics** — `/healthz`, `/readyz`, `/metrics`
- **Docker Compose** — single-node or Slate + MinIO
- **HA cluster** — multi-broker replication via panel-managed cluster (self-host)

### Self-host

- **No usage limits** — full throughput on your hardware
- **Local auth** — panel password + `sk_local_…` API token (no external account)
- **Single binary** — `betterMQ serve` (Rust)

---

## Quick start

### Install CLI (recommended)

```bash
curl -fsSL https://betterMQ.com/install | bash
betterMQ serve
open http://localhost:8080/panel/
```

**Windows (PowerShell):** `powershell -ExecutionPolicy Bypass -c "irm https://betterMQ.com/install.ps1 | iex"`

See [selfhost/README.md](selfhost/README.md) for options, **CLI reference**, Docker, and building from source.

### Deploy on Railway

One-click deploy (HTTPS, public URL, persistent `/data` volume):

[![Deploy on Railway](https://railway.com/button.svg)](https://railway.com/deploy/bettermq?referralCode=O5l32o&utm_medium=integration&utm_source=template&utm_campaign=generic)

After deploy, open `/panel/` on your Railway domain to set a password and copy your API token.

### Docker

Pull the published image (no build required):

```bash
docker pull ghcr.io/bettermq/bettermq:latest
docker run -d --name bettermq -p 8080:8080 -v bettermq-data:/data \
  ghcr.io/bettermq/bettermq:latest serve --data-dir /data
open http://localhost:8080/panel/
```

Or with Compose (pulls `ghcr.io/bettermq/bettermq:latest` by default):

```bash
git clone https://github.com/betterMQ/betterMQ.git
cd betterMQ/selfhost
docker compose up -d
open http://localhost:8080/panel/
```

1. Set a **panel password** and copy your `sk_local_…` API token.
2. Create queues and test enqueue from the panel or curl.
3. Optional: **Infrastructure** → storage (local / Slate) or **Create cluster** for HA.

See [selfhost/README.md](selfhost/README.md) for Slate + MinIO and multi-node setup.

### From source

```bash
cargo build --release -p broker-server
./target/release/betterMQ serve          # default port 8080
./target/release/betterMQ serve -p 9000  # custom port
```

Interactive API reference: `http://localhost:8080/docs`

---

## Authentication

| Context | Header |
|---------|--------|
| API requests | `Authorization: Bearer sk_local_…` |
| First-time setup | Panel at `/panel/` (no token yet) |

Generate or rotate the API token in **Panel → Settings** (panel password required).

Endpoints marked **Public** below do not require a Bearer token.

---

## Core concepts

| Term | Meaning |
|------|---------|
| **queue** | Named destination: `name` + http(s) URL + HMAC secret |
| **queue_id** | Stable UUID — preferred when enqueuing |
| **publish** | One-off job to a URL **or** fan-out to a `group_id` |
| **enqueue** | Job on a registered queue; destination URL snapshotted at accept time |
| **flow profile** | Rate / parallelism limits; referenced by `flow_id` or inline `flowControl` |
| **key** | Per-message identity; default flow-control grouping key |
| **delivery** | `{ "shard", "seq" }` — internal ack coordinates |
| **DLQ** | `{queue}.__dlq` — failed push copies (purge with `DELETE /v1/dlq`) |

**Retention:** primary queue records are removed after successful push or DLQ move. DLQ entries remain until purged (`DELETE /v1/dlq`) or drained.

**Egress:** destination URLs must be `http`/`https`. Loopback and private LAN hosts are blocked by default; set `betterMQ_ALLOW_PRIVATE_DESTINATIONS=1` for local webhooks. Link-local / instance-metadata hosts stay blocked.

---

## API endpoints

All paths are relative to your broker base URL (e.g. `http://localhost:8080`).  
Interactive reference: **`/docs`** (OpenAPI 3.1 + Scalar).

**Auth:** 🔓 no token · 🔑 `Authorization: Bearer sk_local_…`

| Method | Path | Auth |
|--------|------|------|
| GET | `/healthz`, `/readyz` | 🔓 |
| GET | `/metrics` | 🔓 (🔑 if `betterMQ_METRICS_TOKEN` is set) |
| GET | `/docs`, `/api-reference`, `/openapi.json` | 🔓 |
| GET | `/v1/auth/config`, `/v1/local-auth/status` | 🔓 |
| POST | `/v1/local-auth/setup`, `/v1/local-auth/regenerate` | 🔓 |
| GET | `/v1/infra/join/bootstrap` | 🔓 |
| POST | `/v1/infra/cluster/register` | 🔓 |
| All other `/v1/*` routes | 🔑 |

`/internal/v1/*` requires `betterMQ_CLUSTER_SECRET` (header `x-betterMQ-cluster-secret`). Requests without a matching secret return `401`.

Interactive reference when the broker is running: **`/docs`** (OpenAPI 3.1 + Scalar).

---

## API request & response reference

### Error responses

Most failures return JSON:

```json
{ "error": "queue not found: jobs" }
```

| Status | When |
|--------|------|
| `400` | Bad request / validation (incl. missing/invalid Bearer, unknown `group_id` on publish) |
| `401` | Metrics token or cluster secret mismatch |
| `404` | Queue, flow, cron, group, member, lane, or delayed job not found (route-dependent) |
| `409` | Duplicate group name |
| `503` | Replication / readiness failure |
| `500` | Internal / storage errors |

---

### `GET /healthz` · `GET /readyz` · `GET /metrics`

**`GET /healthz`** → `200`

```json
{ "status": "ok", "version": "0.4.0", "protocol": 1 }
```

**`GET /readyz`** → `200` when ready, `503` when not

```json
{ "ready": true, "cluster_healthy": true, "auth_configured": true }
```

**`GET /metrics`** → `200` (optional auth via `betterMQ_METRICS_TOKEN`)

```json
{
  "blocked_hosts": 0,
  "memory_critical": false,
  "cluster_enabled": false,
  "healthy_peers": 1
}
```

Optional fields when process sampling is available: `rss_mb`, `memory_limit_mb`, `memory_percent`, `cpu_percent`.

---

### Local auth (first boot)

**`POST /v1/local-auth/setup`**

```json
{ "password": "your-panel-password" }
```

→ `200`

```json
{ "token": "sk_local_…", "show_once": true }
```

**`GET /v1/auth/config`** → `200`

```json
{ "mode": "local", "configured": true }
```

---

### Queues

**`POST /v1/queues`**

```json
{
  "queue": "jobs",
  "url": "https://app.example/webhooks/jobs",
  "secret": "whsec_example",
  "max_retries": 3,
  "retry_backoff": {
    "kind": "exponential",
    "initialMs": 1000,
    "maxMs": 60000,
    "multiplier": 2
  }
}
```

→ `201`

```json
{
  "queue_id": "550e8400-e29b-41d4-a716-446655440000",
  "queue": "jobs",
  "url": "https://app.example/webhooks/jobs"
}
```

**`GET /v1/queues`** → `200`

```json
{
  "queues": [
    {
      "queue_id": "550e8400-e29b-41d4-a716-446655440000",
      "queue": "jobs",
      "url": "https://app.example/webhooks/jobs"
    }
  ]
}
```

**`DELETE /v1/queues/{queue_id}`** → `200` — returns the deleted queue record (same shape as create).

---

### Publish & enqueue

Shared accept shape for **`POST /v1/publish`**, **`POST /v1/enqueue`**, and **`POST /v1/queues/{queue_id}/enqueue`**.

**`POST /v1/publish`** — provide either `url` + `secret` (1:1) or `group_id` (fan-out), not both. `body` is required.

```json
{
  "url": "https://app.example/hooks/one-off",
  "secret": "whsec_example",
  "key": "user-42",
  "body": { "hello": "world" },
  "priority": 8,
  "flowControl": {
    "key": "user-42",
    "parallelism": 5,
    "rate": 10,
    "period": 60
  },
  "delay": 60000,
  "max_retries": 0,
  "retry_backoff": {
    "kind": "exponential",
    "initialMs": 1000,
    "maxMs": 60000,
    "multiplier": 2
  },
  "idempotency_key": "job-99",
  "method": "POST",
  "headers": { "Content-Type": "application/json" },
  "sign": true
}
```

`flowControl` (alias `flow`) ensures a flow profile by key: if one already exists with the same `parallelism` / `rate` / `period`, it is reused; otherwise it is created or updated. Profiles show up under **Flows** / `GET /v1/flows`. You can still pass a pre-created `flow_id` instead. **Inline flow / `flow_id` are for URL publish only** — not with `group_id` (members carry their own limits).

**`POST /v1/enqueue`**

```json
{
  "queue_id": "550e8400-e29b-41d4-a716-446655440000",
  "key": "user-42",
  "body": { "task": "send_invoice", "invoice_id": 99 },
  "priority": 8,
  "flowControl": {
    "key": "user-42",
    "parallelism": 1,
    "rate": 10,
    "period": 60
  },
  "delay": 60000,
  "idempotency_key": "inv-99",
  "sign": true
}
```

`body` accepts a JSON value or a string. Either `queue_id` or `queue` name is required for enqueue.

**Immediate accept** → `202`

```json
{
  "message_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "queue": "jobs",
  "duplicate": false,
  "delivery": { "shard": 0, "seq": 12 }
}
```

**Idempotent duplicate** → `200` (same body, `"duplicate": true`)

**Delayed** (`delay` set) → `202` — no `message_id` or `delivery`; `scheduled` is present:

```json
{
  "queue": "jobs",
  "duplicate": false,
  "scheduled": {
    "schedule_id": "8f14e45f-ceea-467f-a0fe-7a3abb2af606",
    "deliver_at_ms": 1717189260000
  }
}
```

For publish, `queue` is the internal topic `__direct`. Omitted JSON fields are not sent when empty (`message_id`, `delivery`, `scheduled`).

---

### Batch enqueue

**`POST /v1/enqueue/batch`** and **`POST /v1/gateway/enqueue`** use the **wire format** (`PublishRequest`), not the friendly enqueue field names:

```json
{
  "messages": [
    {
      "topic": "jobs",
      "routing_key": "user-42",
      "payload": { "task": "a" },
      "queue_id": "550e8400-e29b-41d4-a716-446655440000",
      "delay_ms": 5000,
      "priority": 5,
      "idempotency_key": "batch-1"
    }
  ]
}
```

→ `202`

```json
{
  "accepted": 1,
  "message_ids": ["7c9e6679-7425-40de-944b-e07fc1f90ae7"]
}
```

No batch size cap by default (override with `BETTERMQ_MAX_HTTP_BODY_BYTES` / related limits if you need one).

---

### Groups (fan-out)

**`POST /v1/groups`** `{ "name": "billing-alerts" }` → `201`

```json
{
  "group_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "name": "billing-alerts",
  "paused": false
}
```

**`POST /v1/groups/{group_id}/members`**

```json
{
  "name": "primary",
  "url": "https://app.example/hooks/a",
  "secret": "whsec_example",
  "parallelism": 1,
  "rate": 50,
  "period_secs": 60,
  "flow_key": "user-42"
}
```

→ `201` — `MemberResponse` with `member_id`, `group_id`, `name`, `url`, `paused`, `parallelism`, `rate`, `period_secs`, optional `flow_key`.

**`PUT /v1/groups/{group_id}`** — pause or rename a group (partial update):

```json
{ "paused": true }
```

→ `200` — `{ "group_id": "…", "name": "…", "paused": true }`

When a group is paused, fan-out publish skips **all** members. Resume with `{ "paused": false }`. Optional `name` renames the group.

**`PUT /v1/groups/{group_id}/members/{member_id}`** — pause or update one destination:

```json
{ "paused": true }
```

→ `200` — `MemberResponse`. A paused member is skipped during fan-out; other members still receive. Resume with `{ "paused": false }`. Other optional fields: `name`, `url`, `secret`, `parallelism`, `rate`, `period_secs`, `flow_key`.

**`POST /v1/publish`** (URL or group)

Direct URL:

```json
{
  "url": "https://example.com/hook",
  "secret": "whsec_…",
  "key": "user-42",
  "body": { "event": "invoice.paid" },
  "delay": 5000,
  "idempotency_key": "inv-99"
}
```

Fan-out group (same endpoint — delay, retries, idempotency, method/headers/sign; **do not** send `flow_id` / `flowControl`):

```json
{
  "group_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "key": "user-42",
  "body": { "event": "invoice.paid" },
  "delay": 5000,
  "idempotency_key": "inv-99"
}
```

→ `202`

```json
{
  "queue": "__group.a1b2c3d4-…",
  "duplicate": false,
  "group_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "accepted": 2,
  "deliveries": [
    {
      "member_id": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
      "message_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
      "duplicate": false,
      "shard": 0,
      "seq": 4
    }
  ]
}
```

**`GET /v1/groups/{group_id}`** → `200`

```json
{
  "group": { "group_id": "…", "name": "…", "paused": false },
  "members": [ { "member_id": "…", "group_id": "…", "name": "…", "url": "…", "paused": false, "parallelism": 1, "rate": 50, "period_secs": 60 } ]
}
```

**`DELETE /v1/groups/{group_id}`** / **`DELETE /v1/groups/{group_id}/members/{member_id}`** → `200` (deleted record).

---

### Flow profiles & runtime

Lane runtime routes require **exactly one** owner query: `queue_id` (alias `endpoint_id`), `flow_id`, or `group_member_id`. Missing → `400`.

**`POST /v1/flows`**

```json
{ "key": "user-42", "parallelism": 2, "rate": 100, "period_secs": 60 }
```

→ `201`

```json
{
  "flow_id": "660e8400-e29b-41d4-a716-446655440001",
  "key": "user-42",
  "parallelism": 2,
  "rate": 100,
  "period_secs": 60
}
```

**`GET /v1/flows`** → `200` — `{ "flows": [ FlowProfileResponse… ] }`

**`DELETE /v1/flows/{flow_id}`** → `200` — deleted `FlowProfileResponse`.

**`GET /v1/flow/{key}?queue_id=…`** → `200`

```json
{
  "flow_key": "user-42",
  "endpoint_id": "550e8400-e29b-41d4-a716-446655440000",
  "wait_list_size": 3,
  "parallelism_max": 2,
  "parallelism_count": 1,
  "rate_max": 100,
  "rate_count": 12,
  "rate_period_secs": 60,
  "rate_period_start_ms": 1717189200000,
  "paused": false,
  "pinned_parallelism": false,
  "pinned_rate": false
}
```

**`PUT /v1/flow/{key}?queue_id=…`** — body: `{ "parallelism", "rate", "period_secs" }` (no `key` in body) → `200` (`FlowProfileResponse`).

**`POST /v1/flow/{key}/pause|resume|reset-rate?queue_id=…`** → `204` (no body).

**`POST /v1/flow/{key}/pin?queue_id=…`** — `{ "parallelism": 1, "rate": 10, "period_secs": 60 }` → `204`.

**`POST /v1/flow/{key}/unpin?queue_id=…`** — `{ "parallelism": true, "rate": true }` → `204`.

**`GET /v1/flow/global`** → `200` — `{ "parallelism_max": null, "parallelism_count": 4 }`.

---

### Cron & delayed

**`POST /v1/crons`** — provide **`cron`** or **`every_seconds`**, not both. Target via `queue` / `queue_id` or direct `url` + `secret`:

```json
{
  "cron": "0 9 * * *",
  "queue": "jobs",
  "key": "daily-report",
  "body": { "report": true },
  "flow_id": "660e8400-e29b-41d4-a716-446655440001"
}
```

→ `201`

```json
{
  "cron_id": "c1d2e3f4-a5b6-7890-cdef-123456789abc",
  "schedule_type": "cron",
  "cron": "0 9 * * *",
  "queue": "jobs",
  "paused": false,
  "next_run_at_ms": 1717225200000,
  "created_at_ms": 1717189200000,
  "last_run_at_ms": null
}
```

Interval schedules use `"schedule_type": "interval"` and `"every_seconds": 30` (no `cron` field).

**`POST /v1/crons/{cron_id}/pause`** / **`resume`** → `200` (`CronResponse`).

**`GET /v1/crons/{cron_id}`** / **`DELETE /v1/crons/{cron_id}`** → `200` (`CronResponse`).

**`GET /v1/delayed`** → `200`

```json
{
  "delayed": [
    {
      "schedule_id": "8f14e45f-ceea-467f-a0fe-7a3abb2af606",
      "queue": "jobs",
      "key": "user-42",
      "deliver_at_ms": 1717189260000,
      "body": "{\"task\":\"later\"}"
    }
  ]
}
```

**`DELETE /v1/delayed/{schedule_id}`** → `200` — returns the cancelled job (same shape as list item).

---

### DLQ

**`GET /v1/dlq/sources`** → `200` — buckets for named queues, direct publish, and group members (`queue` / `direct` / `group_member`).

**`GET /v1/dlq?queue=jobs&limit=10`** → `200` (default `limit` = 10). Pass `dlq_topic=` instead of `queue=` for non-queue DLQs (e.g. `__direct.__dlq`).

```json
{
  "queue": "jobs",
  "dlq_topic": "jobs.__dlq",
  "messages": [
    {
      "message_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
      "key": "user-42",
      "body": "{\"task\":\"failed\"}",
      "published_at_ms": 1717189200000,
      "partition": 0,
      "offset": 42,
      "reason": "retries exhausted"
    }
  ]
}
```

**`DELETE /v1/dlq?dlq_topic=jobs.__dlq&partition=0&offset=42`** → `204` (purge one message).

---

### Ops & cluster

**`GET /v1/destinations/blocked`** → `200`

```json
{
  "hosts": [
    { "host": "https://api.example.com:443", "remaining_ms": 45000 }
  ]
}
```

**`POST /v1/destinations/block`** `{ "host": "https://api.example.com:443", "duration_ms": 1800000 }` → `200` `{ "host": "…", "remaining_ms": … }` (default duration 30 minutes).

**`POST /v1/destinations/unblock`** `{ "host": "https://api.example.com:443" }` → `200` (empty body).

**`GET /v1/cluster`** → `200` — `enabled`, `cluster_id`, `node_count`, `healthy_count`, `scheduler_leader_id`, `nodes[]`, `shards[]` (see `/docs` for full schema).

---

## Example: enqueue a job

```bash
export TOKEN="sk_local_…"

# 1. Create queue → 201
curl -sS -X POST http://localhost:8080/v1/queues \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "queue": "jobs",
    "url": "https://app.example/webhooks/jobs",
    "secret": "whsec_example"
  }'
# → {"queue_id":"550e8400-…","queue":"jobs","url":"https://app.example/webhooks/jobs"}

# 2. Enqueue → 202
curl -sS -X POST http://localhost:8080/v1/enqueue \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "queue": "jobs",
    "key": "user-42",
    "body": { "task": "send_invoice", "invoice_id": 99 },
    "priority": 8,
    "sign": true
  }'
# → {"message_id":"7c9e6679-…","queue":"jobs","duplicate":false,"delivery":{"shard":0,"seq":12}}

# 3. Fan-out publish → 202 (create group + members first)
curl -sS -X POST http://localhost:8080/v1/publish \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "group_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "key": "user-42",
    "body": { "event": "invoice.paid" },
    "idempotency_key": "inv-99"
  }'
```

betterMQ POSTs `{ "task": "send_invoice", "invoice_id": 99 }` to your webhook URL (plus `betterMQ-Signature` headers when `sign: true`).

---

## Retry policy

Resolution order: **request** → **queue defaults** → **`dispatch.retry` in `betterMQ.json`**.

```json
{
  "max_retries": 5,
  "retry_backoff": {
    "kind": "exponential",
    "initialMs": 1000,
    "maxMs": 60000,
    "multiplier": 2
  }
}
```

`max_retries` is additional attempts after the first failure (`0` = one try total).

---

## Repository layout

| Path | Purpose |
|------|---------|
| [`engine/`](engine/) | Rust workspace — `betterMQ` server binary and crates |
| [`selfhost/`](selfhost/) | Docker Compose, self-host deployment |
| [`docs/`](docs/) | Logos and GitHub banner assets |

Full HTTP reference: run the broker and open **`/docs`** (OpenAPI + Scalar).

---

## Control panel

| URL | Purpose |
|-----|---------|
| `/panel/` | Web UI — setup, queues, flows, crons, DLQ, infrastructure |
| `/docs` | Embedded OpenAPI reference |

---

## Contributions

Pull requests are disabled. Coding agents make it too easy to send a large, low-context change that costs maintainers more time than it saves.

---

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
