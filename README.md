# betterMQ

**HTTP message broker.** Enqueue a job, get a durable `202`, and betterMQ **pushes** it to your webhook. No workers to poll.

<img width="1200" height="630" alt="betterMQ — self-hosted HTTP message broker" src="./docs/assets/gh-banner.png" />

[betterMQ.com](https://betterMQ.com) · [Install](https://betterMQ.com/install) · Open `/docs` on a running broker for the live API

---

Enqueue over HTTP. Store durably. Deliver with signed callbacks. Same binary for a laptop, a 3-node cell, or a regional Cloud fleet.

```bash
curl -fsSL https://betterMQ.com/install | bash
bettermq serve
```

Listens on **port 8080**. Panel: [http://localhost:8080/panel/](http://localhost:8080/panel/)

Windows: `powershell -ExecutionPolicy Bypass -c "irm https://betterMQ.com/install.ps1 | iex"`

---

## Features

- **Push queues** — named destinations with retries and parallelism; 1:1 publish; groups (fan-out)
- **Scheduling** — delay, cron, intervals
- **Ordering** — omit `key` for standard (high throughput); set `key` for FIFO per entity
- **Flow control** — per-key parallelism and rate on publish
- **Retries & DLQ** — backoff, dead-letter, idempotency keys
- **Durable ACK** — `202` after the shard WAL commit (RF=3 in a multi-node cell)
- **Panel** — `/panel/` plus `/admin/v1`; optional standalone `bettermq panel`
- **One binary** — `serve` (all-in-one) or split roles: broker, gateway, dispatch, controller, panel

Delivery is **at-least-once**. Handlers should be idempotent.

---

## Quick start

**Docker**

```bash
docker run -d --name bettermq -p 8080:8080 -v bettermq-data:/data \
  ghcr.io/bettermq/bettermq:latest serve --data-dir /data
```

**Compose**

```bash
git clone https://github.com/betterMQ/betterMQ.git
cd betterMQ/selfhost && docker compose up -d
```

**Railway** — [![Deploy on Railway](https://railway.com/button.svg)](https://railway.com/deploy/bettermq?referralCode=O5l32o&utm_medium=integration&utm_source=template&utm_campaign=generic)

### Open the panel

| | |
|--|--|
| **Default port** | `8080` (`-p 9000` or `--listen 0.0.0.0:9000` to change) |
| **Panel** | `http://localhost:8080/panel/` |
| **API docs** | `http://localhost:8080/docs` |
| **Railway / any host** | `https://<your-domain>/panel/` |

First visit: set a panel password and copy the `sk_local_…` API token. Create a queue in the panel, then:

```bash
export TOKEN=sk_local_…
curl -sS -X POST http://localhost:8080/v1/enqueue \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"queue":"jobs","body":{"task":"send_invoice"}}'
```

Add `"key":"user-42"` for FIFO on that entity. Omit `key` for standard (queue parallelism).

---

## How it runs

```
Client  →  gateway?  →  brokers (WAL + RF=3 replicas)  →  dispatch  →  your HTTPS webhook
                              ↑
                         controller (shard map)
                              ↑
                            panel
```

| Command | What it is |
|---------|------------|
| `bettermq serve` | Default. One process: ingest, store, deliver, panel |
| `bettermq serve --profile broker` | Warehouse only (no local delivery workers) |
| `bettermq serve --profile dispatch` | Delivery workers (`BETTERMQ_BROKER_URLS`) |
| `bettermq serve --profile gateway` | Stateless ingest; forwards to shard leaders. **No WAL** |
| `bettermq panel` | UI + `/admin/v1` only; `--controller` points at a cell |

`--broker-only`, `--gateway-only`, and `--dispatch-fleet` are aliases for those profiles.

**Single node.** `bettermq serve` is enough.

**One region (a cell).** Three brokers, RF=3 / minISR=2. Extra nodes hold *other* shards, not extra copies of every message. Optional gateways in front; optional dispatch fleet. One panel (embedded or `bettermq panel`).

**Several regions.** Each region is its own cell (own brokers, own gateways, own ACK quorum). A load balancer can expose one hostname and route a tenant to their home cell. ACKs never wait on another continent. A global panel can *view* every cell; it does not merge the logs.

Compose profiles: [`selfhost/`](selfhost/). Production notes: [`selfhost/PRODUCTION.md`](selfhost/PRODUCTION.md).

---

## Repository

| Path | |
|------|--|
| [`engine/`](engine/) | Rust workspace — `bettermq` binary |
| [`selfhost/`](selfhost/) | Docker Compose |
| [`docs/`](docs/) | Brand assets |

Reference docs will live on the site. Until then, run the broker and use **`/docs`**.

---

## Contributions

Pull requests are disabled.

## License

[MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
