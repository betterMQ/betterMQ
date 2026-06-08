# BetterMQ engine

Shared Rust crates for the self-hosted webhook broker.

| Area | Path |
|------|------|
| **Server binary** | `broker-server` (built from repo root) |
| **Docker / compose** | [`../selfhost/`](../selfhost/) |
| **Control panel** | `control-panel/` (embedded in broker) |

Crates: `broker-proto`, `broker-storage`, `broker-partition`, `broker-dispatch`, `broker-schedule`, `broker-api`, `broker-server`, `broker-cli`, `broker-config`, etc.

Build from repo root:

```bash
cargo build --release -p broker-server
```

Run locally:

```bash
cargo run -p broker-cli -- serve
```
