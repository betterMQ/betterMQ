# BetterMQ engine

Shared Rust crates used by both products:

| Product | Binary | Features |
|---------|--------|----------|
| **Self-host** | `../selfhost/` | `broker-server` default (local panel auth) |
| **Cloud** | `../cloud/` | `broker-server` with `--features cloud` (Postgres) |

Crates: `broker-proto`, `broker-storage`, `broker-partition`, `broker-dispatch`, `broker-schedule`, `broker-api`, `broker-server`, `broker-cli`, etc.

Build self-host from repo root:

```bash
cargo build --release -p broker-server
```

Build cloud:

```bash
cargo build --release -p broker-server --features cloud
```
