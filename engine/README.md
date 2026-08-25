# BetterMQ engine

Shared Rust crates for the self-hosted webhook broker.

| Area | Path |
|------|------|
| **Server binary** | `broker-server` (built from repo root) |
| **Docker / compose** | [`../selfhost/`](../selfhost/) |
| **Control panel** | `panel/` (Vite + Vue SPA; `dist/` is embedded in the broker) |

Crates: `broker-proto`, `broker-storage`, `broker-partition`, `broker-dispatch`, `broker-schedule`, `broker-api`, `broker-server`, `broker-cli`, `broker-config`, etc.

Build from repo root:

```bash
cargo build --release -p broker-server
```

Run locally:

```bash
cargo run -p broker-cli -- serve
```

The control panel is `panel/` (Vite + Vue 3 + shadcn/vue). `npm run build` writes `dist/`, which `broker-server` embeds. For live UI work:

```bash
cd panel && npm install && npm run dev
```

Open `http://127.0.0.1:5173/panel/` (Vite proxies `/v1` and `/admin/v1` to the broker on `:8080`). Override the embedded UI with `BETTERMQ_PANEL_DIR=engine/panel/dist`.
