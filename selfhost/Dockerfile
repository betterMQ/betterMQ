# Self-host BetterMQ (local or Slate storage, panel auth — no Postgres).
FROM rust:1-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    clang libclang-dev llvm-dev cmake build-essential pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY engine ./engine
RUN cargo build --release -p broker-server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/bettermq /usr/local/bin/bettermq
COPY engine/control-panel /usr/share/bettermq/panel
ENV BETTERMQ_PANEL_DIR=/usr/share/bettermq/panel
EXPOSE 8080
ENTRYPOINT ["bettermq"]
