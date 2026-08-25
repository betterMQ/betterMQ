//! Concurrent webhook sink: uniqueness + per-lane order checks, no GIL.

use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use clap::Parser;
use parking_lot::Mutex;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(name = "bettermq-sink")]
struct Args {
    #[arg(long, default_value = "0.0.0.0:19090")]
    listen: SocketAddr,
    /// Delay before 200 (milliseconds).
    #[arg(long, default_value_t = 0)]
    delay_ms: u64,
    /// Percent of requests that return 500.
    #[arg(long, default_value_t = 0)]
    fail_pct: u8,
    /// Deterministically fail every Nth request (0 disables).
    #[arg(long, default_value_t = 0)]
    fail_every: u64,
    /// Fail the first delivery attempt for each structured run_id/seq body.
    #[arg(long)]
    fail_first_attempt: bool,
}

#[derive(Clone)]
struct SinkState {
    received: Arc<AtomicU64>,
    succeeded: Arc<AtomicU64>,
    failed: Arc<AtomicU64>,
    invalid: Arc<AtomicU64>,
    duplicates: Arc<AtomicU64>,
    unique: Arc<Mutex<HashSet<String>>>,
    last_seq: Arc<Mutex<HashMap<String, u64>>>,
    attempts: Arc<Mutex<HashMap<String, u64>>>,
    inversions: Arc<AtomicU64>,
    started: Instant,
    delay_ms: u64,
    fail_pct: u8,
    fail_every: u64,
    fail_first_attempt: bool,
}

#[derive(Debug, Deserialize)]
struct Body {
    #[serde(default)]
    run_id: Option<String>,
    #[serde(default)]
    seq: Option<u64>,
    #[serde(default)]
    lane: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let state = SinkState {
        received: Arc::new(AtomicU64::new(0)),
        succeeded: Arc::new(AtomicU64::new(0)),
        failed: Arc::new(AtomicU64::new(0)),
        invalid: Arc::new(AtomicU64::new(0)),
        duplicates: Arc::new(AtomicU64::new(0)),
        unique: Arc::new(Mutex::new(HashSet::new())),
        last_seq: Arc::new(Mutex::new(HashMap::new())),
        attempts: Arc::new(Mutex::new(HashMap::new())),
        inversions: Arc::new(AtomicU64::new(0)),
        started: Instant::now(),
        delay_ms: args.delay_ms,
        fail_pct: args.fail_pct.min(100),
        fail_every: args.fail_every,
        fail_first_attempt: args.fail_first_attempt,
    };
    let stats = state.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let n = stats.received.load(Ordering::Relaxed);
            let ok = stats.succeeded.load(Ordering::Relaxed);
            let failed = stats.failed.load(Ordering::Relaxed);
            let u = stats.unique.lock().len();
            let dup = stats.duplicates.load(Ordering::Relaxed);
            let inv = stats.inversions.load(Ordering::Relaxed);
            let rps = n as f64 / stats.started.elapsed().as_secs_f64().max(1e-9);
            eprintln!(
                "sink received={n} succeeded={ok} failed={failed} unique={u} \
                 duplicates={dup} inversions={inv} rps={rps:.0}"
            );
        }
    });
    let app = Router::new()
        .route("/hook", post(hook))
        .route("/stats", axum::routing::get(stats_handler))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    eprintln!("bettermq-sink listening on {}", args.listen);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn hook(State(state): State<SinkState>, body: axum::body::Bytes) -> StatusCode {
    let request_number = state.received.fetch_add(1, Ordering::Relaxed) + 1;
    let parsed = serde_json::from_slice::<Body>(&body).ok();
    let message_key = parsed.as_ref().and_then(|value| {
        value
            .run_id
            .as_ref()
            .zip(value.seq)
            .map(|(run_id, seq)| format!("{run_id}:{seq}"))
    });
    let attempt = message_key.as_ref().map(|key| {
        let mut attempts = state.attempts.lock();
        let attempt = attempts.entry(key.clone()).or_default();
        *attempt += 1;
        *attempt
    });
    if state.delay_ms > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(state.delay_ms)).await;
    }
    let fail_first = state.fail_first_attempt && attempt == Some(1);
    if fail_first || periodic_failure(request_number, state.fail_pct, state.fail_every) {
        state.failed.fetch_add(1, Ordering::Relaxed);
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    state.succeeded.fetch_add(1, Ordering::Relaxed);
    match parsed {
        Some(Body {
            run_id: Some(run),
            seq: Some(seq),
            lane,
        }) => {
            let key = format!("{run}:{seq}");
            if !state.unique.lock().insert(key) {
                state.duplicates.fetch_add(1, Ordering::Relaxed);
            }
            if let Some(lane) = lane {
                let run_lane = format!("{run}:{lane}");
                let mut last = state.last_seq.lock();
                if let Some(prev) = last.get(&run_lane) {
                    if seq < *prev {
                        state.inversions.fetch_add(1, Ordering::Relaxed);
                    }
                }
                last.insert(run_lane, seq);
            }
        }
        _ => {
            state.invalid.fetch_add(1, Ordering::Relaxed);
        }
    }
    StatusCode::OK
}

async fn stats_handler(State(state): State<SinkState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "received": state.received.load(Ordering::Relaxed),
        "succeeded": state.succeeded.load(Ordering::Relaxed),
        "failed": state.failed.load(Ordering::Relaxed),
        "invalid": state.invalid.load(Ordering::Relaxed),
        "unique": state.unique.lock().len(),
        "duplicates": state.duplicates.load(Ordering::Relaxed),
        "inversions": state.inversions.load(Ordering::Relaxed),
    }))
}

fn periodic_failure(request_number: u64, fail_pct: u8, fail_every: u64) -> bool {
    (fail_every > 0 && request_number % fail_every == 0)
        || (fail_pct > 0 && (request_number - 1) % 100 < u64::from(fail_pct))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentage_failure_is_deterministic_per_hundred_requests() {
        let failed = (1..=200)
            .filter(|request| periodic_failure(*request, 10, 0))
            .collect::<Vec<_>>();
        assert_eq!(failed.len(), 20);
        assert_eq!(&failed[..10], &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn fail_every_is_exact_and_composes_with_percentage() {
        assert!(!periodic_failure(3, 0, 4));
        assert!(periodic_failure(4, 0, 4));
        assert!(periodic_failure(100, 1, 4));
    }
}
