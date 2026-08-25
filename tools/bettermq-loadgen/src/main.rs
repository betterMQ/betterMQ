//! HTTP ingest load generator with open-loop and saturation modes.

use anyhow::{bail, Context};
use clap::{Parser, ValueEnum};
use hdrhistogram::Histogram;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum HttpVersion {
    Http1,
    Http2,
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
enum LoadMode {
    /// Keep `concurrency` requests in flight whenever possible.
    MaxThroughput,
    /// Schedule messages at `rate` without waiting for earlier responses.
    FixedRate,
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
enum Endpoint {
    /// Publish for one-record requests, compatibility batch up to 100, HT batch above 100.
    Auto,
    /// POST /v1/publish (requires batch-size=1).
    Publish,
    /// POST /v1/enqueue/batch (maximum 100 records).
    Batch,
    /// POST /v1/ingest/batch (maximum 1000 records).
    IngestBatch,
}

impl Endpoint {
    fn resolve(self, batch_size: usize) -> anyhow::Result<Self> {
        let resolved = match self {
            Self::Auto if batch_size == 1 => Self::Publish,
            Self::Auto if batch_size <= 100 => Self::Batch,
            Self::Auto => Self::IngestBatch,
            explicit => explicit,
        };
        match resolved {
            Self::Publish if batch_size != 1 => bail!("publish endpoint requires --batch-size 1"),
            Self::Batch if batch_size > 100 => bail!("batch endpoint supports at most 100 records"),
            Self::IngestBatch if batch_size > 1000 => {
                bail!("ingest-batch endpoint supports at most 1000 records")
            }
            _ => Ok(resolved),
        }
    }

    fn path(self) -> &'static str {
        match self {
            Self::Publish => "/v1/publish",
            Self::Batch => "/v1/enqueue/batch",
            Self::IngestBatch => "/v1/ingest/batch",
            Self::Auto => unreachable!("endpoint must be resolved"),
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "bettermq-loadgen", about = "BetterMQ ingest load generator")]
struct Args {
    /// Broker base URL, e.g. http://127.0.0.1:8080
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    url: String,
    /// Bearer token (omit with BETTERMQ_INSECURE_NO_AUTH=1)
    #[arg(long, env = "BETTERMQ_TOKEN")]
    token: Option<String>,
    #[arg(long, default_value_t = 10_000)]
    count: u64,
    #[arg(long, default_value_t = 32)]
    concurrency: usize,
    #[arg(long, default_value_t = 100)]
    batch_size: usize,
    #[arg(long, default_value_t = 1024)]
    body_bytes: usize,
    #[arg(long, default_value_t = 16)]
    lanes: u32,
    /// Destination URL frozen on each message.
    #[arg(long, default_value = "http://127.0.0.1:19090/hook")]
    dest: String,
    #[arg(long, default_value = "s3cret")]
    secret: String,
    /// Client-side linger before sending each request.
    #[arg(long, default_value_t = 0)]
    linger_ms: u64,
    #[arg(long)]
    idempotency: bool,
    /// Stable identifier embedded in every delivered body (random UUID by default).
    #[arg(long)]
    run_id: Option<String>,
    #[arg(long, value_enum, default_value_t = Endpoint::Auto)]
    endpoint: Endpoint,
    #[arg(long, value_enum, default_value_t = HttpVersion::Http1)]
    http: HttpVersion,
    #[arg(long, value_enum, default_value_t = LoadMode::MaxThroughput)]
    mode: LoadMode,
    /// Target messages/second for fixed-rate mode.
    #[arg(long)]
    rate: Option<f64>,
    /// Print a JSON object after the human report (for the ACK latency gate).
    #[arg(long, default_value_t = false)]
    json: bool,
}

struct Results {
    accepted: AtomicU64,
    failed_messages: AtomicU64,
    requests: AtomicU64,
    failed_requests: AtomicU64,
    payload_bytes: AtomicU64,
    wire_bytes: AtomicU64,
    service_us: Mutex<Histogram<u64>>,
    scheduled_us: Mutex<Histogram<u64>>,
    errors: Mutex<BTreeMap<String, u64>>,
}

impl Results {
    fn new() -> anyhow::Result<Self> {
        Ok(Self {
            accepted: AtomicU64::new(0),
            failed_messages: AtomicU64::new(0),
            requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            payload_bytes: AtomicU64::new(0),
            wire_bytes: AtomicU64::new(0),
            service_us: Mutex::new(Histogram::new(3)?),
            scheduled_us: Mutex::new(Histogram::new(3)?),
            errors: Mutex::new(BTreeMap::new()),
        })
    }

    fn record(&self, outcome: RequestOutcome, service: Duration, scheduled: Duration) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        record_duration(&self.service_us, service);
        record_duration(&self.scheduled_us, scheduled);
        self.wire_bytes
            .fetch_add(outcome.wire_bytes as u64, Ordering::Relaxed);
        self.accepted.fetch_add(outcome.accepted, Ordering::Relaxed);
        self.failed_messages
            .fetch_add(outcome.failed, Ordering::Relaxed);
        self.payload_bytes
            .fetch_add(outcome.payload_bytes, Ordering::Relaxed);
        if outcome.failed > 0 || outcome.error.is_some() {
            if outcome.accepted == 0 {
                self.failed_requests.fetch_add(1, Ordering::Relaxed);
            }
            if let Some(error) = outcome.error {
                *self
                    .errors
                    .lock()
                    .expect("error lock poisoned")
                    .entry(error)
                    .or_default() += 1;
            }
        }
    }
}

struct RequestOutcome {
    accepted: u64,
    failed: u64,
    payload_bytes: u64,
    wire_bytes: usize,
    error: Option<String>,
}

#[derive(serde::Deserialize)]
struct BatchResponseBody {
    accepted: Option<u64>,
    failed: Option<u64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    validate_args(&args)?;
    let endpoint = args.endpoint.resolve(args.batch_size)?;
    let mut builder = reqwest::Client::builder().pool_max_idle_per_host(args.concurrency);
    builder = match args.http {
        HttpVersion::Http1 => builder.http1_only(),
        HttpVersion::Http2 => builder.http2_prior_knowledge(),
    };
    let http = builder.build()?;
    let run_id = args
        .run_id
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let body = Arc::new("x".repeat(args.body_bytes));
    let results = Arc::new(Results::new()?);
    let slots = Arc::new(Semaphore::new(args.concurrency));
    let started = Instant::now();
    let mut tasks = JoinSet::new();
    let mut seq = 0u64;

    eprintln!(
        "run_id={run_id} endpoint={} protocol={:?} mode={:?} count={} batch={} concurrency={}",
        endpoint.path(),
        args.http,
        args.mode,
        args.count,
        args.batch_size,
        args.concurrency
    );

    while seq < args.count {
        let start_seq = seq;
        let take = (args.count - seq).min(args.batch_size as u64);
        seq += take;
        let scheduled_at = match (args.mode, args.rate) {
            (LoadMode::FixedRate, Some(rate)) => {
                started + Duration::from_secs_f64(start_seq as f64 / rate)
            }
            _ => Instant::now(),
        };
        if scheduled_at > Instant::now() {
            tokio::time::sleep_until(scheduled_at.into()).await;
        }
        if args.linger_ms > 0 {
            tokio::time::sleep(Duration::from_millis(args.linger_ms)).await;
        }
        let permit = slots.clone().acquire_owned().await?;
        let http = http.clone();
        let base = args.url.clone();
        let token = args.token.clone();
        let body = body.clone();
        let dest = args.dest.clone();
        let secret = args.secret.clone();
        let run_id = run_id.clone();
        let results = results.clone();
        let lanes = args.lanes;
        let idempotency = args.idempotency;
        let body_bytes = args.body_bytes as u64;
        tasks.spawn(async move {
            let _permit = permit;
            let service_started = Instant::now();
            let outcome = send_request(
                &http,
                &base,
                token.as_deref(),
                endpoint,
                &body,
                &dest,
                &secret,
                &run_id,
                start_seq,
                take,
                lanes,
                idempotency,
                body_bytes,
            )
            .await;
            let completed = Instant::now();
            results.record(
                outcome,
                completed.duration_since(service_started),
                completed.duration_since(scheduled_at),
            );
        });
    }
    while let Some(joined) = tasks.join_next().await {
        joined.context("load task panicked")?;
    }

    report(&args, endpoint, &run_id, started.elapsed(), &results);
    let failed = results.failed_messages.load(Ordering::Relaxed);
    if failed > 0 {
        bail!("{failed} messages failed");
    }
    Ok(())
}

fn validate_args(args: &Args) -> anyhow::Result<()> {
    if args.count == 0 {
        bail!("--count must be greater than zero");
    }
    if args.concurrency == 0 {
        bail!("--concurrency must be greater than zero");
    }
    if args.batch_size == 0 {
        bail!("--batch-size must be greater than zero");
    }
    if args.lanes == 0 {
        bail!("--lanes must be greater than zero");
    }
    match (args.mode, args.rate) {
        (LoadMode::FixedRate, Some(rate)) if rate.is_finite() && rate > 0.0 => {}
        (LoadMode::FixedRate, _) => bail!("fixed-rate mode requires a positive finite --rate"),
        (LoadMode::MaxThroughput, Some(_)) => bail!("--rate is only valid with --mode fixed-rate"),
        (LoadMode::MaxThroughput, None) => {}
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn send_request(
    http: &reqwest::Client,
    base: &str,
    token: Option<&str>,
    endpoint: Endpoint,
    body: &str,
    dest: &str,
    secret: &str,
    run_id: &str,
    start_seq: u64,
    take: u64,
    lanes: u32,
    idempotency: bool,
    body_bytes: u64,
) -> RequestOutcome {
    let payload = build_request_payload(
        endpoint,
        body,
        dest,
        secret,
        run_id,
        start_seq,
        take,
        lanes,
        idempotency,
    );
    let encoded = match serde_json::to_vec(&payload) {
        Ok(encoded) => encoded,
        Err(error) => {
            return RequestOutcome {
                accepted: 0,
                failed: take,
                payload_bytes: 0,
                wire_bytes: 0,
                error: Some(format!("encode: {error}")),
            };
        }
    };
    let wire_bytes = encoded.len();
    let url = format!("{}{}", base.trim_end_matches('/'), endpoint.path());
    let mut req = http
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(encoded);
    if let Some(token) = token {
        req = req.bearer_auth(token);
    }
    match req.send().await {
        Ok(resp) => {
            let status = resp.status();
            let body = resp
                .text()
                .await
                .unwrap_or_else(|error| format!("<read error: {error}>"));
            parse_http_outcome(endpoint, status, &body, take, body_bytes, wire_bytes)
        }
        Err(error) => RequestOutcome {
            accepted: 0,
            failed: take,
            payload_bytes: 0,
            wire_bytes,
            error: Some(format!("transport: {error}")),
        },
    }
}

fn parse_http_outcome(
    endpoint: Endpoint,
    status: reqwest::StatusCode,
    body: &str,
    take: u64,
    body_bytes: u64,
    wire_bytes: usize,
) -> RequestOutcome {
    let fail = |error: String| RequestOutcome {
        accepted: 0,
        failed: take,
        payload_bytes: 0,
        wire_bytes,
        error: Some(error),
    };
    if !status.is_success() {
        return fail(format!("{status}: {}", truncate(body, 512)));
    }
    if endpoint == Endpoint::Publish {
        return RequestOutcome {
            accepted: take,
            failed: 0,
            payload_bytes: body_bytes.saturating_mul(take),
            wire_bytes,
            error: None,
        };
    }
    match serde_json::from_str::<BatchResponseBody>(body) {
        Ok(parsed) => {
            let accepted = parsed.accepted.unwrap_or(0);
            let failed = parsed
                .failed
                .unwrap_or_else(|| take.saturating_sub(accepted));
            let error = if failed > 0 {
                Some(format!(
                    "{status}: partial accepted={accepted} failed={failed}"
                ))
            } else {
                None
            };
            RequestOutcome {
                accepted,
                failed,
                payload_bytes: body_bytes.saturating_mul(accepted),
                wire_bytes,
                error,
            }
        }
        Err(_) if status.as_u16() == 202 => RequestOutcome {
            accepted: take,
            failed: 0,
            payload_bytes: body_bytes.saturating_mul(take),
            wire_bytes,
            error: None,
        },
        Err(_) => fail(format!("{status}: {}", truncate(body, 512))),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_request_payload(
    endpoint: Endpoint,
    body: &str,
    dest: &str,
    secret: &str,
    run_id: &str,
    start_seq: u64,
    take: u64,
    lanes: u32,
    idempotency: bool,
) -> Value {
    let message = |seq: u64, public_shape: bool| {
        let lane = format!("lane-{}", seq % u64::from(lanes));
        let delivered_body = serde_json::to_string(&json!({
            "run_id": run_id,
            "seq": seq,
            "lane": lane,
            "payload": body,
        }))
        .expect("serializing an in-memory body cannot fail");
        let mut value = if public_shape {
            json!({
                "url": dest,
                "secret": secret,
                "key": lane,
                "body": delivered_body,
            })
        } else {
            json!({
                "url": dest,
                "secret": secret,
                "routing_key": lane,
                "payload": delivered_body,
            })
        };
        if idempotency {
            value["idempotency_key"] = json!(format!("{run_id}:{seq}"));
        }
        value
    };
    if endpoint == Endpoint::Publish {
        message(start_seq, true)
    } else {
        let messages = (0..take)
            .map(|index| message(start_seq + index, false))
            .collect::<Vec<_>>();
        json!({ "messages": messages })
    }
}

fn record_duration(histogram: &Mutex<Histogram<u64>>, duration: Duration) {
    let micros = duration.as_micros().max(1).min(u128::from(u64::MAX)) as u64;
    let _ = histogram
        .lock()
        .expect("histogram lock poisoned")
        .record(micros);
}

fn percentile_ms(histogram: &Histogram<u64>, quantile: f64) -> f64 {
    histogram.value_at_quantile(quantile) as f64 / 1000.0
}

fn report(args: &Args, endpoint: Endpoint, run_id: &str, elapsed: Duration, results: &Results) {
    let elapsed_s = elapsed.as_secs_f64().max(1e-9);
    let accepted = results.accepted.load(Ordering::Relaxed);
    let failed_messages = results.failed_messages.load(Ordering::Relaxed);
    let requests = results.requests.load(Ordering::Relaxed);
    let failed_requests = results.failed_requests.load(Ordering::Relaxed);
    let payload_bytes = results.payload_bytes.load(Ordering::Relaxed);
    let wire_bytes = results.wire_bytes.load(Ordering::Relaxed);
    let service = results.service_us.lock().expect("histogram lock poisoned");
    let scheduled = results
        .scheduled_us
        .lock()
        .expect("histogram lock poisoned");
    println!(
        "run_id={run_id} endpoint={} protocol={:?} mode={:?} accepted={accepted} \
         failed_messages={failed_messages} requests={requests} failed_requests={failed_requests} \
         elapsed_s={elapsed_s:.3} msg_s={:.0} payload_bytes_s={:.0} request_bytes_s={:.0}",
        endpoint.path(),
        args.http,
        args.mode,
        accepted as f64 / elapsed_s,
        payload_bytes as f64 / elapsed_s,
        wire_bytes as f64 / elapsed_s,
    );
    println!(
        "service_latency_ms p50={:.3} p95={:.3} p99={:.3} p99.9={:.3}",
        percentile_ms(&service, 0.50),
        percentile_ms(&service, 0.95),
        percentile_ms(&service, 0.99),
        percentile_ms(&service, 0.999),
    );
    println!(
        "scheduled_latency_ms p50={:.3} p95={:.3} p99={:.3} p99.9={:.3}",
        percentile_ms(&scheduled, 0.50),
        percentile_ms(&scheduled, 0.95),
        percentile_ms(&scheduled, 0.99),
        percentile_ms(&scheduled, 0.999),
    );
    for (error, count) in results.errors.lock().expect("error lock poisoned").iter() {
        eprintln!("error_count={count} error={error:?}");
    }
    if args.json {
        let payload = json!({
            "run_id": run_id,
            "endpoint": endpoint.path(),
            "mode": format!("{:?}", args.mode),
            "accepted": accepted,
            "failed_messages": failed_messages,
            "requests": requests,
            "failed_requests": failed_requests,
            "elapsed_s": elapsed_s,
            "msg_s": accepted as f64 / elapsed_s,
            "min_isr": 2,
            "replication_factor": 3,
            "service_latency_ms": {
                "p50": percentile_ms(&service, 0.50),
                "p95": percentile_ms(&service, 0.95),
                "p99": percentile_ms(&service, 0.99),
                "p99_9": percentile_ms(&service, 0.999),
            },
            "scheduled_latency_ms": {
                "p50": percentile_ms(&scheduled, 0.50),
                "p95": percentile_ms(&scheduled, 0.95),
                "p99": percentile_ms(&scheduled, 0.99),
                "p99_9": percentile_ms(&scheduled, 0.999),
            },
            "gate": {
                "p50_ms_max": 5.0,
                "p99_ms_max": 20.0,
                "msg_s_min": 100_000.0,
                "note": "RF=3 cell / minISR=2. 202 waits for two durable copies, not three.",
            },
        });
        println!("{payload}");
    }
}

fn truncate(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}…")
    } else {
        truncated
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_publish_uses_public_body_shape() {
        let value = build_request_payload(
            Endpoint::Publish,
            "xxx",
            "https://sink.test/hook",
            "secret",
            "run-1",
            7,
            1,
            4,
            true,
        );
        assert!(value.get("payload").is_none());
        assert_eq!(value["key"], "lane-3");
        let delivered: Value = serde_json::from_str(value["body"].as_str().unwrap()).unwrap();
        assert_eq!(delivered["run_id"], "run-1");
        assert_eq!(delivered["seq"], 7);
        assert_eq!(delivered["lane"], "lane-3");
        assert_eq!(delivered["payload"], "xxx");
    }

    #[test]
    fn batch_uses_internal_publish_shape_with_sink_metadata() {
        let value = build_request_payload(
            Endpoint::Batch,
            "x",
            "https://sink.test/hook",
            "secret",
            "run-2",
            10,
            2,
            2,
            false,
        );
        let messages = value["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 2);
        assert!(messages[0].get("body").is_none());
        let delivered: Value =
            serde_json::from_str(messages[0]["payload"].as_str().unwrap()).unwrap();
        assert_eq!(delivered["run_id"], "run-2");
        assert_eq!(delivered["seq"], 10);
        assert_eq!(delivered["lane"], "lane-0");
    }

    #[test]
    fn endpoint_limits_are_explicit() {
        assert_eq!(Endpoint::Auto.resolve(1).unwrap(), Endpoint::Publish);
        assert_eq!(Endpoint::Auto.resolve(100).unwrap(), Endpoint::Batch);
        assert_eq!(Endpoint::Auto.resolve(101).unwrap(), Endpoint::IngestBatch);
        assert!(Endpoint::Publish.resolve(2).is_err());
        assert!(Endpoint::Batch.resolve(101).is_err());
        assert!(Endpoint::IngestBatch.resolve(1001).is_err());
    }
}
