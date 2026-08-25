#!/usr/bin/env python3
"""Local BetterMQ load + order test (stdlib only).

Modes:
  publish  — POST /v1/publish, no flow (concurrent delivery; order not guaranteed)
  flow     — same, flow.parallelism=1 + shared key (FIFO on that lane)
  queue    — named queue (default FIFO / parallelism 1)

Usage:
  python3 scripts/load_test.py --base http://127.0.0.1:18080 --token TOKEN --sink-url http://127.0.0.1:19090 --n 100000
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Sink:
    def __init__(self):
        self.lock = threading.Lock()
        self.count = 0
        self.unique = 0
        self.seen = set()
        self.inversions = 0
        self.last_seq = None
        self.errors = 0
        self.run_id = None

    def reset(self, run_id=None):
        with self.lock:
            self.count = 0
            self.unique = 0
            self.seen = set()
            self.inversions = 0
            self.last_seq = None
            self.errors = 0
            self.run_id = run_id

    def record(self, seq: int, run_id=None):
        with self.lock:
            if self.run_id is not None and run_id != self.run_id:
                return
            self.count += 1
            if seq in self.seen:
                return
            self.seen.add(seq)
            self.unique += 1
            if self.last_seq is not None and seq < self.last_seq:
                self.inversions += 1
            self.last_seq = seq

    def snapshot(self):
        with self.lock:
            return {
                "count": self.count,
                "unique": self.unique,
                "inversions": self.inversions,
                "last_seq": self.last_seq,
                "errors": self.errors,
                "run_id": self.run_id,
            }


SINK = Sink()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        return

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n)
        if self.path == "/reset":
            rid = None
            if raw:
                try:
                    rid = json.loads(raw.decode()).get("run_id")
                except Exception:
                    rid = None
            SINK.reset(rid)
            self.send_response(200)
            self.end_headers()
            return
        if self.path == "/stats":
            body = json.dumps(SINK.snapshot()).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        try:
            data = json.loads(raw.decode() or "{}")
            seq = int(data.get("seq", -1))
            SINK.record(seq, data.get("run_id"))
        except Exception:
            with SINK.lock:
                SINK.errors += 1
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        if self.path == "/stats":
            return self.do_POST()
        self.send_response(404)
        self.end_headers()


def http_json(method: str, url: str, token: str | None, payload=None, timeout=120):
    data = None if payload is None else json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw.decode()) if raw else {}
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        raise RuntimeError(f"{method} {url} -> {e.code} {body[:500]}") from e


def rss_mb(pid: int) -> float | None:
    try:
        import subprocess

        out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], text=True)
        return int(out.strip()) / 1024.0
    except Exception:
        return None


def wait_delivered(sink_url: str, want: int, timeout_s: float, broker_pid: int | None):
    t0 = time.time()
    last = -1
    got = 0
    last_progress_at = t0
    last_progress = -1
    body = {}
    while time.time() - t0 < timeout_s:
        st, body = http_json("GET", f"{sink_url}/stats", None)
        got = int(body.get("unique") or body.get("count") or 0)
        total = int(body.get("count") or 0)
        inv = int(body.get("inversions") or 0)
        rss = rss_mb(broker_pid) if broker_pid else None
        if got != last and (got % 1000 == 0 or got == want or got - last >= 1000):
            rss_s = f" rss={rss:.0f}MB" if rss is not None else ""
            print(
                f"    delivered unique={got}/{want} http={total} inversions={inv}{rss_s}",
                flush=True,
            )
            last = got
        if rss is not None and rss > 10_000:
            raise RuntimeError(f"broker RSS {rss:.0f}MB exceeded 10GB safety stop")
        if got >= want:
            return body, time.time() - t0
        if got != last_progress:
            last_progress = got
            last_progress_at = time.time()
        elif time.time() - last_progress_at > 45:
            print(
                f"    drain stalled at unique={got}/{want} http={total} inversions={inv}",
                flush=True,
            )
            return body, time.time() - t0
        time.sleep(0.5)
    print(
        f"    drain timeout at unique={got}/{want} http={body.get('count')} inversions={body.get('inversions')}",
        flush=True,
    )
    return body, time.time() - t0


def post_batch(base: str, token: str, messages: list) -> int:
    st, body = http_json("POST", f"{base}/v1/enqueue/batch", token, {"messages": messages})
    if st not in (200, 202):
        raise RuntimeError(f"batch status {st} {body}")
    return int(body.get("accepted") or 0)


def ingest(base: str, token: str, n: int, workers: int, make_msg, label: str) -> tuple[float, int]:
    batch_size = 100
    total_batches = (n + batch_size - 1) // batch_size
    seq = 0

    def next_batch():
        nonlocal seq
        if seq >= n:
            return None
        chunk = []
        for _ in range(min(batch_size, n - seq)):
            chunk.append(make_msg(seq))
            seq += 1
        return chunk

    accepted = 0
    t0 = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        in_flight = set()
        for _ in range(min(workers, total_batches)):
            b = next_batch()
            if b is None:
                break
            in_flight.add(ex.submit(post_batch, base, token, b))
        while in_flight:
            fut = next(as_completed(in_flight))
            in_flight.remove(fut)
            accepted += fut.result()
            done += 1
            if done % 50 == 0 or done == total_batches:
                elapsed = time.time() - t0
                rate = accepted / elapsed if elapsed else 0
                print(
                    f"    ingest {label}: {accepted}/{n} ({done}/{total_batches} batches) {rate:.0f} msg/s",
                    flush=True,
                )
            b = next_batch()
            if b is not None:
                in_flight.add(ex.submit(post_batch, base, token, b))
    return time.time() - t0, accepted


def run_mode(args, mode: str, n: int, queue_id: str | None):
    sink = args.sink_url.rstrip("/")
    run_id = f"{mode}-{n}-{int(time.time())}"
    http_json("POST", f"{sink}/reset", None, {"run_id": run_id})

    def make_msg(seq: int):
        # Spread keys across partitions for ingest throughput unless --ordered.
        lane = "load-lane" if args.ordered else f"load-lane-{seq % max(1, args.lanes)}"
        payload = {"seq": seq, "mode": mode, "n": n, "run_id": run_id}
        msg = {
            "payload": payload,
            "routing_key": lane,
            "max_retries": 0,
        }
        if mode == "queue":
            msg["queue_id"] = queue_id
        else:
            msg["url"] = f"{sink}/hook"
            msg["secret"] = "whsec_load"
        if mode == "flow":
            msg["flow"] = {
                "key": lane,
                "parallelism": 1 if args.ordered else 8,
            }
        return msg

    print(f"\n=== {mode} n={n} ===", flush=True)
    ingest_s, accepted = ingest(args.base, args.token, n, args.workers, make_msg, mode)
    ingest_rate = accepted / ingest_s if ingest_s else 0
    print(f"  ingest done: {accepted} in {ingest_s:.1f}s ({ingest_rate:.0f} msg/s)", flush=True)

    timeout = args.drain_timeout if args.drain_timeout is not None else max(180.0, n / 8.0)
    stats, drain_s = wait_delivered(sink, n, timeout, args.broker_pid)
    inv = int(stats.get("inversions") or 0)
    unique = int(stats.get("unique") or stats.get("count") or 0)
    print(
        f"  drain done: unique={unique} http={stats.get('count')} in {drain_s:.1f}s inversions={inv}",
        flush=True,
    )
    return {
        "mode": mode,
        "n": n,
        "ordered": bool(args.ordered),
        "accepted": accepted,
        "ingest_s": round(ingest_s, 2),
        "ingest_msg_s": round(ingest_rate, 1),
        "drain_s": round(drain_s, 2),
        "delivered": stats.get("count"),
        "unique": unique,
        "inversions": inv,
        "rss_mb": rss_mb(args.broker_pid) if args.broker_pid else None,
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base", required=True)
    p.add_argument("--token", required=True)
    p.add_argument("--sink-url", required=True)
    p.add_argument("--n", type=int, default=100_000)
    p.add_argument("--workers", type=int, default=8)
    p.add_argument(
        "--lanes",
        type=int,
        default=16,
        help="distinct routing keys when not --ordered (maps onto 4 partitions)",
    )
    p.add_argument("--broker-pid", type=int, default=None)
    p.add_argument(
        "--drain-timeout",
        type=float,
        default=None,
        help="seconds to wait for unique deliveries (default max(180, n/8))",
    )
    p.add_argument("--serve-sink", action="store_true")
    p.add_argument("--sink-port", type=int, default=19090)
    p.add_argument("--modes", default="publish,flow,queue")
    p.add_argument(
        "--ordered",
        action="store_true",
        help="single routing key + flow parallelism 1 (FIFO). Slower ingest.",
    )
    args = p.parse_args()
    args.base = args.base.rstrip("/")

    if args.serve_sink:
        httpd = ThreadingHTTPServer(("127.0.0.1", args.sink_port), Handler)
        print(f"sink listening on 127.0.0.1:{args.sink_port}", flush=True)
        httpd.serve_forever()
        return

    # health
    st, _ = http_json("GET", f"{args.base}/healthz", None)
    if st != 200:
        sys.exit(f"broker health {st}")

    queue_id = None
    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    if "queue" in modes:
        st, q = http_json(
            "POST",
            f"{args.base}/v1/queues",
            args.token,
            {
                "queue": f"load-{int(time.time())}",
                "url": f"{args.sink_url.rstrip('/')}/hook",
                "secret": "whsec_load",
                "max_retries": 0,
            },
        )
        queue_id = q.get("queue_id")
        print(f"queue_id={queue_id}", flush=True)

    results = []
    for mode in modes:
        results.append(run_mode(args, mode, args.n, queue_id))

    print("\n=== SUMMARY ===")
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
