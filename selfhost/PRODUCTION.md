# Production launch and soak gates

These are release gates, not current benchmark claims.

## Before launch

- `bettermq doctor --data-dir <data-dir>` passes on every broker.
- Every container reports `/readyz`; `/healthz` alone is only liveness.
- A restore drill from a checksummed backup completes within the documented RTO.
- Capacity tests use production body sizes, batch sizes, durability, TLS, and
  destination latency. Keep at least 30% disk, memory, and ingest headroom.
- Slate remains outside the low-latency ACK profile. Core V2 ACKs from the
  local/RF=3 WAL; sealed segment archive is asynchronous.

## Performance gates

Progressively require:

1. at least 10k messages/s on one local-WAL node;
2. at least 50k messages/s per broker with bounded batches;
3. at least 100k messages/s per RF=3 cell, p50 at most 5ms and p99 at most
   20ms, with zero loss of acknowledged records after any one-node failure.

`202` is minISR=2 (two durable copies). Do not wait for three fsyncs.
Record hardware, filesystem, fsync mode, payload distribution, batch size, and
open-loop offered load with every result. A saturated closed-loop benchmark is
not a launch result.

## Soak and fault gates

- Pull requests that change durability, admission, replication, archive, or
  shutdown run a 24-hour soak on dedicated Linux/NVMe hardware.
- Release candidates run 72 hours with rolling restarts, SIGTERM during load,
  SIGKILL, disk-full/fsync failure, slow archive, slow replica, and destination
  outage injection.
- Pass only with no acknowledged-message loss, bounded memory/queued bytes,
  bounded archive lag, no readiness false positives, and stable p99 latency.
- Save metrics, load-generator settings, logs, and a redacted
  `bettermq support-bundle` with the release evidence.
