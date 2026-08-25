# Backup & restore (self-host)

Stop the broker before backup/restore. A live tar can split a WAL commit from
its RocksDB/checkpoint metadata and is not a supported disaster-recovery image.

## Local WAL / RocksDB

```bash
# Backup
./backup.sh /var/lib/bettermq /tmp/bettermq-backup.tgz
# writes /tmp/bettermq-backup.tgz.sha256 when sha256sum/shasum is available

# Restore (parent directory that will contain the data folder)
./restore.sh /tmp/bettermq-backup.tgz /var/lib
# then: bettermq serve --data-dir /var/lib/bettermq
```

## Multi-node local HA (`sharedMetaDir`)

Also copy the shared meta directory (leases, cursors, catalog JSON):

```bash
tar -czf shared-meta.tgz -C /cluster-shared meta
```

Restore that path on the volume every broker mounts before starting nodes.

## Slate + S3/MinIO

- Data dir backup still covers local cache + `bettermq.json` + auth.
- **Message durability is in object storage** — use your provider’s bucket versioning / snapshot (both `bucket` and `payloadBucket`).
- Single-node MinIO in compose is **not** HA; treat it as dev-only.
- Slate is an optional object-store profile, not the Core V2 fast ACK path.
  The low-latency profile acknowledges from the local/RF=3 WAL and archives
  sealed segments asynchronously.

## After restore

1. Run `bettermq doctor --data-dir /var/lib/bettermq --json`.
2. Confirm `BETTERMQ_CLUSTER_SECRET` / panel token still match.
3. Start one node and wait for `/readyz` (not only `/healthz`).
4. For clusters, start peers one at a time and require `/readyz` before the next.
5. Publish an idempotent canary, confirm delivery, then restore normal traffic.

## Restore drill gate

Before every release, restore the latest production-format backup into an
isolated environment, run `bettermq doctor`, verify WAL frame counts, publish
and deliver a canary, and record elapsed restore time. Quarterly drills should
also simulate loss of one broker and loss of the archive credentials.

## Point-in-time archive restore

Sealed-segment archive is asynchronous and off the `202` path. Optional
`--until-unix-ms` restores only manifests archived at or before that time.
See [ARCHIVE.md](../engine/broker-storage/ARCHIVE.md). This is extra storage,
not a substitute for the RF=3 WAL.

## Rolling migration

1. Take and checksum an offline backup. Keep the old binary and data directory.
2. Run the new binary's `bettermq doctor` against a copy of the data.
3. Upgrade one non-leader/dispatch node, wait for `/readyz`, then continue one
   node at a time. Do not upgrade without quorum headroom.
4. Do not reinterpret or move an existing data directory between V1/V2
   layouts. Use a versioned migration command when one is released.
5. Roll back by stopping the new binary and restoring the untouched backup;
   never open a data directory written by a newer incompatible format with an
   older binary.

## Host breaker note

When `BETTERMQ_SHARED_META_DIR` is set, the outbound host circuit breaker is persisted as `host_breaker.json` under that directory (shared across brokers). Without shared meta it is in-memory only and starts cold after restart.
