# Backup & restore (self-host)

Stop the broker before backup/restore when possible (cleanest).

## Local WAL / RocksDB

```bash
# Backup
./backup.sh /var/lib/bettermq /tmp/bettermq-backup.tgz

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

## After restore

1. Confirm `BETTERMQ_CLUSTER_SECRET` / panel token still match.
2. Start one node, check `/healthz` and panel.
3. For clusters, start peers and wait for gossip/health before heavy publish.

## Host breaker note

The outbound host circuit breaker is **in-memory**. After restart it starts cold (hosts unblocked until failures accumulate again).
