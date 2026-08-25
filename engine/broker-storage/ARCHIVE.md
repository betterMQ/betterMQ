# Sealed-segment archive

The archive worker is bounded and asynchronous from normal WAL commits. It
streams each sealed segment, retries failures, writes a SHA-256 manifest, and
blocks new ingest when configured lag limits are exceeded.

Each sealed segment has durable pending/completion markers beside the local
segment. Normal broker startup initializes the archive worker and streams every
unfinished segment back through the bounded queue, including jobs whose retries
were exhausted before a restart.

Local archive:

```bash
BETTERMQ_ARCHIVE_DIR=/srv/bettermq-archive bettermq serve
cargo run -p broker-storage --bin bettermq-archive -- scrub /srv/bettermq-archive
cargo run -p broker-storage --bin bettermq-archive -- restore /srv/bettermq-archive /srv/restored
```

S3, MinIO, or R2 archive:

```bash
export BETTERMQ_ARCHIVE_BUCKET=bettermq-segments
export R2_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com
export R2_ACCESS_KEY=...
export R2_SECRET_KEY=...
export BETTERMQ_ARCHIVE_PREFIX=production

cargo run -p broker-storage --features s3 --bin bettermq-archive -- scrub-s3 production
cargo run -p broker-storage --features s3 --bin bettermq-archive -- restore-s3 /srv/restored production
```

For AWS/MinIO, use `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, and
`S3_REGION`. HTTP endpoints are permitted only when the configured endpoint
explicitly starts with `http://`.

Relevant limits:

- `BETTERMQ_ARCHIVE_QUEUE_CAPACITY` (default 128 segments)
- `BETTERMQ_ARCHIVE_MAX_RETRIES` (default 5)
- `BETTERMQ_ARCHIVE_MAX_QUEUED_BYTES` (default 4 GiB)
- `BETTERMQ_ARCHIVE_MAX_LAG_MS` (default 5 minutes)

Point-in-time restore (optional extra storage; S3 is never on the ACK path):

```bash
# Restore only segments archived at or before this Unix millisecond.
cargo run -p broker-storage --bin bettermq-archive -- restore /srv/bettermq-archive /srv/restored --until-unix-ms 1710000000000
cargo run -p broker-storage --features s3 --bin bettermq-archive -- restore-s3 /srv/restored production --until-unix-ms 1710000000000
```

Restore writes only paths validated as safe relative archive keys and verifies
both byte length and SHA-256 before replacing a destination file. Segments
newer than `--until-unix-ms` are skipped and counted in the report's `skipped`
field.
