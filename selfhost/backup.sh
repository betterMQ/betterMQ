#!/usr/bin/env bash
# Backup a BetterMQ data directory (local WAL / RocksDB / managed config).
# Usage: ./backup.sh /path/to/data [/path/to/backup.tgz]
set -euo pipefail

DATA_DIR="${1:?usage: backup.sh <data-dir> [backup.tgz]}"
OUT="${2:-bettermq-backup-$(date +%Y%m%d-%H%M%S).tgz}"

if [[ ! -d "$DATA_DIR" ]]; then
  echo "error: data dir not found: $DATA_DIR" >&2
  exit 1
fi

# Prefer offline backup (stop broker first). Live backup may miss in-flight WAL.
tar -C "$(dirname "$DATA_DIR")" -czf "$OUT" "$(basename "$DATA_DIR")"
if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "$OUT" > "${OUT}.sha256"
elif command -v shasum >/dev/null 2>&1; then
  shasum -a 256 "$OUT" > "${OUT}.sha256"
else
  echo "warning: no SHA-256 utility found; checksum sidecar not written" >&2
fi
echo "wrote $OUT"
[[ -f "${OUT}.sha256" ]] && echo "wrote ${OUT}.sha256"
echo "tip: also copy BETTERMQ_SHARED_META_DIR if you use multi-node local HA"
echo "tip: Slate mode — also back up S3/MinIO buckets (messages + payloads)"
