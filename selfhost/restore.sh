#!/usr/bin/env bash
# Restore a BetterMQ data directory from backup.sh output.
# Usage: ./restore.sh backup.tgz /path/to/parent-dir
# Creates/replaces <parent>/<archive-root>/ — stop the broker first.
set -euo pipefail

ARCHIVE="${1:?usage: restore.sh <backup.tgz> <parent-dir>}"
PARENT="${2:?usage: restore.sh <backup.tgz> <parent-dir>}"

if [[ ! -f "$ARCHIVE" ]]; then
  echo "error: archive not found: $ARCHIVE" >&2
  exit 1
fi

mkdir -p "$PARENT"
tar -C "$PARENT" -xzf "$ARCHIVE"
echo "restored into $PARENT"
echo "restart bettermq with --data-dir pointing at the restored directory"
echo "if using shared meta / Slate, restore those stores separately before start"
