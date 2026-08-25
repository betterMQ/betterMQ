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

if [[ -f "${ARCHIVE}.sha256" ]]; then
  EXPECTED="$(awk '{print $1; exit}' "${ARCHIVE}.sha256")"
  if command -v sha256sum >/dev/null 2>&1; then
    ACTUAL="$(sha256sum "$ARCHIVE" | awk '{print $1}')"
  elif command -v shasum >/dev/null 2>&1; then
    ACTUAL="$(shasum -a 256 "$ARCHIVE" | awk '{print $1}')"
  else
    echo "error: checksum sidecar exists but no SHA-256 utility is available" >&2
    exit 1
  fi
  [[ "$EXPECTED" == "$ACTUAL" ]] || {
    echo "error: backup checksum mismatch" >&2
    exit 1
  }
fi

# Reject absolute and parent-traversal entries before extracting.
while IFS= read -r entry; do
  case "$entry" in
    /*|../*|*/../*|*/..) echo "error: unsafe archive entry: $entry" >&2; exit 1 ;;
  esac
done < <(tar -tzf "$ARCHIVE")

mkdir -p "$PARENT"
tar -C "$PARENT" -xzf "$ARCHIVE"
echo "restored into $PARENT"
echo "run: bettermq doctor --data-dir <restored-directory>"
echo "restart bettermq with --data-dir pointing at the restored directory"
echo "if using shared meta / Slate, restore those stores separately before start"
