#!/usr/bin/env bash
#
# betterMQ installer.
#
#   curl -fsSL https://bettermq.com/install | bash
#
# Detects your OS + arch, downloads the matching binary from the
# betterMQ/betterMQ GitHub Releases, verifies sha256 (when checksums.txt is
# published for the release), installs to ~/.bettermq/bin and links it into
# ~/.local/bin/bettermq.
#
# Optional env vars / args:
#   $1                    — "latest" (default) or explicit version like "0.3.1"
#   BETTERMQ_INSTALL_DIR  — base dir (default: $HOME/.bettermq)
#   BETTERMQ_BIN_DIR      — link dir (default: $HOME/.local/bin)
#   BETTERMQ_FORCE=1      — reinstall even if the same version is present
#   BETTERMQ_NO_START=1   — install only; never offer to start the server
#

set -euo pipefail

REPO="betterMQ/betterMQ"
BIN_NAME="bettermq"
RELEASES_URL="https://github.com/$REPO/releases"
INSTALL_DIR="${BETTERMQ_INSTALL_DIR:-$HOME/.bettermq}"
BIN_DIR="${BETTERMQ_BIN_DIR:-$HOME/.local/bin}"
TARGET="${1:-latest}"

# --- helpers ----------------------------------------------------------------

# Brand #1f47f0
if [ -t 1 ] && [ "${TERM:-dumb}" != "dumb" ]; then
  C_BRAND=$'\033[38;2;31;71;240m'
  C_BOLD=$'\033[1m'
  C_DIM=$'\033[2m'
  C_GREEN=$'\033[32m'
  C_RESET=$'\033[0m'
else
  C_BRAND= C_BOLD= C_DIM= C_GREEN= C_RESET=
fi

die() { echo "ERROR: $*" >&2; exit 1; }
info() { printf '%s→%s %s\n' "$C_DIM" "$C_RESET" "$*"; }
ok() { printf '%s✓%s %s\n' "$C_GREEN" "$C_RESET" "$*"; }

# Wordmark: better (default) + MQ (brand #1f47f0)
print_logo() {
  if [ -n "$C_BRAND" ]; then
    printf '     better%sMQ%s\n' "$C_BRAND" "$C_RESET"
  else
    printf '     betterMQ\n'
  fi
}

print_welcome() {
  [ -t 1 ] || return 0
  printf '\n'
  print_logo
  printf '\n'
  printf '%s%s     Http Messaging & Scheduling%s\n' "$C_DIM" "$C_BOLD" "$C_RESET"
  printf '\n'
  printf '%s  installing self-hosted HTTP message broker%s\n' "$C_DIM" "$C_RESET"
  printf '%s  ─────────────────────────────────────%s\n\n' "$C_DIM" "$C_RESET"
}

print_success() {
  local ver="$1"
  [ -t 1 ] || {
    echo
    echo "bettermq $ver installed"
    return 0
  }
  printf '\n'
  printf '%s%s✓%s better%sMQ%s %s%s%s installed%s\n\n' \
    "$C_GREEN" "$C_BOLD" "$C_RESET" "$C_BRAND" "$C_RESET" "$C_GREEN" "$C_BOLD" "$ver" "$C_RESET"
}

if command -v curl >/dev/null 2>&1; then
  dl() { curl -fsSL "$1" -o "$2"; }
elif command -v wget >/dev/null 2>&1; then
  dl() { wget -q "$1" -O "$2"; }
else
  die "curl or wget required"
fi

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    echo ""
  fi
}

have_tty() {
  [ "${BETTERMQ_NO_START:-}" != "1" ] && [ -r /dev/tty ] && [ -w /dev/tty ]
}

print_welcome

# --- detect platform ----------------------------------------------------------

case "$(uname -s)" in
  Darwin) os="macos" ;;
  Linux)  os="linux" ;;
  *) die "unsupported OS: $(uname -s) — download manually from $RELEASES_URL" ;;
esac

case "$(uname -m)" in
  x86_64|amd64) arch="amd64" ;;
  arm64|aarch64) arch="arm64" ;;
  *) die "unsupported architecture: $(uname -m)" ;;
esac

# Rosetta 2 — prefer native arm64 on Apple Silicon.
if [ "$os" = "macos" ] && [ "$arch" = "amd64" ] \
  && [ "$(sysctl -n sysctl.proc_translated 2>/dev/null)" = "1" ]; then
  arch="arm64"
fi

asset="$BIN_NAME-$os-$arch"
if [ "$os" = "linux" ] && [ "$arch" = "arm64" ]; then
  die "no prebuilt linux-arm64 binary yet — build from source:
  git clone https://github.com/$REPO.git && cd ${REPO#*/}
  cargo build -p broker-server --release"
fi
info "Platform: $os-$arch"

# --- resolve version ----------------------------------------------------------

version=""
if [ "$TARGET" = "latest" ]; then
  if command -v curl >/dev/null 2>&1; then
    final_url=$(curl -fsSLI -o /dev/null -w '%{url_effective}' "$RELEASES_URL/latest" 2>/dev/null || true)
    version=$(printf '%s' "$final_url" | sed -nE 's#.*/releases/tag/v([0-9]+\.[0-9]+\.[0-9]+)$#\1#p')
  fi
  [ -n "$version" ] || die "could not resolve latest version from $RELEASES_URL"
else
  version="${TARGET#v}"
fi

tag="v$version"
asset_base="$RELEASES_URL/download/$tag"
info "Version: $version"

# --- skip when already installed ----------------------------------------------

version_file="$INSTALL_DIR/bin/$BIN_NAME.version"
if [ "${BETTERMQ_FORCE:-}" != "1" ] \
  && [ -f "$version_file" ] \
  && [ "$(cat "$version_file" 2>/dev/null)" = "$version" ] \
  && [ -x "$INSTALL_DIR/bin/$BIN_NAME" ]; then
  print_success "$version"
  printf '  %sAlready installed — nothing to do.%s\n' "$C_DIM" "$C_RESET"
  printf '  %sForce reinstall:%s BETTERMQ_FORCE=1 curl -fsSL https://bettermq.com/install | bash\n\n' "$C_DIM" "$C_RESET"
  exit 0
fi

# --- download + verify ----------------------------------------------------------

mkdir -p "$INSTALL_DIR/bin"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

zip_path="$tmp/$asset.zip"
info "Downloading $asset.zip…"
dl "$asset_base/$asset.zip" "$zip_path" || die "download failed: $asset_base/$asset.zip"

# checksums.txt is published from v0.4+ — verify when available.
if dl "$asset_base/checksums.txt" "$tmp/checksums.txt" 2>/dev/null; then
  expected=$(grep "  $asset.zip\$" "$tmp/checksums.txt" | awk '{print $1}' || true)
  actual=$(sha256 "$zip_path")
  if [ -n "$expected" ] && [ -n "$actual" ]; then
    [ "$expected" = "$actual" ] || die "checksum mismatch for $asset.zip (expected $expected, got $actual)"
    ok "Verified sha256"
  fi
else
  info "No checksums.txt for $tag — skipping verification."
fi

if command -v unzip >/dev/null 2>&1; then
  unzip -oq "$zip_path" -d "$tmp"
elif command -v ditto >/dev/null 2>&1; then
  ditto -xk "$zip_path" "$tmp"
else
  die "unzip required"
fi

[ -f "$tmp/$asset" ] || die "binary $asset missing from archive"
chmod +x "$tmp/$asset"

final_bin="$INSTALL_DIR/bin/$BIN_NAME"
mv -f "$tmp/$asset" "$final_bin"
# Clear quarantine if a browser ever touched the file (no-op otherwise).
if [ "$os" = "macos" ]; then
  xattr -d com.apple.quarantine "$final_bin" 2>/dev/null || true
fi
printf '%s\n' "$version" > "$version_file"
ok "Installed binary → $final_bin"

# --- link into PATH ----------------------------------------------------------

mkdir -p "$BIN_DIR"
ln -sf "$final_bin" "$BIN_DIR/$BIN_NAME"
ok "Linked → $BIN_DIR/$BIN_NAME"

case ":$PATH:" in
  *:"$BIN_DIR":*) on_path=1 ;;
  *) on_path=0 ;;
esac

print_success "$version"
if [ "$on_path" = "1" ]; then
  printf '  %sStart%s   %s%s serve%s\n' "$C_DIM" "$C_RESET" "$C_BOLD" "$BIN_NAME" "$C_RESET"
else
  printf '  %sStart%s   %s%s serve%s\n' "$C_DIM" "$C_RESET" "$C_BOLD" "$BIN_DIR/$BIN_NAME" "$C_RESET"
  printf '\n'
  printf '  %s(%s is not on $PATH — add to your shell rc:)%s\n' "$C_DIM" "$BIN_DIR" "$C_RESET"
  printf '  %secho '\''export PATH="%s:$PATH"'\'' >> ~/.zshrc%s\n' "$C_DIM" "$BIN_DIR" "$C_RESET"
fi
printf '  %sPanel%s   http://localhost:8080/panel/\n' "$C_DIM" "$C_RESET"
printf '  %sDocs%s    http://localhost:8080/docs\n' "$C_DIM" "$C_RESET"
printf '  %sGitHub%s  https://github.com/%s\n' "$C_DIM" "$C_RESET" "$REPO"
printf '\n'

if have_tty; then
  printf "Start the server now? [Y/n]: " > /dev/tty
  read -r reply < /dev/tty || reply=""
  case "${reply:-y}" in
    [Yy]*|"")
      printf '%sStarting betterMQ…%s  %sPress Ctrl-C to stop.%s\n\n' "$C_BRAND" "$C_RESET" "$C_DIM" "$C_RESET"
      exec "$final_bin" serve
      ;;
    *)
      echo "Run it later with: $BIN_NAME serve"
      ;;
  esac
fi
