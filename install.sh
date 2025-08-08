#!/usr/bin/env bash
set -euo pipefail

# ===== Config =====
GH_USER="Liquidium-Inc"
GH_REPO="liquidium-pipeline"
BRANCH="${BRANCH:-main}"
BIN_NAME="${BIN_NAME:-liquidator}"   # default binary name
INSTALL_DIR="${INSTALL_DIR:-/opt/liquidator}"
REPO="https://github.com/${GH_USER}/${GH_REPO}.git"
SKIP_RUST="${SKIP_RUST:-false}"
YES="${YES:-false}"
CI="${CI:-false}"

RELEASES_DIR="${INSTALL_DIR%/}/releases"

usage() {
  cat <<EOF
Liquidator install script (no systemd).

Usage (install/update):
  curl -fsSL https://raw.githubusercontent.com/${GH_USER}/${GH_REPO}/${BRANCH}/install.sh | sudo bash

Options:
  --branch <name>       (default: ${BRANCH})
  --bin-name <name>     (default: ${BIN_NAME})
  --install-dir <path>  (default: ${INSTALL_DIR})
  --yes                 Skip confirmation

Environment variables:
  SKIP_RUST=true   Skip Rust install
  YES=true         Skip confirmation
  CI=true          Skip confirmation (for CI)
EOF
  exit 1
}

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --branch) BRANCH="$2"; shift 2;;
    --bin-name) BIN_NAME="$2"; shift 2;;
    --install-dir) INSTALL_DIR="$2"; shift 2;;
    --yes) YES="true"; shift;;
    -h|--help) usage;;
    *) echo "Unknown arg: $1"; usage;;
  esac
done

if [[ $EUID -ne 0 ]]; then
  echo "Run as root (sudo)."
  exit 1
fi

mkdir -p "$INSTALL_DIR" "$RELEASES_DIR"

# ===== Confirm (works with curl | bash, defaults to Yes) =====
if [[ "$YES" != "true" && "$CI" != "true" ]]; then
  cat <<EOM
This will:
  - Clone/update: ${REPO} (branch: ${BRANCH})
  - Build binary: ${BIN_NAME}
  - Install to: /usr/local/bin/${BIN_NAME}
  - Source in: ${INSTALL_DIR}
  - Versioned releases in: ${RELEASES_DIR}
EOM

  ans=""
  if [[ -r /dev/tty ]]; then
    read -rp "Proceed? [Y/n] " ans < /dev/tty || true
  else
    ans=""
  fi

  case "${ans:-}" in
    [Nn]*) echo "Aborted."; exit 1;;
    *) ;;  # default Yes
  esac
fi

# ===== Deps (Debian/Ubuntu best-effort) =====
if [[ -f /etc/debian_version ]]; then
  apt-get update -y
  apt-get install -y build-essential pkg-config libssl-dev cmake git curl
fi

command -v git >/dev/null || { echo "git is required"; exit 1; }

# ===== Rust toolchain =====
if [[ "$SKIP_RUST" != "true" ]]; then
  if ! command -v cargo >/dev/null 2>&1; then
    echo "Installing Rust toolchain..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --default-toolchain stable
  fi
  export PATH="$PATH:$HOME/.cargo/bin:/root/.cargo/bin"
fi

command -v cargo >/dev/null || { echo "cargo not found"; exit 1; }

# ===== Clone or update the repo =====
if [[ -d "$INSTALL_DIR/.git" ]]; then
  echo "Updating repository..."
  git -C "$INSTALL_DIR" fetch --all -q
  git -C "$INSTALL_DIR" checkout "$BRANCH" -q
  git -C "$INSTALL_DIR" pull -q --rebase
else
  echo "Cloning repository..."
  rm -rf "${INSTALL_DIR:?}"/*
  git clone --branch "$BRANCH" --depth 1 "$REPO" "$INSTALL_DIR"
fi

# ===== Update configs =====

# Ensure user config directory exists
USER_CONFIG_DIR="$HOME/.config/liquidator"
USER_CONFIG_FILE="$USER_CONFIG_DIR/config.env"
mkdir -p "$USER_CONFIG_DIR"

# Copy default config.env from repo if missing
if [[ ! -f "$USER_CONFIG_FILE" ]]; then
  if [[ -f "$INSTALL_DIR/config.env" ]]; then
    cp "$INSTALL_DIR/config.env" "$USER_CONFIG_FILE"
    chmod 600 "$USER_CONFIG_FILE"
    echo "✅ Created default config at $USER_CONFIG_FILE"
  else
    echo "⚠ No config.env found in repo — skipping"
  fi
else
  echo "ℹ Config already exists at $USER_CONFIG_FILE (not overwritten)"
fi

# ===== Build =====
echo "Building in release mode..."
pushd "$INSTALL_DIR" >/dev/null
if [[ -f Cargo.lock ]]; then
  cargo build --release --locked --bin "$BIN_NAME"
else
  cargo build --release --bin "$BIN_NAME"
fi
GITSHA="$(git rev-parse --short HEAD)"
SRC="$INSTALL_DIR/target/release/$BIN_NAME"
[[ -f "$SRC" ]] || { echo "Build failed: $SRC not found"; exit 1; }
DST="$RELEASES_DIR/${BIN_NAME}-${GITSHA}"
install -m 0755 "$SRC" "$DST"
popd >/dev/null

# ===== Symlink toggle =====
ln -sfn "$DST" "/usr/local/bin/$BIN_NAME"
hash -r 2>/dev/null || true

echo ""
echo "✅ Installed ${BIN_NAME} @ ${DST}"
echo "➡  Symlinked: /usr/local/bin/${BIN_NAME} -> ${DST}"
echo ""
echo "Usage:"
echo "  ${BIN_NAME} run"
echo ""
echo "Other commands:"
echo "  ${BIN_NAME} balance"
echo "  ${BIN_NAME} withdraw <asset_principal> <amount> <to_principal>"
echo ""
echo "Update later: re-run the same curl | bash command."