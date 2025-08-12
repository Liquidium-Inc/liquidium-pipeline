#!/usr/bin/env bash
set -euo pipefail

# ===== Config =====
GH_USER="Liquidium-Inc"
GH_REPO="liquidium-pipeline"
BRANCH="${BRANCH:-main}"
BIN_NAME="${BIN_NAME:-liquidator}"
INSTALL_DIR="${INSTALL_DIR:-$HOME/.liquidium-pipeline}"
REPO="https://github.com/${GH_USER}/${GH_REPO}.git"
SKIP_RUST="${SKIP_RUST:-false}"
YES="${YES:-false}"
CI="${CI:-false}"

RELEASES_DIR="${INSTALL_DIR%/}/releases"
USER_BIN="$HOME/.local/bin"

usage() {
  cat <<EOF
Liquidator install script (user-only, no sudo).

Usage (install/update):
  curl -fsSL https://raw.githubusercontent.com/${GH_USER}/${GH_REPO}/${BRANCH}/install.sh | bash

Options:
  --branch <name>       (default: ${BRANCH})
  --bin-name <name>     (default: ${BIN_NAME})
  --install-dir <path>  (default: ${INSTALL_DIR})
  --yes                 Skip confirmation

Env:
  SKIP_RUST=true   Skip Rust install
  YES=true         Skip confirmation
  CI=true          Skip confirmation (CI)
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

mkdir -p "$INSTALL_DIR" "$RELEASES_DIR" "$USER_BIN"

# ===== PATH ensure (user shell) =====
ensure_path() {
  case "${SHELL##*/}" in
    zsh) PROFILE="$HOME/.zshrc" ;;
    fish) PROFILE="$HOME/.config/fish/config.fish" ;;
    *) PROFILE="$HOME/.bashrc" ;;
  esac

  if ! echo ":$PATH:" | grep -q ":$USER_BIN:"; then
    echo "export PATH=\"$USER_BIN:\$PATH\"" >> "$PROFILE"
    # Also export for current session
    export PATH="$USER_BIN:$PATH"
  fi
}
ensure_path

# ===== Confirm =====
if [[ "$YES" != "true" && "$CI" != "true" ]]; then
  cat <<EOM
This will:
  - Clone/update: ${REPO} (branch: ${BRANCH})
  - Build binary: ${BIN_NAME}
  - Install to: ${USER_BIN}/${BIN_NAME}
  - Source in: ${INSTALL_DIR}
  - Versioned releases in: ${RELEASES_DIR}
EOM
  ans=""
  if [[ -r /dev/tty ]]; then
    read -rp "Proceed? [Y/n] " ans < /dev/tty || true
  fi
  case "${ans:-}" in
    [Nn]*) echo "Aborted."; exit 1;;
    *) ;;
  esac
fi

command -v git >/dev/null || { echo "git is required (install it via your package manager)"; exit 1; }

# ===== Rust toolchain =====
if [[ "$SKIP_RUST" != "true" ]]; then
  if ! command -v cargo >/dev/null 2>&1; then
    echo "Installing Rust toolchain (user)…"
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --default-toolchain stable
  fi
  export PATH="$HOME/.cargo/bin:$PATH"
fi

command -v cargo >/dev/null || { echo "cargo not found; set SKIP_RUST=false or add ~/.cargo/bin to PATH"; exit 1; }

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

# ===== User config =====
USER_CONFIG_DIR="$HOME/.liquidium-pipeline"
USER_CONFIG_FILE="$USER_CONFIG_DIR/config.env"
mkdir -p "$USER_CONFIG_DIR"

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

# ===== Symlink (user space) =====
ln -sfn "$DST" "$USER_BIN/$BIN_NAME"
hash -r 2>/dev/null || true

echo ""
echo "✅ Installed ${BIN_NAME} @ ${DST}"
echo "➡  Symlinked: $USER_BIN/${BIN_NAME} -> ${DST}"
echo ""
echo "Usage:"
echo "  ${BIN_NAME} run"
echo ""
echo "Other commands:"
echo "  ${BIN_NAME} balance"
echo "  ${BIN_NAME} withdraw <asset_principal> <amount> <to_principal>"
echo "  Added $USER_BIN to PATH in $PROFILE. Reload your shell or 'source $PROFILE'."
echo ""
echo "Update later: re-run the same curl | bash command."