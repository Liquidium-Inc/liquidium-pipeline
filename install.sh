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

RELEASES_DIR="${INSTALL_DIR%/}/releases"
USER_BIN="$HOME/.local/bin"
REPO_DIR="${INSTALL_DIR%/}/repo"

usage() {
  cat <<EOF
Liquidator install script (user-only, no sudo).

Usage (install/update):
  curl -fsSL https://raw.githubusercontent.com/${GH_USER}/${GH_REPO}/${BRANCH}/install.sh | bash

Options:
  --branch <name>       (default: ${BRANCH})
  --bin-name <name>     (default: ${BIN_NAME})
  --install-dir <path>  (default: ${INSTALL_DIR})

Env:
  SKIP_RUST=true   Skip Rust install
EOF
  exit 1
}

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --branch) BRANCH="$2"; shift 2;;
    --bin-name) BIN_NAME="$2"; shift 2;;
    --install-dir) INSTALL_DIR="$2"; shift 2;;
    -h|--help) usage;;
    *) echo "Unknown arg: $1"; usage;;
  esac
done

# Safe reset of repo dir and releases; keep INSTALL_DIR for user config and other data
rm -rf "$REPO_DIR" "$RELEASES_DIR"
mkdir -p "$INSTALL_DIR" "$REPO_DIR" "$RELEASES_DIR" "$USER_BIN"

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
if [[ -d "$REPO_DIR/.git" ]]; then
  echo "Updating repository..."
  git -C "$REPO_DIR" fetch --all -q
  git -C "$REPO_DIR" checkout "$BRANCH" -q
  git -C "$REPO_DIR" pull -q --rebase
else
  echo "Cloning repository..."
  git clone --branch "$BRANCH" --depth 1 "$REPO" "$REPO_DIR"
fi

# ===== User config =====
USER_CONFIG_DIR="$HOME/.liquidium-pipeline"
USER_CONFIG_FILE="$USER_CONFIG_DIR/config.env"
mkdir -p "$USER_CONFIG_DIR"

if [[ ! -f "$USER_CONFIG_FILE" ]]; then
  if [[ -f "$REPO_DIR/config.env" ]]; then
    cp "$REPO_DIR/config.env" "$USER_CONFIG_FILE"
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
pushd "$REPO_DIR" >/dev/null
if [[ -f Cargo.lock ]]; then
  cargo build --release --locked --bin "$BIN_NAME"
else
  cargo build --release --bin "$BIN_NAME"
fi
GITSHA="$(git rev-parse --short HEAD)"
SRC="$REPO_DIR/target/release/$BIN_NAME"
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
echo "  ${BIN_NAME} withdraw"
if [[ -n "${PROFILE:-}" ]]; then
  echo "  Added $USER_BIN to PATH in $PROFILE. Reload your shell or 'source $PROFILE'."
else
  echo "  Ensure $USER_BIN is in your PATH (for example by adding 'export PATH=\"$USER_BIN:\$PATH\"' to ~/.bashrc or ~/.zshrc)."
fi
echo ""
echo "Update later: re-run the same curl | bash command."