#!/usr/bin/env bash
set -euo pipefail

# ===== Config =====
GH_USER="Liquidium-Inc"
GH_REPO="liquidium-pipeline"
BRANCH="${BRANCH:-}"
TAG="${TAG:-}"
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
  curl -fsSL https://raw.githubusercontent.com/${GH_USER}/${GH_REPO}/main/install.sh | bash

Options:
  --tag <tag>           Install exact git tag (overrides default latest resolution)
  --branch <name>       Install explicit branch (dev/testing)
  --bin-name <name>     (default: ${BIN_NAME})
  --install-dir <path>  (default: ${INSTALL_DIR})

Env:
  TAG=<tag>        Same as --tag
  BRANCH=<name>    Same as --branch
  SKIP_RUST=true   Skip Rust install
EOF
  exit 1
}

resolve_latest_release_tag() {
  local api_url response compact tag
  api_url="https://api.github.com/repos/${GH_USER}/${GH_REPO}/releases/latest"

  response="$(curl -fsSL "$api_url" 2>/dev/null || true)"
  [[ -n "$response" ]] || return 1

  compact="$(printf '%s' "$response" | tr -d '\n')"
  tag="$(printf '%s' "$compact" | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]\+\)".*/\1/p' | head -n1)"
  [[ -n "$tag" ]] || return 1

  printf '%s\n' "$tag"
}

resolve_latest_semver_tag() {
  local tag
  tag="$(
    git ls-remote --tags --refs --sort='-v:refname' "$REPO" 2>/dev/null \
      | awk '{print $2}' \
      | sed 's|refs/tags/||' \
      | grep -E '^(v)?[0-9]+(\.[0-9]+){2}([.-][0-9A-Za-z.-]+)?$' \
      | head -n1
  )"
  [[ -n "$tag" ]] || return 1
  printf '%s\n' "$tag"
}

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --tag) TAG="$2"; shift 2;;
    --branch) BRANCH="$2"; shift 2;;
    --bin-name) BIN_NAME="$2"; shift 2;;
    --install-dir) INSTALL_DIR="$2"; shift 2;;
    -h|--help) usage;;
    *) echo "Unknown arg: $1"; usage;;
  esac
done

if [[ -n "$TAG" && -n "$BRANCH" ]]; then
  echo "Error: --tag and --branch are mutually exclusive. Choose one."
  exit 1
fi

REF_MODE=""
SELECTED_REF=""
SOURCE_MODE=""

if [[ -n "$TAG" ]]; then
  REF_MODE="tag"
  SELECTED_REF="$TAG"
  SOURCE_MODE="tag"
elif [[ -n "$BRANCH" ]]; then
  REF_MODE="branch"
  SELECTED_REF="$BRANCH"
  SOURCE_MODE="branch"
else
  latest_release_tag="$(resolve_latest_release_tag || true)"
  if [[ -n "$latest_release_tag" ]]; then
    REF_MODE="tag"
    SELECTED_REF="$latest_release_tag"
    SOURCE_MODE="latest->release-tag"
  else
    latest_semver_tag="$(resolve_latest_semver_tag || true)"
    if [[ -n "$latest_semver_tag" ]]; then
      REF_MODE="tag"
      SELECTED_REF="$latest_semver_tag"
      SOURCE_MODE="latest->semver-tag-fallback"
      echo "⚠ Could not resolve latest GitHub release tag; falling back to highest semver tag: $SELECTED_REF"
    else
      echo "Error: could not resolve a latest version (release tag or semver tag)."
      echo "Try again later or pass --tag <tag> explicitly."
      exit 1
    fi
  fi
fi

echo "Install source mode: $SOURCE_MODE"
echo "Resolved ref: $SELECTED_REF"

# ===== Native toolchain (cc) =====
if command -v cc >/dev/null 2>&1; then
  echo "C compiler found: $(cc --version | head -n1)"
else
  if command -v apt-get >/dev/null 2>&1; then
    echo "On Debian/Ubuntu, install build tools:"
    echo "  sudo apt-get update && sudo apt-get install -y build-essential"
    echo ""
    exit 1
  elif command -v yum >/dev/null 2>&1; then
    echo "On RHEL/CentOS, install build tools:"
    echo "  sudo yum groupinstall 'Development Tools'"
    echo ""
    exit 1
  elif command -v apk >/dev/null 2>&1; then
    echo "On Alpine, install build tools:"
    echo "  apk add build-base"
    echo ""
    exit 1
  else
    echo "No C compiler found and unable to auto-install build tools. Please install a C compiler (gcc/clang)."
    exit 1
  fi
fi

command -v git >/dev/null || { echo "git is required (install it via your package manager)"; exit 1; }

# ===== SQLite dev headers (libsqlite3-dev) =====
if command -v pkg-config >/dev/null 2>&1; then
  if ! pkg-config --exists sqlite3 >/dev/null 2>&1; then
    if command -v apt-get >/dev/null 2>&1; then
      echo "On Debian/Ubuntu, install SQLite dev headers:"
      echo "  sudo apt-get update && sudo apt-get install -y libsqlite3-dev"
      echo ""
      exit 1
    elif command -v yum >/dev/null 2>&1; then
      echo "On RHEL/CentOS, install SQLite dev headers:"
      echo "  sudo yum install -y sqlite-devel"
      echo ""
      exit 1
    elif command -v apk >/dev/null 2>&1; then
      echo "On Alpine, install SQLite dev headers:"
      echo "  apk add sqlite-dev"
      echo ""
      exit 1
    elif command -v brew >/dev/null 2>&1; then
      echo "On macOS, install SQLite dev headers:"
      echo "  brew install sqlite"
      echo ""
      exit 1
    else
      echo "SQLite dev headers not found. Install libsqlite3-dev and retry."
      exit 1
    fi
  fi
fi

# ===== Rust toolchain =====
if [[ "$SKIP_RUST" != "true" ]]; then
  if ! command -v cargo >/dev/null 2>&1; then
    echo "Installing Rust toolchain (user)…"
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --default-toolchain 1.91.0
  fi
  export PATH="$HOME/.cargo/bin:$PATH"
fi

command -v cargo >/dev/null || { echo "cargo not found; set SKIP_RUST=false or add ~/.cargo/bin to PATH"; exit 1; }

# ===== Clone or update the repo =====
if [[ -d "$REPO_DIR/.git" ]]; then
  echo "Updating repository..."
  git -C "$REPO_DIR" fetch --all --tags -q
else
  echo "Cloning repository..."
  git clone "$REPO" "$REPO_DIR"
fi

if [[ "$REF_MODE" == "branch" ]]; then
  echo "Checking out branch: $SELECTED_REF"
  git -C "$REPO_DIR" checkout "$SELECTED_REF" -q
  git -C "$REPO_DIR" pull -q --rebase origin "$SELECTED_REF"
else
  echo "Checking out tag (detached): $SELECTED_REF"
  if ! git -C "$REPO_DIR" rev-parse -q --verify "refs/tags/$SELECTED_REF" >/dev/null; then
    git -C "$REPO_DIR" fetch -q origin "refs/tags/$SELECTED_REF:refs/tags/$SELECTED_REF" || {
      echo "Error: tag '$SELECTED_REF' not found in repository."
      exit 1
    }
  fi
  git -C "$REPO_DIR" checkout --detach "tags/$SELECTED_REF" -q
fi

# ===== User config =====
USER_CONFIG_DIR="$HOME/.liquidium-pipeline"
USER_CONFIG_FILE="$USER_CONFIG_DIR/config.env"
mkdir -p "$USER_CONFIG_DIR"
mkdir -p "$RELEASES_DIR" "$USER_BIN"

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
echo "📌 Source: ${SOURCE_MODE} (${SELECTED_REF})"
echo "🔖 Commit: ${GITSHA}"
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
