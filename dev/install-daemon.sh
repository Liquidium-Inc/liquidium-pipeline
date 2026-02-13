#!/usr/bin/env bash
set -euo pipefail

UNIT_NAME="liquidator.service"
SERVICE_USER="liquidator"
SERVICE_GROUP="liquidator"
SOCK_PATH="/run/liquidator/ctl.sock"
RUST_LOG_LEVEL="info"
BIN_PATH=""
ENABLE_ON_BOOT="true"
START_NOW="true"

usage() {
  cat <<EOF
Install/update liquidator as a systemd daemon (Linux only).

Usage:
  ./dev/install-daemon.sh [options]

Options:
  --bin-path <PATH>      Absolute path to liquidator binary (default: command -v liquidator)
  --unit-name <NAME>     systemd unit name (default: liquidator.service)
  --user <NAME>          Service user (default: liquidator)
  --group <NAME>         Service group (default: liquidator)
  --sock-path <PATH>     Control socket path (default: /run/liquidator/ctl.sock)
  --rust-log <LEVEL>     RUST_LOG level (default: info)
  --no-enable            Do not enable unit on boot
  --no-start             Do not start/restart unit now
  -h, --help             Show this help

Examples:
  ./dev/install-daemon.sh
  ./dev/install-daemon.sh --bin-path /usr/local/bin/liquidator
  ./dev/install-daemon.sh --user liquidator --group liquidator
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bin-path)
      BIN_PATH="$2"
      shift 2
      ;;
    --unit-name)
      UNIT_NAME="$2"
      shift 2
      ;;
    --user)
      SERVICE_USER="$2"
      shift 2
      ;;
    --group)
      SERVICE_GROUP="$2"
      shift 2
      ;;
    --sock-path)
      SOCK_PATH="$2"
      shift 2
      ;;
    --rust-log)
      RUST_LOG_LEVEL="$2"
      shift 2
      ;;
    --no-enable)
      ENABLE_ON_BOOT="false"
      shift
      ;;
    --no-start)
      START_NOW="false"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "This installer only supports Linux (systemd)." >&2
  exit 1
fi

if ! command -v systemctl >/dev/null 2>&1; then
  echo "systemctl is required but was not found." >&2
  exit 1
fi

if [[ -z "$BIN_PATH" ]]; then
  BIN_PATH="$(command -v liquidator || true)"
fi

if [[ -z "$BIN_PATH" ]]; then
  echo "Could not find liquidator binary in PATH; pass --bin-path <ABS_PATH>." >&2
  exit 1
fi

if [[ ! -x "$BIN_PATH" ]]; then
  echo "Binary is not executable: $BIN_PATH" >&2
  exit 1
fi

if [[ "$BIN_PATH" != /* ]]; then
  BIN_PATH="$(cd "$(dirname "$BIN_PATH")" && pwd)/$(basename "$BIN_PATH")"
fi

if [[ $EUID -ne 0 ]]; then
  if command -v sudo >/dev/null 2>&1; then
    SUDO_ARGS=(
      --bin-path "$BIN_PATH"
      --unit-name "$UNIT_NAME"
      --user "$SERVICE_USER"
      --group "$SERVICE_GROUP"
      --sock-path "$SOCK_PATH"
      --rust-log "$RUST_LOG_LEVEL"
    )
    if [[ "$ENABLE_ON_BOOT" == "false" ]]; then
      SUDO_ARGS+=(--no-enable)
    fi
    if [[ "$START_NOW" == "false" ]]; then
      SUDO_ARGS+=(--no-start)
    fi
    exec sudo "$0" "${SUDO_ARGS[@]}"
  fi
  echo "Run as root (or install sudo)." >&2
  exit 1
fi

if ! getent group "$SERVICE_GROUP" >/dev/null 2>&1; then
  echo "Creating service group: $SERVICE_GROUP"
  groupadd --system "$SERVICE_GROUP"
fi

if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
  echo "Creating service user: $SERVICE_USER"
  NOLOGIN_BIN="$(command -v nologin || true)"
  if [[ -z "$NOLOGIN_BIN" ]]; then
    NOLOGIN_BIN="/usr/sbin/nologin"
  fi
  useradd \
    --system \
    --gid "$SERVICE_GROUP" \
    --home-dir / \
    --shell "$NOLOGIN_BIN" \
    --no-create-home \
    "$SERVICE_USER"
fi

UNIT_PATH="/etc/systemd/system/$UNIT_NAME"
echo "Writing systemd unit: $UNIT_PATH"
cat >"$UNIT_PATH" <<EOF
[Unit]
Description=Liquidator Daemon
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$SERVICE_USER
Group=$SERVICE_GROUP
RuntimeDirectory=liquidator
# RuntimeDirectoryMode is authoritative for /run/liquidator permissions.
RuntimeDirectoryMode=0770
ExecStart=$BIN_PATH run --sock-path $SOCK_PATH
Restart=always
RestartSec=3
Environment=RUST_LOG=$RUST_LOG_LEVEL

[Install]
WantedBy=multi-user.target
EOF
chmod 0644 "$UNIT_PATH"

echo "Reloading systemd units..."
systemctl daemon-reload

if [[ "$ENABLE_ON_BOOT" == "true" ]]; then
  echo "Enabling $UNIT_NAME..."
  systemctl enable "$UNIT_NAME"
fi

if [[ "$START_NOW" == "true" ]]; then
  echo "Starting/restarting $UNIT_NAME..."
  systemctl restart "$UNIT_NAME"
fi

echo
echo "Installed unit: $UNIT_PATH"
echo "Service: $UNIT_NAME"
echo "Binary: $BIN_PATH"
echo "Socket: $SOCK_PATH"
echo
echo "Useful commands:"
echo "  systemctl status $UNIT_NAME --no-pager"
echo "  journalctl -u $UNIT_NAME -f -o short"
echo "  liquidator tui --sock-path $SOCK_PATH --unit-name $UNIT_NAME"
