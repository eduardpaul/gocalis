#!/usr/bin/env bash
#
# install.sh — build gocalis and register it as a systemd service that starts
# automatically at boot. Run from the repo root with sudo:
#
#   sudo ./scripts/install.sh
#
set -euo pipefail

SERVICE_NAME="gocalis"
UNIT_PATH="/etc/systemd/system/${SERVICE_NAME}.service"

# This script lives in <repo>/scripts; the app directory is its parent.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
APP_DIR="$(cd -- "$SCRIPT_DIR/.." >/dev/null 2>&1 && pwd)"
BIN_PATH="$APP_DIR/gocalis"

# --- Must run as root ---------------------------------------------------------
if [[ ${EUID} -ne 0 ]]; then
  echo "ERROR: this installer must be run as root." >&2
  echo "Usage: sudo ./install.sh" >&2
  exit 1
fi

# The service runs as the user that OWNS the repo (not root) so it keeps access
# to that user's Go module cache (which holds the sherpa-onnx shared libraries
# the binary links against via rpath) and to the audio devices.
RUN_USER="$(stat -c '%U' "$APP_DIR")"
RUN_GROUP="$(id -gn "$RUN_USER")"
RUN_HOME="$(getent passwd "$RUN_USER" | cut -d: -f6)"

echo "==> gocalis installer"
echo "    repo dir : $APP_DIR"
echo "    run user : $RUN_USER ($RUN_GROUP)"
echo "    binary   : $BIN_PATH"

# Helper: run a command as the repo-owning user with a login shell (so their
# PATH picks up go/npm from tools like g, gvm, nvm, asdf, etc.).
#
# The default ~/.bashrc on Debian/Raspberry Pi OS returns early for
# non-interactive shells, so a tarball Go install under /usr/local/go/bin (and a
# per-user ~/go/bin) may not be on PATH here even though it works in an
# interactive terminal. Prepend the common locations so builds still find go/npm.
run_as_user() {
  sudo -u "$RUN_USER" -H bash -lc "export PATH=\"/usr/local/go/bin:\$HOME/go/bin:\$HOME/.local/bin:\$PATH\"; $1"
}

# --- Build --------------------------------------------------------------------
if ! run_as_user 'command -v go >/dev/null'; then
  echo "ERROR: 'go' was not found in ${RUN_USER}'s PATH." >&2
  echo "       Install Go for that user, or build the 'gocalis' binary manually first." >&2
  exit 1
fi

# The Go binary embeds the React dashboard via go:embed, so dist must exist.
if [[ ! -d "$APP_DIR/internal/webserver/dist" ]]; then
  echo "==> Web dashboard (internal/webserver/dist) missing; building it..."
  if run_as_user 'command -v npm >/dev/null'; then
    run_as_user "cd '$APP_DIR/web' && npm install && npm run build"
    run_as_user "rm -rf '$APP_DIR/internal/webserver/dist' && cp -r '$APP_DIR/web/dist' '$APP_DIR/internal/webserver/dist'"
  else
    echo "ERROR: dashboard not built and 'npm' not found in ${RUN_USER}'s PATH." >&2
    echo "       Build it manually (see readme_install.md), then re-run this installer." >&2
    exit 1
  fi
fi

echo "==> Building gocalis binary (this can take a few minutes on a Pi)..."
run_as_user "cd '$APP_DIR' && CGO_ENABLED=1 go build -o '$BIN_PATH' ./cmd"

if [[ ! -x "$BIN_PATH" ]]; then
  echo "ERROR: build did not produce an executable at $BIN_PATH" >&2
  exit 1
fi

# --- Free the ports if a manual instance is still running ---------------------
# The dev workflow starts gocalis manually from /tmp; stop it so the service can
# bind :9090 / :8080.
if pgrep -f '/tmp/gocalis -channel' >/dev/null 2>&1; then
  echo "==> Stopping stray manual /tmp/gocalis instance..."
  pkill -TERM -f '/tmp/gocalis -channel' 2>/dev/null || true
  sleep 2
fi

# --- Install the systemd unit -------------------------------------------------
echo "==> Writing systemd unit: $UNIT_PATH"
cat > "$UNIT_PATH" <<EOF
[Unit]
Description=Gocalis local speech agent (WebRTC proxy + Node-RED WebSocket API)
After=network-online.target sound.target
Wants=network-online.target

[Service]
Type=simple
User=${RUN_USER}
Group=${RUN_GROUP}
SupplementaryGroups=audio
WorkingDirectory=${APP_DIR}
Environment=HOME=${RUN_HOME}
Environment=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ExecStart=${BIN_PATH} -config config.yaml -ws-addr :9090 -http-addr :8080
Restart=on-failure
RestartSec=3
# ONNX model loading can be slow on first start; allow generous startup time.
TimeoutStartSec=120

[Install]
WantedBy=multi-user.target
EOF

echo "==> Reloading systemd and enabling service at boot..."
systemctl daemon-reload
systemctl enable --now "${SERVICE_NAME}.service"

echo
echo "==> Installed. Current status:"
systemctl --no-pager --full status "${SERVICE_NAME}.service" || true
echo
echo "Follow logs with:  sudo journalctl -u ${SERVICE_NAME} -f"
echo "Restart with:      sudo ./scripts/restart.sh"
echo "Uninstall with:    sudo ./scripts/uninstall.sh"
