#!/usr/bin/env bash
#
# restart.sh — restart the gocalis systemd service. Run from the repo root with sudo:
#
#   sudo ./scripts/restart.sh            # just restart the running service
#   sudo ./scripts/restart.sh --build    # rebuild the binary first, then restart
#
set -euo pipefail

SERVICE_NAME="gocalis"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
APP_DIR="$(cd -- "$SCRIPT_DIR/.." >/dev/null 2>&1 && pwd)"
BIN_PATH="$APP_DIR/gocalis"

if [[ ${EUID} -ne 0 ]]; then
  echo "ERROR: this script must be run as root." >&2
  echo "Usage: sudo ./scripts/restart.sh [--build]" >&2
  exit 1
fi

if [[ ! -f "/etc/systemd/system/${SERVICE_NAME}.service" ]]; then
  echo "ERROR: ${SERVICE_NAME} is not installed. Run 'sudo ./scripts/install.sh' first." >&2
  exit 1
fi

if [[ "${1:-}" == "--build" ]]; then
  RUN_USER="$(stat -c '%U' "$APP_DIR")"
  echo "==> Rebuilding gocalis binary as ${RUN_USER}..."
  if [[ -f "$BIN_PATH" ]]; then
    echo "    before: $(stat -c '%y  %s bytes' "$BIN_PATH")"
  else
    echo "    before: (no existing binary)"
  fi
  # The default ~/.bashrc returns early for non-interactive shells, so prepend
  # the common Go locations so a tarball install (/usr/local/go/bin) is found.
  # -v lists the packages actually recompiled; a fast build with no output just
  # means the Go build cache is up to date and only a relink was needed.
  sudo -u "$RUN_USER" -H bash -lc "export PATH=\"/usr/local/go/bin:\$HOME/go/bin:\$HOME/.local/bin:\$PATH\"; cd '$APP_DIR' && CGO_ENABLED=1 go build -v -o '$BIN_PATH' ./cmd"
  echo "    after:  $(stat -c '%y  %s bytes' "$BIN_PATH")"
fi

echo "==> Restarting ${SERVICE_NAME}..."
systemctl restart "${SERVICE_NAME}.service"

echo "==> Status:"
systemctl --no-pager --full status "${SERVICE_NAME}.service" || true
