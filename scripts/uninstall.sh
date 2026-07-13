#!/usr/bin/env bash
#
# uninstall.sh — stop, disable and remove the gocalis systemd service.
# The built binary and the repository are left untouched. Run from the repo
# root with sudo:
#
#   sudo ./scripts/uninstall.sh
#
set -euo pipefail

SERVICE_NAME="gocalis"
UNIT_PATH="/etc/systemd/system/${SERVICE_NAME}.service"

if [[ ${EUID} -ne 0 ]]; then
  echo "ERROR: this script must be run as root." >&2
  echo "Usage: sudo ./scripts/uninstall.sh" >&2
  exit 1
fi

echo "==> Stopping and disabling ${SERVICE_NAME}..."
systemctl disable --now "${SERVICE_NAME}.service" 2>/dev/null || true

if [[ -f "$UNIT_PATH" ]]; then
  rm -f "$UNIT_PATH"
  echo "    removed $UNIT_PATH"
else
  echo "    no unit file found at $UNIT_PATH"
fi

systemctl daemon-reload
systemctl reset-failed "${SERVICE_NAME}.service" 2>/dev/null || true

echo "==> Uninstalled. The 'gocalis' binary and repo were left in place."
