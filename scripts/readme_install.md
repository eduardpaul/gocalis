# Running gocalis at boot (systemd)

These scripts build `gocalis` and register it as a **systemd** service so it
starts automatically when the Raspberry Pi boots.

| Script | Purpose | Command (run from repo root) |
| --- | --- | --- |
| `scripts/install.sh` | Build the binary + install & enable the service | `sudo ./scripts/install.sh` |
| `scripts/restart.sh` | Restart the service (optionally rebuild first) | `sudo ./scripts/restart.sh [--build]` |
| `scripts/uninstall.sh` | Stop, disable and remove the service | `sudo ./scripts/uninstall.sh` |

The service is named **`gocalis`** and its unit file is installed at
`/etc/systemd/system/gocalis.service`.

---

## What the service runs

```
gocalis -config config.yaml -ws-addr :9090 -http-addr :8080
```

- **Working directory:** the repo root (so `config.yaml` and `models/` resolve).
- **Ports:** `:9090` (Node-RED WebSocket API) and `:8080` (dashboard HTTP).
- **User:** the account that *owns the repository* — **not** root. This keeps
  the binary's access to that user's Go module cache (which holds the
  sherpa-onnx shared libraries) and to the audio devices (`audio` group).
- **Auto-restart:** on failure, after 3 seconds.

---

## Prerequisites

Run once, on the machine, as the repo-owning user (not root):

- **Go** (1.24+) available in that user's `PATH`.
- **Node.js + npm** — only needed the first time, to build the embedded web
  dashboard (`internal/webserver/dist`). If that folder already exists, npm is
  not required.
- Model files present under `models/` (see the download scripts in `scripts/`).

`install.sh` will build the dashboard automatically if it is missing, then
compile the `gocalis` binary with `CGO_ENABLED=1`.

---

## Install

```bash
cd /home/eduapaul/repos/gocalis
sudo ./scripts/install.sh
```

This will:
1. Detect the repo-owning user and build the binary as that user.
2. Stop any stray manually-launched `/tmp/gocalis` instance (to free the ports).
3. Write and enable `gocalis.service`, then start it immediately.

## Check status / logs

```bash
sudo systemctl status gocalis
sudo journalctl -u gocalis -f
```

## Restart

```bash
sudo ./scripts/restart.sh            # restart with the current binary
sudo ./scripts/restart.sh --build    # recompile, then restart (use after code changes)
```

## Uninstall

```bash
sudo ./scripts/uninstall.sh
```

This removes the service only; the compiled binary and repository are left in
place.

---

## Troubleshooting

- **Port already in use / service won't start:** a manual instance may still be
  running. Stop it with `pkill -f '/tmp/gocalis'` (or the path you launched),
  then `sudo ./scripts/restart.sh`.
- **No audio from the local node:** confirm the run user is in the `audio`
  group (`groups <user>`). The unit already adds `audio` as a supplementary
  group; a reboot or `sudo ./scripts/restart.sh` applies it.
- **`pattern all:dist: no matching files found` during build:** the web
  dashboard wasn't built. Install npm and re-run `sudo ./scripts/install.sh`, or build
  it manually:
  ```bash
  cd web && npm install && npm run build && cd ..
  cp -r web/dist internal/webserver/dist
  ```
- **`go: command not found` during install:** Go isn't in the repo owner's
  login-shell `PATH`. Ensure `go` works when you run it as that user in a fresh
  shell, then re-run the installer.
