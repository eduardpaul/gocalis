#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
dc() { docker compose -f tools/simulator/compose.yaml "$@"; }
mkdir -p tools/simulator/runs
dc build generate engine-tests transport-tests
dc run --rm --no-deps generate
dc up -d simulator go2rtc
dc run --rm --no-deps engine-tests
dc run --rm --no-deps frontend-checks
dc run --rm --no-deps -e GOCALIS_SIMULATOR= transport-tests go test -race -timeout=2m ./...
dc run --rm --no-deps scenario
dc run --rm --no-deps transport-tests 2>&1 | tee tools/simulator/runs/transport.log
