#!/usr/bin/env sh
set -eu

# Run the Dagster control plane (gRPC code server + daemon + webserver) inside a
# single container. The code location is served by ONE long-lived gRPC server;
# the daemon and webserver connect to it as clients via workspace.yaml rather
# than each spawning their own managed code server from a `-m` module target.
#
# Why: a `-m` target makes each parent (daemon, webserver) spawn a *managed*
# gRPC code server supervised with `--heartbeat --heartbeat-timeout`. Under host
# load the parent cannot ping within the timeout, so the managed server logs
# "No heartbeat received in 20 seconds, shutting down" and is relaunched,
# reloading all definitions in a tight loop. A standalone `dagster api grpc`
# server started WITHOUT --heartbeat never self-terminates, eliminating that
# churn, and lets the daemon/webserver restart without reloading code.

GRPC_HOST="127.0.0.1"
GRPC_PORT="${DAGSTER_GRPC_PORT:-4000}"
WORKSPACE="${DAGSTER_WORKSPACE:-/app/workspace.yaml}"

grpc_pid=""
daemon_pid=""
webserver_pid=""

shutdown() {
  for pid in "$webserver_pid" "$daemon_pid" "$grpc_pid"; do
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
}

trap shutdown INT TERM EXIT

uv run python -m personal_data_warehouse.dagster_bootstrap

# Long-lived code server. No --heartbeat => it does not self-terminate on missed
# pings. The daemon/webserver reach it via workspace.yaml (grpc_server).
uv run dagster api grpc \
  --host "0.0.0.0" \
  --port "$GRPC_PORT" \
  --module-name personal_data_warehouse.definitions \
  --location-name personal_data_warehouse &
grpc_pid="$!"

# Wait for the code server to finish importing definitions and accept requests
# before starting the daemon/webserver (they retry, but this avoids noisy
# startup errors and a race on first load).
i=0
until uv run dagster api grpc-health-check --port "$GRPC_PORT" --host "$GRPC_HOST" 2>/dev/null; do
  if ! kill -0 "$grpc_pid" 2>/dev/null; then
    echo "dagster gRPC code server exited during startup" >&2
    wait "$grpc_pid"
    exit 1
  fi
  i=$((i + 1))
  if [ "$i" -ge 120 ]; then
    echo "dagster gRPC code server did not become healthy within 120s" >&2
    exit 1
  fi
  sleep 1
done

uv run dagster-daemon run -w "$WORKSPACE" &
daemon_pid="$!"

uv run dagster-webserver \
  -h 0.0.0.0 \
  -p "${PORT:-3000}" \
  -w "$WORKSPACE" &
webserver_pid="$!"

while :; do
  if ! kill -0 "$grpc_pid" 2>/dev/null; then
    wait "$grpc_pid"
    exit $?
  fi
  if ! kill -0 "$daemon_pid" 2>/dev/null; then
    wait "$daemon_pid"
    exit $?
  fi
  if ! kill -0 "$webserver_pid" 2>/dev/null; then
    wait "$webserver_pid"
    exit $?
  fi
  sleep 2
done
