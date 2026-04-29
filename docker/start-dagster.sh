#!/usr/bin/env sh
set -eu

daemon_pid=""
webserver_pid=""

shutdown() {
  if [ -n "$webserver_pid" ] && kill -0 "$webserver_pid" 2>/dev/null; then
    kill "$webserver_pid" 2>/dev/null || true
  fi
  if [ -n "$daemon_pid" ] && kill -0 "$daemon_pid" 2>/dev/null; then
    kill "$daemon_pid" 2>/dev/null || true
  fi
}

trap shutdown INT TERM EXIT

uv run python -m personal_data_warehouse.dagster_bootstrap

uv run dagster-daemon run -m personal_data_warehouse.definitions &
daemon_pid="$!"

uv run dagster-webserver \
  -h 0.0.0.0 \
  -p "${PORT:-3000}" \
  -m personal_data_warehouse.definitions &
webserver_pid="$!"

while :; do
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
