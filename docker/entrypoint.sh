#!/usr/bin/env sh
set -eu

if [ -z "${DAGSTER_POSTGRES_URL:-}" ] && [ -n "${DATABASE_URL:-}" ]; then
  export DAGSTER_POSTGRES_URL="$DATABASE_URL"
fi

if [ -z "${DAGSTER_POSTGRES_URL:-}" ]; then
  echo "DAGSTER_POSTGRES_URL is required. In Coolify, set it directly or provide DATABASE_URL from a Postgres resource." >&2
  exit 1
fi

case "$DAGSTER_POSTGRES_URL" in
  postgres://*)
    export DAGSTER_POSTGRES_URL="postgresql://${DAGSTER_POSTGRES_URL#postgres://}"
    ;;
  postgresql+psycopg2://*)
    export DAGSTER_POSTGRES_URL="postgresql://${DAGSTER_POSTGRES_URL#postgresql+psycopg2://}"
    ;;
esac

exec "$@"
