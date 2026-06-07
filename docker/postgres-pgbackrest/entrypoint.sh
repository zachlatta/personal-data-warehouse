#!/usr/bin/env bash
set -Eeuo pipefail

readonly DEFAULT_PGDATA="/var/lib/postgresql/${PG_MAJOR:-18}/docker"
readonly PGBACKREST_CONFIG_PATH="${PGBACKREST_CONFIG:-/etc/pgbackrest/pgbackrest.conf}"

log() {
  printf '[pdw-postgres-pgbackrest] %s\n' "$*" >&2
}

bool_enabled() {
  local value="${1:-}"
  case "${value,,}" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

require_env() {
  local name value missing=0
  for name in "$@"; do
    value="${!name:-}"
    if [ -z "$value" ]; then
      log "missing required environment variable: $name"
      missing=1
    fi
  done
  if [ "$missing" -ne 0 ]; then
    exit 1
  fi
}

append_config_if_set() {
  local option="$1"
  local env_name="$2"
  local value="${!env_name:-}"
  if [ -n "$value" ]; then
    printf '%s=%s\n' "$option" "$value" >> "$PGBACKREST_CONFIG_PATH"
  fi
}

render_pgbackrest_config() {
  local cipher_type="${PGBACKREST_REPO1_CIPHER_TYPE:-aes-256-cbc}"

  require_env \
    PGBACKREST_REPO1_S3_BUCKET \
    PGBACKREST_REPO1_S3_ENDPOINT \
    PGBACKREST_REPO1_S3_KEY \
    PGBACKREST_REPO1_S3_KEY_SECRET

  if [ "$cipher_type" != "none" ]; then
    require_env PGBACKREST_REPO1_CIPHER_PASS
  fi

  install -d -m 0750 -o postgres -g postgres "$(dirname "$PGBACKREST_CONFIG_PATH")"
  install -d -m 0750 -o postgres -g postgres "${PGBACKREST_LOG_PATH:-/var/log/pgbackrest}"
  install -d -m 0750 -o postgres -g postgres "${PGBACKREST_SPOOL_PATH:-/var/spool/pgbackrest}"

  local stanza="${PGBACKREST_STANZA:-pdw}"
  local pgdata="${PGDATA:-$DEFAULT_PGDATA}"
  local repo_path="${PGBACKREST_REPO1_PATH:-/personal-data-warehouse}"
  local region="${PGBACKREST_REPO1_S3_REGION:-auto}"
  local uri_style="${PGBACKREST_REPO1_S3_URI_STYLE:-path}"
  local process_max="${PGBACKREST_PROCESS_MAX:-2}"
  local compress_type="${PGBACKREST_COMPRESS_TYPE:-zst}"
  local retention_full="${PGBACKREST_REPO1_RETENTION_FULL:-4}"
  local pg1_user="${PGBACKREST_PG1_USER:-${POSTGRES_USER:-postgres}}"
  local pg1_database="${PGBACKREST_PG1_DATABASE:-postgres}"

  local original_umask
  original_umask="$(umask)"
  umask 0077
  cat > "$PGBACKREST_CONFIG_PATH" <<EOF
[global]
repo1-type=s3
repo1-path=${repo_path}
repo1-s3-bucket=${PGBACKREST_REPO1_S3_BUCKET}
repo1-s3-endpoint=${PGBACKREST_REPO1_S3_ENDPOINT}
repo1-s3-region=${region}
repo1-s3-key=${PGBACKREST_REPO1_S3_KEY}
repo1-s3-key-secret=${PGBACKREST_REPO1_S3_KEY_SECRET}
repo1-s3-uri-style=${uri_style}
repo1-cipher-type=${cipher_type}
compress-type=${compress_type}
process-max=${process_max}
start-fast=${PGBACKREST_START_FAST:-y}
spool-path=${PGBACKREST_SPOOL_PATH:-/var/spool/pgbackrest}
log-path=${PGBACKREST_LOG_PATH:-/var/log/pgbackrest}
log-level-console=${PGBACKREST_LOG_LEVEL_CONSOLE:-info}
log-level-file=${PGBACKREST_LOG_LEVEL_FILE:-detail}
repo1-retention-full=${retention_full}
repo1-retention-full-type=${PGBACKREST_REPO1_RETENTION_FULL_TYPE:-count}
EOF

  append_config_if_set "repo1-cipher-pass" "PGBACKREST_REPO1_CIPHER_PASS"
  append_config_if_set "repo1-s3-key-type" "PGBACKREST_REPO1_S3_KEY_TYPE"
  append_config_if_set "repo1-s3-host" "PGBACKREST_REPO1_S3_HOST"
  append_config_if_set "repo1-s3-port" "PGBACKREST_REPO1_S3_PORT"
  append_config_if_set "repo1-s3-ca-file" "PGBACKREST_REPO1_S3_CA_FILE"
  append_config_if_set "repo1-s3-ca-path" "PGBACKREST_REPO1_S3_CA_PATH"
  append_config_if_set "repo1-storage-verify-tls" "PGBACKREST_REPO1_STORAGE_VERIFY_TLS"
  append_config_if_set "archive-async" "PGBACKREST_ARCHIVE_ASYNC"
  append_config_if_set "archive-push-queue-max" "PGBACKREST_ARCHIVE_PUSH_QUEUE_MAX"
  append_config_if_set "repo1-retention-diff" "PGBACKREST_REPO1_RETENTION_DIFF"
  append_config_if_set "repo1-retention-archive" "PGBACKREST_REPO1_RETENTION_ARCHIVE"
  append_config_if_set "repo1-retention-archive-type" "PGBACKREST_REPO1_RETENTION_ARCHIVE_TYPE"

  cat >> "$PGBACKREST_CONFIG_PATH" <<EOF

[$stanza]
pg1-path=${pgdata}
pg1-socket-path=${PGBACKREST_PG1_SOCKET_PATH:-/var/run/postgresql}
pg1-user=${pg1_user}
pg1-database=${pg1_database}
EOF

  chown postgres:postgres "$PGBACKREST_CONFIG_PATH"
  chmod 0640 "$PGBACKREST_CONFIG_PATH"
  umask "$original_umask"
  log "rendered pgBackRest config for stanza '$stanza'"
}

start_backup_loop() {
  if ! bool_enabled "${PDW_PGBACKREST_BACKUP_LOOP_ENABLED:-true}"; then
    log "pgBackRest backup loop disabled by PDW_PGBACKREST_BACKUP_LOOP_ENABLED"
    return
  fi

  # Supervise the loop: if it ever exits (it should not), restart it rather
  # than leaving Postgres serving with backups silently stopped. The loop
  # itself is written to survive transient failures, so a restart here is a
  # last-resort backstop, not the normal recovery path.
  local restart_delay="${PDW_PGBACKREST_BACKUP_LOOP_RESTART_SECONDS:-60}"
  (
    while true; do
      if [ "$(id -u)" = "0" ]; then
        gosu postgres pdw-pgbackrest-backup-loop || true
      else
        pdw-pgbackrest-backup-loop || true
      fi
      log "backup loop exited unexpectedly; restarting in ${restart_delay}s"
      sleep "$restart_delay"
    done
  ) &
  log "started pgBackRest backup loop"
}

if [ "${1:-}" = "pdw-pgbackrest-restore" ]; then
  shift
  export PGDATA="${PGDATA:-$DEFAULT_PGDATA}"
  render_pgbackrest_config
  exec pdw-pgbackrest-restore "$@"
fi

if [ "${1:-}" = "pgbackrest" ]; then
  export PGDATA="${PGDATA:-$DEFAULT_PGDATA}"
  render_pgbackrest_config
  if [ "$(id -u)" = "0" ]; then
    exec gosu postgres "$@"
  fi
  exec "$@"
fi

if [ "${1:-}" != "" ] && [ "${1:0:1}" = "-" ]; then
  set -- postgres "$@"
fi

if [ "${1:-}" = "postgres" ] && bool_enabled "${PDW_PGBACKREST_ENABLED:-true}"; then
  export PGDATA="${PGDATA:-$DEFAULT_PGDATA}"
  render_pgbackrest_config
  start_backup_loop

  readonly archive_command="pgbackrest --stanza=${PGBACKREST_STANZA:-pdw} archive-push %p"
  set -- "$@" \
    -c "wal_level=${POSTGRES_WAL_LEVEL:-replica}" \
    -c "archive_mode=${POSTGRES_ARCHIVE_MODE:-on}" \
    -c "archive_timeout=${POSTGRES_ARCHIVE_TIMEOUT:-300s}" \
    -c "archive_command=${POSTGRES_ARCHIVE_COMMAND:-$archive_command}"
elif [ "${1:-}" = "postgres" ]; then
  log "pgBackRest disabled by PDW_PGBACKREST_ENABLED"
fi

exec docker-entrypoint.sh "$@"
