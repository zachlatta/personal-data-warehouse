#!/usr/bin/env bash
set -Eeuo pipefail

log() {
  printf '[pdw-pgbackrest-backup-loop] %s\n' "$*" >&2
}

bool_enabled() {
  local value="${1:-}"
  case "${value,,}" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

run_pgbackrest() {
  pgbackrest --stanza="${PGBACKREST_STANZA:-pdw}" "$@"
}

wait_for_postgres() {
  local timeout="${PDW_PGBACKREST_POSTGRES_READY_TIMEOUT_SECONDS:-600}"
  local deadline=$((SECONDS + timeout))
  local user="${PGBACKREST_PG1_USER:-${POSTGRES_USER:-postgres}}"
  local database="${PGBACKREST_PG1_DATABASE:-postgres}"
  local host="${PDW_PGBACKREST_PGHOST:-/var/run/postgresql}"

  log "waiting for Postgres to accept local connections"
  while [ "$SECONDS" -lt "$deadline" ]; do
    if pg_isready -q -h "$host" -U "$user" -d "$database"; then
      log "Postgres is ready"
      return 0
    fi
    sleep 2
  done

  log "Postgres did not become ready within ${timeout}s"
  return 1
}

ensure_stanza() {
  local info
  info="$(run_pgbackrest info 2>/dev/null || true)"

  if printf '%s\n' "$info" | grep -q 'status: ok'; then
    return
  fi

  if ! bool_enabled "${PDW_PGBACKREST_STANZA_CREATE_ENABLED:-true}"; then
    log "stanza is missing and stanza creation is disabled"
    return 1
  fi

  if [ -n "$info" ] && ! printf '%s\n' "$info" | grep -Eq 'missing stanza|missing stanza path'; then
    log "stanza exists but is not healthy yet; continuing to pgBackRest check"
    return
  fi

  log "creating pgBackRest stanza '${PGBACKREST_STANZA:-pdw}'"
  run_pgbackrest stanza-create
}

run_check() {
  if bool_enabled "${PDW_PGBACKREST_CHECK_ENABLED:-true}"; then
    log "running pgBackRest check"
    run_pgbackrest check
  fi
}

has_backup() {
  run_pgbackrest info --output=json 2>/dev/null | grep -q '"label"'
}

run_backup() {
  local type="$1"
  log "starting ${type} backup"
  if ! run_pgbackrest --type="$type" backup; then
    log "${type} backup failed"
    return 1
  fi
  log "completed ${type} backup"
}

backup_type_for_now() {
  local hour weekday
  hour="$(date -u +%H)"
  weekday="$(date -u +%u)"

  if [ -n "${PDW_PGBACKREST_BACKUP_FORCE_TYPE:-}" ]; then
    printf '%s\n' "$PDW_PGBACKREST_BACKUP_FORCE_TYPE"
  elif [ "$weekday" = "${PDW_PGBACKREST_FULL_BACKUP_WEEKDAY_UTC:-7}" ] && [ "$hour" = "${PDW_PGBACKREST_FULL_BACKUP_HOUR_UTC:-08}" ]; then
    printf 'full\n'
  elif [ "$hour" = "${PDW_PGBACKREST_DIFF_BACKUP_HOUR_UTC:-08}" ]; then
    printf 'diff\n'
  else
    printf 'incr\n'
  fi
}

main() {
  wait_for_postgres
  ensure_stanza
  run_check

  if bool_enabled "${PDW_PGBACKREST_BACKUP_ON_STARTUP:-true}" && ! has_backup; then
    run_backup full
  fi

  local interval="${PDW_PGBACKREST_BACKUP_INTERVAL_SECONDS:-21600}"
  while true; do
    sleep "$interval"
    run_backup "$(backup_type_for_now)" || true
  done
}

main "$@"
