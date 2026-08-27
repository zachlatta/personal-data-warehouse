#!/usr/bin/env bash
set -Eeuo pipefail

readonly DEFAULT_PGDATA="/var/lib/postgresql/${PG_MAJOR:-18}/docker"

log() {
  printf '[pdw-pgbackrest-restore] %s\n' "$*" >&2
}

bool_enabled() {
  local value="${1:-}"
  case "${value,,}" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

main() {
  local pgdata="${PGDATA:-$DEFAULT_PGDATA}"
  local stanza="${PGBACKREST_STANZA:-pdw}"

  if [ -d "$pgdata" ] && [ -n "$(find "$pgdata" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
    # A restore of this database moves ~223 GB over SFTP and runs for tens of
    # minutes, and a single transient read failure aborts the whole thing --
    # observed 2026-08-27, 52 minutes in, on a file that read back perfectly
    # three times immediately afterwards. pgBackRest can resume: the command
    # below already passes --delta, which reuses whatever is already correct on
    # disk. Refusing outright meant the only way forward was to discard 219 GB
    # and start over, turning a blip into an hour.
    #
    # Still refuse by DEFAULT: restoring over a live PGDATA is destructive, and
    # that is the accident this guard exists to prevent. Resuming is an explicit
    # decision the operator has to state.
    if bool_enabled "${PDW_PGBACKREST_RESTORE_RESUME:-}"; then
      log "PGDATA is not empty; resuming with --delta as PDW_PGBACKREST_RESTORE_RESUME is set"
      log "this OVERWRITES $pgdata -- it must not be a cluster you still need"
    else
      log "refusing to restore into non-empty PGDATA: $pgdata"
      log "restore into a fresh volume, or move the existing directory aside intentionally"
      log "to resume an interrupted restore in place, set PDW_PGBACKREST_RESTORE_RESUME=true"
      exit 1
    fi
  fi

  install -d -m 0700 -o postgres -g postgres "$pgdata"

  log "restoring stanza '$stanza' into $pgdata"
  if [ "$(id -u)" = "0" ]; then
    exec gosu postgres pgbackrest --stanza="$stanza" --delta restore "$@"
  fi

  exec pgbackrest --stanza="$stanza" --delta restore "$@"
}

main "$@"
