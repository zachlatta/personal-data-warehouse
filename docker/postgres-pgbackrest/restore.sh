#!/usr/bin/env bash
set -Eeuo pipefail

readonly DEFAULT_PGDATA="/var/lib/postgresql/${PG_MAJOR:-18}/docker"

log() {
  printf '[pdw-pgbackrest-restore] %s\n' "$*" >&2
}

main() {
  local pgdata="${PGDATA:-$DEFAULT_PGDATA}"
  local stanza="${PGBACKREST_STANZA:-pdw}"

  if [ -d "$pgdata" ] && [ -n "$(find "$pgdata" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
    log "refusing to restore into non-empty PGDATA: $pgdata"
    log "restore into a fresh volume, or move the existing directory aside intentionally"
    exit 1
  fi

  install -d -m 0700 -o postgres -g postgres "$pgdata"

  log "restoring stanza '$stanza' into $pgdata"
  if [ "$(id -u)" = "0" ]; then
    exec gosu postgres pgbackrest --stanza="$stanza" --delta restore "$@"
  fi

  exec pgbackrest --stanza="$stanza" --delta restore "$@"
}

main "$@"
