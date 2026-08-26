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

# Publish backup posture into the warehouse.
#
# This exists because NOTHING ELSE CAN. The Dagster freshness collector runs in
# a different container and cannot shell out to pgbackrest, so backups appeared
# in no health surface at all -- and on 2026-08-26 production reported
# `status: error (no valid backups)` for a day while WAL archiving kept
# working, every pipeline read green, and this loop logged "backup failed"
# every six hours to a stdout nobody reads.
#
# It is deliberately best-effort: a reporting failure must never abort or fail
# a backup. Losing the signal is bad; losing the backup because the signal
# broke would be absurd.
report_health() {
  local attempt_type="${1:-}" attempt_ok="${2:-1}" attempt_error="${3:-}"
  if ! bool_enabled "${PDW_PGBACKREST_HEALTH_REPORT_ENABLED:-true}"; then
    return 0
  fi
  command -v psql >/dev/null 2>&1 || return 0

  local stanza="${PGBACKREST_STANZA:-pdw}"
  local info
  info="$(run_pgbackrest info --output=json 2>/dev/null)" || info=""

  # jq is not in this image, so the JSON is reduced by python3 (which the
  # postgres image does ship). Any parse failure yields empty fields rather
  # than a failed report: an unparseable `info` is itself worth recording.
  local parsed
  parsed="$(PGBK_INFO="$info" PGBK_STANZA="$stanza" python3 - <<'PYEOF' 2>/dev/null || true
import json, os
raw = os.environ.get("PGBK_INFO") or ""
stanza = os.environ.get("PGBK_STANZA") or "pdw"
out = {"repo_status": "", "repo_message": "", "full": 0, "diff": 0, "incr": 0,
       "label": "", "type": "", "count": 0, "bytes": 0, "wal_min": "", "wal_max": ""}
try:
    for entry in json.loads(raw):
        if entry.get("name") != stanza:
            continue
        status = entry.get("status") or {}
        out["repo_status"] = "ok" if status.get("code") == 0 else "error"
        out["repo_message"] = str(status.get("message") or "")
        backups = entry.get("backup") or []
        out["count"] = len(backups)
        for b in backups:
            stop = int(((b.get("timestamp") or {}).get("stop")) or 0)
            btype = str(b.get("type") or "")
            if btype in ("full", "diff", "incr") and stop > out[btype]:
                out[btype] = stop
        if backups:
            newest = max(backups, key=lambda b: int(((b.get("timestamp") or {}).get("stop")) or 0))
            out["label"] = str(newest.get("label") or "")
            out["type"] = str(newest.get("type") or "")
            out["bytes"] = int(((newest.get("info") or {}).get("repository") or {}).get("size") or 0)
        for db in entry.get("archive") or []:
            out["wal_min"] = str(db.get("min") or "")
            out["wal_max"] = str(db.get("max") or "")
except Exception:
    pass
print("\t".join(str(out[k]) for k in
      ("repo_status", "repo_message", "full", "diff", "incr", "label", "type", "count", "bytes", "wal_min", "wal_max")))
PYEOF
)"
  [ -n "$parsed" ] || parsed="$(printf 'unparseable\t\t0\t0\t0\t\t\t0\t0\t\t')"

  local repo_status repo_message full diff incr label btype count bytes wal_min wal_max
  IFS="$(printf '\t')" read -r repo_status repo_message full diff incr label btype count bytes wal_min wal_max <<EOF
$parsed
EOF

  # `ON CONFLICT DO UPDATE` keyed by stanza: one durable row per stanza whose
  # collected_at is what tells the view whether it may still be believed.
  PGPASSWORD="${POSTGRES_PASSWORD:-}" psql \
    --no-psqlrc --quiet --set ON_ERROR_STOP=0 \
    -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-${POSTGRES_USER:-postgres}}" \
    -v stanza="$stanza" -v repo_status="${repo_status:-}" -v repo_message="${repo_message:-}" \
    -v full="${full:-0}" -v diff="${diff:-0}" -v incr="${incr:-0}" \
    -v label="${label:-}" -v btype="${btype:-}" -v cnt="${count:-0}" -v bytes="${bytes:-0}" \
    -v wal_min="${wal_min:-}" -v wal_max="${wal_max:-}" \
    -v attempt_type="${attempt_type:-}" -v attempt_ok="${attempt_ok:-1}" \
    -v attempt_error="${attempt_error:-}" >/dev/null 2>&1 <<'SQLEOF' || log "health report failed (backup itself unaffected)"
INSERT INTO ops.pgbackrest_health AS t (
    stanza, repo_status, repo_message,
    last_full_at, last_diff_at, last_incr_at,
    last_backup_label, last_backup_type, backup_count, repo_bytes,
    wal_min, wal_max, archived_count, failed_count, last_archived_at,
    last_attempt_at, last_attempt_type, last_attempt_ok, last_error, collected_at)
SELECT
    :'stanza', :'repo_status', :'repo_message',
    to_timestamp(NULLIF(:'full','')::bigint), to_timestamp(NULLIF(:'diff','')::bigint),
    to_timestamp(NULLIF(:'incr','')::bigint),
    :'label', :'btype', NULLIF(:'cnt','')::bigint, NULLIF(:'bytes','')::bigint,
    :'wal_min', :'wal_max',
    COALESCE(a.archived_count, 0), COALESCE(a.failed_count, 0),
    COALESCE(a.last_archived_time, '1970-01-01 00:00:00+00'::timestamptz),
    now(), :'attempt_type', NULLIF(:'attempt_ok','')::bigint, :'attempt_error', now()
FROM (SELECT archived_count, failed_count, last_archived_time FROM pg_stat_archiver) a
ON CONFLICT (stanza) DO UPDATE SET
    repo_status = EXCLUDED.repo_status, repo_message = EXCLUDED.repo_message,
    last_full_at = EXCLUDED.last_full_at, last_diff_at = EXCLUDED.last_diff_at,
    last_incr_at = EXCLUDED.last_incr_at, last_backup_label = EXCLUDED.last_backup_label,
    last_backup_type = EXCLUDED.last_backup_type, backup_count = EXCLUDED.backup_count,
    repo_bytes = EXCLUDED.repo_bytes, wal_min = EXCLUDED.wal_min, wal_max = EXCLUDED.wal_max,
    archived_count = EXCLUDED.archived_count, failed_count = EXCLUDED.failed_count,
    last_archived_at = EXCLUDED.last_archived_at, last_attempt_at = EXCLUDED.last_attempt_at,
    last_attempt_type = EXCLUDED.last_attempt_type, last_attempt_ok = EXCLUDED.last_attempt_ok,
    last_error = EXCLUDED.last_error, collected_at = EXCLUDED.collected_at
SQLEOF
}

run_backup() {
  local type="$1"
  log "starting ${type} backup"
  if ! run_pgbackrest --type="$type" backup; then
    log "${type} backup failed"
    # Report the FAILURE too. Reporting only successes is how a loop that never
    # succeeds says nothing at all, which is exactly what happened here.
    report_health "$type" 0 "${type} backup failed"
    return 1
  fi
  log "completed ${type} backup"
  report_health "$type" 1 ""
}

backup_state_dir() {
  local spool="${PGBACKREST_SPOOL_PATH:-/var/spool/pgbackrest}"
  printf '%s\n' "$spool/pdw-backup-loop/${PGBACKREST_STANZA:-pdw}"
}

backup_state_mark_done() {
  local key="$1"
  local dir
  dir="$(backup_state_dir)"
  mkdir -p "$dir"
  : > "$dir/$key"
}

backup_state_done() {
  local key="$1"
  test -e "$(backup_state_dir)/$key"
}

backup_type_for_now() {
  local date hour weekday week
  date="$(date -u +%Y%m%d)"
  hour="$(date -u +%H)"
  weekday="$(date -u +%u)"
  week="$(date -u +%G%V)"

  if [ -n "${PDW_PGBACKREST_BACKUP_FORCE_TYPE:-}" ]; then
    printf '%s\n' "$PDW_PGBACKREST_BACKUP_FORCE_TYPE"
  elif [ "$weekday" = "${PDW_PGBACKREST_FULL_BACKUP_WEEKDAY_UTC:-7}" ] \
    && [ "$hour" -ge "${PDW_PGBACKREST_FULL_BACKUP_HOUR_UTC:-08}" ] \
    && ! backup_state_done "full-$week"; then
    printf 'full\n'
  elif [ "$hour" -ge "${PDW_PGBACKREST_DIFF_BACKUP_HOUR_UTC:-08}" ] \
    && ! backup_state_done "diff-$date"; then
    printf 'diff\n'
  else
    printf 'incr\n'
  fi
}

mark_backup_type_done() {
  local type="$1"
  local date week
  date="$(date -u +%Y%m%d)"
  week="$(date -u +%G%V)"

  case "$type" in
    full)
      backup_state_mark_done "full-$week"
      backup_state_mark_done "diff-$date"
      ;;
    diff)
      backup_state_mark_done "diff-$date"
      ;;
  esac
}

main() {
  if ! wait_for_postgres; then
    log "Postgres never became ready; backup loop exiting"
    exit 1
  fi

  # Startup repo preparation is best-effort. A transient repository problem
  # (e.g. the S3 endpoint briefly unreachable, as during a slowking restart)
  # must never kill the loop the way an unguarded failure under `set -e` would
  # -- otherwise backups stop silently until the container is recreated, while
  # Postgres keeps serving. Log and continue; the periodic loop below
  # re-asserts repo health every cycle and retries on its own.
  ensure_stanza || log "stanza preparation failed at startup; will retry each cycle"
  run_check || log "pgBackRest check failed at startup; will retry each cycle"

  if bool_enabled "${PDW_PGBACKREST_BACKUP_ON_STARTUP:-true}" && ! has_backup; then
    if run_backup full; then
      mark_backup_type_done full
    fi
  fi

  local interval="${PDW_PGBACKREST_BACKUP_INTERVAL_SECONDS:-21600}"
  local type
  while true; do
    sleep "$interval"
    # Re-assert repo health each cycle, but never let a transient failure here
    # abort the loop -- fall through to the (guarded) backup attempt regardless.
    ensure_stanza || log "stanza preparation failed; continuing to backup attempt"
    run_check || log "pgBackRest check failed; continuing to backup attempt"
    type="$(backup_type_for_now)"
    if run_backup "$type"; then
      mark_backup_type_done "$type"
    fi
    # And once more per cycle regardless, so `collected_at` keeps moving and the
    # view can tell "no backup yet" from "nobody has looked in three days".
    report_health "" 1 ""
  done
}

main "$@"
