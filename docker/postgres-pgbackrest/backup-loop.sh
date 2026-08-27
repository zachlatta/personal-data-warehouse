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
  local pgdata="${PGDATA:-/var/lib/postgresql/${PG_MAJOR:-18}/docker}"
  local info
  info="$(run_pgbackrest info --output=json 2>/dev/null)" || info=""

  # Reduce the JSON in Postgres, not in a helper interpreter. This image ships
  # no such interpreter (verified in production 2026-08-27), after the reducer
  # had silently fallen through to its "unparseable" fallback on every call and
  # published `backup_count = 0` while a valid, restore-verified full existed.
  # psql is already a hard dependency of this function, and jsonb parsing is
  # free. Guard the cast in shell: malformed JSON must degrade to "no entry
  # found" (which still records the attempt and the archiver counters) rather
  # than abort the statement and record nothing at all.
  case "$info" in
    \[*) : ;;
    *) info="" ;;
  esac

  # The WAL backlog, counted from the filesystem. pg_stat_archiver cannot
  # express it -- archived_count and failed_count both climbed normally right
  # through the 2026-08-26 incident while the queue grew to 5,910 segments,
  # because WAL *was* shipping, just slower than it was produced. Counted with
  # find rather than pg_ls_dir() so this never depends on the reporting role
  # holding superuser or pg_monitor.
  local wal_ready_count
  wal_ready_count="$(find "$pgdata/pg_wal/archive_status" -maxdepth 1 -name '*.ready' 2>/dev/null | wc -l | tr -d '[:space:]')"
  [ -n "$wal_ready_count" ] || wal_ready_count=0

  # `ON CONFLICT DO UPDATE` keyed by stanza: one durable row per stanza whose
  # collected_at is what tells the view whether it may still be believed.
  PGPASSWORD="${POSTGRES_PASSWORD:-}" psql \
    --no-psqlrc --quiet --set ON_ERROR_STOP=0 \
    -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-${POSTGRES_USER:-postgres}}" \
    -v stanza="$stanza" -v info="$info" -v wal_ready="$wal_ready_count" \
    -v attempt_type="${attempt_type:-}" -v attempt_ok="${attempt_ok:-1}" \
    -v attempt_error="${attempt_error:-}" >/dev/null 2>&1 <<'SQLEOF' || log "health report failed (backup itself unaffected)"
WITH doc AS (
    SELECT NULLIF(:'info', '')::jsonb AS j
), entry AS (
    SELECT e
    FROM doc, LATERAL jsonb_array_elements(doc.j) AS e
    WHERE e->>'name' = :'stanza'
    LIMIT 1
), backups AS (
    SELECT b FROM entry, LATERAL jsonb_array_elements(entry.e->'backup') AS b
), agg AS (
    SELECT
        count(*)::bigint AS cnt,
        max((b->'timestamp'->>'stop')::bigint) FILTER (WHERE b->>'type' = 'full') AS full_stop,
        max((b->'timestamp'->>'stop')::bigint) FILTER (WHERE b->>'type' = 'diff') AS diff_stop,
        max((b->'timestamp'->>'stop')::bigint) FILTER (WHERE b->>'type' = 'incr') AS incr_stop
    FROM backups
), newest AS (
    SELECT b FROM backups ORDER BY (b->'timestamp'->>'stop')::bigint DESC LIMIT 1
), arch AS (
    SELECT a->>'min' AS wal_min, a->>'max' AS wal_max
    FROM entry, LATERAL jsonb_array_elements(entry.e->'archive') AS a
    LIMIT 1
)
INSERT INTO ops.pgbackrest_health AS t (
    stanza, repo_status, repo_message,
    last_full_at, last_diff_at, last_incr_at,
    last_backup_label, last_backup_type, backup_count, repo_bytes,
    wal_min, wal_max, wal_ready_count, archived_count, failed_count, last_archived_at,
    last_attempt_at, last_attempt_type, last_attempt_ok, last_error, collected_at)
SELECT
    :'stanza',
    CASE
        WHEN (SELECT e FROM entry) IS NULL THEN 'unparseable'
        WHEN (SELECT e->'status'->>'code' FROM entry) = '0' THEN 'ok'
        ELSE 'error'
    END,
    COALESCE((SELECT e->'status'->>'message' FROM entry), ''),
    to_timestamp(COALESCE((SELECT full_stop FROM agg), 0)),
    to_timestamp(COALESCE((SELECT diff_stop FROM agg), 0)),
    to_timestamp(COALESCE((SELECT incr_stop FROM agg), 0)),
    COALESCE((SELECT b->>'label' FROM newest), ''),
    COALESCE((SELECT b->>'type' FROM newest), ''),
    COALESCE((SELECT cnt FROM agg), 0),
    -- repo1-block=y reports `repository.delta` and omits `repository.size`,
    -- so reading only `size` silently published 0 bytes for every backup.
    COALESCE(
        (SELECT (b->'info'->'repository'->>'size')::bigint FROM newest),
        (SELECT (b->'info'->'repository'->>'delta')::bigint FROM newest),
        0),
    COALESCE((SELECT wal_min FROM arch), ''),
    COALESCE((SELECT wal_max FROM arch), ''),
    NULLIF(:'wal_ready','')::bigint,
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
    wal_ready_count = EXCLUDED.wal_ready_count,
    archived_count = EXCLUDED.archived_count, failed_count = EXCLUDED.failed_count,
    last_archived_at = EXCLUDED.last_archived_at, last_attempt_at = EXCLUDED.last_attempt_at,
    last_attempt_type = EXCLUDED.last_attempt_type, last_attempt_ok = EXCLUDED.last_attempt_ok,
    last_error = EXCLUDED.last_error, collected_at = EXCLUDED.collected_at
SQLEOF
}

run_backup() {
  local type="$1"
  local output status
  log "starting ${type} backup"
  # Capture the output so a LOCK collision can be told apart from a real
  # failure. pgBackRest exits non-zero for both, and they are opposite facts:
  # "another backup is already running" means backups are working.
  output="$(run_pgbackrest --type="$type" backup 2>&1)"
  status=$?
  printf '%s\n' "$output"
  if [ "$status" -ne 0 ]; then
    case "$output" in
      *"unable to acquire lock"*)
        # Exception 050. Seen for real on 2026-08-26: a long manual full held
        # the lock and the 6-hourly loop tripped over it every cycle. Recording
        # that as a failed attempt would paint /pipelines `attention` for the
        # entire duration of a backup that was succeeding.
        log "${type} backup skipped: another pgBackRest operation holds the lock"
        report_health "" 1 ""
        return 1
        ;;
    esac
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
