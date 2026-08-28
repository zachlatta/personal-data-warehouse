from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
ENTRYPOINT = REPO_ROOT / "docker/postgres-pgbackrest/entrypoint.sh"
README = REPO_ROOT / "docker/postgres-pgbackrest/README.md"


def test_pgbackrest_io_timeout_is_configurable_and_safe_for_hdd_repositories() -> None:
    """A slow S3 read must not abort a backup at pgBackRest's 60s default.

    PDW's Garage repository is backed by a large HDD RAID array.  After its
    write-back SSD cache was intentionally retired, healthy but saturated
    reads exceeded 60 seconds and aborted both WAL checks and a differential
    backup.  Keep the default in the generated config so archive-push, checks,
    scheduled backups, and restores all use the same measured budget.

    Raised 600 -> 1800 on 2026-08-26.  600 assumed the repository was merely
    slow; it is also CONTENDED.  slowking runs its own offsite ``restic``
    backup against the same HDD array, and while it does, throughput to Garage
    measured ~13 KB/s -- a running full backup made no progress for 26 minutes,
    and the 08-25 21:38 attempt died with ``ERROR: [042]: timeout after
    600000ms waiting for read``.  The budget must outlast the overlap, because
    nothing coordinates the two schedules.
    """

    entrypoint = ENTRYPOINT.read_text()
    readme = README.read_text()

    assert "io-timeout=${PGBACKREST_IO_TIMEOUT:-1800}" in entrypoint
    assert "PGBACKREST_IO_TIMEOUT=1800" in readme


def test_pgbackrest_archive_timeout_is_configurable_and_safe_for_hdd_repositories() -> None:
    """``io-timeout`` does not govern the wait for the prior WAL segment.

    This is the defect the io-timeout test above could not catch, and it cost
    every backup between 2026-08-25 03:20 UTC and 2026-08-26.  The 2026-08-24
    repair raised ``io-timeout`` to 600s, but pgBackRest applies a *separate*
    ``archive-timeout`` (default **60s**) to the segment switch it forces after
    ``pg_backup_start``.  WAL pushes to the Garage HDD repository measured
    5.7-200.3s, so every scheduled backup died at startup with::

        ERROR: [082]: WAL segment 000000050000044000000011 was not archived
                      before the 60000ms timeout

    and ``pgbackrest info`` reported ``status: error (no valid backups)`` while
    WAL archiving itself kept working and the backup loop kept logging a
    failure to stdout that nothing escalated.  Pin the generated default so a
    slow-but-healthy repository cannot silently prevent every base backup.
    """

    entrypoint = ENTRYPOINT.read_text()
    readme = README.read_text()

    assert "archive-timeout=${PGBACKREST_ARCHIVE_TIMEOUT:-1800}" in entrypoint
    assert "PGBACKREST_ARCHIVE_TIMEOUT=1800" in readme


def test_pgbackrest_archive_timeout_is_not_confused_with_the_postgres_setting() -> None:
    """Two different settings share a name; conflating them re-opens the bug.

    ``POSTGRES_ARCHIVE_TIMEOUT`` is PostgreSQL's ``archive_timeout`` (how often
    an idle server forces a WAL switch, 300s here).  ``PGBACKREST_ARCHIVE_TIMEOUT``
    is pgBackRest's ``archive-timeout`` (how long ``backup`` waits for that
    segment to reach the repository).  They must both survive, independently.
    """

    entrypoint = ENTRYPOINT.read_text()

    assert "archive_timeout=${POSTGRES_ARCHIVE_TIMEOUT:-300s}" in entrypoint
    assert "archive-timeout=${PGBACKREST_ARCHIVE_TIMEOUT:-1800}" in entrypoint


def test_a_lock_collision_is_not_reported_as_a_failed_backup() -> None:
    """"Another backup is already running" is the opposite of a failure.

    pgBackRest exits non-zero for both, so the loop has to read the message.
    Seen for real on 2026-08-26: a long manual full held
    `/tmp/pgbackrest/pdw-backup-1.lock` and the 6-hourly loop tripped over it
    with `ERROR: [050]: unable to acquire lock`. Recording that as
    `last_attempt_ok = 0` would paint /pipelines `attention` for the whole
    duration of a backup that was succeeding — turning the health surface added
    the same day into a source of false alarms.
    """

    loop = (REPO_ROOT / "docker/postgres-pgbackrest/backup-loop.sh").read_text()

    assert '*"unable to acquire lock"*' in loop, (
        "the loop does not distinguish a lock collision from a real failure"
    )
    lock_branch = loop.split('*"unable to acquire lock"*', 1)[1].split(";;", 1)[0]
    assert "report_health \"\" 1 \"\"" in lock_branch, (
        "a lock collision must refresh collected_at without marking the attempt failed"
    )
    assert 'report_health "$type" 0' not in lock_branch, (
        "a lock collision must not be recorded as a failed backup attempt"
    )


def test_a_real_backup_failure_is_still_reported_as_one() -> None:
    """The loosening must not swallow the signal it was built to carry."""

    loop = (REPO_ROOT / "docker/postgres-pgbackrest/backup-loop.sh").read_text()
    assert 'report_health "$type" 0 "${type} backup failed"' in loop
    assert 'report_health "$type" 1 ""' in loop, "a success must be recorded too"


def test_pgbackrest_repository_type_is_configurable_for_sftp() -> None:
    """PDW must be able to back up over SFTP, not only to S3.

    Measured 2026-08-26 against the Garage S3 repository on slowking: a single
    512 MB object took 886.9 s (0.58 MB/s).  The payload was all zeros, so
    Garage's own compression meant almost nothing reached the platters --
    the cost was ~1.73 s per 1 MiB block of metadata commit (LMDB fsync on
    btrfs on a RAID5 array whose queue never drains), not data bandwidth.
    The same 512 MB written to the same array over SFTP took 26 s (19.7 MB/s),
    a 34x improvement, because pgBackRest's large sequential files never get
    shredded into a million blocks.  Keep the repository type env-driven so the
    transport is a deployment decision, not an image rebuild.
    """

    entrypoint = ENTRYPOINT.read_text()

    assert "repo1-type=${repo_type}" in entrypoint
    assert 'repo_type="${PGBACKREST_REPO1_TYPE:-s3}"' in entrypoint

    # SFTP transport options must all be renderable from the environment.
    for option in (
        "repo1-sftp-host=",
        "repo1-sftp-host-user=",
        "repo1-sftp-private-key-file=",
        "repo1-sftp-host-key-hash-type=",
        "repo1-sftp-host-key-check-type=",
    ):
        assert option in entrypoint, option

    for env_name in (
        "PGBACKREST_REPO1_SFTP_PUBLIC_KEY_FILE",
        "PGBACKREST_REPO1_SFTP_KNOWN_HOST",
        "PGBACKREST_REPO1_SFTP_HOST_PORT",
        "PGBACKREST_REPO1_SFTP_HOST_FINGERPRINT",
    ):
        assert env_name in entrypoint, env_name


def test_pgbackrest_s3_credentials_are_only_required_for_an_s3_repository() -> None:
    """An SFTP deployment has no bucket, endpoint, or access key.

    Requiring the S3 quartet unconditionally would make the container refuse to
    start after the transport cutover.
    """

    entrypoint = ENTRYPOINT.read_text()

    # The S3 requirement must sit inside a branch on the repository type.
    s3_requirement = entrypoint.index("PGBACKREST_REPO1_S3_BUCKET")
    type_branch = entrypoint.index('case "$repo_type"')
    assert type_branch < s3_requirement


def test_pgbackrest_wal_archiving_can_run_asynchronously() -> None:
    """Synchronous archive-push pushes one 16 MB segment at a time.

    On 2026-08-26 PDW had 5,075 unarchived segments (79 GB) growing +8.7 GB/h
    net, because each segment cost ~27 s of Garage block commits and archive
    push was serialised.  archive-async plus a spool path lets pgBackRest batch
    and parallelise, so the queue can actually drain.
    """

    entrypoint = ENTRYPOINT.read_text()

    for option, env_name in (
        ("archive-async", "PGBACKREST_ARCHIVE_ASYNC"),
        ("repo1-bundle", "PGBACKREST_REPO1_BUNDLE"),
        ("repo1-block", "PGBACKREST_REPO1_BLOCK"),
    ):
        assert f'append_config_if_set "{option}" "{env_name}"' in entrypoint, option

    # archive-timeout is emitted unconditionally with a default, so it must NOT
    # also be appended conditionally: a duplicated option fails config parsing.
    assert entrypoint.count("archive-timeout") == 1


def test_health_report_does_not_depend_on_python3() -> None:
    """The Postgres image ships no python3, so the reducer never ran.

    Verified in production 2026-08-27: `command -v python3` is empty inside
    ghcr.io/zachlatta/personal-data-warehouse-postgres-pgbackrest.  The health
    writer therefore fell through to its "unparseable" fallback on every call,
    and `ops.pgbackrest_health` recorded `backup_count = 0` and
    `repo_status = 'unparseable'` **while a valid, restore-verified full backup
    existed**.  `marts_ops.pgbackrest_health` read `failing` as designed.

    A monitor that is permanently red is worse than no monitor, because it
    trains the reader to ignore it -- and it would have masked exactly the
    outage it was built to catch.  psql is already a hard dependency here and
    Postgres parses JSON natively, so the reduction belongs in SQL.
    """

    loop = (REPO_ROOT / "docker/postgres-pgbackrest/backup-loop.sh").read_text()

    # The word may appear in the comment that explains this; the *invocation*
    # must not.
    assert "python3 -" not in loop, "python3 is not installed in this image"
    assert "PYEOF" not in loop, "no heredoc-fed interpreter may sit in this path"
    assert "jsonb" in loop, "the info JSON must be reduced by Postgres itself"


def test_health_report_records_the_wal_backlog() -> None:
    """`.ready` count is the earliest signal that archiving is losing.

    pg_stat_archiver's archived_count and failed_count both kept climbing
    normally through the 2026-08-26 incident: WAL *was* being archived, just far
    slower than it was generated.  Nothing in the health row expressed the
    backlog, so a queue that grew to 5,910 segments (96 GB) over two days was
    invisible to every health surface.

    Counted from the filesystem rather than via pg_ls_dir() so the report does
    not depend on the reporting role holding superuser or pg_monitor.
    """

    loop = (REPO_ROOT / "docker/postgres-pgbackrest/backup-loop.sh").read_text()
    schema = (REPO_ROOT / "src/personal_data_warehouse/schema.py").read_text()

    assert "wal_ready_count" in loop
    assert "archive_status" in loop
    assert '"wal_ready_count",' in schema, "the column must exist in the table spec"


def test_marts_view_escalates_on_a_growing_wal_backlog() -> None:
    """A backlog past a threshold is an outage in progress, not a curiosity."""

    postgres = (REPO_ROOT / "src/personal_data_warehouse/postgres.py").read_text()

    assert "wal_ready_count" in postgres
    assert "WAL_READY_ATTENTION" in postgres


def test_new_health_column_is_migrated_onto_existing_tables() -> None:
    """`CREATE TABLE IF NOT EXISTS` never widens a table that already exists.

    Without an explicit ALTER, `wal_ready_count` would be absent in production
    and the backup loop's INSERT would fail -- silently, because the health
    report is best-effort by design.  The monitor would go stale rather than
    loud, which is the same dark failure it exists to prevent.  Fresh-database
    tests cannot catch this by construction.
    """

    postgres = (REPO_ROOT / "src/personal_data_warehouse/postgres.py").read_text()

    assert 'ALTER TABLE @pgbackrest_health ADD COLUMN IF NOT EXISTS' in postgres
    assert '("wal_ready_count", "bigint", "0"),' in postgres


def test_repo_bytes_falls_back_to_the_block_incremental_delta() -> None:
    """`repo1-block=y` reports `repository.delta` and omits `repository.size`.

    Measured against the real repository on 2026-08-27: a 61.6 GB full backup
    published `repo_bytes = 0` because only `size` was read.  Block incremental
    is now on, so `delta` is the field that always exists.
    """

    loop = (REPO_ROOT / "docker/postgres-pgbackrest/backup-loop.sh").read_text()

    assert "'repository'->>'delta'" in loop


def test_health_is_reported_at_startup_before_the_first_sleep() -> None:
    """A container recreated more often than the interval never reports.

    The cycle timer restarts with the container, so the periodic report at the
    end of each six-hour loop is unreachable for a container that keeps being
    recreated -- and the row goes stale for a reason unrelated to the backups.
    """

    loop = (REPO_ROOT / "docker/postgres-pgbackrest/backup-loop.sh").read_text()
    startup = loop[loop.index("def main() {") if "def main() {" in loop else loop.index("main() {"):]
    startup = startup[: startup.index("local interval=")]

    assert 'report_health "" 1 ""' in startup, "startup must refresh the health row"


def test_an_interrupted_restore_can_be_resumed_in_place() -> None:
    """A 223 GB restore over SFTP is long enough to be interrupted.

    Observed 2026-08-27: a diff restore aborted 52 minutes in with
    `FileMissingError` on a file that then read back perfectly three times in a
    row -- a transient SFTP read, not a damaged repository.  The wrapper refused
    to restart into the partially-restored directory, so the only way forward
    was to discard 219 GB and begin again, turning a blip into an hour.

    pgBackRest already passes --delta and can reuse what is correct on disk.
    Resuming must stay opt-in, because restoring over a live PGDATA is exactly
    the accident the guard exists to prevent.
    """

    restore = (REPO_ROOT / "docker/postgres-pgbackrest/restore.sh").read_text()

    assert "PDW_PGBACKREST_RESTORE_RESUME" in restore
    assert "refusing to restore into non-empty PGDATA" in restore, (
        "the default must still refuse"
    )
    # The guard must not be bypassable by accident: an unset variable refuses.
    assert 'bool_enabled "${PDW_PGBACKREST_RESTORE_RESUME:-}"' in restore


def test_archiving_freshness_is_judged_against_the_snapshot_not_now() -> None:
    """`last_archived_at` is a fact captured when the loop reported.

    The loop reports every six hours, so comparing that field against now()
    guarantees it ages past any threshold shorter than the reporting interval.
    Observed 2026-08-27: the view read `attention` with archiving perfectly
    healthy -- WAL had shipped one second before the snapshot was taken, and the
    snapshot was 70 minutes old.

    Whether the snapshot is too old to be believed at all is a different
    question, and the staleness rule above already answers it.
    """

    postgres = (REPO_ROOT / "src/personal_data_warehouse/postgres.py").read_text()

    assert "last_archived_at < collected_at - interval '1 hour'" in postgres
    assert "last_archived_at < now() - interval '1 hour'" not in postgres


def test_restore_drill_columns_are_migrated_and_never_written_by_the_loop() -> None:
    """The drill record is written by a human-run command, not the loop.

    The loop's upsert must not name the restore columns: it runs every six
    hours and would otherwise reset the one fact only a restore can produce.
    And, as with wal_ready_count, CREATE TABLE IF NOT EXISTS cannot add them
    to a live deployment, so the ALTER has to exist.
    """

    postgres = (REPO_ROOT / "src/personal_data_warehouse/postgres.py").read_text()
    loop = (REPO_ROOT / "docker/postgres-pgbackrest/backup-loop.sh").read_text()

    for column in ("last_restore_verified_at", "last_restore_label", "last_restore_rows", "last_restore_note"):
        assert f'("{column}",' in postgres, f"{column} is not migrated onto existing tables"
        assert column not in loop, f"the backup loop must never write {column}"
