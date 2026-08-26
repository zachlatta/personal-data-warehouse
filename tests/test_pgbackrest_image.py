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
