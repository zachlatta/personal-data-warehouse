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
