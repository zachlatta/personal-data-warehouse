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
    """

    entrypoint = ENTRYPOINT.read_text()
    readme = README.read_text()

    assert "io-timeout=${PGBACKREST_IO_TIMEOUT:-600}" in entrypoint
    assert "PGBACKREST_IO_TIMEOUT=600" in readme
