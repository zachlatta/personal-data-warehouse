from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
import tempfile
import time

# How often the process-lock fallback re-checks a contended lock while waiting.
_PROCESS_LOCK_POLL_SECONDS = 0.1


@contextmanager
def exclusive_sync_lock(
    *,
    name: str,
    postgres_lock_id: int,
    wait_seconds: float | None = None,
) -> Iterator[bool]:
    """Acquire the named cross-process sync lock.

    By default this is a non-blocking try-lock: ``acquired`` is ``False`` the
    instant another holder has it, so callers can skip gracefully. When
    ``wait_seconds`` is set the acquisition blocks for up to that long, yielding
    ``True`` as soon as the current holder releases and ``False`` only if the
    wait elapses first. This lets a heavyweight, infrequent stage queue behind
    short high-frequency stages instead of losing a race against them.
    """
    postgres_url = sync_lock_postgres_url(name)
    if postgres_url:
        with exclusive_postgres_advisory_lock(
            postgres_url, postgres_lock_id, wait_seconds=wait_seconds
        ) as acquired:
            yield acquired
        return

    with exclusive_process_lock(sync_lock_path(name), wait_seconds=wait_seconds) as acquired:
        yield acquired


def sync_lock_postgres_url(name: str) -> str | None:
    env_prefix = lock_env_prefix(name)
    return (
        os.getenv(f"{env_prefix}_SYNC_LOCK_POSTGRES_URL")
        or os.getenv("DAGSTER_POSTGRES_URL")
        or os.getenv("DATABASE_URL")
        or None
    )


def sync_lock_path(name: str) -> Path:
    env_prefix = lock_env_prefix(name)
    default_path = Path(tempfile.gettempdir()) / f"personal-data-warehouse-{name}-sync.lock"
    return Path(os.getenv(f"{env_prefix}_SYNC_LOCK_PATH", str(default_path))).expanduser()


def lock_env_prefix(name: str) -> str:
    return "".join(character if character.isalnum() else "_" for character in name).upper()


@contextmanager
def exclusive_postgres_advisory_lock(
    postgres_url: str, lock_id: int, *, wait_seconds: float | None = None
) -> Iterator[bool]:
    import psycopg2

    connection = psycopg2.connect(postgres_url)
    connection.autocommit = True
    cursor = connection.cursor()
    acquired = False
    try:
        if wait_seconds is None:
            cursor.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
            acquired = bool(cursor.fetchone()[0])
        else:
            # lock_timeout bounds the blocking pg_advisory_lock wait; when it
            # fires Postgres raises LockNotAvailable (SQLSTATE 55P03), which we
            # translate to "not acquired" rather than an error.
            cursor.execute("SET lock_timeout = %s", (max(1, int(wait_seconds * 1000)),))
            try:
                cursor.execute("SELECT pg_advisory_lock(%s)", (lock_id,))
                cursor.fetchone()
                acquired = True
            except psycopg2.errors.LockNotAvailable:
                acquired = False
        yield acquired
    finally:
        if acquired:
            # The lock connection is dedicated to this context. Release every
            # advisory lock it holds so repeated acquisition on the same session
            # cannot leave a session-level lock behind after the job succeeds.
            cursor.execute("SELECT pg_advisory_unlock_all()")
        cursor.close()
        connection.close()


@contextmanager
def exclusive_process_lock(path: Path, *, wait_seconds: float | None = None) -> Iterator[bool]:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_file = path.open("a+")
    try:
        acquired = _acquire_flock(lock_file, wait_seconds=wait_seconds)
        if not acquired:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    finally:
        lock_file.close()


def _acquire_flock(lock_file, *, wait_seconds: float | None) -> bool:
    if _try_flock(lock_file):
        return True
    if not wait_seconds:
        return False
    deadline = time.monotonic() + wait_seconds
    while time.monotonic() < deadline:
        time.sleep(_PROCESS_LOCK_POLL_SECONDS)
        if _try_flock(lock_file):
            return True
    return False


def _try_flock(lock_file) -> bool:
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except BlockingIOError:
        return False
