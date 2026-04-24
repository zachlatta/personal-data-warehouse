from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
import tempfile


@contextmanager
def exclusive_sync_lock(*, name: str, postgres_lock_id: int) -> Iterator[bool]:
    postgres_url = sync_lock_postgres_url(name)
    if postgres_url:
        with exclusive_postgres_advisory_lock(postgres_url, postgres_lock_id) as acquired:
            yield acquired
        return

    with exclusive_process_lock(sync_lock_path(name)) as acquired:
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
def exclusive_postgres_advisory_lock(postgres_url: str, lock_id: int) -> Iterator[bool]:
    import psycopg2

    connection = psycopg2.connect(postgres_url)
    connection.autocommit = True
    cursor = connection.cursor()
    acquired = False
    try:
        cursor.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
        acquired = bool(cursor.fetchone()[0])
        yield acquired
    finally:
        if acquired:
            cursor.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))
        cursor.close()
        connection.close()


@contextmanager
def exclusive_process_lock(path: Path) -> Iterator[bool]:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_file = path.open("a+")
    try:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    finally:
        lock_file.close()
