from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import hashlib
import sqlite3
import tempfile


@dataclass(frozen=True)
class SessionSnapshot:
    account: str
    session_key: str
    client_id: str
    database_sha256: str
    database_bytes_size: int
    updated_at: datetime


def default_client_id(*, account: str, session_key: str) -> str:
    return f"personal-data-warehouse:whatsapp:{account}:{session_key}"


class PostgresWhatsAppSessionStore:
    def __init__(
        self,
        *,
        warehouse,
        account: str,
        session_key: str,
        configured_client_id: str = "",
    ) -> None:
        self._warehouse = warehouse
        self._account = account
        self._session_key = session_key
        self._configured_client_id = configured_client_id

    def restore_to_path(self, path: Path) -> str:
        """Restore canonical Postgres session bytes to a runtime SQLite file.

        If Postgres has no snapshot yet but a local runtime file already
        exists, import that file first. That preserves the linked device when
        migrating from the old filesystem-backed setup.
        """
        path = path.expanduser()
        row = self._warehouse.get_whatsapp_client_session(
            account=self._account,
            session_key=self._session_key,
        )
        if row and row.get("database_bytes"):
            client_id = str(
                row.get("client_id")
                or self._configured_client_id
                or default_client_id(account=self._account, session_key=self._session_key)
            )
            _write_atomic(path, bytes(row["database_bytes"]))
            return client_id

        client_id = self._configured_client_id or (
            str(path)
            if path.exists()
            else default_client_id(account=self._account, session_key=self._session_key)
        )
        if path.exists():
            self.snapshot_from_path(path, client_id=client_id)
        return client_id

    def snapshot_from_path(self, path: Path, *, client_id: str) -> SessionSnapshot | None:
        path = path.expanduser()
        if not path.exists():
            return None
        database_bytes = sqlite_database_bytes(path)
        if not database_bytes:
            return None
        updated_at = datetime.now(tz=UTC)
        row = self._warehouse.upsert_whatsapp_client_session(
            account=self._account,
            session_key=self._session_key,
            client_id=client_id,
            database_bytes=database_bytes,
            updated_at=updated_at,
        )
        return SessionSnapshot(
            account=self._account,
            session_key=self._session_key,
            client_id=client_id,
            database_sha256=str(row["database_sha256"]),
            database_bytes_size=int(row["database_bytes_size"]),
            updated_at=updated_at,
        )


def sqlite_database_bytes(path: Path) -> bytes:
    """Return a consistent SQLite database image, folding any WAL content in."""
    path = path.expanduser()
    with tempfile.NamedTemporaryFile(suffix=".sqlite3") as tmp:
        source = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)
        try:
            target = sqlite3.connect(tmp.name)
            try:
                source.backup(target)
            finally:
                target.close()
        finally:
            source.close()
        return Path(tmp.name).read_bytes()


def _write_atomic(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    expected_sha = hashlib.sha256(content).hexdigest()
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        tmp.write(content)
        tmp.flush()
        tmp_path = Path(tmp.name)
    actual_sha = hashlib.sha256(tmp_path.read_bytes()).hexdigest()
    if actual_sha != expected_sha:
        tmp_path.unlink(missing_ok=True)
        raise RuntimeError("restored WhatsApp session snapshot failed checksum verification")
    _remove_sqlite_sidecars(path)
    tmp_path.replace(path)
    _remove_sqlite_sidecars(path)


def _remove_sqlite_sidecars(path: Path) -> None:
    for suffix in ("-wal", "-shm", "-journal"):
        Path(f"{path}{suffix}").unlink(missing_ok=True)
