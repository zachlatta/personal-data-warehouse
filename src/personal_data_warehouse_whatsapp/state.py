from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import sqlite3

STATE_SCHEMA_VERSION = "1"


@dataclass(frozen=True)
class WhatsAppUploadStateEntry:
    source_type: str
    source_id: str
    fingerprint: str
    complete: bool
    content_sha256: str = ""
    storage_key: str = ""
    last_success_at: str = ""
    last_failure_at: str = ""
    last_error: str = ""
    last_checked_at: str = ""


class WhatsAppUploadState:
    def __init__(self, *, path: Path, account: str, store_path: Path | str) -> None:
        self.path = path
        self.account = account
        self.store_path = str(store_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        # The client daemon creates this on the main thread but only ever uses
        # it from the single flusher thread.
        self._connection = sqlite3.connect(path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._ensure_schema()
        self._ensure_metadata()

    @classmethod
    def open(cls, path: Path, *, account: str, store_path: Path | str) -> WhatsAppUploadState:
        return cls(path=path, account=account, store_path=store_path)

    def close(self) -> None:
        self._connection.close()

    def entry_for(self, *, source_type: str, source_id: str) -> WhatsAppUploadStateEntry | None:
        row = self._connection.execute(
            """
            SELECT source_type, source_id, fingerprint, complete, content_sha256, storage_key,
                   last_success_at, last_failure_at, last_error, last_checked_at
            FROM upload_state
            WHERE source_type = ? AND source_id = ?
            """,
            (source_type, source_id),
        ).fetchone()
        if row is None:
            return None
        return WhatsAppUploadStateEntry(
            source_type=str(row["source_type"]),
            source_id=str(row["source_id"]),
            fingerprint=str(row["fingerprint"]),
            complete=bool(row["complete"]),
            content_sha256=str(row["content_sha256"]),
            storage_key=str(row["storage_key"]),
            last_success_at=str(row["last_success_at"]),
            last_failure_at=str(row["last_failure_at"]),
            last_error=str(row["last_error"]),
            last_checked_at=str(row["last_checked_at"]),
        )

    def is_complete(self, *, source_type: str, source_id: str, fingerprint: str) -> bool:
        entry = self.entry_for(source_type=source_type, source_id=source_id)
        return bool(entry and entry.complete and entry.fingerprint == fingerprint)

    def mark_success(
        self,
        *,
        source_type: str,
        source_id: str,
        fingerprint: str,
        now: datetime,
        content_sha256: str = "",
        storage_key: str = "",
    ) -> None:
        timestamp = now.astimezone(UTC).isoformat()
        self._connection.execute(
            """
            INSERT INTO upload_state (
                source_type, source_id, fingerprint, complete, content_sha256, storage_key,
                last_success_at, last_failure_at, last_error, last_checked_at
            )
            VALUES (?, ?, ?, 1, ?, ?, ?, '', '', ?)
            ON CONFLICT(source_type, source_id) DO UPDATE SET
                fingerprint = excluded.fingerprint,
                complete = excluded.complete,
                content_sha256 = excluded.content_sha256,
                storage_key = excluded.storage_key,
                last_success_at = excluded.last_success_at,
                last_failure_at = '',
                last_error = '',
                last_checked_at = excluded.last_checked_at
            """,
            (source_type, source_id, fingerprint, content_sha256, storage_key, timestamp, timestamp),
        )
        self._connection.commit()

    def mark_failure(
        self,
        *,
        source_type: str,
        source_id: str,
        fingerprint: str,
        error: str,
        now: datetime,
    ) -> None:
        timestamp = now.astimezone(UTC).isoformat()
        existing = self.entry_for(source_type=source_type, source_id=source_id)
        self._connection.execute(
            """
            INSERT INTO upload_state (
                source_type, source_id, fingerprint, complete, content_sha256, storage_key,
                last_success_at, last_failure_at, last_error, last_checked_at
            )
            VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_type, source_id) DO UPDATE SET
                fingerprint = excluded.fingerprint,
                complete = 0,
                last_failure_at = excluded.last_failure_at,
                last_error = excluded.last_error,
                last_checked_at = excluded.last_checked_at
            """,
            (
                source_type,
                source_id,
                fingerprint,
                existing.content_sha256 if existing else "",
                existing.storage_key if existing else "",
                existing.last_success_at if existing else "",
                timestamp,
                error,
                timestamp,
            ),
        )
        self._connection.commit()

    def _ensure_schema(self) -> None:
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            )
            """
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS upload_state (
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                fingerprint TEXT NOT NULL DEFAULT '',
                complete INTEGER NOT NULL DEFAULT 0,
                content_sha256 TEXT NOT NULL DEFAULT '',
                storage_key TEXT NOT NULL DEFAULT '',
                last_success_at TEXT NOT NULL DEFAULT '',
                last_failure_at TEXT NOT NULL DEFAULT '',
                last_error TEXT NOT NULL DEFAULT '',
                last_checked_at TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (source_type, source_id)
            )
            """
        )
        self._connection.commit()

    def _ensure_metadata(self) -> None:
        current = {
            str(row["key"]): str(row["value"])
            for row in self._connection.execute("SELECT key, value FROM metadata").fetchall()
        }
        expected = {
            "schema_version": STATE_SCHEMA_VERSION,
            "account": self.account,
            "store_path": self.store_path,
        }
        if current and any(current.get(key) != value for key, value in expected.items()):
            self._connection.execute("DELETE FROM upload_state")
            self._connection.execute("DELETE FROM metadata")
        for key, value in expected.items():
            self._connection.execute(
                """
                INSERT INTO metadata (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )
        self._connection.commit()


def default_state_file() -> Path:
    return Path.home() / "Library" / "Application Support" / "personal-data-warehouse" / "whatsapp-upload-state.sqlite"
