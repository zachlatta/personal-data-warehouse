from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import sqlite3

STATE_SCHEMA_VERSION = "1"


@dataclass(frozen=True)
class AppleContactsUploadStateEntry:
    source_id: str
    contact_id: str
    fingerprint: str
    display_name: str
    is_deleted: bool
    complete: bool


class AppleContactsUploadState:
    def __init__(self, *, path: Path, account: str, store_path: Path | str) -> None:
        self.path = path
        self.account = account
        self.store_path = str(store_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(path)
        self._connection.row_factory = sqlite3.Row
        self._ensure_schema()
        self._ensure_metadata()

    @classmethod
    def open(cls, path: Path, *, account: str, store_path: Path | str) -> "AppleContactsUploadState":
        return cls(path=path, account=account, store_path=store_path)

    def close(self) -> None:
        self._connection.close()

    def entries(self) -> list[AppleContactsUploadStateEntry]:
        rows = self._connection.execute(
            """
            SELECT source_id, contact_id, fingerprint, display_name, is_deleted, complete
            FROM upload_state
            ORDER BY source_id, contact_id
            """
        ).fetchall()
        return [
            AppleContactsUploadStateEntry(
                source_id=str(row["source_id"]),
                contact_id=str(row["contact_id"]),
                fingerprint=str(row["fingerprint"]),
                display_name=str(row["display_name"]),
                is_deleted=bool(row["is_deleted"]),
                complete=bool(row["complete"]),
            )
            for row in rows
        ]

    def is_complete(self, *, source_id: str, contact_id: str, fingerprint: str) -> bool:
        row = self._connection.execute(
            """
            SELECT fingerprint, complete
            FROM upload_state
            WHERE source_id = ? AND contact_id = ?
            """,
            (source_id, contact_id),
        ).fetchone()
        return bool(row and row["complete"] and str(row["fingerprint"]) == fingerprint)

    def mark_success(
        self,
        *,
        source_id: str,
        contact_id: str,
        fingerprint: str,
        display_name: str,
        is_deleted: bool,
        now: datetime,
    ) -> None:
        timestamp = now.astimezone(UTC).isoformat()
        self._connection.execute(
            """
            INSERT INTO upload_state (
                source_id, contact_id, fingerprint, display_name, is_deleted,
                complete, last_success_at
            )
            VALUES (?, ?, ?, ?, ?, 1, ?)
            ON CONFLICT(source_id, contact_id) DO UPDATE SET
                fingerprint = excluded.fingerprint,
                display_name = excluded.display_name,
                is_deleted = excluded.is_deleted,
                complete = 1,
                last_success_at = excluded.last_success_at
            """,
            (source_id, contact_id, fingerprint, display_name, int(is_deleted), timestamp),
        )
        self._connection.commit()

    def _ensure_schema(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS upload_state (
                source_id TEXT NOT NULL,
                contact_id TEXT NOT NULL,
                fingerprint TEXT NOT NULL DEFAULT '',
                display_name TEXT NOT NULL DEFAULT '',
                is_deleted INTEGER NOT NULL DEFAULT 0,
                complete INTEGER NOT NULL DEFAULT 0,
                last_success_at TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (source_id, contact_id)
            );
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
    return (
        Path.home()
        / "Library"
        / "Application Support"
        / "personal-data-warehouse"
        / "apple-contacts-upload-state.sqlite"
    )
