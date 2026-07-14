"""Per-document upload state for the manual-finance uploader.

Sqlite state keyed by the document's CONTENT SHA (a document is its bytes),
so completeness survives files moving between folders or upload roots — the
uploader hashes every candidate anyway, and the server dedups by sha, so the
state only exists to skip re-transferring bytes. The metadata table wipes
state when the account or schema version changes.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

STATE_SCHEMA_VERSION = "1"


class ManualFinanceUploadState:
    def __init__(self, *, path: Path, account: str) -> None:
        self.path = path
        self.account = account
        path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(path)
        self._connection.row_factory = sqlite3.Row
        self._ensure_schema()
        self._ensure_metadata()

    @classmethod
    def open(cls, path: Path, *, account: str) -> "ManualFinanceUploadState":
        return cls(path=path, account=account)

    def close(self) -> None:
        self._connection.close()

    def is_complete(self, *, content_sha256: str) -> bool:
        row = self._connection.execute(
            "SELECT complete FROM upload_state WHERE content_sha256 = ?",
            (content_sha256,),
        ).fetchone()
        return bool(row and row["complete"])

    def mark_success(self, *, content_sha256: str, original_path: str, now: datetime) -> None:
        timestamp = now.astimezone(UTC).isoformat()
        self._connection.execute(
            """
            INSERT INTO upload_state (content_sha256, original_path, complete, last_success_at, last_error)
            VALUES (?, ?, 1, ?, '')
            ON CONFLICT(content_sha256) DO UPDATE SET
                original_path = excluded.original_path,
                complete = 1,
                last_success_at = excluded.last_success_at,
                last_error = ''
            """,
            (content_sha256, original_path, timestamp),
        )
        self._connection.commit()

    def mark_failure(self, *, content_sha256: str, original_path: str, error: str, now: datetime) -> None:
        timestamp = now.astimezone(UTC).isoformat()
        self._connection.execute(
            """
            INSERT INTO upload_state (content_sha256, original_path, complete, last_failure_at, last_error)
            VALUES (?, ?, 0, ?, ?)
            ON CONFLICT(content_sha256) DO UPDATE SET
                original_path = excluded.original_path,
                last_failure_at = excluded.last_failure_at,
                last_error = excluded.last_error
            """,
            (content_sha256, original_path, timestamp, error),
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
                content_sha256 TEXT PRIMARY KEY,
                original_path TEXT NOT NULL DEFAULT '',
                complete INTEGER NOT NULL DEFAULT 0,
                last_success_at TEXT NOT NULL DEFAULT '',
                last_failure_at TEXT NOT NULL DEFAULT '',
                last_error TEXT NOT NULL DEFAULT ''
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
        / "manual-finance-upload-state.sqlite"
    )
