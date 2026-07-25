"""Per-file upload state for the Apple Photos uploader.

Sqlite state DB keyed by (source_type, source_id) with a stable Photos-metadata
fingerprint. The metadata table wipes state whenever the account/library/schema
changes so stale completeness never suppresses uploads against a different
library.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

STATE_SCHEMA_VERSION = "1"

SOURCE_TYPE_ASSET_FILE = "asset_file"


@dataclass(frozen=True)
class PhotosUploadStateEntry:
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
    # Consecutive failed attempts for this fingerprint, and when the current
    # failing streak started. They drive the runner's retry backoff, so a file
    # PhotoKit will never export cannot hold the whole schedule hostage.
    failure_count: int = 0
    first_failure_at: str = ""


class PhotosUploadState:
    def __init__(self, *, path: Path, account: str, library_path: Path | str) -> None:
        self.path = path
        self.account = account
        self.library_path = str(library_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(path)
        self._connection.row_factory = sqlite3.Row
        self._ensure_schema()
        self._ensure_metadata()

    @classmethod
    def open(cls, path: Path, *, account: str, library_path: Path | str) -> PhotosUploadState:
        return cls(path=path, account=account, library_path=library_path)

    def close(self) -> None:
        self._connection.close()

    def entry_for(self, *, source_type: str, source_id: str) -> PhotosUploadStateEntry | None:
        row = self._connection.execute(
            """
            SELECT source_type, source_id, fingerprint, complete, content_sha256, storage_key,
                   last_success_at, last_failure_at, last_error, last_checked_at,
                   failure_count, first_failure_at
            FROM upload_state
            WHERE source_type = ? AND source_id = ?
            """,
            (source_type, source_id),
        ).fetchone()
        if row is None:
            return None
        return PhotosUploadStateEntry(
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
            failure_count=int(row["failure_count"] or 0),
            first_failure_at=str(row["first_failure_at"]),
        )

    def latest_success_at(self) -> datetime | None:
        """When this uploader last proved the whole export path works."""
        row = self._connection.execute(
            "SELECT MAX(last_success_at) AS moment FROM upload_state WHERE last_success_at != ''"
        ).fetchone()
        moment = str(row["moment"] or "") if row is not None else ""
        if not moment:
            return None
        try:
            return datetime.fromisoformat(moment)
        except ValueError:
            return None

    def clear_failures(self) -> int:
        """Forget every failing streak so backed-off files retry immediately."""
        cursor = self._connection.execute(
            "UPDATE upload_state SET failure_count = 0, first_failure_at = '' WHERE failure_count != 0"
        )
        self._connection.commit()
        return int(cursor.rowcount or 0)

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
                last_success_at, last_failure_at, last_error, last_checked_at,
                failure_count, first_failure_at
            )
            VALUES (?, ?, ?, 1, ?, ?, ?, '', '', ?, 0, '')
            ON CONFLICT(source_type, source_id) DO UPDATE SET
                fingerprint = excluded.fingerprint,
                complete = excluded.complete,
                content_sha256 = excluded.content_sha256,
                storage_key = excluded.storage_key,
                last_success_at = excluded.last_success_at,
                last_failure_at = '',
                last_error = '',
                last_checked_at = excluded.last_checked_at,
                failure_count = 0,
                first_failure_at = ''
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
    ) -> PhotosUploadStateEntry:
        """Record one failed attempt and return the updated entry.

        Attempts accumulate only while the fingerprint is unchanged: a re-edited
        asset is a different file to upload and starts a fresh streak.
        """
        timestamp = now.astimezone(UTC).isoformat()
        existing = self.entry_for(source_type=source_type, source_id=source_id)
        same_file = existing is not None and existing.fingerprint == fingerprint
        failure_count = (existing.failure_count if same_file and existing else 0) + 1
        first_failure_at = (
            existing.first_failure_at if same_file and existing and existing.first_failure_at else timestamp
        )
        self._connection.execute(
            """
            INSERT INTO upload_state (
                source_type, source_id, fingerprint, complete, content_sha256, storage_key,
                last_success_at, last_failure_at, last_error, last_checked_at,
                failure_count, first_failure_at
            )
            VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_type, source_id) DO UPDATE SET
                fingerprint = excluded.fingerprint,
                complete = 0,
                last_failure_at = excluded.last_failure_at,
                last_error = excluded.last_error,
                last_checked_at = excluded.last_checked_at,
                failure_count = excluded.failure_count,
                first_failure_at = excluded.first_failure_at
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
                failure_count,
                first_failure_at,
            ),
        )
        self._connection.commit()
        entry = self.entry_for(source_type=source_type, source_id=source_id)
        assert entry is not None  # just written
        return entry

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
                failure_count INTEGER NOT NULL DEFAULT 0,
                first_failure_at TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (source_type, source_id)
            )
            """
        )
        # Add retry-tracking columns to state files written before they existed.
        # Bumping STATE_SCHEMA_VERSION instead would wipe every completed row
        # and re-upload the whole library.
        present = {
            str(row["name"])
            for row in self._connection.execute("PRAGMA table_info(upload_state)").fetchall()
        }
        for column, definition in (
            ("failure_count", "INTEGER NOT NULL DEFAULT 0"),
            ("first_failure_at", "TEXT NOT NULL DEFAULT ''"),
        ):
            if column not in present:
                self._connection.execute(
                    f"ALTER TABLE upload_state ADD COLUMN {column} {definition}"  # noqa: S608 - fixed names
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
            "library_path": self.library_path,
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
    return Path.home() / "Library" / "Application Support" / "personal-data-warehouse" / "photos-upload-state.sqlite"
