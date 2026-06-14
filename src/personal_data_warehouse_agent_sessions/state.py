"""Incremental upload state for agent session transcripts.

The transcripts are append-only, so the only thing we need to remember per file
is how many bytes (and lines) we have already shipped. The next run reads from
that byte offset onward. Line counts let us assign each line a stable absolute
sequence number, which becomes part of the warehouse event's primary key.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import sqlite3

STATE_SCHEMA_VERSION = "1"


@dataclass(frozen=True)
class FileProgress:
    path: str
    uploaded_offset: int
    uploaded_lines: int


class AgentSessionsUploadState:
    def __init__(self, *, path: Path, account: str) -> None:
        self.path = path
        self.account = account
        path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(path)
        self._connection.row_factory = sqlite3.Row
        self._ensure_schema()
        self._ensure_metadata()

    @classmethod
    def open(cls, path: Path, *, account: str) -> "AgentSessionsUploadState":
        return cls(path=path, account=account)

    def close(self) -> None:
        self._connection.close()

    def progress_for(self, path: str) -> FileProgress:
        row = self._connection.execute(
            "SELECT path, uploaded_offset, uploaded_lines FROM file_state WHERE path = ?",
            (path,),
        ).fetchone()
        if row is None:
            return FileProgress(path=path, uploaded_offset=0, uploaded_lines=0)
        return FileProgress(
            path=str(row["path"]),
            uploaded_offset=int(row["uploaded_offset"]),
            uploaded_lines=int(row["uploaded_lines"]),
        )

    def record_progress(self, *, path: str, uploaded_offset: int, uploaded_lines: int, now: datetime) -> None:
        self._connection.execute(
            """
            INSERT INTO file_state (path, uploaded_offset, uploaded_lines, last_uploaded_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                uploaded_offset = excluded.uploaded_offset,
                uploaded_lines = excluded.uploaded_lines,
                last_uploaded_at = excluded.last_uploaded_at
            """,
            (path, uploaded_offset, uploaded_lines, now.astimezone(UTC).isoformat()),
        )
        self._connection.commit()

    def reset(self) -> None:
        self._connection.execute("DELETE FROM file_state")
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
            CREATE TABLE IF NOT EXISTS file_state (
                path TEXT PRIMARY KEY,
                uploaded_offset INTEGER NOT NULL DEFAULT 0,
                uploaded_lines INTEGER NOT NULL DEFAULT 0,
                last_uploaded_at TEXT NOT NULL DEFAULT ''
            )
            """
        )
        self._connection.commit()

    def _ensure_metadata(self) -> None:
        current = {
            str(row["key"]): str(row["value"])
            for row in self._connection.execute("SELECT key, value FROM metadata").fetchall()
        }
        expected = {"schema_version": STATE_SCHEMA_VERSION, "account": self.account}
        if current and any(current.get(key) != value for key, value in expected.items()):
            self._connection.execute("DELETE FROM file_state")
            self._connection.execute("DELETE FROM metadata")
        for key, value in expected.items():
            self._connection.execute(
                "INSERT INTO metadata (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
        self._connection.commit()


def default_state_file() -> Path:
    return (
        Path.home()
        / "Library"
        / "Application Support"
        / "personal-data-warehouse"
        / "agent-sessions-upload-state.sqlite"
    )
