"""Incremental upload state for Claude Desktop (claude.ai) conversations.

Unlike the append-only file transcripts the agent-sessions uploader tails,
claude.ai conversations are mutable: new turns append and a conversation's
``updated_at`` advances. So the cursor we remember per conversation is its last
uploaded ``updated_at``. A run only re-fetches and re-ships conversations whose
``updated_at`` moved past the stored value; the warehouse dedupes the re-shipped
messages by primary key, so re-uploading a whole conversation is cheap and safe.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import sqlite3

STATE_SCHEMA_VERSION = "1"


@dataclass(frozen=True)
class ConversationProgress:
    conversation_id: str
    updated_at: str


class ClaudeDesktopSyncState:
    def __init__(self, *, path: Path, account: str) -> None:
        self.path = path
        self.account = account
        path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(path)
        self._connection.row_factory = sqlite3.Row
        self._ensure_schema()
        self._ensure_metadata()

    @classmethod
    def open(cls, path: Path, *, account: str) -> "ClaudeDesktopSyncState":
        return cls(path=path, account=account)

    def close(self) -> None:
        self._connection.close()

    def updated_at_for(self, conversation_id: str) -> str:
        row = self._connection.execute(
            "SELECT updated_at FROM conversation_state WHERE conversation_id = ?",
            (conversation_id,),
        ).fetchone()
        return str(row["updated_at"]) if row is not None else ""

    def record_progress(self, *, conversation_id: str, updated_at: str, now: datetime) -> None:
        self._connection.execute(
            """
            INSERT INTO conversation_state (conversation_id, updated_at, last_uploaded_at)
            VALUES (?, ?, ?)
            ON CONFLICT(conversation_id) DO UPDATE SET
                updated_at = excluded.updated_at,
                last_uploaded_at = excluded.last_uploaded_at
            """,
            (conversation_id, updated_at, now.astimezone(UTC).isoformat()),
        )
        self._connection.commit()

    def reset(self) -> None:
        self._connection.execute("DELETE FROM conversation_state")
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
            CREATE TABLE IF NOT EXISTS conversation_state (
                conversation_id TEXT PRIMARY KEY,
                updated_at TEXT NOT NULL DEFAULT '',
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
            self._connection.execute("DELETE FROM conversation_state")
            self._connection.execute("DELETE FROM metadata")
        for key, value in expected.items():
            self._connection.execute(
                "INSERT INTO metadata (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
        self._connection.commit()


class WarehouseSyncState:
    """Postgres-durable per-conversation cursor, used by the serverside poller.

    Implements the same ``updated_at_for`` / ``record_progress`` surface as
    :class:`ClaudeDesktopSyncState` so :class:`ClaudeDesktopUploadRunner` is
    storage-agnostic. Scoped by ``account`` because the warehouse table is
    shared across machines/accounts. Backed by
    ``PostgresWarehouse.claude_desktop_cursor`` / ``record_claude_desktop_cursor``.
    """

    def __init__(self, *, warehouse, account: str) -> None:
        self._warehouse = warehouse
        self._account = account

    def updated_at_for(self, conversation_id: str) -> str:
        return self._warehouse.claude_desktop_cursor(
            account=self._account, conversation_id=conversation_id
        )

    def record_progress(self, *, conversation_id: str, updated_at: str, now: datetime) -> None:
        self._warehouse.record_claude_desktop_cursor(
            account=self._account,
            conversation_id=conversation_id,
            updated_at=updated_at,
            now=now,
        )


def default_state_file() -> Path:
    return (
        Path.home()
        / "Library"
        / "Application Support"
        / "personal-data-warehouse"
        / "claude-desktop-upload-state.sqlite"
    )
