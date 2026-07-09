"""Pull Claude Desktop (claude.ai) conversations and ship them through app ingest.

claude.ai keeps no local transcript, so unlike the agent-sessions uploader (which
tails on-disk JSONL) this runner *polls* the claude.ai API: it lists
conversations, and for every one whose ``updated_at`` advanced past the stored
cursor it fetches the full message tree and emits one ``conversation`` header
line plus one ``message`` line per turn.

Those lines ride the exact same envelope/batch/ingest path as the agent-sessions
sources - ``record_type="claude_desktop_event"``, ``tool="claude_desktop"`` -
posted to ``/ingest/agent-sessions/batch``. The app writes them into the
``agent-sessions/inbox`` Drive folder and the existing
``agent_sessions_drive_ingest`` asset normalizes them into ``claude_desktop.events``
plus the unified ``marts.ai_conversation_events`` surface. The warehouse dedupes by
``(source, session_id, event_uuid)``, so re-shipping a whole conversation when it
gains one new turn is cheap and idempotent.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
import gzip
import json
import tempfile
from typing import Any

from personal_data_warehouse_claude_desktop.api import ClaudeAiClient, ClaudeAiRateLimitError
from personal_data_warehouse_claude_desktop.state import ClaudeDesktopSyncState

TOOL = "claude_desktop"
DEFAULT_BATCH_SIZE = 2000
DEFAULT_PAGE_SIZE = 50

StoredObject = dict[str, str]


class UploadBlockedError(RuntimeError):
    """Raised when a network/preflight guard blocks the upload."""


@dataclass(frozen=True)
class ClaudeDesktopUploadSummary:
    conversations_seen: int
    conversations_changed: int
    messages_uploaded: int
    batches_uploaded: int
    deferred_for_limit: bool
    rate_limited: bool = False


class ClaudeDesktopUploadRunner:
    def __init__(
        self,
        *,
        account: str,
        device: str,
        client: ClaudeAiClient,
        batch_uploader: Callable[[bytes, datetime], StoredObject],
        logger,
        upload_state: ClaudeDesktopSyncState | None = None,
        now: Callable[[], datetime] | None = None,
        mode: str = "incremental",
        limit: int | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        page_size: int = DEFAULT_PAGE_SIZE,
        before_upload_check: Callable[[], str | None] | None = None,
    ) -> None:
        if batch_uploader is None:
            raise ValueError("batch_uploader is required")
        if mode not in {"full", "incremental"}:
            raise ValueError("mode must be 'full' or 'incremental'")
        self._account = account
        self._device = device
        self._client = client
        self._batch_uploader = batch_uploader
        self._logger = logger
        self._state = upload_state
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._mode = mode
        self._limit = limit if (limit is None or limit > 0) else None
        self._batch_size = max(1, batch_size)
        self._page_size = max(1, page_size)
        self._before_upload_check = before_upload_check
        self._checked_network = False

    def sync(self) -> ClaudeDesktopUploadSummary:
        batch = _PendingBatch(batch_size=self._batch_size)
        conversations_seen = 0
        conversations_changed = 0
        messages_uploaded = 0
        remaining = self._limit
        deferred = False
        rate_limited = False

        try:
            for summary in self._client.iter_conversations(page_size=self._page_size):
                conversation_id = str(summary.get("uuid", "")).strip()
                if not conversation_id:
                    continue
                conversations_seen += 1
                updated_at = str(summary.get("updated_at", ""))
                if self._is_unchanged(conversation_id, updated_at):
                    continue
                if remaining is not None and remaining <= 0:
                    deferred = True
                    break

                try:
                    conversation = self._client.get_conversation(conversation_id)
                except ClaudeAiRateLimitError as exc:
                    rate_limited = True
                    self._logger.warning(
                        "claude.ai rate limited while fetching conversation %s%s; "
                        "stopping this poll and continuing on the next tick",
                        conversation_id,
                        _retry_suffix(exc),
                    )
                    break
                effective_updated_at = str(conversation.get("updated_at", "") or updated_at)
                for seq, line in enumerate(conversation_lines(conversation)):
                    batch.add(self._envelope(conversation_id=conversation_id, seq=seq, line=line))
                    if line.get("type") == "message":
                        messages_uploaded += 1
                    if batch.full():
                        self._flush(batch)
                # Cursor only after the whole conversation is buffered, so it commits
                # in lockstep with the flush that durably stores its final message.
                batch.register_cursor(conversation_id, effective_updated_at)
                conversations_changed += 1
                if remaining is not None:
                    remaining -= 1
        except ClaudeAiRateLimitError as exc:
            rate_limited = True
            self._logger.warning(
                "claude.ai rate limited while listing conversations%s; "
                "stopping this poll and continuing on the next tick",
                _retry_suffix(exc),
            )

        self._flush(batch)  # trailing partial batch + any pending cursors

        self._logger.info(
            "Claude Desktop upload summary: seen=%s changed=%s messages=%s batches=%s%s%s",
            conversations_seen,
            conversations_changed,
            messages_uploaded,
            batch.batches,
            " (run limit reached; remaining conversations deferred to next run)" if deferred else "",
            " (rate limited)" if rate_limited else "",
        )
        return ClaudeDesktopUploadSummary(
            conversations_seen=conversations_seen,
            conversations_changed=conversations_changed,
            messages_uploaded=messages_uploaded,
            batches_uploaded=batch.batches,
            deferred_for_limit=deferred,
            rate_limited=rate_limited,
        )

    def _is_unchanged(self, conversation_id: str, updated_at: str) -> bool:
        if self._mode != "incremental" or self._state is None:
            return False
        return bool(updated_at) and updated_at == self._state.updated_at_for(conversation_id)

    def _flush(self, batch: "_PendingBatch") -> None:
        if not batch.records and not batch.cursors:
            return
        if batch.records:
            self._run_network_check()
            stored = self._upload_batch(batch.records)
            self._logger.info(
                "Uploaded claude-desktop batch %s with %s records",
                stored["storage_key"],
                len(batch.records),
            )
        # Every registered cursor's records were uploaded in this or a prior
        # flush (cursors are registered only after a conversation is fully
        # buffered), so committing them now is safe.
        for conversation_id, updated_at in batch.cursors.items():
            self._commit(conversation_id, updated_at)
        batch.reset()

    def _run_network_check(self) -> None:
        if self._before_upload_check is None or self._checked_network:
            return
        self._checked_network = True
        reason = self._before_upload_check()
        if reason:
            raise UploadBlockedError(reason)

    def _upload_batch(self, records: list[dict[str, Any]]) -> StoredObject:
        encoded = _gzip_jsonl(records)
        return self._batch_uploader(encoded, self._now())

    def _commit(self, conversation_id: str, updated_at: str) -> None:
        if self._state is not None:
            self._state.record_progress(
                conversation_id=conversation_id, updated_at=updated_at, now=self._now()
            )

    def _envelope(self, *, conversation_id: str, seq: int, line: dict[str, Any]) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "source": "agent_sessions",
            "account": self._account,
            "device": self._device,
            "exported_at": self._now().astimezone(UTC).isoformat(),
            "record_type": "claude_desktop_event",
            "record": {
                "tool": TOOL,
                "session_id": conversation_id,
                "seq": seq,
                "line": line,
            },
        }


def _retry_suffix(exc: ClaudeAiRateLimitError) -> str:
    if exc.retry_after_seconds is None:
        return ""
    return f" (retry_after={exc.retry_after_seconds:g}s)"


def conversation_lines(conversation: dict[str, Any]) -> list[dict[str, Any]]:
    """Turn one claude.ai conversation tree into ordered ingest "line" dicts.

    The first line is a synthetic ``conversation`` header carrying conversation
    metadata (name/model/timestamps); the rest are the raw API messages tagged
    with ``type="message"``. The conversation's model is copied onto assistant
    messages so per-event rows carry it (the API only returns model at the
    conversation level).
    """
    conversation_id = str(conversation.get("uuid", ""))
    model = str(conversation.get("model", ""))
    header = {
        "type": "conversation",
        "uuid": conversation_id,
        "name": conversation.get("name", ""),
        "summary": conversation.get("summary", ""),
        "model": model,
        "created_at": conversation.get("created_at", ""),
        "updated_at": conversation.get("updated_at", ""),
        "settings": conversation.get("settings"),
        "is_starred": conversation.get("is_starred"),
        "project_uuid": conversation.get("project_uuid", ""),
        "current_leaf_message_uuid": conversation.get("current_leaf_message_uuid", ""),
    }
    lines: list[dict[str, Any]] = [header]
    messages = conversation.get("chat_messages")
    if isinstance(messages, list):
        for message in messages:
            if not isinstance(message, dict):
                continue
            line = dict(message)
            line["type"] = "message"
            line["conversation_uuid"] = conversation_id
            if str(message.get("sender", "")) == "assistant" and not line.get("model"):
                line["model"] = model
            lines.append(line)
    return lines


@dataclass
class _PendingBatch:
    """Envelopes accumulating across conversations until they reach batch_size.

    ``cursors`` maps a fully-buffered conversation to the ``updated_at`` to
    persist once its records are durably uploaded.
    """

    batch_size: int
    records: list[dict[str, Any]] = field(default_factory=list)
    cursors: dict[str, str] = field(default_factory=dict)
    batches: int = 0

    def add(self, record: dict[str, Any]) -> None:
        self.records.append(record)

    def register_cursor(self, conversation_id: str, updated_at: str) -> None:
        self.cursors[conversation_id] = updated_at

    def full(self) -> bool:
        return len(self.records) >= self.batch_size

    def reset(self) -> None:
        if self.records:
            self.batches += 1
        self.records = []
        self.cursors = {}


def _gzip_jsonl(records: list[dict[str, Any]]) -> bytes:
    lines = b"".join(
        json.dumps(record, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8") + b"\n"
        for record in records
    )
    with tempfile.NamedTemporaryFile() as file:
        with gzip.GzipFile(fileobj=file, mode="wb", mtime=0) as gzip_file:
            gzip_file.write(lines)
        file.flush()
        file.seek(0)
        return file.read()
