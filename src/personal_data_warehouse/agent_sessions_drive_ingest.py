"""Ingest AI agent session logs into source-owned AI event tables.

Three transports feed the same row schema:

* The CLI tools (Claude Code, Codex, OpenClaw, pi) write append-only JSONL
  transcripts that a per-device uploader tails and batches into
  ``agent-sessions/inbox/batches/`` as gzipped JSONL envelopes. This module
  consumes those batches and normalizes each raw line into source-owned rows
  (``claude_code.events``, ``codex.events``, ``openclaw.events``, or ``pi.events``; lossless
  ``raw_json`` plus queryable columns), then promotes processed batch files into
  ``agent-sessions/library/``.
* Claude Desktop is polled server-side from claude.ai using a credential pushed
  from the local desktop app. It ships ``claude_desktop_event`` envelopes through
  the same Drive inbox, and ``claude_desktop_event_row`` normalizes them into
  ``claude_desktop.events`` rows.
* ChatGPT (the consumer product) is polled server-side from its backend API,
  which returns each conversation as a node-tree. ``chatgpt_conversation_to_event_rows``
  linearizes that tree into ``chatgpt.events`` rows; see
  ``defs/chatgpt_backend_ingest.py``.

The unified raw event surface is ``marts.ai_conversation_events``. The
session-level roll-up (counts, token sums, header) is
``marts.ai_conversation_sessions``, so it stays correct no matter how a
session's lines are split across batches over time, and treats every ``source``
uniformly.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
import gzip
import json
import threading
from typing import Any

from personal_data_warehouse.apple_voice_memos_drive_ingest import parse_datetime
from personal_data_warehouse.objectstore import ObjectListing, ObjectStore

OBJECT_PREFIX = "agent-sessions"
INBOX_PREFIX = f"{OBJECT_PREFIX}/inbox/"
LIBRARY_PREFIX = f"{OBJECT_PREFIX}/library/"
BATCH_KIND = "agent_sessions_export_batch"

# Roles that carry conversational content worth surfacing in search_text().
_TEXT_SEARCH_ROLES = ("user", "assistant")


@dataclass(frozen=True)
class AgentSessionsDriveIngestSummary:
    batches_seen: int
    events_written: int
    files_promoted: int


class AgentSessionsDriveIngestRunner:
    def __init__(
        self,
        *,
        warehouse,
        batch_source: Callable[[], Iterable[Mapping[str, Any]]],
        object_store: ObjectStore | None = None,
        object_store_factory: Callable[[], ObjectStore] | None = None,
        promotion_workers: int = 1,
        logger,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if object_store is not None and object_store_factory is not None:
            raise ValueError("pass only one of object_store or object_store_factory")
        self._warehouse = warehouse
        self._batch_source = batch_source
        self._object_store = object_store
        self._object_store_factory = object_store_factory
        self._promotion_workers = max(1, promotion_workers)
        self._thread_local = threading.local()
        self._logger = logger
        self._now = now or (lambda: datetime.now(tz=UTC))

    def sync(self) -> AgentSessionsDriveIngestSummary:
        self._warehouse.ensure_agent_sessions_tables()
        ingested_at = self._now()
        batches = list(self._batch_source())
        records = sorted(
            [record for batch in batches for record in batch_records(batch)],
            key=record_exported_at,
        )
        event_rows = dedupe_rows(
            [record_to_event_row(record, ingested_at=ingested_at) for record in records],
            ("source", "session_id", "event_uuid"),
        )
        self._warehouse.insert_agent_session_events(event_rows)

        promoted = 0
        promote = self._object_store is not None or self._object_store_factory is not None
        if promote:
            if self._promotion_workers == 1 or len(batches) <= 1:
                promoted = sum(self._promote_batch(batch) for batch in batches)
            else:
                with ThreadPoolExecutor(max_workers=self._promotion_workers, thread_name_prefix="agent-sessions-promote") as executor:
                    futures = [executor.submit(self._promote_batch, batch) for batch in batches]
                    promoted = sum(future.result() for future in as_completed(futures))

        self._logger.info(
            "Ingested %s agent-session batches, %s events",
            len(batches),
            len(event_rows),
        )
        return AgentSessionsDriveIngestSummary(
            batches_seen=len(batches),
            events_written=len(event_rows),
            files_promoted=promoted,
        )

    def _promote_batch(self, batch: Mapping[str, Any]) -> int:
        return promote_batch(self._object_store_for_thread(), batch)

    def _object_store_for_thread(self) -> ObjectStore:
        if self._object_store is not None:
            return self._object_store
        store = getattr(self._thread_local, "object_store", None)
        if store is None:
            if self._object_store_factory is None:
                raise RuntimeError("object_store_factory is not configured")
            store = self._object_store_factory()
            self._thread_local.object_store = store
        return store


# --- batch plumbing ---------------------------------------------------------


def iter_batch_payloads(*, object_store: ObjectStore, stage: str = "inbox") -> Iterable[Mapping[str, Any]]:
    for listing in object_store.list_objects(kind=BATCH_KIND, stage=stage):
        records = list(load_jsonl_gz_object(object_store, listing.ref))
        yield {
            "schema_version": 1,
            "source": "agent_sessions",
            "batch_file": stored_file_context(
                storage_key=batch_storage_key(listing=listing, stage=stage),
                listing=listing,
                extra={"content_sha256": str(listing.app_properties.get("content_sha256", ""))},
            ),
            "records": records,
        }


def has_batch_payloads(*, object_store: ObjectStore, stage: str = "inbox") -> bool:
    return object_store.find_object(kind=BATCH_KIND, stage=stage) is not None


def load_jsonl_gz_object(object_store: ObjectStore, ref: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
    content = gzip.decompress(object_store.get_object(ref)).decode("utf-8")
    for line in content.splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if isinstance(payload, Mapping):
            yield payload


def batch_records(batch: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    records = batch.get("records", [])
    return [record for record in records if isinstance(record, Mapping)]


def promote_batch(object_store: ObjectStore, batch: Mapping[str, Any]) -> int:
    batch_file = nested_mapping(batch, "batch_file")
    file_id = str(batch_file.get("storage_file_id", ""))
    source_key = str(batch_file.get("storage_key", ""))
    target_key = library_storage_key(source_key)
    if not file_id or not target_key or target_key == source_key:
        return 0
    object_store.move_object(batch_file, new_object_key=target_key)
    return 1


def dedupe_rows(rows: list[dict[str, Any]], key_columns: tuple[str, ...]) -> list[dict[str, Any]]:
    """Collapse rows sharing key_columns, keeping the newest (last) row per key.

    Records arrive sorted oldest-first, so a later batch re-uploading the same
    immutable transcript line simply overwrites the earlier copy.
    """
    by_key: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        by_key[tuple(row.get(column) for column in key_columns)] = row
    return list(by_key.values())


def record_exported_at(record: Mapping[str, Any]) -> datetime:
    return parse_datetime(str(record.get("exported_at", "")))


# --- normalization ----------------------------------------------------------


def record_to_event_row(record: Mapping[str, Any], *, ingested_at: datetime) -> dict[str, Any]:
    payload = record.get("record")
    payload = payload if isinstance(payload, Mapping) else {}
    tool = str(payload.get("tool") or _tool_from_record_type(record))
    session_id = str(payload.get("session_id", ""))
    seq = int(payload.get("seq", 0) or 0)
    line = payload.get("line")
    line = line if isinstance(line, Mapping) else {}
    account = str(record.get("account", ""))
    device = str(record.get("device", ""))
    builder = _EVENT_ROW_BUILDERS.get(tool, claude_code_event_row)
    return builder(
        line,
        session_id=session_id,
        account=account,
        device=device,
        seq=seq,
        ingested_at=ingested_at,
    )


def _tool_from_record_type(record: Mapping[str, Any]) -> str:
    record_type = str(record.get("record_type", ""))
    return record_type[: -len("_event")] if record_type.endswith("_event") else record_type


def _base_row(
    *,
    source: str,
    session_id: str,
    account: str,
    device: str,
    seq: int,
    line: Mapping[str, Any],
    event_uuid: str,
    occurred_at: datetime,
    ingested_at: datetime,
) -> dict[str, Any]:
    return {
        "source": source,
        "session_id": session_id,
        "event_uuid": event_uuid,
        "account": account,
        "device": device,
        "seq": seq,
        "occurred_at": occurred_at,
        "role": "meta",
        "event_type": str(line.get("type", "")),
        "subtype": "",
        "parent_uuid": "",
        "turn_id": "",
        "model": "",
        "cwd": "",
        "git_branch": "",
        "git_commit": "",
        "repo_url": "",
        "cli_version": "",
        "entrypoint": "",
        "session_title": "",
        "text": "",
        "tool_name": "",
        "tool_input_json": "",
        "tool_result_json": "",
        "input_tokens": 0,
        "output_tokens": 0,
        "cache_read_tokens": 0,
        "cache_creation_tokens": 0,
        "is_sidechain": 0,
        "raw_json": raw_json(line),
        "ingested_at": ingested_at,
        "sync_version": sync_version(ingested_at),
    }


def claude_code_event_row(
    line: Mapping[str, Any],
    *,
    session_id: str,
    account: str,
    device: str,
    seq: int,
    ingested_at: datetime,
) -> dict[str, Any]:
    event_type = str(line.get("type", ""))
    uuid = str(line.get("uuid") or f"{session_id}#{seq}")
    row = _base_row(
        source="claude_code",
        session_id=session_id,
        account=account,
        device=device,
        seq=seq,
        line=line,
        event_uuid=uuid,
        occurred_at=parse_datetime(str(line.get("timestamp", ""))),
        ingested_at=ingested_at,
    )
    row["parent_uuid"] = str(line.get("parentUuid") or "")
    row["cwd"] = str(line.get("cwd", ""))
    row["git_branch"] = str(line.get("gitBranch", ""))
    row["cli_version"] = str(line.get("version", ""))
    row["entrypoint"] = str(line.get("entrypoint", ""))
    row["is_sidechain"] = int(bool(line.get("isSidechain", False)))

    message = line.get("message") if isinstance(line.get("message"), Mapping) else {}

    if event_type == "user":
        row["role"] = "user"
        content = message.get("content")
        if isinstance(content, str):
            row["subtype"] = "message"
            row["text"] = content
        elif isinstance(content, list):
            if any(isinstance(block, Mapping) and block.get("type") == "tool_result" for block in content):
                row["role"] = "tool"
                row["subtype"] = "tool_result"
                row["text"] = _join_text_blocks(content, text_types=("tool_result",))
                row["tool_result_json"] = raw_json({"content": content})
            else:
                row["subtype"] = "message"
                row["text"] = _join_text_blocks(content)
    elif event_type == "assistant":
        row["role"] = "assistant"
        row["model"] = str(message.get("model", ""))
        content = message.get("content") if isinstance(message.get("content"), list) else []
        row["text"] = _join_text_blocks(content)
        tool_block = next(
            (block for block in content if isinstance(block, Mapping) and block.get("type") == "tool_use"),
            None,
        )
        if tool_block is not None:
            row["subtype"] = "tool_use"
            row["tool_name"] = str(tool_block.get("name", ""))
            row["tool_input_json"] = raw_json(tool_block.get("input", {}))
        elif row["text"]:
            row["subtype"] = "message"
        else:
            row["subtype"] = "thinking"
        _apply_token_usage(row, message.get("usage"))
    elif event_type == "system":
        row["role"] = "system"
        row["subtype"] = str(line.get("subtype", ""))
    elif event_type in ("ai-title", "summary"):
        row["session_title"] = str(line.get("aiTitle") or line.get("summary") or "")

    return row


def claude_desktop_event_row(
    line: Mapping[str, Any],
    *,
    session_id: str,
    account: str,
    device: str,
    seq: int,
    ingested_at: datetime,
) -> dict[str, Any]:
    """Normalize one Claude Desktop (claude.ai) conversation line.

    The uploader emits a synthetic ``conversation`` header line per conversation
    followed by one ``message`` line per turn (the raw claude.ai message tagged
    with ``type="message"``). A message's ``content`` is a list of typed blocks
    (``text``/``thinking``/``tool_use``/``tool_result``); the human-or-assistant
    ``sender`` maps to the row role. claude.ai returns the model at the
    conversation level only, so the uploader copies it onto assistant messages.
    """
    event_type = str(line.get("type", ""))

    if event_type == "conversation":
        row = _base_row(
            source="claude_desktop",
            session_id=session_id,
            account=account,
            device=device,
            seq=seq,
            line=line,
            event_uuid=str(line.get("uuid") or f"{session_id}#{seq}"),
            occurred_at=parse_datetime(str(line.get("created_at", ""))),
            ingested_at=ingested_at,
        )
        row["event_type"] = "conversation"
        row["session_title"] = str(line.get("name") or "")
        row["model"] = str(line.get("model") or "")
        return row

    row = _base_row(
        source="claude_desktop",
        session_id=session_id,
        account=account,
        device=device,
        seq=seq,
        line=line,
        event_uuid=str(line.get("uuid") or f"{session_id}#{seq}"),
        occurred_at=parse_datetime(str(line.get("created_at", ""))),
        ingested_at=ingested_at,
    )
    if event_type != "message":
        return row

    row["parent_uuid"] = str(line.get("parent_message_uuid") or "")
    sender = str(line.get("sender", ""))
    content = line.get("content")

    if sender == "human":
        row["role"] = "user"
        row["subtype"] = "message"
        row["text"] = _claude_desktop_text(line)
    elif sender == "assistant":
        row["role"] = "assistant"
        row["model"] = str(line.get("model") or "")
        row["text"] = _claude_desktop_text(line)
        tool_block = _claude_desktop_block(content, "tool_use")
        if tool_block is not None:
            row["subtype"] = "tool_use"
            row["tool_name"] = str(tool_block.get("name", ""))
            row["tool_input_json"] = _as_json_text(tool_block.get("input"))
            row["turn_id"] = str(tool_block.get("id", ""))
        elif row["text"]:
            row["subtype"] = "message"
        else:
            row["subtype"] = "thinking"
    else:
        row["role"] = "meta"
        row["subtype"] = sender

    return row


def _claude_desktop_text(line: Mapping[str, Any]) -> str:
    """Conversational text of a claude.ai message: its ``text`` content blocks,
    falling back to the flattened top-level ``text`` the API also returns, then
    any attachment-extracted text so uploaded docs stay searchable."""
    content = line.get("content")
    text = _join_text_blocks(content, text_types=("text",))
    if not text:
        text = str(line.get("text") or "")
    extracted = _claude_desktop_attachment_text(line.get("attachments"))
    if extracted:
        text = f"{text}\n{extracted}" if text else extracted
    return text


def _claude_desktop_attachment_text(attachments: Any) -> str:
    if not isinstance(attachments, list):
        return ""
    parts: list[str] = []
    for attachment in attachments:
        if not isinstance(attachment, Mapping):
            continue
        extracted = attachment.get("extracted_content")
        if isinstance(extracted, str) and extracted.strip():
            parts.append(extracted)
    return "\n".join(parts)


def _claude_desktop_block(content: Any, block_type: str) -> Mapping[str, Any] | None:
    if not isinstance(content, list):
        return None
    return next(
        (block for block in content if isinstance(block, Mapping) and block.get("type") == block_type),
        None,
    )


def codex_event_row(
    line: Mapping[str, Any],
    *,
    session_id: str,
    account: str,
    device: str,
    seq: int,
    ingested_at: datetime,
) -> dict[str, Any]:
    payload = line.get("payload") if isinstance(line.get("payload"), Mapping) else {}
    row = _base_row(
        source="codex",
        session_id=session_id,
        account=account,
        device=device,
        seq=seq,
        line=line,
        event_uuid=f"{session_id}#{seq}",
        occurred_at=parse_datetime(str(line.get("timestamp", ""))),
        ingested_at=ingested_at,
    )
    line_type = str(line.get("type", ""))

    if line_type == "session_meta":
        git = payload.get("git") if isinstance(payload.get("git"), Mapping) else {}
        row["cwd"] = str(payload.get("cwd", ""))
        row["git_branch"] = str(git.get("branch", ""))
        row["git_commit"] = str(git.get("commit_hash", ""))
        row["repo_url"] = str(git.get("repository_url", ""))
        row["cli_version"] = str(payload.get("cli_version", ""))
        row["entrypoint"] = str(payload.get("originator", ""))
        row["model"] = str(payload.get("model_provider", ""))
    elif line_type == "turn_context":
        row["model"] = str(payload.get("model", ""))
        row["cwd"] = str(payload.get("cwd", ""))
        row["turn_id"] = str(payload.get("turn_id", ""))
    elif line_type == "response_item":
        _apply_codex_response_item(row, payload)
    elif line_type == "event_msg":
        row["subtype"] = str(payload.get("type", ""))
        row["turn_id"] = str(payload.get("turn_id", ""))
        if payload.get("type") == "token_count":
            # Codex reports per-turn usage under info.last_token_usage; summing
            # the "last" values across events yields the session total (summing
            # "total" would double-count the running cumulative).
            info = payload.get("info") if isinstance(payload.get("info"), Mapping) else {}
            _apply_token_usage(row, info.get("last_token_usage"))
        else:
            _apply_token_usage(row, payload.get("usage"))

    return row


def _apply_codex_response_item(row: dict[str, Any], payload: Mapping[str, Any]) -> None:
    item_type = str(payload.get("type", ""))
    row["subtype"] = item_type
    if item_type == "message":
        row["role"] = _codex_role(str(payload.get("role", "")))
        row["subtype"] = "message"
        row["text"] = _join_text_blocks(payload.get("content"))
    elif item_type == "function_call":
        row["role"] = "assistant"
        row["subtype"] = "tool_use"
        row["tool_name"] = str(payload.get("name", ""))
        row["tool_input_json"] = _as_json_text(payload.get("arguments"))
        row["turn_id"] = str(payload.get("call_id", ""))
    elif item_type == "function_call_output":
        row["role"] = "tool"
        row["subtype"] = "tool_result"
        row["text"] = _stringify(payload.get("output"))
        row["tool_result_json"] = _as_json_text(payload.get("output"))
        row["turn_id"] = str(payload.get("call_id", ""))
    elif item_type == "reasoning":
        row["role"] = "assistant"
        row["subtype"] = "thinking"


def _codex_role(role: str) -> str:
    if role in ("user", "assistant"):
        return role
    if role in ("developer", "system", "tool"):
        return "system" if role != "tool" else "tool"
    return "meta"


def openclaw_event_row(
    line: Mapping[str, Any],
    *,
    session_id: str,
    account: str,
    device: str,
    seq: int,
    ingested_at: datetime,
) -> dict[str, Any]:
    """Normalize one line of an OpenClaw "<sessionId>.jsonl" transcript.

    OpenClaw writes one JSON object per line: a ``session`` header, then a
    ``message`` per turn whose nested ``message`` carries ``role``
    (``user``/``assistant``/``toolResult``), string-or-block ``content``, and —
    on assistant turns — ``model``, ``provider`` and a ``usage`` token block.
    """
    event_type = str(line.get("type", ""))
    uuid = str(line.get("id") or f"{session_id}#{seq}")
    row = _base_row(
        source="openclaw",
        session_id=session_id,
        account=account,
        device=device,
        seq=seq,
        line=line,
        event_uuid=uuid,
        occurred_at=parse_datetime(str(line.get("timestamp", ""))),
        ingested_at=ingested_at,
    )
    row["parent_uuid"] = str(line.get("parentId") or "")

    if event_type == "session":
        row["cwd"] = str(line.get("cwd", ""))
        row["cli_version"] = str(line.get("version", ""))
        return row

    if event_type != "message":
        return row

    message = line.get("message") if isinstance(line.get("message"), Mapping) else {}
    role = str(message.get("role", ""))
    content = message.get("content")

    if role == "user":
        row["role"] = "user"
        row["subtype"] = "message"
        row["text"] = _openclaw_text(content)
    elif role == "assistant":
        row["role"] = "assistant"
        row["model"] = str(message.get("model", ""))
        row["entrypoint"] = str(message.get("provider", ""))
        row["text"] = _openclaw_text(content)
        tool_block = _openclaw_block(content, "toolCall")
        if tool_block is not None:
            row["subtype"] = "tool_use"
            row["tool_name"] = str(tool_block.get("name", ""))
            row["tool_input_json"] = _as_json_text(
                tool_block.get("arguments") if tool_block.get("arguments") is not None else tool_block.get("input")
            )
            row["turn_id"] = str(tool_block.get("id", ""))
        elif row["text"]:
            row["subtype"] = "message"
        else:
            row["subtype"] = "thinking"
        _apply_openclaw_usage(row, message.get("usage"))
    elif role == "toolResult":
        row["role"] = "tool"
        row["subtype"] = "tool_result"
        row["tool_name"] = str(message.get("toolName", ""))
        row["turn_id"] = str(message.get("toolCallId", ""))
        row["text"] = _openclaw_tool_result_text(content)
        row["tool_result_json"] = raw_json({"content": content}) if content is not None else ""
    else:
        row["role"] = "meta"
        row["subtype"] = role

    return row


def pi_event_row(
    line: Mapping[str, Any],
    *,
    session_id: str,
    account: str,
    device: str,
    seq: int,
    ingested_at: datetime,
) -> dict[str, Any]:
    """Normalize one pi coding-agent session JSONL line."""
    event_type = str(line.get("type", ""))
    row = _base_row(
        source="pi",
        session_id=session_id,
        account=account,
        device=device,
        seq=seq,
        line=line,
        event_uuid=str(line.get("id") or f"{session_id}#{seq}"),
        occurred_at=parse_datetime(str(line.get("timestamp", ""))),
        ingested_at=ingested_at,
    )
    row["parent_uuid"] = str(line.get("parentId") or "")

    if event_type == "session":
        row["cwd"] = str(line.get("cwd", ""))
        row["cli_version"] = str(line.get("version", ""))
        return row
    if event_type == "session_info":
        row["session_title"] = str(line.get("name", ""))
        return row
    if event_type == "model_change":
        row["entrypoint"] = str(line.get("provider", ""))
        row["model"] = str(line.get("modelId", ""))
        return row
    if event_type != "message":
        return row

    message = line.get("message") if isinstance(line.get("message"), Mapping) else {}
    role = str(message.get("role", ""))
    content = message.get("content")
    if role == "user":
        row["role"] = "user"
        row["subtype"] = "message"
        row["text"] = _openclaw_text(content)
    elif role == "assistant":
        row["role"] = "assistant"
        row["model"] = str(message.get("model", ""))
        row["entrypoint"] = str(message.get("provider", ""))
        row["text"] = _openclaw_text(content)
        tool_block = _openclaw_block(content, "toolCall")
        if tool_block is not None:
            row["subtype"] = "tool_use"
            row["tool_name"] = str(tool_block.get("name", ""))
            row["tool_input_json"] = _as_json_text(tool_block.get("arguments"))
            row["turn_id"] = str(tool_block.get("id", ""))
        elif row["text"]:
            row["subtype"] = "message"
        else:
            row["subtype"] = "thinking"
        _apply_openclaw_usage(row, message.get("usage"))
    elif role == "toolResult":
        row["role"] = "tool"
        row["subtype"] = "tool_result"
        row["tool_name"] = str(message.get("toolName", ""))
        row["turn_id"] = str(message.get("toolCallId", ""))
        row["text"] = _openclaw_tool_result_text(content)
        row["tool_result_json"] = raw_json({"content": content}) if content is not None else ""
    else:
        row["subtype"] = role
    return row


def _openclaw_text(content: Any) -> str:
    return _join_text_blocks(content, text_types=("text",))


def _openclaw_tool_result_text(content: Any) -> str:
    # OpenClaw tool results are blocks of type "toolResult" (camelCase) carrying
    # a "text"/"content" payload, so the generic text-block joiner skips them.
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for block in content:
        if not isinstance(block, Mapping):
            continue
        value = block.get("text")
        if value is None:
            value = block.get("content")
        parts.append(_stringify(value))
    return "\n".join(part for part in parts if part)


def _openclaw_block(content: Any, block_type: str) -> Mapping[str, Any] | None:
    if not isinstance(content, list):
        return None
    return next(
        (block for block in content if isinstance(block, Mapping) and block.get("type") == block_type),
        None,
    )


def _apply_openclaw_usage(row: dict[str, Any], usage: Any) -> None:
    if not isinstance(usage, Mapping):
        return
    row["input_tokens"] = _int(usage.get("input"))
    row["output_tokens"] = _int(usage.get("output"))
    row["cache_read_tokens"] = _int(usage.get("cacheRead"))
    row["cache_creation_tokens"] = _int(usage.get("cacheWrite"))


def _join_text_blocks(content: Any, *, text_types: tuple[str, ...] = ("text", "input_text", "output_text")) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for block in content:
        if not isinstance(block, Mapping):
            continue
        if block.get("type") not in text_types:
            continue
        value = block.get("text")
        if value is None and block.get("type") == "tool_result":
            value = block.get("content")
        parts.append(_stringify(value))
    return "\n".join(part for part in parts if part)


def _apply_token_usage(row: dict[str, Any], usage: Any) -> None:
    if not isinstance(usage, Mapping):
        return
    row["input_tokens"] = _int(usage.get("input_tokens"))
    row["output_tokens"] = _int(usage.get("output_tokens"))
    row["cache_read_tokens"] = _int(usage.get("cache_read_input_tokens") or usage.get("cached_input_tokens"))
    row["cache_creation_tokens"] = _int(usage.get("cache_creation_input_tokens"))


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return _join_text_blocks(value)
    if isinstance(value, Mapping):
        text = value.get("text") or value.get("content")
        return _stringify(text) if text is not None else raw_json(value)
    return str(value)


def _as_json_text(value: Any) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, str):
        return value
    return raw_json(value)


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


_EVENT_ROW_BUILDERS: dict[str, Callable[..., dict[str, Any]]] = {
    "claude_code": claude_code_event_row,
    "codex": codex_event_row,
    "openclaw": openclaw_event_row,
    "pi": pi_event_row,
    "claude_desktop": claude_desktop_event_row,
}


# --- ChatGPT conversation-tree normalization --------------------------------
#
# The ChatGPT backend (``backend-api/conversation/<id>``) and the official data
# export (``conversations.json``) share one shape: a ``mapping`` of node-id ->
# ``{id, message, parent, children}`` forming a tree (branches appear when a
# turn is edited or regenerated). We linearize the tree depth-first from its
# root(s), following each node's ``children`` in order, and emit one source-owned
# AI event row per message-bearing node. ``seq`` is the depth-first position, so
# it is deterministic across idempotent re-pulls and never depends on the
# (sometimes-null) ``create_time``.

_CHATGPT_EPOCH = datetime.fromtimestamp(0, tz=UTC)


def chatgpt_conversation_to_event_rows(
    conversation: Mapping[str, Any],
    *,
    account: str,
    device: str,
    ingested_at: datetime,
) -> list[dict[str, Any]]:
    """Normalize one ChatGPT conversation tree into ``chatgpt.events`` rows."""
    mapping = conversation.get("mapping")
    mapping = mapping if isinstance(mapping, Mapping) else {}
    session_id = str(conversation.get("conversation_id") or conversation.get("id") or "")
    title = str(conversation.get("title") or "")
    gizmo = str(conversation.get("gizmo_id") or "")

    rows: list[dict[str, Any]] = []
    seq = 0
    for node_id, node in _chatgpt_linear_nodes(mapping):
        message = node.get("message") if isinstance(node.get("message"), Mapping) else None
        if not message:
            continue
        rows.append(
            _chatgpt_event_row(
                message,
                node=node,
                node_id=node_id,
                session_id=session_id,
                account=account,
                device=device,
                seq=seq,
                ingested_at=ingested_at,
                title=title,
                gizmo=gizmo,
            )
        )
        seq += 1
    return rows


def _chatgpt_linear_nodes(mapping: Mapping[str, Any]) -> list[tuple[str, Mapping[str, Any]]]:
    """Return ``(node_id, node)`` pairs in depth-first, children-order traversal."""
    nodes = {nid: node for nid, node in mapping.items() if isinstance(node, Mapping)}
    roots = [
        nid
        for nid, node in nodes.items()
        if not node.get("parent") or node.get("parent") not in nodes
    ]
    roots.sort(key=lambda nid: _chatgpt_root_sort_key(nodes[nid], nid))

    ordered: list[tuple[str, Mapping[str, Any]]] = []
    visited: set[str] = set()
    stack = list(reversed(roots))
    while stack:
        nid = stack.pop()
        if nid in visited or nid not in nodes:
            continue
        visited.add(nid)
        node = nodes[nid]
        ordered.append((nid, node))
        children = node.get("children")
        children = children if isinstance(children, list) else []
        for child in reversed([c for c in children if isinstance(c, str)]):
            if child not in visited:
                stack.append(child)
    # Any nodes unreachable from a root (malformed trees) still get emitted, in a
    # stable order, so nothing is silently dropped.
    for nid, node in nodes.items():
        if nid not in visited:
            visited.add(nid)
            ordered.append((nid, node))
    return ordered


def _chatgpt_root_sort_key(node: Mapping[str, Any], node_id: str) -> tuple[float, str]:
    message = node.get("message") if isinstance(node.get("message"), Mapping) else {}
    create_time = message.get("create_time")
    ts = float(create_time) if isinstance(create_time, (int, float)) else 0.0
    return (ts, node_id)


def _chatgpt_event_row(
    message: Mapping[str, Any],
    *,
    node: Mapping[str, Any],
    node_id: str,
    session_id: str,
    account: str,
    device: str,
    seq: int,
    ingested_at: datetime,
    title: str,
    gizmo: str,
) -> dict[str, Any]:
    author = message.get("author") if isinstance(message.get("author"), Mapping) else {}
    role_raw = str(author.get("role") or "")
    content = message.get("content") if isinstance(message.get("content"), Mapping) else {}
    content_type = str(content.get("content_type") or "")
    metadata = message.get("metadata") if isinstance(message.get("metadata"), Mapping) else {}
    recipient = str(message.get("recipient") or "all")
    event_uuid = str(message.get("id") or node_id or f"{session_id}#{seq}")

    row = _base_row(
        source="chatgpt",
        session_id=session_id,
        account=account,
        device=device,
        seq=seq,
        line=node,
        event_uuid=event_uuid,
        occurred_at=_chatgpt_time(message.get("create_time")),
        ingested_at=ingested_at,
    )
    row["event_type"] = role_raw or content_type
    row["parent_uuid"] = str(node.get("parent") or "")
    row["session_title"] = title
    row["entrypoint"] = gizmo
    row["model"] = str(metadata.get("model_slug") or metadata.get("default_model_slug") or "")

    text = _chatgpt_content_text(content)
    role = _chatgpt_role(role_raw)
    # ChatGPT injects the user-profile / custom-instructions block as a
    # visually-hidden ``user`` message (content_type ``user_editable_context``).
    # It is context, not a turn, so demote it to ``system``; otherwise it
    # becomes the session's ``first_prompt`` and inflates the user-turn count.
    # Hidden ``assistant`` messages (tool calls, model context) stay assistant:
    # they are real model actions.
    if role == "user" and bool(metadata.get("is_visually_hidden_from_conversation")):
        role = "system"
    row["role"] = role
    if role == "tool":
        row["subtype"] = "tool_result"
        row["tool_name"] = str(author.get("name") or "")
        row["text"] = text
        row["tool_result_json"] = raw_json(content) if content else ""
    elif role == "assistant" and recipient != "all":
        # Assistant turn addressed to a tool (e.g. ``python``/``browser``) is the
        # tool invocation; its content is the call payload.
        row["subtype"] = "tool_use"
        row["tool_name"] = recipient
        row["tool_input_json"] = _as_json_text(text or content)
        row["text"] = text
    elif role == "assistant" and content_type in ("thoughts", "reasoning_recap"):
        row["subtype"] = "thinking"
        row["text"] = text
    elif role == "assistant":
        row["subtype"] = "message"
        row["text"] = text
    elif role == "user":
        row["subtype"] = "message"
        row["text"] = text
    else:
        row["subtype"] = content_type or role_raw
        row["text"] = text
    return row


def _chatgpt_role(role_raw: str) -> str:
    if role_raw in ("user", "assistant", "system", "tool"):
        return role_raw
    return "meta"


def _chatgpt_time(value: Any) -> datetime:
    if value is None or value == "":
        return _CHATGPT_EPOCH
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=UTC)
        except (OverflowError, OSError, ValueError):
            return _CHATGPT_EPOCH
    return parse_datetime(str(value))


def _chatgpt_content_text(content: Any) -> str:
    if not isinstance(content, Mapping):
        return ""
    content_type = str(content.get("content_type") or "")
    parts = content.get("parts")
    if isinstance(parts, list):
        text = "\n".join(part for part in parts if isinstance(part, str) and part)
        if text:
            return text
    if content_type == "thoughts":
        thoughts = content.get("thoughts")
        if isinstance(thoughts, list):
            chunks = []
            for thought in thoughts:
                if not isinstance(thought, Mapping):
                    continue
                summary = str(thought.get("summary") or "")
                body = str(thought.get("content") or "")
                chunk = "\n".join(piece for piece in (summary, body) if piece)
                if chunk:
                    chunks.append(chunk)
            return "\n\n".join(chunks)
    if content_type == "user_editable_context":
        profile = str(content.get("user_profile") or "")
        instructions = str(content.get("user_instructions") or "")
        return "\n".join(piece for piece in (profile, instructions) if piece)
    for key in ("text", "result", "content"):
        value = content.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


# --- object key helpers -----------------------------------------------------


def batch_storage_key(*, listing: ObjectListing, stage: str) -> str:
    name = listing.filename
    exported = str(listing.app_properties.get("exported_at", ""))
    exported_at = parse_datetime(exported) if exported else datetime.fromtimestamp(0, tz=UTC)
    if exported_at.year > 1970:
        return f"{OBJECT_PREFIX}/{stage}/batches/{exported_at.year:04d}/{exported_at.month:02d}/{name}"
    return f"{OBJECT_PREFIX}/{stage}/batches/{name}"


def stored_file_context(
    *,
    storage_key: str,
    listing: ObjectListing,
    extra: Mapping[str, str] | None = None,
) -> dict[str, str]:
    context = dict(listing.ref)
    context["storage_key"] = storage_key
    context.update(extra or {})
    return context


def library_storage_key(storage_key: str) -> str:
    if storage_key.startswith(INBOX_PREFIX):
        return f"{LIBRARY_PREFIX}{storage_key[len(INBOX_PREFIX):]}"
    return storage_key


def nested_mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    nested = value.get(key)
    return nested if isinstance(nested, Mapping) else {}


def raw_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def sync_version(ingested_at: datetime) -> int:
    return int(ingested_at.astimezone(UTC).timestamp() * 1_000_000)
