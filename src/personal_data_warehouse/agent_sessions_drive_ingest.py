"""Ingest AI agent CLI session logs (Claude Code, Codex) from the Drive inbox.

A per-device uploader tails the append-only JSONL transcripts each tool writes
(``~/.claude/projects/**/*.jsonl`` and ``~/.codex/sessions/**/rollout-*.jsonl``)
and batches new lines into ``agent-sessions/inbox/batches/`` as gzipped JSONL
envelopes. This module consumes those batches, normalizes each raw line into an
``agent_session_events`` row (lossless ``raw_json`` plus queryable columns), and
promotes processed batch files into ``agent-sessions/library/``.

The session-level roll-up (counts, token sums, header) is the
``clean_agent_sessions`` view over these events, so it stays correct no matter
how a session's lines are split across batches over time.
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

# Roles that carry conversational content worth surfacing in searchable_text.
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
    builder = codex_event_row if tool == "codex" else claude_code_event_row
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
