from __future__ import annotations

from datetime import UTC, datetime
import gzip
import json

from personal_data_warehouse.agent_sessions_drive_ingest import (
    AgentSessionsDriveIngestRunner,
    claude_code_event_row,
    codex_event_row,
    has_batch_payloads,
    iter_batch_payloads,
    record_to_event_row,
)
from personal_data_warehouse.objectstore import ObjectListing


class FakeLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


class FakeWarehouse:
    def __init__(self) -> None:
        self.ensure_called = False
        self.events: list[dict[str, object]] = []

    def ensure_agent_sessions_tables(self) -> None:
        self.ensure_called = True

    def insert_agent_session_events(self, rows) -> None:
        self.events.extend(rows)


class FakeObjectStore:
    backend = "google_drive"

    def __init__(self, *, batch_listings=None, gz_by_id=None) -> None:
        self.batch_listings = list(batch_listings or [])
        self.gz_by_id = dict(gz_by_id or {})
        self.moved: list[tuple[str, str]] = []

    def list_objects(self, *, kind, stage=None, properties=None):
        return list(self.batch_listings) if kind == "agent_sessions_export_batch" else []

    def find_object(self, *, kind, stage=None, properties=None):
        if kind == "agent_sessions_export_batch" and self.batch_listings:
            return self.batch_listings[0]
        return None

    def get_object(self, ref):
        return self.gz_by_id[str(ref["storage_file_id"])]

    def move_object(self, ref, *, new_object_key, app_properties=None):
        self.moved.append((str(ref.get("storage_file_id", "")), new_object_key))
        return {
            "storage_backend": self.backend,
            "storage_key": new_object_key,
            "storage_file_id": str(ref.get("storage_file_id", "")),
            "storage_url": "https://drive/promoted",
        }


INGESTED_AT = datetime(2026, 6, 14, 18, tzinfo=UTC)
CLAUDE_SESSION = "bd51eaa0-f030-4b13-96d9-37c8c1bda53d"
CODEX_SESSION = "019ec722-5a03-7980-bd45-f34ba5bd9502"


def envelope(record: dict, *, tool: str, record_type: str | None = None) -> dict:
    return {
        "schema_version": 1,
        "source": "agent_sessions",
        "account": "zach@example.com",
        "device": "porygon",
        "exported_at": "2026-06-14T18:00:00+00:00",
        "record_type": record_type or f"{tool}_event",
        "record": record,
    }


def claude_record(line: dict, *, seq: int) -> dict:
    return {"tool": "claude_code", "session_id": CLAUDE_SESSION, "seq": seq, "line": line}


def codex_record(line: dict, *, seq: int) -> dict:
    return {"tool": "codex", "session_id": CODEX_SESSION, "seq": seq, "line": line}


# --- Claude Code normalizer -------------------------------------------------


def test_claude_user_prompt_row() -> None:
    line = {
        "parentUuid": None,
        "isSidechain": False,
        "type": "user",
        "message": {"role": "user", "content": "Run the playbook"},
        "uuid": "evt-1",
        "timestamp": "2026-06-12T22:01:10.819Z",
        "userType": "external",
        "entrypoint": "cli",
        "cwd": "/Users/zrl/work/confusion-partridge",
        "sessionId": CLAUDE_SESSION,
        "version": "2.1.176",
        "gitBranch": "confusion-partridge",
    }
    row = claude_code_event_row(
        line, session_id=CLAUDE_SESSION, account="zach@example.com", device="porygon", seq=3, ingested_at=INGESTED_AT
    )
    assert row["source"] == "claude_code"
    assert row["session_id"] == CLAUDE_SESSION
    assert row["event_uuid"] == "evt-1"
    assert row["seq"] == 3
    assert row["role"] == "user"
    assert row["event_type"] == "user"
    assert row["text"] == "Run the playbook"
    assert row["cwd"] == "/Users/zrl/work/confusion-partridge"
    assert row["git_branch"] == "confusion-partridge"
    assert row["cli_version"] == "2.1.176"
    assert row["entrypoint"] == "cli"
    assert row["occurred_at"] == datetime(2026, 6, 12, 22, 1, 10, 819000, tzinfo=UTC)
    assert json.loads(row["raw_json"])["uuid"] == "evt-1"


def test_claude_assistant_row_extracts_text_tool_and_tokens() -> None:
    line = {
        "type": "assistant",
        "parentUuid": "evt-1",
        "isSidechain": False,
        "message": {
            "model": "claude-fable-5",
            "role": "assistant",
            "content": [
                {"type": "thinking", "thinking": "secret reasoning"},
                {"type": "text", "text": "I'll run it now."},
                {"type": "tool_use", "id": "tu-1", "name": "Bash", "input": {"command": "ls"}},
            ],
            "usage": {
                "input_tokens": 2066,
                "output_tokens": 261,
                "cache_read_input_tokens": 16307,
                "cache_creation_input_tokens": 4960,
            },
        },
        "uuid": "evt-2",
        "timestamp": "2026-06-12T22:01:16.258Z",
        "cwd": "/Users/zrl/work/confusion-partridge",
        "version": "2.1.176",
        "gitBranch": "confusion-partridge",
    }
    row = claude_code_event_row(
        line, session_id=CLAUDE_SESSION, account="a", device="d", seq=4, ingested_at=INGESTED_AT
    )
    assert row["role"] == "assistant"
    assert row["model"] == "claude-fable-5"
    assert row["text"] == "I'll run it now."  # thinking excluded from searchable text
    assert "secret reasoning" not in row["text"]
    assert row["subtype"] == "tool_use"
    assert row["tool_name"] == "Bash"
    assert json.loads(row["tool_input_json"]) == {"command": "ls"}
    assert row["input_tokens"] == 2066
    assert row["output_tokens"] == 261
    assert row["cache_read_tokens"] == 16307
    assert row["cache_creation_tokens"] == 4960
    # thinking is still preserved losslessly
    assert "secret reasoning" in row["raw_json"]


def test_claude_user_tool_result_row() -> None:
    line = {
        "type": "user",
        "isSidechain": False,
        "message": {
            "role": "user",
            "content": [
                {"type": "tool_result", "tool_use_id": "tu-1", "content": "file1.txt\nfile2.txt"}
            ],
        },
        "uuid": "evt-3",
        "timestamp": "2026-06-12T22:01:17.000Z",
        "cwd": "/x",
        "sessionId": CLAUDE_SESSION,
    }
    row = claude_code_event_row(
        line, session_id=CLAUDE_SESSION, account="a", device="d", seq=5, ingested_at=INGESTED_AT
    )
    assert row["role"] == "tool"
    assert row["subtype"] == "tool_result"
    assert "file1.txt" in row["text"]


def test_claude_meta_line_without_uuid_gets_synthetic_id_and_title() -> None:
    line = {"type": "ai-title", "aiTitle": "Run the DAU playbook", "sessionId": CLAUDE_SESSION}
    row = claude_code_event_row(
        line, session_id=CLAUDE_SESSION, account="a", device="d", seq=7, ingested_at=INGESTED_AT
    )
    assert row["role"] == "meta"
    assert row["event_type"] == "ai-title"
    assert row["event_uuid"] == f"{CLAUDE_SESSION}#7"
    assert row["session_title"] == "Run the DAU playbook"


def test_claude_sidechain_flag() -> None:
    line = {
        "type": "user",
        "isSidechain": True,
        "message": {"role": "user", "content": "subagent task"},
        "uuid": "evt-9",
        "timestamp": "2026-06-12T22:01:10.000Z",
    }
    row = claude_code_event_row(
        line, session_id=CLAUDE_SESSION, account="a", device="d", seq=9, ingested_at=INGESTED_AT
    )
    assert row["is_sidechain"] == 1


# --- Codex normalizer -------------------------------------------------------


def test_codex_session_meta_row_extracts_context() -> None:
    line = {
        "type": "session_meta",
        "timestamp": "2026-06-14T17:16:46.875Z",
        "payload": {
            "id": CODEX_SESSION,
            "cwd": "/Users/zrl/work/valiant-faucet",
            "originator": "codex-tui",
            "cli_version": "0.139.0",
            "model_provider": "openai",
            "git": {
                "commit_hash": "dcb11de1f117353935baf14fa288713914f4d365",
                "branch": "valiant-faucet",
                "repository_url": "git@github.com:zachlatta/personal-data-warehouse.git",
            },
        },
    }
    row = codex_event_row(
        line, session_id=CODEX_SESSION, account="a", device="d", seq=0, ingested_at=INGESTED_AT
    )
    assert row["source"] == "codex"
    assert row["event_type"] == "session_meta"
    assert row["role"] == "meta"
    assert row["cwd"] == "/Users/zrl/work/valiant-faucet"
    assert row["git_branch"] == "valiant-faucet"
    assert row["git_commit"] == "dcb11de1f117353935baf14fa288713914f4d365"
    assert row["repo_url"] == "git@github.com:zachlatta/personal-data-warehouse.git"
    assert row["cli_version"] == "0.139.0"
    assert row["entrypoint"] == "codex-tui"
    assert row["event_uuid"] == f"{CODEX_SESSION}#0"


def test_codex_response_item_message_row() -> None:
    line = {
        "type": "response_item",
        "timestamp": "2026-06-14T17:16:50.000Z",
        "payload": {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "fix the bug"}],
        },
    }
    row = codex_event_row(
        line, session_id=CODEX_SESSION, account="a", device="d", seq=2, ingested_at=INGESTED_AT
    )
    assert row["role"] == "user"
    assert row["text"] == "fix the bug"


def test_codex_function_call_and_output_rows() -> None:
    call = {
        "type": "response_item",
        "timestamp": "2026-06-14T17:16:51.000Z",
        "payload": {
            "type": "function_call",
            "name": "shell",
            "arguments": "{\"command\":[\"ls\"]}",
            "call_id": "call-1",
        },
    }
    output = {
        "type": "response_item",
        "timestamp": "2026-06-14T17:16:52.000Z",
        "payload": {
            "type": "function_call_output",
            "call_id": "call-1",
            "output": "file1.txt",
        },
    }
    call_row = codex_event_row(call, session_id=CODEX_SESSION, account="a", device="d", seq=3, ingested_at=INGESTED_AT)
    out_row = codex_event_row(output, session_id=CODEX_SESSION, account="a", device="d", seq=4, ingested_at=INGESTED_AT)
    assert call_row["role"] == "assistant"
    assert call_row["subtype"] == "tool_use"
    assert call_row["tool_name"] == "shell"
    assert out_row["role"] == "tool"
    assert out_row["subtype"] == "tool_result"
    assert "file1.txt" in out_row["text"]


def test_codex_token_count_event_uses_last_turn_usage() -> None:
    line = {
        "type": "event_msg",
        "timestamp": "2026-06-14T17:46:47.418Z",
        "payload": {
            "type": "token_count",
            "info": {
                "last_token_usage": {
                    "input_tokens": 14565,
                    "cached_input_tokens": 4480,
                    "output_tokens": 653,
                    "total_tokens": 15218,
                },
                "total_token_usage": {"input_tokens": 999999},
            },
        },
    }
    row = codex_event_row(line, session_id=CODEX_SESSION, account="a", device="d", seq=8, ingested_at=INGESTED_AT)
    assert row["input_tokens"] == 14565
    assert row["output_tokens"] == 653
    assert row["cache_read_tokens"] == 4480
    assert row["subtype"] == "token_count"


def test_codex_turn_context_model() -> None:
    line = {
        "type": "turn_context",
        "timestamp": "2026-06-14T17:16:46.887Z",
        "payload": {"model": "gpt-5.5", "cwd": "/x", "turn_id": "turn-1"},
    }
    row = codex_event_row(line, session_id=CODEX_SESSION, account="a", device="d", seq=1, ingested_at=INGESTED_AT)
    assert row["model"] == "gpt-5.5"
    assert row["turn_id"] == "turn-1"


# --- dispatch + batch plumbing ----------------------------------------------


def test_record_to_event_row_dispatches_on_tool() -> None:
    claude = envelope(
        claude_record({"type": "user", "message": {"role": "user", "content": "hi"}, "uuid": "u1"}, seq=0),
        tool="claude_code",
    )
    codex = envelope(
        codex_record({"type": "session_meta", "payload": {"id": CODEX_SESSION}}, seq=0),
        tool="codex",
    )
    assert record_to_event_row(claude, ingested_at=INGESTED_AT)["source"] == "claude_code"
    assert record_to_event_row(codex, ingested_at=INGESTED_AT)["source"] == "codex"


def test_iter_batch_payloads_decompresses() -> None:
    records = [
        envelope(claude_record({"type": "user", "message": {"role": "user", "content": "hi"}, "uuid": "u1"}, seq=0), tool="claude_code")
    ]
    gz = gzip.compress("\n".join(json.dumps(r) for r in records).encode("utf-8"))
    listing = ObjectListing(
        ref={"storage_backend": "google_drive", "storage_key": "", "storage_file_id": "batch-1", "storage_url": ""},
        app_properties={"content_sha256": "sha", "exported_at": "2026-06-14T18:00:00+00:00"},
        filename="batch.jsonl.gz",
    )
    store = FakeObjectStore(batch_listings=[listing], gz_by_id={"batch-1": gz})
    payloads = list(iter_batch_payloads(object_store=store))
    assert len(payloads) == 1
    assert payloads[0]["records"][0]["record"]["tool"] == "claude_code"
    assert payloads[0]["batch_file"]["storage_file_id"] == "batch-1"


def test_has_batch_payloads() -> None:
    listing = ObjectListing(
        ref={"storage_backend": "google_drive", "storage_key": "", "storage_file_id": "b", "storage_url": ""},
        app_properties={},
        filename="b.jsonl.gz",
    )
    assert has_batch_payloads(object_store=FakeObjectStore(batch_listings=[listing])) is True
    assert has_batch_payloads(object_store=FakeObjectStore()) is False


def batch_payload() -> dict:
    return {
        "schema_version": 1,
        "source": "agent_sessions",
        "batch_file": {
            "storage_backend": "google_drive",
            "storage_key": "agent-sessions/inbox/batches/2026/06/batch.jsonl.gz",
            "storage_file_id": "batch-1",
            "storage_url": "https://drive/batch",
            "content_sha256": "sha",
        },
        "records": [
            envelope(claude_record({"type": "user", "message": {"role": "user", "content": "first"}, "uuid": "u1", "timestamp": "2026-06-14T17:00:00.000Z"}, seq=0), tool="claude_code"),
            envelope(claude_record({"type": "assistant", "message": {"role": "assistant", "model": "claude-fable-5", "content": [{"type": "text", "text": "done"}], "usage": {"input_tokens": 10, "output_tokens": 5}}, "uuid": "u2", "timestamp": "2026-06-14T17:00:01.000Z"}, seq=1), tool="claude_code"),
        ],
    }


def test_runner_ingests_and_promotes() -> None:
    warehouse = FakeWarehouse()
    store = FakeObjectStore()
    summary = AgentSessionsDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: [batch_payload()],
        object_store=store,
        logger=FakeLogger(),
        now=lambda: INGESTED_AT,
    ).sync()
    assert warehouse.ensure_called
    assert summary.batches_seen == 1
    assert summary.events_written == 2
    assert summary.files_promoted == 1
    assert {e["event_uuid"] for e in warehouse.events} == {"u1", "u2"}
    assert ("batch-1", "agent-sessions/library/batches/2026/06/batch.jsonl.gz") in store.moved


def test_runner_dedupes_repeated_lines_idempotently() -> None:
    warehouse = FakeWarehouse()
    summary = AgentSessionsDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: [batch_payload(), batch_payload()],
        logger=FakeLogger(),
        now=lambda: INGESTED_AT,
    ).sync()
    # Same two lines across two batches collapse to two rows.
    assert summary.events_written == 2
    assert len(warehouse.events) == 2
