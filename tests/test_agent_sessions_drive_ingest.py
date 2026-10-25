from __future__ import annotations

from datetime import UTC, datetime
import gzip
import json

from personal_data_warehouse.agent_sessions_drive_ingest import (
    AgentSessionsDriveIngestRunner,
    argon_event_row,
    claude_code_event_row,
    codex_event_row,
    has_batch_payloads,
    iter_batch_payloads,
    muse_code_event_row,
    openclaw_event_row,
    pi_event_row,
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

    def legacy_codex_tool_rows(self, *, limit: int) -> list[dict[str, object]]:
        return []


def test_pi_event_row_normalizes_session_messages_and_tools() -> None:
    session = pi_event_row(
        {"type": "session", "id": "session-1", "timestamp": "2026-07-09T19:45:23Z", "cwd": "/work", "version": "1.2.3"},
        session_id="session-1", account="a", device="d", seq=0, ingested_at=datetime(2026, 7, 9, 20, tzinfo=UTC),
    )
    user = pi_event_row(
        {"type": "message", "id": "user-1", "parentId": "session-1", "timestamp": "2026-07-09T19:46:02Z", "message": {"role": "user", "content": [{"type": "text", "text": "ship it"}]}},
        session_id="session-1", account="a", device="d", seq=1, ingested_at=datetime(2026, 7, 9, 20, tzinfo=UTC),
    )
    assistant = pi_event_row(
        {"type": "message", "id": "assistant-1", "timestamp": "2026-07-09T19:46:09Z", "message": {"role": "assistant", "provider": "provider", "model": "model", "content": [{"type": "toolCall", "id": "call-1", "name": "read", "arguments": {"path": "README.md"}}], "usage": {"input": 10, "output": 4, "cacheRead": 3, "cacheWrite": 2}}},
        session_id="session-1", account="a", device="d", seq=2, ingested_at=datetime(2026, 7, 9, 20, tzinfo=UTC),
    )

    assert (session["source"], session["cwd"], session["cli_version"]) == ("pi", "/work", "1.2.3")
    assert (user["role"], user["text"], user["parent_uuid"]) == ("user", "ship it", "session-1")
    assert (assistant["role"], assistant["subtype"], assistant["tool_name"]) == ("assistant", "tool_use", "read")
    assert (assistant["model"], assistant["entrypoint"]) == ("model", "provider")
    assert (assistant["input_tokens"], assistant["output_tokens"], assistant["cache_read_tokens"], assistant["cache_creation_tokens"]) == (10, 4, 3, 2)


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
OPENCLAW_SESSION = "ebf9f4b8-4f8e-442c-84a7-d5015f64fdb7"


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


def openclaw_record(line: dict, *, seq: int) -> dict:
    return {"tool": "openclaw", "session_id": OPENCLAW_SESSION, "seq": seq, "line": line}


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
    # The call carries the same id its tool_result row does, so a consumer can
    # pair the two. Only the result row had it until 2026-10-25, which left
    # agent_usage's id join matching nothing for Claude Code and every pdw call
    # falling back to adjacency.
    assert row["turn_id"] == "tu-1"
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
    # The tool_use id the result answers: parallel tool calls make the next
    # row the wrong pairing, so consumers pair by id (agent_usage does).
    assert row["turn_id"] == "tu-1"


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


# Codex runs nearly every tool through one *custom* tool since mid-2026: an
# `exec` call whose input is a small JS program that can make several inner
# tool calls, answered by a custom_tool_call_output row with the same call_id.
# Until 2026-10-02 both rows landed as role 'meta' with no tool_name, input or
# result, so every "pair tool calls" query silently missed almost all Codex work.
_CODEX_EXEC_SCRIPT = (
    'text(await tools.exec_command({cmd:"sk read start-here",max_output_tokens:12000}));\n'
    "text(await tools.exec_command({cmd:'pdw call skills__skill_read --data \\'{\"name\":\"x\"}\\'', yield_time_ms:1000}));\n"
    "const r = await tools.mcp__skills__skill_search({query: 'browser'});\n"
    "text(await tools.exec_command({\"cmd\": \"git status --short\\n\"}));\n"
    "text(await tools.exec_command({cmd:`gh pr view ${n} --json title`}));\n"
    "text(ALL_TOOLS.filter(x=>/search/i.test(x.name)));\n"
)


def _codex_custom_call(name: str, tool_input: str, *, call_id: str = "call_exec1") -> dict:
    return {
        "type": "response_item",
        "timestamp": "2026-10-01T09:18:10.233Z",
        "ordinal": 10.0,
        "payload": {
            "type": "custom_tool_call",
            "name": name,
            "call_id": call_id,
            "id": "ctc_1",
            "input": tool_input,
            "status": "completed",
        },
    }


def _codex_custom_output(output, *, call_id: str = "call_exec1") -> dict:
    return {
        "type": "response_item",
        "timestamp": "2026-10-01T09:18:14.740Z",
        "payload": {"type": "custom_tool_call_output", "call_id": call_id, "id": "ctco_1", "output": output},
    }


def test_codex_custom_exec_call_names_the_tool_and_lists_its_inner_calls() -> None:
    row = codex_event_row(
        _codex_custom_call("exec", _CODEX_EXEC_SCRIPT),
        session_id=CODEX_SESSION, account="a", device="d", seq=10, ingested_at=INGESTED_AT,
    )
    assert row["role"] == "assistant"
    assert row["subtype"] == "tool_use"
    assert row["tool_name"] == "exec"
    # The call id is the pairing key, exactly as for a function_call.
    assert row["turn_id"] == "call_exec1"
    # Tool rows carry no turn text: the timeline indexes user/assistant TEXT,
    # and a script is not something the agent said.
    assert row["text"] == ""
    tool_input = json.loads(row["tool_input_json"])
    assert tool_input["input"] == _CODEX_EXEC_SCRIPT
    # Distinct inner tools in first-call order.
    assert tool_input["tools"] == ["exec_command", "mcp__skills__skill_search"]
    assert tool_input["commands"] == [
        "sk read start-here",
        "pdw call skills__skill_read --data '{\"name\":\"x\"}'",
        "git status --short\n",
        "gh pr view ${n} --json title",
    ]


def test_codex_custom_apply_patch_call_lists_the_files_it_touches() -> None:
    patch = (
        "*** Begin Patch\n*** Update File: src/a.py\n@@\n-x\n+y\n"
        "*** Add File: docs/b.md\n+hello\n*** Delete File: old.txt\n*** End Patch\n"
    )
    row = codex_event_row(
        _codex_custom_call("apply_patch", patch, call_id="call_patch"),
        session_id=CODEX_SESSION, account="a", device="d", seq=11, ingested_at=INGESTED_AT,
    )
    assert row["tool_name"] == "apply_patch"
    tool_input = json.loads(row["tool_input_json"])
    assert tool_input == {"input": patch, "files": ["src/a.py", "docs/b.md", "old.txt"]}


def test_codex_custom_tool_output_is_a_tool_result_with_its_exit_codes() -> None:
    chunk = json.dumps({"chunk_id": "ef0e76", "exit_code": 2, "original_token_count": 10, "output": "boom"})
    output = [
        {"type": "input_text", "text": "Script completed\nWall time 0.3 seconds\nOutput:\n"},
        {"type": "input_text", "text": "Warning: truncated output (original token count: 38406)\nTotal output lines: 3\n\n" + chunk + "\n"},
        {"type": "input_text", "text": json.dumps({"chunk_id": "a1", "exit_code": 0, "output": "ok"})},
    ]
    row = codex_event_row(
        _codex_custom_output(output),
        session_id=CODEX_SESSION, account="a", device="d", seq=14, ingested_at=INGESTED_AT,
    )
    assert row["role"] == "tool"
    assert row["subtype"] == "tool_result"
    assert row["tool_name"] == ""
    assert row["turn_id"] == "call_exec1"
    assert "Script completed" in row["text"] and "boom" in row["text"]
    result = json.loads(row["tool_result_json"])
    assert result["output"] == output
    assert result["exit_codes"] == [2, 0]
    assert result["truncated"] is True


def test_codex_custom_tool_output_as_a_plain_string() -> None:
    row = codex_event_row(
        _codex_custom_output("Script running with cell ID 94\nWall time 31.0 seconds\nOutput:\n"),
        session_id=CODEX_SESSION, account="a", device="d", seq=15, ingested_at=INGESTED_AT,
    )
    assert row["subtype"] == "tool_result"
    assert row["text"].startswith("Script running with cell ID 94")
    assert json.loads(row["tool_result_json"]) == {
        "output": "Script running with cell ID 94\nWall time 31.0 seconds\nOutput:\n",
        "exit_codes": [],
        "truncated": False,
    }


def test_codex_hosted_tool_calls_are_named_tool_rows() -> None:
    def row_for(payload: dict, seq: int) -> dict:
        line = {"type": "response_item", "timestamp": "2026-04-29T22:00:15.161Z", "payload": payload}
        return codex_event_row(line, session_id=CODEX_SESSION, account="a", device="d", seq=seq, ingested_at=INGESTED_AT)

    web = row_for(
        {"type": "web_search_call", "status": "completed", "action": {"type": "search", "query": "pgvector hnsw"}}, 1
    )
    assert (web["role"], web["subtype"], web["tool_name"]) == ("assistant", "tool_use", "web_search")
    assert json.loads(web["tool_input_json"]) == {"type": "search", "query": "pgvector hnsw"}

    search = row_for(
        {"type": "tool_search_call", "call_id": "call_ts", "arguments": {"limit": 8, "query": "calendar"}}, 2
    )
    assert (search["role"], search["subtype"], search["tool_name"]) == ("assistant", "tool_use", "tool_search")
    assert search["turn_id"] == "call_ts"
    assert json.loads(search["tool_input_json"]) == {"limit": 8, "query": "calendar"}

    found = row_for(
        {"type": "tool_search_output", "call_id": "call_ts", "tools": [{"name": "mcp__codex_apps__google_calendar"}]}, 3
    )
    assert (found["role"], found["subtype"], found["turn_id"]) == ("tool", "tool_result", "call_ts")
    assert json.loads(found["tool_result_json"]) == {"tools": [{"name": "mcp__codex_apps__google_calendar"}]}

    image = row_for(
        {"type": "image_generation_call", "status": "generating", "revised_prompt": "a fox", "result": "iVBORw0KGgo="}, 4
    )
    assert (image["role"], image["subtype"], image["tool_name"]) == ("assistant", "tool_use", "image_generation")
    assert json.loads(image["tool_input_json"]) == {"revised_prompt": "a fox"}
    # The base64 image stays in raw_json only.
    assert "iVBORw0KGgo" not in image["tool_result_json"]


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


# --- OpenClaw normalizer ----------------------------------------------------


def test_openclaw_session_header_row() -> None:
    line = {
        "type": "session",
        "version": 3,
        "id": "466d43a9-22cf-40dd-b31b-a2e0e44d63e8",
        "timestamp": "2026-06-20T04:25:43.832Z",
        "cwd": "/home/zrl",
    }
    row = openclaw_event_row(line, session_id=OPENCLAW_SESSION, account="a", device="openclaw", seq=0, ingested_at=INGESTED_AT)
    assert row["source"] == "openclaw"
    assert row["event_type"] == "session"
    assert row["cwd"] == "/home/zrl"
    assert row["cli_version"] == "3"


def test_openclaw_user_message_row() -> None:
    line = {
        "type": "message",
        "id": "a1b7592a-ce52-4b4d-a72c-fff42c17ff4c",
        "parentId": None,
        "timestamp": "2026-06-20T04:25:43.832Z",
        "message": {"role": "user", "content": "run the hook"},
    }
    row = openclaw_event_row(line, session_id=OPENCLAW_SESSION, account="a", device="openclaw", seq=1, ingested_at=INGESTED_AT)
    assert row["role"] == "user"
    assert row["subtype"] == "message"
    assert row["text"] == "run the hook"
    assert row["event_uuid"] == "a1b7592a-ce52-4b4d-a72c-fff42c17ff4c"


def test_openclaw_assistant_toolcall_and_tokens_row() -> None:
    line = {
        "type": "message",
        "id": "b3a13e24-0578-443a-a076-3503a2e8b649",
        "parentId": "a1b7592a-ce52-4b4d-a72c-fff42c17ff4c",
        "timestamp": "2026-06-20T04:26:02.135Z",
        "message": {
            "role": "assistant",
            "content": [
                {"type": "text", "text": "Running it now."},
                {"type": "toolCall", "id": "call_1Rvx", "name": "bash", "arguments": {"command": "ls"}, "input": {"command": "ls"}},
            ],
            "provider": "codex",
            "model": "gpt-5.5",
            "usage": {"input": 120, "output": 30, "cacheRead": 10, "cacheWrite": 5, "totalTokens": 165},
        },
    }
    row = openclaw_event_row(line, session_id=OPENCLAW_SESSION, account="a", device="openclaw", seq=2, ingested_at=INGESTED_AT)
    assert row["role"] == "assistant"
    assert row["model"] == "gpt-5.5"
    assert row["entrypoint"] == "codex"
    assert row["subtype"] == "tool_use"
    assert row["tool_name"] == "bash"
    assert json.loads(row["tool_input_json"]) == {"command": "ls"}
    assert row["turn_id"] == "call_1Rvx"
    assert row["text"] == "Running it now."
    assert row["input_tokens"] == 120
    assert row["output_tokens"] == 30
    assert row["cache_read_tokens"] == 10
    assert row["cache_creation_tokens"] == 5


def test_openclaw_tool_result_row() -> None:
    line = {
        "type": "message",
        "id": "55a2de34-e73f-4611-81e0-12f5af79f33d",
        "parentId": "b3a13e24-0578-443a-a076-3503a2e8b649",
        "timestamp": "2026-06-20T04:26:02.137Z",
        "message": {
            "role": "toolResult",
            "toolCallId": "call_1Rvx",
            "toolName": "bash",
            "isError": False,
            "content": [{"type": "toolResult", "toolCallId": "call_1Rvx", "content": "done", "text": "done"}],
        },
    }
    row = openclaw_event_row(line, session_id=OPENCLAW_SESSION, account="a", device="openclaw", seq=3, ingested_at=INGESTED_AT)
    assert row["role"] == "tool"
    assert row["subtype"] == "tool_result"
    assert row["tool_name"] == "bash"
    assert row["turn_id"] == "call_1Rvx"
    assert row["text"] == "done"
    assert json.loads(row["tool_result_json"])["content"][0]["content"] == "done"


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
    openclaw = envelope(
        openclaw_record({"type": "message", "id": "m1", "message": {"role": "user", "content": "hi"}}, seq=0),
        tool="openclaw",
    )
    assert record_to_event_row(claude, ingested_at=INGESTED_AT)["source"] == "claude_code"
    assert record_to_event_row(codex, ingested_at=INGESTED_AT)["source"] == "codex"
    assert record_to_event_row(openclaw, ingested_at=INGESTED_AT)["source"] == "openclaw"


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


class LegacyCodexWarehouse(FakeWarehouse):
    """Holds rows the pre-2026-10-02 normalizer wrote: Codex custom tool calls
    as role 'meta' with no tool_name. A row stops being legacy once rewritten."""

    def __init__(self, legacy: list[dict[str, object]]) -> None:
        super().__init__()
        self.legacy = list(legacy)
        self.limits: list[int] = []

    def legacy_codex_tool_rows(self, *, limit: int) -> list[dict[str, object]]:
        self.limits.append(limit)
        return self.legacy[:limit]

    def insert_agent_session_events(self, rows) -> None:
        super().insert_agent_session_events(rows)
        fixed = {(row["session_id"], row["event_uuid"]) for row in rows}
        self.legacy = [row for row in self.legacy if (row["session_id"], row["event_uuid"]) not in fixed]


def _legacy_codex_row(line: dict, *, seq: int) -> dict[str, object]:
    return {
        "source": "codex",
        "session_id": CODEX_SESSION,
        "event_uuid": f"{CODEX_SESSION}#{seq}",
        "account": "zach@example.com",
        "device": "porygon",
        "seq": seq,
        "raw_json": json.dumps(line, sort_keys=True, separators=(",", ":")),
    }


def test_runner_renormalizes_legacy_codex_tool_rows_from_their_raw_json() -> None:
    # History is fixed from the line each row already stores: the normalizer is
    # a pure function of (line, session, seq), so re-running it over raw_json
    # through the normal upsert rewrites the row in place, same primary key.
    warehouse = LegacyCodexWarehouse(
        [
            _legacy_codex_row(_codex_custom_call("exec", _CODEX_EXEC_SCRIPT), seq=10),
            _legacy_codex_row(_codex_custom_output([{"type": "input_text", "text": "done"}]), seq=14),
            _legacy_codex_row(_codex_custom_call("apply_patch", "*** Update File: a.py\n", call_id="c2"), seq=20),
        ]
    )
    summary = AgentSessionsDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: [],
        logger=FakeLogger(),
        now=lambda: INGESTED_AT,
        codex_renormalize_batch_rows=2,
    ).sync()
    assert summary.codex_rows_renormalized == 3
    assert warehouse.legacy == []
    by_seq = {row["seq"]: row for row in warehouse.events}
    assert by_seq[10]["tool_name"] == "exec" and by_seq[10]["subtype"] == "tool_use"
    assert by_seq[10]["event_uuid"] == f"{CODEX_SESSION}#10"
    assert by_seq[10]["account"] == "zach@example.com" and by_seq[10]["device"] == "porygon"
    assert by_seq[14]["subtype"] == "tool_result" and by_seq[14]["text"] == "done"
    assert by_seq[20]["tool_name"] == "apply_patch"
    # A rewrite is an ingest: it carries this run's ingested_at, so the
    # timeline's incremental pass re-reads the sessions it touched.
    assert {row["ingested_at"] for row in warehouse.events} == {INGESTED_AT}


def test_runner_renormalization_is_bounded_per_run() -> None:
    legacy = [
        _legacy_codex_row(_codex_custom_call("exec", "text(1)", call_id=f"c{seq}"), seq=seq) for seq in range(5)
    ]
    warehouse = LegacyCodexWarehouse(legacy)
    summary = AgentSessionsDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: [],
        logger=FakeLogger(),
        now=lambda: INGESTED_AT,
        codex_renormalize_batch_rows=2,
        codex_renormalize_max_rows=3,
    ).sync()
    # Batches of two until the per-run cap: 2 + 1, and two rows left for later.
    assert warehouse.limits == [2, 1]
    assert summary.codex_rows_renormalized == 3
    assert len(warehouse.legacy) == 2


def test_runner_stops_when_a_legacy_row_cannot_be_renormalized() -> None:
    # A row whose raw_json no longer parses would otherwise be fetched forever.
    warehouse = LegacyCodexWarehouse(
        [{**_legacy_codex_row(_codex_custom_call("exec", "text(1)"), seq=1), "raw_json": "not json"}]
    )
    summary = AgentSessionsDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: [],
        logger=FakeLogger(),
        now=lambda: INGESTED_AT,
    ).sync()
    assert summary.codex_rows_renormalized == 0
    assert warehouse.limits and len(warehouse.limits) == 1


# --- Muse normalizer ---------------------------------------------------
#
# Muse (the Muse models' coding CLI, not Meta's Muse agent)
# keeps one append-only event log per session. Each line is a runtime record
# {id, sequence, recorded_at (epoch MICROseconds), payload_type, payload};
# conversational content rides payload_type 'runtime.session' run events and
# 'runtime.user_intent.accepted'. Fixtures below follow the 1.4.7 shapes.

MUSE_CODE_SESSION = "01a13804-ba3b-7ec0-9308-cb2d86d27ed3"
# 1791941196347462 epoch microseconds.
_MUSE_CODE_AT = datetime(2026, 10, 14, 1, 26, 36, 347462, tzinfo=UTC)


def _muse_code_record(payload_type: str, payload: dict, *, seq: int = 1, record_id: str = "rec-1") -> dict:
    return {
        "schema_version": 1,
        "id": record_id,
        "stream": {"kind": "session", "id": MUSE_CODE_SESSION},
        "sequence": seq,
        "recorded_at": 1791941196347462,
        "record_type": "event",
        "durability": "durable",
        "causation_id": None,
        "payload_type": payload_type,
        "payload_schema_version": 1,
        "payload": payload,
    }


def _muse_code_row(line: dict, *, seq: int = 0, session: dict | None = None) -> dict:
    return muse_code_event_row(
        line,
        session_id=MUSE_CODE_SESSION,
        account="a",
        device="d",
        seq=seq,
        ingested_at=INGESTED_AT,
        session=session or {},
    )


def test_muse_code_metadata_row_carries_cwd_model_and_version() -> None:
    row = _muse_code_row(
        _muse_code_record(
            "runtime.session.metadata",
            {
                "kind": "metadata",
                "record": {
                    "workspace_root": "/work/repo",
                    "provider_id": "meta",
                    "model_id": "muse-wonder-1.0",
                    "build": {"sha": "abc", "semver": "1.4.7"},
                },
            },
        )
    )
    assert row["source"] == "muse_code"
    assert row["role"] == "meta"
    assert row["cwd"] == "/work/repo"
    assert row["model"] == "muse-wonder-1.0"
    assert row["cli_version"] == "1.4.7"
    assert row["event_uuid"] == "rec-1"
    assert row["occurred_at"] == _MUSE_CODE_AT


def test_muse_code_workspace_branch_row_carries_git_fields() -> None:
    row = _muse_code_row(
        _muse_code_record(
            "session.workspace_branch.observed",
            {
                "kind": "workspace_branch",
                "record": {
                    "workspace_root": "/work/repo",
                    "reference": {"kind": "branch", "name": "my-branch"},
                    "vcs": "git",
                    "commit": "0b9fd993d8a4",
                },
            },
        )
    )
    assert (row["cwd"], row["git_branch"], row["git_commit"]) == ("/work/repo", "my-branch", "0b9fd993d8a4")


def test_muse_code_user_intent_is_the_typed_text_not_the_model_message() -> None:
    row = _muse_code_row(
        _muse_code_record(
            "runtime.user_intent.accepted",
            {
                "intent_id": "intent-1",
                "surface": "main",
                "semantic_kind": {"kind": "chat"},
                "refill_blocks": [{"kind": "text", "text": "fix the parser"}],
                "model_messages": [
                    {"content": [{"kind": "text", "text": "<system_instructions> injected </system_instructions>fix the parser"}]}
                ],
            },
        )
    )
    assert row["role"] == "user"
    assert row["subtype"] == "message"
    assert row["text"] == "fix the parser"


def _muse_code_run_event(event: dict, *, record_id: str = "rec-1") -> dict:
    return _muse_code_record(
        "runtime.session",
        {"kind": "run", "run_id": "run-1", "event": event},
        record_id=record_id,
    )


def test_muse_code_assistant_message_is_text_and_reasoning_is_not() -> None:
    message = _muse_code_row(
        _muse_code_run_event(
            {"kind": "assistant_message_committed", "message_id": "m1", "response_id": "r1", "text": "all done"}
        )
    )
    reasoning = _muse_code_row(
        _muse_code_run_event(
            {"kind": "reasoning_summary_committed", "message_id": "m2", "text": "private summary"}
        )
    )
    assert (message["role"], message["subtype"], message["text"]) == ("assistant", "message", "all done")
    assert message["event_type"] == "assistant_message_committed"
    # Reasoning stays in raw_json only, as for every other agent source.
    assert (reasoning["role"], reasoning["subtype"], reasoning["text"]) == ("assistant", "thinking", "")
    assert "private summary" in reasoning["raw_json"]


def test_muse_code_tool_batches_pair_by_message_id() -> None:
    call = _muse_code_row(
        _muse_code_run_event(
            {
                "kind": "assistant_tool_calls_committed",
                "message_id": "batch-1",
                "response_id": "r1",
                "tool_calls": [
                    {"id": "fc1", "call_id": "call-1", "name": "bash", "args": '{"command":"ls"}'},
                    {"id": "fc2", "call_id": "call-2", "name": "read_file", "args": '{"path":"README.md"}'},
                ],
            }
        )
    )
    result = _muse_code_row(
        _muse_code_run_event(
            {
                "kind": "tool_result_batch_committed",
                "batch_id": "batch-1",
                "results": [
                    {"tool_call_index": 0, "tool_call_id": "call-1", "text": "file1.txt"},
                    {"tool_call_index": 1, "tool_call_id": "call-2", "text": "# Title"},
                ],
            }
        )
    )
    assert (call["role"], call["subtype"], call["tool_name"]) == ("assistant", "tool_use", "bash")
    assert call["text"] == ""
    calls = json.loads(call["tool_input_json"])["tool_calls"]
    assert [c["name"] for c in calls] == ["bash", "read_file"]
    assert calls[1]["args"] == '{"path":"README.md"}'
    assert (result["role"], result["subtype"]) == ("tool", "tool_result")
    assert result["text"] == "file1.txt\n# Title"
    # One pairing key for the whole batch, as Codex pairs by turn_id.
    assert call["turn_id"] == result["turn_id"] == "batch-1"


def test_muse_code_model_completed_carries_usage_and_model() -> None:
    row = _muse_code_row(
        _muse_code_run_event(
            {
                "kind": "model_completed",
                "usage": {
                    "input_tokens": 42005,
                    "output_tokens": 162,
                    "cached_tokens": 38388,
                    "cache_write_tokens": 7,
                    "cache_read_tokens": 38388,
                    "reasoning_tokens": 3,
                },
                "duration_ms": 5750,
                "finish_reason": "tool_calls",
                "model": "muse-wonder-1.0",
            }
        )
    )
    assert row["role"] == "meta"
    assert row["model"] == "muse-wonder-1.0"
    assert (row["input_tokens"], row["output_tokens"]) == (42005, 162)
    assert (row["cache_read_tokens"], row["cache_creation_tokens"]) == (38388, 7)


def test_muse_code_subagent_started_prompt_is_a_sidechain_user_row() -> None:
    started = _muse_code_run_event({"kind": "started", "prompt": "investigate the flake"})
    main = _muse_code_row(started)
    sub = _muse_code_row(started, session={"parent_session_id": "parent-session"})
    # In a main session the run prompt is the model message, already carried
    # by runtime.user_intent.accepted; keeping it would double every turn.
    assert (main["role"], main["text"], main["is_sidechain"]) == ("meta", "", 0)
    # A subagent has no user_intent: its brief is the opening prompt, and the
    # whole session is a sidechain of the parent the uploader named.
    assert (sub["role"], sub["text"]) == ("user", "investigate the flake")
    assert sub["is_sidechain"] == 1
    assert sub["parent_uuid"] == "parent-session"
    assert sub["entrypoint"] == "subagent"


def test_muse_code_retained_frame_and_marker_lines_are_meta_never_errors() -> None:
    child = json.dumps(_muse_code_record("runtime.session.permission_format_declared", {"format": "profile_v1"}))
    frame = _muse_code_row(
        {
            "retained_frame": "session_permission_transaction",
            "frame_schema_version": 1,
            "outer_log_ordinal": 1,
            "transaction_id": "txn-1",
            "children": [{"child_index": 0, "record_json": child}],
        },
        seq=0,
    )
    marker = _muse_code_row(
        {
            "retained_marker": "omitted_live_only",
            "schema_version": 1,
            "stream": {"kind": "session", "id": MUSE_CODE_SESSION},
            "position": {"id": "pos-1", "sequence": 77},
            "omitted_record": {"record_type": "status", "payload_type": "runtime.session"},
        },
        seq=5,
    )
    assert (frame["role"], frame["event_uuid"], frame["occurred_at"]) == ("meta", "txn-1", _MUSE_CODE_AT)
    assert (marker["role"], marker["event_uuid"]) == ("meta", "pos-1")
    assert marker["occurred_at"] == datetime(1970, 1, 1, tzinfo=UTC)


def test_muse_code_record_to_event_row_routes_the_tool_and_session() -> None:
    record = envelope(
        {
            "tool": "muse_code",
            "session_id": "sub-1",
            "seq": 2,
            "line": _muse_code_run_event({"kind": "started", "prompt": "do the subtask"}),
            "session": {"parent_session_id": "parent-1"},
        },
        tool="muse_code",
    )
    row = record_to_event_row(record, ingested_at=INGESTED_AT)
    assert row["source"] == "muse_code"
    assert row["session_id"] == "sub-1"
    assert (row["role"], row["text"], row["parent_uuid"]) == ("user", "do the subtask", "parent-1")
    assert (row["account"], row["device"]) == ("zach@example.com", "porygon")


def test_muse_code_malformed_shapes_degrade_to_meta_rows() -> None:
    # The normalizer runs inside the one ingest pass every provider shares:
    # an unexpected Muse shape must never fail that pass.
    for line in (
        {},
        {"payload_type": "runtime.session", "payload": "nope"},
        {"payload_type": "runtime.user_intent.accepted", "payload": {"refill_blocks": "nope"}},
        {"recorded_at": "not-a-time", "payload_type": "something.new", "payload": {}},
    ):
        row = _muse_code_row(line, seq=3)
        assert row["source"] == "muse_code"
        assert row["event_uuid"] == f"{MUSE_CODE_SESSION}#3"


def test_muse_code_recorded_at_survives_the_uploaders_float_round_trip() -> None:
    # The Go uploader decodes each line into map[string]any, so an integer
    # recorded_at reaches ingest as a float (1791941196347462.0). Measured on
    # the first production batches 2026-10-14: 3,120 of 3,132 rows read as the
    # epoch because only an int was accepted. float64 is exact to 2**53, which
    # microsecond timestamps stay under until the 2250s.
    line = _muse_code_record("runtime.session.metadata", {"kind": "metadata", "record": {}})
    line["recorded_at"] = float(line["recorded_at"])
    assert _muse_code_row(line)["occurred_at"] == _MUSE_CODE_AT
    line["recorded_at"] = 1791941196347462.5
    assert _muse_code_row(line)["occurred_at"] == datetime(1970, 1, 1, tzinfo=UTC)


# --- Argon (Google Antigravity CLI, `agy`) normalizer -----------------------
#
# Antigravity CLI keeps one untruncated append-only transcript per session at
# ~/.gemini/antigravity-cli/brain/<id>/.system_generated/logs/transcript_full.jsonl.
# Each line is {step_index, source, type, status, created_at, ...}. Fixtures
# below follow the agy 1.3.4 shapes verified on 2026-10-13.

ARGON_SESSION = "d45f2004-492f-4a80-b387-74acbcae9a78"
_ARGON_AT = datetime(2026, 10, 14, 2, 49, 46, 513447, tzinfo=UTC)


def _argon_step(
    step_type: str,
    *,
    step_index: int = 0,
    source: str = "MODEL",
    status: str = "DONE",
    **extra: object,
) -> dict:
    return {
        "step_index": step_index,
        "source": source,
        "type": step_type,
        "status": status,
        "created_at": "2026-10-14T02:49:46.513447Z",
        **extra,
    }


def _argon_row(line: dict, *, seq: int = 0, session: dict | None = None) -> dict:
    return argon_event_row(
        line,
        session_id=ARGON_SESSION,
        account="a",
        device="d",
        seq=seq,
        ingested_at=INGESTED_AT,
        session=session or {},
    )


def test_argon_user_input_extracts_typed_prompt_and_model_selection() -> None:
    row = _argon_row(
        _argon_step(
            "USER_INPUT",
            step_index=0,
            source="USER_EXPLICIT",
            content=(
                "<USER_REQUEST>\n"
                'Start by reading the start-here skill: `pdw call skills__skill_read --data \'{"name":"start-here"}\'` '
                "(skills are MCP-only; there is no sk CLI).\n\n"
                "Add an argon ingester\n"
                "</USER_REQUEST>\n"
                "<ADDITIONAL_METADATA>\n"
                "The current local time is: 2026-10-13T22:49:44-04:00.\n"
                "</ADDITIONAL_METADATA>\n"
                "<USER_SETTINGS_CHANGE>\n"
                "The user changed setting `Model Selection` from None to Gemini 4 Argon (Medium). "
                "No need to comment on this change if the user doesn't ask about it.\n"
                "</USER_SETTINGS_CHANGE>"
            ),
        )
    )
    assert row["source"] == "argon"
    assert row["event_uuid"] == f"{ARGON_SESSION}#0"
    assert row["occurred_at"] == _ARGON_AT
    assert (row["role"], row["subtype"], row["text"]) == ("user", "message", "Add an argon ingester")
    assert row["model"] == "Gemini 4 Argon (Medium)"


def test_argon_planner_response_message_thinking_and_usage() -> None:
    message = _argon_row(
        _argon_step(
            "PLANNER_RESPONSE",
            step_index=4,
            content="All wired up and tested.",
            input_tokens=15316,
            cache_read_tokens=12400,
            output_tokens=297,
        ),
        seq=4,
    )
    thinking = _argon_row(
        _argon_step(
            "PLANNER_RESPONSE",
            step_index=5,
            content="",
            thinking="Let me check the catalog first.",
            input_tokens=16000,
            output_tokens=45,
        ),
        seq=5,
    )
    assert (message["role"], message["subtype"], message["text"]) == (
        "assistant",
        "message",
        "All wired up and tested.",
    )
    assert (message["input_tokens"], message["output_tokens"], message["cache_read_tokens"]) == (15316, 297, 12400)
    # Reasoning stays in raw_json only, as for every other agent source.
    assert (thinking["role"], thinking["subtype"], thinking["text"]) == ("assistant", "thinking", "")
    assert "Let me check the catalog first." in thinking["raw_json"]


def test_argon_tool_calls_and_generic_tool_results() -> None:
    single_call = _argon_row(
        _argon_step(
            "PLANNER_RESPONSE",
            step_index=1,
            thinking="Run pdw search.",
            tool_calls=[
                {
                    "name": "run_command",
                    "args": {
                        "CommandLine": "pdw search budget --priorities self,direct,cc",
                        "Cwd": "/work/repo",
                        "WaitMsBeforeAsync": 5000,
                    },
                }
            ],
            input_tokens=12000,
            output_tokens=80,
        ),
        seq=1,
    )
    multi_call = _argon_row(
        _argon_step(
            "PLANNER_RESPONSE",
            step_index=2,
            tool_calls=[
                {"name": "view_file", "args": {"AbsolutePath": "/work/repo/a.go"}},
                {"name": "view_file", "args": {"AbsolutePath": "/work/repo/b.go"}},
            ],
        ),
        seq=2,
    )
    result = _argon_row(
        _argon_step(
            "GENERIC",
            step_index=3,
            content="The command exited with code 0.\nOutput:\n1. slack · direct · budget note",
        ),
        seq=3,
    )
    assert (single_call["role"], single_call["subtype"], single_call["tool_name"]) == (
        "assistant",
        "tool_use",
        "run_command",
    )
    assert single_call["cwd"] == "/work/repo"
    assert json.loads(single_call["tool_input_json"])["CommandLine"] == "pdw search budget --priorities self,direct,cc"
    assert (multi_call["role"], multi_call["subtype"], multi_call["tool_name"]) == (
        "assistant",
        "tool_use",
        "view_file",
    )
    calls = json.loads(multi_call["tool_input_json"])["tool_calls"]
    assert [c["args"]["AbsolutePath"] for c in calls] == ["/work/repo/a.go", "/work/repo/b.go"]
    assert (result["role"], result["subtype"]) == ("tool", "tool_result")
    assert "budget note" in result["text"]
    assert json.loads(result["tool_result_json"])["status"] == "DONE"


def test_argon_checkpoint_and_system_message_steps() -> None:
    checkpoint = _argon_row(
        _argon_step(
            "CHECKPOINT",
            step_index=10,
            source="SYSTEM",
            content="# Resuming from a compaction\n\nSummary of progress.",
        ),
        seq=10,
    )
    sys_msg = _argon_row(
        _argon_step(
            "SYSTEM_MESSAGE",
            step_index=11,
            source="SYSTEM",
            content="The following is a <SYSTEM_MESSAGE> not actually sent by the user.",
        ),
        seq=11,
    )
    assert (checkpoint["role"], checkpoint["subtype"]) == ("meta", "compaction")
    assert "Resuming from a compaction" in checkpoint["text"]
    assert (sys_msg["role"], sys_msg["subtype"]) == ("system", "system_message")
    assert "SYSTEM_MESSAGE" in sys_msg["text"]


def test_argon_session_metadata_and_subagent_sidechain() -> None:
    step = _argon_step(
        "USER_INPUT",
        step_index=0,
        source="USER_EXPLICIT",
        content="<USER_REQUEST>\ninvestigate the flake\n</USER_REQUEST>",
    )
    sub = _argon_row(
        step,
        session={
            "cwd": "/work/repo",
            "title": "Investigate Flake",
            "parent_session_id": "parent-session",
        },
    )
    assert (sub["role"], sub["text"]) == ("user", "investigate the flake")
    assert sub["cwd"] == "/work/repo"
    assert sub["session_title"] == "Investigate Flake"
    assert sub["is_sidechain"] == 1
    assert sub["parent_uuid"] == "parent-session"
    assert sub["entrypoint"] == "subagent"


def test_argon_record_to_event_row_routes_the_tool_and_session() -> None:
    record = envelope(
        {
            "tool": "argon",
            "session_id": "sub-1",
            "seq": 2,
            "line": _argon_step(
                "USER_INPUT",
                step_index=0,
                source="USER_EXPLICIT",
                content="<USER_REQUEST>\ndo the subtask\n</USER_REQUEST>",
            ),
            "session": {"parent_session_id": "parent-1", "cwd": "/work/repo"},
        },
        tool="argon",
    )
    row = record_to_event_row(record, ingested_at=INGESTED_AT)
    assert row["source"] == "argon"
    assert row["session_id"] == "sub-1"
    assert (row["role"], row["text"], row["parent_uuid"], row["cwd"]) == (
        "user",
        "do the subtask",
        "parent-1",
        "/work/repo",
    )
    assert (row["account"], row["device"]) == ("zach@example.com", "porygon")


def test_argon_malformed_shapes_degrade_to_meta_rows() -> None:
    for line in (
        {},
        {"type": "PLANNER_RESPONSE", "tool_calls": "nope"},
        {"type": "USER_INPUT", "content": None},
        {"created_at": "not-a-time", "step_index": "bad", "type": "SOMETHING_NEW"},
    ):
        row = _argon_row(line, seq=3)
        assert row["source"] == "argon"
        assert row["event_uuid"] == f"{ARGON_SESSION}#3"
