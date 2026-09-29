"""Muse (Meta's hosted personal agent) transcripts and workspace files.

The uploader (`pdw ingest muse`, run on the Muse VM) ships two record types
through the agent-sessions inbox: ``muse_event`` (one transcript line) and
``muse_file`` (one workspace file's current content, or its tombstone).
"""

from __future__ import annotations

from datetime import UTC, datetime

from personal_data_warehouse.agent_sessions_drive_ingest import (
    AgentSessionsDriveIngestRunner,
    muse_event_row,
    muse_file_row,
    record_to_event_row,
)

INGESTED_AT = datetime(2026, 9, 28, 22, tzinfo=UTC)
MAIN = "9f2189a3-3d40-4253-b3c3-f6342cd83882"
SUB = "cca278e9-4051-462d-9586-55ebbffefb89"
CHAT_SESSION = {"kind": "chat", "opening_source": "runtime", "requester_agent_id": "", "model": "muse-spark"}
SUB_SESSION = {"kind": "subagent", "opening_source": "runtime", "requester_agent_id": MAIN, "model": "muse-spark"}


def row(line: dict, *, session: dict = CHAT_SESSION, session_id: str = MAIN, seq: int = 1) -> dict:
    return muse_event_row(
        line,
        session_id=session_id,
        account="zach@example.com",
        device="muse",
        seq=seq,
        ingested_at=INGESTED_AT,
        session=session,
    )


def item(seq: int, body: dict, *, source: str = "runtime") -> dict:
    return {"type": "item", "seq": seq, "source": source, "item": body, "created_at": "2026-09-27T21:27:02.404728148+00:00"}


class FakeLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


class FakeWarehouse:
    def __init__(self) -> None:
        self.events: list[dict] = []
        self.files: list[dict] = []

    def ensure_agent_sessions_tables(self) -> None:
        pass

    def insert_agent_session_events(self, rows) -> None:
        self.events.extend(rows)

    def insert_muse_files(self, rows) -> None:
        self.files.extend(rows)


def test_session_header_row() -> None:
    header = row(
        {"type": "session_header", "version": 1, "session_id": MAIN, "agent_id": MAIN, "created_at": "2026-09-27T20:39:30.844916987+00:00"},
        seq=0,
    )
    assert (header["source"], header["role"], header["subtype"]) == ("muse", "meta", "session_header")
    assert header["event_uuid"] == f"{MAIN}#header"
    assert header["occurred_at"] == datetime(2026, 9, 27, 20, 39, 30, 844916, tzinfo=UTC)
    assert header["entrypoint"] == "runtime"


def test_typed_user_message_is_a_user_turn() -> None:
    user = row(item(12, {"type": "message", "role": "user", "text": "How do I connect an MCP to you?"}))
    assert (user["role"], user["subtype"], user["text"]) == ("user", "message", "How do I connect an MCP to you?")
    assert user["event_uuid"] == f"{MAIN}#12"
    assert user["is_sidechain"] == 0


def test_machine_injected_user_messages_are_system_not_user() -> None:
    # Muse writes its background loops (self-improvement, the feed, cron
    # workers, onboarding) on the user channel; none of those are Zach.
    for source in ("runtime.self_improvement", "runtime.feed", "scheduler.cron", "runtime.onboarding"):
        injected = row(item(1, {"type": "message", "role": "user", "text": "## Step instructions"}, source=source))
        assert (injected["role"], injected["subtype"]) == ("system", source)
        assert injected["text"] == "## Step instructions"


def test_developer_message_is_system() -> None:
    developer = row(item(0, {"type": "message", "role": "developer", "text": "The user has just completed app setup."}, source="runtime.onboarding"))
    assert (developer["role"], developer["subtype"]) == ("system", "developer")


def test_subagent_session_is_a_sidechain_and_its_brief_is_not_zach() -> None:
    brief = row(
        item(0, {"type": "message_parts", "role": "user", "parts": [
            {"type": "text", "text": "[Subagent Context] You are running as a subagent"},
            {"type": "file_ref", "path": "inputs/photo.jpg"},
        ]}),
        session=SUB_SESSION,
        session_id=SUB,
    )
    assert brief["role"] == "system"
    assert brief["is_sidechain"] == 1
    assert brief["parent_uuid"] == MAIN
    assert brief["text"].startswith("[Subagent Context]")
    assert "inputs/photo.jpg" in brief["text"]


def test_assistant_message_and_commentary_carry_the_model() -> None:
    reply = row(item(7, {"type": "message", "role": "assistant", "text": "Hey Zach"}))
    commentary = row(item(8, {"type": "commentary_text", "text": "Now let me get that driving distance."}))
    assert (reply["role"], reply["subtype"], reply["text"], reply["model"]) == ("assistant", "message", "Hey Zach", "muse-spark")
    assert (commentary["role"], commentary["subtype"], commentary["text"]) == (
        "assistant", "commentary", "Now let me get that driving distance.")


def test_thinking_is_kept_raw_but_not_as_searchable_text() -> None:
    thinking = row(item(2, {"type": "thinking", "thinking": "private chain of thought", "signature": ""}))
    assert (thinking["role"], thinking["subtype"], thinking["text"]) == ("assistant", "thinking", "")
    assert "private chain of thought" in thinking["raw_json"]


def test_function_call_and_output_pair_by_call_id() -> None:
    call = row(item(3, {"type": "function_call", "call_id": "call_1", "name": "exec", "arguments": "{\"cmd\":\"pdw search x\"}"}))
    output = row(item(4, {"type": "function_call_output", "call_id": "call_1", "output": "{\"ok\":true}", "success": True}))
    assert (call["role"], call["subtype"], call["tool_name"], call["turn_id"]) == ("assistant", "tool_use", "exec", "call_1")
    assert call["tool_input_json"] == "{\"cmd\":\"pdw search x\"}"
    assert (output["role"], output["subtype"], output["turn_id"], output["text"]) == ("tool", "tool_result", "call_1", "{\"ok\":true}")


def test_compaction_checkpoint_keeps_its_summary() -> None:
    compaction = row(
        {"type": "compaction_checkpoint", "compaction_id": 3, "trigger": "threshold", "summary": "## Open work",
         "created_at": "2026-09-28T01:00:00+00:00", "checkpoint_seq": 10, "first_kept_seq": 8,
         "tokens_before": 1, "tokens_after": 1, "will_retry": False, "details_json": "{}"},
        seq=40,
    )
    assert (compaction["role"], compaction["subtype"], compaction["text"]) == ("meta", "compaction", "## Open work")
    assert compaction["event_uuid"] == f"{MAIN}#compaction-3"


def test_record_to_event_row_dispatches_muse_with_its_session_metadata() -> None:
    record = {
        "schema_version": 1,
        "source": "agent_sessions",
        "account": "zach@example.com",
        "device": "muse",
        "exported_at": "2026-09-28T22:00:00+00:00",
        "record_type": "muse_event",
        "record": {"tool": "muse", "session_id": SUB, "seq": 1, "session": SUB_SESSION,
                   "line": item(1, {"type": "message", "role": "assistant", "text": "done"})},
    }
    event = record_to_event_row(record, ingested_at=INGESTED_AT)
    assert (event["source"], event["session_id"], event["is_sidechain"]) == ("muse", SUB, 1)


def file_record(path: str, **fields) -> dict:
    record = {
        "path": path,
        "content_sha256": "a" * 64,
        "size_bytes": 12,
        "modified_at": "2026-09-28T17:41:00+00:00",
        "mime_type": "text/markdown",
        "is_text": True,
        "content_text": "# MEMORY\n- Zach",
        "storage_backend": "",
        "storage_key": "",
        "storage_file_id": "",
        "storage_url": "",
        "deleted": False,
    }
    record.update(fields)
    return {
        "schema_version": 1,
        "source": "agent_sessions",
        "account": "zach@example.com",
        "device": "muse",
        "exported_at": "2026-09-28T22:00:00+00:00",
        "record_type": "muse_file",
        "record": record,
    }


def test_muse_file_row_keeps_text_content_and_provenance() -> None:
    file_row = muse_file_row(file_record("MEMORY.md"), ingested_at=INGESTED_AT)
    assert (file_row["account"], file_row["path"], file_row["content_text"]) == ("zach@example.com", "MEMORY.md", "# MEMORY\n- Zach")
    assert (file_row["is_text"], file_row["is_deleted"], file_row["size_bytes"]) == (1, 0, 12)
    assert file_row["modified_at"] == datetime(2026, 9, 28, 17, 41, tzinfo=UTC)
    assert file_row["deleted_at"] == datetime(1970, 1, 1, tzinfo=UTC)
    assert (file_row["directory"], file_row["filename"]) == ("", "MEMORY.md")


def test_muse_file_row_for_a_binary_points_at_its_stored_blob() -> None:
    file_row = muse_file_row(
        file_record(
            "workspace/podcasts/ep/ep.mp3", is_text=False, content_text="", mime_type="audio/mpeg",
            storage_backend="google_drive", storage_key="muse/files/ab/abc.mp3", storage_file_id="drive-1",
        ),
        ingested_at=INGESTED_AT,
    )
    assert (file_row["is_text"], file_row["content_text"]) == (0, "")
    assert (file_row["storage_key"], file_row["storage_file_id"]) == ("muse/files/ab/abc.mp3", "drive-1")
    assert (file_row["directory"], file_row["filename"]) == ("workspace/podcasts/ep", "ep.mp3")


def test_muse_file_tombstone_marks_the_path_deleted() -> None:
    file_row = muse_file_row(file_record("memory/old.md", deleted=True, content_text=""), ingested_at=INGESTED_AT)
    assert file_row["is_deleted"] == 1
    assert file_row["deleted_at"] == INGESTED_AT


def test_runner_routes_muse_files_apart_from_events_and_keeps_the_newest_per_path() -> None:
    warehouse = FakeWarehouse()
    older = file_record("MEMORY.md", content_text="old")
    newer = file_record("MEMORY.md", content_text="new")
    newer["exported_at"] = "2026-09-28T22:05:00+00:00"
    event = {
        "schema_version": 1, "source": "agent_sessions", "account": "zach@example.com", "device": "muse",
        "exported_at": "2026-09-28T22:00:00+00:00", "record_type": "muse_event",
        "record": {"tool": "muse", "session_id": MAIN, "seq": 1, "session": CHAT_SESSION,
                   "line": item(1, {"type": "message", "role": "user", "text": "hi"})},
    }
    summary = AgentSessionsDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: [{"batch_file": {}, "records": [newer, event, older]}],
        logger=FakeLogger(),
        now=lambda: INGESTED_AT,
    ).sync()
    assert [e["source"] for e in warehouse.events] == ["muse"]
    assert [(f["path"], f["content_text"]) for f in warehouse.files] == [("MEMORY.md", "new")]
    assert (summary.events_written, summary.files_written) == (1, 1)


def test_created_at_may_be_an_epoch_number_or_garbage_and_never_raises() -> None:
    # Production 2026-09-29: some Muse lines stamp created_at as epoch seconds
    # ("1790624108.0"), and one unparseable value failed the whole
    # agent-sessions ingest run -- every provider's batches, not just Muse's.
    expected = datetime(2026, 9, 28, 19, 35, 8, tzinfo=UTC)
    for value in (1790624108.0, 1790624108, "1790624108.0", 1790624108000):
        line = {"type": "compaction_checkpoint", "compaction_id": 1, "summary": "s", "created_at": value}
        assert row(line)["occurred_at"] == expected, value
    for value in ("not a time", None, "", {"nested": 1}):
        line = {"type": "compaction_checkpoint", "compaction_id": 1, "summary": "s", "created_at": value}
        assert row(line)["occurred_at"] == datetime(1970, 1, 1, tzinfo=UTC), value
