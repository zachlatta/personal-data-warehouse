from __future__ import annotations

from datetime import UTC, datetime
import os
import uuid

import pytest
from dotenv import load_dotenv

from personal_data_warehouse.agent_sessions_drive_ingest import (
    AgentSessionsDriveIngestRunner,
    claude_desktop_event_row,
)
from personal_data_warehouse.postgres import PostgresWarehouse

INGESTED = datetime(2026, 6, 22, 18, tzinfo=UTC)


# --- row builder (pure, no DB) ----------------------------------------------


def _row(line, *, seq=0):
    return claude_desktop_event_row(
        line,
        session_id="conv-1",
        account="account@example.com",
        device="prod",
        seq=seq,
        ingested_at=INGESTED,
    )


def test_conversation_header_row_carries_title_and_model() -> None:
    row = _row(
        {
            "type": "conversation",
            "uuid": "conv-1",
            "name": "Euro per liter to dollars per gallon",
            "model": "claude-sonnet-4-6",
            "created_at": "2026-06-06T17:40:30Z",
        }
    )
    assert row["source"] == "claude_desktop"
    assert row["event_uuid"] == "conv-1"
    assert row["role"] == "meta"
    assert row["session_title"] == "Euro per liter to dollars per gallon"
    assert row["model"] == "claude-sonnet-4-6"


def test_human_message_row_is_user_text() -> None:
    row = _row(
        {
            "type": "message",
            "uuid": "m1",
            "sender": "human",
            "created_at": "2026-06-06T17:40:31Z",
            "content": [{"type": "text", "text": "2.35 euro per liter is how many dollars per gallon"}],
        },
        seq=1,
    )
    assert row["role"] == "user"
    assert row["subtype"] == "message"
    assert "dollars per gallon" in row["text"]
    assert row["event_uuid"] == "m1"


def test_assistant_message_row_with_model_and_tool() -> None:
    row = _row(
        {
            "type": "message",
            "uuid": "m2",
            "sender": "assistant",
            "model": "claude-sonnet-4-6",
            "created_at": "2026-06-06T17:40:33Z",
            "content": [
                {"type": "text", "text": "Let me calculate."},
                {"type": "tool_use", "id": "tool-9", "name": "repl", "input": {"code": "2.35*3.785"}},
            ],
        },
        seq=2,
    )
    assert row["role"] == "assistant"
    assert row["model"] == "claude-sonnet-4-6"
    assert row["subtype"] == "tool_use"
    assert row["tool_name"] == "repl"
    assert '"code"' in row["tool_input_json"]
    assert row["turn_id"] == "tool-9"
    assert "Let me calculate." in row["text"]


def test_human_message_includes_attachment_text() -> None:
    row = _row(
        {
            "type": "message",
            "uuid": "m3",
            "sender": "human",
            "created_at": "2026-06-06T17:40:31Z",
            "content": [{"type": "text", "text": "summarize this"}],
            "attachments": [{"file_name": "notes.txt", "extracted_content": "QUARTERLY REVENUE 1.2M"}],
        },
        seq=1,
    )
    assert "summarize this" in row["text"]
    assert "QUARTERLY REVENUE 1.2M" in row["text"]


# --- DB-backed tests --------------------------------------------------------


def _postgres_url() -> str:
    load_dotenv()
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if not url:
        pytest.skip("POSTGRES_DATABASE_URL is not set")
    return url


@pytest.fixture()
def warehouse():
    schema = "pdw_test_" + uuid.uuid4().hex
    wh = PostgresWarehouse(_postgres_url(), schema=schema)
    try:
        yield wh
    finally:
        wh._command(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        wh.close()


class FakeLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


def test_claude_desktop_tables_and_cursor_round_trip(warehouse) -> None:
    warehouse.ensure_claude_desktop_tables()
    warehouse.ensure_claude_desktop_tables()  # idempotent
    assert warehouse._relation_exists("claude_desktop_credentials")
    assert warehouse._relation_exists("claude_desktop_conversation_state")

    assert warehouse.claude_desktop_cursor(account="account@example.com", conversation_id="c1") == ""
    warehouse.record_claude_desktop_cursor(
        account="account@example.com", conversation_id="c1", updated_at="2026-06-02T00:00:00Z", now=INGESTED
    )
    assert warehouse.claude_desktop_cursor(account="account@example.com", conversation_id="c1") == "2026-06-02T00:00:00Z"


def test_read_credential_after_manual_insert(warehouse) -> None:
    warehouse.ensure_claude_desktop_tables()
    assert warehouse.read_claude_desktop_credential(account="account@example.com") is None
    warehouse._command(
        """
        INSERT INTO claude_desktop_credentials (account, session_key, org_id, captured_at)
        VALUES (%s, %s, %s, %s)
        """,
        ("account@example.com", "sk-ant-sid-xyz", "org-1", INGESTED),
    )
    cred = warehouse.read_claude_desktop_credential(account="account@example.com")
    assert cred is not None
    assert cred["session_key"] == "sk-ant-sid-xyz"
    assert cred["org_id"] == "org-1"


def test_claude_desktop_persists_end_to_end(warehouse) -> None:
    def _envelope(seq, line):
        return {
            "schema_version": 1,
            "source": "agent_sessions",
            "account": "account@example.com",
            "device": "prod",
            "exported_at": "2026-06-22T18:00:00+00:00",
            "record_type": "claude_desktop_event",
            "record": {"tool": "claude_desktop", "session_id": "conv-xyz", "seq": seq, "line": line},
        }

    batch = {
        "schema_version": 1,
        "source": "agent_sessions",
        "records": [
            _envelope(0, {
                "type": "conversation",
                "uuid": "conv-xyz",
                "name": "Science fair display",
                "model": "claude-sonnet-4-6",
                "created_at": "2026-06-06T17:40:30Z",
            }),
            _envelope(1, {
                "type": "message",
                "uuid": "m1",
                "sender": "human",
                "created_at": "2026-06-06T17:40:31Z",
                "content": [{"type": "text", "text": "what display for a science fair"}],
            }),
            _envelope(2, {
                "type": "message",
                "uuid": "m2",
                "sender": "assistant",
                "model": "claude-sonnet-4-6",
                "created_at": "2026-06-06T17:40:33Z",
                "content": [{"type": "text", "text": "A tri-fold board works well"}],
            }),
        ],
    }

    summary = AgentSessionsDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: [batch],
        logger=FakeLogger(),
        now=lambda: INGESTED,
    ).sync()
    assert summary.events_written == 3

    rows = warehouse._query(
        "SELECT role, text, model FROM agent_session_events "
        "WHERE source = 'claude_desktop' AND session_id = 'conv-xyz' ORDER BY seq"
    )
    assert rows[0][0] == "meta"
    assert rows[1] == ("user", "what display for a science fair", "")
    assert rows[2][0] == "assistant"
    assert rows[2][2] == "claude-sonnet-4-6"

    view = warehouse._query(
        "SELECT title, model, first_prompt, event_count, user_event_count, assistant_event_count "
        "FROM clean_agent_sessions WHERE source = 'claude_desktop' AND session_id = 'conv-xyz'"
    )
    assert view == [("Science fair display", "claude-sonnet-4-6", "what display for a science fair", 3, 1, 1)]
