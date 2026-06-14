from __future__ import annotations

from datetime import UTC, datetime
import os
import uuid

import pytest
from dotenv import load_dotenv

from personal_data_warehouse.agent_sessions_drive_ingest import (
    AgentSessionsDriveIngestRunner,
    sync_version,
)
from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.schema import AGENT_SESSION_EVENT_COLUMNS


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


def _event_row(**overrides) -> dict:
    occurred = overrides.pop("occurred_at", datetime(2026, 6, 14, 17, tzinfo=UTC))
    ingested = datetime(2026, 6, 14, 18, tzinfo=UTC)
    row = {column: "" for column in AGENT_SESSION_EVENT_COLUMNS}
    row.update(
        {
            "source": "claude_code",
            "session_id": "sess-1",
            "event_uuid": "evt-1",
            "account": "zach@example.com",
            "device": "porygon",
            "seq": 0,
            "occurred_at": occurred,
            "role": "user",
            "event_type": "user",
            "subtype": "message",
            "text": "",
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_read_tokens": 0,
            "cache_creation_tokens": 0,
            "is_sidechain": 0,
            "raw_json": "{}",
            "ingested_at": ingested,
            "sync_version": sync_version(ingested),
        }
    )
    row.update(overrides)
    return row


def test_ensure_creates_table_and_view(warehouse) -> None:
    warehouse.ensure_agent_sessions_tables()
    # idempotent
    warehouse.ensure_agent_sessions_tables()
    assert warehouse._relation_exists("agent_session_events")
    assert warehouse._relation_exists("clean_agent_sessions")


def test_insert_upserts_by_primary_key(warehouse) -> None:
    warehouse.ensure_agent_sessions_tables()
    warehouse.insert_agent_session_events([_event_row(text="first", sync_version=1)])
    warehouse.insert_agent_session_events([_event_row(text="second", sync_version=2)])
    rows = warehouse._query("SELECT text FROM agent_session_events WHERE event_uuid = 'evt-1'")
    assert rows == [("second",)]


def test_clean_agent_sessions_view_rolls_up_session(warehouse) -> None:
    warehouse.ensure_agent_sessions_tables()
    warehouse.insert_agent_session_events(
        [
            _event_row(
                event_uuid="e0",
                seq=0,
                role="meta",
                event_type="ai-title",
                session_title="My Session",
                cwd="/work/repo",
                git_branch="main",
                occurred_at=datetime(2026, 6, 14, 17, 0, tzinfo=UTC),
            ),
            _event_row(
                event_uuid="e1",
                seq=1,
                role="user",
                text="do the thing",
                occurred_at=datetime(2026, 6, 14, 17, 1, tzinfo=UTC),
            ),
            _event_row(
                event_uuid="e2",
                seq=2,
                role="assistant",
                event_type="assistant",
                model="claude-fable-5",
                text="done",
                input_tokens=100,
                output_tokens=20,
                cache_read_tokens=5,
                cache_creation_tokens=7,
                occurred_at=datetime(2026, 6, 14, 17, 2, tzinfo=UTC),
            ),
        ]
    )
    rows = warehouse._query(
        """
        SELECT title, cwd, git_branch, model, first_prompt, event_count,
               user_event_count, assistant_event_count, input_tokens, output_tokens,
               cache_read_tokens, cache_creation_tokens, started_at, ended_at
        FROM clean_agent_sessions WHERE session_id = 'sess-1'
        """
    )
    assert len(rows) == 1
    (title, cwd, git_branch, model, first_prompt, event_count, user_count, asst_count,
     in_tok, out_tok, cache_r, cache_c, started, ended) = rows[0]
    assert title == "My Session"
    assert cwd == "/work/repo"
    assert git_branch == "main"
    assert model == "claude-fable-5"
    assert first_prompt == "do the thing"
    assert event_count == 3
    assert user_count == 1
    assert asst_count == 1
    assert in_tok == 100
    assert out_tok == 20
    assert cache_r == 5
    assert cache_c == 7
    assert started == datetime(2026, 6, 14, 17, 0, tzinfo=UTC)
    assert ended == datetime(2026, 6, 14, 17, 2, tzinfo=UTC)


def test_searchable_text_includes_agent_session_branches(warehouse) -> None:
    # All source tables must exist for the searchable_text view to build.
    warehouse.ensure_tables()
    warehouse.ensure_calendar_tables()
    warehouse.ensure_contacts_tables()
    warehouse.ensure_apple_voice_memos_tables(backfill_content_hashes=False)
    warehouse.ensure_apple_notes_tables()
    warehouse.ensure_apple_messages_tables()
    warehouse.ensure_whatsapp_tables()
    warehouse.ensure_slack_tables()
    warehouse.ensure_upstream_mutation_tables()
    warehouse.ensure_agent_sessions_tables()

    warehouse.insert_agent_session_events(
        [
            _event_row(event_uuid="e1", role="user", text="investigate the zanzibar migration"),
            _event_row(
                event_uuid="e2",
                seq=1,
                role="meta",
                event_type="ai-title",
                session_title="zanzibar planning",
            ),
            _event_row(
                event_uuid="e3",
                seq=2,
                role="tool",
                event_type="user",
                subtype="tool_result",
                text="zanzibar appears in a tool result and must NOT be searchable",
            ),
        ]
    )

    rows = warehouse._query(
        "SELECT subsource FROM searchable_text WHERE source = 'agent_session' AND text ~* '\\mzanzibar\\M' ORDER BY subsource"
    )
    subsources = [r[0] for r in rows]
    # user content branch (subsource = source = 'claude_code') and title branch.
    assert "claude_code" in subsources
    assert "title" in subsources
    # tool-result text is stored but excluded from search.
    assert all(s != "tool" for s in subsources)


def test_runner_persists_into_warehouse_end_to_end(warehouse) -> None:
    batch = {
        "schema_version": 1,
        "source": "agent_sessions",
        "records": [
            {
                "schema_version": 1,
                "source": "agent_sessions",
                "account": "zach@example.com",
                "device": "porygon",
                "exported_at": "2026-06-14T18:00:00+00:00",
                "record_type": "claude_code_event",
                "record": {
                    "tool": "claude_code",
                    "session_id": "sess-xyz",
                    "seq": 0,
                    "line": {
                        "type": "user",
                        "message": {"role": "user", "content": "hello warehouse"},
                        "uuid": "u1",
                        "timestamp": "2026-06-14T17:00:00.000Z",
                        "cwd": "/work",
                        "gitBranch": "main",
                        "version": "2.1.0",
                    },
                },
            }
        ],
    }
    summary = AgentSessionsDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: [batch],
        logger=FakeLogger(),
        now=lambda: datetime(2026, 6, 14, 18, tzinfo=UTC),
    ).sync()
    assert summary.events_written == 1
    rows = warehouse._query(
        "SELECT text, role, cwd FROM agent_session_events WHERE session_id = 'sess-xyz'"
    )
    assert rows == [("hello warehouse", "user", "/work")]
    view = warehouse._query(
        "SELECT first_prompt, event_count FROM clean_agent_sessions WHERE session_id = 'sess-xyz'"
    )
    assert view == [("hello warehouse", 1)]
