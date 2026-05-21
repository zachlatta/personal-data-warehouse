from __future__ import annotations

from datetime import UTC, datetime
import os
import uuid

import pytest
from dotenv import load_dotenv

from personal_data_warehouse.clickhouse import (
    CALENDAR_EVENT_COLUMNS,
    SLACK_ACCOUNT_IDENTITY_COLUMNS,
    SLACK_CONVERSATION_COLUMNS,
    SLACK_MESSAGE_COLUMNS,
    VOICE_MEMO_ENRICHMENT_COLUMNS,
    VOICE_MEMO_FILE_COLUMNS,
    VOICE_MEMO_TRANSCRIPTION_RUN_COLUMNS,
)
from personal_data_warehouse.postgres import (
    ARRAY_COLUMNS,
    FLOAT_COLUMNS,
    INTEGER_COLUMNS,
    TIMESTAMP_COLUMNS,
    PostgresWarehouse,
    _normalize_insert_value,
)


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


def _message_row(*, message_id: str, subject: str, labels: list[str], sync_version: int, is_deleted: int = 0):
    now = datetime(2026, 5, 19, 12, tzinfo=UTC)
    return {
        "account": "zach@example.test",
        "message_id": message_id,
        "thread_id": "thread-1",
        "history_id": sync_version,
        "internal_date": now,
        "label_ids": labels,
        "is_deleted": is_deleted,
        "snippet": "snippet",
        "subject": subject,
        "from_address": "sender@example.test",
        "to_addresses": ["zach@example.test"],
        "cc_addresses": [],
        "bcc_addresses": [],
        "delivered_to": "zach@example.test",
        "rfc822_message_id": f"<{message_id}@example.test>",
        "date_header": "Tue, 19 May 2026 12:00:00 +0000",
        "size_estimate": 123,
        "body_text": "body text",
        "body_html": "",
        "body_markdown": "body markdown",
        "body_markdown_full": "body markdown full",
        "body_markdown_clean": "body markdown clean",
        "payload_json": '{"id":"%s"}' % message_id,
        "synced_at": now,
        "sync_version": sync_version,
    }


def _default_row(columns: tuple[str, ...], **overrides):
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    row = {}
    for column in columns:
        if column in ARRAY_COLUMNS:
            row[column] = []
        elif column in TIMESTAMP_COLUMNS:
            row[column] = epoch
        elif column in INTEGER_COLUMNS:
            row[column] = 0
        elif column in FLOAT_COLUMNS:
            row[column] = 0.0
        else:
            row[column] = ""
    row.update(overrides)
    return row


def _slack_conversation_row(*, conversation_id: str, conversation_type: str = "im", **overrides):
    now = datetime(2026, 5, 19, 12, tzinfo=UTC)
    row = _default_row(
        SLACK_CONVERSATION_COLUMNS,
        account="zrl",
        team_id="T1",
        conversation_id=conversation_id,
        conversation_type=conversation_type,
        name=f"{conversation_id}-name",
        is_im=1 if conversation_type == "im" else 0,
        is_mpim=1 if conversation_type == "mpim" else 0,
        is_private=1 if conversation_type == "private_channel" else 0,
        is_channel=1 if conversation_type == "public_channel" else 0,
        is_member=1,
        is_archived=0,
        raw_json=f'{{"id":"{conversation_id}","last_read":"0"}}',
        created_at=now,
        synced_at=now,
        sync_version=1,
    )
    row.update(overrides)
    return row


def _slack_message_row(
    *,
    conversation_id: str,
    message_ts: str,
    message_datetime: datetime,
    sync_version: int = 1,
    is_deleted: int = 0,
    **overrides,
):
    row = _default_row(
        SLACK_MESSAGE_COLUMNS,
        account="zrl",
        team_id="T1",
        conversation_id=conversation_id,
        message_ts=message_ts,
        message_datetime=message_datetime,
        thread_ts=message_ts,
        text=f"message {message_ts}",
        is_deleted=is_deleted,
        raw_json="{}",
        synced_at=message_datetime,
        sync_version=sync_version,
    )
    row.update(overrides)
    return row


def test_postgres_message_upsert_keeps_highest_sync_version(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()

    warehouse.insert_messages([_message_row(message_id="m1", subject="new", labels=["INBOX"], sync_version=20)])
    warehouse.insert_messages([_message_row(message_id="m1", subject="old", labels=["INBOX"], sync_version=10)])

    rows = warehouse._query("SELECT subject, sync_version FROM gmail_messages WHERE message_id = %s", ("m1",))

    assert rows == [("new", 20)]


def test_postgres_insert_normalizes_nul_text_values() -> None:
    assert _normalize_insert_value("before\x00after") == "before\\u0000after"
    assert _normalize_insert_value(["ok", "before\x00after", ("nested\x00value",)]) == [
        "ok",
        "before\\u0000after",
        ["nested\\u0000value"],
    ]


def test_postgres_warehouse_can_create_all_runtime_tables_and_views(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()
    warehouse.ensure_calendar_tables()
    warehouse.ensure_apple_voice_memos_tables()
    warehouse.ensure_slack_tables()
    warehouse.ensure_finance_tables()

    rows = warehouse._query(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name IN ('gmail_messages', 'calendar_events', 'slack_messages', 'apple_voice_memos_files', 'finance_accounts')
        ORDER BY table_name
        """
    )

    assert [row[0] for row in rows] == [
        "apple_voice_memos_files",
        "calendar_events",
        "finance_accounts",
        "gmail_messages",
        "slack_messages",
    ]


def test_postgres_slack_tables_create_recent_message_indexes(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_slack_tables()

    rows = warehouse._query(
        """
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = current_schema()
          AND tablename = 'slack_messages'
        """
    )

    index_names = {row[0] for row in rows}
    assert "slack_messages_recent_scope_time_idx" in index_names
    assert "slack_messages_recent_thread_time_idx" in index_names


def test_postgres_slack_tables_create_conversation_stats_table(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_slack_tables()

    rows = warehouse._query(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'slack_conversation_stats'
        ORDER BY ordinal_position
        """
    )

    assert [row[0] for row in rows] == [
        "account",
        "team_id",
        "conversation_id",
        "message_count",
        "latest_message_at",
        "updated_at",
    ]


def test_postgres_rebuild_slack_conversation_stats_backfills_live_messages(
    warehouse: PostgresWarehouse,
) -> None:
    older = datetime(2026, 5, 19, 11, tzinfo=UTC)
    newer = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.ensure_slack_tables()
    warehouse.insert_slack_messages(
        [
            _slack_message_row(conversation_id="C1", message_ts="1770000000.000001", message_datetime=older),
            _slack_message_row(conversation_id="C1", message_ts="1770000000.000002", message_datetime=newer),
            _slack_message_row(
                conversation_id="C2",
                message_ts="1770000000.000003",
                message_datetime=newer,
                is_deleted=1,
            ),
        ]
    )
    warehouse._command("TRUNCATE slack_conversation_stats")

    warehouse.rebuild_slack_conversation_stats()

    rows = warehouse._query(
        """
        SELECT conversation_id, message_count, latest_message_at
        FROM slack_conversation_stats
        ORDER BY conversation_id
        """
    )
    assert rows == [("C1", 2, newer)]


def test_postgres_ensure_slack_tables_backfills_empty_conversation_stats(
    warehouse: PostgresWarehouse,
) -> None:
    now = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.ensure_slack_tables()
    warehouse.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id="C1",
                message_ts="1770000000.000001",
                message_datetime=now,
            )
        ]
    )
    warehouse._command("TRUNCATE slack_conversation_stats")

    warehouse.ensure_slack_tables()

    rows = warehouse._query(
        "SELECT conversation_id, message_count, latest_message_at FROM slack_conversation_stats",
    )
    assert rows == [("C1", 1, now)]


def test_postgres_insert_slack_messages_refreshes_conversation_stats(
    warehouse: PostgresWarehouse,
) -> None:
    older = datetime(2026, 5, 19, 11, tzinfo=UTC)
    newer = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.ensure_slack_tables()

    warehouse.insert_slack_messages(
        [
            _slack_message_row(conversation_id="C1", message_ts="1770000000.000001", message_datetime=older),
            _slack_message_row(conversation_id="C1", message_ts="1770000000.000002", message_datetime=newer),
        ]
    )

    rows = warehouse._query(
        "SELECT message_count, latest_message_at FROM slack_conversation_stats WHERE conversation_id = %s",
        ("C1",),
    )
    assert rows == [(2, newer)]


def test_postgres_insert_slack_messages_updates_stats_without_full_conversation_recompute(
    warehouse: PostgresWarehouse,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.ensure_slack_tables()

    def fail_full_recompute(keys):
        raise AssertionError(f"unexpected full conversation recompute: {keys}")

    monkeypatch.setattr(warehouse, "_refresh_slack_conversation_stats_for_keys", fail_full_recompute)

    warehouse.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id="C1",
                message_ts="1770000000.000001",
                message_datetime=now,
            )
        ]
    )

    rows = warehouse._query(
        "SELECT message_count, latest_message_at FROM slack_conversation_stats WHERE conversation_id = %s",
        ("C1",),
    )
    assert rows == [(1, now)]


def test_postgres_slack_conversation_stats_follow_tombstones_and_ignore_stale_rows(
    warehouse: PostgresWarehouse,
) -> None:
    older = datetime(2026, 5, 19, 11, tzinfo=UTC)
    newer = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.ensure_slack_tables()
    live = _slack_message_row(
        conversation_id="C1",
        message_ts="1770000000.000001",
        message_datetime=older,
        sync_version=10,
    )

    warehouse.insert_slack_messages(
        [
            live,
            _slack_message_row(
                conversation_id="C1",
                message_ts="1770000000.000002",
                message_datetime=newer,
                sync_version=10,
            ),
        ]
    )
    warehouse.insert_slack_messages([{**live, "is_deleted": 1, "sync_version": 20}])
    warehouse.insert_slack_messages([{**live, "is_deleted": 0, "sync_version": 5}])

    rows = warehouse._query(
        "SELECT message_count, latest_message_at FROM slack_conversation_stats WHERE conversation_id = %s",
        ("C1",),
    )
    assert rows == [(1, newer)]


def test_postgres_slack_conversation_loader_uses_stats_for_zero_message_filter(
    warehouse: PostgresWarehouse,
) -> None:
    now = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.ensure_slack_tables()
    warehouse.insert_slack_conversations(
        [
            _slack_conversation_row(conversation_id="C-empty", raw_json='{"id":"C-empty"}'),
            _slack_conversation_row(conversation_id="C-with-message", raw_json='{"id":"C-with-message"}'),
        ]
    )
    warehouse.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id="C-with-message",
                message_ts="1770000000.000001",
                message_datetime=now,
            )
        ]
    )

    payloads = warehouse.load_slack_conversation_payloads(
        account="zrl",
        team_id="T1",
        zero_messages_only=True,
    )

    assert payloads == [{"id": "C-empty"}]


def test_postgres_slack_conversation_loader_query_uses_stats_not_message_grouping(
    warehouse: PostgresWarehouse,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, str] = {}

    def fake_query(sql, params=None):
        captured["sql"] = sql
        return []

    monkeypatch.setattr(warehouse, "_query", fake_query)

    warehouse.load_slack_conversation_payloads(account="zrl", team_id="T1")

    assert "slack_conversation_stats AS m" in captured["sql"]
    assert "FROM slack_messages" not in captured["sql"]
    assert "GROUP BY account, team_id, conversation_id" not in captured["sql"]


def test_postgres_slack_read_state_candidates_use_stats_latest_message_at(
    warehouse: PostgresWarehouse,
) -> None:
    recent = datetime.now(tz=UTC)
    old = datetime(2026, 1, 1, 12, tzinfo=UTC)
    warehouse.ensure_slack_tables()
    warehouse.insert_slack_conversations(
        [
            _slack_conversation_row(conversation_id="C-recent", raw_json='{"id":"C-recent","last_read":"0"}'),
            _slack_conversation_row(conversation_id="C-old", raw_json='{"id":"C-old","last_read":"0"}'),
            _slack_conversation_row(conversation_id="C-empty", raw_json='{"id":"C-empty","last_read":"0"}'),
        ]
    )
    warehouse.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id="C-recent",
                message_ts="1770000000.000001",
                message_datetime=recent,
            ),
            _slack_message_row(
                conversation_id="C-old",
                message_ts="1760000000.000001",
                message_datetime=old,
            ),
        ]
    )

    payloads = warehouse.load_slack_read_state_candidate_payloads(account="zrl", team_id="T1")

    assert payloads == [{"id": "C-recent", "last_read": "0"}]


def test_postgres_slack_read_state_candidate_query_uses_stats_not_message_grouping(
    warehouse: PostgresWarehouse,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, str] = {}

    def fake_query(sql, params=None):
        captured["sql"] = sql
        return []

    monkeypatch.setattr(warehouse, "_query", fake_query)

    warehouse.load_slack_read_state_candidate_payloads(account="zrl", team_id="T1")

    assert "slack_conversation_stats AS m" in captured["sql"]
    assert "FROM slack_messages" not in captured["sql"]
    assert "GROUP BY account, team_id, conversation_id" not in captured["sql"]


def test_postgres_slack_account_state_query_does_not_materialize_recent_messages(warehouse: PostgresWarehouse) -> None:
    sql = warehouse._slack_account_state_items_select_sql()

    assert "recent_messages AS NOT MATERIALIZED" in sql
    assert "current_conversations AS NOT MATERIALIZED" in sql


def test_postgres_message_upsert_preserves_latest_tombstone(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()

    warehouse.insert_messages([_message_row(message_id="m1", subject="live", labels=["INBOX"], sync_version=10)])
    warehouse.insert_messages([_message_row(message_id="m1", subject="deleted", labels=[], sync_version=20, is_deleted=1)])

    assert warehouse.existing_message_ids(account="zach@example.test", message_ids=["m1"]) == set()
    rows = warehouse._query("SELECT subject, is_deleted, sync_version FROM gmail_messages WHERE message_id = %s", ("m1",))
    assert rows == [("deleted", 1, 20)]


def test_postgres_gmail_clean_inbox_view_matches_current_state(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()

    warehouse.insert_messages(
        [
            _message_row(message_id="m1", subject="older", labels=["INBOX"], sync_version=10),
            {
                **_message_row(message_id="m2", subject="newer", labels=["INBOX", "UNREAD"], sync_version=11),
                "internal_date": datetime(2026, 5, 19, 13, tzinfo=UTC),
            },
        ]
    )

    rows = warehouse._query(
        """
        SELECT thread_id, subject, state, unread_count, important_count, thread_messages_json
        FROM clean_gmail_inbox
        """
    )

    assert len(rows) == 1
    assert rows[0][0:5] == ("thread-1", "newer", "unread", 1, 0)
    assert "body_markdown_clean" in rows[0][5]


def test_postgres_gmail_clean_inbox_preview_uses_clickhouse_byte_prefix(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()
    preview = ("a" * 998) + "€" + "after"
    expected = preview.encode("utf-8")[:1000].decode("utf-8", errors="ignore")
    row = _message_row(message_id="m1", subject="subject", labels=["INBOX"], sync_version=10)
    row["body_markdown_clean"] = preview

    warehouse.insert_messages([row])

    rows = warehouse._query("SELECT latest_preview FROM clean_gmail_inbox")

    assert rows == [(expected,)]


def test_postgres_gmail_clean_inbox_ties_latest_message_by_lowest_message_id(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()
    lower = _message_row(message_id="a", subject="lower", labels=["INBOX"], sync_version=10)
    higher = _message_row(message_id="b", subject="higher", labels=["INBOX"], sync_version=11)

    warehouse.insert_messages([higher, lower])

    rows = warehouse._query("SELECT subject FROM clean_gmail_inbox")

    assert rows == [("lower",)]


def test_postgres_calendar_transcript_views_use_latest_clickhouse_grouping(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_calendar_tables()
    warehouse.ensure_apple_voice_memos_tables()
    older = datetime(2026, 5, 19, 11, tzinfo=UTC)
    newer = datetime(2026, 5, 19, 12, tzinfo=UTC)

    warehouse.insert_calendar_events(
        [
            _default_row(
                CALENDAR_EVENT_COLUMNS,
                account="calendar-a",
                calendar_id="primary",
                event_id="event-1",
                summary="old calendar",
                start_at=older,
                end_at=older,
                synced_at=older,
                sync_version=1,
            ),
            _default_row(
                CALENDAR_EVENT_COLUMNS,
                account="calendar-b",
                calendar_id="primary",
                event_id="event-1",
                summary="new calendar",
                start_at=newer,
                end_at=newer,
                synced_at=newer,
                sync_version=2,
            ),
        ]
    )
    warehouse.insert_apple_voice_memos_files(
        [
            _default_row(
                VOICE_MEMO_FILE_COLUMNS,
                account="recording-account",
                recording_id="rec-1",
                title="recording title",
                recorded_at=older,
                created_at=older,
                sync_version=1,
            )
        ]
    )
    warehouse.insert_apple_voice_memos_enrichments(
        [
            _default_row(
                VOICE_MEMO_ENRICHMENT_COLUMNS,
                account="recording-account",
                recording_id="rec-1",
                content_sha256="sha-old",
                provider="agent",
                model="old",
                prompt_version="v1",
                status="completed",
                calendar_event_id="event-1",
                title="old title",
                created_at=older,
                sync_version=1,
            ),
            _default_row(
                VOICE_MEMO_ENRICHMENT_COLUMNS,
                account="recording-account",
                recording_id="rec-1",
                content_sha256="sha-new",
                provider="agent",
                model="new",
                prompt_version="v1",
                status="completed",
                calendar_event_id="event-1",
                title="new title",
                created_at=newer,
                sync_version=2,
            ),
        ]
    )

    rows = warehouse._query(
        """
        SELECT calendar_account, calendar_title, recording_id, title, created_at
        FROM clean_calendar_with_transcripts
        """
    )

    assert rows == [("calendar-b", "new calendar", "rec-1", "new title", newer)]


def test_postgres_voice_memo_ensure_can_skip_runtime_content_hash_backfill(warehouse: PostgresWarehouse) -> None:
    now = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.ensure_apple_voice_memos_tables(backfill_content_hashes=False)
    warehouse.insert_apple_voice_memos_files(
        [
            _default_row(
                VOICE_MEMO_FILE_COLUMNS,
                account="zach@example.test",
                recording_id="rec-1",
                content_sha256="audio-hash",
                created_at=now,
                sync_version=1,
            )
        ]
    )
    warehouse.insert_apple_voice_memos_transcription_runs(
        [
            _default_row(
                VOICE_MEMO_TRANSCRIPTION_RUN_COLUMNS,
                account="zach@example.test",
                recording_id="rec-1",
                provider="assemblyai",
                content_sha256="",
                requested_at=now,
                sync_version=1,
            )
        ]
    )

    warehouse.ensure_apple_voice_memos_tables(backfill_content_hashes=False)
    assert warehouse._query("SELECT content_sha256 FROM apple_voice_memos_transcription_runs") == [("",)]

    warehouse.ensure_apple_voice_memos_tables()
    assert warehouse._query("SELECT content_sha256 FROM apple_voice_memos_transcription_runs") == [("audio-hash",)]


def test_postgres_slack_account_state_uses_empty_actor_for_missing_user(warehouse: PostgresWarehouse) -> None:
    now = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.ensure_slack_tables()
    warehouse.insert_slack_account_identities(
        [
            _default_row(
                SLACK_ACCOUNT_IDENTITY_COLUMNS,
                account="zrl",
                team_id="T1",
                user_id="U_SELF",
                synced_at=now,
                sync_version=1,
            )
        ]
    )
    warehouse.insert_slack_conversations(
        [
            _default_row(
                SLACK_CONVERSATION_COLUMNS,
                account="zrl",
                team_id="T1",
                conversation_id="C1",
                conversation_type="mpim",
                name="mpdm-test",
                is_mpim=1,
                is_member=1,
                raw_json='{"last_read":"0"}',
                created_at=now,
                synced_at=now,
                sync_version=1,
            )
        ]
    )
    warehouse.insert_slack_messages(
        [
            _default_row(
                SLACK_MESSAGE_COLUMNS,
                account="zrl",
                team_id="T1",
                conversation_id="C1",
                message_ts="1770000000.000001",
                message_datetime=now,
                thread_ts="1770000000.000001",
                user_id="U_MISSING",
                text="hello",
                raw_json="{}",
                synced_at=now,
                sync_version=1,
            )
        ]
    )

    warehouse.refresh_slack_account_state_items(account="zrl", team_id="T1", synced_at=now)

    assert warehouse._query("SELECT actor_name FROM slack_account_state_item_rows WHERE is_deleted = 0") == [("",)]


def test_postgres_load_untranscribed_voice_memos_uses_valid_retryable_error_sql(
    warehouse: PostgresWarehouse,
) -> None:
    now = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.ensure_apple_voice_memos_tables(backfill_content_hashes=False)
    warehouse.insert_apple_voice_memos_files(
        [
            _default_row(
                VOICE_MEMO_FILE_COLUMNS,
                account="zach@example.test",
                recording_id="rec-1",
                filename="memo.m4a",
                content_type="audio/mp4",
                size_bytes=123,
                content_sha256="audio-hash",
                recorded_at=now,
                created_at=now,
                sync_version=1,
            )
        ]
    )

    rows = warehouse.load_untranscribed_apple_voice_memos_files(provider="assemblyai", limit=1)

    assert [row["recording_id"] for row in rows] == ["rec-1"]


def test_postgres_sync_state_round_trips_latest_update(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()
    older = datetime(2026, 5, 19, 11, tzinfo=UTC)
    newer = datetime(2026, 5, 19, 12, tzinfo=UTC)

    warehouse.insert_sync_state(
        account="zach@example.test",
        last_history_id=1,
        last_sync_type="full",
        status="ok",
        error="",
        updated_at=newer,
    )
    warehouse.insert_sync_state(
        account="zach@example.test",
        last_history_id=0,
        last_sync_type="full",
        status="old",
        error="",
        updated_at=older,
    )

    state = warehouse.load_sync_state()["zach@example.test"]
    assert state.last_history_id == 1
    assert state.status == "ok"
