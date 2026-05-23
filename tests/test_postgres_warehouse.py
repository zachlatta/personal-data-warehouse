from __future__ import annotations

from datetime import UTC, datetime
import os
import uuid

import pytest
from dotenv import load_dotenv

from personal_data_warehouse.clickhouse import (
    APPLE_NOTE_ATTACHMENT_COLUMNS,
    APPLE_NOTE_COLUMNS,
    APPLE_NOTE_REVISION_COLUMNS,
    CALENDAR_EVENT_COLUMNS,
    CONTACT_CARD_COLUMNS,
    SLACK_ACCOUNT_IDENTITY_COLUMNS,
    SLACK_CONVERSATION_COLUMNS,
    SLACK_CONVERSATION_MEMBER_COLUMNS,
    SLACK_MESSAGE_COLUMNS,
    VOICE_MEMO_ENRICHMENT_COLUMNS,
    VOICE_MEMO_FILE_COLUMNS,
    VOICE_MEMO_TRANSCRIPTION_RUN_COLUMNS,
)
from personal_data_warehouse.postgres import (
    ARRAY_COLUMNS,
    FLOAT_COLUMNS,
    INTEGER_COLUMNS,
    POSTGRES_TABLES,
    TIMESTAMP_COLUMNS,
    PostgresWarehouse,
    _normalize_insert_value,
    _upsert_clause,
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


def _slack_member_row(*, conversation_id: str, user_id: str, sync_version: int = 1, is_deleted: int = 0, **overrides):
    now = datetime(2026, 5, 19, 12, tzinfo=UTC)
    row = _default_row(
        SLACK_CONVERSATION_MEMBER_COLUMNS,
        account="zrl",
        team_id="T1",
        conversation_id=conversation_id,
        user_id=user_id,
        is_deleted=is_deleted,
        synced_at=now,
        sync_version=sync_version,
    )
    row.update(overrides)
    return row


def _contact_card_row(*, card_id: str, display_name: str, sync_version: int, is_deleted: int = 0, **overrides):
    now = datetime(2026, 5, 19, 12, tzinfo=UTC)
    row = _default_row(
        CONTACT_CARD_COLUMNS,
        source="google_people",
        account="contact@example.test",
        source_kind="google_contacts",
        address_book_id="people/me",
        card_id=card_id,
        etag=f"etag-{card_id}",
        source_uid=f"source-{card_id}",
        display_name=display_name,
        primary_email=f"{card_id}@example.test",
        emails=[{"value": f"{card_id}@example.test"}],
        phones=[],
        addresses=[],
        organizations=[],
        urls=[],
        groups=[],
        dates={"birthdays": [], "events": []},
        photos=[],
        is_deleted=is_deleted,
        source_updated_at=now,
        synced_at=now,
        sync_version=sync_version,
        raw_json={"resourceName": card_id},
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


def test_apple_message_attachment_upsert_preserves_existing_storage_when_metadata_record_is_blank() -> None:
    clause = _upsert_clause("apple_message_attachments", POSTGRES_TABLES["apple_message_attachments"])

    assert (
        "\"storage_file_id\" = COALESCE(NULLIF(EXCLUDED.\"storage_file_id\", ''), "
        "\"apple_message_attachments\".\"storage_file_id\")"
    ) in clause
    assert (
        "\"storage_key\" = COALESCE(NULLIF(EXCLUDED.\"storage_key\", ''), "
        "\"apple_message_attachments\".\"storage_key\")"
    ) in clause
    assert (
        "\"content_sha256\" = COALESCE(NULLIF(EXCLUDED.\"content_sha256\", ''), "
        "\"apple_message_attachments\".\"content_sha256\")"
    ) in clause


def test_postgres_warehouse_can_create_all_runtime_tables_and_views(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()
    warehouse.ensure_calendar_tables()
    warehouse.ensure_contacts_tables()
    warehouse.ensure_apple_voice_memos_tables()
    warehouse.ensure_apple_notes_tables()
    warehouse.ensure_apple_messages_tables()
    warehouse.ensure_slack_tables()

    rows = warehouse._query(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name IN (
            'gmail_messages', 'calendar_events', 'slack_messages', 'apple_voice_memos_files',
            'apple_notes', 'apple_messages', 'contact_cards'
          )
        ORDER BY table_name
        """
    )

    assert [row[0] for row in rows] == [
        "apple_messages",
        "apple_notes",
        "apple_voice_memos_files",
        "calendar_events",
        "contact_cards",
        "gmail_messages",
        "slack_messages",
    ]


def test_postgres_warehouse_drops_removed_personal_finance_schema(warehouse: PostgresWarehouse) -> None:
    warehouse._command("CREATE TABLE finance_accounts (id text PRIMARY KEY)")
    warehouse._command("CREATE VIEW clean_finance_accounts AS SELECT id FROM finance_accounts")

    warehouse.ensure_tables()

    rows = warehouse._query(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name IN ('finance_accounts', 'clean_finance_accounts')
        ORDER BY table_name
        """
    )

    assert rows == []


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
    assert "slack_messages_text_trgm_live_idx" in index_names

    extension_rows = warehouse._query("SELECT extname FROM pg_extension WHERE extname = 'pg_trgm'")
    assert extension_rows == [("pg_trgm",)]


def test_postgres_contacts_tables_use_jsonb_without_changing_existing_raw_json(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_contacts_tables()
    warehouse.ensure_slack_tables()

    rows = warehouse._query(
        """
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND (
            (table_name = 'contact_cards' AND column_name IN ('emails', 'raw_json'))
            OR (table_name = 'slack_conversations' AND column_name = 'raw_json')
          )
        ORDER BY table_name, column_name
        """
    )

    assert rows == [
        ("contact_cards", "emails", "jsonb"),
        ("contact_cards", "raw_json", "jsonb"),
        ("slack_conversations", "raw_json", "text"),
    ]


def test_postgres_contact_cards_upsert_jsonb_and_clean_view(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_contacts_tables()

    warehouse.insert_contact_cards([
        _contact_card_row(card_id="people/c1", display_name="New Name", sync_version=20)
    ])
    warehouse.insert_contact_cards([
        _contact_card_row(card_id="people/c1", display_name="Old Name", sync_version=10)
    ])
    warehouse.insert_contact_cards([
        _contact_card_row(card_id="people/c2", display_name="Deleted", sync_version=20, is_deleted=1)
    ])

    rows = warehouse._query(
        """
        SELECT display_name, emails #>> '{0,value}', raw_json ->> 'resourceName'
        FROM clean_contacts
        ORDER BY card_id
        """
    )

    assert rows == [("New Name", "people/c1@example.test", "people/c1")]


def test_postgres_contact_card_edit_replaces_existing_active_card(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_contacts_tables()

    warehouse.insert_contact_cards([
        _contact_card_row(card_id="people/c1", display_name="Old Name", sync_version=10)
    ])
    warehouse.insert_contact_cards([
        _contact_card_row(
            card_id="people/c1",
            display_name="Edited Name",
            sync_version=20,
            primary_email="edited@example.test",
            emails=[{"value": "edited@example.test"}],
            raw_json={"resourceName": "people/c1", "etag": "edited"},
        )
    ])

    rows = warehouse._query(
        """
        SELECT display_name, primary_email, emails #>> '{0,value}', raw_json ->> 'etag'
        FROM clean_contacts
        WHERE card_id = 'people/c1'
        """
    )

    assert rows == [("Edited Name", "edited@example.test", "edited@example.test", "edited")]


def test_postgres_contact_card_incremental_delete_removes_card_from_clean_contacts(
    warehouse: PostgresWarehouse,
) -> None:
    warehouse.ensure_contacts_tables()

    warehouse.insert_contact_cards([
        _contact_card_row(card_id="people/c1", display_name="Active Name", sync_version=10)
    ])
    warehouse.insert_contact_cards([
        _contact_card_row(
            card_id="people/c1",
            display_name="",
            sync_version=20,
            is_deleted=1,
            primary_email="",
            emails=[],
            raw_json={"resourceName": "people/c1", "metadata": {"deleted": True}},
        )
    ])

    rows = warehouse._query(
        """
        SELECT is_deleted, raw_json #>> '{metadata,deleted}'
        FROM contact_cards
        WHERE card_id = 'people/c1'
        """
    )
    clean_rows = warehouse._query("SELECT count(*) FROM clean_contacts WHERE card_id = 'people/c1'")

    assert rows == [(1, "true")]
    assert clean_rows == [(0,)]


def test_postgres_mark_missing_contact_cards_deleted_tombstones_only_scope(warehouse: PostgresWarehouse) -> None:
    synced_at = datetime(2026, 5, 20, 12, tzinfo=UTC)
    warehouse.ensure_contacts_tables()
    warehouse.insert_contact_cards(
        [
            _contact_card_row(card_id="people/keep", display_name="Keep", sync_version=1),
            _contact_card_row(card_id="people/delete", display_name="Delete", sync_version=1),
            _contact_card_row(
                card_id="people/other",
                display_name="Other",
                sync_version=1,
                account="other@example.test",
            ),
        ]
    )

    deleted = warehouse.mark_missing_contact_cards_deleted(
        source="google_people",
        account="contact@example.test",
        source_kind="google_contacts",
        address_book_id="people/me",
        active_card_ids={"people/keep"},
        synced_at=synced_at,
    )

    rows = warehouse._query(
        """
        SELECT account, card_id, is_deleted, synced_at
        FROM contact_cards
        ORDER BY account, card_id
        """
    )

    assert deleted == 1
    assert rows == [
        ("contact@example.test", "people/delete", 1, synced_at),
        ("contact@example.test", "people/keep", 0, datetime(2026, 5, 19, 12, tzinfo=UTC)),
        ("other@example.test", "people/other", 0, datetime(2026, 5, 19, 12, tzinfo=UTC)),
    ]


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


def test_postgres_replace_slack_conversation_members_tombstones_missing_members(warehouse: PostgresWarehouse) -> None:
    old_sync = datetime(2026, 5, 18, 12, tzinfo=UTC)
    new_sync = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.ensure_slack_tables()
    warehouse.insert_slack_conversation_members(
        [
            _slack_member_row(conversation_id="G1", user_id="U1", synced_at=old_sync, sync_version=1),
            _slack_member_row(conversation_id="G1", user_id="U2", synced_at=old_sync, sync_version=1),
            _slack_member_row(conversation_id="G2", user_id="U9", synced_at=old_sync, sync_version=1),
        ]
    )

    warehouse.replace_slack_conversation_members(
        account="zrl",
        team_id="T1",
        conversation_id="G1",
        rows=[
            _slack_member_row(conversation_id="G1", user_id="U2", synced_at=new_sync, sync_version=2),
            _slack_member_row(conversation_id="G1", user_id="U3", synced_at=new_sync, sync_version=2),
        ],
        synced_at=new_sync,
        sync_version=2,
    )

    rows = warehouse._query(
        """
        SELECT conversation_id, user_id, is_deleted, synced_at, sync_version
        FROM slack_conversation_members
        ORDER BY conversation_id, user_id
        """
    )

    assert rows == [
        ("G1", "U1", 1, new_sync, 2),
        ("G1", "U2", 0, new_sync, 2),
        ("G1", "U3", 0, new_sync, 2),
        ("G2", "U9", 0, old_sync, 1),
    ]


def test_postgres_member_sync_candidates_prioritize_never_synced_private_channels(warehouse: PostgresWarehouse) -> None:
    old_sync = datetime(2026, 5, 18, 12, tzinfo=UTC)
    newer_sync = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.ensure_slack_tables()
    warehouse.insert_slack_conversations(
        [
            _slack_conversation_row(
                conversation_id="G1",
                conversation_type="private_channel",
                raw_json='{"id":"G1","name":"never-synced","is_private":true,"is_member":true}',
                num_members=5,
            ),
            _slack_conversation_row(
                conversation_id="G2",
                conversation_type="private_channel",
                raw_json='{"id":"G2","name":"already-synced","is_private":true,"is_member":true}',
                num_members=20,
            ),
            _slack_conversation_row(
                conversation_id="G3",
                conversation_type="private_channel",
                raw_json='{"id":"G3","name":"archived","is_private":true,"is_member":true,"is_archived":true}',
                is_archived=1,
            ),
            _slack_conversation_row(
                conversation_id="C1",
                conversation_type="public_channel",
                raw_json='{"id":"C1","name":"public","is_channel":true,"is_member":true}',
                num_members=100,
            ),
        ]
    )
    warehouse.insert_slack_sync_state(
        account="zrl",
        team_id="T1",
        object_type="conversation_members",
        object_id="G2",
        cursor_ts="",
        last_sync_type="members",
        status="ok",
        error="",
        updated_at=old_sync,
        sync_version=1,
    )
    warehouse.insert_slack_sync_state(
        account="zrl",
        team_id="T1",
        object_type="conversation_members",
        object_id="C1",
        cursor_ts="",
        last_sync_type="members",
        status="ok",
        error="",
        updated_at=newer_sync,
        sync_version=2,
    )

    payloads = warehouse.load_slack_member_sync_candidate_payloads(
        account="zrl",
        team_id="T1",
        conversation_types=("private_channel",),
        limit=10,
    )

    assert [payload["id"] for payload in payloads] == ["G1", "G2"]


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


def test_postgres_apple_notes_revision_history_keeps_latest_state(warehouse: PostgresWarehouse) -> None:
    older = datetime(2026, 5, 21, 12, tzinfo=UTC)
    newer = datetime(2026, 5, 21, 13, tzinfo=UTC)
    warehouse.ensure_apple_notes_tables()

    warehouse.insert_apple_notes(
        [
            _default_row(
                APPLE_NOTE_COLUMNS,
                account="zach@example.test",
                note_id="note-1",
                latest_revision_id="rev-old",
                title="old",
                modified_at=older,
                ingested_at=older,
                sync_version=1,
            )
        ]
    )
    warehouse.insert_apple_notes(
        [
            _default_row(
                APPLE_NOTE_COLUMNS,
                account="zach@example.test",
                note_id="note-1",
                latest_revision_id="rev-new",
                title="new",
                modified_at=newer,
                ingested_at=newer,
                sync_version=2,
            )
        ]
    )
    warehouse.insert_apple_note_revisions(
        [
            _default_row(
                APPLE_NOTE_REVISION_COLUMNS,
                account="zach@example.test",
                note_id="note-1",
                revision_id="rev-old",
                title="old",
                modified_at=older,
                exported_at=older,
                ingested_at=older,
                sync_version=1,
            ),
            _default_row(
                APPLE_NOTE_REVISION_COLUMNS,
                account="zach@example.test",
                note_id="note-1",
                revision_id="rev-new",
                title="new",
                modified_at=newer,
                exported_at=newer,
                ingested_at=newer,
                sync_version=2,
            ),
        ]
    )
    warehouse.insert_apple_note_attachments(
        [
            _default_row(
                APPLE_NOTE_ATTACHMENT_COLUMNS,
                account="zach@example.test",
                note_id="note-1",
                revision_id="rev-new",
                attachment_id="att-1",
                filename="photo.txt",
                content_sha256="att-sha",
                ingested_at=newer,
                sync_version=2,
            )
        ]
    )

    latest = warehouse._query("SELECT latest_revision_id, title FROM apple_notes WHERE note_id = %s", ("note-1",))
    revisions = warehouse._query("SELECT revision_id FROM apple_note_revisions WHERE note_id = %s ORDER BY revision_id", ("note-1",))
    attachments = warehouse._query("SELECT attachment_id FROM apple_note_attachments WHERE note_id = %s", ("note-1",))

    assert latest == [("rev-new", "new")]
    assert revisions == [("rev-new",), ("rev-old",)]
    assert attachments == [("att-1",)]


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
