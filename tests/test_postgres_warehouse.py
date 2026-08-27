from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from datetime import UTC, datetime, timedelta

import psycopg2
import pytest
from dotenv import load_dotenv

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.slack_sync import SlackApiCallError, SlackSyncRunner

from personal_data_warehouse.schema import (
    APPLE_MESSAGE_ATTACHMENT_COLUMNS,
    APPLE_NOTE_ATTACHMENT_COLUMNS,
    APPLE_NOTE_REVISION_COLUMNS,
    WHATSAPP_MEDIA_ITEM_COLUMNS,
    ALICE_VOICE_RECORDING_COLUMNS,
    APPLE_MESSAGE_COLUMNS,
    APPLE_NOTE_COLUMNS,
    CALENDAR_EVENT_COLUMNS,
    CONTACT_CARD_COLUMNS,
    SLACK_ACCOUNT_IDENTITY_COLUMNS,
    SLACK_CONVERSATION_COLUMNS,
    SLACK_CONVERSATION_MEMBER_COLUMNS,
    SLACK_MESSAGE_COLUMNS,
    VOICE_MEMO_ENRICHMENT_COLUMNS,
    VOICE_MEMO_FILE_COLUMNS,
    VOICE_MEMO_TRANSCRIPTION_RUN_COLUMNS,)
from personal_data_warehouse.relations import relation
from personal_data_warehouse.timeline import TimelineSyncEngine, adapter_by_name
from personal_data_warehouse.postgres import (
    ARRAY_COLUMNS,
    ATTACHMENT_BACKFILL_STATE_COLUMNS,
    ATTACHMENT_COLUMNS,
    FLOAT_COLUMNS,
    INTEGER_COLUMNS,
    POSTGRES_TABLES,
    SEARCH_SCHEMA_REFRESH_LOCK_ID,
    SLACK_ACCOUNT_STATE_REFRESH_LOCK_ID,
    SEARCH_TEXT_LOW_VOLUME_ADAPTERS,
    SEARCH_TEXT_PREVIEW_CHARS,
    SEARCH_TEXT_SOURCE_FLOOR,
    TIMESTAMP_COLUMNS,
    PostgresWarehouse,
    _dedupe_conflict_rows,
    _identifier,
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
    schema = make_test_schema()
    wh = PostgresWarehouse(_postgres_url(), schema=schema)
    try:
        yield wh
    finally:
        cleanup_test_warehouse(wh)


def _physical_relation(warehouse: PostgresWarehouse, logical_name: str):
    return relation(logical_name).with_namespace(warehouse.schema_namespace)


def _index_names(warehouse: PostgresWarehouse, logical_name: str) -> set[str]:
    rel = _physical_relation(warehouse, logical_name)
    return {
        row[0]
        for row in warehouse._query(
            "SELECT indexname FROM pg_indexes WHERE schemaname = %s AND tablename = %s",
            (rel.schema, rel.name),
        )
    }


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


def test_search_view_refresh_takes_advisory_lock(monkeypatch) -> None:
    warehouse = object.__new__(PostgresWarehouse)
    warehouse._schema = "public"
    commands: list[tuple[str, tuple | None]] = []

    monkeypatch.setattr(warehouse, "_command", lambda sql, params=None: commands.append((sql, params)))
    monkeypatch.setattr(warehouse, "_relation_exists", lambda _table: False)
    monkeypatch.setattr(warehouse, "_query", lambda sql, params=None: [])
    monkeypatch.setattr(warehouse, "_raw_command", lambda sql, params=None: commands.append((sql, params)))

    warehouse._ensure_search_views_if_possible()

    assert commands[0] == ("SELECT pg_advisory_lock(%s)", (SEARCH_SCHEMA_REFRESH_LOCK_ID,))
    assert commands[-1] == ("SELECT pg_advisory_unlock(%s)", (SEARCH_SCHEMA_REFRESH_LOCK_ID,))


def test_search_view_refresh_releases_advisory_lock_on_error(monkeypatch) -> None:
    warehouse = object.__new__(PostgresWarehouse)
    commands: list[tuple[str, tuple | None]] = []

    def command(sql, params=None):
        commands.append((sql, params))
        if "pg_advisory_lock" in sql:
            return
        raise RuntimeError("ddl failed")

    monkeypatch.setattr(warehouse, "_command", command)
    monkeypatch.setattr(warehouse, "_relation_exists", lambda _table: True)
    monkeypatch.setattr(warehouse, "_ensure_indexes", lambda tables: None)

    with pytest.raises(RuntimeError, match="ddl failed"):
        warehouse._ensure_search_views_if_possible()

    assert commands[-1] == ("SELECT pg_advisory_unlock(%s)", (SEARCH_SCHEMA_REFRESH_LOCK_ID,))


def test_search_schema_rebuild_is_skipped_when_unchanged(warehouse: PostgresWarehouse) -> None:
    _ensure_all_table_groups(warehouse)
    assert warehouse._search_text_function_exists()

    issued: list[str] = []
    original_command = warehouse._command

    def spy(sql, params=None):
        issued.append(sql)
        return original_command(sql, params)

    warehouse._command = spy
    try:
        warehouse._ensure_search_views_if_possible()
    finally:
        warehouse._command = original_command

    assert not any("CREATE OR REPLACE FUNCTION @search_text(" in sql for sql in issued), (
        "search_text() was recompiled even though its DDL was unchanged"
    )

    warehouse._command(f"DELETE FROM {warehouse.sql_relation('search_schema_state')} WHERE id = 1")
    issued.clear()
    warehouse._command = spy
    try:
        warehouse._ensure_search_views_if_possible()
    finally:
        warehouse._command = original_command

    assert any("CREATE OR REPLACE FUNCTION @search_text(" in sql for sql in issued), (
        "search_text() was not rebuilt after the signature marker was cleared"
    )


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
        nicknames=[],
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

    rows = warehouse._query("SELECT subject, sync_version FROM @gmail_messages WHERE message_id = %s", ("m1",))

    assert rows == [("new", 20)]


def test_dedupe_conflict_rows_collapses_duplicate_primary_keys() -> None:
    columns = SLACK_MESSAGE_COLUMNS
    spec = POSTGRES_TABLES["slack_messages"]

    def _row(*, message_ts: str, sync_version: int, text: str) -> tuple:
        values = _default_row(
            columns,
            account="zrl",
            team_id="T1",
            conversation_id="C1",
            message_ts=message_ts,
            sync_version=sync_version,
            text=text,
        )
        return tuple(values[column] for column in columns)

    # Same primary key four times in one batch; the version guard keeps the
    # highest sync_version and, on ties, the last occurrence.
    rows = [
        _row(message_ts="100.1", sync_version=1, text="first"),
        _row(message_ts="100.1", sync_version=3, text="high-version"),
        _row(message_ts="100.1", sync_version=3, text="tie-last-wins"),
        _row(message_ts="100.1", sync_version=2, text="stale"),
        _row(message_ts="200.2", sync_version=1, text="distinct-key"),
    ]

    deduped = _dedupe_conflict_rows(list(rows), columns, spec)

    ts_index = columns.index("message_ts")
    text_index = columns.index("text")
    winners = {row[ts_index]: row[text_index] for row in deduped}
    assert winners == {"100.1": "tie-last-wins", "200.2": "distinct-key"}


def test_dedupe_conflict_rows_preserves_storage_columns_from_losing_rows() -> None:
    columns = ATTACHMENT_COLUMNS
    spec = POSTGRES_TABLES["gmail_attachments"]

    def _row(*, sync_version: int, **overrides) -> tuple:
        values = _default_row(
            columns,
            account="zach@example.test",
            message_id="m1",
            part_id="p1",
            sync_version=sync_version,
            **overrides,
        )
        return tuple(values[column] for column in columns)

    rows = [
        _row(sync_version=1, content_sha256="sha-1", storage_backend="google_drive", storage_file_id="file-1"),
        _row(sync_version=2),
    ]

    deduped = _dedupe_conflict_rows(rows, columns, spec, table="gmail_attachments")

    assert len(deduped) == 1
    winner = deduped[0]
    assert winner[columns.index("sync_version")] == 2
    assert winner[columns.index("storage_backend")] == "google_drive"
    assert winner[columns.index("storage_file_id")] == "file-1"


def test_dedupe_conflict_rows_leaves_unique_batch_untouched() -> None:
    columns = SLACK_MESSAGE_COLUMNS
    spec = POSTGRES_TABLES["slack_messages"]
    rows = [
        tuple(_default_row(columns, conversation_id="C1", message_ts="1.0", sync_version=1)[c] for c in columns),
        tuple(_default_row(columns, conversation_id="C1", message_ts="2.0", sync_version=1)[c] for c in columns),
    ]

    assert _dedupe_conflict_rows(rows, columns, spec) is rows


def test_postgres_insert_slack_messages_dedupes_duplicate_keys_in_one_batch(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_slack_tables()
    message_datetime = datetime(2026, 5, 19, 12, tzinfo=UTC)

    # Two rows with the SAME (account, team_id, conversation_id, message_ts) in a
    # single insert batch — the exact shape that produced "ON CONFLICT DO UPDATE
    # command cannot affect row a second time" in prod. Must not raise, and the
    # higher sync_version must win.
    warehouse.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id="C1", message_ts="100.1", message_datetime=message_datetime, sync_version=1, text="old"
            ),
            _slack_message_row(
                conversation_id="C1", message_ts="100.1", message_datetime=message_datetime, sync_version=2, text="new"
            ),
        ]
    )

    rows = warehouse._query(
        "SELECT text, sync_version FROM @slack_messages WHERE conversation_id = %s AND message_ts = %s",
        ("C1", "100.1"),
    )
    assert rows == [("new", 2)]


def _gmail_attachment_payload(*, message_id: str) -> str:
    return json.dumps(
        {
            "id": message_id,
            "payload": {
                "parts": [
                    {
                        "partId": "1",
                        "filename": "report.pdf",
                        "mimeType": "application/pdf",
                        "body": {"attachmentId": f"att-{message_id}", "size": 1024},
                    }
                ]
            },
        }
    )


def test_postgres_backfill_candidates_include_storage_pending(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()
    account = "zach@example.test"
    now = datetime(2026, 5, 1, tzinfo=UTC)

    def insert_message(message_id: str) -> None:
        row = _message_row(message_id=message_id, subject="s", labels=["INBOX"], sync_version=1)
        row["payload_json"] = _gmail_attachment_payload(message_id=message_id)
        warehouse.insert_messages([row])

    def mark_enriched(message_id: str) -> None:
        warehouse.insert_attachment_backfill_state(
            [
                _default_row(
                    ATTACHMENT_BACKFILL_STATE_COLUMNS,
                    account=account,
                    message_id=message_id,
                    status="ok",
                    updated_at=now,
                    sync_version=1,
                )
            ]
        )

    def insert_attachment(message_id: str, *, size: int, storage_status: str) -> None:
        warehouse.insert_attachments(
            [
                _default_row(
                    ATTACHMENT_COLUMNS,
                    account=account,
                    message_id=message_id,
                    part_id="1",
                    filename="report.pdf",
                    attachment_id=f"att-{message_id}",
                    size=size,
                    storage_status=storage_status,
                    is_deleted=0,
                    synced_at=now,
                    sync_version=1,
                )
            ]
        )

    storage_max_bytes = 25 * 1024

    # Enriched before Drive storage shipped: blob never stored -> must be reclaimed.
    insert_message("m_pending")
    mark_enriched("m_pending")
    insert_attachment("m_pending", size=1024, storage_status="")
    # Enriched and already stored -> must NOT be reselected (avoids re-upload loop).
    insert_message("m_stored")
    mark_enriched("m_stored")
    insert_attachment("m_stored", size=1024, storage_status="stored")
    # Enriched, unstored, but larger than max bytes -> never storable, must NOT loop forever.
    insert_message("m_toolarge")
    mark_enriched("m_toolarge")
    insert_attachment("m_toolarge", size=10 * 1024 * 1024, storage_status="")
    # Brand-new message, never backfilled -> normal candidate either way.
    insert_message("m_new")

    without_storage = warehouse.load_attachment_backfill_candidate_messages(
        account=account,
        limit=10,
    )
    # Reproduces the stall: backfilled-but-unstored history is invisible to the text-only gate.
    assert {message["id"] for message in without_storage} == {"m_new"}

    with_storage = warehouse.load_attachment_backfill_candidate_messages(
        account=account,
        limit=10,
        include_storage_pending=True,
        storage_max_bytes=storage_max_bytes,
    )
    # Storage-pending history is reclaimed without dropping normal AI candidates,
    # and already-stored / too-large attachments stay excluded.
    assert {message["id"] for message in with_storage} == {"m_new", "m_pending"}


def test_postgres_attachment_enrichment_candidates_select_stored_images(warehouse: PostgresWarehouse) -> None:
    from personal_data_warehouse.agent_runner import AgentRunResult, agent_run_row
    from personal_data_warehouse.file_attachment_enrichment import (
        AGENT_ATTACHMENT_PROMPT_VERSION,
        AGENT_ATTACHMENT_TASK_TYPE,
        GMAIL_SOURCE,
        has_file_enrichment_candidate,
        load_file_enrichment_candidates,
    )
    from personal_data_warehouse.schema import ATTACHMENT_ENRICHMENT_COLUMNS

    warehouse.ensure_tables()
    warehouse.ensure_agent_tables()
    account = "zach@example.test"
    now = datetime(2026, 6, 1, tzinfo=UTC)
    provider, current_model, version = "agent_codex", "current-model", AGENT_ATTACHMENT_PROMPT_VERSION

    def insert_attachment(message_id: str, *, sha: str, filename: str, mime_type: str, **overrides) -> None:
        defaults = dict(
            account=account,
            message_id=message_id,
            part_id="1",
            filename=filename,
            mime_type=mime_type,
            content_sha256=sha,
            size=2048,
            storage_backend="google_drive",
            storage_key=f"gmail-attachments/library/{sha}",
            storage_file_id=f"drive-{sha}",
            storage_status="stored",
            internal_date=now,
            is_deleted=0,
            synced_at=now,
            sync_version=1,
        )
        defaults.update(overrides)
        warehouse.insert_attachments([_default_row(ATTACHMENT_COLUMNS, **defaults)])

    def insert_enrichment(sha: str, *, ai_provider: str, ai_model: str, ai_prompt_version: str, status: str) -> None:
        warehouse.insert_attachment_enrichments(
            [
                _default_row(
                    ATTACHMENT_ENRICHMENT_COLUMNS,
                    content_sha256=sha,
                    ai_provider=ai_provider,
                    ai_model=ai_model,
                    ai_prompt_version=ai_prompt_version,
                    text_extraction_status=status,
                    updated_at=now,
                    sync_version=1,
                )
            ]
        )

    # Pending image attachment: stored blob, deterministic 'unsupported' row -> candidate.
    insert_attachment("m1", sha="sha-pending", filename="logo.png", mime_type="image/png")
    insert_enrichment("sha-pending", ai_provider="", ai_model="", ai_prompt_version="", status="unsupported")
    # Already agent-enriched -> excluded.
    insert_attachment("m2", sha="sha-done", filename="chart.png", mime_type="image/png")
    insert_enrichment(
        "sha-done",
        ai_provider=provider,
        ai_model="previous-model",
        ai_prompt_version=version,
        status="agent_ok",
    )
    # Plain text attachment -> never a vision candidate.
    insert_attachment("m3", sha="sha-text", filename="notes.txt", mime_type="text/plain")
    # Scanned PDF whose deterministic extraction was empty -> candidate.
    insert_attachment("m4", sha="sha-pdf", filename="scan.pdf", mime_type="application/pdf")
    insert_enrichment("sha-pdf", ai_provider="", ai_model="", ai_prompt_version="", status="empty")
    # Text PDF (deterministic ok) -> excluded.
    insert_attachment("m5", sha="sha-pdf-ok", filename="report.pdf", mime_type="application/pdf")
    insert_enrichment("sha-pdf-ok", ai_provider="", ai_model="", ai_prompt_version="", status="ok")
    # Image not yet in the object store -> excluded.
    insert_attachment("m6", sha="sha-unstored", filename="photo.jpg", mime_type="image/jpeg", storage_status="")
    # Image whose agent runs keep failing -> excluded after the attempt budget.
    insert_attachment("m7", sha="sha-flaky", filename="flaky.png", mime_type="image/png")
    for attempt in range(3):
        warehouse.insert_agent_runs(
            [
                agent_run_row(
                    AgentRunResult(
                        run_id=f"run-{attempt}",
                        provider="codex",
                        model="",
                        task_type=AGENT_ATTACHMENT_TASK_TYPE,
                        subject_id="sha-flaky",
                        prompt_version=version,
                        input_sha256="x",
                        status="error",
                        final_output_json={},
                        error="boom",
                        exit_code=1,
                        started_at=now,
                        completed_at=now + timedelta(seconds=attempt + 1),
                        events=[],
                    )
                )
            ]
        )

    # This test isolates the attempt-budget dimension, so disable the rolling
    # error window (error_window_days=0) and count every historical failure.
    # The flaky runs are stamped at the fixed 2026-06-01 base time, which a
    # real-clock window would otherwise age out. Windowing has its own test.
    assert has_file_enrichment_candidate(
        warehouse,
        source=GMAIL_SOURCE,
        provider=provider,
        prompt_version=version,
        max_error_attempts=3,
        error_window_days=0,
    )

    candidates = load_file_enrichment_candidates(
        warehouse,
        source=GMAIL_SOURCE,
        provider=provider,
        prompt_version=version,
        limit=10,
        max_error_attempts=3,
        error_window_days=0,
    )

    assert {candidate["content_sha256"] for candidate in candidates} == {"sha-pending", "sha-pdf"}
    by_sha = {candidate["content_sha256"]: candidate for candidate in candidates}
    assert by_sha["sha-pending"]["source_status"] == "unsupported"
    assert by_sha["sha-pending"]["storage_file_id"] == "drive-sha-pending"
    assert by_sha["sha-pdf"]["source_status"] == "empty"

    # Raising the attempt budget brings the flaky attachment back.
    retried = load_file_enrichment_candidates(
        warehouse,
        source=GMAIL_SOURCE,
        provider=provider,
        prompt_version=version,
        limit=10,
        max_error_attempts=5,
        error_window_days=0,
    )
    assert "sha-flaky" in {candidate["content_sha256"] for candidate in retried}

    insert_enrichment(
        "sha-pending", ai_provider=provider, ai_model=current_model, ai_prompt_version=version, status="agent_ok"
    )
    insert_enrichment(
        "sha-pdf", ai_provider=provider, ai_model=current_model, ai_prompt_version=version, status="agent_ok"
    )
    assert not has_file_enrichment_candidate(
        warehouse,
        source=GMAIL_SOURCE,
        provider=provider,
        prompt_version=version,
        max_error_attempts=3,
        error_window_days=0,
    )
    assert has_file_enrichment_candidate(
        warehouse,
        source=GMAIL_SOURCE,
        provider=provider,
        prompt_version=version,
        max_error_attempts=5,
        error_window_days=0,
    )


def test_postgres_attachment_enrichment_error_window_ages_out_stale_failures(warehouse: PostgresWarehouse) -> None:
    """Stale failures (e.g. attempts exhausted on a since-fixed bug) age out of
    the rolling window so the attachment can be retried, while recent failures
    still count against the per-attachment attempt budget."""
    from datetime import datetime as _datetime

    from personal_data_warehouse.agent_runner import AgentRunResult, agent_run_row
    from personal_data_warehouse.file_attachment_enrichment import (
        AGENT_ATTACHMENT_PROMPT_VERSION,
        AGENT_ATTACHMENT_TASK_TYPE,
        GMAIL_SOURCE,
        load_file_enrichment_candidates,
    )
    from personal_data_warehouse.schema import ATTACHMENT_ENRICHMENT_COLUMNS

    warehouse.ensure_tables()
    warehouse.ensure_agent_tables()
    account = "zach@example.test"
    base = datetime(2026, 6, 1, tzinfo=UTC)
    provider, model, version = "agent_codex", "", AGENT_ATTACHMENT_PROMPT_VERSION
    # Failure timestamps are measured against the database's real now(), so use a
    # real-clock anchor rather than the fixed base time the rows are stamped with.
    real_now = _datetime.now(tz=UTC)

    def insert_attachment(message_id: str, *, sha: str) -> None:
        warehouse.insert_attachments(
            [
                _default_row(
                    ATTACHMENT_COLUMNS,
                    account=account,
                    message_id=message_id,
                    part_id="1",
                    filename=f"{sha}.png",
                    mime_type="image/png",
                    content_sha256=sha,
                    size=2048,
                    storage_backend="google_drive",
                    storage_key=f"gmail-attachments/library/{sha}",
                    storage_file_id=f"drive-{sha}",
                    storage_status="stored",
                    internal_date=base,
                    is_deleted=0,
                    synced_at=base,
                    sync_version=1,
                )
            ]
        )
        warehouse.insert_attachment_enrichments(
            [
                _default_row(
                    ATTACHMENT_ENRICHMENT_COLUMNS,
                    content_sha256=sha,
                    ai_provider="",
                    ai_model="",
                    ai_prompt_version="",
                    text_extraction_status="unsupported",
                    updated_at=base,
                    sync_version=1,
                )
            ]
        )

    def insert_failures(sha: str, *, started_at: datetime, count: int) -> None:
        for attempt in range(count):
            warehouse.insert_agent_runs(
                [
                    agent_run_row(
                        AgentRunResult(
                            run_id=f"{sha}-run-{attempt}",
                            provider="codex",
                            model="",
                            task_type=AGENT_ATTACHMENT_TASK_TYPE,
                            subject_id=sha,
                            prompt_version=version,
                            input_sha256="x",
                            status="error",
                            final_output_json={},
                            error="unable to locate image",
                            exit_code=1,
                            started_at=started_at + timedelta(seconds=attempt),
                            completed_at=started_at + timedelta(seconds=attempt + 1),
                            events=[],
                        )
                    )
                ]
            )

    # Exhausted its 3 attempts 40 days ago on a since-fixed bug -> should re-enter
    # the pool once those failures fall outside a 14-day window.
    insert_attachment("m-stale", sha="sha-stale")
    insert_failures("sha-stale", started_at=real_now - timedelta(days=40), count=3)
    # Exhausted its 3 attempts yesterday -> still inside the window, stays excluded.
    insert_attachment("m-recent", sha="sha-recent")
    insert_failures("sha-recent", started_at=real_now - timedelta(days=1), count=3)

    def candidate_shas(*, error_window_days: int) -> set[str]:
        return {
            candidate["content_sha256"]
            for candidate in load_file_enrichment_candidates(
                warehouse,
                source=GMAIL_SOURCE,
                provider=provider,
                prompt_version=version,
                limit=10,
                max_error_attempts=3,
                error_window_days=error_window_days,
            )
        }

    windowed = candidate_shas(error_window_days=14)
    assert "sha-stale" in windowed
    assert "sha-recent" not in windowed

    # Disabling the window restores the old "count every failure forever" behavior:
    # both attachments are at the attempt cap, so neither is a candidate.
    unwindowed = candidate_shas(error_window_days=0)
    assert "sha-stale" not in unwindowed
    assert "sha-recent" not in unwindowed


def test_postgres_whatsapp_media_enrichment_candidates_select_downloaded_blobs(
    warehouse: PostgresWarehouse,
) -> None:
    from personal_data_warehouse.file_attachment_enrichment import (
        WHATSAPP_SOURCE,
        has_file_enrichment_candidate,
        load_file_enrichment_candidates,
    )
    from personal_data_warehouse.schema import ATTACHMENT_ENRICHMENT_COLUMNS, WHATSAPP_MEDIA_ITEM_COLUMNS

    warehouse.ensure_whatsapp_tables()
    warehouse.ensure_file_attachment_enrichment_tables()
    warehouse.ensure_agent_tables()
    account = "zach@example.test"
    now = datetime(2026, 6, 1, tzinfo=UTC)
    provider, model, version = "agent_codex", "", WHATSAPP_SOURCE.prompt_version

    def insert_media(message_id: str, *, sha: str, filename: str, mime_type: str, **overrides) -> None:
        defaults = dict(
            account=account,
            chat_id="chat-1",
            message_id=message_id,
            media_type="image",
            filename=filename,
            mime_type=mime_type,
            content_sha256=sha,
            size_bytes=2048,
            is_missing=0,
            storage_backend="google_drive",
            storage_key=f"whatsapp/library/media/{sha}",
            storage_file_id=f"drive-{sha}",
            storage_url="https://drive.example/x",
            message_at=now,
            ingested_at=now,
            sync_version=1,
        )
        defaults.update(overrides)
        warehouse.insert_whatsapp_media_items([_default_row(WHATSAPP_MEDIA_ITEM_COLUMNS, **defaults)])

    def insert_enrichment(sha: str, *, ai_provider: str, ai_prompt_version: str, status: str) -> None:
        warehouse.insert_attachment_enrichments(
            [
                _default_row(
                    ATTACHMENT_ENRICHMENT_COLUMNS,
                    content_sha256=sha,
                    ai_provider=ai_provider,
                    ai_model=model,
                    ai_prompt_version=ai_prompt_version,
                    text_extraction_status=status,
                    updated_at=now,
                    sync_version=1,
                )
            ]
        )

    # Downloaded image -> candidate.
    insert_media("m1", sha="wa-image", filename="photo.jpg", mime_type="image/jpeg")
    # Downloaded document PDF -> candidate (WhatsApp has no deterministic extraction step).
    insert_media("m2", sha="wa-pdf", filename="invoice.pdf", mime_type="application/pdf", media_type="document")
    # History-only metadata row (bytes never downloaded) -> excluded.
    insert_media("m3", sha="wa-missing", filename="missing.jpg", mime_type="image/jpeg", is_missing=1)
    # Non-image document (e.g. audio voice note) -> not a vision candidate.
    insert_media("m4", sha="wa-audio", filename="note.ogg", mime_type="audio/ogg", media_type="voice")
    # Already agent-enriched under this identity -> excluded.
    insert_media("m5", sha="wa-done", filename="done.png", mime_type="image/png")
    insert_enrichment("wa-done", ai_provider=provider, ai_prompt_version=version, status="agent_ok")
    # Enriched only under the Gmail identity (different prompt_version) -> still a candidate.
    insert_media("m6", sha="wa-other-source", filename="shared.png", mime_type="image/png")
    insert_enrichment("wa-other-source", ai_provider=provider, ai_prompt_version="gmail-attachment-agent-v1", status="agent_ok")

    assert has_file_enrichment_candidate(
        warehouse,
        source=WHATSAPP_SOURCE,
        provider=provider,
        prompt_version=version,
    )
    candidates = load_file_enrichment_candidates(
        warehouse,
        source=WHATSAPP_SOURCE,
        provider=provider,
        prompt_version=version,
        limit=10,
    )
    assert {candidate["content_sha256"] for candidate in candidates} == {"wa-image", "wa-pdf", "wa-other-source"}
    by_sha = {candidate["content_sha256"]: candidate for candidate in candidates}
    assert by_sha["wa-image"]["storage_file_id"] == "drive-wa-image"
    # size_bytes is projected through the shared "size" candidate column.
    assert by_sha["wa-image"]["size"] == 2048

    insert_enrichment("wa-image", ai_provider=provider, ai_prompt_version=version, status="agent_ok")
    insert_enrichment("wa-pdf", ai_provider=provider, ai_prompt_version=version, status="agent_not_useful")
    insert_enrichment("wa-other-source", ai_provider=provider, ai_prompt_version=version, status="agent_ok")
    assert not has_file_enrichment_candidate(
        warehouse,
        source=WHATSAPP_SOURCE,
        provider=provider,
        prompt_version=version,
    )


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


def test_gmail_attachment_upsert_preserves_existing_storage_when_record_is_blank() -> None:
    clause = _upsert_clause("gmail_attachments", POSTGRES_TABLES["gmail_attachments"])

    assert (
        "\"storage_backend\" = COALESCE(NULLIF(EXCLUDED.\"storage_backend\", ''), "
        "\"gmail_attachments\".\"storage_backend\")"
    ) in clause
    assert (
        "\"storage_key\" = COALESCE(NULLIF(EXCLUDED.\"storage_key\", ''), "
        "\"gmail_attachments\".\"storage_key\")"
    ) in clause
    assert (
        "\"storage_file_id\" = COALESCE(NULLIF(EXCLUDED.\"storage_file_id\", ''), "
        "\"gmail_attachments\".\"storage_file_id\")"
    ) in clause
    assert (
        "\"storage_status\" = COALESCE(NULLIF(EXCLUDED.\"storage_status\", ''), "
        "\"gmail_attachments\".\"storage_status\")"
    ) in clause


def test_whatsapp_chat_upsert_preserves_group_name_when_record_is_blank() -> None:
    clause = _upsert_clause("whatsapp_chats", POSTGRES_TABLES["whatsapp_chats"])

    assert (
        "\"name\" = COALESCE(NULLIF(EXCLUDED.\"name\", ''), \"whatsapp_chats\".\"name\")"
    ) in clause


def test_whatsapp_chat_participant_upsert_preserves_display_name_when_blank() -> None:
    clause = _upsert_clause("whatsapp_chat_participants", POSTGRES_TABLES["whatsapp_chat_participants"])

    assert (
        "\"display_name\" = COALESCE(NULLIF(EXCLUDED.\"display_name\", ''), "
        "\"whatsapp_chat_participants\".\"display_name\")"
    ) in clause


def test_postgres_whatsapp_chat_name_survives_later_blank_history_row(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_whatsapp_tables()
    base = datetime(2026, 5, 21, 12, tzinfo=UTC)

    def chat_row(*, name: str, sync_version: int) -> dict:
        return {
            "account": "zach@example.test",
            "chat_id": "120363274447440808@g.us",
            "name": name,
            "chat_type": "group",
            "is_archived": 0,
            "last_message_at": base,
            "raw_metadata_json": "{}",
            "ingested_at": base,
            "sync_version": sync_version,
        }

    warehouse.insert_whatsapp_chats([chat_row(name="Founders Group", sync_version=1)])
    # A newer history-sync row with no subject must not blank the real name.
    warehouse.insert_whatsapp_chats([chat_row(name="", sync_version=2)])

    rows = warehouse._query(
        "SELECT name FROM @whatsapp_chats WHERE chat_id = '120363274447440808@g.us'"
    )
    assert [row[0] for row in rows] == ["Founders Group"]


def test_postgres_whatsapp_chat_participants_roundtrip(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_whatsapp_tables()
    base = datetime(2026, 5, 21, 12, tzinfo=UTC)
    warehouse.insert_whatsapp_chat_participants(
        [
            {
                "account": "zach@example.test",
                "chat_id": "120363274447440808@g.us",
                "participant_jid": "15550000001@s.whatsapp.net",
                "phone_jid": "",
                "lid_jid": "",
                "display_name": "Alice",
                "is_admin": 1,
                "is_super_admin": 0,
                "raw_metadata_json": "{}",
                "ingested_at": base,
                "sync_version": 1,
            }
        ]
    )

    rows = warehouse._query(
        "SELECT display_name, is_admin FROM @whatsapp_chat_participants "
        "WHERE chat_id = '120363274447440808@g.us'"
    )
    assert rows == [("Alice", 1)]


def _wa_message_row(*, chat_id: str, message_id: str, sender_jid: str = "", is_from_me: int = 0,
                    push_name: str = "", body_text: str = "", sync_version: int = 1,
                    **overrides) -> dict:
    base = datetime(2026, 6, 1, 12, tzinfo=UTC)
    row = {
        "account": "zach@example.test",
        "chat_id": chat_id,
        "message_id": message_id,
        "sender_jid": sender_jid or chat_id,
        "push_name": push_name,
        "is_from_me": is_from_me,
        "body_text": body_text,
        "message_kind": "text",
        "media_type": "",
        "quoted_message_id": "",
        "message_at": base,
        "edited_at": datetime.fromtimestamp(0, tz=UTC),
        "is_deleted": 0,
        "raw_metadata_json": "{}",
        "ingested_at": base,
        "sync_version": sync_version,
    }
    row.update(overrides)
    return row


def test_backfill_whatsapp_chats_fills_gaps_without_clobbering(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_whatsapp_tables()
    base = datetime(2026, 6, 1, 12, tzinfo=UTC)
    # A real, named group chat already exists; backfill must not touch it.
    warehouse.insert_whatsapp_chats([
        {
            "account": "zach@example.test", "chat_id": "111@g.us", "name": "Real Group",
            "chat_type": "group", "is_archived": 0, "last_message_at": base,
            "raw_metadata_json": "{}", "ingested_at": base, "sync_version": 5,
        }
    ])
    warehouse.insert_whatsapp_messages([
        _wa_message_row(chat_id="111@g.us", message_id="g1"),                  # has a chat row
        _wa_message_row(chat_id="status@broadcast", message_id="s1"),         # no chat row -> status
        _wa_message_row(chat_id="222@g.us", message_id="g2"),                 # no chat row -> group
        _wa_message_row(chat_id="15550001@s.whatsapp.net", message_id="d1"),  # -> user
        _wa_message_row(chat_id="98765@lid", message_id="d2"),               # -> user
    ])

    inserted = warehouse.backfill_whatsapp_chats_from_messages()

    assert inserted == 4  # everything except the already-present 111@g.us
    kinds = dict(warehouse._query(
        "SELECT chat_id, chat_type FROM @whatsapp_chats WHERE account='zach@example.test'"
    ))
    assert kinds["status@broadcast"] == "status"
    assert kinds["222@g.us"] == "group"
    assert kinds["15550001@s.whatsapp.net"] == "user"
    assert kinds["98765@lid"] == "user"
    # Existing named group untouched.
    name = warehouse._query("SELECT name FROM @whatsapp_chats WHERE chat_id='111@g.us'")[0][0]
    assert name == "Real Group"
    # Idempotent: a second pass inserts nothing.
    assert warehouse.backfill_whatsapp_chats_from_messages() == 0


def test_clean_whatsapp_messages_view_classifies_and_resolves(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_whatsapp_tables()
    base = datetime(2026, 6, 1, 12, tzinfo=UTC)
    warehouse.insert_whatsapp_contacts([
        {
            "account": "zach@example.test", "jid": "15550001@s.whatsapp.net",
            "push_name": "Pushy", "first_name": "", "full_name": "Alice Example",
            "business_name": "", "raw_metadata_json": "{}", "ingested_at": base, "sync_version": 1,
        }
    ])
    warehouse.insert_whatsapp_messages([
        _wa_message_row(chat_id="status@broadcast", message_id="s1", sender_jid="15559999@s.whatsapp.net", push_name="Statusy"),
        _wa_message_row(chat_id="333@g.us", message_id="g1", sender_jid="15550001@s.whatsapp.net"),
        _wa_message_row(chat_id="15550001@s.whatsapp.net", message_id="d1", sender_jid="15550001@s.whatsapp.net"),
    ])
    warehouse.backfill_whatsapp_chats_from_messages()

    rows = dict(warehouse._query(
        "SELECT message_id, chat_kind FROM @clean_whatsapp_messages WHERE account='zach@example.test'"
    ))
    assert rows["s1"] == "status"
    assert rows["g1"] == "group"
    assert rows["d1"] == "user"
    # sender_name resolves via whatsapp_contacts (full_name wins over push_name).
    sender = warehouse._query(
        "SELECT sender_name FROM @clean_whatsapp_messages WHERE message_id='d1'"
    )[0][0]
    assert sender == "Alice Example"


def test_clean_whatsapp_messages_resolves_lid_sender_through_phone_jid(
    warehouse: PostgresWarehouse,
) -> None:
    warehouse.ensure_whatsapp_tables()
    base = datetime(2026, 7, 1, 12, tzinfo=UTC)
    account = "owner@example.test"
    phone_jid = "15551234567@s.whatsapp.net"
    lid_jid = "123456789012345@lid"
    warehouse.insert_whatsapp_contacts([
        {
            "account": account,
            "jid": phone_jid,
            "push_name": "",
            "first_name": "",
            "full_name": "Example Person",
            "business_name": "",
            "raw_metadata_json": "{}",
            "ingested_at": base,
            "sync_version": 1,
        }
    ])
    warehouse.insert_whatsapp_chat_participants([
        {
            "account": account,
            "chat_id": "123@g.us",
            "participant_jid": lid_jid,
            "phone_jid": phone_jid,
            "lid_jid": lid_jid,
            "display_name": "",
            "is_admin": 0,
            "is_super_admin": 0,
            "raw_metadata_json": "{}",
            "ingested_at": base,
            "sync_version": 1,
        }
    ])
    warehouse.insert_whatsapp_messages([
        _wa_message_row(
            account=account,
            chat_id="123@g.us",
            message_id="lid-message",
            sender_jid=lid_jid,
        )
    ])

    sender = warehouse._query(
        "SELECT sender_name FROM @clean_whatsapp_messages WHERE message_id='lid-message'"
    )[0][0]

    assert sender == "Example Person"


def test_canonical_contacts_and_apple_messages_resolve_apple_contact(
    warehouse: PostgresWarehouse,
) -> None:
    warehouse.ensure_contacts_tables()
    warehouse.ensure_apple_contacts_tables()
    warehouse.ensure_apple_messages_tables()
    base = datetime(2026, 7, 1, 12, tzinfo=UTC)
    apple_row = _contact_card_row(
        card_id="apple-contact-1",
        display_name="Example Person",
        primary_phone="+1 (555) 123-4567",
        phones=[{"value": "+1 (555) 123-4567", "metadata": {"primary": True}}],
        sync_version=1,
    )
    apple_row.update(
        source="apple_contacts",
        source_kind="apple_contacts",
        account="owner@example.test",
        address_book_id="icloud-source",
        source_uid="apple-contact-1",
        synced_at=base,
        source_updated_at=base,
    )
    warehouse.insert_apple_contact_cards([apple_row])
    warehouse.insert_apple_message_handles([
        {
            "account": "owner@example.test",
            "handle_id": "handle-1",
            "handle_rowid": 1,
            "address": "+15551234567",
            "country": "US",
            "service": "iMessage",
            "uncanonicalized_id": "",
            "person_centric_id": "",
            "raw_metadata_json": "{}",
            "ingested_at": base,
            "sync_version": 1,
        }
    ])
    message = _default_row(
        APPLE_MESSAGE_COLUMNS,
        account="owner@example.test",
        message_id="message-1",
        message_rowid=1,
        handle_id="handle-1",
        body_text="hello",
        message_at=base,
        ingested_at=base,
        sync_version=1,
    )
    warehouse.insert_apple_messages([message])

    contacts = warehouse._query(
        "SELECT source, display_name FROM @clean_contacts WHERE card_id='apple-contact-1'"
    )
    resolved = warehouse._query(
        "SELECT sender_name, sender_address FROM @clean_apple_messages WHERE message_id='message-1'"
    )

    assert contacts == [("apple_contacts", "Example Person")]
    assert resolved == [("Example Person", "+15551234567")]


def test_timeline_reemits_old_apple_message_when_contact_identity_changes(
    warehouse: PostgresWarehouse,
) -> None:
    warehouse.ensure_contacts_tables()
    warehouse.ensure_apple_contacts_tables()
    warehouse.ensure_apple_messages_tables()
    warehouse.ensure_file_attachment_enrichment_tables()
    warehouse.ensure_timeline_tables()
    base = datetime(2025, 1, 1, 12, tzinfo=UTC)
    account = "owner@example.test"
    phone = "+15551234567"
    warehouse.insert_apple_message_handles(
        [
            {
                "account": account,
                "handle_id": "handle-1",
                "handle_rowid": 1,
                "address": phone,
                "country": "US",
                "service": "iMessage",
                "uncanonicalized_id": "",
                "person_centric_id": "",
                "raw_metadata_json": "{}",
                "ingested_at": base,
                "sync_version": 1,
            }
        ]
    )
    warehouse.insert_apple_messages(
        [
            _default_row(
                APPLE_MESSAGE_COLUMNS,
                account=account,
                message_id=f"message-{index}",
                message_rowid=index,
                handle_id="handle-1",
                body_text="hello",
                message_at=base + timedelta(minutes=index),
                ingested_at=base + timedelta(minutes=index),
                sync_version=index,
            )
            for index in range(1, 6)
        ]
    )

    engine = TimelineSyncEngine(
        source_url=_postgres_url(),
        source_schema=warehouse._schema,
        dest_schema=warehouse._schema,
        adapters=[adapter_by_name("apple_message")],
        batch_size=2,
    )
    try:
        engine.run()
        assert warehouse._query(
            "SELECT DISTINCT actor FROM @timeline_events WHERE adapter='apple_message'"
        ) == [(phone,)]

        apple_row = _contact_card_row(
            card_id="apple-contact-1",
            display_name="Example Person",
            primary_phone=phone,
            phones=[{"value": phone, "metadata": {"primary": True}}],
            sync_version=2,
        )
        apple_row.update(
            source="apple_contacts",
            source_kind="apple_contacts",
            account=account,
            address_book_id="icloud-source",
            source_uid="apple-contact-1",
            synced_at=base + timedelta(days=1),
            source_updated_at=base + timedelta(days=1),
        )
        warehouse.insert_apple_contact_cards([apple_row])

        engine.run()
        assert warehouse._query(
            "SELECT actor, count(*) FROM @timeline_events "
            "WHERE adapter='apple_message' GROUP BY actor ORDER BY actor"
        ) == [(phone, 3), ("Example Person", 2)]

        engine.run()
        assert warehouse._query(
            "SELECT actor, count(*) FROM @timeline_events "
            "WHERE adapter='apple_message' GROUP BY actor ORDER BY actor"
        ) == [(phone, 1), ("Example Person", 4)]

        engine.run()
    finally:
        engine.close()

    assert warehouse._query(
        "SELECT actor, count(*) FROM @timeline_events "
        "WHERE adapter='apple_message' GROUP BY actor"
    ) == [("Example Person", 5)]


def test_postgres_warehouse_can_create_all_runtime_tables_and_views(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()
    warehouse.ensure_calendar_tables()
    warehouse.ensure_contacts_tables()
    warehouse.ensure_apple_voice_memos_tables()
    warehouse.ensure_apple_notes_tables()
    warehouse.ensure_apple_messages_tables()
    warehouse.ensure_slack_tables()

    expected = [
        "apple_messages",
        "apple_notes",
        "apple_voice_memos_files",
        "calendar_events",
        "contact_cards",
        "gmail_messages",
        "slack_messages",
    ]
    physical = [_physical_relation(warehouse, logical) for logical in expected]
    rows = warehouse._query(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = ANY(%s) AND table_name = ANY(%s)
        ORDER BY table_schema, table_name
        """,
        (sorted({rel.schema for rel in physical}), sorted({rel.name for rel in physical})),
    )

    found = {(schema, table) for schema, table in rows}
    assert {(rel.schema, rel.name) for rel in physical} <= found


def test_postgres_slack_tables_create_recent_message_indexes(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_slack_tables()

    index_names = _index_names(warehouse, "slack_messages")
    assert "slack_messages_recent_scope_time_idx" in index_names
    assert "slack_messages_recent_thread_time_idx" in index_names
    assert "slack_messages_thread_parents_idx" in index_names
    assert "slack_messages_user_time_idx" in index_names
    assert "slack_messages_time_idx" in index_names
    # Raw-table text search is retired: message text is searched through the
    # timeline document (timeline.search_text / timeline.search_text_exact).
    assert "slack_messages_text_trgm_idx" not in index_names
    assert "slack_messages_text_trgm_live_idx" not in index_names

    slack_user_index_names = _index_names(warehouse, "slack_users")
    assert "slack_users_email_lower_idx" in slack_user_index_names


def test_postgres_slack_messages_set_autovacuum_storage_parameters(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_slack_tables()

    rel = _physical_relation(warehouse, "slack_messages")
    rows = warehouse._query(
        """
        SELECT unnest(c.reloptions)
        FROM pg_class AS c
        INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND c.relname = %s
        """,
        (rel.schema, rel.name),
    )
    reloptions = {row[0] for row in rows}
    assert "autovacuum_analyze_scale_factor=0" in reloptions
    assert "autovacuum_analyze_threshold=50000" in reloptions
    assert "autovacuum_vacuum_scale_factor=0" in reloptions
    assert "autovacuum_vacuum_threshold=100000" in reloptions


def test_postgres_ensure_skips_repeat_ddl_when_already_applied(warehouse: PostgresWarehouse, monkeypatch) -> None:
    # ensure_* runs on every Dagster run; unconditional ALTERs take an ACCESS
    # EXCLUSIVE lock on hot tables each time (~2.3k repeats of the timeline
    # ALTERs in one prod stats window, ~2s of DDL per sync run on a 45 GB
    # table). Once the defaults/options are in place, re-ensuring must not
    # re-run them.
    warehouse.ensure_timeline_tables()
    warehouse.ensure_slack_tables()

    executed: list[str] = []
    original_command = PostgresWarehouse._command

    def recording_command(self, sql, params=None):
        executed.append(" ".join(str(sql).split()))
        return original_command(self, sql, params)

    monkeypatch.setattr(PostgresWarehouse, "_command", recording_command)

    warehouse.ensure_timeline_tables()
    warehouse.ensure_slack_tables()

    repeat_ddl = [
        sql
        for sql in executed
        if "ALTER COLUMN" in sql
        or "CREATE SEQUENCE" in sql
        or (sql.startswith("ALTER TABLE") and " SET (" in sql)
    ]
    assert repeat_ddl == []

    # The defaults the skipped DDL maintains really are in place.
    rel = _physical_relation(warehouse, "timeline_events")
    rows = warehouse._query(
        """
        SELECT column_name, column_default
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
          AND column_name IN ('seq', 'first_seen_at', 'updated_at')
        """,
        (rel.schema, rel.name),
    )
    defaults = {row[0]: str(row[1] or "") for row in rows}
    assert "nextval" in defaults["seq"]
    assert defaults["first_seen_at"].startswith("now()")
    assert defaults["updated_at"].startswith("now()")


def test_postgres_ensure_indexes_drops_obsolete_indexes(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_slack_tables()
    # Recreate the legacy partial index out-of-band, simulating an existing deployment
    # that ran on an older revision before the full-coverage index was introduced.
    warehouse._command(
        "CREATE INDEX IF NOT EXISTS slack_messages_text_trgm_live_idx "
        "ON @slack_messages USING gin (text public.gin_trgm_ops) WHERE is_deleted = 0"
    )
    rel = _physical_relation(warehouse, "slack_messages")
    pre_rows = warehouse._query(
        "SELECT indexname FROM pg_indexes WHERE schemaname = %s "
        "AND tablename = %s AND indexname = 'slack_messages_text_trgm_live_idx'",
        (rel.schema, rel.name),
    )
    assert pre_rows, "test setup failed: legacy index should exist before re-running ensure"

    warehouse.ensure_slack_tables()

    post_rows = warehouse._query(
        "SELECT indexname FROM pg_indexes WHERE schemaname = %s "
        "AND tablename = %s AND indexname = 'slack_messages_text_trgm_live_idx'",
        (rel.schema, rel.name),
    )
    assert post_rows == []


def test_postgres_gmail_tables_create_search_indexes(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()

    index_names = _index_names(warehouse, "gmail_messages")
    assert "gmail_messages_internal_date_idx" in index_names
    # Structured predicates keep their indexes: sender/subject lookups, and
    # snippet because the voice-memo identity hints OR it with from/subject (a
    # bitmap-OR plan needs every arm indexed). The body trigram family is
    # retired — body text is searched through the timeline document
    # (timeline.search_text / timeline.search_text_exact).
    assert "gmail_messages_from_trgm_idx" in index_names
    assert "gmail_messages_subject_trgm_idx" in index_names
    assert "gmail_messages_snippet_trgm_idx" in index_names
    assert "gmail_messages_body_text_trgm_idx" not in index_names
    assert "gmail_messages_body_markdown_trgm_idx" not in index_names
    assert "gmail_messages_body_html_trgm_idx" not in index_names


@pytest.mark.parametrize(
    ("schema", "expects_concurrent"),
    [("public", True), ("alternate_deployment", True), ("pdw_test_example", False)],
)
def test_postgres_concurrent_indexes_are_disabled_only_in_test_namespaces(
    monkeypatch: pytest.MonkeyPatch,
    schema: str,
    expects_concurrent: bool,
) -> None:
    warehouse = object.__new__(PostgresWarehouse)
    warehouse._schema = schema
    warehouse._ensured_index_names = set()
    warehouse._pg_trgm_ensured = False
    warehouse._pg_textsearch_ensured = False
    commands: list[str] = []
    monkeypatch.setattr(warehouse, "_index_exists", lambda _name: False)
    monkeypatch.setattr(warehouse, "_drop_invalid_index", lambda _name: None)
    monkeypatch.setattr(warehouse, "_command", lambda sql, params=None: commands.append(sql))

    warehouse._ensure_indexes(["gmail_messages"])

    [date_index_sql] = [sql for sql in commands if "gmail_messages_internal_date_idx" in sql]
    assert ("CREATE INDEX CONCURRENTLY" in date_index_sql) is expects_concurrent


def test_postgres_agent_tables_create_run_lookup_index(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_agent_tables()

    index_names = _index_names(warehouse, "agent_runs")
    assert "ai_processing_agent_runs_task_status_subject_idx" in index_names


def test_postgres_apple_messages_create_handle_history_index(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_apple_messages_tables()

    index_names = _index_names(warehouse, "apple_messages")
    assert "apple_messages_handle_time_idx" in index_names
    assert "apple_messages_body_trgm_idx" not in index_names

    attachment_index_names = _index_names(warehouse, "apple_message_attachments")
    assert "apple_message_attachments_message_idx" in attachment_index_names


def test_postgres_ai_conversation_event_tables_create_read_path_indexes(
    warehouse: PostgresWarehouse,
) -> None:
    # The marts_ai_conversations.events union view has no storage of its own, so
    # session probes, recency scans, first-prompt template detection, and
    # changed-session watermark scans all depend on per-source indexes.
    warehouse.ensure_agent_sessions_tables()

    import personal_data_warehouse.postgres as postgres_module

    for table in postgres_module._AI_CONVERSATION_EVENT_TABLES:
        index_names = _index_names(warehouse, table)
        assert f"{table}_session_seq_idx" in index_names, table
        assert f"{table}_occurred_at_idx" in index_names, table
        assert f"{table}_first_prompt_idx" in index_names, table
        assert f"{table}_ingested_at_idx" in index_names, table


def test_postgres_indexes_define_no_legacy_agent_session_events_specs() -> None:
    # The legacy mixed agent_session_events table is gone; index definitions
    # against it could never be created (the logical name now resolves to the
    # marts union view) and must not linger in the registry.
    import personal_data_warehouse.postgres as postgres_module

    legacy = [ix.name for ix in postgres_module.POSTGRES_INDEXES if ix.table == "agent_session_events"]
    assert legacy == []


def _pg_textsearch_usable(warehouse: PostgresWarehouse) -> bool:
    # The extension files must be installed AND the library preloaded;
    # CREATE EXTENSION fails without both.
    rows = warehouse._query(
        "SELECT 1 FROM pg_available_extensions WHERE name = 'pg_textsearch'"
        " AND current_setting('shared_preload_libraries') LIKE '%pg_textsearch%'"
    )
    return bool(rows)


def test_postgres_timeline_tables_create_only_timeline_bm25_index(warehouse: PostgresWarehouse) -> None:
    if not _pg_textsearch_usable(warehouse):
        pytest.skip("pg_textsearch is not installed/preloaded on this Postgres host")

    _ensure_all_table_groups(warehouse)

    timeline_indexes = _index_names(warehouse, "timeline_events")
    assert "timeline_events_search_text_bm25_idx" in timeline_indexes

    # The legacy source-table BM25 fan-out indexes should not be recreated; the
    # flow is timeline.search_text() on timeline -> detailed SQL on source tables.
    assert "slack_messages_text_bm25_idx" not in _index_names(warehouse, "slack_messages")
    assert "gmail_messages_subject_bm25_idx" not in _index_names(warehouse, "gmail_messages")


def test_postgres_ensure_indexes_tolerates_missing_pg_textsearch(warehouse: PostgresWarehouse, monkeypatch) -> None:
    original_command = warehouse._command

    def failing_command(sql, params=None):
        if "CREATE EXTENSION IF NOT EXISTS pg_textsearch" in sql:
            raise RuntimeError("pg_textsearch unavailable")
        return original_command(sql, params)

    monkeypatch.setattr(warehouse, "_command", failing_command)

    # Must not raise: hosts without the extension skip the timeline BM25 index
    # but keep creating everything else.
    warehouse.ensure_timeline_tables()

    index_names = _index_names(warehouse, "timeline_events")
    assert "timeline_events_search_text_bm25_idx" not in index_names
    assert "timeline_events_time_idx" in index_names


def _ensure_all_table_groups(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()
    warehouse.ensure_calendar_tables()
    warehouse.ensure_contacts_tables()
    warehouse.ensure_apple_voice_memos_tables(backfill_content_hashes=False)
    warehouse.ensure_apple_notes_tables()
    warehouse.ensure_apple_messages_tables()
    warehouse.ensure_whatsapp_tables()
    warehouse.ensure_photos_tables()
    warehouse.ensure_agent_sessions_tables()
    warehouse.ensure_slack_tables()
    warehouse.ensure_upstream_mutation_tables()
    warehouse.ensure_google_drive_source_tables()
    warehouse.ensure_whoop_tables()
    warehouse.ensure_whoop_private_tables()
    warehouse.ensure_plaid_tables()
    warehouse.ensure_manual_finance_tables()
    warehouse.ensure_finance_tables()
    warehouse.ensure_alice_voice_recordings_tables()
    warehouse.ensure_chatgpt_tables()
    warehouse.ensure_claude_desktop_tables()
    warehouse.ensure_agent_tables()
    warehouse.ensure_file_attachment_enrichment_tables()
    warehouse.ensure_search_index_tables()
    # Every source above has a timeline adapter, and timeline sync reads all of
    # them. Sources added since this helper was last updated were missing, so
    # each timeline-backed search_text test died on an UndefinedTable for
    # whichever unprovisioned source the engine reached first. Keep this list
    # exhaustive: it is the whole point of the helper.
    warehouse.ensure_timeline_tables()


def _sync_timeline(warehouse: PostgresWarehouse) -> None:
    engine = TimelineSyncEngine(
        source_url=_postgres_url(),
        source_schema=warehouse._schema,
        dest_schema=warehouse._schema,
    )
    try:
        engine.run()
    finally:
        engine.close()


def _search_text_index_names() -> set[str]:
    """The bm25 index names search_text() references via to_bm25query(), pulled
    straight from the generated function SQL (no DB needed)."""
    import re

    sql = _search_text_function_sql()
    return set(re.findall(r"to_bm25query\([^,]+,\s*'([a-z0-9_]+)'\)", sql))


def _search_text_function_sql(*, include_all: bool = False) -> str:
    import personal_data_warehouse.postgres as postgres_module

    captured: list[str] = []

    class _Capture:
        # _ensure_search_text_function is called unbound with this double as
        # `self`, so it must answer every attribute that function touches. It
        # grew a _raw_command/_search_text_alter_sql call (the search_path pin
        # that fixed the silent app-search outage) and the double was never
        # updated, so all eleven search_text tests errored on AttributeError —
        # leaving the default cross-source search path with no coverage at all.
        # Borrow the real SQL builders so what is captured is exactly what a
        # live warehouse would issue; only execution is faked.
        _schema = "public"
        _SEARCH_PRIORITY_TOKENS = postgres_module.PostgresWarehouse._SEARCH_PRIORITY_TOKENS
        _search_path_sql = postgres_module.PostgresWarehouse._search_path_sql
        _search_text_alter_sql = postgres_module.PostgresWarehouse._search_text_alter_sql
        sql_relation = postgres_module.PostgresWarehouse.sql_relation
        _object_schema = postgres_module.PostgresWarehouse._object_schema
        physical_schema_name = postgres_module.PostgresWarehouse.physical_schema_name
        physical_schema_names = postgres_module.PostgresWarehouse.physical_schema_names

        def _command(self, sql: str) -> None:
            captured.append(sql)

        def _raw_command(self, sql: str) -> None:
            captured.append(sql)

        # Report the vector prerequisites as present so the hybrid function's
        # DDL is generated and captured for the structural tests below.
        def pgvector_available(self) -> bool:
            return True

        def _relation_exists(self, table: str) -> bool:
            return True

    postgres_module.PostgresWarehouse._ensure_search_text_function(_Capture())
    if include_all:
        return "\n".join(captured)
    # The generator issues the core DDL first, then (prerequisites permitting)
    # the search_hybrid DDL as a second command; concatenate so structural
    # tests see everything a fully-equipped warehouse would run.
    return "\n".join(sql for sql in captured if "CREATE OR REPLACE FUNCTION" in sql or "DO $do$" in sql)


def test_search_hybrid_carries_a_comment_saying_it_is_not_callable_from_plain_sql() -> None:
    """The function is a trap from `pdw schema`: agents found it and called
    search_hybrid('terms', 20), which fails with 42883 because it wants a
    precomputed embedding. The catalog cannot comment a function, so the
    comment is published beside the function itself."""
    statements = _search_text_function_sql(include_all=True)
    comments = [sql for sql in statements.split("\n") if "COMMENT ON FUNCTION @search_hybrid(" in sql]
    assert len(comments) == 1
    assert "NOT callable from plain SQL" in comments[0]
    assert "42883" in comments[0]
    assert "timeline.search_text" in comments[0]


def test_search_hybrid_gives_semantic_rank_a_measured_bounded_boost() -> None:
    sql = _search_text_function_sql()
    import personal_data_warehouse.postgres as postgres_module

    assert (
        f"{postgres_module.SEARCH_HYBRID_SEMANTIC_WEIGHT} * COALESCE(1.0 / (60 + s.rnk), 0)"
    ) in sql


def test_search_hybrid_is_composed_from_independently_callable_legs() -> None:
    """The app can only use several host cores if each expensive leg can run
    on its own pooled Postgres connection.

    The public compatibility function must compose those helpers rather than
    retaining a second copy of the legacy monolith; otherwise the two ranking
    implementations drift on the first tuning change.
    """

    sql = _search_text_function_sql()
    for marker in (
        "CREATE OR REPLACE FUNCTION @search_hybrid_semantic(",
        "CREATE OR REPLACE FUNCTION @search_hybrid_exact(",
        "CREATE OR REPLACE FUNCTION @search_hybrid_fuse(",
        "CREATE OR REPLACE FUNCTION @search_hybrid(",
    ):
        assert marker in sql

    wrapper = sql[sql.rindex("CREATE OR REPLACE FUNCTION @search_hybrid("):]
    assert "FROM @search_hybrid_semantic(" in wrapper
    assert "FROM @search_hybrid_exact(" in wrapper
    assert "FROM @search_hybrid_fuse(" in wrapper
    assert "FROM @search_text(" in wrapper
    assert "OPERATOR(public.<=>)" not in wrapper, (
        "the compatibility wrapper must not retain the legacy ANN flow"
    )


def test_search_hybrid_semantic_helper_runs_exactly_one_vector_leg() -> None:
    sql = _search_text_function_sql()
    start = sql.index("CREATE OR REPLACE FUNCTION @search_hybrid_semantic(")
    end = sql.index("CREATE OR REPLACE FUNCTION @search_hybrid_exact(", start)
    semantic = sql[start:end]

    assert semantic.count("OPERATOR(public.<=>) qvec") >= 2  # global + Drive plans
    assert "qvec_alt" not in semantic
    assert "sum(1.0 / (" in semantic
    assert "chunk_id" in semantic


def test_search_hybrid_semantic_helper_can_bound_extra_query_forms() -> None:
    """Term-bag vectors improve quality from the top of their neighborhoods;
    they must not each repeat the original vectors' measured 1,000-row floor.
    """

    sql = _search_text_function_sql()
    start = sql.index("CREATE OR REPLACE FUNCTION @search_hybrid_semantic(")
    end = sql.index("CREATE OR REPLACE FUNCTION @search_hybrid_exact(", start)
    semantic = sql[start:end]

    assert "candidate_limit integer DEFAULT NULL" in semantic
    assert "requested_candidates integer" in semantic
    assert semantic.count("LIMIT requested_candidates") == 2  # global + Drive plans
    assert "DROP FUNCTION IF EXISTS @search_hybrid_semantic(text, text, integer, text[], timestamptz)" in sql


def test_search_hybrid_fuse_accepts_compact_parallel_leg_results() -> None:
    sql = _search_text_function_sql()
    start = sql.index("CREATE OR REPLACE FUNCTION @search_hybrid_fuse(")
    end = sql.rindex("CREATE OR REPLACE FUNCTION @search_hybrid(")
    fuse = sql[start:end]

    assert "lexical_refs text[]" in fuse
    assert "semantic_legs jsonb" in fuse
    assert "exact_refs text[]" in fuse
    assert "jsonb_to_recordset" in fuse
    assert "sum(j.fuse)" in fuse, (
        "per-vector event evidence must still combine by reciprocal rank"
    )


def test_parallel_hybrid_helpers_reject_unknown_source_tokens() -> None:
    # The helpers are public catalogued functions, not private implementation
    # text. Calling one directly with a typo must fail loudly rather than make
    # an invalid scope look like an honestly empty result.
    sql = _search_text_function_sql()
    for function_name in ("search_hybrid_semantic", "search_hybrid_exact"):
        assert f"RAISE EXCEPTION '{function_name}: unknown source filter %'" in sql


def test_search_hybrid_uses_a_deep_filtered_semantic_candidate_pool() -> None:
    sql = _search_text_function_sql()

    # The full-depth formula is computed once per helper invocation; the app
    # invokes the helper independently for each original query vector.
    assert sql.count("least(greatest(per_source * 20, 1000), 2000)") >= 1


def test_search_hybrid_bounds_agent_session_semantic_candidate_work() -> None:
    sql = _search_text_function_sql()

    # One helper invocation searches one vector; the app invokes that same
    # measured plan concurrently for the instructed and raw representations.
    assert sql.count("sem_adapters <@ ARRAY[") >= 1
    assert sql.count("'agent_session', 'agent_session_turn'") >= 1
    assert sql.count("least(greatest(per_source * 4, 40), 200)") >= 1


def test_search_hybrid_scans_drive_embeddings_source_first_in_parallel() -> None:
    sql = _search_text_function_sql()

    # Drive is large enough (223k chunks) for an exact three-worker scan to
    # beat a filtered walk through the global HNSW. OFFSET 0 is the plan
    # barrier that keeps the adapter-first join below the distance sort; text
    # must only be fetched after the top-k or all Drive documents are detoasted.
    assert "sem_adapters = ARRAY['drive_file']::text[]" in sql
    assert "WITH sem_chunks AS" in sql
    assert sql.count("OFFSET 0") >= 1
    semantic = sql[
        sql.index("CREATE OR REPLACE FUNCTION @search_hybrid_semantic(") :
        sql.index("CREATE OR REPLACE FUNCTION @search_hybrid_exact(")
    ]
    assert "JOIN @search_chunks c2" not in semantic
    assert " c.text," not in semantic and " c2.text" not in semantic, (
        "the parallel semantic helper must defer text until final fused top-k"
    )


def test_search_schema_signature_covers_hybrid_tuning(monkeypatch) -> None:
    import personal_data_warehouse.postgres as postgres_module

    warehouse = object.__new__(postgres_module.PostgresWarehouse)
    monkeypatch.setattr(warehouse, "pgvector_available", lambda: True)
    monkeypatch.setattr(warehouse, "_relation_exists", lambda _table: True)
    baseline = warehouse._search_schema_signature()
    baseline_weight = postgres_module.SEARCH_HYBRID_SEMANTIC_WEIGHT
    baseline_multiplier = postgres_module.SEARCH_HYBRID_CANDIDATE_MULTIPLIER

    monkeypatch.setattr(
        postgres_module,
        "SEARCH_HYBRID_SEMANTIC_WEIGHT",
        baseline_weight + 0.01,
    )
    assert warehouse._search_schema_signature() != baseline

    monkeypatch.setattr(
        postgres_module, "SEARCH_HYBRID_SEMANTIC_WEIGHT", baseline_weight
    )
    monkeypatch.setattr(
        postgres_module,
        "SEARCH_HYBRID_CANDIDATE_MULTIPLIER",
        baseline_multiplier + 1,
    )
    assert warehouse._search_schema_signature() != baseline

    monkeypatch.setattr(
        postgres_module,
        "SEARCH_HYBRID_CANDIDATE_MULTIPLIER",
        baseline_multiplier,
    )
    monkeypatch.setattr(
        postgres_module,
        "SEARCH_HYBRID_MIN_CANDIDATES",
        postgres_module.SEARCH_HYBRID_MIN_CANDIDATES + 1,
    )
    assert warehouse._search_schema_signature() != baseline


def test_search_hybrid_uses_iterative_hnsw_for_filtered_recent_searches() -> None:
    sql = _search_text_function_sql()

    assert "set_config('hnsw.iterative_scan', 'relaxed_order', true)" in sql
    assert "greatest(1000, per_source * 8)" in sql
    assert "set_config('hnsw.max_scan_tuples', '100000', true)" in sql
    assert "set_config('hnsw.scan_mem_multiplier', '4', true)" in sql


def _search_text_branch_source_labels() -> list[str]:
    """The per-branch source labels search_text() filters on, parsed from the
    generated `branch_sources` array in the function SQL (no DB needed)."""
    import re

    sql = _search_text_function_sql()
    match = re.search(r"branch_sources text\[\] := ARRAY\[(.*?)\]", sql, re.DOTALL)
    assert match, "expected search_text() to declare a branch_sources array"
    return re.findall(r"'([a-z0-9_]+)'", match.group(1))


def _search_text_sources_helper_labels() -> list[str]:
    """The labels enumerated by the @search_text_sources() helper, parsed from its
    VALUES list in the generated SQL (no DB needed)."""
    import re

    sql = _search_text_function_sql()
    match = re.search(
        r"CREATE OR REPLACE FUNCTION @search_text_sources\(\).*?\$sources\$(.*?)\$sources\$",
        sql,
        re.DOTALL,
    )
    assert match, "expected search_text_sources() to be defined alongside search_text()"
    return re.findall(r"\('([a-z0-9_]+)'\)", match.group(1))


def test_search_text_sources_helper_matches_branch_labels() -> None:
    # search_text_sources() exists so a caller can discover the exact (terse)
    # tokens search_text()'s `sources` arg accepts. The labels are terse and do
    # not always match source-table names (apple messages => 'imessage', voice
    # memos => 'transcript', ...), and an unknown token must raise rather than
    # silently returning nothing. The
    # helper must therefore enumerate exactly the distinct set of branch labels
    # search_text() filters on, sorted, with no drift.
    branch_labels = _search_text_branch_source_labels()
    assert branch_labels, "expected search_text() to declare branch source labels"

    helper_labels = _search_text_sources_helper_labels()
    assert helper_labels == sorted(set(branch_labels)), (
        "search_text_sources() must list every distinct search_text() source label, "
        f"sorted: branches={sorted(set(branch_labels))} helper={helper_labels}"
    )


def test_search_text_sources_filter_skips_unrequested_branches() -> None:
    sql = _search_text_function_sql()
    assert "branch_sources text[]" in sql
    assert "branch_sqls text[]" in sql
    assert "IF sources IS NOT NULL AND NOT branch_source = ANY (sources) THEN" in sql
    assert "CONTINUE;" in sql


def test_search_text_casts_branch_rows_to_the_physical_hit_type() -> None:
    # The per-branch row cast is built inside a SQL string literal, which the
    # relation qualifier deliberately does not rewrite, so it must already be
    # schema-qualified. An unqualified `::search_text_hit` there resolved —
    # through the function's own pinned search_path, whose last entry is public
    # — to the pre-reorganization public.search_text_hit type. Every branch then
    # silently depended on a legacy leftover, and sweeping that leftover emptied
    # all of them at once, because the per-branch guard swallows the type lookup
    # error exactly like it swallowed the missing BM25 index before it.
    sql = _search_text_function_sql()

    assert '::"timeline"."text_hit"' in sql, (
        "search_text()'s EXECUTE'd branch cast must name the physical hit type"
    )
    assert "::search_text_hit" not in sql, (
        "an unqualified hit-type cast resolves to the legacy public copy, not the search schema"
    )


def test_only_timeline_bm25_indexes_are_registered() -> None:
    # BM25 indexes are expensive enough that per-source sprawl once left ~25GB
    # of dead text indexes behind. Exactly four are justified, and every one
    # after the first has to indexes a small DISJOINT slice rather than a
    # second copy of the corpus: the global timeline index; the PARTIAL index
    # over the low-volume adapters the broad pool's second partition scans
    # (2.6% of rows); and the two ATTENTION partitions over
    # priority IN ('self','direct'), which is 2.72% of rows and 7.9% of
    # document bytes. Any further bm25 index needs the same argument made
    # explicitly.
    import personal_data_warehouse.postgres as postgres_module

    bm25_indexes = {
        ix.name: ix
        for ix in postgres_module.POSTGRES_INDEXES
        if getattr(ix, "requires_pg_textsearch", False)
    }
    assert set(bm25_indexes) == {
        "timeline_events_search_text_bm25_idx",
        "timeline_events_search_text_bm25_lowvol_idx",
        "timeline_events_search_text_bm25_attention_idx",
        "timeline_events_search_text_bm25_attention_lowvol_idx",
    }
    lowvol = bm25_indexes["timeline_events_search_text_bm25_lowvol_idx"].sql
    assert "WHERE adapter IN (" in lowvol, (
        "the low-volume bm25 index must be PARTIAL; a second full-corpus index "
        "doubles the most expensive index in the warehouse"
    )
    assert "'gmail_email'" not in lowvol and "'slack_message'" not in lowvol, (
        "high-volume adapters belong to the global index partition only"
    )
    for name in (
        "timeline_events_search_text_bm25_attention_idx",
        "timeline_events_search_text_bm25_attention_lowvol_idx",
    ):
        attention = bm25_indexes[name].sql
        assert (
            f"WHERE priority IN ({postgres_module.SEARCH_TEXT_ATTENTION_PRIORITIES_SQL})"
            in attention
        ), f"{name} must be PARTIAL on the attention tiers, not a second full corpus"
    # The BIG attention partition deliberately carries NO adapter list. A
    # predicate derived from the adapter registry has to be rebuilt when that
    # registry moves, and rebuilding this one is a ~10 minute exclusive lock on
    # a 45 GB table. The tiny low-volume attention partition carries the list
    # (it must, to be the low-volume partition) and rebuilds in seconds.
    big = bm25_indexes["timeline_events_search_text_bm25_attention_idx"]
    small = bm25_indexes["timeline_events_search_text_bm25_attention_lowvol_idx"]
    assert "adapter" not in big.sql, (
        "the big attention index must not carry the adapter registry in its predicate"
    )
    assert not big.rebuild_on_definition_change
    assert "adapter IN (" in small.sql
    assert small.rebuild_on_definition_change, (
        "a partial index whose predicate comes from the adapter registry must "
        "rebuild when the registry moves, or broad search goes down"
    )
    assert "CREATE INDEX CONCURRENTLY" in big.sql, (
        "a multi-minute build on the 45 GB timeline heap must not hold a write lock"
    )


def test_search_text_only_references_defined_bm25_indexes() -> None:
    import personal_data_warehouse.postgres as postgres_module

    defined = {
        ix.name
        for ix in postgres_module.POSTGRES_INDEXES
        if getattr(ix, "requires_pg_textsearch", False)
    }
    referenced = _search_text_index_names()
    assert referenced, "expected search_text() to reference bm25 indexes"
    undefined = sorted(referenced - defined)
    assert not undefined, f"search_text() references undefined bm25 indexes: {undefined}"


def test_search_text_caps_per_branch_topk_for_broad_search() -> None:
    # The score column recomputes the bm25 operator per returned row (the one
    # form correct on every corpus size: a non-match scores 0). Its cost is
    # rows x tokenize(text), so on the huge Google-Drive/attachment-content
    # branches scoring max_results (50) multi-kB docs is the dominant query
    # latency. A broad (unscoped) search never needs that depth — the merge keeps
    # only each source's top-floor plus a global fill — so the function must cap
    # each branch's top-k for broad searches (SEARCH_TEXT_BROAD_PER_BRANCH_CAP)
    # while leaving a scoped (sources => ARRAY[...]) search at full max_results.
    sql = _search_text_function_sql()
    assert "per_branch_limit" in sql and "WHEN sources IS NULL THEN least(per_source," in sql, (
        "search_text() must cap each branch's top-k for broad (unscoped) searches "
        "via per_branch_limit = least(per_source, SEARCH_TEXT_BROAD_PER_BRANCH_CAP)"
    )
    assert "query, per_branch_limit" in sql, (
        "each branch's EXECUTE must use per_branch_limit (the broad cap), not the "
        "full per_source, as its LIMIT"
    )
    # The score must be the operator recompute, NOT bm25_get_current_score():
    # that helper returns a garbage constant whenever the planner doesn't run a
    # bm25 index scan (small/new tables or any join above the scan), which leaks
    # wrong scores silently — the failure class this function exists to avoid.
    assert "bm25_get_current_score" not in sql, (
        "@search_text() must not use bm25_get_current_score() (unreliable off the "
        "index-scan path); recompute the bm25 operator in the SELECT list instead"
    )


def test_search_text_merge_guarantees_per_source_floor() -> None:
    # A flat global `ORDER BY score LIMIT` can bury a low-volume source (one
    # matching contact card / Drive doc) under a high-volume one (dozens of
    # gmail/slack hits). The merge must rank within each source and guarantee
    # every source's top-N hits ahead of the global cut.
    sql = _search_text_function_sql()
    assert "row_number() OVER (" in sql and "PARTITION BY h.source" in sql, (
        "search_text() merge must rank hits per source with "
        "row_number() OVER (PARTITION BY source ...)"
    )
    assert "src_rank >" in sql, (
        "search_text() merge must order a per-source floor (src_rank > N) ahead "
        "of the global score fill"
    )


def test_search_text_pushes_since_into_branches() -> None:
    # `since` used to be applied only after every branch had already collected
    # (and scored) its top-k over all time, so a tightly time-scoped search paid
    # full-corpus cost and could return zero rows despite in-window matches
    # existing below the all-time top-k. Each branch must filter before ranking.
    sql = _search_text_function_sql()
    assert "%3$L::timestamptz IS NULL OR t.event_ts >= %3$L::timestamptz" in sql, (
        "@search_text() branches must push the `since` bound into the branch WHERE"
    )
    assert "query, per_branch_limit, since" in sql, (
        "each branch's EXECUTE must pass `since` as the third format argument"
    )


def test_search_functions_clamp_max_results() -> None:
    # Broad overfetch (max_results 500-1000) followed by client-side filtering
    # was one of the dominant slow-query families; both search functions clamp
    # to a hard ceiling instead of silently accepting any depth.
    import personal_data_warehouse.postgres as postgres_module

    sql = _search_text_function_sql()
    clamp = (
        "least(greatest(coalesce(max_results, 50), 1), "
        f"{postgres_module.SEARCH_TEXT_MAX_RESULTS_CAP})"
    )
    assert sql.count(clamp) >= 2, (
        "both search_text() and search_text_exact() must clamp max_results to "
        "SEARCH_TEXT_MAX_RESULTS_CAP"
    )


def _search_text_exact_sql() -> str:
    sql = _search_text_function_sql()
    marker = "CREATE OR REPLACE FUNCTION @search_text_exact("
    assert marker in sql, "expected search_text_exact() to be generated alongside search_text()"
    return sql.split(marker, 1)[1]


def test_search_text_exact_uses_trigram_ilike_not_bm25() -> None:
    # Exact/substring search is the trigram index's job: one indexed scan over
    # the same timeline document ranked search uses, ordered by recency. Going
    # through BM25 here would re-introduce the overfetch-and-post-filter
    # pattern this function exists to remove.
    sql = _search_text_exact_sql()
    assert "ILIKE pattern" in sql
    assert "to_bm25query" not in sql
    assert "ORDER BY t.event_ts DESC" in sql


def test_search_text_exact_escapes_like_wildcards() -> None:
    sql = _search_text_exact_sql()
    assert (
        "replace(replace(replace(needle, '\\', '\\\\'), '%', '\\%'), '_', '\\_')" in sql
    ), "search_text_exact() must escape LIKE wildcards so needles are literal"


def test_search_text_exact_rejects_short_needles_and_unknown_sources() -> None:
    sql = _search_text_exact_sql()
    assert "at least 3 characters" in sql
    assert "unknown source" in sql


def test_search_text_exact_windows_preview_around_the_match() -> None:
    # A preview cut from the head of a large document (full transcripts, Drive
    # extracts) routinely misses the matched text, which is exactly what pushed
    # agents back to raw-table scans. The preview must be windowed around the
    # first match position instead.
    sql = _search_text_exact_sql()
    assert "position(lower(needle)" in sql


def test_search_text_exact_source_tokens_match_ranked_search() -> None:
    # Both functions must accept the same `sources` vocabulary, discoverable
    # via search_text_sources(); drift between them would make the documented
    # "ranked -> exact" escalation path error on valid tokens.
    import re

    sql = _search_text_exact_sql()
    match = re.search(r"JOIN \(VALUES (.*?)\) AS map\(adapter, source\)", sql, re.DOTALL)
    assert match, "expected search_text_exact() to map adapters to source tokens"
    tokens = set(re.findall(r"\('[a-z0-9_]+', '([a-z0-9_]+)'\)", match.group(1)))
    assert tokens == set(_search_text_branch_source_labels())


def test_search_text_alter_pins_search_path_for_both_functions() -> None:
    import personal_data_warehouse.postgres as postgres_module

    class _Stub:
        def _search_path_sql(self) -> str:
            return "SET search_path TO pdw_x, public"

        def physical_schema_name(self, schema: str) -> str:
            return schema

        _object_schema = postgres_module.PostgresWarehouse._object_schema

    # The ALTER names the function by its FULL argument list. Adding a
    # parameter without updating it here would leave the search_path pin on a
    # signature that no longer exists -- the exact shape of the 16-day
    # silent-zero outage, except the ALTER would now fail loudly instead.
    sql = postgres_module.PostgresWarehouse._search_text_alter_sql(_Stub())  # type: ignore[arg-type]
    assert '"search_text"(text, integer, text[], timestamptz, text[])' in sql
    assert '"search_text_exact"(text, integer, text[], timestamptz, text[])' in sql


def test_search_text_ranks_across_sources_via_bm25(warehouse: PostgresWarehouse) -> None:
    if not _pg_textsearch_usable(warehouse):
        pytest.skip("pg_textsearch is not installed/preloaded on this Postgres host")

    _ensure_all_table_groups(warehouse)
    # search_text() uses the explicit to_bm25query('q', 'index_name') form plus the
    # public-schema bm25 helpers; the schema-isolated test connection keeps public on
    # the canonical multi-schema search_path for them to resolve.
    warehouse._set_search_path()

    # Guard: every bm25 index search_text() names via to_bm25query must actually be
    # built. _ensure_indexes swallows DDL errors, so a missing/typo'd index would
    # otherwise only blow up (UndefinedObject) when the function is called.
    referenced = _search_text_index_names()
    built = {
        row[0]
        for row in warehouse._query(
            "SELECT indexname FROM pg_indexes WHERE schemaname = ANY(%s)",
            (warehouse.physical_schema_names(include_hidden=True),),
        )
    }
    missing = sorted(name for name in referenced if name not in built)
    assert not missing, f"search_text() references bm25 indexes that were not built: {missing}"

    message_datetime = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.insert_slack_conversations(
        [_slack_conversation_row(conversation_id="C1", conversation_type="private_channel", sync_version=1)]
    )
    warehouse.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id="C1",
                message_ts="100.1",
                message_datetime=message_datetime,
                text="planning the zanzibar rollout schedule",
            ),
            _slack_message_row(
                conversation_id="C1",
                message_ts="100.2",
                message_datetime=message_datetime,
                text="lunch plans for friday",
            ),
        ]
    )
    warehouse.insert_messages(
        [_message_row(message_id="m1", subject="zanzibar kickoff", labels=["INBOX"], sync_version=1)]
    )

    # A downloaded WhatsApp media blob whose agent enrichment text mentions the
    # query term must surface through the parent timeline message's search_text.
    from personal_data_warehouse.schema import (
        APPLE_MESSAGE_ATTACHMENT_COLUMNS,
        APPLE_MESSAGE_COLUMNS,
        ATTACHMENT_ENRICHMENT_COLUMNS,
        WHATSAPP_MEDIA_ITEM_COLUMNS,
        WHATSAPP_MESSAGE_COLUMNS,
    )

    warehouse.insert_whatsapp_messages(
        [
            _default_row(
                WHATSAPP_MESSAGE_COLUMNS,
                account="zach@example.com",
                chat_id="chat-1",
                message_id="wamid-1",
                body_text="",
                message_at=message_datetime,
                ingested_at=message_datetime,
                sync_version=1,
            )
        ]
    )
    warehouse.insert_whatsapp_media_items(
        [
            _default_row(
                WHATSAPP_MEDIA_ITEM_COLUMNS,
                account="zach@example.com",
                chat_id="chat-1",
                message_id="wamid-1",
                media_type="image",
                filename="poster.jpg",
                mime_type="image/jpeg",
                content_sha256="wa-zan-sha",
                is_missing=0,
                message_at=message_datetime,
                ingested_at=message_datetime,
                sync_version=1,
            )
        ]
    )
    warehouse.insert_attachment_enrichments(
        [
            _default_row(
                ATTACHMENT_ENRICHMENT_COLUMNS,
                content_sha256="wa-zan-sha",
                ai_provider="agent_codex",
                ai_model="",
                ai_prompt_version="whatsapp-media-agent-v1",
                text="zanzibar rollout launch poster",
                text_extraction_status="agent_ok",
                updated_at=message_datetime,
                sync_version=1,
            )
        ]
    )

    # An iMessage attachment whose agent enrichment text mentions the query term
    # (either vision-OCR'd or a cleaned-up audio transcript - both land in the
    # same shared table) must surface through the parent timeline message.
    warehouse.insert_apple_messages(
        [
            _default_row(
                APPLE_MESSAGE_COLUMNS,
                account="user@example.test",
                message_id="imsg-1",
                body_text="",
                message_at=message_datetime,
                ingested_at=message_datetime,
                sync_version=1,
            )
        ]
    )
    warehouse.insert_apple_message_attachments(
        [
            _default_row(
                APPLE_MESSAGE_ATTACHMENT_COLUMNS,
                account="user@example.test",
                attachment_id="att-1",
                message_id="imsg-1",
                filename="zanzibar_photo.heic",
                mime_type="image/heic",
                content_sha256="im-zan-sha",
                is_missing=0,
                created_at=message_datetime,
                start_at=message_datetime,
                ingested_at=message_datetime,
                sync_version=1,
            )
        ]
    )
    warehouse.insert_attachment_enrichments(
        [
            _default_row(
                ATTACHMENT_ENRICHMENT_COLUMNS,
                content_sha256="im-zan-sha",
                ai_provider="agent_codex",
                ai_model="",
                ai_prompt_version="apple-messages-attachment-agent-v1",
                text="zanzibar rollout launch photo",
                text_extraction_status="agent_ok",
                updated_at=message_datetime,
                sync_version=1,
            )
        ]
    )

    _sync_timeline(warehouse)

    # BM25 non-matches score 0; matches score negative. Isolate matches with score < 0
    # so the assertions hold regardless of how few total rows the fixture has.
    matched = warehouse._query(
        "SELECT source, subsource, ref FROM @search_text('zanzibar rollout', 20) WHERE score < 0"
    )
    matched_sources = {(row[0], row[1]) for row in matched}
    matched_refs = {row[2] for row in matched}
    assert ("slack", "message") in matched_sources
    assert ("gmail", "email") in matched_sources
    assert ("whatsapp", "message") in matched_sources
    assert ("imessage", "message") in matched_sources
    assert any(ref.endswith("100.1") for ref in matched_refs)  # the zanzibar slack message
    assert any(ref == "whatsapp_message:zach@example.com|chat-1|wamid-1" for ref in matched_refs)
    assert any(ref == "apple_message:user@example.test|imsg-1" for ref in matched_refs)
    assert all("100.2" not in ref for ref in matched_refs)  # the unrelated lunch message

    # sources filter restricts the fan-out.
    gmail_only = warehouse._query(
        "SELECT DISTINCT source FROM @search_text('zanzibar', 20, ARRAY['gmail']) WHERE score < 0"
    )
    assert gmail_only == [("gmail",)]

    # since filter excludes rows dated before the cutoff (fixtures are 2026-05-19).
    after_cutoff = warehouse._query(
        "SELECT count(*) FROM @search_text('zanzibar', 20, NULL, '2027-01-01'::timestamptz) WHERE score < 0"
    )
    assert after_cutoff == [(0,)]

    # search_text() must run on the read-only query surface (the MCP/CLI tool is
    # read-only), so it may not do DDL/DML at call time. Run it under a genuine
    # read-only transaction and assert it still returns the matches.
    warehouse._command("SET default_transaction_read_only = on")
    try:
        read_only = warehouse._query(
            "SELECT source, subsource FROM @search_text('zanzibar rollout', 20) WHERE score < 0"
        )
    finally:
        warehouse._command("SET default_transaction_read_only = off")
    read_only_sources = {(row[0], row[1]) for row in read_only}
    assert ("slack", "message") in read_only_sources
    assert ("gmail", "email") in read_only_sources

    # Legacy source-table BM25 indexes are no longer part of the general search
    # path. Dropping any leftovers must leave timeline search intact.
    warehouse._command("DROP INDEX IF EXISTS apple_voice_memos_title_bm25_idx")
    warehouse._command("DROP INDEX IF EXISTS contact_cards_name_bm25_idx")
    survived = warehouse._query(
        "SELECT source, subsource FROM @search_text('zanzibar rollout', 20) WHERE score < 0"
    )
    survived_sources = {(row[0], row[1]) for row in survived}
    assert ("slack", "message") in survived_sources
    assert ("gmail", "email") in survived_sources


def test_search_text_sources_lists_accepted_filter_tokens(warehouse: PostgresWarehouse) -> None:
    # search_text_sources() must return, on the read-only query surface, exactly
    # the tokens search_text()'s `sources` filter accepts — the discoverable
    # source of truth for the terse labels (note/transcript/agent_session/...).
    if not _pg_textsearch_usable(warehouse):
        pytest.skip("pg_textsearch is not installed/preloaded on this Postgres host")

    _ensure_all_table_groups(warehouse)
    warehouse._set_search_path()

    expected = sorted(set(_search_text_branch_source_labels()))
    assert expected, "expected search_text() to declare branch source labels"

    # The MCP/CLI tool is read-only, so search_text_sources() must run under a
    # genuine read-only transaction (no DDL/DML at call time).
    warehouse._command("SET default_transaction_read_only = on")
    try:
        rows = warehouse._query("SELECT source FROM @search_text_sources() ORDER BY source")
    finally:
        warehouse._command("SET default_transaction_read_only = off")
    assert [row[0] for row in rows] == expected

    # Every label search_text_sources() advertises must actually be a token
    # search_text()'s sources filter recognizes (i.e. it does not skip every
    # branch and return nothing for a label it claims to accept). Use a term that
    # cannot match real fixtures so the branch executes but the assertion is about
    # the function compiling/accepting the token, not about hit counts.
    for label in expected:
        warehouse._query(
            "SELECT count(*) FROM @search_text('zzqqxx', 5, ARRAY[%s])",
            (label,),
        )


def test_search_text_rejects_unknown_source_tokens(warehouse: PostgresWarehouse) -> None:
    # An unknown `sources` token must RAISE (pointing the caller at
    # search_text_sources()), NOT silently return nothing. The accepted tokens
    # are terse and differ from some table/source names; the recurring wrong
    # guesses ('apple_messages', 'voice_memos', ...) resolve through
    # SEARCH_SOURCE_ALIASES instead of erroring, but anything outside the alias
    # map and the canonical tokens must stay loud: a silent empty result reads
    # as "nothing matched" and yields confident wrong answers.
    if not _pg_textsearch_usable(warehouse):
        pytest.skip("pg_textsearch is not installed/preloaded on this Postgres host")

    _ensure_all_table_groups(warehouse)
    warehouse._set_search_path()

    with pytest.raises(psycopg2.Error, match="unknown source"):
        warehouse._query("SELECT * FROM @search_text('zzqqxx', 5, ARRAY['file_attachment'])")

    # A mix of one valid and one invalid token still raises (no partial silent
    # drop of the unknown one).
    with pytest.raises(psycopg2.Error, match="unknown source"):
        warehouse._query("SELECT * FROM @search_text('zzqqxx', 5, ARRAY['imessage', 'bogus'])")

    # A valid token is unaffected.
    warehouse._query("SELECT count(*) FROM @search_text('zzqqxx', 5, ARRAY['imessage'])")


def test_search_source_aliases_are_disjoint_from_canonical_tokens() -> None:
    # Every alias must map onto a canonical token and never shadow one:
    # a key that equals a canonical token would silently rewrite a valid
    # request, and a value outside the token set would raise at call time.
    import personal_data_warehouse.postgres as postgres_module

    tokens = {source for source, _, _ in postgres_module.SEARCH_SOURCE_DEFS}
    aliases = postgres_module.SEARCH_SOURCE_ALIASES
    assert not set(aliases) & tokens, "aliases must not shadow canonical tokens"
    assert set(aliases.values()) <= tokens, "every alias must resolve to a canonical token"


def test_search_text_accepts_source_aliases(warehouse: PostgresWarehouse) -> None:
    # Production sessions guess warehouse-familiar names ('apple_messages',
    # 'voice_memos', 'contacts') for the terse source tokens over and over.
    # Both search functions resolve those aliases instead of round-tripping a
    # RAISE at the caller.
    if not _pg_textsearch_usable(warehouse):
        pytest.skip("pg_textsearch is not installed/preloaded on this Postgres host")

    _ensure_all_table_groups(warehouse)
    warehouse._set_search_path()

    for alias in ("apple_messages", "voice_memos", "contacts", "drive", "whoop_sleep"):
        warehouse._query("SELECT count(*) FROM @search_text('zzqqxx', 5, ARRAY[%s])", (alias,))
        warehouse._query(
            "SELECT count(*) FROM @search_text_exact('zzqqxx', 5, ARRAY[%s])", (alias,)
        )

    # Aliased and canonical requests hit the same branch.
    message_datetime = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.insert_slack_conversations(
        [_slack_conversation_row(conversation_id="C1", conversation_type="private_channel", sync_version=1)]
    )
    warehouse.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id="C1",
                message_ts="100.1",
                message_datetime=message_datetime,
                text="quokka sighting on the wallaby trail",
            )
        ]
    )
    _sync_timeline(warehouse)
    canonical = warehouse._query(
        "SELECT ref FROM @search_text('quokka', 5, ARRAY['slack']) WHERE score < 0"
    )
    assert canonical, "expected the canonical token to match the fixture"


def test_search_text_exact_finds_literal_phrases(warehouse: PostgresWarehouse) -> None:
    # The exact-search half of the timeline layer: a literal substring over the
    # same search document ranked search uses, served by the trigram index.
    # This is the supported replacement for raw-table ILIKE scans and for the
    # "overfetch search_text() then outer-ILIKE the preview" pattern.
    _ensure_all_table_groups(warehouse)
    warehouse._set_search_path()

    message_datetime = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.insert_slack_conversations(
        [_slack_conversation_row(conversation_id="C1", conversation_type="private_channel", sync_version=1)]
    )
    warehouse.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id="C1",
                message_ts="100.1",
                message_datetime=message_datetime,
                text="deploy pin: rollout-cadence-7g4 approved for friday",
            ),
            _slack_message_row(
                conversation_id="C1",
                message_ts="100.2",
                message_datetime=message_datetime,
                text="lunch plans for friday",
            ),
        ]
    )
    _sync_timeline(warehouse)

    rows = warehouse._query(
        "SELECT source, ref, text, score FROM @search_text_exact(%s, 10)",
        ("rollout-cadence-7g4",),
    )
    assert [row[0] for row in rows] == ["slack"]
    assert "rollout-cadence-7g4" in rows[0][2]

    # A needle that only matches as a BM25 stem, not a literal substring, must
    # not match: this function is exact.
    assert warehouse._query("SELECT * FROM @search_text_exact('rollout-cadence-9z9', 10)") == []

    # The sources filter uses the same tokens as ranked search.
    assert (
        warehouse._query(
            "SELECT * FROM @search_text_exact(%s, 10, sources => ARRAY['gmail'])",
            ("rollout-cadence-7g4",),
        )
        == []
    )
    with pytest.raises(psycopg2.Error, match="unknown source"):
        warehouse._query("SELECT * FROM @search_text_exact('zzqqxx', 5, ARRAY['file_attachment'])")

    # LIKE wildcards in the needle are literal characters, not patterns.
    assert warehouse._query("SELECT * FROM @search_text_exact('roll%cadence', 10)") == []

    # Needles below trigram length raise loudly instead of degrading to a scan.
    with pytest.raises(psycopg2.Error, match="at least 3 characters"):
        warehouse._query("SELECT * FROM @search_text_exact('ab', 5)")

    # `since` bounds results.
    assert (
        warehouse._query(
            "SELECT * FROM @search_text_exact(%s, 10, since => %s)",
            ("rollout-cadence-7g4", datetime(2026, 6, 1, tzinfo=UTC)),
        )
        == []
    )


def test_search_text_exact_matches_number_format_variants(warehouse: PostgresWarehouse) -> None:
    # Agents paste machine tokens in whatever format the copy source used and
    # then probe variants by hand ('1,441.52' AND '1441.52'; full phone AND
    # fragments — both observed in production sessions). Exact search matches
    # deterministic formatting variants of the needle in one call: thousands
    # separators both ways, and phone punctuation stripped.
    _ensure_all_table_groups(warehouse)
    warehouse._set_search_path()

    message_datetime = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.insert_slack_conversations(
        [_slack_conversation_row(conversation_id="C1", conversation_type="private_channel", sync_version=1)]
    )
    warehouse.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id="C1",
                message_ts="100.1",
                message_datetime=message_datetime,
                text="invoice total came to 1,441.52 for the venue",
            ),
            _slack_message_row(
                conversation_id="C1",
                message_ts="100.2",
                message_datetime=message_datetime,
                text="wire exactly 2716.09 by friday",
            ),
            _slack_message_row(
                conversation_id="C1",
                message_ts="100.3",
                message_datetime=message_datetime,
                text="text me at +14155163303 when you land",
            ),
        ]
    )
    _sync_timeline(warehouse)

    # Needle without separators finds the comma-formatted document.
    rows = warehouse._query("SELECT text FROM @search_text_exact('1441.52', 10)")
    assert len(rows) == 1 and "1,441.52" in rows[0][0]
    # Needle with separators finds the plain document.
    rows = warehouse._query("SELECT text FROM @search_text_exact('2,716.09', 10)")
    assert len(rows) == 1 and "2716.09" in rows[0][0]
    # Phone punctuation in the needle is stripped to a digits-only variant.
    rows = warehouse._query("SELECT text FROM @search_text_exact('(415) 516-3303', 10)")
    assert len(rows) == 1 and "+14155163303" in rows[0][0]
    # The verbatim needle still matches exactly.
    rows = warehouse._query("SELECT text FROM @search_text_exact('1,441.52', 10)")
    assert len(rows) == 1 and "1,441.52" in rows[0][0]

    # Contact cards additionally index digits-only phone variants, so ANY
    # formatting of a stored number matches — not just the app's display form.
    warehouse.insert_contact_cards(
        [
            _contact_card_row(
                card_id="card-ph",
                display_name="Phone Fixture",
                sync_version=1,
                primary_phone="+1 (415) 516-9999",
                phones=[{"value": "+1 (415) 516-9999"}],
            )
        ]
    )
    _sync_timeline(warehouse)
    rows = warehouse._query(
        "SELECT text FROM @search_text_exact('415-516-9999', 10, ARRAY['contact'])"
    )
    assert len(rows) == 1 and "Phone Fixture" in rows[0][0]


def test_search_text_broad_scores_are_the_real_operator_score_on_a_small_corpus(
    warehouse: PostgresWarehouse,
) -> None:
    """A broad hit's score must be the bm25 operator's own number, on ANY corpus.

    The broad pool no longer carries a per-row score -- it carries the scan
    ordinal, and only the few candidates that survive the per-source floor are
    scored. Two things have to stay true for that to be a pure speed change,
    and this test is deliberately run against a TINY corpus, which is where the
    tempting shortcut (`bm25_get_current_score()`, banned by
    test_search_text_scores_with_the_bm25_operator_not_the_scan_helper) returns
    a garbage constant instead of failing:

    * every reported score equals an independent recompute of the operator for
      that exact row, and the scores are not all the same value; and
    * within a source, the returned order is score order -- which is the
      assumption that lets the ordinal stand in for the score.
    """
    if not _pg_textsearch_usable(warehouse):
        pytest.skip("pg_textsearch is not installed/preloaded on this Postgres host")

    _ensure_all_table_groups(warehouse)
    warehouse._set_search_path()

    message_datetime = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.insert_slack_conversations(
        [
            _slack_conversation_row(
                conversation_id="C1", conversation_type="private_channel", sync_version=1
            )
        ]
    )
    # Documents chosen so their BM25 scores genuinely differ: term frequency
    # falls and document length rises down the list. A constant-score bug
    # cannot survive rows whose true scores are this far apart.
    filler = " ".join(f"unrelatedword{n}" for n in range(120))
    warehouse.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id="C1",
                message_ts=f"200.{index}",
                message_datetime=message_datetime,
                text=text,
            )
            for index, text in enumerate(
                [
                    "zanzibar zanzibar zanzibar zanzibar rollout rollout",
                    "zanzibar zanzibar rollout",
                    "zanzibar rollout schedule",
                    "zanzibar schedule",
                    f"zanzibar rollout {filler}",
                    "friday lunch plans",
                ]
            )
        ]
    )
    # ...and a low-volume source too, so the cross-partition merge is exercised
    # rather than one partition's ordering standing in for the whole answer.
    warehouse.insert_contact_cards(
        [
            _contact_card_row(
                card_id="card-zan-1", display_name="Zanzibar Rollout Person", sync_version=1
            ),
            _contact_card_row(
                card_id="card-zan-2", display_name="Zanzibar Person", sync_version=1
            ),
        ]
    )
    _sync_timeline(warehouse)

    hits = warehouse._query(
        "SELECT source, ref, score FROM @search_text('zanzibar rollout', 20)"
    )
    assert len(hits) >= 5, f"expected the fixture corpus to produce hits, got {hits}"
    assert all(score < 0 for _, _, score in hits), (
        "a BM25 hit scores negative; a non-negative score means the merge "
        "returned a document the scan never matched"
    )
    assert len({score for _, _, score in hits}) > 1, (
        "every hit carrying the same score is the signature of a scan-state "
        "helper standing in for the operator"
    )

    # Independent recompute, through the same index the row's pool partition
    # was scanned with. Equality here is what proves the reported score is the
    # operator's own value and not an artifact of how the row was fetched.
    for _, ref, score in hits:
        adapter, event_id = ref.split(":", 1)
        index_name = (
            "timeline_events_search_text_bm25_lowvol_idx"
            if adapter in SEARCH_TEXT_LOW_VOLUME_ADAPTERS
            else "timeline_events_search_text_bm25_idx"
        )
        expected = warehouse._query(
            "SELECT (t.search_text <@> to_bm25query(%s, '" + index_name + "'))::real "
            "FROM @timeline_events t WHERE t.adapter = %s AND t.event_id = %s",
            ("zanzibar rollout", adapter, event_id),
        )
        assert expected and expected[0][0] == score, (
            f"{ref} was reported with score {score} but the bm25 operator "
            f"scores it {expected}"
        )

    # Within one source the output order must be score order. It is produced
    # from the scan ORDINAL, so this is the assertion that fails if the ordinal
    # ever stops tracking the score.
    for source in {source for source, _, _ in hits}:
        scores = [score for hit_source, _, score in hits if hit_source == source]
        assert scores == sorted(scores), (
            f"{source} hits came back out of score order: {scores}"
        )
        assert len(scores) >= 1


def test_search_text_broad_and_scoped_agree_on_a_source_ordering(
    warehouse: PostgresWarehouse,
) -> None:
    """One source, two code paths, one answer.

    The scoped path still scores every branch row with the operator; the broad
    path ranks by scan ordinal and scores only the survivors. If the ordinal
    were not the score order, these two would disagree about the same source --
    silently, because both return plausible-looking hits.
    """
    if not _pg_textsearch_usable(warehouse):
        pytest.skip("pg_textsearch is not installed/preloaded on this Postgres host")

    _ensure_all_table_groups(warehouse)
    warehouse._set_search_path()

    message_datetime = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.insert_slack_conversations(
        [
            _slack_conversation_row(
                conversation_id="C1", conversation_type="private_channel", sync_version=1
            )
        ]
    )
    filler = " ".join(f"unrelatedword{n}" for n in range(80))
    warehouse.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id="C1",
                message_ts=f"300.{index}",
                message_datetime=message_datetime,
                text=text,
            )
            for index, text in enumerate(
                [
                    "quokka quokka quokka migration migration",
                    "quokka migration plan",
                    "quokka migration",
                    f"quokka migration {filler}",
                    "quokka",
                ]
            )
        ]
    )
    _sync_timeline(warehouse)

    broad = [
        (ref, score)
        for source, ref, score in warehouse._query(
            "SELECT source, ref, score FROM @search_text('quokka migration', 20)"
        )
        if source == "slack"
    ]
    scoped = [
        (ref, score)
        for ref, score in warehouse._query(
            "SELECT ref, score FROM @search_text('quokka migration', 20, ARRAY['slack'])"
        )
    ]
    assert broad, "the broad path returned no slack hits for the fixture corpus"
    # The broad path caps each source at SEARCH_TEXT_BROAD_PER_BRANCH_CAP-ish
    # depth via the floor + global fill, so compare the prefix they share.
    depth = min(len(broad), len(scoped))
    assert depth >= 3
    assert broad[:depth] == scoped[:depth], (
        "the ordinal-ranked broad path and the score-ranked scoped path "
        f"disagree: broad={broad[:depth]} scoped={scoped[:depth]}"
    )


def test_search_text_hits_carry_drilldown_columns(warehouse: PostgresWarehouse) -> None:
    # A hit must terminate in one hop: event_ts mirrors occurred_at (agents
    # copy timeline.events column lists into search calls — the #1 recurring
    # 42703), and title/source_table/source_pk point straight at the source
    # row without the intermediate timeline.events lookup.
    if not _pg_textsearch_usable(warehouse):
        pytest.skip("pg_textsearch is not installed/preloaded on this Postgres host")

    _ensure_all_table_groups(warehouse)
    warehouse._set_search_path()

    message_datetime = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.insert_slack_conversations(
        [_slack_conversation_row(conversation_id="C1", conversation_type="private_channel", sync_version=1)]
    )
    warehouse.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id="C1",
                message_ts="100.1",
                message_datetime=message_datetime,
                text="planning the zanzibar rollout schedule",
            )
        ]
    )
    _sync_timeline(warehouse)

    rows = warehouse._query(
        "SELECT occurred_at, event_ts, title, source_table, source_pk"
        " FROM @search_text('zanzibar', 10) WHERE score < 0"
    )
    assert rows, "expected a ranked hit"
    occurred_at, event_ts, title, source_table, source_pk = rows[0]
    assert event_ts == occurred_at
    assert source_table == "slack_messages"
    assert source_pk and "conversation_id" in source_pk
    assert title is not None

    exact_rows = warehouse._query(
        "SELECT occurred_at, event_ts, source_table, source_pk"
        " FROM @search_text_exact('zanzibar rollout', 10)"
    )
    assert exact_rows and exact_rows[0][1] == exact_rows[0][0]
    assert exact_rows[0][2] == "slack_messages"


def test_search_text_windows_ranked_preview_around_match(warehouse: PostgresWarehouse) -> None:
    # The ranked preview must be windowed around the first query-term match,
    # not cut from the head of the document: a head cut routinely misses the
    # matched span in large documents, which made true hits read as false
    # positives (and was already fixed for search_text_exact).
    if not _pg_textsearch_usable(warehouse):
        pytest.skip("pg_textsearch is not installed/preloaded on this Postgres host")

    _ensure_all_table_groups(warehouse)
    warehouse._set_search_path()

    message_datetime = datetime(2026, 5, 19, 12, tzinfo=UTC)
    filler = " ".join(f"filler{i}" for i in range(2500))  # far past the preview cap
    warehouse.insert_slack_conversations(
        [_slack_conversation_row(conversation_id="C1", conversation_type="private_channel", sync_version=1)]
    )
    warehouse.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id="C1",
                message_ts="100.1",
                message_datetime=message_datetime,
                text=f"{filler} the xylocarp shipment arrives tuesday",
            )
        ]
    )
    _sync_timeline(warehouse)

    rows = warehouse._query("SELECT text FROM @search_text('xylocarp', 10) WHERE score < 0")
    assert rows, "expected a ranked hit on the long document"
    assert "xylocarp" in rows[0][0], (
        "ranked preview must include the matched span, not the head of the document"
    )


def test_search_text_raises_when_every_branch_fails(warehouse: PostgresWarehouse) -> None:
    # A broken search layer must be loud. The per-branch guard may degrade a
    # PARTIAL failure (mid-deploy index build) to a WARNING, but when the scan
    # cannot run at all there are no results to degrade to — returning an empty
    # set here is exactly the silent-outage mode that went unnoticed for 16
    # days.
    #
    # The assertion deliberately does NOT pin the old "every source branch
    # failed" wording. A broad search no longer fans out per source; it pools
    # two index-ordered scans, so a missing index now surfaces as the raw
    # undefined-object error instead of the fan-out summary. What must stay
    # true is that it RAISES and names the index, not the exact prose.
    if not _pg_textsearch_usable(warehouse):
        pytest.skip("pg_textsearch is not installed/preloaded on this Postgres host")

    _ensure_all_table_groups(warehouse)
    warehouse._set_search_path()

    warehouse._command("DROP INDEX IF EXISTS timeline_events_search_text_bm25_idx")
    # The per-branch guard (and therefore the aggregate "every branch failed"
    # message) belongs to the SCOPED path. This assertion used to be written
    # against an unscoped call, and silently stopped testing what it claims to
    # when broad search moved off the branch loop onto the pooled scan: it has
    # been failing on main since, with the raw index error instead of the
    # guard's message.
    with pytest.raises(psycopg2.Error, match="every source branch failed"):
        warehouse._query("SELECT * FROM @search_text('zanzibar', 5, ARRAY['slack','gmail'])")
    warehouse._connection.rollback()
    # The BROAD path has no per-branch guard by design — one pooled scan, not
    # eighteen branches — but it must be just as loud. An empty result here is
    # the 16-day silent outage, so a raise (of any message) is the contract.
    with pytest.raises(psycopg2.Error):
        warehouse._query("SELECT * FROM @search_text('zanzibar', 5)")


def test_timeline_backfill_resets_when_adapter_definition_changes(
    warehouse: PostgresWarehouse,
) -> None:
    # Changing an adapter's normalization SQL (new search-document fields,
    # reclassified priority, ...) used to apply only to rows the sync touched
    # afterwards: historical rows kept the old shape forever because the
    # backfill cursor was spent. The engine now fingerprints each adapter's
    # definition in timeline_sync_state and restarts the backfill when it
    # changes, so history converges; the content-guarded upsert keeps
    # unchanged rows free.
    _ensure_all_table_groups(warehouse)
    warehouse._set_search_path()

    message_datetime = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.insert_slack_conversations(
        [_slack_conversation_row(conversation_id="C1", conversation_type="private_channel", sync_version=1)]
    )
    warehouse.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id="C1",
                message_ts="100.1",
                message_datetime=message_datetime,
                text="signature reset fixture",
            )
        ]
    )
    _sync_timeline(warehouse)

    rows = warehouse._query(
        "SELECT adapter_signature, backfill_done, backfill_rows"
        " FROM @timeline_sync_state WHERE adapter = 'slack_message'"
    )
    assert rows, "expected a slack_message sync-state row"
    signature, backfill_done, backfill_rows = rows[0]
    assert signature != "", "the engine must record the adapter definition signature"
    assert backfill_done == 1

    # An unchanged definition must NOT re-walk (the whole point of the guard).
    _sync_timeline(warehouse)
    unchanged = warehouse._query(
        "SELECT backfill_rows FROM @timeline_sync_state WHERE adapter = 'slack_message'"
    )
    assert unchanged[0][0] == backfill_rows, "unchanged definition re-walked the backfill"

    # Simulate a definition change (as a deploy with edited adapter SQL would
    # look to the stored state): the next run restarts and re-walks history.
    warehouse._command(
        "UPDATE @timeline_sync_state SET adapter_signature = 'stale' WHERE adapter = 'slack_message'"
    )
    _sync_timeline(warehouse)
    after = warehouse._query(
        "SELECT adapter_signature, backfill_done, backfill_rows"
        " FROM @timeline_sync_state WHERE adapter = 'slack_message'"
    )
    assert after[0][0] == signature, "the current signature must be re-recorded"
    assert after[0][1] == 1, "the re-walk must run to completion"
    assert after[0][2] > backfill_rows, "history must actually re-walk after a definition change"


def test_timeline_context_returns_neighboring_events(warehouse: PostgresWarehouse) -> None:
    # timeline.context(ref, before, after) turns a search hit into a readable
    # conversation: the surrounding events of the same (source, context)
    # stream, ordered by time, anchored on the ref the search functions return.
    _ensure_all_table_groups(warehouse)
    warehouse._set_search_path()

    base = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.insert_slack_conversations(
        [
            _slack_conversation_row(conversation_id="C1", conversation_type="private_channel", sync_version=1),
            _slack_conversation_row(conversation_id="C2", conversation_type="private_channel", sync_version=1),
        ]
    )
    warehouse.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id="C1",
                message_ts="100.1",
                message_datetime=base,
                text="first: agenda for the offsite",
            ),
            _slack_message_row(
                conversation_id="C1",
                message_ts="100.2",
                message_datetime=base + timedelta(minutes=1),
                text="second: the vexillology budget is approved",
            ),
            _slack_message_row(
                conversation_id="C1",
                message_ts="100.3",
                message_datetime=base + timedelta(minutes=2),
                text="third: lunch afterwards",
            ),
            _slack_message_row(
                conversation_id="C2",
                message_ts="200.1",
                message_datetime=base + timedelta(minutes=1),
                text="unrelated channel chatter",
            ),
        ]
    )
    _sync_timeline(warehouse)

    ref_rows = warehouse._query("SELECT ref FROM @search_text_exact('vexillology', 5)")
    assert ref_rows, "expected the anchor hit"
    ref = ref_rows[0][0]

    rows = warehouse._query(
        "SELECT snippet FROM @timeline_context(%s, 5, 5) ORDER BY event_ts, seq", (ref,)
    )
    snippets = [row[0] for row in rows]
    assert len(snippets) == 3, f"expected exactly the three C1 messages, got {snippets}"
    assert "first" in snippets[0] and "vexillology" in snippets[1] and "third" in snippets[2]
    assert not any("unrelated" in s for s in snippets)

    # before/after bounds are honored.
    rows = warehouse._query("SELECT snippet FROM @timeline_context(%s, 0, 1)", (ref,))
    assert len(rows) == 2

    # A ref that resolves to nothing is loud, not empty.
    with pytest.raises(psycopg2.Error, match="no timeline event"):
        warehouse._query("SELECT * FROM @timeline_context('slack_message:nope', 1, 1)")
    with pytest.raises(psycopg2.Error, match="ref must look like"):
        warehouse._query("SELECT * FROM @timeline_context('not-a-ref', 1, 1)")


def test_search_text_excludes_internal_agent_run_events(warehouse: PostgresWarehouse) -> None:
    # agent_run_events holds the warehouse's OWN internal enrichment-agent
    # operational logs: its `text` column is raw JSON / stderr for every event
    # type (item.completed, turn.started, error, ...), never human-readable
    # content. Surfacing it in the cross-source search_text() only injects raw
    # JSON noise that crowds out real matches. The agent's actual output is
    # already searchable via the enrichment tables and the agent_session source,
    # so the internal agent branch must be excluded entirely.
    if not _pg_textsearch_usable(warehouse):
        pytest.skip("pg_textsearch is not installed/preloaded on this Postgres host")

    _ensure_all_table_groups(warehouse)
    warehouse._set_search_path()

    created_at = datetime(2026, 5, 19, 12, tzinfo=UTC)
    # Raw-JSON event text mirroring what production stores in agent_run_events.
    warehouse.insert_agent_run_events(
        [
            {
                "run_id": "agent-zzz",
                "event_index": 0,
                "stream": "stdout",
                "event_type": "item.completed",
                "event_json": '{"type":"item.completed"}',
                "text": '{"type":"item.completed","item":{"text":"zanzibar rollout plan"}}',
                "created_at": created_at,
                "sync_version": 1,
            }
        ]
    )
    # A real human source carrying the same term, to prove search still works.
    warehouse.insert_slack_conversations(
        [_slack_conversation_row(conversation_id="C1", conversation_type="private_channel", sync_version=1)]
    )
    warehouse.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id="C1",
                message_ts="200.1",
                message_datetime=created_at,
                text="planning the zanzibar rollout schedule",
            )
        ]
    )
    _sync_timeline(warehouse)

    matched = warehouse._query(
        "SELECT DISTINCT source FROM @search_text('zanzibar rollout', 50) WHERE score < 0"
    )
    sources = {row[0] for row in matched}
    assert "slack" in sources
    assert "agent" not in sources

    # 'agent' is not a valid source token — the internal agent branch was removed
    # and the agent *sessions* token is 'agent_session' — so explicitly
    # requesting it now raises (unknown-source guard) rather than silently
    # returning nothing.
    with pytest.raises(psycopg2.Error, match="unknown source"):
        warehouse._query("SELECT count(*) FROM @search_text('zanzibar', 50, ARRAY['agent'])")


def test_search_text_alter_sql_is_prequalified_and_executed_raw() -> None:
    # The ALTER that pins search_text()'s search_path is a list of physical
    # schema names, not relation references. It carries no @markers, so it must
    # already be complete when it is issued — and it goes through _raw_command
    # so nothing can reinterpret it on the way out.
    import inspect

    from personal_data_warehouse.relations import expand_relations

    wh = PostgresWarehouse.__new__(PostgresWarehouse)
    wh._schema = "public"
    statement = wh._search_text_alter_sql()
    assert statement.startswith('ALTER FUNCTION "timeline"."search_text"(')
    assert "SET search_path TO" in statement
    assert expand_relations(statement, namespace="public") == statement
    assert "_raw_command(self._search_text_alter_sql())" in inspect.getsource(
        PostgresWarehouse._ensure_search_text_function
    )


def test_search_text_returns_hits_under_default_search_path(warehouse: PostgresWarehouse) -> None:
    # The app's Go query sessions (pdw sql, the MCP connector, the timeline UI)
    # never set a search path, so the function must not depend on the caller's:
    # to_bm25query() resolves the timeline BM25 index by name and the EXECUTE'd
    # branch SQL resolves the search_text_hit row type through search_path. The
    # function pins its own path (proconfig); without that, the per-branch
    # exception guard swallows the lookup errors and every search silently
    # returns zero rows.
    if not _pg_textsearch_usable(warehouse):
        pytest.skip("pg_textsearch is not installed/preloaded on this Postgres host")

    _ensure_all_table_groups(warehouse)
    warehouse._command(
        """
        INSERT INTO @timeline_events (adapter, event_id, source, kind, event_ts, source_table,
                                     search_text, priority, actor, title, snippet, context)
        VALUES ('slack_message', 'dp1', 'slack', 'message', now(), 'slack_messages',
                'zanzibar default path probe', 'cc', 'a', 't', 's', 'c')
        """
    )
    warehouse._command('SET search_path TO "$user", public')
    try:
        rows = warehouse._query(
            "SELECT ref FROM @search_text('zanzibar', 10, ARRAY['slack']) WHERE score < 0"
        )
    finally:
        warehouse._set_search_path()
    assert rows == [("slack_message:dp1",)]


def test_search_text_caps_hit_text_to_preview(warehouse: PostgresWarehouse) -> None:
    # A search hit's `text` is a relevance PREVIEW, not the full document. Some
    # branches read multi-megabyte columns (Google Drive doc text, large email /
    # attachment bodies); carrying them untrimmed makes search_text() array_agg
    # tens of MB per branch into its intermediate plpgsql array, slow enough to
    # trip the gateway timeout for common terms. Every branch must therefore cap
    # the text it contributes to SEARCH_TEXT_PREVIEW_CHARS.
    if not _pg_textsearch_usable(warehouse):
        pytest.skip("pg_textsearch is not installed/preloaded on this Postgres host")

    _ensure_all_table_groups(warehouse)
    warehouse._set_search_path()

    created_at = datetime(2026, 5, 19, 12, tzinfo=UTC)
    long_body = "zanzibar " + ("padding " * (SEARCH_TEXT_PREVIEW_CHARS // 4))
    assert len(long_body) > SEARCH_TEXT_PREVIEW_CHARS
    short_body = "zanzibar rollout quick note"
    warehouse.insert_slack_conversations(
        [_slack_conversation_row(conversation_id="C1", conversation_type="private_channel", sync_version=1)]
    )
    warehouse.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id="C1",
                message_ts="300.1",
                message_datetime=created_at,
                text=long_body,
            ),
            _slack_message_row(
                conversation_id="C1",
                message_ts="300.2",
                message_datetime=created_at,
                text=short_body,
            ),
        ]
    )
    _sync_timeline(warehouse)

    rows = warehouse._query(
        "SELECT ref, text FROM @search_text('zanzibar', 50, ARRAY['slack']) WHERE score < 0"
    )
    by_ref = {row[0]: row[1] for row in rows}
    long_ref = next(ref for ref in by_ref if ref.endswith("300.1"))
    short_ref = next(ref for ref in by_ref if ref.endswith("300.2"))

    # The oversized hit is truncated to exactly the preview cap, and the preview
    # is a genuine prefix of the stored text (no corruption, no padding).
    assert len(by_ref[long_ref]) == SEARCH_TEXT_PREVIEW_CHARS
    assert long_body[:100] in by_ref[long_ref]
    # A normal-length hit is returned with timeline context and is not truncated.
    assert short_body in by_ref[short_ref]
    assert len(by_ref[short_ref]) < SEARCH_TEXT_PREVIEW_CHARS


def test_search_text_low_volume_source_survives_high_volume_source(warehouse: PostgresWarehouse) -> None:
    # A low-volume source (one matching contact card) must surface in a bare
    # cross-source search even when a high-volume source (many slack hits) would
    # dominate a flat global score LIMIT. The merge guarantees each source's top
    # hits ahead of the cut;
    # without that, the single contact card ranked far down the global list and a
    # "who is X" question wrongly returned nothing from contacts.
    if not _pg_textsearch_usable(warehouse):
        pytest.skip("pg_textsearch is not installed/preloaded on this Postgres host")

    _ensure_all_table_groups(warehouse)
    warehouse._set_search_path()

    created_at = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.insert_slack_conversations(
        [_slack_conversation_row(conversation_id="C1", conversation_type="private_channel", sync_version=1)]
    )
    # Many slack hits for the term — the high-volume source.
    warehouse.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id="C1",
                message_ts=f"500.{i}",
                message_datetime=created_at,
                text="zanzibar zanzibar rollout planning thread",
            )
            for i in range(8)
        ]
    )
    # Exactly one matching contact card — the low-volume source that a flat
    # global score LIMIT would bury under the slack hits.
    warehouse.insert_contact_cards(
        [_contact_card_row(card_id="card-zan", display_name="Zanzibar Person", sync_version=1)]
    )
    _sync_timeline(warehouse)

    # A small max_results makes the global race tight: the per-source floor must
    # still let the lone contact hit through.
    sources = {
        row[0]
        for row in warehouse._query(
            "SELECT source FROM @search_text('zanzibar', 4) WHERE score < 0"
        )
    }
    assert "contact" in sources, (
        "low-volume 'contact' source was starved out of the cross-source merge by "
        "the high-volume 'slack' source"
    )
    assert "slack" in sources


def test_whatsapp_client_session_round_trips_binary_snapshot(warehouse: PostgresWarehouse) -> None:
    now = datetime(2026, 6, 14, 12, tzinfo=UTC)
    payload = b"SQLite format 3\x00binary\x00session"

    summary = warehouse.upsert_whatsapp_client_session(
        account="zach@example.com",
        session_key="default",
        client_id="client-id",
        database_bytes=payload,
        updated_at=now,
    )
    row = warehouse.get_whatsapp_client_session(account="zach@example.com", session_key="default")

    assert summary["database_sha256"] == hashlib.sha256(payload).hexdigest()
    assert row is not None
    assert row["client_id"] == "client-id"
    assert row["database_bytes"] == payload
    assert row["database_sha256"] == hashlib.sha256(payload).hexdigest()
    assert row["database_bytes_size"] == len(payload)


def test_postgres_contacts_tables_use_jsonb_without_changing_existing_raw_json(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_contacts_tables()
    warehouse.ensure_slack_tables()

    contact_rel = _physical_relation(warehouse, "contact_cards")
    slack_rel = _physical_relation(warehouse, "slack_conversations")
    raw_rows = warehouse._query(
        """
        SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        WHERE (
            table_schema = %s AND table_name = %s AND column_name IN ('emails', 'nicknames', 'raw_json')
          ) OR (
            table_schema = %s AND table_name = %s AND column_name = 'raw_json'
          )
        ORDER BY table_schema, table_name, column_name
        """,
        (contact_rel.schema, contact_rel.name, slack_rel.schema, slack_rel.name),
    )
    logical_by_physical = {
        (contact_rel.schema, contact_rel.name): "contact_cards",
        (slack_rel.schema, slack_rel.name): "slack_conversations",
    }
    rows = sorted(
        (
            logical_by_physical[(schema, table)],
            column,
            data_type,
        )
        for schema, table, column, data_type in raw_rows
    )

    assert rows == [
        ("contact_cards", "emails", "jsonb"),
        ("contact_cards", "nicknames", "jsonb"),
        ("contact_cards", "raw_json", "jsonb"),
        ("slack_conversations", "raw_json", "text"),
    ]


def test_postgres_contacts_view_appends_new_nicknames_column_on_existing_view(
    warehouse: PostgresWarehouse,
) -> None:
    warehouse._ensure_table_group(["contact_cards", "contact_sync_state"])
    warehouse._command("ALTER TABLE @contact_cards ADD COLUMN IF NOT EXISTS nicknames jsonb NOT NULL DEFAULT '[]'::jsonb")
    warehouse._command(
        """
        CREATE OR REPLACE VIEW @clean_contacts AS
        SELECT
            source,
            account,
            source_kind,
            address_book_id,
            card_id,
            etag,
            source_uid,
            display_name,
            given_name,
            family_name,
            organization,
            job_title,
            primary_email,
            primary_phone,
            emails,
            phones,
            addresses,
            organizations,
            urls,
            groups,
            dates,
            photos,
            notes,
            source_updated_at,
            synced_at,
            raw_json
        FROM @contact_cards
        WHERE is_deleted = 0
        """
    )

    warehouse.ensure_contacts_tables()

    columns = [
        row[0]
        for row in warehouse._query(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (_physical_relation(warehouse, "clean_contacts").schema, _physical_relation(warehouse, "clean_contacts").name),
        )
    ]
    assert columns[-2:] == ["raw_json", "nicknames"]


def test_postgres_contact_cards_upsert_jsonb_and_clean_view(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_contacts_tables()

    warehouse.insert_contact_cards([
        _contact_card_row(
            card_id="people/c1",
            display_name="New Name",
            sync_version=20,
            nicknames=[{"value": "N"}],
        )
    ])
    warehouse.insert_contact_cards([
        _contact_card_row(card_id="people/c1", display_name="Old Name", sync_version=10)
    ])
    warehouse.insert_contact_cards([
        _contact_card_row(card_id="people/c2", display_name="Deleted", sync_version=20, is_deleted=1)
    ])

    rows = warehouse._query(
        """
        SELECT display_name, emails #>> '{0,value}', nicknames #>> '{0,value}', raw_json ->> 'resourceName'
        FROM @clean_contacts
        ORDER BY card_id
        """
    )

    assert rows == [("New Name", "people/c1@example.test", "N", "people/c1")]


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
        FROM @clean_contacts
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
        FROM @contact_cards
        WHERE card_id = 'people/c1'
        """
    )
    clean_rows = warehouse._query("SELECT count(*) FROM @clean_contacts WHERE card_id = 'people/c1'")

    assert rows == [(1, "true")]
    assert clean_rows == [(0,)]


def test_ensure_view_replaces_view_whose_columns_cannot_be_dropped(warehouse: PostgresWarehouse) -> None:
    # CREATE OR REPLACE VIEW cannot drop a column, and this database is shared:
    # another checkout running a different revision leaves views whose shape does
    # not match this code's. _ensure_view must recreate rather than wedge.
    warehouse._command("CREATE VIEW @clean_photos AS SELECT 1 AS a, 2 AS b")

    warehouse._ensure_view(
        "clean_photos",
        "CREATE OR REPLACE VIEW @clean_photos AS SELECT 1 AS a",
    )

    assert warehouse._query("SELECT * FROM @clean_photos") == [(1,)]


def test_postgres_ensure_contacts_recovers_when_existing_view_has_extra_columns(
    warehouse: PostgresWarehouse,
) -> None:
    warehouse.ensure_contacts_tables()

    # Reproduce out-of-band drift: another checkout ran a newer definition
    # against the shared database, leaving clean_contacts with a trailing
    # column this code's definition does not select. CREATE OR REPLACE VIEW
    # cannot drop view columns, so every ensure used to fail until the
    # definitions matched again.
    viewdef = warehouse._query(
        "SELECT pg_get_viewdef(to_regclass(%s))",
        (warehouse.sql_relation("clean_contacts"),),
    )[0][0]
    warehouse._command("DROP VIEW @clean_contact_points")
    warehouse._command("DROP VIEW @clean_contacts")
    warehouse._command(
        "CREATE VIEW @clean_contacts AS "
        f"SELECT base.*, 'drift'::text AS drift_extra FROM ({viewdef.strip().rstrip(';')}) AS base"
    )

    warehouse.ensure_contacts_tables()

    columns = [
        row[0]
        for row in warehouse._query(
            """
            SELECT attname
            FROM pg_attribute
            WHERE attrelid = to_regclass(%s) AND attnum > 0 AND NOT attisdropped
            ORDER BY attnum
            """,
            (warehouse.sql_relation("clean_contacts"),),
        )
    ]
    assert "drift_extra" not in columns
    assert columns[-2:] == ["raw_json", "nicknames"]


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
        FROM @contact_cards
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

    rel = _physical_relation(warehouse, "slack_conversation_stats")
    rows = warehouse._query(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
        """,
        (rel.schema, rel.name),
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
        FROM @slack_conversation_members
        ORDER BY conversation_id, user_id
        """
    )

    assert rows == [
        ("G1", "U1", 1, new_sync, 2),
        ("G1", "U2", 0, new_sync, 2),
        ("G1", "U3", 0, new_sync, 2),
        ("G2", "U9", 0, old_sync, 1),
    ]


def test_postgres_slack_sync_state_preserves_cursor_ts_when_error_write_has_empty_cursor(
    warehouse: PostgresWarehouse,
) -> None:
    first_sync = datetime(2026, 5, 18, 12, tzinfo=UTC)
    error_sync = datetime(2026, 5, 19, 12, tzinfo=UTC)
    key = ("zrl", "T1", "conversation", "C-cursor")
    warehouse.ensure_slack_tables()

    warehouse.insert_slack_sync_state(
        account=key[0],
        team_id=key[1],
        object_type=key[2],
        object_id=key[3],
        cursor_ts="1700.0001",
        last_sync_type="messages",
        status="ok",
        error="",
        updated_at=first_sync,
        sync_version=1,
    )
    warehouse.insert_slack_sync_state(
        account=key[0],
        team_id=key[1],
        object_type=key[2],
        object_id=key[3],
        cursor_ts="",
        last_sync_type="messages",
        status="error",
        error="channel_not_found",
        updated_at=error_sync,
        sync_version=2,
    )

    state = warehouse.load_slack_sync_state()[key]
    assert state["cursor_ts"] == "1700.0001"
    assert state["status"] == "error"
    assert state["error"] == "channel_not_found"


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
    warehouse._command("TRUNCATE @slack_conversation_stats")

    warehouse.rebuild_slack_conversation_stats()

    rows = warehouse._query(
        """
        SELECT conversation_id, message_count, latest_message_at
        FROM @slack_conversation_stats
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
    warehouse._command("TRUNCATE @slack_conversation_stats")

    warehouse.ensure_slack_tables()

    rows = warehouse._query(
        "SELECT conversation_id, message_count, latest_message_at FROM @slack_conversation_stats",
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
        "SELECT message_count, latest_message_at FROM @slack_conversation_stats WHERE conversation_id = %s",
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
        "SELECT message_count, latest_message_at FROM @slack_conversation_stats WHERE conversation_id = %s",
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
        "SELECT message_count, latest_message_at FROM @slack_conversation_stats WHERE conversation_id = %s",
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


def test_postgres_mark_slack_conversation_inactive_excludes_it_from_active_loads(
    warehouse: PostgresWarehouse,
) -> None:
    warehouse.ensure_slack_tables()
    warehouse.insert_slack_conversations(
        [
            _slack_conversation_row(conversation_id="C-gone", raw_json='{"id":"C-gone"}'),
            _slack_conversation_row(conversation_id="C-live", raw_json='{"id":"C-live"}'),
        ]
    )

    warehouse.mark_slack_conversation_inactive(account="zrl", team_id="T1", conversation_id="C-gone")

    active = warehouse.load_slack_conversation_payloads(account="zrl", team_id="T1")
    assert active == [{"id": "C-live"}]

    archived = warehouse.load_slack_conversation_payloads(
        account="zrl", team_id="T1", archived_only=True
    )
    assert archived == [{"id": "C-gone"}]

    # Re-discovering the channel as active (is_archived=0) self-heals it.
    warehouse.insert_slack_conversations(
        [_slack_conversation_row(conversation_id="C-gone", raw_json='{"id":"C-gone"}')]
    )
    healed = warehouse.load_slack_conversation_payloads(account="zrl", team_id="T1")
    assert {payload["id"] for payload in healed} == {"C-gone", "C-live"}


class _RecordingSlackClient:
    """Minimal Slack client for end-to-end runner tests against a real warehouse."""

    def __init__(self, responses):
        self._responses = {method: list(values) for method, values in responses.items()}
        self.calls = []

    def call(self, method, **params):
        self.calls.append((method, params))
        values = self._responses.get(method)
        if not values:
            raise AssertionError(f"Unexpected Slack call: {method} {params}")
        value = values.pop(0)
        if isinstance(value, Exception):
            raise value
        return value


def test_freshness_sync_end_to_end_archives_gone_channel_in_real_warehouse(
    warehouse: PostgresWarehouse, monkeypatch: pytest.MonkeyPatch
) -> None:
    # End-to-end: drive the real SlackSyncRunner freshness path through the real
    # PostgresWarehouse. A channel_not_found on one channel must not abort the run;
    # the channel must be archived in the DB and the next channel must still sync.
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse.ensure_slack_tables()
    warehouse.insert_slack_conversations(
        [
            _slack_conversation_row(
                conversation_id="C_GONE",
                conversation_type="public_channel",
                raw_json=json.dumps({"id": "C_GONE", "is_channel": True, "latest": {"ts": "1999.000000"}}),
            ),
            _slack_conversation_row(
                conversation_id="C_OK",
                conversation_type="public_channel",
                raw_json=json.dumps({"id": "C_OK", "is_channel": True, "latest": {"ts": "1995.000000"}}),
            ),
        ]
    )
    client = _RecordingSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                SlackApiCallError("conversations.history failed: channel_not_found", code="channel_not_found"),
                {"ok": True, "messages": [{"ts": "1995.000000", "user": "U4", "text": "hi"}], "response_metadata": {}},
            ],
        }
    )

    summary = SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=logging.getLogger("test-freshness-e2e"),
        client_factory=lambda account: client,
        now=lambda: datetime.fromtimestamp(2000, tz=UTC),
        history_window=timedelta(minutes=10),
        sync_users=False,
        sync_members=False,
        use_existing_conversations=True,
        freshness_priority=True,
        sync_thread_replies=False,
        sleep=lambda seconds: None,
    ).sync_all()[0]

    assert summary.messages_written == 1

    # The dead channel is archived in the real DB; the healthy channel stays active.
    archived = warehouse._query(
        "SELECT conversation_id, is_archived FROM @slack_conversations ORDER BY conversation_id"
    )
    assert dict(archived) == {"C_GONE": 1, "C_OK": 0}

    # Subsequent active loads now skip the archived channel entirely.
    active_ids = {p["id"] for p in warehouse.load_slack_conversation_payloads(account="zrl", team_id="T1")}
    assert active_ids == {"C_OK"}

    # The healthy channel's message was persisted.
    persisted = warehouse._query(
        "SELECT conversation_id FROM @slack_messages WHERE is_deleted = 0 ORDER BY conversation_id"
    )
    assert [row[0] for row in persisted] == ["C_OK"]


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
    assert "FROM @slack_messages" not in captured["sql"]
    assert "GROUP BY account, team_id, conversation_id" not in captured["sql"]


def test_postgres_slack_thread_missing_replies_filter_ignores_tombstones(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    warehouse = object.__new__(PostgresWarehouse)
    captured: dict[str, object] = {}

    def fake_query(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(warehouse, "_query", fake_query)

    warehouse.load_slack_thread_parent_refs(
        account="zrl",
        team_id="T1",
        missing_replies_only=True,
        order="oldest",
        limit=5,
    )

    assert "NOT EXISTS" in str(captured["sql"])
    assert "r.thread_ts = m.message_ts" in str(captured["sql"])
    assert "AND r.is_deleted = 0" in str(captured["sql"])
    assert "AND r.is_thread_reply = 1" in str(captured["sql"])
    assert "ORDER BY m.message_datetime ASC, m.message_ts ASC" in str(captured["sql"])
    assert captured["params"] == ("zrl", "T1", 5)


def test_postgres_slack_thread_parent_refs_bound_since_on_indexed_datetime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The numeric message_ts cutoff cannot use an index (it is an expression on
    # a text column), so the candidate query seq-scanned all 42M messages —
    # ~46 GB of buffer reads every ~5 minutes in production. The equivalent
    # message_datetime bound lets the thread-parents partial index range-scan
    # and stop at the cutoff.
    warehouse = object.__new__(PostgresWarehouse)
    captured: dict[str, object] = {}

    def fake_query(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(warehouse, "_query", fake_query)

    warehouse.load_slack_thread_parent_refs(
        account="zrl",
        team_id="T1",
        since_ts=1777000000.5,
        limit=5,
    )

    sql = str(captured["sql"])
    assert "m.message_datetime >= to_timestamp(%s)" in sql
    assert captured["params"] == ("zrl", "T1", 1777000000.5, 1777000000.5, 5)


def test_postgres_slack_thread_parent_refs_since_filters_rows(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_slack_tables()
    warehouse.insert_slack_conversations(
        [_slack_conversation_row(conversation_id="C1", raw_json='{"id":"C1"}')]
    )
    warehouse.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id="C1",
                message_ts="1770000000.000001",
                message_datetime=datetime(2026, 2, 1, 12, tzinfo=UTC),
                reply_count=1,
            ),
            _slack_message_row(
                conversation_id="C1",
                message_ts="1780000000.000001",
                message_datetime=datetime(2026, 5, 28, 12, tzinfo=UTC),
                reply_count=1,
            ),
        ]
    )

    refs = warehouse.load_slack_thread_parent_refs(
        account="zrl", team_id="T1", since_ts=1775000000.0
    )

    assert [ref["thread_ts"] for ref in refs] == ["1780000000.000001"]


def test_postgres_slack_thread_parent_refs_exclude_gone_conversations(
    warehouse: PostgresWarehouse,
) -> None:
    # Once a conversation is marked inactive (channel_not_found etc.), none of its
    # thread parents should ever be offered up for backfill again — trying them
    # wastes an API call that is guaranteed to fail the exact same way every time.
    warehouse.ensure_slack_tables()
    now = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.insert_slack_conversations(
        [
            _slack_conversation_row(conversation_id="C-gone", raw_json='{"id":"C-gone"}'),
            _slack_conversation_row(conversation_id="C-live", raw_json='{"id":"C-live"}'),
        ]
    )
    warehouse.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id="C-gone",
                message_ts="1770000000.000001",
                message_datetime=now,
                reply_count=1,
            ),
            _slack_message_row(
                conversation_id="C-live",
                message_ts="1770000000.000002",
                message_datetime=now,
                reply_count=1,
            ),
        ]
    )

    warehouse.mark_slack_conversation_inactive(account="zrl", team_id="T1", conversation_id="C-gone")

    refs = warehouse.load_slack_thread_parent_refs(account="zrl", team_id="T1")

    assert [ref["conversation_id"] for ref in refs] == ["C-live"]


def _insert_sync_state(
    warehouse: PostgresWarehouse,
    *,
    object_type: str,
    object_id: str,
    status: str,
    error: str = "",
) -> None:
    warehouse.insert_slack_sync_state(
        account="zrl",
        team_id="T1",
        object_type=object_type,
        object_id=object_id,
        cursor_ts="",
        last_sync_type="thread_replies" if object_type == "thread" else "partial",
        status=status,
        error=error,
        updated_at=datetime(2026, 5, 19, 12, tzinfo=UTC),
        sync_version=1,
    )


def test_postgres_slack_thread_parent_refs_skip_gone_but_retry_errors(
    warehouse: PostgresWarehouse,
) -> None:
    # skip_known_errors permanently drops threads recorded as 'gone' (deleted
    # parent / dead channel — retrying is a guaranteed identical failure) but
    # keeps offering threads whose last attempt was a transient 'error', so
    # they self-heal instead of freezing in the failing count forever.
    warehouse.ensure_slack_tables()
    now = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.insert_slack_conversations(
        [
            _slack_conversation_row(conversation_id=cid, raw_json=f'{{"id":"{cid}"}}')
            for cid in ("C-gone", "C-error", "C-fresh")
        ]
    )
    warehouse.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id=cid,
                message_ts=ts,
                message_datetime=now,
                reply_count=1,
            )
            for cid, ts in (
                ("C-gone", "1770000000.000001"),
                ("C-error", "1770000000.000002"),
                ("C-fresh", "1770000000.000003"),
            )
        ]
    )
    _insert_sync_state(
        warehouse,
        object_type="thread",
        object_id="C-gone:1770000000.000001",
        status="gone",
        error="conversations.replies failed: thread_not_found",
    )
    _insert_sync_state(
        warehouse,
        object_type="thread",
        object_id="C-error:1770000000.000002",
        status="error",
        error="conversations.replies failed: internal_error",
    )

    refs = warehouse.load_slack_thread_parent_refs(account="zrl", team_id="T1", skip_known_errors=True)

    assert sorted(ref["conversation_id"] for ref in refs) == ["C-error", "C-fresh"]


def test_postgres_slack_conversation_payloads_skip_gone_but_retry_errors(
    warehouse: PostgresWarehouse,
) -> None:
    warehouse.ensure_slack_tables()
    warehouse.insert_slack_conversations(
        [
            _slack_conversation_row(conversation_id=cid, raw_json=f'{{"id":"{cid}"}}')
            for cid in ("C-gone", "C-error", "C-fresh")
        ]
    )
    _insert_sync_state(
        warehouse,
        object_type="conversation",
        object_id="C-gone",
        status="gone",
        error="conversations.history failed: channel_not_found",
    )
    _insert_sync_state(
        warehouse,
        object_type="conversation",
        object_id="C-error",
        status="error",
        error="conversations.history failed: fatal_error",
    )

    payloads = warehouse.load_slack_conversation_payloads(account="zrl", team_id="T1", skip_known_errors=True)

    assert sorted(p["id"] for p in payloads) == ["C-error", "C-fresh"]


def test_postgres_slack_member_candidates_skip_gone_but_retry_errors(
    warehouse: PostgresWarehouse,
) -> None:
    warehouse.ensure_slack_tables()
    warehouse.insert_slack_conversations(
        [
            _slack_conversation_row(
                conversation_id=cid,
                conversation_type="private_channel",
                raw_json=f'{{"id":"{cid}"}}',
            )
            for cid in ("G-gone", "G-error", "G-fresh")
        ]
    )
    _insert_sync_state(
        warehouse,
        object_type="conversation_members",
        object_id="G-gone",
        status="gone",
        error="conversations.members failed: channel_not_found",
    )
    _insert_sync_state(
        warehouse,
        object_type="conversation_members",
        object_id="G-error",
        status="error",
        error="conversations.members failed: internal_error",
    )

    payloads = warehouse.load_slack_member_sync_candidate_payloads(account="zrl", team_id="T1", skip_known_errors=True)

    assert sorted(p["id"] for p in payloads) == ["G-error", "G-fresh"]


def test_postgres_ensure_slack_tables_reclassifies_legacy_gone_errors(
    warehouse: PostgresWarehouse,
) -> None:
    # Rows written before the 'gone' status existed recorded terminal
    # channel/thread-gone failures as status 'error', which the pipeline health
    # dashboard counts as actively failing forever (they are never retried, so
    # nothing ever resolves them). ensure_slack_tables reclassifies them in
    # place; transient errors are left alone for retry.
    warehouse.ensure_slack_tables()
    legacy = {
        "C-dead:1770000000.000001": ("thread", "conversations.replies failed: channel_not_found"),
        "C-dead-thread:1770000000.000002": ("thread", "conversations.replies failed: thread_not_found"),
        "C-left": ("conversation", "conversations.history failed: not_in_channel"),
        "C-archived": ("conversation", "conversations.history failed: is_archived"),
        "G-dead": ("conversation_members", "conversations.members failed: channel_not_found"),
        "C-flaky": ("conversation", "conversations.history failed: fatal_error"),
    }
    for object_id, (object_type, error) in legacy.items():
        _insert_sync_state(
            warehouse,
            object_type=object_type,
            object_id=object_id,
            status="error",
            error=error,
        )

    warehouse.ensure_slack_tables()

    states = warehouse.load_slack_sync_state()
    by_id = {key[3]: state for key, state in states.items() if key[3] in legacy}
    assert by_id["C-dead:1770000000.000001"]["status"] == "gone"
    assert by_id["C-dead-thread:1770000000.000002"]["status"] == "gone"
    assert by_id["C-left"]["status"] == "gone"
    assert by_id["C-archived"]["status"] == "gone"
    assert by_id["G-dead"]["status"] == "gone"
    # The recorded failure itself is preserved as the reason the object is gone.
    assert by_id["C-left"]["error"] == "conversations.history failed: not_in_channel"
    # A transient failure stays a retryable error.
    assert by_id["C-flaky"]["status"] == "error"


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
    assert "FROM @slack_messages" not in captured["sql"]
    assert "GROUP BY account, team_id, conversation_id" not in captured["sql"]


def test_postgres_slack_account_state_query_does_not_materialize_recent_messages(warehouse: PostgresWarehouse) -> None:
    sql = warehouse._slack_account_state_items_select_sql()

    assert "recent_messages AS NOT MATERIALIZED" in sql
    assert "current_conversations AS NOT MATERIALIZED" in sql


def test_postgres_existing_slack_message_ids_only_returns_top_level_messages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    warehouse = object.__new__(PostgresWarehouse)
    captured: dict[str, object] = {}

    def fake_query(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return [("1713974400.000100",)]

    monkeypatch.setattr(warehouse, "_query", fake_query)

    message_ids = warehouse.existing_slack_message_ids(
        account="zrl",
        team_id="T1",
        conversation_id="C1",
        oldest_ts="1713974000.000000",
        latest_ts="1713975000.000000",
    )

    assert message_ids == {"1713974400.000100"}
    assert "AND is_deleted = 0" in str(captured["sql"])
    assert "AND is_thread_reply = 0" in str(captured["sql"])
    assert captured["params"] == ("zrl", "T1", "C1", 1713974000.0, 1713975000.0)


def test_postgres_message_upsert_preserves_latest_tombstone(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()

    warehouse.insert_messages([_message_row(message_id="m1", subject="live", labels=["INBOX"], sync_version=10)])
    warehouse.insert_messages([_message_row(message_id="m1", subject="deleted", labels=[], sync_version=20, is_deleted=1)])

    assert warehouse.existing_message_ids(account="zach@example.test", message_ids=["m1"]) == set()
    rows = warehouse._query("SELECT subject, is_deleted, sync_version FROM @gmail_messages WHERE message_id = %s", ("m1",))
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
        FROM @clean_gmail_inbox
        """
    )

    assert len(rows) == 1
    assert rows[0][0:5] == ("thread-1", "newer", "unread", 1, 0)
    assert "body_markdown_clean" in rows[0][5]


def test_postgres_gmail_clean_inbox_preview_uses_byte_prefix(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()
    preview = ("a" * 998) + "€" + "after"
    expected = preview.encode("utf-8")[:1000].decode("utf-8", errors="ignore")
    row = _message_row(message_id="m1", subject="subject", labels=["INBOX"], sync_version=10)
    row["body_markdown_clean"] = preview

    warehouse.insert_messages([row])

    rows = warehouse._query("SELECT latest_preview FROM @clean_gmail_inbox")

    assert rows == [(expected,)]


def test_postgres_gmail_clean_inbox_ties_latest_message_by_lowest_message_id(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()
    lower = _message_row(message_id="a", subject="lower", labels=["INBOX"], sync_version=10)
    higher = _message_row(message_id="b", subject="higher", labels=["INBOX"], sync_version=11)

    warehouse.insert_messages([higher, lower])

    rows = warehouse._query("SELECT subject FROM @clean_gmail_inbox")

    assert rows == [("lower",)]


def test_postgres_calendar_transcript_views_use_latest_grouping(warehouse: PostgresWarehouse) -> None:
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
                source="apple_voice_memos",
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
                source="apple_voice_memos",
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
        FROM @clean_calendar_with_transcripts
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
                source="apple_voice_memos",
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
    assert warehouse._query("SELECT content_sha256 FROM @apple_voice_memos_transcription_runs") == [("",)]

    warehouse.ensure_apple_voice_memos_tables()
    assert warehouse._query("SELECT content_sha256 FROM @apple_voice_memos_transcription_runs") == [("audio-hash",)]


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

    latest = warehouse._query("SELECT latest_revision_id, title FROM @apple_notes WHERE note_id = %s", ("note-1",))
    revisions = warehouse._query("SELECT revision_id FROM @apple_note_revisions WHERE note_id = %s ORDER BY revision_id", ("note-1",))
    attachments = warehouse._query("SELECT attachment_id FROM @apple_note_attachments WHERE note_id = %s", ("note-1",))

    assert latest == [("rev-new", "new")]
    assert revisions == [("rev-new",), ("rev-old",)]
    assert attachments == [("att-1",)]


def test_postgres_slack_account_state_uses_empty_actor_for_missing_user(warehouse: PostgresWarehouse) -> None:
    now = datetime.now(tz=UTC)
    message_ts = f"{int(now.timestamp())}.000001"
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
                message_ts=message_ts,
                message_datetime=now,
                thread_ts=message_ts,
                user_id="U_MISSING",
                text="hello",
                raw_json="{}",
                synced_at=now,
                sync_version=1,
            )
        ]
    )

    warehouse.refresh_slack_account_state_items(account="zrl", team_id="T1", synced_at=now)

    assert warehouse._query("SELECT actor_name FROM @slack_account_state_item_rows WHERE is_deleted = 0") == [("",)]


def _seed_slack_inbox_scope(warehouse: PostgresWarehouse, *, now: datetime) -> None:
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


def _seed_slack_dm(
    warehouse: PostgresWarehouse,
    *,
    conversation_id: str,
    now: datetime,
    synced_at: datetime,
    message_ts: str | None = None,
    is_deleted: int = 0,
) -> None:
    message_ts = message_ts or f"{int(now.timestamp())}.{conversation_id[-1]}00001"
    warehouse.insert_slack_conversations(
        [
            _default_row(
                SLACK_CONVERSATION_COLUMNS,
                account="zrl",
                team_id="T1",
                conversation_id=conversation_id,
                conversation_type="im",
                is_im=1,
                raw_json='{"last_read":"0"}',
                created_at=now,
                synced_at=synced_at,
                sync_version=int(synced_at.timestamp()),
            )
        ]
    )
    warehouse.insert_slack_messages(
        [
            _default_row(
                SLACK_MESSAGE_COLUMNS,
                account="zrl",
                team_id="T1",
                conversation_id=conversation_id,
                message_ts=message_ts,
                message_datetime=now,
                thread_ts=message_ts,
                user_id="U_OTHER",
                text="hello",
                raw_json="{}",
                is_deleted=is_deleted,
                synced_at=synced_at,
                sync_version=int(synced_at.timestamp()),
            )
        ]
    )


def _inbox_rows(warehouse: PostgresWarehouse) -> dict[str, tuple[int, int]]:
    rows = warehouse._query(
        "SELECT container_id, is_deleted, sync_version FROM @slack_account_state_item_rows ORDER BY container_id"
    )
    return {str(container): (int(deleted), int(version)) for container, deleted, version in rows}


def test_postgres_slack_account_state_refresh_only_recomputes_changed_conversations(
    warehouse: PostgresWarehouse,
) -> None:
    now = datetime.now(tz=UTC)
    _seed_slack_inbox_scope(warehouse, now=now)
    _seed_slack_dm(warehouse, conversation_id="D1", now=now, synced_at=now - timedelta(hours=6))

    first = warehouse.refresh_slack_account_state_items(account="zrl", team_id="T1", synced_at=now - timedelta(hours=3))
    assert first.mode == "full"
    before = _inbox_rows(warehouse)
    assert before["D1"][0] == 0

    _seed_slack_dm(warehouse, conversation_id="D2", now=now, synced_at=now)
    second = warehouse.refresh_slack_account_state_items(account="zrl", team_id="T1", synced_at=now)

    assert second.mode == "incremental"
    assert second.changed_conversations == 1
    after = _inbox_rows(warehouse)
    assert after["D2"][0] == 0
    # D1 was not touched since the last refresh, so its row is exactly as it was.
    assert after["D1"] == before["D1"]


def test_postgres_slack_account_state_refresh_tombstones_items_of_changed_conversations(
    warehouse: PostgresWarehouse,
) -> None:
    now = datetime.now(tz=UTC)
    _seed_slack_inbox_scope(warehouse, now=now)
    _seed_slack_dm(warehouse, conversation_id="D1", now=now, synced_at=now - timedelta(hours=6))
    _seed_slack_dm(warehouse, conversation_id="D2", now=now, synced_at=now - timedelta(hours=6))
    warehouse.refresh_slack_account_state_items(account="zrl", team_id="T1", synced_at=now - timedelta(hours=3))

    # The only message in D1 is deleted upstream; D2 is untouched.
    _seed_slack_dm(warehouse, conversation_id="D1", now=now, synced_at=now, is_deleted=1)
    warehouse.refresh_slack_account_state_items(account="zrl", team_id="T1", synced_at=now)

    rows = _inbox_rows(warehouse)
    assert rows["D1"][0] == 1
    assert rows["D2"][0] == 0


def test_postgres_slack_account_state_refresh_ages_out_items_without_touching_their_conversation(
    warehouse: PostgresWarehouse,
) -> None:
    now = datetime.now(tz=UTC)
    _seed_slack_inbox_scope(warehouse, now=now)
    _seed_slack_dm(warehouse, conversation_id="D1", now=now, synced_at=now - timedelta(hours=6))
    warehouse.refresh_slack_account_state_items(account="zrl", team_id="T1", synced_at=now - timedelta(hours=3))
    warehouse._command(
        "UPDATE @slack_account_state_item_rows SET latest_activity_at = %s WHERE container_id = 'D1'",
        (now - timedelta(days=40),),
    )

    result = warehouse.refresh_slack_account_state_items(account="zrl", team_id="T1", synced_at=now)

    assert result.mode == "incremental"
    assert _inbox_rows(warehouse)["D1"][0] == 1


def test_postgres_slack_account_state_refresh_reruns_fully_once_a_day(warehouse: PostgresWarehouse) -> None:
    now = datetime.now(tz=UTC)
    _seed_slack_inbox_scope(warehouse, now=now)
    _seed_slack_dm(warehouse, conversation_id="D1", now=now, synced_at=now - timedelta(days=3))
    warehouse.refresh_slack_account_state_items(account="zrl", team_id="T1", synced_at=now - timedelta(days=2))
    before = _inbox_rows(warehouse)

    result = warehouse.refresh_slack_account_state_items(account="zrl", team_id="T1", synced_at=now)

    assert result.mode == "full"
    after = _inbox_rows(warehouse)
    assert after["D1"][0] == 0
    assert after["D1"][1] > before["D1"][1]


def test_postgres_slack_account_state_refresh_skips_while_another_refresh_holds_the_lock(
    warehouse: PostgresWarehouse,
) -> None:
    now = datetime.now(tz=UTC)
    _seed_slack_inbox_scope(warehouse, now=now)
    _seed_slack_dm(warehouse, conversation_id="D1", now=now, synced_at=now)

    other = psycopg2.connect(_postgres_url())
    other.autocommit = True
    try:
        with other.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_lock(%s)", (SLACK_ACCOUNT_STATE_REFRESH_LOCK_ID,))
        result = warehouse.refresh_slack_account_state_items(account="zrl", team_id="T1", synced_at=now)
    finally:
        other.close()

    assert result.mode == "skipped"
    assert _inbox_rows(warehouse) == {}

    result = warehouse.refresh_slack_account_state_items(account="zrl", team_id="T1", synced_at=now)
    assert result.mode == "full"
    assert _inbox_rows(warehouse)["D1"][0] == 0


def test_files_attachments_mart_conforms_every_attachment_source(warehouse: PostgresWarehouse) -> None:
    """One row per attachment from all four sources, in one column set.

    Every enrichment pass and the receipt evidence check read this view, so a
    source missing here is a source that is silently never enriched -- the
    Apple Notes shape (40 audio attachments, 0 transcripts) that motivated it.
    """
    now = datetime(2026, 8, 20, 12, tzinfo=UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    warehouse.ensure_tables()
    warehouse.ensure_whatsapp_tables()
    warehouse.ensure_apple_messages_tables()
    warehouse.ensure_apple_notes_tables()
    warehouse.insert_attachments(
        [
            _default_row(
                ATTACHMENT_COLUMNS,
                account="z", message_id="gm1", part_id="1", attachment_id="ga1", filename="invoice.pdf",
                mime_type="application/pdf", size=10, content_sha256="sha-g", storage_status="stored",
                internal_date=now, synced_at=now,
            ),
            _default_row(
                ATTACHMENT_COLUMNS,
                account="z", message_id="gm2", part_id="1", attachment_id="ga2", filename="pending.pdf",
                mime_type="application/pdf", size=10, content_sha256="sha-g2", storage_status="pending",
                internal_date=epoch, synced_at=now,
            ),
        ]
    )
    warehouse.insert_whatsapp_media_items(
        [
            _default_row(
                WHATSAPP_MEDIA_ITEM_COLUMNS,
                account="z", chat_id="c1", message_id="wm1", filename="receipt.jpg", mime_type="image/jpeg",
                size_bytes=20, content_sha256="sha-w", is_missing=0, message_at=now, ingested_at=now,
            ),
            _default_row(
                WHATSAPP_MEDIA_ITEM_COLUMNS,
                account="z", chat_id="c1", message_id="wm2", filename="gone.jpg", mime_type="image/jpeg",
                size_bytes=20, content_sha256="", is_missing=1, message_at=now, ingested_at=now,
            ),
        ]
    )
    warehouse.insert_apple_message_attachments(
        [
            _default_row(
                APPLE_MESSAGE_ATTACHMENT_COLUMNS,
                account="z", attachment_id="aa1", message_id="am1", filename="voice.caf", mime_type="audio/x-caf",
                size_bytes=30, content_sha256="sha-a", is_missing=0, created_at=now, ingested_at=now,
            )
        ]
    )
    warehouse.insert_apple_note_revisions(
        [
            _default_row(
                APPLE_NOTE_REVISION_COLUMNS,
                account="z", note_id="n1", revision_id="r1", title="memo", modified_at=now, ingested_at=now,
            )
        ]
    )
    warehouse.insert_apple_note_attachments(
        [
            _default_row(
                APPLE_NOTE_ATTACHMENT_COLUMNS,
                account="z", note_id="n1", revision_id="r1", attachment_id="na1", filename="Recording.m4a",
                content_type="audio/mp4a-latm", size_bytes=40, content_sha256="sha-n", is_missing=0,
                ingested_at=now,
            )
        ]
    )

    rows = {
        (row["source"], row["attachment_id"]): row
        for row in warehouse._query_dicts("SELECT * FROM @marts_files_attachments")
    }
    assert set(rows) == {
        ("gmail", "ga1"), ("gmail", "ga2"), ("whatsapp", "wm1"), ("whatsapp", "wm2"),
        ("apple_messages", "aa1"), ("apple_notes", "na1"),
    }
    assert rows[("gmail", "ga1")]["is_stored"] == 1
    assert rows[("gmail", "ga2")]["is_stored"] == 0
    assert rows[("gmail", "ga2")]["occurred_at"] is None  # epoch sentinel translated
    assert rows[("whatsapp", "wm1")]["is_stored"] == 1
    assert rows[("whatsapp", "wm2")]["is_stored"] == 0
    assert rows[("apple_messages", "aa1")]["parent_id"] == "am1"
    note = rows[("apple_notes", "na1")]
    assert note["parent_id"] == "n1"
    assert note["mime_type"] == "audio/mp4a-latm"
    assert note["occurred_at"] == now
    assert note["is_stored"] == 1
    assert {row["size_bytes"] for row in rows.values()} == {10, 20, 30, 40}
    assert all(row["is_deleted"] == 0 for row in rows.values())


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

    rows = warehouse.load_untranscribed_voice_recordings(provider="assemblyai", limit=1)

    assert [row["recording_id"] for row in rows] == ["rec-1"]
    assert rows[0]["source"] == "apple_voice_memos"


def test_voice_derived_tables_migrate_onto_the_source_key_in_place(
    warehouse: PostgresWarehouse,
) -> None:
    """An existing production table has to move onto the new key by itself.

    CREATE TABLE IF NOT EXISTS never revisits a primary key, so without this
    migration the deployed table would keep upserting on the OLD conflict
    target -- which is not a loud error, it is a silent overwrite of a
    different source's row. Every pre-existing row belongs to Apple Voice
    Memos, because nothing else could have written one.
    """
    warehouse.ensure_apple_voice_memos_tables(backfill_content_hashes=False)
    runs = warehouse.sql_relation("apple_voice_memos_transcription_runs")

    # Rewind to the pre-multi-source shape and plant a row in it. The marts
    # views go with it: on the deployed database they are the OLD definitions,
    # which do not reference `source` at all.
    warehouse._command(
        f"DROP VIEW IF EXISTS {warehouse.sql_relation('marts_voice_memos_recordings')} CASCADE"
    )
    warehouse._command(f"ALTER TABLE {runs} DROP CONSTRAINT transcription_runs_pkey")
    warehouse._command(f"ALTER TABLE {runs} DROP COLUMN source")
    warehouse._command(
        f"ALTER TABLE {runs} ADD PRIMARY KEY (account, recording_id, provider)"
    )
    warehouse._command(
        f"INSERT INTO {runs} (account, recording_id, provider, transcript_text) "
        "VALUES ('zach', 'legacy-1', 'assemblyai', 'legacy transcript')"
    )

    warehouse.ensure_apple_voice_memos_tables(backfill_content_hashes=False)

    assert warehouse._primary_key_columns("apple_voice_memos_transcription_runs") == (
        "source",
        "account",
        "recording_id",
        "provider",
    )
    assert warehouse._query(
        "SELECT source, transcript_text FROM @apple_voice_memos_transcription_runs "
        "WHERE recording_id = 'legacy-1'"
    ) == [("apple_voice_memos", "legacy transcript")]


def test_transcription_candidates_include_every_voice_source(
    warehouse: PostgresWarehouse,
) -> None:
    """The defect, as a test: a second voice source must be transcribable.

    Transcription used to scan base_apple_voice_memos.files, so
    base_alice_voice_recordings -- fully registered, catalogued, pipelined and
    on the timeline -- was never a candidate. It sat at 53 recordings, 0
    transcripts and 0 summaries with every ENFORCED registry green, because no
    registry asks what a transformation READS.
    """
    now = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.ensure_apple_voice_memos_tables(backfill_content_hashes=False)
    warehouse.ensure_alice_voice_recordings_tables()
    warehouse.insert_apple_voice_memos_files(
        [
            _default_row(
                VOICE_MEMO_FILE_COLUMNS,
                account="zach@example.test",
                recording_id="rec-1",
                filename="memo.m4a",
                content_type="audio/mp4",
                size_bytes=123,
                content_sha256="apple-hash",
                recorded_at=now,
                storage_backend="google_drive",
                storage_file_id="drive-apple",
                sync_version=1,
            )
        ]
    )
    warehouse.insert_alice_voice_recordings(
        [
            _default_row(
                ALICE_VOICE_RECORDING_COLUMNS,
                account="zach@example.test",
                recording_id="alice-1",
                filename="walk.m4a",
                content_type="audio/mp4",
                size_bytes=456,
                content_sha256="alice-hash",
                recorded_at=now,
                storage_backend="google_drive",
                storage_file_id="drive-alice",
                ingested_at=now,
                sync_version=1,
            )
        ]
    )

    rows = warehouse.load_untranscribed_voice_recordings(provider="assemblyai", limit=10)

    assert {(row["source"], row["recording_id"]) for row in rows} == {
        ("apple_voice_memos", "rec-1"),
        ("alice_voice_recordings", "alice-1"),
    }
    # Every candidate carries what the runner needs to fetch its own bytes.
    for row in rows:
        assert row["storage_file_id"]
        assert row["filename"]


def test_voice_mart_treats_apple_notes_audio_as_a_voice_source(warehouse: PostgresWarehouse) -> None:
    """Audio saved in Apple Notes is a recording, once, on the newest revision.

    40 audio attachments across 10 notes sat in base_apple_notes.attachments
    with 0 transcripts: uploaded, stored, and voice to nobody. The mart is the
    input to transcription, so listing them here is what gets them transcribed,
    enriched, calendar-matched and onto the timeline with no new code.
    """
    now = datetime(2026, 8, 20, 12, tzinfo=UTC)
    warehouse.ensure_apple_notes_tables()
    warehouse.insert_apple_note_revisions(
        [
            _default_row(APPLE_NOTE_REVISION_COLUMNS, account="z", note_id="n1", revision_id="r1",
                         title="Call with the bank", modified_at=now - timedelta(days=1), ingested_at=now),
            _default_row(APPLE_NOTE_REVISION_COLUMNS, account="z", note_id="n1", revision_id="r2",
                         title="Call with the bank (edited)", modified_at=now, ingested_at=now),
        ]
    )
    # 2001-01-01 + 800000000s = 2026-05-09T06:13:20Z (Core Data reference date).
    core_data = '{"raw": {"ZCREATIONDATE": "800000000.5", "ZDURATION": "44.92", "ZTITLE": "Call Recording"}}'
    warehouse.insert_apple_note_attachments(
        [
            _default_row(APPLE_NOTE_ATTACHMENT_COLUMNS, account="z", note_id="n1", revision_id=rev,
                         attachment_id="att-audio", filename="Call Recording.m4a",
                         content_type="audio/mp4a-latm", size_bytes=870960, content_sha256="sha-audio",
                         is_missing=0, storage_backend="google_drive", storage_file_id="drive-note-audio",
                         raw_metadata_json=core_data, ingested_at=now)
            for rev in ("r1", "r2")
        ]
        + [
            _default_row(APPLE_NOTE_ATTACHMENT_COLUMNS, account="z", note_id="n1", revision_id="r2",
                         attachment_id="att-image", filename="photo.png", content_type="image/png",
                         size_bytes=10, content_sha256="sha-img", is_missing=0, ingested_at=now),
            _default_row(APPLE_NOTE_ATTACHMENT_COLUMNS, account="z", note_id="n1", revision_id="r2",
                         attachment_id="att-octet", filename="memo.m4a", content_type="application/octet-stream",
                         size_bytes=10, content_sha256="sha-octet", is_missing=0, ingested_at=now),
            _default_row(APPLE_NOTE_ATTACHMENT_COLUMNS, account="z", note_id="n1", revision_id="r2",
                         attachment_id="att-missing", filename="lost.m4a", content_type="audio/mp4",
                         size_bytes=10, content_sha256="", is_missing=1, ingested_at=now),
        ]
    )

    rows = {
        row["recording_id"]: row
        for row in warehouse._query_dicts(
            "SELECT * FROM @marts_voice_memos_recordings WHERE source = 'apple_notes'"
        )
    }
    # One row per recording (not per revision); the image and the never-stored
    # file are not recordings; an audio file typed octet-stream still is.
    assert set(rows) == {"att-audio", "att-octet"}
    audio = rows["att-audio"]
    assert audio["title"] == "Call Recording"
    assert audio["recorded_at"] == datetime(2026, 5, 9, 6, 13, 20, 500000, tzinfo=UTC)
    assert float(audio["duration_seconds"]) == pytest.approx(44.92)
    assert audio["storage_file_id"] == "drive-note-audio"
    assert audio["transcript"] is None
    assert rows["att-octet"]["title"] == "Call with the bank (edited)"
    assert rows["att-octet"]["recorded_at"] == now

    candidates = warehouse.load_untranscribed_voice_recordings(provider="assemblyai", limit=10)
    assert {(row["source"], row["recording_id"]) for row in candidates} == {
        ("apple_notes", "att-audio"),
        ("apple_notes", "att-octet"),
    }


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


# --- query role privilege sweep --------------------------------------------


def test_query_role_setup_is_skipped_once_privileges_are_correct(warehouse: PostgresWarehouse) -> None:
    # The sweep rewrites the ACL of every table in every managed schema, so
    # re-running it on each of the ~30k warehouse constructions a day churned
    # pg_class by millions of row updates and raced concurrent DDL ("tuple
    # concurrently updated"). A freshly constructed warehouse must report no
    # drift, i.e. the next construction does zero catalog writes.
    assert warehouse._query_role_setup_needed() is False


def test_query_role_setup_detects_and_repairs_revoked_table_privileges(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_plaid_tables()
    # ensure_* created new tables; default privileges keep them readable.
    assert warehouse._query_role_setup_needed() is False

    accounts = warehouse.sql_relation("plaid_accounts")
    warehouse._raw_command(f'REVOKE SELECT ON {accounts} FROM "{warehouse.query_role}"')
    assert warehouse._query_role_setup_needed() is True

    # A new construction notices the drift and repairs it.
    repaired = PostgresWarehouse(_postgres_url(), schema=warehouse.schema_namespace)
    try:
        assert repaired._query_role_setup_needed() is False
        connection = psycopg2.connect(_postgres_url())
        try:
            with connection.cursor() as cursor:
                cursor.execute(f'SET LOCAL ROLE "{repaired.query_role}"')
                cursor.execute(f"SELECT count(*) FROM {accounts}")
                assert cursor.fetchone() == (0,)
        finally:
            connection.rollback()
            connection.close()
    finally:
        repaired.close()


def test_query_role_setup_detects_private_schema_leak(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_plaid_tables()
    tokens = warehouse.sql_relation("plaid_item_tokens")
    private_schema = warehouse.physical_schema_name("private")

    # Anything that hands the query role access to private token storage is
    # drift, no matter how it got granted.
    warehouse._raw_command(f'GRANT USAGE ON SCHEMA "{private_schema}" TO "{warehouse.query_role}"')
    warehouse._raw_command(f'GRANT SELECT ON {tokens} TO "{warehouse.query_role}"')
    assert warehouse._query_role_setup_needed() is True

    repaired = PostgresWarehouse(_postgres_url(), schema=warehouse.schema_namespace)
    try:
        assert repaired._query_role_setup_needed() is False
        connection = psycopg2.connect(_postgres_url())
        try:
            with connection.cursor() as cursor:
                cursor.execute(f'SET LOCAL ROLE "{repaired.query_role}"')
                with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                    cursor.execute(f"SELECT access_token FROM {tokens}")
        finally:
            connection.rollback()
            connection.close()
    finally:
        repaired.close()


def test_query_role_setup_retries_tuple_concurrently_updated(warehouse: PostgresWarehouse, monkeypatch) -> None:
    # The advisory lock serializes competing sweeps but not unrelated DDL
    # touching the same pg_class rows, so the collision has to be survivable.
    attempts: list[int] = []

    def flaky() -> None:
        attempts.append(1)
        if len(attempts) < 3:
            raise psycopg2.errors.InternalError_("tuple concurrently updated")

    monkeypatch.setattr(warehouse, "_query_role_setup_needed", lambda: True)
    monkeypatch.setattr(warehouse, "_ensure_query_role_locked", flaky)
    monkeypatch.setattr("personal_data_warehouse.postgres.time.sleep", lambda _seconds: None)

    warehouse._ensure_query_role()

    assert len(attempts) == 3


def test_query_role_setup_reraises_unrelated_errors(warehouse: PostgresWarehouse, monkeypatch) -> None:
    def boom() -> None:
        raise psycopg2.errors.InsufficientPrivilege("permission denied")

    monkeypatch.setattr(warehouse, "_query_role_setup_needed", lambda: True)
    monkeypatch.setattr(warehouse, "_ensure_query_role_locked", boom)

    with pytest.raises(psycopg2.errors.InsufficientPrivilege):
        warehouse._ensure_query_role()


def test_query_role_setup_detects_non_select_private_grants(warehouse: PostgresWarehouse) -> None:
    # The sweep issues REVOKE ALL on private, so the probe must not certify a
    # boundary narrower than that: a stray INSERT grant is still drift.
    warehouse.ensure_plaid_tables()
    tokens = warehouse.sql_relation("plaid_item_tokens")

    warehouse._raw_command(f'GRANT INSERT ON {tokens} TO "{warehouse.query_role}"')
    assert warehouse._query_role_setup_needed() is True

    repaired = PostgresWarehouse(_postgres_url(), schema=warehouse.schema_namespace)
    try:
        assert repaired._query_role_setup_needed() is False
    finally:
        repaired.close()


def test_search_text_broad_search_runs_one_pooled_bm25_scan_not_a_branch_loop() -> None:
    # A broad (unscoped) search used to execute one BM25 branch per coarse
    # source -- eighteen EXECUTEs in a plpgsql loop, strictly serial on one
    # backend, so wall clock was the SUM of every branch. Measured on the
    # production corpus that was 6.9s warm / 21.7s cold, while a single
    # index-ordered scan of the same BM25 index returns the global top 200 in
    # 36ms: the fan-out cost ~200x what the index costs. Worse, the planner
    # refuses the index for a selective adapter filter and re-scores every row
    # instead (~5.6ms per document), which is why the small `transcript` branch
    # alone took 3.4s.
    #
    # The broad path must therefore be ONE pooled scan, and the branch loop
    # must remain only for scoped (sources => ARRAY[...]) searches.
    sql = _search_text_function_sql()
    assert "IF sources IS NULL THEN" in sql, (
        "search_text() must take a pooled fast path for broad searches instead "
        "of looping every source branch"
    )
    assert "pool_adapter" in sql and "broad_ranked" in sql, (
        "the broad fast path must build a single candidate pool"
    )


def test_search_text_broad_pool_scans_low_volume_adapters_separately() -> None:
    # One flat global scan buries low-volume sources: a matching contact card
    # or voice-memo scores below hundreds of gmail/slack hits, and the
    # per-source floor cannot promote a row the pool never contained. The pool
    # is therefore scanned in two partitions -- high-volume adapters and
    # everything else -- so the floor still has candidates to promote. The
    # second partition costs ~143ms, versus ~4.8s for the per-source branches
    # it replaces.
    sql = _search_text_function_sql()
    for adapter in ("'gmail_email'", "'slack_message'", "'apple_message'"):
        assert adapter in sql
    assert "UNION ALL" in sql
    assert "t.adapter NOT IN (" in sql and "t.adapter IN (" in sql, (
        "the broad pool must scan high-volume adapters and the low-volume "
        "remainder as two separate index-ordered scans"
    )


def test_search_text_broad_pool_forces_the_index_ordered_plan() -> None:
    # With the default cost model the planner reads every row of a selective
    # adapter filter and recomputes the bm25 operator per row, which is ~100x
    # slower than scanning the score-ordered index and filtering. The operator
    # has no cost declaration the planner could use, so the fast path pins the
    # plan for the duration of the scan and restores it immediately.
    sql = _search_text_function_sql()
    assert "set_config('enable_sort', 'off', true)" in sql
    assert "set_config('enable_sort', 'on', true)" in sql, (
        "enable_sort must be restored right after the pooled scan so the hint "
        "cannot leak into the caller's query"
    )
    # ...and it must cover ONLY the scans. Leaving it on for the ranking query
    # too left the planner no sane way to feed the window function: at a 10k
    # pool one query ran for five MINUTES. The pool is therefore collected in
    # its own statement, into arrays, before the hint is restored.
    off_at = sql.index("set_config('enable_sort', 'off', true)")
    on_at = sql.index("set_config('enable_sort', 'on', true)")
    scoped = sql[off_at:on_at]
    assert "array_agg" in scoped, (
        "the pooled scan must be collected in its own statement while the "
        "enable_sort hint is in force"
    )
    assert "PARTITION BY" not in scoped, (
        "the per-source ranking must run AFTER enable_sort is restored"
    )
    # The one window allowed under the hint is the bare `row_number() OVER ()`
    # that captures each partition's scan ordinal. It carries no PARTITION BY
    # and no ORDER BY, so it needs no sort and cannot be the plan the disabled
    # sort would have broken -- it reads its input in the order the
    # index-ordered scan produced it.
    for window in re.findall(r"OVER \([^)]*\)", scoped):
        assert window == "OVER ()", (
            "only an unordered, unpartitioned window may run while enable_sort "
            f"is off; found {window}"
        )
    assert "row_number() OVER ()" in scoped, (
        "the pooled scan must capture its ordinal, which is what lets the "
        "ranking stage rank without recomputing the bm25 operator per row"
    )


def test_search_text_broad_pool_keeps_the_per_source_floor() -> None:
    # Same guarantee the branch merge gave: every source's top-N hits survive
    # ahead of the global score fill.
    sql = _search_text_function_sql()
    pool = sql[sql.index("pool_adapter"):]
    assert "PARTITION BY" in pool and "src_rank >" in pool, (
        "the pooled broad path must rank within each source and put each "
        "source's top-floor hits ahead of the global fill"
    )


def test_search_text_broad_pool_previews_only_the_returned_rows() -> None:
    # The branch loop previewed 12 rows x 18 branches (216 documents) to return
    # 50. Windowing a preview scans up to SEARCH_TEXT_PREVIEW_SCAN_CHARS of a
    # possibly multi-MB document, so previewing candidates that never survive
    # the merge is pure waste.
    sql = _search_text_function_sql()
    pool = sql[sql.index("pool_adapter"):]
    preview_at = pool.index("search_text_preview")
    floor_at = pool.index("src_rank >")
    assert preview_at > floor_at, (
        "the broad path must window previews after the per-source floor merge, "
        "not for every pooled candidate"
    )


def test_search_text_broad_pool_ranks_by_scan_ordinal_not_a_per_row_rescore() -> None:
    """The pooled scan must not recompute the bm25 operator per pooled row.

    Each partition is `ORDER BY <bm25 operator> LIMIT n`, an index-ordered
    scan, so its rows already emerge in exact descending relevance order and
    the scan ordinal IS the rank. Naming the operator in the pool's SELECT list
    as well re-tokenizes every pooled document to recover a number the ordering
    already encoded. Measured on the production corpus 2026-08-26, collecting
    the pool the way the function actually collects it (one array_agg
    statement): 5,000 high-volume rows cost 350ms with the score column and
    108ms without; a whole broad pool 575ms against 85ms; and with
    `priorities => ARRAY['self']`, where every surviving document is one of
    Zach's own large ones, 11.5s against 0.47s.

    This is the guard for the defect, not for the fix: it fails on the shape
    that was shipped.
    """
    sql = _search_text_function_sql()
    off_at = sql.index("set_config('enable_sort', 'off', true)")
    on_at = sql.index("set_config('enable_sort', 'on', true)")
    pooled = sql[off_at:on_at]
    assert "<@>" in pooled, "the pooled scan must still ORDER BY the bm25 operator"
    for match in re.finditer("<@>", pooled):
        assert pooled[: match.start()].rstrip().endswith("ORDER BY t.search_text"), (
            "the bm25 operator may appear only in the pooled scan's ORDER BY; "
            "anywhere else in the pool it re-scores every pooled document"
        )


def test_search_text_broad_candidates_are_scored_through_their_own_partition_index() -> None:
    """A candidate's score must come from the index its partition was scanned through.

    The two partitions are two BM25 corpora with their own term statistics, so
    a low-volume row scored against the global index is not the number the
    merge was built on. The pool therefore carries which partition a row came
    from, and the ranking stage picks the index from it.
    """
    sql = _search_text_function_sql()
    ranking = sql[sql.index("set_config('enable_sort', 'on', true)") :]
    ranking = ranking[: ranking.index("RETURN;")]
    assert "c.part" in ranking, (
        "the ranking stage must know which pool partition a candidate came from"
    )
    assert "to_bm25query(query, 'timeline_events_search_text_bm25_idx')" in ranking
    assert "to_bm25query(query, 'timeline_events_search_text_bm25_lowvol_idx')" in ranking
    # The attention partitions are two more corpora with their own statistics.
    assert "to_bm25query(query, 'timeline_events_search_text_bm25_attention_idx')" in ranking
    assert (
        "to_bm25query(query, 'timeline_events_search_text_bm25_attention_lowvol_idx')" in ranking
    )
    import personal_data_warehouse.postgres as postgres_module

    for part in (
        postgres_module.SEARCH_TEXT_POOL_PART_HIGH_VOLUME,
        postgres_module.SEARCH_TEXT_POOL_PART_LOW_VOLUME,
        postgres_module.SEARCH_TEXT_POOL_PART_ATTENTION_HIGH_VOLUME,
        postgres_module.SEARCH_TEXT_POOL_PART_ATTENTION_LOW_VOLUME,
    ):
        assert f"c.part = {part}" in ranking, (
            f"pool partition {part} has no scoring branch, so its rows are scored "
            "against another partition's term statistics"
        )


def test_search_hybrid_accepts_a_second_query_embedding() -> None:
    # Qwen3-Embedding is instruction-asymmetric: the instructed and the raw
    # form of the same question land in different neighbourhoods, and each
    # finds answers the other misses. Blending them into one vector averages
    # those neighbourhoods away (measured MRR 0.234); searching BOTH and fusing
    # by rank keeps them (0.300 on the same corpus and labels). The second
    # embedding is optional so a deployment without an instruction prefix, and
    # any direct SQL caller, keeps the single-vector behaviour.
    sql = _search_text_function_sql()
    assert "query_embedding_alt text DEFAULT NULL" in sql
    wrapper = sql[sql.rindex("CREATE OR REPLACE FUNCTION @search_hybrid("):]
    assert "query_embedding_alt" in wrapper
    assert wrapper.count("FROM @search_hybrid_semantic(") == 2


def test_search_hybrid_second_leg_is_skipped_when_no_alt_embedding() -> None:
    # A NULL alt embedding must not cost a second ANN scan: the leg is gated on
    # the parameter so the planner can drop it with a one-time filter.
    sql = _search_text_function_sql()
    assert "query_embedding_alt IS NOT NULL" in sql or "qvec_alt IS NOT NULL" in sql


def test_search_hybrid_fuses_semantic_legs_by_rank_not_distance() -> None:
    # Distances from two different query vectors are not comparable (each is
    # calibrated to its own neighbourhood), so the legs merge by reciprocal
    # rank, the same argument that keeps BM25 scores and cosine distances
    # apart in the outer fusion.
    sql = _search_text_function_sql()
    assert "sum(1.0 / (" in sql
    assert "sum(j.fuse)" in sql


def test_search_hybrid_clamps_ef_search_to_pgvectors_maximum() -> None:
    # pgvector rejects hnsw.ef_search above 1000. The exploration floor was
    # written as greatest(1000, max_results * 8), which exceeds that ceiling
    # for any max_results above 125 -- and search_text caps max_results at 200,
    # so `search(..., max_results => 150)` raised
    # `1600 is outside the valid range for parameter "hnsw.ef_search"` instead
    # of searching. The floor must be clamped, not just floored.
    sql = _search_text_function_sql()
    assert "set_config('hnsw.ef_search', greatest(" not in sql, (
        "an unclamped ef_search errors for max_results > 125"
    )
    assert "least(1000, greatest(1000, per_source * 8))" in sql


def test_search_schema_signature_covers_the_broad_pool_constants() -> None:
    # The signature guard skips recompiling the search DDL when nothing
    # changed. It is derived from the generator's SOURCE plus an explicit list
    # of constants, because the source only mentions a constant by NAME --
    # retuning the pool sizes or moving a source between the high- and
    # low-volume partitions would otherwise leave production running the old
    # functions forever.
    import inspect

    import personal_data_warehouse.postgres as postgres_module

    signature_source = inspect.getsource(postgres_module.PostgresWarehouse._search_schema_signature)
    for constant in (
        "SEARCH_TEXT_BROAD_POOL",
        "SEARCH_TEXT_BROAD_SMALL_POOL",
        "SEARCH_TEXT_LOW_VOLUME_ADAPTERS_SQL",
    ):
        assert constant in signature_source, f"{constant} is missing from the search DDL signature"


def test_search_hybrid_drops_its_previous_signature() -> None:
    # CREATE OR REPLACE FUNCTION with a new parameter creates an OVERLOAD, it
    # does not replace. Without an explicit drop, an upgraded warehouse keeps
    # the six-argument search_hybrid alongside the seven-argument one, and any
    # caller that omits the alternate embedding -- hand-written SQL, an agent
    # copying the old signature -- silently gets the OLD implementation with
    # the old ranking. Drop it as part of the same DDL.
    sql = _search_text_function_sql()
    assert "DROP FUNCTION IF EXISTS" in sql
    assert "@search_hybrid(text, text, text, integer, text[], timestamptz)" in sql


def test_search_hybrid_fuses_a_literal_leg_for_short_queries() -> None:
    # An identifier-shaped question is where BM25 tokenization and embeddings
    # both fail and literal matching wins: on the labeled benchmark, exact mode
    # scores MRR 0.518 on that stratum against hybrid's 0.245. Fusing the
    # literal hits in as a third leg took hybrid overall from 0.292 to 0.383
    # and answered three queries that previously had nothing in the top 50.
    sql = _search_text_function_sql()
    assert "exact_refs" in sql and "@search_text_exact(" in sql


def test_search_hybrid_literal_leg_searches_machine_tokens_in_bounded_chunks() -> None:
    # search_text_exact() has to preserve literal lookup across the full
    # timeline document, but hybrid only needs a cheap identifier-recall leg.
    # Calling the full function here makes every short hybrid query recheck
    # multi-megabyte TOASTed documents through the timeline trigram index.
    # The retrieval chunks cover the first 200k characters in bounded 2-6k
    # rows, so their own trigram index can confirm the same short identifiers
    # without decompressing the source document.
    import personal_data_warehouse.postgres as postgres_module

    index = next(
        spec
        for spec in postgres_module.POSTGRES_INDEXES
        if spec.name == "search_chunks_text_trgm_idx"
    )
    assert index.table == "search_chunks"
    assert index.requires_pg_trgm
    assert "CONCURRENTLY" in index.sql
    assert "text public.gin_trgm_ops" in index.sql

    sql = _search_text_function_sql()
    exact = sql[sql.index("CREATE OR REPLACE FUNCTION @search_hybrid_exact("):]
    literal = exact[exact.index("exact_refs"):exact.index("RETURN QUERY")]
    assert "exact_needle ~ '[0-9_./@-]'" in literal
    assert "pg_catalog.to_regclass" in literal
    assert "i.indisvalid" in literal and "i.indisready" in literal
    assert "FROM @search_chunks" in literal
    assert "JOIN @timeline_events t" in literal
    assert "c.anchor NOT LIKE c.adapter || '|w|%'" in literal
    assert "t.metadata->>'deleted'" in literal
    assert "t.adapter = 'drive_file'" in literal
    assert "t.metadata->>'excluded'" in literal
    assert "t.priority::text = ANY (priorities)" in literal, (
        "bounded exact candidates must be priority-scoped before top-k"
    )
    assert "ARRAY['imessage', 'slack', 'whatsapp']" in literal
    assert """ARRAY['imessage', 'slack', 'whatsapp'],
                                      since,
                                      priorities""" in literal, (
        "chat exact-ref recovery must preserve the hybrid priority scope"
    )
    assert "split_part(h.ref, ':', 1) = ANY (sem_adapters)" in literal
    assert "@search_text_exact(" in literal, (
        "ordinary names and chat-window identifiers must keep full-document "
        "matching so hybrid returns the event that actually contains the literal"
    )
    assert "GROUP BY" in literal, (
        "several chunks from one event must produce one literal rank"
    )
    assert "t.adapter = ANY (sem_adapters)" in literal
    assert "t.event_ts >= since" in literal
    assert "c.event_ts + interval '1 hour' > since" in literal


def test_search_hybrid_ranks_symbolic_chunk_matches_by_prominence() -> None:
    # Recency-only literal ranking put the labeled definition of one symbolic
    # identifier at rank 7. Preferring an earlier occurrence within a bounded
    # chunk moved it to rank 2 without moving any other labeled answer. Opaque
    # ids containing digits are different: position is arbitrary there and
    # hurt one label, so they deliberately keep the old recency order.
    sql = _search_text_function_sql()
    exact = sql[sql.index("CREATE OR REPLACE FUNCTION @search_hybrid_exact("):]
    literal = exact[exact.index("exact_refs"):exact.index("RETURN QUERY")]
    assert "WHEN exact_needle ~ '[0-9]' THEN NULL" in literal
    assert "c.chunk_index" in literal
    assert "strpos(lower(c.text), lower(exact_needle))" in literal
    assert "match_chunk ASC NULLS LAST" in literal


def test_search_hybrid_literal_leg_is_gated_on_query_length() -> None:
    # Literal matching is not free and a natural-language question gains
    # nothing from it -- ungated it scored WORSE (0.374 against 0.383) while
    # making every long query pay. It must also never run below
    # search_text_exact's minimum needle length, which raises.
    sql = _search_text_function_sql()
    assert "regexp_split_to_array" in sql, "the leg must be gated on the query's word count"
    assert f"<= {postgres_module_exact_max_words()}" in sql
    assert f"length(btrim(query)) >= {postgres_module_exact_min_chars()}" in sql, (
        "a shorter needle makes search_text_exact raise, which would take the "
        "whole hybrid search down with it"
    )


def postgres_module_exact_max_words() -> int:
    import personal_data_warehouse.postgres as postgres_module

    return postgres_module.SEARCH_HYBRID_EXACT_MAX_WORDS


def postgres_module_exact_min_chars() -> int:
    import personal_data_warehouse.postgres as postgres_module

    return postgres_module.SEARCH_HYBRID_EXACT_MIN_CHARS


def test_search_hybrid_weights_the_literal_leg_above_the_others() -> None:
    # A literal match on a short query is strong evidence; a rank-1 BM25 hit on
    # two common words is not.
    sql = _search_text_function_sql()
    import personal_data_warehouse.postgres as postgres_module

    assert f"{postgres_module.SEARCH_HYBRID_EXACT_WEIGHT} * COALESCE(1.0 / (" in sql


def test_search_hybrid_gives_a_term_bag_query_a_bm25_head_bonus_but_not_a_sentence() -> None:
    # Four ANN legs return hundreds of candidates each, so at a flat weight
    # semantic ranks 1-16 outvote a correct BM25 #1 on a term bag. The head
    # bonus is conditional on the query NOT reading like a sentence, using the
    # same function-word test the app's hint uses.
    sql = _search_text_function_sql()
    import personal_data_warehouse.postgres as postgres_module

    assert (
        f"CASE WHEN NOT query_is_sentence AND l.rnk <= "
        f"{postgres_module.SEARCH_HYBRID_LEXICAL_HEAD_RANKS} THEN "
        f"{postgres_module.SEARCH_HYBRID_LEXICAL_HEAD_WEIGHT} ELSE 1.0 END"
    ) in sql
    assert "query_is_sentence := coalesce(array_length(query_words, 1), 0) >= 5" in sql
    assert postgres_module.SEARCH_HYBRID_SEMANTIC_WEIGHT <= 1.0


def test_search_hybrid_literal_leg_failure_does_not_take_down_the_search() -> None:
    # The literal leg is an enhancement over the ranked and semantic legs.
    # Losing an entire search because it errored (a future validation in
    # search_text_exact, a statement timeout on a pathological needle) would be
    # worse than returning the other two legs -- but the drop must be LOUD,
    # because a silent degrade is how a broken search layer goes unnoticed for
    # weeks. Same contract as search_text's per-branch guard.
    sql = _search_text_function_sql()
    leg = sql[sql.index("exact_refs"):]
    assert "EXCEPTION WHEN OTHERS THEN" in leg
    assert "RAISE WARNING 'search_hybrid_exact: literal leg failed" in leg


def test_each_search_branch_names_the_index_that_covers_its_adapters() -> None:
    # vchord-bm25 raises "query specifies index X but planner chose index Y"
    # when the index named in to_bm25query() is not the one the plan used. A
    # low-volume branch's adapter filter is fully covered by the PARTIAL index,
    # so the planner may legitimately choose it -- and then a branch that names
    # the global index fails, taking a scoped search down with it (all-branches-
    # failed raises). Each branch must name the index that covers its own
    # adapters, which is also the fast plan: the transcript branch went from
    # 3.5s to ~150ms.
    import re

    import personal_data_warehouse.postgres as postgres_module

    sql = _search_text_function_sql()
    body = sql[sql.index("branch_sqls text[]"):]
    for source, adapters, _ in postgres_module.SEARCH_SOURCE_DEFS:
        branch = next(
            (line for line in body.split("$b$") if f"'{source}'::text AS source" in line),
            None,
        )
        assert branch, f"no generated branch for {source}"
        indexes = set(re.findall(r"to_bm25query\([^,]+,\s*'([a-z0-9_]+)'\)", branch))
        low_volume = source not in postgres_module.SEARCH_TEXT_HIGH_VOLUME_SOURCES
        want = (
            "timeline_events_search_text_bm25_lowvol_idx"
            if low_volume
            else "timeline_events_search_text_bm25_idx"
        )
        assert indexes == {want}, f"{source} branch scores through {indexes}, want {want}"


def test_search_text_branch_loop_pins_the_index_ordered_plan() -> None:
    # Same argument as the broad pool: without the hint the planner re-scores
    # every row of a selective adapter filter instead of scanning the index.
    # The hint has to cover the branch EXECUTEs and be restored before the
    # merge, which needs a real sort.
    sql = _search_text_function_sql()
    loop_at = sql.index("FOR branch_idx IN 1..")
    assert "set_config('enable_sort', 'off', true)" in sql[:loop_at]
    merge_at = sql.index("PARTITION BY h.source")
    assert "set_config('enable_sort', 'on', true)" in sql[loop_at:merge_at], (
        "enable_sort must be restored after the branch loop and before the merge"
    )


def test_search_hybrid_pushes_the_source_filter_into_the_ann_scan() -> None:
    # Filtering the semantic legs through a joined adapter->source VALUES list
    # keeps the predicate ABOVE the index scan, so pgvector's iterative scan
    # cannot use it: it walks the graph feeding candidates up to be discarded.
    # Measured on the production corpus, a photo-scoped hybrid search took 44.6s
    # that way -- past the app's 30s statement budget -- while the same ANN scan
    # with the adapter predicate pushed down took 1.0s. Resolve `sources` to
    # adapters once, then filter the scan on c.adapter directly.
    sql = _search_text_function_sql()
    legs = sql[
        sql.index("WITH sem_chunks AS") :
        sql.index("CREATE OR REPLACE FUNCTION @search_hybrid_exact(")
    ]
    assert "sem_adapters" in legs, "the semantic legs must filter on resolved adapters"
    assert "c.adapter = ANY (sem_adapters)" in legs
    assert "map.source = ANY (sources)" not in legs, (
        "a post-join source filter blocks pgvector's filtered iterative scan"
    )


def test_search_functions_accept_a_priorities_filter() -> None:
    # Every timeline event carries a priority tier, and until now an agent
    # could not use it: the human web UI could filter, the agent surface could
    # not, and `priority` was not even on the hit. All three entry points take
    # the same trailing `priorities text[] DEFAULT NULL` so a scoped question
    # ("what did a real person send me") is one parameter, not a post-filter
    # over whatever the unscoped top-k happened to return.
    sql = _search_text_function_sql()
    for function_name in ("@search_text(", "@search_text_exact(", "@search_hybrid("):
        body = sql.split(f"CREATE OR REPLACE FUNCTION {function_name}", 1)
        assert len(body) == 2, f"expected {function_name} to be generated"
        signature = body[1].split(")", 1)[0]
        assert "priorities text[] DEFAULT NULL" in signature, (
            f"{function_name} must accept a trailing priorities filter; got {signature!r}"
        )


def test_search_text_pushes_priorities_into_every_scan() -> None:
    # A tier filter applied AFTER a top-k scan returns the whole corpus's top-k
    # intersected with the tier, which for 'self' (503k of 48M rows) is almost
    # always empty. It has to be part of the WHERE of every scan: each per-source
    # branch AND both partitions of the broad pooled scan.
    sql = _search_text_function_sql()
    branch_count = sql.count("%4$L::text[] IS NULL OR t.priority::text = ANY (%4$L::text[])")
    assert branch_count >= len(postgres_module_source_defs()), (
        "every per-source branch must filter on priority inside its WHERE; "
        f"found {branch_count} branches with the predicate"
    )
    assert "query, per_branch_limit, since, priorities" in sql, (
        "each branch's EXECUTE must pass `priorities` as the fourth format argument"
    )
    # Both pooled partitions (global index + low-volume partial index).
    assert sql.count("AND (priorities IS NULL OR t.priority::text = ANY (priorities))") >= 3, (
        "the broad pooled scan's BOTH partitions and search_text_exact's scan "
        "must filter on priority in the WHERE, not after the top-k"
    )


def postgres_module_source_defs() -> tuple:
    import personal_data_warehouse.postgres as postgres_module

    return postgres_module.SEARCH_SOURCE_DEFS


def test_search_functions_reject_unknown_priority_tokens() -> None:
    # Same contract `sources` has. Silently dropping an unrecognized tier is
    # the worst outcome available: the caller asked for one tier and gets the
    # entire corpus, which reads downstream as a correct answer.
    import personal_data_warehouse.postgres as postgres_module

    sql = _search_text_function_sql()
    for function_name in ("search_text", "search_text_exact", "search_hybrid"):
        assert f"RAISE EXCEPTION '{function_name}: unknown priority %'" in sql, (
            f"{function_name}() must RAISE on an unknown priority token"
        )
    valid = ", ".join(postgres_module.PostgresWarehouse._SEARCH_PRIORITY_TOKENS)
    assert f"valid priorities are {valid}" in sql, (
        "the RAISE must name the whole valid set, so a caller can self-correct"
    )
    # An empty array means "every tier", exactly like omitting the parameter:
    # callers build it from an optional tool field and [] must not mean
    # "match nothing".
    assert "IF priorities IS NOT NULL AND coalesce(array_length(priorities, 1), 0) = 0 THEN" in sql


def test_search_hit_carries_its_priority_tier() -> None:
    # A hit that does not say which tier it came from cannot be triaged, and a
    # filtered search cannot show its work. The composite type is rebuilt
    # (DROP ... CASCADE) whenever its attribute count changes, so this column
    # must be counted in that guard too.
    sql = _search_text_function_sql()
    assert "source_pk jsonb, priority text" in sql, (
        "the search hit composite type must carry priority"
    )
    assert "hit_attr_count IS DISTINCT FROM 14" in sql, (
        "the composite-type rebuild guard must count the new attribute; a stale "
        "count leaves the old 13-column type in place and every branch cast fails"
    )


def test_search_hybrid_filters_the_semantic_leg_on_priority() -> None:
    # The two lexical legs push the tier down into their own scans. The ANN legs
    # cannot (derived_search.chunks carries no priority, and a filter above
    # pgvector's iterative scan is what made a source-scoped search take 44.6s),
    # so the fusion must filter on the timeline row it already probes by primary
    # key. Without it a hybrid search would honor the tier in two legs of three.
    sql = _search_text_function_sql()
    assert "@search_text(query, per_source, sources, since, priorities)" in sql, (
        "search_hybrid's lexical leg must pass the tier filter down"
    )
    assert "query, per_source, sources, since, priorities" in sql, (
        "search_hybrid's literal leg must pass the tier filter down"
    )
    assert "OR t.priority::text = ANY (priorities))" in sql, (
        "search_hybrid must filter its fused output on priority, or the semantic "
        "leg would return rows from tiers the caller excluded"
    )


def test_search_text_exact_scopes_parallel_hints_to_the_recheck() -> None:
    # The trigram index answers in ~170ms; the ILIKE recheck then detoasts every
    # candidate document, and that is where the seconds go -- single-core CPU on
    # a 28-vCPU box (measured on prod: shared hit=50871, zero reads). The planner
    # costs the bitmap heap scan by ROWS and has no idea a row can be a
    # multi-megabyte TOASTed document, so it never parallelizes. Telling it setup
    # is free took the identifier query from 4143ms to 782ms on 8 workers, same
    # buffers, same rows.
    #
    # The scoping is the load-bearing part, exactly like enable_sort in
    # search_text(): a hint left over a whole plan is how a query once ran for
    # five MINUTES. Save, set, run the ONE statement, restore.
    sql = _search_text_exact_sql()
    assert "saved_parallel_setup_cost := current_setting('parallel_setup_cost')" in sql
    assert "saved_min_parallel_scan := current_setting('min_parallel_table_scan_size')" in sql
    set_at = sql.index("PERFORM set_config('parallel_setup_cost', '0', true)")
    query_at = sql.index("RETURN QUERY")
    restore_at = sql.index("PERFORM set_config('parallel_setup_cost', saved_parallel_setup_cost, true)")
    assert set_at < query_at < restore_at, (
        "the parallel hint must be set immediately before the recheck and restored "
        "immediately after it, never left over the rest of the function"
    )
    assert "PERFORM set_config('min_parallel_table_scan_size', saved_min_parallel_scan, true)" in sql, (
        "both hints must be restored to what the deployment actually configures, "
        "not to the shipped defaults"
    )


def test_read_only_search_helpers_are_parallel_safe() -> None:
    # Marking governs the CALLER's plan, and only the bodies that touch session
    # state have to stay unsafe. context() and search_text_sources() read and
    # nothing else, so a caller joining them to anything keeps its parallel plan.
    sql = _search_text_function_sql()
    for marker in ("@search_text_sources()", "@timeline_context("):
        body = sql.split(f"CREATE OR REPLACE FUNCTION {marker}", 1)[1]
        header = body.split("AS $", 1)[0]
        assert "PARALLEL SAFE" in header, f"{marker} should be PARALLEL SAFE: {header!r}"
    # search_text/search_text_exact call set_config(), which raises under
    # IsInParallelMode() in the LEADER as well as in a worker -- so PARALLEL
    # RESTRICTED would not save them either. They must stay unsafe.
    for marker in ("@search_text(", "@search_text_exact("):
        header = sql.split(f"CREATE OR REPLACE FUNCTION {marker}", 1)[1].split("AS $", 1)[0]
        assert "PARALLEL SAFE" not in header and "PARALLEL RESTRICTED" not in header, (
            f"{marker} calls set_config() and must remain PARALLEL UNSAFE: {header!r}"
        )


def test_search_schema_signature_covers_the_priority_tokens() -> None:
    # The generated DDL embeds the tier list; if the list changes without the
    # signature changing, the rebuild guard skips the recompile and the
    # validation keeps accepting (or rejecting) the old set forever.
    import personal_data_warehouse.postgres as postgres_module

    class _Stub:
        _SEARCHABLE_TEXT_TABLES = ("timeline_events",)
        _SEARCH_PRIORITY_TOKENS = postgres_module.PostgresWarehouse._SEARCH_PRIORITY_TOKENS
        _search_schema_signature = postgres_module.PostgresWarehouse._search_schema_signature
        _ensure_search_text_function = postgres_module.PostgresWarehouse._ensure_search_text_function

        def pgvector_available(self) -> bool:
            return True

        def _relation_exists(self, table: str) -> bool:
            return True

    stub = _Stub()
    before = stub._search_schema_signature()
    stub._SEARCH_PRIORITY_TOKENS = ("self", "direct")
    after = stub._search_schema_signature()
    assert before and before != after, (
        "changing the priority token list must change the search schema signature"
    )


def test_search_text_serves_an_attention_scoped_call_from_the_attention_index() -> None:
    """A priority-scoped broad call must scan the PARTIAL attention index.

    `self` is 1.01% of a 49M-row timeline and `self` + `direct` together are
    2.72%. Filling a 5,000-row pool from the global BM25 index therefore walks
    ~500k score-ordered documents and pays a random heap visit for each to
    check the tier -- measured on production 2026-08-26 at 15-20s cold on a
    novel query, which took `pdw search --priority self` through the app's
    multi-leg hybrid past the 60s statement ceiling twice. The same pool from
    an index that contains only those tiers is a shallow scan.
    """
    import personal_data_warehouse.postgres as postgres_module

    sql = _search_text_function_sql()
    pool = sql[
        sql.index("set_config('enable_sort', 'off', true)") : sql.index(
            "set_config('enable_sort', 'on', true)"
        )
    ]
    assert (
        "priorities <@ ARRAY["
        + postgres_module.SEARCH_TEXT_ATTENTION_PRIORITIES_SQL
        + "]" in pool
    ), (
        "the broad pool must choose the attention index only when the requested "
        "tiers are a SUBSET of the ones it contains"
    )
    assert "to_bm25query(query, 'timeline_events_search_text_bm25_attention_idx')" in pool
    assert (
        "to_bm25query(query, 'timeline_events_search_text_bm25_attention_lowvol_idx')" in pool
    )


def test_the_attention_pool_repeats_the_index_predicate_as_a_literal() -> None:
    """The scan's WHERE must literally imply the partial index's predicate.

    A partial index is only usable when the planner can PROVE the query implies
    its predicate, and `priorities` is a runtime array parameter it can prove
    nothing about. Without the literal tier predicate beside it the planner
    falls back to the global index -- and vchord-bm25 then RAISES, because
    to_bm25query() pins an index by name and the plan used another one. So this
    is not an optimization that degrades quietly; getting it wrong takes broad
    priority search down.
    """
    import personal_data_warehouse.postgres as postgres_module

    sql = _search_text_function_sql()
    pool = sql[
        sql.index("set_config('enable_sort', 'off', true)") : sql.index(
            "set_config('enable_sort', 'on', true)"
        )
    ]
    literal = f"t.priority IN ({postgres_module.SEARCH_TEXT_ATTENTION_PRIORITIES_SQL})"
    assert pool.count(literal) == 2, (
        "both attention partitions must carry the index predicate as a literal; "
        f"found {pool.count(literal)}"
    )
    # And the index it names must be partial on exactly that predicate.
    for name in (
        "timeline_events_search_text_bm25_attention_idx",
        "timeline_events_search_text_bm25_attention_lowvol_idx",
    ):
        spec = next(ix for ix in postgres_module.POSTGRES_INDEXES if ix.name == name)
        assert (
            f"priority IN ({postgres_module.SEARCH_TEXT_ATTENTION_PRIORITIES_SQL})" in spec.sql
        )


def test_a_non_subset_priority_call_keeps_the_general_indexes() -> None:
    """`noise`, `cc`, `background` and an unscoped call must NOT read the attention index.

    The attention index contains only `self` and `direct`, so serving any other
    tier from it silently returns nothing -- the worst failure this search layer
    has: an empty result that reads as "no matches". The general pool has to
    stay the default and the attention pool has to be the guarded exception.
    """
    import personal_data_warehouse.postgres as postgres_module

    sql = _search_text_function_sql()
    pool = sql[
        sql.index("set_config('enable_sort', 'off', true)") : sql.index(
            "set_config('enable_sort', 'on', true)"
        )
    ]
    # The attention pool sits under a guard; the general pool is the ELSE.
    guard = "priorities <@ ARRAY[" + postgres_module.SEARCH_TEXT_ATTENTION_PRIORITIES_SQL + "]"
    assert guard in pool
    general = pool[pool.index("ELSE", pool.index(guard)) :]
    assert "attention" not in general, (
        "the fallback pool must not name an attention index: it cannot answer "
        "a call for noise/cc/background at all"
    )
    assert "to_bm25query(query, 'timeline_events_search_text_bm25_idx')" in general
    assert "to_bm25query(query, 'timeline_events_search_text_bm25_lowvol_idx')" in general


def test_search_schema_signature_covers_the_attention_tiers() -> None:
    # The attention tier list is baked into the generated pool SQL and into the
    # index predicates. If it moves without the signature moving, the rebuild
    # guard skips the recompile and the function keeps naming an index whose
    # predicate no longer matches its WHERE -- which is a RAISE, not a slowdown.
    import personal_data_warehouse.postgres as postgres_module

    class _Stub:
        _SEARCHABLE_TEXT_TABLES = ("timeline_events",)
        _SEARCH_PRIORITY_TOKENS = postgres_module.PostgresWarehouse._SEARCH_PRIORITY_TOKENS
        _search_schema_signature = postgres_module.PostgresWarehouse._search_schema_signature
        _ensure_search_text_function = postgres_module.PostgresWarehouse._ensure_search_text_function

        def pgvector_available(self) -> bool:
            return True

        def _relation_exists(self, table: str) -> bool:
            return True

    stub = _Stub()
    before = stub._search_schema_signature()
    original = postgres_module.SEARCH_TEXT_ATTENTION_PRIORITIES_SQL
    postgres_module.SEARCH_TEXT_ATTENTION_PRIORITIES_SQL = "'self'"
    try:
        after = stub._search_schema_signature()
    finally:
        postgres_module.SEARCH_TEXT_ATTENTION_PRIORITIES_SQL = original
    assert before and before != after, (
        "changing the attention tier list must change the search schema signature"
    )


def test_search_text_attention_scoped_results_match_the_general_path(
    warehouse: PostgresWarehouse,
) -> None:
    """Correctness before speed: the fast index must return the same rows.

    Against a real timeline, a call scoped to `self` / `direct` / both --
    served by the attention partitions -- must return exactly what the general
    pool returns filtered to the same tiers, and a call that mixes an attention
    tier with a non-attention one must fall back and still be right.
    """
    if not _pg_textsearch_usable(warehouse):
        pytest.skip("pg_textsearch is not installed/preloaded on this Postgres host")

    _ensure_all_table_groups(warehouse)
    warehouse._set_search_path()

    message_datetime = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.insert_slack_conversations(
        [
            _slack_conversation_row(
                conversation_id="C1", conversation_type="private_channel", sync_version=1
            )
        ]
    )
    warehouse.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id="C1",
                message_ts="900.1",
                message_datetime=message_datetime,
                text="xylocarp rollout schedule",
            )
        ]
    )
    warehouse.insert_messages(
        [
            _message_row(
                message_id="m-xylo",
                subject="xylocarp kickoff",
                labels=["INBOX"],
                sync_version=1,
            )
        ]
    )
    warehouse.insert_contact_cards(
        [_contact_card_row(card_id="card-xylo", display_name="Xylocarp Person", sync_version=1)]
    )
    _sync_timeline(warehouse)

    general = {
        (row[0], row[1])
        for row in warehouse._query(
            "SELECT ref, priority FROM @search_text('xylocarp', 50) WHERE score < 0"
        )
    }
    assert general, "expected ranked hits before any tier filter"

    def scoped(tiers: list[str]) -> set[tuple[str, str]]:
        return {
            (row[0], row[1])
            for row in warehouse._query(
                "SELECT ref, priority FROM @search_text('xylocarp', 50, NULL, NULL, %s::text[]) "
                "WHERE score < 0",
                (tiers,),
            )
        }

    attention_tiers = ["self", "direct"]
    for tiers in (["self"], ["direct"], attention_tiers):
        want = {hit for hit in general if hit[1] in tiers}
        assert scoped(tiers) == want, (
            f"the attention-index path disagrees with the general path for {tiers}"
        )

    # A tier the attention index does not contain must fall back, not vanish.
    mixed = ["self", "noise"]
    assert scoped(mixed) == {hit for hit in general if hit[1] in mixed}
    assert scoped(["noise"]) == {hit for hit in general if hit[1] == "noise"}

    # The test is only meaningful if the seeded corpus actually reaches the
    # attention path with rows in it.
    assert any(hit[1] in attention_tiers for hit in general), (
        "seed a self/direct event or this test proves nothing about the "
        "attention partitions"
    )

    # Scores come from a DIFFERENT corpus here (the attention index has its own
    # term statistics), so they are not comparable to the general path's
    # numbers -- but they must still be real operator scores, and within one
    # source the output must still be in score order, which is what proves the
    # pool ordinal still tracks the score on this path.
    ranked = warehouse._query(
        "SELECT source, score FROM @search_text('xylocarp', 50, NULL, NULL, %s::text[]) "
        "WHERE score < 0",
        (attention_tiers,),
    )
    assert ranked, "expected attention-scoped hits to score"
    for source in {row[0] for row in ranked}:
        scores = [row[1] for row in ranked if row[0] == source]
        assert scores == sorted(scores), (
            f"{source} attention hits came back out of score order: {scores}"
        )


def test_search_functions_drop_their_previous_signature() -> None:
    # CREATE OR REPLACE with a new parameter OVERLOADS rather than replaces.
    # Leaving the four-argument form in place makes every existing positional
    # call ambiguous and lets a caller that omits `priorities` keep reaching an
    # implementation that cannot filter.
    sql = _search_text_function_sql()
    assert "DROP FUNCTION IF EXISTS @search_text(text, integer, text[], timestamptz);" in sql
    assert "DROP FUNCTION IF EXISTS @search_text_exact(text, integer, text[], timestamptz);" in sql
    assert (
        "DROP FUNCTION IF EXISTS @search_hybrid(text, text, text, integer, text[], timestamptz, text);"
        in sql
    )


def test_search_text_filters_hits_to_the_requested_priority(warehouse: PostgresWarehouse) -> None:
    # End-to-end against a real timeline: the tier filter must actually restrict
    # the result set (both ranked and literal search), an unknown tier must
    # RAISE, and an unfiltered call must be unchanged.
    if not _pg_textsearch_usable(warehouse):
        pytest.skip("pg_textsearch is not installed/preloaded on this Postgres host")

    _ensure_all_table_groups(warehouse)
    warehouse._set_search_path()

    message_datetime = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.insert_slack_conversations(
        [_slack_conversation_row(conversation_id="C1", conversation_type="private_channel", sync_version=1)]
    )
    warehouse.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id="C1",
                message_ts="700.1",
                message_datetime=message_datetime,
                text="zanzibar rollout schedule",
            )
        ]
    )
    warehouse.insert_contact_cards(
        [_contact_card_row(card_id="card-zan", display_name="Zanzibar Person", sync_version=1)]
    )
    _sync_timeline(warehouse)

    unfiltered = {
        (row[0], row[1])
        for row in warehouse._query(
            "SELECT ref, priority FROM @search_text('zanzibar', 20) WHERE score < 0"
        )
    }
    assert unfiltered, "expected ranked hits before filtering"
    assert all(priority for _, priority in unfiltered), (
        "every hit must report the tier it came from"
    )
    tiers = {priority for _, priority in unfiltered}

    for tier in sorted(tiers):
        rows = warehouse._query(
            "SELECT ref, priority FROM @search_text('zanzibar', 20, NULL, NULL, %s::text[]) WHERE score < 0",
            ([tier],),
        )
        assert rows, f"expected at least one hit in tier {tier}"
        assert {priority for _, priority in rows} == {tier}
        assert {ref for ref, _ in rows} == {ref for ref, priority in unfiltered if priority == tier}

    # Every tier at once is the same answer as no filter at all.
    all_tiers = warehouse._query(
        "SELECT ref FROM @search_text('zanzibar', 20, NULL, NULL, %s::text[]) WHERE score < 0",
        (sorted(tiers),),
    )
    assert {row[0] for row in all_tiers} == {ref for ref, _ in unfiltered}

    # An EMPTY array means every tier, not "match nothing".
    empty_filter = warehouse._query(
        "SELECT ref FROM @search_text('zanzibar', 20, NULL, NULL, %s::text[]) WHERE score < 0",
        ([],),
    )
    assert {row[0] for row in empty_filter} == {ref for ref, _ in unfiltered}

    # Literal search filters on the same vocabulary.
    exact_tier = sorted(tiers)[0]
    exact_rows = warehouse._query(
        "SELECT priority FROM @search_text_exact('zanzibar', 20, NULL, NULL, %s::text[])",
        ([exact_tier],),
    )
    assert exact_rows and {row[0] for row in exact_rows} == {exact_tier}

    for function in ("@search_text", "@search_text_exact"):
        with pytest.raises(psycopg2.errors.RaiseException) as excinfo:
            warehouse._query(
                f"SELECT ref FROM {function}('zanzibar', 20, NULL, NULL, %s::text[])",
                (["urgent"],),
            )
        assert "unknown priority" in str(excinfo.value)
        warehouse._connection.rollback()


# --- marts_ops.slack_conversation_health --------------------------------------


def _health_conversation_row(
    conversation_id: str,
    conversation_type: str,
    *,
    synced_at: datetime,
) -> dict:
    return {
        "account": "zrl",
        "team_id": "T1",
        "conversation_id": conversation_id,
        "conversation_type": conversation_type,
        "name": conversation_id.lower(),
        "is_channel": 1 if conversation_type.endswith("channel") else 0,
        "is_group": 0,
        "is_im": 1 if conversation_type == "im" else 0,
        "is_mpim": 1 if conversation_type == "mpim" else 0,
        "is_private": 0,
        "is_archived": 0,
        "is_member": 1,
        "creator": "U1",
        "created_at": datetime(2026, 1, 1, tzinfo=UTC),
        "topic": "",
        "purpose": "",
        "num_members": 3,
        "raw_json": "{}",
        "synced_at": synced_at,
        "sync_version": 1,
    }


def test_slack_conversation_health_catches_a_discovery_walk_that_never_advances(
    warehouse: PostgresWarehouse,
) -> None:
    """The exact production failure: page-1-only conversation discovery.

    ``conversations.list`` was re-read from page 1 on every metadata run and
    stopped after one page, so only the first 200 conversations of a type were
    ever re-stamped. 172 group DMs and 1,948 public channels created after the
    last full walk were never discovered at all, and because the freshness pass
    reads *cached* conversation rows they were never polled for messages either.

    marts_ops.pipeline_health could not see it: it aggregates Slack as one
    pipeline, and ~19k public-channel messages a day kept it green. The signal
    that works is per type and is about the *sync attempt*, not the messages —
    mpim legitimately has zero-message days, but a conversation whose metadata
    has not been re-stamped in three months is always a broken walk.
    """
    warehouse.ensure_slack_tables()
    now = datetime.now(tz=UTC)
    fresh = now - timedelta(minutes=30)
    ancient = now - timedelta(days=98)

    warehouse.insert_slack_conversations(
        [
            # A healthy type: the whole list was walked recently.
            _health_conversation_row("D1", "im", synced_at=fresh),
            _health_conversation_row("D2", "im", synced_at=fresh),
            # mpim: only page 1 got re-stamped; the tail is frozen in May.
            _health_conversation_row("C_PAGE1", "mpim", synced_at=fresh),
            _health_conversation_row("C_TAIL_A", "mpim", synced_at=ancient),
            _health_conversation_row("C_TAIL_B", "mpim", synced_at=ancient),
        ]
    )

    rows = {
        row[0]: row[1:]
        for row in warehouse._query(
            """
            SELECT conversation_type, conversation_count, status,
                   oldest_conversation_synced_at, newest_conversation_synced_at
            FROM @marts_ops_slack_conversation_health
            WHERE account = 'zrl'
            ORDER BY conversation_type
            """
        )
    }

    assert rows["im"][1] == "ok"
    assert rows["mpim"][0] == 3
    assert rows["mpim"][1] == "stale", "a three-month-old discovery walk must not read as ok"
    # The newest stamp alone looks perfectly healthy — that is why the view
    # judges the OLDEST one. Reporting max(synced_at) is what let page-1-only
    # discovery hide behind a fresh-looking timestamp for three months.
    assert rows["mpim"][3] >= fresh - timedelta(minutes=1)


def test_slack_conversation_health_reports_the_discovery_cursor(
    warehouse: PostgresWarehouse,
) -> None:
    """A resumable walk's cursor is the evidence that discovery is advancing."""
    warehouse.ensure_slack_tables()
    now = datetime.now(tz=UTC)
    warehouse.insert_slack_conversations(
        [_health_conversation_row("C1", "mpim", synced_at=now - timedelta(minutes=5))]
    )
    warehouse.insert_slack_sync_state(
        account="zrl",
        team_id="T1",
        object_type="conversation_list",
        object_id="mpim",
        cursor_ts="page12",
        last_sync_type="conversation_refresh",
        status="ok",
        error="",
        updated_at=now - timedelta(minutes=5),
        sync_version=1,
    )

    rows = warehouse._query(
        """
        SELECT conversation_type, discovery_cursor, status
        FROM @marts_ops_slack_conversation_health
        WHERE account = 'zrl' AND conversation_type = 'mpim'
        """
    )
    assert rows == [("mpim", "page12", "ok")]


# --- marts_slack.huddles -------------------------------------------------------


def test_slack_huddles_view_extracts_participants_and_duration(
    warehouse: PostgresWarehouse,
) -> None:
    """Huddle *metadata* is capturable, and was already being stored.

    Slack has no API that lists huddles and none that exposes huddle audio or
    Slack-AI huddle notes, so it is easy to conclude huddles are entirely
    missing from the warehouse. They are not: a huddle posts a message with
    subtype 'huddle_thread' whose payload carries a `room` object with
    created_by, date_start, date_end and the full participant_history. 5,942 of
    them were already sitting in base_slack.messages.raw_json, unreadable
    without hand-parsing JSON.

    What genuinely is NOT in PDW is what was *said* in a huddle. Absence of a
    decision in the warehouse is therefore never evidence it was not made.
    """
    warehouse.ensure_slack_tables()
    now = datetime(2026, 8, 20, 15, 0, tzinfo=UTC)
    payload = {
        "metadata": {"event_type": "slack_system.huddle.started"},
        "room": {
            "id": "R0EXAMPLE",
            "name": "sync on the ledger",
            "created_by": "UZACH",
            "date_start": 1787530888,
            "date_end": 1787537706,
            "has_ended": True,
            "call_family": "huddle",
            "huddle_link": "https://app.slack.com/huddle/E1/C1",
            "participant_history": ["UZACH", "UMAX", "UDEV"],
            "channels": ["C1"],
        },
    }
    warehouse.insert_slack_messages(
        [
            {
                "account": "zrl",
                "team_id": "T1",
                "conversation_id": "C1",
                "message_ts": "1787530888.415919",
                "message_datetime": now,
                "thread_ts": "1787530888.415919",
                "parent_message_ts": "",
                "user_id": "UZACH",
                "bot_id": "",
                "username": "",
                "type": "message",
                "subtype": "huddle_thread",
                "text": "A huddle started",
                "blocks_json": "[]",
                "attachments_json": "[]",
                "is_thread_parent": 1,
                "is_thread_reply": 0,
                "reply_count": 3,
                "reply_users_count": 3,
                "latest_reply_ts": "",
                "edited_ts": "",
                "client_msg_id": "",
                "is_deleted": 0,
                "raw_json": json.dumps(payload),
                "synced_at": now,
                "sync_version": 1,
            }
        ]
    )

    rows = warehouse._query(
        """
        SELECT huddle_id, huddle_name, created_by, participant_count,
               duration_seconds, participant_user_ids
        FROM @marts_slack_huddles
        WHERE account = 'zrl'
        """
    )
    assert len(rows) == 1
    huddle_id, name, created_by, participants, duration, user_ids = rows[0]
    assert huddle_id == "R0EXAMPLE"
    assert name == "sync on the ledger"
    assert created_by == "UZACH"
    assert participants == 3
    # date_start/date_end are epoch INTEGERS inside the JSON payload, not the
    # timestamps the rest of the warehouse uses; the view converts them so a
    # caller never has to know that.
    assert duration == 1787537706 - 1787530888
    assert sorted(user_ids) == ["UDEV", "UMAX", "UZACH"]


def test_slack_huddles_view_reports_an_unfinished_huddle_as_null_not_the_epoch(
    warehouse: PostgresWarehouse,
) -> None:
    """A live huddle has no end. It must read NULL, not 1970 and not a duration."""
    warehouse.ensure_slack_tables()
    now = datetime(2026, 8, 20, 15, 0, tzinfo=UTC)
    payload = {
        "room": {
            "id": "R0LIVE",
            "name": "",
            "created_by": "UZACH",
            "date_start": 1787530888,
            "date_end": 0,
            "has_ended": False,
            "participant_history": ["UZACH"],
        }
    }
    warehouse.insert_slack_messages(
        [
            {
                "account": "zrl",
                "team_id": "T1",
                "conversation_id": "C1",
                "message_ts": "1787530999.000100",
                "message_datetime": now,
                "thread_ts": "",
                "parent_message_ts": "",
                "user_id": "UZACH",
                "bot_id": "",
                "username": "",
                "type": "message",
                "subtype": "huddle_thread",
                "text": "A huddle started",
                "blocks_json": "[]",
                "attachments_json": "[]",
                "is_thread_parent": 0,
                "is_thread_reply": 0,
                "reply_count": 0,
                "reply_users_count": 0,
                "latest_reply_ts": "",
                "edited_ts": "",
                "client_msg_id": "",
                "is_deleted": 0,
                "raw_json": json.dumps(payload),
                "synced_at": now,
                "sync_version": 1,
            }
        ]
    )

    rows = warehouse._query(
        """
        SELECT ended_at, duration_seconds, has_ended
        FROM @marts_slack_huddles
        WHERE account = 'zrl' AND huddle_id = 'R0LIVE'
        """
    )
    assert rows == [(None, None, 0)]


def test_slack_conversation_list_cursor_reset_survives_the_upsert(
    warehouse: PostgresWarehouse,
) -> None:
    """A finished walk must be able to say so through the real upsert.

    ops.slack_sync_state preserves a non-empty ``cursor_ts`` against an empty
    write, deliberately: a per-conversation error row records status/error with
    no cursor and must not wipe the message high-water mark from the last
    successful page. That rule also silently swallows "the walk finished, start
    over" if completion is expressed by blanking the cursor — the row keeps the
    last page's cursor and discovery stays pinned to the end of the list
    forever, never cycling back to re-stamp older conversations. Completion is
    therefore carried by `status`, which has no preserve rule.
    """
    warehouse.ensure_slack_tables()
    now = datetime(2026, 8, 24, 3, 0, tzinfo=UTC)
    common = dict(
        account="zrl",
        team_id="T1",
        object_type="conversation_list",
        object_id="mpim",
        last_sync_type="conversation_refresh",
        error="",
        sync_version=1,
    )
    warehouse.insert_slack_sync_state(cursor_ts="page14", status="ok", updated_at=now, **common)
    warehouse.insert_slack_sync_state(
        cursor_ts="", status="complete", updated_at=now + timedelta(minutes=1), **common
    )

    rows = warehouse._query(
        """
        SELECT cursor_ts, status FROM @slack_sync_state
        WHERE object_type = 'conversation_list' AND object_id = 'mpim'
        """
    )
    cursor_ts, status = rows[0]
    # The cursor is preserved by design...
    assert cursor_ts == "page14"
    # ...so the status is the only thing that can mean "begin a new cycle".
    assert status == "complete"


def test_slack_conversation_health_ignores_archived_conversations(
    warehouse: PostgresWarehouse,
) -> None:
    """Archived conversations must not hold the freshness clock hostage.

    Discovery calls conversations.list with exclude_archived=true, so an
    archived conversation's synced_at is frozen at whatever it was when it was
    last active and can never be refreshed. Judging the oldest stamp over ALL
    rows therefore reports 'stale' forever after a perfectly healthy complete
    walk -- measured on production, three archived IMs stuck at 2026-07-06 held
    the im row at 48.9 days old seconds after its walk finished. A monitor that
    cannot ever go green is a monitor everyone learns to ignore.
    """
    warehouse.ensure_slack_tables()
    now = datetime.now(tz=UTC)
    warehouse.insert_slack_conversations(
        [
            _health_conversation_row("D_LIVE", "im", synced_at=now - timedelta(minutes=10)),
            {
                **_health_conversation_row("D_ARCHIVED", "im", synced_at=now - timedelta(days=120)),
                "is_archived": 1,
            },
        ]
    )

    rows = warehouse._query(
        """
        SELECT status, conversation_count, archived_count, oldest_conversation_synced_at
        FROM @marts_ops_slack_conversation_health
        WHERE account = 'zrl' AND conversation_type = 'im'
        """
    )
    status, total, archived, oldest = rows[0]
    assert status == "ok"
    # Both rows are still counted -- the archived one is simply not judged.
    assert (total, archived) == (2, 1)
    assert oldest >= now - timedelta(minutes=11)


def test_slack_conversation_health_tolerates_a_handful_of_unreachable_stragglers(
    warehouse: PostgresWarehouse,
) -> None:
    """Judge the share refreshed, not the single oldest row.

    Excluding archived rows is not enough. A conversation archived *upstream*
    after we last listed it keeps is_archived = 0 in the warehouse forever,
    because the only path that would correct the flag is the same walk that
    excludes it -- so a small tail of rows can never be re-stamped and the
    oldest-row rule reports 'stale' on a perfectly healthy pipeline. Measured
    right after the production walks completed: private_channel 114/115 and
    public_channel 13,165/13,272 refreshed, both flagged stale by the oldest row
    alone.

    The share is what discriminates: those are 99.1% and 99.2%, where the actual
    outage was 200 of 2,597 mpims -- 7.7%.
    """
    warehouse.ensure_slack_tables()
    now = datetime.now(tz=UTC)
    rows = [
        _health_conversation_row(f"C_FRESH_{i}", "private_channel", synced_at=now - timedelta(minutes=5))
        for i in range(99)
    ]
    rows.append(
        _health_conversation_row("C_STRAGGLER", "private_channel", synced_at=now - timedelta(days=120))
    )
    warehouse.insert_slack_conversations(rows)

    status, fraction = warehouse._query(
        """
        SELECT status, refreshed_fraction
        FROM @marts_ops_slack_conversation_health
        WHERE account = 'zrl' AND conversation_type = 'private_channel'
        """
    )[0]
    assert status == "ok"
    assert 0.98 <= float(fraction) <= 0.99


def test_slack_conversation_health_still_catches_the_page_one_outage_by_share(
    warehouse: PostgresWarehouse,
) -> None:
    """The real production shape: one page re-stamped, the rest frozen."""
    warehouse.ensure_slack_tables()
    now = datetime.now(tz=UTC)
    rows = [
        _health_conversation_row(f"C_PAGE1_{i}", "mpim", synced_at=now - timedelta(minutes=5))
        for i in range(8)
    ]
    rows += [
        _health_conversation_row(f"C_TAIL_{i}", "mpim", synced_at=now - timedelta(days=98))
        for i in range(92)
    ]
    warehouse.insert_slack_conversations(rows)

    status, fraction = warehouse._query(
        """
        SELECT status, refreshed_fraction
        FROM @marts_ops_slack_conversation_health
        WHERE account = 'zrl' AND conversation_type = 'mpim'
        """
    )[0]
    assert status == "stale"
    assert abs(float(fraction) - 0.08) < 0.001
