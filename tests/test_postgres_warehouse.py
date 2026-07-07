from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import UTC, datetime, timedelta

import psycopg2
import pytest
from dotenv import load_dotenv

from tests.conftest import make_test_schema

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.slack_sync import SlackApiCallError, SlackSyncRunner

from personal_data_warehouse.schema import (
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
    ATTACHMENT_BACKFILL_STATE_COLUMNS,
    ATTACHMENT_COLUMNS,
    FLOAT_COLUMNS,
    INTEGER_COLUMNS,
    POSTGRES_TABLES,
    SEARCH_SCHEMA_REFRESH_LOCK_ID,
    SEARCH_TEXT_PREVIEW_CHARS,
    TIMESTAMP_COLUMNS,
    PostgresWarehouse,
    _dedupe_conflict_rows,
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


def test_search_view_refresh_takes_advisory_lock(monkeypatch) -> None:
    warehouse = object.__new__(PostgresWarehouse)
    commands: list[tuple[str, tuple | None]] = []

    monkeypatch.setattr(warehouse, "_command", lambda sql, params=None: commands.append((sql, params)))
    monkeypatch.setattr(warehouse, "_relation_exists", lambda _table: False)

    warehouse._ensure_search_views_if_possible()

    assert commands[0] == ("SELECT pg_advisory_lock(%s)", (SEARCH_SCHEMA_REFRESH_LOCK_ID,))
    assert commands[-1] == ("SELECT pg_advisory_unlock(%s)", (SEARCH_SCHEMA_REFRESH_LOCK_ID,))
    assert ("DROP VIEW IF EXISTS searchable_text", None) in commands


def test_search_view_refresh_releases_advisory_lock_on_error(monkeypatch) -> None:
    warehouse = object.__new__(PostgresWarehouse)
    commands: list[tuple[str, tuple | None]] = []

    def command(sql, params=None):
        commands.append((sql, params))
        if sql == "DROP VIEW IF EXISTS searchable_text":
            raise RuntimeError("ddl failed")

    monkeypatch.setattr(warehouse, "_command", command)
    monkeypatch.setattr(warehouse, "_relation_exists", lambda _table: False)

    with pytest.raises(RuntimeError, match="ddl failed"):
        warehouse._ensure_search_views_if_possible()

    assert commands[-1] == ("SELECT pg_advisory_unlock(%s)", (SEARCH_SCHEMA_REFRESH_LOCK_ID,))


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

    rows = warehouse._query("SELECT subject, sync_version FROM gmail_messages WHERE message_id = %s", ("m1",))

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
        "SELECT text, sync_version FROM slack_messages WHERE conversation_id = %s AND message_ts = %s",
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
    provider, model, version = "agent_codex", "", AGENT_ATTACHMENT_PROMPT_VERSION

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
    insert_enrichment("sha-done", ai_provider=provider, ai_model=model, ai_prompt_version=version, status="agent_ok")
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
        model=model,
        prompt_version=version,
        max_error_attempts=3,
        error_window_days=0,
    )

    candidates = load_file_enrichment_candidates(
        warehouse,
        source=GMAIL_SOURCE,
        provider=provider,
        model=model,
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
        model=model,
        prompt_version=version,
        limit=10,
        max_error_attempts=5,
        error_window_days=0,
    )
    assert "sha-flaky" in {candidate["content_sha256"] for candidate in retried}

    insert_enrichment("sha-pending", ai_provider=provider, ai_model=model, ai_prompt_version=version, status="agent_ok")
    insert_enrichment("sha-pdf", ai_provider=provider, ai_model=model, ai_prompt_version=version, status="agent_ok")
    assert not has_file_enrichment_candidate(
        warehouse,
        source=GMAIL_SOURCE,
        provider=provider,
        model=model,
        prompt_version=version,
        max_error_attempts=3,
        error_window_days=0,
    )
    assert has_file_enrichment_candidate(
        warehouse,
        source=GMAIL_SOURCE,
        provider=provider,
        model=model,
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
                model=model,
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
        model=model,
        prompt_version=version,
    )
    candidates = load_file_enrichment_candidates(
        warehouse,
        source=WHATSAPP_SOURCE,
        provider=provider,
        model=model,
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
        model=model,
        prompt_version=version,
    )


def test_postgres_renames_legacy_gmail_attachment_enrichments_table(warehouse: PostgresWarehouse) -> None:
    """The shared file_attachment_enrichments table is the renamed
    gmail_attachment_enrichments. The migration must preserve existing rows and
    leave only the new-named relation + indexes behind."""
    from personal_data_warehouse.schema import ATTACHMENT_ENRICHMENT_COLUMNS

    now = datetime(2026, 6, 1, tzinfo=UTC)
    warehouse.ensure_file_attachment_enrichment_tables()
    warehouse.insert_attachment_enrichments(
        [
            _default_row(
                ATTACHMENT_ENRICHMENT_COLUMNS,
                content_sha256="legacy-sha",
                ai_provider="agent_codex",
                ai_model="",
                ai_prompt_version="gmail-attachment-agent-v1",
                text="legacy enrichment text",
                text_extraction_status="agent_ok",
                updated_at=now,
                sync_version=1,
            )
        ]
    )

    # Simulate a pre-generalization deployment: the table and its indexes still
    # carry the old gmail_attachment_enrichments names.
    warehouse._command("ALTER TABLE file_attachment_enrichments RENAME TO gmail_attachment_enrichments")
    warehouse._command(
        "ALTER INDEX IF EXISTS file_attachment_enrichments_text_bm25_idx "
        "RENAME TO gmail_attachment_enrichments_text_bm25_idx"
    )
    warehouse._command(
        "ALTER INDEX IF EXISTS file_attachment_enrichments_text_trgm_idx "
        "RENAME TO gmail_attachment_enrichments_text_trgm_idx"
    )
    warehouse._command(
        "ALTER INDEX IF EXISTS file_attachment_enrichments_pkey "
        "RENAME TO gmail_attachment_enrichments_pkey"
    )
    assert warehouse._relation_exists("gmail_attachment_enrichments")
    assert not warehouse._relation_exists("file_attachment_enrichments")

    warehouse.ensure_file_attachment_enrichment_tables()

    assert warehouse._relation_exists("file_attachment_enrichments")
    assert not warehouse._relation_exists("gmail_attachment_enrichments")
    preserved = warehouse._query(
        "SELECT text FROM file_attachment_enrichments WHERE content_sha256 = %s",
        ("legacy-sha",),
    )
    assert preserved == [("legacy enrichment text",)]
    index_names = {
        row[0]
        for row in warehouse._query(
            "SELECT indexname FROM pg_indexes WHERE schemaname = current_schema() "
            "AND tablename = 'file_attachment_enrichments'",
            (),
        )
    }
    assert not any(name.startswith("gmail_attachment_enrichments") for name in index_names)


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
        "SELECT name FROM whatsapp_chats WHERE chat_id = '120363274447440808@g.us'"
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
        "SELECT display_name, is_admin FROM whatsapp_chat_participants "
        "WHERE chat_id = '120363274447440808@g.us'"
    )
    assert rows == [("Alice", 1)]


def _wa_message_row(*, chat_id: str, message_id: str, sender_jid: str = "", is_from_me: int = 0,
                    push_name: str = "", body_text: str = "", sync_version: int = 1) -> dict:
    base = datetime(2026, 6, 1, 12, tzinfo=UTC)
    return {
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
        "SELECT chat_id, chat_type FROM whatsapp_chats WHERE account='zach@example.test'"
    ))
    assert kinds["status@broadcast"] == "status"
    assert kinds["222@g.us"] == "group"
    assert kinds["15550001@s.whatsapp.net"] == "user"
    assert kinds["98765@lid"] == "user"
    # Existing named group untouched.
    name = warehouse._query("SELECT name FROM whatsapp_chats WHERE chat_id='111@g.us'")[0][0]
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
        "SELECT message_id, chat_kind FROM clean_whatsapp_messages WHERE account='zach@example.test'"
    ))
    assert rows["s1"] == "status"
    assert rows["g1"] == "group"
    assert rows["d1"] == "user"
    # sender_name resolves via whatsapp_contacts (full_name wins over push_name).
    sender = warehouse._query(
        "SELECT sender_name FROM clean_whatsapp_messages WHERE message_id='d1'"
    )[0][0]
    assert sender == "Alice Example"


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
    assert "slack_messages_user_time_idx" in index_names
    assert "slack_messages_time_idx" in index_names
    assert "slack_messages_text_trgm_idx" in index_names
    # The earlier partial trgm index has been superseded by the full-coverage one.
    assert "slack_messages_text_trgm_live_idx" not in index_names

    slack_user_indexes = warehouse._query(
        """
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = current_schema()
          AND tablename = 'slack_users'
        """
    )
    slack_user_index_names = {row[0] for row in slack_user_indexes}
    assert "slack_users_email_lower_idx" in slack_user_index_names

    extension_rows = warehouse._query("SELECT extname FROM pg_extension WHERE extname = 'pg_trgm'")
    assert extension_rows == [("pg_trgm",)]


def test_postgres_slack_messages_set_autovacuum_storage_parameters(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_slack_tables()

    rows = warehouse._query(
        """
        SELECT unnest(c.reloptions)
        FROM pg_class AS c
        INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE n.nspname = current_schema()
          AND c.relname = 'slack_messages'
        """
    )
    reloptions = {row[0] for row in rows}
    assert "autovacuum_analyze_scale_factor=0" in reloptions
    assert "autovacuum_analyze_threshold=50000" in reloptions
    assert "autovacuum_vacuum_scale_factor=0" in reloptions
    assert "autovacuum_vacuum_threshold=100000" in reloptions


def test_postgres_ensure_indexes_drops_obsolete_indexes(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_slack_tables()
    # Recreate the legacy partial index out-of-band, simulating an existing deployment
    # that ran on an older revision before the full-coverage index was introduced.
    warehouse._command(
        "CREATE INDEX IF NOT EXISTS slack_messages_text_trgm_live_idx "
        "ON slack_messages USING gin (text public.gin_trgm_ops) WHERE is_deleted = 0"
    )
    pre_rows = warehouse._query(
        "SELECT indexname FROM pg_indexes WHERE schemaname = current_schema() "
        "AND tablename = 'slack_messages' AND indexname = 'slack_messages_text_trgm_live_idx'"
    )
    assert pre_rows, "test setup failed: legacy index should exist before re-running ensure"

    warehouse.ensure_slack_tables()

    post_rows = warehouse._query(
        "SELECT indexname FROM pg_indexes WHERE schemaname = current_schema() "
        "AND tablename = 'slack_messages' AND indexname = 'slack_messages_text_trgm_live_idx'"
    )
    assert post_rows == []


def test_postgres_gmail_tables_create_search_indexes(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()

    rows = warehouse._query(
        """
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = current_schema()
          AND tablename = 'gmail_messages'
        """
    )

    index_names = {row[0] for row in rows}
    assert "gmail_messages_internal_date_idx" in index_names
    assert "gmail_messages_from_trgm_idx" in index_names
    assert "gmail_messages_subject_trgm_idx" in index_names
    assert "gmail_messages_snippet_trgm_idx" in index_names
    assert "gmail_messages_body_text_trgm_idx" in index_names
    assert "gmail_messages_body_markdown_trgm_idx" in index_names
    assert "gmail_messages_body_html_trgm_idx" in index_names


def test_postgres_agent_tables_create_run_lookup_index(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_agent_tables()

    rows = warehouse._query(
        """
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = current_schema()
          AND tablename = 'agent_runs'
        """
    )

    index_names = {row[0] for row in rows}
    assert "agent_runs_task_status_subject_idx" in index_names


def _pg_textsearch_usable(warehouse: PostgresWarehouse) -> bool:
    # The extension files must be installed AND the library preloaded;
    # CREATE EXTENSION fails without both.
    rows = warehouse._query(
        "SELECT 1 FROM pg_available_extensions WHERE name = 'pg_textsearch'"
        " AND current_setting('shared_preload_libraries') LIKE '%pg_textsearch%'"
    )
    return bool(rows)


def test_postgres_slack_tables_create_bm25_index_and_rank_matches(warehouse: PostgresWarehouse) -> None:
    if not _pg_textsearch_usable(warehouse):
        pytest.skip("pg_textsearch is not installed/preloaded on this Postgres host")

    warehouse.ensure_slack_tables()

    rows = warehouse._query(
        "SELECT indexname FROM pg_indexes WHERE schemaname = current_schema() "
        "AND tablename = 'slack_messages' AND indexname = 'slack_messages_text_bm25_idx'"
    )
    assert rows, "bm25 index should be created when pg_textsearch is usable"

    message_datetime = datetime(2026, 5, 19, 12, tzinfo=UTC)
    warehouse.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id="C1",
                message_ts="100.1",
                message_datetime=message_datetime,
                text="deploying the staging cluster today",
            ),
            _slack_message_row(
                conversation_id="C1",
                message_ts="100.2",
                message_datetime=message_datetime,
                text="lunch plans for friday",
            ),
        ]
    )

    # pg_textsearch resolves its helper functions and the implicit
    # col <@> 'query' index lookup through the search_path, and the implicit
    # form only finds indexes in the default (public) schema. Production runs
    # with schema=public so the bare implicit syntax works there; in this
    # schema-isolated test, put public on the search_path and name the index
    # explicitly via to_bm25query.
    warehouse._command(f'SET search_path TO "{warehouse._schema}", public')
    rows = warehouse._query(
        "SELECT text FROM slack_messages "
        "ORDER BY text <@> to_bm25query('staging cluster deploy', 'slack_messages_text_bm25_idx') LIMIT 1"
    )
    assert rows == [("deploying the staging cluster today",)]


def test_postgres_ensure_indexes_tolerates_missing_pg_textsearch(warehouse: PostgresWarehouse, monkeypatch) -> None:
    original_command = warehouse._command

    def failing_command(sql, params=None):
        if "CREATE EXTENSION IF NOT EXISTS pg_textsearch" in sql:
            raise RuntimeError("pg_textsearch unavailable")
        return original_command(sql, params)

    monkeypatch.setattr(warehouse, "_command", failing_command)

    # Must not raise: hosts without the extension skip bm25 indexes but keep
    # creating everything else.
    warehouse.ensure_slack_tables()

    index_names = {
        row[0]
        for row in warehouse._query(
            "SELECT indexname FROM pg_indexes WHERE schemaname = current_schema() AND tablename = 'slack_messages'"
        )
    }
    assert "slack_messages_text_bm25_idx" not in index_names
    assert "slack_messages_text_trgm_idx" in index_names


def _ensure_all_table_groups(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()
    warehouse.ensure_calendar_tables()
    warehouse.ensure_contacts_tables()
    warehouse.ensure_apple_voice_memos_tables(backfill_content_hashes=False)
    warehouse.ensure_apple_notes_tables()
    warehouse.ensure_apple_messages_tables()
    warehouse.ensure_whatsapp_tables()
    warehouse.ensure_agent_sessions_tables()
    warehouse.ensure_slack_tables()
    warehouse.ensure_upstream_mutation_tables()
    warehouse.ensure_google_drive_source_tables()


def _search_text_index_names() -> set[str]:
    """The bm25 index names search_text() references via to_bm25query(), pulled
    straight from the generated function SQL (no DB needed)."""
    import re

    sql = _search_text_function_sql()
    return set(re.findall(r"to_bm25query\([^,]+,\s*'([a-z0-9_]+)'\)", sql))


def _search_text_function_sql() -> str:
    import personal_data_warehouse.postgres as postgres_module

    captured: list[str] = []

    class _Capture:
        def _command(self, sql: str) -> None:
            captured.append(sql)

    postgres_module.PostgresWarehouse._ensure_search_text_function(_Capture())
    return captured[0]


def _search_text_branch_source_labels() -> list[str]:
    """The per-branch source labels search_text() filters on, parsed from the
    generated `branch_sources` array in the function SQL (no DB needed)."""
    import re

    sql = _search_text_function_sql()
    match = re.search(r"branch_sources text\[\] := ARRAY\[(.*?)\]", sql, re.DOTALL)
    assert match, "expected search_text() to declare a branch_sources array"
    return re.findall(r"'([a-z0-9_]+)'", match.group(1))


def _search_text_sources_helper_labels() -> list[str]:
    """The labels enumerated by the search_text_sources() helper, parsed from its
    VALUES list in the generated SQL (no DB needed)."""
    import re

    sql = _search_text_function_sql()
    match = re.search(
        r"CREATE OR REPLACE FUNCTION search_text_sources\(\).*?\$sources\$(.*?)\$sources\$",
        sql,
        re.DOTALL,
    )
    assert match, "expected search_text_sources() to be defined alongside search_text()"
    return re.findall(r"\('([a-z0-9_]+)'\)", match.group(1))


def test_search_text_sources_helper_matches_branch_labels() -> None:
    # search_text_sources() exists so a caller can discover the exact (terse)
    # tokens search_text()'s `sources` arg accepts. The labels are terse and do
    # not match the tool-help prose names (apple notes => 'note', meeting
    # transcripts => 'transcript', ...), and an unknown token is silently ignored
    # (returns nothing) rather than erroring, so guessing fails quietly. The
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
        "search_text() must not use bm25_get_current_score() (unreliable off the "
        "index-scan path); recompute the bm25 operator in the SELECT list instead"
    )


def test_search_text_merge_guarantees_per_source_floor() -> None:
    # BM25 scores are not comparable across corpora, so the final merge must not
    # be a flat global `ORDER BY score LIMIT` — that buries a low-volume source
    # (one matching contact card / Drive doc) under a high-volume one (dozens of
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


def test_search_text_ranks_across_sources_via_bm25(warehouse: PostgresWarehouse) -> None:
    if not _pg_textsearch_usable(warehouse):
        pytest.skip("pg_textsearch is not installed/preloaded on this Postgres host")

    _ensure_all_table_groups(warehouse)
    # search_text() uses the explicit to_bm25query('q', 'index_name') form plus the
    # public-schema bm25 helpers; the schema-isolated test connection needs public on
    # the search_path for them to resolve (production runs with schema=public).
    warehouse._command(f'SET search_path TO "{warehouse._schema}", public')

    # Guard: every bm25 index search_text() names via to_bm25query must actually be
    # built. _ensure_indexes swallows DDL errors, so a missing/typo'd index would
    # otherwise only blow up (UndefinedObject) when the function is called.
    referenced = _search_text_index_names()
    built = {
        row[0]
        for row in warehouse._query(
            "SELECT indexname FROM pg_indexes WHERE schemaname = current_schema()"
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
    # query term must surface through the whatsapp_media content branch, which
    # bm25-ranks the shared file_attachment_enrichments table then joins the media
    # row. This covers the new branch end to end (function compiles + returns).
    from personal_data_warehouse.schema import ATTACHMENT_ENRICHMENT_COLUMNS, WHATSAPP_MEDIA_ITEM_COLUMNS

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
    # same shared table) must surface through the apple_message_media content
    # branch, which bm25-ranks file_attachment_enrichments then joins the
    # attachment row.
    from personal_data_warehouse.schema import APPLE_MESSAGE_ATTACHMENT_COLUMNS

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

    # BM25 non-matches score 0; matches score negative. Isolate matches with score < 0
    # so the assertions hold regardless of how few total rows the fixture has.
    matched = warehouse._query(
        "SELECT source, subsource, ref FROM search_text('zanzibar rollout', 20) WHERE score < 0"
    )
    matched_sources = {(row[0], row[1]) for row in matched}
    matched_refs = {row[2] for row in matched}
    assert ("slack", "private_channel") in matched_sources
    assert ("gmail", "subject") in matched_sources
    assert ("whatsapp_media", "content") in matched_sources
    assert ("apple_message_media", "content") in matched_sources
    assert any("100.1" in ref for ref in matched_refs)  # the zanzibar slack message
    assert any("chat-1:wamid-1" == ref for ref in matched_refs)  # the whatsapp media content row
    assert any("imsg-1:att-1" == ref for ref in matched_refs)  # the iMessage attachment content row
    assert all("100.2" not in ref for ref in matched_refs)  # the unrelated lunch message

    # sources filter restricts the fan-out.
    gmail_only = warehouse._query(
        "SELECT DISTINCT source FROM search_text('zanzibar', 20, ARRAY['gmail']) WHERE score < 0"
    )
    assert gmail_only == [("gmail",)]

    # since filter excludes rows dated before the cutoff (fixtures are 2026-05-19).
    after_cutoff = warehouse._query(
        "SELECT count(*) FROM search_text('zanzibar', 20, NULL, '2027-01-01'::timestamptz) WHERE score < 0"
    )
    assert after_cutoff == [(0,)]

    # search_text() must run on the read-only query surface (the MCP/CLI tool is
    # read-only), so it may not do DDL/DML at call time. Run it under a genuine
    # read-only transaction and assert it still returns the matches.
    warehouse._command("SET default_transaction_read_only = on")
    try:
        read_only = warehouse._query(
            "SELECT source, subsource FROM search_text('zanzibar rollout', 20) WHERE score < 0"
        )
    finally:
        warehouse._command("SET default_transaction_read_only = off")
    read_only_sources = {(row[0], row[1]) for row in read_only}
    assert ("slack", "private_channel") in read_only_sources
    assert ("gmail", "subject") in read_only_sources

    # Resilience: a missing/unusable bm25 index must drop only its own source,
    # not break the whole function. Dropping unrelated bm25 indexes must leave
    # the slack + gmail matches intact and must not raise.
    warehouse._command("DROP INDEX apple_voice_memos_title_bm25_idx")
    warehouse._command("DROP INDEX contact_cards_name_bm25_idx")
    survived = warehouse._query(
        "SELECT source, subsource FROM search_text('zanzibar rollout', 20) WHERE score < 0"
    )
    survived_sources = {(row[0], row[1]) for row in survived}
    assert ("slack", "private_channel") in survived_sources
    assert ("gmail", "subject") in survived_sources


def test_search_text_sources_lists_accepted_filter_tokens(warehouse: PostgresWarehouse) -> None:
    # search_text_sources() must return, on the read-only query surface, exactly
    # the tokens search_text()'s `sources` filter accepts — the discoverable
    # source of truth for the terse labels (note/transcript/agent_session/...).
    if not _pg_textsearch_usable(warehouse):
        pytest.skip("pg_textsearch is not installed/preloaded on this Postgres host")

    _ensure_all_table_groups(warehouse)
    warehouse._command(f'SET search_path TO "{warehouse._schema}", public')

    expected = sorted(set(_search_text_branch_source_labels()))
    assert expected, "expected search_text() to declare branch source labels"

    # The MCP/CLI tool is read-only, so search_text_sources() must run under a
    # genuine read-only transaction (no DDL/DML at call time).
    warehouse._command("SET default_transaction_read_only = on")
    try:
        rows = warehouse._query("SELECT source FROM search_text_sources() ORDER BY source")
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
            "SELECT count(*) FROM search_text('zzqqxx', 5, ARRAY[%s])",
            (label,),
        )


def test_search_text_rejects_unknown_source_tokens(warehouse: PostgresWarehouse) -> None:
    # An unknown `sources` token must RAISE (pointing the caller at
    # search_text_sources()), NOT silently return nothing. The accepted tokens
    # are terse and differ from the prose source names, so callers reliably guess
    # wrong ('apple_messages'/'file_attachment'/'whatsapp_media' instead of the
    # real 'imessage'/'gmail_attachment'/'whatsapp'). A silent empty result reads
    # as "nothing matched" and yields confident wrong answers; the failure must
    # be loud and self-correcting.
    if not _pg_textsearch_usable(warehouse):
        pytest.skip("pg_textsearch is not installed/preloaded on this Postgres host")

    _ensure_all_table_groups(warehouse)
    warehouse._command(f'SET search_path TO "{warehouse._schema}", public')

    with pytest.raises(psycopg2.Error, match="unknown source"):
        warehouse._query("SELECT * FROM search_text('zzqqxx', 5, ARRAY['apple_messages'])")

    # A mix of one valid and one invalid token still raises (no partial silent
    # drop of the unknown one).
    with pytest.raises(psycopg2.Error, match="unknown source"):
        warehouse._query("SELECT * FROM search_text('zzqqxx', 5, ARRAY['imessage', 'bogus'])")

    # A valid token is unaffected.
    warehouse._query("SELECT count(*) FROM search_text('zzqqxx', 5, ARRAY['imessage'])")


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
    warehouse._command(f'SET search_path TO "{warehouse._schema}", public')

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

    matched = warehouse._query(
        "SELECT DISTINCT source FROM search_text('zanzibar rollout', 50) WHERE score < 0"
    )
    sources = {row[0] for row in matched}
    assert "slack" in sources
    assert "agent" not in sources

    # 'agent' is not a valid source token — the internal agent branch was removed
    # and the agent *sessions* token is 'agent_session' — so explicitly
    # requesting it now raises (unknown-source guard) rather than silently
    # returning nothing.
    with pytest.raises(psycopg2.Error, match="unknown source"):
        warehouse._query("SELECT count(*) FROM search_text('zanzibar', 50, ARRAY['agent'])")


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
    warehouse._command(f'SET search_path TO "{warehouse._schema}", public')

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

    rows = warehouse._query(
        "SELECT ref, text FROM search_text('zanzibar', 50, ARRAY['slack']) WHERE score < 0"
    )
    by_ref = {row[0]: row[1] for row in rows}
    long_ref = next(ref for ref in by_ref if ref.endswith("300.1"))
    short_ref = next(ref for ref in by_ref if ref.endswith("300.2"))

    # The oversized hit is truncated to exactly the preview cap, and the preview
    # is a genuine prefix of the stored text (no corruption, no padding).
    assert len(by_ref[long_ref]) == SEARCH_TEXT_PREVIEW_CHARS
    assert long_body.startswith(by_ref[long_ref])
    # A normal-length hit is returned untouched — the cap never shortens it.
    assert by_ref[short_ref] == short_body


def test_search_text_low_volume_source_survives_high_volume_source(warehouse: PostgresWarehouse) -> None:
    # A low-volume source (one matching contact card) must surface in a bare
    # cross-source search even when a high-volume source (many slack hits) would
    # dominate a flat global score LIMIT. BM25 scores are not comparable across
    # corpora, so the merge guarantees each source's top hits ahead of the cut;
    # without that, the single contact card ranked far down the global list and a
    # "who is X" question wrongly returned nothing from contacts.
    if not _pg_textsearch_usable(warehouse):
        pytest.skip("pg_textsearch is not installed/preloaded on this Postgres host")

    _ensure_all_table_groups(warehouse)
    warehouse._command(f'SET search_path TO "{warehouse._schema}", public')

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

    # A small max_results makes the global race tight: the per-source floor must
    # still let the lone contact hit through.
    sources = {
        row[0]
        for row in warehouse._query(
            "SELECT source FROM search_text('zanzibar', 4) WHERE score < 0"
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

    rows = warehouse._query(
        """
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND (
            (table_name = 'contact_cards' AND column_name IN ('emails', 'nicknames', 'raw_json'))
            OR (table_name = 'slack_conversations' AND column_name = 'raw_json')
          )
        ORDER BY table_name, column_name
        """
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
    warehouse._command("ALTER TABLE contact_cards ADD COLUMN IF NOT EXISTS nicknames jsonb NOT NULL DEFAULT '[]'::jsonb")
    warehouse._command(
        """
        CREATE OR REPLACE VIEW clean_contacts AS
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
        FROM contact_cards
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
            WHERE table_schema = current_schema()
              AND table_name = 'clean_contacts'
            ORDER BY ordinal_position
            """
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
        FROM clean_contacts
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


def test_ensure_view_replaces_view_whose_columns_cannot_be_dropped(warehouse: PostgresWarehouse) -> None:
    warehouse._command("CREATE VIEW ensure_view_fixture AS SELECT 1 AS a, 2 AS b")

    warehouse._ensure_view(
        "ensure_view_fixture",
        "CREATE OR REPLACE VIEW ensure_view_fixture AS SELECT 1 AS a",
    )

    assert warehouse._query("SELECT * FROM ensure_view_fixture") == [(1,)]


def test_postgres_ensure_contacts_recovers_when_existing_view_has_extra_columns(
    warehouse: PostgresWarehouse,
) -> None:
    warehouse.ensure_contacts_tables()

    # Reproduce out-of-band drift: another checkout ran a newer definition
    # against the shared database, leaving clean_contacts with a trailing
    # column this code's definition does not select. CREATE OR REPLACE VIEW
    # cannot drop view columns, so every ensure used to fail until the
    # definitions matched again.
    viewdef = warehouse._query("SELECT pg_get_viewdef('clean_contacts'::regclass)")[0][0]
    warehouse._command("DROP VIEW clean_contacts")
    warehouse._command(
        "CREATE VIEW clean_contacts AS "
        f"SELECT base.*, 'drift'::text AS drift_extra FROM ({viewdef.strip().rstrip(';')}) AS base"
    )

    warehouse.ensure_contacts_tables()

    columns = [
        row[0]
        for row in warehouse._query(
            """
            SELECT attname
            FROM pg_attribute
            WHERE attrelid = 'clean_contacts'::regclass AND attnum > 0 AND NOT attisdropped
            ORDER BY attnum
            """
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
        "SELECT conversation_id, is_archived FROM slack_conversations ORDER BY conversation_id"
    )
    assert dict(archived) == {"C_GONE": 1, "C_OK": 0}

    # Subsequent active loads now skip the archived channel entirely.
    active_ids = {p["id"] for p in warehouse.load_slack_conversation_payloads(account="zrl", team_id="T1")}
    assert active_ids == {"C_OK"}

    # The healthy channel's message was persisted.
    persisted = warehouse._query(
        "SELECT conversation_id FROM slack_messages WHERE is_deleted = 0 ORDER BY conversation_id"
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
    assert "FROM slack_messages" not in captured["sql"]
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


def test_postgres_gmail_clean_inbox_preview_uses_byte_prefix(warehouse: PostgresWarehouse) -> None:
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
