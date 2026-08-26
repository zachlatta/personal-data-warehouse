"""The marts layer's cross-source contract, and its agreement with live prod.

Zach's C5 contract: a concept fed by several sources gets each source synced
into ``base_<source>``, modelled in ``derived_<domain>``, published through a
``marts_<domain>`` read interface, and surfaced on the timeline. An agent walks
timeline → marts → base. Two domains had no middle: voice memos (two unrelated
sources, a full derived layer, and no mart at all) and messages (two per-source
views sharing exactly one column name), so every consumer hand-rolled the
UNION and the "latest enrichment per recording" de-duplication.

The last three tests are different in kind: they diff the *live* warehouse
against the catalog. ``tests/test_schema_reorg_contract.py`` provisions a fresh
throwaway schema, so it structurally cannot see an object that exists only in
production — which is how ``search_lab``, six shadow copies of the entire search
API, sat in prod uncataloged. The same shape (a stale duplicate of
``search_text``) returned zero rows for sixteen days in July.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import pytest
from dotenv import load_dotenv

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.postgres import (
    ARRAY_COLUMNS,
    DATE_COLUMNS,
    FLOAT_COLUMNS,
    INTEGER_COLUMNS,
    JSONB_ARRAY_COLUMNS_BY_TABLE,
    TIMESTAMP_COLUMNS,
    PostgresWarehouse,
)
from personal_data_warehouse.relations import CATALOG, relation
from personal_data_warehouse.schema import (
    ALICE_VOICE_RECORDING_COLUMNS,
    APPLE_MESSAGE_CHAT_COLUMNS,
    APPLE_MESSAGE_CHAT_MESSAGE_COLUMNS,
    APPLE_MESSAGE_COLUMNS,
    APPLE_MESSAGE_HANDLE_COLUMNS,
    CALENDAR_EVENT_COLUMNS,
    CONTACT_CARD_COLUMNS,
    VOICE_MEMO_ENRICHMENT_COLUMNS,
    VOICE_MEMO_FILE_COLUMNS,
    VOICE_MEMO_TRANSCRIPTION_RUN_COLUMNS,
    VOICE_MEMO_TRANSCRIPT_SEGMENT_COLUMNS,
    WHATSAPP_CHAT_COLUMNS,
    WHATSAPP_MESSAGE_COLUMNS,
    WHOOP_CYCLE_COLUMNS,
    WHOOP_PRIVATE_CYCLE_COLUMNS,
    WHOOP_PRIVATE_HEART_RATE_SAMPLE_COLUMNS,
    WHOOP_PRIVATE_RECOVERY_COLUMNS,
    WHOOP_PRIVATE_SLEEP_COLUMNS,
    WHOOP_PRIVATE_SPORT_COLUMNS,
    WHOOP_PRIVATE_WORKOUT_COLUMNS,
    WHOOP_RECOVERY_COLUMNS,
    WHOOP_SLEEP_COLUMNS,
    WHOOP_WORKOUT_COLUMNS,
)
from personal_data_warehouse.warehouse_catalog import CatalogObject

TS = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _row(columns: tuple[str, ...], *, table: str = "", **overrides):
    """A fully-defaulted row for one table spec, like the warehouse writes."""
    jsonb_arrays = JSONB_ARRAY_COLUMNS_BY_TABLE.get(table, set())
    row: dict = {}
    for column in columns:
        if column in ARRAY_COLUMNS or column in jsonb_arrays:
            row[column] = []
        elif column in TIMESTAMP_COLUMNS:
            row[column] = EPOCH
        elif column in DATE_COLUMNS:
            row[column] = EPOCH.date()
        elif column in INTEGER_COLUMNS:
            row[column] = 0
        elif column in FLOAT_COLUMNS:
            row[column] = 0.0
        else:
            row[column] = ""
    row.update(overrides)
    return row


def _postgres_url() -> str:
    load_dotenv()
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if not url:
        if os.environ.get("CI"):
            pytest.fail("POSTGRES_DATABASE_URL must be configured in CI")
        pytest.skip("POSTGRES_DATABASE_URL is not set")
    return url


@pytest.fixture()
def warehouse():
    wh = PostgresWarehouse(_postgres_url(), schema=make_test_schema("marts"))
    try:
        yield wh
    finally:
        cleanup_test_warehouse(wh)


def _rows(wh: PostgresWarehouse, logical: str, order_by: str = "") -> list[tuple]:
    sql = f"SELECT * FROM {wh.sql_relation(logical)}"
    if order_by:
        sql += f" ORDER BY {order_by}"
    return wh._query(sql)


def _columns(wh: PostgresWarehouse, logical: str) -> list[str]:
    rel = relation(logical).with_namespace(wh.schema_namespace)
    return [
        row[0]
        for row in wh._query(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (rel.schema, rel.name),
        )
    ]


def _column_types(wh: PostgresWarehouse, logical: str) -> list[tuple[str, str]]:
    rel = relation(logical).with_namespace(wh.schema_namespace)
    return [
        (row[0], row[1])
        for row in wh._query(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (rel.schema, rel.name),
        )
    ]


def _dicts(wh: PostgresWarehouse, logical: str, order_by: str = "") -> list[dict]:
    columns = _columns(wh, logical)
    return [dict(zip(columns, row)) for row in _rows(wh, logical, order_by)]


# ---------------------------------------------------------------------------
# catalog contract
# ---------------------------------------------------------------------------


def _obj(logical: str) -> CatalogObject:
    return CATALOG.object(logical)


def test_new_cross_source_marts_are_cataloged_as_public_marts_views() -> None:
    for logical, location in (
        ("marts_voice_memos_recordings", ("marts_voice_memos", "recordings")),
        ("marts_voice_memos_transcript_segments", ("marts_voice_memos", "transcript_segments")),
        ("marts_messages_messages", ("marts_messages", "messages")),
    ):
        obj = _obj(logical)
        assert (obj.schema, obj.name) == location, logical
        assert obj.kind == "view", logical
        assert obj.layer == "marts", logical
        assert obj.discoverable and obj.query_access == "public", logical


def test_every_marts_schema_names_its_entry_point_relation() -> None:
    """A schema comment that says only "start with timeline" wastes the slot.

    The reader is already in ``marts_photos``; what they need next is which of
    its relations to read first.
    """
    for schema in CATALOG.schemas:
        if schema.layer != "marts":
            continue
        relations = [obj for obj in CATALOG.objects if obj.schema == schema.name]
        assert relations, schema.name
        assert any(
            f"{schema.name}.{obj.name}" in schema.comment for obj in relations
        ), f"{schema.name} comment names no relation in the schema: {schema.comment!r}"


def test_marts_entry_points_and_honest_limits_carry_relation_comments() -> None:
    """``obj_description`` was NULL for every marts relation before this.

    The three Plaid-only ``marts_finance`` passthroughs are the sharpest case:
    a domain-mart name promises cross-source and the SQL reads one source, so
    the comment has to say so.
    """
    for logical in (
        "marts_messages_messages",
        "marts_voice_memos_recordings",
        "clean_contacts",
        "clean_photos",
        "timeline_events",
        "apple_voice_memos_enrichments",
    ):
        assert _obj(logical).comment.strip(), logical

    for logical in (
        "marts_finance_investment_holdings",
        "marts_finance_investment_transactions",
        "marts_finance_liabilities",
    ):
        comment = _obj(logical).comment
        assert "Plaid" in comment, logical
        assert "base_plaid" in comment, logical


def test_catalog_comments_reach_postgres(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_apple_messages_tables()
    warehouse.ensure_whatsapp_tables()

    rel = relation("marts_messages_messages").with_namespace(warehouse.schema_namespace)
    comment = warehouse._query(
        "SELECT obj_description(to_regclass(%s), 'pg_class')",
        (f'"{rel.schema}"."{rel.name}"',),
    )[0][0]
    assert comment == _obj("marts_messages_messages").comment


# ---------------------------------------------------------------------------
# marts_voice_memos.recordings
# ---------------------------------------------------------------------------


def _seed_voice_memos(wh: PostgresWarehouse) -> None:
    wh.ensure_apple_voice_memos_tables(backfill_content_hashes=False)
    wh.ensure_alice_voice_recordings_tables()

    wh.insert_apple_voice_memos_files(
        [
            _row(
                VOICE_MEMO_FILE_COLUMNS,
                account="zach",
                recording_id="rec-1",
                title="New Recording 3",
                filename="1.m4a",
                content_type="audio/mp4",
                content_sha256="sha-1",
                recorded_at=TS,
                # The uploader carries the app's own duration here.
                raw_metadata_json='{"recording": {"duration_seconds": 90.5}}',
                ingested_at=TS,
                sync_version=1,
            ),
            _row(
                VOICE_MEMO_FILE_COLUMNS,
                account="zach",
                recording_id="rec-2",
                title="Hand typed title",
                filename="2.m4a",
                content_type="audio/mp4",
                content_sha256="sha-2",
                recorded_at=TS,
                ingested_at=TS,
                sync_version=1,
            ),
        ]
    )
    # Two completed enrichments for one recording, plus a failed one for
    # another: the mart must expose the newest completed attempt, once.
    older = datetime(2026, 8, 2, tzinfo=UTC)
    newer = datetime(2026, 8, 3, tzinfo=UTC)
    wh.insert_apple_voice_memos_enrichments(
        [
            _row(
                VOICE_MEMO_ENRICHMENT_COLUMNS,
                source="apple_voice_memos",
                account="zach",
                recording_id="rec-1",
                content_sha256="sha-1",
                provider="agent",
                model="m",
                prompt_version="v1",
                status="completed",
                calendar_event_id="cal-old",
                calendar_confidence=0.4,
                title="Stale title",
                transcript="old transcript",
                summary="old summary",
                participants_json='["a"]',
                action_items_json="[]",
                created_at=older,
                sync_version=1,
            ),
            _row(
                VOICE_MEMO_ENRICHMENT_COLUMNS,
                source="apple_voice_memos",
                account="zach",
                recording_id="rec-1",
                content_sha256="sha-1",
                provider="agent",
                model="m",
                prompt_version="v2",
                status="completed",
                calendar_event_id="cal-1",
                calendar_confidence=0.9,
                title="Quarterly planning",
                transcript="new transcript",
                summary="new summary",
                participants_json='["a","b"]',
                action_items_json='[{"item":"ship it"}]',
                created_at=newer,
                sync_version=2,
            ),
            _row(
                VOICE_MEMO_ENRICHMENT_COLUMNS,
                source="apple_voice_memos",
                account="zach",
                recording_id="rec-2",
                content_sha256="sha-2",
                provider="agent",
                model="m",
                prompt_version="v2",
                status="error",
                error="boom",
                title="Never use me",
                created_at=newer,
                sync_version=2,
            ),
        ]
    )
    wh.insert_apple_voice_memos_transcription_runs(
        [
            _row(
                VOICE_MEMO_TRANSCRIPTION_RUN_COLUMNS,
                source="apple_voice_memos",
                account="zach",
                recording_id="rec-2",
                content_sha256="sha-2",
                provider="assemblyai",
                provider_transcript_id="tid-2",
                status="completed",
                transcript_text="run transcript",
                requested_at=TS,
                completed_at=TS,
                sync_version=1,
            )
        ]
    )
    wh.insert_apple_voice_memos_transcript_segments(
        [
            _row(
                VOICE_MEMO_TRANSCRIPT_SEGMENT_COLUMNS,
                source="apple_voice_memos",
                account="zach",
                recording_id="rec-1",
                provider="assemblyai",
                provider_transcript_id="tid-1",
                segment_index=0,
                speaker_label="A",
                start_ms=0,
                end_ms=1000,
                confidence=0.9,
                text="hello",
                created_at=TS,
                sync_version=1,
            ),
            _row(
                VOICE_MEMO_TRANSCRIPT_SEGMENT_COLUMNS,
                source="apple_voice_memos",
                account="zach",
                recording_id="rec-1",
                provider="assemblyai",
                provider_transcript_id="tid-1",
                segment_index=1,
                speaker_label="B",
                start_ms=60000,
                end_ms=61000,
                confidence=0.9,
                text="goodbye",
                created_at=TS,
                sync_version=1,
            ),
        ]
    )
    wh.insert_alice_voice_recordings(
        [
            _row(
                ALICE_VOICE_RECORDING_COLUMNS,
                account="zach",
                recording_id="alice-1",
                title="Standup",
                filename="standup.m4a",
                content_type="audio/mp4",
                content_sha256="sha-3",
                recorded_at=TS,
                duration_seconds=300,
                recording_page_url="https://example.invalid/r/alice-1",
                recovery_source="gmail",
                ingested_at=TS,
                sync_version=1,
            )
        ]
    )


def test_voice_memo_mart_is_one_row_per_recording_across_both_sources(
    warehouse: PostgresWarehouse,
) -> None:
    _seed_voice_memos(warehouse)
    rows = _dicts(warehouse, "marts_voice_memos_recordings", "source, recording_id")

    assert [(row["source"], row["recording_id"]) for row in rows] == [
        ("alice_voice_recordings", "alice-1"),
        ("apple_voice_memos", "rec-1"),
        ("apple_voice_memos", "rec-2"),
    ]


def test_voice_memo_mart_resolves_the_latest_completed_enrichment_once(
    warehouse: PostgresWarehouse,
) -> None:
    """802 enrichment rows cover 597 recordings; the mart owns the DISTINCT ON.

    Before this the de-duplication was copy-pasted into the timeline adapter and
    both marts_calendar views, so a change to the tie-break had three homes.
    """
    _seed_voice_memos(warehouse)
    by_id = {
        (row["source"], row["recording_id"]): row
        for row in _dicts(warehouse, "marts_voice_memos_recordings")
    }

    enriched = by_id[("apple_voice_memos", "rec-1")]
    assert enriched["title"] == "Quarterly planning"
    assert enriched["summary"] == "new summary"
    assert enriched["transcript"] == "new transcript"
    assert enriched["calendar_event_id"] == "cal-1"
    assert enriched["enrichment_prompt_version"] == "v2"
    assert enriched["duration_seconds"] == 90.5

    # An enrichment that failed is not an enrichment: rec-2 keeps its own title
    # and falls back to the transcription run for text.
    unenriched = by_id[("apple_voice_memos", "rec-2")]
    assert unenriched["title"] == "Hand typed title"
    assert unenriched["summary"] is None
    assert unenriched["calendar_event_id"] is None
    assert unenriched["transcript"] == "run transcript"


def test_voice_memo_mart_emits_null_where_a_source_has_no_equivalent(
    warehouse: PostgresWarehouse,
) -> None:
    _seed_voice_memos(warehouse)
    alice = next(
        row
        for row in _dicts(warehouse, "marts_voice_memos_recordings")
        if row["source"] == "alice_voice_recordings"
    )
    assert alice["title"] == "Standup"
    assert alice["duration_seconds"] == 300
    assert alice["recording_url"] == "https://example.invalid/r/alice-1"
    # NULL because nothing has enriched THIS recording yet -- not because the
    # source is structurally excluded. The mart used to hardcode NULL for the
    # whole Alice branch, and both enrichment passes scanned
    # base_apple_voice_memos.files, so the NULLs were self-fulfilling:
    # 53 recordings, 0 transcripts, 0 summaries, every registry green.
    for column in (
        "summary",
        "transcript",
        "participants_json",
        "action_items_json",
        "calendar_event_id",
        "calendar_confidence",
        "enriched_at",
    ):
        assert alice[column] is None, column


def test_voice_memo_mart_surfaces_a_second_sources_transcript_and_summary(
    warehouse: PostgresWarehouse,
) -> None:
    """The defect that made this contract necessary, as a test.

    Enrichment output is keyed by source, so a non-Apple recording's transcript
    and summary reach the mart through exactly the same columns Apple's do. If
    the union ever hardcodes NULL for a branch again, this fails.
    """
    _seed_voice_memos(warehouse)
    warehouse.insert_apple_voice_memos_transcription_runs(
        [
            _row(
                VOICE_MEMO_TRANSCRIPTION_RUN_COLUMNS,
                source="alice_voice_recordings",
                account="zach",
                recording_id="alice-1",
                content_sha256="sha-3",
                provider="assemblyai",
                status="completed",
                transcript_text="alice run transcript",
                requested_at=TS,
                completed_at=TS,
                sync_version=1,
            )
        ]
    )
    warehouse.insert_apple_voice_memos_enrichments(
        [
            _row(
                VOICE_MEMO_ENRICHMENT_COLUMNS,
                source="alice_voice_recordings",
                account="zach",
                recording_id="alice-1",
                content_sha256="sha-3",
                provider="agent",
                model="m",
                prompt_version="v2",
                status="completed",
                title="Morning walk debrief",
                transcript="alice enriched transcript",
                summary="alice summary",
                created_at=TS,
                sync_version=1,
            )
        ]
    )

    rows = {
        (row["source"], row["recording_id"]): row
        for row in _dicts(warehouse, "marts_voice_memos_recordings")
    }
    alice = rows[("alice_voice_recordings", "alice-1")]
    assert alice["title"] == "Morning walk debrief"
    assert alice["summary"] == "alice summary"
    assert alice["transcript"] == "alice enriched transcript"
    assert alice["transcript_provider"] == "assemblyai"
    assert alice["transcribed_at"] == TS

    # Same recording_id in another source must not borrow this one's work.
    assert rows[("apple_voice_memos", "rec-1")]["summary"] == "new summary"


def test_derived_voice_tables_are_keyed_by_source(
    warehouse: PostgresWarehouse,
) -> None:
    """Two sources can hold the same recording_id without colliding.

    Before ``source`` joined the key, a second voice source's transcription run
    upserted onto the Apple run for the same (account, recording_id, provider)
    -- so the domain could not have stored a second source's transcript even if
    something had produced one.
    """
    warehouse.ensure_apple_voice_memos_tables(backfill_content_hashes=False)
    for source, text in (
        ("apple_voice_memos", "apple text"),
        ("alice_voice_recordings", "alice text"),
    ):
        warehouse.insert_apple_voice_memos_transcription_runs(
            [
                _row(
                    VOICE_MEMO_TRANSCRIPTION_RUN_COLUMNS,
                    source=source,
                    account="zach",
                    recording_id="shared-id",
                    provider="assemblyai",
                    status="completed",
                    transcript_text=text,
                    requested_at=TS,
                    completed_at=TS,
                    sync_version=1,
                )
            ]
        )
    rows = warehouse._query(
        "SELECT source, transcript_text FROM @apple_voice_memos_transcription_runs "
        "WHERE recording_id = 'shared-id' ORDER BY source"
    )
    assert rows == [
        ("alice_voice_recordings", "alice text"),
        ("apple_voice_memos", "apple text"),
    ]


def test_voice_memo_transcript_segments_carry_recording_context(
    warehouse: PostgresWarehouse,
) -> None:
    _seed_voice_memos(warehouse)
    rows = _dicts(warehouse, "marts_voice_memos_transcript_segments", "recording_id, segment_index")
    assert [row["text"] for row in rows] == ["hello", "goodbye"]
    assert {row["recording_title"] for row in rows} == {"Quarterly planning"}
    # spoken_at turns a millisecond offset into a real wall-clock instant.
    assert rows[1]["spoken_at"] == TS.replace(minute=1)


def test_calendar_transcript_views_read_the_voice_memo_mart(
    warehouse: PostgresWarehouse,
) -> None:
    """The calendar views must not re-derive "latest enrichment" themselves."""
    _seed_voice_memos(warehouse)
    warehouse.ensure_calendar_tables()
    warehouse.insert_calendar_events(
        [
            _row(
                CALENDAR_EVENT_COLUMNS,
                account="zach",
                calendar_id="primary",
                event_id="cal-1",
                status="confirmed",
                summary="Planning",
                start_at=TS,
                end_at=TS,
                organizer_email="zach@example.invalid",
                html_link="https://example.invalid/e",
                synced_at=TS,
                sync_version=1,
            )
        ]
    )
    warehouse._ensure_clean_calendar_transcript_views_if_possible()

    matched = _dicts(warehouse, "clean_calendar_with_transcripts")
    assert [(row["event_id"], row["recording_id"]) for row in matched] == [("cal-1", "rec-1")]
    assert matched[0]["summary"] == "new summary"

    unmatched = _dicts(warehouse, "clean_transcripts_no_calendar_match")
    assert [row["recording_id"] for row in unmatched] == []


# ---------------------------------------------------------------------------
# marts_messages.messages
# ---------------------------------------------------------------------------


def _seed_messages(wh: PostgresWarehouse) -> None:
    wh.ensure_apple_messages_tables()
    wh.ensure_whatsapp_tables()

    wh.insert_apple_contact_cards(
        [
            _row(
                CONTACT_CARD_COLUMNS,
                table="apple_contact_cards",
                source="apple_contacts",
                account="zach",
                source_kind="local",
                address_book_id="ab",
                card_id="card-1",
                display_name="Sample Contact",
                given_name="Sample",
                family_name="Contact",
                primary_phone="+15550001111",
                phones=[{"value": "+15550001111"}],
                source_updated_at=TS,
                synced_at=TS,
                sync_version=1,
            )
        ]
    )
    wh._ensure_clean_contacts_view()
    wh._ensure_clean_contact_points_view()

    wh.insert_apple_message_handles(
        [
            _row(
                APPLE_MESSAGE_HANDLE_COLUMNS,
                account="zach",
                handle_id="h1",
                handle_rowid=1,
                address="+15550001111",
                country="us",
                service="iMessage",
                ingested_at=TS,
                sync_version=1,
            )
        ]
    )
    wh.insert_apple_message_chats(
        [
            # chat.db style 45 = 1:1 conversation, 43 = group.
            _row(
                APPLE_MESSAGE_CHAT_COLUMNS,
                account="zach",
                chat_id="chat-1",
                chat_rowid=1,
                chat_identifier="+15550001111",
                service_name="iMessage",
                style=45,
                ingested_at=TS,
                sync_version=1,
            ),
            _row(
                APPLE_MESSAGE_CHAT_COLUMNS,
                account="zach",
                chat_id="chat-2",
                chat_rowid=2,
                chat_identifier="chat2",
                service_name="iMessage",
                display_name="Weekend plans",
                style=43,
                ingested_at=TS,
                sync_version=1,
            ),
        ]
    )
    wh.insert_apple_messages(
        [
            _row(
                APPLE_MESSAGE_COLUMNS,
                account="zach",
                message_id="m1",
                message_rowid=1,
                handle_id="h1",
                service="iMessage",
                body_text="hi there",
                body_source="text",
                body_decode_status="ok",
                is_from_me=0,
                message_at=TS,
                ingested_at=TS,
                sync_version=1,
            ),
            _row(
                APPLE_MESSAGE_COLUMNS,
                account="zach",
                message_id="m2",
                message_rowid=2,
                handle_id="h1",
                service="iMessage",
                body_text="sounds good",
                body_source="text",
                body_decode_status="ok",
                is_from_me=1,
                message_at=TS,
                ingested_at=TS,
                sync_version=1,
            ),
        ]
    )
    wh.insert_apple_message_chat_messages(
        [
            _row(
                APPLE_MESSAGE_CHAT_MESSAGE_COLUMNS,
                account="zach",
                chat_id="chat-1",
                message_id="m1",
                message_date=TS,
                ingested_at=TS,
                sync_version=1,
            ),
            _row(
                APPLE_MESSAGE_CHAT_MESSAGE_COLUMNS,
                account="zach",
                chat_id="chat-2",
                message_id="m2",
                message_date=TS,
                ingested_at=TS,
                sync_version=1,
            ),
        ]
    )
    wh.insert_whatsapp_chats(
        [
            _row(
                WHATSAPP_CHAT_COLUMNS,
                account="zach",
                chat_id="123@g.us",
                name="Family",
                chat_type="group",
                last_message_at=TS,
                ingested_at=TS,
                sync_version=1,
            )
        ]
    )
    wh.insert_whatsapp_messages(
        [
            _row(
                WHATSAPP_MESSAGE_COLUMNS,
                account="zach",
                chat_id="123@g.us",
                message_id="w1",
                sender_jid="15550002222@s.whatsapp.net",
                push_name="Group Member One",
                is_from_me=0,
                body_text="see you soon",
                message_kind="text",
                message_at=TS,
                ingested_at=TS,
                sync_version=1,
            ),
            _row(
                WHATSAPP_MESSAGE_COLUMNS,
                account="zach",
                chat_id="123@g.us",
                message_id="w2",
                sender_jid="15550003333@s.whatsapp.net",
                push_name="Group Member Two",
                is_from_me=0,
                message_kind="voice",
                media_type="audio/ogg",
                quoted_message_id="w1",
                message_at=TS,
                ingested_at=TS,
                sync_version=1,
            ),
        ]
    )
    wh._ensure_clean_apple_messages_view()
    wh._ensure_clean_whatsapp_messages_view()


def test_unified_messages_mart_conforms_both_sources(warehouse: PostgresWarehouse) -> None:
    _seed_messages(warehouse)
    columns = _columns(warehouse, "marts_messages_messages")
    for required in (
        "source",
        "account",
        "chat_id",
        "chat_name",
        "chat_kind",
        "message_id",
        "sender_name",
        "sender_address",
        "is_from_me",
        "body_text",
        "message_at",
    ):
        assert required in columns, required

    rows = {
        (row["source"], row["message_id"]): row
        for row in _dicts(warehouse, "marts_messages_messages")
    }
    assert len(rows) == 4

    imessage = rows[("apple_messages", "m1")]
    assert imessage["chat_id"] == "chat-1"
    assert imessage["chat_kind"] == "direct"
    assert imessage["sender_name"] == "Sample Contact"
    assert imessage["sender_address"] == "+15550001111"
    assert imessage["is_from_me"] == 0
    assert imessage["service"] == "iMessage"
    assert imessage["message_kind"] == "text"

    group = rows[("apple_messages", "m2")]
    assert group["chat_kind"] == "group"
    assert group["chat_name"] == "Weekend plans"
    assert group["sender_name"] == "me"
    assert group["is_from_me"] == 1

    whatsapp = rows[("whatsapp", "w1")]
    assert whatsapp["chat_kind"] == "group"
    assert whatsapp["chat_name"] == "Family"
    assert whatsapp["sender_address"] == "15550002222@s.whatsapp.net"
    assert whatsapp["message_at"] == TS

    voice = rows[("whatsapp", "w2")]
    assert voice["message_kind"] == "audio"
    assert voice["source_message_kind"] == "voice"
    assert voice["media_type"] == "audio/ogg"
    assert voice["reply_to_message_id"] == "w1"


def test_unified_messages_mart_reports_every_sentinel_timestamp_as_null(
    warehouse: PostgresWarehouse,
) -> None:
    """Absence is stored as 1970-01-01, and the mart is where that is undone.

    The base columns are NOT NULL, so "never read" / "never edited" / "no send
    time" are all written as the epoch — 41% of recent iMessage rows have a
    sentinel date_read and 83% a sentinel date_delivered. A view that
    translated some of them and not others would be the real hazard: one
    conformed column with two spellings of unknown. So this asserts EVERY
    timestamp the view exposes, per column.
    """
    _seed_messages(warehouse)
    warehouse.insert_apple_messages(
        [
            _row(
                APPLE_MESSAGE_COLUMNS,
                account="zach",
                message_id="m-sentinel",
                message_rowid=3,
                handle_id="h1",
                service="iMessage",
                body_text="never read, never delivered, no send time",
                is_from_me=0,
                # every timestamp left at the epoch default
                ingested_at=TS,
                sync_version=1,
            )
        ]
    )
    warehouse.insert_whatsapp_messages(
        [
            _row(
                WHATSAPP_MESSAGE_COLUMNS,
                account="zach",
                chat_id="123@g.us",
                message_id="w-sentinel",
                sender_jid="15550002222@s.whatsapp.net",
                is_from_me=0,
                body_text="never edited",
                message_kind="text",
                ingested_at=TS,
                sync_version=1,
            )
        ]
    )

    rows = {
        (row["source"], row["message_id"]): row
        for row in _dicts(warehouse, "marts_messages_messages")
    }
    timestamps = [
        column
        for column in _columns(warehouse, "marts_messages_messages")
        if column.endswith("_at")
    ]
    assert timestamps == ["message_at", "edited_at", "read_at", "delivered_at"]
    for key in (("apple_messages", "m-sentinel"), ("whatsapp", "w-sentinel")):
        for column in timestamps:
            assert rows[key][column] is None, f"{key} {column}"

    # And a real timestamp still comes through untouched.
    assert rows[("apple_messages", "m1")]["message_at"] == TS


def test_voice_memo_mart_reports_every_sentinel_timestamp_as_null(
    warehouse: PostgresWarehouse,
) -> None:
    _seed_voice_memos(warehouse)
    warehouse.insert_apple_voice_memos_files(
        [
            _row(
                VOICE_MEMO_FILE_COLUMNS,
                account="zach",
                recording_id="rec-undated",
                filename="3.m4a",
                content_sha256="sha-4",
                # recorded_at left at the epoch default
                ingested_at=TS,
                sync_version=1,
            )
        ]
    )
    warehouse.insert_apple_voice_memos_enrichments(
        [
            _row(
                VOICE_MEMO_ENRICHMENT_COLUMNS,
                source="apple_voice_memos",
                account="zach",
                recording_id="rec-undated",
                content_sha256="sha-4",
                provider="agent",
                model="m",
                prompt_version="v1",
                status="completed",
                title="No meeting window",
                # start_at / end_at left at the epoch default
                created_at=TS,
                sync_version=1,
            )
        ]
    )
    warehouse.insert_alice_voice_recordings(
        [
            _row(
                ALICE_VOICE_RECORDING_COLUMNS,
                account="zach",
                recording_id="alice-undated",
                filename="undated.m4a",
                content_sha256="sha-5",
                ingested_at=TS,
                sync_version=1,
            )
        ]
    )
    warehouse.insert_apple_voice_memos_transcript_segments(
        [
            _row(
                VOICE_MEMO_TRANSCRIPT_SEGMENT_COLUMNS,
                source="apple_voice_memos",
                account="zach",
                recording_id="rec-undated",
                provider="assemblyai",
                segment_index=0,
                start_ms=5000,
                end_ms=6000,
                text="who knows when",
                created_at=TS,
                sync_version=1,
            )
        ]
    )

    rows = {
        (row["source"], row["recording_id"]): row
        for row in _dicts(warehouse, "marts_voice_memos_recordings")
    }
    timestamps = [
        column
        for column in _columns(warehouse, "marts_voice_memos_recordings")
        if column.endswith("_at")
    ]
    assert timestamps == [
        "recorded_at",
        "meeting_start_at",
        "meeting_end_at",
        "enriched_at",
        "ingested_at",
        "transcribed_at",
    ]
    for key in (
        ("apple_voice_memos", "rec-undated"),
        ("alice_voice_recordings", "alice-undated"),
    ):
        assert rows[key]["recorded_at"] is None, key
    undated = rows[("apple_voice_memos", "rec-undated")]
    assert undated["meeting_start_at"] is None
    assert undated["meeting_end_at"] is None
    # An Alice recording has no enrichment at all, so its enriched_at is NULL
    # for the other reason — and a sentinel ingested_at is NULL too.
    assert rows[("alice_voice_recordings", "alice-undated")]["ingested_at"] == TS
    assert rows[("alice_voice_recordings", "alice-1")]["enriched_at"] is None
    assert rows[("apple_voice_memos", "rec-1")]["recorded_at"] == TS

    # A segment of an undated recording has no wall-clock instant either —
    # never 1970 plus the offset.
    segment = next(
        row
        for row in _dicts(warehouse, "marts_voice_memos_transcript_segments")
        if row["recording_id"] == "rec-undated"
    )
    assert segment["recorded_at"] is None
    assert segment["spoken_at"] is None


def test_unified_messages_mart_uses_one_conformed_chat_kind_vocabulary(
    warehouse: PostgresWarehouse,
) -> None:
    """WhatsApp calls a DM ``user`` and Apple encodes it as ``style = 45``.

    An agent must not have to know either to ask "my direct messages".
    """
    _seed_messages(warehouse)
    kinds = {
        (row["source"], row["chat_kind"])
        for row in _dicts(warehouse, "marts_messages_messages")
    }
    assert kinds == {
        ("apple_messages", "direct"),
        ("apple_messages", "group"),
        ("whatsapp", "group"),
    }


def test_apple_messages_view_resolves_contacts_per_handle_not_per_message(
    warehouse: PostgresWarehouse,
) -> None:
    """Sender resolution depends only on the handle, so it must be joined once.

    Resolving it inside a per-row LATERAL over marts_contacts.contact_points
    (itself a jsonb-expanding view) cost ~30 ms per message: a 30-day window of
    marts_messages.apple_messages took 59 s in production, past every query
    timeout the CLI and the app impose. Joining at the handle level returns the
    identical rows in under a second.
    """
    _seed_messages(warehouse)
    rel = relation("clean_apple_messages").with_namespace(warehouse.schema_namespace)
    definition = warehouse._query(
        "SELECT pg_get_viewdef(to_regclass(%s), true)", (f'"{rel.schema}"."{rel.name}"',)
    )[0][0]
    assert "LATERAL" not in definition.upper()

    names = {
        row["message_id"]: row["sender_name"]
        for row in _dicts(warehouse, "clean_apple_messages")
    }
    assert names == {"m1": "Sample Contact", "m2": "me"}


# ---------------------------------------------------------------------------
# live production drift
# ---------------------------------------------------------------------------


def _live_connection():
    import psycopg2

    connection = psycopg2.connect(
        _postgres_url(), options="-c default_transaction_read_only=on"
    )
    connection.autocommit = True
    return connection


def _require_deployed_warehouse() -> None:
    """These three tests describe a real deployment, not a scratch database.

    POSTGRES_DATABASE_URL can point at an empty CI Postgres that has never held
    a warehouse (every test provisions into its own pdw_test_* namespace), and
    "no drift" there is vacuous rather than reassuring.
    """
    if _live("SELECT to_regclass('timeline.events') IS NOT NULL")[0][0]:
        return
    pytest.skip("no warehouse deployed in the canonical schemas of this database")


def _live(sql: str, params: tuple = ()) -> list[tuple]:
    connection = _live_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()
    finally:
        connection.close()


def test_live_warehouse_has_no_uncataloged_schema() -> None:
    """Prod had grown a ``search_lab`` schema holding six shadow search
    functions — search_text, search_text_exact, search_hybrid, context,
    search_text_sources, search_text_preview — and no tables.

    The fresh-provisioning contract in test_schema_reorg_contract.py could not
    see it: that test builds a throwaway namespace from the catalog, so
    anything existing only in the real database is invisible to it. A shadow
    copy of the search API is exactly the failure that silently returned zero
    rows for sixteen days in July. search_lab was dropped from production on
    2026-08-23; this is what keeps the next one from living there unnoticed.
    """
    _require_deployed_warehouse()
    live = {
        name
        for (name,) in _live(
            """
            SELECT nspname FROM pg_namespace
            WHERE nspname NOT LIKE 'pg\\_%%'
              AND nspname NOT IN ('information_schema', 'public')
              AND nspname NOT LIKE 'pdw\\_test\\_%%'
            """
        )
    }
    uncataloged = sorted(live - set(CATALOG.all_schemas()))
    assert uncataloged == [], (
        "schemas exist in the live warehouse that the catalog does not declare; "
        "add them to warehouse_catalog.json or drop them in production: "
        + ", ".join(uncataloged)
    )


def test_live_warehouse_relations_and_functions_match_the_catalog() -> None:
    _require_deployed_warehouse()
    schemas = sorted(CATALOG.all_schemas())
    live_relations = {
        (schema, name)
        for schema, name in _live(
            """
            SELECT n.nspname, c.relname
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = ANY(%s) AND c.relkind IN ('r', 'v', 'm', 'p', 'S')
            """,
            (schemas,),
        )
    }
    cataloged_relations = {
        (obj.schema, obj.name)
        for obj in CATALOG.objects
        if obj.kind in {"table", "view", "sequence"}
    }
    assert sorted(live_relations - cataloged_relations) == [], (
        "uncataloged relations in the live warehouse"
    )

    live_functions = {
        (schema, name)
        for schema, name in _live(
            """
            SELECT n.nspname, p.proname
            FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = ANY(%s)
            """,
            (schemas,),
        )
    }
    cataloged_functions = {
        (obj.schema, obj.name) for obj in CATALOG.objects if obj.kind == "function"
    }
    assert sorted(live_functions - cataloged_functions) == [], (
        "uncataloged functions in the live warehouse"
    )


def test_live_public_schema_holds_no_warehouse_objects() -> None:
    """``public.search_text`` once shadowed the real function for 16 days.

    Everything legitimately in ``public`` belongs to an extension (pgvector,
    pg_trgm, vchord_bm25, pg_stat_statements), so an object there that no
    extension owns is a stray copy of ours.
    """
    _require_deployed_warehouse()
    strays = _live(
        """
        SELECT 'relation', c.relname
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind IN ('r', 'v', 'm', 'p', 'S')
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend d
              WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype = 'e'
          )
        UNION ALL
        SELECT 'function', p.proname
        FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public'
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend d
              WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.deptype = 'e'
          )
        """
    )
    assert strays == [], f"non-extension objects in public: {strays}"


# ---------------------------------------------------------------------------
# marts_health.* — the first read interface over BOTH WHOOP sources
# ---------------------------------------------------------------------------


def _seed_health(wh: PostgresWarehouse) -> None:
    wh.ensure_whoop_tables()
    wh.ensure_whoop_private_tables()

    wh.insert_whoop_cycles(
        [
            _row(
                WHOOP_CYCLE_COLUMNS,
                account="zach",
                cycle_id="cycle-done",
                start_at=TS,
                end_at=TS.replace(hour=23),
                score_state="SCORED",
                strain=14.2,
                kilojoule=9000.0,
                average_heart_rate=61,
                max_heart_rate=170,
                synced_at=TS,
                sync_version=1,
            ),
            _row(
                WHOOP_CYCLE_COLUMNS,
                account="zach",
                cycle_id="cycle-running",
                start_at=TS.replace(day=2),
                # end_at left at the epoch: the cycle is still running.
                score_state="PENDING_SCORE",
                strain=3.1,
                synced_at=TS,
                sync_version=1,
            ),
        ]
    )
    wh.insert_whoop_private_cycles(
        [
            _row(
                WHOOP_PRIVATE_CYCLE_COLUMNS,
                account="zach",
                cycle_id="cycle-done",
                start_at=TS,
                end_at=TS.replace(hour=23),
                # day_strain is the RAW unscaled value; scaled_strain is what
                # WHOOP displays and what base_whoop.cycles.strain matches.
                day_strain=0.019,
                scaled_strain=14.2,
                sleep_need=28800.0,
                data_state="complete",
                synced_at=TS,
                sync_version=1,
            )
        ]
    )
    wh.insert_whoop_sleeps(
        [
            _row(
                WHOOP_SLEEP_COLUMNS,
                account="zach",
                sleep_id="sleep-1",
                cycle_id="cycle-done",
                start_at=TS,
                end_at=TS.replace(hour=20),
                nap=0,
                score_state="SCORED",
                sleep_performance_percentage=88.0,
                total_in_bed_time_milli=28_800_000,
                total_rem_sleep_time_milli=5_400_000,
                synced_at=TS,
                sync_version=1,
            )
        ]
    )
    wh.insert_whoop_private_sleeps(
        [
            _row(
                WHOOP_PRIVATE_SLEEP_COLUMNS,
                account="zach",
                activity_id="sleep-1",
                cycle_id="cycle-done",
                start_at=TS,
                end_at=TS.replace(hour=20),
                debt_pre=1800.0,
                debt_post=900.0,
                habitual_sleep_need=27000.0,
                latency=420.0,
                synced_at=TS,
                sync_version=1,
            )
        ]
    )
    wh.insert_whoop_recoveries(
        [
            _row(
                WHOOP_RECOVERY_COLUMNS,
                account="zach",
                cycle_id="cycle-done",
                sleep_id="sleep-1",
                score_state="SCORED",
                recovery_score=71,
                resting_heart_rate=52,
                # The public API's unit: MILLISECONDS.
                hrv_rmssd_milli=84.5,
                synced_at=TS,
                sync_version=1,
            )
        ]
    )
    wh.insert_whoop_private_recoveries(
        [
            _row(
                WHOOP_PRIVATE_RECOVERY_COLUMNS,
                account="zach",
                activity_id="sleep-1",
                recovery_score=71,
                resting_heart_rate=52,
                # The private API's unit: SECONDS. Same measurement, 1000x apart.
                hrv_rmssd_seconds=0.0845,
                hrv_rmssd_milli=84.5,
                hrv_component=0.6,
                rhr_component=0.3,
                synced_at=TS,
                sync_version=1,
            )
        ]
    )
    wh.insert_whoop_workouts(
        [
            _row(
                WHOOP_WORKOUT_COLUMNS,
                account="zach",
                workout_id="workout-1",
                start_at=TS,
                end_at=TS.replace(hour=13),
                sport_id=1,
                # The public row's own token is a slug, when it has one at all.
                sport_name="hiking-rucking",
                score_state="SCORED",
                strain=9.4,
                synced_at=TS,
                sync_version=1,
            )
        ]
    )
    wh.insert_whoop_private_workouts(
        [
            _row(
                WHOOP_PRIVATE_WORKOUT_COLUMNS,
                account="zach",
                activity_id="workout-1",
                sport_id=1,
                start_at=TS,
                end_at=TS.replace(hour=13),
                total_steps=4200,
                synced_at=TS,
                sync_version=1,
            )
        ]
    )
    wh.insert_whoop_private_sports(
        [
            _row(
                WHOOP_PRIVATE_SPORT_COLUMNS,
                account="zach",
                sport_id=1,
                name="Running",
                category="Cardio",
                synced_at=TS,
                sync_version=1,
            )
        ]
    )


def test_the_workout_heart_rate_view_windows_the_one_series_by_the_workout(
    warehouse: PostgresWarehouse,
) -> None:
    """The replacement for the retired workout-scoped table.

    Continuous heart rate is collected at the same six-second grain that table
    held, for every hour rather than only inside a workout, so a second copy of
    those readings was two things to ask and one of them would have rotted.
    "HR during workout X" is now this join, and it must select exactly the
    workout's own bounds -- end-exclusive, or the first sample of whatever came
    next is attributed to the workout.
    """
    _seed_health(warehouse)
    # TS..TS+1h is workout-1's window; the seeded workout ends at 13:00.
    warehouse.insert_whoop_private_heart_rate_samples(
        [
            _row(
                WHOOP_PRIVATE_HEART_RATE_SAMPLE_COLUMNS,
                account="zach",
                sample_at=sample_at,
                heart_rate=heart_rate,
                step_seconds=6,
                synced_at=TS,
                sync_version=1,
            )
            for sample_at, heart_rate in (
                (TS - timedelta(minutes=1), 58),  # before it started
                (TS, 121),
                (TS + timedelta(minutes=30), 154),
                (TS.replace(hour=13), 96),  # exactly the end bound: excluded
                (TS.replace(hour=14), 61),  # after it ended
            )
        ]
    )

    rows = sorted(
        (row for row in _dicts(warehouse, "marts_health_workout_heart_rate_samples")),
        key=lambda row: row["sample_at"],
    )

    assert [row["heart_rate"] for row in rows] == [121, 154]
    assert [row["elapsed_seconds"] for row in rows] == [0, 1800]
    assert {row["workout_id"] for row in rows} == {"workout-1"}
    # The readable name comes through the mart, not the public row's slug.
    assert {row["sport_name"] for row in rows} == {"Running"}
    assert {row["step_seconds"] for row in rows} == {6}


def test_health_mart_conforms_both_whoop_sources(warehouse: PostgresWarehouse) -> None:
    """One read interface over the public API and the app API.

    Reading either alone is wrong in a different direction: the public source
    has no strain components, sleep debt, steps or sport catalog, and the
    private source is missing rows the public one has (measured 2026-08-26:
    305 public sleeps vs 294 private, 268 public workouts vs 257).
    """
    _seed_health(warehouse)

    cycles = {row["cycle_id"]: row for row in _dicts(warehouse, "marts_health_cycles")}
    assert cycles["cycle-done"]["strain"] == 14.2
    assert cycles["cycle-done"]["sleep_need_seconds"] == 28800.0
    assert cycles["cycle-done"]["has_private_detail"] == 1
    assert cycles["cycle-running"]["has_private_detail"] == 0

    sleep = _dicts(warehouse, "marts_health_sleeps")[0]
    assert sleep["sleep_debt_pre_seconds"] == 1800.0
    assert sleep["is_nap"] == 0

    workout = _dicts(warehouse, "marts_health_workouts")[0]
    # The readable name comes from the private source's 204-sport catalog; the
    # provider's own slug stays available beside it.
    assert workout["sport_name"] == "Running"
    assert workout["sport_slug"] == "hiking-rucking"
    assert workout["sport_category"] == "Cardio"
    assert workout["total_steps"] == 4200


def test_health_mart_exposes_one_hrv_unit(warehouse: PostgresWarehouse) -> None:
    """The unit trap, as a test.

    base_whoop_private.recoveries.hrv_rmssd_seconds is SECONDS and
    base_whoop.recoveries.hrv_rmssd_milli is milliseconds. Exposing both under
    similar names in one view is how a 1000x error gets written, so the mart
    publishes exactly one HRV column and it is the milliseconds one.
    """
    _seed_health(warehouse)
    columns = _columns(warehouse, "marts_health_recoveries")
    hrv = [column for column in columns if "hrv_rmssd" in column]
    assert hrv == ["hrv_rmssd_milli"]
    assert "hrv_rmssd_seconds" not in columns

    recovery = _dicts(warehouse, "marts_health_recoveries")[0]
    assert recovery["hrv_rmssd_milli"] == 84.5


def test_health_mart_conforms_sleep_stage_durations_to_one_unit(
    warehouse: PostgresWarehouse,
) -> None:
    """Public stage totals are milliseconds; private ones are seconds.

    Every duration the mart exposes is seconds and says so in its name, so a
    caller cannot add a public column to a private one and be silently 1000x
    out.
    """
    _seed_health(warehouse)
    sleep = _dicts(warehouse, "marts_health_sleeps")[0]
    assert sleep["time_in_bed_seconds"] == 28800.0
    assert sleep["rem_sleep_seconds"] == 5400.0
    assert not [
        column
        for column in _columns(warehouse, "marts_health_sleeps")
        if column.endswith("_milli")
    ]


def test_health_mart_reports_a_running_cycle_as_null_not_the_epoch(
    warehouse: PostgresWarehouse,
) -> None:
    """The sibling of the voice-memo sentinel test, for the sharpest case.

    base_whoop.cycles.end_at holds 1970-01-01 for the cycle still in progress,
    so ORDER BY end_at DESC on the raw table ranks the RUNNING cycle as the
    oldest row in it.
    """
    _seed_health(warehouse)
    cycles = {row["cycle_id"]: row for row in _dicts(warehouse, "marts_health_cycles")}
    assert cycles["cycle-running"]["end_at"] is None
    assert cycles["cycle-running"]["predicted_end"] is None
    assert cycles["cycle-done"]["end_at"] == TS.replace(hour=23)


@pytest.mark.parametrize(
    "logical",
    [
        "marts_health_cycles",
        "marts_health_sleeps",
        "marts_health_recoveries",
        "marts_health_workouts",
    ],
)
def test_health_mart_translates_every_exposed_timestamp_or_none(
    warehouse: PostgresWarehouse, logical: str
) -> None:
    """Per column, because a partial translation MANUFACTURES an inconsistency.

    The sources are internally consistent: every absent timestamp is the epoch.
    A view that NULLIFs one column and forgets its sibling does not inherit a
    problem, it invents one — and then ORDER BY, MIN(), COALESCE and IS NULL
    each disagree depending on which column was asked. Seeding a row whose
    every timestamp is the sentinel and asserting the whole column set comes
    back NULL is the only check that scales with the column list.
    """
    _seed_health(warehouse)
    # Every seeded row above already carries the sentinel in the timestamps its
    # source did not set; the row below carries it in ALL of them.
    warehouse.insert_whoop_cycles(
        [_row(WHOOP_CYCLE_COLUMNS, account="sentinel", cycle_id="c", sync_version=1)]
    )
    warehouse.insert_whoop_sleeps(
        [_row(WHOOP_SLEEP_COLUMNS, account="sentinel", sleep_id="s", sync_version=1)]
    )
    warehouse.insert_whoop_recoveries(
        [_row(WHOOP_RECOVERY_COLUMNS, account="sentinel", cycle_id="c", sync_version=1)]
    )
    warehouse.insert_whoop_workouts(
        [_row(WHOOP_WORKOUT_COLUMNS, account="sentinel", workout_id="w", sync_version=1)]
    )

    timestamps = [
        column
        for column, data_type in _column_types(warehouse, logical)
        if data_type == "timestamp with time zone"
    ]
    assert timestamps, logical
    row = next(row for row in _dicts(warehouse, logical) if row["account"] == "sentinel")
    for column in timestamps:
        assert row[column] is None, f"{logical}.{column} still exposes the epoch sentinel"


def test_voice_memo_mart_column_order_only_ever_grows_at_the_end(
    warehouse: PostgresWarehouse,
) -> None:
    """The deployed view must be replaceable in place.

    ``CREATE OR REPLACE VIEW`` refuses to drop, rename, retype or reorder an
    existing view's columns; it will only accept new ones appended. When it
    refuses, ``_ensure_view`` falls back to dropping and recreating, which on
    this view means taking the two marts_calendar transcript views down with
    it. Pinning the exact prefix — read from the live production view on
    2026-08-26 — keeps a routine edit from turning into that.
    """
    _seed_voice_memos(warehouse)
    deployed = [
        ("source", "text"),
        ("account", "text"),
        ("recording_id", "text"),
        ("recorded_at", "timestamp with time zone"),
        ("duration_seconds", "numeric"),
        ("title", "text"),
        ("source_title", "text"),
        ("filename", "text"),
        ("summary", "text"),
        ("transcript", "text"),
        ("participants_json", "text"),
        ("action_items_json", "text"),
        ("evidence_json", "text"),
        ("calendar_event_id", "text"),
        ("calendar_confidence", "double precision"),
        ("meeting_start_at", "timestamp with time zone"),
        ("meeting_end_at", "timestamp with time zone"),
        ("enrichment_title", "text"),
        ("enrichment_provider", "text"),
        ("enrichment_model", "text"),
        ("enrichment_prompt_version", "text"),
        ("enriched_at", "timestamp with time zone"),
        ("transcript_provider", "text"),
        ("content_type", "text"),
        ("size_bytes", "bigint"),
        ("content_sha256", "text"),
        ("storage_backend", "text"),
        ("storage_key", "text"),
        ("storage_file_id", "text"),
        ("storage_url", "text"),
        ("recording_url", "text"),
        ("is_deleted", "bigint"),
        ("ingested_at", "timestamp with time zone"),
    ]
    actual = _column_types(warehouse, "marts_voice_memos_recordings")
    assert actual[: len(deployed)] == deployed
    assert actual[len(deployed) :] == [("transcribed_at", "timestamp with time zone")]
