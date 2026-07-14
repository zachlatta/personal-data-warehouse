"""Capture-context builder for photo enrichment (time / GPS / calendar)."""

from __future__ import annotations

import os
from datetime import UTC, datetime

import pytest
from dotenv import load_dotenv

from tests.conftest import make_test_schema
from tests.test_photos_warehouse import _photo_asset_row

from personal_data_warehouse.photo_context import photo_enrichment_context
from personal_data_warehouse.postgres import PostgresWarehouse

_TS = datetime(2026, 6, 1, 21, 30, tzinfo=UTC)  # 14:30 local at -07:00


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
        for schema_name in wh.physical_schema_names(include_private=True) + [schema]:
            wh._raw_command(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        wh.close()


def _calendar_event(warehouse, *, event_id, summary, start, end, location="", status="confirmed"):
    warehouse._command(
        """
        INSERT INTO calendar_events (account, calendar_id, event_id, summary, location, status,
                                     start_at, end_at, updated_at, synced_at)
        VALUES ('z@x.test', 'cal1', %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (event_id, summary, location, status, start, end, _TS, _TS),
    )


def test_context_carries_local_time_gps_camera_and_nearby_events(warehouse):
    warehouse.ensure_photos_tables()
    warehouse.ensure_calendar_tables()
    warehouse.insert_photo_assets([_photo_asset_row(capture_ts=_TS)])
    # Overlapping event, a nearby-but-not-overlapping one inside the window,
    # and one far outside the window.
    _calendar_event(
        warehouse, event_id="ev-during", summary="Backyard Concert",
        start=_TS.replace(hour=20), end=_TS.replace(hour=23), location="Home",
    )
    _calendar_event(
        warehouse, event_id="ev-later", summary="Dinner",
        start=_TS.replace(hour=23, minute=45), end=datetime(2026, 6, 2, 1, tzinfo=UTC),
    )
    _calendar_event(
        warehouse, event_id="ev-tomorrow", summary="Standup",
        start=datetime(2026, 6, 2, 16, tzinfo=UTC), end=datetime(2026, 6, 2, 17, tzinfo=UTC),
    )

    context = photo_enrichment_context(warehouse, {"photo_id": "ph1"})

    assert context is not None
    assert context["captured_at_local"].startswith("2026-06-01T14:30:00")
    assert context["local_day_of_week"] == "Monday"
    assert context["gps"] == {"latitude": 45.5, "longitude": -122.6}
    assert context["camera"] == "Apple iPhone 16 Pro"
    events = context["calendar_events_near_capture"]
    # Nearest-first, window-bounded: the during-event leads, tomorrow is absent.
    assert [event["event_id"] for event in events] == ["ev-during", "ev-later"]
    assert events[0]["summary"] == "Backyard Concert"
    assert events[0]["location"] == "Home"
    # Attendees are deliberately not surfaced to the vision prompt.
    assert "attendees" not in events[0]


def test_context_excludes_cancelled_events_and_handles_no_calendar(warehouse):
    warehouse.ensure_photos_tables()
    warehouse.ensure_calendar_tables()
    warehouse.insert_photo_assets([_photo_asset_row(capture_ts=_TS)])
    _calendar_event(
        warehouse, event_id="ev-cancelled", summary="Cancelled thing",
        start=_TS.replace(hour=21), end=_TS.replace(hour=22), status="cancelled",
    )
    context = photo_enrichment_context(warehouse, {"photo_id": "ph1"})
    assert context["calendar_events_near_capture"] == []


def test_context_omits_unknowns_rather_than_guessing(warehouse):
    warehouse.ensure_photos_tables()
    warehouse.ensure_calendar_tables()
    warehouse.insert_photo_assets(
        [
            _photo_asset_row(
                photo_id="ph-bare",
                capture_ts=_TS,
                capture_tz_offset="",
                latitude=0.0,
                longitude=0.0,
                camera_make="",
                camera_model="",
            )
        ]
    )
    context = photo_enrichment_context(warehouse, {"photo_id": "ph-bare"})
    assert context is not None
    assert "captured_at_local" not in context  # unknown offset: no misleading local time
    assert "gps" not in context
    assert "camera" not in context
    assert context["captured_at_utc"].startswith("2026-06-01T21:30")


def test_context_is_none_for_unknown_or_epoch_assets(warehouse):
    warehouse.ensure_photos_tables()
    assert photo_enrichment_context(warehouse, {"photo_id": "nope"}) is None
    assert photo_enrichment_context(warehouse, {}) is None
    warehouse.insert_photo_assets(
        [_photo_asset_row(photo_id="ph-epoch", capture_ts=datetime(1970, 1, 1, tzinfo=UTC))]
    )
    assert photo_enrichment_context(warehouse, {"photo_id": "ph-epoch"}) is None
