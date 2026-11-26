"""The screenshot-vs-self audit: frames that show Zach acting against his `self` tier."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from personal_data_warehouse.pipeline_health import PipelineHealthCollector
from personal_data_warehouse.screen_self_audit import (
    AUDITED_ADAPTERS,
    SETTLE_HOURS,
    run_screen_self_audit,
)
from personal_data_warehouse.timeline import TIMELINE_ADAPTERS
from tests.test_pipeline_health import _provision_every_table, warehouse  # noqa: F401


def _frame(wh, device, captured_at, app, summary, *, state="active"):
    wh._command(
        "INSERT INTO @screen_timelapse_screenshots (device, captured_at, display_index) VALUES (%s, %s, 0)",
        (device, captured_at),
    )
    wh._command(
        "INSERT INTO @screen_timelapse_descriptions "
        "(device, captured_at, display_index, prompt_version, status, screen_state, app, summary, created_at) "
        "VALUES (%s, %s, 0, 'v1', 'completed', %s, %s, %s, %s)",
        (device, captured_at, state, app, summary, captured_at),
    )


def _event(wh, adapter, occurred_at, priority="self"):
    wh._command(
        "INSERT INTO @timeline_events (adapter, event_id, source, kind, occurred_at, source_table, priority) "
        "VALUES (%s, %s, 'x', 'k', %s, 'x', %s::timeline.timeline_priority)".replace(
            "timeline.timeline_priority", wh.physical_schema_name("timeline") + ".timeline_priority"
        ),
        (adapter, f"{adapter}-{occurred_at.isoformat()}", occurred_at, priority),
    )


def test_audited_adapters_are_registered():
    names = {adapter.name for adapter in TIMELINE_ADAPTERS}
    assert set(AUDITED_ADAPTERS) <= names


def test_audit_records_follow_through_and_screen_coverage_per_channel(warehouse):  # noqa: F811
    _provision_every_table(warehouse)
    warehouse.ensure_timeline_tables()
    now = datetime.now(tz=UTC).replace(microsecond=0)
    t = now - timedelta(hours=SETTLE_HOURS + 24)

    # Email: a composed reply that was sent five minutes later, and a frame
    # showing a finished send that never reached the timeline.
    _frame(warehouse, "laptop", t, "Superhuman", "Zach is composing a reply about the fictional invoice.")
    _event(warehouse, "gmail_email", t + timedelta(minutes=5))
    _frame(warehouse, "laptop", t + timedelta(hours=2), "Superhuman", "Zach has just sent a reply to the fictional vendor.")
    # A draft abandoned: no send follows. A non-self event does not count.
    _frame(warehouse, "laptop", t + timedelta(hours=4), "Superhuman", "Zach is drafting an email he never sends.")
    _event(warehouse, "gmail_email", t + timedelta(hours=4, minutes=1), priority="direct")
    # Not Zach's act, and not a messaging app: both ignored.
    _frame(warehouse, "laptop", t + timedelta(hours=5), "Superhuman", "An email from a fictional sender is open.")
    _frame(warehouse, "laptop", t + timedelta(hours=6), "Google Chrome", "Zach is composing a document.")
    # A self send with no screen in use around it.
    _event(warehouse, "gmail_email", t + timedelta(hours=10))

    # Slack: every on-screen post goes nowhere -> failing.
    for minutes in (0, 60, 120):
        _frame(warehouse, "laptop", t + timedelta(hours=12, minutes=minutes), "Slack",
               "Zach posted a message in a fictional channel.")

    # Phone messages: a send from Google Messages may land on either adapter.
    _frame(warehouse, "phone", t + timedelta(hours=14), "Google Messages", "Zach sent a text to a fictional friend.")
    _event(warehouse, "apple_message", t + timedelta(hours=14, minutes=-2))
    # A locked frame is not a witness.
    _frame(warehouse, "phone", t + timedelta(hours=15), "Google Messages", "Zach sent a text.", state="locked")

    # Outside the settled window: too recent, and older than the window.
    _frame(warehouse, "laptop", now - timedelta(minutes=30), "Slack", "Zach posted a message.")
    _frame(warehouse, "laptop", now - timedelta(days=9), "Slack", "Zach posted a message.")

    rows = run_screen_self_audit(warehouse, now=now)
    assert rows is not None
    assert run_screen_self_audit(warehouse, now=now + timedelta(hours=1)) is None, "one run per day"

    view = {
        row["channel"]: row
        for row in warehouse._query_dicts("SELECT * FROM @marts_screen_self_audit WHERE is_latest")
    }
    assert set(view) == {"email", "slack", "phone_messages", "whatsapp"}
    email = view["email"]
    assert (email["sent_frames"], email["sent_frames_matched"]) == (1, 0)
    assert (email["draft_frames"], email["draft_frames_matched"]) == (2, 1)
    assert (email["self_events"], email["self_events_with_screen"]) == (2, 1)
    assert float(email["follow_through"]) == pytest.approx(1 / 3, abs=1e-4)
    assert float(email["screen_coverage"]) == pytest.approx(0.5)
    assert email["status"] == "attention"
    assert email["adapters"] == ["gmail_email"]

    slack = view["slack"]
    assert (slack["sent_frames"], slack["sent_frames_matched"]) == (3, 0)
    assert slack["status"] == "failing"

    phone = view["phone_messages"]
    assert (phone["sent_frames"], phone["sent_frames_matched"]) == (1, 1)
    assert phone["status"] == "ok"
    assert phone["adapters"] == ["android_message", "apple_message"]

    assert view["whatsapp"]["status"] == "unknown", "no evidence either way is not green"

    # A run that stops arriving reads unknown, not yesterday's verdict.
    warehouse._command("UPDATE @screen_self_audit SET measured_at = measured_at - interval '3 days'")
    statuses = {
        row["status"]
        for row in warehouse._query_dicts("SELECT status FROM @marts_screen_self_audit WHERE is_latest")
    }
    assert statuses == {"unknown"}


def test_the_collector_records_a_run(warehouse):  # noqa: F811
    _provision_every_table(warehouse)
    warehouse.ensure_timeline_tables()
    PipelineHealthCollector(warehouse).run()
    rows = warehouse._query_dicts("SELECT channel FROM @screen_self_audit ORDER BY channel")
    assert [row["channel"] for row in rows] == ["email", "phone_messages", "slack", "whatsapp"]
