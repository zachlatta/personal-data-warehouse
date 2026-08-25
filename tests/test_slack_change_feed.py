"""The change feed: which conversations actually need a history call."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from personal_data_warehouse.slack_change_feed import (
    SlackChangeFeed,
    changed_conversations,
)


def _counts(**buckets):
    return {"ok": True, **buckets}


def test_only_conversations_newer_than_our_cursor_are_returned():
    """This is the whole point: 690 conversations in, a handful out.

    The freshness pass polls up to 950 conversations every five minutes because
    the public API cannot say which ones changed. client.counts can, so the pass
    becomes "ask once, then fetch only what moved".
    """
    payload = _counts(
        ims=[
            {"id": "D1", "latest": "1787600000.000000"},
            {"id": "D2", "latest": "1787500000.000000"},
        ],
        mpims=[{"id": "C_MP", "latest": "1787600500.000000"}],
        channels=[{"id": "C1", "latest": "1787400000.000000"}],
    )
    cursors = {
        "D1": 1787500000.0,   # moved
        "D2": 1787500000.0,   # unchanged
        "C_MP": 1787600500.0,  # exactly at our cursor -> unchanged
        "C1": 1787400000.0,   # unchanged
    }
    assert changed_conversations(payload, cursors) == ["D1"]


def test_a_conversation_we_have_never_synced_counts_as_changed():
    payload = _counts(ims=[{"id": "D_NEW", "latest": "1787600000.000000"}])
    assert changed_conversations(payload, {}) == ["D_NEW"]


def test_entries_without_a_latest_marker_are_ignored_not_polled():
    # A bucket entry with no latest tells us nothing; treating it as changed
    # would quietly reintroduce the full-poll behaviour we are removing.
    payload = _counts(ims=[{"id": "D1"}, {"id": "D2", "latest": ""}])
    assert changed_conversations(payload, {}) == []


def test_results_are_ordered_newest_first():
    payload = _counts(
        ims=[
            {"id": "D_OLD", "latest": "1787600000.000000"},
            {"id": "D_NEW", "latest": "1787699999.000000"},
        ]
    )
    assert changed_conversations(payload, {}) == ["D_NEW", "D_OLD"]


def test_a_failed_counts_call_raises_rather_than_reporting_nothing_changed():
    """An empty answer and a broken answer must not look alike.

    Returning [] on failure would read as 'Slack is quiet' and silently stop all
    ingestion -- exactly the class of failure this whole change exists to fix.
    """
    with pytest.raises(SlackChangeFeed.Error, match="not_allowed_token_type"):
        changed_conversations({"ok": False, "error": "not_allowed_token_type"}, {})


def test_feed_reports_coverage_so_the_gap_is_visible():
    """client.counts covers conversations Zach is in -- not all ~13k channels.

    Measured on the real workspace: 316 channels (exactly the 317 he belongs to),
    237 open DMs, 137 open group DMs. The public channels he is not a member of
    are invisible here and still need the slow sweep, so the number is reported
    rather than assumed.
    """
    feed = SlackChangeFeed.from_counts(
        _counts(
            channels=[{"id": "C1", "latest": "1.0"}],
            ims=[{"id": "D1", "latest": "1.0"}],
            mpims=[],
        )
    )
    assert feed.covered_conversation_ids == {"C1", "D1"}
    assert feed.coverage == {"channels": 1, "ims": 1, "mpims": 0}
