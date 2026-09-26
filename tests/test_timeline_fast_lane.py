"""The fast lane lands a source's timeline rows from the source's own ingest run."""

from __future__ import annotations

from datetime import timedelta
import logging

import pytest

from personal_data_warehouse import timeline_fast_lane
from personal_data_warehouse.timeline import AdapterSyncStats
from tests.test_timeline import (  # noqa: F401 - `warehouse` is the DB fixture
    _NOW,
    _engine,
    _ensure_all_source_tables,
    _seed_sources,
    warehouse,
)


class _FakeEngine:
    instances: list["_FakeEngine"] = []

    def __init__(self, *, source_url: str) -> None:
        self.source_url = source_url
        self.calls: list[dict] = []
        self.closed = False
        _FakeEngine.instances.append(self)

    def run_incremental(self, *, adapter_names, max_seconds):
        self.calls.append({"adapter_names": list(adapter_names), "max_seconds": max_seconds})
        return [AdapterSyncStats(adapter=name, incremental_rows=3, backfill_done=True) for name in adapter_names]

    def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def _reset():
    _FakeEngine.instances = []
    yield
    _FakeEngine.instances = []


def test_adapter_names_are_resolved_from_the_registry_by_source() -> None:
    assert timeline_fast_lane.fast_lane_adapter_names(["whatsapp"]) == ["whatsapp_message"]
    assert timeline_fast_lane.fast_lane_adapter_names(["slack"]) == ["slack_message", "slack_file"]
    assert timeline_fast_lane.fast_lane_adapter_names(["agent_sessions"]) == [
        "agent_session",
        "agent_session_turn",
    ]
    with pytest.raises(ValueError, match="no timeline adapter"):
        timeline_fast_lane.fast_lane_adapter_names(["not_a_source"])


def test_land_sources_runs_only_that_sources_adapters_and_closes_the_engine() -> None:
    summary = timeline_fast_lane.land_sources_on_timeline(
        postgres_url="postgresql://example/warehouse",
        sources=["apple_messages"],
        logger=logging.getLogger("test"),
        engine_factory=_FakeEngine,
    )
    engine = _FakeEngine.instances[0]
    assert engine.source_url == "postgresql://example/warehouse"
    assert engine.calls == [
        {"adapter_names": ["apple_message"], "max_seconds": timeline_fast_lane.TIMELINE_FAST_LANE_BUDGET_SECONDS}
    ]
    assert engine.closed
    assert summary["enabled"] is True
    assert summary["adapters"] == ["apple_message"]
    assert summary["rows"] == 3
    assert summary["errors"] == {}


def test_kill_switch_runs_nothing(monkeypatch) -> None:
    monkeypatch.setenv(timeline_fast_lane.TIMELINE_FAST_LANE_ENABLED_ENV, "0")
    summary = timeline_fast_lane.land_sources_on_timeline(
        postgres_url="postgresql://example/warehouse",
        sources=["whatsapp"],
        logger=logging.getLogger("test"),
        engine_factory=_FakeEngine,
    )
    assert summary["enabled"] is False
    assert _FakeEngine.instances == []


def test_an_engine_failure_never_reaches_the_ingest_run() -> None:
    class _Broken:
        def __init__(self, **_kwargs) -> None:
            raise RuntimeError("no database")

    summary = timeline_fast_lane.land_sources_on_timeline(
        postgres_url="postgresql://example/warehouse",
        sources=["gmail"],
        logger=logging.getLogger("test"),
        engine_factory=_Broken,
    )
    assert summary["errors"] == {"fast_lane": "no database"}
    assert summary["rows"] == 0


# -- against a real warehouse -------------------------------------------------


def test_fast_lane_lands_new_rows_for_one_source_only(warehouse):
    _ensure_all_source_tables(warehouse)
    _seed_sources(warehouse)
    engine = _engine(warehouse)
    try:
        engine.run()  # first contact + backfill for every adapter
        later = _NOW + timedelta(minutes=10)
        warehouse._command(
            """
            INSERT INTO @slack_messages (account, team_id, conversation_id, message_ts,
                                        message_datetime, user_id, text, synced_at)
            VALUES ('z', 'T1', 'C1', '2000.1', %s, 'U1', 'fast lane message', %s)
            """,
            (later, later),
        )
        warehouse._command(
            "UPDATE @gmail_messages SET subject = 'edited by gmail', synced_at = %s WHERE message_id = 'm1'",
            (later,),
        )

        stats = engine.run_incremental(adapter_names=["slack_message"])
    finally:
        engine.close()

    assert [s.adapter for s in stats] == ["slack_message"]
    assert stats[0].incremental_rows == 1
    assert stats[0].error == ""
    rows = warehouse._query_dicts(
        "SELECT event_id, title FROM @timeline_events WHERE event_id IN ('z|T1|C1|2000.1', 'z@x.test|m1')"
    )
    by_id = {row["event_id"]: row["title"] for row in rows}
    assert "z|T1|C1|2000.1" in by_id, "the slack row landed in the fast lane"
    assert by_id["z@x.test|m1"] != "edited by gmail", "gmail was not asked for and did not move"


def test_fast_lane_skips_an_adapter_the_scheduled_pass_has_not_initialized(warehouse):
    _ensure_all_source_tables(warehouse)
    _seed_sources(warehouse)
    engine = _engine(warehouse)
    try:
        stats = engine.run_incremental(adapter_names=["whatsapp_message"])
        assert stats[0].incremental_rows == 0 and stats[0].error == ""
        assert warehouse._query("SELECT count(*) FROM @timeline_sync_state")[0][0] == 0, (
            "first contact belongs to the scheduled pass"
        )
        assert warehouse._query("SELECT count(*) FROM @timeline_events")[0][0] == 0
    finally:
        engine.close()


def test_fast_lane_skips_an_adapter_whose_incremental_lock_is_held(warehouse):
    _ensure_all_source_tables(warehouse)
    _seed_sources(warehouse)
    scheduled = _engine(warehouse)
    fast = _engine(warehouse)
    try:
        scheduled.run()
        later = _NOW + timedelta(minutes=10)
        warehouse._command(
            """
            INSERT INTO @slack_messages (account, team_id, conversation_id, message_ts,
                                        message_datetime, user_id, text, synced_at)
            VALUES ('z', 'T1', 'C1', '2000.2', %s, 'U1', 'while locked', %s)
            """,
            (later, later),
        )
        adapter = next(a for a in scheduled._adapters if a.name == "slack_message")
        assert scheduled._try_adapter_incremental_lock(adapter)
        try:
            stats = fast.run_incremental(adapter_names=["slack_message"])
        finally:
            scheduled._release_adapter_incremental_lock(adapter)
        assert stats[0].incremental_rows == 0 and stats[0].error == ""
        assert warehouse._query(
            "SELECT count(*) FROM @timeline_events WHERE event_id = 'z|T1|C1|2000.2'"
        )[0][0] == 0
        # Released: the next fast lane lands it.
        stats = fast.run_incremental(adapter_names=["slack_message"])
        assert stats[0].incremental_rows == 1
    finally:
        scheduled.close()
        fast.close()


def test_a_stale_scheduled_save_cannot_rewind_a_watermark_the_fast_lane_advanced(warehouse):
    _ensure_all_source_tables(warehouse)
    _seed_sources(warehouse)
    scheduled = _engine(warehouse)
    fast = _engine(warehouse)
    try:
        scheduled.run()
        adapter = next(a for a in scheduled._adapters if a.name == "slack_message")
        stale = scheduled._load_state(adapter)  # the scheduled pass's in-memory copy
        later = _NOW + timedelta(minutes=10)
        warehouse._command(
            """
            INSERT INTO @slack_messages (account, team_id, conversation_id, message_ts,
                                        message_datetime, user_id, text, synced_at)
            VALUES ('z', 'T1', 'C1', '2000.3', %s, 'U1', 'advances the watermark', %s)
            """,
            (later, later),
        )
        fast.run_incremental(adapter_names=["slack_message"])
        advanced = warehouse._query(
            "SELECT watermark_ingest_ts FROM @timeline_sync_state WHERE adapter = 'slack_message'"
        )[0][0]
        assert advanced == later

        scheduled._save_state(adapter, stale)  # e.g. after its refresh phase

        assert warehouse._query(
            "SELECT watermark_ingest_ts FROM @timeline_sync_state WHERE adapter = 'slack_message'"
        )[0][0] == later, "the stored watermark only ever moves forward"
    finally:
        scheduled.close()
        fast.close()
