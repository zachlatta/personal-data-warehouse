"""C11: every source states its own SLA and detects it against the source itself.

The registry half is pure: every ``kind="source"`` pipeline states where its
interval came from and names either a detector view or a justified exemption.
The detector half seeds ``ops.source_upstream_observations`` beside base rows
and reads ``marts_ops.source_upstream_health`` back.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import pytest
from dotenv import load_dotenv

from tests.conftest import cleanup_test_warehouse, make_test_schema
from tests.test_schema_reorg_contract import _provision_everything

from personal_data_warehouse.pipeline_health import (
    PIPELINES,
    PipelineHealthCollector,
    worst_detector_status,
)
from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.relations import CATALOG, relation
from personal_data_warehouse.source_sla import (
    SOURCE_UPSTREAM_CHECKS,
    SOURCE_UPSTREAM_HEALTH_VIEW,
    source_upstream_health_view_sql,
    upstream_check,
)

SOURCES = [entry for entry in PIPELINES if entry.kind == "source"]


def _postgres_url() -> str:
    load_dotenv()
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if not url:
        pytest.skip("POSTGRES_DATABASE_URL is not set")
    return url


@pytest.fixture()
def warehouse():
    wh = PostgresWarehouse(_postgres_url(), schema=make_test_schema())
    try:
        yield wh
    finally:
        cleanup_test_warehouse(wh)


# --- the registry (pure) ------------------------------------------------------


@pytest.mark.parametrize("entry", SOURCES, ids=[entry.id for entry in SOURCES])
def test_every_source_states_its_sla_and_names_a_detector_or_an_exemption(entry):
    assert entry.data_basis.strip(), f"{entry.id}: state where its data interval came from (basis=)"
    assert bool(entry.detector) != bool(entry.sla_exemption), (
        f"{entry.id}: name exactly one of detector= (a marts_ops view comparing PDW with the"
        " source itself) or exemption= (why none is possible yet, and what it would need)"
    )


@pytest.mark.parametrize(
    "entry",
    [entry for entry in SOURCES if entry.sla_exemption],
    ids=[entry.id for entry in SOURCES if entry.sla_exemption],
)
def test_an_exemption_is_a_reason_not_a_placeholder(entry):
    # "n/a" or "todo" is how a gap hides inside a filled-in field.
    assert len(entry.sla_exemption) >= 80, entry.id
    assert not any(word in entry.sla_exemption.lower() for word in ("todo", "tbd", "n/a")), entry.id


def test_a_detector_is_a_cataloged_marts_ops_view():
    views = {obj.id for obj in CATALOG.objects if obj.kind == "view" and obj.schema == "marts_ops"}
    for entry in SOURCES:
        if entry.detector:
            assert entry.detector in views, f"{entry.id}: {entry.detector} is not a marts_ops view"


def test_the_upstream_checks_and_the_pipelines_that_read_them_agree():
    readers = {entry.id for entry in PIPELINES if entry.detector == SOURCE_UPSTREAM_HEALTH_VIEW}
    assert readers == {check.pipeline for check in SOURCE_UPSTREAM_CHECKS}
    # The five sources the C11 run named first.
    assert {"gmail", "google_calendar", "google_drive", "apple_messages", "whatsapp"} <= readers


def test_every_upstream_check_is_one_cheap_lookup_per_observation():
    for check in SOURCE_UPSTREAM_CHECKS:
        assert check.tolerance < check.observation_interval * 6, check.pipeline
        assert "o.scope" in check.pdw_newest_sql, check.pipeline
        assert check.evidence, check.pipeline
    assert "CREATE OR REPLACE VIEW @marts_ops_source_upstream_health" in source_upstream_health_view_sql()


def test_detector_statuses_fold_to_the_worst_word():
    assert worst_detector_status(["ok", "late", "ok"]) == "late"
    assert worst_detector_status(["ok", "failing", "late"]) == "failing"
    assert worst_detector_status(["quiet", "ok"]) in ("ok", "quiet")
    assert worst_detector_status(["attention", "ok"]) == "attention"
    # A word no detector used before still outranks ok.
    assert worst_detector_status(["ok", "brand_new_word"]) == "brand_new_word"
    # Silence is not health.
    assert worst_detector_status([]) == "unknown"


# --- the detector view --------------------------------------------------------


def _observe(wh, pipeline, scope, *, newest_at, newest_id="", observed_at):
    wh.record_source_upstream_observation(
        pipeline=pipeline,
        scope=scope,
        upstream_newest_at=newest_at,
        upstream_newest_id=newest_id,
        observed_at=observed_at,
    )


def _health(wh) -> dict[tuple[str, str], dict]:
    return {
        (row["pipeline"], row["scope"]): row
        for row in wh._query_dicts("SELECT * FROM @marts_ops_source_upstream_health")
    }


def test_upstream_health_compares_each_sources_newest_item_with_what_landed(warehouse):
    _provision_everything(warehouse)
    now = datetime.now(tz=UTC)
    gmail_tolerance = upstream_check("gmail").tolerance

    # Gmail: the newest message the API lists has landed.
    warehouse._command(
        "INSERT INTO @gmail_messages (account, message_id, internal_date, synced_at)"
        " VALUES ('a@x.test', 'm-new', %s, %s)",
        (now - timedelta(minutes=5), now),
    )
    _observe(warehouse, "gmail", "a@x.test", newest_at=now - timedelta(minutes=5, milliseconds=400),
             newest_id="m-new", observed_at=now)
    # Gmail, a second mailbox: the API's newest has been missing past tolerance.
    warehouse._command(
        "INSERT INTO @gmail_messages (account, message_id, internal_date, synced_at)"
        " VALUES ('b@x.test', 'm-old', %s, %s)",
        (now - timedelta(days=2), now),
    )
    _observe(warehouse, "gmail", "b@x.test", newest_at=now - 2 * gmail_tolerance,
             newest_id="m-missing", observed_at=now)
    # Calendar: the calendar's own `updated` is far ahead of every event PDW has.
    warehouse._command(
        "INSERT INTO @calendar_events (account, calendar_id, event_id, updated_at, synced_at)"
        " VALUES ('a@x.test', 'primary', 'e1', %s, %s)",
        (now - timedelta(days=3), now),
    )
    _observe(warehouse, "google_calendar", "a@x.test/primary", newest_at=now - timedelta(hours=10),
             observed_at=now)
    # Drive: the newest upstream file is one the sync keeps as excluded.
    warehouse._command(
        "INSERT INTO @google_drive_files (account, file_id, modified_time, is_excluded, trashed)"
        " VALUES ('a@x.test', 'f1', %s, 1, 0)",
        (now - timedelta(hours=5),),
    )
    _observe(warehouse, "google_drive", "a@x.test", newest_at=now - timedelta(hours=5),
             newest_id="f1", observed_at=now)
    # WhatsApp: nothing new arrived, but the client has not looked in a day.
    _observe(warehouse, "whatsapp", "a@x.test", newest_at=None, observed_at=now - timedelta(days=1))
    # Apple Messages: a message newer than anything landed, inside tolerance.
    _observe(warehouse, "apple_messages", "a@x.test", newest_at=now - timedelta(minutes=2),
             observed_at=now)

    rows = _health(warehouse)
    assert rows[("gmail", "a@x.test")]["status"] == "ok"
    assert rows[("gmail", "a@x.test")]["landed"] == 1
    assert rows[("gmail", "a@x.test")]["missing_seconds"] is None

    missing = rows[("gmail", "b@x.test")]
    assert missing["status"] == "late"
    assert missing["landed"] == 0
    assert missing["missing_seconds"] >= 2 * gmail_tolerance.total_seconds() - 60
    assert missing["upstream_newest_id"] == "m-missing"

    assert rows[("google_calendar", "a@x.test/primary")]["status"] == "failing"
    assert rows[("google_drive", "a@x.test")]["status"] == "ok"
    assert rows[("whatsapp", "a@x.test")]["status"] == "unknown"
    # Absence is the epoch in ops and NULL in marts.
    assert rows[("whatsapp", "a@x.test")]["upstream_newest_at"] is None
    assert rows[("apple_messages", "a@x.test")]["status"] == "ok"
    assert rows[("apple_messages", "a@x.test")]["landed"] == 0
    assert rows[("apple_messages", "a@x.test")]["pdw_newest_at"] is None


def test_an_older_observation_never_overwrites_a_newer_one(warehouse):
    _provision_everything(warehouse)
    now = datetime.now(tz=UTC)
    _observe(warehouse, "gmail", "a@x.test", newest_at=now, newest_id="new", observed_at=now)
    _observe(warehouse, "gmail", "a@x.test", newest_at=now - timedelta(days=1), newest_id="old",
             observed_at=now - timedelta(minutes=1))
    assert _health(warehouse)[("gmail", "a@x.test")]["upstream_newest_id"] == "new"


def test_the_collector_writes_each_sources_detector_verdict_or_exemption(warehouse):
    _provision_everything(warehouse)
    now = datetime.now(tz=UTC)
    _observe(warehouse, "google_calendar", "a@x.test/primary", newest_at=now - timedelta(hours=10),
             observed_at=now)
    PipelineHealthCollector(warehouse).run()
    rows = {
        row["pipeline"]: row
        for row in warehouse._query_dicts(
            "SELECT pipeline, kind, data_basis, sla_detector, sla_detector_status, sla_exemption"
            " FROM @marts_pipeline_health"
        )
    }
    calendar = rows["google_calendar"]
    assert calendar["sla_detector"] == "marts_ops.source_upstream_health"
    assert calendar["sla_detector_status"] == "failing"
    # A detector with no row for this pipeline has said nothing, which is not ok.
    assert rows["gmail"]["sla_detector_status"] == "unknown"
    assert rows["slack"]["sla_detector"] == "marts_ops.slack_conversation_health"
    assert rows["hacker_news"]["sla_detector"] is None
    assert rows["hacker_news"]["sla_detector_status"] is None
    assert "re-lists" in rows["hacker_news"]["sla_exemption"]
    for row in rows.values():
        if row["kind"] == "source":
            assert row["data_basis"], row["pipeline"]
            assert bool(row["sla_detector"]) != bool(row["sla_exemption"]), row["pipeline"]
        else:
            assert row["sla_detector"] is None and row["sla_exemption"] is None, row["pipeline"]


def test_the_query_role_can_read_the_detector(warehouse, query_login):
    _provision_everything(warehouse)
    _observe(warehouse, "gmail", "a@x.test", newest_at=None, observed_at=datetime.now(tz=UTC))
    query_login(warehouse)
    connection = warehouse.read_only_connection()
    try:
        with connection.cursor() as cursor:
            rel = relation(SOURCE_UPSTREAM_HEALTH_VIEW).with_namespace(warehouse.schema_namespace)
            cursor.execute(f'SELECT status FROM "{rel.schema}"."{rel.name}"')
            assert cursor.fetchall() == [("ok",)]
    finally:
        connection.close()
