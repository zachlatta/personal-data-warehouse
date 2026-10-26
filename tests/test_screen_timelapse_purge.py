"""screen_timelapse.purge_moment: one reviewed step deletes a leaked screen capture.

A frame that caught something that must not be kept used to need superuser SQL
and a one-off Drive script. The purge is now a reviewed mutation: the Go app refuses a moment that matches nothing, and
the cloud worker deletes the moment's rows in one transaction, then each stored
object, read back until Drive says it is gone. These tests pin the order (rows
first, objects after the commit, retried without redoing the rows), that what is
deleted is not written back by the timeline sync or the enrichment pass, and
that the record keeps storage ids and no content.
"""
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import subprocess
import threading

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import make_dsn
import pytest

from personal_data_warehouse.defs.upstream_mutations import (
    RECLAIMABLE_IDEMPOTENT_OPERATIONS,
    process_upstream_mutation_batch,
)
from personal_data_warehouse.objectstore import ObjectNotFoundError
from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.screen_timelapse_enrichment import load_screen_description_candidates
from personal_data_warehouse.screen_timelapse_purge import (
    SCREEN_TIMELAPSE_PROVIDER,
    SCREEN_TIMELAPSE_PURGE_MOMENT_OPERATION,
    ScreenTimelapsePurgeExecutor,
)
from personal_data_warehouse.timeline import TIMELINE_ADAPTERS, TimelineSyncEngine
from tests.conftest import cleanup_test_warehouse, make_test_schema
from tests.test_marts_layering import _postgres_url
from tests.test_upstream_mutations_postgres import _seed_mutation_request


MOMENT = datetime(2026, 10, 25, 14, 3, tzinfo=UTC)
NEIGHBOUR = datetime(2026, 10, 25, 14, 4, tzinfo=UTC)
DEVICE = "laptop"
# What the leaked frame showed. It must never reach the mutation record.
LEAKED_TEXT = "4111 1111 1111 1111"
SCREEN_CAPTURE = next(adapter for adapter in TIMELINE_ADAPTERS if adapter.name == "screen_capture")


class FakeDrive:
    """An object store that deletes like Drive: a missing file is a no-op."""

    def __init__(self, file_ids, *, failing_deletes: int = 0, deletes_stick: bool = True) -> None:
        self.objects = {file_id: b"bytes" for file_id in file_ids}
        self.failing_deletes = failing_deletes
        self.deletes_stick = deletes_stick
        self.deleted: list[str] = []

    def delete_object(self, ref) -> None:
        if self.failing_deletes > 0:
            self.failing_deletes -= 1
            raise RuntimeError("Drive answered 503")
        self.deleted.append(ref["storage_file_id"])
        if self.deletes_stick:
            self.objects.pop(ref["storage_file_id"], None)

    def get_metadata(self, ref):
        if ref["storage_file_id"] not in self.objects:
            raise ObjectNotFoundError(ref["storage_file_id"])
        return {"storage_file_id": ref["storage_file_id"]}


@contextmanager
def _lock(acquired: bool = True) -> Iterator[bool]:
    yield acquired


def _ensure(wh: PostgresWarehouse) -> None:
    wh.ensure_screen_timelapse_tables()
    wh.ensure_upstream_mutation_tables()
    wh.ensure_timeline_tables()


@pytest.fixture()
def warehouse():
    wh = PostgresWarehouse(_postgres_url(), schema=make_test_schema("screen_purge"))
    try:
        _ensure(wh)
        yield wh
    finally:
        cleanup_test_warehouse(wh)


def _screenshot(wh, captured_at, display, *, sha, file_id) -> None:
    wh._command(
        """
        INSERT INTO @screen_timelapse_screenshots (device, captured_at, display_index,
            source_filename, content_sha256, mime_type, size_bytes, storage_backend,
            storage_key, storage_file_id, storage_url, ingested_at)
        VALUES (%s, %s, %s, %s, %s, 'image/png', 1000, 'google_drive', %s, %s, '', %s)
        """,
        (DEVICE, captured_at, display, f"{file_id}.png", sha, f"key/{file_id}", file_id, captured_at),
    )


def _tree(wh, captured_at, *, file_id) -> None:
    wh._command(
        """
        INSERT INTO @screen_timelapse_accessibility_trees (device, captured_at, source_filename,
            content_sha256, mime_type, size_bytes, storage_backend, storage_key, storage_file_id,
            storage_url, ingested_at, content_text)
        VALUES (%s, %s, %s, %s, 'application/gzip', 100, 'google_drive', %s, %s, '', %s, %s)
        """,
        (DEVICE, captured_at, f"{file_id}.ax", f"sha-{file_id}", f"key/{file_id}", file_id, captured_at, LEAKED_TEXT),
    )


def _reading(captured_at, display, *, sha, prompt_version="v1") -> dict:
    return {
        "device": DEVICE,
        "captured_at": captured_at,
        "display_index": display,
        "prompt_version": prompt_version,
        "content_sha256": sha,
        "status": "completed",
        "screen_state": "active",
        "screen_text": LEAKED_TEXT,
        "created_at": captured_at,
    }


def _seed_moments(wh) -> None:
    """The leaked moment (two displays, a tree, three readings) and its neighbour."""
    _screenshot(wh, MOMENT, 1, sha="sha-main", file_id="drive-shot-1")
    _screenshot(wh, MOMENT, 2, sha="sha-side", file_id="drive-shot-2")
    _tree(wh, MOMENT, file_id="drive-tree")
    _screenshot(wh, NEIGHBOUR, 1, sha="sha-side", file_id="drive-next-1")
    _tree(wh, NEIGHBOUR, file_id="drive-next-tree")
    wh.insert_screen_timelapse_descriptions(
        [
            _reading(MOMENT, 1, sha="sha-main"),
            _reading(MOMENT, 1, sha="sha-main", prompt_version="v2"),
            _reading(MOMENT, 2, sha="sha-side"),
            _reading(NEIGHBOUR, 1, sha="sha-side"),
        ]
    )


def _sync_timeline(wh) -> None:
    engine = TimelineSyncEngine(
        source_url=_postgres_url(), source_schema=wh._schema, dest_schema=wh._schema, adapters=[SCREEN_CAPTURE]
    )
    try:
        engine.run()
    finally:
        engine.close()


def _events(wh) -> list[str]:
    return [
        row["event_id"]
        for row in wh._query_dicts(
            "SELECT event_id FROM @timeline_events WHERE adapter = 'screen_capture' ORDER BY event_id"
        )
    ]


def _count(wh, relation: str, captured_at) -> int:
    return int(wh._query(f"SELECT count(*) FROM @{relation} WHERE device = %s AND captured_at = %s", (DEVICE, captured_at))[0][0])


def _propose(wh, request_id: str, **payload) -> str:
    request = _seed_mutation_request(
        wh,
        request_id=request_id,
        title="Delete a leaked frame",
        account=DEVICE,
        mutations=[
            {
                "provider": SCREEN_TIMELAPSE_PROVIDER,
                "operation": SCREEN_TIMELAPSE_PURGE_MOMENT_OPERATION,
                "payload": {"device": DEVICE, "captured_at": "2026-10-25T14:03:00Z", "reason": "card number on screen", **payload},
            }
        ],
    )
    return request["mutations"][0]["id"]


def _work(wh, drive: FakeDrive, *, lock_acquired: bool = True):
    executor = ScreenTimelapsePurgeExecutor(
        warehouse=wh, object_store_factory=lambda: drive, timeline_lock=lambda: _lock(lock_acquired)
    )
    return process_upstream_mutation_batch(
        warehouse=wh,
        gmail_executor=None,  # type: ignore[arg-type]
        contact_executor=None,  # type: ignore[arg-type]
        calendar_executor=None,  # type: ignore[arg-type]
        slack_executor=None,  # type: ignore[arg-type]
        screen_timelapse_executor=executor,
        limit=10,
        claimed_by="purge-test",
        ensure_tables=False,
        on_gmail_settings_changed=None,
    )


def _all_drive_ids() -> list[str]:
    return ["drive-shot-1", "drive-shot-2", "drive-tree", "drive-next-1", "drive-next-tree"]


def test_a_purge_deletes_the_moment_everywhere_and_records_ids_not_content(warehouse) -> None:
    _seed_moments(warehouse)
    _sync_timeline(warehouse)
    assert _events(warehouse) == ["laptop|2026-10-25T14:03:00Z", "laptop|2026-10-25T14:04:00Z"]
    mutation_id = _propose(warehouse, "req_purge_all")
    drive = FakeDrive(_all_drive_ids())

    summary = _work(warehouse, drive)

    assert summary.succeeded == 1
    for relation in ("screen_timelapse_screenshots", "screen_timelapse_accessibility_trees", "screen_timelapse_descriptions"):
        assert _count(warehouse, relation, MOMENT) == 0, relation
        assert _count(warehouse, relation, NEIGHBOUR) == 1, relation
    assert _events(warehouse) == ["laptop|2026-10-25T14:04:00Z"]
    assert sorted(drive.deleted) == ["drive-shot-1", "drive-shot-2", "drive-tree"]
    assert sorted(drive.objects) == ["drive-next-1", "drive-next-tree"]

    mutation = warehouse.get_upstream_mutation(mutation_id)
    result = mutation["result_json"]
    assert mutation["status"] == "succeeded"
    assert result["phase"] == "complete"
    assert result["deleted"] == {
        "screen_timelapse_screenshots": 2,
        "screen_timelapse_accessibility_trees": 1,
        "screen_timelapse_descriptions": 3,
        "timeline_events": 1,
        "search_chunks": 0,
    }
    assert result["timeline_event_ids"] == ["laptop|2026-10-25T14:03:00Z"]
    assert {(o["storage_file_id"], o["status"]) for o in result["objects"]} == {
        ("drive-shot-1", "deleted"),
        ("drive-shot-2", "deleted"),
        ("drive-tree", "deleted"),
    }
    assert result["not_touched"] == ["local source files on the device", "pgBackRest backups on slowking"]
    events = warehouse._query_dicts(
        "SELECT event_json FROM @upstream_mutation_events WHERE mutation_id = %s", (mutation_id,)
    )
    assert LEAKED_TEXT not in json.dumps(mutation, default=str) + json.dumps(events, default=str)

    # Nothing writes it back: the timeline sync and the enrichment pass see no frame.
    _sync_timeline(warehouse)
    assert _events(warehouse) == ["laptop|2026-10-25T14:04:00Z"]
    candidates = load_screen_description_candidates(
        warehouse, prompt_version="v3", since=datetime(2026, 1, 1, tzinfo=UTC), limit=None
    )
    assert [(c["captured_at"], c["display_index"]) for c in candidates] == [(NEIGHBOUR, 1)]


def test_a_failed_drive_delete_retries_only_the_objects(warehouse) -> None:
    _seed_moments(warehouse)
    mutation_id = _propose(warehouse, "req_purge_retry")
    drive = FakeDrive(_all_drive_ids(), failing_deletes=1)

    first = _work(warehouse, drive)

    assert first.failed_retryable == 1
    mutation = warehouse.get_upstream_mutation(mutation_id)
    assert mutation["status"] == "failed_retryable"
    assert mutation["result_json"]["phase"] == "rows_deleted"
    assert "not yet confirmed gone" in mutation["error"]
    # The rows went in their own transaction before Drive was asked.
    assert _count(warehouse, "screen_timelapse_screenshots", MOMENT) == 0
    assert "drive-shot-1" in drive.objects

    second = _work(warehouse, drive)

    assert second.succeeded == 1
    assert warehouse.get_upstream_mutation(mutation_id)["result_json"]["phase"] == "complete"
    assert sorted(drive.objects) == ["drive-next-1", "drive-next-tree"]
    # The neighbour, a later frame, was never touched by the retry.
    assert _count(warehouse, "screen_timelapse_screenshots", NEIGHBOUR) == 1


def test_an_object_still_present_after_delete_is_not_a_success(warehouse) -> None:
    _seed_moments(warehouse)
    mutation_id = _propose(warehouse, "req_purge_sticky")

    summary = _work(warehouse, FakeDrive(_all_drive_ids(), deletes_stick=False))

    assert summary.failed_retryable == 1
    assert "still present after delete" in warehouse.get_upstream_mutation(mutation_id)["error"]


def test_one_display_keeps_the_timeline_event_but_not_the_moments_tree(warehouse) -> None:
    _seed_moments(warehouse)
    _sync_timeline(warehouse)
    _propose(warehouse, "req_purge_side", display_index=2)
    drive = FakeDrive(_all_drive_ids())

    assert _work(warehouse, drive).succeeded == 1

    shots = warehouse._query("SELECT display_index FROM @screen_timelapse_screenshots WHERE captured_at = %s", (MOMENT,))
    assert shots == [(1,)]
    readings = warehouse._query(
        "SELECT display_index, prompt_version FROM @screen_timelapse_descriptions WHERE captured_at = %s ORDER BY 2",
        (MOMENT,),
    )
    assert readings == [(1, "v1"), (1, "v2")]
    # The tree is one per moment and can hold what display 2 showed.
    assert _count(warehouse, "screen_timelapse_accessibility_trees", MOMENT) == 0
    assert _events(warehouse) == ["laptop|2026-10-25T14:03:00Z", "laptop|2026-10-25T14:04:00Z"]
    assert sorted(drive.deleted) == ["drive-shot-2", "drive-tree"]


def test_a_moment_gone_by_execution_time_fails_and_deletes_nothing(warehouse) -> None:
    _seed_moments(warehouse)
    mutation_id = _propose(warehouse, "req_purge_none", captured_at="2026-10-25T15:00:00Z")
    drive = FakeDrive(_all_drive_ids())

    assert _work(warehouse, drive).failed_terminal == 1

    assert "no screen timelapse rows match" in warehouse.get_upstream_mutation(mutation_id)["error"]
    assert drive.deleted == []
    assert _count(warehouse, "screen_timelapse_screenshots", MOMENT) == 2


def test_a_running_timeline_sync_defers_the_purge(warehouse) -> None:
    _seed_moments(warehouse)
    mutation_id = _propose(warehouse, "req_purge_locked")
    drive = FakeDrive(_all_drive_ids())

    assert _work(warehouse, drive, lock_acquired=False).failed_retryable == 1

    assert _count(warehouse, "screen_timelapse_screenshots", MOMENT) == 2
    assert drive.deleted == []
    assert warehouse.get_upstream_mutation(mutation_id)["result_json"] == {}
    assert _work(warehouse, drive).succeeded == 1


def test_a_reading_written_after_its_frame_was_purged_is_dropped(warehouse) -> None:
    _seed_moments(warehouse)
    _propose(warehouse, "req_purge_stale")
    assert _work(warehouse, FakeDrive(_all_drive_ids())).succeeded == 1

    # An enrichment run that paged the frame before the purge writes its answer.
    warehouse.insert_screen_timelapse_descriptions([_reading(MOMENT, 1, sha="sha-main", prompt_version="v9")])
    # A reading of bytes the frame no longer has is dropped too.
    warehouse.insert_screen_timelapse_descriptions([_reading(NEIGHBOUR, 1, sha="sha-old", prompt_version="v9")])
    warehouse.insert_screen_timelapse_descriptions([_reading(NEIGHBOUR, 1, sha="sha-side", prompt_version="v9")])

    assert _count(warehouse, "screen_timelapse_descriptions", MOMENT) == 0
    assert warehouse._query(
        "SELECT content_sha256 FROM @screen_timelapse_descriptions WHERE captured_at = %s AND prompt_version = 'v9'",
        (NEIGHBOUR,),
    ) == [("sha-side",)]


def test_a_reading_racing_the_purge_waits_for_it_and_then_writes_nothing(warehouse) -> None:
    _seed_moments(warehouse)
    enrichment = PostgresWarehouse(_postgres_url(), schema=warehouse._schema)
    written = threading.Event()
    try:
        with warehouse._connection.cursor() as cursor:
            cursor.execute("BEGIN")
            cursor.execute(
                warehouse._expand_relations("DELETE FROM @screen_timelapse_screenshots WHERE captured_at = %s"),
                (MOMENT,),
            )
            writer = threading.Thread(
                target=lambda: (
                    enrichment.insert_screen_timelapse_descriptions(
                        [_reading(MOMENT, 1, sha="sha-main", prompt_version="v9")]
                    ),
                    written.set(),
                )
            )
            writer.start()
            # The write waits on the purge's row lock instead of reading
            # the frame as still there.
            assert not written.wait(1.0)
            cursor.execute("COMMIT")
        writer.join(10)
        assert written.is_set()
    finally:
        enrichment.close()
    assert warehouse._query(
        "SELECT count(*) FROM @screen_timelapse_descriptions WHERE captured_at = %s AND prompt_version = 'v9'",
        (MOMENT,),
    ) == [(0,)]


def test_a_stale_executing_purge_is_reclaimable() -> None:
    assert (SCREEN_TIMELAPSE_PROVIDER, SCREEN_TIMELAPSE_PURGE_MOMENT_OPERATION) in RECLAIMABLE_IDEMPOTENT_OPERATIONS


@contextmanager
def _isolated_database() -> Iterator[tuple[PostgresWarehouse, str]]:
    """A database of its own at the default layout, which the Go store reads."""
    admin = psycopg2.connect(os.environ["POSTGRES_DATABASE_URL"])
    admin.autocommit = True
    database = make_test_schema("purge")
    warehouse = None
    with admin.cursor() as cursor:
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    try:
        url = make_dsn(os.environ["POSTGRES_DATABASE_URL"], dbname=database)
        warehouse = PostgresWarehouse(url)
        _ensure(warehouse)
        yield warehouse, url
    finally:
        if warehouse is not None:
            warehouse.close()
        with admin.cursor() as cursor:
            cursor.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))
        admin.close()


@pytest.mark.local_integration
def test_the_app_refuses_an_empty_moment_and_the_worker_purges_the_approved_one() -> None:
    with _isolated_database() as (warehouse, url):
        _seed_moments(warehouse)
        name = "TestScreenTimelapsePurgeProposalReviewApproval"
        result = subprocess.run(
            ["go", "test", "./internal/mutations", "-run", f"^{name}$", "-count=1", "-v"],
            cwd=Path(__file__).resolve().parents[1] / "app",
            env={**os.environ, "PDW_SCREEN_TIMELAPSE_PURGE_TEST_URL": url},
            capture_output=True,
            text=True,
            timeout=180,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        # A skipped Go test exits 0 too; the proposal half must actually run.
        assert f"--- PASS: {name}" in result.stdout, result.stdout + result.stderr

        drive = FakeDrive(_all_drive_ids())
        assert _work(warehouse, drive).succeeded == 1
        assert _count(warehouse, "screen_timelapse_screenshots", MOMENT) == 0
        assert sorted(drive.objects) == ["drive-next-1", "drive-next-tree"]
