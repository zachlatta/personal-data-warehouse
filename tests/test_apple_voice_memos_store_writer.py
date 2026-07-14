from __future__ import annotations

from pathlib import Path
import sqlite3
import sys
import zlib

import pytest

if sys.platform != "darwin":  # pragma: no cover - macOS-only integration tests
    pytest.skip("Voice Memos store writer is macOS-only", allow_module_level=True)

pytest.importorskip("CoreData")

from personal_data_warehouse_voice_memos.store_writer import (
    STATUS_RENAMED,
    STATUS_SKIPPED_MISSING,
    STATUS_SKIPPED_NOT_AUTO_NAMED,
    STATUS_WOULD_RENAME,
    apply_renames,
    load_cached_model_data,
)
from personal_data_warehouse_voice_memos.writeback import RenamePlanItem, TRANSACTION_AUTHOR


def _make_model():
    from CoreData import (
        NSAttributeDescription,
        NSEntityDescription,
        NSInteger64AttributeType,
        NSManagedObjectModel,
        NSStringAttributeType,
    )

    def attribute(name: str, attribute_type):
        description = NSAttributeDescription.alloc().init()
        description.setName_(name)
        description.setAttributeType_(attribute_type)
        description.setOptional_(True)
        return description

    entity = NSEntityDescription.alloc().init()
    entity.setName_("CloudRecording")
    entity.setManagedObjectClassName_("NSManagedObject")
    entity.setProperties_(
        [
            attribute("uniqueID", NSStringAttributeType),
            attribute("path", NSStringAttributeType),
            attribute("encryptedTitle", NSStringAttributeType),
            attribute("customLabelForSorting", NSStringAttributeType),
            attribute("customLabel", NSStringAttributeType),
            attribute("flags", NSInteger64AttributeType),
        ]
    )
    model = NSManagedObjectModel.alloc().init()
    model.setEntities_([entity])
    return model


def _open_container(store_path: Path, model):
    from CoreData import (
        NSPersistentContainer,
        NSPersistentHistoryTrackingKey,
        NSPersistentStoreDescription,
    )
    from Foundation import NSURL

    description = NSPersistentStoreDescription.persistentStoreDescriptionWithURL_(
        NSURL.fileURLWithPath_(str(store_path))
    )
    description.setShouldAddStoreAsynchronously_(False)
    description.setOption_forKey_(True, NSPersistentHistoryTrackingKey)
    container = NSPersistentContainer.alloc().initWithName_managedObjectModel_("Fixture", model)
    container.setPersistentStoreDescriptions_([description])
    result: dict[str, object] = {}

    def completion(_description, error) -> None:
        result["error"] = error

    container.loadPersistentStoresWithCompletionHandler_(completion)
    assert result.get("error") is None, f"fixture store failed to open: {result.get('error')}"
    return container


def _ensure_model_cache(store_path: Path, model) -> None:
    """Make the fixture store carry its model in Z_MODELCACHE like the real
    Voice Memos store does (raw-DEFLATE keyed archive)."""
    from Foundation import NSKeyedArchiver

    connection = sqlite3.connect(store_path)
    connection.execute("PRAGMA busy_timeout = 5000")
    try:
        connection.execute("CREATE TABLE IF NOT EXISTS Z_MODELCACHE (Z_CONTENT BLOB)")
        has_row = connection.execute("SELECT COUNT(*) FROM Z_MODELCACHE").fetchone()[0] > 0
        if not has_row:
            data, error = NSKeyedArchiver.archivedDataWithRootObject_requiringSecureCoding_error_(
                model, False, None
            )
            assert data is not None, f"model archive failed: {error}"
            compressor = zlib.compressobj(9, zlib.DEFLATED, -15)
            blob = compressor.compress(bytes(data)) + compressor.flush()
            connection.execute("INSERT INTO Z_MODELCACHE (Z_CONTENT) VALUES (?)", (blob,))
        connection.commit()
    finally:
        connection.close()


def _create_fixture_store(store_path: Path, rows: list[dict]) -> None:
    from CoreData import NSEntityDescription

    model = _make_model()
    container = _open_container(store_path, model)
    context = container.viewContext()
    for row in rows:
        recording = NSEntityDescription.insertNewObjectForEntityForName_inManagedObjectContext_(
            "CloudRecording", context
        )
        for key, value in row.items():
            recording.setValue_forKey_(value, key)
    ok, error = context.save_(None)
    assert ok, f"fixture save failed: {error}"
    _ensure_model_cache(store_path, model)


def _read_rows(store_path: Path) -> dict[str, tuple[str, str, int]]:
    connection = sqlite3.connect(f"file:{store_path}?mode=ro", uri=True)
    try:
        rows = connection.execute(
            "SELECT ZUNIQUEID, ZENCRYPTEDTITLE, ZCUSTOMLABELFORSORTING, ZFLAGS FROM ZCLOUDRECORDING"
        ).fetchall()
    finally:
        connection.close()
    return {row[0]: (row[1], row[2], row[3]) for row in rows}


FIXTURE_ROWS = [
    {
        "uniqueID": "AUTO-1",
        "path": "20260101 090000-AUTO0001.qta",
        "encryptedTitle": "New Recording 3",
        "customLabelForSorting": "New Recording 3",
        "customLabel": "2026-01-01T09:00:00Z",
        "flags": 4100,
    },
    {
        "uniqueID": "MANUAL-1",
        "path": "20260102 100000-MANU0001.qta",
        "encryptedTitle": "My handwritten title",
        "customLabelForSorting": "My handwritten title",
        "customLabel": "2026-01-02T10:00:00Z",
        "flags": 4,
    },
]


def test_apply_renames_updates_only_auto_named_rows(tmp_path) -> None:
    store_path = tmp_path / "CloudRecordings.db"
    _create_fixture_store(store_path, FIXTURE_ROWS)
    plan = [
        RenamePlanItem(
            unique_id="AUTO-1",
            recording_id="20260101 090000-AUTO0001",
            old_title="New Recording 3",
            new_title="Quarterly planning discussion",
        ),
        # A stale plan entry for a hand-renamed memo must be refused by the
        # writer's own re-check, not just by the planner.
        RenamePlanItem(
            unique_id="MANUAL-1",
            recording_id="20260102 100000-MANU0001",
            old_title="My handwritten title",
            new_title="Should never be applied",
        ),
        RenamePlanItem(
            unique_id="GONE-1",
            recording_id="20260103 110000-GONE0001",
            old_title="New Recording 4",
            new_title="Missing memo",
        ),
    ]

    results = apply_renames(store_path, plan)

    assert results == [
        {"unique_id": "AUTO-1", "status": STATUS_RENAMED},
        {"unique_id": "MANUAL-1", "status": STATUS_SKIPPED_NOT_AUTO_NAMED},
        {"unique_id": "GONE-1", "status": STATUS_SKIPPED_MISSING},
    ]
    rows = _read_rows(store_path)
    assert rows["AUTO-1"] == ("Quarterly planning discussion", "Quarterly planning discussion", 4)
    assert rows["MANUAL-1"] == ("My handwritten title", "My handwritten title", 4)


def test_apply_renames_records_persistent_history_with_author(tmp_path) -> None:
    store_path = tmp_path / "CloudRecordings.db"
    _create_fixture_store(store_path, FIXTURE_ROWS)

    apply_renames(
        store_path,
        [
            RenamePlanItem(
                unique_id="AUTO-1",
                recording_id="20260101 090000-AUTO0001",
                old_title="New Recording 3",
                new_title="Quarterly planning discussion",
            )
        ],
    )

    connection = sqlite3.connect(f"file:{store_path}?mode=ro", uri=True)
    try:
        authors = {
            row[0]
            for row in connection.execute("SELECT ZNAME FROM ATRANSACTIONSTRING").fetchall()
        }
    finally:
        connection.close()
    assert TRANSACTION_AUTHOR in authors


def test_apply_renames_dry_run_changes_nothing(tmp_path) -> None:
    store_path = tmp_path / "CloudRecordings.db"
    _create_fixture_store(store_path, FIXTURE_ROWS)

    results = apply_renames(
        store_path,
        [
            RenamePlanItem(
                unique_id="AUTO-1",
                recording_id="20260101 090000-AUTO0001",
                old_title="New Recording 3",
                new_title="Quarterly planning discussion",
            )
        ],
        dry_run=True,
    )

    assert results == [{"unique_id": "AUTO-1", "status": STATUS_WOULD_RENAME}]
    rows = _read_rows(store_path)
    assert rows["AUTO-1"] == ("New Recording 3", "New Recording 3", 4100)


def test_load_cached_model_data_requires_model_cache(tmp_path) -> None:
    store_path = tmp_path / "empty.db"
    sqlite3.connect(store_path).close()

    with pytest.raises((RuntimeError, sqlite3.OperationalError)):
        load_cached_model_data(store_path)
