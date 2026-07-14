"""Core Data writer for the Voice Memos CloudRecordings.db store.

Renames must be real Core Data saves, not raw SQL: the store belongs to
NSPersistentCloudKitContainer (mirrored by the voicememod daemon), and only a
save that records persistent history gets exported to CloudKit and synced to
the user's other devices. A direct sqlite UPDATE would stay local-only and be
clobbered by the next CloudKit import.

Two details make this safe against a store we do not own:

- The managed object model is loaded from the store's own Z_MODELCACHE table
  (the exact model the daemon last saved with), never from a bundled copy, so
  OS updates are tracked automatically.
- The store is opened with automatic migration disabled: if Apple ever ships
  an incompatible model, opening fails loudly instead of mutating the store.
"""

from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import Sequence
import zlib

from personal_data_warehouse_voice_memos.writeback import (
    AUTO_NAMED_FLAG,
    RenamePlanItem,
    TRANSACTION_AUTHOR,
    is_auto_named,
)

STATUS_RENAMED = "renamed"
STATUS_WOULD_RENAME = "would_rename"
STATUS_SKIPPED_NOT_AUTO_NAMED = "skipped_not_auto_named"
STATUS_SKIPPED_MISSING = "skipped_missing"


def load_cached_model_data(store_path: Path | str) -> bytes:
    """Read the archived managed object model cached inside the store.

    Core Data stores it as a raw-DEFLATE-compressed keyed archive in
    Z_MODELCACHE (Apple's COMPRESSION_ZLIB is raw deflate, no zlib header).
    """
    connection = sqlite3.connect(f"file:{Path(store_path)}?mode=ro", uri=True)
    try:
        row = connection.execute("SELECT Z_CONTENT FROM Z_MODELCACHE LIMIT 1").fetchone()
    finally:
        connection.close()
    if row is None or row[0] is None:
        raise RuntimeError(f"store has no cached managed object model: {store_path}")
    return zlib.decompress(row[0], -15)


def _load_cached_model(store_path: Path | str):
    from Foundation import NSData, NSKeyedUnarchiver

    plist = load_cached_model_data(store_path)
    data = NSData.dataWithBytes_length_(plist, len(plist))
    unarchiver, error = NSKeyedUnarchiver.alloc().initForReadingFromData_error_(data, None)
    if unarchiver is None:
        raise RuntimeError(f"could not decode cached model archive: {error}")
    unarchiver.setRequiresSecureCoding_(False)
    model = unarchiver.decodeObjectForKey_("root")
    if model is None:
        raise RuntimeError("cached model archive did not contain a managed object model")
    return model


def _open_store(store_path: Path | str, model):
    from CoreData import (
        NSPersistentContainer,
        NSPersistentHistoryTrackingKey,
        NSPersistentStoreDescription,
        NSPersistentStoreRemoteChangeNotificationPostOptionKey,
    )
    from Foundation import NSURL

    description = NSPersistentStoreDescription.persistentStoreDescriptionWithURL_(
        NSURL.fileURLWithPath_(str(store_path))
    )
    # Never migrate someone else's store: an incompatible model must fail
    # loudly, not rewrite the store's schema metadata.
    description.setShouldMigrateStoreAutomatically_(False)
    description.setShouldInferMappingModelAutomatically_(False)
    description.setShouldAddStoreAsynchronously_(False)
    # Record persistent history for our save (what CloudKit mirroring
    # exports) and post the cross-process change notification the daemon and
    # app listen for.
    description.setOption_forKey_(True, NSPersistentHistoryTrackingKey)
    description.setOption_forKey_(True, NSPersistentStoreRemoteChangeNotificationPostOptionKey)

    container = NSPersistentContainer.alloc().initWithName_managedObjectModel_("VoiceMemos", model)
    container.setPersistentStoreDescriptions_([description])
    load_result: dict[str, object] = {}

    def completion(_description, error) -> None:
        load_result["error"] = error

    container.loadPersistentStoresWithCompletionHandler_(completion)
    error = load_result.get("error")
    if error is not None:
        raise RuntimeError(f"could not open Voice Memos store: {error}")
    return container


def apply_renames(
    store_path: Path | str,
    items: Sequence[RenamePlanItem],
    *,
    author: str = TRANSACTION_AUTHOR,
    dry_run: bool = False,
) -> list[dict[str, str]]:
    """Apply planned renames through a single Core Data save.

    Every item is re-checked against the live row before mutating (the memo
    could have been hand-renamed on another device between planning and
    writing); anything no longer auto-named is skipped, never overwritten.
    """
    try:
        from CoreData import NSFetchRequest, NSMergeByPropertyObjectTrumpMergePolicy
        from Foundation import NSPredicate
    except ImportError as exc:  # pragma: no cover - exercised only off-macOS
        raise RuntimeError(
            "Voice Memos write-back requires pyobjc-framework-CoreData (macOS only)"
        ) from exc

    model = _load_cached_model(store_path)
    container = _open_store(store_path, model)
    context = container.viewContext()
    context.setTransactionAuthor_(author)
    context.setName_("pdw-voice-memos-writeback")
    context.setMergePolicy_(NSMergeByPropertyObjectTrumpMergePolicy)

    results: list[dict[str, str]] = []
    changed = False
    for item in items:
        request = NSFetchRequest.fetchRequestWithEntityName_("CloudRecording")
        request.setPredicate_(
            NSPredicate.predicateWithFormat_argumentArray_("uniqueID == %@", [item.unique_id])
        )
        matches, error = context.executeFetchRequest_error_(request, None)
        if matches is None:
            raise RuntimeError(f"fetch failed for {item.unique_id}: {error}")
        if len(matches) == 0:
            results.append({"unique_id": item.unique_id, "status": STATUS_SKIPPED_MISSING})
            continue
        recording = matches[0]
        current_title = str(recording.valueForKey_("encryptedTitle") or "")
        current_flags = int(recording.valueForKey_("flags") or 0)
        if not is_auto_named(current_title, current_flags):
            results.append({"unique_id": item.unique_id, "status": STATUS_SKIPPED_NOT_AUTO_NAMED})
            continue
        if dry_run:
            results.append({"unique_id": item.unique_id, "status": STATUS_WOULD_RENAME})
            continue
        recording.setValue_forKey_(item.new_title, "encryptedTitle")
        recording.setValue_forKey_(item.new_title, "customLabelForSorting")
        recording.setValue_forKey_(current_flags & ~AUTO_NAMED_FLAG, "flags")
        results.append({"unique_id": item.unique_id, "status": STATUS_RENAMED})
        changed = True

    if changed:
        ok, error = context.save_(None)
        if not ok:
            raise RuntimeError(f"Voice Memos store save failed: {error}")
    return results
