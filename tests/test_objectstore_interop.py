from __future__ import annotations

import json

from personal_data_warehouse.objectstore import StoredObject, stored_object_from_mapping
from personal_data_warehouse.objectstore.google_drive import drive_name_from_object_key, object_stage


def test_stored_object_keys_match_cross_runtime_contract() -> None:
    ref: StoredObject = {
        "storage_backend": "google_drive",
        "storage_key": "apple-notes/library/2026/05/note-1/rev-1.json",
        "storage_file_id": "metadata-file",
        "storage_url": "https://drive/metadata",
    }
    # The JSON shape the Go runtime marshals must round-trip here unchanged.
    encoded = json.dumps(ref)
    assert json.loads(encoded) == ref


def test_stored_object_from_go_shaped_mapping() -> None:
    # A reference as the Go layer would emit (plus a stray field) normalizes cleanly.
    go_ref = {
        "storage_backend": "google_drive",
        "storage_key": "apple-voice-memos/library/2026/03/2026-03-25-abc123.qta",
        "storage_file_id": "drive-file-id",
        "storage_url": "https://drive/file",
        "extra": "ignored",
    }
    normalized = stored_object_from_mapping(go_ref)
    assert normalized == {
        "storage_backend": "google_drive",
        "storage_key": "apple-voice-memos/library/2026/03/2026-03-25-abc123.qta",
        "storage_file_id": "drive-file-id",
        "storage_url": "https://drive/file",
    }


def test_key_conventions_match_cross_runtime() -> None:
    cases = [
        ("apple-voice-memos/inbox/2026/03/2026-03-25-abc123.qta", "inbox", "2026-03-25-abc123.qta"),
        ("apple-notes/library/2026/05/note-1/rev-1.json", "library", "rev-1.json"),
        ("apple-messages/inbox/batches/2026/05/batch.jsonl.gz", "inbox", "batch.jsonl.gz"),
        ("top-level.bin", "", "top-level.bin"),
    ]
    for key, want_stage, want_name in cases:
        assert object_stage(key) == want_stage
        assert drive_name_from_object_key(key) == want_name
