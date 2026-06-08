from __future__ import annotations

from datetime import UTC, datetime
import json

from personal_data_warehouse.apple_voice_memos_drive_ingest import (
    VoiceMemosDriveIngestRunner,
    attach_storage_context,
    clean_metadata_payload,
    has_metadata_payloads,
    iter_metadata_payloads,
    library_metadata_payload,
    metadata_to_row,
)
from personal_data_warehouse.objectstore import ObjectListing


class FakeLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


class FakeWarehouse:
    def __init__(self) -> None:
        self.ensure_apple_voice_memos_tables_called = False
        self.rows: list[dict[str, object]] = []

    def ensure_apple_voice_memos_tables(self) -> None:
        self.ensure_apple_voice_memos_tables_called = True

    def insert_apple_voice_memos_files(self, rows) -> None:
        self.rows.extend(rows)


class FakeObjectStore:
    """Minimal ObjectStore for ingest tests (no Google Drive)."""

    backend = "google_drive"

    def __init__(self, *, metadata_listings=None, audio_listing=None, json_by_id=None) -> None:
        self.metadata_listings = list(metadata_listings or [])
        self.audio_listing = audio_listing
        self.json_by_id = dict(json_by_id or {})
        self.moved: list[tuple[str, str]] = []

    def list_objects(self, *, kind, stage=None, properties=None):
        return list(self.metadata_listings) if kind == "voice_memo_metadata" else []

    def find_object(self, *, kind, stage=None, properties=None):
        if kind == "voice_memo_metadata":
            return self.metadata_listings[0] if self.metadata_listings else None
        if kind == "voice_memo_audio":
            return self.audio_listing
        return None

    def get_object(self, ref):
        return json.dumps(self.json_by_id[str(ref["storage_file_id"])]).encode("utf-8")

    def move_object(self, ref, *, new_object_key, app_properties=None):
        self.moved.append((str(ref.get("storage_file_id", "")), new_object_key))
        return {
            "storage_backend": self.backend,
            "storage_key": new_object_key,
            "storage_file_id": str(ref.get("storage_file_id", "")),
            "storage_url": "https://drive/promoted",
        }


def metadata_payload() -> dict[str, object]:
    return {
        "schema_version": 1,
        "source": "apple_voice_memos",
        "account": "zach@example.com",
        "uploaded_at": "2026-04-27T12:00:00+00:00",
        "recording": {
            "recording_id": "20260325 145019-DAAC9394",
            "title": "The Flying Pig Bookstore 6",
            "original_path": "/Users/zrl/VoiceMemos/20260325 145019-DAAC9394.qta",
            "filename": "20260325 145019-DAAC9394.qta",
            "extension": ".qta",
            "content_type": "audio/quicktime",
            "size_bytes": 123,
            "content_sha256": "abc123",
            "file_created_at": "2026-03-25T18:50:19+00:00",
            "file_modified_at": "2026-03-25T19:05:00+00:00",
            "recorded_at": "2026-03-25T18:50:19+00:00",
        },
    }


def decorated_metadata_payload() -> dict[str, object]:
    payload = metadata_payload()
    payload.update(
        {
            "audio_file": {
                "storage_backend": "google_drive",
                "storage_key": "apple-voice-memos/inbox/2026/03/2026-03-25-abc123.qta",
                "storage_file_id": "drive-file-id",
                "storage_url": "https://drive/audio",
            },
            "metadata_file": {
                "storage_backend": "google_drive",
                "storage_key": "apple-voice-memos/inbox/2026/03/2026-03-25-abc123.json",
                "storage_file_id": "drive-metadata-file-id",
                "storage_url": "https://drive/metadata",
                "content_sha256": "metadata-sha",
            },
        }
    )
    return payload


def metadata_listing() -> ObjectListing:
    return ObjectListing(
        ref={
            "storage_backend": "google_drive",
            "storage_key": "",
            "storage_file_id": "drive-metadata-file-id",
            "storage_url": "https://drive/metadata",
        },
        app_properties={"content_sha256": "metadata-sha"},
        filename="2026-03-25-abc123.json",
    )


def audio_listing() -> ObjectListing:
    return ObjectListing(
        ref={
            "storage_backend": "google_drive",
            "storage_key": "",
            "storage_file_id": "drive-file-id",
            "storage_url": "https://drive/audio",
        },
        app_properties={"content_sha256": "abc123"},
        filename="2026-03-25-abc123.qta",
    )


def test_attach_storage_context_derives_keys_from_listings() -> None:
    payload = attach_storage_context(
        metadata_payload(),
        stage="inbox",
        metadata_listing=metadata_listing(),
        audio_listing=audio_listing(),
    )

    assert payload["audio_file"]["storage_key"] == "apple-voice-memos/inbox/2026/03/2026-03-25-abc123.qta"
    assert payload["audio_file"]["storage_file_id"] == "drive-file-id"
    assert payload["metadata_file"]["storage_key"] == "apple-voice-memos/inbox/2026/03/2026-03-25-abc123.json"
    assert payload["metadata_file"]["content_sha256"] == "metadata-sha"
    assert "audio_file" not in clean_metadata_payload(payload)
    assert "metadata_file" not in clean_metadata_payload(payload)


def test_metadata_to_row_maps_metadata_to_row() -> None:
    row = metadata_to_row(decorated_metadata_payload(), ingested_at=datetime(2026, 4, 27, 13, tzinfo=UTC))

    assert row["account"] == "zach@example.com"
    assert row["recording_id"] == "20260325 145019-DAAC9394"
    assert row["content_sha256"] == "abc123"
    assert row["storage_backend"] == "google_drive"
    assert row["storage_file_id"] == "drive-file-id"
    assert row["metadata_storage_key"] == "apple-voice-memos/inbox/2026/03/2026-03-25-abc123.json"
    assert row["metadata_storage_file_id"] == "drive-metadata-file-id"
    assert row["recorded_at"] == datetime(2026, 3, 25, 18, 50, 19, tzinfo=UTC)
    assert "audio_file" not in row["raw_metadata_json"]


def test_library_metadata_payload_rewrites_inbox_storage_keys() -> None:
    payload = library_metadata_payload(decorated_metadata_payload())

    assert payload["audio_file"]["storage_key"] == "apple-voice-memos/library/2026/03/2026-03-25-abc123.qta"
    assert payload["metadata_file"]["storage_key"] == "apple-voice-memos/library/2026/03/2026-03-25-abc123.json"
    assert payload["metadata_file"]["content_sha256"] == "metadata-sha"


def test_iter_metadata_payloads_reads_through_object_store() -> None:
    store = FakeObjectStore(
        metadata_listings=[metadata_listing()],
        audio_listing=audio_listing(),
        json_by_id={"drive-metadata-file-id": metadata_payload()},
    )

    payloads = list(iter_metadata_payloads(object_store=store))

    assert len(payloads) == 1
    payload = payloads[0]
    assert payload["audio_file"]["storage_key"] == "apple-voice-memos/inbox/2026/03/2026-03-25-abc123.qta"
    assert payload["audio_file"]["storage_file_id"] == "drive-file-id"
    assert payload["metadata_file"]["storage_key"] == "apple-voice-memos/inbox/2026/03/2026-03-25-abc123.json"
    assert payload["metadata_file"]["content_sha256"] == "metadata-sha"


def test_has_metadata_payloads_uses_find_object() -> None:
    assert has_metadata_payloads(object_store=FakeObjectStore(metadata_listings=[metadata_listing()])) is True
    assert has_metadata_payloads(object_store=FakeObjectStore()) is False


def test_drive_ingest_runner_writes_rows_and_promotes_via_object_store() -> None:
    warehouse = FakeWarehouse()
    store = FakeObjectStore()

    summary = VoiceMemosDriveIngestRunner(
        warehouse=warehouse,
        metadata_source=lambda: [decorated_metadata_payload()],
        object_store=store,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 4, 27, 13, tzinfo=UTC),
    ).sync()

    assert warehouse.ensure_apple_voice_memos_tables_called
    assert summary.metadata_seen == 1
    assert summary.rows_written == 1
    assert summary.recordings_promoted == 1
    # Rows record post-promotion (library) keys.
    assert warehouse.rows[0]["storage_key"] == "apple-voice-memos/library/2026/03/2026-03-25-abc123.qta"
    # Both audio + metadata blobs moved to their library keys.
    assert store.moved == [
        ("drive-file-id", "apple-voice-memos/library/2026/03/2026-03-25-abc123.qta"),
        ("drive-metadata-file-id", "apple-voice-memos/library/2026/03/2026-03-25-abc123.json"),
    ]


def test_drive_ingest_runner_without_object_store_keeps_inbox_keys() -> None:
    warehouse = FakeWarehouse()

    summary = VoiceMemosDriveIngestRunner(
        warehouse=warehouse,
        metadata_source=lambda: [decorated_metadata_payload()],
        logger=FakeLogger(),
        now=lambda: datetime(2026, 4, 27, 13, tzinfo=UTC),
    ).sync()

    assert summary.recordings_promoted == 0
    assert warehouse.rows[0]["storage_key"] == "apple-voice-memos/inbox/2026/03/2026-03-25-abc123.qta"
