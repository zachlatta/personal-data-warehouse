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
    def __init__(self, existing_ids: dict[tuple[str, str], list[str]] | None = None) -> None:
        self.ensure_apple_voice_memos_tables_called = False
        self.rows: list[dict[str, object]] = []
        self.existing_ids = dict(existing_ids or {})

    def load_apple_voice_memo_recording_ids_by_sha(self, content_sha256s):
        return {key: ids for key, ids in self.existing_ids.items() if key[1] in set(content_sha256s)}

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


class LibraryAudioObjectStore(FakeObjectStore):
    """The audio was promoted to the library by an earlier sidecar."""

    def find_object(self, *, kind, stage=None, properties=None):
        if kind == "voice_memo_audio":
            return self.audio_listing if stage == "library" else None
        return super().find_object(kind=kind, stage=stage, properties=properties)


def test_a_corrected_sidecar_finds_audio_already_in_the_library() -> None:
    """A sidecar correcting recorded_at arrives after its audio was promoted.

    On 2026-10-16 the uploader started reading recorded_at from the Voice Memos
    store's ZDATE instead of the filename stamp read as UTC, and re-sent only
    the sidecar of every landed memo. Its audio is in the library, not the
    inbox; failing to find it there would fail the whole ingest run.
    """
    corrected = metadata_payload()
    corrected["recording"]["recorded_at"] = "2026-03-25T22:50:19+00:00"
    store = LibraryAudioObjectStore(
        metadata_listings=[metadata_listing()],
        audio_listing=audio_listing(),
        json_by_id={"drive-metadata-file-id": corrected},
    )
    warehouse = FakeWarehouse()

    summary = VoiceMemosDriveIngestRunner(
        warehouse=warehouse,
        metadata_source=lambda: iter_metadata_payloads(object_store=store),
        object_store=store,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 10, 16, 13, tzinfo=UTC),
    ).sync()

    assert summary.rows_written == 1
    assert warehouse.rows[0]["recorded_at"] == datetime(2026, 3, 25, 22, 50, 19, tzinfo=UTC)
    assert warehouse.rows[0]["storage_file_id"] == "drive-file-id"
    assert ("drive-file-id", "apple-voice-memos/library/2026/03/2026-03-25-abc123.qta") in store.moved


def test_a_renamed_memos_sidecar_updates_the_recording_the_warehouse_already_holds() -> None:
    """Voice Memos sometimes renames a file, moving its stamp by hours.

    Until 2026-10-16 the app deduped a renamed memo's sidecar onto the old one
    by audio hash, so the rename never reached the warehouse. Deduping on
    (audio hash, recorded_at) let three renamed memos' corrected sidecars land
    as NEW recordings, transcribed again and queued for enrichment beside the
    originals. The audio is the identity: a sidecar for audio the warehouse
    already holds updates that recording.
    """
    renamed = decorated_metadata_payload()
    renamed["recording"]["recording_id"] = "20260325 155019-DAAC9394"
    warehouse = FakeWarehouse(existing_ids={("zach@example.com", "abc123"): ["20260325 145019-DAAC9394"]})

    VoiceMemosDriveIngestRunner(
        warehouse=warehouse,
        metadata_source=lambda: [renamed],
        logger=FakeLogger(),
        now=lambda: datetime(2026, 10, 16, 13, tzinfo=UTC),
    ).sync()

    assert warehouse.rows[0]["recording_id"] == "20260325 145019-DAAC9394"


def test_a_sidecar_naming_one_of_its_audios_recordings_keeps_its_own_id() -> None:
    warehouse = FakeWarehouse(
        existing_ids={("zach@example.com", "abc123"): ["20260325 145019", "20260325 145019-DAAC9394"]}
    )
    VoiceMemosDriveIngestRunner(
        warehouse=warehouse,
        metadata_source=lambda: [decorated_metadata_payload()],
        logger=FakeLogger(),
        now=lambda: datetime(2026, 10, 16, 13, tzinfo=UTC),
    ).sync()
    assert warehouse.rows[0]["recording_id"] == "20260325 145019-DAAC9394"


def test_empty_audio_is_never_an_identity() -> None:
    # Every zero-byte file has the same hash; two empty memos are not one memo.
    empty = decorated_metadata_payload()
    empty["recording"]["size_bytes"] = 0
    warehouse = FakeWarehouse(existing_ids={("zach@example.com", "abc123"): ["some-other-memo"]})
    VoiceMemosDriveIngestRunner(
        warehouse=warehouse,
        metadata_source=lambda: [empty],
        logger=FakeLogger(),
        now=lambda: datetime(2026, 10, 16, 13, tzinfo=UTC),
    ).sync()
    assert warehouse.rows[0]["recording_id"] == "20260325 145019-DAAC9394"
