from __future__ import annotations

import json

from personal_data_warehouse.alice_voice_recordings_drive_ingest import (
    AliceVoiceRecordingsDriveIngestRunner,
    iter_archive_payloads,
)
from personal_data_warehouse.objectstore import ObjectListing
from personal_data_warehouse.postgres import POSTGRES_TABLES
from personal_data_warehouse.relations import relation
from personal_data_warehouse.schema import (
    ALICE_VOICE_RECORDING_ARTIFACT_COLUMNS,
    ALICE_VOICE_RECORDING_COLUMNS,
)


class FakeLogger:
    def info(self, *_args, **_kwargs) -> None:
        pass


class FakeWarehouse:
    def __init__(self) -> None:
        self.ensured = False
        self.recordings: list[dict] = []
        self.artifacts: list[dict] = []

    def ensure_alice_voice_recordings_tables(self) -> None:
        self.ensured = True

    def insert_alice_voice_recordings(self, rows) -> None:
        self.recordings.extend(rows)

    def insert_alice_voice_recording_artifacts(self, rows) -> None:
        self.artifacts.extend(rows)


class FakeObjectStore:
    backend = "google_drive"

    def __init__(self, listings_by_kind: dict[str, list[ObjectListing]], bodies: dict[str, dict]) -> None:
        self._listings_by_kind = listings_by_kind
        self._bodies = bodies

    def list_objects(self, *, kind: str, stage: str | None = None, properties=None):
        assert stage == "library"
        return self._listings_by_kind.get(kind, [])

    def get_object(self, ref) -> bytes:
        return json.dumps(self._bodies[ref["storage_file_id"]]).encode()


def listing(file_id: str, filename: str, **properties: str) -> ObjectListing:
    return ObjectListing(
        ref={
            "storage_backend": "google_drive",
            "storage_key": f"alice/{filename}",
            "storage_file_id": file_id,
            "storage_url": f"https://drive/{file_id}",
        },
        app_properties={"content_sha256": f"sha-{file_id}", **properties},
        filename=filename,
    )


def test_alice_archive_tables_are_source_owned_and_registered() -> None:
    recordings = relation("alice_voice_recordings")
    assert (recordings.schema, recordings.name) == ("alice_voice_recordings", "recordings")
    assert POSTGRES_TABLES["alice_voice_recordings"].columns == ALICE_VOICE_RECORDING_COLUMNS
    assert POSTGRES_TABLES["alice_voice_recordings"].primary_key == ("account", "recording_id")
    assert (
        POSTGRES_TABLES["alice_voice_recording_artifacts"].columns
        == ALICE_VOICE_RECORDING_ARTIFACT_COLUMNS
    )
    assert POSTGRES_TABLES["alice_voice_recording_artifacts"].primary_key == (
        "account",
        "recording_id",
        "artifact_id",
    )


def test_archive_reader_materializes_api_and_recovery_artifacts() -> None:
    store = FakeObjectStore(
        {
            "voice_recording_metadata": [listing("meta-api", "recording.json")],
            "voice_recording_email_metadata": [listing("meta-email", "email.json")],
            "voice_recording_audio": [
                listing("audio-1", "recording.m4a", alice_upload_id="rec-1")
            ],
            "voice_recording_transcript": [
                listing("transcript-1", "recording.txt", alice_recording_guid="rec-1")
            ],
        },
        {
            "meta-api": {
                "account": "owner@example.test",
                "uploaded_at": "2026-07-19T12:30:00Z",
                "recording": {
                    "recording_id": "rec-1",
                    "title": "Planning walk",
                    "filename": "recording.m4a",
                    "content_type": "audio/mp4",
                    "size_bytes": 123,
                    "recorded_at": "2026-07-18T14:00:00Z",
                },
                "alice": {"duration_in_secs": 61.8},
            },
            "meta-email": {
                "account": "recovery-mailbox@example.test",
                "uploaded_at": "2026-07-20T01:00:00Z",
                "recovery_source": "gmail_transcript_email",
                "recording": {"recording_id": "rec-1", "title": ""},
            },
        },
    )
    warehouse = FakeWarehouse()

    summary = AliceVoiceRecordingsDriveIngestRunner(
        warehouse=warehouse,
        metadata_source=lambda: iter_archive_payloads(object_store=store),
        logger=FakeLogger(),
    ).sync()

    assert warehouse.ensured is True
    assert summary.metadata_seen == 2
    assert summary.recordings_written == 1
    assert summary.artifacts_written == 2
    assert warehouse.recordings[0]["recording_id"] == "rec-1"
    assert warehouse.recordings[0]["title"] == "Planning walk"
    assert warehouse.recordings[0]["duration_seconds"] == 61
    assert warehouse.recordings[0]["storage_file_id"] == "audio-1"
    assert warehouse.recordings[0]["metadata_storage_file_id"] == "meta-api"
    assert warehouse.recordings[0]["recovery_source"] == "gmail_transcript_email"
    assert {row["kind"] for row in warehouse.artifacts} == {
        "voice_recording_audio",
        "voice_recording_transcript",
    }


def test_archive_reader_rejects_metadata_without_a_stable_identity() -> None:
    warehouse = FakeWarehouse()
    runner = AliceVoiceRecordingsDriveIngestRunner(
        warehouse=warehouse,
        metadata_source=lambda: [{"account": "owner@example.test", "recording": {}}],
        logger=FakeLogger(),
    )

    try:
        runner.sync()
    except ValueError as error:
        assert "recording.recording_id" in str(error)
    else:
        raise AssertionError("metadata without a recording id must fail loudly")
