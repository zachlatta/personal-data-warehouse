from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import requests

from personal_data_warehouse_alice_voice_recordings.sync import (
    SOURCE,
    AliceVoiceRecordingsImportRunner,
    build_metadata,
    download_recording,
    media_url_from_recording,
    recording_id_from_upload_request,
)
from personal_data_warehouse.objectstore import ObjectPresence


class FakeLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


class FakeResponse:
    def __init__(self, body: bytes, *, headers: dict[str, str] | None = None, status_code: int = 200) -> None:
        self._body = body
        self.headers = headers or {}
        self.content = body
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error", response=self)

    def iter_content(self, chunk_size: int):
        yield self._body


class FakeSession:
    def __init__(
        self,
        body: bytes = b"alice audio",
        *,
        content_type: str = "audio/mpeg",
        status_code: int = 200,
    ) -> None:
        self.body = body
        self.content_type = content_type
        self.status_code = status_code
        self.urls: list[str] = []

    def get(self, url: str, **kwargs):
        self.urls.append(url)
        return FakeResponse(
            self.body,
            headers={
                "Content-Type": self.content_type,
                "Content-Disposition": 'attachment; filename="Alice Recording.mp3"',
            },
            status_code=self.status_code,
        )


class FakeObjectStore:
    backend = "google_drive"

    def __init__(self) -> None:
        self.files: list[dict[str, object]] = []
        self.json_files: list[dict[str, object]] = []
        self.existing_objects: set[tuple[str, str, str]] = set()

    def presence(self, *, content_sha256: str) -> ObjectPresence:
        return ObjectPresence(audio_exists=False, metadata_exists=False)

    def has_blob(self, *, content_sha256: str) -> bool:
        return False

    def has_metadata(self, *, content_sha256: str) -> bool:
        return False

    def has_object(self, *, kind: str, key: str, value: str) -> bool:
        return (kind, key, value) in self.existing_objects

    def put_file(self, **kwargs):
        self.files.append(kwargs)
        return {
            "storage_backend": "google_drive",
            "storage_key": kwargs["object_key"],
            "storage_file_id": "audio-id",
            "storage_url": "https://drive/audio-id",
        }

    def put_json(self, **kwargs):
        self.json_files.append(kwargs)
        return {
            "storage_backend": "google_drive",
            "storage_key": kwargs["object_key"],
            "storage_file_id": "metadata-id",
            "storage_url": "https://drive/metadata-id",
        }


def upload_request() -> dict[str, object]:
    return {
        "id": "alice-upload-1",
        "status": "completed",
        "message": "Alice field recording",
        "recording_transcription_status": ["transcript_ready", "Transcript is ready"],
        "recording_url": "https://aliceapp.ai/recordings/alice-upload-1/download",
        "created_at": "2026-05-12T02:00:00Z",
        "updated_at": "2026-05-12T02:05:00Z",
    }


def api_recording() -> dict[str, object]:
    return {
        "id": 264444,
        "guid": "alice-guid-1",
        "title": "Challenger",
        "duration_in_secs": 3461,
        "media_file_name": "Challenger.mp3",
        "mp3_download_url": "https://aliceapp.ai/recordings/alice-guid-1/download_media_file",
        "_pdw_recording_page_url": "https://aliceapp.ai/recordings/alice-guid-1",
        "is_transcript_ready?": True,
        "is_media_file_ready?": True,
        "created_at": "2026-04-27T15:53:37.391Z",
        "updated_at": "2026-04-27T15:55:17.763Z",
    }


def test_alice_archive_source_uses_canonical_schema_source_name() -> None:
    assert SOURCE == "alice_voice_recordings"


def test_alice_api_recording_shape_maps_to_archive_identity() -> None:
    assert recording_id_from_upload_request(api_recording(), recording_url="") == "alice-guid-1"
    assert media_url_from_recording(api_recording()).endswith("/download_media_file")


def test_downloaded_alice_recording_uses_api_metadata_and_audio_hash(tmp_path: Path) -> None:
    recording = download_recording(
        upload_request=upload_request(),
        destination=tmp_path / "recording",
        session=FakeSession(body=b"audio bytes"),
    )

    assert recording.recording_id == "alice-upload-1"
    assert recording.title == "Alice field recording"
    assert recording.filename == "Alice Recording.mp3"
    assert recording.extension == ".mp3"
    assert recording.content_type == "audio/mpeg"
    assert recording.size_bytes == len(b"audio bytes")
    assert recording.recorded_at == datetime(2026, 5, 12, 2, tzinfo=UTC)
    assert recording.path.read_bytes() == b"audio bytes"

    metadata = build_metadata(
        account="zach@example.com",
        recording=recording,
        uploaded_at=datetime(2026, 5, 12, 3, tzinfo=UTC),
    )
    assert metadata["source"] == SOURCE
    assert metadata["recording"]["recording_id"] == "alice-upload-1"
    assert metadata["alice"]["raw_upload_request"]["id"] == "alice-upload-1"


def test_alice_import_writes_voice_memos_style_audio_and_json_sidecar() -> None:
    store = FakeObjectStore()

    summary = AliceVoiceRecordingsImportRunner(
        account="zach@example.com",
        upload_requests=[upload_request()],
        object_store=store,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 12, 3, tzinfo=UTC),
        session=FakeSession(body=b"audio bytes"),
    ).sync()

    assert summary.upload_requests_seen == 1
    assert summary.recordings_uploaded == 1
    assert summary.metadata_uploaded == 1
    audio_key = store.files[0]["object_key"]
    metadata_key = store.json_files[0]["object_key"]
    assert str(audio_key).startswith("alice-voice-recordings/library/2026/05/2026-05-12-")
    assert str(audio_key).endswith(".mp3")
    assert str(metadata_key).startswith("alice-voice-recordings/library/2026/05/2026-05-12-")
    assert str(metadata_key).endswith(".json")
    assert store.files[0]["app_properties"] == {"alice_upload_id": "alice-upload-1"}
    assert store.json_files[0]["source_content_sha256"] == store.files[0]["content_sha256"]


def test_alice_incremental_import_skips_recording_ids_that_already_have_metadata() -> None:
    store = FakeObjectStore()
    store.existing_objects.add(("voice_recording_metadata", "alice_upload_id", "alice-guid-1"))
    session = FakeSession(body=b"audio bytes")

    summary = AliceVoiceRecordingsImportRunner(
        account="zach@example.com",
        upload_requests=[api_recording()],
        object_store=store,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 12, 3, tzinfo=UTC),
        session=session,
        mode="incremental",
    ).sync()

    assert summary.recordings_skipped == 1
    assert store.files == []
    assert store.json_files == []
    assert session.urls == []


def test_alice_import_preserves_metadata_when_audio_url_is_missing() -> None:
    store = FakeObjectStore()
    request = upload_request()
    request["recording_url"] = ""

    summary = AliceVoiceRecordingsImportRunner(
        account="zach@example.com",
        upload_requests=[request],
        object_store=store,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 12, 3, tzinfo=UTC),
        session=FakeSession(body=b"audio bytes"),
    ).sync()

    assert summary.upload_requests_seen == 1
    assert summary.recordings_uploaded == 0
    assert summary.metadata_uploaded == 1
    assert store.files == []
    metadata_key = store.json_files[0]["object_key"]
    assert metadata_key == "alice-voice-recordings/library/2026/05/2026-05-12-alice-alice-upload-1.json"
    assert store.json_files[0]["source_content_sha256"] is None


def test_alice_import_uses_recordings_history_shape() -> None:
    store = FakeObjectStore()

    summary = AliceVoiceRecordingsImportRunner(
        account="zach@example.com",
        upload_requests=[api_recording()],
        object_store=store,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 12, 3, tzinfo=UTC),
        session=FakeSession(body=b"audio bytes"),
    ).sync()

    assert summary.recordings_uploaded == 1
    payload = store.json_files[0]["payload"]
    assert payload["recording"]["recording_id"] == "alice-guid-1"
    assert payload["recording"]["title"] == "Challenger"
    assert payload["alice"]["duration_in_secs"] == 3461
    assert payload["alice"]["transcript_ready"] is True


def test_alice_import_preserves_metadata_when_media_download_404s() -> None:
    store = FakeObjectStore()

    summary = AliceVoiceRecordingsImportRunner(
        account="zach@example.com",
        upload_requests=[api_recording()],
        object_store=store,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 12, 3, tzinfo=UTC),
        session=FakeSession(body=b"missing", status_code=404),
    ).sync()

    assert summary.recordings_uploaded == 0
    assert summary.metadata_uploaded == 1
    assert store.files == []
    payload = store.json_files[0]["payload"]
    assert payload["recording"]["recording_id"] == "alice-guid-1"
    assert payload["alice"]["media_download_error"].startswith("HTTP 404")
    assert "page download failed" in payload["alice"]["media_download_error"]


def test_alice_import_archives_recording_page_html_when_media_is_unavailable() -> None:
    store = FakeObjectStore()
    html = b"<html><body><p>Transcript text</p></body></html>"

    summary = AliceVoiceRecordingsImportRunner(
        account="zach@example.com",
        upload_requests=[upload_request()],
        object_store=store,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 12, 3, tzinfo=UTC),
        session=FakeSession(body=html, content_type="text/html"),
    ).sync()

    assert summary.recordings_uploaded == 0
    assert summary.metadata_uploaded == 1
    assert len(store.files) == 1
    assert store.files[0]["kind"] == "voice_recording_page_html"
    assert str(store.files[0]["object_key"]).endswith(".html")
