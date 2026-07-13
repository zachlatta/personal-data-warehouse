"""Client http_app wiring tests.

Object keys / kinds / pdw_* tags are built only by the app now (Go); their
byte-for-byte contract is asserted in app/internal/server/ingest_test.go and
exercised end-to-end (including via the real Python client) in
app/internal/server/ingest_e2e_test.go. These tests cover the Python client
runners' wiring to the ingest SDK.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pathlib import Path

from personal_data_warehouse_voice_memos.sync import VoiceMemosUploadRunner


# --- client http_app wiring -------------------------------------------------


class _CaptureLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


class _FakeIngestClient:
    def __init__(self) -> None:
        self.audio: list[dict] = []
        self.metadata: list[dict] = []

    def upload_voice_memo_audio(self, content, *, recorded_at, extension, content_type):
        self.audio.append(
            {"content": content, "recorded_at": recorded_at, "extension": extension, "content_type": content_type}
        )
        return {"storage_backend": "google_drive", "storage_key": "k", "storage_file_id": "fid", "storage_url": ""}

    def upload_voice_memo_metadata(self, payload, *, recorded_at, audio_content_sha256):
        self.metadata.append({"payload": payload, "recorded_at": recorded_at, "audio_content_sha256": audio_content_sha256})
        return {"storage_backend": "google_drive", "storage_key": "k", "storage_file_id": "fid", "storage_url": ""}


def test_voice_memos_runner_uploads_via_ingest_client(tmp_path: Path) -> None:
    fresh = tmp_path / "20260325 145019-DAAC9394.qta"
    fresh.write_bytes(b"new-recording")
    client = _FakeIngestClient()

    summary = VoiceMemosUploadRunner(
        account="zach@example.com",
        recordings_path=tmp_path,
        extensions=(".qta",),
        ingest_client=client,
        logger=_CaptureLogger(),
        now=lambda: datetime(2026, 4, 27, 12, tzinfo=UTC),
    ).sync()

    assert summary.recordings_uploaded == 1
    assert len(client.audio) == 1 and len(client.metadata) == 1
    audio = client.audio[0]
    assert audio["content"] == b"new-recording"
    assert audio["extension"] == ".qta"
    assert audio["recorded_at"].startswith("2026-03-25")
    # The sidecar is keyed server-side by the audio's content sha (stable).
    meta = client.metadata[0]
    assert meta["audio_content_sha256"] == audio_sha(b"new-recording")
    assert meta["payload"]["schema_version"] == 1


def audio_sha(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


class _FakePhotoIngestClient:
    def __init__(self) -> None:
        self.files: list[dict] = []
        self.metadata: list[dict] = []

    def upload_photo_file(self, content, *, captured_at, extension, content_type):
        self.files.append(
            {"content": content, "captured_at": captured_at, "extension": extension, "content_type": content_type}
        )
        return {"storage_backend": "google_drive", "storage_key": "k", "storage_file_id": "fid", "storage_url": ""}

    def upload_photo_metadata(self, payload, *, captured_at, file_content_sha256, metadata_dedup_sha256):
        self.metadata.append(
            {
                "payload": payload,
                "captured_at": captured_at,
                "file_content_sha256": file_content_sha256,
                "metadata_dedup_sha256": metadata_dedup_sha256,
            }
        )
        return {"storage_backend": "google_drive", "storage_key": "k", "storage_file_id": "fid", "storage_url": ""}


def test_photos_runner_uploads_via_ingest_client(tmp_path: Path) -> None:
    from personal_data_warehouse_photos.envelope import provenance_dedup_sha256
    from personal_data_warehouse_photos.sync import PhotosUploadRunner
    from tests.test_photos_scanner import _build_fixture_library

    library = _build_fixture_library(tmp_path)
    client = _FakePhotoIngestClient()

    summary = PhotosUploadRunner(
        account="zach@example.com",
        library_path=library,
        ingest_client=client,
        logger=_CaptureLogger(),
        now=lambda: datetime(2026, 6, 2, 12, tzinfo=UTC),
    ).sync()

    assert summary.files_uploaded == 3
    assert len(client.files) == 3 and len(client.metadata) == 3
    still = next(m for m in client.metadata if m["payload"]["file"]["role"] == "original" and m["payload"]["file"]["mime_type"] == "image/heic")
    # The envelope carries the source slug the Dagster reader routes on, and
    # the metadata dedup key is the provenance sha (not the file sha).
    assert still["payload"]["schema_version"] == 1
    assert still["payload"]["source"] == "apple_photos"
    assert still["metadata_dedup_sha256"] == provenance_dedup_sha256(
        source="apple_photos",
        account="zach@example.com",
        native_id=still["payload"]["file"]["native_id"],
        role="original",
        file_content_sha256=still["file_content_sha256"],
    )
    assert still["file_content_sha256"] == audio_sha(b"still-bytes")
