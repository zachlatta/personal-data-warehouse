"""PhotosUploadRunner contract: coverage counts, deferrals, state, failures."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from personal_data_warehouse_photos.state import PhotosUploadState
from personal_data_warehouse_photos.sync import PhotosUploadRunner

from tests.test_photos_scanner import _build_fixture_library


class _Logger:
    def __init__(self) -> None:
        self.lines: list[str] = []

    def info(self, message, *args) -> None:
        self.lines.append(message % args if args else message)

    warning = info


class _FakeIngestClient:
    effective_max_upload_bytes = 100 * 1024 * 1024

    def __init__(self, *, fail_filenames: frozenset[str] = frozenset()) -> None:
        self.files: list[dict] = []
        self.metadata: list[dict] = []
        self._fail_filenames = fail_filenames

    def upload_photo_file(self, content, *, captured_at, extension, content_type):
        self.files.append(
            {
                "size": len(content),
                "captured_at": captured_at,
                "extension": extension,
                "content_type": content_type,
            }
        )
        return {"storage_backend": "google_drive", "storage_key": f"photos/inbox/x{len(self.files)}", "storage_file_id": f"fid-{len(self.files)}"}

    def upload_photo_metadata(self, payload, *, captured_at, file_content_sha256, metadata_dedup_sha256):
        if payload["file"]["filename"] in self._fail_filenames:
            raise RuntimeError(f"boom on {payload['file']['filename']}")
        self.metadata.append(
            {
                "payload": json.loads(json.dumps(payload)),
                "captured_at": captured_at,
                "file_content_sha256": file_content_sha256,
                "metadata_dedup_sha256": metadata_dedup_sha256,
            }
        )
        return {"storage_backend": "google_drive", "storage_key": "photos/inbox/meta", "storage_file_id": "fid-m"}


def _runner(tmp_path: Path, client, **kwargs) -> tuple[PhotosUploadRunner, PhotosUploadState, Path]:
    library = kwargs.pop("library", None) or _build_fixture_library(tmp_path)
    state = PhotosUploadState.open(
        tmp_path / "state.sqlite", account="z@x.test", library_path=library
    )
    runner = PhotosUploadRunner(
        account="z@x.test",
        library_path=library,
        ingest_client=client,
        logger=_Logger(),
        now=lambda: datetime(2026, 6, 2, 12, 0, tzinfo=UTC),
        upload_state=state,
        **kwargs,
    )
    return runner, state, library


def test_sync_uploads_present_files_and_reports_coverage(tmp_path):
    client = _FakeIngestClient()
    runner, state, _ = _runner(tmp_path, client)
    summary = runner.sync()
    # 3 assets scanned; live still + live video + plain video are present,
    # the cloud-only original is missing and deferred (not an error).
    assert summary.assets_seen == 3
    assert summary.files_seen == 4
    assert summary.files_present == 3
    assert summary.files_missing == 1
    assert summary.files_uploaded == 3
    assert summary.metadata_uploaded == 3
    assert len(client.files) == 3 and len(client.metadata) == 3
    state.close()


def test_sync_envelopes_carry_source_role_and_provenance(tmp_path):
    client = _FakeIngestClient()
    runner, state, _ = _runner(tmp_path, client)
    runner.sync()
    by_role = {(m["payload"]["file"]["native_id"], m["payload"]["file"]["role"]): m for m in client.metadata}
    live = by_role[("UUID-LIVE", "live_video")]
    assert live["payload"]["source"] == "apple_photos"
    # The .mov ships under the STILL's native id: that is the attach contract.
    assert live["payload"]["file"]["native_id"] == "UUID-LIVE"
    assert live["payload"]["apple_record"]["uuid"] == "UUID-LIVE"
    still = by_role[("UUID-LIVE", "original")]
    assert still["payload"]["file"]["camera_model"] == "iPhone 16 Pro"
    assert still["metadata_dedup_sha256"] != live["metadata_dedup_sha256"]
    state.close()


def test_sync_skips_state_complete_files_and_reuploads_on_fingerprint_change(tmp_path):
    client = _FakeIngestClient()
    runner, state, library = _runner(tmp_path, client)
    runner.sync()
    assert len(client.files) == 3

    # Second run: everything complete, nothing re-uploads.
    client2 = _FakeIngestClient()
    runner2 = PhotosUploadRunner(
        account="z@x.test",
        library_path=library,
        ingest_client=client2,
        logger=_Logger(),
        now=lambda: datetime(2026, 6, 2, 13, 0, tzinfo=UTC),
        upload_state=state,
    )
    summary = runner2.sync()
    assert summary.files_uploaded == 0
    assert summary.files_skipped == 3
    assert client2.files == []

    # Touch one file (fingerprint changes) -> exactly that file re-uploads.
    still = library / "originals" / "A" / "UUID-LIVE.heic"
    still.write_bytes(b"still-bytes-v2")
    client3 = _FakeIngestClient()
    runner3 = PhotosUploadRunner(
        account="z@x.test",
        library_path=library,
        ingest_client=client3,
        logger=_Logger(),
        now=lambda: datetime(2026, 6, 2, 14, 0, tzinfo=UTC),
        upload_state=state,
    )
    summary = runner3.sync()
    assert summary.files_uploaded == 1
    assert client3.files[0]["extension"] == ".heic"
    state.close()


def test_sync_defers_oversize_files_with_visible_count(tmp_path):
    client = _FakeIngestClient()
    runner, state, _ = _runner(tmp_path, client, max_upload_bytes=12)
    summary = runner.sync()
    # live-video-bytes (16) exceeds the 12-byte ceiling; the rest upload.
    assert summary.files_deferred_oversize == 1
    assert summary.files_uploaded == 2
    state.close()


def test_sync_collects_failures_and_reraises_after_the_batch(tmp_path):
    client = _FakeIngestClient(fail_filenames=frozenset({"IMG_0001.HEIC"}))
    runner, state, _ = _runner(tmp_path, client)
    with pytest.raises(RuntimeError, match="boom on IMG_0001.HEIC"):
        runner.sync()
    # The other files still uploaded and are recorded complete...
    assert len(client.metadata) == 2
    entry = state.entry_for(source_type="asset_file", source_id="UUID-VIDEO|original")
    assert entry is not None and entry.complete
    # ...while the failed one is recorded as failing, not complete.
    failed = state.entry_for(source_type="asset_file", source_id="UUID-LIVE|original")
    assert failed is not None and not failed.complete and "boom" in failed.last_error
    state.close()


def test_sync_limit_applies_after_state_selection(tmp_path):
    client = _FakeIngestClient()
    runner, state, library = _runner(tmp_path, client, limit=1)
    summary = runner.sync()
    assert summary.files_uploaded == 1

    # Next limited run progresses to the NEXT file instead of re-considering
    # the already-complete head of the list.
    client2 = _FakeIngestClient()
    runner2 = PhotosUploadRunner(
        account="z@x.test",
        library_path=library,
        ingest_client=client2,
        logger=_Logger(),
        now=lambda: datetime(2026, 6, 2, 13, 0, tzinfo=UTC),
        upload_state=state,
        limit=1,
    )
    summary2 = runner2.sync()
    assert summary2.files_uploaded == 1
    assert summary2.files_skipped == 1
    state.close()


def test_before_upload_check_blocks_the_batch(tmp_path):
    client = _FakeIngestClient()
    runner, state, _ = _runner(tmp_path, client, before_upload_check=lambda: "metered network")
    summary = runner.sync()
    assert summary.files_uploaded == 0
    assert client.files == []
    state.close()


def test_state_wipes_when_library_changes(tmp_path):
    library = _build_fixture_library(tmp_path)
    state = PhotosUploadState.open(tmp_path / "state.sqlite", account="z@x.test", library_path=library)
    state.mark_success(
        source_type="asset_file", source_id="UUID-LIVE|original", fingerprint="1|2",
        now=datetime(2026, 6, 2, tzinfo=UTC),
    )
    state.close()
    other = PhotosUploadState.open(
        tmp_path / "state.sqlite", account="z@x.test", library_path=tmp_path / "Other.photoslibrary"
    )
    assert other.entry_for(source_type="asset_file", source_id="UUID-LIVE|original") is None
    other.close()


def test_state_survives_reopen_with_same_identity(tmp_path):
    library = _build_fixture_library(tmp_path)
    state = PhotosUploadState.open(tmp_path / "state.sqlite", account="z@x.test", library_path=library)
    state.mark_success(
        source_type="asset_file", source_id="UUID-LIVE|original", fingerprint="1|2",
        now=datetime(2026, 6, 2, tzinfo=UTC), content_sha256="sha",
    )
    state.close()
    reopened = PhotosUploadState.open(tmp_path / "state.sqlite", account="z@x.test", library_path=library)
    assert reopened.is_complete(source_type="asset_file", source_id="UUID-LIVE|original", fingerprint="1|2")
    assert not reopened.is_complete(source_type="asset_file", source_id="UUID-LIVE|original", fingerprint="9|9")
    reopened.close()


def test_state_is_a_sqlite_file(tmp_path):
    library = _build_fixture_library(tmp_path)
    state = PhotosUploadState.open(tmp_path / "state.sqlite", account="z@x.test", library_path=library)
    state.close()
    with sqlite3.connect(tmp_path / "state.sqlite") as connection:
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"metadata", "upload_state"} <= tables
