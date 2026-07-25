"""PhotosUploadRunner contract: coverage counts, deferrals, state, failures."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from personal_data_warehouse_photos.exporter import ExportedPhotoFile, PhotoExportError
from personal_data_warehouse_photos.state import PhotosUploadState
from personal_data_warehouse_photos.sync import (
    MAX_FATAL_ATTEMPTS,
    MAX_RETRY_BACKOFF,
    PhotosUploadRunner,
    retry_delay,
)

from tests.test_photos_scanner import _build_fixture_library


class _Logger:
    def __init__(self) -> None:
        self.lines: list[str] = []

    def info(self, message, *args) -> None:
        self.lines.append(message % args if args else message)

    warning = info


class _FakeIngestClient:
    # Photo files must ignore the one-shot route ceiling: they use resumable
    # Drive sessions now, including when the public app route is capped.
    effective_max_upload_bytes = 12

    def __init__(self, *, fail_filenames: frozenset[str] = frozenset()) -> None:
        self.files: list[dict] = []
        self.metadata: list[dict] = []
        self._fail_filenames = fail_filenames

    def upload_photo_file_path(
        self, path, *, captured_at, extension, content_type, content_sha256
    ):
        self.files.append(
            {
                "size": Path(path).stat().st_size,
                "content_sha256": content_sha256,
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


class _FakeExporter:
    _CONTENT = {
        ("UUID-LIVE", "original"): b"still-bytes",
        ("UUID-LIVE", "live_video"): b"live-video-bytes",
        ("UUID-MISSING", "original"): b"cloud",
        ("UUID-VIDEO", "original"): b"video-bytes",
    }

    def __init__(self, *, fail_ids: frozenset[tuple[str, str]] = frozenset()) -> None:
        self.calls: list[tuple[str, str]] = []
        self._fail_ids = fail_ids
        self.max_staged_files = 0

    def export(self, candidate, destination_dir):
        key = (candidate.native_id, candidate.role)
        self.calls.append(key)
        if key in self._fail_ids:
            raise PhotoExportError(f"iCloud failed for {candidate.filename}")
        content = self._CONTENT[key]
        path = Path(destination_dir) / f"{candidate.native_id}-{candidate.role}{candidate.extension}"
        path.write_bytes(content)
        self.max_staged_files = max(
            self.max_staged_files,
            sum(1 for item in Path(destination_dir).iterdir() if item.is_file()),
        )
        return ExportedPhotoFile(
            path=path,
            filename=candidate.filename,
            extension=candidate.extension,
            mime_type=candidate.mime_type,
            size_bytes=len(content),
        )


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
        resource_exporter=kwargs.pop("resource_exporter", _FakeExporter()),
        **kwargs,
    )
    return runner, state, library


def test_sync_downloads_and_uploads_every_original_including_icloud_only(tmp_path):
    client = _FakeIngestClient()
    runner, state, _ = _runner(tmp_path, client)
    summary = runner.sync()
    # 3 assets scanned; PhotoKit exports all four original resources, including
    # the photo with no bytes in the local Photos-library cache.
    assert summary.assets_seen == 3
    assert summary.files_seen == 4
    assert summary.files_exported == 4
    assert summary.files_uploaded == 4
    assert summary.metadata_uploaded == 4
    assert len(client.files) == 4 and len(client.metadata) == 4
    uploaded_ids = {
        (item["payload"]["file"]["native_id"], item["payload"]["file"]["role"])
        for item in client.metadata
    }
    assert ("UUID-MISSING", "original") in uploaded_ids
    state.close()


def test_sync_removes_each_temporary_full_export_after_upload(tmp_path):
    client = _FakeIngestClient()
    exporter = _FakeExporter()
    runner, state, _ = _runner(tmp_path, client, resource_exporter=exporter)

    runner.sync()

    # A large backfill must not retain every hydrated original until the end
    # of the run and consume the size of the whole library in temporary space.
    assert exporter.max_staged_files == 1
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
    assert len(client.files) == 4

    # Second run: everything complete, nothing re-uploads.
    client2 = _FakeIngestClient()
    runner2 = PhotosUploadRunner(
        account="z@x.test",
        library_path=library,
        ingest_client=client2,
        logger=_Logger(),
        now=lambda: datetime(2026, 6, 2, 13, 0, tzinfo=UTC),
        upload_state=state,
        resource_exporter=_FakeExporter(),
    )
    summary = runner2.sync()
    assert summary.files_uploaded == 0
    assert summary.files_skipped == 4
    assert client2.files == []

    # Change one Photos metadata record (fingerprint changes) -> exactly that
    # original resource re-exports and re-uploads.
    with sqlite3.connect(library / "database" / "Photos.sqlite") as connection:
        connection.execute("UPDATE ZASSET SET ZFAVORITE = 1 WHERE ZUUID = 'UUID-VIDEO'")
        connection.commit()
    client3 = _FakeIngestClient()
    runner3 = PhotosUploadRunner(
        account="z@x.test",
        library_path=library,
        ingest_client=client3,
        logger=_Logger(),
        now=lambda: datetime(2026, 6, 2, 14, 0, tzinfo=UTC),
        upload_state=state,
        resource_exporter=_FakeExporter(),
    )
    summary = runner3.sync()
    assert summary.files_uploaded == 1
    assert client3.files[0]["extension"] == ".mov"
    state.close()


def test_sync_uploads_files_above_the_legacy_one_shot_ceiling(tmp_path):
    client = _FakeIngestClient()
    runner, state, _ = _runner(tmp_path, client)
    summary = runner.sync()
    # live-video-bytes exceeds the fake client's 12-byte one-shot ceiling, but
    # resumable photo uploads have no route-size deferral.
    assert summary.files_exported == 4
    assert summary.files_uploaded == 4
    assert not hasattr(summary, "files_deferred_oversize")
    state.close()


def test_sync_collects_failures_and_reraises_after_the_batch(tmp_path):
    client = _FakeIngestClient(fail_filenames=frozenset({"IMG_0001.HEIC"}))
    runner, state, _ = _runner(tmp_path, client)
    with pytest.raises(RuntimeError, match="boom on IMG_0001.HEIC"):
        runner.sync()
    # The other files still uploaded and are recorded complete...
    assert len(client.metadata) == 3
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
        resource_exporter=_FakeExporter(),
        limit=1,
    )
    summary2 = runner2.sync()
    assert summary2.files_uploaded == 1
    assert summary2.files_skipped == 1
    state.close()


def test_before_upload_check_blocks_the_batch(tmp_path):
    client = _FakeIngestClient()
    exporter = _FakeExporter()
    runner, state, _ = _runner(
        tmp_path,
        client,
        before_upload_check=lambda: "metered network",
        resource_exporter=exporter,
    )
    summary = runner.sync()
    assert summary.files_uploaded == 0
    assert client.files == []
    assert exporter.calls == []
    state.close()


def test_sync_fails_loudly_when_an_icloud_original_cannot_be_downloaded(tmp_path):
    client = _FakeIngestClient()
    exporter = _FakeExporter(fail_ids=frozenset({("UUID-MISSING", "original")}))
    runner, state, _ = _runner(tmp_path, client, resource_exporter=exporter)

    with pytest.raises(PhotoExportError, match="iCloud failed for IMG_0002.HEIC"):
        runner.sync()

    # Other originals still upload, but the missing full file makes the run
    # non-zero and records a retryable failure instead of false coverage.
    assert len(client.metadata) == 3
    failed = state.entry_for(source_type="asset_file", source_id="UUID-MISSING|original")
    assert failed is not None and not failed.complete
    state.close()


_UNEXPORTABLE = ("UUID-MISSING", "original")


def _scheduled_run(tmp_path: Path, library: Path, *, at: datetime, exporter, **kwargs):
    """One scheduled run against the shared state file; never raises."""
    client = _FakeIngestClient()
    logger = _Logger()
    state = PhotosUploadState.open(
        tmp_path / "state.sqlite", account="z@x.test", library_path=library
    )
    runner = PhotosUploadRunner(
        account="z@x.test",
        library_path=library,
        ingest_client=client,
        logger=logger,
        now=lambda: at,
        upload_state=state,
        resource_exporter=exporter,
        **kwargs,
    )
    try:
        return runner.sync(), None, client, logger
    except Exception as exc:  # noqa: BLE001 - the assertion subject
        return None, exc, client, logger
    finally:
        state.close()


def test_a_file_photokit_can_never_export_stops_failing_every_later_run(tmp_path):
    library = _build_fixture_library(tmp_path)
    start = datetime(2026, 6, 2, 12, 0, tzinfo=UTC)

    outcomes = []
    for attempt in range(MAX_FATAL_ATTEMPTS):
        # Space runs past the longest backoff so each one really re-attempts.
        at = start + (MAX_RETRY_BACKOFF + timedelta(hours=1)) * attempt
        summary, error, _, _ = _scheduled_run(
            tmp_path,
            library,
            at=at,
            exporter=_FakeExporter(fail_ids=frozenset({_UNEXPORTABLE})),
        )
        outcomes.append((summary, error))

    # The first attempts are loud: a fresh failure is assumed retryable.
    assert all(isinstance(error, PhotoExportError) for _, error in outcomes[:-1])
    # Once the file has burned its attempts — and other files have proven the
    # export path still works — it stops turning every scheduled run red.
    final_summary, final_error = outcomes[-1]
    assert final_error is None
    assert final_summary is not None and final_summary.files_failed == 1
    assert final_summary.files_uploaded == 0


def test_a_failing_file_backs_off_instead_of_consuming_the_next_runs_limit(tmp_path):
    library = _build_fixture_library(tmp_path)
    start = datetime(2026, 6, 2, 12, 0, tzinfo=UTC)
    # The newest candidate is the one PhotoKit rejects, so an unbacked-off
    # retry would eat the whole limit of every run behind it.
    exporter_ids = frozenset({("UUID-LIVE", "original")})

    _, error, first_client, _ = _scheduled_run(
        tmp_path, library, at=start, exporter=_FakeExporter(fail_ids=exporter_ids), limit=1
    )
    assert isinstance(error, PhotoExportError)
    assert first_client.files == []

    summary, error, client, _ = _scheduled_run(
        tmp_path,
        library,
        at=start + timedelta(minutes=5),
        exporter=_FakeExporter(fail_ids=exporter_ids),
        limit=1,
    )
    assert error is None
    assert summary is not None and summary.files_deferred == 1
    # The slot went to the next file instead of the known-bad one.
    assert summary.files_uploaded == 1
    assert client.files[0]["extension"] == ".mov"


def test_a_deferred_file_is_retried_once_its_backoff_expires(tmp_path):
    library = _build_fixture_library(tmp_path)
    start = datetime(2026, 6, 2, 12, 0, tzinfo=UTC)
    _scheduled_run(
        tmp_path, library, at=start, exporter=_FakeExporter(fail_ids=frozenset({_UNEXPORTABLE}))
    )

    summary, _, client, _ = _scheduled_run(
        tmp_path,
        library,
        at=start + retry_delay(1) + timedelta(minutes=1),
        exporter=_FakeExporter(),
    )

    assert summary is not None and summary.files_deferred == 0
    assert [item["extension"] for item in client.files] == [".heic"]


def test_failures_keep_failing_the_run_while_nothing_at_all_succeeds(tmp_path):
    library = _build_fixture_library(tmp_path)
    everything = frozenset(
        {("UUID-LIVE", "original"), ("UUID-LIVE", "live_video"), _UNEXPORTABLE, ("UUID-VIDEO", "original")}
    )
    start = datetime(2026, 6, 2, 12, 0, tzinfo=UTC)

    # A revoked Photos grant or a dead network fails every file. However many
    # attempts that accumulates, the run must stay red rather than reporting
    # a green "everything is just failing" steady state.
    for attempt in range(MAX_FATAL_ATTEMPTS + 3):
        at = start + (MAX_RETRY_BACKOFF + timedelta(hours=1)) * attempt
        _, error, _, _ = _scheduled_run(
            tmp_path, library, at=at, exporter=_FakeExporter(fail_ids=everything)
        )
        assert isinstance(error, PhotoExportError), f"attempt {attempt} went quiet"


def test_editing_the_asset_clears_the_failure_backoff(tmp_path):
    library = _build_fixture_library(tmp_path)
    start = datetime(2026, 6, 2, 12, 0, tzinfo=UTC)
    _scheduled_run(
        tmp_path, library, at=start, exporter=_FakeExporter(fail_ids=frozenset({_UNEXPORTABLE}))
    )

    with sqlite3.connect(library / "database" / "Photos.sqlite") as connection:
        connection.execute("UPDATE ZASSET SET ZFAVORITE = 1 WHERE ZUUID = 'UUID-MISSING'")
        connection.commit()

    summary, error, client, _ = _scheduled_run(
        tmp_path, library, at=start + timedelta(minutes=5), exporter=_FakeExporter()
    )

    assert error is None
    assert summary is not None and summary.files_deferred == 0
    assert len(client.files) == 1


def test_retry_delay_grows_and_is_capped(tmp_path):
    assert retry_delay(0) == timedelta(0)
    assert retry_delay(1) < retry_delay(2) < retry_delay(3)
    assert retry_delay(500) == MAX_RETRY_BACKOFF


def test_state_counts_consecutive_failures_and_resets_them_on_success(tmp_path):
    library = _build_fixture_library(tmp_path)
    state = PhotosUploadState.open(tmp_path / "state.sqlite", account="z@x.test", library_path=library)
    moment = datetime(2026, 6, 2, tzinfo=UTC)

    first = state.mark_failure(
        source_type="asset_file", source_id="UUID-LIVE|original", fingerprint="1|2",
        error="boom", now=moment,
    )
    second = state.mark_failure(
        source_type="asset_file", source_id="UUID-LIVE|original", fingerprint="1|2",
        error="boom", now=moment + timedelta(hours=1),
    )
    assert (first.failure_count, second.failure_count) == (1, 2)
    assert second.first_failure_at == first.first_failure_at

    # A changed fingerprint is a different file: the streak starts over.
    restarted = state.mark_failure(
        source_type="asset_file", source_id="UUID-LIVE|original", fingerprint="9|9",
        error="boom", now=moment + timedelta(hours=2),
    )
    assert restarted.failure_count == 1

    state.mark_success(
        source_type="asset_file", source_id="UUID-LIVE|original", fingerprint="9|9",
        now=moment + timedelta(hours=3),
    )
    entry = state.entry_for(source_type="asset_file", source_id="UUID-LIVE|original")
    assert entry is not None and entry.failure_count == 0 and entry.first_failure_at == ""
    assert state.latest_success_at() == moment + timedelta(hours=3)

    state.mark_failure(
        source_type="asset_file", source_id="UUID-VIDEO|original", fingerprint="1|2",
        error="boom", now=moment + timedelta(hours=4),
    )
    assert state.clear_failures() == 1
    cleared = state.entry_for(source_type="asset_file", source_id="UUID-VIDEO|original")
    assert cleared is not None and cleared.failure_count == 0
    state.close()


def test_state_adds_retry_columns_to_a_state_file_written_before_them(tmp_path):
    library = _build_fixture_library(tmp_path)
    path = tmp_path / "state.sqlite"
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL DEFAULT '');
            CREATE TABLE upload_state (
                source_type TEXT NOT NULL, source_id TEXT NOT NULL,
                fingerprint TEXT NOT NULL DEFAULT '', complete INTEGER NOT NULL DEFAULT 0,
                content_sha256 TEXT NOT NULL DEFAULT '', storage_key TEXT NOT NULL DEFAULT '',
                last_success_at TEXT NOT NULL DEFAULT '', last_failure_at TEXT NOT NULL DEFAULT '',
                last_error TEXT NOT NULL DEFAULT '', last_checked_at TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (source_type, source_id)
            );
            """
        )
        connection.executemany(
            "INSERT INTO metadata VALUES (?, ?)",
            [("schema_version", "1"), ("account", "z@x.test"), ("library_path", str(library))],
        )
        connection.execute(
            "INSERT INTO upload_state (source_type, source_id, fingerprint, complete) VALUES (?, ?, ?, 1)",
            ("asset_file", "UUID-LIVE|original", "fp"),
        )
        connection.commit()

    state = PhotosUploadState.open(path, account="z@x.test", library_path=library)
    # Migrating in place, not wiping: 12k already-uploaded files must not be
    # re-exported just because the schema grew two columns.
    assert state.is_complete(source_type="asset_file", source_id="UUID-LIVE|original", fingerprint="fp")
    entry = state.entry_for(source_type="asset_file", source_id="UUID-LIVE|original")
    assert entry is not None and entry.failure_count == 0
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
