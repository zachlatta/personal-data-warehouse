from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import sqlite3

from personal_data_warehouse.config import GOOGLE_DRIVE_SCOPE, load_settings
from personal_data_warehouse_voice_memos.scanner import recording_from_path, scan_voice_memo_file_candidates, scan_voice_memos
from personal_data_warehouse_voice_memos.state import VoiceMemosUploadState
from personal_data_warehouse_voice_memos.sync import VoiceMemosUploadRunner


class FakeLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def info(self, *args, **kwargs) -> None:
        self.messages.append(args[0] % args[1:] if len(args) > 1 else str(args[0]))

    def warning(self, *args, **kwargs) -> None:
        self.messages.append(args[0] % args[1:] if len(args) > 1 else str(args[0]))


class FakeIngestClient:
    """Stands in for the app ingest client: records the audio + metadata the
    runner posts. The app dedups server-side, so the client always sends."""

    def __init__(self) -> None:
        self.audio_uploads: list[dict] = []
        self.metadata_uploads: list[dict] = []

    def upload_voice_memo_audio(self, content, *, recorded_at, extension, content_type):
        self.audio_uploads.append(
            {"content": content, "recorded_at": recorded_at, "extension": extension, "content_type": content_type}
        )
        return {"storage_backend": "google_drive", "storage_key": "audio", "storage_file_id": "fid-a", "storage_url": ""}

    def upload_voice_memo_metadata(self, payload, *, recorded_at, audio_content_sha256):
        self.metadata_uploads.append(
            {"payload": payload, "recorded_at": recorded_at, "audio_content_sha256": audio_content_sha256}
        )
        return {"storage_backend": "google_drive", "storage_key": "meta", "storage_file_id": "fid-m", "storage_url": ""}


def test_load_settings_adds_drive_scope_when_voice_memos_uses_google_drive(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.com")
    monkeypatch.setenv("VOICE_MEMOS_ACCOUNT", "zach@example.com")
    monkeypatch.setenv("VOICE_MEMOS_GOOGLE_DRIVE_FOLDER_ID", "folder-id")

    settings = load_settings(require_postgres=False, require_gmail=False, require_voice_memos=True)

    assert settings.voice_memos is not None
    assert settings.voice_memos.account == "zach@example.com"
    assert settings.voice_memos.google_drive_folder_id == "folder-id"
    assert GOOGLE_DRIVE_SCOPE in settings.google_scopes


def test_scan_voice_memos_includes_m4a_and_qta(tmp_path) -> None:
    m4a = tmp_path / "20260427 100004-40DC0200.m4a"
    qta = tmp_path / "20260325 145019-DAAC9394.qta"
    ignored = tmp_path / "CloudRecordings.db"
    m4a.write_bytes(b"m4a")
    qta.write_bytes(b"qta")
    ignored.write_bytes(b"db")

    recordings = scan_voice_memos(tmp_path, extensions=(".m4a", ".qta"))

    by_path = {recording.path: recording for recording in recordings}
    assert set(by_path) == {m4a, qta}
    assert by_path[m4a].recording_id == "20260427 100004-40DC0200"
    assert by_path[qta].content_type == "audio/quicktime"


def test_scan_voice_memos_loads_cloud_recording_durations(tmp_path) -> None:
    recording = tmp_path / "20260430 110736-8BB8E57D.qta"
    recording.write_bytes(b"partial-audio")
    create_cloud_recordings_db(
        tmp_path,
        filename=recording.name,
        duration=9349.66,
        local_duration=6228.63,
    )

    [candidate] = scan_voice_memo_file_candidates(tmp_path, extensions=(".qta",))
    [full_recording] = scan_voice_memos(tmp_path, extensions=(".qta",))

    assert candidate.duration_seconds == 9349.66
    assert candidate.local_duration_seconds == 6228.63
    assert full_recording.duration_seconds == 9349.66
    assert full_recording.local_duration_seconds == 6228.63


def test_mac_runner_defers_partially_materialized_voice_memos(tmp_path) -> None:
    recording = tmp_path / "20260430 110736-8BB8E57D.qta"
    recording.write_bytes(b"partial-audio")
    create_cloud_recordings_db(
        tmp_path,
        filename=recording.name,
        duration=9349.66,
        local_duration=6228.63,
    )
    ingest = FakeIngestClient()
    logger = FakeLogger()

    summary = VoiceMemosUploadRunner(
        account="zach@example.com",
        recordings_path=tmp_path,
        extensions=(".qta",),
        ingest_client=ingest,
        logger=logger,
        mode="incremental",
        upload_state=VoiceMemosUploadState.empty(account="zach@example.com", recordings_path=tmp_path),
        now=lambda: datetime(2026, 4, 30, 16, tzinfo=UTC),
    ).sync()

    assert summary.recordings_seen == 1
    assert summary.recordings_selected == 0
    assert summary.recordings_uploaded == 0
    assert summary.recordings_deferred == 1
    assert ingest.audio_uploads == []
    assert ingest.metadata_uploads == []
    assert any("Deferring 20260430 110736-8BB8E57D.qta" in message for message in logger.messages)


def test_mac_runner_defers_short_zero_local_duration_voice_memos(tmp_path) -> None:
    recording = tmp_path / "20250310 210228-0C9BF035.m4a"
    recording.write_bytes(b"")
    create_cloud_recordings_db(
        tmp_path,
        filename=recording.name,
        duration=2.68,
        local_duration=0,
    )
    ingest = FakeIngestClient()

    summary = VoiceMemosUploadRunner(
        account="zach@example.com",
        recordings_path=tmp_path,
        extensions=(".m4a",),
        ingest_client=ingest,
        logger=FakeLogger(),
        mode="incremental",
        upload_state=VoiceMemosUploadState.empty(account="zach@example.com", recordings_path=tmp_path),
        now=lambda: datetime(2026, 3, 10, 22, tzinfo=UTC),
    ).sync()

    assert summary.recordings_deferred == 1
    assert ingest.audio_uploads == []


def test_mac_runner_uploads_audio_files_and_metadata(tmp_path) -> None:
    # The app dedups server-side, so the client posts every recording's audio
    # and metadata; it no longer probes for "already present".
    first = tmp_path / "20260427 100004-40DC0200.m4a"
    fresh = tmp_path / "20260325 145019-DAAC9394.qta"
    first.write_bytes(b"already-synced")
    fresh.write_bytes(b"new-recording")
    fresh_sha = recording_from_path(fresh).content_sha256
    ingest = FakeIngestClient()

    logger = FakeLogger()
    summary = VoiceMemosUploadRunner(
        account="zach@example.com",
        recordings_path=tmp_path,
        extensions=(".m4a", ".qta"),
        ingest_client=ingest,
        logger=logger,
        now=lambda: datetime(2026, 4, 27, 12, tzinfo=UTC),
    ).sync()

    assert summary.recordings_seen == 2
    assert summary.recordings_uploaded == 2
    assert summary.metadata_uploaded == 2
    assert len(ingest.audio_uploads) == 2
    assert len(ingest.metadata_uploads) == 2
    fresh_audio = next(u for u in ingest.audio_uploads if u["content"] == b"new-recording")
    assert fresh_audio["extension"] == ".qta"
    fresh_meta = next(m for m in ingest.metadata_uploads if m["audio_content_sha256"] == fresh_sha)
    payload = fresh_meta["payload"]
    assert payload["schema_version"] == 1
    assert payload["account"] == "zach@example.com"
    assert "audio_file" not in payload
    assert "metadata_file" not in payload
    assert any("Scanning Voice Memos" in message for message in logger.messages)
    assert any("Found 2 Voice Memos recordings" in message for message in logger.messages)
    assert any("] upload" in message for message in logger.messages)
    assert any("Voice Memos upload summary" in message for message in logger.messages)


def test_mac_runner_uploads_audio_and_metadata_for_each_recording(tmp_path) -> None:
    recording = tmp_path / "20260427 100004-40DC0200.m4a"
    recording.write_bytes(b"audio-bytes")
    ingest = FakeIngestClient()

    summary = VoiceMemosUploadRunner(
        account="zach@example.com",
        recordings_path=tmp_path,
        extensions=(".m4a", ".qta"),
        ingest_client=ingest,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 4, 27, 12, tzinfo=UTC),
    ).sync()

    assert summary.recordings_uploaded == 1
    assert len(ingest.audio_uploads) == 1
    assert len(ingest.metadata_uploads) == 1


def test_mac_runner_supports_parallel_workers(tmp_path) -> None:
    first = tmp_path / "20260427 100004-40DC0200.m4a"
    second = tmp_path / "20260428 100004-40DC0200.m4a"
    first.write_bytes(b"first")
    second.write_bytes(b"second")
    ingest = FakeIngestClient()

    summary = VoiceMemosUploadRunner(
        account="zach@example.com",
        recordings_path=tmp_path,
        extensions=(".m4a", ".qta"),
        ingest_client=ingest,
        logger=FakeLogger(),
        workers=2,
    ).sync()

    assert summary.recordings_seen == 2
    assert summary.recordings_uploaded == 2
    assert summary.metadata_uploaded == 2
    assert len(ingest.audio_uploads) == 2
    assert len(ingest.metadata_uploads) == 2


def test_incremental_runner_skips_unchanged_state_complete_files_without_drive_calls(tmp_path) -> None:
    recording = tmp_path / "20260427 100004-40DC0200.m4a"
    recording.write_bytes(b"already-synced")
    full_recording = recording_from_path(recording)
    ingest = FakeIngestClient()
    state = VoiceMemosUploadState.empty(account="zach@example.com", recordings_path=tmp_path)
    candidate = next(iter(scan_voice_memo_file_candidates(tmp_path, extensions=(".m4a",))))
    state.mark_success(
        candidate=candidate,
        content_sha256=full_recording.content_sha256,
        audio_uploaded=True,
        metadata_uploaded=True,
        now=datetime(2026, 4, 27, 12, tzinfo=UTC),
    )
    network_checks = 0

    def before_upload_check() -> str | None:
        nonlocal network_checks
        network_checks += 1
        return None

    summary = VoiceMemosUploadRunner(
        account="zach@example.com",
        recordings_path=tmp_path,
        extensions=(".m4a",),
        ingest_client=ingest,
        logger=FakeLogger(),
        mode="incremental",
        upload_state=state,
        before_upload_check=before_upload_check,
        now=lambda: datetime(2026, 4, 27, 12, 5, tzinfo=UTC),
    ).sync()

    assert summary.recordings_seen == 1
    assert summary.recordings_selected == 0
    assert summary.recordings_skipped == 1
    assert ingest.audio_uploads == []
    assert ingest.metadata_uploads == []
    assert network_checks == 0


def test_incremental_runner_defers_upload_when_network_guard_blocks_before_drive_calls(tmp_path) -> None:
    recording = tmp_path / "20260427 100004-40DC0200.m4a"
    recording.write_bytes(b"new-recording")
    ingest = FakeIngestClient()
    state = VoiceMemosUploadState.empty(account="zach@example.com", recordings_path=tmp_path)

    summary = VoiceMemosUploadRunner(
        account="zach@example.com",
        recordings_path=tmp_path,
        extensions=(".m4a",),
        ingest_client=ingest,
        logger=FakeLogger(),
        mode="incremental",
        upload_state=state,
        before_upload_check=lambda: "blocked Wi-Fi SSID: United Wi-Fi",
        now=lambda: datetime(2026, 4, 27, 12, 5, tzinfo=UTC),
    ).sync()

    assert summary.recordings_seen == 1
    assert summary.recordings_selected == 1
    assert summary.recordings_deferred == 1
    assert ingest.audio_uploads == []
    assert ingest.metadata_uploads == []


def create_cloud_recordings_db(tmp_path: Path, *, filename: str, duration: float, local_duration: float) -> None:
    connection = sqlite3.connect(tmp_path / "CloudRecordings.db")
    try:
        connection.execute(
            """
            CREATE TABLE ZCLOUDRECORDING (
                ZPATH VARCHAR,
                ZDURATION FLOAT,
                ZLOCALDURATION FLOAT
            )
            """
        )
        connection.execute(
            "INSERT INTO ZCLOUDRECORDING (ZPATH, ZDURATION, ZLOCALDURATION) VALUES (?, ?, ?)",
            (filename, duration, local_duration),
        )
        connection.commit()
    finally:
        connection.close()
