from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import sqlite3

from personal_data_warehouse.config import GOOGLE_DRIVE_SCOPE, load_settings
from personal_data_warehouse_voice_memos.scanner import recording_from_path, scan_voice_memo_file_candidates, scan_voice_memos
from personal_data_warehouse_voice_memos.state import VoiceMemosUploadState
from personal_data_warehouse_voice_memos.storage import ObjectPresence
from personal_data_warehouse_voice_memos.sync import VoiceMemosUploadRunner


class FakeLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def info(self, *args, **kwargs) -> None:
        self.messages.append(args[0] % args[1:] if len(args) > 1 else str(args[0]))

    def warning(self, *args, **kwargs) -> None:
        self.messages.append(args[0] % args[1:] if len(args) > 1 else str(args[0]))


class FakeObjectStore:
    backend = "fake"

    def __init__(
        self,
        existing_sha256: set[str] | None = None,
        existing_metadata_sha256: set[str] | None = None,
    ) -> None:
        self.existing_sha256 = existing_sha256 or set()
        self.existing_metadata_sha256 = existing_metadata_sha256 or set()
        self.file_uploads: list[tuple[Path, str]] = []
        self.json_uploads: list[tuple[str, dict[str, object]]] = []
        self.presence_calls: list[str] = []

    def has_blob(self, *, content_sha256: str) -> bool:
        return content_sha256 in self.existing_sha256

    def has_metadata(self, *, content_sha256: str) -> bool:
        return content_sha256 in self.existing_metadata_sha256

    def presence(self, *, content_sha256: str) -> ObjectPresence:
        self.presence_calls.append(content_sha256)
        return ObjectPresence(
            audio_exists=content_sha256 in self.existing_sha256,
            metadata_exists=content_sha256 in self.existing_metadata_sha256,
        )

    def put_file(
        self,
        *,
        path: Path,
        object_key: str,
        content_sha256: str,
        content_type: str,
        skip_existing_check: bool = False,
    ):
        if content_sha256 not in self.existing_sha256:
            self.file_uploads.append((path, object_key))
        self.existing_sha256.add(content_sha256)
        return {
            "storage_backend": self.backend,
            "storage_key": object_key,
            "storage_file_id": f"file-{content_sha256[:8]}",
            "storage_url": f"https://example.test/{object_key}",
        }

    def put_json(
        self,
        *,
        object_key: str,
        payload: dict[str, object],
        content_sha256: str,
        source_content_sha256: str | None = None,
        skip_existing_check: bool = False,
    ):
        self.json_uploads.append((object_key, payload))
        if source_content_sha256:
            self.existing_metadata_sha256.add(source_content_sha256)
        return {
            "storage_backend": self.backend,
            "storage_key": object_key,
            "storage_file_id": f"metadata-{content_sha256[:8]}",
            "storage_url": f"https://example.test/{object_key}",
        }


def test_load_settings_adds_drive_scope_when_voice_memos_uses_google_drive(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.com")
    monkeypatch.setenv("VOICE_MEMOS_ACCOUNT", "zach@example.com")
    monkeypatch.setenv("VOICE_MEMOS_GOOGLE_DRIVE_FOLDER_ID", "folder-id")

    settings = load_settings(require_clickhouse=False, require_gmail=False, require_voice_memos=True)

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
    object_store = FakeObjectStore()
    logger = FakeLogger()

    summary = VoiceMemosUploadRunner(
        account="zach@example.com",
        recordings_path=tmp_path,
        extensions=(".qta",),
        object_store=object_store,
        logger=logger,
        mode="incremental",
        upload_state=VoiceMemosUploadState.empty(account="zach@example.com", recordings_path=tmp_path),
        now=lambda: datetime(2026, 4, 30, 16, tzinfo=UTC),
    ).sync()

    assert summary.recordings_seen == 1
    assert summary.recordings_selected == 0
    assert summary.recordings_uploaded == 0
    assert summary.recordings_deferred == 1
    assert object_store.file_uploads == []
    assert object_store.json_uploads == []
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
    object_store = FakeObjectStore()

    summary = VoiceMemosUploadRunner(
        account="zach@example.com",
        recordings_path=tmp_path,
        extensions=(".m4a",),
        object_store=object_store,
        logger=FakeLogger(),
        mode="incremental",
        upload_state=VoiceMemosUploadState.empty(account="zach@example.com", recordings_path=tmp_path),
        now=lambda: datetime(2026, 3, 10, 22, tzinfo=UTC),
    ).sync()

    assert summary.recordings_deferred == 1
    assert object_store.file_uploads == []


def test_mac_runner_uploads_audio_files_and_metadata_without_clickhouse(tmp_path) -> None:
    existing = tmp_path / "20260427 100004-40DC0200.m4a"
    fresh = tmp_path / "20260325 145019-DAAC9394.qta"
    existing.write_bytes(b"already-synced")
    fresh.write_bytes(b"new-recording")
    existing_sha = recording_from_path(existing).content_sha256
    object_store = FakeObjectStore(existing_sha256={existing_sha}, existing_metadata_sha256={existing_sha})

    logger = FakeLogger()
    summary = VoiceMemosUploadRunner(
        account="zach@example.com",
        recordings_path=tmp_path,
        extensions=(".m4a", ".qta"),
        object_store=object_store,
        logger=logger,
        now=lambda: datetime(2026, 4, 27, 12, tzinfo=UTC),
    ).sync()

    assert summary.recordings_seen == 2
    assert summary.recordings_uploaded == 1
    assert summary.metadata_uploaded == 1
    assert object_store.file_uploads == [(fresh, "apple-voice-memos/inbox/2026/03/2026-03-25-9ae3313949541803350fcf20aa8c1493edcb7d53577ee8572c726616545c60a3.qta")]
    assert object_store.json_uploads[0][0] == "apple-voice-memos/inbox/2026/03/2026-03-25-9ae3313949541803350fcf20aa8c1493edcb7d53577ee8572c726616545c60a3.json"
    metadata = object_store.json_uploads[0][1]
    assert metadata["schema_version"] == 1
    assert metadata["account"] == "zach@example.com"
    assert "audio_file" not in metadata
    assert "metadata_file" not in metadata
    assert metadata["recording"]["content_sha256"] not in {existing_sha}
    assert any("Scanning Voice Memos" in message for message in logger.messages)
    assert any("Found 2 Voice Memos recordings" in message for message in logger.messages)
    assert any("] skip" in message for message in logger.messages)
    assert any("] upload" in message for message in logger.messages)
    assert any("Voice Memos upload summary" in message for message in logger.messages)


def test_mac_runner_recovers_when_audio_exists_but_metadata_is_missing(tmp_path) -> None:
    recording = tmp_path / "20260427 100004-40DC0200.m4a"
    recording.write_bytes(b"audio-exists")
    recording_sha = recording_from_path(recording).content_sha256
    object_store = FakeObjectStore(existing_sha256={recording_sha})

    summary = VoiceMemosUploadRunner(
        account="zach@example.com",
        recordings_path=tmp_path,
        extensions=(".m4a", ".qta"),
        object_store=object_store,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 4, 27, 12, tzinfo=UTC),
    ).sync()

    assert summary.recordings_uploaded == 1
    assert object_store.file_uploads == []
    assert len(object_store.json_uploads) == 1


def test_mac_runner_supports_parallel_workers(tmp_path) -> None:
    first = tmp_path / "20260427 100004-40DC0200.m4a"
    second = tmp_path / "20260428 100004-40DC0200.m4a"
    first.write_bytes(b"first")
    second.write_bytes(b"second")
    object_store = FakeObjectStore()

    summary = VoiceMemosUploadRunner(
        account="zach@example.com",
        recordings_path=tmp_path,
        extensions=(".m4a", ".qta"),
        object_store=object_store,
        logger=FakeLogger(),
        workers=2,
    ).sync()

    assert summary.recordings_seen == 2
    assert summary.recordings_uploaded == 2
    assert summary.metadata_uploaded == 2
    assert len(object_store.file_uploads) == 2
    assert len(object_store.json_uploads) == 2


def test_incremental_runner_skips_unchanged_state_complete_files_without_drive_calls(tmp_path) -> None:
    recording = tmp_path / "20260427 100004-40DC0200.m4a"
    recording.write_bytes(b"already-synced")
    full_recording = recording_from_path(recording)
    object_store = FakeObjectStore()
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
        object_store=object_store,
        logger=FakeLogger(),
        mode="incremental",
        upload_state=state,
        before_upload_check=before_upload_check,
        now=lambda: datetime(2026, 4, 27, 12, 5, tzinfo=UTC),
    ).sync()

    assert summary.recordings_seen == 1
    assert summary.recordings_selected == 0
    assert summary.recordings_skipped == 1
    assert object_store.presence_calls == []
    assert object_store.file_uploads == []
    assert object_store.json_uploads == []
    assert network_checks == 0


def test_incremental_runner_defers_upload_when_network_guard_blocks_before_drive_calls(tmp_path) -> None:
    recording = tmp_path / "20260427 100004-40DC0200.m4a"
    recording.write_bytes(b"new-recording")
    object_store = FakeObjectStore()
    state = VoiceMemosUploadState.empty(account="zach@example.com", recordings_path=tmp_path)

    summary = VoiceMemosUploadRunner(
        account="zach@example.com",
        recordings_path=tmp_path,
        extensions=(".m4a",),
        object_store=object_store,
        logger=FakeLogger(),
        mode="incremental",
        upload_state=state,
        before_upload_check=lambda: "blocked Wi-Fi SSID: United Wi-Fi",
        now=lambda: datetime(2026, 4, 27, 12, 5, tzinfo=UTC),
    ).sync()

    assert summary.recordings_seen == 1
    assert summary.recordings_selected == 1
    assert summary.recordings_deferred == 1
    assert object_store.presence_calls == []
    assert object_store.file_uploads == []
    assert object_store.json_uploads == []


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
