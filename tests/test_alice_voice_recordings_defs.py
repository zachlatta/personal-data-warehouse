from __future__ import annotations

from dagster import build_asset_context, build_schedule_context

from personal_data_warehouse.alice_voice_recordings_drive_ingest import (
    AliceVoiceRecordingsDriveIngestSummary,
)
from personal_data_warehouse.definitions import defs
from personal_data_warehouse.defs import alice_voice_recordings as alice_defs
from personal_data_warehouse_alice_voice_recordings.gmail_recovery import AliceGmailRecoverySummary
from personal_data_warehouse_alice_voice_recordings.sync import AliceVoiceRecordingsImportSummary


def test_alice_voice_recordings_defs_are_registered() -> None:
    repository = defs().get_repository_def()

    assert "alice_voice_recordings_import_job" in {job.name for job in repository.get_all_jobs()}
    assert "alice_voice_recordings_import_daily" in {schedule.name for schedule in repository.schedule_defs}
    assert repository.assets_defs_by_key


def test_alice_voice_recordings_daily_schedule_runs_daily() -> None:
    assert alice_defs.alice_voice_recordings_import_daily.cron_schedule == "17 4 * * *"
    assert alice_defs.alice_voice_recordings_import_daily.default_status.value == "RUNNING"


def test_alice_voice_recordings_schedule_uses_active_run_guard(monkeypatch) -> None:
    calls = []

    def fake_skip_if_job_active(context, *, job_name: str):
        calls.append(job_name)
        return {}

    monkeypatch.setattr(alice_defs, "skip_if_job_active", fake_skip_if_job_active)

    result = alice_defs.alice_voice_recordings_import_daily(build_schedule_context())

    assert result == {}
    assert calls == ["alice_voice_recordings_import_job"]


def test_alice_voice_recordings_asset_writes_summary_metadata(monkeypatch) -> None:
    monkeypatch.setattr(alice_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(alice_defs, "AliceApiClient", FakeAliceClient)
    monkeypatch.setattr(alice_defs, "_alice_object_store", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(alice_defs, "exclusive_sync_lock", fake_exclusive_sync_lock)
    monkeypatch.setattr(alice_defs, "AliceVoiceRecordingsImportRunner", FakeRunner)

    result = alice_defs.alice_voice_recordings_import(build_asset_context())

    metadata = result.metadata
    assert metadata["recordings_seen"].value == 2
    assert metadata["recordings_uploaded"].value == 1
    assert metadata["metadata_uploaded"].value == 2


def test_alice_archive_materialization_writes_summary_metadata(monkeypatch) -> None:
    monkeypatch.setattr(alice_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(alice_defs, "warehouse_from_settings", lambda _settings: object())
    monkeypatch.setattr(alice_defs, "_alice_object_store", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(alice_defs, "iter_archive_payloads", lambda **_kwargs: [])
    monkeypatch.setattr(alice_defs, "exclusive_sync_lock", fake_exclusive_sync_lock)
    monkeypatch.setattr(alice_defs, "AliceVoiceRecordingsDriveIngestRunner", FakeDriveIngestRunner)

    result = alice_defs.alice_voice_recordings_drive_ingest(build_asset_context())

    assert result.metadata["metadata_seen"].value == 5
    assert result.metadata["recordings_written"].value == 3
    assert result.metadata["artifacts_written"].value == 8


def test_alice_gmail_recovery_asset_writes_summary_metadata(monkeypatch) -> None:
    monkeypatch.setattr(alice_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(alice_defs, "warehouse_from_settings", lambda _settings: object())
    monkeypatch.setattr(alice_defs, "load_alice_gmail_transcript_emails", lambda **_kwargs: [])
    monkeypatch.setattr(alice_defs, "_alice_object_store", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(alice_defs, "build_gmail_service", lambda **_kwargs: object())
    monkeypatch.setattr(alice_defs, "exclusive_sync_lock", fake_exclusive_sync_lock)
    monkeypatch.setattr(alice_defs, "AliceGmailRecoveryRunner", FakeGmailRecoveryRunner)

    result = alice_defs.alice_voice_recordings_gmail_recovery(build_asset_context())

    metadata = result.metadata
    assert metadata["emails_seen"].value == 3
    assert metadata["attachments_uploaded"].value == 4
    assert metadata["metadata_uploaded"].value == 3


class FakeConfig:
    account = "alice@example.com"
    key_id = "key-id"
    secret_key = "secret-key"
    base_url = "https://aliceapp.ai"
    request_timeout_seconds = 120
    google_drive_account = "drive@example.com"
    google_drive_folder_id = "drive-folder"


class FakeSettings:
    postgres_database_url = "postgresql://example"
    gmail_accounts = ()
    alice_voice_recordings = FakeConfig()


class FakeAliceClient:
    def __init__(self, **kwargs) -> None:
        pass

    def iter_recordings(self):
        return []


class FakeRunner:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs

    def sync(self) -> AliceVoiceRecordingsImportSummary:
        return AliceVoiceRecordingsImportSummary(
            upload_requests_seen=2,
            recordings_skipped=1,
            recordings_uploaded=1,
            metadata_uploaded=2,
            bytes_uploaded=123,
            bytes_skipped=456,
        )


class FakeGmailRecoveryRunner:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs

    def sync(self) -> AliceGmailRecoverySummary:
        return AliceGmailRecoverySummary(
            emails_seen=3,
            emails_archived=2,
            emails_skipped=1,
            attachments_seen=5,
            attachments_uploaded=4,
            metadata_uploaded=3,
            bytes_uploaded=789,
        )


class FakeDriveIngestRunner:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs

    def sync(self) -> AliceVoiceRecordingsDriveIngestSummary:
        return AliceVoiceRecordingsDriveIngestSummary(
            metadata_seen=5,
            recordings_written=3,
            artifacts_written=8,
        )


class fake_exclusive_sync_lock:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs

    def __enter__(self) -> bool:
        return True

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None
