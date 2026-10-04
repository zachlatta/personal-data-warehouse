from __future__ import annotations

import logging

from dagster import DagsterInstance, RunRequest, SkipReason, build_sensor_context

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.defs import gmail_sync as gmail_sync_defs
from personal_data_warehouse.objectstore import google_drive as objectstore_google_drive

LOGGER = logging.getLogger("test_gmail_sync_defs")


def _settings(monkeypatch, **env):
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@hackclub.com,zach@zachlatta.com")
    defaults = {
        "GOOGLE_DRIVE_SOURCE_ENABLED": "0",
        "VOICE_MEMOS_ACCOUNT": None,
        "VOICE_MEMOS_GOOGLE_DRIVE_FOLDER_ID": None,
        "VOICE_MEMOS_DRIVE_FOLDER_ID": None,
        "ALICE_VOICE_RECORDINGS_ACCOUNT": None,
        "ALICE_API_KEY_ID": None,
        "ALICE_API_SECRET_KEY": None,
        "ALICE_VOICE_RECORDINGS_GOOGLE_DRIVE_ACCOUNT": None,
        "ALICE_VOICE_RECORDINGS_GOOGLE_DRIVE_FOLDER_ID": None,
    }
    for name, value in defaults.items():
        if name in env:
            continue
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    for name, value in env.items():
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    return load_settings(require_postgres=False, require_gmail_client_secrets=False)


def test_factory_is_none_when_storage_disabled(monkeypatch) -> None:
    settings = _settings(
        monkeypatch,
        GMAIL_ATTACHMENT_GOOGLE_DRIVE_FOLDER_ID=None,
        VOICE_MEMOS_GOOGLE_DRIVE_FOLDER_ID=None,
        VOICE_MEMOS_DRIVE_FOLDER_ID=None,
    )

    factory = gmail_sync_defs.build_attachment_object_store_factory(settings=settings, logger=LOGGER)

    assert factory is None


def test_factory_uses_configured_drive_account_for_every_mailbox(monkeypatch) -> None:
    settings = _settings(
        monkeypatch,
        GMAIL_ATTACHMENT_GOOGLE_DRIVE_FOLDER_ID="folder-xyz",
        GMAIL_ATTACHMENT_GOOGLE_DRIVE_ACCOUNT="zach@zachlatta.com",
    )

    captured: list[str] = []

    def fake_build_drive(*, account, settings, request_timeout_seconds=30):  # noqa: ANN001
        captured.append(account)
        return object()

    monkeypatch.setattr(objectstore_google_drive, "build_google_drive_service", fake_build_drive)

    factory = gmail_sync_defs.build_attachment_object_store_factory(settings=settings, logger=LOGGER)
    assert factory is not None

    # A mailbox whose own OAuth project lacks Drive must still upload via the configured account.
    store = factory(settings.account_for_email("zach@hackclub.com"))

    assert store.backend == "google_drive"
    assert captured == ["zach@zachlatta.com"]


def test_factory_uses_shared_voice_memos_account_when_attachment_account_unset(monkeypatch) -> None:
    settings = _settings(
        monkeypatch,
        GMAIL_ATTACHMENT_GOOGLE_DRIVE_FOLDER_ID=None,
        GMAIL_ATTACHMENT_GOOGLE_DRIVE_ACCOUNT=None,
        VOICE_MEMOS_ACCOUNT="zach@zachlatta.com",
        VOICE_MEMOS_GOOGLE_DRIVE_FOLDER_ID="shared-folder-id",
    )

    # Falls back to the shared voice-memos store, so storage is enabled by default.
    assert settings.gmail_attachment_storage_enabled is True
    assert settings.gmail_attachment_google_drive_folder_id == "shared-folder-id"

    captured: list[str] = []

    def fake_build_drive(*, account, settings, request_timeout_seconds=30):  # noqa: ANN001
        captured.append(account)
        return object()

    monkeypatch.setattr(objectstore_google_drive, "build_google_drive_service", fake_build_drive)

    factory = gmail_sync_defs.build_attachment_object_store_factory(settings=settings, logger=LOGGER)
    assert factory is not None

    # Even the hackclub mailbox uploads via the shared account, not its own token.
    factory(settings.account_for_email("zach@hackclub.com"))

    assert captured == ["zach@zachlatta.com"]


def test_gmail_keepalive_sensor_skips_while_a_poll_run_is_alive(monkeypatch) -> None:
    calls: list[str] = []
    expected = SkipReason("busy")

    def fake_skip_if_job_in_progress(context, *, job_name: str):
        calls.append(job_name)
        return expected

    monkeypatch.setattr(gmail_sync_defs, "skip_if_job_in_progress", fake_skip_if_job_in_progress)

    with DagsterInstance.ephemeral() as instance:
        result = gmail_sync_defs.gmail_mailbox_sync_keepalive_sensor(build_sensor_context(instance=instance))

    assert result is expected
    assert calls == ["gmail_mailbox_sync_job"]


def test_gmail_keepalive_sensor_relaunches_the_poll_loop_when_no_run_is_alive() -> None:
    with DagsterInstance.ephemeral() as instance:
        result = gmail_sync_defs.gmail_mailbox_sync_keepalive_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, RunRequest)
    assert result.tags == {"gmail_trigger": "keepalive"}
    assert gmail_sync_defs.gmail_mailbox_sync_keepalive_sensor.minimum_interval_seconds == 30
    assert gmail_sync_defs.gmail_mailbox_sync_keepalive_sensor.default_status.value == "RUNNING"


def test_gmail_keepalive_sensor_waits_out_a_crash_loop(monkeypatch) -> None:
    monkeypatch.setattr(
        gmail_sync_defs,
        "finished_job_runs",
        lambda _instance, *, job_name: [("FAILURE", 1000.0), ("FAILURE", 900.0), ("FAILURE", 800.0)],
    )
    monkeypatch.setattr(gmail_sync_defs.time, "time", lambda: 1060.0)

    with DagsterInstance.ephemeral() as instance:
        result = gmail_sync_defs.gmail_mailbox_sync_keepalive_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, SkipReason)
    assert "gmail_mailbox_sync_job failed 3 consecutive runs" in result.skip_message


def test_the_poll_window_fits_under_the_jobs_runtime_cap(monkeypatch) -> None:
    from personal_data_warehouse.gmail_sync import GmailPollConfig

    monkeypatch.delenv("GMAIL_POLL_WINDOW_SECONDS", raising=False)
    cap = int(gmail_sync_defs.gmail_mailbox_sync_job.tags["dagster/max_runtime"])
    # Room for a stale-cursor full sync (~9 min) that starts at the window's end.
    assert GmailPollConfig.from_env().window_seconds + 600 <= cap


def test_gmail_is_no_longer_driven_by_a_cron_schedule() -> None:
    definitions = gmail_sync_defs.defs()
    assert list(definitions.schedules or []) == []
