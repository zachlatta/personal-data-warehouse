from __future__ import annotations

import logging

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.defs import gmail_sync as gmail_sync_defs

LOGGER = logging.getLogger("test_gmail_sync_defs")


def _settings(monkeypatch, **env):
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@hackclub.com,zach@zachlatta.com")
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

    def fake_build_drive(*, account, settings):  # noqa: ANN001
        captured.append(account)
        return object()

    monkeypatch.setattr(gmail_sync_defs, "build_google_drive_service", fake_build_drive)

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

    def fake_build_drive(*, account, settings):  # noqa: ANN001
        captured.append(account)
        return object()

    monkeypatch.setattr(gmail_sync_defs, "build_google_drive_service", fake_build_drive)

    factory = gmail_sync_defs.build_attachment_object_store_factory(settings=settings, logger=LOGGER)
    assert factory is not None

    # Even the hackclub mailbox uploads via the shared account, not its own token.
    factory(settings.account_for_email("zach@hackclub.com"))

    assert captured == ["zach@zachlatta.com"]


def test_gmail_schedule_skips_when_prior_run_is_in_progress(monkeypatch) -> None:
    calls: list[tuple[object, str]] = []
    expected = object()

    def fake_skip_if_job_in_progress(context, *, job_name: str):
        calls.append((context, job_name))
        return expected

    monkeypatch.setattr(gmail_sync_defs, "skip_if_job_in_progress", fake_skip_if_job_in_progress)
    context = object()

    result = gmail_sync_defs.gmail_mailbox_sync_every_minute._execution_fn.decorated_fn(context)

    assert result is expected
    assert calls == [(context, "gmail_mailbox_sync_job")]
