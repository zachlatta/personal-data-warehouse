from __future__ import annotations

import pytest

from personal_data_warehouse.config import (
    GOOGLE_DRIVE_SCOPE,
    GoogleDriveSourceConfig,
    load_settings,
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    # Strip anything that would make load_settings reach for real credentials or
    # enable unrelated sources during these unit tests.
    for key in list(__import__("os").environ):
        if key.startswith(("GMAIL_", "GOOGLE_", "VOICE_MEMOS_", "APPLE_", "WHATSAPP_", "AGENT_", "ALICE_", "SLACK_", "CONTACT", "CALENDAR")):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@zachlatta.com,zach@hackclub.com")
    monkeypatch.setenv("POSTGRES_DATABASE_URL", "postgresql://localhost/x")


def test_source_enabled_by_default_for_gmail_accounts(monkeypatch):
    # No opt-in env var needed: enabled by default for the GMAIL_ACCOUNTS.
    settings = load_settings(require_postgres=False)
    source = settings.google_drive_source
    assert isinstance(source, GoogleDriveSourceConfig)
    assert source.accounts == ("zach@zachlatta.com", "zach@hackclub.com")
    assert GOOGLE_DRIVE_SCOPE in settings.google_scopes


def test_source_can_be_disabled(monkeypatch):
    monkeypatch.setenv("GOOGLE_DRIVE_SOURCE_ENABLED", "0")
    settings = load_settings(require_postgres=False)
    assert settings.google_drive_source is None
    assert GOOGLE_DRIVE_SCOPE not in settings.google_scopes


def test_transport_folders_excluded_by_default(monkeypatch):
    monkeypatch.setenv("VOICE_MEMOS_GOOGLE_DRIVE_FOLDER_ID", "transport_voice")
    monkeypatch.setenv("WHATSAPP_GOOGLE_DRIVE_FOLDER_ID", "transport_whatsapp")
    monkeypatch.setenv("GOOGLE_DRIVE_EXCLUDE_FOLDER_IDS", "manual_extra")
    settings = load_settings(require_postgres=False)
    source = settings.google_drive_source
    assert "manual_extra" in source.exclude_folder_ids
    assert "transport_voice" in source.exclude_folder_ids
    assert "transport_whatsapp" in source.exclude_folder_ids
    # No duplicates even though several sources fall back to the same folder id.
    assert len(source.exclude_folder_ids) == len(set(source.exclude_folder_ids))


def test_default_text_max_chars(monkeypatch):
    settings = load_settings(require_postgres=False)
    assert settings.google_drive_source.text_max_chars == 5_000_000


def test_explicit_accounts_and_flags(monkeypatch):
    monkeypatch.setenv("GOOGLE_DRIVE_SOURCE_ACCOUNTS", "zach@hackclub.com")
    monkeypatch.setenv("GOOGLE_DRIVE_SOURCE_INCLUDE_SHARED_WITH_ME", "true")
    monkeypatch.setenv("GOOGLE_DRIVE_SOURCE_BINARY_EXTRACTION", "1")
    monkeypatch.setenv("GOOGLE_DRIVE_SOURCE_TEXT_MAX_CHARS", "5000")
    settings = load_settings(require_postgres=False)
    source = settings.google_drive_source
    assert source.accounts == ("zach@hackclub.com",)
    assert source.include_shared_with_me is True
    assert source.include_shared_drives is False
    assert source.binary_extraction_enabled is True
    assert source.text_max_chars == 5000
