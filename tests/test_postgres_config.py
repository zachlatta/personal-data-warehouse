from __future__ import annotations

import pytest

from personal_data_warehouse import config
from personal_data_warehouse.config import load_settings, normalize_postgres_url


def test_normalize_postgres_url_accepts_coolify_postgres_scheme() -> None:
    assert normalize_postgres_url("postgres://user:pass@example.test/db") == "postgresql://user:pass@example.test/db"
    assert (
        normalize_postgres_url("postgresql+psycopg2://user:pass@example.test/db")
        == "postgresql://user:pass@example.test/db"
    )


def test_load_settings_reads_postgres_database_url(monkeypatch) -> None:
    monkeypatch.setattr(config, "load_dotenv", lambda: None)
    monkeypatch.setenv("POSTGRES_DATABASE_URL", "postgres://user:pass@example.test/db")
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.test")

    settings = load_settings()

    assert settings.postgres_database_url == "postgresql://user:pass@example.test/db"


def test_load_settings_requires_postgres_url_for_runtime(monkeypatch) -> None:
    monkeypatch.setattr(config, "load_dotenv", lambda: None)
    monkeypatch.delenv("POSTGRES_DATABASE_URL", raising=False)
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.test")

    with pytest.raises(ValueError, match="POSTGRES_DATABASE_URL"):
        load_settings()


def test_load_settings_skips_warehouse_url_when_explicitly_disabled(monkeypatch) -> None:
    monkeypatch.setattr(config, "load_dotenv", lambda: None)
    monkeypatch.delenv("POSTGRES_DATABASE_URL", raising=False)
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.test")

    settings = load_settings(require_postgres=False)

    assert settings.postgres_database_url is None


def test_local_upload_configs_use_app_ingest_without_drive_folders(monkeypatch) -> None:
    monkeypatch.setattr(config, "load_dotenv", lambda: None)
    for name in (
        "VOICE_MEMOS_GOOGLE_DRIVE_FOLDER_ID",
        "VOICE_MEMOS_DRIVE_FOLDER_ID",
        "APPLE_NOTES_GOOGLE_DRIVE_FOLDER_ID",
        "APPLE_MESSAGES_GOOGLE_DRIVE_FOLDER_ID",
        "WHATSAPP_GOOGLE_DRIVE_FOLDER_ID",
        "AGENT_SESSIONS_GOOGLE_DRIVE_FOLDER_ID",
        "VOICE_MEMOS_STORAGE_BACKEND",
        "APPLE_NOTES_STORAGE_BACKEND",
        "APPLE_MESSAGES_STORAGE_BACKEND",
        "WHATSAPP_STORAGE_BACKEND",
        "AGENT_SESSIONS_STORAGE_BACKEND",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("MCP_BASE_URL", "https://pdw.example.test")
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.test")
    monkeypatch.setenv("VOICE_MEMOS_ACCOUNT", "zach@example.test")
    monkeypatch.setenv("APPLE_NOTES_ACCOUNT", "zach@example.test")
    monkeypatch.setenv("APPLE_MESSAGES_ACCOUNT", "zach@example.test")
    monkeypatch.setenv("WHATSAPP_ACCOUNT", "zach@example.test")
    monkeypatch.setenv("AGENT_SESSIONS_ACCOUNT", "zach@example.test")
    monkeypatch.setenv("GOOGLE_DRIVE_SOURCE_ENABLED", "0")

    settings = load_settings(
        require_postgres=False,
        require_gmail=False,
        require_voice_memos=True,
        require_apple_notes=True,
        require_apple_messages=True,
        require_whatsapp=True,
        require_agent_sessions=True,
    )

    assert settings.voice_memos is not None
    assert settings.apple_notes is not None
    assert settings.apple_messages is not None
    assert settings.whatsapp is not None
    assert settings.agent_sessions is not None
    assert settings.voice_memos.storage_backend == "http_app"
    assert settings.apple_notes.storage_backend == "http_app"
    assert settings.apple_messages.storage_backend == "http_app"
    assert settings.whatsapp.storage_backend == "http_app"
    assert settings.agent_sessions.storage_backend == "http_app"
    assert config.GOOGLE_DRIVE_SCOPE not in settings.google_scopes


def test_whatsapp_downloads_history_media_by_default(monkeypatch) -> None:
    monkeypatch.setattr(config, "load_dotenv", lambda: None)
    monkeypatch.delenv("WHATSAPP_DOWNLOAD_HISTORY_MEDIA", raising=False)
    monkeypatch.setenv("WHATSAPP_ACCOUNT", "zach@example.test")
    monkeypatch.setenv("WHATSAPP_STORAGE_BACKEND", "http_app")
    monkeypatch.setenv("MCP_BASE_URL", "https://pdw.example.test")

    settings = load_settings(require_postgres=False, require_gmail=False, require_whatsapp=True)

    assert settings.whatsapp is not None
    # All attachments (live + history sync) are downloaded to object storage by
    # default; the env var is only an escape hatch to turn it back off.
    assert settings.whatsapp.download_history_media is True


def test_whatsapp_history_media_download_can_be_disabled(monkeypatch) -> None:
    monkeypatch.setattr(config, "load_dotenv", lambda: None)
    monkeypatch.setenv("WHATSAPP_DOWNLOAD_HISTORY_MEDIA", "0")
    monkeypatch.setenv("WHATSAPP_ACCOUNT", "zach@example.test")
    monkeypatch.setenv("WHATSAPP_STORAGE_BACKEND", "http_app")
    monkeypatch.setenv("MCP_BASE_URL", "https://pdw.example.test")

    settings = load_settings(require_postgres=False, require_gmail=False, require_whatsapp=True)

    assert settings.whatsapp is not None
    assert settings.whatsapp.download_history_media is False


def test_local_upload_configs_use_main_api_url_without_drive_folders(monkeypatch) -> None:
    monkeypatch.setattr(config, "load_dotenv", lambda: None)
    for name in (
        "PDW_INGEST_BASE_URL",
        "MCP_BASE_URL",
        "VOICE_MEMOS_GOOGLE_DRIVE_FOLDER_ID",
        "VOICE_MEMOS_DRIVE_FOLDER_ID",
        "VOICE_MEMOS_STORAGE_BACKEND",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("PDW_API_URL", "https://pdw.example.test")
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.test")
    monkeypatch.setenv("VOICE_MEMOS_ACCOUNT", "zach@example.test")
    monkeypatch.setenv("GOOGLE_DRIVE_SOURCE_ENABLED", "0")

    settings = load_settings(require_postgres=False, require_gmail=False, require_voice_memos=True)

    assert settings.voice_memos is not None
    assert settings.voice_memos.storage_backend == "http_app"
    assert config.GOOGLE_DRIVE_SCOPE not in settings.google_scopes


def test_drive_reader_folder_keeps_drive_scope_with_app_url(monkeypatch) -> None:
    monkeypatch.setattr(config, "load_dotenv", lambda: None)
    monkeypatch.delenv("VOICE_MEMOS_STORAGE_BACKEND", raising=False)
    monkeypatch.setenv("MCP_BASE_URL", "https://pdw.example.test")
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.test")
    monkeypatch.setenv("VOICE_MEMOS_ACCOUNT", "zach@example.test")
    monkeypatch.setenv("VOICE_MEMOS_GOOGLE_DRIVE_FOLDER_ID", "drive-folder")

    settings = load_settings(require_postgres=False, require_gmail=False, require_voice_memos=True)

    assert settings.voice_memos is not None
    assert settings.voice_memos.storage_backend == "google_drive"
    assert config.GOOGLE_DRIVE_SCOPE in settings.google_scopes
