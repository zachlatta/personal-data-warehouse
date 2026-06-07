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
