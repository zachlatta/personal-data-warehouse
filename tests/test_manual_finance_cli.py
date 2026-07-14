"""CLI + config wiring for `pdw ingest manual-finance`."""

from __future__ import annotations

from pathlib import Path

import pytest

from personal_data_warehouse.config import load_settings
from personal_data_warehouse_manual_finance import cli
from personal_data_warehouse_manual_finance.sync import ManualFinanceUploadSummary


def test_load_settings_builds_manual_finance_config(monkeypatch):
    for key in list(__import__("os").environ):
        if key.startswith(("MANUAL_FINANCE_", "PHOTOS_", "VOICE_MEMOS_", "GMAIL_")):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("MANUAL_FINANCE_ACCOUNT", "z@x.test")
    monkeypatch.setenv("MANUAL_FINANCE_STORAGE_BACKEND", "http_app")
    settings = load_settings(require_postgres=False, require_gmail=False, require_manual_finance=True)
    assert settings.manual_finance is not None
    assert settings.manual_finance.account == "z@x.test"
    assert settings.manual_finance.storage_backend == "http_app"


def test_load_settings_manual_finance_absent_without_env(monkeypatch):
    for key in list(__import__("os").environ):
        if key.startswith("MANUAL_FINANCE_"):
            monkeypatch.delenv(key, raising=False)
    settings = load_settings(require_postgres=False, require_gmail=False)
    assert settings.manual_finance is None


def test_cli_runs_uploader_with_parsed_args(monkeypatch, tmp_path):
    root = tmp_path / "accounts"
    (root / "acme-checking-0001").mkdir(parents=True)
    (root / "acme-checking-0001" / "statement.pdf").write_bytes(b"%PDF")

    monkeypatch.setenv("MANUAL_FINANCE_ACCOUNT", "z@x.test")
    monkeypatch.setenv("MANUAL_FINANCE_STORAGE_BACKEND", "http_app")

    captured: dict = {}

    class FakeRunner:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def sync(self):
            return ManualFinanceUploadSummary(
                files_seen=1,
                files_ignored=0,
                files_selected=1,
                files_skipped=0,
                files_uploaded=1,
                metadata_uploaded=1,
            )

    monkeypatch.setattr(cli, "ManualFinanceUploadRunner", FakeRunner)
    monkeypatch.setattr(cli, "ingest_client_from_env", lambda: object())
    cli.main(
        [
            str(root),
            "--limit",
            "5",
            "--state-file",
            str(tmp_path / "state.sqlite"),
            "--lock-file",
            str(tmp_path / "state.lock"),
        ]
    )
    assert captured["account"] == "z@x.test"
    assert captured["paths"] == [root]
    assert captured["limit"] == 5
    assert captured["mode"] == "incremental"
    assert captured["upload_state"] is not None
