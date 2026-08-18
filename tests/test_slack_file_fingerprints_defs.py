"""Dagster wiring for the Slack image fingerprint backfill."""

from __future__ import annotations

import pytest

from personal_data_warehouse.definitions import defs


def test_repository_registers_the_fingerprint_pipeline() -> None:
    repository = defs().get_repository_def()

    assert repository.has_job("slack_file_fingerprints_job")
    assert "slack_file_fingerprints_hourly" in {
        schedule.name for schedule in repository.schedule_defs
    }


def test_schedule_is_offset_from_slack_sync_so_they_do_not_compete() -> None:
    from personal_data_warehouse.defs import slack_file_fingerprints as fingerprint_defs

    cron = fingerprint_defs.slack_file_fingerprints_hourly.cron_schedule

    assert cron.split()[0] not in {"*", "0"}, cron


def test_backfill_needs_app_credentials_not_a_slack_token(monkeypatch) -> None:
    """Slack file resolution lives in the app; this asset holds no Slack secret."""
    from personal_data_warehouse.defs import slack_file_fingerprints as fingerprint_defs

    for name in ("PDW_API_URL", "MCP_BASE_URL", "PDW_SECRET_TOKEN", "MCP_SECRET_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    assert fingerprint_defs.app_credentials() == ("", "")

    monkeypatch.setenv("PDW_API_URL", "https://app.example")
    monkeypatch.setenv("PDW_SECRET_TOKEN", "t0ken")
    assert fingerprint_defs.app_credentials() == ("https://app.example", "t0ken")


def test_run_limit_is_bounded_by_default_and_env_overridable(monkeypatch) -> None:
    from personal_data_warehouse.defs import slack_file_fingerprints as fingerprint_defs

    monkeypatch.delenv(fingerprint_defs.SLACK_FILE_FINGERPRINT_LIMIT_ENV, raising=False)
    default = fingerprint_defs.slack_file_fingerprint_limit()
    assert 0 < default <= 2000, "an unbounded default would sweep 552 GB in one run"

    monkeypatch.setenv(fingerprint_defs.SLACK_FILE_FINGERPRINT_LIMIT_ENV, "7")
    assert fingerprint_defs.slack_file_fingerprint_limit() == 7
