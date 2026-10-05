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


def test_backfill_fetches_with_each_accounts_slack_token() -> None:
    from types import SimpleNamespace

    from personal_data_warehouse.defs import slack_file_fingerprints as fingerprint_defs

    settings = SimpleNamespace(
        slack_accounts=(
            SimpleNamespace(account="zrl", token="xoxp-a"),
            SimpleNamespace(account="other", token=""),
        )
    )
    assert fingerprint_defs.slack_tokens_by_account(settings) == {"zrl": "xoxp-a"}


def test_run_limit_is_bounded_by_default_and_env_overridable(monkeypatch) -> None:
    from personal_data_warehouse.defs import slack_file_fingerprints as fingerprint_defs

    monkeypatch.delenv(fingerprint_defs.SLACK_FILE_FINGERPRINT_LIMIT_ENV, raising=False)
    default = fingerprint_defs.slack_file_fingerprint_limit()
    assert 0 < default <= 2000, "an unbounded default would sweep 552 GB in one run"

    monkeypatch.setenv(fingerprint_defs.SLACK_FILE_FINGERPRINT_LIMIT_ENV, "7")
    assert fingerprint_defs.slack_file_fingerprint_limit() == 7


def test_defaults_fetch_thumbnails_gently_and_cap_audited_downloads(monkeypatch) -> None:
    """~6,500 full downloads a day (300 an hour in a five-minute burst) drew an
    `excessive_downloads` anomaly every three hours until 2026-10-05. Thumbnails
    are not audited; the rare full download is capped per run, and fetches are
    spaced so a run never bursts."""
    from personal_data_warehouse.defs import slack_file_fingerprints as fingerprint_defs

    for name in (
        fingerprint_defs.SLACK_FILE_FINGERPRINT_LIMIT_ENV,
        fingerprint_defs.SLACK_FILE_FINGERPRINT_RUN_SECONDS_ENV,
        fingerprint_defs.SLACK_FILE_FINGERPRINT_SPACING_SECONDS_ENV,
        fingerprint_defs.SLACK_FILE_FINGERPRINT_MAX_FULL_DOWNLOADS_ENV,
    ):
        monkeypatch.delenv(name, raising=False)
    assert fingerprint_defs.slack_file_fingerprint_max_full_downloads() <= 5
    assert fingerprint_defs.slack_file_fingerprint_spacing_seconds() >= 0.5
    assert fingerprint_defs.slack_file_fingerprint_run_seconds() < 3600

    monkeypatch.setenv(fingerprint_defs.SLACK_FILE_FINGERPRINT_SPACING_SECONDS_ENV, "2.5")
    assert fingerprint_defs.slack_file_fingerprint_spacing_seconds() == 2.5
    monkeypatch.setenv(fingerprint_defs.SLACK_FILE_FINGERPRINT_MAX_FULL_DOWNLOADS_ENV, "0")
    assert fingerprint_defs.slack_file_fingerprint_max_full_downloads() == 0
