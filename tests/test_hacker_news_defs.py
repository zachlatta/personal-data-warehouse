from __future__ import annotations

from contextlib import contextmanager

from dagster import (
    DagsterInstance,
    MaterializeResult,
    RunRequest,
    SkipReason,
    build_asset_context,
    build_sensor_context,
)

from personal_data_warehouse.definitions import defs
from personal_data_warehouse.defs import hacker_news_sync as hn_defs
from personal_data_warehouse.hacker_news_sync import HackerNewsSyncSummary


class FakeConfig:
    account = "zachlatta"
    session_key = "default"
    api_base_url = "https://api.test"
    site_base_url = "https://site.test"
    poll_interval_seconds = 1800
    max_item_fetches_per_run = 10
    max_list_pages_per_run = 2
    full_walk_interval_seconds = 3600
    live_window_days = 3
    refresh_min_age_hours = 6
    request_timeout_seconds = 5
    enabled = True


class FakeSettings:
    def __init__(self, config):
        self.hacker_news = config


class FakeWarehouse:
    def __init__(self, session_row):
        self.session_row = session_row
        self.closed = False

    def get_hacker_news_session(self, *, account, session_key):
        return self.session_row

    def close(self):
        self.closed = True


class FakeRunner:
    calls: list[dict] = []

    def __init__(self, **kwargs):
        FakeRunner.calls.append(kwargs)

    def sync(self):
        return HackerNewsSyncSummary(account="zachlatta", items_fetched=3)


@contextmanager
def _acquired(**_kwargs):
    yield True


def _patch(monkeypatch, *, session_row):
    monkeypatch.setattr(hn_defs, "load_settings", lambda **_k: FakeSettings(FakeConfig()))
    monkeypatch.setattr(hn_defs, "warehouse_from_settings", lambda _s: FakeWarehouse(session_row))
    monkeypatch.setattr(hn_defs, "HackerNewsSyncRunner", FakeRunner)
    monkeypatch.setattr(hn_defs, "exclusive_sync_lock", _acquired)
    FakeRunner.calls.clear()


def test_repository_includes_hacker_news_definitions() -> None:
    repository = defs().get_repository_def()
    assert "hacker_news_sync_sensor" in {s.name for s in repository.sensor_defs}
    assert "hacker_news_sync_job" in {j.name for j in repository.get_all_jobs()}


def test_the_asset_passes_the_cookie_and_its_fingerprint(monkeypatch) -> None:
    _patch(monkeypatch, session_row={"session_token": "user=z&t", "token_sha256": "sha", "expired_token_sha256": ""})
    result = hn_defs.hacker_news_sync(build_asset_context())
    assert isinstance(result, MaterializeResult)
    call = FakeRunner.calls[0]
    assert call["client"]._cookie == "user=z&t"
    assert call["session_token_sha256"]
    assert result.metadata["items_fetched"].value == 3


def test_a_known_rejected_cookie_runs_public_only(monkeypatch) -> None:
    _patch(monkeypatch, session_row={"session_token": "user=z&t", "token_sha256": "sha", "expired_token_sha256": "sha"})
    hn_defs.hacker_news_sync(build_asset_context())
    call = FakeRunner.calls[0]
    assert call["client"]._cookie == ""
    assert call["session_token_sha256"] == ""


def test_no_session_at_all_still_runs_the_public_half(monkeypatch) -> None:
    _patch(monkeypatch, session_row=None)
    hn_defs.hacker_news_sync(build_asset_context())
    assert FakeRunner.calls[0]["client"]._cookie == ""


def test_sensor_skips_until_configured(monkeypatch) -> None:
    monkeypatch.delenv("HACKER_NEWS_ACCOUNT", raising=False)
    monkeypatch.setattr(hn_defs, "skip_if_job_in_progress", lambda *_a, **_k: {})
    with DagsterInstance.ephemeral() as instance:
        result = hn_defs.hacker_news_sync_sensor(build_sensor_context(instance=instance))
    assert isinstance(result, SkipReason)
    assert "HACKER_NEWS_ACCOUNT" in str(result.skip_message)


def test_sensor_honours_the_poll_interval(monkeypatch) -> None:
    monkeypatch.setenv("HACKER_NEWS_ACCOUNT", "zachlatta")
    monkeypatch.setenv("POSTGRES_DATABASE_URL", "postgresql://x/y")
    monkeypatch.setattr(hn_defs, "skip_if_job_in_progress", lambda *_a, **_k: {})
    monkeypatch.setattr(hn_defs, "load_settings", lambda **_k: FakeSettings(FakeConfig()))
    with DagsterInstance.ephemeral() as instance:
        first = hn_defs.hacker_news_sync_sensor(build_sensor_context(instance=instance))
        assert isinstance(first, RunRequest)
        recent = build_sensor_context(instance=instance, cursor=str(__import__("time").time()))
        second = hn_defs.hacker_news_sync_sensor(recent)
    assert isinstance(second, SkipReason)
