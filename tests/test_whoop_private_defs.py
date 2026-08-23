from __future__ import annotations

from contextlib import contextmanager

import pytest
from dagster import Failure, build_asset_context, build_schedule_context

from personal_data_warehouse.definitions import defs
from personal_data_warehouse.defs import whoop_private_sync as whoop_private_defs
from personal_data_warehouse.whoop_private_sync import (
    WHOOP_PRIVATE_COLLECTIONS,
    WhoopPrivateActionRequiredError,
    WhoopPrivateSyncSummary,
    whoop_private_credential_sha256,
)

def test_whoop_private_defs_are_registered() -> None:
    repository = defs().get_repository_def()

    assert "whoop_private_sync_job" in {job.name for job in repository.get_all_jobs()}
    assert "whoop_private_sync_every_fifteen_minutes" in {
        schedule.name for schedule in repository.schedule_defs
    }


def test_whoop_private_sync_schedule_runs_every_fifteen_minutes() -> None:
    schedule_def = whoop_private_defs.whoop_private_sync_every_fifteen_minutes

    assert schedule_def.cron_schedule == "*/15 * * * *"
    assert schedule_def.default_status == whoop_private_defs.whoop_private_schedule_default_status()


def test_schedule_default_status_needs_an_account_and_a_database(monkeypatch) -> None:
    for name in (
        "WHOOP_PRIVATE_ACCOUNT",
        "WHOOP_ACCOUNT",
        "GMAIL_ACCOUNTS",
        "POSTGRES_DATABASE_URL",
        "WHOOP_PRIVATE_ENABLED",
    ):
        monkeypatch.delenv(name, raising=False)
    assert whoop_private_defs.whoop_private_schedule_default_status().value == "STOPPED"

    # No client id/secret: the credential is a published browser session.
    monkeypatch.setenv("WHOOP_PRIVATE_ACCOUNT", "configured-account")
    monkeypatch.setenv("POSTGRES_DATABASE_URL", "postgresql://warehouse")
    assert whoop_private_defs.whoop_private_schedule_default_status().value == "RUNNING"

    monkeypatch.setenv("WHOOP_PRIVATE_ENABLED", "0")
    assert whoop_private_defs.whoop_private_schedule_default_status().value == "STOPPED"


def test_schedule_uses_the_active_run_guard(monkeypatch) -> None:
    calls = []

    def fake_skip_if_job_active(context, *, job_name):
        calls.append(job_name)
        return {}

    class Warehouse:
        def load_whoop_private_sync_state(self):
            return {}

        def load_whoop_private_session(self, *, account):
            return None

        def close(self):
            pass

    monkeypatch.setattr(whoop_private_defs, "skip_if_job_active", fake_skip_if_job_active)
    monkeypatch.setattr(
        whoop_private_defs,
        "load_settings",
        lambda **_kwargs: type(
            "Settings", (), {"whoop_private": type("Config", (), {"account": "a"})()}
        )(),
    )
    monkeypatch.setattr(whoop_private_defs, "warehouse_from_settings", lambda _settings: Warehouse())

    assert whoop_private_defs.whoop_private_sync_every_fifteen_minutes(build_schedule_context()) == {}
    assert calls == ["whoop_private_sync_job"]


def _rejected_state(account: str, refresh_token: str) -> dict:
    return {
        (account, collection): {
            "status": "action_required",
            "credential_sha256": whoop_private_credential_sha256(refresh_token),
        }
        for collection in WHOOP_PRIVATE_COLLECTIONS
    }


def _schedule_with_session(monkeypatch, *, stored_refresh_token: str, state: dict):
    class Config:
        account = "configured-account"
        session_key = "default"

    class Settings:
        whoop_private = Config()

    class Warehouse:
        closed = False

        def load_whoop_private_sync_state(self):
            return state

        def load_whoop_private_session(self, *, account):
            return {
                "account": account,
                "access_token": "access",
                "refresh_token": stored_refresh_token,
                "access_expires_at": "2026-08-24T00:00:00Z",
                "refresh_expires_at": "2026-09-20T00:00:00Z",
            }

        def close(self):
            Warehouse.closed = True

    monkeypatch.setattr(whoop_private_defs, "skip_if_job_active", lambda *_a, **_k: {})
    monkeypatch.setattr(whoop_private_defs, "load_settings", lambda **_kwargs: Settings())
    monkeypatch.setattr(whoop_private_defs, "warehouse_from_settings", lambda _settings: Warehouse())
    result = whoop_private_defs.whoop_private_sync_every_fifteen_minutes(build_schedule_context())
    return result, Warehouse


def test_schedule_skips_a_rejected_session_until_a_new_one_is_published(monkeypatch) -> None:
    result, warehouse = _schedule_with_session(
        monkeypatch,
        stored_refresh_token="revoked-refresh",
        state=_rejected_state("configured-account", "revoked-refresh"),
    )

    assert result.skip_message is not None
    assert "publish-session" in result.skip_message
    assert warehouse.closed is True


def test_schedule_resumes_immediately_for_a_freshly_published_session(monkeypatch) -> None:
    result, _warehouse = _schedule_with_session(
        monkeypatch,
        stored_refresh_token="freshly-published-refresh",
        state=_rejected_state("configured-account", "revoked-refresh"),
    )

    assert result == {}


def _patch_runner(monkeypatch, runner_cls) -> None:
    class Config:
        enabled = True

    class Settings:
        whoop_private = Config()

    @contextmanager
    def lock(**_kwargs):
        yield True

    monkeypatch.setattr(whoop_private_defs, "load_settings", lambda **_kwargs: Settings())
    monkeypatch.setattr(whoop_private_defs, "warehouse_from_settings", lambda _settings: object())
    monkeypatch.setattr(whoop_private_defs, "exclusive_sync_lock", lock)
    monkeypatch.setattr(whoop_private_defs, "WhoopPrivateSyncRunner", runner_cls)


def test_asset_emits_redacted_summary_metadata(monkeypatch) -> None:
    class Runner:
        def __init__(self, **_kwargs):
            pass

        def sync_all(self):
            return [
                WhoopPrivateSyncSummary(
                    account="configured-account",
                    sync_type="mixed",
                    records_written=42,
                    collections={collection: 6 for collection in WHOOP_PRIVATE_COLLECTIONS},
                )
            ]

    _patch_runner(monkeypatch, Runner)

    result = whoop_private_defs.whoop_private_sync(build_asset_context())

    assert result.metadata["records_written"] == 42
    summary = result.metadata["whoop_private"].value[0]
    assert summary["has_token"] is False
    assert "token" not in str(summary).lower().replace("has_token", "")


def test_asset_does_not_materialize_a_dead_session_as_success(monkeypatch) -> None:
    class Runner:
        def __init__(self, **_kwargs):
            pass

        def sync_all(self):
            raise WhoopPrivateActionRequiredError("run `pdw whoop publish-session`")

    _patch_runner(monkeypatch, Runner)

    with pytest.raises(Failure) as excinfo:
        whoop_private_defs.whoop_private_sync(build_asset_context())

    assert excinfo.value.allow_retries is False
    assert excinfo.value.metadata["action_required"].value is True


def test_asset_skips_when_the_source_is_disabled(monkeypatch) -> None:
    class Config:
        enabled = False

    class Settings:
        whoop_private = Config()

    def explode(_settings):
        raise AssertionError("a disabled source must not open a warehouse connection")

    monkeypatch.setattr(whoop_private_defs, "load_settings", lambda **_kwargs: Settings())
    monkeypatch.setattr(whoop_private_defs, "warehouse_from_settings", explode)

    result = whoop_private_defs.whoop_private_sync(build_asset_context())

    assert result.metadata["skipped"] == "WHOOP_PRIVATE_ENABLED is false"
