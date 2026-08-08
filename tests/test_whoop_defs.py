from __future__ import annotations

from contextlib import contextmanager

from dagster import build_asset_context, build_schedule_context

from personal_data_warehouse.definitions import defs
from personal_data_warehouse.defs import whoop_sync as whoop_defs
from personal_data_warehouse.whoop_sync import (
    WhoopSyncSummary,
    whoop_credential_sha256,
)


def test_whoop_defs_are_registered() -> None:
    repository = defs().get_repository_def()

    assert "whoop_sync_job" in {job.name for job in repository.get_all_jobs()}
    assert "whoop_sync_every_five_minutes" in {schedule.name for schedule in repository.schedule_defs}


def test_whoop_sync_schedule_runs_every_five_minutes() -> None:
    assert whoop_defs.whoop_sync_every_five_minutes.cron_schedule == "*/5 * * * *"
    assert whoop_defs.whoop_sync_every_five_minutes.default_status == whoop_defs.whoop_schedule_default_status()


def test_whoop_schedule_default_status_requires_credentials(monkeypatch) -> None:
    for name in ("WHOOP_ACCOUNT", "GMAIL_ACCOUNTS", "WHOOP_CLIENT_ID", "WHOOP_CLIENT_SECRET", "WHOOP_TOKEN_JSON", "WHOOP_TOKEN_JSON_B64"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv("WHOOP_ENABLED", raising=False)
    assert whoop_defs.whoop_schedule_default_status().value == "STOPPED"

    monkeypatch.setenv("WHOOP_ACCOUNT", "configured-account")
    monkeypatch.setenv("WHOOP_ACCOUNT", "configured-account")
    monkeypatch.setenv("WHOOP_CLIENT_ID", "client-id")
    monkeypatch.setenv("WHOOP_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv("WHOOP_TOKEN_JSON_B64", "encoded-token")

    assert whoop_defs.whoop_schedule_default_status().value == "RUNNING"

    monkeypatch.setenv("WHOOP_ENABLED", "0")
    assert whoop_defs.whoop_schedule_default_status().value == "STOPPED"


def test_whoop_schedule_uses_active_run_guard(monkeypatch) -> None:
    calls = []

    def fake_skip_if_job_active(context, *, job_name):
        calls.append(job_name)
        return {}

    monkeypatch.setattr(whoop_defs, "skip_if_job_active", fake_skip_if_job_active)
    monkeypatch.setattr(whoop_defs, "load_settings", lambda **_kwargs: type(
        "Settings", (), {"whoop": type("Whoop", (), {"account": "a", "token_json": ""})()}
    )())

    class Warehouse:
        def load_whoop_sync_state(self):
            return {}

        def load_whoop_oauth_token(self, *, account):
            return ""

        def close(self):
            pass

    monkeypatch.setattr(whoop_defs, "warehouse_from_settings", lambda _settings: Warehouse())

    assert whoop_defs.whoop_sync_every_five_minutes(build_schedule_context()) == {}
    assert calls == ["whoop_sync_job"]


def test_whoop_schedule_skips_rejected_credential_until_it_changes(monkeypatch) -> None:
    token = '{"access_token":"expired","refresh_token":"revoked","expires_at":1}'
    fingerprint = whoop_credential_sha256(token)

    class WhoopConfig:
        account = "configured-account"
        token_json = token

    class Settings:
        whoop = WhoopConfig()

    class Warehouse:
        closed = False

        def load_whoop_sync_state(self):
            return {
                ("configured-account", collection): {
                    "status": "action_required",
                    "credential_sha256": fingerprint,
                }
                for collection in (
                    "profile",
                    "body_measurement",
                    "cycles",
                    "recovery",
                    "sleep",
                    "workout",
                )
            }

        def load_whoop_oauth_token(self, *, account):
            return token

        def close(self):
            self.closed = True

    warehouse = Warehouse()
    monkeypatch.setattr(whoop_defs, "skip_if_job_active", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(whoop_defs, "load_settings", lambda **_kwargs: Settings())
    monkeypatch.setattr(whoop_defs, "warehouse_from_settings", lambda _settings: warehouse)

    result = whoop_defs.whoop_sync_every_five_minutes(build_schedule_context())

    assert result.skip_message is not None
    assert "re-authorization" in result.skip_message
    assert warehouse.closed is True


def test_whoop_schedule_resumes_immediately_for_a_new_credential(monkeypatch) -> None:
    old_token = '{"access_token":"expired","refresh_token":"revoked","expires_at":1}'
    new_token = '{"access_token":"new","refresh_token":"new-refresh","expires_at":2}'

    class WhoopConfig:
        account = "configured-account"
        token_json = new_token

    class Settings:
        whoop = WhoopConfig()

    class Warehouse:
        def load_whoop_sync_state(self):
            return {
                ("configured-account", collection): {
                    "status": "action_required",
                    "credential_sha256": whoop_credential_sha256(old_token),
                }
                for collection in (
                    "profile",
                    "body_measurement",
                    "cycles",
                    "recovery",
                    "sleep",
                    "workout",
                )
            }

        def load_whoop_oauth_token(self, *, account):
            return old_token

        def close(self):
            pass

    monkeypatch.setattr(whoop_defs, "skip_if_job_active", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(whoop_defs, "load_settings", lambda **_kwargs: Settings())
    monkeypatch.setattr(whoop_defs, "warehouse_from_settings", lambda _settings: Warehouse())

    assert whoop_defs.whoop_sync_every_five_minutes(build_schedule_context()) == {}


def test_whoop_asset_emits_redacted_summary_metadata(monkeypatch) -> None:
    class WhoopConfig:
        enabled = True

    class Settings:
        whoop = WhoopConfig()

    class Runner:
        def __init__(self, **_kwargs):
            pass

        def sync_all(self):
            return [
                WhoopSyncSummary(
                    account="configured-account",
                    sync_type="mixed",
                    records_written=6,
                    collections={"profile": 1, "body_measurement": 1, "cycles": 1, "recovery": 1, "sleep": 1, "workout": 1},
                )
            ]

    @contextmanager
    def lock(**_kwargs):
        yield True

    monkeypatch.setattr(whoop_defs, "load_settings", lambda **_kwargs: Settings())
    monkeypatch.setattr(whoop_defs, "warehouse_from_settings", lambda _settings: object())
    monkeypatch.setattr(whoop_defs, "exclusive_sync_lock", lock)
    monkeypatch.setattr(whoop_defs, "WhoopSyncRunner", Runner)

    result = whoop_defs.whoop_sync(build_asset_context())

    assert result.metadata["records_written"] == 6
    summary = result.metadata["whoop"].value[0]
    assert summary["has_token"] is False
    assert "token" not in str(summary).lower().replace("has_token", "")
