from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import personal_data_warehouse.defs.slack_sync as slack_defs
from personal_data_warehouse.config import Settings
from personal_data_warehouse.defs.slack_sync import run_intelligent_slack_sync, slack_workspace_sync_every_minute
from personal_data_warehouse.slack_sync import SlackSyncSummary
from personal_data_warehouse.sync_locks import (
    exclusive_process_lock,
    exclusive_sync_lock,
    lock_env_prefix,
)


def test_lock_env_prefix_normalizes_names() -> None:
    assert lock_env_prefix("slack") == "SLACK"
    assert lock_env_prefix("slack-workspace") == "SLACK_WORKSPACE"


def test_slack_sync_schedule_runs_every_minute_by_default() -> None:
    assert slack_workspace_sync_every_minute.cron_schedule == "* * * * *"
    assert slack_workspace_sync_every_minute.default_status.value == "RUNNING"


def test_slack_asset_default_runs_intelligent_priority_cycle(monkeypatch) -> None:
    calls = []

    class FakeRunner:
        def __init__(self, **kwargs):
            calls.append(kwargs)

        def sync_all(self):
            return [
                SlackSyncSummary(
                    account="zrl",
                    team_id="T1",
                    sync_type="test",
                    conversations_seen=1,
                    messages_written=2,
                    users_written=0,
                    files_written=0,
                )
            ]

    monkeypatch.setattr(slack_defs, "SlackSyncRunner", FakeRunner)
    settings = Settings(
        clickhouse_url=None,
        gmail_accounts=(),
        gmail_oauth_client_secrets_json=None,
        gmail_scopes=(),
        gmail_page_size=500,
        gmail_include_spam_trash=True,
        gmail_force_full_sync=False,
        gmail_full_sync_query=None,
        slack_accounts=(),
        slack_page_size=200,
        slack_lookback_days=14,
        slack_thread_audit_days=30,
        slack_force_full_sync=False,
    )

    summaries = run_intelligent_slack_sync(
        settings=settings,
        warehouse=SimpleNamespace(),
        logger=SimpleNamespace(),
        now=datetime(2026, 4, 24, 17, 1, tzinfo=UTC),
    )

    assert len(summaries) == 5
    assert [call["conversation_types"] for call in calls[:4]] == [
        ("im",),
        ("mpim",),
        ("private_channel",),
        ("public_channel",),
    ]
    assert all(call["freshness_priority"] for call in calls[:4])
    assert all(call["sync_users"] is False for call in calls[:4])
    assert all(call["sync_members"] is False for call in calls[:4])
    assert all(call["sync_thread_replies"] is False for call in calls[:4])

    coverage_call = calls[4]
    assert coverage_call["use_existing_conversations"] is True
    assert coverage_call["conversation_types"] == ("mpim",)
    assert coverage_call["not_full_only"] is True
    assert coverage_call["skip_known_errors"] is True
    assert coverage_call["conversation_limit"] == 500
    assert coverage_call["settings"].slack_force_full_sync is True
    assert settings.slack_force_full_sync is False


def test_exclusive_process_lock_is_non_blocking(tmp_path) -> None:
    lock_path = tmp_path / "sync.lock"

    with exclusive_process_lock(lock_path) as first_acquired:
        assert first_acquired
        with exclusive_process_lock(lock_path) as second_acquired:
            assert not second_acquired


def test_exclusive_sync_lock_uses_named_postgres_url(monkeypatch) -> None:
    calls: list[tuple[str, int]] = []

    @contextmanager
    def fake_postgres_lock(postgres_url: str, lock_id: int):
        calls.append((postgres_url, lock_id))
        yield True

    monkeypatch.setenv("SLACK_SYNC_LOCK_POSTGRES_URL", "postgresql://postgres/slack")
    monkeypatch.setattr(
        "personal_data_warehouse.sync_locks.exclusive_postgres_advisory_lock",
        fake_postgres_lock,
    )

    with exclusive_sync_lock(name="slack", postgres_lock_id=1234) as acquired:
        assert acquired

    assert calls == [("postgresql://postgres/slack", 1234)]
