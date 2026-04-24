from __future__ import annotations

from contextlib import contextmanager

from personal_data_warehouse.defs.slack_sync import slack_workspace_sync_every_minute
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
