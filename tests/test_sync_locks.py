from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import personal_data_warehouse.defs.slack_sync as slack_defs
from personal_data_warehouse.config import Settings
from personal_data_warehouse.defs.slack_sync import (
    _run_locked_slack_stage,
    run_intelligent_slack_sync,
    run_slack_coverage_sync,
    run_slack_freshness_sync,
    run_slack_member_sync,
    run_slack_metadata_sync,
    run_slack_read_state_sync,
    run_slack_thread_backfill_sync,
    run_slack_thread_sync,
    run_slack_user_sync,
    slack_workspace_member_sync_hourly,
    slack_workspace_coverage_sync_every_seven_minutes,
    slack_workspace_metadata_sync_every_fifteen_minutes,
    slack_workspace_read_state_sync_every_five_minutes,
    slack_workspace_sync_every_five_minutes,
    slack_workspace_thread_sync_every_five_minutes,
    slack_workspace_user_sync_daily,
)
from personal_data_warehouse.slack_sync import SlackSyncSummary
from personal_data_warehouse.sync_locks import (
    exclusive_postgres_advisory_lock,
    exclusive_process_lock,
    exclusive_sync_lock,
    lock_env_prefix,
)


def test_lock_env_prefix_normalizes_names() -> None:
    assert lock_env_prefix("slack") == "SLACK"
    assert lock_env_prefix("slack-workspace") == "SLACK_WORKSPACE"


def test_slack_sync_schedule_runs_every_five_minutes_by_default() -> None:
    assert slack_workspace_sync_every_five_minutes.cron_schedule == "*/5 * * * *"
    assert slack_workspace_sync_every_five_minutes.default_status.value == "RUNNING"
    assert slack_workspace_coverage_sync_every_seven_minutes.cron_schedule == "*/7 * * * *"
    assert slack_workspace_coverage_sync_every_seven_minutes.default_status.value == "RUNNING"
    assert slack_workspace_metadata_sync_every_fifteen_minutes.cron_schedule == "*/15 * * * *"
    assert slack_workspace_metadata_sync_every_fifteen_minutes.default_status.value == "RUNNING"
    assert slack_workspace_user_sync_daily.cron_schedule == "11 8 * * *"
    assert slack_workspace_user_sync_daily.default_status.value == "RUNNING"
    assert slack_workspace_thread_sync_every_five_minutes.cron_schedule == "*/5 * * * *"
    assert slack_workspace_thread_sync_every_five_minutes.default_status.value == "RUNNING"
    assert slack_workspace_read_state_sync_every_five_minutes.cron_schedule == "2-59/5 * * * *"
    assert slack_workspace_read_state_sync_every_five_minutes.default_status.value == "RUNNING"
    assert slack_workspace_member_sync_hourly.cron_schedule == "17 * * * *"
    assert slack_workspace_member_sync_hourly.default_status.value == "RUNNING"


def test_slack_freshness_sync_runs_priority_cycle(monkeypatch) -> None:
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
        gmail_accounts=(),
        gmail_oauth_client_secrets_json=None,
        gmail_scopes=(),
        gmail_page_size=500,
        gmail_include_spam_trash=True,
        gmail_force_full_sync=False,
        gmail_full_sync_query=None,
        gmail_attachment_max_bytes=25 * 1024 * 1024,
        gmail_attachment_text_max_chars=1_000_000,
        gmail_attachment_backfill_batch_size=100,
        slack_accounts=(),
        slack_page_size=200,
        slack_lookback_days=14,
        slack_thread_audit_days=30,
        slack_force_full_sync=False,
    )

    summaries = run_slack_freshness_sync(
        settings=settings,
        warehouse=SimpleNamespace(),
        logger=SimpleNamespace(),
    )

    assert len(summaries) == 5
    assert [call["conversation_types"] for call in calls[:4]] == [
        ("im",),
        ("mpim",),
        ("private_channel",),
        ("public_channel",),
    ]
    assert all(call["freshness_priority"] for call in calls[:4])
    assert all(call["use_existing_conversations"] is True for call in calls[:4])
    assert [call["conversation_limit"] for call in calls[:4]] == [500, 250, 100, 100]
    assert all(call["sync_users"] is False for call in calls)
    assert all(call["sync_members"] is False for call in calls)
    # Freshness fetches replies inline so brand-new threads are captured complete.
    assert all(call["sync_thread_replies"] is True for call in calls[:4])
    # A rate-limit budget hit stops the pass gracefully instead of failing the run.
    assert all(call["skip_known_errors"] is True for call in calls[:4])


def test_slack_freshness_sync_piggybacks_read_state(monkeypatch) -> None:
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
        gmail_accounts=(),
        gmail_oauth_client_secrets_json=None,
        gmail_scopes=(),
        gmail_page_size=500,
        gmail_include_spam_trash=True,
        gmail_force_full_sync=False,
        gmail_full_sync_query=None,
        gmail_attachment_max_bytes=25 * 1024 * 1024,
        gmail_attachment_text_max_chars=1_000_000,
        gmail_attachment_backfill_batch_size=100,
        slack_accounts=(),
        slack_page_size=200,
        slack_lookback_days=14,
        slack_thread_audit_days=30,
        slack_force_full_sync=False,
    )

    summaries = run_slack_freshness_sync(
        settings=settings,
        warehouse=SimpleNamespace(),
        logger=SimpleNamespace(),
    )

    assert len(summaries) == 5
    assert calls[-1]["sync_conversation_info_only"] is True
    assert calls[-1]["conversation_limit"] == 25


def _coverage_test_settings() -> Settings:
    return Settings(
        gmail_accounts=(),
        gmail_oauth_client_secrets_json=None,
        gmail_scopes=(),
        gmail_page_size=500,
        gmail_include_spam_trash=True,
        gmail_force_full_sync=False,
        gmail_full_sync_query=None,
        gmail_attachment_max_bytes=25 * 1024 * 1024,
        gmail_attachment_text_max_chars=1_000_000,
        gmail_attachment_backfill_batch_size=100,
        slack_accounts=(),
        slack_page_size=200,
        slack_lookback_days=14,
        slack_thread_audit_days=30,
        slack_force_full_sync=False,
    )


def _record_coverage_runner_calls(monkeypatch) -> list[dict]:
    calls: list[dict] = []

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
    return calls


def test_slack_coverage_sync_runs_current_stage(monkeypatch) -> None:
    calls = _record_coverage_runner_calls(monkeypatch)
    settings = _coverage_test_settings()

    # The coverage job fires every 7 minutes, so stages rotate over minute // 7.
    # Fire-slot 1 (minutes 7..13) is the mpim stage.
    summaries = run_slack_coverage_sync(
        settings=settings,
        warehouse=SimpleNamespace(),
        logger=SimpleNamespace(),
        now=datetime(2026, 4, 24, 17, 7, tzinfo=UTC),
    )

    assert len(summaries) == 1
    coverage_call = calls[0]
    assert coverage_call["use_existing_conversations"] is True
    assert coverage_call["conversation_types"] == ("mpim",)
    assert coverage_call["not_full_only"] is True
    assert coverage_call["skip_known_errors"] is True
    assert coverage_call["conversation_limit"] == 50
    # Coverage must not force a full sync; channels with a partial cursor need to
    # resume from cursor, not restart from the start of history. Forcing full sync
    # on every coverage pass exhausted the rate-limit budget on huge channels
    # before any progress could be recorded.
    assert coverage_call["settings"] is settings
    assert coverage_call["settings"].slack_force_full_sync is False


def test_slack_coverage_sync_includes_im_backfill_stage(monkeypatch) -> None:
    # IMs (DMs) have no path other than coverage to backfill history that predates
    # the freshness window, so the rotation must include an `im` stage. Fire-slot 6
    # (minutes 42..48) is that stage.
    calls = _record_coverage_runner_calls(monkeypatch)
    settings = _coverage_test_settings()

    summaries = run_slack_coverage_sync(
        settings=settings,
        warehouse=SimpleNamespace(),
        logger=SimpleNamespace(),
        now=datetime(2026, 4, 24, 17, 42, tzinfo=UTC),
    )

    assert len(summaries) == 1
    coverage_call = calls[0]
    assert coverage_call["conversation_types"] == ("im",)
    assert coverage_call["not_full_only"] is True
    assert coverage_call["skip_known_errors"] is True
    assert coverage_call["conversation_limit"] == 50


def test_slack_coverage_rotation_reaches_every_stage_under_seven_minute_cron(monkeypatch) -> None:
    # Regression: stage selection must rotate across all conversation kinds when
    # driven by the */7 cron. A naive minute % 7 collapses to a single stage (every
    # */7 fire-minute is congruent to 0 mod 7); minute // 7 cycles through them.
    settings = _coverage_test_settings()
    fire_minutes = [m for m in range(60) if m % 7 == 0]  # 0,7,14,...,56
    seen_types: set[tuple[str, ...]] = set()

    for minute in fire_minutes:
        calls = _record_coverage_runner_calls(monkeypatch)
        run_slack_coverage_sync(
            settings=settings,
            warehouse=SimpleNamespace(),
            logger=SimpleNamespace(),
            now=datetime(2026, 4, 24, 17, minute, tzinfo=UTC),
        )
        seen_types.add(calls[0]["conversation_types"])

    assert ("im",) in seen_types
    assert ("mpim",) in seen_types
    assert ("private_channel",) in seen_types
    assert ("public_channel",) in seen_types


def test_slack_metadata_sync_refreshes_one_conversation_type(monkeypatch) -> None:
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
                    messages_written=0,
                    users_written=1,
                    files_written=0,
                )
            ]

    monkeypatch.setattr(slack_defs, "SlackSyncRunner", FakeRunner)
    settings = Settings(
        gmail_accounts=(),
        gmail_oauth_client_secrets_json=None,
        gmail_scopes=(),
        gmail_page_size=500,
        gmail_include_spam_trash=True,
        gmail_force_full_sync=False,
        gmail_full_sync_query=None,
        gmail_attachment_max_bytes=25 * 1024 * 1024,
        gmail_attachment_text_max_chars=1_000_000,
        gmail_attachment_backfill_batch_size=100,
        slack_accounts=(),
        slack_page_size=200,
        slack_lookback_days=14,
        slack_thread_audit_days=30,
        slack_force_full_sync=False,
    )

    summaries = run_slack_metadata_sync(
        settings=settings,
        warehouse=SimpleNamespace(),
        logger=SimpleNamespace(),
        now=datetime(2026, 4, 24, 17, 15, tzinfo=UTC),
    )

    assert len(summaries) == 1
    assert calls[0]["sync_conversations_only"] is True
    assert calls[0]["conversation_types"] == ("mpim",)
    assert calls[0]["conversation_page_limit"] == 1


def test_slack_user_sync_refreshes_all_users_without_messages(monkeypatch) -> None:
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
                    conversations_seen=0,
                    messages_written=0,
                    users_written=1,
                    files_written=0,
                )
            ]

    monkeypatch.setattr(slack_defs, "SlackSyncRunner", FakeRunner)
    settings = Settings(
        gmail_accounts=(),
        gmail_oauth_client_secrets_json=None,
        gmail_scopes=(),
        gmail_page_size=500,
        gmail_include_spam_trash=True,
        gmail_force_full_sync=False,
        gmail_full_sync_query=None,
        gmail_attachment_max_bytes=25 * 1024 * 1024,
        gmail_attachment_text_max_chars=1_000_000,
        gmail_attachment_backfill_batch_size=100,
        slack_accounts=(),
        slack_page_size=200,
        slack_lookback_days=14,
        slack_thread_audit_days=30,
        slack_force_full_sync=False,
    )

    summaries = run_slack_user_sync(
        settings=settings,
        warehouse=SimpleNamespace(),
        logger=SimpleNamespace(),
    )

    assert len(summaries) == 1
    assert calls[0]["sync_users"] is True
    assert "user_page_limit" not in calls[0]
    assert calls[0]["use_existing_conversations"] is True
    assert calls[0]["conversation_limit"] == 0
    assert calls[0]["sync_thread_replies"] is False


def test_slack_user_sync_lock_contention_raises_for_retry(monkeypatch) -> None:
    @contextmanager
    def fake_lock(**_kwargs):
        yield False

    class FakeLog:
        def warning(self, *_args):
            pass

    monkeypatch.setattr(slack_defs, "exclusive_sync_lock", fake_lock)
    monkeypatch.setattr(slack_defs, "load_settings", lambda **_kwargs: SimpleNamespace())
    monkeypatch.setattr(slack_defs, "warehouse_from_settings", lambda _settings: SimpleNamespace())

    try:
        _run_locked_slack_stage(
            SimpleNamespace(log=FakeLog()),
            stage_name="users",
            run_fn=lambda **_kwargs: [],
            fail_on_lock_contention=True,
        )
    except RuntimeError as exc:
        assert "could not acquire" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


def test_run_locked_slack_stage_forwards_lock_wait_seconds(monkeypatch) -> None:
    captured: dict[str, object] = {}

    @contextmanager
    def fake_lock(**kwargs):
        captured.update(kwargs)
        yield True

    class FakeLog:
        def info(self, *_args, **_kwargs):
            pass

        def warning(self, *_args, **_kwargs):
            pass

    monkeypatch.setattr(slack_defs, "exclusive_sync_lock", fake_lock)
    monkeypatch.setattr(slack_defs, "load_settings", lambda **_kwargs: SimpleNamespace())
    monkeypatch.setattr(slack_defs, "warehouse_from_settings", lambda _settings: SimpleNamespace())
    monkeypatch.setattr(slack_defs, "build_metadata", lambda: {"git_sha": "test"})

    _run_locked_slack_stage(
        SimpleNamespace(log=FakeLog()),
        stage_name="users",
        run_fn=lambda **_kwargs: [],
        lock_wait_seconds=1800,
    )

    assert captured["wait_seconds"] == 1800


def test_user_sync_lock_wait_seconds_default_and_override(monkeypatch) -> None:
    monkeypatch.delenv("SLACK_USER_SYNC_LOCK_WAIT_SECONDS", raising=False)
    assert slack_defs._user_sync_lock_wait_seconds() == 1800
    monkeypatch.setenv("SLACK_USER_SYNC_LOCK_WAIT_SECONDS", "600")
    assert slack_defs._user_sync_lock_wait_seconds() == 600


def test_slack_thread_sync_backfills_known_threads_conservatively(monkeypatch) -> None:
    calls = []

    class FakeRunner:
        def __init__(self, **kwargs):
            calls.append(kwargs)

        def sync_all(self):
            return [
                SlackSyncSummary(
                    account="zrl",
                    team_id="T1",
                    sync_type="thread_replies",
                    conversations_seen=1,
                    messages_written=2,
                    users_written=0,
                    files_written=0,
                )
            ]

    monkeypatch.setattr(slack_defs, "SlackSyncRunner", FakeRunner)
    settings = Settings(
        gmail_accounts=(),
        gmail_oauth_client_secrets_json=None,
        gmail_scopes=(),
        gmail_page_size=500,
        gmail_include_spam_trash=True,
        gmail_force_full_sync=False,
        gmail_full_sync_query=None,
        gmail_attachment_max_bytes=25 * 1024 * 1024,
        gmail_attachment_text_max_chars=1_000_000,
        gmail_attachment_backfill_batch_size=100,
        slack_accounts=(),
        slack_page_size=200,
        slack_lookback_days=14,
        slack_thread_audit_days=30,
        slack_force_full_sync=False,
    )

    summaries = run_slack_thread_sync(
        settings=settings,
        warehouse=SimpleNamespace(),
        logger=SimpleNamespace(),
    )

    assert len(summaries) == 1
    assert calls[0]["sync_users"] is False
    assert calls[0]["sync_members"] is False
    assert calls[0]["sync_thread_replies_only"] is True
    assert calls[0]["skip_completed_threads"] is True
    assert calls[0]["skip_known_errors"] is True
    assert calls[0]["thread_order"] == "recent"
    assert calls[0]["thread_limit"] == 25
    assert calls[0]["thread_since_days"] == 30


def test_slack_thread_backfill_drains_missing_replies_oldest_first(monkeypatch) -> None:
    calls = []

    class FakeRunner:
        def __init__(self, **kwargs):
            calls.append(kwargs)

        def sync_all(self):
            return [
                SlackSyncSummary(
                    account="zrl",
                    team_id="T1",
                    sync_type="thread_replies",
                    conversations_seen=1,
                    messages_written=2,
                    users_written=0,
                    files_written=0,
                )
            ]

    monkeypatch.setattr(slack_defs, "SlackSyncRunner", FakeRunner)
    settings = Settings(
        gmail_accounts=(),
        gmail_oauth_client_secrets_json=None,
        gmail_scopes=(),
        gmail_page_size=500,
        gmail_include_spam_trash=True,
        gmail_force_full_sync=False,
        gmail_full_sync_query=None,
        gmail_attachment_max_bytes=25 * 1024 * 1024,
        gmail_attachment_text_max_chars=1_000_000,
        gmail_attachment_backfill_batch_size=100,
        slack_accounts=(),
        slack_page_size=200,
        slack_lookback_days=14,
        slack_thread_audit_days=30,
        slack_force_full_sync=False,
    )

    summaries = run_slack_thread_backfill_sync(
        settings=settings,
        warehouse=SimpleNamespace(),
        logger=SimpleNamespace(),
    )

    assert len(summaries) == 1
    assert calls[0]["sync_thread_replies_only"] is True
    assert calls[0]["thread_missing_replies_only"] is True
    # Oldest-first so the historical backlog actually drains instead of churning
    # the same recent threads, and large enough to use the full rate-limit budget.
    assert calls[0]["thread_order"] == "oldest"
    assert calls[0]["thread_limit"] == 100
    assert calls[0]["skip_completed_threads"] is True
    assert calls[0]["skip_known_errors"] is True


def test_slack_member_sync_refreshes_private_members_conservatively(monkeypatch) -> None:
    calls = []

    class FakeRunner:
        def __init__(self, **kwargs):
            calls.append(kwargs)

        def sync_all(self):
            return [
                SlackSyncSummary(
                    account="zrl",
                    team_id="T1",
                    sync_type="members",
                    conversations_seen=1,
                    messages_written=0,
                    users_written=0,
                    files_written=0,
                )
            ]

    monkeypatch.setattr(slack_defs, "SlackSyncRunner", FakeRunner)
    settings = Settings(
        gmail_accounts=(),
        gmail_oauth_client_secrets_json=None,
        gmail_scopes=(),
        gmail_page_size=500,
        gmail_include_spam_trash=True,
        gmail_force_full_sync=False,
        gmail_full_sync_query=None,
        gmail_attachment_max_bytes=25 * 1024 * 1024,
        gmail_attachment_text_max_chars=1_000_000,
        gmail_attachment_backfill_batch_size=100,
        slack_accounts=(),
        slack_page_size=200,
        slack_lookback_days=14,
        slack_thread_audit_days=30,
        slack_force_full_sync=False,
    )

    summaries = run_slack_member_sync(
        settings=settings,
        warehouse=SimpleNamespace(),
        logger=SimpleNamespace(),
    )

    assert len(summaries) == 1
    assert calls[0]["sync_users"] is False
    assert calls[0]["sync_members"] is False
    assert calls[0]["sync_members_only"] is True
    assert calls[0]["use_existing_conversations"] is True
    assert calls[0]["conversation_types"] == ("private_channel",)
    assert calls[0]["conversation_limit"] == 50
    assert calls[0]["sync_thread_replies"] is False


def test_slack_read_state_sync_refreshes_recent_conversation_info(monkeypatch) -> None:
    calls = []

    class FakeRunner:
        def __init__(self, **kwargs):
            calls.append(kwargs)

        def sync_all(self):
            return [
                SlackSyncSummary(
                    account="zrl",
                    team_id="T1",
                    sync_type="conversation_info",
                    conversations_seen=1,
                    messages_written=0,
                    users_written=0,
                    files_written=0,
                )
            ]

    monkeypatch.setattr(slack_defs, "SlackSyncRunner", FakeRunner)
    settings = Settings(
        gmail_accounts=(),
        gmail_oauth_client_secrets_json=None,
        gmail_scopes=(),
        gmail_page_size=500,
        gmail_include_spam_trash=True,
        gmail_force_full_sync=False,
        gmail_full_sync_query=None,
        gmail_attachment_max_bytes=25 * 1024 * 1024,
        gmail_attachment_text_max_chars=1_000_000,
        gmail_attachment_backfill_batch_size=100,
        slack_accounts=(),
        slack_page_size=200,
        slack_lookback_days=14,
        slack_thread_audit_days=30,
        slack_force_full_sync=False,
    )

    summaries = run_slack_read_state_sync(
        settings=settings,
        warehouse=SimpleNamespace(),
        logger=SimpleNamespace(),
    )

    assert len(summaries) == 1
    assert calls[0]["sync_users"] is False
    assert calls[0]["sync_members"] is False
    assert calls[0]["sync_conversation_info_only"] is True
    assert calls[0]["conversation_limit"] == 25


def test_slack_intelligent_sync_keeps_legacy_combined_behavior(monkeypatch) -> None:
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
        gmail_accounts=(),
        gmail_oauth_client_secrets_json=None,
        gmail_scopes=(),
        gmail_page_size=500,
        gmail_include_spam_trash=True,
        gmail_force_full_sync=False,
        gmail_full_sync_query=None,
        gmail_attachment_max_bytes=25 * 1024 * 1024,
        gmail_attachment_text_max_chars=1_000_000,
        gmail_attachment_backfill_batch_size=100,
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

    assert len(summaries) == 6
    assert len(calls) == 6


def test_exclusive_process_lock_is_non_blocking(tmp_path) -> None:
    lock_path = tmp_path / "sync.lock"

    with exclusive_process_lock(lock_path) as first_acquired:
        assert first_acquired
        with exclusive_process_lock(lock_path) as second_acquired:
            assert not second_acquired


def test_exclusive_sync_lock_uses_named_postgres_url(monkeypatch) -> None:
    calls: list[tuple[str, int, float | None]] = []

    @contextmanager
    def fake_postgres_lock(postgres_url: str, lock_id: int, *, wait_seconds: float | None = None):
        calls.append((postgres_url, lock_id, wait_seconds))
        yield True

    monkeypatch.setenv("SLACK_SYNC_LOCK_POSTGRES_URL", "postgresql://postgres/slack")
    monkeypatch.setattr(
        "personal_data_warehouse.sync_locks.exclusive_postgres_advisory_lock",
        fake_postgres_lock,
    )

    with exclusive_sync_lock(name="slack", postgres_lock_id=1234) as acquired:
        assert acquired

    assert calls == [("postgresql://postgres/slack", 1234, None)]


def test_exclusive_sync_lock_forwards_wait_seconds(monkeypatch) -> None:
    calls: list[tuple[str, int, float | None]] = []

    @contextmanager
    def fake_postgres_lock(postgres_url: str, lock_id: int, *, wait_seconds: float | None = None):
        calls.append((postgres_url, lock_id, wait_seconds))
        yield True

    monkeypatch.setenv("SLACK_SYNC_LOCK_POSTGRES_URL", "postgresql://postgres/slack")
    monkeypatch.setattr(
        "personal_data_warehouse.sync_locks.exclusive_postgres_advisory_lock",
        fake_postgres_lock,
    )

    with exclusive_sync_lock(name="slack", postgres_lock_id=1234, wait_seconds=42) as acquired:
        assert acquired

    assert calls == [("postgresql://postgres/slack", 1234, 42)]


def test_exclusive_postgres_advisory_lock_releases_all_session_locks(monkeypatch) -> None:
    import psycopg2

    commands: list[str] = []

    class FakeCursor:
        def execute(self, sql, params=None) -> None:
            commands.append(sql)

        def fetchone(self):
            return (True,)

        def close(self) -> None:
            commands.append("cursor.close")

    class FakeConnection:
        autocommit = False

        def cursor(self):
            return FakeCursor()

        def close(self) -> None:
            commands.append("connection.close")

    monkeypatch.setattr(psycopg2, "connect", lambda postgres_url: FakeConnection())

    with exclusive_postgres_advisory_lock("postgresql://postgres/locks", 1234) as acquired:
        assert acquired

    assert "SELECT pg_try_advisory_lock(%s)" in commands
    assert "SELECT pg_advisory_unlock_all()" in commands
    assert commands[-2:] == ["cursor.close", "connection.close"]


def test_exclusive_process_lock_times_out_when_held(tmp_path) -> None:
    lock_path = tmp_path / "sync.lock"

    with exclusive_process_lock(lock_path) as first_acquired:
        assert first_acquired
        # A bounded wait gives up and reports failure when the holder never releases.
        with exclusive_process_lock(lock_path, wait_seconds=0.2) as second_acquired:
            assert not second_acquired


def test_exclusive_process_lock_waits_for_release(tmp_path) -> None:
    import threading
    import time

    lock_path = tmp_path / "sync.lock"
    holder = exclusive_process_lock(lock_path)
    assert holder.__enter__() is True

    def release_after_delay() -> None:
        time.sleep(0.2)
        holder.__exit__(None, None, None)

    releaser = threading.Thread(target=release_after_delay)
    releaser.start()
    try:
        # The blocking waiter queues behind the holder and acquires once it frees.
        with exclusive_process_lock(lock_path, wait_seconds=5) as acquired:
            assert acquired
    finally:
        releaser.join()
