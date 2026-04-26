from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta

import pytest

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.slack_sync import (
    SlackRateLimitedError,
    SlackApiCallError,
    SlackSyncRunner,
    SlackTransientError,
    conversation_to_row,
    conversation_activity_ts,
    conversation_may_have_activity_since,
    file_rows_from_message,
    iter_cursor_items,
    message_to_row,
    reaction_rows_from_message,
    team_to_row,
    ts_to_datetime,
    user_to_row,
)


class NullLogger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass


class FakeSlackClient:
    def __init__(self, responses):
        self.responses = {method: list(values) for method, values in responses.items()}
        self.calls = []

    def call(self, method, **params):
        self.calls.append((method, params))
        values = self.responses.get(method, [])
        if not values:
            raise AssertionError(f"Unexpected Slack call: {method} {params}")
        value = values.pop(0)
        if isinstance(value, Exception):
            raise value
        return value


class FakeWarehouse:
    def __init__(self, states=None):
        self.states = states or {}
        self.ensure_calls = 0
        self.conversation_payloads = []
        self.conversation_payload_calls = []
        self.thread_refs = []
        self.thread_ref_calls = []
        self.teams = []
        self.users = []
        self.conversations = []
        self.members = []
        self.messages = []
        self.reactions = []
        self.files = []
        self.state_updates = []

    def ensure_slack_tables(self):
        self.ensure_calls += 1

    def load_slack_sync_state(self):
        return dict(self.states)

    def insert_slack_teams(self, rows):
        self.teams.extend(rows)

    def insert_slack_users(self, rows):
        self.users.extend(rows)

    def insert_slack_conversations(self, rows):
        self.conversations.extend(rows)

    def load_slack_conversation_payloads(
        self,
        *,
        account,
        team_id,
        include_archived=False,
        archived_only=False,
        conversation_types=(),
        not_full_only=False,
        zero_messages_only=False,
        skip_known_errors=False,
        limit=None,
    ):
        self.conversation_payload_calls.append(
            {
                "account": account,
                "team_id": team_id,
                "include_archived": include_archived,
                "archived_only": archived_only,
                "conversation_types": conversation_types,
                "not_full_only": not_full_only,
                "zero_messages_only": zero_messages_only,
                "skip_known_errors": skip_known_errors,
                "limit": limit,
            }
        )
        payloads = []
        for payload in self.conversation_payloads:
            is_archived = bool(payload.get("is_archived"))
            if archived_only and not is_archived:
                continue
            if not include_archived and not archived_only and is_archived:
                continue
            if conversation_types and conversation_to_row(
                account=account,
                team_id=team_id,
                conversation=payload,
                synced_at=datetime(2026, 4, 24, tzinfo=UTC),
            )["conversation_type"] not in conversation_types:
                continue
            payloads.append(payload)
        if not_full_only:
            payloads = [
                payload
                for payload in payloads
                if self.states.get((account, team_id, "conversation", str(payload["id"])), {}).get("last_sync_type") != "full"
                or self.states.get((account, team_id, "conversation", str(payload["id"])), {}).get("status") != "ok"
            ]
        if skip_known_errors:
            payloads = [
                payload
                for payload in payloads
                if self.states.get((account, team_id, "conversation", str(payload["id"])), {}).get("status") != "error"
            ]
        if limit is not None:
            payloads = payloads[:limit]
        return payloads

    def load_slack_thread_parent_refs(self, *, account, team_id, since_ts=None, limit=None, skip_completed=False, order="recent"):
        self.thread_ref_calls.append(
            {
                "account": account,
                "team_id": team_id,
                "since_ts": since_ts,
                "limit": limit,
                "skip_completed": skip_completed,
                "order": order,
            }
        )
        refs = list(self.thread_refs)
        if skip_completed:
            refs = [
                ref
                for ref in refs
                if self.states.get((account, team_id, "thread", f"{ref['conversation_id']}:{ref['thread_ts']}"), {}).get("status")
                != "ok"
            ]
        if limit is not None:
            refs = refs[:limit]
        return refs

    def insert_slack_conversation_members(self, rows):
        self.members.extend(rows)

    def insert_slack_messages(self, rows):
        self.messages.extend(rows)

    def insert_slack_message_reactions(self, rows):
        self.reactions.extend(rows)

    def insert_slack_files(self, rows):
        self.files.extend(rows)

    def insert_slack_sync_state(self, **kwargs):
        self.state_updates.append(kwargs)

    def existing_slack_message_ids(self, *, account, team_id, conversation_id, oldest_ts, latest_ts):
        return set()


def test_slack_config_uses_account_slug_for_token(monkeypatch):
    monkeypatch.delenv("GMAIL_ACCOUNTS", raising=False)
    monkeypatch.delenv("GMAIL_OAUTH_CLIENT_SECRETS_JSON", raising=False)
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    monkeypatch.setenv("SLACK_PAGE_SIZE", "123")
    monkeypatch.setenv("SLACK_LOOKBACK_DAYS", "3")
    monkeypatch.setenv("SLACK_THREAD_AUDIT_DAYS", "9")

    settings = load_settings(require_clickhouse=False, require_gmail=False, require_slack=True)

    assert len(settings.slack_accounts) == 1
    assert settings.slack_accounts[0].account == "zrl"
    assert settings.slack_accounts[0].token == "xoxp-test-token"
    assert settings.slack_page_size == 123
    assert settings.slack_lookback_days == 3
    assert settings.slack_thread_audit_days == 9


def test_slack_config_requires_user_token_when_slack_required(monkeypatch):
    monkeypatch.delenv("GMAIL_ACCOUNTS", raising=False)
    monkeypatch.delenv("GMAIL_OAUTH_CLIENT_SECRETS_JSON", raising=False)
    monkeypatch.setenv("SLACK_ACCOUNTS", "missing")
    monkeypatch.delenv("SLACK_MISSING_TOKEN", raising=False)

    with pytest.raises(ValueError, match="SLACK_MISSING_TOKEN"):
        load_settings(require_clickhouse=False, require_gmail=False, require_slack=True)


def test_mapping_rows_preserve_ui_fields_and_raw_json():
    synced_at = datetime(2026, 4, 24, 12, tzinfo=UTC)
    team = team_to_row(
        account="zrl",
        auth_payload={"team_id": "T1", "team": "Hack Club", "user_id": "U1"},
        team_payload={"id": "T1", "name": "Hack Club", "domain": "hackclub"},
        synced_at=synced_at,
    )
    user = user_to_row(
        account="zrl",
        team_id="T1",
        user={"id": "U2", "name": "alice", "profile": {"real_name": "Alice", "email": "a@example.com"}},
        synced_at=synced_at,
    )
    conversation = conversation_to_row(
        account="zrl",
        team_id="T1",
        conversation={"id": "C1", "name": "hq", "is_channel": True, "is_private": False},
        synced_at=synced_at,
    )
    message = message_to_row(
        account="zrl",
        team_id="T1",
        conversation_id="C1",
        message={
            "ts": "1713974400.000200",
            "thread_ts": "1713974400.000100",
            "user": "U2",
            "text": "hello",
            "reply_count": 2,
            "latest_reply": "1713974500.000000",
            "reactions": [{"name": "wave", "users": ["U1", "U2"], "count": 2}],
            "files": [{"id": "F1", "name": "doc.txt", "created": 1713974300}],
        },
        synced_at=synced_at,
    )

    assert team["team_id"] == "T1"
    assert user["email"] == "a@example.com"
    assert conversation["conversation_type"] == "public_channel"
    assert message["message_datetime"] == ts_to_datetime("1713974400.000200")
    assert message["parent_message_ts"] == "1713974400.000100"
    assert message["is_thread_reply"] == 1
    assert message["is_thread_parent"] == 1
    assert "hello" in message["raw_json"]

    reactions = reaction_rows_from_message(
        account="zrl",
        team_id="T1",
        conversation_id="C1",
        message=message,
        source_message={"ts": "1713974400.000200", "reactions": [{"name": "wave", "users": ["U1"], "count": 1}]},
        synced_at=synced_at,
    )
    files = file_rows_from_message(
        account="zrl",
        team_id="T1",
        conversation_id="C1",
        message_ts="1713974400.000200",
        source_message={"files": [{"id": "F1", "user": "U2", "created": 1713974300, "name": "doc.txt"}]},
        synced_at=synced_at,
    )

    assert reactions[0]["reaction_name"] == "wave"
    assert reactions[0]["user_id"] == "U1"
    assert files[0]["file_id"] == "F1"
    assert files[0]["message_ts"] == "1713974400.000200"


def test_iter_cursor_items_pages_until_next_cursor_is_empty():
    client = FakeSlackClient(
        {
            "users.list": [
                {
                    "ok": True,
                    "members": [{"id": "U1"}],
                    "response_metadata": {"next_cursor": "next"},
                },
                {"ok": True, "members": [{"id": "U2"}], "response_metadata": {}},
            ]
        }
    )

    assert list(iter_cursor_items(client, "users.list", "members", limit=2)) == [{"id": "U1"}, {"id": "U2"}]
    assert client.calls[1][1]["cursor"] == "next"


def test_conversation_recency_uses_latest_or_updated_metadata():
    assert conversation_may_have_activity_since({"latest": {"ts": "100.000001"}}, 99.0)
    assert not conversation_may_have_activity_since({"latest": {"ts": "100.000001"}}, 101.0)
    assert conversation_may_have_activity_since({"updated": 100_000}, 99.0)
    assert conversation_may_have_activity_since({"updated": 100_000_000_000}, 99_999_999.0)
    assert conversation_may_have_activity_since({"id": "C1"}, 999.0)
    assert conversation_activity_ts({"latest": {"ts": "120.000001"}}) == pytest.approx(120.000001)
    assert conversation_activity_ts({"updated": 120_000}) == pytest.approx(120_000)
    assert conversation_activity_ts({"updated": 120_000_000_000}) == pytest.approx(120_000_000)


def test_runner_full_sync_collects_workspace_conversations_messages_threads_and_files(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_slack=True)
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club", "user_id": "U1"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club", "domain": "hackclub"}}],
            "users.list": [{"ok": True, "members": [{"id": "U1", "name": "zach"}], "response_metadata": {}}],
            "conversations.list": [
                {
                    "ok": True,
                    "channels": [{"id": "C1", "name": "hq", "is_channel": True}],
                    "response_metadata": {},
                }
            ],
            "conversations.members": [{"ok": True, "members": ["U1"], "response_metadata": {}}],
            "conversations.history": [
                {
                    "ok": True,
                    "messages": [
                        {"ts": "1713974400.000100", "user": "U1", "text": "root", "reply_count": 1},
                        {"ts": "1713974300.000100", "user": "U1", "text": "file", "files": [{"id": "F1"}]},
                    ],
                    "response_metadata": {},
                }
            ],
            "conversations.replies": [
                {
                    "ok": True,
                    "messages": [
                        {"ts": "1713974400.000100", "user": "U1", "text": "root", "reply_count": 1},
                        {"ts": "1713974500.000100", "thread_ts": "1713974400.000100", "user": "U1", "text": "reply"},
                    ],
                    "response_metadata": {},
                }
            ],
        }
    )
    warehouse = FakeWarehouse()

    summaries = SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        sleep=lambda seconds: None,
    ).sync_all()

    assert summaries[0].messages_written == 4
    assert warehouse.ensure_calls == 1
    assert len(warehouse.teams) == 1
    assert len(warehouse.users) == 1
    assert len(warehouse.conversations) == 1
    assert len(warehouse.members) == 1
    assert {row["message_ts"] for row in warehouse.messages} == {
        "1713974300.000100",
        "1713974400.000100",
        "1713974500.000100",
    }
    assert warehouse.files[0]["file_id"] == "F1"
    assert any(update["object_type"] == "conversation" and update["cursor_ts"] == "1713974400.000100" for update in warehouse.state_updates)


def test_runner_can_refresh_conversations_without_fetching_messages(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_slack=True)
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club", "user_id": "U1"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club", "domain": "hackclub"}}],
            "conversations.list": [
                {
                    "ok": True,
                    "channels": [{"id": "C1", "name": "hq", "is_channel": True}],
                    "response_metadata": {"next_cursor": "next"},
                }
            ],
        }
    )
    warehouse = FakeWarehouse()

    summaries = SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        conversation_page_limit=1,
        sync_conversations_only=True,
        sleep=lambda seconds: None,
    ).sync_all()

    assert summaries[0].sync_type == "conversation_refresh"
    assert summaries[0].conversations_seen == 1
    assert summaries[0].messages_written == 0
    assert len(warehouse.conversations) == 1
    assert [method for method, _params in client.calls] == ["auth.test", "team.info", "conversations.list"]


def test_runner_incremental_uses_lookback_and_skips_unchanged_threads(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    monkeypatch.setenv("SLACK_LOOKBACK_DAYS", "2")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_slack=True)
    state_key = ("zrl", "T1", "conversation", "C1")
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club", "user_id": "U1"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "users.list": [{"ok": True, "members": [], "response_metadata": {}}],
            "conversations.list": [
                {
                    "ok": True,
                    "channels": [{"id": "C1", "name": "hq", "is_channel": True}],
                    "response_metadata": {},
                }
            ],
            "conversations.members": [{"ok": True, "members": [], "response_metadata": {}}],
            "conversations.history": [
                {
                    "ok": True,
                    "messages": [{"ts": "1713974600.000100", "user": "U1", "text": "new"}],
                    "response_metadata": {},
                }
            ],
        }
    )
    warehouse = FakeWarehouse(states={state_key: {"cursor_ts": "1713974400.000100", "updated_at": datetime(2026, 4, 23, tzinfo=UTC)}})

    SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        now=lambda: datetime(2026, 4, 24, 12, tzinfo=UTC),
        sleep=lambda seconds: None,
    ).sync_all()

    history_params = [params for method, params in client.calls if method == "conversations.history"][0]
    assert float(history_params["oldest"]) == pytest.approx(1713974400.000100 - 2 * 24 * 60 * 60)
    assert warehouse.messages[0]["text"] == "new"


def test_runner_retries_slack_rate_limits(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_slack=True)
    sleeps = []
    client = FakeSlackClient(
        {
            "auth.test": [SlackRateLimitedError(retry_after=2), {"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "users.list": [{"ok": True, "members": [], "response_metadata": {}}],
            "conversations.list": [{"ok": True, "channels": [], "response_metadata": {}}],
        }
    )

    SlackSyncRunner(
        settings=settings,
        warehouse=FakeWarehouse(),
        logger=NullLogger(),
        client_factory=lambda account: client,
        sleep=sleeps.append,
    ).sync_all()

    assert sleeps == [2]


def test_runner_fails_when_slack_rate_limit_budget_is_exceeded(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_slack=True)
    sleeps = []
    client = FakeSlackClient(
        {
            "auth.test": [
                SlackRateLimitedError(retry_after=2),
                SlackRateLimitedError(retry_after=2),
                {"ok": True, "team_id": "T1", "team": "Hack Club"},
            ],
        }
    )

    with pytest.raises(RuntimeError, match="rate limit budget exceeded"):
        SlackSyncRunner(
            settings=settings,
            warehouse=FakeWarehouse(),
            logger=NullLogger(),
            client_factory=lambda account: client,
            sleep=sleeps.append,
            max_rate_limit_sleep_seconds=3,
        ).sync_all()

    assert sleeps == [2]


def test_runner_retries_transient_slack_request_failures(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_slack=True)
    sleeps = []
    client = FakeSlackClient(
        {
            "auth.test": [SlackTransientError("read timed out"), {"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "users.list": [{"ok": True, "members": [], "response_metadata": {}}],
            "conversations.list": [{"ok": True, "channels": [], "response_metadata": {}}],
        }
    )

    SlackSyncRunner(
        settings=settings,
        warehouse=FakeWarehouse(),
        logger=NullLogger(),
        client_factory=lambda account: client,
        sleep=sleeps.append,
    ).sync_all()

    assert sleeps == [5]


def test_runner_can_backfill_archived_cached_conversations(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.conversation_payloads = [
        {"id": "C1", "name": "active", "is_channel": True, "is_archived": False},
        {"id": "C2", "name": "old", "is_channel": True, "is_archived": True},
    ]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                {"ok": True, "messages": [{"ts": "1713974400.000100", "user": "U1", "text": "archived"}], "response_metadata": {}}
            ],
        }
    )

    SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        sync_users=False,
        sync_members=False,
        use_existing_conversations=True,
        archived_only=True,
        sync_thread_replies=False,
        sleep=lambda seconds: None,
    ).sync_all()

    assert warehouse.conversation_payload_calls[0]["archived_only"] is True
    assert [params["channel"] for method, params in client.calls if method == "conversations.history"] == ["C2"]
    assert warehouse.messages[0]["conversation_id"] == "C2"


def test_runner_can_filter_cached_conversations_by_type(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.conversation_payloads = [
        {"id": "C1", "name": "public", "is_channel": True},
        {"id": "D1", "user": "U1", "is_im": True},
        {"id": "G1", "name": "group", "is_mpim": True},
    ]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                {"ok": True, "messages": [{"ts": "1713974400.000100", "user": "U1", "text": "dm"}], "response_metadata": {}},
                {"ok": True, "messages": [{"ts": "1713974500.000100", "user": "U2", "text": "mpim"}], "response_metadata": {}},
            ],
        }
    )

    SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        sync_users=False,
        sync_members=False,
        use_existing_conversations=True,
        conversation_types=("im", "mpim"),
        sync_thread_replies=False,
        sleep=lambda seconds: None,
    ).sync_all()

    assert warehouse.conversation_payload_calls[0]["conversation_types"] == ("im", "mpim")
    assert [params["channel"] for method, params in client.calls if method == "conversations.history"] == ["D1", "G1"]


def test_runner_can_load_only_not_full_cached_conversations(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse(
        states={("zrl", "T1", "conversation", "C_DONE"): {"status": "ok", "last_sync_type": "full"}}
    )
    warehouse.conversation_payloads = [
        {"id": "C_DONE", "name": "done", "is_channel": True},
        {"id": "C_BACKLOG", "name": "backlog", "is_channel": True},
    ]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                {"ok": True, "messages": [{"ts": "1713974400.000100", "user": "U1", "text": "backlog"}], "response_metadata": {}}
            ],
        }
    )

    SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        sync_users=False,
        sync_members=False,
        use_existing_conversations=True,
        not_full_only=True,
        conversation_limit=10,
        sync_thread_replies=False,
        sleep=lambda seconds: None,
    ).sync_all()

    assert warehouse.conversation_payload_calls[0]["not_full_only"] is True
    assert warehouse.conversation_payload_calls[0]["limit"] == 10
    assert [params["channel"] for method, params in client.calls if method == "conversations.history"] == ["C_BACKLOG"]


def test_runner_can_skip_known_conversation_errors(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse(
        states={("zrl", "T1", "conversation", "C_ERROR"): {"status": "error", "last_sync_type": "full"}}
    )
    warehouse.conversation_payloads = [
        {"id": "C_ERROR", "name": "error", "is_channel": True},
        {"id": "C_OK", "name": "ok", "is_channel": True},
    ]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                {"ok": True, "messages": [{"ts": "1713974400.000100", "user": "U1", "text": "ok"}], "response_metadata": {}}
            ],
        }
    )

    SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        sync_users=False,
        sync_members=False,
        use_existing_conversations=True,
        not_full_only=True,
        skip_known_errors=True,
        sync_thread_replies=False,
        sleep=lambda seconds: None,
    ).sync_all()

    assert warehouse.conversation_payload_calls[0]["skip_known_errors"] is True
    assert [params["channel"] for method, params in client.calls if method == "conversations.history"] == ["C_OK"]


def test_runner_records_conversation_errors_and_continues(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.conversation_payloads = [
        {"id": "C_DENIED", "name": "denied", "is_channel": True},
        {"id": "C_OK", "name": "ok", "is_channel": True},
    ]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                SlackApiCallError("conversations.history failed: not_in_channel"),
                {"ok": True, "messages": [{"ts": "1713974400.000100", "user": "U1", "text": "ok"}], "response_metadata": {}},
            ],
        }
    )

    SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        sync_users=False,
        sync_members=False,
        use_existing_conversations=True,
        sync_thread_replies=False,
        sleep=lambda seconds: None,
    ).sync_all()

    assert any(
        update["object_type"] == "conversation"
        and update["object_id"] == "C_DENIED"
        and update["status"] == "error"
        for update in warehouse.state_updates
    )
    assert warehouse.messages[0]["conversation_id"] == "C_OK"


def test_runner_freshness_priority_refreshes_conversations_and_syncs_ui_order(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_slack=True)
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.list": [
                {
                    "ok": True,
                    "channels": [
                        {"id": "C_PUBLIC", "name": "public", "is_channel": True, "latest": {"ts": "1995.000000"}},
                        {"id": "G_PRIVATE", "name": "private", "is_private": True, "latest": {"ts": "1996.000000"}},
                        {"id": "G_MPIM", "name": "mpim", "is_mpim": True, "latest": {"ts": "1997.000000"}},
                        {"id": "D_OLD", "user": "U0", "is_im": True, "latest": {"ts": "1000.000000"}},
                        {"id": "D_NEW", "user": "U1", "is_im": True, "latest": {"ts": "1999.000000"}},
                    ],
                    "response_metadata": {},
                }
            ],
            "conversations.history": [
                {"ok": True, "messages": [{"ts": "1999.000000", "user": "U1", "text": "dm"}], "response_metadata": {}},
                {"ok": True, "messages": [{"ts": "1997.000000", "user": "U2", "text": "group"}], "response_metadata": {}},
                {"ok": True, "messages": [{"ts": "1996.000000", "user": "U3", "text": "private"}], "response_metadata": {}},
                {"ok": True, "messages": [{"ts": "1995.000000", "user": "U4", "text": "public"}], "response_metadata": {}},
            ],
        }
    )
    warehouse = FakeWarehouse()

    summary = SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        now=lambda: datetime.fromtimestamp(2000, tz=UTC),
        history_window=timedelta(minutes=10),
        sync_users=False,
        sync_members=False,
        freshness_priority=True,
        sync_thread_replies=False,
        sleep=lambda seconds: None,
    ).sync_all()[0]

    list_params = [params for method, params in client.calls if method == "conversations.list"][0]
    history_params = [params for method, params in client.calls if method == "conversations.history"]
    assert list_params["exclude_archived"] == "true"
    assert list_params["types"] == "public_channel,private_channel,mpim,im"
    assert [params["channel"] for params in history_params] == ["D_NEW", "G_MPIM", "G_PRIVATE", "C_PUBLIC"]
    assert all(float(params["oldest"]) == pytest.approx(1400.0) for params in history_params)
    assert summary.sync_type == "freshness_priority"
    assert summary.conversations_seen == 4
    assert summary.messages_written == 4
    assert len(warehouse.conversations) == 5


def test_runner_freshness_priority_can_use_cached_conversations_for_fast_polls(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.conversation_payloads = [
        {"id": "C_PUBLIC", "name": "public", "is_channel": True, "latest": {"ts": "1995.000000"}},
        {"id": "D_NEW", "user": "U1", "is_im": True, "latest": {"ts": "1999.000000"}},
    ]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                {"ok": True, "messages": [{"ts": "1999.000000", "user": "U1", "text": "dm"}], "response_metadata": {}},
                {"ok": True, "messages": [{"ts": "1995.000000", "user": "U4", "text": "public"}], "response_metadata": {}},
            ],
        }
    )

    SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        now=lambda: datetime.fromtimestamp(2000, tz=UTC),
        history_window=timedelta(minutes=10),
        sync_users=False,
        sync_members=False,
        use_existing_conversations=True,
        freshness_priority=True,
        sync_thread_replies=False,
        sleep=lambda seconds: None,
    ).sync_all()

    assert not any(method == "conversations.list" for method, _params in client.calls)
    assert [params["channel"] for method, params in client.calls if method == "conversations.history"] == ["D_NEW", "C_PUBLIC"]
    assert warehouse.conversation_payload_calls[0]["include_archived"] is False


def test_runner_freshness_priority_can_refresh_one_conversation_type(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_slack=True)
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.list": [
                {
                    "ok": True,
                    "channels": [{"id": "D_NEW", "user": "U1", "is_im": True, "latest": {"ts": "1999.000000"}}],
                    "response_metadata": {},
                }
            ],
            "conversations.history": [
                {"ok": True, "messages": [{"ts": "1999.000000", "user": "U1", "text": "dm"}], "response_metadata": {}},
            ],
        }
    )

    SlackSyncRunner(
        settings=settings,
        warehouse=FakeWarehouse(),
        logger=NullLogger(),
        client_factory=lambda account: client,
        now=lambda: datetime.fromtimestamp(2000, tz=UTC),
        history_window=timedelta(minutes=10),
        sync_users=False,
        sync_members=False,
        freshness_priority=True,
        conversation_types=("im",),
        sync_thread_replies=False,
        sleep=lambda seconds: None,
    ).sync_all()

    list_params = [params for method, params in client.calls if method == "conversations.list"][0]
    assert list_params["types"] == "im"
    assert [params["channel"] for method, params in client.calls if method == "conversations.history"] == ["D_NEW"]


def test_runner_thread_replies_only_is_resumable(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.thread_refs = [
        {"conversation_id": "C1", "thread_ts": "1713974400.000100", "reply_count": 1, "latest_reply_ts": "1713974500.000100"},
        {"conversation_id": "C2", "thread_ts": "1713974600.000100", "reply_count": 1, "latest_reply_ts": "1713974700.000100"},
    ]
    warehouse.states = {
        ("zrl", "T1", "thread", "C1:1713974400.000100"): {"status": "ok", "last_sync_type": "thread_replies"}
    }
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.replies": [
                {
                    "ok": True,
                    "messages": [
                        {"ts": "1713974600.000100", "user": "U1", "text": "root", "reply_count": 1},
                        {"ts": "1713974700.000100", "thread_ts": "1713974600.000100", "user": "U2", "text": "reply"},
                    ],
                    "response_metadata": {},
                }
            ],
        }
    )

    summary = SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        sync_thread_replies_only=True,
        skip_completed_threads=True,
        thread_order="reply_count",
        thread_limit=10,
        sleep=lambda seconds: None,
    ).sync_all()[0]

    assert warehouse.thread_ref_calls[0]["skip_completed"] is True
    assert warehouse.thread_ref_calls[0]["order"] == "reply_count"
    assert [params["channel"] for method, params in client.calls if method == "conversations.replies"] == ["C2"]
    assert summary.sync_type == "thread_replies"
    assert summary.messages_written == 2
    assert any(update["object_type"] == "thread" and update["object_id"] == "C2:1713974600.000100" for update in warehouse.state_updates)
