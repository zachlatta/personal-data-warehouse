from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta

import pytest

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.slack_sync import (
    SlackRateLimitedError,
    SlackApiCallError,
    SlackWebApiClient,
    SlackSyncRunner,
    SlackTransientError,
    conversation_to_row,
    conversation_activity_ts,
    conversation_may_have_activity_since,
    file_rows_from_message,
    iter_cursor_items,
    message_to_row,
    reaction_rows_from_message,
    slack_account_identity_to_row,
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
        self.public_sweep_payloads = []
        self.public_sweep_calls = []
        self.conversation_touches = []
        self.member_candidate_payloads = []
        self.member_candidate_calls = []
        self.read_state_candidate_calls = []
        self.thread_refs = []
        self.thread_ref_calls = []
        self.teams = []
        self.identities = []
        self.users = []
        self.conversations = []
        self.members = []
        self.member_replacements = []
        self.messages = []
        self.reactions = []
        self.files = []
        self.state_updates = []
        self.account_state_refreshes = []
        self.inactivated_conversations = []
        self.existing_message_ids: set[str] = set()
        # (account, team_id, conversation_id) -> high-water message ts, used to
        # derive a fallback cursor for conversations whose stored cursor was lost.
        self.message_high_water: dict[tuple[str, str, str], float] = {}
        # (account, team_id, conversation_id) -> oldest top-level message ts we
        # hold, the floor coverage backfills below.
        self.message_low_water: dict[tuple[str, str, str], str] = {}
        self.low_water_calls = []
        self.known_conversation_id_calls: list[list[str]] = []

    def ensure_slack_tables(self):
        self.ensure_calls += 1

    def load_slack_sync_state(self):
        return dict(self.states)

    def insert_slack_teams(self, rows):
        self.teams.extend(rows)

    def insert_slack_account_identities(self, rows):
        self.identities.extend(rows)

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
        conversation_ids=None,
    ):
        self.conversation_payload_calls.append(
            {
                "conversation_ids": conversation_ids,
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
                if self.states.get((account, team_id, "conversation", str(payload["id"])), {}).get("status") != "gone"
            ]
        if limit is not None:
            payloads = payloads[:limit]
        return payloads

    def load_slack_public_sweep_candidate_payloads(
        self,
        *,
        account,
        team_id,
        hot_within_days=7,
        hot_limit=0,
        cold_limit=0,
    ):
        self.public_sweep_calls.append(
            {
                "account": account,
                "team_id": team_id,
                "hot_within_days": hot_within_days,
                "hot_limit": hot_limit,
                "cold_limit": cold_limit,
            }
        )
        return list(self.public_sweep_payloads)

    def touch_slack_conversation_sync_state(self, **kwargs):
        self.conversation_touches.append(kwargs)

    def load_slack_thread_parent_refs(
        self,
        *,
        account,
        team_id,
        since_ts=None,
        before_thread_ts=None,
        before_conversation_id="",
        limit=None,
        skip_completed=False,
        skip_known_errors=False,
        order="recent",
        missing_replies_only=False,
    ):
        self.thread_ref_calls.append(
            {
                "account": account,
                "team_id": team_id,
                "since_ts": since_ts,
                "before_thread_ts": before_thread_ts,
                "before_conversation_id": before_conversation_id,
                "limit": limit,
                "skip_completed": skip_completed,
                "skip_known_errors": skip_known_errors,
                "order": order,
                "missing_replies_only": missing_replies_only,
            }
        )
        refs = list(self.thread_refs)
        if before_thread_ts:
            before_key = (
                float(before_thread_ts),
                str(before_thread_ts),
                str(before_conversation_id),
            )
            refs = [
                ref
                for ref in refs
                if (
                    float(ref["thread_ts"]),
                    str(ref["thread_ts"]),
                    str(ref["conversation_id"]),
                )
                < before_key
            ]
        if skip_known_errors:
            refs = [
                ref
                for ref in refs
                if self.states.get((account, team_id, "thread", f"{ref['conversation_id']}:{ref['thread_ts']}"), {}).get("status")
                != "gone"
            ]
        if skip_completed:
            refs = [
                ref
                for ref in refs
                if not thread_state_covers_ref(
                    self.states.get((account, team_id, "thread", f"{ref['conversation_id']}:{ref['thread_ts']}"), {}),
                    ref,
                )
            ]
        if limit is not None:
            refs = refs[:limit]
        return refs

    def load_slack_read_state_candidate_payloads(self, *, account, team_id, conversation_types=(), limit=None):
        self.read_state_candidate_calls.append(
            {
                "account": account,
                "team_id": team_id,
                "conversation_types": conversation_types,
                "limit": limit,
            }
        )
        payloads = []
        for payload in self.conversation_payloads:
            if conversation_types and conversation_to_row(
                account=account,
                team_id=team_id,
                conversation=payload,
                synced_at=datetime(2026, 4, 24, tzinfo=UTC),
            )["conversation_type"] not in conversation_types:
                continue
            payloads.append(payload)
        if limit is not None:
            payloads = payloads[:limit]
        return payloads

    def load_slack_member_sync_candidate_payloads(
        self,
        *,
        account,
        team_id,
        conversation_types=(),
        limit=None,
        skip_known_errors=False,
    ):
        self.member_candidate_calls.append(
            {
                "account": account,
                "team_id": team_id,
                "conversation_types": conversation_types,
                "limit": limit,
                "skip_known_errors": skip_known_errors,
            }
        )
        payloads = []
        candidates = self.member_candidate_payloads or self.conversation_payloads
        for payload in candidates:
            if conversation_types and conversation_to_row(
                account=account,
                team_id=team_id,
                conversation=payload,
                synced_at=datetime(2026, 4, 24, tzinfo=UTC),
            )["conversation_type"] not in conversation_types:
                continue
            payloads.append(payload)
        if limit is not None:
            payloads = payloads[:limit]
        return payloads

    def insert_slack_conversation_members(self, rows):
        self.members.extend(rows)

    def replace_slack_conversation_members(self, **kwargs):
        self.member_replacements.append(kwargs)
        self.members.extend(kwargs["rows"])

    def insert_slack_messages(self, rows):
        self.messages.extend(rows)

    def insert_slack_message_reactions(self, rows):
        self.reactions.extend(rows)

    def insert_slack_files(self, rows):
        self.files.extend(rows)

    def insert_slack_sync_state(self, **kwargs):
        self.state_updates.append(kwargs)

    def mark_slack_conversation_inactive(self, *, account, team_id, conversation_id):
        self.inactivated_conversations.append(
            {"account": account, "team_id": team_id, "conversation_id": conversation_id}
        )

    def refresh_slack_account_state_items(self, **kwargs):
        self.account_state_refreshes.append(kwargs)

    def existing_slack_message_ids(self, *, account, team_id, conversation_id, oldest_ts, latest_ts):
        # The real implementation filters to is_thread_reply = 0 because
        # conversations.history doesn't return replies and including them here
        # would cause every reply in the window to be tombstoned. Tests set
        # `existing_message_ids` to the set this returns.
        return set(self.existing_message_ids)

    def load_slack_conversation_message_low_water(self, *, account, team_id, conversation_ids):
        self.low_water_calls.append(list(conversation_ids))
        wanted = {str(conversation_id) for conversation_id in conversation_ids}
        return {
            conversation_id: low_water
            for (state_account, state_team_id, conversation_id), low_water in self.message_low_water.items()
            if state_account == account and state_team_id == team_id and conversation_id in wanted
        }

    def load_slack_conversation_message_high_water(self, *, account, team_id, conversation_ids):
        wanted = {str(conversation_id) for conversation_id in conversation_ids}
        return {
            conversation_id: high_water
            for (state_account, state_team_id, conversation_id), high_water in self.message_high_water.items()
            if state_account == account and state_team_id == team_id and conversation_id in wanted
        }

    def load_slack_known_conversation_ids(self, *, account, team_id, conversation_ids):
        wanted = {str(conversation_id) for conversation_id in conversation_ids}
        self.known_conversation_id_calls.append(sorted(wanted))
        return {
            str(payload["id"])
            for payload in self.conversation_payloads
            if str(payload.get("id") or "") in wanted
        }


def test_slack_web_api_client_uses_bounded_timeout(monkeypatch):
    captured = {}

    class FakeWebClient:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setenv("SLACK_API_TIMEOUT_SECONDS", "7")
    monkeypatch.setattr("personal_data_warehouse.slack_sync.WebClient", FakeWebClient)

    SlackWebApiClient("xoxp-test-token")

    assert captured["token"] == "xoxp-test-token"
    assert captured["timeout"] == 7


def test_slack_config_uses_account_slug_for_token(monkeypatch):
    monkeypatch.delenv("GMAIL_ACCOUNTS", raising=False)
    monkeypatch.delenv("GMAIL_OAUTH_CLIENT_SECRETS_JSON", raising=False)
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    monkeypatch.setenv("SLACK_PAGE_SIZE", "123")
    monkeypatch.setenv("SLACK_LOOKBACK_DAYS", "3")
    monkeypatch.setenv("SLACK_THREAD_AUDIT_DAYS", "9")

    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)

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
        load_settings(require_postgres=False, require_gmail=False, require_slack=True)


def test_mapping_rows_preserve_ui_fields_and_raw_json():
    synced_at = datetime(2026, 4, 24, 12, tzinfo=UTC)
    team = team_to_row(
        account="zrl",
        auth_payload={"team_id": "T1", "team": "Hack Club", "user_id": "U1"},
        team_payload={"id": "T1", "name": "Hack Club", "domain": "hackclub"},
        synced_at=synced_at,
    )
    identity = slack_account_identity_to_row(
        account="zrl",
        team_id="T1",
        auth_payload={"team_id": "T1", "team": "Hack Club", "user_id": "U1", "url": "https://hackclub.slack.com/"},
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
    assert identity["user_id"] == "U1"
    assert identity["url"] == "https://hackclub.slack.com/"
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


def test_conversation_recency_uses_latest_or_cursor_state():
    # Slack's `latest.ts`, when present, is authoritative.
    assert conversation_may_have_activity_since({"latest": {"ts": "100.000001"}}, 99.0)
    assert not conversation_may_have_activity_since({"latest": {"ts": "100.000001"}}, 101.0)
    # When no `latest.ts` and no cursor state, default to including the channel.
    # Slack's `updated` (channel-property edit time) is NOT a valid message-activity
    # signal; channels with stale metadata were silently skipped under the old
    # behavior, which is what broke large stale-metadata channels.
    assert conversation_may_have_activity_since({"id": "C1"}, 999.0)
    assert conversation_may_have_activity_since({"updated": 100_000}, 99.0)
    assert conversation_may_have_activity_since({"updated": 100_000}, 200_000)
    # When our own cursor is provided, include regardless of whether we're behind
    # or ahead; the conversation_limit cap bounds the work.
    assert conversation_may_have_activity_since({}, 100.0, cursor_ts=50.0)
    assert conversation_may_have_activity_since({}, 100.0, cursor_ts=200.0)
    # A cached `latest.ts` can be arbitrarily stale (the stored payload is only
    # refreshed periodically), so it must NOT exclude a conversation we already
    # track via our own cursor. A stale latest.ts older than the window used to
    # freeze the conversation on every pass; our cursor must win.
    assert conversation_may_have_activity_since({"latest": {"ts": "100.000001"}}, 101.0, cursor_ts=50.0)

    # Sort key prefers cursor_ts (truthful) over Slack hints.
    assert conversation_activity_ts({"latest": {"ts": "120.000001"}}) == pytest.approx(120.000001)
    assert conversation_activity_ts({"updated": 120_000}) == pytest.approx(120_000)
    assert conversation_activity_ts({"updated": 120_000_000_000}) == pytest.approx(120_000_000)
    assert conversation_activity_ts({"updated": 120_000}, cursor_ts=200.0) == pytest.approx(200.0)
    assert conversation_activity_ts({"latest": {"ts": "100.0"}}, cursor_ts=200.0) == pytest.approx(200.0)


def test_runner_full_sync_collects_workspace_conversations_messages_threads_and_files(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
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
    assert warehouse.identities[0]["user_id"] == "U1"
    assert warehouse.account_state_refreshes[0]["account"] == "zrl"
    assert warehouse.account_state_refreshes[0]["team_id"] == "T1"
    assert isinstance(warehouse.account_state_refreshes[0]["synced_at"], datetime)
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


def test_runner_members_only_syncs_cached_private_member_candidates(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club", "user_id": "U1"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club", "domain": "hackclub"}}],
            "conversations.members": [{"ok": True, "members": ["U1", "U2"], "response_metadata": {}}],
        }
    )
    warehouse = FakeWarehouse()
    warehouse.member_candidate_payloads = [{"id": "G1", "name": "private", "is_private": True, "is_member": True}]

    summaries = SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        sync_members_only=True,
        sync_users=True,
        sync_members=False,
        use_existing_conversations=True,
        conversation_types=("private_channel",),
        conversation_limit=50,
        sleep=lambda seconds: None,
    ).sync_all()

    assert summaries[0].sync_type == "members"
    assert summaries[0].conversations_seen == 1
    assert [method for method, _params in client.calls] == ["auth.test", "team.info", "conversations.members"]
    assert warehouse.member_candidate_calls == [
        {
            "account": "zrl",
            "team_id": "T1",
            "conversation_types": ("private_channel",),
            "limit": 50,
            "skip_known_errors": False,
        }
    ]
    assert [row["user_id"] for row in warehouse.member_replacements[0]["rows"]] == ["U1", "U2"]
    assert warehouse.member_replacements[0]["conversation_id"] == "G1"
    assert warehouse.state_updates[-1]["object_type"] == "conversation_members"
    assert warehouse.state_updates[-1]["object_id"] == "G1"
    assert warehouse.state_updates[-1]["status"] == "ok"


def test_runner_members_only_records_errors_without_replacing_members(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club", "user_id": "U1"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club", "domain": "hackclub"}}],
            "conversations.members": [SlackApiCallError("conversations.members failed: not_in_channel")],
        }
    )
    warehouse = FakeWarehouse()
    warehouse.member_candidate_payloads = [{"id": "G1", "name": "private", "is_private": True, "is_member": True}]

    SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        sync_members_only=True,
        use_existing_conversations=True,
        conversation_types=("private_channel",),
        sleep=lambda seconds: None,
    ).sync_all()

    assert warehouse.member_replacements == []
    assert warehouse.members == []
    assert warehouse.state_updates[-1]["object_type"] == "conversation_members"
    assert warehouse.state_updates[-1]["object_id"] == "G1"
    # Constructed without a Slack error code, so we cannot tell it is
    # permanently gone; it stays a retryable error.
    assert warehouse.state_updates[-1]["status"] == "error"
    assert "not_in_channel" in warehouse.state_updates[-1]["error"]


def test_runner_members_only_marks_gone_channel_terminal(monkeypatch):
    # A coded gone failure from conversations.members is terminal: record the
    # 'gone' status so the members stage stops re-offering the channel and the
    # dashboard does not count it as failing forever.
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club", "user_id": "U1"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club", "domain": "hackclub"}}],
            "conversations.members": [
                SlackApiCallError("conversations.members failed: channel_not_found", code="channel_not_found")
            ],
        }
    )
    warehouse = FakeWarehouse()
    warehouse.member_candidate_payloads = [{"id": "G1", "name": "private", "is_private": True, "is_member": True}]

    SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        sync_members_only=True,
        use_existing_conversations=True,
        conversation_types=("private_channel",),
        sleep=lambda seconds: None,
    ).sync_all()

    assert warehouse.state_updates[-1]["object_type"] == "conversation_members"
    assert warehouse.state_updates[-1]["object_id"] == "G1"
    assert warehouse.state_updates[-1]["status"] == "gone"
    assert "channel_not_found" in warehouse.state_updates[-1]["error"]


def test_runner_can_refresh_conversations_without_fetching_messages(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
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


def test_runner_can_refresh_conversation_info_only(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club", "user_id": "U1"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club", "domain": "hackclub"}}],
            "conversations.info": [
                {
                    "ok": True,
                    "channel": {
                        "id": "C1",
                        "name": "hq",
                        "is_channel": True,
                        "is_member": True,
                        "last_read": "1713974400.000100",
                    },
                }
            ],
        }
    )
    warehouse = FakeWarehouse()
    warehouse.conversation_payloads = [{"id": "C1", "name": "hq", "is_channel": True, "is_member": True}]

    summaries = SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        conversation_limit=1,
        sync_conversation_info_only=True,
        sleep=lambda seconds: None,
    ).sync_all()

    assert summaries[0].sync_type == "conversation_info"
    assert summaries[0].conversations_seen == 1
    assert summaries[0].messages_written == 0
    assert warehouse.read_state_candidate_calls[0]["limit"] == 1
    assert warehouse.conversations[0]["conversation_id"] == "C1"
    assert "last_read" in warehouse.conversations[0]["raw_json"]
    assert [method for method, _params in client.calls] == ["auth.test", "team.info", "conversations.info"]


def test_runner_incremental_uses_lookback_and_skips_unchanged_threads(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    monkeypatch.setenv("SLACK_LOOKBACK_DAYS", "2")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
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
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
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
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
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


def test_runner_returns_partial_when_known_error_sync_hits_rate_limit_budget(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    sleeps = []
    warehouse = FakeWarehouse()
    warehouse.conversation_payloads = [
        {"id": "C_RATE_LIMITED", "name": "slow", "is_channel": True},
        {"id": "C_NOT_REACHED", "name": "later", "is_channel": True},
    ]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                SlackRateLimitedError(retry_after=2),
                SlackRateLimitedError(retry_after=2),
            ],
        }
    )

    summary = SlackSyncRunner(
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
        sleep=sleeps.append,
        max_rate_limit_sleep_seconds=3,
    ).sync_all()[0]

    assert sleeps == [2]
    assert summary.conversations_seen == 2
    assert summary.messages_written == 0
    assert [params["channel"] for method, params in client.calls if method == "conversations.history"] == [
        "C_RATE_LIMITED",
        "C_RATE_LIMITED",
    ]
    assert len(warehouse.account_state_refreshes) == 1
    assert warehouse.account_state_refreshes[0]["account"] == "zrl"
    assert warehouse.account_state_refreshes[0]["team_id"] == "T1"


def test_runner_retries_transient_slack_request_failures(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
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


def test_call_raises_after_transient_attempts_exceed_cap(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    sleeps = []
    client = FakeSlackClient(
        {
            "some.method": [
                SlackTransientError("read timed out"),
                SlackTransientError("read timed out"),
                SlackTransientError("read timed out"),
            ],
        }
    )

    runner = SlackSyncRunner(
        settings=settings,
        warehouse=FakeWarehouse(),
        logger=NullLogger(),
        client_factory=lambda account: client,
        sleep=sleeps.append,
        max_transient_attempts=3,
    )

    with pytest.raises(SlackTransientError):
        runner._call(client, "some.method")

    assert sleeps == [5, 10]


def test_call_recovers_from_transient_failure_before_cap(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    sleeps = []
    client = FakeSlackClient(
        {
            "some.method": [
                SlackTransientError("read timed out"),
                {"ok": True, "value": "recovered"},
            ],
        }
    )

    runner = SlackSyncRunner(
        settings=settings,
        warehouse=FakeWarehouse(),
        logger=NullLogger(),
        client_factory=lambda account: client,
        sleep=sleeps.append,
        max_transient_attempts=3,
    )

    result = runner._call(client, "some.method")

    assert result == {"ok": True, "value": "recovered"}
    assert sleeps == [5]


def test_runner_can_backfill_archived_cached_conversations(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
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
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
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
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
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
    # skip_known_errors excludes conversations recorded as terminally 'gone';
    # transiently-errored conversations stay in the candidate set for retry.
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse(
        states={("zrl", "T1", "conversation", "C_ERROR"): {"status": "gone", "last_sync_type": "full"}}
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
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
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
    # The error was constructed without a Slack error code, so we cannot tell it is
    # permanently gone and must leave the channel active for a later retry.
    assert warehouse.inactivated_conversations == []


def test_runner_full_sync_marks_gone_channel_inactive(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.conversation_payloads = [
        {"id": "C_GONE", "name": "gone", "is_channel": True},
        {"id": "C_OK", "name": "ok", "is_channel": True},
    ]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                SlackApiCallError("conversations.history failed: channel_not_found", code="channel_not_found"),
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

    # Gone-for-good channels record the terminal 'gone' status, not 'error':
    # nothing will ever retry them, so an error row would sit in the failing
    # count on the pipeline health dashboard forever.
    assert any(
        update["object_id"] == "C_GONE" and update["status"] == "gone"
        for update in warehouse.state_updates
    )
    assert warehouse.inactivated_conversations == [
        {"account": "zrl", "team_id": "T1", "conversation_id": "C_GONE"}
    ]
    # The run keeps going and still syncs the healthy channel.
    assert warehouse.messages[0]["conversation_id"] == "C_OK"


def test_runner_full_sync_keeps_transient_error_channel_active(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.conversation_payloads = [
        {"id": "C_FLAKY", "name": "flaky", "is_channel": True},
        {"id": "C_OK", "name": "ok", "is_channel": True},
    ]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                SlackApiCallError("conversations.history failed: internal_error", code="internal_error"),
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
        update["object_id"] == "C_FLAKY" and update["status"] == "error"
        for update in warehouse.state_updates
    )
    # A non-"gone" error is potentially transient; never deactivate the channel.
    assert warehouse.inactivated_conversations == []
    assert warehouse.messages[0]["conversation_id"] == "C_OK"


def test_runner_freshness_priority_skips_and_deactivates_gone_channel(monkeypatch):
    # Regression: a single channel_not_found in the freshness window used to
    # propagate out of _sync_account_freshness_priority -> sync_all and fail the
    # entire 5-minute run, discarding the window. It must now be skipped, recorded,
    # and the dead channel marked inactive while the rest of the window syncs.
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.conversation_payloads = [
        {"id": "C_GONE", "name": "gone", "is_channel": True, "latest": {"ts": "1999.000000"}},
        {"id": "C_OK", "name": "ok", "is_channel": True, "latest": {"ts": "1995.000000"}},
    ]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                SlackApiCallError("conversations.history failed: channel_not_found", code="channel_not_found"),
                {"ok": True, "messages": [{"ts": "1995.000000", "user": "U4", "text": "public"}], "response_metadata": {}},
            ],
        }
    )

    summary = SlackSyncRunner(
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
    ).sync_all()[0]

    history_channels = [params["channel"] for method, params in client.calls if method == "conversations.history"]
    assert history_channels == ["C_GONE", "C_OK"]
    assert summary.sync_type == "freshness_priority"
    # Only the healthy channel's message is written; the run did not abort.
    assert summary.messages_written == 1
    assert warehouse.messages[0]["conversation_id"] == "C_OK"
    assert warehouse.inactivated_conversations == [
        {"account": "zrl", "team_id": "T1", "conversation_id": "C_GONE"}
    ]
    assert any(
        update["object_id"] == "C_GONE" and update["status"] == "gone"
        for update in warehouse.state_updates
    )


def test_runner_freshness_priority_refreshes_conversations_and_syncs_ui_order(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
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
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
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


def test_runner_freshness_priority_stops_gracefully_when_rate_limit_budget_is_exceeded(monkeypatch):
    # Regression: the freshness stage (slack_workspace_sync) was the only Slack job
    # that hard-failed when the per-runner rate-limit budget was exhausted, because
    # _sync_account_freshness_priority let SlackRateLimitBudgetExceeded propagate out
    # of sync_all (~33% of prod runs failed this way). With skip_known_errors=True it
    # now stops gracefully, persisting the history cursor so the next pass resumes.
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    sleeps = []
    warehouse = FakeWarehouse()
    warehouse.conversation_payloads = [
        {"id": "D_SLOW", "user": "U1", "is_im": True, "latest": {"ts": "1999.000000"}},
        {"id": "D_NOT_REACHED", "user": "U2", "is_im": True, "latest": {"ts": "1999.000000"}},
    ]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                SlackRateLimitedError(retry_after=2),
                SlackRateLimitedError(retry_after=2),
            ],
        }
    )

    summary = SlackSyncRunner(
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
        skip_known_errors=True,
        sync_thread_replies=False,
        sleep=sleeps.append,
        max_rate_limit_sleep_seconds=3,
    ).sync_all()[0]

    assert sleeps == [2]
    assert summary.sync_type == "freshness_priority"
    assert summary.conversations_seen == 1
    # Stopped before touching the second conversation rather than failing the run.
    history_channels = [params["channel"] for method, params in client.calls if method == "conversations.history"]
    assert set(history_channels) == {"D_SLOW"}
    assert "D_NOT_REACHED" not in history_channels


def test_runner_freshness_priority_fetches_thread_replies_inline(monkeypatch):
    # Regression: the freshness sync used to skip replies (sync_thread_replies=False),
    # so brand-new threads landed in the warehouse as a lone parent and their replies
    # fell to a heavily throttled backfill job. With inline replies enabled, a thread
    # parent that appears in the recent window is captured complete in the same pass.
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.conversation_payloads = [
        {"id": "C_PUBLIC", "name": "public", "is_channel": True, "latest": {"ts": "1999.000000"}},
    ]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                {
                    "ok": True,
                    "messages": [{"ts": "1999.000000", "user": "U1", "text": "parent", "reply_count": 2}],
                    "response_metadata": {},
                },
            ],
            "conversations.replies": [
                {
                    "ok": True,
                    "messages": [
                        {"ts": "1999.000000", "user": "U1", "text": "parent", "reply_count": 2},
                        {"ts": "1999.000100", "thread_ts": "1999.000000", "user": "U2", "text": "reply one"},
                        {"ts": "1999.000200", "thread_ts": "1999.000000", "user": "U3", "text": "reply two"},
                    ],
                    "response_metadata": {},
                },
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
        sync_thread_replies=True,
        sleep=lambda seconds: None,
    ).sync_all()

    replies_calls = [params for method, params in client.calls if method == "conversations.replies"]
    assert [params["channel"] for params in replies_calls] == ["C_PUBLIC"]
    assert replies_calls[0]["ts"] == "1999.000000"
    stored_replies = {
        row["message_ts"] for row in warehouse.messages if row["is_thread_reply"] == 1
    }
    assert stored_replies == {"1999.000100", "1999.000200"}


def test_runner_freshness_priority_syncs_stuck_channel_without_latest_metadata(monkeypatch):
    # Regression: Slack does not populate `latest` for channels in stored payloads,
    # so the freshness path used to fall back to `conversation.updated` (channel-
    # property edit time, not message activity), which silently skipped any
    # channel whose metadata had not changed recently. With cursor-aware filtering
    # the channel is included via its stored sync_state.cursor_ts.
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.conversation_payloads = [
        # No `latest.ts`. `updated` is before the freshness window (oldest_ts=1400).
        # Under the old behavior this channel would have been skipped silently
        # because the activity filter fell back to `updated < oldest_ts`.
        {"id": "C_STUCK", "name": "large-channel", "is_channel": True, "updated": 500},
    ]
    warehouse.states = {
        ("zrl", "T1", "conversation", "C_STUCK"): {
            "cursor_ts": "1900.000000",
            "last_sync_type": "partial",
            "status": "ok",
        }
    }
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                {
                    "ok": True,
                    "messages": [{"ts": "1999.000000", "user": "U1", "text": "fresh"}],
                    "response_metadata": {},
                }
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

    history_channels = [params["channel"] for method, params in client.calls if method == "conversations.history"]
    assert history_channels == ["C_STUCK"]


def test_runner_freshness_priority_syncs_frozen_dm_with_stale_latest_metadata(monkeypatch):
    # Regression (stale-latest DM freeze): some IM payloads carry a `latest.ts` from an
    # earlier conversations.info read. That cached `latest.ts` goes stale because the
    # stored payload is only refreshed periodically, and the activity filter used it to
    # *exclude* the DM on every freshness pass (latest.ts < freshness window), freezing
    # its cursor forever. A DM whose cached latest.ts predates the window must still be
    # synced when we hold our own cursor, and it must resume from that cursor so the gap
    # that accumulated while it was frozen is backfilled, not just the last window.
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.conversation_payloads = [
        # Stale `latest.ts` (900) is far before the freshness window (oldest_ts=1400).
        # Under the old behavior the activity filter returned latest.ts < oldest_ts and
        # skipped the DM on every pass, never advancing its cursor.
        {"id": "D_FROZEN", "user": "U1", "is_im": True, "latest": {"ts": "900.000000"}},
    ]
    warehouse.states = {
        ("zrl", "T1", "conversation", "D_FROZEN"): {
            "cursor_ts": "1000.000000",
            "last_sync_type": "partial",
            "status": "ok",
        }
    }
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                {
                    "ok": True,
                    "messages": [{"ts": "1500.000000", "user": "U1", "text": "missed while frozen"}],
                    "response_metadata": {},
                }
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

    history_calls = [params for method, params in client.calls if method == "conversations.history"]
    assert [params["channel"] for params in history_calls] == ["D_FROZEN"]
    # Resumes from the stored cursor (1000), not just the freshness window start (1400),
    # so messages that accumulated while the DM was frozen are backfilled.
    assert float(history_calls[0]["oldest"]) == pytest.approx(1000.0)


def test_runner_freshness_priority_derives_cursor_for_cleared_dm_from_message_high_water(monkeypatch):
    # Regression (cleared-cursor DM freeze): a DM whose stored cursor was wiped to ''
    # (the pre-"Preserve Slack cursor on empty partial sync" empty-window bug overwrote
    # real cursors with an empty string) has no cursor to trust, so the activity sort and
    # gate fall back to a stale cached `latest.ts`/`updated`. That buries the DM below the
    # conversation_limit and freezes it forever even though we have its history on disk.
    # The runner must derive a fallback cursor from the high-water mark of the messages we
    # already stored, so the DM is ranked by real progress and backfilled from that mark.
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.conversation_payloads = [
        # No `latest.ts`; a stale `updated` (900) that predates our real progress. Under
        # the old behavior the sort key fell back to `updated`, ranking the DM as if it
        # were last active at 900 and burying it.
        {"id": "D_CLEARED", "user": "U1", "is_im": True, "updated": 900_000},
    ]
    warehouse.states = {
        ("zrl", "T1", "conversation", "D_CLEARED"): {
            # Cursor was clobbered to '' — the exact state that strands a DM.
            "cursor_ts": "",
            "last_sync_type": "partial",
            "status": "ok",
        }
    }
    # We have history on disk up to ts 1000; that high-water mark is the cursor we lost.
    warehouse.message_high_water = {("zrl", "T1", "D_CLEARED"): 1000.0}
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                {
                    "ok": True,
                    "messages": [{"ts": "1500.000000", "user": "U1", "text": "missed while stranded"}],
                    "response_metadata": {},
                }
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

    history_calls = [params for method, params in client.calls if method == "conversations.history"]
    assert [params["channel"] for params in history_calls] == ["D_CLEARED"]
    # Resumes from the derived high-water mark (1000), not the freshness window start
    # (1400), so the gap that accumulated while the DM was stranded is backfilled.
    assert float(history_calls[0]["oldest"]) == pytest.approx(1000.0)


def test_runner_freshness_priority_can_refresh_one_conversation_type(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
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
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.thread_refs = [
        {"conversation_id": "C1", "thread_ts": "1713974400.000100", "reply_count": 1, "latest_reply_ts": "1713974500.000100"},
        {"conversation_id": "C2", "thread_ts": "1713974600.000100", "reply_count": 1, "latest_reply_ts": "1713974700.000100"},
    ]
    warehouse.states = {
        ("zrl", "T1", "thread", "C1:1713974400.000100"): {
            "status": "ok",
            "last_sync_type": "thread_replies",
            "cursor_ts": "1713974500.000100",
        }
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


def test_runner_reprocesses_completed_thread_when_latest_reply_advances(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.thread_refs = [
        {"conversation_id": "C1", "thread_ts": "1713974400.000100", "reply_count": 2, "latest_reply_ts": "1713974600.000100"},
    ]
    warehouse.states = {
        ("zrl", "T1", "thread", "C1:1713974400.000100"): {
            "status": "ok",
            "last_sync_type": "thread_replies",
            "cursor_ts": "1713974500.000100",
        }
    }
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.replies": [
                {
                    "ok": True,
                    "messages": [
                        {"ts": "1713974400.000100", "user": "U1", "text": "root", "reply_count": 2},
                        {"ts": "1713974500.000100", "thread_ts": "1713974400.000100", "user": "U2", "text": "old reply"},
                        {"ts": "1713974600.000100", "thread_ts": "1713974400.000100", "user": "U3", "text": "new reply"},
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
        sleep=lambda seconds: None,
    ).sync_all()[0]

    assert [params["channel"] for method, params in client.calls if method == "conversations.replies"] == ["C1"]
    assert summary.messages_written == 3
    assert warehouse.state_updates[-1]["cursor_ts"] == "1713974600.000100"


def test_runner_thread_replies_only_can_skip_known_errors(monkeypatch):
    # skip_known_errors skips threads recorded as terminally 'gone'; plain
    # 'error' rows are transient and stay retryable (see the retry test below).
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.thread_refs = [
        {"conversation_id": "C_ERROR", "thread_ts": "1713974400.000100", "reply_count": 1, "latest_reply_ts": "1713974500.000100"},
        {"conversation_id": "C_OK", "thread_ts": "1713974600.000100", "reply_count": 1, "latest_reply_ts": "1713974700.000100"},
    ]
    warehouse.states = {
        ("zrl", "T1", "thread", "C_ERROR:1713974400.000100"): {
            "status": "gone",
            "last_sync_type": "thread_replies",
            "cursor_ts": "1713974400.000100",
        }
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

    SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        sync_thread_replies_only=True,
        skip_known_errors=True,
        sleep=lambda seconds: None,
    ).sync_all()

    assert warehouse.thread_ref_calls[0]["skip_known_errors"] is True
    assert [params["channel"] for method, params in client.calls if method == "conversations.replies"] == ["C_OK"]


def test_runner_thread_replies_only_retries_previously_errored_thread(monkeypatch):
    # A thread whose last attempt recorded a transient 'error' must be offered
    # again under skip_known_errors, so it can self-heal to 'ok' instead of
    # sitting in the dashboard's failing count forever.
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.thread_refs = [
        {"conversation_id": "C_RETRY", "thread_ts": "1713974400.000100", "reply_count": 1, "latest_reply_ts": "1713974500.000100"},
    ]
    warehouse.states = {
        ("zrl", "T1", "thread", "C_RETRY:1713974400.000100"): {
            "status": "error",
            "last_sync_type": "thread_replies",
            "cursor_ts": "1713974400.000100",
        }
    }
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.replies": [
                {
                    "ok": True,
                    "messages": [
                        {"ts": "1713974400.000100", "user": "U1", "text": "root", "reply_count": 1},
                        {"ts": "1713974500.000100", "thread_ts": "1713974400.000100", "user": "U2", "text": "reply"},
                    ],
                    "response_metadata": {},
                }
            ],
        }
    )

    SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        sync_thread_replies_only=True,
        skip_known_errors=True,
        sleep=lambda seconds: None,
    ).sync_all()

    assert [params["channel"] for method, params in client.calls if method == "conversations.replies"] == ["C_RETRY"]
    assert any(
        update["object_type"] == "thread" and update["status"] == "ok" and update["object_id"] == "C_RETRY:1713974400.000100"
        for update in warehouse.state_updates
    )


def test_runner_thread_replies_marks_gone_channel_inactive(monkeypatch):
    # Regression: conversations.replies failing with channel_not_found (etc.) used
    # to just record a per-thread error and move on, so every other not-yet-tried
    # thread in the same dead channel wasted its own API call on the same
    # guaranteed failure, one by one, forever. It must instead deactivate the
    # channel immediately, like the conversation-level sync passes already do.
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.thread_refs = [
        {"conversation_id": "C_GONE", "thread_ts": "1713974400.000100", "reply_count": 1, "latest_reply_ts": "1713974500.000100"},
        {"conversation_id": "C_OK", "thread_ts": "1713974600.000100", "reply_count": 1, "latest_reply_ts": "1713974700.000100"},
    ]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.replies": [
                SlackApiCallError("conversations.replies failed: channel_not_found", code="channel_not_found"),
                {
                    "ok": True,
                    "messages": [
                        {"ts": "1713974600.000100", "user": "U1", "text": "root", "reply_count": 1},
                        {"ts": "1713974700.000100", "thread_ts": "1713974600.000100", "user": "U2", "text": "reply"},
                    ],
                    "response_metadata": {},
                },
            ],
        }
    )

    SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        sync_thread_replies_only=True,
        sleep=lambda seconds: None,
    ).sync_all()

    assert warehouse.inactivated_conversations == [
        {"account": "zrl", "team_id": "T1", "conversation_id": "C_GONE"}
    ]
    # Terminal status, not 'error': the thread will never be retried, so an
    # error row would count as an active failure on the dashboard forever.
    assert any(
        update["object_type"] == "thread" and update["status"] == "gone" and update["object_id"] == "C_GONE:1713974400.000100"
        for update in warehouse.state_updates
    )
    # The run keeps going and still syncs the healthy thread.
    assert [params["channel"] for method, params in client.calls if method == "conversations.replies"] == ["C_GONE", "C_OK"]


def test_runner_thread_replies_marks_deleted_thread_gone_without_deactivating_channel(monkeypatch):
    # thread_not_found means the parent message was deleted: terminal for that
    # one thread, but the channel itself is fine and must stay active.
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.thread_refs = [
        {"conversation_id": "C1", "thread_ts": "1713974400.000100", "reply_count": 1, "latest_reply_ts": "1713974500.000100"},
        {"conversation_id": "C1", "thread_ts": "1713974600.000100", "reply_count": 1, "latest_reply_ts": "1713974700.000100"},
    ]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.replies": [
                SlackApiCallError("conversations.replies failed: thread_not_found", code="thread_not_found"),
                {
                    "ok": True,
                    "messages": [
                        {"ts": "1713974600.000100", "user": "U1", "text": "root", "reply_count": 1},
                        {"ts": "1713974700.000100", "thread_ts": "1713974600.000100", "user": "U2", "text": "reply"},
                    ],
                    "response_metadata": {},
                },
            ],
        }
    )

    SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        sync_thread_replies_only=True,
        sleep=lambda seconds: None,
    ).sync_all()

    assert warehouse.inactivated_conversations == []
    assert any(
        update["object_type"] == "thread" and update["status"] == "gone" and update["object_id"] == "C1:1713974400.000100"
        for update in warehouse.state_updates
    )
    # The channel's other threads still sync.
    assert [params["channel"] for method, params in client.calls if method == "conversations.replies"] == ["C1", "C1"]


def test_runner_thread_replies_keeps_transient_error_retryable(monkeypatch):
    # A transient failure (5xx HTML page, internal_error, ...) records status
    # 'error' — NOT the terminal 'gone' — so later passes retry it and either
    # heal it to 'ok' or keep an honest, recent failing signal on the dashboard.
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.thread_refs = [
        {"conversation_id": "C1", "thread_ts": "1713974400.000100", "reply_count": 1, "latest_reply_ts": "1713974500.000100"},
    ]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.replies": [
                SlackApiCallError("conversations.replies failed: internal_error", code="internal_error"),
            ],
        }
    )

    SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        sync_thread_replies_only=True,
        sleep=lambda seconds: None,
    ).sync_all()

    assert warehouse.inactivated_conversations == []
    assert any(
        update["object_type"] == "thread" and update["status"] == "error" and update["object_id"] == "C1:1713974400.000100"
        for update in warehouse.state_updates
    )


def test_runner_thread_replies_only_can_select_missing_replies(monkeypatch):
    warehouse = FakeWarehouse()
    warehouse.thread_refs = [
        {"conversation_id": "C1", "thread_ts": "1713974400.000100", "reply_count": 1, "latest_reply_ts": ""},
    ]
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.replies": [
                {
                    "ok": True,
                    "messages": [
                        {"ts": "1713974400.000100", "user": "U1", "text": "root"},
                        {"ts": "1713974500.000100", "thread_ts": "1713974400.000100", "user": "U2", "text": "reply"},
                    ],
                    "response_metadata": {},
                }
            ],
        }
    )

    SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        sync_thread_replies_only=True,
        thread_missing_replies_only=True,
        thread_order="oldest",
        sleep=lambda seconds: None,
    ).sync_all()

    assert warehouse.thread_ref_calls[0]["missing_replies_only"] is True
    assert warehouse.thread_ref_calls[0]["order"] == "oldest"


def test_runner_partial_sync_tombstones_missing_top_level_but_not_replies(monkeypatch):
    # Regression: when partial sync's deletion-detection compared the set of
    # `conversations.history` results against everything in DB within the
    # window (including thread replies the API never returns inline), every
    # reply in the window got tombstoned. Filter is now applied at the warehouse
    # level (is_thread_reply = 0); assert the runner relies on that and only
    # tombstones top-level messages.
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse(states={("zrl", "T1", "conversation", "C_STUCK"): {"cursor_ts": "1900.000000"}})
    warehouse.conversation_payloads = [
        {"id": "C_STUCK", "name": "large-channel", "is_channel": True, "latest": {"ts": "1999.000000"}},
    ]
    # Top-level messages the API returns now: 1999 and 1990.
    # `existing_message_ids` simulates the warehouse layer correctly filtering
    # to is_thread_reply = 0; replies must NOT appear here. The only stale
    # top-level row in the window is "1980.000000".
    warehouse.existing_message_ids = {"1990.000000", "1999.000000", "1980.000000"}
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                {
                    "ok": True,
                    "messages": [
                        {"ts": "1999.000000", "user": "U1", "text": "newest"},
                        {"ts": "1990.000000", "user": "U2", "text": "older"},
                    ],
                    "response_metadata": {},
                }
            ],
        }
    )

    SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        now=lambda: datetime.fromtimestamp(2000, tz=UTC),
        history_window=timedelta(seconds=200),
        sync_users=False,
        sync_members=False,
        use_existing_conversations=True,
        freshness_priority=True,
        sync_thread_replies=False,
        sleep=lambda seconds: None,
    ).sync_all()

    tombstones = [row for row in warehouse.messages if row.get("is_deleted") == 1]
    assert [row["message_ts"] for row in tombstones] == ["1980.000000"]


def test_runner_partial_sync_with_empty_window_does_not_clear_cursor(monkeypatch):
    # A partial freshness pass can legitimately find no messages in its recent
    # window. That must not overwrite an existing high-water cursor with "",
    # because cached public channels often lack Slack `latest.ts` metadata and
    # rely on warehouse state for prioritization/resume behavior.
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse(
        states={
            ("zrl", "T1", "conversation", "C_STALE"): {
                "cursor_ts": "1778687074.782329",
                "last_sync_type": "partial",
                "status": "ok",
            }
        }
    )
    warehouse.conversation_payloads = [
        {"id": "C_STALE", "name": "stale-channel", "is_channel": True, "latest": {"ts": "1779999999.000000"}},
    ]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [{"ok": True, "messages": [], "response_metadata": {}}],
        }
    )

    summary = SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        now=lambda: datetime.fromtimestamp(1_780_001_000, tz=UTC),
        history_window=timedelta(hours=2),
        sync_users=False,
        sync_members=False,
        use_existing_conversations=True,
        freshness_priority=True,
        sync_thread_replies=False,
        sleep=lambda seconds: None,
    ).sync_all()[0]

    assert summary.conversations_seen == 1
    assert summary.messages_written == 0
    assert warehouse.messages == []
    assert [
        update
        for update in warehouse.state_updates
        if update["object_type"] == "conversation" and update["object_id"] == "C_STALE"
    ] == []


def test_runner_partial_sync_persists_progress_across_pages(monkeypatch):
    # Regression: when a partial sync of a huge channel exhausted the rate-limit
    # budget mid-page, the old materialize-all-then-write approach lost the
    # cursor advance, so the next pass re-fetched the same window and got stuck
    # in the same place. The new streaming partial sync writes rows and updates
    # the cursor per page so progress survives the abort.
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.conversation_payloads = [
        {"id": "C_BIG", "name": "large-channel", "is_channel": True, "latest": {"ts": "5000.000000"}},
    ]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            # First page returns messages and a next_cursor; the budget is then
            # exhausted before the second page is fetched.
            "conversations.history": [
                {
                    "ok": True,
                    "messages": [{"ts": "3000.000000", "user": "U1", "text": "first"}],
                    "response_metadata": {"next_cursor": "next"},
                },
                SlackRateLimitedError(retry_after=999),
            ],
        }
    )

    sleeps: list[int] = []
    with pytest.raises(RuntimeError):
        SlackSyncRunner(
            settings=settings,
            warehouse=warehouse,
            logger=NullLogger(),
            client_factory=lambda account: client,
            now=lambda: datetime.fromtimestamp(3500, tz=UTC),
            history_window=timedelta(seconds=2000),
            sync_users=False,
            sync_members=False,
            use_existing_conversations=True,
            freshness_priority=True,
            sync_thread_replies=False,
            sleep=sleeps.append,
            max_rate_limit_sleep_seconds=10,
        ).sync_all()

    # Page 1 messages must have been persisted before the abort.
    assert any(row["message_ts"] == "3000.000000" for row in warehouse.messages)
    # And the cursor must have advanced to the page 1 high-water mark, so the
    # next pass continues forward instead of restarting.
    assert any(
        update["object_id"] == "C_BIG" and update["cursor_ts"] == "3000.000000"
        for update in warehouse.state_updates
    )


def thread_state_covers_ref(state, ref):
    if state.get("status") != "ok":
        return False
    latest_reply_ts = str(ref.get("latest_reply_ts") or "")
    cursor_ts = str(state.get("cursor_ts") or "")
    if not latest_reply_ts or not cursor_ts:
        return True
    return float(cursor_ts) >= float(latest_reply_ts)


def _conversation_refresh_runner(monkeypatch, *, client, warehouse, page_limit=1, types=("mpim",)):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    return SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        conversation_page_limit=page_limit,
        conversation_types=types,
        sync_conversations_only=True,
        sleep=lambda seconds: None,
    )


def _auth_pages():
    return {
        "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club", "user_id": "U1"}],
        "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club", "domain": "hackclub"}}],
    }


def test_conversation_refresh_records_its_next_cursor(monkeypatch):
    # A bounded metadata pass must remember where it stopped. Without this the
    # walk restarts at page 1 every run, so a workspace with more conversations
    # of one type than a single page can never discover the rest: in production
    # 172 group DMs created after the last full walk were invisible for months.
    client = FakeSlackClient(
        {
            **_auth_pages(),
            "conversations.list": [
                {
                    "ok": True,
                    "channels": [{"id": "C1", "name": "mpdm-zach--alpha-1", "is_mpim": True}],
                    "response_metadata": {"next_cursor": "page2"},
                }
            ],
        }
    )
    warehouse = FakeWarehouse()

    _conversation_refresh_runner(monkeypatch, client=client, warehouse=warehouse).sync_all()

    cursor_updates = [u for u in warehouse.state_updates if u["object_type"] == "conversation_list"]
    assert cursor_updates, "the conversation walk must persist its cursor"
    assert cursor_updates[-1]["object_id"] == "mpim"
    assert cursor_updates[-1]["cursor_ts"] == "page2"
    assert cursor_updates[-1]["status"] == "ok"


def test_conversation_refresh_resumes_from_the_stored_cursor(monkeypatch):
    # The regression that hid 172 group DMs: page 1 of conversations.list for
    # mpim is entirely conversations we already have, and every new one lives on
    # the last pages. Resuming from the stored cursor is what reaches them.
    client = FakeSlackClient(
        {
            **_auth_pages(),
            "conversations.list": [
                {
                    "ok": True,
                    "channels": [{"id": "C_NEW", "name": "mpdm-zach--alpha--bravo-1", "is_mpim": True}],
                    "response_metadata": {"next_cursor": ""},
                }
            ],
        }
    )
    warehouse = FakeWarehouse(
        states={
            ("zrl", "T1", "conversation_list", "mpim"): {
                "cursor_ts": "page12",
                "status": "ok",
            }
        }
    )

    # A finished walk keeps its last cursor_ts on the row (the upsert preserves
    # it), so 'complete' is the only thing that can mean "start over".

    _conversation_refresh_runner(monkeypatch, client=client, warehouse=warehouse).sync_all()

    list_calls = [params for method, params in client.calls if method == "conversations.list"]
    assert list_calls[0]["cursor"] == "page12"
    assert [row["conversation_id"] for row in warehouse.conversations] == ["C_NEW"]


def test_conversation_refresh_restarts_the_walk_after_the_last_page(monkeypatch):
    # Slack signals the end of the list with an empty cursor, and the next pass
    # must start over at page 1 so the walk keeps cycling and re-stamps older
    # conversations. That completion CANNOT be recorded by blanking cursor_ts:
    # ops.slack_sync_state preserves a non-empty cursor_ts against an empty
    # write (so a per-conversation error row cannot wipe a message high-water
    # mark), so the blank is silently dropped and the walk would stay pinned to
    # the last page forever. It is recorded in `status` instead.
    client = FakeSlackClient(
        {
            **_auth_pages(),
            "conversations.list": [
                {
                    "ok": True,
                    "channels": [{"id": "C_LAST", "name": "mpdm-last-1", "is_mpim": True}],
                    "response_metadata": {"next_cursor": ""},
                }
            ],
        }
    )
    warehouse = FakeWarehouse(states={("zrl", "T1", "conversation_list", "mpim"): {"cursor_ts": "page14"}})

    _conversation_refresh_runner(monkeypatch, client=client, warehouse=warehouse).sync_all()

    cursor_updates = [u for u in warehouse.state_updates if u["object_type"] == "conversation_list"]
    assert cursor_updates[-1]["status"] == "complete"


def test_conversation_refresh_restarts_when_slack_rejects_a_stale_cursor(monkeypatch):
    # Slack cursors expire. A rejected cursor must restart the walk rather than
    # wedge discovery for that conversation type forever.
    client = FakeSlackClient(
        {
            **_auth_pages(),
            "conversations.list": [
                SlackApiCallError("conversations.list failed: invalid_cursor", code="invalid_cursor"),
                {
                    "ok": True,
                    "channels": [{"id": "C_FIRST", "name": "mpdm-first-1", "is_mpim": True}],
                    "response_metadata": {"next_cursor": "page2"},
                },
            ],
        }
    )
    warehouse = FakeWarehouse(states={("zrl", "T1", "conversation_list", "mpim"): {"cursor_ts": "expired"}})

    _conversation_refresh_runner(monkeypatch, client=client, warehouse=warehouse).sync_all()

    list_calls = [params for method, params in client.calls if method == "conversations.list"]
    assert list_calls[0]["cursor"] == "expired"
    assert list_calls[1]["cursor"] == ""
    assert [row["conversation_id"] for row in warehouse.conversations] == ["C_FIRST"]


def test_metadata_rotation_prefers_the_conversation_type_furthest_behind(monkeypatch):
    # Wall-clock rotation gives each conversation type one 15-minute slot an hour
    # and silently forfeits it whenever that run loses the shared Slack lock —
    # which in production was most of them, leaving mpim metadata 11.5h stale.
    # Driving the rotation from persisted state instead makes a lost slot
    # recoverable on the very next metadata run.
    from personal_data_warehouse.defs.slack_sync import _metadata_conversation_types

    now = datetime(2026, 8, 24, 1, 45, tzinfo=UTC)  # a wall-clock 'public_channel' slot
    warehouse = FakeWarehouse(
        states={
            ("zrl", "T1", "conversation_list", "im"): {
                "cursor_ts": "", "status": "complete", "updated_at": now - timedelta(minutes=5)},
            ("zrl", "T1", "conversation_list", "mpim"): {
                "cursor_ts": "", "status": "complete", "updated_at": now - timedelta(hours=11)},
            ("zrl", "T1", "conversation_list", "private_channel"): {
                "cursor_ts": "", "status": "complete", "updated_at": now - timedelta(minutes=20)},
            ("zrl", "T1", "conversation_list", "public_channel"): {
                "cursor_ts": "", "status": "complete", "updated_at": now - timedelta(minutes=30)},
        }
    )

    assert _metadata_conversation_types(warehouse=warehouse, now=now) == ("mpim",)


def test_metadata_rotation_finishes_a_started_walk_before_moving_on(monkeypatch):
    # A type mid-walk holds a live cursor. Finishing that walk is what actually
    # reaches the last pages, where every newly created conversation lives.
    from personal_data_warehouse.defs.slack_sync import _metadata_conversation_types

    now = datetime(2026, 8, 24, 1, 45, tzinfo=UTC)
    warehouse = FakeWarehouse(
        states={
            ("zrl", "T1", "conversation_list", "im"): {
                "cursor_ts": "", "status": "complete", "updated_at": now - timedelta(hours=9)},
            ("zrl", "T1", "conversation_list", "mpim"): {
                "cursor_ts": "page12", "status": "ok", "updated_at": now - timedelta(minutes=1)},
            ("zrl", "T1", "conversation_list", "private_channel"): {
                "cursor_ts": "", "status": "complete", "updated_at": now - timedelta(hours=8)},
            ("zrl", "T1", "conversation_list", "public_channel"): {
                "cursor_ts": "", "status": "complete", "updated_at": now - timedelta(hours=7)},
        }
    )

    assert _metadata_conversation_types(warehouse=warehouse, now=now) == ("mpim",)


def test_metadata_rotation_falls_back_to_the_clock_without_state(monkeypatch):
    # A warehouse that cannot answer (or a first run with no rows) must still
    # rotate rather than pinning one type forever.
    from personal_data_warehouse.defs.slack_sync import _metadata_conversation_types
    from types import SimpleNamespace

    now = datetime(2026, 4, 24, 17, 15, tzinfo=UTC)
    assert _metadata_conversation_types(warehouse=SimpleNamespace(), now=now) == ("mpim",)
    assert _metadata_conversation_types(warehouse=FakeWarehouse(), now=now) == ("im",)


def test_conversation_refresh_starts_over_when_the_previous_walk_completed(monkeypatch):
    # cursor_ts survives on the row after a completed walk because the upsert
    # preserves it. Only status='complete' can distinguish "finished the list,
    # begin a new cycle" from "stopped mid-list, resume here".
    client = FakeSlackClient(
        {
            **_auth_pages(),
            "conversations.list": [
                {
                    "ok": True,
                    "channels": [{"id": "C_PAGE1", "name": "mpdm-page-one-1", "is_mpim": True}],
                    "response_metadata": {"next_cursor": "page2"},
                }
            ],
        }
    )
    warehouse = FakeWarehouse(
        states={
            ("zrl", "T1", "conversation_list", "mpim"): {
                "cursor_ts": "page14",
                "status": "complete",
            }
        }
    )

    _conversation_refresh_runner(monkeypatch, client=client, warehouse=warehouse).sync_all()

    list_calls = [params for method, params in client.calls if method == "conversations.list"]
    assert list_calls[0]["cursor"] == ""


def test_coverage_rotation_prefers_the_stage_that_has_waited_longest(monkeypatch):
    # Coverage rotates over seven stages on a wall-clock slot, and a run that
    # loses the shared Slack lock still returns a green MaterializeResult having
    # done nothing -- so that stage's slot is simply forfeited. Measured in
    # production over six hours, 38 of 54 coverage runs (70%) were lock-skipped
    # no-ops, and unarchived public channels only get two slots an hour, so a
    # 1,929-channel backlog drained at roughly one channel per hour. Choosing the
    # stage from persisted state makes a lost slot recoverable on the next run.
    from personal_data_warehouse.defs.slack_sync import _coverage_stage

    now = datetime(2026, 8, 24, 15, 30, tzinfo=UTC)  # a wall-clock archived-public slot
    warehouse = FakeWarehouse(
        states={
            ("zrl", "T1", "coverage_stage", "public_channel"): {"updated_at": now - timedelta(hours=9)},
            ("zrl", "T1", "coverage_stage", "mpim"): {"updated_at": now - timedelta(minutes=5)},
            ("zrl", "T1", "coverage_stage", "private_channel"): {"updated_at": now - timedelta(minutes=20)},
            ("zrl", "T1", "coverage_stage", "private_channel_archived"): {"updated_at": now - timedelta(minutes=30)},
            ("zrl", "T1", "coverage_stage", "public_channel_archived_zero"): {"updated_at": now - timedelta(minutes=40)},
            ("zrl", "T1", "coverage_stage", "public_channel_archived"): {"updated_at": now - timedelta(minutes=50)},
            ("zrl", "T1", "coverage_stage", "im"): {"updated_at": now - timedelta(hours=1)},
        }
    )

    stage = _coverage_stage(warehouse=warehouse, now=now)
    assert stage["key"] == "public_channel"
    assert stage["conversation_types"] == ("public_channel",)
    assert stage["archived_only"] is False


def test_coverage_rotation_runs_a_never_run_stage_first(monkeypatch):
    from personal_data_warehouse.defs.slack_sync import _coverage_stage

    now = datetime(2026, 8, 24, 15, 30, tzinfo=UTC)
    warehouse = FakeWarehouse(
        states={
            ("zrl", "T1", "coverage_stage", "public_channel"): {"updated_at": now - timedelta(hours=9)},
        }
    )
    # Every other stage has no row at all, so one of those wins over the
    # nine-hour-old public_channel stage.
    assert _coverage_stage(warehouse=warehouse, now=now)["key"] != "public_channel"


def test_coverage_rotation_falls_back_to_the_clock_without_state(monkeypatch):
    from personal_data_warehouse.defs.slack_sync import _coverage_stage, _coverage_stage_for_time
    from types import SimpleNamespace

    now = datetime(2026, 8, 24, 15, 30, tzinfo=UTC)
    assert _coverage_stage(warehouse=SimpleNamespace(), now=now) == _coverage_stage_for_time(now)


def test_coverage_sync_records_the_stage_it_ran(monkeypatch):
    # The rotation can only be state-driven if each run writes its own state.
    import personal_data_warehouse.defs.slack_sync as slack_defs
    from personal_data_warehouse.slack_sync import SlackSyncSummary

    calls = []

    class FakeRunner:
        def __init__(self, **kwargs):
            calls.append(kwargs)

        def sync_all(self):
            return [
                SlackSyncSummary(
                    account="zrl", team_id="T1", sync_type="partial",
                    conversations_seen=25, messages_written=3, users_written=0, files_written=0,
                )
            ]

    monkeypatch.setattr(slack_defs, "SlackSyncRunner", FakeRunner)
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()

    slack_defs.run_slack_coverage_sync(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        now=datetime(2026, 8, 24, 15, 30, tzinfo=UTC),
    )

    stage_writes = [u for u in warehouse.state_updates if u["object_type"] == "coverage_stage"]
    assert len(stage_writes) == 1
    assert stage_writes[0]["object_id"] == calls[0]["conversation_types"][0] or stage_writes[0]["object_id"]


def test_freshness_pass_restricts_candidates_to_the_changed_conversations(monkeypatch):
    """The change feed only helps if the freshness loader actually gets the ids.

    _sync_account_freshness_priority loads its own candidates rather than going
    through the generic path in _sync_account, so wiring the filter into one of
    them leaves the other polling everything. In production that looked like
    success -- the run logged "change feed: 690 covered, 175 changed" and then
    fetched 413 conversations anyway.
    """
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club", "user_id": "U1"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club", "domain": "hackclub"}}],
            # Nothing is cached here, so the pass also looks the id up on demand -- see
            # test_runner_freshness_priority_discovers_a_conversation_the_change_feed_names_but_never_cached.
            "conversations.info": [{"ok": True, "channel": {"id": "D_CHANGED", "user": "U2", "is_im": True}}],
            "conversations.history": [{"ok": True, "messages": [], "response_metadata": {}}],
        }
    )
    warehouse = FakeWarehouse()

    SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        freshness_priority=True,
        use_existing_conversations=True,
        sync_users=False,
        sync_members=False,
        conversation_types=("im",),
        conversation_ids=("D_CHANGED",),
        sync_thread_replies=False,
        sleep=lambda seconds: None,
    ).sync_all()

    assert warehouse.conversation_payload_calls, "the freshness pass must load candidates"
    assert warehouse.conversation_payload_calls[0]["conversation_ids"] == ("D_CHANGED",)


def test_freshness_does_not_share_a_lock_with_the_slow_slack_sweeps():
    """The stage that sets DM latency must not queue behind the sweeps.

    Every Slack stage used to serialize on one advisory lock with a
    NON-BLOCKING try, so `slack_workspace_sync` (freshness) simply forfeited
    its turn whenever coverage, threads-backfill or metadata held it -- and
    still returned a green MaterializeResult. Measured in production
    2026-08-26: freshness executed 84 of 225 ticks in 24h (63% were
    `skipped_due_to_lock` no-ops), with p50 15 min, p90 30 min and max 160 min
    between real executions. DM ingest latency was p50 13.4 min but **p95 10.3
    days**, and 13.6% of DMs landed more than a day late.

    Serializing it was right when freshness called conversations.history on
    ~950 conversations against a ~39/min ceiling. The client.counts change feed
    ended that: production logs show it fetching 11-43 of ~690 conversations
    per tick. The sweeps still serialize against each other; freshness no
    longer waits for them.
    """

    from personal_data_warehouse.defs.slack_sync import (
        SLACK_FRESHNESS_POSTGRES_LOCK_ID,
        SLACK_SYNC_POSTGRES_LOCK_ID,
    )
    from personal_data_warehouse.sync_locks import lock_env_prefix, sync_lock_path

    assert SLACK_FRESHNESS_POSTGRES_LOCK_ID != SLACK_SYNC_POSTGRES_LOCK_ID, (
        "freshness must hold a different advisory lock id than the sweeps, or it "
        "keeps losing its turn to them"
    )
    # The file-lock fallback has to separate too, or a host without
    # DAGSTER_POSTGRES_URL silently reintroduces the shared lock.
    assert sync_lock_path("slack-freshness") != sync_lock_path("slack")
    # The lock name has to survive env-prefix normalisation as its own key.
    assert lock_env_prefix("slack-freshness") == "SLACK_FRESHNESS"


def test_only_the_freshness_stage_gets_the_freshness_lock():
    """The sweeps must stay serialized with each other.

    Giving every stage its own lock would let coverage, threads-backfill and
    metadata run concurrently and multiply Slack API concurrency against a
    ~39 conversations.history calls/min ceiling -- the oversubscription the
    change feed was built to end. Only freshness is cheap enough to run beside
    them.
    """

    import inspect

    from personal_data_warehouse.defs import slack_sync

    source = inspect.getsource(slack_sync)
    assert source.count("SLACK_FRESHNESS_POSTGRES_LOCK_ID") == 2, (
        "the freshness lock id should be defined once and used by exactly one "
        "stage; another stage adopting it would un-serialize the sweeps"
    )


def _thread_backfill_runner(settings, warehouse, client, now):
    from personal_data_warehouse.slack_sync import SlackSyncRunner

    return SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        now=lambda: now,
        sync_thread_replies_only=True,
        skip_completed_threads=True,
        skip_known_errors=True,
        thread_limit=100,
        thread_missing_replies_only=True,
        sleep=lambda seconds: None,
    )


def test_drained_thread_backfill_walk_is_remembered_and_bounded_until_cooldown(monkeypatch):
    """The unbounded missing-replies walk reads every thread parent in the
    workspace. Once it comes up empty it must not run again every five
    minutes; only the recent window is checked until the cooldown elapses."""

    from personal_data_warehouse.slack_sync import (
        THREAD_BACKFILL_DRAINED_COOLDOWN,
        THREAD_BACKFILL_RECENT_WINDOW,
        THREAD_BACKFILL_WALK_ID,
        THREAD_BACKFILL_WALK_TYPE,
    )

    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}] * 3,
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}] * 3,
        }
    )
    now = datetime(2026, 8, 26, 12, tzinfo=UTC)

    # First walk: unbounded, finds nothing -> the drained marker is written.
    warehouse = FakeWarehouse()
    _thread_backfill_runner(settings, warehouse, client, now).sync_all()
    assert warehouse.thread_ref_calls[0]["since_ts"] is None
    marker = [
        u for u in warehouse.state_updates
        if u["object_type"] == THREAD_BACKFILL_WALK_TYPE and u["object_id"] == THREAD_BACKFILL_WALK_ID
    ]
    assert len(marker) == 1 and marker[0]["status"] == "ok"

    # Inside the cooldown: only the recent window is walked, no new marker.
    warehouse = FakeWarehouse(
        states={("zrl", "T1", THREAD_BACKFILL_WALK_TYPE, THREAD_BACKFILL_WALK_ID): {
            "status": "ok", "updated_at": now,
        }}
    )
    later = now + timedelta(hours=1)
    _thread_backfill_runner(settings, warehouse, client, later).sync_all()
    expected_since = later.timestamp() - THREAD_BACKFILL_RECENT_WINDOW.total_seconds()
    assert warehouse.thread_ref_calls[0]["since_ts"] == expected_since
    assert not [u for u in warehouse.state_updates if u["object_type"] == THREAD_BACKFILL_WALK_TYPE]

    # After the cooldown: the full walk runs again.
    warehouse = FakeWarehouse(
        states={("zrl", "T1", THREAD_BACKFILL_WALK_TYPE, THREAD_BACKFILL_WALK_ID): {
            "status": "ok", "updated_at": now,
        }}
    )
    much_later = now + THREAD_BACKFILL_DRAINED_COOLDOWN + timedelta(minutes=1)
    _thread_backfill_runner(settings, warehouse, client, much_later).sync_all()
    assert warehouse.thread_ref_calls[0]["since_ts"] is None


def test_full_thread_backfill_walk_that_hits_its_limit_saves_a_keyset_cursor(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    from personal_data_warehouse.slack_sync import (
        THREAD_BACKFILL_WALK_ID,
        THREAD_BACKFILL_WALK_TYPE,
    )

    warehouse = FakeWarehouse()
    warehouse.thread_refs = [
        {"conversation_id": "C1", "thread_ts": f"17139744{i:02d}.000100", "reply_count": 1, "latest_reply_ts": ""}
        for i in range(100)
    ]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.replies": [
                {"ok": True, "messages": [], "response_metadata": {}} for _ in range(100)
            ],
        }
    )
    _thread_backfill_runner(settings, warehouse, client, datetime(2026, 8, 26, tzinfo=UTC)).sync_all()
    markers = [
        update
        for update in warehouse.state_updates
        if update["object_type"] == THREAD_BACKFILL_WALK_TYPE
        and update["object_id"] == THREAD_BACKFILL_WALK_ID
    ]
    assert len(markers) == 1
    assert markers[0]["status"] == "running"
    assert markers[0]["cursor_ts"] == "1713974499.000100|C1"


def test_thread_backfill_walk_resumes_after_its_saved_keyset(monkeypatch):
    from personal_data_warehouse.slack_sync import (
        THREAD_BACKFILL_WALK_ID,
        THREAD_BACKFILL_WALK_TYPE,
    )

    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse(
        states={
            ("zrl", "T1", THREAD_BACKFILL_WALK_TYPE, THREAD_BACKFILL_WALK_ID): {
                "status": "running",
                "cursor_ts": "1713974499.000100|C1",
                "updated_at": datetime(2026, 8, 26, tzinfo=UTC),
            }
        }
    )
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
        }
    )

    _thread_backfill_runner(
        settings, warehouse, client, datetime(2026, 8, 26, 0, 5, tzinfo=UTC)
    ).sync_all()

    call = warehouse.thread_ref_calls[0]
    assert call["since_ts"] is None
    assert call["before_thread_ts"] == "1713974499.000100"
    assert call["before_conversation_id"] == "C1"


def test_thread_backfill_cursor_does_not_advance_past_a_transient_error(monkeypatch):
    """The persisted walk must leave a failed parent visible for retry."""

    from personal_data_warehouse.slack_sync import (
        THREAD_BACKFILL_WALK_ID,
        THREAD_BACKFILL_WALK_TYPE,
    )

    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.thread_refs = [
        {
            "conversation_id": "C1",
            "thread_ts": "1713974500.000100",
            "reply_count": 1,
            "latest_reply_ts": "",
        },
        {
            "conversation_id": "C1",
            "thread_ts": "1713974400.000100",
            "reply_count": 1,
            "latest_reply_ts": "",
        },
    ]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.replies": [
                {"ok": True, "messages": [], "response_metadata": {}},
                SlackApiCallError(
                    "conversations.replies failed: internal_error",
                    code="internal_error",
                ),
            ],
        }
    )

    _thread_backfill_runner(
        settings, warehouse, client, datetime(2026, 8, 26, tzinfo=UTC)
    ).sync_all()

    marker = next(
        update
        for update in warehouse.state_updates
        if update["object_type"] == THREAD_BACKFILL_WALK_TYPE
        and update["object_id"] == THREAD_BACKFILL_WALK_ID
    )
    assert marker["status"] == "running"
    assert marker["cursor_ts"] == "1713974500.000100|C1"


def _sweep_settings(monkeypatch):
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    return load_settings(require_postgres=False, require_gmail=False, require_slack=True)


def test_runner_public_sweep_resumes_a_frozen_non_member_channel_from_its_cursor(monkeypatch):
    # The gap this stage exists to close: a public channel Zach is not in is
    # backfilled once, marked 'full', and then never offered to coverage or the
    # change feed again. The sweep must poll it anyway, resuming at its cursor so
    # a quiet channel costs one call rather than a re-stream of its history.
    settings = _sweep_settings(monkeypatch)
    warehouse = FakeWarehouse(
        states={
            ("zrl", "T1", "conversation", "C_FROZEN"): {
                "status": "ok",
                "last_sync_type": "full",
                "cursor_ts": "1713974400.000100",
            }
        }
    )
    warehouse.public_sweep_payloads = [{"id": "C_FROZEN", "name": "not-a-member", "is_channel": True}]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                {
                    "ok": True,
                    "messages": [{"ts": "1713980000.000100", "user": "U1", "text": "posted while frozen"}],
                    "response_metadata": {},
                }
            ],
        }
    )

    summaries = SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        sync_users=False,
        sync_members=False,
        sync_public_sweep_only=True,
        sweep_hot_within_days=7,
        sweep_hot_limit=30,
        sweep_cold_limit=20,
        sync_thread_replies=False,
        sleep=lambda seconds: None,
    ).sync_all()

    assert warehouse.public_sweep_calls == [
        {
            "account": "zrl",
            "team_id": "T1",
            "hot_within_days": 7,
            "hot_limit": 30,
            "cold_limit": 20,
        }
    ]
    history_calls = [params for method, params in client.calls if method == "conversations.history"]
    assert [params["channel"] for params in history_calls] == ["C_FROZEN"]
    # Resumed at the cursor, not restreamed from the beginning of history.
    assert history_calls[0]["oldest"] == pytest.approx(1713974400.000100)
    assert [row["message_ts"] for row in warehouse.messages] == ["1713980000.000100"]
    assert summaries[0].sync_type == "public_sweep"
    assert summaries[0].conversations_seen == 1


def test_runner_public_sweep_stamps_a_poll_that_found_nothing(monkeypatch):
    # Candidates are ordered by when they were last polled, and a poll that
    # returns no messages writes no cursor. Without an explicit stamp the same
    # quiet channels sort first on every run and the other ~13k are never
    # reached, which is the failure mode the rotation exists to avoid.
    settings = _sweep_settings(monkeypatch)
    warehouse = FakeWarehouse(
        states={
            ("zrl", "T1", "conversation", "C_QUIET"): {
                "status": "ok",
                "last_sync_type": "full",
                "cursor_ts": "1713974400.000100",
            }
        }
    )
    warehouse.public_sweep_payloads = [{"id": "C_QUIET", "name": "quiet", "is_channel": True}]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [{"ok": True, "messages": [], "response_metadata": {}}],
        }
    )

    SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda account: client,
        sync_users=False,
        sync_members=False,
        sync_public_sweep_only=True,
        sweep_hot_limit=30,
        sweep_cold_limit=20,
        sync_thread_replies=False,
        sleep=lambda seconds: None,
    ).sync_all()

    assert warehouse.messages == []
    assert [touch["conversation_id"] for touch in warehouse.conversation_touches] == ["C_QUIET"]


def test_runner_public_sweep_streams_a_channel_that_has_never_been_synced(monkeypatch):
    # New channels are discovered constantly (601 created in July 2026 had still
    # never been fetched by 2026-08-27). One with no cursor has no history at
    # all, so the sweep streams it in full rather than asking for messages since
    # an epoch it does not have.
    settings = _sweep_settings(monkeypatch)
    warehouse = FakeWarehouse()
    warehouse.public_sweep_payloads = [{"id": "C_NEW", "name": "brand-new", "is_channel": True}]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                {
                    "ok": True,
                    "messages": [{"ts": "1713974400.000100", "user": "U1", "text": "first post"}],
                    "response_metadata": {},
                }
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
        sync_public_sweep_only=True,
        sweep_hot_limit=30,
        sweep_cold_limit=20,
        sync_thread_replies=False,
        sleep=lambda seconds: None,
    ).sync_all()

    history_calls = [params for method, params in client.calls if method == "conversations.history"]
    assert history_calls[0].get("oldest") is None
    assert [row["message_ts"] for row in warehouse.messages] == ["1713974400.000100"]
    assert [touch["conversation_id"] for touch in warehouse.conversation_touches] == ["C_NEW"]


def test_runner_public_sweep_records_a_gone_channel_and_keeps_going(monkeypatch):
    settings = _sweep_settings(monkeypatch)
    warehouse = FakeWarehouse()
    warehouse.public_sweep_payloads = [
        {"id": "C_GONE", "name": "gone", "is_channel": True},
        {"id": "C_LIVE", "name": "live", "is_channel": True},
    ]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                SlackApiCallError("conversations.history failed: channel_not_found", code="channel_not_found"),
                {
                    "ok": True,
                    "messages": [{"ts": "1713974400.000100", "user": "U1", "text": "still here"}],
                    "response_metadata": {},
                },
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
        sync_public_sweep_only=True,
        sweep_hot_limit=30,
        sweep_cold_limit=20,
        sync_thread_replies=False,
        sleep=lambda seconds: None,
    ).sync_all()

    assert warehouse.inactivated_conversations == [
        {"account": "zrl", "team_id": "T1", "conversation_id": "C_GONE"}
    ]
    assert [row["message_ts"] for row in warehouse.messages] == ["1713974400.000100"]
    # The gone channel is recorded by the error path, not stamped as polled.
    assert [touch["conversation_id"] for touch in warehouse.conversation_touches] == ["C_LIVE"]


def test_runner_public_sweep_keeps_a_completed_backfill_marked_full(monkeypatch):
    # Coverage selects backfill candidates with NOT (status ok AND type full).
    # Topping a channel up does not make its history incomplete, and demoting it
    # would hand coverage all ~13k public channels to re-walk out of the same
    # rate budget the sweep needs.
    settings = _sweep_settings(monkeypatch)
    warehouse = FakeWarehouse(
        states={
            ("zrl", "T1", "conversation", "C_FULL"): {
                "status": "ok",
                "last_sync_type": "full",
                "cursor_ts": "1713974400.000100",
            },
            ("zrl", "T1", "conversation", "C_PARTIAL"): {
                "status": "ok",
                "last_sync_type": "partial",
                "cursor_ts": "1713974400.000100",
            },
        }
    )
    warehouse.public_sweep_payloads = [
        {"id": "C_FULL", "name": "backfilled", "is_channel": True},
        {"id": "C_PARTIAL", "name": "mid-backfill", "is_channel": True},
    ]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                {"ok": True, "messages": [{"ts": "1713980000.000100", "user": "U1", "text": "a"}], "response_metadata": {}},
                {"ok": True, "messages": [{"ts": "1713980000.000200", "user": "U1", "text": "b"}], "response_metadata": {}},
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
        sync_public_sweep_only=True,
        sweep_hot_limit=30,
        sweep_cold_limit=20,
        sync_thread_replies=False,
        sleep=lambda seconds: None,
    ).sync_all()

    written = {update["object_id"]: update["last_sync_type"] for update in warehouse.state_updates}
    assert written["C_FULL"] == "full"
    # An unfinished backfill stays unfinished, so coverage keeps working on it.
    assert written["C_PARTIAL"] == "partial"


# --- coverage must backfill below a cursor that was set without history ------


def _coverage_runner(settings, warehouse, client, **overrides):
    kwargs = dict(
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
    )
    kwargs.update(overrides)
    return SlackSyncRunner(**kwargs)


def _history_calls(client):
    return [params for method, params in client.calls if method == "conversations.history"]


def test_member_channel_first_seen_by_freshness_is_backfilled_below_its_floor_by_coverage(monkeypatch):
    """The 2026-08-28 hole: discovered late, cursor at "now", history never fetched.

    A member channel that discovery first lists months after its creation
    reaches the freshness stage through the change feed. Freshness fetches its
    four-hour window, persists the newest message as the cursor and the state
    as ``partial`` -- correct so far. Coverage then selected it (not full) and
    topped it up from ``cursor - lookback``, which never reaches further back
    than the window it already had, so the channel stayed ``partial`` forever
    with nothing older than the day it was discovered. Eight of fifteen such
    channels were in that state in production, one of them a 3k-messages-a-day
    channel created in May holding nothing before 08-25.

    Coverage must read the floor -- the oldest message stored -- and stream
    everything older, leaving the forward cursor alone, until the start of the
    conversation marks it ``full``.
    """
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    now = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)
    channel = {"id": "C_LATE", "name": "late-discovered", "is_channel": True, "is_member": True}

    # Step 1: the change feed names the channel; freshness reads its window.
    freshness_warehouse = FakeWarehouse()
    freshness_warehouse.conversation_payloads = [channel]
    freshness_client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club", "user_id": "U1"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club", "domain": "hackclub"}}],
            "conversations.history": [
                {
                    "ok": True,
                    "messages": [
                        {"ts": "1787832000.000200", "user": "U2", "text": "newest"},
                        {"ts": "1787830000.000100", "user": "U2", "text": "older, still in window"},
                    ],
                    "response_metadata": {},
                }
            ],
        }
    )
    SlackSyncRunner(
        settings=settings,
        warehouse=freshness_warehouse,
        logger=NullLogger(),
        client_factory=lambda account: freshness_client,
        now=lambda: now,
        history_window=timedelta(hours=4),
        freshness_priority=True,
        use_existing_conversations=True,
        conversation_types=("public_channel",),
        conversation_ids=("C_LATE",),
        sync_users=False,
        sync_members=False,
        sync_thread_replies=False,
        sleep=lambda seconds: None,
    ).sync_all()

    window_call = _history_calls(freshness_client)[0]
    assert "oldest" in window_call, "freshness reads a window, not the whole history"
    state_write = [u for u in freshness_warehouse.state_updates if u["object_type"] == "conversation"][-1]
    assert state_write["cursor_ts"] == "1787832000.000200"
    assert state_write["last_sync_type"] == "partial", "a windowed first read must not claim complete history"

    # Step 2: coverage sees exactly that state shape, plus the messages the
    # window stored -- the oldest of which is the floor.
    coverage_warehouse = FakeWarehouse(
        states={
            ("zrl", "T1", "conversation", "C_LATE"): {
                "cursor_ts": state_write["cursor_ts"],
                "last_sync_type": state_write["last_sync_type"],
                "status": state_write["status"],
            }
        }
    )
    coverage_warehouse.conversation_payloads = [channel]
    coverage_warehouse.message_low_water[("zrl", "T1", "C_LATE")] = "1787830000.000100"
    coverage_client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                {
                    "ok": True,
                    "messages": [{"ts": "1779000000.000100", "user": "U2", "text": "from May"}],
                    "response_metadata": {"next_cursor": "page2"},
                },
                {
                    "ok": True,
                    "messages": [{"ts": "1778000000.000100", "user": "U2", "text": "first ever"}],
                    "response_metadata": {},
                },
            ],
        }
    )
    summary = _coverage_runner(settings, coverage_warehouse, coverage_client, now=lambda: now).sync_all()[0]

    assert coverage_warehouse.low_water_calls == [["C_LATE"]]
    calls = _history_calls(coverage_client)
    assert calls, "coverage must still select a conversation whose history is incomplete"
    assert calls[0]["latest"] == "1787830000.000100", "the walk starts below the oldest stored message"
    assert "oldest" not in calls[0], "a backfill has no lower bound: it runs to the start of the conversation"
    assert summary.messages_written == 2
    assert {row["message_ts"] for row in coverage_warehouse.messages} == {"1779000000.000100", "1778000000.000100"}

    state_writes = [u for u in coverage_warehouse.state_updates if u["object_type"] == "conversation"]
    assert all(u["cursor_ts"] == "" for u in state_writes), (
        "the forward cursor is preserved by the upsert on empty, and must never be regressed to May"
    )
    assert state_writes[-1]["last_sync_type"] == "full"
    assert state_writes[-1]["status"] == "ok"
    assert summary.sync_type == "backfill"


def test_coverage_backfill_resumes_from_the_floor_after_a_rate_limit_abort(monkeypatch):
    """A budget abort must leave the conversation partial with its cursor intact.

    The floor is the messages table, so nothing else needs recording: the next
    slice reads a lower floor and continues. What must NOT happen is the state
    flipping to ``full`` (coverage would drop it) or the cursor being written.
    """
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse(
        states={
            ("zrl", "T1", "conversation", "C_BIG"): {
                "cursor_ts": "1787832000.000200",
                "last_sync_type": "partial",
                "status": "ok",
            }
        }
    )
    warehouse.conversation_payloads = [{"id": "C_BIG", "name": "big", "is_channel": True, "is_member": True}]
    warehouse.message_low_water[("zrl", "T1", "C_BIG")] = "1787800000.000100"
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                {
                    "ok": True,
                    "messages": [{"ts": "1787700000.000100", "user": "U2", "text": "one page down"}],
                    "response_metadata": {"next_cursor": "page2"},
                },
                SlackRateLimitedError(retry_after=30),
            ],
        }
    )

    summary = _coverage_runner(settings, warehouse, client, max_rate_limit_sleep_seconds=1).sync_all()[0]

    assert summary.sync_type == "backfill"
    assert [row["message_ts"] for row in warehouse.messages] == ["1787700000.000100"], "the page before the abort is kept"
    state_writes = [u for u in warehouse.state_updates if u["object_type"] == "conversation"]
    assert state_writes, "progress below the floor is recorded as partial"
    assert all(u["last_sync_type"] == "partial" for u in state_writes)
    assert all(u["cursor_ts"] == "" for u in state_writes)


def test_coverage_still_streams_a_conversation_with_no_cursor_from_the_top(monkeypatch):
    """No state at all means no history at all: the existing full stream is right.

    The floor lookup is only for conversations that hold *some* history, so a
    never-synced channel must not be asked for a floor it cannot have.
    """
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.conversation_payloads = [{"id": "C_FRESH", "name": "fresh", "is_channel": True}]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                {"ok": True, "messages": [{"ts": "1787832000.000200", "user": "U2", "text": "hi"}], "response_metadata": {}}
            ],
        }
    )

    _coverage_runner(settings, warehouse, client).sync_all()

    assert warehouse.low_water_calls == []
    call = _history_calls(client)[0]
    assert "latest" not in call and "oldest" not in call
    assert [u for u in warehouse.state_updates if u["object_type"] == "conversation"][-1]["last_sync_type"] == "full"


def test_runner_freshness_priority_discovers_a_conversation_the_change_feed_names_but_never_cached(monkeypatch):
    # Regression (new-DM landing latency): the change feed names a conversation id, but
    # the freshness pass loads its candidates from the CACHED base_slack.conversations
    # rows, so a DM or group DM created since the last conversations.list walk was named
    # and then dropped. Discovery is paged and rotates types, so the wait was ~14 hours
    # in production: measured 2026-08-28, a group DM created 16:02 first reached the
    # timeline at 05:36 the next day (13.6h) and a DM created 19:20 landed at 23:30
    # (3.9h) -- the exact minute the discovery walk finally cached it. A conversation the
    # feed names and we have never seen must be fetched with conversations.info there and
    # then, which costs one call for something that happens a handful of times a day.
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    # Nothing cached: this conversation was created after the last discovery walk.
    warehouse.conversation_payloads = []
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.info": [
                {"ok": True, "channel": {"id": "D_BRAND_NEW", "user": "U1", "is_im": True}},
            ],
            "conversations.history": [
                {"ok": True, "messages": [{"ts": "1999.000000", "user": "U1", "text": "hi"}], "response_metadata": {}},
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
        conversation_types=("im",),
        conversation_ids=("D_BRAND_NEW",),
        sync_thread_replies=False,
        sleep=lambda seconds: None,
    ).sync_all()

    assert [params["channel"] for method, params in client.calls if method == "conversations.info"] == ["D_BRAND_NEW"]
    # It is written to base_slack.conversations, so every later stage can see it too.
    assert [row["conversation_id"] for row in warehouse.conversations] == ["D_BRAND_NEW"]
    assert [params["channel"] for method, params in client.calls if method == "conversations.history"] == ["D_BRAND_NEW"]


def test_runner_freshness_priority_streams_a_brand_new_conversation_in_full(monkeypatch):
    # The freshness window is four hours. A conversation we have only just learned about
    # holds nothing at all, so fetching only its last four hours truncates it: production
    # DM was created 19:20 and cached at 23:30, and its 19:22 message fell
    # eight minutes outside the window -- it did not land until the coverage floor walk
    # reached it eight hours later. A brand-new conversation streams in full instead,
    # which is cheap precisely because it is brand new.
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.conversation_payloads = []
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.info": [
                # `latest.ts` predates the freshness window (oldest_ts = 1400), which the
                # activity gate would otherwise read as "nothing happened here".
                {
                    "ok": True,
                    "channel": {"id": "C_NEW_MPIM", "is_mpim": True, "latest": {"ts": "1100.000000"}},
                },
            ],
            "conversations.history": [
                {"ok": True, "messages": [{"ts": "1100.000000", "user": "U1", "text": "first"}], "response_metadata": {}},
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
        conversation_types=("mpim",),
        conversation_ids=("C_NEW_MPIM",),
        sync_thread_replies=False,
        sleep=lambda seconds: None,
    ).sync_all()

    history_calls = [params for method, params in client.calls if method == "conversations.history"]
    assert [params["channel"] for params in history_calls] == ["C_NEW_MPIM"]
    # Full stream: no `oldest` bound, so the conversation's whole (short) history lands.
    assert "oldest" not in history_calls[0]
    assert [row["message_ts"] for row in warehouse.messages] == ["1100.000000"]


def test_runner_freshness_priority_does_not_refetch_conversations_it_already_has(monkeypatch):
    # The on-demand lookup must be scoped to ids we genuinely do not hold. Asking
    # conversations.info for every changed conversation would add ~50 calls per
    # five-minute pass against a measured ~39/min ceiling shared with every other stage.
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.conversation_payloads = [
        {"id": "D_KNOWN", "user": "U1", "is_im": True, "latest": {"ts": "1999.000000"}},
    ]
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.history": [
                {"ok": True, "messages": [{"ts": "1999.000000", "user": "U1", "text": "hi"}], "response_metadata": {}},
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
        conversation_types=("im",),
        conversation_ids=("D_KNOWN",),
        sync_thread_replies=False,
        sleep=lambda seconds: None,
    ).sync_all()

    assert not any(method == "conversations.info" for method, _params in client.calls)


def test_runner_freshness_priority_bounds_how_many_new_conversations_one_pass_discovers(monkeypatch):
    # The lookup is one API call per unknown id, out of the shared rate budget. A feed
    # that suddenly names hundreds of unseen conversations (a fresh workspace, a restored
    # session, a lost conversations table) must not spend the whole pass on metadata:
    # the rest are picked up by the next pass and by the discovery walk.
    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    settings = load_settings(require_postgres=False, require_gmail=False, require_slack=True)
    warehouse = FakeWarehouse()
    warehouse.conversation_payloads = []
    client = FakeSlackClient(
        {
            "auth.test": [{"ok": True, "team_id": "T1", "team": "Hack Club"}],
            "team.info": [{"ok": True, "team": {"id": "T1", "name": "Hack Club"}}],
            "conversations.info": [
                {"ok": True, "channel": {"id": "D_1", "user": "U1", "is_im": True}},
                {"ok": True, "channel": {"id": "D_2", "user": "U2", "is_im": True}},
            ],
            "conversations.history": [
                {"ok": True, "messages": [], "response_metadata": {}},
                {"ok": True, "messages": [], "response_metadata": {}},
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
        conversation_types=("im",),
        conversation_ids=("D_1", "D_2", "D_3", "D_4"),
        new_conversation_limit=2,
        sync_thread_replies=False,
        sleep=lambda seconds: None,
    ).sync_all()

    assert [params["channel"] for method, params in client.calls if method == "conversations.info"] == ["D_1", "D_2"]
