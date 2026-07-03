from __future__ import annotations

from datetime import UTC, datetime

import pytest

from personal_data_warehouse.chatgpt_backend import (
    ChatGPTAuthError,
    ChatGPTRateLimitError,
    ConversationRef,
)
from personal_data_warehouse.chatgpt_backend_ingest import (
    ChatGPTBackendIngestRunner,
    ChatGPTBackendIngestSummary,
    chatgpt_poll_stall_reason,
    chatgpt_session_expiry_skip,
    chatgpt_session_is_marked_expired,
)

INGESTED_AT = datetime(2026, 6, 22, 18, tzinfo=UTC)


class FakeLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


class FakeWarehouse:
    def __init__(self, sync_map=None) -> None:
        self.ensure_called = False
        self.events: list[dict] = []
        self.sync_map = dict(sync_map or {})
        self.recorded: list[dict] = []

    def ensure_agent_sessions_tables(self) -> None:
        self.ensure_called = True

    def chatgpt_conversation_sync_map(self, *, account):
        return dict(self.sync_map)

    def insert_agent_session_events(self, rows) -> None:
        self.events.extend(rows)

    def record_chatgpt_conversation_synced(self, *, account, session_id, update_time, event_count, synced_at) -> None:
        self.recorded.append(
            {"session_id": session_id, "update_time": update_time, "event_count": event_count}
        )


def _convo(convo_id, *, title="t", user_text="hi"):
    return {
        "conversation_id": convo_id,
        "title": title,
        "mapping": {
            "root": {"id": "root", "parent": None, "children": ["u"], "message": None},
            "u": {
                "id": "u",
                "parent": "root",
                "children": [],
                "message": {
                    "id": f"{convo_id}-u",
                    "author": {"role": "user"},
                    "create_time": 100.0,
                    "content": {"content_type": "text", "parts": [user_text]},
                },
            },
        },
    }


class FakeClient:
    def __init__(self, refs, conversations, *, fail_on=None, rate_limit_on=None, rate_limit_listing=False):
        self._refs = refs
        self._conversations = conversations
        self._fail_on = fail_on
        self._rate_limit_on = rate_limit_on
        self._rate_limit_listing = rate_limit_listing
        self.fetched: list[str] = []

    def iter_conversation_refs(self, *, page_size=28):
        if self._rate_limit_listing:
            raise ChatGPTRateLimitError("list returned 429", retry_after_seconds=3)
        yield from self._refs

    def get_conversation(self, conversation_id):
        if self._fail_on == conversation_id:
            raise ChatGPTAuthError("session expired")
        if self._rate_limit_on == conversation_id:
            raise ChatGPTRateLimitError("detail returned 429", retry_after_seconds=7)
        self.fetched.append(conversation_id)
        return self._conversations[conversation_id]


def runner(warehouse, client, **kwargs):
    return ChatGPTBackendIngestRunner(
        warehouse=warehouse, client=client, account="user@example.com", logger=FakeLogger(),
        now=lambda: INGESTED_AT, **kwargs,
    )


def test_fetches_all_new_conversations_and_writes_events():
    refs = [
        ConversationRef("c1", "one", 1.0, 200.0),
        ConversationRef("c2", "two", 2.0, 150.0),
    ]
    convos = {"c1": _convo("c1"), "c2": _convo("c2")}
    wh = FakeWarehouse()
    client = FakeClient(refs, convos)
    summary = runner(wh, client).sync()

    assert wh.ensure_called
    assert client.fetched == ["c1", "c2"]
    assert summary.conversations_seen == 2
    assert summary.conversations_fetched == 2
    assert summary.events_written == 2  # one user row per convo
    assert {r["session_id"] for r in wh.recorded} == {"c1", "c2"}


def test_skips_conversations_not_updated_since_last_sync():
    # Listed newest-first by update_time: the freshly-updated c2 (999) sorts above
    # the unchanged c1 (200, == its last synced value).
    refs = [
        ConversationRef("c2", "two", 2.0, 999.0),  # updated since synced (150)
        ConversationRef("c1", "one", 1.0, 200.0),  # unchanged (synced at 200)
    ]
    convos = {"c1": _convo("c1"), "c2": _convo("c2")}
    wh = FakeWarehouse(sync_map={"c1": 200.0, "c2": 150.0})
    client = FakeClient(refs, convos)
    summary = runner(wh, client).sync()

    assert client.fetched == ["c2"]
    assert summary.conversations_fetched == 1


def test_stops_paging_once_caught_up_to_high_water():
    # Steady state: the whole list is already synced and unchanged. The walk must
    # stop at the first (newest) conversation instead of paging the full history,
    # which is what otherwise trips the backend rate limiter on every tick.
    refs = [
        ConversationRef("c3", "three", 3.0, 300.0),
        ConversationRef("c2", "two", 2.0, 200.0),
        ConversationRef("c1", "one", 1.0, 100.0),
    ]
    convos = {cid: _convo(cid) for cid in ("c1", "c2", "c3")}
    wh = FakeWarehouse(sync_map={"c1": 100.0, "c2": 200.0, "c3": 300.0})
    client = FakeClient(refs, convos)
    summary = runner(wh, client).sync()

    assert client.fetched == []
    assert summary.conversations_seen == 1  # stopped at the newest, no deep paging
    assert summary.conversations_fetched == 0
    assert summary.events_written == 0
    assert summary.rate_limited is False
    assert summary.stopped_at_high_water is True
    # Caught up and idle is healthy, not a stall.
    assert chatgpt_poll_stall_reason(summary, max_conversations_per_run=0) is None


def test_fetches_new_top_conversations_then_stops_at_high_water():
    # A new conversation (c4, above the high-water mark) and an edited one (c3,
    # bumped above the mark) sort to the top; everything at/below the previous
    # high-water mark (c2) is left untouched.
    refs = [
        ConversationRef("c4", "four", 4.0, 400.0),   # brand new
        ConversationRef("c3", "three", 3.0, 350.0),  # edited (synced at 300)
        ConversationRef("c2", "two", 2.0, 200.0),    # unchanged at high-water
        ConversationRef("c1", "one", 1.0, 100.0),
    ]
    convos = {cid: _convo(cid) for cid in ("c1", "c2", "c3", "c4")}
    wh = FakeWarehouse(sync_map={"c1": 100.0, "c2": 200.0, "c3": 300.0})
    client = FakeClient(refs, convos)
    summary = runner(wh, client).sync()

    assert client.fetched == ["c4", "c3"]
    assert summary.conversations_fetched == 2


def test_backfill_mode_disables_high_water_stop():
    # With max_conversations_per_run set (explicit backfill), the early stop is
    # disabled so we can page *down* past already-synced conversations to reach
    # older, not-yet-synced ones below the high-water mark.
    refs = [
        ConversationRef("c3", "three", 3.0, 300.0),  # synced
        ConversationRef("c2", "two", 2.0, 200.0),    # synced
        ConversationRef("c1", "one", 1.0, 100.0),    # NOT synced (older backlog)
    ]
    convos = {cid: _convo(cid) for cid in ("c1", "c2", "c3")}
    wh = FakeWarehouse(sync_map={"c2": 200.0, "c3": 300.0})
    client = FakeClient(refs, convos)
    summary = runner(wh, client, max_conversations_per_run=5).sync()

    assert client.fetched == ["c1"]
    assert summary.conversations_fetched == 1


def test_max_conversations_per_run_bounds_fetches():
    refs = [ConversationRef(f"c{i}", "t", 1.0, 100.0 + i) for i in range(5)]
    convos = {f"c{i}": _convo(f"c{i}") for i in range(5)}
    wh = FakeWarehouse()
    client = FakeClient(refs, convos)
    summary = runner(wh, client, max_conversations_per_run=2).sync()

    assert summary.conversations_fetched == 2
    assert summary.reached_run_limit is True
    assert len(client.fetched) == 2


def test_rate_limit_while_fetching_detail_stops_gracefully_after_recording_progress():
    refs = [
        ConversationRef("c1", "one", 1.0, 200.0),
        ConversationRef("c2", "two", 2.0, 150.0),
        ConversationRef("c3", "three", 3.0, 125.0),
    ]
    convos = {"c1": _convo("c1"), "c2": _convo("c2"), "c3": _convo("c3")}
    wh = FakeWarehouse()
    client = FakeClient(refs, convos, rate_limit_on="c2")
    summary = runner(wh, client).sync()

    assert client.fetched == ["c1"]
    assert [r["session_id"] for r in wh.recorded] == ["c1"]
    assert summary.conversations_seen == 2
    assert summary.conversations_fetched == 1
    assert summary.events_written == 1
    assert summary.rate_limited is True
    assert summary.reached_run_limit is False


def test_rate_limit_while_listing_stops_gracefully_without_writes():
    wh = FakeWarehouse()
    client = FakeClient([], {}, rate_limit_listing=True)
    summary = runner(wh, client).sync()

    assert client.fetched == []
    assert wh.recorded == []
    assert summary.conversations_seen == 0
    assert summary.conversations_fetched == 0
    assert summary.events_written == 0
    assert summary.rate_limited is True


def test_auth_error_propagates_for_loud_failure():
    refs = [ConversationRef("c1", "one", 1.0, 200.0)]
    wh = FakeWarehouse()
    client = FakeClient(refs, {"c1": _convo("c1")}, fail_on="c1")
    with pytest.raises(ChatGPTAuthError):
        runner(wh, client).sync()


def test_rate_limited_listing_with_no_progress_is_a_stall():
    # Throttled before reaching the high-water mark and wrote nothing: this is the
    # silent-freeze shape, so the run must surface it loudly rather than report success.
    wh = FakeWarehouse(sync_map={"c1": 100.0})
    client = FakeClient([], {}, rate_limit_listing=True)
    summary = runner(wh, client).sync()

    assert summary.rate_limited is True
    assert summary.conversations_fetched == 0
    assert summary.stopped_at_high_water is False
    reason = chatgpt_poll_stall_reason(summary, max_conversations_per_run=0)
    assert reason is not None
    assert "rate limited" in reason
    assert "publish-session" in reason


def _summary(**kwargs) -> ChatGPTBackendIngestSummary:
    base = dict(
        conversations_seen=0,
        conversations_fetched=0,
        events_written=0,
        reached_run_limit=False,
        rate_limited=False,
        stopped_at_high_water=False,
    )
    base.update(kwargs)
    return ChatGPTBackendIngestSummary(**base)


def test_stall_reason_silent_when_progress_made():
    # Fetched at least one conversation before being throttled: self-heals next tick.
    summary = _summary(conversations_fetched=1, rate_limited=True)
    assert chatgpt_poll_stall_reason(summary, max_conversations_per_run=0) is None


def test_stall_reason_silent_when_not_rate_limited():
    summary = _summary(conversations_seen=3, rate_limited=False)
    assert chatgpt_poll_stall_reason(summary, max_conversations_per_run=0) is None


def test_stall_reason_silent_during_backfill():
    # A historical backfill deliberately pages past synced conversations and is
    # expected to hit the rate limiter; never treat that as a stall.
    summary = _summary(conversations_seen=140, rate_limited=True)
    assert chatgpt_poll_stall_reason(summary, max_conversations_per_run=50) is None


# --- expired-session skip / re-probe ---------------------------------------

_HINT = "Run `pdw chatgpt publish-session` to refresh it."


def _session_row(*, token_sha, expired_sha, expired_at=None):
    return {
        "session_token": "cookie",
        "token_sha256": token_sha,
        "expired_token_sha256": expired_sha,
        "expired_at": expired_at,
    }


def test_is_marked_expired_only_when_current_token_matches():
    # No mark at all.
    assert chatgpt_session_is_marked_expired(_session_row(token_sha="abc", expired_sha="")) is False
    # The stored token is the one that was rejected.
    assert chatgpt_session_is_marked_expired(_session_row(token_sha="abc", expired_sha="abc")) is True
    # Re-published: the stored token rotated, so the stale mark no longer applies.
    assert chatgpt_session_is_marked_expired(_session_row(token_sha="new", expired_sha="abc")) is False
    # Missing row / empty token are never expired.
    assert chatgpt_session_is_marked_expired(None) is False
    assert chatgpt_session_is_marked_expired(_session_row(token_sha="", expired_sha="")) is False


def test_expiry_skip_returns_reason_within_reprobe_window():
    now = 10_000.0
    row = _session_row(token_sha="abc", expired_sha="abc", expired_at=now - 100)  # 100s ago
    reason = chatgpt_session_expiry_skip(
        row, now=now, reprobe_after_seconds=3600, republish_hint=_HINT
    )
    assert reason is not None
    assert "expired" in reason
    assert "publish-session" in reason
    assert "Re-probing in ~3500s" in reason


def test_expiry_skip_allows_reprobe_after_window_elapses():
    now = 10_000.0
    row = _session_row(token_sha="abc", expired_sha="abc", expired_at=now - 4000)  # > 3600s ago
    assert (
        chatgpt_session_expiry_skip(row, now=now, reprobe_after_seconds=3600, republish_hint=_HINT)
        is None
    )


def test_expiry_skip_none_when_not_marked_or_republished():
    now = 10_000.0
    # Never marked.
    assert (
        chatgpt_session_expiry_skip(
            _session_row(token_sha="abc", expired_sha=""), now=now,
            reprobe_after_seconds=3600, republish_hint=_HINT,
        )
        is None
    )
    # Re-published (token rotated) clears it immediately regardless of the timer.
    assert (
        chatgpt_session_expiry_skip(
            _session_row(token_sha="new", expired_sha="abc", expired_at=now - 10), now=now,
            reprobe_after_seconds=3600, republish_hint=_HINT,
        )
        is None
    )


def test_expiry_skip_none_when_mark_has_no_timestamp():
    # A malformed mark (matching sha but no expired_at) must not skip forever; let a
    # run re-establish a proper mark instead.
    now = 10_000.0
    row = _session_row(token_sha="abc", expired_sha="abc", expired_at=None)
    assert (
        chatgpt_session_expiry_skip(row, now=now, reprobe_after_seconds=3600, republish_hint=_HINT)
        is None
    )


def test_expiry_skip_accepts_datetime_expired_at():
    from datetime import UTC, datetime, timedelta

    recent = datetime.now(tz=UTC) - timedelta(seconds=60)
    row = _session_row(token_sha="abc", expired_sha="abc", expired_at=recent)
    reason = chatgpt_session_expiry_skip(
        row, now=datetime.now(tz=UTC).timestamp(), reprobe_after_seconds=3600, republish_hint=_HINT
    )
    assert reason is not None and "publish-session" in reason
