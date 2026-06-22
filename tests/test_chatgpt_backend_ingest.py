from __future__ import annotations

from datetime import UTC, datetime

import pytest

from personal_data_warehouse.chatgpt_backend import ChatGPTAuthError, ConversationRef
from personal_data_warehouse.chatgpt_backend_ingest import ChatGPTBackendIngestRunner

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
    def __init__(self, refs, conversations, *, fail_on=None):
        self._refs = refs
        self._conversations = conversations
        self._fail_on = fail_on
        self.fetched: list[str] = []

    def iter_conversation_refs(self, *, page_size=28):
        yield from self._refs

    def get_conversation(self, conversation_id):
        if self._fail_on == conversation_id:
            raise ChatGPTAuthError("session expired")
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
    refs = [
        ConversationRef("c1", "one", 1.0, 200.0),  # unchanged (synced at 200)
        ConversationRef("c2", "two", 2.0, 999.0),  # updated since synced (150)
    ]
    convos = {"c1": _convo("c1"), "c2": _convo("c2")}
    wh = FakeWarehouse(sync_map={"c1": 200.0, "c2": 150.0})
    client = FakeClient(refs, convos)
    summary = runner(wh, client).sync()

    assert client.fetched == ["c2"]
    assert summary.conversations_seen == 2
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


def test_auth_error_propagates_for_loud_failure():
    refs = [ConversationRef("c1", "one", 1.0, 200.0)]
    wh = FakeWarehouse()
    client = FakeClient(refs, {"c1": _convo("c1")}, fail_on="c1")
    with pytest.raises(ChatGPTAuthError):
        runner(wh, client).sync()
