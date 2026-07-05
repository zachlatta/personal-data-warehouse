"""Real-Postgres integration test for the ChatGPT server-side ingest path.

Skips unless POSTGRES_DATABASE_URL is set. Exercises the actual schema:
agent_session_events + the clean_agent_sessions view, chatgpt_conversation_sync
(incremental watermark), and the chatgpt_sessions credential store, proving the
runner, normalizer, DDL, and view agree end-to-end and that re-syncs are
idempotent.
"""

from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import os

import pytest
from dotenv import load_dotenv

from tests.conftest import make_test_schema

from personal_data_warehouse.chatgpt_backend import ConversationRef
from personal_data_warehouse.chatgpt_backend_ingest import ChatGPTBackendIngestRunner
from personal_data_warehouse.postgres import PostgresWarehouse

INGESTED_AT = datetime(2026, 6, 22, 18, tzinfo=UTC)
ACCOUNT = "user@example.com"


def _postgres_url() -> str:
    load_dotenv()
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if not url:
        pytest.skip("POSTGRES_DATABASE_URL is not set")
    return url


@pytest.fixture()
def warehouse():
    schema = make_test_schema()
    wh = PostgresWarehouse(_postgres_url(), schema=schema)
    try:
        yield wh
    finally:
        wh._command(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        wh.close()


class FakeLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


def _msg(mid, role, *, parts=None, create_time, content=None, recipient="all", model="", name=None):
    if content is None:
        content = {"content_type": "text", "parts": parts if parts is not None else [""]}
    return {
        "id": mid,
        "author": {"role": role, "name": name},
        "create_time": create_time,
        "content": content,
        "metadata": {"model_slug": model} if model else {},
        "recipient": recipient,
    }


def _node(nid, parent, children, message):
    return {"id": nid, "parent": parent, "children": list(children), "message": message}


def _conversation(cid, title, base_t):
    return {
        "conversation_id": cid,
        "title": title,
        "update_time": base_t + 20,
        "mapping": {
            "root": _node("root", None, ["u1"], None),
            "u1": _node("u1", "root", ["a1"], _msg(f"{cid}-u1", "user", parts=["What is SuperGrok?"], create_time=base_t)),
            "a1": _node("a1", "u1", ["a2"], _msg(f"{cid}-a1", "assistant", parts=["Let me check."], create_time=base_t + 5, model="gpt-4o")),
            "a2": _node("a2", "a1", ["t1"], _msg(f"{cid}-a2", "assistant", content={"content_type": "code", "text": "search('supergrok')"}, create_time=base_t + 8, recipient="browser", model="gpt-4o")),
            "t1": _node("t1", "a2", ["a3"], _msg(f"{cid}-t1", "tool", name="browser", content={"content_type": "tether_browsing_display", "result": "SuperGrok is a premium tier."}, create_time=base_t + 12)),
            "a3": _node("a3", "t1", [], _msg(f"{cid}-a3", "assistant", parts=["SuperGrok is the premium tier."], create_time=base_t + 18, model="gpt-4o")),
        },
    }


class FakeClient:
    def __init__(self, conversations):
        self._c = conversations
        self.fetched: list[str] = []

    def iter_conversation_refs(self, *, page_size=28):
        # The real backend lists conversations newest-first (order=updated), which the
        # runner's high-water early stop relies on; mirror that contract here.
        for cid, convo in sorted(
            self._c.items(), key=lambda kv: kv[1]["update_time"], reverse=True
        ):
            yield ConversationRef(cid, convo["title"], convo.get("create_time", 0.0), convo["update_time"])

    def get_conversation(self, cid):
        self.fetched.append(cid)
        return self._c[cid]


def _runner(warehouse, client):
    return ChatGPTBackendIngestRunner(
        warehouse=warehouse, client=client, account=ACCOUNT, logger=FakeLogger(), now=lambda: INGESTED_AT
    )


def test_full_ingest_view_and_idempotency(warehouse):
    convos = {
        "conv-aaa": _conversation("conv-aaa", "SuperGrok rundown", 1_750_000_000.0),
        "conv-bbb": _conversation("conv-bbb", "Pricing question", 1_750_100_000.0),
    }

    summary = _runner(warehouse, FakeClient(convos)).sync()
    assert summary.conversations_fetched == 2
    assert summary.events_written == 10  # 5 message nodes * 2 conversations

    # clean_agent_sessions roll-up over the real view.
    sessions = warehouse._query_dicts(
        "SELECT session_id, title, model, first_prompt, event_count, user_event_count, "
        "assistant_event_count, started_at, ended_at FROM clean_agent_sessions "
        "WHERE source='chatgpt' ORDER BY session_id"
    )
    assert [s["session_id"] for s in sessions] == ["conv-aaa", "conv-bbb"]
    aaa = sessions[0]
    assert aaa["title"] == "SuperGrok rundown"
    assert aaa["first_prompt"] == "What is SuperGrok?"
    assert aaa["model"] == "gpt-4o"
    assert aaa["event_count"] == 5
    assert aaa["user_event_count"] == 1
    assert aaa["assistant_event_count"] == 3
    assert aaa["started_at"] is not None and aaa["ended_at"] is not None

    # Transcript order + role/subtype mapping.
    events = warehouse._query_dicts(
        "SELECT seq, role, subtype, tool_name FROM agent_session_events "
        "WHERE source='chatgpt' AND session_id='conv-aaa' ORDER BY seq"
    )
    assert [(e["role"], e["subtype"]) for e in events] == [
        ("user", "message"),
        ("assistant", "message"),
        ("assistant", "tool_use"),
        ("tool", "tool_result"),
        ("assistant", "message"),
    ]
    assert events[2]["tool_name"] == "browser"

    # Re-sync is a no-op: nothing fetched, no duplicate rows.
    client2 = FakeClient(convos)
    summary2 = _runner(warehouse, client2).sync()
    assert summary2.conversations_fetched == 0
    assert client2.fetched == []
    assert _chatgpt_row_count(warehouse) == 10

    # An updated conversation is re-fetched; only its new turns net new rows. Editing
    # it floats it to the top of the newest-first list (above conv-bbb's high-water
    # mark), exactly as the backend's order=updated listing would.
    convos["conv-aaa"]["update_time"] = 1_750_100_000.0 + 1000
    convos["conv-aaa"]["mapping"]["a3"]["children"] = ["u2"]
    convos["conv-aaa"]["mapping"]["u2"] = _node(
        "u2", "a3", ["a4"], _msg("conv-aaa-u2", "user", parts=["thanks"], create_time=1_750_000_090.0)
    )
    convos["conv-aaa"]["mapping"]["a4"] = _node(
        "a4", "u2", [], _msg("conv-aaa-a4", "assistant", parts=["welcome"], create_time=1_750_000_095.0, model="gpt-4o")
    )
    client3 = FakeClient(convos)
    summary3 = _runner(warehouse, client3).sync()
    assert summary3.conversations_fetched == 1
    assert client3.fetched == ["conv-aaa"]
    assert _chatgpt_row_count(warehouse) == 12  # 10 + 2 new turns


def test_credential_store_roundtrip(warehouse):
    warehouse.upsert_chatgpt_session(
        account=ACCOUNT, session_key="default", session_token="cookie-header", source_browser="Google Chrome"
    )
    got = warehouse.get_chatgpt_session(account=ACCOUNT, session_key="default")
    assert got["session_token"] == "cookie-header"
    assert got["source_browser"] == "Google Chrome"
    assert got["token_sha256"] == hashlib.sha256(b"cookie-header").hexdigest()

    # Upsert replaces in place (no duplicate row).
    warehouse.upsert_chatgpt_session(
        account=ACCOUNT, session_key="default", session_token="rotated", source_browser="Brave"
    )
    rows = warehouse._query_dicts("SELECT count(*) c FROM chatgpt_sessions WHERE account=%s", (ACCOUNT,))
    assert rows[0]["c"] == 1
    refreshed = warehouse.get_chatgpt_session(account=ACCOUNT, session_key="default")
    assert refreshed["session_token"] == "rotated"
    assert refreshed["source_browser"] == "Brave"


def test_expiry_mark_roundtrip_and_clears_on_republish(warehouse):
    from personal_data_warehouse.chatgpt_backend_ingest import chatgpt_session_is_marked_expired

    warehouse.upsert_chatgpt_session(
        account=ACCOUNT, session_key="default", session_token="cookie-header", source_browser="Chrome"
    )
    row = warehouse.get_chatgpt_session(account=ACCOUNT, session_key="default")
    assert row["expired_at"] is None
    assert row["expired_token_sha256"] == ""
    assert chatgpt_session_is_marked_expired(row) is False

    # A poll rejected this token: mark it expired (keyed to its sha).
    warehouse.mark_chatgpt_session_expired(
        account=ACCOUNT, session_key="default", token_sha256=row["token_sha256"]
    )
    marked = warehouse.get_chatgpt_session(account=ACCOUNT, session_key="default")
    assert marked["expired_at"] is not None
    assert marked["expired_token_sha256"] == row["token_sha256"]
    assert chatgpt_session_is_marked_expired(marked) is True

    # A stale mark against a *different* token is a no-op (guards a concurrent publish).
    warehouse.mark_chatgpt_session_expired(
        account=ACCOUNT, session_key="default", token_sha256="some-other-sha"
    )
    still = warehouse.get_chatgpt_session(account=ACCOUNT, session_key="default")
    assert still["expired_token_sha256"] == row["token_sha256"]

    # Re-publishing a fresh cookie rotates the sha, so the row reads healthy again
    # without any explicit clear.
    warehouse.upsert_chatgpt_session(
        account=ACCOUNT, session_key="default", session_token="rotated-cookie", source_browser="Chrome"
    )
    republished = warehouse.get_chatgpt_session(account=ACCOUNT, session_key="default")
    assert chatgpt_session_is_marked_expired(republished) is False

    # Explicit clear (e.g. a transient 401 that recovered on the re-probe) also resets.
    warehouse.mark_chatgpt_session_expired(
        account=ACCOUNT, session_key="default", token_sha256=republished["token_sha256"]
    )
    assert chatgpt_session_is_marked_expired(
        warehouse.get_chatgpt_session(account=ACCOUNT, session_key="default")
    ) is True
    warehouse.clear_chatgpt_session_expired(account=ACCOUNT, session_key="default")
    cleared = warehouse.get_chatgpt_session(account=ACCOUNT, session_key="default")
    assert cleared["expired_at"] is None
    assert cleared["expired_token_sha256"] == ""
    assert chatgpt_session_is_marked_expired(cleared) is False


def _chatgpt_row_count(warehouse) -> int:
    return warehouse._query_dicts("SELECT count(*) c FROM agent_session_events WHERE source='chatgpt'")[0]["c"]
