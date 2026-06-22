from __future__ import annotations

from datetime import UTC, datetime
import gzip
import json

from personal_data_warehouse_claude_desktop.api import ClaudeAiRateLimitError
from personal_data_warehouse_claude_desktop.state import ClaudeDesktopSyncState
from personal_data_warehouse_claude_desktop.sync import (
    ClaudeDesktopUploadRunner,
    conversation_lines,
)

NOW = datetime(2026, 6, 22, 18, tzinfo=UTC)


class FakeLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


class FakeBatchUploader:
    def __init__(self) -> None:
        self.batches: list[list[dict]] = []

    def __call__(self, encoded: bytes, exported_at) -> dict:
        records = [json.loads(line) for line in gzip.decompress(encoded).decode("utf-8").splitlines() if line.strip()]
        self.batches.append(records)
        return {
            "storage_backend": "google_drive",
            "storage_key": f"agent-sessions/inbox/batches/batch-{len(self.batches)}.jsonl.gz",
            "storage_file_id": f"file-{len(self.batches)}",
            "storage_url": "https://drive/batch",
        }


class FakeClient:
    """Stands in for ClaudeAiClient: serves a fixed list + per-uuid trees."""

    def __init__(self, summaries: list[dict], trees: dict[str, dict]) -> None:
        self._summaries = summaries
        self._trees = trees
        self.fetched: list[str] = []

    def iter_conversations(self, *, page_size: int = 50):
        yield from self._summaries

    def get_conversation(self, conversation_id: str) -> dict:
        self.fetched.append(conversation_id)
        return self._trees[conversation_id]


def _tree(uuid: str, updated_at: str, messages: list[dict], *, name="Conv", model="claude-sonnet-4-6") -> dict:
    return {
        "uuid": uuid,
        "name": name,
        "model": model,
        "created_at": "2026-06-01T00:00:00Z",
        "updated_at": updated_at,
        "chat_messages": messages,
    }


def _msg(uuid: str, sender: str, text: str) -> dict:
    return {
        "uuid": uuid,
        "sender": sender,
        "created_at": "2026-06-01T00:00:01Z",
        "content": [{"type": "text", "text": text}],
    }


def _runner(client, store, state, **kwargs) -> ClaudeDesktopUploadRunner:
    return ClaudeDesktopUploadRunner(
        account="account@example.com",
        device="prod",
        client=client,
        batch_uploader=store,
        logger=FakeLogger(),
        upload_state=state,
        now=lambda: NOW,
        **kwargs,
    )


# --- conversation_lines -----------------------------------------------------


def test_conversation_lines_header_then_messages() -> None:
    tree = _tree("c1", "2026-06-02T00:00:00Z", [_msg("m1", "human", "hi"), _msg("m2", "assistant", "hello")])
    lines = conversation_lines(tree)
    assert lines[0]["type"] == "conversation"
    assert lines[0]["uuid"] == "c1"
    assert lines[0]["model"] == "claude-sonnet-4-6"
    assert [l["type"] for l in lines[1:]] == ["message", "message"]
    # model copied onto the assistant message, not the human one
    assistant = next(l for l in lines if l.get("sender") == "assistant")
    human = next(l for l in lines if l.get("sender") == "human")
    assert assistant["model"] == "claude-sonnet-4-6"
    assert "model" not in human


# --- runner -----------------------------------------------------------------


def test_runner_uploads_header_and_messages(tmp_path) -> None:
    client = FakeClient(
        [{"uuid": "c1", "updated_at": "2026-06-02T00:00:00Z"}],
        {"c1": _tree("c1", "2026-06-02T00:00:00Z", [_msg("m1", "human", "hi"), _msg("m2", "assistant", "hello")])},
    )
    store = FakeBatchUploader()
    state = ClaudeDesktopSyncState.open(tmp_path / "s.sqlite", account="account@example.com")

    summary = _runner(client, store, state).sync()

    assert summary.conversations_seen == 1
    assert summary.conversations_changed == 1
    assert summary.messages_uploaded == 2
    assert summary.batches_uploaded == 1
    records = store.batches[0]
    assert [r["record"]["seq"] for r in records] == [0, 1, 2]
    assert records[0]["record"]["tool"] == "claude_desktop"
    assert records[0]["record_type"] == "claude_desktop_event"
    assert records[0]["record"]["line"]["type"] == "conversation"
    assert records[0]["device"] == "prod"
    state.close()


def test_runner_skips_unchanged_then_repolls_when_updated(tmp_path) -> None:
    client = FakeClient(
        [{"uuid": "c1", "updated_at": "2026-06-02T00:00:00Z"}],
        {"c1": _tree("c1", "2026-06-02T00:00:00Z", [_msg("m1", "human", "hi")])},
    )
    store = FakeBatchUploader()
    state = ClaudeDesktopSyncState.open(tmp_path / "s.sqlite", account="account@example.com")

    first = _runner(client, store, state).sync()
    assert first.conversations_changed == 1
    assert client.fetched == ["c1"]

    # Second run: same updated_at -> skipped, not re-fetched.
    second = _runner(client, store, state).sync()
    assert second.conversations_changed == 0
    assert client.fetched == ["c1"]  # unchanged

    # A new turn advances updated_at -> re-fetched and re-shipped.
    client._summaries = [{"uuid": "c1", "updated_at": "2026-06-03T00:00:00Z"}]
    client._trees["c1"] = _tree(
        "c1", "2026-06-03T00:00:00Z", [_msg("m1", "human", "hi"), _msg("m2", "assistant", "yo")]
    )
    third = _runner(client, store, state).sync()
    assert third.conversations_changed == 1
    assert client.fetched == ["c1", "c1"]
    state.close()


def test_runner_full_mode_refetches_unchanged(tmp_path) -> None:
    client = FakeClient(
        [{"uuid": "c1", "updated_at": "2026-06-02T00:00:00Z"}],
        {"c1": _tree("c1", "2026-06-02T00:00:00Z", [_msg("m1", "human", "hi")])},
    )
    store = FakeBatchUploader()
    state = ClaudeDesktopSyncState.open(tmp_path / "s.sqlite", account="account@example.com")
    _runner(client, store, state).sync()
    summary = _runner(client, store, state, mode="full").sync()
    assert summary.conversations_changed == 1  # re-fetched despite unchanged cursor
    state.close()


def test_runner_limit_defers_remaining(tmp_path) -> None:
    summaries = [{"uuid": f"c{i}", "updated_at": "2026-06-02T00:00:00Z"} for i in range(3)]
    trees = {f"c{i}": _tree(f"c{i}", "2026-06-02T00:00:00Z", [_msg(f"m{i}", "human", "hi")]) for i in range(3)}
    client = FakeClient(summaries, trees)
    store = FakeBatchUploader()
    state = ClaudeDesktopSyncState.open(tmp_path / "s.sqlite", account="account@example.com")

    summary = _runner(client, store, state, limit=2).sync()
    assert summary.conversations_changed == 2
    assert summary.deferred_for_limit is True

    # Next run picks up the remaining one (first two are now cursor-skipped).
    summary2 = _runner(client, store, state, limit=2).sync()
    assert summary2.conversations_changed == 1
    state.close()


def test_runner_commits_cursor_only_after_upload(tmp_path) -> None:
    client = FakeClient(
        [{"uuid": "c1", "updated_at": "2026-06-02T00:00:00Z"}],
        {"c1": _tree("c1", "2026-06-02T00:00:00Z", [_msg("m1", "human", "hi")])},
    )

    class BoomUploader:
        def __call__(self, encoded, exported_at):
            raise RuntimeError("upload failed")

    state = ClaudeDesktopSyncState.open(tmp_path / "s.sqlite", account="account@example.com")
    try:
        _runner(client, BoomUploader(), state).sync()
    except RuntimeError:
        pass
    # Cursor not advanced because the upload failed.
    assert state.updated_at_for("c1") == ""
    state.close()


def test_runner_rate_limited_on_fetch_defers_gracefully(tmp_path) -> None:
    # c0 fetches fine; c1 is throttled (429). The runner should stop, ship the
    # already-buffered c0, commit only c0's cursor, and flag rate_limited - so
    # the next keepalive tick resumes at c1 rather than the run going red.
    summaries = [{"uuid": f"c{i}", "updated_at": "2026-06-02T00:00:00Z"} for i in range(3)]
    trees = {f"c{i}": _tree(f"c{i}", "2026-06-02T00:00:00Z", [_msg(f"m{i}", "human", "hi")]) for i in range(3)}

    class RateLimitedClient(FakeClient):
        def get_conversation(self, conversation_id: str) -> dict:
            if conversation_id == "c1":
                raise ClaudeAiRateLimitError("429", retry_after_seconds=7)
            return super().get_conversation(conversation_id)

    client = RateLimitedClient(summaries, trees)
    store = FakeBatchUploader()
    state = ClaudeDesktopSyncState.open(tmp_path / "s.sqlite", account="account@example.com")

    summary = _runner(client, store, state).sync()

    assert summary.rate_limited is True
    assert summary.conversations_changed == 1  # only c0 completed
    # c0 fetched fine; c1 raised (so it never reached super().fetched); c2 never attempted.
    assert client.fetched == ["c0"]
    assert state.updated_at_for("c0") == "2026-06-02T00:00:00Z"
    assert state.updated_at_for("c1") == ""  # not committed
    state.close()


def test_runner_rate_limited_on_listing_defers_gracefully(tmp_path) -> None:
    class ListingRateLimitedClient(FakeClient):
        def iter_conversations(self, *, page_size: int = 50):
            raise ClaudeAiRateLimitError("429")

    client = ListingRateLimitedClient([], {})
    store = FakeBatchUploader()
    state = ClaudeDesktopSyncState.open(tmp_path / "s.sqlite", account="account@example.com")

    summary = _runner(client, store, state).sync()

    assert summary.rate_limited is True
    assert summary.conversations_seen == 0
    assert summary.batches_uploaded == 0
    state.close()


def test_runner_batches_split_across_messages(tmp_path) -> None:
    # One conversation with 5 messages, batch_size 2 -> header+5 = 6 records -> 3 batches.
    msgs = [_msg(f"m{i}", "human" if i % 2 == 0 else "assistant", f"t{i}") for i in range(5)]
    client = FakeClient(
        [{"uuid": "c1", "updated_at": "2026-06-02T00:00:00Z"}],
        {"c1": _tree("c1", "2026-06-02T00:00:00Z", msgs)},
    )
    store = FakeBatchUploader()
    state = ClaudeDesktopSyncState.open(tmp_path / "s.sqlite", account="account@example.com")
    summary = _runner(client, store, state, batch_size=2).sync()
    assert summary.batches_uploaded == 3
    assert [len(b) for b in store.batches] == [2, 2, 2]
    # Cursor committed after the whole conversation uploaded.
    assert state.updated_at_for("c1") == "2026-06-02T00:00:00Z"
    state.close()
