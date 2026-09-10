"""The keepalive sensor sits out a key claude.ai has already rejected."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from personal_data_warehouse_claude_desktop.state import (
    CLAUDE_DESKTOP_REJECTED_REPROBE,
    claude_desktop_credential_skip,
    session_key_sha256,
)

NOW = datetime(2026, 9, 10, 2, tzinfo=UTC)
HINT = "sign in again"


def _rejected(key: str = "sk-dead", *, rejected_key: str | None = None, ago: timedelta = timedelta(minutes=5)):
    return {
        "session_key": key,
        "status": "action_required",
        "error": "claude.ai returned 403 for /x",
        "rejected_session_sha256": session_key_sha256(rejected_key or key),
        "rejected_at": NOW - ago,
    }


def test_a_freshly_rejected_key_is_skipped_with_the_repair_named() -> None:
    message = claude_desktop_credential_skip(_rejected(), now=NOW, republish_hint=HINT)
    assert message is not None
    assert "403" in message and HINT in message


def test_a_new_key_resumes_at_once_even_while_the_row_reads_action_required() -> None:
    assert claude_desktop_credential_skip(_rejected("sk-new", rejected_key="sk-dead"), now=NOW, republish_hint=HINT) is None


def test_the_same_dead_key_is_re_probed_after_the_reprobe_interval() -> None:
    assert claude_desktop_credential_skip(
        _rejected(ago=CLAUDE_DESKTOP_REJECTED_REPROBE), now=NOW, republish_hint=HINT
    ) is None


def test_a_healthy_or_missing_row_never_skips() -> None:
    assert claude_desktop_credential_skip(None, now=NOW, republish_hint=HINT) is None
    healthy = {**_rejected(), "status": "ok"}
    assert claude_desktop_credential_skip(healthy, now=NOW, republish_hint=HINT) is None
