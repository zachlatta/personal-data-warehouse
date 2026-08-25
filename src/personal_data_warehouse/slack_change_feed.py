"""Turn one ``client.counts`` response into "which conversations moved".

Slack's public API has no bulk change feed. ``conversations.list`` returns no
last-message marker at all -- only ``updated``, which tracks topic and member
edits -- so with an app token the only way to find a new message is to call
``conversations.history`` on every conversation. Measured on this workspace that
is ~950 calls per five-minute cycle against a ceiling of ~39 calls a minute,
which is why the freshness pass spends its time asleep on 429s holding the
shared Slack lock while every backfill stage starves.

``client.counts`` answers the same question in ONE request, but only for a real
signed-in session (see ``slack_session``). This module is the translation layer:
counts in, conversation ids out.

Coverage is deliberately reported rather than assumed. Measured 2026-08-24, the
feed carries 316 channels (exactly the 317 the account belongs to), 237 open DMs
and 137 open group DMs -- so it is complete for everything Zach participates in
and silent about the ~13k public channels he is not a member of. Those keep the
slow sweep.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

# The buckets client.counts returns. Threads are excluded on purpose: a threaded
# reply moves its parent conversation's marker too, and the thread stages already
# walk replies.
COUNT_BUCKETS = ("channels", "ims", "mpims")


@dataclass(frozen=True)
class SlackChangeFeed:
    """One snapshot of what Slack says the account's conversations look like."""

    latest_by_conversation: dict[str, float] = field(default_factory=dict)
    coverage: dict[str, int] = field(default_factory=dict)

    class Error(RuntimeError):
        """client.counts did not answer."""

    @property
    def covered_conversation_ids(self) -> set[str]:
        return set(self.latest_by_conversation)

    @classmethod
    def from_counts(cls, payload: Mapping[str, Any]) -> SlackChangeFeed:
        if not payload.get("ok"):
            # A broken answer must never be mistaken for "nothing changed":
            # that would stop ingestion silently, which is the exact failure
            # this whole path exists to remove.
            raise cls.Error(f"client.counts failed: {payload.get('error') or 'unknown_error'}")
        latest: dict[str, float] = {}
        coverage: dict[str, int] = {}
        for bucket in COUNT_BUCKETS:
            entries = payload.get(bucket) or []
            coverage[bucket] = len(entries)
            for entry in entries:
                if not isinstance(entry, Mapping):
                    continue
                conversation_id = str(entry.get("id") or "")
                raw_latest = str(entry.get("latest") or "")
                if not conversation_id or not raw_latest:
                    # No marker means no information. Treating that as "changed"
                    # would quietly restore the full-poll behaviour.
                    continue
                try:
                    latest[conversation_id] = float(raw_latest)
                except ValueError:
                    continue
        return cls(latest_by_conversation=latest, coverage=coverage)

    def changed_since(self, cursors: Mapping[str, float]) -> list[str]:
        """Conversation ids whose newest message is past our stored cursor."""
        moved = [
            (ts, conversation_id)
            for conversation_id, ts in self.latest_by_conversation.items()
            if ts > float(cursors.get(conversation_id) or 0.0)
        ]
        moved.sort(reverse=True)
        return [conversation_id for _ts, conversation_id in moved]


def changed_conversations(payload: Mapping[str, Any], cursors: Mapping[str, float]) -> list[str]:
    """Convenience wrapper: counts payload + cursors -> ids to fetch, newest first."""
    return SlackChangeFeed.from_counts(payload).changed_since(cursors)


__all__ = ["COUNT_BUCKETS", "SlackChangeFeed", "changed_conversations"]
