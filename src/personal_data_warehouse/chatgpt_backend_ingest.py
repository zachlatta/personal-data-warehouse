"""Poll the ChatGPT backend API and write conversations to ``agent_session_events``.

Server-side counterpart to the CLI agent-session uploader: instead of tailing
local transcripts, this lists the account's conversations newest-first, fetches
each one that is new or updated since the last sync (tracked in
``chatgpt_conversation_sync``), and normalizes its message tree via
``chatgpt_conversation_to_event_rows``.

Auth errors are intentionally *not* swallowed: ``ChatGPTAuthError`` propagates
out of ``sync()`` so the Dagster asset fails loudly and prompts a session
re-publish, rather than silently ingesting nothing.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from personal_data_warehouse.agent_sessions_drive_ingest import (
    chatgpt_conversation_to_event_rows,
)
from personal_data_warehouse.chatgpt_backend import ChatGPTBackendClient, ChatGPTRateLimitError

# A conversation is re-fetched when its backend ``update_time`` exceeds the
# synced value by more than this slack, which absorbs float-rounding noise.
_UPDATE_EPSILON_SECONDS = 0.5


@dataclass(frozen=True)
class ChatGPTBackendIngestSummary:
    conversations_seen: int
    conversations_fetched: int
    events_written: int
    reached_run_limit: bool
    rate_limited: bool = False
    stopped_at_high_water: bool = False


def chatgpt_poll_stall_reason(
    summary: ChatGPTBackendIngestSummary, *, max_conversations_per_run: int
) -> str | None:
    """Return an actionable message when a steady-state poll is pathologically stalled.

    With the high-water early stop, a healthy steady-state poll lists a single page
    and stops at the high-water mark, so it is essentially never rate limited. The
    dangerous state - the one behind the multi-day "saw N, fetched 0, wrote 0 (rate
    limited)" freeze - is a poll that gets throttled, writes nothing, and never even
    reaches the high-water mark to confirm it is caught up. In the run status that is
    indistinguishable from a healthy idle poll (both write nothing), so a persistent
    throttle silently ingests nothing while the asset reports success.

    Surface it as a loud failure, but only for that exact pathological shape: rate
    limited, zero conversations fetched, and not caught up. A poll that made any
    progress (fetched >= 1) or confirmed it was caught up is healthy and self-heals on
    the next tick. An explicit historical backfill (``max_conversations_per_run > 0``)
    deliberately pages down past synced conversations and is expected to trip the rate
    limiter, so it is never treated as a stall.
    """
    if max_conversations_per_run > 0:
        return None
    if (
        summary.rate_limited
        and summary.conversations_fetched == 0
        and not summary.stopped_at_high_water
    ):
        return (
            "ChatGPT backend poll was rate limited, fetched 0 conversations, and never "
            "reached the high-water mark to confirm it is caught up - it is ingesting "
            "nothing. The account or IP is likely throttled. If this persists, re-run "
            "`pdw chatgpt publish-session` to refresh the session and check for a "
            "deep-history re-walk regression in the poller."
        )
    return None


class ChatGPTBackendIngestRunner:
    def __init__(
        self,
        *,
        warehouse,
        client: ChatGPTBackendClient,
        account: str,
        device: str = "",
        page_size: int = 28,
        max_conversations_per_run: int = 0,
        logger,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._warehouse = warehouse
        self._client = client
        self._account = account
        self._device = device
        self._page_size = page_size
        self._max_conversations_per_run = max(0, max_conversations_per_run)
        self._logger = logger
        self._now = now or (lambda: datetime.now(tz=UTC))

    def sync(self) -> ChatGPTBackendIngestSummary:
        self._warehouse.ensure_agent_sessions_tables()
        ingested_at = self._now()
        synced = self._warehouse.chatgpt_conversation_sync_map(account=self._account)

        # Conversations are listed newest-first by ``update_time`` (``order=updated``),
        # and every conversation that needs fetching - brand new or freshly edited -
        # sorts to the top with an ``update_time`` greater than anything we have
        # already synced. So once we reach a conversation at or below the newest
        # update_time we have on record, everything further down is older and already
        # synced, and we can stop paging instead of walking the account's entire
        # history every tick (which otherwise lists deep enough to trip the backend's
        # rate limiter on every run, so the poll never makes progress).
        #
        # The early stop is disabled during an explicit historical backfill
        # (``max_conversations_per_run > 0``), where we intentionally page *down*
        # through already-synced conversations across runs to reach older, not-yet
        # -synced ones below the high-water mark.
        high_water: float | None = None
        if not self._max_conversations_per_run and synced:
            high_water = max(synced.values())

        conversations_seen = 0
        conversations_fetched = 0
        events_written = 0
        reached_limit = False
        rate_limited = False
        stopped_at_high_water = False

        try:
            refs = self._client.iter_conversation_refs(page_size=self._page_size)
            for ref in refs:
                conversations_seen += 1
                if not ref.id:
                    continue
                if high_water is not None and ref.update_time <= high_water + _UPDATE_EPSILON_SECONDS:
                    stopped_at_high_water = True
                    break
                previous = synced.get(ref.id)
                if previous is not None and ref.update_time <= previous + _UPDATE_EPSILON_SECONDS:
                    continue

                try:
                    conversation = self._client.get_conversation(ref.id)
                except ChatGPTRateLimitError as exc:
                    rate_limited = True
                    self._logger.warning(
                        "ChatGPT backend rate limited while fetching conversation %s%s; "
                        "stopping this poll and continuing on the next tick",
                        ref.id,
                        _retry_suffix(exc),
                    )
                    break

                rows = chatgpt_conversation_to_event_rows(
                    conversation,
                    account=self._account,
                    device=self._device,
                    ingested_at=ingested_at,
                )
                if rows:
                    self._warehouse.insert_agent_session_events(rows)
                    events_written += len(rows)
                self._warehouse.record_chatgpt_conversation_synced(
                    account=self._account,
                    session_id=ref.id,
                    update_time=ref.update_time,
                    event_count=len(rows),
                    synced_at=ingested_at,
                )
                conversations_fetched += 1

                if self._max_conversations_per_run and conversations_fetched >= self._max_conversations_per_run:
                    reached_limit = True
                    self._logger.info(
                        "Reached CHATGPT_MAX_CONVERSATIONS_PER_RUN=%s; deferring the rest to the next run",
                        self._max_conversations_per_run,
                    )
                    break
        except ChatGPTRateLimitError as exc:
            rate_limited = True
            self._logger.warning(
                "ChatGPT backend rate limited while listing conversations%s; "
                "stopping this poll and continuing on the next tick",
                _retry_suffix(exc),
            )

        self._logger.info(
            "ChatGPT sync: saw %s conversations, fetched %s, wrote %s events%s%s",
            conversations_seen,
            conversations_fetched,
            events_written,
            " (rate limited)" if rate_limited else "",
            " (caught up)" if stopped_at_high_water else "",
        )
        return ChatGPTBackendIngestSummary(
            conversations_seen=conversations_seen,
            conversations_fetched=conversations_fetched,
            events_written=events_written,
            reached_run_limit=reached_limit,
            rate_limited=rate_limited,
            stopped_at_high_water=stopped_at_high_water,
        )


def _retry_suffix(exc: ChatGPTRateLimitError) -> str:
    if exc.retry_after_seconds is None:
        return ""
    return f" (retry_after={exc.retry_after_seconds:g}s)"
