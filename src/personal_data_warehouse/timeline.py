"""Unified timeline over every warehouse source.

One physical table — ``timeline_events`` — holds one normalized row per unit
of activity anywhere in the warehouse (an email, a Slack message, an agent
session, a calendar event, a Drive file change, ...). It exists so that
cross-source features (the timeline UI, embeddings, "what came in since the
intelligence pass last ran") can consume a single stream instead of
re-implementing per-source queries:

- ``event_ts`` orders the timeline by when things actually happened.
- ``seq`` (a monotonically increasing sequence, bumped whenever a row's
  content changes) orders by *arrival/change*, so a consumer can checkpoint
  "I have processed everything up to seq N" and never miss late backfills.
- ``source_table`` + ``source_pk`` point back at the authoritative row, so
  the timeline stays skinny (capped previews only) and detail views fetch
  the full record from the source table.

Every warehouse table must be accounted for in ``TIMELINE_TABLE_COVERAGE``:
either it feeds an adapter (role ``events``), its rows are surfaced through a
parent event's detail view (role ``detail``, e.g. attachments and reactions),
it is a dimension joined into events (role ``entity``, e.g. slack_users), or
it is internal machinery (role ``state``, e.g. sync cursors and credentials).
``tests/test_timeline.py`` enforces this against the live schema, so adding a
warehouse table without classifying it here fails the suite — the timeline is
guaranteed to represent everything, never a silent subset.

Sync strategy (``TimelineSyncEngine``): per adapter, an initial *backfill*
walks the source newest-first by event time (so the timeline is useful from
the first minutes even while history is still loading), while *incremental*
sync tails the source's ingestion timestamp forward. Both are keyset-paginated
and idempotent; upserts only bump ``seq`` when the normalized content actually
changed. The engine reads from one connection and writes through another, so
the same code serves prod (source == dest) and a local timeline built from a
read-only prod connection (see ``python -m personal_data_warehouse.timeline``).
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
import json
import logging
import time
from typing import Any

import psycopg2
from psycopg2.extras import execute_values

from personal_data_warehouse.config import normalize_postgres_url
from personal_data_warehouse.relations import physical_schema_names, qualify_sql_relations

logger = logging.getLogger(__name__)

TIMELINE_SNIPPET_CHARS = 500
TIMELINE_TITLE_CHARS = 300
TIMELINE_DEFAULT_BATCH_SIZE = 2000

# Priority tiers, classified per row at sync time (1 = highest). The lines
# between tiers are heuristics and expected to be tuned; changing an
# adapter's classification and re-running the backfill reclassifies rows
# (priority participates in the content guard, so seq bumps on change).
TIMELINE_PRIORITY_SELF = 1  # actions Zach initiated (his messages, sessions, memos, notes)
TIMELINE_PRIORITY_DIRECT = 2  # real people reaching him directly (DMs, direct email, small groups)
TIMELINE_PRIORITY_CC = 3  # real-people activity he is peripheral to (cc'd, channels, big groups)
TIMELINE_PRIORITY_NOISE = 4  # bulk/automated traffic (newsletters, bots, non-member channels)
TIMELINE_PRIORITY_BACKGROUND = 5  # the warehouse's own machinery (enrichment, mutation workers)

_EPOCH = "'1970-01-01 00:00:00+00'::timestamptz"
# Sentinel guard: house style stores "no timestamp" as the epoch, so anything
# at or before this is treated as absent.
_EPOCH_GUARD = "'1970-01-02 00:00:00+00'::timestamptz"
# Where the newest-first backfill cursor starts. A finite far-future constant
# (not 'infinity') so the value roundtrips cleanly through drivers and the
# NOT NULL state row.
BACKFILL_CURSOR_START = datetime(9999, 1, 1, tzinfo=UTC)

# The exact output shape every adapter query must produce, in order. The
# engine prepends the adapter name and appends the source table on insert.
TIMELINE_NORMALIZED_COLUMNS = (
    "event_id",
    "source",
    "kind",
    "event_ts",
    "end_ts",
    "actor",
    "title",
    "snippet",
    "context",
    "source_pk",
    "metadata",
    "search_text",
    "ingest_ts",
    "priority",
)


@dataclass(frozen=True)
class TimelineAdapter:
    """Normalizes one source table (plus dimension joins) into timeline rows.

    ``backfill_sql`` pages newest-first by ``(event_ts, event_id)`` with
    ``%(cursor_ts)s``/``%(cursor_id)s``/``%(limit)s`` params; ``incremental_sql``
    pages oldest-first by ``(ingest_ts, event_id)`` with
    ``%(watermark_ts)s``/``%(watermark_id)s``/``%(limit)s`` params. Both return
    exactly ``TIMELINE_NORMALIZED_COLUMNS``. ``max_ingest_sql`` returns the
    source's current maximum ingestion timestamp (one row, one column) and is
    used to initialize the incremental watermark before the backfill starts.
    """

    name: str
    source_table: str
    source: str
    kind: str
    backfill_sql: str
    incremental_sql: str
    max_ingest_sql: str
    batch_size: int = TIMELINE_DEFAULT_BATCH_SIZE
    # When > 0, every sync pass re-walks rows whose event_ts falls in the last
    # N hours and re-upserts them. Classification signals that look forward or
    # arrive late (Zach replying in a chat promotes the surrounding window;
    # his answer to an email promotes the thread) converge through this window
    # instead of freezing at first-ingest values.
    refresh_hours: float = 0.0


def _real_ts(*exprs: str) -> str:
    """First expression that is a real (non-epoch-sentinel) timestamp."""
    parts = ", ".join(f"NULLIF({expr}, {_EPOCH})" for expr in exprs)
    return f"COALESCE({parts}, {_EPOCH})"


def _simple_adapter(
    *,
    name: str,
    source_table: str,
    source: str,
    kind: str,
    from_sql: str,
    event_id: str,
    event_ts: str,
    ingest_ts: str,
    source_pk: str,
    end_ts: str = _EPOCH,
    actor: str = "''",
    title: str = "''",
    snippet: str = "''",
    context: str = "''",
    metadata: str = "'{}'::jsonb",
    search_text: str | None = None,
    priority: str = str(TIMELINE_PRIORITY_CC),
    where: str = "TRUE",
    batch_size: int = TIMELINE_DEFAULT_BATCH_SIZE,
    refresh_hours: float = 0.0,
) -> TimelineAdapter:
    if search_text is None:
        search_text = _search_concat(title, snippet, context, actor)

    # event_ts and ingest_ts are used raw (no defensive COALESCE): they are the
    # ORDER BY / keyset expressions, and wrapping a bare indexed column in a
    # function forces a full sort of the source table on every backfill batch
    # (measured at ~90s/batch on the 30M-row slack_messages in production).
    # Source columns are NOT NULL throughout the warehouse schema; adapters
    # that need fallback chains state them explicitly.
    select = f"""
        SELECT
            COALESCE(({event_id}), '') AS event_id,
            '{source}' AS source,
            '{kind}' AS kind,
            ({event_ts}) AS event_ts,
            COALESCE(({end_ts}), {_EPOCH}) AS end_ts,
            COALESCE(({actor}), '') AS actor,
            COALESCE(({title}), '') AS title,
            COALESCE(({snippet}), '') AS snippet,
            COALESCE(({context}), '') AS context,
            ({source_pk})::text AS source_pk,
            COALESCE(({metadata}), '{{}}'::jsonb)::text AS metadata,
            COALESCE(({search_text}), '') AS search_text,
            ({ingest_ts}) AS ingest_ts,
            COALESCE(({priority}), {TIMELINE_PRIORITY_CC}) AS priority
        FROM {from_sql}
        WHERE ({where})
    """
    backfill_sql = f"""
        {select}
          AND ({event_ts}) <= %(cursor_ts)s
          AND (({event_ts}), COALESCE(({event_id}), ''))
              < (%(cursor_ts)s, %(cursor_id)s)
        ORDER BY 4 DESC, 1 DESC
        LIMIT %(limit)s
    """
    incremental_sql = f"""
        {select}
          AND ({ingest_ts}) >= %(watermark_ts)s
          AND (({ingest_ts}), COALESCE(({event_id}), ''))
              > (%(watermark_ts)s, %(watermark_id)s)
        ORDER BY 13 ASC, 1 ASC
        LIMIT %(limit)s
    """
    max_ingest_sql = f"SELECT max({ingest_ts}) FROM {from_sql} WHERE ({where})"
    return TimelineAdapter(
        name=name,
        source_table=source_table,
        source=source,
        kind=kind,
        backfill_sql=backfill_sql,
        incremental_sql=incremental_sql,
        max_ingest_sql=max_ingest_sql,
        batch_size=batch_size,
        refresh_hours=refresh_hours,
    )


def _snippet(expr: str) -> str:
    return f"left({expr}, {TIMELINE_SNIPPET_CHARS})"


def _search_concat(*exprs: str) -> str:
    """Build a newline-separated, BM25-indexed document for one timeline row."""
    parts = ", ".join(f"NULLIF(({expr})::text, '')" for expr in exprs)
    return f"concat_ws(E'\\n', {parts})"


def _html_unescape(expr: str) -> str:
    """Decode the common HTML entities Gmail leaves in snippets/subjects."""
    result = expr
    semicolon = " || chr(59)"
    for needle, replacement in (
        ("'&#39'" + semicolon, "chr(39)"),
        ("'&#x27'" + semicolon, "chr(39)"),
        ("'&quot'" + semicolon, "chr(34)"),
        ("'&lt'" + semicolon, "'<'"),
        ("'&gt'" + semicolon, "'>'"),
        ("'&nbsp'" + semicolon, "' '"),
        ("'&amp'" + semicolon, "'&'"),
    ):
        result = f"replace({result}, {needle}, {replacement})"
    return result


# Sender-pattern fallbacks for mail Gmail's categorizer misses (it labels
# most modern bulk mail, but pre-2016 history and some transactional senders
# carry no category). Benchmark-tuned (sampling/ 2026-07): pure machine mail
# is noise; the only automated senders kept above noise are ones RELAYING a
# real person's activity (GitHub comments, Docs comments, list mail).
_GMAIL_BULK_SENDER_PATTERN = (
    "'(no-?reply|donotreply|do-not-reply|mailer|postmaster|bounce|"
    "newsletter|marketing@|promo)'"
)
_GMAIL_AUTOMATED_SENDER_PATTERN = (
    "'(notifications?@|digest@|updates@|alerts?@|billing@|receipts?@|invoice|"
    "statements?@|bank@|hcb@|dinobox@|sign@|bot@|replies\\+|info@|contact@|hello@|"
    "support@|feedback@|service@|security@|account@|verify|apply@|jobs@|"
    "calendar-notification@|\\mmail@|menu@|reports?@|abuse@|coolify@|deploy@|"
    "\\mci@|build@|@members\\.|"
    "@(email|mail|msg|notify|alert|news|marketing|info|update)[\\w-]*\\.|"
    "^(education|announce(ments)?|events?|press|community|news)@)'"
)
# Automated senders that carry a human's words to Zach (code-review comments,
# issue replies, list discussion) stay at the cc tier instead of noise.
_GMAIL_RELAY_SENDER_PATTERN = (
    "'(notifications@github\\.com|@noreply\\.github\\.com|gitlab@|"
    "comments-noreply@docs\\.google|notify@aur\\.archlinux)'"
)
# Whitelisted product notifications that relay a specific person's request or
# share to the account owner. They are still generated mail, so keep them at cc
# instead of direct, but they are not newsletter/automation noise.
_GMAIL_HUMAN_ACTION_RELAY = (
    "(t.from_address ~* '(drive-shares-dm-noreply@google\\.com|"
    "no-reply@email\\.figma\\.com|noreply@airtable\\.com|mail@signnow\\.com|"
    "notifications@vercel\\.com|notifications@letsjelly\\.com|"
    "notifications@mail\\.granola\\.ai|feedback@slack\\.com)' "
    " AND (t.subject || ' ' || t.snippet) ~* '(upgrade request|requested access|"
    "access request|signature request|shared (a |the )?(document|folder|form|file|meeting notes)|"
    "shared with you|invited you to (edit|comment|sign|review|join|build)|"
    "has invited you|wants access|action required|deletion request)')"
)
_GMAIL_OTP_SUBJECT_PATTERN = (
    "'(login code|verification code|security code|confirmation code|authentication code|"
    "confirm(ation)? code|one.?time|password reset|identification code|2fa)'"
)
_GMAIL_RSVP_SUBJECT_PATTERN = (
    "'^(accepted|declined|tentatively accepted|updated invitation|"
    "canceled event|invitation)[: ]'"
)
_GMAIL_RELAYED_BOT_BODY_PATTERN = (
    "'(\\[bot\\] left a comment|latest updates on your projects|dependabot)'"
)
_GMAIL_CI_SUBJECT_PATTERN = (
    "'(run failed|workflow run|deploy(ment)? (failed|succeeded)|build failed)'"
)
# Normalized subject prefix used to spot mail-merge blasts: strip reply/fwd
# prefixes, lowercase, first 24 chars. Must match the expression index
# gmail_messages_merge_prefix_idx exactly.
_GMAIL_MERGE_PREFIX = (
    "left(regexp_replace(lower({col}), '^((re|fwd|fw)(\\[\\d+\\])?:\\s*)+', ''), 24)"
)

# From-address belongs to one of Zach's synced mailboxes (any account).
_GMAIL_FROM_SELF = (
    "(t.from_address ILIKE '%%' || t.account || '%%' "
    " OR 'SENT' = ANY(t.label_ids) "
    " OR EXISTS (SELECT 1 FROM gmail_sync_state self "
    "            WHERE self.account <> '' AND t.from_address ILIKE '%%' || self.account || '%%'))"
)
# Addressed to Zach himself: a synced account or his personal domain in To.
_GMAIL_ADDRESSED = (
    "EXISTS (SELECT 1 FROM unnest(t.to_addresses) rcpt "
    "        WHERE rcpt ILIKE '%%' || t.account || '%%' "
    "           OR lower(rcpt) LIKE '%%@zachlatta.com%%' "
    "           OR EXISTS (SELECT 1 FROM gmail_sync_state self "
    "                      WHERE self.account <> '' AND rcpt ILIKE '%%' || self.account || '%%'))"
)
# >=30 self-sent messages sharing a normalized subject prefix within +/-3 days
# = a mail-merge blast (quote-shopping batches stay under the threshold).
_GMAIL_MERGE_CLUSTER = (
    "(SELECT count(*) FROM ("
    " SELECT 1 FROM gmail_messages g2"
    f" WHERE {_GMAIL_MERGE_PREFIX.format(col='g2.subject')} = {_GMAIL_MERGE_PREFIX.format(col='t.subject')}"
    "  AND g2.internal_date BETWEEN t.internal_date - interval '3 days'"
    "                           AND t.internal_date + interval '3 days'"
    "  AND g2.from_address ILIKE '%%' || g2.account || '%%'"
    " LIMIT 30) merge_probe) >= 30"
)
_GMAIL_THREAD_INBOUND_BEFORE = (
    "EXISTS (SELECT 1 FROM gmail_messages g3 "
    "        WHERE g3.thread_id = t.thread_id "
    "          AND g3.internal_date < t.internal_date "
    "          AND g3.from_address NOT ILIKE '%%' || g3.account || '%%' "
    "          AND NOT EXISTS (SELECT 1 FROM gmail_sync_state s3 "
    "                          WHERE s3.account <> '' AND g3.from_address ILIKE '%%' || s3.account || '%%'))"
)
# Zach answered this thread after the message arrived (within 48h): the
# strongest "this conversation has his attention" signal.
_GMAIL_MY_REPLY_AFTER = (
    "EXISTS (SELECT 1 FROM gmail_messages g4 "
    "        WHERE g4.thread_id = t.thread_id "
    "          AND g4.internal_date > t.internal_date "
    "          AND g4.internal_date < t.internal_date + interval '48 hours' "
    "          AND g4.from_address ILIKE '%%' || g4.account || '%%')"
)
_GMAIL_I_POSTED_IN_THREAD = (
    "EXISTS (SELECT 1 FROM gmail_messages g5 "
    "        WHERE g5.thread_id = t.thread_id "
    "          AND g5.from_address ILIKE '%%' || g5.account || '%%')"
)
# Sender is someone Zach has written to at least twice (relationship signal;
# the table is timeline-owned state refreshed by the sync engine).
_GMAIL_KNOWN_CORRESPONDENT = (
    "EXISTS (SELECT 1 FROM timeline_gmail_correspondents gc "
    "        WHERE gc.addr = lower(COALESCE(NULLIF(substring(t.from_address FROM '<([^>]+)>'), ''), "
    "                                       t.from_address)) "
    "          AND gc.n_sent_to >= 2)"
)
_GMAIL_BULK_CATEGORY = (
    "('CATEGORY_PROMOTIONS' = ANY(t.label_ids) OR 'CATEGORY_UPDATES' = ANY(t.label_ids) "
    " OR 'CATEGORY_FORUMS' = ANY(t.label_ids) OR 'CATEGORY_SOCIAL' = ANY(t.label_ids))"
)
# Gmail's CATEGORY_FORUMS bucket spans both real human list discussion and
# newsletter/digest/list-announcement traffic. Preserve the former at cc, but
# demote common broadcast shapes found while sampling the live timeline.
_GMAIL_FORUMS_NOISE = (
    "(t.subject ~* '(digest for .* updates? in .* topics?|recommendations from your substacks)' "
    " OR (t.subject ~* '^\\[[^\\]]+\\]' AND EXISTS ("
    "      SELECT 1 FROM unnest(t.to_addresses || t.cc_addresses) forum_addr "
    "      WHERE forum_addr ~* 'googlegroups\\.com')) "
    " OR t.snippet ~* '(google groups (logo|topic digest)|view all topics|view in browser|"
    "dear .{0,60}(members|newlisters|craigs?newlisters))' "
    " OR t.snippet LIKE '%%͏%%' OR t.snippet LIKE '%%­%%' OR t.snippet LIKE '%%‌%%')"
)
_GMAIL_AUTOMATED_FROM = (
    f"(t.from_address ~* {_GMAIL_BULK_SENDER_PATTERN} "
    f" OR t.from_address ~* {_GMAIL_AUTOMATED_SENDER_PATTERN} "
    " OR t.from_address ~* '\\[bot\\]')"
)

_GMAIL_EMAIL = _simple_adapter(
    name="gmail_email",
    source_table="gmail_messages",
    source="gmail",
    kind="email",
    from_sql="""gmail_messages t
    LEFT JOIN LATERAL (
        SELECT
            string_agg(
                concat_ws(E'\n', NULLIF(a.filename, ''), NULLIF(e.text, '')),
                E'\n' ORDER BY a.part_id, e.updated_at DESC
            ) AS attachment_search_text,
            max(GREATEST(a.synced_at, COALESCE(e.updated_at, a.synced_at))) AS attachment_ingest_ts
        FROM gmail_attachments a
        LEFT JOIN file_attachment_enrichments e ON e.content_sha256 = a.content_sha256
        WHERE a.account = t.account AND a.message_id = t.message_id AND a.is_deleted = 0
    ) att ON TRUE""",
    event_id="concat_ws('|', t.account, t.message_id)",
    # Bare column, not a COALESCE chain: event_ts is the backfill's ORDER BY
    # and keyset key, and only a plain column keeps the scan on
    # gmail_messages_internal_date_idx instead of sorting 800k rows per batch.
    # The handful of rows with an epoch-sentinel internal_date land at 1970,
    # which reads as "date unknown" rather than inventing a sync-time date.
    event_ts="t.internal_date",
    ingest_ts="GREATEST(t.synced_at, COALESCE(att.attachment_ingest_ts, t.synced_at))",
    actor="t.from_address",
    title=_html_unescape("t.subject"),
    snippet=_snippet(_html_unescape("t.snippet")),
    context="t.account",
    source_pk="jsonb_build_object('account', t.account, 'message_id', t.message_id)",
    metadata=(
        "jsonb_build_object("
        "'thread_id', t.thread_id, "
        "'labels', to_jsonb(t.label_ids), "
        "'deleted', t.is_deleted <> 0)"
    ),
    search_text=_search_concat(
        "t.subject",
        "t.from_address",
        "array_to_string(t.to_addresses || t.cc_addresses || t.bcc_addresses, ' ')",
        "t.snippet",
        "t.body_text",
        "t.body_markdown_clean",
        "att.attachment_search_text",
    ),
    # Benchmark-tuned ordering (sampling/rubric.md, 2026-07). Broadly:
    # my own mail (minus mail-merge blasts) > relayed-human notifications at
    # the cc tier > threads I engage with > known humans addressed to me >
    # starred > bulk/automated > addressed to me > everything else at cc.
    priority=(
        "CASE "
        # Mail from any of my synced mailboxes is my own action, including the
        # copy that lands in a different account (cross-account forwards) —
        # unless it is one send of a mail-merge blast nobody had replied to.
        f"WHEN {_GMAIL_FROM_SELF} THEN "
        f"  CASE WHEN {_GMAIL_MERGE_CLUSTER} AND NOT {_GMAIL_THREAD_INBOUND_BEFORE} THEN 4 ELSE 1 END "
        "WHEN 'SPAM' = ANY(t.label_ids) OR 'TRASH' = ANY(t.label_ids) THEN 4 "
        # Relay services carrying a human's activity: mention/author copies are
        # directed at me; bot-authored payloads (CI, deploy status) are noise;
        # the rest is skim-worthy cc.
        f"WHEN t.from_address ~* {_GMAIL_RELAY_SENDER_PATTERN} THEN "
        "  CASE WHEN EXISTS (SELECT 1 FROM unnest(t.to_addresses || t.cc_addresses) a "
        "                    WHERE a ILIKE '%%mention@noreply.github.com%%' "
        "                       OR a ILIKE '%%author@noreply.github.com%%') THEN 2 "
        f"       WHEN t.from_address ~* '\\[bot\\]' OR t.subject ~* {_GMAIL_CI_SUBJECT_PATTERN} "
        f"         OR t.snippet ~* {_GMAIL_RELAYED_BOT_BODY_PATTERN} "
        "         OR t.subject ~* '^(re: )?\\[[^\\]]+\\] bump ' THEN 4 "
        "       ELSE 3 END "
        f"WHEN t.subject ~* {_GMAIL_RSVP_SUBJECT_PATTERN} THEN 3 "
        f"WHEN t.subject ~* {_GMAIL_OTP_SUBJECT_PATTERN} THEN 4 "
        f"WHEN {_GMAIL_AUTOMATED_FROM} AND t.subject ~* '^(re: )?new comment' THEN 3 "
        f"WHEN {_GMAIL_HUMAN_ACTION_RELAY} THEN 3 "
        f"WHEN 'CATEGORY_FORUMS' = ANY(t.label_ids) AND ({_GMAIL_AUTOMATED_FROM} OR {_GMAIL_FORUMS_NOISE}) THEN 4 "
        f"WHEN 'CATEGORY_FORUMS' = ANY(t.label_ids) AND t.from_address !~* {_GMAIL_BULK_SENDER_PATTERN} THEN 3 "
        f"WHEN NOT {_GMAIL_AUTOMATED_FROM} AND {_GMAIL_MY_REPLY_AFTER} THEN 2 "
        f"WHEN NOT {_GMAIL_AUTOMATED_FROM} AND {_GMAIL_I_POSTED_IN_THREAD} THEN "
        f"  CASE WHEN {_GMAIL_ADDRESSED} THEN 2 ELSE 3 END "
        f"WHEN NOT {_GMAIL_AUTOMATED_FROM} AND {_GMAIL_KNOWN_CORRESPONDENT} AND {_GMAIL_ADDRESSED} THEN 2 "
        f"WHEN 'STARRED' = ANY(t.label_ids) AND NOT {_GMAIL_AUTOMATED_FROM} "
        f"  AND NOT {_GMAIL_BULK_CATEGORY} THEN 2 "
        f"WHEN {_GMAIL_BULK_CATEGORY} OR {_GMAIL_AUTOMATED_FROM} THEN 4 "
        f"WHEN {_GMAIL_ADDRESSED} THEN 2 "
        "ELSE 3 END"
    ),
    refresh_hours=72,
)

_SLACK_JOINS = """
    LEFT JOIN slack_users u
        ON u.account = t.account AND u.team_id = t.team_id AND u.user_id = t.user_id
    LEFT JOIN slack_conversations c
        ON c.account = t.account AND c.team_id = t.team_id AND c.conversation_id = t.conversation_id
    LEFT JOIN slack_account_identities ident
        ON ident.account = t.account AND ident.team_id = t.team_id
"""

# The root of this thread is one of Zach's own messages: a reply to him.
# A single primary-key probe per threaded row.
_SLACK_THREAD_ROOT_MINE = (
    "(t.thread_ts <> '' AND ident.user_id <> '' AND EXISTS ("
    "SELECT 1 FROM slack_messages z "
    "WHERE z.account = t.account AND z.team_id = t.team_id "
    "  AND z.conversation_id = t.conversation_id AND z.message_ts = t.thread_ts "
    "  AND z.user_id = ident.user_id AND z.is_deleted = 0))"
)
# Zach posted in this thread within the preceding 12 hours: the reply lands in
# a conversation he is actively part of. (Unbounded thread participation
# over-promoted: RSVP piles in announcement threads and day-old ship threads
# read as ambient, per the labeled benchmark.)
_SLACK_MY_THREAD_RECENT = (
    "(t.thread_ts <> '' AND ident.user_id <> '' AND EXISTS ("
    "SELECT 1 FROM slack_messages z "
    "WHERE z.user_id = ident.user_id "
    "  AND z.message_datetime BETWEEN t.message_datetime - interval '12 hours' "
    "                             AND t.message_datetime "
    "  AND z.account = t.account AND z.team_id = t.team_id "
    "  AND z.conversation_id = t.conversation_id AND z.thread_ts = t.thread_ts "
    "  AND z.is_deleted = 0))"
)


def _slack_my_msgs_in_window(*, before: str, after: str, limit: int) -> str:
    """Count (capped) of Zach's own messages in this conversation around the
    row's time; rides slack_messages_user_time_idx so each probe is a short
    range scan of his messages only. Lazy inside CASE branches, so the
    firehose rows that resolve earlier never pay for it."""
    return (
        "(SELECT count(*) FROM ("
        " SELECT 1 FROM slack_messages z"
        " WHERE z.user_id = ident.user_id"
        f"  AND z.message_datetime BETWEEN t.message_datetime - interval '{before}'"
        f"                             AND t.message_datetime + interval '{after}'"
        "  AND z.account = t.account AND z.team_id = t.team_id"
        "  AND z.conversation_id = t.conversation_id AND z.is_deleted = 0"
        f" LIMIT {limit}) win) "
    )


_SLACK_W6H = _slack_my_msgs_in_window(before="6 hours", after="6 hours", limit=4)
_SLACK_P3D = _slack_my_msgs_in_window(before="3 days", after="0 hours", limit=3)
_SLACK_P24H = _slack_my_msgs_in_window(before="24 hours", after="0 hours", limit=1)

# Channel velocity: messages from anyone in the 24h before this row. The
# "is this channel a firehose" signal — slack_conversations.num_members flaps
# to 0 in production syncs, so size cannot be trusted; behavior can.
_SLACK_CONV_VELOCITY_24H = (
    "(SELECT count(*) FROM ("
    " SELECT 1 FROM slack_messages v"
    " WHERE v.account = t.account AND v.team_id = t.team_id"
    "  AND v.conversation_id = t.conversation_id AND v.is_deleted = 0"
    "  AND v.message_datetime BETWEEN t.message_datetime - interval '24 hours'"
    "                             AND t.message_datetime"
    " LIMIT 151) vel) "
)

_SLACK_DISPLAY_NAME = (
    "COALESCE(NULLIF(u.display_name, ''), NULLIF(u.real_name, ''), "
    "NULLIF(u.name, ''), t.username)"
)
_SLACK_IS_BOT = (
    "(t.bot_id <> '' OR t.user_id LIKE 'USLACK%%' OR u.is_bot = 1 "
    " OR t.subtype LIKE 'bot%%' OR (t.user_id = '' AND t.username <> '') "
    f" OR {_SLACK_DISPLAY_NAME} ~* 'bot\\M')"
)
_SLACK_SYSTEM_SUBTYPES = (
    "('channel_join', 'channel_leave', 'channel_archive', 'channel_name', "
    "'channel_purpose', 'channel_topic', 'group_join', 'group_leave')"
)
_SLACK_MPIM_ROSTER = (
    "GREATEST(c.num_members, (SELECT count(*) FROM slack_conversation_members m "
    "WHERE m.account = t.account AND m.team_id = t.team_id "
    "  AND m.conversation_id = t.conversation_id AND m.is_deleted = 0))"
)
_SLACK_ATTACHMENT_ONLY_MESSAGE = (
    "(t.text = '' AND t.raw_json LIKE '%%\"files\":[%%' AND t.raw_json NOT LIKE '%%\"files\":[]%%')"
)
_SLACK_INACCESSIBLE_FILE_STUB = (
    "(COALESCE(NULLIF(t.title, ''), NULLIF(t.name, '')) IS NULL "
    " AND COALESCE(t.size, 0) = 0 "
    " AND (t.raw_json LIKE '%%\"file_access\":\"not_visible\"%%' "
    "      OR t.raw_json LIKE '%%\"file_access\":\"file_not_found\"%%' "
    "      OR t.filetype = 'quip'))"
)
_SLACK_MESSAGE_TS_AS_TIMESTAMPTZ = (
    "(CASE WHEN t.message_ts ~ '^[0-9]+(\\.[0-9]+)?$' "
    "THEN to_timestamp(t.message_ts::numeric) ELSE NULL END)"
)
_SLACK_DM_CONTEXT = (
    "(SELECT 'DM with ' || COALESCE(NULLIF(peer.display_name, ''), NULLIF(peer.real_name, ''), "
    "                               NULLIF(peer.name, ''), NULLIF(peer.email, ''), dm_peer.user_id) "
    " FROM ("
    "   SELECT m.user_id, 0 AS source_order "
    "   FROM slack_conversation_members m "
    "   WHERE m.account = t.account AND m.team_id = t.team_id "
    "     AND m.conversation_id = t.conversation_id AND m.is_deleted = 0 "
    "     AND (ident.user_id = '' OR m.user_id <> ident.user_id) "
    "   UNION ALL "
    "   SELECT c.name AS user_id, 1 AS source_order WHERE c.name <> ''"
    " ) dm_peer "
    " LEFT JOIN slack_users peer "
    "   ON peer.account = t.account AND peer.team_id = t.team_id AND peer.user_id = dm_peer.user_id "
    " WHERE dm_peer.user_id <> '' "
    " ORDER BY dm_peer.source_order, dm_peer.user_id LIMIT 1)"
)

# Benchmark-tuned ordering (sampling/rubric.md, 2026-07). Mine > bots (app DMs
# relaying a human's action stay skim-worthy) > system messages > DMs >
# mentions and name references > replies to/with him in threads > group DMs he
# is engaged in > channel conversations he is actively part of > ambient
# member channels > the workspace firehose. Two standing reversals from
# sampling: "channels I post in a lot" must NOT promote (lounge-chatter
# flood), and one drive-by message must not promote a busy channel's +/-6h —
# participation means at least two of his messages in the window.
_SLACK_MESSAGE_PRIORITY = (
    "CASE "
    f"WHEN {_SLACK_ATTACHMENT_ONLY_MESSAGE} THEN {TIMELINE_PRIORITY_BACKGROUND} "
    "WHEN t.user_id <> '' AND t.user_id = ident.user_id THEN 1 "
    f"WHEN {_SLACK_IS_BOT} THEN "
    "  CASE WHEN c.is_im = 1 AND t.text ~* '(commented on|shared an item|replied to|"
    "mentioned you|upgrade request|invited you|assigned you)' THEN 3 ELSE 4 END "
    f"WHEN t.subtype IN {_SLACK_SYSTEM_SUBTYPES} THEN 4 "
    "WHEN c.is_im = 1 THEN 2 "
    "WHEN ident.user_id <> '' AND t.text LIKE '%%<@' || ident.user_id || '>%%' THEN 2 "
    "WHEN t.text ~* '\\m(zrl|latta|zach latta|zachlatta)\\M' THEN 2 "
    "WHEN t.text ~* '\\mzach\\M' AND (c.is_private = 1 "
    f"  OR c.num_members <= 1000 OR {_SLACK_W6H} >= 1) THEN 2 "
    f"WHEN {_SLACK_THREAD_ROOT_MINE} THEN 2 "
    f"WHEN {_SLACK_MY_THREAD_RECENT} THEN 2 "
    f"WHEN c.is_mpim = 1 THEN "
    f"  CASE WHEN {_SLACK_W6H} >= 1 OR {_SLACK_MPIM_ROSTER} BETWEEN 1 AND 5 "
    f"        OR {_SLACK_P3D} >= 3 THEN 2 ELSE 3 END "
    "WHEN c.is_member = 1 THEN "
    f"  CASE WHEN {_SLACK_W6H} >= 2 AND ({_SLACK_CONV_VELOCITY_24H} <= 150 "
    f"         OR ({_SLACK_W6H} >= 3 AND {_SLACK_P3D} >= 2)) THEN 2 "
    f"       WHEN c.is_private = 1 AND {_SLACK_MPIM_ROSTER} <= 20 "
    f"         AND {_SLACK_P24H} >= 1 THEN 2 "
    "       ELSE 3 END "
    "ELSE 4 END"
)

_SLACK_FILE_PRIORITY = (
    "CASE "
    f"WHEN {_SLACK_INACCESSIBLE_FILE_STUB} THEN {TIMELINE_PRIORITY_BACKGROUND} "
    "WHEN t.user_id <> '' AND t.user_id = ident.user_id THEN 1 "
    "WHEN u.is_bot = 1 THEN 4 "
    "WHEN c.is_im = 1 THEN 2 "
    f"WHEN c.is_mpim = 1 THEN "
    "  CASE WHEN (SELECT count(*) FROM (SELECT 1 FROM slack_messages z "
    "       WHERE z.user_id = ident.user_id "
    "         AND z.message_datetime BETWEEN t.created_at - interval '6 hours' "
    "                                    AND t.created_at + interval '6 hours' "
    "         AND z.account = t.account AND z.team_id = t.team_id "
    "         AND z.conversation_id = t.conversation_id AND z.is_deleted = 0 "
    f"       LIMIT 1) fw) >= 1 OR {_SLACK_MPIM_ROSTER} BETWEEN 1 AND 5 THEN 2 ELSE 3 END "
    "WHEN c.is_member = 1 THEN "
    "  CASE WHEN (SELECT count(*) FROM (SELECT 1 FROM slack_messages z "
    "       WHERE z.user_id = ident.user_id "
    "         AND z.message_datetime BETWEEN t.created_at - interval '6 hours' "
    "                                    AND t.created_at + interval '6 hours' "
    "         AND z.account = t.account AND z.team_id = t.team_id "
    "         AND z.conversation_id = t.conversation_id AND z.is_deleted = 0 "
    "       LIMIT 2) fw) >= 2 THEN 2 ELSE 3 END "
    "ELSE 4 END"
)
# IM/MPIM checks come first: slack stores a user-id-ish "name" on DM
# conversations, which otherwise renders as a channel called #U0xxxx.
_SLACK_CONTEXT = (
    f"CASE WHEN c.is_im = 1 THEN COALESCE({_SLACK_DM_CONTEXT}, 'DM') "
    "WHEN c.is_mpim = 1 THEN 'group DM' "
    "WHEN NULLIF(c.name, '') IS NOT NULL THEN '#' || c.name "
    "ELSE t.conversation_id END"
)

_SLACK_MESSAGE = _simple_adapter(
    name="slack_message",
    source_table="slack_messages",
    source="slack",
    kind="message",
    from_sql="slack_messages t" + _SLACK_JOINS,
    event_id="concat_ws('|', t.account, t.team_id, t.conversation_id, t.message_ts)",
    # Bare column so the 30M+-row backfill pages via slack_messages_time_idx;
    # an expression here forces a full sort per batch (see gmail_email).
    event_ts="t.message_datetime",
    ingest_ts="t.synced_at",
    actor=(
        "COALESCE(NULLIF(u.display_name, ''), NULLIF(u.real_name, ''), NULLIF(u.name, ''), "
        "NULLIF(t.username, ''), NULLIF(t.user_id, ''), NULLIF(t.bot_id, ''), '')"
    ),
    snippet=_snippet("t.text"),
    context=_SLACK_CONTEXT,
    source_pk=(
        "jsonb_build_object('account', t.account, 'team_id', t.team_id, "
        "'conversation_id', t.conversation_id, 'message_ts', t.message_ts)"
    ),
    metadata=(
        "jsonb_build_object("
        "'thread_ts', t.thread_ts, "
        "'subtype', t.subtype, "
        "'reply_count', t.reply_count, "
        "'bot', t.bot_id <> '', "
        "'edited', t.edited_ts <> '', "
        "'deleted', t.is_deleted <> 0)"
    ),
    search_text=_search_concat(_SLACK_DISPLAY_NAME, _SLACK_CONTEXT, "t.text"),
    priority=_SLACK_MESSAGE_PRIORITY,
    batch_size=5000,
    # 12h covers the +/-6h engagement window with margin while keeping the
    # per-tick re-walk (~9k rows with window probes) inside the work budget.
    refresh_hours=12,
)

_SLACK_FILE = _simple_adapter(
    name="slack_file",
    source_table="slack_files",
    source="slack",
    kind="file_share",
    from_sql="slack_files t" + _SLACK_JOINS,
    event_id="concat_ws('|', t.account, t.team_id, t.file_id, t.conversation_id, t.message_ts)",
    event_ts=_real_ts("t.created_at", _SLACK_MESSAGE_TS_AS_TIMESTAMPTZ, "t.synced_at"),
    ingest_ts="t.synced_at",
    actor=(
        "COALESCE(NULLIF(u.display_name, ''), NULLIF(u.real_name, ''), NULLIF(u.name, ''), "
        "NULLIF(t.user_id, ''), '')"
    ),
    title="COALESCE(NULLIF(t.title, ''), t.name)",
    context=_SLACK_CONTEXT,
    source_pk=(
        "jsonb_build_object('account', t.account, 'team_id', t.team_id, 'file_id', t.file_id, "
        "'conversation_id', t.conversation_id, 'message_ts', t.message_ts)"
    ),
    metadata=(
        "jsonb_build_object("
        "'mimetype', t.mimetype, "
        "'filetype', t.filetype, "
        "'size', t.size, "
        "'deleted', t.is_deleted <> 0)"
    ),
    search_text=_search_concat(
        "t.name",
        "t.title",
        "t.mimetype",
        "COALESCE(NULLIF(u.display_name, ''), NULLIF(u.real_name, ''), NULLIF(u.name, ''), NULLIF(t.user_id, ''), '')",
        _SLACK_CONTEXT,
    ),
    priority=_SLACK_FILE_PRIORITY,
    refresh_hours=48,
)

_APPLE_MESSAGE = _simple_adapter(
    name="apple_message",
    source_table="apple_messages",
    source="apple_messages",
    kind="message",
    # apple_message_chat_messages' PK leads with chat_id, so a per-row lateral
    # lookup by (account, message_id) has no index; the aggregated hash join
    # costs one small scan per batch instead.
    from_sql="""apple_messages t
    LEFT JOIN apple_message_handles h ON h.account = t.account AND h.handle_id = t.handle_id
    LEFT JOIN (
        SELECT account, message_id, min(chat_id) AS chat_id
        FROM apple_message_chat_messages
        GROUP BY account, message_id
    ) cm ON cm.account = t.account AND cm.message_id = t.message_id
    LEFT JOIN apple_message_chats c ON c.account = t.account AND c.chat_id = cm.chat_id
    LEFT JOIN (
        -- Distinct people, not handle rows: device re-syncs leave duplicate
        -- handle records for the same address in chat rosters.
        SELECT ch.account, ch.chat_id,
               count(DISTINCT COALESCE(NULLIF(rh.address, ''), ch.handle_id)) AS n
        FROM apple_message_chat_handles ch
        LEFT JOIN apple_message_handles rh
            ON rh.account = ch.account AND rh.handle_id = ch.handle_id
        GROUP BY ch.account, ch.chat_id
    ) roster ON roster.account = t.account AND roster.chat_id = cm.chat_id
    LEFT JOIN LATERAL (
        SELECT
            string_agg(
                concat_ws(E'\n', COALESCE(NULLIF(a.filename, ''), NULLIF(a.transfer_name, '')), NULLIF(e.text, '')),
                E'\n' ORDER BY a.attachment_id, e.updated_at DESC
            ) AS attachment_search_text,
            max(GREATEST(a.ingested_at, COALESCE(e.updated_at, a.ingested_at))) AS attachment_ingest_ts
        FROM apple_message_attachments a
        LEFT JOIN file_attachment_enrichments e ON e.content_sha256 = a.content_sha256
        WHERE a.account = t.account AND a.message_id = t.message_id
    ) att ON TRUE
    LEFT JOIN LATERAL (
        SELECT count(*) AS attachment_count,
               string_agg(label, ', ' ORDER BY label) AS attachment_labels
        FROM (
            SELECT DISTINCT COALESCE(
                NULLIF(a.transfer_name, ''), NULLIF(regexp_replace(a.filename, '^.*/', ''), ''),
                NULLIF(a.mime_type, ''), NULLIF(a.content_type, ''), 'attachment'
            ) AS label
            FROM apple_message_attachments a
            WHERE a.account = t.account AND a.message_id = t.message_id AND a.is_missing = 0
        ) labels
    ) att_labels ON TRUE""",
    event_id="concat_ws('|', t.account, t.message_id)",
    event_ts=_real_ts("t.message_at", "t.ingested_at"),
    ingest_ts="GREATEST(t.ingested_at, COALESCE(att.attachment_ingest_ts, t.ingested_at))",
    actor=(
        "CASE WHEN t.is_from_me = 1 THEN 'me' "
        "ELSE COALESCE(NULLIF(h.address, ''), NULLIF(t.handle_id, ''), '') END"
    ),
    title="t.subject",
    snippet=_snippet(
        "CASE "
        "WHEN NULLIF(regexp_replace(t.body_text, '^' || chr(65532) || '+', ''), '') IS NOT NULL "
        "  THEN regexp_replace(t.body_text, '^' || chr(65532) || '+', '') "
        "WHEN COALESCE(att_labels.attachment_count, 0) = 1 "
        "  THEN '[attachment: ' || COALESCE(att_labels.attachment_labels, 'attachment') || ']' "
        "WHEN COALESCE(att_labels.attachment_count, 0) > 1 "
        "  THEN '[' || att_labels.attachment_count::text || ' attachments: ' || left(att_labels.attachment_labels, 120) || ']' "
        "WHEN t.cache_has_attachments <> 0 THEN '[attachment]' "
        "ELSE '' END"
    ),
    context=(
        "COALESCE(NULLIF(c.display_name, ''), NULLIF(c.chat_identifier, ''), "
        "NULLIF(h.address, ''), t.service)"
    ),
    source_pk="jsonb_build_object('account', t.account, 'message_id', t.message_id)",
    metadata=(
        "jsonb_build_object("
        "'service', t.service, "
        "'chat_id', COALESCE(cm.chat_id, ''), "
        "'from_me', t.is_from_me <> 0, "
        "'has_attachments', t.cache_has_attachments <> 0, "
        "'tapback', t.associated_message_type <> 0, "
        "'audio', t.is_audio_message <> 0, "
        "'deleted', t.is_deleted <> 0)"
    ),
    search_text=_search_concat(
        "t.subject",
        "t.body_text",
        "COALESCE(NULLIF(c.display_name, ''), NULLIF(c.chat_identifier, ''), NULLIF(h.address, ''), t.service)",
        "CASE WHEN t.is_from_me = 1 THEN 'me' ELSE COALESCE(NULLIF(h.address, ''), NULLIF(t.handle_id, ''), '') END",
        "att.attachment_search_text",
    ),
    # chat.db style: 45 = 1:1 conversation, 43 = group. The roster counts
    # distinct participant addresses excluding Zach's own handle. People
    # accumulate 2-3 numbers/emails over the years, so <= 9 addresses is what
    # actually covers the family/friend-sized groups the benchmark put at the
    # attention tier (the main family chat counts 9 addresses for ~4 humans).
    # A sender that is neither a phone number nor an email address is a
    # business/RCS token (airlines, delivery bots); 3-6 digit senders and
    # shortcode-named group chats are SMS blasts; +1 toll-free numbers are
    # automated services; a 1:1 chat Zach has never once replied to is a
    # one-way broadcast, not a conversation.
    priority=(
        "CASE "
        "WHEN t.is_from_me = 1 THEN 1 "
        "WHEN t.is_system_message = 1 OR t.is_service_message = 1 OR t.is_spam = 1 THEN 4 "
        "WHEN h.address ~ '^[0-9]{3,6}$' "
        "  OR (h.address <> '' AND h.address NOT LIKE '+%%' AND h.address NOT LIKE '%%@%%') THEN 4 "
        "WHEN c.display_name ~ '^[0-9]{3,6}$' OR c.chat_identifier ~ '^[0-9]{3,6}$' THEN 4 "
        "WHEN h.address ~ '^\\+1(800|833|844|855|866|877|888)' THEN 4 "
        "WHEN c.style = 45 OR COALESCE(roster.n, 0) <= 1 THEN "
        # A conversation needs his participation: two replies ever, or one
        # reply that is not drowned by a 20+ message broadcast stream.
        "  CASE WHEN (SELECT count(*) FROM ("
        "         SELECT 1 FROM apple_message_chat_messages zc "
        "         JOIN apple_messages z ON z.account = zc.account AND z.message_id = zc.message_id "
        "         WHERE zc.account = t.account AND zc.chat_id = cm.chat_id "
        "           AND z.is_from_me = 1 LIMIT 2) ow) >= 2 THEN 2 "
        "       WHEN (SELECT count(*) FROM ("
        "         SELECT 1 FROM apple_message_chat_messages zc "
        "         JOIN apple_messages z ON z.account = zc.account AND z.message_id = zc.message_id "
        "         WHERE zc.account = t.account AND zc.chat_id = cm.chat_id "
        "           AND z.is_from_me = 1 LIMIT 2) ow) = 1 "
        "        AND (SELECT count(*) FROM ("
        "         SELECT 1 FROM apple_message_chat_messages zc "
        "         JOIN apple_messages z ON z.account = zc.account AND z.message_id = zc.message_id "
        "         WHERE zc.account = t.account AND zc.chat_id = cm.chat_id "
        "           AND z.is_from_me = 0 LIMIT 20) iw) < 20 THEN 2 "
        "       ELSE 4 END "
        "WHEN COALESCE(roster.n, 0) <= 9 THEN 2 "
        "WHEN EXISTS (SELECT 1 FROM apple_message_chat_messages zc "
        "             JOIN apple_messages z ON z.account = zc.account AND z.message_id = zc.message_id "
        "             WHERE zc.account = t.account AND zc.chat_id = cm.chat_id "
        "               AND zc.message_date BETWEEN t.message_at - interval '6 hours' "
        "                                       AND t.message_at + interval '6 hours' "
        "               AND z.is_from_me = 1) THEN 2 "
        "ELSE 3 END"
    ),
    refresh_hours=48,
)

_WHATSAPP_MESSAGE_SNIPPET = (
    "CASE WHEN NULLIF(t.body_text, '') IS NOT NULL THEN t.body_text "
    "WHEN COALESCE(NULLIF(t.media_type, ''), NULLIF(t.message_kind, '')) IS NOT NULL "
    "  THEN '[' || COALESCE(NULLIF(t.media_type, ''), NULLIF(t.message_kind, '')) || ' message]' "
    "ELSE '' END"
)

_WHATSAPP_MESSAGE = _simple_adapter(
    name="whatsapp_message",
    source_table="whatsapp_messages",
    source="whatsapp",
    kind="message",
    from_sql="""whatsapp_messages t
    LEFT JOIN whatsapp_chats c ON c.account = t.account AND c.chat_id = t.chat_id
    LEFT JOIN whatsapp_contacts ct ON ct.account = t.account AND ct.jid = t.sender_jid
    LEFT JOIN whatsapp_contacts chat_ct ON chat_ct.account = t.account AND chat_ct.jid = t.chat_id
    LEFT JOIN (
        SELECT account, chat_id, count(*) AS n
        FROM whatsapp_chat_participants
        GROUP BY account, chat_id
    ) roster ON roster.account = t.account AND roster.chat_id = t.chat_id
    LEFT JOIN LATERAL (
        SELECT
            string_agg(
                concat_ws(E'\n', NULLIF(m.filename, ''), NULLIF(e.text, '')),
                E'\n' ORDER BY m.media_type, e.updated_at DESC
            ) AS media_search_text,
            max(GREATEST(m.ingested_at, COALESCE(e.updated_at, m.ingested_at))) AS media_ingest_ts
        FROM whatsapp_media_items m
        LEFT JOIN file_attachment_enrichments e ON e.content_sha256 = m.content_sha256
        WHERE m.account = t.account AND m.chat_id = t.chat_id AND m.message_id = t.message_id
    ) media ON TRUE""",
    event_id="concat_ws('|', t.account, t.chat_id, t.message_id)",
    event_ts=_real_ts("t.message_at", "t.ingested_at"),
    ingest_ts="GREATEST(t.ingested_at, COALESCE(media.media_ingest_ts, t.ingested_at))",
    actor=(
        "CASE WHEN t.is_from_me = 1 THEN 'me' "
        "ELSE COALESCE(NULLIF(ct.full_name, ''), NULLIF(ct.push_name, ''), "
        "NULLIF(t.push_name, ''), NULLIF(t.sender_jid, ''), '') END"
    ),
    snippet=_snippet(_WHATSAPP_MESSAGE_SNIPPET),
    context=(
        "COALESCE(NULLIF(c.name, ''), NULLIF(chat_ct.full_name, ''), "
        "NULLIF(chat_ct.push_name, ''), NULLIF(chat_ct.business_name, ''), t.chat_id)"
    ),
    source_pk=(
        "jsonb_build_object('account', t.account, 'chat_id', t.chat_id, 'message_id', t.message_id)"
    ),
    metadata=(
        "jsonb_build_object("
        "'message_kind', t.message_kind, "
        "'media_type', t.media_type, "
        "'from_me', t.is_from_me <> 0, "
        f"'edited', t.edited_at > {_EPOCH_GUARD}, "
        "'deleted', t.is_deleted <> 0)"
    ),
    search_text=_search_concat(
        "t.body_text",
        "COALESCE(NULLIF(c.name, ''), t.chat_id)",
        "CASE WHEN t.is_from_me = 1 THEN 'me' ELSE COALESCE(NULLIF(ct.full_name, ''), NULLIF(ct.push_name, ''), NULLIF(t.push_name, ''), NULLIF(t.sender_jid, ''), '') END",
        "media.media_search_text",
    ),
    # Group roster counts include me, so <= 5 is a group of at most five.
    # Business accounts (incl. Zach's own WhatsApp-bridged agent) are
    # automated; contentless rows with a group-jid "sender" are E2E/system
    # stubs; big groups are attention only while Zach is actively in the
    # conversation (his own message within +/-6 hours).
    priority=(
        "CASE "
        "WHEN t.is_from_me = 1 THEN 1 "
        "WHEN c.chat_type = 'status' THEN 4 "
        "WHEN EXISTS (SELECT 1 FROM whatsapp_contacts b "
        "             WHERE b.account = t.account AND b.jid = t.sender_jid "
        "               AND b.business_name <> '') THEN 4 "
        "WHEN t.body_text = '' AND COALESCE(t.media_type, '') IN ('', 'none', 'unknown') "
        "  AND (t.sender_jid = t.chat_id OR t.sender_jid = '' "
        "       OR COALESCE(c.chat_type, '') <> 'group' "
        "       OR COALESCE(NULLIF(ct.full_name, ''), NULLIF(ct.push_name, ''), "
        "                   NULLIF(t.push_name, '')) IS NULL) THEN 4 "
        "WHEN c.chat_type = 'group' OR t.chat_id LIKE '%%@g.us' THEN "
        "  CASE WHEN COALESCE(roster.n, 99) <= 5 THEN 2 "
        "       WHEN EXISTS (SELECT 1 FROM whatsapp_messages z "
        "                    WHERE z.account = t.account AND z.chat_id = t.chat_id "
        "                      AND z.is_from_me = 1 "
        "                      AND z.message_at BETWEEN t.message_at - interval '6 hours' "
        "                                           AND t.message_at + interval '6 hours') THEN 2 "
        "       ELSE 3 END "
        "ELSE 2 END"
    ),
    refresh_hours=48,
)

_APPLE_NOTE_REVISION = _simple_adapter(
    name="apple_note_revision",
    source_table="apple_note_revisions",
    source="apple_notes",
    kind="note_edit",
    from_sql="apple_note_revisions t",
    event_id="concat_ws('|', t.account, t.note_id, t.revision_id)",
    event_ts=_real_ts("t.modified_at", "t.created_at", "t.exported_at", "t.ingested_at"),
    ingest_ts="t.ingested_at",
    actor="'me'",
    title="t.title",
    snippet=_snippet("t.body_text"),
    context="t.folder_path",
    source_pk=(
        "jsonb_build_object('account', t.account, 'note_id', t.note_id, 'revision_id', t.revision_id)"
    ),
    metadata="jsonb_build_object('note_id', t.note_id, 'deleted', t.is_deleted <> 0)",
    search_text=_search_concat("t.title", "t.folder_path", "t.body_text"),
    priority=str(TIMELINE_PRIORITY_SELF),
)

_VOICE_MEMO = _simple_adapter(
    name="voice_memo",
    source_table="apple_voice_memos_files",
    source="voice_memos",
    kind="voice_memo",
    from_sql=f"""apple_voice_memos_files t
    LEFT JOIN LATERAL (
        SELECT en.title AS en_title, en.summary AS en_summary
        FROM apple_voice_memos_enrichments en
        WHERE en.account = t.account AND en.recording_id = t.recording_id
          AND (NULLIF(en.title, '') IS NOT NULL OR NULLIF(en.summary, '') IS NOT NULL)
        ORDER BY en.created_at DESC
        LIMIT 1
    ) en ON TRUE
    LEFT JOIN LATERAL (
        SELECT
            string_agg(
                concat_ws(E'\n', NULLIF(en2.title, ''), NULLIF(en2.summary, ''),
                          NULLIF(en2.transcript, ''), NULLIF(en2.participants_json, ''),
                          NULLIF(en2.action_items_json, '')),
                E'\n' ORDER BY en2.created_at DESC
            ) AS enrichment_search_text,
            max(en2.created_at) AS enrichment_ingest_ts
        FROM apple_voice_memos_enrichments en2
        WHERE en2.account = t.account AND en2.recording_id = t.recording_id
    ) ens ON TRUE""",
    event_id="concat_ws('|', t.account, t.recording_id)",
    event_ts=_real_ts("t.recorded_at", "t.file_created_at", "t.ingested_at"),
    ingest_ts="GREATEST(t.ingested_at, COALESCE(ens.enrichment_ingest_ts, t.ingested_at))",
    actor="'me'",
    title="COALESCE(NULLIF(en.en_title, ''), NULLIF(t.title, ''), t.filename)",
    snippet=_snippet("COALESCE(en.en_summary, '')"),
    context="t.account",
    source_pk="jsonb_build_object('account', t.account, 'recording_id', t.recording_id)",
    metadata=(
        "jsonb_build_object("
        "'content_type', t.content_type, "
        "'size_bytes', t.size_bytes, "
        "'deleted', t.is_deleted <> 0)"
    ),
    search_text=_search_concat("t.title", "t.filename", "ens.enrichment_search_text"),
    priority=str(TIMELINE_PRIORITY_SELF),
)

_DATE_ONLY = r"'^\d{4}-\d{2}-\d{2}$'"
_CALENDAR_START_TS = (
    f"COALESCE(NULLIF(t.start_at, {_EPOCH}), "
    f"CASE WHEN t.start_date ~ {_DATE_ONLY} THEN t.start_date::date::timestamptz ELSE NULL END, "
    "t.synced_at)"
)

_CALENDAR_EVENT = _simple_adapter(
    name="calendar_event",
    source_table="calendar_events",
    source="calendar",
    kind="event",
    from_sql="calendar_events t",
    event_id="concat_ws('|', t.account, t.calendar_id, t.event_id)",
    event_ts=_CALENDAR_START_TS,
    end_ts=(
        f"COALESCE(NULLIF(t.end_at, {_EPOCH}), "
        f"CASE WHEN t.end_date ~ {_DATE_ONLY} THEN t.end_date::date::timestamptz ELSE NULL END, "
        f"{_EPOCH})"
    ),
    ingest_ts="t.synced_at",
    actor="t.organizer_email",
    title="t.summary",
    snippet=_snippet("t.description"),
    context="t.calendar_id",
    source_pk=(
        "jsonb_build_object('account', t.account, 'calendar_id', t.calendar_id, 'event_id', t.event_id)"
    ),
    metadata=(
        "jsonb_build_object("
        "'location', t.location, "
        "'status', t.status, "
        "'event_type', t.event_type, "
        "'all_day', t.is_all_day <> 0, "
        "'deleted', t.is_deleted <> 0)"
    ),
    search_text=_search_concat("t.summary", "t.description", "t.location", "t.organizer_email", "t.attendees_json"),
    # Subscribed feeds (yoga studios, holidays) and marketing-mail invites
    # (their descriptions carry the invisible-padding chars marketing HTML
    # uses) are noise; cancelled/deleted and not-yet-started rows are not
    # self activity for past-window reviews; events any of Zach's identities
    # organized are his own actions, except Flighty's auto-created ✈-titled
    # flight events; real invites from humans are attention.
    priority=(
        "CASE "
        "WHEN t.is_deleted <> 0 OR t.status = 'cancelled' THEN 4 "
        f"WHEN ({_CALENDAR_START_TS}) > t.synced_at THEN 3 "
        "WHEN t.organizer_email ILIKE '%%group.calendar.google.com%%' "
        "  OR t.organizer_email ILIKE '%%holiday%%' THEN 4 "
        "WHEN t.description LIKE '%%͏%%' OR t.description LIKE '%%­%%' THEN 4 "
        "WHEN t.organizer_email ILIKE '%%' || t.account || '%%' "
        "  OR EXISTS (SELECT 1 FROM gmail_sync_state self "
        "             WHERE self.account <> '' AND t.organizer_email ILIKE '%%' || self.account || '%%') THEN "
        "  CASE WHEN t.summary LIKE '✈%%' THEN 4 ELSE 1 END "
        "ELSE 2 END"
    ),
)

_DRIVE_FILE = _simple_adapter(
    name="drive_file",
    source_table="google_drive_files",
    source="google_drive",
    kind="file_change",
    from_sql="""google_drive_files t
    LEFT JOIN LATERAL (
        SELECT
            string_agg(ft.text, E'\n' ORDER BY ft.extracted_at DESC) AS extracted_search_text,
            max(ft.extracted_at) AS extracted_ingest_ts
        FROM google_drive_file_texts ft
        WHERE ft.account = t.account AND ft.file_id = t.file_id
          AND ft.text_extraction_status = 'ok' AND ft.text != ''
    ) txt ON TRUE""",
    event_id="concat_ws('|', t.account, t.file_id)",
    event_ts=_real_ts("t.modified_time", "t.created_time", "t.ingested_at"),
    ingest_ts="GREATEST(t.ingested_at, COALESCE(txt.extracted_ingest_ts, t.ingested_at))",
    actor="t.last_modifying_user",
    title="t.name",
    context="t.folder_path",
    source_pk="jsonb_build_object('account', t.account, 'file_id', t.file_id)",
    metadata=(
        "jsonb_build_object("
        "'mime_type', t.mime_type, "
        "'size_bytes', t.size_bytes, "
        "'web_view_link', t.web_view_link, "
        "'shared', t.shared <> 0, "
        "'starred', t.starred <> 0, "
        "'trashed', t.trashed <> 0, "
        "'excluded', t.is_excluded <> 0)"
    ),
    search_text=_search_concat("t.name", "t.folder_path", "t.last_modifying_user", "txt.extracted_search_text"),
    # My own files edited by me are my actions; my files edited by someone
    # else are directed at me; the rest of the corpus is ambient. Ownership
    # matches the account email inside owners_json (the raw metadata's
    # lastModifyingUser.me flag is not stored), and "edited by me" means the
    # last modifier is the owning identity's display name.
    priority=(
        "CASE "
        # Excluded files are the warehouse's own storage folders (attachment
        # blobs and export shards it writes to Drive) — machinery, not
        # activity. Sampling showed thousands of them per window at the noise
        # tier drowning real events.
        "WHEN t.is_excluded <> 0 THEN 5 "
        "WHEN t.trashed <> 0 THEN 4 "
        # Google-Forms response uploads and shared-with-organizers intake
        # folders are pipeline traffic, not someone reaching Zach; shortcut
        # churn likewise reads as ambient.
        "WHEN t.folder_path LIKE '%%(File responses)%%' "
        "  OR t.folder_path ~* 'shared with|shared w/' THEN 3 "
        "WHEN t.mime_type = 'application/vnd.google-apps.shortcut' THEN 3 "
        "WHEN EXISTS (SELECT 1 FROM jsonb_array_elements(t.owners_json) o "
        "             WHERE o->>'emailAddress' ILIKE t.account "
        "               AND (t.last_modifying_user = '' OR t.last_modifying_user = o->>'displayName')) THEN 1 "
        "WHEN EXISTS (SELECT 1 FROM jsonb_array_elements(t.owners_json) o "
        "             WHERE o->>'emailAddress' ILIKE t.account) THEN 2 "
        "WHEN t.starred <> 0 THEN 2 "
        "ELSE 3 END"
    ),
)

_PHOTO = _simple_adapter(
    name="photo",
    source_table="photo_assets",
    source="photos",
    kind="photo",
    # One event per logical photo (photos.assets), not per rendition — the
    # identity layer has already deduplicated cross-source copies. The AI
    # caption is keyed by the thumbnail's (or best file's) content sha in the
    # shared file_attachment_enrichments table and arrives after the asset
    # row, so ingest_ts folds the enrichment timestamp in and refresh_hours
    # re-walks recent events until the caption lands.
    from_sql="""photo_assets t
    LEFT JOIN LATERAL (
        SELECT
            string_agg(e.text, E'\n' ORDER BY e.updated_at DESC) AS enrichment_search_text,
            max(GREATEST(e.updated_at, e.ai_processed_at)) AS enrichment_ingest_ts
        FROM file_attachment_enrichments e
        WHERE e.content_sha256 != ''
          AND e.content_sha256 IN (t.thumbnail_content_sha256, t.best_file_sha256)
          AND e.text != ''
    ) enr ON TRUE""",
    event_id="t.photo_id",
    event_ts=_real_ts("t.capture_ts", "t.created_at"),
    ingest_ts="GREATEST(t.updated_at, COALESCE(enr.enrichment_ingest_ts, t.updated_at))",
    actor="'me'",
    title="COALESCE(NULLIF(t.best_file_filename, ''), t.photo_id)",
    snippet=_snippet("COALESCE(enr.enrichment_search_text, '')"),
    context="t.camera_model",
    source_pk="jsonb_build_object('photo_id', t.photo_id)",
    metadata=(
        "jsonb_build_object("
        "'account', t.account, "
        "'kind', t.kind, "
        "'lat', t.latitude, "
        "'lon', t.longitude, "
        "'camera_make', t.camera_make, "
        "'camera_model', t.camera_model, "
        "'width', t.width, "
        "'height', t.height, "
        "'mime_type', t.best_file_mime_type, "
        "'thumbnail_file_id', t.thumbnail_storage_file_id)"
    ),
    search_text=_search_concat(
        "t.best_file_filename",
        "t.camera_make",
        "t.camera_model",
        "enr.enrichment_search_text",
    ),
    # Photos Zach took are his own actions.
    priority=str(TIMELINE_PRIORITY_SELF),
    refresh_hours=48,
)

_CONTACT_UPDATE = _simple_adapter(
    name="contact_update",
    source_table="contact_cards",
    source="contacts",
    kind="contact_update",
    from_sql="contact_cards t",
    event_id="concat_ws('|', t.source, t.account, t.source_kind, t.address_book_id, t.card_id)",
    event_ts=_real_ts("t.source_updated_at", "t.synced_at"),
    ingest_ts="t.synced_at",
    title=(
        "COALESCE(NULLIF(t.display_name, ''), NULLIF(t.organization, ''), "
        "NULLIF(t.primary_email, ''), t.card_id)"
    ),
    context="t.account",
    source_pk=(
        "jsonb_build_object('source', t.source, 'account', t.account, 'source_kind', t.source_kind, "
        "'address_book_id', t.address_book_id, 'card_id', t.card_id)"
    ),
    metadata=(
        "jsonb_build_object("
        "'organization', t.organization, "
        "'job_title', t.job_title, "
        "'primary_email', t.primary_email, "
        "'primary_phone', t.primary_phone, "
        "'deleted', t.is_deleted <> 0)"
    ),
    search_text=_search_concat(
        "t.display_name", "t.organization", "t.job_title", "t.primary_email", "t.primary_phone",
        "t.notes", "t.emails", "t.phones", "t.addresses", "t.urls", "t.nicknames"
    ),
    # Contact-card churn is sync machinery, not traffic aimed at Zach.
    priority=str(TIMELINE_PRIORITY_BACKGROUND),
)

_WHOOP_CYCLE = _simple_adapter(
    name="whoop_cycle",
    source_table="whoop_cycles",
    source="whoop",
    kind="health_cycle",
    from_sql="whoop_cycles t",
    event_id="concat_ws('|', t.account, t.cycle_id)",
    event_ts=_real_ts("t.start_at", "t.created_at", "t.synced_at"),
    end_ts="t.end_at",
    ingest_ts="t.synced_at",
    actor="'me'",
    title="'WHOOP cycle'",
    snippet="concat('Strain ', t.strain::text, ', average HR ', t.average_heart_rate::text)",
    context="t.account",
    source_pk="jsonb_build_object('account', t.account, 'cycle_id', t.cycle_id)",
    metadata=(
        "jsonb_build_object("
        "'score_state', t.score_state, "
        "'strain', t.strain, "
        "'kilojoule', t.kilojoule, "
        "'average_heart_rate', t.average_heart_rate, "
        "'max_heart_rate', t.max_heart_rate)"
    ),
    search_text=_search_concat("t.score_state", "t.strain", "t.average_heart_rate", "t.max_heart_rate"),
    priority=str(TIMELINE_PRIORITY_SELF),
)

_WHOOP_RECOVERY = _simple_adapter(
    name="whoop_recovery",
    source_table="whoop_recoveries",
    source="whoop",
    kind="recovery",
    from_sql=(
        "whoop_recoveries t LEFT JOIN whoop_cycles c "
        "ON c.account = t.account AND c.cycle_id = t.cycle_id"
    ),
    event_id="concat_ws('|', t.account, t.cycle_id)",
    event_ts=_real_ts("c.start_at", "t.updated_at", "t.created_at", "t.synced_at"),
    end_ts="c.end_at",
    ingest_ts="t.synced_at",
    actor="'me'",
    title="'WHOOP recovery'",
    snippet=(
        "concat('Recovery ', t.recovery_score::text, '%%, RHR ', "
        "t.resting_heart_rate::text, ', HRV ', t.hrv_rmssd_milli::text)"
    ),
    context="t.account",
    source_pk="jsonb_build_object('account', t.account, 'cycle_id', t.cycle_id)",
    metadata=(
        "jsonb_build_object("
        "'sleep_id', t.sleep_id, "
        "'score_state', t.score_state, "
        "'recovery_score', t.recovery_score, "
        "'resting_heart_rate', t.resting_heart_rate, "
        "'hrv_rmssd_milli', t.hrv_rmssd_milli, "
        "'spo2_percentage', t.spo2_percentage, "
        "'skin_temp_celsius', t.skin_temp_celsius)"
    ),
    search_text=_search_concat("t.score_state", "t.recovery_score", "t.resting_heart_rate", "t.hrv_rmssd_milli"),
    priority=str(TIMELINE_PRIORITY_SELF),
)

_WHOOP_SLEEP = _simple_adapter(
    name="whoop_sleep",
    source_table="whoop_sleeps",
    source="whoop",
    kind="sleep",
    from_sql="whoop_sleeps t",
    event_id="concat_ws('|', t.account, t.sleep_id)",
    event_ts=_real_ts("t.start_at", "t.created_at", "t.synced_at"),
    end_ts="t.end_at",
    ingest_ts="t.synced_at",
    actor="'me'",
    title="CASE WHEN t.nap <> 0 THEN 'WHOOP nap' ELSE 'WHOOP sleep' END",
    snippet=(
        "concat('Performance ', t.sleep_performance_percentage::text, '%%, efficiency ', "
        "t.sleep_efficiency_percentage::text, '%%, respiratory rate ', t.respiratory_rate::text)"
    ),
    context="t.account",
    source_pk="jsonb_build_object('account', t.account, 'sleep_id', t.sleep_id)",
    metadata=(
        "jsonb_build_object("
        "'cycle_id', t.cycle_id, "
        "'nap', t.nap <> 0, "
        "'score_state', t.score_state, "
        "'sleep_performance_percentage', t.sleep_performance_percentage, "
        "'sleep_efficiency_percentage', t.sleep_efficiency_percentage, "
        "'respiratory_rate', t.respiratory_rate)"
    ),
    search_text=_search_concat("t.score_state", "t.sleep_performance_percentage", "t.respiratory_rate"),
    priority=str(TIMELINE_PRIORITY_SELF),
)

_WHOOP_WORKOUT = _simple_adapter(
    name="whoop_workout",
    source_table="whoop_workouts",
    source="whoop",
    kind="workout",
    from_sql="whoop_workouts t",
    event_id="concat_ws('|', t.account, t.workout_id)",
    event_ts=_real_ts("t.start_at", "t.created_at", "t.synced_at"),
    end_ts="t.end_at",
    ingest_ts="t.synced_at",
    actor="'me'",
    title="concat('WHOOP workout: ', COALESCE(NULLIF(t.sport_name, ''), 'activity'))",
    snippet=(
        "concat('Strain ', t.strain::text, ', average HR ', t.average_heart_rate::text, "
        "', distance ', t.distance_meter::text, ' m')"
    ),
    context="t.account",
    source_pk="jsonb_build_object('account', t.account, 'workout_id', t.workout_id)",
    metadata=(
        "jsonb_build_object("
        "'sport_name', t.sport_name, "
        "'sport_id', t.sport_id, "
        "'score_state', t.score_state, "
        "'strain', t.strain, "
        "'average_heart_rate', t.average_heart_rate, "
        "'max_heart_rate', t.max_heart_rate, "
        "'distance_meter', t.distance_meter)"
    ),
    search_text=_search_concat("t.sport_name", "t.score_state", "t.strain", "t.average_heart_rate"),
    priority=str(TIMELINE_PRIORITY_SELF),
)

_MUTATION = _simple_adapter(
    name="mutation",
    source_table="upstream_mutations",
    source="mutations",
    kind="mutation",
    from_sql="upstream_mutations t",
    event_id="t.id",
    event_ts=f"COALESCE(NULLIF(t.executed_at, {_EPOCH}), t.created_at)",
    ingest_ts="t.updated_at",
    actor="COALESCE(NULLIF(t.requested_by, ''), 'warehouse')",
    title="t.title",
    snippet=_snippet("t.reason"),
    context="concat_ws('.', NULLIF(t.provider, ''), NULLIF(t.operation, ''))",
    source_pk="jsonb_build_object('id', t.id)",
    metadata=(
        "jsonb_build_object("
        "'status', t.status, "
        "'provider', t.provider, "
        "'operation', t.operation, "
        "'account', t.account, "
        "'request_id', t.request_id, "
        "'has_error', t.error <> '')"
    ),
    priority=str(TIMELINE_PRIORITY_BACKGROUND),
)

_MUTATION_REQUEST = _simple_adapter(
    name="mutation_request",
    source_table="upstream_mutation_requests",
    source="mutations",
    kind="mutation_request",
    from_sql="upstream_mutation_requests t",
    event_id="t.id",
    event_ts="t.created_at",
    ingest_ts="t.updated_at",
    actor="COALESCE(NULLIF(t.requested_by, ''), 'warehouse')",
    title="t.title",
    snippet=_snippet("t.reason"),
    source_pk="jsonb_build_object('id', t.id)",
    metadata=("jsonb_build_object('status', t.status, 'has_error', t.error <> '')"),
    priority=str(TIMELINE_PRIORITY_BACKGROUND),
)

_ENRICHMENT_RUN = _simple_adapter(
    name="enrichment_run",
    source_table="agent_runs",
    source="warehouse",
    kind="enrichment_run",
    from_sql="agent_runs t",
    event_id="t.run_id",
    event_ts=_real_ts("t.started_at", "t.completed_at"),
    end_ts="t.completed_at",
    ingest_ts="GREATEST(t.started_at, t.completed_at)",
    actor="COALESCE(NULLIF(t.provider, ''), 'agent')",
    title="t.task_type",
    snippet=_snippet("t.error"),
    context="t.model",
    source_pk="jsonb_build_object('run_id', t.run_id)",
    metadata=(
        "jsonb_build_object("
        "'status', t.status, "
        "'subject_id', t.subject_id, "
        "'prompt_version', t.prompt_version, "
        "'exit_code', t.exit_code)"
    ),
    priority=str(TIMELINE_PRIORITY_BACKGROUND),
)


def _agent_session_adapter() -> TimelineAdapter:
    """Session-level roll-up over marts.ai_conversation_events.

    One timeline row per session/conversation (Claude Code, Codex, OpenClaw,
    Claude Desktop, ChatGPT — the row's ``source`` is the per-session source
    value), matching the marts.ai_conversation_sessions roll-up. Individual transcript
    lines are surfaced through the session's detail view, not as separate
    timeline entries.

    The GROUP BY aggregates only cheap scalars; first/last text-ish fields
    (title, first prompt, model, cwd, ...) and the BM25 search document come
    from per-session LATERAL probes on the (source, session_id, seq) index so
    the normalized timeline row can be the primary search hit.
    """
    rollup = f"""
        SELECT
            concat_ws('|', s.source, s.session_id) AS event_id,
            s.source AS source,
            'agent_session' AS kind,
            s.event_ts AS event_ts,
            s.end_ts AS end_ts,
            COALESCE(s.device, '') AS actor,
            COALESCE(NULLIF(st.session_title, ''),
                     left(fp.text, {TIMELINE_TITLE_CHARS}), '') AS title,
            COALESCE(left(fp.text, {TIMELINE_SNIPPET_CHARS}), '') AS snippet,
            COALESCE(NULLIF(cw.cwd, ''), s.account, '') AS context,
            (jsonb_build_object('source', s.source, 'session_id', s.session_id))::text AS source_pk,
            (jsonb_build_object(
                'events', s.event_count,
                'user_events', s.user_event_count,
                'assistant_events', s.assistant_event_count,
                'entrypoint', s.entrypoint,
                'model', md.model,
                'device', s.device,
                'account', s.account,
                'git_branch', gb.git_branch,
                'repo_url', ru.repo_url,
                'output_tokens', s.output_tokens
            ))::text AS metadata,
            concat_ws(E'\n', NULLIF(st.session_title, ''), NULLIF(fp.text, ''),
                      NULLIF(cw.cwd, ''), NULLIF(gb.git_branch, ''), NULLIF(ru.repo_url, ''),
                      NULLIF(tx.transcript_text, '')) AS search_text,
            s.ingest_ts AS ingest_ts,
            -- Interactive vs background (benchmark-tuned, sampling/ 2026-07).
            -- chatgpt/claude_desktop are always human conversations, and some
            -- sync as a header row with zero user events. Cron/inter-session
            -- prompts, programmatic entrypoints, zero-user-turn transcripts,
            -- and sidechain-only subagent transcripts are machinery.
            CASE WHEN s.source IN ('chatgpt', 'claude_desktop') THEN {TIMELINE_PRIORITY_SELF}
                 WHEN COALESCE(NULLIF(st.session_title, ''), fp.text, '') LIKE '[cron:%%'
                   OR COALESCE(fp.text, '') LIKE '[cron:%%'
                   OR COALESCE(fp.text, '') LIKE '[Inter-session message]%%'
                   OR COALESCE(NULLIF(st.session_title, ''), fp.text, '') LIKE '[Subagent Context]%%'
                   OR COALESCE(fp.text, '') LIKE '[Subagent Context]%%'
                   THEN {TIMELINE_PRIORITY_BACKGROUND}
                 WHEN s.entrypoint IN ('sdk-cli', 'codex_exec', 'zrl-claw')
                   THEN {TIMELINE_PRIORITY_BACKGROUND}
                 WHEN s.user_event_count = 0 THEN {TIMELINE_PRIORITY_BACKGROUND}
                 WHEN s.non_sidechain_count = 0 THEN {TIMELINE_PRIORITY_BACKGROUND}
                 -- The same long opening prompt recurring across sessions is a
                 -- scheduled routine (daily monitor runs), not a human typing.
                 WHEN length(COALESCE(fp.text, '')) > 40 AND (
                      SELECT count(*) FROM (
                          SELECT 1 FROM agent_session_events rep
                          WHERE rep.role = 'user' AND rep.seq <= 5
                            AND left(rep.text, 64) = left(fp.text, 64)
                          LIMIT 4) reps) >= 4
                   THEN {TIMELINE_PRIORITY_BACKGROUND}
                 ELSE {TIMELINE_PRIORITY_SELF} END AS priority
        FROM (
            SELECT
                e.source,
                e.session_id,
                COALESCE(min(e.occurred_at) FILTER (WHERE e.occurred_at > {_EPOCH_GUARD}),
                         max(e.ingested_at)) AS event_ts,
                COALESCE(max(e.occurred_at) FILTER (WHERE e.occurred_at > {_EPOCH_GUARD}),
                         {_EPOCH}) AS end_ts,
                max(NULLIF(e.device, '')) AS device,
                max(NULLIF(e.account, '')) AS account,
                COALESCE(min(NULLIF(e.entrypoint, '')), '') AS entrypoint,
                count(*) AS event_count,
                count(*) FILTER (WHERE e.role = 'user') AS user_event_count,
                count(*) FILTER (WHERE e.role = 'assistant') AS assistant_event_count,
                count(*) FILTER (WHERE e.is_sidechain = 0) AS non_sidechain_count,
                sum(e.output_tokens) AS output_tokens,
                max(e.ingested_at) AS ingest_ts
            FROM agent_session_events e
            {{changed_join}}
            GROUP BY e.source, e.session_id
        ) s
        LEFT JOIN LATERAL (
            SELECT e2.session_title FROM agent_session_events e2
            WHERE e2.source = s.source AND e2.session_id = s.session_id AND e2.session_title != ''
            ORDER BY e2.seq LIMIT 1
        ) st ON TRUE
        LEFT JOIN LATERAL (
            SELECT e2.text FROM agent_session_events e2
            WHERE e2.source = s.source AND e2.session_id = s.session_id
              AND e2.role = 'user' AND e2.text != ''
            ORDER BY e2.seq LIMIT 1
        ) fp ON TRUE
        LEFT JOIN LATERAL (
            SELECT e2.model FROM agent_session_events e2
            WHERE e2.source = s.source AND e2.session_id = s.session_id AND e2.model != ''
            ORDER BY e2.seq DESC LIMIT 1
        ) md ON TRUE
        LEFT JOIN LATERAL (
            SELECT e2.cwd FROM agent_session_events e2
            WHERE e2.source = s.source AND e2.session_id = s.session_id AND e2.cwd != ''
            ORDER BY e2.seq LIMIT 1
        ) cw ON TRUE
        LEFT JOIN LATERAL (
            SELECT e2.git_branch FROM agent_session_events e2
            WHERE e2.source = s.source AND e2.session_id = s.session_id AND e2.git_branch != ''
            ORDER BY e2.seq DESC LIMIT 1
        ) gb ON TRUE
        LEFT JOIN LATERAL (
            SELECT e2.repo_url FROM agent_session_events e2
            WHERE e2.source = s.source AND e2.session_id = s.session_id AND e2.repo_url != ''
            ORDER BY e2.seq LIMIT 1
        ) ru ON TRUE
        LEFT JOIN LATERAL (
            SELECT string_agg(e2.text, E'\n' ORDER BY e2.seq) AS transcript_text
            FROM agent_session_events e2
            WHERE e2.source = s.source AND e2.session_id = s.session_id
              AND e2.text != '' AND e2.role IN ('user', 'assistant')
        ) tx ON TRUE
    """
    backfill_sql = f"""
        SELECT * FROM ({rollup.format(changed_join="")}) roll
        WHERE roll.event_ts <= %(cursor_ts)s
          AND (roll.event_ts, roll.event_id) < (%(cursor_ts)s, %(cursor_id)s)
        ORDER BY roll.event_ts DESC, roll.event_id DESC
        LIMIT %(limit)s
    """
    changed_join = """
        JOIN (
            SELECT DISTINCT source, session_id
            FROM agent_session_events
            WHERE ingested_at >= %(watermark_ts)s
        ) changed ON changed.source = e.source AND changed.session_id = e.session_id
    """
    incremental_sql = f"""
        SELECT * FROM ({rollup.format(changed_join=changed_join)}) roll
        WHERE (roll.ingest_ts, roll.event_id) > (%(watermark_ts)s, %(watermark_id)s)
        ORDER BY roll.ingest_ts ASC, roll.event_id ASC
        LIMIT %(limit)s
    """
    return TimelineAdapter(
        name="agent_session",
        source_table="agent_session_events",
        source="agent_sessions",
        kind="agent_session",
        backfill_sql=backfill_sql,
        incremental_sql=incremental_sql,
        max_ingest_sql="SELECT max(ingested_at) FROM agent_session_events",
        batch_size=10000,
    )


_AGENT_SESSION = _agent_session_adapter()

TIMELINE_ADAPTERS: tuple[TimelineAdapter, ...] = (
    _GMAIL_EMAIL,
    _SLACK_MESSAGE,
    _SLACK_FILE,
    _APPLE_MESSAGE,
    _WHATSAPP_MESSAGE,
    _AGENT_SESSION,
    _APPLE_NOTE_REVISION,
    _VOICE_MEMO,
    _CALENDAR_EVENT,
    _DRIVE_FILE,
    _PHOTO,
    _CONTACT_UPDATE,
    _WHOOP_CYCLE,
    _WHOOP_RECOVERY,
    _WHOOP_SLEEP,
    _WHOOP_WORKOUT,
    _MUTATION,
    _MUTATION_REQUEST,
    _ENRICHMENT_RUN,
)


def adapter_by_name(name: str) -> TimelineAdapter:
    for adapter in TIMELINE_ADAPTERS:
        if adapter.name == name:
            return adapter
    raise KeyError(name)


@dataclass(frozen=True)
class TableCoverage:
    """How one warehouse table is represented in the unified timeline.

    role:
      - ``events``: the table feeds one or more timeline adapters directly.
      - ``detail``: rows are surfaced through the detail view of their parent
        table's timeline events (attachments, reactions, transcript lines, ...).
      - ``entity``: a dimension joined into events for display (users, chats).
      - ``state``: internal machinery (sync cursors, credentials, the timeline
        tables themselves) that is not user activity.
    """

    role: str
    parent: str = ""
    note: str = ""


def _events(note: str = "") -> TableCoverage:
    return TableCoverage(role="events", note=note)


def _detail(parent: str, note: str = "") -> TableCoverage:
    return TableCoverage(role="detail", parent=parent, note=note)


def _entity(note: str = "") -> TableCoverage:
    return TableCoverage(role="entity", note=note)


def _state(note: str = "") -> TableCoverage:
    return TableCoverage(role="state", note=note)


# Every warehouse table (POSTGRES_TABLES plus the raw-DDL control-plane
# tables) must appear here; tests/test_timeline.py fails the suite otherwise.
TIMELINE_TABLE_COVERAGE: dict[str, TableCoverage] = {
    # Gmail
    "gmail_messages": _events(),
    "gmail_attachments": _detail("gmail_messages", "listed in the email's detail view"),
    "gmail_sync_state": _state("gmail sync cursor"),
    "gmail_attachment_backfill_state": _state("attachment backfill progress"),
    "file_attachment_enrichments": _detail(
        "gmail_attachments",
        "AI-extracted text keyed by content sha; surfaced with gmail/whatsapp/imessage attachments",
    ),
    # Calendar
    "calendar_events": _events(),
    "calendar_sync_state": _state("calendar sync cursor"),
    # Contacts
    "contact_cards": _events(),
    "contact_sync_state": _state("contacts sync cursor"),
    # Voice memos
    "apple_voice_memos_files": _events(),
    "apple_voice_memos_transcription_runs": _detail("apple_voice_memos_files"),
    "apple_voice_memos_transcript_segments": _detail("apple_voice_memos_files"),
    "apple_voice_memos_enrichments": _detail("apple_voice_memos_files"),
    # Apple Notes: every note has revision rows (the note row is the current
    # state; the revisions are the edit activity).
    "apple_notes": _entity("current note state; edits surface via apple_note_revisions"),
    "apple_note_revisions": _events(),
    "apple_note_attachments": _detail("apple_note_revisions"),
    # Apple Messages
    "apple_messages": _events(),
    "apple_message_handles": _entity("sender dimension joined into message events"),
    "apple_message_chats": _entity("chat dimension joined into message events"),
    "apple_message_chat_handles": _entity("chat membership"),
    "apple_message_chat_messages": _detail("apple_messages", "chat<->message join rows"),
    "apple_message_attachments": _detail("apple_messages"),
    # Photos (per-source raw tables unified by the photos.* identity layer;
    # one timeline event per logical photo)
    "photo_assets": _events("one event per deduplicated logical photo"),
    "apple_photos_files": _detail("photo_assets", "raw Apple Photos renditions in the photo's detail view"),
    "photo_asset_files": _detail("photo_assets", "identity links + dedup audit (match_method/match_score)"),
    "media_fingerprints": _state("perceptual-hash cache keyed by content sha"),
    # WhatsApp
    "whatsapp_messages": _events(),
    "whatsapp_chats": _entity("chat dimension joined into message events"),
    "whatsapp_chat_participants": _entity("group rosters"),
    "whatsapp_contacts": _entity("sender dimension joined into message events"),
    "whatsapp_media_items": _detail("whatsapp_messages"),
    "whatsapp_client_sessions": _state("linked-device session snapshot"),
    # AI conversations (source-owned raw tables, unified through marts.ai_conversation_events)
    "agent_session_events": _state("legacy mixed table name; migrated into source-owned AI event tables"),
    "chatgpt_events": _events("rolled up to one timeline row per conversation"),
    "claude_desktop_events": _events("rolled up to one timeline row per conversation"),
    "claude_code_events": _events("rolled up to one timeline row per session"),
    "codex_events": _events("rolled up to one timeline row per session"),
    "openclaw_events": _events("rolled up to one timeline row per session"),
    "pi_events": _events("rolled up to one timeline row per session"),
    "chatgpt_sessions": _state("chatgpt.com web-session credential"),
    "chatgpt_conversation_sync": _state("per-conversation poll watermark"),
    "claude_desktop_credentials": _state("claude.ai session credential"),
    "claude_desktop_conversation_state": _state("per-conversation poll cursor"),
    # Warehouse-internal enrichment agent
    "agent_runs": _events("the warehouse's own enrichment agent activity"),
    "agent_run_events": _detail("agent_runs", "raw agent stdout/stderr stream"),
    "agent_run_tool_calls": _detail("agent_runs"),
    # Slack
    "slack_messages": _events(),
    "slack_files": _events("file shares; may exist without a synced message"),
    "slack_message_reactions": _detail("slack_messages"),
    "slack_teams": _entity(),
    "slack_account_identities": _entity("which user_id is Zach per team"),
    "slack_users": _entity("author dimension joined into message events"),
    "slack_conversations": _entity("channel dimension joined into message events"),
    "slack_conversation_members": _entity("membership rosters"),
    "slack_conversation_stats": _state("derived per-conversation counters"),
    "slack_sync_state": _state("per-object sync cursors"),
    "slack_account_state_item_rows": _state("remote inbox snapshot (derived, churn-heavy)"),
    # Google Drive
    "google_drive_files": _events(),
    "google_drive_file_texts": _detail("google_drive_files"),
    "google_drive_sync_state": _state("drive sync cursor"),
    # WHOOP
    "whoop_profiles": _entity("current WHOOP user profile"),
    "whoop_body_measurements": _entity("current WHOOP body measurements"),
    "whoop_cycles": _events(),
    "whoop_recoveries": _events(),
    "whoop_sleeps": _events(),
    "whoop_workouts": _events(),
    "whoop_sync_state": _state("per-collection WHOOP scan watermark"),
    "whoop_oauth_tokens": _state("rotating WHOOP OAuth credential"),
    # Plaid finance data is queryable through plaid.* and marts.finance_* but
    # deliberately excluded from the general communications/activity timeline.
    "plaid_items": _entity("institution dimension for Plaid finance queries"),
    "plaid_accounts": _entity("account and current balance state"),
    "plaid_transactions": _entity("finance query surface; excluded from the general timeline"),
    "plaid_investment_securities": _entity("security dimension"),
    "plaid_investment_holdings": _entity("current investment holding state"),
    "plaid_investment_transactions": _entity("finance query surface; excluded from the general timeline"),
    "plaid_liabilities": _entity("current liability state"),
    "plaid_sync_state": _state("per-item/product sync cursor"),
    "plaid_item_tokens": _state("private Plaid access tokens"),
    # Finance ledger (derived stocks-and-flows layer over the finance
    # sources): queryable through finance.* and marts.finance_*, excluded
    # from the general activity timeline like the raw plaid tables.
    "finance_accounts": _entity("logical account/asset/liability dimension"),
    "finance_account_links": _state("source-account → ledger-account resolution audit"),
    "finance_observations": _entity("append-only balance/valuation history; excluded from the general timeline"),
    # Manually uploaded finance documents: finance query surface, not
    # activity (the upload moment is not a life event).
    "manual_finance_documents": _entity("uploaded financial document inventory"),
    "manual_finance_extractions": _entity("structured agent extraction per document"),
    # Upstream mutations (the warehouse acting on the world)
    "upstream_mutations": _events(),
    "upstream_mutation_requests": _events(),
    "upstream_mutation_events": _detail("upstream_mutations"),
    "upstream_mutation_request_events": _detail("upstream_mutation_requests"),
    # Search surfaces
    "search_schema_state": _state("search_text DDL signature cache"),
    # The timeline itself
    "timeline_events": _state("the unified timeline"),
    "timeline_sync_state": _state("per-adapter sync cursors"),
    "timeline_gmail_correspondents": _state(
        "addresses Zach has written to; feeds the gmail known-correspondent rule"
    ),
}

# Raw-DDL tables created outside POSTGRES_TABLES; kept in sync by the live
# schema test, which enumerates information_schema after running every
# ensure_* method.
RAW_DDL_TABLES: tuple[str, ...] = (
    "timeline_gmail_correspondents",
    "claude_desktop_credentials",
    "claude_desktop_conversation_state",
    "whatsapp_client_sessions",
    "chatgpt_sessions",
    "chatgpt_conversation_sync",
    "upstream_mutation_requests",
    "upstream_mutations",
    "upstream_mutation_events",
    "upstream_mutation_request_events",
    "search_schema_state",
)


_TIMELINE_UPSERT_COLUMNS = (
    "adapter",
    "event_id",
    "source",
    "kind",
    "priority",
    "event_ts",
    "end_ts",
    "actor",
    "title",
    "snippet",
    "context",
    "source_table",
    "source_pk",
    "metadata",
    "search_text",
    "ingest_ts",
)

# Content columns participating in the change guard: a re-sync that only
# bumps the source's ingestion timestamp must NOT bump seq, or arrival-order
# consumers would see every re-synced row as new. priority IS content: a
# reclassification should surface to arrival-order consumers.
_TIMELINE_CONTENT_COLUMNS = (
    "source",
    "kind",
    "priority",
    "event_ts",
    "end_ts",
    "actor",
    "title",
    "snippet",
    "context",
    "source_table",
    "source_pk",
    "metadata",
    "search_text",
)


def timeline_upsert_sql(*, table_ref: str = "timeline_events", sequence_ref: str = "timeline_events_seq") -> str:
    assignments = ", ".join(f"{col} = EXCLUDED.{col}" for col in _TIMELINE_UPSERT_COLUMNS[2:])
    current = ", ".join(f"target.{col}" for col in _TIMELINE_CONTENT_COLUMNS)
    incoming = ", ".join(f"EXCLUDED.{col}" for col in _TIMELINE_CONTENT_COLUMNS)
    return f"""
        INSERT INTO {table_ref} AS target ({", ".join(_TIMELINE_UPSERT_COLUMNS)})
        VALUES %s
        ON CONFLICT (adapter, event_id) DO UPDATE SET
            {assignments},
            seq = nextval('{sequence_ref}'),
            updated_at = now()
        WHERE ({current}) IS DISTINCT FROM ({incoming})
    """


_TIMELINE_INSERT_TEMPLATE = (
    "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s)"
)


@dataclass
class AdapterSyncStats:
    adapter: str
    backfill_rows: int = 0
    incremental_rows: int = 0
    refreshed_rows: int = 0
    backfill_done: bool = False
    error: str = ""


@dataclass
class _AdapterState:
    backfill_cursor_ts: datetime
    backfill_cursor_id: str
    backfill_done: bool
    watermark_ts: datetime
    watermark_id: str


class TimelineSyncEngine:
    """Pumps normalized rows from the source warehouse into timeline_events.

    ``source_url`` is only ever read (the session is forced read-only);
    ``dest_url`` receives the timeline tables. In production both point at the
    same database. Work is budgeted: incremental sync runs first for every
    adapter (keeping the timeline fresh), then backfill batches round-robin
    across adapters that still have history to load, so one giant source
    cannot starve the others.
    """

    def __init__(
        self,
        *,
        source_url: str,
        dest_url: str | None = None,
        source_schema: str = "public",
        dest_schema: str = "public",
        adapters: Sequence[TimelineAdapter] = TIMELINE_ADAPTERS,
        batch_size: int | None = None,
    ) -> None:
        self._source_url = normalize_postgres_url(source_url) or ""
        if not self._source_url:
            raise ValueError("source_url must be set")
        self._dest_url = normalize_postgres_url(dest_url) or self._source_url
        self._source_schema = source_schema
        self._dest_schema = dest_schema
        self._adapters = tuple(adapters)
        self._batch_size = batch_size
        self._source_conn: Any = None
        self._dest_conn: Any = None

    # -- connections ---------------------------------------------------------

    def _search_path_sql(self, namespace: str) -> str:
        parts = ['"' + schema.replace('"', '""') + '"' for schema in physical_schema_names(namespace=namespace)]
        parts.append("public")
        return "SET search_path TO " + ", ".join(parts)

    def _source_sql(self, sql: str) -> str:
        return qualify_sql_relations(sql, namespace=self._source_schema)

    def _dest_sql(self, sql: str) -> str:
        return qualify_sql_relations(sql, namespace=self._dest_schema)

    def _qualified_regclass(self, logical_name: str, *, namespace: str) -> str:
        return qualify_sql_relations(logical_name, namespace=namespace)

    def _connect(self) -> None:
        if self._source_conn is None:
            self._source_conn = psycopg2.connect(self._source_url)
            self._source_conn.autocommit = True
            with self._source_conn.cursor() as cursor:
                cursor.execute("SET default_transaction_read_only = on")
                cursor.execute(self._search_path_sql(self._source_schema))
        if self._dest_conn is None:
            # Import here to avoid a module cycle (postgres.py is the DDL layer).
            from personal_data_warehouse.postgres import PostgresWarehouse

            warehouse = PostgresWarehouse(self._dest_url, schema=self._dest_schema)
            warehouse.ensure_timeline_tables()
            warehouse.close()
            self._dest_conn = psycopg2.connect(self._dest_url)
            self._dest_conn.autocommit = True
            with self._dest_conn.cursor() as cursor:
                cursor.execute(self._search_path_sql(self._dest_schema))

    def close(self) -> None:
        for conn in (self._source_conn, self._dest_conn):
            if conn is not None:
                conn.close()
        self._source_conn = None
        self._dest_conn = None

    # -- state ---------------------------------------------------------------

    def _load_state(self, adapter: TimelineAdapter) -> _AdapterState:
        with self._dest_conn.cursor() as cursor:
            cursor.execute(
                self._dest_sql(
                    """
                    SELECT backfill_cursor_event_ts, backfill_cursor_event_id, backfill_done,
                           watermark_ingest_ts, watermark_event_id
                    FROM timeline_sync_state
                    WHERE adapter = %s
                    """
                ),
                (adapter.name,),
            )
            row = cursor.fetchone()
        if row is not None:
            return _AdapterState(
                backfill_cursor_ts=row[0],
                backfill_cursor_id=row[1],
                backfill_done=bool(row[2]),
                watermark_ts=row[3],
                watermark_id=row[4],
            )
        # First contact: start the incremental watermark at the source's
        # current ingestion high-water so incremental only tails NEW rows,
        # and let the backfill (newest-first) load everything already there.
        with self._source_conn.cursor() as cursor:
            cursor.execute(self._source_sql(adapter.max_ingest_sql))
            max_ingest = cursor.fetchone()[0]
        state = _AdapterState(
            backfill_cursor_ts=BACKFILL_CURSOR_START,
            backfill_cursor_id="",
            backfill_done=False,
            watermark_ts=max_ingest or datetime(1970, 1, 1, tzinfo=UTC),
            watermark_id="",
        )
        self._save_state(adapter, state)
        return state

    def _save_state(self, adapter: TimelineAdapter, state: _AdapterState, error: str = "") -> None:
        with self._dest_conn.cursor() as cursor:
            cursor.execute(
                self._dest_sql(
                    """
                    INSERT INTO timeline_sync_state (
                        adapter, backfill_cursor_event_ts, backfill_cursor_event_id, backfill_done,
                        watermark_ingest_ts, watermark_event_id, last_run_at, last_error, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, now(), %s, now())
                    ON CONFLICT (adapter) DO UPDATE SET
                        backfill_cursor_event_ts = EXCLUDED.backfill_cursor_event_ts,
                        backfill_cursor_event_id = EXCLUDED.backfill_cursor_event_id,
                        backfill_done = EXCLUDED.backfill_done,
                        watermark_ingest_ts = EXCLUDED.watermark_ingest_ts,
                        watermark_event_id = EXCLUDED.watermark_event_id,
                        last_run_at = now(),
                        last_error = EXCLUDED.last_error,
                        updated_at = now()
                    """
                ),
                (
                    adapter.name,
                    state.backfill_cursor_ts,
                    state.backfill_cursor_id,
                    1 if state.backfill_done else 0,
                    state.watermark_ts,
                    state.watermark_id,
                    error,
                ),
            )

    def _bump_counter(self, adapter: TimelineAdapter, column: str, amount: int) -> None:
        if amount <= 0:
            return
        assert column in ("backfill_rows", "incremental_rows")
        with self._dest_conn.cursor() as cursor:
            cursor.execute(
                self._dest_sql(
                    f"UPDATE timeline_sync_state SET {column} = {column} + %s, updated_at = now() "
                    "WHERE adapter = %s"
                ),
                (amount, adapter.name),
            )

    # -- sync ----------------------------------------------------------------

    def _fetch(self, sql: str, params: dict[str, Any]) -> list[tuple[Any, ...]]:
        with self._source_conn.cursor() as cursor:
            cursor.execute(self._source_sql(sql), params)
            return cursor.fetchall()

    def _upsert(self, adapter: TimelineAdapter, rows: list[tuple[Any, ...]]) -> None:
        if not rows:
            return
        # Adapter queries are keyed by event_id, but guard against in-batch
        # duplicates anyway: ON CONFLICT DO UPDATE rejects them outright.
        deduped: dict[str, tuple[Any, ...]] = {}
        for row in rows:
            deduped[row[0]] = row
        values = [
            (adapter.name, row[0], row[1], row[2], row[13], row[3], row[4], row[5], row[6],
             row[7], row[8], adapter.source_table, row[9], row[10], row[11], row[12])
            for row in deduped.values()
        ]
        with self._dest_conn.cursor() as cursor:
            execute_values(
                cursor,
                timeline_upsert_sql(
                    table_ref=self._dest_sql("timeline_events"),
                    sequence_ref=self._dest_sql("timeline_events_seq"),
                ),
                values,
                template=_TIMELINE_INSERT_TEMPLATE,
                page_size=1000,
            )

    def _batch_limit(self, adapter: TimelineAdapter) -> int:
        return self._batch_size or adapter.batch_size

    def _run_incremental(self, adapter: TimelineAdapter, state: _AdapterState, deadline: float | None) -> int:
        total = 0
        limit = self._batch_limit(adapter)
        while True:
            rows = self._fetch(
                adapter.incremental_sql,
                {
                    "watermark_ts": state.watermark_ts,
                    "watermark_id": state.watermark_id,
                    "limit": limit,
                },
            )
            if not rows:
                break
            self._upsert(adapter, rows)
            last = rows[-1]
            state.watermark_ts = last[12]
            state.watermark_id = last[0]
            self._save_state(adapter, state)
            self._bump_counter(adapter, "incremental_rows", len(rows))
            total += len(rows)
            if len(rows) < limit or _past(deadline):
                break
        return total

    def _run_refresh(self, adapter: TimelineAdapter, deadline: float | None) -> int:
        """Re-walk (and re-upsert) the adapter's recent event window.

        Reuses the newest-first backfill query with a local cursor; upserts
        only bump seq when the normalized content (including priority)
        actually changed, so a converged window is close to free.
        """
        cutoff = datetime.now(tz=UTC) - timedelta(hours=adapter.refresh_hours)
        cursor_ts: datetime = BACKFILL_CURSOR_START
        cursor_id = ""
        total = 0
        limit = self._batch_limit(adapter)
        while not _past(deadline):
            rows = self._fetch(
                adapter.backfill_sql,
                {"cursor_ts": cursor_ts, "cursor_id": cursor_id, "limit": limit},
            )
            if not rows:
                break
            fresh = [row for row in rows if row[3] >= cutoff]
            self._upsert(adapter, fresh)
            total += len(fresh)
            if len(fresh) < len(rows) or len(rows) < limit:
                break
            last = rows[-1]
            cursor_ts, cursor_id = last[3], last[0]
        return total

    def _refresh_gmail_correspondents(self) -> None:
        """Maintain the addresses-Zach-has-written-to relationship table.

        Timeline-owned state (created by ensure_timeline_tables) consumed by
        the gmail adapter's known-correspondent rule. Refreshed from the
        source at most once per day; skipped when the source has no gmail.
        """
        timeline_correspondents = self._qualified_regclass("timeline_gmail_correspondents", namespace=self._dest_schema)
        with self._dest_conn.cursor() as cursor:
            cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (timeline_correspondents,))
            if not cursor.fetchone()[0]:
                return
            cursor.execute(self._dest_sql("SELECT max(refreshed_at) FROM timeline_gmail_correspondents"))
            last = cursor.fetchone()[0]
        if last is not None and datetime.now(tz=UTC) - last < timedelta(hours=24):
            return
        gmail_messages = self._qualified_regclass("gmail_messages", namespace=self._source_schema)
        with self._source_conn.cursor() as cursor:
            cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (gmail_messages,))
            if not cursor.fetchone()[0]:
                return
            cursor.execute(
                self._source_sql(
                    """
                    SELECT lower(COALESCE(NULLIF(substring(rcpt FROM '<([^>]+)>'), ''), rcpt)) AS addr,
                           count(*) AS n_sent_to,
                           max(m.internal_date) AS last_sent_at
                    FROM gmail_messages m
                    CROSS JOIN LATERAL unnest(m.to_addresses) AS rcpt
                    WHERE m.from_address ILIKE '%%' || m.account || '%%'
                       OR EXISTS (SELECT 1 FROM gmail_sync_state s
                                  WHERE s.account <> '' AND m.from_address ILIKE '%%' || s.account || '%%')
                    GROUP BY 1
                    """
                )
            )
            rows = cursor.fetchall()
        with self._dest_conn.cursor() as cursor:
            cursor.execute(self._dest_sql("DELETE FROM timeline_gmail_correspondents"))
            execute_values(
                cursor,
                self._dest_sql(
                    "INSERT INTO timeline_gmail_correspondents (addr, n_sent_to, last_sent_at) "
                    "VALUES %s ON CONFLICT (addr) DO NOTHING"
                ),
                rows,
            )

    def _run_backfill_batch(self, adapter: TimelineAdapter, state: _AdapterState) -> int:
        limit = self._batch_limit(adapter)
        rows = self._fetch(
            adapter.backfill_sql,
            {
                "cursor_ts": state.backfill_cursor_ts,
                "cursor_id": state.backfill_cursor_id,
                "limit": limit,
            },
        )
        if rows:
            self._upsert(adapter, rows)
            last = rows[-1]
            state.backfill_cursor_ts = last[3]
            state.backfill_cursor_id = last[0]
        if len(rows) < limit:
            state.backfill_done = True
        self._save_state(adapter, state)
        self._bump_counter(adapter, "backfill_rows", len(rows))
        return len(rows)

    def run(self, *, max_seconds: float | None = None) -> list[AdapterSyncStats]:
        self._connect()
        deadline = time.monotonic() + max_seconds if max_seconds else None
        stats: dict[str, AdapterSyncStats] = {
            adapter.name: AdapterSyncStats(adapter=adapter.name) for adapter in self._adapters
        }
        states: dict[str, _AdapterState] = {}
        failed: list[str] = []

        try:
            self._refresh_gmail_correspondents()
        except Exception:  # noqa: BLE001 - the gmail adapter degrades, others run
            logger.exception("timeline gmail correspondent refresh failed")

        for adapter in self._adapters:
            try:
                state = self._load_state(adapter)
                states[adapter.name] = state
                stats[adapter.name].incremental_rows = self._run_incremental(adapter, state, deadline)
                if adapter.refresh_hours > 0 and state.backfill_done:
                    stats[adapter.name].refreshed_rows = self._run_refresh(adapter, deadline)
                stats[adapter.name].backfill_done = state.backfill_done
            except Exception as exc:  # noqa: BLE001 - keep other adapters running
                logger.exception("timeline incremental sync failed for %s", adapter.name)
                stats[adapter.name].error = str(exc)
                failed.append(adapter.name)
            if _past(deadline):
                break

        active = [
            adapter
            for adapter in self._adapters
            if adapter.name in states
            and not states[adapter.name].backfill_done
            and not stats[adapter.name].error
        ]
        while active and not _past(deadline):
            for adapter in list(active):
                state = states[adapter.name]
                try:
                    stats[adapter.name].backfill_rows += self._run_backfill_batch(adapter, state)
                except Exception as exc:  # noqa: BLE001
                    logger.exception("timeline backfill failed for %s", adapter.name)
                    stats[adapter.name].error = str(exc)
                    failed.append(adapter.name)
                    active.remove(adapter)
                    continue
                stats[adapter.name].backfill_done = state.backfill_done
                if state.backfill_done:
                    active.remove(adapter)
                if _past(deadline):
                    break

        if failed:
            raise TimelineSyncError(
                f"timeline sync failed for adapters: {', '.join(sorted(set(failed)))}",
                stats=list(stats.values()),
            )
        return list(stats.values())


class TimelineSyncError(RuntimeError):
    def __init__(self, message: str, *, stats: list[AdapterSyncStats] | None = None) -> None:
        super().__init__(message)
        self.stats = stats or []


def _past(deadline: float | None) -> bool:
    return deadline is not None and time.monotonic() >= deadline


def main(argv: Sequence[str] | None = None) -> int:
    """Manual/pump entrypoint.

    In production the Dagster asset drives the engine with source == dest.
    This CLI exists for development: point --source-url at the real warehouse
    (the session is forced read-only) and --dest-url at a local Postgres to
    build a local timeline without writing anything to the source.
    """
    from dotenv import load_dotenv
    import os

    load_dotenv()
    parser = argparse.ArgumentParser(description="Sync the unified timeline")
    parser.add_argument("--source-url", default=os.getenv("POSTGRES_DATABASE_URL", ""))
    parser.add_argument("--dest-url", default=os.getenv("TIMELINE_DATABASE_URL", ""))
    parser.add_argument("--source-schema", default="public")
    parser.add_argument("--dest-schema", default="public")
    parser.add_argument("--adapters", default="", help="comma-separated adapter names (default: all)")
    parser.add_argument("--batch-size", type=int, default=0)
    parser.add_argument("--max-seconds", type=float, default=0)
    parser.add_argument("--loop", type=float, default=0, help="re-run every N seconds")
    args = parser.parse_args(argv)

    adapters: Sequence[TimelineAdapter] = TIMELINE_ADAPTERS
    if args.adapters:
        adapters = [adapter_by_name(name.strip()) for name in args.adapters.split(",") if name.strip()]

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    engine = TimelineSyncEngine(
        source_url=args.source_url,
        dest_url=args.dest_url or None,
        source_schema=args.source_schema,
        dest_schema=args.dest_schema,
        adapters=adapters,
        batch_size=args.batch_size or None,
    )
    try:
        while True:
            started = time.monotonic()
            try:
                stats = engine.run(max_seconds=args.max_seconds or None)
            except TimelineSyncError as exc:
                stats = exc.stats
                logger.error("%s", exc)
            summary = {
                s.adapter: {
                    "backfill": s.backfill_rows,
                    "incremental": s.incremental_rows,
                    "done": s.backfill_done,
                    **({"error": s.error} if s.error else {}),
                }
                for s in stats
            }
            logger.info("timeline sync pass in %.1fs: %s", time.monotonic() - started, json.dumps(summary))
            if not args.loop:
                break
            time.sleep(args.loop)
    finally:
        engine.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
