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
import hashlib
import json
import logging
import time
from typing import Any

import psycopg2
from psycopg2.extras import execute_values

from personal_data_warehouse.config import normalize_postgres_url
from personal_data_warehouse.relations import expand_relations, physical_schema_names

logger = logging.getLogger(__name__)

TIMELINE_SNIPPET_CHARS = 500
TIMELINE_TITLE_CHARS = 300
TIMELINE_DEFAULT_BATCH_SIZE = 2000

# Priority tiers, classified per row at sync time. Stored in the
# ``timeline_priority`` Postgres enum so the value is self-describing (the
# column reads 'self'/'direct'/... rather than an opaque number) and the tier
# set is discoverable straight from the schema. Declaration order in the enum
# is the sort order, highest attention first: self > direct > cc > noise >
# background (> unclassified, the not-yet-synced bucket). The lines between
# tiers are heuristics and expected to be tuned; changing an adapter's
# classification and re-running the backfill reclassifies rows (priority
# participates in the content guard, so seq bumps on change). Each constant is
# a quoted SQL label literal so interpolating it into an adapter's SELECT emits
# the enum label directly.
TIMELINE_PRIORITY_SELF = "'self'"  # actions Zach initiated (his messages, sessions, memos, notes)
TIMELINE_PRIORITY_DIRECT = "'direct'"  # real people reaching him directly (DMs, direct email, small groups)
TIMELINE_PRIORITY_CC = "'cc'"  # real-people activity he is peripheral to (cc'd, channels, big groups)
TIMELINE_PRIORITY_NOISE = "'noise'"  # bulk/automated traffic (newsletters, bots, non-member channels)
TIMELINE_PRIORITY_BACKGROUND = "'background'"  # the warehouse's own machinery (enrichment, mutation workers)
# The sixth enum label is the column DEFAULT and nothing else: no adapter may
# emit it. It exists so a row inserted outside the sync engine is visibly
# unclassified instead of silently mis-tiered, and its presence in
# timeline.events is a bug, never a steady state. Dropping the label would
# rewrite a 60 GB column, so it stays — enforced by
# tests/test_timeline.py::test_no_adapter_can_emit_the_unclassified_sentinel.
TIMELINE_PRIORITY_UNCLASSIFIED = "'unclassified'"

# The definitions agents are told to filter on. Published verbatim as the
# Postgres COMMENT on the enum type (postgres.py), because an agent reading the
# schema directly never sees this module.
TIMELINE_PRIORITY_DEFINITIONS: tuple[tuple[str, str], ...] = (
    ("self", "actions Zach initiated"),
    ("direct", "real people reaching him directly"),
    ("cc", "real-people activity he is peripheral to"),
    ("noise", "bulk/automated traffic"),
    ("background", "the warehouse's own machinery"),
    (
        "unclassified",
        "the column default and never valid in steady state; its presence is a bug",
    ),
)
# The five tiers an adapter may emit, in enum declaration (attention) order.
TIMELINE_PRIORITY_LABELS: tuple[str, ...] = tuple(
    label for label, _ in TIMELINE_PRIORITY_DEFINITIONS if label != "unclassified"
)

_EPOCH = "'1970-01-01 00:00:00+00'::timestamptz"
# Sentinel guard: house style stores "no timestamp" as the epoch, so anything
# at or before this is treated as absent.
_EPOCH_GUARD = "'1970-01-02 00:00:00+00'::timestamptz"
# Where the newest-first backfill cursor starts. A finite far-future constant
# (not 'infinity') so the value roundtrips cleanly through drivers and the
# NOT NULL state row.
BACKFILL_CURSOR_START = datetime(9999, 1, 1, tzinfo=UTC)
# The warehouse-wide "absent" sentinel (see AGENTS.md): absence is the
# epoch here, never NULL.
BACKFILL_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

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
    # Deliberate classification expression supplied by the adapter author.
    # This is required even though the expression also appears in the SELECT:
    # registration must not be able to inherit a plausible-looking default.
    priority_expression: str
    batch_size: int = TIMELINE_DEFAULT_BATCH_SIZE
    # Zero means drain the incremental watermark until caught up. A bounded
    # value lets unusually broad dimension invalidations make steady progress
    # without starving later adapters and their refresh windows.
    max_incremental_batches_per_run: int = 0
    # When > 0, every sync pass re-walks rows whose event_ts falls in the last
    # N hours and re-upserts them. Classification signals that look forward or
    # arrive late (Zach replying in a chat promotes the surrounding window;
    # his answer to an email promotes the thread) converge through this window
    # instead of freezing at first-ingest values.
    refresh_hours: float = 0.0
    # How far BEHIND the stored watermark each incremental pass restarts.
    #
    # The engine walks (ingest_ts, event_id) strictly ascending, so a row whose
    # ingest stamp is assigned before the watermark moves but whose transaction
    # commits after this pass has read is skipped FOREVER: the next pass starts
    # above it, and `_run_refresh` re-walks by EVENT time, which for a
    # late-backfilled old message is long past. Measured in production
    # 2026-08-26, that cost 798 of 26,217 Slack rows (3.0%) in one settled day,
    # every one stamped hours behind a watermark already sitting at "now".
    #
    # This is cheap insurance, not the guarantee -- re-reading is bounded by the
    # source's ingest index and the upsert is content-guarded, so a converged
    # window writes nothing. It is deliberately SMALL because a source that
    # restamps rows (Slack rewrites ~330k rows/day for ~25k real messages) makes
    # a wide window expensive. `reconcile_hours` is what actually closes C1.
    incremental_lag_hours: float = 0.25
    # When > 0, every sync pass asks the source which rows in the last N hours
    # of EVENT time are missing from the timeline, and inserts exactly those.
    #
    # Unlike `refresh_hours` (which re-walks a window to reconverge CONTENT),
    # this is an anti-join against the timeline's own primary key, so it returns
    # only genuine gaps and costs one index probe per source row in the window.
    # It is what makes "everything synced eventually lands on the timeline" a
    # property the engine enforces rather than a claim, because it is immune to
    # HOW a row was missed -- a lost race, a crashed pass, a watermark repair.
    reconcile_hours: float = 48.0
    # How often that sweep may run. Its cost is the window, not the gap count,
    # so it gets its own cadence instead of riding every pass.
    reconcile_interval_hours: float = 1.0
    # Opt-in orphan prune. A SELECT returning the adapter's authoritative
    # CURRENT set of event_ids (one text column). Timeline rows for this
    # adapter whose event_id is absent from that set are deleted.
    #
    # Opt-in, never global: an adapter over an append-only source must NEVER
    # prune, because a bounded incremental query legitimately does not return
    # rows it already synced -- pruning on that basis would delete history.
    # Only a RECONCILED derived source, one that is rebuilt each run and can
    # state its full current key set cheaply, may set this. Measured
    # 2026-08-23, derived_finance.transactions had been re-deduplicated and
    # re-keyed repeatedly, leaving 4,944 orphaned timeline rows (25.6%) that
    # search still returned alongside their live replacements.
    prune_sql: str = ""
    # `_simple_adapter` used to wrap every priority expression in
    # COALESCE(..., 'cc'). Removing that unsafe runtime fallback changes the
    # generated SQL text but not any adapter's intentional classification.
    # Keep the legacy generated text only for signature calculation so this
    # safety rollout does not reset all 26 production backfills (48M rows).
    signature_backfill_sql: str = ""
    signature_incremental_sql: str = ""
    # Gap-only query for `reconcile_hours`, generated beside the others. It is
    # deliberately NOT part of `adapter_signature`: it changes no row's
    # normalized content, so including it would reset every adapter's backfill
    # and re-walk 48M rows to add a pass whose whole purpose is to avoid that.
    reconcile_sql: str = ""


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
    priority: str,
    where: str = "TRUE",
    changed_join_sql: str = "",
    changed_join_anchor: str = "",
    batch_size: int = TIMELINE_DEFAULT_BATCH_SIZE,
    max_incremental_batches_per_run: int = 0,
    refresh_hours: float = 0.0,
    prune_sql: str = "",
) -> TimelineAdapter:
    if not priority.strip():
        raise ValueError(f"timeline adapter {name!r} must declare a priority expression")
    if search_text is None:
        search_text = _search_concat(title, snippet, context, actor)

    # event_ts and ingest_ts are used raw (no defensive COALESCE): they are the
    # ORDER BY / keyset expressions, and wrapping a bare indexed column in a
    # function forces a full sort of the source table on every backfill batch
    # (measured at ~90s/batch on the 30M-row slack_messages in production).
    # Source columns are NOT NULL throughout the warehouse schema; adapters
    # that need fallback chains state them explicitly.
    def build_select(from_clause: str, *, legacy_priority_fallback: bool = False) -> str:
        priority_select = (
            f"COALESCE(({priority}), 'cc')" if legacy_priority_fallback else f"({priority})"
        )
        return f"""
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
            {priority_select} AS priority
        FROM {from_clause}
        WHERE ({where})
    """

    select = build_select(from_sql)
    legacy_select = build_select(from_sql, legacy_priority_fallback=True)
    backfill_sql = f"""
        {select}
          AND ({event_ts}) <= %(cursor_ts)s
          AND (({event_ts}), COALESCE(({event_id}), ''))
              < (%(cursor_ts)s, %(cursor_id)s)
        ORDER BY 4 DESC, 1 DESC
        LIMIT %(limit)s
    """
    signature_backfill_sql = f"""
        {legacy_select}
          AND ({event_ts}) <= %(cursor_ts)s
          AND (({event_ts}), COALESCE(({event_id}), ''))
              < (%(cursor_ts)s, %(cursor_id)s)
        ORDER BY 4 DESC, 1 DESC
        LIMIT %(limit)s
    """
    # When ingest_ts is a computed expression (GREATEST over attachment /
    # enrichment LATERALs) no index can serve the watermark predicate, so the
    # bare incremental query re-evaluates the full join for every source row
    # on every tick. Adapters with that shape pass changed_join_sql — an
    # incremental-only inner join to a watermark-driven candidate set covering
    # every input of ingest_ts — so per-tick cost scales with what changed.
    incremental_from = from_sql
    if changed_join_sql:
        if changed_join_anchor:
            if from_sql.count(changed_join_anchor) != 1:
                raise ValueError(
                    f"changed join anchor must occur exactly once: {changed_join_anchor!r}"
                )
            incremental_from = from_sql.replace(
                changed_join_anchor,
                f"{changed_join_anchor}\n    {changed_join_sql}",
                1,
            )
        else:
            incremental_from = f"{from_sql}\n    {changed_join_sql}"
    incremental_sql = f"""
        {build_select(incremental_from)}
          AND ({ingest_ts}) >= %(watermark_ts)s
          AND (({ingest_ts}), COALESCE(({event_id}), ''))
              > (%(watermark_ts)s, %(watermark_id)s)
        ORDER BY 13 ASC, 1 ASC
        LIMIT %(limit)s
    """
    signature_incremental_sql = f"""
        {build_select(incremental_from, legacy_priority_fallback=True)}
          AND ({ingest_ts}) >= %(watermark_ts)s
          AND (({ingest_ts}), COALESCE(({event_id}), ''))
              > (%(watermark_ts)s, %(watermark_id)s)
        ORDER BY 13 ASC, 1 ASC
        LIMIT %(limit)s
    """
    max_ingest_sql = f"SELECT max({ingest_ts}) FROM {from_sql} WHERE ({where})"
    # Rows the source has INGESTED recently that the timeline does not have.
    #
    # Windowed on ingest, never on event time: the rows most likely to be
    # missing are exactly the ones with an old event_ts and a new ingest_ts --
    # a newly discovered Slack channel backfilling years of history, a late
    # attachment, a repaired sync. An event-time window is blind to precisely
    # that population, which is the population that was actually lost.
    #
    # The anti-join probes timeline.events' primary key, (adapter, event_id),
    # so it costs one index lookup per source row in the window and returns
    # only real gaps. Newest ingest first, so a deadline-bounded run repairs
    # the freshest damage before it runs out of budget.
    reconcile_sql = f"""
        {select}
          AND ({ingest_ts}) >= %(window_start)s
          AND (({ingest_ts}), COALESCE(({event_id}), ''))
              < (%(cursor_ts)s, %(cursor_id)s)
          AND NOT EXISTS (
              SELECT 1 FROM @timeline_events tl
              WHERE tl.adapter = %(adapter)s
                AND tl.event_id = COALESCE(({event_id}), '')
          )
        ORDER BY 13 DESC, 1 DESC
        LIMIT %(limit)s
    """
    return TimelineAdapter(
        name=name,
        source_table=source_table,
        source=source,
        kind=kind,
        backfill_sql=backfill_sql,
        incremental_sql=incremental_sql,
        max_ingest_sql=max_ingest_sql,
        priority_expression=priority,
        batch_size=batch_size,
        max_incremental_batches_per_run=max_incremental_batches_per_run,
        refresh_hours=refresh_hours,
        prune_sql=prune_sql,
        signature_backfill_sql=signature_backfill_sql,
        signature_incremental_sql=signature_incremental_sql,
        reconcile_sql=reconcile_sql,
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
    "statements?@|bank@|hcb@|dinobox@|sign@|bot@|metabase@|\\mfact\\w*@|replies\\+|info@|contact@|hello@|"
    "support@|feedback@|service@|security@|accounts?@|verify|apply@|jobs@|"
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
# Google and Outlook both prefix auto-generated RSVP/cancellation mail with the
# verb ("Accepted: ...", "Canceled: ...", "Tentative: ..."); the body is a
# calendar stub, not a person writing.
_GMAIL_RSVP_SUBJECT_PATTERN = (
    "'^(accepted|declined|tentative(ly accepted)?|updated invitation|"
    "cancell?ed( event)?|invitation)[: ]'"
)
# GitHub relays a bot's review under notifications@github.com, so the sender
# cannot say it is a bot: the payload has to. "@x[bot] commented", "x[bot] left
# a comment", Copilot's "was unable to review" all read as a reviewer to the
# old pattern and landed at direct on Zach's own PRs (sampled 2026-08-26).
_GMAIL_RELAYED_BOT_BODY_PATTERN = (
    "'(\\[bot\\]|latest updates on your projects|dependabot|"
    "copilot (commented|was unable to review))'"
)
# GitHub's X-GitHub-Reason lands in the cc list as <reason>@noreply.github.com.
# mention/author/assign/review_requested are the copies aimed at Zach;
# push/ci_activity/state_change are the machinery copies; subscribed is a
# thread he opted into but nobody addressed him in.
_GMAIL_RELAY_REASON_ADDRESSED = (
    "EXISTS (SELECT 1 FROM unnest(t.to_addresses || t.cc_addresses) a "
    "        WHERE a ~* '(mention|author|assign|review_requested)@noreply\\.github\\.com')"
)
_GMAIL_RELAY_REASON_AUTOMATED = (
    "EXISTS (SELECT 1 FROM unnest(t.to_addresses || t.cc_addresses) a "
    "        WHERE a ~* '(push|ci_activity|state_change|security_alert)@noreply\\.github\\.com')"
)
# "Merged #12 into main", "Closed #7", "@who pushed 2 commits": a human did
# something, but the mail is the platform reporting it, not the human writing.
_GMAIL_RELAY_STATE_CHANGE = (
    "((t.subject || ' ' || t.snippet) ~* "
    "'(^|\\s)(merged|closed|reopened) #\\d+|pushed \\d+ commits?')"
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
    " OR EXISTS (SELECT 1 FROM @gmail_sync_state self "
    "            WHERE self.account <> '' AND t.from_address ILIKE '%%' || self.account || '%%'))"
)
# Addressed to Zach himself: a synced account or his personal domain in To.
_GMAIL_ADDRESSED = (
    "EXISTS (SELECT 1 FROM unnest(t.to_addresses) rcpt "
    "        WHERE rcpt ILIKE '%%' || t.account || '%%' "
    "           OR lower(rcpt) LIKE '%%@zachlatta.com%%' "
    "           OR EXISTS (SELECT 1 FROM @gmail_sync_state self "
    "                      WHERE self.account <> '' AND rcpt ILIKE '%%' || self.account || '%%'))"
)
# >=30 self-sent messages sharing a normalized subject prefix within +/-3 days
# = a mail-merge blast (quote-shopping batches stay under the threshold).
_GMAIL_MERGE_CLUSTER = (
    "(SELECT count(*) FROM ("
    " SELECT 1 FROM @gmail_messages g2"
    f" WHERE {_GMAIL_MERGE_PREFIX.format(col='g2.subject')} = {_GMAIL_MERGE_PREFIX.format(col='t.subject')}"
    "  AND g2.internal_date BETWEEN t.internal_date - interval '3 days'"
    "                           AND t.internal_date + interval '3 days'"
    "  AND g2.from_address ILIKE '%%' || g2.account || '%%'"
    " LIMIT 30) merge_probe) >= 30"
)
_GMAIL_THREAD_INBOUND_BEFORE = (
    "EXISTS (SELECT 1 FROM @gmail_messages g3 "
    "        WHERE g3.thread_id = t.thread_id "
    "          AND g3.internal_date < t.internal_date "
    "          AND g3.from_address NOT ILIKE '%%' || g3.account || '%%' "
    "          AND NOT EXISTS (SELECT 1 FROM @gmail_sync_state s3 "
    "                          WHERE s3.account <> '' AND g3.from_address ILIKE '%%' || s3.account || '%%'))"
)
# Zach answered this thread after the message arrived (within 48h): the
# strongest "this conversation has his attention" signal.
_GMAIL_MY_REPLY_AFTER = (
    "EXISTS (SELECT 1 FROM @gmail_messages g4 "
    "        WHERE g4.thread_id = t.thread_id "
    "          AND g4.internal_date > t.internal_date "
    "          AND g4.internal_date < t.internal_date + interval '48 hours' "
    "          AND g4.from_address ILIKE '%%' || g4.account || '%%')"
)
_GMAIL_I_POSTED_IN_THREAD = (
    "EXISTS (SELECT 1 FROM @gmail_messages g5 "
    "        WHERE g5.thread_id = t.thread_id "
    "          AND g5.from_address ILIKE '%%' || g5.account || '%%')"
)
# Sender is someone Zach has written to at least twice (relationship signal;
# the table is timeline-owned state refreshed by the sync engine).
_GMAIL_KNOWN_CORRESPONDENT = (
    "EXISTS (SELECT 1 FROM @timeline_gmail_correspondents gc "
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
    from_sql="""@gmail_messages t
    LEFT JOIN LATERAL (
        SELECT
            string_agg(
                concat_ws(E'\n', NULLIF(a.filename, ''), NULLIF(e.text, '')),
                E'\n' ORDER BY a.part_id, e.updated_at DESC
            ) AS attachment_search_text,
            max(GREATEST(a.synced_at, COALESCE(e.updated_at, a.synced_at))) AS attachment_ingest_ts
        FROM @gmail_attachments a
        LEFT JOIN @file_attachment_enrichments e ON e.content_sha256 = a.content_sha256
        WHERE a.account = t.account AND a.message_id = t.message_id AND a.is_deleted = 0
    ) att ON TRUE""",
    changed_join_sql="""JOIN (
        SELECT account, message_id FROM @gmail_messages
        WHERE synced_at >= %(watermark_ts)s
        UNION
        SELECT a.account, a.message_id FROM @gmail_attachments a
        WHERE a.synced_at >= %(watermark_ts)s
        UNION
        SELECT a.account, a.message_id FROM @gmail_attachments a
        JOIN @file_attachment_enrichments e ON e.content_sha256 = a.content_sha256
        WHERE e.updated_at >= %(watermark_ts)s
    ) pdw_changed ON pdw_changed.account = t.account AND pdw_changed.message_id = t.message_id""",
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
        f"  CASE WHEN {_GMAIL_MERGE_CLUSTER} AND NOT {_GMAIL_THREAD_INBOUND_BEFORE} THEN 'noise' ELSE 'self' END "
        "WHEN 'SPAM' = ANY(t.label_ids) OR 'TRASH' = ANY(t.label_ids) THEN 'noise' "
        # Relay services carrying a human's activity: mention/author copies are
        # directed at me; bot-authored payloads (CI, deploy status) are noise;
        # the rest is skim-worthy cc.
        # Bot payloads are judged BEFORE the addressed-to-me copies: a review
        # bot commenting on Zach's own PR arrives as an author copy, and it is
        # still a machine talking.
        f"WHEN t.from_address ~* {_GMAIL_RELAY_SENDER_PATTERN} THEN "
        f"  CASE WHEN t.from_address ~* '\\[bot\\]' OR t.subject ~* {_GMAIL_CI_SUBJECT_PATTERN} "
        f"         OR t.snippet ~* {_GMAIL_RELAYED_BOT_BODY_PATTERN} "
        "         OR t.subject ~* '^(re: )?\\[[^\\]]+\\] bump ' THEN 'noise' "
        f"       WHEN {_GMAIL_RELAY_STATE_CHANGE} THEN "
        f"         CASE WHEN {_GMAIL_RELAY_REASON_ADDRESSED} THEN 'cc' ELSE 'noise' END "
        f"       WHEN {_GMAIL_RELAY_REASON_ADDRESSED} THEN 'direct' "
        f"       WHEN {_GMAIL_RELAY_REASON_AUTOMATED} THEN 'noise' "
        "       ELSE 'cc' END "
        f"WHEN t.subject ~* {_GMAIL_RSVP_SUBJECT_PATTERN} THEN 'cc' "
        f"WHEN t.subject ~* {_GMAIL_OTP_SUBJECT_PATTERN} THEN 'noise' "
        f"WHEN {_GMAIL_AUTOMATED_FROM} AND t.subject ~* '^(re: )?new comment' THEN 'cc' "
        f"WHEN {_GMAIL_HUMAN_ACTION_RELAY} THEN 'cc' "
        f"WHEN 'CATEGORY_FORUMS' = ANY(t.label_ids) AND ({_GMAIL_AUTOMATED_FROM} OR {_GMAIL_FORUMS_NOISE}) THEN 'noise' "
        f"WHEN 'CATEGORY_FORUMS' = ANY(t.label_ids) AND t.from_address !~* {_GMAIL_BULK_SENDER_PATTERN} THEN 'cc' "
        f"WHEN NOT {_GMAIL_AUTOMATED_FROM} AND {_GMAIL_MY_REPLY_AFTER} THEN 'direct' "
        f"WHEN NOT {_GMAIL_AUTOMATED_FROM} AND {_GMAIL_I_POSTED_IN_THREAD} THEN "
        f"  CASE WHEN {_GMAIL_ADDRESSED} THEN 'direct' ELSE 'cc' END "
        f"WHEN NOT {_GMAIL_AUTOMATED_FROM} AND {_GMAIL_KNOWN_CORRESPONDENT} AND {_GMAIL_ADDRESSED} THEN 'direct' "
        f"WHEN 'STARRED' = ANY(t.label_ids) AND NOT {_GMAIL_AUTOMATED_FROM} "
        f"  AND NOT {_GMAIL_BULK_CATEGORY} THEN 'direct' "
        f"WHEN {_GMAIL_BULK_CATEGORY} OR {_GMAIL_AUTOMATED_FROM} THEN 'noise' "
        f"WHEN {_GMAIL_ADDRESSED} THEN 'direct' "
        "ELSE 'cc' END"
    ),
    refresh_hours=72,
)

_SLACK_JOINS = """
    LEFT JOIN @slack_users u
        ON u.account = t.account AND u.team_id = t.team_id AND u.user_id = t.user_id
    LEFT JOIN @slack_conversations c
        ON c.account = t.account AND c.team_id = t.team_id AND c.conversation_id = t.conversation_id
    LEFT JOIN @slack_account_identities ident
        ON ident.account = t.account AND ident.team_id = t.team_id
"""

# The root of this thread is one of Zach's own messages: a reply to him.
# A single primary-key probe per threaded row.
_SLACK_THREAD_ROOT_MINE = (
    "(t.thread_ts <> '' AND ident.user_id <> '' AND EXISTS ("
    "SELECT 1 FROM @slack_messages z "
    "WHERE z.account = t.account AND z.team_id = t.team_id "
    "  AND z.conversation_id = t.conversation_id AND z.message_ts = t.thread_ts "
    "  AND z.user_id = ident.user_id AND z.is_deleted = 0))"
)
# Slack's own narration of membership/topic changes; never a person writing.
_SLACK_SYSTEM_SUBTYPES = (
    "('channel_join', 'channel_leave', 'channel_archive', 'channel_name', "
    "'channel_purpose', 'channel_topic', 'group_join', 'group_leave')"
)
# The thread root is a broadcast, not a conversation: an announcement with
# <!channel>/<!here>, or a thread that has grown past twenty replies. Replies
# to Zach's announcement ("First", "sigh", a Q&A between two other people) are
# not aimed at him even though he wrote the root -- measured 2026-08-26, every
# reply in a 95-reply <!here> thread of his was 'direct'.
#
# Shape matters: written as EXISTS(... AND (reply_count > 20 OR text ~ ...))
# the planner hashed the whole predicate set instead of probing the root --
# a six-worker parallel seq scan over all 46M slack rows, materialized once
# per 5,000-row backfill page (29s and 48 GB of reads per page, measured on
# prod 2026-08-26). A scalar subquery keyed on the primary key cannot be
# hashed, so the OR is evaluated on the one root row it finds.
_SLACK_THREAD_ROOT_BROADCAST = (
    "(t.thread_ts <> '' AND COALESCE(("
    "SELECT z.reply_count > 20 OR z.text ~ '<!(channel|here|everyone)>' "
    "FROM @slack_messages z "
    "WHERE z.account = t.account AND z.team_id = t.team_id "
    "  AND z.conversation_id = t.conversation_id AND z.message_ts = t.thread_ts "
    "LIMIT 1), false))"
)
# A real Slack ping: the message carries his user id, so Slack itself notified
# him. Distinct from his NAME in the text, which in a public channel is far more
# often people talking about him ("doesn't zach mostly vibecode") than to him.
_SLACK_ID_MENTION = "(ident.user_id <> '' AND t.text LIKE '%%<@' || ident.user_id || '>%%')"
_SLACK_NAME_TEXT_MENTION = "(t.text ~* '\\m(zach|zrl|latta|zachlatta)\\M')"
# Zach posted in this thread within the preceding 12 hours: the reply lands in
# a conversation he is actively part of. (Unbounded thread participation
# over-promoted: RSVP piles in announcement threads and day-old ship threads
# read as ambient, per the labeled benchmark.)
_SLACK_MY_THREAD_RECENT = (
    "(t.thread_ts <> '' AND ident.user_id <> '' AND EXISTS ("
    "SELECT 1 FROM @slack_messages z "
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
        " SELECT 1 FROM @slack_messages z"
        " WHERE z.user_id = ident.user_id"
        f"  AND z.message_datetime BETWEEN t.message_datetime - interval '{before}'"
        f"                             AND t.message_datetime + interval '{after}'"
        "  AND z.account = t.account AND z.team_id = t.team_id"
        "  AND z.conversation_id = t.conversation_id AND z.is_deleted = 0"
        # Slack narrating "<@me> has joined the channel" under his user id is
        # not him taking part: without this, being added to a channel promoted
        # its next six hours of chatter (sampled 2026-08-26).
        f"  AND z.subtype NOT IN {_SLACK_SYSTEM_SUBTYPES}"
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
    " SELECT 1 FROM @slack_messages v"
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
_SLACK_MPIM_ROSTER = (
    "GREATEST(c.num_members, (SELECT count(*) FROM @slack_conversation_members m "
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
    "   FROM @slack_conversation_members m "
    "   WHERE m.account = t.account AND m.team_id = t.team_id "
    "     AND m.conversation_id = t.conversation_id AND m.is_deleted = 0 "
    "     AND (ident.user_id = '' OR m.user_id <> ident.user_id) "
    "   UNION ALL "
    "   SELECT c.name AS user_id, 1 AS source_order WHERE c.name <> ''"
    " ) dm_peer "
    " LEFT JOIN @slack_users peer "
    "   ON peer.account = t.account AND peer.team_id = t.team_id AND peer.user_id = dm_peer.user_id "
    " WHERE dm_peer.user_id <> '' "
    " ORDER BY dm_peer.source_order, dm_peer.user_id LIMIT 1)"
)

# Tier semantics (re-audited 2026-08-26 against the definitions in AGENTS.md):
# direct = a real person addressing HIM -- a DM, a real <@id> ping, a reply in
# a thread of his that is a conversation rather than an announcement, a group
# DM or channel exchange he is actively in, or his name used where addressing
# him by name is plausible (private rooms, group DMs, or while he is engaged).
# cc = real people he is peripheral to: private team channels he sits in,
# group DMs he is not engaged in, replies piling under his broadcasts, and
# people talking ABOUT him in public. noise = everything a public channel
# says that is not aimed at him -- member or not; being in #lounge does not
# make its chatter his. Two standing reversals from the 2026-07 sampling still
# hold: "channels I post in a lot" must NOT promote (lounge-chatter flood), and
# one drive-by message must not promote a busy channel's +/-6h -- participation
# means at least two of his messages in the window. System subtypes are judged
# before 'self': "<@me> has joined the channel" is Slack narrating, not him.
_SLACK_MESSAGE_PRIORITY = (
    "CASE "
    f"WHEN {_SLACK_ATTACHMENT_ONLY_MESSAGE} THEN {TIMELINE_PRIORITY_BACKGROUND} "
    f"WHEN t.subtype IN {_SLACK_SYSTEM_SUBTYPES} THEN 'noise' "
    "WHEN t.user_id <> '' AND t.user_id = ident.user_id THEN 'self' "
    f"WHEN {_SLACK_IS_BOT} THEN "
    "  CASE WHEN c.is_im = 1 AND t.text ~* '(commented on|shared an item|replied to|"
    "mentioned you|upgrade request|invited you|assigned you)' THEN 'cc' ELSE 'noise' END "
    "WHEN c.is_im = 1 THEN 'direct' "
    f"WHEN {_SLACK_ID_MENTION} THEN 'direct' "
    f"WHEN {_SLACK_NAME_TEXT_MENTION} AND (c.is_mpim = 1 OR c.is_private = 1 "
    f"  OR {_SLACK_W6H} >= 1 OR {_SLACK_THREAD_ROOT_MINE} OR {_SLACK_MY_THREAD_RECENT}) "
    "  THEN 'direct' "
    f"WHEN {_SLACK_THREAD_ROOT_MINE} OR {_SLACK_MY_THREAD_RECENT} THEN "
    f"  CASE WHEN {_SLACK_THREAD_ROOT_BROADCAST} THEN 'cc' ELSE 'direct' END "
    f"WHEN {_SLACK_NAME_TEXT_MENTION} THEN 'cc' "
    f"WHEN c.is_mpim = 1 THEN "
    f"  CASE WHEN {_SLACK_W6H} >= 1 OR {_SLACK_MPIM_ROSTER} BETWEEN 1 AND 5 "
    f"        OR {_SLACK_P3D} >= 3 THEN 'direct' ELSE 'cc' END "
    "WHEN c.is_member = 1 THEN "
    f"  CASE WHEN {_SLACK_W6H} >= 2 AND ({_SLACK_CONV_VELOCITY_24H} <= 150 "
    f"         OR ({_SLACK_W6H} >= 3 AND {_SLACK_P3D} >= 2)) THEN 'direct' "
    f"       WHEN c.is_private = 1 AND {_SLACK_MPIM_ROSTER} <= 20 "
    f"         AND {_SLACK_P24H} >= 1 THEN 'direct' "
    "       WHEN c.is_private = 1 THEN 'cc' "
    f"       WHEN {_SLACK_W6H} >= 1 THEN 'cc' "
    "       ELSE 'noise' END "
    "ELSE 'noise' END"
)

_SLACK_FILE_PRIORITY = (
    "CASE "
    f"WHEN {_SLACK_INACCESSIBLE_FILE_STUB} THEN {TIMELINE_PRIORITY_BACKGROUND} "
    "WHEN t.user_id <> '' AND t.user_id = ident.user_id THEN 'self' "
    "WHEN u.is_bot = 1 THEN 'noise' "
    "WHEN c.is_im = 1 THEN 'direct' "
    f"WHEN c.is_mpim = 1 THEN "
    "  CASE WHEN (SELECT count(*) FROM (SELECT 1 FROM @slack_messages z "
    "       WHERE z.user_id = ident.user_id "
    "         AND z.message_datetime BETWEEN t.created_at - interval '6 hours' "
    "                                    AND t.created_at + interval '6 hours' "
    "         AND z.account = t.account AND z.team_id = t.team_id "
    "         AND z.conversation_id = t.conversation_id AND z.is_deleted = 0 "
    f"       LIMIT 1) fw) >= 1 OR {_SLACK_MPIM_ROSTER} BETWEEN 1 AND 5 THEN 'direct' ELSE 'cc' END "
    "WHEN c.is_member = 1 THEN "
    "  CASE WHEN (SELECT count(*) FROM (SELECT 1 FROM @slack_messages z "
    "       WHERE z.user_id = ident.user_id "
    "         AND z.message_datetime BETWEEN t.created_at - interval '6 hours' "
    "                                    AND t.created_at + interval '6 hours' "
    "         AND z.account = t.account AND z.team_id = t.team_id "
    "         AND z.conversation_id = t.conversation_id AND z.is_deleted = 0 "
    "       LIMIT 2) fw) >= 2 THEN 'direct' "
    "       WHEN c.is_private = 1 THEN 'cc' ELSE 'noise' END "
    "ELSE 'noise' END"
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
    from_sql="@slack_messages t" + _SLACK_JOINS,
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
    from_sql="@slack_files t" + _SLACK_JOINS,
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
    # cm/roster are LATERAL probes, not whole-table GROUP BY subqueries: the
    # grouped form materialized every chat's aggregate on every incremental
    # tick regardless of how few messages changed. The probes ride
    # apple_message_chat_messages_message_idx and the chat_handles PK.
    from_sql="""@clean_apple_messages t
    LEFT JOIN @apple_message_handles h ON h.account = t.account AND h.handle_id = t.handle_id
    LEFT JOIN LATERAL (
        SELECT min(chat_id) AS chat_id
        FROM @apple_message_chat_messages
        WHERE account = t.account AND message_id = t.message_id
    ) cm ON TRUE
    LEFT JOIN @apple_message_chats c ON c.account = t.account AND c.chat_id = cm.chat_id
    LEFT JOIN LATERAL (
        -- Distinct people, not handle rows: device re-syncs leave duplicate
        -- handle records for the same address in chat rosters.
        SELECT count(DISTINCT COALESCE(NULLIF(rh.address, ''), ch.handle_id)) AS n
        FROM @apple_message_chat_handles ch
        LEFT JOIN @apple_message_handles rh
            ON rh.account = ch.account AND rh.handle_id = ch.handle_id
        WHERE ch.account = t.account AND ch.chat_id = cm.chat_id
    ) roster ON TRUE
    LEFT JOIN LATERAL (
        SELECT
            string_agg(
                concat_ws(E'\n', COALESCE(NULLIF(a.filename, ''), NULLIF(a.transfer_name, '')), NULLIF(e.text, '')),
                E'\n' ORDER BY a.attachment_id, e.updated_at DESC
            ) AS attachment_search_text,
            max(GREATEST(a.ingested_at, COALESCE(e.updated_at, a.ingested_at))) AS attachment_ingest_ts
        FROM @apple_message_attachments a
        LEFT JOIN @file_attachment_enrichments e ON e.content_sha256 = a.content_sha256
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
            FROM @apple_message_attachments a
            WHERE a.account = t.account AND a.message_id = t.message_id AND a.is_missing = 0
        ) labels
    ) att_labels ON TRUE
    CROSS JOIN (
        SELECT GREATEST(
            COALESCE(
                (SELECT max(synced_at) FROM @contact_cards),
                TIMESTAMPTZ '1970-01-01'
            ),
            COALESCE(
                (SELECT max(synced_at) FROM @apple_contact_cards),
                TIMESTAMPTZ '1970-01-01'
            )
        ) AS latest_synced_at
    ) contact_sync""",
    changed_join_sql="""JOIN (
        SELECT account, message_id FROM @apple_messages
        WHERE ingested_at >= %(watermark_ts)s
        UNION
        SELECT a.account, a.message_id FROM @apple_message_attachments a
        WHERE a.ingested_at >= %(watermark_ts)s
        UNION
        SELECT a.account, a.message_id FROM @apple_message_attachments a
        JOIN @file_attachment_enrichments e ON e.content_sha256 = a.content_sha256
        WHERE e.updated_at >= %(watermark_ts)s
        UNION
        (
        -- Contact edits can add, remove, or replace an address, so the old
        -- affected point is not necessarily present after the upsert. Re-emit
        -- all incoming messages when either contact source changes because
        -- contact batches are sparse and this is the only deletion-safe
        -- invalidation. Page this branch before the expensive message joins:
        -- without the inner keyset LIMIT, every 2,000-row page normalized all
        -- ~100,000 incoming messages before the outer LIMIT.
        SELECT m.account, m.message_id
        FROM @apple_messages m
        CROSS JOIN (
            SELECT GREATEST(
                COALESCE(
                    (SELECT max(synced_at) FROM @contact_cards),
                    TIMESTAMPTZ '1970-01-01'
                ),
                COALESCE(
                    (SELECT max(synced_at) FROM @apple_contact_cards),
                    TIMESTAMPTZ '1970-01-01'
                )
            ) AS latest_synced_at
        ) identity_sync
        WHERE m.is_from_me = 0
          AND (
              identity_sync.latest_synced_at,
              concat_ws('|', m.account, m.message_id)
          ) > (%(watermark_ts)s, %(watermark_id)s)
        ORDER BY identity_sync.latest_synced_at, concat_ws('|', m.account, m.message_id)
        LIMIT %(limit)s
        )
    ) pdw_changed ON pdw_changed.account = t.account AND pdw_changed.message_id = t.message_id""",
    changed_join_anchor="clean_apple_messages t",
    event_id="concat_ws('|', t.account, t.message_id)",
    event_ts=_real_ts("t.message_at", "t.ingested_at"),
    ingest_ts=(
        "GREATEST("
        "t.ingested_at, "
        "COALESCE(att.attachment_ingest_ts, t.ingested_at), "
        "CASE WHEN t.is_from_me = 0 THEN contact_sync.latest_synced_at ELSE t.ingested_at END"
        ")"
    ),
    actor=(
        "COALESCE(NULLIF(t.sender_name, ''), "
        "CASE WHEN t.is_from_me = 1 THEN 'me' "
        "ELSE COALESCE(NULLIF(h.address, ''), NULLIF(t.handle_id, ''), '') END)"
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
        "NULLIF(t.sender_name, ''), NULLIF(h.address, ''), t.service)"
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
        "COALESCE(NULLIF(c.display_name, ''), NULLIF(c.chat_identifier, ''), NULLIF(t.sender_name, ''), NULLIF(h.address, ''), t.service)",
        "COALESCE(NULLIF(t.sender_name, ''), CASE WHEN t.is_from_me = 1 THEN 'me' ELSE COALESCE(NULLIF(h.address, ''), NULLIF(t.handle_id, ''), '') END)",
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
        "WHEN t.is_from_me = 1 THEN 'self' "
        "WHEN t.is_system_message = 1 OR t.is_service_message = 1 OR t.is_spam = 1 THEN 'noise' "
        "WHEN h.address ~ '^[0-9]{3,6}$' "
        "  OR (h.address <> '' AND h.address NOT LIKE '+%%' AND h.address NOT LIKE '%%@%%') THEN 'noise' "
        "WHEN c.display_name ~ '^[0-9]{3,6}$' OR c.chat_identifier ~ '^[0-9]{3,6}$' THEN 'noise' "
        "WHEN h.address ~ '^\\+1(800|833|844|855|866|877|888)' THEN 'noise' "
        "WHEN c.style = 45 OR COALESCE(roster.n, 0) <= 1 THEN "
        # A conversation needs his participation: two replies ever, or one
        # reply that is not drowned by a 20+ message broadcast stream.
        "  CASE WHEN (SELECT count(*) FROM ("
        "         SELECT 1 FROM @apple_message_chat_messages zc "
        "         JOIN @apple_messages z ON z.account = zc.account AND z.message_id = zc.message_id "
        "         WHERE zc.account = t.account AND zc.chat_id = cm.chat_id "
        "           AND z.is_from_me = 1 LIMIT 2) ow) >= 2 THEN 'direct' "
        "       WHEN (SELECT count(*) FROM ("
        "         SELECT 1 FROM @apple_message_chat_messages zc "
        "         JOIN @apple_messages z ON z.account = zc.account AND z.message_id = zc.message_id "
        "         WHERE zc.account = t.account AND zc.chat_id = cm.chat_id "
        "           AND z.is_from_me = 1 LIMIT 2) ow) = 1 "
        "        AND (SELECT count(*) FROM ("
        "         SELECT 1 FROM @apple_message_chat_messages zc "
        "         JOIN @apple_messages z ON z.account = zc.account AND z.message_id = zc.message_id "
        "         WHERE zc.account = t.account AND zc.chat_id = cm.chat_id "
        "           AND z.is_from_me = 0 LIMIT 20) iw) < 20 THEN 'direct' "
        "       ELSE 'noise' END "
        "WHEN COALESCE(roster.n, 0) <= 9 THEN 'direct' "
        "WHEN EXISTS (SELECT 1 FROM @apple_message_chat_messages zc "
        "             JOIN @apple_messages z ON z.account = zc.account AND z.message_id = zc.message_id "
        "             WHERE zc.account = t.account AND zc.chat_id = cm.chat_id "
        "               AND zc.message_date BETWEEN t.message_at - interval '6 hours' "
        "                                       AND t.message_at + interval '6 hours' "
        "               AND z.is_from_me = 1) THEN 'direct' "
        "ELSE 'cc' END"
    ),
    max_incremental_batches_per_run=1,
    refresh_hours=168,
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
    from_sql="""@whatsapp_messages t
    LEFT JOIN @whatsapp_chats c ON c.account = t.account AND c.chat_id = t.chat_id
    LEFT JOIN LATERAL (
        SELECT p.phone_jid
        FROM @whatsapp_chat_participants p
        WHERE p.account = t.account
          AND p.phone_jid <> ''
          AND (p.participant_jid = t.sender_jid OR p.lid_jid = t.sender_jid)
        ORDER BY p.ingested_at DESC, p.chat_id
        LIMIT 1
    ) sender_alias ON TRUE
    LEFT JOIN @whatsapp_contacts ct
      ON ct.account = t.account
     AND ct.jid = COALESCE(NULLIF(sender_alias.phone_jid, ''), t.sender_jid)
    LEFT JOIN @whatsapp_contacts chat_ct ON chat_ct.account = t.account AND chat_ct.jid = t.chat_id
    LEFT JOIN LATERAL (
        -- NULLIF keeps the no-roster case NULL (the priority CASE reads an
        -- unknown roster as "not a known-small group", not as size zero).
        SELECT NULLIF(count(*), 0) AS n
        FROM @whatsapp_chat_participants p
        WHERE p.account = t.account AND p.chat_id = t.chat_id
    ) roster ON TRUE
    LEFT JOIN LATERAL (
        SELECT
            string_agg(
                concat_ws(E'\n', NULLIF(m.filename, ''), NULLIF(e.text, '')),
                E'\n' ORDER BY m.media_type, e.updated_at DESC
            ) AS media_search_text,
            max(GREATEST(m.ingested_at, COALESCE(e.updated_at, m.ingested_at))) AS media_ingest_ts
        FROM @whatsapp_media_items m
        LEFT JOIN @file_attachment_enrichments e ON e.content_sha256 = m.content_sha256
        WHERE m.account = t.account AND m.chat_id = t.chat_id AND m.message_id = t.message_id
    ) media ON TRUE""",
    changed_join_sql="""JOIN (
        SELECT account, chat_id, message_id FROM @whatsapp_messages
        WHERE ingested_at >= %(watermark_ts)s
        UNION
        SELECT m.account, m.chat_id, m.message_id FROM @whatsapp_media_items m
        WHERE m.ingested_at >= %(watermark_ts)s
        UNION
        SELECT m.account, m.chat_id, m.message_id FROM @whatsapp_media_items m
        JOIN @file_attachment_enrichments e ON e.content_sha256 = m.content_sha256
        WHERE e.updated_at >= %(watermark_ts)s
    ) pdw_changed ON pdw_changed.account = t.account
        AND pdw_changed.chat_id = t.chat_id AND pdw_changed.message_id = t.message_id""",
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
        "WHEN t.is_from_me = 1 THEN 'self' "
        "WHEN c.chat_type = 'status' THEN 'noise' "
        "WHEN EXISTS (SELECT 1 FROM @whatsapp_contacts b "
        "             WHERE b.account = t.account AND b.jid = t.sender_jid "
        "               AND b.business_name <> '') THEN 'noise' "
        "WHEN t.body_text = '' AND COALESCE(t.media_type, '') IN ('', 'none', 'unknown') "
        "  AND (t.sender_jid = t.chat_id OR t.sender_jid = '' "
        "       OR COALESCE(c.chat_type, '') <> 'group' "
        "       OR COALESCE(NULLIF(ct.full_name, ''), NULLIF(ct.push_name, ''), "
        "                   NULLIF(t.push_name, '')) IS NULL) THEN 'noise' "
        "WHEN c.chat_type = 'group' OR t.chat_id LIKE '%%@g.us' THEN "
        "  CASE WHEN COALESCE(roster.n, 99) <= 5 THEN 'direct' "
        "       WHEN EXISTS (SELECT 1 FROM @whatsapp_messages z "
        "                    WHERE z.account = t.account AND z.chat_id = t.chat_id "
        "                      AND z.is_from_me = 1 "
        "                      AND z.message_at BETWEEN t.message_at - interval '6 hours' "
        "                                           AND t.message_at + interval '6 hours') THEN 'direct' "
        "       ELSE 'cc' END "
        "ELSE 'direct' END"
    ),
    refresh_hours=48,
)

_APPLE_NOTE_REVISION = _simple_adapter(
    name="apple_note_revision",
    source_table="apple_note_revisions",
    source="apple_notes",
    kind="note_edit",
    from_sql="@apple_note_revisions t",
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
    # A note revision only exists because Zach typed one: a deliberate 'self',
    # not an unexamined default.
    priority=TIMELINE_PRIORITY_SELF,
)

# ONE adapter for every voice source, over the mart that conforms them.
#
# There used to be two: `voice_memo` over base_apple_voice_memos.files and
# `alice_voice_recording` over base_alice_voice_recordings.recordings. Reading
# the raw tables meant every capability had to be built per source, and the
# second source got none of them -- no transcript, no summary, nothing in
# search but a filename. The mart answers the same questions for both, so a
# third voice source reaches the timeline with no adapter work at all.
#
# `event_id` is source-qualified because a recording_id is only unique inside
# its own source. That re-keys the Apple rows this adapter already published,
# which is exactly what `prune_sql` is for: the old ids no longer appear in the
# authoritative set and are removed over the next few runs.
_VOICE_RECORDING = _simple_adapter(
    name="voice_memo",
    source_table="marts_voice_memos_recordings",
    source="voice_memos",
    kind="voice_memo",
    from_sql="@marts_voice_memos_recordings t",
    event_id="concat_ws('|', t.source, t.account, t.recording_id)",
    # The mart translates the epoch sentinel to NULL, so these need real
    # fallbacks: they are the keyset expressions and must never be NULL.
    event_ts=f"COALESCE(t.recorded_at, t.ingested_at, {_EPOCH})",
    ingest_ts=(
        f"GREATEST(COALESCE(t.ingested_at, {_EPOCH}), "
        f"COALESCE(t.transcribed_at, {_EPOCH}), COALESCE(t.enriched_at, {_EPOCH}))"
    ),
    actor="'me'",
    title="t.title",
    snippet=_snippet("COALESCE(t.summary, '')"),
    context="t.account",
    source_pk=(
        "jsonb_build_object('source', t.source, 'account', t.account, "
        "'recording_id', t.recording_id)"
    ),
    metadata=(
        "jsonb_build_object("
        "'voice_source', t.source, "
        "'content_type', t.content_type, "
        "'size_bytes', t.size_bytes, "
        "'duration_seconds', t.duration_seconds, "
        "'deleted', t.is_deleted <> 0)"
    ),
    search_text=_search_concat(
        "t.title",
        "t.filename",
        "t.summary",
        "t.transcript",
        "t.participants_json",
        "t.action_items_json",
    ),
    prune_sql=(
        "SELECT concat_ws('|', t.source, t.account, t.recording_id) "
        "FROM @marts_voice_memos_recordings t"
    ),
    # He pressed record: a deliberate 'self', not an unexamined default.
    priority=TIMELINE_PRIORITY_SELF,
)

_DATE_ONLY = r"'^\d{4}-\d{2}-\d{2}$'"
_CALENDAR_START_TS = (
    f"COALESCE(NULLIF(t.start_at, {_EPOCH}), "
    f"CASE WHEN t.start_date ~ {_DATE_ONLY} THEN t.start_date::date::timestamptz ELSE NULL END, "
    "t.synced_at)"
)
# attendees_json is a text column holding the provider's array; it defaults to
# '' on rows written before an attendee list existed, and casting that to jsonb
# raises. Only cast what looks like an array, so one malformed row cannot take
# the whole adapter down.
_CALENDAR_ATTENDEES = (
    "CASE WHEN t.attendees_json LIKE '[%%' THEN t.attendees_json::jsonb ELSE '[]'::jsonb END"
)
# Google marks the calendar owner's own entry in the attendee list with
# "self": true, and the organizer's entry with "organizer": true. That pair is
# a far better identity signal than matching organizer_email against the
# account string, because it survives aliases and delegated calendars.
_CALENDAR_ORGANIZED_BY_ME = (
    "t.organizer_email ILIKE '%%' || t.account || '%%' "
    "  OR EXISTS (SELECT 1 FROM @gmail_sync_state self "
    "             WHERE self.account <> '' AND t.organizer_email ILIKE '%%' || self.account || '%%') "
    f"  OR EXISTS (SELECT 1 FROM jsonb_array_elements({_CALENDAR_ATTENDEES}) a "
    "             WHERE a->>'self' = 'true' AND a->>'organizer' = 'true')"
)
_CALENDAR_DECLINED_BY_ME = (
    f"EXISTS (SELECT 1 FROM jsonb_array_elements({_CALENDAR_ATTENDEES}) a "
    "        WHERE a->>'self' = 'true' AND a->>'responseStatus' = 'declined')"
)
# Rooms and equipment are attendees too; they must not inflate the headcount.
_CALENDAR_HUMAN_ATTENDEES = (
    f"(SELECT count(*) FROM jsonb_array_elements({_CALENDAR_ATTENDEES}) a "
    "  WHERE COALESCE(a->>'resource', '') <> 'true')"
)
# Prod's base_google_calendar.events holds 145 rows whose (account,
# calendar_id, event_id) bytes duplicate another row's, despite a valid unique
# primary key on exactly those columns (17,141 rows, 16,996 distinct keys under
# a collation-free bytea comparison, measured 2026-08-23). The source table
# needs a REINDEX; until then 30 of those pairs disagree on content, so with no
# tiebreak the timeline row for them flips with batch order and the content
# guard bumps seq — and re-chunks and re-embeds the event — on every sync.
#
# The dedup key is deliberately the bytea form of each column, not the columns
# themselves: whatever admitted the duplicates also makes Postgres' own text
# sort disagree with byte equality, so `DISTINCT ON (account, calendar_id,
# event_id)` returns all 17,141 rows unchanged. bytea has no collation, so the
# comparison is exact; measured, it collapses to exactly 16,996 rows, each the
# copy with the highest sync_version. Note this is also why the event_id is
# fine as it stands: nothing derived from those three columns — concatenation,
# escaping, or a hash — can separate two rows whose three columns are
# byte-identical.
_CALENDAR_KEY_BYTES = (
    "convert_to(account, 'UTF8'), convert_to(calendar_id, 'UTF8'), "
    "convert_to(event_id, 'UTF8')"
)
_CALENDAR_FROM = f"""(
        SELECT DISTINCT ON ({_CALENDAR_KEY_BYTES}) *
        FROM @calendar_events
        ORDER BY {_CALENDAR_KEY_BYTES}, sync_version DESC
    ) t"""

_CALENDAR_EVENT = _simple_adapter(
    name="calendar_event",
    source_table="calendar_events",
    source="calendar",
    kind="event",
    from_sql=_CALENDAR_FROM,
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
    # Who set the meeting up, not when it starts. The rule used to short-circuit
    # on `start_ts > synced_at` — every event that had not happened *at first
    # ingest* was filed 'cc' and, with no refresh window, froze there forever:
    # measured on prod 2026-08-23, `source = 'calendar' AND event_ts > now()`
    # held 0 'self' and 0 'direct' rows out of 2,487, so the documented
    # `priority IN ('self','direct','cc')` review surfaced nothing Zach had
    # organized. An event's tier is a property of who organized it and whether
    # he is going, which is stable, so the ordering below never consults the
    # clock. Subscribed feeds (yoga studios, holidays) and marketing-mail
    # invites (their descriptions carry the invisible-padding chars marketing
    # HTML uses) are noise; events any of Zach's identities organized are his
    # own actions, except Flighty's auto-created ✈-titled flight events; a
    # meeting he declined is not attention owed; an invite alongside a crowd is
    # real-people activity he is peripheral to; the rest are humans inviting him.
    priority=(
        "CASE "
        "WHEN t.is_deleted <> 0 OR t.status = 'cancelled' THEN 'noise' "
        "WHEN t.organizer_email ILIKE '%%group.calendar.google.com%%' "
        "  OR t.organizer_email ILIKE '%%holiday%%' THEN 'noise' "
        # Event platforms (lu.ma, Eventbrite, Meetup) send the invite from a
        # robot; the human who listed the event never addressed him.
        "WHEN t.organizer_email ~* '(calendar-invite@|no-?reply|invite@|eventbrite|"
        "lu\\.ma|meetup\\.com)' THEN 'noise' "
        "WHEN t.description LIKE '%%͏%%' OR t.description LIKE '%%­%%' THEN 'noise' "
        f"WHEN {_CALENDAR_ORGANIZED_BY_ME} THEN "
        "  CASE WHEN t.summary LIKE '✈%%' THEN 'noise' ELSE 'self' END "
        f"WHEN {_CALENDAR_DECLINED_BY_ME} THEN 'noise' "
        # 8 is where prod's future window splits cleanly: invitee counts run
        # 0-6 for the 1:1s and small syncs and 9+ for the recurring team-wide
        # meetings, with nothing in between.
        f"WHEN {_CALENDAR_HUMAN_ATTENDEES} > 8 THEN 'cc' "
        "ELSE 'direct' END"
    ),
    # This adapter had no convergence net at all, which is how the frozen
    # classification above went unnoticed. The refresh walk starts at the
    # far-future cursor, so it re-walks the not-yet-happened tail plus the last
    # two days every pass: any classification input that changes without
    # advancing this adapter's synced_at watermark past its cursor converges on
    # its own instead of freezing at first-ingest values.
    refresh_hours=48,
)

_DRIVE_FILE = _simple_adapter(
    name="drive_file",
    source_table="google_drive_files",
    source="google_drive",
    kind="file_change",
    from_sql="""@google_drive_files t
    LEFT JOIN LATERAL (
        SELECT
            string_agg(ft.text, E'\n' ORDER BY ft.extracted_at DESC) AS extracted_search_text,
            max(ft.extracted_at) AS extracted_ingest_ts
        FROM @google_drive_file_texts ft
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
    # else, and files I starred, keep me in the loop (cc); everything else
    # changing in Drive is other people's work that does not concern me,
    # which the tier definitions file as background -- nobody reaches Zach by
    # editing a document, so this adapter never emits 'direct'. Ownership
    # matches the account email inside owners_json (the raw metadata's
    # lastModifyingUser.me flag is not stored), and "edited by me" means the
    # last modifier is the owning identity's display name.
    priority=(
        "CASE "
        # Excluded files are the warehouse's own storage folders (attachment
        # blobs and export shards it writes to Drive) — machinery, not
        # activity. Sampling showed thousands of them per window at the noise
        # tier drowning real events.
        "WHEN t.is_excluded <> 0 THEN 'background' "
        "WHEN t.trashed <> 0 THEN 'noise' "
        # Google-Forms response uploads and shared-with-organizers intake
        # folders are pipeline traffic, not someone reaching Zach; shortcut
        # churn likewise.
        "WHEN t.folder_path LIKE '%%(File responses)%%' "
        "  OR t.folder_path ~* 'shared with|shared w/' THEN 'background' "
        "WHEN t.mime_type = 'application/vnd.google-apps.shortcut' THEN 'background' "
        "WHEN EXISTS (SELECT 1 FROM jsonb_array_elements(t.owners_json) o "
        "             WHERE o->>'emailAddress' ILIKE t.account "
        "               AND (t.last_modifying_user = '' OR t.last_modifying_user = o->>'displayName')) THEN 'self' "
        "WHEN EXISTS (SELECT 1 FROM jsonb_array_elements(t.owners_json) o "
        "             WHERE o->>'emailAddress' ILIKE t.account) THEN 'cc' "
        "WHEN t.starred <> 0 THEN 'cc' "
        "ELSE 'background' END"
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
    from_sql="""@photo_assets t
    LEFT JOIN LATERAL (
        SELECT
            string_agg(e.text, E'\n' ORDER BY e.updated_at DESC) AS enrichment_search_text,
            max(GREATEST(e.updated_at, e.ai_processed_at)) AS enrichment_ingest_ts
        FROM @file_attachment_enrichments e
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
    priority=TIMELINE_PRIORITY_SELF,
    refresh_hours=48,
)

def _contact_update_adapter(*, name: str, source_table: str) -> TimelineAdapter:
    return _simple_adapter(
        name=name,
        source_table=source_table,
        source="contacts",
        kind=name,
        from_sql=f"@{source_table} t",
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
            "t.notes", "t.emails", "t.phones", "t.addresses", "t.urls", "t.nicknames",
            # Digits-only phone variants: contacts store phones in display
            # formatting ('+1 (415) 516-3303'), while searches arrive as
            # whatever format the copy source used. Appending each number with
            # its punctuation stripped makes any formatting of the same number
            # a literal substring hit (search_text_exact strips the needle the
            # same way).
            "regexp_replace(t.primary_phone, '[^0-9]', '', 'g')",
            "regexp_replace(regexp_replace(t.phones::text, '[^0-9\",]', '', 'g'), '[\",]+', ' ', 'g')",
        ),
        # Contact-card churn is sync machinery, not traffic aimed at Zach.
        priority=TIMELINE_PRIORITY_BACKGROUND,
    )


_CONTACT_UPDATE = _contact_update_adapter(name="contact_update", source_table="contact_cards")
_APPLE_CONTACT_UPDATE = _contact_update_adapter(
    name="apple_contact_update",
    source_table="apple_contact_cards",
)

_WHOOP_CYCLE = _simple_adapter(
    name="whoop_cycle",
    source_table="whoop_cycles",
    source="whoop",
    kind="health_cycle",
    from_sql="@whoop_cycles t",
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
    # A day's strain score is a reading the strap computes about Zach, not
    # something he did: nobody creates a cycle, the device closes one when he
    # next falls asleep. Automated telemetry is 'noise' by the tier
    # definitions, and 'self' here put a machine-generated row per day into the
    # surface agents are told holds his own actions. His workouts stay 'self'
    # below, because he did those.
    priority=TIMELINE_PRIORITY_NOISE,
)

_WHOOP_RECOVERY = _simple_adapter(
    name="whoop_recovery",
    source_table="whoop_recoveries",
    source="whoop",
    kind="recovery",
    from_sql=(
        "@whoop_recoveries t LEFT JOIN @whoop_cycles c "
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
    # Purely a sensor computation (HRV, resting HR, SpO2) — no action of his
    # produced this row. See the cycle adapter above.
    priority=TIMELINE_PRIORITY_NOISE,
)

_WHOOP_SLEEP = _simple_adapter(
    name="whoop_sleep",
    source_table="whoop_sleeps",
    source="whoop",
    kind="sleep",
    from_sql="@whoop_sleeps t",
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
    # The scored sleep record, not the act of going to bed: WHOOP detects and
    # scores it on its own. See the cycle adapter above.
    priority=TIMELINE_PRIORITY_NOISE,
)

_WHOOP_WORKOUT = _simple_adapter(
    name="whoop_workout",
    source_table="whoop_workouts",
    source="whoop",
    kind="workout",
    from_sql="@whoop_workouts t",
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
    # Unlike the cycle/recovery/sleep scores above, a workout IS an action Zach
    # took — the strap only recorded it — so this one stays at the self tier.
    priority=TIMELINE_PRIORITY_SELF,
)

# The private WHOOP API (source `whoop_private`) re-fetches the same cycles,
# sleeps, recoveries and workouts the public API already publishes, at a much
# higher resolution. Exactly ONE of its tables becomes timeline events: the
# journal. Everything else it syncs is classified `detail` of the public
# base_whoop row it duplicates, because those events are already on the
# timeline via the four adapters above — a second adapter over the private
# copies would emit a duplicate of every health event Zach has ever recorded
# onto a 43M-row table.
_WHOOP_PRIVATE_JOURNAL = _simple_adapter(
    name="whoop_private_journal",
    source_table="whoop_private_journal_entries",
    source="whoop_private",
    kind="journal_entry",
    from_sql="@whoop_private_journal_entries t",
    event_id="concat_ws('|', t.account, t.day::text, t.question_id::text)",
    # `day` is the user-local calendar day the entry answers for, stored as a
    # DATE; there is no clock time on a journal answer. Anchor it at UTC
    # midnight the way finance_observation anchors `as_of`.
    event_ts="t.day::timestamp AT TIME ZONE 'UTC'",
    ingest_ts="t.synced_at",
    actor="'me'",
    title=(
        "concat('WHOOP journal: ', "
        "COALESCE(NULLIF(t.question_text, ''), t.question_id::text))"
    ),
    snippet=_snippet("t.answer"),
    context="t.account",
    source_pk=(
        "jsonb_build_object('account', t.account, 'day', t.day, "
        "'question_id', t.question_id)"
    ),
    metadata=(
        "jsonb_build_object("
        "'day', t.day, "
        "'question_id', t.question_id, "
        "'behavior_id', t.behavior_id, "
        "'answer', t.answer)"
    ),
    search_text=_search_concat("t.question_text", "t.answer"),
    # The one WHOOP table that is not sensor telemetry. Cycles, recoveries and
    # sleeps above are `noise` precisely because the strap computes them about
    # Zach with no action of his; a journal entry is the opposite — he opened
    # the app and answered the question himself, which is the definition of the
    # self tier (the same reasoning that keeps whoop_workout at `self`).
    priority=TIMELINE_PRIORITY_SELF,
)

_MUTATION = _simple_adapter(
    name="mutation",
    source_table="upstream_mutations",
    source="mutations",
    kind="mutation",
    from_sql="@upstream_mutations t",
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
    priority=TIMELINE_PRIORITY_BACKGROUND,
)

_MUTATION_REQUEST = _simple_adapter(
    name="mutation_request",
    source_table="upstream_mutation_requests",
    source="mutations",
    kind="mutation_request",
    from_sql="@upstream_mutation_requests t",
    event_id="t.id",
    event_ts="t.created_at",
    ingest_ts="t.updated_at",
    actor="COALESCE(NULLIF(t.requested_by, ''), 'warehouse')",
    title="t.title",
    snippet=_snippet("t.reason"),
    source_pk="jsonb_build_object('id', t.id)",
    metadata=("jsonb_build_object('status', t.status, 'has_error', t.error <> '')"),
    priority=TIMELINE_PRIORITY_BACKGROUND,
)

_ENRICHMENT_RUN = _simple_adapter(
    name="enrichment_run",
    source_table="agent_runs",
    source="warehouse",
    kind="enrichment_run",
    from_sql="@agent_runs t",
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
    priority=TIMELINE_PRIORITY_BACKGROUND,
)

_FINANCE_TRANSACTION = _simple_adapter(
    name="finance_transaction",
    source_table="finance_transactions",
    source="finance",
    kind="transaction",
    from_sql=(
        "@finance_transactions t "
        "LEFT JOIN @finance_accounts a ON a.account_id = t.account_id"
    ),
    event_id="t.transaction_id",
    event_ts="t.posted_at",
    ingest_ts="to_timestamp(t.sync_version / 1000000.0)",
    # derived_finance.transactions is reconciled against its sources every run
    # and re-keys transaction_id when the cross-source dedup changes its mind,
    # so superseded timeline rows accumulate with no upstream row left to
    # correct them. Measured 2026-08-23: 19,316 timeline rows against 14,372
    # live transactions -- 4,944 orphans (25.6%) that search still returned
    # next to their live replacements.
    prune_sql="SELECT t.transaction_id FROM @finance_transactions t",
    actor="COALESCE(NULLIF(t.merchant, ''), t.description)",
    title="COALESCE(NULLIF(t.merchant, ''), NULLIF(t.description, ''), 'Transaction')",
    snippet="concat(t.amount::text, ' ', t.currency)",
    context="concat_ws(' · ', NULLIF(a.institution, ''), NULLIF(a.name, ''))",
    source_pk="jsonb_build_object('transaction_id', t.transaction_id)",
    metadata=(
        "jsonb_build_object("
        "'account_id', t.account_id, "
        "'amount', t.amount, "
        "'currency', t.currency, "
        "'pending', t.pending <> 0, "
        "'source', t.source, "
        "'side', a.side)"
    ),
    search_text=_search_concat(
        "t.description", "t.merchant", "t.amount", "t.currency", "a.name", "a.institution"
    ),
    # The ledger mixes two very different things under one constant 'self':
    # money Zach chose to move (a card swipe, a Venmo payment) and money that
    # moved because a machine was scheduled to move it. The second kind is
    # automated traffic by the tier definitions, and it is the bulk of the
    # table — 3,981 of 14,372 rows on prod (2026-08-23) are brokerage cash
    # sweeps into and out of the core FDIC/money-market position, recurring
    # auto-invest allocations, collateral moves, interest, dividends, fees and
    # rebates, autopay, payroll and bare ACH transfers. Matching is on the
    # provider's own label because that is the only description these carry;
    # `fee` is anchored on word boundaries so a coffee shop is not a fee.
    priority=(
        "CASE WHEN COALESCE(NULLIF(t.merchant, ''), t.description) ~* "
        "  '(\\minterest\\M|dividend|\\mfees?\\M|\\mrebate\\M|core account|"
        "fdic insured deposit|money market|collateral movement|bulk equity order|"
        "\\mrecurring\\M|auto.?pay|automatic payment|payroll|direct deposit|"
        "\\mach (debit|credit|deposit|withdrawal)\\M)' "
        "  THEN 'noise' "
        "ELSE 'self' END"
    ),
)

_FINANCE_OBSERVATION = _simple_adapter(
    name="finance_observation",
    source_table="finance_observations",
    source="finance",
    kind="balance_observation",
    from_sql=(
        "@finance_observations t "
        "LEFT JOIN @finance_accounts a ON a.account_id = t.account_id"
    ),
    event_id="concat_ws('|', t.account_id, t.as_of::text, t.kind, t.source)",
    event_ts="t.as_of::timestamp AT TIME ZONE 'UTC'",
    ingest_ts="to_timestamp(t.sync_version / 1000000.0)",
    actor="'me'",
    title="concat(COALESCE(NULLIF(a.name, ''), t.account_id), ' ', t.kind)",
    snippet="concat(t.value::text, ' ', t.currency)",
    context="COALESCE(a.institution, '')",
    source_pk=(
        "jsonb_build_object('account_id', t.account_id, 'as_of', t.as_of, "
        "'kind', t.kind, 'source', t.source)"
    ),
    metadata=(
        "jsonb_build_object("
        "'account_id', t.account_id, "
        "'value', t.value, "
        "'currency', t.currency, "
        "'source', t.source, "
        "'side', a.side)"
    ),
    search_text=_search_concat("a.name", "a.institution", "t.kind", "t.value", "t.currency"),
    # A balance observation is not an event: nobody did anything at this
    # timestamp. The row exists because the finance_ledger asset snapshots
    # every live account once a day (Plaid keeps only current state, so this
    # table IS the balance history) — the warehouse's own machinery writing its
    # own derived history, which is exactly the background tier. Classifying it
    # 'self' put one machine-written row per account per day into the surface
    # agents are told holds Zach's own actions.
    priority=TIMELINE_PRIORITY_BACKGROUND,
)

_MANUAL_FINANCE_DOCUMENT = _simple_adapter(
    name="manual_finance_document",
    source_table="manual_finance_documents",
    source="finance",
    kind="document",
    from_sql="""@manual_finance_documents t
    LEFT JOIN LATERAL (
        SELECT e.document_type, e.institution, e.account_name_hint, e.period_start,
               e.period_end, e.currency, e.closing_balance, e.summary, e.created_at
        FROM @manual_finance_extractions e
        WHERE e.content_sha256 = t.content_sha256
        ORDER BY e.created_at DESC LIMIT 1
    ) ex ON TRUE""",
    event_id="concat_ws('|', t.source, t.account, t.source_native_id)",
    event_ts=(
        "COALESCE(ex.period_end::timestamp AT TIME ZONE 'UTC', "
        "t.file_modified_at, t.ingested_at)"
    ),
    ingest_ts="GREATEST(t.ingested_at, COALESCE(ex.created_at, t.ingested_at))",
    actor="'me'",
    title="t.filename",
    snippet=_snippet("COALESCE(ex.summary, '')"),
    context="COALESCE(NULLIF(ex.institution, ''), t.original_path)",
    source_pk=(
        "jsonb_build_object('source', t.source, 'account', t.account, "
        "'source_native_id', t.source_native_id)"
    ),
    metadata=(
        "jsonb_build_object("
        "'document_type', ex.document_type, "
        "'institution', ex.institution, "
        "'account_name_hint', ex.account_name_hint, "
        "'period_start', ex.period_start, "
        "'period_end', ex.period_end, "
        "'currency', ex.currency, "
        "'closing_balance', ex.closing_balance, "
        "'mime_type', t.mime_type, "
        "'size_bytes', t.size_bytes, "
        "'deleted', t.is_deleted <> 0)"
    ),
    search_text=_search_concat(
        "t.filename", "t.original_path", "ex.institution", "ex.account_name_hint", "ex.summary"
    ),
    # Every row here is a document Zach chose to collect and upload, so the
    # upload is his action even though the statement itself is machine-issued.
    priority=TIMELINE_PRIORITY_SELF,
)


# An opening prompt another program wrote. Every orchestrator (paseo
# subagents, runbook fan-outs, the daily run-sheet job, /command invocations)
# writes a role brief -- "You are auditing ...", "Research task, web only",
# "Reply with ONLY minified JSON" -- and Zach does not talk to an agent that
# way. This is judged on the prompt's SHAPE because the entrypoint cannot tell
# them apart: paseo launches the sessions Zach types into through the same
# `sdk-cli` entrypoint as the ones it spawns for itself, and classifying by
# entrypoint filed 383 of 390 claude_code sessions in a fortnight as
# background, "how did the freight break down for this?" included (measured on
# prod 2026-08-26).
_AGENT_ORCHESTRATED_PROMPT_PATTERN = (
    "'^\\s*(you are |you''re |your (task|job|goal|mission|role)\\M|"
    "(pure |external |live )*(public )?web research|research task|"
    "(task|goal|context|mission|objective)\\s*[:—-]|\\*\\*(task|goal|context|objective)|"
    "<command-name>|\\x7b\"|small install task|work in /|in the repo (at )?/|"
    "reply with (only|exactly)|respond with only|cleanup task)'"
)

# Harness-injected `role = 'user'` turns. Every agent CLI opens a transcript by
# feeding the model an environment/instructions block through the user channel,
# so the literal first user row is usually not something Zach typed. Taking it
# as the session's first prompt cost codex every 'self' classification it
# should have had: its `<recommended_plugins>` preamble is byte-identical
# across sessions, so the "same long opening prompt in >= 4 sessions is a
# scheduled routine" rule fired on all of them - measured on prod 2026-08-23,
# codex emitted 0 'self' rows over the previous 7 days (28 codex_cli_rs, 6
# Codex Desktop and 4 codex-tui sessions, all 'background') while every sibling
# agent source emitted 'self'. It also made those sessions' titles read
# "<recommended_plugins> Here is a list of plugins...". The patterns are prefix
# matches taken from the corpus, not guesses.
_AGENT_INJECTED_PREAMBLE = (
    "(e2.text LIKE '<recommended_plugins>%%' "
    " OR e2.text LIKE '<environment_context>%%' "
    " OR e2.text LIKE '<codex_internal_context%%' "
    " OR e2.text LIKE '<local-command-caveat>%%' "
    " OR e2.text LIKE '# AGENTS.md instructions for %%')"
)


def _agent_session_adapter() -> TimelineAdapter:
    """Session-level roll-up over marts_ai_conversations.events.

    One timeline row per session/conversation (Claude Code, Codex, OpenClaw,
    Claude Desktop, ChatGPT — the row's ``source`` is the per-session source
    value), matching the marts_ai_conversations.sessions roll-up. Individual transcript
    lines are surfaced through the session's detail view, not as separate
    timeline entries.

    The GROUP BY aggregates only cheap scalars; first/last text-ish fields
    (title, first prompt, model, cwd, ...) come from per-session LATERAL
    probes on each source table's (session_id, seq) index. The session row's
    search document carries only those headline fields — transcript content
    is indexed per turn by the agent_session_turn adapter below.
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
            -- Headline fields only: the transcript itself is indexed at turn
            -- granularity by the agent_session_turn adapter, where BM25
            -- relevance is not diluted across a whole session and the search
            -- preview lands on the matched turn instead of the transcript head.
            concat_ws(E'\n', NULLIF(st.session_title, ''), NULLIF(fp.text, ''),
                      NULLIF(cw.cwd, ''), NULLIF(gb.git_branch, ''), NULLIF(ru.repo_url, '')) AS search_text,
            s.ingest_ts AS ingest_ts,
            -- Interactive vs background (benchmark-tuned, sampling/ 2026-07,
            -- re-audited 2026-08-26). chatgpt/claude_desktop are always human
            -- conversations, and some sync as a header row with zero user
            -- events. Cron/inter-session prompts, programmatic entrypoints,
            -- orchestrator-shaped opening prompts, zero-user-turn transcripts,
            -- and sidechain-only subagent transcripts are machinery.
            CASE WHEN s.source IN ('chatgpt', 'claude_desktop') THEN {TIMELINE_PRIORITY_SELF}
                 WHEN COALESCE(NULLIF(st.session_title, ''), fp.text, '') LIKE '[cron:%%'
                   OR COALESCE(fp.text, '') LIKE '[cron:%%'
                   OR COALESCE(fp.text, '') LIKE '[Inter-session message]%%'
                   OR COALESCE(NULLIF(st.session_title, ''), fp.text, '') LIKE '[Subagent Context]%%'
                   OR COALESCE(fp.text, '') LIKE '[Subagent Context]%%'
                   THEN {TIMELINE_PRIORITY_BACKGROUND}
                 WHEN s.entrypoint IN ('codex_exec', 'zrl-claw')
                   THEN {TIMELINE_PRIORITY_BACKGROUND}
                 WHEN fp.text ~* {_AGENT_ORCHESTRATED_PROMPT_PATTERN}
                   THEN {TIMELINE_PRIORITY_BACKGROUND}
                 WHEN s.user_event_count = 0 THEN {TIMELINE_PRIORITY_BACKGROUND}
                 WHEN s.non_sidechain_count = 0 THEN {TIMELINE_PRIORITY_BACKGROUND}
                 -- fp now skips harness-injected preambles, so a NULL here
                 -- means the transcript has user rows but nobody ever typed
                 -- one: a programmatic run, whatever its entrypoint says.
                 WHEN fp.text IS NULL THEN {TIMELINE_PRIORITY_BACKGROUND}
                 -- The same long opening prompt recurring across sessions is a
                 -- scheduled routine (daily monitor runs), not a human typing.
                 WHEN length(COALESCE(fp.text, '')) > 40 AND (
                      SELECT count(*) FROM (
                          SELECT 1 FROM @ai_conversation_events rep
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
            FROM @ai_conversation_events e
            {{changed_join}}
            GROUP BY e.source, e.session_id
        ) s
        LEFT JOIN LATERAL (
            SELECT e2.session_title FROM @ai_conversation_events e2
            WHERE e2.source = s.source AND e2.session_id = s.session_id AND e2.session_title != ''
            ORDER BY e2.seq LIMIT 1
        ) st ON TRUE
        LEFT JOIN LATERAL (
            SELECT e2.text FROM @ai_conversation_events e2
            WHERE e2.source = s.source AND e2.session_id = s.session_id
              AND e2.role = 'user' AND e2.text != ''
              AND NOT {_AGENT_INJECTED_PREAMBLE}
            ORDER BY e2.seq LIMIT 1
        ) fp ON TRUE
        LEFT JOIN LATERAL (
            SELECT e2.model FROM @ai_conversation_events e2
            WHERE e2.source = s.source AND e2.session_id = s.session_id AND e2.model != ''
            ORDER BY e2.seq DESC LIMIT 1
        ) md ON TRUE
        LEFT JOIN LATERAL (
            SELECT e2.cwd FROM @ai_conversation_events e2
            WHERE e2.source = s.source AND e2.session_id = s.session_id AND e2.cwd != ''
            ORDER BY e2.seq LIMIT 1
        ) cw ON TRUE
        LEFT JOIN LATERAL (
            SELECT e2.git_branch FROM @ai_conversation_events e2
            WHERE e2.source = s.source AND e2.session_id = s.session_id AND e2.git_branch != ''
            ORDER BY e2.seq DESC LIMIT 1
        ) gb ON TRUE
        LEFT JOIN LATERAL (
            SELECT e2.repo_url FROM @ai_conversation_events e2
            WHERE e2.source = s.source AND e2.session_id = s.session_id AND e2.repo_url != ''
            ORDER BY e2.seq LIMIT 1
        ) ru ON TRUE
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
            FROM @ai_conversation_events
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
        source_table="ai_conversation_events",
        source="agent_sessions",
        kind="agent_session",
        backfill_sql=backfill_sql,
        incremental_sql=incremental_sql,
        max_ingest_sql="SELECT max(ingested_at) FROM @ai_conversation_events",
        # The real rule, verbatim in intent from the rollup SELECT above: a
        # session Zach actually drove is `self`; one whose opening user turn
        # repeats near-identically across >=4 sessions is an automated harness
        # firing the same prompt, so it is `background`. This field is metadata
        # (the rollup computes the value), which is exactly why it drifted into
        # a placeholder that named no rule at all -- the registration test only
        # checks that it is non-empty.
        priority_expression=(
            f"CASE WHEN <opening user turn is orchestrator-shaped "
            f"({_AGENT_ORCHESTRATED_PROMPT_PATTERN}) or repeats across >=4 sessions> "
            f"THEN {TIMELINE_PRIORITY_BACKGROUND} ELSE {TIMELINE_PRIORITY_SELF} END"
        ),
        batch_size=10000,
    )


_AGENT_SESSION = _agent_session_adapter()


def _agent_session_turn_adapter() -> TimelineAdapter:
    """One timeline row per user/assistant turn of every agent session.

    The session roll-up above is the browse/priority surface; these rows are
    the SEARCH surface for transcript content. One row per whole session
    diluted BM25 relevance across arbitrarily long transcripts and the hit
    preview routinely missed the matched turn — measured as a primary reason
    agents fell back to raw-table scans. Turn rows are priority 'background'
    so they never crowd the browse/priority views; their `context` is
    '<source>|<session_id>', so timeline.context(ref) pages the surrounding
    turns of the same session.
    """
    select = f"""
        SELECT
            concat_ws('|', e.source, e.session_id, e.seq::text) AS event_id,
            e.source AS source,
            'agent_turn' AS kind,
            e.occurred_at AS event_ts,
            {_EPOCH} AS end_ts,
            COALESCE(e.role, '') AS actor,
            COALESCE(left(e.text, {TIMELINE_TITLE_CHARS}), '') AS title,
            COALESCE(left(e.text, {TIMELINE_SNIPPET_CHARS}), '') AS snippet,
            concat_ws('|', e.source, e.session_id) AS context,
            (jsonb_build_object('source', e.source, 'session_id', e.session_id,
                                'event_uuid', e.event_uuid))::text AS source_pk,
            (jsonb_build_object('seq', e.seq, 'role', e.role, 'device', e.device,
                                'session_title', e.session_title))::text AS metadata,
            concat_ws(E'\n', NULLIF(e.session_title, ''), NULLIF(e.text, '')) AS search_text,
            e.ingested_at AS ingest_ts,
            {TIMELINE_PRIORITY_BACKGROUND} AS priority
        FROM @ai_conversation_events e
        WHERE e.role IN ('user', 'assistant') AND e.text != ''
    """
    backfill_sql = f"""
        {select}
          AND e.occurred_at <= %(cursor_ts)s
          AND (e.occurred_at, concat_ws('|', e.source, e.session_id, e.seq::text))
              < (%(cursor_ts)s, %(cursor_id)s)
        ORDER BY 4 DESC, 1 DESC
        LIMIT %(limit)s
    """
    incremental_sql = f"""
        {select}
          AND e.ingested_at >= %(watermark_ts)s
          AND (e.ingested_at, concat_ws('|', e.source, e.session_id, e.seq::text))
              > (%(watermark_ts)s, %(watermark_id)s)
        ORDER BY 13 ASC, 1 ASC
        LIMIT %(limit)s
    """
    return TimelineAdapter(
        name="agent_session_turn",
        source_table="ai_conversation_events",
        source="agent_sessions",
        kind="agent_turn",
        backfill_sql=backfill_sql,
        incremental_sql=incremental_sql,
        max_ingest_sql=(
            "SELECT max(ingested_at) FROM @ai_conversation_events "
            "WHERE role IN ('user', 'assistant') AND text != ''"
        ),
        priority_expression=TIMELINE_PRIORITY_BACKGROUND,
        batch_size=5000,
    )


_AGENT_SESSION_TURN = _agent_session_turn_adapter()

TIMELINE_ADAPTERS: tuple[TimelineAdapter, ...] = (
    _GMAIL_EMAIL,
    _SLACK_MESSAGE,
    _SLACK_FILE,
    _APPLE_MESSAGE,
    _WHATSAPP_MESSAGE,
    _AGENT_SESSION,
    _AGENT_SESSION_TURN,
    _APPLE_NOTE_REVISION,
    _VOICE_RECORDING,
    _CALENDAR_EVENT,
    _DRIVE_FILE,
    _PHOTO,
    _CONTACT_UPDATE,
    _APPLE_CONTACT_UPDATE,
    _WHOOP_CYCLE,
    _WHOOP_RECOVERY,
    _WHOOP_SLEEP,
    _WHOOP_WORKOUT,
    _WHOOP_PRIVATE_JOURNAL,
    _FINANCE_TRANSACTION,
    _FINANCE_OBSERVATION,
    _MANUAL_FINANCE_DOCUMENT,
    _MUTATION,
    _MUTATION_REQUEST,
    _ENRICHMENT_RUN,
)


# Adapters that no longer exist, and whose rows must therefore go.
#
# timeline.events is keyed (adapter, event_id), so removing an adapter from
# TIMELINE_ADAPTERS does not remove its rows -- it strands them. Nothing else
# in the engine can ever correct them: `prune_sql` only reconciles an adapter
# that still runs. A retired adapter's rows would keep answering searches with
# a duplicate of whatever superseded them, silently and forever.
#
# `alice_voice_recording` was folded into `voice_memo`, which now reads
# marts_voice_memos.recordings and emits both sources.
RETIRED_TIMELINE_ADAPTERS: tuple[str, ...] = ("alice_voice_recording",)


def adapter_by_name(name: str) -> TimelineAdapter:
    for adapter in TIMELINE_ADAPTERS:
        if adapter.name == name:
            return adapter
    raise KeyError(name)


def adapter_definition_signature(adapter: TimelineAdapter) -> str:
    """Fingerprint of everything that shapes an adapter's normalized rows.

    Stored per adapter in timeline_sync_state; when the definition changes
    (new search-document fields, reclassified priority, renamed context) the
    engine resets that adapter's backfill cursor so already-synced history
    re-walks and converges to the new shape. The content-guarded upsert makes
    an unchanged row a no-op, so a reset costs reads, not rewrites.
    """
    # The fail-loud priority rollout removes only the generated
    # COALESCE(..., 'cc') safety bug. That wrapper was engine behavior, not an
    # adapter author's classification rule, so `_simple_adapter` retains its
    # former generated SQL solely here. Existing production signatures remain
    # byte-for-byte stable; a real change to `priority_expression` still
    # changes these compatibility strings and resets only that adapter.
    backfill_sql = adapter.signature_backfill_sql or adapter.backfill_sql
    incremental_sql = adapter.signature_incremental_sql or adapter.incremental_sql
    payload = "\n".join(
        [
            adapter.name,
            adapter.source_table,
            adapter.source,
            adapter.kind,
            backfill_sql,
            incremental_sql,
            adapter.max_ingest_sql,
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


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
    "apple_contact_cards": _events(),
    "contact_sync_state": _state("contacts sync cursor"),
    # Voice. One adapter reads marts_voice_memos.recordings, which conforms
    # every voice source, so each source-owned raw table is what actually feeds
    # the timeline -- the same shape as AI conversations. The three derived
    # tables now serve every source (they are keyed by source), so they are
    # detail of the domain rather than of one source's raw table.
    "apple_voice_memos_files": _events(),
    "apple_voice_memos_transcription_runs": _detail(
        "apple_voice_memos_files", "transcription attempts for every voice source"
    ),
    "apple_voice_memos_transcript_segments": _detail(
        "apple_voice_memos_files", "speaker-labelled utterances for every voice source"
    ),
    "apple_voice_memos_enrichments": _detail(
        "apple_voice_memos_files", "agent enrichment for every voice source"
    ),
    # Alice Voice Recordings
    "alice_voice_recordings": _events(),
    "alice_voice_recording_artifacts": _detail("alice_voice_recordings"),
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
    # AI conversations. The adapter reads the marts_ai_conversations.events
    # union view and emits one row per session, so each source-owned raw table
    # is what actually feeds the timeline.
    "chatgpt_events": _events("one session roll-up row plus one row per user/assistant turn"),
    "claude_desktop_events": _events("one session roll-up row plus one row per user/assistant turn"),
    "claude_code_events": _events("one session roll-up row plus one row per user/assistant turn"),
    "codex_events": _events("one session roll-up row plus one row per user/assistant turn"),
    "openclaw_events": _events("one session roll-up row plus one row per user/assistant turn"),
    "pi_events": _events("one session roll-up row plus one row per user/assistant turn"),
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
    "slack_file_fingerprints": _detail(
        "slack_files",
        "perceptual-hash link (file -> content sha) behind 'who sent this image?'",
    ),
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
    # WHOOP private API (app.whoop.com's own endpoints). It syncs the SAME
    # cycles/sleeps/recoveries/workouts the public API does, at a far higher
    # resolution, so every one of those tables is `detail` of the public row it
    # duplicates: those events already reach the timeline through the four
    # base_whoop adapters, and emitting them a second time would double every
    # health event Zach has. Only the journal — which the public API does not
    # expose at all — is an events table here.
    "whoop_private_cycles": _detail(
        "whoop_cycles", "high-resolution copy of the public cycle (strain components, sleep need)"
    ),
    "whoop_private_sleeps": _detail(
        "whoop_sleeps", "high-resolution copy of the public sleep (stage durations, debt, projections)"
    ),
    "whoop_private_recoveries": _detail(
        "whoop_recoveries", "high-resolution copy of the public recovery (HRV/RHR components)"
    ),
    "whoop_private_workouts": _detail(
        "whoop_workouts", "high-resolution copy of the public workout (zone durations, GPS)"
    ),
    "whoop_private_sleep_events": _detail(
        "whoop_private_sleeps", "the hypnogram: one row per LIGHT/REM/SWS/DISTURBANCE stage"
    ),
    "whoop_private_heart_rate_samples": _detail(
        "whoop_cycles",
        "the one per-6-second heart rate series, every hour of every day, under the day's cycle",
    ),
    "whoop_private_documents": _detail(
        "whoop_cycles",
        "Tier-2 raw UI payloads (trends, stress, cardio details, sleep deep-dive) kept as raw_json",
    ),
    "whoop_private_journal_entries": _events(
        "one event per journal question Zach answered; no public-API equivalent"
    ),
    "whoop_private_sports": _entity("sport catalog resolving a workout's sport_id"),
    "whoop_private_sync_state": _state("per-collection private-API scan watermark"),
    "slack_sessions": _state("captured Slack client session credential (xoxc token + `d` cookie)"),
    "whoop_private_sessions": _state("rotating private-API browser session credential"),
    # Plaid finance data is queryable through base_plaid.* and marts_finance.* but
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
    # Finance ledger. Raw Plaid rows remain source state; their deduplicated
    # ledger transactions and point-in-time observations are timeline events.
    "finance_accounts": _entity("logical account/asset/liability dimension"),
    "finance_account_links": _state("source-account → ledger-account resolution audit"),
    "finance_observations": _events("append-only balance and valuation history"),
    "finance_transactions": _events("one event per deduplicated real-world transaction"),
    "finance_transaction_links": _detail("finance_transactions", "source-row resolution audit"),
    "finance_security_transactions": _detail(
        "finance_transactions", "per-security trade detail behind a brokerage flow"
    ),
    "finance_security_transaction_links": _detail(
        "finance_security_transactions", "source-row resolution audit"
    ),
    "finance_tax_lots": _detail("finance_security_transactions", "FIFO holding lots"),
    "manual_finance_documents": _events("uploaded statements and financial records"),
    "manual_finance_extractions": _detail("manual_finance_documents", "structured extraction history"),
    # Receipts. One transaction-first research result explains an existing
    # finance event; it is not a second timeline event.
    "receipt_transaction_receipts": _detail(
        "finance_transactions",
        "single-operation receipt search, extraction, and match result",
    ),
    # Upstream mutations (the warehouse acting on the world)
    "upstream_mutations": _events(),
    "upstream_mutation_requests": _events(),
    "upstream_mutation_events": _detail("upstream_mutations"),
    "upstream_mutation_request_events": _detail("upstream_mutation_requests"),
    "push_devices": _state("iOS app devices registered for push notifications"),
    # Search surfaces
    "search_schema_state": _state("search_text DDL signature cache"),
    "search_chunks": _state("derived retrieval chunks behind timeline.search_hybrid()"),
    "search_chunk_embeddings": _state("per-content-sha chunk embeddings (semantic search)"),
    "search_chunk_sync_state": _state("chunk builder's timeline seq watermark"),
    # Pipeline freshness (personal_data_warehouse/pipeline_health.py): a
    # measurement of the warehouse's own pipelines, not activity in it.
    "pipeline_health": _state("per-pipeline freshness and health snapshot"),
    "pipeline_table_freshness": _state("per-table freshness snapshot"),
    "mart_view_health": _state("per-marts-view input freshness, non-empty probe, and definition hash"),
    "collation_health": _state("collation drift baselines and unique-index divergence findings"),
    "pgbackrest_health": _state("backup posture: does a restorable backup exist, is WAL shipping"),
    "search_health": _state("search chunk and embedding convergence heartbeats"),
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
    "push_devices",
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


def timeline_upsert_sql(*, table_ref: str, sequence_ref: str) -> str:
    """Build the timeline upsert. Both refs must already be schema-qualified."""
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
    reconciled_rows: int = 0
    pruned_rows: int = 0
    backfill_done: bool = False
    error: str = ""


@dataclass
class _AdapterState:
    backfill_cursor_ts: datetime
    backfill_cursor_id: str
    backfill_done: bool
    watermark_ts: datetime
    watermark_id: str
    last_reconcile_at: datetime = BACKFILL_EPOCH


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
        for adapter in self._adapters:
            if not adapter.priority_expression.strip():
                raise ValueError(
                    f"timeline adapter {adapter.name!r} must declare a priority expression"
                )
        self._batch_size = batch_size
        self._source_conn: Any = None
        self._dest_conn: Any = None

    # -- connections ---------------------------------------------------------

    def _search_path_sql(self, namespace: str) -> str:
        parts = ['"' + schema.replace('"', '""') + '"' for schema in physical_schema_names(namespace=namespace, include_hidden=True)]
        parts.append("public")
        return "SET search_path TO " + ", ".join(parts)

    def _source_sql(self, sql: str) -> str:
        return expand_relations(sql, namespace=self._source_schema)

    def _dest_sql(self, sql: str) -> str:
        return expand_relations(sql, namespace=self._dest_schema)

    def _qualified_regclass(self, logical_name: str, *, namespace: str) -> str:
        return expand_relations('@' + logical_name, namespace=namespace)

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
                           watermark_ingest_ts, watermark_event_id, adapter_signature,
                           last_reconcile_at
                    FROM @timeline_sync_state
                    WHERE adapter = %s
                    """
                ),
                (adapter.name,),
            )
            row = cursor.fetchone()
        if row is not None:
            state = _AdapterState(
                backfill_cursor_ts=row[0],
                backfill_cursor_id=row[1],
                backfill_done=bool(row[2]),
                watermark_ts=row[3],
                watermark_id=row[4],
                last_reconcile_at=row[6],
            )
            stored_signature = row[5]
            if stored_signature != adapter_definition_signature(adapter):
                # The adapter's normalization SQL changed since this cursor was
                # written: already-synced history is stale (old search document,
                # old priority rules, ...). Restart the newest-first backfill so
                # every row re-walks; the incremental watermark stays put (new
                # source rows keep flowing while the re-walk converges), and the
                # content-guarded upsert keeps unchanged rows free.
                logger.info(
                    "timeline adapter %s definition changed (or signature unknown); "
                    "resetting backfill cursor",
                    adapter.name,
                )
                state.backfill_cursor_ts = BACKFILL_CURSOR_START
                state.backfill_cursor_id = ""
                state.backfill_done = False
                self._save_state(adapter, state)
            return state
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
                    INSERT INTO @timeline_sync_state (
                        adapter, backfill_cursor_event_ts, backfill_cursor_event_id, backfill_done,
                        watermark_ingest_ts, watermark_event_id, last_run_at, last_error, updated_at,
                        adapter_signature
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, now(), %s, now(), %s)
                    ON CONFLICT (adapter) DO UPDATE SET
                        backfill_cursor_event_ts = EXCLUDED.backfill_cursor_event_ts,
                        backfill_cursor_event_id = EXCLUDED.backfill_cursor_event_id,
                        backfill_done = EXCLUDED.backfill_done,
                        watermark_ingest_ts = EXCLUDED.watermark_ingest_ts,
                        watermark_event_id = EXCLUDED.watermark_event_id,
                        last_run_at = now(),
                        last_error = EXCLUDED.last_error,
                        updated_at = now(),
                        adapter_signature = EXCLUDED.adapter_signature,
                        last_reconcile_at = EXCLUDED.last_reconcile_at
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
                    adapter_definition_signature(adapter),
                ),
            )

    def _bump_counter(self, adapter: TimelineAdapter, column: str, amount: int) -> None:
        if amount <= 0:
            return
        assert column in ("backfill_rows", "incremental_rows")
        with self._dest_conn.cursor() as cursor:
            cursor.execute(
                self._dest_sql(
                    f"UPDATE @timeline_sync_state SET {column} = {column} + %s, updated_at = now() "
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
        invalid = [
            (str(row[0]) if row else "<missing event_id>", row[13] if len(row) > 13 else None)
            for row in rows
            if len(row) != len(TIMELINE_NORMALIZED_COLUMNS)
            or row[13] not in TIMELINE_PRIORITY_LABELS
        ]
        if invalid:
            event_id, priority = invalid[0]
            raise TimelinePriorityError(
                f"timeline adapter {adapter.name!r} emitted invalid priority {priority!r} "
                f"for event {event_id!r}; expected one of {', '.join(TIMELINE_PRIORITY_LABELS)}"
            )
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
                    table_ref=self._qualified_regclass("timeline_events", namespace=self._dest_schema),
                    sequence_ref=self._qualified_regclass(
                        "timeline_events_seq", namespace=self._dest_schema
                    ),
                ),
                values,
                template=_TIMELINE_INSERT_TEMPLATE,
                page_size=1000,
            )

    def _batch_limit(self, adapter: TimelineAdapter) -> int:
        return self._batch_size or adapter.batch_size

    def _run_incremental(self, adapter: TimelineAdapter, state: _AdapterState, deadline: float | None) -> int:
        total = 0
        batches = 0
        # Re-reading is bounded by the lag WINDOW, not by the budget above, but
        # a pathological source (every row restamped inside the window) must
        # still terminate.
        stale_batches = 0
        max_stale_batches = 8
        limit = self._batch_limit(adapter)
        # Restart a little behind the stored watermark so a row that committed
        # after the previous pass read -- but carries an earlier ingest stamp --
        # is still reachable. See TimelineAdapter.incremental_lag_hours.
        lag = timedelta(hours=adapter.incremental_lag_hours)
        # Rows at or below this are re-reads of the lag window, not new work.
        # Counting them would make every converged run report progress it did
        # not make, and `incremental_rows` is what tells an operator whether an
        # adapter is actually moving.
        start_ts, start_id = state.watermark_ts, state.watermark_id
        if lag:
            cursor_ts = max(state.watermark_ts - lag, datetime(1970, 1, 1, tzinfo=UTC))
            cursor_id = ""
        else:
            cursor_ts, cursor_id = state.watermark_ts, state.watermark_id
        while True:
            rows = self._fetch(
                adapter.incremental_sql,
                {
                    "watermark_ts": cursor_ts,
                    "watermark_id": cursor_id,
                    "limit": limit,
                },
            )
            if not rows:
                break
            self._upsert(adapter, rows)
            fresh = sum(1 for row in rows if (row[12], row[0]) > (start_ts, start_id))
            last = rows[-1]
            cursor_ts, cursor_id = last[12], last[0]
            # The STORED watermark only ever moves forward. Re-reading the lag
            # window must not rewind it: an interrupted pass would then resume
            # even further back each time, and `watermark_age_seconds` would
            # report a regression that never happened.
            if (cursor_ts, cursor_id) > (state.watermark_ts, state.watermark_id):
                state.watermark_ts = cursor_ts
                state.watermark_id = cursor_id
                self._save_state(adapter, state)
            if fresh:
                self._bump_counter(adapter, "incremental_rows", fresh)
            total += fresh
            # A batch that only re-read the lag window must not spend the
            # bounded budget. `max_incremental_batches_per_run` exists to stop
            # ONE adapter's new work from starving the others; charging a
            # re-read to it silently converts a bounded adapter into one that
            # makes no progress at all. apple_message allows a single batch, so
            # before this its every run re-read the lag window and synced
            # nothing -- caught by the contact-identity re-emit test.
            if fresh:
                batches += 1
            else:
                stale_batches += 1
            if (
                len(rows) < limit
                or _past(deadline)
                or stale_batches >= max_stale_batches
                or (
                    adapter.max_incremental_batches_per_run > 0
                    and batches >= adapter.max_incremental_batches_per_run
                )
            ):
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
            cursor.execute(self._dest_sql("SELECT max(refreshed_at) FROM @timeline_gmail_correspondents"))
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
                    FROM @gmail_messages m
                    CROSS JOIN LATERAL unnest(m.to_addresses) AS rcpt
                    WHERE m.from_address ILIKE '%%' || m.account || '%%'
                       OR EXISTS (SELECT 1 FROM @gmail_sync_state s
                                  WHERE s.account <> '' AND m.from_address ILIKE '%%' || s.account || '%%')
                    GROUP BY 1
                    """
                )
            )
            rows = cursor.fetchall()
        with self._dest_conn.cursor() as cursor:
            cursor.execute(self._dest_sql("DELETE FROM @timeline_gmail_correspondents"))
            execute_values(
                cursor,
                self._dest_sql(
                    "INSERT INTO @timeline_gmail_correspondents (addr, n_sent_to, last_sent_at) "
                    "VALUES %s ON CONFLICT (addr) DO NOTHING"
                ),
                rows,
            )

    def _run_coverage_reconcile(
        self, adapter: TimelineAdapter, state: _AdapterState, deadline: float | None
    ) -> int:
        """Insert the rows a recent event window has that the timeline does not.

        C1 says everything synced eventually lands on `timeline.events`. Until
        this pass existed nothing enforced it: the incremental walk could skip a
        row permanently (see `incremental_lag_hours`), and the only other pass,
        `_run_refresh`, re-walks by event time to reconverge CONTENT -- it never
        asks whether a row is present at all. Measured 2026-08-26, Slack was
        missing 798 of 26,217 rows (3.0%) in one settled day and every health
        surface read `ok`, because "adapter is not erroring" was the only thing
        being checked.

        This asks the source the one question that matters -- which of your rows
        am I missing -- as an anti-join on the timeline's primary key. It repairs
        the gap whatever caused it, which is why it is a property and not a
        patch for one race.
        """

        if adapter.reconcile_hours <= 0 or not adapter.reconcile_sql:
            return 0
        # The sweep's cost is the WINDOW's size, not the number of gaps it
        # finds: measured on production 2026-08-26, slack_message took 24s to
        # sweep 48h whether it repaired 62,891 rows or none. Running that on
        # every pass (~288/day) would spend 2 hours of database time a day to
        # answer a question that changes slowly, so it runs on its own cadence.
        interval = timedelta(hours=adapter.reconcile_interval_hours)
        if interval and datetime.now(tz=UTC) - state.last_reconcile_at < interval:
            return 0
        # Anchor the window to the SOURCE's newest ingest, not to wall clock.
        # A source that is paused, slow, or on a skewed clock would otherwise
        # have an empty window and reconcile nothing -- reporting healthy for
        # exactly the reason it is not.
        with self._source_conn.cursor() as cursor:
            cursor.execute(self._source_sql(adapter.max_ingest_sql))
            row = cursor.fetchone()
        newest_ingest = row[0] if row else None
        if newest_ingest is None:
            return 0
        window_start = newest_ingest - timedelta(hours=adapter.reconcile_hours)
        cursor_ts: datetime = BACKFILL_CURSOR_START
        cursor_id = ""
        total = 0
        limit = self._batch_limit(adapter)
        while not _past(deadline):
            rows = self._fetch(
                adapter.reconcile_sql,
                {
                    "window_start": window_start,
                    "cursor_ts": cursor_ts,
                    "cursor_id": cursor_id,
                    "adapter": adapter.name,
                    "limit": limit,
                },
            )
            if not rows:
                break
            self._upsert(adapter, rows)
            total += len(rows)
            self._bump_counter(adapter, "incremental_rows", len(rows))
            last = rows[-1]
            cursor_ts, cursor_id = last[3], last[0]
            if len(rows) < limit:
                break
        state.last_reconcile_at = datetime.now(tz=UTC)
        self._save_state(adapter, state)
        if total:
            # Loud on purpose. A steady-state reconcile finds nothing; a nonzero
            # count means another pass is losing rows, and the number is the
            # evidence for which one.
            logger.warning(
                "timeline reconcile repaired %d missing %s rows in the last %.0fh",
                total,
                adapter.name,
                adapter.reconcile_hours,
            )
        return total

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

    # A prune that runs away is far worse than the orphans it removes: the
    # sync engine has no undo and timeline.events is the read surface for every
    # agent. Refuse rather than delete when the proposed deletion is a large
    # share of the adapter's rows -- that shape means the authoritative query
    # returned a partial answer (a mid-rebuild derived table, a failed join),
    # not that history genuinely disappeared.
    PRUNE_MAX_FRACTION = 0.10
    PRUNE_MIN_KEEP = 50

    def _run_prune(self, adapter: TimelineAdapter) -> int:
        """Delete timeline rows whose source row no longer exists.

        The authoritative key set comes from the SOURCE database and the rows
        live in the DEST, which may be a different connection, so the live set
        is materialized rather than joined. That is affordable only because
        this is opt-in for reconciled derived sources, which are small.
        """
        live_ids = [row[0] for row in self._fetch(adapter.prune_sql, {})]
        events = self._qualified_regclass("timeline_events", namespace=self._dest_schema)
        with self._dest_conn.cursor() as cursor:
            cursor.execute(f"SELECT count(*) FROM {events} WHERE adapter = %s", (adapter.name,))
            total = int(cursor.fetchone()[0])
            if total == 0:
                return 0
            if not live_ids:
                # An empty authoritative set is indistinguishable from a broken
                # query, and deleting every row of an adapter is exactly the
                # runaway this guard exists for.
                logger.error(
                    "timeline prune refused for %s: the authoritative query returned no rows",
                    adapter.name,
                )
                return 0
            cursor.execute(
                f"SELECT count(*) FROM {events} WHERE adapter = %s AND NOT (event_id = ANY(%s))",
                (adapter.name, live_ids),
            )
            doomed = int(cursor.fetchone()[0])
            if doomed == 0:
                return 0
            # Clamp rather than refuse. A real backlog is legitimately large
            # -- production carried 4,944 orphans against 19,316 rows (25.6%)
            # when this shipped -- so a hard refusal would decline the exact
            # cleanup it exists for, forever. Deleting at most a tenth per run
            # converges that in a few passes while keeping any single mistake
            # small, visible, and recoverable from the run before it.
            budget = max(1, int(total * self.PRUNE_MAX_FRACTION))
            if total - doomed < self.PRUNE_MIN_KEEP:
                logger.error(
                    "timeline prune refused for %s: only %s of %s rows would survive; "
                    "the authoritative query is probably incomplete",
                    adapter.name,
                    total - doomed,
                    total,
                )
                return 0
            if doomed > budget:
                logger.warning(
                    "timeline prune clamped for %s: %s of %s rows are orphaned, "
                    "deleting %s this run and the rest on later runs",
                    adapter.name,
                    doomed,
                    total,
                    budget,
                )
            cursor.execute(
                f"""
                DELETE FROM {events}
                WHERE ctid IN (
                    SELECT ctid FROM {events}
                    WHERE adapter = %s AND NOT (event_id = ANY(%s))
                    LIMIT %s
                )
                """,
                (adapter.name, live_ids, budget),
            )
            deleted = cursor.rowcount
        logger.info("timeline prune removed %s orphaned rows for %s", deleted, adapter.name)
        return deleted

    # Deleting a retired adapter's rows is bounded per run for the same
    # reason the prune is: the engine has no undo, and timeline.events is the
    # read surface for every agent. A retirement is a handful of rows in
    # practice, so a small budget converges immediately and caps the blast
    # radius if a live adapter is ever named here by mistake.
    RETIRE_MAX_ROWS_PER_RUN = 5_000

    def _retire_removed_adapters(self) -> int:
        live = {adapter.name for adapter in TIMELINE_ADAPTERS}
        retired = [name for name in RETIRED_TIMELINE_ADAPTERS if name not in live]
        if not retired:
            return 0
        events = self._qualified_regclass("timeline_events", namespace=self._dest_schema)
        state = self._qualified_regclass("timeline_sync_state", namespace=self._dest_schema)
        deleted = 0
        with self._dest_conn.cursor() as cursor:
            for name in retired:
                cursor.execute(
                    f"""
                    DELETE FROM {events}
                    WHERE ctid IN (
                        SELECT ctid FROM {events} WHERE adapter = %s LIMIT %s
                    )
                    """,
                    (name, self.RETIRE_MAX_ROWS_PER_RUN),
                )
                removed = cursor.rowcount or 0
                deleted += removed
                if removed:
                    logger.info("timeline retired adapter %s: removed %s rows", name, removed)
                cursor.execute(f"SELECT count(*) FROM {events} WHERE adapter = %s", (name,))
                if int(cursor.fetchone()[0]) == 0:
                    cursor.execute(f"DELETE FROM {state} WHERE adapter = %s", (name,))
        return deleted

    def run(self, *, max_seconds: float | None = None) -> list[AdapterSyncStats]:
        self._connect()
        deadline = time.monotonic() + max_seconds if max_seconds else None
        stats: dict[str, AdapterSyncStats] = {
            adapter.name: AdapterSyncStats(adapter=adapter.name) for adapter in self._adapters
        }
        states: dict[str, _AdapterState] = {}
        failed: list[str] = []

        try:
            self._retire_removed_adapters()
        except Exception:  # noqa: BLE001 - cleanup must never stop ingestion
            logger.exception("timeline retired-adapter cleanup failed")

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
                if adapter.prune_sql and state.backfill_done:
                    stats[adapter.name].pruned_rows = self._run_prune(adapter)
                stats[adapter.name].backfill_done = state.backfill_done
                # Heartbeat. `_save_state` stamps last_run_at, but every other
                # caller only reaches it when rows were WRITTEN, so a healthy
                # adapter with nothing to do looked identical to one that had
                # stopped running -- `run_age_seconds` meant "last wrote".
                self._save_state(adapter, state)
            except Exception as exc:  # noqa: BLE001 - keep other adapters running
                logger.exception("timeline incremental sync failed for %s", adapter.name)
                stats[adapter.name].error = str(exc)
                state = states.get(adapter.name)
                if state is not None:
                    self._save_state(adapter, state, error=str(exc))
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
                    self._save_state(adapter, state, error=str(exc))
                    failed.append(adapter.name)
                    active.remove(adapter)
                    continue
                stats[adapter.name].backfill_done = state.backfill_done
                if state.backfill_done:
                    active.remove(adapter)
                if _past(deadline):
                    break

        # Coverage reconcile, LAST and STALEST-FIRST.
        #
        # Not inside the per-adapter loop above: that walks a fixed order, and
        # the sweep is the one pass whose cost does not shrink when there is
        # nothing to do (24s for slack_message either way). In fixed order the
        # first few adapters would spend the run's deadline every time and the
        # tail would never reconcile at all -- the same shape as the Slack
        # coverage rotation that silently forfeited a stage's turn on every run
        # that lost the lock, and went unnoticed for three months. Choosing the
        # adapter that has gone longest without a sweep makes starvation
        # impossible instead of unlikely.
        due = sorted(
            (
                adapter
                for adapter in self._adapters
                if adapter.name in states
                and states[adapter.name].backfill_done
                and not stats[adapter.name].error
            ),
            key=lambda adapter: states[adapter.name].last_reconcile_at,
        )
        for adapter in due:
            if _past(deadline):
                break
            state = states[adapter.name]
            try:
                stats[adapter.name].reconciled_rows = self._run_coverage_reconcile(
                    adapter, state, deadline
                )
            except Exception as exc:  # noqa: BLE001 - a sweep must never fail the run
                # Reconcile is a repair pass over data every other pass already
                # delivered. Failing the run on it would turn a self-healing
                # mechanism into an outage, so it reports and yields.
                logger.exception("timeline coverage reconcile failed for %s", adapter.name)

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


class TimelinePriorityError(ValueError):
    """An adapter did not classify a row into one of the five real tiers."""


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
