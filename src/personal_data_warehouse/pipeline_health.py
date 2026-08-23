"""Pipeline freshness and health over every warehouse table.

The warehouse is fed by ~30 independent pipelines — API pollers, LaunchAgent
uploaders on Zach's Macs, a systemd timer on the openclaw VM, a linked WhatsApp
device, agent enrichment passes, and the derived/timeline builders. Each one can
stop delivering on its own, quietly: an expired session token, a revoked macOS
permission, an iCloud sync that froze, an uploader that was never loaded. The
warehouse used to have no single place that answered "what is still arriving,
what stopped, and when did Gmail last land a row?".

This module is that place. It declares, once:

* :data:`PIPELINES` — every pipeline, with how it is driven, how often data is
  expected, and where its run state lives, and
* :data:`TABLE_PIPELINES` — every warehouse table's pipeline, its role in that
  pipeline, and the column that means "the pipeline wrote this row".

:class:`PipelineHealthCollector` turns those declarations into a snapshot
(``ops.pipeline_health`` + ``ops.pipeline_table_freshness``) that
``marts_ops.pipeline_health`` / ``marts_ops.table_freshness`` present with a
live status, and that the app's ``/pipelines`` page renders.

Like ``TIMELINE_TABLE_COVERAGE``, this registry is exhaustive by test:
``tests/test_pipeline_health.py`` fails when a warehouse table exists without a
pipeline classification, so a new source cannot ship invisible to the dashboard.

Freshness has two independent meanings, and collapsing them hides real
failures:

* **data freshness** (``last_write_at``) — when the pipeline last wrote a
  *payload* row. Slack syncing its user directory daily must not make Slack look
  healthy while message ingestion is frozen, so only ``role="data"`` tables
  count toward it.
* **run freshness** (``last_run_at``) — when the pipeline last did *anything*,
  read from its ``role="state"`` cursor/credential/heartbeat tables. A poller
  that runs every five minutes and finds nothing new is healthy; the same
  pipeline silent for a day is not. Sources pushed from a laptop have no
  in-warehouse heartbeat at all, which is exactly why they need watching.

Probe cost is bounded on purpose. ``max(<column>)`` is free against an index and
a full heap scan without one, so the collector probes a column only when an
index leads with it or the table is small (:data:`PROBE_MAX_UNINDEXED_BYTES`),
and records why it skipped otherwise. That keeps a 10-minute cadence from
turning into a recurring 50 GB scan of ``timeline.events``; the timeline's own
health comes from its per-adapter sync state instead.
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections.abc import Iterable, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg2

from personal_data_warehouse.relations import (
    CANONICAL_RELATIONS,
    CATALOG,
    relation as canonical_relation,
)

logger = logging.getLogger(__name__)

__all__ = [
    "ACCOUNT_BASELINE_GAPS",
    "DATA_BASIS_REQUIRED_ABOVE",
    "EXPENSIVE_MART_VIEWS",
    "INHERIT_DATA_INTERVAL",
    "MART_PROBE_STATEMENT_TIMEOUT_MS",
    "MART_VIEW_IDS",
    "MartViewSnapshot",
    "PROBE_SKIPPED_EXPENSIVE",
    "mart_view_ids",
    "ACCOUNT_BASELINE_MAX_DAYS",
    "ACCOUNT_BASELINE_PERCENTILE",
    "ACCOUNT_LATE_MULTIPLIER",
    "ACCOUNT_MIN_BASELINE_GAPS",
    "ACCOUNT_MIN_EXPECTED_GAP_SECONDS",
    "ACCOUNT_STALE_MULTIPLIER",
    "COLLECTOR_STALE_SECONDS",
    "LATE_MULTIPLIER",
    "PIPELINES",
    "PROBE_MAX_UNINDEXED_BYTES",
    "PROBE_STATEMENT_TIMEOUT_MS",
    "STALE_MULTIPLIER",
    "TABLE_PIPELINES",
    "Pipeline",
    "PipelineHealthCollector",
    "PipelineSnapshot",
    "StateSource",
    "TableFreshness",
    "TableSnapshot",
    "pipeline",
    "pipeline_tables",
]

#: House style stores "no timestamp" as the epoch; anything at or before this is
#: absent, not old.
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
EPOCH_GUARD = datetime(1970, 1, 2, tzinfo=UTC)

#: A pipeline is "late" past this multiple of its expected interval and "stale"
#: past the second. Bursty real-world sources need the slack: a five-minute
#: poller that legitimately finds nothing for an hour must not read as broken.
LATE_MULTIPLIER = 2
STALE_MULTIPLIER = 6

#: How stale the snapshot itself may be before the views stop trusting it. The
#: collector runs every ten minutes; past this the dashboard reports 'unknown'
#: instead of quietly presenting hour-old timestamps as current.
COLLECTOR_STALE_SECONDS = 3600

# --- per-account freshness (marts_finance.account_freshness) ------------------
#
# Table-level freshness cannot see one account going quiet. ``base_plaid.
# transactions`` stayed current for four months while the Capital One Venture X
# card contributed nothing to it, because Fidelity, Robinhood, and Venmo kept
# writing to the same table; ``max(posted_at)`` over the whole table is a
# max, so the loudest source hides every silent one. The statement side was
# worse than invisible — ``manual_finance`` declares ``data=None`` (a manual
# upload never goes stale), so the PDF pipeline stopping in March 2026 was by
# construction not a detectable event. Between them the card had no transaction
# from 2026-03-21 to 2026-05-12 and nothing noticed.
#
# The fix has to be per-account, and it cannot use one global threshold: a card
# used daily and a Roth IRA touched twice a year are both healthy. Nor can the
# interval be declared per account — that is 20+ hand-maintained numbers that
# rot the moment spending habits change.
#
# So each account is judged against *its own* measured cadence. The estimator
# is the 90th percentile of the intervals between consecutive transactions,
# which absorbs the burstiness a mean cannot: brokerages trade in clusters and
# then sit idle, and a mean gap flags them constantly. Two properties matter:
#
#   * The baseline window ends at the account's LAST transaction, not at now().
#     Measuring cadence over a trailing window that includes the silence lets a
#     long outage depress its own baseline and hide itself — the longer an
#     account is broken, the more normal being broken looks.
#   * The window is counted in INTERVALS, not days. A fixed span of days cannot
#     serve both ends of the range: 180 days holds hundreds of intervals for a
#     daily-use card but at most six for a monthly one, so every slow account
#     would fall below the minimum and sit permanently unjudged — silently
#     exempt, which is the failure this exists to prevent. Taking the most
#     recent N intervals instead lets each account reach back exactly as far as
#     its own cadence requires.
#   * Accounts with too few observed intervals are reported 'sparse' rather
#     than judged. A p90 over three gaps is not a cadence, and forcing a
#     verdict there is how a monitor earns the ignore-it reflex.
#
# Backtested against the Venture X outage: the card reaches 16x its typical gap
# by 2026-04-05 and 51x by 2026-05-10, so this reports 'stale' roughly two
# weeks in instead of four months. At these thresholds the current warehouse
# has two accounts above 'late' (the broken card, and a receivable that was
# settled by check), and every actively-used account sits below 1.0x.
ACCOUNT_BASELINE_PERCENTILE = 0.9
#: How many recent intervals define an account's cadence.
ACCOUNT_BASELINE_GAPS = 20
#: Outer sanity bound on how far back those intervals may reach, so a dormant
#: account is not judged against how it behaved years ago.
ACCOUNT_BASELINE_MAX_DAYS = 1095
#: Fewer measured intervals than this and the account is 'sparse', not judged.
ACCOUNT_MIN_BASELINE_GAPS = 10
#: Cadence floor. Without it an account posting several times a day is 'late'
#: after a quiet weekend, which is noise, not signal.
ACCOUNT_MIN_EXPECTED_GAP_SECONDS = 12 * 3600
#: Multiples of the account's own typical gap. Wider than the pipeline
#: multipliers above because a p90 is exceeded by 10% of normal gaps by
#: definition, so the margin has to clear routine variance.
ACCOUNT_LATE_MULTIPLIER = 6
ACCOUNT_STALE_MULTIPLIER = 15

#: ``max(col)`` on an unindexed column is a full heap scan. Tables under this
#: size are cheap enough to scan on every collection; larger ones are skipped
#: unless an index leads with the column.
PROBE_MAX_UNINDEXED_BYTES = 256 * 1024 * 1024

#: Per-probe statement budget. A pathological probe is recorded as a timeout
#: rather than being allowed to outlive the collection window.
PROBE_STATEMENT_TIMEOUT_MS = 5_000

PIPELINE_KINDS = ("source", "enrichment", "derived", "internal")
TABLE_ROLES = ("data", "support", "state")

PROBE_OK = "ok"
PROBE_EMPTY = "empty"
PROBE_NO_TIMESTAMP = "no_timestamp"
PROBE_SKIPPED_UNINDEXED = "skipped_unindexed"
#: A view the registry declares too expensive to touch every ten minutes. Same
#: contract as ``skipped_unindexed``: record why, never silently return nothing.
PROBE_SKIPPED_EXPENSIVE = "skipped_expensive"
PROBE_TIMEOUT = "timeout"
PROBE_ERROR = "error"
PROBE_MISSING = "missing"

# --- mart (view) health -------------------------------------------------------
#
# Level 2 of the health contract. The thirty-odd ``marts_*`` views are the
# warehouse's stable read interface — the relations an agent is told to start
# from — and until now not one of them had any health coverage at all:
# ``SELECT layer, count(*) FROM marts_ops.table_freshness GROUP BY 1`` returned
# base/derived/ops/private/timeline and no ``marts`` row.
#
# **The table probe genuinely cannot be pointed at a view.** ``TABLE_PIPELINES``
# measures ``max(<written_at>)`` over a heap; a view has no stamped column to
# take a max of, no ``relpages`` to bound the cost with, and no index for the
# collector's cheapness guard to consult. Pretending otherwise would mean
# either inventing a timestamp column that does not exist or running an
# unbounded aggregate over a union of six source tables every ten minutes.
#
# So a view is measured by the three things that are cheap AND true about it:
#
# 1. **Input freshness.** A view is only ever as fresh as the stalest relation
#    it reads. The inputs are resolved from ``pg_depend``/``pg_rewrite`` at
#    collection time rather than from a hand-written map, so a redefinition
#    cannot leave the map behind, and each input is judged against *its own*
#    pipeline's SLA (``marts_ai_conversations.events`` reads six agent-source
#    tables whose expectations differ by an order of magnitude). This alone
#    surfaces ``pi`` going quiet through every view that reads it.
# 2. **A bounded non-empty probe** — ``SELECT 1 FROM <view> LIMIT 1``. O(1) for
#    almost every view here: measured against production 2026-08-23, thirty-two
#    of the thirty-three returned inside ~15 ms of server time, and the one
#    outlier (``marts_inbox.gmail_threads``, ~2.3 s) is declared below rather
#    than discovered at runtime.
# 3. **Definition drift** — the sha256 of ``pg_get_viewdef()``. A redefinition
#    that silently drops a source table changes nothing measurable about the
#    rows; it changes the definition, so that is what is watched.

#: Per-view probe budget. Deliberately the same order as the table probe: a
#: view that cannot answer "is there a row?" inside this is recorded as a
#: timeout rather than being allowed to stretch the collection window.
MART_PROBE_STATEMENT_TIMEOUT_MS = 5_000

#: Views whose bounded probe is known to cost real work, so the collector does
#: not pay it every ten minutes. They are still measured on inputs and on
#: definition drift; only the row probe is skipped, and the skip is recorded.
#: Measured on the production corpus 2026-08-23 (wall clock includes ~320 ms of
#: round trip): marts_inbox.gmail_threads 2,619 ms — it groups the whole Gmail
#: thread corpus before a LIMIT can bite. Every other view came back in under
#: 540 ms end to end.
EXPENSIVE_MART_VIEWS: frozenset[str] = frozenset({"clean_gmail_inbox"})


def mart_view_ids() -> tuple[str, ...]:
    """Every ``marts_*`` view, read from the catalog rather than a second registry.

    The catalog already declares each mart's logical id, layer and physical
    location, so deriving the list from it means adding a mart is still one
    catalog edit: the health surface picks it up with no parallel list to
    forget. ``tests/test_pipeline_health.py`` asserts the collector emits a row
    for every one of them.
    """
    return tuple(
        sorted(obj.id for obj in CATALOG.objects if obj.layer == "marts" and obj.kind == "view")
    )


#: Snapshot of the above at import time, for callers that want a constant.
MART_VIEW_IDS: tuple[str, ...] = mart_view_ids()


@dataclass(frozen=True)
class StateSource:
    """A pipeline's run state: the heartbeat plus any recorded failure.

    Every sync-state table in the warehouse shares the same shape (``status``,
    ``error``, and a timestamp), so one declaration is enough to surface "this
    pipeline ran at T" and "N of its scopes are failing, most recently with
    this message".
    """

    table: str
    updated_column: str
    status_column: str = ""
    error_column: str = ""
    #: Status values that mean "this scope is broken and no retry will fix it
    #: on its own". The sync writers never agreed on one failure word —
    #: slack_sync writes ``error`` while whoop_sync, calendar_sync,
    #: contacts_sync, and google_drive_source_sync write ``failed`` — so the
    #: default classifies both dialects. (WHOOP spent 26 hours hard-down on
    #: 2026-07-30 reading 'ok' on the dashboard because only ``error`` was
    #: counted; tests/test_pipeline_health.py pins both now.) Plaid records
    #: ``action_required`` for an Item whose login expired; the run stays
    #: green by design, and the dashboard is where that has to become visible.
    #: Benign non-ok states ('gone' tombstones, 'unsupported' products) are
    #: deliberately unclassified.
    error_statuses: tuple[str, ...] = ("error", "failed")
    attention_statuses: tuple[str, ...] = ("action_required",)


#: ``expected_event_interval`` sentinel meaning "judge event time on the data
#: interval". Inheriting is the default so a new source cannot quietly opt out
#: of event-time monitoring by forgetting a field; a pipeline whose event time
#: legitimately lags its writes has to say so, and say why.
INHERIT_DATA_INTERVAL = timedelta.min

#: Above this, a data interval stops being self-evident from the cadence and
#: has to justify itself in ``data_basis``. Enforced by
#: ``test_a_long_data_sla_says_where_its_number_came_from``.
DATA_BASIS_REQUIRED_ABOVE = timedelta(days=7)


@dataclass(frozen=True)
class Pipeline:
    """One thing that keeps part of the warehouse up to date.

    Three intervals, deliberately separate, because collapsing them is how a
    monitor ends up unable to catch anything:

    * ``expected_run_interval`` — how often the pipeline **runs**. It is the
      poll cadence, and it applies only when the pipeline keeps a heartbeat in
      ``state``; an uploader pushing from a Mac has none.
    * ``expected_data_interval`` — how often **data legitimately arrives**. A
      judgement about the real world, not about the schedule: a five-minute
      uploader whose human records a voice memo twice a month is healthy at
      five minutes and healthy at two weeks. Setting this to the cadence turns
      every quiet weekend into an alarm; setting it to a blunt month (as seven
      pipelines did until 2026-08-23) means a six-week outage cannot reach
      ``late``, because ``late`` is 2x and ``stale`` is 6x. Where measurable, it
      is set from the source's own measured gap distribution — see
      ``data_basis``.
    * ``expected_event_interval`` — how far behind **the newest real-world
      event** may fall. Usually the same as the data interval, which is the
      default; a pipeline whose event time legitimately lags its writes (the
      finance ledger dates observations by day) overrides it explicitly.

    ``None`` means "no expectation": manual uploads never go stale.
    """

    id: str
    label: str
    kind: str
    #: Human-readable cadence, shown on the dashboard next to the age.
    cadence: str
    #: How the data physically gets here — the first thing to check when a
    #: pipeline goes quiet.
    transport: str
    expected_data_interval: timedelta | None
    expected_run_interval: timedelta | None = None
    #: Defaults to ``expected_data_interval``; override only with a reason.
    expected_event_interval: timedelta | None = INHERIT_DATA_INTERVAL
    #: Where ``expected_data_interval`` came from. Required once the interval
    #: reaches a week, so a long SLA is a measurement someone can re-check
    #: rather than a number someone once guessed.
    data_basis: str = ""
    state: StateSource | None = None
    note: str = ""

    @property
    def event_interval(self) -> timedelta | None:
        """The interval the newest event time is judged against."""
        if self.expected_event_interval == INHERIT_DATA_INTERVAL:
            return self.expected_data_interval
        return self.expected_event_interval

    @property
    def event_interval_is_inherited(self) -> bool:
        return self.expected_event_interval == INHERIT_DATA_INTERVAL


@dataclass(frozen=True)
class TableFreshness:
    """How one warehouse table participates in its pipeline.

    role:
      - ``data``: payload rows. Their newest write IS the pipeline's data
        freshness.
      - ``support``: dimensions, entities, and sidecars (Slack users, chat
        rosters, attachment blobs) that follow the payload.
      - ``state``: cursors, watermarks, credentials, and heartbeats — the
        pipeline's run state rather than its output.

    ``written_at`` is the column the pipeline stamps when it writes a row;
    ``event_at`` is when the row's content happened in the real world (an
    email's date, a photo's capture time), which is what "current through
    2026-07-27" on the dashboard means. ``None`` for either is a declaration
    that the table has no such column, not an omission.
    """

    pipeline: str
    role: str
    written_at: str | None
    event_at: str | None = None
    note: str = ""


def _source(
    id: str,
    label: str,
    *,
    cadence: str,
    transport: str,
    data: timedelta | None,
    run: timedelta | None = None,
    event: timedelta | None = INHERIT_DATA_INTERVAL,
    basis: str = "",
    state: StateSource | None = None,
    note: str = "",
) -> Pipeline:
    return Pipeline(
        id=id,
        label=label,
        kind="source",
        cadence=cadence,
        transport=transport,
        expected_data_interval=data,
        expected_run_interval=run,
        expected_event_interval=event,
        data_basis=basis,
        state=state,
        note=note,
    )


DAY = timedelta(days=1)
HOUR = timedelta(hours=1)
MINUTE = timedelta(minutes=1)

# Every ``data_basis`` below that says "measured" was taken from the production
# corpus on 2026-08-23 with this shape, over a 730-day window: the distinct
# minutes in which the pipeline's payload table was written, and the
# distribution of the gaps between consecutive ones. That is "how long has this
# pipeline ever legitimately gone without writing anything", which is exactly
# what the SLA has to sit above and the cadence cannot tell you.
#
#   WITH stamps AS (
#     SELECT DISTINCT date_trunc('minute', <written_at>) AS t FROM <table>
#     WHERE <written_at> > now() - interval '730 days'
#   ), gaps AS (
#     SELECT EXTRACT(EPOCH FROM t - lag(t) OVER (ORDER BY t))/86400.0 AS d FROM stamps
#   )
#   SELECT count(*), percentile_cont(0.95) WITHIN GROUP (ORDER BY d), max(d) FROM gaps;
#
# Re-run it before moving one of these numbers. A gap distribution changes when
# habits change, and an SLA that no longer matches the source it describes is
# the failure this registry exists to prevent, in either direction.


# Every pipeline that writes to the warehouse. Adding a source means adding an
# entry here and classifying its tables below; the tests fail otherwise.
PIPELINES: tuple[Pipeline, ...] = (
    _source(
        "gmail",
        "Gmail",
        cadence="every 15 min",
        transport="Dagster gmail_sync → Gmail API (history-id incremental)",
        data=6 * HOUR,
        run=45 * MINUTE,
        state=StateSource(
            table="gmail_sync_state",
            updated_column="updated_at",
            status_column="status",
            error_column="error",
        ),
    ),
    _source(
        "google_calendar",
        "Google Calendar",
        cadence="every 5 min",
        transport="Dagster calendar_sync → Calendar API (sync tokens)",
        data=3 * DAY,
        run=30 * MINUTE,
        state=StateSource(
            table="calendar_sync_state",
            updated_column="updated_at",
            status_column="status",
            error_column="error",
        ),
        note="calendar edits are bursty; the run heartbeat is the real signal",
    ),
    _source(
        "google_contacts",
        "Google Contacts",
        cadence="hourly",
        transport="Dagster contacts_sync → People API (sync tokens)",
        data=60 * DAY,
        run=3 * HOUR,
        basis=(
            "measured: 4 gaps, max 51.75d — contact edits really are that rare,"
            " so the previous 30d was BELOW the source's own longest legitimate"
            " silence and would eventually have cried wolf. This pipeline runs"
            " hourly with a heartbeat, and that heartbeat, not data freshness,"
            " is what catches it breaking"
        ),
        state=StateSource(
            table="contact_sync_state",
            updated_column="updated_at",
            status_column="status",
            error_column="error",
        ),
        note="data goes quiet for months by nature; judge this one on last_run_at",
    ),
    _source(
        "apple_contacts",
        "Apple Contacts",
        cadence="uploader every 5 min",
        transport="Mac LaunchAgent → /ingest/apple-contacts/batch → Drive inbox → Dagster",
        data=21 * DAY,
        basis=(
            "measured: only 6 gaps, p90 8.30d, max 8.43d — a sparse sample, so"
            " the interval is set well above it rather than fitted to it. 21d"
            " (late at 42d) is the tightest honest bound: unlike google_contacts"
            " this uploader keeps no heartbeat, so data freshness is the ONLY"
            " in-warehouse signal that the Mac stopped pushing"
        ),
        note="no in-warehouse heartbeat; check bin/apple-contacts-upload-status on the Mac",
    ),
    _source(
        "google_drive",
        "Google Drive",
        cadence="every 30 min",
        transport="Dagster google_drive_source_sync → Drive changes API",
        data=2 * DAY,
        run=2 * HOUR,
        state=StateSource(
            table="google_drive_sync_state",
            updated_column="updated_at",
            status_column="status",
            error_column="error",
        ),
    ),
    _source(
        "slack",
        "Slack",
        cadence="staged, every 5-15 min",
        transport="Dagster slack_sync stages → Slack Web API",
        data=6 * HOUR,
        run=30 * MINUTE,
        state=StateSource(
            table="slack_sync_state",
            updated_column="updated_at",
            status_column="status",
            error_column="error",
        ),
    ),
    _source(
        "apple_messages",
        "Apple Messages",
        cadence="uploader every 5 min",
        transport="Mac LaunchAgent → /ingest/apple-messages/batch → Drive inbox → Dagster",
        data=2 * DAY,
        note="no in-warehouse heartbeat; check bin/apple-messages-upload-status on the Mac",
    ),
    _source(
        "whatsapp",
        "WhatsApp",
        cadence="linked device, continuous",
        transport="in-Dagster linked-device client → Drive inbox → Dagster",
        data=2 * DAY,
        run=4 * HOUR,
        state=StateSource(table="whatsapp_client_sessions", updated_column="updated_at"),
        note="the session snapshot advances every run window; a frozen one means the device unpaired",
    ),
    _source(
        "apple_notes",
        "Apple Notes",
        cadence="uploader every 5 min",
        transport="Mac LaunchAgent → /ingest/apple-notes/* → Drive inbox → Dagster",
        data=3 * DAY,
        note="quiet usually means iCloud stopped delivering to the Mac, not that nothing changed",
    ),
    _source(
        "apple_voice_memos",
        "Apple Voice Memos",
        cadence="uploader every 5 min",
        transport="Mac LaunchAgent → /ingest/voice-memos/* → Drive inbox → Dagster",
        data=7 * DAY,
        basis=(
            "measured: 145 gaps, p95 3.86d, max 6.56d — a person does not record"
            " a voice memo hourly, but two years of history says they have never"
            " gone a week. 7d puts late at 14d and stale at 42d"
        ),
    ),
    _source(
        "alice_voice_recordings",
        "Alice Voice Recordings",
        cadence="daily 04:17",
        transport="Dagster alice_voice_recordings → Alice API",
        data=7 * DAY,
        # The event side is judged far more loosely than the ingest side, and
        # on purpose: Zach's recording habit is bursty (51 measured event gaps,
        # p90 16.19d, max 223.19d), but the DAILY POLLER writing nothing for
        # weeks is not bursty, it is a poller that stopped.
        event=30 * DAY,
        basis=(
            "a daily poller with no heartbeat, so data freshness is the only"
            " signal there is. Ingest gaps carry none: every recording the"
            " warehouse holds arrived in a single 53-row batch on 2026-07-10, so"
            " 7d (late at 14d, stale at 42d) is set from the poll cadence with"
            " headroom rather than from a distribution that does not exist"
        ),
        note="poller runs daily; a week of silence means it stopped, not that Zach went quiet",
    ),
    _source(
        "apple_photos",
        "Apple Photos",
        cadence="uploader every 30 min",
        transport="Mac LaunchAgent → PhotoKit export → /ingest/photos/* → Drive inbox → Dagster",
        data=3 * DAY,
    ),
    _source(
        "claude_code",
        "Claude Code sessions",
        cadence="uploader every 5 min",
        transport="Mac LaunchAgent tails ~/.claude/projects → /ingest/agent-sessions/batch",
        data=2 * DAY,
    ),
    _source(
        "codex",
        "Codex sessions",
        cadence="uploader every 5 min",
        transport="Mac LaunchAgent tails ~/.codex/sessions → /ingest/agent-sessions/batch",
        data=7 * DAY,
        basis="measured: 2,479 gaps, p95 0.06d, max 3.94d — 7d leaves ample headroom",
    ),
    _source(
        "openclaw",
        "OpenClaw sessions",
        cadence="systemd timer every 5 min",
        transport="openclaw VM systemd timer → /ingest/agent-sessions/batch",
        data=7 * DAY,
        basis="measured: 5,558 gaps, p95 0.02d, max 1.14d — 7d leaves ample headroom",
    ),
    _source(
        "pi",
        "pi sessions",
        cadence="uploader every 5 min",
        transport="Mac LaunchAgent tails ~/.pi/agent/sessions → /ingest/agent-sessions/batch",
        data=3 * DAY,
        basis=(
            "measured: 168 gaps, p95 0.06d, max 2.86d. The previous 30d could not"
            " reach 'late' until sixty days, which is how this source sat quiet"
            " for five weeks under a green dot; 3d puts late at 6d — still twice"
            " the longest silence in two years — and stale at 18d"
        ),
    ),
    _source(
        "claude_desktop",
        "Claude Desktop",
        cadence="poller every 5 min",
        transport="Dagster claude_desktop_client → claude.ai API with a pushed session key",
        data=7 * DAY,
        run=3 * HOUR,
        basis="measured: 111 gaps, p95 1.51d, max 16.71d; the 3h run heartbeat is the sharper signal",
        state=StateSource(table="claude_desktop_credentials", updated_column="updated_at"),
        note="the Mac re-pushes the session key hourly; a stale credential expires the poller",
    ),
    _source(
        "chatgpt",
        "ChatGPT",
        cadence="poller every 5 min",
        transport="Dagster chatgpt_backend_ingest → chatgpt.com backend API with a published session",
        data=7 * DAY,
        run=3 * HOUR,
        basis="measured: 65 gaps, p95 5.29d, max 19.17d; the 3h run heartbeat is the sharper signal",
        state=StateSource(
            table="chatgpt_sessions",
            updated_column="updated_at",
            status_column="status",
            error_column="error",
        ),
        note=(
            "the hourly chatgpt-auth LaunchAgent re-publishes the browser session;"
            " the access token is minted only at chatgpt.com sign-in and lives 10"
            " days, so action_required means either an imminent expiry (still"
            " polling - sign in again) or a rejected token (polling stopped until"
            " a different token is published)"
        ),
    ),
    _source(
        "whoop",
        "WHOOP",
        cadence="every 5 min",
        transport="Dagster whoop_sync → WHOOP API (OAuth refresh)",
        data=2 * DAY,
        run=30 * MINUTE,
        state=StateSource(
            table="whoop_sync_state",
            updated_column="updated_at",
            status_column="status",
            error_column="error",
        ),
        note=(
            "action_required remains visible on /pipelines while repeated calls"
            " are skipped; re-authorize WHOOP with `personal-data-warehouse-whoop-auth --install`"
        ),
    ),
    _source(
        "whoop_private",
        "WHOOP private API",
        cadence="every 15 min",
        transport=(
            "Dagster whoop_private_sync → WHOOP private API"
            " (browser session, rotating refresh token)"
        ),
        data=2 * DAY,
        run=90 * MINUTE,
        state=StateSource(
            table="whoop_private_sync_state",
            updated_column="updated_at",
            status_column="status",
            error_column="error",
        ),
        note=(
            "the access token lives 24h and the refresh token 30 days, rotating on"
            " every refresh, so the source is hands-off until the refresh window"
            " lapses; action_required means the captured browser session is dead —"
            " re-publish it with `pdw whoop publish-session`"
        ),
    ),
    _source(
        "plaid",
        "Plaid finance",
        cadence="every 30 min",
        transport="Dagster plaid_finance_sync → Plaid API per linked Item",
        data=2 * DAY,
        run=2 * HOUR,
        state=StateSource(
            table="plaid_sync_state",
            updated_column="updated_at",
            status_column="status",
            error_column="error",
        ),
        note="action_required means an Item needs `pdw ingest plaid link` again",
    ),
    _source(
        "manual_finance",
        "Manual finance documents",
        cadence="manual upload",
        transport="`pdw ingest manual-finance <files>` → /ingest/manual-finance/* → Drive inbox",
        data=None,
    ),
    Pipeline(
        id="attachment_enrichment",
        label="Attachment & media enrichment",
        kind="enrichment",
        cadence="hourly per source",
        transport="Dagster gmail/whatsapp/imessage/photo enrichment assets → agent container",
        expected_data_interval=7 * DAY,
        expected_run_interval=None,
        data_basis="measured: 32,756 gaps, p95 0.01d, max 1.04d — 7d leaves ample headroom",
        note="one shared table for every source's extracted text, captions, and transcripts",
    ),
    Pipeline(
        id="google_drive_text_extraction",
        label="Drive text extraction",
        kind="enrichment",
        cadence="with each Drive sync",
        transport="Dagster google_drive_source_sync (inline extractors)",
        expected_data_interval=7 * DAY,
        expected_run_interval=None,
        data_basis="measured: 335 gaps, p95 0.83d, max 2.32d — 7d leaves ample headroom",
    ),
    Pipeline(
        id="voice_memo_transcription",
        label="Voice memo transcription",
        kind="enrichment",
        cadence="hourly",
        transport="Dagster apple_voice_memos_transcription → AssemblyAI",
        expected_data_interval=7 * DAY,
        expected_run_interval=None,
        data_basis=(
            "measured: 463 gaps, p95 1.61d, max 6.56d. It transcribes what the"
            " voice-memo uploader delivers, so its honest SLA is its input's, not"
            " its hourly schedule; the previous 30d could not reach 'late' inside"
            " two months"
        ),
    ),
    Pipeline(
        id="voice_memo_enrichment",
        label="Voice memo enrichment",
        kind="enrichment",
        cadence="hourly :17",
        transport="Dagster apple_voice_memos_enrichment → agent container",
        expected_data_interval=7 * DAY,
        expected_run_interval=None,
        data_basis=(
            "measured: 794 gaps, p95 0.71d, max 6.57d — same reasoning as"
            " voice_memo_transcription: it follows its input's cadence, not the clock"
        ),
    ),
    Pipeline(
        id="manual_finance_extraction",
        label="Manual finance extraction",
        kind="enrichment",
        cadence="hourly :53",
        transport="Dagster manual_finance_extraction → agent container",
        expected_data_interval=None,
        expected_run_interval=None,
        note="only runs when a statement was uploaded",
    ),
    Pipeline(
        id="receipt_enrichment",
        label="Receipt research",
        kind="enrichment",
        cadence="hourly :17",
        transport="Dagster receipt_enrichment → agent container over the ledger",
        expected_data_interval=7 * DAY,
        expected_run_interval=None,
        data_basis="measured: 528 gaps, p95 0.36d, max 1.58d — 7d leaves ample headroom",
    ),
    Pipeline(
        id="photo_identity",
        label="Photo identity & dedup",
        kind="derived",
        cadence="hourly :29",
        transport="Dagster photo_identity over every registered photo source",
        expected_data_interval=7 * DAY,
        expected_run_interval=None,
        data_basis="measured: 215 gaps, p95 0.71d, max 2.47d — 7d leaves ample headroom",
    ),
    Pipeline(
        id="slack_file_fingerprints",
        label="Slack image fingerprints",
        kind="derived",
        cadence="hourly :19",
        transport="Dagster slack_file_fingerprints -> files.slack.com, bounded slices",
        expected_data_interval=7 * DAY,
        expected_run_interval=None,
        data_basis="measured: 428 gaps, p95 0.04d, max 0.05d — 7d leaves ample headroom",
        note=(
            "walks a ~905k-image backlog newest-first in bounded slices; the link "
            "table is the cursor, so a slow backfill is normal rather than late"
        ),
    ),
    Pipeline(
        id="finance_ledger",
        label="Finance ledger",
        kind="derived",
        cadence=":07 and :37 hourly",
        transport="Dagster finance_ledger over Plaid + manual_finance",
        expected_data_interval=3 * HOUR,
        expected_run_interval=None,
        # The one pipeline where event time legitimately trails write time, and
        # the reason ``expected_event_interval`` exists as a separate number at
        # all. ``derived_finance.observations.as_of`` is a DATE: an observation
        # written at 15:40 is dated 00:00 that day, so newest_event_at is behind
        # last_write_at by up to a day *while working perfectly*. Judging it on
        # the 3h data interval reported 'late' on a healthy ledger — measured
        # against production 2026-08-23, newest_event_at was 15.8h old against a
        # 6h 'late' threshold. That is the false positive this override exists
        # to prevent; 2d keeps a genuinely frozen ledger detectable at 4d.
        expected_event_interval=2 * DAY,
        note="snapshots every live account's balance, so observations advance every run",
    ),
    Pipeline(
        id="timeline",
        label="Unified timeline",
        kind="derived",
        cadence="every 5 min",
        transport="Dagster timeline_sync over every adapter",
        expected_data_interval=30 * MINUTE,
        expected_run_interval=30 * MINUTE,
        state=StateSource(
            table="timeline_sync_state",
            updated_column="updated_at",
            error_column="last_error",
        ),
        note="timeline.events is too large to probe for writes; per-adapter sync state is the signal",
    ),
    Pipeline(
        id="search_index",
        label="Search index (chunks + embeddings)",
        kind="derived",
        cadence="chunks every 5 min; embeddings every 10 min",
        transport="Dagster search_chunks / search_chunk_embeddings over timeline.events",
        expected_data_interval=2 * HOUR,
        expected_run_interval=None,
        note=(
            "chunks follow the timeline seq cursor; embeddings drain through the "
            "configured OpenAI-compatible endpoint and skip (not fail) while "
            "unconfigured or pre-pgvector"
        ),
    ),
    Pipeline(
        id="upstream_mutations",
        label="Upstream mutations",
        kind="internal",
        cadence="on demand",
        transport="MCP propose_mutation → review UI → Dagster upstream_mutations worker",
        expected_data_interval=None,
        expected_run_interval=None,
    ),
    Pipeline(
        id="enrichment_agent",
        label="Enrichment agent runs",
        kind="internal",
        cadence="on demand",
        transport="agent container invocations logged by every enrichment pipeline",
        expected_data_interval=None,
        expected_run_interval=None,
    ),
    Pipeline(
        # The collector is a pipeline like any other, and it watches itself: if
        # this row is stale, every other row on the dashboard is measured from an
        # old snapshot (which the views also flag as 'unknown').
        id="pipeline_health",
        label="Pipeline freshness collector",
        kind="internal",
        cadence="every 10 min",
        transport="Dagster pipeline_health asset over every registered table",
        expected_data_interval=30 * MINUTE,
        expected_run_interval=None,
    ),
    Pipeline(
        # Separate from pipeline_health on purpose: this one costs a sequential
        # scan of every unique index's heap under the size ceiling (~2 GB of
        # reads on the production shape), which is a daily amount of work, not a
        # ten-minutely one. Folding it into the freshness collector would either
        # make that collector expensive or make this check useless.
        id="collation_health",
        label="Collation drift & index integrity",
        kind="internal",
        cadence="daily 03:41",
        transport="Dagster collation_health asset over pg_database/pg_collation + unique indexes",
        expected_data_interval=2 * DAY,
        expected_run_interval=None,
        data_basis="a daily asset; 2d puts late at 4d, so one missed run is not an alarm",
        note=(
            "this database has NO collation baseline (datcollversion is NULL) and"
            " REFRESH COLLATION VERSION cannot create one, so Postgres will never"
            " warn; the duplicate-key probe is corroboration only and cannot see a"
            " mis-ordered index that has no duplicates — amcheck can"
        ),
    ),
)


def _data(pipeline_id: str, written_at: str | None, event_at: str | None = None, note: str = "") -> TableFreshness:
    return TableFreshness(
        pipeline=pipeline_id, role="data", written_at=written_at, event_at=event_at, note=note
    )


def _support(pipeline_id: str, written_at: str | None, event_at: str | None = None, note: str = "") -> TableFreshness:
    return TableFreshness(
        pipeline=pipeline_id, role="support", written_at=written_at, event_at=event_at, note=note
    )


def _state(pipeline_id: str, written_at: str | None, note: str = "") -> TableFreshness:
    return TableFreshness(pipeline=pipeline_id, role="state", written_at=written_at, note=note)


# Every warehouse table (POSTGRES_TABLES plus the raw-DDL control-plane tables)
# must appear here; tests/test_pipeline_health.py fails the suite otherwise.
TABLE_PIPELINES: dict[str, TableFreshness] = {
    # Gmail
    "gmail_messages": _data("gmail", "synced_at", "internal_date"),
    "gmail_attachments": _support("gmail", "synced_at", "internal_date"),
    "gmail_sync_state": _state("gmail", "updated_at", "per-account history-id cursor"),
    # Google Calendar
    "calendar_events": _data("google_calendar", "synced_at", "start_at"),
    "calendar_sync_state": _state("google_calendar", "updated_at"),
    # Contacts
    "contact_cards": _data("google_contacts", "synced_at", "source_updated_at"),
    "contact_sync_state": _state("google_contacts", "updated_at"),
    "apple_contact_cards": _data("apple_contacts", "synced_at", "source_updated_at"),
    # Google Drive
    "google_drive_files": _data("google_drive", "ingested_at", "modified_time"),
    "google_drive_sync_state": _state("google_drive", "updated_at"),
    "google_drive_file_texts": _data("google_drive_text_extraction", "extracted_at", "source_modified_time"),
    # Slack
    "slack_messages": _data("slack", "synced_at", "message_datetime"),
    "slack_files": _data("slack", "synced_at", "created_at"),
    "slack_message_reactions": _support("slack", "synced_at", note="too large to probe unindexed"),
    "slack_teams": _support("slack", "synced_at"),
    "slack_account_identities": _support("slack", "synced_at"),
    "slack_users": _support("slack", "synced_at"),
    "slack_conversations": _support("slack", "synced_at"),
    "slack_conversation_members": _support("slack", "synced_at"),
    "slack_conversation_stats": _support("slack", "updated_at", "latest_message_at"),
    "slack_account_state_item_rows": _support("slack", "synced_at", "latest_activity_at"),
    "slack_sync_state": _state("slack", "updated_at", "per-object cursors and errors"),
    "slack_file_fingerprints": _data(
        "slack_file_fingerprints",
        "updated_at",
        note="file -> content sha link; the hash itself lives in media_fingerprints",
    ),
    # Apple Messages
    "apple_messages": _data("apple_messages", "ingested_at", "message_at"),
    "apple_message_attachments": _support("apple_messages", "ingested_at"),
    "apple_message_handles": _support("apple_messages", "ingested_at"),
    "apple_message_chats": _support("apple_messages", "ingested_at", "last_read_message_at"),
    "apple_message_chat_handles": _support("apple_messages", "ingested_at"),
    "apple_message_chat_messages": _support("apple_messages", "ingested_at"),
    # WhatsApp
    "whatsapp_messages": _data("whatsapp", "ingested_at", "message_at"),
    "whatsapp_media_items": _support("whatsapp", "ingested_at", "message_at"),
    "whatsapp_chats": _support("whatsapp", "ingested_at", "last_message_at"),
    "whatsapp_chat_participants": _support("whatsapp", "ingested_at"),
    "whatsapp_contacts": _support("whatsapp", "ingested_at"),
    "whatsapp_client_sessions": _state("whatsapp", "updated_at", "linked-device session snapshot"),
    # Apple Notes
    "apple_note_revisions": _data("apple_notes", "ingested_at", "modified_at"),
    "apple_notes": _support("apple_notes", "ingested_at", "modified_at", "current note state"),
    "apple_note_attachments": _support("apple_notes", "ingested_at"),
    # Voice memos
    "apple_voice_memos_files": _data("apple_voice_memos", "ingested_at", "recorded_at"),
    "apple_voice_memos_transcription_runs": _data("voice_memo_transcription", "requested_at"),
    "apple_voice_memos_transcript_segments": _support("voice_memo_transcription", "created_at"),
    "apple_voice_memos_enrichments": _data("voice_memo_enrichment", "created_at", "start_at"),
    # Alice
    "alice_voice_recordings": _data("alice_voice_recordings", "ingested_at", "recorded_at"),
    "alice_voice_recording_artifacts": _support("alice_voice_recordings", "ingested_at"),
    # Photos
    "apple_photos_files": _data("apple_photos", "ingested_at", "captured_at"),
    "photo_assets": _data("photo_identity", "updated_at", "capture_ts"),
    "photo_asset_files": _support("photo_identity", "created_at"),
    "media_fingerprints": _support("photo_identity", "created_at", note="perceptual-hash cache"),
    # AI conversation sources
    "claude_code_events": _data("claude_code", "ingested_at", "occurred_at"),
    "codex_events": _data("codex", "ingested_at", "occurred_at"),
    "openclaw_events": _data("openclaw", "ingested_at", "occurred_at"),
    "pi_events": _data("pi", "ingested_at", "occurred_at"),
    "claude_desktop_events": _data("claude_desktop", "ingested_at", "occurred_at"),
    "claude_desktop_credentials": _state("claude_desktop", "updated_at", "pushed claude.ai session key"),
    "claude_desktop_conversation_state": _state("claude_desktop", "last_synced_at"),
    "chatgpt_events": _data("chatgpt", "ingested_at", "occurred_at"),
    "chatgpt_sessions": _state("chatgpt", "updated_at", "published chatgpt.com session"),
    "chatgpt_conversation_sync": _state("chatgpt", "synced_at"),
    # WHOOP
    "whoop_cycles": _data("whoop", "synced_at", "start_at"),
    "whoop_recoveries": _data("whoop", "synced_at", "created_at"),
    "whoop_sleeps": _data("whoop", "synced_at", "start_at"),
    "whoop_workouts": _data("whoop", "synced_at", "start_at"),
    "whoop_profiles": _support("whoop", "synced_at"),
    "whoop_body_measurements": _support("whoop", "synced_at"),
    "whoop_sync_state": _state("whoop", "updated_at"),
    "whoop_oauth_tokens": _state("whoop", "updated_at", "rotating OAuth credential"),
    # WHOOP private API. A separate pipeline from `whoop`, deliberately: it has
    # its own credential (a captured browser session, not the OAuth app) and
    # its own cadence, so one of them dying must not be hidden by the other
    # still writing. The cycle/sleep/recovery/workout tables here duplicate the
    # public ones at higher resolution, so they are still `data` — this
    # pipeline going quiet is a real outage even though base_whoop keeps
    # filling.
    "whoop_private_cycles": _data("whoop_private", "synced_at", "start_at"),
    "whoop_private_sleeps": _data("whoop_private", "synced_at", "start_at"),
    "whoop_private_recoveries": _data(
        "whoop_private",
        "synced_at",
        "created_at",
        note="a recovery has no span of its own; it is scored against the cycle that precedes it",
    ),
    "whoop_private_workouts": _data("whoop_private", "synced_at", "start_at"),
    "whoop_private_sleep_events": _data("whoop_private", "synced_at", "started_at"),
    "whoop_private_heart_rate_samples": _data(
        "whoop_private",
        "synced_at",
        "sample_at",
        note="the per-6-second series; only synced_at leads an index, so event time may read as skipped",
    ),
    "whoop_private_workout_heart_rate_samples": _data("whoop_private", "synced_at", "sample_at"),
    "whoop_private_journal_entries": _data("whoop_private", "synced_at", "day"),
    "whoop_private_documents": _data(
        "whoop_private",
        "synced_at",
        "collected_at",
        note="Tier-2 UI payloads kept as raw_json; collected_at is when the page was fetched",
    ),
    "whoop_private_sports": _support("whoop_private", "synced_at", note="sport catalog"),
    "whoop_private_sync_state": _state("whoop_private", "updated_at"),
    "whoop_private_sessions": _state(
        "whoop_private", "updated_at", "rotating browser-session credential (24h access, 30d refresh)"
    ),
    # Plaid
    "plaid_transactions": _data("plaid", "synced_at", "posted_at"),
    "plaid_investment_transactions": _data("plaid", "synced_at", "transaction_at"),
    "plaid_accounts": _data("plaid", "synced_at", note="authoritative balance snapshot"),
    "plaid_investment_holdings": _data("plaid", "synced_at"),
    "plaid_liabilities": _data("plaid", "synced_at"),
    "plaid_items": _support("plaid", "synced_at", "linked_at"),
    "plaid_investment_securities": _support("plaid", "synced_at"),
    "plaid_sync_state": _state("plaid", "updated_at", "per-item/product cursor, status, and error"),
    "plaid_item_tokens": _state("plaid", "updated_at", "private access tokens"),
    # Manual finance documents
    "manual_finance_documents": _data("manual_finance", "ingested_at", "file_modified_at"),
    "manual_finance_extractions": _data("manual_finance_extraction", "created_at", "ai_processed_at"),
    # Finance ledger
    "finance_observations": _data("finance_ledger", "observed_at", "as_of"),
    "finance_transactions": _data("finance_ledger", "created_at", "posted_at"),
    "finance_accounts": _support("finance_ledger", "updated_at"),
    "finance_account_links": _support("finance_ledger", "created_at", note="identity resolution audit"),
    "finance_transaction_links": _support("finance_ledger", "created_at"),
    "finance_security_transactions": _data("finance_ledger", "created_at", "trade_date"),
    "finance_security_transaction_links": _support("finance_ledger", "created_at"),
    "finance_tax_lots": _support("finance_ledger", "created_at", note="FIFO reduction of the trade ledger"),
    "receipt_transaction_receipts": _data("receipt_enrichment", "updated_at", "purchased_at"),
    # Shared attachment/media enrichment
    "file_attachment_enrichments": _data("attachment_enrichment", "updated_at", "ai_processed_at"),
    "gmail_attachment_backfill_state": _state("attachment_enrichment", "updated_at"),
    # Timeline
    "timeline_events": _data(
        "timeline",
        "updated_at",
        "event_ts",
        "43M rows and no updated_at index: writes are read from timeline_sync_state instead",
    ),
    "timeline_sync_state": _state("timeline", "updated_at", "per-adapter cursors, run time, and errors"),
    "timeline_gmail_correspondents": _support("timeline", "refreshed_at"),
    # The search interface belongs to the timeline layer (timeline.search_text
    # lives beside timeline.events), so its DDL-signature marker is timeline
    # state rather than a pipeline of its own.
    "search_schema_state": _state("timeline", None, "search DDL signature marker; carries no timestamp"),
    # Derived search-retrieval layer (search_index.py)
    "search_chunks": _data("search_index", "built_at", "event_ts"),
    "search_chunk_embeddings": _support(
        "search_index",
        "embedded_at",
        note="embedding coverage lags chunks by design; staleness here must not page while unconfigured",
    ),
    "search_chunk_sync_state": _state("search_index", "updated_at", "timeline seq watermark"),
    # Upstream mutations
    "upstream_mutations": _data("upstream_mutations", "updated_at", "created_at"),
    "upstream_mutation_requests": _data("upstream_mutations", "updated_at", "created_at"),
    "upstream_mutation_events": _support("upstream_mutations", "created_at"),
    "upstream_mutation_request_events": _support("upstream_mutations", "created_at"),
    # This snapshot itself
    "pipeline_health": _data("pipeline_health", "collected_at"),
    "pipeline_table_freshness": _support("pipeline_health", "collected_at"),
    "mart_view_health": _support(
        "pipeline_health",
        "collected_at",
        note="level 2: the marts_* read interface, measured on inputs rather than a stamped column",
    ),
    "collation_health": _data(
        "collation_health",
        "collected_at",
        note="collation baselines and the corroborating unique-index divergence probe",
    ),
    # The warehouse's own enrichment agent
    "agent_runs": _data("enrichment_agent", "started_at", "started_at"),
    "agent_run_events": _support("enrichment_agent", "created_at"),
    "agent_run_tool_calls": _support("enrichment_agent", "started_at"),
}


_PIPELINES_BY_ID = {entry.id: entry for entry in PIPELINES}


def pipeline(pipeline_id: str) -> Pipeline:
    try:
        return _PIPELINES_BY_ID[pipeline_id]
    except KeyError as exc:
        raise KeyError(f"unknown pipeline {pipeline_id!r}") from exc


def pipeline_tables(pipeline_id: str, *, role: str | None = None) -> tuple[str, ...]:
    return tuple(
        table
        for table, coverage in TABLE_PIPELINES.items()
        if coverage.pipeline == pipeline_id and (role is None or coverage.role == role)
    )


@dataclass
class TableSnapshot:
    """One probed table, as written to ``ops.pipeline_table_freshness``."""

    table_id: str
    pipeline: str
    role: str
    layer: str
    table_schema: str
    table_name: str
    written_at_column: str
    event_at_column: str
    last_write_at: datetime | None
    newest_event_at: datetime | None
    row_estimate: int
    byte_size: int
    probe_status: str
    probe_detail: str
    probe_ms: int
    note: str


@dataclass
class PipelineSnapshot:
    """One pipeline's roll-up, as written to ``ops.pipeline_health``."""

    pipeline: str
    label: str
    kind: str
    cadence: str
    transport: str
    note: str
    data_basis: str
    expected_data_interval_seconds: int
    expected_run_interval_seconds: int
    expected_event_interval_seconds: int
    last_write_at: datetime | None
    newest_event_at: datetime | None
    last_run_at: datetime | None
    #: How many ``data`` tables actually yielded an event timestamp. Zero with a
    #: nonzero expected_event_interval means the event columns exist but were
    #: too expensive to probe — "unmeasured", which the view must not render as
    #: "no data has ever arrived".
    event_tables_probed: int
    row_estimate: int
    byte_size: int
    table_count: int
    tables_probed: int
    tables_skipped: int
    state_table: str
    state_rows: int
    state_error_rows: int
    state_attention_rows: int
    last_error: str
    last_error_at: datetime | None


@dataclass
class MartViewSnapshot:
    """One probed ``marts_*`` view, as written to ``ops.mart_view_health``.

    Facts only, exactly like the table snapshot: what the view reads, whether
    it currently returns a row, how its definition hashes, and how fresh the
    stalest thing it reads is. The verdict is derived at read time by
    ``marts_ops.mart_view_health``, so a snapshot that stops refreshing reports
    ``unknown`` rather than yesterday's green.
    """

    view_id: str
    domain: str
    view_schema: str
    view_name: str
    #: Logical ids of the base TABLES this view reads, transitively through any
    #: intermediate views. Resolved from pg_depend, never hand-maintained.
    input_tables: list[str]
    #: The pipelines those tables belong to — what actually gets judged.
    input_pipelines: list[str]
    input_count: int
    #: The input pipeline furthest past its own SLA, and the numbers needed to
    #: re-derive that judgement live.
    stalest_pipeline: str
    stalest_pipeline_at: datetime | None
    stalest_pipeline_expected_seconds: int
    inputs_unmeasured: int
    has_rows: int
    definition_sha256: str
    #: When THIS definition sha was first observed. A change resets it, which is
    #: how a silent redefinition becomes visible.
    first_seen_at: datetime | None
    probe_status: str
    probe_detail: str
    probe_ms: int
    note: str


def _real(value: datetime | None) -> datetime | None:
    """Collapse the warehouse's epoch sentinel to a real absence."""
    if value is None:
        return None
    if isinstance(value, datetime):
        moment = value if value.tzinfo else value.replace(tzinfo=UTC)
        return None if moment <= EPOCH_GUARD else moment
    # DATE columns (finance as_of, receipt purchased_at) come back as dates.
    stamp = datetime(value.year, value.month, value.day, tzinfo=UTC)
    return None if stamp <= EPOCH_GUARD else stamp


def _newest(*values: datetime | None) -> datetime | None:
    real = [value for value in (_real(value) for value in values) if value is not None]
    return max(real) if real else None


class PipelineHealthCollector:
    """Probes every registered table and writes the freshness snapshot.

    One collection is: one catalog read for sizes, one for index leading
    columns, then a bounded ``max()`` probe per (table, column) the catalog says
    is cheap, then two upserts. Nothing here mutates source data.
    """

    def __init__(self, warehouse, *, now: Any = None) -> None:
        self._warehouse = warehouse
        self._now = now or (lambda: datetime.now(tz=UTC))

    # -- collection --------------------------------------------------------

    def collect(self) -> tuple[list[PipelineSnapshot], list[TableSnapshot]]:
        stats = self._relation_stats()
        indexed = self._indexed_leading_columns()
        # The cost guard above keeps probes cheap in the expected case; the
        # timeout is the backstop for the unexpected one (a bloated index, a
        # concurrent rewrite holding pages hostage). A probe that trips it is
        # recorded as a timeout instead of stretching the collection window.
        with self._probe_budget(PROBE_STATEMENT_TIMEOUT_MS):
            tables = [
                self._probe_table(table_id, coverage, stats, indexed)
                for table_id, coverage in sorted(TABLE_PIPELINES.items())
            ]
            by_pipeline: dict[str, list[TableSnapshot]] = {}
            for snapshot in tables:
                by_pipeline.setdefault(snapshot.pipeline, []).append(snapshot)
            pipelines = [
                self._roll_up(entry, by_pipeline.get(entry.id, [])) for entry in PIPELINES
            ]
        return pipelines, tables

    def collect_marts(
        self, pipelines: Sequence[PipelineSnapshot], tables: Sequence[TableSnapshot]
    ) -> list[MartViewSnapshot]:
        """Measure every ``marts_*`` view against the snapshot just taken.

        Takes the pipeline and table snapshots rather than re-reading them, so a
        view's input freshness is measured at the same instant as its inputs' —
        a roll-up assembled from two different collections could report a view
        as staler than anything it reads.
        """
        by_table = {snapshot.table_id: snapshot for snapshot in tables}
        by_pipeline = {snapshot.pipeline: snapshot for snapshot in pipelines}
        inputs = self._view_input_tables()
        definitions = self._view_definitions()
        previous = self._previous_definition_shas()
        now = self._now()
        with self._probe_budget(MART_PROBE_STATEMENT_TIMEOUT_MS):
            return [
                self._probe_mart_view(
                    view_id, by_table, by_pipeline, inputs, definitions, previous, now
                )
                for view_id in mart_view_ids()
            ]

    def run_all(
        self,
    ) -> tuple[list[PipelineSnapshot], list[TableSnapshot], list[MartViewSnapshot]]:
        """Collect and persist everything this collector measures.

        One ``collected_at`` for all three snapshots, so the views' staleness
        guard applies to the whole dashboard at once rather than letting one
        surface silently outlive another.
        """
        pipelines, tables = self.collect()
        marts = self.collect_marts(pipelines, tables)
        collected_at = self._now()
        self._warehouse.write_pipeline_health(pipelines, tables, collected_at=collected_at)
        self._warehouse.write_mart_view_health(marts, collected_at=collected_at)
        return pipelines, tables, marts

    def run(self) -> tuple[list[PipelineSnapshot], list[TableSnapshot]]:
        """Collect and persist, returning the pipeline and table snapshots."""
        pipelines, tables, _ = self.run_all()
        return pipelines, tables

    @contextmanager
    def _probe_budget(self, milliseconds: int):
        self._warehouse._raw_command(f"SET statement_timeout = {int(milliseconds)}")
        try:
            yield
        finally:
            self._warehouse._raw_command("SET statement_timeout = DEFAULT")

    # -- catalog reads -----------------------------------------------------

    def _relation_stats(self) -> dict[tuple[str, str], tuple[int, int]]:
        rows = self._warehouse._query(
            """
            SELECT n.nspname, c.relname, c.reltuples, pg_total_relation_size(c.oid)
            FROM pg_class AS c
            INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = ANY(%s) AND c.relkind = 'r'
            """,
            (self._warehouse.physical_schema_names(include_hidden=True),),
        )
        # reltuples is -1 on a table that has never been analyzed.
        return {
            (schema, name): (max(0, int(tuples)), int(size or 0))
            for schema, name, tuples, size in rows
        }

    def _indexed_leading_columns(self) -> set[tuple[str, str, str]]:
        """(schema, table, column) triples an index can serve ``max()`` from.

        Only the leading key column counts: a backward scan of
        ``(session_id, seq)`` cannot answer ``max(seq)`` cheaply.
        """
        rows = self._warehouse._query(
            """
            SELECT n.nspname, c.relname, a.attname
            FROM pg_index AS i
            INNER JOIN pg_class AS c ON c.oid = i.indrelid
            INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
            INNER JOIN pg_attribute AS a
              ON a.attrelid = c.oid AND a.attnum = i.indkey[0]
            WHERE n.nspname = ANY(%s) AND i.indisvalid
            """,
            (self._warehouse.physical_schema_names(include_hidden=True),),
        )
        return {(schema, table, column) for schema, table, column in rows}

    # -- mart (view) catalog reads ----------------------------------------

    def _relation_dependencies(self) -> dict[tuple[str, str], set[tuple[str, str, str]]]:
        """Direct relation-level dependencies of every view in our schemas.

        ``pg_depend`` records a view's dependencies against its ``pg_rewrite``
        rule, not against the view relation, which is why the join goes through
        ``pg_rewrite``. Column-level rows are collapsed to the relation and the
        view's dependency on itself is dropped, leaving one edge per
        (view, relation it reads).
        """
        rows = self._warehouse._query(
            """
            SELECT vn.nspname, vc.relname, dn.nspname, dc.relname, dc.relkind
            FROM pg_rewrite AS r
            INNER JOIN pg_class AS vc ON vc.oid = r.ev_class
            INNER JOIN pg_namespace AS vn ON vn.oid = vc.relnamespace
            INNER JOIN pg_depend AS d
              ON d.objid = r.oid
             AND d.classid = 'pg_rewrite'::regclass
             AND d.refclassid = 'pg_class'::regclass
            INNER JOIN pg_class AS dc ON dc.oid = d.refobjid
            INNER JOIN pg_namespace AS dn ON dn.oid = dc.relnamespace
            WHERE vc.relkind = 'v'
              AND vn.nspname = ANY(%s)
              AND d.refobjid <> r.ev_class
            """,
            (self._warehouse.physical_schema_names(include_hidden=True),),
        )
        edges: dict[tuple[str, str], set[tuple[str, str, str]]] = {}
        for view_schema, view_name, dep_schema, dep_name, dep_kind in rows:
            edges.setdefault((view_schema, view_name), set()).add(
                (dep_schema, dep_name, dep_kind)
            )
        return edges

    def _view_input_tables(self) -> dict[tuple[str, str], list[str]]:
        """Resolve each view to the logical ids of the base tables it reads.

        Views read views (``marts_finance.net_worth`` reads
        ``marts_derived_finance.accounts``), so the edges are closed
        transitively down to relkind 'r'. The transitive closure is done here
        rather than in a recursive CTE because it has to terminate on a cycle —
        Postgres permits mutually recursive views — and a visited set is the
        clearest way to say that.
        """
        edges = self._relation_dependencies()
        physical_to_logical = {
            (rel.with_namespace(self._warehouse.schema_namespace).schema, rel.name): logical
            for logical, rel in CANONICAL_RELATIONS.items()
            if logical in TABLE_PIPELINES
        }

        def resolve(start: tuple[str, str]) -> list[str]:
            seen: set[tuple[str, str]] = set()
            queue = [start]
            found: set[str] = set()
            while queue:
                node = queue.pop()
                if node in seen:
                    continue
                seen.add(node)
                for dep_schema, dep_name, dep_kind in edges.get(node, ()):
                    if dep_kind == "v":
                        queue.append((dep_schema, dep_name))
                        continue
                    logical = physical_to_logical.get((dep_schema, dep_name))
                    if logical is not None:
                        found.add(logical)
            return sorted(found)

        return {view: resolve(view) for view in edges}

    def _view_definitions(self) -> dict[tuple[str, str], str]:
        rows = self._warehouse._query(
            """
            SELECT n.nspname, c.relname, pg_get_viewdef(c.oid, true)
            FROM pg_class AS c
            INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE c.relkind = 'v' AND n.nspname = ANY(%s)
            """,
            (self._warehouse.physical_schema_names(include_hidden=True),),
        )
        return {(schema, name): definition or "" for schema, name, definition in rows}

    def _previous_definition_shas(self) -> dict[str, tuple[str, datetime | None]]:
        """The last collection's definition hash per view, to detect a change.

        A first collection (or a database that predates the table) simply has
        no history, which is treated as "this definition was first seen now"
        rather than as a change — an empty table must not report the whole
        marts layer as freshly redefined.
        """
        try:
            rows = self._warehouse._query(
                "SELECT view_id, definition_sha256, first_seen_at FROM @mart_view_health"
            )
        except psycopg2.Error as error:
            logger.info("no previous mart definition hashes: %s", error)
            return {}
        return {view_id: (sha or "", _real(seen)) for view_id, sha, seen in rows}

    # -- probes ------------------------------------------------------------

    def _probe_table(
        self,
        table_id: str,
        coverage: TableFreshness,
        stats: dict[tuple[str, str], tuple[int, int]],
        indexed: set[tuple[str, str, str]],
    ) -> TableSnapshot:
        relation = canonical_relation(table_id).with_namespace(self._warehouse.schema_namespace)
        key = (relation.schema, relation.name)
        row_estimate, byte_size = stats.get(key, (0, 0))
        snapshot = TableSnapshot(
            table_id=table_id,
            pipeline=coverage.pipeline,
            role=coverage.role,
            table_schema=relation.schema,
            table_name=relation.name,
            written_at_column=coverage.written_at or "",
            event_at_column=coverage.event_at or "",
            layer=CATALOG.object(table_id).layer,
            last_write_at=None,
            newest_event_at=None,
            row_estimate=row_estimate,
            byte_size=byte_size,
            probe_status=PROBE_OK,
            probe_detail="",
            probe_ms=0,
            note=coverage.note,
        )
        if key not in stats:
            snapshot.probe_status = PROBE_MISSING
            snapshot.probe_detail = "relation does not exist in this database"
            return snapshot
        if coverage.written_at is None and coverage.event_at is None:
            snapshot.probe_status = PROBE_NO_TIMESTAMP
            snapshot.probe_detail = coverage.note or "table carries no timestamp column"
            return snapshot

        started = time.monotonic()
        statuses: list[str] = []
        details: list[str] = []
        for column, assign, cap_to_now in (
            (coverage.written_at, "last_write_at", False),
            # Event time is capped at now(): the timeline legitimately holds
            # calendar events years in the future, and "newest data we hold"
            # must not read as 2038. An index still serves the capped max as a
            # backward scan from now().
            (coverage.event_at, "newest_event_at", True),
        ):
            if not column:
                continue
            probeable, reason = self._probeable(
                relation.schema, relation.name, column, byte_size, indexed
            )
            if not probeable:
                statuses.append(PROBE_SKIPPED_UNINDEXED)
                details.append(f"{column}: {reason}")
                continue
            value, status, detail = self._max_timestamp(
                relation.schema, relation.name, column, cap_to_now=cap_to_now
            )
            statuses.append(status)
            if detail:
                details.append(f"{column}: {detail}")
            setattr(snapshot, assign, value)
        snapshot.probe_ms = int((time.monotonic() - started) * 1000)
        snapshot.probe_detail = "; ".join(details)
        snapshot.probe_status = _worst_probe_status(statuses)
        if snapshot.probe_status == PROBE_OK and row_estimate == 0 and snapshot.last_write_at is None:
            snapshot.probe_status = PROBE_EMPTY
        return snapshot

    def _probe_mart_view(
        self,
        view_id: str,
        by_table: dict[str, TableSnapshot],
        by_pipeline: dict[str, PipelineSnapshot],
        inputs: dict[tuple[str, str], list[str]],
        definitions: dict[tuple[str, str], str],
        previous: dict[str, tuple[str, datetime | None]],
        now: datetime,
    ) -> MartViewSnapshot:
        obj = CATALOG.object(view_id)
        relation = canonical_relation(view_id).with_namespace(self._warehouse.schema_namespace)
        key = (relation.schema, relation.name)
        input_tables = inputs.get(key, [])
        definition = definitions.get(key)
        snapshot = MartViewSnapshot(
            view_id=view_id,
            domain=obj.domain,
            view_schema=relation.schema,
            view_name=relation.name,
            input_tables=input_tables,
            input_pipelines=[],
            input_count=len(input_tables),
            stalest_pipeline="",
            stalest_pipeline_at=None,
            stalest_pipeline_expected_seconds=0,
            inputs_unmeasured=0,
            has_rows=0,
            definition_sha256="",
            first_seen_at=None,
            probe_status=PROBE_OK,
            probe_detail="",
            probe_ms=0,
            note=obj.comment or "",
        )
        if definition is None:
            snapshot.probe_status = PROBE_MISSING
            snapshot.probe_detail = "view does not exist in this database"
            return snapshot

        snapshot.definition_sha256 = hashlib.sha256(definition.encode("utf-8")).hexdigest()
        seen_before = previous.get(view_id)
        if seen_before is not None and seen_before[0] == snapshot.definition_sha256:
            snapshot.first_seen_at = seen_before[1] or now
        else:
            snapshot.first_seen_at = now

        self._roll_up_inputs(snapshot, by_table, by_pipeline)

        if view_id in EXPENSIVE_MART_VIEWS:
            snapshot.probe_status = PROBE_SKIPPED_EXPENSIVE
            snapshot.probe_detail = (
                "declared too expensive to probe every collection; input freshness "
                "and definition drift still apply"
            )
            return snapshot

        started = time.monotonic()
        sql = f"SELECT 1 FROM {_ident(relation.schema)}.{_ident(relation.name)} LIMIT 1"
        try:
            rows = self._warehouse._query(sql)
        except psycopg2.errors.QueryCanceled as error:
            snapshot.probe_status = PROBE_TIMEOUT
            snapshot.probe_detail = _one_line(str(error))[:500]
        except psycopg2.Error as error:
            snapshot.probe_status = PROBE_ERROR
            snapshot.probe_detail = _one_line(str(error))[:500]
        else:
            snapshot.has_rows = 1 if rows else 0
            snapshot.probe_status = PROBE_OK if rows else PROBE_EMPTY
        snapshot.probe_ms = int((time.monotonic() - started) * 1000)
        return snapshot

    def _roll_up_inputs(
        self,
        snapshot: MartViewSnapshot,
        by_table: dict[str, TableSnapshot],
        by_pipeline: dict[str, PipelineSnapshot],
    ) -> None:
        """Pick the input PIPELINE furthest past its own SLA.

        Two decisions here, both of which were measured against production
        before being made:

        **Judge per pipeline, not per table.** The registry declares an SLA per
        pipeline and the pipeline's own freshness is a ``max()`` over its data
        tables — deliberately, because a pipeline is not broken just because one
        of its tables is quiet. Judging an individual input table against its
        pipeline's SLA breaks that: measured 2026-08-23, it reported four marts
        'stale' because ``derived_finance.transactions`` was 1.1 days old
        against ``finance_ledger``'s three-hour interval, while the ledger was
        writing balance observations every half hour exactly as designed. Those
        are false positives, and a monitoring change that cries wolf is worse
        than the gap it closes. So a mart can never be more broken than the
        pipelines feeding it; the *per-table* detail lives in
        ``marts_ops.table_freshness``, which is where to look when a quiet table
        inside a healthy pipeline is the question.

        **Rank by age relative to SLA, not by raw age.**
        ``marts_ai_conversations.events`` reads six agent sources whose
        expectations differ by an order of magnitude, so "oldest" would
        permanently nominate whichever source is legitimately the quietest
        instead of the one actually misbehaving. The interval is stored beside
        the timestamp so the view re-derives the verdict at read time rather
        than trusting a stored one.

        ``state`` tables are excluded: a cursor's write time is the pipeline's
        heartbeat, not the freshness of anything the mart shows.
        """
        pipelines: list[str] = []
        for table_id in snapshot.input_tables:
            table = by_table.get(table_id)
            if table is None or table.role == "state":
                continue
            if table.pipeline not in pipelines:
                pipelines.append(table.pipeline)
        snapshot.input_pipelines = sorted(pipelines)

        worst_ratio = -1.0
        unmeasured = 0
        for pipeline_id in snapshot.input_pipelines:
            entry = by_pipeline.get(pipeline_id)
            expected = entry.expected_data_interval_seconds if entry else 0
            written = entry.last_write_at if entry else None
            if written is None or expected <= 0:
                unmeasured += 1
                continue
            ratio = (self._now() - written).total_seconds() / expected
            if ratio > worst_ratio:
                worst_ratio = ratio
                snapshot.stalest_pipeline = pipeline_id
                snapshot.stalest_pipeline_at = written
                snapshot.stalest_pipeline_expected_seconds = expected
        snapshot.inputs_unmeasured = unmeasured

    def _probeable(
        self,
        schema: str,
        table: str,
        column: str,
        byte_size: int,
        indexed: set[tuple[str, str, str]],
    ) -> tuple[bool, str]:
        if (schema, table, column) in indexed:
            return True, ""
        if byte_size <= PROBE_MAX_UNINDEXED_BYTES:
            return True, ""
        return False, (
            f"no index leads with it and the relation is {byte_size // (1024 * 1024)} MiB"
        )

    def _max_timestamp(
        self, schema: str, table: str, column: str, *, cap_to_now: bool = False
    ) -> tuple[datetime | None, str, str]:
        where = f" WHERE {_ident(column)} <= now()" if cap_to_now else ""
        sql = (
            f"SELECT max({_ident(column)})::timestamptz "
            f"FROM {_ident(schema)}.{_ident(table)}{where}"
        )
        try:
            rows = self._warehouse._query(sql)
        except psycopg2.errors.QueryCanceled as error:
            return None, PROBE_TIMEOUT, _one_line(str(error))
        except psycopg2.Error as error:
            return None, PROBE_ERROR, _one_line(str(error))
        value = rows[0][0] if rows else None
        return _real(value), PROBE_OK, ""

    # -- roll-up -----------------------------------------------------------

    def _roll_up(self, entry: Pipeline, tables: Sequence[TableSnapshot]) -> PipelineSnapshot:
        data_tables = [table for table in tables if table.role == "data"]
        state_tables = [table for table in tables if table.role == "state"]
        state = self._state_aggregate(entry)
        # Event time is only judgeable where a data table actually declares an
        # event column. Where none does (the freshness collector's own snapshot,
        # the Slack fingerprint link table), the expectation is recorded as
        # zero — "not monitored" — rather than left to read as "no data has ever
        # arrived", which is a different and much louder claim.
        event_columns = [table for table in data_tables if table.event_at_column]
        expected_event = _seconds(entry.event_interval) if event_columns else 0
        return PipelineSnapshot(
            pipeline=entry.id,
            label=entry.label,
            kind=entry.kind,
            cadence=entry.cadence,
            transport=entry.transport,
            note=entry.note,
            data_basis=entry.data_basis,
            expected_data_interval_seconds=_seconds(entry.expected_data_interval),
            expected_run_interval_seconds=_seconds(entry.expected_run_interval),
            expected_event_interval_seconds=expected_event,
            last_write_at=_newest(*(table.last_write_at for table in data_tables)),
            newest_event_at=_newest(*(table.newest_event_at for table in data_tables)),
            last_run_at=_newest(
                state.get("last_run_at"), *(table.last_write_at for table in state_tables)
            ),
            event_tables_probed=sum(
                1 for table in event_columns if table.newest_event_at is not None
            ),
            row_estimate=sum(table.row_estimate for table in data_tables),
            byte_size=sum(table.byte_size for table in tables),
            table_count=len(tables),
            tables_probed=sum(
                1 for table in tables if table.probe_status in {PROBE_OK, PROBE_EMPTY}
            ),
            tables_skipped=sum(
                1
                for table in tables
                if table.probe_status
                in {PROBE_SKIPPED_UNINDEXED, PROBE_TIMEOUT, PROBE_ERROR, PROBE_MISSING}
            ),
            state_table=entry.state.table if entry.state else "",
            state_rows=int(state.get("rows", 0) or 0),
            state_error_rows=int(state.get("error_rows", 0) or 0),
            state_attention_rows=int(state.get("attention_rows", 0) or 0),
            last_error=str(state.get("last_error", "") or ""),
            last_error_at=_real(state.get("last_error_at")),
        )

    def _state_aggregate(self, entry: Pipeline) -> dict[str, Any]:
        """Read one pipeline's heartbeat and failure counts from its state table.

        Every sync-state table shares the ``status``/``error``/timestamp shape,
        so the aggregate is built from the declaration rather than per-source
        SQL. A pipeline without a state table (a laptop uploader) returns
        nothing, which the status ladder reads as "no heartbeat available".
        """
        source = entry.state
        if source is None:
            return {}
        relation = canonical_relation(source.table).with_namespace(self._warehouse.schema_namespace)
        updated = _ident(source.updated_column)
        selects = [f"max({updated})::timestamptz AS last_run_at", "count(*)::bigint AS rows"]
        if source.status_column:
            status = _ident(source.status_column)
            selects.append(
                f"count(*) FILTER (WHERE {status} = ANY(%(errors)s))::bigint AS error_rows"
            )
            selects.append(
                f"count(*) FILTER (WHERE {status} = ANY(%(attention)s))::bigint AS attention_rows"
            )
        else:
            selects.append("0::bigint AS error_rows")
            selects.append("0::bigint AS attention_rows")
        if source.error_column:
            error = _ident(source.error_column)
            # The newest non-empty error, with the timestamp that recorded it.
            # When the table carries a status, only alarm-worthy rows qualify:
            # a terminal expected state (slack's 'gone' channels) keeps its
            # failure text as the reason it was closed out, and that text must
            # not resurface as the pipeline's current failure banner.
            error_filter = f"COALESCE({error}, '') != ''"
            if source.status_column:
                error_filter += f" AND {_ident(source.status_column)} = ANY(%(alarm)s)"
            selects.append(
                f"(array_agg({error} ORDER BY {updated} DESC) "
                f"FILTER (WHERE {error_filter}))[1] AS last_error"
            )
            selects.append(
                f"max({updated}) FILTER (WHERE {error_filter})::timestamptz "
                "AS last_error_at"
            )
        else:
            selects.append("'' AS last_error")
            selects.append("NULL::timestamptz AS last_error_at")
        sql = (
            f"SELECT {', '.join(selects)} FROM {_ident(relation.schema)}.{_ident(relation.name)}"
        )
        params = {
            "errors": list(source.error_statuses),
            "attention": list(source.attention_statuses),
            "alarm": list(source.error_statuses) + list(source.attention_statuses),
        }
        try:
            rows = self._warehouse._query_dicts(sql, params)
        except psycopg2.Error as error:
            logger.warning("pipeline %s state aggregate failed: %s", entry.id, error)
            return {}
        row = rows[0] if rows else {}
        if row.get("last_error"):
            row["last_error"] = _one_line(str(row["last_error"]))[:500]
        return row


def _worst_probe_status(statuses: Iterable[str]) -> str:
    ladder = (
        PROBE_ERROR,
        PROBE_TIMEOUT,
        PROBE_SKIPPED_UNINDEXED,
        PROBE_SKIPPED_EXPENSIVE,
        PROBE_NO_TIMESTAMP,
        PROBE_EMPTY,
        PROBE_OK,
    )
    seen = set(statuses)
    for status in ladder:
        if status in seen:
            return status
    return PROBE_OK


def _seconds(interval: timedelta | None) -> int:
    return int(interval.total_seconds()) if interval else 0


def _one_line(text: str) -> str:
    return " ".join(text.split())


def _ident(value: str) -> str:
    if not value.replace("_", "a").isalnum() or value[0].isdigit():
        raise ValueError(f"invalid SQL identifier: {value!r}")
    return '"' + value + '"'
