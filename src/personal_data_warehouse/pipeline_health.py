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

import logging
import time
from collections.abc import Iterable, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg2

from personal_data_warehouse.relations import CATALOG, relation as canonical_relation

logger = logging.getLogger(__name__)

__all__ = [
    "ACCOUNT_BASELINE_GAPS",
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
PROBE_TIMEOUT = "timeout"
PROBE_ERROR = "error"
PROBE_MISSING = "missing"


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


@dataclass(frozen=True)
class Pipeline:
    """One thing that keeps part of the warehouse up to date.

    ``expected_data_interval`` is how long the pipeline may go without writing a
    payload row before something is probably wrong — a judgement about the real
    world (email arrives hourly, voice memos monthly), not the poll cadence.
    ``expected_run_interval`` is the poll cadence itself, and applies only when
    the pipeline keeps a heartbeat in ``state`` (a laptop uploader does not).
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
    state: StateSource | None = None
    note: str = ""


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
        state=state,
        note=note,
    )


DAY = timedelta(days=1)
HOUR = timedelta(hours=1)
MINUTE = timedelta(minutes=1)


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
        data=30 * DAY,
        run=3 * HOUR,
        state=StateSource(
            table="contact_sync_state",
            updated_column="updated_at",
            status_column="status",
            error_column="error",
        ),
    ),
    _source(
        "apple_contacts",
        "Apple Contacts",
        cadence="uploader every 5 min",
        transport="Mac LaunchAgent → /ingest/apple-contacts/batch → Drive inbox → Dagster",
        data=30 * DAY,
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
        data=30 * DAY,
    ),
    _source(
        "alice_voice_recordings",
        "Alice Voice Recordings",
        cadence="daily 04:17",
        transport="Dagster alice_voice_recordings → Alice API",
        data=30 * DAY,
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
    ),
    _source(
        "openclaw",
        "OpenClaw sessions",
        cadence="systemd timer every 5 min",
        transport="openclaw VM systemd timer → /ingest/agent-sessions/batch",
        data=7 * DAY,
    ),
    _source(
        "pi",
        "pi sessions",
        cadence="uploader every 5 min",
        transport="Mac LaunchAgent tails ~/.pi/agent/sessions → /ingest/agent-sessions/batch",
        data=30 * DAY,
    ),
    _source(
        "claude_desktop",
        "Claude Desktop",
        cadence="poller every 5 min",
        transport="Dagster claude_desktop_client → claude.ai API with a pushed session key",
        data=7 * DAY,
        run=3 * HOUR,
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
        state=StateSource(
            table="chatgpt_sessions",
            updated_column="updated_at",
            status_column="status",
            error_column="error",
        ),
        note=(
            "needs a manual `pdw chatgpt publish-session` roughly weekly; every"
            " successful poll updates the credential heartbeat, and a rejected"
            " token remains action_required until a different token is published"
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
    ),
    Pipeline(
        id="voice_memo_transcription",
        label="Voice memo transcription",
        kind="enrichment",
        cadence="hourly",
        transport="Dagster apple_voice_memos_transcription → AssemblyAI",
        expected_data_interval=30 * DAY,
        expected_run_interval=None,
    ),
    Pipeline(
        id="voice_memo_enrichment",
        label="Voice memo enrichment",
        kind="enrichment",
        cadence="hourly :17",
        transport="Dagster apple_voice_memos_enrichment → agent container",
        expected_data_interval=30 * DAY,
        expected_run_interval=None,
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
    ),
    Pipeline(
        id="photo_identity",
        label="Photo identity & dedup",
        kind="derived",
        cadence="hourly :29",
        transport="Dagster photo_identity over every registered photo source",
        expected_data_interval=7 * DAY,
        expected_run_interval=None,
    ),
    Pipeline(
        id="finance_ledger",
        label="Finance ledger",
        kind="derived",
        cadence=":07 and :37 hourly",
        transport="Dagster finance_ledger over Plaid + manual_finance",
        expected_data_interval=3 * HOUR,
        expected_run_interval=None,
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
    # Upstream mutations
    "upstream_mutations": _data("upstream_mutations", "updated_at", "created_at"),
    "upstream_mutation_requests": _data("upstream_mutations", "updated_at", "created_at"),
    "upstream_mutation_events": _support("upstream_mutations", "created_at"),
    "upstream_mutation_request_events": _support("upstream_mutations", "created_at"),
    # This snapshot itself
    "pipeline_health": _data("pipeline_health", "collected_at"),
    "pipeline_table_freshness": _support("pipeline_health", "collected_at"),
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
    expected_data_interval_seconds: int
    expected_run_interval_seconds: int
    last_write_at: datetime | None
    newest_event_at: datetime | None
    last_run_at: datetime | None
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
        with self._probe_budget():
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

    def run(self) -> tuple[list[PipelineSnapshot], list[TableSnapshot]]:
        """Collect and persist, returning what was written."""
        pipelines, tables = self.collect()
        self._warehouse.write_pipeline_health(pipelines, tables, collected_at=self._now())
        return pipelines, tables

    @contextmanager
    def _probe_budget(self):
        self._warehouse._raw_command(f"SET statement_timeout = {PROBE_STATEMENT_TIMEOUT_MS}")
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
        return PipelineSnapshot(
            pipeline=entry.id,
            label=entry.label,
            kind=entry.kind,
            cadence=entry.cadence,
            transport=entry.transport,
            note=entry.note,
            expected_data_interval_seconds=_seconds(entry.expected_data_interval),
            expected_run_interval_seconds=_seconds(entry.expected_run_interval),
            last_write_at=_newest(*(table.last_write_at for table in data_tables)),
            newest_event_at=_newest(*(table.newest_event_at for table in data_tables)),
            last_run_at=_newest(
                state.get("last_run_at"), *(table.last_write_at for table in state_tables)
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
