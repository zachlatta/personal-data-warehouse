"""C11: a source's own SLA, detected by asking the source.

``marts_ops.pipeline_health`` says whether a pipeline RAN and WROTE. That is
not the same claim as "PDW holds what the source holds": a history cursor that
silently skips a change, a Drive changes feed capped behind a backlog, a
WhatsApp batch stuck between the linked device and the Drive inbox all leave
the pipeline green. The detector here compares the two ends directly.

Each collector that talks to a provider records what the provider says is its
newest item -- Gmail's newest message, a calendar's last modification, Drive's
newest modified file, the WhatsApp linked device's newest live message, the
Mac's own chat.db -- in ``ops.source_upstream_observations`` before it writes
what it fetched. ``marts_ops.source_upstream_health`` then asks, per
(pipeline, scope): has that item landed in ``base_*``? If not, how long has it
existed upstream? A newest item missing past the source's tolerance is
``late`` (``failing`` past six times it); an observation older than the
collector's own cadence allows is ``unknown``, because a detector that stopped
looking is not evidence of health.

Comparing the newest item rather than a count keeps every check one indexed
lookup: the view runs live, on the ten-minute ``pipeline_health`` collection
and on every ``/pipelines`` load.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

MINUTE = timedelta(minutes=1)
HOUR = timedelta(hours=1)

#: The logical id of the view every check below feeds.
SOURCE_UPSTREAM_HEALTH_VIEW = "marts_ops_source_upstream_health"

#: Past this multiple of the tolerance a missing newest item reads ``failing``.
FAILING_MULTIPLIER = 6

#: Timestamps are compared with this much slack: Gmail reports internalDate in
#: milliseconds and the base table keeps what the message row carried, so an
#: exact equality would read a landed message as missing.
LANDED_SLACK_SECONDS = 1


@dataclass(frozen=True)
class UpstreamCheck:
    """How one source's newest upstream item is matched against ``base_*``."""

    pipeline: str
    #: What one observation row's ``scope`` is (an account, a calendar).
    scope: str
    #: Where the observation comes from: the provider call that produced it.
    evidence: str
    #: SQL for the newest landed timestamp for observation ``o``. Must be an
    #: indexed or tiny lookup: the view is read live.
    pdw_newest_sql: str
    #: How long the source's newest item may exist upstream without having
    #: landed before it reads ``late``.
    tolerance: timedelta
    #: How old an observation may be before the detector itself is ``unknown``.
    observation_interval: timedelta
    #: How far PDW's newest may trail the source's newest stamp and still count
    #: as the same item: a source whose newest stamp is not an item's own
    #: field (a calendar's last modification) can stamp it a little later.
    landed_slack: timedelta = timedelta(seconds=LANDED_SLACK_SECONDS)


def _agent_session_check(pipeline: str, *, observation_interval: timedelta = HOUR) -> UpstreamCheck:
    """A device's transcripts: the newest event time and its session, read by
    ``pdw ingest agent-sessions`` (or ``muse``) from the files themselves, and
    the newest event PDW holds for that session (``(session_id, seq)`` index)."""
    return UpstreamCheck(
        pipeline=pipeline,
        scope="account/device",
        evidence=(
            "the device's own transcripts: the newest event time (the field the ingest"
            " reads as occurred_at) and its session, every uploader run"
        ),
        pdw_newest_sql=f"""(SELECT max(e.occurred_at) FROM @{pipeline}_events e
            WHERE e.session_id = o.upstream_newest_id
              AND e.account = split_part(o.scope, '/', 1)
              AND e.device = substr(o.scope, length(split_part(o.scope, '/', 1)) + 2))""",
        # Uploaded every five minutes, then the agent-sessions Drive reader.
        tolerance=HOUR,
        observation_interval=observation_interval,
    )


SOURCE_UPSTREAM_CHECKS: tuple[UpstreamCheck, ...] = (
    UpstreamCheck(
        pipeline="gmail",
        scope="mailbox address",
        evidence=(
            "users.messages.list(maxResults=1) + messages.get(format=minimal): the id and"
            " internalDate of the newest message Gmail lists, every reconcile pass (10 min)"
        ),
        pdw_newest_sql="""GREATEST(
            (SELECT max(m.internal_date) FROM @gmail_messages m WHERE m.account = o.scope),
            (SELECT m.internal_date FROM @gmail_messages m
             WHERE m.account = o.scope AND m.message_id = o.upstream_newest_id))""",
        # The loop polls history every 15 s and reconciles every 10 min.
        tolerance=15 * MINUTE,
        observation_interval=HOUR,
    ),
    UpstreamCheck(
        pipeline="google_calendar",
        scope="account/calendar_id",
        evidence=(
            "events.list(maxResults=1, showDeleted=true) top-level `updated`: the calendar's"
            " own last-modification time, every run (5 min)"
        ),
        pdw_newest_sql="""(SELECT max(e.updated_at) FROM @calendar_events e
            WHERE e.account = split_part(o.scope, '/', 1)
              AND e.calendar_id = substr(o.scope, length(split_part(o.scope, '/', 1)) + 2))""",
        tolerance=30 * MINUTE,
        observation_interval=HOUR,
        # Google stamps the calendar's `updated` after the event's own: on
        # 2026-11-12 the calendar read 1.01 s past its newest event, so the
        # shared one-second slack read a fully landed calendar as missing it.
        landed_slack=timedelta(seconds=10),
    ),
    UpstreamCheck(
        pipeline="google_drive",
        scope="account",
        evidence=(
            "files.list(orderBy='modifiedTime desc', pageSize=1) over the files the sync"
            " crawls: the newest modified file Drive has, every run (30 min)"
        ),
        # The partial index covers live, unexcluded files; the newest upstream
        # file may be one the sync keeps as excluded, so it is also looked up
        # by its primary key.
        pdw_newest_sql="""GREATEST(
            (SELECT max(f.modified_time) FROM @google_drive_files f
             WHERE f.account = o.scope AND f.trashed = 0 AND f.is_excluded = 0),
            (SELECT f.modified_time FROM @google_drive_files f
             WHERE f.account = o.scope AND f.file_id = o.upstream_newest_id))""",
        # Each run takes a capped slice of the changes feed, so a burst can
        # legitimately need a few half-hourly runs.
        tolerance=2 * HOUR,
        observation_interval=4 * HOUR,
    ),
    UpstreamCheck(
        pipeline="whatsapp",
        scope="account",
        evidence=(
            "the linked-device client's newest live message (MessageEv timestamp), recorded"
            " while connected; it lands via the Drive inbox and the Dagster reader"
        ),
        pdw_newest_sql="""(SELECT max(w.message_at) FROM @whatsapp_messages w
            WHERE w.account = o.scope)""",
        tolerance=HOUR,
        # The client runs in three-hour windows with a relaunch between them.
        observation_interval=4 * HOUR,
    ),
    UpstreamCheck(
        pipeline="google_contacts",
        scope="account",
        evidence=(
            "connections.list(personFields=metadata), every page: the newest CONTACT-source"
            " updateTime, every run (hourly)"
        ),
        pdw_newest_sql="""(SELECT max(c.source_updated_at) FROM @contact_cards c
            WHERE c.source = 'google_people' AND c.account = o.scope)""",
        tolerance=3 * HOUR,
        observation_interval=4 * HOUR,
    ),
    UpstreamCheck(
        pipeline="comma",
        scope="account/dongle_id",
        evidence="routes_segments over the retained window: the newest route's start, every poll (15 min)",
        pdw_newest_sql="""(SELECT max(r.start_at) FROM @comma_routes r
            WHERE r.account = split_part(o.scope, '/', 1)
              AND r.dongle_id = substr(o.scope, length(split_part(o.scope, '/', 1)) + 2))""",
        tolerance=HOUR,
        observation_interval=HOUR,
    ),
    UpstreamCheck(
        pipeline="whoop",
        scope="account",
        evidence="GET /v2/cycle?limit=1 with no window: the newest cycle's start, every run (5 min)",
        pdw_newest_sql="""(SELECT max(c.start_at) FROM @whoop_cycles c WHERE c.account = o.scope)""",
        tolerance=HOUR,
        observation_interval=HOUR,
    ),
    UpstreamCheck(
        pipeline="whoop_private",
        scope="account",
        evidence="cycles/details over the last three days: the newest cycle's start, every run (15 min)",
        pdw_newest_sql="""(SELECT max(c.start_at) FROM @whoop_private_cycles c WHERE c.account = o.scope)""",
        tolerance=2 * HOUR,
        observation_interval=2 * HOUR,
    ),
    UpstreamCheck(
        pipeline="github",
        scope="account/credential",
        evidence="/users/<login>/events page 1 (newest first): the newest event, every run (15 min)",
        pdw_newest_sql="""(SELECT max(e.created_at) FROM @github_events e
            WHERE e.account = split_part(o.scope, '/', 1))""",
        tolerance=HOUR,
        observation_interval=HOUR,
    ),
    UpstreamCheck(
        pipeline="chatgpt",
        scope="account",
        evidence=(
            "/backend-api/conversations?order=updated: the first conversation's update_time,"
            " every poll (5 min)"
        ),
        # The poller stores each conversation's listed update_time only after
        # its events are written, so this is what has landed.
        pdw_newest_sql="""(SELECT to_timestamp(max(c.update_time)) FROM @chatgpt_conversation_sync c
            WHERE c.account = o.scope)""",
        # A rate-limited poll stops and resumes on the next tick.
        tolerance=2 * HOUR,
        observation_interval=HOUR,
    ),
    UpstreamCheck(
        pipeline="claude_desktop",
        scope="account",
        evidence=(
            "claude.ai chat_conversations (newest-updated first): the first conversation's"
            " updated_at, every poll (5 min)"
        ),
        # The conversation header row (seq 0) carries the updated_at of the
        # version that landed; one indexed lookup by the newest conversation.
        pdw_newest_sql="""(SELECT max(NULLIF(e.raw_json::jsonb ->> 'updated_at', '')::timestamptz)
            FROM @claude_desktop_events e
            WHERE e.session_id = o.upstream_newest_id AND e.seq = 0
              AND e.event_type = 'conversation' AND e.account = o.scope)""",
        # Uploaded to the Drive inbox, then the agent-sessions reader.
        tolerance=2 * HOUR,
        observation_interval=HOUR,
    ),
    UpstreamCheck(
        pipeline="alice_voice_recordings",
        scope="account",
        evidence="/api/v2/recordings, whole listing: the newest created_at, every daily poll",
        pdw_newest_sql="""(SELECT max(r.recorded_at) FROM @alice_voice_recordings r
            WHERE r.account = o.scope)""",
        # A daily poll, then the Drive ingest.
        tolerance=48 * HOUR,
        observation_interval=36 * HOUR,
    ),
    _agent_session_check("claude_code"),
    _agent_session_check("codex"),
    _agent_session_check("openclaw"),
    # pi and argon run on Macs that can sleep; the observation still comes
    # from every uploader run, idle tool or not.
    _agent_session_check("pi", observation_interval=6 * HOUR),
    _agent_session_check("muse_code", observation_interval=6 * HOUR),
    _agent_session_check("argon", observation_interval=6 * HOUR),
    _agent_session_check("muse", observation_interval=6 * HOUR),
    UpstreamCheck(
        pipeline="apple_contacts",
        scope="account",
        evidence="the Mac's AddressBook: the newest card modification, every uploader run",
        pdw_newest_sql="""(SELECT max(c.source_updated_at) FROM @apple_contact_cards c
            WHERE c.source = 'apple_contacts' AND c.account = o.scope)""",
        tolerance=HOUR,
        observation_interval=2 * HOUR,
    ),
    UpstreamCheck(
        pipeline="apple_notes",
        scope="account",
        evidence=(
            "the Mac's NoteStore: the newest note modification, every uploader run (what"
            " iCloud delivered to the Mac; iCloud's own delivery is beyond any device)"
        ),
        pdw_newest_sql="""(SELECT max(n.modified_at) FROM @apple_notes n WHERE n.account = o.scope)""",
        tolerance=HOUR,
        observation_interval=2 * HOUR,
    ),
    UpstreamCheck(
        pipeline="browser_history",
        scope="account/device",
        evidence="the Mac's History databases: the newest visit across profiles, every uploader run",
        pdw_newest_sql="""(SELECT max(v.visited_at) FROM @browser_history_visits v
            WHERE v.account = split_part(o.scope, '/', 1)
              AND v.device = substr(o.scope, length(split_part(o.scope, '/', 1)) + 2))""",
        tolerance=HOUR,
        observation_interval=2 * HOUR,
    ),
    UpstreamCheck(
        pipeline="android_photos",
        scope="account",
        evidence=(
            "MediaStore on the phone: the newest gallery file by date_added and its relative"
            " path, every uploader run on Wi-Fi"
        ),
        # The exact file by its native id: present means it landed, and then the
        # observation's own time stands in for PDW's.
        pdw_newest_sql="""(SELECT o.upstream_newest_at FROM @android_photos_files f
            WHERE f.source = 'android_photos' AND f.account = o.scope
              AND f.source_native_id = o.upstream_newest_id LIMIT 1)""",
        # Uploads are Wi-Fi only and MediaStore is not read off Wi-Fi, so a day
        # away from Wi-Fi is not a fault.
        tolerance=24 * HOUR,
        observation_interval=3 * 24 * HOUR,
    ),
    UpstreamCheck(
        pipeline="android_messages",
        scope="account/device",
        evidence="bugle_db on the phone: the newest received_timestamp, every uploader run",
        pdw_newest_sql="""(SELECT max(m.message_at) FROM @android_messages m
            WHERE m.account = split_part(o.scope, '/', 1)
              AND m.device = substr(o.scope, length(split_part(o.scope, '/', 1)) + 2))""",
        tolerance=HOUR,
        observation_interval=2 * HOUR,
    ),
    UpstreamCheck(
        pipeline="android_screen_time",
        scope="account/device",
        evidence="UsageStatsManager on the phone: the newest usage event, every uploader run",
        pdw_newest_sql="""(SELECT max(e.event_at) FROM @android_screen_time_events e
            WHERE e.device = substr(o.scope, length(split_part(o.scope, '/', 1)) + 2)
              AND e.account = split_part(o.scope, '/', 1))""",
        tolerance=HOUR,
        observation_interval=2 * HOUR,
    ),
    UpstreamCheck(
        pipeline="apple_messages",
        scope="account",
        evidence=(
            "the Mac uploader's own chat.db: the newest message date in the snapshot it just"
            " read, posted to /ingest/source-observation every run (5 min)"
        ),
        pdw_newest_sql="""(SELECT max(a.message_at) FROM @apple_messages a
            WHERE a.account = o.scope AND a.is_deleted = 0)""",
        # Upload, Drive inbox, then the Dagster reader.
        tolerance=HOUR,
        observation_interval=2 * HOUR,
    ),
)

#: The base tables the view reads; it is created only once all of them exist.
SOURCE_UPSTREAM_INPUT_TABLES: tuple[str, ...] = (
    "source_upstream_observations",
    "gmail_messages",
    "calendar_events",
    "google_drive_files",
    "whatsapp_messages",
    "apple_messages",
    "contact_cards",
    "comma_routes",
    "whoop_cycles",
    "whoop_private_cycles",
    "github_events",
    "chatgpt_conversation_sync",
    "claude_desktop_events",
    "alice_voice_recordings",
    "claude_code_events",
    "codex_events",
    "openclaw_events",
    "pi_events",
    "muse_code_events",
    "argon_events",
    "muse_events",
    "apple_contact_cards",
    "apple_notes",
    "browser_history_visits",
    "android_photos_files",
    "android_messages",
    "android_screen_time_events",
)


def upstream_check(pipeline: str) -> UpstreamCheck:
    for check in SOURCE_UPSTREAM_CHECKS:
        if check.pipeline == pipeline:
            return check
    raise KeyError(pipeline)


def _seconds(value: timedelta) -> int:
    return int(value.total_seconds())


def landed_slack_seconds_sql(pipeline_column: str) -> str:
    """Each check's landed slack, in seconds, for the pipeline in ``pipeline_column``."""
    arms = " ".join(
        f"WHEN '{check.pipeline}' THEN {_seconds(check.landed_slack)}"
        for check in SOURCE_UPSTREAM_CHECKS
        if check.landed_slack != timedelta(seconds=LANDED_SLACK_SECONDS)
    )
    if not arms:
        return str(LANDED_SLACK_SECONDS)
    return f"(CASE {pipeline_column} {arms} ELSE {LANDED_SLACK_SECONDS} END)"


def source_upstream_health_view_sql() -> str:
    """The live detector: one row per observation, judged against base_*."""
    epoch = "'1970-01-01 00:00:00+00'::timestamptz"
    branches = []
    for check in SOURCE_UPSTREAM_CHECKS:
        branches.append(
            f"""
            SELECT o.pipeline, o.scope, o.upstream_newest_at, o.upstream_newest_id,
                   o.detail, o.observed_at, o.pending_since,
                   {check.pdw_newest_sql} AS pdw_newest_at,
                   {_seconds(check.tolerance)}::bigint AS tolerance_seconds,
                   {_seconds(check.observation_interval)}::bigint AS expected_observation_seconds,
                   {_seconds(check.landed_slack)}::bigint AS landed_slack_seconds
            FROM @source_upstream_observations o
            WHERE o.pipeline = '{check.pipeline}'"""
        )
    union = "\n            UNION ALL".join(branches)
    return f"""
        CREATE OR REPLACE VIEW @marts_ops_source_upstream_health AS
        WITH observed AS ({union}
        ),
        judged AS (
            -- Absence is the epoch in ops (AGENTS.md); translated here, once,
            -- for every exposed timestamp.
            SELECT
                pipeline, scope,
                NULLIF(upstream_newest_at, {epoch}) AS upstream_newest_at,
                NULLIF(upstream_newest_id, '') AS upstream_newest_id,
                NULLIF(pdw_newest_at, {epoch}) AS pdw_newest_at,
                NULLIF(observed_at, {epoch}) AS observed_at,
                NULLIF(pending_since, {epoch}) AS carried_pending_since,
                NULLIF(detail, '') AS detail,
                tolerance_seconds, expected_observation_seconds, landed_slack_seconds
            FROM observed
        ),
        measured AS (
            SELECT
                judged.*,
                -- Landed: PDW's newest is at least the source's newest. A
                -- source with nothing at all has nothing to land.
                (upstream_newest_at IS NULL
                 OR COALESCE(pdw_newest_at >= upstream_newest_at
                        - make_interval(secs => landed_slack_seconds), false)) AS landed
            FROM judged
        ),
        pending AS (
            -- How long has the OLDEST unlanded item been missing? The newest
            -- item's own age cannot say: on a busy source every look names a
            -- fresher one, so a stall of days read ok. The collector carries
            -- the first item PDW did not catch up with (pending_since) until
            -- PDW passes it; until it has, the stall is measured from there.
            SELECT
                measured.*,
                CASE
                    WHEN landed THEN NULL
                    WHEN carried_pending_since IS NOT NULL
                     AND (pdw_newest_at IS NULL
                          OR pdw_newest_at < carried_pending_since
                                - make_interval(secs => landed_slack_seconds))
                        THEN LEAST(carried_pending_since, upstream_newest_at)
                    ELSE upstream_newest_at
                END AS pending_since
            FROM measured
        )
        SELECT
            pipeline,
            scope,
            CASE
                WHEN observed_at IS NULL
                  OR now() - observed_at > make_interval(secs => expected_observation_seconds)
                    THEN 'unknown'
                WHEN landed THEN 'ok'
                WHEN now() - pending_since > make_interval(
                        secs => tolerance_seconds * {FAILING_MULTIPLIER}) THEN 'failing'
                WHEN now() - pending_since > make_interval(secs => tolerance_seconds)
                    THEN 'late'
                ELSE 'ok'
            END AS status,
            landed::int AS landed,
            upstream_newest_at,
            upstream_newest_id,
            pdw_newest_at,
            pending_since,
            -- How long the oldest unlanded item has been missing; NULL once
            -- PDW holds the source's newest.
            (EXTRACT(EPOCH FROM now() - pending_since))::bigint AS missing_seconds,
            tolerance_seconds,
            observed_at,
            (EXTRACT(EPOCH FROM now() - observed_at))::bigint AS observation_age_seconds,
            expected_observation_seconds,
            detail
        FROM pending
    """


def record_upstream_observation(
    warehouse,
    logger,
    *,
    pipeline: str,
    scope: str,
    upstream_newest_at: datetime | None,
    upstream_newest_id: str = "",
    detail: str = "",
) -> None:
    """Record what the source says is its newest item; never break the sync.

    The observation is evidence for a detector, so a failure to store it is a
    warning: the sync's own data must still land, and a missing observation
    already reads ``unknown`` once it ages past the source's interval.
    """
    try:
        warehouse.record_source_upstream_observation(
            pipeline=pipeline,
            scope=scope,
            upstream_newest_at=upstream_newest_at,
            upstream_newest_id=upstream_newest_id,
            detail=detail,
        )
    except Exception as exc:  # noqa: BLE001 - evidence must not stop the sync
        logger.warning("Could not record %s upstream observation for %s: %s", pipeline, scope, exc)
