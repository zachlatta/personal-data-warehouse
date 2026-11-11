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
)


def upstream_check(pipeline: str) -> UpstreamCheck:
    for check in SOURCE_UPSTREAM_CHECKS:
        if check.pipeline == pipeline:
            return check
    raise KeyError(pipeline)


def _seconds(value: timedelta) -> int:
    return int(value.total_seconds())


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
                   {_seconds(check.observation_interval)}::bigint AS expected_observation_seconds
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
                tolerance_seconds, expected_observation_seconds
            FROM observed
        ),
        measured AS (
            SELECT
                judged.*,
                -- Landed: PDW's newest is at least the source's newest. A
                -- source with nothing at all has nothing to land.
                (upstream_newest_at IS NULL
                 OR COALESCE(pdw_newest_at >= upstream_newest_at
                        - make_interval(secs => {LANDED_SLACK_SECONDS}), false)) AS landed
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
                                - make_interval(secs => {LANDED_SLACK_SECONDS}))
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
