"""Screenshots versus `self`: does what Zach does on screen reach his own tier?

C2 says `self` is "Zach did it". The screen timelapse is the one independent
witness of what he did: every active frame carries an agent's reading of the
screen (app, summary). This module compares the two in both directions, per
timeline adapter, over a trailing window:

* **Follow-through (recall side).** Frames in a messaging app whose reading says
  Zach is composing, replying, sending or posting. A frame that shows the act
  should be followed by a `self` event from the matching adapter within
  ``MATCH_AFTER`` (or shortly before it, ``MATCH_BEFORE``, since a frame that
  shows a just-sent message lands after the send). Drafts are counted apart:
  a draft on screen is not a send, and one that was abandoned has no event to
  find, so draft follow-through is a lower bound on recall, not recall.
* **Screen coverage (precision side).** `self` events of the same adapters,
  and whether any active frame on any device was captured within
  ``COVERAGE_WINDOW`` of them. A `self` send with no screen in use around it is
  what an automated send misfiled as Zach's looks like.

The frame selection reads the describing agent's own words; it is a measured
baseline, not a judgment of its own: it only routes a frame to the adapter its
app writes to and splits "sent" from "draft" wording. Results are written per
run to ``ops.screen_self_audit`` (one row per channel per run) and published (counts only, no screen text) by
``marts_ops.screen_self_audit``.
"""

from __future__ import annotations

from datetime import datetime, timedelta

#: Trailing window each run measures. Frames from the last ``SETTLE`` are left
#: for the next run: their sends may not have synced yet.
WINDOW_DAYS = 7
SETTLE_HOURS = 2
MATCH_BEFORE_MINUTES = 10
MATCH_AFTER_MINUTES = 30
COVERAGE_WINDOW_MINUTES = 6
#: A run is written at most this often; the collector calls in every ten minutes.
RUN_INTERVAL_HOURS = 24

#: marts_ops.screen_self_audit: how much history it shows, when a run is too
#: old to judge, and the thresholds of its verdict. A channel whose on-screen
#: acts reach no self event at all, or whose self events mostly had no screen
#: in use, is failing; a weaker follow-through or coverage needs attention.
#: Set from the first prod measurement (2026-11-26); its numbers stay in the
#: warehouse, not here.
SCREEN_SELF_AUDIT_HISTORY_DAYS = 30
SCREEN_SELF_AUDIT_STALE_SECONDS = 2 * RUN_INTERVAL_HOURS * 3600 + 3600
SCREEN_SELF_AUDIT_MIN_FRAMES = 3
SCREEN_SELF_AUDIT_MIN_EVENTS = 10
SCREEN_SELF_AUDIT_MIN_FOLLOW_THROUGH = 0.5
SCREEN_SELF_AUDIT_MIN_COVERAGE = 0.8
SCREEN_SELF_AUDIT_FAILING_COVERAGE = 0.5

#: A channel is where an on-screen act lands: the apps it is done in (matched
#: case-insensitively and exactly, as the describing agent names them) and the
#: timeline adapters a send from those apps reaches. A phone message sent from
#: Google Messages may be an SMS/RCS (android_message) or an iMessage relayed
#: through the Mac (apple_message), so either one is a match.
CHANNELS: tuple[tuple[str, tuple[str, ...], tuple[str, ...]], ...] = (
    ("email", ("Superhuman", "Gmail"), ("gmail_email",)),
    ("slack", ("Slack",), ("slack_message",)),
    (
        "phone_messages",
        ("Google Messages", "Messages", "BlueBubbles"),
        ("android_message", "apple_message"),
    ),
    ("whatsapp", ("WhatsApp",), ("whatsapp_message",)),
)

AUDITED_ADAPTERS: tuple[str, ...] = tuple(
    sorted({adapter for _, _, adapters in CHANNELS for adapter in adapters})
)

# The reading names Zach as the actor of a messaging verb. Draft wording wins:
# "Zach is composing a reply" is a draft even though it says "reply".
_ACTS = (
    r"\mzach(.s)? (is |was |has |had |just )*(now )?"
    r"(compos|draft|typ|writ|repl|respond|send|sent|post|messag)"
    r"|\mhis (draft|reply|message|response)\M"
)
_DRAFT = r"\m(compos|draft|typing|unsent|unfinished|writing)"


def _values(rows: list[tuple[str, str]]) -> str:
    return ", ".join(f"('{a}', '{b}')" for a, b in rows)


def audit_sql() -> str:
    """One row per channel for the window ``[%(window_start)s, %(window_end)s)``."""
    apps = [(app.lower(), channel) for channel, names, _ in CHANNELS for app in names]
    adapters = [(adapter, channel) for channel, _, names in CHANNELS for adapter in names]
    return (
        f"""
        WITH channel_apps(app, channel) AS (VALUES {_values(apps)}),
        channel_adapters(adapter, channel) AS (VALUES {_values(adapters)}),
        frames AS (
            SELECT DISTINCT s.device, s.captured_at, c.channel,
                   s.summary ~* '{_DRAFT}' AS is_draft
            FROM @marts_screen_timelapse_active_screens s
            JOIN channel_apps c ON c.app = lower(s.app)
            WHERE s.captured_at >= %(window_start)s
              AND s.captured_at < %(window_end)s
              AND s.summary ~* '{_ACTS}'
        ),
        frame_outcomes AS (
            SELECT f.channel, f.is_draft,
                   EXISTS (
                       SELECT 1 FROM @timeline_events t
                       JOIN channel_adapters ca ON ca.adapter = t.adapter AND ca.channel = f.channel
                       WHERE t.priority = 'self'
                         AND t.occurred_at >= f.captured_at - interval '{MATCH_BEFORE_MINUTES} minutes'
                         AND t.occurred_at <= f.captured_at + interval '{MATCH_AFTER_MINUTES} minutes'
                   ) AS matched
            FROM frames f
        ),
        self_events AS (
            SELECT ca.channel,
                   EXISTS (
                       SELECT 1 FROM @marts_screen_timelapse_active_screens s
                       WHERE s.captured_at >= t.occurred_at - interval '{COVERAGE_WINDOW_MINUTES} minutes'
                         AND s.captured_at <= t.occurred_at + interval '{COVERAGE_WINDOW_MINUTES} minutes'
                   ) AS screen_in_use
            FROM @timeline_events t
            JOIN channel_adapters ca ON ca.adapter = t.adapter
            WHERE t.priority = 'self'
              AND t.adapter IN (SELECT adapter FROM channel_adapters)
              AND t.occurred_at >= %(window_start)s
              AND t.occurred_at < %(window_end)s
        ),
        channels AS (SELECT DISTINCT channel FROM channel_adapters)
        SELECT c.channel,
               (SELECT string_agg(ca.adapter, ',' ORDER BY ca.adapter)
                  FROM channel_adapters ca WHERE ca.channel = c.channel) AS adapters,
               count(fo.*) FILTER (WHERE NOT fo.is_draft)::bigint AS sent_frames,
               count(fo.*) FILTER (WHERE NOT fo.is_draft AND fo.matched)::bigint AS sent_frames_matched,
               count(fo.*) FILTER (WHERE fo.is_draft)::bigint AS draft_frames,
               count(fo.*) FILTER (WHERE fo.is_draft AND fo.matched)::bigint AS draft_frames_matched,
               (SELECT count(*) FROM self_events se WHERE se.channel = c.channel)::bigint AS self_events,
               (SELECT count(*) FROM self_events se
                 WHERE se.channel = c.channel AND se.screen_in_use)::bigint AS self_events_with_screen
        FROM channels c
        LEFT JOIN frame_outcomes fo ON fo.channel = c.channel
        GROUP BY c.channel
        ORDER BY c.channel
        """
    )


def audit_params(window_end: datetime) -> dict:
    """The settled window ending ``SETTLE_HOURS`` before ``window_end``."""
    end = window_end - timedelta(hours=SETTLE_HOURS)
    return {"window_start": end - timedelta(days=WINDOW_DAYS), "window_end": end}


def run_screen_self_audit(warehouse, *, now: datetime, force: bool = False) -> list[dict] | None:
    """Measure and record one run, at most once per ``RUN_INTERVAL_HOURS``.

    Returns the rows written, or None when the last run is recent enough.
    """
    if not force:
        last = warehouse._query_dicts("SELECT max(measured_at) AS last FROM @screen_self_audit")
        latest = last[0]["last"] if last else None
        if latest is not None and now - latest < timedelta(hours=RUN_INTERVAL_HOURS):
            return None
    params = audit_params(now)
    rows = warehouse._query_dicts(audit_sql(), params)
    written = [
        {
            **row,
            "measured_at": now,
            "window_start_at": params["window_start"],
            "window_end_at": params["window_end"],
            "collected_at": now,
        }
        for row in rows
    ]
    warehouse.write_screen_self_audit(written)
    return written
