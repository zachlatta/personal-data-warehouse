"""The Feed tab: cards agents push, and what Zach does with them on the phone.

The Go app owns every write (app/internal/feed): agents push items through
`pdw feed push` / the feed_* tools and the iOS app posts engagement events to
/api/feed/events. This module is the one place the tables and the marts views
are declared, so a fresh warehouse has them before the app's first request.

Removing the feature: docs/feed.md lists every file and registration line.
"""

TABLES = ("feed_items", "feed_events")
VIEWS = ("marts_feed_item_engagement", "marts_feed_source_engagement")

#: Event types the phone records. The Go validator holds the same list
#: (app/internal/feed/feed.go EventTypes); tests/test_feed.py pins the two.
EVENT_TYPES = (
    "session_start", "session_end", "impression", "open", "close", "expand",
    "dismiss", "undismiss",
)

#: The deliberate acts that reach timeline.events (tier self). Impressions,
#: dwell and sessions are telemetry and stay in base_feed.events.
TIMELINE_EVENT_TYPES = ("open", "dismiss")

#: An impression shorter than this is a scroll-past rather than a look.
SCROLL_PAST_MS = 1000

#: Total time on screen (at least half visible) that counts a card as engaged
#: even when he never tapped it: he read the card itself.
ENGAGED_DWELL_MS = 5000

#: engagement_score, the one number agents rank his interest by. There is no
#: like button: interest is measured. Each term is capped so one long sitting
#: on a card cannot drown everything else; a dismissal is a strong negative.
#: opens 2 each (at most 3 counted), expands 1, a minute looking at the card
#: 6, ten minutes reading 10, dismissed -5.
ENGAGEMENT_SCORE_SQL = (
    "round((2 * LEAST(COALESCE(e.opens, 0), 3) + LEAST(COALESCE(e.expands, 0), 1)"
    " + LEAST(COALESCE(e.dwell_ms, 0), 60000) / 10000.0"
    " + LEAST(COALESCE(e.read_ms, 0), 600000) / 60000.0"
    " - CASE WHEN e.dismiss_state = 'dismiss' THEN 5 ELSE 0 END)::numeric, 2)::float8"
)


def ensure_feed_tables(warehouse):
    for statement in STATEMENTS:
        warehouse._command(statement)
    # The views changed shape when likes were replaced by measured
    # engagement; _ensure_view recreates them when CREATE OR REPLACE cannot.
    warehouse._ensure_view(
        "marts_feed_item_engagement", ITEM_ENGAGEMENT_VIEW, dependents=("marts_feed_source_engagement",)
    )
    warehouse._ensure_view("marts_feed_source_engagement", SOURCE_ENGAGEMENT_VIEW)
    for logical in (*TABLES, *VIEWS):
        warehouse._apply_catalog_grant(logical)


STATEMENTS = (
    """CREATE TABLE IF NOT EXISTS @feed_items (
        item_id text PRIMARY KEY,
        source text NOT NULL,
        item_key text NOT NULL,
        title text NOT NULL DEFAULT '',
        body text NOT NULL DEFAULT '',
        url text NOT NULL DEFAULT '',
        image_url text NOT NULL DEFAULT '',
        author text NOT NULL DEFAULT '',
        site text NOT NULL DEFAULT '',
        tags text[] NOT NULL DEFAULT '{}',
        score double precision NOT NULL DEFAULT 0,
        published_at timestamptz NOT NULL DEFAULT 'epoch',
        expires_at timestamptz NOT NULL DEFAULT 'epoch',
        metadata jsonb NOT NULL DEFAULT '{}',
        pushed_by text NOT NULL DEFAULT '',
        created_at timestamptz NOT NULL DEFAULT now(),
        updated_at timestamptz NOT NULL DEFAULT now(),
        removed_at timestamptz NOT NULL DEFAULT 'epoch',
        removed_reason text NOT NULL DEFAULT '',
        links jsonb NOT NULL DEFAULT '[]',
        UNIQUE (source, item_key)
    )""",
    # More links on one card: [{"url", "title", "site"}], shown under the body.
    "ALTER TABLE @feed_items ADD COLUMN IF NOT EXISTS links jsonb NOT NULL DEFAULT '[]'",
    "CREATE INDEX IF NOT EXISTS feed_items_updated_idx ON @feed_items (updated_at)",
    "CREATE INDEX IF NOT EXISTS feed_items_live_idx ON @feed_items (score DESC, created_at DESC) "
    "WHERE removed_at = 'epoch'",
    """CREATE TABLE IF NOT EXISTS @feed_events (
        event_id text PRIMARY KEY,
        event_type text NOT NULL,
        item_id text NOT NULL DEFAULT '',
        session_id text NOT NULL DEFAULT '',
        occurred_at timestamptz NOT NULL,
        received_at timestamptz NOT NULL DEFAULT now(),
        dwell_ms bigint NOT NULL DEFAULT 0,
        visible_fraction double precision NOT NULL DEFAULT 0,
        position bigint NOT NULL DEFAULT -1,
        client_name text NOT NULL DEFAULT '',
        device text NOT NULL DEFAULT '',
        metadata jsonb NOT NULL DEFAULT '{}'
    )""",
    "CREATE INDEX IF NOT EXISTS feed_events_received_idx ON @feed_events (received_at)",
    "CREATE INDEX IF NOT EXISTS feed_events_occurred_idx ON @feed_events (occurred_at DESC)",
    "CREATE INDEX IF NOT EXISTS feed_events_item_idx ON @feed_events (item_id, event_type, occurred_at)",
)

# Booleans are bigint 0/1 and the epoch becomes NULL, as everywhere in
# marts_*. dismissed is the item's CURRENT state: the latest of dismiss and
# undismiss. engaged: he opened it (any link), tapped More, or looked at the
# card for ENGAGED_DWELL_MS. link_opens counts opens per link url.
ITEM_ENGAGEMENT_VIEW = f"""CREATE OR REPLACE VIEW @marts_feed_item_engagement AS
    WITH e AS (
        SELECT item_id,
            count(*) FILTER (WHERE event_type = 'impression') AS impressions,
            COALESCE(sum(dwell_ms) FILTER (WHERE event_type = 'impression'), 0) AS dwell_ms,
            COALESCE(max(visible_fraction) FILTER (WHERE event_type = 'impression'), 0) AS max_visible_fraction,
            count(*) FILTER (WHERE event_type = 'impression' AND dwell_ms < {SCROLL_PAST_MS}) AS scroll_pasts,
            count(*) FILTER (WHERE event_type = 'open') AS opens,
            COALESCE(sum(dwell_ms) FILTER (WHERE event_type = 'close'), 0) AS read_ms,
            count(*) FILTER (WHERE event_type = 'expand') AS expands,
            (array_agg(event_type ORDER BY occurred_at DESC, received_at DESC)
                FILTER (WHERE event_type IN ('dismiss', 'undismiss')))[1] AS dismiss_state,
            (array_agg(metadata->>'reason' ORDER BY occurred_at DESC, received_at DESC)
                FILTER (WHERE event_type = 'dismiss'))[1] AS dismiss_reason,
            count(DISTINCT metadata->>'url') FILTER (WHERE event_type = 'open' AND metadata->>'url' <> '')
                AS links_opened,
            min(occurred_at) FILTER (WHERE event_type = 'impression') AS first_seen_at,
            max(occurred_at) AS last_event_at
        FROM @feed_events
        WHERE item_id <> ''
        GROUP BY item_id
    )
    SELECT i.item_id, i.source, i.title, i.url, i.site, i.author, i.tags, i.score,
           NULLIF(i.published_at, 'epoch'::timestamptz) AS published_at,
           NULLIF(i.created_at, 'epoch'::timestamptz) AS created_at,
           NULLIF(i.updated_at, 'epoch'::timestamptz) AS updated_at,
           NULLIF(i.expires_at, 'epoch'::timestamptz) AS expires_at,
           NULLIF(i.removed_at, 'epoch'::timestamptz) AS removed_at,
           jsonb_array_length(i.links) AS link_count,
           COALESCE(e.impressions, 0) AS impressions,
           COALESCE(e.dwell_ms, 0) AS dwell_ms,
           COALESCE(e.max_visible_fraction, 0) AS max_visible_fraction,
           COALESCE(e.scroll_pasts, 0) AS scroll_pasts,
           COALESCE(e.opens, 0) AS opens,
           COALESCE(e.links_opened, 0) AS links_opened,
           COALESCE(e.read_ms, 0) AS read_ms,
           COALESCE(e.expands, 0) AS expands,
           CASE WHEN e.dismiss_state = 'dismiss' THEN 1 ELSE 0 END AS dismissed,
           CASE WHEN e.dismiss_state = 'dismiss' THEN COALESCE(e.dismiss_reason, '') ELSE '' END
               AS dismiss_reason,
           CASE WHEN COALESCE(e.opens, 0) > 0 OR COALESCE(e.expands, 0) > 0
                     OR COALESCE(e.dwell_ms, 0) >= {ENGAGED_DWELL_MS}
                THEN 1 ELSE 0 END AS engaged,
           CASE WHEN e.item_id IS NULL THEN 0::float8 ELSE {ENGAGEMENT_SCORE_SQL} END AS engagement_score,
           e.first_seen_at,
           e.last_event_at
    FROM @feed_items i
    LEFT JOIN e ON e.item_id = i.item_id"""

SOURCE_ENGAGEMENT_VIEW = """CREATE OR REPLACE VIEW @marts_feed_source_engagement AS
    SELECT source,
           count(*) AS items,
           count(*) FILTER (WHERE removed_at IS NULL) AS live_items,
           count(*) FILTER (WHERE impressions > 0) AS items_seen,
           sum(impressions) AS impressions,
           sum(dwell_ms) AS dwell_ms,
           count(*) FILTER (WHERE opens > 0) AS items_opened,
           sum(read_ms) AS read_ms,
           count(*) FILTER (WHERE engaged = 1) AS items_engaged,
           count(*) FILTER (WHERE dismissed = 1) AS items_dismissed,
           round((count(*) FILTER (WHERE opens > 0))::numeric
                 / NULLIF(count(*) FILTER (WHERE impressions > 0), 0), 3) AS open_rate,
           round((count(*) FILTER (WHERE engaged = 1))::numeric
                 / NULLIF(count(*) FILTER (WHERE impressions > 0), 0), 3) AS engaged_rate,
           round((count(*) FILTER (WHERE dismissed = 1))::numeric
                 / NULLIF(count(*) FILTER (WHERE impressions > 0), 0), 3) AS dismiss_rate,
           round((avg(engagement_score) FILTER (WHERE impressions > 0))::numeric, 2) AS avg_engagement_score,
           max(created_at) AS last_pushed_at,
           max(last_event_at) AS last_event_at
    FROM @marts_feed_item_engagement
    GROUP BY source"""
