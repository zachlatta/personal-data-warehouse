"""The Feed tab: cards agents push, and what Zach does with them on the phone.

The Go app owns every write (app/internal/feed): agents push items through
`pdw feed push` / the feed_* tools and the iOS app posts engagement events to
/api/feed/events. This module is the one place the tables and the marts views
are declared, so a fresh warehouse has them before the app's first request.

Removing the feature: docs/feed.md lists every file and registration line.
"""

TABLES = ("feed_items", "feed_events", "feed_item_versions")
FUNCTIONS = ("feed_item_version", "feed_item_versions_immutable")
VIEWS = ("marts_feed_item_engagement", "marts_feed_source_engagement", "marts_feed_sessions")

#: Event types the phone records. The Go validator holds the same list
#: (app/internal/feed/feed.go EventTypes); tests/test_feed.py pins the two.
EVENT_TYPES = (
    "session_start", "session_end", "impression", "open", "close", "expand",
    "dismiss", "undismiss", "share", "media", "save", "unsave",
    "ui", "scroll", "load",
)

#: The deliberate acts that reach timeline.events (tier self). Impressions,
#: dwell and sessions are telemetry and stay in base_feed.events.
TIMELINE_EVENT_TYPES = ("open", "dismiss", "share", "save")

#: An impression shorter than this is a scroll-past rather than a look.
SCROLL_PAST_MS = 1000

#: Total time on screen (at least half visible) that counts a card as engaged
#: even when he never tapped it: he read the card itself.
ENGAGED_DWELL_MS = 5000

#: engagement_score, the one number agents rank his interest by. There is no
#: like button: interest is measured. Each term is capped so one long sitting
#: on a card cannot drown everything else; a dismissal is a strong negative.
#: opens 2 each (at most 3 counted), expands 1, shared 3, saved for later 3,
#: a minute looking at the card 6, ten minutes reading 10, dismissed -5.
ENGAGEMENT_SCORE_SQL = (
    "round((2 * LEAST(COALESCE(e.opens, 0), 3) + LEAST(COALESCE(e.expands, 0), 1)"
    " + 3 * LEAST(COALESCE(e.shares, 0), 1)"
    " + CASE WHEN e.save_state = 'save' THEN 3 ELSE 0 END"
    " + LEAST(COALESCE(e.dwell_ms, 0), 60000) / 10000.0"
    " + LEAST(COALESCE(e.read_ms, 0), 600000) / 60000.0"
    " - CASE WHEN e.dismiss_state = 'dismiss' THEN 5 ELSE 0 END)::numeric, 2)::float8"
)

#: Version actions. The Go store names the action of each mutation through
#: the transaction-local `pdw.feed_action` setting; a write that does not
#: (plain SQL) is classified by the trigger from what changed.
VERSION_ACTIONS = ("baseline", "create", "update", "rank", "remove", "restore", "delete")


def ensure_feed_tables(warehouse):
    for statement in STATEMENTS:
        warehouse._command(statement)
    # The views changed shape when likes were replaced by measured
    # engagement; _ensure_view recreates them when CREATE OR REPLACE cannot.
    warehouse._ensure_view(
        "marts_feed_item_engagement", ITEM_ENGAGEMENT_VIEW, dependents=("marts_feed_source_engagement",)
    )
    warehouse._ensure_view("marts_feed_source_engagement", SOURCE_ENGAGEMENT_VIEW)
    warehouse._ensure_view("marts_feed_sessions", SESSIONS_VIEW)
    for logical in (*TABLES, *VIEWS, *FUNCTIONS):
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
        media jsonb NOT NULL DEFAULT '[]',
        embed jsonb NOT NULL DEFAULT '{}',
        UNIQUE (source, item_key)
    )""",
    # More links on one card: [{"url", "title", "site"}], shown under the body.
    "ALTER TABLE @feed_items ADD COLUMN IF NOT EXISTS links jsonb NOT NULL DEFAULT '[]'",
    # A swipeable photo/video carousel: [{"type", "url", "thumbnail_url", ...}].
    "ALTER TABLE @feed_items ADD COLUMN IF NOT EXISTS media jsonb NOT NULL DEFAULT '[]'",
    # A post shown inside the card (an X post: author, text, media, quote).
    "ALTER TABLE @feed_items ADD COLUMN IF NOT EXISTS embed jsonb NOT NULL DEFAULT '{}'",
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
    # Card history. Every insert, update and delete of a card writes one
    # immutable row here from a trigger, so no write path (the Go module, a
    # superuser's SQL) can change a card without leaving its past behind. A
    # delete is a tombstone whose snapshot is the card as it was; `pdw feed
    # restore` brings back any version.
    """CREATE TABLE IF NOT EXISTS @feed_item_versions (
        item_id text NOT NULL,
        version bigint NOT NULL,
        action text NOT NULL,
        actor text NOT NULL DEFAULT '',
        agent text NOT NULL DEFAULT '',
        reason text NOT NULL DEFAULT '',
        restored_from bigint NOT NULL DEFAULT 0,
        changed_fields text[] NOT NULL DEFAULT '{}',
        diff jsonb NOT NULL DEFAULT '{}',
        snapshot jsonb NOT NULL,
        changed_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (item_id, version)
    )""",
    "CREATE INDEX IF NOT EXISTS feed_item_versions_changed_idx ON @feed_item_versions (changed_at)",
    """CREATE OR REPLACE FUNCTION @feed_item_version() RETURNS trigger
    LANGUAGE plpgsql AS $$
    DECLARE
        old_row jsonb;
        new_row jsonb;
        diff jsonb := '{}'::jsonb;
        fields text[] := '{}';
        field text;
        act text;
        target text;
        next_version bigint;
    BEGIN
        IF TG_OP = 'DELETE' THEN target := OLD.item_id; ELSE target := NEW.item_id; END IF;
        IF TG_OP <> 'INSERT' THEN old_row := to_jsonb(OLD); END IF;
        IF TG_OP <> 'DELETE' THEN new_row := to_jsonb(NEW); END IF;
        IF TG_OP = 'UPDATE' THEN
            FOR field IN SELECT key FROM jsonb_each(new_row) WHERE key <> 'updated_at' LOOP
                IF new_row->field IS DISTINCT FROM old_row->field THEN
                    diff := diff || jsonb_build_object(field,
                        jsonb_build_object('from', old_row->field, 'to', new_row->field));
                    fields := fields || field;
                END IF;
            END LOOP;
            -- A write that changed nothing a reader can see is not a version.
            IF cardinality(fields) = 0 THEN RETURN NULL; END IF;
        END IF;
        act := NULLIF(current_setting('pdw.feed_action', true), '');
        IF act IS NULL THEN
            act := CASE
                WHEN TG_OP = 'INSERT' THEN 'create'
                WHEN TG_OP = 'DELETE' THEN 'delete'
                WHEN OLD.removed_at = 'epoch' AND NEW.removed_at <> 'epoch' THEN 'remove'
                WHEN OLD.removed_at <> 'epoch' AND NEW.removed_at = 'epoch' THEN 'restore'
                WHEN fields <@ ARRAY['score'] THEN 'rank'
                ELSE 'update'
            END;
        END IF;
        SELECT COALESCE(max(version), 0) + 1 INTO next_version
        FROM @feed_item_versions WHERE item_id = target;
        INSERT INTO @feed_item_versions (item_id, version, action, actor, agent, reason,
            restored_from, changed_fields, diff, snapshot, changed_at)
        VALUES (target, next_version, act,
            COALESCE(NULLIF(current_setting('pdw.feed_actor', true), ''), session_user),
            COALESCE(current_setting('pdw.feed_agent', true), ''),
            COALESCE(current_setting('pdw.feed_reason', true), ''),
            COALESCE(NULLIF(current_setting('pdw.feed_restored_from', true), '')::bigint, 0),
            fields, diff, COALESCE(new_row, old_row), clock_timestamp());
        RETURN NULL;
    END $$""",
    """CREATE OR REPLACE FUNCTION @feed_item_versions_immutable() RETURNS trigger
    LANGUAGE plpgsql AS $$ BEGIN
        RAISE EXCEPTION 'base_feed.item_versions is append-only: a card''s past versions are never changed or deleted';
    END $$""",
    """CREATE OR REPLACE TRIGGER feed_item_versions_append_only
        BEFORE UPDATE OR DELETE ON @feed_item_versions
        FOR EACH ROW EXECUTE FUNCTION @feed_item_versions_immutable()""",
    """CREATE OR REPLACE TRIGGER feed_item_versions_no_truncate
        BEFORE TRUNCATE ON @feed_item_versions
        FOR EACH STATEMENT EXECUTE FUNCTION @feed_item_versions_immutable()""",
    """CREATE OR REPLACE TRIGGER feed_items_versioned
        AFTER INSERT OR UPDATE OR DELETE ON @feed_items
        FOR EACH ROW EXECUTE FUNCTION @feed_item_version()""",
    # Cards that existed before versioning get their current state as v1.
    """INSERT INTO @feed_item_versions (item_id, version, action, actor, snapshot, changed_at)
        SELECT i.item_id, 1, 'baseline', 'versioning', to_jsonb(i), i.updated_at
        FROM @feed_items i
        WHERE NOT EXISTS (SELECT 1 FROM @feed_item_versions v WHERE v.item_id = i.item_id)""",
)

# Booleans are bigint 0/1 and the epoch becomes NULL, as everywhere in
# marts_*. dismissed is the item's CURRENT state: the latest of dismiss and
# undismiss. engaged: he opened it (any link), tapped More, shared it, or
# looked at the card for ENGAGED_DWELL_MS. saved is the CURRENT state of his
# read-later queue (the latest of save and unsave), saved_at when it last went
# in. shares, saved and saved_at are last because CREATE OR REPLACE VIEW can
# only add columns at the end. link_opens counts opens per link url.
ITEM_ENGAGEMENT_VIEW = f"""CREATE OR REPLACE VIEW @marts_feed_item_engagement AS
    WITH e AS (
        SELECT item_id,
            count(*) FILTER (WHERE event_type = 'impression') AS impressions,
            COALESCE(sum(dwell_ms) FILTER (WHERE event_type = 'impression'), 0) AS dwell_ms,
            COALESCE(max(visible_fraction) FILTER (WHERE event_type = 'impression'), 0) AS max_visible_fraction,
            count(*) FILTER (WHERE event_type = 'impression' AND dwell_ms < {SCROLL_PAST_MS}) AS scroll_pasts,
            count(*) FILTER (WHERE event_type = 'open') AS opens,
            COALESCE(sum(dwell_ms) FILTER (WHERE event_type = 'close'), 0) AS read_ms,
            -- Tapping More, swiping a photo carousel or playing a video.
            count(*) FILTER (WHERE event_type IN ('expand', 'media')) AS expands,
            count(*) FILTER (WHERE event_type = 'share') AS shares,
            (array_agg(event_type ORDER BY occurred_at DESC, received_at DESC)
                FILTER (WHERE event_type IN ('dismiss', 'undismiss')))[1] AS dismiss_state,
            (array_agg(metadata->>'reason' ORDER BY occurred_at DESC, received_at DESC)
                FILTER (WHERE event_type = 'dismiss'))[1] AS dismiss_reason,
            (array_agg(event_type ORDER BY occurred_at DESC, received_at DESC)
                FILTER (WHERE event_type IN ('save', 'unsave')))[1] AS save_state,
            max(occurred_at) FILTER (WHERE event_type = 'save') AS saved_at,
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
           CASE WHEN COALESCE(e.opens, 0) > 0 OR COALESCE(e.expands, 0) > 0 OR COALESCE(e.shares, 0) > 0
                     OR e.save_state = 'save' OR COALESCE(e.dwell_ms, 0) >= {ENGAGED_DWELL_MS}
                THEN 1 ELSE 0 END AS engaged,
           CASE WHEN e.item_id IS NULL THEN 0::float8 ELSE {ENGAGEMENT_SCORE_SQL} END AS engagement_score,
           e.first_seen_at,
           e.last_event_at,
           COALESCE(e.shares, 0) AS shares,
           CASE WHEN e.save_state = 'save' THEN 1 ELSE 0 END AS saved,
           CASE WHEN e.save_state = 'save' THEN e.saved_at END AS saved_at
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
           max(last_event_at) AS last_event_at,
           count(*) FILTER (WHERE saved = 1) AS items_saved
    FROM @marts_feed_item_engagement
    GROUP BY source"""

# One row per sitting: the Feed on screen with the app in front, from
# session_start to session_end. trigger is why it began (launch, foreground,
# focus; NULL before 2026-10-21), end_reason why it ended (background, blur).
# Every event the phone stamped with the sitting's id is counted, so this is
# how he uses the feed: how often and when he opens it, for how long, how
# far he scrolls, what he taps. Events in a card's detail view carry no
# sitting (the Feed is off screen then); they are on the card's row in
# item_engagement. An unfinished sitting has NULL ended_at and duration.
SESSIONS_VIEW = """CREATE OR REPLACE VIEW @marts_feed_sessions AS
    SELECT session_id,
           min(occurred_at) FILTER (WHERE event_type = 'session_start') AS started_at,
           max(occurred_at) FILTER (WHERE event_type = 'session_end') AS ended_at,
           max(dwell_ms) FILTER (WHERE event_type = 'session_end') AS duration_ms,
           (array_agg(metadata->>'trigger') FILTER (WHERE event_type = 'session_start'))[1] AS trigger,
           (array_agg(metadata->>'reason') FILTER (WHERE event_type = 'session_end'))[1] AS end_reason,
           max(device) AS device,
           (array_agg(metadata->>'app_version') FILTER (WHERE event_type = 'session_start'))[1] AS app_version,
           (array_agg(metadata->>'update_id') FILTER (WHERE event_type = 'session_start'))[1] AS update_id,
           count(DISTINCT item_id) FILTER (WHERE event_type = 'impression') AS cards_seen,
           count(*) FILTER (WHERE event_type = 'impression') AS impressions,
           COALESCE(sum(dwell_ms) FILTER (WHERE event_type = 'impression'), 0) AS card_dwell_ms,
           max(position) FILTER (WHERE event_type = 'impression') AS deepest_position,
           count(*) FILTER (WHERE event_type = 'scroll') AS scrolls,
           COALESCE(sum(abs((metadata->>'distance')::numeric)) FILTER (WHERE event_type = 'scroll'), 0)::bigint
               AS scroll_distance,
           count(*) FILTER (WHERE event_type = 'open') AS opens,
           count(*) FILTER (WHERE event_type IN ('expand', 'media')) AS expands,
           count(*) FILTER (WHERE event_type = 'share') AS shares,
           count(*) FILTER (WHERE event_type = 'save') AS saves,
           count(*) FILTER (WHERE event_type = 'dismiss') AS dismisses,
           count(*) FILTER (WHERE event_type = 'ui') AS taps,
           count(*) FILTER (WHERE event_type = 'load') AS loads,
           count(*) AS events
    FROM @feed_events
    WHERE session_id <> ''
    GROUP BY session_id"""
