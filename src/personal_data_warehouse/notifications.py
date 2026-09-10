"""Transactional timeline notification outbox. The Go app owns delivery and opens.

Capture inserts, not seq changes: enrichment and priority re-walks must never page
someone again. Installation is inert until the authenticated experiment switch is on.
"""

TABLES = ("notification_state", "notification_events", "notification_deliveries", "web_push_devices")


def ensure_notification_tables(warehouse):
    for statement in STATEMENTS:
        warehouse._command(statement)
    for logical in (*TABLES, "capture_timeline_notification", "marts_notifications", "marts_notification_deliveries", "marts_notification_health"):
        warehouse._apply_catalog_grant(logical)
    # Do not take a table-wide DDL lock on the large timeline at every sync.
    if not warehouse._query("SELECT 1 FROM pg_trigger WHERE tgrelid = %s::regclass "
                            "AND tgname = 'timeline_notification_insert'", (warehouse.sql_relation("timeline_events"),)):
        warehouse._command("""
            DO $$ BEGIN
                PERFORM set_config('lock_timeout', '3s', true);
                PERFORM pg_advisory_xact_lock(hashtext(%s || ':notification-trigger'));
                IF NOT EXISTS (SELECT 1 FROM pg_trigger
                               WHERE tgrelid = %s::regclass
                                 AND tgname = 'timeline_notification_insert') THEN
                    CREATE TRIGGER timeline_notification_insert AFTER INSERT ON @timeline_events
                    FOR EACH ROW WHEN (NEW.priority IN ('direct','cc'))
                    EXECUTE FUNCTION @capture_timeline_notification();
                END IF;
            END $$
        """, (warehouse.sql_relation("timeline_events"), warehouse.sql_relation("timeline_events")))


STATEMENTS = (
    """CREATE TABLE IF NOT EXISTS @notification_state (
        id text PRIMARY KEY, enabled bigint NOT NULL DEFAULT 0,
        updated_at timestamptz NOT NULL DEFAULT now(),
        last_run_at timestamptz NOT NULL DEFAULT 'epoch',
        status text NOT NULL DEFAULT 'paused', error text NOT NULL DEFAULT ''
    )""",
    "INSERT INTO @notification_state (id) VALUES ('timeline') ON CONFLICT DO NOTHING",
    """CREATE TABLE IF NOT EXISTS @notification_events (
        id text PRIMARY KEY DEFAULT gen_random_uuid()::text,
        adapter text NOT NULL, event_id text NOT NULL, source text NOT NULL,
        priority text NOT NULL CHECK (priority IN ('direct','cc')),
        event_ts timestamptz NOT NULL, landed_at timestamptz NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now(),
        payload jsonb NOT NULL, status text NOT NULL DEFAULT 'pending',
        UNIQUE (adapter, event_id)
    )""",
    "CREATE INDEX IF NOT EXISTS notification_events_pending_idx ON @notification_events (created_at, id) WHERE status = 'pending'",
    "CREATE INDEX IF NOT EXISTS notification_events_created_idx ON @notification_events (created_at)",
    """CREATE TABLE IF NOT EXISTS @notification_deliveries (
        id text PRIMARY KEY DEFAULT gen_random_uuid()::text,
        notification_id text NOT NULL, device_id text NOT NULL,
        transport text NOT NULL CHECK (transport IN ('expo','web')),
        endpoint jsonb NOT NULL, payload jsonb NOT NULL DEFAULT '{}',
        status text NOT NULL DEFAULT 'pending', attempt_count bigint NOT NULL DEFAULT 0,
        created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
        next_attempt_at timestamptz NOT NULL DEFAULT now(),
        lease_id text NOT NULL DEFAULT '', lease_until timestamptz NOT NULL DEFAULT 'epoch',
        ticket_id text NOT NULL DEFAULT '', error text NOT NULL DEFAULT '',
        accepted_at timestamptz NOT NULL DEFAULT 'epoch',
        receipt_at timestamptz NOT NULL DEFAULT 'epoch',
        opened_at timestamptz NOT NULL DEFAULT 'epoch',
        UNIQUE (notification_id, device_id)
    )""",
    "CREATE INDEX IF NOT EXISTS notification_deliveries_due_idx ON @notification_deliveries (status, next_attempt_at)",
    "CREATE INDEX IF NOT EXISTS notification_deliveries_notification_idx ON @notification_deliveries (notification_id)",
    "CREATE INDEX IF NOT EXISTS notification_deliveries_updated_idx ON @notification_deliveries (updated_at)",
    """CREATE TABLE IF NOT EXISTS @web_push_devices (
        id text PRIMARY KEY, subscription jsonb NOT NULL,
        device_name text NOT NULL DEFAULT '', status text NOT NULL DEFAULT 'active',
        registered_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
        error text NOT NULL DEFAULT ''
    )""",
    "CREATE INDEX IF NOT EXISTS web_push_devices_updated_idx ON @web_push_devices (updated_at)",
    """CREATE OR REPLACE FUNCTION @capture_timeline_notification() RETURNS trigger
    LANGUAGE plpgsql AS $$ BEGIN
        IF (SELECT enabled = 1 FROM @notification_state WHERE id = 'timeline' FOR SHARE) THEN
            INSERT INTO @notification_events (adapter, event_id, source, priority, event_ts, landed_at, payload)
            VALUES (NEW.adapter, NEW.event_id, NEW.source, NEW.priority::text, NEW.event_ts, NEW.first_seen_at,
                jsonb_build_object('adapter',NEW.adapter,'event_id',NEW.event_id,'source',NEW.source,
                    'source_table',NEW.source_table,'source_pk',NEW.source_pk,
                    'actor',left(NEW.actor,200),'title',left(NEW.title,300),
                    'snippet',left(NEW.snippet,800),'context',left(NEW.context,200),
                    'metadata',jsonb_build_object('thread_ts',NEW.metadata->>'thread_ts',
                                                  'chat_id',NEW.metadata->>'chat_id')))
            ON CONFLICT (adapter, event_id) DO NOTHING;
        END IF;
        RETURN NEW;
    END $$""",
    """CREATE OR REPLACE VIEW @marts_notifications AS
        SELECT id, adapter, event_id, source, priority,
               NULLIF(event_ts,'epoch'::timestamptz) AS event_ts,
               NULLIF(landed_at,'epoch'::timestamptz) AS landed_at,
               NULLIF(created_at,'epoch'::timestamptz) AS created_at,
               payload->>'actor' AS actor, payload->>'title' AS title,
               payload->>'snippet' AS snippet, status
        FROM @notification_events""",
    """CREATE OR REPLACE VIEW @marts_notification_deliveries AS
        SELECT id, notification_id, device_id, transport, status, attempt_count,
               NULLIF(created_at,'epoch'::timestamptz) AS created_at,
               NULLIF(updated_at,'epoch'::timestamptz) AS updated_at, error,
               NULLIF(accepted_at,'epoch'::timestamptz) AS accepted_at,
               NULLIF(receipt_at,'epoch'::timestamptz) AS receipt_at,
               NULLIF(opened_at,'epoch'::timestamptz) AS opened_at
        FROM @notification_deliveries""",
    """CREATE OR REPLACE VIEW @marts_notification_health AS
        SELECT s.enabled, NULLIF(s.last_run_at,'epoch'::timestamptz) AS last_run_at,
               s.error, CASE WHEN s.enabled = 0 THEN 'paused'
                   WHEN s.status = 'error' THEN 'failing'
                   WHEN s.last_run_at < now() - interval '2 minutes' THEN 'stale'
                   WHEN EXISTS (SELECT 1 FROM @notification_deliveries
                                WHERE status IN ('failed','unknown') AND updated_at > now() - interval '1 day')
                        OR EXISTS (SELECT 1 FROM @notification_events WHERE status = 'pending'
                                   AND created_at < now() - interval '2 minutes')
                        OR EXISTS (SELECT 1 FROM @notification_events WHERE status = 'no_devices'
                                   AND created_at > now() - interval '1 day')
                        OR EXISTS (SELECT 1 FROM @notification_deliveries WHERE status IN ('pending','retry','sending')
                                   AND created_at < now() - interval '2 minutes') THEN 'attention'
                   ELSE 'ok' END AS status
        FROM @notification_state s WHERE id = 'timeline'""",
)
