# PDW schema reorganization — production cutover runbook

One-shot migration from the pre-reorganization layout (`gmail`, `slack`, `marts`,
`search`, `util`, …) to the cataloged one (`base_*`, `derived_*`, `marts_*`,
`timeline`, plus hidden `ops` / `private` / `internal`).

Relocation is a **catalog** operation — `ALTER TABLE … SET SCHEMA` / `RENAME` —
so the 70 GB Slack heap, the 50 GB timeline, and the 24 GB Gmail heap keep their
filenodes and are never rewritten. The expected lock window is seconds, not the
hours a copy would take. The migration takes `ACCESS EXCLUSIVE` on each moved
relation for the duration of its `ALTER`, so writers must be stopped.

## Preflight

1. Confirm the environment. From `crobat`/`porygon` (tailnet):

   ```bash
   set -a && source ~/dev/zachlatta/personal-data-warehouse/.env && set +a
   psql "$POSTGRES_DATABASE_URL" -c 'select current_database(), version()'
   ```

2. Resolve the Coolify application UUIDs (never filter logs by guessed names):

   ```bash
   set -a && source ~/dev/zachlatta/sysadmin/.env && set +a
   curl -fsS -H "Authorization: Bearer $COOLIFY_API_KEY" "$COOLIFY_URL/api/v1/applications" \
     | jq -r '.[] | "\(.uuid)\t\(.name)\t\(.fqdn // "-")"'
   ```

   As of the cutover these were `u16g67ivrj3fp5k2gnyp8m3q` (app) and
   `d2ejpxzod5shwz048yoszdru` (personal-data-warehouse-dagster) — re-resolve
   rather than trusting these, the UUIDs change when an app is recreated.

3. Confirm a recoverable backup exists and note the restore path. The migration
   drops the old schemas only after validation, but a restore point is the
   rollback of last resort for anything after that.

4. Record the preflight inventory (the upgrader does this itself, but capture it
   for the handoff):

   ```bash
   uv run python -m personal_data_warehouse.schema_upgrade            # preflight only
   uv run python -m personal_data_warehouse.schema_upgrade --print-plan
   ```

   Expected: `relocating 89 relations`, plus the count of timeline rows whose
   `source_table` is rewritten (`agent_session_events` → `ai_conversation_events`,
   ~20k rows).

## Stop writers

Everything that writes the warehouse must be down before the ALTERs:

- Dagster deployment (assets, sensors, the in-process WhatsApp client) — stop
  the Coolify application.
- The app (`/ingest/*` endpoints, mutation worker) — stop the Coolify
  application. Local uploader LaunchAgents fail loudly and retry on their next
  tick, so they need no action.

## Migrate

```bash
uv run python -m personal_data_warehouse.schema_upgrade --apply
```

It refuses to run twice, refuses if a target name is already taken, and
validates before it exits. On success it prints `filenodes preserved: 89` and
`migration complete and validated`.

## Deploy and restart

Push `main` (auto-deploys both the app and Dagster on rotom). Watch for the two
concurrent BuildKit builds colliding — if one fails, redeploy it alone. Bring the
app up first, then Dagster.

## Validation (must-pass)

```sql
-- 1. layout: 21 base_*, 7 derived_*, 8 marts_*, timeline, ops, private, internal
SELECT nspname FROM pg_namespace
WHERE nspname NOT LIKE 'pg_%' AND nspname NOT IN ('information_schema','public')
ORDER BY 1;

-- 2. no pre-reorg schema survives
SELECT nspname FROM pg_namespace WHERE nspname IN
  ('gmail','slack','marts','search','util','photos','finance','receipts',
   'enrichment','ai_processing','upstream_mutations','plaid','whoop',
   'google_calendar','google_contacts','google_drive','manual_finance',
   'apple_contacts','apple_messages','apple_notes','apple_photos',
   'apple_voice_memos','alice_voice_recordings','whatsapp','chatgpt',
   'claude_code','claude_desktop','codex','openclaw','pi');   -- expect 0 rows

-- 3. nothing shadowing in public
SELECT p.proname FROM pg_proc p WHERE p.pronamespace = 'public'::regnamespace
  AND p.proname LIKE 'search_text%';                          -- expect 0 rows

-- 4. row counts on the big heaps match the preflight numbers
SELECT 'slack', count(*) FROM base_slack.messages
UNION ALL SELECT 'gmail', count(*) FROM base_gmail.messages
UNION ALL SELECT 'timeline', count(*) FROM timeline.events;

-- 5. source_table tokens are all current catalog ids
SELECT source_table, count(*) FROM timeline.events GROUP BY 1 ORDER BY 2 DESC;

-- 6. query-role boundary
SELECT n.nspname, count(*) FILTER (WHERE has_table_privilege('pdw_query', c.oid,'SELECT')) readable,
       count(*) total
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE c.relkind IN ('r','v') AND n.nspname NOT LIKE 'pg_%'
  AND n.nspname NOT IN ('information_schema','public')
GROUP BY 1 ORDER BY 1;
-- every public schema readable == total; ops 8/20; private 0/5
```

Then, through the app:

```bash
pdw schema | head -20                     # starts with "START WITH timeline"
pdw schema | grep -c '^# ops '            # 0
pdw sql -q check "SELECT count(*) FROM timeline.events"
pdw sql -q search "SELECT source, ref FROM timeline.search_text('invoice', 5)"
pdw sql -q denied "SELECT 1 FROM private.chatgpt_sessions"   # must be denied
curl -fsS "$PDW_API_URL/healthz"
```

Timeline freshness (the sync engine must resume, not restart):

```sql
SELECT adapter, backfill_done, last_error, updated_at FROM ops.timeline_sync_state ORDER BY adapter;
SELECT max(event_ts) FROM timeline.events;
```

Production logs, pinned to the resolved UUID:

```bash
~/dev/zachlatta/sysadmin/scripts/coolify-and-server-loki-logs --format-logs --since 30m \
  '{job="coolify",server="rotom"} | json | container_name =~ "(?i).*<resource-uuid>.*"'
```

## Rollback decision point

Roll back if, after the migration and deploy:

- the inventory check finds a missing relation, or
- `filenodes preserved` is less than the preflight relocation count, or
- timeline/search returns empty for a query that worked before, or
- Dagster cannot start.

Before old schemas are dropped, rollback is the inverse `ALTER … SET SCHEMA`
(the upgrader logs every statement it ran). After they are dropped, rollback is
a restore from the backup taken in preflight. Do not improvise destructive
recovery.

## After a stable observation window

- Delete the `previous` blocks from `warehouse_catalog.json`,
  `PreviousLocations` from the Go generator, and
  `src/personal_data_warehouse/schema_upgrade.py`. They are explicitly temporary
  migration scaffolding; the only thing that keeps them useful is the
  old-name-in-an-error-message hint.
- Re-run `uv run python scripts/generate_go_warehouse_catalog.py`.
