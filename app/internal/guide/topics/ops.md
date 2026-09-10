# Ops: is the data current?

A green pipeline is not the same as the right data, so health is inspectable at several
levels, each answering one question. All are plain views; the same facts render on the
app's `/pipelines` page.

| level | relation | answers |
| --- | --- | --- |
| pipelines | `marts_ops.pipeline_health` | is this feed still delivering: `status` (`ok`/`late`/`stale`/`failing`/`attention`/`manual`/`no_data`/`unknown`), `last_write_at`, `newest_event_at`, `last_run_at`, `last_error` |
| tables | `marts_ops.table_freshness` | the quiet table inside a healthy pipeline: `last_write_at`, `data_age_seconds` |
| marts | `marts_ops.mart_view_health` | is a read view built on anything current: `stalest_pipeline`, `stalest_pipeline_at` |
| timeline adapters | `marts_ops.timeline_adapter_health` | is THIS kind of data reaching `timeline.events`, and how far behind its source it is (`ingest_lag_seconds`) |
| priority mix | `marts_ops.timeline_priority_mix` | how each source's last seven days split across the tiers; an `unclassified` row is `failing` |
| notifications | `marts_ops.notification_health` | is the direct/cc experiment paused, running, backlogged or failing |
| agent usage | `marts_ops.agent_usage` | are agents starting at the timeline and scoping by tier, per agent source |
| search | `marts_ops.search_health`, `marts_ops.search_benchmark`, `marts_ops.search_benchmark_history` | chunk/embedding convergence, BM25 index integrity, cache residency; weekly latency and MRR |
| per-source SLAs | `marts_ops.slack_conversation_health`, `marts_ops.plaid_item_health` | Slack per conversation type; each Plaid institution (a `duplicate` or dead Item is a named row) |
| integrity and backups | `marts_ops.collation_health`, `marts_ops.pgbackrest_health` | did the sort order move under an index; is the database backed up and has a restore been drilled |

{{if .CLI}}Run these with `pdw sql --output json -q '<why>' '<SQL>'`.{{else}}Run these through `query`.{{end}}

```sql
SELECT pipeline, status, last_write_at, last_error
FROM marts_ops.pipeline_health WHERE status NOT IN ('ok','manual') ORDER BY status;

SELECT view_schema, view_name, status, stalest_pipeline, stalest_pipeline_at
FROM marts_ops.mart_view_health WHERE status <> 'ok' ORDER BY status;
```

## Reading a status honestly

- Status is computed at **read** time against each pipeline's own expected intervals
  (how often it runs, how often data legitimately arrives, how far behind the newest
  event may fall). A snapshot older than the collector's own window reads `unknown`
  rather than presenting stale facts as current.
- `unmeasured` and `unmonitored` are gaps in the measurement, not evidence about the
  data; they never colour a pipeline red.
- `action_required` is a credential or consent a person must repair (a dead WHOOP
  refresh token, an expired ChatGPT or Claude Desktop session, a Plaid Item needing
  re-consent). It stays until a real success clears it, so an unchanged row is an active
  incident.
- Some sources are legitimately quiet for weeks (voice memos, contacts); their run
  heartbeat is the detector, and a `late` data age alone is not an outage.
- The `ops.*` sync-state tables behind these views are mostly not readable by the query
  role (42501); the views are the interface.

Remote-device uploaders (the Apple sources, agent sessions) report a heartbeat per run,
so a LaunchAgent that fires and fails reads `failing` rather than merely `late`; the
`uploader_heartbeats` row says whether any device is reporting at all.

## Notification experiment

`marts_ops.notifications` records newly landed direct/cc events captured while enabled;
`marts_ops.notification_deliveries` records each device's attempts and observed opens.
Join notification `id` to delivery `notification_id`. Use `/notifications` or phone
Settings to start/pause; read-only SQL cannot change the switch. Existing history and
ordinary edits never replay. Device tokens and open capabilities are private.

**Acceptance is not display, and a tap is not a read.** `accepted_at` means push-service
acceptance; an Expo `provider_accepted` verdict means downstream handoff, not an OS
screen observation. `opened_at` is when the server first observed a tap upload;
offline uploads arrive later. Missing opens do not establish that a person ignored
anything. All absent timestamps in these marts are NULL.

Measure source sync delay separately from landing-to-push delay; a fast push worker
cannot compensate for a source polling slowly. Calendar event time is a scheduled
meeting time, not creation time. Unsupported native group-message links fall back to
the exact PDW event. iOS retains PDW's header identity and uses source artwork as a
thumbnail. See repository `docs/notifications.md` for rollout, limits and cohort SQL.
