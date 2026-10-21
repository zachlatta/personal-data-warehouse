# The Feed tab

A card feed in the PDW iOS app that agents fill through `pdw feed` (or the `feed_*` MCP
tools), with detailed engagement logging into the warehouse. Built 2026-10-20 from Zach's
request for a Muse-style feed he owns, measured the way a social feed measures its readers.
Agents: the how-to is `pdw readme feed` (`app/internal/guide/topics/feed.md`).

## How it works

```
agent ── pdw feed push / feed_push ──▶ app (app/internal/feed) ──▶ base_feed.items
agent ── pdw feed get / edit / remove / restore / history ──▶ the same tools
                         every change ──trigger──▶ base_feed.item_versions (append-only)
phone ── GET /api/feed ◀── ranked live cards (score, freshness, seen cards sink, sources interleaved)
phone ── POST /api/feed/events ──▶ base_feed.events ──▶ marts_feed.item_engagement / source_engagement
                                                  └──▶ timeline.events (open / dismiss, tier self)
```

- **Items** (`base_feed.items`) are idempotent on `(source, item_key)`; `item_id` is
  `source:key` and the key defaults to the url. Removal is soft (`removed_at`). `links`
  (jsonb, at most 10 `{url, title, site}`) are more links on one card, shown as rows under
  the body; `url` stays the main tap target and, if absent, is the first link.
- **Versions** (`base_feed.item_versions`, added 2026-10-20 at Zach's request that "if
  something is edited or deleted the past versions persist"): an `AFTER INSERT OR UPDATE OR
  DELETE` trigger on `base_feed.items` (`internal.feed_item_version`) writes one row per
  change: `version` (1, 2, ... per card), `action` (`create`, `update`, `rank`, `remove`,
  `restore`, `delete`, or `baseline` for cards that predate versioning), `actor` (the pdw
  client), `agent`, `reason`, `restored_from`, `changed_fields`, `diff`
  (`{field: {from, to}}`) and the full `snapshot`. Because the trigger, not the Go code,
  writes history, no path (a tool, a superuser's SQL) changes a card without a version, and
  a write that changes nothing visible writes none. The Go module names the actor, agent,
  reason and action through transaction-local settings (`pdw.feed_*`); plain SQL is
  classified from what changed. `internal.feed_item_versions_immutable` refuses UPDATE,
  DELETE and TRUNCATE on the versions table. A hard DELETE of a card leaves a `delete`
  tombstone whose snapshot is the card as it was, and `restore` recreates it from that.
- **Events** (`base_feed.events`) are written by the phone with device-generated ids, so a
  retried batch is counted once. Types: `session_start`, `session_end` (dwell = sitting
  length; metadata: cards seen, deepest position, acts), `impression` (one stretch of a card
  at least 50% on screen: `dwell_ms` at or above 50%, `visible_fraction` = peak, `position`,
  `metadata.visible_ms` = any part on screen), `open` (`metadata.mode` browser or reader;
  a browser open carries `metadata.url` and `link_index`, 0 = the main url),
  `close` (`dwell_ms` = time spent after the open; reader adds `scroll_depth`), `expand`
  (tapped "More"), `dismiss` (`metadata.reason`: `not_interested` or
  `less_like_this`) / `undismiss` (the 5-second Undo).
- **No likes; engagement is measured.** The like button was removed on 2026-10-20 at Zach's
  request ("we should just have it measure engagement"). `engaged` = opened (any link),
  tapped More, or 5 s on screen; `engagement_score` = 2 per open (up to 3) + 1 for More +
  time on the card (up to 6 for a minute) + reading time (up to 10 for ten minutes) - 5 if
  dismissed (`feed.py` `ENGAGEMENT_SCORE_SQL`). The phone ranks unseen cards first, then
  seen and engaged, then seen and scrolled past (`feed.go` `Rank`).
- **Analytics**: `marts_feed.item_engagement` (per card, current dismissed state, engaged,
  engagement_score, link_count, links_opened; `scroll_pasts` = impressions under 1 s) and
  `marts_feed.source_engagement` (per source rates, average score). `pdw feed stats` reads them.
- **C1/C2**: opens and dismissals are `feed_engagement` timeline events at tier
  `self` (Zach did them). Impressions, closes, expands and sessions are telemetry and stay in
  `base_feed.events`; cards are entities reached through the marts view.
- **C7**: pipeline `feed` with no freshness expectation (it is quiet when nobody pushes or
  reads); `marts_ops.pipeline_health` still shows its last write.

The phone keeps unsent events in `Documents/pdw-feed-outbox.json`, sends them every 15 s, at
40 queued, and when the tab or app goes to the background, and backs off up to 5 minutes
when offline. A batch the server refuses as malformed (400) is dropped rather than retried.

## Agent commands

`pdw feed push` (alias `add`), `get ID [--version N]`, `update` (alias `edit`; ids,
or `--source S` for every live card; `--clear` empties fields), `remove` (alias `delete`;
soft), `restore ID [--version N]` or `--source S`, `history [ID] [--source S] [--full]`,
`rank`, `list`, `stats`, `events`. Mutations take `--agent` (default `$PDW_AGENT_NAME`,
else `$PASEO_AGENT_ID`) and `--reason`. MCP: `feed_push`, `feed_get`, `feed_update`,
`feed_remove`, `feed_restore`, `feed_history`, `feed_rank`, `feed_list`, `feed_stats`,
`feed_events`. No scheduled agent writes the feed yet; Zach asked to hold off.

## Turning it off

- Server: set `PDW_FEED_ENABLED=0` on the app deployment. No routes, no tools; the phone's
  tab then says the feed is off.
- Phone: `FEED_ENABLED = false` in `mobile/src/features/feed/index.tsx` hides the tab
  (an OTA update).

## Removing it

Delete these files and directories:

- `app/internal/feed/` (store, HTTP API, tools, tests)
- `app/cmd/pdw-cli/feed.go`, `app/cmd/pdw-cli/feed_test.go`
- `app/internal/guide/topics/feed.md`
- `mobile/src/features/feed/`, `mobile/src/app/(tabs)/feed.tsx`, `mobile/src/app/feed/`
- `src/personal_data_warehouse/feed.py`, `tests/test_feed.py`, this file

Then remove one registration in each of these places:

| file | line |
| --- | --- |
| `app/internal/server/server.go` | `extra = append(extra, feed.Register(...)...)` and the `feed` import |
| `app/cmd/pdw-cli/run.go` | `case "feed"`, the `"feed": feedUsage` help entry, and the `feed` block in `usage` |
| `app/internal/guide/guide.go` | the `feed` topic; `brief.md` the two Feed tab rows |
| `mobile/src/app/(tabs)/_layout.tsx` | `<Tabs.Screen name="feed" ... />` and its import |
| `mobile/src/app/_layout.tsx` | `<Stack.Screen name="feed/[id]" ... />` |
| `mobile/package.json` | `src/features/*/*.test.mjs` in the test script (harmless if left) |
| `src/personal_data_warehouse/postgres.py` | `ensure_feed_tables(self)` in `ensure_timeline_tables`, the `feed` entry in `SEARCH_SOURCE_DEFS` |
| module `personal_data_warehouse.timeline` | `_FEED_ENGAGEMENT` and its entries in `TIMELINE_ADAPTERS`, `TIMELINE_CONTEXT_GENERIC_ADAPTERS`, `TIMELINE_TABLE_COVERAGE`, `RAW_DDL_TABLES` |
| `src/personal_data_warehouse/pipeline_health.py` | the `feed` pipeline and the three `TABLE_PIPELINES` rows |
| `src/personal_data_warehouse/pdw_cli_commands.py` | `"feed"` |
| `src/personal_data_warehouse/warehouse_catalog.json` | `base_feed`, `marts_feed`, the five `feed_*` / `marts_feed_*` relations and the two `feed_item_version*` functions; then run `uv run python scripts/generate_go_warehouse_catalog.py` |
| `tests/test_timeline.py` | `_seed_feed` and the `feed_engagement` expectations |
| `app/internal/query/overview_guidance_test.go` | the cap note for the feed relations (lower the cap back) |
| `tests/test_schema_reorg_contract.py` | the feed lines in the per-layer object counts (base, marts, internal) |

Finally drop the data (as superuser): `DROP SCHEMA marts_feed CASCADE; DROP SCHEMA
base_feed CASCADE; DROP FUNCTION internal.feed_item_version(),
internal.feed_item_versions_immutable(); DELETE FROM timeline.events WHERE adapter =
'feed_engagement';`. Dropping the schema is the one way past the append-only guard, which
is deliberate.
