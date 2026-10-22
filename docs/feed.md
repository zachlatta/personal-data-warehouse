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
phone ── GET /api/feed ◀── ranked unseen live cards (score, freshness, sources interleaved),
                           then resurfaced seen-but-skipped cards so it is never empty
phone ── POST /api/feed/events ──▶ base_feed.events ──▶ marts_feed.item_engagement / source_engagement / sessions
                                                  └──▶ timeline.events (open / dismiss, tier self)
```

- **Items** (`base_feed.items`) are idempotent on `(source, item_key)`; `item_id` is
  `source:key` and the key defaults to the url. Removal is soft (`removed_at`). `links`
  (jsonb, at most 10 `{url, title, site}`) are more links on one card, shown as rows under
  the body; `url` stays the main tap target and, if absent, is the first link.
- **Rich cards** (added 2026-10-20 from Zach's screen recordings of his PDW, Instagram and
  X feeds: "make my feed as maximally interesting and engaging as possible"): `media`
  (jsonb, at most 10 `{type: image|video|gif, url, thumbnail_url, width, height, alt}`) is a
  full-bleed swipeable carousel; `embed` (jsonb, `{}` = none) is a post shown natively in
  the card. The one embed type is `x_post` (author, avatar, verified, text, created_at,
  replies/reposts/likes/views, media, quote, the author's own thread). The push path fills
  it for any card whose url is an x.com/twitter.com status (`xpost.go`, from
  `api.fxtwitter.com`, 8 s timeout, 8 at a time); a post it cannot read stays a plain card
  and the push still succeeds. The phone draws three card shapes: X post, media
  (Instagram), link. A tap opens the detail view (whole thread or summary, every photo, all
  links); the compass opens the page; a video plays in the system player. A rail of source
  bubbles at the top (ringed while a source has unseen cards) narrows the feed to one
  source.
- **Card context** (added 2026-10-21 after reviewing the recordings): `GET /api/feed`
  sends the phone only `metadata.taste`, `hn_points` and `hn_comments`. A card shows
  `taste` as a small line above it (like X's "reposted" line or Instagram's "Suggested for
  you"); a Muse card shows "Muse Wonder · <kind>" from its leading `*kind*` line instead
  of Muse as the author, and its "**For you:**" text as the summary; a Hacker News card
  shows points and comments as a counts row (tap: the discussion) instead of the
  "80 points · 14 comments on Hacker News · discussion" line (`mobile/.../card-context.ts`).
  A reload that brings new cards while he is scrolled down shows X's "N new" pill; a tap
  on an X post's author opens their profile.
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
  `metadata.visible_ms` = any part on screen), `open` (`metadata.mode` detail, browser, or reader before 2026-10-20;
  a browser open carries `metadata.url` and `link_index`, 0 = the main url),
  `close` (`dwell_ms` = time spent after the open; the detail view adds `scroll_depth`), `expand`
  (tapped "More"), `dismiss` (`metadata.reason`: `not_interested` or
  `less_like_this`) / `undismiss` (the 5-second Undo), `share` (swiped left or tapped a share button to email
  the card from the default mail app; `metadata.via` = `mailto`, `superhuman` before 2026-10-21),
  `media` (`metadata.action`: `swipe` a carousel, `play` a video, `view` a photo opened full
  screen, `view_swipe` to another photo there, `share` one photo from it; `index` is the
  photo's place in the card's media; the detail view adds `metadata.in` = `detail`).
  A tap on a photo in an X post (feed or detail) or in the detail view's carousel opens
  the full-screen viewer: swipe between photos, pinch or double-tap to zoom, and the share
  button downloads that one photo (X photos at `name=orig`) to the iOS share sheet.
  A `close` with `metadata.mode` `video` is watch time.
  `save` / `unsave` put a card in or out of his read-later queue (`metadata.via`: `button`,
  `swipe`, `menu`, `detail`, `undo`, or `agent` when `feed_save` did it for him).
- **Everything else he does** (added 2026-10-21 from Zach's voice memo: "collect
  everything ... a surveillance state of me that you can query later to understand how I'm
  using it"). Three more event types, which need no card:
  - `load`: one fetch of the feed. `dwell_ms` = how long it took; `metadata.trigger`
    (`first`, `stale` = back after 10 minutes, `pull`), `ok`, `cards`, `saved`, and
    `item_ids`, the served order (cut at 12 KB, `item_ids_truncated`), so a query can
    tell what he was shown but never scrolled to.
  - `scroll`: one gesture, finger down to the list at rest (`scroll-gestures.ts`).
    `dwell_ms` = its length; `metadata`: `from_y`, `to_y`, `distance`, `direction`,
    `drag_ms`, `release_velocity` (points/ms), `momentum`, `from_card` / `to_card`
    (position at the top of the screen), `viewport`, `view`. In a card's detail view it
    has the card's `item_id` and `metadata.in` = `detail`, `content_height`.
  - `ui`: any other tap, named by `metadata.action`: `filter` (the source rail; `from`,
    `to`), `refresh` (pull to refresh), `menu_open` / `menu_pick` (`choice`, including
    `cancel`; `dwell_ms` = time the menu was up), `undo`, `swipe_start` (`toward` save or
    share, whether or not it went far enough), `scroll_to_top` (status bar tap), `tab_to_top` (Feed tapped in the tab bar while on it, from `from_y`),
    `end_reached` (scrolled to "That is everything"), `share_failed`, `new_pill_shown` /
    `new_pill` (X's "new posts" pill: a reload brought `count` cards while he was scrolled
    down; `new_pill` = he tapped it, from `from_y`). A tap on an X post's author or a Hacker
    News counts row is an `open` (`via` `post_link`) with the profile or discussion `metadata.url`.
  - `session_start` adds `metadata.trigger` (`launch` = first sitting since the app
    started, `foreground`, `focus` = back from another tab or a card's detail view),
    `gap_ms` since the last sitting, `since_app_start_ms`, `app_version`, `update_id`,
    `channel`, `os`, `color_scheme`, `tz`; `session_end` adds `reason` (`background`,
    `blur`). Impressions add `metadata.view` (`all`, `saved`, `source:<s>`), `height`
    and `ended_at`. `media` adds `zoom` and `view_close` (`dwell_ms` = time in the photo
    viewer). A link tapped in a card's summary is now an `open` (`via` `body_link`).
  - `marts_feed.sessions` rolls these up per sitting: `started_at`, `ended_at`,
    `duration_ms`, `trigger`, `end_reason`, `device`, `app_version`, `update_id`,
    `cards_seen`, `impressions`, `card_dwell_ms`, `deepest_position`, `scrolls`,
    `scroll_distance`, `opens`, `expands`, `shares`, `saves`, `dismisses`, `taps`, `loads`,
    `events`. A detail view takes the Feed off screen, so its events have no sitting.
- **Saved for later** (added 2026-10-21 from Zach's voice memo: "some kind of way to mark
  something as ... an important thing for me to read or consume, like add to my queue"):
  the bookmark on a card (the card's action row, an X post's metrics row, the detail
  view's header, the `...` menu) or a swipe right saves it, with Undo. The rail then leads
  with an orange **Saved** bubble (with a count) that shows the queue, newest saved first.
  The queue is the latest of `save` and `unsave` per card (`marts_feed.item_engagement`
  `saved`, `saved_at`). `GET /api/feed` returns it as `saved` beside `items`; a saved card
  stays in it after it expires or an agent removes it, and leaves only on unsave or a
  dismissal. Agents read it with `pdw feed list --view saved` and save for him only when he
  asks (`pdw feed save ID`, `feed_save`). A save counts as engaged and adds 3 to
  `engagement_score`, and is a `feed_engagement` timeline event. The phone keeps its own
  taps pending for 2 minutes (`saved-queue.ts`) so a reload that beats the event upload
  does not undo them on screen.
- **No likes; engagement is measured.** The like button was removed on 2026-10-20 at Zach's
  request ("we should just have it measure engagement"). `engaged` = opened (any link,
  or the detail view), tapped More, swiped photos or played a video (`media` events count
  with expands), shared, or 5 s on screen; `engagement_score` = 2 per open (up to 3) + 1 for More + 3 for a share +
  time on the card (up to 6 for a minute) + reading time (up to 10 for ten minutes) - 5 if
  dismissed, + 3 while saved for later (`feed.py` `ENGAGEMENT_SCORE_SQL`). The phone shows unseen cards first: a
  card leaves that part at its first impression (Zach, 2026-10-20: "after I've seen a post,
  don't show it to me again"; `store.go` `Feed`), and saved cards stay in Saved. The feed is
  never empty (Zach, 2026-10-21: "just have it always have more stuff"): when the unseen
  cards run out, the page continues with seen cards he passed over (not engaged, saved or
  dismissed), least seen and longest ago first, flagged `resurfaced` ("Seen before" on the
  card). The "all caught up" screen and the end-of-feed footer are gone.
- **Analytics**: `marts_feed.sessions` (per sitting, above), `marts_feed.item_engagement` (per card, current dismissed state, engaged,
  engagement_score, link_count, links_opened; `scroll_pasts` = impressions under 1 s) and
  `marts_feed.source_engagement` (per source rates, average score). `pdw feed stats` reads them.
- **C1/C2**: opens, shares, saves and dismissals are `feed_engagement` timeline events at tier
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
`rank`, `save` / `unsave`, `list` (`--view saved` is the queue), `stats`, `events`. Mutations take `--agent` (default `$PDW_AGENT_NAME`,
else `$PASEO_AGENT_ID`) and `--reason`. MCP: `feed_push`, `feed_get`, `feed_update`,
`feed_remove`, `feed_restore`, `feed_history`, `feed_rank`, `feed_list`, `feed_save`, `feed_stats`,
`feed_events`. No scheduled agent writes the feed yet; Zach asked to hold off.

## Turning it off

- Server: set `PDW_FEED_ENABLED=0` on the app deployment. No routes, no tools; the phone's
  tab then says the feed is off.
- Phone: `FEED_ENABLED = false` in `mobile/src/features/feed/index.tsx` hides the tab
  (an OTA update).

## Removing it

Delete these files and directories:

- `app/internal/feed/` (store, HTTP API, tools, rich cards and X embeds, tests)
- `app/cmd/pdw-cli/feed.go`, `app/cmd/pdw-cli/feed_test.go`
- `app/internal/guide/topics/feed.md`
- `mobile/src/features/feed/` (including `scroll-gestures.ts`), `mobile/src/app/(tabs)/feed.tsx`, `mobile/src/app/feed/`
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
| `src/personal_data_warehouse/warehouse_catalog.json` | `base_feed`, `marts_feed`, the six `feed_*` / `marts_feed_*` relations and the two `feed_item_version*` functions; then run `uv run python scripts/generate_go_warehouse_catalog.py` |
| `tests/test_timeline.py` | `_seed_feed` and the `feed_engagement` expectations |
| `app/internal/query/overview_guidance_test.go` | the cap note for the feed relations (lower the cap back) |
| `tests/test_schema_reorg_contract.py` | the feed lines in the per-layer object counts (base, marts, internal) |

Finally drop the data (as superuser): `DROP SCHEMA marts_feed CASCADE; DROP SCHEMA
base_feed CASCADE; DROP FUNCTION internal.feed_item_version(),
internal.feed_item_versions_immutable(); DELETE FROM timeline.events WHERE adapter =
'feed_engagement';`. Dropping the schema is the one way past the append-only guard, which
is deliberate.
