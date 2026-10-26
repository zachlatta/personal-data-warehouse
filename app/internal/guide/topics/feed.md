# Feed: cards on Zach's phone, and what he does with them

The PDW phone app has a **Feed** tab: full-width cards Zach scrolls like a social feed, with no
unread count. Agents fill it; the phone records everything he does with each card. It is
for things worth his attention that are not messages to him: links, stories, briefings,
finds. Do not put a reply someone is waiting on here (that is {{if .CLI}}`pdw call notify`{{else}}`notify`{{end}} or a mutation).

## Pushing cards

{{if .CLI}}```bash
pdw feed push --source hn --title "Show HN: ..." --url https://news.ycombinator.com/item?id=1 \
  --body "Why it is worth a look, in markdown" --image https://example.com/a.jpg --tag ai \
  --link "Paper|https://arxiv.org/abs/..." --link "HN thread|https://news.ycombinator.com/item?id=2"
jq -c '{source: "muse-picks", items: .}' picks.json | pdw feed push   # a batch
```

Stdin also takes a JSON array of cards, one card object, or JSON Lines.{{else}}`feed_push` `{"source": "muse-picks", "items": [{"title": "...", "url": "https://...", "body": "markdown", "image_url": "https://...", "tags": ["ai"], "links": [{"url": "https://...", "title": "Paper"}]}]}`{{end}}

- **`source`** names the agent or feed (lowercase). Every card is idempotent on
  (source, key) and the key defaults to the url, so re-pushing a link updates its card;
  a re-push replaces the content, keeps the score unless one is given, and writes nothing
  when nothing changed.
- **Before pushing a link, ask whether it is already a card**:
  {{if .CLI}}`pdw feed list --all --url URL` (repeatable){{else}}`feed_list` with `urls` plus
  `include_removed` and `include_dismissed`{{end}} matches a card's url, key and links, removed
  or dismissed ones too. Paging the whole list for it misses everything past the limit.
- A card needs a title, a body or a url. `body` is markdown (the card shows the first
  lines, the reader all of it); `image_url` must be https; `site` defaults to the url's host.
- **X posts are embedded, not linked.** Push an `x.com/<user>/status/<id>` url (no title
  needed) and the server reads the post (fxtwitter) and stores it in `embed`: the card shows
  the author, text, photos, video and quoted post the way X does, and a tap opens the whole
  thread. A `title` on such a card shows above it as the reason it is there ("Karpathy on
  ..."). Re-push to refresh its counts. A post that cannot be read stays a plain link card.
- **Photos and video:** `media` (at most 10, each `{type: image|video|gif, url,
  thumbnail_url}`, https) shows inset as a swipeable carousel; a lone video, or a card whose
  url is a YouTube or Vimeo video, plays inside the card (push the video's own url);
  {{if .CLI}}`--media URL` (repeatable; `.mp4` is a video){{else}}a lone `image_url` is a one-photo carousel{{end}}. Cards with a picture get looked at
  longer: give one whenever the source has a real image (an og:image, a figure, a photo).
- **More links per card:** `links` (at most 10, each `url` + optional `title`) show as rows
  under the body: the paper, the repo, the discussion. A tap on a card opens its detail view
  (whole post or summary, every photo, all links); `url` is what "Open" there and the
  card's compass button go to.
- `score` ranks (higher first; {{if .CLI}}`pdw feed rank ID...`{{else}}`feed_rank`{{end}} sets many at once). The phone
  shows cards he has not seen first: a card leaves that part at its first impression (half of
  it on screen), so every hourly batch must be new cards. The feed never shows empty: after
  the unseen cards come seen ones he passed over (no open, share, save or 5 s on screen),
  marked `resurfaced` and "Seen before"; keep unseen cards stocked so he rarely reaches them. It never shows one source
  three times in a row while another waits. `expires_at` takes RFC3339 or `48h` / `7d`.
- Say who you are: {{if .CLI}}`--agent NAME` (default `$PDW_AGENT_NAME`, else the Paseo agent id) and `--reason`{{else}}`agent` and `reason`{{end}} are kept on every version you write.

## Managing cards: every change is a version

Every create, edit, rank, removal, restore and deletion writes an immutable row to
`base_feed.item_versions` (version, action, client, agent, reason, changed fields with
from/to, full snapshot). Nothing is ever lost, so edit and remove freely.

{{if .CLI}}| You want | Run |
| --- | --- |
| One card in full, or as it was | `pdw feed get ID [--version N]` |
| Edit (any field; empty some) | `pdw feed edit ID... --title T --clear image_url,tags` |
| Edit every live card of a source | `pdw feed edit --source S --expires 2d` |
| Remove (soft) | `pdw feed remove ID...` or `--source S` |
| Bring back, optionally an older version | `pdw feed restore ID [--version N]` or `--source S` |
| What changed, and who did it | `pdw feed history [ID] [--source S] [--full]` |{{else}}| You want | Call |
| --- | --- |
| One card in full, or as it was | `feed_get` `{"item_id", "version"}` |
| Edit (any field; empty some) | `feed_update` `{"item_id" or "item_ids" or "source", fields..., "clear": [...]}` |
| Remove (soft) | `feed_remove` `{"item_ids"}` or `{"source"}` |
| Bring back, optionally an older version | `feed_restore` `{"item_id", "version"}` or `{"source"}` |
| What changed, and who did it | `feed_history` `{"item_id", "source", "snapshots"}` |{{end}}

A dismissal by Zach is engagement, not a version: it hides the card from his feed for good,
whatever an agent does to it.

## Saved for later: his read-later queue

Zach bookmarks (or swipes right on) cards he must read or watch later; they collect in the
**Saved** bubble at the front of the phone's source rail, newest first, and stay there after
they expire or are removed until he unsaves them. Read the queue with {{if .CLI}}`pdw feed list --view saved`{{else}}`feed_list` `{"view": "saved"}`{{end}}
(`saved`, `saved_at` on each card's engagement). Save for him only when he asks ("save that
for me"): {{if .CLI}}`pdw feed save ID... --reason "..."` (`pdw feed unsave` takes it out){{else}}`feed_save` `{"item_ids": [...], "reason": "..."}` (`"unsave": true` takes it out){{end}}.
Push the card first if it is not in the feed.
The phone downloads every saved card and the next 40 in the feed (pictures and article
text, not videos) so they read offline; saving a card is how to make sure it is there.

## Reading engagement

Interest is mostly measured; the heart is the one thing he says outright (`liked`,
`liked_at`; the latest of `like` and `unlike`). A card is `engaged` when he opened it (any
link), opened its detail view, tapped More, swiped its photos or played its video, shared it,
saved it, liked it, or kept it on screen for 5 s; `engagement_score` adds opens
(2 each, up to 3; a detail view is an open), More or a photo swipe or video play (1), a share (3), saved for later (3), liked (5), time on the card (up to 6 for a minute) and reading time (up to 10 for ten
minutes), and takes 5 off a dismissal (a swipe left or the `...` menu). Rank his interests by it.

{{if .CLI}}`pdw feed stats [--source S] [--since 7d]`{{else}}`feed_stats` `{"source": "...", "since": "7d"}`{{end}} — per source: cards seen and engaged, open,
engaged and dismiss rates, average engagement_score, dwell and read time; event counts; the
most engaged cards; and cards seen twice or more with no engagement. Read it before writing
the next batch, and write less of what he skips. {{if .CLI}}`pdw feed list` (`--view feed` is exactly what the phone shows) and `pdw feed events`{{else}}`feed_list` (`view: "feed"` is exactly what the phone shows) and `feed_events`{{end}} give per card and raw detail.

In SQL: `marts_feed.item_engagement` (one row per card: `link_count`, `impressions`,
`dwell_ms`, `scroll_pasts` (impressions under 1 s), `opens`, `links_opened`, `read_ms`,
`expands`, `dismissed`, `dismiss_reason`, `engaged`, `engagement_score`, `shares`, `saved`, `saved_at`, `liked`, `liked_at`),
`marts_feed.source_engagement`, and the raw log `base_feed.events` (`event_type`,
`dwell_ms`, `visible_fraction`, `position`, `session_id`, `metadata`; an open's
`metadata.url` and `link_index` say which link; `metadata.mode` is `detail`, `browser`
or `video`; a `media` event is a carousel swipe or video play with `metadata.index`). An impression is one stretch of a card at
least half on screen, with how long and how much of it was visible. All times (`dwell_ms`, `read_ms`, watch time) are active time since 2026-10-24: the app in front and touched in the last minute, or a video or link open; time in another app, a locked phone or an untouched screen does not count (`metadata.wall_ms` keeps the clock time). Before that they were clock time, and a detail view's time also counted the links and videos opened from it, so a long read before 2026-10-24 may be the phone left open. The marts count only Zach: events from a build under test (the Android emulator, an iOS simulator) stay in `base_feed.events` and are left out of `item_engagement` and `sessions`, so an agent checking the app never marks a card seen for him. A `share` is the paper plane that
opened a mail draft to send the card to someone. Opens, shares, saves, likes and dismissals
are also timeline events (source `feed`, tier `self`); impressions and sessions are not.

How he uses the feed itself (Zach asked on 2026-10-20 to log everything and query it later):
`marts_feed.sessions` is one row per sitting (`started_at`, `duration_ms`, `trigger`
launch / foreground / focus, `end_reason`, `cards_seen`, `deepest_position`, `scrolls`,
`scroll_distance`, `opens`, `taps`, `loads`). In `base_feed.events`, `session_start`
carries app version, OTA update, OS and time zone; `load` is what the server served, in
order (`metadata.item_ids`) and how fast; `scroll` is one gesture (`from_y`, `to_y`,
`direction`, `release_velocity`, `from_card`, `to_card`, `metadata.in` = `detail` inside
a card); `ui` is any other tap, named by `metadata.action` (`filter`, `refresh`,
`menu_open`, `menu_pick`, `undo`, `swipe_start`, `scroll_to_top`, `end_reached`,
`share_failed`). Impressions carry `metadata.view` (`all`, `saved`, `source:<s>`) and the
card's `height`.

Zach's curated picks are source `picks`, refreshed daily by the "Feed picks daily" Paseo
schedule (desk `work/2026-10-20-feed-curation/`); push there only through that job.
