# Feed: cards on Zach's phone, and what he does with them

The PDW iOS app has a **Feed** tab: full-width cards Zach scrolls like a social feed, with no
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
- A card needs a title, a body or a url. `body` is markdown (the card shows the first
  lines, the reader all of it); `image_url` must be https; `site` defaults to the url's host.
- **More links per card:** `links` (at most 10, each `url` + optional `title`) show as rows
  under the body: the paper, the repo, the discussion. `url` stays the card's main tap.
- `score` ranks (higher first; {{if .CLI}}`pdw feed rank ID...`{{else}}`feed_rank`{{end}} sets many at once). The phone
  shows unseen cards first, then seen ones he engaged with, then ones he scrolled past, and
  never shows one source
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

## Reading engagement

There is no like button: interest is measured. A card is `engaged` when he opened it (any
link), tapped More, shared it, or kept it on screen for 5 s; `engagement_score` adds opens
(2 each, up to 3), More (1), a share (3), time on the card (up to 6 for a minute) and reading time (up to 10 for ten
minutes), and takes 5 off a dismissal. Rank his interests by it.

{{if .CLI}}`pdw feed stats [--source S] [--since 7d]`{{else}}`feed_stats` `{"source": "...", "since": "7d"}`{{end}} — per source: cards seen and engaged, open,
engaged and dismiss rates, average engagement_score, dwell and read time; event counts; the
most engaged cards; and cards seen twice or more with no engagement. Read it before writing
the next batch, and write less of what he skips. {{if .CLI}}`pdw feed list` (`--view feed` is exactly what the phone shows) and `pdw feed events`{{else}}`feed_list` (`view: "feed"` is exactly what the phone shows) and `feed_events`{{end}} give per card and raw detail.

In SQL: `marts_feed.item_engagement` (one row per card: `link_count`, `impressions`,
`dwell_ms`, `scroll_pasts` (impressions under 1 s), `opens`, `links_opened`, `read_ms`,
`expands`, `dismissed`, `dismiss_reason`, `engaged`, `engagement_score`, `shares`),
`marts_feed.source_engagement`, and the raw log `base_feed.events` (`event_type`,
`dwell_ms`, `visible_fraction`, `position`, `session_id`, `metadata`; an open's
`metadata.url` and `link_index` say which link). An impression is one stretch of a card at
least half on screen, with how long and how much of it was visible. A `share` is a swipe that
opened a Superhuman draft to send the card to someone. Opens, shares and dismissals
are also timeline events (source `feed`, tier `self`); impressions and sessions are not.

Zach's curated picks are source `picks`, refreshed daily by the "Feed picks daily" Paseo
schedule (desk `work/2026-10-20-feed-curation/`); push there only through that job.
