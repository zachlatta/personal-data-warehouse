# WhatsApp, Hacker News and shared attachment enrichment

Moved out of AGENTS.md on 2026-10-01 so the file every session loads holds only the
contracts and the rules every change needs. Start at [AGENTS.md](../../AGENTS.md).

## WhatsApp Client (linked device)

WhatsApp syncs through a real WhatsApp Web multidevice client (neonize, Python bindings over
whatsmeow), not a local-store scanner. The client registers as a linked device on Zach's
WhatsApp account, holds a persistent connection, and receives live messages plus history sync.

It runs in-process with the production Dagster deployment; no separate image or service:

- Asset/job: `whatsapp_client` / `whatsapp_client_job` (`src/personal_data_warehouse/defs/whatsapp_client.py`)
- The client runs in bounded windows (`WHATSAPP_CLIENT_RUN_SECONDS`, default 10800s) so it
  never trips Dagster run monitoring (`max_runtime_seconds: 14400`); the
  `whatsapp_client_keepalive_sensor` relaunches it whenever no run is active. WhatsApp queues
  messages for offline linked devices, so the seconds between windows lose nothing.
- A Postgres advisory lock prevents two concurrent connections on one session, which would
  corrupt the device state.
- Records land in the same Drive layout as Apple Messages, but the WhatsApp client writes them
  **directly** (it holds the Drive credential), not through the app's ingest endpoints: it builds
  the same `ObjectStore` the reader uses (`google_drive_spec(..., source="whatsapp")`) and writes
  JSONL.gz envelope batches to `whatsapp/inbox/batches/` and media blobs to `whatsapp/inbox/media/`
  itself (kind `whatsapp_export_batch` / `whatsapp_media_item`, deduped by content sha). Because
  the write skips Cloudflare, large media (videos) over the 100 MiB public-body cap upload fine.
  The `whatsapp_drive_inbox_sensor` + `whatsapp_drive_ingest` asset consume and promote them
  unchanged. The batch/media object keys + `pdw_*` tags live in
  `src/personal_data_warehouse_whatsapp/batcher.py`.
- Session state is canonical in Postgres table `whatsapp_client_sessions` as a bytea SQLite
  snapshot keyed by `WHATSAPP_ACCOUNT` + `WHATSAPP_SESSION_KEY` (default `default`). neonize
  still requires a SQLite filename at runtime, so `WHATSAPP_SESSION_PATH` is only a disposable
  cache path restored from Postgres before each run and snapshotted back after pairing,
  connect, contact dumps, flushes, and shutdown.

Enabling and pairing (first time):

1. The client is enabled by default once WhatsApp is configured; leave
   `WHATSAPP_CLIENT_ENABLED` unset or set it to `1`. Set it to `0` only when the client needs
   to be paused. `WHATSAPP_SESSION_PATH` may be left as the default runtime cache path; it does
   not need a persistent volume. Optionally set `WHATSAPP_PAIR_PHONE=<E.164 number without +>`
   to pair with an 8-character code instead of a QR.
2. Wait for the keepalive sensor to launch `whatsapp_client_job` (or launch it from the
   Dagster UI) and open the run logs.
3. Scan the QR printed in the logs (WhatsApp > Settings > Linked Devices > Link a Device), or
   enter the logged pairing code. After pairing, the client snapshots the session into Postgres
   and history sync chunks arrive automatically.

Other env vars: `WHATSAPP_ACCOUNT`, `WHATSAPP_GOOGLE_DRIVE_FOLDER_ID` (both fall back to the
Apple Messages values), `WHATSAPP_SESSION_KEY`, `WHATSAPP_CLIENT_ID` (normally leave unset),
`WHATSAPP_FLUSH_INTERVAL_SECONDS`, `WHATSAPP_MEDIA_BYTES_PER_FLUSH`,
`WHATSAPP_MEDIA_COUNT_PER_FLUSH`, `WHATSAPP_DOWNLOAD_HISTORY_MEDIA` (default **on**: all
attachments — live and history-sync — are downloaded to object storage. Set it to `0` as an
escape hatch if the history-media backfill causes load/ban-risk issues; WhatsApp often will not
serve very old media, so expect some `whatsapp_media_items.is_missing = true` rows for old
messages even with it on).

For local pairing/debugging there is a CLI: `uv run personal-data-warehouse-whatsapp-client`
(requires `brew install libmagic` on macOS). It requires `POSTGRES_DATABASE_URL` because
Postgres is the session source of truth. `--session-file` only selects the runtime cache file.

**A removed linked device is `action_required`, not `late`.** Between 2026-09-09 and
09-19 the client failed fifteen run windows with "pairing required" while `/pipelines`
read only `late`, because every window still re-stamped the session snapshot and the
session row carried no status for the health view to read. The client now records
`status`/`error` on `private.whatsapp_client_sessions` (the pipeline's `StateSource`):
`action_required` on a pairing prompt, a logout, or a window that never connected, and
`ok` the moment it connects. The repair is on the phone: cancel the stalled
`whatsapp_client_job` run so a fresh code is issued, then WhatsApp > Settings > Linked
Devices > Link a Device > "Link with phone number instead" and enter the pairing code from
the new run log within about two minutes. History sync then backfills the gap by itself.

Caveats: unofficial clients violate WhatsApp ToS and carry a small account-ban risk. neonize is
pinned exactly (0.4.3.post0) and **must be bumped when WhatsApp rejects the bundled whatsmeow
version** — the failure looks like `Client outdated (405) connect failure` in the run logs. In
2026-07 the stale pin crash-looped the client for ~2.5 days (~3k red runs) because goneonize also
panics (SIGABRT) marshaling the empty ClientOutdated event. Two rules keep that from recurring:
subscribe only to events the warehouse consumes (`_register_event_handlers` — goneonize marshals
KeepAliveRestored/StreamReplaced/ClientOutdated as empty protos and panics on zero-byte marshal,
and Go only marshals subscribed events), and keep the pin fresh enough for WhatsApp's version
gate. neonize's Go shared library is pre-fetched in the Dockerfile (`import neonize.client` at
build).

WhatsApp SQL starting points are `base_whatsapp.messages`, `base_whatsapp.chats`,
`base_whatsapp.chat_participants` (group rosters: one row per member with admin flags),
`base_whatsapp.contacts`, and `base_whatsapp.media_items`, with the resolved read view at
`marts_messages.whatsapp_messages`. Group subjects and rosters are populated by
a once-per-run-window `get_joined_groups()` dump in the client (history sync never carries
them); `whatsapp_chats.name` is preserved against later empty-name history rows.

Downloaded WhatsApp image (and document-PDF) media is enriched the same way Gmail attachments
are: the `whatsapp_media_enrichment` asset (`defs/whatsapp_media_enrichment.py`) scans
`whatsapp_media_items` for stored blobs (`is_missing = 0`), runs each through the agent-container
vision pipeline, and upserts the structured text into the shared `file_attachment_enrichments`
table — the renamed, source-agnostic successor to `gmail_attachment_enrichments` that both Gmail
and WhatsApp write to (keyed by `content_sha256` + `ai_provider`/`ai_model`/`ai_prompt_version`,
each source under its own `task_type`/`prompt_version`). The runner, image prep, agent prompt,
and candidate query all live in `file_attachment_enrichment.py`; each source is a
`FileEnrichmentSource` descriptor. That enrichment text is folded into the parent WhatsApp
message's timeline search document and surfaced by `search_text()` under `source = 'whatsapp'`.

## Hacker News (his items, his lists, and every discussion under them)

**Start at `base_hacker_news.items`**, or at the timeline with `sources => ARRAY['hacker_news']`.
The archive is deliberately NOT a mirror of Hacker News (the public corpus is ~50M items and
grows ~4M a year); it is every item Zach touched plus the **complete comment tree** of every
story he touched:

| relation | where it comes from | credential |
| --- | --- | --- |
| `submitted` (his stories AND comments) | `/v0/user/<account>` on the Firebase API — the whole list in one request, so it is a full walk every run | none |
| `favorited` | `news.ycombinator.com/favorites?id=<account>` (+ `&comments=t`) | none |
| `upvoted`, `hidden` | `/upvoted`, `/hidden` — shown only to the logged-in user | the browser's `user` cookie, published by `pdw hn publish-session` |

`base_hacker_news.user_items` is one row per `(item_id, relation)`; `removed_at` is the epoch
while the relation is live and a weekly full walk of each list stamps it when an upvote or
favorite is taken back. Every item named by a list is fetched from the API, then its parent
chain up to the story, then the **frontier** — every `kids` id of an archived item that is not
itself archived — until the budget (`HACKER_NEWS_MAX_ITEM_FETCHES_PER_RUN`, 3,000) runs out.
**The table is the walk state**: a run cut short resumes by asking the same set-difference
question, so there is no cursor to repair. Items in discussions whose story is younger than
`HACKER_NEWS_LIVE_WINDOW_DAYS` (3) are re-read every `HACKER_NEWS_REFRESH_MIN_AGE_HOURS` (6),
because a comment's `kids` list is the only way to learn about a new reply; older threads are
settled and left alone.

`account` is the **HN username**, not an email — it keys every row and it is what the timeline
compares an item's `author` to. One adapter (`hacker_news_item`, source `hacker_news`, kind
`hn_item`) covers the source: an item Zach wrote or acted on (upvote, favorite, hide — an action
he took, the same rule that keeps a card purchase at `self`) is `self`, a reply to one of his
items is `direct`, the rest of an archived thread is `noise` (it was `cc` until 2026-09-20, when
it was found paging Zach for every comment under a story he had upvoted: 17,908 pushes in a week
against 62 from every other source). `context` is the root story id, so
`timeline.context()` on any HN hit returns the discussion. `body_text` is the API's HTML body
decoded at ingest; `text` is the raw HTML, kept faithful.

**A login page on a private list is a credential verdict, not an empty list.** The poller marks
that exact cookie rejected (`private.hacker_news_sessions.expired_token_sha256`), records
`action_required` on the `upvoted`/`hidden` rows of `ops.hacker_news_sync_state`, and keeps
running the public lists and the item walk — so a dead cookie degrades to "public only", never to
silence, and `marts_ops.pipeline_health` reads `attention` for the `hacker_news` pipeline. The
repair is `pdw hn publish-session` on the Mac whose Chrome is signed in to news.ycombinator.com
(it refuses to publish one user's cookie under another username, and checks the cookie against a
login-only page before publishing; native Go, `app/internal/browsersessions/hackernews`). HN's login cookie is long-lived, so this is setup, not a chore.

Config (Dagster deployment): `HACKER_NEWS_ACCOUNT` (the username; required to enable the
source), `HACKER_NEWS_ENABLED=0` to pause, and the budgets above. The sensor polls every
`HACKER_NEWS_POLL_INTERVAL_SECONDS` (30 min).

```sql
SELECT i.posted_at, i.item_type, i.author, coalesce(nullif(i.title, ''), left(i.body_text, 80)) AS what,
       array_agg(u.relation) FILTER (WHERE u.relation IS NOT NULL) AS relations
FROM base_hacker_news.items i
LEFT JOIN base_hacker_news.user_items u
  ON u.account = i.account AND u.item_id = i.item_id AND u.removed_at <= '1970-01-01'
WHERE i.author = i.account
GROUP BY 1, 2, 3, 4 ORDER BY i.posted_at DESC LIMIT 20;
```

## Shared file-attachment enrichment

**Start at `marts_files.attachments`**: one conformed row per attachment from every source
that exposes attachment bytes or a retrievable provider URL — `base_gmail.attachments`,
`base_whatsapp.media_items`, `base_apple_messages.attachments`,
`base_apple_notes.attachments`, and `base_slack.files` — with `source`,
`parent_id` (the message or note), `attachment_id`, `filename`, `mime_type`, `size_bytes`,
`content_sha256`, `is_stored` (bigint 0/1: the bytes are in the object store), `is_deleted`,
`occurred_at` and the `storage_*` columns. Apple Notes attachments appear once per note
revision. That view is the **input** to the file (vision), audio (AssemblyAI) and
deterministic-text enrichment passes and to the receipt pass's attachment-evidence check —
never the raw tables. Until 2026-08-27 each `FileEnrichmentSource` /
`AudioEnrichmentSource` / `TextExtractionSource` named its raw table, which is the C5 hole:
a receipt that arrived over WhatsApp was invisible to the receipt pass, Apple Notes
attachments were enriched by nothing, and Slack image fingerprinting scanned its raw table.
Apple Notes image/PDF vision and deterministic text extraction now have explicit descriptors;
Notes audio remains in the multi-source `marts_voice_memos.recordings` transcription flow so
it is not transcribed twice. Slack rows carry `storage_backend = 'slack'` and a live
`storage_url` rather than claiming object-store residency; the fingerprint candidate scan
reads this mart and the existing Slack fetcher retrieves the bytes. `ALLOWED_RAW_ATTACHMENT_SOURCES` in
`tests/test_repo_contracts.py` is empty on purpose; adding an entry re-opens the hole.

`gmail_attachment_enrichments` was renamed to `file_attachment_enrichments` and generalized into
a single source-agnostic enrichment pipeline (`file_attachment_enrichment.py`). To add a new
attachment source: add its UNION branch to `marts_files.attachments`
(`_ensure_files_mart_views`), then define a `FileEnrichmentSource` whose `table` is
`marts_files_attachments` and whose `stored_predicate` is `a.source = '<source>' AND
a.is_stored = 1` (the descriptor exists only to give that source its own task_type/prompt
version), and wire a Dagster asset/sensor that runs `FileAttachmentEnrichmentRunner` with it —
see `defs/gmail_attachment_enrichment.py` and `defs/whatsapp_media_enrichment.py`. The table
rename migrates in place via `ensure_*` (`ALTER TABLE IF EXISTS … RENAME`), preserving
existing rows.
