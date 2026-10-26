# Sources: where each domain starts

Every source lands in `timeline.events` (search first), has a faithful `base_<source>`
copy, and — where several sources describe one thing — a conformed `marts_*` read view.
Start at the mart when one exists; drill to `base_*` from a hit's `source_table`/`source_pk`.
{{if .CLI}}Run `pdw columns <relation>`{{else}}Call `describe_table`{{end}} before writing SQL against any of these.

## Mail (Gmail)

- `base_gmail.messages` — one row per message; `from_address` is text,
  `to_addresses`/`cc_addresses`/`bcc_addresses` are `text[]`; bodies in `body_text`/`body_html`.
- `base_gmail.attachments` — attachment metadata with `storage_file_id` for the bytes.
- `marts_inbox.gmail_threads` — the inbox as threads, for triage.
- `base_gmail.labels` (`label_id` → `name`, so `messages.label_ids` reads as names) and
  `base_gmail.filters` (`criteria_json`, `action_json`, `add_label_ids`) — the mailbox's
  current labels and rules, snapshotted every 30 minutes; filter on `is_deleted = 0`.
- Search scope `gmail`. A Gmail hit's `timeline.context()` is its thread.

**Recent-email review:** pull a bounded local-time window of `timeline.events` with
`priority IN ('self','direct','cc')`, newest first; identify correspondence involving
Zach; narrow to `source = 'gmail'` for those threads; open a thread only to confirm
recipients and whether Zach already replied. Do not lead with automated mail unless asked,
and state the window and freshness.

## Chat (iMessage/SMS, WhatsApp, Slack)

- `marts_messages.messages` — iMessage, SMS and WhatsApp together, senders resolved
  through `marts_contacts.contact_points`; `marts_messages.apple_messages` and
  `marts_messages.whatsapp_messages` per source.
- `base_apple_messages.messages` (+ `chats`, `handles`, `chat_handles`, `chat_messages`,
  `attachments`); `base_whatsapp.messages` (+ `chats`, `chat_participants`, `contacts`,
  `media_items`).
- Slack: `base_slack.messages` (time column `message_datetime`), `conversations`, `users`,
  `files`, `message_reactions`, `teams`; `marts_inbox.slack_items` for what is unread or
  waiting; `marts_slack.huddles` for huddle metadata. Topic `slack`.
- Search scopes `apple_messages`, `whatsapp`, `slack`. A chat hit's `timeline.context()` is
  the rest of that chat; a Slack hit's is its thread or channel.

## Calendar and contacts

- `base_google_calendar.events` — every synced calendar; expanded recurring instances.
- `marts_contacts.contacts` — Apple and Google cards unified; `marts_contacts.contact_points`
  — normalised phones and emails for identity joins. Raw cards: `base_apple_contacts.cards`,
  `base_google_contacts.cards`.
- `marts_calendar.events_with_voice_memos` / `marts_calendar.unmatched_voice_memos` —
  meetings joined to the recording made during them.

## Files, notes, photos

- `base_google_drive.files` with extracted text in `derived_documents.google_drive_file_texts`;
  search scope `drive`. A file id found in a URL or an email searches with `exact`.
- `base_apple_notes.notes`, `.revisions`, `.attachments` (call recordings and voicemails are
  audio attachments and are transcribed through the voice pipeline).
- `marts_photos.photos` — one row per deduplicated logical photo with its AI caption;
  `marts_photos.files` every rendition; `base_apple_photos.files` raw. Search scope `photos`.
- `marts_files.attachments` — one conformed row per attachment from Gmail, WhatsApp,
  iMessage, Android texts, Apple Notes and Slack, with `content_sha256`, `is_stored` and the
  `storage_*` columns; enrichment text in `derived_enrichment.file_attachment_enrichments`.
  Bytes: {{if .CLI}}`pdw call get_object --data '{"storage_file_id": "<id>"}'`{{else}}`get_object` with the `storage_file_id`{{end}} (a Slack `F…` file id works directly).
  Fetch the returned `download_url` with `curl`, `requests` or `wget`, not Python's
  `urllib`: the public edge rejects urllib's default User-Agent with a 403 that looks
  like an expired link. Verify the SHA-256 the response carries.

## Voice recordings and transcripts

- `marts_voice_memos.recordings` — one row per recording from every voice source (Apple
  Voice Memos, Google Recorder on the Pixel from 2026-10-23, a second recorder, Notes
  audio) with `title`, `summary`, `transcript`,
  `participants_json`, `action_items_json` and the calendar match; `marts_voice_memos.transcript_segments`
  the speaker-labelled utterances. Search scope `voice_memos`/`transcripts`. Both name the
  speech model that produced the transcript in `transcript_model`.
- `marts_voice_memos.sessions` — one row per event: back-to-back recordings that an agent
  judged to be one event (a memo stopped and restarted) merged into one `title`, `summary`,
  `transcript`, `participants_json` and `action_items_json`; `recording_ids` lists the members.
- `base_google_recorder.recordings` — the Pixel's Recorder rows, with Recorder's own place
  name in `location_name`; read transcripts through the mart (`source = 'google_recorder'`).
- `recorded_at` is when recording started, as an absolute time. Before 2026-10-16 Apple
  memos stored the phone's local clock as UTC (4-5 h early); memos still on the Mac were
  corrected, so an old memo that reads oddly early may be one that was not.
- The speech model sometimes mishears one organisation name ("Hack Club" as "HackPad");
  search both spellings when completeness matters.

## Health (WHOOP)

Two sources for one wearable: `marts_health.cycles`, `.sleeps`, `.recoveries`, `.workouts`
conform them and translate every unit and sentinel; `base_whoop_private.*` holds the
six-second heart rate, hypnogram, journal and trend documents the public API lacks.
Healthspan (WHOOP Age, Pace of Aging, the contributors behind them, weekly), Advanced Labs
biomarkers and WHOOP Coach chats are `marts_health.healthspan_weeks`, `.healthspan_contributors`,
`.lab_results` and `base_whoop_private.coach_messages`.
Read topic `health` before quoting a number — the unit traps are 1000x errors.

## Finance

`marts_finance.net_worth` (with `staleness` per line), `net_worth_history`, `accounts`,
`transactions` (signed: positive = inflow), `commitments`, `security_transactions`,
`tax_lots`, `position_coverage`, `investment_holdings`, `liabilities`; the ledger facts in
`derived_finance.*`; raw Plaid in `base_plaid.*`, raw SimpleFIN in `base_simplefin.*` (the
same institutions, opposite sign conventions, reconciled by the ledger) and uploaded statements in
`base_manual_finance.documents` with agent extractions in `derived_finance.document_extractions`.
Read topic `finance` before quoting a total: several guards deliberately withhold a number.

## Hacker News

`base_hacker_news.items` is one row per archived item (story, comment, job, poll), keyed by
HN's `item_id`; `account` is the HN username. It holds Zach's own stories and comments,
everything he upvoted, favorited or hidden, and the **complete discussion** under every
one of those stories — not a mirror of all of HN, so absence here says nothing about HN.
`base_hacker_news.user_items` says WHY an item is archived (`relation` in `submitted`,
`favorited`, `upvoted`, `hidden`; `removed_at` is 1970 while live). `root_story_id` groups
a whole thread; `body_text` is the decoded body, `text` the raw HTML. Timeline source
`hacker_news`, search scope `hacker_news`: his items and anything he acted on are `self`,
replies to him `direct`, the rest of a thread `noise`; `timeline.context()` returns the thread.

## GitHub

`base_github.issues` is one row per issue or PR (`is_pull_request = 1`) Zach is involved in
(opened, commented, mentioned, assigned, reviewed); `account` is his login and `repo` +
`issue_number` name it. `base_github.comments` is the whole thread by anyone (`comment_kind`
`issue_comment`, `review`, `review_comment`); `base_github.commits` his authored commits (default
branches); `base_github.events` his 90-day activity feed. Timeline source `github`, search scope
`github`, `context` = `repo#number`: his acts are `self`, a person on his issue or PR or
@-mentioning him `direct`, a thread he was active in that month `cc`, bots `noise`. Prefer these
rows over GitHub notification emails in Gmail, which are `noise`.

## Web browsing (Chrome, Brave, Safari)

`base_browser_history.visits` is one row per visit from the Mac's browsers, every profile:
`browser` (`chrome`/`brave`/`safari`), `profile`/`profile_name`, `device`, `url`, `host`,
`title`, `visited_at`, `transition` (Chromium's `typed`, `link`, `reload`, …; empty for
Safari), `search_term` (set on a web search). `is_redirect`/`is_synced` are 0/1; a synced
visit came from another device (Safari via iCloud includes the phone). Chrome keeps ~90
days, Safari ~a year, so older history never existed here. Timeline source
`browser_history`, search scope `browser_history`, all `self`: kind `search` for a search,
`visit` for a page view. The timeline skips redirect hops, subframes, reloads and
revisits of one page within 30 minutes; query `base_*` for every visit.

## Texts on the Android phone

`base_android_messages.messages` is one row per SMS, MMS or RCS message on the Android phone
(`device` `pichu`) from the switch on 2026-10-23 16:00 EDT; earlier texts are
`base_apple_messages`. Columns: `message_at`, `is_from_me`, `sender_address`, `sender_name`,
`chat_key` (the conversation), `chat_name`, `is_group`, `body_text`, `parts` (jsonb),
`protocol_name` (`sms`, `mms`, `rcs`), `message_kind` (`text`, `attachment`, `reaction`,
`system`), `reaction_emoji` + `reacted_message_key`, `reply_to_message_key`, `edited_at`,
`receipt_read_at`, `link_url`; the rest of Messages' detail (read receipts per recipient,
reaction tallies, link previews) is in `raw_json`. Files are `base_android_messages.attachments`
(`file_status`, `storage_file_id`) and `marts_files.attachments`. `marts_messages.messages`
includes the texts (source `android_messages`). Timeline source `android_messages`, kind `message`.

## Phone notifications (Android)

`base_android_notifications.events` is one row per notification event on the Android phone
(`device` `pichu`, from 2026-10-24): `event_type` `posted` (new, or an update whose text
changed), `removed` (with `removal_reason_name`: `click` is a tap, `cancel` a swipe,
`app_cancel` the app) or `active` (already showing when the listener connected). Rows with
`event_source` `system_log` are what happened on screen, keyed by `notification_key`:
`visibility`, `expansion`, `clicked`, `action_clicked`, `alert`, `canceled`, `heads_up`, plus
`shade_opened`/`shade_closed`, `screen`, `status_bar`; `exposure_ms` is time seen, details in
`raw_json`. `interruption_filter` is Do Not Disturb. Columns:
`package_name`, `app_label`, `title`, `text`, `big_text`, `messages` (jsonb thread:
sender, text), `category` (`msg`, `email`, `call`, …), `channel_name`, `is_ongoing`,
`posted_at`, `notification_key` (one notification across its events). Timeline source
`android_notifications`: kind `notification` is each notification's first appearance with
each text (a one-to-one message or call `direct`, a group conversation `cc`, ongoing
`background`, the rest `noise`); kind `opened` is a tap and `notification_action` a button,
expansion, swipe or snooze, both `self`. Texts themselves are
`base_android_messages`. Android redacts one-time codes and other sensitive notifications for this listener.

## Screen time (Android)

`base_android_screen_time.daily_usage` is Android's own daily screen time, one row per device
(`device`, the Pixel is `pichu`), bucket and app or device-level counter: `usage_kind` `app`
has `package_name`, `app_label`, `total_time_in_foreground_ms`; `usage_kind` `device` has
`usage_event_type_name` `SCREEN_INTERACTIVE` (`total_time_ms` = screen-on time) and
`KEYGUARD_HIDDEN` (`total_time_ms` = unlocked time, `event_count` = unlocks). Buckets run
`bucket_start_at` to `bucket_end_at` (about 24 h, not midnight to midnight).
`base_android_screen_time.events` is every usage event (`usage_event_type_name`
`ACTIVITY_RESUMED`/`ACTIVITY_PAUSED` per `package_name`, `SCREEN_INTERACTIVE`,
`KEYGUARD_HIDDEN`, ...) at `event_at`. Android keeps about ten days, so a device's history
starts ten days before its first upload (`pichu`: from its setup on 2026-10-23). Not on the timeline.

## Screen timelapse (what was on a screen)

A periodic image of a device's whole screen, about once a minute while it is unlocked and
in use: `device` `crobat` (the Mac), `pichu` (the Android phone, from 2026-10-23;
AVIF frames, sent when it is on Wi-Fi, so they can land hours late) and `joltik` (the
iPhone before it). `base_screen_timelapse.screenshots` is one row per image: `device`,
`captured_at`, `display_index` (a two-display Mac writes two per moment), `mime_type`,
`storage_file_id`. `base_screen_timelapse.accessibility_trees` is the accessibility tree of
the same moment, when the loop could read one; join on `(device, captured_at)`, and expect
moments with only one of the two. Both are raw: the pixels and the tree are the stored
object, so read one with `get_object`; a tree's text is also inline in `content_text`.
A moment whose tree shows a card number is redacted at the source: the tree says
`[redacted card number]`, a screen the number can be placed on is boxed out in red, and the
moment's other screens are withheld, so expect such moments with fewer screenshots.
`marts_screen_timelapse.active_screens` is what was on screen: one row per frame an agent
judged in use (black, locked and not-yet-read frames are absent) with `app`, `window_title`,
`summary`, `screen_text`, `evidence` (accessibility tree, screenshot or both) and the frame's
`storage_file_id`. Only frames from the last day are read so far. Timeline source
`screen_timelapse`, kind `screen_capture`, one `noise` event per moment with no search text
yet: reach it by device and time (`pdw context` on a frame shows the frames around it), not
by search.

## Muse

Muse is Meta's hosted personal agent, running on its own VM. Its chats are agent sessions
(source `muse`, topic `agent-sessions`). `base_muse.files` is its persistent workspace as
it stands, one row per `path`: `MEMORY.md` and `memory/` (what it believes about Zach and
the people around him), `workspace/goals/`, feed research, podcasts, deliverables in
`workspace/your_files/`, and what Zach attached in a chat (`workspace/user/`, the only
`self` files). Text is in `content_text`; a binary's bytes are one `get_object` on
`storage_file_id`; `is_deleted = 1` marks a path that is gone. Search scope `muse_file`.
Muse's connectors, phone data and database are not in PDW — only what is on its disk.

## Prior agent sessions

`marts_ai_conversations.sessions` (one row per session across Claude Code, Codex, Claude
Desktop, ChatGPT, OpenClaw, pi, Muse Code, Argon and Muse) and `marts_ai_conversations.events` (one row per turn
or tool call). Search scope `agent_session` with priorities `self,background`. Topic
`agent-sessions`.

## Timeline

`timeline.events` columns worth knowing: `event_ts`, `priority`, `source`, `adapter`,
`kind`, `actor`, `title`, `snippet`, `context` (a display label — not the conversation
identity for Gmail or Slack group DMs), `source_table` + `source_pk` (the one-hop drill
down), `metadata` (per-source extras), `seq` (monotonic change order, for consuming the
timeline exactly once), `first_seen_at`. `search_text` is the indexed document: search it
through the functions, never scan it.
