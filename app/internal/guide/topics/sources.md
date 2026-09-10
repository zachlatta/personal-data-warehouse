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
  iMessage, Apple Notes and Slack, with `content_sha256`, `is_stored` and the
  `storage_*` columns; enrichment text in `derived_enrichment.file_attachment_enrichments`.
  Bytes: {{if .CLI}}`pdw call get_object --data '{"storage_file_id": "<id>"}'`{{else}}`get_object` with the `storage_file_id`{{end}} (a Slack `F…` file id works directly).

## Voice recordings and transcripts

- `marts_voice_memos.recordings` — one row per recording from every voice source (Apple
  Voice Memos, a second recorder, Notes audio) with `title`, `summary`, `transcript`,
  `participants`, `action_items` and the calendar match; `marts_voice_memos.transcript_segments`
  the speaker-labelled utterances. Search scope `voice_memos`/`transcripts`.
- The speech model sometimes mishears one organisation name ("Hack Club" as "HackPad");
  search both spellings when completeness matters.

## Health (WHOOP)

Two sources for one wearable: `marts_health.cycles`, `.sleeps`, `.recoveries`, `.workouts`
conform them and translate every unit and sentinel; `base_whoop_private.*` holds the
six-second heart rate, hypnogram, journal and trend documents the public API lacks.
Read topic `health` before quoting a number — the unit traps are 1000x errors.

## Finance

`marts_finance.net_worth` (with `staleness` per line), `net_worth_history`, `accounts`,
`transactions` (signed: positive = inflow), `commitments`, `security_transactions`,
`tax_lots`, `position_coverage`, `investment_holdings`, `liabilities`; the ledger facts in
`derived_finance.*`; raw Plaid in `base_plaid.*` and uploaded statements in
`base_manual_finance.documents` with agent extractions in `derived_finance.document_extractions`.
Read topic `finance` before quoting a total: several guards deliberately withhold a number.

## Prior agent sessions

`marts_ai_conversations.sessions` (one row per session across Claude Code, Codex, Claude
Desktop, ChatGPT, OpenClaw and pi) and `marts_ai_conversations.events` (one row per turn
or tool call). Search scope `agent_session` with priorities `self,background`. Topic
`agent-sessions`.

## Timeline

`timeline.events` columns worth knowing: `event_ts`, `priority`, `source`, `adapter`,
`kind`, `actor`, `title`, `snippet`, `context` (a display label — not the conversation
identity for Gmail or Slack group DMs), `source_table` + `source_pk` (the one-hop drill
down), `metadata` (per-source extras), `seq` (monotonic change order, for consuming the
timeline exactly once), `first_seen_at`. `search_text` is the indexed document: search it
through the functions, never scan it.
