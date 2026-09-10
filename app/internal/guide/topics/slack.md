# Slack

## Identity

Zach's Slack user id is `U09UE480JHH` (`name = zach`, workspace `T0266FRGM`). Do not
infer it from an older-looking `U02…` id or from a display name: a past session
filtered on the wrong id, reported "no messages from Zach", and had been reading
somebody else. Confirm any id you build a who-said-what query on from `base_slack.users`
(`real_name`, the `@handle` in `name`, and `display_name` are three different things).

## Relations

- `base_slack.messages` — time column `message_datetime`; `thread_ts` marks a reply;
  `subtype` carries Slack's own message kinds. A permalink is
  `https://<team_domain>.slack.com/archives/<conversation_id>/p<ts without the dot>`
  (with `?thread_ts=…&cid=…` for a reply); the team domain is in `base_slack.teams`.
- `base_slack.conversations` — `conversation_kind` says im / mpim / private_channel /
  public_channel. **Render by kind, never by name**: a DM's `name` is the other user's
  id and a group DM's is `mpdm-a--b--c-1`.
- `base_slack.users`, `base_slack.conversation_members`, `base_slack.message_reactions`,
  `base_slack.files`, `base_slack.teams`, `base_slack.account_identities`.
- `marts_inbox.slack_items` — what is unread or waiting on Zach.
- `marts_slack.huddles` — one row per huddle: `started_at`, `ended_at` (NULL while live),
  `duration_seconds`, `created_by`, `participant_user_ids`, `conversation_name`.
- `marts_slack.image_fingerprints` — perceptual hashes of Slack images; "who posted this
  picture" is a hash of the picture ranked by XOR distance, joined to `base_slack.users`.
- `derived_slack.inbox_items`, `derived_slack.conversation_stats`, `derived_slack.file_fingerprints`.

Search scope `slack` (and `slack_files`). A Slack hit's `timeline.context()` is its thread
when it is in one, otherwise the messages around it in its conversation. Public-channel
chatter not aimed at Zach is `noise` even in channels he is a member of; a DM, a real
`<@id>` ping or a conversational thread of his is `direct`; private team channels and
people talking about him in public are `cc`.

## What is synced, and the edges

- Everything Zach participates in — member channels, DMs, group DMs — is refreshed from
  a change feed within minutes; DM landing latency is judged in
  `marts_ops.slack_conversation_health` (`landing_p95_seconds`).
- The ~13k public channels he is **not** in are swept on a rotation of about a day. They
  were listed but never re-read between roughly May and 2026-08-27, so any answer drawn
  from a non-member public channel in that window came from a fraction of the corpus.
  `history_polled_fraction` in the health view says how much of the rotation is current.
- Group DMs and public channels created after 2026-05-18 were invisible until 2026-08-24
  (a discovery walk that restarted at page 1). A negative Slack result from before then
  is not evidence; re-run it.
- **Huddles: metadata yes, content never.** Slack exposes no huddle audio and no AI
  huddle notes, so nothing said in a huddle reaches PDW and nothing can make it. Zach
  makes real decisions in huddles: absence of a decision in the warehouse is never
  evidence it was not made — say so instead of reporting a confident negative.
- Slack file bytes: {{if .CLI}}`pdw call get_object --data '{"storage_file_id": "F…"}'`{{else}}`get_object` with the `F…` file id{{end}} resolves the file live
  through the workspace token and returns a signed download URL. The public
  `slack-files.com` permalink 404s unless the file was shared publicly; do not build a
  second fetch path.

## Health

`marts_ops.pipeline_health` rolls Slack up as one pipeline and ~19k public-channel
messages a day keep it `ok` through a total group-DM outage, so the per-type check is
`marts_ops.slack_conversation_health`: one row per conversation type with
`refreshed_fraction` (share of live conversations re-listed within a cycle — the number
that means something; ok ≥ 95%), `history_polled_fraction` (judged for public channels),
the DM landing latency columns, and `status` as the worst of them. The status is about
the sync attempt, not message volume: group DMs have legitimate zero-message days.
