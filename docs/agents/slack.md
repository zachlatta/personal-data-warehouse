# Slack

Moved out of AGENTS.md on 2026-10-01 so the file every session loads holds only the
contracts and the rules every change needs. Start at [AGENTS.md](../../AGENTS.md).

## Slack conversation discovery, and the page-1 trap

**Slack sync has two independent halves, and only one of them was ever monitored.**
`conversations.list` *discovers* conversations into `base_slack.conversations`; every other
stage (freshness, coverage, read-state, members) then runs off those **cached** rows with
`use_existing_conversations=True`. So a conversation that discovery never sees is not
merely stale — it is invisible to every downstream stage forever, and no amount of healthy
message throughput anywhere else will reveal it.

Discovery walks the list in bounded slices, `SLACK_ASSET_METADATA_CONVERSATION_PAGE_LIMIT`
pages per metadata run. Until 2026-08-24 that walk **restarted at page 1 on every run and
stopped after one page**, because no cursor was persisted. Measured against the live Slack
API that day, the damage was:

| type | live | missing from the warehouse |
| --- | --- | --- |
| `mpim` | 2,788 | **172** — every group DM created after 2026-05-18 |
| `public_channel` | 13,157 | **1,948** — essentially every channel created after 2026-05-18 |
| `im` | 3,628 | 0 |
| `private_channel` | 114 | 0 |

1,181 real messages existed in Slack and nowhere in PDW. `im` and `private_channel`
survived only by luck — 114 private channels fit inside one 200-row page, and page 1 of the
`im` list happens to carry the newest DMs. For `mpim`, page 1 is the *oldest* 200 and every
new group DM lands on pages 12-13, which the walk could never reach. 2,354 of 2,597 mpim
rows still carried `synced_at = 2026-05-18T15:34:34Z`, the last full multi-page walk.

Three things now hold this up, and the second is the one that is easy to undo:

- **The walk is resumable.** `_refresh_active_conversations` stores its `conversations.list`
  cursor in `ops.slack_sync_state` under `object_type = 'conversation_list'` (`object_id` is
  the conversation type), resumes from it, and stores `''` at the end of the list so the next
  pass starts over and keeps cycling. A cursor Slack rejects (`invalid_cursor`) restarts the
  walk rather than wedging that type.
- **Both rotations are driven by state, not the wall clock.**
  `_metadata_conversation_types` picks the conversation type whose walk is furthest behind
  (preferring one mid-walk); `_coverage_stage` picks the coverage stage that has gone
  longest without running, from a `coverage_stage` row per stage in `ops.slack_sync_state`
  written only *after* the work happens. A clock rotation **silently forfeits a stage's
  turn whenever that run loses the shared Slack lock**, and a lock-starved stage still
  returns `MaterializeResult` with `skipped_due_to_lock: true` and a green run. Measured:
  six of every eight metadata runs did nothing (mpim metadata went 11.5 hours between
  refreshes), and **38 of 54 coverage runs over six hours — 70% — were no-ops**, which is
  why the 1,929 newly discovered public channels attempted exactly one backfill in their
  first fifty minutes. Reverting either rotation to the clock reintroduces this. The tell
  in Dagster is a materialization whose metadata carries no work counters at all
  (`conversations_seen` absent rather than zero).
- **`marts_ops.slack_conversation_health` judges the SHARE re-listed, per type.** This is
  the detector that was missing. `marts_ops.pipeline_health` aggregates Slack as a single
  pipeline and ~19k public-channel messages a day kept it `ok` with `state_error_rows = 0`
  through a total group-DM outage.

```sql
SELECT conversation_type, live_count, refreshed_count, refreshed_fraction, status,
       discovery_status, oldest_conversation_synced_at
FROM marts_ops.slack_conversation_health ORDER BY status, conversation_type;
```

**`refreshed_fraction` is the number that means something — not `max(synced_at)`, and not
the single oldest row either.** A page-1-only walk re-stamps the first 200 rows every hour,
so `max()` looks perfect while the tail is months old; that is exactly how this hid for
three months. But `min()` over-fires in the other direction: a conversation archived
*upstream* after we last listed it keeps `is_archived = 0` forever, because the only path
that would correct the flag is the same walk that excludes archived rows — so roughly 1% of
rows can never be re-stamped and the oldest-row rule is permanently red. Measured after the
repair: im 100%, mpim 100%, private_channel 99.1%, public_channel 99.2%, against 200 of
2,597 (**7.7%**) during the outage. Thresholds are `ok` >= 95%, `late` >= 75%, `stale`
below.

The status is also deliberately about the **sync attempt**, not about messages: mpim had
eleven legitimate zero-message days between 2026-07-11 and 2026-08-18, so alerting on "no
group DM messages" is a guaranteed false positive. It would also have been wrong here —
group DMs really were silent from 2026-08-20T18:57 onward, which is exactly what Slack
itself reports.

## Slack channels Zach is not in: discovery is not coverage

**Being listed is not being read, and for four months those were the same word
here.** Discovery walks `conversations.list` and stamps every public channel;
`marts_ops.slack_conversation_health` reported 99.2% of them re-listed and `ok`.
But nothing then *asked* most of them for messages. Freshness asks the change
feed, which only knows the ~690 conversations Zach participates in; coverage
only offers a channel whose history is not yet complete, so a channel drops out
of it for good the moment its backfill finishes. A public channel he can read
but has not joined was therefore fetched exactly once and then frozen.

Measured 2026-08-27 against Slack's own admin analytics (`slack.public_channel_analytics`
in the Hack Club warehouse, which is per-channel-per-day ground truth):

| | Slack | PDW |
| --- | --- | --- |
| public channels posting on 2026-08-21 | 718 | 205 |
| public-channel messages that day | 71,636 | 24,116 |
| public-channel messages in August | 1,662,044 | 662,927 (40%) |
| non-member public channels untouched for 14 days | — | 10,711 of 11,488 |

`#arvutitrack` posted 17,646 messages that day and PDW held none of them.
The monthly capture ratio ran 64-80% from December to June and fell to 51% in
July and 40% in August as more channels finished their backfill and froze.

**The fix is a sweep, and the thing that makes a sweep work is stamping a poll
that found nothing.** `slack_workspace_public_sweep_sync` (`4-59/5 * * * *`, on
its own advisory lock — it has ~13k conversations to walk and on the shared lock
it would get whatever six stages with hundreds of candidates left over, the same
reason freshness has its own) polls live public channels regardless of membership, in
two buckets: `hot` (a message within `SLACK_ASSET_SWEEP_HOT_DAYS`, default 7) and
`cold` (everything else), each ordered by *when we last polled it*
(`ops.slack_sync_state.updated_at`, NULLS FIRST so a never-synced channel goes
first). A channel with a cursor is resumed at it, so a quiet channel costs one
`conversations.history` call; a channel with no cursor is streamed in full, which
is how the 601 channels created in July 2026 and never fetched get their history.

Thread replies are deliberately **not** fetched inline: a page of 200 parents can
carry dozens of threads, and one busy channel would eat the whole run. They are
drained by `slack_workspace_thread_backfill_sync`, which selects parents whose
replies we do not hold from any conversation, newest first.

`SLACK_ASSET_SWEEP_HOT_LIMIT` (20) and `SLACK_ASSET_SWEEP_COLD_LIMIT` (40) are a
**rate budget, not a preference**: they are the sustained call rate this stage
adds (~12/min) against a measured ~39/min `conversations.history` ceiling shared
with freshness, threads, coverage and read-state. Cold is the larger bucket
because there are ~11.8k cold channels to ~1.5k hot ones, which puts the full
rotation at about a day — inside the four-day SLA the health view judges. Raise
them for a catch-up and put them back; every stage aborts gracefully on the
shared rate-limit budget, so over-asking does not fail runs, it starves the
other stages.

- **Membership is deliberately not a filter, and `is_member` is not trustworthy
  anyway.** `base_slack.conversations.is_member` said 202 while Slack's own client
  reported 316 member channels (2026-08); it is refreshed only by whatever last wrote the
  conversation row.
- **A poll that returns nothing must still be recorded**
  (`touch_slack_conversation_sync_state`, which advances `updated_at` and
  preserves the cursor, sync type and status). The candidate order is by last
  poll, so an unstamped poll re-picks the same channels forever and the tail is
  never reached. This is the one invariant that makes the rotation a rotation.
- **`marts_ops.slack_conversation_health` now judges both halves.**
  `history_polled_fraction` is the share of live conversations asked for new
  messages within a cycle, judged for every type since 2026-10-01 (polling is the
  only way a message is found). `status` is the worst of the halves; all are on
  `/pipelines`.
- **Ground truth for this lives outside PDW.** The Hack Club warehouse's
  `slack.public_channel_analytics` is Slack's own admin analytics, one row per
  channel per day with `messages_posted_count`. Compare against it rather than
  against a feeling that search results look thin.

**A member channel discovered late holds nothing before the day it was found, unless
coverage walks DOWN.** Measured 2026-08-28: of 15 member public channels created after
2026-05-18 and first listed by the page-1 discovery fix on 08-24, 8 held nothing before
08-24/25 — one of them a channel created in May that Slack shows posting ~3k messages a
day. Freshness polled them, read its four-hour window and persisted the
newest message as the cursor with `last_sync_type = 'partial'`, and coverage — which
selects on `NOT (ok AND full)` — then topped each one up from `cursor - 14 days` on every
rotation, which never reaches further back than the window it already had. A full stream
cut short by the rate budget leaves the same shape. Coverage now reads each partial
conversation's **floor** — the oldest top-level message in `base_slack.messages`, via
`load_slack_conversation_message_low_water` — and streams `conversations.history` with
`latest = floor` to the start of the conversation, never touching the forward cursor
(every state write carries `cursor_ts = ''`, which the upsert preserves). The messages
table is the cursor, so a budget abort resumes from a lower floor next slice, and the
eight production channels heal on their first coverage slice after deploy with no state
repair: their state already IS "ok, partial, cursor set", which is exactly the shape the
floor lookup selects. `test_member_channel_first_seen_by_freshness_is_backfilled_below_its_floor_by_coverage`
reproduces the whole round trip.

**Landing latency is now a column, judged for DMs.** `marts_ops.slack_conversation_health`
carries `landing_p50_seconds` / `landing_p95_seconds` (over messages written in the last
24 hours, `timeline.events.first_seen_at - event_ts`) and `landing_status`, folded into
`status` as the worst-of — but only for `im` and `mpim`, against
`SLACK_DM_LANDING_P95_SECONDS` (15 min ok, 60 min late, else stale). Measured 2026-08-28
while every other Slack health number read `ok`: 1:1 DMs p50 3.5 min / p95 62 min, group
DMs p50 46 min, and one DM's 18:13–18:30 messages arrived together at 19:15:16 — a single
`synced_at` stamp, twelve green five-minute freshness runs in between, and the timeline
sync landing 80k public-channel rows the whole hour. So the stage ran and the feed did
not name that conversation for an hour; not lock loss (freshness has its own lock), not
the rate budget (DMs are fetched first). **`base_slack.messages.synced_at` is not a
landing stamp**: that same 18:13 message read `synced_at = 19:15:16`, the re-fetch, which
is why the view reads the timeline's `first_seen_at` instead (95ms warm, ~39k buffers,
bounded by `timeline_events_source_time_idx`).

**The freshness stage is ONE runner over all four conversation types, and that is a
latency decision.** Until 2026-09-16 it built four `SlackSyncRunner`s, one per type, so
each type could carry its own history window and candidate cap — and each runner paid the
same fixed preamble (`ensure_slack_tables`, the full `ops.slack_sync_state` read,
`auth.test`) plus a `derived_slack.inbox_items` refresh on the way out. Measured that day
the preamble was ~4 minutes per type (most of it queued behind the index-refresh lock
described under [Timeline search](search.md#timeline-search-and-hybrid-retrieval)), so a 5-minute
cron ran 8–23 minutes, skipped every other tick, and DM landing p95 read ~30 minutes
while every other Slack number was `ok`. The per-type windows and caps survive as
`freshness_window_by_type` / `freshness_limit_by_type`, applied per priority group inside
the one pass; `test_slack_freshness_sync_runs_priority_cycle` pins the single runner.

**The tail was a conversation we had never heard of, not a slow fetch.** The freshness
pass takes its candidates from the *cached* `base_slack.conversations` rows, and discovery
is a paged `conversations.list` walk that rotates conversation types, so a conversation
created since it last passed waits for it: measured 2026-08-28, a group DM created 16:02
first reached `timeline.events` at **05:36 the next day** (13.6h). The median DM was 3
minutes the whole time, which is why this reads as a p95 problem and not as an outage. The
freshness pass now lists DMs, group DMs and private channels itself every pass (see
[Polling](#polling-how-the-sync-finds-new-messages)), and **a brand-new conversation streams in
full and skips the activity gate**: one production DM's first message sat *eight minutes*
outside the four-hour window and waited eight more hours for the coverage floor walk.
Streaming in full is cheap precisely because the conversation is new.

## Polling: how the sync finds new messages

**Slack's API cannot tell PDW which conversations have new messages, so the freshness pass
polls.** `conversations.list` returns no last-message marker — its `updated` tracks topic and
member edits (measured 2026-10-01, it matched the newest message for 16 of 116 recently
active DMs) — so the only way to find a message is `conversations.history` on the
conversation. From 2026-08-24 the sync asked Slack's own `client.counts` instead, in one
request through Zach's pasted web session; on 2026-10-01 Slack answered
`team_is_restricted` to a fresh, valid session (`auth.test` accepted it) and the change feed
was removed. Polling is the design.

One freshness pass runs every five minutes on its own lock (`slack_workspace_sync`) and does
three things:

- **It lists DMs, group DMs and private channels** (`conversations.list`,
  `types=im,mpim,private_channel`, 1,000 a page: ~8 calls for ~6,700 conversations), writes
  only the ids never cached, and streams those in full in the same pass. Before this a new
  conversation waited for the rotating discovery walk: group DMs created on 2026-09-30 took
  ~8 hours to land.
- **It polls each conversation when it is due** (`SLACK_FRESHNESS_DUE_INTERVALS`): active
  inside the freshness window (four hours for DMs and group DMs) every pass, within 14 days
  every 15 minutes, within a year every two hours, older every twelve hours
  (`SLACK_ASSET_FRESHNESS_DUE_MINUTES=warm,cool,cold` overrides; the session allowed
  `15,60,360`). DMs, group DMs and private channels go before any public channel, and
  within each the most overdue go first; one not yet due is skipped. Before this the pass polled every candidate in tier order,
  spent its budget on the ~250 conversations active in the last fortnight, and never reached
  the rest: in the 24 hours to 2026-10-01 only 108 of 3,717 DMs and 114 of 2,889 group DMs
  were polled at all, and a group DM quiet since 09-10 took 28 hours to land.
- **It stamps every poll** (`touch_slack_conversation_sync_state`), including one that found
  nothing, because the schedule reads the last poll.

**It can poll with Zach's pasted session instead of the workspace OAuth token** — his
decision on 2026-10-01, because the session is rate-limited far less, and switched off
again on 2026-10-03 when Slack flagged it (see
[Publishing the Slack session](#publishing-the-slack-session-a-paste-never-a-capture)). With the OAuth token a pass made
~210 `conversations.history` calls before its 120-second sleep budget ran out, and the due
backlog stopped shrinking at ~5,600 of ~6,700 conversations: the schedule asked for more than
the token's ~39 calls a minute could give. `SlackSessionApiClient` sends the session's token,
`d` cookie and the browser's own User-Agent; if Slack refuses the session itself
(`invalid_auth`, `team_is_restricted`, …) `SlackSessionFallbackClient` polls with the OAuth
token for the rest of the pass and logs it. A pass stops after
`SLACK_ASSET_FRESHNESS_PASS_SECONDS` (240) of polling, so the job fits its five-minute
schedule and hot DMs are polled every tick while a backlog drains across passes.
`SLACK_ASSET_FRESHNESS_USE_SESSION=0` polls with the OAuth token only. `marts_ops.slack_conversation_health` judges every type on
`history_polled_fraction` within its cycle (24 hours for DMs, group DMs and private
channels) beside DM landing latency.

**On the OAuth token alone the schedule has to fit ~240 polls a pass (2026-10-03).** The
first three passes after the session was switched off each stopped at the rate limit after
211-417 of 1,327-1,440 due conversations, and ~95 polls a pass went to public channels
active in the last two hours, ahead of due DMs. So public channels now go last, and the
cool and cold intervals doubled (1h to 2h, 6h to 12h): ~150 polls a pass for the quiet
DMs, group DMs and private channels, the rest for whatever is active. The cost is a DM
that wakes after more than a year of quiet: it can take up to twelve hours to land. Only
an event feed fixes that: an Events API subscription on the workspace app, which is
Zach's to set up.

**Read state rotates; it is not refreshed by activity.** `last_read` (what the inbox's
unread state is computed from) arrives only through `conversations.info`, which the
read-state pass calls for 25 conversations a run, both on its own five-minute schedule and
at the end of every freshness pass. Until 2026-10-03 it took the 25 most recently active
conversations every run, so the same busiest ones were refreshed every five minutes and the
rest froze at whatever `last_read` they had when they last ranked: six of eight sampled
conversations were behind Slack, one public channel by 105 days, `inbox_items` showed 313 of
412 recent conversations unread, and 110 approved `slack.mark_conversation_read` mutations
could never reach `observed`. Each refresh now writes a `read_state` row in
`ops.slack_sync_state` (`touch_slack_read_state_sync_state`, refused calls included), and
`load_slack_read_state_candidate_payloads` takes never-refreshed conversations first, then
the ones the warehouse thinks are unread, oldest refresh first. One already read past its
newest message waits, because only a new message can make it unread. A full rotation of the
apparently unread ones takes about half an hour.

**`derived_slack.inbox_items` is refreshed incrementally, and the watermark is the reason.**
Every Slack stage ends by refreshing that snapshot (`refresh_slack_account_state_items`).
Until 2026-08-27 each call rebuilt it from scratch — thirty days of every member
conversation's messages, read from the heap once per UNION branch, then every previous
row re-inserted as a tombstone — at 44s mean, ~40 calls an hour, 22.6 CPU-hours in 46h:
the single largest statement on the host while **eleven** conversations changed per five
minutes. It now records a watermark in `ops.slack_sync_state`
(`object_type = 'account_state_refresh'`, object_id `<team>`; `<team>:full` for the last
full rebuild) and recomputes only conversations whose row or messages were stamped after
it, minus a one-hour overlap, because a stage stamps rows with the `synced_at` it computed
at *start* and may commit minutes later. Items of a changed conversation that are not
re-emitted are tombstoned by `container_id`; items past the 30-day window are tombstoned
without touching their conversation; a full rebuild still runs once a day, so anything the
overlap misses is wrong for at most a day. Concurrent refreshes take
`pg_try_advisory_xact_lock` and **skip** rather than queue — four of them used to wait up
to eleven minutes on each other for an identical result. Delete the two state rows to
force a full rebuild.

### Publishing the Slack session: a paste, never a capture

**Nothing may capture or replay Zach's Slack login on a schedule; it signs him out of
everything.** Until 2026-09-29 an hourly LaunchAgent on crobat read the Slack DESKTOP app's
token and `d` cookie and called `auth.test` + `client.counts` with them, from Go, to pick the
right workspace. Slack's audit log (the Hack Club warehouse syncs it as `slack.audit_logs`)
recorded every successful run as an `anomaly`, reason `unexpected_scraping`,
`scraping_tool: "Go-based tool"` — a Slack desktop User-Agent over Go's TLS fingerprint, on
the desktop's own session, from the desktop's own IP — followed by
`user_sessions_reset_by_anomaly_event_response`: every device signed out. Four resets on
09-29 matched runs to the second. It had been quiet for weeks before (Slack's detector
changed around 09-20, the first reset), and it was quiet 09-20..09-28 only because the
capture was broken and never reached Slack. A manual Go test on 09-28 with a
`Go-http-client` User-Agent drew `unexpected_user_agent` as well.

So the session comes from a browser, once, by hand, and `pdw slack publish-session` talks to
nothing but the warehouse:

```bash
pdw slack publish-session            # prints the steps when run from a terminal
```

It asks for two pastes: the line a DevTools console snippet copies on app.slack.com
(`ConsoleSnippet`: every signed-in team's token, `id`, `user_id`, `enterprise_id` and `url`
from `localStorage.localConfig_v2`, plus `navigator.userAgent`), then the `d` cookie
(`xoxd-…`, DevTools → Application → Cookies). Identity comes from the same localStorage the
token does, so no call to Slack is needed to know whose session it is; the workspace guard
(the warehouse must already sync it, unless `--team-id` names it) runs against the
warehouse through the SQL tool. The browser's User-Agent is stored in
`private.slack_sessions.user_agent` and every server-side request spends the session with
it (`slack_session._slack_post`), rather than the Slack desktop User-Agent the helper used to
hard-code, which a browser-minted session has never presented.

**The server spends the session every five minutes, by Zach's choice, and that is a known
risk.** The freshness pass polls with it (hundreds of `conversations.history` calls a pass)
and sends and mark-reads use it, all from Python on mew-coolify with Python's TLS fingerprint
and the browser's User-Agent. Python spending a session was never flagged in months of
`client.counts`; this volume is new. Watch `slack.audit_logs` (Hack Club warehouse) for an
`anomaly` on Zach's user from mew's address: if Slack flags it, set
`SLACK_ASSET_FRESHNESS_USE_SESSION=0` on the Dagster deployment and tell him, rather than
disguising the requests better.

**It was flagged, and polling is back on the OAuth token (2026-10-03).** From 2026-10-01
17:30Z, three hours after the session was published, `slack.audit_logs` recorded an
`anomaly` on Zach's user from mew's egress IP every hour: `unexpected_api_call_volume`
plus `spoofed_user_agent` and `unexpected_client`, 67 in 35 hours. Slack did not reset his
sessions (its auto-response fires on `unexpected_scraping`, which this never drew), but
those are the reasons a Grid admin reads. `SLACK_ASSET_FRESHNESS_USE_SESSION=0` was set
on the Dagster deployment on 2026-10-03 at about 19:50Z. Sends and mark-reads still spend the
session, one approved request at a time. Those are the only session calls left, so an
`anomaly` from mew after 10-03 means one of them, or the env var was lost in a redeploy.
The `excessive_downloads` anomalies from AWS addresses with a `Go-http-client` User-Agent
in the same log come from AWS addresses, not mew, so they are not PDW.

**Mint the session in a private window used for nothing else, then close it without
signing out.** All six `unexpected_scraping` anomalies from 2026-09-20 to 09-29 in
`slack.audit_logs` (the Hack Club warehouse) carry `scraping_tool: Go-based tool` and a
`previous_ua` of the Slack desktop app: a session Slack knew as the desktop client suddenly
used by another one. The Python server spent those same desktop sessions for months and was
never flagged. A session whose whole history is the warehouse's has no other client to
contradict. Signing out ends it.

**`client.counts` answered `team_is_restricted` to a fresh session** pasted this way on
2026-10-01, although `auth.test` accepted it as the right user and workspace, on every host
and `slack_route` variant. That is why the change feed was removed rather than repaired:
adding browser headers is the disguise this section rules out.

**Enterprise Grid is a live trap here.** Hack Club is an Enterprise Grid org, so a client
session's `auth.test` returns the **org** id `E09V59WQY1E` where the app token returns the
**workspace** id `T0266FRGM` — and all ~45M warehouse rows are keyed by the workspace. Storing
one as the other would not error; it would write a second parallel copy of Slack. The web
client's `localConfig_v2` holds one entry per workspace (`T…`, with `enterprise_id`) and one
for the org itself (`E…`); the paste parser never puts an `E` id in `team_id`, the publish
endpoint rejects it, `publish-session` prefers the workspace entry, and an org-only paste is
resolved through `base_slack.teams.enterprise_id` rather than guessed (an org covering
several workspaces raises instead).

### History: the week the change feed went down (2026-09-21)

Seven days of "change feed unusable" read `ok` everywhere while group DMs stopped landing.
The desktop-app capture had published another workspace's session as production's (the
capture is gone; `publish-session` takes workspace-scoped `T…` entries first and only a
workspace the warehouse syncs), and the poll the sync fell back to walked `im`, then `mpim`,
each by recency, so it spent every pass on the same ~30 DMs: measured 2026-09-28 against
Slack's own history, **164 of 219 group-DM messages** in a week were missing. Both lessons
are now the design above: polling is judged, never a fallback, and it is scheduled by when
each conversation is due.

**To verify a Slack claim against Slack itself**, the user token (`SLACK_<ACCOUNT>_TOKEN`
in the gitignored `.env`) reads `conversations.history` for any DM or group DM; its
`search.messages` is `missing_scope`. It shares the production sync's rate budget, so keep a
sample to a few minutes of calls.

## Slack huddles: metadata yes, content no

**Huddle metadata is in the warehouse; huddle content never will be.** It is easy to
conclude huddles are missing entirely — Slack publishes no API that lists them, and none
that exposes huddle audio or Slack-AI huddle notes. But every huddle posts a message with
`subtype = 'huddle_thread'` whose payload carries a `room` object with `created_by`,
`date_start`, `date_end`, `has_ended` and the full `participant_history`. 5,942 of those
were already being ingested, unreachable only because they sat inside `raw_json`.

`marts_slack.huddles` is that, parsed: one row per huddle with `huddle_id`, `huddle_name`,
`created_by`, `started_at`, `ended_at`, `duration_seconds`, `participant_user_ids`,
`participant_count`, and the conversation it happened in.

```sql
SELECT started_at, conversation_name, huddle_name, duration_seconds, participant_count
FROM marts_slack.huddles
WHERE 'U09UE480JHH' = ANY(participant_user_ids)
ORDER BY started_at DESC LIMIT 20;
```

Two traps. `date_start`/`date_end` are epoch **integers** inside the JSON, not the
`timestamptz` the rest of the warehouse uses, and a huddle still running carries `0` — the
view converts both, reporting a live huddle as `ended_at IS NULL` rather than 1970 or a
negative duration. And a huddle's `participant_history` is everyone who ever joined, not
who was there at any one moment.

**What was said in a huddle is not in PDW and cannot be made to be.** Zach makes real
decisions in huddles, so absence of a decision in the warehouse is never evidence that the
decision was not made. Say so rather than reporting a confident negative.

## Sending Slack messages as Zach (`slack.send_message`)

The second reviewed Slack write beside `slack.mark_conversation_read`, and the first that
says something in his name — so every rule here is about the message going exactly where
the reviewer saw it going, once. Nothing is posted before a human approves the request.

- **The proposal names the recipient exactly.** `conversation_id` (`C…`/`D…`/`G…`) or
  `user_id` (`U…`/`W…`) for a DM, never both; `thread_ts` needs `conversation_id` (a
  thread lives in one conversation); `reply_broadcast` needs `thread_ts`; `text` is
  mrkdwn, at most 4,000 characters, because Slack truncates a longer message with a warning
  the executor could only see after approval. `app/internal/mutations/slack_send.go`
  refuses all of that at proposal time, and the same checks run again at storage.
- **The review resolves it against the warehouse and warns before approval.**
  `enrichSlackSendMessagePreviews` fills the `slack_message` preview at proposal time —
  the workspace, the recipient by name (and the DM the warehouse already holds with a
  person), the thread's parent and nearest replies or the conversation's newest six
  messages, and `warnings` for what the executor will refuse: not synced for this
  account, archived, not a member of the channel, a deactivated or bot user, a thread
  parent the warehouse does not hold. Faces and permalinks are hydrated on read, like
  mark-read. **Only the words are editable** in the web review
  (`POST …/mutations/<id>/update-slack-message`); the recipient and the thread were
  validated at proposal time and a wrong recipient is a deny, not a redirect. The phone
  renders the card read-only and says where to edit.
- **The executor re-checks everything live** (`slack_mutations.py`), through the client
  session `pdw slack publish-session` publishes — it posts as Zach, never as a bot: the
  session's identity (`auth.test`), the conversation in `base_slack.conversations` **for
  the session's team** (an Enterprise Grid session must not post into a sibling
  workspace), `conversations.info` (same id, not archived, and for a DM that
  `channel.user` is the proposed person), the thread parent in `base_slack.messages` (a
  reply's ts is re-pointed at its parent, recorded as `thread_ts_requested`), and for a
  `user_id` the person in `base_slack.users` (live, not a bot) then the synced DM or
  `conversations.open`, which is idempotent and posts nothing.
- **One approval sends one message.** Every attempt carries
  `client_msg_id = uuid5(fixed namespace, mutation id)` and, before posting, looks for it
  in `base_slack.messages` and then in `conversations.history` / `conversations.replies`
  from five minutes before the approval (falling back to a same-author, same-text match,
  because Slack rewrites entities and links in what it stores); a hit is `succeeded` with
  `already_sent: true` and `matched_by`. A pre-check that cannot run is
  `failed_retryable`, never a send. A `failed_retryable` send IS retried by the next
  worker run, which is safe only because of that check; a send is deliberately **not** in
  `RECLAIMABLE_IDEMPOTENT_OPERATIONS`, because a reclaim races a worker that may still be
  mid-post and the pre-check cannot see a message that has not landed. A stale
  `executing` send therefore waits for a human, exactly like `gmail.send_email`. The
  namespace constant must never change: a message sent under the old one would be
  invisible to the retry that follows.
- **Observation closes the loop** when the posted `message_ts` lands in
  `base_slack.messages` (`observe_succeeded_slack_send_message_mutations`), the same
  way a Gmail send is observed through its message id.
- Deployment needs nothing new: `SLACK_ACCOUNTS` already gates Slack proposals in the
  app, both cloud workers run `SlackMutationExecutor` with the published session, and both
  deploy from `main`. The first real send was a one-line DM to Zach himself on
  2026-10-03 (approved on the phone, posted 12 seconds later).
- **Both workers must hold every cloud executor.** The resident low-latency worker
  (`upstream_mutation_worker.py`, supervised inside the Dagster container) was built
  without the Slack executor, so it claimed each approved Slack mutation, deferred it as an
  unknown provider, was woken by its own status change and claimed it again. That
  happened 68 times in 12 seconds for that first send, and 709 times for one mark-read
  batch, until the Dagster fallback job won the lock. `slack_executor` is now a required
  argument of `process_upstream_mutation_batch`, so a worker cannot be built without it.
## Slack file bytes and "who sent this image?"

### Getting a Slack file's bytes: `get_object` already does this

`get_object` takes a `base_slack.files.file_id` (`F...`) directly and returns metadata plus a
signed `download_url` that needs no further auth. The app resolves the file live through
`files.info` across every configured workspace token and downloads `url_private`
(`app/internal/objectstore/slack.go`, wired in `server.go`); it already holds the tokens.

```bash
pdw call get_object --data '{"storage_file_id":"F0EXAMPLE123"}'
curl -L -o poster.png "<download_url from the response>"
```

**A 403 on a fresh `download_url` is the client, not the link.** The public hostname sits
behind Cloudflare, whose bot rule rejects Python urllib's default User-Agent
(`Python-urllib/3.x`) with `browser_signature_banned` before the request reaches the app,
so the app never sees it and its own logs stay silent. Measured 2026-09-09 on a link minted
seconds earlier for a manual-finance statement: urllib **403**, curl / wget / `requests` /
Go all **200**. A Codex session on porygon that day concluded the originals were unreachable
"through that route" while their extracted text was fine; the link had been valid the whole
time. `get_object` now carries a `hint` naming the client beside every `download_url`, and
its description says so before the link is minted. Fetch with `curl`, `requests`, or any
client that sends a descriptive User-Agent — a browser string is not needed, the rule
targets the known-bot signature. The same rule is why every server-side Python caller of
`PDW_API_URL` sets its own User-Agent (`agent_tool_proxy.py`, the benchmark runner, the
Slack fingerprinter).

**Do not build a second Slack fetch path.** A 2026-08-16 session concluded pdw could not fetch
Slack file bytes and guessed an answer; it had tried the *public* `slack-files.com` permalink,
which 404s unless the file was explicitly shared publicly. `url_private` needs the bearer token
and `get_object` supplies it. Sampled 2026-08-18 across recent, 2016-era and mid-era files,
images and non-images: 19/19 returned bytes, including a 20 MB PNG.

Slack's sharpest trap is already handled there: an unauthorized `files.slack.com` GET returns
**200 with an HTML login page**, not a 4xx, and the store rejects that rather than returning it
as content.

### Identifying an image: fingerprints, then plain SQL

Slack image files are fingerprinted with the same 256-bit dhash the photos pipeline uses, into
the same `derived_enrichment.media_fingerprints` table. `derived_slack.file_fingerprints` links
a Slack file to the content sha its bytes hash to (PK `account, team_id, file_id` — one download
per file, not per share) and carries the status/attempts/backoff that make the backfill
resumable. **The bytes are never stored**: ~905k live Slack images total ~552 GB versus ~200
bytes per fingerprint, and a named file's bytes are one `get_object` call away.

There is **no new command**. Hash the picture, then run ordinary SQL:

```bash
uv run python -c "from personal_data_warehouse.slack_image_lookup import lookup_sql_for_image; \
  print(lookup_sql_for_image('/path/to/poster.png'))"     # prints ready-to-run SQL
```

Paste that into `pdw sql` (or the `query` tool). It ranks `marts_slack.image_fingerprints` by
`bit_count` XOR distance and **resolves the uploader** by joining `base_slack.users` — the join
the 2026-08-16 session never made. It reports **real_name, @handle and display_name separately**
because Slack keeps all three and they differ (a real row: `Real Name` / `@realname110` /
`realname`); that session was asked for the *handle* specifically.

The hash must come from that Pillow code path — a fingerprint computed by a different resampler
does not error, it silently stops matching.

Read distances as: **0–6 the same image**, 7–16 very likely, 17–28 possibly related, beyond that
check by eye. Verified on the real corpus: every re-encode/rescale of the motivating poster
hashes to distance **0**, while two *different* 11x17 posters sit at **124–125**. Byte size is
useless here — one re-encode was *larger* than the original, and the two copies in the incident
differed by 1.3 MB.

Backfill: the `slack_file_fingerprints` Dagster asset (hourly `:19`) takes a bounded
newest-first slice (`SLACK_FILE_FINGERPRINT_LIMIT`, default 300; `..._RUN_SECONDS`, default
900). Newest-first because recency is what people ask about. It fetches bytes **through the
app's `get_object`**, so it holds no Slack credential of its own and there is one Slack-file
implementation to fix. A 429 ends the slice cleanly without burning the file's retry budget.
**The table is the cursor**, so it resumes by itself; there is no watermark to repair.

Two gotchas worth knowing:

- **Print artwork breaks photo defaults.** The motivating poster is 420,750,000 pixels (11x17
  inches at 1500 DPI), far past Pillow's ~89 MP decompression-bomb guard. `compute_dhash` takes
  an opt-in `max_pixels` (Slack uses 512 MP) that is scoped and restored, so the photos pipeline
  keeps its own posture. Without it that exact file is `undecodable` forever.
- **A DM's `name` is the other user's id**, and a group DM's is `mpdm-a--b--c-1`. Render by
  `conversation_kind`, never by name, or a DM prints as a channel that does not exist.

Coverage is not proof of absence — only fingerprinted files are searchable:

```bash
pdw sql --output json -q "slack fingerprint coverage" \
  "SELECT status, count(*) FROM derived_slack.file_fingerprints GROUP BY 1 ORDER BY 2 DESC"
```

End-to-end check against real Slack bytes in a throwaway schema (writes nothing to prod):
`uv run python scripts/verify_slack_image_lookup.py --file-id <F...> [--probe copy.png]`.
