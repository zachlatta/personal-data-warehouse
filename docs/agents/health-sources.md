# Health: WHOOP

Moved out of AGENTS.md on 2026-10-01 so the file every session loads holds only the
contracts and the rules every change needs. Start at [AGENTS.md](../../AGENTS.md).

## Health (two WHOOP sources, one read interface)

**Start at `marts_health`, not at either `base_whoop*` schema.** WHOOP arrives twice — the
public developer API (`base_whoop`, summary grain) and the app API (`base_whoop_private`,
high resolution) — and reading either alone is wrong in a different direction. The public
source has no time series, no strain components, no sleep debt and no sport catalog; the
private source is missing rows the public one has (measured 2026-08-26: 305 public sleeps
vs 294 private, 268 public workouts vs 257). `marts_health.{cycles,sleeps,recoveries,workouts}`
conforms them, LEFT-joined from the public row outward, with `has_private_detail` saying
whether the higher-resolution row existed.

```sql
SELECT start_at, strain, sleep_need_seconds, has_private_detail
FROM marts_health.cycles ORDER BY start_at DESC LIMIT 7;
```

Two conforming rules are the reason the mart exists, and both are silent 1000x errors
without it:

- **`hrv_rmssd_milli` is the only HRV column.** `base_whoop_private.recoveries` records it
  in **seconds**; `base_whoop.recoveries` in milliseconds. The mart publishes one column,
  in milliseconds.
- **Every duration is `*_seconds`.** Public sleep-stage totals are milliseconds
  (`total_rem_sleep_time_milli`) and private ones are seconds; the mart exposes no
  `*_milli` duration at all, so two sources' columns cannot be added together by accident.

The epoch sentinel is translated on **every** exposed timestamp, which is what makes
`marts_health.cycles.end_at` NULL for the cycle still running instead of sorting oldest.
`test_health_mart_translates_every_exposed_timestamp_or_none` seeds an all-sentinel row and
checks the whole column list, per view, because translating some columns and not their
siblings manufactures an inconsistency the sources do not have.

The four WHOOP timeline adapters still read `base_whoop` directly and are unchanged:
repointing them would change `adapter_signature` and re-walk those adapters for no
behavioural gain, and the private tables are deliberately classified `detail` so the same
health events are not emitted twice.

## WHOOP (health)

Read-only OAuth sync against the WHOOP v2 API, running as the `whoop_sync` Dagster asset on
`whoop_sync_every_five_minutes`. Six source-owned tables — `base_whoop.profiles`,
`base_whoop.body_measurements`, `base_whoop.cycles`, `base_whoop.recoveries`,
`base_whoop.sleeps`, `base_whoop.workouts` — plus `ops.whoop_sync_state` (per-collection
watermark and status) and `private.whoop_oauth_tokens` (the credential). Cycles, recoveries,
sleeps and workouts each get a timeline adapter; profile and body measurements stay source
entities rather than repeated events.

```sql
SELECT start_at, strain, average_heart_rate FROM base_whoop.cycles ORDER BY start_at DESC LIMIT 30;
SELECT start_at, sleep_performance_percentage FROM base_whoop.sleeps WHERE nap = 0 ORDER BY start_at DESC LIMIT 30;
```

**A cycle is not its start date.** It runs sleep-onset to next sleep-onset, so the day it
reports is the day it is *awake* for: an onset at 11:12 PM Friday ending 12:07 AM Sunday is
the **Saturday** cycle. The in-progress cycle stores `end_at = 1970-01-01T00:00Z` — the
warehouse-wide "absent" sentinel, not NULL — so `ORDER BY end_at DESC` ranks the running
cycle *oldest*; bound on `start_at` instead.

**The credential rotates on every refresh, and that is the whole operational story.** WHOOP
refresh tokens are single-use: a successful refresh invalidates the pair that produced it, so
two concurrent refreshes have one winner and one permanently dead loser. Three production
incidents in 2026-07/08 came from exactly that. The repaired design has one authority —
`private.whoop_oauth_tokens` — and a `WHOOP_TOKEN_JSON_B64` env value may populate an *absent*
row once and can never replace an existing one. Every credential mutation (bootstrap,
scheduled refresh, direct CLI refresh, explicit reauthorization) takes the same Postgres
advisory lock, and refresh additionally holds the account row lock from the provider call
through the commit; a racer with the pre-rotation token waits and adopts the winner rather
than spending a consumed token.

A dead refresh token — the token endpoint answering 400/401/403, which no retry can clear —
records `status = 'action_required'` for every collection, fails the first no-progress run,
and then *skips* later ticks for that same rejected fingerprint so one dead credential cannot
generate hundreds of identical red runs. It stays `attention` on `/pipelines` until a real
success clears it, so an unchanged `action_required` row is an active incident, not a quiet
pipeline:

```bash
pdw sql -q "is WHOOP authentication healthy" \
  "SELECT pipeline, status, last_write_at, last_run_at, last_error
   FROM marts_ops.pipeline_health WHERE pipeline = 'whoop'"
```

Repair it by re-running the OAuth flow from a terminal with production database access:
`uv run personal-data-warehouse-whoop-auth --install` (add `--manual --no-browser` when the
browser is on another machine). The next tick sees the new fingerprint and self-heals with no
deployment restart. Never paste the callback URL, authorization code, or token into chat,
logs, or a commit. Full runbook, including the cross-host reverse-tunnel procedure:
[`docs/whoop-oauth-operations.md`](docs/whoop-oauth-operations.md).

## WHOOP private API (health, high resolution)

`base_whoop` is the *public* developer API, and it is summary-grain: one row per cycle,
sleep, recovery and workout, with **no time series at all**. Per-6-second heart rate, the
sleep hypnogram, the journal, and the trend metrics (VO2 max, weight, body composition,
steps) have no public endpoint whatsoever. Source `whoop_private` is the second WHOOP
source that fills that in by calling the endpoints `app.whoop.com` itself calls. Full
reconnaissance, including the dead ends nobody should re-walk:
[`docs/whoop-private-api.md`](docs/whoop-private-api.md).

**It is a separate pipeline from `whoop`, on purpose.** It has its own credential and its
own cadence, so `marts_ops.pipeline_health` reports `whoop` and `whoop_private`
independently and one of them dying is never hidden by the other still writing.

### SQL starting points

```sql
-- the day's heart rate, one reading every six seconds
SELECT sample_at, heart_rate FROM base_whoop_private.heart_rate_samples
WHERE sample_at >= now() - interval '1 day' ORDER BY sample_at;

-- the readings inside one workout, offset from its start
SELECT elapsed_seconds, heart_rate FROM marts_health.workout_heart_rate_samples
WHERE workout_id = '<id>' ORDER BY sample_at;

-- last night's hypnogram
SELECT stage, started_at, ended_at FROM base_whoop_private.sleep_events
ORDER BY started_at DESC LIMIT 50;
```

| relation | what it holds |
| --- | --- |
| `base_whoop_private.heart_rate_samples` | continuous heart rate at 6s, every hour of every day (`step` 6/60/600 are the only values the API accepts; 6 is the only one stored) |
| `marts_health.workout_heart_rate_samples` | that same series joined to each workout's own bounds, with `elapsed_seconds` |
| `base_whoop_private.sleep_events` | the hypnogram: one row per LIGHT / REM / SWS / DISTURBANCES stage |
| `base_whoop_private.journal_entries` | the journal answers Zach typed; **the only table here with a timeline adapter** |
| `base_whoop_private.cycles`, `.sleeps`, `.recoveries`, `.workouts` | high-resolution copies of the public rows (strain components, sleep debt, HRV/RHR components, zone durations, GPS) |
| `base_whoop_private.sports` | the 204-sport catalog resolving a workout's `sport_id` |
| `base_whoop_private.documents` | Tier-2 raw UI payloads kept as `raw_json`, keyed `(kind, doc_key)`: `trend`, `stress`, `cardio_details`, `sleep_deep_dive`, `strain_deep_dive`, `behavior_impact`, `health_tab` |
| `ops.whoop_private_sync_state` | per-collection watermark, status and error |
| `private.whoop_private_sessions` | the credential |

**The Strain Coach target lives in `documents`, and it is not in strain units.**
`kind = 'strain_deep_dive'` (one row per day) carries WHOOP's recommended strain in its
`SCORE_GAUGE` item as `score_target`, with the optimal band as
`lower_optimal_percentage` / `higher_optimal_percentage`. All three are **gauge
fractions: multiply by 21**. The scale is linear, so `gauge_fill_percentage * 21`
reproduces the displayed strain and is the check that the fields still mean what they
did. Two sibling kinds landed with it: `behavior_impact` (one row per day — WHOOP's own
attribution of yesterday's journal behaviors to today's recovery, which nothing else in
the warehouse can reconstruct) and `health_tab` (one current row under
`doc_key = 'current'` — WHOOP Age, Pace of Aging, Health Monitor statuses). Every day-keyed
kind — those two plus `stress` and `sleep_deep_dive` — is walked backwards to the
account's first cycle, bounded by `WHOOP_PRIVATE_DOCUMENTS_BACKFILL_DAYS_PER_RUN`;
**the documents table is the cursor**, so an interrupted backfill resumes with no
watermark to repair. That budget is set by bytes, not by the rate limit: a recent
`stress` day is ~1.7 MB and `sleep_deep_dive` ~935 KB, against ~5 KB and 326 bytes for
the other two, so lower it (not the kind list) if the pull ever needs to be lighter. Those
are *wire* bytes and they overstate the disk by ~13x: the walk finished 2026-08-24 at the
first cycle (2025-10-23) with 306 days of each kind stored in a 75 MB table.

**The GPS route is `cardio_details`, keyed by workout, and its sweep is cursored by the
same table.** `kind = 'cardio_details'` (one row per `activity_id`) carries the workout's
`map` — `gps_coordinates` as a lat/lng point list (a 1.7 mi run is ~1,200 points), plus
display-string `gps_metrics` — and `base_whoop_private.workouts.gps_data_json` carries only
the distance/elevation summary. Most workouts have no `map` because WHOOP had no phone GPS
for them (measured 2026-08-27: 47 maps across 260 workouts), not because the pull skipped
them. Each run asks for the workouts it just pulled first and then spends what is left of
`WHOOP_PRIVATE_MAX_WORKOUT_REQUESTS` (25) on stored workouts with no document at all,
newest first. Until 2026-08-27 only the run's own newest-25 window was ever asked, so a
workout that landed late — backdated, edited, or restated by a later cycles pull — fell
out of the window and was never fetched; production held three such workouts.

**Heart rate is one series at one grain, and it covers every hour, not just
workouts.** Until 2026-08-26 it was minute-grain all day plus a six-second copy
scoped to workouts, in a second table. The private API in fact serves true
six-second heart rate for **any** window — verified against the live API that day
at one, sixty and two hundred and forty days back — so the workout scoping was
buying nothing but a duplicate of the same readings, and that
workout-scoped table was retired (dropped in place by `ensure_whoop_private_tables`;
`marts_health.workout_heart_rate_samples` is the replacement, and it is a view). It is 14,400 rows a
day and ~5.2M a year, which is the trade that was made deliberately: storage is
recoverable, an unrecorded resolution is not.

**Changing the grain re-walks the history, by itself.**
`ops.whoop_private_sync_state.collection_signature` records what a collection's
rows depend on beyond the window they cover, exactly as `adapter_signature` does
for a timeline adapter. A cursor alone cannot express this: heart rate had
already walked back past the account's first cycle, so it was *finished*, and
resuming it would have left every historic row at the old grain forever. A run
that reads a different signature starts that collection's backfill again from
now — no `force_full_sync`, no operator who has to know. The walk is bounded by
`WHOOP_PRIVATE_HEART_RATE_CHUNKS_PER_RUN` (48 six-hour chunks, twelve days a
run), so a year of history is ~30 runs. The old grain is deleted over exactly
each window the new one has just written, *after* it is written: two grids in one
table double-count every minute whose old timestamp misses the new grid, and
deleting first would leave the series briefly empty.

**The backfill floor is the account's first cycle, not `full_sync_start`.** A
member has no heart rate before they had a WHOOP. Left at the configured floor
the walk spends ten years of runs asking for windows that cannot contain a
reading — production had reached 2025-02-03 for an account whose first cycle is
2025-10-23.

**Only `journal_entries` reaches `timeline.events`** (adapter `whoop_private_journal`,
source `whoop_private`, priority `self` — Zach opened the app and answered the question
himself). The private cycles/sleeps/recoveries/workouts are classified `detail` of the
`base_whoop` row they duplicate: those events are already on the timeline through the four
public adapters, and a second adapter over the private copies would emit a duplicate of
every health event onto a 43M-row table. Read a health *event* from `timeline.events`, then
drill into `base_whoop_private.*` for the resolution the public row does not carry.

### Auth: a captured browser session, not OAuth

MFA is mandatory on this account, so there is no unattended password grant and no login to
implement. The web app's session is captured from ordinary Chrome cookies on `.whoop.com`
(the same Safe Storage keychain machinery the ChatGPT capture uses,
`app/internal/browsersessions/chromium`) and published to the
warehouse, exactly like the ChatGPT session:

- `whoop-auth-token` — the bearer, an AWS Cognito JWT, **24 hours**.
- `whoop-auth-refresh-token` — opaque, **30 days**, and **every refresh returns a new one**.

Persisting the rotation slides the 30-day window forward, so this source is hands-off
indefinitely: unlike ChatGPT's 10-day token, nothing here expires on a fixed clock while
the sync keeps running. The refresh goes to
`POST /auth-service/v2/whoop/refresh` with the **refresh** token in the `Authorization`
header and an **empty body** — sending it as `{"refresh_token": ...}` returns 401, which is
what makes every published recipe fail. Rotations are persisted under the same advisory
lock the public WHOOP credential uses; three production incidents came from treating a
rotating credential casually.

When the refresh window does lapse, `ops.whoop_private_sync_state` goes `action_required`
and `/pipelines` shows it. Repair it from the Mac whose Chrome holds the whoop.com login:

```bash
pdw whoop publish-session          # add --dry-run to verify cookie decryption first
```

### Unit traps

- **`hrv_rmssd` is in SECONDS here.** The public API's `base_whoop.recoveries.hrv_rmssd_milli`
  is milliseconds. Mixing the two is a 1000x error, so the private table stores
  `hrv_rmssd_seconds` *and* a derived milliseconds column rather than one ambiguous name.
- **`during`, `days` and `optimal_sleep_times` are PostgreSQL range notation**
  (`['start','end')`). Parse the bounds; do not cast the string to a timestamp.
- Day boundaries are user-local — take `timezone_offset` from the bootstrap response first.
- The cycle carries `predicted_end` + `data_state`, which is a cleaner in-progress signal
  than the warehouse's epoch sentinel.
- Rate limits are 2,000 per 5 minutes and 144,000 per day (~20x the public API), so a
  backfill is not limit-constrained.
