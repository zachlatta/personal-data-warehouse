# Health (WHOOP)

WHOOP arrives twice. `base_whoop.*` is the public developer API — summary grain, one row
per cycle, sleep, recovery and workout, **no time series at all**. `base_whoop_private.*`
is the app's own API, with everything the public one lacks: six-second heart rate, the
sleep hypnogram, the journal, sports, and the trend/coach documents. Neither alone is
right: the private source misses rows the public one has, and the public one has no
resolution. **Start at `marts_health`**, which conforms them, LEFT-joined from the public
row outward, with `has_private_detail` saying whether the richer row existed.

```sql
SELECT start_at, strain, sleep_need_seconds, has_private_detail
FROM marts_health.cycles ORDER BY start_at DESC LIMIT 7;
```

## The unit traps (1000x errors, all of them)

- **HRV:** `base_whoop_private.recoveries.hrv_rmssd_seconds` is SECONDS where the public
  `base_whoop.recoveries.hrv_rmssd_milli` is milliseconds. The mart publishes one column,
  `hrv_rmssd_milli`.
- **Durations:** public sleep-stage totals are milliseconds (`*_milli`), private ones
  seconds; the mart exposes only `*_seconds`. But in `marts_health.sleeps` the columns
  `habitual_sleep_need_seconds`, `need_from_strain_seconds`, `sleep_debt_pre_seconds`,
  `sleep_debt_post_seconds` and `sleep_latency_seconds` pass **milliseconds** through
  under a `_seconds` name — divide those five by 1000. Verify against the raw row when a
  sleep number looks like 7,669 hours.
- **Strain:** the displayed 0–21 score is `base_whoop.cycles.strain` /
  `base_whoop_private.cycles.scaled_strain`. In the private table `day_strain` is the raw
  unscaled value (max ~0.024) and `intensity_score` is 0 on every row — either reads as
  zeros. The Strain Coach target and its optimal band, in
  `base_whoop_private.documents` kind `strain_deep_dive`, are **gauge fractions:
  multiply by 21**.
- **Sleep performance** is slept ÷ (baseline need + need from strain) and excludes
  accrued sleep debt; including the debt gives a lower number than the app shows.

## Cycles are days, but not calendar days

A cycle runs sleep-onset to next sleep-onset, so it reports the day it is **awake** for:
an onset at 11:12 PM Friday that ends 12:07 AM Sunday is the Saturday cycle. Twelve
hours past local onset lands in the right day. The in-progress cycle stores
`end_at = 1970-01-01T00:00Z` (the warehouse absence sentinel, not NULL), so
`ORDER BY end_at DESC` ranks it oldest — bound on `start_at`. `marts_health.cycles.end_at`
is NULL for it.

## The private tables

| relation | what it holds |
| --- | --- |
| `base_whoop_private.heart_rate_samples` | one series at one grain: a reading every six seconds, every hour of every day (~14,400 rows/day) |
| `marts_health.workout_heart_rate_samples` | the same series joined to each workout's bounds, keyed `workout_id`, with `elapsed_seconds` |
| `base_whoop_private.sleep_events` | the hypnogram: one row per LIGHT / REM / SWS / WAKE / LATENCY / DISTURBANCES stage |
| `base_whoop_private.journal_entries` | the journal answers — the only private table on the timeline (`whoop_private`, priority `self`) |
| `base_whoop_private.cycles`, `.sleeps`, `.recoveries`, `.workouts` | high-resolution copies: strain components, sleep debt, HRV/RHR components, zone durations, GPS summary |
| `base_whoop_private.sports` | the sport catalog resolving a workout's `sport_id` |
| `base_whoop_private.documents` | raw `jsonb` UI payloads keyed `(kind, doc_key)`: `trend` (VO2 max, weight, steps), `stress`, `cardio_details` (a workout's GPS route under `map`), `sleep_deep_dive`, `strain_deep_dive`, `behavior_impact`, `health_tab` (`doc_key = 'current'`: WHOOP Age, Pace of Aging) |

```sql
-- the day's heart rate
SELECT sample_at, heart_rate FROM base_whoop_private.heart_rate_samples
WHERE sample_at >= now() - interval '1 day' ORDER BY sample_at;

-- the Strain Coach target, in strain units
SELECT d.doc_key::date AS day,
       round((i->'content'->>'score_target')::numeric * 21, 2) AS target,
       i->'content'->>'score_display' AS achieved
FROM base_whoop_private.documents d
CROSS JOIN LATERAL jsonb_array_elements(d.raw_json->'sections') s
CROSS JOIN LATERAL jsonb_array_elements(s->'items') i
WHERE d.kind = 'strain_deep_dive' AND i->>'type' = 'SCORE_GAUGE'
ORDER BY day DESC LIMIT 7;
```

`during`, `days` and `optimal_sleep_times` are Postgres range notation — read the bounds.
Day boundaries are user-local. Health telemetry is `noise` on the timeline; only the
journal is `self`.

## When it stops

`whoop` and `whoop_private` are separate pipelines in `marts_ops.pipeline_health`, so one
dying is never hidden by the other. `action_required` on either is a credential that a
person must repair{{if .CLI}} (`pdw whoop publish-session` for the private source, the OAuth flow for the public one; topic `ingest`){{end}}, not a transient error.
