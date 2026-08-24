# The WHOOP private API (source `whoop_private`)

Reconnaissance performed 2026-08-23 against the live account. Everything here was
verified by request, not taken from third-party write-ups; where a public claim was
tested and turned out wrong, that is called out, because those wrong claims are what
a future reader will otherwise find first.

## Why this source exists

The public developer API (`base_whoop`) is summary-grain: one row per cycle, sleep,
recovery and workout. It has **no time series at all**. The private API — the one
`app.whoop.com` itself calls — adds per-6-second heart rate, the sleep hypnogram,
journal entries, and the trend metrics (VO2 max, weight, body composition, steps)
that have no public endpoint whatsoever.

## Authentication: capture the browser's session, never implement the login

MFA is mandatory on this account, so there is no unattended password grant. The web
app stores its session in ordinary Chrome cookies on `.whoop.com`:

| cookie | meaning |
| --- | --- |
| `whoop-auth-token` | the bearer, an AWS Cognito JWT (us-west-2), **24h** |
| `whoop-auth-refresh-token` | opaque, **30 days** |
| `whoop-auth-expiry` | expiry hint, non-numeric |

They decrypt with the same Safe Storage keychain machinery `chatgpt_cookies.py`
already uses, so capture is a solved problem.

**Refresh — the load-bearing detail:**

```
POST https://api.prod.whoop.com/auth-service/v2/whoop/refresh
Authorization: bearer <REFRESH token, not the access token>
body: {}
→ {access_token, access_token_expires_in: 86400,
   refresh_token, refresh_token_expires_in: 2592000}
```

The refresh token goes in the `Authorization` header and the body is empty. Sending
it as `{"refresh_token": ...}` in the body returns 401, which is what makes this
non-obvious. **Every refresh returns a new refresh token**, so persisting the
rotation slides the 30-day window forward and the source never needs a human again.
The old refresh token still worked immediately after a refresh (so it is not
strictly single-use), but persist rotations under the same advisory lock the public
WHOOP credential uses anyway — three production incidents came from treating a
rotating credential casually.

Dead ends, so nobody re-walks them:

- `POST /api-server/oauth/token` — answers `404 api-server path is disabled`. The
  legacy Angular bundle still calls it; that code is dead. This is why published
  password-grant recipes fail.
- `api-7.whoop.com` — no longer resolves (DNS failure).
- Cognito `InitiateAuth` directly — the app client is configured with a secret, so
  `REFRESH_TOKEN_AUTH` fails with `SECRET_HASH was not received`. Refresh must go
  through WHOOP's own auth-service, which holds that secret.
- `id.whoop.com` — behind a Cloudflare managed challenge (403 "Just a moment").
  `api.prod.whoop.com` is **not**: plain `requests` and Chrome-impersonated TLS
  behave identically there, so no fingerprint impersonation is required.

## Rate limits

`x-ratelimit-limit: 2000, 2000;window=300, 144000;window=86400` — 2,000 per 5
minutes and 144,000 per day, about 20x the public API's 100/min + 10k/day.
Backfill is not limit-constrained.

## Two tiers of endpoint, and why the schema treats them differently

**Tier 1 — data APIs.** Stable, data-shaped, safe to map to typed columns.

| endpoint | returns |
| --- | --- |
| `GET /users-service/v2/bootstrap/?accountType=users` | `user_id` (at `$.profile.user_id`), `timezone_offset`, profile, bio_data, membership. Call first. |
| `GET /core-details-bff/v0/cycles/details?id=&startTime=&endTime=&limit=` | per cycle: `cycle`, `recovery`, `sleeps[]`, `workouts[]`, `v2_activities[]` |
| `GET /sleep-service/v1/sleep-events?activityId=` | the hypnogram: `{during, type}`, types `LIGHT`/`REM`/`SWS`/`DISTURBANCES` |
| `GET /metrics-service/v1/metrics/user/{id}?name=heart_rate&start=&end=&step=&order=t` | `{name, start, values:[{time (ms epoch), data (int)}]}` |
| `GET /activities-service/v1/sports/history?countryCode=US` | 204-sport catalog for `sport_id` resolution |
| `GET /journal-service/v2/journals/behaviors/user/{date}` | the behavior catalog (**not** the day's entries, despite the path) |
| `GET /journal-service/v3/journals/drafts/mobile/{date}` | the day's actual journal entries |
| `GET /users-service/v0/users/preference`, `GET /activities-service/v1/user-state` | settings; live sleep/workout state |

**`metrics-service` accepts only `step` = 6, 60, or 600.** 1, 10, 30 and 300 all
return HTTP 400. Verified: `step=6` yields 600 points/hour at exactly 6.0s spacing,
`step=60` at 60.0s.

**`heart_rate` is the only metric name.** `hrv`, `rmssd`, `spo2`, `skin_temp`,
`respiratory_rate` and `steps` all return 400 at a step that works for heart rate.
There is no continuous HRV series here, contrary to what the iOS write-ups imply —
HRV is per-cycle only, and it is already in the public API.

**Tier 2 — BFF (backend-for-frontend) endpoints.** These return *UI* payloads:
`education_carousel`, `design_items`, `onboarding_overlays`, `header_name_display`.
The data is in there, wrapped in presentation scaffolding that WHOOP can restyle at
any time without notice. Every `progression-service` trend returns an identical
top-level key set regardless of metric, because the top level is a template.

`/progression-service/v3/trends/{VO2_MAX|WEIGHT|BODY_COMPOSITION|STEPS|CALORIES|HRV|STRESS_DURING_SLEEP|WHOOP_AGE|PACE_OF_AGING}`,
`/health-service/v2/stress-bff/{date}`, `/health-tab-bff/v1/health-tab`,
`/home-service/v1/deep-dive/sleep/last-night?date=`,
`/home-service/v1/deep-dive/strain?date=`,
`/behavior-impact-service/v1/impact/summary-card/{date}`,
`/core-details-bff/v1/cardio-details?activityId=` (carries `map`, the GPS route).

`RESTING_HEART_RATE` is not a valid trend name (400); `/core-details-bff/v1/strength-details`
and `/activities-service/v1/blackouts/current` both 404.

### The Strain Coach target, and where it actually lives

`/home-service/v1/deep-dive/strain?date=` is the ONLY carrier of the Strain Coach
target. Twelve plausible names for a dedicated service were probed on 2026-08-23 --
`/coaching-service/*`, `/strain-coach-bff/*`, `/strain-coach-service/*`,
`/activities-service/v1/strain-target`, `/core-details-bff/v{1,2}/strain-coach` -- and
every one answered 404. Do not re-walk them.

Inside, the `SCORE_GAUGE` item carries `score_target`, `lower_optimal_percentage` and
`higher_optimal_percentage`. **They are gauge fractions, not strain units: multiply by
21.** So a `score_target` of 0.58 is a target strain of 12.2, and the optimal band is
roughly the target +/- 2.

Three properties were verified against the live account over ten sampled days, and each
one is worth re-checking if the payload is ever restyled:

- **The scale is linear.** `gauge_fill_percentage * 21` reproduced the displayed strain
  on every day sampled. That identity is the cheapest check that these fields still
  mean what they did.
- **The target rises and falls with recovery**, monotonically but not linearly, across
  the full range sampled.
- **It is stable per cycle, and independent of the strain actually achieved.** Two days
  with equal recovery returned bit-identical targets, and days where the achieved strain
  fell far short of the target did not move it. That is what makes it a fact worth
  storing rather than something recomputable.

### Which trends are worth storing, and which only look like it

The 400 from an invalid `graphKey` enumerates all **36** valid names. Most are not worth
collecting: `DAY_STRAIN`, `RECOVERY`, `HRV`, `RHR`, `RESPIRATORY_RATE`,
`SLEEP_PERFORMANCE`, `SLEEP_EFFICIENCY`, `TIME_IN_BED` and `AVERAGE_HR` are already
per-cycle columns, and `SLEEP_DEBT_POST` / `SLEEP_CONSISTENCY` are literally
`base_whoop_private.sleeps.debt_post` and `.sleep_consistency`. Each trend is ~120 KB of
chart scaffolding around a handful of numbers, so adding them costs megabytes per
snapshot to restate typed data. `WHOOP_AGE` and `PACE_OF_AGING` are the exceptions --
they have no other home, and one call carries the whole series (~6 months). `SAGE` and
`HOURS_OF_SLEEP_GOAL` are advertised as valid but return 500; the three
`EXERCISE_PROGRESS_BY_EXERCISE*` keys return 400.

### Payload sizes set the per-run budget, not what gets stored

Measured per day over the wire, 2026-08-23: `stress` ~1.7 MB, `sleep_deep_dive`
~935 KB, `strain_deep_dive` ~5 KB, `behavior_impact` 326 bytes. All four are backfilled
anyway -- storage is recoverable and an unpulled history is not. What those sizes govern
is the *rate*, not the disk: the per-run day budget is set by bytes rather than by the
rate limit (20 days is ~50 MB of payload; 1,220 requests for a year of all four is
nothing against 2,000 per five minutes), and documents are flushed per day rather than
accumulated so a run never holds a whole batch in memory.

**Sizing the stored table from the wire overestimates it by about 13x.** The finished
walk is the measurement to quote: it completed 2026-08-24, reaching the account's first
cycle on 2025-10-23, and stores 306 days of each day-keyed kind in a **75 MB** table
(`stress` 38 MB, `sleep_deep_dive` 19 MB, `cardio_details` 8.9 MB, `strain_deep_dive`
774 kB, `behavior_impact` 115 kB). These payloads are mostly repeated key names and
presentation scaffolding, so Postgres compresses them hard in TOAST: the 2026-08-23
`stress` document is 1,858,608 JSON characters stored in 144,434 bytes, and that day's
`sleep_deep_dive` is 843,694 characters in 59,351. A pre-walk estimate of "a few hundred
MB" was wrong by an order of magnitude in the cheap direction.

The early days are much smaller than the recent ones: `stress` was 94 KB at the account's
second week and 1.5 MB six months later, because the payload carries history up to its
date. Sizing a backfill from a recent day therefore overestimates it.

Every one of the four answers **200 on any date**, including years before the account
existed (an empty payload, not a 404), so a backfill needs no missing-day handling -- but
it also cannot detect its own floor, which is why the walk stops at the first cycle.

### Confirmed absent

No endpoint exists for the Strength Trainer, the weekly Performance Assessment, blood
pressure, hormonal insights, or WHOOP Coach chat history (four service names probed, all
404). Two unresolved leads: `/sleep-service/v1/sleep-planner` and
`/sleep-service/v1/sleep-need` answer **405** to both GET and POST, so the paths exist
but want something else; `/notification-service/v1/notifications` answers **401**, so it
wants different auth.

**Store Tier 2 as faithful `raw_json` and promote typed columns only where the shape
proves stable.** A typed column over a UI payload is a silent-breakage machine: the
app gets restyled, the key moves, and the column quietly goes null.

## Unit traps

- Private `hrv_rmssd` is in **seconds**; the public API's `hrv_rmssd_milli` is
  milliseconds. Mixing them is a 1000x error.
- `during`, `days` and `optimal_sleep_times` use PostgreSQL range notation
  `['start','end')` — parse them, do not cast.
- Day boundaries are user-local. Take `timezone_offset` from bootstrap first.
- The cycle carries `predicted_end` + `data_state`, a cleaner in-progress signal
  than the warehouse's epoch sentinel.
