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

`/progression-service/v3/trends/{VO2_MAX|WEIGHT|BODY_COMPOSITION|STEPS|CALORIES|HRV|STRESS_DURING_SLEEP}`,
`/health-service/v2/stress-bff/{date}`, `/health-tab-bff/v1/health-tab`,
`/home-service/v1/deep-dive/sleep/last-night?date=`,
`/core-details-bff/v1/cardio-details?activityId=` (carries `map`, the GPS route).

`RESTING_HEART_RATE` is not a valid trend name (400); `/core-details-bff/v1/strength-details`
and `/activities-service/v1/blackouts/current` both 404.

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
