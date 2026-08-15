# WHOOP OAuth durability and operations

## Incident and root cause

WHOOP refresh tokens are single-use: a successful refresh rotates both the access token and the
refresh token and invalidates the prior pair. The provider documents that concurrent refreshes can
therefore have one winner and one rejected loser. See the official [WHOOP OAuth
documentation](https://developer.whoop.com/docs/developing/oauth/).

Three production incidents exposed authority, serialization, and failure-semantics bugs in this
integration:

- On 2026-07-30, the 05:55 UTC scheduled sync succeeded and the 06:00 refresh was rejected with
  HTTP 400. Repeated scheduled retries could not recover it.
- On 2026-08-10, scheduled syncs succeeded through 11:55 UTC. The 12:00 run found both the access
  and refresh tokens rejected even though the stored access-token expiry was 12:18 UTC.
- The August 11 repair restored service at 20:50 UTC and kept it healthy for nine hours. At
  2026-08-12 06:00 UTC the first refresh attempt returned HTTP 502, then the same process's next
  five collection attempts received HTTP 400. Dagster retried and still reported the run
  `SUCCESS` with zero records and all six collections `action_required`. At 06:05 another green
  zero-record run replaced the private row with the original env bootstrap, whose access-token
  expiry was already 8 hours 20 minutes old.
- Dagster run history showed no overlapping scheduled WHOOP runs in these failure windows.

The first two failures were not ordinary expiry. The old design had two mutable-looking copies of
a rotating credential (the deployment bootstrap environment value and the private database row),
chose between them by `expires_at`, and protected only the Dagster asset with a Dagster-only
advisory lock. The direct sync CLI did not share that lock.

The August 11 repair added row-locked refresh and stopped choosing by expiry, but it retained one
unsafe inference: after every collection became `action_required`, any env token that differed
from the private row was treated as a reauthorization. The unchanged env value was actually the
original, long-consumed bootstrap. The 06:00 HTTP 502 exposed the provider's unavoidable ambiguous
boundary—a single-use token may be consumed before the replacement response reaches PostgreSQL—
and the 06:05 run then resurrected the stale bootstrap. Treating those no-progress attempts as
successful hid the outage from Dagster.

## Durability invariants

The repaired flow has these invariants:

1. `private.whoop_oauth_tokens` is the sole runtime credential authority. A legacy
   `WHOOP_TOKEN_JSON_B64` can populate an absent row once and can never replace an existing row.
2. Reauthorization is explicit: `personal-data-warehouse-whoop-auth --install` writes the newly
   issued credential directly into the private row. No status or timestamp heuristic interprets
   an env copy as a replacement.
3. First bootstrap, explicit reauthorization, scheduled refresh, and direct CLI refresh all take
   the same PostgreSQL transaction advisory lock. Refresh also locks the account row from the
   provider call through the database update and commit.
4. A racer carrying the pre-rotation token waits, reloads the winning row, and never calls the
   provider with the consumed refresh token. A first-insert race installs exactly one bootstrap.
5. Any failed refresh is terminal within that process, including 5xx/network outcomes whose
   provider result is ambiguous. A fresh Dagster retry may try once more; it either completes the
   rotation or records `action_required`.
6. A no-progress `action_required` attempt fails its Dagster run. Later schedule ticks for the
   unchanged rejected fingerprint are skipped, while `/pipelines` remains in `attention`. A new
   explicitly installed fingerprint resumes automatically.

## Monitoring

The first permanent token-endpoint rejection records `action_required` for all six collections and
fails the run. Later schedule ticks skip the same fingerprint so they do not make hundreds of
redundant provider calls, but the condition remains `attention` on `/pipelines` until a successful
run clears it.

```bash
pdw sql -q "is WHOOP authentication healthy" "
  SELECT pipeline, status, last_write_at, last_run_at, last_error
  FROM marts_ops.pipeline_health
  WHERE pipeline = 'whoop'"
```

An unchanged `action_required` row is an active incident, not a successful or quiet pipeline.

## Cross-host re-authorization

When the terminal and browser are on different Macs, keep the token exchange and deployment work
on the terminal host and forward the registered localhost callback instead of copying an
authorization code through chat:

1. From the terminal host, start a reverse tunnel to the browser host while replacing the host and
   port if needed: `ssh -N -R 8080:127.0.0.1:8080 browser-host`.
2. On the terminal host, load the production `POSTGRES_DATABASE_URL` without printing it, then run
   `uv run personal-data-warehouse-whoop-auth --no-browser --install`.
3. Open the printed authorization URL on the browser host and approve access. Its
   `localhost:8080` redirect crosses the tunnel directly to the waiting terminal-host listener.
4. Do not place the callback URL, authorization code, or emitted token in chat, logs, screenshots,
   or version control.
5. Confirm the command reports that it installed the token into the private warehouse authority.
   Do not update `WHOOP_TOKEN_JSON_B64`; an existing env bootstrap is intentionally ignored.
6. Confirm the next scheduled run sees the new fingerprint and resumes without a deployment
   restart.

Verify without selecting or printing token columns:

```sql
SELECT collection, status, watermark_updated_at, updated_at
FROM ops.whoop_sync_state
ORDER BY collection;

SELECT max(synced_at) AS latest_sync FROM base_whoop.cycles;
SELECT max(synced_at) AS latest_sync FROM base_whoop.recoveries;
SELECT max(synced_at) AS latest_sync FROM base_whoop.sleeps;
SELECT max(synced_at) AS latest_sync FROM base_whoop.workouts;
```

Production verification is complete only after all six state rows are `ok`, real data advances,
several later five-minute cycles succeed, and at least one later cycle crosses an access-token
refresh boundary without returning to `action_required`.
