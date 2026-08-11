# WHOOP OAuth durability and operations

## Incident and root cause

WHOOP refresh tokens are single-use: a successful refresh rotates both the access token and the
refresh token and invalidates the prior pair. The provider documents that concurrent refreshes can
therefore have one winner and one rejected loser. See the official [WHOOP OAuth
documentation](https://developer.whoop.com/docs/developing/oauth/).

Two production incidents exposed an authority and serialization bug in this integration:

- On 2026-07-30, the 05:55 UTC scheduled sync succeeded and the 06:00 refresh was rejected with
  HTTP 400. Repeated scheduled retries could not recover it.
- On 2026-08-10, scheduled syncs succeeded through 11:55 UTC. The 12:00 run found both the access
  and refresh tokens rejected even though the stored access-token expiry was 12:18 UTC.
- Dagster run history showed no overlapping WHOOP runs in either failure window.

The failure was not ordinary expiry. The old design had two mutable-looking copies of a rotating
credential (the deployment bootstrap environment value and the private database row), chose
between them by `expires_at`, and protected only the Dagster asset with a Dagster-only advisory
lock. The direct sync CLI did not share that lock. That left refreshes outside Dagster, copied env
credentials, and stale processes able to consume or restore a token without a database-wide
critical section. Production evidence does not identify which outside consumer invalidated the
August token, but the first rejection before expiry and the absence of overlapping Dagster runs
isolate this class of bug. The missing single authority and store-level serialization was the root
cause.

## Durability invariants

The repaired flow has these invariants:

1. `private.whoop_oauth_tokens` is the sole mutable runtime credential authority after bootstrap.
2. `WHOOP_TOKEN_JSON_B64` cannot replace a healthy private row based on timestamps. A changed
   bootstrap is adopted only when all WHOOP collections marked the stored fingerprint
   `action_required`; that is the explicit re-authorization handoff.
3. Every refresh path, including the direct CLI, locks the account's private row before calling
   WHOOP. The row remains locked from the provider call through the database update and commit.
4. A racer carrying the pre-rotation token waits, reloads the winning row, and never calls the
   provider with the consumed refresh token.
5. A rejected, malformed, or interrupted refresh leaves the prior row unchanged and records a
   failing or `action_required` sync state. There is an unavoidable provider boundary: if a process
   is killed after WHOOP accepts a refresh but before PostgreSQL can commit the replacement, manual
   re-authorization is required. The state remains operator-visible rather than silently replacing
   the row with incomplete data.

## Monitoring

The first permanent token-endpoint rejection records `action_required` for all six collections.
Later schedule ticks skip the same fingerprint so they do not make hundreds of redundant provider
calls, but the condition remains `attention` on `/pipelines` until a successful run clears it.

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
2. On the terminal host, run
   `uv run personal-data-warehouse-whoop-auth --no-browser --write-env`.
3. Open the printed authorization URL on the browser host and approve access. Its
   `localhost:8080` redirect crosses the tunnel directly to the waiting terminal-host listener.
4. Do not place the callback URL, authorization code, or emitted token in chat, logs, screenshots,
   or version control.
5. Update the production Dagster `WHOOP_TOKEN_JSON_B64` secret from the terminal host, restart the
   deployment, and confirm the next scheduled run adopts the changed bootstrap.

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
