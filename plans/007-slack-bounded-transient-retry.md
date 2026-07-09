# Plan 007: Cap Slack transient-error retries so a stuck call fails loud instead of hanging forever

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**:
> `git diff --stat 768f164..HEAD -- src/personal_data_warehouse/slack_sync.py tests/test_slack_sync.py`
> If either changed since this plan was written, compare the "Current state"
> excerpts against the live code before proceeding; on a mismatch, treat it as a
> STOP condition.

## Status

- **Priority**: P1
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: bug
- **Planned at**: commit `768f164`, 2026-07-07

## Why this matters

`SlackSyncRunner._call` wraps every Slack API call. Its rate-limit branch has a budget (`_max_rate_limit_sleep_seconds`) and raises `SlackRateLimitBudgetExceeded` when exceeded — but its **transient-error branch has no cap at all**: it is an unbounded `while True` that sleeps up to 60s and retries forever. `SlackTransientError` is raised for `(SlackRequestError, TimeoutError, OSError)` — i.e. DNS failures, connection resets, and network timeouts, not just momentary blips.

The failure mode: during a Slack or network outage, a scheduled Slack sync run enters this loop and never returns. Because the run holds the shared Slack advisory lock (`SLACK_SYNC_POSTGRES_LOCK_ID`) for its whole lifetime, and the schedule guard only treats a run as stale after ~10 minutes, every subsequent scheduled Slack tick then skips on the lock. A single stuck call becomes an indefinite, silent Slack-sync outage that reports "in progress" forever rather than surfacing a failure Dagster can retry or alert on.

Every other API client in this repo caps transient retries — e.g. Gmail's `execute_gmail_request` uses `max_attempts=5` and re-raises. This plan brings `_call`'s transient branch in line: bound the attempts, then re-raise `SlackTransientError` so the run fails loudly and Dagster's retry policy (already `max_retries` on the Slack assets) takes over.

## Current state

`SlackSyncRunner._call` — `src/personal_data_warehouse/slack_sync.py:1385-1407`:

```python
    def _call(self, client, method: str, **params) -> dict[str, Any]:
        transient_attempts = 0
        while True:
            try:
                return client.call(method, **params)
            except SlackRateLimitedError as exc:
                if (
                    self._max_rate_limit_sleep_seconds is not None
                    and self._rate_limit_sleep_seconds + exc.retry_after > self._max_rate_limit_sleep_seconds
                ):
                    raise SlackRateLimitBudgetExceeded(
                        "Slack API rate limit budget exceeded "
                        f"after {self._rate_limit_sleep_seconds}s of sleeps; "
                        f"next {method} retry requested {exc.retry_after}s"
                    ) from exc
                self._rate_limit_sleep_seconds += exc.retry_after
                self._logger.warning("Slack rate limited %s for %ss", method, exc.retry_after)
                self._sleep(exc.retry_after)
            except SlackTransientError as exc:
                transient_attempts += 1
                sleep_seconds = min(60, 5 * transient_attempts)
                self._logger.warning("Slack transient failure %s; retrying in %ss: %s", method, sleep_seconds, exc)
                self._sleep(sleep_seconds)   # <-- loops forever; no cap, no re-raise
```

`SlackTransientError` is defined at `slack_sync.py:49` and raised in the client wrapper at `slack_sync.py:80-81`:

```python
        except (SlackRequestError, TimeoutError, OSError) as exc:
            raise SlackTransientError(f"{method} transient request failure: {exc}") from exc
```

The runner already has the plumbing this fix mirrors — `SlackSyncRunner.__init__` (`slack_sync.py:91-159`):
- `self._sleep = sleep or time.sleep` (injectable — tests pass a fake `sleep`).
- `self._max_rate_limit_sleep_seconds = max_rate_limit_sleep_seconds` and `self._rate_limit_sleep_seconds = 0` (`slack_sync.py:158-159`) — the exact budget pattern to copy for a transient cap.

The reference convention for a bounded transient retry is `execute_gmail_request` in `src/personal_data_warehouse/gmail_sync.py:1924-1938` (`max_attempts: int = 5`, re-raises on the final attempt).

Existing tests live in `tests/test_slack_sync.py`, which already imports `SlackTransientError` and uses a fake client. Read it before Step 2 to match its fixture style.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Slack sync tests | `uv run pytest tests/test_slack_sync.py -q` | all pass |
| Full suite (sanity) | `uv run pytest -q` | no new failures vs. baseline (831 passed, 113 skipped) |
| Import check | `uv run python -c "import personal_data_warehouse.slack_sync"` | no error |

## Scope

**In scope** (the only files you should modify):
- `src/personal_data_warehouse/slack_sync.py` — add a bounded transient-attempt cap to `_call` and a constructor knob for it.
- `tests/test_slack_sync.py` — add a test that the transient branch re-raises after the cap and that a recovered call still succeeds.

**Out of scope** (do NOT touch):
- The rate-limit branch of `_call` — it already has a budget; leave it exactly as is.
- The Slack schedules / advisory-lock wiring (`defs/slack_sync.py`) — the lock behavior is correct; this plan makes the stuck call terminate so the lock is released.
- `SlackTransientError`'s definition and where it is raised (`slack_sync.py:80-81`) — unchanged.
- Any other API client's retry logic.

## Git workflow

- Branch: `advisor/007-slack-bounded-transient-retry`
- One logical commit. Suggested message (imperative, matches repo style):
  `Cap Slack transient-error retries instead of retrying forever`
- Do NOT push or open a PR unless the operator instructed it.

## Steps

### Step 1: Add a bounded transient-attempt cap to `_call`

In `src/personal_data_warehouse/slack_sync.py`:

1. Add a constructor parameter. In `SlackSyncRunner.__init__`, alongside `max_rate_limit_sleep_seconds: int | None = None` in the signature (`slack_sync.py:124`), add:

   ```python
       max_transient_attempts: int = 8,
   ```

   and store it next to the rate-limit budget assignment (after `slack_sync.py:158-159`):

   ```python
           self._max_transient_attempts = max_transient_attempts
   ```

   (8 attempts with the existing `min(60, 5 * n)` backoff is ~5+10+15+20+25+30+35 ≈ 2.3 minutes of retrying before giving up — comfortably longer than a momentary blip, comfortably shorter than the ~10-minute stale-run window that causes lock starvation.)

2. In `_call`, change the transient branch to increment, and once it exceeds the cap, re-raise so the run fails loudly:

   ```python
               except SlackTransientError as exc:
                   transient_attempts += 1
                   if transient_attempts >= self._max_transient_attempts:
                       self._logger.warning(
                           "Slack transient failure %s exceeded %s attempts; giving up: %s",
                           method, self._max_transient_attempts, exc,
                       )
                       raise
                   sleep_seconds = min(60, 5 * transient_attempts)
                   self._logger.warning(
                       "Slack transient failure %s; retrying in %ss (attempt %s/%s): %s",
                       method, sleep_seconds, transient_attempts, self._max_transient_attempts, exc,
                   )
                   self._sleep(sleep_seconds)
   ```

Leave the rate-limit branch untouched.

**Verify**: `uv run python -c "import personal_data_warehouse.slack_sync"` → no error.

### Step 2: Test the cap

In `tests/test_slack_sync.py`, add two tests (match the file's existing fixture/fake-client style — read it first):

1. **Re-raises after the cap.** Build a `SlackSyncRunner` with a small `max_transient_attempts` (e.g. 3), a fake `sleep` that records calls (so the test does not actually wait), and a fake client whose `call` always raises `SlackTransientError`. Assert `runner._call(client, "some.method")` raises `SlackTransientError`, and that the fake `sleep` was called exactly `max_transient_attempts - 1` times (it sleeps between attempts, not after the final give-up).

2. **Recovers before the cap.** Fake client that raises `SlackTransientError` on the first call and returns a valid payload on the second. Assert `_call` returns the payload and `sleep` was called once.

If the runner requires many constructor args, use the existing test helper/factory in `tests/test_slack_sync.py` that other tests use to build a runner (do not hand-roll a fragile constructor call — reuse the file's pattern).

**Verify**: `uv run pytest tests/test_slack_sync.py -q` → all pass, including the two new tests.

### Step 3: Full-suite sanity

**Verify**: `uv run pytest -q` → no new failures vs. baseline (831 passed, 113 skipped; the count of passed may rise by the two new tests).

## Test plan

- New tests in `tests/test_slack_sync.py`:
  - Transient branch re-raises `SlackTransientError` after `max_transient_attempts`, sleeping `cap - 1` times (uses an injected fake `sleep`, so no real waiting).
  - A transient failure followed by success returns normally after one sleep.
- Use the existing runner-construction pattern in `tests/test_slack_sync.py` as the structural model.
- Verification: `uv run pytest tests/test_slack_sync.py -q` → all pass including 2 new tests.

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `grep -n 'max_transient_attempts' src/personal_data_warehouse/slack_sync.py` shows the constructor param, the stored attribute, and its use in `_call`
- [ ] `_call`'s transient branch contains a `raise` on cap exhaustion (`grep -n 'exceeded .* attempts' src/personal_data_warehouse/slack_sync.py` matches, or inspect the branch)
- [ ] `uv run pytest tests/test_slack_sync.py -q` passes, including the 2 new tests
- [ ] `uv run pytest -q` shows no new failures
- [ ] Only the two in-scope files were modified (`git diff --name-only`)
- [ ] `plans/README.md` status row for 007 updated

## STOP conditions

Stop and report back (do not improvise) if:

- `_call` no longer matches the "Current state" excerpt (e.g. it already caps transient attempts) — the fix may be partly done.
- The runner constructor signature has changed such that adding a keyword-only default arg would break existing callers positionally — report the current signature.
- Any existing test relied on `_call` retrying transient errors more than 8 times — report it rather than loosening the cap silently.

## Maintenance notes

- The default cap (8) and the existing `min(60, 5 * n)` backoff give ~2 minutes of retry. If Slack outages are observed to last longer and a longer local retry is genuinely wanted, raise `max_transient_attempts` (or wire it through the CLI/asset the way `max_rate_limit_sleep_seconds` is threaded) — but keep it well under the stale-run window so a stuck call still releases the shared Slack lock before the next tick is starved.
- A reviewer should confirm the re-raise propagates out of `_call` to the asset so Dagster's `RetryPolicy` on the Slack assets handles it (a surfaced failure is the goal, not a swallowed one).
- Related but separate: the eight Slack stages share one advisory lock (a distinct performance finding). This plan only stops a stuck call from holding that lock indefinitely; it does not change the single-lock design.
