# Plan 005: Add a CI gate that runs the Python suite (with Postgres) and the full Go suite before merge

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md`.
>
> **Drift check (run first)**:
> `git diff --stat e118893..HEAD -- .github/workflows tests/conftest.py`
> If the CI workflows or conftest changed since this plan was written, compare
> the "Current state" facts against the live repo before proceeding.

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: LOW (adds CI only; touches no product code)
- **Depends on**: none — but it is the *verification baseline* that lets plans
  001–004 be checked automatically on future PRs.
- **Category**: dx
- **Planned at**: commit `e118893`, 2026-07-06

## Why this matters

Production auto-deploys from pushes to `origin/main` (both the Dagster pipeline
and the Go app on Coolify). Yet **no CI runs the Python test suite at all**, and
the only Go tests that run are gated to CLI-related path changes — so edits to the
Go server/ingest/mutations packages, and *every* Python change, reach production
with zero automated verification. On top of that, **137 of ~938 Python tests
silently skip** when `POSTGRES_DATABASE_URL` is unset (the entire warehouse SQL
layer — `postgres.py`, the timeline, mutations). A developer or CI without a
Postgres sees green while the exact SQL layer that defines the product never ran.

This plan adds one CI workflow that (a) runs `uv run pytest` against a Postgres
**service container** so those 137 tests actually execute, and (b) runs
`go test ./...` for the whole app on any `app/**` change. It converts "tests
exist but never gate" into a real pre-merge gate — and makes plans 001–004's new
tests meaningful in CI.

## Current state

- `.github/workflows/` contains only `pdw-cli-release.yml` and
  `postgres-pgbackrest-image.yml`. Neither runs `uv run pytest`.
- `pdw-cli-release.yml` runs `go test ./...` (`working-directory: app`) but only
  under a `paths:` filter limited to `app/cmd/pdw-cli/**`, `cliclient`,
  `selfupdate`, `tool`, `api`, `auth`, `go.mod/sum` — so `app/internal/server/**`
  and `app/internal/mutations/**` changes trigger no test run.
- `tests/conftest.py:44-49` — the session fixture loads `.env` and only does work
  when `POSTGRES_DATABASE_URL` is set; warehouse tests call
  `pytest.skip("POSTGRES_DATABASE_URL is not set")` when it's absent (8 test files
  do this, ~137 tests).
- Test commands (verified): Python → `uv run pytest` at repo root (uses `uv`,
  see `README.md` "Verification"); Go → `go test ./...` with
  `working-directory: app`.
- The project uses `uv` (`pyproject.toml`), Python `>=3.10,<3.15`, and pins
  `dagster==1.12.22`. The `neonize` dependency ships a Go shared library and the
  WhatsApp code path uses `libmagic`; on Ubuntu CI install `libmagic1`.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Validate workflow YAML | `uv run python -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml'))"` | no error |
| Go tests (local) | `cd app && go test ./...` | all pass (or report failures) |
| Python collect | `uv run pytest --collect-only -q` | collects (~900+ tests), exit 0 |
| Python tests w/ PG (local) | see Step 3 | pass, incl. previously-skipped warehouse tests |

## Scope

**In scope**:
- `.github/workflows/ci.yml` (new file)

**Out of scope** (do NOT touch):
- Existing workflows `pdw-cli-release.yml`, `postgres-pgbackrest-image.yml`.
- **Product code and tests** — this plan adds CI only. If the suite surfaces
  failing tests, that is a *discovery* to report, not a license to edit product
  code (see STOP conditions).
- Lint/format/typecheck tooling (`ruff`/`mypy`/`golangci-lint`) — a separate,
  larger follow-up; do not add it here.
- Dependabot / vuln scanning — separate follow-up.

## Git workflow

- Branch: `advisor/005-ci-gate`
- One commit. Message: `Add CI running the Python and Go test suites`.
- Do NOT push or open a PR unless the operator instructed it. (Note: the workflow
  only truly runs once pushed to GitHub; local verification is the gate here.)

## Steps

### Step 1: Write the CI workflow

Create `.github/workflows/ci.yml`:

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:

concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true

jobs:
  python-tests:
    name: Python tests (pytest + Postgres)
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_USER: postgres
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: pdw_test
        ports:
          - 5432:5432
        options: >-
          --health-cmd "pg_isready -U postgres"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    env:
      POSTGRES_DATABASE_URL: postgresql://postgres:postgres@localhost:5432/pdw_test
    steps:
      - uses: actions/checkout@v4
      - name: Install system libs
        run: sudo apt-get update && sudo apt-get install -y libmagic1
      - name: Install uv
        uses: astral-sh/setup-uv@v5
      - name: Sync dependencies
        run: uv sync --dev
      - name: Run pytest
        run: uv run pytest -q

  go-tests:
    name: Go tests
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version-file: app/go.mod
      - name: go test
        working-directory: app
        run: go test ./...
```

Notes for the executor:
- Do not add a `paths:` filter — the whole point is that any change is gated.
- `POSTGRES_DATABASE_URL` is a throwaway service-container URL; it is **not** a
  secret and must not reference any real credential.
- Pin action major versions as shown; if `astral-sh/setup-uv@v5` or
  `actions/setup-go@v5` isn't resolvable in this repo's environment, use the
  latest available major and note it.

**Verify**: `uv run python -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))"` → no error.

### Step 2: Confirm the Go suite is green locally

```bash
cd app && go test ./...
```

**Verify**: exit 0, all packages `ok`. If any package fails, this is a
**pre-existing** Go test failure — record the failing package(s) and STOP (see
STOP conditions); do not modify Go code.

### Step 3: Confirm the Python suite is green locally against Postgres

Bring up a local Postgres (Docker), export the URL, and run the suite:

```bash
docker run -d --rm --name pdw-ci-pg -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=pdw_test -p 5432:5432 postgres:16
export POSTGRES_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/pdw_test
uv sync --dev
uv run pytest -q
docker stop pdw-ci-pg
```

Confirm the previously-skipped warehouse tests now **run** (grep the output for
`test_postgres_warehouse` results rather than `skipped`).

**Verify**: `uv run pytest -q` exits 0, and the run reports far fewer `skipped`
than a no-Postgres run (the ~137 warehouse tests execute). If tests **fail**,
distinguish: a failure in code this plan didn't touch is a **pre-existing**
failure → STOP and report (do not fix product code).

### Step 4: Record what the gate covers

The workflow file's comments/name make coverage self-evident; no separate doc
change is required for this plan. (Updating README/AGENTS.md to state the
Postgres requirement is a related docs follow-up — out of scope here.)

## Test plan

- No new unit tests — this plan *is* the test-execution infrastructure.
- Verification is: the workflow YAML parses; `go test ./...` passes locally;
  `uv run pytest` passes locally with a Postgres service and executes the
  previously-skipped warehouse tests.

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `.github/workflows/ci.yml` exists and parses (`yaml.safe_load` succeeds)
- [ ] The workflow has both a `python-tests` job with a `postgres` service and
      `POSTGRES_DATABASE_URL` env, and a `go-tests` job with no `paths:` filter
- [ ] `cd app && go test ./...` exits 0 locally
- [ ] `uv run pytest -q` exits 0 locally with `POSTGRES_DATABASE_URL` set, and the
      warehouse tests ran (not skipped)
- [ ] Only `.github/workflows/ci.yml` added; no product code or other workflow
      changed (`git diff --name-only`)
- [ ] `plans/README.md` status row for 005 updated

## STOP conditions

Stop and report back (do not improvise) if:

- `go test ./...` or `uv run pytest` (with Postgres) reveals **pre-existing**
  failures in code this plan did not touch. Report the exact failing
  tests/packages. Do **not** edit product code to make them pass — the operator
  decides whether to fix-then-gate or land the gate as non-blocking first. This
  is the single most likely outcome and is a valuable finding, not a failure of
  the plan.
- `uv sync` fails on a missing system library other than `libmagic1` — report the
  missing lib; add the corresponding `apt-get install` line only if it's an
  obvious system dependency of a declared package.
- The Postgres tests need extensions or settings the plain `postgres:16` image
  lacks (e.g. a failure mentioning a missing extension) — report which; the
  service image or an init step may need adjusting.
- You cannot run Docker/Postgres locally at all — deliver Steps 1–2, and report
  that Step 3 (Python-with-Postgres) could not be verified locally; do not mark
  done.

## Maintenance notes

- This gate only *runs* the tests; it does not add lint/typecheck. Natural
  follow-ups (separate plans): `ruff check` + `ruff format --check`,
  `go vet ./...`, a Dependabot config, and `govulncheck`/`pip-audit`.
- If CI time becomes a problem, the Python job can shard by test path, but keep
  the warehouse tests in at least one shard — skipping them silently is the exact
  regression this plan closes.
- A reviewer should confirm the `go-tests` job has **no** `paths:` filter (so
  server/mutations changes are covered) and that `POSTGRES_DATABASE_URL` in CI is
  the throwaway service URL, never a real secret.
- Landing this may turn the board red if pre-existing failures exist. If so, the
  operator's options are: fix them first, or merge the workflow with the failing
  job temporarily `continue-on-error: true` and burn the failures down. Do not
  make that call unilaterally.
