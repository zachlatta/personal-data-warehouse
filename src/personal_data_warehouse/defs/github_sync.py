"""Poll GitHub for the account's commits, issues, PRs, reviews and comments.

Same shape as the Hacker News poller: a sensor launches the job every
``GITHUB_POLL_INTERVAL_SECONDS`` (15 min), the run works under a request and
time budget and resumes where it stopped. A rejected token is
``action_required`` on ``/pipelines`` rather than a red run, because the other
credentials (and the thread walk) are still doing useful work.
"""

from __future__ import annotations

import os
import time

from dagster import (
    DefaultSensorStatus,
    Definitions,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    RunRequest,
    SkipReason,
    asset,
    define_asset_job,
    definitions,
    sensor,
)

from personal_data_warehouse.config import GitHubConfig, load_settings
from personal_data_warehouse.github_api import GitHubClient, GitHubCredential, RequestBudget
from personal_data_warehouse.github_sync import GitHubSyncRunner, public_summary
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

# Unique across every *_POSTGRES_LOCK_ID (tests/test_sync_locks.py).
GITHUB_SYNC_POSTGRES_LOCK_ID = 8_407_112_490
GITHUB_SENSOR_TICK_SECONDS = 60
#: Well under the global four-hour run-monitoring cap: a run is budgeted at
#: GITHUB_MAX_RUN_SECONDS (10 min) and must never hold a queue slot for hours.
GITHUB_JOB_MAX_RUNTIME_SECONDS = 1800


def github_configured() -> bool:
    return bool(os.getenv("GITHUB_ACCOUNT") and os.getenv("POSTGRES_DATABASE_URL"))


def github_credentials(config: GitHubConfig) -> list[GitHubCredential]:
    """Every token, else the read-only proxy, else anonymous."""
    if config.tokens:
        return [GitHubCredential.token(t, base_url=config.api_base_url) for t in config.tokens]
    if config.proxy_base_url and config.proxy_api_key:
        return [GitHubCredential.proxy(config.proxy_api_key, base_url=config.proxy_base_url)]
    return [GitHubCredential.anonymous(base_url=config.api_base_url)]


@asset(
    group_name="github",
    retry_policy=RetryPolicy(max_retries=2, delay=60),
)
def github_sync(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_github=True)
    config = settings.github
    if config is None:
        raise RuntimeError("GitHub sync is not configured")
    if not config.enabled:
        return MaterializeResult(metadata={"skipped": MetadataValue.text("GITHUB_ENABLED=0")})

    with exclusive_sync_lock(name="github_sync", postgres_lock_id=GITHUB_SYNC_POSTGRES_LOCK_ID) as acquired:
        if not acquired:
            context.log.warning("Skipping GitHub sync because another run is already active")
            return MaterializeResult(metadata={"skipped": MetadataValue.text("another run active")})

        warehouse = warehouse_from_settings(settings)
        try:
            budget = RequestBudget(max_requests=config.max_requests_per_run, max_seconds=config.max_run_seconds)
            clients = [
                GitHubClient(credential, budget=budget, timeout=config.request_timeout_seconds)
                for credential in github_credentials(config)
            ]
            summary = GitHubSyncRunner(
                warehouse=warehouse,
                clients=clients,
                account=config.account,
                budget=budget,
                logger=context.log,
            ).sync()
        finally:
            warehouse.close()

    return MaterializeResult(
        metadata={
            "github": MetadataValue.json(public_summary(summary)),
            "issues": MetadataValue.int(summary.issues),
            "commits": MetadataValue.int(summary.commits),
            "comments": MetadataValue.int(summary.comments),
            "threads_remaining": MetadataValue.int(summary.threads_remaining),
            "requests": MetadataValue.int(summary.requests),
            "budget_exhausted": MetadataValue.bool(summary.budget_exhausted),
        }
    )


github_sync_job = define_asset_job(
    "github_sync_job",
    selection=[github_sync],
    tags={"dagster/max_runtime": str(GITHUB_JOB_MAX_RUNTIME_SECONDS)},
)


@sensor(
    job=github_sync_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=GITHUB_SENSOR_TICK_SECONDS,
)
def github_sync_sensor(context):
    active = skip_if_job_in_progress(context, job_name="github_sync_job")
    if isinstance(active, SkipReason):
        return active
    if not github_configured():
        return SkipReason("GitHub is not configured: set GITHUB_ACCOUNT (the GitHub login).")
    try:
        settings = load_settings(require_gmail=False, require_github=True)
    except ValueError as exc:
        return SkipReason(f"GitHub is not configured: {exc}")
    config = settings.github
    if config is None or not config.enabled:
        return SkipReason("GitHub sync is disabled by GITHUB_ENABLED=0.")

    now = time.time()
    last_run = float(context.cursor) if context.cursor else 0.0
    if now - last_run < config.poll_interval_seconds:
        return SkipReason(f"Waiting for poll interval ({config.poll_interval_seconds}s) since last GitHub poll.")
    context.update_cursor(str(now))
    return RunRequest(tags={"github_trigger": "poll"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[github_sync],
        jobs=[github_sync_job],
        sensors=[github_sync_sensor],
    )
