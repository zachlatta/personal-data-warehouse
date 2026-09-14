"""Poll Hacker News for the account's items, lists and the discussions under them.

The credential story mirrors the ChatGPT poller: the login-only lists read a
browser cookie published by ``pdw hn publish-session``; a login page marks
that exact cookie rejected so the private lists sit out until a different one
is published, while the public lists and the item walk keep running every
tick. A run never goes red for a dead cookie -- ``/pipelines`` carries the
``action_required`` state -- because the public half is still doing useful
work and a red run would hide a real failure behind a known one.
"""

from __future__ import annotations

from datetime import timedelta
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

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.hacker_news_api import HackerNewsClient
from personal_data_warehouse.hacker_news_sync import (
    HackerNewsSyncRunner,
    credential_sha256,
    public_summary,
)
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

# Unique across every *_POSTGRES_LOCK_ID (tests/test_sync_locks.py).
HACKER_NEWS_SYNC_POSTGRES_LOCK_ID = 8_407_112_485
HACKER_NEWS_SENSOR_TICK_SECONDS = 60


def hacker_news_configured() -> bool:
    return bool(os.getenv("HACKER_NEWS_ACCOUNT") and os.getenv("POSTGRES_DATABASE_URL"))


@asset(
    group_name="hacker_news",
    retry_policy=RetryPolicy(max_retries=2, delay=60),
)
def hacker_news_sync(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_hacker_news=True)
    config = settings.hacker_news
    if config is None:
        raise RuntimeError("Hacker News sync is not configured")
    if not config.enabled:
        return MaterializeResult(metadata={"skipped": MetadataValue.text("HACKER_NEWS_ENABLED=0")})

    with exclusive_sync_lock(name="hacker_news_sync", postgres_lock_id=HACKER_NEWS_SYNC_POSTGRES_LOCK_ID) as acquired:
        if not acquired:
            context.log.warning("Skipping Hacker News sync because another run is already active")
            return MaterializeResult(metadata={"skipped": MetadataValue.text("another run active")})

        warehouse = warehouse_from_settings(settings)
        try:
            session_row = warehouse.get_hacker_news_session(account=config.account, session_key=config.session_key) or {}
            cookie = str(session_row.get("session_token") or "")
            rejected = str(session_row.get("expired_token_sha256") or "")
            token_sha = str(session_row.get("token_sha256") or "")
            if cookie and rejected and rejected == token_sha:
                # Known-dead cookie: run public-only rather than hammer the login page.
                context.log.warning("Hacker News session is marked rejected; running the public lists only")
                cookie = ""
            client = HackerNewsClient(
                session_cookie=cookie,
                api_base_url=config.api_base_url,
                site_base_url=config.site_base_url,
                timeout=config.request_timeout_seconds,
            )
            summary = HackerNewsSyncRunner(
                warehouse=warehouse,
                client=client,
                account=config.account,
                session_key=config.session_key,
                session_token_sha256=credential_sha256(cookie) if cookie else "",
                max_item_fetches=config.max_item_fetches_per_run,
                max_list_pages=config.max_list_pages_per_run,
                full_walk_interval=timedelta(seconds=config.full_walk_interval_seconds),
                live_window=timedelta(days=config.live_window_days),
                refresh_min_age=timedelta(hours=config.refresh_min_age_hours),
                logger=context.log,
            ).sync()
        finally:
            warehouse.close()

    public = public_summary(summary)
    return MaterializeResult(
        metadata={
            "hacker_news": MetadataValue.json(public),
            "items_fetched": MetadataValue.int(summary.items_fetched),
            "items_refreshed": MetadataValue.int(summary.items_refreshed),
            "frontier_remaining": MetadataValue.int(summary.frontier_remaining),
            "lists_failed": MetadataValue.int(len(summary.lists_failed)),
            "session_rejected": MetadataValue.bool(summary.session_rejected),
            "rate_limited": MetadataValue.bool(summary.rate_limited),
            "budget_exhausted": MetadataValue.bool(summary.budget_exhausted),
        }
    )


hacker_news_sync_job = define_asset_job("hacker_news_sync_job", selection=[hacker_news_sync])


@sensor(
    job=hacker_news_sync_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=HACKER_NEWS_SENSOR_TICK_SECONDS,
)
def hacker_news_sync_sensor(context):
    active = skip_if_job_in_progress(context, job_name="hacker_news_sync_job")
    if isinstance(active, SkipReason):
        return active
    if not hacker_news_configured():
        return SkipReason("Hacker News is not configured: set HACKER_NEWS_ACCOUNT (the HN username).")
    try:
        settings = load_settings(require_gmail=False, require_hacker_news=True)
    except ValueError as exc:
        return SkipReason(f"Hacker News is not configured: {exc}")
    config = settings.hacker_news
    if config is None or not config.enabled:
        return SkipReason("Hacker News sync is disabled by HACKER_NEWS_ENABLED=0.")

    now = time.time()
    last_run = float(context.cursor) if context.cursor else 0.0
    if now - last_run < config.poll_interval_seconds:
        return SkipReason(f"Waiting for poll interval ({config.poll_interval_seconds}s) since last Hacker News poll.")
    context.update_cursor(str(now))
    return RunRequest(tags={"hacker_news_trigger": "poll"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[hacker_news_sync],
        jobs=[hacker_news_sync_job],
        sensors=[hacker_news_sync_sensor],
    )
