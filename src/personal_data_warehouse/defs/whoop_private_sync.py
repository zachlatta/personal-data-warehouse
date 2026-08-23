from __future__ import annotations

import os

from dagster import (
    DefaultScheduleStatus,
    Definitions,
    Failure,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    SkipReason,
    asset,
    define_asset_job,
    definitions,
    schedule,
)

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings
from personal_data_warehouse.whoop_private_sync import (
    WhoopPrivateActionRequiredError,
    WhoopPrivateSyncRunner,
    public_whoop_private_sync_summary,
    session_from_row,
    whoop_private_reauthorization_skip_reason,
)

# A new id: sharing one with another pipeline would make two unrelated syncs
# block each other. Uniqueness across every `*_POSTGRES_LOCK_ID` in the package
# is enforced by tests/test_sync_locks.py -- note 8_407_112_476 is already the
# session-authority lock in postgres.py, which this run also takes when it
# persists a rotation.
WHOOP_PRIVATE_SYNC_POSTGRES_LOCK_ID = 8_407_112_477


def whoop_private_schedule_default_status() -> DefaultScheduleStatus:
    enabled = os.getenv("WHOOP_PRIVATE_ENABLED", "1").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }
    # No client id/secret here: the credential is a published browser session,
    # so an account label plus a database is the whole requirement.
    configured = bool(
        (
            os.getenv("WHOOP_PRIVATE_ACCOUNT")
            or os.getenv("WHOOP_ACCOUNT")
            or os.getenv("GMAIL_ACCOUNTS")
        )
        and os.getenv("POSTGRES_DATABASE_URL")
    )
    return DefaultScheduleStatus.RUNNING if enabled and configured else DefaultScheduleStatus.STOPPED


@asset(
    group_name="whoop",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def whoop_private_sync(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_whoop_private=True)
    if settings.whoop_private is not None and not settings.whoop_private.enabled:
        return MaterializeResult(metadata={"skipped": "WHOOP_PRIVATE_ENABLED is false"})
    warehouse = warehouse_from_settings(settings)
    with exclusive_sync_lock(
        name="whoop_private", postgres_lock_id=WHOOP_PRIVATE_SYNC_POSTGRES_LOCK_ID
    ) as acquired:
        if not acquired:
            context.log.warning(
                "Skipping WHOOP private sync because another WHOOP private sync is already running"
            )
            summaries = []
        else:
            try:
                summaries = WhoopPrivateSyncRunner(
                    settings=settings,
                    warehouse=warehouse,
                    logger=context.log,
                ).sync_all()
            except WhoopPrivateActionRequiredError as exc:
                # One red run is actionable; retrying a browser session only a
                # human can re-publish is not. Later ticks are guarded by the
                # recorded action_required fingerprint.
                raise Failure(
                    description=str(exc),
                    metadata={"action_required": True},
                    allow_retries=False,
                ) from exc

    public_summaries = [public_whoop_private_sync_summary(summary) for summary in summaries]
    return MaterializeResult(
        metadata={
            "whoop_private": MetadataValue.json(public_summaries),
            "account_count": len(public_summaries),
            "records_written": sum(summary.records_written for summary in summaries),
            "rate_limited": MetadataValue.bool(any(summary.rate_limited for summary in summaries)),
        }
    )


whoop_private_sync_job = define_asset_job(
    "whoop_private_sync_job",
    selection=[whoop_private_sync],
)


@schedule(
    cron_schedule="*/15 * * * *",
    job=whoop_private_sync_job,
    default_status=whoop_private_schedule_default_status(),
)
def whoop_private_sync_every_fifteen_minutes(context):
    active = skip_if_job_active(context, job_name="whoop_private_sync_job")
    if isinstance(active, SkipReason):
        return active

    # Every private-API collection shares one browser session. Once that exact
    # session has been rejected, another run every fifteen minutes can only
    # repeat the same doomed refresh. A newly published session has a different
    # fingerprint and clears this guard on the very next tick.
    try:
        settings = load_settings(require_gmail=False, require_whoop_private=True)
        if settings.whoop_private is None:
            return active
        warehouse = warehouse_from_settings(settings)
        try:
            state = warehouse.load_whoop_private_sync_state()
            session = session_from_row(
                warehouse.load_whoop_private_session(account=settings.whoop_private.account),
                account=settings.whoop_private.account,
            )
        finally:
            warehouse.close()
        if session is None:
            return active
        reason = whoop_private_reauthorization_skip_reason(
            state,
            account=settings.whoop_private.account,
            refresh_token=session.refresh_token,
        )
    except Exception as exc:
        # A fresh schema may not have these tables until the first asset run
        # ensures them. Let that run proceed; the asset stays the fail-loud
        # authority for unexpected configuration/database errors.
        context.log.warning("WHOOP private credential guard unavailable: %s", exc)
        return active
    return active if reason is None else SkipReason(reason)


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[whoop_private_sync],
        jobs=[whoop_private_sync_job],
        schedules=[whoop_private_sync_every_fifteen_minutes],
    )
