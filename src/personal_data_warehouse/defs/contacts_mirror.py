from __future__ import annotations

from datetime import UTC, datetime
import os

from dagster import (
    DefaultScheduleStatus,
    Definitions,
    MaterializeResult,
    MetadataValue,
    asset,
    define_asset_job,
    definitions,
    schedule,
)

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.contacts_mirror import (
    AppProposer,
    google_account,
    load_cards,
    pending_mirror_requests,
    plan_mirror,
    proposal_payloads,
)
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

CONTACTS_MIRROR_POSTGRES_LOCK_ID = 7_403_111_855
# Daily, after both contact syncs have had the night to land, and off the
# on-the-hour schedules.
CONTACTS_MIRROR_CRON = "23 5 * * *"


def app_credentials() -> tuple[str, str]:
    base_url = (os.getenv("PDW_API_URL") or os.getenv("MCP_BASE_URL") or "").strip()
    secret_token = (os.getenv("PDW_SECRET_TOKEN") or os.getenv("MCP_SECRET_TOKEN") or "").strip()
    return base_url, secret_token


def _proposer(base_url: str, secret_token: str) -> AppProposer:
    return AppProposer(base_url=base_url, secret_token=secret_token)


@asset(group_name="warehouse")
def contacts_mirror(context) -> MaterializeResult:
    """Propose the delta that keeps iCloud and Google Contacts one set.

    Google is canonical. The asset never writes a card itself: it computes the
    difference between the two synced books and files it as reviewed mutation
    requests through the app, so every change still passes a human. It stays
    quiet while a previous mirror request is waiting for review, because
    re-proposing the same delta every day is how a queue fills with duplicates.
    """
    base_url, secret_token = app_credentials()
    if not base_url or not secret_token:
        context.log.warning("Skipping contacts mirror: PDW_API_URL / PDW_SECRET_TOKEN are not set")
        return MaterializeResult(metadata={"skipped": MetadataValue.text("no app credentials")})

    settings = load_settings(require_gmail=False)
    warehouse = warehouse_from_settings(settings)
    counts: dict[str, int] = {}
    request_ids: list[str] = []
    try:
        with exclusive_sync_lock(
            name="contacts_mirror", postgres_lock_id=CONTACTS_MIRROR_POSTGRES_LOCK_ID
        ) as acquired:
            if not acquired:
                context.log.warning("Skipping contacts mirror because another run is already active")
                return MaterializeResult(metadata={"skipped": MetadataValue.text("lock held")})
            pending = pending_mirror_requests(warehouse)
            if pending:
                context.log.info("Skipping contacts mirror: %d mirror request(s) still pending review", pending)
                return MaterializeResult(metadata={"skipped": MetadataValue.text("pending review"), "pending_requests": MetadataValue.int(pending)})
            account = google_account()
            google, apple = load_cards(warehouse, google_account_email=account)
            plan = plan_mirror(google, apple, apple_account=account, google_account_email=account)
            counts = plan.counts
            payloads = proposal_payloads(plan, run_date=datetime.now(tz=UTC).date().isoformat())
            if payloads:
                proposer = _proposer(base_url, secret_token)
                for payload in payloads:
                    request_ids.append(proposer.propose(payload))
    finally:
        warehouse.close()

    context.log.info("contacts mirror: %s -> %s", counts, request_ids)
    return MaterializeResult(
        metadata={
            "request_ids": MetadataValue.json(request_ids),
            **{key: MetadataValue.int(value) for key, value in counts.items()},
        }
    )


contacts_mirror_job = define_asset_job("contacts_mirror_job", selection=[contacts_mirror])


@schedule(cron_schedule=CONTACTS_MIRROR_CRON, job=contacts_mirror_job, default_status=DefaultScheduleStatus.RUNNING)
def contacts_mirror_daily(context):
    return skip_if_job_active(context, job_name="contacts_mirror_job")


@definitions
def defs() -> Definitions:
    return Definitions(assets=[contacts_mirror], jobs=[contacts_mirror_job], schedules=[contacts_mirror_daily])
