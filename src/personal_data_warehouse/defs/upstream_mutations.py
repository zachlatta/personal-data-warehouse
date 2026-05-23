from __future__ import annotations

from dataclasses import asdict, dataclass
import os
import socket

from dagster import (
    DefaultSensorStatus,
    Definitions,
    RunRequest,
    SkipReason,
    definitions,
    job,
    op,
    sensor,
)

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.contact_mutations import (
    GOOGLE_CONTACTS_BATCH_MUTATION_OPERATION,
    GoogleContactMutationExecutor,
)
from personal_data_warehouse.gmail_mutations import GmailMutationExecutor
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings


UPSTREAM_MUTATION_WORKER_POSTGRES_LOCK_ID = 7_403_111_843
UPSTREAM_MUTATION_SENSOR_INTERVAL_SECONDS = 10
DEFAULT_UPSTREAM_MUTATION_BATCH_SIZE = 10


@dataclass(frozen=True)
class UpstreamMutationWorkerSummary:
    claimed: int = 0
    succeeded: int = 0
    failed_retryable: int = 0
    failed_terminal: int = 0
    blocked_missing_credentials: int = 0
    observed: int = 0
    skipped_due_to_lock: bool = False


@op
def process_upstream_mutations(context) -> dict[str, object]:
    settings = load_settings(require_gmail=False)
    warehouse = warehouse_from_settings(settings)
    batch_size = int(os.getenv("UPSTREAM_MUTATION_BATCH_SIZE", str(DEFAULT_UPSTREAM_MUTATION_BATCH_SIZE)))
    claimed_by = f"dagster:{socket.gethostname()}:upstream_mutation_worker"

    try:
        with exclusive_sync_lock(
            name="upstream_mutation_worker",
            postgres_lock_id=UPSTREAM_MUTATION_WORKER_POSTGRES_LOCK_ID,
        ) as acquired:
            if not acquired:
                context.log.warning("Skipping upstream mutation worker because another run is active")
                return asdict(UpstreamMutationWorkerSummary(skipped_due_to_lock=True))
            summary = process_upstream_mutation_batch(
                warehouse=warehouse,
                gmail_executor=GmailMutationExecutor(settings=settings),
                contact_executor=GoogleContactMutationExecutor(settings=settings),
                limit=batch_size,
                claimed_by=claimed_by,
            )
    finally:
        warehouse.close()

    context.log.info("Processed upstream mutations: %s", summary)
    return asdict(summary)


@job
def upstream_mutation_worker_job():
    process_upstream_mutations()


@sensor(
    job=upstream_mutation_worker_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=UPSTREAM_MUTATION_SENSOR_INTERVAL_SECONDS,
)
def upstream_mutation_sensor(context):
    active = skip_if_job_in_progress(context, job_name="upstream_mutation_worker_job")
    if isinstance(active, SkipReason):
        return active

    settings = load_settings(require_gmail=False)
    warehouse = warehouse_from_settings(settings)
    try:
        count = warehouse.approved_upstream_mutation_count()
    finally:
        warehouse.close()
    if count <= 0:
        return SkipReason("No approved upstream mutations found.")

    return RunRequest(tags={"upstream_mutation_trigger": "approved_backlog", "approved_mutation_count": str(count)})


def process_upstream_mutation_batch(
    *,
    warehouse,
    gmail_executor: GmailMutationExecutor,
    contact_executor: GoogleContactMutationExecutor,
    limit: int,
    claimed_by: str,
) -> UpstreamMutationWorkerSummary:
    warehouse.ensure_upstream_mutation_tables()
    observed = warehouse.observe_succeeded_gmail_archive_mutations()
    observed += warehouse.observe_succeeded_gmail_unarchive_mutations()
    observed += warehouse.observe_succeeded_gmail_email_mutations()
    observed += warehouse.observe_succeeded_contact_mutations()
    claimed = warehouse.claim_approved_upstream_mutations(limit=limit, claimed_by=claimed_by)

    succeeded = 0
    failed_retryable = 0
    failed_terminal = 0
    blocked_missing_credentials = 0
    for mutation in claimed:
        if mutation.get("provider") == "google_people" and mutation.get("operation") == GOOGLE_CONTACTS_BATCH_MUTATION_OPERATION:
            result = contact_executor.execute(mutation)
        else:
            result = gmail_executor.execute(mutation)
        if result.status == "succeeded":
            warehouse.complete_upstream_mutation(
                str(mutation["id"]),
                result_json=result.result_json,
                actor_id=claimed_by,
            )
            succeeded += 1
        else:
            warehouse.fail_upstream_mutation(
                str(mutation["id"]),
                status=result.status,
                error=result.error,
                result_json=result.result_json,
                actor_id=claimed_by,
            )
            if result.status == "failed_retryable":
                failed_retryable += 1
            elif result.status == "blocked_missing_credentials":
                blocked_missing_credentials += 1
            else:
                failed_terminal += 1

    return UpstreamMutationWorkerSummary(
        claimed=len(claimed),
        succeeded=succeeded,
        failed_retryable=failed_retryable,
        failed_terminal=failed_terminal,
        blocked_missing_credentials=blocked_missing_credentials,
        observed=observed,
    )


@definitions
def defs() -> Definitions:
    return Definitions(
        jobs=[upstream_mutation_worker_job],
        sensors=[upstream_mutation_sensor],
    )
