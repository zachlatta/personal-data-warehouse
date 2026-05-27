from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import timedelta
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

from personal_data_warehouse.calendar_mutations import (
    CALENDAR_PROVIDER,
    CalendarMutationResult,
    CalendarMutationExecutor,
)
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.contact_mutations import (
    GOOGLE_CONTACTS_BATCH_MUTATION_OPERATION,
    GoogleContactMutationExecutor,
)
from personal_data_warehouse.gmail_mutations import (
    GMAIL_ARCHIVE_OPERATION,
    GMAIL_UNARCHIVE_OPERATION,
    GmailMutationExecutor,
    GmailMutationResult,
)
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings


UPSTREAM_MUTATION_WORKER_POSTGRES_LOCK_ID = 7_403_111_843
UPSTREAM_MUTATION_SENSOR_INTERVAL_SECONDS = 10
DEFAULT_UPSTREAM_MUTATION_BATCH_SIZE = 500
DEFAULT_UPSTREAM_MUTATION_RECLAIM_AFTER_SECONDS = 900
GMAIL_THREAD_LABEL_OPERATIONS = {GMAIL_ARCHIVE_OPERATION, GMAIL_UNARCHIVE_OPERATION}
# Operations whose retry semantics make it safe to reclaim a stale `executing` row without
# risking duplicate user-visible side effects. Gmail's batchModify is idempotent for label
# add/remove; anything that creates or sends data (calendar events, emails, contacts) is not.
RECLAIMABLE_IDEMPOTENT_OPERATIONS: tuple[tuple[str, str], ...] = (
    ("gmail", GMAIL_ARCHIVE_OPERATION),
    ("gmail", GMAIL_UNARCHIVE_OPERATION),
)


@dataclass(frozen=True)
class UpstreamMutationWorkerSummary:
    claimed: int = 0
    succeeded: int = 0
    failed_retryable: int = 0
    failed_terminal: int = 0
    blocked_missing_credentials: int = 0
    observed: int = 0
    reclaimed: int = 0
    skipped_due_to_lock: bool = False


@op
def process_upstream_mutations(context) -> dict[str, object]:
    settings = load_settings(require_gmail=False)
    warehouse = warehouse_from_settings(settings)
    batch_size = _upstream_mutation_batch_size()
    reclaim_after = _upstream_mutation_reclaim_after()
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
                calendar_executor=CalendarMutationExecutor(settings=settings),
                limit=batch_size,
                claimed_by=claimed_by,
                reclaim_after=reclaim_after,
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
        if count <= 0:
            reclaimable_count = warehouse.stale_reclaimable_upstream_mutation_count(
                stale_after=_upstream_mutation_reclaim_after(),
                idempotent_operations=RECLAIMABLE_IDEMPOTENT_OPERATIONS,
            )
        else:
            reclaimable_count = 0
    finally:
        warehouse.close()
    if count <= 0:
        if reclaimable_count <= 0:
            return SkipReason("No approved or stale reclaimable upstream mutations found.")
        return RunRequest(
            tags={
                "upstream_mutation_trigger": "stale_executing_reclaim",
                "approved_mutation_count": "0",
                "reclaimable_mutation_count": str(reclaimable_count),
            }
        )

    return RunRequest(tags={"upstream_mutation_trigger": "approved_backlog", "approved_mutation_count": str(count)})


def process_upstream_mutation_batch(
    *,
    warehouse,
    gmail_executor: GmailMutationExecutor,
    contact_executor: GoogleContactMutationExecutor,
    calendar_executor: CalendarMutationExecutor,
    limit: int,
    claimed_by: str,
    reclaim_after: timedelta = timedelta(seconds=DEFAULT_UPSTREAM_MUTATION_RECLAIM_AFTER_SECONDS),
) -> UpstreamMutationWorkerSummary:
    warehouse.ensure_upstream_mutation_tables()
    reclaimed = warehouse.reclaim_stale_executing_mutations(
        stale_after=reclaim_after,
        idempotent_operations=RECLAIMABLE_IDEMPOTENT_OPERATIONS,
        actor_id=claimed_by,
    )
    observed = warehouse.observe_succeeded_gmail_archive_mutations()
    observed += warehouse.observe_succeeded_gmail_unarchive_mutations()
    observed += warehouse.observe_succeeded_gmail_email_mutations()
    observed += warehouse.observe_succeeded_contact_mutations()
    observed += warehouse.observe_succeeded_calendar_event_mutations()
    claimed = warehouse.claim_approved_upstream_mutations(limit=limit, claimed_by=claimed_by)

    succeeded = 0
    failed_retryable = 0
    failed_terminal = 0
    blocked_missing_credentials = 0
    processed_mutation_ids: set[str] = set()
    for group in _gmail_thread_label_mutation_groups(claimed):
        processed, counts = _process_gmail_thread_label_mutation_group(
            warehouse=warehouse,
            gmail_executor=gmail_executor,
            mutations=group,
            claimed_by=claimed_by,
        )
        processed_mutation_ids.update(processed)
        succeeded += counts["succeeded"]
        failed_retryable += counts["failed_retryable"]
        failed_terminal += counts["failed_terminal"]
        blocked_missing_credentials += counts["blocked_missing_credentials"]

    for mutation in claimed:
        if str(mutation["id"]) in processed_mutation_ids:
            continue
        provider = mutation.get("provider")
        if provider == "google_people" and mutation.get("operation") == GOOGLE_CONTACTS_BATCH_MUTATION_OPERATION:
            result = contact_executor.execute(mutation)
        elif provider == CALENDAR_PROVIDER:
            result = calendar_executor.execute(mutation)
        elif provider == "gmail":
            result = gmail_executor.execute(mutation)
        else:
            # Unknown providers should not be burned to failed_terminal by a stale worker
            # version. Leave them claimable for a worker that understands them.
            result = CalendarMutationResult(
                status="failed_retryable",
                result_json={"reason": "unknown provider for this worker version"},
                error=f"unsupported provider {provider!r}; deferring for a newer worker",
            )
        status = _record_mutation_result(
            warehouse=warehouse,
            mutation=mutation,
            result=result,
            claimed_by=claimed_by,
        )
        if status == "succeeded":
            succeeded += 1
        elif status == "failed_retryable":
            failed_retryable += 1
        elif status == "blocked_missing_credentials":
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
        reclaimed=reclaimed,
    )


def _upstream_mutation_batch_size() -> int:
    return int(os.getenv("UPSTREAM_MUTATION_BATCH_SIZE", str(DEFAULT_UPSTREAM_MUTATION_BATCH_SIZE)))


def _upstream_mutation_reclaim_after() -> timedelta:
    return timedelta(
        seconds=int(
            os.getenv(
                "UPSTREAM_MUTATION_RECLAIM_AFTER_SECONDS",
                str(DEFAULT_UPSTREAM_MUTATION_RECLAIM_AFTER_SECONDS),
            )
        )
    )


def _gmail_thread_label_mutation_groups(claimed: list[dict]) -> list[list[dict]]:
    groups: dict[tuple[str, str], list[dict]] = {}
    for mutation in claimed:
        if not _is_gmail_thread_label_mutation(mutation):
            continue
        key = (str(mutation.get("account") or ""), str(mutation.get("operation") or ""))
        groups.setdefault(key, []).append(mutation)
    return list(groups.values())


def _is_gmail_thread_label_mutation(mutation: dict) -> bool:
    return (
        mutation.get("provider") == "gmail"
        and mutation.get("operation") in GMAIL_THREAD_LABEL_OPERATIONS
        and bool(_mutation_thread_ids(mutation))
    )


def _process_gmail_thread_label_mutation_group(
    *,
    warehouse,
    gmail_executor: GmailMutationExecutor,
    mutations: list[dict],
    claimed_by: str,
) -> tuple[set[str], dict[str, int]]:
    processed_mutation_ids: set[str] = set()
    counts = {"succeeded": 0, "failed_retryable": 0, "failed_terminal": 0, "blocked_missing_credentials": 0}
    if not mutations:
        return processed_mutation_ids, counts

    account = str(mutations[0].get("account") or "")
    operation = str(mutations[0].get("operation") or "")
    archive = operation == GMAIL_ARCHIVE_OPERATION
    all_thread_ids = _unique(
        thread_id
        for mutation in mutations
        for thread_id in _mutation_thread_ids(mutation)
    )
    try:
        message_ids_by_thread = warehouse.gmail_message_ids_for_thread_label_mutation(
            account=account,
            thread_ids=all_thread_ids,
            archive=archive,
        )
    except Exception:
        return processed_mutation_ids, counts

    batch_mutations: list[dict] = []
    fallback_mutations: list[dict] = []
    message_ids_by_mutation_id: dict[str, list[str]] = {}
    for mutation in mutations:
        mutation_id = str(mutation["id"])
        message_ids = _unique(
            message_id
            for thread_id in _mutation_thread_ids(mutation)
            for message_id in message_ids_by_thread.get(thread_id, [])
        )
        message_ids_by_mutation_id[mutation_id] = message_ids
        if message_ids:
            batch_mutations.append(mutation)
        else:
            fallback_mutations.append(mutation)

    if batch_mutations:
        batch_message_ids = _unique(
            message_id
            for mutation in batch_mutations
            for message_id in message_ids_by_mutation_id[str(mutation["id"])]
        )
        batch_result = gmail_executor.execute_message_batch_modify(
            account=account,
            operation=operation,
            message_ids=batch_message_ids,
        )
        if batch_result.status == "succeeded":
            for mutation in batch_mutations:
                mutation_id = str(mutation["id"])
                result = GmailMutationResult(
                    status="succeeded",
                    result_json=_gmail_thread_label_batch_result_json(
                        operation=operation,
                        mutation=mutation,
                        message_ids=message_ids_by_mutation_id[mutation_id],
                    ),
                )
                status = _record_mutation_result(
                    warehouse=warehouse,
                    mutation=mutation,
                    result=result,
                    claimed_by=claimed_by,
                )
                counts[status] += 1
                processed_mutation_ids.add(mutation_id)
        elif batch_result.status == "failed_terminal":
            fallback_mutations.extend(batch_mutations)
        else:
            modified_message_ids = set(batch_result.result_json.get("batch_modified_message_ids") or [])
            for mutation in batch_mutations:
                mutation_id = str(mutation["id"])
                result_json = _gmail_thread_label_batch_result_json(
                    operation=operation,
                    mutation=mutation,
                    message_ids=[
                        message_id
                        for message_id in message_ids_by_mutation_id[mutation_id]
                        if message_id in modified_message_ids
                    ],
                )
                result = GmailMutationResult(
                    status=batch_result.status,
                    result_json=result_json,
                    error=batch_result.error,
                )
                status = _record_mutation_result(
                    warehouse=warehouse,
                    mutation=mutation,
                    result=result,
                    claimed_by=claimed_by,
                )
                counts[status] += 1
                processed_mutation_ids.add(mutation_id)

    for mutation in fallback_mutations:
        mutation_id = str(mutation["id"])
        if mutation_id in processed_mutation_ids:
            continue
        result = gmail_executor.execute(mutation)
        status = _record_mutation_result(
            warehouse=warehouse,
            mutation=mutation,
            result=result,
            claimed_by=claimed_by,
        )
        counts[status] += 1
        processed_mutation_ids.add(mutation_id)

    return processed_mutation_ids, counts


def _gmail_thread_label_batch_result_json(
    *,
    operation: str,
    mutation: dict,
    message_ids: list[str],
) -> dict[str, object]:
    thread_ids = _mutation_thread_ids(mutation)
    progress_key = "archived_thread_ids" if operation == GMAIL_ARCHIVE_OPERATION else "unarchived_thread_ids"
    return {
        progress_key: thread_ids,
        "batch_modified_message_ids": message_ids,
    }


def _record_mutation_result(*, warehouse, mutation: dict, result, claimed_by: str) -> str:
    if result.status == "succeeded":
        warehouse.complete_upstream_mutation(
            str(mutation["id"]),
            result_json=result.result_json,
            actor_id=claimed_by,
        )
        return "succeeded"
    warehouse.fail_upstream_mutation(
        str(mutation["id"]),
        status=result.status,
        error=result.error,
        result_json=result.result_json,
        actor_id=claimed_by,
    )
    if result.status == "failed_retryable":
        return "failed_retryable"
    if result.status == "blocked_missing_credentials":
        return "blocked_missing_credentials"
    return "failed_terminal"


def _mutation_thread_ids(mutation: dict) -> list[str]:
    payload = mutation.get("payload_json")
    if not isinstance(payload, dict):
        return []
    thread_ids = payload.get("thread_ids")
    if isinstance(thread_ids, str) or not isinstance(thread_ids, list):
        return []
    return _unique(str(thread_id).strip() for thread_id in thread_ids if str(thread_id).strip())


def _unique(values) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        if value and value not in seen:
            normalized.append(value)
            seen.add(value)
    return normalized


@definitions
def defs() -> Definitions:
    return Definitions(
        jobs=[upstream_mutation_worker_job],
        sensors=[upstream_mutation_sensor],
    )
