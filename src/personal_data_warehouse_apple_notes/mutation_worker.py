"""Apply approved Apple Notes mutations on this Mac.

The cloud mutation worker deliberately skips provider ``apple_notes`` -- Notes has no
server API, so approved rows would sit in ``ops.upstream_mutation_operations`` forever
without something running where Notes.app runs. This module is that something. A resident
LaunchAgent listens for approval notifications, while the existing five-minute uploader
calls the same processor as a fallback before it scans Notes into the warehouse.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import logging
import os
import signal
import socket
import threading

from personal_data_warehouse.apple_notes_mutations import (
    APPLE_NOTES_PROVIDER,
    AppleNotesMutationExecutor,
)
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.mutation_notifications import (
    DEFAULT_MUTATION_QUEUE_POLL_SECONDS,
    PostgresMutationNotificationListener,
    run_notification_loop,
)


# Distinct from the cloud worker's lock id: the two queues are disjoint by provider, so
# sharing a lock would make a long cloud batch block Notes writes for no reason.
APPLE_NOTES_MUTATION_LOCK_ID = 7_403_111_851
DEFAULT_BATCH_SIZE = 25
# Notes writes are serialized through one app on one Mac, so a stuck claim should return
# to the queue quickly; there is no second worker that might still be mid-flight.
DEFAULT_RECLAIM_AFTER_SECONDS = 600
DEFAULT_RESIDENT_RETRY_SECONDS = 10.0
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AppleNotesMutationSummary:
    claimed: int = 0
    succeeded: int = 0
    failed_retryable: int = 0
    failed_terminal: int = 0
    blocked_missing_credentials: int = 0
    skipped_due_to_lock: bool = False

    def describe(self) -> str:
        if self.skipped_due_to_lock:
            return "Apple Notes mutations skipped: another worker holds the lock"
        return (
            "Apple Notes mutations: "
            f"claimed={self.claimed} "
            f"succeeded={self.succeeded} "
            f"retryable={self.failed_retryable} "
            f"terminal={self.failed_terminal} "
            f"blocked={self.blocked_missing_credentials}"
        )


def process_apple_notes_mutations(
    *,
    warehouse,
    executor: AppleNotesMutationExecutor | None = None,
    limit: int = DEFAULT_BATCH_SIZE,
    claimed_by: str | None = None,
    reclaim_after: timedelta | None = None,
    ensure_tables: bool = True,
) -> AppleNotesMutationSummary:
    executor = executor or AppleNotesMutationExecutor()
    claimed_by = claimed_by or f"mac:{socket.gethostname()}:apple_notes_mutation_worker"
    reclaim_after = reclaim_after or timedelta(seconds=DEFAULT_RECLAIM_AFTER_SECONDS)

    with exclusive_sync_lock(
        name="apple_notes_mutation_worker",
        postgres_lock_id=APPLE_NOTES_MUTATION_LOCK_ID,
    ) as acquired:
        if not acquired:
            return AppleNotesMutationSummary(skipped_due_to_lock=True)

        if ensure_tables:
            warehouse.ensure_upstream_mutation_tables()
        # A create is NOT idempotent -- replaying it makes a second note -- so only the
        # update path is reclaimed. A stale create is left executing for a human to look
        # at rather than silently duplicated.
        warehouse.reclaim_stale_executing_mutations(
            stale_after=reclaim_after,
            idempotent_operations=(
                (APPLE_NOTES_PROVIDER, "apple_notes.update_note"),
            ),
            actor_id=claimed_by,
            ensure_tables=False,
        )
        claimed = warehouse.claim_approved_upstream_mutations(
            limit=limit,
            claimed_by=claimed_by,
            providers=(APPLE_NOTES_PROVIDER,),
            ensure_tables=False,
        )

        counts = {
            "succeeded": 0,
            "failed_retryable": 0,
            "failed_terminal": 0,
            "blocked_missing_credentials": 0,
        }
        for mutation in claimed:
            result = executor.execute(mutation)
            if result.status == "succeeded":
                warehouse.complete_upstream_mutation(
                    str(mutation["id"]),
                    result_json=result.result_json,
                    actor_id=claimed_by,
                )
                counts["succeeded"] += 1
                continue
            warehouse.fail_upstream_mutation(
                str(mutation["id"]),
                status=result.status,
                error=result.error,
                result_json=result.result_json,
                actor_id=claimed_by,
            )
            counts[result.status] = counts.get(result.status, 0) + 1

    return AppleNotesMutationSummary(
        claimed=len(claimed),
        succeeded=counts["succeeded"],
        failed_retryable=counts["failed_retryable"],
        failed_terminal=counts["failed_terminal"],
        blocked_missing_credentials=counts["blocked_missing_credentials"],
    )


def apple_notes_mutations_enabled() -> bool:
    return os.getenv("APPLE_NOTES_MUTATIONS_ENABLED", "1").strip() not in {"0", "false", "no"}


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


class AppleNotesResidentProcessor:
    def __init__(self) -> None:
        from personal_data_warehouse.config import load_settings
        from personal_data_warehouse.warehouse import warehouse_from_settings

        self.settings = load_settings(require_postgres=True, require_gmail=False)
        self.warehouse = warehouse_from_settings(self.settings)
        self.warehouse.ensure_upstream_mutation_tables()
        self.executor = AppleNotesMutationExecutor()
        self.claimed_by = f"mac-resident:{socket.gethostname()}:apple_notes_mutation_worker"

    @property
    def database_url(self) -> str:
        value = self.settings.postgres_database_url
        if not value:
            raise RuntimeError("POSTGRES_DATABASE_URL is required by the Apple Notes mutation worker")
        return value

    def process_pending(self) -> bool:
        summary = process_apple_notes_mutations(
            warehouse=self.warehouse,
            executor=self.executor,
            limit=int(os.getenv("APPLE_NOTES_MUTATION_BATCH_SIZE", str(DEFAULT_BATCH_SIZE))),
            claimed_by=self.claimed_by,
            ensure_tables=False,
        )
        if summary.claimed:
            logger.info(summary.describe())
        return summary.claimed > 0

    def close(self) -> None:
        self.warehouse.close()


def run_resident_apple_notes_worker(stop_event: threading.Event) -> None:
    poll_seconds = float(
        os.getenv("APPLE_NOTES_MUTATION_POLL_SECONDS", str(DEFAULT_MUTATION_QUEUE_POLL_SECONDS))
    )
    retry_seconds = float(os.getenv("APPLE_NOTES_MUTATION_RETRY_SECONDS", str(DEFAULT_RESIDENT_RETRY_SECONDS)))
    while not stop_event.is_set():
        processor: AppleNotesResidentProcessor | None = None
        try:
            processor = AppleNotesResidentProcessor()
            run_notification_loop(
                listener_factory=lambda: PostgresMutationNotificationListener(processor.database_url),
                process_pending=processor.process_pending,
                stop_requested=stop_event.is_set,
                poll_interval_seconds=poll_seconds,
            )
        except Exception:  # noqa: BLE001 - reconnect instead of losing the resident worker
            logger.exception("Apple Notes resident mutation worker failed; reconnecting")
            stop_event.wait(retry_seconds)
        finally:
            if processor is not None:
                processor.close()


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    stop_event = threading.Event()

    def stop(_signum, _frame) -> None:
        stop_event.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    logger.info("starting resident Apple Notes mutation worker")
    run_resident_apple_notes_worker(stop_event)


if __name__ == "__main__":
    main()
