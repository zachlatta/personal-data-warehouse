"""Apply approved Apple Notes mutations on this Mac.

The cloud mutation worker deliberately skips provider ``apple_notes`` -- Notes has no
server API, so approved rows would sit in ``ops.upstream_mutation_operations`` forever
without something running where Notes.app runs. This module is that something. It rides
the existing apple-notes LaunchAgent rather than adding a second scheduler, so the note
is written and then uploaded back into the warehouse by the very next stage of the same
run, which is also what makes the round trip observable.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import os
import socket

from personal_data_warehouse.apple_notes_mutations import (
    APPLE_NOTES_PROVIDER,
    AppleNotesMutationExecutor,
)
from personal_data_warehouse.sync_locks import exclusive_sync_lock


# Distinct from the cloud worker's lock id: the two queues are disjoint by provider, so
# sharing a lock would make a long cloud batch block Notes writes for no reason.
APPLE_NOTES_MUTATION_LOCK_ID = 7_403_111_851
DEFAULT_BATCH_SIZE = 25
# Notes writes are serialized through one app on one Mac, so a stuck claim should return
# to the queue quickly; there is no second worker that might still be mid-flight.
DEFAULT_RECLAIM_AFTER_SECONDS = 600


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
        )
        claimed = warehouse.claim_approved_upstream_mutations(
            limit=limit,
            claimed_by=claimed_by,
            providers=(APPLE_NOTES_PROVIDER,),
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
