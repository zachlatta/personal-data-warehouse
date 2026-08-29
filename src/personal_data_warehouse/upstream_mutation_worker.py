"""Resident low-latency executor for approved cloud mutations."""

from __future__ import annotations

import logging
import os
import signal
import socket
import threading

from personal_data_warehouse.calendar_mutations import CalendarMutationExecutor
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.contact_mutations import GoogleContactMutationExecutor
from personal_data_warehouse.defs.upstream_mutations import (
    UPSTREAM_MUTATION_WORKER_POSTGRES_LOCK_ID,
    _upstream_mutation_batch_size,
    _upstream_mutation_reclaim_after,
    process_upstream_mutation_batch,
)
from personal_data_warehouse.gmail_mutations import GmailMutationExecutor
from personal_data_warehouse.mutation_notifications import (
    DEFAULT_MUTATION_QUEUE_POLL_SECONDS,
    PostgresMutationNotificationListener,
    run_notification_loop,
)
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings


logger = logging.getLogger(__name__)
DEFAULT_RESIDENT_WORKER_RETRY_SECONDS = 10.0


class CloudMutationProcessor:
    def __init__(self) -> None:
        self.settings = load_settings(require_gmail=False)
        self.warehouse = warehouse_from_settings(self.settings)
        self.warehouse.ensure_upstream_mutation_tables()
        self.gmail_executor = GmailMutationExecutor(settings=self.settings)
        self.contact_executor = GoogleContactMutationExecutor(settings=self.settings)
        self.calendar_executor = CalendarMutationExecutor(settings=self.settings)
        self.claimed_by = f"resident:{socket.gethostname()}:upstream_mutation_worker"

    @property
    def database_url(self) -> str:
        value = self.settings.postgres_database_url
        if not value:
            raise RuntimeError("POSTGRES_DATABASE_URL is required by the resident mutation worker")
        return value

    def process_pending(self) -> bool:
        with exclusive_sync_lock(
            name="upstream_mutation_worker",
            postgres_lock_id=UPSTREAM_MUTATION_WORKER_POSTGRES_LOCK_ID,
        ) as acquired:
            if not acquired:
                logger.info("mutation execution lock is held by the Dagster fallback worker")
                return False
            summary = process_upstream_mutation_batch(
                warehouse=self.warehouse,
                gmail_executor=self.gmail_executor,
                contact_executor=self.contact_executor,
                calendar_executor=self.calendar_executor,
                limit=_upstream_mutation_batch_size(),
                claimed_by=self.claimed_by,
                reclaim_after=_upstream_mutation_reclaim_after(),
                ensure_tables=False,
            )
        if summary.claimed or summary.reclaimed:
            logger.info("processed upstream mutations: %s", summary)
        return summary.claimed > 0

    def close(self) -> None:
        self.warehouse.close()


def run_resident_worker(stop_event: threading.Event) -> None:
    poll_seconds = float(
        os.getenv("UPSTREAM_MUTATION_RESIDENT_POLL_SECONDS", str(DEFAULT_MUTATION_QUEUE_POLL_SECONDS))
    )
    retry_seconds = float(
        os.getenv("UPSTREAM_MUTATION_RESIDENT_RETRY_SECONDS", str(DEFAULT_RESIDENT_WORKER_RETRY_SECONDS))
    )
    while not stop_event.is_set():
        processor: CloudMutationProcessor | None = None
        try:
            processor = CloudMutationProcessor()
            run_notification_loop(
                listener_factory=lambda: PostgresMutationNotificationListener(processor.database_url),
                process_pending=processor.process_pending,
                stop_requested=stop_event.is_set,
                poll_interval_seconds=poll_seconds,
            )
        except Exception:  # noqa: BLE001 - a resident worker must reconnect after DB/provider failures
            logger.exception("resident mutation worker failed; reconnecting")
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
    logger.info("starting resident mutation worker (batch_size=%d)", _upstream_mutation_batch_size())
    run_resident_worker(stop_event)


if __name__ == "__main__":
    main()
