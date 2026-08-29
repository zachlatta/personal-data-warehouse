"""Durable-queue wakeups for reviewed upstream mutations.

The rows in ``ops.upstream_mutation_operations`` are the queue.  PostgreSQL
``NOTIFY`` only removes the polling delay; it is deliberately not treated as
durable, so every resident worker drains once at startup and again after each
bounded wait even when no notification arrived.
"""

from __future__ import annotations

from collections.abc import Callable
import select

import psycopg2


MUTATION_NOTIFICATION_CHANNEL = "pdw_upstream_mutations"
DEFAULT_MUTATION_QUEUE_POLL_SECONDS = 30.0


class PostgresMutationNotificationListener:
    def __init__(self, database_url: str) -> None:
        self._connection = psycopg2.connect(database_url)
        self._connection.autocommit = True
        with self._connection.cursor() as cursor:
            cursor.execute(f'LISTEN "{MUTATION_NOTIFICATION_CHANNEL}"')

    def wait(self, timeout_seconds: float) -> bool:
        readable, _, _ = select.select([self._connection], [], [], timeout_seconds)
        if not readable:
            return False
        self._connection.poll()
        notified = bool(self._connection.notifies)
        self._connection.notifies.clear()
        return notified

    def close(self) -> None:
        self._connection.close()


def run_notification_loop(
    *,
    listener_factory: Callable[[], object],
    process_pending: Callable[[], bool],
    stop_requested: Callable[[], bool],
    poll_interval_seconds: float = DEFAULT_MUTATION_QUEUE_POLL_SECONDS,
) -> None:
    """Drain now, then after every notification or fallback poll interval.

    ``process_pending`` returns true when it claimed a full/partial batch.  The
    loop calls it again until the durable queue is empty, which prevents a
    batch-size cap from stranding work until another approval happens.
    """

    listener = listener_factory()
    try:
        while not stop_requested():
            while process_pending():
                if stop_requested():
                    return
            if stop_requested():
                return
            listener.wait(poll_interval_seconds)
    finally:
        listener.close()
