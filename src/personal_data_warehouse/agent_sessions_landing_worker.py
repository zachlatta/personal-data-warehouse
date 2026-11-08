"""Resident lander for agent sessions: a posted batch to timeline rows in seconds.

Measured 2026-11-08 over seven days, agent turns landed p50 3-4 min after they
happened, and the server's share was a 60 s inbox sensor, a Dagster run start
and a fast lane that spent 14 of its 15 s compiling JIT code. The app NOTIFYs
``pdw_agent_sessions_landing`` when it stores an agent-sessions batch; this
worker, supervised in the Dagster container beside the voice-memo worker,
wakes on it and runs the asset's own ingest (one path, two triggers) and then
the timeline fast lane. The Drive inbox is the durable queue and the sensor
stays the floor, so a missed notification costs latency, never a turn.
"""

from __future__ import annotations

import logging
import os
import signal
import threading

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.defs.agent_sessions_drive_ingest import (
    agent_sessions_inbox_has_batches,
    ingest_agent_sessions_inbox,
    land_agent_sessions_on_timeline,
)
from personal_data_warehouse.mutation_notifications import (
    PostgresMutationNotificationListener,
    run_notification_loop,
)

logger = logging.getLogger(__name__)

AGENT_SESSIONS_LANDING_CHANNEL = "pdw_agent_sessions_landing"
# The fallback poll; the Dagster sensor already polls every 60 s.
DEFAULT_AGENT_SESSIONS_LANDING_POLL_SECONDS = 300.0
DEFAULT_AGENT_SESSIONS_LANDING_RETRY_SECONDS = 10.0


class AgentSessionsLandingProcessor:
    def __init__(self) -> None:
        # The same settings the asset and its sensor load.
        self.settings = load_settings(require_gmail=False, require_agent_sessions=True)

    @property
    def database_url(self) -> str:
        value = self.settings.postgres_database_url
        if not value:
            raise RuntimeError("POSTGRES_DATABASE_URL is required by the agent sessions landing worker")
        return value

    def process_pending(self) -> bool:
        """One pass. True when it landed a batch, since another may have arrived behind it."""
        # One Drive list, so an idle wake costs no ingest run.
        if not agent_sessions_inbox_has_batches(self.settings):
            return False
        summary = ingest_agent_sessions_inbox(settings=self.settings, logger=logger)
        fast_lane = land_agent_sessions_on_timeline(settings=self.settings, summary=summary, logger=logger)
        logger.info(
            "agent sessions landing: %d batch(es), %d event(s), %d timeline row(s)",
            summary.batches_seen,
            summary.events_written,
            int(fast_lane.get("rows", 0) or 0),
        )
        return bool(summary.batches_seen)


def run_resident_worker(stop_event: threading.Event) -> None:
    poll_seconds = float(
        os.getenv("AGENT_SESSIONS_LANDING_POLL_SECONDS", str(DEFAULT_AGENT_SESSIONS_LANDING_POLL_SECONDS))
    )
    retry_seconds = float(
        os.getenv("AGENT_SESSIONS_LANDING_RETRY_SECONDS", str(DEFAULT_AGENT_SESSIONS_LANDING_RETRY_SECONDS))
    )
    while not stop_event.is_set():
        try:
            processor = AgentSessionsLandingProcessor()
        except ValueError as error:
            logger.warning("agent sessions are not configured (%s); landing worker idle", error)
            stop_event.wait(poll_seconds)
            continue
        try:
            run_notification_loop(
                listener_factory=lambda: PostgresMutationNotificationListener(
                    processor.database_url, channel=AGENT_SESSIONS_LANDING_CHANNEL
                ),
                process_pending=processor.process_pending,
                stop_requested=stop_event.is_set,
                poll_interval_seconds=poll_seconds,
            )
        except Exception:  # noqa: BLE001 - a resident worker must reconnect after DB/Drive failures
            logger.exception("agent sessions landing worker failed; reconnecting")
            stop_event.wait(retry_seconds)


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    stop_event = threading.Event()

    def stop(_signum, _frame) -> None:
        stop_event.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    logger.info("starting agent sessions landing worker")
    run_resident_worker(stop_event)


if __name__ == "__main__":
    main()
