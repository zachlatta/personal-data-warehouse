"""Resident lander for voice memos: inbox to transcript to timeline in seconds.

Measured 2026-10-16 over 30 days of Apple Voice Memos, end of recording to
transcript was p50 9 min and almost none of it was work: a 60 s inbox sensor
and a Dagster run start, another 60 s transcription sensor and run start, then
the five-minute ``timeline_sync``. AssemblyAI itself took 7 s on a short memo.

The app NOTIFYs ``pdw_voice_memo_landing`` when it stores a voice-memo sidecar.
This worker, supervised in the Dagster container beside the mutation worker,
wakes on it and runs the same ingest and transcription functions the Dagster
assets run (one path, two triggers), then lands the recordings on the timeline
through the fast lane. The Drive inbox and the untranscribed-recordings query
are the durable queue; the notification only removes the wait, and the
sensors stay as the floor, so a missed notification costs latency, never a
recording. Enrichment is minutes of agent time and stays on its own sensor.
"""

from __future__ import annotations

from collections.abc import Callable
import logging
import os
import signal
import threading
from typing import Any

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.defs.apple_voice_memos_drive_ingest import (
    ingest_voice_memos_inbox,
    voice_memos_inbox_has_metadata,
)
from personal_data_warehouse.defs.apple_voice_memos_transcription import (
    transcribe_voice_recordings,
    voice_memos_transcription_batch_size,
    voice_recordings_await_transcription,
)
from personal_data_warehouse.mutation_notifications import (
    PostgresMutationNotificationListener,
    run_notification_loop,
)
from personal_data_warehouse.timeline_fast_lane import land_sources_on_timeline


logger = logging.getLogger(__name__)

VOICE_MEMO_LANDING_CHANNEL = "pdw_voice_memo_landing"
# The fallback poll. The Dagster sensors already poll every 60 s, so this only
# needs to catch work the worker was woken for while it was busy.
DEFAULT_VOICE_MEMO_LANDING_POLL_SECONDS = 300.0
DEFAULT_VOICE_MEMO_LANDING_RETRY_SECONDS = 10.0


def land_pending_voice_memos(
    *,
    ingest: Callable[[], int],
    transcribe: Callable[[], int],
    land_on_timeline: Callable[[], Any],
    logger: Any,
) -> bool:
    """One pass: inbox to warehouse, transcribe, then land on the timeline.

    Returns true when transcription did work, since a batch may have left more
    behind it; the notification loop then calls again until the queue is empty.
    """
    ingested = ingest()
    transcribed = transcribe()
    if ingested or transcribed:
        land_on_timeline()
        logger.info("voice memo landing: %d ingested, %d transcribed", ingested, transcribed)
    return bool(transcribed)


class VoiceMemoLandingProcessor:
    def __init__(self) -> None:
        self.settings = load_settings(require_gmail=False)

    @property
    def database_url(self) -> str:
        value = self.settings.postgres_database_url
        if not value:
            raise RuntimeError("POSTGRES_DATABASE_URL is required by the voice memo landing worker")
        return value

    def _ingest(self) -> int:
        # Checked first so an idle wake costs one Drive list, not a run.
        if not voice_memos_inbox_has_metadata(self.settings):
            return 0
        summary = ingest_voice_memos_inbox(settings=self.settings, logger=logger)
        return summary.rows_written if summary is not None else 0

    def _transcribe(self) -> int:
        if self.settings.voice_memos is None or self.settings.assemblyai is None:
            return 0
        if not voice_recordings_await_transcription(self.settings):
            return 0
        summary = transcribe_voice_recordings(
            settings=self.settings,
            logger=logger,
            limit=voice_memos_transcription_batch_size(),
        )
        return summary.recordings_transcribed if summary is not None else 0

    def _land_on_timeline(self) -> Any:
        return land_sources_on_timeline(
            postgres_url=self.settings.postgres_database_url or "",
            sources=["voice_memos"],
            logger=logger,
        )

    def process_pending(self) -> bool:
        return land_pending_voice_memos(
            ingest=self._ingest,
            transcribe=self._transcribe,
            land_on_timeline=self._land_on_timeline,
            logger=logger,
        )


def run_resident_worker(stop_event: threading.Event) -> None:
    poll_seconds = float(
        os.getenv("VOICE_MEMO_LANDING_POLL_SECONDS", str(DEFAULT_VOICE_MEMO_LANDING_POLL_SECONDS))
    )
    retry_seconds = float(
        os.getenv("VOICE_MEMO_LANDING_RETRY_SECONDS", str(DEFAULT_VOICE_MEMO_LANDING_RETRY_SECONDS))
    )
    while not stop_event.is_set():
        try:
            processor = VoiceMemoLandingProcessor()
            run_notification_loop(
                listener_factory=lambda: PostgresMutationNotificationListener(
                    processor.database_url, channel=VOICE_MEMO_LANDING_CHANNEL
                ),
                process_pending=processor.process_pending,
                stop_requested=stop_event.is_set,
                poll_interval_seconds=poll_seconds,
            )
        except Exception:  # noqa: BLE001 - a resident worker must reconnect after DB/provider failures
            logger.exception("voice memo landing worker failed; reconnecting")
            stop_event.wait(retry_seconds)


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    stop_event = threading.Event()

    def stop(_signum, _frame) -> None:
        stop_event.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    logger.info("starting voice memo landing worker")
    run_resident_worker(stop_event)


if __name__ == "__main__":
    main()
