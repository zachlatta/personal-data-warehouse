"""Land a source's timeline rows from the source's own ingest run.

Measured 2026-09-25 from ``timeline.events.first_seen_at - event_ts``: every
chat source landed in p50 4-5 min / p95 9 min, and almost all of it was two
serial five-minute clocks -- the source's own poll or upload, then the
``timeline_sync`` schedule. Two uniform 0-5 minute waits is ~5 min expected
and ~10 min worst case, which is exactly what the tiers read. The sources
themselves were fast (the WhatsApp client is live; a Slack DM is one
``client.counts`` call); the schedule was the latency.

So an ingest asset that just wrote rows calls :func:`land_sources_on_timeline`
before it returns, and the timeline rows of THAT source land in the same run
-- no queue slot, no subprocess start, no second clock. It is incremental
only: backfill, refresh, prune, reconcile and first contact stay with the
scheduled pass, which is unchanged and remains the thing that makes the
timeline converge (C1). The fast lane is a latency optimization with a kill
switch, never a correctness dependency: it never raises into the ingest run,
it skips an adapter the scheduled pass is mid-incremental on, and it skips an
adapter the scheduled pass has not initialized.
"""

from __future__ import annotations

from collections.abc import Sequence
import logging
import os
import time
from typing import Any

from personal_data_warehouse.timeline import TIMELINE_ADAPTERS, TimelineSyncEngine

TIMELINE_FAST_LANE_ENABLED_ENV = "TIMELINE_FAST_LANE_ENABLED"
TIMELINE_FAST_LANE_BUDGET_SECONDS = 90.0


def fast_lane_enabled() -> bool:
    value = os.getenv(TIMELINE_FAST_LANE_ENABLED_ENV, "").strip().lower()
    return value not in {"0", "false", "no", "off"}


def fast_lane_adapter_names(sources: Sequence[str]) -> list[str]:
    """Every registered adapter whose ``source`` is one of ``sources``, in registry order."""
    wanted = set(sources)
    unknown = wanted - {adapter.source for adapter in TIMELINE_ADAPTERS}
    if unknown:
        raise ValueError(f"no timeline adapter reads source(s): {', '.join(sorted(unknown))}")
    return [adapter.name for adapter in TIMELINE_ADAPTERS if adapter.source in wanted]


def land_sources_on_timeline(
    *,
    postgres_url: str,
    sources: Sequence[str],
    logger: logging.Logger | Any,
    max_seconds: float = TIMELINE_FAST_LANE_BUDGET_SECONDS,
    engine_factory=TimelineSyncEngine,
) -> dict[str, Any]:
    """Run the incremental timeline pass for ``sources`` now. Never raises.

    Returns a JSON-able summary for the calling asset's metadata:
    ``{"enabled", "adapters", "rows", "seconds", "skipped", "errors"}``.
    """
    summary: dict[str, Any] = {
        "enabled": fast_lane_enabled(),
        "adapters": [],
        "rows": 0,
        "seconds": 0.0,
        "skipped": [],
        "errors": {},
    }
    if not summary["enabled"]:
        return summary
    started = time.monotonic()
    try:
        adapter_names = fast_lane_adapter_names(sources)
        engine = engine_factory(source_url=postgres_url)
        try:
            stats = engine.run_incremental(adapter_names=adapter_names, max_seconds=max_seconds)
        finally:
            engine.close()
    except Exception as error:  # noqa: BLE001 - the fast lane must never fail ingestion
        logger.warning("timeline fast lane failed for %s: %s", ",".join(sources), error)
        summary["errors"]["fast_lane"] = str(error)
        summary["seconds"] = round(time.monotonic() - started, 3)
        return summary
    for stat in stats:
        summary["adapters"].append(stat.adapter)
        summary["rows"] += stat.incremental_rows
        if stat.error:
            summary["errors"][stat.adapter] = stat.error
        elif stat.incremental_rows == 0 and not stat.backfill_done:
            summary["skipped"].append(stat.adapter)
    summary["seconds"] = round(time.monotonic() - started, 3)
    logger.info(
        "timeline fast lane landed %d row(s) for %s in %.1fs",
        summary["rows"],
        ",".join(sources),
        summary["seconds"],
    )
    return summary
