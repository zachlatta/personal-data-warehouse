"""Capture context for photo vision enrichment.

A photo alone can't tell the agent it was taken at an event — the warehouse
can. This builds the ``capture_context`` block the vision prompt receives:
the LOCAL capture time (the asset's own timezone offset), GPS coordinates,
camera, and the owner's calendar events around the capture instant (the same
±3h distance-ordered window the voice-memo enrichment uses). The agent is
instructed to use it only when consistent with the pixels.

The builder is wired into FileAttachmentEnrichmentRunner via
``context_builder`` and keyed by the candidate's ``photo_id`` extra column;
failures degrade to a context-free caption, never a blocked enrichment.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

CALENDAR_WINDOW_HOURS = 3
CALENDAR_CANDIDATE_LIMIT = 8

_EPOCH_GUARD = datetime(1970, 1, 2, tzinfo=UTC)


def photo_enrichment_context(warehouse, candidate: Mapping[str, Any]) -> dict[str, Any] | None:
    photo_id = str(candidate.get("photo_id", "") or "")
    if not photo_id:
        return None
    rows = warehouse._query_dicts(
        """
        SELECT capture_ts, capture_tz_offset, latitude, longitude, camera_make, camera_model
        FROM photo_assets
        WHERE photo_id = %s
        """,
        (photo_id,),
    )
    if not rows:
        return None
    asset = rows[0]
    capture_ts = asset.get("capture_ts")
    if not isinstance(capture_ts, datetime):
        return None
    if capture_ts.tzinfo is None:
        capture_ts = capture_ts.replace(tzinfo=UTC)
    if capture_ts <= _EPOCH_GUARD:
        return None

    context: dict[str, Any] = {
        "what_this_is": (
            "Trusted metadata about the moment this photo was taken, from the "
            "owner's data warehouse. Ground the description in it, but never "
            "let it override what is actually visible."
        ),
        "captured_at_utc": capture_ts.astimezone(UTC).isoformat(),
    }
    local = _localize(capture_ts, str(asset.get("capture_tz_offset", "") or ""))
    if local is not None:
        context["captured_at_local"] = local.isoformat()
        context["local_day_of_week"] = local.strftime("%A")
    latitude = float(asset.get("latitude", 0.0) or 0.0)
    longitude = float(asset.get("longitude", 0.0) or 0.0)
    if latitude or longitude:
        context["gps"] = {"latitude": round(latitude, 5), "longitude": round(longitude, 5)}
    camera = " ".join(
        part
        for part in (str(asset.get("camera_make", "") or ""), str(asset.get("camera_model", "") or ""))
        if part
    )
    if camera:
        context["camera"] = camera
    context["calendar_events_near_capture"] = _calendar_events_near(warehouse, capture_ts)
    return context


def _localize(capture_ts: datetime, tz_offset: str) -> datetime | None:
    """Apply the asset's own "+HH:MM" offset; None when the offset is unknown
    (a UTC wall clock would be misleading, so we omit local time entirely)."""
    if not tz_offset:
        return None
    try:
        sign = 1 if tz_offset.startswith("+") else -1
        hours, minutes = tz_offset.lstrip("+-").split(":")
        offset = timezone(sign * timedelta(hours=int(hours), minutes=int(minutes)))
    except (ValueError, AttributeError):
        return None
    return capture_ts.astimezone(offset)


def _calendar_events_near(warehouse, capture_ts: datetime) -> list[dict[str, Any]]:
    """Owner's calendar entries overlapping capture time ±3h, nearest first —
    the voice-memo enrichment's window. Attendee lists are deliberately NOT
    included: the vision prompt must never be tempted to name people."""
    instant = capture_ts.astimezone(UTC).isoformat()
    rows = warehouse._query(
        f"""
        SELECT event_id, summary, start_at, end_at, location
        FROM calendar_events
        WHERE start_at <= %s::timestamptz + INTERVAL '{CALENDAR_WINDOW_HOURS} hours'
          AND end_at >= %s::timestamptz - INTERVAL '{CALENDAR_WINDOW_HOURS} hours'
          AND is_deleted = 0
          AND status <> 'cancelled'
        ORDER BY ABS(EXTRACT(EPOCH FROM (start_at - %s::timestamptz)))
        LIMIT {CALENDAR_CANDIDATE_LIMIT}
        """,
        (instant, instant, instant),
    )
    events = []
    for event_id, summary, start_at, end_at, location in rows:
        events.append(
            {
                "event_id": str(event_id),
                "summary": str(summary),
                "start_at": start_at.isoformat() if hasattr(start_at, "isoformat") else str(start_at),
                "end_at": end_at.isoformat() if hasattr(end_at, "isoformat") else str(end_at),
                "location": str(location or ""),
            }
        )
    return events
