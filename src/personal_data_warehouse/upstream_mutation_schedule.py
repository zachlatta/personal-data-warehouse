"""Preconditions on scheduled mutations (app/internal/mutations/schedule.go).

A step of an approved plan may carry a precondition the worker checks right
before running it. A failed check ends the mutation as ``skipped_precondition``
(never a write), and the app tells Zach.

``not_home`` (the away step of a trip plan): skip when phone location in the
last ``within_minutes`` puts the phone within the home radius, and also when
there is no location in that window at all, because missing data must never
send the house to away. Home is the deployment's ``PDW_HOME_LATITUDE`` /
``PDW_HOME_LONGITUDE`` (``PDW_HOME_RADIUS_METERS``, default 1000), which never
go in Git; with none set the step is skipped.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
import json
import math
import os
from typing import Any

PRECONDITION_NOT_HOME = "not_home"
DEFAULT_HOME_RADIUS_METERS = 1000.0


def _json(value: Any) -> Any:
    if isinstance(value, (bytes, str)):
        try:
            return json.loads(value)
        except ValueError:
            return None
    return value


def _distance_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6_371_000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * radius * math.asin(math.sqrt(a))


def _home(environ: Mapping[str, str]) -> tuple[float, float, float] | None:
    try:
        latitude = float(environ["PDW_HOME_LATITUDE"])
        longitude = float(environ["PDW_HOME_LONGITUDE"])
    except (KeyError, ValueError):
        return None
    try:
        radius = float(environ.get("PDW_HOME_RADIUS_METERS") or DEFAULT_HOME_RADIUS_METERS)
    except ValueError:
        radius = DEFAULT_HOME_RADIUS_METERS
    return latitude, longitude, radius


def precondition_failure(
    query: Callable[[str, Any], list[dict[str, Any]]],
    mutation: Mapping[str, Any],
    *,
    now: datetime,
    relation_exists: Callable[[str], bool],
    environ: Mapping[str, str] | None = None,
) -> str | None:
    """Why a scheduled step must be skipped, or None to run it."""
    precondition = _json(mutation.get("precondition_json")) or {}
    if not isinstance(precondition, dict) or not precondition:
        return None
    kind = str(precondition.get("kind") or "")
    if kind != PRECONDITION_NOT_HOME:
        return f"unknown precondition {kind!r}; skipped rather than guessed"
    minutes = precondition.get("within_minutes")
    if not isinstance(minutes, (int, float)) or isinstance(minutes, bool) or minutes <= 0:
        return "the not_home precondition has no within_minutes"
    home = _home(environ if environ is not None else os.environ)
    if home is None:
        return "home is not configured on the deployment (PDW_HOME_LATITUDE/LONGITUDE), so the phone cannot be placed away"
    if not relation_exists("android_location_samples"):
        return "there is no phone location table, so the phone cannot be placed away"
    since = now - timedelta(minutes=float(minutes))
    samples = query(
        """
        SELECT latitude, longitude, measured_at FROM @android_location_samples
        WHERE measured_at >= %s ORDER BY measured_at DESC LIMIT 500
        """,
        (since,),
    )
    if not samples:
        return f"no phone location in the last {minutes:g} minutes; the away step waits for a person rather than guessing"
    latitude, longitude, radius = home
    for sample in samples:
        if _distance_meters(latitude, longitude, float(sample["latitude"]), float(sample["longitude"])) <= radius:
            return f"the phone was at home at {sample['measured_at'].isoformat()}"
    return None
