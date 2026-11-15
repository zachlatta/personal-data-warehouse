"""Claim-time re-check of a policy-approved mutation.

A mutation PDW approved under a policy (``approved_by = policy:<id>@<version>``,
written by the Go app, app/internal/mutations/policy*.go) is checked again in
the transaction that claims it, with the policy, grant and kill-switch rows
locked FOR SHARE, so a switch-off or revoke committed before the claim always
wins. A failure sends the request back to Zach's review (``pending_review``)
with the reason; it never fails the mutation. A mutation Zach approved by hand
does not depend on a policy and is never re-checked here.

The checks: the kill switch (``PDW_MUTATION_AUTO_APPROVE=0`` or the settings
row), the policy still enforced and at the version that approved it, inside
its valid dates, the proposing token's grant still active, the rate limits
still passing over what other requests under the policy have claimed, and no
setpoint change the policy did not make inside its pause window.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
import json
import os
from typing import Any

POLICY_ACTOR_PREFIX = "policy:"
# The per-policy advisory lock every writer of a policy, its grants or the kill
# switch takes, and the claim takes before re-checking: the same key in Go
# (policy_store.go, policyLockPrefix) and here, so a version insert, a revoke
# or a switch-off and a claim never interleave. The settings key cannot be a
# policy id (ids are [a-z0-9-]). Order: settings first, then ids sorted.
POLICY_LOCK_PREFIX = "pdw_mutation_policy:"
SETTINGS_LOCK_ID = "*settings"
AUTO_APPROVE_ENV = "PDW_MUTATION_AUTO_APPROVE"
AUTO_APPROVE_SETTING_KEY = "auto_approve"
RESIDEO_SET_THERMOSTAT_OPERATION = "resideo.set_thermostat"
RESIDEO_ACCOUNT = "home"
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# The twin of thermostatManualChangeSQL in app/internal/mutations/policy_store.go:
# a setpoint change in the readings (one row per state change) at or after
# `since` that no succeeded mutation this policy approved explains.
THERMOSTAT_MANUAL_CHANGE_SQL = """
    WITH readings AS (
        SELECT observed_at, heat_setpoint, cool_setpoint,
               lag(heat_setpoint) OVER (ORDER BY observed_at) AS previous_heat,
               lag(cool_setpoint) OVER (ORDER BY observed_at) AS previous_cool
        FROM @resideo_thermostat_readings
        WHERE account = %(account)s AND device_id = %(device)s
          AND observed_at >= %(since)s::timestamptz - interval '7 days'
    )
    SELECT readings.observed_at
    FROM readings
    WHERE readings.observed_at >= %(since)s
      AND readings.previous_heat IS NOT NULL
      AND (readings.heat_setpoint IS DISTINCT FROM readings.previous_heat
           OR readings.cool_setpoint IS DISTINCT FROM readings.previous_cool)
      AND NOT EXISTS (
        SELECT 1 FROM @upstream_mutations AS mutation
        WHERE mutation.operation = 'resideo.set_thermostat'
          AND mutation.status = 'succeeded'
          AND starts_with(mutation.approved_by, %(pattern)s)
          AND mutation.payload_json->>'device_id' = %(device)s
          AND (NOT (mutation.payload_json ? 'heat_setpoint')
               OR (mutation.payload_json->>'heat_setpoint')::float8 = readings.heat_setpoint)
          AND (NOT (mutation.payload_json ? 'cool_setpoint')
               OR (mutation.payload_json->>'cool_setpoint')::float8 = readings.cool_setpoint)
          AND readings.observed_at BETWEEN mutation.executed_at - interval '30 minutes'
                                       AND mutation.executed_at + interval '5 minutes'
      )
    ORDER BY readings.observed_at DESC
    LIMIT 1
"""


def lock_policy_state(query: Callable[[str, Any], list[dict[str, Any]]], policy_ids: list[str]) -> None:
    """Take the kill switch's and each policy's advisory lock, in the order
    every writer takes them, for the rest of the transaction."""
    for key in [SETTINGS_LOCK_ID, *sorted(set(policy_ids))]:
        query("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0)) AS locked", (POLICY_LOCK_PREFIX + key,))


def parse_policy_actor(approved_by: str) -> tuple[str, int] | None:
    """``policy:<id>@<version>`` -> (id, version); None for anything else."""
    value = str(approved_by or "")
    if not value.startswith(POLICY_ACTOR_PREFIX):
        return None
    rest = value[len(POLICY_ACTOR_PREFIX):]
    policy_id, sep, version = rest.rpartition("@")
    if not sep or not policy_id:
        return None
    try:
        return policy_id, int(version)
    except ValueError:
        return None


def auto_approve_forced_off(environ: dict[str, str] | None = None) -> bool:
    value = (environ if environ is not None else os.environ).get(AUTO_APPROVE_ENV, "").strip().lower()
    return value in {"0", "false", "off"}


def _json(value: Any) -> Any:
    if isinstance(value, (bytes, str)):
        try:
            return json.loads(value)
        except ValueError:
            return None
    return value


def _setting_enabled(value: Any) -> bool:
    parsed = _json(value)
    return isinstance(parsed, dict) and parsed.get("enabled") is True


def _limit(limits: dict[str, Any], key: str) -> float:
    value = limits.get(key)
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else 0.0


def policy_recheck_failure(
    query: Callable[[str, Any], list[dict[str, Any]]],
    mutation: dict[str, Any],
    *,
    now: datetime,
    relation_exists: Callable[[str], bool],
    environ: dict[str, str] | None = None,
) -> str | None:
    """Why a policy-approved mutation may no longer run, or None.

    ``query`` runs SQL (with @relation markers) inside the claim transaction.
    """
    parsed = parse_policy_actor(str(mutation.get("approved_by") or ""))
    if parsed is None:
        return None
    policy_id, version = parsed
    if auto_approve_forced_off(environ):
        return f"auto-approval is switched off ({AUTO_APPROVE_ENV}=0), so this waits for your approval"
    setting = query(
        "SELECT value FROM @mutation_policy_settings WHERE key = %s FOR SHARE", (AUTO_APPROVE_SETTING_KEY,)
    )
    if setting and not _setting_enabled(setting[0]["value"]):
        return "auto-approval was switched off before this ran, so it waits for your approval"
    # A policy approves one change per request (its limits count changes);
    # a batch is never run under one, whatever approved it.
    siblings = query(
        "SELECT count(*) AS n FROM @upstream_mutations WHERE request_id = %s", (mutation.get("request_id"),)
    )
    if siblings and int(siblings[0]["n"]) > 1:
        return f"a request with {int(siblings[0]['n'])} changes is never run under a policy; it waits for your approval"
    policies = query(
        """
        SELECT version, mode, valid_from, valid_until, limits_json
        FROM @mutation_policies WHERE id = %s ORDER BY version DESC LIMIT 1 FOR SHARE
        """,
        (policy_id,),
    )
    if not policies:
        return f"policy {policy_id} no longer exists"
    policy = policies[0]
    if str(policy["mode"]) != "enforce":
        return f"policy {policy_id} is {policy['mode']} now, so this waits for your approval"
    if int(policy["version"]) != version:
        return f"policy {policy_id} changed (v{version} approved this, v{policy['version']} is current)"
    valid_from, valid_until = policy["valid_from"], policy["valid_until"]
    if valid_from and valid_from > _EPOCH and now < valid_from:
        return f"policy {policy_id} has not started"
    if valid_until and valid_until > _EPOCH and now >= valid_until:
        return f"policy {policy_id} has ended"
    request = query(
        "SELECT requested_by_token_id FROM @upstream_mutation_requests WHERE id = %s",
        (mutation.get("request_id"),),
    )
    token_id = str(request[0]["requested_by_token_id"] or "") if request else ""
    grants = query(
        """
        SELECT id FROM @mutation_policy_grants
        WHERE policy_id = %s AND token_id = %s AND token_id <> ''
          AND revoked_at <= '1970-01-01 00:00:00+00'::timestamptz
          AND expires_at > %s
        FOR SHARE
        """,
        (policy_id, token_id, now),
    )
    if not grants:
        return f"the proposing token's grant on policy {policy_id} was revoked or has expired"

    limits = _json(policy["limits_json"]) or {}
    # An exact prefix, never LIKE: a '_' in an id would match any character.
    pattern = f"{POLICY_ACTOR_PREFIX}{policy_id}@"
    for key, window in (("max_per_day", timedelta(days=1)), ("max_per_hour", timedelta(hours=1))):
        maximum = _limit(limits, key)
        if maximum <= 0:
            continue
        claimed = query(
            """
            SELECT count(DISTINCT request_id) AS n FROM @upstream_mutations
            WHERE starts_with(approved_by, %s) AND request_id <> %s AND claimed_at >= %s
            """,
            (pattern, mutation.get("request_id"), now - window),
        )
        if claimed and int(claimed[0]["n"]) >= maximum:
            return f"policy {policy_id} already ran {int(claimed[0]['n'])} changes in the {key.removeprefix('max_per_')} (limit {int(maximum)})"
    interval = _limit(limits, "min_interval_minutes")
    if interval > 0:
        last = query(
            "SELECT max(claimed_at) AS at FROM @upstream_mutations WHERE starts_with(approved_by, %s) AND request_id <> %s",
            (pattern, mutation.get("request_id")),
        )
        at = last[0]["at"] if last else None
        if at is not None and at > _EPOCH and now - at < timedelta(minutes=interval):
            return f"policy {policy_id} needs {int(interval)} minutes between changes"
    pause = _limit(limits, "pause_after_manual_change_hours")
    if pause > 0 and mutation.get("operation") == RESIDEO_SET_THERMOSTAT_OPERATION:
        if not relation_exists("resideo_thermostat_readings"):
            return "the thermostat's change history cannot be read, so a hand change cannot be ruled out"
        payload = _json(mutation.get("payload_json")) or {}
        changed = query(
            THERMOSTAT_MANUAL_CHANGE_SQL,
            {
                "account": RESIDEO_ACCOUNT,
                "device": str(payload.get("device_id") or ""),
                "since": now - timedelta(hours=pause),
                "pattern": pattern,
            },
        )
        if changed:
            return (
                f"the setpoint was changed at {changed[0]['observed_at'].isoformat()} by something other than "
                f"policy {policy_id}, which pauses it for {pause:g} hours"
            )
    return None
