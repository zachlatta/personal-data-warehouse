from __future__ import annotations

import json
from pathlib import Path


ARTIFACT = Path(__file__).resolve().parents[1] / "docs" / "verification" / "whoop-live-api-2026-07-11.json"


def test_redacted_live_whoop_verification_artifact_covers_required_api() -> None:
    artifact = json.loads(ARTIFACT.read_text())

    assert artifact["verification"] == "live_whoop_v2_read_only_api"
    assert artifact["user_assisted_oauth"] is True
    assert set(artifact["verified_scopes"]) == {
        "offline",
        "read:profile",
        "read:body_measurement",
        "read:cycles",
        "read:recovery",
        "read:sleep",
        "read:workout",
    }

    result = artifact["result"]
    assert result["profile_endpoint"] == "ok"
    assert set(result["profile_fields"]) >= {"user_id", "email", "first_name", "last_name"}
    assert result["body_measurement_endpoint"] == "ok"
    assert set(result["body_measurement_fields"]) >= {"height_meter", "weight_kilogram", "max_heart_rate"}

    collections = result["collection_endpoints"]
    assert set(collections) == {"cycles", "recovery", "sleep", "workout"}
    for endpoint in collections.values():
        assert endpoint["status"] == "ok"
        assert endpoint["record_count"] >= 1
        assert endpoint["record_fields"]

    tests = artifact["test_verification"]
    assert tests["full_python_suite"]["result"] == "897 passed, 125 skipped, 0 failed"
    assert tests["whoop_with_real_postgres"]["result"] == "33 passed, 0 failed"
    assert tests["go_warehouse_query"]["result"] == "passed"


def test_redacted_live_whoop_verification_artifact_contains_no_credentials_or_values() -> None:
    artifact = json.loads(ARTIFACT.read_text())
    serialized = json.dumps(artifact, sort_keys=True).lower()

    for forbidden in (
        "access_token",
        "refresh_token",
        "client_secret",
        "authorization_code",
        "bearer ",
        "?code=",
    ):
        assert forbidden not in serialized

    # The result is intentionally shape-only: booleans, counts, status labels,
    # and field names. No WHOOP response objects or account values are retained.
    result = artifact["result"]
    assert set(result) == {
        "account_configured",
        "profile_endpoint",
        "profile_fields",
        "body_measurement_endpoint",
        "body_measurement_fields",
        "collection_endpoints",
    }
