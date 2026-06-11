from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


BUILD_INFO_PATH = Path("/app/build-info.json")
UNKNOWN = "unknown"


def git_sha() -> str:
    for name in (
        "PDW_GIT_SHA",
        "SOURCE_COMMIT",
        "GIT_SHA",
        "GIT_COMMIT",
        "COMMIT_SHA",
        "COOLIFY_GIT_COMMIT",
    ):
        value = os.getenv(name, "").strip()
        if value:
            return value

    try:
        data: dict[str, Any] = json.loads(BUILD_INFO_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return UNKNOWN

    value = str(data.get("git_sha", "")).strip()
    return value or UNKNOWN


def build_metadata() -> dict[str, str]:
    return {"git_sha": git_sha()}
