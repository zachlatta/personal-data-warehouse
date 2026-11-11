"""The request-status ladder is one contract for Python and Go: both read
tests/contracts/request_status_ladder.json (Go: app/internal/mutations
TestDeriveRequestStatusMatchesTheSharedLadder)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from personal_data_warehouse.postgres import derive_request_status

CASES = json.loads((Path(__file__).parent / "contracts" / "request_status_ladder.json").read_text())["cases"]


@pytest.mark.parametrize("case", CASES, ids=lambda case: "+".join(case["statuses"]) or "none")
def test_python_rolls_up_a_request_like_the_shared_ladder(case) -> None:
    assert derive_request_status(case["statuses"], "current") == case["want"]
