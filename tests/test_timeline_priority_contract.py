"""One generated contract for timeline priority meanings and scope selection."""

from __future__ import annotations

from pathlib import Path

from personal_data_warehouse.timeline import TIMELINE_PRIORITY_DEFINITIONS
from personal_data_warehouse.warehouse_catalog import CATALOG


REPO_ROOT = Path(__file__).resolve().parent.parent


def test_priority_contract_is_the_python_definition_source() -> None:
    contract = CATALOG.timeline_priorities

    assert [tier.name for tier in contract.tiers] == [
        "self",
        "direct",
        "cc",
        "noise",
        "background",
    ]
    assert contract.attention_priorities == ("self", "direct", "cc")
    assert contract.sentinel.name == "unclassified"
    assert TIMELINE_PRIORITY_DEFINITIONS == tuple(
        [(tier.name, tier.meaning) for tier in contract.tiers]
        + [(contract.sentinel.name, contract.sentinel.meaning)]
    )


def test_background_and_selection_guidance_cover_the_recall_traps() -> None:
    contract = CATALOG.timeline_priorities
    background = next(tier for tier in contract.tiers if tier.name == "background")
    background_text = f"{background.meaning} {background.typical_rows}".lower()
    for required in ("model answers", "tool output", "orchestrated", "other people's"):
        assert required in background_text

    guide = {entry.intent: entry.priorities for entry in contract.selection_guide}
    assert guide["attention or correspondence"] == ("self", "direct", "cc")
    assert guide["Zach's own acts or words"] == ("self",)
    assert guide["prior agent conclusions"] == ("self", "background")
    assert guide["notifications, CI, or telemetry"] == ("noise",)
    assert guide["broad topical discovery or uncertain scope"] == ()


def test_generated_priority_documentation_matches_the_contract() -> None:
    text = (REPO_ROOT / "AGENTS.md").read_text()
    start = "<!-- BEGIN GENERATED TIMELINE PRIORITY CONTRACT -->"
    end = "<!-- END GENERATED TIMELINE PRIORITY CONTRACT -->"
    assert text.count(start) == text.count(end) == 1
    generated = text.split(start, 1)[1].split(end, 1)[0]

    contract = CATALOG.timeline_priorities
    for tier in contract.tiers:
        assert f"`{tier.name}`" in generated
        assert tier.meaning in generated
        assert tier.typical_rows in generated
    for entry in contract.selection_guide:
        assert entry.intent in generated
        expected = ",".join(entry.priorities) if entry.priorities else "all tiers (omit the filter)"
        assert expected in generated
