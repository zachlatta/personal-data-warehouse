"""Repo-level contracts: CI, the search source registry, and the documentation.

The rest of the suite tests the warehouse. This file tests the things that keep
the warehouse's own guardrails honest, and it exists because each of these was
load-bearing and unenforced:

* **CI actually runs.** Until this file landed, ``.github/workflows`` was
  path-filtered to ``app/**`` and ``docker/postgres-pgbackrest/**``, so a pull
  request touching only ``src/``, ``tests/``, ``scripts/`` or the docs triggered
  no CI at all — every Python contract test was an honor system. Worse, the
  strongest tests ``pytest.skip`` when ``POSTGRES_DATABASE_URL`` is unset, so on
  a machine without ``.env`` they evaporate green. A guardrail that silently
  disappears is not a guardrail: that is exactly how a stale ``public.search_text``
  shadow shipped and returned zero rows for 16 days.
* **The search source registry.** ``SEARCH_SOURCE_DEFS`` and ``TIMELINE_ADAPTERS``
  agreed 25-for-25 by luck. A missed entry is not a loud error — it silently puts
  the new adapter in the wrong BM25 index partition (documented as costing 15-16s
  on an unlucky query) and makes the source unscopeable in ``search``.
* **The documentation.** ``priority`` is the single most agent-facing feature of
  ``timeline.events`` and appeared zero times in ``AGENTS.md`` and ``README.md``
  for the four weeks after it shipped.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import pytest
import yaml
from dotenv import load_dotenv

from personal_data_warehouse.pipeline_health import PIPELINES, TABLE_PIPELINES
from personal_data_warehouse.postgres import (
    SEARCH_SOURCE_DEFS,
    SEARCH_TEXT_HIGH_VOLUME_SOURCES,
    SEARCH_TEXT_LOW_VOLUME_ADAPTERS,
)
from personal_data_warehouse.timeline import (
    TIMELINE_ADAPTERS,
    TIMELINE_PRIORITY_BACKGROUND,
    TIMELINE_PRIORITY_CC,
    TIMELINE_PRIORITY_DIRECT,
    TIMELINE_PRIORITY_NOISE,
    TIMELINE_PRIORITY_SELF,
    TIMELINE_TABLE_COVERAGE,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
AGENTS_MD = REPO_ROOT / "AGENTS.md"
README_MD = REPO_ROOT / "README.md"
PYTHON_TESTS_WORKFLOW = REPO_ROOT / ".github/workflows/python-tests.yml"

# The tier labels are stored as quoted SQL literals so they can be interpolated
# straight into an adapter's SELECT; strip the quotes to get the enum label.
PRIORITY_TIERS = tuple(
    label.strip("'")
    for label in (
        TIMELINE_PRIORITY_SELF,
        TIMELINE_PRIORITY_DIRECT,
        TIMELINE_PRIORITY_CC,
        TIMELINE_PRIORITY_NOISE,
        TIMELINE_PRIORITY_BACKGROUND,
    )
)


# ---------------------------------------------------------------------------
# CI: the guardrails must not be able to vanish quietly
# ---------------------------------------------------------------------------


def test_a_ci_run_has_a_database_or_fails_loudly() -> None:
    """Under CI, a missing ``POSTGRES_DATABASE_URL`` is a failure, not a skip.

    The database-backed suites are the only tests that touch a real schema, a
    real BM25 index, and the real read-only role. Without a URL they skip, and a
    CI run with no database is a green run that proved nothing. Named with a
    leading ``a_`` so it reports before the suites it is protecting.
    """
    load_dotenv()
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if url:
        return
    if os.environ.get("CI"):
        pytest.fail(
            "POSTGRES_DATABASE_URL is not set inside CI. The database-backed contract "
            "suites (timeline, pipeline health, schema reorg) would silently skip and "
            "report green. Start the warehouse Postgres image and export the URL."
        )
    pytest.skip("POSTGRES_DATABASE_URL is not set (set CI=1 to make this a failure)")


def _workflow(path: Path) -> dict:
    # PyYAML parses the unquoted `on:` key as the boolean True (the Norway
    # problem); accept either spelling rather than depending on it.
    loaded = yaml.safe_load(path.read_text())
    triggers = loaded.get("on", loaded.get(True))
    assert isinstance(triggers, dict), f"{path.name}: no trigger block"
    loaded["on"] = triggers
    return loaded


def test_python_ci_workflow_exists_and_has_no_path_filter() -> None:
    """The Python suite must run on every pull request, whatever it touched.

    Both pre-existing workflows are path-filtered to their own subtree, which is
    right for an image build and wrong for a correctness gate: the schema
    catalog, the timeline adapters, the pipeline registries and the docs all
    live outside those paths, so the tests that guard them never ran in CI.
    """
    assert PYTHON_TESTS_WORKFLOW.exists(), "the Python test workflow is missing"
    workflow = _workflow(PYTHON_TESTS_WORKFLOW)
    triggers = workflow["on"]

    assert "pull_request" in triggers, "the Python suite must run on pull requests"
    pull_request = triggers["pull_request"] or {}
    assert "paths" not in pull_request and "paths-ignore" not in pull_request, (
        "the Python suite must not be path-filtered: a PR that touches only src/, "
        "tests/, scripts/ or the docs is exactly the PR it exists to catch"
    )

    push = triggers.get("push") or {}
    assert "main" in (push.get("branches") or []), "the Python suite must run on pushes to main"
    assert "paths" not in push and "paths-ignore" not in push


def test_ci_runs_the_database_backed_contract_suites() -> None:
    """CI must run the whole suite, or explicitly name every contract suite.

    Naming a curated list was the earlier gate, back when two pre-existing
    failures kept the tree from running green on a fresh database. Both are
    fixed, so the workflow now runs everything — which is strictly stronger,
    because a contract suite nobody remembered to add to the list is exactly
    the one that rots. Accept either shape so the guardrail keeps meaning
    something if the tree ever has to be narrowed again.
    """
    text = PYTHON_TESTS_WORKFLOW.read_text()
    runs_everything = any(
        line.strip() in {"run: uv run pytest -q -rs", "run: uv run pytest -q"}
        or line.strip().startswith("run: uv run pytest -q -rs\n")
        for line in text.splitlines()
    )
    if runs_everything:
        return
    for suite in (
        "tests/test_timeline.py",
        "tests/test_pipeline_health.py",
        "tests/test_schema_reorg_contract.py",
        "tests/test_repo_contracts.py",
    ):
        assert suite in text, f"{suite} is not run by the Python CI workflow"


# ---------------------------------------------------------------------------
# the search source registry
# ---------------------------------------------------------------------------


def _search_registry_adapters() -> set[str]:
    return {adapter for _, adapters, _ in SEARCH_SOURCE_DEFS for adapter in adapters}


def test_every_timeline_adapter_has_a_search_source_token() -> None:
    """Every adapter must be reachable through ``sources`` and correctly partitioned.

    ``SEARCH_SOURCE_DEFS`` is the single vocabulary both ``timeline.search_text``
    and ``timeline.search_text_exact`` are generated from, and it is also what
    derives ``SEARCH_TEXT_LOW_VOLUME_ADAPTERS`` — the partial BM25 index the
    broad search pool scans for the low-volume tail. An adapter missing from it
    is therefore wrong twice over: it cannot be scoped by a caller, and its rows
    are only reachable through the global index, which means walking past
    millions of gmail/slack documents (15-16s on an unlucky query).
    """
    missing = sorted({adapter.name for adapter in TIMELINE_ADAPTERS} - _search_registry_adapters())
    assert not missing, (
        "timeline adapters with no SEARCH_SOURCE_DEFS entry: "
        + ", ".join(missing)
        + " — add each to a source token in src/personal_data_warehouse/postgres.py"
    )


def test_search_source_registry_names_only_real_adapters() -> None:
    """And nothing may claim an adapter that does not exist.

    A typo here is silent in the other direction: the generated SQL filters on a
    ``source``/``kind`` value nothing ever emits, so the token exists, is
    accepted, and always returns zero rows.
    """
    unknown = sorted(_search_registry_adapters() - {adapter.name for adapter in TIMELINE_ADAPTERS})
    assert not unknown, "SEARCH_SOURCE_DEFS names unknown timeline adapters: " + ", ".join(unknown)


def test_search_source_tokens_are_unique_and_high_volume_tokens_are_real() -> None:
    tokens = [token for token, _, _ in SEARCH_SOURCE_DEFS]
    assert len(tokens) == len(set(tokens)), "duplicate SEARCH_SOURCE_DEFS token"
    unknown = sorted(set(SEARCH_TEXT_HIGH_VOLUME_SOURCES) - set(tokens))
    assert not unknown, (
        "SEARCH_TEXT_HIGH_VOLUME_SOURCES names tokens that are not sources: " + ", ".join(unknown)
    )


def test_the_two_bm25_pool_partitions_cover_every_adapter_exactly_once() -> None:
    """High-volume and low-volume must partition the adapters, not overlap or gap.

    The broad pool is two index-ordered scans: the global BM25 index for the
    high-volume adapters and a partial index for everything else. An adapter in
    neither partition is invisible to a broad search; one in both is scored
    twice.
    """
    high = {
        adapter
        for token, adapters, _ in SEARCH_SOURCE_DEFS
        if token in SEARCH_TEXT_HIGH_VOLUME_SOURCES
        for adapter in adapters
    }
    low = set(SEARCH_TEXT_LOW_VOLUME_ADAPTERS)
    assert not (high & low), "adapter in both BM25 pool partitions: " + ", ".join(sorted(high & low))
    assert high | low == _search_registry_adapters()


# ---------------------------------------------------------------------------
# the documented contracts
# ---------------------------------------------------------------------------


def test_agents_md_states_every_contract() -> None:
    """Every contract must be stated where future work will read them.

    Zach's requirement is that "future developers / developer agents understand
    these contracts and honor them on future work in this repo". A contract that
    lives only in a Python comment is not a contract anyone can honor.

    Grew from seven to eleven on 2026-08-26. The four added were each already a
    real requirement that nothing in the repo named, and an unnamed contract is
    one nobody is accountable for: search quality and embedding freshness (C8),
    one obvious way to do a thing (C9), backups that have actually been restored
    (C10), and per-source SLAs (C11). C10 is the cautionary one -- production
    had no valid backup for a day while every health surface read green.
    """
    text = AGENTS_MD.read_text()
    assert "## The eleven contracts" in text
    section = text.split("## The eleven contracts", 1)[1]
    for marker in ("C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "C10", "C11"):
        assert re.search(rf"\*\*{marker}\b", section[:12000]), f"contract {marker} is not stated"


def test_c5_requires_enrichment_to_read_the_intermediate_layer() -> None:
    """C5 must say what a transformation READS, not only where its output lives.

    The original wording said identity and enrichment "live in derived_*" and
    stopped there. Every enforced registry therefore passed while
    base_alice_voice_recordings sat at 53 recordings with 0 transcripts: the
    transcription pass named base_apple_voice_memos.files directly, so a second
    voice source was invisible to it. The contract has to constrain the input.
    """
    text = AGENTS_MD.read_text()
    section = text.split("## The eleven contracts", 1)[1].split("\n## ", 1)[0]
    c5 = section.split("**C5", 1)[1].split("**C6", 1)[0]
    assert "READ" in c5, "C5 does not say what a transformation must read from"
    assert "alice" in c5.lower(), (
        "C5 does not carry the evidence that made the input rule necessary"
    )


def test_agents_md_documents_every_priority_tier() -> None:
    """All five tiers, plus the ``unclassified`` sentinel, must be documented.

    ``priority`` is what an agent filters a timeline read by, and it appeared
    zero times in AGENTS.md and README.md. Deriving the tier list from the
    registry means adding a sixth tier fails here until it is written down.
    """
    text = AGENTS_MD.read_text()
    assert "## Timeline priority tiers" in text
    section = text.split("## Timeline priority tiers", 1)[1]
    for tier in PRIORITY_TIERS:
        assert f"`{tier}`" in section, f"priority tier '{tier}' is not documented"
    assert "unclassified" in section, "the unclassified fail-loud sentinel is not documented"
    assert "adapter_signature" in section, (
        "the re-walk cost of changing an adapter's classification is not documented"
    )


def test_the_priority_filter_is_documented_where_search_is_documented() -> None:
    """``priorities`` is implemented in SQL, the CLI and MCP, and documented nowhere.

    Thirty days of real agent sessions used ``--priority`` six times while
    writing 321 ``ILIKE``-on-``base_*`` queries. The tiers were written down;
    the *filter that applies them* was not, in either reader-facing document.
    """
    for path in (AGENTS_MD, README_MD):
        text = path.read_text()
        assert "priorities" in text, f"{path.name} never mentions the priorities filter"
        assert "priorities => ARRAY" in text, (
            f"{path.name} does not show the SQL form of the priority filter"
        )
        assert "--priority" in text, (
            f"{path.name} does not show the CLI form of the priority filter"
        )


def test_the_unclassified_sentinel_is_not_presented_as_a_sixth_tier() -> None:
    """``unclassified`` is accepted by the filter but is not a tier.

    Scoping a search to it is how a classification outage is found, so it stays
    valid input — which is exactly why every place that lists it has to say what
    it is. The CLI's own error used to print it in the same breath as the five
    real tiers with nothing to distinguish it.
    """
    for path in (AGENTS_MD, README_MD):
        text = path.read_text()
        assert "unclassified" in text, f"{path.name} never mentions the sentinel"
        window = text[text.index("unclassified") : text.index("unclassified") + 400]
        assert "sentinel" in window, (
            f"{path.name} mentions unclassified without saying it is a fail-loud sentinel"
        )


def test_agents_md_has_a_general_add_a_source_checklist() -> None:
    """A checklist for *any* source, not only the photo-source one that existed.

    Each enforced registry is named so a reader can find the test that will fail
    them; each silent one is named because nothing else will.
    """
    text = AGENTS_MD.read_text()
    assert "## Adding a warehouse source" in text
    section = text.split("## Adding a warehouse source", 1)[1].split("\n## ", 1)[0]
    for registry in (
        "warehouse_catalog.json",
        "generate_go_warehouse_catalog.py",
        "TableSpec",
        "TIMELINE_TABLE_COVERAGE",
        "TABLE_PIPELINES",
        "TIMELINE_ADAPTERS",
        "SEARCH_SOURCE_DEFS",
    ):
        assert registry in section, f"the add-a-source checklist does not mention {registry}"
    assert "ENFORCED" in section and "SILENT" in section, (
        "the checklist must say which steps a test catches and which fail silently"
    )


def test_agents_md_states_the_performance_contract() -> None:
    """C6 is documented nowhere and has already cost three incidents."""
    text = AGENTS_MD.read_text()
    assert "## Performance contract" in text
    section = text.split("## Performance contract", 1)[1].split("\n## ", 1)[0]
    assert "statement timeout" in section
    assert "parallel_workers_launched" in section, (
        "the 'confirm we are using the host before optimizing' evidence is missing"
    )


def test_agents_md_covers_whoop() -> None:
    """WHOOP appeared zero times in AGENTS.md despite being a first-class source."""
    text = AGENTS_MD.read_text()
    assert "## WHOOP" in text, "AGENTS.md does not mention WHOOP at all"
    section = text.split("## WHOOP", 1)[1].split("\n## ", 1)[0]
    assert "base_whoop.cycles" in section
    assert "private.whoop_oauth_tokens" in section
    assert "docs/whoop-oauth-operations.md" in section


def test_agents_md_covers_whoop_private() -> None:
    """The private API is a second WHOOP source with its own credential.

    It is the only source whose auth is a captured browser session, and the
    only one whose units disagree with a sibling source (seconds vs
    milliseconds of the same measurement), so both have to be written down
    where an agent reading AGENTS.md will hit them.
    """
    text = AGENTS_MD.read_text()
    assert "## WHOOP private API" in text, "AGENTS.md does not mention the WHOOP private API"
    section = text.split("## WHOOP private API", 1)[1].split("\n## ", 1)[0]
    assert "base_whoop_private.journal_entries" in section
    assert "private.whoop_private_sessions" in section
    assert "docs/whoop-private-api.md" in section
    assert "pdw whoop publish-session" in section, (
        "the section must name the command that repairs a dead session"
    )
    assert "hrv_rmssd" in section and "second" in section, (
        "the seconds-vs-milliseconds trap against the public API is a 1000x error"
    )


def _registered_names(prefix: str, *, excluding: tuple[str, ...] = ()) -> tuple[set[str], set[str]]:
    """The coverage tables and timeline adapters belonging to one source.

    ``whoop_private_*`` shares the ``whoop_`` prefix with the public WHOOP
    source, so a bare ``startswith`` glob folds two sources into one set: the
    public assertion then passes on the private source's rows and the private
    assertion can never fail on its own. Longer prefixes living under a shorter
    one are subtracted explicitly rather than tolerated.
    """

    def matches(name: str) -> bool:
        return name.startswith(prefix) and not any(name.startswith(x) for x in excluding)

    return (
        {name for name in TIMELINE_TABLE_COVERAGE if matches(name)},
        {adapter.name for adapter in TIMELINE_ADAPTERS if matches(adapter.name)},
    )


def _assert_source_is_registered(
    prefix: str,
    *,
    excluding: tuple[str, ...] = (),
    pipeline_id: str,
    expected_adapters: set[str],
) -> None:
    tables, adapters = _registered_names(prefix, excluding=excluding)
    assert tables, f"no {prefix}* tables registered — update this test"
    assert tables <= set(TABLE_PIPELINES), (
        f"{prefix}* tables missing from TABLE_PIPELINES: {sorted(tables - set(TABLE_PIPELINES))}"
    )
    assert pipeline_id in {entry.id for entry in PIPELINES}
    assert {name for name in TABLE_PIPELINES if TABLE_PIPELINES[name].pipeline == pipeline_id} <= tables
    # Pinning the exact adapter set is the point: a source that grows an event
    # table and forgets TIMELINE_ADAPTERS fails silently everywhere else.
    assert adapters == expected_adapters
    for name in adapters:
        source_table = next(a.source_table for a in TIMELINE_ADAPTERS if a.name == name)
        assert source_table in tables, f"{name} reads {source_table}, which this source does not own"
        assert TIMELINE_TABLE_COVERAGE[source_table].role == "events", source_table


def test_whoop_is_registered_in_both_registries() -> None:
    """The claim the WHOOP section makes about coverage is checked, not asserted."""
    _assert_source_is_registered(
        "whoop_",
        excluding=("whoop_private_",),
        pipeline_id="whoop",
        expected_adapters={"whoop_cycle", "whoop_recovery", "whoop_sleep", "whoop_workout"},
    )


def test_whoop_private_is_registered_in_both_registries() -> None:
    """The private API is a separate source and needs its own registrations.

    It syncs the same cycles/sleeps/recoveries/workouts the public API does, so
    exactly one of its tables may be an events table: the journal, which has no
    public equivalent. A second adapter over the private copies would emit a
    duplicate of every health event onto a 43M-row timeline.
    """
    _assert_source_is_registered(
        "whoop_private_",
        pipeline_id="whoop_private",
        expected_adapters={"whoop_private_journal"},
    )
    tables, _ = _registered_names("whoop_private_")
    duplicated = {
        "whoop_private_cycles",
        "whoop_private_sleeps",
        "whoop_private_recoveries",
        "whoop_private_workouts",
        "whoop_private_heart_rate_samples",
        "whoop_private_workout_heart_rate_samples",
        "whoop_private_sleep_events",
        "whoop_private_documents",
    }
    assert duplicated <= tables
    for table in duplicated:
        assert TIMELINE_TABLE_COVERAGE[table].role == "detail", (
            f"{table} is classified {TIMELINE_TABLE_COVERAGE[table].role!r}; the public "
            "base_whoop adapters already put these events on the timeline"
        )
