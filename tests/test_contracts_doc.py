"""AGENTS.md names the tests that hold its rules up, and every one must exist.

A contract whose named test no longer exists is one refactor away from quietly
becoming untrue -- the document itself says so. The same goes for every other
section: "copy this test" pointing at a test nobody ever wrote sends the next
reader hunting for a shape that is not there. So this reads every test cited
anywhere in AGENTS.md (CLAUDE.md is a symlink to it) -- Python ``test_*``
functions, Go ``Test*`` functions, ``tests/`` files and Go ``*_test.go`` files
-- and requires each to exist, so a rename breaks the document rather than the
rule. The extractor is the one C12's living audit grades with
(``scripts/contract_audit.py``), so the test and the grade cannot disagree.
"""

from __future__ import annotations

import re

from scripts.contract_audit import (
    AGENTS_MD,
    cited_test_names,
    cited_test_paths,
    dangling_test_citations,
    defined_test_names,
)


def _document() -> str:
    return AGENTS_MD.read_text(encoding="utf-8")


def _contracts_section(text: str) -> str:
    start = text.index("## The contracts")
    end = text.index("\n## ", start + 10)
    # Markdown wraps prose at ~95 columns, so "*Held up\n  by*" is one phrase.
    return re.sub(r"\n[ \t]+", " ", text[start:end])


def test_the_extractor_finds_python_go_and_file_citations() -> None:
    sample = (
        "copy `test_a_view_reads_null` and run `go test` for `TestBudget`; "
        "see `tests/test_timeline.py`, also test_timeline.py, `tests/local_test_runtime.py`, "
        "`app/internal/guide/guide_test.go`, `guide_test.go`, `postgres.py`, `.pytest_cache` and "
        "`tests/test_repo_contracts.py::test_every_token`."
    )
    assert cited_test_names(sample) == {"test_a_view_reads_null", "TestBudget", "test_every_token"}
    assert cited_test_paths(sample) == {
        "tests/test_timeline.py",
        "test_timeline.py",
        "tests/local_test_runtime.py",
        "app/internal/guide/guide_test.go",
        "guide_test.go",
        "tests/test_repo_contracts.py",
    }


def test_a_cited_path_must_exist_at_that_path_not_just_by_name() -> None:
    assert dangling_test_citations("`tests/test_timeline.py` and `test_timeline.py`") == []
    assert dangling_test_citations("`tests/unit/test_timeline.py`") == ["tests/unit/test_timeline.py"]
    assert dangling_test_citations("`tests/no_such_helper.py`") == ["tests/no_such_helper.py"]
    assert dangling_test_citations("`app/internal/no_such_test.go`") == ["app/internal/no_such_test.go"]
    assert dangling_test_citations("`no_such_test.go`") == ["no_such_test.go"]


def test_the_definition_scan_sees_class_methods_and_go_tests() -> None:
    defined = defined_test_names()
    assert "test_every_test_agents_md_cites_exists" in defined
    # A method of tests/test_search_benchmark.py's TestPredicateMatching class.
    assert "test_source_predicate_accepts_listed_source" in defined
    assert "TestOverviewGuidanceStaysWithinBudget" in defined


def test_every_test_agents_md_cites_exists() -> None:
    text = _document()
    contracts = _contracts_section(text)
    assert len(cited_test_names(contracts)) + len(cited_test_paths(contracts)) >= 8, (
        "the contracts section should name the tests that hold it up"
    )
    missing = dangling_test_citations(text)
    assert missing == [], (
        "AGENTS.md cites tests that do not exist; a renamed test must rename its mention, "
        "or the rule it holds up is unenforced without anyone noticing, and a 'copy this "
        f"test' pointer sends the reader after a shape that is not there: {missing}"
    )


def test_every_contract_names_what_holds_it_up() -> None:
    section = _contracts_section(_document())
    contracts = re.findall(r"^- \*\*C(\d+) — ", section, re.M)
    numbers = [int(n) for n in contracts]
    # C1-C12 are the founding contracts. Later ones keep the number the
    # 2026-10-27 product vision gave them (C13-C22) and land in any order, so
    # the list is ascending with gaps rather than contiguous.
    assert numbers == sorted(set(numbers)), numbers
    assert set(range(1, 13)) <= set(numbers), numbers
    for block in re.split(r"^- \*\*C\d+ — ", section, flags=re.M)[1:]:
        assert "*Held up by*" in block, block[:80]
