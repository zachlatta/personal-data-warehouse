"""AGENTS.md names the tests that hold its rules up, and every one must exist.

A contract whose named test no longer exists is one refactor away from quietly
becoming untrue -- the document itself says so. The same goes for every other
section: "copy this test" pointing at a test nobody ever wrote sends the next
reader hunting for a shape that is not there. So this reads every test name
cited anywhere in AGENTS.md (CLAUDE.md is a symlink to it) -- Python
``test_*`` functions, Go ``Test*`` functions and ``tests/*.py`` files -- and
requires each to be defined in the repository, so a rename breaks the document
rather than the rule.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
AGENTS_MD = REPO_ROOT / "AGENTS.md"
TESTS_DIR = REPO_ROOT / "tests"
APP_DIR = REPO_ROOT / "app"

# A test file path, with or without its tests/ directory: `tests/test_timeline.py`
# and the bare `test_timeline.py` the prose sometimes uses.
_TEST_FILE = re.compile(r"\b(?:tests/)?(test_[A-Za-z0-9_]+\.py)\b")
# A test function: Python `test_snake_case` or Go `TestCamelCase`. The lookahead
# keeps the stem of a file name (test_timeline in test_timeline.py) out.
_TEST_NAME = re.compile(r"\b(test_[A-Za-z0-9_]+|Test[A-Z][A-Za-z0-9_]*)\b(?!\.py)")


def _document() -> str:
    return AGENTS_MD.read_text(encoding="utf-8")


def _contracts_section(text: str) -> str:
    start = text.index("## The contracts")
    end = text.index("\n## ", start + 10)
    # Markdown wraps prose at ~95 columns, so "*Held up\n  by*" is one phrase.
    return re.sub(r"\n[ \t]+", " ", text[start:end])


def _cited_test_files(text: str) -> set[str]:
    return set(_TEST_FILE.findall(text))


def _cited_tests(text: str) -> set[str]:
    return set(_TEST_NAME.findall(text))


def _defined_tests() -> set[str]:
    defined: set[str] = set()
    for path in TESTS_DIR.rglob("*.py"):
        # Module-level functions and methods of test classes alike.
        defined.update(
            re.findall(r"^[ \t]*(?:async[ \t]+)?def[ \t]+(test_[A-Za-z0-9_]+)\(", path.read_text(encoding="utf-8"), re.M)
        )
    for path in APP_DIR.rglob("*_test.go"):
        defined.update(re.findall(r"^func (Test[A-Za-z0-9_]+)\(", path.read_text(encoding="utf-8"), re.M))
    return defined


def test_the_extractor_finds_python_go_and_file_citations() -> None:
    sample = (
        "copy `test_a_view_reads_null` and run `go test` for `TestBudget`; "
        "see `tests/test_timeline.py`, also test_timeline.py and "
        "`tests/test_repo_contracts.py::test_every_token`."
    )
    assert _cited_tests(sample) == {"test_a_view_reads_null", "TestBudget", "test_every_token"}
    assert _cited_test_files(sample) == {"test_timeline.py", "test_repo_contracts.py"}


def test_the_definition_scan_sees_class_methods_and_go_tests() -> None:
    defined = _defined_tests()
    assert "test_every_test_agents_md_cites_exists" in defined
    assert "TestOverviewGuidanceStaysWithinBudget" in defined


def test_every_test_agents_md_cites_exists() -> None:
    text = _document()
    contracts = _contracts_section(text)
    assert len(_cited_tests(contracts)) + len(_cited_test_files(contracts)) >= 8, (
        "the contracts section should name the tests that hold it up"
    )
    defined = _defined_tests()
    missing = sorted(name for name in _cited_tests(text) if name not in defined)
    missing += sorted(name for name in _cited_test_files(text) if not (TESTS_DIR / name).exists())
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
