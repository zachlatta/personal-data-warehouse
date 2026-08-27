"""The eleven contracts in CLAUDE.md name the tests that hold them up.

A contract whose named test no longer exists is one refactor away from quietly
becoming untrue -- the document itself says so. This test reads every
backticked ``test_*`` name in the contracts section and requires a test
function of that name somewhere under ``tests/``, so a rename breaks the
document rather than the contract.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CLAUDE_MD = REPO_ROOT / "CLAUDE.md"
TESTS_DIR = REPO_ROOT / "tests"


def _contracts_section() -> str:
    text = CLAUDE_MD.read_text(encoding="utf-8")
    start = text.index("## The eleven contracts")
    end = text.index("\n## ", start + 10)
    # Markdown wraps prose at ~95 columns, so "*Held up\n  by*" is one phrase.
    return re.sub(r"\n[ \t]+", " ", text[start:end])


def _named_tests(section: str) -> set[str]:
    names = set()
    for match in re.finditer(r"`(?:[\w./-]+::)?(test_[A-Za-z0-9_]+)`", section):
        names.add(match.group(1))
    return names


def _named_test_files(section: str) -> set[str]:
    return set(re.findall(r"`(tests/test_[A-Za-z0-9_]+\.py)`", section))


def _defined_tests() -> set[str]:
    defined: set[str] = set()
    for path in TESTS_DIR.rglob("test_*.py"):
        defined.update(re.findall(r"^def (test_[A-Za-z0-9_]+)\(", path.read_text(encoding="utf-8"), re.M))
    for path in (REPO_ROOT / "app").rglob("*_test.go"):
        defined.update(re.findall(r"^func (Test[A-Za-z0-9_]+)\(", path.read_text(encoding="utf-8"), re.M))
    return defined


def test_every_test_named_by_a_contract_exists() -> None:
    section = _contracts_section()
    named = _named_tests(section)
    files = _named_test_files(section)
    assert len(named) + len(files) >= 8, "the contracts section should name the tests that hold it up"
    defined = _defined_tests()
    missing = sorted(name for name in named if name not in defined)
    missing += sorted(path for path in files if not (REPO_ROOT / path).exists())
    assert missing == [], (
        "CLAUDE.md's contracts name tests that no longer exist; a renamed test must rename "
        f"its mention or the contract is unenforced without anyone noticing: {missing}"
    )


def test_every_contract_names_what_holds_it_up() -> None:
    section = _contracts_section()
    contracts = re.findall(r"^- \*\*C(\d+) — ", section, re.M)
    assert [int(n) for n in contracts] == list(range(1, 12))
    for block in re.split(r"^- \*\*C\d+ — ", section, flags=re.M)[1:]:
        assert "*Held up by*" in block, block[:80]
