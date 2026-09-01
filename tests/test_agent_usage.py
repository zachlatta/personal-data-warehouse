"""Contract C3, measured: how agents use PDW, from their own sessions."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from personal_data_warehouse.agent_usage import (
    AgentUsageCollector,
    AgentUsageSnapshot,
    analyze_search_calls,
    parse_search_call,
)
from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.schema import AGENT_SESSION_EVENT_COLUMNS
from tests.test_postgres_warehouse import _default_row, warehouse  # noqa: F401 - fixture


def _event(session: str, seq: int, *, source: str = "claude_code", tool: str = "", inp: str = "",
           res: str = "", subtype: str = "message", raw: str = "{}", when: datetime | None = None) -> dict:
    when = when or datetime.now(tz=UTC) - timedelta(hours=1)
    return _default_row(
        AGENT_SESSION_EVENT_COLUMNS,
        source=source, session_id=session, event_uuid=f"{session}-{seq}", account="z", device="porygon",
        seq=seq, occurred_at=when, role="assistant" if tool else "user", event_type="assistant",
        subtype=subtype, tool_name=tool, tool_input_json=inp, tool_result_json=res, raw_json=raw,
        ingested_at=when,
    )


def _seed(wh: PostgresWarehouse) -> None:
    wh.ensure_agent_sessions_tables()
    wh.ensure_pipeline_health_tables()
    wh.insert_agent_session_events(
        [
            # Session A: schema first, then SQL against base_* that errors, then a search with priorities.
            _event("a", 1, tool="Bash", inp='{"command":"pdw schema"}'),
            _event("a", 2, res="ok"),
            _event("a", 3, tool="Bash", inp='{"command":"pdw sql -q x \"SELECT * FROM base_slack.messages WHERE text ILIKE \'%x%\'\""}'),
            _event("a", 4, res="ERROR: column x does not exist (SQLSTATE 42703)"),
            _event("a", 5, tool="Bash", inp='{"command":"pdw search --priority self,direct \"budget\""}'),
            _event(
                "a", 6,
                res='Search: "budget" — 3 results (hybrid)\nScope: self, direct\n'
                    'Returned priorities: self=1, direct=2',
            ),
            # Session B: search first via MCP, no priorities.
            _event("b", 1, tool="mcp__claude_ai_Personal_Data_Warehouse__search", inp='{"query":"lease"}'),
            _event(
                "b", 2,
                res='{"query":"lease","priority_scope":"all","selected_priorities":[],"returned_priority_counts":{"noise":4,"direct":1},"hint_codes":["consider_attention_scope"],"suggested_priorities":["self","direct","cc"],"total_rows":5,"rows":[]}',
            ),
            # Session C: never touched PDW.
            _event("c", 1),
            # Session D (codex script mode): invented command first.
            _event("d", 1, source="codex", subtype="custom_tool_call",
                   raw='{"cmd":"pdw query \"SELECT 1\""}'),
            _event("d", 2, source="codex", raw='{"output":"unknown command"}'),
            # Session E: the three letters "pdw" appear, but nothing ran the CLI.
            # Every one of these was counted as a PDW session by the substring
            # matcher this replaced -- 18 of 284 sessions on 2026-08-28.
            _event("e", 1, tool="Bash",
                   inp='{"command":"sk read personal-data-warehouse","description":"Read pdw skill"}'),
            _event("e", 2, tool="Bash", inp='{"command":"pgbackrest --stanza=pdw info"}'),
            _event("e", 3, tool="Bash", inp='{"command":"which pdw hcb"}'),
            # Session F: an uploader run and a version check, then a search. The
            # admin calls are not evidence about where an agent starts a QUESTION,
            # so the first READ call is what decides the opener.
            _event("f", 1, tool="Bash", inp='{"command":"pdw ingest voice-memos --mode incremental"}'),
            _event("f", 2, res="uploaded 3"),
            _event("f", 3, tool="Bash", inp='{"command":"pdw version"}'),
            _event("f", 4, res="pdw 1.2.3"),
            _event("f", 5, tool="Bash", inp='{"command":"pdw search --priority self \"lease\""}'),
            _event(
                "f", 6,
                res='Search: "lease" — 2 results (hybrid)\nScope: self\n'
                    'Returned priorities: self=2',
            ),
            # Session G: only ever ran an uploader. Not a question, so not part of
            # the denominator of a metric about how questions start.
            _event("g", 1, tool="Bash", inp='{"command":"pdw ingest apple-notes"}'),
            _event("g", 2, res="ok"),
        ]
    )


def test_agent_usage_measures_first_call_priority_filter_and_base_only_sql(warehouse: PostgresWarehouse) -> None:
    _seed(warehouse)

    snapshots = {s.source: s for s in AgentUsageCollector(warehouse, window_days=14).run()}

    a = snapshots["all"]
    assert a.sessions == 7
    # Sessions A, B, D and F asked the warehouse something. C never touched PDW,
    # E only mentioned the three letters, and G only ran an uploader.
    assert a.pdw_sessions == 4
    assert (a.first_search, a.first_schema, a.first_sql, a.first_invented) == (2, 1, 0, 1)
    assert a.admin_calls == 3
    assert a.search_calls == 3
    assert a.search_with_priority == 2
    assert a.search_attention_only == 2
    assert a.search_including_lower_tiers == 0
    assert a.search_noop_priority == 0
    assert a.search_invalid_or_failed_priority == 0
    assert a.bulk_hints_shown == 1
    assert a.bulk_hint_scoped_retries == 0
    assert a.bulk_hint_improved_retries == 0
    assert a.sql_calls == 1
    assert a.sql_base_only == 1
    assert a.sql_error_sessions == 1
    assert a.invented_calls == 1
    assert snapshots["claude_code"].pdw_sessions == 3
    assert snapshots["codex"].first_invented == 1

    rows = {row["source"]: row for row in warehouse._query_dicts("SELECT * FROM @marts_agent_usage")}
    assert float(rows["all"]["priority_filter_rate"]) == round(2 / 3, 3)
    assert int(rows["all"]["search_attention_only"]) == 2
    assert int(rows["all"]["bulk_hints_shown"]) == 1
    assert float(rows["all"]["bulk_hint_retry_rate"]) == 0.0
    assert float(rows["all"]["sql_base_only_rate"]) == 1.0
    # Four PDW sessions is not a sample: the verdict withholds itself.
    assert rows["all"]["status"] == "no_data"


def test_agent_usage_view_judges_against_the_targets(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_pipeline_health_tables()
    now = datetime.now(tz=UTC)

    def snap(source: str, **over) -> AgentUsageSnapshot:
        base = dict(source=source, window_days=14, sessions=100, pdw_sessions=40, first_search=30,
                    first_schema=8, first_sql=2, first_invented=0, search_calls=50, search_with_priority=25,
                    search_attention_only=20, search_including_lower_tiers=5,
                    search_noop_priority=3, search_invalid_or_failed_priority=2,
                    bulk_hints_shown=8, bulk_hint_scoped_retries=5,
                    bulk_hint_improved_retries=4,
                    sql_calls=60, sql_base_only=10, sql_error_sessions=2, sql_timeouts=0, invented_calls=0,
                    admin_calls=4, newest_session_at=now)
        base.update(over)
        return AgentUsageSnapshot(**base)

    warehouse.write_agent_usage(
        [snap("all"), snap("codex", first_search=5), snap("pi", sessions=3, pdw_sessions=3, first_search=3)],
        collected_at=now,
    )
    rows = {row["source"]: row for row in warehouse._query_dicts("SELECT * FROM @marts_agent_usage")}
    assert rows["all"]["status"] == "ok"
    assert float(rows["all"]["search_first_rate"]) == 0.75
    assert float(rows["all"]["attention_scope_rate"]) == 0.4
    assert float(rows["all"]["bulk_hint_retry_rate"]) == 0.625
    assert float(rows["all"]["bulk_hint_improvement_rate"]) == 0.8
    assert rows["codex"]["status"] == "attention"  # 5/40 search-first
    assert rows["pi"]["status"] == "no_data"

    # The snapshot upsert is version-guarded on collected_at (an older write
    # never overwrites a newer one), so age the row in place instead.
    warehouse._command("UPDATE @agent_usage SET collected_at = %s", (now - timedelta(days=3),))
    warehouse.write_agent_usage([snap("all", newest_session_at=now)], collected_at=now - timedelta(days=3))
    rows = {row["source"]: row for row in warehouse._query_dicts("SELECT * FROM @marts_agent_usage")}
    assert rows["all"]["status"] == "unknown"
    assert set(rows) == {"all"}


def _search_row(inp: str, result: str, *, seq: int = 1, session: str = "s") -> dict:
    return {
        "source": "codex",
        "session_id": session,
        "seq": seq,
        "occurred_at": datetime.now(tz=UTC) + timedelta(seconds=seq),
        "tool_name": "Bash",
        "inp": inp,
        "result": result,
        "is_mcp": False,
    }


def test_search_scope_measurement_requires_an_effective_successful_filter() -> None:
    attention = parse_search_call(
        _search_row(
            '{"command":"pdw search --priority self,direct budget"}',
            'Search: "budget" — 3 results (hybrid)\nScope: self, direct\n'
            'Returned priorities: self=1, direct=2',
        )
    )
    assert attention.explicit_filter and attention.success
    assert attention.priorities == ("self", "direct")
    assert attention.effective_filter and attention.attention_only
    assert not attention.includes_lower_tiers
    assert not attention.noop_filter
    assert not attention.invalid_or_failed_filter

    lower = parse_search_call(
        _search_row(
            '{"command":"pdw search --priority noise,background alerts"}',
            'Search: "alerts" — 2 results (hybrid)\nScope: noise, background\n'
            'Returned priorities: noise=1, background=1',
        )
    )
    assert lower.effective_filter and lower.includes_lower_tiers
    assert not lower.attention_only


def test_empty_all_five_invalid_and_failed_filters_are_not_successes() -> None:
    success = '{"query":"q","priority_scope":"all","selected_priorities":[],"returned_priority_counts":{},"hint_codes":[],"suggested_priorities":[],"total_rows":0,"rows":[]}'
    empty = parse_search_call(
        {**_search_row('{"query":"q","priorities":[]}', success), "is_mcp": True}
    )
    assert empty.noop_filter and not empty.effective_filter

    all_five = parse_search_call(
        {**_search_row(
            '{"query":"q","priorities":["background","noise","cc","direct","self"]}',
            '{"query":"q","priority_scope":"selected","selected_priorities":["background","noise","cc","direct","self"],"returned_priority_counts":{},"hint_codes":[],"suggested_priorities":[],"total_rows":0,"rows":[]}',
        ), "is_mcp": True}
    )
    assert all_five.noop_filter and not all_five.effective_filter

    all_with_sentinel = parse_search_call(
        {**_search_row(
            '{"query":"q","priorities":["self","direct","cc","noise","background","unclassified"]}',
            '{"query":"q","priority_scope":"selected","selected_priorities":["self","direct","cc","noise","background","unclassified"],"returned_priority_counts":{},"hint_codes":[],"suggested_priorities":[],"total_rows":0,"rows":[]}',
        ), "is_mcp": True}
    )
    assert all_with_sentinel.noop_filter and not all_with_sentinel.effective_filter

    invalid = parse_search_call(
        {**_search_row(
            '{"query":"q","priorities":["urgent"]}',
            '{"query":"q","priority_scope":"invalid","selected_priorities":["urgent"],"returned_priority_counts":{},"hint_codes":[],"suggested_priorities":[],"total_rows":0,"error":"unknown priority"}',
        ), "is_mcp": True}
    )
    assert invalid.invalid_or_failed_filter and not invalid.effective_filter

    failed = parse_search_call(
        _search_row(
            '{"command":"pdw search --priority self q"}',
            'pdw search: connection reset',
        )
    )
    assert failed.invalid_or_failed_filter and not failed.effective_filter

    scope_mismatch = parse_search_call(
        {**_search_row(
            '{"query":"q","priorities":["self"]}',
            '{"query":"q","priority_scope":"all","selected_priorities":[],"returned_priority_counts":{},"hint_codes":[],"suggested_priorities":[],"total_rows":0,"rows":[]}',
        ), "is_mcp": True}
    )
    assert scope_mismatch.invalid_or_failed_filter
    assert not scope_mismatch.effective_filter


def test_bulk_hint_retry_is_counted_only_when_the_mix_improves() -> None:
    rows = [
        _search_row(
            '{"command":"pdw search budget"}',
            'Search: "budget" — 6 results (hybrid)\nScope: all tiers\n'
            'Returned priorities: direct=1, noise=4, background=1\n'
            'Hint: Most of these hits are noise/background; retry with priorities.',
            seq=1,
        ),
        _search_row(
            '{"command":"pdw search --priority self,direct,cc budget"}',
            'Search: "budget" — 3 results (hybrid)\nScope: self, direct, cc\n'
            'Returned priorities: self=1, direct=2',
            seq=2,
        ),
    ]
    metrics = analyze_search_calls(rows)
    assert metrics.search_calls == 2
    assert metrics.search_with_priority == 1
    assert metrics.bulk_hints_shown == 1
    assert metrics.bulk_hint_scoped_retries == 1
    assert metrics.bulk_hint_improved_retries == 1


# --- what counts as a PDW invocation (the C3 instrument's own contract) -------
#
# These are regex-level so the rules are readable without a database. Each case
# is a real shape taken from production transcripts on 2026-08-28.

import re

from personal_data_warehouse.agent_usage import (
    CLI_ADMIN_RE,
    CLI_INVENTED_RE,
    CLI_INVOKED_RE,
    CLI_SCHEMA_RE,
    CLI_SEARCH_RE,
)


def _pg_to_python(pattern: str) -> str:
    """The same pattern Postgres compiles, in Python's dialect."""
    return pattern.replace("[[:space:]]", r"[ \t\n]").replace("[^A-Za-z0-9_-]", r"[^A-Za-z0-9_-]")


def _matches(pattern: str, text: str) -> bool:
    return re.search(_pg_to_python(pattern), text) is not None


def test_mentioning_pdw_is_not_invoking_it():
    # The tool input is the tool's JSON, so the old bare `pdw ` substring matched
    # the description field, another program's flag, and a `which` probe.
    assert not _matches(CLI_INVOKED_RE, '{"command":"sk read x","description":"Read pdw skill"}')
    assert not _matches(CLI_INVOKED_RE, '{"command":"pgbackrest --stanza=pdw info"}')
    assert not _matches(CLI_INVOKED_RE, '{"command":"which pdw hcb"}')
    assert not _matches(CLI_INVOKED_RE, '{"command":"ls ~/dev/pdw-notes"}')


def test_a_real_invocation_is_recognised_wherever_the_shell_puts_it():
    assert _matches(CLI_SEARCH_RE, '{"command":"pdw search --priority self \'lease\'"}')
    assert _matches(CLI_SEARCH_RE, '{"command":"cd /tmp && pdw search x"}')
    assert _matches(CLI_SEARCH_RE, '{"command":"echo hi; pdw search x"}')
    # A JSON-encoded newline is a command separator too.
    assert _matches(CLI_SEARCH_RE, '{"command":"set -e\\npdw search x"}')
    assert _matches(CLI_SCHEMA_RE, '{"command":"/bin/bash -lc \'command -v pdw && pdw schema\'"}')
    assert _matches(CLI_ADMIN_RE, '{"command":"pdw ingest voice-memos --mode incremental"}')


def test_a_subcommand_the_cli_does_not_have_is_invented():
    # These are the shapes the CLI answers with `unknown command` or a redirect.
    assert _matches(CLI_INVENTED_RE, '{"command":"pdw query \\"SELECT 1\\""}')
    assert _matches(CLI_INVENTED_RE, '{"command":"pdw --version"}')
    # `pdw call <tool>` is fenced by runCall (C9: one obvious way per surface).
    # The old classifier only knew about `call sql`/`call query`, so the 81
    # `pdw call search` invocations in the fortnight to 2026-08-28 were counted
    # as ordinary calls rather than as the redirect they receive.
    assert _matches(CLI_INVENTED_RE, "{\"command\":\"pdw call search --data '{}'\"}")
    # A real subcommand inside shell quotes is not invented: the token boundary
    # is any non-word character, not only whitespace.
    assert not _matches(CLI_INVENTED_RE, '{"command":"/bin/bash -lc \'pdw schema\'"}')
    assert not _matches(CLI_INVENTED_RE, '{"command":"pdw --help | head"}')
    # Prose capitalised after `pdw` is documentation, not a command attempt.
    assert not _matches(CLI_INVENTED_RE, '{"command":"cat <<EOF\\npdw CLI Full Disk Access\\nEOF"}')
