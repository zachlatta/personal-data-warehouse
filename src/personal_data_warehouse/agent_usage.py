"""How agents actually use PDW, measured from their own transcripts.

Contract C3 says an agent starts at the timeline (the ``search`` tool or SQL
over ``timeline.events``) and scopes by priority tier. The surfaces honor it;
whether agents DO is only knowable from the sessions PDW already indexes.
Measured by hand on 2026-08-26 over 14 days: 27% of PDW sessions opened with a
search, 51% with schema discovery; the priorities filter was on 6% of search
calls; 1,034 SQL calls went straight to ``base_*`` with no timeline reference.
A one-off audit is a number that decays. This collector takes the same
measurement daily into ``ops.agent_usage`` so ``marts_ops.agent_usage`` and
``/pipelines`` show whether the guidance is landing.

A PDW call is an MCP tool named ``*Personal_Data_Warehouse*`` or a shell tool
whose input **invokes** ``pdw <subcommand>`` (codex script-mode calls carry
their argv in ``raw_json``); the first such READ call per session decides
``search`` / ``schema`` / ``sql`` / ``invented``.

Two things about that sentence are load-bearing, and the first version of this
collector got both wrong:

- **"Invokes" is not "mentions".** The tool input is the tool's JSON, so a bare
  ``pdw `` substring also matches Claude Code's ``description`` field, a
  pgBackRest ``--stanza=pdw info``, a ``which pdw hcb``, and a filename with
  ``pdw`` in it. Measured 2026-08-28, that counted 18 of 284 sessions that
  never ran the CLI, and it decided their opener from prose. ``pdw`` now has to
  sit at a shell command position and be followed by a real subcommand -- and
  a command-position ``pdw`` followed by anything else is ``invented``, which
  is exactly what the CLI itself answers with ``unknown command``.
- **Admin calls are not questions.** ``pdw ingest``, ``pdw version``,
  ``pdw login`` and the credential publishers say nothing about where an agent
  starts a *question*, so they are counted in ``admin_calls`` and excluded from
  the denominator and from the first-call decision. A session that only ran an
  uploader is not a session that failed to start at the timeline.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg2

from personal_data_warehouse.warehouse_catalog import CATALOG

logger = logging.getLogger(__name__)

_REAL_PRIORITY_TIERS = tuple(tier.name for tier in CATALOG.timeline_priorities.tiers)
_ATTENTION_PRIORITY_TIERS = CATALOG.timeline_priorities.attention_priorities
_LOWER_PRIORITY_TIERS = tuple(
    priority for priority in _REAL_PRIORITY_TIERS if priority not in _ATTENTION_PRIORITY_TIERS
)
_ACCEPTED_PRIORITIES = {*_REAL_PRIORITY_TIERS, CATALOG.timeline_priorities.sentinel.name}

#: Window measured per row. Two weeks is enough sessions to make a rate mean
#: something and short enough that a guidance change shows within days.
AGENT_USAGE_WINDOW_DAYS = 14
AGENT_USAGE_STATEMENT_TIMEOUT_MS = 240_000

#: Targets the view judges against. Below any of them the row reads
#: ``attention``: the guidance is not landing and the skill/tool text needs work.
SEARCH_FIRST_TARGET = 0.6
PRIORITY_FILTER_TARGET = 0.4
SQL_ERROR_SESSION_CEILING = 0.1

#: The pdw CLI's real subcommands, split by whether they are a question.
#: Kept in the same order the dispatcher lists them so a new one is easy to add;
#: app/cmd/pdw-cli/usage_test.go is what keeps that list honest on the Go side.
PDW_READ_SUBCOMMANDS = ("search", "sql", "schema", "columns", "call", "list", "describe")
PDW_ADMIN_SUBCOMMANDS = (
    "ingest", "login", "logout", "config", "chatgpt", "slack", "whoop", "version", "update",
    # run() accepts all three spellings of help before it dispatches.
    "--help", "-h", "help",
)

#: `pdw` as the invoked binary rather than the three letters. A command position
#: is the start of the string (which in a tool's JSON is right after
#: `"command":"`), a shell separator, or a JSON/quote boundary; a space is
#: deliberately NOT one, which is what excludes "Read pdw skill",
#: "--stanza=pdw info" and "which pdw hcb".
#: A real newline (a shell separator) and the two characters a JSON-encoded
#: command carries in its place are both command boundaries.
_BOUNDARY = '(^|["`;|&(' + chr(10) + ']|' + chr(92) + chr(92) + "n)"
_CLI_BEFORE = _BOUNDARY + '[[:space:]]*(sudo[[:space:]]+)?pdw[[:space:]]+'
#: The subcommand must end at a token boundary. Not just whitespace: a command
#: inside a shell quote ends at `'`, and inside JSON at `\\`, so a
#: whitespace-only boundary read `command -v pdw && pdw schema'` as an unknown
#: subcommand and reported it invented.
_CLI_AFTER = '([^A-Za-z0-9_-]|$)'


def _cli_re(alternation: str) -> str:
    return _CLI_BEFORE + "(" + alternation + ")" + _CLI_AFTER


#: A command-position `pdw` whose subcommand is not one the CLI has. This is the
#: `unknown command` path, plus `pdw --version` (the flag form that fails) and
#: the fenced `pdw call sql|query|search|...` redirects.
_KNOWN = "|".join(PDW_READ_SUBCOMMANDS + PDW_ADMIN_SUBCOMMANDS)
#: The typed token must also START like a subcommand -- lowercase or a flag.
#: Prose is the residual limit here: the input is the tool's JSON, so a heredoc
#: that writes documentation can put `pdw` at the start of a line. Requiring a
#: lowercase token removes "pdw CLI Full Disk Access" and its kind; a lowercase
#: English word after `pdw` at a command boundary still counts, which measured
#: ~5% of the invented total on 2026-08-28. Read invented_calls as a floor with
#: a small prose tail, not as an exact count.
CLI_INVENTED_RE = (
    _CLI_BEFORE + r"(?!(" + _KNOWN + r")" + _CLI_AFTER + r")[-a-z]"
    + "|"
    + _cli_re("call") + r"(sql|query|search|schema_overview|describe_table)" + _CLI_AFTER
)
CLI_SEARCH_RE = _cli_re("search")
CLI_SQL_RE = _cli_re("sql")
CLI_SCHEMA_RE = _cli_re("schema|columns")
CLI_OTHER_READ_RE = _cli_re("call|list|describe")
CLI_ADMIN_RE = _cli_re("|".join(PDW_ADMIN_SUBCOMMANDS))
#: Any real invocation at all -- what makes a session a PDW session.
CLI_INVOKED_RE = _CLI_BEFORE


@dataclass(frozen=True)
class AgentUsageSnapshot:
    """One source's PDW usage over the window (``all`` is every source)."""

    source: str
    window_days: int
    sessions: int
    pdw_sessions: int
    first_search: int
    first_schema: int
    first_sql: int
    first_invented: int
    search_calls: int
    search_with_priority: int
    search_attention_only: int
    search_including_lower_tiers: int
    search_noop_priority: int
    search_invalid_or_failed_priority: int
    bulk_hints_shown: int
    bulk_hint_scoped_retries: int
    bulk_hint_improved_retries: int
    sql_calls: int
    sql_base_only: int
    sql_error_sessions: int
    sql_timeouts: int
    invented_calls: int
    admin_calls: int
    newest_session_at: datetime | None


@dataclass(frozen=True)
class SearchCallObservation:
    """The effective outcome of one search call, not merely its syntax."""

    source: str
    session_id: str
    seq: int
    occurred_at: datetime | None
    query: str
    explicit_filter: bool
    priorities: tuple[str, ...]
    success: bool
    returned_priority_counts: dict[str, int]
    bulk_hint_shown: bool

    @property
    def valid_filter(self) -> bool:
        return all(priority in _ACCEPTED_PRIORITIES for priority in self.priorities)

    @property
    def noop_filter(self) -> bool:
        """An explicitly empty or all-five scope that does not narrow real tiers."""
        if not self.explicit_filter or not self.valid_filter:
            return False
        return not self.priorities or set(_REAL_PRIORITY_TIERS) <= set(self.priorities)

    @property
    def effective_filter(self) -> bool:
        return (
            self.explicit_filter
            and self.success
            and self.valid_filter
            and bool(self.priorities)
            and not self.noop_filter
        )

    @property
    def attention_only(self) -> bool:
        return self.effective_filter and set(self.priorities) <= set(_ATTENTION_PRIORITY_TIERS)

    @property
    def includes_lower_tiers(self) -> bool:
        return self.effective_filter and bool(set(self.priorities) & set(_LOWER_PRIORITY_TIERS))

    @property
    def invalid_or_failed_filter(self) -> bool:
        return self.explicit_filter and (not self.valid_filter or not self.success)

    @property
    def lower_tier_share(self) -> float | None:
        total = sum(self.returned_priority_counts.values())
        if total <= 0:
            return None
        lower = sum(
            self.returned_priority_counts.get(name, 0) for name in _LOWER_PRIORITY_TIERS
        )
        return lower / total


@dataclass(frozen=True)
class SearchUsageMetrics:
    search_calls: int = 0
    search_with_priority: int = 0
    search_attention_only: int = 0
    search_including_lower_tiers: int = 0
    search_noop_priority: int = 0
    search_invalid_or_failed_priority: int = 0
    bulk_hints_shown: int = 0
    bulk_hint_scoped_retries: int = 0
    bulk_hint_improved_retries: int = 0


_CLI_PRIORITY_RE = re.compile(
    r"--priorit(?:y|ies)(?:=|\s+)(?:\"([^\"]*)\"|'([^']*)'|([^\s\"';&|]+))",
    re.IGNORECASE,
)
_CLI_SEARCH_QUERY_RE = re.compile(r'^Search:\s*["\'](.*?)["\']\s*[—-]', re.MULTILINE)
_CLI_PRIORITY_COUNTS_RE = re.compile(r"^Returned priorities:\s*(.*)$", re.MULTILINE)


def _walk_json(value: Any, *, depth: int = 0):
    """Yield nested JSON containers, decoding text envelopes along the way."""
    if depth > 8:
        return
    if isinstance(value, str):
        stripped = value.strip()
        if stripped and stripped[0] in "[{\"":
            try:
                decoded = json.loads(stripped)
            except (json.JSONDecodeError, TypeError):
                return
            if decoded != value:
                yield from _walk_json(decoded, depth=depth + 1)
        return
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_json(child, depth=depth + 1)
    elif isinstance(value, list):
        yield value
        for child in value:
            yield from _walk_json(child, depth=depth + 1)


def _nested_strings(value: Any) -> list[str]:
    strings: list[str] = []
    if isinstance(value, str):
        strings.append(value)
        stripped = value.strip()
        if stripped and stripped[0] in "[{\"":
            try:
                decoded = json.loads(stripped)
            except (json.JSONDecodeError, TypeError):
                return strings
            if decoded != value:
                strings.extend(_nested_strings(decoded))
    elif isinstance(value, dict):
        for child in value.values():
            strings.extend(_nested_strings(child))
    elif isinstance(value, list):
        for child in value:
            strings.extend(_nested_strings(child))
    return strings


def _as_priorities(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    values = value if isinstance(value, list) else str(value).split(",")
    return tuple(str(item).strip().lower() for item in values if str(item).strip())


def _input_scope(raw: str, *, is_mcp: bool) -> tuple[bool, tuple[str, ...]]:
    if is_mcp:
        for value in _walk_json(raw):
            if isinstance(value, dict) and "priorities" in value:
                return True, _as_priorities(value.get("priorities"))
        return False, ()

    command_texts: list[str] = []
    for value in _walk_json(raw):
        if not isinstance(value, dict):
            continue
        for key in ("command", "cmd"):
            if isinstance(value.get(key), str):
                command_texts.append(value[key])
    if not command_texts:
        command_texts = [raw]
    selected: list[str] = []
    explicit = False
    for command in command_texts:
        if not re.search(r"(?:^|[\"`;|&(\n])\s*pdw\s+search(?:\s|$)", command):
            continue
        for match in _CLI_PRIORITY_RE.finditer(command):
            explicit = True
            value = next((group for group in match.groups() if group is not None), "")
            selected.extend(_as_priorities(value))
        if explicit:
            break
    return explicit, tuple(selected)


def _response_dict(raw: str) -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []
    for value in _walk_json(raw):
        if not isinstance(value, dict):
            continue
        if "priority_scope" in value or "returned_priority_counts" in value:
            return value
        if "query" in value and ("rows" in value or "total_rows" in value or "error" in value):
            candidates.append(value)
    return candidates[0] if candidates else None


def _response_counts(response: dict[str, Any] | None, text: str) -> dict[str, int]:
    if response is not None and isinstance(response.get("returned_priority_counts"), dict):
        return {
            str(name): int(count)
            for name, count in response["returned_priority_counts"].items()
            if isinstance(count, (int, float))
        }
    if response is not None and isinstance(response.get("rows"), list):
        counts: dict[str, int] = {}
        for row in response["rows"]:
            if isinstance(row, dict) and row.get("priority"):
                priority = str(row["priority"])
                counts[priority] = counts.get(priority, 0) + 1
        if counts:
            return counts
    match = _CLI_PRIORITY_COUNTS_RE.search(text)
    if not match:
        return {}
    counts = {}
    for name, count in re.findall(r"([A-Za-z_]+)\s*=\s*(\d+)", match.group(1)):
        counts[name.lower()] = int(count)
    return counts


def parse_search_call(row: dict[str, Any]) -> SearchCallObservation:
    """Parse request and result into the effective search-scope outcome."""
    raw_input = str(row.get("inp") or "")
    raw_result = str(row.get("result") or "")
    explicit, priorities = _input_scope(raw_input, is_mcp=bool(row.get("is_mcp")))
    response = _response_dict(raw_result)
    text = "\n".join(_nested_strings(raw_result))

    failed_envelope = any(
        isinstance(value, dict) and value.get("isError") is True
        for value in _walk_json(raw_result)
    )
    if response is not None:
        scope = str(response.get("priority_scope") or "")
        reported_priorities = _as_priorities(response.get("selected_priorities"))
        if not explicit and scope in {"selected", "invalid"}:
            explicit = True
            priorities = reported_priorities
        success = not failed_envelope and not response.get("error") and scope != "invalid"
        # New responses echo the scope that actually ran. When present, make
        # it part of success rather than crediting a syntactically scoped call
        # whose result quietly came from another scope. Older responses had no
        # echo and retain their historical success detection.
        if scope:
            if explicit and priorities:
                success = (
                    success
                    and scope == "selected"
                    and reported_priorities == priorities
                )
            elif explicit:
                success = success and scope == "all" and not reported_priorities
            else:
                success = success and scope == "all" and not reported_priorities
        query = str(response.get("query") or "").strip()
        codes = response.get("hint_codes")
        bulk_hint = isinstance(codes, list) and "consider_attention_scope" in codes
    else:
        # Older CLI results predate the explicit Scope line but the Search
        # header was already emitted only after a successful API response.
        success = bool(re.search(r"^Search:", text, re.MULTILINE))
        query_match = _CLI_SEARCH_QUERY_RE.search(text)
        query = query_match.group(1).strip() if query_match else ""
        bulk_hint = False
    lowered = text.lower()
    bulk_hint = bulk_hint or "consider_attention_scope" in lowered or (
        "most of these hits" in lowered and "noise/background" in lowered
    )
    return SearchCallObservation(
        source=str(row.get("source") or ""),
        session_id=str(row.get("session_id") or ""),
        seq=int(row.get("seq") or 0),
        occurred_at=row.get("occurred_at"),
        query=query,
        explicit_filter=explicit,
        priorities=priorities,
        success=success,
        returned_priority_counts=_response_counts(response, text),
        bulk_hint_shown=bulk_hint,
    )


def analyze_search_calls(rows: list[dict[str, Any]]) -> SearchUsageMetrics:
    observations = [parse_search_call(row) for row in rows]
    hints = sum(observation.bulk_hint_shown for observation in observations)
    retries = 0
    improved = 0
    by_session: dict[tuple[str, str], list[SearchCallObservation]] = {}
    for observation in observations:
        by_session.setdefault((observation.source, observation.session_id), []).append(observation)
    for calls in by_session.values():
        calls.sort(key=lambda call: call.seq)
        for before, after in zip(calls, calls[1:], strict=False):
            if not before.bulk_hint_shown or not after.effective_filter:
                continue
            if before.query and after.query and before.query.casefold() != after.query.casefold():
                continue
            retries += 1
            old_share = before.lower_tier_share
            new_share = after.lower_tier_share
            if old_share is not None and new_share is not None and new_share < old_share:
                improved += 1
    return SearchUsageMetrics(
        search_calls=len(observations),
        search_with_priority=sum(observation.effective_filter for observation in observations),
        search_attention_only=sum(observation.attention_only for observation in observations),
        search_including_lower_tiers=sum(
            observation.includes_lower_tiers for observation in observations
        ),
        search_noop_priority=sum(observation.noop_filter for observation in observations),
        search_invalid_or_failed_priority=sum(
            observation.invalid_or_failed_filter for observation in observations
        ),
        bulk_hints_shown=hints,
        bulk_hint_scoped_retries=retries,
        bulk_hint_improved_retries=improved,
    )


_AGENT_USAGE_CALLS_CTE = """
WITH ev AS (
  SELECT source, session_id, seq, occurred_at, tool_name,
         CASE WHEN source = 'codex' AND subtype = 'custom_tool_call' THEN raw_json ELSE tool_input_json END AS inp0,
         CASE WHEN source = 'codex' THEN raw_json ELSE tool_result_json END AS res0
  FROM @ai_conversation_events
  WHERE occurred_at >= now() - make_interval(days => %(days)s)
),
sessions AS (
  SELECT source, session_id, max(occurred_at) AS newest FROM ev GROUP BY source, session_id
),
ev2 AS (
  SELECT *, lead(res0) OVER (PARTITION BY source, session_id ORDER BY seq) AS next_res FROM ev
),
calls AS (
  SELECT source, session_id, seq, occurred_at, tool_name, inp0 AS inp,
         coalesce(NULLIF(next_res, ''), res0) AS result,
         (tool_name ILIKE '%%personal_data_warehouse%%') AS is_mcp
  FROM ev2
  WHERE tool_name ILIKE '%%personal_data_warehouse%%'
     OR (tool_name IN ('Bash', 'bash', 'shell', 'exec_command', 'container.exec', 'exec')
         AND inp0 ~ '{invoked}')
     OR (source = 'codex' AND inp0 ~ '{invoked}')
),
pdw AS (
  SELECT *,
    CASE
      WHEN NOT is_mcp AND inp ~ '{invented}' THEN 'invented'
      WHEN tool_name ILIKE '%%__search' OR tool_name ILIKE '%%.search'
        OR (NOT is_mcp AND inp ~ '{search}') THEN 'search'
      WHEN tool_name ILIKE '%%__query' OR tool_name ILIKE '%%.query'
        OR (NOT is_mcp AND inp ~ '{sql}') THEN 'sql'
      WHEN tool_name ILIKE '%%schema_overview' OR tool_name ILIKE '%%describe_table'
        OR (NOT is_mcp AND inp ~ '{schema}') THEN 'schema'
      WHEN NOT is_mcp AND inp ~ '{admin}' THEN 'admin'
      -- Every remaining MCP tool (get_rows, grep_rows, get_field, get_object,
      -- notify, propose_mutation) and `pdw call|list|describe` is a warehouse
      -- call that is not one of the three named entry points.
      ELSE 'other_read'
    END AS kind
  FROM calls
)
"""


AGENT_USAGE_SQL = _AGENT_USAGE_CALLS_CTE + """,
reads AS (
  SELECT *, row_number() OVER (PARTITION BY source, session_id ORDER BY seq) AS nth
  FROM pdw WHERE kind <> 'admin'
),
per_session AS (
  SELECT source, session_id,
         max(CASE WHEN nth = 1 AND kind = 'search' THEN 1 ELSE 0 END) AS first_search,
         max(CASE WHEN nth = 1 AND kind = 'schema' THEN 1 ELSE 0 END) AS first_schema,
         max(CASE WHEN nth = 1 AND kind = 'sql' THEN 1 ELSE 0 END) AS first_sql,
         max(CASE WHEN nth = 1 AND kind = 'invented' THEN 1 ELSE 0 END) AS first_invented,
         count(*) FILTER (WHERE kind = 'search') AS search_calls,
         count(*) FILTER (WHERE kind = 'sql') AS sql_calls,
         count(*) FILTER (WHERE kind = 'sql' AND inp ~* 'base_[a-z0-9_]+[.]'
                            AND inp !~* 'timeline[.]' AND inp !~* 'marts_') AS sql_base_only,
         max(CASE WHEN kind = 'sql' AND result ~ '(42703|42P01|42883|42501|57014)' THEN 1 ELSE 0 END)
             AS sql_error_session,
         count(*) FILTER (WHERE kind = 'sql' AND result ~ '57014') AS sql_timeouts,
         count(*) FILTER (WHERE kind = 'invented') AS invented_calls
  FROM reads
  GROUP BY source, session_id
),
admin AS (
  SELECT source, session_id, count(*) AS admin_calls
  FROM pdw WHERE kind = 'admin' GROUP BY source, session_id
),
by_source AS (
  SELECT s.source,
         count(*) AS sessions,
         count(p.session_id) AS pdw_sessions,
         coalesce(sum(p.first_search), 0) AS first_search,
         coalesce(sum(p.first_schema), 0) AS first_schema,
         coalesce(sum(p.first_sql), 0) AS first_sql,
         coalesce(sum(p.first_invented), 0) AS first_invented,
         coalesce(sum(p.search_calls), 0) AS search_calls,
         coalesce(sum(p.sql_calls), 0) AS sql_calls,
         coalesce(sum(p.sql_base_only), 0) AS sql_base_only,
         coalesce(sum(p.sql_error_session), 0) AS sql_error_sessions,
         coalesce(sum(p.sql_timeouts), 0) AS sql_timeouts,
         coalesce(sum(p.invented_calls), 0) AS invented_calls,
         coalesce(sum(a.admin_calls), 0) AS admin_calls,
         max(s.newest) AS newest_session_at
  FROM sessions s
  LEFT JOIN per_session p ON p.source = s.source AND p.session_id = s.session_id
  LEFT JOIN admin a ON a.source = s.source AND a.session_id = s.session_id
  GROUP BY s.source
)
SELECT * FROM by_source
UNION ALL
SELECT 'all', sum(sessions), sum(pdw_sessions), sum(first_search), sum(first_schema), sum(first_sql),
       sum(first_invented), sum(search_calls), sum(sql_calls),
       sum(sql_base_only), sum(sql_error_sessions), sum(sql_timeouts), sum(invented_calls),
       sum(admin_calls), max(newest_session_at)
FROM by_source
"""


# Use the exact same call classifier for the outcome-aware search pass. The
# aggregate above deliberately does not inspect priority syntax at all; only
# the request/result pair below can tell whether a scope was valid, successful,
# and actually narrower than the default.
AGENT_USAGE_SEARCH_CALLS_SQL = _AGENT_USAGE_CALLS_CTE + """
SELECT source, session_id, seq, occurred_at, tool_name, inp, result, is_mcp
FROM pdw
WHERE kind = 'search'
ORDER BY source, session_id, seq
"""


def _expand_agent_usage_regexes(statement: str) -> str:
    return (
        statement.replace("{invoked}", CLI_INVOKED_RE)
        .replace("{invented}", CLI_INVENTED_RE)
        .replace("{search}", CLI_SEARCH_RE)
        .replace("{sql}", CLI_SQL_RE)
        .replace("{schema}", CLI_SCHEMA_RE)
        .replace("{admin}", CLI_ADMIN_RE)
    )


AGENT_USAGE_SQL = _expand_agent_usage_regexes(AGENT_USAGE_SQL)
AGENT_USAGE_SEARCH_CALLS_SQL = _expand_agent_usage_regexes(AGENT_USAGE_SEARCH_CALLS_SQL)


class AgentUsageCollector:
    def __init__(self, warehouse: Any, *, window_days: int = AGENT_USAGE_WINDOW_DAYS) -> None:
        self._warehouse = warehouse
        self._window_days = int(window_days)

    def collect(self) -> list[AgentUsageSnapshot]:
        self._warehouse._raw_command(f"SET statement_timeout = {AGENT_USAGE_STATEMENT_TIMEOUT_MS}")
        try:
            rows = self._warehouse._query_dicts(AGENT_USAGE_SQL, {"days": self._window_days})
            search_rows = self._warehouse._query_dicts(
                AGENT_USAGE_SEARCH_CALLS_SQL, {"days": self._window_days}
            )
        finally:
            self._warehouse._raw_command("SET statement_timeout = DEFAULT")
        out: list[AgentUsageSnapshot] = []
        for row in rows:
            if row.get("sessions") is None:
                continue
            source = str(row["source"])
            scoped_rows = (
                search_rows
                if source == "all"
                else [search_row for search_row in search_rows if str(search_row["source"]) == source]
            )
            search = analyze_search_calls(scoped_rows)
            out.append(
                AgentUsageSnapshot(
                    source=source,
                    window_days=self._window_days,
                    sessions=int(row["sessions"] or 0),
                    pdw_sessions=int(row["pdw_sessions"] or 0),
                    first_search=int(row["first_search"] or 0),
                    first_schema=int(row["first_schema"] or 0),
                    first_sql=int(row["first_sql"] or 0),
                    first_invented=int(row["first_invented"] or 0),
                    search_calls=search.search_calls,
                    search_with_priority=search.search_with_priority,
                    search_attention_only=search.search_attention_only,
                    search_including_lower_tiers=search.search_including_lower_tiers,
                    search_noop_priority=search.search_noop_priority,
                    search_invalid_or_failed_priority=search.search_invalid_or_failed_priority,
                    bulk_hints_shown=search.bulk_hints_shown,
                    bulk_hint_scoped_retries=search.bulk_hint_scoped_retries,
                    bulk_hint_improved_retries=search.bulk_hint_improved_retries,
                    sql_calls=int(row["sql_calls"] or 0),
                    sql_base_only=int(row["sql_base_only"] or 0),
                    sql_error_sessions=int(row["sql_error_sessions"] or 0),
                    sql_timeouts=int(row["sql_timeouts"] or 0),
                    invented_calls=int(row["invented_calls"] or 0),
                    admin_calls=int(row["admin_calls"] or 0),
                    newest_session_at=row["newest_session_at"],
                )
            )
        return out

    def run(self) -> list[AgentUsageSnapshot]:
        try:
            snapshots = self.collect()
        except psycopg2.Error as error:
            logger.warning("agent usage aggregate failed: %s", error)
            raise
        self._warehouse.write_agent_usage(snapshots, collected_at=self._warehouse_now())
        return snapshots

    def _warehouse_now(self) -> datetime:
        from datetime import UTC

        return datetime.now(tz=UTC)
