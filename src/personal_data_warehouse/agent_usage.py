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

Classification is the audit's, unchanged: a PDW call is an MCP tool named
``*Personal_Data_Warehouse*`` or a shell tool whose input runs ``pdw <cmd>``
(codex script-mode calls carry their argv in ``raw_json``); the first such call
per session decides ``search`` / ``schema`` / ``sql`` / ``invented``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg2

logger = logging.getLogger(__name__)

#: Window measured per row. Two weeks is enough sessions to make a rate mean
#: something and short enough that a guidance change shows within days.
AGENT_USAGE_WINDOW_DAYS = 14
AGENT_USAGE_STATEMENT_TIMEOUT_MS = 240_000

#: Targets the view judges against. Below any of them the row reads
#: ``attention``: the guidance is not landing and the skill/tool text needs work.
SEARCH_FIRST_TARGET = 0.6
PRIORITY_FILTER_TARGET = 0.4
SQL_ERROR_SESSION_CEILING = 0.1


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
    sql_calls: int
    sql_base_only: int
    sql_error_sessions: int
    sql_timeouts: int
    invented_calls: int
    newest_session_at: datetime | None


AGENT_USAGE_SQL = """
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
  SELECT source, session_id, seq, tool_name, inp0 AS inp,
         coalesce(NULLIF(next_res, ''), res0) AS result
  FROM ev2
  WHERE tool_name ILIKE '%%personal_data_warehouse%%'
     OR (tool_name IN ('Bash', 'bash', 'shell', 'exec_command', 'container.exec', 'exec')
         AND inp0 ~ '(^|[^A-Za-z0-9_/.-])pdw ')
     OR (source = 'codex' AND inp0 ~ '(^|[^A-Za-z0-9_/.-])pdw ')
),
pdw AS (
  SELECT *,
    CASE
      WHEN tool_name ILIKE '%%__search' OR tool_name ILIKE '%%.search'
        OR inp ~ 'pdw search' OR inp ~ 'pdw call search' THEN 'search'
      WHEN tool_name ILIKE '%%__query' OR tool_name ILIKE '%%.query' OR inp ~ 'pdw sql' THEN 'sql'
      WHEN tool_name ILIKE '%%schema_overview' OR tool_name ILIKE '%%describe_table'
        OR inp ~ 'pdw (schema|columns)' THEN 'schema'
      WHEN inp ~ 'pdw (query|schema_overview|call sql|call query|describe_table|--version)' THEN 'invented'
      ELSE 'other'
    END AS kind,
    row_number() OVER (PARTITION BY source, session_id ORDER BY seq) AS nth
  FROM calls
),
per_session AS (
  SELECT source, session_id,
         max(CASE WHEN nth = 1 AND kind = 'search' THEN 1 ELSE 0 END) AS first_search,
         max(CASE WHEN nth = 1 AND kind = 'schema' THEN 1 ELSE 0 END) AS first_schema,
         max(CASE WHEN nth = 1 AND kind = 'sql' THEN 1 ELSE 0 END) AS first_sql,
         max(CASE WHEN nth = 1 AND kind = 'invented' THEN 1 ELSE 0 END) AS first_invented,
         count(*) FILTER (WHERE kind = 'search') AS search_calls,
         count(*) FILTER (WHERE kind = 'search'
                            AND (inp ~ '--priorit' OR inp ~ '"priorities"')) AS search_with_priority,
         count(*) FILTER (WHERE kind = 'sql') AS sql_calls,
         count(*) FILTER (WHERE kind = 'sql' AND inp ~* 'base_[a-z0-9_]+[.]'
                            AND inp !~* 'timeline[.]' AND inp !~* 'marts_') AS sql_base_only,
         max(CASE WHEN kind = 'sql' AND result ~ '(42703|42P01|42883|42501|57014)' THEN 1 ELSE 0 END)
             AS sql_error_session,
         count(*) FILTER (WHERE kind = 'sql' AND result ~ '57014') AS sql_timeouts,
         count(*) FILTER (WHERE kind = 'invented') AS invented_calls
  FROM pdw
  GROUP BY source, session_id
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
         coalesce(sum(p.search_with_priority), 0) AS search_with_priority,
         coalesce(sum(p.sql_calls), 0) AS sql_calls,
         coalesce(sum(p.sql_base_only), 0) AS sql_base_only,
         coalesce(sum(p.sql_error_session), 0) AS sql_error_sessions,
         coalesce(sum(p.sql_timeouts), 0) AS sql_timeouts,
         coalesce(sum(p.invented_calls), 0) AS invented_calls,
         max(s.newest) AS newest_session_at
  FROM sessions s
  LEFT JOIN per_session p ON p.source = s.source AND p.session_id = s.session_id
  GROUP BY s.source
)
SELECT * FROM by_source
UNION ALL
SELECT 'all', sum(sessions), sum(pdw_sessions), sum(first_search), sum(first_schema), sum(first_sql),
       sum(first_invented), sum(search_calls), sum(search_with_priority), sum(sql_calls),
       sum(sql_base_only), sum(sql_error_sessions), sum(sql_timeouts), sum(invented_calls),
       max(newest_session_at)
FROM by_source
"""


class AgentUsageCollector:
    def __init__(self, warehouse: Any, *, window_days: int = AGENT_USAGE_WINDOW_DAYS) -> None:
        self._warehouse = warehouse
        self._window_days = int(window_days)

    def collect(self) -> list[AgentUsageSnapshot]:
        self._warehouse._raw_command(f"SET statement_timeout = {AGENT_USAGE_STATEMENT_TIMEOUT_MS}")
        try:
            rows = self._warehouse._query_dicts(AGENT_USAGE_SQL, {"days": self._window_days})
        finally:
            self._warehouse._raw_command("SET statement_timeout = DEFAULT")
        out: list[AgentUsageSnapshot] = []
        for row in rows:
            if row.get("sessions") is None:
                continue
            out.append(
                AgentUsageSnapshot(
                    source=str(row["source"]),
                    window_days=self._window_days,
                    sessions=int(row["sessions"] or 0),
                    pdw_sessions=int(row["pdw_sessions"] or 0),
                    first_search=int(row["first_search"] or 0),
                    first_schema=int(row["first_schema"] or 0),
                    first_sql=int(row["first_sql"] or 0),
                    first_invented=int(row["first_invented"] or 0),
                    search_calls=int(row["search_calls"] or 0),
                    search_with_priority=int(row["search_with_priority"] or 0),
                    sql_calls=int(row["sql_calls"] or 0),
                    sql_base_only=int(row["sql_base_only"] or 0),
                    sql_error_sessions=int(row["sql_error_sessions"] or 0),
                    sql_timeouts=int(row["sql_timeouts"] or 0),
                    invented_calls=int(row["invented_calls"] or 0),
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
