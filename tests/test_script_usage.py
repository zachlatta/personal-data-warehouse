import json

from personal_data_warehouse.agent_usage import AgentUsageCollector, AgentFailureShapeCollector
from tests.test_agent_usage import _event
from tests.test_postgres_warehouse import warehouse  # noqa: F401


def test_script_inner_search_opens_session_and_caught_sql_error_counts(warehouse):
    warehouse.ensure_agent_sessions_tables()
    warehouse.ensure_pipeline_health_tables()
    run_id = "a" * 32
    operations = [{"method": "search", "priorities": ["self"],
                   "returned_priority_counts": {"self": 1}},
                  {"method": "sql", "error": {"code": "operation_error", "message": "SQLSTATE 42703"}}]
    warehouse._command("""INSERT INTO @script_runs (occurred_at,run_id,operations_json,outcome)
        VALUES (now(),%s,%s,'ok')""", (run_id, json.dumps(operations)))
    warehouse.insert_agent_session_events([
        _event("script-session", 1, tool="mcp__personal_data_warehouse__run", inp='{"code":"test"}'),
        _event("script-session", 2, subtype="tool_result", res=json.dumps({"run_id": run_id})),
    ])
    snapshots = {r.source: r for r in AgentUsageCollector(warehouse).run()}
    all_sources = snapshots["all"]
    assert all_sources.first_search == 1
    assert all_sources.search_calls == 1
    assert all_sources.search_with_priority == 1
    assert all_sources.sql_calls == 1
    assert all_sources.sql_error_sessions == 1
    failures = AgentFailureShapeCollector(warehouse).run()
    assert any(f.sqlstate == "42703" for f in failures)
