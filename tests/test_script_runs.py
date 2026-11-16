"""The pilot has a public-safe telemetry surface and no C16 bypass."""
from personal_data_warehouse.postgres import POSTGRES_TABLES, _postgres_type
from personal_data_warehouse.relations import relation


def test_script_runs_catalog_and_types():
    assert relation("script_runs").schema == "ops"
    spec = POSTGRES_TABLES["script_runs"]
    assert spec.primary_key == ("run_id",)
    assert _postgres_type("inner_errors", table="script_runs") == "bigint"
    assert _postgres_type("operations_json", table="script_runs") == "text"
    assert relation("marts_script_runs").schema == "marts_ops"


def test_benchmark_proxy_can_forward_script_exclusions():
    from personal_data_warehouse.agent_tool_proxy import DEFAULT_AGENT_TOOL_ALLOWLIST
    assert "run" in DEFAULT_AGENT_TOOL_ALLOWLIST


def test_run_is_a_known_read_command_and_call_run_is_invented():
    from personal_data_warehouse.pdw_cli_commands import PDW_CLI_READ_SUBCOMMANDS
    from personal_data_warehouse.agent_usage import failure_shape
    assert "run" in PDW_CLI_READ_SUBCOMMANDS
    assert failure_shape("invented", "pdw call run", "")[2] == "pdw call run"
