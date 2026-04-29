from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from personal_data_warehouse.clickhouse_readonly import (
    ClickHouseReadOnlyRunner,
    ClickHouseReadOnlyService,
    RawResult,
    rows_to_csv,
    strip_markdown_code_fence,
    validate_readonly_sql,
)


def test_validate_readonly_sql_allows_read_statements() -> None:
    for sql in [
        "SELECT * FROM gmail_messages LIMIT 1",
        "WITH x AS (SELECT 1) SELECT * FROM x",
        "SHOW TABLES",
        "DESCRIBE TABLE calendar_events",
        "EXPLAIN SELECT 1",
    ]:
        validate_readonly_sql(sql)


def test_validate_readonly_sql_rejects_writes_and_multiple_statements() -> None:
    for sql in [
        "DROP TABLE gmail_messages",
        "SELECT 1; DELETE FROM gmail_messages WHERE 1",
        "WITH x AS (SELECT 1) INSERT INTO y SELECT * FROM x",
        "OPTIMIZE TABLE voice_memo_enrichments FINAL",
    ]:
        with pytest.raises(ValueError, match="read-only|multiple"):
            validate_readonly_sql(sql)


def test_validate_readonly_sql_ignores_keywords_inside_strings() -> None:
    validate_readonly_sql("SELECT 'DROP TABLE nope' AS text")
    validate_readonly_sql("SELECT * FROM `drop`")


class FakeRunner:
    def __init__(self) -> None:
        self.max_rows = []

    def query(self, sql: str, *, max_rows: int) -> RawResult:
        self.max_rows.append(max_rows)
        return RawResult(
            columns=["body"],
            rows=[
                {"body": "abcdefghijklmnopqrstuvwxyz"},
                {"body": "second"},
                {"body": "third"},
            ],
        )


class FakeSchemaRunner:
    def __init__(self) -> None:
        self.sql = []

    def query(self, sql: str, *, max_rows: int) -> RawResult:
        self.sql.append((sql, max_rows))
        if sql == "SELECT currentDatabase() AS database":
            return RawResult(columns=["database"], rows=[{"database": "pdw"}])
        if sql == "SHOW TABLES":
            return RawResult(columns=["name"], rows=[{"name": "calendar_events"}])
        if sql == "DESCRIBE TABLE `calendar_events`":
            return RawResult(
                columns=["name", "type"],
                rows=[{"name": "event_id", "type": "String"}, {"name": "summary", "type": "String"}],
            )
        if sql.startswith("SELECT substring"):
            return RawResult(
                columns=["event_id", "__pdw_len_0", "summary", "__pdw_len_1"],
                rows=[
                    {
                        "event_id": "event-1",
                        "__pdw_len_0": 7,
                        "summary": "very long summa",
                        "__pdw_len_1": 30,
                    }
                ],
            )
        raise AssertionError(sql)


def test_readonly_service_truncates_rows_and_fields() -> None:
    runner = FakeRunner()
    service = ClickHouseReadOnlyService(runner, max_rows=2, max_field_chars=10)

    result = service.execute_one("SELECT body FROM gmail_messages")

    assert runner.max_rows == [3]
    assert result.error == ""
    assert result.csv == "body\nabcdefghij\nsecond"
    assert result.truncated.rows is True
    assert len(result.truncated.fields) == 1
    assert "substring(body" in result.truncated.fields[0].instructions


def test_readonly_service_returns_error_csv_for_rejected_sql() -> None:
    result = ClickHouseReadOnlyService(FakeRunner()).execute_one("DROP TABLE x")

    assert "read-only" in result.error
    assert "error" in result.csv


def test_readonly_service_schema_overview_samples_tables_like_mcp() -> None:
    runner = FakeSchemaRunner()

    result = ClickHouseReadOnlyService(runner).schema_overview()

    assert result.error == ""
    assert result.csv == "# pdw.calendar_events\n\nevent_id,summary\nevent-1,very long summa"
    assert runner.sql[0] == ("SELECT currentDatabase() AS database", 1)
    assert runner.sql[1] == ("SHOW TABLES", 0)
    assert runner.sql[2] == ("DESCRIBE TABLE `calendar_events`", 0)
    assert "length(toString(`summary`)) AS `__pdw_len_1`" in runner.sql[3][0]
    assert result.truncated.max_rows == 3
    assert result.truncated.max_field_chars == 15
    assert result.truncated.fields[0].column == "summary"


def test_rows_to_csv_matches_mcp_csv_conventions() -> None:
    csv = rows_to_csv(
        ["subject", "labels", "created_at"],
        [
            {
                "subject": 'hello, "world"\nnext',
                "labels": ["INBOX", "STARRED"],
                "created_at": datetime(2026, 4, 27, 12, tzinfo=UTC),
            }
        ],
    )

    assert csv == 'subject,labels,created_at\n"hello, ""world""\nnext","[""INBOX"",""STARRED""]",2026-04-27T12:00:00+00:00'


@dataclass
class FakeNativeClient:
    calls: list

    def execute(self, sql, *, with_column_types, settings):
        self.calls.append((sql, with_column_types, settings))
        return [("value",)], [("column", "String")]


class FakeWarehouse:
    def __init__(self) -> None:
        self._client_type = "native"
        self._client = FakeNativeClient([])

    def _with_clickhouse_retries(self, operation):
        return operation()


def test_clickhouse_runner_uses_clickhouse_readonly_setting() -> None:
    warehouse = FakeWarehouse()

    result = ClickHouseReadOnlyRunner(warehouse).query("SELECT 1", max_rows=25)

    assert result.columns == ["column"]
    assert result.rows == [{"column": "value"}]
    assert warehouse._client.calls[0][2]["readonly"] == 1
    assert warehouse._client.calls[0][2]["max_result_rows"] == 25
    assert warehouse._client.calls[0][2]["result_overflow_mode"] == "break"


def test_strip_markdown_code_fence_handles_sql_blocks() -> None:
    assert strip_markdown_code_fence("```sql\nSELECT 1\n```") == "SELECT 1"
