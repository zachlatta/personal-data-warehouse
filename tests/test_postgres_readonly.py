from __future__ import annotations

from personal_data_warehouse.postgres_readonly import PostgresReadOnlyService, RawResult, validate_postgres_readonly_sql


def normalize_sql(sql: str) -> str:
    return " ".join(sql.split())


def test_postgres_readonly_sql_rejects_describe() -> None:
    validate_postgres_readonly_sql("SELECT * FROM gmail_messages LIMIT 1")
    validate_postgres_readonly_sql("WITH x AS (SELECT 1) SELECT * FROM x")
    validate_postgres_readonly_sql("SHOW search_path")
    validate_postgres_readonly_sql("EXPLAIN SELECT 1")

    try:
        validate_postgres_readonly_sql("DESCRIBE TABLE gmail_messages")
    except ValueError as exc:
        assert "SELECT, WITH, SHOW, or EXPLAIN" in str(exc)
    else:
        raise AssertionError("DESCRIBE should not be accepted as Postgres read-only SQL")


def test_postgres_readonly_service_schema_overview_uses_information_schema() -> None:
    class FakeRunner:
        def __init__(self) -> None:
            self.sql = []

        def query(self, sql: str, *, max_rows: int) -> RawResult:
            normalized = normalize_sql(sql)
            self.sql.append((normalized, max_rows))
            if normalized == "SELECT current_database() AS database":
                return RawResult(columns=["database"], rows=[{"database": "pdw"}])
            if normalized == (
                "SELECT table_name AS name FROM information_schema.tables WHERE table_schema = current_schema() "
                "AND table_type = 'BASE TABLE' ORDER BY table_name"
            ):
                return RawResult(columns=["name"], rows=[{"name": "calendar_events"}])
            if normalized == (
                "SELECT column_name AS name FROM information_schema.columns WHERE table_schema = current_schema() "
                "AND table_name = 'calendar_events' ORDER BY ordinal_position"
            ):
                return RawResult(columns=["name"], rows=[{"name": "event_id"}, {"name": "summary"}])
            if normalized.startswith('SELECT substring("event_id"::text'):
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

    runner = FakeRunner()

    result = PostgresReadOnlyService(runner).schema_overview()

    assert result.error == ""
    assert result.csv == "# pdw.calendar_events\n\nevent_id,summary\nevent-1,very long summa"
    assert runner.sql[0] == ("SELECT current_database() AS database", 1)
    assert runner.sql[1][0].startswith("SELECT table_name AS name FROM information_schema.tables")
    assert runner.sql[2][0].startswith("SELECT column_name AS name FROM information_schema.columns")
    assert 'char_length("summary"::text) AS "__pdw_len_1"' in runner.sql[3][0]
    assert result.truncated.max_rows == 3
    assert result.truncated.max_field_chars == 15
    assert result.truncated.fields[0].column == "summary"
