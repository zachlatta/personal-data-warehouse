from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from personal_data_warehouse.readonly_sql import (
    DEFAULT_MAX_FIELD_CHARS,
    DEFAULT_MAX_ROWS,
    FieldTruncation,
    RawResult,
    ReadOnlyQueryResult,
    Truncation,
    csv_value,
    described_column_names,
    full_field_instructions,
    int_value,
    rows_to_csv,
    strip_markdown_code_fence,
    validate_readonly_sql,
)

SCHEMA_SAMPLE_ROWS = 3
SCHEMA_SAMPLE_FIELD_CHARS = 15
SCHEMA_SAMPLE_EXCLUDED_TABLES = frozenset({"agent_run_events", "agent_run_tool_calls", "agent_runs"})


class PostgresReadOnlyRunner:
    def __init__(self, warehouse) -> None:
        self._warehouse = warehouse

    def query(self, sql: str, *, max_rows: int) -> RawResult:
        with self._warehouse._connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '30s'")
            cursor.execute(sql)
            columns = [description.name for description in cursor.description or ()]
            if max_rows > 0:
                rows = cursor.fetchmany(max_rows)
            else:
                rows = cursor.fetchall()
        return rows_to_raw_result(columns, rows)


class PostgresReadOnlyService:
    def __init__(
        self,
        runner: PostgresReadOnlyRunner,
        *,
        max_rows: int = DEFAULT_MAX_ROWS,
        max_field_chars: int = DEFAULT_MAX_FIELD_CHARS,
    ) -> None:
        self._runner = runner
        self._max_rows = max_rows if max_rows > 0 else DEFAULT_MAX_ROWS
        self._max_field_chars = max_field_chars if max_field_chars > 0 else DEFAULT_MAX_FIELD_CHARS

    def execute_one(self, sql: str) -> ReadOnlyQueryResult:
        try:
            validate_postgres_readonly_sql(sql)
            raw = self._runner.query(sql, max_rows=self._max_rows + 1)
            rows, truncation = self._truncate_rows(raw.rows)
            return ReadOnlyQueryResult(sql=sql, csv=rows_to_csv(raw.columns, rows), truncated=truncation)
        except Exception as exc:
            message = str(exc)
            return ReadOnlyQueryResult(sql=sql, csv=rows_to_csv(["error"], [{"error": message}]), error=message)

    def schema_overview(self) -> ReadOnlyQueryResult:
        result_sql = "SELECT current_database() + information_schema.tables + SELECT * FROM <each table> LIMIT 3"
        try:
            database_result = self._runner.query("SELECT current_database() AS database", max_rows=1)
            database = current_database_name(database_result) or "postgres"
            tables_result = self._runner.query(
                """
                SELECT table_name AS name
                FROM information_schema.tables
                WHERE table_schema = current_schema()
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """,
                max_rows=0,
            )
            tables = table_names(tables_result)

            sections: list[str] = []
            fields: list[FieldTruncation] = []
            for table in tables:
                columns = self._describe_columns(table=table)
                if table in SCHEMA_SAMPLE_EXCLUDED_TABLES:
                    if columns:
                        sections.append(f"# {database}.{table}\n\n{rows_to_csv(['column'], [{'column': column} for column in columns])}")
                    continue
                sample = self._sample_table(table=table, columns=columns)
                if sample.error:
                    return sample
                sections.append(f"# {database}.{table}\n\n{sample.csv}")
                fields.extend(sample.truncated.fields)
            return ReadOnlyQueryResult(
                sql=result_sql,
                csv="\n\n".join(sections),
                truncated=Truncation(
                    rows=False,
                    max_rows=SCHEMA_SAMPLE_ROWS,
                    max_field_chars=SCHEMA_SAMPLE_FIELD_CHARS,
                    fields=fields,
                ),
            )
        except Exception as exc:
            message = str(exc)
            return ReadOnlyQueryResult(sql=result_sql, csv=rows_to_csv(["error"], [{"error": message}]), error=message)

    def _describe_columns(self, *, table: str) -> list[str]:
        result = self._runner.query(
            f"""
            SELECT column_name AS name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = '{table.replace("'", "''")}'
            ORDER BY ordinal_position
            """,
            max_rows=0,
        )
        return described_column_names(result)

    def _sample_table(self, *, table: str, columns: Sequence[str]) -> ReadOnlyQueryResult:
        if not columns:
            return ReadOnlyQueryResult(sql=f"SELECT * FROM {quote_postgres_identifier(table)} LIMIT 0", csv="")
        sample_sql = preview_sample_sql(quote_postgres_identifier(table), columns)
        raw = self._runner.query(sample_sql, max_rows=SCHEMA_SAMPLE_ROWS)
        rows, truncation = preview_rows(columns, raw.rows)
        return ReadOnlyQueryResult(sql=sample_sql, csv=rows_to_csv(columns, rows), truncated=truncation)

    def _truncate_rows(self, rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], Truncation]:
        truncated_rows = len(rows) > self._max_rows
        if truncated_rows:
            rows = rows[: self._max_rows]
        fields: list[FieldTruncation] = []
        out: list[dict[str, Any]] = []
        for row_index, row in enumerate(rows):
            copied: dict[str, Any] = {}
            for column, value in row.items():
                if isinstance(value, str) and len(value) > self._max_field_chars:
                    fields.append(
                        FieldTruncation(
                            row=row_index,
                            column=column,
                            returned_chars=self._max_field_chars,
                            original_chars=len(value),
                            instructions=full_field_instructions(column, self._max_field_chars),
                        )
                    )
                    copied[column] = value[: self._max_field_chars]
                else:
                    copied[column] = value
            out.append(copied)
        return out, Truncation(rows=truncated_rows, max_rows=self._max_rows, max_field_chars=self._max_field_chars, fields=fields)


def rows_to_raw_result(columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> RawResult:
    column_names = list(columns)
    return RawResult(
        columns=column_names,
        rows=[{column: row[index] for index, column in enumerate(column_names)} for row in rows],
    )


def validate_postgres_readonly_sql(sql: str) -> None:
    validate_readonly_sql(sql)
    keyword = first_sql_keyword(strip_markdown_code_fence(sql))
    if keyword in {"DESCRIBE", "DESC"}:
        raise ValueError("query tool is read-only; statement must start with SELECT, WITH, SHOW, or EXPLAIN")


def first_sql_keyword(sql: str) -> str:
    trimmed = sql.lstrip()
    chars: list[str] = []
    for char in trimmed:
        if char.isalpha() or char.isdigit() or char == "_":
            chars.append(char)
            continue
        break
    return "".join(chars).upper()


def current_database_name(result: RawResult) -> str:
    if not result.rows or not result.columns:
        return ""
    return str(result.rows[0].get(result.columns[0]) or "")


def table_names(result: RawResult) -> list[str]:
    if not result.columns:
        return []
    first_column = result.columns[0]
    return [str(row.get(first_column) or "") for row in result.rows if row.get(first_column)]


def quote_postgres_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def preview_length_column(index: int) -> str:
    return f"__pdw_len_{index}"


def preview_sample_sql(table_identifier: str, columns: Sequence[str]) -> str:
    expressions: list[str] = []
    for index, column in enumerate(columns):
        identifier = quote_postgres_identifier(column)
        length_alias = quote_postgres_identifier(preview_length_column(index))
        expressions.append(f"substring({identifier}::text from 1 for {SCHEMA_SAMPLE_FIELD_CHARS}) AS {identifier}")
        expressions.append(f"char_length({identifier}::text) AS {length_alias}")
    return f"SELECT {', '.join(expressions)} FROM {table_identifier} LIMIT {SCHEMA_SAMPLE_ROWS}"


def preview_rows(columns: Sequence[str], rows: Sequence[dict[str, Any]]) -> tuple[list[dict[str, Any]], Truncation]:
    fields: list[FieldTruncation] = []
    out: list[dict[str, Any]] = []
    for row_index, row in enumerate(rows):
        copied: dict[str, Any] = {}
        for column_index, column in enumerate(columns):
            preview = csv_value(row.get(column))
            copied[column] = preview
            original_chars = int_value(row.get(preview_length_column(column_index)))
            if original_chars > SCHEMA_SAMPLE_FIELD_CHARS:
                fields.append(
                    FieldTruncation(
                        row=row_index,
                        column=column,
                        returned_chars=len(preview),
                        original_chars=original_chars,
                        instructions=full_field_instructions(column, SCHEMA_SAMPLE_FIELD_CHARS),
                    )
                )
        out.append(copied)
    return out, Truncation(rows=False, max_rows=SCHEMA_SAMPLE_ROWS, max_field_chars=SCHEMA_SAMPLE_FIELD_CHARS, fields=fields)
