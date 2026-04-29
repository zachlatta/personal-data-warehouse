from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
import csv
import io
import json
import re
from typing import Any


ALLOWED_FIRST_KEYWORDS = {"SELECT", "WITH", "SHOW", "DESCRIBE", "DESC", "EXPLAIN"}
FORBIDDEN_KEYWORDS = {
    "ALTER",
    "ATTACH",
    "CREATE",
    "DELETE",
    "DETACH",
    "DROP",
    "GRANT",
    "INSERT",
    "KILL",
    "OPTIMIZE",
    "RENAME",
    "REVOKE",
    "SYSTEM",
    "TRUNCATE",
    "UPDATE",
}

DEFAULT_MAX_ROWS = 100
DEFAULT_MAX_FIELD_CHARS = 4000
SCHEMA_SAMPLE_ROWS = 3
SCHEMA_SAMPLE_FIELD_CHARS = 15


@dataclass(frozen=True)
class RawResult:
    columns: list[str]
    rows: list[dict[str, Any]]


@dataclass(frozen=True)
class FieldTruncation:
    row: int
    column: str
    returned_chars: int
    original_chars: int
    instructions: str


@dataclass(frozen=True)
class Truncation:
    rows: bool = False
    max_rows: int = DEFAULT_MAX_ROWS
    max_field_chars: int = DEFAULT_MAX_FIELD_CHARS
    fields: list[FieldTruncation] = field(default_factory=list)

    def empty(self) -> bool:
        return not self.rows and not self.fields


@dataclass(frozen=True)
class ReadOnlyQueryResult:
    sql: str
    csv: str
    error: str = ""
    truncated: Truncation = field(default_factory=Truncation)

    def as_tool_payload(self) -> dict[str, Any]:
        return {
            "sql": self.sql,
            "csv": self.csv,
            "error": self.error,
            "truncated": {
                "rows": self.truncated.rows,
                "max_rows": self.truncated.max_rows,
                "max_field_chars": self.truncated.max_field_chars,
                "fields": [field.__dict__ for field in self.truncated.fields],
            },
        }


def validate_readonly_sql(sql: str) -> None:
    trimmed = sql.strip()
    if not trimmed:
        raise ValueError("SQL must not be empty")
    _reject_multiple_statements(trimmed)

    words = _sql_words(trimmed)
    if not words:
        raise ValueError("SQL must contain a statement")
    if words[0] not in ALLOWED_FIRST_KEYWORDS:
        raise ValueError("query tool is read-only; statement must start with SELECT, WITH, SHOW, DESCRIBE, DESC, or EXPLAIN")
    for word in words:
        if word in FORBIDDEN_KEYWORDS:
            raise ValueError(f"query tool is read-only; {word} statements are not allowed")


def _reject_multiple_statements(sql: str) -> None:
    in_single = False
    in_double = False
    in_backtick = False
    for index, char in enumerate(sql):
        if char == "'" and not in_double and not in_backtick:
            in_single = not in_single
        elif char == '"' and not in_single and not in_backtick:
            in_double = not in_double
        elif char == "`" and not in_single and not in_double:
            in_backtick = not in_backtick
        elif char == ";" and not in_single and not in_double and not in_backtick:
            if sql[index + 1 :].strip():
                raise ValueError("multiple SQL statements are not allowed")


def _sql_words(sql: str) -> list[str]:
    words: list[str] = []
    in_single = False
    in_double = False
    in_backtick = False
    current: list[str] = []

    def flush() -> None:
        if current:
            words.append("".join(current).upper())
            current.clear()

    for char in sql:
        if char == "'" and not in_double and not in_backtick:
            in_single = not in_single
            flush()
            continue
        if char == '"' and not in_single and not in_backtick:
            in_double = not in_double
            flush()
            continue
        if char == "`" and not in_single and not in_double:
            in_backtick = not in_backtick
            flush()
            continue
        if in_single or in_double or in_backtick:
            continue
        if char.isalnum() or char == "_":
            current.append(char)
            continue
        flush()
    flush()
    return words


class ClickHouseReadOnlyRunner:
    def __init__(self, warehouse) -> None:
        self._warehouse = warehouse

    def query(self, sql: str, *, max_rows: int) -> RawResult:
        settings = {"readonly": 1}
        if max_rows > 0:
            settings["max_result_rows"] = max_rows
            settings["result_overflow_mode"] = "break"

        def run() -> RawResult:
            if self._warehouse._client_type == "native":
                rows, columns = self._warehouse._client.execute(
                    sql,
                    with_column_types=True,
                    settings=settings,
                )
                column_names = [name for name, _column_type in columns]
            else:
                result = self._warehouse._client.query(sql, settings=settings)
                rows = result.result_rows
                column_names = list(result.column_names)
            return rows_to_raw_result(column_names, rows)

        return self._warehouse._with_clickhouse_retries(run)


class ClickHouseReadOnlyService:
    def __init__(
        self,
        runner: ClickHouseReadOnlyRunner,
        *,
        max_rows: int = DEFAULT_MAX_ROWS,
        max_field_chars: int = DEFAULT_MAX_FIELD_CHARS,
    ) -> None:
        self._runner = runner
        self._max_rows = max_rows if max_rows > 0 else DEFAULT_MAX_ROWS
        self._max_field_chars = max_field_chars if max_field_chars > 0 else DEFAULT_MAX_FIELD_CHARS

    def execute_one(self, sql: str) -> ReadOnlyQueryResult:
        try:
            validate_readonly_sql(sql)
            raw = self._runner.query(sql, max_rows=self._max_rows + 1)
            rows, truncation = self._truncate_rows(raw.rows)
            return ReadOnlyQueryResult(sql=sql, csv=rows_to_csv(raw.columns, rows), truncated=truncation)
        except Exception as exc:
            message = str(exc)
            return ReadOnlyQueryResult(sql=sql, csv=rows_to_csv(["error"], [{"error": message}]), error=message)

    def schema_overview(self) -> ReadOnlyQueryResult:
        result_sql = "SELECT currentDatabase() + SHOW TABLES + SELECT * FROM <each table> LIMIT 3"
        try:
            database_result = self._runner.query("SELECT currentDatabase() AS database", max_rows=1)
            database = current_database_name(database_result) or "default"
            tables_result = self._runner.query("SHOW TABLES", max_rows=0)
            tables = table_names(tables_result)

            sections: list[str] = []
            fields: list[FieldTruncation] = []
            for table in tables:
                sample = self._sample_table(database=database, table=table)
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

    def _sample_table(self, *, database: str, table: str) -> ReadOnlyQueryResult:
        table_identifier = quote_clickhouse_identifier(table)
        describe_sql = f"DESCRIBE TABLE {table_identifier}"
        describe_result = self._runner.query(describe_sql, max_rows=0)
        columns = described_column_names(describe_result)
        if not columns:
            return ReadOnlyQueryResult(sql=describe_sql, csv="")

        sample_sql = preview_sample_sql(table_identifier, columns)
        raw = self._runner.query(sample_sql, max_rows=SCHEMA_SAMPLE_ROWS)
        rows, truncation = preview_rows(columns, raw.rows)
        return ReadOnlyQueryResult(
            sql=sample_sql,
            csv=rows_to_csv(columns, rows),
            truncated=truncation,
        )

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
        rows=[{column: normalize_value(row[index]) for index, column in enumerate(column_names)} for row in rows],
    )


def current_database_name(result: RawResult) -> str:
    if not result.rows or not result.columns:
        return ""
    value = result.rows[0].get(result.columns[0])
    return str(value or "")


def table_names(result: RawResult) -> list[str]:
    if not result.columns:
        return []
    first_column = result.columns[0]
    return [str(row.get(first_column) or "") for row in result.rows if row.get(first_column)]


def described_column_names(result: RawResult) -> list[str]:
    if not result.rows:
        return []
    name_column = "name" if "name" in result.columns else result.columns[0]
    return [str(row.get(name_column) or "") for row in result.rows if row.get(name_column)]


def quote_clickhouse_identifier(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def preview_length_column(index: int) -> str:
    return f"__pdw_len_{index}"


def preview_sample_sql(table_identifier: str, columns: Sequence[str]) -> str:
    expressions: list[str] = []
    for index, column in enumerate(columns):
        identifier = quote_clickhouse_identifier(column)
        length_alias = quote_clickhouse_identifier(preview_length_column(index))
        expressions.append(f"substring(toString({identifier}), 1, {SCHEMA_SAMPLE_FIELD_CHARS}) AS {identifier}")
        expressions.append(f"length(toString({identifier})) AS {length_alias}")
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


def int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def rows_to_csv(columns: Sequence[str], rows: Sequence[dict[str, Any]]) -> str:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(list(columns))
    for row in rows:
        writer.writerow([csv_value(row.get(column)) for column in columns])
    return buffer.getvalue().removesuffix("\n")


def csv_value(value: Any) -> str:
    value = normalize_value(value)
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, separators=(",", ":"))
    except TypeError:
        return str(value)


def normalize_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat() if value.tzinfo else value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def full_field_instructions(column: str, chunk_size: int) -> str:
    return (
        "This field was truncated. To retrieve it fully, first query "
        f"length({column}), then request chunks with substring({column}, start, {chunk_size}) "
        "using 1-based start offsets smaller than the reported length."
    )


def strip_markdown_code_fence(value: str) -> str:
    match = re.fullmatch(r"\s*```(?:sql)?\s*(.*?)\s*```\s*", value, flags=re.IGNORECASE | re.DOTALL)
    return match.group(1).strip() if match else value.strip()
