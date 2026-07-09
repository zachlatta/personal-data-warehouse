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
    "INTO",
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


def described_column_names(result: RawResult) -> list[str]:
    if not result.rows:
        return []
    name_column = "name" if "name" in result.columns else result.columns[0]
    return [str(row.get(name_column) or "") for row in result.rows if row.get(name_column)]


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
