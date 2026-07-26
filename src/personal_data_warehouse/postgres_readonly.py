"""A dedicated read-only Postgres connection.

This used to also carry a query/schema-overview service that agent containers
reached through a bespoke proxy. Agents now use the real `pdw` CLI against the
app's `/api/tools` surface (see ``agent_tool_proxy``), so that second, drifting
implementation of the warehouse read surface is gone; what remains is the
read-only connection itself.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RawResult:
    columns: list[str]
    rows: list[dict[str, Any]]


class PostgresReadOnlyRunner:
    def __init__(self, warehouse) -> None:
        self._warehouse = warehouse
        self._connection = warehouse.read_only_connection()

    def query(self, sql: str, *, max_rows: int) -> RawResult:
        with self._connection.cursor() as cursor:
            cursor.execute(sql)
            columns = [description.name for description in cursor.description or ()]
            if max_rows > 0:
                rows = cursor.fetchmany(max_rows)
            else:
                rows = cursor.fetchall()
        return rows_to_raw_result(columns, rows)

    def close(self) -> None:
        try:
            self._connection.close()
        except Exception:
            pass


def rows_to_raw_result(columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> RawResult:
    column_names = list(columns)
    return RawResult(
        columns=column_names,
        rows=[{column: row[index] for index, column in enumerate(column_names)} for row in rows],
    )
