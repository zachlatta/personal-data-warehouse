from __future__ import annotations

import argparse
from collections.abc import Callable
import csv
from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, date, datetime
import hashlib
import io
import json
import os
from typing import Any

from dotenv import load_dotenv

from personal_data_warehouse.clickhouse import ClickHouseWarehouse
from personal_data_warehouse.postgres import (
    ARRAY_COLUMNS,
    FLOAT_COLUMNS,
    INTEGER_COLUMNS,
    JSONB_ARRAY_COLUMNS_BY_TABLE,
    JSONB_COLUMNS_BY_TABLE,
    POSTGRES_TABLES,
    TIMESTAMP_COLUMNS,
    PostgresWarehouse,
    _identifier,
    _upsert_clause,
)


DEFAULT_CHUNK_SIZE = 50_000
POSTGRES_TEXT_NUL_REPLACEMENT = "\\u0000"

CLEAN_VIEW_COLUMNS: dict[str, tuple[str, ...]] = {
    "clean_gmail_inbox": (
        "account",
        "thread_id",
        "latest_at",
        "latest_from_address",
        "subject",
        "latest_preview",
        "state",
        "unread_count",
        "important_count",
        "thread_messages_json",
    ),
    "clean_slack_inbox": (
        "account",
        "team_id",
        "kind",
        "state",
        "priority",
        "latest_at",
        "conversation_id",
        "conversation_name",
        "thread_ts",
        "message_ts",
        "actor_id",
        "actor_name",
        "title",
        "preview",
        "unread_count",
        "reason",
    ),
    "clean_calendar_with_transcripts": (
        "calendar_account",
        "recording_account",
        "calendar_id",
        "event_id",
        "recording_id",
        "title",
        "start_at",
        "end_at",
        "organizer_email",
        "calendar_title",
        "calendar_description",
        "location",
        "calendar_start_at",
        "calendar_end_at",
        "is_all_day",
        "attendees_json",
        "calendar_url",
        "calendar_confidence",
        "participants_json",
        "transcript",
        "summary",
        "action_items_json",
        "evidence_json",
        "created_at",
    ),
    "clean_transcripts_no_calendar_match": (
        "account",
        "recording_id",
        "recorded_at",
        "title",
        "start_at",
        "end_at",
        "attempted_calendar_event_id",
        "calendar_confidence",
        "calendar_match_issue",
        "participants_json",
        "transcript",
        "summary",
        "action_items_json",
        "evidence_json",
        "created_at",
    ),
}

CLICKHOUSE_VIEW_OVERRIDES: dict[str, str] = {
    # The deployed ClickHouse view uses argMax(value, internal_date), which is
    # unstable for messages with identical internal_date values. Use the same
    # deterministic tie-breaker as the Postgres view for parity verification.
    "clean_gmail_inbox": """
        SELECT
            account AS account,
            thread_id AS thread_id,
            latest_activity_at AS latest_at,
            latest_from_address AS latest_from_address,
            latest_subject AS subject,
            latest_preview AS latest_preview,
            multiIf(unread_count > 0, 'unread', important_count > 0, 'important', starred_count > 0, 'starred', 'inbox') AS state,
            toUInt64(unread_count) AS unread_count,
            toUInt64(important_count) AS important_count,
            toJSONString(thread_messages) AS thread_messages_json
        FROM (
            SELECT
                account,
                thread_id,
                max(internal_date) AS latest_activity_at,
                argMin(subject, tuple(-toInt64(toUnixTimestamp64Milli(internal_date)), message_id)) AS latest_subject,
                argMin(from_address, tuple(-toInt64(toUnixTimestamp64Milli(internal_date)), message_id)) AS latest_from_address,
                substring(
                    argMin(
                        if(
                            body_markdown_clean != '',
                            body_markdown_clean,
                            if(body_markdown != '', body_markdown, if(body_text != '', body_text, snippet))
                        ),
                        tuple(-toInt64(toUnixTimestamp64Milli(internal_date)), message_id)
                    ),
                    1,
                    1000
                ) AS latest_preview,
                countIf(has(label_ids, 'UNREAD')) AS unread_count,
                countIf(has(label_ids, 'IMPORTANT')) AS important_count,
                countIf(has(label_ids, 'STARRED')) AS starred_count,
                arrayMap(
                    message -> CAST(
                        tuple(
                            tupleElement(message, 1),
                            tupleElement(message, 2),
                            tupleElement(message, 3),
                            tupleElement(message, 4),
                            tupleElement(message, 5)
                        ),
                        'Tuple(internal_date DateTime64(3, ''UTC''), from_address String, to_addresses Array(String), cc_addresses Array(String), body_markdown_clean String)'
                    ),
                    arraySort(
                        message -> (tupleElement(message, 1), tupleElement(message, 6)),
                        groupArray(
                            CAST(
                                tuple(
                                    internal_date,
                                    from_address,
                                    to_addresses,
                                    cc_addresses,
                                    body_markdown_clean,
                                    message_id
                                ),
                                'Tuple(internal_date DateTime64(3, ''UTC''), from_address String, to_addresses Array(String), cc_addresses Array(String), body_markdown_clean String, message_id String)'
                            )
                        )
                    )
                ) AS thread_messages
            FROM gmail_messages FINAL
            WHERE is_deleted = 0
              AND has(label_ids, 'INBOX')
              AND NOT has(label_ids, 'TRASH')
              AND NOT has(label_ids, 'SPAM')
            GROUP BY account, thread_id
        )
    """,
}

METHOD_CHECKS: dict[str, Callable[[Any], Any]] = {
    "load_sync_state": lambda warehouse: warehouse.load_sync_state(),
    "load_calendar_sync_state": lambda warehouse: warehouse.load_calendar_sync_state(),
    "load_slack_sync_state": lambda warehouse: warehouse.load_slack_sync_state(),
}


@dataclass(frozen=True)
class TableVerification:
    table: str
    clickhouse_rows: int
    postgres_rows: int
    clickhouse_hash: str
    postgres_hash: str
    clickhouse_tombstones: int
    postgres_tombstones: int

    @property
    def ok(self) -> bool:
        return (
            self.clickhouse_rows == self.postgres_rows
            and self.clickhouse_hash == self.postgres_hash
            and self.clickhouse_tombstones == self.postgres_tombstones
        )


@dataclass(frozen=True)
class ParityVerification:
    name: str
    clickhouse_rows: int
    postgres_rows: int
    clickhouse_hash: str
    postgres_hash: str

    @property
    def ok(self) -> bool:
        return self.clickhouse_rows == self.postgres_rows and self.clickhouse_hash == self.postgres_hash


def main(argv: list[str] | None = None) -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Migrate and verify ClickHouse FINAL state in Postgres.")
    parser.add_argument("command", choices=["migrate", "verify", "migrate-and-verify"])
    parser.add_argument("--clickhouse-url", default=os.getenv("CLICKHOUSE_URL") or "")
    parser.add_argument("--postgres-database-url", default=os.getenv("POSTGRES_DATABASE_URL") or "")
    parser.add_argument("--schema", default=os.getenv("POSTGRES_SCHEMA") or "public")
    parser.add_argument("--table", action="append", dest="tables", help="Limit to one table; can be passed multiple times")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--no-truncate", action="store_true", help="Do not truncate target tables before migrating")
    args = parser.parse_args(argv)

    if not args.clickhouse_url:
        raise SystemExit("CLICKHOUSE_URL must be set for migration and verification")
    if not args.postgres_database_url:
        raise SystemExit("POSTGRES_DATABASE_URL must be set for migration and verification")
    if args.chunk_size < 1:
        raise SystemExit("--chunk-size must be positive")

    tables = tuple(args.tables or POSTGRES_TABLES.keys())
    unknown = sorted(set(tables) - set(POSTGRES_TABLES))
    if unknown:
        raise SystemExit(f"unknown table(s): {', '.join(unknown)}")

    print("connecting to ClickHouse source", flush=True)
    clickhouse = ClickHouseWarehouse(args.clickhouse_url)
    tables = filter_existing_clickhouse_tables(clickhouse, tables=tables, explicit=args.tables is not None)
    print(f"connecting to Postgres target schema {args.schema}", flush=True)
    postgres = PostgresWarehouse(args.postgres_database_url, schema=args.schema)
    try:
        if args.command in {"migrate", "migrate-and-verify"}:
            migrate(clickhouse=clickhouse, postgres=postgres, tables=tables, chunk_size=args.chunk_size, truncate=not args.no_truncate)
        if args.command in {"verify", "migrate-and-verify"}:
            ensure_all_tables(postgres)
            results = verify(clickhouse=clickhouse, postgres=postgres, tables=tables, chunk_size=args.chunk_size)
            view_results = verify_views(clickhouse=clickhouse, postgres=postgres, chunk_size=args.chunk_size) if args.tables is None else []
            method_results = verify_methods(clickhouse=clickhouse, postgres=postgres) if args.tables is None else []
            print_verification(results, view_results, method_results)
            if not all(result.ok for result in [*results, *view_results, *method_results]):
                raise SystemExit(1)
    finally:
        postgres.close()


def migrate(
    *,
    clickhouse: ClickHouseWarehouse,
    postgres: PostgresWarehouse,
    tables: tuple[str, ...],
    chunk_size: int,
    truncate: bool,
) -> None:
    print("ensuring Postgres tables and views", flush=True)
    ensure_all_tables(postgres)
    if truncate:
        print("truncating selected Postgres target tables", flush=True)
        truncate_tables(postgres, tables)
    for table in tables:
        table_chunk_size = effective_chunk_size(table, chunk_size)
        print(f"{table}: starting migration (chunk size {table_chunk_size})", flush=True)
        migrated = 0
        prepare_postgres_copy_stage(postgres, columns=POSTGRES_TABLES[table].columns)
        for rows in iter_clickhouse_current_rows(clickhouse, table=table, chunk_size=table_chunk_size):
            postgres_copy_insert(postgres, table=table, rows=rows, columns=POSTGRES_TABLES[table].columns)
            migrated += len(rows)
            print(f"{table}: migrated {migrated} rows", flush=True)
        print(f"{table}: migration complete ({migrated} rows)", flush=True)
    if "slack_messages" in tables:
        print("slack_conversation_stats: rebuilding from migrated slack_messages", flush=True)
        postgres.rebuild_slack_conversation_stats()


def verify(
    *,
    clickhouse: ClickHouseWarehouse,
    postgres: PostgresWarehouse,
    tables: tuple[str, ...],
    chunk_size: int,
) -> list[TableVerification]:
    results: list[TableVerification] = []
    for table in tables:
        print(f"{table}: verifying table rows, hashes, and tombstones", flush=True)
        table_chunk_size = effective_chunk_size(table, chunk_size)
        clickhouse_count, clickhouse_hash, clickhouse_tombstones = clickhouse_table_stats(
            clickhouse,
            table=table,
            chunk_size=table_chunk_size,
        )
        postgres_count, postgres_hash, postgres_tombstones = postgres_table_stats(
            postgres,
            table=table,
            chunk_size=table_chunk_size,
        )
        results.append(
            TableVerification(
                table=table,
                clickhouse_rows=clickhouse_count,
                postgres_rows=postgres_count,
                clickhouse_hash=clickhouse_hash,
                postgres_hash=postgres_hash,
                clickhouse_tombstones=clickhouse_tombstones,
                postgres_tombstones=postgres_tombstones,
            )
        )
    return results


def effective_chunk_size(table: str, requested_chunk_size: int) -> int:
    caps = {
        "gmail_messages": 5_000,
        "gmail_attachments": 10_000,
    }
    return min(requested_chunk_size, caps.get(table, requested_chunk_size))


def filter_existing_clickhouse_tables(
    clickhouse: ClickHouseWarehouse,
    *,
    tables: tuple[str, ...],
    explicit: bool,
) -> tuple[str, ...]:
    existing: list[str] = []
    missing: list[str] = []
    for table in tables:
        if clickhouse_table_exists(clickhouse, table=table):
            existing.append(table)
        else:
            missing.append(table)
    if missing and explicit:
        raise SystemExit(f"ClickHouse source table(s) missing: {', '.join(missing)}")
    for table in missing:
        print(f"{table}: skipping because ClickHouse source table is missing", flush=True)
    return tuple(existing)


def clickhouse_table_exists(clickhouse: ClickHouseWarehouse, *, table: str) -> bool:
    rows = clickhouse._query(f"SHOW TABLES LIKE {clickhouse_literal(table)}")
    return bool(rows)


def verify_views(
    *,
    clickhouse: ClickHouseWarehouse,
    postgres: PostgresWarehouse,
    chunk_size: int,
) -> list[ParityVerification]:
    results: list[ParityVerification] = []
    for view, columns in CLEAN_VIEW_COLUMNS.items():
        print(f"{view}: verifying clean view rows and hash", flush=True)
        clickhouse_count, clickhouse_hash = clickhouse_relation_count_and_hash(
            clickhouse,
            relation=view,
            columns=columns,
            chunk_size=chunk_size,
        )
        postgres_count, postgres_hash = postgres_relation_count_and_hash(
            postgres,
            relation=view,
            columns=columns,
            chunk_size=chunk_size,
        )
        results.append(
            ParityVerification(
                name=view,
                clickhouse_rows=clickhouse_count,
                postgres_rows=postgres_count,
                clickhouse_hash=clickhouse_hash,
                postgres_hash=postgres_hash,
            )
        )
    return results


def verify_methods(
    *,
    clickhouse: ClickHouseWarehouse,
    postgres: PostgresWarehouse,
) -> list[ParityVerification]:
    results: list[ParityVerification] = []
    for name, loader in METHOD_CHECKS.items():
        print(f"{name}: verifying warehouse method output", flush=True)
        clickhouse_payload = canonical_row((loader(clickhouse),))
        postgres_payload = canonical_row((loader(postgres),))
        results.append(
            ParityVerification(
                name=name,
                clickhouse_rows=1,
                postgres_rows=1,
                clickhouse_hash=hash_text(clickhouse_payload),
                postgres_hash=hash_text(postgres_payload),
            )
        )
    return results


def ensure_all_tables(postgres: PostgresWarehouse) -> None:
    postgres.ensure_tables()
    postgres.ensure_calendar_tables()
    postgres.ensure_contacts_tables()
    postgres.ensure_apple_voice_memos_tables(backfill_content_hashes=False)
    postgres.ensure_slack_tables()


def truncate_tables(postgres: PostgresWarehouse, tables: tuple[str, ...]) -> None:
    for table in reversed(tables):
        postgres._command(f'TRUNCATE TABLE "{table}" CASCADE')


def prepare_postgres_copy_stage(postgres: PostgresWarehouse, *, columns: tuple[str, ...]) -> None:
    column_sql = ", ".join(f"{_identifier(column)} text" for column in columns)
    postgres._command("SET temp_buffers = '512MB'")
    postgres._command("DROP TABLE IF EXISTS _pdw_migration_stage")
    postgres._command(f"CREATE TEMP TABLE _pdw_migration_stage ({column_sql})")


def postgres_copy_insert(
    postgres: PostgresWarehouse,
    *,
    table: str,
    rows: list[tuple[Any, ...]],
    columns: tuple[str, ...],
) -> None:
    if not rows:
        return
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    for row in rows:
        writer.writerow([copy_stage_value(value) for value in row])
    buffer.seek(0)

    column_sql = ", ".join(_identifier(column) for column in columns)
    select_sql = ", ".join(postgres_stage_cast(table, column) for column in columns)
    sql = f"""
        INSERT INTO {_identifier(table)} ({column_sql})
        SELECT {select_sql}
        FROM _pdw_migration_stage AS stage
        {_upsert_clause(table, POSTGRES_TABLES[table], columns)}
    """
    with postgres._connection.cursor() as cursor:
        cursor.execute("TRUNCATE _pdw_migration_stage")
        cursor.copy_expert(
            f"COPY _pdw_migration_stage ({column_sql}) FROM STDIN WITH (FORMAT CSV, NULL '\\N__PDW_NULL__')",
            buffer,
        )
        cursor.execute(sql)


def copy_stage_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(UTC)
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return json.dumps(list(value), ensure_ascii=False, separators=(",", ":"))
    return postgres_text_value(str(value))


def postgres_text_value(value: str) -> str:
    return value.replace("\x00", POSTGRES_TEXT_NUL_REPLACEMENT)


def postgres_stage_cast(table: str, column: str) -> str:
    identifier = _identifier(column)
    if column in ARRAY_COLUMNS:
        return f"COALESCE(ARRAY(SELECT jsonb_array_elements_text(NULLIF(stage.{identifier}, '')::jsonb)), '{{}}'::text[]) AS {identifier}"
    if column in JSONB_COLUMNS_BY_TABLE.get(table, set()):
        default = "'[]'::jsonb" if column in JSONB_ARRAY_COLUMNS_BY_TABLE.get(table, set()) else "'{}'::jsonb"
        return f"COALESCE(NULLIF(stage.{identifier}, '')::jsonb, {default}) AS {identifier}"
    if column in TIMESTAMP_COLUMNS:
        return f"NULLIF(stage.{identifier}, '')::timestamptz AS {identifier}"
    if column in INTEGER_COLUMNS:
        return f"COALESCE(NULLIF(stage.{identifier}, '')::bigint, 0) AS {identifier}"
    if column in FLOAT_COLUMNS:
        return f"COALESCE(NULLIF(stage.{identifier}, '')::double precision, 0) AS {identifier}"
    return f"stage.{identifier} AS {identifier}"


def iter_clickhouse_current_rows(
    clickhouse: ClickHouseWarehouse,
    *,
    table: str,
    chunk_size: int,
):
    yield from iter_clickhouse_current_rows_keyset(clickhouse, table=table, chunk_size=chunk_size)


def iter_clickhouse_current_rows_streaming(
    clickhouse: ClickHouseWarehouse,
    *,
    table: str,
    chunk_size: int,
):
    spec = POSTGRES_TABLES[table]
    columns = spec.columns
    key_indexes = [columns.index(column) for column in spec.primary_key]
    version_index = columns.index(spec.version_column)
    for prefix in clickhouse_key_prefixes(clickhouse, table=table):
        where_clauses = [
            f"{column} = {clickhouse_literal(value)}"
            for column, value in zip(spec.primary_key, prefix, strict=False)
        ]
        where = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        sql = f"""
            SELECT {", ".join(columns)}
            FROM {table}
            {where}
            ORDER BY {", ".join(spec.primary_key)}
        """
        current_key: tuple[Any, ...] | None = None
        current_group: list[tuple[Any, ...]] = []
        output: list[tuple[Any, ...]] = []
        for row in clickhouse._client.execute_iter(sql, settings={"max_block_size": max(1000, min(chunk_size, 100_000))}):
            row = tuple(row)
            key = tuple(row[index] for index in key_indexes)
            if current_key is None:
                current_key = key
            if key != current_key:
                output.append(latest_row_for_key(current_group, version_index=version_index))
                if len(output) >= chunk_size:
                    yield output
                    output = []
                current_key = key
                current_group = [row]
            else:
                current_group.append(row)
        if current_group:
            output.append(latest_row_for_key(current_group, version_index=version_index))
        if output:
            yield output


def iter_clickhouse_current_rows_keyset(
    clickhouse: ClickHouseWarehouse,
    *,
    table: str,
    chunk_size: int,
):
    spec = POSTGRES_TABLES[table]
    columns = spec.columns
    key_indexes = [columns.index(column) for column in spec.primary_key]
    version_index = columns.index(spec.version_column)
    for prefix in clickhouse_key_prefixes(clickhouse, table=table):
        last_emitted_key: tuple[Any, ...] | None = None
        while True:
            where_clauses = []
            for column, value in zip(spec.primary_key, prefix, strict=False):
                where_clauses.append(f"{column} = {clickhouse_literal(value)}")
            if last_emitted_key is not None:
                key_columns = ", ".join(spec.primary_key)
                key_values = ", ".join(clickhouse_literal(value) for value in last_emitted_key)
                where_clauses.append(f"({key_columns}) > ({key_values})")
            where = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
            sql = f"""
                SELECT {", ".join(columns)}
                FROM {table}
                {where}
                ORDER BY {", ".join(spec.primary_key)}
                LIMIT {int(chunk_size)}
            """
            rows = [tuple(row) for row in clickhouse._query(sql)]
            if not rows:
                break
            current_rows, last_emitted_key = collapse_clickhouse_rows(
                clickhouse,
                table=table,
                rows=rows,
                key_indexes=key_indexes,
                version_index=version_index,
                chunk_was_full=len(rows) == chunk_size,
            )
            if current_rows:
                yield current_rows
            if last_emitted_key is None:
                break


def clickhouse_key_prefixes(clickhouse: ClickHouseWarehouse, *, table: str) -> list[tuple[Any, ...]]:
    spec = POSTGRES_TABLES[table]
    first_key = spec.primary_key[0]
    if first_key not in {"account", "source"}:
        return [()]
    rows = clickhouse._query(f"SELECT {first_key} FROM {table} GROUP BY {first_key} ORDER BY {first_key}")
    return [(row[0],) for row in rows]


def collapse_clickhouse_rows(
    clickhouse: ClickHouseWarehouse,
    *,
    table: str,
    rows: list[tuple[Any, ...]],
    key_indexes: list[int],
    version_index: int,
    chunk_was_full: bool,
) -> tuple[list[tuple[Any, ...]], tuple[Any, ...] | None]:
    groups = group_rows_by_key(rows, key_indexes)
    if chunk_was_full:
        groups_to_emit = groups[:-1]
    else:
        groups_to_emit = groups
    if not groups_to_emit and groups:
        key = groups[0][0]
        return [latest_clickhouse_key_row(clickhouse, table=table, key=key, version_index=version_index)], key
    current_rows = [latest_row_for_key(group_rows, version_index=version_index) for _, group_rows in groups_to_emit]
    last_key = groups_to_emit[-1][0] if groups_to_emit else None
    return current_rows, last_key


def group_rows_by_key(rows: list[tuple[Any, ...]], key_indexes: list[int]) -> list[tuple[tuple[Any, ...], list[tuple[Any, ...]]]]:
    groups: list[tuple[tuple[Any, ...], list[tuple[Any, ...]]]] = []
    for row in rows:
        key = tuple(row[index] for index in key_indexes)
        if groups and groups[-1][0] == key:
            groups[-1][1].append(row)
        else:
            groups.append((key, [row]))
    return groups


def latest_clickhouse_key_row(
    clickhouse: ClickHouseWarehouse,
    *,
    table: str,
    key: tuple[Any, ...],
    version_index: int,
) -> tuple[Any, ...]:
    spec = POSTGRES_TABLES[table]
    columns = spec.columns
    where = " AND ".join(
        f"{column} = {clickhouse_literal(value)}" for column, value in zip(spec.primary_key, key, strict=True)
    )
    rows = [tuple(row) for row in clickhouse._query(f"SELECT {', '.join(columns)} FROM {table} WHERE {where}")]
    return latest_row_for_key(rows, version_index=version_index)


def latest_row_for_key(rows: list[tuple[Any, ...]], *, version_index: int) -> tuple[Any, ...]:
    return max(enumerate(rows), key=lambda item: (item[1][version_index], item[0]))[1]


def clickhouse_table_stats(
    clickhouse: ClickHouseWarehouse,
    *,
    table: str,
    chunk_size: int,
) -> tuple[int, str, int]:
    columns = POSTGRES_TABLES[table].columns
    is_deleted_index = columns.index("is_deleted") if "is_deleted" in columns else None
    count = 0
    tombstones = 0
    digest = hashlib.sha256()
    next_report = 250_000
    for rows in iter_clickhouse_current_rows(clickhouse, table=table, chunk_size=chunk_size):
        for row in rows:
            count += 1
            if is_deleted_index is not None and int(row[is_deleted_index]) != 0:
                tombstones += 1
            digest.update(canonical_row(row).encode("utf-8"))
            digest.update(b"\n")
        if count >= next_report:
            print(f"{table}: verified {count} ClickHouse rows", flush=True)
            next_report += 250_000
    return count, digest.hexdigest(), tombstones


def postgres_table_stats(
    postgres: PostgresWarehouse,
    *,
    table: str,
    chunk_size: int,
) -> tuple[int, str, int]:
    spec = POSTGRES_TABLES[table]
    columns = spec.columns
    is_deleted_index = columns.index("is_deleted") if "is_deleted" in columns else None
    count = 0
    tombstones = 0
    digest = hashlib.sha256()
    next_report = 250_000
    for rows in iter_postgres_rows(postgres, table=table, columns=columns, key_columns=spec.primary_key, chunk_size=chunk_size):
        for row in rows:
            count += 1
            if is_deleted_index is not None and int(row[is_deleted_index]) != 0:
                tombstones += 1
            digest.update(canonical_row(row).encode("utf-8"))
            digest.update(b"\n")
        if count >= next_report:
            print(f"{table}: verified {count} Postgres rows", flush=True)
            next_report += 250_000
    return count, digest.hexdigest(), tombstones


def iter_postgres_rows(
    postgres: PostgresWarehouse,
    *,
    table: str,
    columns: tuple[str, ...],
    key_columns: tuple[str, ...],
    chunk_size: int,
):
    last_key: tuple[Any, ...] | None = None
    while True:
        where = ""
        params: tuple[Any, ...] = ()
        if last_key is not None:
            where = f"WHERE ({', '.join(_identifier(column) for column in key_columns)}) > ({', '.join(['%s'] * len(last_key))})"
            params = last_key
        rows = postgres._query(
            f"""
            SELECT {", ".join(_identifier(column) for column in columns)}
            FROM {_identifier(table)}
            {where}
            ORDER BY {", ".join(_identifier(column) for column in key_columns)}
            LIMIT %s
            """,
            (*params, chunk_size),
        )
        if not rows:
            break
        yield rows
        key_indexes = [columns.index(column) for column in key_columns]
        last_key = tuple(rows[-1][index] for index in key_indexes)


def clickhouse_relation_count_and_hash(
    clickhouse: ClickHouseWarehouse,
    *,
    relation: str,
    columns: tuple[str, ...],
    chunk_size: int,
) -> tuple[int, str]:
    count = 0
    digest = hashlib.sha256()
    offset = 0
    select_columns = ", ".join(columns)
    order_columns = ", ".join(columns)
    source_override = CLICKHOUSE_VIEW_OVERRIDES.get(relation)
    source = f"({source_override})" if source_override is not None else relation
    while True:
        rows = clickhouse._query(
            f"""
            SELECT {select_columns}
            FROM {source}
            ORDER BY {order_columns}
            LIMIT {int(chunk_size)} OFFSET {int(offset)}
            """
        )
        if not rows:
            break
        for row in rows:
            count += 1
            digest.update(canonical_row(row).encode("utf-8"))
            digest.update(b"\n")
        offset += len(rows)
    return count, digest.hexdigest()


def postgres_relation_count_and_hash(
    postgres: PostgresWarehouse,
    *,
    relation: str,
    columns: tuple[str, ...],
    chunk_size: int,
) -> tuple[int, str]:
    count = 0
    digest = hashlib.sha256()
    offset = 0
    select_columns = ", ".join('"' + column + '"' for column in columns)
    order_columns = ", ".join('"' + column + '"' for column in columns)
    while True:
        rows = postgres._query(
            f"""
            SELECT {select_columns}
            FROM "{relation}"
            ORDER BY {order_columns}
            LIMIT %s OFFSET %s
            """,
            (chunk_size, offset),
        )
        if not rows:
            break
        for row in rows:
            count += 1
            digest.update(canonical_row(row).encode("utf-8"))
            digest.update(b"\n")
        offset += len(rows)
    return count, digest.hexdigest()


def hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def canonical_row(row: tuple[Any, ...]) -> str:
    return json.dumps([canonical_value(value) for value in row], ensure_ascii=False, separators=(",", ":"))


def canonical_value(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(UTC)
        return value.isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    if is_dataclass(value) and not isinstance(value, type):
        return canonical_value(asdict(value))
    if isinstance(value, dict):
        return [[canonical_value(key), canonical_value(item)] for key, item in sorted(value.items(), key=lambda item: str(item[0]))]
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8", errors="ignore")
    if isinstance(value, str):
        return postgres_text_value(value)
    if isinstance(value, tuple):
        return [canonical_value(item) for item in value]
    if isinstance(value, list):
        return [canonical_value(item) for item in value]
    return value


def clickhouse_literal(value: Any) -> str:
    if isinstance(value, datetime):
        return "'" + value.isoformat().replace("'", "\\'") + "'"
    if isinstance(value, str):
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
    return str(value)


def print_verification(
    results: list[TableVerification],
    view_results: list[ParityVerification],
    method_results: list[ParityVerification],
) -> None:
    print("table\tclickhouse_rows\tpostgres_rows\tclickhouse_hash\tpostgres_hash\tclickhouse_tombstones\tpostgres_tombstones\tok")
    for result in results:
        print(
            "\t".join(
                [
                    result.table,
                    str(result.clickhouse_rows),
                    str(result.postgres_rows),
                    result.clickhouse_hash,
                    result.postgres_hash,
                    str(result.clickhouse_tombstones),
                    str(result.postgres_tombstones),
                    "yes" if result.ok else "no",
                ]
            )
        )
    if view_results:
        print()
        print("view\tclickhouse_rows\tpostgres_rows\tclickhouse_hash\tpostgres_hash\tok")
        print_parity_results(view_results)
    if method_results:
        print()
        print("method\tclickhouse_rows\tpostgres_rows\tclickhouse_hash\tpostgres_hash\tok")
        print_parity_results(method_results)


def print_parity_results(results: list[ParityVerification]) -> None:
    for result in results:
        print(
            "\t".join(
                [
                    result.name,
                    str(result.clickhouse_rows),
                    str(result.postgres_rows),
                    result.clickhouse_hash,
                    result.postgres_hash,
                    "yes" if result.ok else "no",
                ]
            )
        )


if __name__ == "__main__":
    main()
