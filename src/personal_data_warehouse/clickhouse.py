from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import time
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import clickhouse_connect
from clickhouse_driver import Client as NativeClickHouseClient
from clickhouse_driver.errors import NetworkError, SocketTimeoutError

MESSAGE_COLUMNS = (
    "account",
    "message_id",
    "thread_id",
    "history_id",
    "internal_date",
    "label_ids",
    "is_deleted",
    "snippet",
    "subject",
    "from_address",
    "to_addresses",
    "cc_addresses",
    "bcc_addresses",
    "delivered_to",
    "rfc822_message_id",
    "date_header",
    "size_estimate",
    "body_text",
    "body_html",
    "body_markdown",
    "body_markdown_full",
    "body_markdown_clean",
    "payload_json",
    "synced_at",
    "sync_version",
)

SYNC_STATE_COLUMNS = (
    "account",
    "last_history_id",
    "last_sync_type",
    "status",
    "error",
    "updated_at",
)

SLACK_TEAM_COLUMNS = (
    "account",
    "team_id",
    "team_name",
    "domain",
    "enterprise_id",
    "raw_json",
    "synced_at",
    "sync_version",
)

SLACK_USER_COLUMNS = (
    "account",
    "team_id",
    "user_id",
    "team_user_id",
    "name",
    "real_name",
    "display_name",
    "email",
    "is_bot",
    "is_app_user",
    "is_deleted",
    "tz",
    "raw_json",
    "synced_at",
    "sync_version",
)

SLACK_CONVERSATION_COLUMNS = (
    "account",
    "team_id",
    "conversation_id",
    "conversation_type",
    "name",
    "is_channel",
    "is_group",
    "is_im",
    "is_mpim",
    "is_private",
    "is_archived",
    "is_member",
    "creator",
    "created_at",
    "topic",
    "purpose",
    "num_members",
    "raw_json",
    "synced_at",
    "sync_version",
)

SLACK_CONVERSATION_MEMBER_COLUMNS = (
    "account",
    "team_id",
    "conversation_id",
    "user_id",
    "is_deleted",
    "synced_at",
    "sync_version",
)

SLACK_MESSAGE_COLUMNS = (
    "account",
    "team_id",
    "conversation_id",
    "message_ts",
    "message_datetime",
    "thread_ts",
    "parent_message_ts",
    "user_id",
    "bot_id",
    "username",
    "type",
    "subtype",
    "text",
    "blocks_json",
    "attachments_json",
    "is_thread_parent",
    "is_thread_reply",
    "reply_count",
    "reply_users_count",
    "latest_reply_ts",
    "edited_ts",
    "client_msg_id",
    "is_deleted",
    "raw_json",
    "synced_at",
    "sync_version",
)

SLACK_REACTION_COLUMNS = (
    "account",
    "team_id",
    "conversation_id",
    "message_ts",
    "reaction_name",
    "user_id",
    "reaction_count",
    "is_deleted",
    "raw_json",
    "synced_at",
    "sync_version",
)

SLACK_FILE_COLUMNS = (
    "account",
    "team_id",
    "file_id",
    "conversation_id",
    "message_ts",
    "user_id",
    "created_at",
    "name",
    "title",
    "mimetype",
    "filetype",
    "url_private",
    "size",
    "is_deleted",
    "raw_json",
    "synced_at",
    "sync_version",
)

SLACK_SYNC_STATE_COLUMNS = (
    "account",
    "team_id",
    "object_type",
    "object_id",
    "cursor_ts",
    "last_sync_type",
    "status",
    "error",
    "updated_at",
    "sync_version",
)


@dataclass(frozen=True)
class SyncState:
    account: str
    last_history_id: int
    last_sync_type: str
    status: str
    error: str
    updated_at: datetime


class ClickHouseWarehouse:
    def __init__(self, clickhouse_url: str):
        if not clickhouse_url:
            raise ValueError("CLICKHOUSE_URL must be set")
        parsed = _parse_clickhouse_url(clickhouse_url)
        self._client_type = parsed["client_type"]
        if self._client_type == "native":
            self._client = NativeClickHouseClient(**parsed["kwargs"])
        else:
            try:
                self._client = clickhouse_connect.get_client(**parsed["kwargs"])
            except Exception as exc:
                if "is for clickhouse-client" not in str(exc):
                    raise
                self._client_type = "native"
                self._client = NativeClickHouseClient(
                    **_native_client_kwargs_from_url(clickhouse_url)
                )

    def ensure_tables(self) -> None:
        self._command(
            """
            CREATE TABLE IF NOT EXISTS gmail_messages (
                account LowCardinality(String),
                message_id String,
                thread_id String,
                history_id UInt64,
                internal_date DateTime64(3, 'UTC'),
                label_ids Array(String),
                is_deleted UInt8,
                snippet String,
                subject String,
                from_address String,
                to_addresses Array(String),
                cc_addresses Array(String),
                bcc_addresses Array(String),
                delivered_to String,
                rfc822_message_id String,
                date_header String,
                size_estimate UInt32,
                body_text String,
                body_html String,
                body_markdown String,
                body_markdown_full String,
                body_markdown_clean String,
                payload_json String,
                synced_at DateTime64(3, 'UTC'),
                sync_version UInt64
            )
            ENGINE = ReplacingMergeTree(sync_version)
            PARTITION BY toYYYYMM(synced_at)
            ORDER BY (account, message_id)
            """
        )
        self._command("ALTER TABLE gmail_messages ADD COLUMN IF NOT EXISTS body_markdown String AFTER body_html")
        self._command("ALTER TABLE gmail_messages ADD COLUMN IF NOT EXISTS body_markdown_full String AFTER body_markdown")
        self._command(
            "ALTER TABLE gmail_messages ADD COLUMN IF NOT EXISTS body_markdown_clean String AFTER body_markdown_full"
        )
        self._command(
            """
            CREATE OR REPLACE VIEW gmail_thread_messages AS
            SELECT
                account,
                thread_id,
                internal_date,
                message_id,
                subject,
                from_address,
                to_addresses,
                cc_addresses,
                bcc_addresses,
                label_ids,
                row_number() OVER (
                    PARTITION BY account, thread_id
                    ORDER BY internal_date, message_id
                ) AS thread_message_index,
                count() OVER (
                    PARTITION BY account, thread_id
                ) AS thread_message_count,
                body_markdown,
                body_markdown_full,
                body_markdown_clean,
                snippet,
                history_id,
                synced_at
            FROM gmail_messages FINAL
            WHERE is_deleted = 0
            ORDER BY account, thread_id, internal_date, message_id
            """
        )
        self._command(
            """
            CREATE TABLE IF NOT EXISTS gmail_sync_state (
                account LowCardinality(String),
                last_history_id UInt64,
                last_sync_type LowCardinality(String),
                status LowCardinality(String),
                error String,
                updated_at DateTime64(3, 'UTC')
            )
            ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (account)
            """
        )

    def ensure_slack_tables(self) -> None:
        self._command(
            """
            CREATE TABLE IF NOT EXISTS slack_teams (
                account LowCardinality(String),
                team_id String,
                team_name String,
                domain String,
                enterprise_id String,
                raw_json String,
                synced_at DateTime64(3, 'UTC'),
                sync_version UInt64
            )
            ENGINE = ReplacingMergeTree(sync_version)
            ORDER BY (account, team_id)
            """
        )
        self._command(
            """
            CREATE TABLE IF NOT EXISTS slack_users (
                account LowCardinality(String),
                team_id String,
                user_id String,
                team_user_id String,
                name String,
                real_name String,
                display_name String,
                email String,
                is_bot UInt8,
                is_app_user UInt8,
                is_deleted UInt8,
                tz String,
                raw_json String,
                synced_at DateTime64(3, 'UTC'),
                sync_version UInt64
            )
            ENGINE = ReplacingMergeTree(sync_version)
            ORDER BY (account, team_id, user_id)
            """
        )
        self._command(
            """
            CREATE TABLE IF NOT EXISTS slack_conversations (
                account LowCardinality(String),
                team_id String,
                conversation_id String,
                conversation_type LowCardinality(String),
                name String,
                is_channel UInt8,
                is_group UInt8,
                is_im UInt8,
                is_mpim UInt8,
                is_private UInt8,
                is_archived UInt8,
                is_member UInt8,
                creator String,
                created_at DateTime64(3, 'UTC'),
                topic String,
                purpose String,
                num_members UInt32,
                raw_json String,
                synced_at DateTime64(3, 'UTC'),
                sync_version UInt64
            )
            ENGINE = ReplacingMergeTree(sync_version)
            ORDER BY (account, team_id, conversation_id)
            """
        )
        self._command(
            """
            CREATE TABLE IF NOT EXISTS slack_conversation_members (
                account LowCardinality(String),
                team_id String,
                conversation_id String,
                user_id String,
                is_deleted UInt8,
                synced_at DateTime64(3, 'UTC'),
                sync_version UInt64
            )
            ENGINE = ReplacingMergeTree(sync_version)
            ORDER BY (account, team_id, conversation_id, user_id)
            """
        )
        self._command(
            """
            CREATE TABLE IF NOT EXISTS slack_messages (
                account LowCardinality(String),
                team_id String,
                conversation_id String,
                message_ts String,
                message_datetime DateTime64(6, 'UTC'),
                thread_ts String,
                parent_message_ts String,
                user_id String,
                bot_id String,
                username String,
                type LowCardinality(String),
                subtype LowCardinality(String),
                text String,
                blocks_json String,
                attachments_json String,
                is_thread_parent UInt8,
                is_thread_reply UInt8,
                reply_count UInt32,
                reply_users_count UInt32,
                latest_reply_ts String,
                edited_ts String,
                client_msg_id String,
                is_deleted UInt8,
                raw_json String,
                synced_at DateTime64(3, 'UTC'),
                sync_version UInt64
            )
            ENGINE = ReplacingMergeTree(sync_version)
            PARTITION BY toYYYYMM(message_datetime)
            ORDER BY (account, team_id, conversation_id, message_ts)
            """
        )
        self._command(
            """
            CREATE TABLE IF NOT EXISTS slack_message_reactions (
                account LowCardinality(String),
                team_id String,
                conversation_id String,
                message_ts String,
                reaction_name String,
                user_id String,
                reaction_count UInt32,
                is_deleted UInt8,
                raw_json String,
                synced_at DateTime64(3, 'UTC'),
                sync_version UInt64
            )
            ENGINE = ReplacingMergeTree(sync_version)
            ORDER BY (account, team_id, conversation_id, message_ts, reaction_name, user_id)
            """
        )
        self._command(
            """
            CREATE TABLE IF NOT EXISTS slack_files (
                account LowCardinality(String),
                team_id String,
                file_id String,
                conversation_id String,
                message_ts String,
                user_id String,
                created_at DateTime64(3, 'UTC'),
                name String,
                title String,
                mimetype String,
                filetype String,
                url_private String,
                size UInt64,
                is_deleted UInt8,
                raw_json String,
                synced_at DateTime64(3, 'UTC'),
                sync_version UInt64
            )
            ENGINE = ReplacingMergeTree(sync_version)
            ORDER BY (account, team_id, file_id, conversation_id, message_ts)
            """
        )
        self._command(
            """
            CREATE TABLE IF NOT EXISTS slack_sync_state (
                account LowCardinality(String),
                team_id String,
                object_type LowCardinality(String),
                object_id String,
                cursor_ts String,
                last_sync_type LowCardinality(String),
                status LowCardinality(String),
                error String,
                updated_at DateTime64(3, 'UTC'),
                sync_version UInt64
            )
            ENGINE = ReplacingMergeTree(sync_version)
            ORDER BY (account, team_id, object_type, object_id)
            """
        )
        self._command(
            """
            CREATE OR REPLACE VIEW slack_conversation_timeline AS
            SELECT
                m.account AS account,
                m.team_id AS team_id,
                c.conversation_type,
                c.name AS conversation_name,
                m.conversation_id AS conversation_id,
                m.message_ts AS message_ts,
                m.message_datetime AS message_datetime,
                m.thread_ts AS thread_ts,
                m.user_id AS user_id,
                u.display_name,
                u.real_name,
                u.name AS user_name,
                m.text AS text,
                m.reply_count AS reply_count,
                m.latest_reply_ts AS latest_reply_ts,
                m.raw_json AS raw_json
            FROM (SELECT * FROM slack_messages FINAL) AS m
            LEFT JOIN (SELECT * FROM slack_conversations FINAL) AS c
                ON m.account = c.account AND m.team_id = c.team_id AND m.conversation_id = c.conversation_id
            LEFT JOIN (SELECT * FROM slack_users FINAL) AS u
                ON m.account = u.account AND m.team_id = u.team_id AND m.user_id = u.user_id
            WHERE m.is_deleted = 0 AND m.is_thread_reply = 0
            ORDER BY m.account, m.team_id, m.conversation_id, m.message_datetime
            """
        )
        self._command(
            """
            CREATE OR REPLACE VIEW slack_thread_messages AS
            SELECT
                m.account AS account,
                m.team_id AS team_id,
                m.conversation_id AS conversation_id,
                c.name AS conversation_name,
                m.thread_ts AS thread_ts,
                m.message_ts AS message_ts,
                m.message_datetime AS message_datetime,
                m.user_id AS user_id,
                u.display_name,
                u.real_name,
                u.name AS user_name,
                m.text AS text,
                m.raw_json AS raw_json
            FROM (SELECT * FROM slack_messages FINAL) AS m
            LEFT JOIN (SELECT * FROM slack_conversations FINAL) AS c
                ON m.account = c.account AND m.team_id = c.team_id AND m.conversation_id = c.conversation_id
            LEFT JOIN (SELECT * FROM slack_users FINAL) AS u
                ON m.account = u.account AND m.team_id = u.team_id AND m.user_id = u.user_id
            WHERE m.is_deleted = 0
            ORDER BY m.account, m.team_id, m.conversation_id, m.thread_ts, m.message_datetime, m.message_ts
            """
        )
        self._command(
            """
            CREATE OR REPLACE VIEW slack_ui_messages AS
            SELECT
                m.account AS account,
                m.team_id AS team_id,
                c.conversation_type,
                c.name AS conversation_name,
                m.conversation_id AS conversation_id,
                m.message_ts AS message_ts,
                m.message_datetime AS message_datetime,
                m.thread_ts AS thread_ts,
                m.parent_message_ts AS parent_message_ts,
                m.is_thread_parent AS is_thread_parent,
                m.is_thread_reply AS is_thread_reply,
                m.user_id AS user_id,
                u.display_name,
                u.real_name,
                u.name AS user_name,
                m.text AS text,
                m.reply_count AS reply_count,
                m.latest_reply_ts AS latest_reply_ts,
                m.raw_json AS raw_json
            FROM (SELECT * FROM slack_messages FINAL) AS m
            LEFT JOIN (SELECT * FROM slack_conversations FINAL) AS c
                ON m.account = c.account AND m.team_id = c.team_id AND m.conversation_id = c.conversation_id
            LEFT JOIN (SELECT * FROM slack_users FINAL) AS u
                ON m.account = u.account AND m.team_id = u.team_id AND m.user_id = u.user_id
            WHERE m.is_deleted = 0
            ORDER BY m.account, m.team_id, m.message_datetime, m.message_ts
            """
        )

    def load_sync_state(self) -> dict[str, SyncState]:
        rows = self._query(
            """
            SELECT
                account,
                last_history_id,
                last_sync_type,
                status,
                error,
                updated_at
            FROM gmail_sync_state FINAL
            """
        )
        states: dict[str, SyncState] = {}
        for row in rows:
            state = SyncState(
                account=row[0],
                last_history_id=int(row[1]),
                last_sync_type=row[2],
                status=row[3],
                error=row[4],
                updated_at=row[5],
            )
            states[state.account] = state
        return states

    def insert_messages(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        self._insert(
            "gmail_messages",
            [tuple(row[column] for column in MESSAGE_COLUMNS) for row in rows],
            MESSAGE_COLUMNS,
        )

    def existing_message_ids(self, *, account: str, message_ids: list[str]) -> set[str]:
        if not message_ids:
            return set()
        account_value = _sql_string(account)
        id_values = ", ".join(_sql_string(message_id) for message_id in message_ids)
        rows = self._query(
            f"""
            SELECT message_id
            FROM gmail_messages FINAL
            WHERE account = {account_value}
              AND is_deleted = 0
              AND message_id IN ({id_values})
            """
        )
        return {str(row[0]) for row in rows}

    def insert_sync_state(
        self,
        *,
        account: str,
        last_history_id: int,
        last_sync_type: str,
        status: str,
        error: str,
        updated_at: datetime,
    ) -> None:
        self._insert(
            "gmail_sync_state",
            [
                (
                    account,
                    int(last_history_id),
                    last_sync_type,
                    status,
                    error,
                    updated_at,
                )
            ],
            SYNC_STATE_COLUMNS,
        )

    def load_slack_sync_state(self) -> dict[tuple[str, str, str, str], dict[str, Any]]:
        rows = self._query(
            """
            SELECT
                account,
                team_id,
                object_type,
                object_id,
                cursor_ts,
                last_sync_type,
                status,
                error,
                updated_at
            FROM slack_sync_state FINAL
            """
        )
        states: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        for row in rows:
            key = (str(row[0]), str(row[1]), str(row[2]), str(row[3]))
            states[key] = {
                "account": row[0],
                "team_id": row[1],
                "object_type": row[2],
                "object_id": row[3],
                "cursor_ts": row[4],
                "last_sync_type": row[5],
                "status": row[6],
                "error": row[7],
                "updated_at": row[8],
            }
        return states

    def insert_slack_teams(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("slack_teams", rows, SLACK_TEAM_COLUMNS)

    def insert_slack_users(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("slack_users", rows, SLACK_USER_COLUMNS)

    def insert_slack_conversations(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("slack_conversations", rows, SLACK_CONVERSATION_COLUMNS)

    def load_slack_conversation_payloads(
        self,
        *,
        account: str,
        team_id: str,
        include_archived: bool = False,
        archived_only: bool = False,
        conversation_types: tuple[str, ...] = (),
    ) -> list[dict[str, Any]]:
        archived_clause = ""
        if archived_only:
            archived_clause = "AND is_archived = 1"
        elif not include_archived:
            archived_clause = "AND is_archived = 0"
        type_clause = ""
        if conversation_types:
            values = ", ".join(_sql_string(conversation_type) for conversation_type in conversation_types)
            type_clause = f"AND conversation_type IN ({values})"
        rows = self._query(
            f"""
            SELECT raw_json
            FROM slack_conversations FINAL
            WHERE account = {_sql_string(account)}
              AND team_id = {_sql_string(team_id)}
              {archived_clause}
              {type_clause}
            """
        )
        payloads = []
        for (raw_json,) in rows:
            try:
                payloads.append(json.loads(raw_json))
            except json.JSONDecodeError:
                continue
        return payloads

    def load_slack_thread_parent_refs(
        self,
        *,
        account: str,
        team_id: str,
        since_ts: float | None = None,
        limit: int | None = None,
        skip_completed: bool = False,
        order: str = "recent",
    ) -> list[dict[str, Any]]:
        where_clauses = [
            f"m.account = {_sql_string(account)}",
            f"m.team_id = {_sql_string(team_id)}",
            "m.is_deleted = 0",
            "m.reply_count > 0",
            "m.is_thread_reply = 0",
        ]
        if since_ts is not None:
            where_clauses.append(f"toFloat64(m.message_ts) >= {since_ts:.6f}")
        if skip_completed:
            where_clauses.append("s.object_id = ''")
        order_by = "m.message_datetime DESC, m.message_ts DESC"
        if order == "reply_count":
            order_by = "m.reply_count DESC, m.message_datetime DESC, m.message_ts DESC"
        limit_clause = f"LIMIT {int(limit)}" if limit is not None else ""
        rows = self._query(
            f"""
            SELECT
                m.conversation_id,
                m.message_ts,
                m.reply_count,
                m.latest_reply_ts,
                m.message_datetime
            FROM (SELECT * FROM slack_messages FINAL) AS m
            LEFT JOIN (SELECT * FROM slack_sync_state FINAL) AS s
                ON m.account = s.account
                AND m.team_id = s.team_id
                AND s.object_type = 'thread'
                AND concat(m.conversation_id, ':', m.message_ts) = s.object_id
                AND s.status = 'ok'
            WHERE {" AND ".join(where_clauses)}
            ORDER BY {order_by}
            {limit_clause}
            """
        )
        return [
            {
                "conversation_id": str(row[0]),
                "thread_ts": str(row[1]),
                "reply_count": int(row[2]),
                "latest_reply_ts": str(row[3]),
                "message_datetime": row[4],
            }
            for row in rows
        ]

    def insert_slack_conversation_members(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("slack_conversation_members", rows, SLACK_CONVERSATION_MEMBER_COLUMNS)

    def insert_slack_messages(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("slack_messages", rows, SLACK_MESSAGE_COLUMNS)

    def insert_slack_message_reactions(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("slack_message_reactions", rows, SLACK_REACTION_COLUMNS)

    def insert_slack_files(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("slack_files", rows, SLACK_FILE_COLUMNS)

    def insert_slack_sync_state(
        self,
        *,
        account: str,
        team_id: str,
        object_type: str,
        object_id: str,
        cursor_ts: str,
        last_sync_type: str,
        status: str,
        error: str,
        updated_at: datetime,
        sync_version: int,
    ) -> None:
        self._insert(
            "slack_sync_state",
            [
                (
                    account,
                    team_id,
                    object_type,
                    object_id,
                    cursor_ts,
                    last_sync_type,
                    status,
                    error,
                    updated_at,
                    int(sync_version),
                )
            ],
            SLACK_SYNC_STATE_COLUMNS,
        )

    def existing_slack_message_ids(
        self,
        *,
        account: str,
        team_id: str,
        conversation_id: str,
        oldest_ts: str,
        latest_ts: str,
    ) -> set[str]:
        rows = self._query(
            f"""
            SELECT message_ts
            FROM slack_messages FINAL
            WHERE account = {_sql_string(account)}
              AND team_id = {_sql_string(team_id)}
              AND conversation_id = {_sql_string(conversation_id)}
              AND is_deleted = 0
              AND toFloat64(message_ts) >= toFloat64({_sql_string(oldest_ts)})
              AND toFloat64(message_ts) <= toFloat64({_sql_string(latest_ts)})
            """
        )
        return {str(row[0]) for row in rows}

    def _command(self, sql: str) -> None:
        def run() -> None:
            if self._client_type == "native":
                self._client.execute(sql)
            else:
                self._client.command(sql)

        self._with_clickhouse_retries(run)

    def _query(self, sql: str) -> list[tuple[Any, ...]]:
        def run() -> list[tuple[Any, ...]]:
            if self._client_type == "native":
                return self._client.execute(sql)
            return self._client.query(sql).result_rows

        return self._with_clickhouse_retries(run)

    def _insert(self, table: str, rows: list[tuple[Any, ...]], columns: tuple[str, ...]) -> None:
        def run() -> None:
            if self._client_type == "native":
                quoted_columns = ", ".join(columns)
                self._client.execute(f"INSERT INTO {table} ({quoted_columns}) VALUES", rows)
            else:
                self._client.insert(table, rows, column_names=list(columns))

        self._with_clickhouse_retries(run)

    def _insert_rows(self, table: str, rows: list[dict[str, Any]], columns: tuple[str, ...]) -> None:
        if not rows:
            return
        self._insert(table, [tuple(row[column] for column in columns) for row in rows], columns)

    def _with_clickhouse_retries(self, operation):
        for attempt in range(1, 6):
            try:
                return operation()
            except (NetworkError, SocketTimeoutError, TimeoutError, ConnectionError, OSError, EOFError):
                if attempt == 5:
                    raise
                time.sleep(min(60, 5 * attempt))
        raise RuntimeError("unreachable ClickHouse retry state")


def _parse_clickhouse_url(clickhouse_url: str) -> dict[str, Any]:
    parsed = urlparse(clickhouse_url)
    if parsed.scheme in {"clickhouse", "clickhouses"}:
        return {
            "client_type": "native",
            "kwargs": _native_client_kwargs_from_url(clickhouse_url),
        }
    return {
        "client_type": "http",
        "kwargs": _http_client_kwargs_from_url(clickhouse_url),
    }


def _http_client_kwargs_from_url(clickhouse_url: str) -> dict[str, Any]:
    parsed = urlparse(clickhouse_url)
    query = parse_qs(parsed.query)
    secure_from_scheme = parsed.scheme in {"https", "clickhouses", "clickhouse+https"}
    secure = _query_bool(query.get("secure", []), default=secure_from_scheme)

    database = parsed.path.lstrip("/") or _first(query.get("database"))
    port = parsed.port or (8443 if secure else 8123)

    return {
        "host": parsed.hostname,
        "port": port,
        "username": unquote(parsed.username) if parsed.username else "default",
        "password": unquote(parsed.password) if parsed.password else "",
        "database": database,
        "secure": secure,
    }


def _native_client_kwargs_from_url(clickhouse_url: str) -> dict[str, Any]:
    parsed = urlparse(clickhouse_url)
    query = parse_qs(parsed.query)
    secure = _query_bool(query.get("secure", []), default=parsed.scheme == "clickhouses")
    database = parsed.path.lstrip("/") or _first(query.get("database")) or "default"
    port = parsed.port or (9440 if secure else 9000)

    return {
        "host": parsed.hostname,
        "port": port,
        "user": unquote(parsed.username) if parsed.username else "default",
        "password": unquote(parsed.password) if parsed.password else "",
        "database": database,
        "secure": secure,
    }


def _first(values: list[str] | None) -> str | None:
    if not values:
        return None
    return values[0]


def _query_bool(values: list[str], *, default: bool) -> bool:
    if not values:
        return default
    return values[0].strip().lower() in {"1", "true", "yes", "y", "on"}


def _sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
