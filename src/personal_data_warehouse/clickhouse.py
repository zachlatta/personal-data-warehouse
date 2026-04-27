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

ATTACHMENT_COLUMNS = (
    "account",
    "message_id",
    "thread_id",
    "history_id",
    "internal_date",
    "part_id",
    "attachment_id",
    "filename",
    "mime_type",
    "content_id",
    "content_disposition",
    "size",
    "content_sha256",
    "text",
    "text_extraction_status",
    "text_extraction_error",
    "ai_provider",
    "ai_model",
    "ai_base_url",
    "ai_prompt_version",
    "ai_prompt_sha256",
    "ai_prompt",
    "ai_source_status",
    "ai_elapsed_ms",
    "ai_processed_at",
    "is_deleted",
    "part_json",
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

ATTACHMENT_BACKFILL_STATE_COLUMNS = (
    "account",
    "message_id",
    "status",
    "attachment_rows_written",
    "error",
    "ai_provider",
    "ai_model",
    "ai_prompt_version",
    "updated_at",
    "sync_version",
)

CALENDAR_EVENT_COLUMNS = (
    "account",
    "calendar_id",
    "event_id",
    "recurring_event_id",
    "i_cal_uid",
    "status",
    "is_deleted",
    "summary",
    "description",
    "location",
    "creator_email",
    "organizer_email",
    "start_at",
    "end_at",
    "start_date",
    "end_date",
    "is_all_day",
    "html_link",
    "attendees_json",
    "reminders_json",
    "recurrence",
    "event_type",
    "raw_json",
    "updated_at",
    "synced_at",
    "sync_version",
)

CALENDAR_SYNC_STATE_COLUMNS = (
    "account",
    "calendar_id",
    "sync_token",
    "last_sync_type",
    "status",
    "error",
    "updated_at",
    "sync_version",
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

SLACK_ACCOUNT_IDENTITY_COLUMNS = (
    "account",
    "team_id",
    "user_id",
    "team_name",
    "url",
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
SLACK_CONVERSATION_READ_STATE_FIELDS = (
    "last_read",
    "unread_count",
    "unread_count_display",
    "is_open",
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

SLACK_ACCOUNT_STATE_ITEM_ROW_COLUMNS = (
    "source",
    "account",
    "scope_id",
    "item_id",
    "item_type",
    "item_state",
    "priority_rank",
    "latest_activity_at",
    "container_id",
    "container_name",
    "thread_id",
    "message_id",
    "actor_id",
    "actor_name",
    "title",
    "preview",
    "unread_count",
    "reason",
    "source_table",
    "drilldown_hint",
    "is_deleted",
    "synced_at",
    "sync_version",
)

OBSOLETE_VIEWS = (
    "calendar_event_search",
    "gmail_attachment_search",
    "gmail_thread_messages",
    "slack_ui_messages",
    "slack_conversation_timeline",
    "slack_thread_messages",
    "slack_current_conversations",
    "slack_current_threads",
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
        self._drop_obsolete_views()

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
            CREATE TABLE IF NOT EXISTS gmail_attachments (
                account LowCardinality(String),
                message_id String,
                thread_id String,
                history_id UInt64,
                internal_date DateTime64(3, 'UTC'),
                part_id String,
                attachment_id String,
                filename String,
                mime_type LowCardinality(String),
                content_id String,
                content_disposition String,
                size UInt64,
                content_sha256 String,
                text String,
                text_extraction_status LowCardinality(String),
                text_extraction_error String,
                ai_provider LowCardinality(String),
                ai_model String,
                ai_base_url String,
                ai_prompt_version LowCardinality(String),
                ai_prompt_sha256 String,
                ai_prompt String,
                ai_source_status LowCardinality(String),
                ai_elapsed_ms UInt64,
                ai_processed_at DateTime64(3, 'UTC'),
                is_deleted UInt8,
                part_json String,
                synced_at DateTime64(3, 'UTC'),
                sync_version UInt64
            )
            ENGINE = ReplacingMergeTree(sync_version)
            PARTITION BY toYYYYMM(synced_at)
            ORDER BY (account, message_id, part_id, filename)
            """
        )
        self._command(
            "ALTER TABLE gmail_attachments ADD COLUMN IF NOT EXISTS ai_provider LowCardinality(String) AFTER text_extraction_error"
        )
        self._command(
            "ALTER TABLE gmail_attachments ADD COLUMN IF NOT EXISTS ai_model String AFTER ai_provider"
        )
        self._command(
            "ALTER TABLE gmail_attachments ADD COLUMN IF NOT EXISTS ai_base_url String AFTER ai_model"
        )
        self._command(
            "ALTER TABLE gmail_attachments ADD COLUMN IF NOT EXISTS ai_prompt_version LowCardinality(String) AFTER ai_base_url"
        )
        self._command(
            "ALTER TABLE gmail_attachments ADD COLUMN IF NOT EXISTS ai_prompt_sha256 String AFTER ai_prompt_version"
        )
        self._command(
            "ALTER TABLE gmail_attachments ADD COLUMN IF NOT EXISTS ai_prompt String AFTER ai_prompt_sha256"
        )
        self._command(
            "ALTER TABLE gmail_attachments ADD COLUMN IF NOT EXISTS ai_source_status LowCardinality(String) AFTER ai_prompt"
        )
        self._command(
            "ALTER TABLE gmail_attachments ADD COLUMN IF NOT EXISTS ai_elapsed_ms UInt64 AFTER ai_source_status"
        )
        self._command(
            "ALTER TABLE gmail_attachments ADD COLUMN IF NOT EXISTS ai_processed_at DateTime64(3, 'UTC') AFTER ai_elapsed_ms"
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
        self._command(
            """
            CREATE TABLE IF NOT EXISTS gmail_attachment_backfill_state (
                account LowCardinality(String),
                message_id String,
                status LowCardinality(String),
                attachment_rows_written UInt32,
                error String,
                ai_provider LowCardinality(String),
                ai_model String,
                ai_prompt_version LowCardinality(String),
                updated_at DateTime64(3, 'UTC'),
                sync_version UInt64
            )
            ENGINE = ReplacingMergeTree(sync_version)
            ORDER BY (account, message_id)
            """
        )
        self._command(
            "ALTER TABLE gmail_attachment_backfill_state ADD COLUMN IF NOT EXISTS ai_provider LowCardinality(String) AFTER error"
        )
        self._command(
            "ALTER TABLE gmail_attachment_backfill_state ADD COLUMN IF NOT EXISTS ai_model String AFTER ai_provider"
        )
        self._command(
            "ALTER TABLE gmail_attachment_backfill_state ADD COLUMN IF NOT EXISTS ai_prompt_version LowCardinality(String) AFTER ai_model"
        )
        self._ensure_gmail_account_state_view()
        self._ensure_combined_account_state_view_if_possible()

    def ensure_calendar_tables(self) -> None:
        self._drop_obsolete_views()

        self._command(
            """
            CREATE TABLE IF NOT EXISTS calendar_events (
                account LowCardinality(String),
                calendar_id String,
                event_id String,
                recurring_event_id String,
                i_cal_uid String,
                status LowCardinality(String),
                is_deleted UInt8,
                summary String,
                description String,
                location String,
                creator_email String,
                organizer_email String,
                start_at DateTime64(3, 'UTC'),
                end_at DateTime64(3, 'UTC'),
                start_date String,
                end_date String,
                is_all_day UInt8,
                html_link String,
                attendees_json String,
                reminders_json String,
                recurrence Array(String),
                event_type LowCardinality(String),
                raw_json String,
                updated_at DateTime64(3, 'UTC'),
                synced_at DateTime64(3, 'UTC'),
                sync_version UInt64
            )
            ENGINE = ReplacingMergeTree(sync_version)
            PARTITION BY toYYYYMM(synced_at)
            ORDER BY (account, calendar_id, event_id)
            """
        )
        self._command(
            """
            CREATE TABLE IF NOT EXISTS calendar_sync_state (
                account LowCardinality(String),
                calendar_id String,
                sync_token String,
                last_sync_type LowCardinality(String),
                status LowCardinality(String),
                error String,
                updated_at DateTime64(3, 'UTC'),
                sync_version UInt64
            )
            ENGINE = ReplacingMergeTree(sync_version)
            ORDER BY (account, calendar_id)
            """
        )

    def ensure_slack_tables(self) -> None:
        self._drop_obsolete_views()

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
            CREATE TABLE IF NOT EXISTS slack_account_identities (
                account LowCardinality(String),
                team_id String,
                user_id String,
                team_name String,
                url String,
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
        self._command("DROP TABLE IF EXISTS slack_account_state_items")
        self._command(
            """
            CREATE TABLE IF NOT EXISTS slack_account_state_item_rows (
                source LowCardinality(String),
                account LowCardinality(String),
                scope_id String,
                item_id String,
                item_type LowCardinality(String),
                item_state LowCardinality(String),
                priority_rank UInt8,
                latest_activity_at DateTime64(6, 'UTC'),
                container_id String,
                container_name String,
                thread_id String,
                message_id String,
                actor_id String,
                actor_name String,
                title String,
                preview String,
                unread_count UInt64,
                reason String,
                source_table LowCardinality(String),
                drilldown_hint String,
                is_deleted UInt8,
                synced_at DateTime64(3, 'UTC'),
                sync_version UInt64
            )
            ENGINE = ReplacingMergeTree(sync_version)
            ORDER BY (source, account, scope_id, item_id)
            """
        )
        self._ensure_slack_account_state_view()
        self._ensure_combined_account_state_view_if_possible()

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

    def insert_attachments(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("gmail_attachments", rows, ATTACHMENT_COLUMNS)

    def load_attachment_backfill_candidate_messages(
        self,
        *,
        account: str,
        limit: int,
        ai_provider: str = "",
        ai_model: str = "",
        ai_prompt_version: str = "",
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        rows = self._query(
            f"""
            SELECT
                payload_json
            FROM gmail_messages FINAL
            WHERE account = {_sql_string(account)}
              AND is_deleted = 0
              AND {_gmail_attachment_candidate_clause()}
              AND message_id NOT IN (
                  SELECT message_id
                  FROM gmail_attachment_backfill_state FINAL
                  WHERE account = {_sql_string(account)}
                    AND status = 'ok'
                    AND ai_provider = {_sql_string(ai_provider)}
                    AND ai_model = {_sql_string(ai_model)}
                    AND ai_prompt_version = {_sql_string(ai_prompt_version)}
              )
            ORDER BY internal_date DESC, message_id DESC
            LIMIT {int(limit)}
            """
        )
        messages: list[dict[str, Any]] = []
        for (payload_json,) in rows:
            try:
                parsed = json.loads(str(payload_json))
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                messages.append(parsed)
        return messages

    def insert_attachment_backfill_state(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows(
            "gmail_attachment_backfill_state",
            rows,
            ATTACHMENT_BACKFILL_STATE_COLUMNS,
        )

    def insert_calendar_events(self, rows: list[dict[str, Any]]) -> None:
        rows_by_partition: dict[tuple[int, int], list[dict[str, Any]]] = {}
        for row in rows:
            rows_by_partition.setdefault(_calendar_event_partition_key(row), []).append(row)
        for partition_rows in rows_by_partition.values():
            self._insert_rows("calendar_events", partition_rows, CALENDAR_EVENT_COLUMNS)

    def load_calendar_sync_state(self) -> dict[tuple[str, str], dict[str, Any]]:
        rows = self._query(
            """
            SELECT
                account,
                calendar_id,
                sync_token,
                last_sync_type,
                status,
                error,
                updated_at
            FROM calendar_sync_state FINAL
            """
        )
        states: dict[tuple[str, str], dict[str, Any]] = {}
        for row in rows:
            key = (str(row[0]), str(row[1]))
            states[key] = {
                "account": row[0],
                "calendar_id": row[1],
                "sync_token": row[2],
                "last_sync_type": row[3],
                "status": row[4],
                "error": row[5],
                "updated_at": row[6],
            }
        return states

    def insert_calendar_sync_state(
        self,
        *,
        account: str,
        calendar_id: str,
        sync_token: str,
        last_sync_type: str,
        status: str,
        error: str,
        updated_at: datetime,
    ) -> None:
        self._insert(
            "calendar_sync_state",
            [
                (
                    account,
                    calendar_id,
                    sync_token,
                    last_sync_type,
                    status,
                    error,
                    updated_at,
                    int(updated_at.timestamp() * 1_000_000),
                )
            ],
            CALENDAR_SYNC_STATE_COLUMNS,
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

    def existing_attachment_keys(
        self,
        *,
        account: str,
        message_ids: list[str],
    ) -> set[tuple[str, str, str]]:
        if not message_ids:
            return set()
        account_value = _sql_string(account)
        id_values = ", ".join(_sql_string(message_id) for message_id in message_ids)
        rows = self._query(
            f"""
            SELECT
                message_id,
                part_id,
                filename
            FROM gmail_attachments FINAL
            WHERE account = {account_value}
              AND is_deleted = 0
              AND message_id IN ({id_values})
            """
        )
        return {(str(row[0]), str(row[1]), str(row[2])) for row in rows}

    def load_message_payloads(
        self,
        *,
        account: str,
        message_ids: list[str],
    ) -> dict[str, dict[str, Any]]:
        if not message_ids:
            return {}
        account_value = _sql_string(account)
        id_values = ", ".join(_sql_string(message_id) for message_id in message_ids)
        rows = self._query(
            f"""
            SELECT
                message_id,
                payload_json
            FROM gmail_messages FINAL
            WHERE account = {account_value}
              AND is_deleted = 0
              AND message_id IN ({id_values})
            """
        )
        payloads: dict[str, dict[str, Any]] = {}
        for message_id, payload_json in rows:
            try:
                parsed = json.loads(str(payload_json))
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                payloads[str(message_id)] = parsed
        return payloads

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

    def insert_slack_account_identities(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("slack_account_identities", rows, SLACK_ACCOUNT_IDENTITY_COLUMNS)

    def insert_slack_users(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("slack_users", rows, SLACK_USER_COLUMNS)

    def insert_slack_conversations(self, rows: list[dict[str, Any]]) -> None:
        rows = self._preserve_slack_conversation_read_state(rows)
        self._insert_rows("slack_conversations", rows, SLACK_CONVERSATION_COLUMNS)

    def _preserve_slack_conversation_read_state(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows_missing_read_state = []
        for row in rows:
            try:
                payload = json.loads(str(row.get("raw_json", "")))
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue
            if any(_missing_json_field(payload, field) for field in SLACK_CONVERSATION_READ_STATE_FIELDS):
                rows_missing_read_state.append(row)
        if not rows_missing_read_state:
            return rows

        ids_by_scope: dict[tuple[str, str], set[str]] = {}
        for row in rows_missing_read_state:
            scope = (str(row["account"]), str(row["team_id"]))
            ids_by_scope.setdefault(scope, set()).add(str(row["conversation_id"]))

        existing_payloads: dict[tuple[str, str, str], dict[str, Any]] = {}
        for (account, team_id), conversation_ids in ids_by_scope.items():
            id_values = ", ".join(_sql_string(conversation_id) for conversation_id in sorted(conversation_ids))
            existing_rows = self._query(
                f"""
                SELECT conversation_id, raw_json
                FROM slack_conversations FINAL
                WHERE account = {_sql_string(account)}
                  AND team_id = {_sql_string(team_id)}
                  AND conversation_id IN ({id_values})
                """
            )
            for conversation_id, raw_json in existing_rows:
                try:
                    existing_payload = json.loads(str(raw_json))
                except json.JSONDecodeError:
                    continue
                if isinstance(existing_payload, dict):
                    existing_payloads[(account, team_id, str(conversation_id))] = existing_payload

        preserved_rows = []
        for row in rows:
            key = (str(row["account"]), str(row["team_id"]), str(row["conversation_id"]))
            existing_payload = existing_payloads.get(key)
            if not existing_payload:
                preserved_rows.append(row)
                continue
            try:
                payload = json.loads(str(row.get("raw_json", "")))
            except json.JSONDecodeError:
                preserved_rows.append(row)
                continue
            if not isinstance(payload, dict):
                preserved_rows.append(row)
                continue
            changed = False
            for field in SLACK_CONVERSATION_READ_STATE_FIELDS:
                if _missing_json_field(payload, field) and not _missing_json_field(existing_payload, field):
                    payload[field] = existing_payload[field]
                    changed = True
            if not changed:
                preserved_rows.append(row)
                continue
            preserved_row = dict(row)
            preserved_row["raw_json"] = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
            preserved_rows.append(preserved_row)
        return preserved_rows

    def load_slack_conversation_payloads(
        self,
        *,
        account: str,
        team_id: str,
        include_archived: bool = False,
        archived_only: bool = False,
        conversation_types: tuple[str, ...] = (),
        not_full_only: bool = False,
        zero_messages_only: bool = False,
        skip_known_errors: bool = False,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        archived_clause = ""
        if archived_only:
            archived_clause = "AND c.is_archived = 1"
        elif not include_archived:
            archived_clause = "AND c.is_archived = 0"
        type_clause = ""
        if conversation_types:
            values = ", ".join(_sql_string(conversation_type) for conversation_type in conversation_types)
            type_clause = f"AND c.conversation_type IN ({values})"
        backlog_clauses = []
        if not_full_only:
            backlog_clauses.append("NOT (s.status = 'ok' AND s.last_sync_type = 'full')")
        if zero_messages_only:
            backlog_clauses.append("m.message_count = 0")
        if skip_known_errors:
            backlog_clauses.append("s.status != 'error'")
        backlog_clause = ""
        if backlog_clauses:
            backlog_clause = "AND " + " AND ".join(backlog_clauses)
        limit_clause = f"LIMIT {int(limit)}" if limit is not None else ""
        rows = self._query(
            f"""
            SELECT c.raw_json
            FROM (SELECT * FROM slack_conversations FINAL) AS c
            LEFT JOIN (SELECT * FROM slack_sync_state FINAL WHERE object_type = 'conversation') AS s
                ON c.account = s.account AND c.team_id = s.team_id AND c.conversation_id = s.object_id
            LEFT JOIN (
                SELECT account, team_id, conversation_id, count() AS message_count
                FROM slack_messages FINAL
                WHERE is_deleted = 0
                GROUP BY account, team_id, conversation_id
            ) AS m
                ON c.account = m.account AND c.team_id = m.team_id AND c.conversation_id = m.conversation_id
            WHERE c.account = {_sql_string(account)}
              AND c.team_id = {_sql_string(team_id)}
              {archived_clause}
              {type_clause}
              {backlog_clause}
            ORDER BY
                NOT (s.status = 'ok' AND s.last_sync_type = 'full') DESC,
                m.message_count = 0 DESC,
                multiIf(
                    c.conversation_type = 'im', 1,
                    c.conversation_type = 'mpim', 2,
                    c.conversation_type = 'private_channel', 3,
                    c.conversation_type = 'public_channel', 4,
                    5
                ),
                c.is_archived,
                c.conversation_id
            {limit_clause}
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
        skip_known_errors: bool = False,
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
        if skip_known_errors:
            where_clauses.append("(s.object_id = '' OR s.status != 'error')")
        if skip_completed:
            where_clauses.append(
                "("
                "s.object_id = '' "
                "OR s.status != 'ok' "
                "OR (m.latest_reply_ts != '' AND s.cursor_ts != '' AND toFloat64(m.latest_reply_ts) > toFloat64(s.cursor_ts))"
                ")"
            )
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

    def load_slack_read_state_candidate_payloads(
        self,
        *,
        account: str,
        team_id: str,
        conversation_types: tuple[str, ...] = (),
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        type_clause = ""
        if conversation_types:
            values = ", ".join(_sql_string(conversation_type) for conversation_type in conversation_types)
            type_clause = f"AND c.conversation_type IN ({values})"
        limit_clause = f"LIMIT {int(limit)}" if limit is not None else ""
        rows = self._query(
            f"""
            SELECT c.raw_json
            FROM (SELECT * FROM slack_conversations FINAL) AS c
            LEFT JOIN (
                SELECT
                    account,
                    team_id,
                    conversation_id,
                    max(message_datetime) AS latest_message_at
                FROM slack_messages FINAL
                WHERE is_deleted = 0
                  AND message_datetime >= now('UTC') - INTERVAL 30 DAY
                GROUP BY account, team_id, conversation_id
            ) AS m
                ON c.account = m.account AND c.team_id = m.team_id AND c.conversation_id = m.conversation_id
            WHERE c.account = {_sql_string(account)}
              AND c.team_id = {_sql_string(team_id)}
              AND c.is_archived = 0
              AND (c.is_member = 1 OR c.is_im = 1 OR c.is_mpim = 1)
              AND m.latest_message_at IS NOT NULL
              {type_clause}
            ORDER BY
                toDecimal64OrZero(JSONExtractString(c.raw_json, 'last_read'), 6) = 0 DESC,
                m.latest_message_at DESC,
                multiIf(c.is_im = 1, 1, c.is_mpim = 1, 2, c.is_private = 1, 3, 4),
                c.conversation_id
            {limit_clause}
            """
        )
        payloads = []
        for (raw_json,) in rows:
            try:
                parsed = json.loads(raw_json)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                payloads.append(parsed)
        return payloads

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

    def refresh_slack_account_state_items(self, *, account: str, team_id: str, synced_at: datetime) -> None:
        sync_version = int(synced_at.timestamp() * 1_000_000)
        synced_at_sql = _sql_string(synced_at.isoformat())
        account_sql = _sql_string(account)
        team_id_sql = _sql_string(team_id)
        columns = ", ".join(SLACK_ACCOUNT_STATE_ITEM_ROW_COLUMNS)

        active_rows = self._query(
            f"""
            SELECT {columns}
            FROM slack_account_state_item_rows FINAL
            WHERE account = {account_sql}
              AND scope_id = {team_id_sql}
              AND is_deleted = 0
            """
        )
        self._command(
            f"""
            INSERT INTO slack_account_state_item_rows ({columns})
            {self._slack_account_state_items_select_sql(account=account, team_id=team_id, synced_at=synced_at, sync_version=sync_version + 1)}
            """
        )
        if active_rows:
            tombstones = []
            is_deleted_index = SLACK_ACCOUNT_STATE_ITEM_ROW_COLUMNS.index("is_deleted")
            synced_at_index = SLACK_ACCOUNT_STATE_ITEM_ROW_COLUMNS.index("synced_at")
            sync_version_index = SLACK_ACCOUNT_STATE_ITEM_ROW_COLUMNS.index("sync_version")
            for row in active_rows:
                values = list(row)
                values[is_deleted_index] = 1
                values[synced_at_index] = synced_at
                values[sync_version_index] = sync_version
                tombstones.append(tuple(values))
            self._insert("slack_account_state_item_rows", tombstones, SLACK_ACCOUNT_STATE_ITEM_ROW_COLUMNS)

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

    def _ensure_gmail_account_state_view(self) -> None:
        self._command(
            """
            CREATE OR REPLACE VIEW gmail_account_state_items AS
            SELECT
                'gmail' AS source,
                account AS account,
                '' AS scope_id,
                concat('gmail:', account, ':thread:', thread_id) AS item_id,
                multiIf(
                    unread_count > 0, 'unread_inbox_thread',
                    important_count > 0, 'important_inbox_thread',
                    starred_count > 0, 'starred_inbox_thread',
                    'inbox_thread'
                ) AS item_type,
                multiIf(unread_count > 0, 'unread', 'inbox') AS item_state,
                toUInt8(multiIf(unread_count > 0, 10, important_count > 0, 20, starred_count > 0, 30, 40)) AS priority_rank,
                latest_activity_at AS latest_activity_at,
                thread_id AS container_id,
                '' AS container_name,
                thread_id AS thread_id,
                latest_message_id AS message_id,
                latest_from_address AS actor_id,
                latest_from_address AS actor_name,
                latest_subject AS title,
                latest_preview AS preview,
                toUInt64(unread_count) AS unread_count,
                multiIf(
                    unread_count > 0, 'Unread Gmail inbox thread',
                    important_count > 0, 'Important Gmail inbox thread',
                    starred_count > 0, 'Starred Gmail inbox thread',
                    'Gmail inbox thread'
                ) AS reason,
                'gmail_messages' AS source_table,
                'Query gmail_messages by account and thread_id for full message history.' AS drilldown_hint
            FROM (
                SELECT
                    account,
                    thread_id,
                    max(internal_date) AS latest_activity_at,
                    argMax(message_id, internal_date) AS latest_message_id,
                    argMax(subject, internal_date) AS latest_subject,
                    argMax(from_address, internal_date) AS latest_from_address,
                    substring(
                        argMax(
                            if(
                                body_markdown_clean != '',
                                body_markdown_clean,
                                if(body_markdown != '', body_markdown, if(body_text != '', body_text, snippet))
                            ),
                            internal_date
                        ),
                        1,
                        1000
                    ) AS latest_preview,
                    countIf(has(label_ids, 'UNREAD')) AS unread_count,
                    countIf(has(label_ids, 'IMPORTANT')) AS important_count,
                    countIf(has(label_ids, 'STARRED')) AS starred_count
                FROM gmail_messages FINAL
                WHERE is_deleted = 0
                  AND has(label_ids, 'INBOX')
                  AND NOT has(label_ids, 'TRASH')
                  AND NOT has(label_ids, 'SPAM')
                GROUP BY account, thread_id
            )
            """
        )

    def _slack_account_state_items_select_sql(
        self,
        *,
        account: str,
        team_id: str,
        synced_at: datetime,
        sync_version: int,
    ) -> str:
        account_sql = _sql_string(account)
        team_id_sql = _sql_string(team_id)
        synced_at_sql = _sql_string(synced_at.isoformat())
        return f"""
            WITH
                now('UTC') - INTERVAL 30 DAY AS state_since,
                parseDateTime64BestEffort({synced_at_sql}) AS state_synced_at,
                recent_messages AS (
                    SELECT *
                    FROM slack_messages FINAL
                    WHERE account = {account_sql}
                      AND team_id = {team_id_sql}
                      AND is_deleted = 0
                      AND message_datetime >= state_since
                ),
                current_conversations AS (
                    SELECT
                        *,
                        toDecimal64OrZero(JSONExtractString(raw_json, 'last_read'), 6) AS last_read_ts
                    FROM slack_conversations FINAL
                    WHERE account = {account_sql}
                      AND team_id = {team_id_sql}
                      AND is_archived = 0
                      AND (is_member = 1 OR is_im = 1 OR is_mpim = 1)
                )
            SELECT
                'slack' AS source,
                c.account AS account,
                c.team_id AS scope_id,
                concat('slack:', c.account, ':', c.team_id, ':dm:', c.conversation_id) AS item_id,
                if(c.is_im = 1, 'direct_message', 'group_direct_message') AS item_type,
                if(c.last_read_ts > 0 AND max(toDecimal64OrZero(m.message_ts, 6)) > c.last_read_ts, 'unread', 'recent') AS item_state,
                toUInt8(multiIf(
                    c.last_read_ts > 0 AND max(toDecimal64OrZero(m.message_ts, 6)) > c.last_read_ts AND c.is_im = 1, 10,
                    c.last_read_ts > 0 AND max(toDecimal64OrZero(m.message_ts, 6)) > c.last_read_ts AND c.is_mpim = 1, 15,
                    c.is_im = 1, 35,
                    36
                )) AS priority_rank,
                max(m.message_datetime) AS latest_activity_at,
                c.conversation_id AS container_id,
                c.name AS container_name,
                '' AS thread_id,
                argMax(m.message_ts, m.message_datetime) AS message_id,
                argMax(m.user_id, m.message_datetime) AS actor_id,
                argMax(if(u.display_name != '', u.display_name, if(u.real_name != '', u.real_name, u.name)), m.message_datetime) AS actor_name,
                if(c.is_im = 1 AND actor_name != '', actor_name, if(c.name != '', c.name, if(c.is_im = 1, 'Direct message', 'Group direct message'))) AS title,
                substring(argMax(m.text, m.message_datetime), 1, 1000) AS preview,
                toUInt64(countIf(c.last_read_ts > 0 AND toDecimal64OrZero(m.message_ts, 6) > c.last_read_ts)) AS unread_count,
                if(
                    c.last_read_ts > 0 AND max(toDecimal64OrZero(m.message_ts, 6)) > c.last_read_ts,
                    if(c.is_im = 1, 'Unread Slack direct message', 'Unread Slack group direct message'),
                    if(c.is_im = 1, 'Recent Slack direct message; read state unavailable or already read', 'Recent Slack group direct message; read state unavailable or already read')
                ) AS reason,
                'slack_messages' AS source_table,
                'Query slack_messages by account, team_id, conversation_id, and thread_ts/message_ts for full context.' AS drilldown_hint,
                toUInt8(0) AS is_deleted,
                state_synced_at AS synced_at,
                toUInt64({sync_version}) AS sync_version
            FROM (SELECT * FROM slack_account_identities FINAL WHERE account = {account_sql} AND team_id = {team_id_sql}) AS i
            INNER JOIN current_conversations AS c
                ON i.account = c.account AND i.team_id = c.team_id
            INNER JOIN recent_messages AS m
                ON c.account = m.account AND c.team_id = m.team_id AND c.conversation_id = m.conversation_id
            LEFT JOIN (SELECT * FROM slack_users FINAL WHERE account = {account_sql} AND team_id = {team_id_sql}) AS u
                ON m.account = u.account AND m.team_id = u.team_id AND m.user_id = u.user_id
            WHERE c.is_im = 1 OR c.is_mpim = 1
                GROUP BY c.account, c.team_id, c.conversation_id, c.name, c.is_im, c.is_mpim, c.last_read_ts, i.user_id
            HAVING argMax(m.user_id, m.message_datetime) != i.user_id
               AND argMax(if(u.display_name != '', u.display_name, if(u.real_name != '', u.real_name, u.name)), m.message_datetime) != c.account

            UNION ALL

            SELECT
                'slack' AS source,
                c.account AS account,
                c.team_id AS scope_id,
                concat('slack:', c.account, ':', c.team_id, ':mention:', m.conversation_id, ':', m.message_ts) AS item_id,
                'mention' AS item_type,
                if(c.last_read_ts > 0 AND toDecimal64OrZero(m.message_ts, 6) > c.last_read_ts, 'unread', 'mentioned') AS item_state,
                toUInt8(if(c.last_read_ts > 0 AND toDecimal64OrZero(m.message_ts, 6) > c.last_read_ts, 20, 22)) AS priority_rank,
                m.message_datetime AS latest_activity_at,
                c.conversation_id AS container_id,
                c.name AS container_name,
                m.thread_ts AS thread_id,
                m.message_ts AS message_id,
                m.user_id AS actor_id,
                if(u.display_name != '', u.display_name, if(u.real_name != '', u.real_name, u.name)) AS actor_name,
                if(c.name != '', c.name, c.conversation_id) AS title,
                substring(m.text, 1, 1000) AS preview,
                toUInt64(1) AS unread_count,
                if(
                    c.last_read_ts > 0 AND toDecimal64OrZero(m.message_ts, 6) > c.last_read_ts,
                    'Unread Slack message mentioning the authenticated user',
                    'Recent Slack message mentioning the authenticated user'
                ) AS reason,
                'slack_messages' AS source_table,
                'Query slack_messages by account, team_id, conversation_id, and thread_ts/message_ts for full context.' AS drilldown_hint,
                toUInt8(0) AS is_deleted,
                state_synced_at AS synced_at,
                toUInt64({sync_version}) AS sync_version
            FROM (SELECT * FROM slack_account_identities FINAL WHERE account = {account_sql} AND team_id = {team_id_sql}) AS i
            INNER JOIN current_conversations AS c
                ON i.account = c.account AND i.team_id = c.team_id
            INNER JOIN recent_messages AS m
                ON c.account = m.account AND c.team_id = m.team_id AND c.conversation_id = m.conversation_id
            LEFT JOIN (SELECT * FROM slack_users FINAL WHERE account = {account_sql} AND team_id = {team_id_sql}) AS u
                ON m.account = u.account AND m.team_id = u.team_id AND m.user_id = u.user_id
            WHERE m.user_id != i.user_id
              AND if(u.display_name != '', u.display_name, if(u.real_name != '', u.real_name, u.name)) != c.account
              AND position(m.text, concat('<@', i.user_id, '>')) > 0

            UNION ALL

            SELECT
                'slack' AS source,
                p.account AS account,
                p.team_id AS scope_id,
                concat('slack:', p.account, ':', p.team_id, ':thread:', p.conversation_id, ':', p.message_ts) AS item_id,
                'participating_thread' AS item_type,
                if(if(p.thread_last_read_ts > 0, p.thread_last_read_ts, c.last_read_ts) > 0 AND max(toDecimal64OrZero(r.message_ts, 6)) > if(p.thread_last_read_ts > 0, p.thread_last_read_ts, c.last_read_ts), 'unread', 'recent') AS item_state,
                toUInt8(if(if(p.thread_last_read_ts > 0, p.thread_last_read_ts, c.last_read_ts) > 0 AND max(toDecimal64OrZero(r.message_ts, 6)) > if(p.thread_last_read_ts > 0, p.thread_last_read_ts, c.last_read_ts), 25, 45)) AS priority_rank,
                max(r.message_datetime) AS latest_activity_at,
                p.conversation_id AS container_id,
                c.name AS container_name,
                p.message_ts AS thread_id,
                p.message_ts AS message_id,
                argMax(r.user_id, r.message_datetime) AS actor_id,
                argMax(if(ru.display_name != '', ru.display_name, if(ru.real_name != '', ru.real_name, ru.name)), r.message_datetime) AS actor_name,
                if(c.name != '', c.name, p.conversation_id) AS title,
                substring(p.text, 1, 1000) AS preview,
                toUInt64(countIf(if(p.thread_last_read_ts > 0, p.thread_last_read_ts, c.last_read_ts) > 0 AND toDecimal64OrZero(r.message_ts, 6) > if(p.thread_last_read_ts > 0, p.thread_last_read_ts, c.last_read_ts))) AS unread_count,
                if(
                    if(p.thread_last_read_ts > 0, p.thread_last_read_ts, c.last_read_ts) > 0 AND max(toDecimal64OrZero(r.message_ts, 6)) > if(p.thread_last_read_ts > 0, p.thread_last_read_ts, c.last_read_ts),
                    'Unread replies in a Slack thread the authenticated user has participated in',
                    'Recent replies in a Slack thread the authenticated user has participated in'
                ) AS reason,
                'slack_messages' AS source_table,
                'Query slack_messages by account, team_id, conversation_id, and thread_ts for the full thread.' AS drilldown_hint,
                toUInt8(0) AS is_deleted,
                state_synced_at AS synced_at,
                toUInt64({sync_version}) AS sync_version
            FROM (SELECT * FROM slack_account_identities FINAL WHERE account = {account_sql} AND team_id = {team_id_sql}) AS i
            INNER JOIN (
                SELECT
                    *,
                    toDecimal64OrZero(JSONExtractString(raw_json, 'last_read'), 6) AS thread_last_read_ts,
                    JSONExtractBool(raw_json, 'subscribed') AS is_subscribed
                FROM slack_messages FINAL
                WHERE account = {account_sql}
                  AND team_id = {team_id_sql}
                  AND is_deleted = 0
                  AND is_thread_reply = 0
                  AND reply_count > 0
                  AND toFloat64OrZero(latest_reply_ts) >= toFloat64(toUnixTimestamp(state_since))
            ) AS p
                ON i.account = p.account AND i.team_id = p.team_id
            INNER JOIN current_conversations AS c
                ON p.account = c.account AND p.team_id = c.team_id AND p.conversation_id = c.conversation_id
            INNER JOIN recent_messages AS r
                ON p.account = r.account
                AND p.team_id = r.team_id
                AND p.conversation_id = r.conversation_id
                AND p.message_ts = r.thread_ts
            LEFT JOIN (SELECT * FROM slack_users FINAL WHERE account = {account_sql} AND team_id = {team_id_sql}) AS u
                ON p.account = u.account AND p.team_id = u.team_id AND p.user_id = u.user_id
            LEFT JOIN (SELECT * FROM slack_users FINAL WHERE account = {account_sql} AND team_id = {team_id_sql}) AS ru
                ON r.account = ru.account AND r.team_id = ru.team_id AND r.user_id = ru.user_id
            GROUP BY p.account, p.team_id, p.conversation_id, p.message_ts, p.user_id, p.text, p.thread_last_read_ts, p.is_subscribed, c.name, c.last_read_ts, u.display_name, u.real_name, u.name, i.user_id
            HAVING (countIf(r.user_id = i.user_id OR p.user_id = i.user_id) > 0 OR p.is_subscribed = 1)
               AND argMax(r.user_id, r.message_datetime) != i.user_id
               AND argMax(if(ru.display_name != '', ru.display_name, if(ru.real_name != '', ru.real_name, ru.name)), r.message_datetime) != p.account

            UNION ALL

            SELECT
                'slack' AS source,
                c.account AS account,
                c.team_id AS scope_id,
                concat('slack:', c.account, ':', c.team_id, ':channel:', c.conversation_id) AS item_id,
                'channel_unread' AS item_type,
                'unread' AS item_state,
                toUInt8(50) AS priority_rank,
                max(m.message_datetime) AS latest_activity_at,
                c.conversation_id AS container_id,
                c.name AS container_name,
                '' AS thread_id,
                argMax(m.message_ts, m.message_datetime) AS message_id,
                argMax(m.user_id, m.message_datetime) AS actor_id,
                argMax(if(u.display_name != '', u.display_name, if(u.real_name != '', u.real_name, u.name)), m.message_datetime) AS actor_name,
                if(c.name != '', c.name, c.conversation_id) AS title,
                substring(argMax(m.text, m.message_datetime), 1, 1000) AS preview,
                toUInt64(count()) AS unread_count,
                'Unread Slack channel messages' AS reason,
                'slack_messages' AS source_table,
                'Query slack_messages by account, team_id, conversation_id, and message_ts for full context.' AS drilldown_hint,
                toUInt8(0) AS is_deleted,
                state_synced_at AS synced_at,
                toUInt64({sync_version}) AS sync_version
            FROM (SELECT * FROM slack_account_identities FINAL WHERE account = {account_sql} AND team_id = {team_id_sql}) AS i
            INNER JOIN current_conversations AS c
                ON i.account = c.account AND i.team_id = c.team_id
            INNER JOIN recent_messages AS m
                ON c.account = m.account AND c.team_id = m.team_id AND c.conversation_id = m.conversation_id
            LEFT JOIN (SELECT * FROM slack_users FINAL WHERE account = {account_sql} AND team_id = {team_id_sql}) AS u
                ON m.account = u.account AND m.team_id = u.team_id AND m.user_id = u.user_id
            WHERE c.is_im = 0
              AND c.is_mpim = 0
              AND c.last_read_ts > 0
              AND m.is_thread_reply = 0
              AND m.user_id != i.user_id
              AND if(u.display_name != '', u.display_name, if(u.real_name != '', u.real_name, u.name)) != c.account
              AND toDecimal64OrZero(m.message_ts, 6) > c.last_read_ts
              AND position(m.text, concat('<@', i.user_id, '>')) = 0
            GROUP BY c.account, c.team_id, c.conversation_id, c.name
        """

    def _ensure_slack_account_state_view(self) -> None:
        self._command(
            """
            CREATE OR REPLACE VIEW slack_account_state_items AS
            SELECT
                source,
                account,
                scope_id,
                item_id,
                item_type,
                item_state,
                priority_rank,
                latest_activity_at,
                container_id,
                container_name,
                thread_id,
                message_id,
                actor_id,
                actor_name,
                title,
                preview,
                unread_count,
                reason,
                source_table,
                drilldown_hint
            FROM slack_account_state_item_rows FINAL
            WHERE is_deleted = 0
            """
        )

    def _ensure_combined_account_state_view_if_possible(self) -> None:
        if not self._relation_exists("gmail_account_state_items") or not self._relation_exists("slack_account_state_items"):
            return
        self._command(
            """
            CREATE OR REPLACE VIEW account_state_items AS
            SELECT * FROM gmail_account_state_items
            UNION ALL
            SELECT * FROM slack_account_state_items
            """
        )

    def _relation_exists(self, relation: str) -> bool:
        try:
            rows = self._query(f"EXISTS TABLE {relation}")
        except Exception:
            return False
        return bool(rows and int(rows[0][0]) == 1)

    def _drop_obsolete_views(self) -> None:
        for view in OBSOLETE_VIEWS:
            self._command(f"DROP VIEW IF EXISTS {view}")

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


def _missing_json_field(payload: dict[str, Any], field: str) -> bool:
    return field not in payload or payload[field] is None or payload[field] == ""


def _sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _gmail_attachment_candidate_clause() -> str:
    filename_pattern = r"\"filename\":\"[^\"]+\""
    return (
        "(position(payload_json, '\"attachmentId\"') > 0 "
        f"OR match(payload_json, {_sql_string(filename_pattern)}) "
        "OR positionCaseInsensitive(payload_json, 'Content-Disposition') > 0)"
    )


def _calendar_event_partition_key(row: dict[str, Any]) -> tuple[int, int]:
    start_at = row.get("start_at")
    if isinstance(start_at, datetime):
        return (start_at.year, start_at.month)
    return (1970, 1)
