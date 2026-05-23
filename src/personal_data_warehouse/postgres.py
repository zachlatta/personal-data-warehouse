from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
import json
import re
from typing import Any

import psycopg2
from psycopg2.extras import execute_values

from personal_data_warehouse.clickhouse import (
    AGENT_RUN_COLUMNS,
    AGENT_RUN_EVENT_COLUMNS,
    AGENT_RUN_TOOL_CALL_COLUMNS,
    ATTACHMENT_BACKFILL_STATE_COLUMNS,
    ATTACHMENT_COLUMNS,
    ATTACHMENT_ENRICHMENT_COLUMNS,
    APPLE_NOTE_ATTACHMENT_COLUMNS,
    APPLE_NOTE_COLUMNS,
    APPLE_NOTE_REVISION_COLUMNS,
    APPLE_MESSAGE_ATTACHMENT_COLUMNS,
    APPLE_MESSAGE_CHAT_COLUMNS,
    APPLE_MESSAGE_CHAT_HANDLE_COLUMNS,
    APPLE_MESSAGE_CHAT_MESSAGE_COLUMNS,
    APPLE_MESSAGE_COLUMNS,
    APPLE_MESSAGE_HANDLE_COLUMNS,
    CALENDAR_EVENT_COLUMNS,
    CALENDAR_SYNC_STATE_COLUMNS,
    FINANCE_ACCOUNT_COLUMNS,
    FINANCE_INVESTMENT_HOLDING_COLUMNS,
    FINANCE_INVESTMENT_SECURITY_COLUMNS,
    FINANCE_INVESTMENT_TRANSACTION_COLUMNS,
    FINANCE_ITEM_COLUMNS,
    FINANCE_LIABILITY_COLUMNS,
    FINANCE_SYNC_STATE_COLUMNS,
    FINANCE_TRANSACTION_COLUMNS,
    MESSAGE_COLUMNS,
    RETRYABLE_VOICE_MEMO_TRANSCRIPTION_ERROR_PATTERNS,
    SLACK_ACCOUNT_IDENTITY_COLUMNS,
    SLACK_ACCOUNT_STATE_ITEM_ROW_COLUMNS,
    SLACK_CONVERSATION_COLUMNS,
    SLACK_CONVERSATION_MEMBER_COLUMNS,
    SLACK_CONVERSATION_READ_STATE_FIELDS,
    SLACK_FILE_COLUMNS,
    SLACK_MESSAGE_COLUMNS,
    SLACK_REACTION_COLUMNS,
    SLACK_SYNC_STATE_COLUMNS,
    SLACK_TEAM_COLUMNS,
    SLACK_USER_COLUMNS,
    SYNC_STATE_COLUMNS,
    VOICE_MEMO_ENRICHMENT_COLUMNS,
    VOICE_MEMO_FILE_COLUMNS,
    VOICE_MEMO_TRANSCRIPTION_RUN_COLUMNS,
    VOICE_MEMO_TRANSCRIPT_SEGMENT_COLUMNS,
    SyncState,
)
from personal_data_warehouse.config import normalize_postgres_url

POSTGRES_TEXT_NUL_REPLACEMENT = "\\u0000"
SLACK_CONVERSATION_STATS_COLUMNS = (
    "account",
    "team_id",
    "conversation_id",
    "message_count",
    "latest_message_at",
    "updated_at",
)


@dataclass(frozen=True)
class TableSpec:
    columns: tuple[str, ...]
    primary_key: tuple[str, ...]
    version_column: str = "sync_version"


POSTGRES_TABLES: dict[str, TableSpec] = {
    "gmail_messages": TableSpec(MESSAGE_COLUMNS, ("account", "message_id")),
    "gmail_attachments": TableSpec(ATTACHMENT_COLUMNS, ("account", "message_id", "part_id", "filename")),
    "gmail_sync_state": TableSpec(SYNC_STATE_COLUMNS, ("account",), "updated_at"),
    "gmail_attachment_backfill_state": TableSpec(
        ATTACHMENT_BACKFILL_STATE_COLUMNS,
        ("account", "message_id"),
    ),
    "gmail_attachment_enrichments": TableSpec(
        ATTACHMENT_ENRICHMENT_COLUMNS,
        ("content_sha256", "ai_provider", "ai_model", "ai_prompt_version"),
    ),
    "calendar_events": TableSpec(CALENDAR_EVENT_COLUMNS, ("account", "calendar_id", "event_id")),
    "calendar_sync_state": TableSpec(CALENDAR_SYNC_STATE_COLUMNS, ("account", "calendar_id")),
    "apple_voice_memos_files": TableSpec(VOICE_MEMO_FILE_COLUMNS, ("account", "recording_id")),
    "apple_voice_memos_transcription_runs": TableSpec(
        VOICE_MEMO_TRANSCRIPTION_RUN_COLUMNS,
        ("account", "recording_id", "provider"),
    ),
    "apple_voice_memos_transcript_segments": TableSpec(
        VOICE_MEMO_TRANSCRIPT_SEGMENT_COLUMNS,
        ("account", "recording_id", "provider", "segment_index"),
    ),
    "apple_voice_memos_enrichments": TableSpec(
        VOICE_MEMO_ENRICHMENT_COLUMNS,
        ("account", "recording_id", "provider", "model", "prompt_version"),
    ),
    "apple_notes": TableSpec(APPLE_NOTE_COLUMNS, ("account", "note_id")),
    "apple_note_revisions": TableSpec(APPLE_NOTE_REVISION_COLUMNS, ("account", "note_id", "revision_id")),
    "apple_note_attachments": TableSpec(
        APPLE_NOTE_ATTACHMENT_COLUMNS,
        ("account", "note_id", "revision_id", "attachment_id"),
    ),
    "apple_message_handles": TableSpec(APPLE_MESSAGE_HANDLE_COLUMNS, ("account", "handle_id")),
    "apple_message_chats": TableSpec(APPLE_MESSAGE_CHAT_COLUMNS, ("account", "chat_id")),
    "apple_message_chat_handles": TableSpec(APPLE_MESSAGE_CHAT_HANDLE_COLUMNS, ("account", "chat_id", "handle_id")),
    "apple_messages": TableSpec(APPLE_MESSAGE_COLUMNS, ("account", "message_id")),
    "apple_message_chat_messages": TableSpec(
        APPLE_MESSAGE_CHAT_MESSAGE_COLUMNS,
        ("account", "chat_id", "message_id"),
    ),
    "apple_message_attachments": TableSpec(
        APPLE_MESSAGE_ATTACHMENT_COLUMNS,
        ("account", "attachment_id", "message_id"),
    ),
    "agent_runs": TableSpec(AGENT_RUN_COLUMNS, ("run_id",)),
    "agent_run_events": TableSpec(AGENT_RUN_EVENT_COLUMNS, ("run_id", "event_index")),
    "agent_run_tool_calls": TableSpec(AGENT_RUN_TOOL_CALL_COLUMNS, ("run_id", "event_index", "tool_name")),
    "slack_teams": TableSpec(SLACK_TEAM_COLUMNS, ("account", "team_id")),
    "slack_account_identities": TableSpec(SLACK_ACCOUNT_IDENTITY_COLUMNS, ("account", "team_id")),
    "slack_users": TableSpec(SLACK_USER_COLUMNS, ("account", "team_id", "user_id")),
    "slack_conversations": TableSpec(SLACK_CONVERSATION_COLUMNS, ("account", "team_id", "conversation_id")),
    "slack_conversation_members": TableSpec(
        SLACK_CONVERSATION_MEMBER_COLUMNS,
        ("account", "team_id", "conversation_id", "user_id"),
    ),
    "slack_messages": TableSpec(SLACK_MESSAGE_COLUMNS, ("account", "team_id", "conversation_id", "message_ts")),
    "slack_conversation_stats": TableSpec(
        SLACK_CONVERSATION_STATS_COLUMNS,
        ("account", "team_id", "conversation_id"),
        "updated_at",
    ),
    "slack_message_reactions": TableSpec(
        SLACK_REACTION_COLUMNS,
        ("account", "team_id", "conversation_id", "message_ts", "reaction_name", "user_id"),
    ),
    "slack_files": TableSpec(
        SLACK_FILE_COLUMNS,
        ("account", "team_id", "file_id", "conversation_id", "message_ts"),
    ),
    "slack_sync_state": TableSpec(SLACK_SYNC_STATE_COLUMNS, ("account", "team_id", "object_type", "object_id")),
    "slack_account_state_item_rows": TableSpec(
        SLACK_ACCOUNT_STATE_ITEM_ROW_COLUMNS,
        ("source", "account", "scope_id", "item_id"),
    ),
    "finance_items": TableSpec(FINANCE_ITEM_COLUMNS, ("source", "item_id")),
    "finance_accounts": TableSpec(FINANCE_ACCOUNT_COLUMNS, ("source", "item_id", "account_id")),
    "finance_transactions": TableSpec(FINANCE_TRANSACTION_COLUMNS, ("source", "item_id", "transaction_id")),
    "finance_investment_holdings": TableSpec(
        FINANCE_INVESTMENT_HOLDING_COLUMNS,
        ("source", "item_id", "account_id", "security_id"),
    ),
    "finance_investment_securities": TableSpec(
        FINANCE_INVESTMENT_SECURITY_COLUMNS,
        ("source", "item_id", "security_id"),
    ),
    "finance_investment_transactions": TableSpec(
        FINANCE_INVESTMENT_TRANSACTION_COLUMNS,
        ("source", "item_id", "investment_transaction_id"),
    ),
    "finance_liabilities": TableSpec(
        FINANCE_LIABILITY_COLUMNS,
        ("source", "item_id", "account_id", "liability_type"),
    ),
    "finance_sync_state": TableSpec(
        FINANCE_SYNC_STATE_COLUMNS,
        ("source", "item_name", "item_id", "object_type"),
    ),
}

POSTGRES_INSERT_PAGE_SIZES = {
    "apple_notes": 50,
    "apple_note_revisions": 50,
    "apple_note_attachments": 250,
    "apple_messages": 500,
    "apple_message_attachments": 500,
}


ARRAY_COLUMNS = {
    "label_ids",
    "to_addresses",
    "cc_addresses",
    "bcc_addresses",
    "recurrence",
}

TIMESTAMP_COLUMNS = {
    "internal_date",
    "synced_at",
    "updated_at",
    "expanded_synced_at",
    "expanded_window_start",
    "expanded_window_end",
    "start_at",
    "end_at",
    "file_created_at",
    "file_modified_at",
    "recorded_at",
    "ingested_at",
    "modified_at",
    "exported_at",
    "requested_at",
    "completed_at",
    "created_at",
    "started_at",
    "latest_activity_at",
    "latest_message_at",
    "message_datetime",
    "message_at",
    "message_date",
    "last_read_message_at",
    "date_read",
    "date_delivered",
    "date_played",
    "date_edited",
    "date_retracted",
    "date_recovered",
    "ai_processed_at",
    "datetime",
    "authorized_datetime",
}

INTEGER_COLUMNS = {
    "history_id",
    "is_deleted",
    "size_estimate",
    "size",
    "last_history_id",
    "attachment_rows_written",
    "ai_elapsed_ms",
    "is_all_day",
    "size_bytes",
    "segment_index",
    "start_ms",
    "end_ms",
    "exit_code",
    "event_index",
    "is_bot",
    "is_app_user",
    "is_channel",
    "is_group",
    "is_im",
    "is_mpim",
    "is_private",
    "is_archived",
    "is_member",
    "num_members",
    "is_thread_parent",
    "is_thread_reply",
    "reply_count",
    "reply_users_count",
    "reaction_count",
    "message_count",
    "priority_rank",
    "unread_count",
    "sync_version",
    "pending",
    "is_removed",
    "is_overdue",
    "is_missing",
    "handle_rowid",
    "chat_rowid",
    "message_rowid",
    "attachment_rowid",
    "style",
    "state",
    "message_type",
    "message_item_type",
    "group_action_type",
    "message_action_type",
    "message_source",
    "associated_message_type",
    "date_ns",
    "message_date_ns",
    "total_bytes",
    "transfer_state",
    "is_from_me",
    "is_read",
    "is_sent",
    "is_delivered",
    "is_finished",
    "is_system_message",
    "is_service_message",
    "is_forward",
    "is_empty",
    "is_audio_message",
    "is_played",
    "cache_has_attachments",
    "has_unseen_mention",
    "is_spam",
    "is_outgoing",
    "is_sticker",
    "hide_attachment",
    "is_filtered",
    "is_recovered",
    "is_pending_review",
}

FLOAT_COLUMNS = {
    "confidence",
    "calendar_confidence",
    "current_balance",
    "available_balance",
    "limit_balance",
    "amount",
    "quantity",
    "cost_basis",
    "institution_price",
    "institution_value",
    "vested_quantity",
    "vested_value",
    "close_price",
    "price",
    "fees",
    "interest_rate_percentage",
    "last_payment_amount",
    "next_monthly_payment",
    "original_principal_balance",
    "outstanding_principal_balance",
    "ytd_interest_paid",
    "ytd_principal_paid",
    "minimum_payment_amount",
    "last_statement_balance",
}


class PostgresWarehouse:
    def __init__(self, postgres_database_url: str, *, schema: str = "public") -> None:
        normalized = normalize_postgres_url(postgres_database_url)
        if not normalized:
            raise ValueError("POSTGRES_DATABASE_URL must be set")
        self._schema = _validate_identifier(schema)
        self._connection = psycopg2.connect(normalized)
        self._connection.autocommit = True
        self._command(f"CREATE SCHEMA IF NOT EXISTS {_identifier(self._schema)}")
        self._command(f"SET search_path TO {_identifier(self._schema)}")

    def close(self) -> None:
        self._connection.close()

    def ensure_tables(self) -> None:
        self._ensure_table_group(
            [
                "gmail_messages",
                "gmail_attachments",
                "gmail_sync_state",
                "gmail_attachment_backfill_state",
                "gmail_attachment_enrichments",
            ]
        )
        self._ensure_clean_gmail_inbox_view()

    def ensure_calendar_tables(self) -> None:
        self._ensure_table_group(["calendar_events", "calendar_sync_state"])
        self._ensure_clean_calendar_transcript_views_if_possible()

    def ensure_apple_voice_memos_tables(self, *, backfill_content_hashes: bool = True) -> None:
        self._ensure_table_group(
            [
                "apple_voice_memos_files",
                "apple_voice_memos_transcription_runs",
                "apple_voice_memos_transcript_segments",
                "apple_voice_memos_enrichments",
                "agent_runs",
                "agent_run_events",
                "agent_run_tool_calls",
            ]
        )
        if backfill_content_hashes:
            self._backfill_voice_memo_transcription_run_content_hashes()
            self._backfill_voice_memo_enrichment_content_hashes()
        self._ensure_clean_calendar_transcript_views_if_possible()

    def ensure_voice_memos_tables(self) -> None:
        self.ensure_apple_voice_memos_tables()

    def ensure_apple_notes_tables(self) -> None:
        self._ensure_table_group(["apple_notes", "apple_note_revisions", "apple_note_attachments"])

    def ensure_apple_messages_tables(self) -> None:
        self._ensure_table_group(
            [
                "apple_message_handles",
                "apple_message_chats",
                "apple_message_chat_handles",
                "apple_messages",
                "apple_message_chat_messages",
                "apple_message_attachments",
            ]
        )

    def ensure_voice_memo_transcription_tables(self) -> None:
        self.ensure_apple_voice_memos_tables()

    def ensure_agent_tables(self) -> None:
        self._ensure_table_group(["agent_runs", "agent_run_events", "agent_run_tool_calls"])

    def ensure_slack_tables(self) -> None:
        self._ensure_table_group(
            [
                "slack_teams",
                "slack_account_identities",
                "slack_users",
                "slack_conversations",
                "slack_conversation_members",
                "slack_messages",
                "slack_conversation_stats",
                "slack_message_reactions",
                "slack_files",
                "slack_sync_state",
                "slack_account_state_item_rows",
            ]
        )
        self._ensure_slack_conversation_stats_backfilled()
        self._ensure_clean_slack_inbox_view()

    def ensure_finance_tables(self) -> None:
        self._ensure_table_group(
            [
                "finance_items",
                "finance_accounts",
                "finance_transactions",
                "finance_investment_holdings",
                "finance_investment_securities",
                "finance_investment_transactions",
                "finance_liabilities",
                "finance_sync_state",
            ]
        )
        self._ensure_clean_finance_views()

    def _ensure_table_group(self, tables: Sequence[str]) -> None:
        for table in tables:
            self._ensure_table(table)
        self._ensure_indexes()

    def _ensure_table(self, table: str) -> None:
        spec = POSTGRES_TABLES[table]
        column_sql = [f"{_identifier(column)} {_postgres_type(column)} NOT NULL DEFAULT {_default_sql(column)}" for column in spec.columns]
        primary_key = ", ".join(_identifier(column) for column in spec.primary_key)
        self._command(
            f"""
            CREATE TABLE IF NOT EXISTS {_identifier(table)} (
                {", ".join(column_sql)},
                PRIMARY KEY ({primary_key})
            )
            """
        )

    def _ensure_indexes(self) -> None:
        for sql in (
            "CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public",
            "CREATE INDEX IF NOT EXISTS gmail_messages_thread_idx ON gmail_messages (account, thread_id, internal_date DESC)",
            "CREATE INDEX IF NOT EXISTS gmail_messages_label_ids_idx ON gmail_messages USING gin (label_ids)",
            "CREATE INDEX IF NOT EXISTS gmail_attachments_message_idx ON gmail_attachments (account, message_id)",
            "CREATE INDEX IF NOT EXISTS calendar_events_time_idx ON calendar_events (start_at, end_at)",
            "CREATE INDEX IF NOT EXISTS voice_memo_files_recorded_idx ON apple_voice_memos_files (recorded_at DESC)",
            "CREATE INDEX IF NOT EXISTS apple_notes_modified_idx ON apple_notes (modified_at DESC) WHERE is_deleted = 0",
            "CREATE INDEX IF NOT EXISTS apple_note_revisions_note_idx ON apple_note_revisions (account, note_id, modified_at DESC)",
            "CREATE INDEX IF NOT EXISTS apple_note_attachments_hash_idx ON apple_note_attachments (content_sha256)",
            "CREATE INDEX IF NOT EXISTS apple_messages_time_idx ON apple_messages (message_at DESC) WHERE is_deleted = 0",
            "CREATE INDEX IF NOT EXISTS apple_messages_body_trgm_idx ON apple_messages USING gin (body_text public.gin_trgm_ops) WHERE is_deleted = 0",
            "CREATE INDEX IF NOT EXISTS apple_message_chat_messages_chat_time_idx ON apple_message_chat_messages (account, chat_id, message_date DESC)",
            "CREATE INDEX IF NOT EXISTS apple_message_attachments_hash_idx ON apple_message_attachments (content_sha256)",
            "CREATE INDEX IF NOT EXISTS slack_messages_conversation_time_idx ON slack_messages (account, team_id, conversation_id, message_datetime DESC)",
            "CREATE INDEX IF NOT EXISTS slack_messages_recent_scope_time_idx ON slack_messages (account, team_id, message_datetime DESC) WHERE is_deleted = 0",
            "CREATE INDEX IF NOT EXISTS slack_messages_recent_thread_time_idx ON slack_messages (account, team_id, thread_ts, message_datetime DESC) WHERE is_deleted = 0",
            "CREATE INDEX IF NOT EXISTS slack_messages_thread_idx ON slack_messages (account, team_id, conversation_id, thread_ts)",
            "CREATE INDEX IF NOT EXISTS slack_messages_text_trgm_live_idx ON slack_messages USING gin (text public.gin_trgm_ops) WHERE is_deleted = 0",
            "CREATE INDEX IF NOT EXISTS slack_conversations_scope_idx ON slack_conversations (account, team_id, conversation_type)",
            "CREATE INDEX IF NOT EXISTS slack_state_scope_idx ON slack_sync_state (account, team_id, object_type, object_id)",
            "CREATE INDEX IF NOT EXISTS finance_accounts_item_idx ON finance_accounts (source, item_id, type, subtype)",
            "CREATE INDEX IF NOT EXISTS finance_transactions_account_date_idx ON finance_transactions (source, account_id, date DESC)",
            "CREATE INDEX IF NOT EXISTS finance_holdings_account_idx ON finance_investment_holdings (source, item_id, account_id)",
            "CREATE INDEX IF NOT EXISTS finance_liabilities_item_idx ON finance_liabilities (source, item_id, liability_type)",
        ):
            try:
                self._command(sql)
            except Exception:
                # Tests often create only a subset of tables. Missing-table index failures
                # are harmless because ensure_* is called again by each runtime asset.
                pass

    def load_sync_state(self) -> dict[str, SyncState]:
        rows = self._query(
            """
            SELECT account, last_history_id, last_sync_type, status, error, updated_at
            FROM gmail_sync_state
            """
        )
        return {
            str(row[0]): SyncState(
                account=str(row[0]),
                last_history_id=int(row[1]),
                last_sync_type=str(row[2]),
                status=str(row[3]),
                error=str(row[4]),
                updated_at=row[5],
            )
            for row in rows
        }

    def insert_messages(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("gmail_messages", rows, MESSAGE_COLUMNS)

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
            SELECT payload_json
            FROM gmail_messages
            WHERE account = %s
              AND is_deleted = 0
              AND {_postgres_gmail_attachment_candidate_clause()}
              AND NOT EXISTS (
                  SELECT 1
                  FROM gmail_attachment_backfill_state state
                  WHERE state.account = gmail_messages.account
                    AND state.message_id = gmail_messages.message_id
                    AND state.status = 'ok'
                    AND state.ai_provider = %s
                    AND state.ai_model = %s
                    AND state.ai_prompt_version = %s
              )
            ORDER BY internal_date DESC, message_id DESC
            LIMIT %s
            """,
            (account, ai_provider, ai_model, ai_prompt_version, int(limit)),
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
        self._insert_rows("gmail_attachment_backfill_state", rows, ATTACHMENT_BACKFILL_STATE_COLUMNS)

    def load_attachment_enrichments(
        self,
        *,
        content_sha256s: list[str],
        ai_provider: str,
        ai_model: str,
        ai_prompt_version: str,
    ) -> dict[str, dict[str, Any]]:
        hashes = sorted({value for value in content_sha256s if value})
        if not hashes:
            return {}
        columns = (
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
        )
        rows = self._query(
            f"""
            SELECT {", ".join(_identifier(column) for column in columns)}
            FROM gmail_attachment_enrichments
            WHERE content_sha256 = ANY(%s)
              AND ai_provider = %s
              AND ai_model = %s
              AND ai_prompt_version = %s
            """,
            (hashes, ai_provider, ai_model, ai_prompt_version),
        )
        return {str(row[0]): dict(zip(columns, row, strict=True)) for row in rows}

    def insert_attachment_enrichments(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("gmail_attachment_enrichments", rows, ATTACHMENT_ENRICHMENT_COLUMNS)

    def insert_calendar_events(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("calendar_events", rows, CALENDAR_EVENT_COLUMNS)

    def load_active_recurring_calendar_event_ids(
        self,
        *,
        account: str,
        calendar_id: str,
        window_start: datetime,
        window_end: datetime,
    ) -> list[str]:
        rows = self._query(
            """
            SELECT event_id
            FROM calendar_events
            WHERE account = %s
              AND calendar_id = %s
              AND recurring_event_id != ''
              AND is_deleted = 0
              AND start_at < %s
              AND end_at > %s
            """,
            (account, calendar_id, _ensure_utc(window_end), _ensure_utc(window_start)),
        )
        return [str(row[0]) for row in rows]

    def mark_calendar_events_deleted(
        self,
        *,
        account: str,
        calendar_id: str,
        event_ids: list[str],
        synced_at: datetime,
    ) -> int:
        if not event_ids:
            return 0
        rows = self._query(
            f"""
            SELECT {", ".join(_identifier(column) for column in CALENDAR_EVENT_COLUMNS)}
            FROM calendar_events
            WHERE account = %s
              AND calendar_id = %s
              AND event_id = ANY(%s)
              AND is_deleted = 0
            """,
            (account, calendar_id, event_ids),
        )
        tombstones: list[dict[str, Any]] = []
        sync_version = int(_ensure_utc(synced_at).timestamp() * 1_000_000)
        for row in rows:
            tombstone = dict(zip(CALENDAR_EVENT_COLUMNS, row, strict=True))
            tombstone["status"] = "cancelled"
            tombstone["is_deleted"] = 1
            tombstone["synced_at"] = synced_at
            tombstone["sync_version"] = sync_version
            tombstones.append(tombstone)
        self.insert_calendar_events(tombstones)
        return len(tombstones)

    def load_calendar_sync_state(self) -> dict[tuple[str, str], dict[str, Any]]:
        columns = (
            "account",
            "calendar_id",
            "sync_token",
            "last_sync_type",
            "status",
            "error",
            "expanded_synced_at",
            "expanded_window_start",
            "expanded_window_end",
            "updated_at",
        )
        rows = self._query(f"SELECT {', '.join(_identifier(column) for column in columns)} FROM calendar_sync_state")
        return {
            (str(row[0]), str(row[1])): dict(zip(columns, row, strict=True))
            for row in rows
        }

    def insert_calendar_sync_state(
        self,
        *,
        account: str,
        calendar_id: str,
        sync_token: str,
        last_sync_type: str,
        status: str,
        error: str,
        expanded_synced_at: datetime,
        expanded_window_start: datetime,
        expanded_window_end: datetime,
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
                    expanded_synced_at,
                    expanded_window_start,
                    expanded_window_end,
                    updated_at,
                    int(_ensure_utc(updated_at).timestamp() * 1_000_000),
                )
            ],
            CALENDAR_SYNC_STATE_COLUMNS,
        )

    def insert_apple_voice_memos_files(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("apple_voice_memos_files", rows, VOICE_MEMO_FILE_COLUMNS)

    def insert_voice_memo_files(self, rows: list[dict[str, Any]]) -> None:
        self.insert_apple_voice_memos_files(rows)

    def insert_apple_voice_memos_transcription_runs(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("apple_voice_memos_transcription_runs", rows, VOICE_MEMO_TRANSCRIPTION_RUN_COLUMNS)

    def insert_voice_memo_transcription_runs(self, rows: list[dict[str, Any]]) -> None:
        self.insert_apple_voice_memos_transcription_runs(rows)

    def insert_apple_voice_memos_transcript_segments(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("apple_voice_memos_transcript_segments", rows, VOICE_MEMO_TRANSCRIPT_SEGMENT_COLUMNS)

    def insert_voice_memo_transcript_segments(self, rows: list[dict[str, Any]]) -> None:
        self.insert_apple_voice_memos_transcript_segments(rows)

    def insert_apple_voice_memos_enrichments(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("apple_voice_memos_enrichments", rows, VOICE_MEMO_ENRICHMENT_COLUMNS)

    def insert_voice_memo_enrichments(self, rows: list[dict[str, Any]]) -> None:
        self.insert_apple_voice_memos_enrichments(rows)

    def insert_apple_notes(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("apple_notes", rows, APPLE_NOTE_COLUMNS)

    def insert_apple_note_revisions(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("apple_note_revisions", rows, APPLE_NOTE_REVISION_COLUMNS)

    def insert_apple_note_attachments(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("apple_note_attachments", rows, APPLE_NOTE_ATTACHMENT_COLUMNS)

    def insert_apple_message_handles(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("apple_message_handles", rows, APPLE_MESSAGE_HANDLE_COLUMNS)

    def insert_apple_message_chats(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("apple_message_chats", rows, APPLE_MESSAGE_CHAT_COLUMNS)

    def insert_apple_message_chat_handles(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("apple_message_chat_handles", rows, APPLE_MESSAGE_CHAT_HANDLE_COLUMNS)

    def insert_apple_messages(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("apple_messages", rows, APPLE_MESSAGE_COLUMNS)

    def insert_apple_message_chat_messages(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("apple_message_chat_messages", rows, APPLE_MESSAGE_CHAT_MESSAGE_COLUMNS)

    def insert_apple_message_attachments(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("apple_message_attachments", rows, APPLE_MESSAGE_ATTACHMENT_COLUMNS)

    def insert_agent_runs(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("agent_runs", rows, AGENT_RUN_COLUMNS)

    def insert_agent_run_events(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("agent_run_events", rows, AGENT_RUN_EVENT_COLUMNS)

    def insert_agent_run_tool_calls(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("agent_run_tool_calls", rows, AGENT_RUN_TOOL_CALL_COLUMNS)

    def load_untranscribed_apple_voice_memos_files(self, *, provider: str, limit: int) -> list[dict[str, Any]]:
        rows = self._query(
            f"""
            SELECT
                f.account,
                f.recording_id,
                f.title,
                f.filename,
                f.extension,
                f.content_type,
                f.size_bytes,
                f.content_sha256,
                f.recorded_at,
                f.storage_backend,
                f.storage_key,
                f.storage_file_id,
                f.storage_url
            FROM apple_voice_memos_files AS f
            LEFT JOIN (
                SELECT account, recording_id, content_sha256, completed_at
                FROM apple_voice_memos_transcription_runs
                WHERE provider = %s
                  AND (
                    status = 'completed'
                    OR (status = 'error' AND NOT ({_postgres_retryable_error_clause('error')}))
                  )
            ) AS terminal
              ON f.account = terminal.account
             AND f.recording_id = terminal.recording_id
             AND terminal.content_sha256 = f.content_sha256
            WHERE terminal.recording_id IS NULL
              AND f.size_bytes > 0
            ORDER BY f.recorded_at DESC
            LIMIT %s
            """,
            (provider, int(limit)),
        )
        columns = (
            "account",
            "recording_id",
            "title",
            "filename",
            "extension",
            "content_type",
            "size_bytes",
            "content_sha256",
            "recorded_at",
            "storage_backend",
            "storage_key",
            "storage_file_id",
            "storage_url",
        )
        return [dict(zip(columns, row, strict=True)) for row in rows]

    def load_untranscribed_voice_memo_files(self, *, provider: str, limit: int) -> list[dict[str, Any]]:
        return self.load_untranscribed_apple_voice_memos_files(provider=provider, limit=limit)

    def existing_message_ids(self, *, account: str, message_ids: list[str]) -> set[str]:
        if not message_ids:
            return set()
        rows = self._query(
            """
            SELECT message_id
            FROM gmail_messages
            WHERE account = %s
              AND is_deleted = 0
              AND message_id = ANY(%s)
            """,
            (account, message_ids),
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
        rows = self._query(
            """
            SELECT message_id, part_id, filename
            FROM gmail_attachments
            WHERE account = %s
              AND is_deleted = 0
              AND message_id = ANY(%s)
            """,
            (account, message_ids),
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
        rows = self._query(
            """
            SELECT message_id, payload_json
            FROM gmail_messages
            WHERE account = %s
              AND is_deleted = 0
              AND message_id = ANY(%s)
            """,
            (account, message_ids),
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
            [(account, int(last_history_id), last_sync_type, status, error, updated_at)],
            SYNC_STATE_COLUMNS,
        )

    def insert_finance_items(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("finance_items", rows, FINANCE_ITEM_COLUMNS)

    def insert_finance_accounts(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("finance_accounts", rows, FINANCE_ACCOUNT_COLUMNS)

    def insert_finance_transactions(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("finance_transactions", rows, FINANCE_TRANSACTION_COLUMNS)

    def insert_finance_investment_holdings(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("finance_investment_holdings", rows, FINANCE_INVESTMENT_HOLDING_COLUMNS)

    def insert_finance_investment_securities(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("finance_investment_securities", rows, FINANCE_INVESTMENT_SECURITY_COLUMNS)

    def insert_finance_investment_transactions(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("finance_investment_transactions", rows, FINANCE_INVESTMENT_TRANSACTION_COLUMNS)

    def insert_finance_liabilities(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("finance_liabilities", rows, FINANCE_LIABILITY_COLUMNS)

    def load_finance_sync_state(self) -> dict[tuple[str, str, str, str], dict[str, Any]]:
        columns = (
            "source",
            "item_name",
            "item_id",
            "object_type",
            "cursor",
            "start_date",
            "end_date",
            "status",
            "error",
            "updated_at",
        )
        rows = self._query(f"SELECT {', '.join(_identifier(column) for column in columns)} FROM finance_sync_state")
        return {
            (str(row[0]), str(row[1]), str(row[2]), str(row[3])): dict(zip(columns, row, strict=True))
            for row in rows
        }

    def insert_finance_sync_state(
        self,
        *,
        source: str,
        item_name: str,
        item_id: str,
        object_type: str,
        cursor: str,
        start_date: str,
        end_date: str,
        status: str,
        error: str,
        updated_at: datetime,
    ) -> None:
        self._insert(
            "finance_sync_state",
            [
                (
                    source,
                    item_name,
                    item_id,
                    object_type,
                    cursor,
                    start_date,
                    end_date,
                    status,
                    error,
                    updated_at,
                    int(_ensure_utc(updated_at).timestamp() * 1_000_000),
                )
            ],
            FINANCE_SYNC_STATE_COLUMNS,
        )

    def load_slack_sync_state(self) -> dict[tuple[str, str, str, str], dict[str, Any]]:
        columns = (
            "account",
            "team_id",
            "object_type",
            "object_id",
            "cursor_ts",
            "last_sync_type",
            "status",
            "error",
            "updated_at",
        )
        rows = self._query(f"SELECT {', '.join(_identifier(column) for column in columns)} FROM slack_sync_state")
        return {
            (str(row[0]), str(row[1]), str(row[2]), str(row[3])): dict(zip(columns, row, strict=True))
            for row in rows
        }

    def insert_slack_teams(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("slack_teams", rows, SLACK_TEAM_COLUMNS)

    def insert_slack_account_identities(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("slack_account_identities", rows, SLACK_ACCOUNT_IDENTITY_COLUMNS)

    def insert_slack_users(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("slack_users", rows, SLACK_USER_COLUMNS)

    def insert_slack_conversations(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("slack_conversations", self._preserve_slack_conversation_read_state(rows), SLACK_CONVERSATION_COLUMNS)

    def _preserve_slack_conversation_read_state(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows_missing_read_state = []
        for row in rows:
            try:
                payload = json.loads(str(row.get("raw_json", "")))
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict) and any(_missing_json_field(payload, field) for field in SLACK_CONVERSATION_READ_STATE_FIELDS):
                rows_missing_read_state.append(row)
        if not rows_missing_read_state:
            return rows

        ids_by_scope: dict[tuple[str, str], set[str]] = {}
        for row in rows_missing_read_state:
            ids_by_scope.setdefault((str(row["account"]), str(row["team_id"])), set()).add(str(row["conversation_id"]))

        existing_payloads: dict[tuple[str, str, str], dict[str, Any]] = {}
        for (account, team_id), conversation_ids in ids_by_scope.items():
            existing_rows = self._query(
                """
                SELECT conversation_id, raw_json
                FROM slack_conversations
                WHERE account = %s
                  AND team_id = %s
                  AND conversation_id = ANY(%s)
                """,
                (account, team_id, sorted(conversation_ids)),
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
            if changed:
                row = dict(row)
                row["raw_json"] = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
            preserved_rows.append(row)
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
        where = ["c.account = %s", "c.team_id = %s"]
        params: list[Any] = [account, team_id]
        if archived_only:
            where.append("c.is_archived = 1")
        elif not include_archived:
            where.append("c.is_archived = 0")
        if conversation_types:
            where.append("c.conversation_type = ANY(%s)")
            params.append(list(conversation_types))
        if not_full_only:
            where.append("NOT (COALESCE(s.status, '') = 'ok' AND COALESCE(s.last_sync_type, '') = 'full')")
        if zero_messages_only:
            where.append("COALESCE(m.message_count, 0) = 0")
        if skip_known_errors:
            where.append("COALESCE(s.status, '') != 'error'")
        limit_clause = "LIMIT %s" if limit is not None else ""
        if limit is not None:
            params.append(int(limit))
        rows = self._query(
            f"""
            SELECT c.raw_json
            FROM slack_conversations AS c
            LEFT JOIN slack_sync_state AS s
              ON c.account = s.account
             AND c.team_id = s.team_id
             AND c.conversation_id = s.object_id
             AND s.object_type = 'conversation'
            LEFT JOIN slack_conversation_stats AS m
              ON c.account = m.account
             AND c.team_id = m.team_id
             AND c.conversation_id = m.conversation_id
            WHERE {" AND ".join(where)}
            ORDER BY
                (NOT (COALESCE(s.status, '') = 'ok' AND COALESCE(s.last_sync_type, '') = 'full')) DESC,
                (COALESCE(m.message_count, 0) = 0) DESC,
                CASE c.conversation_type
                    WHEN 'im' THEN 1
                    WHEN 'mpim' THEN 2
                    WHEN 'private_channel' THEN 3
                    WHEN 'public_channel' THEN 4
                    ELSE 5
                END,
                c.is_archived,
                c.conversation_id
            {limit_clause}
            """,
            tuple(params),
        )
        return _json_payloads(rows)

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
        where = [
            "m.account = %s",
            "m.team_id = %s",
            "m.is_deleted = 0",
            "m.reply_count > 0",
            "m.is_thread_reply = 0",
        ]
        params: list[Any] = [account, team_id]
        if since_ts is not None:
            where.append(_numeric_ts("m.message_ts") + " >= %s")
            params.append(since_ts)
        if skip_known_errors:
            where.append("(s.object_id IS NULL OR s.status != 'error')")
        if skip_completed:
            where.append(
                "("
                "s.object_id IS NULL "
                "OR s.status != 'ok' "
                "OR (m.latest_reply_ts != '' AND s.cursor_ts != '' AND "
                + _numeric_ts("m.latest_reply_ts")
                + " > "
                + _numeric_ts("s.cursor_ts")
                + ")"
                ")"
            )
        order_by = "m.message_datetime DESC, m.message_ts DESC"
        if order == "reply_count":
            order_by = "m.reply_count DESC, m.message_datetime DESC, m.message_ts DESC"
        limit_clause = "LIMIT %s" if limit is not None else ""
        if limit is not None:
            params.append(int(limit))
        rows = self._query(
            f"""
            SELECT m.conversation_id, m.message_ts, m.reply_count, m.latest_reply_ts, m.message_datetime
            FROM slack_messages AS m
            LEFT JOIN slack_sync_state AS s
              ON m.account = s.account
             AND m.team_id = s.team_id
             AND s.object_type = 'thread'
             AND m.conversation_id || ':' || m.message_ts = s.object_id
            WHERE {" AND ".join(where)}
            ORDER BY {order_by}
            {limit_clause}
            """,
            tuple(params),
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
        where = [
            "c.account = %s",
            "c.team_id = %s",
            "c.is_archived = 0",
            "(c.is_member = 1 OR c.is_im = 1 OR c.is_mpim = 1)",
            "m.latest_message_at >= now() - INTERVAL '30 days'",
        ]
        params: list[Any] = [account, team_id]
        if conversation_types:
            where.append("c.conversation_type = ANY(%s)")
            params.append(list(conversation_types))
        limit_clause = "LIMIT %s" if limit is not None else ""
        if limit is not None:
            params.append(int(limit))
        rows = self._query(
            f"""
            SELECT c.raw_json
            FROM slack_conversations AS c
            LEFT JOIN slack_conversation_stats AS m
              ON c.account = m.account
             AND c.team_id = m.team_id
             AND c.conversation_id = m.conversation_id
            WHERE {" AND ".join(where)}
            ORDER BY
                ({_json_numeric("c.raw_json", "last_read")} = 0) DESC,
                m.latest_message_at DESC,
                CASE
                    WHEN c.is_im = 1 THEN 1
                    WHEN c.is_mpim = 1 THEN 2
                    WHEN c.is_private = 1 THEN 3
                    ELSE 4
                END,
                c.conversation_id
            {limit_clause}
            """,
            tuple(params),
        )
        return _json_payloads(rows)

    def load_slack_member_sync_candidate_payloads(
        self,
        *,
        account: str,
        team_id: str,
        conversation_types: tuple[str, ...] = ("private_channel",),
        limit: int | None = None,
        skip_known_errors: bool = False,
    ) -> list[dict[str, Any]]:
        where = [
            "c.account = %s",
            "c.team_id = %s",
            "c.is_archived = 0",
            "c.is_member = 1",
        ]
        params: list[Any] = [account, team_id]
        if conversation_types:
            where.append("c.conversation_type = ANY(%s)")
            params.append(list(conversation_types))
        if skip_known_errors:
            where.append("COALESCE(s.status, '') != 'error'")
        limit_clause = "LIMIT %s" if limit is not None else ""
        if limit is not None:
            params.append(int(limit))
        rows = self._query(
            f"""
            SELECT c.raw_json
            FROM slack_conversations AS c
            LEFT JOIN slack_sync_state AS s
              ON c.account = s.account
             AND c.team_id = s.team_id
             AND c.conversation_id = s.object_id
             AND s.object_type = 'conversation_members'
            WHERE {" AND ".join(where)}
            ORDER BY
                (COALESCE(s.status, '') = 'ok') ASC,
                s.updated_at ASC NULLS FIRST,
                c.num_members DESC,
                c.conversation_id
            {limit_clause}
            """,
            tuple(params),
        )
        return _json_payloads(rows)

    def insert_slack_conversation_members(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("slack_conversation_members", rows, SLACK_CONVERSATION_MEMBER_COLUMNS)

    def replace_slack_conversation_members(
        self,
        *,
        account: str,
        team_id: str,
        conversation_id: str,
        rows: list[dict[str, Any]],
        synced_at: datetime,
        sync_version: int,
    ) -> None:
        self.insert_slack_conversation_members(rows)
        active_user_ids = sorted({str(row["user_id"]) for row in rows})
        params: list[Any] = [synced_at, sync_version, account, team_id, conversation_id, sync_version]
        active_filter = ""
        if active_user_ids:
            active_filter = "AND NOT (user_id = ANY(%s))"
            params.append(active_user_ids)
        self._command(
            f"""
            UPDATE slack_conversation_members
               SET is_deleted = 1,
                   synced_at = %s,
                   sync_version = %s
             WHERE account = %s
               AND team_id = %s
               AND conversation_id = %s
               AND sync_version <= %s
               {active_filter}
            """,
            tuple(params),
        )

    def insert_slack_messages(self, rows: list[dict[str, Any]]) -> None:
        increments, latest_candidates, recompute_keys = self._slack_conversation_stat_changes_for_message_rows(rows)
        self._insert_rows("slack_messages", rows, SLACK_MESSAGE_COLUMNS)
        self._apply_slack_conversation_stat_changes(increments, latest_candidates, recompute_keys)

    def insert_slack_message_reactions(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("slack_message_reactions", rows, SLACK_REACTION_COLUMNS)

    def insert_slack_files(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("slack_files", rows, SLACK_FILE_COLUMNS)

    def rebuild_slack_conversation_stats(self, *, account: str | None = None, team_id: str | None = None) -> None:
        if team_id is not None and account is None:
            raise ValueError("account is required when team_id is set")
        where: list[str] = ["is_deleted = 0"]
        params: list[Any] = []
        delete_where: list[str] = []
        delete_params: list[Any] = []
        if account is not None:
            where.append("account = %s")
            params.append(account)
            delete_where.append("account = %s")
            delete_params.append(account)
        if team_id is not None:
            where.append("team_id = %s")
            params.append(team_id)
            delete_where.append("team_id = %s")
            delete_params.append(team_id)

        delete_sql = "DELETE FROM slack_conversation_stats"
        if delete_where:
            delete_sql += " WHERE " + " AND ".join(delete_where)
        try:
            self._command("BEGIN")
            self._command(delete_sql, tuple(delete_params))
            self._command(
                f"""
                INSERT INTO slack_conversation_stats (
                    account,
                    team_id,
                    conversation_id,
                    message_count,
                    latest_message_at,
                    updated_at
                )
                SELECT
                    account,
                    team_id,
                    conversation_id,
                    count(*)::bigint AS message_count,
                    max(message_datetime) AS latest_message_at,
                    clock_timestamp() AS updated_at
                FROM slack_messages
                WHERE {" AND ".join(where)}
                GROUP BY account, team_id, conversation_id
                """,
                tuple(params),
            )
            self._command("COMMIT")
        except Exception:
            self._command("ROLLBACK")
            raise

    def _ensure_slack_conversation_stats_backfilled(self) -> None:
        rows = self._query(
            """
            SELECT
                EXISTS (SELECT 1 FROM slack_conversation_stats LIMIT 1),
                EXISTS (SELECT 1 FROM slack_messages LIMIT 1)
            """
        )
        if rows and not bool(rows[0][0]) and bool(rows[0][1]):
            self.rebuild_slack_conversation_stats()

    def _slack_conversation_stat_changes_for_message_rows(
        self,
        rows: list[dict[str, Any]],
    ) -> tuple[dict[tuple[str, str, str], int], dict[tuple[str, str, str], datetime], set[tuple[str, str, str]]]:
        existing_rows = self._load_existing_slack_message_stat_rows(rows)
        increments: dict[tuple[str, str, str], int] = {}
        latest_candidates: dict[tuple[str, str, str], datetime] = {}
        recompute_keys: set[tuple[str, str, str]] = set()
        for row in rows:
            message_key = (
                str(row["account"]),
                str(row["team_id"]),
                str(row["conversation_id"]),
                str(row["message_ts"]),
            )
            conversation_key = message_key[:3]
            existing = existing_rows.get(message_key)
            incoming_sync_version = int(row["sync_version"])
            if existing is not None and int(existing["sync_version"]) > incoming_sync_version:
                continue

            old_live = existing is not None and int(existing["is_deleted"]) == 0
            new_live = int(row["is_deleted"]) == 0
            new_datetime = _ensure_utc(row["message_datetime"])
            if old_live and not new_live:
                recompute_keys.add(conversation_key)
                continue
            if old_live and new_live:
                old_datetime = _ensure_utc(existing["message_datetime"])
                if new_datetime < old_datetime:
                    recompute_keys.add(conversation_key)
                elif new_datetime > old_datetime:
                    current_latest = latest_candidates.get(conversation_key)
                    if current_latest is None or new_datetime > current_latest:
                        latest_candidates[conversation_key] = new_datetime
                continue
            if not old_live and new_live:
                increments[conversation_key] = increments.get(conversation_key, 0) + 1
                current_latest = latest_candidates.get(conversation_key)
                if current_latest is None or new_datetime > current_latest:
                    latest_candidates[conversation_key] = new_datetime
        return increments, latest_candidates, recompute_keys

    def _load_existing_slack_message_stat_rows(self, rows: list[dict[str, Any]]) -> dict[tuple[str, str, str, str], dict[str, Any]]:
        keys = sorted(
            {
                (
                    str(row["account"]),
                    str(row["team_id"]),
                    str(row["conversation_id"]),
                    str(row["message_ts"]),
                )
                for row in rows
            }
        )
        if not keys:
            return {}
        with self._connection.cursor() as cursor:
            execute_values(
                cursor,
                """
                WITH incoming(account, team_id, conversation_id, message_ts) AS (VALUES %s)
                SELECT
                    m.account,
                    m.team_id,
                    m.conversation_id,
                    m.message_ts,
                    m.is_deleted,
                    m.message_datetime,
                    m.sync_version
                FROM slack_messages AS m
                INNER JOIN incoming AS i
                  ON m.account = i.account
                 AND m.team_id = i.team_id
                 AND m.conversation_id = i.conversation_id
                 AND m.message_ts = i.message_ts
                """,
                keys,
                template="(%s, %s, %s, %s)",
                page_size=max(len(keys), 1),
            )
            existing = cursor.fetchall()
        return {
            (str(row[0]), str(row[1]), str(row[2]), str(row[3])): {
                "is_deleted": int(row[4]),
                "message_datetime": row[5],
                "sync_version": int(row[6]),
            }
            for row in existing
        }

    def _apply_slack_conversation_stat_changes(
        self,
        increments: dict[tuple[str, str, str], int],
        latest_candidates: dict[tuple[str, str, str], datetime],
        recompute_keys: set[tuple[str, str, str]],
    ) -> None:
        incremental_rows = [
            (
                account,
                team_id,
                conversation_id,
                increments.get((account, team_id, conversation_id), 0),
                latest_candidates[(account, team_id, conversation_id)],
            )
            for account, team_id, conversation_id in sorted(latest_candidates)
            if (account, team_id, conversation_id) not in recompute_keys
        ]
        if incremental_rows:
            self._upsert_slack_conversation_stat_increments(incremental_rows)
        if recompute_keys:
            self._refresh_slack_conversation_stats_for_keys(sorted(recompute_keys))

    def _upsert_slack_conversation_stat_increments(
        self,
        rows: list[tuple[str, str, str, int, datetime]],
    ) -> None:
        if not rows:
            return
        with self._connection.cursor() as cursor:
            execute_values(
                cursor,
                """
                INSERT INTO slack_conversation_stats (
                    account,
                    team_id,
                    conversation_id,
                    message_count,
                    latest_message_at,
                    updated_at
                )
                VALUES %s
                ON CONFLICT (account, team_id, conversation_id) DO UPDATE SET
                    message_count = slack_conversation_stats.message_count + EXCLUDED.message_count,
                    latest_message_at = GREATEST(
                        slack_conversation_stats.latest_message_at,
                        EXCLUDED.latest_message_at
                    ),
                    updated_at = EXCLUDED.updated_at
                """,
                [
                    (
                        account,
                        team_id,
                        conversation_id,
                        int(message_count),
                        _ensure_utc(latest_message_at),
                        datetime.now(tz=UTC),
                    )
                    for account, team_id, conversation_id, message_count, latest_message_at in rows
                ],
                template="(%s, %s, %s, %s, %s, %s)",
                page_size=1000,
            )

    def _refresh_slack_conversation_stats_for_keys(self, keys: list[tuple[str, str, str]]) -> None:
        try:
            self._command("BEGIN")
            with self._connection.cursor() as cursor:
                execute_values(
                    cursor,
                    """
                    WITH affected(account, team_id, conversation_id) AS (VALUES %s)
                    DELETE FROM slack_conversation_stats AS s
                    USING affected AS a
                    WHERE s.account = a.account
                      AND s.team_id = a.team_id
                      AND s.conversation_id = a.conversation_id
                    """,
                    keys,
                    template="(%s, %s, %s)",
                    page_size=1000,
                )
                execute_values(
                    cursor,
                    """
                    WITH affected(account, team_id, conversation_id) AS (VALUES %s)
                    INSERT INTO slack_conversation_stats (
                        account,
                        team_id,
                        conversation_id,
                        message_count,
                        latest_message_at,
                        updated_at
                    )
                    SELECT
                        m.account,
                        m.team_id,
                        m.conversation_id,
                        count(*)::bigint AS message_count,
                        max(m.message_datetime) AS latest_message_at,
                        clock_timestamp() AS updated_at
                    FROM slack_messages AS m
                    INNER JOIN affected AS a
                      ON m.account = a.account
                     AND m.team_id = a.team_id
                     AND m.conversation_id = a.conversation_id
                    WHERE m.is_deleted = 0
                    GROUP BY m.account, m.team_id, m.conversation_id
                    """,
                    keys,
                    template="(%s, %s, %s)",
                    page_size=1000,
                )
            self._command("COMMIT")
        except Exception:
            self._command("ROLLBACK")
            raise

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
            [(account, team_id, object_type, object_id, cursor_ts, last_sync_type, status, error, updated_at, int(sync_version))],
            SLACK_SYNC_STATE_COLUMNS,
        )

    def refresh_slack_account_state_items(self, *, account: str, team_id: str, synced_at: datetime) -> None:
        sync_version = int(_ensure_utc(synced_at).timestamp() * 1_000_000)
        columns = ", ".join(_identifier(column) for column in SLACK_ACCOUNT_STATE_ITEM_ROW_COLUMNS)
        active_rows = self._query(
            f"""
            SELECT {columns}
            FROM slack_account_state_item_rows
            WHERE account = %s
              AND scope_id = %s
              AND is_deleted = 0
            """,
            (account, team_id),
        )
        self._command(
            f"""
            INSERT INTO slack_account_state_item_rows ({columns})
            {self._slack_account_state_items_select_sql()}
            {_upsert_clause("slack_account_state_item_rows", POSTGRES_TABLES["slack_account_state_item_rows"])}
            """,
            (account, team_id, synced_at, sync_version + 1),
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
            FROM slack_messages
            WHERE account = %s
              AND team_id = %s
              AND conversation_id = %s
              AND is_deleted = 0
              AND {_numeric_ts("message_ts")} >= %s
              AND {_numeric_ts("message_ts")} <= %s
            """,
            (account, team_id, conversation_id, float(oldest_ts), float(latest_ts)),
        )
        return {str(row[0]) for row in rows}

    def _backfill_voice_memo_transcription_run_content_hashes(self) -> None:
        self._command(
            """
            UPDATE apple_voice_memos_transcription_runs AS r
            SET content_sha256 = f.content_sha256,
                sync_version = GREATEST(r.sync_version + 1, (extract(epoch from clock_timestamp()) * 1000000)::bigint)
            FROM apple_voice_memos_files AS f
            WHERE r.account = f.account
              AND r.recording_id = f.recording_id
              AND r.content_sha256 = ''
              AND f.content_sha256 != ''
            """
        )

    def _backfill_voice_memo_enrichment_content_hashes(self) -> None:
        self._command(
            """
            UPDATE apple_voice_memos_enrichments AS e
            SET content_sha256 = f.content_sha256,
                sync_version = GREATEST(e.sync_version + 1, (extract(epoch from clock_timestamp()) * 1000000)::bigint)
            FROM apple_voice_memos_files AS f
            WHERE e.account = f.account
              AND e.recording_id = f.recording_id
              AND e.content_sha256 = ''
              AND f.content_sha256 != ''
            """
        )

    def _ensure_clean_gmail_inbox_view(self) -> None:
        self._ensure_utf8_byte_prefix_function()
        self._command(
            """
            CREATE OR REPLACE VIEW clean_gmail_inbox AS
            SELECT
                account,
                thread_id,
                max(internal_date) AS latest_at,
                (array_agg(from_address ORDER BY internal_date DESC, message_id ASC))[1] AS latest_from_address,
                (array_agg(subject ORDER BY internal_date DESC, message_id ASC))[1] AS subject,
                pdw_utf8_byte_prefix(
                    (array_agg(
                        COALESCE(NULLIF(body_markdown_clean, ''), NULLIF(body_markdown, ''), NULLIF(body_text, ''), snippet)
                        ORDER BY internal_date DESC, message_id ASC
                    ))[1],
                    1000
                ) AS latest_preview,
                CASE
                    WHEN count(*) FILTER (WHERE 'UNREAD' = ANY(label_ids)) > 0 THEN 'unread'
                    WHEN count(*) FILTER (WHERE 'IMPORTANT' = ANY(label_ids)) > 0 THEN 'important'
                    WHEN count(*) FILTER (WHERE 'STARRED' = ANY(label_ids)) > 0 THEN 'starred'
                    ELSE 'inbox'
                END AS state,
                count(*) FILTER (WHERE 'UNREAD' = ANY(label_ids))::bigint AS unread_count,
                count(*) FILTER (WHERE 'IMPORTANT' = ANY(label_ids))::bigint AS important_count,
                '[' || string_agg(
                    '{"internal_date":' || replace(to_json(to_char(internal_date AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.MS'))::text, '/', '\\/') ||
                    ',"from_address":' || replace(to_json(from_address)::text, '/', '\\/') ||
                    ',"to_addresses":' || replace(array_to_json(to_addresses)::text, '/', '\\/') ||
                    ',"cc_addresses":' || replace(array_to_json(cc_addresses)::text, '/', '\\/') ||
                    ',"body_markdown_clean":' || replace(to_json(body_markdown_clean)::text, '/', '\\/') ||
                    '}',
                    ',' ORDER BY internal_date ASC, message_id ASC
                ) || ']' AS thread_messages_json
            FROM gmail_messages
            WHERE is_deleted = 0
              AND 'INBOX' = ANY(label_ids)
              AND NOT ('TRASH' = ANY(label_ids))
              AND NOT ('SPAM' = ANY(label_ids))
            GROUP BY account, thread_id
            """
        )

    def _ensure_utf8_byte_prefix_function(self) -> None:
        self._command(
            """
            CREATE OR REPLACE FUNCTION pdw_utf8_byte_prefix(value text, max_bytes integer)
            RETURNS text
            LANGUAGE plpgsql
            IMMUTABLE
            STRICT
            AS $$
            DECLARE
                raw bytea;
                byte_count integer;
            BEGIN
                raw := substring(convert_to(value, 'UTF8') from 1 for greatest(max_bytes, 0));
                byte_count := length(raw);

                WHILE byte_count >= 0 LOOP
                    BEGIN
                        RETURN convert_from(substring(raw from 1 for byte_count), 'UTF8');
                    EXCEPTION WHEN others THEN
                        byte_count := byte_count - 1;
                    END;
                END LOOP;

                RETURN '';
            END;
            $$;
            """
        )

    def _ensure_clean_slack_inbox_view(self) -> None:
        self._command(
            """
            CREATE OR REPLACE VIEW clean_slack_inbox AS
            SELECT
                account,
                scope_id AS team_id,
                item_type AS kind,
                item_state AS state,
                priority_rank AS priority,
                latest_activity_at AS latest_at,
                container_id AS conversation_id,
                container_name AS conversation_name,
                thread_id AS thread_ts,
                message_id AS message_ts,
                actor_id,
                actor_name,
                title,
                preview,
                unread_count,
                reason
            FROM slack_account_state_item_rows
            WHERE is_deleted = 0
            """
        )

    def _ensure_clean_finance_views(self) -> None:
        self._command(
            """
            CREATE OR REPLACE VIEW clean_finance_accounts AS
            SELECT
                a.source,
                a.item_name,
                a.item_id,
                i.institution_name,
                a.account_id,
                a.name,
                a.official_name,
                a.mask,
                a.type,
                a.subtype,
                a.current_balance,
                a.available_balance,
                a.limit_balance,
                a.iso_currency_code,
                a.unofficial_currency_code,
                a.synced_at
            FROM finance_accounts AS a
            LEFT JOIN finance_items AS i
              ON a.source = i.source
             AND a.item_id = i.item_id
            WHERE a.is_deleted = 0
            """
        )
        self._command(
            """
            CREATE OR REPLACE VIEW clean_finance_transactions AS
            SELECT
                t.source,
                t.item_name,
                t.item_id,
                i.institution_name,
                t.account_id,
                a.name AS account_name,
                t.transaction_id,
                t.pending_transaction_id,
                t.date,
                t.authorized_date,
                t.name,
                t.merchant_name,
                t.amount,
                t.iso_currency_code,
                t.personal_finance_category_primary,
                t.personal_finance_category_detailed,
                t.payment_channel,
                t.pending,
                t.synced_at
            FROM finance_transactions AS t
            LEFT JOIN finance_items AS i
              ON t.source = i.source
             AND t.item_id = i.item_id
            LEFT JOIN finance_accounts AS a
              ON t.source = a.source
             AND t.item_id = a.item_id
             AND t.account_id = a.account_id
            WHERE t.is_removed = 0
            """
        )
        self._command(
            """
            CREATE OR REPLACE VIEW clean_finance_holdings AS
            SELECT
                h.source,
                h.item_name,
                h.item_id,
                i.institution_name,
                h.account_id,
                a.name AS account_name,
                h.security_id,
                s.name AS security_name,
                s.ticker_symbol,
                s.type AS security_type,
                h.quantity,
                h.cost_basis,
                h.institution_price,
                h.institution_value,
                h.iso_currency_code,
                h.synced_at
            FROM finance_investment_holdings AS h
            LEFT JOIN finance_items AS i
              ON h.source = i.source
             AND h.item_id = i.item_id
            LEFT JOIN finance_accounts AS a
              ON h.source = a.source
             AND h.item_id = a.item_id
             AND h.account_id = a.account_id
            LEFT JOIN finance_investment_securities AS s
              ON h.source = s.source
             AND h.item_id = s.item_id
             AND h.security_id = s.security_id
            WHERE h.is_deleted = 0
            """
        )

    def _ensure_clean_calendar_transcript_views_if_possible(self) -> None:
        if not all(
            self._relation_exists(table)
            for table in ("calendar_events", "apple_voice_memos_files", "apple_voice_memos_enrichments")
        ):
            return
        self._command(
            """
            CREATE OR REPLACE VIEW clean_calendar_with_transcripts AS
            WITH latest_calendar_events AS (
                SELECT DISTINCT ON (event_id)
                    account AS calendar_account,
                    event_id,
                    calendar_id,
                    organizer_email,
                    summary,
                    description,
                    location,
                    start_at,
                    end_at,
                    is_all_day,
                    attendees_json,
                    html_link
                FROM calendar_events
                WHERE is_deleted = 0
                ORDER BY event_id, synced_at DESC, account DESC, calendar_id DESC
            ),
            latest_enrichments AS (
                SELECT DISTINCT ON (account, recording_id)
                    account,
                    recording_id,
                    calendar_event_id,
                    calendar_confidence,
                    title,
                    start_at,
                    end_at,
                    participants_json,
                    transcript,
                    summary,
                    action_items_json,
                    evidence_json,
                    created_at AS enriched_at
                FROM apple_voice_memos_enrichments
                WHERE status = 'completed'
                ORDER BY account, recording_id, created_at DESC, provider DESC, model DESC, prompt_version DESC
            )
            SELECT
                c.calendar_account AS calendar_account,
                e.account AS recording_account,
                c.calendar_id,
                c.event_id,
                e.recording_id,
                CASE WHEN e.title != '' THEN e.title ELSE c.summary END AS title,
                e.start_at,
                e.end_at,
                c.organizer_email,
                c.summary AS calendar_title,
                c.description AS calendar_description,
                c.location,
                c.start_at AS calendar_start_at,
                c.end_at AS calendar_end_at,
                c.is_all_day,
                c.attendees_json,
                c.html_link AS calendar_url,
                e.calendar_confidence,
                e.participants_json,
                e.transcript,
                e.summary,
                e.action_items_json,
                e.evidence_json,
                e.enriched_at AS created_at
            FROM latest_calendar_events AS c
            INNER JOIN latest_enrichments AS e
              ON c.event_id = e.calendar_event_id
            WHERE e.calendar_event_id != ''
            """
        )
        self._command(
            """
            CREATE OR REPLACE VIEW clean_transcripts_no_calendar_match AS
            WITH latest_calendar_events AS (
                SELECT event_id
                FROM calendar_events
                WHERE is_deleted = 0
                GROUP BY event_id
            ),
            latest_enrichments AS (
                SELECT DISTINCT ON (account, recording_id)
                    account,
                    recording_id,
                    calendar_event_id,
                    calendar_confidence,
                    title,
                    start_at,
                    end_at,
                    participants_json,
                    transcript,
                    summary,
                    action_items_json,
                    evidence_json,
                    created_at AS enriched_at
                FROM apple_voice_memos_enrichments
                WHERE status = 'completed'
                ORDER BY account, recording_id, created_at DESC, provider DESC, model DESC, prompt_version DESC
            )
            SELECT
                e.account,
                e.recording_id,
                f.recorded_at,
                CASE WHEN e.title != '' THEN e.title ELSE f.title END AS title,
                e.start_at,
                e.end_at,
                e.calendar_event_id AS attempted_calendar_event_id,
                e.calendar_confidence,
                CASE
                    WHEN e.calendar_event_id = '' THEN 'no_calendar_event_id'
                    WHEN e.calendar_confidence <= 0 THEN 'low_calendar_confidence'
                    WHEN c.event_id IS NULL THEN 'calendar_event_not_found'
                    ELSE 'no_calendar_match'
                END AS calendar_match_issue,
                e.participants_json,
                e.transcript,
                e.summary,
                e.action_items_json,
                e.evidence_json,
                e.enriched_at AS created_at
            FROM latest_enrichments AS e
            LEFT JOIN apple_voice_memos_files AS f
              ON e.account = f.account
             AND e.recording_id = f.recording_id
             AND f.is_deleted = 0
            LEFT JOIN latest_calendar_events AS c
              ON e.calendar_event_id = c.event_id
            WHERE e.calendar_event_id = ''
               OR e.calendar_confidence <= 0
               OR c.event_id IS NULL
            """
        )

    def _relation_exists(self, relation: str) -> bool:
        rows = self._query(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = %s
            UNION ALL
            SELECT 1
            FROM information_schema.views
            WHERE table_schema = %s
              AND table_name = %s
            LIMIT 1
            """,
            (self._schema, relation, self._schema, relation),
        )
        return bool(rows)

    def _slack_account_state_items_select_sql(self) -> str:
        last_read = _json_numeric("c.raw_json", "last_read")
        message_ts = _numeric_ts("m.message_ts")
        parent_thread_last_read = _json_numeric("p.raw_json", "last_read")
        parent_is_subscribed = "COALESCE((p.raw_json::jsonb ->> 'subscribed')::boolean, false)"
        return f"""
            WITH
                vars AS (
                    SELECT %s::text AS account, %s::text AS team_id, %s::timestamptz AS synced_at, %s::bigint AS sync_version
                ),
                recent_messages AS NOT MATERIALIZED (
                    SELECT m.*
                    FROM slack_messages AS m, vars
                    WHERE m.account = vars.account
                      AND m.team_id = vars.team_id
                      AND m.is_deleted = 0
                      AND m.message_datetime >= now() - INTERVAL '30 days'
                ),
                current_conversations AS NOT MATERIALIZED (
                    SELECT c.*, {last_read} AS last_read_ts
                    FROM slack_conversations AS c, vars
                    WHERE c.account = vars.account
                      AND c.team_id = vars.team_id
                      AND c.is_archived = 0
                      AND (c.is_member = 1 OR c.is_im = 1 OR c.is_mpim = 1)
                )
            SELECT
                'slack' AS source,
                c.account,
                c.team_id AS scope_id,
                'slack:' || c.account || ':' || c.team_id || ':dm:' || c.conversation_id AS item_id,
                CASE WHEN c.is_im = 1 THEN 'direct_message' ELSE 'group_direct_message' END AS item_type,
                CASE WHEN c.last_read_ts > 0 AND max({message_ts}) > c.last_read_ts THEN 'unread' ELSE 'recent' END AS item_state,
                CASE
                    WHEN c.last_read_ts > 0 AND max({message_ts}) > c.last_read_ts AND c.is_im = 1 THEN 10
                    WHEN c.last_read_ts > 0 AND max({message_ts}) > c.last_read_ts AND c.is_mpim = 1 THEN 15
                    WHEN c.is_im = 1 THEN 35
                    ELSE 36
                END AS priority_rank,
                max(m.message_datetime) AS latest_activity_at,
                c.conversation_id AS container_id,
                c.name AS container_name,
                '' AS thread_id,
                (array_agg(m.message_ts ORDER BY m.message_datetime DESC))[1] AS message_id,
                (array_agg(m.user_id ORDER BY m.message_datetime DESC))[1] AS actor_id,
                (array_agg(COALESCE(NULLIF(u.display_name, ''), NULLIF(u.real_name, ''), u.name, '') ORDER BY m.message_datetime DESC))[1] AS actor_name,
                CASE
                    WHEN c.is_im = 1 AND (array_agg(COALESCE(NULLIF(u.display_name, ''), NULLIF(u.real_name, ''), u.name, '') ORDER BY m.message_datetime DESC))[1] != ''
                        THEN (array_agg(COALESCE(NULLIF(u.display_name, ''), NULLIF(u.real_name, ''), u.name, '') ORDER BY m.message_datetime DESC))[1]
                    WHEN c.name != '' THEN c.name
                    WHEN c.is_im = 1 THEN 'Direct message'
                    ELSE 'Group direct message'
                END AS title,
                substring((array_agg(m.text ORDER BY m.message_datetime DESC))[1] FROM 1 FOR 1000) AS preview,
                count(*) FILTER (WHERE c.last_read_ts > 0 AND {message_ts} > c.last_read_ts)::bigint AS unread_count,
                CASE
                    WHEN c.last_read_ts > 0 AND max({message_ts}) > c.last_read_ts
                        THEN CASE WHEN c.is_im = 1 THEN 'Unread Slack direct message' ELSE 'Unread Slack group direct message' END
                    ELSE CASE WHEN c.is_im = 1 THEN 'Recent Slack direct message; read state unavailable or already read' ELSE 'Recent Slack group direct message; read state unavailable or already read' END
                END AS reason,
                'slack_messages' AS source_table,
                'Query slack_messages by account, team_id, conversation_id, and thread_ts/message_ts for full context.' AS drilldown_hint,
                0 AS is_deleted,
                vars.synced_at,
                vars.sync_version
            FROM vars
            INNER JOIN slack_account_identities AS i
              ON i.account = vars.account AND i.team_id = vars.team_id
            INNER JOIN current_conversations AS c
              ON i.account = c.account AND i.team_id = c.team_id
            INNER JOIN recent_messages AS m
              ON c.account = m.account AND c.team_id = m.team_id AND c.conversation_id = m.conversation_id
            LEFT JOIN slack_users AS u
              ON m.account = u.account AND m.team_id = u.team_id AND m.user_id = u.user_id
            WHERE c.is_im = 1 OR c.is_mpim = 1
            GROUP BY vars.synced_at, vars.sync_version, c.account, c.team_id, c.conversation_id, c.name, c.is_im, c.is_mpim, c.last_read_ts, i.user_id
            HAVING (array_agg(m.user_id ORDER BY m.message_datetime DESC))[1] != i.user_id

            UNION ALL

            SELECT
                'slack',
                c.account,
                c.team_id,
                'slack:' || c.account || ':' || c.team_id || ':mention:' || m.conversation_id || ':' || m.message_ts,
                'mention',
                CASE WHEN c.last_read_ts > 0 AND {message_ts} > c.last_read_ts THEN 'unread' ELSE 'mentioned' END,
                CASE WHEN c.last_read_ts > 0 AND {message_ts} > c.last_read_ts THEN 20 ELSE 22 END,
                m.message_datetime,
                c.conversation_id,
                c.name,
                m.thread_ts,
                m.message_ts,
                m.user_id,
                COALESCE(NULLIF(u.display_name, ''), NULLIF(u.real_name, ''), u.name, ''),
                CASE WHEN c.name != '' THEN c.name ELSE c.conversation_id END,
                substring(m.text FROM 1 FOR 1000),
                1::bigint,
                CASE WHEN c.last_read_ts > 0 AND {message_ts} > c.last_read_ts THEN 'Unread Slack message mentioning the authenticated user' ELSE 'Recent Slack message mentioning the authenticated user' END,
                'slack_messages',
                'Query slack_messages by account, team_id, conversation_id, and thread_ts/message_ts for full context.',
                0,
                vars.synced_at,
                vars.sync_version
            FROM vars
            INNER JOIN slack_account_identities AS i
              ON i.account = vars.account AND i.team_id = vars.team_id
            INNER JOIN current_conversations AS c
              ON i.account = c.account AND i.team_id = c.team_id
            INNER JOIN recent_messages AS m
              ON c.account = m.account AND c.team_id = m.team_id AND c.conversation_id = m.conversation_id
            LEFT JOIN slack_users AS u
              ON m.account = u.account AND m.team_id = u.team_id AND m.user_id = u.user_id
            WHERE m.user_id != i.user_id
              AND position('<@' || i.user_id || '>' in m.text) > 0

            UNION ALL

            SELECT
                'slack',
                p.account,
                p.team_id,
                'slack:' || p.account || ':' || p.team_id || ':thread:' || p.conversation_id || ':' || p.message_ts,
                'participating_thread',
                CASE WHEN COALESCE(NULLIF({parent_thread_last_read}, 0), c.last_read_ts) > 0
                       AND max({_numeric_ts("r.message_ts")}) > COALESCE(NULLIF({parent_thread_last_read}, 0), c.last_read_ts)
                    THEN 'unread' ELSE 'recent' END,
                CASE WHEN COALESCE(NULLIF({parent_thread_last_read}, 0), c.last_read_ts) > 0
                       AND max({_numeric_ts("r.message_ts")}) > COALESCE(NULLIF({parent_thread_last_read}, 0), c.last_read_ts)
                    THEN 25 ELSE 45 END,
                max(r.message_datetime),
                p.conversation_id,
                c.name,
                p.message_ts,
                p.message_ts,
                (array_agg(r.user_id ORDER BY r.message_datetime DESC))[1],
                (array_agg(COALESCE(NULLIF(ru.display_name, ''), NULLIF(ru.real_name, ''), ru.name, '') ORDER BY r.message_datetime DESC))[1],
                CASE WHEN c.name != '' THEN c.name ELSE p.conversation_id END,
                substring(p.text FROM 1 FOR 1000),
                count(*) FILTER (
                    WHERE COALESCE(NULLIF({parent_thread_last_read}, 0), c.last_read_ts) > 0
                      AND {_numeric_ts("r.message_ts")} > COALESCE(NULLIF({parent_thread_last_read}, 0), c.last_read_ts)
                )::bigint,
                CASE WHEN COALESCE(NULLIF({parent_thread_last_read}, 0), c.last_read_ts) > 0
                       AND max({_numeric_ts("r.message_ts")}) > COALESCE(NULLIF({parent_thread_last_read}, 0), c.last_read_ts)
                    THEN 'Unread replies in a Slack thread the authenticated user has participated in'
                    ELSE 'Recent replies in a Slack thread the authenticated user has participated in' END,
                'slack_messages',
                'Query slack_messages by account, team_id, conversation_id, and thread_ts for the full thread.',
                0,
                vars.synced_at,
                vars.sync_version
            FROM vars
            INNER JOIN slack_account_identities AS i
              ON i.account = vars.account AND i.team_id = vars.team_id
            INNER JOIN recent_messages AS p
              ON p.account = i.account
             AND p.team_id = i.team_id
             AND p.is_thread_reply = 0
             AND p.reply_count > 0
             AND {_numeric_ts("p.latest_reply_ts")} >= extract(epoch from now() - INTERVAL '30 days')
            INNER JOIN current_conversations AS c
              ON p.account = c.account AND p.team_id = c.team_id AND p.conversation_id = c.conversation_id
            INNER JOIN recent_messages AS r
              ON p.account = r.account
             AND p.team_id = r.team_id
             AND p.conversation_id = r.conversation_id
             AND p.message_ts = r.thread_ts
            LEFT JOIN slack_users AS ru
              ON r.account = ru.account AND r.team_id = ru.team_id AND r.user_id = ru.user_id
            GROUP BY vars.synced_at, vars.sync_version, p.account, p.team_id, p.conversation_id, p.message_ts, p.user_id, p.text, p.raw_json, c.name, c.last_read_ts, i.user_id
            HAVING (count(*) FILTER (WHERE r.user_id = i.user_id OR p.user_id = i.user_id) > 0 OR {parent_is_subscribed})
               AND (array_agg(r.user_id ORDER BY r.message_datetime DESC))[1] != i.user_id

            UNION ALL

            SELECT
                'slack',
                c.account,
                c.team_id,
                'slack:' || c.account || ':' || c.team_id || ':channel:' || c.conversation_id,
                'channel_unread',
                'unread',
                50,
                max(m.message_datetime),
                c.conversation_id,
                c.name,
                '',
                (array_agg(m.message_ts ORDER BY m.message_datetime DESC))[1],
                (array_agg(m.user_id ORDER BY m.message_datetime DESC))[1],
                (array_agg(COALESCE(NULLIF(u.display_name, ''), NULLIF(u.real_name, ''), u.name, '') ORDER BY m.message_datetime DESC))[1],
                CASE WHEN c.name != '' THEN c.name ELSE c.conversation_id END,
                substring((array_agg(m.text ORDER BY m.message_datetime DESC))[1] FROM 1 FOR 1000),
                count(*)::bigint,
                'Unread Slack channel messages',
                'slack_messages',
                'Query slack_messages by account, team_id, conversation_id, and message_ts for full context.',
                0,
                vars.synced_at,
                vars.sync_version
            FROM vars
            INNER JOIN slack_account_identities AS i
              ON i.account = vars.account AND i.team_id = vars.team_id
            INNER JOIN current_conversations AS c
              ON i.account = c.account AND i.team_id = c.team_id
            INNER JOIN recent_messages AS m
              ON c.account = m.account AND c.team_id = m.team_id AND c.conversation_id = m.conversation_id
            LEFT JOIN slack_users AS u
              ON m.account = u.account AND m.team_id = u.team_id AND m.user_id = u.user_id
            WHERE c.is_im = 0
              AND c.is_mpim = 0
              AND c.last_read_ts > 0
              AND m.is_thread_reply = 0
              AND m.user_id != i.user_id
              AND {_numeric_ts("m.message_ts")} > c.last_read_ts
              AND position('<@' || i.user_id || '>' in m.text) = 0
            GROUP BY vars.synced_at, vars.sync_version, c.account, c.team_id, c.conversation_id, c.name
        """

    def _command(self, sql: str, params: Sequence[Any] | None = None) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(sql, params)

    def _query(self, sql: str, params: Sequence[Any] | None = None) -> list[tuple[Any, ...]]:
        with self._connection.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

    def _insert(self, table: str, rows: list[tuple[Any, ...]], columns: tuple[str, ...]) -> None:
        if not rows:
            return
        spec = POSTGRES_TABLES[table]
        column_sql = ", ".join(_identifier(column) for column in columns)
        template = "(" + ", ".join(["%s"] * len(columns)) + ")"
        sql = f"""
            INSERT INTO {_identifier(table)} ({column_sql})
            VALUES %s
            {_upsert_clause(table, spec, columns)}
        """
        with self._connection.cursor() as cursor:
            execute_values(cursor, sql, rows, template=template, page_size=POSTGRES_INSERT_PAGE_SIZES.get(table, 1000))

    def _insert_rows(self, table: str, rows: list[dict[str, Any]], columns: tuple[str, ...]) -> None:
        self._insert(table, [tuple(_normalize_insert_value(row[column]) for column in columns) for row in rows], columns)


def _postgres_type(column: str) -> str:
    if column in ARRAY_COLUMNS:
        return "text[]"
    if column in TIMESTAMP_COLUMNS:
        return "timestamptz"
    if column in FLOAT_COLUMNS:
        return "double precision"
    if column in INTEGER_COLUMNS:
        return "bigint"
    return "text"


def _default_sql(column: str) -> str:
    if column in ARRAY_COLUMNS:
        return "'{}'::text[]"
    if column in TIMESTAMP_COLUMNS:
        return "'1970-01-01 00:00:00+00'::timestamptz"
    if column in FLOAT_COLUMNS:
        return "0"
    if column in INTEGER_COLUMNS:
        return "0"
    return "''"


def _upsert_clause(table: str, spec: TableSpec, columns: tuple[str, ...] | None = None) -> str:
    columns = columns or spec.columns
    update_columns = [column for column in columns if column not in spec.primary_key]
    conflict_columns = ", ".join(_identifier(column) for column in spec.primary_key)
    if not update_columns:
        return f"ON CONFLICT ({conflict_columns}) DO NOTHING"
    preserve_non_empty_columns = {
        "apple_message_attachments": {
            "content_sha256",
            "storage_backend",
            "storage_key",
            "storage_file_id",
            "storage_url",
        }
    }.get(table, set())
    assignments = ", ".join(
        _upsert_assignment(table=table, column=column, preserve_non_empty=column in preserve_non_empty_columns)
        for column in update_columns
    )
    version_column = spec.version_column
    return (
        f"ON CONFLICT ({conflict_columns}) DO UPDATE SET {assignments} "
        f"WHERE {_identifier(table)}.{_identifier(version_column)} <= EXCLUDED.{_identifier(version_column)}"
    )


def _upsert_assignment(*, table: str, column: str, preserve_non_empty: bool) -> str:
    quoted_column = _identifier(column)
    excluded_column = f"EXCLUDED.{quoted_column}"
    if preserve_non_empty:
        return f"{quoted_column} = COALESCE(NULLIF({excluded_column}, ''), {_identifier(table)}.{quoted_column})"
    return f"{quoted_column} = {excluded_column}"


def _identifier(value: str) -> str:
    return '"' + _validate_identifier(value).replace('"', '""') + '"'


def _validate_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"invalid SQL identifier: {value!r}")
    return value


def _normalize_insert_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if isinstance(value, str):
        return value.replace("\x00", POSTGRES_TEXT_NUL_REPLACEMENT)
    if isinstance(value, list):
        return [_normalize_insert_value(item) for item in value]
    if isinstance(value, tuple):
        return [_normalize_insert_value(item) for item in value]
    return value


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _missing_json_field(payload: dict[str, Any], field: str) -> bool:
    return field not in payload or payload[field] is None or payload[field] == ""


def _json_payloads(rows: Iterable[tuple[Any, ...]]) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for (raw_json,) in rows:
        try:
            parsed = json.loads(str(raw_json))
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            payloads.append(parsed)
    return payloads


def _postgres_retryable_error_clause(column: str) -> str:
    return " OR ".join(
        f"{column} ILIKE '%%{_escape_like(pattern)}%%' ESCAPE E'\\\\'"
        for pattern in RETRYABLE_VOICE_MEMO_TRANSCRIPTION_ERROR_PATTERNS
    )


def _escape_like(value: str) -> str:
    return value.replace("'", "''").replace("%", r"\%").replace("_", r"\_")


def _postgres_gmail_attachment_candidate_clause() -> str:
    return (
        "(position('\"attachmentId\"' in payload_json) > 0 "
        "OR payload_json ~ '\"filename\":\"[^\"]+\"' "
        "OR position(lower('Content-Disposition') in lower(payload_json)) > 0)"
    )


def _numeric_ts(expression: str) -> str:
    return f"COALESCE(NULLIF({expression}, '')::numeric, 0)"


def _json_numeric(expression: str, field: str) -> str:
    return f"COALESCE(NULLIF(({expression}::jsonb ->> '{field}'), '')::numeric, 0)"
