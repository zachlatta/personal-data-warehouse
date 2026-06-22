from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import hashlib
import json
import re
from typing import Any
import uuid

import psycopg2
from psycopg2 import Binary
from psycopg2.extras import Json, execute_values

from personal_data_warehouse.schema import (
    AGENT_RUN_COLUMNS,
    AGENT_RUN_EVENT_COLUMNS,
    AGENT_RUN_TOOL_CALL_COLUMNS,
    AGENT_SESSION_EVENT_COLUMNS,
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
    CONTACT_CARD_COLUMNS,
    CONTACT_SYNC_STATE_COLUMNS,
    GOOGLE_DRIVE_FILE_COLUMNS,
    GOOGLE_DRIVE_FILE_TEXT_COLUMNS,
    GOOGLE_DRIVE_SYNC_STATE_COLUMNS,
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
    WHATSAPP_CHAT_COLUMNS,
    WHATSAPP_CHAT_PARTICIPANT_COLUMNS,
    WHATSAPP_CONTACT_COLUMNS,
    WHATSAPP_MEDIA_ITEM_COLUMNS,
    WHATSAPP_MESSAGE_COLUMNS,
    GoogleDriveSyncState,
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
UPSTREAM_MUTATION_CLAIMABLE_STATUSES = ("approved", "failed_retryable")
GOOGLE_CONTACTS_BATCH_MUTATION_OPERATION = "contacts.batch_mutation"
GMAIL_ARCHIVE_OPERATION = "gmail.archive_threads"
GMAIL_UNARCHIVE_OPERATION = "gmail.unarchive_threads"
GMAIL_SEND_EMAIL_OPERATION = "gmail.send_email"
CALENDAR_PROVIDER = "google_calendar"
CALENDAR_CREATE_EVENT_OPERATION = "calendar.create_event"
CALENDAR_UPDATE_EVENT_OPERATION = "calendar.update_event"
CALENDAR_DELETE_EVENT_OPERATION = "calendar.delete_event"
CALENDAR_EVENT_OPERATIONS = (
    CALENDAR_CREATE_EVENT_OPERATION,
    CALENDAR_UPDATE_EVENT_OPERATION,
    CALENDAR_DELETE_EVENT_OPERATION,
)
REMOVED_PERSONAL_FINANCE_VIEWS = (
    "clean_finance_accounts",
    "clean_finance_transactions",
    "clean_finance_holdings",
)
REMOVED_PERSONAL_FINANCE_TABLES = (
    "finance_sync_state",
    "finance_liabilities",
    "finance_investment_transactions",
    "finance_investment_securities",
    "finance_investment_holdings",
    "finance_transactions",
    "finance_accounts",
    "finance_items",
)


@dataclass(frozen=True)
class TableSpec:
    columns: tuple[str, ...]
    primary_key: tuple[str, ...]
    version_column: str = "sync_version"
    # Per-table storage parameters applied via ALTER TABLE ... SET after creation.
    # Used to override autovacuum thresholds on large, append-heavy tables whose
    # default size-proportional triggers would otherwise rarely fire (leaving stale
    # planner statistics and unreclaimed dead tuples).
    storage_parameters: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class IndexSpec:
    name: str
    table: str
    sql: str
    requires_pg_trgm: bool = False
    requires_pg_textsearch: bool = False


POSTGRES_TABLES: dict[str, TableSpec] = {
    "gmail_messages": TableSpec(
        MESSAGE_COLUMNS,
        ("account", "message_id"),
        storage_parameters=(
            ("autovacuum_analyze_scale_factor", "0.02"),
            ("autovacuum_vacuum_scale_factor", "0.05"),
        ),
    ),
    "gmail_attachments": TableSpec(ATTACHMENT_COLUMNS, ("account", "message_id", "part_id", "filename")),
    "gmail_sync_state": TableSpec(SYNC_STATE_COLUMNS, ("account",), "updated_at"),
    "gmail_attachment_backfill_state": TableSpec(
        ATTACHMENT_BACKFILL_STATE_COLUMNS,
        ("account", "message_id"),
    ),
    "file_attachment_enrichments": TableSpec(
        ATTACHMENT_ENRICHMENT_COLUMNS,
        ("content_sha256", "ai_provider", "ai_model", "ai_prompt_version"),
    ),
    "calendar_events": TableSpec(CALENDAR_EVENT_COLUMNS, ("account", "calendar_id", "event_id")),
    "calendar_sync_state": TableSpec(CALENDAR_SYNC_STATE_COLUMNS, ("account", "calendar_id")),
    "contact_cards": TableSpec(
        CONTACT_CARD_COLUMNS,
        ("source", "account", "source_kind", "address_book_id", "card_id"),
    ),
    "contact_sync_state": TableSpec(
        CONTACT_SYNC_STATE_COLUMNS,
        ("source", "account", "source_kind", "address_book_id"),
    ),
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
    "whatsapp_chats": TableSpec(WHATSAPP_CHAT_COLUMNS, ("account", "chat_id")),
    "whatsapp_chat_participants": TableSpec(
        WHATSAPP_CHAT_PARTICIPANT_COLUMNS,
        ("account", "chat_id", "participant_jid"),
    ),
    "whatsapp_contacts": TableSpec(WHATSAPP_CONTACT_COLUMNS, ("account", "jid")),
    # Protocol message IDs are sender-generated, so they are only unique
    # within a chat; the chat JID is part of the key.
    "whatsapp_messages": TableSpec(WHATSAPP_MESSAGE_COLUMNS, ("account", "chat_id", "message_id")),
    "whatsapp_media_items": TableSpec(
        WHATSAPP_MEDIA_ITEM_COLUMNS,
        ("account", "chat_id", "message_id"),
    ),
    "agent_session_events": TableSpec(
        AGENT_SESSION_EVENT_COLUMNS,
        ("source", "session_id", "event_uuid"),
        storage_parameters=(
            ("autovacuum_analyze_scale_factor", "0"),
            ("autovacuum_analyze_threshold", "50000"),
            ("autovacuum_vacuum_scale_factor", "0"),
            ("autovacuum_vacuum_threshold", "100000"),
        ),
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
    "slack_messages": TableSpec(
        SLACK_MESSAGE_COLUMNS,
        ("account", "team_id", "conversation_id", "message_ts"),
        storage_parameters=(
            ("autovacuum_analyze_scale_factor", "0"),
            ("autovacuum_analyze_threshold", "50000"),
            ("autovacuum_vacuum_scale_factor", "0"),
            ("autovacuum_vacuum_threshold", "100000"),
        ),
    ),
    "slack_conversation_stats": TableSpec(
        SLACK_CONVERSATION_STATS_COLUMNS,
        ("account", "team_id", "conversation_id"),
        "updated_at",
    ),
    "slack_message_reactions": TableSpec(
        SLACK_REACTION_COLUMNS,
        ("account", "team_id", "conversation_id", "message_ts", "reaction_name", "user_id"),
        storage_parameters=(
            ("autovacuum_analyze_scale_factor", "0"),
            ("autovacuum_analyze_threshold", "20000"),
            ("autovacuum_vacuum_scale_factor", "0"),
            ("autovacuum_vacuum_threshold", "50000"),
        ),
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
    "google_drive_files": TableSpec(
        GOOGLE_DRIVE_FILE_COLUMNS,
        ("account", "file_id"),
        storage_parameters=(
            ("autovacuum_analyze_scale_factor", "0.02"),
            ("autovacuum_vacuum_scale_factor", "0.05"),
        ),
    ),
    "google_drive_file_texts": TableSpec(
        GOOGLE_DRIVE_FILE_TEXT_COLUMNS,
        ("account", "file_id", "extractor"),
    ),
    "google_drive_sync_state": TableSpec(
        GOOGLE_DRIVE_SYNC_STATE_COLUMNS,
        ("account",),
        "updated_at",
    ),
}

POSTGRES_INDEXES: tuple[IndexSpec, ...] = (
    IndexSpec(
        "gmail_messages_thread_idx",
        "gmail_messages",
        "CREATE INDEX IF NOT EXISTS gmail_messages_thread_idx ON gmail_messages (account, thread_id, internal_date DESC)",
    ),
    IndexSpec(
        "gmail_messages_internal_date_idx",
        "gmail_messages",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS gmail_messages_internal_date_idx ON gmail_messages (internal_date DESC)",
    ),
    IndexSpec(
        "gmail_messages_label_ids_idx",
        "gmail_messages",
        "CREATE INDEX IF NOT EXISTS gmail_messages_label_ids_idx ON gmail_messages USING gin (label_ids)",
    ),
    IndexSpec(
        "gmail_messages_from_trgm_idx",
        "gmail_messages",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS gmail_messages_from_trgm_idx ON gmail_messages USING gin (from_address public.gin_trgm_ops)",
        requires_pg_trgm=True,
    ),
    IndexSpec(
        "gmail_messages_subject_trgm_idx",
        "gmail_messages",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS gmail_messages_subject_trgm_idx ON gmail_messages USING gin (subject public.gin_trgm_ops)",
        requires_pg_trgm=True,
    ),
    IndexSpec(
        "gmail_messages_snippet_trgm_idx",
        "gmail_messages",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS gmail_messages_snippet_trgm_idx ON gmail_messages USING gin (snippet public.gin_trgm_ops)",
        requires_pg_trgm=True,
    ),
    IndexSpec(
        "gmail_messages_body_text_trgm_idx",
        "gmail_messages",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS gmail_messages_body_text_trgm_idx ON gmail_messages USING gin (body_text public.gin_trgm_ops)",
        requires_pg_trgm=True,
    ),
    IndexSpec(
        "gmail_messages_body_markdown_trgm_idx",
        "gmail_messages",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS gmail_messages_body_markdown_trgm_idx ON gmail_messages USING gin (body_markdown_clean public.gin_trgm_ops)",
        requires_pg_trgm=True,
    ),
    IndexSpec(
        "gmail_messages_body_html_trgm_idx",
        "gmail_messages",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS gmail_messages_body_html_trgm_idx ON gmail_messages USING gin (body_html public.gin_trgm_ops)",
        requires_pg_trgm=True,
    ),
    # BM25 relevance-ranked word search (pg_textsearch). Full-coverage,
    # single-column indexes so callers can use the implicit
    # ORDER BY col <@> 'query' LIMIT n syntax; partial bm25 indexes would
    # force the explicit to_bm25query() form. body_html is skipped on
    # purpose: markup tokens pollute the BM25 lexicon and
    # body_markdown_clean already covers that content.
    IndexSpec(
        "gmail_messages_subject_bm25_idx",
        "gmail_messages",
        "CREATE INDEX IF NOT EXISTS gmail_messages_subject_bm25_idx ON gmail_messages USING bm25 (subject) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "gmail_messages_body_text_bm25_idx",
        "gmail_messages",
        "CREATE INDEX IF NOT EXISTS gmail_messages_body_text_bm25_idx ON gmail_messages USING bm25 (body_text) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "gmail_messages_body_markdown_bm25_idx",
        "gmail_messages",
        "CREATE INDEX IF NOT EXISTS gmail_messages_body_markdown_bm25_idx ON gmail_messages USING bm25 (body_markdown_clean) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "gmail_attachments_message_idx",
        "gmail_attachments",
        "CREATE INDEX IF NOT EXISTS gmail_attachments_message_idx ON gmail_attachments (account, message_id)",
    ),
    IndexSpec(
        "file_attachment_enrichments_text_bm25_idx",
        "file_attachment_enrichments",
        "CREATE INDEX IF NOT EXISTS file_attachment_enrichments_text_bm25_idx ON file_attachment_enrichments USING bm25 (text) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "file_attachment_enrichments_text_trgm_idx",
        "file_attachment_enrichments",
        "CREATE INDEX IF NOT EXISTS file_attachment_enrichments_text_trgm_idx ON file_attachment_enrichments USING gin (text public.gin_trgm_ops)",
        requires_pg_trgm=True,
    ),
    IndexSpec(
        "calendar_events_time_idx",
        "calendar_events",
        "CREATE INDEX IF NOT EXISTS calendar_events_time_idx ON calendar_events (start_at, end_at)",
    ),
    IndexSpec(
        "contact_cards_display_idx",
        "contact_cards",
        "CREATE INDEX IF NOT EXISTS contact_cards_display_idx ON contact_cards (account, source_kind, display_name) WHERE is_deleted = 0",
    ),
    IndexSpec(
        "contact_cards_primary_email_idx",
        "contact_cards",
        "CREATE INDEX IF NOT EXISTS contact_cards_primary_email_idx ON contact_cards (lower(primary_email)) WHERE is_deleted = 0 AND primary_email != ''",
    ),
    IndexSpec(
        "contact_cards_primary_phone_idx",
        "contact_cards",
        "CREATE INDEX IF NOT EXISTS contact_cards_primary_phone_idx ON contact_cards (lower(primary_phone)) WHERE is_deleted = 0 AND primary_phone != ''",
    ),
    IndexSpec(
        "contact_cards_source_updated_idx",
        "contact_cards",
        "CREATE INDEX IF NOT EXISTS contact_cards_source_updated_idx ON contact_cards (source_updated_at DESC)",
    ),
    IndexSpec(
        "contact_cards_raw_json_idx",
        "contact_cards",
        "CREATE INDEX IF NOT EXISTS contact_cards_raw_json_idx ON contact_cards USING gin (raw_json)",
    ),
    IndexSpec(
        "voice_memo_files_recorded_idx",
        "apple_voice_memos_files",
        "CREATE INDEX IF NOT EXISTS voice_memo_files_recorded_idx ON apple_voice_memos_files (recorded_at DESC)",
    ),
    IndexSpec(
        "apple_voice_memos_transcript_bm25_idx",
        "apple_voice_memos_enrichments",
        "CREATE INDEX IF NOT EXISTS apple_voice_memos_transcript_bm25_idx ON apple_voice_memos_enrichments USING bm25 (transcript) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "apple_voice_memos_transcript_trgm_idx",
        "apple_voice_memos_enrichments",
        "CREATE INDEX IF NOT EXISTS apple_voice_memos_transcript_trgm_idx ON apple_voice_memos_enrichments USING gin (transcript public.gin_trgm_ops)",
        requires_pg_trgm=True,
    ),
    IndexSpec(
        "apple_notes_modified_idx",
        "apple_notes",
        "CREATE INDEX IF NOT EXISTS apple_notes_modified_idx ON apple_notes (modified_at DESC) WHERE is_deleted = 0",
    ),
    IndexSpec(
        "apple_notes_title_bm25_idx",
        "apple_notes",
        "CREATE INDEX IF NOT EXISTS apple_notes_title_bm25_idx ON apple_notes USING bm25 (title) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "apple_notes_body_bm25_idx",
        "apple_notes",
        "CREATE INDEX IF NOT EXISTS apple_notes_body_bm25_idx ON apple_notes USING bm25 (body_text) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "apple_notes_title_trgm_idx",
        "apple_notes",
        "CREATE INDEX IF NOT EXISTS apple_notes_title_trgm_idx ON apple_notes USING gin (title public.gin_trgm_ops)",
        requires_pg_trgm=True,
    ),
    IndexSpec(
        "apple_notes_body_trgm_idx",
        "apple_notes",
        "CREATE INDEX IF NOT EXISTS apple_notes_body_trgm_idx ON apple_notes USING gin (body_text public.gin_trgm_ops)",
        requires_pg_trgm=True,
    ),
    IndexSpec(
        "apple_note_revisions_note_idx",
        "apple_note_revisions",
        "CREATE INDEX IF NOT EXISTS apple_note_revisions_note_idx ON apple_note_revisions (account, note_id, modified_at DESC)",
    ),
    IndexSpec(
        "apple_note_attachments_hash_idx",
        "apple_note_attachments",
        "CREATE INDEX IF NOT EXISTS apple_note_attachments_hash_idx ON apple_note_attachments (content_sha256)",
    ),
    IndexSpec(
        "apple_messages_time_idx",
        "apple_messages",
        "CREATE INDEX IF NOT EXISTS apple_messages_time_idx ON apple_messages (message_at DESC) WHERE is_deleted = 0",
    ),
    IndexSpec(
        "apple_messages_body_trgm_idx",
        "apple_messages",
        "CREATE INDEX IF NOT EXISTS apple_messages_body_trgm_idx ON apple_messages USING gin (body_text public.gin_trgm_ops) WHERE is_deleted = 0",
        requires_pg_trgm=True,
    ),
    IndexSpec(
        # Full coverage (no is_deleted filter) so the implicit <@> syntax
        # stays index-backed; callers filter is_deleted in SQL.
        "apple_messages_body_bm25_idx",
        "apple_messages",
        "CREATE INDEX IF NOT EXISTS apple_messages_body_bm25_idx ON apple_messages USING bm25 (body_text) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "apple_message_chat_messages_chat_time_idx",
        "apple_message_chat_messages",
        "CREATE INDEX IF NOT EXISTS apple_message_chat_messages_chat_time_idx ON apple_message_chat_messages (account, chat_id, message_date DESC)",
    ),
    IndexSpec(
        "apple_message_attachments_hash_idx",
        "apple_message_attachments",
        "CREATE INDEX IF NOT EXISTS apple_message_attachments_hash_idx ON apple_message_attachments (content_sha256)",
    ),
    IndexSpec(
        "whatsapp_messages_time_idx",
        "whatsapp_messages",
        "CREATE INDEX IF NOT EXISTS whatsapp_messages_time_idx ON whatsapp_messages (message_at DESC) WHERE is_deleted = 0",
    ),
    IndexSpec(
        "whatsapp_messages_chat_time_idx",
        "whatsapp_messages",
        "CREATE INDEX IF NOT EXISTS whatsapp_messages_chat_time_idx ON whatsapp_messages (account, chat_id, message_at DESC)",
    ),
    IndexSpec(
        "whatsapp_messages_body_trgm_idx",
        "whatsapp_messages",
        "CREATE INDEX IF NOT EXISTS whatsapp_messages_body_trgm_idx ON whatsapp_messages USING gin (body_text public.gin_trgm_ops) WHERE is_deleted = 0",
        requires_pg_trgm=True,
    ),
    IndexSpec(
        # Full coverage (no is_deleted filter) so the implicit <@> syntax
        # stays index-backed; callers filter is_deleted in SQL.
        "whatsapp_messages_body_bm25_idx",
        "whatsapp_messages",
        "CREATE INDEX IF NOT EXISTS whatsapp_messages_body_bm25_idx ON whatsapp_messages USING bm25 (body_text) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "whatsapp_media_items_hash_idx",
        "whatsapp_media_items",
        "CREATE INDEX IF NOT EXISTS whatsapp_media_items_hash_idx ON whatsapp_media_items (content_sha256)",
    ),
    IndexSpec(
        "agent_runs_task_status_subject_idx",
        "agent_runs",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS agent_runs_task_status_subject_idx ON agent_runs (task_type, status, subject_id)",
    ),
    IndexSpec(
        "agent_session_events_session_seq_idx",
        "agent_session_events",
        "CREATE INDEX IF NOT EXISTS agent_session_events_session_seq_idx ON agent_session_events (source, session_id, seq)",
    ),
    IndexSpec(
        "agent_session_events_time_idx",
        "agent_session_events",
        "CREATE INDEX IF NOT EXISTS agent_session_events_time_idx ON agent_session_events (occurred_at DESC)",
    ),
    IndexSpec(
        "agent_session_events_text_trgm_idx",
        "agent_session_events",
        "CREATE INDEX IF NOT EXISTS agent_session_events_text_trgm_idx ON agent_session_events USING gin (text public.gin_trgm_ops) WHERE text != ''",
        requires_pg_trgm=True,
    ),
    IndexSpec(
        "agent_session_events_text_bm25_idx",
        "agent_session_events",
        "CREATE INDEX IF NOT EXISTS agent_session_events_text_bm25_idx ON agent_session_events USING bm25 (text) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "agent_run_events_created_idx",
        "agent_run_events",
        "CREATE INDEX IF NOT EXISTS agent_run_events_created_idx ON agent_run_events (created_at DESC)",
    ),
    IndexSpec(
        "slack_messages_conversation_time_idx",
        "slack_messages",
        "CREATE INDEX IF NOT EXISTS slack_messages_conversation_time_idx ON slack_messages (account, team_id, conversation_id, message_datetime DESC)",
    ),
    IndexSpec(
        # Single-column index on message_datetime so global MIN/MAX/COUNT
        # probes and time-only date-range scans use an index instead of a
        # full table scan across all 30M+ messages.
        "slack_messages_time_idx",
        "slack_messages",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS slack_messages_time_idx ON slack_messages (message_datetime DESC)",
    ),
    IndexSpec(
        "slack_messages_user_time_idx",
        "slack_messages",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS slack_messages_user_time_idx ON slack_messages (user_id, message_datetime DESC)",
    ),
    IndexSpec(
        "slack_messages_synced_at_idx",
        "slack_messages",
        "CREATE INDEX IF NOT EXISTS slack_messages_synced_at_idx ON slack_messages (synced_at)",
    ),
    IndexSpec(
        "slack_messages_recent_scope_time_idx",
        "slack_messages",
        "CREATE INDEX IF NOT EXISTS slack_messages_recent_scope_time_idx ON slack_messages (account, team_id, message_datetime DESC) WHERE is_deleted = 0",
    ),
    IndexSpec(
        "slack_messages_recent_thread_time_idx",
        "slack_messages",
        "CREATE INDEX IF NOT EXISTS slack_messages_recent_thread_time_idx ON slack_messages (account, team_id, thread_ts, message_datetime DESC) WHERE is_deleted = 0",
    ),
    IndexSpec(
        "slack_messages_thread_idx",
        "slack_messages",
        "CREATE INDEX IF NOT EXISTS slack_messages_thread_idx ON slack_messages (account, team_id, conversation_id, thread_ts)",
    ),
    IndexSpec(
        # Full-coverage trgm index on slack_messages.text. Replaces the
        # earlier partial (WHERE is_deleted=0) index so queries that omit
        # the is_deleted filter still get index acceleration.
        "slack_messages_text_trgm_idx",
        "slack_messages",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS slack_messages_text_trgm_idx ON slack_messages USING gin (text public.gin_trgm_ops)",
        requires_pg_trgm=True,
    ),
    IndexSpec(
        # pg_textsearch has no CONCURRENTLY support, so a cold build here
        # write-blocks slack_messages for the build duration. Pre-build this
        # one manually before deploying to fresh large datasets.
        "slack_messages_text_bm25_idx",
        "slack_messages",
        "CREATE INDEX IF NOT EXISTS slack_messages_text_bm25_idx ON slack_messages USING bm25 (text) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "slack_conversations_scope_idx",
        "slack_conversations",
        "CREATE INDEX IF NOT EXISTS slack_conversations_scope_idx ON slack_conversations (account, team_id, conversation_type)",
    ),
    IndexSpec(
        "slack_conversations_synced_at_idx",
        "slack_conversations",
        "CREATE INDEX IF NOT EXISTS slack_conversations_synced_at_idx ON slack_conversations (synced_at)",
    ),
    IndexSpec(
        "slack_users_email_lower_idx",
        "slack_users",
        "CREATE INDEX IF NOT EXISTS slack_users_email_lower_idx ON slack_users (lower(email)) WHERE email != ''",
    ),
    IndexSpec(
        "slack_users_synced_at_idx",
        "slack_users",
        "CREATE INDEX IF NOT EXISTS slack_users_synced_at_idx ON slack_users (synced_at)",
    ),
    IndexSpec(
        "slack_conversation_members_synced_at_idx",
        "slack_conversation_members",
        "CREATE INDEX IF NOT EXISTS slack_conversation_members_synced_at_idx ON slack_conversation_members (synced_at)",
    ),
    IndexSpec(
        "slack_state_scope_idx",
        "slack_sync_state",
        "CREATE INDEX IF NOT EXISTS slack_state_scope_idx ON slack_sync_state (account, team_id, object_type, object_id)",
    ),
    IndexSpec(
        "slack_account_state_live_scope_idx",
        "slack_account_state_item_rows",
        "CREATE INDEX IF NOT EXISTS slack_account_state_live_scope_idx ON slack_account_state_item_rows (account, scope_id, priority_rank, latest_activity_at DESC) WHERE is_deleted = 0",
    ),
    # BM25 indexes backing the cross-source search_text() function for every
    # source the (removed) searchable_text view used to cover. Full coverage (no
    # partial WHERE) so the implicit/explicit <@> top-k stays index-backed;
    # search_text() applies is_deleted / non-empty filters in SQL. The heavy
    # sources (gmail/slack/messages/notes/whatsapp/transcripts/agent sessions)
    # already have their bm25 indexes above; these add the remaining columns.
    IndexSpec(
        "gmail_attachments_filename_bm25_idx",
        "gmail_attachments",
        "CREATE INDEX IF NOT EXISTS gmail_attachments_filename_bm25_idx ON gmail_attachments USING bm25 (filename) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "slack_conversations_name_bm25_idx",
        "slack_conversations",
        "CREATE INDEX IF NOT EXISTS slack_conversations_name_bm25_idx ON slack_conversations USING bm25 (name) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "slack_conversations_topic_bm25_idx",
        "slack_conversations",
        "CREATE INDEX IF NOT EXISTS slack_conversations_topic_bm25_idx ON slack_conversations USING bm25 (topic) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "slack_conversations_purpose_bm25_idx",
        "slack_conversations",
        "CREATE INDEX IF NOT EXISTS slack_conversations_purpose_bm25_idx ON slack_conversations USING bm25 (purpose) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "slack_files_name_bm25_idx",
        "slack_files",
        "CREATE INDEX IF NOT EXISTS slack_files_name_bm25_idx ON slack_files USING bm25 (name) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "slack_files_title_bm25_idx",
        "slack_files",
        "CREATE INDEX IF NOT EXISTS slack_files_title_bm25_idx ON slack_files USING bm25 (title) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "apple_voice_memos_title_bm25_idx",
        "apple_voice_memos_enrichments",
        "CREATE INDEX IF NOT EXISTS apple_voice_memos_title_bm25_idx ON apple_voice_memos_enrichments USING bm25 (title) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "apple_voice_memos_summary_bm25_idx",
        "apple_voice_memos_enrichments",
        "CREATE INDEX IF NOT EXISTS apple_voice_memos_summary_bm25_idx ON apple_voice_memos_enrichments USING bm25 (summary) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "apple_voice_memos_participants_bm25_idx",
        "apple_voice_memos_enrichments",
        "CREATE INDEX IF NOT EXISTS apple_voice_memos_participants_bm25_idx ON apple_voice_memos_enrichments USING bm25 (participants_json) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "apple_voice_memos_action_items_bm25_idx",
        "apple_voice_memos_enrichments",
        "CREATE INDEX IF NOT EXISTS apple_voice_memos_action_items_bm25_idx ON apple_voice_memos_enrichments USING bm25 (action_items_json) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "apple_note_revisions_body_bm25_idx",
        "apple_note_revisions",
        "CREATE INDEX IF NOT EXISTS apple_note_revisions_body_bm25_idx ON apple_note_revisions USING bm25 (body_text) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "whatsapp_chats_name_bm25_idx",
        "whatsapp_chats",
        "CREATE INDEX IF NOT EXISTS whatsapp_chats_name_bm25_idx ON whatsapp_chats USING bm25 (name) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "whatsapp_media_items_filename_bm25_idx",
        "whatsapp_media_items",
        "CREATE INDEX IF NOT EXISTS whatsapp_media_items_filename_bm25_idx ON whatsapp_media_items USING bm25 (filename) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "calendar_events_summary_bm25_idx",
        "calendar_events",
        "CREATE INDEX IF NOT EXISTS calendar_events_summary_bm25_idx ON calendar_events USING bm25 (summary) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "calendar_events_description_bm25_idx",
        "calendar_events",
        "CREATE INDEX IF NOT EXISTS calendar_events_description_bm25_idx ON calendar_events USING bm25 (description) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "calendar_events_location_bm25_idx",
        "calendar_events",
        "CREATE INDEX IF NOT EXISTS calendar_events_location_bm25_idx ON calendar_events USING bm25 (location) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "calendar_events_attendees_bm25_idx",
        "calendar_events",
        "CREATE INDEX IF NOT EXISTS calendar_events_attendees_bm25_idx ON calendar_events USING bm25 (attendees_json) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "contact_cards_name_bm25_idx",
        "contact_cards",
        "CREATE INDEX IF NOT EXISTS contact_cards_name_bm25_idx ON contact_cards USING bm25 (display_name) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "contact_cards_organization_bm25_idx",
        "contact_cards",
        "CREATE INDEX IF NOT EXISTS contact_cards_organization_bm25_idx ON contact_cards USING bm25 (organization) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "contact_cards_job_title_bm25_idx",
        "contact_cards",
        "CREATE INDEX IF NOT EXISTS contact_cards_job_title_bm25_idx ON contact_cards USING bm25 (job_title) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "contact_cards_notes_bm25_idx",
        "contact_cards",
        "CREATE INDEX IF NOT EXISTS contact_cards_notes_bm25_idx ON contact_cards USING bm25 (notes) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "agent_run_events_text_bm25_idx",
        "agent_run_events",
        "CREATE INDEX IF NOT EXISTS agent_run_events_text_bm25_idx ON agent_run_events USING bm25 (text) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "agent_session_events_title_bm25_idx",
        "agent_session_events",
        "CREATE INDEX IF NOT EXISTS agent_session_events_title_bm25_idx ON agent_session_events USING bm25 (session_title) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "upstream_mutations_title_bm25_idx",
        "upstream_mutations",
        "CREATE INDEX IF NOT EXISTS upstream_mutations_title_bm25_idx ON upstream_mutations USING bm25 (title) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "upstream_mutation_requests_title_bm25_idx",
        "upstream_mutation_requests",
        "CREATE INDEX IF NOT EXISTS upstream_mutation_requests_title_bm25_idx ON upstream_mutation_requests USING bm25 (title) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "upstream_mutation_requests_reason_bm25_idx",
        "upstream_mutation_requests",
        "CREATE INDEX IF NOT EXISTS upstream_mutation_requests_reason_bm25_idx ON upstream_mutation_requests USING bm25 (reason) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "google_drive_files_modified_idx",
        "google_drive_files",
        "CREATE INDEX IF NOT EXISTS google_drive_files_modified_idx ON google_drive_files (account, modified_time DESC) WHERE trashed = 0 AND is_excluded = 0",
    ),
    IndexSpec(
        "google_drive_files_name_bm25_idx",
        "google_drive_files",
        "CREATE INDEX IF NOT EXISTS google_drive_files_name_bm25_idx ON google_drive_files USING bm25 (name) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "google_drive_files_name_trgm_idx",
        "google_drive_files",
        "CREATE INDEX IF NOT EXISTS google_drive_files_name_trgm_idx ON google_drive_files USING gin (name public.gin_trgm_ops)",
        requires_pg_trgm=True,
    ),
    IndexSpec(
        "google_drive_file_texts_text_bm25_idx",
        "google_drive_file_texts",
        "CREATE INDEX IF NOT EXISTS google_drive_file_texts_text_bm25_idx ON google_drive_file_texts USING bm25 (text) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "google_drive_file_texts_text_trgm_idx",
        "google_drive_file_texts",
        "CREATE INDEX IF NOT EXISTS google_drive_file_texts_text_trgm_idx ON google_drive_file_texts USING gin (text public.gin_trgm_ops)",
        requires_pg_trgm=True,
    ),
)

# Indexes that used to exist but have been superseded. Dropped idempotently
# during _ensure_indexes so existing deployments converge with fresh installs.
POSTGRES_OBSOLETE_INDEXES: tuple[tuple[str, str], ...] = (
    # Replaced by the full-coverage slack_messages_text_trgm_idx.
    ("slack_messages_text_trgm_live_idx", "slack_messages"),
)



POSTGRES_INSERT_PAGE_SIZES = {
    "apple_notes": 50,
    "apple_note_revisions": 50,
    "apple_note_attachments": 250,
    "apple_messages": 500,
    "apple_message_attachments": 500,
    "whatsapp_messages": 500,
    "whatsapp_media_items": 500,
    "agent_session_events": 500,
}


ARRAY_COLUMNS = {
    "label_ids",
    "to_addresses",
    "cc_addresses",
    "bcc_addresses",
    "recurrence",
}

JSONB_COLUMNS_BY_TABLE = {
    "contact_cards": {
        "emails",
        "phones",
        "addresses",
        "organizations",
        "urls",
        "groups",
        "dates",
        "photos",
        "raw_json",
    },
    "google_drive_files": {
        "parents_json",
        "owners_json",
        "raw_metadata_json",
    },
}

JSONB_ARRAY_COLUMNS_BY_TABLE = {
    "contact_cards": {
        "emails",
        "phones",
        "addresses",
        "organizations",
        "urls",
        "groups",
        "photos",
    },
    "google_drive_files": {
        "parents_json",
        "owners_json",
    },
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
    "full_synced_at",
    "source_updated_at",
    "date_read",
    "date_delivered",
    "date_played",
    "date_edited",
    "date_retracted",
    "date_recovered",
    "ai_processed_at",
    "last_message_at",
    "edited_at",
    "occurred_at",
    "created_time",
    "modified_time",
    "viewed_by_me_time",
    "source_modified_time",
    "full_crawled_at",
    "extracted_at",
}

INTEGER_COLUMNS = {
    "seq",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_creation_tokens",
    "is_sidechain",
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
    "is_admin",
    "is_super_admin",
    "is_google_native",
    "starred",
    "shared",
    "trashed",
    "is_excluded",
    "truncated",
    "char_count",
    "files_seen",
}

FLOAT_COLUMNS = {
    "confidence",
    "calendar_confidence",
}

_WHATSAPP_TABLES = (
    "whatsapp_chats",
    "whatsapp_chat_participants",
    "whatsapp_contacts",
    "whatsapp_messages",
    "whatsapp_media_items",
)


class PostgresWarehouse:
    def __init__(self, postgres_database_url: str, *, schema: str = "public") -> None:
        normalized = normalize_postgres_url(postgres_database_url)
        if not normalized:
            raise ValueError("POSTGRES_DATABASE_URL must be set")
        self._schema = _validate_identifier(schema)
        self._connection = psycopg2.connect(normalized)
        self._connection.autocommit = True
        self._ensured_index_names: set[str] = set()
        self._pg_trgm_ensured = False
        self._pg_textsearch_ensured = False
        self._command(f"CREATE SCHEMA IF NOT EXISTS {_identifier(self._schema)}")
        self._command(f"SET search_path TO {_identifier(self._schema)}")

    def close(self) -> None:
        self._connection.close()

    def ensure_tables(self) -> None:
        self.drop_personal_finance_schema()
        self._migrate_file_attachment_enrichments_rename()
        self._ensure_table_group(
            [
                "gmail_messages",
                "gmail_attachments",
                "gmail_sync_state",
                "gmail_attachment_backfill_state",
                "file_attachment_enrichments",
            ]
        )
        for column in ("storage_backend", "storage_key", "storage_file_id", "storage_url", "storage_status"):
            self._command(
                f"ALTER TABLE gmail_attachments ADD COLUMN IF NOT EXISTS {_identifier(column)} text NOT NULL DEFAULT ''"
            )
        self._ensure_clean_gmail_inbox_view()
        self._ensure_search_views_if_possible()

    def ensure_file_attachment_enrichment_tables(self) -> None:
        """Ensure the shared file_attachment_enrichments table exists.

        Used by the source-agnostic attachment enrichment runner so it can write
        results without depending on any one source's ensure_* path.
        """
        self._migrate_file_attachment_enrichments_rename()
        self._ensure_table_group(["file_attachment_enrichments"])
        self._ensure_search_views_if_possible()

    def _migrate_file_attachment_enrichments_rename(self) -> None:
        # gmail_attachment_enrichments became the shared file_attachment_enrichments
        # table (Gmail + WhatsApp + any future source). Rename in place to preserve
        # existing enrichments, and drop the old-named bm25/trgm indexes so the
        # ensure step rebuilds them under the new names. Idempotent: once renamed,
        # the IF EXISTS / IF NOT EXISTS clauses no-op on every later deploy.
        self._command(
            "ALTER TABLE IF EXISTS gmail_attachment_enrichments RENAME TO file_attachment_enrichments"
        )
        self._command("DROP INDEX IF EXISTS gmail_attachment_enrichments_text_bm25_idx")
        self._command("DROP INDEX IF EXISTS gmail_attachment_enrichments_text_trgm_idx")

    def ensure_calendar_tables(self) -> None:
        self._ensure_table_group(["calendar_events", "calendar_sync_state"])
        self._ensure_clean_calendar_transcript_views_if_possible()
        self._ensure_search_views_if_possible()

    def ensure_contacts_tables(self) -> None:
        self._ensure_table_group(["contact_cards", "contact_sync_state"])
        self._ensure_clean_contacts_view()
        self._ensure_search_views_if_possible()

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
        self._ensure_search_views_if_possible()

    def ensure_voice_memos_tables(self) -> None:
        self.ensure_apple_voice_memos_tables()

    def ensure_apple_notes_tables(self) -> None:
        self._ensure_table_group(["apple_notes", "apple_note_revisions", "apple_note_attachments"])
        self._ensure_search_views_if_possible()

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
        # The WhatsApp tables ride along here so the search_text() function,
        # whose gate references them, stays recreatable on deployments where the
        # WhatsApp ingest has not run yet.
        self._ensure_table_group(_WHATSAPP_TABLES)
        self._ensure_search_views_if_possible()

    def ensure_whatsapp_tables(self) -> None:
        self._ensure_table_group(_WHATSAPP_TABLES)
        self.ensure_whatsapp_client_session_table()
        self._ensure_clean_whatsapp_messages_view()
        self._ensure_search_views_if_possible()

    def ensure_agent_sessions_tables(self) -> None:
        self._ensure_table_group(["agent_session_events"])
        self.ensure_chatgpt_tables()
        self._ensure_clean_agent_sessions_view()
        self._ensure_search_views_if_possible()

    def ensure_whatsapp_client_session_table(self) -> None:
        self._command(
            """
            CREATE TABLE IF NOT EXISTS whatsapp_client_sessions (
                account text NOT NULL,
                session_key text NOT NULL DEFAULT 'default',
                client_id text NOT NULL DEFAULT '',
                database_bytes bytea NOT NULL DEFAULT ''::bytea,
                database_sha256 text NOT NULL DEFAULT '',
                database_bytes_size bigint NOT NULL DEFAULT 0,
                restored_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
                updated_at timestamptz NOT NULL DEFAULT now(),
                sync_version bigint NOT NULL DEFAULT 1,
                PRIMARY KEY (account, session_key)
            )
            """
        )
        self._command("ALTER TABLE whatsapp_client_sessions ADD COLUMN IF NOT EXISTS client_id text NOT NULL DEFAULT ''")
        self._command("ALTER TABLE whatsapp_client_sessions ADD COLUMN IF NOT EXISTS database_bytes bytea NOT NULL DEFAULT ''::bytea")
        self._command("ALTER TABLE whatsapp_client_sessions ADD COLUMN IF NOT EXISTS database_sha256 text NOT NULL DEFAULT ''")
        self._command("ALTER TABLE whatsapp_client_sessions ADD COLUMN IF NOT EXISTS database_bytes_size bigint NOT NULL DEFAULT 0")
        self._command(
            "ALTER TABLE whatsapp_client_sessions ADD COLUMN IF NOT EXISTS restored_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz"
        )
        self._command("ALTER TABLE whatsapp_client_sessions ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now()")
        self._command("ALTER TABLE whatsapp_client_sessions ADD COLUMN IF NOT EXISTS sync_version bigint NOT NULL DEFAULT 1")

    def get_whatsapp_client_session(self, *, account: str, session_key: str) -> dict[str, Any] | None:
        self.ensure_whatsapp_client_session_table()
        rows = self._query_dicts(
            """
            SELECT account, session_key, client_id, database_bytes, database_sha256,
                   database_bytes_size, restored_at, updated_at, sync_version
            FROM whatsapp_client_sessions
            WHERE account = %s AND session_key = %s
            """,
            (account, session_key),
        )
        if not rows:
            return None
        row = rows[0]
        row["database_bytes"] = bytes(row["database_bytes"])
        return row

    def upsert_whatsapp_client_session(
        self,
        *,
        account: str,
        session_key: str,
        client_id: str,
        database_bytes: bytes,
        restored_at: datetime | None = None,
        updated_at: datetime | None = None,
    ) -> dict[str, Any]:
        self.ensure_whatsapp_client_session_table()
        now = updated_at or datetime.now(tz=UTC)
        restored = restored_at or datetime(1970, 1, 1, tzinfo=UTC)
        database_sha256 = hashlib.sha256(database_bytes).hexdigest()
        sync_version = int(now.astimezone(UTC).timestamp() * 1_000_000)
        self._command(
            """
            INSERT INTO whatsapp_client_sessions (
                account, session_key, client_id, database_bytes, database_sha256,
                database_bytes_size, restored_at, updated_at, sync_version
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (account, session_key) DO UPDATE SET
                client_id = EXCLUDED.client_id,
                database_bytes = EXCLUDED.database_bytes,
                database_sha256 = EXCLUDED.database_sha256,
                database_bytes_size = EXCLUDED.database_bytes_size,
                restored_at = EXCLUDED.restored_at,
                updated_at = EXCLUDED.updated_at,
                sync_version = EXCLUDED.sync_version
            """,
            (
                account,
                session_key,
                client_id,
                Binary(database_bytes),
                database_sha256,
                len(database_bytes),
                restored,
                now,
                sync_version,
            ),
        )
        return {
            "account": account,
            "session_key": session_key,
            "client_id": client_id,
            "database_sha256": database_sha256,
            "database_bytes_size": len(database_bytes),
            "restored_at": restored,
            "updated_at": now,
            "sync_version": sync_version,
        }

    def ensure_chatgpt_tables(self) -> None:
        self.ensure_chatgpt_session_table()
        self.ensure_chatgpt_conversation_sync_table()

    def ensure_chatgpt_session_table(self) -> None:
        """Server-side store for the chatgpt.com web session credential.

        The local ``pdw chatgpt publish-session`` helper captures the session
        cookie from a browser and POSTs it to the app, which upserts it here;
        the Dagster poller reads it to authenticate the backend API. Mirrors
        ``whatsapp_client_sessions`` but holds an opaque token string rather than
        a SQLite snapshot.
        """
        self._command(
            """
            CREATE TABLE IF NOT EXISTS chatgpt_sessions (
                account text NOT NULL,
                session_key text NOT NULL DEFAULT 'default',
                session_token text NOT NULL DEFAULT '',
                source_browser text NOT NULL DEFAULT '',
                token_sha256 text NOT NULL DEFAULT '',
                published_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
                updated_at timestamptz NOT NULL DEFAULT now(),
                sync_version bigint NOT NULL DEFAULT 1,
                PRIMARY KEY (account, session_key)
            )
            """
        )
        self._command("ALTER TABLE chatgpt_sessions ADD COLUMN IF NOT EXISTS source_browser text NOT NULL DEFAULT ''")
        self._command("ALTER TABLE chatgpt_sessions ADD COLUMN IF NOT EXISTS token_sha256 text NOT NULL DEFAULT ''")
        self._command(
            "ALTER TABLE chatgpt_sessions ADD COLUMN IF NOT EXISTS published_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz"
        )
        self._command("ALTER TABLE chatgpt_sessions ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now()")
        self._command("ALTER TABLE chatgpt_sessions ADD COLUMN IF NOT EXISTS sync_version bigint NOT NULL DEFAULT 1")

    def ensure_chatgpt_conversation_sync_table(self) -> None:
        """Per-conversation incremental sync watermark for the ChatGPT poller."""
        self._command(
            """
            CREATE TABLE IF NOT EXISTS chatgpt_conversation_sync (
                account text NOT NULL,
                session_id text NOT NULL,
                update_time double precision NOT NULL DEFAULT 0,
                event_count integer NOT NULL DEFAULT 0,
                synced_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (account, session_id)
            )
            """
        )

    def get_chatgpt_session(self, *, account: str, session_key: str) -> dict[str, Any] | None:
        self.ensure_chatgpt_session_table()
        rows = self._query_dicts(
            """
            SELECT account, session_key, session_token, source_browser, token_sha256,
                   published_at, updated_at, sync_version
            FROM chatgpt_sessions
            WHERE account = %s AND session_key = %s
            """,
            (account, session_key),
        )
        return rows[0] if rows else None

    def upsert_chatgpt_session(
        self,
        *,
        account: str,
        session_key: str,
        session_token: str,
        source_browser: str = "",
        published_at: datetime | None = None,
        updated_at: datetime | None = None,
    ) -> dict[str, Any]:
        self.ensure_chatgpt_session_table()
        now = updated_at or datetime.now(tz=UTC)
        published = published_at or now
        token_sha256 = hashlib.sha256(session_token.encode("utf-8")).hexdigest()
        sync_version = int(now.astimezone(UTC).timestamp() * 1_000_000)
        self._command(
            """
            INSERT INTO chatgpt_sessions (
                account, session_key, session_token, source_browser, token_sha256,
                published_at, updated_at, sync_version
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (account, session_key) DO UPDATE SET
                session_token = EXCLUDED.session_token,
                source_browser = EXCLUDED.source_browser,
                token_sha256 = EXCLUDED.token_sha256,
                published_at = EXCLUDED.published_at,
                updated_at = EXCLUDED.updated_at,
                sync_version = EXCLUDED.sync_version
            """,
            (
                account,
                session_key,
                session_token,
                source_browser,
                token_sha256,
                published,
                now,
                sync_version,
            ),
        )
        return {
            "account": account,
            "session_key": session_key,
            "source_browser": source_browser,
            "token_sha256": token_sha256,
            "published_at": published,
            "updated_at": now,
            "sync_version": sync_version,
        }

    def ensure_voice_memo_transcription_tables(self) -> None:
        self.ensure_apple_voice_memos_tables()

    def ensure_agent_tables(self) -> None:
        self._ensure_table_group(["agent_runs", "agent_run_events", "agent_run_tool_calls"])
        self._ensure_search_views_if_possible()

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
        self._ensure_search_views_if_possible()

    def drop_personal_finance_schema(self) -> None:
        for view in REMOVED_PERSONAL_FINANCE_VIEWS:
            self._command(f"DROP VIEW IF EXISTS {_identifier(view)} CASCADE")
        for table in REMOVED_PERSONAL_FINANCE_TABLES:
            self._command(f"DROP TABLE IF EXISTS {_identifier(table)} CASCADE")

    def ensure_upstream_mutation_tables(self) -> None:
        self._command(
            """
            CREATE TABLE IF NOT EXISTS upstream_mutation_requests (
                id text PRIMARY KEY,
                status text NOT NULL DEFAULT 'pending_review',
                title text NOT NULL DEFAULT '',
                reason text NOT NULL DEFAULT '',
                context_json jsonb NOT NULL DEFAULT '{}'::jsonb,
                result_json jsonb NOT NULL DEFAULT '{}'::jsonb,
                error text NOT NULL DEFAULT '',
                idempotency_key text NOT NULL DEFAULT '',
                revision bigint NOT NULL DEFAULT 1,
                requested_by text NOT NULL DEFAULT '',
                approved_by text NOT NULL DEFAULT '',
                created_at timestamptz NOT NULL DEFAULT now(),
                updated_at timestamptz NOT NULL DEFAULT now(),
                approved_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
                executed_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
                observed_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz
            )
            """
        )
        self._command(
            """
            CREATE TABLE IF NOT EXISTS upstream_mutations (
                id text PRIMARY KEY,
                request_id text NOT NULL DEFAULT '',
                request_index bigint NOT NULL DEFAULT 0,
                provider text NOT NULL DEFAULT '',
                operation text NOT NULL DEFAULT '',
                account text NOT NULL DEFAULT '',
                status text NOT NULL DEFAULT 'pending_review',
                title text NOT NULL DEFAULT '',
                reason text NOT NULL DEFAULT '',
                payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
                preview_json jsonb NOT NULL DEFAULT '{}'::jsonb,
                result_json jsonb NOT NULL DEFAULT '{}'::jsonb,
                error text NOT NULL DEFAULT '',
                idempotency_key text NOT NULL DEFAULT '',
                revision bigint NOT NULL DEFAULT 1,
                attempt_count bigint NOT NULL DEFAULT 0,
                requested_by text NOT NULL DEFAULT '',
                approved_by text NOT NULL DEFAULT '',
                claimed_by text NOT NULL DEFAULT '',
                claimed_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
                created_at timestamptz NOT NULL DEFAULT now(),
                updated_at timestamptz NOT NULL DEFAULT now(),
                approved_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
                executed_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
                observed_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz
            )
            """
        )
        self._command("ALTER TABLE upstream_mutations ADD COLUMN IF NOT EXISTS request_id text NOT NULL DEFAULT ''")
        self._command("ALTER TABLE upstream_mutations ADD COLUMN IF NOT EXISTS request_index bigint NOT NULL DEFAULT 0")
        self._command(
            """
            CREATE TABLE IF NOT EXISTS upstream_mutation_events (
                mutation_id text NOT NULL,
                event_index bigint NOT NULL,
                event_type text NOT NULL DEFAULT '',
                actor_type text NOT NULL DEFAULT '',
                actor_id text NOT NULL DEFAULT '',
                event_json jsonb NOT NULL DEFAULT '{}'::jsonb,
                created_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (mutation_id, event_index)
            )
            """
        )
        self._command(
            """
            CREATE TABLE IF NOT EXISTS upstream_mutation_request_events (
                request_id text NOT NULL,
                event_index bigint NOT NULL,
                event_type text NOT NULL DEFAULT '',
                actor_type text NOT NULL DEFAULT '',
                actor_id text NOT NULL DEFAULT '',
                event_json jsonb NOT NULL DEFAULT '{}'::jsonb,
                created_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (request_id, event_index)
            )
            """
        )
        for sql in (
            "CREATE UNIQUE INDEX IF NOT EXISTS upstream_mutation_requests_idempotency_idx ON upstream_mutation_requests (idempotency_key) WHERE idempotency_key != ''",
            "CREATE INDEX IF NOT EXISTS upstream_mutation_requests_status_updated_idx ON upstream_mutation_requests (status, updated_at)",
            "CREATE UNIQUE INDEX IF NOT EXISTS upstream_mutations_idempotency_idx ON upstream_mutations (idempotency_key) WHERE idempotency_key != ''",
            "CREATE INDEX IF NOT EXISTS upstream_mutations_request_idx ON upstream_mutations (request_id, request_index, created_at, id)",
            "CREATE INDEX IF NOT EXISTS upstream_mutations_status_updated_idx ON upstream_mutations (status, updated_at)",
            "CREATE INDEX IF NOT EXISTS upstream_mutation_request_events_request_idx ON upstream_mutation_request_events (request_id, event_index)",
            "CREATE INDEX IF NOT EXISTS upstream_mutation_events_mutation_idx ON upstream_mutation_events (mutation_id, event_index)",
        ):
            self._command(sql)
        # These raw-DDL tables also carry BM25 IndexSpecs (for search_text); they
        # get built (with the pg_textsearch bootstrap + graceful-skip) by
        # _ensure_search_views_if_possible, which builds every searchable table's
        # indexes before recreating the function.
        self._ensure_search_views_if_possible()

    def gmail_archive_thread_previews(self, *, account: str, thread_ids: Sequence[str]) -> list[dict[str, Any]]:
        normalized_thread_ids = _normalize_thread_ids(thread_ids)
        if not normalized_thread_ids:
            return []
        rows = self._query(
            """
            SELECT
                thread_id,
                (array_agg(subject ORDER BY internal_date DESC, message_id ASC))[1] AS subject,
                (array_agg(from_address ORDER BY internal_date DESC, message_id ASC))[1] AS latest_from_address,
                max(internal_date) AS latest_at,
                count(*)::bigint AS inbox_message_count
            FROM gmail_messages
            WHERE account = %s
              AND thread_id = ANY(%s)
              AND is_deleted = 0
              AND 'INBOX' = ANY(label_ids)
              AND NOT ('TRASH' = ANY(label_ids))
              AND NOT ('SPAM' = ANY(label_ids))
            GROUP BY thread_id
            """,
            (account, list(normalized_thread_ids)),
        )
        rows_by_thread_id = {
            str(row[0]): {
                "thread_id": str(row[0]),
                "subject": str(row[1]),
                "latest_from_address": str(row[2]),
                "latest_at": row[3],
                "inbox_message_count": int(row[4]),
            }
            for row in rows
        }
        return [rows_by_thread_id[thread_id] for thread_id in normalized_thread_ids if thread_id in rows_by_thread_id]

    def gmail_unarchive_thread_previews(self, *, account: str, thread_ids: Sequence[str]) -> list[dict[str, Any]]:
        normalized_thread_ids = _normalize_thread_ids(thread_ids)
        if not normalized_thread_ids:
            return []
        rows = self._query(
            """
            SELECT
                thread_id,
                (array_agg(subject ORDER BY internal_date DESC, message_id ASC))[1] AS subject,
                (array_agg(from_address ORDER BY internal_date DESC, message_id ASC))[1] AS latest_from_address,
                max(internal_date) AS latest_at,
                count(*)::bigint AS message_count
            FROM gmail_messages
            WHERE account = %s
              AND thread_id = ANY(%s)
              AND is_deleted = 0
              AND NOT ('INBOX' = ANY(label_ids))
              AND NOT ('TRASH' = ANY(label_ids))
              AND NOT ('SPAM' = ANY(label_ids))
            GROUP BY thread_id
            """,
            (account, list(normalized_thread_ids)),
        )
        rows_by_thread_id = {
            str(row[0]): {
                "thread_id": str(row[0]),
                "subject": str(row[1]),
                "latest_from_address": str(row[2]),
                "latest_at": row[3],
                "message_count": int(row[4]),
            }
            for row in rows
        }
        return [rows_by_thread_id[thread_id] for thread_id in normalized_thread_ids if thread_id in rows_by_thread_id]

    def propose_mutation(
        self,
        *,
        title: str,
        reason: str,
        mutations: Sequence[Mapping[str, Any]],
        context: dict[str, Any] | None = None,
        requested_by: str = "mcp",
    ) -> dict[str, Any]:
        self.ensure_tables()
        self.ensure_contacts_tables()
        self.ensure_upstream_mutation_tables()
        if not title.strip():
            raise ValueError("title must not be blank")
        if not reason.strip():
            raise ValueError("reason must not be blank")
        normalized_mutations = self._normalize_upstream_mutation_request_mutations(
            request_reason=reason,
            request_context=context or {},
            mutations=mutations,
        )
        if not normalized_mutations:
            raise ValueError("mutations must include at least one mutation")

        idempotency_key = _upstream_mutation_request_idempotency_key(
            title=title,
            reason=reason,
            mutations=normalized_mutations,
        )
        existing = self.get_upstream_mutation_request_by_idempotency_key(idempotency_key)
        if existing is not None:
            return existing

        now = datetime.now(tz=UTC)
        request_id = f"req_{uuid.uuid4().hex}"
        self._command(
            """
            INSERT INTO upstream_mutation_requests (
                id, status, title, reason, context_json, result_json, idempotency_key,
                requested_by, created_at, updated_at
            )
            VALUES (%s, 'pending_review', %s, %s, %s, '{}'::jsonb, %s, %s, %s, %s)
            """,
            (
                request_id,
                title,
                reason,
                _jsonb_param(context or {}),
                idempotency_key,
                requested_by,
                now,
                now,
            ),
        )
        self._append_upstream_mutation_request_event(
            request_id,
            event_type="created",
            actor_type="agent",
            actor_id=requested_by,
            event_json={"title": title, "reason": reason, "context": context or {}, "mutation_count": len(normalized_mutations)},
        )
        for index, mutation in enumerate(normalized_mutations):
            mutation_id = f"mut_{uuid.uuid4().hex}"
            self._command(
                """
                INSERT INTO upstream_mutations (
                    id, request_id, request_index, provider, operation, account, status, title, reason,
                    payload_json, preview_json, result_json, idempotency_key,
                    requested_by, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, 'pending_review', %s, %s, %s, %s, '{}'::jsonb, '', %s, %s, %s)
                """,
                (
                    mutation_id,
                    request_id,
                    index,
                    mutation["provider"],
                    mutation["operation"],
                    mutation["account"],
                    mutation["title"],
                    mutation["reason"],
                    _jsonb_param(mutation["payload_json"]),
                    _jsonb_param(mutation["preview_json"]),
                    requested_by,
                    now,
                    now,
                ),
            )
            self._append_upstream_mutation_event(
                mutation_id,
                event_type="created",
                actor_type="agent",
                actor_id=requested_by,
                event_json={
                    "request_id": request_id,
                    "request_index": index,
                    "title": mutation["title"],
                    "reason": mutation["reason"],
                    "payload": mutation["payload_json"],
                    "preview": mutation["preview_json"],
                },
            )
        created = self.get_upstream_mutation_request(request_id)
        if created is None:
            raise RuntimeError(f"created mutation request {request_id} could not be loaded")
        return created

    def get_upstream_mutation(self, mutation_id: str) -> dict[str, Any] | None:
        rows = self._query_dicts("SELECT * FROM upstream_mutations WHERE id = %s", (mutation_id,))
        return rows[0] if rows else None

    def get_upstream_mutation_request(self, request_id: str) -> dict[str, Any] | None:
        self.ensure_upstream_mutation_tables()
        rows = self._query_dicts("SELECT * FROM upstream_mutation_requests WHERE id = %s", (request_id,))
        if not rows:
            return None
        request = rows[0]
        request["mutations"] = self.list_upstream_mutations_for_request(request_id)
        return request

    def get_upstream_mutation_request_by_idempotency_key(self, idempotency_key: str) -> dict[str, Any] | None:
        rows = self._query_dicts(
            "SELECT id FROM upstream_mutation_requests WHERE idempotency_key = %s",
            (idempotency_key,),
        )
        return self.get_upstream_mutation_request(str(rows[0]["id"])) if rows else None

    def list_upstream_mutation_requests(
        self,
        *,
        statuses: Sequence[str] | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        self.ensure_upstream_mutation_tables()
        if statuses:
            return self._query_dicts(
                """
                SELECT request.*,
                       count(mutation.id)::bigint AS mutation_count
                FROM upstream_mutation_requests AS request
                LEFT JOIN upstream_mutations AS mutation ON mutation.request_id = request.id
                WHERE request.status = ANY(%s)
                GROUP BY request.id
                ORDER BY request.created_at DESC, request.id DESC
                LIMIT %s
                """,
                (list(statuses), int(limit)),
            )
        return self._query_dicts(
            """
            SELECT request.*,
                   count(mutation.id)::bigint AS mutation_count
            FROM upstream_mutation_requests AS request
            LEFT JOIN upstream_mutations AS mutation ON mutation.request_id = request.id
            GROUP BY request.id
            ORDER BY request.created_at DESC, request.id DESC
            LIMIT %s
            """,
            (int(limit),),
        )

    def list_upstream_mutations_for_request(self, request_id: str) -> list[dict[str, Any]]:
        self.ensure_upstream_mutation_tables()
        return self._query_dicts(
            """
            SELECT *
            FROM upstream_mutations
            WHERE request_id = %s
            ORDER BY request_index ASC, created_at ASC, id ASC
            """,
            (request_id,),
        )

    def list_upstream_mutation_request_events(self, request_id: str) -> list[dict[str, Any]]:
        return self._query_dicts(
            """
            SELECT *
            FROM upstream_mutation_request_events
            WHERE request_id = %s
            ORDER BY event_index ASC
            """,
            (request_id,),
        )

    def list_upstream_mutations(
        self,
        *,
        statuses: Sequence[str] | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        self.ensure_upstream_mutation_tables()
        if statuses:
            return self._query_dicts(
                """
                SELECT *
                FROM upstream_mutations
                WHERE status = ANY(%s)
                ORDER BY created_at DESC, id DESC
                LIMIT %s
                """,
                (list(statuses), int(limit)),
            )
        return self._query_dicts(
            """
            SELECT *
            FROM upstream_mutations
            ORDER BY created_at DESC, id DESC
            LIMIT %s
            """,
            (int(limit),),
        )

    def list_upstream_mutation_events(self, mutation_id: str) -> list[dict[str, Any]]:
        return self._query_dicts(
            """
            SELECT *
            FROM upstream_mutation_events
            WHERE mutation_id = %s
            ORDER BY event_index ASC
            """,
            (mutation_id,),
        )

    def remove_upstream_mutation_from_request(
        self,
        *,
        request_id: str,
        mutation_id: str,
        actor_id: str = "reviewer",
    ) -> dict[str, Any]:
        request = self.get_upstream_mutation_request(request_id)
        if request is None:
            raise ValueError(f"unknown mutation request: {request_id}")
        if request["status"] != "pending_review":
            raise ValueError(f"cannot edit request with status {request['status']}")
        mutation = self.get_upstream_mutation(mutation_id)
        if mutation is None or mutation["request_id"] != request_id:
            raise ValueError(f"unknown mutation for request {request_id}: {mutation_id}")
        if mutation["status"] != "pending_review":
            raise ValueError(f"cannot remove mutation with status {mutation['status']}")
        remaining = [
            child
            for child in self.list_upstream_mutations_for_request(request_id)
            if child["id"] != mutation_id and child["status"] == "pending_review"
        ]
        if not remaining:
            raise ValueError("cannot remove every pending mutation from a request")

        now = datetime.now(tz=UTC)
        self._command(
            """
            UPDATE upstream_mutations
               SET status = 'rejected',
                   error = 'removed during review',
                   updated_at = %s
             WHERE id = %s
            """,
            (now, mutation_id),
        )
        self._command(
            """
            UPDATE upstream_mutation_requests
               SET revision = revision + 1,
                   updated_at = %s
             WHERE id = %s
            """,
            (now, request_id),
        )
        self._append_upstream_mutation_event(
            mutation_id,
            event_type="removed",
            actor_type="human",
            actor_id=actor_id,
            event_json={"request_id": request_id},
        )
        self._append_upstream_mutation_request_event(
            request_id,
            event_type="mutation_removed",
            actor_type="human",
            actor_id=actor_id,
            event_json={"mutation_id": mutation_id},
        )
        updated = self.get_upstream_mutation_request(request_id)
        if updated is None:
            raise RuntimeError(f"edited mutation request {request_id} could not be loaded")
        return updated

    def approve_upstream_mutation_request(self, request_id: str, *, actor_id: str = "reviewer") -> dict[str, Any]:
        request = self.get_upstream_mutation_request(request_id)
        if request is None:
            raise ValueError(f"unknown mutation request: {request_id}")
        if request["status"] != "pending_review":
            raise ValueError(f"cannot approve request with status {request['status']}")
        pending = [mutation for mutation in request["mutations"] if mutation["status"] == "pending_review"]
        if not pending:
            raise ValueError("cannot approve a request without pending mutations")
        for mutation in pending:
            self._validate_upstream_mutation_approvable(mutation)

        now = datetime.now(tz=UTC)
        self._command(
            """
            UPDATE upstream_mutation_requests
               SET status = 'approved',
                   approved_by = %s,
                   approved_at = %s,
                   updated_at = %s
             WHERE id = %s
            """,
            (actor_id, now, now, request_id),
        )
        self._command(
            """
            UPDATE upstream_mutations
               SET status = 'approved',
                   approved_by = %s,
                   approved_at = %s,
                   updated_at = %s
             WHERE request_id = %s
               AND status = 'pending_review'
            """,
            (actor_id, now, now, request_id),
        )
        for mutation in pending:
            self._append_upstream_mutation_event(
                str(mutation["id"]),
                event_type="approved",
                actor_type="human",
                actor_id=actor_id,
                event_json={"request_id": request_id},
            )
        self._append_upstream_mutation_request_event(
            request_id,
            event_type="approved",
            actor_type="human",
            actor_id=actor_id,
            event_json={"approved_mutation_ids": [str(mutation["id"]) for mutation in pending]},
        )
        updated = self.get_upstream_mutation_request(request_id)
        if updated is None:
            raise RuntimeError(f"approved mutation request {request_id} could not be loaded")
        return updated

    def reject_upstream_mutation_request(
        self,
        request_id: str,
        *,
        actor_id: str = "reviewer",
        reason: str = "",
    ) -> dict[str, Any]:
        request = self.get_upstream_mutation_request(request_id)
        if request is None:
            raise ValueError(f"unknown mutation request: {request_id}")
        if request["status"] != "pending_review":
            raise ValueError(f"cannot reject request with status {request['status']}")
        now = datetime.now(tz=UTC)
        self._command(
            """
            UPDATE upstream_mutation_requests
               SET status = 'rejected',
                   error = %s,
                   updated_at = %s
             WHERE id = %s
            """,
            (reason, now, request_id),
        )
        self._command(
            """
            UPDATE upstream_mutations
               SET status = 'rejected',
                   error = %s,
                   updated_at = %s
             WHERE request_id = %s
               AND status = 'pending_review'
            """,
            (reason, now, request_id),
        )
        pending = [mutation for mutation in request["mutations"] if mutation["status"] == "pending_review"]
        for mutation in pending:
            self._append_upstream_mutation_event(
                str(mutation["id"]),
                event_type="rejected",
                actor_type="human",
                actor_id=actor_id,
                event_json={"request_id": request_id, "reason": reason},
            )
        self._append_upstream_mutation_request_event(
            request_id,
            event_type="rejected",
            actor_type="human",
            actor_id=actor_id,
            event_json={"reason": reason},
        )
        updated = self.get_upstream_mutation_request(request_id)
        if updated is None:
            raise RuntimeError(f"rejected mutation request {request_id} could not be loaded")
        return updated

    def _validate_upstream_mutation_approvable(self, mutation: Mapping[str, Any]) -> None:
        payload = _as_json_dict(mutation["payload_json"])
        if (
            mutation["provider"] == "gmail"
            and mutation["operation"] in {GMAIL_ARCHIVE_OPERATION, GMAIL_UNARCHIVE_OPERATION}
            and not _normalize_thread_ids(payload.get("thread_ids") or [])
        ):
            raise ValueError("cannot approve a Gmail thread mutation without thread IDs")
        if mutation["provider"] == "gmail" and mutation["operation"] == GMAIL_SEND_EMAIL_OPERATION:
            message = _json_mapping(payload.get("message"))
            _gmail_email_delivery_mode(payload.get("delivery_mode"))
            if not any(_normalize_email_recipients(message.get(field)) for field in ("to", "cc", "bcc")):
                raise ValueError("cannot approve a Gmail email mutation without recipients")
            if not str(message.get("subject") or "").strip():
                raise ValueError("cannot approve a Gmail email mutation without a subject")
            if not str(message.get("body_text") or "").strip() and not str(message.get("body_html") or "").strip():
                raise ValueError("cannot approve a Gmail email mutation without a body")
        if (
            mutation["provider"] == "google_people"
            and mutation["operation"] == GOOGLE_CONTACTS_BATCH_MUTATION_OPERATION
            and not _json_list(payload.get("operations"))
        ):
            raise ValueError("cannot approve a contact mutation without operations")

    def remove_threads_from_gmail_archive_mutation(
        self,
        *,
        mutation_id: str,
        thread_ids: Sequence[str],
        actor_id: str = "reviewer",
    ) -> dict[str, Any]:
        mutation = self.get_upstream_mutation(mutation_id)
        if mutation is None:
            raise ValueError(f"unknown mutation: {mutation_id}")
        if mutation["status"] != "pending_review":
            raise ValueError(f"cannot edit mutation with status {mutation['status']}")
        payload = _as_json_dict(mutation["payload_json"])
        current_thread_ids = _normalize_thread_ids(payload.get("thread_ids") or [])
        remove_thread_ids = set(_normalize_thread_ids(thread_ids))
        remaining_thread_ids = [thread_id for thread_id in current_thread_ids if thread_id not in remove_thread_ids]
        if len(remaining_thread_ids) == len(current_thread_ids):
            raise ValueError("no matching thread IDs were removed")
        if not remaining_thread_ids:
            raise ValueError("cannot remove every thread from a pending archive mutation")
        payload["thread_ids"] = remaining_thread_ids
        preview = _as_json_dict(mutation["preview_json"])
        preview["thread_count"] = len(remaining_thread_ids)
        if isinstance(preview.get("threads"), list):
            preview["threads"] = [
                item
                for item in preview["threads"]
                if isinstance(item, dict) and str(item.get("thread_id") or "") in set(remaining_thread_ids)
            ]
        now = datetime.now(tz=UTC)
        self._command(
            """
            UPDATE upstream_mutations
               SET payload_json = %s,
                   preview_json = %s,
                   revision = revision + 1,
                   updated_at = %s
             WHERE id = %s
            """,
            (_jsonb_param(payload), _jsonb_param(preview), now, mutation_id),
        )
        self._append_upstream_mutation_event(
            mutation_id,
            event_type="edited",
            actor_type="human",
            actor_id=actor_id,
            event_json={"removed_thread_ids": sorted(remove_thread_ids), "remaining_thread_ids": remaining_thread_ids},
        )
        updated = self.get_upstream_mutation(mutation_id)
        if updated is None:
            raise RuntimeError(f"edited mutation {mutation_id} could not be loaded")
        return updated

    def remove_operations_from_contact_mutation(
        self,
        *,
        mutation_id: str,
        operation_indexes: Sequence[int],
        actor_id: str = "reviewer",
    ) -> dict[str, Any]:
        mutation = self.get_upstream_mutation(mutation_id)
        if mutation is None:
            raise ValueError(f"unknown mutation: {mutation_id}")
        if mutation["status"] != "pending_review":
            raise ValueError(f"cannot edit mutation with status {mutation['status']}")
        if mutation["provider"] != "google_people" or mutation["operation"] != GOOGLE_CONTACTS_BATCH_MUTATION_OPERATION:
            raise ValueError("mutation is not a Google contacts batch mutation")
        payload = _as_json_dict(mutation["payload_json"])
        operations = _json_list(payload.get("operations"))
        remove_indexes = sorted({int(index) for index in operation_indexes if int(index) >= 0})
        if not remove_indexes:
            raise ValueError("operation_indexes must include at least one operation index")
        existing_indexes = set(range(len(operations)))
        missing_indexes = [index for index in remove_indexes if index not in existing_indexes]
        if missing_indexes:
            raise ValueError(f"unknown operation indexes: {', '.join(str(index) for index in missing_indexes)}")
        remaining_operations = [operation for index, operation in enumerate(operations) if index not in set(remove_indexes)]
        if not remaining_operations:
            raise ValueError("cannot remove every operation from a pending contact mutation")

        preview = _as_json_dict(mutation["preview_json"])
        preview_operations = _json_list(preview.get("operations"))
        removed_operations = [
            operation for operation in preview_operations if int(operation.get("op_index", -1)) in set(remove_indexes)
        ]
        remaining_preview_operations = [
            {**operation, "op_index": new_index}
            for new_index, operation in enumerate(
                operation for operation in preview_operations if int(operation.get("op_index", -1)) not in set(remove_indexes)
            )
        ]
        preview["operation_count"] = len(remaining_operations)
        preview["operations"] = remaining_preview_operations
        payload["operations"] = remaining_operations
        now = datetime.now(tz=UTC)
        self._command(
            """
            UPDATE upstream_mutations
               SET payload_json = %s,
                   preview_json = %s,
                   revision = revision + 1,
                   updated_at = %s
             WHERE id = %s
            """,
            (_jsonb_param(payload), _jsonb_param(preview), now, mutation_id),
        )
        self._append_upstream_mutation_event(
            mutation_id,
            event_type="edited",
            actor_type="human",
            actor_id=actor_id,
            event_json={
                "removed_operation_indexes": remove_indexes,
                "removed_operations": removed_operations,
                "remaining_operation_count": len(remaining_operations),
            },
        )
        updated = self.get_upstream_mutation(mutation_id)
        if updated is None:
            raise RuntimeError(f"edited mutation {mutation_id} could not be loaded")
        return updated

    def update_gmail_email_mutation(
        self,
        *,
        mutation_id: str,
        message: Mapping[str, Any],
        delivery_mode: str,
        actor_id: str = "reviewer",
    ) -> dict[str, Any]:
        mutation = self.get_upstream_mutation(mutation_id)
        if mutation is None:
            raise ValueError(f"unknown mutation: {mutation_id}")
        if mutation["status"] != "pending_review":
            raise ValueError(f"cannot edit mutation with status {mutation['status']}")
        if mutation["provider"] != "gmail" or mutation["operation"] != GMAIL_SEND_EMAIL_OPERATION:
            raise ValueError("mutation is not a Gmail email mutation")
        payload = _as_json_dict(mutation["payload_json"])
        existing_message = _json_mapping(payload.get("message"))
        merged_message = {**existing_message, **dict(message)}
        preview = _as_json_dict(mutation["preview_json"])
        normalized_payload, normalized_preview = self._normalize_gmail_email_payload(
            account=str(mutation["account"]),
            delivery_mode=delivery_mode,
            message=merged_message,
            request_context=_json_mapping(preview.get("context")),
        )
        now = datetime.now(tz=UTC)
        self._command(
            """
            UPDATE upstream_mutations
               SET payload_json = %s,
                   preview_json = %s,
                   revision = revision + 1,
                   updated_at = %s
             WHERE id = %s
            """,
            (_jsonb_param(normalized_payload), _jsonb_param(normalized_preview), now, mutation_id),
        )
        self._append_upstream_mutation_event(
            mutation_id,
            event_type="edited",
            actor_type="human",
            actor_id=actor_id,
            event_json={
                "delivery_mode": normalized_payload["delivery_mode"],
                "message": normalized_payload["message"],
            },
        )
        updated = self.get_upstream_mutation(mutation_id)
        if updated is None:
            raise RuntimeError(f"edited mutation {mutation_id} could not be loaded")
        return updated

    def approve_upstream_mutation(self, mutation_id: str, *, actor_id: str = "reviewer") -> dict[str, Any]:
        mutation = self.get_upstream_mutation(mutation_id)
        if mutation is None:
            raise ValueError(f"unknown mutation: {mutation_id}")
        if mutation["status"] != "pending_review":
            raise ValueError(f"cannot approve mutation with status {mutation['status']}")
        self._validate_upstream_mutation_approvable(mutation)
        now = datetime.now(tz=UTC)
        self._command(
            """
            UPDATE upstream_mutations
               SET status = 'approved',
                   approved_by = %s,
                   approved_at = %s,
                   updated_at = %s
             WHERE id = %s
            """,
            (actor_id, now, now, mutation_id),
        )
        self._append_upstream_mutation_event(
            mutation_id,
            event_type="approved",
            actor_type="human",
            actor_id=actor_id,
            event_json={},
        )
        if mutation.get("request_id"):
            self._refresh_upstream_mutation_request_status(str(mutation["request_id"]))
        updated = self.get_upstream_mutation(mutation_id)
        if updated is None:
            raise RuntimeError(f"approved mutation {mutation_id} could not be loaded")
        return updated

    def reject_upstream_mutation(
        self,
        mutation_id: str,
        *,
        actor_id: str = "reviewer",
        reason: str = "",
    ) -> dict[str, Any]:
        mutation = self.get_upstream_mutation(mutation_id)
        if mutation is None:
            raise ValueError(f"unknown mutation: {mutation_id}")
        if mutation["status"] != "pending_review":
            raise ValueError(f"cannot reject mutation with status {mutation['status']}")
        now = datetime.now(tz=UTC)
        self._command(
            """
            UPDATE upstream_mutations
               SET status = 'rejected',
                   error = %s,
                   updated_at = %s
             WHERE id = %s
            """,
            (reason, now, mutation_id),
        )
        self._append_upstream_mutation_event(
            mutation_id,
            event_type="rejected",
            actor_type="human",
            actor_id=actor_id,
            event_json={"reason": reason},
        )
        if mutation.get("request_id"):
            self._refresh_upstream_mutation_request_status(str(mutation["request_id"]))
        updated = self.get_upstream_mutation(mutation_id)
        if updated is None:
            raise RuntimeError(f"rejected mutation {mutation_id} could not be loaded")
        return updated

    def claim_approved_upstream_mutations(self, *, limit: int, claimed_by: str) -> list[dict[str, Any]]:
        self.ensure_upstream_mutation_tables()
        if limit <= 0:
            return []
        now = datetime.now(tz=UTC)
        rows = self._query_dicts(
            """
            WITH candidates AS (
                SELECT id
                FROM upstream_mutations
                WHERE status = ANY(%s)
                ORDER BY approved_at ASC, created_at ASC, id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT %s
            )
            UPDATE upstream_mutations AS mutation
               SET status = 'executing',
                   claimed_by = %s,
                   claimed_at = %s,
                   updated_at = %s,
                   attempt_count = attempt_count + 1
              FROM candidates
             WHERE mutation.id = candidates.id
            RETURNING mutation.*
            """,
            (list(UPSTREAM_MUTATION_CLAIMABLE_STATUSES), int(limit), claimed_by, now, now),
        )
        for row in rows:
            self._append_upstream_mutation_event(
                str(row["id"]),
                event_type="claimed",
                actor_type="dagster",
                actor_id=claimed_by,
                event_json={"attempt_count": int(row["attempt_count"])},
            )
        for request_id in sorted({str(row.get("request_id") or "") for row in rows if row.get("request_id")}):
            self._refresh_upstream_mutation_request_status(request_id)
        return rows

    def reclaim_stale_executing_mutations(
        self,
        *,
        stale_after: timedelta,
        idempotent_operations: Sequence[tuple[str, str]],
        actor_id: str,
    ) -> int:
        # Only safe to call while holding the upstream-mutation worker advisory lock. The reset
        # reuses approved_at ordering so reclaimed rows go to the head of the queue, but it does
        # not protect against a concurrent worker that still believes it owns the claim.
        self.ensure_upstream_mutation_tables()
        if not idempotent_operations:
            return 0
        now = datetime.now(tz=UTC)
        cutoff = now - stale_after
        providers = [provider for provider, _ in idempotent_operations]
        operations = [operation for _, operation in idempotent_operations]
        rows = self._query_dicts(
            """
            WITH candidates AS (
                SELECT id, request_id, claimed_by, attempt_count
                FROM upstream_mutations
                WHERE status = 'executing'
                  AND claimed_at < %s
                  AND (provider, operation) IN (
                      SELECT * FROM UNNEST(%s::text[], %s::text[])
                  )
                FOR UPDATE SKIP LOCKED
            )
            UPDATE upstream_mutations AS mutation
               SET status = 'approved',
                   claimed_by = '',
                   claimed_at = '1970-01-01 00:00:00+00'::timestamptz,
                   updated_at = %s
              FROM candidates
             WHERE mutation.id = candidates.id
            RETURNING
                mutation.id,
                mutation.request_id,
                candidates.claimed_by AS previous_claimed_by,
                candidates.attempt_count
            """,
            (cutoff, providers, operations, now),
        )
        for row in rows:
            self._append_upstream_mutation_event(
                str(row["id"]),
                event_type="reclaimed",
                actor_type="dagster",
                actor_id=actor_id,
                event_json={
                    "previous_claimed_by": str(row.get("previous_claimed_by") or ""),
                    "attempt_count": int(row.get("attempt_count") or 0),
                    "stale_after_seconds": int(stale_after.total_seconds()),
                },
            )
        for request_id in sorted({str(row.get("request_id") or "") for row in rows if row.get("request_id")}):
            self._refresh_upstream_mutation_request_status(request_id)
        return len(rows)

    def stale_reclaimable_upstream_mutation_count(
        self,
        *,
        stale_after: timedelta,
        idempotent_operations: Sequence[tuple[str, str]],
        ensure_tables: bool = True,
    ) -> int:
        if ensure_tables:
            self.ensure_upstream_mutation_tables()
        if not idempotent_operations:
            return 0
        cutoff = datetime.now(tz=UTC) - stale_after
        providers = [provider for provider, _ in idempotent_operations]
        operations = [operation for _, operation in idempotent_operations]
        rows = self._query(
            """
            SELECT count(*)::bigint
            FROM upstream_mutations
            WHERE status = 'executing'
              AND claimed_at < %s
              AND (provider, operation) IN (
                  SELECT * FROM UNNEST(%s::text[], %s::text[])
              )
            """,
            (cutoff, providers, operations),
        )
        return int(rows[0][0]) if rows else 0

    def complete_upstream_mutation(self, mutation_id: str, *, result_json: dict[str, Any], actor_id: str) -> None:
        now = datetime.now(tz=UTC)
        self._command(
            """
            UPDATE upstream_mutations
               SET status = 'succeeded',
                   result_json = %s,
                   error = '',
                   executed_at = %s,
                   updated_at = %s
             WHERE id = %s
            """,
            (_jsonb_param(result_json), now, now, mutation_id),
        )
        self._append_upstream_mutation_event(
            mutation_id,
            event_type="executed",
            actor_type="dagster",
            actor_id=actor_id,
            event_json=result_json,
        )
        mutation = self.get_upstream_mutation(mutation_id)
        if mutation and mutation.get("request_id"):
            self._refresh_upstream_mutation_request_status(str(mutation["request_id"]))

    def complete_upstream_mutations(
        self,
        *,
        completions: Sequence[tuple[str, Mapping[str, Any]]],
        actor_id: str,
    ) -> int:
        self.ensure_upstream_mutation_tables()
        if not completions:
            return 0
        now = datetime.now(tz=UTC)
        completion_rows = [
            {"id": mutation_id, "result_json": dict(result_json)}
            for mutation_id, result_json in completions
        ]
        rows = self._query_dicts(
            """
            WITH completion_data AS (
                SELECT id, result_json
                FROM jsonb_to_recordset(%s::jsonb) AS row(id text, result_json jsonb)
            ),
            updated AS (
                UPDATE upstream_mutations AS mutation
                   SET status = 'succeeded',
                       result_json = completion_data.result_json,
                       error = '',
                       executed_at = %s,
                       updated_at = %s
                  FROM completion_data
                 WHERE mutation.id = completion_data.id
                RETURNING mutation.id, mutation.request_id, completion_data.result_json
            )
            SELECT id, request_id, result_json
            FROM updated
            """,
            (_jsonb_param(completion_rows), now, now),
        )
        if not rows:
            return 0
        event_rows = [
            {"mutation_id": str(row["id"]), "event_json": _as_json_dict(row["result_json"])}
            for row in rows
        ]
        self._command(
            """
            WITH event_data AS (
                SELECT mutation_id, event_json
                FROM jsonb_to_recordset(%s::jsonb) AS row(mutation_id text, event_json jsonb)
            ),
            next_indexes AS (
                SELECT
                    event_data.mutation_id,
                    COALESCE(max(event.event_index) + 1, 0) AS event_index
                FROM event_data
                LEFT JOIN upstream_mutation_events AS event
                  ON event.mutation_id = event_data.mutation_id
                GROUP BY event_data.mutation_id
            )
            INSERT INTO upstream_mutation_events (
                mutation_id, event_index, event_type, actor_type, actor_id, event_json, created_at
            )
            SELECT
                event_data.mutation_id,
                next_indexes.event_index,
                'executed',
                'dagster',
                %s,
                event_data.event_json,
                %s
            FROM event_data
            JOIN next_indexes ON next_indexes.mutation_id = event_data.mutation_id
            """,
            (_jsonb_param(event_rows), actor_id, now),
        )
        for request_id in sorted({str(row.get("request_id") or "") for row in rows if row.get("request_id")}):
            self._refresh_upstream_mutation_request_status(request_id)
        return len(rows)

    def fail_upstream_mutation(
        self,
        mutation_id: str,
        *,
        status: str,
        error: str,
        result_json: dict[str, Any] | None = None,
        actor_id: str,
    ) -> None:
        if status not in {"failed_retryable", "failed_terminal", "blocked_missing_credentials"}:
            raise ValueError(f"unsupported failure status: {status}")
        now = datetime.now(tz=UTC)
        self._command(
            """
            UPDATE upstream_mutations
               SET status = %s,
                   error = %s,
                   result_json = %s,
                   updated_at = %s
             WHERE id = %s
            """,
            (status, error, _jsonb_param(result_json or {}), now, mutation_id),
        )
        self._append_upstream_mutation_event(
            mutation_id,
            event_type="failed",
            actor_type="dagster",
            actor_id=actor_id,
            event_json={"status": status, "error": error, "result": result_json or {}},
        )
        mutation = self.get_upstream_mutation(mutation_id)
        if mutation and mutation.get("request_id"):
            self._refresh_upstream_mutation_request_status(str(mutation["request_id"]))

    def approved_upstream_mutation_count(self, *, ensure_tables: bool = True) -> int:
        if ensure_tables:
            self.ensure_upstream_mutation_tables()
        rows = self._query(
            """
            SELECT count(*)::bigint
            FROM upstream_mutations
            WHERE status = ANY(%s)
            """,
            (list(UPSTREAM_MUTATION_CLAIMABLE_STATUSES),),
        )
        return int(rows[0][0]) if rows else 0

    def gmail_message_ids_for_thread_label_mutation(
        self,
        *,
        account: str,
        thread_ids: Sequence[str],
        archive: bool,
    ) -> dict[str, list[str]]:
        normalized_thread_ids = _normalize_thread_ids(thread_ids)
        if not normalized_thread_ids:
            return {}
        inbox_filter = "AND 'INBOX' = ANY(label_ids)" if archive else ""
        rows = self._query(
            f"""
            SELECT thread_id, message_id
            FROM gmail_messages
            WHERE account = %s
              AND thread_id = ANY(%s)
              AND is_deleted = 0
              AND NOT ('TRASH' = ANY(label_ids))
              AND NOT ('SPAM' = ANY(label_ids))
              {inbox_filter}
            ORDER BY thread_id ASC, internal_date ASC, message_id ASC
            """,
            (account, list(normalized_thread_ids)),
        )
        ids_by_thread_id = {thread_id: [] for thread_id in normalized_thread_ids}
        for thread_id, message_id in rows:
            normalized_thread_id = str(thread_id)
            if normalized_thread_id in ids_by_thread_id:
                ids_by_thread_id[normalized_thread_id].append(str(message_id))
        return ids_by_thread_id

    def observe_succeeded_gmail_archive_mutations(self, *, limit: int = 100) -> int:
        self.ensure_upstream_mutation_tables()
        mutations = self._query_dicts(
            """
            SELECT *
            FROM upstream_mutations
            WHERE provider = 'gmail'
              AND operation = 'gmail.archive_threads'
              AND status = 'succeeded'
            ORDER BY executed_at ASC, id ASC
            LIMIT %s
            """,
            (int(limit),),
        )
        observed = 0
        for mutation in mutations:
            payload = _as_json_dict(mutation["payload_json"])
            thread_ids = _normalize_thread_ids(payload.get("thread_ids") or [])
            if not thread_ids:
                continue
            live_rows = self._query(
                """
                SELECT thread_id
                FROM gmail_messages
                WHERE account = %s
                  AND thread_id = ANY(%s)
                  AND is_deleted = 0
                  AND 'INBOX' = ANY(label_ids)
                  AND NOT ('TRASH' = ANY(label_ids))
                  AND NOT ('SPAM' = ANY(label_ids))
                LIMIT 1
                """,
                (mutation["account"], list(thread_ids)),
            )
            if live_rows:
                continue
            now = datetime.now(tz=UTC)
            self._command(
                """
                UPDATE upstream_mutations
                   SET status = 'observed',
                       observed_at = %s,
                       updated_at = %s
                 WHERE id = %s
                   AND status = 'succeeded'
                """,
                (now, now, mutation["id"]),
            )
            self._append_upstream_mutation_event(
                str(mutation["id"]),
                event_type="observed",
                actor_type="dagster",
                actor_id="upstream_mutation_worker",
                event_json={"thread_ids": thread_ids},
            )
            if mutation.get("request_id"):
                self._refresh_upstream_mutation_request_status(str(mutation["request_id"]))
            observed += 1
        return observed

    def observe_succeeded_gmail_unarchive_mutations(self, *, limit: int = 100) -> int:
        self.ensure_upstream_mutation_tables()
        mutations = self._query_dicts(
            """
            SELECT *
            FROM upstream_mutations
            WHERE provider = 'gmail'
              AND operation = 'gmail.unarchive_threads'
              AND status = 'succeeded'
            ORDER BY executed_at ASC, id ASC
            LIMIT %s
            """,
            (int(limit),),
        )
        observed = 0
        for mutation in mutations:
            payload = _as_json_dict(mutation["payload_json"])
            thread_ids = _normalize_thread_ids(payload.get("thread_ids") or [])
            if not thread_ids:
                continue
            inbox_rows = self._query(
                """
                SELECT DISTINCT thread_id
                FROM gmail_messages
                WHERE account = %s
                  AND thread_id = ANY(%s)
                  AND is_deleted = 0
                  AND 'INBOX' = ANY(label_ids)
                  AND NOT ('TRASH' = ANY(label_ids))
                  AND NOT ('SPAM' = ANY(label_ids))
                """,
                (mutation["account"], list(thread_ids)),
            )
            observed_thread_ids = {str(row[0]) for row in inbox_rows}
            if any(thread_id not in observed_thread_ids for thread_id in thread_ids):
                continue
            now = datetime.now(tz=UTC)
            self._command(
                """
                UPDATE upstream_mutations
                   SET status = 'observed',
                       observed_at = %s,
                       updated_at = %s
                 WHERE id = %s
                   AND status = 'succeeded'
                """,
                (now, now, mutation["id"]),
            )
            self._append_upstream_mutation_event(
                str(mutation["id"]),
                event_type="observed",
                actor_type="dagster",
                actor_id="upstream_mutation_worker",
                event_json={"thread_ids": thread_ids},
            )
            if mutation.get("request_id"):
                self._refresh_upstream_mutation_request_status(str(mutation["request_id"]))
            observed += 1
        return observed

    def observe_succeeded_gmail_email_mutations(self, *, limit: int = 100) -> int:
        self.ensure_upstream_mutation_tables()
        mutations = self._query_dicts(
            """
            SELECT *
            FROM upstream_mutations
            WHERE provider = 'gmail'
              AND operation = %s
              AND status = 'succeeded'
            ORDER BY executed_at ASC, id ASC
            LIMIT %s
            """,
            (GMAIL_SEND_EMAIL_OPERATION, int(limit)),
        )
        observed = 0
        for mutation in mutations:
            result = _as_json_dict(mutation["result_json"])
            message_ids = [
                value
                for value in [
                    str(result.get("sent_message_id") or "").strip(),
                    str(result.get("draft_message_id") or "").strip(),
                ]
                if value
            ]
            if not message_ids:
                continue
            rows = self._query(
                """
                SELECT message_id
                FROM gmail_messages
                WHERE account = %s
                  AND message_id = ANY(%s)
                  AND is_deleted = 0
                """,
                (mutation["account"], message_ids),
            )
            observed_message_ids = {str(row[0]) for row in rows}
            if any(message_id not in observed_message_ids for message_id in message_ids):
                continue
            now = datetime.now(tz=UTC)
            self._command(
                """
                UPDATE upstream_mutations
                   SET status = 'observed',
                       observed_at = %s,
                       updated_at = %s
                 WHERE id = %s
                   AND status = 'succeeded'
                """,
                (now, now, mutation["id"]),
            )
            self._append_upstream_mutation_event(
                str(mutation["id"]),
                event_type="observed",
                actor_type="dagster",
                actor_id="upstream_mutation_worker",
                event_json={"message_ids": message_ids, "delivery_mode": str(result.get("delivery_mode") or "")},
            )
            if mutation.get("request_id"):
                self._refresh_upstream_mutation_request_status(str(mutation["request_id"]))
            observed += 1
        return observed

    def observe_succeeded_contact_mutations(self, *, limit: int = 100) -> int:
        self.ensure_contacts_tables()
        self.ensure_upstream_mutation_tables()
        mutations = self._query_dicts(
            """
            SELECT *
            FROM upstream_mutations
            WHERE provider = 'google_people'
              AND operation = %s
              AND status = 'succeeded'
            ORDER BY executed_at ASC, id ASC
            LIMIT %s
            """,
            (GOOGLE_CONTACTS_BATCH_MUTATION_OPERATION, int(limit)),
        )
        observed = 0
        for mutation in mutations:
            payload = _as_json_dict(mutation["payload_json"])
            result = _as_json_dict(mutation["result_json"])
            operations = _json_list(payload.get("operations"))
            if not operations:
                continue
            if not self._contact_mutation_observed(account=str(mutation["account"]), operations=operations, result=result):
                continue
            now = datetime.now(tz=UTC)
            self._command(
                """
                UPDATE upstream_mutations
                   SET status = 'observed',
                       observed_at = %s,
                       updated_at = %s
                 WHERE id = %s
                   AND status = 'succeeded'
                """,
                (now, now, mutation["id"]),
            )
            self._append_upstream_mutation_event(
                str(mutation["id"]),
                event_type="observed",
                actor_type="dagster",
                actor_id="upstream_mutation_worker",
                event_json={"operation_count": len(operations)},
            )
            if mutation.get("request_id"):
                self._refresh_upstream_mutation_request_status(str(mutation["request_id"]))
            observed += 1
        return observed

    def observe_succeeded_calendar_event_mutations(self, *, limit: int = 100) -> int:
        self.ensure_calendar_tables()
        self.ensure_upstream_mutation_tables()
        mutations = self._query_dicts(
            """
            SELECT *
            FROM upstream_mutations
            WHERE provider = %s
              AND operation = ANY(%s)
              AND status = 'succeeded'
            ORDER BY executed_at ASC, id ASC
            LIMIT %s
            """,
            (
                CALENDAR_PROVIDER,
                list(CALENDAR_EVENT_OPERATIONS),
                int(limit),
            ),
        )
        observed = 0
        for mutation in mutations:
            payload = _as_json_dict(mutation["payload_json"])
            result = _as_json_dict(mutation["result_json"])
            calendar_id = str(payload.get("calendar_id") or result.get("calendar_id") or "primary").strip() or "primary"
            event_id = str(result.get("event_id") or payload.get("event_id") or "").strip()
            operation = str(mutation["operation"])
            if not event_id:
                continue
            if not self._calendar_event_mutation_observed(
                account=str(mutation["account"]),
                calendar_id=calendar_id,
                event_id=event_id,
                operation=operation,
                result=result,
            ):
                continue
            now = datetime.now(tz=UTC)
            self._command(
                """
                UPDATE upstream_mutations
                   SET status = 'observed',
                       observed_at = %s,
                       updated_at = %s
                 WHERE id = %s
                   AND status = 'succeeded'
                """,
                (now, now, mutation["id"]),
            )
            self._append_upstream_mutation_event(
                str(mutation["id"]),
                event_type="observed",
                actor_type="dagster",
                actor_id="upstream_mutation_worker",
                event_json={"calendar_id": calendar_id, "event_id": event_id, "operation": operation},
            )
            if mutation.get("request_id"):
                self._refresh_upstream_mutation_request_status(str(mutation["request_id"]))
            observed += 1
        return observed

    def _calendar_event_mutation_observed(
        self,
        *,
        account: str,
        calendar_id: str,
        event_id: str,
        operation: str,
        result: Mapping[str, Any],
    ) -> bool:
        rows = self._query_dicts(
            """
            SELECT is_deleted, raw_json
            FROM calendar_events
            WHERE account = %s
              AND calendar_id = %s
              AND event_id = %s
            LIMIT 1
            """,
            (account, calendar_id, event_id),
        )
        if not rows:
            return False
        row = rows[0]
        is_deleted = int(row.get("is_deleted") or 0) != 0
        if operation == CALENDAR_DELETE_EVENT_OPERATION:
            return is_deleted
        if is_deleted:
            return False
        expected_etag = str(result.get("etag") or "").strip()
        if not expected_etag:
            return True
        live_event = _as_json_dict(row.get("raw_json"))
        return str(live_event.get("etag") or "").strip() == expected_etag

    def _refresh_upstream_mutation_request_status(self, request_id: str) -> None:
        request = self._query_dicts("SELECT * FROM upstream_mutation_requests WHERE id = %s", (request_id,))
        if not request:
            return
        mutations = self.list_upstream_mutations_for_request(request_id)
        if not mutations:
            return
        statuses = [str(mutation["status"]) for mutation in mutations]
        active_statuses = [status for status in statuses if status != "rejected"]
        if not active_statuses:
            status = "rejected"
        elif any(status == "pending_review" for status in active_statuses):
            status = "pending_review"
        elif any(status == "executing" for status in active_statuses):
            status = "executing"
        elif any(status == "approved" for status in active_statuses):
            status = "approved"
        elif any(status == "failed_retryable" for status in active_statuses):
            status = "failed_retryable"
        elif any(status == "blocked_missing_credentials" for status in active_statuses):
            status = "blocked_missing_credentials"
        elif any(status == "failed_terminal" for status in active_statuses):
            status = "failed_terminal"
        elif all(status == "observed" for status in active_statuses):
            status = "observed"
        elif all(status in {"succeeded", "observed"} for status in active_statuses):
            status = "succeeded"
        else:
            status = request[0]["status"]

        now = datetime.now(tz=UTC)
        executed_at = max((mutation["executed_at"] for mutation in mutations), default=request[0]["executed_at"])
        observed_at = max((mutation["observed_at"] for mutation in mutations), default=request[0]["observed_at"])
        result_json = {
            "mutation_statuses": {str(mutation["id"]): str(mutation["status"]) for mutation in mutations},
        }
        self._command(
            """
            UPDATE upstream_mutation_requests
               SET status = %s,
                   result_json = %s,
                   executed_at = CASE WHEN %s > executed_at THEN %s ELSE executed_at END,
                   observed_at = CASE WHEN %s > observed_at THEN %s ELSE observed_at END,
                   updated_at = %s
             WHERE id = %s
            """,
            (
                status,
                _jsonb_param(result_json),
                executed_at,
                executed_at,
                observed_at,
                observed_at,
                now,
                request_id,
            ),
        )

    def _normalize_upstream_mutation_request_mutations(
        self,
        *,
        request_reason: str,
        request_context: Mapping[str, Any],
        mutations: Sequence[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        if not mutations:
            return normalized
        for index, mutation in enumerate(mutations):
            if not isinstance(mutation, Mapping):
                raise ValueError(f"mutation {index} must be an object")
            mutation_type = str(mutation.get("type") or mutation.get("operation") or "").strip()
            if mutation_type in {GMAIL_ARCHIVE_OPERATION, GMAIL_UNARCHIVE_OPERATION}:
                account = str(mutation.get("account") or "").strip().lower()
                thread_ids = _normalize_thread_ids(mutation.get("thread_ids") or [])
                if not account:
                    raise ValueError(f"mutation {index} must include account")
                if not thread_ids:
                    raise ValueError(f"mutation {index} must include thread_ids")
                previews = (
                    self.gmail_archive_thread_previews(account=account, thread_ids=thread_ids)
                    if mutation_type == GMAIL_ARCHIVE_OPERATION
                    else self.gmail_unarchive_thread_previews(account=account, thread_ids=thread_ids)
                )
                previews_by_thread_id = {str(preview["thread_id"]): preview for preview in previews}
                missing_thread_ids = [thread_id for thread_id in thread_ids if thread_id not in previews_by_thread_id]
                if missing_thread_ids:
                    state = "non-inbox" if mutation_type == "gmail.archive_threads" else "non-archived"
                    raise ValueError(f"unknown or {state} Gmail thread IDs for {account}: {', '.join(missing_thread_ids)}")
                for thread_id in thread_ids:
                    preview_row = _json_ready(previews_by_thread_id[thread_id])
                    subject = str(preview_row.get("subject") or thread_id)
                    archive = mutation_type == GMAIL_ARCHIVE_OPERATION
                    normalized.append(
                        {
                            "provider": "gmail",
                            "operation": mutation_type,
                            "account": account,
                            "title": str(mutation.get("title") or f"{'Archive' if archive else 'Unarchive'}: {subject}"),
                            "reason": str(mutation.get("reason") or request_reason),
                            "payload_json": (
                                {"thread_ids": [thread_id], "remove_label_ids": ["INBOX"]}
                                if archive
                                else {"thread_ids": [thread_id], "add_label_ids": ["INBOX"]}
                            ),
                            "preview_json": {
                                "thread_count": 1,
                                "threads": [preview_row],
                                "context": _normalize_json_value(dict(request_context)),
                            },
                        }
                    )
            elif mutation_type == GMAIL_SEND_EMAIL_OPERATION:
                account = str(mutation.get("account") or "").strip().lower()
                if not account:
                    raise ValueError(f"mutation {index} must include account")
                payload, preview = self._normalize_gmail_email_payload(
                    account=account,
                    delivery_mode=str(mutation.get("delivery_mode") or "send"),
                    message=_json_mapping(mutation.get("message")),
                    request_context=request_context,
                )
                normalized.append(
                    {
                        "provider": "gmail",
                        "operation": GMAIL_SEND_EMAIL_OPERATION,
                        "account": account,
                        "title": str(mutation.get("title") or _gmail_email_title(payload["message"])),
                        "reason": str(mutation.get("reason") or request_reason),
                        "payload_json": payload,
                        "preview_json": preview,
                    }
                )
            elif mutation_type in {"google_people.contacts", "contacts.batch_mutation"}:
                account = str(mutation.get("account") or "").strip().lower()
                if not account:
                    raise ValueError(f"mutation {index} must include account")
                operations = self._normalize_contact_mutation_operations(
                    account=account,
                    operations=_json_list(mutation.get("operations")),
                )
                if not operations:
                    raise ValueError(f"mutation {index} must include operations")
                for operation in operations:
                    preview_operation = self._contact_mutation_operation_preview(
                        account=account,
                        operation=operation,
                        op_index=0,
                    )
                    normalized.append(
                        {
                            "provider": "google_people",
                            "operation": GOOGLE_CONTACTS_BATCH_MUTATION_OPERATION,
                            "account": account,
                            "title": str(mutation.get("title") or _contact_mutation_title(preview_operation)),
                            "reason": str(mutation.get("reason") or request_reason),
                            "payload_json": {"operations": [operation]},
                            "preview_json": {
                                "operation_count": 1,
                                "operations": [preview_operation],
                                "context": _normalize_json_value(dict(request_context)),
                            },
                        }
                    )
            elif mutation_type in CALENDAR_EVENT_OPERATIONS:
                account = str(mutation.get("account") or "").strip().lower()
                if not account:
                    raise ValueError(f"mutation {index} must include account")
                calendar_id = _calendar_id(mutation.get("calendar_id"))
                send_updates = _calendar_send_updates(mutation.get("send_updates"))
                expected_etag = str(mutation.get("expected_etag") or "").strip()
                if mutation_type == CALENDAR_CREATE_EVENT_OPERATION:
                    event = _json_mapping(mutation.get("event"))
                    if not event:
                        raise ValueError(f"mutation {index} must include event")
                    if "start" not in event:
                        raise ValueError(f"mutation {index} event must include start")
                    if "end" not in event:
                        raise ValueError(f"mutation {index} event must include end")
                    payload = {
                        "calendar_id": calendar_id,
                        "send_updates": send_updates,
                        "event": _normalize_json_value(event),
                    }
                    preview_event = _calendar_event_preview(
                        event=event,
                        operation="create",
                        calendar_id=calendar_id,
                        send_updates=send_updates,
                    )
                    title = str(mutation.get("title") or _calendar_event_title("Create event", event))
                elif mutation_type == CALENDAR_UPDATE_EVENT_OPERATION:
                    event_id = str(mutation.get("event_id") or "").strip()
                    patch = _json_mapping(mutation.get("patch"))
                    if not event_id:
                        raise ValueError(f"mutation {index} must include event_id")
                    if not patch:
                        raise ValueError(f"mutation {index} must include patch")
                    payload = {
                        "calendar_id": calendar_id,
                        "send_updates": send_updates,
                        "event_id": event_id,
                        "expected_etag": expected_etag,
                        "patch": _normalize_json_value(patch),
                    }
                    preview_event = _calendar_event_preview(
                        event=patch,
                        operation="update",
                        calendar_id=calendar_id,
                        send_updates=send_updates,
                        event_id=event_id,
                        expected_etag=expected_etag,
                    )
                    title = str(mutation.get("title") or _calendar_event_title("Update event", patch))
                else:
                    event_id = str(mutation.get("event_id") or "").strip()
                    if not event_id:
                        raise ValueError(f"mutation {index} must include event_id")
                    payload = {
                        "calendar_id": calendar_id,
                        "send_updates": send_updates,
                        "event_id": event_id,
                        "expected_etag": expected_etag,
                    }
                    preview_event = _calendar_event_preview(
                        event={},
                        operation="delete",
                        calendar_id=calendar_id,
                        send_updates=send_updates,
                        event_id=event_id,
                        expected_etag=expected_etag,
                    )
                    title = str(mutation.get("title") or f"Delete event {event_id}")
                normalized.append(
                    {
                        "provider": CALENDAR_PROVIDER,
                        "operation": mutation_type,
                        "account": account,
                        "title": title,
                        "reason": str(mutation.get("reason") or request_reason),
                        "payload_json": payload,
                        "preview_json": {
                            "event": preview_event,
                            "context": _normalize_json_value(dict(request_context)),
                        },
                    }
                )
            else:
                raise ValueError(
                    f"mutation {index} has unsupported type {mutation_type!r}; expected gmail.archive_threads, gmail.unarchive_threads, gmail.send_email, google_people.contacts, calendar.create_event, calendar.update_event, or calendar.delete_event"
                )
        return normalized

    def _normalize_gmail_email_payload(
        self,
        *,
        account: str,
        delivery_mode: str,
        message: Mapping[str, Any],
        request_context: Mapping[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        mode = _gmail_email_delivery_mode(delivery_mode)
        normalized_message = {
            "to": _normalize_email_recipients(message.get("to")),
            "cc": _normalize_email_recipients(message.get("cc")),
            "bcc": _normalize_email_recipients(message.get("bcc")),
            "subject": str(message.get("subject") or "").strip(),
            "body_text": str(message.get("body_text") or ""),
            "body_html": str(message.get("body_html") or ""),
        }
        reply_to_thread_id = str(
            message.get("reply_to_thread_id") or message.get("thread_id") or message.get("replyToThreadId") or ""
        ).strip()
        reply_context: dict[str, Any] = {}
        if reply_to_thread_id:
            reply_context = self._gmail_reply_thread_context(account=account, thread_id=reply_to_thread_id)
            if not reply_context:
                raise ValueError(f"unknown Gmail reply thread ID for {account}: {reply_to_thread_id}")
            normalized_message["reply_to_thread_id"] = reply_to_thread_id
            if not normalized_message["subject"]:
                normalized_message["subject"] = _reply_subject(str(reply_context.get("subject") or ""))
            in_reply_to = str(message.get("in_reply_to") or message.get("inReplyTo") or "").strip()
            if not in_reply_to:
                in_reply_to = str(reply_context.get("rfc822_message_id") or "").strip()
            if in_reply_to:
                normalized_message["in_reply_to"] = in_reply_to
            references = _normalize_email_recipients(message.get("references"))
            if not references and in_reply_to:
                references = [in_reply_to]
            if references:
                normalized_message["references"] = references
        elif not normalized_message["subject"]:
            raise ValueError("Gmail email mutation must include subject")

        if not any(normalized_message[field] for field in ("to", "cc", "bcc")):
            raise ValueError("Gmail email mutation must include at least one recipient")
        if not normalized_message["body_text"].strip() and not normalized_message["body_html"].strip():
            raise ValueError("Gmail email mutation must include body_text or body_html")

        payload = {
            "delivery_mode": mode,
            "message": _normalize_json_value(normalized_message),
        }
        preview = {
            "email": {
                "mode": "reply" if reply_to_thread_id else "new_thread",
                "delivery_mode": mode,
                **_normalize_json_value(normalized_message),
            },
            "context": _normalize_json_value(dict(request_context)),
        }
        if reply_context:
            preview["email"]["reply_context"] = _normalize_json_value(reply_context)
        return payload, preview

    def _gmail_reply_thread_context(self, *, account: str, thread_id: str) -> dict[str, Any]:
        rows = self._query_dicts(
            """
            SELECT
                thread_id,
                subject,
                from_address AS latest_from_address,
                rfc822_message_id,
                internal_date AS latest_at
            FROM gmail_messages
            WHERE account = %s
              AND thread_id = %s
              AND is_deleted = 0
              AND NOT ('TRASH' = ANY(label_ids))
              AND NOT ('SPAM' = ANY(label_ids))
            ORDER BY internal_date DESC, message_id ASC
            LIMIT 1
            """,
            (account, thread_id),
        )
        return _json_ready(rows[0]) if rows else {}

    def _normalize_contact_mutation_operations(
        self,
        *,
        account: str,
        operations: Sequence[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for index, operation in enumerate(operations):
            if not isinstance(operation, Mapping):
                raise ValueError(f"operation {index} must be an object")
            op = str(operation.get("op") or "").strip()
            client_op_id = str(operation.get("client_op_id") or f"op-{index}").strip() or f"op-{index}"
            if op == "create_contact":
                person = _json_mapping(operation.get("person"))
                if not person:
                    raise ValueError(f"operation {index} create_contact must include person")
                if person.get("resourceName"):
                    raise ValueError(f"operation {index} create_contact person must not include resourceName")
                normalized.append({"op": op, "client_op_id": client_op_id, "person": _normalize_json_value(person)})
            elif op == "update_contact":
                resource_name = _contact_resource_name(operation)
                expected_etag = _contact_expected_etag(operation)
                fields = _contact_update_fields(operation.get("update_person_fields") or operation.get("updatePersonFields"))
                person = _json_mapping(operation.get("person"))
                if not person:
                    raise ValueError(f"operation {index} update_contact must include person")
                current = self._contact_card(account=account, resource_name=resource_name)
                if current is None:
                    raise ValueError(f"operation {index} update_contact references unknown contact {resource_name}")
                if expected_etag and str(current["etag"]) != expected_etag:
                    raise ValueError(f"operation {index} update_contact etag does not match current contact {resource_name}")
                person = dict(person)
                person["resourceName"] = resource_name
                if expected_etag and not person.get("etag"):
                    person["etag"] = expected_etag
                normalized.append(
                    {
                        "op": op,
                        "client_op_id": client_op_id,
                        "resource_name": resource_name,
                        "expected_etag": expected_etag,
                        "update_person_fields": fields,
                        "person": _normalize_json_value(person),
                    }
                )
            elif op == "delete_contact":
                resource_name = _contact_resource_name(operation)
                expected_etag = _contact_expected_etag(operation)
                current = self._contact_card(account=account, resource_name=resource_name)
                if current is None:
                    raise ValueError(f"operation {index} delete_contact references unknown contact {resource_name}")
                if expected_etag and str(current["etag"]) != expected_etag:
                    raise ValueError(f"operation {index} delete_contact etag does not match current contact {resource_name}")
                normalized.append(
                    {
                        "op": op,
                        "client_op_id": client_op_id,
                        "resource_name": resource_name,
                        "expected_etag": expected_etag,
                        "reason": str(operation.get("reason") or ""),
                    }
                )
            else:
                raise ValueError(
                    f"operation {index} has unsupported op {op!r}; expected create_contact, update_contact, or delete_contact"
                )
        return normalized

    def _contact_mutation_operation_preview(
        self,
        *,
        account: str,
        operation: Mapping[str, Any],
        op_index: int,
    ) -> dict[str, Any]:
        op = str(operation.get("op") or "")
        preview: dict[str, Any] = {
            "op_index": op_index,
            "op": op,
            "client_op_id": str(operation.get("client_op_id") or ""),
        }
        if op == "create_contact":
            person = _json_mapping(operation.get("person"))
            preview.update(
                {
                    "summary": _contact_person_summary(person),
                    "after": person,
                }
            )
        elif op in {"update_contact", "delete_contact"}:
            resource_name = str(operation.get("resource_name") or "")
            current = self._contact_card(account=account, resource_name=resource_name)
            before = _as_json_dict(current["raw_json"]) if current else {}
            preview.update(
                {
                    "resource_name": resource_name,
                    "expected_etag": str(operation.get("expected_etag") or ""),
                    "summary": _contact_person_summary(before),
                    "before": before,
                }
            )
            if op == "update_contact":
                preview["after"] = _json_mapping(operation.get("person"))
                preview["update_person_fields"] = list(operation.get("update_person_fields") or [])
            else:
                preview["reason"] = str(operation.get("reason") or "")
        return preview

    def _contact_card(self, *, account: str, resource_name: str) -> dict[str, Any] | None:
        rows = self._query_dicts(
            """
            SELECT *
            FROM contact_cards
            WHERE source = 'google_people'
              AND account = %s
              AND source_kind = 'google_contacts'
              AND address_book_id = 'people/me'
              AND card_id = %s
              AND is_deleted = 0
            """,
            (account, resource_name),
        )
        return rows[0] if rows else None

    def _contact_mutation_observed(
        self,
        *,
        account: str,
        operations: Sequence[Mapping[str, Any]],
        result: Mapping[str, Any],
    ) -> bool:
        created_by_client_id = {
            str(item.get("client_op_id") or ""): str(item.get("resource_name") or "")
            for item in _json_list(result.get("operation_results"))
            if item.get("op") == "create_contact"
        }
        for operation in operations:
            op = str(operation.get("op") or "")
            resource_name = str(operation.get("resource_name") or "")
            if op == "create_contact":
                resource_name = created_by_client_id.get(str(operation.get("client_op_id") or ""), "")
                if not resource_name:
                    return False
                if self._contact_card(account=account, resource_name=resource_name) is None:
                    return False
            elif op == "update_contact":
                row = self._contact_card(account=account, resource_name=resource_name)
                if row is None:
                    return False
                expected_result = _operation_result_for_resource(result, resource_name)
                result_etag = str(expected_result.get("etag") or "")
                if result_etag and str(row["etag"]) != result_etag:
                    return False
            elif op == "delete_contact":
                if self._contact_card(account=account, resource_name=resource_name) is not None:
                    return False
            else:
                return False
        return True

    def _append_upstream_mutation_event(
        self,
        mutation_id: str,
        *,
        event_type: str,
        actor_type: str,
        actor_id: str,
        event_json: dict[str, Any],
    ) -> None:
        self._command(
            """
            INSERT INTO upstream_mutation_events (
                mutation_id, event_index, event_type, actor_type, actor_id, event_json, created_at
            )
            SELECT
                %s,
                COALESCE(max(event_index) + 1, 0),
                %s,
                %s,
                %s,
                %s,
                %s
            FROM upstream_mutation_events
            WHERE mutation_id = %s
            """,
            (
                mutation_id,
                event_type,
                actor_type,
                actor_id,
                _jsonb_param(event_json),
                datetime.now(tz=UTC),
                mutation_id,
            ),
        )

    def _append_upstream_mutation_request_event(
        self,
        request_id: str,
        *,
        event_type: str,
        actor_type: str,
        actor_id: str,
        event_json: dict[str, Any],
    ) -> None:
        self._command(
            """
            INSERT INTO upstream_mutation_request_events (
                request_id, event_index, event_type, actor_type, actor_id, event_json, created_at
            )
            SELECT
                %s,
                COALESCE(max(event_index) + 1, 0),
                %s,
                %s,
                %s,
                %s,
                %s
            FROM upstream_mutation_request_events
            WHERE request_id = %s
            """,
            (
                request_id,
                event_type,
                actor_type,
                actor_id,
                _jsonb_param(event_json),
                datetime.now(tz=UTC),
                request_id,
            ),
        )

    def _ensure_table_group(self, tables: Sequence[str]) -> None:
        for table in tables:
            self._ensure_table(table)
        self._ensure_indexes(tables)

    def _ensure_table(self, table: str) -> None:
        spec = POSTGRES_TABLES[table]
        column_sql = [
            f"{_identifier(column)} {_postgres_type(column, table=table)} NOT NULL DEFAULT {_default_sql(column, table=table)}"
            for column in spec.columns
        ]
        primary_key = ", ".join(_identifier(column) for column in spec.primary_key)
        self._command(
            f"""
            CREATE TABLE IF NOT EXISTS {_identifier(table)} (
                {", ".join(column_sql)},
                PRIMARY KEY ({primary_key})
            )
            """
        )
        if spec.storage_parameters:
            settings = ", ".join(f"{key} = {value}" for key, value in spec.storage_parameters)
            self._command(f"ALTER TABLE {_identifier(table)} SET ({settings})")

    def _ensure_indexes(self, tables: Sequence[str]) -> None:
        table_names = set(tables)
        for index in POSTGRES_INDEXES:
            if index.table not in table_names or index.name in self._ensured_index_names:
                continue
            try:
                if self._index_exists(index.name):
                    self._ensured_index_names.add(index.name)
                    continue
                if index.requires_pg_trgm and not self._pg_trgm_ensured:
                    self._command("CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public")
                    self._pg_trgm_ensured = True
                if index.requires_pg_textsearch and not self._pg_textsearch_ensured:
                    # Fails (and is harmlessly skipped, like missing-table indexes)
                    # on hosts whose Postgres lacks the pg_textsearch preload.
                    self._command("CREATE EXTENSION IF NOT EXISTS pg_textsearch WITH SCHEMA public")
                    self._pg_textsearch_ensured = True
                self._command(index.sql)
                self._ensured_index_names.add(index.name)
            except Exception:
                # Tests often create only a subset of tables. Missing-table index failures
                # are harmless because ensure_* is called again by each runtime asset.
                pass
        for obsolete_name, obsolete_table in POSTGRES_OBSOLETE_INDEXES:
            if obsolete_table not in table_names:
                continue
            try:
                if self._index_exists(obsolete_name):
                    self._command(f"DROP INDEX CONCURRENTLY IF EXISTS {_identifier(obsolete_name)}")
            except Exception:
                pass

    def _index_exists(self, index_name: str) -> bool:
        rows = self._query(
            """
            SELECT 1
            FROM pg_class AS c
            INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
            INNER JOIN pg_index AS i ON i.indexrelid = c.oid
            WHERE n.nspname = %s
              AND c.relname = %s
              AND c.relkind = 'i'
              AND i.indisvalid
              AND i.indisready
            LIMIT 1
            """,
            (self._schema, index_name),
        )
        return bool(rows)

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
        include_storage_pending: bool = False,
        storage_max_bytes: int = 0,
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        # Historical state rows carry the ai_provider/model/prompt_version of the
        # removed inline Ollama fallback; any successful backfill counts now that
        # sync-time extraction is deterministic-only.
        ai_pending_clause = """
              NOT EXISTS (
                  SELECT 1
                  FROM gmail_attachment_backfill_state state
                  WHERE state.account = gmail_messages.account
                    AND state.message_id = gmail_messages.message_id
                    AND state.status = 'ok'
              )"""
        params: list[Any] = [account]
        pending_clause = ai_pending_clause
        if include_storage_pending and storage_max_bytes > 0:
            pending_clause = f"""({ai_pending_clause}
              OR EXISTS (
                  SELECT 1
                  FROM gmail_attachments pending
                  WHERE pending.account = gmail_messages.account
                    AND pending.message_id = gmail_messages.message_id
                    AND pending.is_deleted = 0
                    AND pending.size > 0
                    AND pending.size <= %s
                    AND pending.storage_status <> 'stored'
              ))"""
            params.append(int(storage_max_bytes))
        params.append(int(limit))
        rows = self._query(
            f"""
            SELECT payload_json
            FROM gmail_messages
            WHERE account = %s
              AND is_deleted = 0
              AND {_postgres_gmail_attachment_candidate_clause()}
              AND {pending_clause}
            ORDER BY internal_date DESC, message_id DESC
            LIMIT %s
            """,
            tuple(params),
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
            FROM file_attachment_enrichments
            WHERE content_sha256 = ANY(%s)
              AND ai_provider = %s
              AND ai_model = %s
              AND ai_prompt_version = %s
            """,
            (hashes, ai_provider, ai_model, ai_prompt_version),
        )
        return {str(row[0]): dict(zip(columns, row, strict=True)) for row in rows}

    def insert_attachment_enrichments(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("file_attachment_enrichments", rows, ATTACHMENT_ENRICHMENT_COLUMNS)

    def ensure_google_drive_source_tables(self) -> None:
        self._ensure_table_group(
            [
                "google_drive_files",
                "google_drive_file_texts",
                "google_drive_sync_state",
            ]
        )
        self._ensure_search_views_if_possible()

    def insert_google_drive_files(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("google_drive_files", rows, GOOGLE_DRIVE_FILE_COLUMNS)

    def insert_google_drive_file_texts(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("google_drive_file_texts", rows, GOOGLE_DRIVE_FILE_TEXT_COLUMNS)

    def upsert_google_drive_sync_state(self, row: dict[str, Any]) -> None:
        self._insert_rows("google_drive_sync_state", [row], GOOGLE_DRIVE_SYNC_STATE_COLUMNS)

    def load_google_drive_text_state(self, account: str) -> dict[str, tuple[datetime, str]]:
        rows = self._query(
            """
            SELECT file_id, source_modified_time, content_sha256
            FROM google_drive_file_texts
            WHERE account = %s AND text_extraction_status = 'ok'
            """,
            (account,),
        )
        return {str(row[0]): (row[1], str(row[2])) for row in rows}

    def load_google_drive_text_modified_times(self, account: str) -> dict[str, datetime]:
        return {file_id: state[0] for file_id, state in self.load_google_drive_text_state(account).items()}

    def mark_google_drive_files_trashed(
        self, *, account: str, file_ids: Sequence[str], sync_version: int
    ) -> None:
        if not file_ids:
            return
        self._command(
            """
            UPDATE google_drive_files
            SET trashed = 1,
                sync_version = GREATEST(sync_version + 1, %s)
            WHERE account = %s AND file_id = ANY(%s)
            """,
            (sync_version, account, list(file_ids)),
        )

    def load_google_drive_sync_state(self) -> dict[str, GoogleDriveSyncState]:
        rows = self._query(
            """
            SELECT account, start_page_token, last_page_token, drive_id,
                   last_sync_type, status, error, full_crawled_at, files_seen
            FROM google_drive_sync_state
            """
        )
        return {
            str(row[0]): GoogleDriveSyncState(
                account=str(row[0]),
                start_page_token=str(row[1]),
                last_page_token=str(row[2]),
                drive_id=str(row[3]),
                last_sync_type=str(row[4]),
                status=str(row[5]),
                error=str(row[6]),
                full_crawled_at=row[7],
                files_seen=int(row[8]),
            )
            for row in rows
        }

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

    def insert_contact_cards(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("contact_cards", rows, CONTACT_CARD_COLUMNS)

    def load_contact_sync_state(self) -> dict[tuple[str, str, str, str], dict[str, Any]]:
        columns = CONTACT_SYNC_STATE_COLUMNS
        rows = self._query(f"SELECT {', '.join(_identifier(column) for column in columns)} FROM contact_sync_state")
        return {
            (str(row[0]), str(row[1]), str(row[2]), str(row[3])): dict(zip(columns, row, strict=True))
            for row in rows
        }

    def insert_contact_sync_state(
        self,
        *,
        source: str,
        account: str,
        source_kind: str,
        address_book_id: str,
        sync_token: str,
        last_sync_type: str,
        status: str,
        error: str,
        full_synced_at: datetime,
        updated_at: datetime,
    ) -> None:
        self._insert(
            "contact_sync_state",
            [
                (
                    source,
                    account,
                    source_kind,
                    address_book_id,
                    sync_token,
                    last_sync_type,
                    status,
                    error,
                    full_synced_at,
                    updated_at,
                    int(_ensure_utc(updated_at).timestamp() * 1_000_000),
                )
            ],
            CONTACT_SYNC_STATE_COLUMNS,
        )

    def mark_missing_contact_cards_deleted(
        self,
        *,
        source: str,
        account: str,
        source_kind: str,
        address_book_id: str,
        active_card_ids: set[str],
        synced_at: datetime,
    ) -> int:
        params: list[Any] = [source, account, source_kind, address_book_id]
        active_filter = ""
        if active_card_ids:
            active_filter = "AND NOT (card_id = ANY(%s))"
            params.append(sorted(active_card_ids))
        rows = self._query(
            f"""
            SELECT {", ".join(_identifier(column) for column in CONTACT_CARD_COLUMNS)}
            FROM contact_cards
            WHERE source = %s
              AND account = %s
              AND source_kind = %s
              AND address_book_id = %s
              AND is_deleted = 0
              {active_filter}
            """,
            tuple(params),
        )
        tombstones: list[dict[str, Any]] = []
        sync_version = int(_ensure_utc(synced_at).timestamp() * 1_000_000)
        for row in rows:
            tombstone = dict(zip(CONTACT_CARD_COLUMNS, row, strict=True))
            tombstone["is_deleted"] = 1
            tombstone["synced_at"] = synced_at
            tombstone["sync_version"] = sync_version
            tombstones.append(tombstone)
        self.insert_contact_cards(tombstones)
        return len(tombstones)

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

    def insert_whatsapp_chats(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("whatsapp_chats", rows, WHATSAPP_CHAT_COLUMNS)

    def insert_whatsapp_chat_participants(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("whatsapp_chat_participants", rows, WHATSAPP_CHAT_PARTICIPANT_COLUMNS)

    def backfill_whatsapp_chats_from_messages(self) -> int:
        """Ensure every chat_id seen in messages has a whatsapp_chats row.

        History/group sync never emit a chat row for some chat_ids (notably the
        status@broadcast feed), so a message->chat join falls through to NULL and
        a status post is indistinguishable from a DM. This fills only the gaps:
        ON CONFLICT DO NOTHING never touches a real chat row (its name, type,
        etc.). chat_type is derived from the JID to match
        ``events.chat_type_for_jid``. Returns the number of rows inserted.
        """
        rows = self._query(
            """
            INSERT INTO whatsapp_chats (
                account, chat_id, name, chat_type, is_archived,
                last_message_at, raw_metadata_json, ingested_at, sync_version
            )
            SELECT m.account, m.chat_id, '',
                CASE
                    WHEN m.chat_id = 'status@broadcast' THEN 'status'
                    WHEN m.chat_id LIKE '%@s.whatsapp.net' THEN 'user'
                    WHEN m.chat_id LIKE '%@lid' THEN 'user'
                    WHEN m.chat_id LIKE '%@g.us' THEN 'group'
                    WHEN m.chat_id LIKE '%@broadcast' THEN 'broadcast'
                    WHEN m.chat_id LIKE '%@newsletter' THEN 'newsletter'
                    WHEN position('@' in m.chat_id) > 0 THEN split_part(m.chat_id, '@', 2)
                    ELSE 'unknown'
                END,
                0,
                '1970-01-01 00:00:00+00'::timestamptz,
                '{"source":"synthesized_from_message"}',
                now(),
                1
            FROM (SELECT DISTINCT account, chat_id FROM whatsapp_messages) m
            LEFT JOIN whatsapp_chats c ON c.account = m.account AND c.chat_id = m.chat_id
            WHERE c.chat_id IS NULL AND m.chat_id <> ''
            ON CONFLICT (account, chat_id) DO NOTHING
            RETURNING 1
            """
        )
        return len(rows)

    def insert_whatsapp_contacts(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("whatsapp_contacts", rows, WHATSAPP_CONTACT_COLUMNS)

    def insert_whatsapp_messages(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("whatsapp_messages", rows, WHATSAPP_MESSAGE_COLUMNS)

    def insert_whatsapp_media_items(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("whatsapp_media_items", rows, WHATSAPP_MEDIA_ITEM_COLUMNS)

    def insert_agent_session_events(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("agent_session_events", rows, AGENT_SESSION_EVENT_COLUMNS)

    def chatgpt_conversation_sync_map(self, *, account: str) -> dict[str, float]:
        """Return ``{session_id: update_time}`` already synced for ``account``.

        The poller skips any backend conversation whose ``update_time`` is not
        newer than its recorded value (and re-fetches the rest). Re-ingest is
        idempotent, so this only ever bounds wasted work.
        """
        self.ensure_chatgpt_conversation_sync_table()
        rows = self._query_dicts(
            """
            SELECT session_id, update_time
            FROM chatgpt_conversation_sync
            WHERE account = %s
            """,
            (account,),
        )
        return {str(row["session_id"]): float(row["update_time"] or 0.0) for row in rows}

    def record_chatgpt_conversation_synced(
        self,
        *,
        account: str,
        session_id: str,
        update_time: float,
        event_count: int,
        synced_at: datetime | None = None,
    ) -> None:
        self.ensure_chatgpt_conversation_sync_table()
        synced = synced_at or datetime.now(tz=UTC)
        self._command(
            """
            INSERT INTO chatgpt_conversation_sync (account, session_id, update_time, event_count, synced_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (account, session_id) DO UPDATE SET
                update_time = EXCLUDED.update_time,
                event_count = EXCLUDED.event_count,
                synced_at = EXCLUDED.synced_at
            """,
            (account, session_id, float(update_time), int(event_count), synced),
        )

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

    def mark_slack_conversation_inactive(
        self,
        *,
        account: str,
        team_id: str,
        conversation_id: str,
    ) -> None:
        """Flag a conversation as archived/inactive after Slack reports it gone.

        Freshness and coverage passes filter on ``is_archived = 0``, so this stops
        a deleted/archived/left channel from being re-polled every cycle once
        Slack starts returning channel_not_found (etc.) for it. A later
        conversations.list refresh re-inserts the channel with its live
        ``is_archived`` value, so a conversation that becomes reachable again
        self-heals back to active.
        """
        self._command(
            """
            UPDATE slack_conversations
               SET is_archived = 1
             WHERE account = %s
               AND team_id = %s
               AND conversation_id = %s
            """,
            (account, team_id, conversation_id),
        )

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
                s.updated_at ASC NULLS FIRST,
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
        missing_replies_only: bool = False,
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
        if missing_replies_only:
            where.append(
                "NOT EXISTS ("
                "SELECT 1 FROM slack_messages AS r "
                "WHERE r.account = m.account "
                "AND r.team_id = m.team_id "
                "AND r.conversation_id = m.conversation_id "
                "AND r.thread_ts = m.message_ts "
                "AND r.is_deleted = 0 "
                "AND r.is_thread_reply = 1"
                ")"
            )
        order_by = "m.message_datetime DESC, m.message_ts DESC"
        if order == "reply_count":
            order_by = "m.reply_count DESC, m.message_datetime DESC, m.message_ts DESC"
        elif order == "oldest":
            order_by = "m.message_datetime ASC, m.message_ts ASC"
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
        # Restrict to top-level messages: `conversations.history` never returns
        # thread replies inline, so the caller's "seen" set will not include them
        # either. Returning replies here would make every reply within the window
        # look like a deletion to the caller, and they would get tombstoned on
        # the next partial sync.
        rows = self._query(
            f"""
            SELECT message_ts
            FROM slack_messages
            WHERE account = %s
              AND team_id = %s
              AND conversation_id = %s
              AND is_deleted = 0
              AND is_thread_reply = 0
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

    # Tables the cross-source search views read. The views are only (re)created
    # once every referenced table exists, so partial test schemas and staged
    # rollouts of new source groups degrade to "view not there yet".
    _SEARCHABLE_TEXT_TABLES = (
        "gmail_messages",
        "gmail_attachments",
        "file_attachment_enrichments",
        "slack_messages",
        "slack_conversations",
        "slack_users",
        "slack_files",
        "apple_notes",
        "apple_note_revisions",
        "apple_messages",
        "apple_message_handles",
        "whatsapp_messages",
        "whatsapp_chats",
        "whatsapp_contacts",
        "whatsapp_media_items",
        "apple_voice_memos_enrichments",
        "calendar_events",
        "contact_cards",
        "agent_run_events",
        "agent_session_events",
        "upstream_mutations",
        "upstream_mutation_requests",
        "google_drive_files",
        "google_drive_file_texts",
    )

    def _ensure_search_views_if_possible(self) -> None:
        # The person_identities view was removed; drop any copy left behind by
        # deployments that created it.
        self._command("DROP VIEW IF EXISTS person_identities")
        # The legacy cross-source searchable_text view (a ~30-branch UNION that
        # callers scanned with ILIKE/~*, often OR-ing the un-indexable computed
        # `who` column, and which therefore timed out) has been replaced by the
        # BM25 search_text() function. Drop any copy older deployments left
        # behind so it cannot be used as a slow fallback.
        self._command("DROP VIEW IF EXISTS searchable_text")
        if all(self._relation_exists(table) for table in self._SEARCHABLE_TEXT_TABLES):
            # Build every bm25 index search_text() references BEFORE (re)creating
            # the function, so it can never point at a not-yet-built index.
            # ensure_* builds indexes lazily per table group, but the function
            # references all sources at once. Without this, the first group to run
            # after a deploy recreates the function while other groups' bm25
            # indexes are still missing, and every search_text() call fails with
            # "index ... does not exist" until those (often sensor/new-data-driven)
            # groups happen to tick. The gate above guarantees all referenced
            # tables exist, so building all their indexes here is safe.
            self._ensure_indexes(self._SEARCHABLE_TEXT_TABLES)
            self._ensure_search_text_function()

    def _ensure_search_text_function(self) -> None:
        # search_text() is the single, default cross-source search path. It fans
        # out to the per-table BM25 (pg_textsearch) indexes — one index-backed
        # top-k scan per source/column (ORDER BY col <@> to_bm25query(...) LIMIT
        # n) — then merges and re-ranks. This replaces the old searchable_text
        # view: pg_textsearch's <@> operator cannot run through a view, so broad
        # cross-source searches used to fall back to ILIKE/~* full scans over the
        # whole UNION and time out.
        #
        # Output is (source, subsource, context, who, occurred_at, account, ref,
        # text, score); drill into the underlying table via `ref` for full rows.
        # Coverage mirrors what the old view searched — every BM25-indexable text
        # column across gmail, slack, attachments, transcripts, notes, imessage,
        # whatsapp, calendar, contacts, agent sessions, and mutations. Deliberate
        # omissions: upstream_mutations.payload_json (raw jsonb; a structured blob
        # with marginal full-text value, though the mutation title is still
        # covered) and agent_run_events (the internal enrichment agent's raw-JSON
        # operational logs — see the NOTE on its dropped branch below).
        # Transcript branches read the raw enrichments table (where
        # the BM25 index lives) rather than the de-duplicated clean view, so they
        # can surface more than one prompt-version row per recording.
        #
        # Two pg_textsearch facts shape the SQL: (1) the explicit
        # to_bm25query('query', 'index_name') form names the index directly, so it
        # works regardless of search_path schema (the implicit col <@> 'query'
        # form only resolves indexes in the default schema); (2) BM25 scores are
        # per-corpus, so cross-source ranking is approximate — good for recall
        # ("find every mention"), not a global relevance guarantee. Requires
        # `public` on search_path for the operator/helpers (true in production).

        def branch(
            source: str,
            subsource: str,
            context: str,
            who: str,
            occurred: str,
            account: str,
            ref: str,
            from_sql: str,
            col: str,
            idx: str,
            where: str = "",
        ) -> str:
            # One bm25-indexed text column -> one index-backed top-k subquery.
            where_sql = f" WHERE {where}" if where else ""
            rank = f"{col} <@> to_bm25query(%1$L, '{idx}')"
            return (
                f"( SELECT {source} AS source, {subsource} AS subsource, "
                f"{context} AS context, {who} AS who, {occurred} AS occurred_at, "
                f"{account} AS account, {ref} AS ref, {col} AS text, "
                f"({rank})::real AS score "
                f"FROM {from_sql}{where_sql} ORDER BY {rank} LIMIT %2$s )"
            )

        def join_branch(select_sql: str, inner_sql: str, joins_sql: str, where: str = "") -> str:
            # Branches whose metadata needs joins keep the bm25 top-k on the base
            # table (inner_sql), then join for context/who; the join runs over
            # only the top-k rows.
            where_sql = f" WHERE {where}" if where else ""
            return f"( {select_sql} FROM ( {inner_sql} ) m {joins_sql}{where_sql} )"

        branches = [
            branch(
                "'gmail'::text", "'subject'::text", "''::text", "from_address",
                "internal_date", "account", "account || ':' || message_id",
                "gmail_messages", "subject", "gmail_messages_subject_bm25_idx",
                where="is_deleted = 0",
            ),
            branch(
                "'gmail'", "'body'", "''", "from_address", "internal_date", "account",
                "account || ':' || message_id", "gmail_messages", "body_text",
                "gmail_messages_body_text_bm25_idx", where="is_deleted = 0",
            ),
            # gmail attachment content: bm25 top-k on the shared enrichment text,
            # then join the gmail attachment row for filename/account/date. The
            # inner join to gmail_attachments naturally restricts the shared
            # file_attachment_enrichments table to gmail-origin rows.
            join_branch(
                "SELECT 'gmail_attachment' AS source, 'content' AS subsource, a.filename AS context, "
                "'' AS who, a.internal_date AS occurred_at, a.account AS account, "
                "a.account || ':' || a.message_id || ':' || a.content_sha256 AS ref, "
                "m.text AS text, m.score AS score",
                "SELECT content_sha256, text, "
                "(text <@> to_bm25query(%1$L, 'file_attachment_enrichments_text_bm25_idx'))::real AS score "
                "FROM file_attachment_enrichments "
                "ORDER BY text <@> to_bm25query(%1$L, 'file_attachment_enrichments_text_bm25_idx') LIMIT %2$s",
                "JOIN gmail_attachments a USING (content_sha256)",
                where="a.is_deleted = 0",
            ),
            branch(
                "'gmail_attachment'", "'filename'", "mime_type", "''", "internal_date", "account",
                "account || ':' || message_id || ':' || content_sha256", "gmail_attachments",
                "filename", "gmail_attachments_filename_bm25_idx", where="is_deleted = 0",
            ),
            # slack message: bm25 top-k on the message text, then join conversation
            # (for name/type) and user (for who).
            join_branch(
                "SELECT 'slack' AS source, c.conversation_type AS subsource, c.name AS context, "
                "COALESCE(NULLIF(u.real_name, ''), NULLIF(u.name, ''), m.user_id) AS who, "
                "m.message_datetime AS occurred_at, m.account AS account, "
                "m.team_id || ':' || m.conversation_id || ':' || m.message_ts AS ref, "
                "m.text AS text, m.score AS score",
                "SELECT account, team_id, conversation_id, user_id, message_ts, message_datetime, text, "
                "(text <@> to_bm25query(%1$L, 'slack_messages_text_bm25_idx'))::real AS score "
                "FROM slack_messages WHERE is_deleted = 0 "
                "ORDER BY text <@> to_bm25query(%1$L, 'slack_messages_text_bm25_idx') LIMIT %2$s",
                "JOIN slack_conversations c "
                "ON c.account = m.account AND c.team_id = m.team_id AND c.conversation_id = m.conversation_id "
                "LEFT JOIN slack_users u "
                "ON u.account = m.account AND u.team_id = m.team_id AND u.user_id = m.user_id",
            ),
            branch(
                "'slack_channel'", "'name'", "conversation_type", "''", "synced_at", "account",
                "team_id || ':' || conversation_id", "slack_conversations", "name",
                "slack_conversations_name_bm25_idx",
            ),
            branch(
                "'slack_channel'", "'topic'", "name", "''", "synced_at", "account",
                "team_id || ':' || conversation_id", "slack_conversations", "topic",
                "slack_conversations_topic_bm25_idx", where="topic != ''",
            ),
            branch(
                "'slack_channel'", "'purpose'", "name", "''", "synced_at", "account",
                "team_id || ':' || conversation_id", "slack_conversations", "purpose",
                "slack_conversations_purpose_bm25_idx", where="purpose != ''",
            ),
            branch(
                "'slack_file'", "'name'", "mimetype", "user_id", "created_at", "account",
                "team_id || ':' || file_id", "slack_files", "name",
                "slack_files_name_bm25_idx", where="is_deleted = 0",
            ),
            branch(
                "'slack_file'", "'title'", "mimetype", "user_id", "created_at", "account",
                "team_id || ':' || file_id", "slack_files", "title",
                "slack_files_title_bm25_idx", where="is_deleted = 0 AND title != ''",
            ),
            branch(
                "'transcript'", "'transcript'", "title", "''", "start_at", "account", "recording_id",
                "apple_voice_memos_enrichments", "transcript", "apple_voice_memos_transcript_bm25_idx",
                where="transcript != ''",
            ),
            branch(
                "'transcript'", "'title'", "''", "''", "start_at", "account", "recording_id",
                "apple_voice_memos_enrichments", "title", "apple_voice_memos_title_bm25_idx",
                where="title != ''",
            ),
            branch(
                "'transcript'", "'summary'", "title", "''", "start_at", "account", "recording_id",
                "apple_voice_memos_enrichments", "summary", "apple_voice_memos_summary_bm25_idx",
                where="summary != ''",
            ),
            branch(
                "'transcript'", "'participants'", "title", "''", "start_at", "account", "recording_id",
                "apple_voice_memos_enrichments", "participants_json",
                "apple_voice_memos_participants_bm25_idx", where="participants_json NOT IN ('', '[]')",
            ),
            branch(
                "'transcript'", "'action_items'", "title", "''", "start_at", "account", "recording_id",
                "apple_voice_memos_enrichments", "action_items_json",
                "apple_voice_memos_action_items_bm25_idx", where="action_items_json NOT IN ('', '[]')",
            ),
            branch(
                "'note'", "'title'", "folder_path", "title", "modified_at", "account", "note_id",
                "apple_notes", "title", "apple_notes_title_bm25_idx", where="is_deleted = 0",
            ),
            branch(
                "'note'", "'body'", "folder_path", "title", "modified_at", "account", "note_id",
                "apple_notes", "body_text", "apple_notes_body_bm25_idx", where="is_deleted = 0",
            ),
            branch(
                "'note'", "'revision'", "folder_path", "title", "modified_at", "account",
                "note_id || '@' || revision_id", "apple_note_revisions", "body_text",
                "apple_note_revisions_body_bm25_idx",
            ),
            # imessage: bm25 top-k on the message body, then join the handle for who.
            join_branch(
                "SELECT 'imessage' AS source, m.service AS subsource, m.group_title AS context, "
                "CASE WHEN m.is_from_me = 1 THEN 'me' ELSE COALESCE(h.address, '') END AS who, "
                "m.message_at AS occurred_at, m.account AS account, m.message_id AS ref, "
                "m.body_text AS text, m.score AS score",
                "SELECT account, handle_id, is_from_me, service, group_title, message_at, message_id, body_text, "
                "(body_text <@> to_bm25query(%1$L, 'apple_messages_body_bm25_idx'))::real AS score "
                "FROM apple_messages WHERE is_deleted = 0 "
                "ORDER BY body_text <@> to_bm25query(%1$L, 'apple_messages_body_bm25_idx') LIMIT %2$s",
                "LEFT JOIN apple_message_handles h ON h.account = m.account AND h.handle_id = m.handle_id",
            ),
            # whatsapp body: bm25 top-k on the message body, then join chat + contact.
            join_branch(
                "SELECT 'whatsapp' AS source, 'body' AS subsource, COALESCE(NULLIF(c.name, ''), m.chat_id) AS context, "
                "CASE WHEN m.is_from_me = 1 THEN 'me' ELSE COALESCE(NULLIF(ct.full_name, ''), "
                "NULLIF(ct.push_name, ''), NULLIF(m.push_name, ''), m.sender_jid) END AS who, "
                "m.message_at AS occurred_at, m.account AS account, m.chat_id || ':' || m.message_id AS ref, "
                "m.body_text AS text, m.score AS score",
                "SELECT account, chat_id, sender_jid, push_name, is_from_me, message_at, message_id, body_text, "
                "(body_text <@> to_bm25query(%1$L, 'whatsapp_messages_body_bm25_idx'))::real AS score "
                "FROM whatsapp_messages WHERE is_deleted = 0 "
                "ORDER BY body_text <@> to_bm25query(%1$L, 'whatsapp_messages_body_bm25_idx') LIMIT %2$s",
                "LEFT JOIN whatsapp_chats c ON c.account = m.account AND c.chat_id = m.chat_id "
                "LEFT JOIN whatsapp_contacts ct ON ct.account = m.account AND ct.jid = m.sender_jid",
            ),
            branch(
                "'whatsapp_chat'", "'name'", "chat_type", "''", "last_message_at", "account", "chat_id",
                "whatsapp_chats", "name", "whatsapp_chats_name_bm25_idx", where="name != ''",
            ),
            branch(
                "'whatsapp_media'", "'filename'", "mime_type", "''", "message_at", "account",
                "chat_id || ':' || message_id", "whatsapp_media_items", "filename",
                "whatsapp_media_items_filename_bm25_idx", where="filename != ''",
            ),
            # whatsapp media content: bm25 top-k on the shared enrichment text,
            # then join the media row for filename/account/date. The inner join to
            # whatsapp_media_items restricts the shared enrichment table to
            # whatsapp-origin rows.
            join_branch(
                "SELECT 'whatsapp_media' AS source, 'content' AS subsource, a.filename AS context, "
                "'' AS who, a.message_at AS occurred_at, a.account AS account, "
                "a.chat_id || ':' || a.message_id AS ref, "
                "m.text AS text, m.score AS score",
                "SELECT content_sha256, text, "
                "(text <@> to_bm25query(%1$L, 'file_attachment_enrichments_text_bm25_idx'))::real AS score "
                "FROM file_attachment_enrichments "
                "ORDER BY text <@> to_bm25query(%1$L, 'file_attachment_enrichments_text_bm25_idx') LIMIT %2$s",
                "JOIN whatsapp_media_items a USING (content_sha256)",
            ),
            branch(
                "'calendar'", "'summary'", "''", "organizer_email", "start_at", "account",
                "calendar_id || ':' || event_id", "calendar_events", "summary",
                "calendar_events_summary_bm25_idx", where="is_deleted = 0",
            ),
            branch(
                "'calendar'", "'description'", "summary", "organizer_email", "start_at", "account",
                "calendar_id || ':' || event_id", "calendar_events", "description",
                "calendar_events_description_bm25_idx", where="is_deleted = 0 AND description != ''",
            ),
            branch(
                "'calendar'", "'location'", "summary", "organizer_email", "start_at", "account",
                "calendar_id || ':' || event_id", "calendar_events", "location",
                "calendar_events_location_bm25_idx", where="is_deleted = 0 AND location != ''",
            ),
            branch(
                "'calendar'", "'attendees'", "summary", "organizer_email", "start_at", "account",
                "calendar_id || ':' || event_id", "calendar_events", "attendees_json",
                "calendar_events_attendees_bm25_idx",
                where="is_deleted = 0 AND attendees_json NOT IN ('', '[]')",
            ),
            branch(
                "'contact'", "'name'", "source_kind", "primary_email", "source_updated_at", "account",
                "card_id", "contact_cards", "display_name", "contact_cards_name_bm25_idx",
                where="is_deleted = 0",
            ),
            branch(
                "'contact'", "'organization'", "display_name", "primary_email", "source_updated_at",
                "account", "card_id", "contact_cards", "organization",
                "contact_cards_organization_bm25_idx", where="is_deleted = 0 AND organization != ''",
            ),
            branch(
                "'contact'", "'job_title'", "display_name", "primary_email", "source_updated_at",
                "account", "card_id", "contact_cards", "job_title", "contact_cards_job_title_bm25_idx",
                where="is_deleted = 0 AND job_title != ''",
            ),
            branch(
                "'contact'", "'notes'", "display_name", "primary_email", "source_updated_at",
                "account", "card_id", "contact_cards", "notes", "contact_cards_notes_bm25_idx",
                where="is_deleted = 0 AND notes != ''",
            ),
            # NOTE: agent_run_events (the warehouse's own internal
            # enrichment-agent operational logs) is deliberately NOT searched
            # here. Its `text` column is raw JSON / stderr for every event type
            # (item.completed, turn.started, error, ...), never human-readable
            # content, so it only injects raw-JSON noise that crowds out real
            # cross-source matches. The agent's actual output is already
            # searchable via the enrichment tables (voice-memo summaries, file
            # attachments) and the user-facing agent_session source below. The
            # agent_run_events_text_bm25_idx index is kept for direct queries.
            branch(
                "'agent_session'", "source", "role", "account", "occurred_at", "account",
                "source || ':' || session_id || ':' || event_uuid", "agent_session_events", "text",
                "agent_session_events_text_bm25_idx", where="text != '' AND role IN ('user', 'assistant')",
            ),
            branch(
                "'agent_session'", "'title'", "source", "account", "occurred_at", "account",
                "source || ':' || session_id || ':' || event_uuid || ':title'", "agent_session_events",
                "session_title", "agent_session_events_title_bm25_idx", where="session_title != ''",
            ),
            branch(
                "'mutation'", "status", "operation", "requested_by", "created_at", "account", "id",
                "upstream_mutations", "title", "upstream_mutations_title_bm25_idx",
            ),
            branch(
                "'mutation_request'", "status", "''", "requested_by", "created_at", "''", "id",
                "upstream_mutation_requests", "title", "upstream_mutation_requests_title_bm25_idx",
            ),
            branch(
                "'mutation_request'", "status", "title", "requested_by", "created_at", "''",
                "id || ':reason'", "upstream_mutation_requests", "reason",
                "upstream_mutation_requests_reason_bm25_idx", where="reason != ''",
            ),
            branch(
                "'google_drive'", "'filename'", "folder_path", "last_modifying_user", "modified_time",
                "account", "account || ':' || file_id", "google_drive_files", "name",
                "google_drive_files_name_bm25_idx", where="trashed = 0 AND is_excluded = 0",
            ),
            join_branch(
                "SELECT 'google_drive' AS source, 'content' AS subsource, f.folder_path AS context, "
                "f.last_modifying_user AS who, f.modified_time AS occurred_at, f.account AS account, "
                "f.account || ':' || f.file_id AS ref, m.text AS text, m.score AS score",
                "SELECT account, file_id, text, "
                "(text <@> to_bm25query(%1$L, 'google_drive_file_texts_text_bm25_idx'))::real AS score "
                "FROM google_drive_file_texts "
                "WHERE text_extraction_status = 'ok' AND text != '' "
                "ORDER BY text <@> to_bm25query(%1$L, 'google_drive_file_texts_text_bm25_idx') LIMIT %2$s",
                "JOIN google_drive_files f USING (account, file_id)",
                where="f.trashed = 0 AND f.is_excluded = 0",
            ),
        ]

        # Run each branch as its own statement (dollar-quoted in a plpgsql array)
        # so a single missing or unusable bm25 index drops only that one source
        # instead of failing the entire function. Without this, one bad branch
        # ("index ... does not exist" — a not-yet-built index right after a
        # deploy, a failed/half-built index, or a future source whose index never
        # built) makes the whole cross-source search path 100% unusable.
        #
        # Results accumulate in a plpgsql array (NOT a temp table): the function
        # must run on the read-only query surface (app/internal/query enforces
        # read-only by statement parsing today, but the function must stay correct
        # under a genuine read-only transaction / read replica too), so it does no
        # DDL or DML at call time — only EXECUTE ... SELECT and array building.
        # Each branch's rows are array_agg'd into the search_text_hit row type;
        # we then unnest, filter, rank, and limit across every source that
        # succeeded.
        #
        # The row type is created ONLY if absent (stable OID), never dropped on a
        # routine ensure. ensure_* runs constantly, and dropping + recreating the
        # type each time churns its OID — the already-compiled function (which
        # binds search_text_hit by OID) and any cached plan then fail mid-call
        # with "cache lookup failed for type <stale oid>". The function is
        # CREATE OR REPLACE'd (stable OID, same return type) over that fixed type.
        # Changing search_text_hit's columns therefore needs a manual
        # DROP TYPE ... CASCADE first (rare; the column set is stable).
        branch_array = ",\n                        ".join(f"$b${b}$b$" for b in branches)
        self._command(
            r"""
            DO $do$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_type t
                    JOIN pg_namespace n ON n.oid = t.typnamespace
                    WHERE t.typname = 'search_text_hit' AND n.nspname = current_schema()
                ) THEN
                    CREATE TYPE search_text_hit AS (
                        source text, subsource text, context text, who text,
                        occurred_at timestamptz, account text, ref text,
                        text text, score real
                    );
                END IF;
            END
            $do$;
            CREATE OR REPLACE FUNCTION search_text(
                query text,
                max_results integer DEFAULT 50,
                sources text[] DEFAULT NULL,
                since timestamptz DEFAULT NULL
            )
            RETURNS SETOF search_text_hit
            LANGUAGE plpgsql
            STABLE
            AS $fn$
            DECLARE
                per_source integer := greatest(coalesce(max_results, 50), 1);
                branch text;
                hits search_text_hit[] := '{}';
                branch_hits search_text_hit[];
            BEGIN
                FOREACH branch IN ARRAY ARRAY[
                        """
            + branch_array
            + r"""
                ] LOOP
                    BEGIN
                        EXECUTE format(
                            'SELECT array_agg(x::search_text_hit) FROM (' || branch || ') x',
                            query, per_source
                        ) INTO branch_hits;
                        IF branch_hits IS NOT NULL THEN
                            hits := hits || branch_hits;
                        END IF;
                    EXCEPTION WHEN OTHERS THEN
                        -- Missing/unusable bm25 index (or any branch error): skip
                        -- this source; the rest of the search still returns.
                        NULL;
                    END;
                END LOOP;
                RETURN QUERY
                    SELECT h.source, h.subsource, h.context, h.who, h.occurred_at,
                           h.account, h.ref, h.text, h.score
                    FROM unnest(hits) AS h
                    WHERE (sources IS NULL OR h.source = ANY (sources))
                      AND (since IS NULL OR h.occurred_at >= since)
                    ORDER BY h.score ASC NULLS LAST
                    LIMIT per_source;
            END;
            $fn$;
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

    def _ensure_clean_contacts_view(self) -> None:
        self._command(
            """
            CREATE OR REPLACE VIEW clean_contacts AS
            SELECT
                source,
                account,
                source_kind,
                address_book_id,
                card_id,
                etag,
                source_uid,
                display_name,
                given_name,
                family_name,
                organization,
                job_title,
                primary_email,
                primary_phone,
                emails,
                phones,
                addresses,
                organizations,
                urls,
                groups,
                dates,
                photos,
                notes,
                source_updated_at,
                synced_at,
                raw_json
            FROM contact_cards
            WHERE is_deleted = 0
            """
        )

    def _ensure_clean_whatsapp_messages_view(self) -> None:
        # Ergonomic layer over whatsapp_messages: a single chat_kind so callers
        # never re-derive "is this a DM / group / status post" from chat_id, plus
        # resolved sender_name and chat_name. chat_kind reads the (now complete)
        # whatsapp_chats.chat_type, falling back to a JID-derived value so the
        # view is correct even before the chat backfill runs. The fallback CASE
        # mirrors events.chat_type_for_jid.
        self._command(
            """
            CREATE OR REPLACE VIEW clean_whatsapp_messages AS
            SELECT
                m.account,
                m.chat_id,
                m.message_id,
                COALESCE(
                    NULLIF(c.chat_type, ''),
                    CASE
                        WHEN m.chat_id = 'status@broadcast' THEN 'status'
                        WHEN m.chat_id LIKE '%@s.whatsapp.net' THEN 'user'
                        WHEN m.chat_id LIKE '%@lid' THEN 'user'
                        WHEN m.chat_id LIKE '%@g.us' THEN 'group'
                        WHEN m.chat_id LIKE '%@broadcast' THEN 'broadcast'
                        WHEN m.chat_id LIKE '%@newsletter' THEN 'newsletter'
                        WHEN position('@' in m.chat_id) > 0 THEN split_part(m.chat_id, '@', 2)
                        ELSE 'unknown'
                    END
                ) AS chat_kind,
                CASE
                    WHEN m.chat_id = 'status@broadcast' THEN 'Status'
                    WHEN m.chat_id LIKE '%@g.us' THEN NULLIF(c.name, '')
                    ELSE COALESCE(NULLIF(cc.full_name, ''), NULLIF(cc.push_name, ''), m.chat_id)
                END AS chat_name,
                m.sender_jid,
                m.is_from_me,
                CASE
                    WHEN m.is_from_me = 1 THEN 'me'
                    ELSE COALESCE(
                        NULLIF(ct.full_name, ''), NULLIF(ct.push_name, ''),
                        NULLIF(m.push_name, ''), m.sender_jid
                    )
                END AS sender_name,
                m.body_text,
                m.message_kind,
                m.media_type,
                m.quoted_message_id,
                m.message_at,
                m.edited_at,
                m.is_deleted
            FROM whatsapp_messages m
            LEFT JOIN whatsapp_chats c ON c.account = m.account AND c.chat_id = m.chat_id
            LEFT JOIN whatsapp_contacts cc ON cc.account = m.account AND cc.jid = m.chat_id
            LEFT JOIN whatsapp_contacts ct ON ct.account = m.account AND ct.jid = m.sender_jid
            """
        )

    def _ensure_clean_agent_sessions_view(self) -> None:
        # Session-level roll-up over the per-line event log. Header fields take
        # the first/last non-empty value seen so a session split across batches
        # converges; counts and token sums aggregate the whole session, which a
        # stored-aggregate upsert could not do correctly across batches.
        self._command(
            """
            CREATE OR REPLACE VIEW clean_agent_sessions AS
            SELECT
                source,
                session_id,
                max(account) AS account,
                max(device) AS device,
                (array_agg(session_title ORDER BY seq) FILTER (WHERE session_title != ''))[1] AS title,
                (array_agg(cwd ORDER BY seq) FILTER (WHERE cwd != ''))[1] AS cwd,
                (array_agg(git_branch ORDER BY seq DESC) FILTER (WHERE git_branch != ''))[1] AS git_branch,
                (array_agg(git_commit ORDER BY seq DESC) FILTER (WHERE git_commit != ''))[1] AS git_commit,
                (array_agg(repo_url ORDER BY seq) FILTER (WHERE repo_url != ''))[1] AS repo_url,
                (array_agg(model ORDER BY seq DESC) FILTER (WHERE model != ''))[1] AS model,
                (array_agg(cli_version ORDER BY seq DESC) FILTER (WHERE cli_version != ''))[1] AS cli_version,
                (array_agg(entrypoint ORDER BY seq) FILTER (WHERE entrypoint != ''))[1] AS entrypoint,
                (array_agg(text ORDER BY seq) FILTER (WHERE role = 'user' AND text != ''))[1] AS first_prompt,
                min(occurred_at) FILTER (WHERE occurred_at > '1970-01-01 00:00:00+00'::timestamptz) AS started_at,
                max(occurred_at) FILTER (WHERE occurred_at > '1970-01-01 00:00:00+00'::timestamptz) AS ended_at,
                count(*)::bigint AS event_count,
                count(*) FILTER (WHERE role = 'user')::bigint AS user_event_count,
                count(*) FILTER (WHERE role = 'assistant')::bigint AS assistant_event_count,
                sum(input_tokens)::bigint AS input_tokens,
                sum(output_tokens)::bigint AS output_tokens,
                sum(cache_read_tokens)::bigint AS cache_read_tokens,
                sum(cache_creation_tokens)::bigint AS cache_creation_tokens
            FROM agent_session_events
            GROUP BY source, session_id
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

    def _query_dicts(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        with self._connection.cursor() as cursor:
            cursor.execute(sql, params)
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    def _insert(self, table: str, rows: list[tuple[Any, ...]], columns: tuple[str, ...]) -> None:
        if not rows:
            return
        spec = POSTGRES_TABLES[table]
        rows = _dedupe_conflict_rows(rows, columns, spec, table=table)
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
        self._insert(
            table,
            [
                tuple(_normalize_insert_value(row[column], table=table, column=column) for column in columns)
                for row in rows
            ],
            columns,
        )


def _postgres_type(column: str, *, table: str | None = None) -> str:
    if _is_jsonb_column(table, column):
        return "jsonb"
    if column in ARRAY_COLUMNS:
        return "text[]"
    if column in TIMESTAMP_COLUMNS:
        return "timestamptz"
    if column in FLOAT_COLUMNS:
        return "double precision"
    if column in INTEGER_COLUMNS:
        return "bigint"
    return "text"


def _default_sql(column: str, *, table: str | None = None) -> str:
    if _is_jsonb_column(table, column):
        if column in JSONB_ARRAY_COLUMNS_BY_TABLE.get(table or "", set()):
            return "'[]'::jsonb"
        return "'{}'::jsonb"
    if column in ARRAY_COLUMNS:
        return "'{}'::text[]"
    if column in TIMESTAMP_COLUMNS:
        return "'1970-01-01 00:00:00+00'::timestamptz"
    if column in FLOAT_COLUMNS:
        return "0"
    if column in INTEGER_COLUMNS:
        return "0"
    return "''"


def _is_jsonb_column(table: str | None, column: str) -> bool:
    return bool(table and column in JSONB_COLUMNS_BY_TABLE.get(table, set()))


# Columns that an ``ON CONFLICT DO UPDATE`` keeps from the existing row when
# the incoming row's value is empty (see _upsert_assignment). In-batch dedupe
# applies the same merge so collapsing rows cannot drop these values.
PRESERVE_NON_EMPTY_COLUMNS_BY_TABLE: dict[str, tuple[str, ...]] = {
    "apple_message_attachments": (
        "content_sha256",
        "storage_backend",
        "storage_key",
        "storage_file_id",
        "storage_url",
    ),
    "gmail_attachments": (
        "storage_backend",
        "storage_key",
        "storage_file_id",
        "storage_url",
        "storage_status",
    ),
    "whatsapp_media_items": (
        "content_sha256",
        "storage_backend",
        "storage_key",
        "storage_file_id",
        "storage_url",
    ),
    # Pushname-only updates must not wipe fuller names from contact-store dumps.
    "whatsapp_contacts": (
        "push_name",
        "first_name",
        "full_name",
        "business_name",
    ),
    # Empty-name history-sync chat rows must not wipe a real group subject that
    # only the live joined-groups dump can supply.
    "whatsapp_chats": ("name",),
    # A later participant snapshot that drops a display name must not blank it.
    "whatsapp_chat_participants": ("display_name",),
}


def _dedupe_conflict_rows(
    rows: list[tuple[Any, ...]],
    columns: tuple[str, ...],
    spec: TableSpec,
    *,
    table: str = "",
) -> list[tuple[Any, ...]]:
    """Collapse rows that share an ON CONFLICT key within a single batch.

    Postgres rejects an ``INSERT ... ON CONFLICT DO UPDATE`` whose VALUES list
    targets the same conflict row twice ("ON CONFLICT DO UPDATE command cannot
    affect row a second time"). A sync window can legitimately yield two rows
    with the same primary key (e.g. an edited Slack message appearing twice in
    one ``conversations.history`` page), which used to fail the entire run.

    Keep the row that the version guard (``table.version <= EXCLUDED.version``)
    would leave persisted: the highest ``version_column`` value, and the last
    occurrence on ties. First-seen order of distinct keys is preserved. For
    the table's preserve-non-empty columns, the winner inherits values the
    losing rows carried when its own are empty — collapsing in-process must
    not drop data the SQL upsert would have preserved.
    """
    primary_key = spec.primary_key
    if len(rows) <= 1 or not primary_key:
        return rows
    try:
        key_indexes = tuple(columns.index(column) for column in primary_key)
    except ValueError:
        # A primary-key column is absent from this partial insert; without the
        # full key we can't dedupe safely, so leave the batch untouched.
        return rows
    version_index = columns.index(spec.version_column) if spec.version_column in columns else None
    preserve_indexes = tuple(
        columns.index(column)
        for column in PRESERVE_NON_EMPTY_COLUMNS_BY_TABLE.get(table, ())
        if column in columns
    )

    winners: dict[tuple[Any, ...], tuple[Any, ...]] = {}
    for row in rows:
        key = tuple(row[index] for index in key_indexes)
        existing = winners.get(key)
        if existing is None:
            winners[key] = row
            continue
        if _conflict_row_wins(row, existing, version_index):
            winner, loser = row, existing
        else:
            winner, loser = existing, row
        winners[key] = _merge_preserved_columns(winner, loser, preserve_indexes)
    if len(winners) == len(rows):
        return rows
    return list(winners.values())


def _merge_preserved_columns(
    winner: tuple[Any, ...],
    loser: tuple[Any, ...],
    preserve_indexes: tuple[int, ...],
) -> tuple[Any, ...]:
    if not preserve_indexes:
        return winner
    merged = list(winner)
    changed = False
    for index in preserve_indexes:
        if not merged[index] and loser[index]:
            merged[index] = loser[index]
            changed = True
    return tuple(merged) if changed else winner


def _conflict_row_wins(
    candidate: tuple[Any, ...],
    existing: tuple[Any, ...],
    version_index: int | None,
) -> bool:
    """Whether ``candidate`` (later in batch order) supersedes ``existing``.

    Mirrors the SQL guard ``table.version <= EXCLUDED.version``: a later row
    wins when its version is greater than or equal to the kept row's, so ties
    fall to the last writer. Falls back to last-wins when there is no version
    column or the values are not comparable.
    """
    if version_index is None:
        return True
    try:
        return candidate[version_index] >= existing[version_index]
    except TypeError:
        return True


def _upsert_clause(table: str, spec: TableSpec, columns: tuple[str, ...] | None = None) -> str:
    columns = columns or spec.columns
    update_columns = [column for column in columns if column not in spec.primary_key]
    conflict_columns = ", ".join(_identifier(column) for column in spec.primary_key)
    if not update_columns:
        return f"ON CONFLICT ({conflict_columns}) DO NOTHING"
    preserve_non_empty_columns = PRESERVE_NON_EMPTY_COLUMNS_BY_TABLE.get(table, ())
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


def _normalize_insert_value(value: Any, *, table: str | None = None, column: str | None = None) -> Any:
    if column and _is_jsonb_column(table, column):
        return Json(_normalize_json_value(value), dumps=lambda data: json.dumps(data, sort_keys=True, separators=(",", ":"), default=str))
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if isinstance(value, str):
        return value.replace("\x00", POSTGRES_TEXT_NUL_REPLACEMENT)
    if isinstance(value, list):
        return [_normalize_insert_value(item) for item in value]
    if isinstance(value, tuple):
        return [_normalize_insert_value(item) for item in value]
    return value


def _normalize_json_value(value: Any) -> Any:
    if isinstance(value, str):
        return value.replace("\x00", POSTGRES_TEXT_NUL_REPLACEMENT)
    if isinstance(value, datetime):
        return _ensure_utc(value).isoformat()
    if isinstance(value, dict):
        return {str(key): _normalize_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_json_value(item) for item in value]
    return value


def _jsonb_param(value: Any) -> Json:
    return Json(_normalize_json_value(value), dumps=lambda data: json.dumps(data, sort_keys=True, separators=(",", ":"), default=str))


def _as_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value:
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    return {}


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _json_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _calendar_id(value: Any) -> str:
    return str(value or "primary").strip() or "primary"


def _calendar_send_updates(value: Any) -> str:
    send_updates = str(value or "all").strip()
    if send_updates not in {"all", "externalOnly", "none"}:
        raise ValueError("send_updates must be all, externalOnly, or none")
    return send_updates


def _calendar_event_title(prefix: str, event: Mapping[str, Any]) -> str:
    summary = str(event.get("summary") or "").strip()
    return f"{prefix}: {summary}" if summary else prefix


def _calendar_event_preview(
    *,
    event: Mapping[str, Any],
    operation: str,
    calendar_id: str,
    send_updates: str,
    event_id: str = "",
    expected_etag: str = "",
) -> dict[str, Any]:
    preview: dict[str, Any] = {
        "operation": operation,
        "calendar_id": calendar_id,
        "send_updates": send_updates,
    }
    if event_id:
        preview["event_id"] = event_id
    if expected_etag:
        preview["expected_etag"] = expected_etag
    for key in (
        "summary",
        "description",
        "location",
        "start",
        "end",
        "attendees",
        "recurrence",
        "reminders",
        "transparency",
        "visibility",
        "status",
        "color_id",
        "colorId",
    ):
        if key in event:
            preview[key] = _normalize_json_value(event[key])
    return preview


def _json_ready(value: dict[str, Any]) -> dict[str, Any]:
    return {key: _normalize_json_value(item) for key, item in value.items()}


def _normalize_thread_ids(thread_ids: Any) -> list[str]:
    if not isinstance(thread_ids, Sequence) or isinstance(thread_ids, (str, bytes)):
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for value in thread_ids:
        thread_id = str(value).strip()
        if thread_id and thread_id not in seen:
            normalized.append(thread_id)
            seen.add(thread_id)
    return normalized


def _gmail_email_delivery_mode(value: Any) -> str:
    mode = str(value or "send").strip().lower()
    if mode not in {"send", "draft"}:
        raise ValueError("Gmail email delivery_mode must be send or draft")
    return mode


def _normalize_email_recipients(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw_values = value.split(",")
    elif isinstance(value, Sequence):
        raw_values = value
    else:
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_value in raw_values:
        item = str(raw_value).strip()
        if item and item not in seen:
            normalized.append(item)
            seen.add(item)
    return normalized


def _reply_subject(subject: str) -> str:
    stripped = subject.strip()
    if not stripped:
        return "Re:"
    return stripped if stripped.lower().startswith("re:") else f"Re: {stripped}"


def _gmail_email_title(message: Mapping[str, Any]) -> str:
    subject = str(message.get("subject") or "email")
    prefix = "Reply" if str(message.get("reply_to_thread_id") or "").strip() else "Send"
    return f"{prefix}: {subject}"


def _upstream_mutation_request_idempotency_key(
    *,
    title: str,
    reason: str,
    mutations: Sequence[Mapping[str, Any]],
) -> str:
    mutation_keys = [
        {
            "provider": str(mutation.get("provider") or ""),
            "operation": str(mutation.get("operation") or ""),
            "account": str(mutation.get("account") or ""),
            "payload_json": _normalize_json_value(mutation.get("payload_json")),
        }
        for mutation in mutations
    ]
    payload = {
        "mutations": sorted(
            mutation_keys,
            key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
        ),
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    return f"upstream_mutation_request:{digest}"


def _contact_resource_name(operation: Mapping[str, Any]) -> str:
    resource_name = str(operation.get("resource_name") or operation.get("resourceName") or "").strip()
    if not resource_name.startswith("people/"):
        raise ValueError("contact resource_name must be a people/* resource name")
    return resource_name


def _contact_expected_etag(operation: Mapping[str, Any]) -> str:
    return str(operation.get("expected_etag") or operation.get("etag") or "").strip()


def _contact_update_fields(value: Any) -> list[str]:
    if isinstance(value, str):
        fields = [field.strip() for field in value.split(",")]
    elif isinstance(value, Sequence):
        fields = [str(field).strip() for field in value]
    else:
        fields = []
    normalized = [field for field in fields if field]
    if not normalized:
        raise ValueError("update_contact must include update_person_fields")
    allowed = {
        "addresses",
        "biographies",
        "birthdays",
        "calendarUrls",
        "clientData",
        "emailAddresses",
        "events",
        "externalIds",
        "genders",
        "imClients",
        "interests",
        "locales",
        "locations",
        "memberships",
        "miscKeywords",
        "names",
        "nicknames",
        "occupations",
        "organizations",
        "phoneNumbers",
        "relations",
        "sipAddresses",
        "urls",
        "userDefined",
    }
    unsupported = [field for field in normalized if field not in allowed]
    if unsupported:
        raise ValueError(f"unsupported update_person_fields: {', '.join(unsupported)}")
    return list(dict.fromkeys(normalized))


def _contact_person_summary(person: Mapping[str, Any]) -> dict[str, Any]:
    names = person.get("names")
    emails = person.get("emailAddresses")
    phones = person.get("phoneNumbers")
    organizations = person.get("organizations")
    return {
        "display_name": _first_contact_value(names, "displayName"),
        "primary_email": _first_contact_value(emails, "value"),
        "primary_phone": _first_contact_value(phones, "canonicalForm") or _first_contact_value(phones, "value"),
        "organization": _first_contact_value(organizations, "name"),
        "resource_name": str(person.get("resourceName") or ""),
        "etag": str(person.get("etag") or ""),
    }


def _contact_mutation_title(preview_operation: Mapping[str, Any]) -> str:
    op = str(preview_operation.get("op") or "")
    summary = _json_mapping(preview_operation.get("summary"))
    label = str(summary.get("display_name") or summary.get("primary_email") or preview_operation.get("resource_name") or "contact")
    if op == "create_contact":
        return f"Create contact: {label}"
    if op == "update_contact":
        return f"Update contact: {label}"
    if op == "delete_contact":
        return f"Delete contact: {label}"
    return f"Change contact: {label}"


def _first_contact_value(value: Any, key: str) -> str:
    if not isinstance(value, list):
        return ""
    for item in value:
        if isinstance(item, Mapping) and item.get(key):
            return str(item.get(key))
    return ""


def _operation_result_for_resource(result: Mapping[str, Any], resource_name: str) -> dict[str, Any]:
    for item in _json_list(result.get("operation_results")):
        if str(item.get("resource_name") or "") == resource_name:
            return item
    return {}


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
