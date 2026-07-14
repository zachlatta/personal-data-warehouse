from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

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
    "storage_backend",
    "storage_key",
    "storage_file_id",
    "storage_url",
    "storage_status",
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

ATTACHMENT_ENRICHMENT_COLUMNS = (
    "content_sha256",
    "ai_provider",
    "ai_model",
    "ai_prompt_version",
    "text",
    "text_extraction_status",
    "text_extraction_error",
    "ai_base_url",
    "ai_prompt_sha256",
    "ai_prompt",
    "ai_source_status",
    "ai_elapsed_ms",
    "ai_processed_at",
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
    "expanded_synced_at",
    "expanded_window_start",
    "expanded_window_end",
    "updated_at",
    "sync_version",
)

CONTACT_CARD_COLUMNS = (
    "source",
    "account",
    "source_kind",
    "address_book_id",
    "card_id",
    "etag",
    "source_uid",
    "display_name",
    "given_name",
    "family_name",
    "organization",
    "job_title",
    "primary_email",
    "primary_phone",
    "emails",
    "phones",
    "addresses",
    "organizations",
    "urls",
    "nicknames",
    "groups",
    "dates",
    "photos",
    "notes",
    "is_deleted",
    "source_updated_at",
    "synced_at",
    "sync_version",
    "raw_json",
)

CONTACT_SYNC_STATE_COLUMNS = (
    "source",
    "account",
    "source_kind",
    "address_book_id",
    "sync_token",
    "last_sync_type",
    "status",
    "error",
    "full_synced_at",
    "updated_at",
    "sync_version",
)

VOICE_MEMO_FILE_COLUMNS = (
    "account",
    "recording_id",
    "title",
    "original_path",
    "filename",
    "extension",
    "content_type",
    "size_bytes",
    "content_sha256",
    "file_created_at",
    "file_modified_at",
    "recorded_at",
    "storage_backend",
    "storage_key",
    "storage_file_id",
    "storage_url",
    "metadata_storage_key",
    "metadata_storage_file_id",
    "metadata_storage_url",
    "metadata_content_sha256",
    "is_deleted",
    "raw_metadata_json",
    "ingested_at",
    "sync_version",
)

VOICE_MEMO_TRANSCRIPTION_RUN_COLUMNS = (
    "account",
    "recording_id",
    "content_sha256",
    "provider",
    "provider_transcript_id",
    "model",
    "status",
    "error",
    "transcript_text",
    "raw_result_json",
    "requested_at",
    "completed_at",
    "sync_version",
)

RETRYABLE_VOICE_MEMO_TRANSCRIPTION_ERROR_PATTERNS = (
    "current account balance is negative",
    "please top up",
    "upload failed, please try again",
    "too many requests",
    "rate limit",
    "rate limited",
    "timeout",
    "timed out",
    "temporarily",
    "temporary",
    "500 server error",
    "502 server error",
    "503 server error",
    "504 server error",
)

VOICE_MEMO_TRANSCRIPT_SEGMENT_COLUMNS = (
    "account",
    "recording_id",
    "provider",
    "provider_transcript_id",
    "segment_index",
    "speaker_label",
    "start_ms",
    "end_ms",
    "confidence",
    "text",
    "words_json",
    "created_at",
    "sync_version",
)

VOICE_MEMO_ENRICHMENT_COLUMNS = (
    "account",
    "recording_id",
    "content_sha256",
    "provider",
    "model",
    "prompt_version",
    "status",
    "error",
    "calendar_event_id",
    "calendar_confidence",
    "title",
    "start_at",
    "end_at",
    "participants_json",
    "transcript",
    "summary",
    "action_items_json",
    "evidence_json",
    "raw_result_json",
    "created_at",
    "sync_version",
)

APPLE_NOTE_COLUMNS = (
    "account",
    "note_id",
    "latest_revision_id",
    "title",
    "folder_id",
    "folder_path",
    "apple_account_id",
    "apple_account_name",
    "created_at",
    "modified_at",
    "body_text",
    "body_html",
    "body_markdown",
    "content_sha256",
    "attachments_json",
    "storage_backend",
    "metadata_storage_key",
    "metadata_storage_file_id",
    "metadata_storage_url",
    "metadata_content_sha256",
    "html_storage_key",
    "html_storage_file_id",
    "html_storage_url",
    "html_content_sha256",
    "is_deleted",
    "raw_metadata_json",
    "ingested_at",
    "sync_version",
)

APPLE_NOTE_REVISION_COLUMNS = (
    "account",
    "note_id",
    "revision_id",
    "title",
    "folder_id",
    "folder_path",
    "apple_account_id",
    "apple_account_name",
    "created_at",
    "modified_at",
    "exported_at",
    "body_text",
    "body_html",
    "body_markdown",
    "content_sha256",
    "attachments_json",
    "storage_backend",
    "metadata_storage_key",
    "metadata_storage_file_id",
    "metadata_storage_url",
    "metadata_content_sha256",
    "html_storage_key",
    "html_storage_file_id",
    "html_storage_url",
    "html_content_sha256",
    "is_deleted",
    "raw_metadata_json",
    "ingested_at",
    "sync_version",
)

APPLE_NOTE_ATTACHMENT_COLUMNS = (
    "account",
    "note_id",
    "revision_id",
    "attachment_id",
    "filename",
    "content_type",
    "size_bytes",
    "content_sha256",
    "is_missing",
    "error",
    "storage_backend",
    "storage_key",
    "storage_file_id",
    "storage_url",
    "raw_metadata_json",
    "ingested_at",
    "sync_version",
)

APPLE_MESSAGE_HANDLE_COLUMNS = (
    "account",
    "handle_id",
    "handle_rowid",
    "address",
    "country",
    "service",
    "uncanonicalized_id",
    "person_centric_id",
    "raw_metadata_json",
    "ingested_at",
    "sync_version",
)

APPLE_MESSAGE_CHAT_COLUMNS = (
    "account",
    "chat_id",
    "chat_rowid",
    "guid",
    "chat_identifier",
    "service_name",
    "display_name",
    "room_name",
    "account_login",
    "style",
    "state",
    "is_archived",
    "is_filtered",
    "is_recovered",
    "is_pending_review",
    "last_read_message_at",
    "raw_metadata_json",
    "ingested_at",
    "sync_version",
)

APPLE_MESSAGE_CHAT_HANDLE_COLUMNS = (
    "account",
    "chat_id",
    "handle_id",
    "raw_metadata_json",
    "ingested_at",
    "sync_version",
)

APPLE_MESSAGE_COLUMNS = (
    "account",
    "message_id",
    "message_rowid",
    "handle_id",
    "service",
    "message_account",
    "body_text",
    "body_source",
    "body_decode_status",
    "body_decode_error",
    "attributed_body_sha256",
    "subject",
    "country",
    "message_type",
    "message_item_type",
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
    "reply_to_guid",
    "associated_message_guid",
    "associated_message_type",
    "associated_message_emoji",
    "balloon_bundle_id",
    "group_title",
    "group_action_type",
    "message_action_type",
    "message_source",
    "expressive_send_style_id",
    "message_at",
    "date_ns",
    "date_read",
    "date_delivered",
    "date_played",
    "date_edited",
    "date_retracted",
    "date_recovered",
    "is_deleted",
    "raw_metadata_json",
    "ingested_at",
    "sync_version",
)

APPLE_MESSAGE_CHAT_MESSAGE_COLUMNS = (
    "account",
    "chat_id",
    "message_id",
    "message_date",
    "message_date_ns",
    "raw_metadata_json",
    "ingested_at",
    "sync_version",
)

APPLE_MESSAGE_ATTACHMENT_COLUMNS = (
    "account",
    "attachment_id",
    "attachment_rowid",
    "message_id",
    "guid",
    "original_guid",
    "filename",
    "transfer_name",
    "content_type",
    "uti",
    "mime_type",
    "total_bytes",
    "size_bytes",
    "content_sha256",
    "is_missing",
    "error",
    "is_outgoing",
    "is_sticker",
    "hide_attachment",
    "transfer_state",
    "created_at",
    "start_at",
    "storage_backend",
    "storage_key",
    "storage_file_id",
    "storage_url",
    "raw_metadata_json",
    "ingested_at",
    "sync_version",
)

WHATSAPP_CHAT_COLUMNS = (
    "account",
    "chat_id",
    "name",
    "chat_type",
    "is_archived",
    "last_message_at",
    "raw_metadata_json",
    "ingested_at",
    "sync_version",
)

WHATSAPP_CHAT_PARTICIPANT_COLUMNS = (
    "account",
    "chat_id",
    "participant_jid",
    "phone_jid",
    "lid_jid",
    "display_name",
    "is_admin",
    "is_super_admin",
    "raw_metadata_json",
    "ingested_at",
    "sync_version",
)

WHATSAPP_CONTACT_COLUMNS = (
    "account",
    "jid",
    "push_name",
    "first_name",
    "full_name",
    "business_name",
    "raw_metadata_json",
    "ingested_at",
    "sync_version",
)

WHATSAPP_MESSAGE_COLUMNS = (
    "account",
    "chat_id",
    "message_id",
    "sender_jid",
    "push_name",
    "is_from_me",
    "body_text",
    "message_kind",
    "media_type",
    "quoted_message_id",
    "message_at",
    "edited_at",
    "is_deleted",
    "raw_metadata_json",
    "ingested_at",
    "sync_version",
)

WHATSAPP_MEDIA_ITEM_COLUMNS = (
    "account",
    "chat_id",
    "message_id",
    "media_type",
    "filename",
    "mime_type",
    "total_bytes",
    "size_bytes",
    "file_sha256",
    "content_sha256",
    "is_missing",
    "error",
    "message_at",
    "storage_backend",
    "storage_key",
    "storage_file_id",
    "storage_url",
    "raw_metadata_json",
    "ingested_at",
    "sync_version",
)

PLAID_ITEM_COLUMNS = (
    "account",
    "item_id",
    "institution_id",
    "institution_name",
    "available_products",
    "billed_products",
    "webhook",
    "consent_expiration_time",
    "error_json",
    "raw_json",
    "linked_at",
    "synced_at",
    "sync_version",
)

PLAID_ITEM_TOKEN_COLUMNS = (
    "account",
    "item_id",
    "access_token",
    "institution_id",
    "institution_name",
    "linked_at",
    "updated_at",
    "sync_version",
)

PLAID_ACCOUNT_COLUMNS = (
    "account",
    "item_id",
    "account_id",
    "name",
    "official_name",
    "mask",
    "type",
    "subtype",
    "available_balance",
    "current_balance",
    "limit_balance",
    "iso_currency_code",
    "unofficial_currency_code",
    "is_removed",
    "raw_json",
    "synced_at",
    "sync_version",
)

PLAID_TRANSACTION_COLUMNS = (
    "account",
    "item_id",
    "account_id",
    "transaction_id",
    "posted_at",
    "authorized_at",
    "name",
    "merchant_name",
    "amount",
    "iso_currency_code",
    "unofficial_currency_code",
    "category_json",
    "payment_channel",
    "pending",
    "pending_transaction_id",
    "is_removed",
    "raw_json",
    "synced_at",
    "sync_version",
)

PLAID_INVESTMENT_SECURITY_COLUMNS = (
    "account",
    "security_id",
    "name",
    "ticker_symbol",
    "type",
    "close_price",
    "close_price_as_of",
    "iso_currency_code",
    "unofficial_currency_code",
    "raw_json",
    "synced_at",
    "sync_version",
)

PLAID_INVESTMENT_HOLDING_COLUMNS = (
    "account",
    "item_id",
    "account_id",
    "security_id",
    "quantity",
    "institution_value",
    "institution_price",
    "institution_price_as_of",
    "cost_basis",
    "iso_currency_code",
    "unofficial_currency_code",
    "raw_json",
    "synced_at",
    "sync_version",
)

PLAID_INVESTMENT_TRANSACTION_COLUMNS = (
    "account",
    "item_id",
    "account_id",
    "investment_transaction_id",
    "security_id",
    "transaction_at",
    "name",
    "quantity",
    "amount",
    "price",
    "fees",
    "type",
    "subtype",
    "iso_currency_code",
    "unofficial_currency_code",
    "raw_json",
    "synced_at",
    "sync_version",
)

PLAID_LIABILITY_COLUMNS = (
    "account",
    "item_id",
    "account_id",
    "liability_type",
    "last_payment_amount",
    "last_statement_balance",
    "minimum_payment_amount",
    "next_payment_due_at",
    "origination_principal_amount",
    "outstanding_interest_amount",
    "is_overdue",
    "iso_currency_code",
    "unofficial_currency_code",
    "raw_json",
    "synced_at",
    "sync_version",
)

PLAID_SYNC_STATE_COLUMNS = (
    "account",
    "item_id",
    "product",
    "cursor",
    "status",
    "error",
    "last_synced_at",
    "updated_at",
    "sync_version",
)


@dataclass(frozen=True)
class PlaidLinkedItem:
    account: str
    item_id: str
    access_token: str
    institution_id: str = ""
    institution_name: str = ""


# Finance ledger (derived `finance` schema): the cross-source stocks-and-flows
# layer. Every finance source (plaid now; manual_finance documents next) is a
# witness to one of two fact types — a flow (money moved) or a stock (something
# was worth X at time T). Raw source rows never learn about ledger identity
# (photos pattern): the finance_ledger asset resolves them into logical
# accounts via finance.account_links and appends observations. Facts only:
# categories and other opinions live in future enrichment tables, never here.
FINANCE_ACCOUNT_COLUMNS = (
    "account_id",
    "account",
    "name",
    "kind",
    "side",
    "currency",
    "institution",
    "mask",
    "created_at",
    "updated_at",
    "sync_version",
)

# Source-account → ledger-account resolution audit (finance.account_links):
# one row per source account, recording which logical account it resolved
# into and why. Deleting links and re-running the ledger asset replays every
# decision.
FINANCE_ACCOUNT_LINK_COLUMNS = (
    "source",
    "account",
    "source_account_key",
    "account_id",
    "match_method",
    "match_score",
    "created_at",
    "sync_version",
)

# Append-only point-in-time values (finance.observations): one row per
# account per day per kind per source. `balance` (bank/credit/brokerage),
# `valuation` (property/vehicle/private funds), `principal` (loans). Net
# worth is the latest observation per account summed by account side.
FINANCE_OBSERVATION_COLUMNS = (
    "account_id",
    "as_of",
    "kind",
    "value",
    "currency",
    "source",
    "observed_at",
    "sync_version",
)


# Photos: every photo source (apple_photos now; google_photos / photo_imports
# later) lands raw file rows with this exact shared shape in its own
# source-named schema (<source>.files). Cross-source identity lives in the
# derived photos.* tables below; raw rows never learn about identity.
PHOTO_SOURCE_FILE_COLUMNS = (
    "source",
    "account",
    "source_native_id",
    "role",
    "filename",
    "mime_type",
    "size_bytes",
    "width",
    "height",
    "content_sha256",
    "captured_at",
    "capture_tz_offset",
    "camera_make",
    "camera_model",
    "raw_metadata_json",
    "storage_backend",
    "storage_key",
    "storage_file_id",
    "storage_url",
    "metadata_storage_key",
    "metadata_storage_file_id",
    "metadata_storage_url",
    "metadata_content_sha256",
    "is_deleted",
    "ingested_at",
    "sync_version",
)

# One row per logical photo (photos.assets): the deduplicated identity that
# renditions from every source resolve into. Canonical fields are re-resolved
# by the identity runner whenever a new rendition links in.
PHOTO_ASSET_COLUMNS = (
    "photo_id",
    "account",
    "kind",
    "capture_ts",
    "capture_tz_offset",
    "latitude",
    "longitude",
    "camera_make",
    "camera_model",
    "width",
    "height",
    "best_file_sha256",
    "best_file_mime_type",
    "best_file_filename",
    "best_file_size_bytes",
    "thumbnail_content_sha256",
    "thumbnail_content_type",
    "thumbnail_size_bytes",
    "thumbnail_storage_backend",
    "thumbnail_storage_key",
    "thumbnail_storage_file_id",
    "thumbnail_storage_url",
    "created_at",
    "updated_at",
    "sync_version",
)

# Identity link + dedup audit (photos.asset_files): one row per raw file row,
# recording which asset it resolved into and why (match_method/match_score).
# Merges never mutate raw rows; deleting these links and re-running the
# identity asset replays every decision.
PHOTO_ASSET_FILE_COLUMNS = (
    "source",
    "account",
    "source_native_id",
    "role",
    "content_sha256",
    "photo_id",
    "match_method",
    "match_score",
    "created_at",
    "sync_version",
)

# Perceptual-hash cache (enrichment.media_fingerprints), keyed by content sha
# so any blob in the warehouse can be fingerprinted once. Deliberately not
# photo-named: a future linker may fingerprint message/mail attachments into
# the same table.
MEDIA_FINGERPRINT_COLUMNS = (
    "content_sha256",
    "hash_version",
    "dhash",
    "width",
    "height",
    "created_at",
    "sync_version",
)

AGENT_RUN_COLUMNS = (
    "run_id",
    "provider",
    "model",
    "task_type",
    "subject_id",
    "prompt_version",
    "status",
    "input_sha256",
    "final_output_json",
    "error",
    "exit_code",
    "started_at",
    "completed_at",
    "sync_version",
)

AGENT_RUN_EVENT_COLUMNS = (
    "run_id",
    "event_index",
    "stream",
    "event_type",
    "event_json",
    "text",
    "created_at",
    "sync_version",
)

AGENT_RUN_TOOL_CALL_COLUMNS = (
    "run_id",
    "event_index",
    "tool_name",
    "arguments_json",
    "result_json",
    "error",
    "started_at",
    "completed_at",
    "sync_version",
)

# AI conversation logs (Claude Code, Codex, OpenClaw, Claude Desktop, ChatGPT).
# One row per raw transcript/conversation event; source-owned physical tables
# share this row shape, and marts.ai_conversation_sessions provides the
# session-level roll-up so cross-batch counts and token sums stay correct.
AGENT_SESSION_EVENT_COLUMNS = (
    "source",
    "session_id",
    "event_uuid",
    "account",
    "device",
    "seq",
    "occurred_at",
    "role",
    "event_type",
    "subtype",
    "parent_uuid",
    "turn_id",
    "model",
    "cwd",
    "git_branch",
    "git_commit",
    "repo_url",
    "cli_version",
    "entrypoint",
    "session_title",
    "text",
    "tool_name",
    "tool_input_json",
    "tool_result_json",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_creation_tokens",
    "is_sidechain",
    "raw_json",
    "ingested_at",
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

WHOOP_PROFILE_COLUMNS = (
    "account",
    "whoop_user_id",
    "email",
    "first_name",
    "last_name",
    "raw_json",
    "synced_at",
    "sync_version",
)

WHOOP_BODY_MEASUREMENT_COLUMNS = (
    "account",
    "height_meter",
    "weight_kilogram",
    "max_heart_rate",
    "raw_json",
    "synced_at",
    "sync_version",
)

WHOOP_CYCLE_COLUMNS = (
    "account",
    "cycle_id",
    "whoop_user_id",
    "created_at",
    "updated_at",
    "start_at",
    "end_at",
    "timezone_offset",
    "score_state",
    "strain",
    "kilojoule",
    "average_heart_rate",
    "max_heart_rate",
    "score_json",
    "raw_json",
    "synced_at",
    "sync_version",
)

WHOOP_RECOVERY_COLUMNS = (
    "account",
    "cycle_id",
    "sleep_id",
    "whoop_user_id",
    "created_at",
    "updated_at",
    "score_state",
    "user_calibrating",
    "recovery_score",
    "resting_heart_rate",
    "hrv_rmssd_milli",
    "spo2_percentage",
    "skin_temp_celsius",
    "score_json",
    "raw_json",
    "synced_at",
    "sync_version",
)

WHOOP_SLEEP_COLUMNS = (
    "account",
    "sleep_id",
    "cycle_id",
    "v1_id",
    "whoop_user_id",
    "created_at",
    "updated_at",
    "start_at",
    "end_at",
    "timezone_offset",
    "nap",
    "score_state",
    "respiratory_rate",
    "sleep_performance_percentage",
    "sleep_consistency_percentage",
    "sleep_efficiency_percentage",
    "total_in_bed_time_milli",
    "total_awake_time_milli",
    "total_no_data_time_milli",
    "total_light_sleep_time_milli",
    "total_slow_wave_sleep_time_milli",
    "total_rem_sleep_time_milli",
    "sleep_cycle_count",
    "disturbance_count",
    "stage_summary_json",
    "sleep_needed_json",
    "score_json",
    "raw_json",
    "synced_at",
    "sync_version",
)

WHOOP_WORKOUT_COLUMNS = (
    "account",
    "workout_id",
    "v1_id",
    "whoop_user_id",
    "created_at",
    "updated_at",
    "start_at",
    "end_at",
    "timezone_offset",
    "sport_name",
    "sport_id",
    "score_state",
    "strain",
    "average_heart_rate",
    "max_heart_rate",
    "kilojoule",
    "percent_recorded",
    "distance_meter",
    "altitude_gain_meter",
    "altitude_change_meter",
    "zone_durations_json",
    "score_json",
    "raw_json",
    "synced_at",
    "sync_version",
)

WHOOP_SYNC_STATE_COLUMNS = (
    "account",
    "collection",
    "watermark_updated_at",
    "last_sync_type",
    "status",
    "error",
    "updated_at",
    "sync_version",
)

WHOOP_OAUTH_TOKEN_COLUMNS = (
    "account",
    "token_json",
    "updated_at",
)

GOOGLE_DRIVE_FILE_COLUMNS = (
    "account",
    "file_id",
    "drive_id",
    "name",
    "mime_type",
    "is_google_native",
    "parents_json",
    "folder_path",
    "parent_folder_id",
    "size_bytes",
    "md5_checksum",
    "content_sha256",
    "web_view_link",
    "icon_link",
    "owners_json",
    "last_modifying_user",
    "created_time",
    "modified_time",
    "viewed_by_me_time",
    "starred",
    "shared",
    "trashed",
    "is_excluded",
    "exclude_reason",
    "storage_backend",
    "storage_key",
    "storage_file_id",
    "storage_url",
    "storage_status",
    "raw_metadata_json",
    "ingested_at",
    "sync_version",
)

GOOGLE_DRIVE_FILE_TEXT_COLUMNS = (
    "account",
    "file_id",
    "content_sha256",
    "extractor",
    "extractor_version",
    "text",
    "text_extraction_status",
    "text_extraction_error",
    "char_count",
    "truncated",
    "source_modified_time",
    "extracted_at",
    "sync_version",
)

# Unified timeline: one normalized row per unit of activity anywhere in the
# warehouse (see personal_data_warehouse/timeline.py). `seq` is assigned from
# the timeline_events_seq sequence and bumped on every content change, giving
# consumers a durable "what's new since I last looked" arrival order.
TIMELINE_EVENT_COLUMNS = (
    "adapter",
    "event_id",
    "source",
    "kind",
    "priority",
    "event_ts",
    "end_ts",
    "actor",
    "title",
    "snippet",
    "context",
    "source_table",
    "source_pk",
    "metadata",
    "search_text",
    "ingest_ts",
    "seq",
    "first_seen_at",
    "updated_at",
)

TIMELINE_SYNC_STATE_COLUMNS = (
    "adapter",
    "backfill_cursor_event_ts",
    "backfill_cursor_event_id",
    "backfill_done",
    "backfill_rows",
    "incremental_rows",
    "watermark_ingest_ts",
    "watermark_event_id",
    "last_run_at",
    "last_error",
    "updated_at",
)

GOOGLE_DRIVE_SYNC_STATE_COLUMNS = (
    "account",
    "start_page_token",
    "last_page_token",
    "drive_id",
    "last_sync_type",
    "status",
    "error",
    "full_crawled_at",
    "files_seen",
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


@dataclass(frozen=True)
class GoogleDriveSyncState:
    account: str
    start_page_token: str
    last_page_token: str
    drive_id: str
    last_sync_type: str
    status: str
    error: str
    full_crawled_at: datetime
    files_seen: int
