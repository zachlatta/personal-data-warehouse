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


# Alice recordings are archived independently from Apple Voice Memos. Keep
# their source identity and recovery artifacts intact instead of folding them
# into apple_voice_memos.files, whose primary key has no source component.
ALICE_VOICE_RECORDING_COLUMNS = (
    "account",
    "recording_id",
    "title",
    "filename",
    "content_type",
    "size_bytes",
    "content_sha256",
    "recorded_at",
    "duration_seconds",
    "recording_page_url",
    "recovery_source",
    "storage_backend",
    "storage_key",
    "storage_file_id",
    "storage_url",
    "metadata_storage_key",
    "metadata_storage_file_id",
    "metadata_storage_url",
    "metadata_content_sha256",
    "raw_metadata_json",
    "ingested_at",
    "sync_version",
)


ALICE_VOICE_RECORDING_ARTIFACT_COLUMNS = (
    "account",
    "recording_id",
    "artifact_id",
    "kind",
    "filename",
    "content_type",
    "size_bytes",
    "content_sha256",
    "storage_backend",
    "storage_key",
    "storage_file_id",
    "storage_url",
    "raw_metadata_json",
    "ingested_at",
    "sync_version",
)

#: The Alice poller's run heartbeat. Alice is a DAILY Dagster poll against a
#: third-party API, not an uploader, so it has no ops.uploader_heartbeats row
#: and, until 2026-08-27, no run state of any kind -- data freshness was the
#: only signal it had. That cannot work for a source recorded a few times a
#: year: measured over 17 months, Zach used the device on 34 days with a
#: longest gap of 223 days, so any SLA tight enough to catch the poller dying
#: fires constantly on him simply not recording. One row per account.
ALICE_VOICE_RECORDINGS_SYNC_STATE_COLUMNS = (
    "account",
    "last_sync_type",
    "status",
    "error",
    "recordings_seen",
    "last_success_at",
    "updated_at",
    "sync_version",
)

# The three derived voice tables are keyed by ``source`` first, because voice
# is a MULTI-SOURCE domain and a recording_id is only unique inside its own
# source. Without it, base_alice_voice_recordings could not be transcribed at
# all: a second source's run would collide with an Apple run on
# (account, recording_id, provider) and silently overwrite it. That collision,
# plus transcription reading base_apple_voice_memos.files directly, is why
# Alice sat at 53 recordings with 0 transcripts for weeks.
VOICE_MEMO_TRANSCRIPTION_RUN_COLUMNS = (
    "source",
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

# A transcription attempt ends in one of three states, and the third one is the
# reason this distinction exists at all.
#
# ``error`` means the PROVIDER failed us and a retry may work -- a negative
# balance, a rate limit, a 5xx. ``rejected`` means the provider looked at the
# INPUT and will refuse it identically forever: a memo containing no speech, a
# recording too short to transcribe, a file that is not really audio. Both used
# to be written as ``error``, which made the pipeline's own health surface
# unable to tell an outage from a silent voice memo -- and since
# ``voice_memo_transcription`` counts error rows over the whole table with no
# time bound, ONE such recording pinned the pipeline to ``failing`` forever.
# Measured 2026-08-27: eleven rows, the oldest from 2026-05-01, every one of
# them a permanently unacceptable input, so the row was already red when the
# AssemblyAI billing outage arrived and the outage changed nothing.
#
# ``rejected`` is deliberately outside ``StateSource.error_statuses``, the same
# way slack's terminal ``gone`` is, so it stays terminal for the candidate query
# while never colouring the pipeline. This mirrors ``STATUS_UNREADABLE`` in
# file_attachment_enrichment.py, which solved the identical problem for
# attachments.
VOICE_MEMO_TRANSCRIPTION_STATUS_COMPLETED = "completed"
VOICE_MEMO_TRANSCRIPTION_STATUS_ERROR = "error"
VOICE_MEMO_TRANSCRIPTION_STATUS_REJECTED = "rejected"
VOICE_MEMO_TRANSCRIPTION_TERMINAL_STATUSES = (
    VOICE_MEMO_TRANSCRIPTION_STATUS_COMPLETED,
    VOICE_MEMO_TRANSCRIPTION_STATUS_REJECTED,
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


# The provider looked at the INPUT and will refuse it identically forever.
# This list is deliberately an ALLOW-LIST of recognised rejections rather than
# "anything the retryable list does not match", because the two mistakes are
# not symmetric: calling a permanent rejection retryable costs one wasted API
# call and a red row a human can act on, while calling a TRANSIENT failure
# permanent silently stops retrying that recording forever AND hides the
# failure from /pipelines. An unrecognised error therefore stays ``error`` --
# red, and someone classifies it.
PERMANENT_VOICE_MEMO_TRANSCRIPTION_REJECTION_PATTERNS = (
    "no spoken audio",
    "audio duration is too short",
    "does not appear to contain audio",
    "file does not appear to be",
    "audio file is too short",
    "transcoding failed",
)


def is_retryable_voice_memo_transcription_error(error: str) -> bool:
    """True when a transcription failure is worth attempting again.

    The pattern list is the single authority on this question, shared with the
    SQL candidate query, so the Python writer and the re-attempt filter can
    never drift into disagreeing about whether a given recording is finished.
    """
    haystack = str(error or "").lower()
    return any(
        pattern in haystack for pattern in RETRYABLE_VOICE_MEMO_TRANSCRIPTION_ERROR_PATTERNS
    )


def is_permanent_voice_memo_transcription_rejection(error: str) -> bool:
    """True only for a rejection of the AUDIO ITSELF, which no retry can fix.

    A retryable pattern always wins: a provider that names a transient reason
    is transient even if the sentence happens to contain a rejection word.
    """
    if is_retryable_voice_memo_transcription_error(error):
        return False
    haystack = str(error or "").lower()
    return any(
        pattern in haystack
        for pattern in PERMANENT_VOICE_MEMO_TRANSCRIPTION_REJECTION_PATTERNS
    )


def voice_memo_transcription_failure_status(error: str) -> str:
    """``rejected`` for a recognised impossible input, otherwise ``error``."""
    if is_permanent_voice_memo_transcription_rejection(error):
        return VOICE_MEMO_TRANSCRIPTION_STATUS_REJECTED
    return VOICE_MEMO_TRANSCRIPTION_STATUS_ERROR

VOICE_MEMO_TRANSCRIPT_SEGMENT_COLUMNS = (
    "source",
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
    "source",
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

# Unified deduped flow ledger (finance.transactions): one row per real-world
# money movement, resolved across sources (a Plaid transaction and the same
# transaction on an uploaded statement merge into one row; every source row
# gets a finance.transaction_links audit row). Amounts are signed NUMERIC,
# positive = inflow to the account (Plaid's positive-out amounts are negated
# at ingest). Facts only — categorization is a future enrichment layer. The
# ledger build reconciles this table to the current source rows each run and
# is fully replayable (deterministic ft_<sha> ids from the founding row).
FINANCE_TRANSACTION_COLUMNS = (
    "transaction_id",
    "account_id",
    "posted_at",
    "amount",
    "currency",
    "description",
    "merchant",
    "pending",
    "source",
    "created_at",
    "sync_version",
)

# Source-row → ledger-transaction resolution audit (finance.transaction_links):
# one row per source transaction row, recording which ledger row it resolved
# into and why (source_id / pending_id / fuzzy_amount_date / new).
FINANCE_TRANSACTION_LINK_COLUMNS = (
    "source",
    "source_row_key",
    "transaction_id",
    "match_method",
    "match_score",
    "created_at",
    "sync_version",
)

# Unified deduped SECURITY trade ledger (finance.security_transactions): one
# row per real-world share movement, resolved across sources. The cash ledger
# above records that money left the account; this records which security, how
# many shares, and at what price — the facts a purchase lot is made of, and
# the ones a v1 statement extraction threw away. Plaid only reaches back 730
# days, so the manual statement corpus is the sole source before that; the
# ~20-month overlap is deduped (same account/security/side/quantity within a
# few days, Plaid winning precedence) via finance.security_transaction_links.
# `price_is_derived` marks a price computed from amount/quantity because the
# document did not print one.
FINANCE_SECURITY_TRANSACTION_COLUMNS = (
    "transaction_id",
    "account_id",
    "security_key",
    "ticker",
    "cusip",
    "security_name",
    # spot | option. An option contract prints under the underlying's
    # ticker but is 100 shares, so it gets its own security_key and must stay
    # distinguishable from the stock in every read surface.
    "asset_class",
    "trade_date",
    "side",
    "quantity",
    # Price per quantity unit: per share for equities, per contract for options.
    "price",
    "amount",
    "fees",
    "currency",
    "price_is_derived",
    "source",
    "created_at",
    "sync_version",
)

# Source-row → security-trade resolution audit
# (finance.security_transaction_links): one row per source trade row recording
# which unified trade it resolved into and why (source_id when it founded the
# row, security_quantity_date when it merged into a Plaid twin).
FINANCE_SECURITY_TRANSACTION_LINK_COLUMNS = (
    "source",
    "source_row_key",
    "transaction_id",
    "match_method",
    "match_score",
    "created_at",
    "sync_version",
)

# Holding lots (finance.tax_lots): the FIFO reduction of the security trade
# ledger, one row per acquisition lot plus one per sale that had no
# acquisition to draw from. Derived and fully replayable — never hand-edited.
# `basis_known` is false when a lot was opened by a share transfer (its real
# basis lives at the origin account) or when no price was recorded, so a
# reader can tell a known cost from an absent one instead of seeing a
# confident zero. `method` records the lot-matching election used, because
# FIFO is a choice and the broker's own election governs at tax time.
FINANCE_TAX_LOT_COLUMNS = (
    "lot_id",
    "account_id",
    "security_key",
    "acquired_on",
    "acquired_source",
    "opening_transaction_id",
    "method",
    "quantity",
    "quantity_remaining",
    # Per share for equities, per contract for options.
    "cost_per_unit",
    "cost_basis",
    "cost_basis_remaining",
    "basis_known",
    "proceeds",
    "realized_gain",
    "disposed_on",
    "status",
    "term",
    "created_at",
    "sync_version",
)

# manual_finance: manually uploaded finance documents (bank/mortgage
# statements, Zillow screenshots, fund position docs, CSV/OFX exports). One
# row per uploaded document; `source` is 'manual', the native id is the
# content sha (a document IS its bytes), and `original_path` preserves the
# uploader's folder organization as an account-resolution hint (the folder
# name carries institution + account name + mask).
MANUAL_FINANCE_DOCUMENT_COLUMNS = (
    "source",
    "account",
    "source_native_id",
    "filename",
    "original_path",
    "mime_type",
    "size_bytes",
    "content_sha256",
    "file_modified_at",
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

# Agent-first structured extraction per document (voice-memos structured
# pattern): typed columns for the ledger-relevant facts plus the full agent
# payload in raw_result_json. Keyed by content sha + model/prompt version so
# a prompt bump re-extracts without clobbering prior results. The ledger
# consumes the explicit dated entries in transactions/balances/valuations
# arrays; the scalar closing_balance is a query convenience mirroring the
# period_end balance entry.
MANUAL_FINANCE_EXTRACTION_COLUMNS = (
    "content_sha256",
    "ai_provider",
    "ai_model",
    "ai_prompt_version",
    "status",
    "error",
    "document_type",
    "institution",
    "account_name_hint",
    "account_mask",
    # Whose money the document reports, and on what basis. A fund's own
    # unaudited financial statements and its investor's capital account
    # statement are the same shape in every other column, so without these a
    # partnership's total members' equity is indistinguishable from the
    # owner's holding -- which is how a fund's whole balance sheet became the
    # largest asset in a personal net worth on 2026-08-27.
    "reporting_scope",
    "account_holder",
    "value_basis",
    "period_start",
    "period_end",
    "currency",
    "closing_balance",
    "transactions_json",
    "balances_json",
    "valuations_json",
    "positions_json",
    # Capital commitments: the committed/called/unfunded triple a private
    # fund prints. Unfunded capital is a real future cash obligation and is
    # a fact of its own, not a balance.
    "commitments_json",
    "summary",
    "uncertainties_json",
    "raw_result_json",
    "ai_elapsed_ms",
    "ai_processed_at",
    "created_at",
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

# Slack file -> content sha link (derived_slack.file_fingerprints).
#
# base_slack.files cannot carry a content sha: a sha is only knowable after
# downloading the bytes, and the bytes live behind an authenticated
# files.slack.com URL. This table records the download's outcome so the
# ~905k-image / ~552 GB corpus can be walked in bounded, resumable slices --
# it IS the backfill cursor. The perceptual hash itself goes in the shared
# derived_enrichment.media_fingerprints table, keyed by that sha.
#
# The bytes are deliberately never stored: caching them would cost ~3000x what
# the "who sent this image?" answer needs, and the bytes stay one on-demand
# fetch away via url_private.
SLACK_FILE_FINGERPRINT_COLUMNS = (
    "account",
    "team_id",
    "file_id",
    "content_sha256",
    "hash_version",
    "status",
    "attempts",
    "fetched_bytes",
    "last_error",
    "last_attempt_at",
    "next_attempt_at",
    "created_at",
    "updated_at",
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
# share this row shape, and marts_ai_conversations.sessions provides the
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
    # Hash of the exact OAuth token rejected with a permanent auth error.
    # Schedules skip only while this still matches the configured/private
    # credential, then resume immediately after re-authorization rotates it.
    "credential_sha256",
)

WHOOP_OAUTH_TOKEN_COLUMNS = (
    "account",
    "token_json",
    "updated_at",
)

# --------------------------------------------------------------------------
# WHOOP private (app) API -- source `whoop_private`.
#
# The public developer API (WHOOP_* above) is summary grain. This source adds
# the time series app.whoop.com itself renders: per-6-second heart rate, the
# sleep hypnogram, journal entries, and the BFF "documents" that have no public
# endpoint at all. See docs/whoop-private-api.md.
#
# Two unit traps are baked into these names on purpose:
#   * the private API's HRV is in SECONDS, the public API's is milliseconds --
#     hence hrv_rmssd_seconds AND hrv_rmssd_milli side by side, never one bare
#     `hrv_rmssd` that a cross-source join can be wrong about by 1000x;
#   * `during`, `days` and `optimal_sleep_times` arrive as PostgreSQL range
#     literals and are split into explicit start/end columns here, because a
#     range stored as text is not orderable and not indexable.
# --------------------------------------------------------------------------

#: The private API reports ``hrv_rmssd`` in seconds; every other HRV number in
#: the warehouse (base_whoop.recoveries.hrv_rmssd_milli, and anything derived
#: from it) is milliseconds. Both are stored, and this is the only conversion.
WHOOP_PRIVATE_HRV_MILLI_PER_SECOND = 1000.0


def whoop_private_hrv_rmssd_milli(hrv_rmssd_seconds: float) -> float:
    """Convert the private API's seconds-grain HRV to the warehouse's millis.

    Kept as a named function rather than an inline ``* 1000`` so that the one
    place the two units meet is greppable and test-covered: a recovery whose
    ``hrv_rmssd_seconds`` is 0.0821 is 82.1 ms, and reading the seconds column
    as if it were the public API's milliseconds understates HRV by 1000x.
    """
    return float(hrv_rmssd_seconds) * WHOOP_PRIVATE_HRV_MILLI_PER_SECOND


WHOOP_PRIVATE_CYCLE_COLUMNS = (
    "account",
    "cycle_id",
    "whoop_user_id",
    # `during` split: the cycle runs sleep-onset to next sleep-onset, so
    # start_at is NOT midnight and end_at carries the epoch sentinel while the
    # cycle is still running (data_state/predicted_end are the cleaner signal).
    "start_at",
    "end_at",
    # `days` split: the user-local calendar day(s) the cycle is awake for.
    "day_start",
    "day_end",
    "day_strain",
    "scaled_strain",
    "day_kilojoules",
    "day_avg_heart_rate",
    "day_max_heart_rate",
    "intensity_score",
    "sleep_need",
    "predicted_end",
    "data_state",
    "timezone_offset",
    "created_at",
    "updated_at",
    "raw_json",
    "synced_at",
    "sync_version",
)

WHOOP_PRIVATE_SLEEP_COLUMNS = (
    "account",
    "activity_id",
    "cycle_id",
    "whoop_user_id",
    "start_at",
    "end_at",
    "is_nap",
    "score",
    "state",
    "latency",
    "arousal_time",
    "total_wake_events",
    "in_sleep_efficiency",
    "debt_pre",
    "debt_post",
    "habitual_sleep_need",
    "credit_from_naps",
    "need_from_strain",
    "quality_duration",
    "light_sleep_duration",
    "slow_wave_sleep_duration",
    "rem_sleep_duration",
    "wake_duration",
    "no_data_duration",
    "time_in_bed",
    "disturbances",
    "cycles_count",
    "respiratory_rate",
    "sleep_consistency",
    "projected_score",
    "projected_sleep",
    # `optimal_sleep_times` split, same reason as `during` above.
    "optimal_sleep_start",
    "optimal_sleep_end",
    "algo_version",
    "survey_response_id",
    "timezone_offset",
    "created_at",
    "updated_at",
    "raw_json",
    "synced_at",
    "sync_version",
)

WHOOP_PRIVATE_RECOVERY_COLUMNS = (
    "account",
    "activity_id",
    "recovery_score",
    "resting_heart_rate",
    # The private API's own unit. See whoop_private_hrv_rmssd_milli.
    "hrv_rmssd_seconds",
    # The same measurement in the unit every other WHOOP relation uses, so a
    # cross-source query cannot silently be off by 1000x.
    "hrv_rmssd_milli",
    "skin_temp_celsius",
    "spo2",
    "calibrating",
    "prob_covid",
    "hr_baseline",
    "hrv_component",
    "rhr_component",
    "recovery_rate",
    "state",
    "algo_version",
    "history_size",
    "survey_response_id",
    "created_at",
    "updated_at",
    "raw_json",
    "synced_at",
    "sync_version",
)

WHOOP_PRIVATE_WORKOUT_COLUMNS = (
    "account",
    "activity_id",
    "sport_id",
    "start_at",
    "end_at",
    "score",
    "intensity_score",
    "raw_intensity_score",
    "cumulative_workout_intensity",
    "kilojoules",
    "average_heart_rate",
    "max_heart_rate",
    "percent_recorded",
    "total_steps",
    "msk_score",
    "zone_durations_json",
    "zone_durations_v2_json",
    "gps_data_json",
    "source",
    "survey_response_id",
    "timezone_offset",
    "created_at",
    "updated_at",
    "raw_json",
    "synced_at",
    "sync_version",
)

WHOOP_PRIVATE_SLEEP_EVENT_COLUMNS = (
    "account",
    "activity_id",
    # Position in the hypnogram, so the stages stay ordered without depending
    # on a float timestamp comparison.
    "event_index",
    # LIGHT / REM / SWS / DISTURBANCES, WHOOP's `type`.
    "stage",
    "started_at",
    "ended_at",
    "raw_json",
    "synced_at",
    "sync_version",
)

WHOOP_PRIVATE_HEART_RATE_SAMPLE_COLUMNS = (
    "account",
    "sample_at",
    "heart_rate",
    # 6, 60 or 600 -- the metrics-service `step` the sample was fetched at.
    # Kept per row because a backfill may coarsen old windows.
    "step_seconds",
    "raw_json",
    "synced_at",
    "sync_version",
)

WHOOP_PRIVATE_JOURNAL_ENTRY_COLUMNS = (
    "account",
    # User-local calendar day the entry was logged for, not an instant.
    "day",
    "question_id",
    "question_text",
    "answer",
    "behavior_id",
    "raw_json",
    "synced_at",
    "sync_version",
)

WHOOP_PRIVATE_SPORT_COLUMNS = (
    "account",
    "sport_id",
    "name",
    "category",
    "has_gps",
    "has_survey",
    "activity_type_internal_name",
    "is_current",
    "raw_json",
    "synced_at",
    "sync_version",
)

WHOOP_PRIVATE_DOCUMENT_COLUMNS = (
    "account",
    # trend / stress / cardio_details / sleep_deep_dive / health_tab ...
    "kind",
    # metric name, date, or activity id -- whatever identifies this document
    # within its kind.
    "doc_key",
    "collected_at",
    # Tier-2 BFF payloads are UI documents: faithful raw only, never typed
    # columns, because WHOOP restyles them without notice.
    "raw_json",
    "synced_at",
    "sync_version",
)

WHOOP_PRIVATE_SYNC_STATE_COLUMNS = (
    "account",
    "collection",
    "watermark_updated_at",
    "last_sync_type",
    "status",
    "error",
    "updated_at",
    "sync_version",
    # Hash of the exact session rejected with a permanent auth error, so the
    # schedule skips only while that same dead credential is still installed.
    "credential_sha256",
    # What this collection's stored rows depend on beyond the window they cover
    # -- today, the heart-rate grain. A run that reads a different signature
    # restarts that collection's backfill instead of resuming a cursor that has
    # already reached its floor, which is the only way a grain change reaches
    # rows the walk is finished with. Same contract as timeline adapter_signature.
    # Appended, never inserted: the writer below positions its values by index.
    "collection_signature",
)

#: The captured browser session. This tuple is the Python half of a table the
#: app also writes: ``app/internal/whoopsession/store.go`` creates the
#: idempotent twin so the very first ``pdw whoop publish-session`` succeeds
#: before any poll has run. The two definitions must agree column for column --
#: the app's upsert names ``ON CONFLICT (account, session_key)``, so even the
#: primary key is part of the contract.
# The Slack *client* session: an xoxc token plus the `d` cookie, which are
# useless apart. It exists because Slack's public API has no bulk "what changed"
# call -- see slack_session.py -- and client.counts does, but only for a real
# signed-in session.
SLACK_SESSION_COLUMNS = (
    "account",
    "session_key",
    "session_token",
    "session_cookie",
    # Non-secret fingerprint of the token, so sync state can record which
    # credential was rejected without storing the secret a second time.
    "token_sha256",
    # Hack Club is Enterprise Grid, so the session authenticates against the ORG
    # and auth.test returns an E-id. Every warehouse row is keyed by the
    # workspace T-id, so the two are stored apart on purpose; conflating them
    # would silently fork the dataset. See slack_session.py.
    "team_id",
    "enterprise_id",
    "user_id",
    "team_url",
    "source_app",
    "cookie_expires_at",
    "published_at",
    "updated_at",
    "sync_version",
    "status",
    "error",
)

WHOOP_PRIVATE_SESSION_COLUMNS = (
    "account",
    "session_key",
    "access_token",
    # Rotates on EVERY refresh. Mutating this row outside
    # PostgresWarehouse.rotate_whoop_private_session / .replace_... is how a
    # rotating credential gets lost; see docs/whoop-oauth-operations.md.
    "refresh_token",
    "access_expires_at",
    "refresh_expires_at",
    # Fingerprint of the live refresh token: lets sync state record *which*
    # credential was rejected without ever storing the secret twice.
    "refresh_token_sha256",
    "source_browser",
    # When a browser capture last published this session (a human logged in).
    "published_at",
    "updated_at",
    "sync_version",
    "status",
    "error",
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

# Derived search-retrieval layer (personal_data_warehouse/search_index.py):
# chunked documents behind timeline.search_hybrid(). One row per chunk; the
# anchor ties a chunk back to the timeline rows it was built from so a changed
# event replaces exactly its own chunks.
SEARCH_CHUNK_COLUMNS = (
    "chunk_id",
    "anchor",
    "adapter",
    "event_id",
    "source",
    "context",
    "event_ts",
    "chunk_index",
    "text",
    "text_sha256",
    "char_count",
    "built_at",
)

# One embedding per distinct chunk text per model — identical text (repeated
# messages, re-chunked docs) is embedded and paid for once. The vector column
# itself (halfvec) is added conditionally when the pgvector extension is
# available, so this spec stays creatable on hosts that predate the install.
SEARCH_CHUNK_EMBEDDING_COLUMNS = (
    "text_sha256",
    "model",
    "token_count",
    "embedded_at",
)

# One row per search-index stage. The chunk builder's row ("timeline") is a
# cursor over timeline.events.seq. The embedding drain's row ("embeddings")
# carries its two persisted cursors: the built_at watermark behind which every
# chunk has been offered to the embedder, and the newest-first (event_ts,
# chunk_id) keyset of the one-time historical backfill. Without persistence
# the drain restarted at the newest chunk on every run and re-scanned the
# whole 7 GB chunk heap each time, which evicted the search indexes from the
# page cache ten minutes after every search warmed them.
SEARCH_CHUNK_SYNC_STATE_COLUMNS = (
    "id",
    "last_seq",
    "updated_at",
    "embed_fresh_built_at",
    "embed_cursor_ts",
    "embed_cursor_id",
    "embed_backfill_status",
)

# One row per search stage.  Unlike generic table freshness this records the
# convergence facts that determine whether hybrid retrieval is complete.
SEARCH_HEALTH_COLUMNS = (
    "component",
    "model",
    "configured",
    "pgvector_available",
    "timeline_max_seq",
    "chunk_cursor_seq",
    "caught_up",
    "processed_rows",
    # Exact when caught_up=1 (zero); -1 means the bounded worker proved a
    # backlog exists but deliberately did not scan millions of rows to count it.
    "pending_count",
    "oldest_pending_at",
    "last_success_at",
    "last_run_at",
    "last_error",
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
    # sha256 of the adapter's normalization SQL at the last sync. A changed
    # definition resets the backfill cursor so historical rows converge to the
    # new shape (the content-guarded upsert makes an unchanged row a no-op).
    "adapter_signature",
    # When the coverage reconcile last ran for this adapter. The pass is an
    # anti-join over an ingest window, so its cost is the window's size and NOT
    # the number of gaps it finds: measured 2026-08-26, slack_message took 24s
    # to sweep 48h whether it repaired 62,891 rows or none. Gating it on this
    # column keeps a sweep that must be wide from running every few minutes.
    "last_reconcile_at",
)

# Pipeline freshness snapshot (see personal_data_warehouse/pipeline_health.py).
# Facts only: measured timestamps plus the registry metadata that gives them
# meaning. Status is derived live in the marts_ops views, because "ok" computed
# at collection time would still read "ok" days after the collector died.
PIPELINE_HEALTH_COLUMNS = (
    "pipeline",
    "label",
    "kind",
    "cadence",
    "transport",
    "note",
    # Where expected_data_interval_seconds came from. A long SLA that nobody can
    # re-derive is a number that rots; this is the audit trail for it.
    "data_basis",
    "expected_data_interval_seconds",
    "expected_run_interval_seconds",
    # How far behind the newest REAL-WORLD event may fall. Usually the same as
    # the data interval, but the finance ledger dates observations by day, so
    # its event time trails its writes while working perfectly. Zero means no
    # data table declares an event column at all — unmonitored, not late.
    "expected_event_interval_seconds",
    # Newest payload write, newest real-world event, and newest run heartbeat.
    "last_write_at",
    "newest_event_at",
    "last_run_at",
    # Data tables that actually yielded an event timestamp. Zero alongside a
    # nonzero expectation means the columns exist but were too expensive to
    # probe: unmeasured, which is not the same claim as "nothing ever arrived".
    "event_tables_probed",
    "row_estimate",
    "byte_size",
    "table_count",
    "tables_probed",
    "tables_skipped",
    # Rolled up from the pipeline's declared sync-state table, when it has one.
    "state_table",
    "state_rows",
    "state_error_rows",
    "state_attention_rows",
    "last_error",
    "last_error_at",
    "collected_at",
)

PIPELINE_TABLE_FRESHNESS_COLUMNS = (
    "table_id",
    "pipeline",
    "role",
    "layer",
    "table_schema",
    "table_name",
    "written_at_column",
    "event_at_column",
    "last_write_at",
    "newest_event_at",
    "row_estimate",
    "byte_size",
    # Why a timestamp is missing is as important as the timestamp: an unprobed
    # 50 GB heap and a genuinely empty table look identical otherwise.
    "probe_status",
    "probe_detail",
    "probe_ms",
    "note",
    "collected_at",
)

# Mart (view) health — level 2 of the health contract. A view has no stamped
# column to take a max() of and no relpages to bound a probe with, so it is
# measured on the three things that ARE cheap and true about it: how fresh the
# stalest relation it reads is, whether it currently returns a row, and whether
# its definition changed. See personal_data_warehouse/pipeline_health.py.
MART_VIEW_HEALTH_COLUMNS = (
    "view_id",
    "domain",
    "view_schema",
    "view_name",
    # Base tables this view reads, resolved transitively from pg_depend at
    # collection time rather than from a hand-written map, and the pipelines
    # they belong to — which is what actually gets judged.
    "input_tables",
    "input_pipelines",
    "input_count",
    # The input PIPELINE furthest past its own SLA, with the interval it was
    # judged against so the verdict can be re-derived live. Per pipeline rather
    # than per table on purpose: a pipeline's freshness is already a max() over
    # its data tables, so judging one quiet table against the whole pipeline's
    # interval invents staleness (measured: four marts read 'stale' off a
    # perfectly healthy finance ledger).
    "stalest_pipeline",
    "stalest_pipeline_at",
    "stalest_pipeline_expected_seconds",
    "inputs_unmeasured",
    "has_rows",
    "definition_sha256",
    # When THIS definition hash was first observed: a silent redefinition that
    # drops a source table changes nothing measurable about the rows.
    "first_seen_at",
    "probe_status",
    "probe_detail",
    "probe_ms",
    "note",
    "collected_at",
)

# Collation drift and index integrity (personal_data_warehouse/collation_health.py).
# One row per checked object across three scopes: the database's own collation,
# the named collations an index actually depends on, and the unique indexes the
# corroborating divergence probe could afford.
#: One row per pgBackRest stanza, written by the backup loop INSIDE the Postgres
#: container -- the only process that can see both pgBackRest and the warehouse.
#: The Dagster collector runs elsewhere and cannot shell out to pgbackrest, which
#: is precisely why backups appeared in no health surface at all: on 2026-08-26
#: production reported `status: error (no valid backups)` and had for a day,
#: while WAL archiving kept working, every pipeline read green, and the loop
#: logged "backup failed" to stdout every six hours where nothing escalated it.
# The weekly search benchmark, one row per retrieval mode: serial latency
# over fixed probe queries and labeled quality (MRR, hit@k) through the app's
# own search tool. mrr is stored x1000 as a bigint; the marts view divides.
#
# The four host columns are the C6 half of the row: whether the machine was
# saturated WHILE the latency probes ran. Measured 2026-08-28 during three
# concurrent hybrid searches on mew-coolify: CPU 42% idle / 38% iowait, PSI
# io full avg10 20%, the semantic ANN leg ~100% shared-buffer-read wait --
# I/O-bound, not CPU-bound -- and nothing recorded that beside the latency
# number. Sampled from /proc/pressure/{io,cpu}, /proc/loadavg and
# os.cpu_count() at the start and end of the probes; the worse sample is
# stored. -1 means the file was unreadable (no PSI on this kernel, or not
# Linux) and the note says so; the view reads it as `unmeasured`.
SEARCH_BENCHMARK_RUN_COLUMNS = (
    "mode",
    "probe_queries",
    "latency_p50_ms",
    "latency_p90_ms",
    "latency_max_ms",
    "labeled_cases",
    "found",
    "hit_at_1",
    "hit_at_5",
    "hit_at_10",
    "mrr_milli",
    "errors",
    "note",
    "io_pressure_full_avg10",
    "cpu_pressure_some_avg10",
    "load_1m",
    "cpu_count",
    "collected_at",
)

# The benchmark's labels, kept in the warehouse (private: they are Zach's own
# queries and timeline refs) so losing a gitignored directory can no longer
# make retrieval quality unmeasurable. Loaded with `search_benchmark
# publish-labels`, exported with `pull-labels`.
SEARCH_BENCHMARK_LABEL_COLUMNS = (
    "query",
    "stratum",
    "verdict",
    "truth_refs_json",
    "truth_predicate_json",
    "sources_json",
    "since",
    "note",
    "updated_at",
    "sync_version",
)

# One row per agent source (plus 'all') measuring how agents use PDW over a
# trailing window, from their own transcripts: what their first PDW call was,
# how often searches carried a priority filter, how much SQL bypassed the
# timeline. Contract C3, as a number that is re-taken daily.
AGENT_USAGE_COLUMNS = (
    "source",
    "window_days",
    "sessions",
    "pdw_sessions",
    "first_search",
    "first_schema",
    "first_sql",
    "first_invented",
    "search_calls",
    "search_with_priority",
    "sql_calls",
    "sql_base_only",
    "sql_error_sessions",
    "sql_timeouts",
    "invented_calls",
    "newest_session_at",
    "collected_at",
)

# One row per (source, priority tier) over the last seven days of
# timeline.events, taken by the pipeline_health collector. Contract C2 says
# every row is classified into one of five tiers; this is the surface that
# shows the classification actually happening, per source, so a tier that
# silently swallows a source (or an `unclassified` row) is a red row rather
# than a search that comes back full of newsletters.
TIMELINE_PRIORITY_MIX_COLUMNS = (
    "source",
    "priority",
    "events_7d",
    "events_1d",
    "newest_event_at",
    "collected_at",
)

# One row per (pipeline, device) from the machines that push data through the
# app's /ingest/* endpoints: the run's own verdict, posted by the upload
# wrapper after the uploader exits. It is the only in-warehouse heartbeat a
# laptop uploader has; without it a run that fires every five minutes and
# fails every time is indistinguishable from a quiet source.
UPLOADER_HEARTBEAT_COLUMNS = (
    "pipeline",
    "device",
    "ran_at",
    "status",
    "error",
    "exit_code",
    "duration_seconds",
    "updated_at",
    "sync_version",
)

PGBACKREST_HEALTH_COLUMNS = (
    "stanza",
    # pgBackRest's own words, from `info --output=json`: "ok", "error", or the
    # message it prints when the repository holds no valid backup.
    "repo_status",
    "repo_message",
    # Newest backup of each type. Absent is the epoch sentinel, per the
    # warehouse-wide convention, and the marts view translates it back to NULL.
    "last_full_at",
    "last_diff_at",
    "last_incr_at",
    "last_backup_label",
    "last_backup_type",
    "backup_count",
    "repo_bytes",
    # WAL continuity: archiving can be healthy while no base backup exists, and
    # reporting only one of them is how this stayed invisible.
    "wal_min",
    "wal_max",
    # The archive backlog: unarchived .ready segments. pg_stat_archiver cannot
    # express this -- archived_count and failed_count both climbed normally
    # through the 2026-08-26 incident while the queue reached 5,910 segments,
    # because WAL was shipping, just slower than it was produced.
    "wal_ready_count",
    "archived_count",
    "failed_count",
    "last_archived_at",
    # The loop's own attempt, which is NOT the same question as whether a valid
    # backup exists: a failing loop with an old good backup and a succeeding
    # loop with none are different emergencies.
    "last_attempt_at",
    "last_attempt_type",
    "last_attempt_ok",
    "last_error",
    "collected_at",
    # The restore drill. A backup nobody has restored is a hypothesis, and
    # until 2026-08-28 the only evidence a restore had ever worked was a
    # commit message. Written by `personal_data_warehouse.pgbackrest_restore_drill`
    # after a restore into a throwaway cluster has been counted, never by the
    # backup loop (which must not overwrite it).
    "last_restore_verified_at",
    "last_restore_label",
    "last_restore_rows",
    "last_restore_note",
)


COLLATION_HEALTH_COLUMNS = (
    "object_id",
    "scope",
    "object_name",
    "provider",
    # '' when pg_database.datcollversion / pg_collation.collversion is NULL —
    # which IS the finding here, not a neutral state.
    "recorded_version",
    "actual_version",
    "dependent_indexes",
    "finding",
    "detail",
    # Index-scope columns.
    "table_name",
    "is_unique",
    "is_partial",
    # The partial predicate. Ignoring it made a clean index report 53,035
    # phantom excess rows, so it is stored as evidence of what was counted.
    "predicate",
    "key_columns",
    "heap_rows",
    "distinct_keys",
    "excess_rows",
    "probe_ms",
    # Rigorous, bounded-rotation btree structural verification. This is separate from the
    # duplicate-key corroboration above: amcheck sees mis-ordering even when it
    # has not produced duplicates, including indexes whose heaps are too large
    # for the count(DISTINCT) probe.
    "amcheck_status",
    "amcheck_detail",
    "amcheck_ms",
    "amcheck_at",
    "collected_at",
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


# receipts: one agent research result per recent ledger transaction.
#
# Search, source validation, extraction, and matching happen in one operation.
# Negative results are durable and retryable; trusted positive results carry
# the consolidated receipt facts in the same row. There is deliberately no
# archive-wide artifact triage or separate receipt-to-transaction link table.
RECEIPT_TRANSACTION_RECEIPT_COLUMNS = (
    "transaction_id",
    "record_id",
    "decision",
    "reasoning",
    "sources_searched_json",
    "primary_source",
    "primary_native_id",
    "evidence_json",
    "occurred_at",
    "purchased_at",
    "merchant_name",
    "merchant_location",
    "currency",
    "total",
    "subtotal",
    "tax",
    "tip",
    "amount_charged",
    "card_last4",
    "order_id",
    "line_items_json",
    "summary",
    "record_confidence",
    "match_confidence",
    "match_reason",
    "attempt_count",
    "last_attempt_at",
    "settled",
    "raw_result_json",
    "ai_provider",
    "ai_model",
    "ai_prompt_version",
    "ai_elapsed_ms",
    "ai_processed_at",
    "agent_run_id",
    "created_at",
    "updated_at",
    "sync_version",
)
