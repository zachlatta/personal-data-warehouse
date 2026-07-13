from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re


SOURCE_RAW_SCHEMAS: tuple[str, ...] = (
    "gmail",
    "google_calendar",
    "google_contacts",
    "google_drive",
    "plaid",
    "slack",
    "apple_notes",
    "apple_messages",
    "apple_voice_memos",
    "apple_photos",
    "alice_voice_recordings",
    "whoop",
    "whatsapp",
    "chatgpt",
    "claude_desktop",
    "claude_code",
    "codex",
    "openclaw",
    "pi",
)

DERIVED_SCHEMAS: tuple[str, ...] = (
    "marts",
    "timeline",
    "search",
    "enrichment",
    "photos",
    "ai_processing",
    "upstream_mutations",
    "util",
)

PRIVATE_SCHEMAS: tuple[str, ...] = ("private",)

QUERYABLE_SCHEMAS: tuple[str, ...] = SOURCE_RAW_SCHEMAS + DERIVED_SCHEMAS
ALL_CANONICAL_SCHEMAS: tuple[str, ...] = QUERYABLE_SCHEMAS + PRIVATE_SCHEMAS


@dataclass(frozen=True)
class Relation:
    logical_name: str
    schema: str
    name: str

    def with_namespace(self, namespace: str = "public") -> "Relation":
        return Relation(
            logical_name=self.logical_name,
            schema=physical_schema_name(self.schema, namespace=namespace),
            name=self.name,
        )

    def sql(self, namespace: str = "public") -> str:
        rel = self.with_namespace(namespace)
        return f"{quote_identifier(rel.schema)}.{quote_identifier(rel.name)}"


# Canonical relation names keyed by the legacy logical names used in the Python
# warehouse code. The physical table/view names intentionally drop duplicated
# source prefixes because the schema now owns the source namespace.
_CANONICAL_RELATION_ROWS: tuple[tuple[str, str, str], ...] = (
    # Gmail
    ("gmail_messages", "gmail", "messages"),
    ("gmail_attachments", "gmail", "attachments"),
    ("gmail_sync_state", "gmail", "sync_state"),
    ("gmail_attachment_backfill_state", "gmail", "attachment_backfill_state"),
    # Google Calendar / Contacts / Drive
    ("calendar_events", "google_calendar", "events"),
    ("calendar_sync_state", "google_calendar", "sync_state"),
    ("contact_cards", "google_contacts", "cards"),
    ("contact_sync_state", "google_contacts", "sync_state"),
    ("google_drive_files", "google_drive", "files"),
    ("google_drive_file_texts", "google_drive", "file_texts"),
    ("google_drive_sync_state", "google_drive", "sync_state"),
    # WHOOP
    ("whoop_profiles", "whoop", "profiles"),
    ("whoop_body_measurements", "whoop", "body_measurements"),
    ("whoop_cycles", "whoop", "cycles"),
    ("whoop_recoveries", "whoop", "recoveries"),
    ("whoop_sleeps", "whoop", "sleeps"),
    ("whoop_workouts", "whoop", "workouts"),
    ("whoop_sync_state", "whoop", "sync_state"),
    # Apple Voice Memos
    ("apple_voice_memos_files", "apple_voice_memos", "files"),
    ("apple_voice_memos_transcription_runs", "apple_voice_memos", "transcription_runs"),
    ("apple_voice_memos_transcript_segments", "apple_voice_memos", "transcript_segments"),
    ("apple_voice_memos_enrichments", "apple_voice_memos", "enrichments"),
    # Apple Notes
    ("apple_notes", "apple_notes", "notes"),
    ("apple_note_revisions", "apple_notes", "revisions"),
    ("apple_note_attachments", "apple_notes", "attachments"),
    # Apple Messages
    ("apple_message_handles", "apple_messages", "handles"),
    ("apple_message_chats", "apple_messages", "chats"),
    ("apple_message_chat_handles", "apple_messages", "chat_handles"),
    ("apple_messages", "apple_messages", "messages"),
    ("apple_message_chat_messages", "apple_messages", "chat_messages"),
    ("apple_message_attachments", "apple_messages", "attachments"),
    # Photos: per-source raw file tables (apple_photos now; each future photo
    # source adds its own <source>.files row here) unified through the derived
    # photos.* identity tables and the marts views below.
    ("apple_photos_files", "apple_photos", "files"),
    ("photo_assets", "photos", "assets"),
    ("photo_asset_files", "photos", "asset_files"),
    ("media_fingerprints", "enrichment", "media_fingerprints"),
    # WhatsApp
    ("whatsapp_chats", "whatsapp", "chats"),
    ("whatsapp_chat_participants", "whatsapp", "chat_participants"),
    ("whatsapp_contacts", "whatsapp", "contacts"),
    ("whatsapp_messages", "whatsapp", "messages"),
    ("whatsapp_media_items", "whatsapp", "media_items"),
    # Source-owned AI conversation raw tables. The historical mixed
    # agent_session_events table is intentionally absent; ingest splits by source
    # and marts.ai_conversation_events re-unifies them.
    ("chatgpt_events", "chatgpt", "events"),
    ("chatgpt_conversation_sync", "chatgpt", "conversation_sync"),
    ("claude_desktop_events", "claude_desktop", "events"),
    ("claude_desktop_conversation_state", "claude_desktop", "conversation_state"),
    ("claude_code_events", "claude_code", "events"),
    ("codex_events", "codex", "events"),
    ("openclaw_events", "openclaw", "events"),
    ("pi_events", "pi", "events"),
    # Plaid source data
    ("plaid_items", "plaid", "items"),
    ("plaid_accounts", "plaid", "accounts"),
    ("plaid_transactions", "plaid", "transactions"),
    ("plaid_investment_securities", "plaid", "investment_securities"),
    ("plaid_investment_holdings", "plaid", "investment_holdings"),
    ("plaid_investment_transactions", "plaid", "investment_transactions"),
    ("plaid_liabilities", "plaid", "liabilities"),
    ("plaid_sync_state", "plaid", "sync_state"),
    # Slack
    ("slack_teams", "slack", "teams"),
    ("slack_account_identities", "slack", "account_identities"),
    ("slack_users", "slack", "users"),
    ("slack_conversations", "slack", "conversations"),
    ("slack_conversation_members", "slack", "conversation_members"),
    ("slack_messages", "slack", "messages"),
    ("slack_conversation_stats", "slack", "conversation_stats"),
    ("slack_message_reactions", "slack", "message_reactions"),
    ("slack_files", "slack", "files"),
    ("slack_sync_state", "slack", "sync_state"),
    ("slack_account_state_item_rows", "slack", "account_state_item_rows"),
    # Derived / cross-source
    ("clean_gmail_inbox", "marts", "gmail_inbox"),
    ("clean_slack_inbox", "marts", "slack_inbox"),
    ("clean_contacts", "marts", "google_contacts"),
    ("clean_whatsapp_messages", "marts", "whatsapp_messages"),
    ("clean_agent_sessions", "marts", "ai_conversation_sessions"),
    ("ai_conversation_events", "marts", "ai_conversation_events"),
    ("photo_files", "marts", "photo_files"),
    ("clean_photos", "marts", "photos"),
    ("photo_canonical_renditions", "marts", "photo_canonical_renditions"),
    ("clean_calendar_with_transcripts", "marts", "google_calendar_with_apple_voice_memos"),
    ("clean_transcripts_no_calendar_match", "marts", "apple_voice_memos_without_calendar_match"),
    ("file_attachment_enrichments", "enrichment", "file_attachment_enrichments"),
    ("agent_runs", "ai_processing", "agent_runs"),
    ("agent_run_events", "ai_processing", "agent_run_events"),
    ("agent_run_tool_calls", "ai_processing", "agent_run_tool_calls"),
    ("upstream_mutation_requests", "upstream_mutations", "requests"),
    ("upstream_mutations", "upstream_mutations", "operations"),
    ("upstream_mutation_events", "upstream_mutations", "operation_events"),
    ("upstream_mutation_request_events", "upstream_mutations", "request_events"),
    ("timeline_events", "timeline", "events"),
    ("timeline_sync_state", "timeline", "sync_state"),
    ("timeline_gmail_correspondents", "timeline", "gmail_correspondents"),
    ("timeline_events_seq", "timeline", "events_seq"),
    ("search_text_hit", "search", "text_hit"),
    ("search_text", "search", "search_text"),
    ("search_text_sources", "search", "search_text_sources"),
    ("search_schema_state", "search", "schema_state"),
    # Private/control-plane secrets and session snapshots
    ("chatgpt_sessions", "private", "chatgpt_sessions"),
    ("claude_desktop_credentials", "private", "claude_desktop_credentials"),
    ("whatsapp_client_sessions", "private", "whatsapp_client_sessions"),
    ("whoop_oauth_tokens", "private", "whoop_oauth_tokens"),
    ("plaid_item_tokens", "private", "plaid_item_tokens"),
)

CANONICAL_RELATIONS: dict[str, Relation] = {
    logical: Relation(logical_name=logical, schema=schema, name=name)
    for logical, schema, name in _CANONICAL_RELATION_ROWS
}

# Internal SQL written before the split still references agent_session_events.
# That is not a canonical physical relation; it is a derived read surface over
# source-owned AI event tables.
LEGACY_QUERY_ALIASES: dict[str, str] = {
    "agent_session_events": "ai_conversation_events",
}

AI_EVENT_SOURCE_RELATIONS: dict[str, str] = {
    "chatgpt": "chatgpt_events",
    "claude_desktop": "claude_desktop_events",
    "claude_code": "claude_code_events",
    "codex": "codex_events",
    "openclaw": "openclaw_events",
    "pi": "pi_events",
}

# THE extension point for photo sources. Maps a photo source slug (the
# `source` field every /ingest/photos/* envelope carries) to its raw file
# table. This single registry drives Drive-inbox ingest routing, the identity
# runner's unresolved-row scan, and the marts.photo_files union — adding a
# photo source is: a new <source>.files TableSpec (reusing
# PHOTO_SOURCE_FILE_COLUMNS) + relation rows above, one entry here, an
# uploader that posts the shared photo envelope with its own `source`, and a
# TIMELINE_TABLE_COVERAGE entry. Identity, dedup, thumbnails, enrichment,
# timeline, and search then follow automatically.
PHOTO_SOURCE_RELATIONS: dict[str, str] = {
    "apple_photos": "apple_photos_files",
}


def relation(logical_name: str) -> Relation:
    try:
        return CANONICAL_RELATIONS[logical_name]
    except KeyError as exc:
        raise KeyError(f"unknown warehouse relation {logical_name!r}") from exc


def query_relation(logical_name: str) -> Relation:
    return relation(LEGACY_QUERY_ALIASES.get(logical_name, logical_name))


def qualify_sql_relations(sql: str, *, namespace: str = "public") -> str:
    """Replace legacy logical relation tokens with canonical schema-qualified names.

    This is a transitional safety net for the large pre-existing SQL surface in
    postgres.py and timeline.py. It rewrites identifiers outside string/comment
    literals, and also rewrites quoted identifiers whose entire contents are a
    known logical relation name. It intentionally does not create public views or
    aliases; the SQL sent to Postgres names the canonical schemas directly.
    """
    names = set(CANONICAL_RELATIONS) | set(LEGACY_QUERY_ALIASES)
    if not any(name in sql for name in names):
        return sql

    def mapped(name: str) -> str | None:
        try:
            return query_relation(name).sql(namespace=namespace)
        except KeyError:
            return None

    out: list[str] = []
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]

        if ch == "'":
            start = i
            i += 1
            while i < n:
                if sql[i] == "'":
                    i += 1
                    if i < n and sql[i] == "'":
                        i += 1
                        continue
                    break
                i += 1
            out.append(sql[start:i])
            continue

        if ch == '"':
            start = i
            i += 1
            buf: list[str] = []
            while i < n:
                if sql[i] == '"':
                    i += 1
                    if i < n and sql[i] == '"':
                        buf.append('"')
                        i += 1
                        continue
                    break
                buf.append(sql[i])
                i += 1
            quoted_name = "".join(buf)
            replacement = mapped(quoted_name)
            # search_text is both the public search function's logical name and
            # a timeline column. Quoted identifiers here are columns/types, not
            # function calls, so never rewrite the quoted column name.
            if quoted_name == "search_text":
                replacement = None
            if replacement and not _adjacent_to_dot(sql, start, i):
                out.append(replacement)
            else:
                out.append(sql[start:i])
            continue

        if ch == "-" and i + 1 < n and sql[i + 1] == "-":
            start = i
            i += 2
            while i < n and sql[i] != "\n":
                i += 1
            out.append(sql[start:i])
            continue

        if ch == "/" and i + 1 < n and sql[i + 1] == "*":
            start = i
            i += 2
            while i + 1 < n and not (sql[i] == "*" and sql[i + 1] == "/"):
                i += 1
            i = min(n, i + 2)
            out.append(sql[start:i])
            continue

        if _is_ident_start(ch):
            start = i
            i += 1
            while i < n and _is_ident_part(sql[i]):
                i += 1
            token = sql[start:i]
            replacement = mapped(token)
            # search_text is also a timeline column. Only its function-call
            # spelling should resolve to search.search_text; bare/quoted column
            # definitions and index expressions must remain untouched.
            if token == "search_text" and not _followed_by_open_paren(sql, i):
                replacement = None
            if replacement and not _adjacent_to_dot(sql, start, i):
                out.append(replacement)
            else:
                out.append(token)
            continue

        out.append(ch)
        i += 1
    return "".join(out)


def physical_schema_name(schema: str, *, namespace: str = "public") -> str:
    _validate_identifier(schema)
    _validate_identifier(namespace)
    if namespace in {"", "public"}:
        return schema
    combined = f"{namespace}_{schema}"
    if len(combined) <= 63:
        return combined
    # Postgres silently truncates identifiers past NAMEDATALEN-1 (63 bytes),
    # which would collapse every test schema that shares a long namespace into
    # the same physical schema. Keep the pdw_test_ timestamp prefix for the leak
    # reaper, include a namespace hash for uniqueness, and preserve the canonical
    # schema suffix for readability.
    digest = hashlib.sha1(namespace.encode("utf-8")).hexdigest()[:8]
    max_prefix = 63 - len(schema) - len(digest) - 2
    if max_prefix < 1:
        raise ValueError(f"schema name is too long for Postgres identifier: {schema!r}")
    return f"{namespace[:max_prefix]}_{digest}_{schema}"


def physical_schema_names(*, namespace: str = "public", include_private: bool = False) -> list[str]:
    schemas = ALL_CANONICAL_SCHEMAS if include_private else QUERYABLE_SCHEMAS
    return [physical_schema_name(schema, namespace=namespace) for schema in schemas]


def quote_identifier(value: str) -> str:
    return '"' + _validate_identifier(value).replace('"', '""') + '"'


def _validate_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"invalid SQL identifier: {value!r}")
    return value


def _is_ident_start(ch: str) -> bool:
    return ch == "_" or ch.isalpha()


def _is_ident_part(ch: str) -> bool:
    return ch == "_" or ch.isalpha() or ch.isdigit()


def _followed_by_open_paren(sql: str, end: int) -> bool:
    after = end
    while after < len(sql) and sql[after].isspace():
        after += 1
    return after < len(sql) and sql[after] == "("


def _adjacent_to_dot(sql: str, start: int, end: int) -> bool:
    before = start - 1
    while before >= 0 and sql[before].isspace():
        before -= 1
    after = end
    while after < len(sql) and sql[after].isspace():
        after += 1
    return (before >= 0 and sql[before] == ".") or (after < len(sql) and sql[after] == ".")
