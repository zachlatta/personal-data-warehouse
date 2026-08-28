from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
import hashlib
import inspect
import json
import logging
import os
import re
import time
from typing import Any

import psycopg2
from psycopg2 import Binary
from psycopg2.extras import Json, execute_values

from personal_data_warehouse.search_benchmark_runner import (
    LATENCY_P50_TARGET_MS,
    MRR_FLOOR,
    SATURATION_CPU_SOME_AVG10,
    SATURATION_IO_FULL_AVG10,
)
from personal_data_warehouse.agent_usage import (
    PRIORITY_FILTER_TARGET,
    SEARCH_FIRST_TARGET,
    SQL_ERROR_SESSION_CEILING,
)
from personal_data_warehouse.schema import (
    ALICE_VOICE_RECORDING_ARTIFACT_COLUMNS,
    ALICE_VOICE_RECORDINGS_SYNC_STATE_COLUMNS,
    ALICE_VOICE_RECORDING_COLUMNS,
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
    FINANCE_ACCOUNT_COLUMNS,
    FINANCE_ACCOUNT_LINK_COLUMNS,
    FINANCE_OBSERVATION_COLUMNS,
    FINANCE_SECURITY_TRANSACTION_COLUMNS,
    FINANCE_SECURITY_TRANSACTION_LINK_COLUMNS,
    FINANCE_TAX_LOT_COLUMNS,
    FINANCE_TRANSACTION_COLUMNS,
    FINANCE_TRANSACTION_LINK_COLUMNS,
    MANUAL_FINANCE_DOCUMENT_COLUMNS,
    MANUAL_FINANCE_EXTRACTION_COLUMNS,
    RECEIPT_TRANSACTION_RECEIPT_COLUMNS,
    PLAID_ACCOUNT_COLUMNS,
    PLAID_INVESTMENT_HOLDING_COLUMNS,
    PLAID_INVESTMENT_SECURITY_COLUMNS,
    PLAID_INVESTMENT_TRANSACTION_COLUMNS,
    PLAID_ITEM_COLUMNS,
    PLAID_LIABILITY_COLUMNS,
    PLAID_ITEM_TOKEN_COLUMNS,
    PLAID_SYNC_STATE_COLUMNS,
    PLAID_TRANSACTION_COLUMNS,
    PlaidLinkedItem,
    GOOGLE_DRIVE_FILE_COLUMNS,
    GOOGLE_DRIVE_FILE_TEXT_COLUMNS,
    GOOGLE_DRIVE_SYNC_STATE_COLUMNS,
    MEDIA_FINGERPRINT_COLUMNS,
    MESSAGE_COLUMNS,
    PHOTO_ASSET_COLUMNS,
    PHOTO_ASSET_FILE_COLUMNS,
    PHOTO_SOURCE_FILE_COLUMNS,
    COLLATION_HEALTH_COLUMNS,
    PGBACKREST_HEALTH_COLUMNS,
    UPLOADER_HEARTBEAT_COLUMNS,
    TIMELINE_PRIORITY_MIX_COLUMNS,
    AGENT_USAGE_COLUMNS,
    SEARCH_BENCHMARK_LABEL_COLUMNS,
    SEARCH_BENCHMARK_RUN_COLUMNS,
    MART_VIEW_HEALTH_COLUMNS,
    PIPELINE_HEALTH_COLUMNS,
    PIPELINE_TABLE_FRESHNESS_COLUMNS,
    PERMANENT_VOICE_MEMO_TRANSCRIPTION_REJECTION_PATTERNS,
    RETRYABLE_VOICE_MEMO_TRANSCRIPTION_ERROR_PATTERNS,
    VOICE_MEMO_TRANSCRIPTION_TERMINAL_STATUSES,
    SLACK_ACCOUNT_IDENTITY_COLUMNS,
    SLACK_ACCOUNT_STATE_ITEM_ROW_COLUMNS,
    SLACK_CONVERSATION_COLUMNS,
    SLACK_CONVERSATION_MEMBER_COLUMNS,
    SLACK_CONVERSATION_READ_STATE_FIELDS,
    SLACK_FILE_COLUMNS,
    SLACK_FILE_FINGERPRINT_COLUMNS,
    SLACK_MESSAGE_COLUMNS,
    SLACK_REACTION_COLUMNS,
    SLACK_SYNC_STATE_COLUMNS,
    SLACK_TEAM_COLUMNS,
    SEARCH_CHUNK_COLUMNS,
    SEARCH_CHUNK_EMBEDDING_COLUMNS,
    SEARCH_HEALTH_COLUMNS,
    SEARCH_CHUNK_SYNC_STATE_COLUMNS,
    SLACK_USER_COLUMNS,
    SYNC_STATE_COLUMNS,
    TIMELINE_EVENT_COLUMNS,
    TIMELINE_SYNC_STATE_COLUMNS,
    VOICE_MEMO_ENRICHMENT_COLUMNS,
    VOICE_MEMO_FILE_COLUMNS,
    VOICE_MEMO_TRANSCRIPTION_RUN_COLUMNS,
    VOICE_MEMO_TRANSCRIPT_SEGMENT_COLUMNS,
    WHATSAPP_CHAT_COLUMNS,
    WHATSAPP_CHAT_PARTICIPANT_COLUMNS,
    WHATSAPP_CONTACT_COLUMNS,
    WHATSAPP_MEDIA_ITEM_COLUMNS,
    WHATSAPP_MESSAGE_COLUMNS,
    WHOOP_BODY_MEASUREMENT_COLUMNS,
    WHOOP_CYCLE_COLUMNS,
    WHOOP_OAUTH_TOKEN_COLUMNS,
    WHOOP_PRIVATE_CYCLE_COLUMNS,
    WHOOP_PRIVATE_DOCUMENT_COLUMNS,
    WHOOP_PRIVATE_HEART_RATE_SAMPLE_COLUMNS,
    WHOOP_PRIVATE_JOURNAL_ENTRY_COLUMNS,
    WHOOP_PRIVATE_RECOVERY_COLUMNS,
    SLACK_SESSION_COLUMNS,
    WHOOP_PRIVATE_SESSION_COLUMNS,
    WHOOP_PRIVATE_SLEEP_COLUMNS,
    WHOOP_PRIVATE_SLEEP_EVENT_COLUMNS,
    WHOOP_PRIVATE_SPORT_COLUMNS,
    WHOOP_PRIVATE_SYNC_STATE_COLUMNS,
    WHOOP_PRIVATE_WORKOUT_COLUMNS,
    WHOOP_PROFILE_COLUMNS,
    WHOOP_RECOVERY_COLUMNS,
    WHOOP_SLEEP_COLUMNS,
    WHOOP_SYNC_STATE_COLUMNS,
    WHOOP_WORKOUT_COLUMNS,
    GoogleDriveSyncState,
    SyncState,
)
from personal_data_warehouse.chatgpt_backend import token_expiry_warning
from personal_data_warehouse.config import normalize_postgres_url
from personal_data_warehouse.pipeline_health import (
    ACCOUNT_BASELINE_GAPS,
    ACCOUNT_BASELINE_MAX_DAYS,
    ACCOUNT_BASELINE_PERCENTILE,
    ACCOUNT_LATE_MULTIPLIER,
    ACCOUNT_MIN_BASELINE_GAPS,
    ACCOUNT_MIN_EXPECTED_GAP_SECONDS,
    ACCOUNT_STALE_MULTIPLIER,
    COLLATION_SNAPSHOT_STALE_SECONDS,
    COLLECTOR_STALE_SECONDS,
    PGBACKREST_SNAPSHOT_STALE_SECONDS,
    PGBACKREST_RESTORE_DRILL_STALE_SECONDS,
    WAL_READY_ATTENTION,
    WAL_READY_FAILING,
    EPOCH as PIPELINE_HEALTH_EPOCH,
    LATE_MULTIPLIER,
    STALE_MULTIPLIER,
)
from personal_data_warehouse.relations import (
    ALL_CANONICAL_SCHEMAS,
    CANONICAL_RELATIONS,
    CATALOG,
    PHOTO_SOURCE_RELATIONS,
    expand_relations,
    physical_schema_name,
    physical_schema_names,
    relation as canonical_relation,
)
# The tier definitions belong to the timeline's classifier, so they are
# imported rather than restated here; this module only publishes them as
# Postgres COMMENTs. timeline.py imports nothing from postgres.py, so there is
# no cycle.
from personal_data_warehouse.timeline import (
    TIMELINE_PRIORITY_DEFINITIONS,
    timeline_context_branch_sql,
)

logger = logging.getLogger(__name__)

POSTGRES_TEXT_NUL_REPLACEMENT = "\\u0000"
# A search_text() hit's `text` is a relevance PREVIEW, not the full document.
# Timeline search documents can include multi-megabyte source/detail text
# (Google Drive docs, transcripts, email bodies, attachment enrichments).
# search_text() array_agg's each source branch's top-k text into an intermediate
# plpgsql array before the final cross-source rank+limit, so carrying untrimmed
# text can move tens of MB through that array. Capping each branch's contributed
# text keeps the array small while preserving a generous preview; the caller
# fetches full content via the returned timeline `ref`.
SEARCH_TEXT_PREVIEW_CHARS = 8000
# How much of a document the preview windowing scans for the first match.
# Unbounded scanning meant lower()+strpos over ENTIRE multi-MB TOASTed
# documents per returned hit — measured at ~7s and 4M buffer reads for one
# broad search_text() call. Beyond this prefix the preview falls back to the
# head cut, which is what it would have shown anyway before windowing.
SEARCH_TEXT_PREVIEW_SCAN_CHARS = 200_000
# search_text() still runs one timeline branch per coarse source. Even though
# those branches share one BM25 corpus, a single flat `ORDER BY score LIMIT n`
# would let high-volume sources (gmail/slack) crowd out one matching contact card
# or Drive doc. The merge guarantees each source's top-SEARCH_TEXT_SOURCE_FLOOR
# hits survive the cut, then fills the remaining slots by score.
SEARCH_TEXT_SOURCE_FLOOR = 3
# Per-branch top-k cap for a BROAD (unscoped) search_text() call. The score
# column recomputes the bm25 operator per returned row, whose cost is
# rows x tokenize(text); on huge timeline documents (Drive docs, transcripts,
# attachment/media enrichments) scoring 50 multi-kB docs dominates latency. A
# broad search never needs that depth -- the cross-source merge keeps only each
# source's top-SEARCH_TEXT_SOURCE_FLOOR hits plus a global score fill -- so capping each
# branch to this many rows bounds the recompute with no change to the merged
# output. A scoped search (sources => ARRAY[...]) bypasses the cap and uses the
# full max_results so a single-source deep search still returns everything.
SEARCH_TEXT_BROAD_PER_BRANCH_CAP = 12
# A BROAD (unscoped) search_text() call does not fan out over per-source
# branches at all: it pools candidates from index-ordered scans of the same
# BM25 index and applies the per-source floor to that pool. The fan-out was
# eighteen EXECUTEs in a strictly serial plpgsql loop, so its wall clock was
# the SUM of every branch -- 6.9s warm and 21.7s cold on the production corpus,
# while one index-ordered scan returns the global top 200 in 36ms. Two
# partitions, not one: a single flat scan is dominated by the high-volume
# sources, and the floor cannot promote a low-volume hit the pool never
# contained. The low-volume partition costs ~143ms and replaces ~4.8s of
# branch scans.
# Pool depth is a measured trade, not a guess. On the labeled benchmark,
# 2000/300 scored MRR 0.278, and 5000/800 scored 0.292 with hit@5 11 -> 12 and
# hit@10 12 -> 13: a deeper pool gives the per-source floor more to promote.
# 10000/1500 scored the same as 5000/800 on every hit@k (it answers one more
# query somewhere inside the top 50) for twice the added latency -- serial p50
# 0.46s at 2000, 0.63s at 5000, 1.15s at 10000 -- so 5000 is where the curve
# flattens. Re-measure with search_benchmark before moving it.
SEARCH_TEXT_BROAD_POOL = 5000
SEARCH_TEXT_BROAD_SMALL_POOL = 800
# Which BM25 index a pooled row was scanned through, carried on the row so the
# ranking stage can score a candidate against the same corpus statistics the
# merge was built on. Two partitions means two bm25 corpora, and a low-volume
# row scored against the global index is a different number.
SEARCH_TEXT_POOL_PART_HIGH_VOLUME = 0
SEARCH_TEXT_POOL_PART_LOW_VOLUME = 1
# The same two partitions, scanned through the ATTENTION indexes instead of the
# general ones. Separate part numbers because they are separate BM25 corpora:
# a row scored against the attention index's term statistics is a different
# number than the same row scored against the global index's.
SEARCH_TEXT_POOL_PART_ATTENTION_HIGH_VOLUME = 2
SEARCH_TEXT_POOL_PART_ATTENTION_LOW_VOLUME = 3
# The tiers the partial "attention" BM25 indexes contain.
#
# `priorities => ARRAY['self']` is the single most important query an agent
# makes -- it is what contract C3 ("agents start at the timeline and can filter
# by priority") is for -- and it was the slowest thing in the search layer.
# Measured on production 2026-08-26: `self` is 496,049 of 49,010,739 rows
# (1.01%) and `self` + `direct` is 1,333,799 (2.72%). A broad pool wants 5,000
# candidates, so filling it from the global index walks ~500k score-ordered
# documents and pays a RANDOM HEAP VISIT on each one to read the tier -- 15-20s
# cold on a novel query, and past the app's 60s statement ceiling through the
# multi-leg hybrid. The same pool taken from an index that contains only those
# tiers is a shallow scan of a small corpus.
#
# Why these two tiers and no more: they are 7.9% of the corpus by document
# bytes (1,451 MB of 18.4 GB), so the partial index is a fraction of the global
# index rather than a second copy of it. Adding `cc` would pull in 6.9M more
# rows and most of that argument.
SEARCH_TEXT_ATTENTION_PRIORITIES: tuple[str, ...] = ("self", "direct")
SEARCH_TEXT_ATTENTION_PRIORITIES_SQL = ", ".join(
    f"'{priority}'" for priority in SEARCH_TEXT_ATTENTION_PRIORITIES
)
# Sources whose documents dominate the corpus, and therefore the global BM25
# ordering. Everything else is scanned as the second pool partition.
SEARCH_TEXT_HIGH_VOLUME_SOURCES: tuple[str, ...] = (
    "agent_session",
    "gmail",
    "google_drive",
    "imessage",
    "slack",
    "whatsapp",
)
# Hard ceiling on max_results for both search functions. Production logs showed
# broad overfetch (max_results 500-1000, then client-side filtering) as a
# dominant slow-query family; no legitimate agent flow reads deeper than this,
# and callers needing exhaustive results should scope by sources/since instead.
SEARCH_TEXT_MAX_RESULTS_CAP = 200
# The declarative source map both search functions are generated from: one
# entry per coarse source token as (token, timeline adapters, subsource
# expression). Keeping this the single source of truth guarantees ranked and
# exact search accept the same `sources` vocabulary.
SEARCH_SOURCE_DEFS: tuple[tuple[str, tuple[str, ...], str], ...] = (
    ("agent_session", ("agent_session", "agent_session_turn"), "t.source"),
    ("calendar", ("calendar_event",), "t.kind"),
    ("contact", ("contact_update", "apple_contact_update"), "t.kind"),
    ("gmail", ("gmail_email",), "t.kind"),
    ("google_drive", ("drive_file",), "t.kind"),
    ("imessage", ("apple_message",), "t.kind"),
    (
        "finance",
        ("finance_transaction", "finance_observation", "manual_finance_document"),
        "t.kind",
    ),
    ("mutation", ("mutation",), "COALESCE(t.metadata->>'status', t.kind)"),
    ("mutation_request", ("mutation_request",), "COALESCE(t.metadata->>'status', t.kind)"),
    ("note", ("apple_note_revision",), "t.kind"),
    ("photo", ("photo",), "t.kind"),
    ("slack", ("slack_message",), "t.kind"),
    ("slack_file", ("slack_file",), "t.kind"),
    # ONE adapter covers every voice source (it reads
    # marts_voice_memos.recordings), so `transcript` scopes all of them. The
    # retired per-source token survives as an alias below, so a scoped call
    # written against the old registry still resolves.
    ("transcript", ("voice_memo",), "t.kind"),
    ("warehouse", ("enrichment_run",), "t.kind"),
    ("whatsapp", ("whatsapp_message",), "t.kind"),
    ("whoop", ("whoop_cycle", "whoop_recovery", "whoop_sleep", "whoop_workout"), "t.kind"),
    # The private (app) API contributes exactly one adapter: the journal. Its
    # cycles/sleeps/recoveries/workouts are the same real-world events the
    # public API already puts on the timeline, so re-emitting them would double
    # every health event.
    ("whoop_private", ("whoop_private_journal",), "t.kind"),
)
# The adapters the broad-search pool scans through the low-volume partial BM25
# index, derived from the source map so a new source cannot be forgotten.
SEARCH_TEXT_LOW_VOLUME_ADAPTERS: tuple[str, ...] = tuple(
    sorted(
        adapter
        for source, adapters, _ in SEARCH_SOURCE_DEFS
        if source not in SEARCH_TEXT_HIGH_VOLUME_SOURCES
        for adapter in adapters
    )
)
SEARCH_TEXT_LOW_VOLUME_ADAPTERS_SQL = ", ".join(
    f"'{adapter}'" for adapter in SEARCH_TEXT_LOW_VOLUME_ADAPTERS
)
# Familiar-name aliases the search functions accept for `sources` tokens. The
# canonical tokens are terse and diverge from every other name in the warehouse
# ('imessage' not 'apple_messages', 'note' not 'apple_notes', 'transcript' not
# 'voice_memos'), and production agent sessions show the same wrong guesses
# recurring for months. An alias resolves to its canonical token before
# validation; anything not in this map and not a canonical token still raises.
# Every key must stay disjoint from the canonical token set (test-enforced).
SEARCH_SOURCE_ALIASES: dict[str, str] = {
    "agent_sessions": "agent_session",
    "alice": "transcript",
    "alice_voice_recording": "transcript",
    "alice_voice_recordings": "transcript",
    "apple_message": "imessage",
    "apple_messages": "imessage",
    "apple_note": "note",
    "apple_notes": "note",
    "calendar_event": "calendar",
    "calendar_events": "calendar",
    "contacts": "contact",
    "drive": "google_drive",
    "email": "gmail",
    "emails": "gmail",
    "gdrive": "google_drive",
    "imessages": "imessage",
    "mutation_requests": "mutation_request",
    "mutations": "mutation",
    "notes": "note",
    "photos": "photo",
    "slack_files": "slack_file",
    "transcripts": "transcript",
    "voice_memo": "transcript",
    "voice_memos": "transcript",
    "whatsapp_media": "whatsapp",
    "whoop_cycle": "whoop",
    "whoop_recovery": "whoop",
    "whoop_sleep": "whoop",
    "whoop_workout": "whoop",
    "whoop_journal": "whoop_private",
    "whoop_private_journal": "whoop_private",
}
# Hybrid retrieval (search_hybrid): reciprocal-rank-fusion constant. Rank-based
# fusion sidesteps the cross-corpus score-comparability problem entirely — BM25
# scores and cosine distances never meet, only ranks do. 60 is the standard
# RRF k from the literature; it dampens the head so one branch's #1 cannot
# drown the other branch's top few.
SEARCH_HYBRID_RRF_K = 60
# The literal leg now protects exact lexical answers that an earlier 1.5x
# experiment drowned. Re-measured against the independent 26-label benchmark,
# 1.5 improved three ranks across all three query strata with zero regressions
# (MRR 0.429 -> 0.446); 1.7 crossed the safe boundary and regressed two labels.
SEARCH_HYBRID_SEMANTIC_WEIGHT = 1.0
# The BM25 head bonus for a query that is NOT sentence-shaped (a term bag or
# an identifier): its top-ranked lexical hits count double. Re-measured on
# the 73-case benchmark with the offline fusion lab
# (scripts/search_fusion_lab.py) on 2026-08-26: four ANN legs each return
# hundreds of candidates and the old 1.5 semantic weight let semantic ranks
# 1-16 outvote a correct BM25 #1 on every term bag (keyword alone: hit@1 7/20,
# hybrid: 2/20). Semantic 1.0 + literal 3.0 + this head bonus took hybrid from
# MRR 0.339 / hit@1 15 / hit@10 40 to 0.394 / 20 / 44 on 67 scored queries
# with no loss of found@50 (51 both ways); flat weights (lexical 2, semantic
# 0.5) scored MRR 0.400 but lost seven found@50, which is why the bonus is a
# head bonus and not a flat weight. Sentence-shaped queries keep 1.0 lexical
# throughout: their BM25 head is where the wrong answers live.
SEARCH_HYBRID_LEXICAL_HEAD_WEIGHT = 2.0
# The BM25 health probe's query: common, mixed-frequency terms so the scan
# touches many posting lists across the index rather than one.
BM25_PROBE_QUERY = "meeting order email update warehouse magazine budget photo call plan"
# The function words the app's sentence detector counts (searchSentenceWords in
# app/internal/query/search.go); the fuse repeats the test in SQL so the
# direct-SQL wrapper and the app agree on which queries get the head bonus.
SEARCH_SENTENCE_WORDS = (
    "a", "an", "the", "my", "our", "your", "their", "is", "are", "was", "were", "will",
    "would", "can", "of", "for", "with", "that", "this", "at", "on", "in", "to", "from",
    "and", "or", "by", "about", "how", "what", "when", "where", "why", "who", "which",
    "did", "does", "do", "should", "could", "me", "i",
)
SEARCH_SENTENCE_WORDS_SQL = ", ".join("'" + w + "'" for w in SEARCH_SENTENCE_WORDS)
SEARCH_HYBRID_LEXICAL_HEAD_RANKS = 5
# Filtered ANN recall depends on asking the iterative scan for a deep enough
# qualifying pool. A 4x pool was adequate at 30 days but became unstable at 90
# days because the global HNSW index had three times as many filtered-out rows.
# Hybrid retrieval also runs a LITERAL-SUBSTRING leg, but only for a short
# query. Identifier-shaped questions ("admin/api-keys", a Drive file id, a
# person's name) are exactly where BM25 tokenization and embeddings both fail
# and literal matching wins: on the labeled benchmark the exact MODE scores MRR
# 0.518 on that stratum against hybrid's 0.245. Folding it in as a third fused
# leg took hybrid from MRR 0.292 to 0.403, hit@5 12 -> 15, and answered three
# queries that previously had nothing in the top 50.
#
# It is gated on query length because literal matching is not free, and a
# natural-language question gains nothing from it -- ungated it scored *worse*
# (0.374) while making every long query pay. Machine tokens search the bounded
# retrieval chunks; ordinary names retain the full-document exact path because
# chunk anchoring regressed one proper-name label. Weight 2 because a literal
# match on a short query is strong evidence, where a rank-1 BM25 hit on two
# common words is not.
SEARCH_HYBRID_EXACT_WEIGHT = 3.0
SEARCH_HYBRID_EXACT_MAX_WORDS = 3
# search_text_exact() raises below this needle length, so the leg must not be
# attempted for a shorter query.
SEARCH_HYBRID_EXACT_MIN_CHARS = 3
SEARCH_HYBRID_CANDIDATE_MULTIPLIER = 20
SEARCH_HYBRID_MIN_CANDIDATES = 1000
SEARCH_HYBRID_MAX_CANDIDATES = 2000
# Agent-session chunks are only 3.05% of the global HNSW. Asking that index for
# 1000 qualifying rows made each query-vector leg scan ~97k embeddings and take
# 31.2s; two legs time out before fusion. LIMIT 40 took 2.25s. Agent sessions
# have p95 three chunks/event, so a 4x pool with a 40-row floor still gives a
# depth-10 search enough distinct event candidates. Drive keeps the deeper pool
# but obtains it with the source-first exact path below; every other scope uses
# the global HNSW until it gets its own evidence.
SEARCH_HYBRID_AGENT_CANDIDATE_MULTIPLIER = 4
SEARCH_HYBRID_AGENT_MIN_CANDIDATES = 40
SEARCH_HYBRID_AGENT_MAX_CANDIDATES = 200
# Embedding space for the semantic branch. 512-dim halfvec keeps ~10M chunks
# around 10 GB of vectors; models are OpenAI-compatible `/v1/embeddings`
# (cloud OpenAI, Gemini's compat endpoint, or a self-hosted server), requested
# with dimensions=512 (Matryoshka truncation).
SEARCH_EMBEDDING_DIMENSIONS = 512
SEARCH_EMBEDDING_DEFAULT_MODEL = "text-embedding-3-small"
# Timeline rows every search must exclude, beyond the per-row deleted flag.
SEARCH_DRIVE_EXCLUSION_SQL = (
    "NOT (t.adapter = 'drive_file' "
    "AND (COALESCE((t.metadata->>'trashed')::boolean, false) "
    "OR COALESCE((t.metadata->>'excluded')::boolean, false)))"
)
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
SEARCH_SCHEMA_REFRESH_LOCK_ID = 8_407_112_465
# Serializes _ensure_query_role's shared GRANT/REVOKEs across processes.
# Distinct from TIMELINE_SYNC_POSTGRES_LOCK_ID, which held the same id: the two
# happen to live in different databases today (this one on the warehouse, that
# one on Dagster's), so they never actually contended, but advisory-lock ids
# have to stay globally unique or moving either lock silently deadlocks it
# against the other. test_advisory_lock_ids_are_unique enforces that.
QUERY_ROLE_SETUP_LOCK_ID = 8_407_112_469
# The advisory lock above only excludes other privilege sweeps. Ordinary DDL
# (CREATE TABLE, CREATE OR REPLACE VIEW) from unrelated processes touches the
# same pg_class rows, and Postgres reports the collision as "tuple concurrently
# updated". Retry rather than failing a whole sensor tick or asset run.
QUERY_ROLE_SETUP_ATTEMPTS = 4
QUERY_ROLE_SETUP_RETRY_SECONDS = 0.25
QUERY_ROLE_CONCURRENT_UPDATE_MESSAGE = "tuple concurrently updated"
# Serializes every mutation of WHOOP's single-use rotating credential,
# including first bootstrap, scheduled refresh, direct CLI refresh, and
# explicit reauthorization. A row lock alone cannot serialize the first insert.
WHOOP_TOKEN_AUTHORITY_LOCK_ID = 8_407_112_472
# The same discipline for the WHOOP *private* (app API) browser session. Its
# refresh token rotates on every single refresh -- see
# docs/whoop-private-api.md -- so two unsynchronized refreshes leave one winner
# and one caller holding a credential the next refresh will reject. Distinct id
# from the public credential's: the two rotate independently and must not
# serialize against each other.
WHOOP_PRIVATE_SESSION_AUTHORITY_LOCK_ID = 8_407_112_476

# Single-flight guard for the Slack inbox snapshot (derived_slack.inbox_items).
# Every Slack stage refreshes it at the end of sync_all(); before this lock four
# copies of the refresh queued behind each other for up to eleven minutes.
# One waiter is pointless: the refresh that holds the lock produces the same
# snapshot the waiter would have, so a contender skips instead of queueing.
SLACK_ACCOUNT_STATE_REFRESH_LOCK_ID = 8_407_112_478

# How far the incremental inbox refresh looks back past its own watermark. A
# sync stage stamps every row with the `synced_at` it computed when it STARTED,
# then may run for minutes before committing, so rows can land carrying a stamp
# older than a refresh that already ran. The overlap re-reads that window; a
# stage longer than this is caught by the daily full refresh below.
#
# Measured 2026-08-28 on production: with a one-hour overlap every incremental
# call re-selected ~1,085 "changed" conversations -- 280 of them member
# channels holding ~400k messages in the 30-day window, re-stamped by the
# five-minute polls whether or not anything arrived -- and took 26s, 66 times
# an hour, the largest statement on the host and the reason the search working
# set kept being evicted. A ten-minute window selected 178. Fifteen minutes
# still covers a stage that stamps at start and commits minutes later.
SLACK_ACCOUNT_STATE_REFRESH_OVERLAP = timedelta(minutes=15)
# Every Slack stage ends by refreshing, so the refresh ran up to 66 times an
# hour for a snapshot whose inputs change every five minutes. A refresh that
# ran this recently is skipped; the next one's overlap covers the gap.
SLACK_ACCOUNT_STATE_REFRESH_DEBOUNCE = timedelta(minutes=5)
SLACK_ACCOUNT_STATE_FULL_REFRESH_INTERVAL = timedelta(hours=24)
SLACK_ACCOUNT_STATE_REFRESH_OBJECT_TYPE = "account_state_refresh"
SLACK_ACCOUNT_STATE_ITEM_WINDOW = timedelta(days=30)

# How fast a direct message must LAND to read `ok` on
# marts_ops.slack_conversation_health, as the p95 of
# timeline.events.first_seen_at - event_ts over the last 24 hours of im/mpim
# messages. The numbers come from the 2026-08-28 production audit: 1:1 DMs
# landed p50 3.5 min / p95 62 min and group DMs p50 46 min, with multi-hour
# tails where a whole conversation arrived in one batch an hour or more after
# it was written -- while every Slack pipeline row read green, because nothing
# measured landing at all. The ok bound is what the machinery promises when it
# works: a five-minute freshness tick plus a five-minute timeline sync, with
# one tick of slack. `late` is one hour, the batch shape the audit found;
# anything past that is the DM outage this row exists to name. Judged only for
# im and mpim: a public channel Zach is not in is polled by the sweep on a
# rotation measured in days, and its landing time is a rate budget, not a
# fault.
SLACK_DM_LANDING_P95_SECONDS = 900
SLACK_DM_LANDING_LATE_P95_SECONDS = 3600

# marts_ops.search_health reports `late` once the oldest timeline row the chunk
# builder has not reached has waited this long. The builder runs every five
# minutes and converges in one run when the timeline is quiet, so an hour of
# waiting means the semantic corpus is materially behind what search_text
# already sees, not merely one tick behind.
SEARCH_HEALTH_LATE_AFTER_MINUTES = 60


@dataclass(frozen=True)
class SlackAccountStateRefresh:
    """What one derived_slack.inbox_items refresh did.

    ``mode`` is ``full`` (every member conversation recomputed), ``incremental``
    (only conversations whose rows changed since the last refresh) or ``skipped``
    (another refresh held the lock).
    """

    mode: str
    changed_conversations: int = 0
    rows_tombstoned: int = 0


# Serializes the one-time timeline priority bigint -> enum rewrite. Without it
# two processes booting together both see a bigint column, both issue the ALTER,
# and the second one rewrites the whole table again behind the first.


def _sha256_hex(value: str) -> str:
    """Fingerprint a credential so state can name it without storing it twice."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _canonical_whoop_token_json(value: str) -> str:
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ValueError("WHOOP token JSON must be an object")
    return json.dumps(parsed, sort_keys=True, separators=(",", ":"))


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
    requires_pgvector: bool = False
    #: Rebuild this index when its DEFINITION changes, not merely when it is
    #: absent. `CREATE INDEX IF NOT EXISTS` cannot express "the predicate moved",
    #: so a partial index whose WHERE clause is derived from code silently keeps
    #: the predicate it was born with. That is not cosmetic for the bm25 search
    #: indexes: the search layer pins an index BY NAME and vchord-bm25 raises if
    #: the planner picks a different one, so a partial index that no longer
    #: covers its adapter list takes DOWN broad search. That is exactly what
    #: happened in production on 2026-08-23 when a new timeline adapter joined
    #: the low-volume list. Opting in stamps the definition's fingerprint on the
    #: index as a comment and rebuilds when it drifts.
    rebuild_on_definition_change: bool = False


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
    "apple_contact_cards": TableSpec(
        CONTACT_CARD_COLUMNS,
        ("source", "account", "source_kind", "address_book_id", "card_id"),
    ),
    "contact_sync_state": TableSpec(
        CONTACT_SYNC_STATE_COLUMNS,
        ("source", "account", "source_kind", "address_book_id"),
    ),
    "plaid_items": TableSpec(PLAID_ITEM_COLUMNS, ("account", "item_id")),
    "plaid_item_tokens": TableSpec(PLAID_ITEM_TOKEN_COLUMNS, ("account", "item_id")),
    "plaid_accounts": TableSpec(PLAID_ACCOUNT_COLUMNS, ("account", "account_id")),
    "plaid_transactions": TableSpec(PLAID_TRANSACTION_COLUMNS, ("account", "transaction_id")),
    "plaid_investment_securities": TableSpec(PLAID_INVESTMENT_SECURITY_COLUMNS, ("account", "security_id")),
    "plaid_investment_holdings": TableSpec(
        PLAID_INVESTMENT_HOLDING_COLUMNS,
        ("account", "account_id", "security_id"),
    ),
    "plaid_investment_transactions": TableSpec(
        PLAID_INVESTMENT_TRANSACTION_COLUMNS,
        ("account", "investment_transaction_id"),
    ),
    "plaid_liabilities": TableSpec(PLAID_LIABILITY_COLUMNS, ("account", "account_id", "liability_type")),
    "plaid_sync_state": TableSpec(PLAID_SYNC_STATE_COLUMNS, ("account", "item_id", "product"), "updated_at"),
    # Finance ledger: derived stocks-and-flows layer (see finance_ledger.py).
    # Accounts are logical cross-source identities; observations are
    # append-only per-day values (the PK makes re-syncs upsert in place while
    # history accrues across days).
    "finance_accounts": TableSpec(FINANCE_ACCOUNT_COLUMNS, ("account_id",)),
    "finance_account_links": TableSpec(
        FINANCE_ACCOUNT_LINK_COLUMNS,
        ("source", "account", "source_account_key"),
    ),
    "finance_observations": TableSpec(
        FINANCE_OBSERVATION_COLUMNS,
        ("account_id", "as_of", "kind", "source"),
    ),
    "finance_transactions": TableSpec(FINANCE_TRANSACTION_COLUMNS, ("transaction_id",)),
    "receipt_transaction_receipts": TableSpec(
        RECEIPT_TRANSACTION_RECEIPT_COLUMNS,
        ("transaction_id",),
    ),
    "finance_transaction_links": TableSpec(
        FINANCE_TRANSACTION_LINK_COLUMNS,
        ("source", "source_row_key"),
    ),
    "finance_security_transactions": TableSpec(
        FINANCE_SECURITY_TRANSACTION_COLUMNS,
        ("transaction_id",),
    ),
    "finance_security_transaction_links": TableSpec(
        FINANCE_SECURITY_TRANSACTION_LINK_COLUMNS,
        ("source", "source_row_key"),
    ),
    "finance_tax_lots": TableSpec(FINANCE_TAX_LOT_COLUMNS, ("lot_id",)),
    # Manually uploaded finance documents + their structured extractions.
    "manual_finance_documents": TableSpec(
        MANUAL_FINANCE_DOCUMENT_COLUMNS,
        ("source", "account", "source_native_id", "content_sha256"),
    ),
    "manual_finance_extractions": TableSpec(
        MANUAL_FINANCE_EXTRACTION_COLUMNS,
        ("content_sha256", "ai_provider", "ai_model", "ai_prompt_version"),
    ),
    "apple_voice_memos_files": TableSpec(VOICE_MEMO_FILE_COLUMNS, ("account", "recording_id")),
    # ``source`` leads every key here: derived_voice_memos.* serves EVERY voice
    # source, and a recording_id is only unique within its own source. Keyed
    # without it, a second source's transcription run collides with an Apple
    # one and the upsert silently overwrites the wrong recording.
    "apple_voice_memos_transcription_runs": TableSpec(
        VOICE_MEMO_TRANSCRIPTION_RUN_COLUMNS,
        ("source", "account", "recording_id", "provider"),
    ),
    "apple_voice_memos_transcript_segments": TableSpec(
        VOICE_MEMO_TRANSCRIPT_SEGMENT_COLUMNS,
        ("source", "account", "recording_id", "provider", "segment_index"),
    ),
    "apple_voice_memos_enrichments": TableSpec(
        VOICE_MEMO_ENRICHMENT_COLUMNS,
        ("source", "account", "recording_id", "provider", "model", "prompt_version"),
    ),
    "alice_voice_recordings": TableSpec(
        ALICE_VOICE_RECORDING_COLUMNS,
        ("account", "recording_id"),
    ),
    "alice_voice_recording_artifacts": TableSpec(
        ALICE_VOICE_RECORDING_ARTIFACT_COLUMNS,
        ("account", "recording_id", "artifact_id"),
    ),
    "alice_voice_recordings_sync_state": TableSpec(
        ALICE_VOICE_RECORDINGS_SYNC_STATE_COLUMNS,
        ("account",),
        "updated_at",
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
    # Photos: one raw file table per source (all sharing
    # PHOTO_SOURCE_FILE_COLUMNS — see PHOTO_SOURCE_RELATIONS in relations.py),
    # unified by the derived photos.assets/asset_files identity tables and the
    # marts_photos.files / marts_photos.photos / marts_photos.canonical_renditions views.
    "apple_photos_files": TableSpec(
        PHOTO_SOURCE_FILE_COLUMNS,
        ("source", "account", "source_native_id", "content_sha256"),
    ),
    "photo_assets": TableSpec(PHOTO_ASSET_COLUMNS, ("photo_id",)),
    "photo_asset_files": TableSpec(
        PHOTO_ASSET_FILE_COLUMNS,
        ("source", "account", "source_native_id", "content_sha256"),
    ),
    "media_fingerprints": TableSpec(MEDIA_FINGERPRINT_COLUMNS, ("content_sha256", "hash_version")),
    # One row per Slack file, NOT per (file, conversation) share: the same
    # upload cross-posted into five channels is one set of bytes and one
    # download. The perceptual hash lives in media_fingerprints, keyed by
    # the content sha this table resolves.
    "slack_file_fingerprints": TableSpec(
        SLACK_FILE_FINGERPRINT_COLUMNS,
        ("account", "team_id", "file_id"),
    ),
    "chatgpt_events": TableSpec(
        AGENT_SESSION_EVENT_COLUMNS,
        ("source", "session_id", "event_uuid"),
        storage_parameters=(
            ("autovacuum_analyze_scale_factor", "0"),
            ("autovacuum_analyze_threshold", "50000"),
            ("autovacuum_vacuum_scale_factor", "0"),
            ("autovacuum_vacuum_threshold", "100000"),
        ),
    ),
    "claude_desktop_events": TableSpec(AGENT_SESSION_EVENT_COLUMNS, ("source", "session_id", "event_uuid")),
    "claude_code_events": TableSpec(AGENT_SESSION_EVENT_COLUMNS, ("source", "session_id", "event_uuid")),
    "codex_events": TableSpec(AGENT_SESSION_EVENT_COLUMNS, ("source", "session_id", "event_uuid")),
    "openclaw_events": TableSpec(AGENT_SESSION_EVENT_COLUMNS, ("source", "session_id", "event_uuid")),
    "pi_events": TableSpec(AGENT_SESSION_EVENT_COLUMNS, ("source", "session_id", "event_uuid")),
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
    "whoop_profiles": TableSpec(WHOOP_PROFILE_COLUMNS, ("account",)),
    "whoop_body_measurements": TableSpec(WHOOP_BODY_MEASUREMENT_COLUMNS, ("account",)),
    "whoop_cycles": TableSpec(WHOOP_CYCLE_COLUMNS, ("account", "cycle_id")),
    "whoop_recoveries": TableSpec(WHOOP_RECOVERY_COLUMNS, ("account", "cycle_id")),
    "whoop_sleeps": TableSpec(WHOOP_SLEEP_COLUMNS, ("account", "sleep_id")),
    "whoop_workouts": TableSpec(WHOOP_WORKOUT_COLUMNS, ("account", "workout_id")),
    "whoop_sync_state": TableSpec(WHOOP_SYNC_STATE_COLUMNS, ("account", "collection")),
    "whoop_oauth_tokens": TableSpec(WHOOP_OAUTH_TOKEN_COLUMNS, ("account",), "updated_at"),
    # WHOOP private (app) API. Summary grain mirrors the public tables; the
    # sample tables are the reason this source exists.
    "whoop_private_cycles": TableSpec(WHOOP_PRIVATE_CYCLE_COLUMNS, ("account", "cycle_id")),
    "whoop_private_sleeps": TableSpec(WHOOP_PRIVATE_SLEEP_COLUMNS, ("account", "activity_id")),
    "whoop_private_recoveries": TableSpec(WHOOP_PRIVATE_RECOVERY_COLUMNS, ("account", "activity_id")),
    "whoop_private_workouts": TableSpec(WHOOP_PRIVATE_WORKOUT_COLUMNS, ("account", "activity_id")),
    "whoop_private_sleep_events": TableSpec(
        WHOOP_PRIVATE_SLEEP_EVENT_COLUMNS,
        ("account", "activity_id", "event_index"),
    ),
    # ~525k rows/year at the 6-second grain, and append-heavy: the default
    # size-proportional autovacuum triggers would leave planner statistics
    # stale for months at a time (the gmail_messages precedent).
    "whoop_private_heart_rate_samples": TableSpec(
        WHOOP_PRIVATE_HEART_RATE_SAMPLE_COLUMNS,
        ("account", "sample_at"),
        storage_parameters=(
            ("autovacuum_analyze_scale_factor", "0.02"),
            ("autovacuum_vacuum_scale_factor", "0.05"),
        ),
    ),
    "whoop_private_journal_entries": TableSpec(
        WHOOP_PRIVATE_JOURNAL_ENTRY_COLUMNS,
        ("account", "day", "question_id"),
    ),
    "whoop_private_sports": TableSpec(WHOOP_PRIVATE_SPORT_COLUMNS, ("account", "sport_id")),
    "whoop_private_documents": TableSpec(
        WHOOP_PRIVATE_DOCUMENT_COLUMNS,
        ("account", "kind", "doc_key"),
    ),
    "whoop_private_sync_state": TableSpec(
        WHOOP_PRIVATE_SYNC_STATE_COLUMNS,
        ("account", "collection"),
    ),
    # PK (account, session_key) is load-bearing: the app's publish endpoint
    # upserts this same table with ON CONFLICT (account, session_key). See
    # app/internal/slacksession/store.go.
    "slack_sessions": TableSpec(
        SLACK_SESSION_COLUMNS,
        ("account", "session_key"),
    ),
    # PK (account, session_key) is load-bearing: the app's publish endpoint
    # upserts this same table with ON CONFLICT (account, session_key). See
    # app/internal/whoopsession/store.go.
    "whoop_private_sessions": TableSpec(
        WHOOP_PRIVATE_SESSION_COLUMNS,
        ("account", "session_key"),
    ),
    # Unified timeline (personal_data_warehouse/timeline.py). Row volume tracks
    # the sum of every event source (slack_messages dominates), so it gets the
    # same append-heavy autovacuum thresholds.
    "timeline_events": TableSpec(
        TIMELINE_EVENT_COLUMNS,
        ("adapter", "event_id"),
        "updated_at",
        storage_parameters=(
            ("autovacuum_analyze_scale_factor", "0"),
            ("autovacuum_analyze_threshold", "50000"),
            ("autovacuum_vacuum_scale_factor", "0"),
            ("autovacuum_vacuum_threshold", "100000"),
        ),
    ),
    "timeline_sync_state": TableSpec(TIMELINE_SYNC_STATE_COLUMNS, ("adapter",), "updated_at"),
    # Derived search-retrieval layer (search_index.py). Chunks churn with the
    # timeline, so give autovacuum the same append-heavy posture.
    "search_chunks": TableSpec(
        SEARCH_CHUNK_COLUMNS,
        ("chunk_id",),
        "built_at",
        storage_parameters=(
            ("autovacuum_vacuum_scale_factor", "0.02"),
            ("autovacuum_analyze_scale_factor", "0.02"),
        ),
    ),
    "search_chunk_embeddings": TableSpec(
        SEARCH_CHUNK_EMBEDDING_COLUMNS,
        ("text_sha256", "model"),
        "embedded_at",
    ),
    "search_chunk_sync_state": TableSpec(SEARCH_CHUNK_SYNC_STATE_COLUMNS, ("id",), "updated_at"),
    "search_health": TableSpec(SEARCH_HEALTH_COLUMNS, ("component",), "updated_at"),
    # Pipeline freshness snapshot (personal_data_warehouse/pipeline_health.py).
    # One row per pipeline and one per warehouse table, replaced in place by each
    # collection; collected_at is the version column so a stale collector can
    # never overwrite a newer snapshot.
    "pipeline_health": TableSpec(PIPELINE_HEALTH_COLUMNS, ("pipeline",), "collected_at"),
    "pipeline_table_freshness": TableSpec(
        PIPELINE_TABLE_FRESHNESS_COLUMNS,
        ("table_id",),
        "collected_at",
    ),
    # Mart (view) health and collation/index integrity, same snapshot contract:
    # measured facts keyed by the object, replaced in place each collection.
    "mart_view_health": TableSpec(MART_VIEW_HEALTH_COLUMNS, ("view_id",), "collected_at"),
    "collation_health": TableSpec(COLLATION_HEALTH_COLUMNS, ("object_id",), "collected_at"),
    "pgbackrest_health": TableSpec(PGBACKREST_HEALTH_COLUMNS, ("stanza",), "collected_at"),
    "uploader_heartbeats": TableSpec(UPLOADER_HEARTBEAT_COLUMNS, ("pipeline", "device")),
    "timeline_priority_mix": TableSpec(TIMELINE_PRIORITY_MIX_COLUMNS, ("source", "priority"), "collected_at"),
    "agent_usage": TableSpec(AGENT_USAGE_COLUMNS, ("source",), "collected_at"),
    "search_benchmark_runs": TableSpec(SEARCH_BENCHMARK_RUN_COLUMNS, ("mode",), "collected_at"),
    "search_benchmark_labels": TableSpec(SEARCH_BENCHMARK_LABEL_COLUMNS, ("query",)),
}

#: The marts_ops snapshot tables: one ensure path, and the only tables whose
#: columns are reconciled against their TableSpec on every run. Their whole
#: content is rewritten by each collection, so an added column is metadata-only
#: and there is no heap to lock -- which is what makes reconciling safe here and
#: not everywhere.
PIPELINE_HEALTH_SNAPSHOT_TABLES = (
    "pipeline_health",
    "pipeline_table_freshness",
    "mart_view_health",
    "collation_health",
    "search_health",
    "pgbackrest_health",
    "uploader_heartbeats",
    "timeline_priority_mix",
    "agent_usage",
    "search_benchmark_runs",
    "search_benchmark_labels",
)

# Every table whose rows belong to exactly one linked Plaid Item, data first
# and the credential last. plaid_investment_securities is absent on purpose:
# securities are keyed by account and shared across Items.
PLAID_ITEM_SCOPED_TABLES = (
    "plaid_items",
    "plaid_accounts",
    "plaid_transactions",
    "plaid_investment_holdings",
    "plaid_investment_transactions",
    "plaid_liabilities",
    "plaid_sync_state",
    "plaid_item_tokens",
)

# The source-owned AI conversation event tables (claude_code.events,
# codex.events, ...). They share AGENT_SESSION_EVENT_COLUMNS and are read
# together through the marts_ai_conversations.events union view, so they all
# need the same read-path indexes.
_AI_CONVERSATION_EVENT_TABLES = (
    "chatgpt_events",
    "claude_desktop_events",
    "claude_code_events",
    "codex_events",
    "openclaw_events",
    "pi_events",
)


def _ai_conversation_event_index_specs() -> tuple[IndexSpec, ...]:
    """Read-path indexes for every source-owned AI conversation event table.

    The marts_ai_conversations.events union view has no storage of its own, so
    every probe through it (the timeline agent_session adapter's per-session
    LATERAL lookups, session roll-ups, recency scans, changed-session
    detection) is only as good as the per-source indexes underneath.
    """
    specs: list[IndexSpec] = []
    for table in _AI_CONVERSATION_EVENT_TABLES:
        specs.append(
            IndexSpec(
                f"{table}_session_seq_idx",
                table,
                f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {table}_session_seq_idx "
                f"ON @{table} (session_id, seq)",
            )
        )
        specs.append(
            IndexSpec(
                f"{table}_occurred_at_idx",
                table,
                f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {table}_occurred_at_idx "
                f"ON @{table} (occurred_at DESC)",
            )
        )
        # First-prompt template lookups for the timeline's scheduled-session
        # detection; expression must match the adapter's probe.
        specs.append(
            IndexSpec(
                f"{table}_first_prompt_idx",
                table,
                f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {table}_first_prompt_idx "
                f"ON @{table} ((left(text, 64))) WHERE role = 'user' AND seq <= 5",
            )
        )
        specs.append(
            IndexSpec(
                f"{table}_ingested_at_idx",
                table,
                f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {table}_ingested_at_idx "
                f"ON @{table} (ingested_at)",
            )
        )
    return tuple(specs)


POSTGRES_INDEXES: tuple[IndexSpec, ...] = (
    IndexSpec(
        "finance_transactions_account_time_idx",
        "finance_transactions",
        "CREATE INDEX IF NOT EXISTS finance_transactions_account_time_idx "
        "ON @finance_transactions (account_id, posted_at DESC)",
    ),
    IndexSpec(
        "finance_security_transactions_account_time_idx",
        "finance_security_transactions",
        "CREATE INDEX IF NOT EXISTS finance_security_transactions_account_time_idx "
        "ON @finance_security_transactions (account_id, trade_date DESC)",
    ),
    # Freshness probing reads max(created_at); leading with it keeps the probe
    # an index scan rather than a heap sweep.
    IndexSpec(
        "finance_security_transactions_created_idx",
        "finance_security_transactions",
        "CREATE INDEX IF NOT EXISTS finance_security_transactions_created_idx "
        "ON @finance_security_transactions (created_at DESC)",
    ),
    IndexSpec(
        "finance_tax_lots_account_security_idx",
        "finance_tax_lots",
        "CREATE INDEX IF NOT EXISTS finance_tax_lots_account_security_idx "
        "ON @finance_tax_lots (account_id, security_key, acquired_on)",
    ),
    IndexSpec(
        "finance_tax_lots_created_idx",
        "finance_tax_lots",
        "CREATE INDEX IF NOT EXISTS finance_tax_lots_created_idx "
        "ON @finance_tax_lots (created_at DESC)",
    ),
    IndexSpec(
        "gmail_messages_thread_idx",
        "gmail_messages",
        "CREATE INDEX IF NOT EXISTS gmail_messages_thread_idx ON @gmail_messages (account, thread_id, internal_date DESC)",
    ),
    # Recipient-membership lookups ('a@b' = ANY(to_addresses || cc_addresses
    # || bcc_addresses)) are structured raw-table predicates. Codified from
    # the out-of-band production index created on the unmerged
    # pdw-slow-query-diagnosis branch (commit 5300f75); the definition must
    # stay byte-compatible with that deployed index.
    IndexSpec(
        "gmail_messages_recipients_array_idx",
        "gmail_messages",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS gmail_messages_recipients_array_idx ON @gmail_messages "
        "USING gin ((to_addresses || cc_addresses || bcc_addresses)) WHERE is_deleted = 0",
    ),
    # Normalized-subject-prefix lookups for the timeline's mail-merge
    # detection (timeline.py _GMAIL_MERGE_CLUSTER); the expression must match
    # that probe's expression exactly.
    IndexSpec(
        "gmail_messages_merge_prefix_idx",
        "gmail_messages",
        "CREATE INDEX IF NOT EXISTS gmail_messages_merge_prefix_idx ON @gmail_messages "
        "(account, (left(regexp_replace(lower(subject), '^((re|fwd|fw)(\\[\\d+\\])?:\\s*)+', ''), 24)), "
        "internal_date)",
    ),
    IndexSpec(
        "gmail_messages_internal_date_idx",
        "gmail_messages",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS gmail_messages_internal_date_idx ON @gmail_messages (internal_date DESC)",
    ),
    IndexSpec(
        "gmail_messages_label_ids_idx",
        "gmail_messages",
        "CREATE INDEX IF NOT EXISTS gmail_messages_label_ids_idx ON @gmail_messages USING gin (label_ids)",
    ),
    IndexSpec(
        "gmail_messages_from_trgm_idx",
        "gmail_messages",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS gmail_messages_from_trgm_idx ON @gmail_messages USING gin (from_address public.gin_trgm_ops)",
        requires_pg_trgm=True,
    ),
    IndexSpec(
        "gmail_messages_subject_trgm_idx",
        "gmail_messages",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS gmail_messages_subject_trgm_idx ON @gmail_messages USING gin (subject public.gin_trgm_ops)",
        requires_pg_trgm=True,
    ),
    # Kept alongside from/subject: the voice-memo speaker-identity hints scan
    # gmail with (from_address ILIKE .. OR subject ILIKE .. OR snippet ILIKE
    # ..), and a bitmap-OR plan needs every arm indexed — one unindexed arm
    # degrades the whole query to a full 23 GB scan inside the enrichment
    # pipeline (apple_voice_memos_enrichment.py).
    IndexSpec(
        "gmail_messages_snippet_trgm_idx",
        "gmail_messages",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS gmail_messages_snippet_trgm_idx ON @gmail_messages USING gin (snippet public.gin_trgm_ops)",
        requires_pg_trgm=True,
    ),
    IndexSpec(
        "gmail_attachments_message_idx",
        "gmail_attachments",
        "CREATE INDEX IF NOT EXISTS gmail_attachments_message_idx ON @gmail_attachments (account, message_id)",
    ),
    IndexSpec(
        "file_attachment_enrichments_text_trgm_idx",
        "file_attachment_enrichments",
        "CREATE INDEX IF NOT EXISTS file_attachment_enrichments_text_trgm_idx ON @file_attachment_enrichments USING gin (text public.gin_trgm_ops)",
        requires_pg_trgm=True,
    ),
    IndexSpec(
        "calendar_events_time_idx",
        "calendar_events",
        "CREATE INDEX IF NOT EXISTS calendar_events_time_idx ON @calendar_events (start_at, end_at)",
    ),
    IndexSpec(
        "contact_cards_display_idx",
        "contact_cards",
        "CREATE INDEX IF NOT EXISTS contact_cards_display_idx ON @contact_cards (account, source_kind, display_name) WHERE is_deleted = 0",
    ),
    IndexSpec(
        "contact_cards_primary_email_idx",
        "contact_cards",
        "CREATE INDEX IF NOT EXISTS contact_cards_primary_email_idx ON @contact_cards (lower(primary_email)) WHERE is_deleted = 0 AND primary_email != ''",
    ),
    IndexSpec(
        "contact_cards_primary_phone_idx",
        "contact_cards",
        "CREATE INDEX IF NOT EXISTS contact_cards_primary_phone_idx ON @contact_cards (lower(primary_phone)) WHERE is_deleted = 0 AND primary_phone != ''",
    ),
    IndexSpec(
        "contact_cards_source_updated_idx",
        "contact_cards",
        "CREATE INDEX IF NOT EXISTS contact_cards_source_updated_idx ON @contact_cards (source_updated_at DESC)",
    ),
    IndexSpec(
        "contact_cards_raw_json_idx",
        "contact_cards",
        "CREATE INDEX IF NOT EXISTS contact_cards_raw_json_idx ON @contact_cards USING gin (raw_json)",
    ),
    IndexSpec(
        "voice_memo_files_recorded_idx",
        "apple_voice_memos_files",
        "CREATE INDEX IF NOT EXISTS voice_memo_files_recorded_idx ON @apple_voice_memos_files (recorded_at DESC)",
    ),
    IndexSpec(
        "alice_voice_recordings_recorded_idx",
        "alice_voice_recordings",
        "CREATE INDEX IF NOT EXISTS alice_voice_recordings_recorded_idx ON @alice_voice_recordings (recorded_at DESC)",
    ),
    IndexSpec(
        "alice_voice_recording_artifacts_recording_idx",
        "alice_voice_recording_artifacts",
        "CREATE INDEX IF NOT EXISTS alice_voice_recording_artifacts_recording_idx ON @alice_voice_recording_artifacts (account, recording_id, kind)",
    ),
    IndexSpec(
        "apple_photos_files_ingested_at_idx",
        "apple_photos_files",
        "CREATE INDEX IF NOT EXISTS apple_photos_files_ingested_at_idx ON @apple_photos_files (ingested_at)",
    ),
    IndexSpec(
        "apple_photos_files_content_sha256_idx",
        "apple_photos_files",
        "CREATE INDEX IF NOT EXISTS apple_photos_files_content_sha256_idx ON @apple_photos_files (content_sha256)",
    ),
    IndexSpec(
        "photo_assets_capture_ts_idx",
        "photo_assets",
        "CREATE INDEX IF NOT EXISTS photo_assets_capture_ts_idx ON @photo_assets (capture_ts DESC)",
    ),
    IndexSpec(
        "photo_asset_files_photo_id_idx",
        "photo_asset_files",
        "CREATE INDEX IF NOT EXISTS photo_asset_files_photo_id_idx ON @photo_asset_files (photo_id)",
    ),
    IndexSpec(
        "apple_voice_memos_transcript_trgm_idx",
        "apple_voice_memos_enrichments",
        "CREATE INDEX IF NOT EXISTS apple_voice_memos_transcript_trgm_idx ON @apple_voice_memos_enrichments USING gin (transcript public.gin_trgm_ops)",
        requires_pg_trgm=True,
    ),
    IndexSpec(
        "apple_notes_modified_idx",
        "apple_notes",
        "CREATE INDEX IF NOT EXISTS apple_notes_modified_idx ON @apple_notes (modified_at DESC) WHERE is_deleted = 0",
    ),
    IndexSpec(
        "apple_notes_title_trgm_idx",
        "apple_notes",
        "CREATE INDEX IF NOT EXISTS apple_notes_title_trgm_idx ON @apple_notes USING gin (title public.gin_trgm_ops)",
        requires_pg_trgm=True,
    ),
    IndexSpec(
        "apple_notes_body_trgm_idx",
        "apple_notes",
        "CREATE INDEX IF NOT EXISTS apple_notes_body_trgm_idx ON @apple_notes USING gin (body_text public.gin_trgm_ops)",
        requires_pg_trgm=True,
    ),
    IndexSpec(
        "apple_note_revisions_note_idx",
        "apple_note_revisions",
        "CREATE INDEX IF NOT EXISTS apple_note_revisions_note_idx ON @apple_note_revisions (account, note_id, modified_at DESC)",
    ),
    IndexSpec(
        "apple_note_attachments_hash_idx",
        "apple_note_attachments",
        "CREATE INDEX IF NOT EXISTS apple_note_attachments_hash_idx ON @apple_note_attachments (content_sha256)",
    ),
    IndexSpec(
        "apple_messages_time_idx",
        "apple_messages",
        "CREATE INDEX IF NOT EXISTS apple_messages_time_idx ON @apple_messages (message_at DESC) WHERE is_deleted = 0",
    ),
    # Per-correspondent history ("latest/prior messages with this handle") is a
    # structured raw-table access pattern; without this index it planned as a
    # 115M-cost scan per handle in production.
    IndexSpec(
        "apple_messages_handle_time_idx",
        "apple_messages",
        "CREATE INDEX IF NOT EXISTS apple_messages_handle_time_idx ON @apple_messages (account, handle_id, message_at DESC) WHERE is_deleted = 0",
    ),
    IndexSpec(
        "apple_message_chat_messages_chat_time_idx",
        "apple_message_chat_messages",
        "CREATE INDEX IF NOT EXISTS apple_message_chat_messages_chat_time_idx ON @apple_message_chat_messages (account, chat_id, message_date DESC)",
    ),
    IndexSpec(
        "apple_message_chat_messages_message_idx",
        "apple_message_chat_messages",
        "CREATE INDEX IF NOT EXISTS apple_message_chat_messages_message_idx ON @apple_message_chat_messages (account, message_id, chat_id)",
    ),
    IndexSpec(
        "apple_message_attachments_hash_idx",
        "apple_message_attachments",
        "CREATE INDEX IF NOT EXISTS apple_message_attachments_hash_idx ON @apple_message_attachments (content_sha256)",
    ),
    # The timeline apple_message adapter probes attachments by (account,
    # message_id) per candidate message; the PK is (account, attachment_id,
    # message_id), so those probes need their own index.
    IndexSpec(
        "apple_message_attachments_message_idx",
        "apple_message_attachments",
        "CREATE INDEX IF NOT EXISTS apple_message_attachments_message_idx ON @apple_message_attachments (account, message_id)",
    ),
    IndexSpec(
        "whatsapp_messages_time_idx",
        "whatsapp_messages",
        "CREATE INDEX IF NOT EXISTS whatsapp_messages_time_idx ON @whatsapp_messages (message_at DESC) WHERE is_deleted = 0",
    ),
    IndexSpec(
        "whatsapp_messages_chat_time_idx",
        "whatsapp_messages",
        "CREATE INDEX IF NOT EXISTS whatsapp_messages_chat_time_idx ON @whatsapp_messages (account, chat_id, message_at DESC)",
    ),
    IndexSpec(
        "whatsapp_messages_body_trgm_idx",
        "whatsapp_messages",
        "CREATE INDEX IF NOT EXISTS whatsapp_messages_body_trgm_idx ON @whatsapp_messages USING gin (body_text public.gin_trgm_ops) WHERE is_deleted = 0",
        requires_pg_trgm=True,
    ),
    IndexSpec(
        "whatsapp_media_items_hash_idx",
        "whatsapp_media_items",
        "CREATE INDEX IF NOT EXISTS whatsapp_media_items_hash_idx ON @whatsapp_media_items (content_sha256)",
    ),
    IndexSpec(
        "ai_processing_agent_runs_task_status_subject_idx",
        "agent_runs",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ai_processing_agent_runs_task_status_subject_idx "
        "ON @agent_runs (task_type, status, subject_id)",
    ),
    IndexSpec(
        "ai_processing_agent_run_events_created_idx",
        "agent_run_events",
        "CREATE INDEX IF NOT EXISTS ai_processing_agent_run_events_created_idx ON @agent_run_events (created_at DESC)",
    ),
    IndexSpec(
        "slack_messages_conversation_time_idx",
        "slack_messages",
        "CREATE INDEX IF NOT EXISTS slack_messages_conversation_time_idx ON @slack_messages (account, team_id, conversation_id, message_datetime DESC)",
    ),
    IndexSpec(
        # Single-column index on message_datetime so global MIN/MAX/COUNT
        # probes and time-only date-range scans use an index instead of a
        # full table scan across all 30M+ messages.
        "slack_messages_time_idx",
        "slack_messages",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS slack_messages_time_idx ON @slack_messages (message_datetime DESC)",
    ),
    IndexSpec(
        "slack_messages_user_time_idx",
        "slack_messages",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS slack_messages_user_time_idx ON @slack_messages (user_id, message_datetime DESC)",
    ),
    IndexSpec(
        "slack_messages_synced_at_idx",
        "slack_messages",
        "CREATE INDEX IF NOT EXISTS slack_messages_synced_at_idx ON @slack_messages (synced_at)",
    ),
    # The freshness collector refuses max() over a large heap unless an index
    # leads with the column, so without this base_slack.message_reactions (1.5 GiB)
    # reported probe_status 'skipped_unindexed' and had no freshness signal at
    # all — a reaction backlog could freeze indefinitely and nothing would say so.
    # marts_slack.huddles filters 45M messages down to ~6k huddle_thread rows.
    # Without this the view is a 6.1M-buffer parallel seq scan measured at 30.4s,
    # which no read budget tolerates and which the mart-view health probe would
    # run every ten minutes. The partial index covers 0.013% of the heap.
    # The metadata stage reads only the conversations.list walk cursors (four
    # rows). slack_state_scope_idx leads with (account, team_id), so filtering on
    # object_type alone would seq-scan a 363 MB heap every 15 minutes.
    IndexSpec(
        "slack_sync_state_conversation_list_idx",
        "slack_sync_state",
        "CREATE INDEX IF NOT EXISTS slack_sync_state_conversation_list_idx ON @slack_sync_state (object_type, object_id) WHERE object_type = 'conversation_list'",
    ),
    # Same reason, for the seven coverage-stage rows the coverage rotation reads
    # every seven minutes. A separate partial index rather than a widened
    # predicate: CREATE INDEX IF NOT EXISTS cannot express "the predicate moved",
    # so changing an existing one leaves production on the old definition.
    IndexSpec(
        "slack_sync_state_coverage_stage_idx",
        "slack_sync_state",
        "CREATE INDEX IF NOT EXISTS slack_sync_state_coverage_stage_idx ON @slack_sync_state (object_type, object_id) WHERE object_type = 'coverage_stage'",
    ),
    IndexSpec(
        "slack_messages_huddle_idx",
        "slack_messages",
        "CREATE INDEX IF NOT EXISTS slack_messages_huddle_idx ON @slack_messages (message_datetime DESC) WHERE subtype = 'huddle_thread'",
    ),
    IndexSpec(
        "slack_message_reactions_synced_at_idx",
        "slack_message_reactions",
        "CREATE INDEX IF NOT EXISTS slack_message_reactions_synced_at_idx ON @slack_message_reactions (synced_at)",
    ),
    IndexSpec(
        "slack_messages_recent_scope_time_idx",
        "slack_messages",
        "CREATE INDEX IF NOT EXISTS slack_messages_recent_scope_time_idx ON @slack_messages (account, team_id, message_datetime DESC) WHERE is_deleted = 0",
    ),
    IndexSpec(
        "slack_messages_recent_thread_time_idx",
        "slack_messages",
        "CREATE INDEX IF NOT EXISTS slack_messages_recent_thread_time_idx ON @slack_messages (account, team_id, thread_ts, message_datetime DESC) WHERE is_deleted = 0",
    ),
    IndexSpec(
        "slack_messages_thread_idx",
        "slack_messages",
        "CREATE INDEX IF NOT EXISTS slack_messages_thread_idx ON @slack_messages (account, team_id, conversation_id, thread_ts)",
    ),
    IndexSpec(
        # Thread-backfill candidate selection: only ~1.3M of 42M messages are
        # live thread parents, and without this partial index the candidate
        # query seq-scanned the whole 46 GB heap every ~5 minutes (the single
        # largest query cost in production, and a page-cache thrasher for
        # everything else). Ordered to serve the recent-first ORDER BY with an
        # early-stopping range scan.
        "slack_messages_thread_parents_idx",
        "slack_messages",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS slack_messages_thread_parents_idx "
        "ON @slack_messages (account, team_id, message_datetime DESC, message_ts DESC) "
        "WHERE is_deleted = 0 AND is_thread_reply = 0 AND reply_count > 0",
    ),
    IndexSpec(
        "slack_conversations_scope_idx",
        "slack_conversations",
        "CREATE INDEX IF NOT EXISTS slack_conversations_scope_idx ON @slack_conversations (account, team_id, conversation_type)",
    ),
    IndexSpec(
        "slack_conversations_synced_at_idx",
        "slack_conversations",
        "CREATE INDEX IF NOT EXISTS slack_conversations_synced_at_idx ON @slack_conversations (synced_at)",
    ),
    IndexSpec(
        "slack_users_email_lower_idx",
        "slack_users",
        "CREATE INDEX IF NOT EXISTS slack_users_email_lower_idx ON @slack_users (lower(email)) WHERE email != ''",
    ),
    IndexSpec(
        "slack_users_synced_at_idx",
        "slack_users",
        "CREATE INDEX IF NOT EXISTS slack_users_synced_at_idx ON @slack_users (synced_at)",
    ),
    IndexSpec(
        "slack_conversation_members_synced_at_idx",
        "slack_conversation_members",
        "CREATE INDEX IF NOT EXISTS slack_conversation_members_synced_at_idx ON @slack_conversation_members (synced_at)",
    ),
    IndexSpec(
        "slack_state_scope_idx",
        "slack_sync_state",
        "CREATE INDEX IF NOT EXISTS slack_state_scope_idx ON @slack_sync_state (account, team_id, object_type, object_id)",
    ),
    IndexSpec(
        "slack_account_state_live_scope_idx",
        "slack_account_state_item_rows",
        "CREATE INDEX IF NOT EXISTS slack_account_state_live_scope_idx ON @slack_account_state_item_rows (account, scope_id, priority_rank, latest_activity_at DESC) WHERE is_deleted = 0",
    ),
    IndexSpec(
        "google_drive_files_modified_idx",
        "google_drive_files",
        "CREATE INDEX IF NOT EXISTS google_drive_files_modified_idx ON @google_drive_files (account, modified_time DESC) WHERE trashed = 0 AND is_excluded = 0",
    ),
    IndexSpec(
        "google_drive_file_texts_text_trgm_idx",
        "google_drive_file_texts",
        "CREATE INDEX IF NOT EXISTS google_drive_file_texts_text_trgm_idx ON @google_drive_file_texts USING gin (text public.gin_trgm_ops)",
        requires_pg_trgm=True,
    ),
    IndexSpec(
        "whoop_cycles_start_idx",
        "whoop_cycles",
        "CREATE INDEX IF NOT EXISTS whoop_cycles_start_idx ON @whoop_cycles (account, start_at DESC)",
    ),
    IndexSpec(
        "whoop_sleeps_start_idx",
        "whoop_sleeps",
        "CREATE INDEX IF NOT EXISTS whoop_sleeps_start_idx ON @whoop_sleeps (account, start_at DESC)",
    ),
    IndexSpec(
        "whoop_workouts_start_idx",
        "whoop_workouts",
        "CREATE INDEX IF NOT EXISTS whoop_workouts_start_idx ON @whoop_workouts (account, start_at DESC)",
    ),
    IndexSpec(
        "whoop_recoveries_updated_idx",
        "whoop_recoveries",
        "CREATE INDEX IF NOT EXISTS whoop_recoveries_updated_idx ON @whoop_recoveries (account, updated_at DESC)",
    ),
    # WHOOP private (app API). Every data table carries an index LEADING with
    # synced_at, which is the column the freshness collector probes with
    # max(): it refuses to run that over a large unindexed heap, so a table
    # without this index reports no freshness at all rather than reporting it
    # late (pipeline_health.PROBE_SKIPPED_UNINDEXED).
    IndexSpec(
        "whoop_private_cycles_synced_idx",
        "whoop_private_cycles",
        "CREATE INDEX IF NOT EXISTS whoop_private_cycles_synced_idx ON @whoop_private_cycles (synced_at)",
    ),
    IndexSpec(
        "whoop_private_cycles_start_idx",
        "whoop_private_cycles",
        "CREATE INDEX IF NOT EXISTS whoop_private_cycles_start_idx ON @whoop_private_cycles (account, start_at DESC)",
    ),
    IndexSpec(
        "whoop_private_sleeps_synced_idx",
        "whoop_private_sleeps",
        "CREATE INDEX IF NOT EXISTS whoop_private_sleeps_synced_idx ON @whoop_private_sleeps (synced_at)",
    ),
    IndexSpec(
        "whoop_private_sleeps_start_idx",
        "whoop_private_sleeps",
        "CREATE INDEX IF NOT EXISTS whoop_private_sleeps_start_idx ON @whoop_private_sleeps (account, start_at DESC)",
    ),
    IndexSpec(
        "whoop_private_recoveries_synced_idx",
        "whoop_private_recoveries",
        "CREATE INDEX IF NOT EXISTS whoop_private_recoveries_synced_idx ON @whoop_private_recoveries (synced_at)",
    ),
    IndexSpec(
        "whoop_private_recoveries_updated_idx",
        "whoop_private_recoveries",
        "CREATE INDEX IF NOT EXISTS whoop_private_recoveries_updated_idx ON @whoop_private_recoveries (account, updated_at DESC)",
    ),
    IndexSpec(
        "whoop_private_workouts_synced_idx",
        "whoop_private_workouts",
        "CREATE INDEX IF NOT EXISTS whoop_private_workouts_synced_idx ON @whoop_private_workouts (synced_at)",
    ),
    IndexSpec(
        "whoop_private_workouts_start_idx",
        "whoop_private_workouts",
        "CREATE INDEX IF NOT EXISTS whoop_private_workouts_start_idx ON @whoop_private_workouts (account, start_at DESC)",
    ),
    IndexSpec(
        "whoop_private_sleep_events_synced_idx",
        "whoop_private_sleep_events",
        "CREATE INDEX IF NOT EXISTS whoop_private_sleep_events_synced_idx ON @whoop_private_sleep_events (synced_at)",
    ),
    IndexSpec(
        "whoop_private_sleep_events_time_idx",
        "whoop_private_sleep_events",
        "CREATE INDEX IF NOT EXISTS whoop_private_sleep_events_time_idx ON @whoop_private_sleep_events (account, started_at DESC)",
    ),
    # The time-range scan for the 6-second heart-rate series is served by the
    # primary key (account, sample_at) -- it is already a btree ordered exactly
    # that way, in both directions -- so only the freshness index is added here.
    IndexSpec(
        "whoop_private_heart_rate_samples_synced_idx",
        "whoop_private_heart_rate_samples",
        "CREATE INDEX IF NOT EXISTS whoop_private_heart_rate_samples_synced_idx "
        "ON @whoop_private_heart_rate_samples (synced_at)",
    ),
    # The freshness collector probes max(sample_at) for this table's event time
    # and refuses a max() over a large heap unless an index LEADS with that
    # column. The primary key leads with account, so without this the event-time
    # probe would report skipped_unindexed once the table grows past the probe
    # threshold -- roughly half a million rows a year.
    IndexSpec(
        "whoop_private_heart_rate_samples_time_idx",
        "whoop_private_heart_rate_samples",
        "CREATE INDEX IF NOT EXISTS whoop_private_heart_rate_samples_time_idx "
        "ON @whoop_private_heart_rate_samples (sample_at DESC)",
    ),
    IndexSpec(
        "whoop_private_journal_entries_synced_idx",
        "whoop_private_journal_entries",
        "CREATE INDEX IF NOT EXISTS whoop_private_journal_entries_synced_idx "
        "ON @whoop_private_journal_entries (synced_at)",
    ),
    # The journal is the one private-API table with a timeline adapter; its
    # backfill pages newest-first by the entry's day.
    IndexSpec(
        "whoop_private_journal_entries_day_idx",
        "whoop_private_journal_entries",
        "CREATE INDEX IF NOT EXISTS whoop_private_journal_entries_day_idx "
        "ON @whoop_private_journal_entries (account, day DESC)",
    ),
    IndexSpec(
        "whoop_private_sports_synced_idx",
        "whoop_private_sports",
        "CREATE INDEX IF NOT EXISTS whoop_private_sports_synced_idx ON @whoop_private_sports (synced_at)",
    ),
    IndexSpec(
        "whoop_private_documents_synced_idx",
        "whoop_private_documents",
        "CREATE INDEX IF NOT EXISTS whoop_private_documents_synced_idx ON @whoop_private_documents (synced_at)",
    ),
    IndexSpec(
        "whoop_private_documents_kind_idx",
        "whoop_private_documents",
        "CREATE INDEX IF NOT EXISTS whoop_private_documents_kind_idx "
        "ON @whoop_private_documents (account, kind, collected_at DESC)",
    ),
    # Unified timeline read paths: keyset pagination by event time (with seq as
    # the tiebreak) and per-source filtered scans. The kind filter rides on the
    # time/priority indexes as a residual predicate.
    IndexSpec(
        "timeline_events_time_idx",
        "timeline_events",
        "CREATE INDEX IF NOT EXISTS timeline_events_time_idx ON @timeline_events (event_ts DESC, seq DESC)",
    ),
    IndexSpec(
        "timeline_events_source_time_idx",
        "timeline_events",
        "CREATE INDEX IF NOT EXISTS timeline_events_source_time_idx ON @timeline_events (source, event_ts DESC, seq DESC)",
    ),
    IndexSpec(
        "timeline_events_priority_time_idx",
        "timeline_events",
        "CREATE INDEX IF NOT EXISTS timeline_events_priority_time_idx ON @timeline_events (priority, event_ts DESC, seq DESC)",
    ),
    # timeline.context(ref, before, after) walks a hit's neighbors within the
    # same (source, context) stream — the surrounding events a search hit needs
    # to be readable without a raw-table drill. This serves every adapter in
    # TIMELINE_CONTEXT_GENERIC_ADAPTERS; the conversational sources resolve
    # their neighbours in the SOURCE table's own indexes instead, which is why
    # that fix needed no new index on this 45 GB heap.
    # CONCURRENTLY: timeline_events is ~47M rows in production.
    IndexSpec(
        "timeline_events_context_time_idx",
        "timeline_events",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS timeline_events_context_time_idx "
        "ON @timeline_events (source, context, event_ts DESC, seq DESC)",
    ),
    # The search-chunk builder pages timeline changes by seq (its watermark is
    # a timeline.events.seq cursor), which nothing else does — the bare-seq
    # index retired in POSTGRES_OBSOLETE_INDEXES is revived for exactly this.
    IndexSpec(
        "timeline_events_seq_idx",
        "timeline_events",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS timeline_events_seq_idx "
        "ON @timeline_events (seq)",
    ),
    # Search-chunk maintenance paths: replace-by-anchor on rebuild, embedding
    # join by content sha, and the semantic branch's time/adapter filters.
    IndexSpec(
        "search_chunks_anchor_idx",
        "search_chunks",
        "CREATE INDEX IF NOT EXISTS search_chunks_anchor_idx ON @search_chunks (anchor)",
    ),
    # Covering: the semantic leg joins each ANN candidate to its chunk by
    # content sha and needs only these columns, so the join is index-only and
    # never visits the 7 GB chunk heap. Measured cold on production
    # 2026-08-26, that heap probe was ~2.4ms per candidate on top of the ANN
    # scan itself; with 1,000-2,000 candidates per leg and four legs per
    # hybrid call it was seconds of random I/O per search.
    IndexSpec(
        "search_chunks_sha_cover_idx",
        "search_chunks",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS search_chunks_sha_cover_idx "
        "ON @search_chunks (text_sha256) INCLUDE (adapter, event_id, chunk_id, event_ts)",
    ),
    IndexSpec(
        "search_chunks_adapter_ts_idx",
        "search_chunks",
        "CREATE INDEX IF NOT EXISTS search_chunks_adapter_ts_idx "
        "ON @search_chunks (adapter, event_ts DESC)",
    ),
    # The embedding drain's fresh pass reads only chunks built since its
    # persisted watermark, so a run over a caught-up corpus touches a few
    # thousand rows instead of the whole heap.
    IndexSpec(
        "search_chunks_built_at_sha_idx",
        "search_chunks",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS search_chunks_built_at_sha_idx "
        "ON @search_chunks (built_at) INCLUDE (text_sha256, chunk_id)",
    ),
    # Newest-first keyset cursor for the embedding drain (event_ts, chunk_id).
    # Covering for the same reason: the backfill only needs the sha to decide
    # whether a row still wants a vector, so the walk is an index-only scan.
    IndexSpec(
        "search_chunks_ts_chunk_sha_idx",
        "search_chunks",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS search_chunks_ts_chunk_sha_idx "
        "ON @search_chunks (event_ts DESC, chunk_id DESC) INCLUDE (text_sha256)",
    ),
    # Hybrid's short-query literal leg needs identifier recall, not the full
    # multi-megabyte timeline document returned by search_text_exact(). Search
    # the retrieval chunks instead: every row is bounded to 2-6k characters
    # (and oversized documents cover their first 200k), so the trigram recheck
    # never has to detoast and decompress a multi-megabyte source document.
    IndexSpec(
        "search_chunks_text_trgm_idx",
        "search_chunks",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS search_chunks_text_trgm_idx "
        "ON @search_chunks USING gin (text public.gin_trgm_ops)",
        requires_pg_trgm=True,
    ),
    # ANN index for the hybrid semantic branch. Only buildable once the
    # pgvector extension and the conditional halfvec column exist; on hosts
    # without them the creation fails and is harmlessly skipped like every
    # other missing-prerequisite index.
    IndexSpec(
        "search_chunk_embeddings_hnsw_idx",
        "search_chunk_embeddings",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS search_chunk_embeddings_hnsw_idx "
        "ON @search_chunk_embeddings USING hnsw (embedding public.halfvec_cosine_ops)",
        requires_pgvector=True,
    ),
    IndexSpec(
        "timeline_events_search_text_bm25_idx",
        "timeline_events",
        "CREATE INDEX IF NOT EXISTS timeline_events_search_text_bm25_idx ON @timeline_events USING bm25 (search_text) WITH (text_config='english')",
        requires_pg_textsearch=True,
    ),
    # A broad search_text() call scans the corpus in two partitions so the
    # per-source floor still has low-volume candidates to promote. Scanning
    # low-volume adapters through the GLOBAL bm25 index means walking the
    # score-ordered index past millions of gmail/slack documents to find them
    # (15-16s on the production corpus for an unlucky query). Their own partial
    # index answers the same scan in ~26ms and costs 61s to build over 1.2M
    # documents / 147MB of text -- the whole low-volume tail is 2.6% of the
    # corpus by rows and 1.6% by bytes.
    IndexSpec(
        "timeline_events_search_text_bm25_lowvol_idx",
        "timeline_events",
        "CREATE INDEX IF NOT EXISTS timeline_events_search_text_bm25_lowvol_idx "
        "ON @timeline_events USING bm25 (search_text) WITH (text_config='english') "
        f"WHERE adapter IN ({SEARCH_TEXT_LOW_VOLUME_ADAPTERS_SQL})",
        requires_pg_textsearch=True,
        # The adapter list above grows whenever a low-volume timeline adapter is
        # added, and a stale predicate breaks broad search outright.
        rebuild_on_definition_change=True,
    ),
    # The ATTENTION partitions. Same two-partition shape as the pair above --
    # the broad pool needs a low-volume partition or the per-source floor has
    # nothing to promote -- but restricted to the tiers an attention-scoped
    # search asks for. Measured on production 2026-08-26: 1,243,419 high-volume
    # documents (1,397 MB of text) and 90,256 low-volume ones (54 MB), against
    # a global index of 10.2 GB over the whole 49M-row corpus.
    #
    # The BIG one deliberately carries NO adapter list. Its predicate is the
    # tier alone, and the high-volume partition adds `adapter NOT IN (...)` as
    # an ordinary filter on top. A predicate derived from the adapter registry
    # has to be REBUILT when that registry moves, and the rebuild path is a
    # plain (non-concurrent) DROP+CREATE -- minutes of exclusive lock on the
    # 45 GB timeline heap. The low-volume one must carry the list to be the
    # low-volume partition at all, and at 54 MB of text its rebuild is seconds.
    IndexSpec(
        "timeline_events_search_text_bm25_attention_idx",
        "timeline_events",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS timeline_events_search_text_bm25_attention_idx "
        "ON @timeline_events USING bm25 (search_text) WITH (text_config='english') "
        f"WHERE priority IN ({SEARCH_TEXT_ATTENTION_PRIORITIES_SQL})",
        requires_pg_textsearch=True,
    ),
    IndexSpec(
        "timeline_events_search_text_bm25_attention_lowvol_idx",
        "timeline_events",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
        "timeline_events_search_text_bm25_attention_lowvol_idx "
        "ON @timeline_events USING bm25 (search_text) WITH (text_config='english') "
        f"WHERE priority IN ({SEARCH_TEXT_ATTENTION_PRIORITIES_SQL}) "
        f"AND adapter IN ({SEARCH_TEXT_LOW_VOLUME_ADAPTERS_SQL})",
        requires_pg_textsearch=True,
        # Same reason as the low-volume index above: a stale adapter predicate
        # means the planner picks another index and vchord-bm25 RAISES.
        rebuild_on_definition_change=True,
    ),
    IndexSpec(
        "timeline_events_search_text_trgm_idx",
        "timeline_events",
        "CREATE INDEX IF NOT EXISTS timeline_events_search_text_trgm_idx ON @timeline_events USING gin (search_text public.gin_trgm_ops)",
        requires_pg_trgm=True,
    ),
    # Ingestion-timestamp indexes on the larger event sources so the timeline's
    # incremental sync (WHERE ingest_ts > watermark) never falls back to a
    # per-tick sequential scan. Created CONCURRENTLY where the table is big in
    # production. Small event sources (upstream_mutations, agent_runs, ...) are
    # cheap to scan and get no index.
    IndexSpec(
        "gmail_messages_synced_at_idx",
        "gmail_messages",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS gmail_messages_synced_at_idx ON @gmail_messages (synced_at)",
    ),
    IndexSpec(
        "slack_files_synced_at_idx",
        "slack_files",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS slack_files_synced_at_idx ON @slack_files (synced_at)",
    ),
    IndexSpec(
        "slack_files_created_at_idx",
        "slack_files",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS slack_files_created_at_idx ON @slack_files (created_at DESC)",
    ),
    IndexSpec(
        "apple_messages_ingested_at_idx",
        "apple_messages",
        "CREATE INDEX IF NOT EXISTS apple_messages_ingested_at_idx ON @apple_messages (ingested_at)",
    ),
    IndexSpec(
        "google_drive_files_ingested_at_idx",
        "google_drive_files",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS google_drive_files_ingested_at_idx ON @google_drive_files (ingested_at)",
    ),
    IndexSpec(
        "calendar_events_synced_at_idx",
        "calendar_events",
        "CREATE INDEX IF NOT EXISTS calendar_events_synced_at_idx ON @calendar_events (synced_at)",
    ),
    IndexSpec(
        "contact_cards_synced_at_idx",
        "contact_cards",
        "CREATE INDEX IF NOT EXISTS contact_cards_synced_at_idx ON @contact_cards (synced_at)",
    ),
    IndexSpec(
        "apple_note_revisions_ingested_at_idx",
        "apple_note_revisions",
        "CREATE INDEX IF NOT EXISTS apple_note_revisions_ingested_at_idx ON @apple_note_revisions (ingested_at)",
    ),
    IndexSpec(
        "apple_voice_memos_files_ingested_at_idx",
        "apple_voice_memos_files",
        "CREATE INDEX IF NOT EXISTS apple_voice_memos_files_ingested_at_idx ON @apple_voice_memos_files (ingested_at)",
    ),
    IndexSpec(
        "alice_voice_recordings_ingested_at_idx",
        "alice_voice_recordings",
        "CREATE INDEX IF NOT EXISTS alice_voice_recordings_ingested_at_idx ON @alice_voice_recordings (ingested_at)",
    ),
    IndexSpec(
        "whatsapp_messages_ingested_at_idx",
        "whatsapp_messages",
        "CREATE INDEX IF NOT EXISTS whatsapp_messages_ingested_at_idx ON @whatsapp_messages (ingested_at)",
    ),
    *_ai_conversation_event_index_specs(),
    # The shared enrichment table is only ~100k rows but ~0.5 GB of extracted
    # text, so the freshness collector's cost guard would skip an unindexed
    # max(updated_at) and leave every attachment/media/caption enrichment
    # pipeline unmeasurable. A ~2 MB timestamp index buys the whole enrichment
    # layer a real "last produced a result at".
    IndexSpec(
        "file_attachment_enrichments_updated_at_idx",
        "file_attachment_enrichments",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS file_attachment_enrichments_updated_at_idx "
        "ON @file_attachment_enrichments (updated_at)",
    ),
)

# Indexes that used to exist but have been superseded. Dropped idempotently
# during _ensure_indexes so existing deployments converge with fresh installs.
POSTGRES_OBSOLETE_INDEXES: tuple[tuple[str, str], ...] = (
    # Replaced by the covering search_chunks_sha_cover_idx and
    # search_chunks_ts_chunk_sha_idx (same keys, INCLUDE columns added).
    ("search_chunks_sha_idx", "search_chunks"),
    ("search_chunks_ts_chunk_idx", "search_chunks"),
    # Replaced by the full-coverage slack_messages_text_trgm_idx.
    ("slack_messages_text_trgm_live_idx", "slack_messages"),
    # Legacy per-source BM25 indexes from the pre-timeline cross-source search.
    ("gmail_messages_subject_bm25_idx", "gmail_messages"),
    ("gmail_messages_body_text_bm25_idx", "gmail_messages"),
    ("gmail_messages_body_markdown_bm25_idx", "gmail_messages"),
    ("gmail_attachments_filename_bm25_idx", "gmail_attachments"),
    ("gmail_attachment_enrichments_text_bm25_idx", "file_attachment_enrichments"),
    ("file_attachment_enrichments_text_bm25_idx", "file_attachment_enrichments"),
    ("slack_messages_text_bm25_idx", "slack_messages"),
    ("slack_conversations_name_bm25_idx", "slack_conversations"),
    ("slack_conversations_topic_bm25_idx", "slack_conversations"),
    ("slack_conversations_purpose_bm25_idx", "slack_conversations"),
    ("slack_files_name_bm25_idx", "slack_files"),
    ("slack_files_title_bm25_idx", "slack_files"),
    ("apple_notes_title_bm25_idx", "apple_notes"),
    ("apple_notes_body_bm25_idx", "apple_notes"),
    ("apple_note_revisions_body_bm25_idx", "apple_note_revisions"),
    ("apple_messages_body_bm25_idx", "apple_messages"),
    ("apple_message_attachments_filename_bm25_idx", "apple_message_attachments"),
    ("whatsapp_messages_body_bm25_idx", "whatsapp_messages"),
    ("whatsapp_chats_name_bm25_idx", "whatsapp_chats"),
    ("whatsapp_media_items_filename_bm25_idx", "whatsapp_media_items"),
    ("apple_voice_memos_transcript_bm25_idx", "apple_voice_memos_enrichments"),
    ("apple_voice_memos_title_bm25_idx", "apple_voice_memos_enrichments"),
    ("apple_voice_memos_summary_bm25_idx", "apple_voice_memos_enrichments"),
    ("apple_voice_memos_participants_bm25_idx", "apple_voice_memos_enrichments"),
    ("apple_voice_memos_action_items_bm25_idx", "apple_voice_memos_enrichments"),
    ("calendar_events_summary_bm25_idx", "calendar_events"),
    ("calendar_events_description_bm25_idx", "calendar_events"),
    ("calendar_events_location_bm25_idx", "calendar_events"),
    ("calendar_events_attendees_bm25_idx", "calendar_events"),
    ("contact_cards_name_bm25_idx", "contact_cards"),
    ("contact_cards_organization_bm25_idx", "contact_cards"),
    ("contact_cards_job_title_bm25_idx", "contact_cards"),
    ("contact_cards_notes_bm25_idx", "contact_cards"),
    ("agent_run_events_text_bm25_idx", "agent_run_events"),
    ("upstream_mutations_title_bm25_idx", "upstream_mutations"),
    ("upstream_mutation_requests_title_bm25_idx", "upstream_mutation_requests"),
    ("upstream_mutation_requests_reason_bm25_idx", "upstream_mutation_requests"),
    ("google_drive_files_name_bm25_idx", "google_drive_files"),
    ("google_drive_file_texts_text_bm25_idx", "google_drive_file_texts"),
    # Raw-table text-scan trigram indexes retired 2026-07: production usage
    # counters showed them essentially never scanned (~13 GB of dead weight)
    # while the timeline search document covers the same text through
    # timeline.search_text() / timeline.search_text_exact(). Raw tables serve
    # structured predicates; text search belongs to the timeline layer.
    # (from/subject/snippet trgm stay: the voice-memo identity hints and other
    # structured sender/subject lookups ride them.)
    ("gmail_messages_body_text_trgm_idx", "gmail_messages"),
    ("gmail_messages_body_markdown_trgm_idx", "gmail_messages"),
    ("gmail_messages_body_html_trgm_idx", "gmail_messages"),
    ("slack_messages_text_trgm_idx", "slack_messages"),
    ("apple_messages_body_trgm_idx", "apple_messages"),
    ("google_drive_files_name_trgm_idx", "google_drive_files"),
    # Out-of-band production index from the unmerged pdw-slow-query-diagnosis
    # branch (commit 5300f75); never scanned in production (the recipient
    # index from that branch was codified instead — see
    # gmail_messages_recipients_array_idx in POSTGRES_INDEXES).
    ("slack_messages_live_human_thread_scan_idx", "slack_messages"),
    # Timeline btrees with zero lifetime scans; the kind filter rides the
    # time/priority indexes. (timeline_events_seq_idx was retired here too,
    # then revived in POSTGRES_INDEXES: the search-chunk builder pages by seq.)
    ("timeline_events_kind_time_idx", "timeline_events"),
)



POSTGRES_INSERT_PAGE_SIZES = {
    "apple_notes": 50,
    "apple_note_revisions": 50,
    "apple_note_attachments": 250,
    "apple_messages": 500,
    "apple_message_attachments": 500,
    "whatsapp_messages": 500,
    "whatsapp_media_items": 500,
    "chatgpt_events": 500,
    "claude_desktop_events": 500,
    "claude_code_events": 500,
    "codex_events": 500,
    "openclaw_events": 500,
    "pi_events": 500,
    "plaid_accounts": 500,
    "plaid_transactions": 500,
    "plaid_investment_securities": 500,
    "plaid_investment_holdings": 500,
    "plaid_investment_transactions": 500,
    "plaid_liabilities": 500,
}


ARRAY_COLUMNS = {
    # mart health: the base tables a view reads (logical ids) and the pipelines
    # they belong to, resolved from pg_depend. collation health: a unique
    # index's key columns.
    "input_tables",
    "input_pipelines",
    "key_columns",
    "label_ids",
    "to_addresses",
    "cc_addresses",
    "bcc_addresses",
    "recurrence",
    "available_products",
    "billed_products",
}

JSONB_COLUMNS_BY_TABLE = {
    "contact_cards": {
        "emails",
        "phones",
        "addresses",
        "organizations",
        "urls",
        "nicknames",
        "groups",
        "dates",
        "photos",
        "raw_json",
    },
    "apple_contact_cards": {
        "emails",
        "phones",
        "addresses",
        "organizations",
        "urls",
        "nicknames",
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
    "apple_photos_files": {"raw_metadata_json"},
    "timeline_events": {
        "source_pk",
        "metadata",
    },
    "whoop_profiles": {"raw_json"},
    "whoop_body_measurements": {"raw_json"},
    "whoop_cycles": {"score_json", "raw_json"},
    "whoop_recoveries": {"score_json", "raw_json"},
    "whoop_sleeps": {"stage_summary_json", "sleep_needed_json", "score_json", "raw_json"},
    "whoop_workouts": {"zone_durations_json", "score_json", "raw_json"},
    "whoop_private_cycles": {"raw_json"},
    "whoop_private_sleeps": {"raw_json"},
    "whoop_private_recoveries": {"raw_json"},
    "whoop_private_workouts": {
        "zone_durations_json",
        "zone_durations_v2_json",
        "gps_data_json",
        "raw_json",
    },
    "whoop_private_sleep_events": {"raw_json"},
    "whoop_private_heart_rate_samples": {"raw_json"},
    "whoop_private_journal_entries": {"raw_json"},
    "whoop_private_sports": {"raw_json"},
    # Tier-2 BFF payloads: faithful raw only. See docs/whoop-private-api.md --
    # a typed column over a UI payload goes quietly null when WHOOP restyles.
    "whoop_private_documents": {"raw_json"},
    "plaid_items": {"error_json", "raw_json"},
    "plaid_accounts": {"raw_json"},
    "plaid_transactions": {"category_json", "raw_json"},
    "plaid_investment_securities": {"raw_json"},
    "plaid_investment_holdings": {"raw_json"},
    "plaid_investment_transactions": {"raw_json"},
    "plaid_liabilities": {"raw_json"},
    "manual_finance_documents": {"raw_metadata_json"},
    "manual_finance_extractions": {
        "transactions_json",
        "balances_json",
        "valuations_json",
        "positions_json",
        "commitments_json",
        "uncertainties_json",
        "raw_result_json",
    },
    "alice_voice_recordings": {"raw_metadata_json"},
    "alice_voice_recording_artifacts": {"raw_metadata_json"},
}

JSONB_ARRAY_COLUMNS_BY_TABLE = {
    "contact_cards": {
        "emails",
        "phones",
        "addresses",
        "organizations",
        "urls",
        "nicknames",
        "groups",
        "photos",
    },
    "apple_contact_cards": {
        "emails",
        "phones",
        "addresses",
        "organizations",
        "urls",
        "nicknames",
        "groups",
        "photos",
    },
    "google_drive_files": {
        "parents_json",
        "owners_json",
    },
    "plaid_transactions": {"category_json"},
    "manual_finance_extractions": {
        "transactions_json",
        "balances_json",
        "valuations_json",
        "positions_json",
        "commitments_json",
        "uncertainties_json",
    },
}

# Money in the finance ledger is exact NUMERIC, never double precision. The
# classification is per-table (like JSONB_COLUMNS_BY_TABLE) because several
# ledger column names ("amount", "value") collide with raw-source columns
# that are already claimed by the global FLOAT_COLUMNS set; the per-table
# check runs first in _postgres_type/_default_sql.
NUMERIC_COLUMNS_BY_TABLE = {
    "finance_observations": {"value"},
    "finance_transactions": {"amount"},
    # Share counts and prices are exact NUMERIC for the same reason money is:
    # a fractional-share position accumulated over hundreds of buys must not
    # drift through binary floating point.
    "finance_security_transactions": {"quantity", "price", "amount", "fees"},
    "finance_tax_lots": {
        "quantity",
        "quantity_remaining",
        "cost_per_unit",
        "cost_basis",
        "cost_basis_remaining",
        "proceeds",
        "realized_gain",
    },
    "manual_finance_extractions": {"closing_balance"},
    "receipt_transaction_receipts": {
        "total",
        "subtotal",
        "tax",
        "tip",
        "amount_charged",
    },
}

# Day-granularity columns (DATE, not timestamptz): an observation is "the
# value on this day", not an instant; a statement period is a day range.
DATE_COLUMNS = {
    "as_of",
    "period_start",
    "period_end",
    # a trade settles on a day, and a lot's holding period is counted in days
    "trade_date",
    "acquired_on",
    "disposed_on",
    # the date printed on a receipt is a day, not an instant
    "purchased_at",
    # WHOOP private: a journal entry is logged for a user-local calendar day,
    # and a cycle reports the day(s) it is awake for -- neither is an instant.
    "day",
    "day_start",
    "day_end",
}


def _is_numeric_column(table: str | None, column: str) -> bool:
    return column in NUMERIC_COLUMNS_BY_TABLE.get(table or "", set())


# Columns whose name is claimed by a global type set but which are plain text
# in this particular table. Checked before every global set, exactly like
# NUMERIC_COLUMNS_BY_TABLE, so one source's label column cannot be forced into
# another source's numeric column of the same name.
#
# `state` is the live example: apple_message_chats stores a numeric chat state,
# while WHOOP's private API stores a label ("COMPLETE"). Without this override
# the label lands in a bigint column and every insert fails.
TEXT_COLUMNS_BY_TABLE = {
    "whoop_private_sleeps": {"state"},
    "whoop_private_recoveries": {"state"},
}


def _is_text_column(table: str | None, column: str) -> bool:
    return column in TEXT_COLUMNS_BY_TABLE.get(table or "", set())

TIMESTAMP_COLUMNS = {
    "newest_session_at",
    "ran_at",
    "amcheck_at",
    # search embedding drain cursors (search_chunk_sync_state)
    "embed_fresh_built_at",
    "embed_cursor_ts",
    # timeline: when the coverage reconcile last swept this adapter
    "last_reconcile_at",
    # backups: newest backup of each type, and when WAL last shipped
    "last_full_at",
    "last_diff_at",
    "last_incr_at",
    "last_archived_at",
    "last_restore_verified_at",
    "oldest_pending_at",
    "last_success_at",
    # mart health: the stalest input pipeline's last write. (When the view's
    # current definition hash was first observed reuses first_seen_at.)
    "stalest_pipeline_at",
    # search index: when a chunk was (re)built / a text embedded
    "built_at",
    "embedded_at",
    # receipts: the last transaction research attempt drives its retry window
    "last_attempt_at",
    # slack file fingerprints: when the backoff lets this file be retried.
    # Every warehouse column is NOT NULL, so a terminal row carries the epoch
    # sentinel rather than NULL ("no retry scheduled").
    "next_attempt_at",
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
    "event_ts",
    "end_ts",
    "ingest_ts",
    "first_seen_at",
    "backfill_cursor_event_ts",
    "watermark_ingest_ts",
    "last_run_at",
    "watermark_updated_at",
    "linked_at",
    "consent_expiration_time",
    "posted_at",
    "authorized_at",
    "close_price_as_of",
    "institution_price_as_of",
    "transaction_at",
    "next_payment_due_at",
    "last_synced_at",
    "captured_at",
    "capture_ts",
    "observed_at",
    # pipeline freshness snapshot
    "last_write_at",
    "newest_event_at",
    "last_error_at",
    "collected_at",
    # WHOOP private (app API)
    "published_at",
    "sample_at",
    "ended_at",
    "predicted_end",
    "optimal_sleep_start",
    "optimal_sleep_end",
    "access_expires_at",
    "refresh_expires_at",
}

INTEGER_COLUMNS = {
    "recordings_seen",
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
    "cpu_count",
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
    "admin_calls",
    "events_7d",
    "events_1d",
    # backups: counts and sizes, plus last_attempt_ok as the warehouse's
    # bigint 0/1 boolean convention.
    "backup_count",
    "repo_bytes",
    "wal_ready_count",
    "last_restore_rows",
    "archived_count",
    "failed_count",
    "last_attempt_ok",
    "configured",
    "pgvector_available",
    "timeline_max_seq",
    "chunk_cursor_seq",
    "caught_up",
    "processed_rows",
    "pending_count",
    "amcheck_ms",
    # mart health: input counts and the bounded non-empty probe's answer
    "input_count",
    "inputs_unmeasured",
    "stalest_pipeline_expected_seconds",
    "has_rows",
    # pipeline health: the event-time SLA and how many data tables yielded one
    "expected_event_interval_seconds",
    "event_tables_probed",
    # collation health: the corroborating unique-index divergence probe
    "dependent_indexes",
    "is_unique",
    "is_partial",
    "heap_rows",
    "distinct_keys",
    "excess_rows",
    "seq",
    # search index: chunk ordinal within an anchor, embedding token count,
    # and the chunk builder's timeline-seq watermark
    "chunk_index",
    "token_count",
    "last_seq",
    # slack file fingerprints: retry counter and downloaded size
    "attempts",
    "fetched_bytes",
    # receipts: boolean-ish flags and counters stored as bigint like the
    # rest of the warehouse's is_* columns
    "is_purchase_record",
    "settled",
    "attempt_count",
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
    "is_removed",
    "pending",
    # 0/1 flags on the securities ledger: whether a lot's cost basis is known
    # (a transferred-in lot's basis lives at the origin account), and whether a
    # trade's price was computed rather than printed on the document.
    "basis_known",
    "price_is_derived",
    "is_overdue",
    "is_google_native",
    "starred",
    "shared",
    "trashed",
    "is_excluded",
    "truncated",
    "char_count",
    "files_seen",
    "backfill_done",
    "backfill_rows",
    "incremental_rows",
    "whoop_user_id",
    "average_heart_rate",
    "max_heart_rate",
    "resting_heart_rate",
    "recovery_score",
    "user_calibrating",
    "nap",
    "v1_id",
    "sport_id",
    "total_in_bed_time_milli",
    "total_awake_time_milli",
    "total_no_data_time_milli",
    "total_light_sleep_time_milli",
    "total_slow_wave_sleep_time_milli",
    "total_rem_sleep_time_milli",
    "sleep_cycle_count",
    "disturbance_count",
    "width",
    "height",
    "best_file_size_bytes",
    "thumbnail_size_bytes",
    "duration_seconds",
    # pipeline freshness snapshot
    "expected_data_interval_seconds",
    "expected_run_interval_seconds",
    "row_estimate",
    "byte_size",
    "table_count",
    "tables_probed",
    "tables_skipped",
    "state_rows",
    "state_error_rows",
    "state_attention_rows",
    "probe_ms",
    # WHOOP private (app API). Counts, beats-per-minute samples, and the 0/1
    # flags the warehouse stores as bigint rather than boolean.
    "day_avg_heart_rate",
    "day_max_heart_rate",
    "heart_rate",
    "step_seconds",
    "total_wake_events",
    "disturbances",
    "cycles_count",
    "total_steps",
    "history_size",
    "is_nap",
    "calibrating",
    "has_gps",
    "has_survey",
    "is_current",
}

FLOAT_COLUMNS = {
    # search_benchmark_runs: host saturation sampled beside the latency probes
    "io_pressure_full_avg10",
    "cpu_pressure_some_avg10",
    "load_1m",
    "confidence",
    "calendar_confidence",
    "height_meter",
    "weight_kilogram",
    "strain",
    "kilojoule",
    "hrv_rmssd_milli",
    "spo2_percentage",
    "skin_temp_celsius",
    "respiratory_rate",
    "sleep_performance_percentage",
    "sleep_consistency_percentage",
    "sleep_efficiency_percentage",
    "percent_recorded",
    "distance_meter",
    "altitude_gain_meter",
    "altitude_change_meter",
    "available_balance",
    "current_balance",
    "limit_balance",
    "amount",
    "close_price",
    "quantity",
    "institution_value",
    "institution_price",
    "cost_basis",
    "price",
    "fees",
    "last_payment_amount",
    "last_statement_balance",
    "minimum_payment_amount",
    "origination_principal_amount",
    "outstanding_interest_amount",
    "latitude",
    "longitude",
    "match_score",
    # WHOOP private (app API). Scores, strains and durations arrive as the
    # provider's own reals; nothing here is money, so double precision is the
    # right storage and NUMERIC would be false precision.
    "score",
    "day_strain",
    "scaled_strain",
    "day_kilojoules",
    "intensity_score",
    "raw_intensity_score",
    "cumulative_workout_intensity",
    "kilojoules",
    "msk_score",
    "sleep_need",
    "latency",
    "arousal_time",
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
    "sleep_consistency",
    "projected_score",
    "projected_sleep",
    # The private API's HRV is SECONDS; hrv_rmssd_milli beside it is the same
    # measurement in the unit every other WHOOP relation uses.
    "hrv_rmssd_seconds",
    "spo2",
    "prob_covid",
    "hr_baseline",
    "hrv_component",
    "rhr_component",
    "recovery_rate",
}

# Order matters only for readability; _ensure_table_group creates each with
# CREATE TABLE IF NOT EXISTS and then applies the group's indexes.
_WHOOP_PRIVATE_TABLES = (
    "whoop_private_cycles",
    "whoop_private_sleeps",
    "whoop_private_recoveries",
    "whoop_private_workouts",
    "whoop_private_sleep_events",
    "whoop_private_heart_rate_samples",
    "whoop_private_journal_entries",
    "whoop_private_sports",
    "whoop_private_documents",
    "whoop_private_sync_state",
    "whoop_private_sessions",
)

_WHATSAPP_TABLES = (
    "whatsapp_chats",
    "whatsapp_chat_participants",
    "whatsapp_contacts",
    "whatsapp_messages",
    "whatsapp_media_items",
)

_PHOTO_TABLES = tuple(PHOTO_SOURCE_RELATIONS.values()) + (
    "photo_assets",
    "photo_asset_files",
    "media_fingerprints",
    # The marts_photos.photos caption join reads the shared enrichment table and the
    # enrichment candidate query counts agent_runs failures, so the photos
    # ensure must be able to run first on a fresh schema (voice-memos
    # precedent).
    "file_attachment_enrichments",
    "agent_runs",
    "agent_run_events",
    "agent_run_tool_calls",
)

_AI_EVENT_TABLE_BY_SOURCE = {
    "chatgpt": "chatgpt_events",
    "claude_desktop": "claude_desktop_events",
    "claude_code": "claude_code_events",
    "codex": "codex_events",
    "openclaw": "openclaw_events",
    "pi": "pi_events",
}


class PostgresWarehouse:
    def __init__(self, postgres_database_url: str, *, schema: str = "public") -> None:
        normalized = normalize_postgres_url(postgres_database_url)
        if not normalized:
            raise ValueError("POSTGRES_DATABASE_URL must be set")
        self._database_url = normalized
        # `schema` is a namespace prefix for test/alternate deployments. In the
        # normal production case (`public`) canonical schemas are named exactly
        # gmail, slack, marts, private, ... . In tests we use e.g.
        # pdw_test_x_gmail, pdw_test_x_slack, ... so independent tests do not
        # collide in the shared Postgres database.
        self._schema = _validate_identifier(schema)
        self._query_role = _validate_identifier(os.getenv("PDW_QUERY_POSTGRES_ROLE", "pdw_query"))
        self._connection = psycopg2.connect(normalized)
        self._connection.autocommit = True
        self._ensured_index_names: set[str] = set()
        self._pg_trgm_ensured = False
        self._pg_textsearch_ensured = False
        self._pgvector_ensured = False
        self._ensure_canonical_schemas()
        self._set_search_path()
        self._ensure_query_role()
        self._ensure_schema_comments()

    @property
    def schema_namespace(self) -> str:
        return self._schema

    @property
    def query_role(self) -> str:
        return self._query_role

    def physical_schema_names(self, *, include_hidden: bool = False) -> list[str]:
        return physical_schema_names(namespace=self._schema, include_hidden=include_hidden)

    def physical_schema_name(self, schema: str) -> str:
        return physical_schema_name(schema, namespace=self._schema)

    def _object_schema(self, logical_name: str) -> str:
        """Physical schema holding one catalog object (types/functions included)."""
        return self.physical_schema_name(canonical_relation(logical_name).schema)

    def sql_relation(self, logical_name: str) -> str:
        return canonical_relation(logical_name).sql(namespace=self._schema)

    def close(self) -> None:
        self._connection.close()

    def _ensure_canonical_schemas(self) -> None:
        # One round trip, not one per schema: every PostgresWarehouse
        # construction runs this (~30k/day in production across sensor ticks and
        # asset runs) and the reorg took the managed schema count from 31 to 40.
        statements = [
            f"CREATE SCHEMA IF NOT EXISTS {_identifier(schema)}"
            for schema in self.physical_schema_names(include_hidden=True)
        ]
        if self._schema != "public":
            statements.insert(0, f"CREATE SCHEMA IF NOT EXISTS {_identifier(self._schema)}")
        with self._connection.cursor() as cursor:
            cursor.execute("; ".join(statements))

    def _schema_comments(self) -> dict[str, str]:
        comments: dict[str, str] = {}
        for schema in CATALOG.schemas:
            comment = schema.comment
            if schema.name == CATALOG.start_here.schema:
                comment = f"{CATALOG.start_here.headline} {comment}"
            comments[self.physical_schema_name(schema.name)] = comment
        return comments

    def _ensure_schema_comments(self) -> None:
        """Publish the catalog's layer guidance as Postgres COMMENTs.

        An agent inspecting the database directly (psql's \\dn+, a generic SQL
        client) never calls schema_overview, so the same "start with timeline,
        then marts_*, then base_*" contract has to live in what Postgres itself
        hands out. Probe first and write only on drift: like the query-role
        sweep, an unconditional COMMENT ON per construction would churn
        pg_description on every sensor tick.
        """
        expected = self._schema_comments()
        current = {
            schema: comment
            for schema, comment in self._query(
                """
                SELECT n.nspname, obj_description(n.oid, 'pg_namespace')
                FROM pg_namespace n
                WHERE n.nspname = ANY(%s)
                """,
                (list(expected),),
            )
        }
        for schema, comment in expected.items():
            if current.get(schema) == comment:
                continue
            self._raw_command(f"COMMENT ON SCHEMA {_identifier(schema)} IS %s", (comment,))

    def _search_path_sql(self) -> str:
        # Every managed schema except `private`, so unqualified helper/index
        # references still resolve. Relation references never rely on this:
        # warehouse SQL names relations through the catalog (`@logical_id`),
        # because dozens of schemas now hold a `messages` or a `sync_state`.
        parts = [
            _identifier(self.physical_schema_name(schema))
            for schema in ALL_CANONICAL_SCHEMAS
            if schema != "private"
        ]
        parts.append("public")  # extensions such as pg_trgm / pg_textsearch live here.
        return "SET search_path TO " + ", ".join(parts)

    def _set_search_path(self) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(self._search_path_sql())

    def read_only_connection(self):
        """Open a dedicated connection for untrusted read-only SQL."""
        connection = psycopg2.connect(
            self._database_url,
            options="-c default_transaction_read_only=on -c statement_timeout=30000",
        )
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(self._search_path_sql())
            cursor.execute(f"SET ROLE {_identifier(self._query_role)}")
        return connection

    def ensure_tables(self) -> None:
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
                f"ALTER TABLE @gmail_attachments ADD COLUMN IF NOT EXISTS {_identifier(column)} text NOT NULL DEFAULT ''"
            )
        self._ensure_clean_gmail_inbox_view()
        self._ensure_files_mart_views()
        self._ensure_search_views_if_possible()

    def ensure_file_attachment_enrichment_tables(self) -> None:
        """Ensure the shared file_attachment_enrichments table exists.

        Used by the source-agnostic attachment enrichment runner so it can write
        results without depending on any one source's ensure_* path.
        """
        self._ensure_table_group(["file_attachment_enrichments"])
        self._ensure_search_views_if_possible()

    def ensure_calendar_tables(self) -> None:
        self._ensure_table_group(["calendar_events", "calendar_sync_state"])
        self._ensure_clean_calendar_transcript_views_if_possible()
        self._ensure_search_views_if_possible()

    def ensure_contacts_tables(self) -> None:
        self._ensure_table_group(["contact_cards", "contact_sync_state", "apple_contact_cards"])
        self._command("ALTER TABLE @contact_cards ADD COLUMN IF NOT EXISTS nicknames jsonb NOT NULL DEFAULT '[]'::jsonb")
        self._command("ALTER TABLE @apple_contact_cards ADD COLUMN IF NOT EXISTS nicknames jsonb NOT NULL DEFAULT '[]'::jsonb")
        rebuild_apple_messages_view = self._prepare_contacts_view_replacement()
        self._ensure_clean_contacts_view()
        self._ensure_clean_contact_points_view()
        if rebuild_apple_messages_view:
            messages = canonical_relation("apple_messages").with_namespace(self._schema)
            if self._physical_table_exists(schema=messages.schema, table=messages.name):
                self._ensure_clean_apple_messages_view()
        self._ensure_search_views_if_possible()

    def ensure_apple_contacts_tables(self) -> None:
        self.ensure_contacts_tables()

    def ensure_plaid_tables(self) -> None:
        self._ensure_table_group(
            [
                "plaid_items",
                "plaid_item_tokens",
                "plaid_accounts",
                "plaid_transactions",
                "plaid_investment_securities",
                "plaid_investment_holdings",
                "plaid_investment_transactions",
                "plaid_liabilities",
                "plaid_sync_state",
            ]
        )
        self._ensure_plaid_finance_mart_views()

    def _ensure_query_role(self) -> None:
        # The sweep in _ensure_query_role_locked rewrites the ACL of every
        # table in every managed schema, every schema's nspacl, and two
        # pg_default_acl rows per schema. Every PostgresWarehouse construction
        # ran it — roughly 30k sensor ticks plus 4k asset runs a day — which
        # churned pg_class by 8M row updates against 907 live rows, kept
        # autovacuum permanently busy on the catalog, and collided with
        # concurrent DDL often enough to fail ~20 sensor ticks a day with
        # "tuple concurrently updated".
        #
        # Probe first and write only when the privileges have actually
        # drifted, so the steady state is one cheap catalog read and zero
        # catalog writes. Drift is self-correcting: a table created without
        # SELECT for the query role is detected on the next construction, and
        # repairing it also re-applies the default privileges that keep later
        # tables readable without another sweep.
        if not self._query_role_setup_needed():
            return
        for attempt in range(QUERY_ROLE_SETUP_ATTEMPTS):
            try:
                self._run_query_role_setup()
                return
            except psycopg2.Error as exc:
                last_attempt = attempt == QUERY_ROLE_SETUP_ATTEMPTS - 1
                if last_attempt or QUERY_ROLE_CONCURRENT_UPDATE_MESSAGE not in str(exc):
                    raise
                time.sleep(QUERY_ROLE_SETUP_RETRY_SECONDS * (attempt + 1))

    def _run_query_role_setup(self) -> None:
        # Serialize competing sweeps across processes with a transaction-scoped
        # advisory lock, then re-check under it: another process may have
        # repaired the same drift while we waited.
        with self._connection.cursor() as cursor:
            cursor.execute("BEGIN")
            try:
                cursor.execute("SELECT pg_advisory_xact_lock(%s)", (QUERY_ROLE_SETUP_LOCK_ID,))
                if self._query_role_setup_needed():
                    self._ensure_query_role_locked()
                cursor.execute("COMMIT")
            except Exception:
                cursor.execute("ROLLBACK")
                raise

    def _query_role_setup_needed(self) -> bool:
        """Report whether the query role's privileges have drifted.

        Read-only: it never touches the catalog, so the common "everything is
        already granted" case costs one indexless scan of pg_class/pg_proc
        instead of a few hundred GRANT statements.

        The contract it checks is the catalog's: every relation and function in
        a discoverable schema readable, every allowlisted object outside them
        readable, and everything else — ``ops`` beyond the allowlist,
        ``private`` in full — unreachable for the role AND for PUBLIC.
        """

        schemas = self.physical_schema_names()
        allowed_schemas, allowed_relations, allowed_functions = self._query_role_allowlist()
        denied_schemas = [
            self.physical_schema_name(schema) for schema in CATALOG.denied_schemas()
        ]
        denied_relations = [
            f"{self.physical_schema_name(obj.schema)}.{obj.name}"
            for obj in CATALOG.objects
            if obj.query_access == "denied" and obj.is_relation
        ]
        with self._connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (self._query_role,))
            if cursor.fetchone() is None:
                return True
            cursor.execute(
                """
                SELECT
                    -- the connecting user must be able to hand out the role's grants
                    NOT pg_has_role(current_user, %(role)s, 'MEMBER')
                    -- every schema the role reaches must exist and be usable
                 OR EXISTS (
                        SELECT 1
                        FROM unnest(%(schemas)s::text[] || %(allowed_schemas)s::text[]) AS s(name)
                        LEFT JOIN pg_namespace n ON n.nspname = s.name
                        WHERE n.oid IS NULL
                           OR NOT has_schema_privilege(%(role)s, n.oid, 'USAGE')
                    )
                    -- ...every relation and function in the public ones readable
                 OR EXISTS (
                        SELECT 1
                        FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = ANY(%(schemas)s::text[])
                          AND c.relkind = ANY (ARRAY['r', 'v', 'm', 'p', 'f']::"char"[])
                          AND NOT has_table_privilege(%(role)s, c.oid, 'SELECT')
                    )
                 OR EXISTS (
                        SELECT 1
                        FROM pg_proc p
                        JOIN pg_namespace n ON n.oid = p.pronamespace
                        WHERE n.nspname = ANY(%(schemas)s::text[])
                          AND NOT has_function_privilege(%(role)s, p.oid, 'EXECUTE')
                    )
                    -- ...and each individually allowlisted object outside them
                 OR EXISTS (
                        SELECT 1
                        FROM unnest(%(allowed_relations)s::text[]) AS r(name)
                        WHERE to_regclass(r.name) IS NOT NULL
                          AND NOT has_table_privilege(%(role)s, to_regclass(r.name), 'SELECT')
                    )
                 OR EXISTS (
                        SELECT 1
                        FROM unnest(%(allowed_functions)s::text[]) AS f(name)
                        WHERE to_regprocedure(f.name) IS NOT NULL
                          AND NOT has_function_privilege(%(role)s, to_regprocedure(f.name), 'EXECUTE')
                    )
                    -- Credentials and un-allowlisted operational state must stay
                    -- unreachable, for the query role and for PUBLIC. Checked
                    -- against *any* privilege, mirroring the REVOKE ALL the
                    -- sweep issues, so this can never certify a narrower
                    -- boundary than it sets.
                 OR EXISTS (
                        SELECT 1
                        FROM pg_namespace n
                        WHERE n.nspname = ANY(%(denied_schemas)s::text[])
                          AND (
                                has_schema_privilege(%(role)s, n.oid, 'USAGE, CREATE')
                             OR has_schema_privilege('public', n.oid, 'USAGE, CREATE')
                          )
                    )
                 OR EXISTS (
                        SELECT 1
                        FROM unnest(%(denied_relations)s::text[]) AS r(name)
                        WHERE to_regclass(r.name) IS NOT NULL
                          AND (
                                has_table_privilege(%(role)s, to_regclass(r.name), %(any_privilege)s)
                             OR has_table_privilege('public', to_regclass(r.name), %(any_privilege)s)
                          )
                    )
                """,
                {
                    "role": self._query_role,
                    "schemas": schemas,
                    "allowed_schemas": allowed_schemas,
                    "allowed_relations": allowed_relations,
                    "allowed_functions": allowed_functions,
                    "denied_schemas": denied_schemas,
                    "denied_relations": denied_relations,
                    "any_privilege": "SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER",
                },
            )
            return bool(cursor.fetchone()[0])

    def _query_role_allowlist(self) -> tuple[list[str], list[str], list[str]]:
        """Objects granted individually outside the blanket-granted schemas.

        ``ops`` relations the app's own timeline/mutation surfaces read, and the
        ``internal`` helper the inbox mart depends on. Everything else in those
        schemas stays unreadable, so ``ops`` is not made publicly queryable just
        to keep the operational UI working.
        """
        schemas: list[str] = []
        relations: list[str] = []
        functions: list[str] = []
        for obj in CATALOG.query_role_extra_objects():
            schema = self.physical_schema_name(obj.schema)
            if schema not in schemas:
                schemas.append(schema)
            qualified = f"{_identifier(schema)}.{_identifier(obj.name)}"
            if obj.is_relation:
                relations.append(qualified)
            elif obj.kind == "function":
                functions.append(f"{qualified}(text, integer)")
        return schemas, relations, functions

    def _ensure_query_role_locked(self) -> None:
        role = _identifier(self._query_role)
        role_literal = self._query_role.replace("'", "''")
        self._raw_command(
            f"""
            DO $role$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role_literal}') THEN
                    CREATE ROLE {role} NOLOGIN NOINHERIT;
                END IF;
            END
            $role$
            """
        )
        current_user = str(self._query("SELECT current_user")[0][0])
        self._raw_command(f"GRANT {role} TO {_identifier(current_user)}")

        revokes: list[str] = []
        for schema_name in CATALOG.denied_schemas():
            schema = _identifier(self.physical_schema_name(schema_name))
            revokes += [
                f"REVOKE ALL ON SCHEMA {schema} FROM PUBLIC",
                f"REVOKE ALL ON ALL TABLES IN SCHEMA {schema} FROM PUBLIC",
                f"REVOKE ALL ON SCHEMA {schema} FROM {role}",
                f"REVOKE ALL ON ALL TABLES IN SCHEMA {schema} FROM {role}",
            ]

        # ops holds both allowlisted and denied relations. Revoke the whole
        # schema, then re-grant exactly the allowlist below; ops carries no
        # default privileges, so a table created later stays unreadable until
        # the catalog says otherwise.
        for schema_name in CATALOG.hidden_schemas():
            if schema_name in CATALOG.denied_schemas():
                continue
            schema = _identifier(self.physical_schema_name(schema_name))
            revokes += [
                f"REVOKE ALL ON SCHEMA {schema} FROM PUBLIC",
                f"REVOKE ALL ON ALL TABLES IN SCHEMA {schema} FROM PUBLIC, {role}",
            ]
        self._raw_command("; ".join(revokes))

        grants: list[str] = []
        for schema_name in self.physical_schema_names():
            schema = _identifier(schema_name)
            grants += [
                f"GRANT USAGE ON SCHEMA {schema} TO {role}",
                f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {role}",
                f"GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {schema} TO {role}",
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT SELECT ON TABLES TO {role}",
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT EXECUTE ON FUNCTIONS TO {role}",
            ]
        self._raw_command("; ".join(grants))

        # The individually allowlisted objects outside those schemas. Grant only
        # what exists: ensure_* creates them after this sweep, and each one is
        # granted at creation (_apply_catalog_grant) plus by the next sweep the
        # drift probe triggers.
        allowed_schemas, allowed_relations, allowed_functions = self._query_role_allowlist()
        allowlist = [
            f"GRANT USAGE ON SCHEMA {_identifier(schema_name)} TO {role}"
            for schema_name in allowed_schemas
        ]
        existing_relations = {
            row[0]
            for row in self._query(
                "SELECT name FROM unnest(%s::text[]) AS r(name) WHERE to_regclass(r.name) IS NOT NULL",
                (allowed_relations,),
            )
        }
        allowlist += [f"GRANT SELECT ON {qualified} TO {role}" for qualified in existing_relations]
        existing_functions = {
            row[0]
            for row in self._query(
                "SELECT name FROM unnest(%s::text[]) AS f(name) WHERE to_regprocedure(f.name) IS NOT NULL",
                (allowed_functions,),
            )
        }
        allowlist += [
            f"GRANT EXECUTE ON FUNCTION {qualified} TO {role}" for qualified in existing_functions
        ]
        if allowlist:
            self._raw_command("; ".join(allowlist))

    def _ensure_plaid_finance_mart_views(self) -> None:
        # marts_finance.accounts / marts_finance.transactions are ledger views
        # now (see _ensure_finance_ledger_mart_views); only the plaid-specific
        # investment/liability passthroughs remain here.
        view_sql = [
            ("marts_finance_investment_holdings", """
            CREATE OR REPLACE VIEW @marts_finance_investment_holdings AS
            SELECT
                h.account,
                h.item_id,
                h.account_id,
                h.security_id,
                s.ticker_symbol,
                s.name AS security_name,
                s.type AS security_type,
                h.quantity,
                h.institution_value,
                h.institution_price,
                h.institution_price_as_of,
                h.cost_basis,
                h.iso_currency_code,
                h.unofficial_currency_code,
                h.synced_at
            FROM @plaid_investment_holdings AS h
            LEFT JOIN @plaid_investment_securities AS s
              ON s.account = h.account
             AND s.security_id = h.security_id
            """),
            ("marts_finance_investment_transactions", """
            CREATE OR REPLACE VIEW @marts_finance_investment_transactions AS
            SELECT
                t.account,
                t.item_id,
                t.account_id,
                t.investment_transaction_id,
                t.security_id,
                s.ticker_symbol,
                s.name AS security_name,
                t.transaction_at,
                t.name,
                t.quantity,
                t.amount,
                t.price,
                t.fees,
                t.type,
                t.subtype,
                t.iso_currency_code,
                t.unofficial_currency_code,
                t.synced_at
            FROM @plaid_investment_transactions AS t
            LEFT JOIN @plaid_investment_securities AS s
              ON s.account = t.account
             AND s.security_id = t.security_id
            """),
            ("marts_finance_liabilities", """
            CREATE OR REPLACE VIEW @marts_finance_liabilities AS
            SELECT
                account,
                item_id,
                account_id,
                liability_type,
                last_payment_amount,
                last_statement_balance,
                minimum_payment_amount,
                next_payment_due_at,
                origination_principal_amount,
                outstanding_interest_amount,
                is_overdue,
                iso_currency_code,
                unofficial_currency_code,
                synced_at
            FROM @plaid_liabilities
            """),
            # Plaid records a broken Item by writing the API's error payload to
            # base_plaid.items.error_json — and until this view, nothing in the
            # warehouse ever read that column back. The Capital One Item sat in
            # ITEM_ERROR / NO_ACCOUNTS with its transactions frozen while the
            # only visible trace was a count of 'action_required' rows rolled
            # up in marts_ops.pipeline_health, which says how many Items need
            # attention but never which ones. Naming the Item, its institution,
            # and the accounts that stop updating with it is the whole point.
            #
            # The run deliberately stays green when this fires (see the note in
            # plaid_sync.py: failing the asset on a broken Item once produced
            # 262 consecutive failed runs over five days and buried every other
            # signal), so a readable row here is the compensating control.
            ("marts_ops_plaid_item_health", """
            CREATE OR REPLACE VIEW @marts_ops_plaid_item_health AS
            SELECT
                i.account,
                i.item_id,
                i.institution_name,
                i.linked_at,
                i.synced_at,
                COALESCE(i.error_json->>'error_code', '') AS error_code,
                COALESCE(i.error_json->>'error_type', '') AS error_type,
                COALESCE(i.error_json->>'error_message', '') AS error_message,
                CASE
                    WHEN COALESCE(i.error_json->>'error_code', '') <> '' THEN 'action_required'
                    WHEN COALESCE(array_length(dup.item_ids, 1), 0) > 0 THEN 'duplicate'
                    ELSE 'ok'
                END AS status,
                COALESCE(dup.item_ids, ARRAY[]::text[]) AS duplicate_item_ids,
                linked.account_count,
                COALESCE(linked.account_names, '') AS account_names,
                linked.newest_transaction_at,
                (EXTRACT(EPOCH FROM now() - linked.newest_transaction_at))::bigint
                    AS transaction_age_seconds
            FROM @plaid_items AS i
            LEFT JOIN LATERAL (
                SELECT
                    count(*)::bigint AS account_count,
                    string_agg(a.name, ', ' ORDER BY a.name) AS account_names,
                    max(t.newest_transaction_at) AS newest_transaction_at
                FROM @plaid_accounts AS a
                LEFT JOIN LATERAL (
                    SELECT max(posted_at) AS newest_transaction_at
                    FROM @plaid_transactions AS p
                    WHERE p.account = a.account
                      AND p.account_id = a.account_id
                ) AS t ON TRUE
                WHERE a.account = i.account
                  AND a.item_id = i.item_id
            ) AS linked ON TRUE
            -- A re-link can mint a SECOND live Item for the same real
            -- accounts (2026-07-25, and again 2026-08-28). Both then sync
            -- and every balance is counted twice while each row reads ok,
            -- so an Item is only ok when no OTHER live Item at the same
            -- institution carries a live account with the same mask, type
            -- and subtype.
            LEFT JOIN LATERAL (
                SELECT array_agg(DISTINCT o.item_id ORDER BY o.item_id) AS item_ids
                FROM @plaid_accounts AS mine
                JOIN @plaid_accounts AS theirs
                  ON theirs.account = mine.account
                 AND theirs.item_id <> mine.item_id
                 AND theirs.mask = mine.mask
                 AND theirs.type = mine.type
                 AND theirs.subtype = mine.subtype
                 AND theirs.is_removed = 0
                JOIN @plaid_items AS o
                  ON o.account = theirs.account
                 AND o.item_id = theirs.item_id
                 AND o.institution_id = i.institution_id
                WHERE mine.account = i.account
                  AND mine.item_id = i.item_id
                  AND mine.is_removed = 0
                  AND mine.mask <> ''
            ) AS dup ON TRUE
            """),
        ]
        for logical, sql in view_sql:
            self._ensure_view(logical, sql)

    def record_pgbackrest_restore_drill(
        self,
        *,
        stanza: str,
        label: str,
        rows: int,
        note: str,
        verified_at: datetime | None = None,
    ) -> None:
        """Record that a backup was restored and counted.

        The row is keyed by stanza and normally written by the backup loop; a
        drill only touches the four restore columns, so it never disturbs the
        loop's facts and the loop never disturbs it. When no loop row exists
        yet (a fresh deployment) the drill founds the row with its own
        collected_at left at the epoch, so the view still reads `unknown`
        until the loop reports -- a restore record must not stand in for a
        backup report.
        """
        if not label.strip():
            raise ValueError("a restore drill must name the backup label it restored")
        if rows <= 0:
            raise ValueError("a restore drill must report the rows it counted (> 0)")
        self._command(
            """
            INSERT INTO @pgbackrest_health AS t
                (stanza, last_restore_verified_at, last_restore_label,
                 last_restore_rows, last_restore_note)
            VALUES (%s, COALESCE(%s, now()), %s, %s, %s)
            ON CONFLICT (stanza) DO UPDATE SET
                last_restore_verified_at = EXCLUDED.last_restore_verified_at,
                last_restore_label = EXCLUDED.last_restore_label,
                last_restore_rows = EXCLUDED.last_restore_rows,
                last_restore_note = EXCLUDED.last_restore_note
            """,
            (stanza, verified_at, label.strip(), int(rows), note.strip()),
        )

    def ensure_finance_tables(self) -> None:
        self._ensure_table_group(
            [
                "finance_accounts",
                "finance_account_links",
                "finance_observations",
                "finance_transactions",
                "finance_transaction_links",
                "finance_security_transactions",
                "finance_security_transaction_links",
                "finance_tax_lots",
            ]
        )
        # The lot/coverage views price positions from Plaid's current holdings,
        # so those tables must exist before the views are created. The ledger
        # runner already reads them; declaring the dependency here keeps a
        # finance-only ensure (a fresh schema, or the extraction runner) from
        # failing on a missing relation.
        self._ensure_table_group(
            [
                "plaid_accounts",
                "plaid_investment_securities",
                "plaid_investment_holdings",
            ]
        )
        self._ensure_finance_ledger_mart_views()

    def _ensure_finance_ledger_mart_views(self) -> None:
        # Each account contributes its single latest observation. Same-day
        # ties resolve by kind: balance (institution-authoritative) beats
        # principal beats valuation.
        kind_rank = "CASE o.kind WHEN 'balance' THEN 0 WHEN 'principal' THEN 1 ELSE 2 END"
        # Observation kinds that are facts about an account but NOT what it is
        # worth today. They are stored in the same table on purpose — the
        # ledger holds facts, and status is derived at read time — so every
        # reader of a VALUE must filter them out explicitly. Two of them are
        # incidents: a Schedule K-1's tax-basis capital sat in net worth beside
        # the same fund's NAV (double-counted, on two incompatible measures),
        # and an unfunded capital commitment had nowhere to live at all.
        # Mirrors NON_VALUE_OBSERVATION_KINDS in finance_ledger.py; the test
        # named there fails if the two lists drift.
        value_kinds = "o.kind NOT IN ('tax_basis', 'commitment', 'called_capital', 'unfunded_commitment')"
        self._ensure_view(
            "marts_finance_net_worth",
            f"""
            CREATE OR REPLACE VIEW @marts_finance_net_worth AS
            SELECT
                a.account_id,
                a.account,
                a.name,
                a.kind,
                a.side,
                a.currency,
                a.institution,
                a.mask,
                o.kind AS observation_kind,
                o.as_of,
                o.value,
                o.source,
                o.observed_at,
                CASE WHEN a.side = 'liability' THEN -o.value ELSE o.value END AS signed_value,
                -- Appended, never inserted: CREATE OR REPLACE VIEW only
                -- tolerates new columns at the end.
                --
                -- A net worth is only as current as its stalest input, and the
                -- manual sources have no pipeline SLA (the upload is `manual`
                -- on /pipelines). Measured 2026-08-26 the private-fund
                -- valuation was 4.5 months old and the mortgage 8 weeks, with
                -- nothing flagging either. Each account kind carries the
                -- refresh its source can honestly promise: Plaid balances land
                -- daily; a mortgage statement is monthly; property, vehicle and
                -- fund valuations are quarterly documents. `late` is past the
                -- interval, `stale` past three of them.
                (CURRENT_DATE - o.as_of)::bigint AS age_days,
                CASE a.kind
                    WHEN 'mortgage' THEN 35
                    WHEN 'property' THEN 120
                    WHEN 'vehicle' THEN 120
                    WHEN 'private_fund' THEN 120
                    WHEN 'receivable' THEN 120
                    WHEN 'other' THEN 120
                    ELSE 3
                END::bigint AS expected_refresh_days,
                CASE
                    WHEN (CURRENT_DATE - o.as_of) > 3 * CASE a.kind
                        WHEN 'mortgage' THEN 35
                        WHEN 'property' THEN 120
                        WHEN 'vehicle' THEN 120
                        WHEN 'private_fund' THEN 120
                        WHEN 'receivable' THEN 120
                        WHEN 'other' THEN 120
                        ELSE 3 END THEN 'stale'
                    WHEN (CURRENT_DATE - o.as_of) > CASE a.kind
                        WHEN 'mortgage' THEN 35
                        WHEN 'property' THEN 120
                        WHEN 'vehicle' THEN 120
                        WHEN 'private_fund' THEN 120
                        WHEN 'receivable' THEN 120
                        WHEN 'other' THEN 120
                        ELSE 3 END THEN 'late'
                    ELSE 'ok'
                END AS staleness
            FROM @finance_accounts AS a
            JOIN LATERAL (
                SELECT o.kind, o.as_of, o.value, o.source, o.observed_at
                FROM @finance_observations AS o
                WHERE o.account_id = a.account_id AND {value_kinds}
                ORDER BY o.as_of DESC, {kind_rank}, o.observed_at DESC
                LIMIT 1
            ) AS o ON TRUE
            """,
        )
        self._ensure_view(
            "marts_finance_net_worth_history",
            f"""
            CREATE OR REPLACE VIEW @marts_finance_net_worth_history AS
            WITH days AS (
                SELECT generate_series(
                    (SELECT min(as_of) FROM @finance_observations),
                    CURRENT_DATE,
                    interval '1 day'
                )::date AS day
            ),
            account_days AS (
                SELECT d.day, a.side, o.value
                FROM days AS d
                CROSS JOIN @finance_accounts AS a
                LEFT JOIN LATERAL (
                    SELECT o.value
                    FROM @finance_observations AS o
                    WHERE o.account_id = a.account_id AND o.as_of <= d.day AND {value_kinds}
                    ORDER BY o.as_of DESC, {kind_rank}, o.observed_at DESC
                    LIMIT 1
                ) AS o ON TRUE
            )
            SELECT
                day,
                SUM(CASE WHEN side = 'asset' THEN value ELSE 0 END) AS assets,
                SUM(CASE WHEN side = 'liability' THEN value ELSE 0 END) AS liabilities,
                SUM(CASE WHEN side = 'liability' THEN -value ELSE value END) AS net_worth
            FROM account_days
            WHERE value IS NOT NULL
            GROUP BY day
            """,
        )
        # The ledger read surface REPLACES the old plaid passthrough views of
        # the same names (different columns — _ensure_view drops and recreates
        # when CREATE OR REPLACE refuses).
        self._ensure_view(
            "marts_finance_accounts",
            f"""
            CREATE OR REPLACE VIEW @marts_finance_accounts AS
            SELECT
                a.account_id,
                a.account,
                a.name,
                a.kind,
                a.side,
                a.currency,
                a.institution,
                a.mask,
                o.value AS latest_value,
                o.as_of AS latest_as_of,
                o.kind AS latest_observation_kind,
                o.source AS latest_observation_source,
                a.created_at,
                a.updated_at
            FROM @finance_accounts AS a
            LEFT JOIN LATERAL (
                SELECT o.kind, o.as_of, o.value, o.source
                FROM @finance_observations AS o
                WHERE o.account_id = a.account_id AND {value_kinds}
                ORDER BY o.as_of DESC, {kind_rank}, o.observed_at DESC
                LIMIT 1
            ) AS o ON TRUE
            """,
        )
        # Unfunded capital is a real future cash obligation, and until 2026-08-27
        # it appeared nowhere in the model: a five-figure uncalled commitment to
        # a private fund was invisible in every finance surface, even though the
        # capital call notices stating it were in the corpus. It is deliberately NOT a
        # liability in net worth -- a commitment is contingent on the fund
        # calling it, and booking it as debt would make net worth disagree with
        # every statement -- so it gets its own read surface instead.
        self._ensure_view(
            "marts_finance_commitments",
            """
            CREATE OR REPLACE VIEW @marts_finance_commitments AS
            SELECT
                a.account_id,
                a.account,
                a.name,
                a.kind,
                a.institution,
                a.currency,
                c.as_of,
                c.committed,
                c.called,
                -- The document's own figure, NULL when it did not print one.
                c.unfunded AS unfunded_stated,
                -- What is actually still owed. A NULL `unfunded` read as "no
                -- obligation" is backwards -- it means the document was silent,
                -- not that the commitment is discharged -- so fall back to
                -- committed - called. Floored at zero because an SPV routinely
                -- calls slightly MORE than the subscription (fees), and a
                -- negative "still owed" is not a refund. Written as a CASE and
                -- not COALESCE(..., GREATEST(...)): GREATEST IGNORES its NULL
                -- arguments, so a subscription that states committed with no
                -- call yet would derive GREATEST(NULL, 0) = 0 and publish the
                -- whole obligation as "nothing owed" -- the exact reading this
                -- column exists to stop. Underivable stays NULL.
                CASE
                    WHEN c.unfunded IS NOT NULL THEN c.unfunded
                    WHEN c.committed IS NOT NULL AND c.called IS NOT NULL
                        THEN GREATEST(c.committed - c.called, 0)
                END AS unfunded,
                CASE
                    WHEN c.unfunded IS NOT NULL THEN 'stated'
                    WHEN c.committed IS NOT NULL AND c.called IS NOT NULL THEN 'derived'
                    ELSE 'unknown'
                END AS unfunded_basis,
                (CURRENT_DATE - c.as_of)::bigint AS age_days
            FROM @finance_accounts AS a
            JOIN LATERAL (
                SELECT
                    o.as_of,
                    max(o.value) FILTER (WHERE o.kind = 'commitment') AS committed,
                    max(o.value) FILTER (WHERE o.kind = 'called_capital') AS called,
                    max(o.value) FILTER (WHERE o.kind = 'unfunded_commitment') AS unfunded
                FROM @finance_observations AS o
                WHERE o.account_id = a.account_id
                  AND o.kind IN ('commitment', 'called_capital', 'unfunded_commitment')
                GROUP BY o.as_of
                ORDER BY o.as_of DESC
                LIMIT 1
            ) AS c ON TRUE
            """,
        )
        self._ensure_view(
            "marts_finance_transactions",
            f"""
            CREATE OR REPLACE VIEW @marts_finance_transactions AS
            SELECT
                t.transaction_id,
                t.account_id,
                a.account,
                a.name AS account_name,
                a.kind AS account_kind,
                a.side,
                a.institution,
                a.mask,
                t.posted_at,
                -- Signed: positive = inflow to the account (Plaid's
                -- positive-out amounts were negated at ledger ingest).
                t.amount,
                t.currency,
                t.description,
                t.merchant,
                t.pending,
                t.source
            FROM @finance_transactions AS t
            JOIN @finance_accounts AS a ON a.account_id = t.account_id
            """,
        )
        # Cross-source security trades. The plaid-only passthrough at
        # marts_finance.investment_transactions stays for Plaid drill-down;
        # this is the one that reaches back past Plaid's 730-day window.
        self._ensure_view(
            "marts_finance_security_transactions",
            """
            CREATE OR REPLACE VIEW @marts_finance_security_transactions AS
            SELECT
                t.transaction_id,
                t.account_id,
                a.account,
                a.name AS account_name,
                a.kind AS account_kind,
                a.institution,
                a.mask,
                t.security_key,
                t.ticker,
                t.cusip,
                t.security_name,
                t.asset_class,
                t.trade_date,
                t.side,
                t.quantity,
                -- Per quantity unit: per share for equities, per contract for
                -- options (statement option premiums are normalized by 100).
                -- 0 is the NOT NULL sentinel for "the document printed none";
                -- no real trade has a zero price or a zero amount.
                NULLIF(t.price, 0) AS price,
                NULLIF(t.amount, 0) AS amount,
                t.fees,
                t.currency,
                -- 1 when the price was computed from amount/quantity because
                -- the document did not print one.
                t.price_is_derived,
                t.source
            FROM @finance_security_transactions AS t
            JOIN @finance_accounts AS a ON a.account_id = t.account_id
            """,
        )
        # Lots joined to the latest price we have, so an open lot carries an
        # unrealized gain. Price comes from Plaid's current holding for the
        # same ticker; a lot in a security Plaid does not report keeps a NULL
        # market value rather than borrowing a stale one.
        self._ensure_view(
            "marts_finance_tax_lots",
            """
            CREATE OR REPLACE VIEW @marts_finance_tax_lots AS
            WITH latest_price AS (
                SELECT DISTINCT ON (upper(s.ticker_symbol))
                       upper(s.ticker_symbol) AS ticker,
                       h.institution_price AS price,
                       h.institution_price_as_of AS price_as_of
                FROM @plaid_investment_holdings AS h
                JOIN @plaid_investment_securities AS s
                  ON s.account = h.account AND s.security_id = h.security_id
                WHERE s.ticker_symbol <> '' AND s.ticker_symbol IS NOT NULL
                ORDER BY upper(s.ticker_symbol),
                         h.institution_price_as_of DESC,
                         h.synced_at DESC,
                         h.account,
                         h.security_id
            )
            SELECT
                l.lot_id,
                l.account_id,
                a.account,
                a.name AS account_name,
                a.kind AS account_kind,
                a.institution,
                a.mask,
                l.security_key,
                t.ticker,
                t.security_name,
                t.asset_class,
                -- Absence is written as a sentinel in the NOT NULL fact table
                -- and restored to NULL here, so no reader sees 1970 as a date
                -- or a 0 basis it could mistake for a free acquisition.
                NULLIF(l.acquired_on, '1970-01-01'::date) AS acquired_on,
                NULLIF(l.disposed_on, '1970-01-01'::date) AS disposed_on,
                l.status,
                l.term,
                l.method,
                l.basis_known,
                l.quantity,
                l.quantity_remaining,
                CASE WHEN l.basis_known = 1 THEN l.cost_per_unit END AS cost_per_unit,
                CASE WHEN l.basis_known = 1 THEN l.cost_basis END AS cost_basis,
                CASE WHEN l.basis_known = 1 THEN l.cost_basis_remaining END AS cost_basis_remaining,
                l.proceeds,
                CASE WHEN l.basis_known = 1 THEN l.realized_gain END AS realized_gain,
                p.price AS latest_price,
                p.price_as_of AS latest_price_as_of,
                CASE WHEN p.price IS NOT NULL
                     THEN round((l.quantity_remaining * p.price)::numeric, 2) END AS market_value,
                CASE WHEN p.price IS NOT NULL AND l.basis_known = 1
                     THEN round((l.quantity_remaining * p.price - l.cost_basis_remaining)::numeric, 2)
                     END AS unrealized_gain,
                l.acquired_source,
                l.opening_transaction_id
            FROM @finance_tax_lots AS l
            JOIN @finance_accounts AS a ON a.account_id = l.account_id
            LEFT JOIN LATERAL (
                SELECT st.ticker, st.security_name, st.asset_class
                FROM @finance_security_transactions AS st
                WHERE st.security_key = l.security_key
                ORDER BY st.ticker DESC, st.trade_date DESC
                LIMIT 1
            ) AS t ON TRUE
            -- Only equities are priced from Plaid holdings: an option contract
            -- shares the underlying's ticker and would otherwise be valued as
            -- though it were 100x-cheaper stock.
            LEFT JOIN latest_price AS p
              ON p.ticker = upper(t.ticker) AND t.asset_class = 'spot'
            """,
        )
        # How much of each position actually has a reconstructed lot history.
        # An agent asking "what are my returns since I bought in" must be able
        # to see that a position is 0% reconstructed instead of silently
        # trusting an aggregate cost basis with no acquisition date behind it.
        self._ensure_view(
            "marts_finance_position_coverage",
            """
            CREATE OR REPLACE VIEW @marts_finance_position_coverage AS
            WITH held AS (
                SELECT a.account_id,
                       upper(s.ticker_symbol) AS ticker,
                       sum(h.quantity::numeric) AS quantity_held,
                       sum(h.cost_basis::numeric) AS reported_cost_basis
                FROM @plaid_investment_holdings AS h
                JOIN @plaid_investment_securities AS s
                  ON s.account = h.account AND s.security_id = h.security_id
                JOIN @finance_account_links AS fl
                  ON fl.source = 'plaid' AND fl.account = h.account
                 AND fl.source_account_key = h.account_id
                JOIN @finance_accounts AS a ON a.account_id = fl.account_id
                WHERE h.quantity > 0
                  AND s.ticker_symbol <> '' AND s.ticker_symbol IS NOT NULL
                  AND upper(s.ticker_symbol) NOT LIKE 'CUR:%'
                  -- Money-market sweep vehicles (SPAXX and friends) are cash:
                  -- their basis IS their value and their "trades" are sweeps
                  -- no statement prints, so coverage against them is pure
                  -- basis_mismatch noise.
                  AND lower(coalesce(s.type, '')) <> 'cash'
                GROUP BY 1, 2
            ), reconstructed AS (
                SELECT l.account_id,
                       upper(t.ticker) AS ticker,
                       sum(l.quantity_remaining) AS quantity_with_lots,
                       sum(CASE WHEN l.basis_known = 1 THEN l.quantity_remaining ELSE 0 END)
                           AS quantity_with_known_basis,
                       sum(CASE WHEN l.basis_known = 1 THEN l.cost_basis_remaining ELSE 0 END)
                           AS reconstructed_cost_basis,
                       min(NULLIF(l.acquired_on, '1970-01-01'::date)) AS earliest_acquisition
                FROM @finance_tax_lots AS l
                LEFT JOIN LATERAL (
                    SELECT st.ticker, st.asset_class
                    FROM @finance_security_transactions AS st
                    WHERE st.security_key = l.security_key
                    ORDER BY st.ticker DESC, st.trade_date DESC
                    LIMIT 1
                ) AS t ON TRUE
                -- Same ticker gate the held side applies: a lot whose
                -- security never printed a symbol has nothing to compare
                -- against and would report as a nameless phantom position.
                WHERE l.status = 'open' AND t.asset_class = 'spot'
                  AND COALESCE(t.ticker, '') <> ''
                GROUP BY 1, 2
            ), covered_accounts AS (
                -- Accounts whose holdings plaid actually reports. Only for
                -- these does "no holding" mean something: a statement-only
                -- account has no holdings feed to disagree with, and judging
                -- its lots against silence would make every reconstructed
                -- position look broken.
                SELECT DISTINCT account_id FROM held
            ), positions AS (
                -- FULL join, not held-driven: open lots for a security the
                -- account does NOT hold produced no row at all, so the one
                -- signature of cross-account double-booking was invisible
                -- here. A brokerage's crypto statements were booked against
                -- its cash-brokerage account for six weeks, carrying open lots
                -- against a position that account never held, and this view
                -- reported the real crypto account 'complete' throughout.
                SELECT
                    COALESCE(held.account_id, r.account_id) AS account_id,
                    COALESCE(held.ticker, r.ticker) AS ticker,
                    COALESCE(held.quantity_held, 0) AS quantity_held,
                    COALESCE(held.reported_cost_basis, 0) AS reported_cost_basis,
                    held.account_id IS NOT NULL AS is_held,
                    r.quantity_with_lots,
                    r.quantity_with_known_basis,
                    r.reconstructed_cost_basis,
                    r.earliest_acquisition
                FROM held
                FULL OUTER JOIN reconstructed AS r
                  ON r.account_id = held.account_id AND r.ticker = held.ticker
            )
            SELECT
                p.account_id,
                a.name AS account_name,
                a.institution,
                a.mask,
                p.ticker,
                p.quantity_held,
                COALESCE(p.quantity_with_lots, 0) AS quantity_with_lots,
                COALESCE(p.quantity_with_known_basis, 0) AS quantity_with_known_basis,
                p.reported_cost_basis,
                p.reconstructed_cost_basis,
                CASE WHEN p.reconstructed_cost_basis IS NOT NULL
                     THEN p.reconstructed_cost_basis - p.reported_cost_basis
                     END AS basis_difference,
                p.earliest_acquisition,
                CASE WHEN p.quantity_held > 0
                     THEN round(
                         least(COALESCE(p.quantity_with_known_basis, 0), p.quantity_held)
                         / p.quantity_held * 100, 1)
                     END AS pct_quantity_with_basis,
                -- The percentage alone can only understate a problem: it is
                -- capped at 100, so a position holding MORE open lots than
                -- shares (a disposal we have no record of, e.g. shares sold in
                -- a month whose statement was never uploaded) would read as a
                -- clean 100%. That case gets its own status instead.
                CASE
                    WHEN NOT p.is_held THEN 'no_holding'
                    WHEN COALESCE(p.quantity_with_lots, 0) = 0 THEN 'none'
                    WHEN p.quantity_with_lots > p.quantity_held * 1.001
                        THEN 'lots_exceed_holding'
                    WHEN COALESCE(p.quantity_with_known_basis, 0) >= p.quantity_held * 0.999
                         AND abs(p.reconstructed_cost_basis - p.reported_cost_basis)
                             > greatest(0.02::numeric, abs(p.reported_cost_basis) * 0.001)
                        THEN 'basis_mismatch'
                    WHEN COALESCE(p.quantity_with_known_basis, 0) >= p.quantity_held * 0.999
                        THEN 'complete'
                    ELSE 'partial'
                END AS coverage_status
            FROM positions AS p
            JOIN @finance_accounts AS a ON a.account_id = p.account_id
            WHERE p.is_held OR p.account_id IN (SELECT account_id FROM covered_accounts)
            """,
        )
        self._ensure_account_freshness_mart_view()

    def _ensure_account_freshness_mart_view(self) -> None:
        """Per-account transaction recency, judged against the account's own cadence.

        ``marts_ops.table_freshness`` answers "is this table still being
        written to"; nothing answered "is this *account* still being written
        to", and the difference hid a four-month gap on one credit card inside
        a table that never stopped looking current. See the rationale block in
        ``pipeline_health.py`` for the incident and how the thresholds were
        chosen.

        Status is derived here rather than stored, for the same reason the
        pipeline health views derive theirs: a stored verdict keeps asserting
        itself long after whatever produced it stopped running. Every input is
        measured live off the ledger, which is small (~15k transactions) and
        indexed on ``(account_id, posted_at DESC)``, so the whole view is an
        index-only pass rather than something needing a collector.
        """
        baseline = f"make_interval(days => {ACCOUNT_BASELINE_MAX_DAYS})"
        self._ensure_view(
            "marts_finance_account_freshness",
            f"""
            CREATE OR REPLACE VIEW @marts_finance_account_freshness AS
            WITH last_txn AS (
                SELECT
                    account_id,
                    max(posted_at) AS last_transaction_at,
                    count(*)::bigint AS transaction_count
                FROM @finance_transactions
                GROUP BY account_id
            ),
            -- Intervals between consecutive transactions, ending at the
            -- account's last one. Anchoring to the last transaction rather than
            -- now() is what stops a long silence from diluting the very
            -- baseline it should be measured against.
            all_gaps AS (
                SELECT
                    t.account_id,
                    t.posted_at,
                    EXTRACT(EPOCH FROM t.posted_at - lag(t.posted_at) OVER (
                        PARTITION BY t.account_id ORDER BY t.posted_at
                    )) AS gap_seconds
                FROM @finance_transactions AS t
                JOIN last_txn AS l ON l.account_id = t.account_id
                WHERE t.posted_at > l.last_transaction_at - {baseline}
                  AND t.posted_at <= l.last_transaction_at
            ),
            -- Most recent N intervals, counted in intervals rather than days:
            -- a fixed span of days holds hundreds for a daily-use card and a
            -- handful for a monthly one, which would leave every slow account
            -- below the minimum and permanently unjudged.
            recent_gaps AS (
                SELECT
                    account_id,
                    gap_seconds,
                    row_number() OVER (
                        PARTITION BY account_id ORDER BY posted_at DESC
                    ) AS recency
                FROM all_gaps
                WHERE gap_seconds IS NOT NULL
            ),
            cadence AS (
                SELECT
                    account_id,
                    count(*)::bigint AS baseline_gaps,
                    percentile_cont({ACCOUNT_BASELINE_PERCENTILE})
                        WITHIN GROUP (ORDER BY gap_seconds) AS typical_gap_seconds
                FROM recent_gaps
                WHERE recency <= {ACCOUNT_BASELINE_GAPS}
                GROUP BY account_id
            ),
            measured AS (
                SELECT
                    a.account_id,
                    a.account,
                    a.name,
                    a.kind,
                    a.side,
                    a.institution,
                    a.mask,
                    l.last_transaction_at,
                    COALESCE(l.transaction_count, 0)::bigint AS transaction_count,
                    COALESCE(c.baseline_gaps, 0)::bigint AS baseline_gaps,
                    GREATEST(
                        c.typical_gap_seconds,
                        {ACCOUNT_MIN_EXPECTED_GAP_SECONDS}
                    ) AS expected_gap_seconds,
                    EXTRACT(EPOCH FROM now() - l.last_transaction_at) AS quiet_seconds
                FROM @finance_accounts AS a
                LEFT JOIN last_txn AS l ON l.account_id = a.account_id
                LEFT JOIN cadence AS c ON c.account_id = a.account_id
            )
            SELECT
                account_id,
                account,
                name,
                kind,
                side,
                institution,
                mask,
                last_transaction_at,
                transaction_count,
                baseline_gaps,
                expected_gap_seconds::bigint AS expected_gap_seconds,
                quiet_seconds::bigint AS quiet_seconds,
                round((quiet_seconds / expected_gap_seconds)::numeric, 2) AS quiet_ratio,
                CASE
                    -- Valuation-only accounts (a house, a car, a private fund)
                    -- have observations but never transactions. Silence is
                    -- their normal state, not a fault.
                    WHEN last_transaction_at IS NULL THEN 'no_transactions'
                    -- Too few measured intervals for a percentile to mean
                    -- anything. Reported, not judged.
                    WHEN baseline_gaps < {ACCOUNT_MIN_BASELINE_GAPS} THEN 'sparse'
                    WHEN quiet_seconds
                        > expected_gap_seconds * {ACCOUNT_STALE_MULTIPLIER} THEN 'stale'
                    WHEN quiet_seconds
                        > expected_gap_seconds * {ACCOUNT_LATE_MULTIPLIER} THEN 'late'
                    ELSE 'ok'
                END AS status
            FROM measured
            """,
        )

    def ensure_receipt_tables(self) -> None:
        self._ensure_table_group(["receipt_transaction_receipts"])
        self._ensure_receipt_mart_views()

    def _ensure_receipt_mart_views(self) -> None:
        # Every ledger transaction stays visible. The joined row contains the
        # durable search decision and, only for a verified high-confidence
        # match, receipt facts produced by that same agent operation.
        self._ensure_view(
            "marts_transaction_receipts",
            """
            CREATE OR REPLACE VIEW @marts_transaction_receipts AS
            SELECT
                t.transaction_id,
                t.account_id,
                t.posted_at,
                t.amount,
                t.currency AS transaction_currency,
                t.merchant,
                t.description,
                NULLIF(r.record_id, '') AS record_id,
                NULLIF(r.summary, '') AS receipt_summary,
                CASE WHEN r.decision = 'receipt_found' THEN r.line_items_json END AS line_items_json,
                NULLIF(r.merchant_name, '') AS receipt_merchant,
                NULLIF(r.merchant_location, '') AS merchant_location,
                NULLIF(r.purchased_at, '1970-01-01'::date) AS purchased_at,
                NULLIF(r.currency, '') AS receipt_currency,
                NULLIF(r.total, 0) AS receipt_total,
                NULLIF(r.tax, 0) AS receipt_tax,
                NULLIF(r.tip, 0) AS receipt_tip,
                NULLIF(r.order_id, '') AS order_id,
                NULLIF(r.card_last4, '') AS card_last4,
                NULLIF(r.primary_source, '') AS receipt_source,
                NULLIF(r.primary_native_id, '') AS receipt_native_id,
                NULLIF(r.match_confidence, '') AS link_confidence,
                NULLIF(r.match_reason, '') AS link_reason,
                r.decision AS receipt_decision,
                r.reasoning AS receipt_reasoning,
                r.sources_searched_json,
                r.evidence_json,
                r.attempt_count,
                r.last_attempt_at,
                r.settled,
                r.ai_model,
                r.ai_processed_at
            FROM @finance_transactions AS t
            LEFT JOIN @receipt_transaction_receipts AS r
                ON r.transaction_id = t.transaction_id
            """,
        )

    def ensure_manual_finance_tables(self) -> None:
        self._ensure_table_group(
            [
                "manual_finance_documents",
                "manual_finance_extractions",
                # The extraction candidate/retry queries must work on a fresh
                # schema (photos/voice-memos precedent).
                "agent_runs",
                "agent_run_events",
                "agent_run_tool_calls",
            ]
        )
        # ``CREATE TABLE IF NOT EXISTS`` does not evolve an existing production
        # table when the extraction contract gains a field. Keep this explicit
        # migration beside the owning ensure path so both the extraction asset
        # and the finance ledger can safely deploy v2 before any v2 row exists.
        self._command(
            "ALTER TABLE @manual_finance_extractions "
            "ADD COLUMN IF NOT EXISTS positions_json jsonb NOT NULL DEFAULT '[]'::jsonb"
        )
        # v3: whose money the document reports, on what basis, and the
        # committed/called/unfunded triple a private fund prints. Empty on
        # every pre-v3 row, which the ledger reads as "unknown" -- it never
        # reads absence as "this is the owner's position".
        for column, ddl in (
            ("reporting_scope", "text NOT NULL DEFAULT ''"),
            ("account_holder", "text NOT NULL DEFAULT ''"),
            ("value_basis", "text NOT NULL DEFAULT ''"),
            ("commitments_json", "jsonb NOT NULL DEFAULT '[]'::jsonb"),
        ):
            self._command(
                f"ALTER TABLE @manual_finance_extractions ADD COLUMN IF NOT EXISTS {column} {ddl}"
            )

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
        # marts_voice_memos.* unions both voice sources, so either source's
        # ensure_* path builds it.
        self._ensure_voice_memos_mart_views()
        self._ensure_transcription_runs_rejections_reclassified()
        self._ensure_clean_calendar_transcript_views_if_possible()
        self._ensure_search_views_if_possible()

    def ensure_alice_voice_recordings_tables(self) -> None:
        self._ensure_table_group(
            [
                "alice_voice_recordings",
                "alice_voice_recording_artifacts",
                "alice_voice_recordings_sync_state",
            ]
        )
        self._ensure_voice_memos_mart_views()
        self._ensure_search_views_if_possible()

    def ensure_voice_memos_tables(self) -> None:
        self.ensure_apple_voice_memos_tables()

    def ensure_apple_notes_tables(self) -> None:
        self._ensure_table_group(["apple_notes", "apple_note_revisions", "apple_note_attachments"])
        self._ensure_files_mart_views()
        # Audio attachments are a voice source: marts_voice_memos.recordings
        # unions them, so this source's ensure_* path builds that mart too.
        self._ensure_voice_memos_mart_views()
        self._ensure_search_views_if_possible()

    def ensure_apple_messages_tables(self) -> None:
        self.ensure_contacts_tables()
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
        self._ensure_clean_apple_messages_view()
        self._ensure_files_mart_views()
        self._ensure_search_views_if_possible()

    def ensure_whatsapp_tables(self) -> None:
        self._ensure_table_group(_WHATSAPP_TABLES)
        self.ensure_whatsapp_client_session_table()
        self._ensure_clean_whatsapp_messages_view()
        self._ensure_files_mart_views()
        self._ensure_search_views_if_possible()

    def ensure_photos_tables(self) -> None:
        self._ensure_table_group(_PHOTO_TABLES)
        self._ensure_photo_marts_views()
        self._ensure_search_views_if_possible()

    def ensure_whoop_tables(self) -> None:
        self._ensure_table_group(
            [
                "whoop_profiles",
                "whoop_body_measurements",
                "whoop_cycles",
                "whoop_recoveries",
                "whoop_sleeps",
                "whoop_workouts",
                "whoop_sync_state",
                "whoop_oauth_tokens",
            ]
        )
        self._command(
            "ALTER TABLE @whoop_sync_state "
            "ADD COLUMN IF NOT EXISTS credential_sha256 text NOT NULL DEFAULT ''"
        )
        # marts_health.* conforms both WHOOP sources, so either source's
        # ensure_* path can be the one that builds it.
        self._ensure_health_marts_views()

    def ensure_whoop_private_tables(self) -> None:
        """Provision the WHOOP private (app) API source.

        Separate from ensure_whoop_tables: the two sources share a provider but
        not a credential, a cadence, or a failure mode, and the private source
        can be paused without touching the public one.
        """
        self._ensure_table_group(list(_WHOOP_PRIVATE_TABLES))
        # The session table is written by the app too
        # (app/internal/whoopsession/store.go), whose CREATE gives these two
        # columns semantic defaults. Restate them here so it makes no difference
        # which side created the table first: a publish that omits session_key
        # must land on the same row the poller reads, and a row that has never
        # failed must read 'ok' rather than the empty string.
        self._command(
            "ALTER TABLE @whoop_private_sessions ALTER COLUMN session_key SET DEFAULT 'default'"
        )
        self._command("ALTER TABLE @whoop_private_sessions ALTER COLUMN status SET DEFAULT 'ok'")
        self._command(
            "ALTER TABLE @whoop_private_sync_state "
            "ADD COLUMN IF NOT EXISTS collection_signature text NOT NULL DEFAULT ''"
        )
        # Retired 2026-08-26: continuous heart rate is collected at the same
        # six-second grain this table held, for every hour rather than only
        # inside a workout, so it was an exact second copy of those readings.
        # "HR during workout X" is marts_health.workout_heart_rate_samples,
        # which joins the one series to the workout's own bounds. Dropped here
        # rather than left orphaned: a table absent from the catalog but present
        # in the database is invisible to every registry that would have noticed
        # it going stale.
        retired_schema = self._object_schema("whoop_private_heart_rate_samples")
        self._raw_command(
            f"DROP TABLE IF EXISTS {_identifier(retired_schema)}.workout_heart_rate_samples"
        )
        self._ensure_health_marts_views()

    def _ensure_health_marts_views(self) -> None:
        """marts_health.*: one health read interface over BOTH WHOOP sources.

        `base_whoop` is the public developer API and `base_whoop_private` is the
        app API; they describe the SAME events at different resolutions, and
        reading either alone is wrong in a different direction. The public one
        has no time series and no strain components; the private one is missing
        rows the public one has (measured 2026-08-26: 305 public sleeps vs 294
        private, 268 public workouts vs 257). So the union is LEFT-joined from
        the public row outward: the public source is the spine, the private one
        adds resolution where it exists.

        Two conforming rules are load-bearing and neither is cosmetic:

        * **Units.** The private API records HRV in SECONDS where the public API
          uses milliseconds, and its sleep-stage durations in seconds where the
          public API uses milliseconds. A view that exposed both under one name
          would be a silent 1000x error, so every conformed column states its
          unit in its name and one source is converted into the other's.
        * **The epoch sentinel.** Warehouse timestamps are NOT NULL and store
          absence as 1970-01-01, so `end_at` on a running cycle sorts OLDEST.
          Every exposed timestamp is translated with NULLIF, or none would be:
          translating some and not others does not inherit an inconsistency
          from internally-consistent sources, it MANUFACTURES one, and then
          ORDER BY, MIN() and IS NULL disagree depending on which column was
          asked.
        """
        self._ensure_table_group(
            [
                "whoop_cycles",
                "whoop_recoveries",
                "whoop_sleeps",
                "whoop_workouts",
                "whoop_private_cycles",
                "whoop_private_recoveries",
                "whoop_private_sleeps",
                "whoop_private_workouts",
                "whoop_private_sports",
                # marts_health.workout_heart_rate_samples reads it, and either
                # source's ensure_* path may be the one that builds these views.
                "whoop_private_heart_rate_samples",
            ]
        )
        epoch = "TIMESTAMPTZ '1970-01-01 00:00:00+00'"
        self._ensure_view(
            "marts_health_cycles",
            f"""
            CREATE OR REPLACE VIEW @marts_health_cycles AS
            SELECT
                c.account,
                c.cycle_id,
                NULLIF(c.start_at, {epoch}) AS start_at,
                -- The in-progress cycle stores the epoch here, not NULL, which
                -- is why ORDER BY end_at DESC on the raw table ranks the
                -- RUNNING cycle as the oldest row in it.
                NULLIF(c.end_at, {epoch}) AS end_at,
                p.day_start,
                p.day_end,
                c.timezone_offset,
                c.score_state,
                -- The displayed 0-21 score. In the private table that is
                -- `scaled_strain`; `day_strain` is the raw unscaled value
                -- (max ~0.024) and reads as zero.
                c.strain,
                p.scaled_strain AS private_strain,
                c.kilojoule,
                c.average_heart_rate,
                c.max_heart_rate,
                p.sleep_need AS sleep_need_seconds,
                NULLIF(p.predicted_end, {epoch}) AS predicted_end,
                p.data_state,
                (p.cycle_id IS NOT NULL)::int::bigint AS has_private_detail,
                NULLIF(c.created_at, {epoch}) AS created_at,
                NULLIF(c.updated_at, {epoch}) AS updated_at,
                NULLIF(c.synced_at, {epoch}) AS synced_at
            FROM @whoop_cycles c
            LEFT JOIN @whoop_private_cycles p
              ON p.account = c.account AND p.cycle_id = c.cycle_id
            """,
        )
        self._ensure_view(
            "marts_health_sleeps",
            f"""
            CREATE OR REPLACE VIEW @marts_health_sleeps AS
            SELECT
                s.account,
                s.sleep_id,
                s.cycle_id,
                NULLIF(s.start_at, {epoch}) AS start_at,
                NULLIF(s.end_at, {epoch}) AS end_at,
                s.timezone_offset,
                -- bigint 0/1 throughout the warehouse, never boolean.
                s.nap AS is_nap,
                s.score_state,
                s.sleep_performance_percentage,
                s.sleep_consistency_percentage,
                s.sleep_efficiency_percentage,
                s.respiratory_rate,
                -- Public stage totals are MILLISECONDS; the private source
                -- records the same measurements in seconds. Everything here is
                -- seconds so the two can never be added together by accident.
                s.total_in_bed_time_milli / 1000.0 AS time_in_bed_seconds,
                s.total_awake_time_milli / 1000.0 AS awake_seconds,
                s.total_no_data_time_milli / 1000.0 AS no_data_seconds,
                s.total_light_sleep_time_milli / 1000.0 AS light_sleep_seconds,
                s.total_slow_wave_sleep_time_milli / 1000.0 AS slow_wave_sleep_seconds,
                s.total_rem_sleep_time_milli / 1000.0 AS rem_sleep_seconds,
                s.sleep_cycle_count,
                s.disturbance_count,
                p.debt_pre AS sleep_debt_pre_seconds,
                p.debt_post AS sleep_debt_post_seconds,
                p.habitual_sleep_need AS habitual_sleep_need_seconds,
                p.need_from_strain AS need_from_strain_seconds,
                p.latency AS sleep_latency_seconds,
                NULLIF(p.optimal_sleep_start, {epoch}) AS optimal_sleep_start,
                NULLIF(p.optimal_sleep_end, {epoch}) AS optimal_sleep_end,
                (p.activity_id IS NOT NULL)::int::bigint AS has_private_detail,
                NULLIF(s.created_at, {epoch}) AS created_at,
                NULLIF(s.updated_at, {epoch}) AS updated_at,
                NULLIF(s.synced_at, {epoch}) AS synced_at
            FROM @whoop_sleeps s
            LEFT JOIN @whoop_private_sleeps p
              ON p.account = s.account AND p.activity_id = s.sleep_id
            """,
        )
        self._ensure_view(
            "marts_health_recoveries",
            f"""
            CREATE OR REPLACE VIEW @marts_health_recoveries AS
            SELECT
                r.account,
                r.cycle_id,
                r.sleep_id,
                r.score_state,
                r.user_calibrating AS is_calibrating,
                r.recovery_score,
                r.resting_heart_rate,
                -- ONE HRV column, in milliseconds. base_whoop_private stores
                -- hrv_rmssd_seconds in SECONDS beside a milliseconds copy;
                -- exposing both units under similar names is how a 1000x error
                -- gets written.
                r.hrv_rmssd_milli,
                r.spo2_percentage,
                r.skin_temp_celsius,
                p.hrv_component,
                p.rhr_component,
                p.recovery_rate,
                p.hr_baseline,
                p.prob_covid,
                (p.activity_id IS NOT NULL)::int::bigint AS has_private_detail,
                NULLIF(r.created_at, {epoch}) AS created_at,
                NULLIF(r.updated_at, {epoch}) AS updated_at,
                NULLIF(r.synced_at, {epoch}) AS synced_at
            FROM @whoop_recoveries r
            LEFT JOIN @whoop_private_recoveries p
              ON p.account = r.account AND p.activity_id = r.sleep_id
            """,
        )
        self._ensure_view(
            "marts_health_workouts",
            f"""
            CREATE OR REPLACE VIEW @marts_health_workouts AS
            SELECT
                w.account,
                w.workout_id,
                NULLIF(w.start_at, {epoch}) AS start_at,
                NULLIF(w.end_at, {epoch}) AS end_at,
                w.timezone_offset,
                w.sport_id,
                -- The public row's sport_name is a slug ('hiking-rucking');
                -- the private source ships the 204-sport catalog that gives it
                -- a display name ('Hiking'), and is sometimes the only source
                -- of one at all. Both are exposed: the readable name under
                -- sport_name, the provider's own token under sport_slug.
                COALESCE(NULLIF(sp.name, ''), NULLIF(w.sport_name, '')) AS sport_name,
                NULLIF(w.sport_name, '') AS sport_slug,
                sp.category AS sport_category,
                w.score_state,
                w.strain,
                w.average_heart_rate,
                w.max_heart_rate,
                w.kilojoule,
                w.percent_recorded,
                w.distance_meter,
                w.altitude_gain_meter,
                w.altitude_change_meter,
                w.zone_durations_json,
                p.total_steps,
                p.msk_score,
                p.zone_durations_v2_json,
                (p.gps_data_json IS NOT NULL AND p.gps_data_json <> '{{}}'::jsonb)::int::bigint AS has_gps,
                (p.activity_id IS NOT NULL)::int::bigint AS has_private_detail,
                NULLIF(w.created_at, {epoch}) AS created_at,
                NULLIF(w.updated_at, {epoch}) AS updated_at,
                NULLIF(w.synced_at, {epoch}) AS synced_at
            FROM @whoop_workouts w
            LEFT JOIN @whoop_private_workouts p
              ON p.account = w.account AND p.activity_id = w.workout_id
            LEFT JOIN @whoop_private_sports sp
              ON sp.account = w.account AND sp.sport_id = w.sport_id
            """,
        )
        self._ensure_view(
            "marts_health_workout_heart_rate_samples",
            f"""
            CREATE OR REPLACE VIEW @marts_health_workout_heart_rate_samples AS
            SELECT
                w.account,
                w.workout_id,
                w.sport_name,
                w.start_at AS workout_start_at,
                w.end_at AS workout_end_at,
                NULLIF(s.sample_at, {epoch}) AS sample_at,
                EXTRACT(EPOCH FROM (s.sample_at - w.start_at))::bigint AS elapsed_seconds,
                s.heart_rate,
                s.step_seconds
            FROM @marts_health_workouts w
            JOIN @whoop_private_heart_rate_samples s
              ON s.account = w.account
             AND s.sample_at >= w.start_at
             AND s.sample_at < w.end_at
            -- A workout still running exposes end_at as NULL here (the raw row
            -- holds the epoch sentinel), and an open-ended range is not a
            -- window. It reappears the moment the workout is scored.
            WHERE w.start_at IS NOT NULL AND w.end_at IS NOT NULL
            """,
        )

    def ensure_agent_sessions_tables(self) -> None:
        self._ensure_table_group(_AI_CONVERSATION_EVENT_TABLES)
        self.ensure_chatgpt_tables()
        self.ensure_claude_desktop_tables()
        self._ensure_ai_conversation_events_view()
        self._ensure_clean_agent_sessions_view()
        self._ensure_search_views_if_possible()

    def ensure_pipeline_health_tables(self) -> None:
        """Snapshot tables plus the marts_ops read interface over them.

        See personal_data_warehouse/pipeline_health.py: the tables hold measured
        facts, the views turn them into a live status.
        """
        self._ensure_table_group(list(PIPELINE_HEALTH_SNAPSHOT_TABLES))
        # _ensure_table_group only CREATEs; it does not widen an existing table,
        # so a warehouse provisioned before any of these columns existed would
        # keep the old shape and everything that names the column -- the
        # collectors' inserts and the marts views below -- would fail on every
        # run. Fresh-database tests cannot catch that by construction, which is
        # exactly how it reached production three times (ops.pipeline_health
        # 2026-08-23, ops.pgbackrest_health 2026-08-27, ops.agent_usage
        # 2026-08-28). Derived from the TableSpec rather than hand-listed, so
        # the next column added here needs no migration line and cannot be
        # forgotten.
        for table in PIPELINE_HEALTH_SNAPSHOT_TABLES:
            added = self._reconcile_table_columns(table)
            if added:
                logger.info("widened %s with %s", table, ", ".join(added))
        self._ensure_pipeline_health_mart_views()

    def _ensure_pipeline_health_mart_views(self) -> None:
        """Publish pipeline freshness with a status computed at read time.

        Status has to be derived here rather than stored by the collector: a row
        that said 'ok' when it was written still says 'ok' three days after the
        collector stopped running, which is precisely the failure this dashboard
        exists to catch. So the snapshot stores facts (measured timestamps and
        the declared SLA) and these views compare them against ``now()``,
        including how old the snapshot itself is.
        """
        epoch = "'1970-01-01 00:00:00+00'::timestamptz"
        # A pipeline's own SLA drives the thresholds, so a 5-minute poller and a
        # monthly upload are judged on their own terms.
        def freshness_status(at: str, expected: str, *, unmonitored: str) -> str:
            return f"""
            CASE
                WHEN {expected} = 0 THEN
                    CASE WHEN {at} IS NULL THEN '{unmonitored}' ELSE 'manual' END
                WHEN {at} IS NULL THEN 'no_data'
                WHEN now() - {at} > make_interval(
                    secs => {expected} * {STALE_MULTIPLIER}) THEN 'stale'
                WHEN now() - {at} > make_interval(
                    secs => {expected} * {LATE_MULTIPLIER}) THEN 'late'
                ELSE 'ok'
            END
            """

        self._ensure_view(
            "marts_pipeline_health",
            f"""
            CREATE OR REPLACE VIEW @marts_pipeline_health AS
            WITH measured AS (
                SELECT
                    pipeline, label, kind, cadence, transport, note,
                    NULLIF(data_basis, '') AS data_basis,
                    expected_data_interval_seconds,
                    expected_run_interval_seconds,
                    expected_event_interval_seconds,
                    NULLIF(last_write_at, {epoch}) AS last_write_at,
                    NULLIF(newest_event_at, {epoch}) AS newest_event_at,
                    NULLIF(last_run_at, {epoch}) AS last_run_at,
                    event_tables_probed,
                    row_estimate, byte_size,
                    table_count, tables_probed, tables_skipped,
                    state_table, state_rows, state_error_rows, state_attention_rows,
                    NULLIF(last_error, '') AS last_error,
                    NULLIF(last_error_at, {epoch}) AS last_error_at,
                    NULLIF(collected_at, {epoch}) AS collected_at
                FROM @pipeline_health
            ),
            classified AS (
                SELECT
                    measured.*,
                    {freshness_status(
                        "last_write_at", "expected_data_interval_seconds", unmonitored="no_data"
                    )} AS data_status,
                    {freshness_status(
                        "last_run_at", "expected_run_interval_seconds", unmonitored="unmonitored"
                    )} AS run_status,
                    -- Event freshness: how old the newest REAL-WORLD event is,
                    -- as opposed to how recently a row was written. Collected,
                    -- stored, shipped over the API and rendered on the page
                    -- since this dashboard shipped, and until 2026-08-23 never
                    -- judged -- which is why alice_voice_recordings showed a
                    -- green dot beside an event 118 days old.
                    --
                    -- 'unmeasured' is a distinct answer from 'no_data' on
                    -- purpose. google_drive and attachment_enrichment DO
                    -- declare an event column; it sits on a 376 MiB / 561 MiB
                    -- heap with no index leading with it, so the collector
                    -- skips the max() by design. Reporting that as "nothing has
                    -- ever arrived" would be a louder and different claim than
                    -- the truth, which is "we did not look".
                    CASE
                        WHEN expected_event_interval_seconds = 0 THEN 'unmonitored'
                        WHEN event_tables_probed = 0 THEN 'unmeasured'
                        WHEN newest_event_at IS NULL THEN 'no_data'
                        WHEN now() - newest_event_at > make_interval(
                            secs => expected_event_interval_seconds
                                    * {STALE_MULTIPLIER}) THEN 'stale'
                        WHEN now() - newest_event_at > make_interval(
                            secs => expected_event_interval_seconds
                                    * {LATE_MULTIPLIER}) THEN 'late'
                        ELSE 'ok'
                    END AS event_status
                FROM measured
            )
            SELECT
                pipeline,
                label,
                kind,
                cadence,
                transport,
                CASE
                    -- A snapshot this old is evidence about the collector, not
                    -- about the pipelines it describes.
                    WHEN collected_at IS NULL
                      OR now() - collected_at > make_interval(
                            secs => {COLLECTOR_STALE_SECONDS}) THEN 'unknown'
                    WHEN state_error_rows > 0 THEN 'failing'
                    WHEN state_attention_rows > 0 THEN 'attention'
                    -- Event lateness escalates exactly like write lateness.
                    -- Only 'late'/'stale' escalate: 'unmonitored', 'unmeasured'
                    -- and 'no_data' are statements about the measurement, and
                    -- a measurement gap must never colour a pipeline red.
                    WHEN 'stale' IN (data_status, run_status, event_status) THEN 'stale'
                    WHEN 'late' IN (data_status, run_status, event_status) THEN 'late'
                    WHEN data_status = 'no_data'
                     AND run_status IN ('no_data', 'unmonitored') THEN 'no_data'
                    WHEN data_status = 'manual' THEN 'manual'
                    ELSE 'ok'
                END AS status,
                data_status,
                run_status,
                event_status,
                last_write_at,
                newest_event_at,
                last_run_at,
                (EXTRACT(EPOCH FROM now() - last_write_at))::bigint AS data_age_seconds,
                (EXTRACT(EPOCH FROM now() - last_run_at))::bigint AS run_age_seconds,
                (EXTRACT(EPOCH FROM now() - newest_event_at))::bigint AS event_age_seconds,
                expected_data_interval_seconds,
                expected_run_interval_seconds,
                expected_event_interval_seconds,
                event_tables_probed,
                data_basis,
                row_estimate,
                byte_size,
                table_count,
                tables_probed,
                tables_skipped,
                state_table,
                state_rows,
                state_error_rows,
                state_attention_rows,
                last_error,
                last_error_at,
                collected_at,
                (EXTRACT(EPOCH FROM now() - collected_at))::bigint AS snapshot_age_seconds,
                note
            FROM classified
            """,
        )
        self._ensure_view(
            "marts_pipeline_table_freshness",
            f"""
            CREATE OR REPLACE VIEW @marts_pipeline_table_freshness AS
            SELECT
                table_id,
                pipeline,
                role,
                layer,
                table_schema,
                table_name,
                written_at_column,
                event_at_column,
                NULLIF(last_write_at, {epoch}) AS last_write_at,
                NULLIF(newest_event_at, {epoch}) AS newest_event_at,
                (EXTRACT(EPOCH FROM now() - NULLIF(last_write_at, {epoch})))::bigint
                    AS data_age_seconds,
                row_estimate,
                byte_size,
                probe_status,
                NULLIF(probe_detail, '') AS probe_detail,
                probe_ms,
                NULLIF(collected_at, {epoch}) AS collected_at,
                note
            FROM @pipeline_table_freshness
            """,
        )

        # Level 2 of the health contract: the marts_* read interface itself.
        # Thirty-three views -- the relations every agent is told to start from
        # -- had zero coverage until 2026-08-23: `SELECT layer, count(*) FROM
        # marts_ops.table_freshness GROUP BY 1` returned base/derived/ops/
        # private/timeline and no marts row at all.
        #
        # It is worth being explicit about why they were not simply added to
        # TABLE_PIPELINES: a VIEW HAS NO STAMPED COLUMN TO TAKE A max() OF and
        # no relpages for the cheapness guard to consult, so the existing
        # table-probe mechanism genuinely cannot be pointed at one. What IS
        # cheap and true about a view is measured instead -- input freshness,
        # a bounded non-empty probe, and definition drift -- and the status
        # below is derived from those at read time.
        self._ensure_view(
            "marts_mart_view_health",
            f"""
            CREATE OR REPLACE VIEW @marts_mart_view_health AS
            WITH measured AS (
                SELECT
                    view_id, domain, view_schema, view_name,
                    input_tables, input_pipelines, input_count,
                    NULLIF(stalest_pipeline, '') AS stalest_pipeline,
                    NULLIF(stalest_pipeline_at, {epoch}) AS stalest_pipeline_at,
                    stalest_pipeline_expected_seconds,
                    inputs_unmeasured, has_rows,
                    definition_sha256,
                    NULLIF(first_seen_at, {epoch}) AS first_seen_at,
                    probe_status,
                    NULLIF(probe_detail, '') AS probe_detail,
                    probe_ms,
                    NULLIF(note, '') AS note,
                    NULLIF(collected_at, {epoch}) AS collected_at
                FROM @mart_view_health
            ),
            classified AS (
                SELECT
                    measured.*,
                    -- A view is only ever as fresh as the stalest PIPELINE
                    -- feeding it, and each is judged against ITS OWN expected
                    -- interval: marts_ai_conversations.events unions six agent
                    -- sources whose expectations differ by an order of
                    -- magnitude, so a single global threshold would permanently
                    -- nominate whichever one is legitimately the quietest.
                    -- Per pipeline rather than per table -- see
                    -- _roll_up_inputs: a pipeline's own freshness is a max()
                    -- over its data tables, so judging one quiet table against
                    -- the whole pipeline's interval manufactures staleness.
                    CASE
                        WHEN stalest_pipeline_at IS NULL
                          OR stalest_pipeline_expected_seconds = 0 THEN 'unmeasured'
                        WHEN now() - stalest_pipeline_at > make_interval(
                            secs => stalest_pipeline_expected_seconds
                                    * {STALE_MULTIPLIER}) THEN 'stale'
                        WHEN now() - stalest_pipeline_at > make_interval(
                            secs => stalest_pipeline_expected_seconds
                                    * {LATE_MULTIPLIER}) THEN 'late'
                        ELSE 'ok'
                    END AS input_status
                FROM measured
            )
            SELECT
                view_id,
                domain,
                view_schema,
                view_name,
                CASE
                    -- Same self-distrust as marts_ops.pipeline_health: a
                    -- snapshot this old is evidence about the collector, not
                    -- about the views it describes.
                    WHEN collected_at IS NULL
                      OR now() - collected_at > make_interval(
                            secs => {COLLECTOR_STALE_SECONDS}) THEN 'unknown'
                    WHEN probe_status IN ('error', 'missing') THEN 'failing'
                    WHEN probe_status = 'timeout' THEN 'attention'
                    WHEN input_status = 'stale' THEN 'stale'
                    WHEN input_status = 'late' THEN 'late'
                    WHEN probe_status = 'empty' THEN 'no_data'
                    ELSE 'ok'
                END AS status,
                input_status,
                probe_status,
                probe_detail,
                probe_ms,
                has_rows,
                input_tables,
                input_pipelines,
                input_count,
                inputs_unmeasured,
                stalest_pipeline,
                stalest_pipeline_at,
                (EXTRACT(EPOCH FROM now() - stalest_pipeline_at))::bigint
                    AS stalest_pipeline_age_seconds,
                stalest_pipeline_expected_seconds,
                definition_sha256,
                first_seen_at,
                -- How long the current definition has stood. A redefinition
                -- that silently drops a source table changes nothing
                -- measurable about the rows, so the definition is what is
                -- watched; a recent first_seen_at next to a surprising
                -- input_tables list is the shape to look for.
                (EXTRACT(EPOCH FROM now() - first_seen_at))::bigint
                    AS definition_age_seconds,
                collected_at,
                (EXTRACT(EPOCH FROM now() - collected_at))::bigint AS snapshot_age_seconds,
                note
            FROM classified
            """,
        )

        # Backups. The one health question with no collector, because the
        # Dagster collector cannot shell out to pgbackrest from another
        # container -- so the backup loop writes this row itself. Two states are
        # reported separately on purpose: whether a RESTORABLE BACKUP EXISTS and
        # whether WAL is still shipping. On 2026-08-25 the second was perfect
        # and the first was `error (no valid backups)` for a day, and you
        # cannot restore from WAL alone.
        self._ensure_view(
            "marts_pgbackrest_health",
            f"""
            CREATE OR REPLACE VIEW @marts_pgbackrest_health AS
            WITH measured AS (
                SELECT
                    stanza,
                    NULLIF(repo_status, '') AS repo_status,
                    NULLIF(repo_message, '') AS repo_message,
                    NULLIF(last_full_at, '1970-01-01 00:00:00+00'::timestamptz) AS last_full_at,
                    NULLIF(last_diff_at, '1970-01-01 00:00:00+00'::timestamptz) AS last_diff_at,
                    NULLIF(last_incr_at, '1970-01-01 00:00:00+00'::timestamptz) AS last_incr_at,
                    NULLIF(last_backup_label, '') AS last_backup_label,
                    NULLIF(last_backup_type, '') AS last_backup_type,
                    backup_count,
                    repo_bytes,
                    NULLIF(wal_min, '') AS wal_min,
                    NULLIF(wal_max, '') AS wal_max,
                    wal_ready_count,
                    archived_count,
                    failed_count,
                    NULLIF(last_archived_at, '1970-01-01 00:00:00+00'::timestamptz) AS last_archived_at,
                    NULLIF(last_attempt_at, '1970-01-01 00:00:00+00'::timestamptz) AS last_attempt_at,
                    NULLIF(last_attempt_type, '') AS last_attempt_type,
                    last_attempt_ok,
                    NULLIF(last_error, '') AS last_error,
                    NULLIF(collected_at, '1970-01-01 00:00:00+00'::timestamptz) AS collected_at,
                    NULLIF(last_restore_verified_at, '1970-01-01 00:00:00+00'::timestamptz)
                        AS last_restore_verified_at,
                    NULLIF(last_restore_label, '') AS last_restore_label,
                    last_restore_rows,
                    NULLIF(last_restore_note, '') AS last_restore_note
                FROM @pgbackrest_health
            )
            SELECT
                stanza,
                CASE
                    -- Snapshot too old to speak for the present. Store facts,
                    -- derive status -- the same rule the rest of marts_ops uses.
                    WHEN collected_at IS NULL
                      OR collected_at < now() - interval '{PGBACKREST_SNAPSHOT_STALE_SECONDS} seconds'
                        THEN 'unknown'
                    -- No restorable backup at all. This is the state that was
                    -- invisible, and it outranks every other signal.
                    WHEN backup_count = 0 OR last_full_at IS NULL THEN 'failing'
                    WHEN repo_status IS DISTINCT FROM 'ok' THEN 'failing'
                    -- Archiving is losing to WAL generation. pg_wal grows
                    -- without bound and no new backup can complete, because
                    -- `backup stop` waits on the WAL beneath it. This is the
                    -- signal that was missing on 2026-08-26, when the queue
                    -- reached 5,910 segments while archived_count kept rising
                    -- and every other field read healthy.
                    WHEN wal_ready_count >= {WAL_READY_FAILING} THEN 'failing'
                    WHEN wal_ready_count >= {WAL_READY_ATTENTION} THEN 'attention'
                    -- WAL archiving broken: the backup is a floor, not a
                    -- recovery point, until shipping resumes. Judged against
                    -- collected_at, NOT now(): last_archived_at is a fact
                    -- captured when the loop reported, and the loop reports
                    -- every six hours, so measuring it against now() means the
                    -- row necessarily ages past any threshold shorter than the
                    -- reporting interval and the view sits at 'attention'
                    -- forever. Observed 2026-08-27 with archiving perfectly
                    -- healthy -- WAL had shipped one second before collection.
                    -- Whether the snapshot is too old to believe at all is a
                    -- separate question, already answered above.
                    WHEN last_archived_at IS NULL
                      OR last_archived_at < collected_at - interval '1 hour' THEN 'attention'
                    WHEN last_full_at < now() - interval '14 days' THEN 'stale'
                    WHEN last_full_at < now() - interval '8 days' THEN 'late'
                    -- The loop is failing while an older good backup still
                    -- stands: not an outage yet, but the clock is running.
                    WHEN last_attempt_ok = 0 THEN 'attention'
                    -- A backup nobody has restored is a hypothesis. The drill
                    -- is recorded here by hand (pgbackrest_restore_drill), so
                    -- an old or missing record is the row saying "unverified",
                    -- not a fact about the repository -- attention, never
                    -- failing, and only after every fact about the backups
                    -- themselves has been judged.
                    WHEN last_restore_verified_at IS NULL
                      OR last_restore_verified_at
                         < now() - interval '{PGBACKREST_RESTORE_DRILL_STALE_SECONDS} seconds'
                        THEN 'attention'
                    ELSE 'ok'
                END AS status,
                CASE
                    WHEN last_restore_verified_at IS NULL THEN 'never'
                    WHEN last_restore_verified_at
                         < now() - interval '{PGBACKREST_RESTORE_DRILL_STALE_SECONDS} seconds'
                        THEN 'stale'
                    ELSE 'ok'
                END AS restore_status,
                last_restore_verified_at,
                last_restore_label,
                last_restore_rows,
                last_restore_note,
                EXTRACT(EPOCH FROM now() - last_restore_verified_at)::bigint AS restore_age_seconds,
                repo_status,
                repo_message,
                last_full_at,
                last_diff_at,
                last_incr_at,
                last_backup_label,
                last_backup_type,
                backup_count,
                repo_bytes,
                wal_min,
                wal_max,
                wal_ready_count,
                archived_count,
                failed_count,
                last_archived_at,
                last_attempt_at,
                last_attempt_type,
                last_attempt_ok,
                last_error,
                collected_at,
                EXTRACT(EPOCH FROM now() - last_full_at)::bigint AS full_age_seconds,
                EXTRACT(EPOCH FROM now() - collected_at)::bigint AS snapshot_age_seconds
            FROM measured
            """,
        )

        # Collation drift: the check Postgres cannot do for this database.
        # datcollversion is NULL while the live library reports a version, and
        # REFRESH COLLATION VERSION refuses to create a baseline from NULL, so
        # there will never be a mismatch warning here. Note the finding is
        # written as an explicit NULL test in collation_health.py rather than
        # `recorded <> actual`, which evaluates to NULL and reports CLEAN.
        self._ensure_view(
            "marts_collation_health",
            f"""
            CREATE OR REPLACE VIEW @marts_collation_health AS
            WITH measured AS (
                SELECT
                    object_id, scope, object_name,
                    NULLIF(provider, '') AS provider,
                    NULLIF(recorded_version, '') AS recorded_version,
                    NULLIF(actual_version, '') AS actual_version,
                    dependent_indexes, finding,
                    NULLIF(detail, '') AS detail,
                    NULLIF(table_name, '') AS table_name,
                    is_unique, is_partial,
                    NULLIF(predicate, '') AS predicate,
                    key_columns,
                    heap_rows, distinct_keys, excess_rows, probe_ms,
                    amcheck_status, NULLIF(amcheck_detail, '') AS amcheck_detail,
                    amcheck_ms, NULLIF(amcheck_at, {epoch}) AS amcheck_at,
                    NULLIF(collected_at, {epoch}) AS collected_at
                FROM @collation_health
            )
            SELECT
                object_id,
                scope,
                object_name,
                CASE
                    -- Judged against the COLLATION collector's own cadence, not
                    -- the ten-minutely freshness collector's. That asset runs
                    -- daily at 03:41 because it costs a bounded sequential scan
                    -- of every unique index's heap; measuring it against
                    -- COLLECTOR_STALE_SECONDS (1 hour) made every one of its
                    -- 252 rows read `unknown` for ~96% of each day -- the level
                    -- was structurally dark rather than clean, and a real
                    -- finding would have been indistinguishable from the
                    -- permanent state.
                    WHEN collected_at IS NULL
                      OR now() - collected_at > make_interval(
                            secs => {COLLATION_SNAPSHOT_STALE_SECONDS}) THEN 'unknown'
                    -- Duplicate rows under a UNIQUE index are the loudest
                    -- evidence: a working ON CONFLICT cannot produce them.
                    WHEN scope = 'index' AND amcheck_status = 'failed' THEN 'failing'
                    WHEN finding = 'duplicate_keys' THEN 'failing'
                    -- A recorded baseline that no longer matches the library.
                    WHEN finding = 'version_changed' THEN 'failing'
                    -- No baseline at all IS the finding, not a neutral state:
                    -- it means this database can never warn about the next
                    -- drift either, and text index ordering is unverified.
                    WHEN finding = 'no_baseline' THEN 'attention'
                    WHEN finding = 'unknown_actual' THEN 'attention'
                    WHEN scope = 'index'
                      AND amcheck_status IN ('timeout', 'error') THEN 'attention'
                    -- `unavailable` describes the EXTENSION, not the index. It
                    -- is only a finding while amcheck is genuinely missing;
                    -- once the extension exists a row still carrying it is a
                    -- verdict recorded before the extension did -- a
                    -- measurement gap, read `unmeasured` like `never_checked`.
                    -- The view asks pg_extension itself rather than trusting
                    -- the snapshot, because on 2026-08-27 production held 98
                    -- such rows as `attention` beside 48 rows the same
                    -- collector had just amchecked `ok`.
                    WHEN scope = 'index' AND amcheck_status = 'unavailable'
                      AND NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'amcheck')
                      THEN 'attention'
                    WHEN scope = 'index'
                      AND amcheck_status IN ('pending', 'never_checked', 'unavailable')
                      THEN 'unmeasured'
                    WHEN scope = 'index' AND amcheck_at IS NOT NULL
                      AND now() - amcheck_at > interval '14 days' THEN 'attention'
                    WHEN finding IN ('timeout', 'error') THEN 'attention'
                    WHEN finding IN ('skipped_expression', 'skipped_large')
                      AND amcheck_status <> 'ok' THEN 'unmeasured'
                    ELSE 'ok'
                END AS status,
                finding,
                detail,
                provider,
                recorded_version,
                actual_version,
                dependent_indexes,
                table_name,
                is_unique,
                is_partial,
                predicate,
                key_columns,
                heap_rows,
                distinct_keys,
                excess_rows,
                probe_ms,
                amcheck_status,
                amcheck_detail,
                amcheck_ms,
                amcheck_at,
                collected_at,
                (EXTRACT(EPOCH FROM now() - collected_at))::bigint AS snapshot_age_seconds
            FROM measured
            """,
        )

        # Search convergence cannot be inferred from max(built_at): a chunk
        # worker or embedder can keep writing recent rows while a backlog grows.
        # `seq_lag` is the backlog in timeline rows; `oldest_pending_at` is when
        # the oldest unprocessed timeline row was written, which is the honest
        # lateness (a re-walk emits millions of rows and the chunker converges
        # at ~70k a run, so a seq threshold would mean nothing).
        self._ensure_view(
            "marts_search_health",
            f"""
            CREATE OR REPLACE VIEW @marts_search_health AS
            WITH measured AS (
                SELECT component, NULLIF(model, '') AS model,
                       configured, pgvector_available,
                       timeline_max_seq, chunk_cursor_seq, caught_up,
                       processed_rows,
                       CASE WHEN pending_count < 0 THEN NULL ELSE pending_count END AS pending_count,
                       NULLIF(oldest_pending_at, {epoch}) AS oldest_pending_at,
                       NULLIF(last_success_at, {epoch}) AS last_success_at,
                       NULLIF(last_run_at, {epoch}) AS last_run_at,
                       NULLIF(last_error, '') AS last_error,
                       NULLIF(updated_at, {epoch}) AS updated_at
                FROM @search_health
            )
            SELECT component,
                   CASE
                     WHEN updated_at IS NULL OR now() - updated_at > interval '30 minutes' THEN 'unknown'
                     WHEN configured = 0 OR pgvector_available = 0 THEN 'failing'
                     WHEN last_error IS NOT NULL THEN 'failing'
                     WHEN caught_up = 0
                      AND oldest_pending_at < now() - interval '{SEARCH_HEALTH_LATE_AFTER_MINUTES} minutes'
                       THEN 'late'
                     WHEN caught_up = 0 THEN 'backfilling'
                     ELSE 'ok'
                   END AS status,
                   model, configured, pgvector_available,
                   timeline_max_seq, chunk_cursor_seq,
                   GREATEST(0, timeline_max_seq - chunk_cursor_seq) AS seq_lag,
                   caught_up, processed_rows, pending_count, oldest_pending_at,
                   (EXTRACT(EPOCH FROM now() - oldest_pending_at))::bigint AS pending_age_seconds,
                   last_success_at, last_run_at, last_error, updated_at,
                   (EXTRACT(EPOCH FROM now() - updated_at))::bigint AS snapshot_age_seconds
            FROM measured
            """,
        )

        # C2 made visible: the tier mix per source over the last seven days.
        # `unclassified` is a fail-loud sentinel, so a row carrying it is a
        # classification outage, not a sixth tier; `share_7d` is what shows a
        # source's mix quietly collapsing into one tier after an adapter edit.
        self._ensure_view(
            "marts_timeline_priority_mix",
            f"""
            CREATE OR REPLACE VIEW @marts_timeline_priority_mix AS
            WITH measured AS (
                SELECT source, priority, events_7d, events_1d,
                       NULLIF(newest_event_at, {epoch}) AS newest_event_at,
                       NULLIF(collected_at, {epoch}) AS collected_at,
                       sum(events_7d) OVER (PARTITION BY source) AS source_events_7d
                FROM @timeline_priority_mix
            )
            SELECT
                source,
                priority,
                CASE
                    WHEN collected_at IS NULL
                      OR now() - collected_at > make_interval(secs => {COLLECTOR_STALE_SECONDS})
                        THEN 'unknown'
                    WHEN priority = 'unclassified' THEN 'failing'
                    ELSE 'ok'
                END AS status,
                events_7d,
                events_1d,
                CASE WHEN source_events_7d > 0
                     THEN round(events_7d::numeric / source_events_7d, 4) ELSE 0 END AS share_7d,
                source_events_7d,
                newest_event_at,
                collected_at,
                (EXTRACT(EPOCH FROM now() - collected_at))::bigint AS snapshot_age_seconds
            FROM measured
            """,
        )

        # Contract C3 as a measurement: are agents starting at the timeline and
        # scoping by tier? Rates come from the daily snapshot; the verdict is
        # judged here against the targets so a guidance change shows within
        # days and a regression is a row, not a re-audit.
        self._ensure_view(
            "marts_agent_usage",
            f"""
            CREATE OR REPLACE VIEW @marts_agent_usage AS
            WITH measured AS (
                SELECT *,
                       NULLIF(newest_session_at, {epoch}) AS newest_session,
                       NULLIF(collected_at, {epoch}) AS collected
                FROM @agent_usage
            ),
            rated AS (
                SELECT source, window_days, sessions, pdw_sessions,
                       first_search, first_schema, first_sql, first_invented,
                       search_calls, search_with_priority, sql_calls, sql_base_only,
                       sql_error_sessions, sql_timeouts, invented_calls, admin_calls,
                       newest_session AS newest_session_at,
                       collected AS collected_at,
                       CASE WHEN pdw_sessions > 0 THEN round(first_search::numeric / pdw_sessions, 3) END AS search_first_rate,
                       CASE WHEN search_calls > 0 THEN round(search_with_priority::numeric / search_calls, 3) END AS priority_filter_rate,
                       CASE WHEN sql_calls > 0 THEN round(sql_base_only::numeric / sql_calls, 3) END AS sql_base_only_rate,
                       CASE WHEN pdw_sessions > 0 THEN round(sql_error_sessions::numeric / pdw_sessions, 3) END AS sql_error_session_rate
                FROM measured
            )
            SELECT *,
                   CASE
                       WHEN collected_at IS NULL OR now() - collected_at > interval '2 days' THEN 'unknown'
                       WHEN pdw_sessions < 10 THEN 'no_data'
                       WHEN search_first_rate < {SEARCH_FIRST_TARGET}
                         OR priority_filter_rate < {PRIORITY_FILTER_TARGET}
                         OR sql_error_session_rate > {SQL_ERROR_SESSION_CEILING} THEN 'attention'
                       ELSE 'ok'
                   END AS status,
                   (EXTRACT(EPOCH FROM now() - collected_at))::bigint AS snapshot_age_seconds
            FROM rated
            """,
        )

        # C8 measured: the weekly benchmark's latency and labeled quality as a
        # health row, judged against the goal (p50 under two seconds) and the
        # MRR floor. A row older than ten days reads unknown -- the asset is
        # weekly and a benchmark that stopped running must not keep its last
        # green.
        self._ensure_view(
            "marts_search_benchmark",
            f"""
            CREATE OR REPLACE VIEW @marts_search_benchmark AS
            SELECT mode,
                   CASE
                       WHEN collected_at = {epoch} OR now() - collected_at > interval '10 days' THEN 'unknown'
                       WHEN probe_queries = 0 THEN 'no_data'
                       WHEN latency_p50_ms > {LATENCY_P50_TARGET_MS} THEN 'attention'
                       WHEN labeled_cases > 0 AND mrr_milli < {int(MRR_FLOOR * 1000)} THEN 'attention'
                       ELSE 'ok'
                   END AS status,
                   probe_queries, latency_p50_ms, latency_p90_ms, latency_max_ms,
                   labeled_cases, found, hit_at_1, hit_at_5, hit_at_10,
                   round(mrr_milli / 1000.0, 3) AS mrr,
                   errors, NULLIF(note, '') AS note,
                   io_pressure_full_avg10, cpu_pressure_some_avg10, load_1m, cpu_count,
                   -- C6: was the host being used while the probes ran? io_bound
                   -- and cpu_bound say the machine was busy; `idle` is the case
                   -- C6 says to fix FIRST -- slow (p50 over the target) on a
                   -- machine that was doing nothing, so the query was never
                   -- allowed to use the host. -1 in any sample is unmeasured.
                   CASE
                       WHEN io_pressure_full_avg10 < 0 OR cpu_pressure_some_avg10 < 0
                            OR load_1m < 0 OR cpu_count < 0 THEN 'unmeasured'
                       WHEN io_pressure_full_avg10 >= {SATURATION_IO_FULL_AVG10} THEN 'io_bound'
                       WHEN cpu_pressure_some_avg10 >= {SATURATION_CPU_SOME_AVG10}
                            OR load_1m >= cpu_count THEN 'cpu_bound'
                       WHEN latency_p50_ms > {LATENCY_P50_TARGET_MS} THEN 'idle'
                       ELSE 'ok'
                   END AS saturation,
                   NULLIF(collected_at, {epoch}) AS collected_at,
                   (EXTRACT(EPOCH FROM now() - NULLIF(collected_at, {epoch})))::bigint AS snapshot_age_seconds
            FROM @search_benchmark_runs
            """,
        )

        # Level 3 of the health contract: "is THIS kind of data current on the
        # timeline?" The pipeline row cannot answer it. `timeline` is a single
        # pipeline whose run heartbeat is a max() over every adapter, so one
        # frozen adapter is arithmetically invisible behind twenty-four healthy
        # ones -- measured 2026-08-23, six adapters had not run in ~60 hours
        # against a 30-minute cadence while the pipeline reported
        # `run_age = 0.00d, ok`.
        #
        # Facts, not just a verdict: `last_run_at` is NOT "when the adapter last
        # ran". `_save_state` only stamps it when a batch returned rows, so an
        # adapter with nothing to do looks identical to a wedged one. Anyone
        # alarming on `run_age_seconds` alone will page falsely. The honest
        # signal is `watermark_ingest_ts` compared against the newest row in the
        # adapter's own source relation, which is why both are exposed side by
        # side and the status stays deliberately conservative.
        # Guarded: ensure_pipeline_health_tables can run before
        # ensure_timeline_tables has created the state table it reads.
        if not self._relation_exists("timeline_sync_state"):
            return
        self._ensure_view(
            "marts_timeline_adapter_health",
            f"""
            CREATE OR REPLACE VIEW @marts_timeline_adapter_health AS
            SELECT
                adapter,
                backfill_done,
                backfill_rows,
                incremental_rows,
                NULLIF(watermark_ingest_ts, {epoch}) AS watermark_ingest_ts,
                NULLIF(last_run_at, {epoch}) AS last_run_at,
                (EXTRACT(EPOCH FROM now() - NULLIF(watermark_ingest_ts, {epoch})))::bigint
                    AS watermark_age_seconds,
                (EXTRACT(EPOCH FROM now() - NULLIF(last_run_at, {epoch})))::bigint
                    AS run_age_seconds,
                CASE
                    WHEN NULLIF(last_error, '') IS NOT NULL THEN 'failing'
                    WHEN backfill_done = 0 THEN 'backfilling'
                    ELSE 'ok'
                END AS status,
                NULLIF(last_error, '') AS last_error,
                NULLIF(updated_at, {epoch}) AS updated_at,
                adapter_signature
            FROM @timeline_sync_state
            """,
        )




    def write_pipeline_health(
        self,
        pipelines: Sequence[Any],
        tables: Sequence[Any],
        *,
        collected_at: datetime,
    ) -> None:
        """Replace the freshness snapshot with one collection's measurements.

        Rows for pipelines or tables that no longer exist in the registry are
        deleted, so a retired source disappears from the dashboard instead of
        lingering as permanently stale.
        """
        self._insert_rows(
            "pipeline_health",
            [_pipeline_health_row(snapshot, collected_at=collected_at) for snapshot in pipelines],
            PIPELINE_HEALTH_COLUMNS,
        )
        self._insert_rows(
            "pipeline_table_freshness",
            [_pipeline_health_row(snapshot, collected_at=collected_at) for snapshot in tables],
            PIPELINE_TABLE_FRESHNESS_COLUMNS,
        )
        self._command(
            "DELETE FROM @pipeline_health WHERE pipeline <> ALL(%s)",
            ([snapshot.pipeline for snapshot in pipelines],),
        )
        self._command(
            "DELETE FROM @pipeline_table_freshness WHERE table_id <> ALL(%s)",
            ([snapshot.table_id for snapshot in tables],),
        )

    def write_mart_view_health(
        self, views: Sequence[Any], *, collected_at: datetime
    ) -> None:
        """Replace the mart-health snapshot with one collection's measurements."""
        self._insert_rows(
            "mart_view_health",
            [_pipeline_health_row(snapshot, collected_at=collected_at) for snapshot in views],
            MART_VIEW_HEALTH_COLUMNS,
        )
        self._command(
            "DELETE FROM @mart_view_health WHERE view_id <> ALL(%s)",
            ([snapshot.view_id for snapshot in views],),
        )

    def write_collation_health(
        self, findings: Sequence[Any], *, collected_at: datetime
    ) -> None:
        """Replace the collation/index-integrity snapshot.

        Retired objects are pruned like every other snapshot here: a dropped
        index must disappear rather than linger as a permanently failing row
        nobody can act on.
        """
        self._insert_rows(
            "collation_health",
            [_pipeline_health_row(finding, collected_at=collected_at) for finding in findings],
            COLLATION_HEALTH_COLUMNS,
        )
        self._command(
            "DELETE FROM @collation_health WHERE object_id <> ALL(%s)",
            ([finding.object_id for finding in findings],),
        )

    def bm25_timeline_index_names(self) -> list[str]:
        """Every BM25 index declared on the timeline, in registry order."""

        return [
            spec.name for spec in POSTGRES_INDEXES
            if spec.table == "timeline_events" and spec.requires_pg_textsearch
        ]

    def probe_bm25_indexes(self) -> dict[str, str]:
        """Scan one row through each timeline BM25 index; return errors by name.

        pg_textsearch indexes are not covered by amcheck, and a crash can
        leave one with bad pages while it still reads `indisvalid`. On
        2026-08-27 an OOM kill did exactly that to two of the four: every
        low-volume source then failed keyword and hybrid search with
        "invalid page index at block N" and nothing on /pipelines moved,
        because no health surface ever read the indexes. This does, once per
        chunk-builder run, with the cheapest query that touches them.
        """

        errors: dict[str, str] = {}
        for name in self.bm25_timeline_index_names():
            if not self._index_exists(name):
                continue
            # A partial index is only usable when the query implies its
            # predicate; without it the planner picks the global index and
            # to_bm25query RAISES ("query specifies index X but planner chose
            # Y") -- a false "failing" that says nothing about the pages.
            predicate = self._query(
                "SELECT pg_get_expr(i.indpred, i.indrelid) FROM pg_index i "
                "JOIN pg_class c ON c.oid = i.indexrelid "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE c.relname = %s AND n.nspname = %s",
                (name, self._object_schema("timeline_events")),
            )
            where = f"WHERE {predicate[0][0]} " if predicate and predicate[0][0] else ""
            try:
                # Several common words rather than one: a bad page is only
                # found by a scan that reads it, and each term walks its own
                # posting list. On 2026-08-27 a single-term probe read `ok`
                # while a query on other terms hit block 704084.
                self._query(
                    "SELECT 1 FROM @timeline_events t "
                    + where +
                    "ORDER BY t.search_text OPERATOR(public.<@>) "
                    f"public.to_bm25query({_literal(BM25_PROBE_QUERY)}, {_literal(name)}) LIMIT 50"
                )
                errors[name] = ""
            except Exception as error:  # noqa: BLE001 - reported, not raised
                errors[name] = str(error).strip()[:300]
        return errors

    def write_search_benchmark_runs(self, rows: Sequence[Any], *, collected_at: datetime) -> None:
        """Replace one mode's benchmark row with this run's measurement."""
        self._insert_rows(
            "search_benchmark_runs",
            [_pipeline_health_row(run, collected_at=collected_at) for run in rows],
            SEARCH_BENCHMARK_RUN_COLUMNS,
        )

    def load_search_benchmark_labels(self) -> list[dict[str, Any]]:
        return self._query_dicts(
            "SELECT query, stratum, verdict, truth_refs_json, truth_predicate_json, sources_json, since, note "
            "FROM @search_benchmark_labels ORDER BY query"
        )

    def publish_search_benchmark_labels(self, cases: Sequence[Any], *, replace: bool = True) -> int:
        """Store the benchmark's labels in the warehouse (private schema).

        `replace` drops labels absent from the new set, so the table mirrors
        the file that was published rather than accumulating retired cases.
        """
        now = datetime.now(tz=UTC)
        version = int(now.timestamp())
        rows = [
            {
                "query": case.query,
                "stratum": case.stratum,
                "verdict": case.verdict,
                "truth_refs_json": json.dumps(list(case.truth_refs)),
                "truth_predicate_json": json.dumps(case.truth_predicate) if case.truth_predicate else "",
                "sources_json": json.dumps(list(case.sources)),
                "since": case.since,
                "note": case.note,
                "updated_at": now,
                "sync_version": version,
            }
            for case in cases
        ]
        self._insert_rows("search_benchmark_labels", rows, SEARCH_BENCHMARK_LABEL_COLUMNS)
        if replace:
            self._command(
                "DELETE FROM @search_benchmark_labels WHERE query <> ALL(%s)",
                ([case.query for case in cases],),
            )
        return len(rows)

    def write_agent_usage(self, rows: Sequence[Any], *, collected_at: datetime) -> None:
        """Replace the agent-usage snapshot with one collection's measurements."""
        self._insert_rows(
            "agent_usage",
            [_pipeline_health_row(snapshot, collected_at=collected_at) for snapshot in rows],
            AGENT_USAGE_COLUMNS,
        )
        self._command(
            "DELETE FROM @agent_usage WHERE source <> ALL(%s)",
            ([snapshot.source for snapshot in rows],),
        )

    def write_timeline_priority_mix(
        self, rows: Sequence[Any], *, collected_at: datetime
    ) -> None:
        """Replace the per-source tier-mix snapshot with one collection's counts."""
        self._insert_rows(
            "timeline_priority_mix",
            [_pipeline_health_row(snapshot, collected_at=collected_at) for snapshot in rows],
            TIMELINE_PRIORITY_MIX_COLUMNS,
        )
        self._command(
            "DELETE FROM @timeline_priority_mix WHERE (source || ':' || priority) <> ALL(%s)",
            ([f"{snapshot.source}:{snapshot.priority}" for snapshot in rows],),
        )

    def write_search_health(self, component: str, **facts: Any) -> None:
        """Upsert one search-stage heartbeat without scanning the corpus."""
        if component not in {"chunks", "embeddings", "bm25_indexes"}:
            raise ValueError(f"unknown search health component: {component}")
        now = datetime.now(tz=UTC)
        row = {
            "component": component,
            "model": "",
            "configured": 1,
            "pgvector_available": 1,
            "timeline_max_seq": 0,
            "chunk_cursor_seq": 0,
            "caught_up": 0,
            "processed_rows": 0,
            "pending_count": -1,
            "oldest_pending_at": datetime.fromtimestamp(0, tz=UTC),
            "last_success_at": datetime.fromtimestamp(0, tz=UTC),
            "last_run_at": now,
            "last_error": "",
            "updated_at": now,
        }
        previous = self._query_dicts(
            "SELECT " + ", ".join(SEARCH_HEALTH_COLUMNS) +
            " FROM @search_health WHERE component = %s",
            (component,),
        )
        if previous:
            row.update(previous[0])
            row["last_run_at"] = now
            row["updated_at"] = now
        row.update(facts)
        self._insert_rows("search_health", [row], SEARCH_HEALTH_COLUMNS)

    def ensure_timeline_tables(self) -> None:
        """Tables for the unified timeline (personal_data_warehouse/timeline.py).

        ``seq`` is served by a dedicated sequence rather than the generic
        integer default so it survives upsert churn: the timeline upsert bumps
        it via nextval() whenever a row's content changes, giving consumers a
        durable arrival/change order that a plain event-time sort cannot
        provide (late backfills land in the past by event_ts but in the
        present by seq).
        """
        # The priority enum must exist before the table is created: on a fresh
        # install _postgres_type builds the priority column as this type.
        self._ensure_timeline_priority_type()
        self._ensure_table_group(["timeline_events", "timeline_sync_state"])
        # Migration for pre-existing sync-state tables (the table is ~24 rows,
        # so the unconditional ACCESS EXCLUSIVE lock here is harmless).
        self._command(
            "ALTER TABLE @timeline_sync_state "
            "ADD COLUMN IF NOT EXISTS adapter_signature text NOT NULL DEFAULT ''"
        )
        self._command(
            "ALTER TABLE @timeline_sync_state ADD COLUMN IF NOT EXISTS "
            "last_reconcile_at timestamptz NOT NULL "
            "DEFAULT '1970-01-01 00:00:00+00'::timestamptz"
        )
        # These ALTERs run under an ACCESS EXCLUSIVE lock on a 45+ GB table and
        # ensure_timeline_tables runs on every timeline sync (~288/day in
        # prod), so each is gated on the current catalog state instead of
        # re-issued unconditionally.
        sequence_ref = self.sql_relation("timeline_events_seq")
        defaults = self._column_defaults("timeline_events")
        if not self._default_uses_sequence(defaults.get("seq", ""), sequence_ref):
            self._command("CREATE SEQUENCE IF NOT EXISTS @timeline_events_seq")
            self._command(f"ALTER TABLE @timeline_events ALTER COLUMN seq SET DEFAULT nextval('{sequence_ref}')")
        for column in ("first_seen_at", "updated_at"):
            if not defaults.get(column, "").startswith("now()"):
                self._command(f"ALTER TABLE @timeline_events ALTER COLUMN {column} SET DEFAULT now()")
        # Addresses Zach has ever written to, with counts — the relationship
        # signal the gmail adapter's known-correspondent rule reads. Refreshed
        # by TimelineSyncEngine at most once per day.
        self._command(
            """
            CREATE TABLE IF NOT EXISTS @timeline_gmail_correspondents (
                addr text PRIMARY KEY,
                n_sent_to bigint NOT NULL DEFAULT 0,
                last_sent_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
                refreshed_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        self._ensure_timeline_comments()
        self._ensure_search_views_if_possible()

    def pgvector_available(self) -> bool:
        """Whether this Postgres can CREATE EXTENSION vector.

        The extension ships in the warehouse's postgres image; a host running
        an older image simply lacks it, and every vector-dependent surface
        (embedding column, HNSW index, search_hybrid) degrades until the image
        is rolled — code never assumes it.
        """
        rows = self._query(
            "SELECT 1 FROM pg_available_extensions WHERE name = 'vector'"
        )
        return bool(rows)

    def ensure_search_index_tables(self) -> None:
        """Tables for the derived search-retrieval layer (search_index.py).

        The chunk tables are plain and always creatable; the embedding vector
        column and its HNSW index exist only where the pgvector extension is
        installable, so the same code runs against pre-pgvector hosts (the
        chunk backlog builds up and embeds later).
        """
        self._ensure_table_group(
            ["search_chunks", "search_chunk_embeddings", "search_chunk_sync_state"]
        )
        # The embedding drain's persisted cursors joined an existing table;
        # CREATE TABLE IF NOT EXISTS cannot add them to a live deployment.
        for column in ("embed_fresh_built_at", "embed_cursor_ts", "embed_cursor_id",
                       "embed_backfill_status"):
            self._command(
                "ALTER TABLE @search_chunk_sync_state ADD COLUMN IF NOT EXISTS "
                f"{_identifier(column)} {_postgres_type(column)} NOT NULL DEFAULT "
                f"{_default_sql(column)}"
            )
        if self.pgvector_available():
            if not self._pgvector_ensured:
                self._command("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public")
                self._pgvector_ensured = True
            self._command(
                "ALTER TABLE @search_chunk_embeddings "
                "ADD COLUMN IF NOT EXISTS embedding public.halfvec(512)"
            )
            # Re-walk the index specs now that the vector column exists (the
            # first pass above ran before it and skipped the HNSW build).
            self._ensured_index_names.discard("search_chunk_embeddings_hnsw_idx")
            self._ensure_indexes(["search_chunk_embeddings"])
        self._ensure_search_views_if_possible()

    def _ensure_timeline_priority_type(self) -> None:
        """Create the ``@timeline_priority`` enum if it does not exist.

        Mirrors the ``@search_text_hit`` composite-type bootstrap. Both the
        probe and the CREATE name the type through the catalog: an unqualified
        CREATE TYPE would land in whichever schema the search_path happens to
        list first, which is a base_* source schema.

        The labels are the five real tiers plus ``unclassified``, which is the
        column default and never a value an adapter emits — see
        ``TIMELINE_PRIORITY_UNCLASSIFIED``. Dropping the label would rewrite a
        60 GB column, so it stays as a fail-loud sentinel; the type's COMMENT
        (``_ensure_timeline_comments``) says so to anyone reading the schema.
        """
        # Generated from the one definition list so the labels and the COMMENT
        # can never drift apart. Order is load-bearing: enum declaration order
        # is the tier sort order.
        labels = ", ".join(f"'{label}'" for label, _ in TIMELINE_PRIORITY_DEFINITIONS)
        type_ref = self.sql_relation("timeline_priority")
        type_literal = type_ref.replace("\'", "\'\'")
        self._command(
            r"""
            DO $do$
            BEGIN
                IF to_regtype('"""
            + type_literal
            + r"""') IS NULL THEN
                    CREATE TYPE """
            + type_ref
            + r""" AS ENUM ("""
            + labels
            + r""");
                END IF;
            END
            $do$;
            """
        )

    # What every agent-facing timeline.events column means, published as
    # Postgres COMMENTs. The timeline is the documented entry point and agents
    # are told to filter on `priority`, but col_description() returned NULL for
    # all nineteen columns, so anything reading the schema directly (psql \d+,
    # a generic SQL client, an agent that never calls describe_table) got the
    # names and nothing else.
    _TIMELINE_EVENT_COLUMN_COMMENTS = {
        "adapter": (
            "Which timeline adapter produced this row (timeline.py). Stable "
            "identifier; one source can have several (slack_message vs slack_file)."
        ),
        "event_id": (
            "Adapter-scoped identity of the underlying thing. (adapter, event_id) "
            "is the primary key, so re-syncing the same source row updates in place."
        ),
        "source": (
            "The originating system: gmail, slack, calendar, whatsapp, photos, "
            "finance, and for agent sessions the provider (claude_code, codex, ...)."
        ),
        "kind": "The shape of the event within its source: email, message, event, photo, agent_turn, ...",
        "priority": (
            "Attention tier. See the timeline_priority type's own comment for the "
            "five definitions; filter priority IN ('self','direct','cc') for a review "
            "of what involved Zach, and exclude 'noise'/'background' machinery."
        ),
        "event_ts": "When the thing happened in the real world. The timeline's default sort.",
        "end_ts": (
            "When it finished (meeting end, workout end, session end), or the epoch "
            "sentinel 1970-01-01 when the event is instantaneous or the end is unknown."
        ),
        "actor": "Who or what caused it, as the source names them: sender, uploader, organizer, 'me'.",
        "title": "Short headline (subject, summary, filename, first prompt), capped.",
        "snippet": "Capped preview of the body. The full record lives behind source_table/source_pk.",
        "context": (
            "A DISPLAY label for the stream this event belongs to: channel, chat, folder, "
            "calendar, or '<provider>|<session_id>' for agent turns. It is a label, not an "
            "identity -- gmail stores the mailbox account and every Slack group DM stores the "
            "literal 'group DM' -- so timeline.context() resolves a chat/email conversation "
            "from the source row instead, and pages over this column only for the sources "
            "where it IS the identity."
        ),
        "source_table": (
            "Catalog logical id of the authoritative relation (gmail_messages, "
            "slack_messages, ...), not a physical schema-qualified name."
        ),
        "source_pk": "JSON primary key of the authoritative row in source_table. One hop to the full record.",
        "metadata": "Per-source structured extras kept out of the flat columns (counts, ids, flags, sizes).",
        "search_text": (
            "The BM25/trigram-indexed document for this event, assembled per adapter. "
            "Search it through timeline.search_text() rather than scanning it."
        ),
        "ingest_ts": (
            "When the warehouse last learned about this row from its source. Drives "
            "the adapter's incremental watermark; unrelated to event_ts."
        ),
        "seq": (
            "Monotonic arrival/change order, bumped only when the row's content "
            "changes. Checkpoint on this to consume the timeline exactly once, "
            "including late backfills that land in the past by event_ts."
        ),
        "first_seen_at": "When this timeline row was first written.",
        "updated_at": "When this timeline row was last written.",
    }

    def _ensure_timeline_comments(self) -> None:
        """Publish the priority tiers and column meanings as Postgres COMMENTs.

        Same posture as _ensure_schema_comments: probe first and write only on
        drift, because ensure_timeline_tables runs on every timeline sync
        (~288/day in prod) and an unconditional COMMENT ON per column would
        churn pg_description forever.
        """
        type_ref = self.sql_relation("timeline_priority")
        tiers = "; ".join(
            f"{label} = {meaning}" for label, meaning in TIMELINE_PRIORITY_DEFINITIONS
        )
        type_comment = (
            "Attention tier of a timeline.events row, classified per row at sync "
            "time by its adapter. Enum declaration order is the sort order, highest "
            f"attention first. {tiers}."
        )
        current_type = self._query(
            "SELECT obj_description(%s::regtype, 'pg_type')", (type_ref,)
        )
        if not current_type or current_type[0][0] != type_comment:
            self._raw_command(f"COMMENT ON TYPE {type_ref} IS %s", (type_comment,))

        table_ref = self.sql_relation("timeline_events")
        current = {
            column: comment
            for column, comment in self._query(
                """
                SELECT a.attname, col_description(a.attrelid, a.attnum)
                FROM pg_attribute a
                WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped
                """,
                (table_ref,),
            )
        }
        for column, comment in self._TIMELINE_EVENT_COLUMN_COMMENTS.items():
            if column not in current or current[column] == comment:
                continue
            self._raw_command(
                f"COMMENT ON COLUMN {table_ref}.{_identifier(column)} IS %s", (comment,)
            )

    def ensure_claude_desktop_tables(self) -> None:
        """Tables for the serverside Claude Desktop poller.

        ``claude_desktop_credentials`` is *also* created by the Go app (the
        clientside auth pusher writes it through ``/ingest/claude-desktop/credential``);
        both use the identical idempotent DDL so whichever runs first wins. The
        poller only reads it. ``claude_desktop_conversation_state`` is the
        Postgres-durable per-conversation ``updated_at`` cursor so the poller does
        not re-fetch every conversation after a deploy.
        """
        self._command(
            """
            CREATE TABLE IF NOT EXISTS @claude_desktop_credentials (
                account text PRIMARY KEY,
                session_key text NOT NULL,
                org_id text NOT NULL DEFAULT '',
                expires_at timestamptz NULL,
                captured_at timestamptz NOT NULL DEFAULT now(),
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        self._command(
            """
            CREATE TABLE IF NOT EXISTS @claude_desktop_conversation_state (
                account text NOT NULL,
                conversation_id text NOT NULL,
                updated_at text NOT NULL DEFAULT '',
                last_synced_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (account, conversation_id)
            )
            """
        )

    def read_claude_desktop_credential(self, *, account: str) -> dict[str, Any] | None:
        self.ensure_claude_desktop_tables()
        rows = self._query_dicts(
            """
            SELECT account, session_key, org_id, expires_at, captured_at, updated_at
            FROM @claude_desktop_credentials
            WHERE account = %s
            """,
            (account,),
        )
        return rows[0] if rows else None

    def read_latest_claude_desktop_credential(self) -> dict[str, Any] | None:
        """Return the most recently pushed credential, regardless of its account label.

        The serverside poller resolves the *real* account from the live session
        (see ``ClaudeAiClient.account_email``), so it does not need to know the
        account up front - it reads whichever credential the clientside pusher
        last wrote. This keeps the account out of serverside env entirely (the
        stored label is only a fallback). Single Claude Desktop login in practice,
        so "latest" is unambiguous.
        """
        self.ensure_claude_desktop_tables()
        rows = self._query_dicts(
            """
            SELECT account, session_key, org_id, expires_at, captured_at, updated_at
            FROM @claude_desktop_credentials
            ORDER BY updated_at DESC
            LIMIT 1
            """
        )
        return rows[0] if rows else None

    def claude_desktop_cursor(self, *, account: str, conversation_id: str) -> str:
        rows = self._query(
            """
            SELECT updated_at FROM @claude_desktop_conversation_state
            WHERE account = %s AND conversation_id = %s
            """,
            (account, conversation_id),
        )
        return str(rows[0][0]) if rows else ""

    def record_claude_desktop_cursor(
        self, *, account: str, conversation_id: str, updated_at: str, now: datetime | None = None
    ) -> None:
        synced = now or datetime.now(tz=UTC)
        self._command(
            """
            INSERT INTO @claude_desktop_conversation_state (account, conversation_id, updated_at, last_synced_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (account, conversation_id) DO UPDATE SET
                updated_at = EXCLUDED.updated_at,
                last_synced_at = EXCLUDED.last_synced_at
            """,
            (account, conversation_id, updated_at, synced),
        )

    def ensure_whatsapp_client_session_table(self) -> None:
        self._command(
            """
            CREATE TABLE IF NOT EXISTS @whatsapp_client_sessions (
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
        self._command("ALTER TABLE @whatsapp_client_sessions ADD COLUMN IF NOT EXISTS client_id text NOT NULL DEFAULT ''")
        self._command("ALTER TABLE @whatsapp_client_sessions ADD COLUMN IF NOT EXISTS database_bytes bytea NOT NULL DEFAULT ''::bytea")
        self._command("ALTER TABLE @whatsapp_client_sessions ADD COLUMN IF NOT EXISTS database_sha256 text NOT NULL DEFAULT ''")
        self._command("ALTER TABLE @whatsapp_client_sessions ADD COLUMN IF NOT EXISTS database_bytes_size bigint NOT NULL DEFAULT 0")
        self._command(
            "ALTER TABLE @whatsapp_client_sessions ADD COLUMN IF NOT EXISTS restored_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz"
        )
        self._command("ALTER TABLE @whatsapp_client_sessions ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now()")
        self._command("ALTER TABLE @whatsapp_client_sessions ADD COLUMN IF NOT EXISTS sync_version bigint NOT NULL DEFAULT 1")

    def get_whatsapp_client_session(self, *, account: str, session_key: str) -> dict[str, Any] | None:
        self.ensure_whatsapp_client_session_table()
        rows = self._query_dicts(
            """
            SELECT account, session_key, client_id, database_bytes, database_sha256,
                   database_bytes_size, restored_at, updated_at, sync_version
            FROM @whatsapp_client_sessions
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
            INSERT INTO @whatsapp_client_sessions (
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
            CREATE TABLE IF NOT EXISTS @chatgpt_sessions (
                account text NOT NULL,
                session_key text NOT NULL DEFAULT 'default',
                session_token text NOT NULL DEFAULT '',
                source_browser text NOT NULL DEFAULT '',
                token_sha256 text NOT NULL DEFAULT '',
                published_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
                updated_at timestamptz NOT NULL DEFAULT now(),
                sync_version bigint NOT NULL DEFAULT 1,
                expired_at timestamptz,
                expired_token_sha256 text NOT NULL DEFAULT '',
                token_expires_at timestamptz,
                status text NOT NULL DEFAULT 'ok',
                error text NOT NULL DEFAULT '',
                PRIMARY KEY (account, session_key)
            )
            """
        )
        self._command("ALTER TABLE @chatgpt_sessions ADD COLUMN IF NOT EXISTS source_browser text NOT NULL DEFAULT ''")
        self._command("ALTER TABLE @chatgpt_sessions ADD COLUMN IF NOT EXISTS token_sha256 text NOT NULL DEFAULT ''")
        self._command(
            "ALTER TABLE @chatgpt_sessions ADD COLUMN IF NOT EXISTS published_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz"
        )
        self._command("ALTER TABLE @chatgpt_sessions ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now()")
        self._command("ALTER TABLE @chatgpt_sessions ADD COLUMN IF NOT EXISTS sync_version bigint NOT NULL DEFAULT 1")
        # A poll that gets a 401 marks the current token expired here so the sensor can
        # skip instead of relaunching doomed runs; keyed to the token's sha so a fresh
        # publish (which rotates the sha) clears it automatically.
        self._command("ALTER TABLE @chatgpt_sessions ADD COLUMN IF NOT EXISTS expired_at timestamptz")
        self._command("ALTER TABLE @chatgpt_sessions ADD COLUMN IF NOT EXISTS expired_token_sha256 text NOT NULL DEFAULT ''")
        # The access token's own hard expiry, learned on each successful poll. It
        # is the only reliable warning that the credential is about to lapse: the
        # token is minted at browser sign-in and lives exactly 10 days, and no
        # server-side call renews it.
        self._command("ALTER TABLE @chatgpt_sessions ADD COLUMN IF NOT EXISTS token_expires_at timestamptz")
        self._command("ALTER TABLE @chatgpt_sessions ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'ok'")
        self._command("ALTER TABLE @chatgpt_sessions ADD COLUMN IF NOT EXISTS error text NOT NULL DEFAULT ''")
        # ``expired_*`` predates the health status columns. On upgrade, an
        # already-rejected current token receives the new columns' defaults
        # (ok/empty), while the sensor immediately starts skipping that token.
        # Without this data migration no later asset run can correct the false
        # green state. The predicate makes this a one-time repair, and tying it
        # to the current token preserves the concurrency guarantee used by the
        # poller and publisher.
        self._command(
            """
            UPDATE @chatgpt_sessions
            SET status = 'action_required',
                error = CASE
                    WHEN error = '' THEN %s
                    ELSE error
                END,
                updated_at = GREATEST(updated_at, expired_at)
            WHERE expired_at IS NOT NULL
              AND expired_token_sha256 <> ''
              AND token_sha256 = expired_token_sha256
              AND (status <> 'action_required' OR error = '')
            """,
            ("ChatGPT session expired; run `pdw chatgpt publish-session` to refresh it.",),
        )

    def ensure_chatgpt_conversation_sync_table(self) -> None:
        """Per-conversation incremental sync watermark for the ChatGPT poller."""
        self._command(
            """
            CREATE TABLE IF NOT EXISTS @chatgpt_conversation_sync (
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
                   published_at, updated_at, sync_version, expired_at, expired_token_sha256,
                   token_expires_at, status, error
            FROM @chatgpt_sessions
            WHERE account = %s AND session_key = %s
            """,
            (account, session_key),
        )
        return rows[0] if rows else None

    def mark_chatgpt_session_expired(
        self,
        *,
        account: str,
        session_key: str,
        token_sha256: str,
        when: datetime | None = None,
    ) -> None:
        """Record that the stored session token was rejected (HTTP 401).

        Guarded on ``token_sha256`` so a concurrent re-publish (which rotates the
        token and its hash) is never clobbered: only the exact token that failed is
        marked. A later publish changes ``token_sha256`` so it no longer matches
        ``expired_token_sha256`` and the poller resumes on its own.
        """
        if not token_sha256:
            return
        self.ensure_chatgpt_session_table()
        when = when or datetime.now(tz=UTC)
        self._command(
            """
            UPDATE @chatgpt_sessions
            SET expired_at = %s,
                expired_token_sha256 = %s,
                status = 'action_required',
                error = %s,
                updated_at = %s
            WHERE account = %s AND session_key = %s AND token_sha256 = %s
            """,
            (
                when,
                token_sha256,
                "ChatGPT session expired; run `pdw chatgpt publish-session` to refresh it.",
                when,
                account,
                session_key,
                token_sha256,
            ),
        )

    def record_chatgpt_session_success(
        self,
        *,
        account: str,
        session_key: str,
        token_sha256: str,
        token_expires_at: datetime | None = None,
        now: datetime | None = None,
    ) -> None:
        """Record a successful poll and clear health errors for that exact token.

        ``token_expires_at`` is the access token's hard expiry as read from its
        JWT. Because that expiry is fixed at browser sign-in and unreachable from
        here, an imminent lapse is announced as ``action_required`` *while the
        poller keeps working* - the credential still authenticates, a human just
        needs to sign in again soon. Deliberately distinct from
        ``mark_chatgpt_session_expired``: this never sets ``expired_at``, so the
        sensor does not stop polling a session that is still perfectly good.
        """
        if not token_sha256:
            return
        self.ensure_chatgpt_session_table()
        now = now or datetime.now(tz=UTC)
        warning = (
            token_expiry_warning(token_expires_at.timestamp(), now=now.timestamp())
            if token_expires_at is not None
            else None
        )
        self._command(
            """
            UPDATE @chatgpt_sessions
            SET expired_at = NULL,
                expired_token_sha256 = '',
                token_expires_at = COALESCE(%s, token_expires_at),
                status = %s,
                error = %s,
                updated_at = now()
            WHERE account = %s AND session_key = %s AND token_sha256 = %s
            """,
            (
                token_expires_at,
                "action_required" if warning else "ok",
                warning or "",
                account,
                session_key,
                token_sha256,
            ),
        )

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
            INSERT INTO @chatgpt_sessions (
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

    def _ensure_transcription_runs_rejections_reclassified(self) -> None:
        """Reclassify legacy permanent input rejections from 'error' to 'rejected'.

        Rows written before 'rejected' existed recorded "no spoken audio",
        "audio duration is too short" and "does not appear to contain audio" as
        status 'error'. Nothing ever retries them -- the candidate query already
        treated a non-retryable error as terminal -- so once
        voice_memo_transcription declared this table as its StateSource they
        pinned it to 'failing' permanently, because the error count is over the
        whole table with no time bound. Measured on production 2026-08-27:
        eleven such rows, the oldest from 2026-05-01, which is why the row was
        already red before the AssemblyAI billing outage it was meant to catch.

        The retryable-pattern list is the same authority the candidate query and
        the Python writer use, so this cannot reclassify a row that a retry
        would have fixed. The error text is kept as the reason.
        """
        self._command(
            f"""
            UPDATE @apple_voice_memos_transcription_runs
            SET status = 'rejected'
            WHERE status = 'error'
              AND COALESCE(error, '') != ''
              AND NOT ({_postgres_retryable_error_clause('error')})
              AND ({_postgres_permanent_rejection_clause('error')})
            """
        )

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
                # The fingerprint link table provisions with the rest of Slack
                # so a fresh warehouse has it; the asset's narrower
                # ensure_slack_file_fingerprint_tables() is a subset of this.
                "slack_file_fingerprints",
                "media_fingerprints",
                "slack_sync_state",
                "slack_account_state_item_rows",
                # The captured client session that lets the sync ask Slack what
                # changed in one request instead of polling every conversation.
                "slack_sessions",
            ]
        )
        self._ensure_slack_conversation_stats_backfilled()
        self._ensure_slack_sync_state_gone_reclassified()
        self._ensure_clean_slack_inbox_view()
        self._ensure_slack_image_fingerprint_view()
        self._ensure_slack_huddles_view()
        self._ensure_slack_conversation_health_view()
        self._ensure_search_views_if_possible()

    def ensure_upstream_mutation_tables(self) -> None:
        self._ensure_upstream_mutation_tables_ddl()
        for logical in (
            "upstream_mutation_requests",
            "upstream_mutations",
            "upstream_mutation_events",
            "upstream_mutation_request_events",
            "push_devices",
        ):
            self._apply_catalog_grant(logical)

    def _ensure_upstream_mutation_tables_ddl(self) -> None:
        self._command(
            """
            CREATE TABLE IF NOT EXISTS @upstream_mutation_requests (
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
                observed_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
                superseded_by_request_id text NOT NULL DEFAULT ''
            )
            """
        )
        self._command(
            """
            CREATE TABLE IF NOT EXISTS @upstream_mutations (
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
        self._command("ALTER TABLE @upstream_mutations ADD COLUMN IF NOT EXISTS request_id text NOT NULL DEFAULT ''")
        self._command("ALTER TABLE @upstream_mutations ADD COLUMN IF NOT EXISTS request_index bigint NOT NULL DEFAULT 0")
        self._command(
            """
            CREATE TABLE IF NOT EXISTS @upstream_mutation_events (
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
            CREATE TABLE IF NOT EXISTS @upstream_mutation_request_events (
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
            # The Go app owns this table's write path and declares the same
            # column; both ensure paths must agree, or whichever bootstraps a
            # database first decides its shape.
            "ALTER TABLE @upstream_mutation_requests ADD COLUMN IF NOT EXISTS superseded_by_request_id text NOT NULL DEFAULT ''",
            "CREATE UNIQUE INDEX IF NOT EXISTS upstream_mutation_requests_idempotency_idx ON @upstream_mutation_requests (idempotency_key) WHERE idempotency_key != ''",
            "CREATE INDEX IF NOT EXISTS upstream_mutation_requests_status_updated_idx ON @upstream_mutation_requests (status, updated_at)",
            "CREATE UNIQUE INDEX IF NOT EXISTS upstream_mutations_idempotency_idx ON @upstream_mutations (idempotency_key) WHERE idempotency_key != ''",
            "CREATE INDEX IF NOT EXISTS upstream_mutations_request_idx ON @upstream_mutations (request_id, request_index, created_at, id)",
            "CREATE INDEX IF NOT EXISTS upstream_mutations_status_updated_idx ON @upstream_mutations (status, updated_at)",
            "CREATE INDEX IF NOT EXISTS upstream_mutation_request_events_request_idx ON @upstream_mutation_request_events (request_id, event_index)",
            "CREATE INDEX IF NOT EXISTS upstream_mutation_events_mutation_idx ON @upstream_mutation_events (mutation_id, event_index)",
            # Devices registered by the PDW iOS app for push notifications. The
            # Go app owns the writes (POST /api/push/register) and the sends; the
            # idempotent twin of this DDL lives in app/internal/push/store.go.
            # A device stays a row after it stops working: `status` flips to
            # `disabled` with the provider's reason so an unreachable phone is a
            # fact, not a silently shrinking fan-out.
            """
            CREATE TABLE IF NOT EXISTS @push_devices (
                expo_push_token text PRIMARY KEY,
                client_name text NOT NULL DEFAULT '',
                device_name text NOT NULL DEFAULT '',
                platform text NOT NULL DEFAULT '',
                app_version text NOT NULL DEFAULT '',
                status text NOT NULL DEFAULT 'active',
                error text NOT NULL DEFAULT '',
                registered_at timestamptz NOT NULL DEFAULT now(),
                updated_at timestamptz NOT NULL DEFAULT now(),
                last_sent_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
                last_error_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz
            )
            """,
            "CREATE INDEX IF NOT EXISTS push_devices_status_updated_idx ON @push_devices (status, updated_at)",
        ):
            self._command(sql)
        # Recreate search_text() if needed; general search now reads the
        # timeline, but mutation ensure still participates in the shared search
        # schema convergence path.
        self._ensure_search_views_if_possible()

    def list_upstream_mutations_for_request(self, request_id: str) -> list[dict[str, Any]]:
        self.ensure_upstream_mutation_tables()
        return self._query_dicts(
            """
            SELECT *
            FROM @upstream_mutations
            WHERE request_id = %s
            ORDER BY request_index ASC, created_at ASC, id ASC
            """,
            (request_id,),
        )

    def get_upstream_mutation(self, mutation_id: str) -> dict[str, Any] | None:
        rows = self._query_dicts("SELECT * FROM @upstream_mutations WHERE id = %s", (mutation_id,))
        return rows[0] if rows else None

    def claim_approved_upstream_mutations(
        self,
        *,
        limit: int,
        claimed_by: str,
        providers: Sequence[str] | None = None,
        exclude_providers: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Claim approved mutations, optionally scoped to a set of providers.

        Scoping is what keeps two workers off one queue. Apple Notes can only be applied
        on a Mac running Notes.app, so the cloud worker excludes that provider and the
        Mac worker claims only it. Without the filter the cloud worker would claim an
        apple_notes row, fail it as unknown-provider, and bump attempt_count on every
        tick while the Mac never got a chance at it.
        """

        self.ensure_upstream_mutation_tables()
        if limit <= 0:
            return []
        provider_filter, provider_params = _upstream_mutation_provider_filter(
            providers=providers,
            exclude_providers=exclude_providers,
        )
        now = datetime.now(tz=UTC)
        rows = self._query_dicts(
            f"""
            WITH candidates AS (
                SELECT id
                FROM @upstream_mutations
                WHERE status = ANY(%s)
                  {provider_filter}
                ORDER BY approved_at ASC, created_at ASC, id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT %s
            )
            UPDATE @upstream_mutations AS mutation
               SET status = 'executing',
                   claimed_by = %s,
                   claimed_at = %s,
                   updated_at = %s,
                   attempt_count = attempt_count + 1
              FROM candidates
             WHERE mutation.id = candidates.id
            RETURNING mutation.*
            """,
            (
                list(UPSTREAM_MUTATION_CLAIMABLE_STATUSES),
                *provider_params,
                int(limit),
                claimed_by,
                now,
                now,
            ),
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
                FROM @upstream_mutations
                WHERE status = 'executing'
                  AND claimed_at < %s
                  AND (provider, operation) IN (
                      SELECT * FROM UNNEST(%s::text[], %s::text[])
                  )
                FOR UPDATE SKIP LOCKED
            )
            UPDATE @upstream_mutations AS mutation
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
            FROM @upstream_mutations
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
            UPDATE @upstream_mutations
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
                UPDATE @upstream_mutations AS mutation
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
                LEFT JOIN @upstream_mutation_events AS event
                  ON event.mutation_id = event_data.mutation_id
                GROUP BY event_data.mutation_id
            )
            INSERT INTO @upstream_mutation_events (
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
            UPDATE @upstream_mutations
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

    def approved_upstream_mutation_count(
        self,
        *,
        ensure_tables: bool = True,
        providers: Sequence[str] | None = None,
        exclude_providers: Sequence[str] | None = None,
    ) -> int:
        if ensure_tables:
            self.ensure_upstream_mutation_tables()
        provider_filter, provider_params = _upstream_mutation_provider_filter(
            providers=providers,
            exclude_providers=exclude_providers,
        )
        rows = self._query(
            f"""
            SELECT count(*)::bigint
            FROM @upstream_mutations
            WHERE status = ANY(%s)
              {provider_filter}
            """,
            (list(UPSTREAM_MUTATION_CLAIMABLE_STATUSES), *provider_params),
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
            FROM @gmail_messages
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
            FROM @upstream_mutations
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
                FROM @gmail_messages
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
                UPDATE @upstream_mutations
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
            FROM @upstream_mutations
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
                FROM @gmail_messages
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
                UPDATE @upstream_mutations
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
            FROM @upstream_mutations
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
                FROM @gmail_messages
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
                UPDATE @upstream_mutations
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
            FROM @upstream_mutations
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
                UPDATE @upstream_mutations
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
            FROM @upstream_mutations
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
                UPDATE @upstream_mutations
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
            FROM @calendar_events
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
        request = self._query_dicts("SELECT * FROM @upstream_mutation_requests WHERE id = %s", (request_id,))
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
            UPDATE @upstream_mutation_requests
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

    def _contact_card(self, *, account: str, resource_name: str) -> dict[str, Any] | None:
        rows = self._query_dicts(
            """
            SELECT *
            FROM @contact_cards
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
                if result_etag and str(row["etag"]) != result_etag and not _contact_update_fields_observed(row, operation):
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
            INSERT INTO @upstream_mutation_events (
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
            FROM @upstream_mutation_events
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

    def _ensure_table_group(self, tables: Sequence[str]) -> None:
        for table in tables:
            self._ensure_table(table)
        self._ensure_indexes(tables)

    def _reconcile_table_columns(self, table: str) -> list[str]:
        """Add any ``TableSpec`` column an existing table is missing.

        ``CREATE TABLE IF NOT EXISTS`` never revisits an existing table, so a
        column added to a spec reaches every fresh database -- and every test,
        and CI -- while a long-lived warehouse keeps the old shape. Whatever
        names the column then fails on every run, and the suite stays green,
        because no fresh-database test can reproduce "provisioned before the
        column existed".

        That has now happened three times on the health snapshots
        (``ops.pipeline_health`` 2026-08-23, ``ops.pgbackrest_health``
        2026-08-27, ``ops.agent_usage`` 2026-08-28), each time repaired by
        hand-writing one more ``ADD COLUMN IF NOT EXISTS`` beside the last.
        Hand-written lists are the bug: the author who adds the column is
        exactly the author who does not know it needs a migration line. This
        derives the same DDL from the spec the creation path already uses, so
        the two cannot disagree.

        Deliberately NOT applied to every table: it is called for the small
        snapshot tables whose whole content is rewritten each collection, where
        an added column is metadata-only and the lock is irrelevant. Returns the
        columns it added, so a caller can log a real migration.
        """
        rel = canonical_relation(table).with_namespace(self._schema)
        present = {
            str(row[0])
            for row in self._query(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s",
                (rel.schema, rel.name),
            )
        }
        if not present:
            # The table does not exist yet; _ensure_table is about to create it
            # with every column.
            return []
        added: list[str] = []
        for column in POSTGRES_TABLES[table].columns:
            if column in present:
                continue
            self._command(
                f"ALTER TABLE {self.sql_relation(table)} ADD COLUMN IF NOT EXISTS "
                f"{_identifier(column)} {_postgres_type(column, table=table)} "
                f"NOT NULL DEFAULT {_default_sql(column, table=table)}"
            )
            added.append(column)
        return added

    def _ensure_table(self, table: str) -> None:
        spec = POSTGRES_TABLES[table]
        column_sql = [
            f"{_identifier(column)} {_postgres_type(column, table=table)} NOT NULL DEFAULT {_default_sql(column, table=table)}"
            for column in spec.columns
        ]
        primary_key = ", ".join(_identifier(column) for column in spec.primary_key)
        self._command(
            f"""
            CREATE TABLE IF NOT EXISTS {self.sql_relation(table)} (
                {", ".join(column_sql)},
                PRIMARY KEY ({primary_key})
            )
            """
        )
        if spec.storage_parameters:
            # ensure_* runs on every Dagster run, and ALTER TABLE ... SET takes
            # an ACCESS EXCLUSIVE lock even when it changes nothing — ~2.3k
            # no-op repeats on timeline.events in one prod stats window. Only
            # run it when an option is actually missing or different.
            desired = {f"{key}={value}" for key, value in spec.storage_parameters}
            if not desired <= self._table_reloptions(table):
                settings = ", ".join(f"{key} = {value}" for key, value in spec.storage_parameters)
                self._command(f"ALTER TABLE {self.sql_relation(table)} SET ({settings})")
        self._apply_catalog_grant(table)

    def _primary_key_columns(self, table: str) -> tuple[str, ...]:
        rel = canonical_relation(table).with_namespace(self._schema)
        rows = self._query(
            """
            SELECT a.attname
            FROM pg_index AS i
            INNER JOIN pg_class AS c ON c.oid = i.indrelid
            INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
            INNER JOIN pg_attribute AS a
              ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
            WHERE i.indisprimary AND n.nspname = %s AND c.relname = %s
            ORDER BY array_position(i.indkey, a.attnum)
            """,
            (rel.schema, rel.name),
        )
        return tuple(str(row[0]) for row in rows)

    def _ensure_primary_key(self, table: str) -> bool:
        """Move an existing table onto the key its ``TableSpec`` now declares.

        ``CREATE TABLE IF NOT EXISTS`` never revisits the primary key, so a
        table that gains a key column keeps upserting on the OLD conflict
        target -- which is not a loud error, it is a silent overwrite of a
        different row. Returns True when it changed anything.
        """
        desired = tuple(POSTGRES_TABLES[table].primary_key)
        if self._primary_key_columns(table) == desired:
            return False
        rel = canonical_relation(table).with_namespace(self._schema)
        rows = self._query(
            """
            SELECT con.conname
            FROM pg_constraint AS con
            INNER JOIN pg_class AS c ON c.oid = con.conrelid
            INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE con.contype = 'p' AND n.nspname = %s AND c.relname = %s
            """,
            (rel.schema, rel.name),
        )
        relation_sql = self.sql_relation(table)
        for (constraint_name,) in rows:
            self._command(f"ALTER TABLE {relation_sql} DROP CONSTRAINT {_identifier(str(constraint_name))}")
        columns = ", ".join(_identifier(column) for column in desired)
        self._command(f"ALTER TABLE {relation_sql} ADD PRIMARY KEY ({columns})")
        return True

    def _table_reloptions(self, table: str) -> set[str]:
        rel = canonical_relation(table).with_namespace(self._schema)
        rows = self._query(
            """
            SELECT unnest(c.reloptions)
            FROM pg_class AS c
            INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
            """,
            (rel.schema, rel.name),
        )
        return {str(row[0]) for row in rows}

    def _column_defaults(self, table: str) -> dict[str, str]:
        rel = canonical_relation(table).with_namespace(self._schema)
        rows = self._query(
            """
            SELECT column_name, column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (rel.schema, rel.name),
        )
        return {str(row[0]): str(row[1] or "") for row in rows}

    def _default_uses_sequence(self, column_default: str, sequence_ref: str) -> bool:
        """Does this column default draw from exactly this sequence?

        Postgres renders a stored ``nextval`` default with or without the
        schema qualifier depending on the connection's search_path, so a text
        comparison is unreliable (the unqualified-name trap behind the 07-25
        search_text outage). Resolve both names through ``to_regclass`` on the
        same connection and compare identities instead.
        """
        match = re.match(r"nextval\('(.+)'::regclass\)$", column_default)
        if match is None:
            return False
        rows = self._query(
            "SELECT to_regclass(%s) IS NOT NULL AND to_regclass(%s) = to_regclass(%s)",
            (match.group(1), match.group(1), sequence_ref),
        )
        return bool(rows and rows[0][0])

    def _apply_catalog_grant(self, logical_name: str) -> None:
        """Grant a hidden-schema object the exact access the catalog allows.

        Schemas outside the discoverable set carry no default privileges, so an
        allowlisted ops relation or the internal helper has to be granted when it
        is created — the role sweep runs at connection time, before ensure_*
        creates anything.
        """
        obj = CATALOG.object(logical_name)
        if obj.query_access not in {"app_only", "execute_only"}:
            return
        role = _identifier(self._query_role)
        rel = canonical_relation(logical_name).with_namespace(self._schema)
        qualified = f"{_identifier(rel.schema)}.{_identifier(rel.name)}"
        self._raw_command(f"GRANT USAGE ON SCHEMA {_identifier(rel.schema)} TO {role}")
        if obj.kind == "function":
            self._raw_command(f"GRANT EXECUTE ON FUNCTION {qualified}(text, integer) TO {role}")
        else:
            self._raw_command(f"GRANT SELECT ON {qualified} TO {role}")

    def _common_table_columns(
        self,
        *,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
    ) -> list[str]:
        rows = self._query(
            """
            SELECT source.column_name
            FROM information_schema.columns AS source
            INNER JOIN information_schema.columns AS target
              ON target.column_name = source.column_name
             AND target.table_schema = %s
             AND target.table_name = %s
            WHERE source.table_schema = %s
              AND source.table_name = %s
            ORDER BY source.ordinal_position
            """,
            (target_schema, target_table, source_schema, source_table),
        )
        return [row[0] for row in rows]

    def _physical_table_exists(self, *, schema: str, table: str) -> bool:
        rows = self._query(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            LIMIT 1
            """,
            (schema, table),
        )
        return bool(rows)

    def _ensure_indexes(self, tables: Sequence[str]) -> None:
        table_names = set(tables)
        for index in POSTGRES_INDEXES:
            if index.table not in table_names or index.name in self._ensured_index_names:
                continue
            try:
                if self._index_exists(index.name):
                    if not self._index_definition_drifted(index):
                        self._ensured_index_names.add(index.name)
                        continue
                    # Rebuild atomically: DROP and CREATE go to the server as one
                    # command string, so concurrent readers block on the lock
                    # instead of meeting a window where the pinned index name
                    # does not exist.
                    self._command(
                        "DROP INDEX "
                        + _identifier(index.name)
                        + "; "
                        + self._expanded_index_sql(index)
                        + "; "
                        + self._index_fingerprint_comment_sql(index)
                    )
                    self._ensured_index_names.add(index.name)
                    continue
                self._drop_invalid_index(index.name)
                if index.requires_pg_trgm and not self._pg_trgm_ensured:
                    self._command("CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public")
                    self._pg_trgm_ensured = True
                if index.requires_pg_textsearch and not self._pg_textsearch_ensured:
                    # Fails (and is harmlessly skipped, like missing-table indexes)
                    # on hosts whose Postgres lacks the pg_textsearch preload.
                    self._command("CREATE EXTENSION IF NOT EXISTS pg_textsearch WITH SCHEMA public")
                    self._pg_textsearch_ensured = True
                if index.requires_pgvector and not self._pgvector_ensured:
                    # Fails (and is harmlessly skipped) on hosts whose Postgres
                    # image predates the pgvector install.
                    self._command("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public")
                    self._pgvector_ensured = True
                index_sql = index.sql
                if self._schema.startswith("pdw_test_"):
                    # CONCURRENTLY waits for every older transaction in the
                    # shared database, including unrelated long-running
                    # read-only jobs. Throwaway test tables are private and
                    # empty, so ordinary index creation is both safe and keeps
                    # integration tests isolated from those global snapshots.
                    index_sql = index_sql.replace("CREATE INDEX CONCURRENTLY", "CREATE INDEX", 1)
                self._command(index_sql)
                if index.rebuild_on_definition_change:
                    self._command(self._index_fingerprint_comment_sql(index))
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

    @staticmethod
    def index_definition_fingerprint(index: IndexSpec) -> str:
        """A stable short hash of the index's declared SQL."""
        return hashlib.sha256(" ".join(index.sql.split()).encode("utf-8")).hexdigest()[:16]

    def _index_fingerprint_comment_sql(self, index: IndexSpec) -> str:
        marker = f"pdw-index-def:{self.index_definition_fingerprint(index)}"
        return f"COMMENT ON INDEX {_identifier(index.name)} IS '{marker}'"

    def _expanded_index_sql(self, index: IndexSpec) -> str:
        sql = index.sql.replace("CREATE INDEX CONCURRENTLY", "CREATE INDEX", 1)
        return sql.replace("IF NOT EXISTS ", "", 1)

    def _index_definition_drifted(self, index: IndexSpec) -> bool:
        """True when the live index was built from a different definition.

        Only indexes that opt in are checked; for everything else an existing
        index is accepted as-is, which is the historical behaviour.
        """
        if not index.rebuild_on_definition_change:
            return False
        expected = f"pdw-index-def:{self.index_definition_fingerprint(index)}"
        rows = self._query(
            "SELECT obj_description(c.oid, 'pg_class') FROM pg_class c "
            "INNER JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relname = %s AND n.nspname = ANY(%s) AND c.relkind = 'i' LIMIT 1",
            (index.name, self.physical_schema_names(include_hidden=True)),
        )
        if not rows:
            return False
        return (rows[0][0] or "") != expected

    def _drop_invalid_index(self, index_name: str) -> None:
        """Clear a failed CREATE INDEX CONCURRENTLY leftover so it can be rebuilt.

        An interrupted concurrent build leaves an index that is neither valid
        nor usable, and it occupies the name: every later ensure_* sees "not
        valid" from _index_exists, tries to create it, and gets "already
        exists" — which the caller swallows. The index is dead weight forever
        and its table silently loses that access path. Production carried five
        of these across the agent-session event tables.
        """
        invalid = self._query(
            """
            SELECT n.nspname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_index i ON i.indexrelid = c.oid
            WHERE n.nspname = ANY(%s)
              AND c.relname = %s
              AND c.relkind = 'i'
              AND NOT (i.indisvalid AND i.indisready)
            LIMIT 1
            """,
            (self.physical_schema_names(include_hidden=True), index_name),
        )
        if not invalid:
            return
        schema = _identifier(str(invalid[0][0]))
        logger.warning("dropping invalid index %s.%s so it can be rebuilt", schema, index_name)
        self._raw_command(f"DROP INDEX CONCURRENTLY IF EXISTS {schema}.{_identifier(index_name)}")

    def _index_exists(self, index_name: str) -> bool:
        rows = self._query(
            """
            SELECT 1
            FROM pg_class AS c
            INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
            INNER JOIN pg_index AS i ON i.indexrelid = c.oid
            WHERE n.nspname = ANY(%s)
              AND c.relname = %s
              AND c.relkind = 'i'
              AND i.indisvalid
              AND i.indisready
            LIMIT 1
            """,
            (self.physical_schema_names(include_hidden=True), index_name),
        )
        return bool(rows)

    def load_sync_state(self) -> dict[str, SyncState]:
        rows = self._query(
            """
            SELECT account, last_history_id, last_sync_type, status, error, updated_at
            FROM @gmail_sync_state
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
                  FROM @gmail_attachment_backfill_state state
                  WHERE state.account = gm.account
                    AND state.message_id = gm.message_id
                    AND state.status = 'ok'
              )"""
        params: list[Any] = [account]
        pending_clause = ai_pending_clause
        if include_storage_pending and storage_max_bytes > 0:
            pending_clause = f"""({ai_pending_clause}
              OR EXISTS (
                  SELECT 1
                  FROM @gmail_attachments pending
                  WHERE pending.account = gm.account
                    AND pending.message_id = gm.message_id
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
            FROM @gmail_messages AS gm
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
            FROM @file_attachment_enrichments
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
            FROM @google_drive_file_texts
            WHERE account = %s AND text_extraction_status = 'ok'
            """,
            (account,),
        )
        return {str(row[0]): (row[1], str(row[2])) for row in rows}

    def load_google_drive_text_modified_times(self, account: str) -> dict[str, datetime]:
        return {file_id: state[0] for file_id, state in self.load_google_drive_text_state(account).items()}

    def load_google_drive_docx_backfill_files(
        self, account: str, *, limit: int
    ) -> list[dict[str, Any]]:
        rows = self._query(
            """
            SELECT f.raw_metadata_json
            FROM @google_drive_files AS f
            JOIN @google_drive_file_texts AS legacy
              ON legacy.account = f.account AND legacy.file_id = f.file_id
            WHERE f.account = %s
              AND f.trashed = 0
              AND f.is_excluded = 0
              AND legacy.extractor = 'none'
              AND legacy.text_extraction_status = 'unsupported'
              AND (
                f.mime_type = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
                OR right(lower(f.name), 5) = '.docx'
              )
              AND NOT EXISTS (
                SELECT 1
                FROM @google_drive_file_texts AS attempt
                WHERE attempt.account = f.account
                  AND attempt.file_id = f.file_id
                  AND attempt.extractor = 'docx'
                  AND (
                    attempt.text_extraction_status IN ('ok', 'empty')
                    OR attempt.content_sha256 != ''
                  )
              )
            ORDER BY f.modified_time DESC, f.file_id
            LIMIT %s
            """,
            (account, limit),
        )
        files: list[dict[str, Any]] = []
        for (raw_metadata,) in rows:
            parsed: object = raw_metadata
            if isinstance(parsed, str):
                try:
                    parsed = json.loads(parsed)
                except json.JSONDecodeError:
                    continue
            if isinstance(parsed, Mapping):
                files.append(dict(parsed))
        return files

    def mark_google_drive_files_trashed(
        self, *, account: str, file_ids: Sequence[str], sync_version: int
    ) -> None:
        if not file_ids:
            return
        self._command(
            """
            UPDATE @google_drive_files
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
            FROM @google_drive_sync_state
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

    def insert_whoop_profiles(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("whoop_profiles", rows, WHOOP_PROFILE_COLUMNS)

    def insert_whoop_body_measurements(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("whoop_body_measurements", rows, WHOOP_BODY_MEASUREMENT_COLUMNS)

    def insert_whoop_cycles(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("whoop_cycles", rows, WHOOP_CYCLE_COLUMNS)

    def insert_whoop_recoveries(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("whoop_recoveries", rows, WHOOP_RECOVERY_COLUMNS)

    def insert_whoop_sleeps(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("whoop_sleeps", rows, WHOOP_SLEEP_COLUMNS)

    def insert_whoop_workouts(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("whoop_workouts", rows, WHOOP_WORKOUT_COLUMNS)

    def load_whoop_sync_state(self) -> dict[tuple[str, str], dict[str, Any]]:
        columns = WHOOP_SYNC_STATE_COLUMNS
        rows = self._query(f"SELECT {', '.join(_identifier(column) for column in columns)} FROM @whoop_sync_state")
        return {(str(row[0]), str(row[1])): dict(zip(columns, row, strict=True)) for row in rows}

    def load_whoop_oauth_token(self, *, account: str) -> str:
        rows = self._query("SELECT token_json FROM @whoop_oauth_tokens WHERE account = %s", (account,))
        return str(rows[0][0]) if rows else ""

    def load_or_bootstrap_whoop_oauth_token(
        self,
        *,
        account: str,
        bootstrap_token_json: str,
        updated_at: datetime,
    ) -> str:
        """Return the database authority, installing the env bootstrap once.

        The bootstrap is considered only when the account has no private row.
        It can never replace an existing row, even after a refresh failure.
        """

        relation = self.sql_relation("whoop_oauth_tokens")
        connection = psycopg2.connect(self._database_url)
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(%s)",
                    (WHOOP_TOKEN_AUTHORITY_LOCK_ID,),
                )
                cursor.execute(
                    f"SELECT token_json FROM {relation} WHERE account = %s FOR UPDATE",
                    (account,),
                )
                row = cursor.fetchone()
                if row is not None:
                    connection.commit()
                    return str(row[0])
                if not bootstrap_token_json:
                    raise RuntimeError(
                        "WHOOP has no installed OAuth credential; run "
                        "`uv run personal-data-warehouse-whoop-auth --install`"
                    )
                installed = _canonical_whoop_token_json(bootstrap_token_json)
                cursor.execute(
                    f"INSERT INTO {relation} (account, token_json, updated_at) "
                    "VALUES (%s, %s, %s)",
                    (account, installed, updated_at),
                )
            connection.commit()
            return installed
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def replace_whoop_oauth_token(
        self,
        *,
        account: str,
        token_json: str,
        updated_at: datetime,
    ) -> None:
        """Explicitly install a reauthorized token under the authority lock."""

        installed = _canonical_whoop_token_json(token_json)
        relation = self.sql_relation("whoop_oauth_tokens")
        connection = psycopg2.connect(self._database_url)
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(%s)",
                    (WHOOP_TOKEN_AUTHORITY_LOCK_ID,),
                )
                cursor.execute(
                    f"INSERT INTO {relation} (account, token_json, updated_at) "
                    "VALUES (%s, %s, %s) "
                    "ON CONFLICT (account) DO UPDATE SET "
                    "token_json = EXCLUDED.token_json, updated_at = EXCLUDED.updated_at",
                    (account, installed, updated_at),
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def rotate_whoop_oauth_token(
        self,
        *,
        account: str,
        expected_token_json: str,
        rotate: Callable[[str], str],
        updated_at: datetime,
    ) -> str:
        """Serialize a single-use OAuth refresh against the authoritative row.

        WHOOP invalidates a refresh token as soon as it returns its replacement.
        The provider call therefore runs while a row lock is held. A racer that
        arrived with the old token waits, observes the winner, and returns that
        token without calling WHOOP again. A failed or incomplete refresh rolls
        the transaction back and leaves the last known token untouched.
        """

        expected_canonical = _canonical_whoop_token_json(expected_token_json)
        relation = self.sql_relation("whoop_oauth_tokens")
        connection = psycopg2.connect(self._database_url)
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(%s)",
                    (WHOOP_TOKEN_AUTHORITY_LOCK_ID,),
                )
                cursor.execute(
                    f"SELECT token_json FROM {relation} WHERE account = %s FOR UPDATE",
                    (account,),
                )
                row = cursor.fetchone()
                if row is None:
                    raise RuntimeError("WHOOP OAuth token row disappeared before refresh")
                current_token_json = str(row[0])
                if _canonical_whoop_token_json(current_token_json) != expected_canonical:
                    connection.commit()
                    return current_token_json

                rotated_token_json = _canonical_whoop_token_json(rotate(current_token_json))
                cursor.execute(
                    f"UPDATE {relation} SET token_json = %s, updated_at = %s WHERE account = %s",
                    (rotated_token_json, updated_at, account),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError("WHOOP OAuth token row disappeared during refresh")
            connection.commit()
            return rotated_token_json
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def insert_whoop_sync_state(
        self,
        *,
        account: str,
        collection: str,
        watermark_updated_at: datetime,
        last_sync_type: str,
        status: str,
        error: str,
        updated_at: datetime,
        credential_sha256: str = "",
    ) -> None:
        self._insert(
            "whoop_sync_state",
            [
                (
                    account,
                    collection,
                    watermark_updated_at,
                    last_sync_type,
                    status,
                    error,
                    updated_at,
                    int(_ensure_utc(updated_at).timestamp() * 1_000_000),
                    credential_sha256,
                )
            ],
            WHOOP_SYNC_STATE_COLUMNS,
        )

    # -- WHOOP private (app) API ------------------------------------------

    def insert_whoop_private_cycles(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("whoop_private_cycles", rows, WHOOP_PRIVATE_CYCLE_COLUMNS)

    def insert_whoop_private_sleeps(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("whoop_private_sleeps", rows, WHOOP_PRIVATE_SLEEP_COLUMNS)

    def insert_whoop_private_recoveries(self, rows: list[dict[str, Any]]) -> None:
        """Upsert recoveries.

        Callers must populate BOTH ``hrv_rmssd_seconds`` (the private API's own
        unit) and ``hrv_rmssd_milli`` (the unit base_whoop.recoveries and every
        derived HRV number use). ``schema.whoop_private_hrv_rmssd_milli`` is the
        one sanctioned conversion.
        """
        self._insert_rows("whoop_private_recoveries", rows, WHOOP_PRIVATE_RECOVERY_COLUMNS)

    def insert_whoop_private_workouts(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("whoop_private_workouts", rows, WHOOP_PRIVATE_WORKOUT_COLUMNS)

    def insert_whoop_private_sleep_events(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("whoop_private_sleep_events", rows, WHOOP_PRIVATE_SLEEP_EVENT_COLUMNS)

    def insert_whoop_private_heart_rate_samples(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows(
            "whoop_private_heart_rate_samples",
            rows,
            WHOOP_PRIVATE_HEART_RATE_SAMPLE_COLUMNS,
        )

    def delete_whoop_private_heart_rate_samples(
        self, *, account: str, start: datetime, end: datetime, keep_step_seconds: int
    ) -> None:
        """Clear any other grain over a window the current grain just covered.

        The series is one grid. A sample at a retired step whose timestamp does
        not land on the current grid survives the upsert -- window boundaries
        drift by milliseconds -- and then avg(heart_rate) over any range weights
        that instant twice. Called AFTER the window is written, so the series is
        never briefly empty.
        """
        self._command(
            "DELETE FROM @whoop_private_heart_rate_samples "
            "WHERE account = %s AND sample_at >= %s AND sample_at < %s AND step_seconds <> %s",
            (account, start, end, int(keep_step_seconds)),
        )

    def insert_whoop_private_journal_entries(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("whoop_private_journal_entries", rows, WHOOP_PRIVATE_JOURNAL_ENTRY_COLUMNS)

    def insert_whoop_private_sports(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("whoop_private_sports", rows, WHOOP_PRIVATE_SPORT_COLUMNS)

    def insert_whoop_private_documents(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("whoop_private_documents", rows, WHOOP_PRIVATE_DOCUMENT_COLUMNS)

    def whoop_private_document_keys(self, *, account: str, kinds: Sequence[str]) -> set[tuple[str, str]]:
        """Which (kind, doc_key) documents are already stored.

        This IS the document backfill's cursor -- there is no watermark to
        repair, so an interrupted backfill resumes by itself. Reads only the
        two key columns, which the primary key covers.
        """
        if not kinds:
            return set()
        rows = self._query(
            "SELECT kind, doc_key FROM @whoop_private_documents "
            "WHERE account = %s AND kind = ANY(%s)",
            (account, list(kinds)),
        )
        return {(str(row[0]), str(row[1])) for row in rows}

    def whoop_private_workouts_without_cardio_details(
        self, *, account: str, limit: int
    ) -> list[tuple[str, datetime]]:
        """Stored workouts with no ``cardio_details`` document, newest first.

        The documents table is the cursor here too: a workout that landed
        after it had fallen out of the run's newest-N window -- backdated,
        edited, or restated by a later cycles pull -- would otherwise never be
        asked for its GPS route. Bounded so the sweep shares the per-run
        request budget rather than owning it.
        """
        if limit <= 0:
            return []
        rows = self._query(
            "SELECT w.activity_id, w.start_at FROM @whoop_private_workouts w "
            "LEFT JOIN @whoop_private_documents d "
            "ON d.account = w.account AND d.kind = 'cardio_details' AND d.doc_key = w.activity_id "
            "WHERE w.account = %s AND d.doc_key IS NULL AND w.start_at > %s "
            "ORDER BY w.start_at DESC LIMIT %s",
            (account, datetime.fromtimestamp(0, tz=UTC), limit),
        )
        return [(str(row[0]), row[1].astimezone(UTC)) for row in rows]

    def whoop_private_earliest_cycle_day(self, *, account: str) -> date | None:
        """The account's first cycle day -- the floor for the document backfill.

        A member has no deep dives before they had a WHOOP, so this stops the
        walk instead of `full_sync_start` sending it to 2015. ``None`` means no
        cycle has landed yet, which is a reason not to backfill rather than a
        reason to guess.
        """
        rows = self._query(
            "SELECT min(start_at) FROM @whoop_private_cycles WHERE account = %s",
            (account,),
        )
        if not rows or rows[0][0] is None:
            return None
        earliest = rows[0][0]
        # The warehouse-wide absence sentinel, not NULL. A cycles table holding
        # only the epoch has no usable floor.
        if earliest <= datetime.fromtimestamp(0, tz=UTC):
            return None
        return earliest.astimezone(UTC).date()

    def prune_whoop_private_sync_state(
        self, *, account: str, keep_collections: Sequence[str]
    ) -> None:
        """Drop state rows for collections this sync no longer has.

        The read surfaces judge a pipeline from every row in its sync-state
        table, so a retired collection's last status outlives it and nothing can
        clear it -- a row left saying `action_required` would read as an open
        incident forever. Retiring a collection has to remove its state the same
        way retiring a table removes the table.
        """
        self._command(
            "DELETE FROM @whoop_private_sync_state "
            "WHERE account = %s AND NOT (collection = ANY(%s))",
            (account, list(keep_collections)),
        )

    def load_whoop_private_sync_state(self) -> dict[tuple[str, str], dict[str, Any]]:
        columns = WHOOP_PRIVATE_SYNC_STATE_COLUMNS
        rows = self._query(
            f"SELECT {', '.join(_identifier(column) for column in columns)} FROM @whoop_private_sync_state"
        )
        return {(str(row[0]), str(row[1])): dict(zip(columns, row, strict=True)) for row in rows}

    def insert_whoop_private_sync_state(
        self,
        *,
        account: str,
        collection: str,
        watermark_updated_at: datetime,
        last_sync_type: str,
        status: str,
        error: str,
        updated_at: datetime,
        credential_sha256: str = "",
        collection_signature: str = "",
    ) -> None:
        self._insert(
            "whoop_private_sync_state",
            [
                (
                    account,
                    collection,
                    watermark_updated_at,
                    last_sync_type,
                    status,
                    error,
                    updated_at,
                    int(_ensure_utc(updated_at).timestamp() * 1_000_000),
                    credential_sha256,
                    collection_signature,
                )
            ],
            WHOOP_PRIVATE_SYNC_STATE_COLUMNS,
        )

    def load_slack_session(self, *, account: str, session_key: str = "default") -> dict[str, Any]:
        """The stored Slack client session, or an empty dict when none is published.

        Empty rather than None so a caller cannot mistake "no credential yet" for
        a row with falsy fields; the sync checks for both halves and falls back
        to polling when either is missing.
        """
        columns = SLACK_SESSION_COLUMNS
        rows = self._query(
            f"SELECT {', '.join(_identifier(column) for column in columns)} "
            "FROM @slack_sessions WHERE account = %s AND session_key = %s",
            (account, session_key),
        )
        if not rows:
            return {}
        return dict(zip(columns, rows[0], strict=True))

    def load_slack_conversation_cursors(self, *, account: str, team_id: str) -> dict[str, float]:
        """Per-conversation high-water marks, for diffing against client.counts.

        Read from derived_slack.conversation_stats, not from a max() over
        base_slack.messages. Measured on production the direct aggregate is a
        parallel index-only scan of 45M rows at **34.9s**, against **115ms** for
        the 19k-row stats table -- and this runs on every freshness pass, so the
        difference is the whole benefit.

        Staleness here is safe in the only direction that matters: a stats row
        behind the messages table yields a low cursor, so the conversation is
        re-fetched. A conversation with no row at all reads as 0 and is fetched.
        Both err toward doing work, never toward missing a message.
        """
        rows = self._query(
            "SELECT conversation_id, EXTRACT(EPOCH FROM latest_message_at) "
            "FROM @slack_conversation_stats WHERE account = %s AND team_id = %s",
            (account, team_id),
        )
        cursors: dict[str, float] = {}
        for conversation_id, latest in rows:
            try:
                cursors[str(conversation_id)] = float(latest)
            except (TypeError, ValueError):
                continue
        return cursors

    def load_whoop_private_session(self, *, account: str, session_key: str = "default") -> dict[str, Any]:
        """The stored browser session, or an empty dict when none is published.

        Empty rather than None so a caller cannot accidentally treat "no
        credential yet" as a row with falsy fields; the sync path checks for a
        refresh_token and reports action_required.
        """

        columns = WHOOP_PRIVATE_SESSION_COLUMNS
        rows = self._query(
            f"SELECT {', '.join(_identifier(column) for column in columns)} "
            "FROM @whoop_private_sessions WHERE account = %s AND session_key = %s",
            (account, session_key),
        )
        if not rows:
            return {}
        return dict(zip(columns, rows[0], strict=True))

    def replace_whoop_private_session(
        self,
        *,
        account: str,
        access_token: str,
        refresh_token: str,
        access_expires_at: datetime,
        refresh_expires_at: datetime,
        published_at: datetime,
        updated_at: datetime,
        session_key: str = "default",
        source_browser: str = "",
        status: str = "ok",
        error: str = "",
    ) -> None:
        """Install a freshly captured browser session under the authority lock.

        This is the "a human logged in again" path, and it always wins: it
        clears any action_required state, because the point of publishing is to
        repair a rejected credential. It takes the same lock a rotation takes,
        so a publish cannot interleave with a rotation that is mid-flight and
        silently lose whichever committed second.

        The app writes this same row over its own HMAC-signed endpoint
        (app/internal/whoopsession); this method is the Python-side twin used by
        tests and by any in-process publisher.
        """

        if not refresh_token:
            raise ValueError("WHOOP private session needs a refresh token")
        relation = self.sql_relation("whoop_private_sessions")
        connection = psycopg2.connect(self._database_url)
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(%s)",
                    (WHOOP_PRIVATE_SESSION_AUTHORITY_LOCK_ID,),
                )
                cursor.execute(
                    f"INSERT INTO {relation} (account, session_key, access_token, refresh_token, "
                    "access_expires_at, refresh_expires_at, refresh_token_sha256, source_browser, "
                    "published_at, updated_at, sync_version, status, error) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (account, session_key) DO UPDATE SET "
                    "access_token = EXCLUDED.access_token, "
                    "refresh_token = EXCLUDED.refresh_token, "
                    "access_expires_at = EXCLUDED.access_expires_at, "
                    "refresh_expires_at = EXCLUDED.refresh_expires_at, "
                    "refresh_token_sha256 = EXCLUDED.refresh_token_sha256, "
                    "source_browser = EXCLUDED.source_browser, "
                    "published_at = EXCLUDED.published_at, "
                    "updated_at = EXCLUDED.updated_at, "
                    "sync_version = EXCLUDED.sync_version, "
                    "status = EXCLUDED.status, "
                    "error = EXCLUDED.error",
                    (
                        account,
                        session_key,
                        access_token,
                        refresh_token,
                        access_expires_at,
                        refresh_expires_at,
                        _sha256_hex(refresh_token),
                        source_browser,
                        published_at,
                        updated_at,
                        int(_ensure_utc(updated_at).timestamp() * 1_000_000),
                        status,
                        error,
                    ),
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def rotate_whoop_private_session(
        self,
        *,
        account: str,
        expected_refresh_token: str,
        access_token: str,
        refresh_token: str,
        access_expires_at: datetime,
        refresh_expires_at: datetime,
        updated_at: datetime,
        session_key: str = "default",
    ) -> dict[str, Any]:
        """Persist a rotation as a compare-and-swap under the authority lock.

        WHOOP's private auth-service returns a NEW refresh token on every
        refresh, so the stored credential is a moving target: two callers that
        both refreshed from the same starting token produce two different live
        sessions, and a last-writer-wins UPDATE installs whichever committed
        second -- which may be the one whose token the other caller has already
        superseded. The public WHOOP credential produced three production
        incidents from exactly this shape (docs/whoop-oauth-operations.md), and
        rotate_whoop_oauth_token is the discipline being mirrored: advisory
        lock, row lock, compare, then write.

        The comparison is on the refresh token the caller *started from*. If it
        no longer matches, someone else already rotated; this call makes no
        change and returns the winning row, so the loser adopts the live
        credential instead of overwriting it with a superseded one.

        Unlike the public credential, the provider call happens before this
        method rather than inside the lock. That is deliberate and safe here:
        the private API's old refresh token keeps working immediately after a
        refresh (verified 2026-08-23), so the danger is a lost update, not a
        consumed token -- and a lost update is exactly what the compare
        prevents. Returns the row that is live after the call.
        """

        if not expected_refresh_token:
            raise ValueError("WHOOP private session rotation needs the expected refresh token")
        if not refresh_token:
            raise ValueError("WHOOP private refresh returned no refresh token")
        columns = WHOOP_PRIVATE_SESSION_COLUMNS
        select_list = ", ".join(_identifier(column) for column in columns)
        relation = self.sql_relation("whoop_private_sessions")
        connection = psycopg2.connect(self._database_url)
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(%s)",
                    (WHOOP_PRIVATE_SESSION_AUTHORITY_LOCK_ID,),
                )
                cursor.execute(
                    f"SELECT {select_list} FROM {relation} "
                    "WHERE account = %s AND session_key = %s FOR UPDATE",
                    (account, session_key),
                )
                row = cursor.fetchone()
                if row is None:
                    raise RuntimeError(
                        "WHOOP private session row disappeared before rotation; "
                        "re-publish it with `pdw whoop publish-session`"
                    )
                current = dict(zip(columns, row, strict=True))
                if str(current["refresh_token"]) != expected_refresh_token:
                    # Someone else already rotated. Their token is the live one.
                    connection.commit()
                    return current

                rotated = dict(current)
                rotated.update(
                    {
                        "access_token": access_token,
                        "refresh_token": refresh_token,
                        "access_expires_at": access_expires_at,
                        "refresh_expires_at": refresh_expires_at,
                        "refresh_token_sha256": _sha256_hex(refresh_token),
                        "updated_at": updated_at,
                        "sync_version": int(_ensure_utc(updated_at).timestamp() * 1_000_000),
                        # A successful rotation is proof the credential works.
                        "status": "ok",
                        "error": "",
                    }
                )
                cursor.execute(
                    f"UPDATE {relation} SET access_token = %s, refresh_token = %s, "
                    "access_expires_at = %s, refresh_expires_at = %s, refresh_token_sha256 = %s, "
                    "updated_at = %s, sync_version = %s, status = %s, error = %s "
                    "WHERE account = %s AND session_key = %s",
                    (
                        rotated["access_token"],
                        rotated["refresh_token"],
                        rotated["access_expires_at"],
                        rotated["refresh_expires_at"],
                        rotated["refresh_token_sha256"],
                        rotated["updated_at"],
                        rotated["sync_version"],
                        rotated["status"],
                        rotated["error"],
                        account,
                        session_key,
                    ),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError("WHOOP private session row disappeared during rotation")
            connection.commit()
            return rotated
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

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
            FROM @calendar_events
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
            FROM @calendar_events
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
        rows = self._query(f"SELECT {', '.join(_identifier(column) for column in columns)} FROM @calendar_sync_state")
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

    def insert_apple_contact_cards(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("apple_contact_cards", rows, CONTACT_CARD_COLUMNS)

    def load_contact_sync_state(self) -> dict[tuple[str, str, str, str], dict[str, Any]]:
        columns = CONTACT_SYNC_STATE_COLUMNS
        rows = self._query(f"SELECT {', '.join(_identifier(column) for column in columns)} FROM @contact_sync_state")
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

    def upsert_plaid_item_token(
        self,
        *,
        account: str,
        item_id: str,
        access_token: str,
        institution_id: str = "",
        institution_name: str = "",
        linked_at: datetime,
    ) -> None:
        self._insert_rows(
            "plaid_item_tokens",
            [
                {
                    "account": account,
                    "item_id": item_id,
                    "access_token": access_token,
                    "institution_id": institution_id,
                    "institution_name": institution_name,
                    "linked_at": linked_at,
                    "updated_at": linked_at,
                    "sync_version": int(_ensure_utc(linked_at).timestamp() * 1_000_000),
                }
            ],
            PLAID_ITEM_TOKEN_COLUMNS,
        )

    def load_plaid_item_tokens(self) -> list[PlaidLinkedItem]:
        rows = self._query(
            """
            SELECT account, item_id, access_token, institution_id, institution_name
            FROM @plaid_item_tokens
            ORDER BY account, institution_name, item_id
            """
        )
        return [
            PlaidLinkedItem(
                account=str(row[0]),
                item_id=str(row[1]),
                access_token=str(row[2]),
                institution_id=str(row[3]),
                institution_name=str(row[4]),
            )
            for row in rows
        ]

    def load_plaid_sync_state(self) -> dict[tuple[str, str, str], dict[str, Any]]:
        columns = PLAID_SYNC_STATE_COLUMNS
        rows = self._query(f"SELECT {', '.join(_identifier(column) for column in columns)} FROM @plaid_sync_state")
        return {
            (str(row[0]), str(row[1]), str(row[2])): dict(zip(columns, row, strict=True))
            for row in rows
        }

    def insert_plaid_items(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("plaid_items", rows, PLAID_ITEM_COLUMNS)

    def insert_plaid_accounts(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("plaid_accounts", rows, PLAID_ACCOUNT_COLUMNS)

    def mark_missing_plaid_accounts_removed(
        self,
        *,
        account: str,
        item_id: str,
        active_account_ids: set[str],
        synced_at: datetime,
    ) -> int:
        params: list[Any] = [account, item_id]
        active_filter = ""
        if active_account_ids:
            active_filter = "AND NOT (account_id = ANY(%s))"
            params.append(sorted(active_account_ids))
        rows = self._query(
            f"""
            SELECT {", ".join(_identifier(column) for column in PLAID_ACCOUNT_COLUMNS)}
            FROM @plaid_accounts
            WHERE account = %s
              AND item_id = %s
              AND is_removed = 0
              {active_filter}
            """,
            tuple(params),
        )
        sync_version = int(_ensure_utc(synced_at).timestamp() * 1_000_000)
        tombstones: list[dict[str, Any]] = []
        for row in rows:
            tombstone = dict(zip(PLAID_ACCOUNT_COLUMNS, row, strict=True))
            tombstone["is_removed"] = 1
            tombstone["synced_at"] = synced_at
            tombstone["sync_version"] = sync_version
            tombstones.append(tombstone)
        self.insert_plaid_accounts(tombstones)
        return len(tombstones)

    def insert_plaid_transactions(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("plaid_transactions", rows, PLAID_TRANSACTION_COLUMNS)

    def mark_plaid_transactions_removed(
        self,
        *,
        account: str,
        item_id: str,
        transaction_ids: list[str],
        synced_at: datetime,
    ) -> int:
        if not transaction_ids:
            return 0
        rows = self._query(
            f"""
            SELECT {", ".join(_identifier(column) for column in PLAID_TRANSACTION_COLUMNS)}
            FROM @plaid_transactions
            WHERE account = %s
              AND item_id = %s
              AND transaction_id = ANY(%s)
              AND is_removed = 0
            """,
            (account, item_id, transaction_ids),
        )
        tombstones: list[dict[str, Any]] = []
        sync_version = int(_ensure_utc(synced_at).timestamp() * 1_000_000)
        for row in rows:
            tombstone = dict(zip(PLAID_TRANSACTION_COLUMNS, row, strict=True))
            tombstone["is_removed"] = 1
            tombstone["synced_at"] = synced_at
            tombstone["sync_version"] = sync_version
            tombstones.append(tombstone)
        self.insert_plaid_transactions(tombstones)
        return len(tombstones)

    def insert_plaid_investment_securities(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("plaid_investment_securities", rows, PLAID_INVESTMENT_SECURITY_COLUMNS)

    def insert_plaid_investment_holdings(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("plaid_investment_holdings", rows, PLAID_INVESTMENT_HOLDING_COLUMNS)

    def delete_missing_plaid_investment_holdings(
        self,
        *,
        account: str,
        item_id: str,
        active_holding_keys: set[tuple[str, str]],
    ) -> int:
        return self._delete_missing_plaid_snapshot_rows(
            table="plaid_investment_holdings",
            account=account,
            item_id=item_id,
            key_columns=("account_id", "security_id"),
            active_keys=active_holding_keys,
        )

    def insert_plaid_investment_transactions(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("plaid_investment_transactions", rows, PLAID_INVESTMENT_TRANSACTION_COLUMNS)

    def insert_plaid_liabilities(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("plaid_liabilities", rows, PLAID_LIABILITY_COLUMNS)

    def delete_missing_plaid_liabilities(
        self,
        *,
        account: str,
        item_id: str,
        active_liability_keys: set[tuple[str, str]],
    ) -> int:
        return self._delete_missing_plaid_snapshot_rows(
            table="plaid_liabilities",
            account=account,
            item_id=item_id,
            key_columns=("account_id", "liability_type"),
            active_keys=active_liability_keys,
        )

    def _delete_missing_plaid_snapshot_rows(
        self,
        *,
        table: str,
        account: str,
        item_id: str,
        key_columns: tuple[str, str],
        active_keys: set[tuple[str, str]],
    ) -> int:
        if table not in {"plaid_investment_holdings", "plaid_liabilities"}:
            raise ValueError(f"unsupported Plaid snapshot table: {table}")
        first_key, second_key = key_columns
        rows = self._query(
            f"""
            SELECT {_identifier(first_key)}, {_identifier(second_key)}
            FROM {self.sql_relation(table)}
            WHERE account = %s AND item_id = %s
            """,
            (account, item_id),
        )
        stale_keys = [(str(row[0]), str(row[1])) for row in rows if (str(row[0]), str(row[1])) not in active_keys]
        if not stale_keys:
            return 0
        predicates = " OR ".join(
            f"({_identifier(first_key)} = %s AND {_identifier(second_key)} = %s)"
            for _ in stale_keys
        )
        params: list[Any] = [account, item_id]
        for first, second in stale_keys:
            params.extend((first, second))
        self._command(
            f"""
            DELETE FROM {self.sql_relation(table)}
            WHERE account = %s AND item_id = %s
              AND ({predicates})
            """,
            tuple(params),
        )
        return len(stale_keys)

    def load_plaid_item_accounts(self, *, account: str, item_id: str) -> list[dict[str, Any]]:
        """The accounts one linked Item reports, for operator-facing output."""
        return self._query_dicts(
            """
            SELECT account_id, name, mask, type, subtype, current_balance, is_removed
            FROM @plaid_accounts
            WHERE account = %s AND item_id = %s
            ORDER BY mask, account_id
            """,
            (account, item_id),
        )

    def count_plaid_item_rows(self, *, account: str, item_id: str) -> dict[str, int]:
        """Row counts per table for one linked Item — what unlink would delete."""
        return {
            table: int(
                self._query(
                    f"SELECT count(*) FROM {self.sql_relation(table)} WHERE account = %s AND item_id = %s",
                    (account, item_id),
                )[0][0]
            )
            for table in PLAID_ITEM_SCOPED_TABLES
        }

    def delete_plaid_item(self, *, account: str, item_id: str) -> dict[str, int]:
        """Delete every row belonging to one linked Plaid Item, atomically.

        Retiring an Item is a deliberate operator action (`pdw ingest plaid
        unlink`), not a sync outcome: re-linking an institution can mint a NEW
        item_id instead of repairing the old one, and both Items then keep
        reporting the same real accounts — double-counting balances in
        marts_finance.net_worth and duplicating every transaction in the
        overlap window. Tombstones would not help; those rows have to stop
        existing. plaid_investment_securities is deliberately absent: it is
        keyed by account, not item, and is shared across Items.
        """
        deletes = ", ".join(
            f"{_identifier('d_' + table)} AS ("
            f"DELETE FROM {self.sql_relation(table)} WHERE account = %s AND item_id = %s RETURNING 1)"
            for table in PLAID_ITEM_SCOPED_TABLES
        )
        selects = ", ".join(
            f"(SELECT count(*) FROM {_identifier('d_' + table)})" for table in PLAID_ITEM_SCOPED_TABLES
        )
        params: list[Any] = []
        for _ in PLAID_ITEM_SCOPED_TABLES:
            params.extend((account, item_id))
        row = self._query(f"WITH {deletes} SELECT {selects}", tuple(params))[0]
        return dict(zip(PLAID_ITEM_SCOPED_TABLES, (int(value) for value in row), strict=True))

    def insert_plaid_sync_state(
        self,
        *,
        account: str,
        item_id: str,
        product: str,
        cursor: str = "",
        status: str,
        error: str = "",
        last_synced_at: datetime,
        updated_at: datetime,
    ) -> None:
        self._insert_rows(
            "plaid_sync_state",
            [
                {
                    "account": account,
                    "item_id": item_id,
                    "product": product,
                    "cursor": cursor,
                    "status": status,
                    "error": error,
                    "last_synced_at": last_synced_at,
                    "updated_at": updated_at,
                    "sync_version": int(_ensure_utc(updated_at).timestamp() * 1_000_000),
                }
            ],
            PLAID_SYNC_STATE_COLUMNS,
        )

    def insert_finance_accounts(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("finance_accounts", rows, FINANCE_ACCOUNT_COLUMNS)

    def insert_finance_account_links(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("finance_account_links", rows, FINANCE_ACCOUNT_LINK_COLUMNS)

    def insert_finance_observations(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("finance_observations", rows, FINANCE_OBSERVATION_COLUMNS)

    def insert_finance_transactions(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("finance_transactions", rows, FINANCE_TRANSACTION_COLUMNS)

    def insert_finance_transaction_links(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("finance_transaction_links", rows, FINANCE_TRANSACTION_LINK_COLUMNS)

    def insert_finance_security_transactions(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows(
            "finance_security_transactions", rows, FINANCE_SECURITY_TRANSACTION_COLUMNS
        )

    def insert_finance_security_transaction_links(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows(
            "finance_security_transaction_links", rows, FINANCE_SECURITY_TRANSACTION_LINK_COLUMNS
        )

    def insert_finance_tax_lots(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("finance_tax_lots", rows, FINANCE_TAX_LOT_COLUMNS)

    def replace_finance_tax_lots(self, rows: list[dict[str, Any]]) -> int:
        """Lots are a pure reduction of the trade ledger, not accumulated state.

        A rebuild must therefore be able to *shrink* the table — a corrected
        extraction can delete a trade, and a stale lot left behind would be a
        confident fiction. Replaced wholesale inside one transaction.
        """
        with self._connection:
            self._command("DELETE FROM @finance_tax_lots")
            if rows:
                self._insert_rows("finance_tax_lots", rows, FINANCE_TAX_LOT_COLUMNS)
        return len(rows)

    def delete_missing_finance_security_transactions(self, keep_ids: list[str]) -> int:
        """Drop unified trades whose source rows no longer exist (same
        reconciliation the cash ledger does — derived state follows its
        sources, and raw rows are never touched)."""
        if keep_ids:
            removed = self._query(
                """
                WITH removed_links AS (
                    DELETE FROM @finance_security_transaction_links
                    WHERE transaction_id <> ALL(%s)
                    RETURNING 1
                ), removed_trades AS (
                    DELETE FROM @finance_security_transactions
                    WHERE transaction_id <> ALL(%s)
                    RETURNING 1
                )
                SELECT (SELECT count(*) FROM removed_trades)
                """,
                (keep_ids, keep_ids),
            )
        else:
            removed = self._query(
                """
                WITH removed_links AS (
                    DELETE FROM @finance_security_transaction_links RETURNING 1
                ), removed_trades AS (
                    DELETE FROM @finance_security_transactions RETURNING 1
                )
                SELECT (SELECT count(*) FROM removed_trades)
                """
            )
        return int(removed[0][0]) if removed else 0

    def delete_missing_document_observations(self, keep_keys: list[str]) -> int:
        """Drop statement-derived observations the current corpus no longer says.

        Manual observations are wholly derived from the documents present now,
        so this reconciles them the way the ledger reconciles transactions.
        It matters because an observation's identity includes its account: when
        a document group re-resolves to a different ledger account, its old
        rows would otherwise stay behind as history the source no longer
        claims. Scoped to ``manual_finance`` on purpose — Plaid's daily balance
        rows ARE the balance history and are never rebuildable from a source.
        Keys are compared as ``account_id|as_of|kind`` strings.
        """
        removed = self._query(
            """
            WITH removed AS (
                DELETE FROM @finance_observations
                WHERE source = 'manual_finance'
                  AND (account_id || '|' || as_of::text || '|' || kind) <> ALL(%s::text[])
                RETURNING 1
            )
            SELECT count(*) FROM removed
            """,
            (keep_keys,),
        )
        return int(removed[0][0]) if removed else 0

    def delete_missing_document_account_links(self, keep_keys: list[str]) -> int:
        """Drop manual-document account links no document group claims any more.

        A link is a derived decision, and 7adf12e made document links
        re-resolve every run so a decision made from thinner evidence cannot
        freeze. That is only half of it: a link whose GROUP no longer exists --
        because its documents were deleted, moved into a folder, or refused as
        unidentifiable -- is never revisited by re-resolution, because nothing
        iterates it. It then keeps its ledger account alive past
        ``prune_unlinked_finance_accounts``, which only reaches accounts with
        zero links. An ``<institution>|`` catch-all is the case: withholding
        its documents removes their observations, but without this the account
        and its link would sit in ``derived_finance.accounts`` forever.

        Scoped to ``manual_finance``; Plaid links are keyed on live source rows
        and reconciled by their own resolver. Keys are ``account|source_account_key``.

        An EMPTY ``keep_keys`` means "no group survives", which is a real
        state (every document withheld) and deletes every manual link. The
        caller must therefore not confuse it with "the extraction asset has
        not run on this deployment yet" -- ``FinanceLedgerRunner.sync`` skips
        the call entirely when the corpus itself is empty.
        """
        removed = self._query(
            """
            WITH removed AS (
                DELETE FROM @finance_account_links
                WHERE source = 'manual_finance'
                  AND (account || '|' || source_account_key) <> ALL(%s::text[])
                RETURNING 1
            )
            SELECT count(*) FROM removed
            """,
            (keep_keys,),
        )
        return int(removed[0][0]) if removed else 0

    def insert_receipt_transaction_receipts(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows(
            "receipt_transaction_receipts",
            rows,
            RECEIPT_TRANSACTION_RECEIPT_COLUMNS,
        )

    def reconcile_finance_transactions(self, *, transaction_ids: list[str], link_keys: list[str]) -> int:
        """Hard-delete ledger transactions/links absent from the desired set.

        The ledger is derived state rebuilt from raw source rows each run
        (e.g. a Plaid pending row tombstones once its posted row arrives, and
        its ledger row must go with it). Raw rows are never touched.
        Link keys are compared as ``source|source_row_key`` strings.
        """
        removed = self._query(
            """
            WITH removed_links AS (
                DELETE FROM @finance_transaction_links
                WHERE (source || '|' || source_row_key) <> ALL(%s)
                RETURNING 1
            ),
            removed_transactions AS (
                DELETE FROM @finance_transactions
                WHERE transaction_id <> ALL(%s)
                RETURNING 1
            )
            SELECT (SELECT count(*) FROM removed_links) + (SELECT count(*) FROM removed_transactions)
            """,
            (link_keys, transaction_ids),
        )
        return int(removed[0][0]) if removed else 0

    def prune_unlinked_finance_accounts(self) -> int:
        """Delete ledger accounts no source links to, with their observations.

        Every account is founded by a link, and links are only ever added or
        re-pointed, so an account reaches zero links exactly once: when the
        source that founded it merged into an older account for the same real
        account (a Plaid re-link forks one). What is left is derived residue —
        keeping it double-counts the account in marts_finance.net_worth.
        Transactions are reconciled separately by the same run.
        """
        removed = self._query(
            """
            WITH unlinked AS (
                SELECT a.account_id
                FROM @finance_accounts AS a
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM @finance_account_links AS l
                    WHERE l.account_id = a.account_id
                )
            ),
            removed_observations AS (
                DELETE FROM @finance_observations
                WHERE account_id IN (SELECT account_id FROM unlinked)
                RETURNING 1
            ),
            removed_accounts AS (
                DELETE FROM @finance_accounts
                WHERE account_id IN (SELECT account_id FROM unlinked)
                RETURNING 1
            )
            SELECT (SELECT count(*) FROM removed_accounts)
            """
        )
        return int(removed[0][0]) if removed else 0

    def clear_uncorroborated_finance_account_masks(
        self, keep: set[tuple[str, str]], *, document_source: str
    ) -> int:
        """Blank the mask on document-founded accounts nothing vouches for.

        A mask is a DERIVED decision, exactly like an account link, so it has
        to be re-resolved rather than made once and frozen. It was written only
        when a group FOUNDED its account, so an account created before the
        corroboration rule existed kept a number no folder and no provider ever
        confirmed -- in production, a vehicle purchase order's dealer stock
        number and a payee's bank account, both still presented as the owner's
        account identity through `marts_finance.accounts.mask` after the guard
        that was supposed to stop exactly that had shipped.

        `keep` is every (account_id, mask) the current run corroborated.
        `document_source` is the caller's own link-source token, passed in
        rather than spelled here, because the two differ ("manual_finance",
        not "manual") and a literal that drifts would silently select nothing
        and report a clean zero forever.

        An account any non-document source also links is never touched: a
        provider-reported mask is authoritative by definition. This only ever
        CLEARS, so the failure direction is a missing mask, never somebody
        else's.
        """
        rows = self._query(
            """
            SELECT a.account_id, a.mask
            FROM @finance_accounts AS a
            WHERE a.mask <> ''
              AND EXISTS (
                  SELECT 1 FROM @finance_account_links AS l
                  WHERE l.account_id = a.account_id AND l.source = %s
              )
              AND NOT EXISTS (
                  SELECT 1 FROM @finance_account_links AS l
                  WHERE l.account_id = a.account_id AND l.source <> %s
              )
            """,
            (document_source, document_source),
        )
        stale = [
            str(account_id)
            for account_id, mask in rows
            if (str(account_id), str(mask)) not in keep
        ]
        if not stale:
            return 0
        self._command(
            "UPDATE @finance_accounts SET mask = '' WHERE account_id = ANY(%s)",
            (stale,),
        )
        return len(stale)

    def insert_manual_finance_documents(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("manual_finance_documents", rows, MANUAL_FINANCE_DOCUMENT_COLUMNS)

    def insert_manual_finance_extractions(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("manual_finance_extractions", rows, MANUAL_FINANCE_EXTRACTION_COLUMNS)

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
            FROM @contact_cards
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

    def insert_alice_voice_recordings(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("alice_voice_recordings", rows, ALICE_VOICE_RECORDING_COLUMNS)

    def insert_alice_voice_recording_artifacts(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows(
            "alice_voice_recording_artifacts",
            rows,
            ALICE_VOICE_RECORDING_ARTIFACT_COLUMNS,
        )

    def upsert_alice_voice_recordings_sync_state(self, row: dict[str, Any]) -> None:
        """Record one Alice poll: that it ran, and whether it worked.

        The row is upserted per account and guarded by ``updated_at``, so a
        late-committing run can never stamp over a newer one. It is the
        pipeline's ONLY run signal -- Alice is a Dagster poller, not an
        uploader, so nothing writes it an ops.uploader_heartbeats row.
        """
        self._insert_rows(
            "alice_voice_recordings_sync_state",
            [row],
            ALICE_VOICE_RECORDINGS_SYNC_STATE_COLUMNS,
        )

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

    def insert_photo_source_files(self, table: str, rows: list[dict[str, Any]]) -> None:
        """Insert raw photo file rows into one source's files table.

        ``table`` must be a registered PHOTO_SOURCE_RELATIONS value; routing by
        envelope source happens in the drive-ingest layer, which fails loud on
        unknown sources.
        """
        if table not in PHOTO_SOURCE_RELATIONS.values():
            raise ValueError(f"unknown photo source table {table!r}")
        self._insert_rows(table, rows, PHOTO_SOURCE_FILE_COLUMNS)

    def insert_photo_assets(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("photo_assets", rows, PHOTO_ASSET_COLUMNS)

    def insert_photo_asset_files(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("photo_asset_files", rows, PHOTO_ASSET_FILE_COLUMNS)

    def insert_media_fingerprints(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("media_fingerprints", rows, MEDIA_FINGERPRINT_COLUMNS)

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
            INSERT INTO @whatsapp_chats (
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
            FROM (SELECT DISTINCT account, chat_id FROM @whatsapp_messages) m
            LEFT JOIN @whatsapp_chats c ON c.account = m.account AND c.chat_id = m.chat_id
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
        rows_by_table: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            source = str(row.get("source") or "")
            table = _AI_EVENT_TABLE_BY_SOURCE.get(source)
            if table is None:
                raise ValueError(f"unknown AI conversation event source: {source!r}")
            rows_by_table.setdefault(table, []).append(row)
        for table, table_rows in rows_by_table.items():
            self._insert_rows(table, table_rows, AGENT_SESSION_EVENT_COLUMNS)

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
            FROM @chatgpt_conversation_sync
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
            INSERT INTO @chatgpt_conversation_sync (account, session_id, update_time, event_count, synced_at)
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

    def load_untranscribed_voice_recordings(self, *, provider: str, limit: int) -> list[dict[str, Any]]:
        """Recordings from EVERY voice source that still need transcription.

        Reads marts_voice_memos.recordings, not base_apple_voice_memos.files.
        Scanning the raw table was the whole defect: a second registered voice
        source (base_alice_voice_recordings, 53 recordings) was never a
        candidate, so it carried 0 transcripts and 0 summaries while every
        enforced registry stayed green. The transcription-run join stays on the
        derived table because it is the authority on what has already been
        attempted, and it now matches on ``source`` too -- a recording_id is
        unique only inside its own source.
        """
        rows = self._query(
            f"""
            SELECT
                r.source,
                r.account,
                r.recording_id,
                r.source_title,
                r.filename,
                r.content_type,
                r.size_bytes,
                r.content_sha256,
                r.recorded_at,
                r.storage_backend,
                r.storage_key,
                r.storage_file_id,
                r.storage_url
            FROM @marts_voice_memos_recordings AS r
            LEFT JOIN (
                SELECT source, account, recording_id, content_sha256, completed_at
                FROM @apple_voice_memos_transcription_runs
                WHERE provider = %s
                  AND (
                    status = ANY(%s)
                    -- Rows written before 'rejected' existed are still plain
                    -- 'error', so the pattern test stays as the fallback for
                    -- them. Both halves read the same authority.
                    OR (status = 'error' AND NOT ({_postgres_retryable_error_clause('error')}))
                  )
            ) AS terminal
              ON r.source = terminal.source
             AND r.account = terminal.account
             AND r.recording_id = terminal.recording_id
             AND terminal.content_sha256 = r.content_sha256
            WHERE terminal.recording_id IS NULL
              AND r.size_bytes > 0
              AND r.is_deleted = 0
            ORDER BY r.recorded_at DESC NULLS LAST
            LIMIT %s
            """,
            (provider, list(VOICE_MEMO_TRANSCRIPTION_TERMINAL_STATUSES), int(limit)),
        )
        columns = (
            "source",
            "account",
            "recording_id",
            "title",
            "filename",
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

    def existing_message_ids(self, *, account: str, message_ids: list[str]) -> set[str]:
        if not message_ids:
            return set()
        rows = self._query(
            """
            SELECT message_id
            FROM @gmail_messages
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
            FROM @gmail_attachments
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
            FROM @gmail_messages
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

    def load_slack_sync_state_by_type(self, object_type: str) -> dict[tuple[str, str, str, str], dict[str, Any]]:
        """Sync-state rows of one object_type — a handful, not the whole table.

        The metadata and coverage stages pick what to do next from this state,
        and neither must pay for load_slack_sync_state()'s full-table read to do
        it: ops.slack_sync_state is 1.1M rows / 363 MB, and that whole dict is
        already materialised once per stage. The partial indexes keep these
        single-digit-row lookups instead of a seq scan over the heap.
        """
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
        rows = self._query(
            f"SELECT {', '.join(_identifier(column) for column in columns)} FROM @slack_sync_state "
            "WHERE object_type = %s",
            (object_type,),
        )
        return {
            (str(row[0]), str(row[1]), str(row[2]), str(row[3])): dict(zip(columns, row, strict=True))
            for row in rows
        }

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
        rows = self._query(f"SELECT {', '.join(_identifier(column) for column in columns)} FROM @slack_sync_state")
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
                FROM @slack_conversations
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
            UPDATE @slack_conversations
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
        conversation_ids: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        where = ["c.account = %s", "c.team_id = %s"]
        params: list[Any] = [account, team_id]
        if conversation_ids is not None:
            # Restricting to an explicit set is how the change-feed path avoids
            # the blanket poll: Slack has already told us which conversations
            # moved, so everything else is known not to need a history call.
            where.append("c.conversation_id = ANY(%s)")
            params.append(list(conversation_ids))
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
            # 'gone' is terminal (deleted/archived/left channel) so retrying is a
            # guaranteed identical failure; a transient 'error' stays in the
            # candidate set so it can heal to 'ok' on the next attempt.
            where.append("COALESCE(s.status, '') != 'gone'")
        limit_clause = "LIMIT %s" if limit is not None else ""
        if limit is not None:
            params.append(int(limit))
        rows = self._query(
            f"""
            SELECT c.raw_json
            FROM @slack_conversations AS c
            LEFT JOIN @slack_sync_state AS s
              ON c.account = s.account
             AND c.team_id = s.team_id
             AND c.conversation_id = s.object_id
             AND s.object_type = 'conversation'
            LEFT JOIN @slack_conversation_stats AS m
              ON c.account = m.account
             AND c.team_id = m.team_id
             AND c.conversation_id = m.conversation_id
            WHERE {" AND ".join(where)}
            ORDER BY
                (NOT (COALESCE(s.status, '') = 'ok' AND COALESCE(s.last_sync_type, '') = 'full')) DESC,
                (COALESCE(m.message_count, 0) = 0) DESC,
                -- Channels Zach is IN, before the ~13k he is not. Membership is
                -- the strongest available signal that a conversation's contents
                -- are addressed to him, and coverage is rate-limited enough that
                -- ordering decides what actually gets synced rather than merely
                -- what gets synced first. Measured 2026-08-26: 1,609 discovered
                -- public channels had never been fetched, and two of them --
                -- #athena-announcements (2,054 members) and #hc-videos -- were
                -- channels he belongs to, indistinguishable in this ORDER BY
                -- from any channel he has never opened.
                c.is_member DESC,
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

    def load_slack_known_conversation_ids(
        self,
        *,
        account: str,
        team_id: str,
        conversation_ids: Sequence[str],
    ) -> set[str]:
        """Which of these conversation ids we already hold a row for.

        The freshness pass asks this about the ids ``client.counts`` said moved, so it
        can tell "nothing new happened here" from "we have never seen this conversation
        at all". The second case used to be indistinguishable from the first and cost
        hours of landing latency on every newly created DM and group DM.
        """
        wanted = [str(conversation_id) for conversation_id in conversation_ids if conversation_id]
        if not wanted:
            return set()
        rows = self._query(
            """
            SELECT conversation_id
            FROM @slack_conversations
            WHERE account = %s AND team_id = %s AND conversation_id = ANY(%s)
            """,
            (account, team_id, wanted),
        )
        return {str(row[0]) for row in rows}

    def load_slack_public_sweep_candidate_payloads(
        self,
        *,
        account: str,
        team_id: str,
        hot_within_days: int = 7,
        hot_limit: int = 0,
        cold_limit: int = 0,
    ) -> list[dict[str, Any]]:
        """Live public channels due a history poll, hottest bucket first.

        Membership is deliberately NOT a filter here. The change feed
        (``client.counts``) only reports conversations Zach participates in, and
        coverage only offers a channel whose history has never been completed, so
        a public channel he is not in was polled exactly once — at backfill — and
        then never again. Measured 2026-08-27 against Slack's own admin
        analytics: 11,488 non-member public channels were marked ``full`` and
        10,711 of those had not been touched in fourteen days, and PDW held 40%
        of August's public-channel messages.

        The two buckets are the whole design. ``hot`` is channels that have said
        something recently, so they are re-polled often enough to be useful;
        ``cold`` is a round-robin over everything else so a channel that wakes up
        after months is still noticed. Both order by *when we last polled*
        (``sync_state.updated_at``, NULLS FIRST so a never-synced channel is
        picked up first), which is why the sweep must stamp that column even when
        a poll returns nothing — otherwise the same channels are re-picked
        forever and the tail never advances.
        """
        payloads: list[dict[str, Any]] = []
        seen: set[str] = set()
        for hot in (True, False):
            limit = hot_limit if hot else cold_limit
            if limit <= 0:
                continue
            activity_predicate = (
                "m.latest_message_at >= now() - make_interval(days => %s)"
                if hot
                else "(m.latest_message_at IS NULL"
                " OR m.latest_message_at < now() - make_interval(days => %s))"
            )
            rows = self._query(
                f"""
                SELECT c.raw_json
                FROM @slack_conversations AS c
                LEFT JOIN @slack_sync_state AS s
                  ON c.account = s.account
                 AND c.team_id = s.team_id
                 AND c.conversation_id = s.object_id
                 AND s.object_type = 'conversation'
                LEFT JOIN @slack_conversation_stats AS m
                  ON c.account = m.account
                 AND c.team_id = m.team_id
                 AND c.conversation_id = m.conversation_id
                WHERE c.account = %s
                  AND c.team_id = %s
                  AND c.conversation_type = 'public_channel'
                  AND c.is_archived = 0
                  AND COALESCE(s.status, '') <> 'gone'
                  AND {activity_predicate}
                ORDER BY s.updated_at ASC NULLS FIRST, c.conversation_id
                LIMIT %s
                """,
                (account, team_id, int(hot_within_days), int(limit)),
            )
            for payload in _json_payloads(rows):
                conversation_id = str(payload.get("id") or "")
                if conversation_id and conversation_id in seen:
                    continue
                seen.add(conversation_id)
                payloads.append(payload)
        return payloads

    def touch_slack_conversation_sync_state(
        self,
        *,
        account: str,
        team_id: str,
        conversation_id: str,
        updated_at: datetime,
        sync_version: int,
    ) -> None:
        """Record that we polled a conversation, without claiming what we found.

        A poll that returns no new messages writes no cursor, so nothing else
        advances ``updated_at`` — and ``updated_at`` is what the sweep orders by.
        Without this a quiet channel is re-polled every run and the rest of the
        workspace is never reached. The existing cursor, sync type and status are
        preserved on conflict: this says when we last looked, not what we know.
        """
        self._command(
            """
            INSERT INTO @slack_sync_state (
                account, team_id, object_type, object_id,
                cursor_ts, last_sync_type, status, error, updated_at, sync_version
            )
            VALUES (%s, %s, 'conversation', %s, '', 'sweep', 'ok', '', %s, %s)
            ON CONFLICT (account, team_id, object_type, object_id) DO UPDATE
               SET updated_at = EXCLUDED.updated_at,
                   sync_version = EXCLUDED.sync_version
            """,
            (account, team_id, conversation_id, _ensure_utc(updated_at), int(sync_version)),
        )

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
            # A conversation already known gone (channel_not_found etc.) will fail
            # conversations.replies for every one of its threads identically, so
            # never offer up a never-before-tried thread from it either.
            "COALESCE(c.is_archived, 0) = 0",
        ]
        params: list[Any] = [account, team_id]
        if since_ts is not None:
            where.append(_numeric_ts("m.message_ts") + " >= %s")
            params.append(since_ts)
            # Same cutoff on the indexed timestamp column: the numeric
            # message_ts expression cannot use an index, and without this bound
            # the query seq-scanned the whole messages heap (~46 GB of buffer
            # reads every ~5 minutes in production). message_datetime is
            # derived from message_ts, so the two predicates agree.
            where.append("m.message_datetime >= to_timestamp(%s)")
            params.append(since_ts)
        if skip_known_errors:
            # Terminally-gone threads (deleted parent, dead channel) are never
            # retried; transient 'error' threads are, so they self-heal.
            where.append("(s.object_id IS NULL OR s.status != 'gone')")
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
                "SELECT 1 FROM @slack_messages AS r "
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
            FROM @slack_messages AS m
            LEFT JOIN @slack_sync_state AS s
              ON m.account = s.account
             AND m.team_id = s.team_id
             AND s.object_type = 'thread'
             AND m.conversation_id || ':' || m.message_ts = s.object_id
            LEFT JOIN @slack_conversations AS c
              ON m.account = c.account
             AND m.team_id = c.team_id
             AND m.conversation_id = c.conversation_id
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
            FROM @slack_conversations AS c
            LEFT JOIN @slack_conversation_stats AS m
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
            where.append("COALESCE(s.status, '') != 'gone'")
        limit_clause = "LIMIT %s" if limit is not None else ""
        if limit is not None:
            params.append(int(limit))
        rows = self._query(
            f"""
            SELECT c.raw_json
            FROM @slack_conversations AS c
            LEFT JOIN @slack_sync_state AS s
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
            UPDATE @slack_conversation_members
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

    # --- Slack image fingerprints ------------------------------------------
    #
    # "Who sent this image?" needs three things joined: the perceptual hash
    # (shared with photos), the Slack file row, and the uploader's identity.
    # The last one is what the 2026-08-16 agent never joined, so the marts view
    # resolves it rather than leaving it to the caller.

    def ensure_slack_file_fingerprint_tables(self) -> None:
        self._ensure_table_group(
            ["slack_files", "slack_users", "slack_conversations", "slack_file_fingerprints", "media_fingerprints"]
        )
        self._ensure_slack_image_fingerprint_view()

    # Slack publishes no API that lists huddles, and none that exposes huddle
    # audio or Slack-AI huddle notes — which makes it easy to conclude that
    # huddles are simply absent from the warehouse. Their METADATA is not: every
    # huddle posts a message with subtype 'huddle_thread' whose payload carries a
    # `room` object with created_by, date_start, date_end and the full
    # participant_history. Those rows were already being ingested and were only
    # unreachable because they sat inside raw_json.
    #
    # What is genuinely missing is the huddle's CONTENT. Nothing said in a huddle
    # reaches PDW, so absence of a decision in the warehouse is never evidence
    # that the decision was not made — this view exists partly so that gap is
    # visible rather than inferred.
    #
    # date_start / date_end are epoch INTEGERS inside the JSON, not the
    # timestamptz the rest of the warehouse uses, and a huddle still running
    # carries 0. Both are normalised here so no caller has to know that.
    def _ensure_slack_huddles_view(self) -> None:
        self._ensure_view(
            "marts_slack_huddles",
            """
            CREATE OR REPLACE VIEW @marts_slack_huddles AS
            WITH huddle AS (
                SELECT
                    m.account,
                    m.team_id,
                    m.conversation_id,
                    m.message_ts,
                    m.message_datetime,
                    m.user_id,
                    m.reply_count,
                    (m.raw_json::jsonb -> 'room') AS room
                FROM @slack_messages AS m
                WHERE m.subtype = 'huddle_thread'
                  AND m.is_deleted = 0
                  AND m.raw_json <> ''
                  AND jsonb_typeof(m.raw_json::jsonb -> 'room') = 'object'
            )
            SELECT
                h.account,
                h.team_id,
                h.conversation_id,
                COALESCE(c.name, '') AS conversation_name,
                COALESCE(c.conversation_type, '') AS conversation_type,
                h.message_ts,
                COALESCE(h.room ->> 'id', '') AS huddle_id,
                COALESCE(h.room ->> 'name', '') AS huddle_name,
                COALESCE(NULLIF(h.room ->> 'created_by', ''), h.user_id) AS created_by,
                h.message_datetime AS posted_at,
                to_timestamp(NULLIF((h.room ->> 'date_start'), '')::bigint) AS started_at,
                to_timestamp(NULLIF(NULLIF((h.room ->> 'date_end'), ''), '0')::bigint) AS ended_at,
                (NULLIF(NULLIF((h.room ->> 'date_end'), ''), '0')::bigint
                    - NULLIF((h.room ->> 'date_start'), '')::bigint) AS duration_seconds,
                CASE WHEN (h.room ->> 'has_ended')::boolean THEN 1 ELSE 0 END::bigint AS has_ended,
                COALESCE(participants.user_ids, ARRAY[]::text[]) AS participant_user_ids,
                COALESCE(array_length(participants.user_ids, 1), 0)::bigint AS participant_count,
                h.reply_count AS thread_message_count,
                COALESCE(h.room ->> 'huddle_link', '') AS huddle_link
            FROM huddle AS h
            LEFT JOIN LATERAL (
                SELECT array_agg(DISTINCT value) AS user_ids
                FROM jsonb_array_elements_text(
                    CASE
                        WHEN jsonb_typeof(h.room -> 'participant_history') = 'array'
                            THEN h.room -> 'participant_history'
                        ELSE '[]'::jsonb
                    END
                ) AS value
            ) AS participants ON TRUE
            LEFT JOIN @slack_conversations AS c
                   ON c.account = h.account
                  AND c.team_id = h.team_id
                  AND c.conversation_id = h.conversation_id
            """,
        )

    # Per conversation type, because Slack is one pipeline in
    # marts_ops.pipeline_health and ~19k public-channel messages a day kept that
    # row green while group-DM ingestion was completely dead. Two things are
    # deliberate here:
    #
    #  * The status is the SHARE of live conversations re-stamped within one
    #    expected cycle -- never max(synced_at), and not the single oldest row
    #    either. max() is useless: a page-1-only walk re-stamped the first 200
    #    rows hourly and looked perfect while 2,120 conversations were never
    #    discovered at all. min() over-fires: rows archived upstream after we
    #    last listed them keep is_archived = 0 forever (the walk that would fix
    #    the flag is the one that excludes them), so a permanent 1% tail can
    #    never be re-stamped. The share separates them cleanly -- measured after
    #    the repair, im 100%, mpim 100%, private_channel 99.1%, public_channel
    #    99.2%, against 200-of-2,597 = 7.7% during the outage.
    #  * Message ages are reported but never drive the status. mpim genuinely has
    #    zero-message days — eleven of them between 2026-07-11 and 2026-08-18 —
    #    so alerting on "no group DM messages" is a guaranteed false positive.
    #    "No sync attempt" has no such excuse.
    #
    # Expected cycle intervals are per type because list sizes differ by two
    # orders of magnitude (114 private channels versus ~13k public ones), so one
    # threshold would either cry wolf on public_channel or never fire on mpim.
    #
    # The third half, since 2026-08-28, is LANDING: how long a message takes to
    # become visible. Discovery and polling can both read perfect while a DM
    # written at 18:13 lands at 19:15 -- measured that day, 1:1 DMs p95 62 min,
    # group DMs p50 46 min, with whole conversations arriving in one batch --
    # and no row said so. The stamp is timeline.events.first_seen_at, not
    # base_slack.messages.synced_at: synced_at is re-stamped by every re-fetch
    # of the freshness window (the same 18:13 message read synced_at 19:15:16
    # after landing), so it cannot distinguish arrival from re-reading. Judged
    # only for im/mpim (see SLACK_DM_LANDING_P95_SECONDS); a channel's landing
    # time is the sweep's rate budget, not a fault.
    def _ensure_slack_conversation_health_view(self) -> None:
        # The view reads timeline.events. On a fresh database the Slack ensure
        # can run before any timeline sync has created it; make it exist once
        # rather than guard the view out of the inventory.
        if not self._relation_exists("timeline_events"):
            self.ensure_timeline_tables()
        self._ensure_view(
            "marts_ops_slack_conversation_health",
            f"""
            CREATE OR REPLACE VIEW @marts_ops_slack_conversation_health AS
            WITH expected(conversation_type, cycle_seconds, history_cycle_seconds, landing_p95_seconds) AS (
                -- history_cycle_seconds is how often we must re-ASK a
                -- conversation for new messages. It is NULL for the three types
                -- the change feed covers (client.counts reports every
                -- conversation Zach participates in), because there "we have not
                -- polled it" is not evidence of anything: Slack told us nothing
                -- happened. Public channels have no such signal -- he is not in
                -- 13k of them -- so only a poll can find out, and only there is
                -- the poll age judged. The number is the sweep's own rotation
                -- (~2 days at its default limits) with margin.
                --
                -- landing_p95_seconds is the opposite way round: judged for the
                -- two DM types only, where a person is waiting on the other end
                -- and the change feed plus a five-minute tick is supposed to
                -- deliver in minutes. NULL for channels, whose landing time is
                -- the sweep rotation by design.
                VALUES
                    ('im', 172800::bigint, NULL::bigint, {SLACK_DM_LANDING_P95_SECONDS}::bigint),
                    ('mpim', 172800::bigint, NULL::bigint, {SLACK_DM_LANDING_P95_SECONDS}::bigint),
                    ('private_channel', 172800::bigint, NULL::bigint, NULL::bigint),
                    ('public_channel', 432000::bigint, 345600::bigint, NULL::bigint)
            ),
            per_type AS (
                SELECT
                    c.account,
                    c.team_id,
                    c.conversation_type,
                    count(*)::bigint AS conversation_count,
                    count(*) FILTER (WHERE c.is_archived = 1)::bigint AS archived_count,
                    count(*) FILTER (WHERE c.is_archived = 0)::bigint AS live_count,
                    -- Unarchived conversations only. Discovery lists with
                    -- exclude_archived=true, so an archived row's synced_at can
                    -- never be refreshed and judging it would pin this view to
                    -- 'stale' forever after a perfectly healthy walk.
                    min(c.synced_at) FILTER (WHERE c.is_archived = 0)
                        AS oldest_conversation_synced_at,
                    max(c.synced_at) AS newest_conversation_synced_at,
                    count(*) FILTER (
                        WHERE c.is_archived = 0
                          AND c.synced_at > now() - make_interval(
                              secs => COALESCE(e.cycle_seconds, 172800))
                    )::bigint AS refreshed_count,
                    max(s.latest_message_at) AS newest_message_at,
                    count(*) FILTER (
                        WHERE c.is_archived = 0
                          AND h.updated_at > now() - make_interval(
                              secs => COALESCE(e.history_cycle_seconds, 345600))
                    )::bigint AS history_polled_count,
                    min(h.updated_at) FILTER (WHERE c.is_archived = 0)
                        AS oldest_history_poll_at
                FROM @slack_conversations AS c
                LEFT JOIN @slack_conversation_stats AS s
                       ON s.account = c.account
                      AND s.team_id = c.team_id
                      AND s.conversation_id = c.conversation_id
                LEFT JOIN @slack_sync_state AS h
                       ON h.account = c.account
                      AND h.team_id = c.team_id
                      AND h.object_type = 'conversation'
                      AND h.object_id = c.conversation_id
                LEFT JOIN expected AS e ON e.conversation_type = c.conversation_type
                WHERE c.conversation_type <> ''
                GROUP BY c.account, c.team_id, c.conversation_type, e.cycle_seconds, e.history_cycle_seconds
            ),
            landing AS (
                -- Landing latency = first_seen_at - event_ts, over the messages
                -- WRITTEN in the last 24 hours (bounded by event_ts, which
                -- timeline_events_source_time_idx serves; a backfill landing
                -- old messages is therefore excluded rather than read as a
                -- day-long delay). A message written in that window that has
                -- not landed at all is invisible here -- discovery and the
                -- change feed are the detectors for that, and this row is the
                -- detector for "it landed, but late". Measured 2026-08-28 on
                -- production: ~50k rows, 95ms warm, 39k shared buffers, all of
                -- them the newest pages of the heap.
                SELECT
                    c.account,
                    c.team_id,
                    c.conversation_type,
                    count(*)::bigint AS landing_sample_24h,
                    percentile_cont(0.5) WITHIN GROUP (
                        ORDER BY GREATEST(0, EXTRACT(EPOCH FROM e.first_seen_at - e.event_ts))
                    ) AS landing_p50,
                    percentile_cont(0.95) WITHIN GROUP (
                        ORDER BY GREATEST(0, EXTRACT(EPOCH FROM e.first_seen_at - e.event_ts))
                    ) AS landing_p95
                FROM @timeline_events AS e
                JOIN @slack_conversations AS c
                  ON c.account = e.source_pk ->> 'account'
                 AND c.team_id = e.source_pk ->> 'team_id'
                 AND c.conversation_id = e.source_pk ->> 'conversation_id'
                WHERE e.source = 'slack'
                  AND e.adapter = 'slack_message'
                  AND e.event_ts >= now() - interval '24 hours'
                GROUP BY c.account, c.team_id, c.conversation_type
            ),
            judged AS (
                SELECT
                    p.*,
                    e.cycle_seconds,
                    e.history_cycle_seconds,
                    e.landing_p95_seconds AS expected_landing_p95_seconds,
                    COALESCE(l.landing_sample_24h, 0)::bigint AS landing_sample_24h,
                    round(l.landing_p50)::bigint AS landing_p50_seconds,
                    round(l.landing_p95)::bigint AS landing_p95_seconds,
                    CASE
                        WHEN p.live_count = 0 THEN 'unknown'
                        WHEN e.landing_p95_seconds IS NULL THEN 'ok'
                        -- A DM type with nothing written in 24h has no
                        -- latency to judge. mpim has real zero-message days,
                        -- so this is 'unknown', never a fault, and the fold
                        -- into status below ignores it.
                        WHEN l.landing_p95 IS NULL THEN 'unknown'
                        WHEN l.landing_p95 > {SLACK_DM_LANDING_LATE_P95_SECONDS} THEN 'stale'
                        WHEN l.landing_p95 > e.landing_p95_seconds THEN 'late'
                        ELSE 'ok'
                    END AS landing_status
                FROM per_type AS p
                LEFT JOIN expected AS e ON e.conversation_type = p.conversation_type
                LEFT JOIN landing AS l
                       ON l.account = p.account
                      AND l.team_id = p.team_id
                      AND l.conversation_type = p.conversation_type
            )
            SELECT
                p.account,
                p.team_id,
                p.conversation_type,
                p.conversation_count,
                p.archived_count,
                p.live_count,
                p.refreshed_count,
                round(p.refreshed_count::numeric / NULLIF(p.live_count, 0), 4)
                    AS refreshed_fraction,
                p.oldest_conversation_synced_at,
                p.newest_conversation_synced_at,
                (EXTRACT(EPOCH FROM now() - p.oldest_conversation_synced_at))::bigint
                    AS discovery_age_seconds,
                COALESCE(p.cycle_seconds, 172800) AS expected_cycle_seconds,
                p.history_polled_count,
                round(p.history_polled_count::numeric / NULLIF(p.live_count, 0), 4)
                    AS history_polled_fraction,
                p.oldest_history_poll_at,
                p.history_cycle_seconds AS expected_history_cycle_seconds,
                CASE
                    WHEN st.status = 'complete' THEN ''
                    ELSE COALESCE(st.cursor_ts, '')
                END AS discovery_cursor,
                COALESCE(st.status, '') AS discovery_status,
                st.updated_at AS last_discovery_at,
                NULLIF(p.newest_message_at, '1970-01-01 00:00:00+00'::timestamptz)
                    AS newest_message_at,
                (EXTRACT(
                    EPOCH FROM now()
                    - NULLIF(p.newest_message_at, '1970-01-01 00:00:00+00'::timestamptz)
                ))::bigint AS message_age_seconds,
                CASE
                    WHEN p.live_count = 0 THEN 'unknown'
                    WHEN p.history_cycle_seconds IS NULL THEN 'ok'
                    WHEN p.history_polled_count::numeric / p.live_count < 0.75 THEN 'stale'
                    WHEN p.history_polled_count::numeric / p.live_count < 0.95 THEN 'late'
                    ELSE 'ok'
                END AS history_status,
                p.landing_sample_24h,
                p.landing_p50_seconds,
                p.landing_p95_seconds,
                p.expected_landing_p95_seconds,
                p.landing_status,
                -- Discovery, history and landing are separate failures and any
                -- one alone makes the type wrong, so the row reports the worst
                -- of them. Listing a channel we then never read is the shape
                -- that hid 11,488 frozen public channels behind a 99.2%
                -- discovery number for four months; a DM that lands an hour
                -- after it was written is the shape both of those halves read
                -- as healthy on 2026-08-28.
                CASE
                    WHEN p.live_count = 0 THEN 'unknown'
                    WHEN p.refreshed_count::numeric / p.live_count < 0.75
                      OR (p.history_cycle_seconds IS NOT NULL
                          AND p.history_polled_count::numeric / p.live_count < 0.75)
                      OR p.landing_status = 'stale'
                        THEN 'stale'
                    WHEN p.refreshed_count::numeric / p.live_count < 0.95
                      OR (p.history_cycle_seconds IS NOT NULL
                          AND p.history_polled_count::numeric / p.live_count < 0.95)
                      OR p.landing_status = 'late'
                        THEN 'late'
                    ELSE 'ok'
                END AS status
            FROM judged AS p
            LEFT JOIN @slack_sync_state AS st
                   ON st.account = p.account
                  AND st.team_id = p.team_id
                  AND st.object_type = 'conversation_list'
                  AND st.object_id = p.conversation_type
            """,
        )

    def _ensure_slack_image_fingerprint_view(self) -> None:
        self._ensure_view(
            "slack_image_fingerprints",
            """
            CREATE OR REPLACE VIEW @slack_image_fingerprints AS
            SELECT
                f.account,
                f.team_id,
                f.file_id,
                f.conversation_id,
                f.message_ts,
                f.name,
                f.title,
                f.mimetype,
                f.filetype,
                f.size,
                f.created_at,
                f.is_deleted,
                f.url_private,
                f.user_id AS uploader_user_id,
                COALESCE(u.display_name, '') AS uploader_display_name,
                COALESCE(u.real_name, '') AS uploader_real_name,
                COALESCE(u.name, '') AS uploader_name,
                COALESCE(u.email, '') AS uploader_email,
                COALESCE(u.is_bot, 0) AS uploader_is_bot,
                COALESCE(c.name, '') AS conversation_name,
                COALESCE(
                    NULLIF(c.conversation_type, ''),
                    CASE
                        WHEN c.is_im = 1 THEN 'im'
                        WHEN c.is_mpim = 1 THEN 'mpim'
                        WHEN c.is_group = 1 THEN 'group'
                        WHEN c.is_channel = 1 THEN 'channel'
                        ELSE ''
                    END
                ) AS conversation_kind,
                COALESCE(c.is_private, 0) AS conversation_is_private,
                l.content_sha256,
                l.fetched_bytes,
                m.hash_version,
                m.dhash,
                m.width,
                m.height
            FROM @slack_files f
            JOIN @slack_file_fingerprints l
                ON l.account = f.account AND l.team_id = f.team_id AND l.file_id = f.file_id
            JOIN @media_fingerprints m
                ON m.content_sha256 = l.content_sha256 AND m.hash_version = l.hash_version
            LEFT JOIN @slack_users u
                ON u.account = f.account AND u.team_id = f.team_id AND u.user_id = f.user_id
            LEFT JOIN @slack_conversations c
                ON c.account = f.account AND c.team_id = f.team_id
                AND c.conversation_id = f.conversation_id
            WHERE l.status = 'ok' AND l.content_sha256 <> ''
            """,
        )

    def slack_file_fingerprint_candidates(
        self, *, limit: int, now: datetime, max_attempts: int = 5
    ) -> list[dict[str, Any]]:
        """Slack image files still needing a fingerprint, newest first.

        Newest-first because recency is what people actually ask about, and it
        makes a bounded slice immediately useful instead of useful only once the
        whole 552 GB backlog is done.

        The GROUP BY collapses a file shared into several conversations to one
        candidate: same bytes, one download.

        Scaling note: this walks the ``created_at DESC`` index and skips rows
        already recorded, so the cost of finding new work grows with the size of
        the finished set. That is fine at this corpus size (index scan plus
        primary-key probes); if it ever stops being fine, the fix is a
        high-water mark for the incremental half plus a separate descending
        backfill cursor, not a bigger limit.
        """
        rows = self._query_dicts(
            """
            SELECT
                f.account,
                f.team_id,
                f.file_id,
                max(f.url_private) AS url_private,
                max(f.mimetype) AS mimetype,
                max(f.name) AS name,
                max(f.size) AS size,
                max(f.created_at) AS created_at,
                COALESCE(max(l.attempts), 0) AS attempts
            FROM @slack_files f
            LEFT JOIN @slack_file_fingerprints l
                ON l.account = f.account AND l.team_id = f.team_id AND l.file_id = f.file_id
            WHERE f.mimetype LIKE %s
              AND f.is_deleted = 0
              AND f.url_private <> ''
              AND (
                    l.file_id IS NULL
                    OR (
                        l.status NOT IN ('ok', 'undecodable', 'too_large')
                        AND l.attempts < %s
                        AND (l.next_attempt_at IS NULL OR l.next_attempt_at <= %s)
                    )
              )
            GROUP BY f.account, f.team_id, f.file_id
            ORDER BY max(f.created_at) DESC
            LIMIT %s
            """,
            ("image/%", int(max_attempts), now, int(limit)),
        )
        return rows

    def upsert_slack_file_fingerprints(self, rows: list[dict[str, Any]]) -> None:
        self._insert_rows("slack_file_fingerprints", rows, SLACK_FILE_FINGERPRINT_COLUMNS)

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

        delete_sql = "DELETE FROM @slack_conversation_stats"
        if delete_where:
            delete_sql += " WHERE " + " AND ".join(delete_where)
        try:
            self._command("BEGIN")
            self._command(delete_sql, tuple(delete_params))
            self._command(
                f"""
                INSERT INTO @slack_conversation_stats (
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
                FROM @slack_messages
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
                EXISTS (SELECT 1 FROM @slack_conversation_stats LIMIT 1),
                EXISTS (SELECT 1 FROM @slack_messages LIMIT 1)
            """
        )
        if rows and not bool(rows[0][0]) and bool(rows[0][1]):
            self.rebuild_slack_conversation_stats()

    def _ensure_slack_sync_state_gone_reclassified(self) -> None:
        """Reclassify legacy terminal gone-code failures from 'error' to 'gone'.

        Rows written before the 'gone' status existed recorded channel_not_found
        (and the other gone codes) as status 'error'. Nothing ever retries a
        gone object, so those rows could never resolve and sat in the pipeline
        health dashboard's failing count forever. The recorded error text is
        exactly '<method> failed: <code>' (SlackApiCallError), so a suffix
        match per gone code identifies them precisely; the error text itself is
        kept as the reason the object is gone.
        """
        # Imported here: these are Slack API semantics owned by the sync module,
        # and the storage layer must not drift from the codes it records.
        from personal_data_warehouse.slack_sync import (
            SLACK_CONVERSATION_GONE_CODES,
            SLACK_THREAD_GONE_CODES,
        )

        codes = sorted(SLACK_CONVERSATION_GONE_CODES | SLACK_THREAD_GONE_CODES)
        self._command(
            """
            UPDATE @slack_sync_state
            SET status = 'gone'
            WHERE status = 'error'
              AND error LIKE ANY(%s)
            """,
            ([f"% failed: {code}" for code in codes],),
        )

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
                self._expand_relations(
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
                    FROM @slack_messages AS m
                    INNER JOIN incoming AS i
                      ON m.account = i.account
                     AND m.team_id = i.team_id
                     AND m.conversation_id = i.conversation_id
                     AND m.message_ts = i.message_ts
                    """
                ),
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
                self._expand_relations(
                    """
                    INSERT INTO @slack_conversation_stats AS target (
                        account,
                        team_id,
                        conversation_id,
                        message_count,
                        latest_message_at,
                        updated_at
                    )
                    VALUES %s
                    ON CONFLICT (account, team_id, conversation_id) DO UPDATE SET
                        message_count = target.message_count + EXCLUDED.message_count,
                        latest_message_at = GREATEST(
                            target.latest_message_at,
                            EXCLUDED.latest_message_at
                        ),
                        updated_at = EXCLUDED.updated_at
                    """
                ),
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
                    self._expand_relations(
                        """
                        WITH affected(account, team_id, conversation_id) AS (VALUES %s)
                        DELETE FROM @slack_conversation_stats AS s
                        USING affected AS a
                        WHERE s.account = a.account
                          AND s.team_id = a.team_id
                          AND s.conversation_id = a.conversation_id
                        """
                    ),
                    keys,
                    template="(%s, %s, %s)",
                    page_size=1000,
                )
                execute_values(
                    cursor,
                    self._expand_relations(
                        """
                        WITH affected(account, team_id, conversation_id) AS (VALUES %s)
                        INSERT INTO @slack_conversation_stats (
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
                        FROM @slack_messages AS m
                        INNER JOIN affected AS a
                          ON m.account = a.account
                         AND m.team_id = a.team_id
                         AND m.conversation_id = a.conversation_id
                        WHERE m.is_deleted = 0
                        GROUP BY m.account, m.team_id, m.conversation_id
                        """
                    ),
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

    def refresh_slack_account_state_items(
        self, *, account: str, team_id: str, synced_at: datetime
    ) -> SlackAccountStateRefresh:
        """Bring derived_slack.inbox_items up to date for one account/team.

        The snapshot used to be rebuilt from scratch on every call: all thirty
        days of every member conversation's messages, re-read from the heap four
        times (one per UNION branch), then every previous row re-inserted as a
        tombstone. Called by every Slack stage, that was 44s mean and the single
        largest consumer on the host (22.6 CPU-hours in 46h) while eleven
        conversations actually changed per five minutes.

        Now a watermark in ``ops.slack_sync_state`` records when the last refresh
        ran. Only conversations whose own row or messages were stamped after
        it (minus SLACK_ACCOUNT_STATE_REFRESH_OVERLAP) are recomputed; their
        stale items are tombstoned by container, and items anywhere that have
        aged past the thirty-day window are tombstoned without touching their
        conversation. A full rebuild still runs when there is no watermark and
        once every SLACK_ACCOUNT_STATE_FULL_REFRESH_INTERVAL, so a row missed by
        the overlap is wrong for at most a day, never forever.
        """
        synced_at = _ensure_utc(synced_at)
        sync_version = int(synced_at.timestamp() * 1_000_000)
        columns = ", ".join(_identifier(column) for column in SLACK_ACCOUNT_STATE_ITEM_ROW_COLUMNS)
        self._command("BEGIN")
        try:
            (locked,) = self._query(
                "SELECT pg_try_advisory_xact_lock(%s)", (SLACK_ACCOUNT_STATE_REFRESH_LOCK_ID,)
            )[0]
            if not locked:
                self._command("ROLLBACK")
                return SlackAccountStateRefresh(mode="skipped")
            # The four-branch UNION compiles ~540 JIT functions (5.6s measured)
            # for a statement that now touches a handful of conversations.
            self._command("SET LOCAL jit = off")

            watermark, last_full_at = self._slack_account_state_watermark(account=account, team_id=team_id)
            full = (
                watermark is None
                or last_full_at is None
                or synced_at - last_full_at >= SLACK_ACCOUNT_STATE_FULL_REFRESH_INTERVAL
            )
            if not full and synced_at - watermark < SLACK_ACCOUNT_STATE_REFRESH_DEBOUNCE:
                # ``debounced``: a refresh ran moments ago and would recompute
                # the same rows; the next one's overlap covers this window.
                self._command("ROLLBACK")
                return SlackAccountStateRefresh(mode="debounced")
            changed: list[str] = []
            if not full:
                since = watermark - SLACK_ACCOUNT_STATE_REFRESH_OVERLAP
                changed = [
                    str(row[0])
                    for row in self._query(
                        """
                        SELECT conversation_id FROM @slack_conversations
                        WHERE account = %s AND team_id = %s AND synced_at > %s
                        UNION
                        SELECT DISTINCT conversation_id FROM @slack_messages
                        WHERE account = %s AND team_id = %s AND synced_at > %s
                        """,
                        (account, team_id, since, account, team_id, since),
                    )
                ]

            if full or changed:
                select_sql = self._slack_account_state_items_select_sql(scoped=not full)
                params: tuple[Any, ...] = (account, team_id, synced_at, sync_version)
                if not full:
                    params = (*params, changed)
                self._command(
                    f"""
                    INSERT INTO @slack_account_state_item_rows AS target ({columns})
                    {select_sql}
                    {_upsert_clause("slack_account_state_item_rows", POSTGRES_TABLES["slack_account_state_item_rows"], target_alias="target")}
                    """,
                    params,
                )

            tombstone_scope = "" if full else "AND (container_id = ANY(%s::text[]) OR latest_activity_at < %s)"
            tombstone_params: tuple[Any, ...] = (synced_at, sync_version, account, team_id, sync_version)
            if not full:
                tombstone_params = (*tombstone_params, changed, synced_at - SLACK_ACCOUNT_STATE_ITEM_WINDOW)
            with self._connection.cursor() as cursor:
                cursor.execute(
                    self._expand_relations(
                        f"""
                        UPDATE @slack_account_state_item_rows
                        SET is_deleted = 1, synced_at = %s, sync_version = %s
                        WHERE account = %s AND scope_id = %s AND is_deleted = 0 AND sync_version < %s
                          {tombstone_scope}
                        """
                    ),
                    tombstone_params,
                )
                tombstoned = int(cursor.rowcount or 0)

            self._record_slack_account_state_watermark(
                account=account,
                team_id=team_id,
                synced_at=synced_at,
                sync_version=sync_version,
                full=full,
            )
            self._command("COMMIT")
        except Exception:
            self._command("ROLLBACK")
            raise
        return SlackAccountStateRefresh(
            mode="full" if full else "incremental",
            changed_conversations=len(changed),
            rows_tombstoned=tombstoned,
        )

    def _slack_account_state_watermark(
        self, *, account: str, team_id: str
    ) -> tuple[datetime | None, datetime | None]:
        """(last refresh, last FULL refresh) for one account/team, or Nones.

        Two ``ops.slack_sync_state`` rows of object_type
        ``account_state_refresh``: object_id ``<team>`` is stamped by every
        refresh, ``<team>:full`` only by a full one, so the daily backstop reads
        its own row instead of overloading a column meant for something else.
        """
        rows = self._query(
            """
            SELECT object_id, cursor_ts
            FROM @slack_sync_state
            WHERE account = %s AND team_id = %s AND object_type = %s AND object_id = ANY(%s::text[])
            """,
            (account, team_id, SLACK_ACCOUNT_STATE_REFRESH_OBJECT_TYPE, [team_id, f"{team_id}:full"]),
        )
        stamps = {str(object_id): str(cursor_ts or "") for object_id, cursor_ts in rows}
        watermark = stamps.get(team_id) or ""
        last_full = stamps.get(f"{team_id}:full") or ""
        return (
            datetime.fromisoformat(watermark) if watermark else None,
            datetime.fromisoformat(last_full) if last_full else None,
        )

    def _record_slack_account_state_watermark(
        self,
        *,
        account: str,
        team_id: str,
        synced_at: datetime,
        sync_version: int,
        full: bool,
    ) -> None:
        object_ids = [team_id, f"{team_id}:full"] if full else [team_id]
        self._insert(
            "slack_sync_state",
            [
                (
                    account,
                    team_id,
                    SLACK_ACCOUNT_STATE_REFRESH_OBJECT_TYPE,
                    object_id,
                    synced_at.isoformat(),
                    "full" if full else "incremental",
                    "ok",
                    "",
                    synced_at,
                    sync_version,
                )
                for object_id in object_ids
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
        # Restrict to top-level messages: `conversations.history` never returns
        # thread replies inline, so the caller's "seen" set will not include them
        # either. Returning replies here would make every reply within the window
        # look like a deletion to the caller, and they would get tombstoned on
        # the next partial sync.
        rows = self._query(
            f"""
            SELECT message_ts
            FROM @slack_messages
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

    def load_slack_conversation_message_high_water(
        self,
        *,
        account: str,
        team_id: str,
        conversation_ids: Sequence[str],
    ) -> dict[str, float]:
        # Highest top-level message ts we have stored per conversation. Used to
        # reconstruct a freshness cursor for conversations whose stored cursor was lost
        # (see _sync_account_freshness_priority). Restrict to is_thread_reply = 0 to
        # mirror the cursor the partial sync advances on (conversations.history, which
        # never returns thread replies).
        unique_ids = sorted({str(conversation_id) for conversation_id in conversation_ids})
        if not unique_ids:
            return {}
        rows = self._query(
            f"""
            SELECT conversation_id, MAX({_numeric_ts("message_ts")}) AS high_water
            FROM @slack_messages
            WHERE account = %s
              AND team_id = %s
              AND conversation_id = ANY(%s)
              AND is_thread_reply = 0
            GROUP BY conversation_id
            """,
            (account, team_id, unique_ids),
        )
        high_water: dict[str, float] = {}
        for conversation_id, value in rows:
            if value is None:
                continue
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                continue
            if numeric > 0:
                high_water[str(conversation_id)] = numeric
        return high_water

    def load_slack_conversation_message_low_water(
        self,
        *,
        account: str,
        team_id: str,
        conversation_ids: Sequence[str],
    ) -> dict[str, str]:
        """Oldest top-level message ts we hold per conversation, as Slack's string.

        This is the backfill cursor. A conversation's forward cursor
        (``ops.slack_sync_state.cursor_ts``) says how far *up* we have read; it
        says nothing about how far *down*, and a conversation whose first fetch
        was a freshness window -- or a full stream cut short by the rate budget
        -- holds a cursor at "now" and no history below it. The messages
        themselves are the only record of that floor, so coverage reads it here
        and asks Slack for everything older (see ``_sync_conversation_history_below``).

        One ordered index probe per conversation
        (``slack_messages_conversation_time_idx``), never a ``min()`` over the
        table: a busy channel holds millions of rows and the caller is bounded
        to a coverage slice of a few dozen conversations.
        """
        unique_ids = sorted({str(conversation_id) for conversation_id in conversation_ids})
        if not unique_ids:
            return {}
        rows = self._query(
            """
            SELECT ids.conversation_id, m.message_ts
            FROM unnest(%s::text[]) AS ids(conversation_id)
            CROSS JOIN LATERAL (
                SELECT message_ts
                FROM @slack_messages
                WHERE account = %s
                  AND team_id = %s
                  AND conversation_id = ids.conversation_id
                  AND is_thread_reply = 0
                ORDER BY message_datetime ASC
                LIMIT 1
            ) AS m
            """,
            (unique_ids, account, team_id),
        )
        return {str(conversation_id): str(message_ts) for conversation_id, message_ts in rows if message_ts}

    # Every derived voice row written before the domain became multi-source
    # belongs to Apple Voice Memos; nothing else could write one, because the
    # key had no room for a second source.
    _VOICE_DERIVED_TABLES = (
        "apple_voice_memos_transcription_runs",
        "apple_voice_memos_transcript_segments",
        "apple_voice_memos_enrichments",
    )

    def _migrate_voice_derived_tables_to_source_keyed(self) -> None:
        for table in self._VOICE_DERIVED_TABLES:
            self._command(
                f"ALTER TABLE @{table} ADD COLUMN IF NOT EXISTS source text NOT NULL DEFAULT ''"
            )
            # Backfill BEFORE the key moves: '' is not a source and would make
            # every pre-existing row collide with itself under the new key.
            self._command(f"UPDATE @{table} SET source = 'apple_voice_memos' WHERE source = ''")
            self._ensure_primary_key(table)

    def _backfill_voice_memo_transcription_run_content_hashes(self) -> None:
        self._command(
            """
            UPDATE @apple_voice_memos_transcription_runs AS r
            SET content_sha256 = f.content_sha256,
                sync_version = GREATEST(r.sync_version + 1, (extract(epoch from clock_timestamp()) * 1000000)::bigint)
            FROM @apple_voice_memos_files AS f
            WHERE r.account = f.account
              AND r.recording_id = f.recording_id
              AND r.content_sha256 = ''
              AND f.content_sha256 != ''
            """
        )

    def _backfill_voice_memo_enrichment_content_hashes(self) -> None:
        self._command(
            """
            UPDATE @apple_voice_memos_enrichments AS e
            SET content_sha256 = f.content_sha256,
                sync_version = GREATEST(e.sync_version + 1, (extract(epoch from clock_timestamp()) * 1000000)::bigint)
            FROM @apple_voice_memos_files AS f
            WHERE e.account = f.account
              AND e.recording_id = f.recording_id
              AND e.content_sha256 = ''
              AND f.content_sha256 != ''
            """
        )

    # Tables the cross-source search function reads. General search is backed
    # by the unified timeline's BM25 document, so callers search one normalized
    # stream instead of fanning out across source-specific text columns.
    _SEARCHABLE_TEXT_TABLES = ("timeline_events",)

    _SEARCH_SCHEMA_MARKER_TABLE = "search_schema_state"

    # The timeline priority tiers, in enum declaration order (highest attention
    # first). Search takes them as a `priorities` filter so an agent can ask the
    # corpus the question a human asks it -- "what did a real person send me" --
    # instead of retrieving 39.7M noise rows and hoping the ranker sorts it out.
    # Validated in SQL against this exact list: a mistyped tier must RAISE with
    # the valid set, the same contract `sources` has, because the silent
    # alternative is a search that quietly widens back to the whole corpus and
    # answers a different question than the one asked.
    _SEARCH_PRIORITY_TOKENS = ("self", "direct", "cc", "noise", "background", "unclassified")

    def _ensure_search_views_if_possible(self) -> None:
        # Several Dagster assets can call ensure_* concurrently on deploy. The
        # shared search_text() function/index refresh mutates global Postgres
        # catalog rows, so serialize it to avoid "tuple concurrently updated"
        # races between otherwise-idempotent DDL statements.
        self._command("SELECT pg_advisory_lock(%s)", (SEARCH_SCHEMA_REFRESH_LOCK_ID,))
        try:
            self._ensure_search_views_if_possible_locked()
        finally:
            self._command("SELECT pg_advisory_unlock(%s)", (SEARCH_SCHEMA_REFRESH_LOCK_ID,))

    def _ensure_search_views_if_possible_locked(self) -> None:
        if not all(self._relation_exists(table) for table in self._SEARCHABLE_TEXT_TABLES):
            return
        # Build the timeline BM25 index search_text() references BEFORE
        # (re)creating the function, so it can never point at a not-yet-built
        # index. The gate above guarantees the referenced timeline table exists.
        self._ensure_indexes(self._SEARCHABLE_TEXT_TABLES)
        signature = self._search_schema_signature()
        if (
            signature
            and self._stored_search_schema_signature() == signature
            and self._search_text_function_exists()
        ):
            # Generated search DDL unchanged since the last build and the
            # function is present — skip the CREATE OR REPLACE recompile.
            return
        self._ensure_search_text_function()
        self._write_search_schema_signature(signature)

    def _search_schema_signature(self) -> str:
        """Signature of everything that determines the generated search DDL.

        Derived from the source code of the generator method plus the searched
        table set and the source floor, so edits to search_text() DDL, adding a
        source, or changing the source floor force a one-time rebuild. If source
        introspection is unavailable, return an empty signature so the guard
        never matches and safely degrades to the old always-rebuild behavior.

        Anything the generator INTERPOLATES has to be in here too, or a
        deployment keeps serving the old function forever with no symptom.
        timeline.context()'s per-adapter branches come from
        TIMELINE_CONTEXT_STREAMS in timeline.py, so a registry edit that never
        touches this module would otherwise leave production on the previous
        conversation shapes.
        """
        try:
            source = inspect.getsource(type(self)._ensure_search_text_function)
        except (OSError, TypeError):
            return ""
        try:
            # Flipping pgvector availability (or the chunk tables appearing)
            # changes what the generator emits (search_hybrid), so it is part
            # of the signature: rolling the postgres image to one with pgvector
            # triggers exactly one rebuild that creates the hybrid function.
            hybrid_state = f"{self.pgvector_available()}:{self._relation_exists('search_chunks')}"
        except Exception:  # noqa: BLE001 - degrade to always-rebuild
            hybrid_state = "unknown"
        payload = "\n".join(
            [
                source,
                repr(SEARCH_SOURCE_DEFS),
                repr(sorted(SEARCH_SOURCE_ALIASES.items())),
                SEARCH_DRIVE_EXCLUSION_SQL,
                ",".join(sorted(self._SEARCHABLE_TEXT_TABLES)),
                str(SEARCH_TEXT_SOURCE_FLOOR),
                str(SEARCH_TEXT_MAX_RESULTS_CAP),
                str(SEARCH_TEXT_PREVIEW_CHARS),
                str(SEARCH_TEXT_BROAD_PER_BRANCH_CAP),
                str(SEARCH_TEXT_BROAD_POOL),
                str(SEARCH_TEXT_BROAD_SMALL_POOL),
                SEARCH_TEXT_LOW_VOLUME_ADAPTERS_SQL,
                SEARCH_TEXT_ATTENTION_PRIORITIES_SQL,
                timeline_context_branch_sql(),
                ",".join(self._SEARCH_PRIORITY_TOKENS),
                str(SEARCH_HYBRID_RRF_K),
                str(SEARCH_HYBRID_SEMANTIC_WEIGHT),
                str(SEARCH_HYBRID_CANDIDATE_MULTIPLIER),
                str(SEARCH_HYBRID_EXACT_WEIGHT),
                str(SEARCH_HYBRID_LEXICAL_HEAD_WEIGHT),
                str(SEARCH_HYBRID_LEXICAL_HEAD_RANKS),
                str(SEARCH_HYBRID_EXACT_MAX_WORDS),
                str(SEARCH_HYBRID_MIN_CANDIDATES),
                str(SEARCH_HYBRID_MAX_CANDIDATES),
                str(SEARCH_HYBRID_AGENT_CANDIDATE_MULTIPLIER),
                str(SEARCH_HYBRID_AGENT_MIN_CANDIDATES),
                str(SEARCH_HYBRID_AGENT_MAX_CANDIDATES),
                str(SEARCH_EMBEDDING_DIMENSIONS),
                hybrid_state,
            ]
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _stored_search_schema_signature(self) -> str | None:
        self._command(
            f"CREATE TABLE IF NOT EXISTS {self.sql_relation(self._SEARCH_SCHEMA_MARKER_TABLE)} "
            "(id smallint PRIMARY KEY DEFAULT 1, signature text NOT NULL, "
            "CONSTRAINT search_schema_state_single_row CHECK (id = 1))"
        )
        rows = self._query(
            f"SELECT signature FROM {self.sql_relation(self._SEARCH_SCHEMA_MARKER_TABLE)} WHERE id = 1"
        )
        if not rows:
            return None
        return rows[0][0]

    def _write_search_schema_signature(self, signature: str) -> None:
        self._command(
            f"INSERT INTO {self.sql_relation(self._SEARCH_SCHEMA_MARKER_TABLE)} (id, signature) "
            "VALUES (1, %s) ON CONFLICT (id) DO UPDATE SET signature = EXCLUDED.signature",
            (signature,),
        )

    def _search_text_function_exists(self) -> bool:
        expected = (
            (self._object_schema("search_text"), "search_text"),
            (self._object_schema("search_text_exact"), "search_text_exact"),
            (self._object_schema("search_text_preview"), "search_text_preview"),
            (self._object_schema("timeline_context"), "context"),
        )
        if self.pgvector_available() and self._relation_exists("search_chunks"):
            expected += (
                (self._object_schema("search_hybrid"), "search_hybrid"),
                (self._object_schema("search_hybrid_semantic"), "search_hybrid_semantic"),
                (self._object_schema("search_hybrid_exact"), "search_hybrid_exact"),
                (self._object_schema("search_hybrid_fuse"), "search_hybrid_fuse"),
            )
        rows = self._query(
            """
            SELECT count(DISTINCT (n.nspname, p.proname))
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE (n.nspname, p.proname) IN %s
            """,
            (expected,),
        )
        return bool(rows) and int(rows[0][0]) == len(expected)

    def _ensure_search_text_function(self) -> None:
        # search_text() is the default RANKED cross-source search path and
        # search_text_exact() is its literal-substring sibling; both read the
        # unified timeline's `search_text` document, rather than fanning out
        # over source-specific tables. Timeline adapters own the hard
        # normalization work: they pick the event timestamp, actor, context,
        # priority, stable ref, and full text document (including detail text
        # like Drive extracts, transcripts, and media enrichments).
        #
        # Output is (source, subsource, context, who, occurred_at, account,
        # ref, text, score) for both. `ref` is a timeline ref of the form
        # `<adapter>:<event_id>`; drill into timeline.events by adapter/event_id
        # (or use source_table/source_pk from that row) for source-specific rows.
        #
        # search_text() executes one BM25 top-k branch per coarse source so
        # broad searches cannot starve low-volume sources. All branches read the
        # same timeline_events_search_text_bm25_idx index and differ only in the
        # adapter filter and source label they return. search_text_exact() is a
        # single ILIKE scan over the same document served by
        # timeline_events_search_text_trgm_idx, ordered by recency.

        def adapter_filter(adapters: tuple[str, ...]) -> str:
            if len(adapters) == 1:
                return f"t.adapter = '{adapters[0]}'"
            adapter_list = ", ".join(f"'{adapter}'" for adapter in adapters)
            return f"t.adapter IN ({adapter_list})"

        def branch(source: str, adapters: tuple[str, ...], subsource: str) -> tuple[str, str]:
            # Score through the index that COVERS this branch's adapters.
            # vchord-bm25 raises when the named index is not the one the plan
            # used, and a low-volume branch's adapter filter is fully covered by
            # the partial index -- so naming the global index there is a branch
            # failure waiting for a plan flip. It is also the fast plan.
            index_name = (
                "timeline_events_search_text_bm25_idx"
                if source in SEARCH_TEXT_HIGH_VOLUME_SOURCES
                else "timeline_events_search_text_bm25_lowvol_idx"
            )
            rank = f"t.search_text <@> to_bm25query(%1$L, '{index_name}')"
            # The priority filter is pushed into the branch WHERE, not applied
            # to the branch's results: filtering after a top-k scan returns the
            # top-k of the WHOLE corpus intersected with the tier, which for
            # 'self' (503k of 48M rows) is almost always empty. Served by
            # timeline_events_priority_time_idx.
            where_sql = (
                f"({adapter_filter(adapters)}) "
                "AND t.search_text != '' "
                "AND NOT COALESCE((t.metadata->>'deleted')::boolean, false) "
                "AND (%3$L::timestamptz IS NULL OR t.event_ts >= %3$L::timestamptz) "
                "AND (%4$L::text[] IS NULL OR t.priority::text = ANY (%4$L::text[]))"
            )
            if source == "google_drive":
                where_sql += f" AND {SEARCH_DRIVE_EXCLUSION_SQL}"
            return (
                source,
                f"( SELECT '{source}'::text AS source, {subsource} AS subsource, "
                "t.context AS context, t.actor AS who, t.event_ts AS occurred_at, "
                "COALESCE(t.source_pk->>'account', t.metadata->>'account', '') AS account, "
                "t.adapter || ':' || t.event_id AS ref, t.search_text AS text, "
                f"({rank})::real AS score, t.title AS title, "
                "t.source_table AS source_table, t.source_pk AS source_pk, "
                "t.priority::text AS priority "
                "FROM @timeline_events t "
                f"WHERE {where_sql} ORDER BY {rank} LIMIT %2$s )"
            )

        branches = [branch(*definition) for definition in SEARCH_SOURCE_DEFS]

        # The BROAD (unscoped) path. It does not touch the branch array at all:
        # both partitions are index-ordered scans of a BM25 index, the pool
        # carries only ranking keys (never the multi-MB document), and the
        # surviving rows are hydrated by primary key afterwards.
        low_volume_list = SEARCH_TEXT_LOW_VOLUME_ADAPTERS_SQL
        pool_source_case = (
            "CASE t.adapter "
            + " ".join(
                f"WHEN '{adapter}' THEN '{source}'"
                for source, adapters, _ in SEARCH_SOURCE_DEFS
                for adapter in adapters
            )
            + " END"
        )
        pool_subsource_case = (
            "CASE "
            + " ".join(
                f"WHEN {adapter_filter(adapters)} THEN {subsource}"
                for source, adapters, subsource in SEARCH_SOURCE_DEFS
            )
            + " ELSE t.kind END"
        )
        pool_where = (
            "t.search_text != '' "
            "AND NOT COALESCE((t.metadata->>'deleted')::boolean, false) "
            "AND (since IS NULL OR t.event_ts >= since) "
            "AND (priorities IS NULL OR t.priority::text = ANY (priorities)) "
            f"AND {SEARCH_DRIVE_EXCLUSION_SQL}"
        )

        # The pool carries the scan's ORDINAL, not its score. Each partition is
        # `ORDER BY <bm25 operator> LIMIT n`, so its rows already emerge in
        # exact descending relevance order and the ordinal IS the rank -- while
        # the score column re-runs the operator over every pooled document.
        # Measured on the production corpus 2026-08-26, collecting the pool the
        # way the function actually collects it (one array_agg statement, not
        # `count(*)`, which lets the planner delete the unused expression and
        # made an earlier measurement of this read as free): 5000 high-volume
        # rows cost 350ms with the score column and 108ms without; a broad
        # search's two partitions cost 575ms with and 85ms without; and with
        # `priorities => ARRAY['self']`, where every surviving document is one
        # of Zach's own large ones, 11.5s with and 0.47s without.
        # The ordinal is safe where `bm25_get_current_score()` is not: it is
        # assigned AFTER an explicit ORDER BY, so it is right whatever plan the
        # planner picked, including the seq-scan-and-sort plan a small or new
        # table gets. The helper returns a garbage constant on exactly that
        # plan, which is why tests/test_postgres_warehouse.py bans it.
        # `row_number() OVER ()` numbers rows in the order its input produces
        # them; the input is an ORDER BY ... LIMIT subquery, which cannot be
        # pulled up and which no window without PARTITION BY/ORDER BY re-sorts.
        def pool_partition(
            adapter_sql: str,
            index_name: str,
            limit: int,
            part: int,
            extra_where: str = "",
        ) -> str:
            rank = f"t.search_text <@> to_bm25query(query, '{index_name}')"
            return (
                "( SELECT p.adapter, p.event_id, p.source, "
                f"{part} AS part, row_number() OVER () AS pool_rank "
                "FROM ( SELECT t.adapter, t.event_id, "
                f"{pool_source_case} AS source "
                "FROM @timeline_events t "
                f"WHERE {pool_where} AND {adapter_sql}{extra_where} "
                f"ORDER BY {rank} LIMIT {limit} ) p )"
            )

        def two_partition_pool(
            high_index: str, low_index: str, high_part: int, low_part: int, extra_where: str = ""
        ) -> str:
            return (
                pool_partition(
                    f"t.adapter NOT IN ({low_volume_list})",
                    high_index,
                    SEARCH_TEXT_BROAD_POOL,
                    high_part,
                    extra_where,
                )
                + "\n                        UNION ALL\n                        "
                + pool_partition(
                    f"t.adapter IN ({low_volume_list})",
                    low_index,
                    SEARCH_TEXT_BROAD_SMALL_POOL,
                    low_part,
                    extra_where,
                )
            )

        broad_pool_sql = two_partition_pool(
            "timeline_events_search_text_bm25_idx",
            "timeline_events_search_text_bm25_lowvol_idx",
            SEARCH_TEXT_POOL_PART_HIGH_VOLUME,
            SEARCH_TEXT_POOL_PART_LOW_VOLUME,
        )
        # The ATTENTION pool: the identical two-partition shape, taken from the
        # partial indexes that contain only `self` and `direct`. Used only when
        # the requested tiers are a SUBSET of those -- a call for `noise` or an
        # unscoped call cannot be answered from an index that does not hold
        # those rows, and answering it from there would return silently empty
        # results, the worst failure mode this layer has.
        #
        # The literal tier predicate is REQUIRED, not belt-and-braces. A
        # partial index is usable only when the planner can prove the query
        # implies its predicate, and `priorities` is a runtime array parameter
        # it can prove nothing about. Without the literal the planner falls
        # back to the global index and vchord-bm25 raises, because
        # to_bm25query() pins an index by name and checks the plan used it.
        # The runtime `priorities` filter in pool_where still does the actual
        # selection (a call for just `self` must not return `direct`).
        attention_pool_sql = two_partition_pool(
            "timeline_events_search_text_bm25_attention_idx",
            "timeline_events_search_text_bm25_attention_lowvol_idx",
            SEARCH_TEXT_POOL_PART_ATTENTION_HIGH_VOLUME,
            SEARCH_TEXT_POOL_PART_ATTENTION_LOW_VOLUME,
            f" AND t.priority IN ({SEARCH_TEXT_ATTENTION_PRIORITIES_SQL})",
        )
        # Scoring a candidate needs the index its partition was scanned
        # through: the two partitions are two BM25 corpora with their own
        # statistics, so a low-volume row scored against the global index is
        # not the number the merge was built on.
        broad_candidate_score_sql = (
            f"CASE WHEN c.part = {SEARCH_TEXT_POOL_PART_HIGH_VOLUME} "
            "THEN (t.search_text <@> "
            "to_bm25query(query, 'timeline_events_search_text_bm25_idx'))::real "
            f"WHEN c.part = {SEARCH_TEXT_POOL_PART_LOW_VOLUME} "
            "THEN (t.search_text <@> "
            "to_bm25query(query, 'timeline_events_search_text_bm25_lowvol_idx'))::real "
            f"WHEN c.part = {SEARCH_TEXT_POOL_PART_ATTENTION_HIGH_VOLUME} "
            "THEN (t.search_text <@> "
            "to_bm25query(query, 'timeline_events_search_text_bm25_attention_idx'))::real "
            f"WHEN c.part = {SEARCH_TEXT_POOL_PART_ATTENTION_LOW_VOLUME} "
            "THEN (t.search_text <@> "
            "to_bm25query(query, "
            "'timeline_events_search_text_bm25_attention_lowvol_idx'))::real "
            # Deliberately no ELSE: every partition names itself. A catch-all
            # would score a future partition's rows against whichever corpus
            # happened to be last, which is a WRONG number rather than a
            # missing one. An unscored partition yields NULL and is dropped by
            # the `score < 0` guard below, and
            # test_search_text_broad_candidates_are_scored_through_their_own_partition_index
            # fails the moment a declared part has no branch here.
            "END"
        )

        # Run each source branch independently so a missing/unusable BM25 index
        # (for example during a deploy before the timeline index finishes) drops
        # search results rather than making the read-only query surface error.
        branch_sources_array = ", ".join(f"'{source}'" for source, _ in branches)
        branch_sql_array = ",\n                        ".join(f"$b${sql}$b$" for _, sql in branches)
        distinct_sources = sorted({source for source, _ in branches})
        search_text_sources_values = ", ".join(f"('{source}')" for source in distinct_sources)
        # search_text_exact() runs as ONE scan over the timeline document, so
        # its source filter is an adapter -> token map and its subsource is a
        # CASE over the same defs the ranked branches are generated from.
        adapter_source_values = ", ".join(
            f"('{adapter}', '{source}')"
            for source, adapters, _ in SEARCH_SOURCE_DEFS
            for adapter in adapters
        )
        subsource_whens = "\n                    ".join(
            f"WHEN {adapter_filter(adapters)} THEN {subsource}"
            for source, adapters, subsource in SEARCH_SOURCE_DEFS
            if subsource != "t.kind"
        )
        exact_subsource_case = (
            "CASE\n                    "
            + subsource_whens
            + "\n                    ELSE t.kind\n                END"
        )
        # Familiar-name aliases resolve to canonical tokens before validation,
        # in both functions, so 'apple_messages' works instead of round-tripping
        # a RAISE at the caller. Generated as a CASE from SEARCH_SOURCE_ALIASES.
        alias_whens = "\n                            ".join(
            f"WHEN '{alias}' THEN '{token}'"
            for alias, token in sorted(SEARCH_SOURCE_ALIASES.items())
        )
        alias_case = (
            "CASE s.token\n                            "
            + alias_whens
            + "\n                            ELSE s.token\n                        END"
        )
        sources_alias_sql = (
            "IF sources IS NOT NULL THEN\n"
            "                    sources := ARRAY(\n"
            "                        SELECT " + alias_case + "\n"
            "                        FROM unnest(sources) AS s(token)\n"
            "                    );\n"
            "                END IF;"
        )
        # `priorities` normalization + validation, shared verbatim by all three
        # entry points so a tier token means the same thing everywhere.
        priority_tokens_sql = ", ".join(f"'{token}'" for token in self._SEARCH_PRIORITY_TOKENS)
        priority_tokens_hint = ", ".join(self._SEARCH_PRIORITY_TOKENS)

        def priorities_guard_sql(function_name: str) -> str:
            return (
                # An EMPTY array must mean "every tier", exactly like omitting
                # the parameter. Callers build this array from an optional tool
                # field, and treating [] as "match nothing" turns a caller's
                # unset filter into a silently empty result set.
                "IF priorities IS NOT NULL AND coalesce(array_length(priorities, 1), 0) = 0 THEN\n"
                "                    priorities := NULL;\n"
                "                END IF;\n"
                "                IF priorities IS NOT NULL THEN\n"
                "                    FOREACH requested_priority IN ARRAY priorities LOOP\n"
                "                        IF NOT requested_priority = ANY (ARRAY["
                + priority_tokens_sql
                + "]) THEN\n"
                f"                            RAISE EXCEPTION '{function_name}: unknown priority %', requested_priority\n"
                "                                USING HINT = 'valid priorities are "
                + priority_tokens_hint
                + "';\n"
                "                        END IF;\n"
                "                    END LOOP;\n"
                "                END IF;"
            )
        # The per-branch row cast below lives inside a SQL string literal, which
        # relation expansion deliberately leaves alone, so it has to be written
        # schema-qualified here. An unqualified `::text_hit` resolved — through
        # the function's own pinned search_path, whose last entry is public — to
        # the pre-reorganization public.search_text_hit type. Every branch then
        # depended on a legacy leftover, and dropping it silently emptied all of
        # them (the per-branch guard swallows the type lookup error).
        hit_type_sql = self.sql_relation("search_text_hit")
        hit_type_literal = hit_type_sql.replace("'", "''")
        preview_fn_sql = self.sql_relation("search_text_preview")
        # The hit type carries drill-down columns (event_ts mirrors occurred_at
        # because agents copy timeline.events column lists into search calls;
        # title/source_table/source_pk make a hit one hop from its source row).
        # Reshaping a composite type in place is impossible, so a shape change
        # drops it WITH CASCADE (the only dependents are the search functions,
        # recreated immediately below) and recreates.
        # `priority` is on the hit because a hit that does not say which tier it
        # came from cannot be triaged: an agent filtering to 'direct' has no way
        # to show its work, and an unfiltered search cannot tell a real person's
        # message from bulk traffic without a second query per hit.
        hit_type_columns_sql = (
            "source text, subsource text, context text, who text, "
            "occurred_at timestamptz, account text, ref text, "
            "text text, score real, event_ts timestamptz, title text, "
            "source_table text, source_pk jsonb, priority text"
        )
        hit_type_attr_count = len(hit_type_columns_sql.split(","))
        self._command(
            r"""
            DO $do$
            DECLARE
                hit_attr_count integer;
            BEGIN
                IF to_regtype('"""
            + hit_type_literal
            + r"""') IS NULL THEN
                    CREATE TYPE """
            + hit_type_sql
            + r""" AS (
                        """
            + hit_type_columns_sql
            + r"""
                    );
                ELSE
                    SELECT count(*) INTO hit_attr_count
                    FROM pg_attribute
                    WHERE attrelid = to_regclass('"""
            + hit_type_literal
            + r"""')
                      AND attnum > 0 AND NOT attisdropped;
                    IF hit_attr_count IS DISTINCT FROM """
            + str(hit_type_attr_count)
            + r""" THEN
                        DROP TYPE """
            + hit_type_sql
            + r""" CASCADE;
                        CREATE TYPE """
            + hit_type_sql
            + r""" AS (
                            """
            + hit_type_columns_sql
            + r"""
                        );
                    END IF;
                END IF;
            END
            $do$;
            -- CREATE OR REPLACE with a new parameter OVERLOADS rather than
            -- replaces. Leaving the four-argument signature in place would make
            -- every existing positional call ambiguous (both candidates match)
            -- and, worse, would let a caller that omits `priorities` keep
            -- reaching an implementation that cannot filter. Drop them first.
            -- (The type rebuild above already CASCADEs them away when the hit
            -- shape changed; these make the transition explicit either way.)
            DROP FUNCTION IF EXISTS @search_text(text, integer, text[], timestamptz);
            DROP FUNCTION IF EXISTS @search_text_exact(text, integer, text[], timestamptz);
            -- Relevance preview: window the returned text around the first
            -- occurrence of any query term instead of cutting the head of the
            -- document. A head cut routinely misses the matched span in large
            -- documents (Drive extracts, transcripts), which made true hits
            -- read as false positives and pushed agents back to raw-table
            -- scans. Terms the tokenizer stemmed away simply fall back to the
            -- head cut. IMMUTABLE PARALLEL SAFE so the planner can pre-evaluate
            -- and parallelize freely.
            CREATE OR REPLACE FUNCTION @search_text_preview(doc text, query text)
            RETURNS text
            LANGUAGE sql
            IMMUTABLE
            PARALLEL SAFE
            AS $preview$
                SELECT CASE
                    WHEN doc IS NULL OR length(doc) <= """
            + str(SEARCH_TEXT_PREVIEW_CHARS)
            + r""" THEN doc
                    ELSE COALESCE(
                        (
                            SELECT substring(
                                doc
                                FROM greatest(min(strpos(lowdoc.d, term.t)) - """
            + str(SEARCH_TEXT_PREVIEW_CHARS // 2)
            + r""", 1)
                                FOR """
            + str(SEARCH_TEXT_PREVIEW_CHARS)
            + r""")
                            FROM (SELECT lower(left(doc, """
            + str(SEARCH_TEXT_PREVIEW_SCAN_CHARS)
            + r""")) AS d) lowdoc
                            CROSS JOIN (
                                SELECT DISTINCT lower(m[1]) AS t
                                FROM regexp_matches(coalesce(query, ''), '[[:alnum:]][[:alnum:]@._+-]*', 'g') AS m
                                WHERE length(m[1]) >= 3
                                LIMIT 8
                            ) term
                            WHERE strpos(lowdoc.d, term.t) > 0
                        ),
                        left(doc, """
            + str(SEARCH_TEXT_PREVIEW_CHARS)
            + r""")
                    )
                END
            $preview$;
            CREATE OR REPLACE FUNCTION @search_text(
                query text,
                max_results integer DEFAULT 50,
                sources text[] DEFAULT NULL,
                since timestamptz DEFAULT NULL,
                priorities text[] DEFAULT NULL
            )
            RETURNS SETOF @search_text_hit
            LANGUAGE plpgsql
            STABLE
            -- Deliberately NOT parallel safe/restricted: this function calls
            -- set_config() below, and set_config raises "cannot set parameters
            -- during a parallel operation" whenever IsInParallelMode() is true
            -- -- which includes the LEADER of a parallel plan, so PARALLEL
            -- RESTRICTED would not save it either. The marking costs nothing:
            -- it only governs whether a CALLER's plan may parallelize, and
            -- measurement shows the parallelism that matters here is inside the
            -- body (a plpgsql RETURN QUERY plans and parallelizes on its own,
            -- regardless of the enclosing function's label).
            AS $fn$
            DECLARE
                per_source integer := least(greatest(coalesce(max_results, 50), 1), """
            + str(SEARCH_TEXT_MAX_RESULTS_CAP)
            + r""");
                per_branch_limit integer := CASE
                    WHEN sources IS NULL THEN least(per_source, """
            + str(SEARCH_TEXT_BROAD_PER_BRANCH_CAP)
            + r""")
                    ELSE per_source
                END;
                branch_sources text[] := ARRAY[
                        """
            + branch_sources_array
            + r"""
                ];
                branch_sqls text[] := ARRAY[
                        """
            + branch_sql_array
            + r"""
                ];
                branch_source text;
                requested_priority text;
                branch text;
                branch_idx integer;
                hits @search_text_hit[] := '{}';
                -- Broad-path pool, carried as parallel arrays: the scans need
                -- enable_sort off to stay on the BM25 index, but the ranking
                -- above them needs a real sort. Collecting the pool in its own
                -- statement scopes the hint to the scans; leaving it over the
                -- whole plan cost a five-MINUTE query at a 10k pool, because
                -- the planner then had no sane way to feed the window.
                pool_adapter text[];
                pool_event_id text[];
                pool_source text[];
                pool_part integer[];
                pool_rank bigint[];
                branch_hits @search_text_hit[];
                executed_branches integer := 0;
                failed_sources text[] := '{}';
                first_branch_error text;
            BEGIN
                """
            + sources_alias_sql
            + r"""
                IF sources IS NOT NULL THEN
                    FOREACH branch_source IN ARRAY sources LOOP
                        IF NOT branch_source = ANY (branch_sources) THEN
                            RAISE EXCEPTION 'search_text: unknown source %', branch_source
                                USING HINT = 'call search_text_sources() to list the valid source tokens';
                        END IF;
                    END LOOP;
                END IF;
                """
            + priorities_guard_sql("search_text")
            + r"""
                -- BROAD SEARCH: one pooled scan, not eighteen branches.
                -- Both partitions are index-ordered scans of a BM25 index, so
                -- the pool costs tens of milliseconds where the serial branch
                -- loop cost seconds.  enable_sort is pinned off for the scans
                -- because the planner has no cost model for the bm25 operator
                -- and otherwise reads every row of a selective adapter filter
                -- and re-scores it (~5.6ms per document); it is restored
                -- immediately so the hint cannot leak into the caller's query.
                IF sources IS NULL THEN
                    PERFORM set_config('enable_sort', 'off', true);
                    -- Two pools, one shape. The ATTENTION pool reads the
                    -- partial indexes that contain only """ + SEARCH_TEXT_ATTENTION_PRIORITIES_SQL + r""",
                    -- so it is usable ONLY when every requested tier is one of
                    -- them; anything else (including no filter at all) has to
                    -- read the general indexes or it returns silently empty.
                    -- `priorities` is NULL-normalized to "all tiers" above, so
                    -- an unscoped call correctly fails this test.
                    IF priorities IS NOT NULL
                       AND priorities <@ ARRAY[""" + SEARCH_TEXT_ATTENTION_PRIORITIES_SQL + r"""] THEN
                        SELECT array_agg(p.adapter), array_agg(p.event_id),
                               array_agg(p.source), array_agg(p.part), array_agg(p.pool_rank)
                          INTO pool_adapter, pool_event_id, pool_source, pool_part, pool_rank
                          FROM (
                            """ + attention_pool_sql + r"""
                          ) p;
                    ELSE
                        SELECT array_agg(p.adapter), array_agg(p.event_id),
                               array_agg(p.source), array_agg(p.part), array_agg(p.pool_rank)
                          INTO pool_adapter, pool_event_id, pool_source, pool_part, pool_rank
                          FROM (
                            """ + broad_pool_sql + r"""
                          ) p;
                    END IF;
                    PERFORM set_config('enable_sort', 'on', true);
                    RETURN QUERY
                        -- The per-source floor needs a RANK, not a score, and
                        -- inside one partition the scan ordinal IS the score
                        -- order. The two partitions split by adapter and every
                        -- source's adapters live entirely in one of them
                        -- (test_the_two_bm25_pool_partitions_cover_every_adapter_exactly_once),
                        -- so ordering a source's rows by ordinal orders them by
                        -- score, without scoring any of them.
                        WITH broad_ranked AS (
                            SELECT u.adapter, u.event_id, u.source, u.part, u.pool_rank,
                                   row_number() OVER (
                                       PARTITION BY u.source ORDER BY u.part, u.pool_rank
                                   ) AS src_rank
                            FROM unnest(pool_adapter, pool_event_id, pool_source,
                                        pool_part, pool_rank)
                                 AS u(adapter, event_id, source, part, pool_rank)
                        ),
                        broad_floor AS (
                            SELECT * FROM broad_ranked
                            WHERE src_rank <= """ + str(SEARCH_TEXT_SOURCE_FLOOR) + r"""
                        ),
                        -- Only the CROSS-partition fill needs comparable score
                        -- VALUES, and the top-k of two score-sorted lists is
                        -- contained in each list's own first k. So the fill
                        -- candidates are the first (per_source - floor rows)
                        -- by ordinal from each partition -- at most a couple
                        -- hundred documents to score, against the ~5,800 the
                        -- pool used to score.
                        broad_fill AS (
                            SELECT f.adapter, f.event_id, f.source, f.part,
                                   f.pool_rank, f.src_rank
                            FROM (
                                SELECT r.*, row_number() OVER (
                                           PARTITION BY r.part ORDER BY r.pool_rank
                                       ) AS fill_rank
                                FROM broad_ranked r
                                WHERE r.src_rank > """ + str(SEARCH_TEXT_SOURCE_FLOOR) + r"""
                            ) f
                            WHERE f.fill_rank <= greatest(
                                per_source - (SELECT count(*) FROM broad_floor), 0)
                        ),
                        broad_candidates AS (
                            SELECT adapter, event_id, source, part, src_rank FROM broad_floor
                            UNION ALL
                            SELECT adapter, event_id, source, part, src_rank FROM broad_fill
                        ),
                        -- One heap visit per candidate: the score and the
                        -- windowed preview both read the same document, so
                        -- doing them in one join detoasts it once. A pooled row
                        -- always carries a matching (negative) score -- a BM25
                        -- index scan only emits documents that contain a query
                        -- term -- so this filter is a guard, not the merge.
                        broad_scored AS (
                            SELECT c.source AS source,
                                   """ + pool_subsource_case + r""" AS subsource,
                                   t.context AS context, t.actor AS who,
                                   t.event_ts AS occurred_at,
                                   COALESCE(t.source_pk->>'account',
                                            t.metadata->>'account', '') AS account,
                                   t.adapter || ':' || t.event_id AS ref,
                                   """ + preview_fn_sql + r"""(t.search_text, query) AS text,
                                   """ + broad_candidate_score_sql + r""" AS score,
                                   t.title AS title, t.source_table AS source_table,
                                   t.source_pk AS source_pk, t.priority::text AS priority,
                                   c.src_rank AS src_rank
                            FROM broad_candidates c
                            JOIN @timeline_events t
                              ON t.adapter = c.adapter AND t.event_id = c.event_id
                        )
                        SELECT s.source, s.subsource, s.context, s.who, s.occurred_at,
                               s.account, s.ref, s.text, s.score, s.occurred_at,
                               s.title, s.source_table, s.source_pk, s.priority
                        FROM broad_scored s
                        WHERE s.score < 0
                        ORDER BY (s.src_rank > """ + str(SEARCH_TEXT_SOURCE_FLOOR) + r""") ASC,
                                 s.score ASC
                        LIMIT per_source;
                    RETURN;
                END IF;
"""
            + r"""
                -- Same argument as the broad pool: the planner has no cost
                -- model for the bm25 operator and otherwise re-scores every row
                -- of a selective adapter filter (~5.6ms per document, 3.5s for
                -- one small branch). Restored right after the loop, because the
                -- merge below needs a real sort.
                PERFORM set_config('enable_sort', 'off', true);
                FOR branch_idx IN 1..coalesce(array_length(branch_sqls, 1), 0) LOOP
                    branch_source := branch_sources[branch_idx];
                    IF sources IS NOT NULL AND NOT branch_source = ANY (sources) THEN
                        CONTINUE;
                    END IF;
                    branch := branch_sqls[branch_idx];
                    executed_branches := executed_branches + 1;
                    BEGIN
                        EXECUTE format(
                            'SELECT array_agg(ROW(x.source, x.subsource, x.context, '
                            'x.who, x.occurred_at, x.account, x.ref, """
            + preview_fn_sql
            + r"""(x.text, %1$L), x.score, x.occurred_at, x.title, '
                            'x.source_table, x.source_pk, x.priority)::"""
            + hit_type_sql
            + r""") FROM (' || branch || ') x',
                            query, per_branch_limit, since, priorities
                        ) INTO branch_hits;
                        IF branch_hits IS NOT NULL THEN
                            hits := hits || branch_hits;
                        END IF;
                    EXCEPTION WHEN OTHERS THEN
                        -- A branch failure must never be silent: an empty
                        -- result that is really a broken branch reads as "no
                        -- matches" and has caused multi-day silent outages.
                        -- Degrade with a WARNING while other branches still
                        -- work (mid-deploy index builds), but if every branch
                        -- failed the search layer itself is broken — raise.
                        failed_sources := failed_sources || branch_source;
                        IF first_branch_error IS NULL THEN
                            first_branch_error := SQLERRM;
                        END IF;
                    END;
                END LOOP;
                PERFORM set_config('enable_sort', 'on', true);
                IF coalesce(array_length(failed_sources, 1), 0) > 0 THEN
                    IF array_length(failed_sources, 1) = executed_branches THEN
                        RAISE EXCEPTION 'search_text: every source branch failed; first error: %', first_branch_error
                            USING HINT = 'the timeline search layer is broken or mid-deploy — this is NOT an empty result';
                    END IF;
                    RAISE WARNING 'search_text: % source branch(es) failed (%) and are missing from results; first error: %',
                        array_length(failed_sources, 1),
                        array_to_string(failed_sources, ', '),
                        first_branch_error;
                END IF;
                RETURN QUERY
                    WITH ranked AS (
                        SELECT h.source, h.subsource, h.context, h.who, h.occurred_at,
                               h.account, h.ref, h.text, h.score,
                               h.event_ts, h.title, h.source_table, h.source_pk,
                               h.priority,
                               row_number() OVER (
                                   PARTITION BY h.source ORDER BY h.score ASC NULLS LAST
                               ) AS src_rank
                        FROM unnest(hits) AS h
                        WHERE (sources IS NULL OR h.source = ANY (sources))
                          AND (since IS NULL OR h.occurred_at >= since)
                          AND h.score < 0
                    )
                    SELECT r.source, r.subsource, r.context, r.who, r.occurred_at,
                           r.account, r.ref, r.text, r.score,
                           r.event_ts, r.title, r.source_table, r.source_pk,
                           r.priority
                    FROM ranked r
                    ORDER BY (r.src_rank > """
            + str(SEARCH_TEXT_SOURCE_FLOOR)
            + r""") ASC, r.score ASC NULLS LAST
                    LIMIT per_source;
            END;
            $fn$;
            CREATE OR REPLACE FUNCTION @search_text_exact(
                query text,
                max_results integer DEFAULT 50,
                sources text[] DEFAULT NULL,
                since timestamptz DEFAULT NULL,
                priorities text[] DEFAULT NULL
            )
            RETURNS SETOF @search_text_hit
            LANGUAGE plpgsql
            STABLE
            -- Same reason as search_text: this body calls set_config(), which
            -- raises under IsInParallelMode() in the leader as well as in a
            -- worker, so PARALLEL UNSAFE is the only correct label. It costs
            -- nothing -- the parallelism this function needs is INSIDE it.
            AS $fn$
            DECLARE
                per_source integer := least(greatest(coalesce(max_results, 50), 1), """
            + str(SEARCH_TEXT_MAX_RESULTS_CAP)
            + r""");
                needle text := trim(coalesce(query, ''));
                -- Machine-token variants. Agents paste amounts/phone numbers in
                -- whatever formatting the copy source used, then probe format
                -- variants by hand ('1,441.52' vs '1441.52'). Match a small set
                -- of deterministic variants of the needle in one call instead:
                -- needle_b strips thousands separators; needle_c inserts them
                -- (plain >=4-digit numbers) or strips phone punctuation.
                needle_b text;
                needle_c text;
                grouped text;
                pattern text;
                pattern_b text;
                pattern_c text;
                requested_source text;
                requested_priority text;
                -- Saved so the parallel hints below are RESTORED to whatever
                -- this deployment actually configures, not to the shipped
                -- defaults: the hint must not leak past the one statement it
                -- exists for, exactly like search_text's enable_sort scoping.
                saved_parallel_setup_cost text;
                saved_min_parallel_scan text;
            BEGIN
                IF length(needle) < 3 THEN
                    RAISE EXCEPTION 'search_text_exact: query must be at least 3 characters'
                        USING HINT = 'substring search needs >= 3 characters for the trigram index; use search_text() for ranked keyword search';
                END IF;
                """
            + sources_alias_sql
            + r"""
                IF sources IS NOT NULL THEN
                    FOREACH requested_source IN ARRAY sources LOOP
                        IF NOT requested_source = ANY (ARRAY[
                        """
            + branch_sources_array
            + r"""
                        ]) THEN
                            RAISE EXCEPTION 'search_text_exact: unknown source %', requested_source
                                USING HINT = 'call search_text_sources() to list the valid source tokens';
                        END IF;
                    END LOOP;
                END IF;
                """
            + priorities_guard_sql("search_text_exact")
            + r"""
                needle_b := regexp_replace(needle, '([0-9]),([0-9])', '\1\2', 'g');
                IF length(needle_b) < 3 THEN
                    needle_b := needle;
                END IF;
                needle_c := needle;
                IF needle ~ '^[0-9]{4,}([.][0-9]+)?$' THEN
                    LOOP
                        grouped := regexp_replace(needle_c, '([0-9])([0-9]{3})([.,]|$)', '\1,\2\3');
                        EXIT WHEN grouped = needle_c;
                        needle_c := grouped;
                    END LOOP;
                ELSIF needle ~ '^\+?[0-9() .-]{7,}$' THEN
                    needle_c := regexp_replace(needle, '[^0-9]', '', 'g');
                    IF length(needle_c) < 3 THEN
                        needle_c := needle;
                    END IF;
                END IF;
                pattern := '%' || replace(replace(replace(needle, '\', '\\'), '%', '\%'), '_', '\_') || '%';
                pattern_b := '%' || replace(replace(replace(needle_b, '\', '\\'), '%', '\%'), '_', '\_') || '%';
                pattern_c := '%' || replace(replace(replace(needle_c, '\', '\\'), '%', '\%'), '_', '\_') || '%';
                -- The trigram index answers in ~170ms; the ILIKE RECHECK then
                -- detoasts every candidate document and that is where the
                -- seconds go -- pure single-core CPU (measured on prod:
                -- shared hit=50871, zero reads) while a 28-vCPU box sat 90%+
                -- idle. The planner never chooses a parallel plan for it
                -- because it costs a bitmap heap scan by ROWS, and has no idea
                -- a row here can be a multi-megabyte TOASTed document.
                -- Telling it that setup is free, for this ONE statement, moved
                -- the identifier query from 4143ms serial to 782ms on 8
                -- workers with identical buffers and identical rows.
                -- Scoped exactly like enable_sort in search_text(): a hint left
                -- over the whole plan is how a query once ran for five MINUTES.
                saved_parallel_setup_cost := current_setting('parallel_setup_cost');
                saved_min_parallel_scan := current_setting('min_parallel_table_scan_size');
                PERFORM set_config('parallel_setup_cost', '0', true);
                PERFORM set_config('min_parallel_table_scan_size', '0', true);
                RETURN QUERY
                    SELECT hit.source, hit.subsource, hit.context, hit.who,
                           hit.occurred_at, hit.account, hit.ref,
                           -- Window the preview around the first match: a
                           -- head-of-document cut routinely misses the matched
                           -- text in large documents (transcripts, Drive docs).
                           CASE
                               WHEN length(hit.text) <= """
            + str(SEARCH_TEXT_PREVIEW_CHARS)
            + r""" THEN hit.text
                               ELSE substring(
                                   hit.text
                                   FROM greatest(COALESCE(
                                       NULLIF(position(lower(needle) IN ld.lowdoc), 0),
                                       NULLIF(position(lower(needle_b) IN ld.lowdoc), 0),
                                       NULLIF(position(lower(needle_c) IN ld.lowdoc), 0),
                                       1) - """
            + str(SEARCH_TEXT_PREVIEW_CHARS // 2)
            + r""", 1)
                                   FOR """
            + str(SEARCH_TEXT_PREVIEW_CHARS)
            + r""")
                           END AS text,
                           hit.score,
                           hit.occurred_at AS event_ts, hit.title,
                           hit.source_table, hit.source_pk, hit.priority
                    FROM (
                        SELECT map.source AS source,
                               """
            + exact_subsource_case
            + r""" AS subsource,
                               t.context AS context, t.actor AS who,
                               t.event_ts AS occurred_at,
                               COALESCE(t.source_pk->>'account', t.metadata->>'account', '') AS account,
                               t.adapter || ':' || t.event_id AS ref,
                               t.search_text AS text,
                               NULL::real AS score,
                               t.title AS title, t.source_table AS source_table,
                               t.source_pk AS source_pk,
                               t.priority::text AS priority
                        FROM @timeline_events t
                        JOIN (VALUES """
            + adapter_source_values
            + r""") AS map(adapter, source) ON map.adapter = t.adapter
                        WHERE (t.search_text ILIKE pattern
                               OR t.search_text ILIKE pattern_b
                               OR t.search_text ILIKE pattern_c)
                          AND t.search_text != ''
                          AND NOT COALESCE((t.metadata->>'deleted')::boolean, false)
                          AND """
            + SEARCH_DRIVE_EXCLUSION_SQL
            + r"""
                          AND (sources IS NULL OR map.source = ANY (sources))
                          AND (since IS NULL OR t.event_ts >= since)
                          AND (priorities IS NULL OR t.priority::text = ANY (priorities))
                        ORDER BY t.event_ts DESC
                        LIMIT per_source
                    ) hit
                    CROSS JOIN LATERAL (
                        SELECT CASE
                            WHEN length(hit.text) <= """
            + str(SEARCH_TEXT_PREVIEW_CHARS)
            + r""" THEN ''
                            ELSE lower(left(hit.text, """
            + str(SEARCH_TEXT_PREVIEW_SCAN_CHARS)
            + r"""))
                        END AS lowdoc
                    ) ld
                    ORDER BY hit.occurred_at DESC;
                PERFORM set_config('parallel_setup_cost', saved_parallel_setup_cost, true);
                PERFORM set_config('min_parallel_table_scan_size', saved_min_parallel_scan, true);
            END;
            $fn$;
            CREATE OR REPLACE FUNCTION @search_text_sources()
            RETURNS TABLE (source text)
            LANGUAGE sql
            IMMUTABLE
            PARALLEL SAFE
            AS $sources$
                SELECT s.source
                FROM (VALUES """
            + search_text_sources_values
            + r""") AS s(source)
                ORDER BY s.source
            $sources$;
            -- timeline.context(ref, before, after): the conversation around
            -- one timeline row — the rest of the email thread, the replies in
            -- the Slack thread or the messages around it in its channel, the
            -- neighbouring messages of the same iMessage/WhatsApp chat. `ref`
            -- is exactly what search_text()/search_text_exact() return
            -- ('<adapter>:<event_id>'), so a hit terminates in one hop instead
            -- of a raw-table drill.
            --
            -- Conversational sources resolve their neighbours in the SOURCE
            -- table, whose indexes already express the real conversation, and
            -- join the resolved ids back by timeline.events' primary key.
            -- `context` is a DISPLAY string and cannot carry that identity:
            -- gmail stores the mailbox account (1,187 emails over 472 threads
            -- in one account-week) and slack stores the literal 'group DM'
            -- for every mpim (65 conversations interleaved over 30 days), so
            -- the generic walk answered those two with strangers. Every other
            -- adapter keeps that generic (source, context) walk below, served
            -- by timeline_events_context_time_idx; which adapters do is
            -- declared in TIMELINE_CONTEXT_STREAMS /
            -- TIMELINE_CONTEXT_GENERIC_ADAPTERS, never inferred.
            CREATE OR REPLACE FUNCTION @timeline_context(
                ref text,
                before integer DEFAULT 5,
                after integer DEFAULT 5
            )
            RETURNS SETOF @timeline_events
            LANGUAGE plpgsql
            STABLE
            -- Unlike its search siblings this body only reads: no set_config,
            -- no SET clause, nothing that touches session state. Marking it
            -- PARALLEL SAFE lets a caller that joins context() to anything else
            -- keep a parallel plan instead of being forced serial by the call.
            PARALLEL SAFE
            AS $ctx$
            DECLARE
                anchor @timeline_events%ROWTYPE;
                ref_adapter text := split_part(coalesce(ref, ''), ':', 1);
                ref_event_id text;
                n_before integer := least(greatest(coalesce(before, 5), 0), 50);
                n_after integer := least(greatest(coalesce(after, 5), 0), 50);
                anchor_pk jsonb;
                -- The anchor's own SOURCE row, read once per call into a
                -- plpgsql record so every ordering bound below is a parameter
                -- the source index can use. Joining it in as a CTE instead
                -- made the same walk time out; see _context_stream_sql.
                src record;
                matched integer := 0;
            BEGIN
                IF position(':' IN coalesce(ref, '')) = 0 THEN
                    RAISE EXCEPTION 'context: ref must look like <adapter>:<event_id>, got %', ref
                        USING HINT = 'pass the ref column returned by search_text()/search_text_exact()';
                END IF;
                ref_event_id := substring(ref FROM length(ref_adapter) + 2);
                SELECT * INTO anchor FROM @timeline_events t
                WHERE t.adapter = ref_adapter AND t.event_id = ref_event_id;
                IF NOT FOUND THEN
                    RAISE EXCEPTION 'context: no timeline event for ref %', ref
                        USING HINT = 'refs come from search_text()/search_text_exact() as <adapter>:<event_id>';
                END IF;
                anchor_pk := anchor.source_pk;
"""
            + timeline_context_branch_sql()
            + r"""
                -- The generic walk: neighbours in time within the same
                -- (source, context) stream.
                RETURN QUERY
                    SELECT w.* FROM (
                        (
                            SELECT t.* FROM @timeline_events t
                            WHERE t.source = anchor.source
                              AND t.context = anchor.context
                              AND (t.event_ts, t.seq) < (anchor.event_ts, anchor.seq)
                            ORDER BY t.event_ts DESC, t.seq DESC
                            LIMIT n_before
                        )
                        UNION ALL
                        (
                            SELECT t.* FROM @timeline_events t
                            WHERE t.source = anchor.source
                              AND t.context = anchor.context
                              AND (t.event_ts, t.seq) >= (anchor.event_ts, anchor.seq)
                            ORDER BY t.event_ts ASC, t.seq ASC
                            LIMIT n_after + 1
                        )
                    ) w
                    ORDER BY w.event_ts ASC, w.seq ASC;
            END;
            $ctx$;
            """
        )
        if self.pgvector_available() and self._relation_exists("search_chunks"):
            # Hybrid retrieval: BM25 (search_text) + ANN over chunk embeddings,
            # merged by reciprocal rank fusion. Only created where pgvector and
            # the chunk tables exist; the app's `search` tool falls back to
            # search_text() when this function is absent. Every vector type and
            # operator is schema-qualified (public.halfvec / OPERATOR(public.<=>))
            # so resolution never depends on the caller's search_path — the
            # exact failure class behind the 16-day silent-zero outage.
            chunk_index_regclass = (
                f"{self._object_schema('search_chunks')}.search_chunks_text_trgm_idx"
            ).replace("'", "''")
            # Each expensive hybrid leg is independently callable. The app fans
            # them out over separate pooled Postgres connections so ANN, BM25,
            # and literal retrieval use several host cores instead of adding
            # their wall times inside one backend. search_hybrid() below remains
            # the direct-SQL compatibility entry point and composes these same
            # helpers -- there is no second legacy ranking flow to drift.
            self._command(
                r"""
            -- CREATE OR REPLACE with a new parameter OVERLOADS rather than
            -- replaces, so obsolete public signatures have to go explicitly.
            DROP FUNCTION IF EXISTS @search_hybrid(text, text, text, integer, text[], timestamptz);
            DROP FUNCTION IF EXISTS @search_hybrid(text, text, text, integer, text[], timestamptz, text);
            DROP FUNCTION IF EXISTS @search_hybrid_semantic(text, text, integer, text[], timestamptz);
            CREATE OR REPLACE FUNCTION @search_hybrid_semantic(
                query_embedding text,
                embedding_model text DEFAULT '"""
                + SEARCH_EMBEDDING_DEFAULT_MODEL
                + r"""',
                max_results integer DEFAULT 50,
                sources text[] DEFAULT NULL,
                since timestamptz DEFAULT NULL,
                candidate_limit integer DEFAULT NULL
            )
            RETURNS TABLE (ref text, best bigint, fuse double precision, chunk_id text)
            LANGUAGE plpgsql
            STABLE
            AS $semantic$
            DECLARE
                per_source integer := least(greatest(coalesce(max_results, 50), 1), """
                + str(SEARCH_TEXT_MAX_RESULTS_CAP)
                + r""");
                requested_candidates integer;
                sem_adapters text[];
                qvec public.halfvec("""
                + str(SEARCH_EMBEDDING_DIMENSIONS)
                + r""");
            BEGIN
                IF query_embedding IS NULL OR trim(query_embedding) = '' THEN
                    RAISE EXCEPTION 'search_hybrid_semantic: query_embedding is required'
                        USING HINT = 'pass one query embedding as a vector literal';
                END IF;
                qvec := query_embedding::public.halfvec("""
                + str(SEARCH_EMBEDDING_DIMENSIONS)
                + r""");
                -- Each invocation owns exactly one vector. Running two calls
                -- concurrently preserves the two distinct Qwen neighbourhoods
                -- without making one Postgres backend scan them serially.
                PERFORM set_config('hnsw.ef_search', least(1000, greatest(1000, per_source * 8))::text, true);
                PERFORM set_config('hnsw.iterative_scan', 'relaxed_order', true);
                PERFORM set_config('hnsw.max_scan_tuples', '100000', true);
                PERFORM set_config('hnsw.scan_mem_multiplier', '4', true);
                """
                + sources_alias_sql
                + r"""
                IF sources IS NOT NULL THEN
                    sem_adapters := ARRAY(
                        SELECT map.adapter
                        FROM (VALUES """ + adapter_source_values + r""") AS map(adapter, source)
                        WHERE map.source = ANY (sources)
                    );
                    IF coalesce(array_length(sem_adapters, 1), 0) = 0 THEN
                        RAISE EXCEPTION 'search_hybrid_semantic: unknown source filter %', sources
                            USING HINT = 'use timeline.search_text_sources() to list accepted source tokens';
                    END IF;
                END IF;
                requested_candidates := CASE
                    WHEN candidate_limit IS NOT NULL THEN least(
                        greatest(candidate_limit, per_source),
                        CASE WHEN sem_adapters <@ ARRAY[
                            'agent_session', 'agent_session_turn'
                        ]::text[] THEN """
                + str(SEARCH_HYBRID_AGENT_MAX_CANDIDATES)
                + r""" ELSE """
                + str(SEARCH_HYBRID_MAX_CANDIDATES)
                + r""" END
                    )
                    WHEN sem_adapters <@ ARRAY[
                        'agent_session', 'agent_session_turn'
                    ]::text[] THEN least(greatest(per_source * """
                + str(SEARCH_HYBRID_AGENT_CANDIDATE_MULTIPLIER)
                + ", "
                + str(SEARCH_HYBRID_AGENT_MIN_CANDIDATES)
                + "), "
                + str(SEARCH_HYBRID_AGENT_MAX_CANDIDATES)
                + r""")
                    ELSE least(greatest(per_source * """
                + str(SEARCH_HYBRID_CANDIDATE_MULTIPLIER)
                + ", "
                + str(SEARCH_HYBRID_MIN_CANDIDATES)
                + "), "
                + str(SEARCH_HYBRID_MAX_CANDIDATES)
                + r""")
                END;
                RETURN QUERY
                WITH sem_chunks AS (
                    -- Most scopes use global HNSW. Drive is excluded because a
                    -- source-first exact scan is materially faster for its
                    -- selective 223k-chunk partition.
                    (
                    SELECT c.adapter || ':' || c.event_id AS ref,
                           c.chunk_id,
                           row_number() OVER (
                               ORDER BY (e.embedding OPERATOR(public.<=>) qvec)
                           ) AS rnk
                    FROM @search_chunk_embeddings e
                    JOIN @search_chunks c ON c.text_sha256 = e.text_sha256
                    WHERE e.model = embedding_model
                      AND e.embedding IS NOT NULL
                      AND sem_adapters IS DISTINCT FROM ARRAY['drive_file']::text[]
                      AND (sem_adapters IS NULL OR c.adapter = ANY (sem_adapters))
                      AND (since IS NULL OR c.event_ts >= since)
                    ORDER BY e.embedding OPERATOR(public.<=>) qvec
                    LIMIT requested_candidates
                    )
                    UNION ALL
                    (
                    -- OFFSET 0 is the measured plan barrier that keeps this
                    -- source-first and parallel instead of returning to global
                    -- filtered HNSW. Text is deferred all the way to final
                    -- fusion, so even the top-k candidates are not detoasted.
                    SELECT top.adapter || ':' || top.event_id AS ref,
                           top.chunk_id,
                           top.rnk
                    FROM (
                        SELECT s.*,
                               row_number() OVER (ORDER BY s.distance) AS rnk
                        FROM (
                            SELECT c.chunk_id, c.adapter, c.event_id,
                                   e.embedding OPERATOR(public.<=>) qvec AS distance
                            FROM @search_chunks c
                            JOIN @search_chunk_embeddings e
                              ON e.text_sha256 = c.text_sha256
                            WHERE sem_adapters = ARRAY['drive_file']::text[]
                              AND c.adapter = ANY (sem_adapters)
                              AND e.model = embedding_model
                              AND e.embedding IS NOT NULL
                              AND (since IS NULL OR c.event_ts >= since)
                            OFFSET 0
                        ) s
                        ORDER BY s.distance
                        LIMIT requested_candidates
                    ) top
                    ORDER BY top.distance
                    )
                )
                SELECT sc.ref,
                       min(sc.rnk)::bigint AS best,
                       sum(1.0 / ("""
                + str(SEARCH_HYBRID_RRF_K)
                + r""" + sc.rnk))::double precision AS fuse,
                       (array_agg(sc.chunk_id ORDER BY sc.rnk))[1] AS chunk_id
                FROM sem_chunks sc
                GROUP BY sc.ref;
            END;
            $semantic$;
            """
            )
            self._command(
                r"""
            CREATE OR REPLACE FUNCTION @search_hybrid_exact(
                query text,
                max_results integer DEFAULT 50,
                sources text[] DEFAULT NULL,
                since timestamptz DEFAULT NULL,
                priorities text[] DEFAULT NULL
            )
            RETURNS TABLE (ref text, rnk bigint)
            LANGUAGE plpgsql
            STABLE
            AS $exact$
            DECLARE
                per_source integer := least(greatest(coalesce(max_results, 50), 1), """
                + str(SEARCH_TEXT_MAX_RESULTS_CAP)
                + r""");
                exact_refs text[];
                chat_exact_refs text[];
                exact_needle text := btrim(coalesce(query, ''));
                exact_needle_b text;
                exact_needle_c text;
                exact_grouped text;
                exact_pattern text;
                exact_pattern_b text;
                exact_pattern_c text;
                requested_priority text;
                sem_adapters text[];
            BEGIN
                """
                + sources_alias_sql
                + r"""
                """
                + priorities_guard_sql("search_hybrid_exact")
                + r"""
                IF sources IS NOT NULL THEN
                    sem_adapters := ARRAY(
                        SELECT map.adapter
                        FROM (VALUES """ + adapter_source_values + r""") AS map(adapter, source)
                        WHERE map.source = ANY (sources)
                    );
                    IF coalesce(array_length(sem_adapters, 1), 0) = 0 THEN
                        RAISE EXCEPTION 'search_hybrid_exact: unknown source filter %', sources
                            USING HINT = 'use timeline.search_text_sources() to list accepted source tokens';
                    END IF;
                END IF;
                -- The literal-substring leg. Gated on a short query: it is where
                -- BM25 tokenization and embeddings both fail (identifiers,
                -- names, paths) and literal matching wins. Plain-document
                -- machine tokens use the bounded chunk index below. Ordinary
                -- names and matching conversation windows keep the full exact
                -- path for quality and correct event identity. A natural-
                -- language question gains nothing from either one.
                -- The length floor matters: search_text_exact RAISES below it,
                -- which would take the whole hybrid search down.
                IF length(btrim(query)) >= """ + str(SEARCH_HYBRID_EXACT_MIN_CHARS) + r""" AND coalesce(
                       array_length(regexp_split_to_array(btrim(query), '\s+'), 1), 0
                   ) <= """ + str(SEARCH_HYBRID_EXACT_MAX_WORDS) + r""" THEN
                    BEGIN
                        -- search_text_exact() has to search the full timeline
                        -- document because exact mode promises whole-corpus
                        -- literal lookup. Hybrid only needs a cheap recall leg
                        -- for short identifiers. Retrieval chunks cover the
                        -- first 200k characters in bounded 2-6k rows, avoiding
                        -- the multi-megabyte TOAST recheck that dominated every
                        -- short hybrid query. Keep exact mode itself unchanged.
                        -- Match the exact function's deterministic amount and
                        -- phone variants so moving the leg does not narrow it.
                        -- Keep ordinary alphabetic names on the full-document
                        -- path. Chunk-window anchoring moved one labeled proper
                        -- name from rank 1 to rank 2; machine tokens (digits or
                        -- identifier punctuation) were quality-identical and
                        -- are the calls whose old recheck has the worst tail.
                        IF exact_needle ~ '[0-9_./@-]'
                           AND EXISTS (
                               SELECT 1
                               FROM pg_catalog.pg_index i
                               WHERE i.indexrelid = pg_catalog.to_regclass('"""
                + chunk_index_regclass
                + r"""')
                                 AND i.indisvalid
                                 AND i.indisready
                           ) THEN
                            exact_needle_b := regexp_replace(
                                exact_needle, '([0-9]),([0-9])', '\1\2', 'g'
                            );
                            IF length(exact_needle_b) < 3 THEN
                                exact_needle_b := exact_needle;
                            END IF;
                            exact_needle_c := exact_needle;
                            IF exact_needle ~ '^[0-9]{4,}([.][0-9]+)?$' THEN
                                LOOP
                                    exact_grouped := regexp_replace(
                                        exact_needle_c,
                                        '([0-9])([0-9]{3})([.,]|$)',
                                        '\1,\2\3'
                                    );
                                    EXIT WHEN exact_grouped = exact_needle_c;
                                    exact_needle_c := exact_grouped;
                                END LOOP;
                            ELSIF exact_needle ~ '^\+?[0-9() .-]{7,}$' THEN
                                exact_needle_c := regexp_replace(
                                    exact_needle, '[^0-9]', '', 'g'
                                );
                                IF length(exact_needle_c) < 3 THEN
                                    exact_needle_c := exact_needle;
                                END IF;
                            END IF;
                            exact_pattern := '%' || replace(replace(replace(
                                exact_needle, '\', '\\'), '%', '\%'), '_', '\_') || '%';
                            exact_pattern_b := '%' || replace(replace(replace(
                                exact_needle_b, '\', '\\'), '%', '\%'), '_', '\_') || '%';
                            exact_pattern_c := '%' || replace(replace(replace(
                                exact_needle_c, '\', '\\'), '%', '\%'), '_', '\_') || '%';
                            SELECT array_agg(
                                       x.ref
                                       ORDER BY x.match_chunk ASC NULLS LAST,
                                                x.match_pos ASC NULLS LAST,
                                                x.event_ts DESC
                                   )
                              INTO exact_refs
                              FROM (
                                -- Plain document chunks retain the source event
                                -- id. Join back to timeline both to validate that
                                -- ref and to apply exact's deleted/Drive filters.
                                SELECT t.adapter || ':' || t.event_id AS ref,
                                       max(t.event_ts) AS event_ts,
                                       CASE
                                           WHEN exact_needle ~ '[0-9]' THEN NULL
                                           ELSE min(c.chunk_index)
                                       END AS match_chunk,
                                       CASE
                                           -- Symbolic identifiers prefer their
                                           -- earliest chunk and position. Opaque
                                           -- numeric ids preserve recency.
                                           WHEN exact_needle ~ '[0-9]' THEN NULL
                                           ELSE (array_agg(
                                               strpos(lower(c.text), lower(exact_needle))
                                               ORDER BY c.chunk_index,
                                                        strpos(
                                                            lower(c.text),
                                                            lower(exact_needle)
                                                        )
                                           ))[1]
                                       END AS match_pos
                                FROM @search_chunks c
                                JOIN @timeline_events t
                                  ON t.adapter = c.adapter
                                 AND t.event_id = c.event_id
                                WHERE c.anchor NOT LIKE c.adapter || '|w|%'
                                  AND (c.text ILIKE exact_pattern ESCAPE '\'
                                       OR c.text ILIKE exact_pattern_b ESCAPE '\'
                                       OR c.text ILIKE exact_pattern_c ESCAPE '\')
                                  AND t.search_text != ''
                                  AND NOT COALESCE(
                                      (t.metadata->>'deleted')::boolean, false
                                  )
                                  AND """
            + SEARCH_DRIVE_EXCLUSION_SQL
            + r"""
                                  AND (
                                      sem_adapters IS NULL
                                      OR t.adapter = ANY (sem_adapters)
                                  )
                                  AND (since IS NULL OR t.event_ts >= since)
                                  AND (
                                      priorities IS NULL
                                      OR t.priority::text = ANY (priorities)
                                  )
                                GROUP BY t.adapter, t.event_id
                                ORDER BY match_chunk ASC NULLS LAST,
                                         match_pos ASC NULLS LAST,
                                         event_ts DESC
                                LIMIT per_source
                              ) x;

                            -- A conversation-window chunk represents the last
                            -- event in its hour, not necessarily the member that
                            -- contains the literal. Only if the bounded index
                            -- finds a matching chat window, use exact's full-
                            -- document path to recover the actual member ref.
                            IF EXISTS (
                                SELECT 1
                                FROM @search_chunks c
                                WHERE c.anchor LIKE c.adapter || '|w|%'
                                  AND (c.text ILIKE exact_pattern ESCAPE '\'
                                       OR c.text ILIKE exact_pattern_b ESCAPE '\'
                                       OR c.text ILIKE exact_pattern_c ESCAPE '\')
                                  AND (
                                      sem_adapters IS NULL
                                      OR c.adapter = ANY (sem_adapters)
                                  )
                                  AND (
                                      since IS NULL
                                      OR c.event_ts + interval '1 hour' > since
                                  )
                                LIMIT 1
                            ) THEN
                                SELECT array_agg(h.ref ORDER BY h.event_ts DESC)
                                  INTO chat_exact_refs
                                  FROM @search_text_exact(
                                      query,
                                      per_source,
                                      ARRAY['imessage', 'slack', 'whatsapp'],
                                      since,
                                      priorities
                                  ) AS h
                                  WHERE sem_adapters IS NULL
                                     OR split_part(h.ref, ':', 1) = ANY (sem_adapters);
                                exact_refs := (
                                    coalesce(exact_refs, ARRAY[]::text[])
                                    || coalesce(chat_exact_refs, ARRAY[]::text[])
                                )[1:per_source];
                            END IF;
                        ELSE
                            SELECT array_agg(x.ref)
                              INTO exact_refs
                              FROM (
                                SELECT h.ref
                                FROM @search_text_exact(
                                    query, per_source, sources, since, priorities
                                ) AS h
                              ) x;
                        END IF;
                    EXCEPTION WHEN OTHERS THEN
                        -- The literal leg is an enhancement; losing the whole
                        -- search because it failed would be worse than
                        -- returning the other two legs. Loud, though: a silent
                        -- drop is how a degraded search layer goes unnoticed
                        -- for weeks. Same contract as search_text's per-branch
                        -- guard.
                        RAISE WARNING 'search_hybrid_exact: literal leg failed (%); returning no literal evidence', SQLERRM;
                        exact_refs := NULL;
                    END;
                END IF;
                RETURN QUERY
                SELECT u.ref, u.ordinality::bigint AS rnk
                FROM unnest(coalesce(exact_refs, '{}'::text[]))
                     WITH ORDINALITY AS u(ref, ordinality);
            END;
            $exact$;
            """
            )
            self._command(
                r"""
            CREATE OR REPLACE FUNCTION @search_hybrid_fuse(
                query text,
                max_results integer DEFAULT 50,
                lexical_refs text[] DEFAULT NULL,
                semantic_legs jsonb DEFAULT '[]'::jsonb,
                exact_refs text[] DEFAULT NULL,
                priorities text[] DEFAULT NULL
            )
            RETURNS SETOF @search_text_hit
            LANGUAGE plpgsql
            STABLE
            AS $fuse$
            DECLARE
                per_source integer := least(greatest(coalesce(max_results, 50), 1), """
                + str(SEARCH_TEXT_MAX_RESULTS_CAP)
                + r""");
                requested_priority text;
                -- The app's sentence detector, mirrored: five or more words
                -- of which at least two are function words. A sentence keeps
                -- flat lexical weight; a term bag or identifier gets the
                -- BM25 head bonus below.
                query_words text[] := regexp_split_to_array(lower(btrim(coalesce(query, ''))), '\s+');
                query_is_sentence boolean;
            BEGIN
                """
                + priorities_guard_sql("search_hybrid_fuse")
                + r"""
                query_is_sentence := coalesce(array_length(query_words, 1), 0) >= 5 AND (
                    SELECT count(*) FROM unnest(query_words) AS w(word)
                    WHERE btrim(w.word, '.,!?;:''"') = ANY (ARRAY["""
                + SEARCH_SENTENCE_WORDS_SQL
                + r"""])
                ) >= 2;
                RETURN QUERY
                WITH lex AS (
                    SELECT u.ref, u.ordinality AS rnk
                    FROM unnest(coalesce(lexical_refs, '{}'::text[]))
                         WITH ORDINALITY AS u(ref, ordinality)
                ),
                sem_input AS (
                    SELECT j.ref, j.best, j.fuse, j.chunk_id
                    FROM jsonb_to_recordset(coalesce(semantic_legs, '[]'::jsonb))
                         AS j(ref text, best bigint, fuse double precision, chunk_id text)
                ),
                sem_ranked AS (
                    SELECT g.ref, g.chunk_id,
                           row_number() OVER (ORDER BY g.fuse DESC, g.best ASC) AS rnk
                    FROM (
                        SELECT j.ref,
                               min(j.best) AS best,
                               sum(j.fuse) AS fuse,
                               (array_agg(j.chunk_id ORDER BY j.best))[1] AS chunk_id
                        FROM sem_input j
                        GROUP BY j.ref
                    ) g
                ),
                exact_ranked AS (
                    SELECT u.ref, u.ordinality AS rnk
                    FROM unnest(coalesce(exact_refs, '{}'::text[]))
                         WITH ORDINALITY AS u(ref, ordinality)
                ),
                merged AS (
                    SELECT COALESCE(l.ref, s.ref, x.ref) AS ref,
                           s.chunk_id,
                           (COALESCE(
                                CASE WHEN NOT query_is_sentence AND l.rnk <= """
                + str(SEARCH_HYBRID_LEXICAL_HEAD_RANKS)
                + r""" THEN """
                + str(SEARCH_HYBRID_LEXICAL_HEAD_WEIGHT)
                + r""" ELSE 1.0 END / ("""
                + str(SEARCH_HYBRID_RRF_K)
                + r""" + l.rnk), 0) + """
                + str(SEARCH_HYBRID_SEMANTIC_WEIGHT)
                + r""" * COALESCE(1.0 / ("""
                + str(SEARCH_HYBRID_RRF_K)
                + r""" + s.rnk), 0) + """
                + str(SEARCH_HYBRID_EXACT_WEIGHT)
                + r""" * COALESCE(1.0 / ("""
                + str(SEARCH_HYBRID_RRF_K)
                + r""" + x.rnk), 0)) AS rrf
                    FROM lex l
                    FULL OUTER JOIN sem_ranked s ON s.ref = l.ref
                    FULL OUTER JOIN exact_ranked x ON x.ref = COALESCE(l.ref, s.ref)
                )
                SELECT COALESCE(tmap.source, t.source) AS source,
                       """ + exact_subsource_case + r""" AS subsource,
                       t.context,
                       COALESCE(t.actor, '') AS who,
                       t.event_ts AS occurred_at,
                       COALESCE(t.source_pk->>'account', t.metadata->>'account', '') AS account,
                       m.ref,
                       COALESCE(c.text, """ + preview_fn_sql + r"""(t.search_text, query)) AS text,
                       (-m.rrf)::real AS score,
                       t.event_ts,
                       COALESCE(t.title, '') AS title,
                       COALESCE(t.source_table, '') AS source_table,
                       t.source_pk,
                       t.priority::text AS priority
                FROM merged m
                -- Per-result primary-key probes avoid a 47M-row hash join.
                LEFT JOIN LATERAL (
                    SELECT te.* FROM @timeline_events te
                    WHERE te.adapter = split_part(m.ref, ':', 1)
                      AND te.event_id = substring(m.ref FROM length(split_part(m.ref, ':', 1)) + 2)
                    LIMIT 1
                ) t ON TRUE
                LEFT JOIN @search_chunks c ON c.chunk_id = m.chunk_id
                LEFT JOIN (VALUES """
                + adapter_source_values
                + r""") AS tmap(adapter, source) ON tmap.adapter = t.adapter
                WHERE t.event_id IS NOT NULL
                  AND (priorities IS NULL OR t.priority::text = ANY (priorities))
                ORDER BY m.rrf DESC, t.event_ts DESC
                LIMIT per_source;
            END;
            $fuse$;
            """
            )
            self._command(
                r"""
            CREATE OR REPLACE FUNCTION @search_hybrid(
                query text,
                query_embedding text,
                embedding_model text DEFAULT '"""
                + SEARCH_EMBEDDING_DEFAULT_MODEL
                + r"""',
                max_results integer DEFAULT 50,
                sources text[] DEFAULT NULL,
                since timestamptz DEFAULT NULL,
                query_embedding_alt text DEFAULT NULL,
                priorities text[] DEFAULT NULL
            )
            RETURNS SETOF @search_text_hit
            LANGUAGE plpgsql
            STABLE
            AS $hybrid$
            DECLARE
                per_source integer := least(greatest(coalesce(max_results, 50), 1), """
                + str(SEARCH_TEXT_MAX_RESULTS_CAP)
                + r""");
                lexical_refs text[];
                semantic_legs jsonb := '[]'::jsonb;
                one_semantic_leg jsonb;
                exact_refs text[];
                requested_priority text;
            BEGIN
                """
                + priorities_guard_sql("search_hybrid")
                + r"""
                IF query_embedding IS NULL OR trim(query_embedding) = '' THEN
                    RAISE EXCEPTION 'search_hybrid: query_embedding is required'
                        USING HINT = 'pass the query embedding as a vector literal; use search_text() for keyword-only search';
                END IF;
                SELECT array_agg(x.ref ORDER BY x.rnk)
                  INTO lexical_refs
                  FROM (
                    SELECT h.ref, row_number() OVER () AS rnk
                    FROM @search_text(query, per_source, sources, since, priorities) h
                  ) x;
                SELECT coalesce(jsonb_agg(to_jsonb(s)), '[]'::jsonb)
                  INTO semantic_legs
                  FROM @search_hybrid_semantic(
                      query_embedding, embedding_model, per_source, sources, since
                  ) s;
                IF query_embedding_alt IS NOT NULL AND trim(query_embedding_alt) <> '' THEN
                    SELECT coalesce(jsonb_agg(to_jsonb(s)), '[]'::jsonb)
                      INTO one_semantic_leg
                      FROM @search_hybrid_semantic(
                          query_embedding_alt, embedding_model, per_source, sources, since
                      ) s;
                    semantic_legs := semantic_legs || one_semantic_leg;
                END IF;
                SELECT array_agg(x.ref ORDER BY x.rnk)
                  INTO exact_refs
                  FROM @search_hybrid_exact(
                      query, per_source, sources, since, priorities
                  ) x;
                RETURN QUERY
                SELECT * FROM @search_hybrid_fuse(
                    query, per_source, lexical_refs, semantic_legs, exact_refs, priorities
                );
            END;
            $hybrid$;
            """
            )
            # The catalog cannot comment a function, and this one is a trap
            # from `pdw schema`: agents found it, called search_hybrid('terms',
            # 20) and got 42883. The comment travels with the function so any
            # \df / describe surface says so before the call is made.
            self._command(
                "COMMENT ON FUNCTION @search_hybrid(text, text, text, integer, text[], timestamptz, text, text[]) "
                "IS $c$NOT callable from plain SQL: takes a precomputed query embedding only the app can "
                "produce, so search_hybrid('terms', 20) fails with 42883. Hybrid retrieval is the search tool "
                "/ pdw search; from SQL use timeline.search_text (BM25) or timeline.search_text_exact.$c$"
            )
        # to_bm25query() resolves the timeline BM25 index by NAME, and the
        # EXECUTE'd branch SQL resolves the search_text_hit row type, both
        # through the CALLER's search_path. App/API query sessions run with
        # the default path ('"$user", public'), which stopped covering those
        # objects when the schema reorganization moved them out of public —
        # the per-branch exception guard then swallowed the lookup errors and
        # every search through the app silently returned zero rows. Pin the
        # function's own search_path so resolution never depends on the
        # session.
        #
        # Executed RAW: this statement quotes schema names, and in the public
        # namespace several of them ("apple_notes", "apple_messages", ...) are
        # ALSO canonical logical relation names, which _command's qualifier
        # would rewrite into schema.table references mid-list (a syntax
        # error). Everything here is already physical, so qualification is
        # both unnecessary and harmful.
        self._raw_command(self._search_text_alter_sql())

    def _search_text_alter_sql(self) -> str:
        function_path = self._search_path_sql().removeprefix("SET search_path TO ")
        search_schema = _identifier(self._object_schema("search_text"))
        return "; ".join(
            f'ALTER FUNCTION {search_schema}."{function_name}"(text, integer, text[], timestamptz, text[]) '
            f"SET search_path TO {function_path}"
            for function_name in ("search_text", "search_text_exact")
        )

    def _ensure_view(self, view: str, create_sql: str, *, dependents: tuple[str, ...] = ()) -> None:
        # CREATE OR REPLACE VIEW refuses to drop, rename, or retype an existing
        # view's columns, and this database is shared: another checkout running
        # a different revision can leave a view whose columns no longer match
        # this code's definition, wedging every ensure until the definitions
        # converge again. Views are derived state, so recreate from scratch when
        # in-place replacement is impossible. Plain DROP (no CASCADE) so a view
        # that grew dependent objects still fails loudly instead of silently
        # dropping them.
        #
        # `dependents` is the exception: views this ensure path recreates itself
        # a few statements later. Without naming them the plain DROP fails on
        # the dependency and every ensure wedges — the failure mode the drop is
        # meant to expose is only useful for objects nobody here rebuilds.
        try:
            self._command(create_sql)
        except psycopg2.errors.InvalidTableDefinition:
            for dependent in dependents:
                self._command(f"DROP VIEW IF EXISTS {self.sql_relation(dependent)}")
            self._command(f"DROP VIEW IF EXISTS {self.sql_relation(view)}")
            self._command(create_sql)
        self._ensure_relation_comments()

    #: Relations whose Postgres COMMENT this connection has already confirmed
    #: matches the catalog. Class-level default so every instance starts empty.
    _relation_comments_verified: frozenset[tuple[str, str]] = frozenset()

    def _ensure_relation_comments(self) -> None:
        """Publish the catalog's per-relation guidance as Postgres COMMENTs.

        The schema comment says which layer you are in; this says which relation
        in it to read first, and — for the Plaid-only marts_finance passthroughs
        — exactly what a domain-mart name is NOT delivering. Both are written
        once, in warehouse_catalog.json.

        Probe first and write only on drift, like the schema-comment sweep: an
        unconditional COMMENT ON per ensure would churn pg_description on every
        sensor tick. Relations already confirmed on this connection are not
        re-probed, so the sweep runs once per ensure path plus once more for
        each relation created after it — never once per statement.
        """
        expected: dict[tuple[str, str], tuple[str, str]] = {}
        for obj in CATALOG.objects:
            if not obj.comment or not obj.is_relation:
                continue
            rel = canonical_relation(obj.id).with_namespace(self._schema)
            key = (rel.schema, rel.name)
            if key in self._relation_comments_verified:
                continue
            expected[key] = (obj.kind, obj.comment)
        if not expected:
            return
        current = {
            (schema, name): comment
            for schema, name, comment in self._query(
                """
                SELECT n.nspname, c.relname, obj_description(c.oid, 'pg_class')
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ANY(%s) AND c.relkind IN ('r', 'v', 'm', 'p')
                """,
                (self.physical_schema_names(include_hidden=True),),
            )
        }
        verified = set(self._relation_comments_verified)
        for (schema, name), (kind, comment) in expected.items():
            if (schema, name) not in current:
                continue  # not provisioned in this deployment yet
            if current[(schema, name)] != comment:
                keyword = "VIEW" if kind == "view" else "TABLE"
                self._raw_command(
                    f"COMMENT ON {keyword} {_identifier(schema)}.{_identifier(name)} IS %s",
                    (comment,),
                )
            verified.add((schema, name))
        self._relation_comments_verified = frozenset(verified)

    def _ensure_clean_gmail_inbox_view(self) -> None:
        self._ensure_utf8_byte_prefix_function()
        self._ensure_view(
            "clean_gmail_inbox",
            """
            CREATE OR REPLACE VIEW @clean_gmail_inbox AS
            SELECT
                account,
                thread_id,
                max(internal_date) AS latest_at,
                (array_agg(from_address ORDER BY internal_date DESC, message_id ASC))[1] AS latest_from_address,
                (array_agg(subject ORDER BY internal_date DESC, message_id ASC))[1] AS subject,
                @utf8_byte_prefix(
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
            FROM @gmail_messages
            WHERE is_deleted = 0
              AND 'INBOX' = ANY(label_ids)
              AND NOT ('TRASH' = ANY(label_ids))
              AND NOT ('SPAM' = ANY(label_ids))
            GROUP BY account, thread_id
            """,
        )

    def _ensure_utf8_byte_prefix_function(self) -> None:
        self._command(
            """
            CREATE OR REPLACE FUNCTION @utf8_byte_prefix(value text, max_bytes integer)
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
        self._apply_catalog_grant("utf8_byte_prefix")

    def _ensure_clean_slack_inbox_view(self) -> None:
        self._ensure_view(
            "clean_slack_inbox",
            """
            CREATE OR REPLACE VIEW @clean_slack_inbox AS
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
            FROM @slack_account_state_item_rows
            WHERE is_deleted = 0
            """
        )

    def _ensure_clean_contacts_view(self) -> None:
        self._ensure_view(
            "clean_contacts",
            """
            CREATE OR REPLACE VIEW @clean_contacts AS
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
                raw_json,
                nicknames
            FROM @contact_cards
            WHERE is_deleted = 0
            UNION ALL
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
                raw_json,
                nicknames
            FROM @apple_contact_cards
            WHERE is_deleted = 0
            """
        )

    def _prepare_contacts_view_replacement(self) -> bool:
        expected_columns = (
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
            "groups",
            "dates",
            "photos",
            "notes",
            "source_updated_at",
            "synced_at",
            "raw_json",
            "nicknames",
        )
        contacts = canonical_relation("clean_contacts").with_namespace(self._schema)
        actual_columns = tuple(
            row[0]
            for row in self._query(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (contacts.schema, contacts.name),
            )
        )
        if not actual_columns or actual_columns == expected_columns:
            return False

        # contact_points depends on contacts, apple_messages depends on
        # contact_points, and the unified messages mart depends on
        # apple_messages. Remove only these derived views before replacing a
        # drifted contacts view; all three are recreated in this ensure path.
        for logical_name in (
            "marts_messages_messages",
            "clean_apple_messages",
            "clean_contact_points",
        ):
            relation = canonical_relation(logical_name).with_namespace(self._schema)
            self._raw_command(
                f"DROP VIEW IF EXISTS {_identifier(relation.schema)}.{_identifier(relation.name)}"
            )
        return True

    def _ensure_clean_contact_points_view(self) -> None:
        self._ensure_view(
            "clean_contact_points",
            """
            CREATE OR REPLACE VIEW @clean_contact_points AS
            SELECT DISTINCT
                c.source,
                c.account,
                c.source_kind,
                c.address_book_id,
                c.card_id,
                c.display_name,
                c.organization,
                c.source_updated_at,
                points.point_type,
                points.point_value,
                points.point_label,
                CASE
                    WHEN points.point_type = 'email' THEN lower(trim(points.point_value))
                    ELSE CASE
                        WHEN length(regexp_replace(points.point_value, '[^0-9]', '', 'g')) = 10
                            THEN '1' || regexp_replace(points.point_value, '[^0-9]', '', 'g')
                        ELSE regexp_replace(points.point_value, '[^0-9]', '', 'g')
                    END
                END AS normalized_value
            FROM @clean_contacts c
            CROSS JOIN LATERAL (
                SELECT 'email'::text AS point_type,
                       value->>'value' AS point_value,
                       COALESCE(value->>'type', value->>'label', '') AS point_label
                FROM jsonb_array_elements(COALESCE(c.emails, '[]'::jsonb)) value
                WHERE COALESCE(value->>'value', '') <> ''
                UNION
                SELECT 'email', c.primary_email, 'primary'
                WHERE c.primary_email <> ''
                UNION
                SELECT 'phone',
                       COALESCE(NULLIF(value->>'canonicalForm', ''), value->>'value'),
                       COALESCE(value->>'type', value->>'label', '')
                FROM jsonb_array_elements(COALESCE(c.phones, '[]'::jsonb)) value
                WHERE COALESCE(NULLIF(value->>'canonicalForm', ''), value->>'value', '') <> ''
                UNION
                SELECT 'phone', c.primary_phone, 'primary'
                WHERE c.primary_phone <> ''
            ) points
            WHERE points.point_value <> ''
            """,
        )

    def _ensure_clean_apple_messages_view(self) -> None:
        # Sender identity is a property of the HANDLE, not of the message: the
        # resolution below reads only h.address. Resolving it in a per-row
        # LATERAL therefore recomputed marts_contacts.contact_points — itself a
        # jsonb-expanding view over every contact card — once per message, about
        # 30 ms a row. In production a 30-day window of this view took 59
        # seconds and a full scan never finished inside any query timeout, which
        # is why nothing could be built on top of it. Resolving once per handle
        # (3,975 of them against 13,116 contact points, ~190 ms) returns rows
        # identical to the LATERAL — verified over a 21-day window, 1,480 rows,
        # zero differences — and takes the same 30-day window to 0.4 s and a
        # full 172k-row scan to 0.7 s.
        self._ensure_view(
            "clean_apple_messages",
            """
            CREATE OR REPLACE VIEW @clean_apple_messages AS
            WITH resolved_handles AS (
                SELECT DISTINCT ON (h.account, h.handle_id)
                    h.account,
                    h.handle_id,
                    h.address,
                    COALESCE(cp.source, '') AS contact_source,
                    COALESCE(cp.card_id, '') AS contact_card_id,
                    COALESCE(cp.display_name, '') AS contact_display_name
                FROM @apple_message_handles h
                LEFT JOIN @clean_contact_points cp
                  ON cp.point_type = CASE WHEN h.address LIKE '%@%' THEN 'email' ELSE 'phone' END
                 AND cp.normalized_value = CASE
                     WHEN h.address LIKE '%@%' THEN lower(trim(h.address))
                     WHEN length(regexp_replace(h.address, '[^0-9]', '', 'g')) = 10
                         THEN '1' || regexp_replace(h.address, '[^0-9]', '', 'g')
                     ELSE regexp_replace(h.address, '[^0-9]', '', 'g')
                 END
                ORDER BY
                    h.account,
                    h.handle_id,
                    (cp.source = 'apple_contacts') DESC,
                    cp.source_updated_at DESC,
                    cp.card_id
            )
            SELECT
                m.*,
                COALESCE(r.address, '') AS sender_address,
                CASE
                    WHEN m.is_from_me = 1 THEN 'me'
                    ELSE COALESCE(NULLIF(r.contact_display_name, ''), NULLIF(r.address, ''), m.handle_id)
                END AS sender_name,
                COALESCE(r.contact_source, '') AS contact_source,
                COALESCE(r.contact_card_id, '') AS contact_card_id
            FROM @apple_messages m
            LEFT JOIN resolved_handles r
              ON r.account = m.account AND r.handle_id = m.handle_id
            """,
            dependents=("marts_messages_messages",),
        )
        self._ensure_messages_mart_view_if_possible()

    def _ensure_clean_whatsapp_messages_view(self) -> None:
        # Ergonomic layer over whatsapp_messages: a single chat_kind so callers
        # never re-derive "is this a DM / group / status post" from chat_id, plus
        # resolved sender_name and chat_name. chat_kind reads the (now complete)
        # whatsapp_chats.chat_type, falling back to a JID-derived value so the
        # view is correct even before the chat backfill runs. The fallback CASE
        # mirrors events.chat_type_for_jid.
        self._ensure_view(
            "clean_whatsapp_messages",
            """
            CREATE OR REPLACE VIEW @clean_whatsapp_messages AS
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
            FROM @whatsapp_messages m
            LEFT JOIN @whatsapp_chats c ON c.account = m.account AND c.chat_id = m.chat_id
            LEFT JOIN @whatsapp_contacts cc ON cc.account = m.account AND cc.jid = m.chat_id
            LEFT JOIN LATERAL (
                SELECT p.phone_jid
                FROM @whatsapp_chat_participants p
                WHERE p.account = m.account
                  AND p.phone_jid <> ''
                  AND (p.participant_jid = m.sender_jid OR p.lid_jid = m.sender_jid)
                ORDER BY p.ingested_at DESC, p.chat_id
                LIMIT 1
            ) sender_alias ON TRUE
            LEFT JOIN @whatsapp_contacts ct
              ON ct.account = m.account
             AND ct.jid = COALESCE(NULLIF(sender_alias.phone_jid, ''), m.sender_jid)
            """,
            dependents=("marts_messages_messages",),
        )
        self._ensure_messages_mart_view_if_possible()

    def _ensure_messages_mart_view_if_possible(self) -> None:
        """marts_messages.messages: iMessage/SMS and WhatsApp on one column set.

        Before this the two per-source views shared exactly one column name
        (message_at), so "all my messages with X last month" meant hand-writing
        the cross-source UNION and its column map — the very thing the search
        contract tells agents never to do. Both sources are conformed here, and
        each keeps its own view for full provider detail.

        Guarded on both sources existing: it is ensured from whichever of the
        two per-source paths runs, and a deployment holding only one of them
        simply has no unified view yet.
        """
        if not all(
            self._relation_exists(name)
            for name in ("clean_apple_messages", "clean_whatsapp_messages")
        ):
            return
        # chat_kind is one vocabulary across sources: WhatsApp calls a DM
        # 'user' and Apple encodes it as chat.style = 45, and an agent asking
        # for direct messages must not have to know either. Apple's own kinds
        # (43/45) are the only two chat.db styles present. WhatsApp's remaining
        # kinds (status/broadcast/newsletter) are real distinctions with no
        # Apple counterpart, so they pass through unchanged.
        #
        # message_kind is likewise conformed, with source_message_kind keeping
        # the provider's own token so nothing is lost. Apple cannot say image
        # vs video at the message level (that lives in
        # base_apple_messages.attachments), so its media rows read
        # 'attachment'.
        self._ensure_view(
            "marts_messages_messages",
            """
            CREATE OR REPLACE VIEW @marts_messages_messages AS
            SELECT
                'apple_messages'::text AS source,
                t.account,
                COALESCE(cm.chat_id, '') AS chat_id,
                COALESCE(
                    NULLIF(c.display_name, ''), NULLIF(c.chat_identifier, ''),
                    NULLIF(t.sender_name, ''), NULLIF(t.sender_address, ''), t.service
                ) AS chat_name,
                CASE c.style WHEN 45 THEN 'direct' WHEN 43 THEN 'group' ELSE 'unknown' END AS chat_kind,
                t.message_id,
                t.sender_address,
                t.sender_name,
                t.is_from_me,
                t.body_text,
                CASE
                    WHEN t.associated_message_type <> 0 THEN 'reaction'
                    WHEN t.is_system_message = 1 OR t.is_service_message = 1 THEN 'system'
                    WHEN t.is_audio_message = 1 THEN 'audio'
                    WHEN t.cache_has_attachments <> 0 THEN 'attachment'
                    WHEN t.body_text <> '' THEN 'text'
                    ELSE 'other'
                END AS message_kind,
                ''::text AS source_message_kind,
                NULL::text AS media_type,
                t.service,
                NULLIF(t.reply_to_guid, '') AS reply_to_message_id,
                -- EVERY timestamp this view exposes is NULLIF'd against the
                -- epoch, never some of them: the base columns are NOT NULL and
                -- store "never" as 1970-01-01 (228 of 172,631 iMessage rows
                -- have no real send time, 172,608 have never been edited, 41%
                -- of recent rows were never read and 83% never delivered).
                -- Translating a subset would be worse than translating none —
                -- it is what would give one conformed column two spellings of
                -- unknown. Precedent: marts_ops.pipeline_health.
                NULLIF(t.message_at, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS message_at,
                NULLIF(t.date_edited, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS edited_at,
                NULLIF(t.date_read, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS read_at,
                NULLIF(t.date_delivered, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS delivered_at,
                t.is_deleted
            FROM @clean_apple_messages t
            LEFT JOIN LATERAL (
                SELECT min(chat_id) AS chat_id
                FROM @apple_message_chat_messages
                WHERE account = t.account AND message_id = t.message_id
            ) cm ON TRUE
            LEFT JOIN @apple_message_chats c
              ON c.account = t.account AND c.chat_id = cm.chat_id
            UNION ALL
            SELECT
                'whatsapp'::text AS source,
                w.account,
                w.chat_id,
                COALESCE(w.chat_name, w.chat_id) AS chat_name,
                CASE w.chat_kind WHEN 'user' THEN 'direct' ELSE w.chat_kind END AS chat_kind,
                w.message_id,
                w.sender_jid AS sender_address,
                w.sender_name,
                w.is_from_me,
                w.body_text,
                CASE w.message_kind
                    WHEN 'text' THEN 'text'
                    WHEN 'image' THEN 'image'
                    WHEN 'video' THEN 'video'
                    WHEN 'voice' THEN 'audio'
                    WHEN 'audio' THEN 'audio'
                    WHEN 'document' THEN 'document'
                    WHEN 'sticker' THEN 'sticker'
                    WHEN 'reaction' THEN 'reaction'
                    WHEN 'encReactionMessage' THEN 'reaction'
                    WHEN 'revoke' THEN 'revoked'
                    ELSE 'other'
                END AS message_kind,
                w.message_kind AS source_message_kind,
                NULLIF(w.media_type, '') AS media_type,
                NULL::text AS service,
                NULLIF(w.quoted_message_id, '') AS reply_to_message_id,
                NULLIF(w.message_at, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS message_at,
                NULLIF(w.edited_at, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS edited_at,
                -- WhatsApp exposes no read/delivered receipt to a linked device.
                NULL::timestamptz AS read_at,
                NULL::timestamptz AS delivered_at,
                w.is_deleted
            FROM @clean_whatsapp_messages w
            """,
        )

    def _ensure_files_mart_views(self) -> None:
        """marts_files.attachments: one entry point for every stored attachment.

        Four sources hold attachment bytes -- Gmail, WhatsApp, iMessage and
        Apple Notes -- each in its own raw table with its own column names
        (`size` vs `size_bytes`, `storage_status = 'stored'` vs `is_missing =
        0`, `mime_type` vs `content_type`). Until this view every attachment
        enrichment pass carried a per-source descriptor naming the raw table,
        which is the shape C5 forbids: a receipt that arrived over WhatsApp was
        invisible to the receipt pass because that pass only knew Gmail, and
        Apple Notes attachments were enriched by nothing at all.

        **This view is the INPUT to file, audio and text enrichment and to
        receipt evidence checks**, so a new attachment source is one UNION
        branch here, never a new scan. Any source's ensure_* path may run
        first, so every branch's table is ensured before the union.
        """
        self._ensure_table_group(
            [
                "gmail_attachments",
                "whatsapp_media_items",
                "apple_message_attachments",
                "apple_note_attachments",
                "apple_note_revisions",
            ]
        )
        epoch = "TIMESTAMPTZ '1970-01-01 00:00:00+00'"
        self._ensure_view(
            "marts_files_attachments",
            f"""
            CREATE OR REPLACE VIEW @marts_files_attachments AS
            SELECT
                'gmail'::text AS source,
                a.account,
                a.message_id AS parent_id,
                a.attachment_id,
                a.filename,
                a.mime_type,
                a.size::bigint AS size_bytes,
                a.content_sha256,
                (CASE WHEN a.is_deleted = 0 AND a.content_sha256 <> '' AND a.storage_status = 'stored'
                      THEN 1 ELSE 0 END)::bigint AS is_stored,
                a.is_deleted::bigint AS is_deleted,
                NULLIF(a.internal_date, {epoch}) AS occurred_at,
                a.storage_backend, a.storage_key, a.storage_file_id, a.storage_url,
                NULLIF(a.synced_at, {epoch}) AS ingested_at
            FROM @gmail_attachments a
            UNION ALL
            SELECT
                'whatsapp'::text,
                a.account,
                a.message_id,
                a.message_id,
                a.filename,
                a.mime_type,
                a.size_bytes::bigint,
                a.content_sha256,
                (CASE WHEN a.is_missing = 0 AND a.content_sha256 <> '' THEN 1 ELSE 0 END)::bigint,
                0::bigint,
                NULLIF(a.message_at, {epoch}),
                a.storage_backend, a.storage_key, a.storage_file_id, a.storage_url,
                NULLIF(a.ingested_at, {epoch})
            FROM @whatsapp_media_items a
            UNION ALL
            SELECT
                'apple_messages'::text,
                a.account,
                a.message_id,
                a.attachment_id,
                a.filename,
                a.mime_type,
                a.size_bytes::bigint,
                a.content_sha256,
                (CASE WHEN a.is_missing = 0 AND a.content_sha256 <> '' THEN 1 ELSE 0 END)::bigint,
                0::bigint,
                NULLIF(a.created_at, {epoch}),
                a.storage_backend, a.storage_key, a.storage_file_id, a.storage_url,
                NULLIF(a.ingested_at, {epoch})
            FROM @apple_message_attachments a
            UNION ALL
            SELECT
                'apple_notes'::text,
                a.account,
                a.note_id,
                a.attachment_id,
                a.filename,
                a.content_type,
                a.size_bytes::bigint,
                a.content_sha256,
                (CASE WHEN a.is_missing = 0 AND a.content_sha256 <> '' THEN 1 ELSE 0 END)::bigint,
                0::bigint,
                NULLIF(r.modified_at, {epoch}),
                a.storage_backend, a.storage_key, a.storage_file_id, a.storage_url,
                NULLIF(a.ingested_at, {epoch})
            FROM @apple_note_attachments a
            LEFT JOIN @apple_note_revisions r
              ON r.account = a.account AND r.note_id = a.note_id AND r.revision_id = a.revision_id
            """,
        )

    def _ensure_voice_memos_mart_views(self) -> None:
        """marts_voice_memos.*: one entry point for every voice recording.

        Two unrelated sources feed this domain — base_apple_voice_memos.files
        and base_alice_voice_recordings.recordings — and the domain had a full
        derived layer with no mart at all, so "my voice memos" had no correct
        entry point: the timeline's voice_memos source missed every Alice
        recording, and transcript/participants/action items were folded into
        search_text without ever being columns.

        This is also where "latest enrichment per recording" lives.
        derived_voice_memos.enrichments is keyed by
        (source, account, recording_id, provider, model, prompt_version) — 802
        rows for 597 recordings — so every consumer had to re-derive the
        DISTINCT ON, and three of them had copy-pasted it.

        **This view is the INPUT to transcription and enrichment, not just an
        output.** It used to hardcode NULL transcript/summary for the Alice
        branch while both runners scanned base_apple_voice_memos.files
        directly, which made the NULLs self-fulfilling: 53 Alice recordings, 0
        transcripts, 0 summaries, with every enforced registry green. The
        sources are conformed here once and both runners read the union, so a
        third voice source is transcribed by existing code.
        """
        # Both sources' tables must exist for the union, and either source's
        # ensure_* path can be the one that runs. Declaring the dependency the
        # way ensure_finance_tables declares its Plaid tables keeps a
        # single-source ensure from failing on a missing relation.
        self._ensure_table_group(
            [
                "apple_voice_memos_files",
                "apple_voice_memos_transcription_runs",
                "apple_voice_memos_transcript_segments",
                "apple_voice_memos_enrichments",
                "alice_voice_recordings",
                "apple_note_attachments",
                "apple_note_revisions",
            ]
        )
        # Before the view, and in the one path both sources' ensure_* reach:
        # the view joins the derived tables on `source`, so the column has to
        # exist whichever source provisioned the domain first.
        self._migrate_voice_derived_tables_to_source_keyed()
        self._ensure_view(
            "marts_voice_memos_recordings",
            """
            CREATE OR REPLACE VIEW @marts_voice_memos_recordings AS
            WITH recordings AS (
                -- The conformed shape every voice source is mapped onto. Each
                -- source answers the same questions or says NULL; nothing here
                -- is a plausible-looking default.
                --
                -- Absence is stored as the epoch, not NULL: these columns are
                -- NOT NULL, so a recording with no known date reads
                -- 1970-01-01 and sorts oldest instead of unknown. Translating
                -- the sentinel is part of conforming the sources, not a
                -- nicety -- marts_ops.pipeline_health does the same. Every
                -- exposed timestamp is translated, or none: a view that
                -- translates one column and not its sibling manufactures an
                -- inconsistency the sources do not have.
                SELECT
                    'apple_voice_memos'::text AS source,
                    f.account,
                    f.recording_id,
                    NULLIF(f.recorded_at, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS recorded_at,
                    -- The uploader records the app's own duration for
                    -- recordings whose metadata carried one; older exports
                    -- have none.
                    CASE
                        WHEN left(f.raw_metadata_json, 1) = '{'
                            THEN (f.raw_metadata_json::jsonb -> 'recording' ->> 'duration_seconds')::numeric
                    END AS duration_seconds,
                    f.title,
                    f.filename,
                    f.content_type,
                    f.size_bytes,
                    f.content_sha256,
                    f.storage_backend,
                    f.storage_key,
                    f.storage_file_id,
                    f.storage_url,
                    NULL::text AS recording_url,
                    f.is_deleted,
                    NULLIF(f.ingested_at, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS ingested_at
                FROM @apple_voice_memos_files f
                UNION ALL
                SELECT
                    'alice_voice_recordings'::text AS source,
                    r.account,
                    r.recording_id,
                    NULLIF(r.recorded_at, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS recorded_at,
                    NULLIF(r.duration_seconds, 0)::numeric AS duration_seconds,
                    r.title,
                    r.filename,
                    r.content_type,
                    r.size_bytes,
                    r.content_sha256,
                    r.storage_backend,
                    r.storage_key,
                    r.storage_file_id,
                    r.storage_url,
                    NULLIF(r.recording_page_url, '') AS recording_url,
                    -- The Alice archive never tombstones a recording.
                    0::bigint AS is_deleted,
                    NULLIF(r.ingested_at, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS ingested_at
                FROM @alice_voice_recordings r
                UNION ALL
                SELECT * FROM (
                -- Audio recorded or saved inside Apple Notes (call recordings,
                -- voicemails, the Notes app's own audio recorder). The bytes
                -- were uploaded and stored for months while nothing knew they
                -- were voice: 40 attachments, 10 recordings, 0 transcripts,
                -- the Alice defect one source later. An attachment appears
                -- once per note REVISION with the same sha, so it is collapsed
                -- to one recording on the newest revision. Core Data stamps
                -- (seconds since 2001-01-01) are the recording's own creation
                -- time; the revision's date is only the fallback.
                SELECT DISTINCT ON (a.account, a.attachment_id)
                    'apple_notes'::text AS source,
                    a.account,
                    a.attachment_id AS recording_id,
                    COALESCE(
                        CASE
                            WHEN left(a.raw_metadata_json, 1) = '{'
                             AND (a.raw_metadata_json::jsonb -> 'raw' ->> 'ZCREATIONDATE') ~ '^[0-9.]+$'
                                THEN TIMESTAMPTZ '2001-01-01 00:00:00+00'
                                     + make_interval(secs => (a.raw_metadata_json::jsonb -> 'raw' ->> 'ZCREATIONDATE')::numeric)
                        END,
                        NULLIF(n.modified_at, TIMESTAMPTZ '1970-01-01 00:00:00+00')
                    ) AS recorded_at,
                    CASE
                        WHEN left(a.raw_metadata_json, 1) = '{'
                         AND (a.raw_metadata_json::jsonb -> 'raw' ->> 'ZDURATION') ~ '^[0-9.]+$'
                            THEN NULLIF((a.raw_metadata_json::jsonb -> 'raw' ->> 'ZDURATION')::numeric, 0)
                    END AS duration_seconds,
                    COALESCE(
                        CASE
                            WHEN left(a.raw_metadata_json, 1) = '{'
                                THEN NULLIF(a.raw_metadata_json::jsonb -> 'raw' ->> 'ZTITLE', '')
                        END,
                        NULLIF(n.title, ''),
                        a.filename
                    ) AS title,
                    a.filename,
                    a.content_type,
                    a.size_bytes,
                    a.content_sha256,
                    a.storage_backend,
                    a.storage_key,
                    a.storage_file_id,
                    a.storage_url,
                    NULL::text AS recording_url,
                    0::bigint AS is_deleted,
                    NULLIF(a.ingested_at, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS ingested_at
                FROM @apple_note_attachments a
                LEFT JOIN @apple_note_revisions n
                  ON n.account = a.account AND n.note_id = a.note_id AND n.revision_id = a.revision_id
                WHERE a.is_missing = 0
                  AND a.content_sha256 <> ''
                  AND (
                      lower(a.content_type) LIKE 'audio/%'
                      OR lower(a.filename) ~ '\\.(m4a|mp3|caf|wav|aac|aiff?|opus|ogg)$'
                  )
                ORDER BY a.account, a.attachment_id, n.modified_at DESC, a.revision_id DESC
                ) AS note_audio
            ),
            latest_enrichment AS (
                -- THE definition of "the enrichment that counts" for a
                -- recording: newest completed attempt per recording, tie-broken
                -- deterministically. derived_voice_memos.enrichments is keyed by
                -- (source, account, recording_id, provider, model,
                -- prompt_version), so anything that skips this de-duplication
                -- double-counts.
                SELECT DISTINCT ON (source, account, recording_id)
                    source,
                    account,
                    recording_id,
                    provider,
                    model,
                    prompt_version,
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
                    created_at
                FROM @apple_voice_memos_enrichments
                WHERE status = 'completed'
                ORDER BY source, account, recording_id, created_at DESC, provider DESC, model DESC, prompt_version DESC
            ),
            latest_transcription AS (
                SELECT DISTINCT ON (source, account, recording_id)
                    source,
                    account,
                    recording_id,
                    provider,
                    transcript_text,
                    completed_at
                FROM @apple_voice_memos_transcription_runs
                WHERE status = 'completed'
                ORDER BY source, account, recording_id, completed_at DESC, requested_at DESC, provider DESC
            )
            SELECT
                v.source,
                v.account,
                v.recording_id,
                v.recorded_at,
                v.duration_seconds,
                COALESCE(NULLIF(en.title, ''), NULLIF(v.title, ''), v.filename) AS title,
                v.title AS source_title,
                v.filename,
                NULLIF(en.summary, '') AS summary,
                COALESCE(NULLIF(en.transcript, ''), NULLIF(run.transcript_text, '')) AS transcript,
                NULLIF(en.participants_json, '') AS participants_json,
                NULLIF(en.action_items_json, '') AS action_items_json,
                NULLIF(en.evidence_json, '') AS evidence_json,
                NULLIF(en.calendar_event_id, '') AS calendar_event_id,
                en.calendar_confidence,
                NULLIF(en.start_at, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS meeting_start_at,
                NULLIF(en.end_at, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS meeting_end_at,
                en.title AS enrichment_title,
                en.provider AS enrichment_provider,
                en.model AS enrichment_model,
                en.prompt_version AS enrichment_prompt_version,
                NULLIF(en.created_at, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS enriched_at,
                run.provider AS transcript_provider,
                v.content_type,
                v.size_bytes,
                v.content_sha256,
                v.storage_backend,
                v.storage_key,
                v.storage_file_id,
                v.storage_url,
                v.recording_url,
                v.is_deleted,
                v.ingested_at,
                -- Appended, never inserted: CREATE OR REPLACE VIEW only
                -- tolerates new columns at the end.
                NULLIF(run.completed_at, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS transcribed_at
            FROM recordings v
            LEFT JOIN latest_enrichment en
              ON en.source = v.source AND en.account = v.account AND en.recording_id = v.recording_id
            LEFT JOIN latest_transcription run
              ON run.source = v.source AND run.account = v.account AND run.recording_id = v.recording_id
            """,
            dependents=(
                "marts_voice_memos_transcript_segments",
                "clean_calendar_with_transcripts",
                "clean_transcripts_no_calendar_match",
            ),
        )
        self._ensure_view(
            "marts_voice_memos_transcript_segments",
            """
            CREATE OR REPLACE VIEW @marts_voice_memos_transcript_segments AS
            SELECT
                s.source,
                s.account,
                s.recording_id,
                r.recorded_at,
                r.title AS recording_title,
                s.provider,
                s.provider_transcript_id,
                s.segment_index,
                s.speaker_label,
                s.start_ms,
                s.end_ms,
                s.confidence,
                s.text,
                r.recorded_at + make_interval(secs => s.start_ms / 1000.0) AS spoken_at
            FROM @apple_voice_memos_transcript_segments s
            LEFT JOIN @marts_voice_memos_recordings r
              ON r.source = s.source
             AND r.account = s.account
             AND r.recording_id = s.recording_id
            """,
        )
        # The marts_calendar transcript views consume the mart, so they are
        # rebuilt here: the column-drift path above may have had to drop them
        # to replace the mart, and this is the one place that always runs
        # afterwards.
        self._ensure_calendar_transcript_views()

    def _ensure_photo_marts_views(self) -> None:
        # marts_photos.files: every rendition from every photo source, one
        # relation. Generated from PHOTO_SOURCE_RELATIONS so registering a new
        # photo source automatically adds its raw table to the union.
        per_source_selects = []
        for table in PHOTO_SOURCE_RELATIONS.values():
            per_source_selects.append(
                f"""
            SELECT
                f.source, f.account, f.source_native_id, f.role, f.filename,
                f.mime_type, f.size_bytes, f.width, f.height, f.content_sha256,
                f.captured_at, f.capture_tz_offset, f.camera_make, f.camera_model,
                f.storage_backend, f.storage_key, f.storage_file_id, f.storage_url,
                f.is_deleted, f.ingested_at,
                COALESCE(l.photo_id, '') AS photo_id,
                COALESCE(l.match_method, '') AS match_method,
                COALESCE(l.match_score, 0) AS match_score
            FROM @{table} f
            LEFT JOIN @photo_asset_files l
              ON l.source = f.source AND l.account = f.account
             AND l.source_native_id = f.source_native_id
             AND l.content_sha256 = f.content_sha256
                """
            )
        union_sql = "\n            UNION ALL\n            ".join(per_source_selects)
        self._ensure_view(
            "photo_files",
            f"""
            CREATE OR REPLACE VIEW @photo_files AS
            {union_sql}
            """,
        )
        # marts_photos.photos: one row per logical photo, with rendition/source counts
        # and the newest AI caption (enrichment keyed by the thumbnail or best
        # file sha).
        self._ensure_view(
            "clean_photos",
            """
            CREATE OR REPLACE VIEW @clean_photos AS
            SELECT
                a.photo_id,
                a.account,
                a.kind,
                a.capture_ts,
                a.capture_tz_offset,
                a.latitude,
                a.longitude,
                a.camera_make,
                a.camera_model,
                a.width,
                a.height,
                a.best_file_sha256,
                a.best_file_mime_type,
                a.best_file_filename,
                a.thumbnail_content_sha256,
                a.thumbnail_content_type,
                a.thumbnail_storage_file_id,
                COALESCE(l.rendition_count, 0) AS rendition_count,
                COALESCE(l.source_count, 0) AS source_count,
                COALESCE(e.caption, '') AS caption,
                a.created_at,
                a.updated_at
            FROM @photo_assets a
            LEFT JOIN (
                SELECT photo_id, count(*) AS rendition_count, count(DISTINCT source) AS source_count
                FROM @photo_asset_files
                GROUP BY photo_id
            ) l ON l.photo_id = a.photo_id
            LEFT JOIN LATERAL (
                SELECT e.text AS caption
                FROM @file_attachment_enrichments e
                WHERE e.content_sha256 != ''
                  AND e.content_sha256 IN (a.thumbnail_content_sha256, a.best_file_sha256)
                  AND e.text != ''
                ORDER BY e.updated_at DESC
                LIMIT 1
            ) e ON TRUE
            """,
        )
        # marts_photos.canonical_renditions: exactly one enrichable still per
        # logical photo — the identity runner's 1280px JPEG thumbnail — shaped
        # for FileEnrichmentSource's default column names. Video-only assets
        # and assets whose thumbnail has not been generated yet are excluded.
        self._ensure_view(
            "photo_canonical_renditions",
            """
            CREATE OR REPLACE VIEW @photo_canonical_renditions AS
            SELECT
                a.photo_id,
                a.account,
                a.thumbnail_content_sha256 AS content_sha256,
                COALESCE(NULLIF(a.best_file_filename, ''), a.photo_id || '.jpg') AS filename,
                a.thumbnail_content_type AS mime_type,
                a.thumbnail_size_bytes AS size_bytes,
                a.thumbnail_storage_backend AS storage_backend,
                a.thumbnail_storage_key AS storage_key,
                a.thumbnail_storage_file_id AS storage_file_id,
                a.thumbnail_storage_url AS storage_url,
                a.capture_ts
            FROM @photo_assets a
            WHERE a.kind = 'image' AND a.thumbnail_content_sha256 != ''
            """,
        )

    def _ensure_ai_conversation_events_view(self) -> None:
        union_sql = "\n            UNION ALL\n            ".join(
            f"SELECT * FROM {self.sql_relation(table)}" for table in _AI_CONVERSATION_EVENT_TABLES
        )
        self._ensure_view(
            "ai_conversation_events",
            f"""
            CREATE OR REPLACE VIEW @ai_conversation_events AS
            {union_sql}
            """,
        )

    def _ensure_clean_agent_sessions_view(self) -> None:
        # Session-level roll-up over the per-line event log. Header fields take
        # the first/last non-empty value seen so a session split across batches
        # converges; counts and token sums aggregate the whole session, which a
        # stored-aggregate upsert could not do correctly across batches.
        self._ensure_view(
            "clean_agent_sessions",
            """
            CREATE OR REPLACE VIEW @clean_agent_sessions AS
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
            FROM @ai_conversation_events
            GROUP BY source, session_id
            """
        )

    def _ensure_clean_calendar_transcript_views_if_possible(self) -> None:
        """Entry point for the calendar side of the voice-memo transcripts.

        Both views read the recording side through marts_voice_memos.recordings,
        so the mart is ensured first and owns their (re)creation — see
        _ensure_voice_memos_mart_views, which calls the body below. The call
        goes one way only: wrapper → mart → body.
        """
        if not self._relation_exists("calendar_events"):
            return
        self._ensure_voice_memos_mart_views()

    def _ensure_calendar_transcript_views(self) -> None:
        if not all(
            self._relation_exists(table)
            for table in ("calendar_events", "apple_voice_memos_files", "apple_voice_memos_enrichments")
        ):
            return
        self._ensure_view(
            "clean_calendar_with_transcripts",
            """
            CREATE OR REPLACE VIEW @clean_calendar_with_transcripts AS
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
                FROM @calendar_events
                WHERE is_deleted = 0
                ORDER BY event_id, synced_at DESC, account DESC, calendar_id DESC
            ),
            enriched_recordings AS (
                SELECT
                    account,
                    recording_id,
                    recorded_at,
                    title AS resolved_title,
                    enrichment_title,
                    COALESCE(calendar_event_id, '') AS calendar_event_id,
                    COALESCE(calendar_confidence, 0) AS calendar_confidence,
                    meeting_start_at AS start_at,
                    meeting_end_at AS end_at,
                    COALESCE(participants_json, '') AS participants_json,
                    COALESCE(transcript, '') AS transcript,
                    COALESCE(summary, '') AS summary,
                    COALESCE(action_items_json, '') AS action_items_json,
                    COALESCE(evidence_json, '') AS evidence_json,
                    enriched_at
                FROM @marts_voice_memos_recordings
                -- enrichment_provider, not enriched_at: the timestamp is
                -- NULL-when-sentinel, and "has a completed enrichment" must not
                -- ride on whether that enrichment carried a real clock reading.
                WHERE source = 'apple_voice_memos' AND enrichment_provider IS NOT NULL
            )
            SELECT
                c.calendar_account AS calendar_account,
                e.account AS recording_account,
                c.calendar_id,
                c.event_id,
                e.recording_id,
                COALESCE(NULLIF(e.enrichment_title, ''), c.summary) AS title,
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
            INNER JOIN enriched_recordings AS e
              ON c.event_id = e.calendar_event_id
            WHERE e.calendar_event_id != ''
            """
        )
        self._ensure_view(
            "clean_transcripts_no_calendar_match",
            """
            CREATE OR REPLACE VIEW @clean_transcripts_no_calendar_match AS
            WITH latest_calendar_events AS (
                SELECT event_id
                FROM @calendar_events
                WHERE is_deleted = 0
                GROUP BY event_id
            ),
            enriched_recordings AS (
                SELECT
                    account,
                    recording_id,
                    recorded_at,
                    title AS resolved_title,
                    enrichment_title,
                    COALESCE(calendar_event_id, '') AS calendar_event_id,
                    COALESCE(calendar_confidence, 0) AS calendar_confidence,
                    meeting_start_at AS start_at,
                    meeting_end_at AS end_at,
                    COALESCE(participants_json, '') AS participants_json,
                    COALESCE(transcript, '') AS transcript,
                    COALESCE(summary, '') AS summary,
                    COALESCE(action_items_json, '') AS action_items_json,
                    COALESCE(evidence_json, '') AS evidence_json,
                    enriched_at
                FROM @marts_voice_memos_recordings
                -- enrichment_provider, not enriched_at: the timestamp is
                -- NULL-when-sentinel, and "has a completed enrichment" must not
                -- ride on whether that enrichment carried a real clock reading.
                WHERE source = 'apple_voice_memos' AND enrichment_provider IS NOT NULL
            )
            SELECT
                e.account,
                e.recording_id,
                e.recorded_at,
                e.resolved_title AS title,
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
            FROM enriched_recordings AS e
            LEFT JOIN latest_calendar_events AS c
              ON e.calendar_event_id = c.event_id
            WHERE e.calendar_event_id = ''
               OR e.calendar_confidence <= 0
               OR c.event_id IS NULL
            """
        )

    def _relation_exists(self, relation_name: str) -> bool:
        try:
            rel = canonical_relation(relation_name).with_namespace(self._schema)
        except KeyError:
            schemas = self.physical_schema_names(include_hidden=True) + [self._schema]
            name = relation_name
        else:
            schemas = [rel.schema]
            name = rel.name
        rows = self._query(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = ANY(%s)
              AND table_name = %s
            UNION ALL
            SELECT 1
            FROM information_schema.views
            WHERE table_schema = ANY(%s)
              AND table_name = %s
            LIMIT 1
            """,
            (schemas, name, schemas, name),
        )
        return bool(rows)

    def _slack_account_state_items_select_sql(self, *, scoped: bool = False) -> str:
        """The inbox snapshot for one account/team.

        With ``scoped`` the conversation set is restricted to an array bound as
        the fifth parameter, which is what makes the incremental refresh cheap:
        every branch below joins through ``current_conversations``, so the scan
        of ``@slack_messages`` is bounded by the conversations that changed
        rather than by every member conversation's last thirty days.
        """
        last_read = _json_numeric("c.raw_json", "last_read")
        conversation_scope = "AND c.conversation_id = ANY(%s::text[])" if scoped else ""
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
                    FROM @slack_messages AS m, vars
                    WHERE m.account = vars.account
                      AND m.team_id = vars.team_id
                      AND m.is_deleted = 0
                      AND m.message_datetime >= now() - INTERVAL '30 days'
                ),
                current_conversations AS NOT MATERIALIZED (
                    SELECT c.*, {last_read} AS last_read_ts
                    FROM @slack_conversations AS c, vars
                    WHERE c.account = vars.account
                      AND c.team_id = vars.team_id
                      AND c.is_archived = 0
                      AND (c.is_member = 1 OR c.is_im = 1 OR c.is_mpim = 1)
                      {conversation_scope}
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
            INNER JOIN @slack_account_identities AS i
              ON i.account = vars.account AND i.team_id = vars.team_id
            INNER JOIN current_conversations AS c
              ON i.account = c.account AND i.team_id = c.team_id
            INNER JOIN recent_messages AS m
              ON c.account = m.account AND c.team_id = m.team_id AND c.conversation_id = m.conversation_id
            LEFT JOIN @slack_users AS u
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
            INNER JOIN @slack_account_identities AS i
              ON i.account = vars.account AND i.team_id = vars.team_id
            INNER JOIN current_conversations AS c
              ON i.account = c.account AND i.team_id = c.team_id
            INNER JOIN recent_messages AS m
              ON c.account = m.account AND c.team_id = m.team_id AND c.conversation_id = m.conversation_id
            LEFT JOIN @slack_users AS u
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
            INNER JOIN @slack_account_identities AS i
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
            LEFT JOIN @slack_users AS ru
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
            INNER JOIN @slack_account_identities AS i
              ON i.account = vars.account AND i.team_id = vars.team_id
            INNER JOIN current_conversations AS c
              ON i.account = c.account AND i.team_id = c.team_id
            INNER JOIN recent_messages AS m
              ON c.account = m.account AND c.team_id = m.team_id AND c.conversation_id = m.conversation_id
            LEFT JOIN @slack_users AS u
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

    def _expand_relations(self, sql: str) -> str:
        return expand_relations(sql, namespace=self._schema)

    def _raw_command(self, sql: str, params: Sequence[Any] | None = None) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(sql, params)

    def _command(self, sql: str, params: Sequence[Any] | None = None) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(self._expand_relations(sql), params)

    def _query(self, sql: str, params: Sequence[Any] | None = None) -> list[tuple[Any, ...]]:
        with self._connection.cursor() as cursor:
            cursor.execute(self._expand_relations(sql), params)
            return cursor.fetchall()

    def _query_dicts(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        with self._connection.cursor() as cursor:
            cursor.execute(self._expand_relations(sql), params)
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
            INSERT INTO {self.sql_relation(table)} AS target ({column_sql})
            VALUES %s
            {_upsert_clause(table, spec, columns, target_alias="target")}
        """
        with self._connection.cursor() as cursor:
            execute_values(cursor, self._expand_relations(sql), rows, template=template, page_size=POSTGRES_INSERT_PAGE_SIZES.get(table, 1000))

    def _insert_rows(self, table: str, rows: list[dict[str, Any]], columns: tuple[str, ...]) -> None:
        self._insert(
            table,
            [
                tuple(_normalize_insert_value(row[column], table=table, column=column) for column in columns)
                for row in rows
            ],
            columns,
        )


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _postgres_type(column: str, *, table: str | None = None) -> str:
    # The timeline's priority tier is a self-describing native enum (see
    # timeline.py). The marker resolves to the timeline schema when the CREATE
    # TABLE runs through relation expansion.
    if table == "timeline_events" and column == "priority":
        return "@timeline_priority"
    if _is_jsonb_column(table, column):
        return "jsonb"
    if column in ARRAY_COLUMNS:
        return "text[]"
    # Per-table TEXT/NUMERIC run before the global name sets: ledger column
    # names collide with raw-source float columns, and one source's label
    # column collides with another source's numeric column of the same name.
    if _is_text_column(table, column):
        return "text"
    if _is_numeric_column(table, column):
        return "numeric"
    if column in DATE_COLUMNS:
        return "date"
    if column in TIMESTAMP_COLUMNS:
        return "timestamptz"
    if column in FLOAT_COLUMNS:
        return "double precision"
    if column in INTEGER_COLUMNS:
        return "bigint"
    return "text"


#: Columns whose "absent" value is -1 rather than the warehouse's usual 0,
#: because 0 is a meaningful reading there. The host-saturation gauges beside
#: the search benchmark's latency (C6) are the case: a zero reads as an IDLE
#: host, which is the one verdict C6 acts on, so an unmeasured field must not
#: be able to produce it. Until 2026-08-28 only the hand-written migration used
#: -1 and a freshly created table used 0, so a new warehouse's first benchmark
#: row would have claimed an idle host it never measured.
UNMEASURED_SENTINEL_COLUMNS_BY_TABLE: dict[str, tuple[str, ...]] = {
    "search_benchmark_runs": (
        "io_pressure_full_avg10",
        "cpu_pressure_some_avg10",
        "load_1m",
        "cpu_count",
    ),
}


def _default_sql(column: str, *, table: str | None = None) -> str:
    if column in UNMEASURED_SENTINEL_COLUMNS_BY_TABLE.get(table or "", ()):
        return "-1"
    if table == "timeline_events" and column == "priority":
        # A fail-loud sentinel, not a tier. Every adapter emits one of the five
        # real tiers, so a row can only carry 'unclassified' if it was inserted
        # outside the sync engine — i.e. it marks a bug rather than filing the
        # row silently under a plausible-looking tier. Enforced by
        # tests/test_timeline.py::test_no_adapter_can_emit_the_unclassified_sentinel;
        # prod carried 0 such rows at 2026-08-23.
        return "'unclassified'"
    if _is_jsonb_column(table, column):
        if column in JSONB_ARRAY_COLUMNS_BY_TABLE.get(table or "", set()):
            return "'[]'::jsonb"
        return "'{}'::jsonb"
    if column in ARRAY_COLUMNS:
        return "'{}'::text[]"
    if _is_text_column(table, column):
        return "''"
    if _is_numeric_column(table, column):
        return "0"
    if column in DATE_COLUMNS:
        return "'1970-01-01'::date"
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
    # Per-conversation Slack errors record status/error with an empty cursor;
    # do not let that wipe the high-water mark from the last successful page.
    "slack_sync_state": ("cursor_ts",),
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


def _upsert_clause(
    table: str,
    spec: TableSpec,
    columns: tuple[str, ...] | None = None,
    *,
    target_alias: str | None = None,
) -> str:
    columns = columns or spec.columns
    update_columns = [column for column in columns if column not in spec.primary_key]
    conflict_columns = ", ".join(_identifier(column) for column in spec.primary_key)
    if not update_columns:
        return f"ON CONFLICT ({conflict_columns}) DO NOTHING"
    preserve_non_empty_columns = PRESERVE_NON_EMPTY_COLUMNS_BY_TABLE.get(table, ())
    target_ref = _identifier(target_alias) if target_alias else _identifier(table)
    assignments = ", ".join(
        _upsert_assignment(
            target_ref=target_ref,
            column=column,
            preserve_non_empty=column in preserve_non_empty_columns,
        )
        for column in update_columns
    )
    version_column = spec.version_column
    return (
        f"ON CONFLICT ({conflict_columns}) DO UPDATE SET {assignments} "
        f"WHERE {target_ref}.{_identifier(version_column)} <= EXCLUDED.{_identifier(version_column)}"
    )


def _upsert_assignment(*, target_ref: str, column: str, preserve_non_empty: bool) -> str:
    quoted_column = _identifier(column)
    excluded_column = f"EXCLUDED.{quoted_column}"
    if preserve_non_empty:
        return f"{quoted_column} = COALESCE(NULLIF({excluded_column}, ''), {target_ref}.{quoted_column})"
    return f"{quoted_column} = {excluded_column}"


def _identifier(value: str) -> str:
    return '"' + _validate_identifier(value).replace('"', '""') + '"'


def _validate_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"invalid SQL identifier: {value!r}")
    return value


def _pipeline_health_row(snapshot: Any, *, collected_at: datetime) -> dict[str, Any]:
    """Flatten one freshness snapshot for insert.

    Warehouse timestamp columns are NOT NULL with the epoch as "absent", so an
    unmeasured probe becomes the epoch here and the marts_ops views turn it back
    into NULL on the way out.
    """
    row = {**asdict(snapshot), "collected_at": collected_at}
    return {
        column: (PIPELINE_HEALTH_EPOCH if value is None and column in TIMESTAMP_COLUMNS else value)
        for column, value in row.items()
    }


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


def _contact_update_fields_observed(row: Mapping[str, Any], operation: Mapping[str, Any]) -> bool:
    fields = {str(field) for field in operation.get("update_person_fields") or []}
    person = _json_mapping(operation.get("person"))
    if fields == {"nicknames"}:
        expected = _contact_nickname_values(_json_list(person.get("nicknames")))
        observed = _contact_nickname_values(_json_list(row.get("nicknames")))
        observed.update(_contact_nickname_values(_json_list(_as_json_dict(row.get("raw_json")).get("nicknames"))))
        if expected:
            return expected <= observed
        return not observed
    return False


def _contact_nickname_values(items: Sequence[Mapping[str, Any]]) -> set[str]:
    return {str(item.get("value") or "").strip().casefold() for item in items if str(item.get("value") or "").strip()}


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


def _postgres_permanent_rejection_clause(column: str) -> str:
    """Recognised rejections of the audio itself. An ALLOW-list on purpose --
    see PERMANENT_VOICE_MEMO_TRANSCRIPTION_REJECTION_PATTERNS: an unrecognised
    error must stay 'error' and stay red rather than be silently retired."""
    return " OR ".join(
        f"{column} ILIKE '%%{_escape_like(pattern)}%%' ESCAPE E'\\\\'"
        for pattern in PERMANENT_VOICE_MEMO_TRANSCRIPTION_REJECTION_PATTERNS
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


def _upstream_mutation_provider_filter(
    *,
    providers: Sequence[str] | None,
    exclude_providers: Sequence[str] | None,
) -> tuple[str, tuple[Any, ...]]:
    """Build the provider predicate shared by the claim and count queries.

    Returned as SQL text plus positional params so both callers interpolate the same
    clause in the same parameter order; the two drifting apart would let the sensor
    report work the worker then refuses to claim, which reads as a stuck queue.
    """

    clauses: list[str] = []
    params: list[Any] = []
    included = [str(item) for item in (providers or []) if str(item).strip()]
    excluded = [str(item) for item in (exclude_providers or []) if str(item).strip()]
    if included:
        clauses.append("AND provider = ANY(%s)")
        params.append(included)
    if excluded:
        clauses.append("AND NOT (provider = ANY(%s))")
        params.append(excluded)
    return " ".join(clauses), tuple(params)
