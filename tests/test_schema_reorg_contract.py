"""The warehouse schema contract: base_* → derived_* → marts_* → timeline.

These tests describe the target layout itself rather than any one source's
tables, so they are the place a schema change has to be argued:

* the catalog is internally consistent and is the only editable authority
  (Python reads it, the Go file is generated from it);
* a fresh real-Postgres warehouse contains exactly the cataloged objects;
* the read-only query role can read every public relation, cannot reach
  ``private``, and reaches ``ops``/``internal`` only through the explicit
  application allowlist;
* discovery sorts base → derived → marts → timeline, hides the implementation
  schemas, and prominently recommends timeline as the starting point;
* no pre-reorg schema, relation, function, type, or rewriter survives.
"""

from __future__ import annotations

import ast
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.postgres import POSTGRES_TABLES, PostgresWarehouse
from personal_data_warehouse.relations import (
    ALL_CANONICAL_SCHEMAS,
    BASE_SCHEMAS,
    CANONICAL_RELATIONS,
    CATALOG,
    DERIVED_SCHEMAS,
    DISCOVERABLE_SCHEMAS,
    HIDDEN_SCHEMAS,
    MARTS_SCHEMAS,
    expand_relations,
    physical_schema_name,
    relation,
)
from personal_data_warehouse.schema_upgrade import UPGRADE_PLAN
from personal_data_warehouse.timeline import TIMELINE_ADAPTERS, TIMELINE_TABLE_COVERAGE
from personal_data_warehouse_alice_voice_recordings.sync import SOURCE as ALICE_VOICE_RECORDINGS_SOURCE

REPO_ROOT = Path(__file__).resolve().parent.parent


def _postgres_url() -> str:
    load_dotenv()
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if not url:
        pytest.skip("POSTGRES_DATABASE_URL is not set")
    return url


@pytest.fixture()
def warehouse():
    namespace = make_test_schema("reorg")
    wh = PostgresWarehouse(_postgres_url(), schema=namespace)
    try:
        yield wh
    finally:
        cleanup_test_warehouse(wh)


def _provision_everything(wh: PostgresWarehouse) -> None:
    wh.ensure_tables()
    wh.ensure_calendar_tables()
    wh.ensure_contacts_tables()
    wh.ensure_google_drive_source_tables()
    wh.ensure_slack_tables()
    wh.ensure_apple_notes_tables()
    wh.ensure_apple_messages_tables()
    wh.ensure_apple_voice_memos_tables(backfill_content_hashes=False)
    wh.ensure_alice_voice_recordings_tables()
    wh.ensure_whatsapp_tables()
    wh.ensure_photos_tables()
    wh.ensure_whoop_tables()
    wh.ensure_whoop_private_tables()
    wh.ensure_agent_sessions_tables()
    wh.ensure_plaid_tables()
    wh.ensure_finance_tables()
    wh.ensure_manual_finance_tables()
    wh.ensure_receipt_tables()
    wh.ensure_agent_tables()
    wh.ensure_whatsapp_client_session_table()
    wh.ensure_timeline_tables()
    wh.ensure_search_index_tables()
    wh.ensure_upstream_mutation_tables()
    wh.ensure_pipeline_health_tables()


# ---------------------------------------------------------------------------
# catalog contract
# ---------------------------------------------------------------------------


def test_catalog_layers_and_schema_names_are_consistent() -> None:
    for schema in BASE_SCHEMAS:
        assert schema.startswith("base_")
    for schema in DERIVED_SCHEMAS:
        assert schema.startswith("derived_")
    for schema in MARTS_SCHEMAS:
        assert schema.startswith("marts_")
    assert "timeline" in DISCOVERABLE_SCHEMAS
    assert set(HIDDEN_SCHEMAS) == {"ops", "private", "internal"}
    assert set(DISCOVERABLE_SCHEMAS).isdisjoint(HIDDEN_SCHEMAS)
    assert set(ALL_CANONICAL_SCHEMAS) == set(DISCOVERABLE_SCHEMAS) | set(HIDDEN_SCHEMAS)


def test_public_schemas_sort_base_then_derived_then_marts_then_timeline() -> None:
    """Plain alphabetical order is the intended reading order.

    A bare ``\\dn`` in psql, an ORDER BY table_schema, and the schema overview
    all agree without anyone hand-sorting them.
    """
    ordered = list(DISCOVERABLE_SCHEMAS)
    assert ordered == sorted(ordered)
    layers = [CATALOG.schema(name).layer for name in ordered]
    assert layers == sorted(layers, key=["base", "derived", "marts", "timeline"].index)


def test_catalog_ids_and_physical_locations_are_unique() -> None:
    ids = [obj.id for obj in CATALOG.objects]
    assert len(ids) == len(set(ids))
    physical = [(obj.schema, obj.name, obj.kind) for obj in CATALOG.objects]
    assert len(physical) == len(set(physical))


def test_catalog_object_counts_match_the_target_map() -> None:
    by_layer: dict[str, int] = {}
    for obj in CATALOG.objects:
        by_layer[obj.layer] = by_layer.get(obj.layer, 0) + 1
    assert by_layer == {
        # +10 base: the whoop_private source (base_whoop_private), which adds
        # the time series the public WHOOP API has no endpoint for.
        "base": 62,
        # +1 derived / +1 marts: derived_slack.file_fingerprints and its
        # marts_slack.image_fingerprints read view (Slack image identification).
        "derived": 23,
        # +3 marts: the cross-source entry points marts_messages.messages and
        # marts_voice_memos.recordings/.transcript_segments, which gave the two
        # domains that had per-source views but no unified read interface one.
        # +2 marts / +2 ops: marts_ops.mart_view_health (level 2 of the health
        # contract -- the marts layer itself had no coverage at all) and
        # marts_ops.collation_health, over ops.mart_view_health and
        # ops.collation_health.
        # +2 marts: marts_ops.slack_conversation_health (per-conversation-type
        # discovery freshness -- pipeline_health rolls Slack up as one pipeline
        # and cannot see one type stall) and marts_slack.huddles (huddle
        # metadata parsed out of the huddle_thread message payload).
        # +4 marts: marts_health.cycles/.sleeps/.recoveries/.workouts, the first
        # read interface over BOTH WHOOP sources. Reading either raw schema alone
        # is wrong in a different direction, and their units disagree (private
        # HRV in SECONDS, public in milliseconds).
        "marts": 42,
        # +1 timeline: timeline.context(ref, before, after), the search-hit
        # neighborhood reader. +3 timeline: the semantic, literal, and fusion
        # helpers that let the app execute hybrid retrieval legs concurrently.
        # +1 internal: internal.search_text_preview, the match-windowed preview
        # helper both search functions use.
        "timeline": 12,
        # +1 ops / +1 private: whoop_private's sync state and its rotating
        # browser-session credential. Search convergence adds one ops row;
        # slack_sessions adds the captured Slack client session that lets the
        # sync ask client.counts what changed instead of polling everything.
        "ops": 27,
        "private": 7,
        "internal": 2,
    }


def test_catalog_records_the_target_physical_locations() -> None:
    expected = {
        # base: faithful source data
        "gmail_messages": ("base_gmail", "messages"),
        "gmail_attachments": ("base_gmail", "attachments"),
        "calendar_events": ("base_google_calendar", "events"),
        "contact_cards": ("base_google_contacts", "cards"),
        "google_drive_files": ("base_google_drive", "files"),
        "plaid_transactions": ("base_plaid", "transactions"),
        "slack_messages": ("base_slack", "messages"),
        "apple_contact_cards": ("base_apple_contacts", "cards"),
        "apple_notes": ("base_apple_notes", "notes"),
        "apple_messages": ("base_apple_messages", "messages"),
        "apple_voice_memos_files": ("base_apple_voice_memos", "files"),
        "apple_photos_files": ("base_apple_photos", "files"),
        "alice_voice_recordings": ("base_alice_voice_recordings", "recordings"),
        "whoop_sleeps": ("base_whoop", "sleeps"),
        "whatsapp_messages": ("base_whatsapp", "messages"),
        "chatgpt_events": ("base_chatgpt", "events"),
        "claude_code_events": ("base_claude_code", "events"),
        "manual_finance_documents": ("base_manual_finance", "documents"),
        # derived: modelled facts
        "apple_voice_memos_enrichments": ("derived_voice_memos", "enrichments"),
        "google_drive_file_texts": ("derived_documents", "google_drive_file_texts"),
        "slack_conversation_stats": ("derived_slack", "conversation_stats"),
        "slack_account_state_item_rows": ("derived_slack", "inbox_items"),
        "file_attachment_enrichments": ("derived_enrichment", "file_attachment_enrichments"),
        "media_fingerprints": ("derived_enrichment", "media_fingerprints"),
        "photo_assets": ("derived_photos", "assets"),
        "photo_asset_files": ("derived_photos", "asset_files"),
        "finance_observations": ("derived_finance", "observations"),
        "finance_security_transactions": ("derived_finance", "security_transactions"),
        "finance_tax_lots": ("derived_finance", "tax_lots"),
        "manual_finance_extractions": ("derived_finance", "document_extractions"),
        "receipt_transaction_receipts": ("derived_receipts", "transaction_receipts"),
        # marts: domain read interfaces
        "clean_gmail_inbox": ("marts_inbox", "gmail_threads"),
        "clean_slack_inbox": ("marts_inbox", "slack_items"),
        "clean_contacts": ("marts_contacts", "contacts"),
        "clean_contact_points": ("marts_contacts", "contact_points"),
        "clean_apple_messages": ("marts_messages", "apple_messages"),
        "clean_whatsapp_messages": ("marts_messages", "whatsapp_messages"),
        "ai_conversation_events": ("marts_ai_conversations", "events"),
        "clean_agent_sessions": ("marts_ai_conversations", "sessions"),
        "photo_files": ("marts_photos", "files"),
        "clean_photos": ("marts_photos", "photos"),
        "photo_canonical_renditions": ("marts_photos", "canonical_renditions"),
        "clean_calendar_with_transcripts": ("marts_calendar", "events_with_voice_memos"),
        "clean_transcripts_no_calendar_match": ("marts_calendar", "unmatched_voice_memos"),
        "marts_finance_net_worth": ("marts_finance", "net_worth"),
        "marts_finance_net_worth_history": ("marts_finance", "net_worth_history"),
        "marts_finance_account_freshness": ("marts_finance", "account_freshness"),
        "marts_transaction_receipts": ("marts_receipts", "transaction_receipts"),
        # The warehouse's own operational read interface: freshness and health
        # per pipeline, with the ops snapshot behind it.
        "marts_pipeline_health": ("marts_ops", "pipeline_health"),
        "marts_pipeline_table_freshness": ("marts_ops", "table_freshness"),
        "marts_ops_plaid_item_health": ("marts_ops", "plaid_item_health"),
        # timeline: the entry point plus the search interface
        "timeline_events": ("timeline", "events"),
        "timeline_events_seq": ("timeline", "events_seq"),
        "timeline_priority": ("timeline", "timeline_priority"),
        "search_text_hit": ("timeline", "text_hit"),
        "search_text": ("timeline", "search_text"),
        "search_text_exact": ("timeline", "search_text_exact"),
        "search_text_sources": ("timeline", "search_text_sources"),
        # ops: source-prefixed so one flat schema cannot collide
        "gmail_sync_state": ("ops", "gmail_sync_state"),
        "pipeline_health": ("ops", "pipeline_health"),
        "pipeline_table_freshness": ("ops", "pipeline_table_freshness"),
        "calendar_sync_state": ("ops", "google_calendar_sync_state"),
        "contact_sync_state": ("ops", "google_contacts_sync_state"),
        "slack_sync_state": ("ops", "slack_sync_state"),
        "timeline_sync_state": ("ops", "timeline_sync_state"),
        "search_schema_state": ("ops", "search_schema_state"),
        "agent_runs": ("ops", "ai_processing_agent_runs"),
        "upstream_mutations": ("ops", "upstream_mutation_operations"),
        "upstream_mutation_requests": ("ops", "upstream_mutation_requests"),
        # private + internal
        "plaid_item_tokens": ("private", "plaid_item_tokens"),
        "chatgpt_sessions": ("private", "chatgpt_sessions"),
        "utf8_byte_prefix": ("internal", "utf8_byte_prefix"),
    }
    for logical, location in expected.items():
        rel = relation(logical)
        assert (rel.schema, rel.name) == location, logical


def test_alice_archive_source_uses_source_owned_name() -> None:
    assert ALICE_VOICE_RECORDINGS_SOURCE == "alice_voice_recordings"


def test_every_postgres_table_spec_is_a_cataloged_table() -> None:
    for name in POSTGRES_TABLES:
        obj = CATALOG.object(name)
        assert obj.kind == "table", name


def test_query_access_policy_matches_the_layer_contract() -> None:
    for obj in CATALOG.objects:
        if obj.layer in {"base", "derived", "marts", "timeline"}:
            assert obj.query_access == "public", obj.id
            assert obj.discoverable, obj.id
        elif obj.layer == "private":
            assert obj.query_access == "denied", obj.id
            assert obj.secret, obj.id
        else:
            assert obj.query_access in {"denied", "app_only", "execute_only"}, obj.id
            assert not obj.discoverable, obj.id

    # ops is reachable only for the operational surfaces the app itself renders.
    app_read = {obj.id for obj in CATALOG.objects if obj.query_access == "app_only"}
    assert app_read == {
        "timeline_sync_state",
        "search_chunk_sync_state",
        # Read through the marts_ops views by the /pipelines dashboard; granted
        # directly too so "when did gmail last update?" is answerable in SQL.
        "pipeline_health",
        "pipeline_table_freshness",
        # Levels 2 and 4 of the same dashboard: the marts layer's own health and
        # the collation-drift findings, both rendered by /pipelines.
        "mart_view_health",
        "collation_health",
        "search_health",
        "upstream_mutations",
        "upstream_mutation_requests",
        "upstream_mutation_events",
        "upstream_mutation_request_events",
        "agent_runs",
        "agent_run_events",
        "agent_run_tool_calls",
    }
    assert {obj.id for obj in CATALOG.objects if obj.query_access == "execute_only"} == {
        "utf8_byte_prefix",
        "search_text_preview",
    }
    assert set(CATALOG.denied_schemas()) == {"private"}


def test_namespaced_schema_identifiers_stay_within_postgres_limit() -> None:
    namespace = make_test_schema("a_deliberately_long_integration_label")
    physicals = [physical_schema_name(schema, namespace=namespace) for schema in ALL_CANONICAL_SCHEMAS]
    for physical in physicals:
        assert len(physical.encode("utf-8")) <= 63, physical
        # The leak reaper keys on the pdw_test_<timestamp>_ prefix.
        assert physical.startswith("pdw_test_")
    assert len(set(physicals)) == len(physicals)


# ---------------------------------------------------------------------------
# catalog is the only authority
# ---------------------------------------------------------------------------


def test_go_catalog_is_generated_from_the_json_catalog() -> None:
    result = subprocess.run(
        [sys.executable, "scripts/generate_go_warehouse_catalog.py", "--check"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr or result.stdout


def test_go_catalog_has_no_second_editable_relation_table() -> None:
    relations_go = (REPO_ROOT / "app/internal/warehouse/relations.go").read_text()
    assert "var Relations = map[string]Relation{" not in relations_go
    assert "var QueryableSchemas = []string{" not in relations_go
    generated = (REPO_ROOT / "app/internal/warehouse/catalog_gen.go").read_text()
    assert "DO NOT EDIT" in generated
    for logical in ("gmail_messages", "timeline_events", "search_text"):
        assert f'ID: "{logical}"' in generated


def test_catalog_json_round_trips_through_the_generator() -> None:
    payload = json.loads((REPO_ROOT / "src/personal_data_warehouse/warehouse_catalog.json").read_text())
    assert payload["version"] == CATALOG.version
    assert len(payload["objects"]) == len(CATALOG.objects)
    assert payload["start_here"]["schema"] == "timeline"


# ---------------------------------------------------------------------------
# no legacy naming layer
# ---------------------------------------------------------------------------


def test_relation_rewriters_and_legacy_aliases_are_gone() -> None:
    relations_py = (REPO_ROOT / "src/personal_data_warehouse/relations.py").read_text()
    assert "def qualify_sql_relations" not in relations_py
    assert "LEGACY_QUERY_ALIASES" not in relations_py
    assert "def query_relation" not in relations_py

    relations_go = (REPO_ROOT / "app/internal/warehouse/relations.go").read_text()
    assert "func QualifySQL" not in relations_go

    assert "agent_session_events" not in CANONICAL_RELATIONS
    assert "agent_session_events" not in POSTGRES_TABLES


def test_unknown_relation_markers_fail_instead_of_passing_through() -> None:
    with pytest.raises(KeyError):
        expand_relations("SELECT * FROM @not_a_relation")
    # A bare legacy name is left exactly as written, so Postgres rejects it
    # rather than the code silently resolving it somewhere.
    assert expand_relations("SELECT * FROM gmail_messages") == "SELECT * FROM gmail_messages"


def test_relation_markers_are_not_expanded_inside_literals_or_comments() -> None:
    sql = (
        "SELECT '@gmail_messages' AS literal, \"@gmail_messages\" AS ident\n"
        "-- @gmail_messages in a comment\n"
        "FROM @gmail_messages WHERE addr LIKE '%@example.com'"
    )
    expanded = expand_relations(sql)
    assert "'@gmail_messages'" in expanded
    assert '"@gmail_messages"' in expanded
    assert "-- @gmail_messages in a comment" in expanded
    assert "'%@example.com'" in expanded
    assert 'FROM "base_gmail"."messages"' in expanded


def test_no_runtime_legacy_migration_paths_remain() -> None:
    postgres_py = (REPO_ROOT / "src/personal_data_warehouse/postgres.py").read_text()
    for banned in (
        "_migrate_legacy_table_if_present",
        "_migrate_legacy_named_table_if_present",
        "_migrate_legacy_agent_session_events_if_present",
        "_migrate_file_attachment_enrichments_rename",
        "_migrate_timeline_priority_to_enum",
        "_drop_legacy_search_routines_if_present",
        "_drop_legacy_view_if_present",
        "_ensure_ai_conversation_events_insert_trigger",
    ):
        assert banned not in postgres_py, banned


def test_warehouse_sql_never_names_a_relation_bare() -> None:
    """A logical id in SQL must be written as an explicit @marker.

    This is the enforcement test for the rewriter's removal: bare tokens used to
    be rewritten for you, so re-introducing one would look like it worked
    locally and then resolve through the search_path in production.
    """
    names = set(CANONICAL_RELATIONS)

    def code_only(text: str) -> str:
        """Blank out SQL string literals and comments, like expand_relations does."""
        out: list[str] = []
        i, n = 0, len(text)
        while i < n:
            ch = text[i]
            if ch == "'":
                start = i
                i += 1
                while i < n:
                    if text[i] == "'":
                        i += 1
                        if i < n and text[i] == "'":
                            i += 1
                            continue
                        break
                    i += 1
                out.append(" " * (i - start))
                continue
            if ch == "-" and i + 1 < n and text[i + 1] == "-":
                start = i
                while i < n and text[i] != "\n":
                    i += 1
                out.append(" " * (i - start))
                continue
            out.append(ch)
            i += 1
        return "".join(out)

    sql_keyword = re.compile(
        r"\b(SELECT|INSERT\s+INTO|UPDATE|DELETE\s+FROM|FROM|JOIN|CREATE|ALTER|DROP|TRUNCATE)\b"
    )
    offenders: list[str] = []
    for path in (
        REPO_ROOT / "src/personal_data_warehouse/postgres.py",
        REPO_ROOT / "src/personal_data_warehouse/timeline.py",
    ):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                parts = [node.value]
            elif isinstance(node, ast.JoinedStr):
                parts = [
                    v.value
                    for v in node.values
                    if isinstance(v, ast.Constant) and isinstance(v.value, str)
                ]
            else:
                continue
            text = " ".join(parts)
            if not sql_keyword.search(text):
                continue
            text = code_only(text)
            for match in re.finditer(r"(?<![@.\w])([A-Za-z_][A-Za-z0-9_]*)(?![\w.])", text):
                token = match.group(1)
                if token in names and token != "search_text":
                    offenders.append(f"{path.name}:{node.lineno}: {token}")
    assert not offenders, "bare warehouse relation tokens in SQL: " + "; ".join(
        sorted(set(offenders))[:20]
    )


def _stale_physical_names() -> set[str]:
    old_names = {
        f"{obj.previous_schema}.{obj.previous_name}"
        for obj in CATALOG.objects
        if obj.previous_schema
    }
    current = {f"{obj.schema}.{obj.name}" for obj in CATALOG.objects}
    return old_names - current


def _python_string_literals(path: Path) -> list[tuple[int, str]]:
    out: list[tuple[int, str]] = []
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            parts = [node.value]
        elif isinstance(node, ast.JoinedStr):
            parts = [
                v.value for v in node.values if isinstance(v, ast.Constant) and isinstance(v.value, str)
            ]
        else:
            continue
        out.extend((node.lineno, text) for text in parts)
    return out


# Interpreted ("...") and raw (`...`) Go string literals. Comments are excluded
# deliberately: a stale name in a comment misleads a reader, a stale name in a
# literal is shipped behaviour — the query service's error hints are literals
# and are read by an agent as instructions.
_GO_STRING = re.compile(r'"(?:[^"\\\n]|\\.)*"|`[^`]*`', re.MULTILINE)


def _go_string_literals(path: Path) -> list[tuple[int, str]]:
    text = path.read_text()
    return [
        (text.count("\n", 0, m.start()) + 1, m.group(0)) for m in _GO_STRING.finditer(text)
    ]


def _markdown_documents() -> list[Path]:
    """Every prose document, with ``CLAUDE.md``'s symlink to ``AGENTS.md`` collapsed."""
    seen: set[Path] = set()
    docs: list[Path] = []
    for path in sorted(REPO_ROOT.glob("*.md")) + sorted((REPO_ROOT / "docs").rglob("*.md")):
        real = path.resolve()
        if real in seen:
            continue
        seen.add(real)
        docs.append(path)
    return docs


def test_no_module_names_a_pre_reorg_physical_relation() -> None:
    """No string anywhere may still spell an old ``schema.name``.

    The @marker sweep only reaches SQL the warehouse expands. SQL that crosses a
    boundary first — the voice-memo write-back queries the app's HTTP tool API,
    a report script builds its own statement — carries the physical name as a
    plain string, so it is exactly where a rename rots silently. It did: the
    write-back shipped ``apple_voice_memos.enrichments`` and broke on the first
    run after the cutover.

    The sweep covers Python, the Go app, **and the Markdown**, because the docs
    are executable for an agent: an agent that reads ``README.md`` and types the
    name it found there gets an undefined-relation error, and the docs went 28
    days after the reorg still telling it to query ``clean_gmail_inbox``.
    """
    stale = _stale_physical_names()
    # schema_upgrade.py is the one module whose whole job is the old layout.
    skip = {
        REPO_ROOT / "src/personal_data_warehouse/schema_upgrade.py",
        # Generated from the catalog's own `previous` blocks: mapping the old
        # location onto the new one is the entire point of the file.
        REPO_ROOT / "app/internal/warehouse/catalog_gen.go",
    }
    sources: list[tuple[Path, list[tuple[int, str]]]] = []
    for path in sorted((REPO_ROOT / "src").rglob("*.py")) + sorted((REPO_ROOT / "scripts").rglob("*.py")):
        if path not in skip:
            sources.append((path, _python_string_literals(path)))
    for path in sorted((REPO_ROOT / "app").rglob("*.go")):
        # Go tests feed old names in on purpose: they assert the undefined-relation
        # error still points at the new location.
        if path in skip or path.name.endswith("_test.go"):
            continue
        sources.append((path, _go_string_literals(path)))
    for path in _markdown_documents():
        sources.append((path, list(enumerate(path.read_text().splitlines(), 1))))

    offenders: list[str] = []
    for path, chunks in sources:
        for lineno, text in chunks:
            for name in stale:
                if re.search(rf"(?<![\w.@]){re.escape(name)}(?![\w])", text):
                    offenders.append(f"{path.relative_to(REPO_ROOT)}:{lineno}: {name}")
    assert not offenders, "pre-reorg physical names still in source: " + "; ".join(
        sorted(set(offenders))[:20]
    )


# A qualified relation reference, by shape rather than by a known-schema list, so
# a schema that never existed (``marts_derived_finance``) is caught too — that
# exact invention sat in four places in the docs, one of them presented as
# runnable verification SQL, for a month after the reorg.
_QUALIFIED_RELATION = re.compile(
    r"(?<![\w.])((?:base|derived|marts)_[a-z0-9_]+|timeline|ops|private|internal)"
    r"\.([a-z_][a-z0-9_]*)(\*?)"
)


def test_docs_only_name_relations_that_exist() -> None:
    """Every ``schema.relation`` in the prose must be in the catalog.

    Agents read these documents and type what they find. The stale-name sweep
    above only catches names the catalog remembers moving; this catches the
    other half — a name that was never right, or a relation that was deleted
    outright — by requiring every qualified reference to resolve.
    """
    live = {f"{obj.schema}.{obj.name}" for obj in CATALOG.objects}
    offenders: list[str] = []
    for path in _markdown_documents():
        for lineno, line in enumerate(path.read_text().splitlines(), 1):
            for match in _QUALIFIED_RELATION.finditer(line):
                if match.group(3) == "*":  # a `marts_finance.investment_*` glob in prose
                    continue
                token = f"{match.group(1)}.{match.group(2)}"
                if token not in live:
                    offenders.append(f"{path.relative_to(REPO_ROOT)}:{lineno}: {token}")
    assert not offenders, "docs name relations that do not exist: " + "; ".join(
        sorted(set(offenders))[:20]
    )


# ---------------------------------------------------------------------------
# timeline routing
# ---------------------------------------------------------------------------


def test_timeline_source_tables_are_catalog_ids() -> None:
    for adapter in TIMELINE_ADAPTERS:
        assert adapter.source_table in CANONICAL_RELATIONS, adapter.name
    for name in TIMELINE_TABLE_COVERAGE:
        assert name in CANONICAL_RELATIONS, name


def test_renamed_timeline_source_tables_are_recorded_for_stored_rows() -> None:
    assert CATALOG.renamed_timeline_source_tables == {
        "agent_session_events": "ai_conversation_events"
    }
    assert UPGRADE_PLAN.timeline_source_table_renames == {
        "agent_session_events": "ai_conversation_events"
    }


# ---------------------------------------------------------------------------
# real Postgres: fresh provisioning matches the catalog exactly
# ---------------------------------------------------------------------------


def _inventory(wh: PostgresWarehouse, sql: str) -> set[tuple[str, str]]:
    return {
        (schema, name)
        for schema, name in wh._query(sql, (wh.physical_schema_names(include_hidden=True),))
    }


def test_fresh_database_object_inventory_matches_the_catalog(warehouse: PostgresWarehouse) -> None:
    _provision_everything(warehouse)
    namespace = warehouse.schema_namespace

    def expected(kinds: set[str]) -> set[tuple[str, str]]:
        return {
            (physical_schema_name(obj.schema, namespace=namespace), obj.name)
            for obj in CATALOG.objects
            if obj.kind in kinds
        }

    assert _inventory(
        warehouse,
        """
        SELECT n.nspname, c.relname
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = ANY(%s) AND c.relkind IN ('r', 'v', 'm', 'p')
        """,
    ) == expected({"table", "view"})

    assert _inventory(
        warehouse,
        """
        SELECT n.nspname, c.relname
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = ANY(%s) AND c.relkind = 'S'
        """,
    ) == expected({"sequence"})

    assert _inventory(
        warehouse,
        """
        SELECT n.nspname, p.proname
        FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = ANY(%s)
        """,
    ) == expected({"function"})

    assert _inventory(
        warehouse,
        """
        SELECT n.nspname, t.typname
        FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = ANY(%s)
          AND (t.typrelid = 0 OR (SELECT c.relkind FROM pg_class c WHERE c.oid = t.typrelid) = 'c')
          AND NOT EXISTS (SELECT 1 FROM pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)
        """,
    ) == expected({"type"})


def test_fresh_database_has_no_pre_reorg_schemas_or_shadowing_routines(
    warehouse: PostgresWarehouse,
) -> None:
    _provision_everything(warehouse)
    namespace = warehouse.schema_namespace
    legacy_schemas = [
        physical_schema_name(schema, namespace=namespace)
        for schema in (
            "gmail",
            "google_calendar",
            "google_contacts",
            "google_drive",
            "slack",
            "whatsapp",
            "apple_notes",
            "apple_messages",
            "apple_voice_memos",
            "apple_photos",
            "apple_contacts",
            "alice_voice_recordings",
            "whoop",
            "plaid",
            "chatgpt",
            "claude_code",
            "claude_desktop",
            "codex",
            "openclaw",
            "pi",
            "manual_finance",
            "marts",
            "search",
            "enrichment",
            "photos",
            "finance",
            "receipts",
            "ai_processing",
            "upstream_mutations",
            "util",
        )
    ]
    present = warehouse._query(
        "SELECT nspname FROM pg_namespace WHERE nspname = ANY(%s)", (legacy_schemas,)
    )
    assert present == []

    # The base namespace must hold nothing at all: a stale search_text() left
    # there silently shadowed the real one for 16 days, because unqualified
    # callers resolve through Postgres' default '"$user", public' search_path.
    leftovers = warehouse._query(
        """
        SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relkind IN ('r', 'v', 'm', 'S')
        UNION ALL
        SELECT p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = %s
        UNION ALL
        SELECT t.typname FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = %s
        """,
        (namespace, namespace, namespace),
    )
    assert leftovers == []


def test_schema_comments_publish_the_start_here_guidance(warehouse: PostgresWarehouse) -> None:
    timeline_schema = warehouse.physical_schema_name("timeline")
    comment = warehouse._query(
        "SELECT obj_description(n.oid, 'pg_namespace') FROM pg_namespace n WHERE n.nspname = %s",
        (timeline_schema,),
    )[0][0]
    assert "Start with timeline" in comment
    base_comment = warehouse._query(
        "SELECT obj_description(n.oid, 'pg_namespace') FROM pg_namespace n WHERE n.nspname = %s",
        (warehouse.physical_schema_name("base_gmail"),),
    )[0][0]
    assert "source data" in base_comment


def test_query_role_reads_public_relations_and_is_denied_private(
    warehouse: PostgresWarehouse,
) -> None:
    _provision_everything(warehouse)
    namespace = warehouse.schema_namespace
    role = warehouse.query_role

    for obj in CATALOG.objects:
        if not obj.is_relation:
            continue
        qualified = f'"{physical_schema_name(obj.schema, namespace=namespace)}"."{obj.name}"'
        readable = warehouse._query(
            "SELECT has_table_privilege(%s, %s, 'SELECT')", (role, qualified)
        )[0][0]
        if obj.query_access in {"public", "app_only"}:
            assert readable, f"{obj.id} should be readable by {role}"
        else:
            assert not readable, f"{obj.id} must not be readable by {role}"
            public_readable = warehouse._query(
                "SELECT has_table_privilege('public', %s, 'SELECT')", (qualified,)
            )[0][0]
            assert not public_readable, f"{obj.id} must not be readable by PUBLIC"

    private_schema = warehouse.physical_schema_name("private")
    usable = warehouse._query(
        "SELECT has_schema_privilege(%s, %s, 'USAGE')", (role, private_schema)
    )[0][0]
    assert not usable


def test_query_role_can_execute_the_search_interface(warehouse: PostgresWarehouse) -> None:
    _provision_everything(warehouse)
    role = warehouse.query_role
    # The trailing text[] is the priority-tier filter. It is part of the
    # signature the query role must be able to execute, not an optional extra:
    # adding the parameter OVERLOADS rather than replaces, so the old four-arg
    # forms were dropped deliberately and naming one here would assert a
    # privilege on a function that no longer exists.
    for logical, signature in (
        ("search_text", "(text, integer, text[], timestamptz, text[])"),
        ("search_text_exact", "(text, integer, text[], timestamptz, text[])"),
        ("search_text_sources", "()"),
        ("utf8_byte_prefix", "(text, integer)"),
    ):
        rel = relation(logical).with_namespace(warehouse.schema_namespace)
        executable = warehouse._query(
            "SELECT has_function_privilege(%s, %s, 'EXECUTE')",
            (role, f'"{rel.schema}"."{rel.name}"{signature}'),
        )[0][0]
        assert executable, logical
