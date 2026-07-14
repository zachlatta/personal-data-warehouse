"""Warehouse schema contract for the manual_finance source.

Manually uploaded finance documents (bank/mortgage statements, Zillow
screenshots, fund position docs, CSV exports) land as one row per document in
manual_finance.documents; the agent-first extraction writes structured rows
into manual_finance.extractions (voice-memos structured-columns pattern,
keyed by content sha + model/prompt version). Money is NUMERIC, statement
periods are DATE.
"""

from __future__ import annotations

import os
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from dotenv import load_dotenv

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.postgres import POSTGRES_TABLES, PostgresWarehouse
from personal_data_warehouse.relations import SOURCE_RAW_SCHEMAS, relation
from personal_data_warehouse.schema import (
    MANUAL_FINANCE_DOCUMENT_COLUMNS,
    MANUAL_FINANCE_EXTRACTION_COLUMNS,
)


def _postgres_url() -> str:
    load_dotenv()
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if not url:
        pytest.skip("POSTGRES_DATABASE_URL is not set")
    return url


@pytest.fixture()
def warehouse():
    schema = make_test_schema()
    wh = PostgresWarehouse(_postgres_url(), schema=schema)
    try:
        yield wh
    finally:
        cleanup_test_warehouse(wh)


_TS = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)


def _document_row(**overrides) -> dict:
    row = {
        "source": "manual",
        "account": "z@x.test",
        "source_native_id": "sha-doc-1",
        "filename": "acme-checking-2026-06.pdf",
        "original_path": "acme-checking-0001/acme-checking-2026-06.pdf",
        "mime_type": "application/pdf",
        "size_bytes": 54321,
        "content_sha256": "sha-doc-1",
        "file_modified_at": _TS,
        "raw_metadata_json": {"file": {"native_id": "sha-doc-1"}},
        "storage_backend": "google_drive",
        "storage_key": "manual-finance/inbox/acme-checking-0001/2026-07-13-sha-doc-1.pdf",
        "storage_file_id": "drive-1",
        "storage_url": "",
        "metadata_storage_key": "manual-finance/inbox/acme-checking-0001/2026-07-13-sha-doc-1.json",
        "metadata_storage_file_id": "drive-2",
        "metadata_storage_url": "",
        "metadata_content_sha256": "sha-meta-1",
        "is_deleted": 0,
        "ingested_at": _TS,
        "sync_version": 1,
    }
    row.update(overrides)
    return row


def _extraction_row(**overrides) -> dict:
    row = {
        "content_sha256": "sha-doc-1",
        "ai_provider": "agent_codex",
        "ai_model": "m",
        "ai_prompt_version": "manual-finance-agent-v1",
        "status": "ok",
        "error": "",
        "document_type": "bank_statement",
        "institution": "Acme Bank",
        "account_name_hint": "Checking",
        "account_mask": "0001",
        "period_start": date(2026, 6, 1),
        "period_end": date(2026, 6, 30),
        "currency": "USD",
        "closing_balance": Decimal("1234.56"),
        "transactions_json": [
            {"date": "2026-06-05", "description": "COFFEE", "amount": "-4.50", "direction": "out"}
        ],
        "balances_json": [{"date": "2026-06-30", "balance": "1234.56"}],
        "valuations_json": [],
        "summary": "June checking statement",
        "uncertainties_json": [],
        "raw_result_json": {"document_type": "bank_statement"},
        "ai_elapsed_ms": 1200,
        "ai_processed_at": _TS,
        "created_at": _TS,
        "sync_version": 1,
    }
    row.update(overrides)
    return row


# --- pure registry contracts ---------------------------------------------------


def test_manual_finance_relations_are_registered():
    assert "manual_finance" in SOURCE_RAW_SCHEMAS
    assert (relation("manual_finance_documents").schema, relation("manual_finance_documents").name) == (
        "manual_finance",
        "documents",
    )
    assert (relation("manual_finance_extractions").schema, relation("manual_finance_extractions").name) == (
        "manual_finance",
        "extractions",
    )


def test_manual_finance_table_specs():
    documents = POSTGRES_TABLES["manual_finance_documents"]
    assert documents.columns == MANUAL_FINANCE_DOCUMENT_COLUMNS
    assert documents.primary_key == ("source", "account", "source_native_id", "content_sha256")
    extractions = POSTGRES_TABLES["manual_finance_extractions"]
    assert extractions.columns == MANUAL_FINANCE_EXTRACTION_COLUMNS
    # Bumping prompt_version re-extracts without clobbering prior results.
    assert extractions.primary_key == (
        "content_sha256",
        "ai_provider",
        "ai_model",
        "ai_prompt_version",
    )


# --- live schema (Postgres) -----------------------------------------------------


def test_ensure_manual_finance_tables_is_idempotent(warehouse):
    warehouse.ensure_manual_finance_tables()
    warehouse.ensure_manual_finance_tables()
    rows = warehouse._query(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = ANY(%s) AND table_type = 'BASE TABLE'
        """,
        (warehouse.physical_schema_names(include_private=True),),
    )
    tables = {(schema, table) for schema, table in rows}
    manual_finance = warehouse.physical_schema_name("manual_finance")
    assert (manual_finance, "documents") in tables
    assert (manual_finance, "extractions") in tables
    # The extraction candidate/retry queries must work on a fresh schema.
    ai_processing = warehouse.physical_schema_name("ai_processing")
    assert (ai_processing, "agent_runs") in tables


def test_extraction_money_is_numeric_and_periods_are_dates(warehouse):
    warehouse.ensure_manual_finance_tables()
    rows = warehouse._query(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = 'extractions'
        """,
        (warehouse.physical_schema_name("manual_finance"),),
    )
    types = dict(rows)
    assert types["closing_balance"] == "numeric"
    assert types["period_start"] == "date"
    assert types["period_end"] == "date"
    assert types["transactions_json"] == "jsonb"
    assert types["raw_result_json"] == "jsonb"


def test_document_upsert_is_idempotent_by_provenance(warehouse):
    warehouse.ensure_manual_finance_tables()
    warehouse.insert_manual_finance_documents([_document_row()])
    # A re-upload after the file moved folders updates the path hint in place.
    warehouse.insert_manual_finance_documents(
        [_document_row(original_path="statements/acme-checking-2026-06.pdf", sync_version=2)]
    )
    rows = warehouse._query("SELECT original_path, sync_version FROM manual_finance_documents")
    assert rows == [("statements/acme-checking-2026-06.pdf", 2)]


def test_extraction_rows_round_trip(warehouse):
    warehouse.ensure_manual_finance_tables()
    warehouse.insert_manual_finance_extractions([_extraction_row()])
    rows = warehouse._query(
        """
        SELECT document_type, closing_balance, period_end,
               transactions_json -> 0 ->> 'description'
        FROM manual_finance_extractions
        """
    )
    assert rows == [("bank_statement", Decimal("1234.56"), date(2026, 6, 30), "COFFEE")]
    # A prompt bump writes a second row instead of clobbering the first.
    warehouse.insert_manual_finance_extractions(
        [_extraction_row(ai_prompt_version="manual-finance-agent-v2")]
    )
    assert warehouse._query("SELECT count(*) FROM manual_finance_extractions") == [(2,)]
