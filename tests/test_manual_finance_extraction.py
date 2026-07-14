"""Agent-first extraction for manual finance documents.

Input prep never bypasses the agent: it only chooses text vs page images.
Candidates come from manual_finance.documents; the retry cap counts
error/unreadable rows in the EXTRACTIONS table (input-prep failures never
reach agent_runs — the audio-enrichment lesson).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest
from dotenv import load_dotenv

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.manual_finance_extraction import (
    PROMPT_VERSION,
    STATUS_ERROR,
    STATUS_NOT_USEFUL,
    STATUS_OK,
    STATUS_UNREADABLE,
    ManualFinanceExtractionRunner,
    finance_document_extraction_schema,
    finance_extraction_prompt,
    has_extraction_candidate,
    load_extraction_candidates,
    prepare_document_inputs,
    validate_finance_extraction_result,
    DocumentInputs,
)
from personal_data_warehouse.file_attachment_enrichment import AttachmentPreparationError
from personal_data_warehouse.postgres import PostgresWarehouse

_TS = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)


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
        "raw_metadata_json": {},
        "storage_backend": "google_drive",
        # The real Drive store's listing refs carry only the file id; rows land
        # with an empty storage_key (photos precedent) — candidates must gate
        # on storage_file_id.
        "storage_key": "",
        "storage_file_id": "drive-1",
        "storage_url": "",
        "metadata_storage_key": "",
        "metadata_storage_file_id": "",
        "metadata_storage_url": "",
        "metadata_content_sha256": "",
        "is_deleted": 0,
        "ingested_at": _TS,
        "sync_version": 1,
    }
    row.update(overrides)
    return row


def _agent_output(**overrides) -> dict[str, Any]:
    output = {
        "is_financial": True,
        "document_type": "bank_statement",
        "institution": "Acme Bank",
        "account_name_hint": "Checking",
        "account_mask": "0001",
        "period_start": "2026-06-01",
        "period_end": "2026-06-30",
        "currency": "USD",
        "closing_balance": "1234.56",
        "transactions": [
            {"date": "2026-06-05", "description": "COFFEE SHOP", "amount": "4.50", "direction": "out"}
        ],
        "balances": [{"date": "2026-06-30", "balance": "1234.56"}],
        "valuations": [],
        "summary": "June checking statement",
        "uncertainties": [],
    }
    output.update(overrides)
    return output


@dataclass
class FakeAgentResult:
    run_id: str = "agent-test"
    provider: str = "codex"
    model: str = ""
    task_type: str = "manual_finance_extraction"
    subject_id: str = ""
    prompt_version: str = PROMPT_VERSION
    input_sha256: str = ""
    status: str = "completed"
    exit_code: int = 0
    error: str = ""
    started_at: datetime = _TS
    completed_at: datetime = _TS
    final_output_json: dict = field(default_factory=dict)
    final_output_text: str = ""
    events: tuple = ()


class FakeAgent:
    def __init__(self, output: dict[str, Any]):
        self._output = output
        self.requests: list[Any] = []
        self.warehouses: list[Any] = []

    def run_with_warehouse(self, request, *, warehouse, **kwargs):
        self.requests.append(request)
        self.warehouses.append(warehouse)
        return FakeAgentResult(
            subject_id=request.subject_id,
            final_output_json=dict(self._output),
            final_output_text=json.dumps(self._output),
        )


class FakeObjectStore:
    def __init__(self, content: bytes):
        self._content = content

    def get_object(self, ref):
        return self._content


class Logger:
    def info(self, *args) -> None:
        pass

    def warning(self, *args) -> None:
        pass


_PDF_WITH_TEXT = None  # built lazily via fpdf-free path below


def _text_pdf(text: str) -> bytes:
    # Minimal single-page PDF with a text layer (enough for pypdf to extract).
    stream = f"BT /F1 12 Tf 72 720 Td ({text}) Tj ET".encode()
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R "
        b"/Resources << /Font << /F1 5 0 R >> >> >>",
        b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    out = bytearray(b"%PDF-1.4\n")
    offsets = []
    for index, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{index} 0 obj\n".encode() + body + b"\nendobj\n"
    xref_at = len(out)
    out += f"xref\n0 {len(objects) + 1}\n".encode()
    out += b"0000000000 65535 f \n"
    for offset in offsets:
        out += f"{offset:010d} 00000 n \n".encode()
    out += (
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_at}\n%%EOF\n"
    ).encode()
    return bytes(out)


# --- input preparation ------------------------------------------------------------


def test_prepare_inputs_uses_text_layer_when_rich():
    text = "STATEMENT " + "transactions and balances " * 20
    inputs = prepare_document_inputs(
        content=_text_pdf(text), mime_type="application/pdf", filename="statement.pdf"
    )
    assert "STATEMENT" in inputs.text
    assert inputs.input_files == {}


def test_prepare_inputs_renders_pages_for_scanned_pdf():
    if not os.popen("which pdftoppm").read().strip():
        pytest.skip("pdftoppm is not installed")
    # A text-less PDF falls back to page rendering; a thin text layer rides along.
    inputs = prepare_document_inputs(
        content=_text_pdf("tiny"), mime_type="application/pdf", filename="scan.pdf", render_max_pages=2
    )
    assert list(inputs.input_files) == ["page-01.png"]
    assert inputs.input_files["page-01.png"].startswith(b"\x89PNG")


def test_prepare_inputs_passes_raw_text_for_exports():
    csv = b"Date,Description,Amount\n2026-06-05,COFFEE,-4.50\n"
    inputs = prepare_document_inputs(content=csv, mime_type="text/csv", filename="export.csv")
    assert "COFFEE" in inputs.text
    assert inputs.input_files == {}
    ofx = b"OFXHEADER:100\n<OFX><STMTTRN><TRNAMT>-4.50</TRNAMT></STMTTRN></OFX>"
    inputs = prepare_document_inputs(content=ofx, mime_type="application/x-ofx", filename="export.ofx")
    assert "TRNAMT" in inputs.text


def test_prepare_inputs_raises_unreadable_for_undecodable_image():
    with pytest.raises(AttachmentPreparationError):
        prepare_document_inputs(content=b"not-an-image", mime_type="image/png", filename="shot.png")


# --- prompt + schema ----------------------------------------------------------------


def test_prompt_carries_path_hint_known_accounts_and_page_guidance():
    prompt = finance_extraction_prompt(
        candidate=_document_row(),
        inputs=DocumentInputs(text="", input_files={"page-01.png": b"x", "page-02.png": b"y"}),
        known_accounts=[{"name": "Checking", "institution": "Acme Bank", "mask": "0001"}],
    )
    payload = json.loads(prompt)
    assert payload["document"]["original_path"] == "acme-checking-0001/acme-checking-2026-06.pdf"
    assert [page["path"] for page in payload["document"]["pages"]] == [
        "inputs/page-01.png",
        "inputs/page-02.png",
    ]
    assert "verbatim" in payload["document"]["how_to_view_pages"]
    assert payload["known_accounts"][0]["mask"] == "0001"
    schema = payload["final_output_contract"]["schema"]
    # Strict structured outputs: every property required (codex constraint).
    assert set(schema["required"]) == set(schema["properties"])


def test_validate_finance_extraction_result():
    assert validate_finance_extraction_result(_agent_output()) == []
    issues = validate_finance_extraction_result(
        _agent_output(transactions=[{"date": "2026-06-05"}], closing_balance="not-a-number")
    )
    assert any("transactions[0]" in issue for issue in issues)
    assert any("closing_balance" in issue for issue in issues)


# --- candidates + retry cap (live Postgres) ----------------------------------------


def test_candidates_and_retry_cap(warehouse):
    warehouse.ensure_manual_finance_tables()
    warehouse.insert_manual_finance_documents([_document_row()])
    provider = "agent_codex"

    def candidates():
        return load_extraction_candidates(
            warehouse, provider=provider, prompt_version=PROMPT_VERSION, limit=10
        )

    assert [c["content_sha256"] for c in candidates()] == ["sha-doc-1"]
    assert has_extraction_candidate(warehouse, provider=provider, prompt_version=PROMPT_VERSION)

    def record_failed_run(index: int) -> None:
        warehouse._command(
            """
            INSERT INTO agent_runs (run_id, task_type, subject_id, status, started_at)
            VALUES (%s, %s, %s, 'error', now())
            """,
            (f"agent-fail-{index}", "manual_finance_extraction", "sha-doc-1"),
        )

    # Agent-stage failures count per RUN in agent_runs: below the cap the
    # candidate stays, at the cap it is excluded.
    record_failed_run(1)
    record_failed_run(2)
    assert [c["content_sha256"] for c in candidates()] == ["sha-doc-1"]
    record_failed_run(3)
    assert candidates() == []
    assert not has_extraction_candidate(warehouse, provider=provider, prompt_version=PROMPT_VERSION)
    warehouse._command("DELETE FROM agent_runs")

    runner = ManualFinanceExtractionRunner(
        warehouse=warehouse,
        agent=None,
        object_store_factory=lambda: FakeObjectStore(b""),
        logger=Logger(),
        provider="codex",
        now=lambda: _TS,
    )
    # A permanent input-prep failure (UNREADABLE) excludes without any agent run.
    runner._record_failure(warehouse, _document_row(), error="corrupt pdf", status=STATUS_UNREADABLE)
    assert candidates() == []
    warehouse._command("DELETE FROM manual_finance_extractions")

    # A plain ERROR row does NOT exclude on its own (transient pre-agent
    # failures retry; only agent_runs counting and UNREADABLE bind).
    runner._record_failure(warehouse, _document_row(), error="drive hiccup", status=STATUS_ERROR)
    assert [c["content_sha256"] for c in candidates()] == ["sha-doc-1"]
    warehouse._command("DELETE FROM manual_finance_extractions")

    # A completed extraction (any status in COMPLETED_STATUSES) removes it.
    ok_row = runner._extraction_row(
        _document_row(), output=_agent_output(), status=STATUS_OK, error="", result=None
    )
    warehouse.insert_manual_finance_extractions([ok_row])
    assert candidates() == []


def test_runner_extracts_and_writes_typed_row(warehouse):
    warehouse.ensure_manual_finance_tables()
    warehouse.ensure_finance_tables()
    warehouse.insert_manual_finance_documents(
        [_document_row(mime_type="text/csv", filename="export.csv", original_path="acme-checking-0001/export.csv")]
    )
    agent = FakeAgent(_agent_output())
    summary = ManualFinanceExtractionRunner(
        warehouse=warehouse,
        agent=agent,
        object_store_factory=lambda: FakeObjectStore(b"Date,Amount\n2026-06-05,-4.50\n"),
        logger=Logger(),
        provider="codex",
        now=lambda: _TS,
    ).sync(limit=None)
    assert summary.documents_extracted == 1
    assert summary.documents_failed == 0
    # The agent ran with warehouse access and a structured request.
    assert agent.warehouses == [warehouse]
    assert agent.requests[0].task_type == "manual_finance_extraction"
    rows = warehouse._query(
        """
        SELECT status, document_type, institution, account_mask, period_end,
               closing_balance, transactions_json -> 0 ->> 'description'
        FROM manual_finance_extractions
        """
    )
    assert rows == [
        (
            STATUS_OK,
            "bank_statement",
            "Acme Bank",
            "0001",
            date(2026, 6, 30),
            Decimal("1234.56"),
            "COFFEE SHOP",
        )
    ]
    # Idempotent: the completed doc is no longer a candidate.
    assert not has_extraction_candidate(
        warehouse, provider="agent_codex", prompt_version=PROMPT_VERSION
    )


def test_runner_parallel_workers_extract_all_candidates(warehouse):
    warehouse.ensure_manual_finance_tables()
    warehouse.ensure_finance_tables()
    docs = [
        _document_row(
            content_sha256=f"sha-{i}",
            source_native_id=f"sha-{i}",
            filename=f"export-{i}.csv",
            mime_type="text/csv",
            original_path=f"acme-checking-0001/export-{i}.csv",
        )
        for i in range(5)
    ]
    warehouse.insert_manual_finance_documents(docs)
    agent = FakeAgent(_agent_output())
    summary = ManualFinanceExtractionRunner(
        warehouse=warehouse,
        agent=agent,
        object_store_factory=lambda: FakeObjectStore(b"Date,Amount\n2026-06-05,-4.50\n"),
        logger=Logger(),
        provider="codex",
        now=lambda: _TS,
        workers=3,
        warehouse_factory=lambda: PostgresWarehouse(_postgres_url(), schema=warehouse.schema_namespace),
    ).sync(limit=None)
    assert summary.documents_seen == 5
    assert summary.documents_extracted == 5
    assert summary.documents_failed == 0
    assert warehouse._query("SELECT count(*) FROM manual_finance_extractions WHERE status = 'ok'") == [(5,)]
    # Every agent run got a warehouse handle (its read-only tool proxy).
    assert len(agent.warehouses) == 5


def test_runner_parallel_requires_warehouse_factory():
    with pytest.raises(ValueError, match="warehouse_factory"):
        ManualFinanceExtractionRunner(
            warehouse=None,
            agent=None,
            object_store_factory=lambda: None,
            logger=Logger(),
            provider="codex",
            workers=3,
        )


def test_runner_records_unreadable_and_not_useful(warehouse):
    warehouse.ensure_manual_finance_tables()
    warehouse.insert_manual_finance_documents(
        [
            _document_row(
                content_sha256="sha-img",
                source_native_id="sha-img",
                filename="shot.png",
                mime_type="image/png",
                original_path="real-estate/shot.png",
            )
        ]
    )
    summary = ManualFinanceExtractionRunner(
        warehouse=warehouse,
        agent=FakeAgent(_agent_output()),
        object_store_factory=lambda: FakeObjectStore(b"not-an-image"),
        logger=Logger(),
        provider="codex",
        now=lambda: _TS,
    ).sync(limit=None)
    assert summary.documents_unreadable == 1
    rows = warehouse._query("SELECT status FROM manual_finance_extractions")
    assert rows == [(STATUS_UNREADABLE,)]

    # Non-financial documents record not_useful (terminal).
    warehouse._command("DELETE FROM manual_finance_extractions")
    warehouse.insert_manual_finance_documents(
        [
            _document_row(
                content_sha256="sha-csv",
                source_native_id="sha-csv",
                filename="notes.csv",
                mime_type="text/csv",
                original_path="misc/notes.csv",
            )
        ]
    )
    summary = ManualFinanceExtractionRunner(
        warehouse=warehouse,
        agent=FakeAgent(_agent_output(is_financial=False, transactions=[], balances=[])),
        object_store_factory=lambda: FakeObjectStore(b"a,b\n1,2\n"),
        logger=Logger(),
        provider="codex",
        now=lambda: _TS,
    ).sync(limit=None)
    assert summary.documents_not_useful == 1
