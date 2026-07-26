from __future__ import annotations

from datetime import UTC, datetime, timedelta
import logging

import pytest

from personal_data_warehouse.receipt_enrichment import (
    DECISION_MATCHED,
    DECISION_NO_MATCH,
    SOURCE_GMAIL_MESSAGE,
    SOURCE_PHOTO,
    VERDICT_PURCHASE,
    ReceiptEnrichmentRunner,
)

NOW = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)


class FakeResult:
    def __init__(self, payload, *, exit_code=0, error=""):
        self.final_output_json = payload
        self.exit_code = exit_code
        self.error = error
        self.events = [
            {"type": "turn.completed", "usage": {"input_tokens": 100, "output_tokens": 10}}
        ]


class FakeAgent:
    """Records every request and replays canned responses by task_type."""

    def __init__(self, triage=None, enrichment=None):
        self._triage = triage or {"verdicts": []}
        self._enrichment = enrichment or {}
        self.requests = []

    def run(self, request):
        self.requests.append(request)
        return FakeResult(self._triage)

    def run_with_pdw(self, request, **kwargs):
        self.requests.append(request)
        payload = self._enrichment
        if callable(payload):
            payload = payload(request)
        return FakeResult(payload)


class FakeWarehouse:
    def __init__(self, rows_by_marker):
        self.rows_by_marker = rows_by_marker
        self.triage_rows = []
        self.records = []
        self.links = []
        self.queries = []
        self.ensured = False

    def ensure_receipt_tables(self):
        self.ensured = True

    def _query_dicts(self, sql, params=None):
        self.queries.append((sql, params))
        for marker, rows in self.rows_by_marker.items():
            if marker in sql:
                return rows(params) if callable(rows) else rows
        return []

    def insert_receipt_triage(self, rows):
        self.triage_rows.extend(rows)

    def insert_receipt_records(self, rows):
        self.records.extend(rows)

    def insert_receipt_transaction_links(self, rows):
        self.links.extend(rows)


def _runner(warehouse, agent, **kwargs):
    return ReceiptEnrichmentRunner(
        warehouse=warehouse,
        agent=agent,
        logger=logging.getLogger("test"),
        provider="codex",
        model="gpt-5.6-terra",
        now=NOW,
        **kwargs,
    )


def test_triage_only_considers_the_lookback_window():
    warehouse = FakeWarehouse({"FROM clean_photos": [], "FROM gmail_messages": [], "FROM gmail_attachments": []})
    agent = FakeAgent()
    _runner(warehouse, agent, lookback_days=30).sync()
    since_values = {params["since"] for sql, params in warehouse.queries if params and "since" in params}
    assert since_values, "candidate queries must be time-bounded"
    for since in since_values:
        assert since == NOW - timedelta(days=30)


def test_triage_writes_a_row_per_artifact_and_counts_purchases():
    photo_rows = [
        {"native_id": "ph_1", "occurred_at": NOW - timedelta(days=5),
         "body": "Document type: photo: receipt\n\nSummary: a slip"},
        {"native_id": "ph_2", "occurred_at": NOW - timedelta(days=6),
         "body": "Document type: photo: landscape\n\nSummary: a hill"},
    ]
    warehouse = FakeWarehouse({
        "FROM clean_photos AS p\nLEFT JOIN": photo_rows,
        "FROM gmail_messages AS m\nLEFT JOIN": [],
        "FROM gmail_attachments AS a": [],
        "FROM receipt_triage AS t": [],
    })
    agent = FakeAgent(triage={"verdicts": [
        {"artifact_id": "photo:ph_1", "verdict": "purchase_record", "reason": "receipt"},
        {"artifact_id": "photo:ph_2", "verdict": "not_a_purchase", "reason": "landscape"},
    ]})
    summary = _runner(warehouse, agent).sync()

    assert warehouse.ensured
    assert summary.triaged == 2
    assert summary.purchase_records == 1
    assert {row["native_id"] for row in warehouse.triage_rows} == {"ph_1", "ph_2"}
    assert summary.usage["input_tokens"] > 0


def test_triage_prompt_excludes_the_full_ocr():
    body = "Document type: photo: receipt\n\nSummary: a slip\n\nVisible text:\nSECRET-TOTAL-LINE\n"
    warehouse = FakeWarehouse({
        "FROM clean_photos AS p\nLEFT JOIN": [
            {"native_id": "ph_1", "occurred_at": NOW - timedelta(days=5), "body": body}
        ],
        "FROM gmail_messages AS m\nLEFT JOIN": [],
        "FROM gmail_attachments AS a": [],
        "FROM receipt_triage AS t": [],
    })
    agent = FakeAgent(triage={"verdicts": []})
    _runner(warehouse, agent).sync()
    triage_prompts = [r.prompt for r in agent.requests if r.task_type == "receipt_triage"]
    assert triage_prompts
    assert "SECRET-TOTAL-LINE" not in triage_prompts[0]


def test_enrichment_writes_record_and_high_confidence_link():
    candidate = {
        "source": SOURCE_PHOTO, "native_id": "ph_1",
        "occurred_at": NOW - timedelta(days=5),
        "record_id": None, "attempt_count": 0, "last_attempt_at": None,
    }
    warehouse = FakeWarehouse({
        "FROM clean_photos AS p\nLEFT JOIN": [],
        "FROM gmail_messages AS m\nLEFT JOIN": [],
        "FROM gmail_attachments AS a": [],
        "FROM receipt_triage AS t": [candidate],
        "FROM clean_photos AS p WHERE": [
            {"text": "EXAMPLE CAFE\nTOTAL 30.79", "title": "", "occurred_at": NOW - timedelta(days=5)}
        ],
        "FROM finance_transactions WHERE": [{"transaction_id": "ft_1"}],
    })
    agent = FakeAgent(enrichment={
        "is_purchase_record": True,
        "receipt": {
            "merchant_name": "Example Cafe", "merchant_location": "Springfield, XX",
            "purchased_at": "2026-07-20", "currency": "USD", "total": "30.79",
            "subtotal": "29.00", "tax": "0.79", "tip": "1.00",
            "amount_charged_to_card": "", "card_last4": "1234", "order_id": "",
            "line_items": [], "summary": "Lunch", "confidence": "high",
        },
        "decision": DECISION_MATCHED,
        "matches": [{"transaction_id": "ft_1", "confidence": "high",
                     "relationship": "exact", "why": "exact cents"}],
        "reasoning": "exact",
    })
    summary = _runner(warehouse, agent).sync()

    assert summary.enriched == 1
    assert summary.matched == 1
    assert summary.trusted_links == 1
    assert warehouse.records[0]["merchant_name"] == "Example Cafe"
    assert warehouse.records[0]["settled"] == 1
    assert warehouse.links[0]["transaction_id"] == "ft_1"


def test_enrichment_drops_a_link_to_a_nonexistent_transaction():
    candidate = {
        "source": SOURCE_PHOTO, "native_id": "ph_1",
        "occurred_at": NOW - timedelta(days=5),
        "record_id": None, "attempt_count": 0, "last_attempt_at": None,
    }
    warehouse = FakeWarehouse({
        "FROM clean_photos AS p\nLEFT JOIN": [],
        "FROM gmail_messages AS m\nLEFT JOIN": [],
        "FROM gmail_attachments AS a": [],
        "FROM receipt_triage AS t": [candidate],
        "FROM clean_photos AS p WHERE": [
            {"text": "slip", "title": "", "occurred_at": NOW}
        ],
        # the ledger does not contain the claimed id
        "FROM finance_transactions WHERE": [],
    })
    agent = FakeAgent(enrichment={
        "is_purchase_record": True,
        "receipt": {
            "merchant_name": "X", "merchant_location": "", "purchased_at": "",
            "currency": "USD", "total": "1.00", "subtotal": "", "tax": "", "tip": "",
            "amount_charged_to_card": "", "card_last4": "", "order_id": "",
            "line_items": [], "summary": "s", "confidence": "high",
        },
        "decision": DECISION_MATCHED,
        "matches": [{"transaction_id": "ft_invented", "confidence": "high",
                     "relationship": "exact", "why": "made up"}],
        "reasoning": "r",
    })
    summary = _runner(warehouse, agent).sync()
    assert summary.enriched == 1
    assert warehouse.links == [], "a hallucinated transaction id must not create a link"
    assert summary.trusted_links == 0


def test_enrichment_candidate_query_applies_settle_and_retry_windows():
    captured = {}

    def capture(params):
        captured.update(params)
        return []

    warehouse = FakeWarehouse({
        "FROM clean_photos AS p\nLEFT JOIN": [],
        "FROM gmail_messages AS m\nLEFT JOIN": [],
        "FROM gmail_attachments AS a": [],
        "FROM receipt_triage AS t": capture,
    })
    _runner(warehouse, FakeAgent(), settle_days=2, retry_after_days=7, max_attempts=2).sync()
    assert captured["settle_before"] == NOW - timedelta(days=2)
    assert captured["retry_before"] == NOW - timedelta(days=7)
    assert captured["max_attempts"] == 2
    assert captured["purchase"] == VERDICT_PURCHASE


def test_container_failure_leaves_the_record_untouched_for_a_later_run():
    class FailingAgent(FakeAgent):
        def run_with_pdw(self, request, **kwargs):
            self.requests.append(request)
            return FakeResult({}, exit_code=1, error="container exited 1")

    candidate = {
        "source": SOURCE_GMAIL_MESSAGE, "native_id": "m1",
        "occurred_at": NOW - timedelta(days=5),
        "record_id": None, "attempt_count": 0, "last_attempt_at": None,
    }
    warehouse = FakeWarehouse({
        "FROM clean_photos AS p\nLEFT JOIN": [],
        "FROM gmail_messages AS m\nLEFT JOIN": [],
        "FROM gmail_attachments AS a": [],
        "FROM receipt_triage AS t": [candidate],
        "FROM gmail_messages AS m WHERE": [{"text": "body", "title": "t", "occurred_at": NOW}],
    })
    summary = _runner(warehouse, FailingAgent()).sync()
    assert summary.failed == 1
    assert warehouse.records == [], "an infra failure is not evidence about the receipt"


def test_defaults_match_the_agreed_cadence():
    from personal_data_warehouse.defs import receipt_enrichment as defs_module

    assert defs_module.receipt_lookback_days() == 30
    assert defs_module.receipt_settle_days() == 2
    assert defs_module.receipt_retry_after_days() == 7
    assert defs_module.receipt_max_attempts() == 2, "one attempt plus one retry"
    assert defs_module.receipt_model() == "gpt-5.6-terra"


def test_definitions_expose_asset_job_and_schedule():
    from personal_data_warehouse.defs import receipt_enrichment as defs_module

    definitions = defs_module.defs()
    assert [a.key.to_user_string() for a in definitions.assets] == ["receipt_enrichment"]
    assert [j.name for j in definitions.jobs] == ["receipt_enrichment_job"]
    assert [s.name for s in definitions.schedules] == ["receipt_enrichment_hourly"]


@pytest.mark.parametrize("value,expected", [("0", False), ("false", False), ("", True), ("1", True)])
def test_enabled_flag(monkeypatch, value, expected):
    from personal_data_warehouse.defs import receipt_enrichment as defs_module

    monkeypatch.setenv(defs_module.RECEIPT_ENABLED_ENV, value)
    assert defs_module.receipt_enrichment_enabled() is expected
