from __future__ import annotations

from datetime import UTC, datetime, timedelta
import logging

import pytest

from personal_data_warehouse.receipt_enrichment import (
    DECISION_FOUND,
    DECISION_INSUFFICIENT,
    DECISION_NOT_FOUND,
    PROMPT_VERSION,
    SOURCE_GMAIL_ATTACHMENT,
    SOURCE_GMAIL_MESSAGE,
    SOURCE_PHOTO,
    ReceiptEnrichmentRunner,
)

NOW = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)


class FakeResult:
    def __init__(self, payload, *, exit_code=0, error=""):
        self.final_output_json = payload
        self.exit_code = exit_code
        self.error = error
        self.events = [
            {"type": "turn.completed", "usage": {"input_tokens": 100, "output_tokens": 10}}
        ]
        # Fields agent_run_row() persists into ops.ai_processing_agent_runs.
        self.run_id = "agent-test"
        self.provider = "codex"
        self.model = "gpt-5.6-terra"
        self.task_type = "receipt_transaction_match"
        self.subject_id = "ft_1"
        self.prompt_version = PROMPT_VERSION
        self.status = "completed" if exit_code == 0 else "error"
        self.input_sha256 = ""
        self.started_at = NOW
        self.completed_at = NOW


class FakeAgent:
    def __init__(self, result=None):
        self._result = result or {}
        self.requests = []

    def run(self, request):
        raise AssertionError("receipt matching must never run a separate non-PDW extraction step")

    def run_with_pdw(self, request, **kwargs):
        self.requests.append(request)
        payload = self._result(request) if callable(self._result) else self._result
        return FakeResult(payload)


class FakeWarehouse:
    def __init__(self, rows_by_marker):
        self.rows_by_marker = rows_by_marker
        self.rows = []
        self.queries = []
        self.ensured = False
        self.agent_runs = []

    def ensure_receipt_tables(self):
        self.ensured = True

    def ensure_agent_tables(self):
        pass

    def _query_dicts(self, sql, params=None):
        self.queries.append((sql, params))
        for marker, rows in self.rows_by_marker.items():
            if marker in sql:
                return rows(params) if callable(rows) else rows
        return []

    def insert_receipt_transaction_receipts(self, rows):
        self.rows.extend(rows)

    def insert_agent_runs(self, rows):
        self.agent_runs.extend(rows)


def _transaction(transaction_id="ft_1", **overrides):
    row = {
        "transaction_id": transaction_id,
        "account_id": "fa_1",
        "account_name": "Everyday Card",
        "account_kind": "credit",
        "institution": "Example Bank",
        "mask": "1234",
        "posted_at": NOW - timedelta(days=1),
        "amount": "-30.79",
        "currency": "USD",
        "merchant": "Example Cafe",
        "description": "EXAMPLE CAFE",
        "source": "plaid",
        "attempt_count": 0,
        "last_attempt_at": None,
    }
    row.update(overrides)
    return row


def _found_result():
    return {
        "decision": DECISION_FOUND,
        "sources_searched": [SOURCE_PHOTO, SOURCE_GMAIL_MESSAGE],
        "receipt": {
            "primary_source": SOURCE_PHOTO,
            "primary_native_id": "ph_1",
            "evidence": [
                {
                    "source": SOURCE_PHOTO,
                    "native_id": "ph_1",
                    "role": "primary",
                    "why": "exact total",
                }
            ],
            "merchant_name": "Example Cafe",
            "merchant_location": "",
            "purchased_at": "2026-07-25",
            "currency": "USD",
            "total": "30.79",
            "subtotal": "",
            "tax": "",
            "tip": "",
            "amount_charged_to_card": "",
            "card_last4": "1234",
            "order_id": "",
            "line_items": [],
            "summary": "Lunch.",
            "record_confidence": "high",
            "match_confidence": "high",
            "match_reason": "Exact merchant, date, and settled amount.",
        },
        "reasoning": "The image and transaction agree.",
    }


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


def test_worklist_is_recent_posted_transactions_not_artifacts():
    captured = {}

    def capture(params):
        captured.update(params)
        return []

    warehouse = FakeWarehouse({"FROM @finance_transactions AS t": capture})
    _runner(warehouse, FakeAgent()).sync()

    assert warehouse.ensured
    assert captured["since"] == NOW - timedelta(days=30)
    candidate_sql = next(sql for sql, _ in warehouse.queries if "FROM @finance_transactions AS t" in sql)
    assert "t.posted_at >= %(since)s" in candidate_sql
    assert "t.pending = 0" in candidate_sql
    assert "clean_photos" not in candidate_sql
    assert "gmail_messages" not in candidate_sql
    assert "receipt_triage" not in candidate_sql


def test_agent_runs_are_recorded_for_monitoring():
    # Every other agent-backed enrichment persists its runs into
    # ops.ai_processing_agent_runs; receipts silently didn't, so 197 decisions
    # over weeks were invisible to the shared agent-run failure monitoring.
    warehouse = FakeWarehouse({"FROM @finance_transactions AS t": [_transaction()]})

    _runner(warehouse, FakeAgent(_found_result())).sync()

    assert len(warehouse.agent_runs) == 1
    run = warehouse.agent_runs[0]
    assert run["task_type"] == "receipt_transaction_match"
    assert run["subject_id"] == "ft_1"
    assert run["status"] == "completed"


def test_one_pdw_agent_operation_per_transaction():
    transactions = [_transaction("ft_1"), _transaction("ft_2", amount="-12.00")]
    warehouse = FakeWarehouse(
        {
            "FROM @finance_transactions AS t": transactions,
            "FROM @clean_photos WHERE": lambda params: [
                {"native_id": native_id} for native_id in params[0] if native_id == "ph_1"
            ],
            "FROM @gmail_messages WHERE": [],
            "FROM @gmail_attachments WHERE": [],
        }
    )
    agent = FakeAgent(_found_result())

    summary = _runner(warehouse, agent).sync()

    assert len(agent.requests) == 2
    assert {request.subject_id for request in agent.requests} == {"ft_1", "ft_2"}
    assert {request.task_type for request in agent.requests} == {"receipt_transaction_match"}
    assert all("pdw sql" in request.prompt for request in agent.requests)
    assert summary.researched == 2
    assert summary.receipts_found == 2
    assert len(warehouse.rows) == 2
    assert {row["transaction_id"] for row in warehouse.rows} == {"ft_1", "ft_2"}


def test_receipt_is_not_published_when_agent_invents_evidence():
    warehouse = FakeWarehouse(
        {
            "FROM @finance_transactions AS t": [_transaction()],
            "FROM @clean_photos WHERE": [],
            "FROM @gmail_messages WHERE": [],
            "FROM @gmail_attachments WHERE": [],
        }
    )
    summary = _runner(warehouse, FakeAgent(_found_result())).sync()

    assert warehouse.rows[0]["decision"] == DECISION_INSUFFICIENT
    assert warehouse.rows[0]["record_id"] == ""
    assert summary.receipts_found == 0
    assert summary.insufficient == 1


def test_gmail_attachment_evidence_is_validated_by_attachment_id():
    result = _found_result()
    result["receipt"] = {
        **result["receipt"],
        "primary_source": SOURCE_GMAIL_ATTACHMENT,
        "primary_native_id": "att_1",
        "evidence": [
            {
                "source": SOURCE_GMAIL_ATTACHMENT,
                "native_id": "att_1",
                "role": "primary",
                "why": "The attached invoice shows the exact total.",
            }
        ],
    }
    warehouse = FakeWarehouse(
        {
            "FROM @finance_transactions AS t": [_transaction()],
            "FROM @gmail_attachments WHERE": [{"native_id": "att_1"}],
        }
    )

    summary = _runner(warehouse, FakeAgent(result)).sync()

    assert summary.receipts_found == 1
    assert warehouse.rows[0]["primary_native_id"] == "att_1"
    attachment_sql = next(
        sql for sql, _ in warehouse.queries if "FROM @gmail_attachments WHERE" in sql
    )
    assert "SELECT attachment_id AS native_id" in attachment_sql
    assert "WHERE attachment_id = ANY(%s)" in attachment_sql


def test_no_receipt_result_is_durable_and_retryable():
    result = {
        "decision": DECISION_NOT_FOUND,
        "sources_searched": [SOURCE_PHOTO, SOURCE_GMAIL_MESSAGE],
        "receipt": {},
        "reasoning": "No relevant source evidence found.",
    }
    warehouse = FakeWarehouse({"FROM @finance_transactions AS t": [_transaction()]})
    summary = _runner(warehouse, FakeAgent(result)).sync()

    assert summary.not_found == 1
    assert warehouse.rows[0]["decision"] == DECISION_NOT_FOUND
    assert warehouse.rows[0]["settled"] == 0


def test_retry_window_and_budget_are_applied_to_the_transaction_row():
    captured = {}

    def capture(params):
        captured.update(params)
        return []

    warehouse = FakeWarehouse({"FROM @finance_transactions AS t": capture})
    _runner(warehouse, FakeAgent(), retry_after_days=7, max_attempts=2).sync()
    assert captured["retry_before"] == NOW - timedelta(days=7)
    assert captured["max_attempts"] == 2


def test_prompt_change_requeues_recent_rows_before_unresearched_transactions():
    captured = {}

    def capture(params):
        captured.update(params)
        return []

    warehouse = FakeWarehouse({"FROM @finance_transactions AS t": capture})
    _runner(warehouse, FakeAgent()).sync()
    candidate_sql = next(
        sql for sql, _ in warehouse.queries if "FROM @finance_transactions AS t" in sql
    )

    assert captured["prompt_version"] == PROMPT_VERSION
    assert captured["found_decision"] == DECISION_FOUND
    assert "brokerage" in captured["non_retail_account_kinds"]
    assert "r.decision IS DISTINCT FROM %(found_decision)s" in candidate_sql
    assert "a.kind = ANY(%(non_retail_account_kinds)s)" in candidate_sql
    assert (
        "r.transaction_id IS NOT NULL\n"
        "              AND r.ai_prompt_version IS DISTINCT FROM %(prompt_version)s\n"
        "              AND"
        in candidate_sql
    )


def test_prompt_change_starts_a_fresh_negative_retry_budget():
    result = {
        "decision": DECISION_NOT_FOUND,
        "sources_searched": [SOURCE_PHOTO, SOURCE_GMAIL_MESSAGE],
        "receipt": {},
        "reasoning": "No relevant source evidence found.",
    }
    warehouse = FakeWarehouse(
        {
            "FROM @finance_transactions AS t": [
                _transaction(
                    attempt_count=2,
                    prior_prompt_version="receipt-transaction-research-old",
                )
            ]
        }
    )
    _runner(warehouse, FakeAgent(result)).sync()

    assert warehouse.rows[0]["attempt_count"] == 1
    assert warehouse.rows[0]["settled"] == 0


def test_agent_failure_leaves_transaction_unwritten_for_next_run():
    class FailingAgent(FakeAgent):
        def run_with_pdw(self, request, **kwargs):
            self.requests.append(request)
            return FakeResult({}, exit_code=1, error="container exited 1")

    warehouse = FakeWarehouse({"FROM @finance_transactions AS t": [_transaction()]})
    summary = _runner(warehouse, FailingAgent()).sync()
    assert summary.failed == 1
    assert warehouse.rows == []


def test_defaults_are_a_hard_30_day_window(monkeypatch):
    from personal_data_warehouse.defs import receipt_enrichment as defs_module

    monkeypatch.setenv("RECEIPT_LOOKBACK_DAYS", "730")
    assert defs_module.receipt_lookback_days() == 30
    assert defs_module.receipt_retry_after_days() == 7
    assert defs_module.receipt_max_attempts() == 2
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
