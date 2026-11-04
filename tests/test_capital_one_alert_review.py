"""The agent review pass: it pages each open alert with the posted rows on its
card, records the agent's decision, and the next ledger run applies it."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from personal_data_warehouse.agent_runner import AgentRunResult
from personal_data_warehouse.capital_one_alert_review import (
    ALERT_REVIEW_TASK_TYPE,
    PROMPT_VERSION,
    AlertReviewRunner,
    review_row,
)
from personal_data_warehouse.finance_ledger import FinanceLedgerRunner
from tests.test_capital_one_alerts import email
from tests.test_finance_ledger import (  # noqa: F401  (fixture)
    _plaid_account_row,
    _plaid_item_row,
    _plaid_transaction_row,
    warehouse,
)

PURCHASE = datetime(2026, 9, 16, 18, tzinfo=UTC)


class ChoosingAgent:
    """Answers like an agent would: picks the posted row whose description it was told to."""

    def __init__(self, choose: dict[str, str], *, exit_code: int = 0) -> None:
        self.choose = choose
        self.exit_code = exit_code
        self.prompts: list[str] = []

    def run_with_pdw(self, request, **kwargs):
        self.prompts.append(request.prompt)
        payload = json.loads(request.prompt.split("--- alert and candidates ---\n", 1)[1])
        merchant = payload["alert"]["merchant"]
        wanted = self.choose.get(merchant, "")
        if wanted == "not_posted":
            output = {"decision": "not_posted", "transaction_id": "", "confidence": "high", "reasoning": "hold"}
        else:
            ids = [
                row["transaction_id"]
                for row in payload["posted_transactions_on_this_card"]
                if row["description"] == wanted
            ]
            output = (
                {"decision": "settled_by", "transaction_id": ids[0], "confidence": "high", "reasoning": "tip"}
                if ids
                else {"decision": "undetermined", "transaction_id": "", "confidence": "low", "reasoning": "?"}
            )
        return AgentRunResult(
            run_id=request.run_id, provider="test", model="test", task_type=request.task_type,
            subject_id=request.subject_id, prompt_version=request.prompt_version, input_sha256="sha",
            status="succeeded" if self.exit_code == 0 else "failed",
            final_output_json=output if self.exit_code == 0 else {},
            error="" if self.exit_code == 0 else "boom", exit_code=self.exit_code,
            started_at=PURCHASE, completed_at=PURCHASE, events=[],
        )


def _seed(warehouse, alerts):
    warehouse.ensure_plaid_tables()
    warehouse.insert_plaid_items([_plaid_item_row(institution_name="Capital One")])
    warehouse.insert_plaid_accounts([_plaid_account_row(type="credit", subtype="credit card", mask="1234")])
    warehouse.ensure_tables()
    for alert in alerts:
        warehouse._command(
            """INSERT INTO @gmail_messages (account, message_id, from_address, subject, body_text, payload_json)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (alert["account"], alert["message_id"], alert["from_address"], alert["subject"],
             alert["body_text"], json.dumps(alert["payload_json"])),
        )


def _alert(message_id, merchant, amount):
    base = email(account="z@x.test", message_id=message_id)
    return base | {"body_text": base["body_text"].replace("SP TEST COFFEE", merchant).replace("$5.97", f"${amount}")}


def _runner(warehouse, agent, now):
    return AlertReviewRunner(
        warehouse=warehouse, agent=agent, logger=logging.getLogger("test"),
        provider="test", model="test", now=now,
    )


STATUS = "SELECT amount, source, reconciliation_status FROM @marts_finance_transactions ORDER BY amount, source"


def test_reviewed_tips_and_same_price_repeats_settle_one_to_one(warehouse):
    # Two 4.40 alerts the same day, each tipped to 5.40 on posting, plus a
    # merchant the bank spells longer than the alert does.
    _seed(warehouse, [
        _alert("m1", "CORNER BREWING CO", "4.40"),
        _alert("m2", "CORNER BREWING CO", "4.40"),
        _alert("m3", "TEMP HOLD", "9.99"),
    ])
    warehouse.insert_plaid_transactions([
        _plaid_transaction_row(transaction_id=tid, amount=5.40, name="CORNER BREWING COMPANY",
                               merchant_name="Corner Brewing", posted_at=PURCHASE + timedelta(days=2))
        for tid in ("p1", "p2")
    ])
    FinanceLedgerRunner(warehouse=warehouse, now=PURCHASE).sync()
    assert [row[2] for row in warehouse._query(STATUS)] == [
        "provisional", "posted", "posted", "provisional", "provisional",
    ]

    # Too fresh: the bank may not have posted yet, so nothing is asked.
    agent = ChoosingAgent({"CORNER BREWING CO": "CORNER BREWING COMPANY", "TEMP HOLD": "not_posted"})
    assert _runner(warehouse, agent, PURCHASE + timedelta(days=1)).sync().candidates == 0

    later = PURCHASE + timedelta(days=3)
    summary = _runner(warehouse, agent, later).sync()
    assert (summary.reviewed, summary.settled, summary.not_posted) == (3, 2, 1)
    # Each prompt shows the card's other open alerts, so repeats are seen together.
    assert all("other_unresolved_alerts_on_this_card" in prompt for prompt in agent.prompts)
    assert warehouse._query(
        "SELECT count(*) FROM @agent_runs WHERE task_type = %s", (ALERT_REVIEW_TASK_TYPE,)
    ) == [(3,)]

    # This fake picks the first matching row for both alerts; the ledger must
    # refuse a double claim rather than book one purchase twice.
    FinanceLedgerRunner(warehouse=warehouse, now=later).sync()
    assert warehouse._query(STATUS) == [
        (Decimal("-9.99"), "capital_one_alert", "review_not_posted"),
        (Decimal("-5.40"), "plaid", "posted"),
        (Decimal("-5.40"), "plaid", "posted"),
        (Decimal("-4.40"), "capital_one_alert", "provisional"),
        (Decimal("-4.40"), "capital_one_alert", "provisional"),
    ]
    # Settled reviews whose claim was refused are retried the next day.
    assert _runner(warehouse, agent, later + timedelta(hours=1)).sync().candidates == 0
    assert _runner(warehouse, agent, later + timedelta(days=1, hours=1)).sync().candidates == 2


def test_distinct_choices_settle_both_alerts(warehouse):
    _seed(warehouse, [_alert("m1", "CAFE ONE", "4.40"), _alert("m2", "CAFE TWO", "4.40")])
    warehouse.insert_plaid_transactions([
        _plaid_transaction_row(transaction_id="p1", amount=5.40, name="CAFE ONE LLC", merchant_name="",
                               posted_at=PURCHASE + timedelta(days=2)),
        _plaid_transaction_row(transaction_id="p2", amount=5.40, name="CAFE 2", merchant_name="",
                               posted_at=PURCHASE + timedelta(days=2)),
    ])
    FinanceLedgerRunner(warehouse=warehouse, now=PURCHASE).sync()
    later = PURCHASE + timedelta(days=3)
    _runner(warehouse, ChoosingAgent({"CAFE ONE": "CAFE ONE LLC", "CAFE TWO": "CAFE 2"}), later).sync()
    FinanceLedgerRunner(warehouse=warehouse, now=later).sync()
    assert warehouse._query(STATUS) == [
        (Decimal("-5.40"), "plaid", "posted"),
        (Decimal("-5.40"), "plaid", "posted"),
    ]
    assert warehouse._query(
        "SELECT match_method, count(*) FROM @finance_transaction_links WHERE source = 'capital_one_alert' GROUP BY 1"
    ) == [("alert_agent_review", 2)]
    # Resolved alerts are no longer due.
    assert _runner(warehouse, ChoosingAgent({}), later + timedelta(days=5)).sync().candidates == 0


def test_failed_agent_run_writes_no_decision(warehouse):
    _seed(warehouse, [_alert("m1", "CAFE ONE", "4.40")])
    FinanceLedgerRunner(warehouse=warehouse, now=PURCHASE).sync()
    summary = _runner(warehouse, ChoosingAgent({}, exit_code=1), PURCHASE + timedelta(days=3)).sync()
    assert (summary.failed, summary.reviewed) == (1, 0)
    assert warehouse._query("SELECT count(*) FROM @finance_alert_reviews") == [(0,)]


def test_a_settled_by_outside_the_listed_rows_is_recorded_as_undetermined():
    alert = {"source_row_key": "a|m", "alert_transaction_id": "ft_x", "account_id": "card"}
    row = review_row(
        {"decision": "settled_by", "transaction_id": "invented", "confidence": "high", "reasoning": "r"},
        alert=alert, candidate_ids={"real"}, provider="p", model="m", agent_run_id="run",
        elapsed_ms=1, now=PURCHASE,
    )
    assert (row["decision"], row["transaction_id"], row["decision_confidence"]) == ("undetermined", "", "low")
    assert row["ai_prompt_version"] == PROMPT_VERSION
    assert row["attempt_count"] == 1
