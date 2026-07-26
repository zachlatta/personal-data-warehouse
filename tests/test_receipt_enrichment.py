from __future__ import annotations

from datetime import UTC, datetime
import json

from personal_data_warehouse.receipt_enrichment import (
    DECISION_FOUND,
    DECISION_INSUFFICIENT,
    DECISION_NOT_RECEIPTABLE,
    DECISION_NOT_FOUND,
    PROMPT_VERSION,
    SOURCE_GMAIL_ATTACHMENT,
    SOURCE_GMAIL_MESSAGE,
    SOURCE_PHOTO,
    merge_usage,
    record_id_for,
    transaction_prompt,
    transaction_receipt_row,
    transaction_schema,
    usage_from_events,
)

NOW = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)
TRANSACTION = {
    "transaction_id": "ft_1",
    "account_id": "fa_1",
    "account_name": "Everyday Card",
    "account_kind": "credit",
    "institution": "Example Bank",
    "mask": "1234",
    "posted_at": datetime(2026, 7, 25, 12, 0, tzinfo=UTC),
    "amount": "-30.79",
    "currency": "USD",
    "merchant": "Example Cafe",
    "description": "EXAMPLE CAFE",
    "source": "plaid",
}


def _result(**overrides):
    result = {
        "decision": DECISION_FOUND,
        "sources_searched": [SOURCE_PHOTO, SOURCE_GMAIL_MESSAGE, SOURCE_GMAIL_ATTACHMENT],
        "receipt": {
            "primary_source": SOURCE_PHOTO,
            "primary_native_id": "ph_1",
            "evidence": [
                {
                    "source": SOURCE_PHOTO,
                    "native_id": "ph_1",
                    "role": "primary",
                    "why": "The photographed receipt shows the exact settled total.",
                },
                {
                    "source": SOURCE_GMAIL_MESSAGE,
                    "native_id": "gm_1",
                    "role": "corroborating",
                    "why": "The order confirmation names the same items.",
                },
            ],
            "merchant_name": "Example Cafe",
            "merchant_location": "Springfield, XX",
            "purchased_at": "2026-07-23",
            "currency": "usd",
            "total": "30.79",
            "subtotal": "29.00",
            "tax": "0.79",
            "tip": "1.00",
            "amount_charged_to_card": "",
            "card_last4": "1234",
            "order_id": "",
            "line_items": [
                {"description": "Mortadella", "quantity": "1", "amount": "12.00"}
            ],
            "summary": "Lunch at Example Cafe.",
            "record_confidence": "high",
            "match_confidence": "high",
            "match_reason": "The date, merchant, currency, and exact settled total agree.",
        },
        "reasoning": "The photo is legible and the email independently corroborates it.",
    }
    result.update(overrides)
    return result


def _row(result=None, *, attempt_count=0, known_evidence=None, transaction=None):
    return transaction_receipt_row(
        result or _result(),
        transaction=transaction or TRANSACTION,
        attempt_count=attempt_count,
        max_attempts=2,
        known_evidence=known_evidence
        if known_evidence is not None
        else {(SOURCE_PHOTO, "ph_1"), (SOURCE_GMAIL_MESSAGE, "gm_1")},
        provider="codex",
        model="gpt-5.6-terra",
        agent_run_id="run-1",
        elapsed_ms=1234,
        now=NOW,
    )


def test_record_id_is_stable_and_transaction_scoped():
    assert record_id_for("ft_1") == record_id_for("ft_1")
    assert record_id_for("ft_1") != record_id_for("ft_2")
    assert record_id_for("ft_1").startswith("rr_")


def test_transaction_schema_is_closed_and_supports_source_evidence():
    schema = transaction_schema()
    assert schema["additionalProperties"] is False
    assert schema["required"] == ["decision", "sources_searched", "receipt", "reasoning"]
    assert schema["properties"]["decision"]["enum"] == [
        DECISION_FOUND,
        DECISION_NOT_FOUND,
        DECISION_NOT_RECEIPTABLE,
        DECISION_INSUFFICIENT,
    ]
    evidence = schema["properties"]["receipt"]["properties"]["evidence"]["items"]
    assert evidence["properties"]["source"]["enum"] == [
        SOURCE_PHOTO,
        SOURCE_GMAIL_MESSAGE,
        SOURCE_GMAIL_ATTACHMENT,
    ]
    assert evidence["additionalProperties"] is False


def test_prompt_is_transaction_first_and_requires_real_photo_inspection():
    prompt = transaction_prompt(TRANSACTION)
    assert "ft_1" in prompt
    assert "Example Cafe" in prompt
    assert "pdw sql" in prompt
    assert "search.search_text" in prompt
    assert "gmail.messages" in prompt
    assert "gmail.attachments.attachment_id" in prompt
    assert "message_id is not an attachment_id" in prompt
    assert "Brokerage, security, and cryptocurrency trades" in prompt
    assert "marts.photos" in prompt
    assert "pdw call get_object" in prompt
    assert "inspect the actual image" in prompt
    assert "caption" in prompt
    assert "artifact triage" not in prompt.lower()


def test_found_receipt_is_normalized_and_settles_immediately():
    row = _row()
    assert row["transaction_id"] == "ft_1"
    assert row["record_id"] == record_id_for("ft_1")
    assert row["decision"] == DECISION_FOUND
    assert row["primary_source"] == SOURCE_PHOTO
    assert row["primary_native_id"] == "ph_1"
    assert row["currency"] == "USD"
    assert row["total"] == "30.79"
    assert row["purchased_at"] == "2026-07-23"
    assert row["attempt_count"] == 1
    assert row["settled"] == 1
    assert row["ai_prompt_version"] == PROMPT_VERSION
    assert json.loads(row["line_items_json"])[0]["description"] == "Mortadella"
    evidence = json.loads(row["evidence_json"])
    assert {(item["source"], item["native_id"]) for item in evidence} == {
        (SOURCE_PHOTO, "ph_1"),
        (SOURCE_GMAIL_MESSAGE, "gm_1"),
    }


def test_unverified_evidence_cannot_publish_a_receipt():
    row = _row(known_evidence=set())
    assert row["decision"] == DECISION_INSUFFICIENT
    assert row["record_id"] == ""
    assert row["primary_source"] == ""
    assert row["summary"] == ""
    assert row["settled"] == 0
    assert json.loads(row["raw_result_json"])["decision"] == DECISION_FOUND


def test_only_high_confidence_matches_publish_receipts():
    result = _result(
        receipt={**_result()["receipt"], "match_confidence": "medium"}
    )
    row = _row(result)
    assert row["decision"] == DECISION_INSUFFICIENT
    assert row["record_id"] == ""
    assert row["settled"] == 0


def test_invalid_money_and_dates_are_stored_as_absent_sentinels():
    result = _result(
        receipt={
            **_result()["receipt"],
            "total": "see attached",
            "purchased_at": "last Tuesday",
            "tip": "",
        }
    )
    row = _row(result)
    assert row["total"] == "0"
    assert row["purchased_at"] == "1970-01-01"
    assert row["tip"] == "0"


def test_no_receipt_gets_one_retry_then_settles():
    result = _result(
        decision=DECISION_NOT_FOUND,
        receipt={},
        reasoning="Searched the relevant photo and email windows without finding evidence.",
    )
    first = _row(result, attempt_count=0)
    assert first["decision"] == DECISION_NOT_FOUND
    assert first["settled"] == 0
    assert first["record_id"] == ""

    second = _row(result, attempt_count=1)
    assert second["attempt_count"] == 2
    assert second["settled"] == 1


def test_non_purchase_transaction_settles_without_receipt_fields():
    row = _row(
        _result(
            decision=DECISION_NOT_RECEIPTABLE,
            sources_searched=[],
            receipt={},
            reasoning="This is an account transfer, not a purchase.",
        )
    )
    assert row["decision"] == DECISION_NOT_RECEIPTABLE
    assert row["settled"] == 1
    assert row["record_id"] == ""
    assert row["summary"] == ""


def test_non_retail_brokerage_activity_cannot_publish_a_receipt():
    transaction = {
        **TRANSACTION,
        "account_kind": "brokerage",
        "merchant": "",
        "description": "buy shares of an index fund - PURCHASED",
    }
    row = _row(_result(), transaction=transaction)
    assert row["decision"] == DECISION_NOT_RECEIPTABLE
    assert row["record_id"] == ""
    assert row["merchant_name"] == ""
    assert row["settled"] == 1
    assert "brokerage" in row["reasoning"].lower()
    assert json.loads(row["raw_result_json"])["decision"] == DECISION_FOUND


def test_brokerage_cash_account_with_a_merchant_can_still_publish_a_receipt():
    transaction = {
        **TRANSACTION,
        "account_kind": "brokerage",
        "merchant": "Example Cafe",
    }
    assert _row(_result(), transaction=transaction)["decision"] == DECISION_FOUND


def test_unknown_decision_is_retried_as_insufficient_evidence():
    row = _row(_result(decision="invented", receipt={}))
    assert row["decision"] == DECISION_INSUFFICIENT
    assert row["settled"] == 0


def test_usage_only_counts_turn_completed_events():
    class Event:
        def __init__(self, payload):
            self.event_json = payload

    events = [
        Event({"type": "turn.completed", "usage": {"input_tokens": 10, "output_tokens": 2}}),
        Event({"type": "turn.started", "usage": {"input_tokens": 999, "output_tokens": 999}}),
        Event({"type": "item.completed"}),
        Event("not a mapping"),
    ]
    assert usage_from_events(events) == {
        "input_tokens": 10,
        "cached_input_tokens": 0,
        "output_tokens": 2,
    }


def test_merge_usage_accumulates():
    totals = {"input_tokens": 5}
    merge_usage(totals, {"input_tokens": 3, "output_tokens": 1})
    assert totals == {"input_tokens": 8, "output_tokens": 1}


def test_row_builder_covers_the_transaction_receipt_table():
    from personal_data_warehouse.schema import RECEIPT_TRANSACTION_RECEIPT_COLUMNS

    assert set(RECEIPT_TRANSACTION_RECEIPT_COLUMNS) <= set(_row())
