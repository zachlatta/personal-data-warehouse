from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json

from personal_data_warehouse.receipt_enrichment import (
    DECISION_INSUFFICIENT,
    DECISION_MATCHED,
    DECISION_NO_MATCH,
    DECISION_NOT_A_PURCHASE,
    ENRICHMENT_PROMPT_VERSION,
    TRIAGE_PROMPT_VERSION,
    VERDICT_NOT_PURCHASE,
    VERDICT_PURCHASE,
    TriageCandidate,
    attachment_descriptor,
    enrichment_prompt,
    enrichment_schema,
    gmail_descriptor,
    link_rows,
    merge_usage,
    photo_descriptor,
    record_id_for,
    record_row,
    triage_prompt,
    triage_rows,
    triage_schema,
    usage_from_events,
)

NOW = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)


def _candidate(source: str = "photo", native_id: str = "ph_1") -> TriageCandidate:
    return TriageCandidate(
        source=source,
        native_id=native_id,
        occurred_at=NOW - timedelta(days=3),
        descriptor="Document type: photo: receipt",
        kind="photo",
    )


def _result(**overrides):
    result = {
        "is_purchase_record": True,
        "receipt": {
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
            "line_items": [{"description": "Mortadella", "quantity": "1", "amount": "12.00"}],
            "summary": "Lunch at Example Cafe",
            "confidence": "high",
        },
        "decision": DECISION_MATCHED,
        "matches": [
            {
                "transaction_id": "ft_1",
                "confidence": "high",
                "relationship": "exact",
                "why": "exact cents",
            }
        ],
        "reasoning": "one exact match",
    }
    result.update(overrides)
    return result


# --- identity -------------------------------------------------------------


def test_record_id_is_stable_and_source_scoped():
    assert record_id_for("photo", "ph_1") == record_id_for("photo", "ph_1")
    assert record_id_for("photo", "ph_1") != record_id_for("gmail_message", "ph_1")
    assert record_id_for("photo", "ph_1").startswith("rr_")


# --- triage ---------------------------------------------------------------


def test_triage_rows_ignore_unknown_ids_and_keep_first_verdict():
    candidate = _candidate()
    candidates = {candidate.artifact_id: candidate}
    rows = triage_rows(
        [
            {"artifact_id": candidate.artifact_id, "verdict": VERDICT_PURCHASE, "reason": "receipt"},
            # a batched response echoing the same id must not flip the verdict
            {"artifact_id": candidate.artifact_id, "verdict": VERDICT_NOT_PURCHASE, "reason": "oops"},
            {"artifact_id": "photo:never-asked", "verdict": VERDICT_PURCHASE, "reason": "hallucinated"},
        ],
        candidates=candidates,
        provider="codex",
        model="gpt-5.6-terra",
        agent_run_id="run-1",
        decided_at=NOW,
    )
    assert len(rows) == 1
    assert rows[0]["native_id"] == "ph_1"
    assert rows[0]["verdict"] == VERDICT_PURCHASE
    assert rows[0]["ai_prompt_version"] == TRIAGE_PROMPT_VERSION
    assert rows[0]["occurred_at"] == candidate.occurred_at


def test_triage_prompt_lists_every_artifact_id():
    candidates = [_candidate(native_id=f"ph_{i}") for i in range(3)]
    prompt = triage_prompt(
        [
            {
                "artifact_id": c.artifact_id,
                "kind": c.kind,
                "occurred_at": c.occurred_at,
                "descriptor": c.descriptor,
            }
            for c in candidates
        ]
    )
    for candidate in candidates:
        assert candidate.artifact_id in prompt
    assert "purchase_record" in prompt


def test_triage_schema_requires_one_verdict_shape():
    schema = triage_schema()
    item = schema["properties"]["verdicts"]["items"]
    assert item["required"] == ["artifact_id", "verdict", "reason"]
    assert item["additionalProperties"] is False


# --- record rows ----------------------------------------------------------


def test_record_row_normalizes_money_and_dates():
    row = record_row(
        _result(),
        source="photo",
        native_id="ph_1",
        occurred_at=NOW,
        attempt_count=0,
        max_attempts=2,
        provider="codex",
        model="gpt-5.6-terra",
        agent_run_id="run-1",
        elapsed_ms=1234,
        now=NOW,
    )
    assert row["total"] == "30.79"
    assert row["purchased_at"] == "2026-07-23"
    assert row["currency"] == "USD"
    assert row["is_purchase_record"] == 1
    assert row["attempt_count"] == 1
    assert row["ai_prompt_version"] == ENRICHMENT_PROMPT_VERSION
    assert json.loads(row["line_items_json"])[0]["description"] == "Mortadella"


def test_record_row_drops_unparseable_money_and_dates():
    row = record_row(
        _result(
            receipt={
                **_result()["receipt"],
                "total": "see attached",
                "purchased_at": "last Tuesday",
                "tip": "",
            }
        ),
        source="photo",
        native_id="ph_1",
        occurred_at=NOW,
        attempt_count=0,
        max_attempts=2,
        provider="codex",
        model="gpt-5.6-terra",
        agent_run_id="run-1",
        elapsed_ms=1,
        now=NOW,
    )
    # storage holds sentinels, never NULL (warehouse convention); the marts
    # views map these back to NULL so "absent" stays distinguishable from zero
    assert row["total"] == "0"
    assert row["purchased_at"] == "1970-01-01"
    assert row["tip"] == "0"


def test_matched_record_settles_immediately():
    row = record_row(
        _result(),
        source="photo", native_id="ph_1", occurred_at=NOW,
        attempt_count=0, max_attempts=2, provider="codex",
        model="m", agent_run_id="r", elapsed_ms=1, now=NOW,
    )
    assert row["settled"] == 1


def test_no_match_stays_open_for_its_single_retry_then_settles():
    first = record_row(
        _result(decision=DECISION_NO_MATCH, matches=[]),
        source="photo", native_id="ph_1", occurred_at=NOW,
        attempt_count=0, max_attempts=2, provider="codex",
        model="m", agent_run_id="r", elapsed_ms=1, now=NOW,
    )
    assert first["attempt_count"] == 1
    assert first["settled"] == 0, "a charge may still post; it gets one retry"

    second = record_row(
        _result(decision=DECISION_NO_MATCH, matches=[]),
        source="photo", native_id="ph_1", occurred_at=NOW,
        attempt_count=1, max_attempts=2, provider="codex",
        model="m", agent_run_id="r", elapsed_ms=1, now=NOW,
    )
    assert second["attempt_count"] == 2
    assert second["settled"] == 1, "retry budget spent"


def test_not_a_purchase_settles_without_burning_the_retry():
    row = record_row(
        _result(is_purchase_record=False, decision=DECISION_NOT_A_PURCHASE, matches=[]),
        source="gmail_message", native_id="m1", occurred_at=NOW,
        attempt_count=0, max_attempts=2, provider="codex",
        model="m", agent_run_id="r", elapsed_ms=1, now=NOW,
    )
    assert row["settled"] == 1
    assert row["is_purchase_record"] == 0


def test_insufficient_evidence_is_retried():
    row = record_row(
        _result(decision=DECISION_INSUFFICIENT, matches=[]),
        source="photo", native_id="ph_1", occurred_at=NOW,
        attempt_count=0, max_attempts=2, provider="codex",
        model="m", agent_run_id="r", elapsed_ms=1, now=NOW,
    )
    assert row["settled"] == 0


# --- link rows ------------------------------------------------------------


def test_only_high_confidence_links_are_persisted():
    rows = link_rows(
        _result(
            matches=[
                {"transaction_id": "ft_high", "confidence": "high", "relationship": "exact", "why": "a"},
                {"transaction_id": "ft_med", "confidence": "medium", "relationship": "fx", "why": "b"},
                {"transaction_id": "ft_low", "confidence": "low", "relationship": "guess", "why": "c"},
            ]
        ),
        record_id="rr_1",
        agent_run_id="run-1",
        now=NOW,
    )
    assert [row["transaction_id"] for row in rows] == ["ft_high"]


def test_links_require_a_matched_decision():
    assert link_rows(
        _result(decision=DECISION_NO_MATCH),
        record_id="rr_1", agent_run_id="run-1", now=NOW,
    ) == []


def test_links_drop_hallucinated_transaction_ids():
    rows = link_rows(
        _result(
            matches=[
                {"transaction_id": "ft_real", "confidence": "high", "relationship": "exact", "why": "a"},
                {"transaction_id": "ft_invented", "confidence": "high", "relationship": "exact", "why": "b"},
            ]
        ),
        record_id="rr_1",
        agent_run_id="run-1",
        known_transaction_ids={"ft_real"},
        now=NOW,
    )
    assert [row["transaction_id"] for row in rows] == ["ft_real"]


def test_links_dedupe_repeated_transaction_ids():
    rows = link_rows(
        _result(
            matches=[
                {"transaction_id": "ft_1", "confidence": "high", "relationship": "exact", "why": "a"},
                {"transaction_id": "ft_1", "confidence": "high", "relationship": "exact", "why": "a"},
            ]
        ),
        record_id="rr_1", agent_run_id="run-1", now=NOW,
    )
    assert len(rows) == 1


def test_split_shipments_keep_every_high_confidence_leg():
    rows = link_rows(
        _result(
            matches=[
                {"transaction_id": "ft_a", "confidence": "high", "relationship": "shipment 1", "why": "a"},
                {"transaction_id": "ft_b", "confidence": "high", "relationship": "shipment 2", "why": "b"},
            ]
        ),
        record_id="rr_1", agent_run_id="run-1", now=NOW,
    )
    assert {row["transaction_id"] for row in rows} == {"ft_a", "ft_b"}


# --- descriptors ----------------------------------------------------------


def test_photo_descriptor_keeps_headers_not_the_whole_ocr():
    caption = (
        "AI attachment extraction\n\n"
        "Document type: photo: receipt\n\n"
        "Summary: A hand holds a takeout receipt.\n\n"
        "Visible text:\nEXAMPLE CAFE\n" + "line\n" * 200
    )
    descriptor = photo_descriptor({"body": caption})
    assert "Document type: photo: receipt" in descriptor
    assert "Summary:" in descriptor
    assert "EXAMPLE CAFE" not in descriptor
    assert len(descriptor) <= 400


def test_gmail_and_attachment_descriptors_are_compact():
    gmail = gmail_descriptor(
        {"from_address": "auto-confirm@amazon.com", "subject": "Ordered: 1 item", "snippet": "x" * 500}
    )
    assert "auto-confirm@amazon.com" in gmail
    assert len(gmail) < 400
    attachment = attachment_descriptor(
        {"filename": "invoice.pdf", "subject": "Invoice", "from_address": "billing@x.com", "head": "y" * 500}
    )
    assert "invoice.pdf" in attachment
    assert len(attachment) < 400


# --- prompt / schema contracts -------------------------------------------


def test_enrichment_prompt_names_the_real_query_tool():
    prompt = enrichment_prompt(source="photo", title="t", observed_at="2026-07-25", text="body")
    # the container carries the authenticated pdw CLI; naming a tool that is not
    # there once made a model conclude the database was unavailable and give up
    assert "pdw sql" in prompt
    assert "PDW_POSTGRES_QUERY" not in prompt
    assert "body" in prompt


def test_enrichment_schema_is_closed_and_covers_the_ledger_fields():
    schema = enrichment_schema()
    assert schema["additionalProperties"] is False
    receipt = schema["properties"]["receipt"]["properties"]
    for field in ("total", "currency", "card_last4", "line_items", "summary"):
        assert field in receipt
    match = schema["properties"]["matches"]["items"]["properties"]
    assert match["confidence"]["enum"] == ["high", "medium", "low"]


# --- usage ----------------------------------------------------------------


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


# --- column coverage ------------------------------------------------------
# Every row builder must emit every column its table declares; a missing key
# only surfaces as a KeyError at insert time, i.e. in production.


def test_row_builders_cover_their_table_columns():
    from personal_data_warehouse.schema import (
        RECEIPT_RECORD_COLUMNS,
        RECEIPT_TRANSACTION_LINK_COLUMNS,
        RECEIPT_TRIAGE_COLUMNS,
    )

    candidate = _candidate()
    triage = triage_rows(
        [{"artifact_id": candidate.artifact_id, "verdict": VERDICT_PURCHASE, "reason": "r"}],
        candidates={candidate.artifact_id: candidate},
        provider="codex", model="m", agent_run_id="run", decided_at=NOW,
    )[0]
    assert set(RECEIPT_TRIAGE_COLUMNS) <= set(triage)

    record = record_row(
        _result(),
        source="photo", native_id="ph_1", occurred_at=NOW,
        attempt_count=0, max_attempts=2, provider="codex",
        model="m", agent_run_id="run", elapsed_ms=1, now=NOW,
    )
    assert set(RECEIPT_RECORD_COLUMNS) <= set(record)

    link = link_rows(_result(), record_id="rr_1", agent_run_id="run", now=NOW)[0]
    assert set(RECEIPT_TRANSACTION_LINK_COLUMNS) <= set(link)


def test_sync_version_is_monotonic_with_time():
    from personal_data_warehouse.receipt_enrichment import sync_version_for
    from datetime import timedelta

    assert sync_version_for(NOW + timedelta(seconds=1)) > sync_version_for(NOW)
