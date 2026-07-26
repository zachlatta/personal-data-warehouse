"""Transaction-first receipt research.

The ledger is the worklist. For each posted transaction in the most recent
30 days, one PDW-enabled agent searches source emails, attachments, and photos,
reads the best evidence, and returns the receipt facts and match decision
together. There is no archive-wide receipt scan, no artifact triage, and no
separate extraction or linking pass.

One durable row per transaction records both positive and negative findings.
Only high-confidence matches backed by a real source identifier publish receipt
facts to the mart. A negative finding gets one delayed retry so late-arriving
mail, photo uploads, and attachment extraction can still fill the transaction.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
import hashlib
import json
import time
from typing import Any

PROMPT_VERSION = "receipt-transaction-research-v4"

DECISION_FOUND = "receipt_found"
DECISION_NOT_FOUND = "no_receipt_found"
DECISION_NOT_RECEIPTABLE = "not_receiptable"
DECISION_INSUFFICIENT = "insufficient_evidence"
DECISIONS = (
    DECISION_FOUND,
    DECISION_NOT_FOUND,
    DECISION_NOT_RECEIPTABLE,
    DECISION_INSUFFICIENT,
)

SOURCE_PHOTO = "photo"
SOURCE_GMAIL_MESSAGE = "gmail_message"
SOURCE_GMAIL_ATTACHMENT = "gmail_attachment"
EVIDENCE_SOURCES = (
    SOURCE_PHOTO,
    SOURCE_GMAIL_MESSAGE,
    SOURCE_GMAIL_ATTACHMENT,
)

TRUSTED_CONFIDENCE = "high"
NON_RETAIL_ACCOUNT_KINDS = frozenset(
    {
        "brokerage",
        "crypto",
        "investment",
        "ira",
        "mortgage",
        "private_fund",
        "property",
        "receivable",
        "vehicle",
    }
)
ABSENT_MONEY = "0"
ABSENT_DATE = "1970-01-01"

DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_RETRY_AFTER_DAYS = 7
DEFAULT_MAX_ATTEMPTS = 2
DEFAULT_TRANSACTION_LIMIT = 10


def sync_version_for(moment: datetime) -> int:
    """Warehouse convention: microsecond epoch, monotonic per write."""
    return int(moment.timestamp() * 1_000_000)


def record_id_for(transaction_id: str) -> str:
    digest = hashlib.sha256(f"transaction-receipt\x00{transaction_id}".encode()).hexdigest()
    return f"rr_{digest[:24]}"


def _money(description: str = 'Decimal string like "12.34", or "" if absent.') -> dict[str, Any]:
    return {"type": "string", "description": description}


def transaction_schema() -> dict[str, Any]:
    evidence_item = {
        "type": "object",
        "additionalProperties": False,
        "required": ["source", "native_id", "role", "why"],
        "properties": {
            "source": {"type": "string", "enum": list(EVIDENCE_SOURCES)},
            "native_id": {
                "type": "string",
                "description": "Exact source primary key returned by PDW.",
            },
            "role": {
                "type": "string",
                "description": "For example primary, corroborating, or duplicate view.",
            },
            "why": {
                "type": "string",
                "description": "What this source proves about the receipt or match.",
            },
        },
    }
    receipt_required = [
        "primary_source",
        "primary_native_id",
        "evidence",
        "merchant_name",
        "merchant_location",
        "purchased_at",
        "currency",
        "total",
        "subtotal",
        "tax",
        "tip",
        "amount_charged_to_card",
        "card_last4",
        "order_id",
        "line_items",
        "summary",
        "record_confidence",
        "match_confidence",
        "match_reason",
    ]
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["decision", "sources_searched", "receipt", "reasoning"],
        "properties": {
            "decision": {"type": "string", "enum": list(DECISIONS)},
            "sources_searched": {
                "type": "array",
                "items": {"type": "string", "enum": list(EVIDENCE_SOURCES)},
                "description": (
                    "Source families actually queried. Usually photo and gmail_message; "
                    "gmail_attachment when relevant. Empty is valid only for an obviously "
                    "non-purchase transaction."
                ),
            },
            "receipt": {
                "type": "object",
                "additionalProperties": False,
                "required": receipt_required,
                "properties": {
                    "primary_source": {
                        "type": "string",
                        "enum": ["", *EVIDENCE_SOURCES],
                        "description": "Best evidence source, or empty when no receipt was found.",
                    },
                    "primary_native_id": {
                        "type": "string",
                        "description": "Exact best-evidence primary key, or empty.",
                    },
                    "evidence": {"type": "array", "items": evidence_item},
                    "merchant_name": {"type": "string"},
                    "merchant_location": {
                        "type": "string",
                        "description": 'Printed address or city/state, else "".',
                    },
                    "purchased_at": {
                        "type": "string",
                        "description": (
                            'YYYY-MM-DD printed on source evidence, else "". Never infer '
                            "it from the photo, email, or transaction timestamp."
                        ),
                    },
                    "currency": {
                        "type": "string",
                        "description": 'ISO code supported by source evidence, else "".',
                    },
                    "total": _money("Grand total printed on the receipt, including gratuity."),
                    "subtotal": _money(),
                    "tax": _money(),
                    "tip": _money(),
                    "amount_charged_to_card": _money(
                        "Only when the source states a settled card amount distinct from total."
                    ),
                    "card_last4": {
                        "type": "string",
                        "description": 'Exactly four digits printed by the source, else "".',
                    },
                    "order_id": {"type": "string"},
                    "line_items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["description", "quantity", "amount"],
                            "properties": {
                                "description": {"type": "string"},
                                "quantity": {"type": "string"},
                                "amount": _money(),
                            },
                        },
                    },
                    "summary": {
                        "type": "string",
                        "description": "One factual sentence describing what was bought.",
                    },
                    "record_confidence": {
                        "type": "string",
                        "enum": ["", "high", "medium", "low"],
                    },
                    "match_confidence": {
                        "type": "string",
                        "enum": ["", "high", "medium", "low"],
                    },
                    "match_reason": {
                        "type": "string",
                        "description": "Specific evidence connecting this receipt to the transaction.",
                    },
                },
            },
            "reasoning": {
                "type": "string",
                "description": "Concise research summary, at most three sentences.",
            },
        },
    }


TRANSACTION_RESEARCH_INSTRUCTIONS = """\
You are researching ONE ledger transaction. Find its receipt, if one exists in the
personal data warehouse, and read the receipt in this same operation. The transaction is
the work item: do not scan the archive to extract unrelated receipts, and do not hand work
off to a separate extraction step.

The `pdw` CLI is on PATH and already authenticated to a read-only warehouse surface.
Use it actively. Start with `pdw schema` and `pdw columns <schema.table>` rather than
guessing columns. Run SQL with:

    pdw sql --output json -q 'why you are asking' 'SELECT ...'

Search both photos and Gmail unless the ledger row is plainly not a purchase/refund:

- `search.search_text(query, max_results, sources, since)` searches source text. Its
  source names include `photo` and `gmail`.
- `marts.photos` has `photo_id`, `capture_ts`, `caption`, and
  `thumbnail_storage_file_id`.
- `gmail.messages` has message metadata and bodies. `gmail.attachments` joins
  `enrichment.file_attachment_enrichments` by `content_sha256` for extracted attachment
  text. Evidence IDs must be the primary key for the source you name:
  `gmail_message` uses `gmail.messages.message_id`, while `gmail_attachment` uses
  `gmail.attachments.attachment_id`. A message_id is not an attachment_id. Query and
  return the actual attachment row when an attachment is the receipt; do not reuse its
  parent message ID.
- `receipts.transaction_receipts` shows decisions already made for other transactions.
  Do not reuse one source receipt for two transactions unless the source itself clearly
  proves that it covers both (for example, a split settlement).

Search with several clues, not just one: statement merchant/description tokens, exact
amount strings, likely order wording, and a date window that allows ordering, shipping,
posting, and upload lag. A redacted statement merchant is a reason to search harder, not
to give up. Query focused rows and widen only when evidence warrants it.

For every plausible photo, query its `thumbnail_storage_file_id`, locate it with:

    pdw call get_object --data '{"storage_file_id":"..."}'

Download the returned signed URL and inspect the actual image. Never trust the existing
photo caption or OCR as authoritative: nearby photos can be duplicate views of one
receipt, and captions can confidently mistranscribe merchant names, addresses, totals,
and line items. Reconcile duplicate views into ONE receipt and use the clearest original
evidence. Likewise, read the full relevant email or extracted attachment rather than
deciding from a subject/snippet alone.

Decision meanings:

- `receipt_found`: source evidence identifies the purchase and connects it to this exact
  transaction. Return one consolidated logical receipt, with every supporting source in
  `evidence`. Reserve `match_confidence=high` for a link you would defend.
- `no_receipt_found`: this could be a purchase, but a diligent photo and Gmail search
  found no receipt.
- `not_receiptable`: the ledger row is plainly a transfer, cash movement, fee, interest,
  income, or other non-purchase for which a receipt is not expected.
- `insufficient_evidence`: relevant evidence exists but is degraded or conflicting.
  Tool awkwardness is not evidence; retry the query instead.

Read amounts exactly from source evidence. Do not compute or reconcile receipt fields.
The transaction amount is signed (negative is money leaving an account), its timestamp is
a posting date, and an account mask may differ from the physical or wallet card last four.
Merchant statement text can differ from the receipt name. Never fabricate a line item,
source identifier, date, merchant, or total.

Brokerage, security, and cryptocurrency trades are not retail purchases. Dividends,
interest, reinvestments, asset valuations, and internal investment cash movements are
also `not_receiptable`; an order confirmation, trade fill, or account statement does not
turn them into receipts. A brokerage cash-management transaction with a real retail
merchant may still have a receipt.

When no receipt is found, return every receipt field as an empty string/list. Return ONLY
a JSON object matching the provided schema.
"""


def transaction_prompt(transaction: Mapping[str, Any]) -> str:
    transaction_payload = {
        key: str(transaction.get(key) or "")
        for key in (
            "transaction_id",
            "account_id",
            "account_name",
            "account_kind",
            "institution",
            "mask",
            "posted_at",
            "amount",
            "currency",
            "merchant",
            "description",
            "source",
        )
    }
    return (
        f"{TRANSACTION_RESEARCH_INSTRUCTIONS}\n"
        "--- transaction to research ---\n"
        f"{json.dumps(transaction_payload, indent=2, sort_keys=True)}\n"
    )


TRANSACTION_CANDIDATES_SQL = """
SELECT t.transaction_id,
       t.account_id,
       a.name AS account_name,
       a.kind AS account_kind,
       a.institution,
       a.mask,
       t.posted_at,
       t.amount,
       t.currency,
       t.description,
       t.merchant,
       t.source,
       COALESCE(r.attempt_count, 0) AS attempt_count,
       r.last_attempt_at,
       r.ai_prompt_version AS prior_prompt_version
FROM finance_transactions AS t
JOIN finance_accounts AS a ON a.account_id = t.account_id
LEFT JOIN receipt_transaction_receipts AS r
    ON r.transaction_id = t.transaction_id
WHERE t.posted_at >= %(since)s
  AND t.pending = 0
  AND (
        r.transaction_id IS NULL
     OR (
            r.ai_prompt_version IS DISTINCT FROM %(prompt_version)s
        AND (
               r.decision IS DISTINCT FROM %(found_decision)s
            OR (
                   a.kind = ANY(%(non_retail_account_kinds)s)
               AND BTRIM(COALESCE(t.merchant, '')) = ''
            )
        )
     )
     OR (
            r.settled = 0
        AND r.attempt_count < %(max_attempts)s
        AND r.last_attempt_at <= %(retry_before)s
     )
  )
ORDER BY (r.transaction_id IS NOT NULL
              AND r.ai_prompt_version IS DISTINCT FROM %(prompt_version)s
              AND (
                     r.decision IS DISTINCT FROM %(found_decision)s
                  OR (
                         a.kind = ANY(%(non_retail_account_kinds)s)
                     AND BTRIM(COALESCE(t.merchant, '')) = ''
                  )
              )) DESC,
         (r.transaction_id IS NULL) DESC,
         (t.amount < 0) DESC,
         t.posted_at DESC,
         t.transaction_id
LIMIT %(limit)s
"""

_EVIDENCE_SQL_BY_SOURCE = {
    SOURCE_PHOTO: (
        "SELECT photo_id AS native_id FROM clean_photos "
        "WHERE photo_id = ANY(%s)"
    ),
    SOURCE_GMAIL_MESSAGE: (
        "SELECT message_id AS native_id FROM gmail_messages "
        "WHERE message_id = ANY(%s) AND is_deleted = 0"
    ),
    SOURCE_GMAIL_ATTACHMENT: (
        "SELECT content_sha256 AS native_id FROM gmail_attachments "
        "WHERE content_sha256 = ANY(%s) AND is_deleted = 0"
    ),
}


def usage_from_events(events: Sequence[Any]) -> dict[str, int]:
    """Token usage summed over the run's turn.completed events."""
    totals = {"input_tokens": 0, "cached_input_tokens": 0, "output_tokens": 0}
    for event in events:
        payload = getattr(event, "event_json", event)
        if not isinstance(payload, Mapping):
            continue
        usage = payload.get("usage")
        if isinstance(usage, Mapping) and str(payload.get("type", "")).endswith("turn.completed"):
            for key in totals:
                try:
                    totals[key] += int(usage.get(key, 0) or 0)
                except (TypeError, ValueError):
                    continue
    return totals


def merge_usage(into: dict[str, int], extra: Mapping[str, int]) -> dict[str, int]:
    for key, value in extra.items():
        into[key] = into.get(key, 0) + int(value or 0)
    return into


def _decimal_or_absent(value: Any) -> str:
    text = str(value or "").replace("$", "").replace(",", "").strip()
    if not text:
        return ABSENT_MONEY
    try:
        parsed = Decimal(text)
    except InvalidOperation:
        return ABSENT_MONEY
    if not parsed.is_finite():
        return ABSENT_MONEY
    return text


def _date_or_absent(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ABSENT_DATE
    try:
        datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        return ABSENT_DATE
    return text


def _is_plainly_non_retail_financial_activity(transaction: Mapping[str, Any]) -> bool:
    """Identify account activity that cannot become a retail receipt."""
    account_kind = str(transaction.get("account_kind") or "").strip().lower()
    merchant = str(transaction.get("merchant") or "").strip()
    return account_kind in NON_RETAIL_ACCOUNT_KINDS and not merchant


def _validated_evidence(
    receipt: Mapping[str, Any],
    known_evidence: set[tuple[str, str]],
) -> tuple[list[dict[str, str]], tuple[str, str]]:
    validated: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    raw_evidence = receipt.get("evidence")
    if isinstance(raw_evidence, Sequence) and not isinstance(raw_evidence, (str, bytes)):
        for item in raw_evidence:
            if not isinstance(item, Mapping):
                continue
            pair = (
                str(item.get("source") or "").strip(),
                str(item.get("native_id") or "").strip(),
            )
            if pair not in known_evidence or pair in seen:
                continue
            seen.add(pair)
            validated.append(
                {
                    "source": pair[0],
                    "native_id": pair[1],
                    "role": str(item.get("role") or "")[:100],
                    "why": str(item.get("why") or "")[:1000],
                }
            )
    primary = (
        str(receipt.get("primary_source") or "").strip(),
        str(receipt.get("primary_native_id") or "").strip(),
    )
    if primary in known_evidence and primary not in seen:
        validated.insert(
            0,
            {
                "source": primary[0],
                "native_id": primary[1],
                "role": "primary",
                "why": "Primary source evidence.",
            },
        )
    return validated, primary


def transaction_receipt_row(
    result: Mapping[str, Any],
    *,
    transaction: Mapping[str, Any],
    attempt_count: int,
    max_attempts: int,
    known_evidence: set[tuple[str, str]],
    provider: str,
    model: str,
    agent_run_id: str,
    elapsed_ms: int,
    now: datetime | None = None,
) -> dict[str, Any]:
    stamp = now or datetime.now(UTC)
    raw_receipt = result.get("receipt")
    receipt = raw_receipt if isinstance(raw_receipt, Mapping) else {}
    evidence, primary = _validated_evidence(receipt, known_evidence)
    decision = str(result.get("decision") or DECISION_INSUFFICIENT)
    if decision not in DECISIONS:
        decision = DECISION_INSUFFICIENT
    reasoning = str(result.get("reasoning") or "")[:2000]
    if _is_plainly_non_retail_financial_activity(transaction):
        decision = DECISION_NOT_RECEIPTABLE
        reasoning = (
            "This is non-retail brokerage or asset-account activity without a merchant; "
            "a trade or account confirmation is not a receipt."
        )

    sources_searched = []
    raw_sources = result.get("sources_searched")
    if isinstance(raw_sources, Sequence) and not isinstance(raw_sources, (str, bytes)):
        sources_searched = list(
            dict.fromkeys(
                str(source)
                for source in raw_sources
                if str(source) in EVIDENCE_SOURCES
            )
        )

    trusted_receipt = (
        decision == DECISION_FOUND
        and primary in known_evidence
        and str(receipt.get("record_confidence") or "").lower() == TRUSTED_CONFIDENCE
        and str(receipt.get("match_confidence") or "").lower() == TRUSTED_CONFIDENCE
    )
    if decision == DECISION_FOUND and not trusted_receipt:
        decision = DECISION_INSUFFICIENT
    if decision == DECISION_NOT_FOUND and not {
        SOURCE_PHOTO,
        SOURCE_GMAIL_MESSAGE,
    }.issubset(sources_searched):
        decision = DECISION_INSUFFICIENT

    publish = receipt if trusted_receipt else {}
    attempts = attempt_count + 1
    settled = (
        decision in {DECISION_FOUND, DECISION_NOT_RECEIPTABLE}
        or attempts >= max_attempts
    )
    transaction_id = str(transaction.get("transaction_id") or "")
    return {
        "transaction_id": transaction_id,
        "record_id": record_id_for(transaction_id) if trusted_receipt else "",
        "decision": decision,
        "reasoning": reasoning,
        "sources_searched_json": json.dumps(sources_searched, sort_keys=True),
        "primary_source": primary[0] if trusted_receipt else "",
        "primary_native_id": primary[1] if trusted_receipt else "",
        "evidence_json": json.dumps(evidence if trusted_receipt else [], sort_keys=True),
        "occurred_at": transaction.get("posted_at") or stamp,
        "purchased_at": _date_or_absent(publish.get("purchased_at")),
        "merchant_name": str(publish.get("merchant_name") or ""),
        "merchant_location": str(publish.get("merchant_location") or ""),
        "currency": str(publish.get("currency") or "").upper(),
        "total": _decimal_or_absent(publish.get("total")),
        "subtotal": _decimal_or_absent(publish.get("subtotal")),
        "tax": _decimal_or_absent(publish.get("tax")),
        "tip": _decimal_or_absent(publish.get("tip")),
        "amount_charged": _decimal_or_absent(publish.get("amount_charged_to_card")),
        "card_last4": str(publish.get("card_last4") or ""),
        "order_id": str(publish.get("order_id") or ""),
        "line_items_json": json.dumps(publish.get("line_items") or [], sort_keys=True),
        "summary": str(publish.get("summary") or ""),
        "record_confidence": str(publish.get("record_confidence") or ""),
        "match_confidence": str(publish.get("match_confidence") or ""),
        "match_reason": str(publish.get("match_reason") or "")[:1000],
        "attempt_count": attempts,
        "last_attempt_at": stamp,
        "settled": 1 if settled else 0,
        "raw_result_json": json.dumps(result, sort_keys=True, default=str),
        "ai_provider": provider,
        "ai_model": model,
        "ai_prompt_version": PROMPT_VERSION,
        "ai_elapsed_ms": elapsed_ms,
        "ai_processed_at": stamp,
        "agent_run_id": agent_run_id,
        "created_at": stamp,
        "updated_at": stamp,
        "sync_version": sync_version_for(stamp),
    }


@dataclass(frozen=True)
class EnrichmentSummary:
    candidates: int = 0
    researched: int = 0
    receipts_found: int = 0
    not_found: int = 0
    not_receiptable: int = 0
    insufficient: int = 0
    failed: int = 0
    usage: dict[str, int] = field(default_factory=dict)

    def as_metadata(self) -> dict[str, Any]:
        return {
            "candidates": self.candidates,
            "researched": self.researched,
            "receipts_found": self.receipts_found,
            "not_found": self.not_found,
            "not_receiptable": self.not_receiptable,
            "insufficient": self.insufficient,
            "failed": self.failed,
            **{f"tokens_{key}": value for key, value in sorted(self.usage.items())},
        }


class ReceiptEnrichmentRunner:
    """Research recent ledger transactions with one PDW-enabled agent each."""

    def __init__(
        self,
        *,
        warehouse,
        agent,
        logger,
        provider: str,
        model: str,
        lookback_days: int = DEFAULT_LOOKBACK_DAYS,
        retry_after_days: int = DEFAULT_RETRY_AFTER_DAYS,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        transaction_limit: int = DEFAULT_TRANSACTION_LIMIT,
        now: datetime | None = None,
    ) -> None:
        self._warehouse = warehouse
        self._agent = agent
        self._log = logger
        self._provider = provider
        self._model = model
        # This pipeline is intentionally hard-capped at 30 days. A caller may
        # choose a smaller diagnostic window, but configuration cannot turn it
        # into an archive scan.
        self._lookback_days = min(max(1, lookback_days), DEFAULT_LOOKBACK_DAYS)
        self._retry_after_days = retry_after_days
        self._max_attempts = max_attempts
        self._transaction_limit = max(1, transaction_limit)
        self._now = now

    def _clock(self) -> datetime:
        return self._now or datetime.now(UTC)

    def _candidates(self) -> list[dict[str, Any]]:
        now = self._clock()
        return self._warehouse._query_dicts(
            TRANSACTION_CANDIDATES_SQL,
            {
                "since": now - timedelta(days=self._lookback_days),
                "retry_before": now - timedelta(days=self._retry_after_days),
                "max_attempts": self._max_attempts,
                "prompt_version": PROMPT_VERSION,
                "found_decision": DECISION_FOUND,
                "non_retail_account_kinds": sorted(NON_RETAIL_ACCOUNT_KINDS),
                "limit": self._transaction_limit,
            },
        )

    @staticmethod
    def _claimed_evidence(result: Mapping[str, Any]) -> dict[str, set[str]]:
        claimed = {source: set() for source in EVIDENCE_SOURCES}
        receipt = result.get("receipt")
        if not isinstance(receipt, Mapping):
            return claimed
        primary_source = str(receipt.get("primary_source") or "")
        primary_id = str(receipt.get("primary_native_id") or "")
        if primary_source in claimed and primary_id:
            claimed[primary_source].add(primary_id)
        evidence = receipt.get("evidence")
        if isinstance(evidence, Sequence) and not isinstance(evidence, (str, bytes)):
            for item in evidence:
                if not isinstance(item, Mapping):
                    continue
                source = str(item.get("source") or "")
                native_id = str(item.get("native_id") or "")
                if source in claimed and native_id:
                    claimed[source].add(native_id)
        return claimed

    def _known_evidence(self, result: Mapping[str, Any]) -> set[tuple[str, str]]:
        known: set[tuple[str, str]] = set()
        for source, native_ids in self._claimed_evidence(result).items():
            if not native_ids:
                continue
            rows = self._warehouse._query_dicts(
                _EVIDENCE_SQL_BY_SOURCE[source],
                (sorted(native_ids),),
            )
            known.update(
                (source, str(row["native_id"]))
                for row in rows
                if str(row.get("native_id") or "")
            )
        return known

    def sync(self) -> EnrichmentSummary:
        from personal_data_warehouse.agent_runner import AgentRunRequest

        self._warehouse.ensure_receipt_tables()
        candidates = self._candidates()
        if not candidates:
            return EnrichmentSummary()
        self._log.info(
            f"receipt transaction research: {len(candidates)} recent posted transactions due"
        )

        researched = receipts_found = not_found = not_receiptable = insufficient = failed = 0
        usage: dict[str, int] = {}
        for transaction in candidates:
            transaction_id = str(transaction["transaction_id"])
            request = AgentRunRequest(
                prompt=transaction_prompt(transaction),
                schema=transaction_schema(),
                task_type="receipt_transaction_match",
                subject_id=transaction_id,
                prompt_version=PROMPT_VERSION,
            )
            started = time.monotonic()
            result = self._agent.run_with_pdw(request)
            merge_usage(usage, usage_from_events(result.events))
            if result.exit_code != 0:
                self._log.warning(
                    f"receipt transaction research failed for {transaction_id}: {result.error}"
                )
                failed += 1
                continue

            payload = result.final_output_json or {}
            row = transaction_receipt_row(
                payload,
                transaction=transaction,
                attempt_count=(
                    int(transaction.get("attempt_count") or 0)
                    if str(transaction.get("prior_prompt_version") or "") == PROMPT_VERSION
                    else 0
                ),
                max_attempts=self._max_attempts,
                known_evidence=self._known_evidence(payload),
                provider=self._provider,
                model=self._model,
                agent_run_id=request.run_id,
                elapsed_ms=int((time.monotonic() - started) * 1000),
                now=self._clock(),
            )
            self._warehouse.insert_receipt_transaction_receipts([row])
            researched += 1
            if row["decision"] == DECISION_FOUND:
                receipts_found += 1
            elif row["decision"] == DECISION_NOT_FOUND:
                not_found += 1
            elif row["decision"] == DECISION_NOT_RECEIPTABLE:
                not_receiptable += 1
            else:
                insufficient += 1

        return EnrichmentSummary(
            candidates=len(candidates),
            researched=researched,
            receipts_found=receipts_found,
            not_found=not_found,
            not_receiptable=not_receiptable,
            insufficient=insufficient,
            failed=failed,
            usage=usage,
        )
