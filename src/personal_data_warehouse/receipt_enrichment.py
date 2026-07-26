"""Receipt → transaction enrichment: what was actually purchased.

A statement line reading ``AMAZON *************** -34.97`` says nothing about
what arrived in the box. The documents that do say — order emails, photographed
paper slips, PDF invoices — are already in the warehouse; this module reads them
with an agent and links them to the charge they paid for.

Every judgment belongs to the agent. There are no format parsers, no amount
bands, no posting-lag windows and no scoring weights here, because receipt and
email formats drift constantly (Amazon's own order email stopped naming items
mid-2026) and rules written against today's formats rot silently. Code selects
candidates by time, moves text, and records outcomes; the agent decides what a
document says and which charge it paid for, using read-only SQL to check itself.

Two stages, both agent-driven:

  triage      one cheap batched pass over compact artifact metadata, deciding
              what is even a purchase record. A 30-day window holds ~10,600
              artifacts but only ~135 real receipts, so this is what keeps the
              expensive stage from running 78x more often than it needs to.
  enrichment  one agent call per purchase record: read the document, then find
              the transaction. Extraction and linking share a call because the
              linking half already needs warehouse access, so folding them
              together halves the number of container runs — and the container's
              fixed prompt, not the receipt text, is what actually costs money.

Timing. Receipts almost always arrive *before* the charge: an Amazon order email
precedes its posting by a median of 2 days and a 95th percentile of 17, because
Amazon bills at shipment. Photos need a median 18 hours to upload and be
captioned. So a record is left to settle before the first attempt, and a failure
gets exactly one retry a week later, by which point all but the longest
backorders have posted.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
import hashlib
import json
from typing import Any

TRIAGE_PROMPT_VERSION = "receipt-triage-v1"
ENRICHMENT_PROMPT_VERSION = "receipt-enrich-v1"

VERDICT_PURCHASE = "purchase_record"
VERDICT_NOT_PURCHASE = "not_a_purchase"
VERDICT_UNCERTAIN = "uncertain"

DECISION_MATCHED = "matched"
DECISION_NO_MATCH = "no_matching_transaction"
DECISION_NOT_A_PURCHASE = "not_a_purchase"
DECISION_INSUFFICIENT = "insufficient_evidence"

# Only these reach marts.transaction_receipts as trustworthy links. Measured on
# a 40-receipt sample: every high-confidence link was exact to the cent, and
# every error the agent made was one it had already rated below high.
TRUSTED_LINK_CONFIDENCE = "high"

SOURCE_PHOTO = "photo"
SOURCE_GMAIL_MESSAGE = "gmail_message"
SOURCE_GMAIL_ATTACHMENT = "gmail_attachment"


def sync_version_for(moment: datetime) -> int:
    """Warehouse convention: microsecond epoch, monotonic per write."""
    return int(moment.timestamp() * 1_000_000)


def record_id_for(source: str, native_id: str) -> str:
    digest = hashlib.sha256(f"{source}\x00{native_id}".encode("utf-8")).hexdigest()
    return f"rr_{digest[:24]}"


# ---------------------------------------------------------------------------
# Triage: which artifacts are purchase records at all?
# ---------------------------------------------------------------------------


def triage_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["verdicts"],
        "properties": {
            "verdicts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["artifact_id", "verdict", "reason"],
                    "properties": {
                        "artifact_id": {
                            "type": "string",
                            "description": "Echo the artifact_id verbatim.",
                        },
                        "verdict": {
                            "type": "string",
                            "enum": [VERDICT_PURCHASE, VERDICT_NOT_PURCHASE, VERDICT_UNCERTAIN],
                        },
                        "reason": {"type": "string", "description": "A few words."},
                    },
                },
            }
        },
    }


TRIAGE_INSTRUCTIONS = """\
You are sorting artifacts from a personal data warehouse into "records a purchase" and
"does not", so that a more expensive step only runs on the first kind.

You get a compact description of each artifact — for an email its sender, subject and
snippet; for a photo the document type and one-line summary a vision model already wrote;
for an attachment its filename and opening lines. You do NOT get the full document, and
you do not need it: you are deciding what deserves a closer look, not extracting anything.

Answer `purchase_record` when the artifact looks like evidence of a specific purchase,
refund, or payment Zach made — an order confirmation, a shipping notice with prices, an
emailed or photographed receipt, an invoice he paid, a card slip.

Answer `not_a_purchase` for marketing and promotional mail (including "your cart is
waiting" and sale announcements), newsletters, account and security notices, delivery
notices with no purchase detail, calendar and social notifications, bank statements
summarizing many transactions, tax and legal documents, and anything belonging to someone
else's purchase.

Answer `uncertain` only when the description is too thin to tell. Prefer a real answer:
`uncertain` is treated as "look closer" and costs real money downstream, so use it when
you genuinely cannot tell, not as a hedge.

Being wrong in the `purchase_record` direction wastes a little money. Being wrong in the
`not_a_purchase` direction loses the receipt permanently, because artifacts are triaged
once. When it is close, lean toward `purchase_record`.

Return exactly one verdict per artifact_id, echoing each id verbatim.
"""


def triage_prompt(artifacts: Sequence[Mapping[str, Any]]) -> str:
    lines = [TRIAGE_INSTRUCTIONS, "\n--- artifacts ---"]
    for artifact in artifacts:
        lines.append(
            f"\nartifact_id: {artifact['artifact_id']}\n"
            f"kind: {artifact['kind']}\n"
            f"when: {str(artifact.get('occurred_at') or '')[:10]}\n"
            f"{artifact['descriptor']}"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Enrichment: read the document, then find the charge.
# ---------------------------------------------------------------------------


def _money(description: str = "Decimal string like \"12.34\", or \"\" if absent.") -> dict[str, Any]:
    return {"type": "string", "description": description}


def enrichment_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["is_purchase_record", "receipt", "decision", "matches", "reasoning"],
        "properties": {
            "is_purchase_record": {
                "type": "boolean",
                "description": "False for marketing, statements, tax forms, contracts, and anything that is not a specific purchase by Zach.",
            },
            "receipt": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "merchant_name", "merchant_location", "purchased_at", "currency",
                    "total", "subtotal", "tax", "tip", "amount_charged_to_card",
                    "card_last4", "order_id", "line_items", "summary", "confidence",
                ],
                "properties": {
                    "merchant_name": {"type": "string"},
                    "merchant_location": {"type": "string", "description": "City/state or printed address, else \"\"."},
                    "purchased_at": {
                        "type": "string",
                        "description": "YYYY-MM-DD as printed ON the document, else \"\". Never guess it from the artifact timestamp.",
                    },
                    "currency": {
                        "type": "string",
                        "description": (
                            "ISO code inferred from the document. Canadian slips show TPS/TVQ, "
                            "Gulf ones AED and VAT, European ones VAT or EUR. Getting this wrong "
                            "is worse than leaving it \"\", because a foreign receipt settles in "
                            "USD at a rate you do not know."
                        ),
                    },
                    "total": _money("Grand total printed on the document, INCLUDING any gratuity line."),
                    "subtotal": _money(),
                    "tax": _money(),
                    "tip": _money(),
                    "amount_charged_to_card": _money(
                        "Only when the document states a settled amount different from total; else \"\"."
                    ),
                    "card_last4": {"type": "string", "description": "Exactly 4 digits as printed, or \"\"."},
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
                        "description": "One sentence: what was actually bought, as you would want it to read on a bank statement.",
                    },
                    "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
                },
            },
            "decision": {
                "type": "string",
                "enum": [DECISION_MATCHED, DECISION_NO_MATCH, DECISION_NOT_A_PURCHASE, DECISION_INSUFFICIENT],
            },
            "matches": {
                "type": "array",
                "description": "Empty unless decision is \"matched\". Several entries only when the purchase genuinely settled as several charges.",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["transaction_id", "confidence", "relationship", "why"],
                    "properties": {
                        "transaction_id": {"type": "string"},
                        "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
                        "relationship": {
                            "type": "string",
                            "description": "In your own words, e.g. \"exact\", \"tip added after slip\", \"one of several shipments\".",
                        },
                        "why": {"type": "string", "description": "The specific evidence, one sentence."},
                    },
                },
            },
            "reasoning": {"type": "string", "description": "Three sentences max."},
        },
    }


ENRICHMENT_INSTRUCTIONS = """\
You are enriching Zach's personal finance ledger. Given ONE artifact — a photographed
receipt, an email, or a document attachment — describe what was purchased and find the
bank or card transaction that paid for it.

You have the `pdw` CLI on PATH, already authenticated for this run, reaching a
read-only surface. Query with:

    pdw sql -q 'why you are asking' 'SELECT ...'

Add `--output json` if you prefer JSON to CSV. `pdw columns finance.transactions`
prints exact columns for a relation when you want to confirm one. The tables you need:

    finance.transactions  transaction_id, account_id, posted_at, amount, currency,
                          description, merchant, pending, source
    finance.accounts      account_id, name, kind, side, institution, mask

Reading the document:
- Copy amounts exactly as printed. Never compute, round, or reconcile them.
- `total` is the grand total INCLUDING any gratuity line. A slip reading
  "Purchase 115.00 / Gratuity 11.50 / TOTAL 126.50" has a total of 126.50.
- Detect the currency from the document itself.
- Leave `purchased_at` empty rather than inferring it from when the photo was taken or
  the email arrived.
- Itemize what the document lists. Never invent an item that is not written down; an
  empty list is a fine answer, a fabricated one poisons the warehouse.

Finding the charge — treat these as things to verify, not rules to apply:
- `amount` is signed; money leaving an account is negative.
- `posted_at` is a POSTING date, not the purchase date, and the lag varies by account.
  Some merchants bill at shipment, so an order can post more than a week after it.
- The last-4 printed on a receipt is the physical card or phone-wallet number, and is
  frequently NOT `finance.accounts.mask`. Equal values are strong evidence; unequal
  values are weak evidence, not disproof.
- A merchant's statement string often differs from how it names itself on a receipt, and
  some charges arrive with the descriptor redacted entirely.
- A purchase sometimes settles as several charges, and a restaurant may authorize one
  amount and settle another.

Not every purchase is in this ledger. Zach photographs receipts paid on a work card that
is not synced here, so if the slip names a card you cannot tie to any account and nothing
fits, `no_matching_transaction` is the correct and useful answer. Use
`insufficient_evidence` only when the DOCUMENT is too degraded to read — never because a
tool call was awkward; the query tool is available, so retry it instead.

Reserve `high` confidence for links you would defend. Everything below `high` is held back
from the ledger view, so an honest `medium` costs nothing while a confident wrong link is
expensive.

Choose your own tolerances for THIS purchase: a $4 coffee and a $1,900 flight do not
deserve the same slack. Query as much as you need, then stop.

Return ONLY a JSON object matching the schema.
"""


def enrichment_prompt(*, source: str, title: str, observed_at: str, text: str) -> str:
    return (
        f"{ENRICHMENT_INSTRUCTIONS}\n"
        f"---\n"
        f"Artifact source: {source}\n"
        f"Artifact title: {title or '(none)'}\n"
        f"Captured/received at (context only — the purchase may predate this): {observed_at}\n"
        f"---\n"
        f"{text}\n"
    )


# ---------------------------------------------------------------------------
# Candidate selection. Time windows only: no opinion about what a receipt is.
# ---------------------------------------------------------------------------

# Artifacts inside the lookback window with no triage row yet. Photos are only
# visible once the vision pass has written a caption, which is also what makes
# their text readable, so an uncaptioned photo is simply not a candidate yet.
UNTRIAGED_PHOTOS_SQL = """
SELECT p.photo_id AS native_id,
       p.capture_ts AS occurred_at,
       p.caption AS body
FROM clean_photos AS p
LEFT JOIN receipt_triage AS t
    ON t.source = %(source)s AND t.native_id = p.photo_id
WHERE p.caption <> ''
  AND p.capture_ts >= %(since)s
  AND t.native_id IS NULL
ORDER BY p.capture_ts DESC
LIMIT %(limit)s
"""

UNTRIAGED_GMAIL_SQL = """
SELECT m.message_id AS native_id,
       m.internal_date AS occurred_at,
       m.from_address,
       m.subject,
       m.snippet
FROM gmail_messages AS m
LEFT JOIN receipt_triage AS t
    ON t.source = %(source)s AND t.native_id = m.message_id
WHERE m.is_deleted = 0
  AND m.internal_date >= %(since)s
  AND t.native_id IS NULL
ORDER BY m.internal_date DESC
LIMIT %(limit)s
"""

UNTRIAGED_GMAIL_ATTACHMENTS_SQL = """
SELECT a.content_sha256 AS native_id,
       a.internal_date AS occurred_at,
       a.filename,
       m.subject,
       m.from_address,
       LEFT(e.text, 400) AS head
FROM gmail_attachments AS a
JOIN file_attachment_enrichments AS e
    ON e.content_sha256 = a.content_sha256
LEFT JOIN gmail_messages AS m
    ON m.message_id = a.message_id AND m.account = a.account
LEFT JOIN receipt_triage AS t
    ON t.source = %(source)s AND t.native_id = a.content_sha256
WHERE a.is_deleted = 0
  AND a.internal_date >= %(since)s
  AND e.text IS NOT NULL AND e.text <> ''
  AND t.native_id IS NULL
ORDER BY a.internal_date DESC
LIMIT %(limit)s
"""

# Triaged as a purchase, old enough to have settled, and either never attempted
# or eligible for its single retry. `settled = 1` means the retry budget is
# spent and the record is final.
ENRICHMENT_CANDIDATES_SQL = """
SELECT t.source,
       t.native_id,
       t.occurred_at,
       r.record_id,
       COALESCE(r.attempt_count, 0) AS attempt_count,
       r.last_attempt_at
FROM receipt_triage AS t
LEFT JOIN receipt_records AS r
    ON r.source = t.source AND r.native_id = t.native_id
WHERE t.verdict IN (%(purchase)s, %(uncertain)s)
  AND t.occurred_at >= %(since)s
  AND t.occurred_at <= %(settle_before)s
  AND COALESCE(r.settled, 0) = 0
  AND (
        r.record_id IS NULL
     OR (COALESCE(r.attempt_count, 0) < %(max_attempts)s
         AND r.last_attempt_at <= %(retry_before)s)
  )
ORDER BY t.occurred_at DESC
LIMIT %(limit)s
"""

# Full text for one artifact, fetched only once it is worth enriching.
PHOTO_TEXT_SQL = """
SELECT p.caption AS text, '' AS title, p.capture_ts AS occurred_at
FROM clean_photos AS p WHERE p.photo_id = %(native_id)s
"""

GMAIL_TEXT_SQL = """
SELECT COALESCE(NULLIF(m.body_markdown_clean, ''), NULLIF(m.body_text, ''), m.snippet) AS text,
       m.subject AS title,
       m.internal_date AS occurred_at
FROM gmail_messages AS m WHERE m.message_id = %(native_id)s LIMIT 1
"""

GMAIL_ATTACHMENT_TEXT_SQL = """
SELECT e.text AS text,
       COALESCE(a.filename, '') AS title,
       a.internal_date AS occurred_at
FROM gmail_attachments AS a
JOIN file_attachment_enrichments AS e ON e.content_sha256 = a.content_sha256
WHERE a.content_sha256 = %(native_id)s
ORDER BY a.internal_date DESC
LIMIT 1
"""


@dataclass(frozen=True)
class TriageCandidate:
    source: str
    native_id: str
    occurred_at: datetime
    descriptor: str
    kind: str

    @property
    def artifact_id(self) -> str:
        return f"{self.source}:{self.native_id}"


@dataclass(frozen=True)
class EnrichmentSummary:
    triaged: int = 0
    purchase_records: int = 0
    enriched: int = 0
    matched: int = 0
    trusted_links: int = 0
    settled: int = 0
    failed: int = 0
    triage_batches: int = 0
    usage: dict[str, int] = field(default_factory=dict)

    def as_metadata(self) -> dict[str, Any]:
        return {
            "triaged": self.triaged,
            "purchase_records": self.purchase_records,
            "enriched": self.enriched,
            "matched": self.matched,
            "trusted_links": self.trusted_links,
            "settled": self.settled,
            "failed": self.failed,
            "triage_batches": self.triage_batches,
            **{f"tokens_{key}": value for key, value in sorted(self.usage.items())},
        }


def photo_descriptor(row: Mapping[str, Any]) -> str:
    """The caption's own header lines — enough to judge, far short of the whole OCR."""
    caption = str(row.get("body") or "")
    keep: list[str] = []
    for line in caption.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        lowered = stripped.lower()
        if lowered.startswith(("document type:", "summary:")):
            keep.append(stripped)
        if len(keep) >= 2:
            break
    if not keep:
        keep.append(caption[:200])
    return "\n".join(keep)[:400]


def gmail_descriptor(row: Mapping[str, Any]) -> str:
    return (
        f"from: {row.get('from_address') or ''}\n"
        f"subject: {row.get('subject') or ''}\n"
        f"snippet: {str(row.get('snippet') or '')[:240]}"
    )


def attachment_descriptor(row: Mapping[str, Any]) -> str:
    return (
        f"filename: {row.get('filename') or ''}\n"
        f"email subject: {row.get('subject') or ''}\n"
        f"from: {row.get('from_address') or ''}\n"
        f"opening text: {str(row.get('head') or '')[:240]}"
    )


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


def triage_rows(
    verdicts: Sequence[Mapping[str, Any]],
    *,
    candidates: Mapping[str, TriageCandidate],
    provider: str,
    model: str,
    agent_run_id: str,
    decided_at: datetime | None = None,
) -> list[dict[str, Any]]:
    """Turn one triage response into rows, ignoring ids we did not ask about.

    Batched responses occasionally echo an id more than once; the first verdict
    wins so a repeat cannot flip an artifact's fate.
    """
    decided = decided_at or datetime.now(UTC)
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for verdict in verdicts:
        artifact_id = str(verdict.get("artifact_id") or "")
        candidate = candidates.get(artifact_id)
        if candidate is None or artifact_id in seen:
            continue
        seen.add(artifact_id)
        rows.append(
            {
                "source": candidate.source,
                "native_id": candidate.native_id,
                "occurred_at": candidate.occurred_at,
                "verdict": str(verdict.get("verdict") or VERDICT_UNCERTAIN),
                "reason": str(verdict.get("reason") or "")[:500],
                "ai_provider": provider,
                "ai_model": model,
                "ai_prompt_version": TRIAGE_PROMPT_VERSION,
                "agent_run_id": agent_run_id,
                "decided_at": decided,
                "sync_version": sync_version_for(decided),
            }
        )
    return rows


# The warehouse stores no NULLs: every column is NOT NULL with a sentinel
# default. So "the document did not print a subtotal" is stored as 0 and "no
# date printed" as the epoch, and the marts views map those sentinels back to
# NULL so the read surface can still tell absent from zero.
ABSENT_MONEY = "0"
ABSENT_DATE = "1970-01-01"


def _decimal_or_absent(value: Any) -> str:
    text = str(value or "").replace("$", "").replace(",", "").strip()
    if not text:
        return ABSENT_MONEY
    try:
        float(text)
    except ValueError:
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


def record_row(
    result: Mapping[str, Any],
    *,
    source: str,
    native_id: str,
    occurred_at: datetime,
    attempt_count: int,
    max_attempts: int,
    provider: str,
    model: str,
    agent_run_id: str,
    elapsed_ms: int,
    now: datetime | None = None,
) -> dict[str, Any]:
    stamp = now or datetime.now(UTC)
    receipt = result.get("receipt") if isinstance(result.get("receipt"), Mapping) else {}
    decision = str(result.get("decision") or DECISION_INSUFFICIENT)
    is_purchase = bool(result.get("is_purchase_record"))
    attempts = attempt_count + 1
    # A record stops being retried when it matched, when the agent said it is
    # not a purchase at all, or when its one retry is spent. Everything else
    # gets exactly one more look a week later.
    settled = (
        decision == DECISION_MATCHED
        or decision == DECISION_NOT_A_PURCHASE
        or not is_purchase
        or attempts >= max_attempts
    )
    return {
        "record_id": record_id_for(source, native_id),
        "source": source,
        "native_id": native_id,
        "occurred_at": occurred_at,
        "purchased_at": _date_or_absent(receipt.get("purchased_at")),
        "merchant_name": str(receipt.get("merchant_name") or ""),
        "merchant_location": str(receipt.get("merchant_location") or ""),
        "currency": str(receipt.get("currency") or "").upper(),
        "total": _decimal_or_absent(receipt.get("total")),
        "subtotal": _decimal_or_absent(receipt.get("subtotal")),
        "tax": _decimal_or_absent(receipt.get("tax")),
        "tip": _decimal_or_absent(receipt.get("tip")),
        "amount_charged": _decimal_or_absent(receipt.get("amount_charged_to_card")),
        "card_last4": str(receipt.get("card_last4") or ""),
        "order_id": str(receipt.get("order_id") or ""),
        "line_items_json": json.dumps(receipt.get("line_items") or [], sort_keys=True, default=str),
        "summary": str(receipt.get("summary") or ""),
        "record_confidence": str(receipt.get("confidence") or ""),
        "is_purchase_record": 1 if is_purchase else 0,
        "decision": decision,
        "reasoning": str(result.get("reasoning") or "")[:2000],
        "attempt_count": attempts,
        "last_attempt_at": stamp,
        "settled": 1 if settled else 0,
        "raw_result_json": json.dumps(result, sort_keys=True, default=str),
        "ai_provider": provider,
        "ai_model": model,
        "ai_prompt_version": ENRICHMENT_PROMPT_VERSION,
        "ai_elapsed_ms": elapsed_ms,
        "ai_processed_at": stamp,
        "agent_run_id": agent_run_id,
        "created_at": stamp,
        "updated_at": stamp,
        "sync_version": sync_version_for(stamp),
    }


def link_rows(
    result: Mapping[str, Any],
    *,
    record_id: str,
    agent_run_id: str,
    known_transaction_ids: set[str] | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Link rows for one enrichment result.

    Only `high` confidence is persisted: on the benchmark sample every
    high-confidence link was exact to the cent, and every mistake the agent made
    was one it had already flagged as `medium`. Lower-confidence guesses stay
    visible in `records.raw_result_json` without entering the ledger view.

    `known_transaction_ids`, when supplied, drops ids that are not real rows —
    an agent that hallucinates an id should not create a dangling link.
    """
    stamp = now or datetime.now(UTC)
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    if str(result.get("decision") or "") != DECISION_MATCHED:
        return rows
    for match in result.get("matches") or []:
        if not isinstance(match, Mapping):
            continue
        transaction_id = str(match.get("transaction_id") or "").strip()
        confidence = str(match.get("confidence") or "").lower()
        if not transaction_id or transaction_id in seen:
            continue
        if confidence != TRUSTED_LINK_CONFIDENCE:
            continue
        if known_transaction_ids is not None and transaction_id not in known_transaction_ids:
            continue
        seen.add(transaction_id)
        rows.append(
            {
                "record_id": record_id,
                "transaction_id": transaction_id,
                "confidence": confidence,
                "relationship": str(match.get("relationship") or "")[:200],
                "why": str(match.get("why") or "")[:1000],
                "ai_prompt_version": ENRICHMENT_PROMPT_VERSION,
                "agent_run_id": agent_run_id,
                "created_at": stamp,
                "sync_version": sync_version_for(stamp),
            }
        )
    return rows


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_SETTLE_DAYS = 2
DEFAULT_RETRY_AFTER_DAYS = 7
DEFAULT_MAX_ATTEMPTS = 2
DEFAULT_TRIAGE_BATCH_SIZE = 100
DEFAULT_TRIAGE_ARTIFACT_LIMIT = 1_000
DEFAULT_ENRICHMENT_LIMIT = 40
DEFAULT_ENRICHMENT_TEXT_CHARS = 14_000

_TEXT_SQL_BY_SOURCE = {
    SOURCE_PHOTO: PHOTO_TEXT_SQL,
    SOURCE_GMAIL_MESSAGE: GMAIL_TEXT_SQL,
    SOURCE_GMAIL_ATTACHMENT: GMAIL_ATTACHMENT_TEXT_SQL,
}


class ReceiptEnrichmentRunner:
    """Triage artifacts, then enrich the ones that are purchases.

    Both stages are bounded by a lookback window so a first run in production
    cannot walk the entire archive, and every artifact is triaged exactly once
    so cost is proportional to what is new.
    """

    def __init__(
        self,
        *,
        warehouse,
        agent,
        logger,
        provider: str,
        model: str,
        lookback_days: int = DEFAULT_LOOKBACK_DAYS,
        settle_days: int = DEFAULT_SETTLE_DAYS,
        retry_after_days: int = DEFAULT_RETRY_AFTER_DAYS,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        triage_batch_size: int = DEFAULT_TRIAGE_BATCH_SIZE,
        triage_artifact_limit: int = DEFAULT_TRIAGE_ARTIFACT_LIMIT,
        enrichment_limit: int = DEFAULT_ENRICHMENT_LIMIT,
        text_chars: int = DEFAULT_ENRICHMENT_TEXT_CHARS,
        now: datetime | None = None,
    ) -> None:
        self._warehouse = warehouse
        self._agent = agent
        self._log = logger
        self._provider = provider
        self._model = model
        self._lookback_days = lookback_days
        self._settle_days = settle_days
        self._retry_after_days = retry_after_days
        self._max_attempts = max_attempts
        self._triage_batch_size = max(1, triage_batch_size)
        self._triage_artifact_limit = triage_artifact_limit
        self._enrichment_limit = enrichment_limit
        self._text_chars = text_chars
        self._now = now

    # -- helpers ----------------------------------------------------------

    def _clock(self) -> datetime:
        return self._now or datetime.now(UTC)

    def _since(self) -> datetime:
        from datetime import timedelta

        return self._clock() - timedelta(days=self._lookback_days)

    def sync(self) -> EnrichmentSummary:
        from personal_data_warehouse.agent_runner import AgentRunRequest  # local: keeps import graph light

        self._warehouse.ensure_receipt_tables()
        usage: dict[str, int] = {}
        triaged, purchases, batches = self._triage(AgentRunRequest, usage)
        enriched, matched, trusted, settled, failed = self._enrich(AgentRunRequest, usage)
        return EnrichmentSummary(
            triaged=triaged,
            purchase_records=purchases,
            enriched=enriched,
            matched=matched,
            trusted_links=trusted,
            settled=settled,
            failed=failed,
            triage_batches=batches,
            usage=usage,
        )

    # -- stage 1 ----------------------------------------------------------

    def _triage_candidates(self) -> list[TriageCandidate]:
        since = self._since()
        limit = self._triage_artifact_limit
        out: list[TriageCandidate] = []
        for source, sql, descriptor, kind in (
            (SOURCE_PHOTO, UNTRIAGED_PHOTOS_SQL, photo_descriptor, "photo"),
            (SOURCE_GMAIL_MESSAGE, UNTRIAGED_GMAIL_SQL, gmail_descriptor, "email"),
            (SOURCE_GMAIL_ATTACHMENT, UNTRIAGED_GMAIL_ATTACHMENTS_SQL, attachment_descriptor, "attachment"),
        ):
            rows = self._warehouse._query_dicts(
                sql, {"source": source, "since": since, "limit": limit}
            )
            for row in rows:
                out.append(
                    TriageCandidate(
                        source=source,
                        native_id=str(row["native_id"]),
                        occurred_at=row["occurred_at"],
                        descriptor=descriptor(row),
                        kind=kind,
                    )
                )
        return out

    def _triage(self, request_cls, usage: dict[str, int]) -> tuple[int, int, int]:
        candidates = self._triage_candidates()
        if not candidates:
            return 0, 0, 0
        self._log.info(f"receipt triage: {len(candidates)} untriaged artifacts in the window")

        triaged = purchases = batches = 0
        for start in range(0, len(candidates), self._triage_batch_size):
            batch = candidates[start : start + self._triage_batch_size]
            by_id = {candidate.artifact_id: candidate for candidate in batch}
            prompt = triage_prompt(
                [
                    {
                        "artifact_id": candidate.artifact_id,
                        "kind": candidate.kind,
                        "occurred_at": candidate.occurred_at,
                        "descriptor": candidate.descriptor,
                    }
                    for candidate in batch
                ]
            )
            request = request_cls(
                prompt=prompt,
                schema=triage_schema(),
                task_type="receipt_triage",
                subject_id=f"batch-{start // self._triage_batch_size}",
                prompt_version=TRIAGE_PROMPT_VERSION,
            )
            result = self._agent.run(request)
            batches += 1
            merge_usage(usage, usage_from_events(result.events))
            if result.exit_code != 0:
                self._log.warning(f"receipt triage batch failed: {result.error}")
                continue
            rows = triage_rows(
                (result.final_output_json or {}).get("verdicts") or [],
                candidates=by_id,
                provider=self._provider,
                model=self._model,
                agent_run_id=request.run_id,
                decided_at=self._clock(),
            )
            # An artifact the model silently dropped stays untriaged and is
            # simply picked up next run; never guess a verdict for it.
            self._warehouse.insert_receipt_triage(rows)
            triaged += len(rows)
            purchases += sum(1 for row in rows if row["verdict"] == VERDICT_PURCHASE)
        return triaged, purchases, batches

    # -- stage 2 ----------------------------------------------------------

    def _enrichment_candidates(self) -> list[dict[str, Any]]:
        from datetime import timedelta

        now = self._clock()
        return self._warehouse._query_dicts(
            ENRICHMENT_CANDIDATES_SQL,
            {
                "purchase": VERDICT_PURCHASE,
                "uncertain": VERDICT_UNCERTAIN,
                "since": self._since(),
                # settle before the first look: the charge may not have posted
                "settle_before": now - timedelta(days=self._settle_days),
                "retry_before": now - timedelta(days=self._retry_after_days),
                "max_attempts": self._max_attempts,
                "limit": self._enrichment_limit,
            },
        )

    def _artifact_text(self, source: str, native_id: str) -> dict[str, Any] | None:
        sql = _TEXT_SQL_BY_SOURCE.get(source)
        if sql is None:
            return None
        rows = self._warehouse._query_dicts(sql, {"native_id": native_id})
        return rows[0] if rows else None

    def _enrich(self, request_cls, usage: dict[str, int]) -> tuple[int, int, int, int, int]:
        candidates = self._enrichment_candidates()
        if not candidates:
            return 0, 0, 0, 0, 0
        self._log.info(f"receipt enrichment: {len(candidates)} records due")

        enriched = matched = trusted = settled = failed = 0
        for candidate in candidates:
            source = str(candidate["source"])
            native_id = str(candidate["native_id"])
            artifact = self._artifact_text(source, native_id)
            if artifact is None or not str(artifact.get("text") or "").strip():
                self._log.warning(f"receipt enrichment: no text for {source}:{native_id}")
                failed += 1
                continue

            occurred_at = candidate["occurred_at"] or artifact.get("occurred_at")
            prompt = enrichment_prompt(
                source=source,
                title=str(artifact.get("title") or ""),
                observed_at=str(occurred_at),
                text=str(artifact["text"])[: self._text_chars],
            )
            request = request_cls(
                prompt=prompt,
                schema=enrichment_schema(),
                task_type="receipt_enrichment",
                subject_id=f"{source}:{native_id}",
                prompt_version=ENRICHMENT_PROMPT_VERSION,
            )
            started = self._clock()
            result = self._agent.run_with_pdw(request)
            merge_usage(usage, usage_from_events(result.events))
            if result.exit_code != 0:
                # Leave the record untouched so the next run retries it; a
                # container failure is not evidence about the receipt.
                self._log.warning(f"receipt enrichment failed for {source}:{native_id}: {result.error}")
                failed += 1
                continue

            payload = result.final_output_json or {}
            elapsed_ms = int((self._clock() - started).total_seconds() * 1000)
            row = record_row(
                payload,
                source=source,
                native_id=native_id,
                occurred_at=occurred_at,
                attempt_count=int(candidate.get("attempt_count") or 0),
                max_attempts=self._max_attempts,
                provider=self._provider,
                model=self._model,
                agent_run_id=request.run_id,
                elapsed_ms=elapsed_ms,
                now=self._clock(),
            )
            self._warehouse.insert_receipt_records([row])
            enriched += 1
            settled += row["settled"]
            if row["decision"] == DECISION_MATCHED:
                matched += 1

            links = link_rows(
                payload,
                record_id=row["record_id"],
                agent_run_id=request.run_id,
                known_transaction_ids=self._known_transaction_ids(payload),
                now=self._clock(),
            )
            if links:
                self._warehouse.insert_receipt_transaction_links(links)
                trusted += len(links)
        return enriched, matched, trusted, settled, failed

    def _known_transaction_ids(self, payload: Mapping[str, Any]) -> set[str]:
        """Which of the claimed transaction ids actually exist.

        An agent that invents an id would otherwise create a link pointing at
        nothing, which reads as a real match in the mart view.
        """
        claimed = [
            str(match.get("transaction_id") or "").strip()
            for match in (payload.get("matches") or [])
            if isinstance(match, Mapping) and str(match.get("transaction_id") or "").strip()
        ]
        if not claimed:
            return set()
        rows = self._warehouse._query_dicts(
            "SELECT transaction_id FROM finance_transactions WHERE transaction_id = ANY(%s)",
            (claimed,),
        )
        return {str(row["transaction_id"]) for row in rows}
