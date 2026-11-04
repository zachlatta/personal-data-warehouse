"""Agent review of Capital One purchase alerts that exact matching left open.

The finance ledger reconciles an alert in code only on an unambiguous
account/currency/merchant/exact-amount edge. Capital One reaches Plaid
posted-only for Zach's cards (no pending row, no ``pending_transaction_id``),
so the bank never says which posted charge an alert became. Measured
2026-11-03: of 30 open alerts, most were a tip added after authorization
(4.40 -> 5.40), a merchant the alert and the bank spell differently
(a name truncated one letter short, "Capital One Travel" / "COT*FLT"),
or two same-price alerts facing two same-price posted rows.

Those are judgments, so an agent with PDW access makes them, one alert at a
time. Code only pages the alert with the posted rows on its card around its
date, validates that a chosen row was one of them, and records the decision
in ``derived_finance.alert_reviews``. The next ledger run applies trusted
decisions one-to-one (``capital_one_alerts.reconcile_alerts``).
"""

from __future__ import annotations

import json
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from personal_data_warehouse.capital_one_alerts import (
    REVIEW_NOT_POSTED,
    REVIEW_SETTLED_BY,
    SOURCE,
    UNRESOLVED_METHODS,
)

PROMPT_VERSION = "capital-one-alert-review-v2"
#: ops.ai_processing_agent_runs task_type; the pipeline heartbeat scopes to it.
ALERT_REVIEW_TASK_TYPE = "capital_one_alert_review"

REVIEW_UNDETERMINED = "undetermined"
DECISIONS = (REVIEW_SETTLED_BY, REVIEW_NOT_POSTED, REVIEW_UNDETERMINED)
CONFIDENCES = ("high", "medium", "low")

#: Give the bank time to post before asking: Capital One posts in days, and a
#: review before the charge exists can only say "not yet".
DEFAULT_MIN_ALERT_AGE_DAYS = 2
DEFAULT_RETRY_AFTER_DAYS = 1
DEFAULT_MAX_ATTEMPTS = 7
DEFAULT_ALERT_LIMIT = 20
#: The page of posted rows shown with an alert: its card, from a few days
#: before the purchase date to three weeks after. Paging, not matching.
CANDIDATE_DAYS_BEFORE = 3
CANDIDATE_DAYS_AFTER = 21
CANDIDATE_LIMIT = 80
#: Other open alerts on the card shown beside it, so same-price repeats are
#: decided together rather than each claiming the same posted row.
OTHER_ALERT_DAYS = 7

MATCHED_METHODS = (
    "alert_amount_merchant_date",
    "alert_pending_successor",
    "alert_agent_review",
)

INSTRUCTIONS = """\
You reconcile one Capital One purchase-alert email against Zach's posted card
transactions in his personal data warehouse (PDW). Decide which posted
transaction, if any, is the same purchase as the alert.

How the evidence arises:
- Capital One emails the alert when a purchase is AUTHORIZED, with the amount
  and merchant text at that moment and the purchase date.
- The bank later POSTS the final charge (usually 1-5 days later) and Plaid
  delivers it. The posted row can differ from the alert:
  - a tip added after authorization at a restaurant, bar or cafe (the posted
    amount is usually higher; e.g. 4.40 authorized, 5.40 posted);
  - a fuel pump or hotel pre-authorization replaced by the final amount;
  - merchant text spelled or truncated differently (processor prefixes like
    SP/SQ/TST*, a cut-off name, a brand vs a descriptor such as
    "Capital One Travel" vs "COT*FLT", "Google" vs "Google Workspace");
  - a posted date a few days after the alert date.
- Some authorizations never post as their own charge: $0.01 or $1 card
  checks, temporary holds ("TEMP AUTH") whose final charge posts separately
  (sometimes as one combined amount), and cancelled purchases.

Rules:
- Each posted transaction is at most ONE purchase. A row listed with
  `claimed_by_alert` already belongs to another alert; do not choose it
  unless you are sure that claim is wrong. Other unresolved alerts on the
  card are listed: two alerts for the same price on the same day are usually
  two purchases, not a duplicate, and each needs its own posted row.
- Interchangeable purchases: when N open alerts (this one included) and N
  unclaimed posted rows are the same purchases in any pairing (same merchant,
  same amounts allowing for the same tip, close dates), any pairing books the
  ledger correctly. Pair them in order: alerts by (posted_at,
  gmail_message_id), posted rows by (posted_at, transaction_id), and settle
  this alert by its position. That is a `high` confidence decision; refusing
  to choose leaves real purchases unreconciled.
- Use PDW to check when it helps: read the alert email (Gmail message id
  below), receipts or order emails from the merchant, other alerts.
- `settled_by`: the posted transaction is this purchase. Give its exact
  transaction_id from the list. Use `high` confidence only when merchant,
  date and amount (allowing for a tip or final total) leave no reasonable
  doubt and no other alert competes for the row.
- `not_posted`: the evidence shows this authorization did not post as its
  own charge (a card check, a hold whose final charge is another row or
  another alert, a cancellation). Give transaction_id "". Use `high` when you
  can say what happened to it: e.g. the merchant's posted charges in the
  window are all accounted for by other alerts and a week or more has passed.
- `undetermined`: anything else, including "the charge has probably not
  posted yet". It is retried later.

Return ONLY a JSON object matching the schema.
"""


def review_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["decision", "transaction_id", "confidence", "reasoning"],
        "properties": {
            "decision": {"type": "string", "enum": list(DECISIONS)},
            "transaction_id": {
                "type": "string",
                "description": "Exact transaction_id from the posted list for settled_by, else empty.",
            },
            "confidence": {"type": "string", "enum": list(CONFIDENCES)},
            "reasoning": {
                "type": "string",
                "description": "What matched or ruled out each plausible row (amount, tip, merchant, date).",
            },
        },
    }


def _jsonable(row: Mapping[str, Any], keys: tuple[str, ...]) -> dict[str, str]:
    return {key: str(row.get(key) if row.get(key) is not None else "") for key in keys}


def review_prompt(
    alert: Mapping[str, Any],
    posted: list[Mapping[str, Any]],
    other_alerts: list[Mapping[str, Any]],
    *,
    now: datetime,
) -> str:
    account, _, message_id = str(alert["source_row_key"]).partition("|")
    payload = {
        "today": now.date().isoformat(),
        "alert": {
            **_jsonable(
                alert,
                ("account_name", "institution", "mask", "posted_at", "amount", "currency", "merchant"),
            ),
            "gmail_account": account,
            "gmail_message_id": message_id,
            "current_status": str(alert.get("match_method") or ""),
            "previous_review": _jsonable(
                alert, ("prior_decision", "prior_transaction_id", "prior_confidence", "prior_reasoning")
            )
            if alert.get("prior_decision")
            else None,
        },
        "posted_transactions_on_this_card": [
            _jsonable(
                row,
                ("transaction_id", "posted_at", "amount", "merchant", "description", "source", "claimed_by_alert"),
            )
            for row in posted
        ],
        "other_unresolved_alerts_on_this_card": [
            {
                **_jsonable(row, ("posted_at", "amount", "merchant", "match_method")),
                "gmail_message_id": str(row.get("source_row_key") or "").partition("|")[2],
            }
            for row in other_alerts
        ],
    }
    return f"{INSTRUCTIONS}\n--- alert and candidates ---\n{json.dumps(payload, indent=2)}\n"


ALERTS_DUE_SQL = """
SELECT l.source_row_key,
       l.match_method,
       t.transaction_id AS alert_transaction_id,
       t.account_id,
       a.name AS account_name,
       a.institution,
       a.mask,
       t.posted_at,
       t.amount,
       t.currency,
       t.merchant,
       COALESCE(r.attempt_count, 0) AS attempt_count,
       r.ai_prompt_version AS prior_prompt_version,
       r.decision AS prior_decision,
       r.transaction_id AS prior_transaction_id,
       r.decision_confidence AS prior_confidence,
       r.reasoning AS prior_reasoning
FROM @finance_transaction_links AS l
JOIN @finance_transactions AS t
    ON t.transaction_id = l.transaction_id AND t.source = %(source)s
JOIN @finance_accounts AS a ON a.account_id = t.account_id
LEFT JOIN @finance_alert_reviews AS r ON r.source_row_key = l.source_row_key
WHERE l.source = %(source)s
  AND l.match_method = ANY(%(unresolved)s)
  AND t.posted_at <= %(settle_before)s
  AND (
        r.source_row_key IS NULL
     OR r.ai_prompt_version IS DISTINCT FROM %(prompt_version)s
     OR (r.attempt_count < %(max_attempts)s AND r.last_attempt_at <= %(retry_before)s)
  )
ORDER BY (r.source_row_key IS NULL) DESC, t.posted_at, l.source_row_key
LIMIT %(limit)s
"""

POSTED_CANDIDATES_SQL = """
SELECT t.transaction_id, t.posted_at, t.amount, t.merchant, t.description, t.source,
       COALESCE((
           SELECT string_agg(l.source_row_key, ', ' ORDER BY l.source_row_key)
           FROM @finance_transaction_links AS l
           WHERE l.source = %(source)s AND l.transaction_id = t.transaction_id
             AND l.match_method = ANY(%(matched)s)
       ), '') AS claimed_by_alert
FROM @finance_transactions AS t
WHERE t.account_id = %(account_id)s
  AND t.source <> %(source)s
  AND t.pending = 0
  AND t.amount < 0
  AND t.posted_at >= %(since)s
  AND t.posted_at < %(until)s
ORDER BY t.posted_at, t.transaction_id
LIMIT %(limit)s
"""

OTHER_ALERTS_SQL = """
SELECT l.source_row_key, t.posted_at, t.amount, t.merchant, l.match_method
FROM @finance_transaction_links AS l
JOIN @finance_transactions AS t
    ON t.transaction_id = l.transaction_id AND t.source = %(source)s
WHERE l.source = %(source)s
  AND l.match_method = ANY(%(unresolved)s)
  AND l.source_row_key <> %(source_row_key)s
  AND t.account_id = %(account_id)s
  AND t.posted_at >= %(since)s
  AND t.posted_at < %(until)s
ORDER BY t.posted_at, l.source_row_key
LIMIT %(limit)s
"""


def review_row(
    payload: Mapping[str, Any],
    *,
    alert: Mapping[str, Any],
    candidate_ids: set[str],
    provider: str,
    model: str,
    agent_run_id: str,
    elapsed_ms: int,
    now: datetime,
) -> dict[str, Any]:
    decision = str(payload.get("decision") or "")
    transaction_id = str(payload.get("transaction_id") or "").strip()
    confidence = str(payload.get("confidence") or "")
    reasoning = str(payload.get("reasoning") or "")
    if decision not in DECISIONS or confidence not in CONFIDENCES:
        decision, confidence = REVIEW_UNDETERMINED, "low"
        reasoning = f"[invalid agent output] {reasoning}"
    if decision == REVIEW_SETTLED_BY and transaction_id not in candidate_ids:
        reasoning = f"[rejected: {transaction_id!r} was not a listed posted transaction] {reasoning}"
        decision, confidence = REVIEW_UNDETERMINED, "low"
    if decision != REVIEW_SETTLED_BY:
        transaction_id = ""
    prior_attempts = (
        int(alert.get("attempt_count") or 0)
        if str(alert.get("prior_prompt_version") or "") == PROMPT_VERSION
        else 0
    )
    return dict(
        source_row_key=str(alert["source_row_key"]),
        alert_transaction_id=str(alert["alert_transaction_id"]),
        account_id=str(alert["account_id"]),
        decision=decision,
        transaction_id=transaction_id,
        decision_confidence=confidence,
        reasoning=reasoning,
        attempt_count=prior_attempts + 1,
        last_attempt_at=now,
        raw_result_json=json.dumps(dict(payload), sort_keys=True),
        ai_provider=provider,
        ai_model=model,
        ai_prompt_version=PROMPT_VERSION,
        ai_elapsed_ms=elapsed_ms,
        agent_run_id=agent_run_id,
        created_at=now,
        updated_at=now,
        sync_version=int(now.timestamp() * 1_000_000),
    )


@dataclass
class ReviewSummary:
    candidates: int = 0
    reviewed: int = 0
    settled: int = 0
    not_posted: int = 0
    undetermined: int = 0
    failed: int = 0
    usage: dict[str, int] = field(default_factory=dict)

    def as_metadata(self) -> dict[str, int]:
        return {
            "candidates": self.candidates,
            "reviewed": self.reviewed,
            "settled": self.settled,
            "not_posted": self.not_posted,
            "undetermined": self.undetermined,
            "failed": self.failed,
        }


class AlertReviewRunner:
    """One PDW-enabled agent per unresolved alert; decisions only, no ledger writes."""

    def __init__(
        self,
        *,
        warehouse,
        agent,
        logger,
        provider: str,
        model: str,
        min_alert_age_days: int = DEFAULT_MIN_ALERT_AGE_DAYS,
        retry_after_days: int = DEFAULT_RETRY_AFTER_DAYS,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        alert_limit: int = DEFAULT_ALERT_LIMIT,
        now: datetime | None = None,
    ) -> None:
        self._warehouse = warehouse
        self._agent = agent
        self._log = logger
        self._provider = provider
        self._model = model
        self._min_alert_age_days = min_alert_age_days
        self._retry_after_days = retry_after_days
        self._max_attempts = max_attempts
        self._alert_limit = max(1, alert_limit)
        self._now = now

    def _clock(self) -> datetime:
        return self._now or datetime.now(UTC)

    def _due(self) -> list[dict[str, Any]]:
        now = self._clock()
        return self._warehouse._query_dicts(
            ALERTS_DUE_SQL,
            {
                "source": SOURCE,
                "unresolved": list(UNRESOLVED_METHODS),
                "settle_before": now - timedelta(days=self._min_alert_age_days),
                "prompt_version": PROMPT_VERSION,
                "max_attempts": self._max_attempts,
                "retry_before": now - timedelta(days=self._retry_after_days),
                "limit": self._alert_limit,
            },
        )

    def _window(self, alert: Mapping[str, Any]) -> dict[str, Any]:
        day = alert["posted_at"]
        return {
            "source": SOURCE,
            "account_id": alert["account_id"],
            "since": day - timedelta(days=CANDIDATE_DAYS_BEFORE),
            "until": day + timedelta(days=CANDIDATE_DAYS_AFTER + 1),
            "limit": CANDIDATE_LIMIT,
        }

    def _record_agent_result(self, result) -> None:
        from personal_data_warehouse.agent_runner import agent_run_event_rows, agent_run_row

        self._warehouse.ensure_agent_tables()
        self._warehouse.insert_agent_runs([agent_run_row(result)])
        event_rows = agent_run_event_rows(result)
        if event_rows:
            self._warehouse.insert_agent_run_events(event_rows)

    def sync(self) -> ReviewSummary:
        from personal_data_warehouse.agent_runner import AgentRunRequest
        from personal_data_warehouse.receipt_enrichment import merge_usage, usage_from_events

        self._warehouse.ensure_finance_tables()
        due = self._due()
        summary = ReviewSummary(candidates=len(due))
        if not due:
            return summary
        self._log.info(f"capital one alert review: {len(due)} unresolved alerts due")
        for alert in due:
            window = self._window(alert)
            posted = self._warehouse._query_dicts(
                POSTED_CANDIDATES_SQL, {**window, "matched": list(MATCHED_METHODS)}
            )
            others = self._warehouse._query_dicts(
                OTHER_ALERTS_SQL,
                {
                    **window,
                    "since": alert["posted_at"] - timedelta(days=OTHER_ALERT_DAYS),
                    "until": alert["posted_at"] + timedelta(days=OTHER_ALERT_DAYS + 1),
                    "unresolved": list(UNRESOLVED_METHODS),
                    "source_row_key": alert["source_row_key"],
                },
            )
            now = self._clock()
            request = AgentRunRequest(
                prompt=review_prompt(alert, posted, others, now=now),
                schema=review_schema(),
                task_type=ALERT_REVIEW_TASK_TYPE,
                subject_id=str(alert["source_row_key"]),
                prompt_version=PROMPT_VERSION,
            )
            started = time.monotonic()
            result = self._agent.run_with_pdw(request)
            self._record_agent_result(result)
            merge_usage(summary.usage, usage_from_events(result.events))
            if result.exit_code != 0:
                self._log.warning(
                    f"capital one alert review failed for {alert['source_row_key']}: {result.error}"
                )
                summary.failed += 1
                continue
            row = review_row(
                result.final_output_json or {},
                alert=alert,
                candidate_ids={str(row["transaction_id"]) for row in posted},
                provider=self._provider,
                model=self._model,
                agent_run_id=request.run_id,
                elapsed_ms=int((time.monotonic() - started) * 1000),
                now=now,
            )
            self._warehouse.insert_finance_alert_reviews([row])
            summary.reviewed += 1
            if row["decision"] == REVIEW_SETTLED_BY:
                summary.settled += 1
            elif row["decision"] == REVIEW_NOT_POSTED:
                summary.not_posted += 1
            else:
                summary.undetermined += 1
        return summary
