"""Deterministic Capital One email evidence, not a second bank balance feed.

Unknown templates/account identities fail closed. Matching is a one-to-one graph:
only an unambiguous account/currency/merchant/amount/date edge can reconcile.
Changed authorizations (including tips) require review, never a guessed merge.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from email.utils import parseaddr
from typing import Any

SOURCE = "capital_one_alert"
SENDER = "capitalone@notification.capitalone.com"
SUBJECT = "A new transaction was charged to your account"
_PURCHASE = re.compile(
    r"on (?P<date>[A-Za-z]+\.? \d{1,2}, \d{4}), at (?P<merchant>[^\r\n]+?), "
    r"a pending authorization or purchase in the amount of \$(?P<amount>\d{1,3}(?:,\d{3})*\.\d{2}|\d+\.\d{2}) "
    r"was placed or charged on your [^\r\n]+ Card\."
)


def _authenticated(row: dict[str, Any]) -> bool:
    try:
        payload = row.get("payload_json", {})
        if isinstance(payload, str):
            payload = json.loads(payload)
        headers = payload.get("payload", {}).get("headers", [])
        # Use the first receiving-Gmail verdict, not a later forwarded header.
        verdict = next(
            (
                h.get("value", "").lower()
                for h in headers
                if h.get("name", "").lower() == "authentication-results"
                and h.get("value", "").lower().startswith("mx.google.com;")
            ),
            "",
        )
        return bool(
            re.search(r"(?:^|;)\s*dkim=pass\s[^;]*header\.i=@notification\.capitalone\.com(?:\s|;|$)", verdict)
            and re.search(r"(?:^|;)\s*dmarc=pass\s[^;]*header\.from=notification\.capitalone\.com(?:\s|;|$)", verdict)
        )
    except (ValueError, TypeError, AttributeError):
        return False


def parse_purchase_alert(row: dict[str, Any]) -> dict[str, Any] | None:
    if parseaddr(str(row["from_address"]))[1].lower() != SENDER or row["subject"] != SUBJECT:
        return None
    if not _authenticated(row):
        return None
    body = str(row["body_text"])
    masks = re.findall(r"About your [^\r\n]+ Card ending in (\d{4})\b", body)
    purchases = list(_PURCHASE.finditer(body))
    if len(masks) != 1 or len(purchases) != 1:
        return None
    purchase = purchases[0]
    try:
        day = datetime.strptime(purchase["date"].replace(".", ""), "%b %d, %Y").replace(tzinfo=UTC)
    except ValueError:
        return None
    return dict(
        source_row_key=f"{row['account']}|{row['message_id']}",
        account=row["account"],
        mask=masks[0],
        posted_at=day,
        merchant=purchase["merchant"].strip(),
        amount=-Decimal(purchase["amount"].replace(",", "")),
        currency="USD",
    )


def _merchant(value: str) -> str:
    normalized = " ".join(re.findall(r"[a-z0-9]+", value.lower()))
    return re.sub(r"^(?:sp|sq) ", "", normalized)


def reconcile_alerts(
    emails: list[dict[str, Any]],
    accounts: list[dict[str, Any]],
    authoritative: list[dict[str, Any]],
    now: datetime,
    sync_version: int,
    *,
    authorizations: Sequence[dict[str, Any]] = (),
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    alerts = {}
    for email in emails:
        alert = parse_purchase_alert(email)
        if alert is None:
            continue
        matches = [
            a
            for a in accounts
            if a["account"] == alert["account"]
            and a["mask"] == alert["mask"]
            and a["kind"] == "credit"
            and _merchant(a["institution"]) == "capital one"
        ]
        if len(matches) != 1:
            continue
        alert["account_id"] = matches[0]["account_id"]
        alerts[alert["source_row_key"]] = alert

    def same_purchase(alert: dict[str, Any], transaction: dict[str, Any]) -> bool:
        return (
            transaction["account_id"] == alert["account_id"]
            and transaction["currency"] == alert["currency"]
            and 0 <= (transaction["posted_at"].date() - alert["posted_at"].date()).days <= 7
            and _merchant(alert["merchant"])
            in {_merchant(transaction["merchant"]), _merchant(transaction["description"])}
            and transaction["amount"] < 0
        )

    witnesses = {
        key: [t for t in authorizations if same_purchase(alert, t) and t["amount"] == alert["amount"]]
        for key, alert in alerts.items()
    }
    witness_claims: dict[str, int] = {}
    for candidates in witnesses.values():
        for witness in candidates:
            tid = witness["transaction_id"]
            witness_claims[tid] = witness_claims.get(tid, 0) + 1
    unique_witnesses = {
        key: candidates[0]
        for key, candidates in witnesses.items()
        if len(candidates) == 1 and witness_claims[candidates[0]["transaction_id"]] == 1
    }
    authoritative_by_id = {t["transaction_id"]: t for t in authoritative}
    native_successors = {}

    # Exact-amount edges decide reconciliation. Near edges surface changed
    # authorizations for review without preventing distinct-price purchases.
    near = {}
    exact = {}
    claims: dict[str, int] = {}
    for key, alert in alerts.items():
        candidates = [t for t in authoritative if same_purchase(alert, t)]
        near[key] = candidates
        exact[key] = [t for t in candidates if t["amount"] == alert["amount"]]
        witness = unique_witnesses.get(key)
        successor = authoritative_by_id.get(witness["successor_transaction_id"]) if witness else None
        if (
            successor is not None
            and successor["account_id"] == alert["account_id"]
            and successor["currency"] == alert["currency"]
            and not successor["pending"]
        ):
            # A provider's pending_transaction_id is stronger evidence than
            # merchant text or amount: tips and final hotel totals may change.
            exact[key] = [successor]
            native_successors[key] = successor["transaction_id"]
        for candidate in exact[key]:
            tid = candidate["transaction_id"]
            claims[tid] = claims.get(tid, 0) + 1

    rows, links = [], []
    for key, alert in sorted(alerts.items()):
        candidates = exact[key]
        match = candidates[0] if len(candidates) == 1 else None
        if match is not None and claims[match["transaction_id"]] != 1:
            match = None
        if match is not None:
            transaction_id = match["transaction_id"]
            method = "alert_pending_successor" if key in native_successors else "alert_amount_merchant_date"
        else:
            transaction_id = "ft_" + hashlib.sha256(f"{SOURCE}|{key}".encode()).hexdigest()[:24]
            stale = (now.date() - alert["posted_at"].date()).days > 30
            witness = unique_witnesses.get(key)
            if witness and witness["removed"] and not near[key]:
                method = "authorization_removed"
            elif near[key] or witnesses[key]:
                method = "needs_review"
            else:
                method = "expired_unconfirmed" if stale else "provisional"
            rows.append(
                dict(
                    transaction_id=transaction_id,
                    account_id=alert["account_id"],
                    posted_at=alert["posted_at"],
                    amount=alert["amount"],
                    currency=alert["currency"],
                    description=alert["merchant"],
                    merchant=alert["merchant"],
                    pending=1,
                    source=SOURCE,
                    created_at=now,
                    sync_version=sync_version,
                )
            )
        links.append(
            dict(
                source=SOURCE,
                source_row_key=key,
                transaction_id=transaction_id,
                match_method=method,
                match_score=1.0 if match else 0.0,
                created_at=now,
                sync_version=sync_version,
            )
        )
    return rows, links
