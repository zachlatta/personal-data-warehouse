"""Finance ledger: the derived stocks-and-flows layer over the finance sources.

Every finance source is a witness to one of two fact types: a **flow** (money
moved: a transaction) or a **stock** (something was worth X at time T: a
balance, valuation, or principal). This runner resolves raw source rows into
logical `finance.accounts` (via `finance.account_links`, the photos-identity
pattern: raw rows never learn about identity), appends per-day
`finance.observations`, and builds the unified deduped `finance.transactions`
ledger (via `finance.transaction_links`). Net worth is the latest observation
per account summed by side — read through `marts.finance_net_worth` /
`marts.finance_net_worth_history`; transactions through
`marts.finance_transactions`.

Sign convention: ledger amounts are signed NUMERIC, **positive = inflow to
the account**. Plaid reports positive-out, so Plaid amounts are negated at
ingest; document transactions carry an explicit in/out direction.

The ledger stores facts only. Categories and other opinions belong to future
enrichment layers, never to these tables.

Replayability contract: this runner never mutates raw source rows.
Accounts/links/observations are append-or-update; transactions+links are
reconciled to the current source rows every run (a Plaid pending row's ledger
row disappears when its posted successor arrives). Deleting every `finance.*`
row and re-running rebuilds identically (ids are deterministic from source
provenance) — pinned by tests/test_finance_ledger.py.
"""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from personal_data_warehouse.postgres import PostgresWarehouse

LEDGER_SOURCE_PLAID = "plaid"
LEDGER_SOURCE_MANUAL = "manual_finance"

OBSERVATION_KIND_BALANCE = "balance"
OBSERVATION_KIND_VALUATION = "valuation"
OBSERVATION_KIND_PRINCIPAL = "principal"

ACCOUNT_SIDE_ASSET = "asset"
ACCOUNT_SIDE_LIABILITY = "liability"

# Logical account kinds. Plaid covers the linked-institution kinds; manual
# documents introduce property / vehicle / private_fund accounts.
ACCOUNT_KINDS = (
    "checking",
    "savings",
    "credit",
    "brokerage",
    "ira",
    "mortgage",
    "property",
    "vehicle",
    "private_fund",
    "other",
)

# Cross-source transaction dedup: a statement transaction merges into a Plaid
# ledger row when the account and amount match exactly and the dates are
# within this many days (posted-vs-statement date drift).
FUZZY_MATCH_MAX_DAYS = 3

_SAVINGS_SUBTYPES = {"savings", "hsa", "cd", "money market"}
_IRA_SUBTYPES = {"ira", "roth", "roth ira", "401k", "403b", "457b", "sep ira", "simple ira", "pension"}
_INVESTMENT_TYPES = {"investment", "brokerage"}


def stable_finance_account_id(source: str, account: str, source_account_key: str) -> str:
    """Deterministic ledger account id from the founding source row's provenance."""
    digest = hashlib.sha256(f"{source}|{account}|{source_account_key}".encode()).hexdigest()
    return f"fa_{digest[:24]}"


def stable_finance_transaction_id(source: str, source_row_key: str) -> str:
    """Deterministic ledger transaction id from the founding source row."""
    digest = hashlib.sha256(f"{source}|{source_row_key}".encode()).hexdigest()
    return f"ft_{digest[:24]}"


def plaid_account_kind_side(type_: str, subtype: str) -> tuple[str, str]:
    t = (type_ or "").strip().lower()
    s = (subtype or "").strip().lower()
    if t == "depository":
        if s == "checking":
            return ("checking", ACCOUNT_SIDE_ASSET)
        if s in _SAVINGS_SUBTYPES:
            return ("savings", ACCOUNT_SIDE_ASSET)
        return ("other", ACCOUNT_SIDE_ASSET)
    if t == "credit":
        return ("credit", ACCOUNT_SIDE_LIABILITY)
    if t == "loan":
        if s == "mortgage":
            return ("mortgage", ACCOUNT_SIDE_LIABILITY)
        return ("other", ACCOUNT_SIDE_LIABILITY)
    if t in _INVESTMENT_TYPES:
        if s in _IRA_SUBTYPES:
            return ("ira", ACCOUNT_SIDE_ASSET)
        return ("brokerage", ACCOUNT_SIDE_ASSET)
    return ("other", ACCOUNT_SIDE_ASSET)


def document_kind_side(document_type: str, *, name_hint: str = "", account_folder: str = "") -> tuple[str, str]:
    """Kind/side for an account founded by a document (no Plaid counterpart)."""
    doc_type = (document_type or "").strip().lower()
    hint = f"{name_hint} {account_folder}".lower()
    if "vehicle" in doc_type or "vehicle" in hint:
        return ("vehicle", ACCOUNT_SIDE_ASSET)
    if "mortgage" in doc_type or "mortgage" in hint:
        return ("mortgage", ACCOUNT_SIDE_LIABILITY)
    if "property" in doc_type or "real-estate" in hint or "real estate" in hint:
        return ("property", ACCOUNT_SIDE_ASSET)
    if "fund" in doc_type or doc_type == "positions":
        return ("private_fund", ACCOUNT_SIDE_ASSET)
    if "credit" in doc_type:
        return ("credit", ACCOUNT_SIDE_LIABILITY)
    if "brokerage" in doc_type or "investment" in doc_type:
        return ("brokerage", ACCOUNT_SIDE_ASSET)
    if "bank" in doc_type or "checking" in hint:
        if "savings" in hint:
            return ("savings", ACCOUNT_SIDE_ASSET)
        return ("checking", ACCOUNT_SIDE_ASSET)
    if "savings" in hint:
        return ("savings", ACCOUNT_SIDE_ASSET)
    return ("other", ACCOUNT_SIDE_ASSET)


def document_account_key(*, original_path: str, institution: str, mask: str, filename: str) -> str:
    """Stable per-account key for document-derived accounts.

    The uploader's folder-per-account organization is the most stable
    identity (agent-extracted institution/mask can vary between statements of
    the same account); fall back to institution|mask, then the filename stem.
    """
    parts = [part for part in original_path.split("/") if part]
    if len(parts) > 1:
        return parts[0].strip().lower()
    if institution.strip() or mask.strip():
        return f"{_slug(institution)}|{mask.strip()}"
    return _slug(filename.rsplit(".", 1)[0])


def normalize_description(text: str) -> str:
    return re.sub(r"[^a-z0-9 ]+", " ", text.lower()).strip()


def description_similarity(a: str, b: str) -> float:
    tokens_a = set(normalize_description(a).split())
    tokens_b = set(normalize_description(b).split())
    if not tokens_a or not tokens_b:
        return 0.0
    return len(tokens_a & tokens_b) / len(tokens_a | tokens_b)


@dataclass(frozen=True)
class FinanceLedgerSummary:
    accounts_seen: int
    accounts_created: int
    links_created: int
    observations_upserted: int
    transactions_upserted: int = 0
    transactions_merged: int = 0
    transactions_skipped: int = 0
    transactions_removed: int = 0


class FinanceLedgerRunner:
    def __init__(
        self,
        *,
        warehouse: PostgresWarehouse,
        logger: logging.Logger | None = None,
        now: datetime | None = None,
    ) -> None:
        self._warehouse = warehouse
        self._logger = logger or logging.getLogger(__name__)
        self._now = now

    def sync(self) -> FinanceLedgerSummary:
        self._warehouse.ensure_finance_tables()
        # The ledger consumes manual_finance extractions; ensure that source's
        # tables so a fresh schema (or a deploy where the extraction asset has
        # not run yet) doesn't fail the scan.
        self._warehouse.ensure_manual_finance_tables()
        now = self._now or datetime.now(tz=UTC)
        sync_version = int(now.timestamp() * 1_000_000)

        existing_created_at = self._load_account_created_at()
        accounts_created = 0
        links_created = 0
        observation_rows: list[dict[str, Any]] = []

        # --- plaid accounts + daily balance observations -----------------------
        plaid_accounts = self._load_plaid_accounts()
        plaid_links = self._load_links(LEDGER_SOURCE_PLAID)
        account_rows: list[dict[str, Any]] = []
        link_rows: list[dict[str, Any]] = []
        plaid_account_map: dict[tuple[str, str], str] = {}
        for row in plaid_accounts:
            link_key = (row["account"], row["account_id"])
            account_id = plaid_links.get(link_key)
            if account_id is None:
                account_id = stable_finance_account_id(
                    LEDGER_SOURCE_PLAID, row["account"], row["account_id"]
                )
                link_rows.append(
                    self._link_row(
                        source=LEDGER_SOURCE_PLAID,
                        account=row["account"],
                        source_account_key=row["account_id"],
                        account_id=account_id,
                        match_method="source_id",
                        match_score=1.0,
                        now=now,
                        sync_version=sync_version,
                    )
                )
                links_created += 1
            plaid_account_map[link_key] = account_id

            kind, side = plaid_account_kind_side(row["type"], row["subtype"])
            created_at = existing_created_at.get(account_id)
            if created_at is None:
                accounts_created += 1
            account_rows.append(
                {
                    "account_id": account_id,
                    "account": row["account"],
                    "name": row["name"] or row["official_name"],
                    "kind": kind,
                    "side": side,
                    "currency": row["iso_currency_code"],
                    "institution": row["institution_name"],
                    "mask": row["mask"],
                    "created_at": created_at or now,
                    "updated_at": now,
                    "sync_version": sync_version,
                }
            )
            observation_rows.append(
                {
                    "account_id": account_id,
                    "as_of": now.date(),
                    "kind": OBSERVATION_KIND_BALANCE,
                    "value": row["current_balance"],
                    "currency": row["iso_currency_code"],
                    "source": LEDGER_SOURCE_PLAID,
                    "observed_at": now,
                    "sync_version": sync_version,
                }
            )

        self._warehouse.insert_finance_accounts(account_rows)
        self._warehouse.insert_finance_account_links(link_rows)

        # --- document accounts, observations, and the unified transactions -----
        extractions = self._load_latest_extractions()
        manual_links = self._load_links(LEDGER_SOURCE_MANUAL)
        account_index = self._load_account_index()
        doc_account_rows: list[dict[str, Any]] = []
        doc_link_rows: list[dict[str, Any]] = []
        doc_accounts: dict[str, str] = {}  # extraction sha -> ledger account id
        doc_account_kinds: dict[str, str] = {}
        for extraction in extractions:
            key = document_account_key(
                original_path=str(extraction["original_path"]),
                institution=str(extraction["institution"]),
                mask=str(extraction["account_mask"]),
                filename=str(extraction["filename"]),
            )
            owner = str(extraction["account"])
            link_key = (owner, key)
            account_id = manual_links.get(link_key)
            if account_id is None:
                account_id, match_method, match_score = self._resolve_document_account(
                    extraction, account_index=account_index
                )
                if match_method == "new":
                    kind, side = document_kind_side(
                        str(extraction["document_type"]),
                        name_hint=str(extraction["account_name_hint"]),
                        account_folder=key,
                    )
                    account_id = stable_finance_account_id(LEDGER_SOURCE_MANUAL, owner, key)
                    doc_account_rows.append(
                        {
                            "account_id": account_id,
                            "account": owner,
                            "name": str(extraction["account_name_hint"]) or key,
                            "kind": kind,
                            "side": side,
                            "currency": str(extraction["currency"]),
                            "institution": str(extraction["institution"]),
                            "mask": str(extraction["account_mask"]),
                            "created_at": now,
                            "updated_at": now,
                            "sync_version": sync_version,
                        }
                    )
                    account_index.append(
                        {
                            "account_id": account_id,
                            "mask": str(extraction["account_mask"]),
                            "institution": str(extraction["institution"]),
                            "kind": kind,
                            "side": side,
                        }
                    )
                    accounts_created += 1
                    match_method = "document_new"
                    match_score = 1.0
                doc_link_rows.append(
                    self._link_row(
                        source=LEDGER_SOURCE_MANUAL,
                        account=owner,
                        source_account_key=key,
                        account_id=account_id,
                        match_method=match_method,
                        match_score=match_score,
                        now=now,
                        sync_version=sync_version,
                    )
                )
                manual_links[link_key] = account_id
                links_created += 1
            doc_accounts[str(extraction["content_sha256"])] = account_id
            doc_account_kinds[str(extraction["content_sha256"])] = next(
                (
                    str(entry.get("kind", ""))
                    for entry in account_index
                    if str(entry.get("account_id", "")) == account_id
                ),
                "",
            )

        self._warehouse.insert_finance_accounts(doc_account_rows)
        self._warehouse.insert_finance_account_links(doc_link_rows)

        observation_rows.extend(
            self._document_observation_rows(
                extractions,
                doc_accounts=doc_accounts,
                doc_account_kinds=doc_account_kinds,
                now=now,
                sync_version=sync_version,
            )
        )
        self._warehouse.insert_finance_observations(observation_rows)

        transactions = self._build_transactions(
            plaid_account_map=plaid_account_map,
            extractions=extractions,
            doc_accounts=doc_accounts,
            now=now,
            sync_version=sync_version,
        )

        summary = FinanceLedgerSummary(
            accounts_seen=len(plaid_accounts) + len(extractions),
            accounts_created=accounts_created,
            links_created=links_created,
            observations_upserted=len(observation_rows),
            transactions_upserted=transactions["upserted"],
            transactions_merged=transactions["merged"],
            transactions_skipped=transactions["skipped"],
            transactions_removed=transactions["removed"],
        )
        self._logger.info(
            "Finance ledger: accounts_seen=%s accounts_created=%s links_created=%s observations=%s "
            "transactions=%s merged=%s skipped=%s removed=%s",
            summary.accounts_seen,
            summary.accounts_created,
            summary.links_created,
            summary.observations_upserted,
            summary.transactions_upserted,
            summary.transactions_merged,
            summary.transactions_skipped,
            summary.transactions_removed,
        )
        return summary

    # --- transactions ---------------------------------------------------------

    def _build_transactions(
        self,
        *,
        plaid_account_map: dict[tuple[str, str], str],
        extractions: list[dict[str, Any]],
        doc_accounts: dict[str, str],
        now: datetime,
        sync_version: int,
    ) -> dict[str, int]:
        transaction_rows: dict[str, dict[str, Any]] = {}
        link_rows: list[dict[str, Any]] = []
        merged = 0
        skipped = 0

        # Plaid flows first (they win field precedence and found the pool the
        # fuzzy dedup matches against). Deterministic order so replay
        # reproduces founding ids.
        plaid_rows = self._load_plaid_transactions()
        posted_by_pending_id = {
            str(row["pending_transaction_id"]): row
            for row in plaid_rows
            if row["pending_transaction_id"]
        }
        # Per-account pool for cross-source dedup: (account_id, amount) -> entries.
        pool: dict[tuple[str, Decimal], list[dict[str, Any]]] = {}
        for row in plaid_rows:
            owner = str(row["account"])
            source_row_key = f"{owner}|{row['transaction_id']}"
            account_id = plaid_account_map.get((owner, str(row["account_id"])))
            if account_id is None:
                skipped += 1
                continue
            posted_successor = (
                int(row["pending"]) == 1 and str(row["transaction_id"]) in posted_by_pending_id
            )
            if posted_successor:
                # A live pending row whose posted successor is already in the
                # set: the posted row is the ledger transaction; the pending
                # row just links to it.
                successor = posted_by_pending_id[str(row["transaction_id"])]
                link_rows.append(
                    self._link_row(
                        source=LEDGER_SOURCE_PLAID,
                        account="",
                        source_account_key="",
                        account_id="",
                        match_method="pending_id",
                        match_score=1.0,
                        now=now,
                        sync_version=sync_version,
                        as_transaction=True,
                        source_row_key=source_row_key,
                        transaction_id=stable_finance_transaction_id(
                            LEDGER_SOURCE_PLAID, f"{successor['account']}|{successor['transaction_id']}"
                        ),
                    )
                )
                merged += 1
                continue
            transaction_id = stable_finance_transaction_id(LEDGER_SOURCE_PLAID, source_row_key)
            amount = -_as_decimal(row["amount"])
            transaction_rows[transaction_id] = {
                "transaction_id": transaction_id,
                "account_id": account_id,
                "posted_at": row["posted_at"],
                # Plaid reports positive-out; the ledger is positive-in.
                "amount": amount,
                "currency": str(row["iso_currency_code"]),
                "description": str(row["name"]),
                "merchant": str(row["merchant_name"]),
                "pending": int(row["pending"]),
                "source": LEDGER_SOURCE_PLAID,
                "created_at": now,
                "sync_version": sync_version,
            }
            link_rows.append(
                self._link_row(
                    source=LEDGER_SOURCE_PLAID,
                    account="",
                    source_account_key="",
                    account_id="",
                    match_method="source_id",
                    match_score=1.0,
                    now=now,
                    sync_version=sync_version,
                    as_transaction=True,
                    source_row_key=source_row_key,
                    transaction_id=transaction_id,
                )
            )
            pool.setdefault((account_id, amount), []).append(
                {
                    "transaction_id": transaction_id,
                    "posted_on": _as_date(row["posted_at"]),
                    "description": str(row["name"]),
                    "used": False,
                }
            )

        # Document flows: merge into the Plaid pool where possible (the
        # statement/Plaid overlap seam), otherwise found new ledger rows.
        for extraction in sorted(extractions, key=lambda e: str(e["content_sha256"])):
            sha = str(extraction["content_sha256"])
            account_id = doc_accounts.get(sha)
            if account_id is None:
                continue
            currency = str(extraction["currency"])
            for index, entry in enumerate(extraction["transactions_json"] or []):
                source_row_key = f"{sha}|{index}"
                posted_on = _parse_iso_date(str(entry.get("date", "")))
                amount = _parse_money(str(entry.get("amount", "")))
                direction = str(entry.get("direction", "")).strip().lower()
                if posted_on is None or amount is None or direction not in {"in", "out"}:
                    skipped += 1
                    continue
                signed = amount if direction == "in" else -amount
                match = self._best_pool_match(
                    pool,
                    account_id=account_id,
                    amount=signed,
                    posted_on=posted_on,
                    description=str(entry.get("description", "")),
                )
                if match is not None:
                    match["used"] = True
                    link_rows.append(
                        self._link_row(
                            source=LEDGER_SOURCE_MANUAL,
                            account="",
                            source_account_key="",
                            account_id="",
                            match_method="fuzzy_amount_date",
                            match_score=match["score"],
                            now=now,
                            sync_version=sync_version,
                            as_transaction=True,
                            source_row_key=source_row_key,
                            transaction_id=match["transaction_id"],
                        )
                    )
                    merged += 1
                    continue
                transaction_id = stable_finance_transaction_id(LEDGER_SOURCE_MANUAL, source_row_key)
                transaction_rows[transaction_id] = {
                    "transaction_id": transaction_id,
                    "account_id": account_id,
                    "posted_at": datetime(posted_on.year, posted_on.month, posted_on.day, tzinfo=UTC),
                    "amount": signed,
                    "currency": currency,
                    "description": str(entry.get("description", "")),
                    "merchant": "",
                    "pending": 0,
                    "source": LEDGER_SOURCE_MANUAL,
                    "created_at": now,
                    "sync_version": sync_version,
                }
                link_rows.append(
                    self._link_row(
                        source=LEDGER_SOURCE_MANUAL,
                        account="",
                        source_account_key="",
                        account_id="",
                        match_method="source_id",
                        match_score=1.0,
                        now=now,
                        sync_version=sync_version,
                        as_transaction=True,
                        source_row_key=source_row_key,
                        transaction_id=transaction_id,
                    )
                )

        self._warehouse.insert_finance_transactions(list(transaction_rows.values()))
        self._warehouse.insert_finance_transaction_links(link_rows)
        removed = self._warehouse.reconcile_finance_transactions(
            transaction_ids=list(transaction_rows.keys()),
            link_keys=[f"{row['source']}|{row['source_row_key']}" for row in link_rows],
        )
        return {
            "upserted": len(transaction_rows),
            "merged": merged,
            "skipped": skipped,
            "removed": removed,
        }

    def _best_pool_match(
        self,
        pool: dict[tuple[str, Decimal], list[dict[str, Any]]],
        *,
        account_id: str,
        amount: Decimal,
        posted_on: date,
        description: str,
    ) -> dict[str, Any] | None:
        candidates = pool.get((account_id, amount), [])
        best: dict[str, Any] | None = None
        best_rank: tuple[int, float, str] | None = None
        for candidate in candidates:
            if candidate["used"]:
                continue
            day_diff = abs((candidate["posted_on"] - posted_on).days)
            if day_diff > FUZZY_MATCH_MAX_DAYS:
                continue
            similarity = description_similarity(candidate["description"], description)
            rank = (day_diff, -similarity, candidate["transaction_id"])
            if best_rank is None or rank < best_rank:
                best_rank = rank
                best = candidate
        if best is None:
            return None
        day_diff = abs((best["posted_on"] - posted_on).days)
        return {**best, "score": round(1.0 - day_diff / (FUZZY_MATCH_MAX_DAYS + 1), 3)}

    # --- documents -> accounts/observations ------------------------------------

    def _resolve_document_account(
        self, extraction: dict[str, Any], *, account_index: list[dict[str, Any]]
    ) -> tuple[str, str, float]:
        """Match a document to an existing ledger account by mask (+ institution
        tiebreak). Returns (account_id, match_method, match_score); method
        'new' means the caller must create the account."""
        mask = str(extraction["account_mask"]).strip()
        institution = str(extraction["institution"]).strip().lower()
        if mask:
            matches = [entry for entry in account_index if str(entry.get("mask", "")).strip() == mask]
            if len(matches) > 1 and institution:
                narrowed = [
                    entry
                    for entry in matches
                    if institution in str(entry.get("institution", "")).lower()
                    or str(entry.get("institution", "")).lower() in institution
                ]
                matches = narrowed or matches
            if len(matches) == 1:
                return (str(matches[0]["account_id"]), "mask", 0.9)
        return ("", "new", 1.0)

    def _document_observation_rows(
        self,
        extractions: list[dict[str, Any]],
        *,
        doc_accounts: dict[str, str],
        doc_account_kinds: dict[str, str],
        now: datetime,
        sync_version: int,
    ) -> list[dict[str, Any]]:
        rows: dict[tuple[str, date, str], dict[str, Any]] = {}
        for extraction in extractions:
            sha = str(extraction["content_sha256"])
            account_id = doc_accounts.get(sha)
            if account_id is None:
                continue
            currency = str(extraction["currency"])
            # A mortgage statement's balance is its outstanding principal.
            balance_kind = (
                OBSERVATION_KIND_PRINCIPAL
                if doc_account_kinds.get(sha) == "mortgage"
                else OBSERVATION_KIND_BALANCE
            )
            entries: list[tuple[dict[str, Any], str, str]] = [
                (entry, balance_kind, "balance") for entry in extraction["balances_json"] or []
            ] + [
                (entry, OBSERVATION_KIND_VALUATION, "value")
                for entry in extraction["valuations_json"] or []
            ]
            for entry, kind, value_key in entries:
                as_of = _parse_iso_date(str(entry.get("date", "")))
                value = _parse_money(str(entry.get(value_key, "")))
                if as_of is None or value is None:
                    continue
                rows[(account_id, as_of, kind)] = {
                    "account_id": account_id,
                    "as_of": as_of,
                    "kind": kind,
                    "value": value,
                    "currency": currency,
                    "source": LEDGER_SOURCE_MANUAL,
                    "observed_at": now,
                    "sync_version": sync_version,
                }
        return list(rows.values())

    # --- loading -------------------------------------------------------------

    def _load_plaid_accounts(self) -> list[dict[str, Any]]:
        return self._warehouse._query_dicts(
            """
            SELECT a.account, a.account_id, a.name, a.official_name, a.mask,
                   a.type, a.subtype, a.current_balance, a.iso_currency_code,
                   COALESCE(i.institution_name, '') AS institution_name
            FROM plaid_accounts a
            LEFT JOIN plaid_items i
              ON i.account = a.account AND i.item_id = a.item_id
            WHERE a.is_removed = 0
            ORDER BY a.account, a.account_id
            """
        )

    def _load_plaid_transactions(self) -> list[dict[str, Any]]:
        return self._warehouse._query_dicts(
            """
            SELECT account, account_id, transaction_id, posted_at, name,
                   merchant_name, amount, iso_currency_code, pending,
                   pending_transaction_id
            FROM plaid_transactions
            WHERE is_removed = 0
            ORDER BY posted_at, transaction_id
            """
        )

    def _load_latest_extractions(self) -> list[dict[str, Any]]:
        """The latest completed-ok extraction per document, with its document's
        provenance (owner account + original_path)."""
        return self._warehouse._query_dicts(
            """
            SELECT DISTINCT ON (e.content_sha256)
                   e.content_sha256, e.document_type, e.institution,
                   e.account_name_hint, e.account_mask, e.currency,
                   e.transactions_json, e.balances_json, e.valuations_json,
                   d.account, d.original_path, d.filename
            FROM manual_finance_extractions e
            JOIN manual_finance_documents d
              ON d.content_sha256 = e.content_sha256 AND d.is_deleted = 0
            WHERE e.status = 'ok'
            ORDER BY e.content_sha256, e.created_at DESC
            """
        )

    def _load_links(self, source: str) -> dict[tuple[str, str], str]:
        rows = self._warehouse._query(
            """
            SELECT account, source_account_key, account_id
            FROM finance_account_links
            WHERE source = %s
            """,
            (source,),
        )
        return {(str(account), str(key)): str(account_id) for account, key, account_id in rows}

    def _load_account_created_at(self) -> dict[str, datetime]:
        rows = self._warehouse._query("SELECT account_id, created_at FROM finance_accounts")
        return {str(account_id): created_at for account_id, created_at in rows}

    def _load_account_index(self) -> list[dict[str, Any]]:
        return self._warehouse._query_dicts(
            "SELECT account_id, mask, institution, kind, side FROM finance_accounts"
        )

    def _link_row(
        self,
        *,
        source: str,
        account: str,
        source_account_key: str,
        account_id: str,
        match_method: str,
        match_score: float,
        now: datetime,
        sync_version: int,
        as_transaction: bool = False,
        source_row_key: str = "",
        transaction_id: str = "",
    ) -> dict[str, Any]:
        if as_transaction:
            return {
                "source": source,
                "source_row_key": source_row_key,
                "transaction_id": transaction_id,
                "match_method": match_method,
                "match_score": match_score,
                "created_at": now,
                "sync_version": sync_version,
            }
        return {
            "source": source,
            "account": account,
            "source_account_key": source_account_key,
            "account_id": account_id,
            "match_method": match_method,
            "match_score": match_score,
            "created_at": now,
            "sync_version": sync_version,
        }


def has_pending_finance_observations(warehouse: PostgresWarehouse) -> bool:
    """True when a live plaid account is missing its link or today's observation."""
    rows = warehouse._query(
        """
        SELECT 1
        FROM plaid_accounts a
        LEFT JOIN finance_account_links l
          ON l.source = %s AND l.account = a.account AND l.source_account_key = a.account_id
        LEFT JOIN finance_observations o
          ON o.account_id = l.account_id
         AND o.source = %s
         AND o.kind = %s
         AND o.as_of = CURRENT_DATE
        WHERE a.is_removed = 0
          AND (l.account_id IS NULL OR o.account_id IS NULL)
        LIMIT 1
        """,
        (LEDGER_SOURCE_PLAID, LEDGER_SOURCE_PLAID, OBSERVATION_KIND_BALANCE),
    )
    return bool(rows)


def _parse_iso_date(value: str) -> date | None:
    value = value.strip()
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _parse_money(value: str) -> Decimal | None:
    cleaned = value.strip().replace(",", "").replace("$", "")
    if not cleaned:
        return None
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def _as_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    parsed = _parse_iso_date(str(value))
    return parsed or date(1970, 1, 1)


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-")
