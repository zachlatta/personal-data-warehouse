"""Finance ledger: the derived stocks-and-flows layer over the finance sources.

Every finance source is a witness to one of two fact types: a **flow** (money
moved: a transaction) or a **stock** (something was worth X at time T: a
balance, valuation, or principal). This runner resolves raw source rows into
logical `derived_finance.accounts` (via `derived_finance.account_links`, the photos-identity
pattern: raw rows never learn about identity), appends per-day
`derived_finance.observations`, and builds the unified deduped `derived_finance.transactions`
ledger (via `derived_finance.transaction_links`). Net worth is the latest observation
per account summed by side — read through `marts_finance.net_worth` /
`marts_finance.net_worth_history`; transactions through
`marts_finance.transactions`.

Sign convention: ledger amounts are signed NUMERIC, **positive = inflow to
the account**. Plaid reports positive-out (in both the transactions and the
investments products), so Plaid amounts are negated at ingest; document
transactions carry an explicit in/out direction.

Plaid contributes two flow feeds: `plaid_transactions` (depository/credit
accounts) and `plaid_investment_transactions` (brokerage/IRA accounts, whose
entire activity — cash movements and trades — arrives via the investments
product). Each account's movements arrive via exactly one feed.

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
from personal_data_warehouse.securities_ledger import (
    SecurityIdentity,
    SecurityResolver,
    build_tax_lots,
    dedupe_security_trades,
    ASSET_CLASS_OPTION,
    classify_asset_class,
    document_trade_entry,
    option_identity,
    plaid_trade_side,
)


LEDGER_SOURCE_PLAID = "plaid"
LEDGER_SOURCE_MANUAL = "manual_finance"

OBSERVATION_KIND_BALANCE = "balance"
OBSERVATION_KIND_VALUATION = "valuation"
OBSERVATION_KIND_PRINCIPAL = "principal"

ACCOUNT_SIDE_ASSET = "asset"
ACCOUNT_SIDE_LIABILITY = "liability"

# Logical account kinds. Plaid covers the linked-institution kinds; manual
# documents introduce property / vehicle / private_fund / receivable accounts.
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
    "receivable",
    "other",
)

# Cross-source transaction dedup: a statement transaction merges into a Plaid
# ledger row when the account and amount match exactly and the dates are
# within this many days (posted-vs-statement date drift).
FUZZY_MATCH_MAX_DAYS = 3

# Absence sentinel for NOT NULL day columns (the receipts precedent).
# The marts views NULLIF it back out, so no reader sees 1970 as a date.
_SENTINEL_DATE = date(1970, 1, 1)

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
    # Money owed TO the owner (personal loans, debt records). Keyed on the
    # explicit "receivable" wording only — a bare "debt"/"loan" doesn't say
    # which side of it the owner is on.
    if "receivable" in doc_type or "receivable" in hint:
        return ("receivable", ACCOUNT_SIDE_ASSET)
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
    accounts_merged: int = 0
    accounts_pruned: int = 0
    links_relinked: int = 0
    observations_removed: int = 0
    security_trades_upserted: int = 0
    security_trades_merged: int = 0
    security_trades_removed: int = 0
    tax_lots_built: int = 0


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
        links_relinked = 0
        observation_rows: list[dict[str, Any]] = []

        # --- plaid accounts + daily balance observations -----------------------
        plaid_accounts = self._load_plaid_accounts()
        # A dead Item (action_required: login expired, NO_ACCOUNTS, ...) stops
        # updating base_plaid.accounts, but the last-known balances stay in
        # place. Re-stamping them as fresh daily observations would present
        # week-old numbers as current in net worth — so a dead item's accounts
        # stop accruing observations until the re-link, leaving the last honest
        # as_of visible.
        frozen_items = self._load_action_required_item_ids()
        plaid_links = self._load_links(LEDGER_SOURCE_PLAID)
        account_rows: list[dict[str, Any]] = []
        link_rows: list[dict[str, Any]] = []
        resolutions = self._resolve_plaid_accounts(
            plaid_accounts, links=plaid_links, index=self._load_account_index()
        )
        accounts_merged = 0
        plaid_account_map: dict[tuple[str, str], str] = {}
        for row in plaid_accounts:
            link_key = (row["account"], row["account_id"])
            account_id, match_method = resolutions[link_key]
            if match_method:
                # A new source account, or one whose link now points at the
                # account it merged into: (re)write the audit row.
                link_rows.append(
                    self._link_row(
                        source=LEDGER_SOURCE_PLAID,
                        account=row["account"],
                        source_account_key=row["account_id"],
                        account_id=account_id,
                        match_method=match_method,
                        match_score=1.0,
                        now=now,
                        sync_version=sync_version,
                    )
                )
                if link_key in plaid_links:
                    accounts_merged += 1
                else:
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
            if (row["account"], row["item_id"]) in frozen_items:
                self._logger.warning(
                    "Skipping daily balance for %s (%s ****%s): its Plaid item is "
                    "action_required, so the source balance is frozen at its last "
                    "pre-failure value",
                    account_id,
                    row["institution_name"],
                    row["mask"],
                )
                continue
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
        # Group extractions per logical account key first: one folder can span
        # an account-number change (Robinhood's Apex-era statements carry a
        # different mask than its later ones), so resolution must consider
        # EVERY mask the group's documents report — not whichever document
        # happened to process first.
        groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for extraction in extractions:
            key = document_account_key(
                original_path=str(extraction["original_path"]),
                institution=str(extraction["institution"]),
                mask=str(extraction["account_mask"]),
                filename=str(extraction["filename"]),
            )
            groups.setdefault((str(extraction["account"]), key), []).append(extraction)

        for (owner, key), group in sorted(groups.items()):
            group.sort(key=lambda e: str(e["content_sha256"]))
            link_key = (owner, key)
            linked_account_id = manual_links.get(link_key)
            # A link is a derived decision, not a fact, so it is re-resolved
            # every run rather than consulted and trusted. A link founded on
            # thinner evidence — fewer statements extracted, a plaid account
            # not yet linked, a superseded resolver — otherwise freezes: the
            # Robinhood crypto folder linked to the BROKERAGE account because
            # the one statement extracted at the time printed the brokerage
            # number in its header, and every later statement naming the
            # crypto account was ignored. The crypto trades then sat in an
            # account where nothing could dedupe them against the plaid rows
            # describing the same trades, double-booking the position.
            account_id, match_method, match_score = self._resolve_document_account_group(
                group, account_index=account_index
            )
            if match_method == "new" and linked_account_id is not None:
                # Nothing in the index claims any of this group's masks, which
                # is not a reason to abandon the account its own documents
                # founded on an earlier run.
                account_id, match_method = linked_account_id, ""
            elif match_method == "new":
                founding = group[0]
                kind, side = document_kind_side(
                    str(founding["document_type"]),
                    name_hint=str(founding["account_name_hint"]),
                    account_folder=key,
                )
                account_id = stable_finance_account_id(LEDGER_SOURCE_MANUAL, owner, key)
                doc_account_rows.append(
                    {
                        "account_id": account_id,
                        "account": owner,
                        "name": str(founding["account_name_hint"]) or key,
                        "kind": kind,
                        "side": side,
                        "currency": str(founding["currency"]),
                        "institution": str(founding["institution"]),
                        "mask": _group_primary_mask(group),
                        "created_at": now,
                        "updated_at": now,
                        "sync_version": sync_version,
                    }
                )
                account_index.append(
                    {
                        "account_id": account_id,
                        "mask": _group_primary_mask(group),
                        "institution": str(founding["institution"]),
                        "kind": kind,
                        "side": side,
                    }
                )
                accounts_created += 1
                match_method = "document_new"
                match_score = 1.0
            # Only write when the decision actually changed, so an unchanged
            # link keeps the timestamp of the run that first made it.
            if match_method and account_id != linked_account_id:
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
                if linked_account_id is None:
                    links_created += 1
                else:
                    links_relinked += 1
            account_kind = next(
                (
                    str(entry.get("kind", ""))
                    for entry in account_index
                    if str(entry.get("account_id", "")) == account_id
                ),
                "",
            )
            for extraction in group:
                doc_accounts[str(extraction["content_sha256"])] = account_id
                doc_account_kinds[str(extraction["content_sha256"])] = account_kind

        self._warehouse.insert_finance_accounts(doc_account_rows)
        self._warehouse.insert_finance_account_links(doc_link_rows)

        document_observations = self._document_observation_rows(
            extractions,
            doc_accounts=doc_accounts,
            doc_account_kinds=doc_account_kinds,
            now=now,
            sync_version=sync_version,
        )
        observation_rows.extend(document_observations)
        # Statement observations are rebuilt from the corpus every run, so a
        # row the corpus no longer produces is residue — most sharply after a
        # group re-resolves to a different account, which leaves its old
        # account holding balances no document claims any more.
        observations_removed = self._warehouse.delete_missing_document_observations(
            [
                f"{row['account_id']}|{row['as_of']}|{row['kind']}"
                for row in document_observations
            ]
        )
        # Manual documents are immutable source facts. Rebuilding the ledger
        # used to re-upsert every historical observation with ``observed_at =
        # now()``, making old statements look freshly written and sending all
        # of them through timeline incremental sync again. Plaid's same-day
        # balance is a real live re-observation and still advances each run;
        # unchanged manual facts do not.
        observation_rows = self._changed_observation_rows(observation_rows)
        self._warehouse.insert_finance_observations(observation_rows)

        transactions = self._build_transactions(
            plaid_account_map=plaid_account_map,
            extractions=extractions,
            doc_accounts=doc_accounts,
            now=now,
            sync_version=sync_version,
        )

        # Security-level trades and the lots they imply. Runs off the same
        # loaded sources as the cash pass, but is an independent fact table:
        # a brokerage buy is one cash flow AND one share movement.
        securities = self._build_security_trades(
            plaid_account_map=plaid_account_map,
            extractions=extractions,
            doc_accounts=doc_accounts,
            now=now,
            sync_version=sync_version,
        )

        # Merge residue: an account every source has stopped linking to is not
        # a fact, it is the leftover half of a merge. Nothing else empties an
        # account's links (they are only ever added or re-pointed), so this
        # cannot reach an account a live source still claims.
        accounts_pruned = self._warehouse.prune_unlinked_finance_accounts()

        summary = FinanceLedgerSummary(
            accounts_seen=len(plaid_accounts) + len(extractions),
            accounts_created=accounts_created,
            links_created=links_created,
            observations_upserted=len(observation_rows),
            transactions_upserted=transactions["upserted"],
            transactions_merged=transactions["merged"],
            transactions_skipped=transactions["skipped"],
            transactions_removed=transactions["removed"],
            accounts_merged=accounts_merged,
            accounts_pruned=accounts_pruned,
            links_relinked=links_relinked,
            observations_removed=observations_removed,
            security_trades_upserted=securities["upserted"],
            security_trades_merged=securities["merged"],
            security_trades_removed=securities["removed"],
            tax_lots_built=securities["lots"],
        )
        self._logger.info(
            "Finance ledger: accounts_seen=%s accounts_created=%s links_created=%s observations=%s "
            "transactions=%s merged=%s skipped=%s removed=%s accounts_merged=%s accounts_pruned=%s "
            "links_relinked=%s observations_removed=%s "
            "security_trades=%s security_merged=%s security_removed=%s tax_lots=%s",
            summary.accounts_seen,
            summary.accounts_created,
            summary.links_created,
            summary.observations_upserted,
            summary.transactions_upserted,
            summary.transactions_merged,
            summary.transactions_skipped,
            summary.transactions_removed,
            summary.accounts_merged,
            summary.accounts_pruned,
            summary.links_relinked,
            summary.observations_removed,
            summary.security_trades_upserted,
            summary.security_trades_merged,
            summary.security_trades_removed,
            summary.tax_lots_built,
        )
        return summary

    # --- security trades + tax lots ---------------------------------------------

    def _build_security_trades(
        self,
        *,
        plaid_account_map: dict[tuple[str, str], str],
        extractions: list[dict[str, Any]],
        doc_accounts: dict[str, str],
        now: datetime,
        sync_version: int,
    ) -> dict[str, int]:
        """Unify Plaid and statement share movements, then reduce them to lots.

        Plaid only reaches back 730 days; the statement corpus reaches 2018.
        Their ~20-month overlap is deduped so a trade described by both sources
        is one fact, not two — a doubled trade yields a confidently wrong lot.
        """
        resolver = SecurityResolver()
        plaid_trades: list[dict[str, Any]] = []
        for row in self._load_plaid_investment_transactions():
            owner = str(row["account"])
            account_id = plaid_account_map.get((owner, str(row["account_id"])))
            if account_id is None:
                continue
            quantity = _as_decimal(row["quantity"])
            side = plaid_trade_side(row["type"], row["subtype"], quantity)
            if side is None:
                continue
            asset_class = classify_asset_class(
                name=str(row["security_name"] or ""),
                description=str(row["name"] or ""),
                plaid_type=str(row["security_type"] or ""),
            )
            if asset_class == ASSET_CLASS_OPTION:
                # Plaid names the security after the UNDERLYING ("IonQ Inc")
                # and puts the strike only in the transaction text, so the
                # contract identity has to be read out of that text.
                identity = option_identity(
                    ticker=str(row["ticker_symbol"]), text=str(row["name"] or "")
                )
            else:
                identity = SecurityIdentity(
                    ticker=str(row["ticker_symbol"]),
                    cusip="",
                    name=str(row["security_name"] or row["name"]),
                )
            if identity.is_empty or quantity == 0:
                continue
            plaid_trades.append(
                {
                    "account_id": account_id,
                    "identity": identity,
                    "trade_date": _as_date(row["transaction_at"]),
                    "side": side,
                    "quantity": abs(quantity),
                    "price": _as_decimal(row["price"]) or None,
                    "amount": abs(_as_decimal(row["amount"])),
                    "fees": _as_decimal(row["fees"]),
                    "currency": str(row["iso_currency_code"] or ""),
                    "source": LEDGER_SOURCE_PLAID,
                    "source_row_key": f"{owner}|investment|{row['investment_transaction_id']}",
                }
            )

        document_trades: list[dict[str, Any]] = []
        for extraction in sorted(extractions, key=lambda e: str(e["content_sha256"])):
            sha = str(extraction["content_sha256"])
            account_id = doc_accounts.get(sha)
            if account_id is None:
                continue
            currency = str(extraction["currency"])
            for index, entry in enumerate(extraction["transactions_json"] or []):
                trade = document_trade_entry(entry)
                if trade is None:
                    continue
                trade_date = _parse_iso_date(str(entry.get("date", "")))
                if trade_date is None:
                    continue
                document_trades.append(
                    {
                        "account_id": account_id,
                        "identity": trade["identity"],
                        "trade_date": trade_date,
                        "side": trade["side"],
                        "quantity": trade["quantity"],
                        "price": trade["price"],
                        "price_is_derived": trade["price_is_derived"],
                        "amount": trade["amount"],
                        "fees": trade["fees"],
                        "currency": currency,
                        "source": LEDGER_SOURCE_MANUAL,
                        "source_row_key": f"{sha}|{index}",
                    }
                )

        trade_rows, link_rows, merged = dedupe_security_trades(
            plaid_trades, document_trades, resolver=resolver
        )
        # Warehouse columns are NOT NULL with defaults (the house convention),
        # so absence is written as a sentinel and restored to NULL by the marts
        # views — never as a bare 0 a reader could mistake for a real price.
        for row in trade_rows:
            row["price"] = row["price"] if row["price"] is not None else Decimal("0")
            row["amount"] = row["amount"] if row["amount"] is not None else Decimal("0")
            row["created_at"] = now
            row["sync_version"] = sync_version
        for row in link_rows:
            row["created_at"] = now
            row["sync_version"] = sync_version

        self._warehouse.insert_finance_security_transactions(trade_rows)
        self._warehouse.insert_finance_security_transaction_links(link_rows)
        removed = self._warehouse.delete_missing_finance_security_transactions(
            [str(row["transaction_id"]) for row in trade_rows]
        )

        lots = build_tax_lots(
            [
                {**row, "source_row_key": row["transaction_id"]}
                for row in trade_rows
            ],
            as_of=_as_date(now),
        )
        for lot in lots:
            lot["basis_known"] = 1 if lot["basis_known"] else 0
            lot["acquired_on"] = lot["acquired_on"] or _SENTINEL_DATE
            lot["disposed_on"] = lot["disposed_on"] or _SENTINEL_DATE
            for money in ("cost_per_unit", "cost_basis", "cost_basis_remaining", "realized_gain"):
                if lot[money] is None:
                    lot[money] = Decimal("0")
            lot["created_at"] = now
            lot["sync_version"] = sync_version
        self._warehouse.replace_finance_tax_lots(lots)

        return {
            "upserted": len(trade_rows),
            "merged": merged,
            "removed": removed,
            "lots": len(lots),
        }

    # --- plaid account identity -------------------------------------------------

    def _resolve_plaid_accounts(
        self,
        rows: list[dict[str, Any]],
        *,
        links: dict[tuple[str, str], str],
        index: list[dict[str, Any]],
    ) -> dict[tuple[str, str], tuple[str, str]]:
        """Resolve each live plaid account to a logical account.

        Plaid account ids are **item-scoped**: re-linking an institution mints
        a new item_id AND new account_ids for the same real accounts. Keying
        ledger identity on the plaid id alone therefore forks every account on
        every re-link and double-counts net worth, which is exactly what a
        re-linked card issuer did in production. So a plaid account resolves the
        way a statement document already does — by institution+mask — and only
        founds a new logical account when nothing matches.

        Returns ``{(owner, plaid_account_id): (account_id, match_method)}``;
        an empty match_method means the existing link already said this and
        needs no new audit row.
        """

        by_id = {str(entry["account_id"]): entry for entry in index}
        ordered = sorted(rows, key=lambda row: (str(row["account"]), str(row["account_id"])))
        resolved: dict[tuple[str, str], tuple[str, str]] = {}
        # One logical account per live source account: claiming keeps two live
        # plaid accounts (a re-link the operator has not retired yet) from
        # racing each other's daily balance observation.
        claimed: set[str] = set()

        # An existing link is a decision already made; it wins.
        for row in ordered:
            key = (str(row["account"]), str(row["account_id"]))
            linked = links.get(key)
            if linked is not None:
                resolved[key] = (linked, "")
                claimed.add(linked)

        for row in ordered:
            key = (str(row["account"]), str(row["account_id"]))
            _, side = plaid_account_kind_side(str(row["type"]), str(row["subtype"]))
            candidate = _best_identity_match(
                index,
                owner=str(row["account"]),
                institution=str(row["institution_name"]),
                mask=str(row["mask"]),
                side=side,
                exclude=claimed,
            )
            current = resolved.get(key, (None, ""))[0]
            if current is None:
                account_id = candidate or stable_finance_account_id(
                    LEDGER_SOURCE_PLAID, str(row["account"]), str(row["account_id"])
                )
                resolved[key] = (account_id, "institution_mask" if candidate else "source_id")
                claimed.add(account_id)
                continue
            # A fork left over from a re-link: this account's link points at a
            # duplicate of an older account nothing live claims. Re-point it.
            # Strictly older only — on a tie there is no evidence which one is
            # the established account, so keep both rather than guess.
            if candidate is not None and _created_before(by_id.get(candidate), by_id.get(current)):
                resolved[key] = (candidate, "institution_mask")
                claimed.discard(current)
                claimed.add(candidate)
        return resolved

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

        # Investment-product flows: brokerage accounts (cash-management
        # brokerages et al.) report ALL their activity — deposits,
        # withdrawals, interest, dividends, and trades — through the
        # investments product, never the transactions product, so these ARE
        # those accounts' flow facts. Same positive-out sign convention as
        # transactions. The two Plaid feeds are assumed disjoint per account
        # (an account's movements arrive via exactly one feed); there is no
        # cross-feed dedup.
        for row in self._load_plaid_investment_transactions():
            owner = str(row["account"])
            source_row_key = f"{owner}|investment|{row['investment_transaction_id']}"
            account_id = plaid_account_map.get((owner, str(row["account_id"])))
            if account_id is None:
                skipped += 1
                continue
            transaction_id = stable_finance_transaction_id(LEDGER_SOURCE_PLAID, source_row_key)
            amount = -_as_decimal(row["amount"])
            transaction_rows[transaction_id] = {
                "transaction_id": transaction_id,
                "account_id": account_id,
                "posted_at": row["transaction_at"],
                "amount": amount,
                "currency": str(row["iso_currency_code"]),
                "description": str(row["name"]),
                "merchant": "",
                "pending": 0,
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
                    "posted_on": _as_date(row["transaction_at"]),
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

        # The ledger is replayed from all source rows, but a replay is not a
        # new write. Filter against the current derived facts before the
        # upsert so 17k unchanged transactions and 18k resolution links do not
        # acquire a new timestamp/version every run. A semantic source change
        # still writes the row with ``created_at = now`` so timeline's
        # watermark sees it.
        existing_transactions = self._load_existing_transactions()
        changed_transactions = [
            row
            for row in transaction_rows.values()
            if not _row_matches(
                existing_transactions.get(str(row["transaction_id"])),
                row,
                _TRANSACTION_SEMANTIC_COLUMNS,
            )
        ]
        desired_links = {
            (str(row["source"]), str(row["source_row_key"])): row for row in link_rows
        }
        existing_links = self._load_existing_transaction_links()
        changed_links = [
            row
            for key, row in desired_links.items()
            if not _row_matches(
                existing_links.get(key), row, _TRANSACTION_LINK_SEMANTIC_COLUMNS
            )
        ]
        self._warehouse.insert_finance_transactions(changed_transactions)
        self._warehouse.insert_finance_transaction_links(changed_links)
        removed = self._warehouse.reconcile_finance_transactions(
            transaction_ids=list(transaction_rows.keys()),
            link_keys=[f"{source}|{source_row_key}" for source, source_row_key in desired_links],
        )
        return {
            "upserted": len(changed_transactions),
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

    def _resolve_document_account_group(
        self, group: list[dict[str, Any]], *, account_index: list[dict[str, Any]]
    ) -> tuple[str, str, float]:
        """Match a document group to an existing ledger account by any of the
        masks its documents report (most common first, institution tiebreak).
        Returns (account_id, match_method, match_score); method 'new' means
        the caller must create the account."""
        for mask in _group_masks_by_frequency(group):
            institutions = {
                str(extraction["institution"]).strip().lower()
                for extraction in group
                if str(extraction["account_mask"]).strip() == mask
            }
            matches = [entry for entry in account_index if str(entry.get("mask", "")).strip() == mask]
            if len(matches) > 1 and institutions:
                narrowed = [
                    entry
                    for entry in matches
                    if any(
                        institution
                        and (
                            institution in str(entry.get("institution", "")).lower()
                            or str(entry.get("institution", "")).lower() in institution
                        )
                        for institution in institutions
                    )
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
            for entry in extraction["balances_json"] or []:
                as_of = _parse_iso_date(str(entry.get("date", "")))
                value = _parse_money(str(entry.get("balance", "")))
                if as_of is None or value is None:
                    continue
                rows[(account_id, as_of, balance_kind)] = {
                    "account_id": account_id,
                    "as_of": as_of,
                    "kind": balance_kind,
                    "value": value,
                    "currency": currency,
                    "source": LEDGER_SOURCE_MANUAL,
                    "observed_at": now,
                    "sync_version": sync_version,
                }
            # A valuation document may report several positions for one day
            # (e.g. a fund export listing every entity plus a totals row, all
            # attributed to the folder's account): the account's value for the
            # day is the explicit total when one exists, else the sum of the
            # parts.
            for (as_of, value) in _daily_valuations(extraction["valuations_json"] or []):
                rows[(account_id, as_of, OBSERVATION_KIND_VALUATION)] = {
                    "account_id": account_id,
                    "as_of": as_of,
                    "kind": OBSERVATION_KIND_VALUATION,
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
            SELECT a.account, a.item_id, a.account_id, a.name, a.official_name,
                   a.mask, a.type, a.subtype, a.current_balance, a.iso_currency_code,
                   COALESCE(i.institution_name, '') AS institution_name
            FROM @plaid_accounts a
            LEFT JOIN @plaid_items i
              ON i.account = a.account AND i.item_id = a.item_id
            WHERE a.is_removed = 0
            ORDER BY a.account, a.account_id
            """
        )

    def _load_action_required_item_ids(self) -> set[tuple[str, str]]:
        rows = self._warehouse._query(
            """
            SELECT DISTINCT account, item_id
            FROM @plaid_sync_state
            WHERE status = 'action_required'
            """
        )
        return {(str(row[0]), str(row[1])) for row in rows}

    def _load_plaid_transactions(self) -> list[dict[str, Any]]:
        return self._warehouse._query_dicts(
            """
            SELECT account, account_id, transaction_id, posted_at, name,
                   merchant_name, amount, iso_currency_code, pending,
                   pending_transaction_id
            FROM @plaid_transactions
            WHERE is_removed = 0
            ORDER BY posted_at, transaction_id
            """
        )

    def _load_plaid_investment_transactions(self) -> list[dict[str, Any]]:
        # security_id/quantity/price/type/subtype feed the SECURITY ledger; the
        # cash ledger only needs the amount. Joined to the security so a trade
        # carries the ticker Plaid knows it by (Plaid reports no CUSIP).
        return self._warehouse._query_dicts(
            """
            SELECT t.account, t.account_id, t.investment_transaction_id,
                   t.transaction_at, t.name, t.amount, t.iso_currency_code,
                   t.security_id, t.quantity, t.price, t.fees, t.type, t.subtype,
                   COALESCE(s.ticker_symbol, '') AS ticker_symbol,
                   COALESCE(s.name, '') AS security_name,
                   COALESCE(s.type, '') AS security_type
            FROM @plaid_investment_transactions AS t
            LEFT JOIN @plaid_investment_securities AS s
              ON s.account = t.account AND s.security_id = t.security_id
            ORDER BY t.transaction_at, t.investment_transaction_id
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
                   e.positions_json, e.period_end,
                   d.account, d.original_path, d.filename
            FROM @manual_finance_extractions e
            JOIN @manual_finance_documents d
              ON d.content_sha256 = e.content_sha256 AND d.is_deleted = 0
            WHERE e.status = 'ok'
            ORDER BY e.content_sha256, e.created_at DESC
            """
        )

    def _changed_observation_rows(
        self, rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        existing = {
            (str(row["account_id"]), row["as_of"], str(row["kind"]), str(row["source"])): row
            for row in self._warehouse._query_dicts(
                """
                SELECT account_id, as_of, kind, value, currency, source
                FROM @finance_observations
                WHERE source = %s
                """,
                (LEDGER_SOURCE_MANUAL,),
            )
        }
        changed: list[dict[str, Any]] = []
        for row in rows:
            if str(row["source"]) != LEDGER_SOURCE_MANUAL:
                changed.append(row)
                continue
            key = (
                str(row["account_id"]),
                row["as_of"],
                str(row["kind"]),
                str(row["source"]),
            )
            if not _row_matches(existing.get(key), row, _OBSERVATION_SEMANTIC_COLUMNS):
                changed.append(row)
        return changed

    def _load_existing_transactions(self) -> dict[str, dict[str, Any]]:
        rows = self._warehouse._query_dicts(
            """
            SELECT transaction_id, account_id, posted_at, amount, currency,
                   description, merchant, pending, source
            FROM @finance_transactions
            """
        )
        return {str(row["transaction_id"]): row for row in rows}

    def _load_existing_transaction_links(
        self,
    ) -> dict[tuple[str, str], dict[str, Any]]:
        rows = self._warehouse._query_dicts(
            """
            SELECT source, source_row_key, transaction_id, match_method, match_score
            FROM @finance_transaction_links
            """
        )
        return {(str(row["source"]), str(row["source_row_key"])): row for row in rows}

    def _load_links(self, source: str) -> dict[tuple[str, str], str]:
        rows = self._warehouse._query(
            """
            SELECT account, source_account_key, account_id
            FROM @finance_account_links
            WHERE source = %s
            """,
            (source,),
        )
        return {(str(account), str(key)): str(account_id) for account, key, account_id in rows}

    def _load_account_created_at(self) -> dict[str, datetime]:
        rows = self._warehouse._query("SELECT account_id, created_at FROM @finance_accounts")
        return {str(account_id): created_at for account_id, created_at in rows}

    def _load_account_index(self) -> list[dict[str, Any]]:
        return self._warehouse._query_dicts(
            "SELECT account_id, account, mask, institution, kind, side, created_at FROM @finance_accounts"
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


def _institution_matches(left: str, right: str) -> bool:
    """Loose institution equality: agent-extracted and Plaid-reported names for
    one institution differ in decoration ("Example Bank" / "Example Bank NA")."""
    left = left.strip().lower()
    right = right.strip().lower()
    if not left or not right:
        return False
    return left == right or left in right or right in left


def _best_identity_match(
    index: list[dict[str, Any]],
    *,
    owner: str,
    institution: str,
    mask: str,
    side: str,
    exclude: set[str],
) -> str | None:
    """The oldest existing logical account that is the same real account.

    Identity evidence is owner + institution + account mask + side. Without a
    mask or an institution there is no evidence at all (Venmo and the
    valuation-founded accounts report neither), so those never merge.
    """

    if not mask.strip() or not institution.strip():
        return None
    matches = [
        entry
        for entry in index
        if str(entry.get("account_id", "")) not in exclude
        and str(entry.get("account", "")) == owner
        and str(entry.get("mask", "")).strip() == mask.strip()
        and str(entry.get("side", "")) == side
        and _institution_matches(institution, str(entry.get("institution", "")))
    ]
    if not matches:
        return None
    matches.sort(key=lambda entry: (entry["created_at"], str(entry["account_id"])))
    return str(matches[0]["account_id"])


def _created_before(candidate: dict[str, Any] | None, current: dict[str, Any] | None) -> bool:
    if candidate is None:
        return False
    if current is None:
        return True
    return candidate["created_at"] < current["created_at"]


def has_pending_finance_observations(warehouse: PostgresWarehouse) -> bool:
    """True when a live plaid account is missing its link or today's observation."""
    rows = warehouse._query(
        """
        SELECT 1
        FROM @plaid_accounts a
        LEFT JOIN @finance_account_links l
          ON l.source = %s AND l.account = a.account AND l.source_account_key = a.account_id
        LEFT JOIN @finance_observations o
          ON o.account_id = l.account_id
         AND o.source = %s
         AND o.kind = %s
         AND o.as_of = CURRENT_DATE
        WHERE a.is_removed = 0
          AND NOT EXISTS (
              SELECT 1
              FROM @plaid_sync_state s
              WHERE s.account = a.account
                AND s.item_id = a.item_id
                AND s.status = 'action_required'
          )
          AND (l.account_id IS NULL OR o.account_id IS NULL)
        LIMIT 1
        """,
        (LEDGER_SOURCE_PLAID, LEDGER_SOURCE_PLAID, OBSERVATION_KIND_BALANCE),
    )
    return bool(rows)


_OBSERVATION_SEMANTIC_COLUMNS = ("value", "currency")
_TRANSACTION_SEMANTIC_COLUMNS = (
    "account_id",
    "posted_at",
    "amount",
    "currency",
    "description",
    "merchant",
    "pending",
    "source",
)
_TRANSACTION_LINK_SEMANTIC_COLUMNS = (
    "transaction_id",
    "match_method",
    "match_score",
)


def _row_matches(
    existing: dict[str, Any] | None,
    desired: dict[str, Any],
    columns: tuple[str, ...],
) -> bool:
    return existing is not None and all(existing.get(column) == desired.get(column) for column in columns)


def _group_masks_by_frequency(group: list[dict[str, Any]]) -> list[str]:
    counts: dict[str, int] = {}
    for extraction in group:
        mask = str(extraction["account_mask"]).strip()
        if mask:
            counts[mask] = counts.get(mask, 0) + 1
    return sorted(counts, key=lambda mask: (-counts[mask], mask))


def _group_primary_mask(group: list[dict[str, Any]]) -> str:
    masks = _group_masks_by_frequency(group)
    return masks[0] if masks else ""


def _daily_valuations(entries: list[Any]) -> list[tuple[date, Decimal]]:
    """Collapse a document's valuation entries to one value per day.

    An entry described as a total wins (a fund export listing every entity
    plus a totals row). Otherwise the FIRST entry of the day wins: valuation
    documents usually restate the same asset several ways (point estimate,
    low/high bounds, rental estimates, assessed-value variants) with the
    primary figure listed first, and summing alternative measures of one
    asset inflates it catastrophically. A parts-only multi-entity document
    without a totals row undercounts to its first position — a visible,
    benign failure the extraction's totals coverage makes rare.
    """
    by_day: dict[date, dict[str, Any]] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        as_of = _parse_iso_date(str(entry.get("date", "")))
        value = _parse_money(str(entry.get("value", "")))
        if as_of is None or value is None:
            continue
        # Negative "valuations" are deltas, not asset values (an Edmunds
        # appraisal's "total depreciation over five years: -11,330" both went
        # negative AND matched the totals heuristic). Assets are worth >= 0.
        if value < 0:
            continue
        day = by_day.setdefault(as_of, {"first": value, "total": None})
        is_total = "total" in str(entry.get("description", "")).lower()
        if is_total and day["total"] is None:
            day["total"] = value
    return [
        (as_of, day["total"] if day["total"] is not None else day["first"])
        for as_of, day in sorted(by_day.items())
    ]


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
