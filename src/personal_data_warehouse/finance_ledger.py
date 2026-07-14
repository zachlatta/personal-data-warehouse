"""Finance ledger: the derived stocks-and-flows layer over the finance sources.

Every finance source is a witness to one of two fact types: a **flow** (money
moved: a transaction) or a **stock** (something was worth X at time T: a
balance, valuation, or principal). This runner resolves raw source rows into
logical `finance.accounts` (via `finance.account_links`, the photos-identity
pattern: raw rows never learn about identity) and appends one observation per
account per day into `finance.observations`. Net worth is the latest
observation per account summed by side — read it through
`marts.finance_net_worth` / `marts.finance_net_worth_history`.

The ledger stores facts only. Categories and other opinions belong to future
enrichment layers, never to these tables.

Replayability contract: this runner never mutates raw source rows. Deleting
every `finance.*` row and re-running rebuilds the same accounts (ids are
deterministic from source provenance) — pinned by tests/test_finance_ledger.py.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from personal_data_warehouse.postgres import PostgresWarehouse

LEDGER_SOURCE_PLAID = "plaid"

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

_SAVINGS_SUBTYPES = {"savings", "hsa", "cd", "money market"}
_IRA_SUBTYPES = {"ira", "roth", "roth ira", "401k", "403b", "457b", "sep ira", "simple ira", "pension"}
_INVESTMENT_TYPES = {"investment", "brokerage"}


def stable_finance_account_id(source: str, account: str, source_account_key: str) -> str:
    """Deterministic ledger account id from the founding source row's provenance."""
    digest = hashlib.sha256(f"{source}|{account}|{source_account_key}".encode()).hexdigest()
    return f"fa_{digest[:24]}"


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


@dataclass(frozen=True)
class FinanceLedgerSummary:
    accounts_seen: int
    accounts_created: int
    links_created: int
    observations_upserted: int


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
        now = self._now or datetime.now(tz=UTC)
        sync_version = int(now.timestamp() * 1_000_000)

        source_accounts = self._load_plaid_accounts()
        existing_links = self._load_links(LEDGER_SOURCE_PLAID)
        existing_created_at = self._load_account_created_at()

        account_rows: list[dict[str, Any]] = []
        link_rows: list[dict[str, Any]] = []
        observation_rows: list[dict[str, Any]] = []
        accounts_created = 0
        links_created = 0

        for row in source_accounts:
            link_key = (row["account"], row["account_id"])
            account_id = existing_links.get(link_key)
            if account_id is None:
                account_id = stable_finance_account_id(
                    LEDGER_SOURCE_PLAID, row["account"], row["account_id"]
                )
                link_rows.append(
                    {
                        "source": LEDGER_SOURCE_PLAID,
                        "account": row["account"],
                        "source_account_key": row["account_id"],
                        "account_id": account_id,
                        "match_method": "source_id",
                        "match_score": 1.0,
                        "created_at": now,
                        "sync_version": sync_version,
                    }
                )
                links_created += 1

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
        self._warehouse.insert_finance_observations(observation_rows)

        summary = FinanceLedgerSummary(
            accounts_seen=len(source_accounts),
            accounts_created=accounts_created,
            links_created=links_created,
            observations_upserted=len(observation_rows),
        )
        self._logger.info(
            "Finance ledger: seen=%s accounts_created=%s links_created=%s observations=%s",
            summary.accounts_seen,
            summary.accounts_created,
            summary.links_created,
            summary.observations_upserted,
        )
        return summary

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
