"""Replay the LIVE finance corpus through the ledger into a throwaway schema.

``verify_securities_ledger_e2e.py`` replays local extraction payloads, which
proves the runner reads a document correctly. This script answers the other
question: what would the ledger look like if every decision it has ever stored
were made again, right now, by the current code?

That difference is the whole point. ``derived_finance.account_links`` is
derived state, but a stored link used to be consulted before resolution and
never revisited, so a link founded on thin evidence froze — Robinhood's crypto
folder pointed at the BROKERAGE account for six weeks because the one
statement extracted at the time printed the brokerage number in its header,
and the crypto trades sat where nothing could dedupe them against the Plaid
rows describing the same trades. A replay shows that immediately; production
cannot, because production is the thing carrying the frozen decision.

    uv run python scripts/verify_finance_ledger_replay.py
    uv run python scripts/verify_finance_ledger_replay.py --ticker BTC --ticker ETH

Reads the live warehouse read-only through the ``pdw`` CLI, writes only into a
``pdw_test_*`` schema, and drops it again unless ``--retain-schema`` is passed.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
from datetime import UTC, datetime
from typing import Any

from dotenv import load_dotenv

from personal_data_warehouse.finance_ledger import FinanceLedgerRunner
from personal_data_warehouse.postgres import POSTGRES_TABLES, PostgresWarehouse

# Logical table -> the live relation to copy it from. Only the inputs the
# ledger reads; everything under derived_finance is what we are rebuilding.
COPY_TABLES = {
    "plaid_items": "base_plaid.items",
    "plaid_accounts": "base_plaid.accounts",
    "plaid_transactions": "base_plaid.transactions",
    "plaid_investment_securities": "base_plaid.investment_securities",
    "plaid_investment_holdings": "base_plaid.investment_holdings",
    "plaid_investment_transactions": "base_plaid.investment_transactions",
    "manual_finance_documents": "base_manual_finance.documents",
    "manual_finance_extractions": "derived_finance.document_extractions",
}
# ``ops.plaid_sync_state`` is deliberately absent: the read-only query role
# cannot see it, so a replay treats every Plaid item as healthy and records the
# daily balance observations a dead item would be skipped for. That affects
# balances only, never trade or lot reconstruction.
#
# Multi-megabyte provenance blobs the ledger never reads. Copying them turns a
# 10-second replay into a timeout for no gain.
DROPPED_COLUMNS = {"raw_json", "raw_result_json", "raw_metadata_json", "error_json"}


def pdw_json(sql: str) -> list[dict[str, Any]]:
    completed = subprocess.run(
        ["pdw", "sql", "--output", "json", "-q", "finance ledger replay verification copy", sql],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(completed.stdout or "[]")


def coerce(value: Any) -> Any:
    if isinstance(value, str) and "T" in value and value.endswith("Z") and len(value) > 18:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return value
    return value


def copy_inputs(warehouse: PostgresWarehouse) -> dict[str, int]:
    counts: dict[str, int] = {}
    for logical, relation in COPY_TABLES.items():
        table_columns = POSTGRES_TABLES[logical].columns
        columns = [column for column in table_columns if column not in DROPPED_COLUMNS]
        select = ", ".join(columns)
        rows = pdw_json(f"SELECT {select} FROM {relation}")
        typed = []
        for row in rows:
            typed_row = {key: coerce(value) for key, value in row.items()}
            for column in DROPPED_COLUMNS & set(table_columns):
                typed_row[column] = {}
            for key, value in list(typed_row.items()):
                if key.endswith("_json") and isinstance(value, str):
                    typed_row[key] = json.loads(value) if value else {}
            typed.append(typed_row)
        getattr(warehouse, f"insert_{logical}")(typed)
        counts[relation] = len(typed)
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ticker",
        action="append",
        default=[],
        help="report lots for this ticker specifically (repeatable)",
    )
    parser.add_argument("--retain-schema", action="store_true")
    args = parser.parse_args()

    load_dotenv()
    url = os.environ["POSTGRES_DATABASE_URL"]
    schema = f"pdw_test_{datetime.now(tz=UTC):%Y%m%d%H%M%S}_ledger_replay"
    warehouse = PostgresWarehouse(url, schema=schema)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        warehouse.ensure_plaid_tables()
        warehouse.ensure_finance_tables()
        warehouse.ensure_manual_finance_tables()
        print("copied:", copy_inputs(warehouse), flush=True)

        summary = FinanceLedgerRunner(warehouse=warehouse).sync()
        print("summary:", summary, flush=True)

        print("\n-- document account links (replayed) --")
        for row in warehouse._query(
            "SELECT source_account_key, account_id, match_method FROM @finance_account_links "
            "WHERE source = 'manual_finance' ORDER BY source_account_key"
        ):
            print(" ", *row)

        print("\n-- coverage that is not complete --")
        for row in warehouse._query(
            "SELECT account_name, ticker, coverage_status, quantity_held, quantity_with_lots "
            "FROM @marts_finance_position_coverage WHERE coverage_status <> 'complete' "
            "ORDER BY coverage_status, ticker"
        ):
            print(" ", *row)

        if args.ticker:
            print("\n-- open lots for the requested tickers --")
            for row in warehouse._query(
                "SELECT account_name, ticker, count(*), sum(quantity_remaining), "
                "round(sum(cost_basis_remaining)::numeric, 2) "
                "FROM @marts_finance_tax_lots WHERE status = 'open' AND ticker = ANY(%s) "
                "GROUP BY 1, 2 ORDER BY 2, 1",
                ([t.upper() for t in args.ticker],),
            ):
                print(" ", *row)
        return 0
    finally:
        if not args.retain_schema:
            for name in [
                *warehouse.physical_schema_names(include_hidden=True),
                warehouse.schema_namespace,
            ]:
                warehouse._raw_command(f'DROP SCHEMA IF EXISTS "{name}" CASCADE')
        else:
            print(f"\nschema retained: {schema}")
        warehouse.close()


if __name__ == "__main__":
    raise SystemExit(main())
