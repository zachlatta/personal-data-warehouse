from __future__ import annotations

import argparse
from datetime import UTC, datetime
import os
from pathlib import Path

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.warehouse import warehouse_from_settings

DEFAULT_OUTPUT = Path("reports/plaid-linking-report.private.md")


def load_safe_status_rows() -> tuple[list[tuple], list[tuple]]:
    settings = load_settings(require_gmail=False, require_plaid=True)
    warehouse = warehouse_from_settings(settings)
    try:
        accounts = warehouse._query(
            """
            WITH tx AS (
                SELECT account, item_id, account_id, count(*) FILTER (WHERE is_removed = 0) AS n
                FROM plaid.transactions GROUP BY account, item_id, account_id
            ), holdings AS (
                SELECT account, item_id, account_id, count(*) AS n
                FROM plaid.investment_holdings GROUP BY account, item_id, account_id
            ), inv_tx AS (
                SELECT account, item_id, account_id, count(*) AS n
                FROM plaid.investment_transactions GROUP BY account, item_id, account_id
            ), liabilities AS (
                SELECT account, item_id, account_id, count(*) AS n
                FROM plaid.liabilities GROUP BY account, item_id, account_id
            ), status AS (
                SELECT
                    i.institution_name,
                    a.account_id,
                    a.type,
                    a.subtype,
                    (a.raw_json ? 'balances') AS balance_present,
                    COALESCE(tx.n, 0) > 0 AS transactions_present,
                    (COALESCE(holdings.n, 0) > 0 OR COALESCE(inv_tx.n, 0) > 0) AS investments_present,
                    COALESCE(liabilities.n, 0) > 0 AS liabilities_present,
                    a.synced_at
                FROM plaid.accounts AS a
                JOIN plaid.items AS i USING (account, item_id)
                LEFT JOIN tx USING (account, item_id, account_id)
                LEFT JOIN holdings USING (account, item_id, account_id)
                LEFT JOIN inv_tx USING (account, item_id, account_id)
                LEFT JOIN liabilities USING (account, item_id, account_id)
                WHERE a.is_removed = 0
            )
            SELECT
                institution_name,
                row_number() OVER (PARTITION BY institution_name ORDER BY account_id),
                type,
                subtype,
                balance_present,
                transactions_present,
                investments_present,
                liabilities_present,
                synced_at
            FROM status
            ORDER BY institution_name, 2
            """
        )
        products = warehouse._query(
            """
            SELECT i.institution_name, s.product, s.status, s.last_synced_at
            FROM plaid.sync_state AS s
            JOIN plaid.items AS i USING (account, item_id)
            ORDER BY i.institution_name, s.product
            """
        )
        return accounts, products
    finally:
        warehouse.close()


def render_report(accounts: list[tuple], products: list[tuple]) -> str:
    lines = [
        "# Private Plaid Linking Report",
        "",
        f"Generated: {datetime.now(tz=UTC).isoformat()}",
        "",
        "This local, gitignored report intentionally omits account IDs, names, masks, balances,",
        "transaction descriptions/amounts, security positions/values, and access tokens.",
        "`present` means Plaid returned at least one applicable row; `none returned` is not a failure",
        "when that product does not apply to the account.",
        "",
        "## Institution/product status",
        "",
        "| Institution | Product | Status | Last successful pull |",
        "|---|---|---|---|",
    ]
    for institution, product, status, last_pull in products:
        lines.append(f"| {institution} | {product} | {status} | {last_pull.isoformat()} |")
    lines += [
        "",
        "## Per-account status",
        "",
        "| Institution/account | Type/subtype | Link/sync status | Balance payload | Transactions | Investments | Liabilities | Last account pull |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for row in accounts:
        institution, ordinal, account_type, subtype = row[:4]
        balance, transactions, investments, liabilities, synced_at = row[4:]
        lines.append(
            f"| {institution} / account {ordinal} | {account_type}/{subtype} | linked; synced | "
            f"{'present' if balance else 'none returned'} | "
            f"{'present' if transactions else 'none returned'} | "
            f"{'present' if investments else 'none returned'} | "
            f"{'present' if liabilities else 'none returned'} | {synced_at.isoformat()} |"
        )
    lines += [
        "",
        f"Total linked institutions: {len({row[0] for row in accounts})}",
        f"Total active accounts: {len(accounts)}",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Write an aggregate-safe private Plaid linking report.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    accounts, products = load_safe_status_rows()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as output:
        output.write(render_report(accounts, products))
    args.output.chmod(0o600)
    print(f"Wrote {len(accounts)} account statuses and {len(products)} product statuses to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
