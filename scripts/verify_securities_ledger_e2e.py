"""End-to-end check of the securities ledger against REAL data.

Builds a throwaway schema, loads the real Plaid investment feed (read-only
copy from the live warehouse) plus real v2 extraction payloads produced by
scripts/verify_manual_finance_extraction_v2.py, runs the production
FinanceLedgerRunner over both, and reports the lots it reconstructs.

The point is grounding, not coverage: every number printed here should be
checkable against the source PDF or the Plaid row it came from.

    uv run python scripts/verify_securities_ledger_e2e.py /tmp/extraction-v2/*.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from personal_data_warehouse.finance_ledger import FinanceLedgerRunner
from personal_data_warehouse.postgres import PostgresWarehouse

PLAID_COPY_TABLES = {
    "plaid_items": "base_plaid.items",
    "plaid_accounts": "base_plaid.accounts",
    "plaid_investment_securities": "base_plaid.investment_securities",
    "plaid_investment_holdings": "base_plaid.investment_holdings",
    "plaid_investment_transactions": "base_plaid.investment_transactions",
}

# The ledger resolves a document's account from original_path's first segment.
# Payloads use their own institution+mask as that folder hint, so this file
# carries no private account list.
def pdw_json(sql: str) -> list[dict[str, Any]]:
    completed = subprocess.run(
        ["pdw", "sql", "--output", "json", "-q", "securities ledger e2e verification copy", sql],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(completed.stdout or "[]")


def coerce(value: Any, column: str) -> Any:
    if value is None:
        return None
    if isinstance(value, str) and value.endswith("Z") and "T" in value and len(value) > 18:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return value
    return value


def copy_plaid(warehouse: PostgresWarehouse) -> dict[str, int]:
    counts: dict[str, int] = {}
    for logical, relation in PLAID_COPY_TABLES.items():
        rows = pdw_json(f"SELECT * FROM {relation} LIMIT 100000")
        typed = [{k: coerce(v, k) for k, v in row.items()} for row in rows]
        for row in typed:
            for key, value in list(row.items()):
                if key.endswith("_json") and isinstance(value, str):
                    row[key] = json.loads(value) if value else {}
        getattr(warehouse, f"insert_{logical}")(typed)
        counts[logical] = len(typed)
    return counts


def document_rows(
    payload_paths: list[Path], *, folder_by_stem: dict[str, str] | None = None
) -> tuple[list[dict], list[dict]]:
    documents: list[dict] = []
    extractions: list[dict] = []
    now = datetime(2026, 8, 14, 12, tzinfo=UTC)
    for path in payload_paths:
        payload = json.loads(path.read_text())
        stem = path.stem
        folder = _mapped_folder(stem, folder_by_stem or {}) or _folder_from_payload(payload)
        sha = f"sha-{stem}"
        documents.append(
            {
                "source": "manual",
                "account": "owner@example.test",
                "source_native_id": sha,
                "filename": f"{stem}.pdf",
                "original_path": f"{folder}/{stem}.pdf",
                "mime_type": "application/pdf",
                "size_bytes": 1,
                "content_sha256": sha,
                "file_modified_at": now,
                "raw_metadata_json": {},
                "storage_backend": "google_drive",
                "storage_key": "",
                "storage_file_id": f"drive-{stem}",
                "storage_url": "",
                "metadata_storage_key": "",
                "metadata_storage_file_id": "",
                "metadata_storage_url": "",
                "metadata_content_sha256": "",
                "is_deleted": 0,
                "ingested_at": now,
                "sync_version": 1,
            }
        )
        extractions.append(
            {
                "content_sha256": sha,
                "ai_provider": "agent_codex",
                "ai_model": "gpt-5.6-sol",
                "ai_prompt_version": "manual-finance-agent-v2",
                "status": "ok",
                "error": "",
                "document_type": payload.get("document_type", ""),
                "institution": payload.get("institution", ""),
                "account_name_hint": payload.get("account_name_hint", ""),
                "account_mask": payload.get("account_mask", ""),
                "period_start": _as_date(payload.get("period_start")),
                "period_end": _as_date(payload.get("period_end")),
                "currency": payload.get("currency", "USD"),
                "closing_balance": Decimal(str(payload.get("closing_balance") or "0").replace(",", "")),
                "transactions_json": payload.get("transactions", []),
                "balances_json": payload.get("balances", []),
                "valuations_json": payload.get("valuations", []),
                "positions_json": payload.get("positions", []),
                "summary": payload.get("summary", ""),
                "uncertainties_json": payload.get("uncertainties", []),
                "raw_result_json": payload,
                "ai_elapsed_ms": 0,
                "ai_processed_at": now,
                "created_at": now,
                "sync_version": 1,
            }
        )
    return documents, extractions


def _folder_from_payload(payload: dict[str, Any]) -> str:
    """Account-folder hint derived from what the document itself says."""
    institution = str(payload.get("institution", "")).strip().lower().replace(" ", "-")
    mask = str(payload.get("account_mask", "")).strip()
    return "-".join(part for part in (institution, mask) if part) or "unknown-account"


def _mapped_folder(stem: str, folder_by_stem: dict[str, str]) -> str:
    """Exact or longest-prefix mapping, so one private local flag can cover a series."""
    matches = [
        (prefix, folder)
        for prefix, folder in folder_by_stem.items()
        if stem == prefix or stem.startswith(f"{prefix}-")
    ]
    return max(matches, key=lambda item: len(item[0]))[1] if matches else ""


def _as_date(value: Any) -> date:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return date(1970, 1, 1)


def main(
    paths: list[str],
    *,
    folder_by_stem: dict[str, str] | None = None,
    retain_schema: bool = False,
) -> int:
    load_dotenv()
    url = os.environ["POSTGRES_DATABASE_URL"]
    schema = f"pdw_test_{datetime.now(tz=UTC):%Y%m%d%H%M%S}_securities_e2e"
    warehouse = PostgresWarehouse(url, schema=schema)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        warehouse.ensure_plaid_tables()
        warehouse.ensure_finance_tables()
        warehouse.ensure_manual_finance_tables()
        print("plaid copied:", copy_plaid(warehouse), flush=True)

        payload_paths = []
        for raw_path in paths:
            path = Path(raw_path)
            if path.name == "summary.json":
                continue
            if isinstance(json.loads(path.read_text()), dict):
                payload_paths.append(path)
        documents, extractions = document_rows(
            payload_paths, folder_by_stem=folder_by_stem
        )
        warehouse.insert_manual_finance_documents(documents)
        warehouse.insert_manual_finance_extractions(extractions)
        print(f"documents loaded: {len(documents)}", flush=True)

        summary = FinanceLedgerRunner(warehouse=warehouse).sync()
        print("\n=== ledger summary ===")
        print(summary, flush=True)

        invariant_failures = dict(
            warehouse._query(
                """
                SELECT 'mixed_asset_class_keys', count(*) FROM (
                    SELECT security_key
                    FROM @finance_security_transactions
                    GROUP BY security_key
                    HAVING count(DISTINCT asset_class) > 1
                ) q
                UNION ALL
                SELECT 'negative_quantities', count(*)
                FROM @finance_security_transactions WHERE quantity < 0
                UNION ALL
                SELECT 'known_basis_mismatches', count(*)
                FROM @finance_tax_lots l
                JOIN @finance_security_transactions t
                  ON t.transaction_id = l.opening_transaction_id
                WHERE l.basis_known = 1
                  AND abs(
                      l.cost_basis
                      - CASE WHEN t.amount <> 0
                             THEN abs(t.amount) + abs(t.fees)
                             ELSE abs(t.price) * t.quantity + abs(t.fees)
                        END
                  ) > 0.0001
                """
            )
        )
        print("invariants:", invariant_failures, flush=True)
        if any(int(value) for value in invariant_failures.values()):
            raise RuntimeError(f"securities ledger invariant failure: {invariant_failures}")

        for title, sql in REPORTS:
            print(f"\n=== {title} ===", flush=True)
            for row in warehouse._query_dicts(sql):
                print(json.dumps({k: str(v) for k, v in row.items()}), flush=True)
        if retain_schema:
            print(f"\nschema retained for inspection: {schema}")
        return 0
    finally:
        if not retain_schema:
            warehouse._raw_command(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        warehouse.close()


REPORTS = [
    (
        "trades by source",
        """
        SELECT source, count(*) AS trades, min(trade_date) AS earliest, max(trade_date) AS latest
        FROM @finance_security_transactions GROUP BY 1 ORDER BY 1
        """,
    ),
    (
        "link provenance (how each source row resolved)",
        """
        SELECT source, match_method, count(*) AS n
        FROM @finance_security_transaction_links GROUP BY 1,2 ORDER BY 1,2
        """,
    ),
    (
        # Lots whose acquisition predates Plaid's 730-day window — i.e. exactly
        # the ones only a statement can witness. Kept generic so this script
        # carries no holdings list of its own.
        "lots acquired before Plaid's lookback",
        """
        SELECT ticker, account_name, acquired_on, quantity, quantity_remaining,
               cost_per_unit, cost_basis_remaining, term, status, basis_known
        FROM @marts_finance_tax_lots
        WHERE acquired_on < current_date - INTERVAL '730 days'
        ORDER BY acquired_on, ticker
        LIMIT 40
        """,
    ),
    (
        "position coverage",
        """
        SELECT ticker, account_name, quantity_held, quantity_with_lots,
               quantity_with_known_basis, reported_cost_basis, reconstructed_cost_basis,
               basis_difference,
               earliest_acquisition, pct_quantity_with_basis, coverage_status
        FROM @marts_finance_position_coverage
        ORDER BY pct_quantity_with_basis DESC NULLS LAST, ticker
        LIMIT 40
        """,
    ),
]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--folder-map",
        action="append",
        default=[],
        metavar="STEM=FOLDER",
        help="Map an extraction filename stem/prefix to its uploaded account folder",
    )
    parser.add_argument("paths", nargs="+")
    parser.add_argument(
        "--retain-schema",
        action="store_true",
        help="Keep the generated throwaway schema for manual inspection",
    )
    args = parser.parse_args()
    mappings: dict[str, str] = {}
    for item in args.folder_map:
        if "=" not in item:
            parser.error("--folder-map must be STEM=FOLDER")
        stem, folder = item.split("=", 1)
        mappings[stem] = folder
    raise SystemExit(
        main(
            args.paths,
            folder_by_stem=mappings,
            retain_schema=args.retain_schema,
        )
    )
