"""Read-only preview of what the entity/identity guards drop from the ledger.

Production is the thing carrying the bad rows, so it cannot show you what it
would look like without them. This runs the SAME predicates the deployed
``FinanceLedgerRunner`` uses -- ``document_account_key``,
``document_reports_an_entity`` and ``document_reports_a_tax_basis`` -- over
the live extraction corpus read through the read-only ``pdw`` CLI, and prints:

  * every document group whose key names an institution but no account
  * every extraction that declares itself an ENTITY's own books
  * every ledger account, observation, transaction and tax lot behind them
  * net worth as reported, and as it reads once they are gone

It WRITES NOTHING. The repair itself needs no migration: deploy the code and
let ``finance_ledger`` run. Its own reconciliation
(``delete_missing_document_observations``, ``reconcile_finance_transactions``,
``delete_missing_finance_security_transactions``, ``replace_finance_tax_lots``,
``delete_missing_document_account_links``, ``prune_unlinked_finance_accounts``)
removes exactly what this prints.

    uv run python scripts/audit_finance_entity_documents.py
"""

from __future__ import annotations

import json
import subprocess
from decimal import Decimal
from typing import Any

from personal_data_warehouse.finance_ledger import (
    UNIDENTIFIED_ACCOUNT_KEY,
    document_account_key,
    document_reports_a_tax_basis,
    document_reports_an_entity,
)


def pdw_json(intent: str, sql: str) -> list[dict[str, Any]]:
    completed = subprocess.run(
        ["pdw", "sql", "--output", "json", "-q", intent, sql],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(completed.stdout or "[]")


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value or 0))


def main() -> int:
    extractions = pdw_json(
        "latest manual finance extraction per document, with its upload path",
        """
        SELECT DISTINCT ON (e.content_sha256)
               e.content_sha256, e.institution, e.account_mask,
               e.account_name_hint, e.document_type,
               -- Read through to_jsonb so this runs against a deployment
               -- that has not migrated to the v3 columns yet: an absent key
               -- is NULL, which the ledger's predicates read as "not
               -- established" -- exactly what they do with a pre-v3 row.
               COALESCE(to_jsonb(e) ->> 'reporting_scope', '') AS reporting_scope,
               COALESCE(to_jsonb(e) ->> 'value_basis', '')     AS value_basis,
               d.account, d.original_path, d.filename
        FROM derived_finance.document_extractions e
        JOIN base_manual_finance.documents d
          ON d.content_sha256 = e.content_sha256 AND d.is_deleted = 0
        WHERE e.status = 'ok'
        ORDER BY e.content_sha256, e.created_at DESC
        """,
    )

    withheld: dict[str, list[dict[str, Any]]] = {"entity": [], "unidentified": [], "tax_basis": []}
    kept_keys: set[tuple[str, str]] = set()
    for extraction in extractions:
        if document_reports_an_entity(extraction):
            withheld["entity"].append(extraction)
            continue
        key = document_account_key(
            original_path=str(extraction["original_path"]),
            institution=str(extraction["institution"]),
            mask=str(extraction["account_mask"]),
            filename=str(extraction["filename"]),
        )
        if key == UNIDENTIFIED_ACCOUNT_KEY:
            withheld["unidentified"].append(extraction)
            continue
        kept_keys.add((str(extraction["account"]), key))
        if document_reports_a_tax_basis(extraction):
            withheld["tax_basis"].append(extraction)

    for label, rows in withheld.items():
        print(f"\n== withheld: {label} ({len(rows)}) ==")
        for row in sorted(rows, key=lambda r: str(r["original_path"])):
            print(
                f"  {row['original_path']}\n"
                f"      institution={row['institution']!r} mask={row['account_mask']!r} "
                f"type={row['document_type']!r} scope={row['reporting_scope']!r} "
                f"basis={row['value_basis']!r}"
            )

    links = pdw_json(
        "manual document account links, to find the ones no group claims",
        """
        SELECT account, source_account_key, account_id, match_method, created_at
        FROM derived_finance.account_links
        WHERE source = 'manual_finance'
        ORDER BY source_account_key
        """,
    )
    stale = [
        link
        for link in links
        if (str(link["account"]), str(link["source_account_key"])) not in kept_keys
    ]
    print(f"\n== links no document group claims ({len(stale)}) ==")
    for link in stale:
        print(f"  {link['source_account_key']!r} -> {link['account_id']} ({link['match_method']})")

    doomed = sorted({str(link["account_id"]) for link in stale})
    if doomed:
        ids = ", ".join(f"'{account_id}'" for account_id in doomed)
        rows = pdw_json(
            "what the orphaned ledger accounts currently carry",
            f"""
            SELECT a.account_id, a.name, a.kind, a.side,
                   (SELECT count(*) FROM derived_finance.observations o
                     WHERE o.account_id = a.account_id) AS observations,
                   (SELECT count(*) FROM derived_finance.transactions t
                     WHERE t.account_id = a.account_id) AS transactions,
                   (SELECT count(*) FROM derived_finance.tax_lots l
                     WHERE l.account_id = a.account_id) AS tax_lots
            FROM derived_finance.accounts a
            WHERE a.account_id IN ({ids})
            ORDER BY a.name
            """,
        )
        print("\n== ledger accounts that lose their last link ==")
        for row in rows:
            print(
                f"  {row['account_id']} {row['name']!r} kind={row['kind']} side={row['side']} "
                f"observations={row['observations']} transactions={row['transactions']} "
                f"tax_lots={row['tax_lots']}"
            )

    net_worth = pdw_json(
        "net worth line items",
        """
        SELECT account_id, name, kind, side, observation_kind, as_of, value, signed_value
        FROM marts_finance.net_worth
        ORDER BY abs(signed_value) DESC
        """,
    )
    reported = sum(_decimal(row["signed_value"]) for row in net_worth)
    dropped = {*doomed}
    remaining = [row for row in net_worth if str(row["account_id"]) not in dropped]
    corrected = sum(_decimal(row["signed_value"]) for row in remaining)
    print("\n== net worth ==")
    print(f"  reported now : {reported:>18,.2f}")
    print(f"  after guards : {corrected:>18,.2f}")
    print(f"  difference   : {reported - corrected:>18,.2f}")
    print("\n  lines that disappear:")
    for row in net_worth:
        if str(row["account_id"]) in dropped:
            print(f"    {row['name']!r} {row['observation_kind']} {row['as_of']} {row['value']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
