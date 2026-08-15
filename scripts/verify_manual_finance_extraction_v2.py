"""Run the REAL v2 extraction prompt+schema against REAL statement PDFs.

This is the grounding check for the securities work: it does not assert that
the code is self-consistent (the unit tests do that), it asserts that the
agent, given the production prompt and the production strict schema, copies
the per-trade security detail a real brokerage statement actually prints.

Bytes come from the local document corpus instead of Drive; everything else —
input preparation, prompt, schema, validation — is the production path.

    uv run python scripts/verify_manual_finance_extraction_v2.py <pdf> [<pdf>...]
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from decimal import Decimal, InvalidOperation
from pathlib import Path

from personal_data_warehouse.manual_finance_extraction import (
    finance_document_extraction_schema,
    finance_extraction_prompt,
    prepare_document_inputs,
    validate_finance_extraction_result,
)
from personal_data_warehouse.securities_ledger import (
    ASSET_CLASS_OPTION,
    classify_asset_class,
)

MODEL = "gpt-5.6-sol"


def run_codex(prompt: str, schema: dict) -> dict:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        schema_path = tmp_path / "schema.json"
        schema_path.write_text(json.dumps(schema))
        final_path = tmp_path / "final.json"
        prompt_path = tmp_path / "prompt.txt"
        prompt_path.write_text(prompt)
        command = [
            "codex",
            "exec",
            "--json",
            "--skip-git-repo-check",
            "--dangerously-bypass-approvals-and-sandbox",
            "--model",
            MODEL,
            "--output-last-message",
            str(final_path),
            "--output-schema",
            str(schema_path),
            "-",
        ]
        with prompt_path.open("rb") as stdin:
            completed = subprocess.run(
                command, stdin=stdin, capture_output=True, text=True, timeout=1800
            )
        if not final_path.exists():
            raise RuntimeError(
                f"codex produced no final message (rc={completed.returncode})\n"
                f"{completed.stderr[-2000:]}"
            )
        return json.loads(final_path.read_text())


def _decimal(value: object) -> Decimal | None:
    text = str(value or "").strip().replace(",", "").replace("$", "")
    if not text:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def _source_contains(source_text: str, value: object) -> bool:
    """Whether a copied numeric token occurs verbatim modulo $ and commas."""
    token = str(value or "").strip().replace(",", "").replace("$", "")
    normalized_source = source_text.replace(",", "").replace("$", "")
    return bool(token) and token in normalized_source


def grounding_summary(output: dict, *, source_text: str) -> dict:
    """Mechanical checks against the exact text supplied to the agent."""
    trades = [
        row
        for row in output.get("transactions", [])
        if str(row.get("trade_side") or "").strip()
    ]
    quantities = [row for row in trades if str(row.get("quantity") or "").strip()]
    prices = [row for row in trades if str(row.get("price_per_share") or "").strip()]
    arithmetic_outliers: list[dict] = []
    for row in prices:
        quantity = _decimal(row.get("quantity"))
        price = _decimal(row.get("price_per_share"))
        amount = _decimal(row.get("amount"))
        fees = abs(_decimal(row.get("fees")) or Decimal("0"))
        if quantity is None or price is None or amount is None:
            continue
        is_option = (
            classify_asset_class(
                name=str(row.get("security_name") or ""),
                description=str(row.get("description") or ""),
            )
            == ASSET_CLASS_OPTION
        )
        multiplier = Decimal("100") if is_option else Decimal("1")
        difference = abs(abs(amount) - abs(quantity * price * multiplier))
        # Printed fractional-share totals are rounded to cents; explicit fees
        # may either be itemized beside or folded into the cash total. A price
        # printed to cents can contribute up to half a cent of rounding error
        # per share; options use their exact 100-share multiplier.
        quote_rounding = Decimal("0") if is_option else quantity * Decimal("0.005")
        if difference > fees + quote_rounding + Decimal("0.02"):
            arithmetic_outliers.append(
                {
                    "date": row.get("date"),
                    "ticker": row.get("ticker"),
                    "quantity": row.get("quantity"),
                    "price_per_share": row.get("price_per_share"),
                    "amount": row.get("amount"),
                    "difference": str(difference),
                }
            )
    return {
        "quantities_grounded_in_source": sum(
            _source_contains(source_text, row.get("quantity")) for row in quantities
        ),
        "quantities_checked": len(quantities),
        "prices_grounded_in_source": sum(
            _source_contains(source_text, row.get("price_per_share")) for row in prices
        ),
        "prices_checked": len(prices),
        "arithmetic_outliers": arithmetic_outliers[:10],
        "arithmetic_outlier_count": len(arithmetic_outliers),
    }


def summarize(path: Path, output: dict, *, source_text: str) -> dict:
    trades = [
        t
        for t in output.get("transactions", [])
        if str(t.get("ticker") or t.get("security_name") or "").strip()
        and str(t.get("quantity") or "").strip()
    ]
    priced = [t for t in trades if str(t.get("price_per_share") or "").strip()]
    cash = [t for t in output.get("transactions", []) if t not in trades]
    return {
        "document": path.name,
        "document_type": output.get("document_type"),
        "period": f"{output.get('period_start')}..{output.get('period_end')}",
        "transactions": len(output.get("transactions", [])),
        "trades": len(trades),
        "trades_with_price": len(priced),
        "cash_rows": len(cash),
        "positions": len(output.get("positions", [])),
        "sample_trades": trades[:4],
        "sample_positions": output.get("positions", [])[:4],
        "uncertainties": output.get("uncertainties", []),
        "grounding": grounding_summary(output, source_text=source_text),
    }


def main(paths: list[str]) -> int:
    schema = finance_document_extraction_schema()
    results = []
    for raw in paths:
        path = Path(raw).expanduser()
        content = path.read_bytes()
        inputs = prepare_document_inputs(
            content=content, mime_type="application/pdf", filename=path.name
        )
        candidate = {
            "original_path": f"{path.parent.name}/{path.name}",
            "filename": path.name,
            "mime_type": "application/pdf",
        }
        prompt = finance_extraction_prompt(
            candidate=candidate, inputs=inputs, known_accounts=[]
        )
        output = run_codex(prompt, schema)
        issues = validate_finance_extraction_result(output)
        summary = summarize(path, output, source_text=inputs.text)
        summary["validation_issues"] = issues
        results.append(summary)
        out_path = Path("/tmp/extraction-v2") / f"{path.stem}.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(output, indent=2))
        print(json.dumps(summary, indent=2), flush=True)
    Path("/tmp/extraction-v2/summary.json").write_text(json.dumps(results, indent=2))
    failed = any(
        result["validation_issues"]
        or result["grounding"]["quantities_grounded_in_source"]
        != result["grounding"]["quantities_checked"]
        or result["grounding"]["prices_grounded_in_source"]
        != result["grounding"]["prices_checked"]
        or result["grounding"]["arithmetic_outlier_count"]
        for result in results
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
