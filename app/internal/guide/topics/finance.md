# Finance

Two sources — Plaid (linked institutions, synced every 30 minutes) and manually uploaded
documents (statements, valuations, fund positions, tax records) — feed one cross-source
ledger in `derived_finance.*`, read through `marts_finance.*`. The ledger stores **facts**
only: a *flow* (money moved) or a *stock* (something was worth X on day T). No categories.

## Start here

| question | relation |
| --- | --- |
| net worth, per account, with how stale each line is | `marts_finance.net_worth` (`age_days`, `expected_refresh_days`, `staleness`), `marts_finance.net_worth_history` |
| accounts and their latest observation | `marts_finance.accounts`, `marts_finance.account_freshness` |
| spending and income | `marts_finance.transactions` — signed, **positive = inflow to the account**, deduped across Plaid and statements |
| capital commitments (private funds) | `marts_finance.commitments` — outside net worth, but a real future cash obligation |
| holdings, trades, cost basis | `marts_finance.investment_holdings`, `marts_finance.security_transactions`, `marts_finance.tax_lots`, `marts_finance.position_coverage` |
| debts | `marts_finance.liabilities` |
| raw provider rows | `base_plaid.accounts`, `.transactions`, `.investment_holdings`, `.investment_transactions`, `.liabilities`, `.items` |
| uploaded documents and what the agent extracted from them | `base_manual_finance.documents`, `derived_finance.document_extractions` |
| is each institution alive | `marts_ops.plaid_item_health` |

Search scope `finance` covers transactions; a receipt's link to its transaction is
`marts_receipts.transaction_receipts`.

## Quote a number with its provenance

- **Every net-worth line says how stale it is.** `staleness` is `ok`/`late`/`stale`
  against each kind's own refresh expectation (days for a Plaid balance, weeks for a
  mortgage statement, months for a property or fund valuation). Quote a net worth with
  its stalest input, not as today's number.
- **Sanity-check every line against the scale of Zach's actual finances.** Two past
  incidents booked somebody else's balance sheet as one of his accounts — an
  entity-scoped fund statement, and a SAFE's post-money valuation cap — and net worth
  read an order of magnitude high with every health surface green. Both are guarded now
  (`reporting_scope`, `measure`, and a refusal when two different documents claim one
  account-day), which means a line can be *missing* for a day rather than wrong; the
  `finance_ledger` asset's `observation_conflicts` counter says so.
- **Check `position_coverage.coverage_status` before quoting a return.** `complete`,
  `partial`, `none`, `lots_exceed_holding`, `basis_mismatch`, `no_holding`. The last three
  mean open lots the provider's own holdings do not back; a lot-derived gain for that
  account is an upper bound, not an answer. Stock splits and wash sales are not modelled
  and show up as `partial`.
- **A NULL `unfunded` commitment means the document was silent, not that nothing is
  owed.** Read `unfunded_basis` (`stated`/`derived`/`unknown`) with the figure.
- **Tax basis is not market value.** A K-1's partner capital account is stored as a
  `tax_basis` observation and excluded from net worth.

## History has edges

- **Plaid holds at most 730 days of transactions, and one card issuer hands it only ~90
  days and no pending items.** Anything older lives only in the statement corpus, so a
  spending-history question is answered from `marts_finance.transactions` (which merges
  statements in), never from `base_plaid.transactions` alone. `authorized_at` is the epoch
  sentinel where the institution sends none.
- **Do not answer holdings or cost-basis questions from `marts_finance.investment_transactions`**
  — that is a Plaid passthrough bounded by the same lookback. `security_transactions` and
  `tax_lots` reach back through the statements.
- **A re-link can mint a second live Plaid Item** and double-count an institution while
  everything reads `ok`. `marts_ops.plaid_item_health` reads `duplicate` for it; the
  retirement is deliberate ({{if .CLI}}`pdw ingest plaid unlink <item-id> --dry-run` first{{else}}an operator's `pdw ingest plaid unlink`{{end}}).
- Uploaded documents: the **upload folder is the account**
  (`<institution>-<name>-<mask>/statement.pdf` is preserved as `original_path`). A
  document at the corpus root with no account mask books nothing, on purpose; if a
  source's documents look missing from the ledger, check `original_path` first, then the
  `finance_ledger` asset's `documents_withheld_*` counters.

## Ledger model, for when provenance matters

- `derived_finance.accounts` — one row per logical account/asset/liability (kinds include
  checking, credit, brokerage, ira, mortgage, property, vehicle, private_fund,
  receivable); `derived_finance.account_links` records how each source row resolved to it.
  Identity is owner + institution + mask + side for both sources, so a re-link does not
  fork an account. Links are re-resolved every run.
- `derived_finance.observations` — append-only per-day values (`balance`, `principal`,
  `valuation`, `commitment`, `called_capital`, `unfunded_commitment`, `tax_basis`). Plaid
  itself keeps only current state; this table **is** the balance history.
- `derived_finance.transactions` + `transaction_links` — the deduped flow ledger; a
  statement row within ±3 days and an exact amount of a Plaid row merges into it.
- `derived_finance.security_transactions` + `tax_lots` — share movements and their FIFO
  reduction, rebuilt wholesale each run; `basis_known = 0` marks a transferred-in lot; a
  sale with no acquisition is an `unmatched_sale` row, never a negative position. Options
  carry their own `security_key` (one contract is 100 shares).
- `derived_finance.document_extractions` — the agent's strict-schema reading of each
  document (`reporting_scope`, `account_holder`, `value_basis`, per-entry `measure`),
  keyed by prompt version so a re-extraction never clobbers the last one.
