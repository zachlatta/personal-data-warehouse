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
- **A multi-position report books only the folder's own line.** A fund administrator's
  positions export lists every vehicle plus a total, uploaded into one vehicle's folder;
  the line whose description names that folder (`pwv-fund-i-lp` ↔ "PWV Fund I LP") is the
  account's value and lines naming other ledger accounts are theirs, never the total. A
  report that names other accounts but not its own folder's books nothing for that day
  and increments `valuations_withheld_unattributed`.
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

## Capital One purchase alerts

Authenticated Capital One purchase-alert emails feed the same ledger as provisional
flows (`source = 'capital_one_alert'`, `pending = 1`). The finance job checks every five
minutes, in addition to Gmail's own polling delay; this is near-real-time, not push.
Alerts do not change balances or net worth. `posted_at` is the purchase date in the
email, not a claim about the exact authorization time.

A unique match by owner/account, currency, merchant, exact amount and a posting date
within seven days replaces the provisional row with the Plaid or statement row.
`derived_finance.transaction_links` retains the email's `account|message_id` as
`source_row_key` for drill-down. Replays do not add another purchase.

In `marts_finance.transactions`, sum `settled_amount` for actual net flows and
`active_pending_amount` separately for provisional spending (both signed positive-in).
Do not sum raw `amount` across all statuses. `reconciliation_status` distinguishes
`posted`, `pending`, `provisional`, `needs_review`, `authorization_removed`, and
`expired_unconfirmed`.

An explicit Plaid pending-row removal retires an unambiguously matched alert from
active pending totals; merely missing from a sync does not prove cancellation.
Unmatched alerts older than 30 days become `expired_unconfirmed`: retained for audit
and review, but excluded from both totals, never asserted to be bank cancellations.
A later posted match can still settle either retired state. A provider's explicit
`pending_transaction_id` can reconcile changed tip/hold amounts; without that proof,
changed amounts and ambiguous matches need review and are excluded from active totals.

Refunds remain separate positive posted flows, offsetting the original purchase in
`settled_amount`, including partial refunds. They never replace the purchase or match
its email alert. Refund and payment emails are not purchase alerts; those credits
still arrive through Plaid or statements. Alerts never modify balance observations.
Unknown templates, failed authentication and ambiguous account identities are withheld,
counted as `alerts_withheld`; unresolved/stale cases are `alerts_needing_review` in job
metadata. Replays retain evidence without reviving a removed authorization as spending.
