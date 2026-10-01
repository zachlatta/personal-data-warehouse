# Finance

Moved out of AGENTS.md on 2026-10-01 so the file every session loads holds only the
contracts and the rules every change needs. Start at [AGENTS.md](../../AGENTS.md).

## Plaid Finance

Personal financial data is linked through Plaid and stored in the source-owned `plaid` schema.
Raw/query tables are `base_plaid.items`, `base_plaid.accounts`, `base_plaid.transactions`,
`base_plaid.investment_securities`, `base_plaid.investment_holdings`, `base_plaid.investment_transactions`,
`base_plaid.liabilities`, and `ops.plaid_sync_state`. Finance-domain read views are
`marts_finance.accounts`, `marts_finance.transactions`, `marts_finance.investment_holdings`,
`marts_finance.investment_transactions`, and `marts_finance.liabilities`. Access tokens are
isolated in `private.plaid_item_tokens`. Warehouse initialization provisions the NOLOGIN
`PDW_QUERY_POSTGRES_ROLE` (default `pdw_query`), revokes `private` from it/`PUBLIC`, and both Go and
Python read-only query runners assume that role for every user-authored query; never bypass this
boundary or expose the token table through normal query surfaces.

For a finance read or investigation, start with `pdw search` or bounded SQL over
`timeline.events`, move to `marts_finance.*` / `marts_ops.*`, and drill into `base_plaid.*`
only when the stable read interfaces are insufficient. Before writing SQL, run
`pdw columns <relation>` for every relation whose columns are not already known; do not guess.

Configure `PLAID_ACCOUNT`, `PLAID_CLIENT_ID`, `PLAID_SECRET`, and `PLAID_ENV` on the machine doing
interactive linking and in the production Dagster deployment. Use `pdw ingest plaid link` only
for a genuinely new institution: it opens the localhost Plaid Link flow and persists the exchanged
token. Never use it to repair an existing Item.
`pdw ingest plaid items` lists what is linked; `pdw ingest plaid unlink <item-id>` retires one
(revokes it at Plaid, then deletes exactly that Item's rows — use it only after deliberately
confirming a duplicate Item, never as an automatic repair).
`pdw ingest plaid sync` performs an immediate pull. Production uses the `plaid_finance_sync` asset
and `plaid_finance_sync_every_thirty_minutes` schedule. Account, holding, and liability responses
are authoritative snapshots: reconcile missing accounts/holdings/liabilities rather than leaving
stale current rows. Product errors must persist a redacted `ops.plaid_sync_state` row before the run
fails. The exception is a permanent Item error — `NO_ACCOUNTS`, `ITEM_LOGIN_REQUIRED`, and the rest
of `PLAID_ACTION_REQUIRED_ERROR_CODES` — which no retry can clear: those record status
`action_required` (keeping the prior cursor and last-success time so an Item update resumes instead of
replaying), warn in the run log, count in the asset's `action_required` metadata, and leave the run
green. Otherwise one dead institution keeps the every-30-minutes schedule permanently red and
buries the transient failures that are worth paging on. Run `pdw ingest plaid items` to identify
the exact existing Item, then repair its consent with `pdw ingest plaid update <item-id>`.
Update mode uses that Item's existing access token and account selection; it must report that the
Item identity and credential are unchanged plus `accounts available: N` with `N > 0`. A failed
verification or zero available accounts returns nonzero and leaves the existing Item in place.

**Historical duplicate trap.** Before update mode existed, a fresh Link run used as a repair only
sometimes repaired the existing Item; it could instead mint a NEW `item_id` with NEW account ids
for the same real accounts and leave the dead Item linked beside it (this happened on 2026-07-25).
Both Items then sync: every
balance is counted twice in `marts_finance.net_worth` and the transaction overlap is duplicated.
When auditing an old repair or suspected duplicate, start with `marts_ops.plaid_item_health`, which
reads `duplicate` (with
`duplicate_item_ids`) for every live Item whose live accounts share mask + type + subtype with
another live Item at the same institution, because it happened a second time — two Capital One
Items, both `ok`, both syncing, two `marts_finance.net_worth` rows per card — and every health
surface stayed green for at least a day. Only after confirming which Item is the duplicate should
an operator deliberately retire it with `pdw ingest plaid unlink <item-id>` (`--dry-run` first; the
id may be an unambiguous prefix). Unlink is never an automatic `action_required` remediation. The
ledger side self-heals from there: plaid account identity resolves by
owner + institution + mask + side, so the surviving Item's accounts merge back into the logical
accounts they duplicated, and the residue is pruned.
Optional products default to read-only `transactions,investments,liabilities`; no
payment/money-movement Plaid products are requested.
New Links request `PLAID_TRANSACTIONS_LOOKBACK_DAYS` of Transactions history, defaulting to Plaid's
730-day maximum; the same setting controls the Investments transaction query window. Transactions
is the required Link product, while configured Investments and Liabilities are additional
consented products so partial-product institutions remain linkable. Sync marks products absent
from an Item's Plaid product metadata as `unsupported` without failing supported products. Plaid
cannot expand an existing Item's Transactions history grant, so Items created with a shorter
window must be removed and linked again. Preserve and verify warehouse history during that
migration before deleting rows belonging to the old Item.
Run `uv run python scripts/plaid_linking_report.py` after linking/live verification to refresh the
mode-0600, gitignored `reports/plaid-linking-report.private.md` artifact with every institution and
anonymous account status plus last-pull evidence. See the README's **Plaid Finance Sync** section
for all settings and safe aggregate verification queries.

## SimpleFIN Finance (a second provider over the same accounts)

SimpleFIN is the second provider feed beside Plaid, added 2026-09-27 because Plaid's
Capital One Item kept going quiet (it was 5–20 hours stale on the day this landed) while
the SimpleFIN Bridge served the same cards with a same-day balance. It is read-only, one
claimed access URL for every institution connected in the bridge, and it lands in
`base_simplefin.accounts` / `.transactions` / `.holdings` with `ops.simplefin_sync_state`
as its state (`simplefin_sync.py`, Dagster asset `simplefin_finance_sync`, hourly at `:07`).

**The whole point is that both providers describe the SAME accounts, so the ledger has to
land them on one logical account each — or net worth doubles.** The bridge reports no
account type and no mask, so `finance_ledger.py` infers both: the last four digits in the
account name (`simplefin_account_mask`: "Venture X (5520)", "Checking ...4871") and a
kind/side from name keywords with the balance sign as tiebreak
(`simplefin_account_kind_side`). Resolution then runs exactly as Plaid's does — owner +
institution + mask + side against the ledger index, which already holds the Plaid-founded
accounts — and a SimpleFIN account whose name prints no mask is matched by the
transactions the two feeds share (`transaction_overlap`: ≥ 3 exact amount/±3-day pairs
with one Plaid-linked account at that institution, and no runner-up as good). Anything
weaker founds its own account, because under-merging reads as two lines while over-merging
silently halves a balance. Two live SimpleFIN accounts never share a ledger account.

Once landed: the bridge balance is a second `balance` observation with `source =
'simplefin'`, stamped with the bridge's own `balance-date` (not the run time), and a Plaid
balance observation now carries `base_plaid.accounts.synced_at` rather than the run time
(it used to be re-stamped `now()` by every five-minute ledger run). `marts_finance.net_worth`
takes the newer DAY outright and, on the same day, ranks the bridge's institution-stamped
balance ahead of Plaid's poll-stamped one (`source_rank` in the view): measured
2026-09-27, Plaid's Capital One balance read 4.11 at 13:30 while the bridge read 32.55 at
12:56, and 32.55 was 4.11 plus the two purchases Plaid had not posted yet. **That
preference is conditional on the flows**, because the bridge refreshes about once a day
(around 01:00Z) while Plaid posts through the day: measured 2026-09-29, the bridge's
01:00 balance read 32.55 with nothing posted after 09-26 while Plaid had posted nine
purchases dated 09-28 and read 172.53, and net worth quoted 32.55 all day. A bridge
balance whose newest POSTED movement for the account (read from each provider's own
rows through the account links, UTC days) is older than Plaid's by that day now ranks
behind Plaid's; with no such evidence the bridge still wins the day. And
each SimpleFIN transaction merges into the Plaid row for the same movement by exact
amount within ±3 days (`fuzzy_amount_date`), founding its own row only when Plaid has
none. Plaid goes first and keeps field precedence. **Signs are opposite and stored
faithfully**: SimpleFIN `amount` is positive-in (the ledger's convention, no negation),
Plaid's is positive-out; a SimpleFIN credit-card `balance` is negative when owed, so a
liability-side observation is booked as `-balance` — **except that the bridge also
reports a card in CREDIT as negative**: Capital One Savor carried an 11.54 cash-back
credit that Plaid reported as -11.54 and the bridge as -11.54 too, which booked an asset
as a debt. The bridge's magnitude is right and its sign is not evidence, so where Plaid
reports the same account Plaid's sign decides (`simplefin_liability_value`).
`marts_ops.simplefin_account_health` is the reconciliation surface — `ledger_account_id`,
`match_method`, `shared_with_plaid`, and `net_worth_value` / `net_worth_source` /
`plaid_newest_transaction_at` for which provider net worth is quoting and why — and a row at a Plaid-linked institution with `shared_with_plaid = 0` is the thing to
investigate. SimpleFIN holdings are stored raw and are not yet in the securities ledger.

Credential and failure modes: the bridge's setup token is base64 of a claim URL that can
be POSTed once; `uv run python -m personal_data_warehouse.simplefin_sync claim <token>`
prints the access URL, which is `SIMPLEFIN_ACCESS_URL` on the Dagster deployment
(`SIMPLEFIN_ACCOUNT` defaults to `PLAID_ACCOUNT`, and must, or every shared account
forks). A 401/402/403 is `action_required` on the connection row (`account_id = ''`) —
claim a new token; the run stays green and the ledger stops re-observing the frozen
balances. The bridge's own "connection may need attention" messages are `attention` on
the row they name and are repaired by a re-login inside the bridge. Each run reads one
bounded window (the oldest account cursor minus a 7-day overlap; the full
`SIMPLEFIN_LOOKBACK_DAYS` when an account has never been read), paged in ≤ 90-day
requests because the bridge caps a range at 90 days; pending rows the window no longer
carries are tombstoned, posted rows are never removed by absence.

## Finance Ledger (stocks and flows)

The derived `finance` schema is the cross-source ledger over the finance sources (Plaid +
manual_finance). Every source is a witness to one of two fact types: a **flow** (money moved: a
transaction) or a **stock** (something was worth X at time T: a balance, valuation, or principal).
The ledger stores **facts only** — no categories or other opinions; categorization is a future
enrichment layer.

- `derived_finance.accounts` — one row per logical account/asset/liability (kinds incl.
  checking/credit/brokerage/ira/mortgage/property/vehicle/private_fund/receivable), resolved across sources
  via `derived_finance.account_links` (photos-identity pattern: raw rows never learn about identity;
  deterministic `fa_<sha>` ids; delete links + rerun replays every decision). Identity evidence is
  owner + institution + mask + side for **both** sources: plaid account ids are item-scoped, so a
  re-link that mints a new Item would otherwise fork every account and double-count net worth.
  Two simultaneously-live plaid accounts never merge (nothing says which is authoritative) —
  retire the dead Item and the survivor adopts the older account on the next run, leaving residue
  with no links, which is pruned with its observations.
- `derived_finance.observations` — append-only per-day values (PK account_id/as_of/kind/source; NUMERIC
  money, DATE days). The `finance_ledger` asset (schedule `*/5 * * * *`, picking up Gmail, Plaid and statement
  updates) snapshots every live Plaid account's balance daily — Plaid itself only keeps
  current-state, so this table IS the balance history.
- `derived_finance.transactions` — the unified deduped flow ledger (+
  `derived_finance.transaction_links` audit): one row per real-world money movement across Plaid and
  uploaded statements, plus authenticated Capital One purchase alerts (`pending = 1`). Alerts
  reconcile only against a unique account/currency/merchant/exact-amount match within seven
  days; unresolved holds stay provisional, and changed amounts or ambiguous matches need
  review. `marts_finance.transactions.reconciliation_status` distinguishes these from posted
  spending; sum `settled_amount` and `active_pending_amount` separately. Removed authorizations
  retire; 30-day unmatched alerts expire as unconfirmed, never asserted cancelled. Refunds are
  separate posted credits. Bank-native pending links permit changed final amounts. Alerts never alter balance
  observations. Amounts are signed NUMERIC, **positive = inflow to the account** (Plaid's
  positive-out is negated at ingest; document rows carry explicit in/out). Cross-source dedup at
  the Plaid/statement overlap seam: same account + exact amount + dates within ±3 days merge
  (Plaid wins field precedence; `match_method` records source_id/pending_id/fuzzy_amount_date).
  Pending Plaid rows merge into their posted successor via `pending_transaction_id`; the
  transactions table is reconciled to current source rows every run (derived state — raw rows
  never touched). Statement balances become `balance` observations (`principal` on mortgage
  accounts); valuation docs (Zillow screenshots, fund positions) become `valuation` observations
  and found property/vehicle/private_fund accounts.
- Net worth: `marts_finance.net_worth` (latest observation per account, signed by side; net worth
  = `SUM(signed_value)`) and `marts_finance.net_worth_history` (forward-filled daily
  assets/liabilities/net series). **Every net-worth row says how stale it is**: `age_days`,
  `expected_refresh_days` (3 for Plaid-fed kinds, 35 for a mortgage statement, 120 for
  property / vehicle / private_fund / receivable valuations) and `staleness`
  (`ok` / `late` / `stale` at 1x / 3x). Measured 2026-08-26 the private-fund valuation was
  4.5 months old and the mortgage 8 weeks with nothing flagging either; quote a net worth
  with its stalest input, not as today's number. `marts_finance.accounts` (accounts + latest observation) and
  `marts_finance.transactions` (the ledger joined to accounts) REPLACED the old Plaid passthrough
  views of the same names; the plaid-specific `marts_finance.investment_*` / `marts_finance.liabilities`
  passthroughs remain.

## Manual Finance Documents (manual_finance)

Manually uploaded finance documents — bank/credit/brokerage/mortgage statements, property/vehicle
valuation screenshots, private-fund position docs, CSV/OFX/QFX exports — land in
`base_manual_finance.documents` (one row per doc, native id = content sha) with agent extractions in
`derived_finance.document_extractions`. The mortgage servicer is not Plaid-supported, so mortgage
statements are the mortgage's only source.

- Upload: `pdw ingest manual-finance <files-or-dir>` (native Go,
  `app/internal/uploaders/manualfinance`; the envelope's provenance dedup sha is mirrored
  server-side in `personal_data_warehouse.manual_finance_envelope` for the re-file script).
  The folder-per-account organization
  (`<institution>-<name>-<mask>/statement.pdf`) is preserved as `original_path` (the primary
  account-resolution hint) and as the object key's account segment:
  `manual-finance/inbox/<account-folder>/<date>-<sha><ext>`. Content-sha dedup + sha-keyed local
  state make re-runs cheap; `--limit`, `--mode full`, `--root` supported.
- **Tax returns, payroll records, and supporting evidence:** use
  `pdw ingest manual-finance --evidence-only <files-or-dir>`. This preserves arbitrary
  year/document folders without interpreting them as accounts. Originals use the same
  storage, extraction, health, and timeline/search pipeline, with provenance source
  `manual_evidence` rather than `manual`. The ledger deterministically withholds any
  content hash with a live evidence claim, even if an earlier ordinary manual claim or
  a confident extraction exists. Re-running ordinary upload cannot undo that protection.
  `documents_withheld_evidence` is reported by the finance ledger asset. Upload state and
  metadata dedup distinguish the two claims, while file bytes still dedup by content hash.
  This is searchable evidence, not verified tax calculations or a tax-filing engine.
  Deploy the ledger guard before using the new uploader against production.
- Transport: `/ingest/manual-finance/file` + `/metadata` (photos pattern, HMAC-signed,
  provenance-sha metadata dedup that excludes `original_path`, so moving a file does not
  duplicate the document — but note it does not update the hint either: an identical dedup
  sha makes the app return the existing object and write nothing at all. See the re-filing
  paragraph below). Dagster `manual_finance_drive_inbox_sensor` + `manual_finance_drive_ingest`
  consume the inbox and promote objects to `manual-finance/library/` keeping the account segment.
- **Agent-first extraction** (`manual_finance_extraction.py`): bank files are structured in
  terrible ways, so there are NO format-specific parsers and no deterministic path that bypasses
  the agent. Input prep only chooses what the agent sees (pypdf text layer when rich; pdftoppm
  page renders — `MANUAL_FINANCE_RENDER_MAX_PAGES`, default 10 — when scanned; raw text for
  CSV/OFX/RTF; normalized JPEG for screenshots). The agent runs with read-only warehouse access
  (`run_with_pdw`), gets known `derived_finance.accounts` + `original_path` as context, and returns
  a strict-schema payload (transactions[]/balances[]/valuations[]/positions[]/commitments[]
  with decimal-string money, plus `reporting_scope` / `account_holder` / `value_basis`)
  mapped into typed columns; bumping `PROMPT_VERSION` re-extracts without clobbering. Retry cap:
  agent failures count per run in `agent_runs`; permanent input-prep failures record status
  `unreadable` and are excluded within the error window.
- Config: folder ids fall back to the shared Drive folder; set
  `PDW_INGEST_MANUAL_FINANCE_FOLDER_ID` (app) + `MANUAL_FINANCE_GOOGLE_DRIVE_FOLDER_ID` (Dagster
  reader) to use a dedicated `manual-finance` subfolder inside the existing PDW Drive folder.

### A document about an ENTITY is never one of Zach's accounts

**The upload folder is the account, and a document that names no account is refused.**
On 2026-08-27 a batch of private-fund files was uploaded to the *root* of the corpus. With
no folder, `document_account_key` fell back to `institution|mask` — and a fund's financial
statement prints no account mask, so the key degenerated to **`<institution>|`**: a
catch-all keyed on a counterparty rather than an account. It swallowed three unrelated
investment vehicles, a tax notice, Zach's real capital account statements and the fund's OWN
unaudited financials, then reported the **fund's** total members' equity as his largest
asset — `marts_finance.net_worth` came back more than an order of magnitude too high. The
same account took the fund's portfolio SAFE purchases into `marts_finance.transactions` and
two phantom `marts_finance.tax_lots` for a company Zach has never held directly. Every other
branch of that function names exactly one account (a folder by the uploader's contract, a
mask by account number, a filename by document), so `institution|` with an empty mask now
returns `UNIDENTIFIED_ACCOUNT_KEY` and the group books **nothing** — no account, no
observation, no transaction. Understating is recoverable; fabricating is not.

**`reporting_scope` is the guard the key cannot be.** A private fund sends its LPs two
documents that are identical in every extracted field — its own financial statements
(balance sheet, "total members' equity", its portfolio purchases) and that investor's
capital account statement — so a fund statement sitting in a *correct* account folder would
still book the wrong number. The v3 extraction contract
(`PROMPT_VERSION = manual-finance-agent-v3`) asks the agent directly: `reporting_scope` is
`account_holder` / `entity` / `unknown`, `account_holder` is the individual named on the
document, and an `entity`-scoped extraction contributes nothing to the ledger while
remaining a fact of record in `derived_finance.document_extractions`. **Absence is not
`account_holder`**: a pre-v3 row has no scope and is read as "not established", which is why
the key guard exists — it is the half that protects a corpus nobody has re-extracted yet.

**A contractual figure inside the holder's own document is not the holder's
position.** An executed SAFE prints a **post-money valuation cap** — a ceiling on the
ISSUER's valuation, contractually, not what the investor's stake is worth. It is the
largest and most prominent number on the page, so every "primary figure first" heuristic
picks it, and the document is unambiguously the investor's own, so `reporting_scope` cannot
help. Found live 2026-08-28 on a small angel position whose SAFE stated a cap **12,500x
its cost basis**, fifteen minutes before a scheduled ledger run. Two guards, because either
alone is insufficient:

- Each `valuations[]` entry now carries a **`measure`** — `position_value` / `cost_basis` /
  `reference` / `unknown`. `_daily_valuations` prefers `position_value`, falls back to
  `cost_basis` (an angel SAFE really is carried at cost), and **drops `reference`
  outright**: a valuation cap, an option strike, a bond's face value, a discount rate, an
  appraisal's high/low bound. A document whose only number is a `reference` contributes no
  valuation at all, which is the correct answer rather than a 12,500x wrong one. Pre-v3
  entries have no `measure`, are read as `unknown`, and behave exactly as before.
- **Two DIFFERENT documents claiming one `(account, day, kind)` book NEITHER**, and the
  runner logs both content shas. Silent last-writer-wins is the mechanism behind both
  incidents — it replaced a real capital balance with a fund's members' equity, and it was
  about to replace an angel position with that SAFE's valuation cap — and both times the
  wrong document
  won only because it sorted later by content sha. Refusing understates by one day, which
  the account's other observations and `net_worth_history`'s forward fill absorb. The
  refusal is **cross-document only**: one statement restating a running balance several
  times for a day (a credit-card statement in this corpus does it five times) is the
  document restating itself. Tolerance is measured, not guessed — over the whole corpus
  (904 claims, 17 multi-document account-days) the only other genuine disagreement is a
  $0.00-vs-$0.51 rounding difference on a 2018 statement, so 1% or $1 keeps it.
  `observation_conflicts` on the `finance_ledger` asset counts refusals; unlike the
  withholding counters it is **not self-healing** and means a source document needs a human.

**A mask is only identity when something other than the agent vouches for it.** An
extracted `account_mask` is whatever looked most account-number-shaped on the page, and
documents routinely print somebody else's number: a wire-instruction sheet prints the
PAYEE's bank account, a vehicle purchase order the dealer's stock number. Both ended up
stamped on personal ledger accounts. A mask now becomes identity only when the **upload
folder names it** (the uploader's `<institution>-<name>-<mask>/` convention, so a human
typed it) or a **provider reports it** (Plaid listing an account with that mask makes it
his by definition) — `mask_is_corroborated`. It must be both tests: one brokerage folder is
named for a retired account number while the live mask differs, so folder-only would strip
a mask Plaid confirms, and provider-only would strip every statement-only account's. An
uncorroborated mask is dropped rather than stamped, and cannot be matched on; the account
keeps its folder identity and its values are untouched. **A mask is re-resolved every run,
like an account link** — it used to be written only when a group FOUNDED its account, so an
account created before this rule kept a number nothing vouched for, and two such accounts
were still presenting a counterparty's number as the owner's identity a day after the guard
shipped. `clear_uncorroborated_finance_account_masks` reconciles them, counted as
`masks_cleared` on the run summary. It only ever CLEARS, and never an account a provider
also links, so the failure direction is a missing mask rather than somebody else's. **The
voucher is the FOLDER, not
the account key** — `document_account_key` falls back to `institution|mask` for a document
uploaded bare, which contains the agent's own mask by construction, so testing against the
key would let the agent corroborate itself in exactly the case where no human typed
anything. `document_account_folder` is the separate accessor that says so.

**A commitment triple is one fact about one vehicle, so an ambiguous day publishes none of
it.** `commitments[]` is one entry per vehicle, so a fund-administrator positions report
states committed/called for every vehicle on a single as-of date — and they collide on
`(account_id, as_of, kind)` where the account model has no vehicle key. Sort order picked
the winner, and one vehicle's numbers were published under another's name. Unlike a
repeated BALANCE (a statement restating a running balance, where the last entry is the
closing one), a repeated commitment is a different obligation, so a within-document repeat
is refused too. The refusal poisons the whole `(account, day)` triple, not just the kind
that collided: a positions report states `unfunded` for only some vehicles, so refusing the
two colliders alone would publish a lone surviving `unfunded` — a commitment row with no
commitment, from a set the ledger just said it could not attribute.

**A NULL `unfunded` means the document was silent, not that nothing is owed.**
`marts_finance.commitments` publishes `unfunded_stated` (the document's own figure),
`unfunded` (falling back to `committed - called`, floored at zero because an SPV routinely
calls slightly more than the subscription and a negative "still owed" is not a refund) and
`unfunded_basis` (`stated` / `derived` / `unknown`). Reading a NULL as no obligation is how
a real five-figure future capital call stayed invisible. **`unfunded` is NULL, never zero,
when the basis is `unknown`** — a signed subscription states committed and nothing else,
and the obvious `COALESCE(unfunded, GREATEST(committed - called, 0))` publishes that whole
obligation as `0`, because Postgres `GREATEST` **ignores** its NULL arguments rather than
returning NULL. The view is a `CASE` for that reason; do not simplify it back.

**Re-filing an already-uploaded document needs no local copy of it.** A document upload is
two posts and only the second carries the path: the blob is deduped by content sha and is
already in Drive. Everything the envelope needs is in `base_manual_finance.documents`, so
`scripts/refile_manual_finance_documents.py` reads the warehouse and re-posts only the
metadata — dry-run by default, `--apply` to write. That is the repair when files were
uploaded bare and the identity guard is (correctly) withholding them; the local corpus is
not needed and may not even exist on the machine you are on.

**The re-file's dedup sha folds in the destination folder, and it has to.** The obvious
version re-posts under `provenance_dedup_sha256`, which excludes `original_path` — and
that makes the re-post byte-identical to the original claim, so the app's `PutJSON` finds
the existing object by that sha and returns it **without writing**. Measured 2026-08-28:
19 re-posts, zero new inbox objects, and `manual_finance_drive_inbox_sensor` went on
reporting *"No manual finance inbox metadata found in object storage"* while the script
printed 19 successes. Excluding the path prevents a DUPLICATE; it does not perform an
UPDATE. `refile_dedup_sha256` derives a sha from the provenance sha plus the folder, so a
new inbox object is written, the reader upserts it onto the same document row — the PK is
`source`/`account`/`source_native_id`/`content_sha256`, none of which a re-file moves —
and `original_path` changes. Re-running the same map is still a no-op, because the same
folder yields the same sha. **A script that reports success is not evidence the object
landed**: check `manual_finance_drive_inbox_sensor`'s skip reason, or that
`base_manual_finance.documents.original_path` actually moved.

**`value_basis` keeps a tax number out of a market total.** A Schedule K-1's partner capital
account is a *tax* basis. It sat in net worth beside the same fund's NAV: the position
counted twice, and the second count on an incompatible measure. A `value_basis = 'tax'`
document's balances become `tax_basis` observations, which `marts_finance.net_worth`
excludes, and its line items book no transactions (a K-1 reports allocated shares of
partnership income, not money that moved).

**Unfunded capital commitments are facts, not liabilities.** `commitments[]` on the v3
contract carries the committed / called / unfunded triple a fund prints, stored as the
`commitment`, `called_capital` and `unfunded_commitment` observation kinds and read through
**`marts_finance.commitments`**. They are deliberately outside net worth — a commitment is
contingent on the fund calling it, and booking it as debt would make net worth disagree with
every statement — but they are a real future cash obligation, and before this they appeared
nowhere at all.

**A document link whose group no longer exists is residue re-resolution cannot reach.**
7adf12e made document links re-resolve every run so a decision made from thinner evidence
cannot freeze. That only walks groups that still exist, so a link left behind by deleted,
moved or refused documents kept its ledger account alive past
`prune_unlinked_finance_accounts` (which only reaches accounts with *zero* links).
`delete_missing_document_account_links` reconciles them, and the runner skips it when the
extraction corpus is empty — "the extraction asset has not run yet" and "every document was
withheld" are different states.

**One document reporting several vehicles still lands on one account.** A fund-administrator
positions report lists each vehicle with its own NAV plus a total, and the ledger books the
total on the folder's account. Splitting it would need per-position account attribution in
the extraction contract, which does not exist. So keep all investor-level documents for one
administrator's portfolio in **one** folder: per-vehicle folders plus a multi-vehicle
positions report double-counts the portfolio.

**`scripts/audit_finance_entity_documents.py` previews the effect, read-only.** Production
is the thing carrying the bad rows, so it cannot show you what it looks like without them;
that script runs the deployed predicates over the live corpus through `pdw sql` and prints
what would be withheld, which links lose their group, and net worth before and after. The
repair itself needs no migration — deploying and letting `finance_ledger` run reconciles
observations, transactions, security trades, tax lots, links and accounts by itself.

Finance SQL starting points: `marts_finance.net_worth`, `marts_finance.net_worth_history`,
`marts_finance.commitments`, `derived_finance.accounts`, `derived_finance.observations`,
`base_manual_finance.documents`, `derived_finance.document_extractions`, plus the existing
`base_plaid.*` / `marts_finance.*` views.

## Securities: trades, tax lots, and coverage

The cash ledger records that money left a brokerage account; it cannot say which security, how
many shares, or at what price. Those are the facts a purchase **lot** is made of, and they live
in their own layer.

**Do not answer a holdings/returns/cost-basis question from
`marts_finance.investment_transactions`** — that view is a Plaid passthrough, and Plaid's
lookback is a hard **730 days** (trade history starts 2024-07-16). Reading it alone is how an
agent concluded in 2026-08 that pre-2024 lots were "not reconstructable from Plaid" and told
the user to ask the broker, while the buys sat in the statement corpus back to 2018.

Use instead:

| relation | what it answers |
| --- | --- |
| `marts_finance.security_transactions` | every share movement, both sources, deduped |
| `marts_finance.tax_lots` | FIFO lots: acquired_on, basis, term, unrealized gain |
| `marts_finance.position_coverage` | **how much of a position actually has a lot history** |

- `derived_finance.security_transactions` is one row per real share movement across Plaid and
  manual statements, with `derived_finance.security_transaction_links` recording how each source
  row resolved (`source_id` founded it, `security_quantity_date` merged it into a Plaid twin).
  The ~20-month statement/Plaid overlap **must** dedup — a doubled trade yields a confidently
  wrong lot. Sides are `buy` / `sell` / `transfer_in` / `transfer_out`.
- `derived_finance.tax_lots` is the FIFO reduction, rebuilt wholesale each run (it is a
  reduction, not accumulated state, so it must be able to shrink). It never invents a basis:
  `basis_known = 0` for a transferred-in lot (the real basis is at the origin account), and a
  sale with no acquisition becomes an `unmatched_sale` row rather than a negative position.
  `method` records the lot election used — FIFO is a *choice*, and the broker's own election
  governs at tax time.
- **`asset_class` is not cosmetic.** An option prints under the underlying's ticker but one
  contract is 100 shares. Plaid compounds this by labelling option trades `type = 'equity'` on
  the security while the transaction name reads *"buy 2.000 QBIT call with strike of $12.00"*.
  Options therefore get their own `security_key` and are excluded from spot pricing/coverage.
- **Check `position_coverage` before quoting a return.** `coverage_status` is `complete` /
  `partial` / `none` / `lots_exceed_holding` / `basis_mismatch` / `no_holding`. The last three
  mean either more open lots than shares held (a missing disposal), reconstructed basis
  disagreeing materially with the provider's independent position basis, or open lots in an
  account that holds none of that security at all. A percentage alone can only understate these
  problems because it is capped at 100.
- **A trade is only deduped against its twin inside ONE ledger account, so an account
  misresolution double-books the position and nothing downstream can tell.** That is what
  happened to Robinhood crypto, and it survived six weeks because it was invisible from every
  direction. Robinhood reports crypto as its own Plaid account while its crypto statements live
  in their own upload folder; the folder's `derived_finance.account_links` row was made from the
  one statement extracted at the time, which printed the *brokerage* account number in its
  header, and links were consulted before resolution and never revisited — so 48 statement
  crypto trades sat in the brokerage account, where nothing could merge them into the Plaid rows
  describing the same trades. `marts_finance.tax_lots` then reported phantom open BTC/ETH/SOL
  lots — worth more than the whole real position — in an account that never held a coin.
  Three things now hold this shut, and each was independently necessary:
  - **Document account links are re-resolved every run** and an evidence-backed match supersedes
    a stored link (`links_relinked` in the run summary). A link is a derived decision, and this
    is what makes "delete the links and rerun replays every decision" actually true. A group
    that matches nothing keeps the account its own documents founded, so a private-fund folder
    is never orphaned. Statement observations are reconciled to the current corpus for the same
    reason — otherwise a relinked group leaves balances behind on its old account.
  - **Quantity equality tolerates the coarser source's rounding.** Plaid prints crypto share
    counts to six decimals; on a 0.003183 BTC buy that is 1.4e-4 *relative*, wider than
    `QUANTITY_MATCH_TOLERANCE`, so even in one account five of those buys still would not have
    merged. A quantity now also matches when the finer value rounds exactly to the coarser one,
    bounded to sub-microshare differences so a share count printed without decimals can never
    absorb a trade half a share larger.
  - **`marts_finance.position_coverage` reports lots the account does not hold.** It used to
    start from held positions, so the phantom lots produced no row and the real crypto account
    read `complete` throughout. It is a FULL join now, and open lots with no holding behind them
    read `no_holding` — but only for an account whose holdings Plaid actually reports, because a
    statement-only account has no feed to disagree with and judging its lots against silence
    would make every reconstructed position look broken.

Extraction contract: `PROMPT_VERSION = manual-finance-agent-v2` captures per-trade
`ticker`/`cusip`/`quantity`/`price_per_share`/`trade_side`/`fees` plus a `positions[]` snapshot
of the statement's portfolio summary. v1 stored a brokerage buy as an anonymous cash debit.
Bumping the version re-extracts the corpus without clobbering v1 (the extractions PK includes
the prompt version). `price_is_derived = 1` marks a price computed from amount/quantity because
the document did not print one.

**Known limits — state them when the answer depends on them.** Neither is modelled, and both
show up as a `partial` / `lots_exceed_holding` coverage status rather than a wrong-looking
number:

- **Stock splits.** A split changes the share count with no trade, so a lot opened from a
  pre-split statement carries pre-split quantities against a post-split holding.
- **Wash sales.** Lots are raw FIFO facts. A harvested loss inside the ±30-day wash window is
  still shown as realized, because disallowance is a tax *opinion*, not a fact the sources
  witness. Say so before anyone trades on a harvesting number.

External verification scripts (they hit the real agent and real source data, so run them
deliberately):
`scripts/verify_manual_finance_extraction_v2.py <pdf>...` checks the agent against a statement's
printed detail; `scripts/verify_securities_ledger_e2e.py <extraction json>...` replays real Plaid
data plus real extractions through the production runner into a throwaway schema. The ledger's
full-replay contract is deterministic test coverage rather than an operator script:
`test_crypto_relink_converges_to_the_same_state_as_a_full_replay` first reproduces the frozen
Robinhood link, repairs it incrementally, deletes all derived finance state, and proves a clean
rebuild from the complete source corpus produces exactly the same links, trades, and lots.
