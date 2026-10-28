# The agent guide: why it is shaped this way, and how agents stumble

The rules for the guide itself (where it lives, its byte caps, what its tests enforce) are
in [AGENTS.md](../../AGENTS.md#the-agent-guide). This file keeps the measurements behind
them, and how contract C17's failures are found and fixed.

## Why the brief is the default

Measured over twelve real sessions on
2026-09-22, the skill plus the full guide cost 25–45 KB before the first data call
whatever the question was — a one-line phone-number lookup paid eight times its own
answer in preamble — which is why the default is the brief. The same audit added
`pdw context <ref>` (the first-class spelling of `timeline.context()`, naming its columns:
`SELECT *` there returned `metadata` and the full `search_text` of every row, 3.8 KB
against 0.5 KB for the three columns anyone reads, and it was used a quarter as often as
search because the ref had to be quoted inside a quoted statement), a one-line-per-hit
search output (`--full` for the old previews, `-n 10` default, an unknown flag anywhere
is an error rather than query text), and on MCP `connections` + `connection_call` in
place of a flat listing of every connected upstream tool: that list was 196 tools /
153 KB (~38k tokens) of definitions in every MCP session, 142 KB of it proxied, and the
sessions paying it almost never called one (the flat listing is gone, not switchable;
`pdw list` / `pdw call` are unchanged). Since 2026-10-01 `context` is a server tool on both
surfaces and `pdw context` calls it, so a hit's conversation is read one way everywhere. The follow-up the same day: search
previews are cleaned after windowing (tracking URLs, markdown-table scaffolding, zero-width
padding — a newsletter hit's whole 800-char preview had been squarespace-mail.com redirect
links); a textless Slack hit (a file, image or canvas post, ~5,000 a week) is labelled at
**read time** in the search service rather than in the adapter's snippet, because the
adapter edit would change its signature and re-walk 46.8M Slack rows for a label; the SQL
tool hints at `body_markdown_clean` when a statement reads `body_text`/`body_html` from
`base_gmail.messages`; `schema_overview` / `pdw schema` take a schema name or layer prefix
so one domain costs one schema instead of the 36 KB whole; and `marts_ops.agent_usage`
pairs a tool result with its call by `turn_id` (Claude Code's `tool_use_id`, now stamped on
result rows too) before falling back to adjacency, and reads a piped CLI search with no
`Search:` header as unknown rather than failed — adjacency alone had lost the result of
26% of pdw search calls and nearly every proxied MCP call, and the header rule had put 29%
of a fortnight's searches in `search_invalid_or_failed_priority`.

## Finding and fixing stumbles

Contract C17 says agents do not stumble: SQL-error sessions under 10%, calls to commands that
do not exist near zero, and each week's top three failure shapes fixed in the guide or the
CLI before the next week.

**The measurement.** The daily `agent_usage` asset reads every PDW call agents made from
their transcripts (`marts_ai_conversations.events`) and writes two things:

- `marts_ops.agent_usage`: the fortnight's rates. A session is a SQL-error session when a
  `pdw sql` / `query` result carries `SQLSTATE` 42703, 42P01, 42883, 42501, 57014 or 42804.
  Until 2026-10-28 it matched the bare digits, so a result row holding `42703` counted, and
  42804 (`NOT is_deleted` on an integer flag) did not count at all.
- `marts_ops.agent_top_failures`: one row per ISO week (Monday, UTC) and failure shape,
  ranked by sessions. A shape is the SQLSTATE plus the normalized message (aliases and
  numbers dropped), with the relations the statement read for a missing column or a
  timeout (`42703 column "id" does not exist on base_gmail.messages`). Or it is the `pdw`
  command the CLI does not have (`pdw query`, `pdw call sql`). A top-three shape reads
  `attention`. One that was also top three the week before reads `failing`, and that is
  what `scripts/contract_audit.py` grades as C17, on the last complete week.

**The weekly fix.** Read the last complete week:

```sql
SELECT rank, shape, sessions, failed_calls, sources, example, status
FROM marts_ops.agent_top_failures
WHERE week_start = (date_trunc('week', now()) - interval '7 days')::date
ORDER BY rank LIMIT 10;
```

Fix each top-three shape with the smallest change that prevents it, not one that only
recovers from it. The SQL service's hints (`app/internal/query/service.go`) already answer a
missing column with the relation's real columns and a "did you mean". So a second hint helps
the next call, but the session has already failed. A line in the brief (what every session
reads) prevents the first error. A command the CLI does not have is answered by a one-line
redirect (`commandRedirects` in `app/cmd/pdw-cli/run.go`), and since 2026-10-28 that runs
before `--help` handling. Before that, `pdw query --help` printed the global usage and exited
0, and the agent's next call was `pdw query "<sql>"`.

Two things make the top three move week to week:

- **Agents translate MCP tool names.** A Claude Code session with the PDW connector sees
  tools named `query`, `schema_overview` and `describe_table`, and types them as `pdw`
  commands. Zach chose on 2026-10-28 to keep refusing them with the real command rather
  than alias them (C9: one spelling per surface).
- **A recurring schedule repeats its mistake every run.** Each run is a fresh agent. Of
  the 255 SQL-error sessions from 2026-10-20 to 2026-10-27, 44% were recurring desk-task
  runs. One hourly task produced 51 `pdw query` sessions in two days. Its prompt did not
  teach the command, so the fix is upstream of the run (the fleet's start-here skill)
  rather than in its README.

**Numbers, measured from production with the 2026-10-28 SQL.**

| window ending 2026-10-28 | PDW sessions | SQL-error sessions | invented calls |
| --- | --- | --- | --- |
| 14 days, old definition | 1,557 | 341 (21.9%) | 269 |
| 14 days, new definition | 1,557 | 344 (22.1%) | 269 |
| 7 days | 1,008 | 254 (25.2%) | 210 |
| 2 days (after the `occurred_at` rename) | 367 | 56 (15.3%) | 63 |

The contract audit on 2026-10-27 read 24.6% and 260 over the fortnight then. The biggest
single fix was the `timeline.events` rename to `occurred_at` (2026-10-25): `column
"occurred_at" does not exist on timeline.events` was 55 sessions in the week of
2026-10-19 and none since. Top shapes by week:

| week of | 1 | 2 | 3 |
| --- | --- | --- | --- |
| 2026-10-19 | `occurred_at` on `timeline.events` (55) | 42804 `NOT` on an integer flag (41) | `pdw connections` (22) |
| 2026-10-26 (2 days) | `pdw query` (50) | `pdw call query` (12) | `pdw call sql` (10) |

Fixed on 2026-10-28: the brief names `pdw query`, `pdw tools` and `pdw connections` as not
commands, and gives the `NOT is_deleted`, `jsonb` `LIKE` and bare-`id` traps. The CLI
redirects `pdw query --help` and `pdw call query --help` instead of printing the usage.
