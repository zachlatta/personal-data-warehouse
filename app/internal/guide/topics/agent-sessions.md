# Prior agent sessions

PDW indexes every past AI agent session across six providers — Claude Code, Codex, Claude
Desktop, ChatGPT, OpenClaw and pi — from every machine, unified in
`marts_ai_conversations`. It is the fleet's own transcript archive and the single most
underused thing in the warehouse. Before re-deriving anything, ask it: *have we solved
this before, and how? what did that agent conclude? how does the fleet really call tool
X?* One search routinely replaces an hour of rediscovery.

## Finding a session

Search first, drill second; never `ILIKE` the event tables.

{{if .CLI -}}
```sh
pdw search --source agent_session --priority self,background -n 20 'photos exporter PhotoKit'
pdw search --source agent_session --mode exact 'ugd76wbdoyr5blb6uda3su5o56'   # an id, error or URL
```
{{- else -}}
`search` with `{"query": "photos exporter PhotoKit", "sources": ["agent_session"], "priorities": ["self","background"], "max_results": 20}`;
for an id, error string or URL add `"mode": "exact"`.
{{- end}}

`self,background` is the right scope: Zach's prompts are `self`, the model's replies and
tool output are `background`, and orchestrator-spawned sessions are `background` entirely.
Transcripts are indexed **per turn**, so a hit lands on the matching turn: a turn ref is
`agent_session_turn:<provider>|<session_id>|<seq>`, a session headline ref is
`agent_session:<provider>|<session_id>`. `timeline.context(ref, 5, 5)` pages the
neighbouring turns. **Conclusions live at the end** — read the last assistant turns of
a hit's session before trusting a plan you found in its middle:

```sql
SELECT seq, occurred_at, role, event_type, tool_name, left(text, 800) AS text
FROM marts_ai_conversations.events
WHERE source = 'codex' AND session_id = '<session_id>' AND role = 'assistant'
ORDER BY seq DESC LIMIT 5;
```

## The relations

- `marts_ai_conversations.sessions` — one row per session: `source`, `session_id`,
  `account`, `device`, `title`, `cwd`, `git_branch`, `git_commit`, `repo_url`, `model`,
  `cli_version`, `entrypoint`, `first_prompt`, `started_at`, `ended_at`, `event_count`,
  token sums. `first_prompt` and `title` are the cheapest way to skim many sessions.
- `marts_ai_conversations.events` — one row per turn or tool call: `seq`, `occurred_at`,
  `role`, `event_type`, `subtype`, `text`, `tool_name`, `tool_input_json`,
  `tool_result_json`, `is_sidechain`, `raw_json`, per-event token counts.
- `base_claude_code.events`, `base_codex.events`, `base_claude_desktop.events`,
  `base_chatgpt.events`, `base_openclaw.events`, `base_pi.events` — the raw per-provider
  streams the mart unions.

`entrypoint` separates interactive from automated work: `cli` and `codex-tui` are Zach at
a keyboard; `sdk-cli`, `codex_exec` and OpenClaw's entrypoints are scheduled or
fleet-driven runs. `chatgpt` sessions have no entrypoint and `claude_desktop` `device`
values are opaque; read nothing into either.

## Pairing tool calls with results

A tool *call* and its *result* are separate rows: the assistant row carries `tool_name` +
`tool_input_json`, and the result arrives on the **following** `role = 'tool'` row's
`tool_result_json` with an empty `tool_name`. Pair them with
`lead(tool_result_json) OVER (PARTITION BY source, session_id ORDER BY seq)`, coalesced
with the same row's value because some providers fill both. Absent values are empty
strings, not NULL (`tool_name <> ''`). All three JSON columns are `text`; cast before `->>`.
`is_sidechain = 1` marks subagent turns: include them when auditing what an agent did,
exclude them when reconstructing the human-visible conversation.

```sql
-- How does the fleet actually call a tool? Real arguments beat a description.
SELECT source, tool_name, left(tool_input_json, 400) AS args, occurred_at
FROM marts_ai_conversations.events
WHERE tool_name ILIKE '%skill_write%'
ORDER BY occurred_at DESC LIMIT 25;

-- What work happened in a repo, or on a machine?
SELECT source, device, session_id, title, started_at, event_count, output_tokens
FROM marts_ai_conversations.sessions
WHERE repo_url ILIKE '%personal-data-warehouse%'
ORDER BY started_at DESC LIMIT 30;
```

The same MCP tool appears under several spellings across providers (bare, `mcp__…__`
namespaced, UUID-prefixed, app-prefixed): match with `ILIKE '%name%'` and group the
variants yourself, or you will undercount. When scanning results for failures, match
structured markers (`SQLSTATE`, `is_error":true`, `command not found`) rather than the
word "error", which appears constantly in ordinary content.

## What search can and cannot find

The search document for a session is its title, first prompt, working directory and the
user/assistant turns — **tool calls and tool results are not in it**. A path, command or
error that only appeared in tool output is found in `marts_ai_conversations.events`, not
by search. The flip side: anything you want a future agent to find by search has to
appear in your own reply text.

Sessions with auto-generated titles (workspace-naming prompts, one-line JSON replies)
are wrappers, not real work — skip them. Transcripts contain everything Zach pasted into
an agent, so the warehouse's privacy rules apply to them in full. Token columns exist on
both relations; report `input_tokens`/`output_tokens` separately from `cache_read_tokens`
rather than summing them.
