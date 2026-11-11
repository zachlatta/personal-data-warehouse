# Read-only JavaScript scripts

Use a script when one answer needs several dependent reads or filtering before results
reach the model. For one lookup use search, context or SQL directly. Scripts execute on
the PDW server for every client. Each run starts fresh; there is no persistent REPL.

{{if .CLI}}Run `pdw run script.js`, or `pdw run -` with source on stdin.
Pass data with `--args @input.json`; `--timeout-ms 60000` raises the wall deadline.
`pdw call run` is refused: use the dedicated command.{{else}}Call `run` with
`{"code":"return await pdw.search({query: args.term, priorities: ['self','direct','cc']});","args":{"term":"anchor"} }`.
Optional `timeout_ms` raises the wall deadline, up to 60000.{{end}}

The source is an async function body. Use `await` and `return`. Globals are a frozen
`pdw` object, immutable JSON `args`, and JavaScript built-ins. Imports, network, files,
timers, connections, mutations and approval APIs are unavailable. Dates are ISO strings;
unsafe bigint integers and exact decimals are strings. Do not convert them to Number
when exact values matter. JavaScript BigInt results serialize as strings too.
`Date.now()` and `new Date()` use the real server clock (UTC); timers remain unavailable.

| Method | Input | Result |
|---|---|---|
| `pdw.search` | `{query, priorities?, sources?, since?, mode?, max_results?}` | `{rows, selected_priorities, returned_priority_counts, hint, ...}` |
| `pdw.context` | `{ref, before?, after?, full?}` | `{rows, ref, ...}` |
| `pdw.columns` | `{table}` | `{columns: [{name,type}, ...]}` |
| `pdw.schema` | `{schema?}` | `{rows: [{schema,name}, ...]}` |
| `pdw.sql` | `{sql, question?}` | `{rows, column_names, total_rows, hint, ...}` |

Use short search terms and priority filters as usual. Read returned context before
drawing conclusions. Use a hit's exact ref; never construct it. Inspect columns before
writing unfamiliar SQL; putting guessed SQL after a columns call in a script does not
make the guess valid. SQL supports SELECT and WITH through the same read-only role.
Each operation is a separate live read, not one consistent database snapshot.

```javascript
const hits = await pdw.search({
  query: args.term, priorities: ['self', 'direct', 'cc'], max_results: 3
});
const conversations = await Promise.all(hits.rows.map(hit =>
  pdw.context({ref: hit.ref, before: 2, after: 2, full: true})
));
return {hits: hits.rows, conversations};
```

```javascript
const columns = await pdw.columns({table: 'timeline.events'});
if (!columns.columns.some(c => c.name === 'occurred_at')) {
  throw new Error('Inspect the changed schema before writing SQL');
}
return await pdw.sql({sql: `SELECT source, count(*) AS events
  FROM timeline.events WHERE occurred_at >= now() - interval '1 day'
  AND priority IN ('self','direct','cc') GROUP BY source LIMIT 20` });
```

The envelope has `value`, `run_id`, `operations`, `duration_ms`, `truncated` and `sources`.
`sources` preserves observed refs and priorities even when the script projects them out.
Carry those refs into the answer. Errors have `code`, `message` and optional `hint`;
operation errors throw and can be caught. Caught errors remain in `operations` and usage
telemetry. A top-level error returns an envelope with `error` and a null value.
No results is not evidence of absence: preserve freshness and truncation warnings.

| Limit | Ceiling |
|---|---|
| Code / args | 32 KiB each |
| Wall time | 30s default; 60s maximum |
| Worker CPU / guest memory | 2s cumulative CPU watchdog (3s OS kill); 32 MiB Wasm memory |
| Worker RSS | 128 MiB watchdog; not a cgroup memory reservation |
| Calls | 12 per run, four runs globally, two per token-and-client pair (root uses client name) |
| Parallel reads | Six globally; hybrid search has its own parallel query legs |
| Per-operation query | 15s; at most 1,000 rows and 1 MiB wire data |
| Aggregate rows / data | 5,000 / 4 MiB |
| Final envelope | 128 KiB, including citations |

Limits are errors rather than silent partial results. Narrow an exhausted query; do
not automatically retry the same script. `Promise.all` can overlap independent reads;
it cannot raise server limits. Scripts require Linux sandbox support and the configured
query role and a verified, dedicated unprivileged database login; unsafe or missing
configuration fails closed for both SQL and scripts. Recursion is catchable as QuickJS's
`InternalError: stack overflow`; uncaught recursion returns `stack_limit`.

The all-client pilot is reviewed after one week. Read `marts_ops.script_runs` for daily
runs by outcome (including `busy`, `cpu_limit`, `script_error` and `stack_limit`), inner
calls, errors, bytes and latency, alongside `marts_ops.agent_usage` and
`marts_ops.agent_top_failures`. Count outer round trips and inner reads separately.
Telemetry retains method/error categories and timing, not source code, arguments or results.
