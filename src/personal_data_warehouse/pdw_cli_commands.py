"""The `pdw` CLI's subcommands, as the Go dispatcher (app/cmd/pdw-cli/run.go) has them.

Two Python surfaces read agents' shell commands and must know which `pdw <word>`
is a real command: the agent-usage collector (C3) and the enrichment runner's
tool-call attribution. Each kept its own hand-written list, and when
`pdw context` shipped on 2026-09-22 neither learned about it -- the collector
counted every context read as an invented command for a week.
tests/test_agent_usage.py reads the dispatcher and fails when this drifts.
"""

from __future__ import annotations

#: Questions to the warehouse: the commands a session's first move is judged by.
PDW_CLI_READ_SUBCOMMANDS: tuple[str, ...] = (
    "search", "sql", "schema", "columns", "context", "call", "list", "describe",
)

#: Setup, credentials, local workers and the manual: never a question, so they
#: are left out of the first-call decision and of the denominator.
PDW_CLI_ADMIN_SUBCOMMANDS: tuple[str, ...] = (
    "readme", "help",
    "ingest", "login", "logout", "config", "version", "update",
    "chatgpt", "slack", "whoop", "hn", "heartbeat", "mutations",
)

#: Flag spellings run() accepts in place of a subcommand (help and version).
PDW_CLI_FLAG_SPELLINGS: tuple[str, ...] = ("--help", "-h", "--version", "-version", "-v")
