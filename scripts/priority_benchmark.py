"""Priority benchmark: private labels, checked proposed SQL, publication and measurement."""

import argparse
import json
from pathlib import Path

from personal_data_warehouse import priority_benchmark as benchmark
from personal_data_warehouse.relations import expand_relations
from personal_data_warehouse.timeline import adapter_by_name, gmail_correspondents_sql


def replace_required(sql, target, replacement, *, count=None):
    found = sql.count(target)
    if not found or (count is not None and found != count):
        raise ValueError(
            f'Adapter SQL shape changed: expected {count or "at least one"} occurrence(s) of {target!r}, found {found}'
        )
    return sql.replace(target, replacement)


def values(keys):
    return ",".join("(" + ",".join(map(benchmark.quote, key)) + ")" for key in keys)


def proposed_sql(payload, *, output="summary", namespace="public"):
    benchmark.validate_payload(payload)
    supported = {"agent_session", "agent_session_turn", "slack_message", "gmail_email"}
    unsupported = set(payload["requirements"]) - supported
    if unsupported:
        raise ValueError(
            "No proposed adapter SQL for: " + ", ".join(sorted(unsupported))
        )
    labels = payload["labels"]
    params = dict(cursor_ts="'9999-01-01'::timestamptz", cursor_id="'~'", limit=100000)
    queries, ctes = [], []
    sessions = sorted(
        {
            tuple(r["event_id"].split("|")[:2])
            for r in labels
            if r["adapter"] in ("agent_session", "agent_session_turn")
        }
    )
    if sessions:
        sql = (
            replace_required(
                adapter_by_name("agent_session").backfill_sql,
                "FROM @ai_conversation_events e\n",
                f"FROM @ai_conversation_events e JOIN (VALUES {values(sessions)}) wanted(source,session_id) "
                "ON e.source=wanted.source AND e.session_id=wanted.session_id\n",
                count=1,
            )
            % params
        )
        ctes.append("proposed_sessions AS MATERIALIZED (" + sql + ")")
        queries.append(
            "SELECT 'agent_session'::text adapter,event_id,priority::text FROM proposed_sessions"
        )
    for name, table, fields in (
        (
            "slack_message",
            "slack_messages",
            ("account", "team_id", "conversation_id", "message_ts"),
        ),
        ("gmail_email", "gmail_messages", ("account", "message_id")),
    ):
        selected = sorted(
            {tuple(r["event_id"].split("|")) for r in labels if r["adapter"] == name}
        )
        if not selected:
            continue
        predicate = " AND ".join(f"original.{f}=wanted.{f}" for f in fields)
        bound = f'(SELECT original.* FROM @{table} original JOIN (VALUES {values(selected)}) wanted({",".join(fields)}) ON {predicate}) t'
        sql = (
            replace_required(
                adapter_by_name(name).backfill_sql, f"@{table} t", bound, count=1
            )
            % params
        )
        if name == "gmail_email":
            ctes += [
                "benchmark_accounts AS MATERIALIZED (SELECT DISTINCT account FROM @gmail_messages)",
                "benchmark_correspondents AS MATERIALIZED ("
                + gmail_correspondents_sql("benchmark_accounts") % {}
                + ")",
            ]
            sql = replace_required(sql, "@gmail_sync_state", "benchmark_accounts")
            sql = replace_required(
                sql, "@timeline_gmail_correspondents", "benchmark_correspondents"
            )
        queries.append(
            f"SELECT '{name}'::text adapter,event_id,priority::text FROM ({sql}) normalized"
        )
    selected = sorted(
        {
            tuple(r["event_id"].split("|"))
            for r in labels
            if r["adapter"] == "agent_session_turn"
        }
    )
    if selected:
        sql = replace_required(
            adapter_by_name("agent_session_turn").backfill_sql,
            "FROM @ai_conversation_events e",
            f"FROM @ai_conversation_events e JOIN (VALUES {values(selected)}) wanted(source,session_id,seq) "
            "ON e.source=wanted.source AND e.session_id=wanted.session_id AND e.seq=wanted.seq::bigint",
            count=1,
        )
        sql = replace_required(
            sql,
            "LEFT JOIN @timeline_events sess",
            "LEFT JOIN proposed_sessions sess",
            count=1,
        )
        sql = (
            replace_required(
                sql, "ON sess.adapter = 'agent_session'", "ON TRUE", count=1
            )
            % params
        )
        queries.append(
            f"SELECT 'agent_session_turn'::text adapter,event_id,priority::text FROM ({sql}) normalized"
        )
    ctes.append("proposed AS (" + " UNION ALL ".join(queries) + ")")
    return expand_relations(
        benchmark.benchmark_sql(
            payload, actual_relation="proposed", ctes=ctes, output=output
        ),
        namespace=namespace,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("sql", "publish-labels", "pull-labels", "measure")
    )
    parser.add_argument("--labels", type=Path, default=benchmark.DEFAULT_LABELS)
    parser.add_argument("--proposed", action="store_true")
    parser.add_argument(
        "--output", choices=("summary", "rows", "details"), default="summary"
    )
    args = parser.parse_args()
    if args.command == "sql":
        payload = benchmark.load_labels(args.labels)
        print(
            proposed_sql(payload, output=args.output)
            if args.proposed
            else expand_relations(benchmark.benchmark_sql(payload, output=args.output))
        )
        return
    from personal_data_warehouse.config import load_settings
    from personal_data_warehouse.warehouse import warehouse_from_settings

    warehouse = warehouse_from_settings(load_settings(require_gmail=False))
    try:
        if args.command != "pull-labels":
            warehouse.ensure_priority_benchmark_tables()
        if args.command == "publish-labels":
            print(
                "Published labels:",
                benchmark.publish_labels(warehouse, benchmark.load_labels(args.labels)),
            )
        elif args.command == "pull-labels":
            path = benchmark.assert_private_path(args.labels)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(benchmark.pull_labels(warehouse), indent=2) + "\n"
            )
        else:
            benchmark.measure(warehouse)
            print(
                json.dumps(
                    warehouse._query_dicts(
                        "SELECT * FROM @marts_timeline_priority_correctness"
                    ),
                    default=str,
                )
            )
    finally:
        warehouse.close()


if __name__ == "__main__":
    main()
