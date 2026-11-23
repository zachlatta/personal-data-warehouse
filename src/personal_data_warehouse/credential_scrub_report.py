"""Read-only credential count over stored conversation rows. Counts only.

The before/after gate for ``scrub_conversation_cards --scope credentials``:
it applies the storage boundary's credential masker to every AI conversation
source table (and, with ``--derived``, the agent rows of timeline_events and
search_chunks) and reports matches by table, column, role and kind. A clean
warehouse reports zero matches. Rows are prefiltered in SQL with the same
superset the scrub uses, then confirmed in Python.

Never prints a matched value. ``--examples`` writes up to N masked, truncated
contexts per kind (with the secret's length and character classes) to a new
0600 file for a hand check of false positives; keep it out of the repository.

    python -m personal_data_warehouse.credential_scrub_report --since 2026-11-16T00:00:00Z
    python -m personal_data_warehouse.credential_scrub_report --derived --examples .factory-scratch/examples.json
"""
from __future__ import annotations

import argparse
import json
import os
import re
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
import multiprocessing

import psycopg2

from personal_data_warehouse.card_prefilter import available_cpus, sql_prefilter
from personal_data_warehouse.credential_redaction import credential_edits
from personal_data_warehouse.postgres import POSTGRES_TABLES, PostgresWarehouse, _AI_CONVERSATION_EVENT_TABLES

DERIVED = ('timeline_events', 'search_chunks')
_KIND = re.compile(r'\[REDACTED:([a-z_]+):[0-9a-f]{6}\]')
CONTEXT = 24


def _shape(secret):
    classes = ''.join(label for label, test in (('A', str.isupper), ('a', str.islower), ('0', str.isdigit))
                      if any(c.isascii() and test(c) for c in secret))
    if any(not (c.isascii() and c.isalnum()) for c in secret):
        classes += '+'
    return f'len={len(secret)} classes={classes}'


def row_matches(row):
    """[(column, kind, masked context, secret shape)] for one row; no raw values."""
    found = []
    for column, value in row.items():
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        if not isinstance(value, str):
            continue
        edits = credential_edits(value)
        if not edits:
            continue
        masked = []
        cursor = 0
        for start, end, replacement in edits:
            masked.append(value[cursor:start])
            masked.append(replacement)
            cursor = end
        masked.append(value[cursor:])
        cleaned = ''.join(masked)
        offset = 0
        for start, end, replacement in edits:
            at = start + offset
            offset += len(replacement) - (end - start)
            context = cleaned[max(0, at - CONTEXT):at + len(replacement) + CONTEXT]
            found.append((column, _KIND.match(replacement)[1], context, _shape(value[start:end])))
    return found


def _scope_sql(table, since, until):
    where, params = [], []
    if table in DERIVED:
        where.append("adapter IN ('agent_session', 'agent_session_turn')")
    if since:
        where.append('occurred_at >= %s::timestamptz')
        params.append(since)
    if until:
        where.append('occurred_at < %s::timestamptz')
        params.append(until)
    return (' AND '.join(where) or 'TRUE'), params


def scan(database_url, schema, *, tables, since=None, until=None, workers=1, page_size=200, examples=0, progress=None):
    warehouse = PostgresWarehouse(database_url, schema=schema, initialize=False)
    warehouse._connection.set_session(readonly=True, autocommit=True)
    connection = psycopg2.connect(warehouse._database_url)
    connection.set_session(readonly=True, autocommit=True)
    started = time.monotonic()
    counts = Counter()
    rows_with = Counter()
    scanned = Counter()
    prefiltered = Counter()
    samples = {}
    matched_rows = Counter()
    session_rows = {}
    pool = ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context('spawn')) if workers > 1 else None
    try:
        for table in tables:
            where, params = _scope_sql(table, since, until)
            keys = POSTGRES_TABLES[table].primary_key
            with connection.cursor() as cursor:
                cursor.execute(warehouse._expand_relations(f'SELECT count(*) FROM @{table} WHERE {where}'), params)
                scanned[table] = cursor.fetchone()[0]
                cursor.execute(warehouse._expand_relations(
                    f'SELECT {",".join(keys)} FROM @{table} candidate WHERE {where} AND '
                    + sql_prefilter('to_jsonb(candidate)::text', scope='credentials')), params)
                identities = cursor.fetchall()
            prefiltered[table] = len(identities)
            for offset in range(0, len(identities), page_size):
                batch = identities[offset:offset + page_size]
                placeholders = ','.join('(' + ','.join('%s' for _ in keys) + ')' for _ in batch)
                with connection.cursor() as cursor:
                    cursor.execute(warehouse._expand_relations(
                        f'SELECT * FROM @{table} WHERE ({",".join(keys)}) IN ({placeholders})'),
                        tuple(v for key in batch for v in key))
                    columns = [d[0] for d in cursor.description]
                    rows = [dict(zip(columns, values)) for values in cursor.fetchall()]
                results = list(pool.map(row_matches, rows, chunksize=8)) if pool else list(map(row_matches, rows))
                for row, found in zip(rows, results, strict=True):
                    if not found:
                        continue
                    if table not in DERIVED:
                        matched_rows[table] += 1
                        session_rows.setdefault(table, {})[row['source'], row['session_id']] = 0
                    role = 'derived' if table in DERIVED else (row.get('role') or 'unknown')
                    for column, kind, context, shape in found:
                        counts[table, role, column, kind] += 1
                        if examples and len(samples.setdefault(kind, [])) < examples:
                            samples[kind].append(dict(table=table, role=role, column=column, context=context, secret=shape))
                    for kind in {kind for _, kind, _, _ in found}:
                        rows_with[table, role, kind] += 1
                if progress:
                    progress(dict(table=table, done=min(offset + page_size, len(identities)), prefiltered=len(identities)))
            # Size the backfill: every row of an affected session is regenerated.
            sessions = sorted(session_rows.get(table, {}))
            for offset in range(0, len(sessions), 500):
                batch = sessions[offset:offset + 500]
                with connection.cursor() as cursor:
                    cursor.execute(warehouse._expand_relations(
                        f'SELECT source, session_id, count(*) FROM @{table} WHERE (source, session_id) IN ('
                        + ','.join('(%s,%s)' for _ in batch) + ') GROUP BY 1, 2'), tuple(v for key in batch for v in key))
                    for source, session, n in cursor.fetchall():
                        session_rows[table][source, session] = n
    finally:
        if pool:
            pool.shutdown(cancel_futures=True)
        connection.close()
        warehouse.close()
    report = dict(
        scope=dict(since=since, until=until, tables=list(tables)),
        rows_scanned=dict(scanned), rows_prefiltered=dict(prefiltered),
        matches=sum(counts.values()),
        source_rows_matched=dict(matched_rows),
        sessions=dict(
            affected=sum(len(v) for v in session_rows.values()),
            canonical_rows=sum(sum(v.values()) for v in session_rows.values()),
            largest_rows=max((n for v in session_rows.values() for n in v.values()), default=0),
            over_10000_rows=sum(n > 10000 for v in session_rows.values() for n in v.values()),
        ),
        seconds=round(time.monotonic() - started, 1),
    )
    by_kind, by_role, by_column, by_table = Counter(), Counter(), Counter(), Counter()
    for (table, role, column, kind), n in counts.items():
        by_kind[kind] += n
        by_role[role] += n
        by_column[column] += n
        by_table[table] += n
    report.update(by_kind=dict(by_kind), by_role=dict(by_role), by_column=dict(by_column), by_table=dict(by_table),
                  rows_by_table_role_kind=[dict(table=t, role=r, kind=k, rows=n) for (t, r, k), n in sorted(rows_with.items())],
                  matches_by_table_role_column_kind=[dict(table=t, role=r, column=c, kind=k, matches=n)
                                                     for (t, r, c, k), n in sorted(counts.items())])
    return report, samples


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--since', help='inclusive occurred_at lower bound (ISO 8601)')
    parser.add_argument('--until', help='exclusive occurred_at upper bound (ISO 8601)')
    parser.add_argument('--table', action='append', choices=(*_AI_CONVERSATION_EVENT_TABLES, *DERIVED),
                        help='repeatable; default every source table')
    parser.add_argument('--derived', action='store_true', help='also count timeline_events and search_chunks agent rows')
    parser.add_argument('--workers', type=int, default=available_cpus())
    parser.add_argument('--examples', help='new 0600 file for masked, truncated contexts')
    parser.add_argument('--examples-per-kind', type=int, default=25)
    parser.add_argument('--database-url-env', default='POSTGRES_DATABASE_URL')
    parser.add_argument('--schema', default=os.environ.get('POSTGRES_SCHEMA', 'public'))
    args = parser.parse_args()
    tables = tuple(args.table or _AI_CONVERSATION_EVENT_TABLES) + (DERIVED if args.derived else ())
    report, samples = scan(os.environ[args.database_url_env], args.schema, tables=tables, since=args.since,
                           until=args.until, workers=max(1, args.workers),
                           examples=args.examples_per_kind if args.examples else 0,
                           progress=lambda p: print(json.dumps(p), file=__import__('sys').stderr, flush=True))
    if args.examples:
        fd = os.open(args.examples, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(fd, 'w') as stream:
            json.dump(samples, stream, indent=1)
    print(json.dumps(report, indent=1))
    if report['matches']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
