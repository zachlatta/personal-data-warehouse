"""Explicit, idempotent maintenance command; dry-run unless --apply is supplied.

Pause conversation writers, timeline sync and search workers during application.
Output contains counts only. This cannot erase historical backups or WAL.
"""
from __future__ import annotations

import argparse
from collections import Counter
import json
import os
import html
from pathlib import Path
from psycopg2.extras import Json

from personal_data_warehouse.card_redaction import card_count, redact_row
from personal_data_warehouse.postgres import PostgresWarehouse, POSTGRES_TABLES, _AI_CONVERSATION_EVENT_TABLES


def scrub(warehouse, *, apply=False, page_size=500):
    counts = Counter()
    affected_sessions = set()
    affected_events = set()
    tables = (*_AI_CONVERSATION_EVENT_TABLES, 'timeline_events', 'search_chunks')
    for table in tables:
        spec = POSTGRES_TABLES[table]
        keys = spec.primary_key
        key_sql = ', '.join(keys)
        cursor = None
        scope = "adapter IN ('agent_session', 'agent_session_turn')" if table in ('timeline_events', 'search_chunks') else 'TRUE'
        while True:
            predicate = f' AND ({key_sql}) > ({", ".join(["%s"] * len(keys))})' if cursor else ''
            rows = warehouse._query_dicts(f'SELECT * FROM @{table} WHERE {scope}{predicate} ORDER BY {key_sql} LIMIT %s', (*cursor, page_size) if cursor else (page_size,))
            if not rows:
                break
            cursor = tuple(rows[-1][k] for k in keys)
            for row in rows:
                # Count occurrences, including duplicate representations in JSON.
                matches = sum(card_count(v if isinstance(v, str) else json.dumps(v)) for v in row.values() if isinstance(v, (str, dict, list)))
                identity = session_identity(row) if table in ('timeline_events', 'search_chunks') else (row['source'], row['session_id'])
                event_key = (row.get('adapter'), row.get('event_id'))
                affected = table in ('timeline_events', 'search_chunks') and (identity in affected_sessions or event_key in affected_events)
                if not matches and not affected:
                    continue
                if table in _AI_CONVERSATION_EVENT_TABLES:
                    affected_sessions.add(identity)
                elif table == 'timeline_events':
                    affected_events.add(event_key)
                month = row['occurred_at'].strftime('%Y-%m')
                counts[(table, month, row['source'], 'rows')] += 1
                counts[(table, month, row['source'], 'matches')] += matches
                if not apply:
                    continue
                where = ' AND '.join(f'{k} = %s' for k in keys)
                params = tuple(row[k] for k in keys)
                if table == 'search_chunks':
                    # The old embedding encodes the sensitive text. Remove it,
                    # even if shared; the normal drain rebuilds any safe users.
                    warehouse._command('DELETE FROM @search_chunk_embeddings WHERE text_sha256 = %s', (row['text_sha256'],))
                    warehouse._command(f'DELETE FROM @search_chunks WHERE {where}', params)
                    continue
                if table == 'timeline_events':
                    # Full-card detection cannot clean a truncated PAN. Remove
                    # the whole derived row and rebuild through the real adapter.
                    warehouse._command(f'DELETE FROM @timeline_events WHERE {where}', params)
                    continue
                cleaned = redact_row(row)
                columns = [k for k in row if cleaned[k] != row[k] and k not in keys]
                assignments = [f'{k} = %s' for k in columns]
                assignments.append('ingested_at = now()')
                warehouse._command(f'UPDATE @{table} SET {", ".join(assignments)} WHERE {where}', tuple(Json(cleaned[k]) if isinstance(cleaned[k], (dict, list)) else cleaned[k] for k in columns) + params)
    if apply:
        # Refresh every event in each affected session, including safe turns
        # whose derived copies were removed. Incremental adapters then rebuild
        # from sanitized content without resetting a global backfill cursor.
        for source, session_id in sorted(affected_sessions):
            from personal_data_warehouse.postgres import _AI_EVENT_TABLE_BY_SOURCE
            table = _AI_EVENT_TABLE_BY_SOURCE.get(source)
            if table:
                warehouse._command(f'UPDATE @{table} SET ingested_at = now() WHERE source = %s AND session_id = %s', (source, session_id))
    return [dict(table=t, month=m, source=s, measure=k, count=n) for (t,m,s,k),n in sorted(counts.items())]


def session_identity(row):
    """Rollups use cwd/account as context; identity comes from source_pk/id."""
    pk = row.get('source_pk')
    if isinstance(pk, str):
        try:
            pk = json.loads(pk)
        except ValueError:
            pk = None
    if isinstance(pk, dict) and pk.get('source') and pk.get('session_id'):
        return pk['source'], pk['session_id']
    event_id = row.get('event_id', '')
    parts = event_id.split('|')
    if len(parts) >= 2 and parts[0] == row.get('source'):
        return parts[0], parts[1]
    # Older turn copies can be identified by context; never use rollup context.
    if row.get('adapter') == 'agent_session_turn':
        parts = row.get('context', '').split('|')
        if len(parts) == 2 and parts[0] == row.get('source'):
            return tuple(parts)
    return None


def month_source_report(before, after=None):
    """One counts-only table, retaining zero-after rows for baseline groups.

    Count source events once; detailed per-copy counts remain a separate audit.
    None means no after-scan was performed, never an assumed zero.
    """
    def aggregate(details):
        result = Counter()
        for row in details:
            if row['table'] in _AI_CONVERSATION_EVENT_TABLES:
                result[row['month'], row['source'], row['measure']] += row['count']
        return result
    first = aggregate(before)
    last = aggregate(after or [])
    groups = sorted({(m,s) for m,s,_ in first.keys() | last.keys()})
    return [dict(month=m, source=s, rows_before=first[m,s,'rows'], matches_before=first[m,s,'matches'],
                 rows_after=last[m,s,'rows'] if after is not None else None,
                 matches_after=last[m,s,'matches'] if after is not None else None) for m,s in groups]


def report_html(rows):
    columns = ('month','source','rows_before','matches_before','rows_after','matches_after')
    output = ['<table><tr>' + ''.join('<th>' + c.replace('_',' ') + '</th>' for c in columns) + '</tr>']
    for row in rows:
        output.append('<tr>' + ''.join('<td>' + html.escape(str(row[c]) if row[c] is not None else 'pending') + '</td>' for c in columns) + '</tr>')
    output.append('</table>')
    return ''.join(output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--report-html', type=Path, help='counts-only table for the private email body')
    args = parser.parse_args()
    warehouse = PostgresWarehouse(os.environ['POSTGRES_DATABASE_URL'])
    try:
        before = scrub(warehouse)
        after = None
        if args.apply:
            # All related copies change atomically. A failed run leaves the
            # original candidates available to the next run's context scan.
            with warehouse._connection:
                scrub(warehouse, apply=True)
            after = scrub(warehouse)
        table = month_source_report(before, after)
        print(json.dumps({'counts_by_month_source': table, 'details_before': before, 'details_after': after}))
        if args.report_html:
            args.report_html.write_text(report_html(table))
    finally:
        warehouse.close()


if __name__ == '__main__':
    main()
