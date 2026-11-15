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


def table_rows(warehouse, table, page_size):
    keys = POSTGRES_TABLES[table].primary_key
    key_sql = ', '.join(keys)
    cursor = None
    scope = "adapter IN ('agent_session', 'agent_session_turn')" if table in ('timeline_events', 'search_chunks') else 'TRUE'
    while True:
        predicate = f' AND ({key_sql}) > ({", ".join(["%s"] * len(keys))})' if cursor else ''
        rows = warehouse._query_dicts(f'SELECT * FROM @{table} WHERE {scope}{predicate} ORDER BY {key_sql} LIMIT %s', (*cursor, page_size) if cursor else (page_size,))
        if not rows:
            return
        cursor = tuple(rows[-1][k] for k in keys)
        yield from rows


def row_matches(row):
    return sum(card_count(v if isinstance(v, str) else json.dumps(v)) for v in row.values() if isinstance(v, (str, dict, list)))


def scrub(warehouse, *, apply=False, page_size=500):
    if apply:
        # Source repair, canonical regeneration, removal of stale copies and
        # embedding cleanup either all commit or all roll back, including when
        # called directly rather than through the CLI.
        with warehouse._connection:
            return _scrub(warehouse, apply=True, page_size=page_size)
    return _scrub(warehouse, apply=False, page_size=page_size)


def _scrub(warehouse, *, apply, page_size):
    counts = Counter()
    affected_sessions = set()
    affected_events = set()
    tables = (*_AI_CONVERSATION_EVENT_TABLES, 'timeline_events', 'search_chunks')
    # Discover BEFORE changing anything: a match may exist only in a derived
    # copy, after its source was cleaned without advancing an old watermark.
    for table in tables:
        for row in table_rows(warehouse, table, page_size):
            if not row_matches(row):
                continue
            identity = session_identity(row) if table in ('timeline_events', 'search_chunks') else (row['source'], row['session_id'])
            if identity:
                affected_sessions.add(identity)
            elif apply:
                raise RuntimeError('cannot identify canonical session for a derived match; no changes committed')
    old_timeline = []
    chunks = []
    for table in tables:
        for row in table_rows(warehouse, table, page_size):
            matches = row_matches(row)
            identity = session_identity(row) if table in ('timeline_events', 'search_chunks') else (row['source'], row['session_id'])
            event_key = (row.get('adapter'), row.get('event_id'))
            affected = table in ('timeline_events', 'search_chunks') and (identity in affected_sessions or event_key in affected_events)
            if not matches and not affected:
                continue
            month = row['occurred_at'].strftime('%Y-%m')
            counts[(table, month, row['source'], 'rows')] += 1
            counts[(table, month, row['source'], 'matches')] += matches
            if table == 'timeline_events':
                affected_events.add(event_key)
                old_timeline.append((event_key, identity))
            elif table == 'search_chunks':
                chunks.append((row['chunk_id'], row['text_sha256'], event_key, identity))
            elif apply:
                keys = POSTGRES_TABLES[table].primary_key
                cleaned = redact_row(row)
                columns = [k for k in row if cleaned[k] != row[k] and k not in keys]
                if columns:
                    where = ' AND '.join(f'{k} = %s' for k in keys)
                    warehouse._command(f'UPDATE @{table} SET {", ".join(f"{k} = %s" for k in columns)} WHERE {where}', tuple(Json(cleaned[k]) if isinstance(cleaned[k], (dict, list)) else cleaned[k] for k in columns) + tuple(row[k] for k in keys))
    if apply and affected_sessions:
        new_keys, regenerated_keys = rebuild_sessions(warehouse, affected_sessions, page_size)
        if any((*key, identity) not in regenerated_keys for key, identity in old_timeline) or any((*key, identity) not in regenerated_keys for _, _, key, identity in chunks):
            raise RuntimeError('canonical event missing for derived copy; no changes committed')
        # Existing canonical keys are rewritten in place. Delete a stale key
        # only after the adapters successfully generated its canonical session.
        for (adapter, event_id), _ in old_timeline:
            if (adapter, event_id) not in new_keys:
                warehouse._command('DELETE FROM @timeline_events WHERE adapter = %s AND event_id = %s', (adapter, event_id))
        for chunk_id, sha, _, _ in chunks:
            warehouse._command('DELETE FROM @search_chunk_embeddings WHERE text_sha256 = %s', (sha,))
            warehouse._command('DELETE FROM @search_chunks WHERE chunk_id = %s', (chunk_id,))
    return [dict(table=t, month=m, source=s, measure=k, count=n) for (t,m,s,k),n in sorted(counts.items())]


def rebuild_sessions(warehouse, sessions, page_size):
    from personal_data_warehouse.postgres import _AI_EVENT_TABLE_BY_SOURCE
    from personal_data_warehouse.timeline import TimelineSyncEngine, adapter_by_name

    stamp = warehouse._query('SELECT now()')[0][0]
    for source, session_id in sorted(sessions):
        table = _AI_EVENT_TABLE_BY_SOURCE.get(source)
        if table is None:
            raise RuntimeError('unknown canonical session source; no changes committed')
        warehouse._command(f'UPDATE @{table} SET ingested_at = %s WHERE source = %s AND session_id = %s', (stamp, source, session_id))
    # Borrow the SAME transaction for both canonical adapter reads and writes.
    # Do not connect/close the engine: this helper owns no extra connection,
    # changes no global adapter watermark and runs no independent backfill.
    engine = TimelineSyncEngine(source_url=warehouse._database_url, source_schema=warehouse._schema, dest_schema=warehouse._schema)
    engine._source_conn = engine._dest_conn = warehouse._connection
    keys = set()
    regenerated_keys = set()
    rebuilt_sessions = set()
    for name in ('agent_session', 'agent_session_turn'):
        adapter = adapter_by_name(name)
        watermark_ts, watermark_id = stamp, ''
        while True:
            rows = engine._fetch(adapter.incremental_sql, dict(watermark_ts=watermark_ts, watermark_id=watermark_id, limit=page_size))
            if not rows:
                break
            selected = []
            for row in rows:
                identity = session_identity(dict(source_pk=row[9]))
                if identity not in sessions:
                    continue
                if any(card_count(str(row[i])) for i in (6, 7, 10, 11)):
                    raise RuntimeError('canonical regeneration still contains card data; no changes committed')
                selected.append(row)
                keys.add((name, row[0]))
                regenerated_keys.add((name, row[0], identity))
                # Some legacy turn copies used event_uuid as event_id. Their
                # replacement is guaranteed only if that exact source event
                # also appeared in the canonical adapter output.
                if name == 'agent_session_turn':
                    pk = json.loads(row[9]) if isinstance(row[9], str) else row[9]
                    if pk.get('event_uuid'):
                        regenerated_keys.add((name, pk['event_uuid'], identity))
                if name == 'agent_session':
                    rebuilt_sessions.add(identity)
            engine._upsert(adapter, selected)
            watermark_ts, watermark_id = rows[-1][12], rows[-1][0]
    if rebuilt_sessions != sessions:
        raise RuntimeError('canonical source missing for affected session; no changes committed')
    # A chunk-only leak can leave canonical timeline content unchanged. The
    # normal content guard correctly skips that upsert, so explicitly enqueue
    # regenerated keys for the chunk builder before removing the old chunks.
    for adapter, event_id in sorted(keys):
        warehouse._command('UPDATE @timeline_events SET seq = nextval(%s::regclass), updated_at = now() WHERE adapter = %s AND event_id = %s',
                           (warehouse.sql_relation('timeline_events_seq'), adapter, event_id))
    return keys, regenerated_keys

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
