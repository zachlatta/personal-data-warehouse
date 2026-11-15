"""Explicit, idempotent maintenance command; dry-run unless --apply is supplied.

Pause conversation writers, timeline sync and search workers during application.
Output contains counts only. This cannot erase historical backups or WAL.
"""
from __future__ import annotations

import argparse
from collections import Counter
import json
import os
from psycopg2.extras import Json

from personal_data_warehouse.card_redaction import card_count, redact_row
from personal_data_warehouse.postgres import PostgresWarehouse, POSTGRES_TABLES, _AI_CONVERSATION_EVENT_TABLES


def scrub(warehouse, *, apply=False, page_size=500):
    counts = Counter()
    affected_contexts = set()
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
                context = row.get('context', '')
                affected = table in ('timeline_events', 'search_chunks') and context in affected_contexts
                if not matches and not affected:
                    continue
                if table in _AI_CONVERSATION_EVENT_TABLES:
                    affected_contexts.add(f"{row['source']}|{row['session_id']}")
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
                cleaned = redact_row(row)
                if affected:
                    from personal_data_warehouse.card_redaction import redact
                    cleaned = {k: redact(v, card_context=True) if isinstance(v, str) else v for k, v in row.items()}
                columns = [k for k in row if cleaned[k] != row[k] and k not in keys]
                assignments = [f'{k} = %s' for k in columns]
                extra = ()
                if table == 'timeline_events':
                    assignments.append('seq = nextval(%s::regclass)')
                    extra = (warehouse.sql_relation('timeline_events_seq'),)
                else:
                    assignments.append('ingested_at = now()')
                warehouse._command(f'UPDATE @{table} SET {", ".join(assignments)} WHERE {where}', tuple(Json(cleaned[k]) if isinstance(cleaned[k], (dict, list)) else cleaned[k] for k in columns) + extra + params)
    return [dict(table=t, month=m, source=s, measure=k, count=n) for (t,m,s,k),n in sorted(counts.items())]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    warehouse = PostgresWarehouse(os.environ['POSTGRES_DATABASE_URL'])
    try:
        print(json.dumps({'before': scrub(warehouse)}))
        if args.apply:
            # All related copies change atomically. A failed run leaves the
            # original candidates available to the next run's context scan.
            with warehouse._connection:
                scrub(warehouse, apply=True)
            print(json.dumps({'after': scrub(warehouse)}))
    finally:
        warehouse.close()


if __name__ == '__main__':
    main()
