"""Explicit, idempotent maintenance command; dry-run unless --apply is supplied.

Discovery is read-only and runs while workers remain live. Save its private
ID/count plan with --plan, then reuse it with --apply. Pause conversation
writers, timeline sync and search workers only during application.
Output contains counts only. This cannot erase historical backups or WAL.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
import os
import html
from pathlib import Path
from psycopg2.extras import Json, execute_batch
import psycopg2

from personal_data_warehouse.card_scrub_selector import ScrubSelector, resolve_selector, add_selector_arguments
from personal_data_warehouse.card_redaction import card_count, is_card, redact_row
from personal_data_warehouse.postgres import PostgresWarehouse, POSTGRES_TABLES, _AI_CONVERSATION_EVENT_TABLES
from personal_data_warehouse.card_prefilter import ConfirmationPool, available_cpus, count_row, sql_prefilter


TABLES = (*_AI_CONVERSATION_EVENT_TABLES, 'timeline_events', 'search_chunks')
DERIVED = ('timeline_events', 'search_chunks')
row_matches = count_row


def _scope(warehouse):
    row = warehouse._query('SELECT current_database(), inet_server_addr()::text, inet_server_port()')[0]
    return [*row, warehouse._schema]


def _details(candidates):
    counts = Counter()
    for item in candidates:
        counts[item['table'],item['month'],item['source'],'rows'] += 1
        counts[item['table'],item['month'],item['source'],'matches'] += item['matches']
    return [dict(table=t,month=m,source=s,measure=k,count=n) for (t,m,s,k),n in sorted(counts.items())]


def _scan_table(warehouse, table, pool, page_size, connection=None, snapshot=None, blocks=None):
    owned = connection is None
    conn = connection or psycopg2.connect(warehouse._database_url)
    try:
        if owned:
            conn.set_session(readonly=True,autocommit=not bool(snapshot),isolation_level='REPEATABLE READ' if snapshot else 'READ COMMITTED')
        with conn.cursor() as cursor:
            if snapshot:cursor.execute('SET TRANSACTION SNAPSHOT %s',(snapshot,))
            # A normal cursor allows PostgreSQL parallel plans; DECLARE/server
            # cursors and keyset LIMIT/ORDER scans suppress that parallelism.
            cursor.execute('SET max_parallel_workers_per_gather = %s', (pool.workers if pool.workers > 1 and not blocks else 0,))
            if pool.workers > 1 and not blocks:
                cursor.execute('SET min_parallel_table_scan_size=0')
                cursor.execute('SET parallel_setup_cost=0')
                cursor.execute('SET parallel_tuple_cost=0')
            scope = "candidate.adapter IN ('agent_session', 'agent_session_turn')" if table in DERIVED else 'TRUE'
            predicate = sql_prefilter('to_jsonb(candidate)::text')
            keys=POSTGRES_TABLES[table].primary_key
            key_sql=','.join(keys)
            bounded = ' AND candidate.ctid >= %s::tid AND candidate.ctid < %s::tid' if blocks else ''
            params=(f'({blocks[0]},0)',f'({blocks[1]},0)') if blocks else None
            # Buffer only IDs, never every prefiltered payload. Fetch matching
            # content in bounded PK batches using this same MVCC snapshot.
            cursor.execute(warehouse._expand_relations(f'SELECT {key_sql} FROM @{table} candidate WHERE {scope} AND ({predicate}){bounded}'),params)
            result = []
            while values := cursor.fetchmany(page_size):
                placeholders=','.join('('+','.join('%s' for _ in keys)+')' for _ in values)
                rows=_read_on(warehouse,conn,f'SELECT * FROM @{table} WHERE ({key_sql}) IN ({placeholders})',tuple(v for key in values for v in key))
                for row,matches in zip(rows,pool.counts(rows),strict=True):
                    if not matches: continue
                    identity = session_identity(row) if table in DERIVED else (row['source'],row['session_id'])
                    result.append(dict(table=table,key=[row[k] for k in POSTGRES_TABLES[table].primary_key],
                                       session=list(identity) if identity else None,month=row['occurred_at'].strftime('%Y-%m'),
                                       source=row['source'],matches=matches))
            return result
    finally:
        if owned: conn.close()


def _read_on(warehouse,connection,sql,params=None):
    with connection.cursor() as cursor:
        cursor.execute(warehouse._expand_relations(sql),params)
        columns=[d[0] for d in cursor.description]
        return [dict(zip(columns,row,strict=True)) for row in cursor.fetchall()]


def _watermarks(warehouse,connection):
    timeline=_read_on(warehouse,connection,'SELECT COALESCE(max(seq),0) AS value FROM @timeline_events')[0]['value']
    chunks=_read_on(warehouse,connection,"SELECT COALESCE(max(built_at),'1970-01-01'::timestamptz)::text AS value FROM @search_chunks")[0]['value']
    return {'timeline_events':timeline,'search_chunks':chunks}


def _attach_related(warehouse,candidates,connection):
    indexed={(c['table'],tuple(c['key'])):c for c in candidates}
    sessions={tuple(c['session']) for c in candidates if c['session']}
    event_keys={tuple(c['key']) for c in candidates if c['table']=='timeline_events'}
    for table in DERIVED:
        if not sessions and not event_keys:break
        for row in _related_rows(warehouse,table,sessions,event_keys,connection=connection,ids_only=True,lock=False):
            key=[row[k] for k in POSTGRES_TABLES[table].primary_key]
            identity=session_identity(row)
            indexed.setdefault((table,tuple(key)),dict(table=table,key=key,session=list(identity) if identity else None,
                month=row['occurred_at'].strftime('%Y-%m'),source=row['source'],matches=0))
            event_keys.add((row['adapter'],row['event_id']))
    return list(indexed.values())


def discover(warehouse, *, workers=1, page_size=500, connection=None, include_related=True, selector=None):
    """One read-only discovery pass. CLI defaults to all available host CPUs.

    Table scans run concurrently and allow server-side parallel scans; only
    prefiltered rows cross memory-only IPC to spawned Python confirmations.
    Supplying the existing connection verifies uncommitted application changes.
    """
    with ConfirmationPool(workers, selector) as pool:
        if connection is not None:
            watermarks=_watermarks(warehouse,connection)
            candidates = [item for table in TABLES for item in _scan_table(warehouse,table,pool,page_size,connection)]
            if include_related:candidates=_attach_related(warehouse,candidates,connection)
        elif workers == 1:
            conn=psycopg2.connect(warehouse._database_url)
            try:
                conn.set_session(readonly=True,autocommit=True)
                watermarks=_watermarks(warehouse,conn)
                candidates=[item for table in TABLES for item in _scan_table(warehouse,table,pool,page_size,conn)]
                if include_related:candidates=_attach_related(warehouse,candidates,conn)
            finally:conn.close()
        else:
            # Physical non-overlapping block ranges use all host CPUs even if
            # PostgreSQL's background-worker ceiling is lower. Export one live
            # read-only snapshot so tuple movement cannot hide a row between
            # range reads. Views/small tables use normal parallel SELECTs.
            coordinator=psycopg2.connect(warehouse._database_url)
            try:
                coordinator.set_session(readonly=True,isolation_level='REPEATABLE READ',autocommit=False)
                with coordinator.cursor() as cursor:
                    cursor.execute('SELECT pg_export_snapshot()');snapshot=cursor.fetchone()[0]
                    watermarks=_watermarks(warehouse,coordinator)
                    jobs=[]
                    for table in TABLES:
                        cursor.execute("SELECT COALESCE(pg_relation_size(%s::regclass),0)/current_setting('block_size')::int",(warehouse.sql_relation(table),))
                        pages=cursor.fetchone()[0]
                        if pages >= workers*128:
                            size=(pages+workers-1)//workers
                            jobs.extend((table,(start,min(start+size,pages))) for start in range(0,pages,size))
                        else:jobs.append((table,None))
                with ThreadPoolExecutor(max_workers=workers) as scans:
                    groups=scans.map(lambda job:_scan_table(warehouse,job[0],pool,page_size,snapshot=snapshot,blocks=job[1]),jobs)
                    candidates=[item for group in groups for item in group]
                if include_related:candidates=_attach_related(warehouse,candidates,coordinator)
            finally:coordinator.close()
    plan = dict(version=2,scope=_scope(warehouse),watermarks=watermarks,candidates=candidates,counts=_details(candidates))
    if selector and selector.scope != "all":
        plan.update(selector=selector.specification,left_alone=pool.left_alone)
    return plan


def write_plan(path, plan):
    _validate_plan(plan)
    fd = os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
    with os.fdopen(fd,'w') as stream:
        json.dump(plan,stream)


def read_plan(path):
    plan = json.loads(path.read_text())
    _validate_plan(plan)
    return plan


def _validate_plan(plan):
    if "selector" in plan:
        from personal_data_warehouse.card_scrub_selector import validate_specification
        validate_specification(plan["selector"])
        if type(plan.get("left_alone")) is not int or plan["left_alone"] < 0: raise ValueError("invalid left-alone count")
    if set(plan) - {"selector","left_alone"} != {'version','scope','watermarks','candidates','counts'} or plan['version'] != 2:
        raise ValueError('invalid discovery plan')
    if (set(plan['watermarks']) != set(DERIVED) or type(plan['watermarks']['timeline_events']) is not int or
            not isinstance(plan['watermarks']['search_chunks'],str)):
        raise ValueError('invalid discovery cursors')
    for item in plan['candidates']:
        if (set(item) != {'table','key','session','month','source','matches'} or item['table'] not in TABLES or
                len(item['key']) != len(POSTGRES_TABLES[item['table']].primary_key)):
            raise ValueError('invalid candidate identity')
        if type(item['matches']) is not int or item['matches']<0 or not isinstance(item['month'],str) or not isinstance(item['source'],str):
            raise ValueError('invalid discovery counts')
        if item['session'] is not None and (not isinstance(item['session'],list) or len(item['session'])!=2):
            raise ValueError('invalid session identity')
        identifiers = item['key'] + (item['session'] or [])
        if any(not isinstance(v,(str,int)) or isinstance(v,bool) or isinstance(v,str) and is_card(v) for v in identifiers):
            raise ValueError('sensitive or invalid discovery identity; no plan written')
    if plan['counts'] != _details(plan['candidates']):raise ValueError('discovery counts do not match identities')
    identities={(c['table'],tuple(c['key'])) for c in plan['candidates']}
    if len(identities)!=len(plan['candidates']):raise ValueError('duplicate discovery identities')


def _related_rows(warehouse, table, sessions, event_keys, *, connection=None,ids_only=False,lock=True,delta=None):
    # Identity follows session_identity(), including legacy turn contexts;
    # rollup context is NEVER used as a session identifier.
    identity = """CASE
      WHEN NULLIF(source_pk::jsonb->>'source','') IS NOT NULL AND NULLIF(source_pk::jsonb->>'session_id','') IS NOT NULL
        THEN jsonb_build_array(source_pk::jsonb->>'source',source_pk::jsonb->>'session_id')
      WHEN strpos(event_id,'|') > 0 AND split_part(event_id,'|',1)=source
        THEN jsonb_build_array(source,split_part(event_id,'|',2))
      WHEN adapter='agent_session_turn' AND cardinality(string_to_array(context,'|'))=2 AND split_part(context,'|',1)=source
        THEN jsonb_build_array(source,split_part(context,'|',2)) END"""
    if table == 'search_chunks': identity = identity.replace('source_pk',"'{}'::jsonb")
    columns='*'
    if ids_only:
        columns=','.join(sorted(set(POSTGRES_TABLES[table].primary_key)|{'adapter','event_id','source','context','occurred_at'}|({'source_pk'} if table=='timeline_events' else set())))
    params=([json.dumps(s) for s in sorted(sessions)],[json.dumps(k) for k in sorted(event_keys)])
    since=''
    if delta is not None:
        since=' AND seq > %s' if table=='timeline_events' else ' AND built_at >= %s::timestamptz'
        params+= (delta[table],)
    return _read_on(warehouse,connection or warehouse._connection,
        f"SELECT {columns} FROM @{table} WHERE adapter IN ('agent_session','agent_session_turn') AND (({identity}) = ANY(%s::jsonb[]) OR jsonb_build_array(adapter,event_id) = ANY(%s::jsonb[])){since}"+(' FOR UPDATE' if lock else ''),params)


def scrub(warehouse, *, apply=False, page_size=500, workers=1, plan=None, selector=None):
    plan = plan if plan is not None else discover(warehouse,workers=workers,page_size=page_size,selector=selector)
    _validate_plan(plan)
    if plan.get("selector", ScrubSelector().specification) != (selector or ScrubSelector()).specification:
        raise ValueError("plan selector differs from current selector")
    if plan['scope'] != _scope(warehouse): raise ValueError('discovery plan belongs to another warehouse')
    if not apply: return plan['counts']
    with warehouse._connection:
        _apply_plan(warehouse,plan,page_size,workers,selector)
        # Fast full verification sees this transaction's sanitized rows. New
        # candidates outside the saved plan cause rollback, never a false zero.
        if discover(warehouse,workers=workers,page_size=page_size,connection=warehouse._connection,include_related=False,selector=selector)['candidates']:
            raise RuntimeError('new candidates require discovery; no changes committed')
    return plan['counts']


def _apply_plan(warehouse, plan, page_size, workers, selector=None):
    sessions = {tuple(c['session']) for c in plan['candidates'] if c['session']}
    current = {}
    for item in plan['candidates']:
        table = item['table']; keys = POSTGRES_TABLES[table].primary_key
        rows = warehouse._query_dicts(f'SELECT * FROM @{table} WHERE '+ ' AND '.join(f'{k}=%s' for k in keys)+' FOR UPDATE',item['key'])
        for row in rows: current[table,tuple(row[k] for k in keys)] = row
    for (table,_),row in current.items():
        identity=session_identity(row) if table in DERIVED else (row['source'],row['session_id'])
        if identity:sessions.add(identity)
    event_keys = {(row['adapter'],row['event_id']) for (table,_),row in current.items() if table in DERIVED}
    for table in DERIVED:
        if not sessions and not event_keys: break
        for row in _related_rows(warehouse,table,sessions,event_keys,delta=plan['watermarks']):
            current[table,tuple(row[k] for k in POSTGRES_TABLES[table].primary_key)] = row
            event_keys.add((row['adapter'],row['event_id']))
    active = set()
    # A live re-ingest can clean a source after discovery while leaving only
    # truncated PAN/CVV copies. Historical candidate context still requires
    # canonical regeneration for related copies even if their full count is 0.
    for (table,_),row in current.items():
        if table in DERIVED:
            identity=session_identity(row)
            if identity in sessions:active.add(identity)
    updates={}
    with ConfirmationPool(workers, selector) as pool:
        sources=[]
        for ((table,_),row),matches in zip(current.items(),pool.counts(list(current.values())),strict=True):
            if not matches: continue
            identity = session_identity(row) if table in DERIVED else (row['source'],row['session_id'])
            if not identity: raise RuntimeError('cannot identify canonical session for a derived match; no changes committed')
            active.add(identity)
            if table not in DERIVED:
                sources.append((table,row))
        for offset in range(0,len(sources),page_size):
            batch=sources[offset:offset+page_size]
            for (table,row),cleaned in zip(batch,pool.redacted([row for _,row in batch]),strict=True):
                keys=POSTGRES_TABLES[table].primary_key
                columns = [k for k in row if cleaned[k] != row[k] and k not in keys]
                if columns:
                    params=tuple(Json(cleaned[k]) if isinstance(cleaned[k],(dict,list)) else cleaned[k] for k in columns)+tuple(row[k] for k in keys)
                    updates.setdefault((table,tuple(columns)),[]).append(params)
    for (table,columns),params in updates.items():
        keys=POSTGRES_TABLES[table].primary_key
        sql=f'UPDATE @{table} SET '+','.join(f'{k}=%s' for k in columns)+' WHERE '+' AND '.join(f'{k}=%s' for k in keys)
        with warehouse._connection.cursor() as cursor:
            execute_batch(cursor,warehouse._expand_relations(sql),params,page_size=100)
    if not active: return
    old_timeline = []; chunks = []
    for (table,_),row in current.items():
        if table not in DERIVED: continue
        identity = session_identity(row)
        if identity not in active: continue
        key = (row['adapter'],row['event_id'])
        if table == 'timeline_events': old_timeline.append((key,identity))
        else: chunks.append((row['chunk_id'],row['text_sha256'],key,identity))
    new_keys, regenerated = rebuild_sessions(warehouse,active,page_size,selector)
    if any((*key,identity) not in regenerated for key,identity in old_timeline) or any((*key,identity) not in regenerated for _,_,key,identity in chunks):
        raise RuntimeError('canonical event missing for derived copy; no changes committed')
    for (adapter,event_id),_ in old_timeline:
        if (adapter,event_id) not in new_keys:
            warehouse._command('DELETE FROM @timeline_events WHERE adapter=%s AND event_id=%s',(adapter,event_id))
    for chunk_id,sha,_,_ in chunks:
        warehouse._command('DELETE FROM @search_chunk_embeddings WHERE text_sha256=%s',(sha,))
        warehouse._command('DELETE FROM @search_chunks WHERE chunk_id=%s',(chunk_id,))


def rebuild_sessions(warehouse, sessions, page_size, selector=None):
    from personal_data_warehouse.postgres import _AI_EVENT_TABLE_BY_SOURCE
    from personal_data_warehouse.timeline import TimelineSyncEngine, adapter_by_name

    stamp = warehouse._query('SELECT now()')[0][0]
    if any(source not in _AI_EVENT_TABLE_BY_SOURCE for source,_ in sessions):
        raise RuntimeError('unknown canonical session source; no changes committed')
    # A transaction-local view scopes the existing canonical SQL to planned
    # sessions and presents a synthetic ingestion watermark. Do not UPDATE
    # every source neighbour merely to trigger adapter reads (or generate WAL).
    with warehouse._connection.cursor() as cursor:
        cursor.execute(warehouse._expand_relations('SELECT * FROM @ai_conversation_events LIMIT 0'))
        columns=[d[0] for d in cursor.description]
    select=','.join('%s::timestamptz AS ingested_at' if c=='ingested_at' else f'e."{c}"' for c in columns)
    ordered=sorted(sessions)
    where=','.join('(%s,%s)' for _ in ordered)
    warehouse._command('CREATE OR REPLACE TEMP VIEW card_scrub_events AS SELECT '+select+
                       ' FROM @ai_conversation_events e WHERE (e.source,e.session_id) IN ('+where+')',
                       (stamp,)+tuple(v for pair in ordered for v in pair))
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
            sql=adapter.incremental_sql.replace('@ai_conversation_events','pg_temp.card_scrub_events')
            rows = engine._fetch(sql, dict(watermark_ts=watermark_ts, watermark_id=watermark_id, limit=page_size))
            if not rows:
                break
            selected = []
            for row in rows:
                identity = session_identity(dict(source_pk=row[9]))
                if identity not in sessions:
                    continue
                if any((selector.count if selector else card_count)(str(row[i])) for i in (6, 7, 10, 11)):
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
    ordered=sorted(keys)
    for offset in range(0,len(ordered),500):
        batch=ordered[offset:offset+500]
        warehouse._command('UPDATE @timeline_events SET seq = nextval(%s::regclass), updated_at = now() WHERE (adapter,event_id) IN ('+
                           ','.join('(%s,%s)' for _ in batch)+')',
                           (warehouse.sql_relation('timeline_events_seq'),)+tuple(v for key in batch for v in key))
    warehouse._command('DROP VIEW pg_temp.card_scrub_events')
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
    parser.add_argument('--verify', action='store_true', help='read-only fast verification without writing a plan')
    parser.add_argument('--plan', type=Path, help='private ID-only discovery plan; new file for discovery, existing file for apply')
    parser.add_argument('--workers', type=int, default=available_cpus(), help='Python confirmation processes and concurrent SQL scan budget')
    parser.add_argument('--report-html', type=Path, help='counts-only table for the private email body')
    add_selector_arguments(parser)
    args = parser.parse_args()
    if args.workers < 1: parser.error('--workers must be positive')
    if args.apply and args.verify: parser.error('--apply and --verify are mutually exclusive')
    if not args.verify and args.plan is None: parser.error('--plan is required for discovery and apply')
    warehouse = PostgresWarehouse(os.environ['POSTGRES_DATABASE_URL'],initialize=False)
    warehouse._connection.set_session(readonly=not args.apply,autocommit=True)
    try:
        selector = resolve_selector(warehouse,args.scope,args.seed_row)
        after = None
        if args.apply:
            plan = read_plan(args.plan)
            before = plan['counts']
            scrub(warehouse,apply=True,plan=plan,workers=args.workers,selector=selector)
            after = [] # Verified against this transaction before committing.
        elif args.verify:
            if args.plan:
                baseline = read_plan(args.plan)
                if baseline.get('selector',ScrubSelector().specification) != selector.specification:
                    raise ValueError('plan selector differs from current selector')
            after = discover(warehouse,workers=args.workers,selector=selector)['counts']
            before = baseline['counts'] if args.plan else after
        else:
            plan = discover(warehouse,workers=args.workers,selector=selector)
            write_plan(args.plan,plan)
            before = plan['counts']
        table = month_source_report(before, after)
        print(json.dumps({'counts_by_month_source': table, 'details_before': before, 'details_after': after, 'left_alone': plan.get('left_alone') if not args.verify else None}))
        if args.report_html:
            args.report_html.write_text(report_html(table))
    finally:
        warehouse.close()


if __name__ == '__main__':
    main()
