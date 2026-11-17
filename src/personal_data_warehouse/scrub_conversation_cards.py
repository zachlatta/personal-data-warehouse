"""Explicit, idempotent maintenance command; dry-run unless --apply is supplied.

Discovery is read-only and runs while workers remain live. Save its private
ID/count plan with --plan, then reuse it with --apply. Pause conversation
writers, timeline sync and search workers only during application.
Output contains counts only. This cannot erase historical backups or WAL.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import html
import signal
import threading
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from psycopg2.extras import Json, execute_batch
import psycopg2

from personal_data_warehouse.card_scrub_selector import ScrubSelector, resolve_selector, add_selector_arguments
from personal_data_warehouse.card_redaction import sensitive_count, is_card, redact_row
from personal_data_warehouse.postgres import PostgresWarehouse, POSTGRES_TABLES, _AI_CONVERSATION_EVENT_TABLES
from personal_data_warehouse.card_prefilter import ConfirmationPool, available_cpus, count_row, sql_prefilter


TABLES = (*_AI_CONVERSATION_EVENT_TABLES, 'timeline_events', 'search_chunks')
DERIVED = ('timeline_events', 'search_chunks')
row_matches = count_row


def connection_workers(warehouse,requested,headroom=6):
    maximum,reserved,used=warehouse._query("SELECT current_setting('max_connections')::int,current_setting('superuser_reserved_connections')::int,(SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend')")[0]
    spare=maximum-reserved-used-headroom
    if spare<3: raise RuntimeError('insufficient connection headroom; no maintenance started')
    return max(1,min(requested,spare-2))


@contextmanager
def maintenance_deadline(seconds):
    if threading.current_thread() is not threading.main_thread():
        raise RuntimeError('bounded maintenance must run on the main thread')
    if seconds<=0: raise ValueError('maintenance budget must be positive')
    def expired(*_): raise TimeoutError('maintenance budget exhausted; rollback required')
    previous=signal.signal(signal.SIGALRM,expired)
    timer=signal.setitimer(signal.ITIMER_REAL,seconds)
    try: yield
    finally:
        signal.setitimer(signal.ITIMER_REAL,*timer)
        signal.signal(signal.SIGALRM,previous)


@contextmanager
def _maintenance_transaction(connection):
    """A pending SQL timeout must not interrupt rollback after SIGALRM.

    psycopg's transaction context can itself receive the server cancellation
    while cleaning up the signal exception. Explicitly roll back once that
    cancellation has been consumed, before returning control to restoration.
    """
    try:
        with connection:
            yield
    except BaseException as error:
        connection.rollback()
        if isinstance(error,psycopg2.errors.QueryCanceled):
            raise TimeoutError('maintenance budget exhausted; rollback required') from None
        raise


def _scope(warehouse):
    row = warehouse._query('SELECT current_database(), inet_server_addr()::text, inet_server_port()')[0]
    return [*row, warehouse._schema]


def _details(candidates):
    counts = Counter()
    for item in candidates:
        counts[item['table'],item['month'],item['source'],'rows'] += 1
        counts[item['table'],item['month'],item['source'],'matches'] += item['matches']
    return [dict(table=t,month=m,source=s,measure=k,count=n) for (t,m,s,k),n in sorted(counts.items())]


def _scan_table(warehouse, table, pool, page_size, connection=None, snapshot=None, blocks=None,
                since=None, month=None, selected_limit=None, statistics=None):
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
            predicate = sql_prefilter('to_jsonb(candidate)::text', scope=pool.selector.scope if pool.selector else 'all')
            keys=POSTGRES_TABLES[table].primary_key
            key_sql=','.join(keys)
            bounded = ' AND candidate.ctid >= %s::tid AND candidate.ctid < %s::tid' if blocks else ''
            params=(f'({blocks[0]},0)',f'({blocks[1]},0)') if blocks else ()
            if since:
                bounded += ' AND age(candidate.xmin) <= age(pg_snapshot_xmin(%s::pg_snapshot)::xid)'
                params += (since,)
            if month:
                bounded += " AND candidate.occurred_at >= %s::timestamptz AND candidate.occurred_at < ((%s::date + interval '1 month') AT TIME ZONE 'UTC')"
                params += (month+'-01T00:00:00Z',month+'-01')
            # Buffer only IDs, never every prefiltered payload. Fetch matching
            # content in bounded PK batches using this same MVCC snapshot.
            cursor.execute(warehouse._expand_relations(f'SELECT {key_sql} FROM @{table} candidate WHERE {scope} AND ({predicate}){bounded}'),params)
            result = []
            while values := cursor.fetchmany(page_size):
                placeholders=','.join('('+','.join('%s' for _ in keys)+')' for _ in values)
                rows=_read_on(warehouse,conn,f'SELECT * FROM @{table} WHERE ({key_sql}) IN ({placeholders})',tuple(v for key in values for v in key))
                counts=pool.counts(rows,statistics=statistics) if statistics is not None else pool.counts(rows)
                for row,matches in zip(rows,counts,strict=True):
                    if not matches: continue
                    identity = session_identity(row) if table in DERIVED else (row['source'],row['session_id'])
                    result.append(dict(table=table,key=[row[k] for k in POSTGRES_TABLES[table].primary_key],
                                       session=list(identity) if identity else None,month=row['occurred_at'].strftime('%Y-%m'),
                                       source=row['source'],matches=matches))
                    if selected_limit is not None and len(result)>selected_limit:
                        raise RuntimeError('delta limit exceeded; selected readings require catch-up')
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
    snapshot=_read_on(warehouse,connection,'SELECT pg_current_snapshot()::text AS value')[0]['value']
    return {'timeline_events':timeline,'search_chunks':chunks,'snapshot':snapshot}


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


def _checkpoint(path, plan):
    """Atomic private replacement; interruption retains the last complete pass."""
    path = Path(path)
    _validate_plan(plan)
    fd,name=tempfile.mkstemp(prefix=path.name+'.',suffix='.pending',dir=path.parent)
    temporary=Path(name)
    try:
        with os.fdopen(fd,'w') as stream:
            json.dump(plan,stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _check_horizon(warehouse, snapshot):
    horizon=warehouse._query("SELECT pg_current_xact_id()::text::numeric - pg_snapshot_xmin(%s::pg_snapshot)::text::numeric",(snapshot,))[0][0]
    if not 0<=horizon<2**31:
        raise RuntimeError('discovery transaction horizon expired; rediscovery required')


def _selected_delta(warehouse, plan, pool, page_size=500, limit=None):
    # N6: oldest top-level active xid conservatively includes its savepoints.
    # Check the full epoch before making modular age comparisons. SQL first
    # filters revisions and cheap candidate text; Python sees only those hits.
    snapshot=plan['watermarks']['snapshot']
    _check_horizon(warehouse,snapshot)
    candidates=[]
    for table in TABLES:
        candidates.extend(_scan_table(warehouse,table,pool,page_size,warehouse._connection,
            since=snapshot,selected_limit=None if limit is None else limit-len(candidates)))
    return candidates


def catch_up(warehouse, plan, *, selector=None, workers=1, path=None, max_passes=5,
             remaining_limit=0, progress=None):
    """Read-only live passes. Advance the horizon only after a complete scan.

    Keep historical IDs for truncated derived copies; merge new selected IDs.
    An old open writer may repeat a candidate: stop at the bounded pass count
    and leave the final locked selected gate to decide whether apply is safe.
    """
    _validate_plan(plan)
    if plan['scope'] != _scope(warehouse) or plan.get('selector',ScrubSelector().specification) != (selector or ScrubSelector()).specification:
        raise ValueError('plan scope or selector differs')
    if 'discovery' in plan: raise ValueError('partition discovery incomplete')
    for _ in range(max_passes):
        watermarks=_watermarks(warehouse,warehouse._connection)
        with ConfirmationPool(workers,selector) as pool:
            delta=_selected_delta(warehouse,plan,pool)
        indexed={(c['table'],tuple(c['key'])):c for c in plan['candidates']}
        indexed.update({(c['table'],tuple(c['key'])):c for c in delta})
        candidates=_attach_related(warehouse,list(indexed.values()),warehouse._connection)
        plan=dict(plan,watermarks=watermarks,candidates=candidates,counts=_details(candidates))
        if path: _checkpoint(path,plan)
        if progress: progress(dict(selected_delta_rows=len(delta),planned_rows=len(candidates)))
        if len(delta)<=remaining_limit: break
    return plan


def discover_resumable(warehouse, path, *, selector=None, workers=1, page_size=500, progress=None):
    """Checkpoint each source-table/month while writers remain live.

    Run one owner per checkpoint. Threads share the measured worker budget.
    The original conservative horizon survives every restart, so revisions
    moved across month boundaries or committed later are caught by catch_up.
    """
    path=Path(path)
    if path.exists():
        plan=read_plan(path)
        if plan['scope'] != _scope(warehouse) or plan.get('selector',ScrubSelector().specification) != (selector or ScrubSelector()).specification:
            raise ValueError('plan scope or selector differs')
        if 'discovery' not in plan: return plan
        _check_horizon(warehouse,plan['watermarks']['snapshot'])
    else:
        watermarks=_watermarks(warehouse,warehouse._connection)
        jobs=[]
        for table in TABLES:
            months=warehouse._query(f"SELECT DISTINCT to_char(occurred_at AT TIME ZONE 'UTC','YYYY-MM') FROM @{table} ORDER BY 1")
            jobs.extend([table,m] for m, in months)
        plan=dict(version=2,scope=_scope(warehouse),watermarks=watermarks,candidates=[],counts=[],
                  discovery=dict(jobs=jobs,completed=[]))
        if selector and selector.scope!='all': plan.update(selector=selector.specification,left_alone=0)
        _checkpoint(path,plan)
    state=plan['discovery']
    pending=[job for job in state['jobs'] if job not in state['completed']]
    def scan(job, pool):
        statistics={'left_alone':0}
        rows=_scan_table(warehouse,job[0],pool,page_size,month=job[1],statistics=statistics)
        return rows,statistics['left_alone']
    with ConfirmationPool(workers,selector) as pool, ThreadPoolExecutor(max_workers=workers) as scans:
        futures={scans.submit(scan,job,pool):job for job in pending}
        for future in as_completed(futures):
            job=futures[future]
            group,left_alone=future.result()
            indexed={(c['table'],tuple(c['key'])):c for c in plan['candidates']}
            indexed.update({(c['table'],tuple(c['key'])):c for c in group})
            plan['candidates']=list(indexed.values())
            plan['counts']=_details(plan['candidates'])
            state['completed'].append(job)
            if 'left_alone' in plan: plan['left_alone']+=left_alone
            _checkpoint(path,plan)
            if progress: progress(dict(completed_partitions=len(state['completed']),total_partitions=len(state['jobs']),selected_rows=len(plan['candidates'])))
    plan['candidates']=_attach_related(warehouse,plan['candidates'],warehouse._connection)
    plan['counts']=_details(plan['candidates'])
    del plan['discovery']
    _checkpoint(path,plan)
    return plan


def read_plan(path):
    plan = json.loads(path.read_text())
    _validate_plan(plan)
    return plan


def _validate_plan(plan):
    if "selector" in plan:
        from personal_data_warehouse.card_scrub_selector import validate_specification
        validate_specification(plan["selector"])
        if type(plan.get("left_alone")) is not int or plan["left_alone"] < 0: raise ValueError("invalid left-alone count")
    if set(plan) - {"selector","left_alone","discovery","batches"} != {'version','scope','watermarks','candidates','counts'} or plan['version'] != 2:
        raise ValueError('invalid discovery plan')
    if 'batches' in plan:
        state=plan['batches']
        if set(state)!={'completed','oversized'}: raise ValueError('invalid batch checkpoint')
        for group in state['completed']+state['oversized']:
            if set(group)!={'sessions','canonical_rows','seconds'} or not isinstance(group['sessions'],list):
                raise ValueError('invalid batch record')
            if type(group['canonical_rows']) is not int or group['canonical_rows']<0 or not isinstance(group['seconds'],(float,int)) or not 0<=group['seconds']<float('inf'):
                raise ValueError('invalid batch measurements')
            known={tuple(c['session']) for c in plan['candidates'] if c['session']}
            if any(not isinstance(s,list) or len(s)!=2 or tuple(s) not in known for s in group['sessions']):
                raise ValueError('invalid batch session')
    if 'discovery' in plan:
        state=plan['discovery']
        if (set(state)!={'jobs','completed'} or any(not isinstance(j,list) or len(j)!=2 or j[0] not in TABLES or
            not isinstance(j[1],str) or len(j[1])!=7 for j in state['jobs']) or
            any(j not in state['jobs'] for j in state['completed'])):
            raise ValueError('invalid partition checkpoint')
    if (set(plan['watermarks']) != set(DERIVED)|{'snapshot'} or type(plan['watermarks']['timeline_events']) is not int or
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


def scrub(warehouse, *, apply=False, page_size=500, workers=1, plan=None, selector=None,
          max_source_rows=500, max_delta_rows=10000, window_seconds=60, max_plan_rows=20000,
          max_canonical_rows=None, _known_plan=None, planned_only=False,
          exact_source_rows=None, allow_clean=False):
    plan = plan if plan is not None else discover(warehouse,workers=workers,page_size=page_size,selector=selector)
    _validate_plan(plan)
    if plan.get("selector", ScrubSelector().specification) != (selector or ScrubSelector()).specification:
        raise ValueError("plan selector differs from current selector")
    if plan['scope'] != _scope(warehouse): raise ValueError('discovery plan belongs to another warehouse')
    if not apply: return plan['counts']
    if planned_only and (not selector or selector.scope != 'tokens'):
        raise ValueError('planned-only repair requires the token selector')
    if not 0<window_seconds<=60: raise ValueError('maintenance budget must be at most 60 seconds')
    if 'discovery' in plan: raise ValueError('partition discovery incomplete')
    if exact_source_rows is not None and not 0<exact_source_rows<=max_source_rows:
        raise ValueError('invalid exact source-row gate')
    source_rows=sum(c['table'] not in DERIVED and c['matches']>0 for c in plan['candidates'])
    if source_rows>max_source_rows: raise ValueError('candidate gate exceeded; no changes committed')
    if len(plan['candidates'])>max_plan_rows: raise ValueError('total candidate gate exceeded; no changes committed')
    with maintenance_deadline(window_seconds), _maintenance_transaction(warehouse._connection):
        warehouse._command('SET LOCAL statement_timeout = %s',(int(window_seconds*1000),))
        # Fail immediately on a live writer. Hold every lock until commit so
        # nothing can appear between delta confirmation and verification.
        warehouse._command('LOCK TABLE '+','.join('@'+t for t in (*TABLES,'search_chunk_embeddings'))+' IN SHARE ROW EXCLUSIVE MODE NOWAIT')
        if max_canonical_rows is not None:
            sessions={tuple(c['session']) for c in plan['candidates'] if c['session']}
            if sum(_canonical_counts(warehouse,sessions,max_canonical_rows).values())>max_canonical_rows:
                raise ValueError('canonical row gate exceeded; no changes committed')
        planned={(c['table'],tuple(c['key'])) for c in (_known_plan or plan)['candidates']}
        with ConfirmationPool(workers,selector) as pool:
            delta=(_session_candidates(warehouse,plan,pool) if planned_only else
                   _selected_delta(warehouse,plan,pool,page_size,max_delta_rows))
            for item in delta:
                if (item['table'],tuple(item['key'])) not in planned:
                    raise RuntimeError('new candidates require discovery; no changes committed')
            # Saved matches are only a live preflight. Known zero-match IDs
            # can acquire selected readings before these writer locks. Recount
            # the exact source identities apply will read, in bounded pages,
            # under the same locks/transaction/deadline as the mutation.
            sources=[row for _,row in _planned_rows(warehouse,[c for c in plan['candidates'] if c['table'] not in DERIVED])]
            current_sources=sum(sum(n>0 for n in pool.counts(sources[i:i+page_size]))
                                for i in range(0,len(sources),page_size))
            if current_sources>max_source_rows:
                raise ValueError('current source-row gate exceeded; no changes committed')
            if (exact_source_rows is not None and current_sources!=exact_source_rows and
                    not (allow_clean and current_sources==0)):
                raise ValueError('exact source-row gate differs under locks; no changes committed')
        if planned_only:
            _apply_plan(warehouse,plan,page_size,workers,selector,planned_only=True)
        else:
            _apply_plan(warehouse,plan,page_size,workers,selector)
        # Canonical new keys are checked before upsert by rebuild_sessions.
        # Recheck saved keys without running whole-corpus detection in-window.
        rows=[row for _,row in _planned_rows(warehouse,plan['candidates'])]
        with ConfirmationPool(workers,selector) as pool:
            if any(pool.counts(rows)): raise RuntimeError('planned row verification failed; no changes committed')
    return plan['counts']


def _session_candidates(warehouse, plan, pool):
    """Indexed source/session confirmation for explicit planned-only token repair.

    This does not certify unrelated sessions. Full post-restore verification
    remains mandatory. Unknown selected rows IN a planned session fail closed.
    """
    from personal_data_warehouse.postgres import _AI_EVENT_TABLE_BY_SOURCE
    selected=[]
    for source,session in sorted({tuple(c['session']) for c in plan['candidates'] if c['session']}):
        table=_AI_EVENT_TABLE_BY_SOURCE[source]
        rows=warehouse._query_dicts(f'SELECT * FROM @{table} WHERE source=%s AND session_id=%s', (source,session))
        for row,n in zip(rows,pool.counts(rows),strict=True):
            if n: selected.append(dict(table=table,key=[row[k] for k in POSTGRES_TABLES[table].primary_key]))
    return selected


def _planned_rows(warehouse, candidates, *, lock=False):
    rows=[]
    for table in sorted({c['table'] for c in candidates}):
        keys=POSTGRES_TABLES[table].primary_key
        identities=[c['key'] for c in candidates if c['table']==table]
        for offset in range(0,len(identities),500):
            batch=identities[offset:offset+500]
            rows.extend((table,r) for r in warehouse._query_dicts(
                f'SELECT * FROM @{table} WHERE ('+','.join(keys)+') IN ('+
                ','.join('('+','.join('%s' for _ in keys)+')' for _ in batch)+')'+(' FOR UPDATE' if lock else ''),
                tuple(v for identity in batch for v in identity)))
    return rows


def _indexed_related(warehouse, table, event_keys):
    """Timeline PK and canonical chunk anchor probes, never computed identities."""
    result=[]
    ordered=sorted(event_keys)
    for offset in range(0,len(ordered),500):
        batch=ordered[offset:offset+500]
        if table=='timeline_events':
            result.extend(warehouse._query_dicts('SELECT * FROM @timeline_events WHERE (adapter,event_id) IN ('+
                ','.join('(%s,%s)' for _ in batch)+')',tuple(v for key in batch for v in key)))
        else:
            result.extend(warehouse._query_dicts('SELECT * FROM @search_chunks WHERE anchor = ANY(%s)',
                ([f'{a}|{e}' for a,e in batch],)))
    return result


def _canonical_counts(warehouse, sessions, limit):
    # Bound each count, including a session that grew since live planning.
    return {s:warehouse._query('SELECT count(*) FROM (SELECT 1 FROM @ai_conversation_events '
        'WHERE source=%s AND session_id=%s LIMIT %s) bounded',(*s,limit+1))[0][0]
        for s in sorted(sessions)}


def session_subplan(warehouse, plan, session, *, selector):
    """Reconfirm one session from a fully scanned checkpoint while live.

    Does not advance the horizon or discard the caller's full known-ID set.
    A final-partition connection failure may leave a complete discovery ledger
    without its related-row attachment; that attachment is repeated here.
    """
    _validate_plan(plan)
    state = plan.get('discovery')
    if state is not None:
        jobs = [tuple(j) for j in state['jobs']]
        done = [tuple(j) for j in state['completed']]
        if len(set(jobs)) != len(jobs) or len(set(done)) != len(done) or set(jobs) != set(done):
            raise ValueError('partition discovery incomplete or duplicated')
    if plan['scope'] != _scope(warehouse):
        raise ValueError('plan belongs to another warehouse')
    spec = plan.get('selector')
    if (selector is None or selector.scope != 'seed' or not selector.seeds or
            spec is None or selector.seed_revision != spec['seed_revision'] or
            list(selector.seed_rows) != spec['seed_rows']):
        raise ValueError('retained seed selector differs from checkpoint')
    candidates = []
    for c in plan['candidates']:
        if c['session'] != list(session) or c['table'] in DERIVED: continue
        keys = POSTGRES_TABLES[c['table']].primary_key
        rows = warehouse._query_dicts(f'SELECT * FROM @{c["table"]} WHERE ' +
            ' AND '.join(f'{k}=%s' for k in keys), c['key'])
        n = sum(selector.count_row(r) for r in rows)
        # Keep clean IDs for commit-before-checkpoint replay.
        candidates.append(dict(c, matches=n))
    if not candidates: raise ValueError('session absent from checkpoint')
    candidates = _attach_related(warehouse, candidates, warehouse._connection)
    subset = {k:v for k,v in plan.items() if k not in ('discovery','batches')}
    subset.update(candidates=candidates, counts=_details(candidates), selector=selector.specification)
    return subset


def apply_session(warehouse, plan, session, *, selector, window, exact_source_rows,
                  workers=1, max_canonical_rows=2500, window_seconds=45,
                  max_plan_rows=20000, max_delta_rows=1000, allow_clean=False):
    """One bounded window; restore belongs to the supplied context manager.

    Keep ``selector`` in the owner through restoration, JSONL, and global
    verification. On connection failure replace the connection, not the selector.
    """
    if not 0 < max_canonical_rows <= 5000:
        raise ValueError('canonical row gate exceeded')
    if not 0 < exact_source_rows <= 500:
        raise ValueError('invalid exact source-row gate')
    if len(plan['candidates']) > min(20000, max_plan_rows):
        raise ValueError('total candidate gate exceeded')
    subset = session_subplan(warehouse, plan, session, selector=selector)
    n = sum(c['table'] not in DERIVED and c['matches'] > 0 for c in subset['candidates'])
    if n != exact_source_rows and not (allow_clean and n == 0):
        raise ValueError('exact source-row gate differs')
    if sum(_canonical_counts(warehouse, {tuple(session)}, max_canonical_rows).values()) > max_canonical_rows:
        raise ValueError('canonical row gate exceeded')
    known = dict(plan, candidates=plan['candidates'] + subset['candidates'])
    with window(0):
        scrub(warehouse, apply=True, plan=subset, selector=selector, workers=workers,
              max_source_rows=exact_source_rows, max_plan_rows=max_plan_rows,
              max_delta_rows=max_delta_rows, max_canonical_rows=max_canonical_rows,
              window_seconds=window_seconds, _known_plan=known,
              exact_source_rows=exact_source_rows, allow_clean=allow_clean)
    return subset


def apply_batches(warehouse, path, *, window, selector=None, workers=1, allow_partial=False,
                  max_canonical_rows=2500, window_seconds=45, gap_seconds=15,
                  max_source_rows=500, max_plan_rows=20000, max_delta_rows=1000,
                  progress=None, planned_only=False):
    """One owner; live planning, then an atomic transaction per session group.

    ``window(index)`` must pause writers on entry and restore them in finally,
    including on partial entry failure. Completion is persisted AFTER restore.
    A crash after commit is safe: replay rechecks and applies the same IDs.
    Partial discovery retains its original horizon and pending partitions;
    no partial run constitutes final verification. Selector stays in memory.
    """
    if not 0<max_canonical_rows<=(10000 if planned_only else 5000) or not 0<window_seconds<=60 or gap_seconds<0:
        raise ValueError('invalid batch budget')
    plan=read_plan(Path(path))
    if 'discovery' in plan and not allow_partial: raise ValueError('partition discovery incomplete')
    if plan['scope']!=_scope(warehouse) or plan.get('selector',ScrubSelector().specification)!=(selector or ScrubSelector()).specification:
        raise ValueError('plan scope or selector differs')
    if sum(c['table'] not in DERIVED and c['matches']>0 for c in plan['candidates'])>min(500,max_source_rows) or len(plan['candidates'])>min(20000,max_plan_rows):
        raise ValueError('candidate gate exceeded')
    if any(not c['session'] for c in plan['candidates']):
        raise ValueError('cannot group unidentified session')
    state=plan.setdefault('batches',dict(completed=[],oversized=[]))
    completed={tuple(s) for g in state['completed'] for s in g['sessions']}
    # Never silently trust a completion if an old key has become selected again.
    with ConfirmationPool(workers,selector) as pool:
        finished=[c for c in plan['candidates'] if tuple(c['session']) in completed]
        if any(pool.counts([row for _,row in _planned_rows(warehouse,finished)])):
            raise RuntimeError('completed session changed; rediscovery required')
    sessions={tuple(c['session']) for c in plan['candidates']}-completed
    counts=_canonical_counts(warehouse,sessions,max_canonical_rows)
    state['oversized']=[dict(sessions=[list(s)],canonical_rows=n,seconds=0) for s,n in counts.items() if n>max_canonical_rows]
    groups=[]; group=[]; total=0
    for s,n in counts.items():
        if n>max_canonical_rows: continue
        if group and total+n>max_canonical_rows:
            groups.append((group,total)); group=[]; total=0
        group.append(s); total+=n
    if group: groups.append((group,total))
    _checkpoint(path,plan)
    if progress: progress(dict(groups=len(groups),oversized_sessions=len(state['oversized']),canonical_rows=sum(counts.values())))
    for index,(sessions,total) in enumerate(groups):
        time.sleep(gap_seconds)  # workers LIVE, including after a resumed run
        candidates=[c for c in plan['candidates'] if tuple(c['session']) in sessions]
        if not planned_only: candidates=_attach_related(warehouse,candidates,warehouse._connection)
        if len(candidates)>min(20000,max_plan_rows):
            raise ValueError('group candidate gate exceeded; no maintenance started')
        subset={k:v for k,v in plan.items() if k not in ('discovery','batches')}
        subset.update(candidates=candidates,counts=_details(candidates))
        known=dict(plan,candidates=plan['candidates']+candidates)
        started=time.monotonic()
        with window(index):
            scrub(warehouse,apply=True,plan=subset,selector=selector,workers=workers,
                  max_source_rows=max_source_rows,max_plan_rows=max_plan_rows,
                  max_delta_rows=max_delta_rows,window_seconds=window_seconds,
                  max_canonical_rows=max_canonical_rows,_known_plan=known,planned_only=planned_only)
        elapsed=time.monotonic()-started
        state['completed'].append(dict(sessions=[list(s) for s in sessions],canonical_rows=total,seconds=elapsed))
        _checkpoint(path,plan)
        if progress: progress(dict(completed_groups=len(state['completed']),canonical_rows=total,window_seconds=elapsed))
    return plan


def _apply_plan(warehouse, plan, page_size, workers, selector=None, *, planned_only=False):
    sessions = {tuple(c['session']) for c in plan['candidates'] if c['session']}
    current = {(table,tuple(row[k] for k in POSTGRES_TABLES[table].primary_key)):row
               for table,row in _planned_rows(warehouse,plan['candidates'],lock=True)}
    for (table,_),row in current.items():
        identity=session_identity(row) if table in DERIVED else (row['source'],row['session_id'])
        if identity:sessions.add(identity)
    event_keys = {(row['adapter'],row['event_id']) for (table,_),row in current.items() if table in DERIVED}
    for table in DERIVED:
        if not sessions and not event_keys: break
        related=(_indexed_related(warehouse,table,event_keys) if planned_only else
                 _related_rows(warehouse,table,sessions,event_keys,delta=plan['watermarks']))
        for row in related:
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
    if planned_only:
        # Include canonical chunks created since discovery, including events
        # that were not yet present in the original timeline plan.
        extra=_indexed_related(warehouse,'search_chunks',new_keys)
        chunks.extend((r['chunk_id'],r['text_sha256'],(r['adapter'],r['event_id']),session_identity(r)) for r in extra)
        chunks=list({r[0]:r for r in chunks}.values())
    for offset in range(0,len(chunks),500):
        batch=chunks[offset:offset+500]
        warehouse._command('DELETE FROM @search_chunk_embeddings WHERE text_sha256 = ANY(%s)',([c[1] for c in batch],))
        warehouse._command('DELETE FROM @search_chunks WHERE chunk_id = ANY(%s)',([c[0] for c in batch],))


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
                if any((selector.count if selector else sensitive_count)(str(row[i])) for i in (6, 7, 10, 11)):
                    raise RuntimeError('canonical regeneration still contains selected sensitive data; no changes committed')
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
    parser.add_argument('--resume', action='store_true', help='checkpoint source-table/month discovery and resume an existing private plan')
    parser.add_argument('--catch-up', action='store_true', help='refresh an existing plan with live selected-delta passes')
    parser.add_argument('--plan', type=Path, help='private ID-only discovery plan; new file for discovery, existing file for apply')
    parser.add_argument('--workers', type=int, default=available_cpus(), help='Python confirmation processes and concurrent SQL scan budget')
    parser.add_argument('--max-source-rows',type=int,default=500)
    parser.add_argument('--only-session', nargs=2, metavar=('SOURCE','SESSION'),
                        help='apply only seed-matched readings in this session from a completed checkpoint')
    parser.add_argument('--max-delta-rows',type=int,default=10000)
    parser.add_argument('--max-plan-rows',type=int,default=20000)
    parser.add_argument('--window-seconds',type=int,default=60)
    parser.add_argument('--session-batches',action='store_true',help='apply complete sessions in separate bounded transactions')
    parser.add_argument('--planned-only',action='store_true',help='tokens only: indexed planned-session repair; unrelated sessions require full post-restore verify')
    parser.add_argument('--writers-paused',action='store_true',help='acknowledge external writer shutdown and guaranteed restoration')
    parser.add_argument('--max-canonical-rows',type=int,default=10000)
    parser.add_argument('--report-html', type=Path, help='counts-only table for the private email body')
    add_selector_arguments(parser)
    args = parser.parse_args()
    if args.workers < 1: parser.error('--workers must be positive')
    if args.apply and args.verify: parser.error('--apply and --verify are mutually exclusive')
    if args.only_session and (args.planned_only or args.session_batches):
        parser.error('--only-session is a seed repair; it cannot combine with token session batches')
    if args.planned_only or args.session_batches:
        if not (args.apply and args.planned_only and args.session_batches and args.writers_paused and args.scope=='tokens'):
            parser.error('session batches require --apply --scope tokens --planned-only --writers-paused')
    if not args.verify and args.plan is None: parser.error('--plan is required for discovery and apply')
    warehouse = PostgresWarehouse(os.environ['POSTGRES_DATABASE_URL'],initialize=False)
    warehouse._connection.set_session(readonly=not args.apply,autocommit=True)
    try:
        args.workers=connection_workers(warehouse,args.workers)
        baseline = read_plan(args.plan) if args.plan and args.plan.exists() and (args.apply or args.verify or args.resume or args.catch_up) else None
        if args.only_session and (not args.apply or baseline is None):
            parser.error('--only-session requires --apply and a completed checkpoint')
        binding_scope = baseline['selector']['scope'] if args.only_session else args.scope
        selector = resolve_selector(warehouse,binding_scope,args.seed_row,expected=baseline.get('selector') if baseline else None)
        if args.only_session: selector = selector.seed_only(baseline['selector'])
        after = None
        if args.catch_up:
            if baseline is None: parser.error('--catch-up requires an existing complete plan')
            plan=catch_up(warehouse,baseline,workers=args.workers,selector=selector,path=args.plan,progress=lambda counts:print(json.dumps(counts),flush=True))
            before=plan['counts']
        elif args.apply:
            plan = read_plan(args.plan)
            before = plan['counts']
            if args.session_batches:
                from contextlib import nullcontext
                plan=apply_batches(warehouse,args.plan,window=lambda _:nullcontext(),selector=selector,
                    workers=args.workers,planned_only=True,max_canonical_rows=args.max_canonical_rows,
                    window_seconds=args.window_seconds,gap_seconds=0,max_source_rows=args.max_source_rows,
                    max_plan_rows=args.max_plan_rows,max_delta_rows=args.max_delta_rows)
                if plan['batches']['oversized']: raise RuntimeError('oversized sessions remain; repair incomplete')
            elif args.only_session:
                from contextlib import nullcontext
                apply_session(warehouse,plan,args.only_session,selector=selector,
                    window=lambda _:nullcontext(),workers=args.workers,
                    exact_source_rows=args.max_source_rows,max_delta_rows=args.max_delta_rows,
                    window_seconds=args.window_seconds,max_plan_rows=args.max_plan_rows)
            else:
                scrub(warehouse,apply=True,plan=plan,workers=args.workers,selector=selector,max_source_rows=args.max_source_rows,max_delta_rows=args.max_delta_rows,window_seconds=args.window_seconds,max_plan_rows=args.max_plan_rows)
            # Only planned/delta rows were checked in-window. A full read-only
            # post-resume scan must establish corpus-wide after counts.
            after = None
        elif args.verify:
            if args.plan:
                baseline = read_plan(args.plan)
                if baseline.get('selector',ScrubSelector().specification) != selector.specification:
                    raise ValueError('plan selector differs from current selector')
            after = discover(warehouse,workers=args.workers,selector=selector)['counts']
            before = baseline['counts'] if args.plan else after
        else:
            if args.resume:
                plan=discover_resumable(warehouse,args.plan,workers=args.workers,selector=selector,
                    progress=lambda counts:print(json.dumps(counts),flush=True))
                plan=catch_up(warehouse,plan,workers=args.workers,selector=selector,path=args.plan)
            else:
                plan = discover(warehouse,workers=args.workers,selector=selector)
                write_plan(args.plan,plan)
            before = plan['counts']
        table = month_source_report(before, after)
        print(json.dumps({'counts_by_month_source': table, 'details_before': before, 'details_after': after, 'verification_pending':args.apply, 'left_alone': plan.get('left_alone') if not args.verify else None}))
        if args.report_html:
            args.report_html.write_text(report_html(table))
        if args.verify and args.scope == 'tokens' and any(r['measure']=='matches' and r['count'] for r in after):
            raise SystemExit('token verification failed: secrets remain')
    finally:
        warehouse.close()


if __name__ == '__main__':
    main()
