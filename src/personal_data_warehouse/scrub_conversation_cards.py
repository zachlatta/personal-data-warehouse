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
import hashlib
import json
import os
import html
import re
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
# A session this large runs in its own group (see apply_batches).
LARGE_SESSION_ROWS = 10000
# The in-window recheck costs about 0.7 s per prefiltered MB in production, so a
# group of at most 40 MB stays well inside its 60-second transaction; a heavier
# session runs alone under the 300-second large-session deadline.
MAX_GROUP_BYTES = 40_000_000
LARGE_SESSION_WINDOW_SECONDS = 300
DERIVED = ('timeline_events', 'search_chunks')
row_matches = count_row
_SNAPSHOT = re.compile(r'[0-9]+:[0-9]+:[0-9,]*')


@contextmanager
def apply_phase(timings, phase):
    """Record only a fixed phase name and elapsed seconds, including failures."""
    start = time.monotonic()
    try:
        yield
    finally:
        if timings is not None:
            timings.append(dict(phase=phase, seconds=time.monotonic()-start))


@contextmanager
def apply_clock(timings):
    current = None
    def phase(name):
        nonlocal current
        if current is not None:
            current.__exit__(None, None, None)
        current = apply_phase(timings, name)
        current.__enter__()
    try:
        yield phase
    finally:
        if current is not None:
            current.__exit__(None, None, None)


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
def _maintenance_transaction(connection, phase=lambda name: None):
    """A pending SQL timeout must not interrupt rollback after SIGALRM.

    psycopg's transaction context can itself receive the server cancellation
    while cleaning up the signal exception. Explicitly roll back once that
    cancellation has been consumed, before returning control to restoration.
    """
    try:
        with connection:
            yield
            phase('commit')
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
            # Keep payload conversion/TOAST reads behind the cheap MVCC test;
            # WHERE conjunct order is not an execution-order guarantee.
            if since:
                predicate = 'CASE WHEN age(candidate.xmin) <= age(pg_snapshot_xmin(%s::pg_snapshot)::xid) THEN ('+predicate+') ELSE false END'
                params = (since,)+params
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
        with ConfirmationPool(workers,selector,tally=False) as pool:
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
    if set(plan) - {"selector","left_alone","discovery","batches","session_bytes"} != {'version','scope','watermarks','candidates','counts'} or plan['version'] != 2:
        raise ValueError('invalid discovery plan')
    # Built once: every progress save validates the whole checkpoint while
    # writers are stopped, and a set per batch record cost groups x candidates.
    known={tuple(c['session']) for c in plan['candidates'] if c['session']}
    if 'session_bytes' in plan:
        if any(not isinstance(e,list) or len(e)!=3 or tuple(e[:2]) not in known or type(e[2]) is not int or e[2]<0
               for e in plan['session_bytes']):
            raise ValueError('invalid session sizes')
    if 'batches' in plan:
        state=plan['batches']
        if set(state)!={'completed','oversized'}: raise ValueError('invalid batch checkpoint')
        # A completed group also records the snapshot taken after its commit.
        records=[(g,{'sessions','canonical_rows','seconds','snapshot'}) for g in state['completed']]
        records+=[(g,{'sessions','canonical_rows','seconds'}) for g in state['oversized']]
        for group,fields in records:
            if set(group)!=fields or not isinstance(group['sessions'],list):
                raise ValueError('invalid batch record')
            if 'snapshot' in group and not (isinstance(group['snapshot'],str) and _SNAPSHOT.fullmatch(group['snapshot'])):
                raise ValueError('invalid batch snapshot')
            if type(group['canonical_rows']) is not int or group['canonical_rows']<0 or not isinstance(group['seconds'],(float,int)) or not 0<=group['seconds']<float('inf'):
                raise ValueError('invalid batch measurements')
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
          exact_source_rows=None, allow_clean=False, timings=None):
    plan = plan if plan is not None else discover(warehouse,workers=workers,page_size=page_size,selector=selector)
    _validate_plan(plan)
    if plan.get("selector", ScrubSelector().specification) != (selector or ScrubSelector()).specification:
        raise ValueError("plan selector differs from current selector")
    if plan['scope'] != _scope(warehouse): raise ValueError('discovery plan belongs to another warehouse')
    if not apply: return plan['counts']
    if planned_only and (not selector or selector.scope != 'credentials'):
        raise ValueError('planned-only repair requires the credential selector')
    # 60 seconds per group; apply_batches may give one large session up to 300.
    if not 0<window_seconds<=300: raise ValueError('maintenance budget must be at most 300 seconds')
    if 'discovery' in plan: raise ValueError('partition discovery incomplete')
    if exact_source_rows is not None and not 0<exact_source_rows<=max_source_rows:
        raise ValueError('invalid exact source-row gate')
    source_rows=sum(c['table'] not in DERIVED and c['matches']>0 for c in plan['candidates'])
    if source_rows>max_source_rows: raise ValueError('candidate gate exceeded; no changes committed')
    if len(plan['candidates'])>max_plan_rows: raise ValueError('total candidate gate exceeded; no changes committed')
    with maintenance_deadline(window_seconds), apply_clock(timings) as phase, _maintenance_transaction(warehouse._connection, phase):
        phase('lock_acquisition')
        warehouse._command('SET LOCAL statement_timeout = %s',(int(window_seconds*1000),))
        # Fail immediately on a live writer. Hold every lock until commit so
        # nothing can appear between delta confirmation and verification.
        warehouse._command('LOCK TABLE '+','.join('@'+t for t in (*TABLES,'search_chunk_embeddings'))+' IN SHARE ROW EXCLUSIVE MODE NOWAIT')
        phase('recount')
        if max_canonical_rows is not None:
            sessions={tuple(c['session']) for c in plan['candidates'] if c['session']}
            if sum(_canonical_counts(warehouse,sessions,max_canonical_rows).values())>max_canonical_rows:
                raise ValueError('canonical row gate exceeded; no changes committed')
        planned={(c['table'],tuple(c['key'])) for c in (_known_plan or plan)['candidates']}
        phase('delta_guard')
        with ConfirmationPool(workers,selector,tally=False) as pool:
            delta=(_session_candidates(warehouse,plan,pool) if planned_only else
                   _selected_delta(warehouse,plan,pool,page_size,max_delta_rows))
            for item in delta:
                if (item['table'],tuple(item['key'])) not in planned:
                    raise RuntimeError('new candidates require discovery; no changes committed')
            # Saved matches are only a live preflight. Known zero-match IDs
            # can acquire selected readings before these writer locks. Recount
            # the exact source identities apply will read, in bounded pages,
            # under the same locks/transaction/deadline as the mutation.
            phase('recount')
            sources=[row for _,row in _planned_rows(warehouse,[c for c in plan['candidates'] if c['table'] not in DERIVED])]
            current_sources=sum(sum(n>0 for n in pool.counts(sources[i:i+page_size]))
                                for i in range(0,len(sources),page_size))
            if current_sources>max_source_rows:
                raise ValueError('current source-row gate exceeded; no changes committed')
            if (exact_source_rows is not None and current_sources!=exact_source_rows and
                    not (allow_clean and current_sources==0)):
                raise ValueError('exact source-row gate differs under locks; no changes committed')
        if planned_only:
            _apply_plan(warehouse,plan,page_size,workers,selector,planned_only=True,phase=phase)
        else:
            _apply_plan(warehouse,plan,page_size,workers,selector,phase=phase)
        # Canonical new keys are checked before upsert by rebuild_sessions.
        # Recheck saved keys without running whole-corpus detection in-window.
        phase('verification')
        rows=[row for _,row in _planned_rows(warehouse,plan['candidates'],prefilter_scope=selector.scope if selector else 'all')]
        with ConfirmationPool(workers,selector,tally=False) as pool:
            if any(pool.counts(rows)): raise RuntimeError('planned row verification failed; no changes committed')
    return plan['counts']


def _session_candidates(warehouse, plan, pool):
    """Indexed source/session confirmation for explicit planned-only credential repair.

    This does not certify unrelated sessions. Full post-restore verification
    remains mandatory. Unknown selected rows IN a planned session fail closed.
    """
    from personal_data_warehouse.postgres import _AI_EVENT_TABLE_BY_SOURCE
    selected=[]
    for source,session in sorted({tuple(c['session']) for c in plan['candidates'] if c['session']}):
        table=_AI_EVENT_TABLE_BY_SOURCE[source]
        # Only rows passing the scan's superset prefilter can hold a selected
        # reading; the in-window recheck of a large session reads just those.
        prefilter=sql_prefilter('to_jsonb(candidate)::text',scope=pool.selector.scope if pool.selector else 'all')
        rows=warehouse._query_dicts(f'SELECT * FROM @{table} candidate WHERE source=%s AND session_id=%s AND {prefilter}', (source,session))
        for row,n in zip(rows,pool.counts(rows),strict=True):
            if n: selected.append(dict(table=table,key=[row[k] for k in POSTGRES_TABLES[table].primary_key]))
    return selected


def _planned_rows(warehouse, candidates, *, lock=False, prefilter_scope=None):
    """Saved identities in 500-key batches; ``prefilter_scope`` reads only rows
    that pass the scan's superset prefilter (the rest cannot hold a reading)."""
    rows=[]
    prefilter=(' AND '+sql_prefilter('to_jsonb(candidate)::text',scope=prefilter_scope)) if prefilter_scope else ''
    for table in sorted({c['table'] for c in candidates}):
        keys=POSTGRES_TABLES[table].primary_key
        identities=[c['key'] for c in candidates if c['table']==table]
        for offset in range(0,len(identities),500):
            batch=identities[offset:offset+500]
            rows.extend((table,r) for r in warehouse._query_dicts(
                f'SELECT * FROM @{table} candidate WHERE ('+','.join(keys)+') IN ('+
                ','.join('('+','.join('%s' for _ in keys)+')' for _ in batch)+')'+prefilter+(' FOR UPDATE' if lock else ''),
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


def _session_bytes(warehouse, sessions, scope):
    """Bytes of each session's rows that pass the scan prefilter, read live.

    The in-window recheck confirms exactly these rows, so its time follows
    these bytes (about 0.7 s per MB in production), not the row count.
    """
    from personal_data_warehouse.postgres import _AI_EVENT_TABLE_BY_SOURCE
    prefilter=sql_prefilter('to_jsonb(candidate)::text',scope=scope)
    return {s:int(warehouse._query(f'SELECT COALESCE(sum(octet_length(to_jsonb(candidate)::text)),0) '
        f'FROM @{_AI_EVENT_TABLE_BY_SOURCE[s[0]]} candidate WHERE source=%s AND session_id=%s AND {prefilter}',s)[0][0])
        for s in sorted(sessions)}


def _post_commit_snapshot(warehouse):
    return _read_on(warehouse,warehouse._connection,'SELECT pg_current_snapshot()::text AS value')[0]['value']


def _written_since(warehouse, sessions, snapshot):
    """Source rows of ``sessions`` written by a transaction ``snapshot`` does not see:
    an xid at or past its xmax, or one still in progress when it was taken."""
    from personal_data_warehouse.postgres import _AI_EVENT_TABLE_BY_SOURCE
    _check_horizon(warehouse,snapshot)
    rows=[]
    for table in sorted({_AI_EVENT_TABLE_BY_SOURCE[source] for source,_ in sessions}):
        ordered=sorted(s for s in sessions if _AI_EVENT_TABLE_BY_SOURCE[s[0]]==table)
        rows.extend(warehouse._query_dicts(
            f'SELECT * FROM @{table} candidate WHERE (source,session_id) IN ('+','.join('(%s,%s)' for _ in ordered)+') '
            'AND (age(candidate.xmin) <= age(pg_snapshot_xmax(%s::pg_snapshot)::xid) OR '
            'candidate.xmin = ANY(ARRAY(SELECT x::xid FROM pg_snapshot_xip(%s::pg_snapshot) x)))',
            tuple(v for pair in ordered for v in pair)+(snapshot,snapshot)))
    return rows


def session_bytes_entries(warehouse, plan, scope):
    """``plan['session_bytes']``, measured while writers are live (see apply_batches)."""
    sizes=_session_bytes(warehouse,{tuple(c['session']) for c in plan['candidates'] if c['session']},scope)
    return [[*s,n] for s,n in sorted(sizes.items())]


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
    subset = {k:v for k,v in plan.items() if k not in ('discovery','batches','session_bytes')}
    subset.update(candidates=candidates, counts=_details(candidates), selector=selector.specification)
    return subset


def apply_session(warehouse, plan, session, *, selector, window, exact_source_rows,
                  workers=1, max_canonical_rows=2500, window_seconds=45,
                  max_plan_rows=20000, max_delta_rows=1000, allow_clean=False, timings=None):
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
    # Capture BEFORE the live scan. Every change racing any part of that scan
    # is checked again under the writer locks, including active parents' child
    # transactions (the N6 oldest-active-xid rule). Never persist or overwrite
    # the checkpoint horizon: this fence is valid only for this apply attempt.
    fence = _watermarks(warehouse, warehouse._connection)
    known_ids = {(c['table'], tuple(c['key'])) for c in plan['candidates']}
    with ConfirmationPool(workers,selector,tally=False) as pool:
        for item in _selected_delta(warehouse, plan, pool, limit=max_delta_rows):
            if (item['table'], tuple(item['key'])) not in known_ids:
                raise RuntimeError('new candidates require discovery; no changes committed')
    subset = session_subplan(warehouse, plan, session, selector=selector)
    n = sum(c['table'] not in DERIVED and c['matches'] > 0 for c in subset['candidates'])
    if n != exact_source_rows and not (allow_clean and n == 0):
        raise ValueError('exact source-row gate differs')
    if sum(_canonical_counts(warehouse, {tuple(session)}, max_canonical_rows).values()) > max_canonical_rows:
        raise ValueError('canonical row gate exceeded')
    known = dict(plan, candidates=plan['candidates'] + subset['candidates'])
    with window(0):
        scrub(warehouse, apply=True, plan=dict(subset, watermarks=fence), selector=selector, workers=workers,
              max_source_rows=exact_source_rows, max_plan_rows=max_plan_rows,
              max_delta_rows=max_delta_rows, max_canonical_rows=max_canonical_rows,
              window_seconds=window_seconds, _known_plan=known,
              exact_source_rows=exact_source_rows, allow_clean=allow_clean, timings=timings)
    return subset


def apply_batches(warehouse, path, *, window, selector=None, workers=1, allow_partial=False,
                  max_canonical_rows=2500, window_seconds=45, gap_seconds=15,
                  max_source_rows=500, max_plan_rows=20000, max_delta_rows=1000,
                  progress=None, planned_only=False, max_groups=None,
                  large_session_window_seconds=LARGE_SESSION_WINDOW_SECONDS, max_group_bytes=MAX_GROUP_BYTES):
    """One owner; live planning, then an atomic transaction per session group.

    ``max_groups`` bounds one run (one writer outage); the checkpoint records
    each completed group, so the next run resumes with the rest. A group holds
    at most ``max_canonical_rows`` rows and ``max_group_bytes`` prefiltered
    bytes (the in-window recheck's cost). A session over LARGE_SESSION_ROWS
    rows or over the byte cap runs alone, under ``large_session_window_seconds``
    (at most 300, the default), so one heavy session can still finish.

    ``window(index)`` must pause writers on entry and restore them in finally,
    including on partial entry failure. Completion is persisted AFTER restore.
    A crash after commit is safe: replay rechecks and applies the same IDs.
    Partial discovery retains its original horizon and pending partitions;
    no partial run constitutes final verification. Selector stays in memory.
    """
    # A planned-only group may hold one large session (30,000 rows); the
    # 60-second transaction deadline still rolls back a group that overruns.
    if (not 0<max_canonical_rows<=(30000 if planned_only else 5000) or not 0<window_seconds<=60 or gap_seconds<0
            or max_groups is not None and max_groups<1
            or not 0<large_session_window_seconds<=300 or not 0<max_group_bytes):
        raise ValueError('invalid batch budget')
    plan=read_plan(Path(path))
    if 'discovery' in plan and not allow_partial: raise ValueError('partition discovery incomplete')
    if plan['scope']!=_scope(warehouse) or plan.get('selector',ScrubSelector().specification)!=(selector or ScrubSelector()).specification:
        raise ValueError('plan scope or selector differs')
    # Plan-wide gates are the explicit flags (defaults 500 / 20,000); each group
    # stays bounded by the canonical-row cap and its transaction deadline.
    if sum(c['table'] not in DERIVED and c['matches']>0 for c in plan['candidates'])>max_source_rows or len(plan['candidates'])>max_plan_rows:
        raise ValueError('candidate gate exceeded')
    if any(not c['session'] for c in plan['candidates']):
        raise ValueError('cannot group unidentified session')
    state=plan.setdefault('batches',dict(completed=[],oversized=[]))
    completed={tuple(s) for g in state['completed'] for s in g['sessions']}
    # Never silently trust a completion if an old key has become selected again.
    # Writers are stopped here, so read only source rows written after each
    # group's commit; rereading every finished row grew with progress.
    with ConfirmationPool(workers,selector,tally=False) as pool:
        for g in state['completed']:
            if any(pool.counts(_written_since(warehouse,{tuple(s) for s in g['sessions']},g['snapshot']))):
                raise RuntimeError('completed session changed; rediscovery required')
    sessions={tuple(c['session']) for c in plan['candidates']}-completed
    counts=_canonical_counts(warehouse,sessions,max_canonical_rows)
    state['oversized']=[dict(sessions=[list(s)],canonical_rows=n,seconds=0) for s,n in counts.items() if n>max_canonical_rows]
    # Sizes recorded at plan time (writers live); measure only a session the plan lacks.
    sizes={(e[0],e[1]):e[2] for e in plan.get('session_bytes',[])}
    missing={s for s,n in counts.items() if n<=max_canonical_rows and s not in sizes}
    if missing: sizes.update(_session_bytes(warehouse,missing,selector.scope if selector else 'all'))
    groups=[]; group=[]; total=0; size=0
    for s,n in counts.items():
        if n>max_canonical_rows: continue
        if n>LARGE_SESSION_ROWS or sizes[s]>max_group_bytes:
            groups.append(([s],n,True)); continue
        if group and (total+n>max_canonical_rows or size+sizes[s]>max_group_bytes):
            groups.append((group,total,False)); group=[]; total=0; size=0
        group.append(s); total+=n; size+=sizes[s]
    if group: groups.append((group,total,False))
    _checkpoint(path,plan)
    if progress: progress(dict(groups=len(groups),oversized_sessions=len(state['oversized']),canonical_rows=sum(counts.values()),
                               prefiltered_bytes=sum(sizes[s] for s,n in counts.items() if n<=max_canonical_rows)))
    for index,(sessions,total,alone) in enumerate(groups):
        if max_groups is not None and index>=max_groups: break
        time.sleep(gap_seconds)  # workers LIVE, including after a resumed run
        candidates=[c for c in plan['candidates'] if tuple(c['session']) in sessions]
        if not planned_only: candidates=_attach_related(warehouse,candidates,warehouse._connection)
        if len(candidates)>max_plan_rows:
            raise ValueError('group candidate gate exceeded; no maintenance started')
        subset={k:v for k,v in plan.items() if k not in ('discovery','batches','session_bytes')}
        subset.update(candidates=candidates,counts=_details(candidates))
        known=dict(plan,candidates=plan['candidates']+candidates)
        started=time.monotonic()
        with window(index):
            scrub(warehouse,apply=True,plan=subset,selector=selector,workers=workers,
                  max_source_rows=max_source_rows,max_plan_rows=max_plan_rows,
                  max_delta_rows=max_delta_rows,window_seconds=large_session_window_seconds if alone else window_seconds,
                  max_canonical_rows=max_canonical_rows,_known_plan=known,planned_only=planned_only)
            # Before writers restart: any later write gets a newer xid.
            snapshot=_post_commit_snapshot(warehouse)
        elapsed=time.monotonic()-started
        state['completed'].append(dict(sessions=[list(s) for s in sessions],canonical_rows=total,seconds=elapsed,
                                       snapshot=snapshot))
        _checkpoint(path,plan)
        if progress: progress(dict(completed_groups=len(state['completed']),canonical_rows=total,window_seconds=elapsed))
    return plan


def _apply_plan(warehouse, plan, page_size, workers, selector=None, *, planned_only=False, phase=lambda name: None):
    phase('sanitize')
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
    with ConfirmationPool(workers,selector,tally=False) as pool:
        sources=[]
        # A derived row of an already-active session is regenerated or masked in
        # place whatever its count; confirming it here only cost window time.
        unsettled=[(key,row) for key,row in current.items()
                   if key[0] not in DERIVED or session_identity(row) not in active]
        for ((table,_),row),matches in zip(unsettled,pool.counts([row for _,row in unsettled]),strict=True):
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
    phase('regenerate_timeline')
    new_keys, regenerated = rebuild_sessions(warehouse,active,page_size,selector)
    # A stale copy is one the canonical rebuild no longer produces (an older
    # turn builder, or a source event a re-upload removed); timeline sync never
    # deletes it. It is masked where it is, not deleted. A key the rebuild DID
    # write, but for another session, is not stale: refuse it as before.
    stale_keys=({(key,identity) for key,identity in old_timeline if (*key,identity) not in regenerated}|
                {(key,identity) for _,_,key,identity in chunks if (*key,identity) not in regenerated})
    if any(key in new_keys for key,_ in stale_keys):
        raise RuntimeError('canonical event missing for derived copy; no changes committed')
    stale_events={key for key,_ in stale_keys}
    for (adapter,event_id),_ in old_timeline:
        if (adapter,event_id) not in new_keys and (adapter,event_id) not in stale_events:
            warehouse._command('DELETE FROM @timeline_events WHERE adapter=%s AND event_id=%s',(adapter,event_id))
    phase('mask_stale_copies')
    stale_rows=[(table,row) for (table,_),row in current.items()
                if table in DERIVED and ((row['adapter'],row['event_id']),session_identity(row)) in stale_keys]
    _mask_stale_copies(warehouse,stale_rows,workers,selector,page_size)
    chunks=[c for c in chunks if (c[2],c[3]) not in stale_keys]
    phase('regenerate_chunks_embeddings')
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


def _mask_stale_copies(warehouse, rows, workers, selector, page_size):
    """Mask selected spans in derived copies the rebuild cannot regenerate.

    The row is kept and only changed columns are written; a copy with nothing
    selected stays byte-identical. A masked timeline row takes a new seq so the
    chunk builder re-chunks it from the masked text; a masked chunk gets its
    new text hash and built_at (the embedding drain's watermark), and the old
    hash's embedding is dropped, as for a regenerated chunk.
    """
    updates={}; old_shas=[]
    with ConfirmationPool(workers,selector,tally=False) as pool:
        for offset in range(0,len(rows),page_size):
            batch=rows[offset:offset+page_size]
            for (table,row),cleaned in zip(batch,pool.redacted([row for _,row in batch]),strict=True):
                keys=POSTGRES_TABLES[table].primary_key
                columns=[k for k in row if k not in keys and cleaned[k]!=row[k]]
                if not columns: continue
                sets=[f'{k}=%s' for k in columns]
                values=[Json(cleaned[k]) if isinstance(cleaned[k],(dict,list)) else cleaned[k] for k in columns]
                if table=='timeline_events':
                    sets+=['seq=nextval(%s::regclass)','updated_at=now()']
                    values.append(warehouse.sql_relation('timeline_events_seq'))
                else:
                    if 'text' in columns:
                        sets+=['text_sha256=%s','char_count=%s']
                        values+=[hashlib.sha256(cleaned['text'].encode()).hexdigest(),len(cleaned['text'])]
                        old_shas.append(row['text_sha256'])
                    sets.append('built_at=now()')
                updates.setdefault((table,tuple(sets)),[]).append(tuple(values)+tuple(row[k] for k in keys))
    for (table,sets),params in updates.items():
        keys=POSTGRES_TABLES[table].primary_key
        sql=f'UPDATE @{table} SET '+','.join(sets)+' WHERE '+' AND '.join(f'{k}=%s' for k in keys)
        with warehouse._connection.cursor() as cursor:
            execute_batch(cursor,warehouse._expand_relations(sql),params,page_size=100)
    for offset in range(0,len(old_shas),500):
        warehouse._command('DELETE FROM @search_chunk_embeddings WHERE text_sha256 = ANY(%s)',(old_shas[offset:offset+500],))


def rebuild_sessions(warehouse, sessions, page_size, selector=None):
    from personal_data_warehouse.postgres import _AI_EVENT_TABLE_BY_SOURCE
    from personal_data_warehouse.timeline import (TIMELINE_MASKED_COLUMNS, TIMELINE_NORMALIZED_COLUMNS,
                                                  TimelineSyncEngine, adapter_by_name, masked_rows)

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
    checked = [TIMELINE_NORMALIZED_COLUMNS.index(c) for c in TIMELINE_MASKED_COLUMNS]
    for name in ('agent_session', 'agent_session_turn'):
        adapter = adapter_by_name(name)
        watermark_ts, watermark_id = stamp, ''
        while True:
            sql=adapter.incremental_sql.replace('@ai_conversation_events','pg_temp.card_scrub_events')
            rows = engine._fetch(sql, dict(watermark_ts=watermark_ts, watermark_id=watermark_id, limit=page_size))
            if not rows:
                break
            selected = []
            # The same post-cut masking ingest-time sync applies, checked
            # before the write: a cut fragment is masked, anything else refuses.
            for row in (masked_rows(rows) if adapter.mask_credentials else rows):
                identity = session_identity(dict(source_pk=row[9]))
                if identity not in sessions:
                    continue
                if any((selector.count if selector else sensitive_count)(str(row[i])) for i in checked):
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
            engine._upsert(adapter, selected, masked=True)
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
    parser.add_argument('--large-session-window-seconds',type=int,default=LARGE_SESSION_WINDOW_SECONDS,
                        help='session batches: deadline (<= 300) for a lone session over 10,000 rows or over --max-group-mb')
    parser.add_argument('--max-group-mb',type=int,default=MAX_GROUP_BYTES//1_000_000,
                        help='session batches: prefiltered megabytes per group; a heavier session runs alone')
    parser.add_argument('--session-batches',action='store_true',help='apply complete sessions in separate bounded transactions')
    parser.add_argument('--planned-only',action='store_true',help='credentials only: indexed planned-session repair; unrelated sessions require full post-restore verify')
    parser.add_argument('--writers-paused',action='store_true',help='acknowledge external writer shutdown and guaranteed restoration')
    parser.add_argument('--max-canonical-rows',type=int,default=5000)
    parser.add_argument('--max-groups',type=int,help='session batches: stop after N groups; a rerun resumes')
    parser.add_argument('--report-html', type=Path, help='counts-only table for the private email body')
    add_selector_arguments(parser)
    args = parser.parse_args()
    if args.workers < 1: parser.error('--workers must be positive')
    if not 0 < args.window_seconds <= 60: parser.error('--window-seconds must be 1..60')
    if args.apply and args.verify: parser.error('--apply and --verify are mutually exclusive')
    if args.only_session and (args.planned_only or args.session_batches):
        parser.error('--only-session is a seed repair; it cannot combine with credential session batches')
    if args.planned_only or args.session_batches:
        if not (args.apply and args.planned_only and args.session_batches and args.writers_paused and args.scope=='credentials'):
            parser.error('session batches require --apply --scope credentials --planned-only --writers-paused')
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
                    max_plan_rows=args.max_plan_rows,max_delta_rows=args.max_delta_rows,max_groups=args.max_groups,
                    large_session_window_seconds=args.large_session_window_seconds,max_group_bytes=args.max_group_mb*1_000_000)
                if plan['batches']['oversized']: raise RuntimeError('oversized sessions remain; repair incomplete')
                done={tuple(s) for g in plan['batches']['completed'] for s in g['sessions']}
                left={tuple(c['session']) for c in plan['candidates']}-done
                if left: raise SystemExit(f'{len(left)} planned sessions remain; rerun apply to continue')
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
                plan['session_bytes']=session_bytes_entries(warehouse,plan,selector.scope)
                _checkpoint(args.plan,plan)
            else:
                plan = discover(warehouse,workers=args.workers,selector=selector)
                plan['session_bytes']=session_bytes_entries(warehouse,plan,selector.scope)
                write_plan(args.plan,plan)
            before = plan['counts']
        table = month_source_report(before, after)
        print(json.dumps({'counts_by_month_source': table, 'details_before': before, 'details_after': after, 'verification_pending':args.apply, 'left_alone': plan.get('left_alone') if not args.verify else None}))
        if args.report_html:
            args.report_html.write_text(report_html(table))
        if args.verify and args.scope == 'credentials' and any(r['measure']=='matches' and r['count'] for r in after):
            raise SystemExit('credential verification failed: secrets remain')
    finally:
        warehouse.close()


if __name__ == '__main__':
    main()
