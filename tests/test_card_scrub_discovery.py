"""Discovery must be fast without making a negative decision on encoded data."""
import json
from pathlib import Path

import pytest
from psycopg2.extras import execute_values

from personal_data_warehouse.card_redaction import card_count
from personal_data_warehouse.schema import AGENT_SESSION_EVENT_COLUMNS
from tests.test_conversation_card_storage import event, warehouse


def test_sql_prefilter_is_superset_of_shared_and_generated_cases(warehouse):
    from personal_data_warehouse.card_prefilter import might_contain_card, sql_prefilter
    from tests.card_redaction_properties import property_cases
    cases = json.loads((Path(__file__).parent/'fixtures/card_redaction_cases.json').read_text())['cases']
    values = [c['input'] for c in cases] + [value for _,value,_ in property_cases()]
    # Arbitrarily long raw backslash runs must not defeat a bounded normalized window.
    encoded = ''.join(r'\u%04x'%ord(c) for c in '4111111111111111')
    for _ in range(9):
        encoded = json.dumps({'payload':encoded})
    values.append(encoded)
    # Escape the backslash AND the 'u' and numeric characters of each escape.
    # Counting only literal u003X units would be an unsafe negative filter.
    units=''.join('\\u%04x'%ord(c) for c in ''.join(r'\u%04x'%ord(c) for c in '4111111111111111'))
    values.append('{"text":"'+units+'"}')
    expected = {i for i,value in enumerate(values) if card_count(value)}
    assert expected <= {i for i,value in enumerate(values) if might_contain_card(value)}
    warehouse._command('CREATE TEMP TABLE scrub_prefilter_cases (id integer, value text)')
    with warehouse._connection.cursor() as cursor:
        execute_values(cursor,'INSERT INTO scrub_prefilter_cases VALUES %s',list(enumerate(values)),page_size=500)
    selected = {r[0] for r in warehouse._query('SELECT id FROM scrub_prefilter_cases WHERE '+sql_prefilter('value'))}
    assert expected <= selected, sorted(expected-selected)[:10]
    wrapped = {r[0] for r in warehouse._query("SELECT id FROM scrub_prefilter_cases WHERE "+sql_prefilter("jsonb_build_object('value',value)::text"))}
    assert expected <= wrapped, sorted(expected-wrapped)[:10]


def test_parallel_confirmation_agrees_with_serial():
    from personal_data_warehouse.card_prefilter import ConfirmationPool
    rows=[{'text':'4111111111111111'} if i%2 else {'text':'safe'} for i in range(200)]
    with ConfirmationPool(2) as pool:
        assert pool.counts(rows) == [i%2 for i in range(200)]
        assert pool.redacted(rows) == [{'text':'[card ****1111]'} if i%2 else {'text':'safe'} for i in range(200)]
        assert pool._pool is not None


def test_object_apply_uses_only_saved_candidate_ids(tmp_path, monkeypatch):
    import gzip
    from types import SimpleNamespace
    from personal_data_warehouse.scrub_conversation_objects import discover_objects, scrub_objects
    monkeypatch.chdir(tmp_path)
    class Store:
        def __init__(self):
            self.objects={str(i):gzip.compress(b'{"text":"safe"}\n') for i in range(100)}
            self.objects['candidate']=gzip.compress(b'{"text":"4111111111111111"}\n')
            self.reads=[]
        def list_objects(self,**kw):
            return [SimpleNamespace(ref={'storage_file_id':key,'storage_key':key},app_properties={}) for key in self.objects]
        def get_object(self,ref):
            self.reads.append(ref['storage_file_id']);return self.objects[ref['storage_file_id']]
        def put_file(self,**kw):
            self.objects['replacement']=kw['path'].read_bytes()
            return {'storage_file_id':'replacement','storage_key':'candidate'}
        def delete_object(self,ref):del self.objects[ref['storage_file_id']]
    store=Store();plan=discover_objects(store,workers=2)
    assert plan['counts']['matches']==1
    assert '4111111111111111' not in json.dumps(plan)
    store.reads=[]
    scrub_objects(store,apply=True,plan=plan,workers=2)
    assert set(store.reads) == {'candidate','replacement'}


def test_discovery_plan_is_read_only_ids_only_and_reused(warehouse, tmp_path, monkeypatch):
    import personal_data_warehouse.scrub_conversation_cards as module
    row = event()
    warehouse._insert_rows('claude_code_events',[row],AGENT_SESSION_EVENT_COLUMNS)
    plan = module.discover(warehouse,workers=1)
    path = tmp_path/'plan.json'
    module.write_plan(path,plan)
    assert path.stat().st_mode & 0o777 == 0o600
    persisted=path.read_text()
    assert row['text'] not in persisted and '4111111111111111' not in persisted
    assert set(plan['candidates'][0]) == {'table','key','session','month','source','matches'}
    before=warehouse._query('SELECT count(*) FROM @claude_code_events')[0][0]
    assert card_count(warehouse._query('SELECT text FROM @claude_code_events')[0][0])
    with pytest.raises(FileExistsError): module.write_plan(path,plan)
    # Candidate application must not invoke another whole-corpus discovery.
    real_discover=module.discover
    real_related=module._related_rows
    def verify_only(*args,**kwargs):
        assert kwargs.get('connection') is warehouse._connection
        return real_discover(*args,**kwargs)
    monkeypatch.setattr(module,'discover',verify_only)
    def deltas_only(*args,**kwargs):
        assert kwargs.get('delta') is not None
        return real_related(*args,**kwargs)
    monkeypatch.setattr(module,'_related_rows',deltas_only)
    module.scrub(warehouse,apply=True,plan=module.read_plan(path),workers=1)
    assert warehouse._query('SELECT count(*) FROM @claude_code_events')[0][0] == before
    assert not card_count(warehouse._query('SELECT text FROM @claude_code_events')[0][0])


def test_apply_rechecks_plan_and_rolls_back_for_new_candidate(warehouse):
    from personal_data_warehouse.scrub_conversation_cards import discover, scrub
    first=event()
    warehouse._insert_rows('claude_code_events',[first],AGENT_SESSION_EVENT_COLUMNS)
    plan=discover(warehouse,workers=1)
    # A separate session appeared after the live discovery snapshot.
    second=dict(first,event_uuid='new-event',session_id='new-session')
    warehouse._insert_rows('claude_code_events',[second],AGENT_SESSION_EVENT_COLUMNS)
    with pytest.raises(RuntimeError,match='new candidates'):
        scrub(warehouse,apply=True,plan=plan,workers=1)
    assert all(card_count(r[0]) for r in warehouse._query('SELECT text FROM @claude_code_events'))


def test_read_only_constructor_and_database_discovery(warehouse):
    from personal_data_warehouse.postgres import PostgresWarehouse
    from personal_data_warehouse.scrub_conversation_cards import discover
    readonly=PostgresWarehouse(warehouse._database_url,schema=warehouse._schema,initialize=False)
    try:
        readonly._connection.set_session(readonly=True,autocommit=True)
        assert discover(readonly,workers=1)['candidates'] == []
    finally: readonly.close()


def test_plan_is_reverified_against_current_session_identity(warehouse):
    from personal_data_warehouse.scrub_conversation_cards import discover, scrub
    row=event();warehouse._insert_rows('claude_code_events',[row],AGENT_SESSION_EVENT_COLUMNS)
    plan=discover(warehouse,workers=1)
    # Cached reporting metadata cannot substitute for the canonical row identity.
    plan['candidates'][0]['session']=['claude_code','stale-session']
    scrub(warehouse,apply=True,plan=plan,workers=1)
    assert not discover(warehouse,workers=1)['candidates']


def test_cached_plan_regenerates_partial_copies_if_source_was_cleaned(warehouse):
    from personal_data_warehouse.card_redaction import redact_row
    from personal_data_warehouse.scrub_conversation_cards import discover, scrub
    row=event();warehouse._insert_rows('claude_code_events',[row],AGENT_SESSION_EVENT_COLUMNS)
    warehouse._command("INSERT INTO @timeline_events (adapter,event_id,source,context,snippet) VALUES ('agent_session_turn','test-event','claude_code','claude_code|test-session',%s)",('partial '+row['text'][:8]+' CVV: 987',))
    plan=discover(warehouse,workers=1)
    clean=redact_row(row)
    warehouse._command('UPDATE @claude_code_events SET text=%s,raw_json=%s',(clean['text'],clean['raw_json']))
    scrub(warehouse,apply=True,plan=plan,workers=1)
    assert warehouse._query("SELECT count(*) FROM @timeline_events WHERE snippet LIKE 'partial %'")[0][0]==0


def test_live_discovery_uses_one_snapshot_without_blocking_writer(warehouse, monkeypatch):
    import threading
    from concurrent.futures import ThreadPoolExecutor
    import personal_data_warehouse.scrub_conversation_cards as module
    row=event();warehouse._insert_rows('claude_code_events',[row],AGENT_SESSION_EVENT_COLUMNS)
    entered=threading.Event();release=threading.Event();real_scan=module._scan_table
    def wait_for_update(*args,**kwargs):
        assert kwargs['snapshot']
        entered.set();assert release.wait(10)
        return real_scan(*args,**kwargs)
    monkeypatch.setattr(module,'_scan_table',wait_for_update)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future=executor.submit(module.discover,warehouse,workers=2)
        try:
            assert entered.wait(10)
            warehouse._command("UPDATE @claude_code_events SET text='safe',raw_json='{}'")
        finally:release.set()
        assert len(future.result(timeout=20)['candidates'])==1


def test_parallel_block_ranges_cover_all_seeded_candidates(warehouse,monkeypatch):
    import personal_data_warehouse.scrub_conversation_cards as module
    warehouse._command("""INSERT INTO @claude_code_events (source,session_id,event_uuid,text)
      SELECT 'claude_code','parallel-session-'||i::text,'parallel-event-'||i::text,
      %s||repeat('padding-text-',80) FROM generate_series(1,10000) i""",('4111111111111111 ',))
    ranges=[];real_scan=module._scan_table
    def record(*args,**kwargs):
        if args[1]=='claude_code_events' and kwargs.get('blocks'):ranges.append(kwargs['blocks'])
        return real_scan(*args,**kwargs)
    monkeypatch.setattr(module,'_scan_table',record)
    plan=module.discover(warehouse,workers=4)
    assert len(plan['candidates'])==10000
    assert len(ranges)>1
    ordered=sorted(ranges)
    assert ordered[0][0]==0
    assert all(left[1]==right[0] for left,right in zip(ordered,ordered[1:]))


def test_canonical_rebuild_does_not_rewrite_unaffected_source_timestamps(warehouse):
    from datetime import UTC, datetime
    from personal_data_warehouse.scrub_conversation_cards import discover, scrub
    row=dict(event(),ingested_at=datetime(2020,1,1,tzinfo=UTC))
    safe=dict(row,event_uuid='safe-neighbor',seq=2,text='safe content',raw_json='{}')
    warehouse._insert_rows('claude_code_events',[row,safe],AGENT_SESSION_EVENT_COLUMNS)
    before=warehouse._query('SELECT event_uuid,ingested_at FROM @claude_code_events ORDER BY event_uuid')
    scrub(warehouse,apply=True,plan=discover(warehouse,workers=1),workers=1)
    assert warehouse._query('SELECT event_uuid,ingested_at FROM @claude_code_events ORDER BY event_uuid')==before
