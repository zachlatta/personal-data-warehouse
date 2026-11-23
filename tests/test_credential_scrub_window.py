"""Indexed token repair windows handle large sessions without corpus scans."""
from contextlib import nullcontext
import json

import pytest

from tests.test_conversation_card_storage import event, warehouse
from tests.test_credential_scrub_scope import TOKEN, MASKED, CARD
from personal_data_warehouse.schema import AGENT_SESSION_EVENT_COLUMNS
from personal_data_warehouse.card_scrub_selector import ScrubSelector


def test_large_session_planned_batches_never_scan_delta(warehouse, tmp_path, monkeypatch):
    from personal_data_warehouse import scrub_conversation_cards as m
    from personal_data_warehouse.card_scrub_selector import ScrubSelector
    selector = ScrubSelector('credentials')
    row = dict(event(), text=CARD + TOKEN, raw_json=json.dumps({'text': TOKEN}))
    warehouse._insert_rows('claude_code_events', [row], AGENT_SESSION_EVENT_COLUMNS)
    warehouse._command("INSERT INTO @claude_code_events (source,session_id,event_uuid,seq,role,text) "
                       "SELECT 'claude_code','test-session','safe-'||i,i+1,'user','safe' FROM generate_series(1,7060) i")
    from personal_data_warehouse.timeline import TimelineSyncEngine, adapter_by_name
    from personal_data_warehouse.search_index import SearchChunkBuilder
    engine=TimelineSyncEngine(source_url=warehouse._database_url,source_schema=warehouse._schema,
                             dest_schema=warehouse._schema,adapters=[adapter_by_name(n) for n in ('agent_session','agent_session_turn')])
    try: engine.run()
    finally: engine.close()
    SearchChunkBuilder(warehouse).run()
    path = tmp_path/'plan.json'
    m.write_plan(path, m.discover(warehouse, selector=selector))
    monkeypatch.setattr(m, '_selected_delta', lambda *a, **k: pytest.fail('corpus delta scan in planned window'))
    monkeypatch.setattr(m, '_related_rows', lambda *a, **k: pytest.fail('unindexed derived scan in planned window'))
    plan = m.apply_batches(warehouse, path, window=lambda _: nullcontext(), selector=selector,
                           planned_only=True, max_canonical_rows=10000, gap_seconds=0)
    assert not plan['batches']['oversized']
    assert plan['batches']['completed'][0]['canonical_rows'] == 7061
    assert warehouse._query('SELECT count(*) FROM @timeline_events')[0][0] == 7062
    assert warehouse._query('SELECT count(*) FROM @search_chunks')[0][0] == 0
    assert warehouse._query('SELECT text FROM @claude_code_events WHERE event_uuid=%s', (row['event_uuid'],))[0][0] == CARD + MASKED
    # Resume does not repeat a committed session.
    m.apply_batches(warehouse, path, window=lambda _: nullcontext(), selector=selector,
                    planned_only=True, max_canonical_rows=10000, gap_seconds=0)


def test_indexed_window_rechecks_session_growth_and_new_chunk(warehouse):
    from personal_data_warehouse import scrub_conversation_cards as m
    from personal_data_warehouse.card_scrub_selector import ScrubSelector
    from personal_data_warehouse.search_index import SearchChunkBuilder
    from personal_data_warehouse.timeline import TimelineSyncEngine, adapter_by_name
    selector = ScrubSelector('credentials')
    row = dict(event(), text=TOKEN, raw_json='{}')
    warehouse._insert_rows('claude_code_events', [row], AGENT_SESSION_EVENT_COLUMNS)
    engine = TimelineSyncEngine(source_url=warehouse._database_url, source_schema=warehouse._schema,
                                dest_schema=warehouse._schema, adapters=[adapter_by_name(n) for n in ('agent_session','agent_session_turn')])
    try:
        engine.run()
        plan = m.discover(warehouse, selector=selector)
        # A chunk written after discovery still gets found via its indexed anchor.
        SearchChunkBuilder(warehouse).run()
        warehouse._command("INSERT INTO @search_chunk_embeddings (text_sha256,model) SELECT DISTINCT text_sha256,'test' FROM @search_chunks")
        warehouse._insert_rows('claude_code_events', [dict(row,event_uuid='late',seq=2)], AGENT_SESSION_EVENT_COLUMNS)
        with pytest.raises(RuntimeError, match='new candidates'):
            m.scrub(warehouse, apply=True, plan=plan, selector=selector, planned_only=True)
        warehouse._command("DELETE FROM @claude_code_events WHERE event_uuid='late'")
        m.scrub(warehouse, apply=True, plan=plan, selector=selector, planned_only=True)
        assert warehouse._query('SELECT count(*) FROM @search_chunks')[0][0] == 0
        assert warehouse._query('SELECT count(*) FROM @search_chunk_embeddings')[0][0] == 0
        assert not m.discover(warehouse, selector=selector)['candidates']
    finally:
        engine.close()


def test_planned_only_is_explicit_and_credential_only(warehouse):
    from personal_data_warehouse import scrub_conversation_cards as m
    plan = m.discover(warehouse)
    with pytest.raises(ValueError, match='credential'):
        m.scrub(warehouse, apply=True, plan=plan, planned_only=True)


def test_batch_cli_requires_shutdown_acknowledgment(monkeypatch):
    import sys
    from personal_data_warehouse import scrub_conversation_cards as m
    monkeypatch.setattr(sys,'argv',['scrub','--scope','credentials','--apply','--session-batches','--planned-only'])
    with pytest.raises(SystemExit) as error: m.main()
    assert error.value.code == 2


def test_batch_cli_commits_and_global_verify_still_finds_unrelated_session(warehouse, tmp_path, monkeypatch):
    import sys
    from personal_data_warehouse import scrub_conversation_cards as m
    from personal_data_warehouse.card_scrub_selector import ScrubSelector
    selector=ScrubSelector('credentials')
    row=dict(event(),text=TOKEN,raw_json='{}')
    warehouse._insert_rows('claude_code_events',[row],AGENT_SESSION_EVENT_COLUMNS)
    path=tmp_path/'plan.json'
    m.write_plan(path,m.discover(warehouse,selector=selector))
    warehouse._insert_rows('claude_code_events',[dict(row,session_id='unrelated')],AGENT_SESSION_EVENT_COLUMNS)
    monkeypatch.setattr(m,'PostgresWarehouse',lambda *a,**k:warehouse)
    monkeypatch.setattr(warehouse,'close',lambda:None)
    monkeypatch.setattr(sys,'argv',['scrub','--scope','credentials','--apply','--session-batches',
                                 '--planned-only','--writers-paused','--workers','1','--plan',str(path)])
    m.main()
    assert len(m.read_plan(path)['batches']['completed']) == 1
    assert warehouse._query("SELECT text FROM @claude_code_events WHERE session_id='test-session'")[0][0] == MASKED
    remaining=m.discover(warehouse,selector=selector)
    assert {tuple(c['session']) for c in remaining['candidates']} == {('claude_code','unrelated')}


def test_plan_wide_row_gates_follow_the_explicit_flags(tmp_path, monkeypatch):
    import personal_data_warehouse.scrub_conversation_cards as module
    candidates = [dict(table='claude_code_events', key=['claude_code', f's{i}', f'e{i}'], session=['claude_code', f's{i}'],
                       month='2026-11', source='claude_code', matches=1) for i in range(501)]
    plan = dict(version=2, scope=['db', None, None, 'public'], watermarks={'timeline_events': 0, 'search_chunks': '', 'snapshot': '1:1:'},
                candidates=candidates, counts=module._details(candidates), selector=ScrubSelector('credentials').specification, left_alone=0)
    path = tmp_path / 'plan.json'
    path.write_text(json.dumps(plan))
    monkeypatch.setattr(module, '_scope', lambda warehouse: plan['scope'])
    monkeypatch.setattr(module, '_planned_rows', lambda warehouse, candidates, **k: [])
    monkeypatch.setattr(module, '_canonical_counts', lambda warehouse, sessions, limit: {})
    from contextlib import nullcontext
    with pytest.raises(ValueError, match='candidate gate exceeded'):
        module.apply_batches(None, path, window=lambda _: nullcontext(), selector=ScrubSelector('credentials'), planned_only=True)
    result = module.apply_batches(None, path, window=lambda _: nullcontext(), selector=ScrubSelector('credentials'),
                                  planned_only=True, max_source_rows=501, max_plan_rows=501)
    assert result['batches']['completed'] == []


def test_session_groups_can_be_applied_a_bounded_number_at_a_time(tmp_path, monkeypatch):
    import personal_data_warehouse.scrub_conversation_cards as module
    candidates = [dict(table='claude_code_events', key=['claude_code', f's{i}', f'e{i}'], session=['claude_code', f's{i}'],
                       month='2026-11', source='claude_code', matches=1) for i in range(3)]
    plan = dict(version=2, scope=['db', None, None, 'public'], watermarks={'timeline_events': 0, 'search_chunks': '', 'snapshot': '1:1:'},
                candidates=candidates, counts=module._details(candidates), selector=ScrubSelector('credentials').specification, left_alone=0)
    path = tmp_path / 'plan.json'
    path.write_text(json.dumps(plan))
    applied = []
    monkeypatch.setattr(module, '_scope', lambda warehouse: plan['scope'])
    monkeypatch.setattr(module, '_planned_rows', lambda warehouse, candidates, **k: [])
    monkeypatch.setattr(module, '_canonical_counts', lambda warehouse, sessions, limit: {s: 12000 for s in sessions})
    monkeypatch.setattr(module, 'scrub', lambda warehouse, plan, **k: applied.append([c['session'] for c in plan['candidates']]))
    kwargs = dict(window=lambda _: nullcontext(), selector=ScrubSelector('credentials'), planned_only=True,
                  max_canonical_rows=12000, gap_seconds=0)
    first = module.apply_batches(None, path, max_groups=2, **kwargs)
    assert len(applied) == 2 and len(first['batches']['completed']) == 2 and first['batches']['oversized'] == []
    second = module.apply_batches(None, path, max_groups=2, **kwargs)
    assert len(applied) == 3 and len(second['batches']['completed']) == 3
    with pytest.raises(ValueError, match='budget'):
        module.apply_batches(None, path, max_canonical_rows=30001, **{k: v for k, v in kwargs.items() if k != 'max_canonical_rows'})


def test_credential_selector_skips_full_confirmation_without_an_anchor(monkeypatch):
    from personal_data_warehouse import credential_redaction
    selector = ScrubSelector('credentials')
    calls = []
    original = credential_redaction.credential_edits
    monkeypatch.setattr(credential_redaction, 'credential_edits', lambda value: calls.append(1) or original(value))
    assert selector.count_row({'text': 'ordinary prose ' * 500, 'raw_json': {'k': 'v'}, 'seq': 1}) == 0
    assert calls == []
    assert selector.count_row({'text': 'use ' + TOKEN}) == 1 and calls


def test_planned_session_recheck_reads_only_prefiltered_rows(warehouse):
    from personal_data_warehouse import scrub_conversation_cards as m
    from personal_data_warehouse.card_prefilter import ConfirmationPool
    selector = ScrubSelector('credentials')
    rows = [dict(event(), event_uuid=f'p{i}', seq=i + 1, text='plain words', raw_json='{}') for i in range(5)]
    rows.append(dict(event(), event_uuid='dirty', seq=99, text='use ' + TOKEN, raw_json='{}'))
    warehouse._insert_rows('claude_code_events', rows, AGENT_SESSION_EVENT_COLUMNS)
    read = []
    original = warehouse._query_dicts
    warehouse._query_dicts = lambda sql, params=None: read.extend(r := original(sql, params)) or r
    plan = dict(candidates=[dict(table='claude_code_events', key=['claude_code', 'test-session', 'dirty'],
                                 session=['claude_code', 'test-session'], month='2026-11', source='claude_code', matches=1)])
    with ConfirmationPool(1, selector) as pool:
        selected = m._session_candidates(warehouse, plan, pool)
    assert [s['key'][2] for s in selected] == ['dirty']
    assert [r['event_uuid'] for r in read] == ['dirty']


def test_planned_rows_can_read_only_prefiltered_rows(warehouse):
    from personal_data_warehouse import scrub_conversation_cards as m
    rows = [dict(event(), event_uuid=f'p{i}', seq=i + 1, text='plain words', raw_json='{}') for i in range(3)]
    rows.append(dict(event(), event_uuid='dirty', seq=99, text='use ' + TOKEN, raw_json='{}'))
    warehouse._insert_rows('claude_code_events', rows, AGENT_SESSION_EVENT_COLUMNS)
    candidates = [dict(table='claude_code_events', key=['claude_code', 'test-session', r['event_uuid']]) for r in rows]
    assert len(m._planned_rows(warehouse, candidates)) == 4
    assert [r['event_uuid'] for _, r in m._planned_rows(warehouse, candidates, prefilter_scope='credentials')] == ['dirty']


def test_a_large_session_runs_alone_under_its_own_deadline(tmp_path, monkeypatch):
    import personal_data_warehouse.scrub_conversation_cards as module
    sessions = {('claude_code', 'big'): 25000, ('claude_code', 'a'): 100, ('claude_code', 'b'): 200}
    candidates = [dict(table='claude_code_events', key=['claude_code', s, 'e'], session=['claude_code', s],
                       month='2026-11', source='claude_code', matches=1) for _, s in sessions]
    plan = dict(version=2, scope=['db', None, None, 'public'], watermarks={'timeline_events': 0, 'search_chunks': '', 'snapshot': '1:1:'},
                candidates=candidates, counts=module._details(candidates), selector=ScrubSelector('credentials').specification, left_alone=0)
    path = tmp_path / 'plan.json'
    path.write_text(json.dumps(plan))
    windows = {}
    monkeypatch.setattr(module, '_scope', lambda warehouse: plan['scope'])
    monkeypatch.setattr(module, '_planned_rows', lambda warehouse, candidates, **k: [])
    monkeypatch.setattr(module, '_canonical_counts', lambda warehouse, s, limit: {k: sessions[k] for k in s})
    monkeypatch.setattr(module, 'scrub', lambda warehouse, plan, **k: windows.update(
        {tuple(sorted(c['session'][1] for c in plan['candidates'])): k['window_seconds']}))
    module.apply_batches(None, path, window=lambda _: nullcontext(), selector=ScrubSelector('credentials'), planned_only=True,
                         max_canonical_rows=30000, window_seconds=60, gap_seconds=0, large_session_window_seconds=180)
    assert windows == {('big',): 180, ('a', 'b'): 60}
    with pytest.raises(ValueError, match='budget'):
        module.apply_batches(None, path, window=lambda _: nullcontext(), selector=ScrubSelector('credentials'),
                             planned_only=True, large_session_window_seconds=301)
