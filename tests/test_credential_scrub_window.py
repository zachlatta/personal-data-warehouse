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
    monkeypatch.setattr(module, '_session_bytes', lambda warehouse, sessions, scope: {})
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
    monkeypatch.setattr(module, '_session_bytes', lambda warehouse, sessions, scope: {s: 1000 for s in sessions})
    monkeypatch.setattr(module, '_post_commit_snapshot', lambda warehouse: '1:1:')
    monkeypatch.setattr(module, '_written_since', lambda warehouse, sessions, snapshot: [])
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
    monkeypatch.setattr(module, '_session_bytes', lambda warehouse, s, scope: {k: 1000 for k in s})
    monkeypatch.setattr(module, '_post_commit_snapshot', lambda warehouse: '1:1:')
    monkeypatch.setattr(module, '_written_since', lambda warehouse, sessions, snapshot: [])
    monkeypatch.setattr(module, 'scrub', lambda warehouse, plan, **k: windows.update(
        {tuple(sorted(c['session'][1] for c in plan['candidates'])): k['window_seconds']}))
    module.apply_batches(None, path, window=lambda _: nullcontext(), selector=ScrubSelector('credentials'), planned_only=True,
                         max_canonical_rows=30000, window_seconds=60, gap_seconds=0, large_session_window_seconds=180)
    assert windows == {('big',): 180, ('a', 'b'): 60}
    with pytest.raises(ValueError, match='budget'):
        module.apply_batches(None, path, window=lambda _: nullcontext(), selector=ScrubSelector('credentials'),
                             planned_only=True, large_session_window_seconds=301)


def _byte_plan(module, tmp_path, names):
    candidates = [dict(table='claude_code_events', key=['claude_code', s, 'e'], session=['claude_code', s],
                       month='2026-11', source='claude_code', matches=1) for s in names]
    plan = dict(version=2, scope=['db', None, None, 'public'], watermarks={'timeline_events': 0, 'search_chunks': '', 'snapshot': '1:1:'},
                candidates=candidates, counts=module._details(candidates), selector=ScrubSelector('credentials').specification, left_alone=0)
    path = tmp_path / 'plan.json'
    path.write_text(json.dumps(plan))
    return plan, path


def test_groups_are_sized_by_prefiltered_bytes_and_a_heavy_session_gets_the_long_deadline(tmp_path, monkeypatch):
    """Recheck time follows the bytes the prefilter passes, not the row count.

    A few rows can hold 150 MB of tool output, so a row cap alone cannot keep a
    group inside its 60-second transaction. A session heavier than the byte cap
    runs alone under the large-session deadline, which defaults to the 300 s
    maximum so a heavy session can always finish.
    """
    import personal_data_warehouse.scrub_conversation_cards as module
    mb = 1_000_000
    rows = {'heavy': 40, 'a': 100, 'b': 100, 'c': 100, 'd': 100}
    sizes = {'heavy': 150 * mb, 'a': 15 * mb, 'b': 15 * mb, 'c': 15 * mb, 'd': 1 * mb}
    plan, path = _byte_plan(module, tmp_path, rows)
    windows = []
    monkeypatch.setattr(module, '_scope', lambda warehouse: plan['scope'])
    monkeypatch.setattr(module, '_planned_rows', lambda warehouse, candidates, **k: [])
    monkeypatch.setattr(module, '_canonical_counts', lambda warehouse, s, limit: {k: rows[k[1]] for k in sorted(s)})
    scopes = []
    monkeypatch.setattr(module, '_session_bytes', lambda warehouse, s, scope: scopes.append(scope) or {k: sizes[k[1]] for k in s})
    monkeypatch.setattr(module, '_post_commit_snapshot', lambda warehouse: '1:1:')
    monkeypatch.setattr(module, '_written_since', lambda warehouse, sessions, snapshot: [])
    monkeypatch.setattr(module, 'scrub', lambda warehouse, plan, **k: windows.append(
        (tuple(sorted(c['session'][1] for c in plan['candidates'])), k['window_seconds'])))
    module.apply_batches(None, path, window=lambda _: nullcontext(), selector=ScrubSelector('credentials'), planned_only=True,
                         max_canonical_rows=5000, window_seconds=60, gap_seconds=0, max_group_bytes=40 * mb)
    assert scopes == ['credentials']
    assert sorted(windows) == [(('a', 'b'), 60), (('c', 'd'), 60), (('heavy',), 300)]
    with pytest.raises(ValueError, match='budget'):
        module.apply_batches(None, path, window=lambda _: nullcontext(), selector=ScrubSelector('credentials'),
                             planned_only=True, max_group_bytes=0)


def test_session_bytes_count_only_rows_the_prefilter_passes(warehouse):
    from personal_data_warehouse import scrub_conversation_cards as m
    plain = [dict(event(), event_uuid=f'p{i}', seq=i + 1, text='plain words ' * 1000, raw_json='{}') for i in range(3)]
    dirty = dict(event(), event_uuid='dirty', seq=99, text='use ' + TOKEN, raw_json='{}')
    warehouse._insert_rows('claude_code_events', plain + [dirty], AGENT_SESSION_EVENT_COLUMNS)
    [(expected,)] = warehouse._query("SELECT octet_length(to_jsonb(c)::text) FROM @claude_code_events c WHERE event_uuid='dirty'")
    sizes = m._session_bytes(warehouse, {('claude_code', 'test-session'), ('claude_code', 'absent')}, 'credentials')
    assert sizes == {('claude_code', 'test-session'): expected, ('claude_code', 'absent'): 0}


def test_checkpoint_validation_does_not_rescan_candidates_per_completed_group():
    """Each progress save validates the whole checkpoint while writers are stopped.

    Rebuilding the set of planned sessions once per completed group made a save
    cost groups x candidates (about 80 s per save at production size).
    """
    import personal_data_warehouse.scrub_conversation_cards as module

    class Counted(list):
        reads = 0

        def __iter__(self):
            Counted.reads += 1
            return super().__iter__()

    def reads(groups):
        candidates = [dict(table='claude_code_events', key=['claude_code', f's{i}', 'e'], session=['claude_code', f's{i}'],
                           month='2026-11', source='claude_code', matches=1) for i in range(50)]
        plan = dict(version=2, scope=['db', None, None, 'public'],
                    watermarks={'timeline_events': 0, 'search_chunks': '', 'snapshot': '1:1:'},
                    candidates=candidates, counts=module._details(candidates),
                    batches=dict(completed=[dict(sessions=[['claude_code', f's{i}']], canonical_rows=1, seconds=1.0, snapshot='1:1:')
                                            for i in range(groups)], oversized=[]))
        plan['candidates'] = Counted(candidates)
        Counted.reads = 0
        module._validate_plan(plan)
        return Counted.reads

    assert reads(1) == reads(40)
    with pytest.raises(ValueError, match='invalid batch session'):
        module._validate_plan(dict(version=2, scope=['db', None, None, 'public'],
                                   watermarks={'timeline_events': 0, 'search_chunks': '', 'snapshot': '1:1:'},
                                   candidates=[], counts=[],
                                   batches=dict(completed=[dict(sessions=[['claude_code', 'unknown']], canonical_rows=1,
                                                                seconds=1.0, snapshot='1:1:')], oversized=[])))


def test_a_completed_session_is_rechecked_only_for_rows_written_after_its_commit(warehouse, tmp_path, monkeypatch):
    """Every apply run starts with writers stopped and rechecks completed sessions.

    Reconfirming every finished row grew with progress (the whole backfill by the
    last run). Each completed group records a snapshot taken after its commit,
    so a later run reads only source rows written since; a secret re-ingested
    into a completed session still stops the run.
    """
    from personal_data_warehouse import scrub_conversation_cards as m
    selector = ScrubSelector('credentials')
    rows = [dict(event(), session_id=s, event_uuid=f'{s}-dirty', text='use ' + TOKEN, raw_json='{}') for s in ('s-a', 's-b')]
    warehouse._insert_rows('claude_code_events', rows, AGENT_SESSION_EVENT_COLUMNS)
    path = tmp_path / 'plan.json'
    m.write_plan(path, m.discover(warehouse, selector=selector))
    kwargs = dict(window=lambda _: nullcontext(), selector=selector, planned_only=True, gap_seconds=0, max_groups=1,
                  max_canonical_rows=1)
    first = m.apply_batches(warehouse, path, **kwargs)
    assert [g['sessions'] for g in first['batches']['completed']] == [[['claude_code', 's-a']]]
    assert first['batches']['completed'][0]['snapshot']
    counted = []
    original = ScrubSelector.count_row
    monkeypatch.setattr(ScrubSelector, 'count_row', lambda self, row: counted.append(row.get('session_id')) or original(self, row))
    m.apply_batches(warehouse, path, **kwargs)
    assert 's-a' not in counted and 's-b' in counted
    warehouse._insert_rows('claude_code_events', [dict(rows[0], event_uuid='s-a-again', seq=7)], AGENT_SESSION_EVENT_COLUMNS)
    with pytest.raises(RuntimeError, match='completed session changed'):
        m.apply_batches(warehouse, path, **kwargs)


def test_rows_written_since_include_a_transaction_in_progress_at_the_snapshot(warehouse):
    import psycopg2
    from personal_data_warehouse import scrub_conversation_cards as m
    warehouse._insert_rows('claude_code_events', [dict(event(), event_uuid='before', raw_json='{}')], AGENT_SESSION_EVENT_COLUMNS)
    other = psycopg2.connect(warehouse._database_url)
    try:
        with other.cursor() as cursor:
            cursor.execute(warehouse._expand_relations(
                "INSERT INTO @claude_code_events (source,session_id,event_uuid,seq,role,text) "
                "VALUES ('claude_code','test-session','in-progress',5,'user','late')"))
        snapshot = m._post_commit_snapshot(warehouse)
        other.commit()
    finally:
        other.close()
    warehouse._insert_rows('claude_code_events', [dict(event(), event_uuid='after', seq=9, raw_json='{}')], AGENT_SESSION_EVENT_COLUMNS)
    rows = m._written_since(warehouse, {('claude_code', 'test-session')}, snapshot)
    assert sorted(r['event_uuid'] for r in rows) == ['after', 'in-progress']


def test_discovery_records_session_bytes_so_the_outage_does_not_measure_them(warehouse, tmp_path, monkeypatch):
    from personal_data_warehouse import scrub_conversation_cards as m
    row = dict(event(), text='use ' + TOKEN, raw_json='{}')
    warehouse._insert_rows('claude_code_events', [row], AGENT_SESSION_EVENT_COLUMNS)
    path = tmp_path / 'plan.json'
    monkeypatch.setattr(m, 'PostgresWarehouse', lambda *a, **k: warehouse)
    monkeypatch.setattr(warehouse, 'close', lambda: None)
    monkeypatch.setattr('sys.argv', ['scrub', '--scope', 'credentials', '--plan', str(path), '--workers', '1'])
    m.main()
    warehouse._connection.set_session(readonly=False)  # discovery opened it read-only
    plan = m.read_plan(path)
    expected = m._session_bytes(warehouse, {('claude_code', 'test-session')}, 'credentials')
    assert plan['session_bytes'] == [['claude_code', 'test-session', expected[('claude_code', 'test-session')]]]
    monkeypatch.setattr(m, '_session_bytes', lambda *a, **k: pytest.fail('session bytes measured while writers are stopped'))
    state = m.apply_batches(warehouse, path, window=lambda _: nullcontext(), selector=ScrubSelector('credentials'),
                            planned_only=True, gap_seconds=0)
    assert len(state['batches']['completed']) == 1


def test_the_rebuild_pages_by_the_adapter_batch_not_the_scrub_page(warehouse, monkeypatch):
    """Each rebuild page re-reads and sorts the whole group's temporary view, so
    500-row pages made regeneration superlinear in a session's size."""
    from personal_data_warehouse import scrub_conversation_cards as m
    from personal_data_warehouse.timeline import TimelineSyncEngine
    rows = [dict(event(), event_uuid=f't{i}', seq=i + 1, text=f'turn {i}', raw_json='{}') for i in range(1200)]
    warehouse._insert_rows('claude_code_events', rows, AGENT_SESSION_EVENT_COLUMNS)
    fetched = []
    original = TimelineSyncEngine._fetch
    monkeypatch.setattr(TimelineSyncEngine, '_fetch', lambda self, sql, params: fetched.append(params['limit']) or original(self, sql, params))
    keys, _ = m.rebuild_sessions(warehouse, {('claude_code', 'test-session')}, 500, ScrubSelector('credentials'))
    assert len([k for k in keys if k[0] == 'agent_session_turn']) == 1200
    assert len(fetched) == 4  # one page and one empty page per adapter


def test_sanitize_does_not_confirm_derived_copies_the_rebuild_replaces(warehouse, monkeypatch):
    """A derived row of a session already being rebuilt is regenerated or masked
    in place whatever its count, so confirming it in sanitize was wasted time."""
    from personal_data_warehouse import scrub_conversation_cards as m
    selector = ScrubSelector('credentials')
    rows = [dict(event(), event_uuid=f't{i}', seq=i + 1, text=f'turn {i} ' + TOKEN, raw_json='{}') for i in range(3)]
    warehouse._insert_rows('claude_code_events', rows, AGENT_SESSION_EVENT_COLUMNS)
    from personal_data_warehouse.timeline import TimelineSyncEngine, adapter_by_name
    engine = TimelineSyncEngine(source_url=warehouse._database_url, source_schema=warehouse._schema, dest_schema=warehouse._schema,
                                adapters=[adapter_by_name(n) for n in ('agent_session', 'agent_session_turn')])
    try:
        engine.run()
    finally:
        engine.close()
    # The copies an older sync wrote, before ingest masked the source.
    warehouse._command("UPDATE @timeline_events SET search_text = search_text || ' ' || %s", (TOKEN,))
    plan = m.discover(warehouse, selector=selector)
    timings = []
    in_sanitize = []
    original = ScrubSelector.count_row
    monkeypatch.setattr(ScrubSelector, 'count_row', lambda self, row: in_sanitize.append(
        'adapter' in row and bool(timings) and timings[-1]['phase'] == 'recount') or original(self, row))
    m.scrub(warehouse, apply=True, plan=plan, selector=selector, planned_only=True, timings=timings)
    assert 'sanitize' in [t['phase'] for t in timings]
    assert not any(in_sanitize)
    assert not m.discover(warehouse, selector=selector)['candidates']


def test_the_timed_window_skips_the_left_alone_recount(warehouse, tmp_path, monkeypatch):
    """The left-alone statistic belongs to discovery; inside the window it doubled the recheck."""
    from personal_data_warehouse import scrub_conversation_cards as m
    from personal_data_warehouse import card_prefilter
    selector = ScrubSelector('credentials')
    row = dict(event(), text=CARD + ' use ' + TOKEN, raw_json=json.dumps({'text': TOKEN}))
    warehouse._insert_rows('claude_code_events', [row], AGENT_SESSION_EVENT_COLUMNS)
    plan = m.discover(warehouse, selector=selector)
    assert plan['left_alone'] > 0  # the card is left alone by the credential scope
    path = tmp_path / 'plan.json'
    m.write_plan(path, plan)
    monkeypatch.setattr(card_prefilter, 'count_row', lambda row: pytest.fail('full sensitive recount in the timed window'))
    state = m.apply_batches(warehouse, path, window=lambda _: nullcontext(), selector=selector, planned_only=True, gap_seconds=0)
    assert len(state['batches']['completed']) == 1
    assert warehouse._query('SELECT text FROM @claude_code_events WHERE event_uuid=%s', (row['event_uuid'],))[0][0] == CARD + ' use ' + MASKED
