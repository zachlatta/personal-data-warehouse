"""A retained exact selector repairs one session without broadening its scope."""
from contextlib import nullcontext
import json

import pytest

from personal_data_warehouse import scrub_conversation_cards as m
from personal_data_warehouse.card_scrub_selector import resolve_selector
from personal_data_warehouse.schema import AGENT_SESSION_EVENT_COLUMNS
from personal_data_warehouse.scrub_conversation_jsonl import scrub_jsonl
from tests.test_conversation_card_storage import event, warehouse


def test_one_session_retains_selector_after_connection_loss(warehouse, tmp_path):
    chosen = dict(event(), session_id='chosen')
    other = dict(event(), session_id='other')
    # Published processor test fixture; deliberately distinct from event().
    unrelated = 'card: ' + ''.join(['5' * 12, '4444']) + ' CVV: 123'
    chosen['text'] += '\n' + unrelated
    warehouse._insert_rows('claude_code_events', [chosen, other], AGENT_SESSION_EVENT_COLUMNS)
    key = {k: chosen[k] for k in ('source', 'session_id', 'event_uuid')}
    identity = 'claude_code_events:' + json.dumps(key)
    # Resolve from a seed row containing only the selected test reading.
    warehouse._command('UPDATE @claude_code_events SET text=%s WHERE session_id=%s',
                       (event()['text'], 'chosen'))
    retained = resolve_selector(warehouse, 'labelled', [identity])
    warehouse._command('UPDATE @claude_code_events SET text=%s WHERE session_id=%s',
                       (chosen['text'], 'chosen'))
    plan = m.discover(warehouse, selector=retained)
    plan['discovery'] = {'jobs': [['claude_code_events', '1970-01']],
                         'completed': [['claude_code_events', '1970-01']]}
    exact = retained.seed_only(plan['selector'])
    before_other = warehouse._query_dicts('SELECT * FROM @claude_code_events WHERE session_id=%s', ('other',))
    with pytest.raises(ValueError, match='exact source'):
        m.apply_session(warehouse, plan, ('claude_code', 'chosen'), selector=exact,
                        window=lambda _: nullcontext(), exact_source_rows=2)
    with pytest.raises(ValueError, match='canonical'):
        m.apply_session(warehouse, plan, ('claude_code', 'chosen'), selector=exact,
                        window=lambda _: nullcontext(), exact_source_rows=1, max_canonical_rows=0)
    m.apply_session(warehouse, plan, ('claude_code', 'chosen'), selector=exact,
                    window=lambda _: nullcontext(), exact_source_rows=1)
    assert warehouse._query_dicts('SELECT * FROM @claude_code_events WHERE session_id=%s', ('other',)) == before_other
    assert unrelated in warehouse._query('SELECT text FROM @claude_code_events WHERE session_id=%s', ('chosen',))[0][0]
    # The bound selector can be rebound without touching the sanitized seed.
    warehouse._connection.close()
    import psycopg2
    warehouse._connection = psycopg2.connect(warehouse._database_url)
    warehouse._connection.autocommit = True
    replacement = warehouse
    try:
        resumed = retained.seed_only(plan['selector'])
        m.apply_session(replacement, plan, ('claude_code', 'chosen'), selector=resumed,
                        window=lambda _: nullcontext(), exact_source_rows=1, allow_clean=True)
        path = tmp_path / 'chosen.jsonl'
        path.write_text(json.dumps({'text': chosen['text']}) + '\n')
        assert scrub_jsonl(tmp_path, 'chosen', apply=True, selector=resumed)['matches'] > 0
        assert unrelated in path.read_text()
        assert scrub_jsonl(tmp_path, 'chosen', selector=resumed)['matches'] == 0
    finally:
        pass  # The fixture owns the replacement connection too.


def test_incomplete_or_duplicate_partitions_refused(warehouse):
    plan = m.discover(warehouse)
    job = ['claude_code_events', '1970-01']
    for completed in ([], [job, job]):
        plan['discovery'] = {'jobs': [job], 'completed': completed}
        with pytest.raises(ValueError, match='partition'):
            m.session_subplan(warehouse, plan, ('claude_code', 'chosen'), selector=None)


def test_keeper_recovers_interface_error_and_verifies_globally(warehouse, tmp_path, monkeypatch):
    import psycopg2
    from contextlib import contextmanager
    from personal_data_warehouse.card_scrub_keeper import SessionKeeper
    row = event()
    warehouse._insert_rows('claude_code_events', [row], AGENT_SESSION_EVENT_COLUMNS)
    identity = 'claude_code_events:' + json.dumps({k: row[k] for k in ('source','session_id','event_uuid')})
    bound = resolve_selector(warehouse, 'labelled', [identity])
    plan = m.discover(warehouse, selector=bound)
    keeper = SessionKeeper(warehouse, plan, ('claude_code', row['session_id']))
    # Deployment handover must bind the replacement before sanitation; its
    # own retained selector remains usable after the old owner goes away.
    replacement_owner = SessionKeeper(warehouse, plan, ('claude_code', row['session_id']))
    assert replacement_owner.selector.specification == keeper.selector.specification
    assert replacement_owner.selector.seeds == keeper.selector.seeds
    restored = []
    @contextmanager
    def broken(_):
        try:
            warehouse._connection.close()
            yield
        finally:
            restored.append(True)
    with pytest.raises(psycopg2.InterfaceError):
        keeper.apply(window=broken, exact_source_rows=1)
    assert restored == [True]
    def connect():
        warehouse._connection = psycopg2.connect(warehouse._database_url)
        warehouse._connection.autocommit = True
        return warehouse
    keeper.reconnect(connect)
    keeper.apply(window=lambda _:nullcontext(), exact_source_rows=1)
    assert replacement_owner.selector.count(row['text']) > 0
    # Reconnect AGAIN after seed sanitation; forbid re-resolution explicitly.
    monkeypatch.setattr('personal_data_warehouse.card_scrub_keeper.resolve_selector',
                        lambda *a, **kw: pytest.fail('sanitized seed read'))
    warehouse._connection.close()
    keeper.reconnect(connect)
    path = tmp_path / (row['session_id'] + '.jsonl')
    path.write_text(json.dumps({'text': row['text']}) + '\n')
    keeper.jsonl(tmp_path, apply=True)
    assert keeper.jsonl(tmp_path)['matches'] == 0
    assert not keeper.verify()['candidates']
    assert keeper.plan['watermarks'] == plan['watermarks']
    assert keeper.plan['candidates'] == plan['candidates']


def test_exact_gate_rechecks_known_row_becoming_selected(warehouse):
    from contextlib import contextmanager
    row = dict(event(), raw_json='{}')
    other = dict(row, event_uuid='other', text='card: ' + ''.join(['5'*12, '4444']) + ' CVV: 123')
    warehouse._insert_rows('claude_code_events', [row, other], AGENT_SESSION_EVENT_COLUMNS)
    identity = 'claude_code_events:' + json.dumps({k: row[k] for k in ('source','session_id','event_uuid')})
    bound = resolve_selector(warehouse, 'labelled', [identity])
    plan = m.discover(warehouse, selector=bound)
    selector = bound.seed_only(plan['selector'])
    restored = []
    @contextmanager
    def window(_):
        warehouse._command('UPDATE @claude_code_events SET text=%s WHERE event_uuid=%s', (row['text'], 'other'))
        try: yield
        finally: restored.append(True)
    with pytest.raises(ValueError, match='source-row gate'):
        m.apply_session(warehouse, plan, ('claude_code', row['session_id']),
                        selector=selector, exact_source_rows=1, window=window)
    assert restored == [True]
    assert warehouse._query("SELECT count(*) FROM @claude_code_events WHERE text LIKE '%%[card %%'")[0][0] == 0
    assert warehouse._query('SELECT text FROM @claude_code_events WHERE event_uuid=%s', ('test-event',))[0][0] == row['text']


def test_live_fence_keeps_original_horizon_and_checks_racing_unknown_row(warehouse):
    from contextlib import contextmanager
    row = dict(event(), raw_json='{}')
    warehouse._insert_rows('claude_code_events', [row], AGENT_SESSION_EVENT_COLUMNS)
    identity = 'claude_code_events:' + json.dumps({k: row[k] for k in ('source','session_id','event_uuid')})
    retained = resolve_selector(warehouse, 'labelled', [identity])
    plan = m.discover(warehouse, selector=retained)
    original = dict(plan['watermarks'])
    @contextmanager
    def racing(_):
        warehouse._insert_rows('claude_code_events', [dict(row,session_id='other',event_uuid='late')], AGENT_SESSION_EVENT_COLUMNS)
        yield
    timings = []
    with pytest.raises(RuntimeError, match='new candidates'):
        m.apply_session(warehouse, plan, ('claude_code',row['session_id']),
                        selector=retained.seed_only(plan['selector']),
                        window=racing, exact_source_rows=1, timings=timings)
    assert plan['watermarks'] == original
    assert [t['phase'] for t in timings] == ['lock_acquisition','recount','delta_guard']
    assert warehouse._query('SELECT text FROM @claude_code_events WHERE event_uuid=%s', (row['event_uuid'],))[0][0] == row['text']


def test_live_fence_checks_unknown_changes_before_the_new_snapshot(warehouse):
    row = dict(event(), raw_json='{}')
    warehouse._insert_rows('claude_code_events', [row], AGENT_SESSION_EVENT_COLUMNS)
    identity = 'claude_code_events:' + json.dumps({k: row[k] for k in ('source','session_id','event_uuid')})
    retained = resolve_selector(warehouse, 'labelled', [identity])
    plan = m.discover(warehouse, selector=retained)
    warehouse._insert_rows('claude_code_events', [dict(row,session_id='other',event_uuid='late')], AGENT_SESSION_EVENT_COLUMNS)
    with pytest.raises(RuntimeError, match='new candidates'):
        m.apply_session(warehouse, plan, ('claude_code',row['session_id']),
                        selector=retained.seed_only(plan['selector']),
                        window=lambda _:pytest.fail('pause before live confirmation'), exact_source_rows=1)


def test_live_fence_includes_active_parent_savepoint_committed_at_pause(warehouse):
    import psycopg2
    from contextlib import contextmanager
    row = dict(event(), raw_json='{}')
    warehouse._insert_rows('claude_code_events', [row], AGENT_SESSION_EVENT_COLUMNS)
    identity = 'claude_code_events:' + json.dumps({k: row[k] for k in ('source','session_id','event_uuid')})
    retained = resolve_selector(warehouse, 'labelled', [identity])
    plan = m.discover(warehouse, selector=retained)
    with psycopg2.connect(warehouse._database_url) as writer:
        with writer.cursor() as cursor:
            cursor.execute('SAVEPOINT child')
            cursor.execute(warehouse._expand_relations("INSERT INTO @claude_code_events (source,session_id,event_uuid,text) VALUES ('claude_code','other','child',%s)"), (row['text'],))
            cursor.execute('RELEASE SAVEPOINT child')
        @contextmanager
        def racing(_):
            writer.commit()
            yield
        with pytest.raises(RuntimeError, match='new candidates'):
            m.apply_session(warehouse, plan, ('claude_code',row['session_id']),
                            selector=retained.seed_only(plan['selector']),
                            window=racing, exact_source_rows=1)


@pytest.mark.parametrize('scope', ['seed', 'labelled', 'all'])
@pytest.mark.parametrize('selected', [0, 1, 2])
def test_locked_source_gate_and_clean_replay_in_every_scope(warehouse, monkeypatch, scope, selected):
    from personal_data_warehouse.card_scrub_selector import ScrubSelector
    row = dict(event(), raw_json='{}')
    warehouse._insert_rows('claude_code_events', [row, dict(row,event_uuid='other')], AGENT_SESSION_EVENT_COLUMNS)
    selector = ScrubSelector(scope, seeds=('4111111111111111',))
    plan = m.discover(warehouse, selector=selector)
    # Keep both known identities, but the saved preflight sees only one match.
    plan['candidates'][1]['matches'] = 0
    plan['counts'] = m._details(plan['candidates'])
    if selected < 2:
        warehouse._command('UPDATE @claude_code_events SET text=%s WHERE event_uuid=%s', ('safe', 'other'))
    if selected == 0:
        warehouse._command('UPDATE @claude_code_events SET text=%s', ('safe',))
    original = m._apply_plan
    called = []
    def apply(*args, **kwargs):
        called.append(True)
        return original(*args, **kwargs)
    monkeypatch.setattr(m, '_apply_plan', apply)
    if selected != 1:
        with pytest.raises(ValueError, match='source-row gate'):
            m.scrub(warehouse, apply=True, plan=plan, selector=selector,
                    max_source_rows=1, exact_source_rows=1)
        assert not called
    if selected == 0:
        m.scrub(warehouse, apply=True, plan=plan, selector=selector,
                max_source_rows=1, exact_source_rows=1, allow_clean=True)
    elif selected == 1:
        m.scrub(warehouse, apply=True, plan=plan, selector=selector,
                max_source_rows=1, exact_source_rows=1)
    else:
        # The current-count ceiling also applies without an exact count.
        with pytest.raises(ValueError, match='source-row gate'):
            m.scrub(warehouse, apply=True, plan=plan, selector=selector, max_source_rows=1)
