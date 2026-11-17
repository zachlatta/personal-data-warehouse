"""Session repairs commit independently and retain private resume evidence."""
import pytest

from personal_data_warehouse.schema import AGENT_SESSION_EVENT_COLUMNS
from tests.test_conversation_card_storage import event, warehouse


def seed(warehouse, sessions=3, rows=3):
    for i in range(sessions):
        row = event()
        row['session_id'] = f'batch-{i}'
        warehouse._insert_rows('claude_code_events', [row], AGENT_SESSION_EVENT_COLUMNS)
        warehouse._command("""INSERT INTO @claude_code_events (source,session_id,event_uuid,text)
            SELECT 'claude_code',%s,'clean-'||i,'safe' FROM generate_series(1,%s) i""",
            (row['session_id'], rows-1))


def test_groups_rollback_resume_and_restore(warehouse, tmp_path):
    from contextlib import contextmanager
    from personal_data_warehouse import scrub_conversation_cards as m
    seed(warehouse)
    plan=m.discover(warehouse)
    path=tmp_path/'plan.json'
    m.write_plan(path,plan)
    windows=[]
    @contextmanager
    def window(index):
        windows.append(('pause',index))
        try: yield
        finally: windows.append(('restore',index))
    original=m.rebuild_sessions
    def fail(w,s,*args,**kwargs):
        if ('claude_code','batch-1') in s: raise RuntimeError('injected failure')
        return original(w,s,*args,**kwargs)
    from unittest.mock import patch
    with patch.object(m,'rebuild_sessions',fail), pytest.raises(RuntimeError,match='injected'):
        m.apply_batches(warehouse,path,window=window,max_canonical_rows=3,gap_seconds=0)
    saved=m.read_plan(path)
    assert len(saved['batches']['completed'])==1
    assert windows==[('pause',0),('restore',0),('pause',1),('restore',1)]
    assert len([c for c in m.discover(warehouse)['candidates'] if c['table']=='claude_code_events'])==2
    result=m.apply_batches(warehouse,path,window=window,max_canonical_rows=3,gap_seconds=0)
    assert len(result['batches']['completed'])==3
    assert not m.discover(warehouse)['candidates']
    assert path.stat().st_mode & 0o777==0o600
    assert '4111111111111111' not in path.read_text()


def test_partial_opt_in_and_oversized_session(warehouse,tmp_path):
    from contextlib import nullcontext
    from personal_data_warehouse import scrub_conversation_cards as m
    seed(warehouse,2,4)
    plan=m.discover(warehouse)
    plan['discovery']={'jobs':[['claude_code_events','1970-01']], 'completed':[]}
    path=tmp_path/'plan.json'
    m.write_plan(path,plan)
    with pytest.raises(ValueError,match='incomplete'):
        m.apply_batches(warehouse,path,window=lambda _:nullcontext(),gap_seconds=0)
    result=m.apply_batches(warehouse,path,window=lambda _:nullcontext(),allow_partial=True,
                           max_canonical_rows=3,gap_seconds=0)
    assert len(result['batches']['oversized'])==2
    assert not result['batches']['completed']
    assert result['watermarks']==plan['watermarks']
    assert result['discovery']==plan['discovery']


def test_group_gate_rechecks_growth_and_unknown_delta(warehouse,tmp_path):
    from contextlib import contextmanager
    from personal_data_warehouse import scrub_conversation_cards as m
    seed(warehouse,1,3)
    path=tmp_path/'plan.json'
    m.write_plan(path,m.discover(warehouse))
    @contextmanager
    def growing(_):
        warehouse._command("INSERT INTO @claude_code_events (source,session_id,event_uuid,text) VALUES ('claude_code','batch-0','growth','safe')")
        yield
    with pytest.raises(ValueError,match='canonical'):
        m.apply_batches(warehouse,path,window=growing,max_canonical_rows=3,gap_seconds=0)
    assert not m.read_plan(path)['batches']['completed']


def test_commit_before_checkpoint_is_replayed_safely(warehouse,tmp_path,monkeypatch):
    from contextlib import nullcontext
    from personal_data_warehouse import scrub_conversation_cards as m
    seed(warehouse,1,3)
    path=tmp_path/'plan.json'
    m.write_plan(path,m.discover(warehouse))
    checkpoint=m._checkpoint
    def interrupt(path,plan):
        if plan.get('batches',{}).get('completed'): raise InterruptedError()
        checkpoint(path,plan)
    monkeypatch.setattr(m,'_checkpoint',interrupt)
    with pytest.raises(InterruptedError):
        m.apply_batches(warehouse,path,window=lambda _:nullcontext(),gap_seconds=0)
    assert not m.read_plan(path)['batches']['completed']
    monkeypatch.setattr(m,'_checkpoint',checkpoint)
    result=m.apply_batches(warehouse,path,window=lambda _:nullcontext(),gap_seconds=0)
    assert len(result['batches']['completed'])==1
    assert not m.discover(warehouse)['candidates']


@pytest.mark.parametrize('partial',[False,True])
def test_unknown_selected_delta_stops_and_restores(warehouse,tmp_path,partial):
    from contextlib import contextmanager
    from personal_data_warehouse import scrub_conversation_cards as m
    seed(warehouse,1,3)
    path=tmp_path/'plan.json'
    plan=m.discover(warehouse)
    if partial: plan['discovery']={'jobs':[['claude_code_events','1970-01']], 'completed':[]}
    m.write_plan(path,plan)
    restored=[]
    @contextmanager
    def window(_):
        row=dict(event(),session_id='new-session')
        warehouse._insert_rows('claude_code_events',[row],AGENT_SESSION_EVENT_COLUMNS)
        try: yield
        finally: restored.append(True)
    with pytest.raises(RuntimeError,match='new candidates'):
        m.apply_batches(warehouse,path,window=window,gap_seconds=0,allow_partial=partial)
    assert restored==[True]
    assert not m.read_plan(path)['batches']['completed']


def test_partial_apply_then_finish_partition_discovery(warehouse,tmp_path):
    from contextlib import nullcontext
    from personal_data_warehouse import scrub_conversation_cards as m
    seed(warehouse,1,3)
    warehouse._command("INSERT INTO @claude_code_events (source,session_id,event_uuid,text,occurred_at) VALUES ('claude_code','later','safe','masked','2026-02-01')")
    full=m.discover(warehouse)
    candidates=[c for c in full['candidates'] if c['session']==['claude_code','batch-0']]
    plan=dict(full,candidates=candidates,counts=m._details(candidates),discovery={
        'jobs':[['claude_code_events','1970-01'],['claude_code_events','2026-02']],
        'completed':[['claude_code_events','1970-01']]})
    path=tmp_path/'plan.json';m.write_plan(path,plan)
    m.apply_batches(warehouse,path,window=lambda _:nullcontext(),allow_partial=True,gap_seconds=0)
    resumed=m.discover_resumable(warehouse,path)
    assert 'discovery' not in resumed
    assert len(resumed['batches']['completed'])==1
    m.apply_batches(warehouse,path,window=lambda _:nullcontext(),gap_seconds=0)
    assert not m.discover(warehouse)['candidates']


def test_concurrent_clean_writer_between_groups(warehouse,tmp_path):
    import threading
    import psycopg2
    from contextlib import nullcontext
    from personal_data_warehouse import scrub_conversation_cards as m
    seed(warehouse)
    path=tmp_path/'plan.json';m.write_plan(path,m.discover(warehouse))
    writes=[]
    def progress(counts):
        if 'completed_groups' not in counts: return
        def write():
            conn=psycopg2.connect(warehouse._database_url)
            try:
                with conn,conn.cursor() as cur:
                    cur.execute(warehouse._expand_relations("INSERT INTO @claude_code_events (source,session_id,event_uuid,text) VALUES ('claude_code','live',%s,'masked')"),(str(len(writes)),))
                writes.append(True)
            finally: conn.close()
        thread=threading.Thread(target=write);thread.start();thread.join(10)
        assert not thread.is_alive()
    m.apply_batches(warehouse,path,window=lambda _:nullcontext(),max_canonical_rows=3,
                    gap_seconds=0,progress=progress)
    assert len(writes)==3
    assert not m.discover(warehouse)['candidates']
