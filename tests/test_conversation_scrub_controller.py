"""The deployment window records state before stop and restores on any failure."""
import json
from pathlib import Path

import pytest


def test_controller_restores_partial_stop_and_apply_failures(tmp_path, monkeypatch):
    from personal_data_warehouse import scrub_conversation_window as m
    state={'writers':[{'Id':'dagster'}, {'Id':'app'}], 'image':'image', 'network':'net'}
    (tmp_path/'state.json').write_text(json.dumps(state))
    calls=[]
    monkeypatch.setattr(m,'snapshot',lambda state: {'schedules': [], 'runs': []})
    monkeypatch.setattr(m,'assert_current',lambda state: None)
    def docker(args, **kwargs):
        calls.append(args)
        if args[0]=='stop': raise RuntimeError('partial stop')
        return ''
    monkeypatch.setattr(m,'docker',docker)
    with pytest.raises(RuntimeError,match='partial stop'):
        m.apply(tmp_path)
    assert calls[-2:] == [['start','app'], ['start','dagster']]
    assert (tmp_path/'before-stop.json').exists()
    calls.clear()
    def docker(args, **kwargs):
        calls.append(args)
        if args[0]=='run': raise RuntimeError('repair failed')
        return ''
    monkeypatch.setattr(m,'docker',docker)
    with pytest.raises(RuntimeError,match='repair failed'):
        m.apply(tmp_path)
    assert calls[-2:] == [['start','app'], ['start','dagster']]


def test_controller_runtime_has_no_secret_arguments(tmp_path, monkeypatch):
    from personal_data_warehouse import scrub_conversation_window as m
    calls=[]
    monkeypatch.setattr(m,'docker',lambda args, **k: calls.append(args))
    m.runtime(tmp_path,{'image':'image','network':'net'},'cards', '--apply','--session-batches')
    args=calls[0]
    assert '--env-file' in args and str(tmp_path/'runtime.env') in args
    assert '--entrypoint' in args and '/app/.venv/bin/python' in args
    assert 'personal_data_warehouse.scrub_conversation_cards' in args
    assert '--scope' in args and 'tokens' in args


def test_controller_private_state_files(tmp_path):
    from personal_data_warehouse import scrub_conversation_window as m
    path=tmp_path/'state.json'
    m.save(path,{'safe':'state'})
    assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError): m.save(path,{'other':'state'})


def test_controller_stops_surviving_runtime_before_restoring(tmp_path, monkeypatch):
    from personal_data_warehouse import scrub_conversation_window as m
    (tmp_path/'state.json').write_text(json.dumps({'writers':[{'Id':'dagster'},{'Id':'app'}]}))
    calls=[]
    def docker(args, **kwargs):
        calls.append(args)
        return 'maintenance-id' if args[0]=='ps' else ''
    monkeypatch.setattr(m,'docker',docker)
    m.restore(tmp_path)
    assert calls[1:] == [['stop','--time','30','maintenance-id'], ['start','app'], ['start','dagster']]


def test_runtime_mounts_nothing_from_the_writer(tmp_path, monkeypatch):
    # The scrub reads Postgres and Drive through its environment only; the
    # Dagster container's volumes include the Docker socket.
    from personal_data_warehouse import scrub_conversation_window as m
    calls=[]
    monkeypatch.setattr(m,'docker',lambda args, **k: calls.append(args))
    m.runtime(tmp_path,{'writers':[{'Id':'dagster'}],'image':'image','network':'net'},'cards')
    assert '--volumes-from' not in calls[0]
    assert not any('docker.sock' in a for a in calls[0])


def test_apply_passes_the_group_cap_so_a_retry_can_isolate_a_session(tmp_path, monkeypatch):
    from personal_data_warehouse import scrub_conversation_window as m
    (tmp_path/'state.json').write_text(json.dumps({'writers':[{'Id':'dagster'},{'Id':'app'}],'image':'i','network':'n'}))
    calls=[]
    monkeypatch.setattr(m,'snapshot',lambda state: {'schedules': [], 'runs': []})
    monkeypatch.setattr(m,'assert_current',lambda state: None)
    monkeypatch.setattr(m,'docker',lambda args, **k: calls.append(args) or '')
    m.apply(tmp_path,max_canonical_rows=7061)
    cards=next(c for c in calls if 'personal_data_warehouse.scrub_conversation_cards' in c)
    assert cards[cards.index('--max-canonical-rows')+1] == '7061'
    with pytest.raises(ValueError):
        m.apply(tmp_path,max_canonical_rows=10001)


def test_verify_removes_the_runtime_environment_only_after_clean_scans(tmp_path, monkeypatch):
    from personal_data_warehouse import scrub_conversation_window as m
    (tmp_path/'state.json').write_text(json.dumps({'writers':[{'Id':'dagster'},{'Id':'app'}],'image':'i','network':'n'}))
    (tmp_path/'before-stop.json').write_text(json.dumps({'schedules': [], 'runs': []}))
    (tmp_path/'runtime.env').write_text('KEY=value\n')
    monkeypatch.setattr(m,'snapshot',lambda state: {'schedules': [], 'runs': ['other']})
    monkeypatch.setattr(m,'assert_current',lambda state: None)
    def failing(args, **k):
        if args[0]=='run': raise RuntimeError('secrets remain')
    monkeypatch.setattr(m,'docker',failing)
    with pytest.raises(RuntimeError): m.verify(tmp_path)
    assert (tmp_path/'runtime.env').exists()
    monkeypatch.setattr(m,'docker',lambda args, **k: '')
    m.verify(tmp_path)
    assert not (tmp_path/'runtime.env').exists()


def test_jsonl_token_cli_reports_remaining_secret(tmp_path,monkeypatch):
    import sys
    from personal_data_warehouse import scrub_conversation_jsonl as m
    from tests.test_token_scrub_scope import TOKEN
    path=tmp_path/'session.jsonl'
    path.write_text(json.dumps({'text':TOKEN})+'\n')
    args=['scrub','--scope','tokens','--root',str(tmp_path),'--session','session']
    monkeypatch.setattr(sys,'argv',args)
    with pytest.raises(SystemExit,match='secrets remain'): m.main()
    monkeypatch.setattr(sys,'argv',args+['--apply'])
    m.main()


def test_database_window_never_runs_archive(tmp_path, monkeypatch):
    from personal_data_warehouse import scrub_conversation_window as m
    state={'writers':[{'Id':'dagster'},{'Id':'app'}],'image':'i','network':'n'}
    (tmp_path/'state.json').write_text(json.dumps(state))
    calls=[]
    monkeypatch.setattr(m,'assert_current',lambda _: None)
    monkeypatch.setattr(m,'snapshot',lambda _: {'schedules':[]})
    monkeypatch.setattr(m,'docker',lambda *a,**k: '')
    monkeypatch.setattr(m,'runtime',lambda d,s,module,*a: calls.append(module))
    m.apply(tmp_path)
    m.verify(tmp_path)
    assert calls == ['cards','cards']


def test_cleanup_confined_and_refuses_unknown_files_and_links(tmp_path, monkeypatch):
    from personal_data_warehouse import scrub_conversation_window as m
    monkeypatch.setattr(m,'MAINTENANCE_ROOT',tmp_path)
    monkeypatch.setattr(m,'docker',lambda *a,**k: '')
    directory=tmp_path/'stale'
    directory.mkdir()
    (directory/'runtime.env').write_text('secret')
    (directory/'unowned').write_text('keep')
    with pytest.raises(ValueError): m.cleanup(directory)
    assert (directory/'runtime.env').exists()
    (directory/'unowned').unlink()
    (directory/'state.json').symlink_to(tmp_path/'outside')
    with pytest.raises(ValueError): m.cleanup(directory)
    (directory/'state.json').unlink()
    m.cleanup(directory)
    assert not directory.exists()
    m.cleanup(directory)  # retry
    with pytest.raises(ValueError): m.cleanup(tmp_path)
    with pytest.raises(ValueError): m.cleanup(tmp_path.parent/'outside')


def test_live_archive_never_stops_writers(tmp_path, monkeypatch):
    from personal_data_warehouse import scrub_conversation_window as m
    (tmp_path/'state.json').write_text(json.dumps({'writers':[]}))
    (tmp_path/'runtime.env').write_text('private')
    calls=[]
    monkeypatch.setattr(m,'assert_current',lambda _:None)
    monkeypatch.setattr(m,'runtime',lambda d,s,module,*args:calls.append((module,args)))
    monkeypatch.setattr(m,'docker',lambda *a,**k:pytest.fail('live archive stopped a writer'))
    m.archive(tmp_path,'apply')
    m.archive(tmp_path,'verify')
    assert calls==[('objects',('--apply',)),('objects',('--verify',))]
    assert not (tmp_path/'runtime.env').exists()


def test_cleanup_preserves_recovery_for_stopped_writer(tmp_path, monkeypatch):
    from personal_data_warehouse import scrub_conversation_window as m
    monkeypatch.setattr(m,'MAINTENANCE_ROOT',tmp_path)
    directory=tmp_path/'stale'
    directory.mkdir()
    (directory/'state.json').write_text(json.dumps({'writers':[{'Id':'saved'}]}))
    (directory/'runtime.env').write_text('private')
    def docker(args, **kw):
        if args[0]=='inspect': return json.dumps([{'State':{'Running':False}}])
        return 'saved' if '-a' in args or '-aq' in args else ''
    monkeypatch.setattr(m,'docker',docker)
    with pytest.raises(RuntimeError,match='stopped'): m.cleanup(directory)
    assert (directory/'runtime.env').exists()


def test_cleanup_removes_crash_left_controller_temp_files(tmp_path, monkeypatch):
    from personal_data_warehouse import scrub_conversation_window as m
    monkeypatch.setattr(m,'MAINTENANCE_ROOT',tmp_path)
    monkeypatch.setattr(m,'docker',lambda *a,**k: '')
    directory=tmp_path/'crashed'
    directory.mkdir()
    (directory/'before-stop.json.k2j3h4').write_text('{}')
    (directory/'token-cards-plan.json.a8b7.pending').write_text('{}')
    m.cleanup(directory)
    assert not directory.exists()


def test_cleanup_refuses_parent_references(tmp_path, monkeypatch):
    from personal_data_warehouse import scrub_conversation_window as m
    monkeypatch.setattr(m,'MAINTENANCE_ROOT',tmp_path)
    monkeypatch.setattr(m,'docker',lambda *a,**k: '')
    (tmp_path/'a').mkdir()
    (tmp_path/'b').mkdir()
    for path in (tmp_path/'..', tmp_path/'a'/'..'/'b'):
        with pytest.raises(ValueError,match='parent'): m.cleanup(path)
    assert (tmp_path/'b').exists()
