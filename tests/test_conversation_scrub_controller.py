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


def test_restore_does_not_start_an_originally_stopped_writer(tmp_path,monkeypatch):
    from personal_data_warehouse import scrub_conversation_window as m
    (tmp_path/'state.json').write_text(json.dumps({'writers':[
        {'Id':'dagster','State':{'Running':False}},{'Id':'app','State':{'Running':True}}]}))
    calls=[]
    monkeypatch.setattr(m,'docker',lambda args,**kwargs: calls.append(args) or '')
    m.restore(tmp_path)
    assert calls[-1] == ['start','app']
    assert ['start','dagster'] not in calls


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
