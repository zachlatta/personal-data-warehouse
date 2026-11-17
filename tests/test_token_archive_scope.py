import gzip
from types import SimpleNamespace

import pytest

from personal_data_warehouse import scrub_conversation_objects as m
from personal_data_warehouse.card_scrub_selector import ScrubSelector
from tests.test_token_scrub_scope import TOKEN


def test_scoped_plan_and_verify_never_download_outside_export_interval():
    class Store:
        def __init__(self): self.reads=[]
        def list_objects(self, **kw):
            return [SimpleNamespace(filename=t+'-batch.jsonl.gz',app_properties={},
                    ref={'storage_file_id':t,'storage_key':t})
                    for t in ('20000101T000000Z','20000102T000000Z','20000108T000000Z')]
        def get_object(self, ref):
            self.reads.append(ref['storage_file_id'])
            return gzip.compress(TOKEN.encode())
    store=Store()
    scope=dict(exported_from='2000-01-02T00:00:00Z',exported_before='2000-01-08T00:00:00Z',
               max_objects=2,max_bytes=10000)
    selector=ScrubSelector('tokens')
    plan=m.discover_objects(store,selector=selector,scan_scope=scope)
    assert plan['scan_scope']==scope
    m._validate_object_plan(plan)
    assert plan['counts']['matches']==1
    assert store.reads==['20000102T000000Z']
    # Verification reuses the bound; it must include new replacement IDs.
    m.discover_objects(store,selector=selector,scan_scope=plan['scan_scope'])
    assert store.reads==['20000102T000000Z']*2
    with pytest.raises(RuntimeError,match='object limit'):
        m.discover_objects(store,scan_scope=dict(scope,max_objects=0))
    with pytest.raises(RuntimeError,match='byte limit'):
        m.discover_objects(store,scan_scope=dict(scope,max_bytes=1))


def test_scope_refuses_invalid_or_missing_dates():
    with pytest.raises(ValueError): m.validate_scan_scope({'exported_from':'invalid'})
    with pytest.raises(ValueError):
        m.validate_scan_scope(dict(exported_from='2000-01-08T00:00:00Z',
            exported_before='2000-01-02T00:00:00Z',max_objects=1,max_bytes=1))


def test_scoped_apply_verify_checks_replacement_id(tmp_path, monkeypatch):
    from tests.test_card_scrub_discovery import MemoryArchive
    monkeypatch.chdir(tmp_path)
    store=MemoryArchive([TOKEN])
    original=store.list_objects
    def listings(**kw):
        rows=original(**kw)
        for row in rows: row.filename='20000102T000000Z-synthetic.jsonl.gz'
        return rows
    monkeypatch.setattr(store,'list_objects',listings)
    selector=ScrubSelector('tokens')
    scope=dict(exported_from='2000-01-02T00:00:00Z',exported_before='2000-01-03T00:00:00Z',
               max_objects=2,max_bytes=10000)
    plan=m.discover_objects(store,selector=selector,scan_scope=scope)
    m.scrub_objects(store,apply=True,plan=plan,selector=selector)
    assert set(store.objects)=={'replacement'}
    assert m.discover_objects(store,selector=selector,scan_scope=plan['scan_scope'])['counts']['matches']==0


def test_parallel_downloads_use_thread_owned_clients():
    import threading
    class Reader:
        def __init__(self): self.owner=threading.get_ident()
        def get_object(self, ref):
            assert self.owner==threading.get_ident()
            return gzip.compress(b'safe')
    class Store:
        def list_objects(self, **kw):
            return [SimpleNamespace(filename='20000102T000000Z-synthetic.jsonl.gz',
                    app_properties={},ref={'storage_file_id':str(i)}) for i in range(20)]
        def get_object(self, ref): pytest.fail('shared client used')
    plan=m.discover_objects(Store(),store_factory=Reader,download_workers=4)
    assert plan['counts']['matches']==0
    with pytest.raises(ValueError,match='separate client'):
        m.discover_objects(Store(),download_workers=4)


def test_scoped_scan_reports_counts_only_progress(capsys):
    class Store:
        def list_objects(self, **kw):
            return [SimpleNamespace(filename='20000102T000000Z-synthetic.jsonl.gz',
                    app_properties={},ref={'storage_file_id':str(i)}) for i in range(5)]
        def get_object(self, ref): return gzip.compress(TOKEN.encode())
    scope=dict(exported_from='2000-01-02T00:00:00Z',exported_before='2000-01-03T00:00:00Z',
               max_objects=5,max_bytes=10**6)
    m.discover_objects(Store(),selector=ScrubSelector('tokens'),scan_scope=scope,progress_every=2)
    lines=capsys.readouterr().err.splitlines()
    assert [line.split()[:2] for line in lines]==[['archive','scan']]*3
    assert lines[-1].startswith('archive scan 5/5 objects')
    assert TOKEN not in ''.join(lines)
