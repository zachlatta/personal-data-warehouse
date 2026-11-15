"""Seed legacy data directly, then prove all read surfaces are cleaned."""
from datetime import UTC, datetime
import json
import os

import pytest

from personal_data_warehouse.postgres import PostgresWarehouse, _AI_EVENT_TABLE_BY_SOURCE
from personal_data_warehouse.agent_sessions_drive_ingest import claude_code_event_row
from personal_data_warehouse.schema import AGENT_SESSION_EVENT_COLUMNS
from personal_data_warehouse.scrub_conversation_cards import scrub
from personal_data_warehouse.scrub_conversation_jsonl import scrub_jsonl
from personal_data_warehouse.scrub_conversation_objects import scrub_objects
from tests.conftest import cleanup_test_warehouse, make_test_schema


@pytest.fixture
def warehouse():
    wh = PostgresWarehouse(os.environ['POSTGRES_DATABASE_URL'], schema=make_test_schema())
    wh.ensure_agent_sessions_tables()
    wh.ensure_timeline_tables()
    wh.ensure_search_index_tables()
    yield wh
    cleanup_test_warehouse(wh)


def event():
    return claude_code_event_row({'type': 'user', 'uuid': 'test-event', 'message': {'role': 'user', 'content': '4111111111111111 CVV: 987'}}, session_id='test-session', account='test', device='test', seq=1, ingested_at=datetime.now(UTC))


def test_every_source_writer_masks_all_content(warehouse):
    for source in _AI_EVENT_TABLE_BY_SOURCE:
        row = event()
        row['source'] = source
        row['tool_input_json'] = row['tool_result_json'] = json.dumps({'number': '4111111111111111', 'cvc': 987})
        warehouse.insert_agent_session_events([row])
    assert scrub(warehouse) == []


def test_legacy_scrub_cleans_timeline_chunks_and_is_idempotent(warehouse):
    row = event()
    warehouse._insert_rows('claude_code_events', [row], AGENT_SESSION_EVENT_COLUMNS)
    warehouse._command("""INSERT INTO @timeline_events (adapter,event_id,source,context,title,snippet,search_text)
        VALUES ('agent_session_turn','test-event','claude_code','claude_code|test-session',%s,%s,%s)""", ('4111111111111111', 'CVV: 987', '4111111111111111 CVV: 987'))
    warehouse._command("""INSERT INTO @search_chunks (chunk_id,adapter,event_id,source,context,text,text_sha256)
        VALUES ('test-chunk','agent_session_turn','test-event','claude_code','claude_code|test-session',%s,'test-sha')""", ('4111111111111111',))
    warehouse._command("INSERT INTO @search_chunk_embeddings (text_sha256, model) VALUES ('test-sha', 'test')")
    assert scrub(warehouse, page_size=1)
    scrub(warehouse, apply=True, page_size=1)
    assert scrub(warehouse, page_size=1) == []
    assert warehouse._query('SELECT count(*) FROM @search_chunks')[0][0] == 0
    assert warehouse._query('SELECT count(*) FROM @search_chunk_embeddings')[0][0] == 0
    assert warehouse._query('SELECT snippet FROM @timeline_events')[0][0] == 'CVV: [redacted]'


def test_local_jsonl_scrub(tmp_path):
    path = tmp_path / 'test-session.jsonl'
    path.write_text(json.dumps({'text': '4111111111111111', 'cvv': 987}) + '\n')
    assert scrub_jsonl(tmp_path, 'test-session')['matches'] == 1
    assert scrub_jsonl(tmp_path, 'test-session', apply=True)['files'] == 1
    assert scrub_jsonl(tmp_path, 'test-session')['matches'] == 0


def test_object_scrub_verifies_replacement_before_deleting(tmp_path, monkeypatch):
    import gzip
    from types import SimpleNamespace
    monkeypatch.chdir(tmp_path)

    class Store:
        def __init__(self):
            self.objects = {'old': gzip.compress(json.dumps({'text': '4111111111111111', 'cvv': 987}).encode())}
            self.deleted = []

        def list_objects(self, **kwargs):
            return [SimpleNamespace(ref={'storage_file_id': k, 'storage_key': 'batch'}, app_properties={}) for k in self.objects]

        def get_object(self, ref):
            return self.objects[ref['storage_file_id']]

        def put_file(self, **kwargs):
            self.objects['new'] = kwargs['path'].read_bytes()
            assert b'4111111111111111' not in gzip.decompress(self.objects['new'])
            return {'storage_file_id': 'new', 'storage_key': 'batch'}

        def delete_object(self, ref):
            assert 'new' in self.objects
            self.deleted.append(ref['storage_file_id'])
            del self.objects[ref['storage_file_id']]

    store = Store()
    assert scrub_objects(store)['matches'] == 1
    assert not store.deleted
    assert scrub_objects(store, apply=True)['objects'] == 1
    assert store.deleted == ['old']
    assert scrub_objects(store)['matches'] == 0
    assert list(tmp_path.iterdir()) == []
