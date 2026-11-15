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
from personal_data_warehouse.card_redaction import redact_row
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


def test_delimited_cards_are_masked_by_writer_and_found_by_scan(warehouse):
    from pathlib import Path
    from personal_data_warehouse.card_redaction import card_count
    cases = [c for c in json.loads((Path(__file__).parent / 'fixtures/card_redaction_cases.json').read_text()) if c['name'].startswith('delimiter_')]
    for index, case in enumerate(cases):
        row = dict(event(), event_uuid=f'writer-{index}', seq=index + 1, text=case['input'],
                   tool_input_json=json.dumps({'text': case['input']}), tool_result_json=json.dumps({'text': case['input']}),
                   raw_json=json.dumps({'text': case['input']}))
        warehouse.insert_agent_session_events([row])
        stored = warehouse._query_dicts('SELECT text,tool_input_json,tool_result_json,raw_json FROM @claude_code_events WHERE event_uuid = %s', (row['event_uuid'],))[0]
        assert stored['text'] == case['expected']
        assert all(card_count(value) == 0 and '987' not in value for value in stored.values())
        legacy = dict(row, event_uuid=f'legacy-{index}', seq=index + len(cases) + 1)
        warehouse._insert_rows('claude_code_events', [legacy], AGENT_SESSION_EVENT_COLUMNS)
    before = scrub(warehouse, page_size=1)
    assert next(r['count'] for r in before if r['table']=='claude_code_events' and r['measure']=='rows') == len(cases)
    scrub(warehouse, apply=True, page_size=1)
    assert scrub(warehouse, page_size=1) == []
    assert warehouse._query('SELECT count(*) FROM @claude_code_events')[0][0] == 2 * len(cases)


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
    assert warehouse._query('SELECT count(*) FROM @timeline_events')[0][0] == 2


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


def test_session_rollup_context_is_not_a_session_key(warehouse):
    row = event()
    warehouse._insert_rows('claude_code_events', [row], AGENT_SESSION_EVENT_COLUMNS)
    warehouse._command("""INSERT INTO @timeline_events
      (adapter,event_id,source,context,source_pk,snippet,search_text)
      VALUES ('agent_session','claude_code|test-session','claude_code','/workspace',%s,%s,%s)""",
      (json.dumps(dict(source='claude_code',session_id='test-session')), 'CVV: 987', 'CVV: 987'))
    warehouse._command("""INSERT INTO @search_chunks
      (chunk_id,adapter,event_id,source,context,text,text_sha256)
      VALUES ('rollup-chunk','agent_session','claude_code|test-session','claude_code','/workspace',%s,'rollup-sha')""", ('CVV: 987',))
    warehouse._command("INSERT INTO @search_chunk_embeddings (text_sha256,model) VALUES ('rollup-sha','test')")
    scrub(warehouse,apply=True)
    # Verify the bad outcome without returning any candidate content.
    assert scrub(warehouse) == []
    assert warehouse._query("SELECT count(*) FROM @timeline_events WHERE snippet LIKE 'CVV: %' AND snippet NOT LIKE '%redacted%'")[0][0] == 0
    assert warehouse._query('SELECT count(*) FROM @search_chunk_embeddings')[0][0] == 0

def test_partial_pan_in_turn_snippet_is_regenerated(warehouse):
    row = event()
    warehouse._insert_rows('claude_code_events', [row], AGENT_SESSION_EVENT_COLUMNS)
    warehouse._command("""INSERT INTO @timeline_events
      (adapter,event_id,source,context,snippet,search_text)
      VALUES ('agent_session_turn','test-event','claude_code','claude_code|test-session',%s,%s)""",
      ('card ' + row['text'][:12], row['text']))
    scrub(warehouse,apply=True)
    assert scrub(warehouse) == []
    assert warehouse._query("SELECT count(*) FROM @timeline_events WHERE snippet LIKE 'card 4%'")[0][0] == 0


def test_month_source_report_retains_zero_after_groups():
    from personal_data_warehouse.scrub_conversation_cards import month_source_report, report_html
    baseline = [dict(table='claude_code_events', month='2026-01', source='claude_code', measure='rows', count=2),
                dict(table='claude_code_events', month='2026-01', source='claude_code', measure='matches', count=5),
                dict(table='timeline_events', month='2026-01', source='claude_code', measure='matches', count=3)]
    report = month_source_report(baseline, [])
    assert report == [dict(month='2026-01', source='claude_code', rows_before=2, matches_before=5, rows_after=0, matches_after=0)]
    assert '<td>2026-01</td><td>claude_code</td><td>2</td><td>5</td><td>0</td><td>0</td>' in report_html(report)
    assert month_source_report(baseline)[0]['matches_after'] is None
    assert 'pending' in report_html(month_source_report(baseline))


def test_real_adapters_rebuild_rollups_and_truncated_copies(warehouse):
    from personal_data_warehouse.timeline import TimelineSyncEngine, adapter_by_name
    row = event()
    row['occurred_at'] = datetime.now(UTC)
    row['cwd'] = '/workspace'
    text = 'x' * 292 + '4111111111111111'
    text += 'y' * (492 - len(text)) + '4111111111111111 CVV2: 987'
    row['text'] = text
    row['raw_json'] = json.dumps({'text': text})
    safe = dict(row, event_uuid='safe-event', seq=2, text='safe following turn', raw_json='{}')
    warehouse._insert_rows('claude_code_events', [row, safe], AGENT_SESSION_EVENT_COLUMNS)
    engine = TimelineSyncEngine(source_url=os.environ['POSTGRES_DATABASE_URL'], source_schema=warehouse._schema,
                                dest_schema=warehouse._schema, adapters=[adapter_by_name(n) for n in ('agent_session','agent_session_turn')])
    try:
        engine.run()
        assert warehouse._query('SELECT count(*) FROM @timeline_events')[0][0] == 3
        assert warehouse._query("SELECT count(*) FROM @timeline_events WHERE right(title,8) = '41111111'")[0][0] == 2
        scrub(warehouse, apply=True)
        assert warehouse._query('SELECT count(*) FROM @timeline_events')[0][0] == 3
        engine.run_incremental(adapter_names=['agent_session', 'agent_session_turn'])
        rebuilt = warehouse._query_dicts('SELECT title,snippet,search_text FROM @timeline_events')
        assert len(rebuilt) == 3
        for copy in rebuilt:
            assert all('41111111' not in value and '987' not in value for value in copy.values())
        assert scrub(warehouse) == []
    finally:
        engine.close()


def test_derived_only_match_regenerates_session(warehouse):
    from personal_data_warehouse.timeline import TimelineSyncEngine, adapter_by_name
    row = event()
    row['occurred_at'] = datetime.now(UTC)
    row['ingested_at'] = datetime(2020, 1, 1, tzinfo=UTC)
    safe = dict(row, session_id='unrelated-session', event_uuid='safe-event', text='safe content', raw_json='{}', ingested_at=datetime.now(UTC))
    warehouse._insert_rows('claude_code_events', [row, safe], AGENT_SESSION_EVENT_COLUMNS)
    engine = TimelineSyncEngine(source_url=os.environ['POSTGRES_DATABASE_URL'], source_schema=warehouse._schema,
                                dest_schema=warehouse._schema, adapters=[adapter_by_name(n) for n in ('agent_session','agent_session_turn')])
    try:
        engine.run()
        before = warehouse._query('SELECT count(*) FROM @timeline_events')[0][0]
        assert before == 4
        # A previously cleaned source with stale derived copies; do not move its
        # ingestion watermark. No real data or production connection is used.
        cleaned = redact_row(row)
        warehouse._command("UPDATE @claude_code_events SET text=%s, raw_json=%s WHERE session_id='test-session'", (cleaned['text'], cleaned['raw_json']))
        scrub(warehouse, apply=True)
        assert warehouse._query('SELECT count(*) FROM @timeline_events')[0][0] == before
        assert scrub(warehouse) == []
        engine.run_incremental(adapter_names=['agent_session', 'agent_session_turn'])
        assert warehouse._query('SELECT count(*) FROM @timeline_events')[0][0] == before
    finally:
        engine.close()


def test_regeneration_failure_rolls_back_all_repairs(warehouse, monkeypatch):
    from personal_data_warehouse.timeline import TimelineSyncEngine
    row = event()
    warehouse._insert_rows('claude_code_events', [row], AGENT_SESSION_EVENT_COLUMNS)
    warehouse._command("""INSERT INTO @timeline_events (adapter,event_id,source,context,search_text)
      VALUES ('agent_session','claude_code|test-session','claude_code','/workspace',%s)""", (row['text'],))
    def fail(*args, **kwargs):
        raise RuntimeError('injected regeneration failure')
    monkeypatch.setattr(TimelineSyncEngine, '_upsert', fail)
    with pytest.raises(RuntimeError, match='injected regeneration failure'):
        scrub(warehouse, apply=True)
    assert warehouse._query('SELECT count(*) FROM @timeline_events')[0][0] == 1
    assert warehouse._query('SELECT count(*) FROM @claude_code_events WHERE text = %s', (row['text'],))[0][0] == 1


def test_missing_canonical_session_refuses_to_delete_derived_row(warehouse):
    row = event()
    warehouse._command("""INSERT INTO @timeline_events (adapter,event_id,source,search_text)
      VALUES ('agent_session','claude_code|missing-session','claude_code',%s)""", (row['text'],))
    with pytest.raises(RuntimeError, match='canonical source missing'):
        scrub(warehouse, apply=True)
    assert warehouse._query('SELECT count(*) FROM @timeline_events')[0][0] == 1


def test_missing_canonical_turn_refuses_to_delete_derived_row(warehouse):
    row = event()
    warehouse.insert_agent_session_events([dict(row, text='safe content', raw_json='{}')])
    warehouse._command("""INSERT INTO @timeline_events (adapter,event_id,source,context,search_text)
      VALUES ('agent_session_turn','claude_code|test-session|999','claude_code','claude_code|test-session',%s)""", (row['text'],))
    with pytest.raises(RuntimeError, match='canonical event missing'):
        scrub(warehouse, apply=True)
    assert warehouse._query('SELECT count(*) FROM @timeline_events')[0][0] == 1


def test_chunk_only_match_rebuilds_without_losing_timeline_rows(warehouse):
    from personal_data_warehouse.search_index import SearchChunkBuilder
    from personal_data_warehouse.timeline import TimelineSyncEngine, adapter_by_name
    row = event()
    row['occurred_at'] = datetime.now(UTC)
    warehouse.insert_agent_session_events([row])
    engine = TimelineSyncEngine(source_url=os.environ['POSTGRES_DATABASE_URL'], source_schema=warehouse._schema,
                                dest_schema=warehouse._schema, adapters=[adapter_by_name(n) for n in ('agent_session','agent_session_turn')])
    try:
        engine.run()
        builder = SearchChunkBuilder(warehouse)
        builder.run()
        before = warehouse._query('SELECT max(seq),count(*) FROM @timeline_events')[0]
        warehouse._command("UPDATE @search_chunks SET text=%s,text_sha256='stale-sha'", (row['text'],))
        warehouse._command("INSERT INTO @search_chunk_embeddings (text_sha256,model) VALUES ('stale-sha','test')")
        scrub(warehouse, apply=True)
        after = warehouse._query('SELECT max(seq),count(*) FROM @timeline_events')[0]
        assert after[1] == before[1] == 2
        assert after[0] > before[0]
        assert warehouse._query('SELECT count(*) FROM @search_chunk_embeddings')[0][0] == 0
        builder.run()
        assert warehouse._query('SELECT count(*) FROM @search_chunks')[0][0] == 2
        assert scrub(warehouse) == []
    finally:
        engine.close()
