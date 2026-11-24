"""A derived copy the canonical rebuild no longer produces is masked in place.

Timeline sync upserts and never deletes, so a turn the adapter stopped emitting
(an older turn builder, or a source event a re-upload removed) keeps its
timeline row and search chunks. The scrub cannot regenerate such a copy, so it
masks the stored text where it is, keeps the row, and makes search rebuild it.
"""
from contextlib import nullcontext
import hashlib
import json

import pytest

from tests.test_conversation_card_storage import event, warehouse
from tests.test_credential_scrub_scope import TOKEN, MASKED
from personal_data_warehouse.schema import AGENT_SESSION_EVENT_COLUMNS
from personal_data_warehouse.card_scrub_selector import ScrubSelector

STALE = 'claude_code|test-session|999'
CLEAN_STALE = 'claude_code|test-session|998'
DIRTY_TEXT = 'old turn ' + TOKEN
CLEAN_TEXT = 'old turn without anything secret'


def _sha(text):
    return hashlib.sha256(text.encode()).hexdigest()


def _sync(warehouse):
    from personal_data_warehouse.search_index import SearchChunkBuilder
    from personal_data_warehouse.timeline import TimelineSyncEngine, adapter_by_name
    engine = TimelineSyncEngine(source_url=warehouse._database_url, source_schema=warehouse._schema,
                                dest_schema=warehouse._schema,
                                adapters=[adapter_by_name(n) for n in ('agent_session', 'agent_session_turn')])
    try:
        engine.run()
    finally:
        engine.close()
    SearchChunkBuilder(warehouse).run()


def _stale_copy(warehouse, event_id, text):
    """A turn row and chunk shaped like the canonical ones, with no source event."""
    warehouse._command(
        """INSERT INTO @timeline_events (adapter,event_id,source,kind,priority,occurred_at,end_ts,actor,title,snippet,
             context,source_table,source_pk,metadata,search_text,ingest_ts,seq,first_seen_at,updated_at)
           SELECT adapter,%s,source,kind,priority,occurred_at,end_ts,actor,%s,%s,context,source_table,%s,metadata,%s,
             ingest_ts,nextval(%s::regclass),first_seen_at,updated_at
           FROM @timeline_events WHERE adapter='agent_session_turn' LIMIT 1""",
        (event_id, text, text, json.dumps(dict(source='claude_code', session_id='test-session', seq=int(event_id.rsplit('|', 1)[1]))),
         text, warehouse.sql_relation('timeline_events_seq')))
    anchor = f'agent_session_turn|{event_id}'
    warehouse._command(
        """INSERT INTO @search_chunks (chunk_id,anchor,adapter,event_id,source,context,occurred_at,chunk_index,text,
             text_sha256,char_count,built_at)
           VALUES (%s,%s,'agent_session_turn',%s,'claude_code','claude_code|test-session',now(),0,%s,%s,%s,now())""",
        (anchor + '#0', anchor, event_id, text, _sha(text), len(text)))
    warehouse._command("INSERT INTO @search_chunk_embeddings (text_sha256,model) VALUES (%s,'test')", (_sha(text),))


def _copies(warehouse, event_id):
    return {table: warehouse._query_dicts(f'SELECT * FROM @{table} WHERE event_id=%s ORDER BY 1,2', (event_id,))
            for table in ('timeline_events', 'search_chunks')}


@pytest.mark.parametrize('source', ['dirty', 'clean'])
@pytest.mark.parametrize('mode', ['scrub', 'planned_batches'])
def test_stale_derived_copies_are_masked_in_place_and_the_count_reaches_zero(warehouse, tmp_path, source, mode):
    from personal_data_warehouse import scrub_conversation_cards as m
    from personal_data_warehouse.credential_scrub_report import scan
    from personal_data_warehouse.search_index import SearchChunkBuilder
    text = 'see ' + (TOKEN if source == 'dirty' else 'nothing secret')
    row = dict(event(), text=text, raw_json=json.dumps({'text': text}), tool_input_json='{}', tool_result_json='')
    warehouse._insert_rows('claude_code_events', [row], AGENT_SESSION_EVENT_COLUMNS)
    _sync(warehouse)
    _stale_copy(warehouse, STALE, DIRTY_TEXT)
    _stale_copy(warehouse, CLEAN_STALE, CLEAN_TEXT)
    old_seq = _copies(warehouse, STALE)['timeline_events'][0]['seq']
    clean_before = _copies(warehouse, CLEAN_STALE)
    selector = ScrubSelector('credentials')
    plan = m.discover(warehouse, selector=selector)
    assert {('timeline_events', 'agent_session_turn'), ('search_chunks', f'agent_session_turn|{STALE}#0')} <= {
        (c['table'], c['key'][0]) for c in plan['candidates']}
    if mode == 'scrub':
        m.scrub(warehouse, apply=True, plan=plan, selector=selector)
    else:
        path = tmp_path / 'plan.json'
        m.write_plan(path, plan)
        state = m.apply_batches(warehouse, path, window=lambda _: nullcontext(), selector=selector,
                                planned_only=True, gap_seconds=0)
        assert len(state['batches']['completed']) == 1
    masked = DIRTY_TEXT.replace(TOKEN, MASKED)
    stale = _copies(warehouse, STALE)
    [timeline] = stale['timeline_events']
    assert (timeline['title'], timeline['snippet'], timeline['search_text']) == (masked, masked, masked)
    assert timeline['seq'] > old_seq  # the chunk builder re-chunks it from the masked text
    [chunk] = stale['search_chunks']
    assert (chunk['text'], chunk['text_sha256'], chunk['char_count']) == (masked, _sha(masked), len(masked))
    assert not warehouse._query('SELECT 1 FROM @search_chunk_embeddings WHERE text_sha256=%s', (_sha(DIRTY_TEXT),))
    assert _copies(warehouse, CLEAN_STALE) == clean_before
    assert warehouse._query('SELECT 1 FROM @search_chunk_embeddings WHERE text_sha256=%s', (_sha(CLEAN_TEXT),))
    assert not m.discover(warehouse, selector=selector)['candidates']
    after, _ = scan(warehouse._database_url, warehouse._schema,
                    tables=('claude_code_events', 'timeline_events', 'search_chunks'), workers=1)
    assert after['matches'] == 0
    SearchChunkBuilder(warehouse).run()
    stale = _copies(warehouse, STALE)
    assert [c['text'] for c in stale['search_chunks']] == [masked]
    everything = [warehouse._query_dicts(f'SELECT * FROM @{t}') for t in ('timeline_events', 'search_chunks', 'claude_code_events')]
    assert TOKEN not in json.dumps(everything, default=str)


def test_a_key_the_rebuild_writes_for_another_session_still_refuses(warehouse):
    """A copy whose key the rebuild wrote for another session is not stale.

    Masking it in place would keep a row the rebuild has just overwritten, so the
    group refuses and the transaction leaves every row as it was.
    """
    from personal_data_warehouse import scrub_conversation_cards as m
    selector = ScrubSelector('credentials')
    text = 'see ' + TOKEN
    other = dict(event(), session_id='other-session', event_uuid='other-event',
                 text=text, raw_json=json.dumps({'text': text}), tool_input_json='{}', tool_result_json='')
    warehouse._insert_rows('claude_code_events', [other], AGENT_SESSION_EVENT_COLUMNS)
    [(seq,)] = warehouse._query("SELECT seq FROM @ai_conversation_events WHERE session_id='other-session'")
    # The turn key the rebuild writes for other-session, held by a copy that names test-session.
    warehouse._command(
        """INSERT INTO @timeline_events (adapter,event_id,source,context,source_pk,search_text)
           VALUES ('agent_session_turn',%s,'claude_code','claude_code|test-session',%s,%s)""",
        (f'claude_code|other-session|{seq}', json.dumps(dict(source='claude_code', session_id='test-session', seq=1)), text))
    warehouse._insert_rows('claude_code_events', [dict(event(), text='safe content', raw_json='{}')], AGENT_SESSION_EVENT_COLUMNS)
    before = [warehouse._query_dicts(f'SELECT * FROM @{t} ORDER BY 1,2') for t in ('timeline_events', 'claude_code_events')]
    with pytest.raises(RuntimeError, match='canonical event missing for derived copy'):
        m.scrub(warehouse, apply=True, selector=selector)
    assert [warehouse._query_dicts(f'SELECT * FROM @{t} ORDER BY 1,2') for t in ('timeline_events', 'claude_code_events')] == before

