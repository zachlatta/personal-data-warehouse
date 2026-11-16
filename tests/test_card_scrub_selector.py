"""Narrow maintenance policy is independent of the ingest boundary."""
import gzip
import json
import pytest

from personal_data_warehouse.schema import AGENT_SESSION_EVENT_COLUMNS

from tests.test_conversation_card_storage import event, warehouse
from tests.test_card_scrub_discovery import MemoryArchive, synthetic_pan

SEED = synthetic_pan(15)
GROUPED = SEED[:4]+"-"+SEED[4:10]+"-"+SEED[10:]


def test_selector_preserves_unselected_bytes_and_json_lexemes():
    from personal_data_warehouse.card_scrub_selector import ScrubSelector
    selector = ScrubSelector('labelled', seeds=(SEED,))
    untouched = '4111111111111111 exp: 12/29'
    assert selector.redact(untouched) == untouched
    value = untouched + ' | pan=4111111111111111 | ' + GROUPED + ' 987'
    clean = selector.redact(value)
    assert clean.startswith(untouched)
    assert 'pan=[card ' in clean and '987' not in clean
    assert selector.count(value) == 2 and selector.count(clean) == 0
    value = '{"id":4111111111111111,"card_number":4111111111111111,"cvv":987,"n":1e400}'
    clean = selector.redact(value)
    assert clean.startswith('{"id":4111111111111111,') and clean.endswith(',"n":1e400}')
    assert selector.count(clean) == 0 and '987' not in clean
    escaped = ''.join(r'\u%04x' % ord(c) for c in SEED)
    for _ in range(4):
        escaped = json.dumps({'payload': escaped})
    assert selector.count(escaped) == 1
    assert selector.count(selector.redact(escaped)) == 0


def test_labelled_warehouse_plan_apply_and_derived(warehouse):
    from personal_data_warehouse.card_scrub_selector import ScrubSelector, resolve_selector
    from personal_data_warehouse.scrub_conversation_cards import discover, scrub
    seed = dict(event(), text=SEED, raw_json='{}')
    warehouse._insert_rows('claude_code_events', [seed], AGENT_SESSION_EVENT_COLUMNS)
    selector = resolve_selector(warehouse, 'labelled', ['claude_code_events:'+json.dumps({k:seed[k] for k in ('source','session_id','event_uuid')})])
    text = 'id 4111111111111111 | exp 4111111111111111 12/29 | card: 4111111111111111 | ' + GROUPED + ' CVV: 987'
    row = dict(event(), event_uuid='other', seq=2, text=text, raw_json=json.dumps({'text':text}),
               tool_input_json=json.dumps({'payload':text}),tool_result_json=json.dumps({'payload':text}))
    warehouse._insert_rows('claude_code_events', [row], AGENT_SESSION_EVENT_COLUMNS)
    other=dict(row,source='codex',session_id='other-source',event_uuid='cross-source',text=json.dumps({'text':GROUPED}))
    warehouse._insert_rows('codex_events',[other],AGENT_SESSION_EVENT_COLUMNS)
    pk=json.dumps({k:row[k] for k in ('source','session_id','event_uuid')})
    warehouse._command("INSERT INTO @timeline_events (adapter,event_id,source,source_pk,snippet) VALUES ('agent_session_turn','other','claude_code',%s,%s)",
                       (pk,'partial '+SEED[:8]))
    warehouse._command("INSERT INTO @search_chunks (chunk_id,adapter,event_id,source,context,text,text_sha256) VALUES ('old-copy','agent_session_turn','other','claude_code','claude_code|test-session',%s,'synthetic-digest')",(GROUPED,))
    plan = discover(warehouse, selector=selector)
    assert plan['selector']['scope'] == 'labelled'
    assert SEED not in json.dumps(plan)
    assert plan['left_alone'] > 0
    scrub(warehouse, apply=True, plan=plan, selector=selector)
    assert not discover(warehouse, selector=selector)['candidates']
    stored = warehouse._query("SELECT text FROM @claude_code_events WHERE event_uuid='other'")[0][0]
    assert stored.startswith('id 4111111111111111 | exp 4111111111111111 12/29')
    assert '[card ' in stored and '987' not in stored
    assert warehouse._query('SELECT text FROM @codex_events')[0][0] == selector.redact(other['text'])
    assert all(value==selector.redact(row[column]) for column,value in warehouse._query_dicts("SELECT tool_input_json,tool_result_json,raw_json FROM @claude_code_events WHERE event_uuid='other'")[0].items())
    assert warehouse._query('SELECT count(*) FROM @timeline_events')[0][0] > 0
    assert warehouse._query("SELECT count(*) FROM @timeline_events WHERE snippet LIKE 'partial %'")[0][0]==0
    assert warehouse._query('SELECT count(*) FROM @search_chunks')[0][0]==0


def test_labelled_archive_and_jsonl(tmp_path, monkeypatch):
    from personal_data_warehouse.card_scrub_selector import ScrubSelector
    from personal_data_warehouse.scrub_conversation_objects import discover_objects, scrub_objects
    from personal_data_warehouse.scrub_conversation_jsonl import scrub_jsonl
    monkeypatch.chdir(tmp_path)
    selector = ScrubSelector('labelled', seeds=(SEED,))
    values = ['4111111111111111', '4111111111111111 exp: 12/29', '4111111111111111 CVV2: 987', GROUPED]
    store = MemoryArchive(values)
    plan = discover_objects(store, selector=selector)
    assert plan['counts']['matches'] == 2
    scrub_objects(store, apply=True, plan=plan, selector=selector)
    assert discover_objects(store, selector=selector)['counts']['matches'] == 0
    output = gzip.decompress(store.objects['replacement']).decode()
    assert json.dumps({'text': values[0]}) in output
    path = tmp_path/'session.jsonl'
    path.write_text('\n'.join(json.dumps({'text':v}) for v in values)+'\n')
    assert scrub_jsonl(tmp_path,'session',selector=selector)['matches'] == 2
    scrub_jsonl(tmp_path,'session',apply=True,selector=selector)
    assert scrub_jsonl(tmp_path,'session',selector=selector)['matches'] == 0
    assert values[1] in path.read_text()


@pytest.mark.parametrize('label',['CVV','CVC','CVV2','CVC2','CID','CSC','security code'])
def test_code_labels_select_only_neighbouring_card(label):
    from personal_data_warehouse.card_scrub_selector import ScrubSelector
    selector=ScrubSelector('labelled')
    value='4111111111111111 '+label+': 987 | unrelated 4111111111111111'
    clean=selector.redact(value)
    assert selector.count(value)==1
    assert clean.endswith('| unrelated 4111111111111111')
    assert '987' not in clean


def test_json_labels_are_local_and_parallel_selection_agrees():
    from personal_data_warehouse.card_scrub_selector import ScrubSelector
    from personal_data_warehouse.card_prefilter import ConfirmationPool
    selector=ScrubSelector('labelled')
    value='{"id":"4111111111111111","nested":{"cvv":987},"text":"safe"}'
    assert selector.redact(value)==value
    assert selector.redact('card: 4111111111111111 | 4111111111111111').endswith('| 4111111111111111')
    assert selector.count('4111111111111111 exp: 12/29 CVV: 987')==1
    rows=[{'text':'pan=4111111111111111 | 4111111111111111'}]*128
    with ConfirmationPool(2,selector) as pool:
        assert pool.counts(rows)==[1]*128
        assert pool.left_alone==128
        assert all(row['text'].endswith('| 4111111111111111') for row in pool.redacted(rows))


def test_selector_plan_mismatch_and_sanitized_seed_fail_closed(warehouse):
    from personal_data_warehouse.card_scrub_selector import ScrubSelector, resolve_selector
    from personal_data_warehouse.scrub_conversation_cards import discover, scrub
    selector=ScrubSelector('labelled')
    plan=discover(warehouse,selector=selector)
    with pytest.raises(ValueError,match='selector differs'):
        scrub(warehouse,apply=True,plan=plan)
    row=dict(event(),text='safe',raw_json='{}')
    warehouse._insert_rows('claude_code_events',[row],AGENT_SESSION_EVENT_COLUMNS)
    pk=json.dumps({k:row[k] for k in ('source','session_id','event_uuid')})
    with pytest.raises(ValueError,match='seed unavailable'):
        resolve_selector(warehouse,'labelled',['claude_code_events:'+pk])
