"""Issuer recognition and identifier boundaries use runtime synthetic values."""
import json
from pathlib import Path

from personal_data_warehouse.card_redaction import card_count, redact
from tests.test_conversation_card_storage import warehouse,event


def number(prefix,length):
    from tests.card_redaction_properties import checksum_valid
    stem=(prefix+'1'*length)[:length-1]
    return next(stem+str(i) for i in range(10) if checksum_valid(stem+str(i)))


def test_shared_issuer_and_identifier_cases():
    cases=json.loads((Path(__file__).parent/'fixtures/card_issuer_cases.json').read_text())
    for case in cases:
        pan=number(case['prefix'],case['length'])
        value=case.get('before','')+pan+case.get('after','')
        if case.get('pad_to_quartet'):
            import base64
            value+='A'*(-len(value)%4)
            assert base64.b64encode(base64.b64decode(value,validate=True)).decode()==value
        expected=case.get('before','')+'[card ****'+pan[-4:]+']'+case.get('after','') if case['mask'] else value
        assert redact(value)==expected,case['name']
        assert card_count(value)==int(case['mask']),case['name']


def test_nonbrand_number_is_preserved_even_with_code_label():
    value=number('17',13)+' CVV2: 987'
    assert redact(value)==value
    assert not card_count(value)


def payment_label_cases():
    """Cross product consumed in Python and independently generated in Go."""
    from tests.card_redaction_properties import render_groups
    for prefix,length in (('4',13),('4',16),('4',19),('3095',16),('3095',19)):
        pan=number(prefix,length)
        forms=[pan]
        if length==16:forms += [render_groups(pan,(4,4,4,4),variant) for variant in ('space','dash','mixed_runs')]
        for rendered in forms:
            for delimiter in ('/','|',';','#','=','&'):
                for label in ('CVV','CVC','CVC2','CVV2','CID','CSC','security code','card code','exp','expiry'):
                    value='pan='+rendered+delimiter+label+'=987'
                    expected='pan=[card ****'+pan[-4:]+']'+delimiter+label+'='+('987' if label in ('exp','expiry') else '[redacted]')
                    yield value,expected
                    yield label+'='+rendered+delimiter,label+'=[card ****'+pan[-4:]+']'+delimiter


def test_payment_labels_across_punctuation():
    for value,expected in payment_label_cases():
        assert redact(value)==expected
        assert card_count(value)==1


def test_payment_labels_through_writer_archive_and_legacy_repair(warehouse,tmp_path,monkeypatch):
    from personal_data_warehouse.postgres import _AI_EVENT_TABLE_BY_SOURCE
    from personal_data_warehouse.schema import AGENT_SESSION_EVENT_COLUMNS
    from personal_data_warehouse.scrub_conversation_cards import discover,scrub,write_plan,read_plan
    from personal_data_warehouse.scrub_conversation_objects import discover_objects,scrub_objects
    from tests.test_card_scrub_discovery import MemoryArchive
    sources=list(_AI_EVENT_TABLE_BY_SOURCE.items())
    values=[]
    for i,(value,expected) in enumerate(payment_label_cases()):
        source,table=sources[i%len(sources)]
        row=dict(event(),source=source,event_uuid=f'label-{i}',session_id='label-session',seq=2*i+1,
                 text=value,raw_json=json.dumps({'text':value}),tool_input_json=json.dumps({'text':value}),tool_result_json=json.dumps({'text':value}))
        warehouse.insert_agent_session_events([row])
        stored=warehouse._query(f'SELECT text FROM @{table} WHERE event_uuid=%s',(row['event_uuid'],))[0][0]
        assert stored==expected
        legacy=dict(row,event_uuid=f'legacy-label-{i}',seq=2*i+2)
        warehouse._insert_rows(table,[legacy],AGENT_SESSION_EVENT_COLUMNS)
        values.append(value)
    plan=discover(warehouse,workers=2)
    assert sum(c['matches']>0 for c in plan['candidates'])==len(values)
    path=tmp_path/'labels-plan.json';write_plan(path,plan)
    scrub(warehouse,apply=True,plan=read_plan(path),workers=2)
    assert not discover(warehouse,workers=2)['candidates']
    for table in _AI_EVENT_TABLE_BY_SOURCE.values():
        assert all(not card_count(v) for row in warehouse._query(f'SELECT text,raw_json,tool_input_json,tool_result_json FROM @{table}') for v in row)
    assert all(not card_count(v) for row in warehouse._query('SELECT title,snippet,search_text FROM @timeline_events') for v in row)
    monkeypatch.chdir(tmp_path)
    store=MemoryArchive(values);archive=discover_objects(store,workers=2)
    assert archive['counts']['lines']==len(values)
    scrub_objects(store,apply=True,plan=archive,workers=2)
    assert not discover_objects(store,workers=2)['counts']['matches']


def test_go_policy_matches_the_shared_file():
    import subprocess,sys
    subprocess.run([sys.executable,'scripts/generate_go_card_policy.py','--check'],check=True)


def test_issuer_policy_through_writer_scrub_and_sql_prefilter(warehouse):
    from personal_data_warehouse.postgres import _AI_EVENT_TABLE_BY_SOURCE
    from personal_data_warehouse.card_prefilter import sql_prefilter
    from personal_data_warehouse.scrub_conversation_cards import discover,scrub
    cases=json.loads((Path(__file__).parent/'fixtures/card_issuer_cases.json').read_text())
    for i,case in enumerate(cases):
        pan=number(case['prefix'],case['length'])
        value=case.get('before','')+pan+case.get('after','')
        if case.get('pad_to_quartet'):value+='A'*(-len(value)%4)
        encoded=json.dumps({'text':value})
        if case['mask']:
            assert warehouse._query('SELECT '+sql_prefilter('%s'),(encoded,))[0][0]
        for source,table in _AI_EVENT_TABLE_BY_SOURCE.items():
            row=dict(event(),source=source,event_uuid=f'issuer-{i}',session_id=f'issuer-session-{i}',text=value,
                     raw_json=encoded,tool_input_json=encoded,tool_result_json=encoded)
            warehouse.insert_agent_session_events([row])
            stored=warehouse._query(f'SELECT text,raw_json,tool_input_json,tool_result_json FROM @{table} WHERE event_uuid=%s',(row['event_uuid'],))[0]
            assert not any(card_count(v) for v in stored),case['name']
            if not case['mask']:assert stored==(value,encoded,encoded,encoded),case['name']
    plan=discover(warehouse,workers=2)
    assert not plan['candidates']
    assert scrub(warehouse,apply=True,plan=plan)==[]
