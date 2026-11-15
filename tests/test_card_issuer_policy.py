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
        expected=case.get('before','')+'[card ****'+pan[-4:]+']'+case.get('after','') if case['mask'] else value
        assert redact(value)==expected,case['name']
        assert card_count(value)==int(case['mask']),case['name']


def test_nonbrand_number_is_preserved_even_with_code_label():
    value=number('17',13)+' CVV2: 987'
    assert redact(value)==value
    assert not card_count(value)


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
