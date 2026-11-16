"""Opaque encoded data must survive even when it contains payment-like segments."""
import base64
import json
from pathlib import Path

from personal_data_warehouse.card_redaction import card_count,redact,redact_row
from tests.test_card_issuer_policy import number
from tests.test_conversation_card_storage import warehouse,event


def encoded_token_cases():
    config=json.loads((Path(__file__).parent/'fixtures/card_encoded_token_property.json').read_text())
    state=config['seed']
    def rand(n):
        nonlocal state
        state=(1664525*state+1013904223)&0xffffffff
        return state%n
    def text():return 'Q'+''.join(config['alphabet'][rand(len(config['alphabet']))] for _ in range(rand(17)))
    for i in range(config['iterations']):
        pan=number('4',config['lengths'][i%len(config['lengths'])])
        left=text();right=text();label=config['labels'][i%len(config['labels'])]
        if i%4==3:label='CVV-987'
        value=left+('/'+label+'/' if i%3==1 else '/')+pan+('/'+label+'/' if i%3==2 else '/')+right
        if i%2:value=value.replace('+','-').replace('/','_')
        if i%4==3:value=value.replace('_','-')
        value+='A'*(-len(value)%4)
        yield value,bool(i%2)


def test_random_base64_and_urlsafe_tokens_are_byte_identical():
    actual=number('4',16)
    for value,urlsafe in encoded_token_cases():
        codec=base64.urlsafe_b64decode if urlsafe else base64.b64decode
        encode=base64.urlsafe_b64encode if urlsafe else base64.b64encode
        assert encode(codec(value)).decode()==value
        assert redact(value)==value and card_count(value)==0
        payload=json.dumps({'text':value})
        assert redact(payload)==payload
        assert redact_row({'text':actual,'tool_result_json':payload})['tool_result_json']==payload


def test_menu_compatibility_requires_whole_token_or_real_field():
    pan=number('4',16)
    for label in ('CVV','CVC2'):
        value='menu'+pan+'/'+label+': 987'
        assert redact(value)=='menu[card ****'+pan[-4:]+']/'+label+': [redacted]'
    value='menu'+pan+'/exp/Zm9v'
    assert redact(value)==value


def test_numeric_prefix_cannot_join_an_opaque_grouped_token():
    from tests.card_redaction_properties import checksum_valid
    pan=number('4',16)
    prefix=next(str(i) for i in range(8100,8200) if checksum_valid(str(i)+pan[:12]))
    opaque='-'.join(pan[i:i+4] for i in range(0,16,4))+'-end'
    value=prefix+' '+opaque
    assert redact(value)==value and card_count(value)==0
    assert redact('pan='+opaque)=='pan=[card ****'+pan[-4:]+']-end'


def test_encoded_tokens_survive_all_writers_discovery_and_archive(warehouse):
    from personal_data_warehouse.postgres import _AI_EVENT_TABLE_BY_SOURCE
    from personal_data_warehouse.schema import AGENT_SESSION_EVENT_COLUMNS
    from personal_data_warehouse.scrub_conversation_cards import discover,scrub
    from personal_data_warehouse.scrub_conversation_objects import discover_objects
    from tests.test_card_scrub_discovery import MemoryArchive
    values=[v for v,_ in encoded_token_cases()]
    for source,table in _AI_EVENT_TABLE_BY_SOURCE.items():
        for i,value in enumerate(values[::100]):
            payload=json.dumps({'text':value})
            row=dict(event(),source=source,event_uuid=f'encoded-{i}',session_id='encoded-session',seq=2*i+1,
                     text=value,tool_input_json=payload,tool_result_json=payload,raw_json=payload)
            warehouse.insert_agent_session_events([row])
            warehouse._insert_rows(table,[dict(row,event_uuid=f'legacy-encoded-{i}',seq=2*i+2)],AGENT_SESSION_EVENT_COLUMNS)
            stored=warehouse._query(f'SELECT text,raw_json,tool_input_json,tool_result_json FROM @{table} WHERE event_uuid=%s',(row['event_uuid'],))[0]
            assert stored==(value,payload,payload,payload)
    plan=discover(warehouse,workers=2)
    assert not plan['candidates']
    assert scrub(warehouse,apply=True,plan=plan)==[]
    assert discover_objects(MemoryArchive(values),workers=2)['counts']==dict(objects=0,lines=0,matches=0)
