import json

import pytest

from personal_data_warehouse.card_redaction import redact, redact_row
from pathlib import Path


@pytest.mark.parametrize('case', json.loads((Path(__file__).parent / 'fixtures/card_redaction_cases.json').read_text())['cases'], ids=lambda c: c['name'])
def test_shared_adversarial_cases(case):
    actual = redact(case['input'])
    if case.get('json_result'):
        assert json.loads(actual) == json.loads(case['expected'])
    else:
        assert actual == case['expected']


def test_generated_numeric_prose_and_inserted_card_properties():
    import hashlib
    from tests.card_redaction_properties import property_cases
    digest = hashlib.sha256()
    count = 0
    for name,value,expected in property_cases():
        assert redact(value) == expected, name
        digest.update((name+'\0'+value+'\0'+expected+'\n').encode())
        count += 1
    config = json.loads((Path(__file__).parent/'fixtures/card_redaction_cases.json').read_text())['property']
    assert count >= 17680
    assert digest.hexdigest() == config['corpus_sha256']


@pytest.mark.parametrize('label', ['CVV2', 'CVC2', 'CID', 'CSC', 'security code', 'card code'])
def test_security_code_json_keys(label):
    result = json.loads(redact(json.dumps({'number': '4111111111111111', label: 987})))
    assert result[label] == '[redacted]'


@pytest.mark.parametrize('separator', ['', ' ', '-'])
def test_card_and_adjacent_security_code(separator):
    number = separator.join(['4111', '1111', '1111', '1111'])
    assert redact(f'card {number}, CVV: 987') == 'card [card ****1111], CVV: [redacted]'


def test_json_escaped_and_numeric_values_stay_valid_json():
    value = json.dumps({'number': '4111111111111111', 'security_code': 987})
    escaped = value.replace('1', r'\u0031')
    result = json.loads(redact(escaped))
    assert result == {'number': '[card ****1111]', 'security_code': '[redacted]'}
    assert json.loads(redact('{"card":4111111111111111,"cvc":987}'))['cvc'] == '[redacted]'


def test_invalid_id_and_long_digit_run_unchanged():
    for value in ['4111111111111112', '123456789012345678901234']:
        assert redact(value) == value


def test_row_context_masks_code_in_another_field_and_is_idempotent():
    row = {'text': '4111111111111111', 'tool_input_json': '{"cvv":987}', 'seq': 5}
    result = redact_row(row)
    assert json.loads(result['tool_input_json'])['cvv'] == '[redacted]'
    assert result['seq'] == 5
    assert redact_row(result) == result


@pytest.mark.parametrize('length', range(13, 20))
def test_all_supported_lengths(length):
    from personal_data_warehouse.card_redaction import is_card
    prefix = '4' + '1' * (length - 2)
    number = next(prefix + str(n) for n in range(10) if is_card(prefix + str(n)))
    assert redact(number).startswith('[card ****')
    grouped = ' '.join(number[i:i+4] for i in range(0,len(number),4))
    masked = redact(grouped + ' 987')
    assert masked.startswith('[card ****') and masked.endswith('] [redacted]') and '987' not in masked


def test_nested_escaped_json_in_tool_output():
    value = json.dumps({'result': json.dumps({'number': '4111111111111111', 'CVV': '987'}).replace('1', r'\u0031')})
    inner = json.loads(json.loads(redact(value))['result'])
    assert inner['number'] == '[card ****1111]'
    assert inner['CVV'] == '[redacted]'
