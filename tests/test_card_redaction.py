import json

import pytest

from personal_data_warehouse.card_redaction import redact, redact_row


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


def test_nested_escaped_json_in_tool_output():
    value = json.dumps({'result': json.dumps({'number': '4111111111111111', 'CVV': '987'}).replace('1', r'\u0031')})
    inner = json.loads(json.loads(redact(value))['result'])
    assert inner['number'] == '[card ****1111]'
    assert inner['CVV'] == '[redacted]'
