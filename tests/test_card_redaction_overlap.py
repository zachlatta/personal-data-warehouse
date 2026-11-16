"""Adjacent security-code chains must not overlap subsequent card edits."""
import json

from personal_data_warehouse.card_redaction import _text_edits, card_count, redact


def test_adjacent_code_chain_followed_by_another_card():
    # Published sandbox Visa recipes, constructed at runtime.
    first = '4' + '1' * 15
    second = '42' * 8
    grouped = ' '.join(second[i:i + 4] for i in range(0, len(second), 4))
    text = first + ' - 012 ' + grouped + ' done'
    edits = _text_edits(text)
    assert all(left[1] <= right[0] for left, right in zip(edits, edits[1:])), 'overlapping edits'
    for value in (text, json.dumps({'text': text})):
        result = redact(value)
        assert card_count(result) == 0
        cleaned_text = result if value == text else json.loads(result)['text']
        assert cleaned_text.endswith(' done')
