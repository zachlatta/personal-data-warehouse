"""Credential masking at the conversation storage boundary (shared corpus with Go)."""
import json
import re

import pytest

from personal_data_warehouse import card_redaction as cr
from personal_data_warehouse.credential_redaction import credential_count, credential_edits, credential_kinds, redact_credentials
from tests.credential_redaction_properties import FIXTURE, MARKER, corpus, corpus_digest, expand


def test_generated_corpus_matches_the_go_corpus():
    assert corpus_digest(corpus()) == FIXTURE['property']['corpus_sha256']


@pytest.mark.parametrize('case', FIXTURE['cases'], ids=lambda c: c['name'])
def test_each_shape_masks_only_the_secret(case):
    value = expand(case['input'])
    expected = expand(case.get('expected', case['input']))
    assert redact_credentials(value) == expected
    assert credential_count(value) == len(MARKER.findall(expected)) - len(MARKER.findall(value))
    assert credential_count(expected) == 0
    assert redact_credentials(expected) == expected


def test_generated_wrappers_mask_identically():
    for name, value, expected in corpus():
        assert redact_credentials(value) == expected, name
        assert redact_credentials(expected) == expected, name


def test_kinds_name_each_masked_shape():
    kinds = {}
    for case in FIXTURE['cases']:
        for kind, n in credential_kinds(expand(case['input'])).items():
            kinds[kind] = kinds.get(kind, 0) + n
    assert set(kinds) == {'url_password', 'slack_token', 'github_token', 'openai_key', 'anthropic_key',
                          'aws_secret_key', 'private_key', 'pdw_token', 'bearer_token', 'password', 'secret'}


def test_card_and_credential_boundaries_compose():
    card = 'card: 4111111111111111 CVV: 987'
    slack = expand('<<slack>>')
    value = json.dumps({'text': card + ' ' + slack})
    cleaned = cr.redact(value)
    assert '[card ****1111]' in cleaned and '[redacted]' in cleaned
    assert slack not in cleaned and expand('<<mask:slack_token:slack>>') in cleaned
    assert cr.sensitive_count(cleaned) == 0
    assert cr.sensitive_count(value) == 2


def test_a_digit_run_inside_a_credential_is_not_a_card():
    value = 'password=Pw4111111111111111x'
    edits = credential_edits(value)
    assert len(edits) == 1
    assert '[card' not in cr.redact(value, card_context=True)


def test_redact_row_masks_every_column():
    slack = expand('<<slack>>')
    row = {'text': 'use ' + slack, 'tool_input_json': json.dumps({'command': 'echo ' + slack}),
           'raw_json': {'message': {'content': [{'text': slack}]}}, 'seq': 3}
    cleaned = cr.redact_row(row)
    assert slack not in json.dumps(cleaned)
    assert cleaned['seq'] == 3
    assert all(re.search(r'\[REDACTED:slack_token:[0-9a-f]{6}\]', json.dumps(cleaned[k])) for k in ('text', 'tool_input_json', 'raw_json'))
