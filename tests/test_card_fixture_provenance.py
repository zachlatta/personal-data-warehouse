"""Keep committed checksum-valid fixture literals demonstrably synthetic."""
import json
from pathlib import Path

from tests.card_redaction_properties import checksum_valid


# Published sandbox Visa example; never substitute a live payment credential.
TEST_PAN = '4111111111111111'
REFERENCE = 'https://developer.paypal.com/braintree/docs/reference/general/testing/python/'
CONSTRUCTED_CASES = {
    'ambiguous_grouped_pan_or_code', 'ambiguous_numeric_prefix_readings',
    'bare_code', 'spaced_bare_code', 'review4_suffix_1_2', 'review4_suffix_2_2',
    'review4_safe_9', 'review5_future_date_tokens', 'review6_independent_order_ids',
}


def constructed_input(recipe, name):
    groups = [TEST_PAN[i:i+4] for i in range(0, 16, 4)]
    phone = f'{200+12:03}-{500+55:03}-{100+98:04}'
    if recipe == 'canonical_pan_plus_luhn_code':
        code = next(str(n) for n in range(100, 1000) if checksum_valid(TEST_PAN+str(n)))
        return '-'.join(groups)+'-'+code+'|'
    if recipe == 'canonical_pan_with_overlapping_prefix':
        prefix = next(str(n) for n in range(1000, 10000) if checksum_valid(str(n)+TEST_PAN[:12]))
        return prefix+' '+' '.join(groups)
    if recipe == 'canonical_pan_with_descending_test_code':
        pan = ' '.join(groups) if name == 'spaced_bare_code' else TEST_PAN
        code = ''.join(str(n) for n in range(9, 6, -1))
        assert checksum_valid(TEST_PAN+code)
        return pan+' '+code
    if recipe == 'canonical_pan_with_example_phone':
        separator = ' ' if name == 'review4_suffix_1_2' else '-'
        return separator.join(groups)+' '+phone
    if recipe == 'canonical_invalid_id_plus_luhn_quantity':
        invalid = TEST_PAN[:-1]+str((int(TEST_PAN[-1])+1)%10)
        quantity = next(str(n) for n in range(10, 100) if checksum_valid(invalid+str(n)))
        return 'Order IDs: '+invalid+' '+quantity
    if recipe == 'consecutive_future_dates':
        return ' '.join(f'{2100+i}-11-15' for i in range(3))
    if recipe == 'constructed_numeric_prose':
        return ' '.join(['2026-11-15T09:12:13Z', '+1-'+phone, phone, '2026/11/15',
                         '123:45', '123.45', '123_abc', '123abc', '9'*80])
    raise AssertionError('Unknown synthetic fixture recipe')


def test_constructed_card_fixtures_are_reproducible():
    fixture = json.loads((Path(__file__).parent/'fixtures/card_redaction_cases.json').read_text())
    documented = {case['name']:case for case in fixture['cases'] if 'synthetic_provenance' in case}
    assert set(documented) == CONSTRUCTED_CASES
    for name, case in documented.items():
        provenance = case['synthetic_provenance']
        assert provenance['description'], name
        if provenance['recipe'].startswith('canonical_'):
            assert provenance['base_test_pan'] == TEST_PAN, name
            assert provenance['reference'] == REFERENCE, name
        if case['input'] != constructed_input(provenance['recipe'], name):
            raise AssertionError('Fixture does not reproduce its construction: '+name)
