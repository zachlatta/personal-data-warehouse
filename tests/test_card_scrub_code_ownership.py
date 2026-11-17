"""A neighbouring JSON code belongs to the nearest reading, not any edit."""
import gzip
import json

import pytest

from personal_data_warehouse.card_scrub_selector import ScrubSelector
from personal_data_warehouse.schema import AGENT_SESSION_EVENT_COLUMNS
from tests.test_card_scrub_discovery import MemoryArchive, synthetic_pan
from tests.test_conversation_card_storage import event, warehouse

SEED = '4111111111111111'
OTHER = synthetic_pan(15)


def mixed_value(before, nearest, depth):
    readings = [SEED, OTHER] if nearest == before else [OTHER, SEED]
    text = ' | '.join(readings)
    for _ in range(depth):
        text = json.dumps(text)
    return json.dumps({'cvv': '123', 'text': text} if before else
                      {'text': text, 'cvv': '123'})


@pytest.mark.parametrize('before', [False, True])
@pytest.mark.parametrize('nearest', [False, True])
@pytest.mark.parametrize('depth', [0, 1, 3])
@pytest.mark.parametrize('scope', ['seed', 'labelled', 'all'])
def test_json_code_requires_its_nearest_reading_selected(before, nearest, depth, scope):
    selector = ScrubSelector(scope, seeds=(SEED,))
    value = mixed_value(before, nearest, depth)
    clean = selector.redact(value)
    if scope == 'seed':
        expected = value.replace(SEED, '[card ****1111]')
        if nearest: expected = expected.replace('"123"', '"[redacted]"')
        assert clean == expected
    else:
        assert '"123"' not in clean
    assert selector.count(clean) == 0
    assert selector.redact(clean) == clean


@pytest.mark.parametrize('before', [False, True])
def test_unselected_edge_code_survives_all_maintenance_surfaces(warehouse, tmp_path, monkeypatch, before):
    from personal_data_warehouse.scrub_conversation_cards import discover, scrub
    from personal_data_warehouse.scrub_conversation_jsonl import scrub_jsonl
    from personal_data_warehouse.scrub_conversation_objects import discover_objects, scrub_objects
    monkeypatch.chdir(tmp_path)
    selector = ScrubSelector('seed', seeds=(SEED,))
    value = mixed_value(before, False, 2)
    expected = value.replace(SEED, '[card ****1111]')
    row = dict(event(), text=value, raw_json=value,
               tool_input_json=value, tool_result_json=value)
    warehouse._insert_rows('claude_code_events', [row], AGENT_SESSION_EVENT_COLUMNS)
    plan = discover(warehouse, selector=selector)
    scrub(warehouse, apply=True, plan=plan, selector=selector)
    stored = warehouse._query_dicts('SELECT text,raw_json,tool_input_json,tool_result_json FROM @claude_code_events')[0]
    assert all(v == expected for v in stored.values())
    path = tmp_path / 'session.jsonl'
    path.write_text(value + '\n')
    scrub_jsonl(tmp_path, 'session', apply=True, selector=selector)
    assert path.read_text() == expected + '\n'
    store = MemoryArchive([value])
    archive = discover_objects(store, selector=selector)
    scrub_objects(store, apply=True, plan=archive, selector=selector)
    output = gzip.decompress(store.objects['replacement']).decode()
    assert json.dumps({'text': expected}) in output
