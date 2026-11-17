"""Token-only maintenance preserves payment data across every repair surface."""
import argparse
import gzip
import json

import pytest

from personal_data_warehouse.card_scrub_selector import ScrubSelector, add_selector_arguments, validate_specification
from personal_data_warehouse.schema import AGENT_SESSION_EVENT_COLUMNS
from tests.test_conversation_card_storage import event, warehouse
from tests.test_card_scrub_discovery import MemoryArchive

TOKEN = 'pdw_' + 'a' * 16 + '_' + 'B' * 40
MASKED = 'pdw_' + 'a' * 16 + '_[REDACTED]'
CARD = 'card: 4111111111111111 CVV: 987'


@pytest.mark.parametrize('depth', range(5))
def test_tokens_preserve_every_other_byte(depth):
    selector = ScrubSelector('tokens')
    value = '{ "card":4111111111111111,"cvv":987,"n":1e400,"text":"' + TOKEN + '" }'
    expected = value.replace(TOKEN, MASKED)
    for _ in range(depth):
        value, expected = json.dumps(value), json.dumps(expected)
    assert selector.count(value) == 1
    assert selector.redact(value) == expected
    assert selector.count(expected) == 0
    assert selector.redact(CARD) == CARD


def test_token_scope_cli_and_plan_identity():
    parser = argparse.ArgumentParser()
    add_selector_arguments(parser)
    assert parser.parse_args(['--scope', 'tokens']).scope == 'tokens'
    validate_specification(ScrubSelector('tokens').specification)
    with pytest.raises(ValueError, match='seed'):
        ScrubSelector('tokens', seed_rows=('unused',))


def test_token_prefilter_includes_nested_unicode(warehouse):
    from personal_data_warehouse.card_prefilter import sql_prefilter
    selector = ScrubSelector('tokens')
    encoded = '"' + ''.join(r'\u%04x' % ord(c) for c in TOKEN) + '"'
    values = [TOKEN, CARD, 'ordinary identifier', encoded]
    for _ in range(4):
        values.append(json.dumps({'nested': values[-1]}))
    for value in values:
        selected = warehouse._query('SELECT ' + sql_prefilter('%s', scope='tokens'), (value, value))[0][0]
        assert not selector.count(value) or selected
    assert all(selector.count(value) == 1 for value in values[3:])
    assert not warehouse._query('SELECT ' + sql_prefilter('%s', scope='tokens'), (CARD, CARD))[0][0]


@pytest.mark.parametrize('dirty', ['source', 'timeline', 'chunk'])
def test_tokens_rebuild_all_database_copies(warehouse, dirty, tmp_path):
    from personal_data_warehouse.scrub_conversation_cards import discover, scrub, write_plan, read_plan
    from personal_data_warehouse.search_index import SearchChunkBuilder
    from personal_data_warehouse.timeline import TimelineSyncEngine, adapter_by_name
    selector = ScrubSelector('tokens')
    text = CARD + ' ' + (TOKEN if dirty == 'source' else MASKED)
    row = dict(event(), text=text, raw_json=json.dumps({'text': text}), tool_input_json=text, tool_result_json=text)
    warehouse._insert_rows('claude_code_events', [row], AGENT_SESSION_EVENT_COLUMNS)
    engine = TimelineSyncEngine(source_url=warehouse._database_url, source_schema=warehouse._schema,
                                dest_schema=warehouse._schema, adapters=[adapter_by_name(n) for n in ('agent_session', 'agent_session_turn')])
    try:
        engine.run()
        builder = SearchChunkBuilder(warehouse)
        builder.run()
        if dirty == 'timeline':
            warehouse._command('UPDATE @timeline_events SET search_text=%s,snippet=%s,title=%s', (text + TOKEN,) * 3)
        if dirty == 'chunk':
            warehouse._command("UPDATE @search_chunks SET text=%s,text_sha256='dirty-token-sha'", (text + TOKEN,))
        warehouse._command("INSERT INTO @search_chunk_embeddings (text_sha256,model) SELECT DISTINCT text_sha256,'test' FROM @search_chunks")
        plan = discover(warehouse, selector=selector)
        assert plan['candidates'] and TOKEN not in json.dumps(plan)
        path = tmp_path / 'plan.json'
        write_plan(path, plan)
        assert read_plan(path) == plan
        assert path.stat().st_mode & 0o777 == 0o600
        with pytest.raises(ValueError, match='selector differs'):
            scrub(warehouse, apply=True, plan=plan, selector=ScrubSelector('labelled'))
        if dirty == 'source':
            with pytest.raises(ValueError, match='candidate gate'):
                scrub(warehouse, apply=True, plan=plan, selector=selector, max_source_rows=0)
        scrub(warehouse, apply=True, plan=plan, selector=selector)
        assert not discover(warehouse, selector=selector)['candidates']
        assert warehouse._query('SELECT count(*) FROM @search_chunk_embeddings')[0][0] == 0
        builder.run()
        for table in ('claude_code_events', 'ai_conversation_events', 'timeline_events', 'search_chunks'):
            copies = warehouse._query_dicts(f'SELECT * FROM @{table}')
            assert copies and TOKEN not in json.dumps(copies, default=str)
            assert any(CARD in str(v) for copy in copies for v in copy.values())
        # Verification must also discover newly introduced secrets after apply.
        warehouse._command('UPDATE @claude_code_events SET tool_result_json=%s', (TOKEN,))
        assert discover(warehouse, selector=selector)['candidates']
    finally:
        engine.close()


def test_token_archives_and_host_jsonl(tmp_path, monkeypatch):
    from personal_data_warehouse.scrub_conversation_objects import discover_objects, scrub_objects
    from personal_data_warehouse.scrub_conversation_jsonl import scrub_jsonl
    monkeypatch.chdir(tmp_path)
    selector = ScrubSelector('tokens')
    values = [CARD, CARD + ' ' + TOKEN, json.dumps({'nested': TOKEN})]
    store = MemoryArchive(values)
    plan = discover_objects(store, selector=selector)
    assert plan['counts']['lines'] == 2
    scrub_objects(store, apply=True, plan=plan, selector=selector)
    assert discover_objects(store, selector=selector)['counts']['matches'] == 0
    output = gzip.decompress(store.objects['replacement']).decode()
    assert TOKEN not in output and output.count(CARD) == 2
    path = tmp_path / 'session.jsonl'
    raw = ('\r\n'.join(json.dumps({'text': v}) for v in values) + '\r\n').encode()
    path.write_bytes(raw)
    scrub_jsonl(tmp_path, 'session', apply=True, selector=selector)
    assert path.read_bytes() == raw.replace(TOKEN.encode(), MASKED.encode())
    assert scrub_jsonl(tmp_path, 'session', selector=selector)['matches'] == 0


def test_token_db_verify_cli_fails_until_clean(warehouse, monkeypatch):
    import sys
    import personal_data_warehouse.scrub_conversation_cards as module
    monkeypatch.setattr(module, 'PostgresWarehouse', lambda *a, **k: warehouse)
    monkeypatch.setattr(warehouse, 'close', lambda: warehouse._connection.set_session(readonly=False, autocommit=True))
    monkeypatch.setattr(sys, 'argv', ['scrub', '--scope', 'tokens', '--verify', '--workers', '1'])
    row = dict(event(), text=TOKEN, raw_json='{}')
    warehouse._insert_rows('claude_code_events', [row], AGENT_SESSION_EVENT_COLUMNS)
    with pytest.raises(SystemExit, match='secrets remain'):
        module.main()
    warehouse._command('UPDATE @claude_code_events SET text=%s', (MASKED,))
    module.main()


def test_token_object_verify_cli_fails_until_clean(monkeypatch):
    import sys
    import personal_data_warehouse.config as config
    import personal_data_warehouse.defs.agent_sessions_drive_ingest as ingest
    from personal_data_warehouse.scrub_conversation_objects import main
    store = MemoryArchive([TOKEN])
    monkeypatch.setattr(config, 'load_settings', lambda **k: None)
    monkeypatch.setattr(ingest, '_agent_sessions_object_store', lambda settings: store)
    monkeypatch.setattr(sys, 'argv', ['scrub', '--scope', 'tokens', '--verify', '--workers', '1'])
    with pytest.raises(SystemExit, match='secrets remain'):
        main()
    store.objects.clear()
    main()
