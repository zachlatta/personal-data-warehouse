"""A derived copy cut out of masked conversation text is masked after the cut.

Timeline titles and snippets are prefixes of a turn's text and search chunks are
windows of it. The full text is masked at ingest, but a cut can turn a value
that is not a secret (a placeholder, a call) into a secret-shaped fragment, and
it can cut through a marker. The timeline sync (ingest and the scrub's rebuild
alike) and the chunk builder mask what they cut, so the report, the scrub's
safety check and the stored rows agree.
"""
from contextlib import nullcontext
import json
import re

import pytest

from tests.test_conversation_card_storage import event, warehouse
from tests.test_credential_scrub_scope import TOKEN
from personal_data_warehouse.card_scrub_selector import ScrubSelector
from personal_data_warehouse.credential_redaction import credential_count, marker
from personal_data_warehouse.schema import AGENT_SESSION_EVENT_COLUMNS
from personal_data_warehouse.timeline import TIMELINE_SNIPPET_CHARS, TIMELINE_TITLE_CHARS

# A labelled value that is a placeholder in full ('placeholder' follows it) but
# a secret-shaped 30-character fragment once a 300-character title cuts it.
LABEL = ' api_key="'
VALUE = 'k9' * 20 + 'placeholder'
LEAD = 'q' * (TIMELINE_TITLE_CHARS - 30 - len(LABEL))
CUT_TEXT = LEAD + LABEL + VALUE + '" and more words after it'
CUT = LEAD + LABEL + VALUE[:30]
MARKER = marker('secret', 'k9' * 20)
FULL_MARKER = re.compile(r'\[REDACTED:[a-z_]+:[0-9a-f]{6}\]')


def _no_partial_marker(text):
    return 'REDACTED' not in FULL_MARKER.sub('', text) and not re.match(r'[0-9a-f]{0,6}\]', text)


def _sync(warehouse):
    from personal_data_warehouse.search_index import SearchChunkBuilder
    from personal_data_warehouse.timeline import TimelineSyncEngine, adapter_by_name
    engine = TimelineSyncEngine(source_url=warehouse._database_url, source_schema=warehouse._schema,
                                dest_schema=warehouse._schema,
                                adapters=[adapter_by_name(n) for n in ('agent_session', 'agent_session_turn')])
    try:
        engine.run()
    finally:
        engine.close()
    SearchChunkBuilder(warehouse).run()


def _turn(text, **extra):
    return dict(event(), text=text, raw_json=json.dumps({'text': text}), tool_input_json='{}', tool_result_json='', **extra)


def test_the_fixture_is_a_placeholder_in_full_and_a_secret_once_cut():
    assert credential_count(CUT_TEXT) == 0
    assert credential_count(CUT_TEXT[:TIMELINE_TITLE_CHARS]) == 1
    assert CUT_TEXT[:TIMELINE_TITLE_CHARS] == CUT


def test_mask_cut_masks_the_fragment_and_drops_a_cut_marker():
    from personal_data_warehouse.derived_redaction import mask_cut
    assert mask_cut(CUT, cut_end=True) == LEAD + LABEL + marker('secret', VALUE[:30])
    assert mask_cut(CUT) == mask_cut(CUT, cut_end=True)  # masking does not depend on knowing it was cut
    assert mask_cut(CUT_TEXT) == CUT_TEXT  # nothing selected: byte-identical
    text = 'see ' + MARKER + ' done'
    for cut in range(len('see ') + 1, len('see ') + len(MARKER)):
        assert mask_cut(text[:cut], cut_end=True) == 'see '
        assert mask_cut(text[cut:], cut_start=True) == text[len('see ') + len(MARKER):]
    # An intact marker at either edge stays, and an uncut value is never trimmed.
    assert mask_cut('see ' + MARKER, cut_end=True) == 'see ' + MARKER
    assert mask_cut(MARKER + ' done', cut_start=True) == MARKER + ' done'
    assert mask_cut('ends with [RED') == 'ends with [RED'


def test_sync_masks_a_title_cut_into_a_secret_shaped_fragment(warehouse):
    """Ingest-time sync: the cut title is masked, the uncut snippet and search text are not."""
    warehouse._insert_rows('claude_code_events', [_turn(CUT_TEXT)], AGENT_SESSION_EVENT_COLUMNS)
    _sync(warehouse)
    rows = warehouse._query_dicts("SELECT adapter, title, snippet, search_text, metadata FROM @timeline_events ORDER BY adapter")
    assert [r['adapter'] for r in rows] == ['agent_session', 'agent_session_turn']
    for row in rows:
        assert row['title'] == LEAD + LABEL + marker('secret', VALUE[:30])
        assert row['snippet'] == CUT_TEXT[:TIMELINE_SNIPPET_CHARS]
        assert CUT_TEXT in row['search_text']
        assert not any(credential_count(str(row[c])) for c in ('title', 'snippet', 'search_text', 'metadata'))


def test_sync_drops_a_marker_the_title_cut_through(warehouse):
    head = 'm' * (TIMELINE_TITLE_CHARS - 10) + ' '
    text = head + MARKER + ' and the rest of the turn'
    warehouse._insert_rows('claude_code_events', [_turn(text)], AGENT_SESSION_EVENT_COLUMNS)
    _sync(warehouse)
    [(title, snippet)] = warehouse._query("SELECT title, snippet FROM @timeline_events WHERE adapter='agent_session_turn'")
    assert title == head
    assert snippet == text


def test_search_chunks_mask_their_cuts(warehouse):
    from personal_data_warehouse.search_index import CHUNK_MAX_CHARS, CHUNK_OVERLAP_CHARS
    # Chunk 0 ends at CHUNK_MAX_CHARS inside the labelled value; chunk 1 starts
    # CHUNK_OVERLAP_CHARS earlier, inside a marker.
    second = CHUNK_MAX_CHARS - CHUNK_OVERLAP_CHARS
    before_marker = 'p' * (second - 5 - 1) + ' '
    middle = ' ' + 'r' * (CHUNK_MAX_CHARS - 30 - len(LABEL) - len(before_marker) - len(MARKER) - 1)
    text = before_marker + MARKER + middle + LABEL + VALUE + '" tail ' + 's' * 300
    assert len(before_marker) + 5 == second and text.index(LABEL) + len(LABEL) + 30 == CHUNK_MAX_CHARS
    assert credential_count(text) == 0
    warehouse._insert_rows('claude_code_events', [_turn(text)], AGENT_SESSION_EVENT_COLUMNS)
    _sync(warehouse)
    chunks = [r[0] for r in warehouse._query(
        "SELECT text FROM @search_chunks WHERE adapter='agent_session_turn' ORDER BY chunk_index")]
    assert len(chunks) >= 2
    assert chunks[0].endswith(LABEL + marker('secret', VALUE[:30]))
    assert chunks[1].startswith(middle.strip())
    for chunk in chunks:
        assert credential_count(chunk) == 0 and _no_partial_marker(chunk)


@pytest.mark.parametrize('mode', ['scrub', 'planned_batches'])
def test_the_scrub_rebuilds_a_cut_title_and_the_count_reaches_zero(warehouse, tmp_path, mode):
    """Production: one turn title cut at 300 characters refused its whole group."""
    from personal_data_warehouse import scrub_conversation_cards as m
    from personal_data_warehouse.credential_scrub_report import scan
    selector = ScrubSelector('credentials')
    dirty = _turn('use ' + TOKEN, event_uuid='dirty', seq=2)
    warehouse._insert_rows('claude_code_events', [_turn(CUT_TEXT), dirty], AGENT_SESSION_EVENT_COLUMNS)
    _sync(warehouse)
    # The copy an earlier sync wrote: the cut fragment, unmasked.
    warehouse._command("UPDATE @timeline_events SET title=%s WHERE adapter='agent_session_turn' AND event_id LIKE %s",
                       (CUT, '%|1'))
    before, _ = scan(warehouse._database_url, warehouse._schema,
                     tables=('claude_code_events', 'timeline_events', 'search_chunks'), workers=1)
    assert before['matches'] > 0
    plan = m.discover(warehouse, selector=selector)
    if mode == 'scrub':
        m.scrub(warehouse, apply=True, plan=plan, selector=selector)
    else:
        path = tmp_path / 'plan.json'
        m.write_plan(path, plan)
        state = m.apply_batches(warehouse, path, window=lambda _: nullcontext(), selector=selector,
                                planned_only=True, gap_seconds=0)
        assert len(state['batches']['completed']) == 1
    from personal_data_warehouse.search_index import SearchChunkBuilder
    SearchChunkBuilder(warehouse).run()
    after, _ = scan(warehouse._database_url, warehouse._schema,
                    tables=('claude_code_events', 'timeline_events', 'search_chunks'), workers=1)
    assert after['matches'] == 0
    assert not m.discover(warehouse, selector=selector)['candidates']
    titles = {r[0] for r in warehouse._query("SELECT title FROM @timeline_events WHERE adapter='agent_session_turn'")}
    assert LEAD + LABEL + marker('secret', VALUE[:30]) in titles
