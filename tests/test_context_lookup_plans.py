"""Context recovery must probe existing keys rather than scan transcript text."""
import json
from pathlib import Path
import re

from tests.test_postgres_warehouse import warehouse  # noqa: F401


def test_context_recovery_has_index_conditions(warehouse):
    warehouse.ensure_tables()
    warehouse.ensure_timeline_tables()
    source = Path('app/internal/query/context.go').read_text()
    # Even on an empty fixture, require a usable key condition. A forced index
    # scan with only a suffix Filter would still walk the whole production index.
    warehouse._command('SET enable_seqscan = off')
    cases = [('contextRefSuggestionSQL', 'timeline_events', 'event_id', ('not-an-adapter', 'missing-id')),
             ('contextGmailRefSuggestionSQL', 'gmail_messages', 'message_id', ('missing-id',))]
    for constant, logical_id, key, args in cases:
        sql = re.search(r'const ' + constant + r' = `([^`]+)`', source)[1]
        physical = 'timeline.events' if logical_id == 'timeline_events' else 'base_gmail.messages'
        sql = sql.replace(physical, '@' + logical_id)
        sql = re.sub(r'\$(\d+)', lambda m: '%(p' + m[1] + ')s', sql)
        params = {'p' + str(index + 1): value for index, value in enumerate(args)}
        plan = warehouse._query('EXPLAIN (FORMAT JSON) ' + sql, params)[0][0]
        def nodes(value):
            if isinstance(value, dict):
                yield value
                for child in value.values(): yield from nodes(child)
            elif isinstance(value, list):
                for child in value: yield from nodes(child)
        scans = [node for node in nodes(plan) if node.get('Relation Name') == warehouse.sql_relation(logical_id).split('.')[-1].strip('"')]
        assert scans, json.dumps(plan)
        assert all(key in node.get('Index Cond', '') for node in scans), json.dumps(plan)
