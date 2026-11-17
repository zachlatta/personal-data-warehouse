from pathlib import Path
import re

from tests.test_postgres_warehouse import warehouse  # noqa: F401


def test_mutation_status_query_uses_the_real_keys(warehouse):
    warehouse.ensure_upstream_mutation_tables()
    guide = Path('app/internal/guide/topics/mutations.md').read_text()
    sql = re.search(r'```sql\n(.*?)\n```', guide, re.S)[1]
    # Test warehouses isolate the same catalog objects in per-test schemas.
    sql = sql.replace('ops.upstream_mutation_requests', '@upstream_mutation_requests')
    sql = sql.replace('ops.upstream_mutation_operations', '@upstream_mutations')
    assert warehouse._query_dicts(sql) == []
