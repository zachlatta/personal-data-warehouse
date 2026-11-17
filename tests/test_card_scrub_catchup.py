"""Live maintenance counts selected readings, not ordinary ingestion churn."""
import json

import pytest

from personal_data_warehouse.schema import AGENT_SESSION_EVENT_COLUMNS
from tests.test_conversation_card_storage import event, warehouse


def test_guard_counts_only_clients():
    from personal_data_warehouse.scrub_conversation_cards import connection_workers
    class Probe:
        def _query(self, sql):
            assert "backend_type = 'client backend'" in sql
            return [(60, 5, 39)]
    assert connection_workers(Probe(), 2) == 2


def test_many_changed_clean_rows_pass_selected_gate(warehouse):
    from personal_data_warehouse.scrub_conversation_cards import discover, scrub
    row = event()
    warehouse._insert_rows('claude_code_events', [row], AGENT_SESSION_EVENT_COLUMNS)
    plan = discover(warehouse)
    warehouse._command("""INSERT INTO @claude_code_events (source,session_id,event_uuid,text)
        SELECT 'claude_code','safe','safe-'||i,'already masked' FROM generate_series(1,2000) i""")
    # An unrelated old transaction can conservatively retain the one planned
    # selected row in the N6 horizon. The 2,000 clean rows consume no budget.
    scrub(warehouse, apply=True, plan=plan, max_delta_rows=1)
    assert not discover(warehouse)['candidates']


def test_catchup_finds_concurrent_commit_and_preserves_old_snapshot(warehouse, tmp_path):
    import psycopg2
    from personal_data_warehouse.scrub_conversation_cards import discover, catch_up, scrub, read_plan
    row = event()
    warehouse._insert_rows('claude_code_events', [row], AGENT_SESSION_EVENT_COLUMNS)
    writer = psycopg2.connect(warehouse._database_url)
    try:
        with writer.cursor() as cursor:
            cursor.execute('SAVEPOINT child')
            cursor.execute(warehouse._expand_relations("INSERT INTO @claude_code_events (source,session_id,event_uuid,text) VALUES ('claude_code','late','late',%s)"), (row['text'],))
        plan = discover(warehouse)
        first = plan['watermarks']['snapshot']
        writer.commit()
        path = tmp_path/'plan.json'
        updated = catch_up(warehouse, plan, path=path, max_passes=3)
        assert len([c for c in updated['candidates'] if c['table']=='claude_code_events']) == 2
        assert updated['watermarks']['snapshot'] != first
        assert read_plan(path) == updated
        assert row['text'] not in path.read_text()
        scrub(warehouse, apply=True, plan=updated)
        assert not discover(warehouse)['candidates']
    finally:
        writer.close()


def test_partition_resume_after_interruption(warehouse, tmp_path):
    from personal_data_warehouse.scrub_conversation_cards import discover_resumable, discover
    row = event()
    warehouse._insert_rows('claude_code_events', [row], AGENT_SESSION_EVENT_COLUMNS)
    path = tmp_path/'checkpoint.json'
    def interrupt(counts):
        assert set(counts) == {'completed_partitions', 'total_partitions', 'selected_rows'}
        raise InterruptedError()
    with pytest.raises(InterruptedError):
        discover_resumable(warehouse, path, progress=interrupt)
    assert path.exists()
    resumed = discover_resumable(warehouse, path, workers=2)
    assert resumed['counts'] == discover(warehouse)['counts']
    assert '4111111111111111' not in json.dumps(resumed)


def test_month_partitions_are_utc_even_across_dst(warehouse):
    from personal_data_warehouse.card_prefilter import ConfirmationPool
    from personal_data_warehouse.scrub_conversation_cards import _scan_table
    warehouse._command("SET timezone='America/New_York'")
    row = event()
    from datetime import datetime, UTC
    row['occurred_at'] = datetime(2026, 3, 31, 23, 30, tzinfo=UTC)
    warehouse._insert_rows('claude_code_events', [row], AGENT_SESSION_EVENT_COLUMNS)
    with ConfirmationPool(1) as pool:
        rows = _scan_table(warehouse, 'claude_code_events', pool, 500,
                           warehouse._connection, month='2026-03')
    assert len(rows) == 1


def test_partial_checkpoint_cannot_apply(warehouse, tmp_path):
    from personal_data_warehouse.scrub_conversation_cards import discover_resumable, read_plan, scrub
    warehouse._insert_rows('claude_code_events', [event()], AGENT_SESSION_EVENT_COLUMNS)
    path=tmp_path/'plan.json'
    def stop(_): raise InterruptedError()
    with pytest.raises(InterruptedError): discover_resumable(warehouse,path,progress=stop)
    with pytest.raises(ValueError,match='incomplete'):
        scrub(warehouse,apply=True,plan=read_plan(path))


def test_pending_sql_cancel_cannot_skip_explicit_rollback():
    import psycopg2
    from personal_data_warehouse.scrub_conversation_cards import _maintenance_transaction
    class Connection:
        rolled_back=False
        def __enter__(self): return self
        def __exit__(self,*args): raise psycopg2.errors.QueryCanceled()
        def rollback(self): self.rolled_back=True
    connection=Connection()
    with pytest.raises(TimeoutError,match='budget exhausted'):
        with _maintenance_transaction(connection):
            raise TimeoutError('signal expired')
    assert connection.rolled_back
