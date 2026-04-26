from __future__ import annotations

from personal_data_warehouse.clickhouse import ClickHouseWarehouse


def test_gmail_schema_drops_thread_view_and_does_not_recreate_it() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    commands: list[str] = []

    warehouse._command = commands.append

    warehouse.ensure_tables()

    assert "DROP VIEW IF EXISTS gmail_thread_messages" in commands
    assert all("CREATE OR REPLACE VIEW gmail_thread_messages" not in command for command in commands)
