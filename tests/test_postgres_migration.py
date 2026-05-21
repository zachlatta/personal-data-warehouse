from __future__ import annotations

from personal_data_warehouse import postgres_migration
from personal_data_warehouse.postgres_migration import canonical_value, collapse_clickhouse_rows, migrate


def test_collapse_clickhouse_rows_keeps_highest_version_per_key() -> None:
    rows = [
        ("account-a", "msg-1", "old", 1),
        ("account-a", "msg-1", "new", 2),
        ("account-a", "msg-2", "only", 1),
    ]

    current, last_key = collapse_clickhouse_rows(
        NoFetchClickHouse(),
        table="gmail_messages",
        rows=rows,
        key_indexes=[0, 1],
        version_index=3,
        chunk_was_full=False,
    )

    assert current == [("account-a", "msg-1", "new", 2), ("account-a", "msg-2", "only", 1)]
    assert last_key == ("account-a", "msg-2")


def test_collapse_clickhouse_rows_holds_back_last_key_when_chunk_may_split_versions() -> None:
    rows = [
        ("account-a", "msg-1", "old", 1),
        ("account-a", "msg-1", "new", 2),
        ("account-a", "msg-2", "maybe-incomplete", 1),
    ]

    current, last_key = collapse_clickhouse_rows(
        NoFetchClickHouse(),
        table="gmail_messages",
        rows=rows,
        key_indexes=[0, 1],
        version_index=3,
        chunk_was_full=True,
    )

    assert current == [("account-a", "msg-1", "new", 2)]
    assert last_key == ("account-a", "msg-1")


def test_canonical_value_decodes_clickhouse_byte_strings_as_valid_utf8_prefix() -> None:
    assert canonical_value(b"hello \xe2\x82") == "hello "


def test_migrate_rebuilds_slack_conversation_stats_after_slack_messages(monkeypatch) -> None:
    events: list[str] = []
    postgres = FakePostgres(events)

    monkeypatch.setattr(postgres_migration, "ensure_all_tables", lambda postgres: None)
    monkeypatch.setattr(postgres_migration, "truncate_tables", lambda postgres, tables: None)
    monkeypatch.setattr(postgres_migration, "prepare_postgres_copy_stage", lambda postgres, columns: None)
    monkeypatch.setattr(postgres_migration, "iter_clickhouse_current_rows", lambda *args, **kwargs: iter(()))

    migrate(
        clickhouse=object(),
        postgres=postgres,
        tables=("slack_messages",),
        chunk_size=1000,
        truncate=True,
    )

    assert events == ["rebuild"]


class NoFetchClickHouse:
    def _query(self, sql: str):
        raise AssertionError(sql)


class FakePostgres:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    def rebuild_slack_conversation_stats(self) -> None:
        self._events.append("rebuild")
