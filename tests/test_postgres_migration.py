from __future__ import annotations

from personal_data_warehouse.postgres_migration import canonical_value, collapse_clickhouse_rows


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


class NoFetchClickHouse:
    def _query(self, sql: str):
        raise AssertionError(sql)
