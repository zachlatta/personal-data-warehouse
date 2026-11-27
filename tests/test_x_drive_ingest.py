from __future__ import annotations

from datetime import UTC, datetime
import gzip
import json
import os

from dagster import DagsterInstance, SkipReason, build_sensor_context
from dotenv import load_dotenv
import pytest

from personal_data_warehouse.defs import x_drive_ingest as defs_module
from personal_data_warehouse.objectstore import ObjectListing
from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.x_drive_ingest import (
    XDriveIngestRunner,
    iter_batch_payloads,
    record_to_post_row,
    record_to_relation_row,
)
from tests.conftest import cleanup_test_warehouse, make_test_schema


@pytest.fixture()
def warehouse():
    load_dotenv()
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if not url:
        pytest.skip("POSTGRES_DATABASE_URL is not set")
    wh = PostgresWarehouse(url, schema=make_test_schema())
    try:
        yield wh
    finally:
        cleanup_test_warehouse(wh)


class FakeLogger:
    def info(self, *_args, **_kwargs) -> None:
        pass


class FakeWarehouse:
    def __init__(self) -> None:
        self.ensure_called = False
        self.posts: list[dict[str, object]] = []
        self.relations: list[dict[str, object]] = []

    def ensure_x_tables(self) -> None:
        self.ensure_called = True

    def insert_x_posts(self, rows) -> None:
        self.posts.extend(rows)

    def insert_x_user_posts(self, rows) -> None:
        self.relations.extend(rows)


class FakeObjectStore:
    backend = "google_drive"

    def __init__(self, *, listings=None, payloads=None) -> None:
        self.listings = list(listings or [])
        self.payloads = dict(payloads or {})
        self.moved: list[tuple[str, str]] = []
        self.fetched: list[str] = []

    def list_objects(self, *, kind, stage=None, properties=None):
        return list(self.listings) if kind == "x_export_batch" else []

    def find_object(self, *, kind, stage=None, properties=None):
        if kind == "x_export_batch" and self.listings:
            return self.listings[0]
        return None

    def get_object(self, ref):
        self.fetched.append(str(ref["storage_file_id"]))
        return self.payloads[str(ref["storage_file_id"])]

    def move_object(self, ref, *, new_object_key, app_properties=None):
        self.moved.append((str(ref["storage_file_id"]), new_object_key))
        return {**ref, "storage_key": new_object_key}


def post_envelope(
    post_id: str = "1900000000000000001",
    *,
    text: str = "Shipping the new warehouse source today",
    like_count: int = 3,
    exported_at: str = "2026-11-27T12:00:00+00:00",
):
    return {
        "schema_version": 1,
        "source": "x",
        "account": "ZachLatta",
        "exported_at": exported_at,
        "record_type": "post",
        "record": {
            "post_id": post_id,
            "author_user_id": "100",
            "author_screen_name": "zachlatta",
            "author_name": "Zach Latta",
            "created_at": "2026-11-27T11:00:00+00:00",
            "text": text,
            "lang": "en",
            "conversation_id": post_id,
            "in_reply_to_post_id": "",
            "in_reply_to_user_id": "",
            "in_reply_to_screen_name": "",
            "quoted_post_id": "",
            "retweeted_post_id": "",
            "is_retweet": False,
            "is_quote": False,
            "mentions_account": False,
            "reply_count": 1,
            "retweet_count": 2,
            # The Go uploader's JSON numbers may arrive as floats.
            "like_count": float(like_count),
            "quote_count": 0,
            "bookmark_count": 0,
            "view_count": 120,
            "url": f"https://x.com/zachlatta/status/{post_id}",
            "raw": {"rest_id": post_id},
        },
    }


def relation_envelope(
    post_id: str = "1900000000000000001",
    relation: str = "authored",
    *,
    exported_at: str = "2026-11-27T12:00:00+00:00",
):
    return {
        "schema_version": 1,
        "source": "x",
        "account": "zachlatta",
        "exported_at": exported_at,
        "record_type": "relation",
        "record": {
            "post_id": post_id,
            "relation": relation,
            "discovered_at": "2026-11-27T11:30:00+00:00",
            "sort_index": 1999999999999999999,
        },
    }


def listing(file_id: str, exported_at: str) -> ObjectListing:
    return ObjectListing(
        ref={"storage_backend": "google_drive", "storage_key": "", "storage_file_id": file_id, "storage_url": ""},
        app_properties={"content_sha256": file_id, "exported_at": exported_at},
        filename=f"{file_id}.jsonl.gz",
    )


def gz(*records) -> bytes:
    return gzip.compress("\n".join(json.dumps(record) for record in records).encode())


def test_record_to_post_row_maps_every_column_and_lowercases_the_account() -> None:
    row = record_to_post_row(post_envelope())
    assert row["account"] == "zachlatta"
    assert row["post_id"] == "1900000000000000001"
    assert row["created_at"] == datetime(2026, 11, 27, 11, tzinfo=UTC)
    assert row["like_count"] == 3
    assert row["is_retweet"] == 0
    assert row["mentions_account"] == 0
    assert row["raw_json"] == {"rest_id": "1900000000000000001"}
    assert row["sync_version"] == int(datetime(2026, 11, 27, 12, tzinfo=UTC).timestamp() * 1_000_000)


def test_record_to_relation_row_keeps_the_sort_index_exactly() -> None:
    row = record_to_relation_row(relation_envelope(relation="liked"))
    assert (row["account"], row["post_id"], row["relation"]) == ("zachlatta", "1900000000000000001", "liked")
    assert row["sort_index"] == 1999999999999999999
    assert row["discovered_at"] == datetime(2026, 11, 27, 11, 30, tzinfo=UTC)


def test_an_unknown_relation_is_refused() -> None:
    with pytest.raises(ValueError, match="relation"):
        record_to_relation_row(relation_envelope(relation="followed"))


def test_drive_ingest_writes_and_promotes_each_batch_oldest_first_up_to_the_cap() -> None:
    store = FakeObjectStore(
        listings=[
            listing("newer", "2026-11-27T13:00:00+00:00"),
            listing("older", "2026-11-27T12:00:00+00:00"),
            listing("newest", "2026-11-27T14:00:00+00:00"),
        ],
        payloads={
            "older": gz(post_envelope("1"), post_envelope("1", like_count=9), relation_envelope("1")),
            "newer": gz(post_envelope("2"), relation_envelope("2", "liked")),
            "newest": gz(post_envelope("3")),
        },
    )
    warehouse = FakeWarehouse()
    summary = XDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: iter_batch_payloads(object_store=store),
        object_store=store,
        logger=FakeLogger(),
        max_batches=2,
    ).sync()

    assert warehouse.ensure_called
    assert (summary.batches_seen, summary.posts_written, summary.relations_written) == (2, 2, 2)
    assert [p["post_id"] for p in warehouse.posts] == ["1", "2"]
    # The later read of the same post in one batch wins.
    assert warehouse.posts[0]["like_count"] == 9
    assert [(r["post_id"], r["relation"]) for r in warehouse.relations] == [("1", "authored"), ("2", "liked")]
    assert store.moved == [
        ("older", "x/library/batches/2026/11/older.jsonl.gz"),
        ("newer", "x/library/batches/2026/11/newer.jsonl.gz"),
    ]
    # The cap never downloads a batch it will not write.
    assert store.fetched == ["older", "newer"]


def test_posts_and_relations_round_trip_through_the_warehouse_and_upsert(warehouse) -> None:
    XDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: [{"records": [post_envelope(), relation_envelope()]}],
        logger=FakeLogger(),
    ).sync()
    XDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: [
            {
                "records": [
                    post_envelope(like_count=40, exported_at="2026-11-27T12:30:00+00:00"),
                    relation_envelope(exported_at="2026-11-27T12:30:00+00:00"),
                    relation_envelope(relation="bookmarked", exported_at="2026-11-27T12:30:00+00:00"),
                ]
            }
        ],
        logger=FakeLogger(),
    ).sync()
    posts = warehouse._query_dicts("SELECT account, like_count, created_at, is_quote, raw_json FROM @x_posts")
    assert posts == [
        {
            "account": "zachlatta",
            "like_count": 40,
            "created_at": datetime(2026, 11, 27, 11, tzinfo=UTC),
            "is_quote": 0,
            "raw_json": {"rest_id": "1900000000000000001"},
        }
    ]
    relations = warehouse._query_dicts("SELECT relation, sort_index FROM @x_user_posts ORDER BY relation")
    assert relations == [
        {"relation": "authored", "sort_index": 1999999999999999999},
        {"relation": "bookmarked", "sort_index": 1999999999999999999},
    ]


class FakeSettings:
    x = type("X", (), {"google_drive_folder_id": "folder", "google_drive_account": "owner@example.test"})()


def test_sensor_skips_an_empty_inbox(monkeypatch) -> None:
    assert defs_module.x_drive_inbox_sensor.minimum_interval_seconds == 60
    monkeypatch.setattr(defs_module, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(defs_module, "_x_object_store", lambda _settings: object())
    monkeypatch.setattr(defs_module, "has_batch_payloads", lambda **_kwargs: False)
    with DagsterInstance.ephemeral() as instance:
        result = defs_module.x_drive_inbox_sensor(build_sensor_context(instance=instance))
    assert isinstance(result, SkipReason)
