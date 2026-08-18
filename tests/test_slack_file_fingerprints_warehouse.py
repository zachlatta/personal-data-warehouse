"""The warehouse side of Slack image fingerprints, against real Postgres.

Covers the candidate selection that makes the 552 GB backfill bounded and
resumable, and the marts view that answers "who sent this?" — including the
uploader join whose absence caused the 2026-08-16 fabricated answer.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import pytest
from dotenv import load_dotenv

from personal_data_warehouse.postgres import POSTGRES_TABLES, PostgresWarehouse
from personal_data_warehouse.photo_fingerprint import HASH_VERSION
from personal_data_warehouse.slack_file_fingerprints import (
    NO_RETRY,
    STATUS_FAILED,
    STATUS_OK,
    STATUS_TOO_LARGE,
)
from personal_data_warehouse.slack_image_lookup import build_lookup_sql, parse_matches
from tests.conftest import cleanup_test_warehouse, make_test_schema

NOW = datetime(2026, 8, 18, 12, 0, tzinfo=UTC)
POSTER_DHASH = "f0e1d2c3b4a59687" * 4
NEAR_DHASH = "f0e1d2c3b4a59687" * 3 + "f0e1d2c3b4a59686"  # 1 bit away
FAR_DHASH = "0" * 64


def _postgres_url() -> str:
    load_dotenv()
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if not url:
        pytest.skip("POSTGRES_DATABASE_URL is not set")
    return url


@pytest.fixture()
def warehouse():
    schema = make_test_schema("slackfp")
    wh = PostgresWarehouse(_postgres_url(), schema=schema)
    try:
        wh.ensure_slack_tables()
        wh.ensure_slack_file_fingerprint_tables()
        yield wh
    finally:
        cleanup_test_warehouse(wh)


def file_row(**overrides) -> dict:
    row = {
        "account": "zrl",
        "team_id": "T_TESTTEAM",
        "file_id": "F_TESTPOSTER",
        "conversation_id": "D_TESTDM",
        "message_ts": "1786473484.611059",
        "user_id": "U_TESTUPLOADER",
        "created_at": NOW - timedelta(days=7),
        "name": "11x17.png",
        "title": "11x17.png",
        "mimetype": "image/png",
        "filetype": "png",
        "url_private": "https://files.slack.com/files-pri/T09-F_TESTPOSTER/11x17.png",
        "size": 20055308,
        "is_deleted": 0,
        "raw_json": "{}",
        "synced_at": NOW,
        "sync_version": 1,
    }
    row.update(overrides)
    return row


def insert_files(warehouse, rows):
    warehouse.insert_slack_files(rows)


def candidates(warehouse, limit=50, now=NOW):
    return warehouse.slack_file_fingerprint_candidates(limit=limit, now=now)


def ids(rows):
    return [row["file_id"] for row in rows]


# --- the table --------------------------------------------------------------


def test_table_is_registered_and_created(warehouse):
    assert "slack_file_fingerprints" in POSTGRES_TABLES
    spec = POSTGRES_TABLES["slack_file_fingerprints"]
    # One download per file, not per (file, conversation) share.
    assert spec.primary_key == ("account", "team_id", "file_id")
    for column in ("content_sha256", "status", "attempts", "next_attempt_at", "last_error"):
        assert column in spec.columns


def test_the_link_table_stores_no_bytes_and_no_credential(warehouse):
    """The 552 GB of image bytes and the Slack token are both absent by design.

    This table is ``query_access: public``, so anything in it is reachable by
    the read-only role. It may hold a *hash* of content, never content, and
    never a credential.
    """
    columns = set(POSTGRES_TABLES["slack_file_fingerprints"].columns)

    for forbidden in ("content", "body", "data", "blob", "bytes", "payload",
                      "token", "secret", "credential", "authorization", "url"):
        assert forbidden not in columns, forbidden

    # A column may reference content only as a digest of it.
    for column in columns:
        if "content" in column:
            assert column.endswith("_sha256"), column


# --- candidate selection: bounded and resumable -----------------------------


def test_only_undone_live_images_are_candidates(warehouse):
    insert_files(
        warehouse,
        [
            file_row(file_id="F_IMG"),
            file_row(file_id="F_PDF", mimetype="application/pdf", filetype="pdf"),
            file_row(file_id="F_DELETED", is_deleted=1),
        ],
    )

    assert ids(candidates(warehouse)) == ["F_IMG"]


def test_newest_first_so_a_bounded_slice_covers_what_people_ask_about(warehouse):
    insert_files(
        warehouse,
        [
            file_row(file_id="F_OLD", created_at=NOW - timedelta(days=900)),
            file_row(file_id="F_NEW", created_at=NOW - timedelta(days=1)),
            file_row(file_id="F_MID", created_at=NOW - timedelta(days=30)),
        ],
    )

    assert ids(candidates(warehouse, limit=2)) == ["F_NEW", "F_MID"]


def test_a_file_shared_into_two_conversations_is_one_download(warehouse):
    insert_files(
        warehouse,
        [
            file_row(conversation_id="C1", message_ts="1.1"),
            file_row(conversation_id="C2", message_ts="2.2"),
        ],
    )

    assert ids(candidates(warehouse)) == ["F_TESTPOSTER"]


def test_finished_files_are_not_re_fetched(warehouse):
    insert_files(warehouse, [file_row(), file_row(file_id="F_BIG")])
    warehouse.upsert_slack_file_fingerprints(
        [
            _link(status=STATUS_OK, content_sha256="a" * 64),
            _link(file_id="F_BIG", status=STATUS_TOO_LARGE),
        ]
    )

    assert candidates(warehouse) == []


def test_a_backed_off_failure_is_skipped_until_its_time_then_returns(warehouse):
    insert_files(warehouse, [file_row()])
    warehouse.upsert_slack_file_fingerprints(
        [_link(status=STATUS_FAILED, attempts=1, next_attempt_at=NOW + timedelta(hours=2))]
    )

    assert candidates(warehouse, now=NOW) == []
    assert ids(candidates(warehouse, now=NOW + timedelta(hours=3))) == ["F_TESTPOSTER"]


def test_a_file_that_keeps_failing_is_eventually_dropped_from_selection(warehouse):
    insert_files(warehouse, [file_row()])
    warehouse.upsert_slack_file_fingerprints(
        [_link(status=STATUS_FAILED, attempts=99, next_attempt_at=NOW - timedelta(days=1))]
    )

    assert candidates(warehouse, now=NOW) == []


def test_candidates_carry_what_the_fetcher_needs(warehouse):
    insert_files(warehouse, [file_row()])

    row = candidates(warehouse)[0]

    for key in ("account", "team_id", "file_id", "url_private", "mimetype", "name", "size", "attempts"):
        assert key in row
    assert row["url_private"].startswith("https://files.slack.com/")


def test_upsert_is_idempotent(warehouse):
    insert_files(warehouse, [file_row()])
    warehouse.upsert_slack_file_fingerprints([_link(status=STATUS_FAILED, attempts=1)])
    warehouse.upsert_slack_file_fingerprints([_link(status=STATUS_OK, content_sha256="b" * 64, attempts=2)])

    rows = warehouse._query(
        "SELECT status, attempts, content_sha256 FROM @slack_file_fingerprints"
    )
    assert rows == [(STATUS_OK, 2, "b" * 64)]


def _link(**overrides) -> dict:
    row = {
        "account": "zrl",
        "team_id": "T_TESTTEAM",
        "file_id": "F_TESTPOSTER",
        "content_sha256": "",
        "hash_version": "",
        "status": STATUS_OK,
        "attempts": 1,
        "fetched_bytes": 0,
        "last_error": "",
        "last_attempt_at": NOW,
        "next_attempt_at": NO_RETRY,
        "created_at": NOW,
        "updated_at": NOW,
        "sync_version": 1,
    }
    row.update(overrides)
    return row


# --- the marts view and the actual question ---------------------------------


def _seed_matchable(warehouse, *, dhash=POSTER_DHASH, sha="c" * 64):
    insert_files(warehouse, [file_row()])
    warehouse.insert_slack_users(
        [
            {
                "account": "zrl",
                "team_id": "T_TESTTEAM",
                "user_id": "U_TESTUPLOADER",
                "team_user_id": "T_TESTTEAM:U_TESTUPLOADER",
                "name": "designer",
                "real_name": "Poster Designer",
                "display_name": "Poster Designer",
                "email": "",
                "is_bot": 0,
                "is_app_user": 0,
                "is_deleted": 0,
                "tz": "",
                "raw_json": "{}",
                "synced_at": NOW,
                "sync_version": 1,
            }
        ]
    )
    warehouse.insert_slack_conversations(
        [
            {
                "account": "zrl",
                "team_id": "T_TESTTEAM",
                "conversation_id": "D_TESTDM",
                "conversation_type": "im",
                "name": "",
                "is_channel": 0,
                "is_group": 0,
                "is_im": 1,
                "is_mpim": 0,
                "is_private": 1,
                "is_archived": 0,
                "is_member": 1,
                "creator": "",
                "created_at": NOW - timedelta(days=400),
                "topic": "",
                "purpose": "",
                "num_members": 2,
                "raw_json": "{}",
                "synced_at": NOW,
                "sync_version": 1,
            }
        ]
    )
    warehouse.insert_media_fingerprints(
        [
            {
                "content_sha256": sha,
                "hash_version": HASH_VERSION,
                "dhash": dhash,
                "width": 3400,
                "height": 5280,
                "created_at": NOW,
                "sync_version": 1,
            }
        ]
    )
    warehouse.upsert_slack_file_fingerprints(
        [_link(status=STATUS_OK, content_sha256=sha, hash_version=HASH_VERSION)]
    )


def test_view_resolves_the_uploader_and_the_conversation(warehouse):
    _seed_matchable(warehouse)

    rows = warehouse._query(
        "SELECT uploader_display_name, uploader_name, conversation_kind, dhash, is_deleted "
        "FROM @slack_image_fingerprints"
    )

    assert rows == [("Poster Designer", "designer", "im", POSTER_DHASH, 0)]


def test_ranking_query_finds_a_near_duplicate_and_names_who_sent_it(warehouse):
    """The acceptance shape: a *different* hash, one bit off, still matches."""
    _seed_matchable(warehouse)

    sql = build_lookup_sql(NEAR_DHASH, limit=5, max_distance=40)
    matches = parse_matches(_ndjson(warehouse, sql))

    assert len(matches) == 1
    match = matches[0]
    assert match.file_id == "F_TESTPOSTER"
    assert match.distance == 1
    assert match.uploader == "Poster Designer"
    assert match.uploader_user_id == "U_TESTUPLOADER"
    assert match.conversation_id == "D_TESTDM"


def test_a_distant_image_is_excluded_by_the_ceiling(warehouse):
    _seed_matchable(warehouse)

    sql = build_lookup_sql(FAR_DHASH, limit=5, max_distance=10)

    assert _ndjson(warehouse, sql).strip() == ""


def _ndjson(warehouse, sql: str) -> str:
    """Run the lookup SQL against the throwaway schema and return ndjson.

    build_lookup_sql emits the canonical schema name (the HTTP tool API is the
    real caller); tests point it at their namespaced copy.
    """
    import json

    from personal_data_warehouse.relations import relation

    view = relation("slack_image_fingerprints").with_namespace(warehouse.schema_namespace)
    canonical = relation("slack_image_fingerprints")
    localized = sql.replace(f"{canonical.schema}.{canonical.name}", f"{view.schema}.{view.name}")

    with warehouse._connection.cursor() as cursor:
        cursor.execute(localized)
        columns = [description[0] for description in cursor.description]
        rows = cursor.fetchall()
    return "\n".join(
        json.dumps({column: _jsonable(value) for column, value in zip(columns, row)}) for row in rows
    )


def _jsonable(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return value
