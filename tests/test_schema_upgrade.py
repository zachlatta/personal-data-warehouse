"""The one-shot upgrade from the pre-reorganization layout.

The fixture builds a miniature old-layout database — source schemas, the
``marts`` views, the ``search`` functions and row type, the ``util`` helper, the
flat ``ops``-bound state tables (including the ``sync_state_pkey`` collision that
``plaid`` and ``whoop`` both carry), private credentials, seeded rows, and the
orphaned pre-reorg leftovers — then migrates it and asserts the result.

The load-bearing property is that relocating is a *catalog* operation:
``pg_relation_filenode`` must be unchanged for every moved table, because a
70 GB Slack heap cannot be copied inside a maintenance window.
"""

from __future__ import annotations

import os

import psycopg2
import pytest
from dotenv import load_dotenv

from tests.conftest import make_test_schema

from personal_data_warehouse.postgres import (
    POSTGRES_TABLES,
    PostgresWarehouse,
    _default_sql,
    _identifier,
    _postgres_type,
)
from personal_data_warehouse.relations import expand_relations, physical_schema_name
from personal_data_warehouse.schema_upgrade import (
    UPGRADE_PLAN,
    AlreadyUpgraded,
    SchemaUpgrader,
    UpgradePreflightError,
    provision_everything,
)
from personal_data_warehouse.warehouse_catalog import CATALOG


def _postgres_url() -> str:
    load_dotenv()
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if not url:
        pytest.skip("POSTGRES_DATABASE_URL is not set")
    return url


# The subset of the old layout the fixture reproduces: enough shape (renames,
# splits, a name collision, a dependent view/function, a credential table) that
# every branch of the upgrader runs. Each table is built from its real TableSpec
# at its OLD location, so provisioning after the migration sees the columns it
# expects rather than a stub.
_OLD_TABLES: tuple[tuple[str, str, str], ...] = (
    ("gmail_messages", "gmail", "messages"),
    ("gmail_sync_state", "gmail", "sync_state"),
    ("plaid_sync_state", "plaid", "sync_state"),
    ("whoop_sync_state", "whoop", "sync_state"),
    ("slack_account_state_item_rows", "slack", "account_state_item_rows"),
    ("manual_finance_extractions", "manual_finance", "extractions"),
)

# Raw-DDL tables (no TableSpec), created by hand alongside the spec-built ones.
_OLD_RAW_TABLES: tuple[tuple[str, str, str], ...] = (
    (
        "search",
        "schema_state",
        "id smallint PRIMARY KEY DEFAULT 1, signature text NOT NULL, "
        "CONSTRAINT search_schema_state_single_row CHECK (id = 1)",
    ),
    (
        "private",
        "chatgpt_sessions",
        "account text NOT NULL, session_key text NOT NULL DEFAULT 'default', "
        "session_token text NOT NULL DEFAULT '', PRIMARY KEY (account, session_key)",
    ),
)

_SEED: tuple[str, ...] = (
    "INSERT INTO {gmail}.messages (account, message_id, subject) VALUES ('zach@example.test','m1','preserved subject')",
    "INSERT INTO {gmail}.sync_state (account, last_history_id) VALUES ('zach@example.test', 4242)",
    "INSERT INTO {plaid}.sync_state (account, item_id, product) VALUES ('zach@example.test','item-1','transactions')",
    "INSERT INTO {whoop}.sync_state (account, collection) VALUES ('zach@example.test','sleep')",
    "INSERT INTO {slack}.account_state_item_rows (account, scope_id, item_type, item_id) VALUES ('zach@example.test','T1','mention','i1')",
    "INSERT INTO {manual_finance}.extractions (content_sha256) VALUES ('sha-1')",
    "INSERT INTO {search}.schema_state (id, signature) VALUES (1, 'old-signature')",
    "INSERT INTO {private}.chatgpt_sessions (account, session_key, session_token) VALUES ('zach@example.test','default','REDACTED-TEST-TOKEN')",
)


def _create_table_sql(logical: str, schema: str, name: str, *, namespace: str) -> str:
    spec = POSTGRES_TABLES[logical]
    columns = ", ".join(
        f"{_identifier(column)} {_postgres_type(column, table=logical)} NOT NULL "
        f"DEFAULT {_default_sql(column, table=logical)}"
        for column in spec.columns
    )
    primary_key = ", ".join(_identifier(column) for column in spec.primary_key)
    sql = f'CREATE TABLE "{schema}"."{name}" ({columns}, PRIMARY KEY ({primary_key}))'
    # timeline_events.priority is typed through the catalog marker.
    return expand_relations(sql, namespace=namespace)



class _OldLayout:
    """A throwaway namespaced database in the pre-reorganization layout."""

    def __init__(self, url: str, namespace: str) -> None:
        self.namespace = namespace
        self.connection = psycopg2.connect(url)
        self.connection.autocommit = True

    def physical(self, schema: str) -> str:
        return physical_schema_name(schema, namespace=self.namespace)

    def execute(self, sql: str, params: tuple = ()) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)

    def query(self, sql: str, params: tuple = ()) -> list[tuple]:
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

    def build(self) -> None:
        old_schemas = sorted({move.old_schema for move in UPGRADE_PLAN.moves})
        for schema in old_schemas + ["util", "marts"]:
            self.execute(f'CREATE SCHEMA IF NOT EXISTS "{self.physical(schema)}"')
        self.execute(f'CREATE SCHEMA IF NOT EXISTS "{self.namespace}"')

        for logical, schema, name in _OLD_TABLES:
            self.execute(_create_table_sql(logical, self.physical(schema), name, namespace=self.namespace))
        for schema, name, columns in _OLD_RAW_TABLES:
            self.execute(f'CREATE TABLE "{self.physical(schema)}"."{name}" ({columns})')
        seed_schemas = {schema: f'"{self.physical(schema)}"' for _, schema, _ in _OLD_TABLES}
        seed_schemas.update({schema: f'"{self.physical(schema)}"' for schema, _, _ in _OLD_RAW_TABLES})
        for statement in _SEED:
            self.execute(statement.format(**seed_schemas))

        # A timeline table carrying both a live and a renamed source_table token.
        timeline = f'"{self.physical("timeline")}"'
        self.execute(f"CREATE SEQUENCE {timeline}.events_seq")
        self.execute(
            f"CREATE TYPE {timeline}.timeline_priority AS ENUM "
            "('self', 'direct', 'cc', 'noise', 'background', 'unclassified')"
        )
        self.execute(
            _create_table_sql("timeline_events", self.physical("timeline"), "events", namespace=self.namespace)
        )
        self.execute(
            f"ALTER TABLE {timeline}.events ALTER COLUMN seq "
            f"SET DEFAULT nextval('{self.physical('timeline')}.events_seq')"
        )
        self.execute(
            f"INSERT INTO {timeline}.events (adapter, event_id, source_table) VALUES "
            "('gmail_email','m1','gmail_messages'), ('agent_session','s1','agent_session_events')"
        )

        # A generated helper, a dependent view, and the search row type +
        # functions: everything the upgrade rebuilds rather than moves.
        util = f'"{self.physical("util")}"'
        marts = f'"{self.physical("marts")}"'
        search = f'"{self.physical("search")}"'
        self.execute(
            f"CREATE FUNCTION {util}.utf8_byte_prefix(value text, max_bytes integer) RETURNS text "
            "LANGUAGE sql IMMUTABLE AS $$ SELECT left(value, max_bytes) $$"
        )
        self.execute(
            f"CREATE VIEW {marts}.gmail_inbox AS SELECT account, "
            f"{util}.utf8_byte_prefix(subject, 10) AS subject "
            f'FROM "{self.physical("gmail")}".messages'
        )
        self.execute(f"CREATE TYPE {search}.text_hit AS (source text, ref text, text text, score real)")
        self.execute(
            f"CREATE FUNCTION {search}.search_text(query text, max_results integer DEFAULT 50, "
            "sources text[] DEFAULT NULL, since timestamptz DEFAULT NULL) "
            f"RETURNS SETOF {search}.text_hit LANGUAGE sql STABLE AS "
            "$$ SELECT NULL::text, NULL::text, NULL::text, NULL::real WHERE false $$"
        )

        # The pre-reorg leftovers that shadow cataloged objects if left behind.
        base = f'"{self.namespace}"'
        self.execute(f"CREATE SEQUENCE {base}.timeline_events_seq")
        self.execute(
            f"CREATE FUNCTION {base}.pdw_utf8_byte_prefix(value text, max_bytes integer) RETURNS text "
            "LANGUAGE sql IMMUTABLE AS $$ SELECT left(value, max_bytes) $$"
        )

    def schemas(self) -> list[str]:
        """Every physical schema this namespace owns, old layout and new.

        Not a LIKE on the namespace: long labels are hashed into the middle of
        the physical name, so a prefix match silently returns nothing.
        """
        names = {move.old_schema for move in UPGRADE_PLAN.moves}
        names |= set(CATALOG.all_schemas()) | {"util", "marts"}
        return [self.physical(schema) for schema in sorted(names)]

    def inventory(self) -> dict[str, tuple[int, int]]:
        rows = self.query(
            """
            SELECT n.nspname || '.' || c.relname, c.oid::bigint, pg_relation_filenode(c.oid)::bigint
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r' AND n.nspname = ANY(%s)
            """,
            (self.schemas(),),
        )
        return {name: (oid, filenode) for name, oid, filenode in rows}

    def drop(self) -> None:
        try:
            for schema in self.schemas():
                self.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            self.execute(f'DROP SCHEMA IF EXISTS "{self.namespace}" CASCADE')
        finally:
            self.connection.close()


@pytest.fixture()
def old_layout():
    url = _postgres_url()
    layout = _OldLayout(url, make_test_schema("upgrade"))
    layout.build()
    try:
        yield layout
    finally:
        layout.drop()


def test_upgrade_moves_relations_without_rewriting_them(old_layout: _OldLayout) -> None:
    before = old_layout.inventory()
    upgrader = SchemaUpgrader(old_layout.connection, namespace=old_layout.namespace)

    assert not upgrader.already_upgraded()
    report = upgrader.preflight()
    # every fixture table except private.chatgpt_sessions, which stays put
    assert report["relocating"] == len(_OLD_TABLES) + 1
    assert report["timeline_source_table_rewrites"] == {"agent_session_events": 1}

    upgrader.apply()

    after = old_layout.inventory()
    moved = {
        ("gmail", "messages"): ("base_gmail", "messages"),
        ("gmail", "sync_state"): ("ops", "gmail_sync_state"),
        ("plaid", "sync_state"): ("ops", "plaid_sync_state"),
        ("whoop", "sync_state"): ("ops", "whoop_sync_state"),
        ("slack", "account_state_item_rows"): ("derived_slack", "inbox_items"),
        ("manual_finance", "extractions"): ("derived_finance", "document_extractions"),
        ("search", "schema_state"): ("ops", "search_schema_state"),
    }
    for (old_schema, old_name), (new_schema, new_name) in moved.items():
        old_key = f"{old_layout.physical(old_schema)}.{old_name}"
        new_key = f"{old_layout.physical(new_schema)}.{new_name}"
        assert old_key not in after, f"{old_key} still present"
        assert new_key in after, f"{new_key} missing"
        assert before[old_key] == after[new_key], f"{new_key} was rewritten, not moved"


def test_upgrade_preserves_data_and_rewrites_renamed_source_tables(old_layout: _OldLayout) -> None:
    SchemaUpgrader(old_layout.connection, namespace=old_layout.namespace).apply()

    assert old_layout.query(
        f'SELECT subject FROM "{old_layout.physical("base_gmail")}".messages'
    ) == [("preserved subject",)]
    assert old_layout.query(
        f'SELECT last_history_id FROM "{old_layout.physical("ops")}".gmail_sync_state'
    ) == [(4242,)]
    assert old_layout.query(
        f'SELECT signature FROM "{old_layout.physical("ops")}".search_schema_state'
    ) == [("old-signature",)]
    assert old_layout.query(
        f'SELECT session_token FROM "{old_layout.physical("private")}".chatgpt_sessions'
    ) == [("REDACTED-TEST-TOKEN",)]

    assert sorted(
        old_layout.query(
            f'SELECT source_table, count(*) FROM "{old_layout.physical("timeline")}".events '
            "GROUP BY 1 ORDER BY 1"
        )
    ) == [("ai_conversation_events", 1), ("gmail_messages", 1)]


def test_upgrade_renames_only_colliding_indexes(old_layout: _OldLayout) -> None:
    SchemaUpgrader(old_layout.connection, namespace=old_layout.namespace).apply()
    ops = old_layout.physical("ops")
    names = {
        row[0]
        for row in old_layout.query(
            """
            SELECT i.relname
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_index ix ON ix.indrelid = c.oid
            JOIN pg_class i ON i.oid = ix.indexrelid
            WHERE n.nspname = %s
            """,
            (ops,),
        )
    }
    # plaid.sync_state and whoop.sync_state both arrived carrying sync_state_pkey.
    assert "sync_state_pkey" not in names
    assert {"plaid_sync_state_pkey", "whoop_sync_state_pkey"} <= names
    # A name that was already unique keeps it.
    assert "gmail_sync_state_pkey" in names


def test_upgrade_removes_old_schemas_and_shadowing_leftovers(old_layout: _OldLayout) -> None:
    SchemaUpgrader(old_layout.connection, namespace=old_layout.namespace).apply()

    gone = [old_layout.physical(schema) for schema in UPGRADE_PLAN.dropped_schemas]
    assert old_layout.query(
        "SELECT nspname FROM pg_namespace WHERE nspname = ANY(%s)", (gone,)
    ) == []
    # The base namespace must hold nothing: a stale search_text() there shadows
    # the real one for every caller without a warehouse search_path.
    assert old_layout.query(
        """
        SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relkind IN ('r', 'v', 'S')
        UNION ALL
        SELECT p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = %s
        """,
        (old_layout.namespace, old_layout.namespace),
    ) == []


def test_upgrade_is_one_shot(old_layout: _OldLayout) -> None:
    upgrader = SchemaUpgrader(old_layout.connection, namespace=old_layout.namespace)
    upgrader.apply()
    assert upgrader.already_upgraded()
    with pytest.raises(AlreadyUpgraded):
        upgrader.apply()


def test_upgrade_refuses_when_a_target_name_is_already_taken(old_layout: _OldLayout) -> None:
    old_layout.execute(f'CREATE SCHEMA "{old_layout.physical("base_gmail")}"')
    old_layout.execute(f'CREATE TABLE "{old_layout.physical("base_gmail")}".messages (x text)')
    upgrader = SchemaUpgrader(old_layout.connection, namespace=old_layout.namespace)
    with pytest.raises(UpgradePreflightError, match="already exist"):
        upgrader.preflight()


def test_migrated_database_provisions_and_validates_clean(old_layout: _OldLayout) -> None:
    upgrader = SchemaUpgrader(old_layout.connection, namespace=old_layout.namespace)
    upgrader.apply()

    warehouse = PostgresWarehouse(_postgres_url(), schema=old_layout.namespace)
    try:
        provision_everything(warehouse)
    finally:
        warehouse.close()

    result = upgrader.validate()
    assert result["problems"] == []
    assert result["filenodes_preserved"] == len(_OLD_TABLES) + 1
