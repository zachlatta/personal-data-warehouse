"""A test warehouse cloned from a template is the warehouse its helper builds.

Tests that call a ``@provisions_template`` helper start from a database cloned
from a template that helper built once per worker (tests/local_test_runtime.py).
That is only sound while the clone is indistinguishable from what the helper
builds from scratch, so this compares the two catalogs object by object.
"""

from __future__ import annotations

import os

import psycopg2
import pytest

from personal_data_warehouse.postgres import (
    PostgresWarehouse,
    index_refresh_lock_key,
    query_role_setup_lock_key,
    search_chunk_vectors_lock_key,
)
from personal_data_warehouse.relations import ALL_CANONICAL_SCHEMAS, physical_schema_name
from tests.local_test_runtime import DisposableDatabases
from tests.test_pipeline_health import _provision_every_table
from tests.test_timeline import _ensure_all_source_tables

# Columns whose value is the moment provisioning ran, so a template built
# minutes earlier legitimately differs from a fresh build.
_BUILD_TIME_COLUMNS = {"updated_at", "created_at"}


def _owned_postgres_url() -> str:
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if not url:
        pytest.skip("POSTGRES_DATABASE_URL is not set")
    return url


def _catalog(url: str, namespace: str) -> dict[str, object]:
    """Every object a warehouse namespace holds, named by logical schema."""
    # Physical names embed (a hash of) the namespace; longest first so a name
    # that contains the bare namespace is rewritten as a whole.
    logical = {physical_schema_name(schema, namespace=namespace): f"<{schema}>" for schema in ALL_CANONICAL_SCHEMAS}
    logical[namespace] = "<namespace>"
    # Advisory-lock keys are derived from the namespace too (trigger bodies
    # embed them), so they name the lock, not its per-namespace number.
    lock_keys = {
        str(key(namespace)): f"<{key.__name__}>"
        for key in (index_refresh_lock_key, query_role_setup_lock_key, search_chunk_vectors_lock_key)
    }
    renames = sorted({**logical, **lock_keys}.items(), key=lambda item: -len(item[0]))

    def normalized(value: object) -> str:
        text = str(value)
        for physical, name in renames:
            text = text.replace(physical, name)
        return text

    connection = psycopg2.connect(url)
    try:
        with connection.cursor() as cursor:
            def rows(sql: str) -> list[tuple]:
                cursor.execute(sql, {"schemas": list(logical)})
                return sorted(tuple(normalized(value) for value in row) for row in cursor.fetchall())

            in_namespace = "n.nspname = ANY(%(schemas)s)"
            catalog: dict[str, object] = {
                "schemas": rows(
                    f"SELECT n.nspname, obj_description(n.oid, 'pg_namespace'), n.nspacl "
                    f"FROM pg_namespace n WHERE {in_namespace}"
                ),
                "relations": rows(
                    f"SELECT n.nspname, c.relname, c.relkind, obj_description(c.oid, 'pg_class'), c.relacl, "
                    f"c.reloptions FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                    f"WHERE {in_namespace}"
                ),
                "columns": rows(
                    f"SELECT n.nspname, c.relname, a.attname, format_type(a.atttypid, a.atttypmod), "
                    f"a.attnotnull, pg_get_expr(d.adbin, d.adrelid), a.attacl "
                    f"FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
                    f"JOIN pg_namespace n ON n.oid = c.relnamespace "
                    f"LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum "
                    f"WHERE {in_namespace} AND a.attnum > 0 AND NOT a.attisdropped"
                ),
                "indexes": rows(
                    f"SELECT n.nspname, c.relname, pg_get_indexdef(i.indexrelid), i.indisvalid "
                    f"FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
                    f"JOIN pg_namespace n ON n.oid = c.relnamespace WHERE {in_namespace}"
                ),
                "constraints": rows(
                    f"SELECT n.nspname, con.conname, pg_get_constraintdef(con.oid) "
                    f"FROM pg_constraint con JOIN pg_namespace n ON n.oid = con.connamespace "
                    f"WHERE {in_namespace}"
                ),
                "functions": rows(
                    f"SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid), "
                    f"pg_get_functiondef(p.oid), p.proacl "
                    f"FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
                    f"WHERE {in_namespace} AND p.prokind IN ('f', 'p', 'w')"
                ),
                "views": rows(
                    f"SELECT n.nspname, c.relname, pg_get_viewdef(c.oid) "
                    f"FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                    f"WHERE {in_namespace} AND c.relkind IN ('v', 'm')"
                ),
                "triggers": rows(
                    f"SELECT n.nspname, c.relname, pg_get_triggerdef(t.oid) "
                    f"FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
                    f"JOIN pg_namespace n ON n.oid = c.relnamespace "
                    f"WHERE {in_namespace} AND NOT t.tgisinternal"
                ),
                "types": rows(
                    f"SELECT n.nspname, t.typname, t.typtype FROM pg_type t "
                    f"JOIN pg_namespace n ON n.oid = t.typnamespace WHERE {in_namespace}"
                ),
            }
            cursor.execute(
                "SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = ANY(%s) AND c.relkind IN ('r', 'p')",
                (list(logical),),
            )
            data = {}
            for schema, table in cursor.fetchall():
                cursor.execute(f'SELECT * FROM "{schema}"."{table}"')
                names = [column.name for column in cursor.description]
                data[f"{normalized(schema)}.{table}"] = sorted(
                    tuple(
                        "<build time>" if name in _BUILD_TIME_COLUMNS else normalized(value)
                        for name, value in zip(names, row)
                    )
                    for row in cursor.fetchall()
                )
            catalog["rows"] = data
            return catalog
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("provision", "worker"),
    [(_ensure_all_source_tables, "gw9000"), (_provision_every_table, "gw9001")],
    ids=["_ensure_all_source_tables", "_provision_every_table"],
)
def test_a_cloned_template_is_exactly_what_its_helper_builds(provision, worker) -> None:
    databases = DisposableDatabases.create(_owned_postgres_url(), worker=worker)
    try:
        fresh_url = databases.clone(databases.base_template)
        fresh_namespace = f"{databases.namespace}_fresh"
        warehouse = PostgresWarehouse(fresh_url, schema=fresh_namespace)
        try:
            provision.__wrapped__(warehouse)
        finally:
            warehouse.close()

        cloned_url = databases.clone(databases.template_for(provision))

        fresh = _catalog(fresh_url, fresh_namespace)
        cloned = _catalog(cloned_url, databases.namespace)
        assert len(fresh["relations"]) > 500, "the helper built less than a whole warehouse"
        for kind in fresh:
            assert cloned[kind] == fresh[kind], f"cloned template differs from a fresh build in {kind}"
    finally:
        databases.close()
