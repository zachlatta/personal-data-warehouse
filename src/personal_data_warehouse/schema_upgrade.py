"""One-shot upgrade from the pre-reorganization layout to the cataloged one.

This is deliberately NOT part of ``ensure_*``. Every runtime provisioning path
creates only the target layout; relocating a database that predates the reorg is
an explicit, operator-run, maintenance-window operation:

    uv run python -m personal_data_warehouse.schema_upgrade --check     # preflight only
    uv run python -m personal_data_warehouse.schema_upgrade --apply

What it does, in order:

1. Preflight — confirm the old layout is present, refuse if a target name is
   already taken, and record every moved table's OID, filenode, row estimate,
   size, and grants.
2. Drop the derived surfaces that are regenerated from code: the ``marts``
   views (with the writable-view trigger), the ``search`` functions and row
   type, and the ``util`` helper.
3. Move every table, sequence, and type with ``ALTER ... SET SCHEMA`` /
   ``RENAME``, which is a catalog-only operation: a 70 GB Slack heap keeps its
   filenode and is never copied. Indexes and constraints are renamed only where
   the flat ``ops`` schema would otherwise collide (``plaid.sync_state`` and
   ``whoop.sync_state`` both carry ``sync_state_pkey``).
4. Rewrite the historical ``timeline.events.source_table`` tokens the catalog
   records as renamed.
5. Drop the now-empty old schemas and the orphaned pre-reorg leftovers.
6. Re-run provisioning so the marts, search interface, indexes, grants and
   schema comments are rebuilt in their new homes.
7. Validate — object inventory matches the catalog, every moved table kept its
   OID and filenode, and no old schema, function, or type survives.

The upgrade is one-shot: a second ``--apply`` on an already-migrated database
reports that and exits without touching anything.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass, field

from personal_data_warehouse.config import normalize_postgres_url
from personal_data_warehouse.relations import physical_schema_name, quote_identifier
from personal_data_warehouse.warehouse_catalog import CATALOG, CatalogObject

logger = logging.getLogger(__name__)

#: Objects an older deployment created outside the managed layout and that
#: nothing references any more. They are dropped by the upgrade so the base
#: namespace cannot shadow a cataloged object (a stale ``public.search_text``
#: silently emptied every unqualified search for 16 days).
ORPHANED_LEFTOVERS: tuple[tuple[str, str, str], ...] = (
    ("sequence", "public", "timeline_events_seq"),
    ("function", "public", "pdw_utf8_byte_prefix(text, integer)"),
    ("function", "public", "search_text(text, integer, text[], timestamptz)"),
    ("function", "public", "search_text_exact(text, integer, text[], timestamptz)"),
    ("function", "public", "search_text_sources()"),
    ("type", "public", "search_text_hit"),
    ("view", "public", "searchable_text"),
    ("view", "public", "person_identities"),
)


#: Types that provisioning recreates from code. Everything else must be moved:
#: ``timeline.timeline_priority`` backs a stored column, so dropping it would
#: rewrite the 43M-row timeline. ``search.text_hit`` is only the search
#: functions' row type, and those are regenerated with it.
REGENERATED_TYPE_IDS: frozenset[str] = frozenset({"search_text_hit"})


@dataclass(frozen=True)
class ObjectMove:
    """One catalog object's pre-reorg location and its target."""

    id: str
    kind: str
    old_schema: str
    old_name: str
    new_schema: str
    new_name: str

    @property
    def regenerated(self) -> bool:
        """True when provisioning rebuilds this object instead of moving it."""
        if self.kind in {"view", "function"}:
            return True
        return self.kind == "type" and self.id in REGENERATED_TYPE_IDS

    @property
    def moves_schema(self) -> bool:
        return self.old_schema != self.new_schema

    @property
    def renames(self) -> bool:
        return self.old_name != self.new_name


@dataclass(frozen=True)
class UpgradePlan:
    """The complete old → new mapping, derived from the catalog."""

    moves: tuple[ObjectMove, ...]
    dropped_schemas: tuple[str, ...]
    timeline_source_table_renames: dict[str, str] = field(default_factory=dict)

    @property
    def relocations(self) -> tuple[ObjectMove, ...]:
        """Moves that actually change a physical name."""
        return tuple(move for move in self.moves if move.moves_schema or move.renames)

    @property
    def regenerated(self) -> tuple[ObjectMove, ...]:
        """Objects dropped and rebuilt from code rather than relocated."""
        return tuple(move for move in self.moves if move.regenerated)

    @property
    def relocated_tables(self) -> tuple[ObjectMove, ...]:
        return tuple(move for move in self.relocations if move.kind == "table")

def _build_plan() -> UpgradePlan:
    moves: list[ObjectMove] = []
    for obj in CATALOG.objects:
        if not obj.previous_schema:
            # An object added after the reorganization never existed in the old
            # layout, so there is nothing to relocate: normal provisioning
            # creates it. Only objects that predate the reorg carry a previous
            # location, and only those are part of this migration.
            continue
        moves.append(
            ObjectMove(
                id=obj.id,
                kind=obj.kind,
                old_schema=obj.previous_schema,
                old_name=obj.previous_name,
                new_schema=obj.schema,
                new_name=obj.name,
            )
        )
    old_schemas = {move.old_schema for move in moves}
    new_schemas = set(CATALOG.all_schemas())
    return UpgradePlan(
        moves=tuple(moves),
        dropped_schemas=tuple(sorted(old_schemas - new_schemas)),
        timeline_source_table_renames=dict(CATALOG.renamed_timeline_source_tables),
    )


UPGRADE_PLAN = _build_plan()


class AlreadyUpgraded(RuntimeError):
    """Raised when the database is already in the cataloged layout."""


class UpgradePreflightError(RuntimeError):
    """Raised when the database is not in a state this upgrade can migrate."""


class SchemaUpgrader:
    def __init__(self, connection, *, namespace: str = "public") -> None:
        self._connection = connection
        self._namespace = namespace
        self.preflight_report: dict[str, object] = {}

    # -- helpers -----------------------------------------------------------

    def _physical(self, schema: str) -> str:
        return physical_schema_name(schema, namespace=self._namespace)

    def _query(self, sql: str, params: tuple = ()) -> list[tuple]:
        with self._connection.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

    def _execute(self, sql: str, params: tuple = ()) -> None:
        logger.info("upgrade: %s", " ".join(sql.split())[:200])
        with self._connection.cursor() as cursor:
            cursor.execute(sql, params)

    def _relation_exists(self, schema: str, name: str) -> bool:
        return bool(
            self._query(
                """
                SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s LIMIT 1
                """,
                (schema, name),
            )
        )

    # -- preflight ---------------------------------------------------------

    def already_upgraded(self) -> bool:
        """True when no relocation is left to do."""
        for move in UPGRADE_PLAN.relocated_tables:
            if self._relation_exists(self._physical(move.old_schema), move.old_name):
                return False
        return True

    def preflight(self) -> dict[str, object]:
        """Record the pre-migration state and refuse anything ambiguous."""
        missing: list[str] = []
        collisions: list[str] = []
        inventory: list[dict[str, object]] = []

        for move in UPGRADE_PLAN.relocated_tables:
            old_schema = self._physical(move.old_schema)
            new_schema = self._physical(move.new_schema)
            old_present = self._relation_exists(old_schema, move.old_name)
            new_present = self._relation_exists(new_schema, move.new_name)
            if not old_present and not new_present:
                missing.append(f"{old_schema}.{move.old_name}")
                continue
            if old_present and new_present:
                collisions.append(
                    f"{old_schema}.{move.old_name} -> {new_schema}.{move.new_name} (target exists)"
                )
                continue
            if not old_present:
                continue
            rows = self._query(
                """
                SELECT c.oid::bigint, pg_relation_filenode(c.oid)::bigint,
                       c.reltuples::bigint, pg_total_relation_size(c.oid)::bigint
                FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s
                """,
                (old_schema, move.old_name),
            )
            oid, filenode, tuples, size = rows[0]
            inventory.append(
                {
                    "id": move.id,
                    "from": f"{old_schema}.{move.old_name}",
                    "to": f"{new_schema}.{move.new_name}",
                    "oid": oid,
                    "filenode": filenode,
                    "row_estimate": tuples,
                    "total_bytes": size,
                }
            )

        if collisions:
            raise UpgradePreflightError(
                "target relations already exist; refusing to migrate:\n  " + "\n  ".join(collisions)
            )

        self.preflight_report = {
            "namespace": self._namespace,
            "relocating": len(inventory),
            "missing_sources": missing,
            "inventory": inventory,
            "timeline_source_table_rewrites": self._timeline_rewrite_counts(),
        }
        return self.preflight_report

    def _timeline_rewrite_counts(self) -> dict[str, int]:
        timeline_schema = self._physical("timeline")
        if not self._relation_exists(timeline_schema, "events"):
            return {}
        counts: dict[str, int] = {}
        for legacy in UPGRADE_PLAN.timeline_source_table_renames:
            rows = self._query(
                f"SELECT count(*) FROM {quote_identifier(timeline_schema)}.\"events\" WHERE source_table = %s",
                (legacy,),
            )
            counts[legacy] = int(rows[0][0])
        return counts

    # -- migration ---------------------------------------------------------

    def apply(self) -> dict[str, object]:
        if self.already_upgraded():
            raise AlreadyUpgraded("database is already in the cataloged layout")
        report = self.preflight()
        self._create_target_schemas()
        self._drop_regenerated_objects()
        self._relocate()
        self._rewrite_timeline_source_tables()
        self._drop_orphaned_leftovers()
        self._drop_old_schemas()
        return report

    def _create_target_schemas(self) -> None:
        for schema in CATALOG.all_schemas():
            self._execute(f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(self._physical(schema))}")

    def _drop_regenerated_objects(self) -> None:
        """Drop the derived surfaces provisioning rebuilds from code.

        Views, the search functions and their row type, and the helper function
        are all generated; moving them would only preserve a definition that the
        next ``ensure_*`` overwrites anyway. Dropping the old ``marts`` view set
        also removes the writable-view INSTEAD OF trigger and its function,
        which the reorg retires (writes route to the source tables directly).
        """
        for move in UPGRADE_PLAN.regenerated:
            schema = quote_identifier(self._physical(move.old_schema))
            name = quote_identifier(move.old_name)
            if move.kind == "view":
                self._execute(f"DROP VIEW IF EXISTS {schema}.{name} CASCADE")
            elif move.kind == "function":
                for signature in ("(text, integer, text[], timestamptz)", "(text, integer)", "()"):
                    self._execute(f"DROP FUNCTION IF EXISTS {schema}.{name}{signature} CASCADE")
            else:
                self._execute(f"DROP TYPE IF EXISTS {schema}.{name} CASCADE")

        # The writable-mart compatibility trigger function is not a catalog
        # object in the target layout, so it has to be named explicitly.
        marts = quote_identifier(self._physical("marts"))
        self._execute(f"DROP FUNCTION IF EXISTS {marts}.\"ai_conversation_events_insert\"() CASCADE")

    def _relocate(self) -> None:
        claimed: dict[str, set[str]] = {}
        for move in UPGRADE_PLAN.relocations:
            if move.regenerated:
                continue  # rebuilt from code, not moved
            old_schema = self._physical(move.old_schema)
            new_schema = self._physical(move.new_schema)
            if not self._relation_exists(old_schema, move.old_name) and move.kind != "type":
                continue
            keyword = {"table": "TABLE", "sequence": "SEQUENCE", "type": "TYPE"}[move.kind]
            old_ref = f"{quote_identifier(old_schema)}.{quote_identifier(move.old_name)}"
            if move.moves_schema:
                self._execute(f"ALTER {keyword} {old_ref} SET SCHEMA {quote_identifier(new_schema)}")
            moved_ref = f"{quote_identifier(new_schema)}.{quote_identifier(move.old_name)}"
            if move.renames:
                self._execute(
                    f"ALTER {keyword} {moved_ref} RENAME TO {quote_identifier(move.new_name)}"
                )
            if move.kind == "table":
                self._rename_colliding_indexes(move, new_schema, claimed)

    def _rename_colliding_indexes(
        self, move: ObjectMove, new_schema: str, claimed: dict[str, set[str]]
    ) -> None:
        """Keep index/constraint names unique inside the flat ops schema.

        Index names are per-schema, and the reorg funnels eight ``sync_state``
        tables into one ``ops`` schema — ``plaid.sync_state`` and
        ``whoop.sync_state`` both arrive carrying ``sync_state_pkey``. Rename
        only what would actually collide, so every other index keeps the name
        operators already know.
        """
        taken = claimed.setdefault(new_schema, set())
        indexes = [
            row[0]
            for row in self._query(
                """
                SELECT i.relname
                FROM pg_class t
                JOIN pg_namespace n ON n.oid = t.relnamespace
                JOIN pg_index ix ON ix.indrelid = t.oid
                JOIN pg_class i ON i.oid = ix.indexrelid
                WHERE n.nspname = %s AND t.relname = %s
                ORDER BY i.relname
                """,
                (new_schema, move.new_name),
            )
        ]
        for index in indexes:
            desired = index
            if index.startswith(f"{move.old_name}_"):
                desired = f"{move.new_name}_{index[len(move.old_name) + 1 :]}"
            elif index in taken:
                desired = f"{move.new_name}_{index}"
            if len(desired.encode("utf-8")) > 63:
                raise UpgradePreflightError(f"renamed index {desired!r} exceeds 63 bytes")
            if desired != index:
                self._execute(
                    f"ALTER INDEX {quote_identifier(new_schema)}.{quote_identifier(index)} "
                    f"RENAME TO {quote_identifier(desired)}"
                )
            taken.add(desired)

    def _rewrite_timeline_source_tables(self) -> None:
        """Point historical timeline rows at their current catalog id.

        Only the renamed tokens are touched — a few thousand rows — never the
        43M-row table as a whole. Physical relocations do not change
        ``source_table`` at all: it stores catalog ids, not SQL names.
        """
        timeline = quote_identifier(self._physical("timeline"))
        if not self._relation_exists(self._physical("timeline"), "events"):
            return
        for legacy, current in UPGRADE_PLAN.timeline_source_table_renames.items():
            self._execute(
                f'UPDATE {timeline}."events" SET source_table = %s WHERE source_table = %s',
                (current, legacy),
            )

    def _drop_orphaned_leftovers(self) -> None:
        base = quote_identifier(self._namespace)
        for kind, schema, name in ORPHANED_LEFTOVERS:
            target = base if schema == "public" and self._namespace != "public" else quote_identifier(schema)
            keyword = {"sequence": "SEQUENCE", "function": "FUNCTION", "type": "TYPE", "view": "VIEW"}[kind]
            reference = name if kind == "function" else quote_identifier(name)
            self._execute(f"DROP {keyword} IF EXISTS {target}.{reference}")

    def _drop_old_schemas(self) -> None:
        for schema in UPGRADE_PLAN.dropped_schemas:
            physical = self._physical(schema)
            remaining = self._query(
                """
                SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relkind IN ('r', 'p', 'S')
                """,
                (physical,),
            )
            if remaining:
                raise UpgradePreflightError(
                    f"refusing to drop schema {physical}: still holds {[row[0] for row in remaining]}"
                )
            # Only views/functions/types can be left, and all of them are
            # regenerated by provisioning.
            self._execute(f"DROP SCHEMA IF EXISTS {quote_identifier(physical)} CASCADE")

    # -- validation --------------------------------------------------------

    def validate(self) -> dict[str, object]:
        problems: list[str] = []

        expected_relations = {
            (self._physical(obj.schema), obj.name) for obj in CATALOG.objects if obj.is_relation
        }
        schemas = [self._physical(schema) for schema in CATALOG.all_schemas()]
        actual_relations = {
            (schema, name)
            for schema, name in self._query(
                """
                SELECT n.nspname, c.relname
                FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ANY(%s) AND c.relkind IN ('r', 'p', 'v', 'm')
                """,
                (schemas,),
            )
        }
        for missing in sorted(expected_relations - actual_relations):
            problems.append(f"missing relation {missing[0]}.{missing[1]}")
        for extra in sorted(actual_relations - expected_relations):
            problems.append(f"unexpected relation {extra[0]}.{extra[1]}")

        surviving = self._query(
            "SELECT nspname FROM pg_namespace WHERE nspname = ANY(%s)",
            ([self._physical(schema) for schema in UPGRADE_PLAN.dropped_schemas],),
        )
        for (schema,) in surviving:
            problems.append(f"pre-reorg schema still present: {schema}")

        preserved = 0
        for entry in self.preflight_report.get("inventory", []):  # type: ignore[union-attr]
            schema, _, name = str(entry["to"]).partition(".")
            rows = self._query(
                """
                SELECT c.oid::bigint, pg_relation_filenode(c.oid)::bigint
                FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s
                """,
                (schema, name),
            )
            if not rows:
                problems.append(f"moved relation missing after migration: {entry['to']}")
                continue
            oid, filenode = rows[0]
            if oid != entry["oid"] or filenode != entry["filenode"]:
                problems.append(
                    f"{entry['to']} was rewritten (oid {entry['oid']}->{oid}, "
                    f"filenode {entry['filenode']}->{filenode})"
                )
            else:
                preserved += 1

        return {"problems": problems, "filenodes_preserved": preserved}


def _object_signature(obj: CatalogObject) -> str:
    return f"{obj.previous_schema}.{obj.previous_name} -> {obj.schema}.{obj.name}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="run the migration (default: preflight only)")
    parser.add_argument("--check", action="store_true", help="preflight only, never mutate")
    parser.add_argument("--namespace", default="public", help="schema namespace (tests only)")
    parser.add_argument("--print-plan", action="store_true", help="print the old -> new map and exit")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.print_plan:
        for obj in CATALOG.objects:
            print(_object_signature(obj))
        return 0

    import psycopg2

    url = normalize_postgres_url(os.environ.get("POSTGRES_DATABASE_URL", ""))
    if not url:
        print("POSTGRES_DATABASE_URL must be set", file=sys.stderr)
        return 2
    connection = psycopg2.connect(url)
    connection.autocommit = True
    upgrader = SchemaUpgrader(connection, namespace=args.namespace)
    try:
        if upgrader.already_upgraded():
            print("already migrated: nothing to do")
            return 0
        report = upgrader.preflight()
        print(f"relocating {report['relocating']} relations")
        if report["missing_sources"]:
            print(f"  (absent in this database: {report['missing_sources']})")
        print(f"  timeline source_table rewrites: {report['timeline_source_table_rewrites']}")
        if not args.apply or args.check:
            print("preflight only; pass --apply to migrate")
            return 0

        upgrader.apply()

        from personal_data_warehouse.postgres import PostgresWarehouse

        warehouse = PostgresWarehouse(url, schema=args.namespace)
        try:
            provision_everything(warehouse)
        finally:
            warehouse.close()

        result = upgrader.validate()
        print(f"filenodes preserved: {result['filenodes_preserved']}")
        if result["problems"]:
            for problem in result["problems"]:  # type: ignore[union-attr]
                print(f"  PROBLEM: {problem}", file=sys.stderr)
            return 1
        print("migration complete and validated")
        return 0
    finally:
        connection.close()


def provision_everything(warehouse) -> None:
    """Run every ensure_* path so the new layout is fully built."""
    warehouse.ensure_tables()
    warehouse.ensure_calendar_tables()
    warehouse.ensure_contacts_tables()
    warehouse.ensure_google_drive_source_tables()
    warehouse.ensure_slack_tables()
    warehouse.ensure_apple_notes_tables()
    warehouse.ensure_apple_messages_tables()
    warehouse.ensure_apple_voice_memos_tables(backfill_content_hashes=False)
    warehouse.ensure_alice_voice_recordings_tables()
    warehouse.ensure_whatsapp_tables()
    warehouse.ensure_whatsapp_client_session_table()
    warehouse.ensure_photos_tables()
    warehouse.ensure_whoop_tables()
    warehouse.ensure_agent_sessions_tables()
    warehouse.ensure_plaid_tables()
    warehouse.ensure_finance_tables()
    warehouse.ensure_manual_finance_tables()
    warehouse.ensure_receipt_tables()
    warehouse.ensure_agent_tables()
    warehouse.ensure_timeline_tables()
    warehouse.ensure_upstream_mutation_tables()
    warehouse.ensure_pipeline_health_tables()


if __name__ == "__main__":
    raise SystemExit(main())
