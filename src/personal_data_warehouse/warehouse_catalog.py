"""The authoritative warehouse catalog.

``warehouse_catalog.json`` next to this module is the single editable authority
for every managed warehouse object: tables, views, the timeline sequence, the
search functions, the custom types, and the implementation-only helper. Python
loads it directly; the Go representation in ``app/internal/warehouse`` is
generated from it by ``scripts/generate_go_warehouse_catalog.py`` and pinned by
a ``--check`` test, so there is exactly one place to edit when a warehouse
object is added, moved, or renamed.

Layers
------
``base``      faithful provider/source data and full-detail drill-down
``derived``   optional persisted modelling (normalization, identity resolution,
              enrichment, historical facts) — never assume it is disposable
``marts``     stable structured interfaces for domain-specific consumption
``timeline``  the recommended cross-source starting point and search interface
``ops``       cursors/watermarks/runtime state, hidden from ordinary discovery
``private``   credentials, never reachable by the read-only query role
``internal``  implementation-only helpers, hidden from ordinary discovery

This is a graph, not a mandatory staged pipeline: relations may flow straight
from ``base_*`` into ``timeline``, and domains that need modelling pass through
``derived_*`` and/or ``marts_*``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Iterable

CATALOG_PATH = Path(__file__).with_name("warehouse_catalog.json")

#: Layers whose schemas ordinary discovery lists, in the order ``pdw schema``
#: presents them. They also sort naturally that way as plain strings.
PUBLIC_LAYERS: tuple[str, ...] = ("base", "derived", "marts", "timeline")
HIDDEN_LAYERS: tuple[str, ...] = ("internal", "ops", "private")

#: Layers whose schema name is ``<layer>_<domain>``. The rest name the schema
#: after the layer itself.
DOMAIN_PREFIXED_LAYERS: tuple[str, ...] = ("base", "derived", "marts")

#: ``query_access`` values.
#: ``public``        readable by the read-only query role and discoverable
#: ``app_only``      readable by the query role (the app's own operational
#:                   surfaces need it) but never advertised in discovery
#: ``execute_only``  EXECUTE granted so dependent views work; never advertised
#: ``denied``        the query role must not reach it at all
QUERY_ACCESS_VALUES: tuple[str, ...] = ("public", "app_only", "execute_only", "denied")

MAX_IDENTIFIER_BYTES = 63


@dataclass(frozen=True)
class CatalogSchema:
    name: str
    layer: str
    domain: str
    discoverable: bool
    comment: str


@dataclass(frozen=True)
class CatalogObject:
    id: str
    kind: str
    layer: str
    domain: str
    schema: str
    name: str
    discoverable: bool
    query_access: str
    secret: bool
    #: Where this object lived in the pre-reorganization layout. Read ONLY by
    #: the one-shot upgrader (``schema_upgrade.py``) so the migration is
    #: derivable from the catalog instead of hand-transcribed; runtime code must
    #: never resolve a relation through it. Deletable once the upgrade has been
    #: applied everywhere.
    previous_schema: str = ""
    previous_name: str = ""

    @property
    def is_relation(self) -> bool:
        """True for objects that live in ``pg_class`` as a queryable relation."""
        return self.kind in {"table", "view"}


@dataclass(frozen=True)
class StartHere:
    schema: str
    headline: str
    lines: tuple[str, ...]


class WarehouseCatalog:
    def __init__(self, payload: dict) -> None:
        self.version: int = int(payload["version"])
        start = payload["start_here"]
        self.start_here = StartHere(
            schema=start["schema"],
            headline=start["headline"],
            lines=tuple(start["lines"]),
        )
        self.schemas: tuple[CatalogSchema, ...] = tuple(
            CatalogSchema(
                name=row["name"],
                layer=row["layer"],
                domain=row["domain"],
                discoverable=bool(row["discoverable"]),
                comment=row["comment"],
            )
            for row in payload["schemas"]
        )
        self.objects: tuple[CatalogObject, ...] = tuple(
            CatalogObject(
                id=row["id"],
                kind=row["kind"],
                layer=row["layer"],
                domain=row["domain"],
                schema=row["schema"],
                name=row["name"],
                discoverable=bool(row["discoverable"]),
                query_access=row["query_access"],
                secret=bool(row["secret"]),
                previous_schema=row.get("previous", {}).get("schema", ""),
                previous_name=row.get("previous", {}).get("name", ""),
            )
            for row in payload["objects"]
        )
        self.renamed_timeline_source_tables: dict[str, str] = dict(
            payload.get("renamed_timeline_source_tables", {})
        )
        self._by_id = {obj.id: obj for obj in self.objects}
        self._schema_by_name = {schema.name: schema for schema in self.schemas}
        self.validate()

    # -- lookup ------------------------------------------------------------

    def object(self, logical_id: str) -> CatalogObject:
        try:
            return self._by_id[logical_id]
        except KeyError as exc:
            raise KeyError(f"unknown warehouse relation {logical_id!r}") from exc

    def schema(self, name: str) -> CatalogSchema:
        return self._schema_by_name[name]

    def schema_names(self, *, layers: Iterable[str] | None = None) -> tuple[str, ...]:
        wanted = set(layers) if layers is not None else None
        return tuple(
            schema.name for schema in self.schemas if wanted is None or schema.layer in wanted
        )

    # -- policy ------------------------------------------------------------

    def discoverable_schemas(self) -> tuple[str, ...]:
        """Schemas ordinary discovery lists, in presentation order.

        ``base_* → derived_* → marts_* → timeline``, which is also plain
        alphabetical order, so a raw ``\\dn`` in psql reads the same way.
        """
        return tuple(sorted(schema.name for schema in self.schemas if schema.discoverable))

    def all_schemas(self) -> tuple[str, ...]:
        return tuple(schema.name for schema in self.schemas)

    def hidden_schemas(self) -> tuple[str, ...]:
        return tuple(schema.name for schema in self.schemas if not schema.discoverable)

    def query_role_extra_objects(self) -> tuple[CatalogObject, ...]:
        """Individually granted objects outside the blanket-granted schemas."""
        return tuple(
            obj
            for obj in self.objects
            if obj.query_access in {"app_only", "execute_only"}
        )

    def denied_schemas(self) -> tuple[str, ...]:
        """Schemas no object of which may ever be reachable by the query role."""
        return tuple(
            schema.name
            for schema in self.schemas
            if all(
                obj.query_access == "denied"
                for obj in self.objects
                if obj.schema == schema.name
            )
        )

    # -- validation --------------------------------------------------------

    def validate(self) -> None:
        seen_ids: set[str] = set()
        seen_physical: set[tuple[str, str, str]] = set()
        schema_names = {schema.name for schema in self.schemas}

        for schema in self.schemas:
            _check_identifier(schema.name)
            if schema.layer not in PUBLIC_LAYERS + HIDDEN_LAYERS:
                raise ValueError(f"catalog schema {schema.name!r} has unknown layer {schema.layer!r}")
            expected = expected_schema_name(schema.layer, schema.domain)
            if schema.name != expected:
                raise ValueError(
                    f"catalog schema {schema.name!r} does not match layer/domain "
                    f"(expected {expected!r})"
                )
            if schema.discoverable != (schema.layer in PUBLIC_LAYERS):
                raise ValueError(
                    f"catalog schema {schema.name!r} discoverability disagrees with its layer"
                )
            if not schema.comment.strip():
                raise ValueError(f"catalog schema {schema.name!r} needs a comment")

        for obj in self.objects:
            if obj.id in seen_ids:
                raise ValueError(f"duplicate catalog id {obj.id!r}")
            seen_ids.add(obj.id)
            _check_identifier(obj.id)
            _check_identifier(obj.name)
            if obj.kind not in {"table", "view", "sequence", "function", "type"}:
                raise ValueError(f"catalog object {obj.id!r} has unknown kind {obj.kind!r}")
            if obj.layer not in PUBLIC_LAYERS + HIDDEN_LAYERS:
                raise ValueError(f"catalog object {obj.id!r} has unknown layer {obj.layer!r}")
            if obj.schema not in schema_names:
                raise ValueError(f"catalog object {obj.id!r} names unregistered schema {obj.schema!r}")
            schema = self._schema_by_name[obj.schema]
            if (schema.layer, schema.domain) != (obj.layer, obj.domain):
                raise ValueError(
                    f"catalog object {obj.id!r} layer/domain disagrees with schema {obj.schema!r}"
                )
            key = (obj.schema, obj.name, obj.kind)
            if key in seen_physical:
                raise ValueError(f"duplicate physical catalog object {key!r}")
            seen_physical.add(key)
            if obj.query_access not in QUERY_ACCESS_VALUES:
                raise ValueError(
                    f"catalog object {obj.id!r} has unknown query_access {obj.query_access!r}"
                )
            if obj.discoverable != (obj.layer in PUBLIC_LAYERS):
                raise ValueError(f"catalog object {obj.id!r} discoverability disagrees with its layer")
            if obj.discoverable and obj.query_access != "public":
                raise ValueError(f"discoverable catalog object {obj.id!r} must be publicly queryable")
            if obj.secret != (obj.layer == "private"):
                raise ValueError(f"catalog object {obj.id!r} secret flag disagrees with its layer")
            if obj.secret and obj.query_access != "denied":
                raise ValueError(f"secret catalog object {obj.id!r} must deny the query role")
            if bool(obj.previous_schema) != bool(obj.previous_name):
                raise ValueError(f"catalog object {obj.id!r} has a half-specified previous location")

        if self.start_here.schema not in schema_names:
            raise ValueError("start_here names a schema that is not in the catalog")
        for legacy, current in self.renamed_timeline_source_tables.items():
            if legacy in seen_ids:
                raise ValueError(f"renamed timeline source_table {legacy!r} is still a live catalog id")
            if current not in seen_ids:
                raise ValueError(f"renamed timeline source_table maps to unknown id {current!r}")


def expected_schema_name(layer: str, domain: str) -> str:
    if layer in DOMAIN_PREFIXED_LAYERS:
        return f"{layer}_{domain}"
    return layer


def _check_identifier(value: str) -> None:
    if not value or not value.replace("_", "a").isalnum() or value[0].isdigit():
        raise ValueError(f"invalid catalog identifier {value!r}")
    if len(value.encode("utf-8")) > MAX_IDENTIFIER_BYTES:
        raise ValueError(f"catalog identifier {value!r} exceeds PostgreSQL's 63-byte limit")


@lru_cache(maxsize=1)
def load_catalog() -> WarehouseCatalog:
    return WarehouseCatalog(json.loads(CATALOG_PATH.read_text()))


CATALOG = load_catalog()
