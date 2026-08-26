"""Catalog-backed relation references for warehouse SQL.

Every warehouse object is declared once in ``warehouse_catalog.json``. This
module turns that catalog into the two things runtime code needs:

* :func:`relation` — a stable logical id resolved to its physical
  ``schema.name`` (namespaced for throwaway test deployments), and
* :func:`expand_relations` — expansion of the explicit ``@logical_id`` marker
  used inside SQL text.

The ``@`` marker replaced an earlier rewriter that silently qualified *bare*
identifiers anywhere in a statement. That was load-bearing and wrong: it could
not tell ``search_text`` the timeline column from ``search_text()`` the
function, and an unknown name simply passed through to Postgres unqualified.
``@name`` is unambiguous — a reference, never a column — and an unknown one
raises here instead of resolving to whatever the search_path happens to hold.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from personal_data_warehouse.warehouse_catalog import CATALOG, WarehouseCatalog

__all__ = [
    "CATALOG",
    "CANONICAL_RELATIONS",
    "ALL_CANONICAL_SCHEMAS",
    "BASE_SCHEMAS",
    "DERIVED_SCHEMAS",
    "DISCOVERABLE_SCHEMAS",
    "HIDDEN_SCHEMAS",
    "MARTS_SCHEMAS",
    "AI_EVENT_SOURCE_RELATIONS",
    "VOICE_EVENT_SOURCE_RELATIONS",
    "PHOTO_SOURCE_RELATIONS",
    "Relation",
    "expand_relations",
    "physical_schema_name",
    "physical_schema_names",
    "quote_identifier",
    "relation",
]


@dataclass(frozen=True)
class Relation:
    logical_name: str
    schema: str
    name: str

    def with_namespace(self, namespace: str = "public") -> "Relation":
        return Relation(
            logical_name=self.logical_name,
            schema=physical_schema_name(self.schema, namespace=namespace),
            name=self.name,
        )

    def sql(self, namespace: str = "public") -> str:
        rel = self.with_namespace(namespace)
        return f"{quote_identifier(rel.schema)}.{quote_identifier(rel.name)}"


def _relations(catalog: WarehouseCatalog) -> dict[str, Relation]:
    return {
        obj.id: Relation(logical_name=obj.id, schema=obj.schema, name=obj.name)
        for obj in catalog.objects
    }


CANONICAL_RELATIONS: dict[str, Relation] = _relations(CATALOG)

BASE_SCHEMAS: tuple[str, ...] = CATALOG.schema_names(layers=("base",))
DERIVED_SCHEMAS: tuple[str, ...] = CATALOG.schema_names(layers=("derived",))
MARTS_SCHEMAS: tuple[str, ...] = CATALOG.schema_names(layers=("marts",))
DISCOVERABLE_SCHEMAS: tuple[str, ...] = CATALOG.discoverable_schemas()
HIDDEN_SCHEMAS: tuple[str, ...] = CATALOG.hidden_schemas()
ALL_CANONICAL_SCHEMAS: tuple[str, ...] = tuple(sorted(CATALOG.all_schemas()))

# Raw AI-conversation event tables, keyed by the ``source`` every agent-session
# envelope carries. Ingest splits by source; marts_ai_conversations.events
# re-unifies them.
AI_EVENT_SOURCE_RELATIONS: dict[str, str] = {
    source: f"{source}_events"
    for source in ("chatgpt", "claude_desktop", "claude_code", "codex", "openclaw", "pi")
}

# Raw voice tables, keyed by the ``source`` marts_voice_memos.recordings tags
# each row with. The timeline reads the MART, not these -- one adapter, one
# transcription pass, one enrichment pass for every voice source -- so this map
# is what says which raw tables that single adapter actually covers.
VOICE_EVENT_SOURCE_RELATIONS: dict[str, str] = {
    "apple_voice_memos": "apple_voice_memos_files",
    "alice_voice_recordings": "alice_voice_recordings",
}

# THE extension point for photo sources. Maps a photo source slug (the
# ``source`` field every /ingest/photos/* envelope carries) to its raw file
# table. This single registry drives Drive-inbox ingest routing, the identity
# runner's unresolved-row scan, and the photo-files mart union — adding a photo
# source is: a new base_<source>.files catalog entry + TableSpec (reusing
# PHOTO_SOURCE_FILE_COLUMNS), one entry here, an uploader that posts the shared
# photo envelope with its own ``source``, and a TIMELINE_TABLE_COVERAGE entry.
# Identity, dedup, thumbnails, enrichment, timeline, and search then follow.
PHOTO_SOURCE_RELATIONS: dict[str, str] = {
    "apple_photos": "apple_photos_files",
}


def relation(logical_name: str) -> Relation:
    try:
        return CANONICAL_RELATIONS[logical_name]
    except KeyError as exc:
        raise KeyError(f"unknown warehouse relation {logical_name!r}") from exc


_RELATION_MARKER = re.compile(r"@([A-Za-z_][A-Za-z0-9_]*)")


def expand_relations(sql: str, *, namespace: str = "public") -> str:
    """Expand ``@logical_id`` markers into schema-qualified relation names.

    Markers inside SQL string literals and comments are left alone, so an email
    address or a ``--`` note never turns into a relation reference. An unknown
    id raises: SQL that names something the catalog does not know must fail
    here, not silently reach Postgres as an unqualified identifier.
    """
    if "@" not in sql:
        return sql

    out: list[str] = []
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]

        if ch == "'":
            start = i
            i += 1
            while i < n:
                if sql[i] == "'":
                    i += 1
                    if i < n and sql[i] == "'":
                        i += 1
                        continue
                    break
                i += 1
            out.append(sql[start:i])
            continue

        if ch == '"':
            start = i
            i += 1
            while i < n:
                if sql[i] == '"':
                    i += 1
                    if i < n and sql[i] == '"':
                        i += 1
                        continue
                    break
                i += 1
            out.append(sql[start:i])
            continue

        if ch == "-" and i + 1 < n and sql[i + 1] == "-":
            start = i
            i += 2
            while i < n and sql[i] != "\n":
                i += 1
            out.append(sql[start:i])
            continue

        if ch == "/" and i + 1 < n and sql[i + 1] == "*":
            start = i
            i += 2
            while i + 1 < n and not (sql[i] == "*" and sql[i + 1] == "/"):
                i += 1
            i = min(n, i + 2)
            out.append(sql[start:i])
            continue

        if ch == "@" and (match := _RELATION_MARKER.match(sql, i)):
            out.append(relation(match.group(1)).sql(namespace=namespace))
            i = match.end()
            continue

        out.append(ch)
        i += 1
    return "".join(out)


def physical_schema_name(schema: str, *, namespace: str = "public") -> str:
    _validate_identifier(schema)
    _validate_identifier(namespace)
    if namespace in {"", "public"}:
        return schema
    combined = f"{namespace}_{schema}"
    if len(combined) <= 63:
        return combined
    # Postgres silently truncates identifiers past NAMEDATALEN-1 (63 bytes),
    # which would collapse every test schema that shares a long namespace into
    # the same physical schema. Keep the pdw_test_ timestamp prefix for the leak
    # reaper, include a namespace hash for uniqueness, and preserve the canonical
    # schema suffix for readability.
    digest = hashlib.sha1(namespace.encode("utf-8")).hexdigest()[:8]
    max_prefix = 63 - len(schema) - len(digest) - 2
    if max_prefix < 1:
        raise ValueError(f"schema name is too long for Postgres identifier: {schema!r}")
    return f"{namespace[:max_prefix]}_{digest}_{schema}"


def physical_schema_names(*, namespace: str = "public", include_hidden: bool = False) -> list[str]:
    schemas = ALL_CANONICAL_SCHEMAS if include_hidden else DISCOVERABLE_SCHEMAS
    return [physical_schema_name(schema, namespace=namespace) for schema in schemas]


def quote_identifier(value: str) -> str:
    return '"' + _validate_identifier(value).replace('"', '""') + '"'


def _validate_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"invalid SQL identifier: {value!r}")
    return value
