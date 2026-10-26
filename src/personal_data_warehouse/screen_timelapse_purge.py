"""Execute `screen_timelapse.purge_moment`: delete one captured screen moment.

The proposal half is Go (app/internal/mutations/screen_timelapse_purge.go);
this is the cloud worker's half. A purge runs in two phases, and the order is
the point:

1. **Rows, in one transaction.** The moment's screenshots (one display or all),
   its accessibility tree (one per moment, so it goes whichever display is
   named), the agent readings of those frames, and -- when display 1 goes --
   the moment's `screen_capture` timeline event. The same transaction writes
   the storage ids it removed into the mutation's own `result_json`, so the
   object list survives a crash or a failed Drive call.
2. **Objects, after the commit.** Each stored file is deleted through the
   object store and read back until Drive answers not-found. A failure leaves
   the mutation `failed_retryable` with phase `rows_deleted`; the next claim
   skips straight to the objects. Deleting a Drive file that is already gone is
   a no-op, so a retry is always safe, and no row ever points at a deleted file.

Nothing that is deleted comes back by itself:

- The timeline sync is held off by its own advisory lock while the rows go, so
  no run holding a snapshot from before the purge can write the event back.
- The enrichment pass writes a reading only while its screenshot row still
  exists with the same bytes (`insert_screen_timelapse_descriptions`).
- The uploader re-sends a file only when its size or mtime changed; that is how
  a redacted copy comes back in, as a new row.

Local files on the device and pgBackRest backups on slowking are not touched;
the result says so. No content (screen text, tree text, readings) is recorded.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from personal_data_warehouse.objectstore import ObjectNotFoundError
from personal_data_warehouse.postgres import _jsonb_param
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.timeline import TIMELINE_ADAPTERS


SCREEN_TIMELAPSE_PROVIDER = "screen_timelapse"
SCREEN_TIMELAPSE_PURGE_MOMENT_OPERATION = "screen_timelapse.purge_moment"

# The timeline sync's own lock (defs/timeline_sync.py).
TIMELINE_SYNC_LOCK_NAME = "timeline_sync"
# A sync run takes up to its 240s budget every five minutes; waiting a little
# catches the gap often, and a miss is a retry on the next worker tick.
TIMELINE_SYNC_LOCK_WAIT_SECONDS = 30.0

PHASE_ROWS_DELETED = "rows_deleted"
PHASE_COMPLETE = "complete"
NOT_TOUCHED = (
    "local source files on the device",
    "pgBackRest backups on slowking",
)

_SCREEN_CAPTURE_ADAPTER = next(adapter for adapter in TIMELINE_ADAPTERS if adapter.name == "screen_capture")


@dataclass(frozen=True)
class ScreenTimelapsePurgeResult:
    status: str
    result_json: dict[str, Any]
    error: str = ""


def _timeline_sync_lock() -> AbstractContextManager[bool]:
    # Imported here: the defs module pulls in Dagster definitions.
    from personal_data_warehouse.defs.timeline_sync import TIMELINE_SYNC_POSTGRES_LOCK_ID

    return exclusive_sync_lock(
        name=TIMELINE_SYNC_LOCK_NAME,
        postgres_lock_id=TIMELINE_SYNC_POSTGRES_LOCK_ID,
        wait_seconds=TIMELINE_SYNC_LOCK_WAIT_SECONDS,
    )


class ScreenTimelapsePurgeExecutor:
    def __init__(
        self,
        *,
        warehouse,
        object_store_factory: Callable[[], Any],
        timeline_lock: Callable[[], AbstractContextManager[bool]] = _timeline_sync_lock,
    ) -> None:
        self._warehouse = warehouse
        self._object_store_factory = object_store_factory
        self._timeline_lock = timeline_lock
        self._object_store = None

    def execute(self, mutation: Mapping[str, Any]) -> ScreenTimelapsePurgeResult:
        if (
            mutation.get("provider") != SCREEN_TIMELAPSE_PROVIDER
            or mutation.get("operation") != SCREEN_TIMELAPSE_PURGE_MOMENT_OPERATION
        ):
            return ScreenTimelapsePurgeResult(
                status="failed_terminal",
                result_json={},
                error=f"unsupported screen timelapse mutation {mutation.get('operation')!r}",
            )
        prior = dict(mutation.get("result_json") or {})
        if prior.get("phase") == PHASE_ROWS_DELETED:
            return self._delete_objects(prior)
        try:
            target = _target(mutation.get("payload_json") or {})
        except ValueError as exc:
            return ScreenTimelapsePurgeResult(status="failed_terminal", result_json={}, error=str(exc))
        with self._timeline_lock() as acquired:
            if not acquired:
                return ScreenTimelapsePurgeResult(
                    status="failed_retryable",
                    result_json={},
                    error="the timeline sync is running; the rows are deleted once it finishes",
                )
            recorded = self._delete_rows(str(mutation["id"]), target)
        if recorded is None:
            return ScreenTimelapsePurgeResult(
                status="failed_terminal",
                result_json={"device": target["device"], "captured_at": target["captured_at"].isoformat()},
                error="no screen timelapse rows match this moment any more; nothing was deleted",
            )
        return self._delete_objects(recorded)

    def _delete_rows(self, mutation_id: str, target: Mapping[str, Any]) -> dict[str, Any] | None:
        """Phase 1. Returns the recorded result, or None when nothing matched."""
        warehouse = self._warehouse
        device = target["device"]
        captured_at = target["captured_at"]
        display = int(target["display_index"])
        storage_columns = "content_sha256, size_bytes, storage_backend, storage_key, storage_file_id"
        with warehouse._connection.cursor() as cursor:
            cursor.execute("BEGIN")
            try:
                # Screenshots first: their row locks are what a concurrent
                # enrichment write waits on before it finds the frame gone.
                cursor.execute(
                    warehouse._expand_relations(
                        f"""
                        DELETE FROM @screen_timelapse_screenshots
                        WHERE device = %s AND captured_at = %s AND (%s = 0 OR display_index = %s)
                        RETURNING display_index, {storage_columns}
                        """
                    ),
                    (device, captured_at, display, display),
                )
                screenshots = [_object_row(row, relation="screen_timelapse_screenshots") for row in cursor.fetchall()]
                cursor.execute(
                    warehouse._expand_relations(
                        f"""
                        DELETE FROM @screen_timelapse_accessibility_trees
                        WHERE device = %s AND captured_at = %s
                        RETURNING NULL::bigint, {storage_columns}
                        """
                    ),
                    (device, captured_at),
                )
                trees = [_object_row(row, relation="screen_timelapse_accessibility_trees") for row in cursor.fetchall()]
                if not screenshots and not trees:
                    cursor.execute("ROLLBACK")
                    return None
                cursor.execute(
                    warehouse._expand_relations(
                        """
                        DELETE FROM @screen_timelapse_descriptions
                        WHERE device = %s AND captured_at = %s AND (%s = 0 OR display_index = %s)
                        """
                    ),
                    (device, captured_at, display, display),
                )
                descriptions = cursor.rowcount or 0
                timeline_event_ids: list[str] = []
                search_chunks = 0
                if any(row["display_index"] == 1 for row in screenshots) and warehouse._relation_exists(
                    "timeline_events"
                ):
                    event_id = self._capture_event_id(cursor, device, captured_at)
                    if warehouse._relation_exists("search_chunks"):
                        cursor.execute(
                            warehouse._expand_relations(
                                "DELETE FROM @search_chunks WHERE adapter = %s AND event_id = %s"
                            ),
                            (_SCREEN_CAPTURE_ADAPTER.name, event_id),
                        )
                        search_chunks = cursor.rowcount or 0
                    cursor.execute(
                        warehouse._expand_relations(
                            "DELETE FROM @timeline_events WHERE adapter = %s AND event_id = %s RETURNING event_id"
                        ),
                        (_SCREEN_CAPTURE_ADAPTER.name, event_id),
                    )
                    timeline_event_ids = [str(row[0]) for row in cursor.fetchall()]
                recorded = {
                    "phase": PHASE_ROWS_DELETED,
                    "device": device,
                    "captured_at": captured_at.isoformat(),
                    "display_index": display or "all",
                    "deleted": {
                        "screen_timelapse_screenshots": len(screenshots),
                        "screen_timelapse_accessibility_trees": len(trees),
                        "screen_timelapse_descriptions": descriptions,
                        "timeline_events": len(timeline_event_ids),
                        "search_chunks": search_chunks,
                    },
                    "timeline_event_ids": timeline_event_ids,
                    "objects": [*screenshots, *trees],
                    "not_touched": list(NOT_TOUCHED),
                }
                cursor.execute(
                    warehouse._expand_relations(
                        "UPDATE @upstream_mutations SET result_json = %s, updated_at = now() WHERE id = %s"
                    ),
                    (_jsonb_param(recorded), mutation_id),
                )
                cursor.execute("COMMIT")
            except BaseException:
                cursor.execute("ROLLBACK")
                raise
        return recorded

    def _capture_event_id(self, cursor, device: str, captured_at: datetime) -> str:
        """The screen_capture adapter's own event_id expression, not a copy of it."""
        cursor.execute(
            f"SELECT {_SCREEN_CAPTURE_ADAPTER.event_id_expression} "
            "FROM (SELECT %s::text AS device, %s::timestamptz AS captured_at) AS t",
            (device, captured_at),
        )
        return str(cursor.fetchone()[0])

    def _delete_objects(self, recorded: Mapping[str, Any]) -> ScreenTimelapsePurgeResult:
        """Phase 2: delete each stored object and read back that it is gone."""
        objects = [dict(item) for item in recorded.get("objects") or []]
        failures: list[str] = []
        for item in objects:
            if item.get("status") in {"deleted", "absent", "still_referenced"}:
                continue
            if not item.get("storage_file_id"):
                item["status"] = "absent"
                continue
            if self._still_referenced(item["storage_file_id"]):
                # Another row still names this file; deleting it would leave
                # that row pointing at nothing.
                item["status"] = "still_referenced"
                continue
            try:
                store = self._store()
                ref = _storage_ref(item)
                store.delete_object(ref)
                try:
                    store.get_metadata(ref)
                except ObjectNotFoundError:
                    item["status"] = "deleted"
                else:
                    failures.append(f"{item['storage_file_id']}: still present after delete")
            except Exception as exc:  # noqa: BLE001 - recorded and retried
                failures.append(f"{item['storage_file_id']}: {exc}")
        result = {**dict(recorded), "objects": objects}
        if failures:
            result["phase"] = PHASE_ROWS_DELETED
            return ScreenTimelapsePurgeResult(
                status="failed_retryable",
                result_json=result,
                error="rows deleted; stored objects not yet confirmed gone: " + "; ".join(failures)[:1500],
            )
        result["phase"] = PHASE_COMPLETE
        return ScreenTimelapsePurgeResult(status="succeeded", result_json=result)

    def _still_referenced(self, storage_file_id: str) -> bool:
        rows = self._warehouse._query(
            """
            SELECT 1 FROM @screen_timelapse_screenshots WHERE storage_file_id = %s
            UNION ALL
            SELECT 1 FROM @screen_timelapse_accessibility_trees WHERE storage_file_id = %s
            LIMIT 1
            """,
            (storage_file_id, storage_file_id),
        )
        return bool(rows)

    def _store(self):
        if self._object_store is None:
            self._object_store = self._object_store_factory()
        return self._object_store


def _target(payload: Mapping[str, Any]) -> dict[str, Any]:
    device = str(payload.get("device") or "").strip()
    if not device:
        raise ValueError("payload has no device")
    try:
        captured_at = datetime.fromisoformat(str(payload.get("captured_at") or "").replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"payload captured_at is not a timestamp: {payload.get('captured_at')!r}") from exc
    if captured_at.tzinfo is None:
        raise ValueError("payload captured_at has no time zone")
    display = payload.get("display_index")
    display_index = 0 if display in (None, "") else int(display)
    if display_index < 0:
        raise ValueError("payload display_index must be at least 1")
    return {"device": device, "captured_at": captured_at, "display_index": display_index}


def _object_row(row, *, relation: str) -> dict[str, Any]:
    display_index, content_sha256, size_bytes, backend, key, file_id = row
    out = {
        "relation": relation,
        "content_sha256": str(content_sha256 or ""),
        "size_bytes": int(size_bytes or 0),
        "storage_backend": str(backend or ""),
        "storage_key": str(key or ""),
        "storage_file_id": str(file_id or ""),
        "status": "pending",
    }
    if display_index is not None:
        out["display_index"] = int(display_index)
    return out


def _storage_ref(item: Mapping[str, Any]) -> dict[str, str]:
    return {
        "storage_backend": str(item.get("storage_backend") or ""),
        "storage_key": str(item.get("storage_key") or ""),
        "storage_file_id": str(item.get("storage_file_id") or ""),
        "storage_url": "",
    }

