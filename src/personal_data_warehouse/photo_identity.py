"""Photo identity resolution: raw per-source file rows -> deduplicated assets.

The runner scans every registered photo source table (``PHOTO_SOURCE_RELATIONS``)
for rows without a ``photos.asset_files`` link, fingerprints stills, applies the
merge policy, and maintains ``photos.assets`` (canonical fields + thumbnail).
Raw rows are NEVER mutated; deleting the link rows and re-running replays every
decision.

Merge policy (``resolve_merge``), in strict order:

1. exact bytes            -> the existing file's asset
2. same (source, account, native id)  -> same asset (attaches live_video/edited
   roles and makes re-imports free)
3. BURST GUARD: files from the same (source, account) with different native
   ids are excluded from perceptual matching entirely — burst frames are
   near-identical pixels but genuinely different photos, and source-native
   identity outranks pixel similarity within one source.
4. cross-source dhash Hamming <= AUTO_MERGE_MAX_HAMMING  -> merge
5. Hamming <= CORROBORATED_MAX_HAMMING -> merge only with metadata
   corroboration (capture instants within a few seconds, or identical camera
   with the same capture second)
6. otherwise: a new asset. Videos only ever pass rules 1-2.
"""

from __future__ import annotations

import hashlib
import json
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from personal_data_warehouse.file_attachment_enrichment import normalized_model_image
from personal_data_warehouse.photo_fingerprint import HASH_VERSION, compute_dhash, hamming
from personal_data_warehouse.relations import PHOTO_SOURCE_RELATIONS

AUTO_MERGE_MAX_HAMMING = 8
CORROBORATED_MAX_HAMMING = 24
CAPTURE_TIME_TOLERANCE_SECONDS = 5.0

# Canonical-field precedence: earlier sources win (Apple's asset record is
# richer and better-normalized than EXIF or a Takeout sidecar). A future
# source slots itself in here.
PHOTO_SOURCE_PRECEDENCE: tuple[str, ...] = ("apple_photos",)

_ROLE_RANK = {"original": 0, "edited": 1, "live_video": 2}

THUMBNAIL_KEY_PREFIX = "photos/derived/thumbnails/"
THUMBNAIL_KIND = "photo_thumbnail"
THUMBNAIL_CONTENT_TYPE = "image/jpeg"

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass(frozen=True)
class PhotoFileRecord:
    """A raw file row, as seen by the merge policy (candidate or linked)."""

    source: str
    account: str
    source_native_id: str
    role: str
    content_sha256: str
    filename: str = ""
    mime_type: str = ""
    size_bytes: int = 0
    width: int = 0
    height: int = 0
    captured_at: datetime | None = None
    capture_tz_offset: str = ""
    camera_make: str = ""
    camera_model: str = ""
    raw_metadata: Mapping[str, Any] = field(default_factory=dict)
    storage: Mapping[str, str] = field(default_factory=dict)
    photo_id: str = ""  # set once linked

    @property
    def is_image(self) -> bool:
        return self.mime_type.startswith("image/")

    @property
    def is_still(self) -> bool:
        return self.is_image and self.role in ("original", "edited")


@dataclass(frozen=True)
class MergeDecision:
    photo_id: str  # "" -> new asset
    match_method: str  # exact_sha | source_id | perceptual | corroborated | new
    match_score: float  # hamming distance for perceptual paths, else 0


@dataclass(frozen=True)
class PhotoIdentitySummary:
    files_seen: int
    files_linked: int
    assets_created: int
    assets_updated: int
    fingerprints_computed: int
    thumbnails_generated: int
    undecodable_files: int


def resolve_merge(
    candidate: PhotoFileRecord,
    existing: list[PhotoFileRecord],
    dhash_by_sha: Mapping[str, str],
    *,
    auto_threshold: int = AUTO_MERGE_MAX_HAMMING,
    corroborated_threshold: int = CORROBORATED_MAX_HAMMING,
) -> MergeDecision:
    # 1. Exact bytes.
    for record in existing:
        if record.content_sha256 == candidate.content_sha256:
            return MergeDecision(record.photo_id, "exact_sha", 0.0)
    # 2. Source-native identity.
    for record in existing:
        if (
            record.source == candidate.source
            and record.account == candidate.account
            and record.source_native_id == candidate.source_native_id
        ):
            return MergeDecision(record.photo_id, "source_id", 0.0)
    # Videos never merge perceptually.
    if not candidate.is_image:
        return MergeDecision("", "new", 0.0)
    candidate_hash = dhash_by_sha.get(candidate.content_sha256, "")
    if not candidate_hash:
        return MergeDecision("", "new", 0.0)

    best: tuple[int, PhotoFileRecord] | None = None
    for record in existing:
        # 3. Burst guard: same source+account with a different native id is a
        # DIFFERENT photo no matter how similar the pixels.
        if record.source == candidate.source and record.account == candidate.account:
            continue
        record_hash = dhash_by_sha.get(record.content_sha256, "")
        if not record_hash:
            continue
        distance = hamming(candidate_hash, record_hash)
        if best is None or distance < best[0]:
            best = (distance, record)
    if best is None:
        return MergeDecision("", "new", 0.0)

    distance, record = best
    # 4. Unambiguously the same shot.
    if distance <= auto_threshold:
        return MergeDecision(record.photo_id, "perceptual", float(distance))
    # 5. Borderline band: only with metadata corroboration.
    if distance <= corroborated_threshold and _corroborates(candidate, record):
        return MergeDecision(record.photo_id, "corroborated", float(distance))
    return MergeDecision("", "new", 0.0)


def _corroborates(candidate: PhotoFileRecord, record: PhotoFileRecord) -> bool:
    a, b = candidate.captured_at, record.captured_at
    if a is not None and b is not None:
        delta = abs((a - b).total_seconds())
        if delta <= CAPTURE_TIME_TOLERANCE_SECONDS:
            return True
        same_camera = (
            candidate.camera_make
            and candidate.camera_model
            and candidate.camera_make == record.camera_make
            and candidate.camera_model == record.camera_model
        )
        # Identical camera capturing the same wall-clock second: timezone
        # re-interpretation between sources can shift the instant by whole
        # hours while the second-of-minute survives.
        if same_camera and a.replace(microsecond=0) == b.replace(microsecond=0):
            return True
    return False


def stable_photo_id(*, source: str, account: str, source_native_id: str) -> str:
    """Deterministic asset id derived from the founding file's provenance, so
    re-running identity from scratch reproduces the same ids."""
    seed = f"{source}|{account}|{source_native_id}"
    return "ph_" + hashlib.sha256(seed.encode("utf-8")).hexdigest()[:24]


def extract_exif_fields(content: bytes) -> dict[str, Any]:
    """Best-effort EXIF facts for sources without a rich native record
    (manual imports later; Apple rows already carry these in apple_record)."""
    from PIL import ExifTags, Image
    from io import BytesIO

    fields: dict[str, Any] = {}
    try:
        with Image.open(BytesIO(content)) as image:
            exif = image.getexif()
            if not exif:
                return fields
            ifd = exif.get_ifd(ExifTags.IFD.Exif)
            make = str(exif.get(ExifTags.Base.Make, "") or "").strip()
            model = str(exif.get(ExifTags.Base.Model, "") or "").strip()
            taken = str(ifd.get(ExifTags.Base.DateTimeOriginal, "") or "")
            offset = str(ifd.get(ExifTags.Base.OffsetTimeOriginal, "") or "")
            if make:
                fields["camera_make"] = make
            if model:
                fields["camera_model"] = model
            if taken:
                fields["captured_at"] = taken.replace(":", "-", 2).replace(" ", "T")
            if offset:
                fields["capture_tz_offset"] = offset
            gps = exif.get_ifd(ExifTags.IFD.GPSInfo)
            latitude = _gps_coordinate(gps.get(2), gps.get(1))
            longitude = _gps_coordinate(gps.get(4), gps.get(3))
            if latitude is not None and longitude is not None:
                fields["latitude"] = latitude
                fields["longitude"] = longitude
    except Exception:  # noqa: BLE001 - EXIF is best-effort by definition
        return fields
    return fields


def _gps_coordinate(dms, ref) -> float | None:
    if not dms or len(dms) != 3:
        return None
    try:
        degrees = float(dms[0]) + float(dms[1]) / 60 + float(dms[2]) / 3600
    except (TypeError, ValueError, ZeroDivisionError):
        return None
    if ref in ("S", "W"):
        degrees = -degrees
    return degrees


class PhotoIdentityRunner:
    def __init__(
        self,
        *,
        warehouse,
        object_store,
        logger,
        now: Callable[[], datetime] | None = None,
        batch_size: int | None = 500,
        hash_version: str = HASH_VERSION,
    ) -> None:
        self._warehouse = warehouse
        self._object_store = object_store
        self._logger = logger
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._batch_size = batch_size
        self._hash_version = hash_version
        self._content_cache: dict[str, bytes] = {}
        self._undecodable: set[str] = set()

    def sync(self) -> PhotoIdentitySummary:
        self._warehouse.ensure_photos_tables()
        now = self._now()
        sync_version = int(now.timestamp() * 1_000_000)

        unresolved = self._load_unresolved()
        existing = self._load_existing()
        dhash_by_sha = self._load_fingerprints()
        existing_asset_ids = {record.photo_id for record in existing}

        fingerprints_computed = 0
        linked = 0
        touched_ids: list[str] = []
        for candidate in unresolved:
            if candidate.is_image and candidate.content_sha256 not in dhash_by_sha:
                fingerprint = self._fingerprint(candidate)
                if fingerprint is not None:
                    dhash_by_sha[candidate.content_sha256] = fingerprint.dhash
                    self._warehouse.insert_media_fingerprints(
                        [
                            {
                                "content_sha256": candidate.content_sha256,
                                "hash_version": self._hash_version,
                                "dhash": fingerprint.dhash,
                                "width": fingerprint.width,
                                "height": fingerprint.height,
                                "created_at": now,
                                "sync_version": sync_version,
                            }
                        ]
                    )
                    fingerprints_computed += 1

            decision = resolve_merge(candidate, existing, dhash_by_sha)
            photo_id = decision.photo_id or stable_photo_id(
                source=candidate.source,
                account=candidate.account,
                source_native_id=candidate.source_native_id,
            )
            self._warehouse.insert_photo_asset_files(
                [
                    {
                        "source": candidate.source,
                        "account": candidate.account,
                        "source_native_id": candidate.source_native_id,
                        "role": candidate.role,
                        "content_sha256": candidate.content_sha256,
                        "photo_id": photo_id,
                        "match_method": decision.match_method,
                        "match_score": decision.match_score,
                        "created_at": now,
                        "sync_version": sync_version,
                    }
                ]
            )
            linked += 1
            # Later candidates in this batch must see this link (a Live
            # Photo's .mov typically arrives right behind its still).
            existing.append(replace(candidate, photo_id=photo_id))
            if photo_id not in touched_ids:
                touched_ids.append(photo_id)

        assets_created = 0
        assets_updated = 0
        thumbnails_generated = 0
        for photo_id in touched_ids:
            created, thumbnail = self._canonicalize_asset(
                photo_id,
                now=now,
                sync_version=sync_version,
                is_new=photo_id not in existing_asset_ids,
            )
            assets_created += int(created)
            assets_updated += int(not created)
            thumbnails_generated += int(thumbnail)

        summary = PhotoIdentitySummary(
            files_seen=len(unresolved),
            files_linked=linked,
            assets_created=assets_created,
            assets_updated=assets_updated,
            fingerprints_computed=fingerprints_computed,
            thumbnails_generated=thumbnails_generated,
            undecodable_files=len(self._undecodable),
        )
        self._logger.info(
            "Photo identity: linked=%s assets_created=%s assets_updated=%s "
            "fingerprints=%s thumbnails=%s undecodable=%s",
            summary.files_linked,
            summary.assets_created,
            summary.assets_updated,
            summary.fingerprints_computed,
            summary.thumbnails_generated,
            summary.undecodable_files,
        )
        return summary

    # --- loading -------------------------------------------------------------

    def _load_unresolved(self) -> list[PhotoFileRecord]:
        records: list[PhotoFileRecord] = []
        for table in PHOTO_SOURCE_RELATIONS.values():
            limit_sql = "LIMIT %s" if self._batch_size else ""
            params: tuple[Any, ...] = (self._batch_size,) if self._batch_size else ()
            rows = self._warehouse._query_dicts(
                f"""
                SELECT f.source, f.account, f.source_native_id, f.role, f.content_sha256,
                       f.filename, f.mime_type, f.size_bytes, f.width, f.height,
                       f.captured_at, f.capture_tz_offset, f.camera_make, f.camera_model,
                       f.raw_metadata_json, f.storage_backend, f.storage_key,
                       f.storage_file_id, f.storage_url
                FROM {table} f
                WHERE NOT EXISTS (
                    SELECT 1 FROM photo_asset_files l
                    WHERE l.source = f.source AND l.account = f.account
                      AND l.source_native_id = f.source_native_id
                      AND l.content_sha256 = f.content_sha256
                )
                ORDER BY f.ingested_at, f.source_native_id, f.role
                {limit_sql}
                """,
                params,
            )
            records.extend(self._record_from_row(row) for row in rows)
        return records

    def _load_existing(self) -> list[PhotoFileRecord]:
        rows = self._warehouse._query_dicts(
            """
            SELECT l.source, l.account, l.source_native_id, l.role, l.content_sha256,
                   l.photo_id, a.capture_ts AS captured_at, a.camera_make, a.camera_model,
                   a.best_file_mime_type AS mime_type
            FROM photo_asset_files l
            JOIN photo_assets a ON a.photo_id = l.photo_id
            """
        )
        return [
            PhotoFileRecord(
                source=str(row["source"]),
                account=str(row["account"]),
                source_native_id=str(row["source_native_id"]),
                role=str(row["role"]),
                content_sha256=str(row["content_sha256"]),
                mime_type=str(row["mime_type"] or ""),
                captured_at=_real_timestamp(row["captured_at"]),
                camera_make=str(row["camera_make"] or ""),
                camera_model=str(row["camera_model"] or ""),
                photo_id=str(row["photo_id"]),
            )
            for row in rows
        ]

    def _load_fingerprints(self) -> dict[str, str]:
        rows = self._warehouse._query(
            "SELECT content_sha256, dhash FROM media_fingerprints WHERE hash_version = %s",
            (self._hash_version,),
        )
        return {str(sha): str(dhash) for sha, dhash in rows}

    def _record_from_row(self, row: Mapping[str, Any]) -> PhotoFileRecord:
        raw = row.get("raw_metadata_json")
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except ValueError:
                raw = {}
        return PhotoFileRecord(
            source=str(row["source"]),
            account=str(row["account"]),
            source_native_id=str(row["source_native_id"]),
            role=str(row["role"]),
            content_sha256=str(row["content_sha256"]),
            filename=str(row["filename"] or ""),
            mime_type=str(row["mime_type"] or ""),
            size_bytes=int(row["size_bytes"] or 0),
            width=int(row["width"] or 0),
            height=int(row["height"] or 0),
            captured_at=_real_timestamp(row["captured_at"]),
            capture_tz_offset=str(row["capture_tz_offset"] or ""),
            camera_make=str(row["camera_make"] or ""),
            camera_model=str(row["camera_model"] or ""),
            raw_metadata=raw if isinstance(raw, Mapping) else {},
            storage={
                "storage_backend": str(row["storage_backend"] or ""),
                "storage_key": str(row["storage_key"] or ""),
                "storage_file_id": str(row["storage_file_id"] or ""),
                "storage_url": str(row["storage_url"] or ""),
            },
        )

    # --- content + fingerprints ----------------------------------------------

    def _content(self, record: PhotoFileRecord) -> bytes | None:
        sha = record.content_sha256
        if sha in self._content_cache:
            return self._content_cache[sha]
        if self._object_store is None or not record.storage.get("storage_file_id"):
            return None
        content = self._object_store.get_object(dict(record.storage))
        self._content_cache[sha] = content
        return content

    def _fingerprint(self, record: PhotoFileRecord):
        if record.content_sha256 in self._undecodable:
            return None
        content = self._content(record)
        if content is None:
            return None
        try:
            return compute_dhash(content)
        except Exception as exc:  # noqa: BLE001 - undecodable is a classification
            # No fingerprint means no perceptual matching for this file; the
            # sha/native-id rules still apply, so this is never fatal.
            self._undecodable.add(record.content_sha256)
            self._logger.warning(
                "Could not fingerprint %s (%s): %s", record.filename, record.content_sha256, exc
            )
            return None

    # --- canonicalization ------------------------------------------------------

    def _canonicalize_asset(
        self,
        photo_id: str,
        *,
        now: datetime,
        sync_version: int,
        is_new: bool,
    ) -> tuple[bool, bool]:
        files = self._load_asset_files(photo_id)
        if not files:
            return (False, False)
        ranked = sorted(files, key=_canonical_rank)
        primary = next((record for record in ranked if record.is_still), ranked[0])
        latitude, longitude = _canonical_gps(ranked)
        kind = "image" if primary.is_still else "video"

        existing_asset = self._load_asset(photo_id)
        thumbnail = _thumbnail_block(existing_asset)
        generated = False
        if kind == "image" and not thumbnail["thumbnail_content_sha256"]:
            thumbnail_new = self._generate_thumbnail(primary)
            if thumbnail_new is not None:
                thumbnail = thumbnail_new
                generated = True

        created_at = existing_asset["created_at"] if existing_asset else now

        self._warehouse.insert_photo_assets(
            [
                {
                    "photo_id": photo_id,
                    "account": primary.account,
                    "kind": kind,
                    "capture_ts": primary.captured_at or _EPOCH,
                    "capture_tz_offset": primary.capture_tz_offset,
                    "latitude": latitude,
                    "longitude": longitude,
                    "camera_make": primary.camera_make,
                    "camera_model": primary.camera_model,
                    "width": primary.width,
                    "height": primary.height,
                    "best_file_sha256": primary.content_sha256,
                    "best_file_mime_type": primary.mime_type,
                    "best_file_filename": primary.filename,
                    "best_file_size_bytes": primary.size_bytes,
                    **thumbnail,
                    "created_at": created_at,
                    "updated_at": now,
                    "sync_version": sync_version,
                }
            ]
        )
        return (is_new, generated)

    def _load_asset_files(self, photo_id: str) -> list[PhotoFileRecord]:
        records: list[PhotoFileRecord] = []
        for table in PHOTO_SOURCE_RELATIONS.values():
            rows = self._warehouse._query_dicts(
                f"""
                SELECT f.source, f.account, f.source_native_id, f.role, f.content_sha256,
                       f.filename, f.mime_type, f.size_bytes, f.width, f.height,
                       f.captured_at, f.capture_tz_offset, f.camera_make, f.camera_model,
                       f.raw_metadata_json, f.storage_backend, f.storage_key,
                       f.storage_file_id, f.storage_url
                FROM photo_asset_files l
                JOIN {table} f
                  ON f.source = l.source AND f.account = l.account
                 AND f.source_native_id = l.source_native_id
                 AND f.content_sha256 = l.content_sha256
                WHERE l.photo_id = %s
                """,
                (photo_id,),
            )
            records.extend(self._record_from_row(row) for row in rows)
        return records

    def _load_asset(self, photo_id: str) -> dict[str, Any] | None:
        rows = self._warehouse._query_dicts(
            "SELECT * FROM photo_assets WHERE photo_id = %s", (photo_id,)
        )
        return rows[0] if rows else None

    def _generate_thumbnail(self, primary: PhotoFileRecord) -> dict[str, Any] | None:
        if primary.content_sha256 in self._undecodable:
            return None
        content = self._content(primary)
        if content is None or self._object_store is None:
            return None
        try:
            rendered = normalized_model_image(content)
        except Exception as exc:  # noqa: BLE001 - undecodable is a classification
            self._undecodable.add(primary.content_sha256)
            self._logger.warning("Could not render thumbnail for %s: %s", primary.filename, exc)
            return None
        sha = hashlib.sha256(rendered).hexdigest()
        with tempfile.NamedTemporaryFile(prefix="pdw-photo-thumb-", suffix=".jpg") as handle:
            path = Path(handle.name)
            path.write_bytes(rendered)
            stored = self._object_store.put_file(
                path=path,
                object_key=f"{THUMBNAIL_KEY_PREFIX}{sha}.jpg",
                content_sha256=sha,
                content_type=THUMBNAIL_CONTENT_TYPE,
                kind=THUMBNAIL_KIND,
            )
        return {
            "thumbnail_content_sha256": sha,
            "thumbnail_content_type": THUMBNAIL_CONTENT_TYPE,
            "thumbnail_size_bytes": len(rendered),
            "thumbnail_storage_backend": str(stored.get("storage_backend", "")),
            "thumbnail_storage_key": str(stored.get("storage_key", "")),
            "thumbnail_storage_file_id": str(stored.get("storage_file_id", "")),
            "thumbnail_storage_url": str(stored.get("storage_url", "")),
        }


def has_unresolved_photo_files(warehouse) -> bool:
    for table in PHOTO_SOURCE_RELATIONS.values():
        rows = warehouse._query(
            f"""
            SELECT 1 FROM {table} f
            WHERE NOT EXISTS (
                SELECT 1 FROM photo_asset_files l
                WHERE l.source = f.source AND l.account = f.account
                  AND l.source_native_id = f.source_native_id
                  AND l.content_sha256 = f.content_sha256
            )
            LIMIT 1
            """
        )
        if rows:
            return True
    return False


def _canonical_rank(record: PhotoFileRecord) -> tuple[int, int, int]:
    try:
        source_rank = PHOTO_SOURCE_PRECEDENCE.index(record.source)
    except ValueError:
        source_rank = len(PHOTO_SOURCE_PRECEDENCE)
    role_rank = _ROLE_RANK.get(record.role, len(_ROLE_RANK))
    return (source_rank, role_rank, -(record.width * record.height))


def _canonical_gps(ranked: list[PhotoFileRecord]) -> tuple[float, float]:
    """First source-record block carrying GPS, in canonical rank order. The
    record blocks are source-named (apple_record, takeout_sidecar, ...), so
    scan mappings rather than hardcoding source knowledge here."""
    for record in ranked:
        for value in record.raw_metadata.values():
            if isinstance(value, Mapping) and "latitude" in value and "longitude" in value:
                try:
                    return (float(value["latitude"]), float(value["longitude"]))
                except (TypeError, ValueError):
                    continue
    return (0.0, 0.0)


def _thumbnail_block(asset: Mapping[str, Any] | None) -> dict[str, Any]:
    if asset is None:
        asset = {}
    return {
        "thumbnail_content_sha256": str(asset.get("thumbnail_content_sha256", "") or ""),
        "thumbnail_content_type": str(asset.get("thumbnail_content_type", "") or ""),
        "thumbnail_size_bytes": int(asset.get("thumbnail_size_bytes", 0) or 0),
        "thumbnail_storage_backend": str(asset.get("thumbnail_storage_backend", "") or ""),
        "thumbnail_storage_key": str(asset.get("thumbnail_storage_key", "") or ""),
        "thumbnail_storage_file_id": str(asset.get("thumbnail_storage_file_id", "") or ""),
        "thumbnail_storage_url": str(asset.get("thumbnail_storage_url", "") or ""),
    }


def _real_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    if value <= _EPOCH:
        return None
    return value
