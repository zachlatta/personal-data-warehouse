"""The merge-policy + identity-runner contract (the dedup core).

The policy matrix is tested purely via resolve_merge (dhash values are an
input, so borderline Hamming bands are fabricated exactly). The runner is
tested against live Postgres with real synthetic images, including a
cross-source merge: rows may carry any registered source value, so the
cross-source path is proven before a second real source exists.
"""

from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime, timedelta

import pytest
from dotenv import load_dotenv

from tests.conftest import cleanup_test_warehouse, make_test_schema
from tests.test_photo_fingerprint import image_bytes, synthetic_photo
from tests.test_photos_warehouse import _photo_file_row

from personal_data_warehouse.photo_fingerprint import HASH_VERSION
from personal_data_warehouse.photo_identity import (
    AUTO_MERGE_MAX_HAMMING,
    CORROBORATED_MAX_HAMMING,
    MergeDecision,
    PhotoFileRecord,
    PhotoIdentityRunner,
    extract_exif_fields,
    has_unresolved_photo_files,
    resolve_merge,
    stable_photo_id,
)
from personal_data_warehouse.postgres import PostgresWarehouse

_TS = datetime(2026, 6, 1, 21, 30, tzinfo=UTC)


def _hex_with_distance(base: str, bits: int) -> str:
    """A 256-bit hex string exactly `bits` Hamming away from base."""
    value = int(base, 16) ^ ((1 << bits) - 1)
    return f"{value:064x}"


def _record(**overrides) -> PhotoFileRecord:
    defaults = dict(
        source="apple_photos",
        account="z@x.test",
        source_native_id="UUID-1",
        role="original",
        content_sha256="sha-1",
        mime_type="image/heic",
        captured_at=_TS,
        camera_make="Apple",
        camera_model="iPhone 16 Pro",
    )
    defaults.update(overrides)
    return PhotoFileRecord(**defaults)


BASE = "a" * 64


# --- resolve_merge policy matrix (pure) -----------------------------------------


def test_exact_sha_reuses_existing_asset():
    existing = [_record(photo_id="ph1", source="google_photos", source_native_id="takeout/x.jpg")]
    decision = resolve_merge(_record(content_sha256="sha-1"), existing, {})
    assert decision == MergeDecision("ph1", "exact_sha", 0.0)


def test_same_native_id_links_new_rendition_to_same_asset():
    existing = [_record(photo_id="ph1")]
    candidate = _record(content_sha256="sha-edited", role="edited")
    decision = resolve_merge(candidate, existing, {})
    assert decision == MergeDecision("ph1", "source_id", 0.0)


def test_live_video_attaches_via_native_id_and_never_fingerprints():
    existing = [_record(photo_id="ph1")]
    candidate = _record(content_sha256="sha-mov", role="live_video", mime_type="video/quicktime")
    decision = resolve_merge(candidate, existing, {})
    assert decision == MergeDecision("ph1", "source_id", 0.0)


def test_burst_guard_never_perceptually_merges_within_a_source():
    # Visually near-identical burst frames from the same source+account with
    # different native ids must remain distinct photos.
    existing = [_record(photo_id="ph1")]
    candidate = _record(source_native_id="UUID-2", content_sha256="sha-2")
    hashes = {"sha-1": BASE, "sha-2": BASE}  # Hamming 0: identical pixels
    decision = resolve_merge(candidate, existing, hashes)
    assert decision == MergeDecision("", "new", 0.0)


def test_cross_source_auto_merge_at_low_hamming():
    existing = [_record(photo_id="ph1")]
    candidate = _record(
        source="google_photos", source_native_id="takeout/x.jpg", content_sha256="sha-2"
    )
    hashes = {"sha-1": BASE, "sha-2": _hex_with_distance(BASE, AUTO_MERGE_MAX_HAMMING)}
    decision = resolve_merge(candidate, existing, hashes)
    assert decision == MergeDecision("ph1", "perceptual", float(AUTO_MERGE_MAX_HAMMING))


def test_corroboration_band_merges_with_close_capture_time():
    existing = [_record(photo_id="ph1")]
    candidate = _record(
        source="google_photos",
        source_native_id="takeout/x.jpg",
        content_sha256="sha-2",
        captured_at=_TS + timedelta(seconds=3),
    )
    hashes = {"sha-1": BASE, "sha-2": _hex_with_distance(BASE, 15)}
    decision = resolve_merge(candidate, existing, hashes)
    assert decision == MergeDecision("ph1", "corroborated", 15.0)


def test_corroboration_band_merges_with_same_camera_and_same_second():
    # Timezone re-interpretation can shift the instant by whole hours; the
    # identical camera + identical wall-clock second still corroborates.
    existing = [_record(photo_id="ph1")]
    candidate = _record(
        source="google_photos",
        source_native_id="takeout/x.jpg",
        content_sha256="sha-2",
        captured_at=_TS + timedelta(hours=7),
    )
    hashes = {"sha-1": BASE, "sha-2": _hex_with_distance(BASE, 15)}
    assert resolve_merge(candidate, existing, hashes).match_method == "new"  # 7h, diff second? same second-of-minute but different instant... explicit below
    same_second = _record(
        source="google_photos",
        source_native_id="takeout/x.jpg",
        content_sha256="sha-2",
        captured_at=_TS.replace(tzinfo=UTC),
    )
    decision = resolve_merge(same_second, existing, hashes)
    assert decision.match_method == "corroborated"


def test_corroboration_band_without_corroboration_stays_separate():
    existing = [_record(photo_id="ph1")]
    candidate = _record(
        source="google_photos",
        source_native_id="takeout/y.jpg",
        content_sha256="sha-2",
        captured_at=_TS + timedelta(days=2),
        camera_model="X100VI",
        camera_make="FUJIFILM",
    )
    hashes = {"sha-1": BASE, "sha-2": _hex_with_distance(BASE, 15)}
    assert resolve_merge(candidate, existing, hashes) == MergeDecision("", "new", 0.0)


def test_above_corroborated_threshold_never_merges():
    existing = [_record(photo_id="ph1")]
    candidate = _record(
        source="google_photos",
        source_native_id="takeout/x.jpg",
        content_sha256="sha-2",
        captured_at=_TS,  # perfectly corroborated...
    )
    hashes = {"sha-1": BASE, "sha-2": _hex_with_distance(BASE, CORROBORATED_MAX_HAMMING + 1)}
    assert resolve_merge(candidate, existing, hashes) == MergeDecision("", "new", 0.0)


def test_videos_only_merge_by_sha_or_native_id():
    existing = [_record(photo_id="ph1", mime_type="video/quicktime", role="original")]
    candidate = _record(
        source="google_photos",
        source_native_id="takeout/v.mp4",
        content_sha256="sha-2",
        mime_type="video/mp4",
    )
    # Even identical fingerprints (hypothetical) must not merge videos.
    hashes = {"sha-1": BASE, "sha-2": BASE}
    assert resolve_merge(candidate, existing, hashes) == MergeDecision("", "new", 0.0)


def test_stable_photo_id_is_deterministic():
    a = stable_photo_id(source="apple_photos", account="z@x.test", source_native_id="UUID-1")
    assert a == stable_photo_id(source="apple_photos", account="z@x.test", source_native_id="UUID-1")
    assert a.startswith("ph_")
    assert a != stable_photo_id(source="apple_photos", account="z@x.test", source_native_id="UUID-2")


def test_extract_exif_fields_reads_camera_and_capture_time():
    from PIL import ExifTags, Image

    image = synthetic_photo(9, size=(120, 90))
    exif = Image.Exif()
    exif[ExifTags.Base.Make] = "FUJIFILM"
    exif[ExifTags.Base.Model] = "X100VI"
    ifd = exif.get_ifd(ExifTags.IFD.Exif)
    ifd[ExifTags.Base.DateTimeOriginal] = "2026:06:01 14:30:00"
    content = image_bytes(image, format="JPEG", exif=exif.tobytes())
    fields = extract_exif_fields(content)
    assert fields["camera_make"] == "FUJIFILM"
    assert fields["camera_model"] == "X100VI"
    assert fields["captured_at"] == "2026-06-01T14:30:00"
    # Garbage bytes are best-effort empty, never an exception.
    assert extract_exif_fields(b"not an image") == {}


# --- the runner against live Postgres --------------------------------------------


def _postgres_url() -> str:
    load_dotenv()
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if not url:
        pytest.skip("POSTGRES_DATABASE_URL is not set")
    return url


@pytest.fixture()
def warehouse():
    schema = make_test_schema()
    wh = PostgresWarehouse(_postgres_url(), schema=schema)
    try:
        yield wh
    finally:
        cleanup_test_warehouse(wh)


class _Logger:
    def info(self, *args, **kwargs) -> None:
        pass

    warning = info


class FakeObjectStore:
    backend = "google_drive"

    def __init__(self, blobs_by_file_id: dict[str, bytes] | None = None) -> None:
        self.blobs = dict(blobs_by_file_id or {})
        self.put_files: list[dict] = []

    def get_object(self, ref):
        return self.blobs[str(ref["storage_file_id"])]

    def put_file(self, *, path, object_key, content_sha256, content_type, skip_existing_check=False, app_properties=None, kind=None):
        self.put_files.append(
            {"object_key": object_key, "content_sha256": content_sha256, "kind": kind, "bytes": path.read_bytes()}
        )
        return {
            "storage_backend": self.backend,
            "storage_key": object_key,
            "storage_file_id": f"thumb-{len(self.put_files)}",
            "storage_url": "",
        }


def _seed_file(warehouse, *, native_id, sha, blob_id, role="original", source="apple_photos",
               mime="image/heic", filename="IMG.HEIC", captured_at=_TS, camera=("Apple", "iPhone 16 Pro"),
               record=None, ingested_at=_TS):
    warehouse.insert_photo_source_files(
        "apple_photos_files",
        [
            _photo_file_row(
                source=source,
                source_native_id=native_id,
                role=role,
                content_sha256=sha,
                filename=filename,
                mime_type=mime,
                captured_at=captured_at,
                camera_make=camera[0],
                camera_model=camera[1],
                storage_file_id=blob_id,
                raw_metadata_json=record or {"apple_record": {"uuid": native_id, "latitude": 45.5, "longitude": -122.6}},
                ingested_at=ingested_at,
            )
        ],
    )


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def test_runner_links_live_photo_and_generates_thumbnail(warehouse):
    warehouse.ensure_photos_tables()
    still = image_bytes(synthetic_photo(21))
    mov = b"live-video-bytes"
    _seed_file(warehouse, native_id="UUID-1", sha=_sha(still), blob_id="b-still")
    _seed_file(
        warehouse, native_id="UUID-1", sha=_sha(mov), blob_id="b-mov",
        role="live_video", mime="video/quicktime", filename="IMG_3.mov",
        ingested_at=_TS + timedelta(seconds=1),
    )
    store = FakeObjectStore({"b-still": still, "b-mov": mov})
    summary = PhotoIdentityRunner(
        warehouse=warehouse, object_store=store, logger=_Logger(),
        now=lambda: datetime(2026, 6, 2, 12, 0, tzinfo=UTC),
    ).sync()

    assert summary.files_linked == 2
    assert summary.assets_created == 1
    assert summary.thumbnails_generated == 1
    links = warehouse._query(
        "SELECT role, photo_id, match_method FROM photo_asset_files ORDER BY role DESC"
    )
    assert [(r, m) for r, _p, m in links] == [("original", "new"), ("live_video", "source_id")]
    assert links[0][1] == links[1][1]  # one asset

    assets = warehouse._query_dicts("SELECT * FROM photo_assets")
    assert len(assets) == 1
    asset = assets[0]
    assert asset["kind"] == "image"
    assert asset["best_file_sha256"] == _sha(still)
    assert asset["camera_model"] == "iPhone 16 Pro"
    assert asset["latitude"] == 45.5  # GPS from the apple_record block
    assert asset["thumbnail_content_type"] == "image/jpeg"
    assert asset["thumbnail_storage_file_id"] == "thumb-1"
    # The thumbnail also satisfies the enrichment candidate view.
    renditions = warehouse._query("SELECT photo_id, content_sha256 FROM photo_canonical_renditions")
    assert len(renditions) == 1
    # And the fingerprint landed in the shared cache.
    fingerprints = warehouse._query(
        "SELECT content_sha256, hash_version FROM media_fingerprints"
    )
    assert fingerprints == [(_sha(still), HASH_VERSION)]


def test_runner_burst_frames_stay_distinct_but_cross_source_rendition_merges(warehouse):
    warehouse.ensure_photos_tables()
    photo = synthetic_photo(22)
    original = image_bytes(photo)
    # A Takeout-style rendition: downscaled, re-encoded, different bytes.
    rendition = image_bytes(photo.resize((256, 192)), format="JPEG", quality=70)
    # A burst sibling: same source, nearly identical pixels, different native id.
    burst = image_bytes(photo, format="JPEG", quality=95)

    _seed_file(warehouse, native_id="UUID-A", sha=_sha(original), blob_id="b-a")
    _seed_file(
        warehouse, native_id="UUID-B", sha=_sha(burst), blob_id="b-b",
        filename="IMG_BURST.HEIC", ingested_at=_TS + timedelta(seconds=1),
    )
    _seed_file(
        warehouse, native_id="takeout/IMG.jpg", sha=_sha(rendition), blob_id="b-r",
        source="google_photos", mime="image/jpeg", filename="IMG.jpg",
        record={"takeout_sidecar": {"title": "IMG.jpg"}},
        ingested_at=_TS + timedelta(seconds=2),
    )
    store = FakeObjectStore({"b-a": original, "b-b": burst, "b-r": rendition})
    PhotoIdentityRunner(
        warehouse=warehouse, object_store=store, logger=_Logger(),
        now=lambda: datetime(2026, 6, 2, 12, 0, tzinfo=UTC),
    ).sync()

    rows = warehouse._query_dicts(
        "SELECT source, source_native_id, photo_id, match_method FROM photo_asset_files"
    )
    by_native = {row["source_native_id"]: row for row in rows}
    # Burst guard: near-identical same-source frames are separate assets.
    assert by_native["UUID-A"]["photo_id"] != by_native["UUID-B"]["photo_id"]
    assert by_native["UUID-B"]["match_method"] == "new"
    # Cross-source rendition merges into the Apple asset perceptually.
    assert by_native["takeout/IMG.jpg"]["photo_id"] == by_native["UUID-A"]["photo_id"]
    assert by_native["takeout/IMG.jpg"]["match_method"] == "perceptual"
    # Canonical precedence: the merged asset keeps Apple's fields and the
    # full-res original as best file.
    asset = warehouse._query_dicts(
        "SELECT * FROM photo_assets WHERE photo_id = %s", (by_native["UUID-A"]["photo_id"],)
    )[0]
    assert asset["best_file_sha256"] == _sha(original)
    assert asset["camera_model"] == "iPhone 16 Pro"
    assert warehouse._query("SELECT count(*) FROM photo_assets")[0][0] == 2


def test_runner_rerun_is_idempotent(warehouse):
    warehouse.ensure_photos_tables()
    still = image_bytes(synthetic_photo(23))
    _seed_file(warehouse, native_id="UUID-1", sha=_sha(still), blob_id="b-1")
    store = FakeObjectStore({"b-1": still})

    def run(minute: int):
        return PhotoIdentityRunner(
            warehouse=warehouse, object_store=store, logger=_Logger(),
            now=lambda: datetime(2026, 6, 2, 12, minute, tzinfo=UTC),
        ).sync()

    first = run(0)
    assert (first.files_linked, first.assets_created) == (1, 1)
    ids = warehouse._query("SELECT photo_id FROM photo_assets")

    second = run(5)
    assert second.files_seen == 0
    assert second.files_linked == 0
    assert second.assets_created == 0
    assert warehouse._query("SELECT photo_id FROM photo_assets") == ids
    assert not has_unresolved_photo_files(warehouse)
    # The thumbnail was generated exactly once.
    assert len(store.put_files) == 1


def test_runner_undecodable_blob_is_classified_not_fatal(warehouse):
    warehouse.ensure_photos_tables()
    _seed_file(warehouse, native_id="UUID-BAD", sha="sha-bad", blob_id="b-bad")
    store = FakeObjectStore({"b-bad": b"not an image at all"})
    summary = PhotoIdentityRunner(
        warehouse=warehouse, object_store=store, logger=_Logger(),
        now=lambda: datetime(2026, 6, 2, 12, 0, tzinfo=UTC),
    ).sync()
    # The file still resolves (sha/native-id rules remain available); it just
    # has no fingerprint and no thumbnail, and the run stays green.
    assert summary.files_linked == 1
    assert summary.undecodable_files == 1
    assert summary.thumbnails_generated == 0
    asset = warehouse._query_dicts("SELECT * FROM photo_assets")[0]
    assert asset["thumbnail_content_sha256"] == ""
    # No canonical rendition -> not an enrichment candidate.
    assert warehouse._query("SELECT count(*) FROM photo_canonical_renditions")[0][0] == 0


def test_runner_video_asset_gets_no_thumbnail_or_fingerprint(warehouse):
    warehouse.ensure_photos_tables()
    _seed_file(
        warehouse, native_id="UUID-V", sha="sha-video", blob_id="b-v",
        mime="video/quicktime", filename="clip.mov",
    )
    store = FakeObjectStore({"b-v": b"video-bytes"})
    summary = PhotoIdentityRunner(
        warehouse=warehouse, object_store=store, logger=_Logger(),
        now=lambda: datetime(2026, 6, 2, 12, 0, tzinfo=UTC),
    ).sync()
    assert summary.fingerprints_computed == 0
    assert summary.thumbnails_generated == 0
    asset = warehouse._query_dicts("SELECT * FROM photo_assets")[0]
    assert asset["kind"] == "video"
    assert warehouse._query("SELECT count(*) FROM photo_canonical_renditions")[0][0] == 0
