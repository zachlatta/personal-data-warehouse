"""Apple Photos scanner contract: fixture Photos.sqlite -> file candidates."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from personal_data_warehouse_photos.scanner import (
    COCOA_EPOCH_UNIX_OFFSET,
    scan_photo_file_candidates,
    snapshot_photos_store,
)

# 2026-06-01T21:30:00Z in Cocoa seconds.
_CAPTURE_COCOA = 1780349400 - COCOA_EPOCH_UNIX_OFFSET


def _build_fixture_library(root: Path) -> Path:
    """A minimal .photoslibrary with the schema subset the scanner reads."""
    library = root / "Fixture.photoslibrary"
    (library / "database").mkdir(parents=True)
    db = library / "database" / "Photos.sqlite"
    connection = sqlite3.connect(db)
    connection.executescript(
        """
        CREATE TABLE ZASSET (
            Z_PK INTEGER PRIMARY KEY,
            ZUUID TEXT, ZDIRECTORY TEXT, ZFILENAME TEXT,
            ZKIND INTEGER, ZKINDSUBTYPE INTEGER, ZUNIFORMTYPEIDENTIFIER TEXT,
            ZDATECREATED REAL, ZADDEDDATE REAL, ZMODIFICATIONDATE REAL,
            ZWIDTH INTEGER, ZHEIGHT INTEGER,
            ZLATITUDE REAL, ZLONGITUDE REAL,
            ZFAVORITE INTEGER, ZHIDDEN INTEGER, ZADJUSTMENTSSTATE INTEGER,
            ZTRASHEDSTATE INTEGER
        );
        CREATE TABLE ZADDITIONALASSETATTRIBUTES (
            Z_PK INTEGER PRIMARY KEY, ZASSET INTEGER,
            ZORIGINALFILENAME TEXT, ZORIGINALFILESIZE INTEGER,
            ZTIMEZONEOFFSET INTEGER, ZINFERREDTIMEZONEOFFSET INTEGER,
            ZTIMEZONENAME TEXT, ZEXIFTIMESTAMPSTRING TEXT
        );
        CREATE TABLE ZEXTENDEDATTRIBUTES (
            Z_PK INTEGER PRIMARY KEY, ZASSET INTEGER,
            ZCAMERAMAKE TEXT, ZCAMERAMODEL TEXT, ZLENSMODEL TEXT
        );
        """
    )
    rows = [
        # Live photo still (kind 0, subtype 2) with GPS + camera.
        (1, "UUID-LIVE", "A", "UUID-LIVE.heic", 0, 2, "public.heic",
         _CAPTURE_COCOA, _CAPTURE_COCOA + 10, _CAPTURE_COCOA + 10,
         4284, 5712, 45.5, -122.6, 0, 0, 0, 0),
        # Plain photo whose original is cloud-only.
        (2, "UUID-MISSING", "A", "UUID-MISSING.heic", 0, 0, "public.heic",
         _CAPTURE_COCOA - 100, _CAPTURE_COCOA, _CAPTURE_COCOA,
         4032, 3024, -180.0, -180.0, 0, 0, 0, 0),
        # Video asset.
        (3, "UUID-VIDEO", "A", "UUID-VIDEO.mov", 1, 0, "com.apple.quicktime-movie",
         _CAPTURE_COCOA - 200, _CAPTURE_COCOA, _CAPTURE_COCOA,
         1920, 1080, -180.0, -180.0, 0, 0, 0, 0),
        # Trashed asset: must never appear.
        (4, "UUID-TRASHED", "A", "UUID-TRASHED.heic", 0, 0, "public.heic",
         _CAPTURE_COCOA - 300, _CAPTURE_COCOA, _CAPTURE_COCOA,
         100, 100, -180.0, -180.0, 0, 0, 0, 1),
    ]
    connection.executemany("INSERT INTO ZASSET VALUES (" + ",".join("?" * 18) + ")", rows)
    connection.executemany(
        "INSERT INTO ZADDITIONALASSETATTRIBUTES VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 1, "IMG_0001.HEIC", 2400000, -25200, 0, "America/Los_Angeles", "2026:06:01 14:30:00"),
            (2, 2, "IMG_0002.HEIC", 2000000, 0, 7200, "", ""),
            (3, 3, "IMG_0003.MOV", 900000, 0, 0, "", ""),
        ],
    )
    connection.executemany(
        "INSERT INTO ZEXTENDEDATTRIBUTES VALUES (?, ?, ?, ?, ?)",
        [(1, 1, "Apple", "iPhone 16 Pro", "wide lens")],
    )
    connection.commit()
    connection.close()

    return library


def _scan(tmp_path: Path):
    library = _build_fixture_library(tmp_path)
    snapshot = snapshot_photos_store(library, tmp_path / "snap")
    return scan_photo_file_candidates(snapshot)


def test_scanner_excludes_trashed_and_includes_icloud_only_assets(tmp_path):
    (tmp_path / "snap").mkdir()
    candidates = _scan(tmp_path)
    by_id = {(c.native_id, c.role): c for c in candidates}
    assert ("UUID-TRASHED", "original") not in by_id
    cloud_only = by_id[("UUID-MISSING", "original")]
    assert cloud_only.filename == "IMG_0002.HEIC"
    assert cloud_only.expected_size_bytes == 2_000_000
    # Local disk presence is deliberately not part of the scanner contract.
    assert not hasattr(cloud_only, "present")
    assert not hasattr(cloud_only, "path")


def test_scanner_emits_live_video_sibling_under_the_stills_native_id(tmp_path):
    (tmp_path / "snap").mkdir()
    candidates = _scan(tmp_path)
    by_id = {(c.native_id, c.role): c for c in candidates}
    still = by_id[("UUID-LIVE", "original")]
    live = by_id[("UUID-LIVE", "live_video")]
    assert still.mime_type == "image/heic"
    assert live.mime_type == "video/quicktime"
    assert live.filename == "IMG_0001.MOV"
    # Same native id is the contract that attaches the .mov to the still.
    assert live.native_id == still.native_id
    # The non-live, non-video assets get no sibling.
    assert ("UUID-MISSING", "live_video") not in by_id
    assert ("UUID-VIDEO", "live_video") not in by_id


def test_scanner_resolves_wall_clock_capture_and_camera(tmp_path):
    (tmp_path / "snap").mkdir()
    candidates = _scan(tmp_path)
    still = next(c for c in candidates if c.native_id == "UUID-LIVE" and c.role == "original")
    # 21:30Z at -07:00 is 14:30 local wall-clock.
    assert still.captured_at == "2026-06-01T14:30:00"
    assert still.capture_tz_offset == "-07:00"
    assert still.camera_make == "Apple"
    assert still.camera_model == "iPhone 16 Pro"
    assert still.filename == "IMG_0001.HEIC"  # original filename preferred
    assert still.apple_record["latitude"] == 45.5
    assert still.apple_record["uuid"] == "UUID-LIVE"
    # GPS sentinel (-180) is omitted from the record entirely.
    missing = next(c for c in candidates if c.native_id == "UUID-MISSING")
    assert "latitude" not in missing.apple_record
    # Inferred tz offset is the fallback when the explicit one is absent... but
    # an explicit 0 wins (UTC is a real answer).
    assert missing.capture_tz_offset == "+00:00"


def test_snapshot_fails_fast_when_open_blocks(tmp_path):
    # macOS TCC can BLOCK open(2) on Photos-library files indefinitely for a
    # launchd process without Full Disk Access; a hung run holds the uploader
    # lock and looks healthy. A FIFO reproduces the blocking-open behavior:
    # O_RDONLY on a writerless FIFO parks in the kernel exactly like the TCC
    # stall, and the probe must convert it into a loud PermissionError.
    import pytest

    from personal_data_warehouse_photos.scanner import _probe_openable

    fifo = tmp_path / "blocking-open"
    os.mkfifo(fifo)
    with pytest.raises(PermissionError, match="blocked for"):
        _probe_openable(fifo, timeout_seconds=0.5)


def test_snapshot_probe_errors_convert_to_permission_error(tmp_path):
    import pytest

    from personal_data_warehouse_photos.scanner import _probe_openable

    # A readable file passes silently.
    readable = tmp_path / "ok.sqlite"
    readable.write_bytes(b"x")
    _probe_openable(readable, timeout_seconds=2)
    # A missing/denied file raises the same guidance the other uploaders give.
    with pytest.raises(PermissionError, match="Full Disk Access"):
        _probe_openable(tmp_path / "missing.sqlite", timeout_seconds=2)


def test_scanner_orders_newest_first_and_fingerprints_photos_metadata(tmp_path):
    (tmp_path / "snap").mkdir()
    candidates = _scan(tmp_path)
    originals = [c for c in candidates if c.role == "original"]
    assert [c.native_id for c in originals] == ["UUID-LIVE", "UUID-MISSING", "UUID-VIDEO"]
    still = originals[0]
    assert still.fingerprint.startswith("photokit-v2|")
    assert len(still.fingerprint) == len("photokit-v2|") + 64
    assert still.state_id == "UUID-LIVE|original"
