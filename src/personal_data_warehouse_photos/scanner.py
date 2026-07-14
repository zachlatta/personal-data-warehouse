"""Apple Photos library scanner.

Reads a snapshot of the library's ``Photos.sqlite`` (never the live file: WAL
churn from Photos.app would tear reads) and resolves each non-trashed asset to
the file(s) to upload: the original, plus a Live Photo's ``<uuid>_3.mov``
motion sibling. Originals in an "Optimize Mac Storage" library are routinely
cloud-only placeholders (~60% of Zach's library at design time), so a missing
file is a first-class ``present=False`` classification the sync layer defers
and re-checks every run — never an error.
"""

from __future__ import annotations

import os
import sqlite3
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

# Core Data stores timestamps as seconds since 2001-01-01T00:00:00Z.
COCOA_EPOCH_UNIX_OFFSET = 978307200

# Photos stores "no GPS" as -180.0 in both latitude and longitude.
GPS_UNSET_SENTINEL = -180.0

# ZASSET.ZKINDSUBTYPE for a Live Photo still.
KIND_SUBTYPE_LIVE_PHOTO = 2

_UTI_MIME_TYPES = {
    "public.heic": "image/heic",
    "public.heif": "image/heif",
    "public.jpeg": "image/jpeg",
    "public.png": "image/png",
    "public.tiff": "image/tiff",
    "org.webmproject.webp": "image/webp",
    "com.compuserve.gif": "image/gif",
    "com.adobe.raw-image": "image/x-adobe-dng",
    "com.apple.quicktime-movie": "video/quicktime",
    "public.mpeg-4": "video/mp4",
}

_EXTENSION_MIME_TYPES = {
    ".heic": "image/heic",
    ".heif": "image/heif",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".tif": "image/tiff",
    ".tiff": "image/tiff",
    ".webp": "image/webp",
    ".gif": "image/gif",
    ".dng": "image/x-adobe-dng",
    ".mov": "video/quicktime",
    ".mp4": "video/mp4",
}


class ApplePhotosSchemaError(RuntimeError):
    pass


PROBE_TIMEOUT_SECONDS = 15.0


def _probe_openable(path: Path, timeout_seconds: float = PROBE_TIMEOUT_SECONDS) -> None:
    """Fail fast when opening the Photos library would hang.

    Unlike ~/Library/Messages (where a launchd process without Full Disk
    Access gets an immediate EPERM), open(2) on the Photos-library files can
    BLOCK indefinitely inside macOS TCC. A hung run holds the uploader lock
    forever, never writes a heartbeat exit code, and reads as healthy — the
    exact silent-failure mode the heartbeat work exists to prevent. Probe the
    open in a daemon thread and convert a stall into the same loud
    PermissionError the other uploaders raise.
    """
    outcome: list[BaseException | None] = []

    def attempt() -> None:
        try:
            handle = os.open(path, os.O_RDONLY)
            os.close(handle)
            outcome.append(None)
        except BaseException as exc:  # noqa: BLE001 - re-raised below with guidance
            outcome.append(exc)

    thread = threading.Thread(target=attempt, name="photos-open-probe", daemon=True)
    thread.start()
    thread.join(timeout_seconds)
    if not outcome:
        raise PermissionError(
            f"Opening {path} blocked for {timeout_seconds:.0f}s — macOS TCC is stalling this "
            "process. Grant Full Disk Access to the launching executable chain "
            "(see the Apple Photos section of AGENTS.md) and retry."
        )
    if isinstance(outcome[0], BaseException):
        raise PermissionError(
            f"Could not open Apple Photos store at {path}. Grant Full Disk Access to the "
            "launching executable chain and retry."
        ) from outcome[0]


@dataclass(frozen=True)
class PhotoFileCandidate:
    """One uploadable file claim: an asset's original or its live-video sibling."""

    native_id: str  # the still asset's ZUUID for BOTH roles (this attaches the .mov)
    role: str  # original | live_video
    asset_kind: str  # image | video (of the parent asset)
    path: Path
    filename: str
    extension: str
    mime_type: str
    present: bool
    size_bytes: int
    file_modified_ns: int
    width: int
    height: int
    captured_at: str  # local wall-clock ISO, "" when unknown
    capture_tz_offset: str  # "+HH:MM" / "-HH:MM" / ""
    camera_make: str
    camera_model: str
    apple_record: dict[str, Any]

    @property
    def fingerprint(self) -> str:
        return f"{self.size_bytes}|{self.file_modified_ns}"

    @property
    def state_id(self) -> str:
        return f"{self.native_id}|{self.role}"


def snapshot_photos_store(library_path: Path | str, destination_dir: Path | str) -> Path:
    """Copy Photos.sqlite via the sqlite backup API (repo precedent: apple-messages)."""
    source_path = Path(library_path).expanduser() / "database" / "Photos.sqlite"
    destination = Path(destination_dir) / "Photos.sqlite"
    _probe_openable(source_path)
    source_uri = source_path.resolve().as_uri() + "?mode=ro"
    try:
        source = sqlite3.connect(source_uri, uri=True)
    except sqlite3.Error as exc:
        raise PermissionError(
            f"Could not open Apple Photos store at {source_path}. "
            "Grant Full Disk Access to the launching executable chain and retry."
        ) from exc
    try:
        target = sqlite3.connect(destination)
        try:
            source.backup(target)
        finally:
            target.close()
    finally:
        source.close()
    return destination


def scan_photo_file_candidates(
    snapshot_path: Path | str,
    *,
    originals_root: Path | str,
) -> list[PhotoFileCandidate]:
    """Newest-first file candidates for every non-trashed asset."""
    originals = Path(originals_root).expanduser()
    connection = sqlite3.connect(Path(snapshot_path))
    connection.row_factory = sqlite3.Row
    try:
        tables = {
            str(row["name"])
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        required = {"ZASSET", "ZADDITIONALASSETATTRIBUTES", "ZEXTENDEDATTRIBUTES"}
        missing = required - tables
        if missing:
            raise ApplePhotosSchemaError(
                f"Unsupported Apple Photos schema: missing {', '.join(sorted(missing))}"
            )
        rows = connection.execute(
            """
            SELECT
                a.ZUUID AS uuid,
                a.ZDIRECTORY AS directory,
                a.ZFILENAME AS filename,
                a.ZKIND AS kind,
                a.ZKINDSUBTYPE AS kind_subtype,
                a.ZUNIFORMTYPEIDENTIFIER AS uti,
                a.ZDATECREATED AS date_created,
                a.ZADDEDDATE AS added_date,
                a.ZMODIFICATIONDATE AS modification_date,
                a.ZWIDTH AS width,
                a.ZHEIGHT AS height,
                a.ZLATITUDE AS latitude,
                a.ZLONGITUDE AS longitude,
                a.ZFAVORITE AS favorite,
                a.ZHIDDEN AS hidden,
                a.ZADJUSTMENTSSTATE AS adjustments_state,
                aa.ZORIGINALFILENAME AS original_filename,
                aa.ZORIGINALFILESIZE AS original_file_size,
                aa.ZTIMEZONEOFFSET AS timezone_offset,
                aa.ZINFERREDTIMEZONEOFFSET AS inferred_timezone_offset,
                aa.ZTIMEZONENAME AS timezone_name,
                aa.ZEXIFTIMESTAMPSTRING AS exif_timestamp_string,
                ea.ZCAMERAMAKE AS camera_make,
                ea.ZCAMERAMODEL AS camera_model,
                ea.ZLENSMODEL AS lens_model
            FROM ZASSET a
            LEFT JOIN ZADDITIONALASSETATTRIBUTES aa ON aa.ZASSET = a.Z_PK
            LEFT JOIN ZEXTENDEDATTRIBUTES ea ON ea.ZASSET = a.Z_PK
            WHERE a.ZTRASHEDSTATE = 0
            ORDER BY a.ZDATECREATED DESC
            """
        ).fetchall()
    finally:
        connection.close()

    candidates: list[PhotoFileCandidate] = []
    for row in rows:
        candidates.extend(_candidates_for_asset(row, originals))
    return candidates


def _candidates_for_asset(row: sqlite3.Row, originals: Path) -> list[PhotoFileCandidate]:
    uuid = str(row["uuid"] or "")
    filename = str(row["filename"] or "")
    if not uuid or not filename:
        return []
    directory = str(row["directory"] or "")
    original_path = originals / directory / filename
    kind = int(row["kind"] or 0)
    asset_kind = "video" if kind == 1 else "image"

    tz_seconds = _first_int(row["timezone_offset"], row["inferred_timezone_offset"])
    captured_at, tz_offset = _wall_clock(row["date_created"], tz_seconds)
    record = _apple_record(row)

    extension = _suffix(filename)
    original = _file_candidate(
        row,
        native_id=uuid,
        role="original",
        asset_kind=asset_kind,
        path=original_path,
        filename=str(row["original_filename"] or "") or filename,
        extension=extension,
        mime_type=_mime_type(str(row["uti"] or ""), extension),
        captured_at=captured_at,
        tz_offset=tz_offset,
        record=record,
    )
    candidates = [original]

    if asset_kind == "image" and int(row["kind_subtype"] or 0) == KIND_SUBTYPE_LIVE_PHOTO:
        # The motion component sits next to the still as <uuid>_3.mov and is
        # uploaded under the SAME native id with role live_video, which is what
        # lets the identity layer attach it to the still's asset.
        live_name = f"{Path(filename).stem}_3.mov"
        candidates.append(
            _file_candidate(
                row,
                native_id=uuid,
                role="live_video",
                asset_kind=asset_kind,
                path=original_path.with_name(live_name),
                filename=live_name,
                extension=".mov",
                mime_type="video/quicktime",
                captured_at=captured_at,
                tz_offset=tz_offset,
                record=record,
            )
        )
    return candidates


def _file_candidate(
    row: sqlite3.Row,
    *,
    native_id: str,
    role: str,
    asset_kind: str,
    path: Path,
    filename: str,
    extension: str,
    mime_type: str,
    captured_at: str,
    tz_offset: str,
    record: dict[str, Any],
) -> PhotoFileCandidate:
    size_bytes = 0
    modified_ns = 0
    present = False
    try:
        stat = path.stat()
        size_bytes = int(stat.st_size)
        modified_ns = int(stat.st_mtime_ns)
        present = size_bytes > 0
    except OSError:
        present = False
    width = int(row["width"] or 0) if role == "original" else 0
    height = int(row["height"] or 0) if role == "original" else 0
    return PhotoFileCandidate(
        native_id=native_id,
        role=role,
        asset_kind=asset_kind,
        path=path,
        filename=filename,
        extension=extension,
        mime_type=mime_type,
        present=present,
        size_bytes=size_bytes,
        file_modified_ns=modified_ns,
        width=width,
        height=height,
        captured_at=captured_at,
        capture_tz_offset=tz_offset,
        camera_make=str(row["camera_make"] or ""),
        camera_model=str(row["camera_model"] or ""),
        apple_record=record,
    )


def _apple_record(row: sqlite3.Row) -> dict[str, Any]:
    """The archival raw payload: everything the scanner read, losslessly."""
    record: dict[str, Any] = {
        "uuid": str(row["uuid"] or ""),
        "kind": int(row["kind"] or 0),
        "kind_subtype": int(row["kind_subtype"] or 0),
        "uti": str(row["uti"] or ""),
        "directory": str(row["directory"] or ""),
        "filename": str(row["filename"] or ""),
        "original_filename": str(row["original_filename"] or ""),
        "date_created": _iso_utc(row["date_created"]),
        "added_date": _iso_utc(row["added_date"]),
        "modification_date": _iso_utc(row["modification_date"]),
        "timezone_name": str(row["timezone_name"] or ""),
        "timezone_offset_seconds": _first_int(row["timezone_offset"], row["inferred_timezone_offset"]),
        "exif_timestamp_string": str(row["exif_timestamp_string"] or ""),
        "width": int(row["width"] or 0),
        "height": int(row["height"] or 0),
        "original_file_size": int(row["original_file_size"] or 0),
        "favorite": int(row["favorite"] or 0),
        "hidden": int(row["hidden"] or 0),
        "adjustments_state": int(row["adjustments_state"] or 0),
        "camera_make": str(row["camera_make"] or ""),
        "camera_model": str(row["camera_model"] or ""),
        "lens_model": str(row["lens_model"] or ""),
    }
    latitude = _gps_value(row["latitude"])
    longitude = _gps_value(row["longitude"])
    if latitude is not None and longitude is not None:
        record["latitude"] = latitude
        record["longitude"] = longitude
    return record


def _gps_value(value: Any) -> float | None:
    if value is None:
        return None
    number = float(value)
    if number == GPS_UNSET_SENTINEL:
        return None
    return number


def _first_int(*values: Any) -> int:
    for value in values:
        if value is not None:
            return int(value)
    return 0


def _cocoa_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    return datetime.fromtimestamp(float(value) + COCOA_EPOCH_UNIX_OFFSET, tz=UTC)


def _iso_utc(value: Any) -> str:
    moment = _cocoa_datetime(value)
    return moment.isoformat() if moment else ""


def _wall_clock(date_created: Any, tz_seconds: int) -> tuple[str, str]:
    """(local wall-clock ISO without offset, "+HH:MM" offset) for the capture."""
    moment = _cocoa_datetime(date_created)
    if moment is None:
        return "", ""
    local = moment + timedelta(seconds=tz_seconds)
    sign = "+" if tz_seconds >= 0 else "-"
    magnitude = abs(tz_seconds)
    offset = f"{sign}{magnitude // 3600:02d}:{(magnitude % 3600) // 60:02d}"
    return local.strftime("%Y-%m-%dT%H:%M:%S"), offset


def _suffix(filename: str) -> str:
    return Path(filename).suffix.lower()


def _mime_type(uti: str, extension: str) -> str:
    if uti in _UTI_MIME_TYPES:
        return _UTI_MIME_TYPES[uti]
    if extension in _EXTENSION_MIME_TYPES:
        return _EXTENSION_MIME_TYPES[extension]
    return "application/octet-stream"


def default_originals_root(library_path: Path | str) -> Path:
    return Path(library_path).expanduser() / "originals"
