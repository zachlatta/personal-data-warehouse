"""Export full-resolution Apple Photos originals through a native PhotoKit helper.

The Photos library's ``originals/`` tree is only a local cache when Optimize
Mac Storage is enabled. The helper has an embedded privacy usage description,
so macOS can grant it stable Photos access; it requests original resources
with iCloud network access enabled and writes them to a temporary path.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import platform
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

from personal_data_warehouse_photos.scanner import PhotoFileCandidate, _mime_type, _suffix

HELPER_IDENTIFIER = "com.zachlatta.pdw.photos-exporter"
HELPER_BUILD_TIMEOUT_SECONDS = 120
PHOTOS_AUTHORIZED_STATUS = 3


class PhotoExportError(RuntimeError):
    """PhotoKit could not produce a complete original resource."""


@dataclass(frozen=True)
class ExportedPhotoFile:
    path: Path
    filename: str
    extension: str
    mime_type: str
    size_bytes: int


class PhotoKitAssetExporter:
    """Synchronously export one original PHAssetResource, downloading as needed."""

    def __init__(
        self,
        *,
        timeout_seconds: float = 3600,
        helper_path: Path | str | None = None,
        command_runner=None,
    ) -> None:
        self._timeout_seconds = timeout_seconds
        self._helper_path = Path(helper_path) if helper_path is not None else None
        self._command_runner = command_runner or subprocess.run

    def export(
        self,
        candidate: PhotoFileCandidate,
        destination_dir: Path | str,
    ) -> ExportedPhotoFile:
        destination_root = Path(destination_dir)
        destination_root.mkdir(parents=True, exist_ok=True)
        safe_id = "".join(
            character if character.isalnum() or character in "._-" else "_"
            for character in candidate.native_id
        )
        destination = destination_root / f"{safe_id}-{candidate.role}{candidate.extension}"
        payload = self._run_helper(
            "export",
            "--uuid",
            candidate.native_id,
            "--role",
            candidate.role,
            "--kind",
            candidate.asset_kind,
            "--filename",
            candidate.filename,
            "--destination",
            str(destination),
        )

        filename = str(payload.get("filename") or candidate.filename)
        extension = _suffix(filename) or candidate.extension
        mime_type = _mime_type(str(payload.get("uti") or ""), extension)
        try:
            size_bytes = destination.stat().st_size
        except OSError as exc:
            raise PhotoExportError(
                f"PhotoKit reported success but did not export {candidate.filename}"
            ) from exc
        if size_bytes <= 0:
            destination.unlink(missing_ok=True)
            raise PhotoExportError(f"PhotoKit exported an empty original for {candidate.filename}")
        resource_size = int(payload.get("size_bytes") or 0)
        expected_size = candidate.expected_size_bytes or resource_size
        if expected_size and size_bytes != expected_size:
            destination.unlink(missing_ok=True)
            raise PhotoExportError(
                f"PhotoKit exported {size_bytes} bytes for {candidate.filename}, but Photos "
                f"metadata says the full original is {expected_size} bytes"
            )
        return ExportedPhotoFile(
            path=destination,
            filename=filename,
            extension=extension,
            mime_type=mime_type,
            size_bytes=size_bytes,
        )

    def request_authorization(self) -> int:
        """Prompt for one-time Full Photos access and return the resulting status."""
        return int(self._run_helper("authorize").get("status", -1))

    def authorization_status(self) -> int:
        """Return the current PhotoKit read/write authorization status."""
        return int(self._run_helper("status").get("status", -1))

    def _run_helper(self, *arguments: str) -> dict:
        helper = self._helper_path or build_photokit_helper()
        command = [str(helper), *arguments]
        try:
            result = self._command_runner(
                command,
                capture_output=True,
                text=True,
                timeout=self._timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise PhotoExportError(
                f"Timed out after {self._timeout_seconds:g}s waiting for Apple Photos/iCloud"
            ) from exc
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or "unknown PhotoKit error"
            raise PhotoExportError(detail)
        try:
            payload = json.loads(result.stdout)
        except (TypeError, json.JSONDecodeError) as exc:
            raise PhotoExportError("The native PhotoKit helper returned invalid output") from exc
        if not isinstance(payload, dict):
            raise PhotoExportError("The native PhotoKit helper returned invalid output")
        return payload


def build_photokit_helper(*, destination_dir: Path | None = None) -> Path:
    """Build and cache the native helper with its privacy plist embedded."""
    if platform.system() != "Darwin":
        raise PhotoExportError("Apple Photos export is only available on macOS")
    source_dir = Path(__file__).with_name("macos")
    source = source_dir / "PhotoExporter.swift"
    info_plist = source_dir / "Info.plist"
    digest = hashlib.sha256(source.read_bytes() + b"\0" + info_plist.read_bytes()).hexdigest()
    root = destination_dir or (
        Path.home() / "Library" / "Application Support" / "personal-data-warehouse" / "photos-helper"
    )
    binary = root / "pdw-photos-exporter"
    stamp = root / "source.sha256"
    lock_path = root / "build.lock"
    root.mkdir(parents=True, exist_ok=True)

    with lock_path.open("a+") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        if binary.is_file() and stamp.is_file() and stamp.read_text(encoding="utf-8").strip() == digest:
            return binary
        xcrun = shutil.which("xcrun")
        codesign = shutil.which("codesign")
        if not xcrun or not codesign:
            raise PhotoExportError(
                "Building the Apple Photos helper requires the macOS Command Line Tools"
            )
        with tempfile.TemporaryDirectory(prefix="pdw-photos-helper-", dir=root) as temporary:
            temporary_binary = Path(temporary) / binary.name
            compile_result = subprocess.run(
                [
                    xcrun,
                    "swiftc",
                    "-O",
                    str(source),
                    "-o",
                    str(temporary_binary),
                    "-Xlinker",
                    "-sectcreate",
                    "-Xlinker",
                    "__TEXT",
                    "-Xlinker",
                    "__info_plist",
                    "-Xlinker",
                    str(info_plist),
                ],
                capture_output=True,
                text=True,
                timeout=HELPER_BUILD_TIMEOUT_SECONDS,
                check=False,
            )
            if compile_result.returncode != 0:
                raise PhotoExportError(
                    "Could not compile the native Apple Photos helper: "
                    + (compile_result.stderr.strip() or compile_result.stdout.strip())
                )
            sign_result = subprocess.run(
                [
                    codesign,
                    "--force",
                    "--sign",
                    "-",
                    "--identifier",
                    HELPER_IDENTIFIER,
                    str(temporary_binary),
                ],
                capture_output=True,
                text=True,
                timeout=HELPER_BUILD_TIMEOUT_SECONDS,
                check=False,
            )
            if sign_result.returncode != 0:
                raise PhotoExportError(
                    "Could not sign the native Apple Photos helper: "
                    + (sign_result.stderr.strip() or sign_result.stdout.strip())
                )
            os.replace(temporary_binary, binary)
            temporary_stamp = Path(temporary) / stamp.name
            temporary_stamp.write_text(digest + "\n", encoding="utf-8")
            os.replace(temporary_stamp, stamp)
    return binary
