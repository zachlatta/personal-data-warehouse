"""Native PhotoKit original-resource export contract."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from personal_data_warehouse_photos.exporter import PhotoExportError, PhotoKitAssetExporter
from personal_data_warehouse_photos.scanner import PhotoFileCandidate


def _candidate(*, role: str = "original", asset_kind: str = "image") -> PhotoFileCandidate:
    return PhotoFileCandidate(
        native_id="UUID-ICLOUD",
        role=role,
        asset_kind=asset_kind,
        filename="IMG_0357.HEIC" if role == "original" else "IMG_0357.MOV",
        extension=".heic" if role == "original" else ".mov",
        mime_type="image/heic" if role == "original" else "video/quicktime",
        expected_size_bytes=20 if role == "original" else 0,
        width=4032,
        height=3024,
        captured_at="2026-07-01T12:00:00",
        capture_tz_offset="-04:00",
        camera_make="Apple",
        camera_model="iPhone",
        apple_record={"uuid": "UUID-ICLOUD", "modification_date": "2026-07-01T16:00:00+00:00"},
    )


class _HelperRunner:
    def __init__(
        self,
        *,
        content: bytes = b"full-icloud-original",
        error: str = "",
        status: int = 3,
    ) -> None:
        self.content = content
        self.error = error
        self.status = status
        self.calls: list[list[str]] = []

    def __call__(self, command, **_kwargs):
        command = [str(value) for value in command]
        self.calls.append(command)
        if self.error:
            return subprocess.CompletedProcess(command, 1, stdout="", stderr=self.error)
        if command[1] == "export":
            destination = Path(command[command.index("--destination") + 1])
            destination.write_bytes(self.content)
            role = command[command.index("--role") + 1]
            filename = "IMG_0357.MOV" if role == "live_video" else "IMG_0357.HEIC"
            uti = "com.apple.quicktime-movie" if role == "live_video" else "public.heic"
            payload = {"filename": filename, "uti": uti, "size_bytes": len(self.content)}
        else:
            payload = {"status": self.status}
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps(payload), stderr="")


def _exporter(runner: _HelperRunner, tmp_path: Path) -> PhotoKitAssetExporter:
    return PhotoKitAssetExporter(
        helper_path=tmp_path / "pdw-photos-exporter",
        command_runner=runner,
        timeout_seconds=1,
    )


def test_export_downloads_full_original_from_icloud(tmp_path):
    runner = _HelperRunner()

    exported = _exporter(runner, tmp_path).export(_candidate(), tmp_path)

    command = runner.calls[0]
    assert command[1] == "export"
    assert command[command.index("--uuid") + 1] == "UUID-ICLOUD"
    assert command[command.index("--role") + 1] == "original"
    assert exported.path.read_bytes() == b"full-icloud-original"
    assert exported.filename == "IMG_0357.HEIC"
    assert exported.size_bytes == 20


def test_export_requests_live_photos_original_paired_video(tmp_path):
    runner = _HelperRunner(content=b"original-live-video")

    exported = _exporter(runner, tmp_path).export(
        _candidate(role="live_video"),
        tmp_path,
    )

    command = runner.calls[0]
    assert command[command.index("--role") + 1] == "live_video"
    assert exported.path.read_bytes() == b"original-live-video"
    assert exported.extension == ".mov"


def test_export_fails_loudly_when_photokit_cannot_download(tmp_path):
    runner = _HelperRunner(error="iCloud is unavailable")

    with pytest.raises(PhotoExportError, match="iCloud is unavailable"):
        _exporter(runner, tmp_path).export(_candidate(), tmp_path)


def test_export_rejects_a_truncated_original(tmp_path):
    runner = _HelperRunner(content=b"truncated")

    with pytest.raises(PhotoExportError, match="full original is 20 bytes"):
        _exporter(runner, tmp_path).export(_candidate(), tmp_path)


def test_request_authorization_uses_native_helper(tmp_path):
    runner = _HelperRunner(status=3)

    assert _exporter(runner, tmp_path).request_authorization() == 3
    assert runner.calls == [[str(tmp_path / "pdw-photos-exporter"), "authorize"]]


def test_native_helper_pins_full_original_and_icloud_contract():
    helper_dir = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "personal_data_warehouse_photos"
        / "macos"
    )
    source = (helper_dir / "PhotoExporter.swift").read_text(encoding="utf-8")
    info_plist = (helper_dir / "Info.plist").read_text(encoding="utf-8")

    assert "options.isNetworkAccessAllowed = true" in source
    assert "wantedType = .photo" in source
    assert "wantedType = .video" in source
    assert "wantedType = .pairedVideo" in source
    assert "PHAsset.fetchAssets(with: nil)" in source  # non-default library-scope fallback
    assert "NSPhotoLibraryUsageDescription" in info_plist
    assert "com.zachlatta.pdw.photos-exporter" in info_plist
