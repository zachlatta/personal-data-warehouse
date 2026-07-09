from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.message import Message
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from urllib.parse import unquote, urljoin, urlparse
import hashlib
import json
import mimetypes
import re

import requests

from personal_data_warehouse.objectstore import ObjectPresence, ObjectStore

SOURCE = "alice_voice_recordings"
OBJECT_PREFIX = "alice-voice-recordings"
INBOX_PREFIX = f"{OBJECT_PREFIX}/inbox"
LIBRARY_PREFIX = f"{OBJECT_PREFIX}/library"
DEFAULT_AUDIO_EXTENSION = ".mp3"
DEFAULT_AUDIO_CONTENT_TYPE = "audio/mpeg"
METADATA_KIND = "voice_recording_metadata"
PAGE_HTML_KIND = "voice_recording_page_html"


@dataclass(frozen=True)
class AliceVoiceRecordingsImportSummary:
    upload_requests_seen: int
    recordings_skipped: int
    recordings_uploaded: int
    metadata_uploaded: int
    bytes_uploaded: int
    bytes_skipped: int


@dataclass(frozen=True)
class AliceDownloadedRecording:
    path: Path | None
    upload_request: Mapping[str, Any]
    recording_id: str
    title: str
    filename: str
    extension: str
    content_type: str
    size_bytes: int
    content_sha256: str
    file_created_at: datetime
    file_modified_at: datetime
    recorded_at: datetime
    page_html_path: Path | None = None
    page_html_content_sha256: str = ""
    page_html_size_bytes: int = 0
    media_download_error: str = ""


class AliceVoiceRecordingsImportRunner:
    def __init__(
        self,
        *,
        account: str,
        upload_requests: Iterable[Mapping[str, Any]],
        object_store: ObjectStore,
        logger,
        now=None,
        session: requests.Session | None = None,
        stage: str = "library",
        mode: str = "full",
    ) -> None:
        if stage not in {"inbox", "library"}:
            raise ValueError("stage must be 'inbox' or 'library'")
        if mode not in {"incremental", "full"}:
            raise ValueError("mode must be 'incremental' or 'full'")
        self._account = account
        self._upload_requests = upload_requests
        self._object_store = object_store
        self._logger = logger
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._session = session or requests.Session()
        self._stage = stage
        self._mode = mode

    def sync(self) -> AliceVoiceRecordingsImportSummary:
        seen = 0
        uploaded = 0
        skipped = 0
        metadata_uploaded = 0
        bytes_uploaded = 0
        bytes_skipped = 0

        with TemporaryDirectory(prefix="pdw-alice-recordings-") as temp_dir:
            temp_root = Path(temp_dir)
            for upload_request in self._upload_requests:
                seen += 1
                recording_url = media_url_from_recording(upload_request)
                recording_id = recording_id_from_upload_request(upload_request, recording_url=recording_url)
                if self._mode == "incremental" and object_store_has_object(
                    self._object_store,
                    kind=METADATA_KIND,
                    key="alice_upload_id",
                    value=recording_id,
                ):
                    self._logger.info("skip Alice recording %s: metadata already archived", recording_id)
                    skipped += 1
                    continue
                downloaded = download_recording(
                    upload_request=upload_request,
                    destination=temp_root / f"{recording_id or seen}.download",
                    session=self._session,
                )
                presence = (
                    self._object_store.presence(content_sha256=downloaded.content_sha256)
                    if downloaded.content_sha256
                    else ObjectPresence(audio_exists=False, metadata_exists=False)
                )
                audio_key = audio_object_key(downloaded, stage=self._stage)
                metadata_key = metadata_object_key(downloaded, stage=self._stage)
                metadata_exists = presence.metadata_exists or object_store_has_object(
                    self._object_store,
                    kind=METADATA_KIND,
                    key="alice_upload_id",
                    value=downloaded.recording_id,
                )
                if downloaded.path is not None and presence.audio_exists and metadata_exists:
                    self._logger.info(
                        "skip Alice recording %s (%s, sha256=%s): audio and metadata already present",
                        downloaded.recording_id,
                        format_bytes(downloaded.size_bytes),
                        short_sha256(downloaded.content_sha256),
                    )
                    skipped += 1
                    bytes_skipped += downloaded.size_bytes
                    continue

                app_properties = {"alice_upload_id": downloaded.recording_id}
                if downloaded.path is not None and not presence.audio_exists:
                    self._logger.info("upload Alice recording %s -> %s", downloaded.recording_id, audio_key)
                    self._object_store.put_file(
                        path=downloaded.path,
                        object_key=audio_key,
                        content_sha256=downloaded.content_sha256,
                        content_type=downloaded.content_type,
                        skip_existing_check=True,
                        app_properties=app_properties,
                    )
                    uploaded += 1
                    bytes_uploaded += downloaded.size_bytes
                if downloaded.page_html_path is not None and not object_store_has_object(
                    self._object_store,
                    kind=PAGE_HTML_KIND,
                    key="content_sha256",
                    value=downloaded.page_html_content_sha256,
                ):
                    page_key = page_html_object_key(downloaded, stage=self._stage)
                    self._logger.info("archive Alice recording page %s -> %s", downloaded.recording_id, page_key)
                    self._object_store.put_file(
                        path=downloaded.page_html_path,
                        object_key=page_key,
                        content_sha256=downloaded.page_html_content_sha256,
                        content_type="text/html",
                        skip_existing_check=True,
                        app_properties=app_properties,
                        kind=PAGE_HTML_KIND,
                    )
                if not metadata_exists:
                    payload = build_metadata(account=self._account, recording=downloaded, uploaded_at=self._now())
                    self._logger.info("metadata Alice recording %s -> %s", downloaded.recording_id, metadata_key)
                    self._object_store.put_json(
                        object_key=metadata_key,
                        payload=payload,
                        content_sha256=json_sha256(payload),
                        source_content_sha256=downloaded.content_sha256 or None,
                        skip_existing_check=True,
                        app_properties=app_properties,
                        kind=METADATA_KIND,
                    )
                    metadata_uploaded += 1

        return AliceVoiceRecordingsImportSummary(
            upload_requests_seen=seen,
            recordings_skipped=skipped,
            recordings_uploaded=uploaded,
            metadata_uploaded=metadata_uploaded,
            bytes_uploaded=bytes_uploaded,
            bytes_skipped=bytes_skipped,
        )


def download_recording(
    *,
    upload_request: Mapping[str, Any],
    destination: Path,
    session: requests.Session,
) -> AliceDownloadedRecording:
    recording_url = media_url_from_recording(upload_request)
    created_at = parse_datetime(str(upload_request.get("created_at", "")))
    updated_at = parse_datetime(str(upload_request.get("updated_at", ""))) or created_at
    recording_id = recording_id_from_upload_request(upload_request, recording_url=recording_url)
    title = title_from_upload_request(upload_request, filename=recording_id)
    if not recording_url:
        return metadata_only_recording(
            upload_request=upload_request,
            destination=destination,
            recording_id=recording_id,
            title=title,
            created_at=created_at,
            updated_at=updated_at,
            session=session,
            media_download_error="missing media URL",
        )

    try:
        response = session.get(recording_url, stream=True, timeout=120)
        response.raise_for_status()
    except requests.HTTPError as exc:
        return metadata_only_recording(
            upload_request=upload_request,
            destination=destination,
            recording_id=recording_id,
            title=title,
            created_at=created_at,
            updated_at=updated_at,
            session=session,
            media_download_error=download_error_message(exc),
        )
    media_response, page_html = media_response_from_alice_response(
        response=response,
        recording_url=recording_url,
        session=session,
    )
    page_html_path = None
    page_html_content_sha256 = ""
    page_html_size_bytes = 0
    if page_html is not None:
        page_html_path = destination.with_suffix(".html")
        page_html_path.write_bytes(page_html)
        page_html_content_sha256 = hashlib.sha256(page_html).hexdigest()
        page_html_size_bytes = len(page_html)

    if media_response is None:
        return metadata_only_recording(
            upload_request=upload_request,
            destination=destination,
            recording_id=recording_id,
            title=title,
            created_at=created_at,
            updated_at=updated_at,
            session=session,
            page_html_path=page_html_path,
            page_html_content_sha256=page_html_content_sha256,
            page_html_size_bytes=page_html_size_bytes,
            media_download_error="media file not found in Alice recording page",
        )

    content_type = media_response.headers.get("Content-Type", "").split(";", 1)[0].strip() or DEFAULT_AUDIO_CONTENT_TYPE
    filename = filename_from_response(media_response.headers, recording_url, upload_request=upload_request)
    extension = Path(filename).suffix.lower() or extension_for_content_type(content_type)
    destination = destination.with_suffix(extension)
    digest = hashlib.sha256()
    size_bytes = 0
    with destination.open("wb") as file:
        for chunk in media_response.iter_content(chunk_size=1024 * 1024):
            if not chunk:
                continue
            digest.update(chunk)
            size_bytes += len(chunk)
            file.write(chunk)

    title = title_from_upload_request(upload_request, filename=filename)
    return AliceDownloadedRecording(
        path=destination,
        upload_request=upload_request,
        recording_id=recording_id,
        title=title,
        filename=filename,
        extension=extension,
        content_type=content_type,
        size_bytes=size_bytes,
        content_sha256=digest.hexdigest(),
        file_created_at=created_at,
        file_modified_at=updated_at,
        recorded_at=created_at,
        page_html_path=page_html_path,
        page_html_content_sha256=page_html_content_sha256,
        page_html_size_bytes=page_html_size_bytes,
    )


def media_response_from_alice_response(*, response, recording_url: str, session: requests.Session):
    content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
    if content_type and content_type not in {"text/html", "application/xhtml+xml"}:
        return response, None
    html_bytes = response.content
    html = html_bytes.decode("utf-8", errors="ignore")
    match = re.search(r'href="([^"]*/download_media_file[^"]*)"', html)
    if match is None:
        return None, html_bytes
    media_url = urljoin(recording_url, match.group(1).replace("&amp;", "&"))
    media_response = session.get(media_url, stream=True, timeout=120)
    media_response.raise_for_status()
    return media_response, html_bytes


def build_metadata(
    *,
    account: str,
    recording: AliceDownloadedRecording,
    uploaded_at: datetime,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "source": SOURCE,
        "account": account,
        "uploaded_at": uploaded_at.isoformat(),
        "recording": {
            "recording_id": recording.recording_id,
            "title": recording.title,
            "original_path": media_url_from_recording(recording.upload_request)
            or page_url_from_recording(recording.upload_request),
            "filename": recording.filename,
            "extension": recording.extension,
            "content_type": recording.content_type,
            "size_bytes": recording.size_bytes,
            "content_sha256": recording.content_sha256,
            "file_created_at": recording.file_created_at.isoformat(),
            "file_modified_at": recording.file_modified_at.isoformat(),
            "recorded_at": recording.recorded_at.isoformat(),
        },
        "alice": {
            "upload_request_id": recording.recording_id,
            "status": str(recording.upload_request.get("status", "")),
            "message": str(recording.upload_request.get("message", "")),
            "recording_transcription_status": recording.upload_request.get("recording_transcription_status", []),
            "recording_url": media_url_from_recording(recording.upload_request),
            "recording_page_url": page_url_from_recording(recording.upload_request),
            "created_at": str(recording.upload_request.get("created_at", "")),
            "updated_at": str(recording.upload_request.get("updated_at", "")),
            "duration_in_secs": recording.upload_request.get("duration_in_secs"),
            "transcript_ready": recording.upload_request.get("is_transcript_ready?"),
            "media_file_ready": recording.upload_request.get("is_media_file_ready?"),
            "media_download_error": recording.media_download_error,
            "page_html_content_sha256": recording.page_html_content_sha256,
            "page_html_size_bytes": recording.page_html_size_bytes,
            "raw_upload_request": dict(recording.upload_request),
        },
    }


def metadata_only_recording(
    *,
    upload_request: Mapping[str, Any],
    destination: Path,
    recording_id: str,
    title: str,
    created_at: datetime,
    updated_at: datetime,
    session: requests.Session,
    media_download_error: str,
    page_html_path: Path | None = None,
    page_html_content_sha256: str = "",
    page_html_size_bytes: int = 0,
) -> AliceDownloadedRecording:
    if page_html_path is None:
        page_url = page_url_from_recording(upload_request)
        if page_url:
            try:
                page_response = session.get(page_url, stream=True, timeout=120)
                page_response.raise_for_status()
                page_html = page_response.content
                page_html_path = destination.with_suffix(".html")
                page_html_path.write_bytes(page_html)
                page_html_content_sha256 = hashlib.sha256(page_html).hexdigest()
                page_html_size_bytes = len(page_html)
            except requests.HTTPError as exc:
                media_download_error = f"{media_download_error}; page download failed: {download_error_message(exc)}"

    return AliceDownloadedRecording(
        path=None,
        upload_request=upload_request,
        recording_id=recording_id,
        title=title,
        filename=recording_id,
        extension="",
        content_type="",
        size_bytes=0,
        content_sha256="",
        file_created_at=created_at,
        file_modified_at=updated_at,
        recorded_at=created_at,
        page_html_path=page_html_path,
        page_html_content_sha256=page_html_content_sha256,
        page_html_size_bytes=page_html_size_bytes,
        media_download_error=media_download_error,
    )


def audio_object_key(recording: AliceDownloadedRecording, *, stage: str = "library") -> str:
    return f"{object_key_base(recording, stage=stage)}{recording.extension}"


def metadata_object_key(recording: AliceDownloadedRecording, *, stage: str = "library") -> str:
    return f"{object_key_base(recording, stage=stage)}.json"


def page_html_object_key(recording: AliceDownloadedRecording, *, stage: str = "library") -> str:
    return f"{object_key_base(recording, stage=stage)}.html"


def object_key_base(recording: AliceDownloadedRecording, *, stage: str = "library") -> str:
    recorded_at = recording.recorded_at
    prefix = LIBRARY_PREFIX if stage == "library" else INBOX_PREFIX
    object_id = recording.content_sha256 or f"alice-{safe_object_key_part(recording.recording_id)}"
    return (
        f"{prefix}/{recorded_at.year:04d}/{recorded_at.month:02d}/"
        f"{recorded_at.date().isoformat()}-{object_id}"
    )


def filename_from_response(headers: Mapping[str, str], url: str, *, upload_request: Mapping[str, Any]) -> str:
    disposition = headers.get("Content-Disposition") or headers.get("content-disposition")
    if disposition:
        message = Message()
        message["Content-Disposition"] = disposition
        filename = message.get_filename()
        if filename:
            return safe_filename(filename)
    path_name = Path(unquote(urlparse(url).path)).name
    if path_name and "." in path_name:
        return safe_filename(path_name)
    media_file_name = str(upload_request.get("media_file_name", "") or "")
    if media_file_name:
        return safe_filename(media_file_name)
    recording_id = str(upload_request.get("id", "")) or stable_id_from_url(url)
    return f"{recording_id}{DEFAULT_AUDIO_EXTENSION}"


def title_from_upload_request(upload_request: Mapping[str, Any], *, filename: str) -> str:
    title = str(upload_request.get("title", "")).strip()
    if title:
        return title
    message = str(upload_request.get("message", "")).strip()
    if message:
        return message
    return Path(filename).stem


def safe_filename(value: str) -> str:
    return re.sub(r"[/\0]+", "-", value).strip() or f"recording{DEFAULT_AUDIO_EXTENSION}"


def safe_object_key_part(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "-", value).strip("-") or "unknown"


def object_store_has_object(object_store: ObjectStore, *, kind: str, key: str, value: str) -> bool:
    has_object = getattr(object_store, "has_object", None)
    if callable(has_object):
        return bool(has_object(kind=kind, key=key, value=value))
    return False


def download_error_message(exc: requests.HTTPError) -> str:
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    if status_code:
        return f"HTTP {status_code}"
    return exc.__class__.__name__


def media_url_from_recording(upload_request: Mapping[str, Any]) -> str:
    for key in ("_pdw_media_url", "mp3_download_url", "recording_url", "media_url", "download_url"):
        value = str(upload_request.get(key, "") or "")
        if value:
            return value
    return ""


def page_url_from_recording(upload_request: Mapping[str, Any]) -> str:
    return str(upload_request.get("_pdw_recording_page_url", "") or upload_request.get("recording_page_url", "") or "")


def recording_id_from_upload_request(upload_request: Mapping[str, Any], *, recording_url: str) -> str:
    for key in ("guid", "id"):
        value = str(upload_request.get(key, "") or "")
        if value:
            return value
    return stable_id_from_url(recording_url)


def extension_for_content_type(content_type: str) -> str:
    if content_type == DEFAULT_AUDIO_CONTENT_TYPE:
        return DEFAULT_AUDIO_EXTENSION
    extension = mimetypes.guess_extension(content_type)
    return extension or DEFAULT_AUDIO_EXTENSION


def parse_datetime(value: str) -> datetime:
    if not value:
        return datetime.fromtimestamp(0, tz=UTC)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def stable_id_from_url(url: str) -> str:
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
    return digest[:24]


def json_sha256(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def short_sha256(value: str) -> str:
    return value[:12]


def format_bytes(size: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{size} B"
