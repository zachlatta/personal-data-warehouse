"""Client for the app's semantic ingestion endpoints (``POST /ingest/...``).

This is the write-path replacement for talking to Google Drive directly. Client
uploaders hand domain payloads (a gzipped batch, an attachment file, ...) to the
matching semantic method; the app owns every storage detail (folder ids, object
keys, ``kind`` values, ``pdw_*`` tags, and the Drive credential). Devices hold
only the app base URL and the shared signing key.

Requests are authenticated with the same HMAC signing scheme the app uses for
signed object download links (see ``app/internal/auth/objectupload.go``): the
signature covers the endpoint, the body's sha256, and an expiry, so a link can
neither be replayed against another endpoint nor reused for a different body.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import threading
import time
from collections.abc import Callable, Mapping
from urllib.parse import urlencode

import requests

# Domain separator; must match objectUploadKind in the Go auth package.
OBJECT_UPLOAD_KIND = "object-upload"

DEFAULT_LINK_TTL_SECONDS = 900
DEFAULT_TIMEOUT_SECONDS = 120.0

StoredObjectDict = dict[str, str]


def sign_object_upload(secret: bytes, endpoint: str, content_sha256: str, exp_unix: int) -> str:
    """Return the base64url (unpadded) HMAC authorizing an upload.

    Mirrors ``Service.SignObjectUpload`` in Go: HMAC-SHA256 over
    ``"object-upload\\n<endpoint>\\n<content_sha256>\\n<exp_unix>"``.
    """

    message = f"{OBJECT_UPLOAD_KIND}\n{endpoint}\n{content_sha256}\n{exp_unix}".encode("utf-8")
    digest = hmac.new(secret, message, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


class IngestClient:
    """Posts domain payloads to the app's semantic ingestion endpoints."""

    def __init__(
        self,
        *,
        base_url: str,
        signing_key: bytes,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        link_ttl_seconds: int = DEFAULT_LINK_TTL_SECONDS,
        session: requests.Session | None = None,
        now: Callable[[], float] = time.time,
    ) -> None:
        if not base_url:
            raise ValueError("base_url is required")
        if not signing_key:
            raise ValueError("signing_key is required")
        self._base_url = base_url.rstrip("/")
        self._signing_key = signing_key
        self._timeout = timeout
        self._link_ttl_seconds = link_ttl_seconds
        self._session = session
        self._thread_local = threading.local()
        self._now = now

    def _session_for_thread(self) -> requests.Session:
        if self._session is not None:
            return self._session
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = requests.Session()
            self._thread_local.session = session
        return session

    def _post_json(
        self,
        endpoint: str,
        *,
        payload: Mapping[str, object],
        params: Mapping[str, str | None],
    ) -> StoredObjectDict:
        # Canonical JSON so the sidecar bytes (and their sha) are deterministic
        # and match what the legacy Drive path stored.
        body = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        return self._post(endpoint, body=body, content_type="application/json", params=params)

    def _post(
        self,
        endpoint: str,
        *,
        body: bytes,
        content_type: str,
        params: Mapping[str, str | None],
    ) -> StoredObjectDict:
        content_sha256 = hashlib.sha256(body).hexdigest()
        exp_unix = int(self._now()) + self._link_ttl_seconds
        signature = sign_object_upload(self._signing_key, endpoint, content_sha256, exp_unix)
        query: dict[str, str] = {key: value for key, value in params.items() if value is not None}
        query["content_sha256"] = content_sha256
        query["exp"] = str(exp_unix)
        query["sig"] = signature
        url = f"{self._base_url}{endpoint}?{urlencode(query)}"
        response = self._session_for_thread().post(
            url,
            data=body,
            headers={"Content-Type": content_type},
            timeout=self._timeout,
        )
        response.raise_for_status()
        payload = response.json()
        return {
            "storage_backend": str(payload.get("storage_backend", "")),
            "storage_key": str(payload.get("storage_key", "")),
            "storage_file_id": str(payload.get("storage_file_id", "")),
            "storage_url": str(payload.get("storage_url", "")),
        }

    # --- agent sessions -----------------------------------------------------
    def upload_agent_sessions_batch(self, gzip_bytes: bytes, *, exported_at: str) -> StoredObjectDict:
        return self._post(
            "/ingest/agent-sessions/batch",
            body=gzip_bytes,
            content_type="application/gzip",
            params={"exported_at": exported_at},
        )

    # --- apple messages -----------------------------------------------------
    def upload_apple_messages_batch(self, gzip_bytes: bytes, *, exported_at: str) -> StoredObjectDict:
        return self._post(
            "/ingest/apple-messages/batch",
            body=gzip_bytes,
            content_type="application/gzip",
            params={"exported_at": exported_at},
        )

    def upload_apple_messages_attachment(
        self,
        content: bytes,
        *,
        attachment_guid: str,
        message_guid: str,
        content_type: str,
        created_at: str,
        filename: str,
    ) -> StoredObjectDict:
        return self._post(
            "/ingest/apple-messages/attachment",
            body=content,
            content_type=content_type or "application/octet-stream",
            params={
                "attachment_guid": attachment_guid,
                "message_guid": message_guid,
                "content_type": content_type,
                "created_at": created_at,
                "filename": filename,
            },
        )

    # --- whatsapp -----------------------------------------------------------
    def upload_whatsapp_batch(self, gzip_bytes: bytes, *, exported_at: str) -> StoredObjectDict:
        return self._post(
            "/ingest/whatsapp/batch",
            body=gzip_bytes,
            content_type="application/gzip",
            params={"exported_at": exported_at},
        )

    def upload_whatsapp_media(
        self,
        content: bytes,
        *,
        chat_id: str,
        message_id: str,
        content_type: str,
        message_at: str,
        filename: str,
        mime_type: str,
    ) -> StoredObjectDict:
        return self._post(
            "/ingest/whatsapp/media",
            body=content,
            content_type=content_type or "application/octet-stream",
            params={
                "chat_id": chat_id,
                "message_id": message_id,
                "content_type": content_type,
                "message_at": message_at,
                "filename": filename,
                "mime_type": mime_type,
            },
        )

    # --- voice memos --------------------------------------------------------
    def upload_voice_memo_audio(
        self,
        content: bytes,
        *,
        recorded_at: str,
        extension: str,
        content_type: str,
    ) -> StoredObjectDict:
        return self._post(
            "/ingest/voice-memos/audio",
            body=content,
            content_type=content_type or "application/octet-stream",
            params={"recorded_at": recorded_at, "extension": extension, "content_type": content_type},
        )

    def upload_voice_memo_metadata(
        self,
        payload: Mapping[str, object],
        *,
        recorded_at: str,
        audio_content_sha256: str,
    ) -> StoredObjectDict:
        return self._post_json(
            "/ingest/voice-memos/metadata",
            payload=payload,
            params={"recorded_at": recorded_at, "audio_content_sha256": audio_content_sha256},
        )

    # --- apple notes --------------------------------------------------------
    def upload_apple_notes_body(
        self,
        html: bytes,
        *,
        note_id: str,
        revision_id: str,
        modified_at: str,
    ) -> StoredObjectDict:
        return self._post(
            "/ingest/apple-notes/body",
            body=html,
            content_type="text/html",
            params={"note_id": note_id, "revision_id": revision_id, "modified_at": modified_at},
        )

    def upload_apple_notes_attachment(
        self,
        content: bytes,
        *,
        note_id: str,
        revision_id: str,
        modified_at: str,
        attachment_id: str,
        filename: str,
        content_type: str,
    ) -> StoredObjectDict:
        return self._post(
            "/ingest/apple-notes/attachment",
            body=content,
            content_type=content_type or "application/octet-stream",
            params={
                "note_id": note_id,
                "revision_id": revision_id,
                "modified_at": modified_at,
                "attachment_id": attachment_id,
                "filename": filename,
                "content_type": content_type,
            },
        )

    def upload_apple_notes_revision(
        self,
        payload: Mapping[str, object],
        *,
        note_id: str,
        revision_id: str,
        modified_at: str,
        note_content_sha256: str,
    ) -> StoredObjectDict:
        return self._post_json(
            "/ingest/apple-notes/revision",
            payload=payload,
            params={
                "note_id": note_id,
                "revision_id": revision_id,
                "modified_at": modified_at,
                "note_content_sha256": note_content_sha256,
            },
        )


def ingest_upload_config_problem() -> str | None:
    """Return a human-readable reason http_app upload env is missing, else None.

    Mirrors the env names :func:`ingest_client_from_env` reads so callers can
    detect a misconfigured deployment *before* doing any work. Long-running
    clients launched by a keepalive sensor (e.g. the WhatsApp client) use this
    to skip launching a run that would only crash-loop on missing config,
    turning silent run-failure floods into one clear skip reason.
    """

    if not (os.getenv("PDW_API_URL") or os.getenv("MCP_BASE_URL") or "").strip():
        return "PDW_API_URL (or MCP_BASE_URL) must be set for http_app uploads"
    if not (os.getenv("PDW_SECRET_TOKEN") or os.getenv("MCP_SECRET_TOKEN") or "").strip():
        return "PDW_SECRET_TOKEN (or MCP_SECRET_TOKEN) must be set for http_app uploads"
    return None


def ingest_client_from_env(
    *,
    now: Callable[[], float] = time.time,
    session: requests.Session | None = None,
) -> IngestClient:
    """Build an IngestClient from the shared app env.

    The app base URL is the warehouse's main API URL: ``PDW_API_URL`` is the
    canonical name (the same URL pdw uses for queries; ``pdw ingest`` passes it
    and the token down automatically), with ``MCP_BASE_URL`` accepted as an
    alias. The signing key is the app secret token (``PDW_SECRET_TOKEN``), also
    accepted as ``MCP_SECRET_TOKEN``. Folder ids and the Drive credential are
    deliberately NOT read here; those live only in the app.
    """

    problem = ingest_upload_config_problem()
    if problem:
        raise ValueError(problem)
    base_url = (
        os.getenv("PDW_API_URL")
        or os.getenv("MCP_BASE_URL")
        or ""
    ).strip()
    secret = (
        os.getenv("PDW_SECRET_TOKEN")
        or os.getenv("MCP_SECRET_TOKEN")
        or ""
    ).strip()
    return IngestClient(base_url=base_url, signing_key=secret.encode("utf-8"), now=now, session=session)
