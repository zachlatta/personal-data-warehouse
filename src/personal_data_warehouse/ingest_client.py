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
import logging
import os
import shutil
import subprocess
import threading
import time
from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import urlencode, urlsplit

import requests

# Domain separator; must match objectUploadKind in the Go auth package.
OBJECT_UPLOAD_KIND = "object-upload"

DEFAULT_LINK_TTL_SECONDS = 900
DEFAULT_TIMEOUT_SECONDS = 120.0
# Floor throughput used to scale the upload timeout with body size, so a large
# blob gets proportionally longer to finish streaming to object storage instead
# of tripping the fixed base timeout. ~1 MiB/s is deliberately pessimistic
# (a 512 MiB body → ~512 s) to tolerate slow Drive writes.
_MIN_UPLOAD_BYTES_PER_SEC = 1024 * 1024

# The app accepts bodies up to PDW_INGEST_MAX_OBJECT_BYTES (Go default 512 MiB).
# Mirror that default so size-aware clients (e.g. voice memos) can defer a file
# that the app would only 413 anyway instead of wedging the whole run on it.
DEFAULT_INGEST_MAX_OBJECT_BYTES = 512 * 1024 * 1024
# Cloudflare (which fronts the public app hostname) hard-caps request bodies at
# 100 MiB on non-Enterprise plans, so an upload that goes through the public URL
# is really limited to this regardless of the app's own cap. The Tailscale-direct
# origin reaches the app behind Cloudflare and lifts the body to the app cap.
CLOUDFLARE_MAX_BODY_BYTES = 100 * 1024 * 1024

_logger = logging.getLogger(__name__)

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
        upload_base_url: str | None = None,
        max_object_bytes: int = DEFAULT_INGEST_MAX_OBJECT_BYTES,
    ) -> None:
        if not base_url:
            raise ValueError("base_url is required")
        if not signing_key:
            raise ValueError("signing_key is required")
        self._base_url = base_url.rstrip("/")
        # Uploads may be routed to a different origin (e.g. the app reached
        # directly over Tailscale, bypassing Cloudflare's body-size cap) while
        # still authenticating as the public host. The HMAC signature covers
        # only the endpoint path + body sha + expiry, never the host, so the
        # app verifies an upload identically no matter which origin served it.
        self._upload_base_url = (upload_base_url or base_url).rstrip("/")
        self._host_header = urlsplit(self._base_url).netloc if upload_base_url else None
        self._max_object_bytes = max_object_bytes
        self._signing_key = signing_key
        self._timeout = timeout
        self._link_ttl_seconds = link_ttl_seconds
        self._session = session
        self._thread_local = threading.local()
        self._now = now

    @property
    def effective_max_upload_bytes(self) -> int:
        """Largest body this client can actually deliver on its chosen route.

        When uploads go straight to the app (Tailscale-direct), the ceiling is
        the app's own cap. When they traverse the public host, Cloudflare's
        100 MiB body limit applies first, so callers that can defer oversized
        items (e.g. voice memos) should honor the smaller of the two.
        """

        if self._host_header is not None:
            return self._max_object_bytes
        return min(self._max_object_bytes, CLOUDFLARE_MAX_BODY_BYTES)

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

    def _signed_post(
        self,
        endpoint: str,
        *,
        body: bytes,
        content_type: str,
        params: Mapping[str, str | None],
    ) -> Any:
        content_sha256 = hashlib.sha256(body).hexdigest()
        exp_unix = int(self._now()) + self._link_ttl_seconds
        signature = sign_object_upload(self._signing_key, endpoint, content_sha256, exp_unix)
        query: dict[str, str] = {key: value for key, value in params.items() if value is not None}
        query["content_sha256"] = content_sha256
        query["exp"] = str(exp_unix)
        query["sig"] = signature
        url = f"{self._upload_base_url}{endpoint}?{urlencode(query)}"
        headers = {"Content-Type": content_type}
        if self._host_header is not None:
            # Route to the direct origin but keep the public Host so Traefik
            # still maps the request to the app's router.
            headers["Host"] = self._host_header
        response = self._session_for_thread().post(
            url,
            data=body,
            headers=headers,
            timeout=self._upload_timeout(len(body)),
        )
        response.raise_for_status()
        return response.json()

    def _upload_timeout(self, body_bytes: int) -> float:
        """Scale the request timeout with body size.

        The fixed 120 s default is fine for batches and small blobs but too
        short for large voice-memo audio: the app streams the whole body to
        object storage before responding, so a few-hundred-MiB upload routinely
        ran past 120 s and the client aborted it (the server then logged a 499,
        which re-raised and wedged the run). Allow at least
        ``_MIN_UPLOAD_BYTES_PER_SEC`` of throughput on top of the base timeout.
        """

        return max(self._timeout, body_bytes / _MIN_UPLOAD_BYTES_PER_SEC)

    def _post(
        self,
        endpoint: str,
        *,
        body: bytes,
        content_type: str,
        params: Mapping[str, str | None],
    ) -> StoredObjectDict:
        payload = self._signed_post(endpoint, body=body, content_type=content_type, params=params)
        return {
            "storage_backend": str(payload.get("storage_backend", "")),
            "storage_key": str(payload.get("storage_key", "")),
            "storage_file_id": str(payload.get("storage_file_id", "")),
            "storage_url": str(payload.get("storage_url", "")),
        }

    # --- chatgpt session credential ----------------------------------------
    def publish_chatgpt_session(
        self,
        *,
        account: str,
        session_token: str,
        session_key: str = "default",
        source_browser: str = "",
    ) -> Mapping[str, Any]:
        """Publish a captured chatgpt.com web session to the app (Postgres-backed).

        Returns the app's acknowledgement (account/session_key/token_sha256/...);
        the token itself is never echoed back.
        """
        payload = {
            "account": account,
            "session_key": session_key,
            "session_token": session_token,
            "source_browser": source_browser,
        }
        body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return self._signed_post(
            "/ingest/chatgpt/session",
            body=body,
            content_type="application/json",
            params={},
        )

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

    # --- photos (all photo sources share these two endpoints) ---------------
    def upload_photo_file(
        self,
        content: bytes,
        *,
        captured_at: str,
        extension: str,
        content_type: str,
    ) -> StoredObjectDict:
        return self._post(
            "/ingest/photos/file",
            body=content,
            content_type=content_type or "application/octet-stream",
            params={"captured_at": captured_at, "extension": extension, "content_type": content_type},
        )

    def upload_photo_metadata(
        self,
        payload: Mapping[str, object],
        *,
        captured_at: str,
        file_content_sha256: str,
        metadata_dedup_sha256: str,
    ) -> StoredObjectDict:
        return self._post_json(
            "/ingest/photos/metadata",
            payload=payload,
            params={
                "captured_at": captured_at,
                "file_content_sha256": file_content_sha256,
                "metadata_dedup_sha256": metadata_dedup_sha256,
            },
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


def _tailscale_binary() -> str | None:
    """Locate the ``tailscale`` CLI (env override, common installs, then PATH)."""

    override = (os.getenv("PDW_TAILSCALE_BIN") or "").strip()
    if override:
        return override if os.path.isfile(override) else None
    for candidate in (
        "/opt/homebrew/bin/tailscale",
        "/usr/local/bin/tailscale",
        "/usr/bin/tailscale",
        "/Applications/Tailscale.app/Contents/MacOS/Tailscale",
    ):
        if os.path.isfile(candidate):
            return candidate
    return shutil.which("tailscale")


def _tailscale_ipv4(host: str, *, runner: Callable[[list[str]], str] | None = None) -> str | None:
    """Return ``host``'s tailnet IPv4 via ``tailscale ip -4 <host>``, or None."""

    binary = _tailscale_binary()
    if not binary:
        return None
    run = runner or (
        lambda argv: subprocess.run(
            argv, capture_output=True, text=True, timeout=5, check=True
        ).stdout
    )
    try:
        out = run([binary, "ip", "-4", host])
    except Exception:  # noqa: BLE001 - any failure just means "no direct route"
        return None
    for line in out.splitlines():
        ip = line.strip()
        if ip.count(".") == 3 and all(part.isdigit() for part in ip.split(".")):
            return ip
    return None


def _probe_direct_origin(direct_url: str, host_header: str, *, timeout: float = 3.0) -> bool:
    """True when ``direct_url`` answers ``/healthz`` as the app for ``host_header``."""

    try:
        resp = requests.get(
            f"{direct_url.rstrip('/')}/healthz",
            headers={"Host": host_header},
            timeout=timeout,
        )
    except requests.RequestException:
        return False
    return resp.status_code < 400


def resolve_direct_ingest_origin(
    base_url: str,
    *,
    explicit_direct_url: str | None,
    tailscale_host: str | None,
    ipv4_resolver: Callable[[str], str | None] = _tailscale_ipv4,
    probe: Callable[[str, str], bool] = _probe_direct_origin,
    logger: logging.Logger = _logger,
) -> str | None:
    """Pick a same-app origin reachable off the public (Cloudflare) path, if any.

    Prefers an explicit ``PDW_INGEST_DIRECT_URL``; otherwise, when
    ``PDW_INGEST_TAILSCALE_HOST`` names a tailnet node, resolves that node's
    current tailnet IPv4 and targets it over plain HTTP (Tailscale is the
    transport encryption). The candidate is only used when it actually answers
    ``/healthz`` as the app, so an off-tailnet host transparently falls back to
    the public URL. Returns the direct base URL or None.
    """

    host_header = urlsplit(base_url).netloc
    candidate = (explicit_direct_url or "").strip()
    if not candidate and tailscale_host:
        ip = ipv4_resolver(tailscale_host)
        if ip:
            candidate = f"http://{ip}"
    if not candidate:
        return None
    if not probe(candidate, host_header):
        logger.info("Tailscale-direct ingest origin %s not reachable; using %s", candidate, base_url)
        return None
    logger.info(
        "Preferring Tailscale-direct ingest origin %s for %s (bypasses the Cloudflare body-size cap)",
        candidate,
        host_header,
    )
    return candidate


def ingest_client_from_env(
    *,
    now: Callable[[], float] = time.time,
    session: requests.Session | None = None,
    resolve_direct: Callable[..., str | None] = resolve_direct_ingest_origin,
) -> IngestClient:
    """Build an IngestClient from the shared app env.

    The app base URL is the warehouse's main API URL: ``PDW_API_URL`` is the
    canonical name (the same URL pdw uses for queries; ``pdw ingest`` passes it
    and the token down automatically), with ``MCP_BASE_URL`` accepted as an
    alias. The signing key is the app secret token (``PDW_SECRET_TOKEN``), also
    accepted as ``MCP_SECRET_TOKEN``. Folder ids and the Drive credential are
    deliberately NOT read here; those live only in the app.

    Large uploads (voice memos, attachments, media) can exceed Cloudflare's
    100 MiB body cap on the public host. When ``PDW_INGEST_TAILSCALE_HOST`` (a
    tailnet node name, e.g. ``rotom``) or ``PDW_INGEST_DIRECT_URL`` is set and
    actually reachable, uploads are sent straight to that origin instead, which
    lifts the ceiling to the app's own ``PDW_INGEST_MAX_OBJECT_BYTES`` cap.
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
    max_object_bytes = _positive_int_env("PDW_INGEST_MAX_OBJECT_BYTES", DEFAULT_INGEST_MAX_OBJECT_BYTES)
    upload_base_url = resolve_direct(
        base_url,
        explicit_direct_url=(os.getenv("PDW_INGEST_DIRECT_URL") or "").strip() or None,
        tailscale_host=(os.getenv("PDW_INGEST_TAILSCALE_HOST") or "").strip() or None,
    )
    return IngestClient(
        base_url=base_url,
        signing_key=secret.encode("utf-8"),
        now=now,
        session=session,
        upload_base_url=upload_base_url,
        max_object_bytes=max_object_bytes,
    )


def _positive_int_env(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default
