"""Perceptual fingerprints for Slack image files.

The photos pipeline already solved "is this the same picture?": a 256-bit dhash
in ``derived_enrichment.media_fingerprints``, keyed by content sha so *any*
blob in the warehouse can be fingerprinted once. That table's docstring
anticipated this exact caller ("a future linker may fingerprint message/mail
attachments into the same table"), so this module reuses it rather than
growing a parallel hashing scheme that could drift out of agreement.

The only genuinely new state is the **link**: ``base_slack.files`` has no
content sha, because a sha is only knowable after downloading the bytes.
``derived_slack.file_fingerprints`` records file -> sha, plus the bookkeeping
that makes walking a 552 GB corpus survivable.

Two deliberate choices:

* **The bytes are discarded.** ~905k live Slack images total ~552 GB. Caching
  them to answer a rare "who sent this?" would cost ~3000x what the answer
  needs; the fingerprint is ~200 bytes per file. Once a lookup names a file,
  its bytes are one ``get_object`` call away, so nothing is lost.
* **The table is the cursor.** No row = never attempted; ``next_attempt_at`` in
  the future = backed off. A run takes a bounded newest-first slice, so an
  interrupted run loses only the in-flight file and the next run resumes.
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from personal_data_warehouse.photo_fingerprint import HASH_VERSION, ImageTooLargeError, compute_dhash

__all__ = [
    "AppObjectFetcher",
    "SlackFileFetchError",
    "SlackFileMissingError",
    "SlackFileRateLimitedError",
    "SlackFileRef",
    "SlackFileTooLargeError",
    "DEFAULT_LIMIT",
    "DEFAULT_MAX_PIXELS",
    "MAX_ATTEMPTS",
    "NO_RETRY",
    "STATUS_FAILED",
    "STATUS_MISSING",
    "STATUS_OK",
    "STATUS_TOO_LARGE",
    "STATUS_UNDECODABLE",
    "SlackFileFingerprintRunner",
    "SlackFileFingerprintSummary",
    "TERMINAL_STATUSES",
]

STATUS_OK = "ok"
STATUS_UNDECODABLE = "undecodable"
STATUS_TOO_LARGE = "too_large"
STATUS_MISSING = "missing"
STATUS_FAILED = "failed"

#: Statuses that will never change on their own, so they are never re-fetched.
#: ``missing`` and ``failed`` stay retryable (with backoff) because Slack
#: outages and transient scope problems do resolve.
TERMINAL_STATUSES = (STATUS_OK, STATUS_UNDECODABLE, STATUS_TOO_LARGE)

DEFAULT_LIMIT = 500
MAX_ATTEMPTS = 5

#: Slack carries print artwork, not just camera photos. The file that motivated
#: this pipeline is 420,750,000 pixels (11x17 inches at 1500 DPI), well past
#: Pillow's ~89 MP default guard. 512 MP covers print posters with headroom
#: while still bounding the decode; beyond it a file is honestly recorded as
#: too_large rather than silently mislabelled corrupt.
DEFAULT_MAX_PIXELS = 512_000_000
_BASE_RETRY = timedelta(minutes=30)
_MAX_RETRY = timedelta(days=7)

#: Every warehouse column is NOT NULL, so "no retry scheduled" is the epoch
#: sentinel rather than NULL — the same convention the ledger tables use.
NO_RETRY = datetime(1970, 1, 1, tzinfo=UTC)


# --- fetching bytes ---------------------------------------------------------
#
# There is exactly one Slack-file-fetch implementation and it is not here: the
# app already resolves a Slack file id through files.info across every
# configured workspace token, downloads url_private, and rejects Slack's
# 200-with-an-HTML-login-page answer (app/internal/objectstore/slack.go, served
# by the get_object tool). The app also already holds the tokens.
#
# So this backfill is a *client* of that, not a second copy of it. Python never
# sees a Slack credential, and a fix to Slack file resolution lands in one
# place for every caller.


class SlackFileFetchError(Exception):
    """Any failure to turn a file row into bytes."""


class SlackFileMissingError(SlackFileFetchError):
    """The app reports the file no longer exists."""


class SlackFileTooLargeError(SlackFileFetchError):
    """Bigger than this run is willing to download."""


class SlackFileRateLimitedError(SlackFileFetchError):
    def __init__(self, message: str, *, retry_after: int = 60) -> None:
        super().__init__(message)
        self.retry_after = retry_after


#: One pathological upload must not eat a whole run's time and memory.
DEFAULT_MAX_FETCH_BYTES = int(os.getenv("SLACK_FILE_FETCH_MAX_BYTES", str(64 * 1024 * 1024)))
DEFAULT_FETCH_TIMEOUT_SECONDS = float(os.getenv("SLACK_FILE_FETCH_TIMEOUT_SECONDS", "180"))

_CHUNK_BYTES = 256 * 1024
_IMAGE_MAGIC: tuple[bytes, ...] = (
    b"\x89PNG\r\n\x1a\n", b"\xff\xd8\xff", b"GIF87a", b"GIF89a",
    b"BM", b"II*\x00", b"MM\x00*", b"\x00\x00\x01\x00",
)


def _looks_like_an_image(head: bytes) -> bool:
    if any(head.startswith(magic) for magic in _IMAGE_MAGIC):
        return True
    if head[:4] == b"RIFF" and head[8:12] == b"WEBP":
        return True
    return head[4:8] == b"ftyp"


@dataclass(frozen=True)
class SlackFileRef:
    """The parts of a candidate row needed to ask the app for its bytes."""

    account: str
    team_id: str
    file_id: str
    mimetype: str = ""
    name: str = ""
    size: int = 0

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "SlackFileRef":
        return cls(
            account=str(row.get("account") or ""),
            team_id=str(row.get("team_id") or ""),
            file_id=str(row.get("file_id") or ""),
            mimetype=str(row.get("mimetype") or ""),
            name=str(row.get("name") or ""),
            size=int(row.get("size") or 0),
        )


class AppObjectFetcher:
    """Fetch a Slack file's bytes through the app's get_object tool."""

    def __init__(
        self,
        *,
        base_url: str,
        secret_token: str,
        session: Any | None = None,
        client_name: str = "pdw",
        max_bytes: int = DEFAULT_MAX_FETCH_BYTES,
        timeout: float = DEFAULT_FETCH_TIMEOUT_SECONDS,
    ) -> None:
        if session is None:
            import requests

            session = requests.Session()
        self._session = session
        self._base_url = base_url.rstrip("/")
        self._secret_token = secret_token
        self._client_name = client_name
        self._max_bytes = int(max_bytes)
        self._timeout = timeout

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._client_name}:{self._secret_token}",
            # Cloudflare 403s default urllib-ish agents in front of the app.
            "User-Agent": "personal-data-warehouse-slack-fingerprints/1",
        }

    def fetch(self, ref: SlackFileRef) -> bytes:
        # The row already knows the size; do not spend a request to learn it.
        if ref.size and ref.size > self._max_bytes:
            raise SlackFileTooLargeError(
                f"file {ref.file_id} is {ref.size} bytes, over the {self._max_bytes} ceiling"
            )

        response = self._session.post(
            f"{self._base_url}/api/tools/get_object",
            json={"storage_file_id": ref.file_id},
            headers=self._headers(),
            timeout=self._timeout,
        )
        response.raise_for_status()
        data = (response.json() or {}).get("data") or {}

        error = str(data.get("error") or "")
        if error:
            # The app surfaces Slack's own error text; a 429 in it means the
            # workspace is throttling and the whole slice should stop.
            if "429" in error or "rate" in error.lower():
                raise SlackFileRateLimitedError(f"Slack rate limited {ref.file_id}: {error}")
            if "not found" in error.lower() or "deleted" in error.lower():
                raise SlackFileMissingError(f"{ref.file_id}: {error}")
            raise SlackFileFetchError(f"{ref.file_id}: {error}")
        if data.get("exists") is False:
            raise SlackFileMissingError(f"the app reports Slack file {ref.file_id} does not exist")

        declared = data.get("size_bytes")
        if isinstance(declared, int) and declared > self._max_bytes:
            raise SlackFileTooLargeError(
                f"file {ref.file_id} is {declared} bytes, over the {self._max_bytes} ceiling"
            )
        download_url = str(data.get("download_url") or "")
        if not download_url:
            raise SlackFileFetchError(f"get_object returned no download_url for {ref.file_id}")

        return self._download(ref, download_url)

    def _download(self, ref: SlackFileRef, download_url: str) -> bytes:
        # The signed URL needs no auth, so no credential leaves this process.
        response = self._session.get(download_url, headers={
            "User-Agent": "personal-data-warehouse-slack-fingerprints/1",
        }, stream=True, timeout=self._timeout)
        try:
            status = int(getattr(response, "status_code", 0))
            if status == 404:
                raise SlackFileMissingError(f"signed download for {ref.file_id} returned 404")
            if status >= 400:
                raise SlackFileFetchError(f"HTTP {status} downloading {ref.file_id}")
            chunks: list[bytes] = []
            total = 0
            for chunk in response.iter_content(chunk_size=_CHUNK_BYTES):
                if not chunk:
                    continue
                total += len(chunk)
                if total > self._max_bytes:
                    raise SlackFileTooLargeError(
                        f"file {ref.file_id} exceeded the {self._max_bytes} byte ceiling mid-stream"
                    )
                chunks.append(chunk)
            content = b"".join(chunks)
            if not content:
                raise SlackFileFetchError(f"empty body for {ref.file_id}")
            if not _looks_like_an_image(content[:16]):
                raise SlackFileFetchError(
                    f"body for {ref.file_id} is not a recognized image container"
                )
            return content
        finally:
            close = getattr(response, "close", None)
            if callable(close):
                close()


@dataclass
class SlackFileFingerprintSummary:
    candidates: int = 0
    fingerprinted: int = 0
    undecodable: int = 0
    too_large: int = 0
    missing: int = 0
    failed: int = 0
    bytes_downloaded: int = 0
    rate_limited: bool = False
    stopped_for_time: bool = False

    @property
    def attempted(self) -> int:
        return self.fingerprinted + self.undecodable + self.too_large + self.missing + self.failed


def _backoff(prior_attempts: int) -> timedelta:
    delay = _BASE_RETRY * (2 ** max(0, int(prior_attempts)))
    return min(delay, _MAX_RETRY)


class SlackFileFingerprintRunner:
    def __init__(
        self,
        *,
        warehouse,
        fetcher,
        logger,
        now=None,
        sleep=None,
        limit: int = DEFAULT_LIMIT,
        hash_version: str = HASH_VERSION,
        max_run_seconds: float | None = None,
        max_pixels: int = DEFAULT_MAX_PIXELS,
    ) -> None:
        self._warehouse = warehouse
        self._fetcher = fetcher
        self._logger = logger
        self._now = now or (lambda: datetime.now(tz=UTC))
        if sleep is None:
            import time

            sleep = time.sleep
        self._sleep = sleep
        self._limit = int(limit)
        self._hash_version = hash_version
        self._max_run_seconds = max_run_seconds
        self._max_pixels = int(max_pixels)

    def run(self) -> SlackFileFingerprintSummary:
        self._warehouse.ensure_slack_file_fingerprint_tables()
        started = self._now()
        sync_version = int(started.timestamp() * 1_000_000)
        summary = SlackFileFingerprintSummary()

        candidates = list(
            self._warehouse.slack_file_fingerprint_candidates(limit=self._limit, now=started)
        )
        summary.candidates = len(candidates)

        for row in candidates:
            now = self._now()
            if (
                self._max_run_seconds is not None
                and (now - started).total_seconds() >= self._max_run_seconds
            ):
                summary.stopped_for_time = True
                break

            ref = SlackFileRef.from_row(row)
            prior_attempts = int(row.get("attempts") or 0)
            try:
                content = self._fetcher.fetch(ref)
            except SlackFileRateLimitedError as exc:
                # End the slice. The file keeps its retry budget: it never got
                # a fair try, and the next run picks it up unchanged.
                summary.rate_limited = True
                self._logger.warning(
                    "Slack rate limited file fingerprinting; stopping this run (retry after %ss)",
                    getattr(exc, "retry_after", 60),
                )
                break
            except SlackFileTooLargeError as exc:
                summary.too_large += 1
                self._record(row, now, sync_version, status=STATUS_TOO_LARGE, error=exc, prior_attempts=prior_attempts)
                continue
            except SlackFileMissingError as exc:
                summary.missing += 1
                self._record(row, now, sync_version, status=STATUS_MISSING, error=exc, prior_attempts=prior_attempts)
                continue
            except SlackFileFetchError as exc:
                summary.failed += 1
                self._logger.warning("Could not fetch Slack file %s: %s", ref.file_id, exc)
                self._record(row, now, sync_version, status=STATUS_FAILED, error=exc, prior_attempts=prior_attempts)
                continue

            summary.bytes_downloaded += len(content)
            content_sha256 = hashlib.sha256(content).hexdigest()
            try:
                fingerprint = compute_dhash(content, max_pixels=self._max_pixels)
            except ImageTooLargeError as exc:
                # A bounded-resource decision, not corrupt bytes. Terminal, but
                # recorded distinctly so the reason is visible in the table.
                summary.too_large += 1
                self._logger.warning(
                    "Slack file %s has too many pixels to fingerprint: %s", ref.file_id, exc
                )
                self._record(
                    row,
                    now,
                    sync_version,
                    status=STATUS_TOO_LARGE,
                    error=exc,
                    prior_attempts=prior_attempts,
                )
                continue
            except Exception as exc:  # noqa: BLE001 - undecodable is a classification
                summary.undecodable += 1
                self._logger.warning(
                    "Could not fingerprint Slack file %s (%s): %s", ref.file_id, ref.name, exc
                )
                self._record(
                    row,
                    now,
                    sync_version,
                    status=STATUS_UNDECODABLE,
                    error=exc,
                    prior_attempts=prior_attempts,
                    content_sha256=content_sha256,
                )
                continue

            # Upsert on (content_sha256, hash_version): two Slack files with
            # identical bytes collapse to one fingerprint row.
            self._warehouse.insert_media_fingerprints(
                [
                    {
                        "content_sha256": content_sha256,
                        "hash_version": self._hash_version,
                        "dhash": fingerprint.dhash,
                        "width": fingerprint.width,
                        "height": fingerprint.height,
                        "created_at": now,
                        "sync_version": sync_version,
                    }
                ]
            )
            summary.fingerprinted += 1
            self._record(
                row,
                now,
                sync_version,
                status=STATUS_OK,
                error=None,
                prior_attempts=prior_attempts,
                content_sha256=content_sha256,
                fetched_bytes=len(content),
            )

        self._logger.info(
            "Slack file fingerprints: %s candidates, %s fingerprinted, %s undecodable, "
            "%s too large, %s missing, %s failed, %.1f MB downloaded%s",
            summary.candidates,
            summary.fingerprinted,
            summary.undecodable,
            summary.too_large,
            summary.missing,
            summary.failed,
            summary.bytes_downloaded / 1_048_576,
            " (rate limited)" if summary.rate_limited else "",
        )
        return summary

    def _record(
        self,
        row: Mapping[str, Any],
        now: datetime,
        sync_version: int,
        *,
        status: str,
        error: Exception | None,
        prior_attempts: int,
        content_sha256: str = "",
        fetched_bytes: int = 0,
    ) -> None:
        terminal = status in TERMINAL_STATUSES
        attempts = prior_attempts + 1
        self._warehouse.upsert_slack_file_fingerprints(
            [
                {
                    "account": str(row.get("account") or ""),
                    "team_id": str(row.get("team_id") or ""),
                    "file_id": str(row.get("file_id") or ""),
                    "content_sha256": content_sha256,
                    "hash_version": self._hash_version if content_sha256 else "",
                    "status": status,
                    "attempts": attempts,
                    "fetched_bytes": int(fetched_bytes),
                    # Errors can quote a URL or a header; the fetcher redacts
                    # tokens before they get here, and this column is readable
                    # by the query role.
                    "last_error": "" if error is None else str(error)[:1000],
                    "last_attempt_at": now,
                    "next_attempt_at": NO_RETRY if terminal else now + _backoff(prior_attempts),
                    "created_at": now,
                    "updated_at": now,
                    "sync_version": sync_version,
                }
            ]
        )
