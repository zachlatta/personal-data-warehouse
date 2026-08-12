"""Deterministic text extraction and format classification for attachments.

The agent vision pipeline (``file_attachment_enrichment``) handles images and
PDFs. Everything else in a message attachment table used to fall through it
silently: never selected, never classified, invisible in coverage reports. On
2026-08-12 that was 4,735 iMessage ``.pluginPayloadAttachment`` blobs, 833
videos, and 129 text/vCard files — reported as a 5,700-attachment "gap" that was
really three different things.

This module closes that hole deterministically, with no agent and no model:

* **Text-bearing formats** (vCards, plain text, CSV, HTML, Markdown) are parsed
  into a searchable text block. vCards are the valuable case — 121 of the 129
  iMessage text attachments are contact cards carrying names, phone numbers,
  addresses, and URLs.
* **Formats the warehouse deliberately does not enrich** (app-extension plugin
  payloads, video, archives) get a stable ``unsupported`` classification with a
  human-readable reason, so "we chose not to" is distinguishable from "it broke".

Both are written to the shared ``file_attachment_enrichments`` table under the
deterministic identity (empty ``ai_provider``/``ai_model``/``ai_prompt_version``),
which the timeline adapters already fold into the parent event's search document
— so extracted text becomes searchable with no extra wiring.

Classification is decided from the declared MIME and filename ALONE and never
reads the blob. That is deliberate: the plugin payloads are 1.1 GB and the videos
25 GB, and downloading 26 GB to conclude "not enrichable" would be pure waste.
Only text-bearing candidates, which are tiny, are fetched.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import re
from typing import Any, Callable

from personal_data_warehouse.file_attachment_enrichment import (
    AMBIGUOUS_MIME_TYPES,
    IMAGE_EXTENSIONS,
    IMAGE_MIME_TYPES,
)
from personal_data_warehouse.google_drive_source_sync import extract_docx_text

# The deterministic identity in file_attachment_enrichments. Gmail's own
# extraction already uses this triple, and both the timeline adapters and the
# vision pass's `det` join key on it.
DETERMINISTIC_PROVIDER = ""
DETERMINISTIC_MODEL = ""
DETERMINISTIC_PROMPT_VERSION = ""

STATUS_OK = "ok"
STATUS_EMPTY = "empty"
STATUS_UNSUPPORTED = "unsupported"

DEFAULT_TEXT_MAX_CHARS = 20_000
# Rows ACTED ON per run (extracted or classified).
DEFAULT_TEXT_EXTRACTION_BATCH_SIZE = 500
# Rows examined per run. Must comfortably exceed the count of vision-owned
# attachments, which share the "no deterministic row" condition and are skipped:
# in production 15,575 rows qualify for the scan while only 5,708 are actionable.
DEFAULT_TEXT_EXTRACTION_SCAN_LIMIT = 50_000

VCARD_MIME_TYPES = ("text/vcard", "text/x-vcard", "text/directory", "text/x-vlocation")
VCARD_EXTENSIONS = (".vcf", ".vcard")
PLAIN_TEXT_EXTENSIONS = (".txt", ".text", ".md", ".markdown", ".csv", ".tsv", ".log", ".json")
HTML_EXTENSIONS = (".html", ".htm")

# Formats classified without ever reading the bytes. Each entry is
# (matcher-friendly token, human reason) — the reason is stored on the row so a
# later audit can tell a deliberate choice from a silent omission.
UNSUPPORTED_EXTENSIONS: dict[str, str] = {
    ".pluginpayloadattachment": (
        "iMessage app-extension payload (link preview); the shared URL is already "
        "indexed from the message body"
    ),
    ".alfredworkflow": "Alfred workflow bundle; no extractable document text",
}
UNSUPPORTED_MIME_PREFIXES: dict[str, str] = {
    "video/": "video; the vision pass extracts stills only and video is not yet supported",
    "audio/": "audio; handled by the separate audio transcription pipeline",
}
UNSUPPORTED_MIME_TYPES: dict[str, str] = {
    "application/zip": "archive; contents are not unpacked for indexing",
    "application/x-plist": "binary property list payload; no document text",
    "application/gzip": "archive; contents are not unpacked for indexing",
}

# Formats owned by the agent vision pass. A deterministic row must not claim
# them, or it would race the pipeline that actually produces their text.
#
# Derived from the vision pass's OWN constants rather than a hand-copied list.
# A prefix test like "image/*" would be wrong in both directions: image/svg+xml
# is really XML text, and image/heic-sequence is a burst container the vision
# pass cannot decode — handing either to that pipeline drops them silently,
# which is the exact failure mode this module exists to end.
VISION_OWNED_MIME_TYPES = (*IMAGE_MIME_TYPES, "application/pdf")
VISION_OWNED_EXTENSIONS = (*IMAGE_EXTENSIONS, ".pdf")

SVG_MIME_TYPES = ("image/svg+xml",)
SVG_EXTENSIONS = (".svg",)
DOCX_MIME_TYPES = ("application/vnd.openxmlformats-officedocument.wordprocessingml.document",)
DOCX_EXTENSIONS = (".docx",)

_VCARD_FIELD_LABELS = {
    "FN": "Name",
    "N": "Name",
    "NICKNAME": "Nickname",
    "ORG": "Organization",
    "TITLE": "Title",
    "ROLE": "Role",
    "TEL": "Phone",
    "EMAIL": "Email",
    "ADR": "Address",
    "URL": "URL",
    "NOTE": "Note",
    "BDAY": "Birthday",
    "IMPP": "Messaging",
    "X-SOCIALPROFILE": "Social",
    "GEO": "Location",
}
# Structural/binary vCard properties that carry no searchable meaning.
_VCARD_SKIP_FIELDS = frozenset({"BEGIN", "END", "VERSION", "PRODID", "REV", "UID", "PHOTO", "LOGO", "KEY"})

_SCRIPT_STYLE_RE = re.compile(r"<(script|style)\b.*?</\1>", re.IGNORECASE | re.DOTALL)
_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"[ \t\r\f\v]+")
_BLANK_LINES_RE = re.compile(r"\n{3,}")


@dataclass(frozen=True)
class AttachmentTextPlan:
    """What to do with one attachment, decided from metadata alone."""

    needs_bytes: bool
    status: str = ""
    reason: str = ""


@dataclass(frozen=True)
class TextExtractionSource:
    """Describes one attachment table the deterministic pass can scan."""

    name: str
    label: str
    table: str
    stored_predicate: str
    sha_column: str = "content_sha256"
    filename_column: str = "filename"
    mime_column: str = "mime_type"
    size_column: str = "size_bytes"
    order_column: str = "created_at"


APPLE_MESSAGES_TEXT_SOURCE = TextExtractionSource(
    name="apple_messages",
    label="iMessage attachment",
    table="apple_message_attachments",
    stored_predicate="a.is_missing = 0 AND a.content_sha256 <> ''",
    size_column="size_bytes",
    order_column="created_at",
)

WHATSAPP_TEXT_SOURCE = TextExtractionSource(
    name="whatsapp",
    label="WhatsApp media attachment",
    table="whatsapp_media_items",
    stored_predicate="a.is_missing = 0 AND a.content_sha256 <> ''",
    size_column="size_bytes",
    order_column="message_at",
)


@dataclass(frozen=True)
class AttachmentTextExtractionSummary:
    seen: int
    extracted: int
    classified: int
    empty: int
    failed: int


def _extension(filename: str) -> str:
    return Path(filename.lower().strip()).suffix


def _is_vision_owned(mime: str, extension: str) -> bool:
    return mime in VISION_OWNED_MIME_TYPES or extension in VISION_OWNED_EXTENSIONS


def _is_text_bearing(mime: str, extension: str) -> bool:
    if mime in VCARD_MIME_TYPES or extension in VCARD_EXTENSIONS:
        return True
    if mime in SVG_MIME_TYPES or extension in SVG_EXTENSIONS:
        return True
    if mime in DOCX_MIME_TYPES or extension in DOCX_EXTENSIONS:
        return True
    if extension in PLAIN_TEXT_EXTENSIONS or extension in HTML_EXTENSIONS:
        return True
    # text/* is text by definition, but only after the vision- and
    # explicitly-unsupported checks have had their say.
    return mime.startswith("text/")


def attachment_text_plan(*, mime_type: str, filename: str) -> AttachmentTextPlan | None:
    """Decide what to do with an attachment from its metadata alone.

    Returns ``None`` when the attachment belongs to another pipeline (the agent
    vision pass) or is not recognized at all, so this pass leaves it untouched
    rather than staking a claim it cannot honor.
    """
    mime = (mime_type or "").strip().lower()
    name = (filename or "").strip()
    extension = _extension(name)

    if _is_vision_owned(mime, extension):
        return None

    # Metadata asserts nothing at all (no usable MIME, no extension). The vision
    # pass admits exactly this shape and identifies it from its bytes, so leave
    # it alone rather than classifying a real image as unsupported sight-unseen —
    # iMessage's extension-less "GroupPhotoImage"/"BrandLogoImage" blobs are
    # genuine images that arrive looking like this.
    if mime in AMBIGUOUS_MIME_TYPES and not extension:
        return None

    reason = UNSUPPORTED_EXTENSIONS.get(extension)
    if reason is None:
        reason = UNSUPPORTED_MIME_TYPES.get(mime)
    if reason is None:
        for prefix, prefix_reason in UNSUPPORTED_MIME_PREFIXES.items():
            if mime.startswith(prefix):
                reason = prefix_reason
                break
    if reason is not None:
        return AttachmentTextPlan(needs_bytes=False, status=STATUS_UNSUPPORTED, reason=reason)

    if _is_text_bearing(mime, extension):
        return AttachmentTextPlan(needs_bytes=True)

    # Catch-all. Nothing may fall through unrecorded: an attachment with no row
    # at all is indistinguishable from one the pipeline forgot, which is how
    # 5,700 iMessage attachments sat unexplained for months. The reason names the
    # observed shape so a future format addition can target exactly these rows.
    return AttachmentTextPlan(
        needs_bytes=False,
        status=STATUS_UNSUPPORTED,
        reason=(
            f"unrecognized format (mime {mime or 'none'}, extension {extension or 'none'}); "
            "no extractor claims it"
        ),
    )


def vcard_to_text(raw: str) -> str:
    """Flatten one or more vCards into a readable, searchable block.

    Contact cards are the highest-value deterministic content in iMessage
    attachments: names, phone numbers, emails, addresses, and URLs that appear
    nowhere else in the message. Structural properties (BEGIN/VERSION/PHOTO) are
    dropped so they cannot pollute the search document.
    """
    lines: list[str] = []
    # RFC 6350 line folding: a leading space or tab continues the previous line.
    for physical_line in raw.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        if physical_line[:1] in (" ", "\t") and lines:
            lines[-1] += physical_line[1:]
        else:
            lines.append(physical_line)

    seen: set[str] = set()
    out: list[str] = []
    for line in lines:
        if ":" not in line:
            continue
        prefix, _, value = line.partition(":")
        # Strip vCard parameters: "TEL;type=CELL" -> "TEL"; also drop any
        # group prefix ("item1.TEL" -> "TEL").
        field = prefix.split(";", 1)[0].split(".")[-1].strip().upper()
        if not field or field in _VCARD_SKIP_FIELDS:
            continue
        # Structured values are semicolon-separated component lists.
        parts = [part.strip() for part in value.split(";") if part.strip()]
        cleaned = ", ".join(parts)
        if not cleaned:
            continue
        label = _VCARD_FIELD_LABELS.get(field)
        rendered = f"{label}: {cleaned}" if label else cleaned
        if rendered not in seen:
            seen.add(rendered)
            out.append(rendered)
    return "\n".join(out)


def html_to_text(raw: str) -> str:
    """Strip markup so the words, not the tags, land in the search document."""
    without_code = _SCRIPT_STYLE_RE.sub(" ", raw)
    without_tags = _TAG_RE.sub(" ", without_code)
    unescaped = (
        without_tags.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", '"')
        .replace("&#39;", "'")
    )
    return unescaped


def _normalize_whitespace(text: str) -> str:
    collapsed = _WHITESPACE_RE.sub(" ", text)
    lines = [line.strip() for line in collapsed.split("\n")]
    return _BLANK_LINES_RE.sub("\n\n", "\n".join(lines)).strip()


def extract_attachment_text(
    *,
    content: bytes,
    mime_type: str,
    filename: str,
    max_chars: int = DEFAULT_TEXT_MAX_CHARS,
) -> tuple[str, str]:
    """Turn text-bearing attachment bytes into ``(status, text)``.

    Decoding uses ``errors="replace"`` rather than failing: real attachments
    carry mixed and broken encodings, and a decode error must not permanently
    lose otherwise readable content.
    """
    mime = (mime_type or "").strip().lower()
    extension = _extension(filename or "")
    decoded = content.decode("utf-8", errors="replace")

    if mime in DOCX_MIME_TYPES or extension in DOCX_EXTENSIONS:
        try:
            text = extract_docx_text(content)
        except (ValueError, KeyError, OSError):
            # A DOCX that is not a valid OOXML package will never become one:
            # terminal, not a transient failure to retry forever.
            return STATUS_UNSUPPORTED, ""
    elif mime in VCARD_MIME_TYPES or extension in VCARD_EXTENSIONS or decoded.lstrip().startswith("BEGIN:VCARD"):
        text = vcard_to_text(decoded)
    elif (
        mime == "text/html"
        or extension in HTML_EXTENSIONS
        or mime in SVG_MIME_TYPES
        or extension in SVG_EXTENSIONS
    ):
        # SVG is XML: the same tag-stripping that makes HTML searchable also
        # recovers the <text> labels that carry an SVG's only readable content.
        text = html_to_text(decoded)
    else:
        text = decoded

    text = _normalize_whitespace(text)
    if not text:
        return STATUS_EMPTY, ""
    if len(text) > max_chars:
        text = text[:max_chars]
    return STATUS_OK, text


def load_text_extraction_candidates(
    warehouse,
    *,
    source: TextExtractionSource,
    limit: int | None,
) -> list[dict[str, Any]]:
    """Stored attachments with no deterministic extraction row yet.

    Deliberately unfiltered by format: :func:`attachment_text_plan` decides per
    row, in Python, where the format tables live. Encoding that ruleset twice
    (once in SQL, once in Python) is exactly how the vision pass's gates drifted
    out of sync with reality in the first place.

    ``limit`` therefore bounds the METADATA scan, not the work — most rows here
    belong to the vision pass and are skipped. The runner applies its own bound
    to the rows it actually acts on. Sizing matters: in production 15,575 rows
    lack a deterministic row while only 5,708 need one, so a scan bound below
    ~10k would return nothing but vision-owned rows and the pass would spin
    without ever reaching its work.
    """
    columns = (
        "account",
        "content_sha256",
        "filename",
        "mime_type",
        "size",
        "storage_backend",
        "storage_key",
        "storage_file_id",
        "storage_url",
    )
    limit_sql = "LIMIT %s" if limit is not None and limit > 0 else ""
    params: list[Any] = []
    if limit_sql:
        params.append(int(limit))
    rows = warehouse._query(
        f"""
        SELECT DISTINCT ON (a.{source.sha_column})
               a.account,
               a.{source.sha_column} AS content_sha256,
               a.{source.filename_column} AS filename,
               a.{source.mime_column} AS mime_type,
               a.{source.size_column} AS size,
               a.storage_backend, a.storage_key, a.storage_file_id, a.storage_url
        FROM @{source.table} a
        WHERE {source.stored_predicate}
          AND NOT EXISTS (
              SELECT 1
              FROM file_attachment_enrichments det
              -- The deterministic identity is the empty provider/model/prompt
              -- triple, matching Gmail's existing extraction rows and the `det`
              -- join in the vision candidate query.
              WHERE det.content_sha256 = a.{source.sha_column}
                AND det.ai_provider = ''
                AND det.ai_model = ''
                AND det.ai_prompt_version = ''
          )
        ORDER BY a.{source.sha_column}, a.{source.order_column} DESC
        {limit_sql}
        """,
        tuple(params),
    )
    return [dict(zip(columns, row, strict=True)) for row in rows]


class AttachmentTextExtractionRunner:
    """Scans one source, extracting text or recording a stable classification."""

    def __init__(
        self,
        *,
        source: TextExtractionSource,
        warehouse,
        object_store_factory: Callable[[str], Any],
        logger,
        text_max_chars: int = DEFAULT_TEXT_MAX_CHARS,
        scan_limit: int = DEFAULT_TEXT_EXTRACTION_SCAN_LIMIT,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._source = source
        self._warehouse = warehouse
        self._object_store_factory = object_store_factory
        self._logger = logger
        self._text_max_chars = text_max_chars
        self._scan_limit = scan_limit
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._object_stores: dict[str, Any] = {}

    def sync(self, *, limit: int | None) -> AttachmentTextExtractionSummary:
        self._warehouse.ensure_file_attachment_enrichment_tables()
        candidates = load_text_extraction_candidates(
            self._warehouse, source=self._source, limit=self._scan_limit
        )
        if self._scan_limit and len(candidates) >= self._scan_limit:
            # Never let a bound truncate silently: a full scan window means there
            # may be actionable rows this run cannot see.
            self._logger.warning(
                "%s text extraction scanned the full %s-row window; "
                "raise the scan limit if the backlog stops draining",
                self._source.label,
                self._scan_limit,
            )
        extracted = classified = empty = failed = 0
        acted = 0
        for candidate in candidates:
            if limit is not None and limit > 0 and acted >= limit:
                break
            plan = attachment_text_plan(
                mime_type=str(candidate.get("mime_type", "")),
                filename=str(candidate.get("filename", "")),
            )
            if plan is None:
                # Belongs to the vision pass. Skipped WITHOUT counting toward the
                # work bound: in production these outnumber the actionable rows
                # roughly 2:1, so counting them would let a batch fill entirely
                # with skips and make no progress, run after run.
                continue
            acted += 1
            if not plan.needs_bytes:
                self._write(candidate, status=plan.status, text="", error=plan.reason)
                classified += 1
                continue
            try:
                content = self._object_store(str(candidate.get("account", ""))).get_object(
                    _storage_ref(candidate)
                )
            except Exception as exc:  # noqa: BLE001 - transport failures are retryable
                # No row is written: a Drive outage must not be recorded as a
                # permanent property of the attachment. The next run retries it.
                failed += 1
                self._logger.warning(
                    "could not read %s %s: %s",
                    self._source.label,
                    candidate.get("content_sha256", ""),
                    exc,
                )
                continue
            status, text = extract_attachment_text(
                content=content,
                mime_type=str(candidate.get("mime_type", "")),
                filename=str(candidate.get("filename", "")),
                max_chars=self._text_max_chars,
            )
            self._write(candidate, status=status, text=text, error="")
            if status == STATUS_OK:
                extracted += 1
            else:
                empty += 1

        self._logger.info(
            "%s text extraction: saw %s, extracted %s, classified %s, empty %s, failed %s",
            self._source.label,
            len(candidates),
            extracted,
            classified,
            empty,
            failed,
        )
        return AttachmentTextExtractionSummary(
            seen=len(candidates),
            extracted=extracted,
            classified=classified,
            empty=empty,
            failed=failed,
        )

    def _write(self, candidate: Mapping[str, Any], *, status: str, text: str, error: str) -> None:
        updated_at = self._now()
        self._warehouse.insert_attachment_enrichments(
            [
                {
                    "content_sha256": str(candidate.get("content_sha256", "")),
                    "ai_provider": DETERMINISTIC_PROVIDER,
                    "ai_model": DETERMINISTIC_MODEL,
                    "ai_prompt_version": DETERMINISTIC_PROMPT_VERSION,
                    "text": text,
                    "text_extraction_status": status,
                    "text_extraction_error": error[:2000],
                    "ai_base_url": "",
                    "ai_prompt_sha256": "",
                    "ai_prompt": "",
                    "ai_source_status": "",
                    "ai_elapsed_ms": 0,
                    "ai_processed_at": updated_at,
                    "updated_at": updated_at,
                    "sync_version": int(updated_at.timestamp() * 1000),
                }
            ]
        )

    def _object_store(self, account: str):
        store = self._object_stores.get(account)
        if store is None:
            store = self._object_store_factory(account)
            self._object_stores[account] = store
        return store


def _storage_ref(candidate: Mapping[str, Any]) -> dict[str, str]:
    return {
        "storage_backend": str(candidate.get("storage_backend", "")),
        "storage_key": str(candidate.get("storage_key", "")),
        "storage_file_id": str(candidate.get("storage_file_id", "")),
        "storage_url": str(candidate.get("storage_url", "")),
    }
