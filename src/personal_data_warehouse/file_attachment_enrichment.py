"""Agent-container vision enrichment for stored file attachments.

A single source-agnostic pipeline scans a source's attachment table for image
(and scanned-PDF) blobs that have no useful searchable text yet, runs each
through the sandboxed agent container (codex/claude CLI), and upserts the
structured result into the shared ``file_attachment_enrichments`` table under
its own ``ai_provider``/``ai_model``/``ai_prompt_version`` identity. Timeline
adapters fold those enrichment rows into the parent event's search document,
which is what feeds ``search_text()``.

Each source (Gmail attachments, WhatsApp media, …) is described by a
:class:`FileEnrichmentSource` that names its candidate table and the columns the
generic candidate query reads. The runner, image preparation, agent prompt,
validation, and text composition are entirely shared.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
from io import BytesIO
from pathlib import Path
import shutil
import subprocess
import tempfile
from typing import Any, Callable

import pillow_heif
from PIL import Image, ImageFile, ImageOps, UnidentifiedImageError

from personal_data_warehouse.agent_runner import (
    DEFAULT_AGENT_INPUTS_DIR_NAME,
    AgentRunRequest,
    AgentRunResult,
    agent_run_event_rows,
    agent_run_row,
    agent_run_tool_call_rows,
)

# Many real email/WhatsApp images arrive slightly truncated (a few trailing
# bytes lost in transit or storage). Without this, PIL raises "image file is
# truncated" and the attachment can never be enriched even though the bulk of
# the picture is intact and readable. Allowing truncated loads lets PIL decode
# the available scanlines (gray-filling any missing tail) so the agent can still
# extract whatever text/content is present.
ImageFile.LOAD_TRUNCATED_IMAGES = True

# Registers a PIL plugin so Image.open() can decode HEIC/HEIF, the default
# iPhone camera format. Without this, iMessage's large HEIC majority is
# structurally excluded from the vision pipeline (PIL raises
# UnidentifiedImageError on the raw bytes).
pillow_heif.register_heif_opener()

ENRICHMENT_TABLE = "file_attachment_enrichments"

DEFAULT_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS = 3
# Only errors within this rolling window count toward the per-attachment retry
# cap. Without it, errors accumulate forever, so attachments that exhausted
# their attempts on a since-fixed bug (e.g. a harness "unable to locate image"
# failure) would stay permanently excluded even after the bug is gone. The
# window lets those attachments re-enter the candidate pool once the stale
# failures age out. Set the window to 0 to count every historical error.
DEFAULT_ATTACHMENT_ENRICHMENT_ERROR_WINDOW_DAYS = 14

STATUS_OK = "agent_ok"
STATUS_NOT_USEFUL = "agent_not_useful"
STATUS_ERROR = "agent_error"
# A deterministic preparation failure: the stored bytes could not be turned into
# an image at all (undecodable/corrupt image, unrenderable PDF). Distinct from
# STATUS_ERROR (a transient agent-run failure) because the failure is permanent
# for a content-addressed blob — the same bytes always fail the same way — so it
# must not be retried on every run the way the recycling agent_error rows were.
STATUS_UNREADABLE = "agent_unreadable"
COMPLETED_STATUSES = (STATUS_OK, STATUS_NOT_USEFUL)


class AttachmentPreparationError(RuntimeError):
    """The stored attachment bytes could not be turned into an image to view.

    Raised when image decoding or PDF rendering fails on the source bytes
    themselves (as opposed to a transient agent/container failure). Because
    attachments are content-addressed by sha256 the bytes never change, so this
    failure is permanent for the current preparation pipeline. The runner records
    it as STATUS_UNREADABLE and the candidate query excludes such attachments for
    a rolling window instead of re-downloading and re-failing them every run.
    """

IMAGE_MIME_TYPES = (
    "image/png",
    "image/jpeg",
    "image/jpg",
    "image/webp",
    "image/gif",
    "image/bmp",
    "image/tiff",
    "image/heic",
    "image/heif",
)
IMAGE_EXTENSIONS = (".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp", ".tiff", ".tif", ".heic", ".heif")
# Source-status values (from a source's deterministic text-extraction row) that
# mark a PDF as worth a vision pass. Only consulted for sources that run a
# deterministic extraction first (see FileEnrichmentSource.pdf_requires_prior_extraction).
PDF_VISION_SOURCE_STATUSES = ("empty", "invalid_pdf")
MIN_IMAGE_BYTES = 256
MODEL_IMAGE_MAX_EDGE = 1280
MODEL_IMAGE_JPEG_QUALITY = 85
PDF_RENDER_MAX_PAGES = 1
PDF_RENDER_TIMEOUT_SECONDS = 60

AGENT_ATTACHMENT_INPUT_BASENAME = "attachment"


@dataclass(frozen=True)
class FileEnrichmentSource:
    """Describes one attachment source the shared enrichment runner can scan.

    ``table`` is the source's attachment table; the column names map the generic
    candidate query onto it. ``stored_predicate`` is a SQL fragment (using table
    alias ``a``) that selects only rows whose blob is in the object store. Every
    source's storage columns are assumed to be the standard
    ``storage_backend``/``storage_key``/``storage_file_id``/``storage_url``.
    """

    name: str
    label: str
    task_type: str
    prompt_version: str
    table: str
    stored_predicate: str
    sha_column: str = "content_sha256"
    filename_column: str = "filename"
    mime_column: str = "mime_type"
    size_column: str = "size"
    order_column: str = "internal_date"
    # When True, a PDF is only a vision candidate once the source's own
    # deterministic extraction produced no useful text (status in
    # PDF_VISION_SOURCE_STATUSES). When False, any PDF is eligible directly —
    # used by sources that have no deterministic extraction step.
    pdf_requires_prior_extraction: bool = True


GMAIL_SOURCE = FileEnrichmentSource(
    name="gmail",
    label="Gmail attachment",
    # Kept stable so this source's historical agent_runs still count toward the
    # per-attachment error budget after the generalization rename.
    task_type="gmail_attachment_enrichment",
    prompt_version="gmail-attachment-agent-v1",
    table="gmail_attachments",
    stored_predicate="a.is_deleted = 0 AND a.content_sha256 <> '' AND a.storage_status = 'stored'",
    size_column="size",
    order_column="internal_date",
    pdf_requires_prior_extraction=True,
)

WHATSAPP_SOURCE = FileEnrichmentSource(
    name="whatsapp",
    label="WhatsApp media attachment",
    task_type="whatsapp_media_enrichment",
    prompt_version="whatsapp-media-agent-v1",
    table="whatsapp_media_items",
    # is_missing is stored as an integer flag (0/1). A downloaded media blob has
    # is_missing=0 and a content hash; metadata-only history rows (is_missing=1)
    # carry no bytes to enrich.
    stored_predicate="a.is_missing = 0 AND a.content_sha256 <> ''",
    size_column="size_bytes",
    order_column="message_at",
    pdf_requires_prior_extraction=False,
)

APPLE_MESSAGES_SOURCE = FileEnrichmentSource(
    name="apple_messages",
    label="iMessage attachment",
    task_type="apple_messages_attachment_enrichment",
    prompt_version="apple-messages-attachment-agent-v1",
    table="apple_message_attachments",
    # Same shape as WhatsApp: is_missing=0 marks a blob that was actually
    # uploaded to the object store; metadata-only rows (never downloaded) carry
    # no bytes to enrich.
    stored_predicate="a.is_missing = 0 AND a.content_sha256 <> ''",
    size_column="size_bytes",
    order_column="created_at",
    # No deterministic PDF text-extraction stage exists for iMessage, same as
    # WhatsApp, so any PDF is directly eligible.
    pdf_requires_prior_extraction=False,
)

# Backwards-friendly aliases retained for the Gmail-specific call sites/tests
# that predate the generalization.
AGENT_ATTACHMENT_PROMPT_VERSION = GMAIL_SOURCE.prompt_version
AGENT_ATTACHMENT_TASK_TYPE = GMAIL_SOURCE.task_type


@dataclass(frozen=True)
class FileAttachmentEnrichmentSummary:
    attachments_seen: int
    attachments_enriched: int
    attachments_not_useful: int
    attachments_failed: int


class FileAttachmentEnrichmentRunner:
    def __init__(
        self,
        *,
        source: FileEnrichmentSource,
        warehouse,
        agent,
        object_store_factory: Callable[[str], Any],
        logger,
        provider: str,
        model: str = "",
        text_max_chars: int = 20_000,
        max_error_attempts: int = DEFAULT_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS,
        error_window_days: int = DEFAULT_ATTACHMENT_ENRICHMENT_ERROR_WINDOW_DAYS,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._source = source
        self._warehouse = warehouse
        self._agent = agent
        self._object_store_factory = object_store_factory
        self._logger = logger
        self._provider = provider
        self._model = model
        self._prompt_version = source.prompt_version
        self._text_max_chars = text_max_chars
        self._max_error_attempts = max_error_attempts
        self._error_window_days = error_window_days
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._object_stores: dict[str, Any] = {}

    @property
    def enrichment_provider(self) -> str:
        return f"agent_{self._provider}"

    def sync(self, *, limit: int | None) -> FileAttachmentEnrichmentSummary:
        self._warehouse.ensure_file_attachment_enrichment_tables()
        self._warehouse.ensure_agent_tables()
        candidates = load_file_enrichment_candidates(
            self._warehouse,
            source=self._source,
            provider=self.enrichment_provider,
            model=self._model,
            prompt_version=self._prompt_version,
            limit=limit,
            max_error_attempts=self._max_error_attempts,
            error_window_days=self._error_window_days,
        )
        enriched = 0
        not_useful = 0
        failed = 0
        for index, candidate in enumerate(candidates, start=1):
            content_sha256 = str(candidate.get("content_sha256", ""))
            label = f"{candidate.get('filename') or '<unnamed>'} ({content_sha256[:12]})"
            try:
                self._logger.info(
                    "[%s/%s] enriching %s %s", index, len(candidates), self._source.label, label
                )
                status = self._enrich_candidate(candidate)
            except AttachmentPreparationError as exc:
                # Permanent: the bytes can't be decoded/rendered. Record it as
                # unreadable so the candidate query stops re-selecting it every
                # run (it re-enters only after the rolling error window).
                failed += 1
                self._record_failure(candidate, error=str(exc), status=STATUS_UNREADABLE)
                self._logger.warning(
                    "[%s/%s] unreadable %s: %s", index, len(candidates), label, exc
                )
                continue
            except Exception as exc:
                failed += 1
                self._record_failure(candidate, error=str(exc), status=STATUS_ERROR)
                self._logger.warning("[%s/%s] failed %s: %s", index, len(candidates), label, exc)
                continue
            if status == STATUS_OK:
                enriched += 1
            else:
                not_useful += 1
        self._logger.info(
            "%s enrichment: saw %s attachments, enriched %s, not useful %s, failed %s",
            self._source.label,
            len(candidates),
            enriched,
            not_useful,
            failed,
        )
        return FileAttachmentEnrichmentSummary(
            attachments_seen=len(candidates),
            attachments_enriched=enriched,
            attachments_not_useful=not_useful,
            attachments_failed=failed,
        )

    def _enrich_candidate(self, candidate: Mapping[str, Any]) -> str:
        content = self._object_store(str(candidate.get("account", ""))).get_object(
            attachment_storage_ref(candidate)
        )
        image, image_name = prepare_attachment_image(
            content=content,
            mime_type=str(candidate.get("mime_type", "")),
            filename=str(candidate.get("filename", "")),
        )
        prompt = attachment_vision_prompt(
            image_name=image_name, candidate=candidate, source_label=self._source.label
        )
        request = AgentRunRequest(
            prompt=prompt,
            schema=attachment_vision_schema(),
            task_type=self._source.task_type,
            subject_id=str(candidate.get("content_sha256", "")),
            prompt_version=self._prompt_version,
            provider=self._provider,
            model=self._model,
            input_files={image_name: image},
        )
        result = self._agent.run(request)
        self._record_agent_result(result)
        if result.status != "completed":
            raise RuntimeError(result.error or f"agent run {result.run_id} failed")
        output = dict(result.final_output_json)
        issues = validate_attachment_vision_result(output)
        if issues:
            raise RuntimeError("agent output failed validation: " + "; ".join(issues[:5]))

        text = attachment_enrichment_text(output)
        status = STATUS_OK if text else STATUS_NOT_USEFUL
        if len(text) > self._text_max_chars:
            text = text[: self._text_max_chars]
        self._warehouse.insert_attachment_enrichments(
            [
                self._enrichment_row(
                    candidate,
                    text=text,
                    status=status,
                    error="",
                    prompt=prompt,
                    result=result,
                )
            ]
        )
        return status

    def _record_failure(
        self, candidate: Mapping[str, Any], *, error: str, status: str = STATUS_ERROR
    ) -> None:
        try:
            self._warehouse.insert_attachment_enrichments(
                [
                    self._enrichment_row(
                        candidate,
                        text="",
                        status=status,
                        error=error[:2000],
                        prompt="",
                        result=None,
                    )
                ]
            )
        except Exception as exc:
            self._logger.warning(
                "Could not record %s enrichment failure for %s: %s",
                self._source.label,
                candidate.get("content_sha256", ""),
                exc,
            )

    def _enrichment_row(
        self,
        candidate: Mapping[str, Any],
        *,
        text: str,
        status: str,
        error: str,
        prompt: str,
        result: AgentRunResult | None,
    ) -> dict[str, Any]:
        updated_at = self._now()
        elapsed_ms = 0
        processed_at = updated_at
        if result is not None:
            elapsed_ms = int((result.completed_at - result.started_at).total_seconds() * 1000)
            processed_at = result.completed_at
        return {
            "content_sha256": str(candidate.get("content_sha256", "")),
            "ai_provider": self.enrichment_provider,
            "ai_model": self._model,
            "ai_prompt_version": self._prompt_version,
            "text": text,
            "text_extraction_status": status,
            "text_extraction_error": error,
            "ai_base_url": "",
            "ai_prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest() if prompt else "",
            "ai_prompt": prompt,
            "ai_source_status": str(candidate.get("source_status", "")),
            "ai_elapsed_ms": elapsed_ms,
            "ai_processed_at": processed_at,
            "updated_at": updated_at,
            "sync_version": int(updated_at.timestamp() * 1000),
        }

    def _record_agent_result(self, result: AgentRunResult) -> None:
        self._warehouse.insert_agent_runs([agent_run_row(result)])
        event_rows = agent_run_event_rows(result)
        if event_rows:
            self._warehouse.insert_agent_run_events(event_rows)
        tool_call_rows = agent_run_tool_call_rows(result)
        if tool_call_rows:
            self._warehouse.insert_agent_run_tool_calls(tool_call_rows)

    def _object_store(self, account: str):
        store = self._object_stores.get(account)
        if store is None:
            store = self._object_store_factory(account)
            self._object_stores[account] = store
        return store


def _error_window_clause(
    error_window_days: int, *, column: str = "started_at"
) -> tuple[str, list[Any]]:
    """SQL fragment + params that limit a failure record to a recent window.

    ``column`` is the timestamp column compared against the window (``started_at``
    on agent_runs for the agent-failure cap; ``unreadable.updated_at`` on the
    enrichment table for the unreadable-attachment exclusion). Returns an empty
    fragment (count every historical failure, i.e. exclude forever) when the
    window is zero or negative, matching the "disabled" semantics of
    ``max_error_attempts``.
    """
    if error_window_days and int(error_window_days) > 0:
        return f"AND {column} > now() - make_interval(days => %s)", [int(error_window_days)]
    return "", []


def _candidate_query(source: FileEnrichmentSource, *, projection: str, tail: str) -> str:
    """Build a candidate-selection query for ``source``.

    ``projection`` is the SELECT list of the inner per-attachment scan and
    ``tail`` is appended after it (e.g. an ORDER BY / LIMIT wrapper, or a bare
    ``LIMIT 1`` existence probe). Image/PDF eligibility, the stored predicate,
    the attempt-budget join, and the not-yet-enriched guard are identical across
    sources; only the table, column names, and PDF rule vary.
    """
    sha = source.sha_column
    filename = source.filename_column
    mime = source.mime_column
    size = source.size_column
    pdf_is_match = f"(lower(a.{mime}) = 'application/pdf' OR lower(a.{filename}) LIKE '%%.pdf')"
    if source.pdf_requires_prior_extraction:
        pdf_clause = f"({pdf_is_match} AND COALESCE(det.text_extraction_status, '') = ANY(%s))"
    else:
        pdf_clause = pdf_is_match
    return f"""
        WITH failed_runs AS (
            SELECT subject_id, count(*) AS error_attempts
            FROM agent_runs
            WHERE task_type = %s
              AND status = 'error'
              {{error_window_sql}}
            GROUP BY subject_id
        )
        {projection}
        FROM {source.table} a
        LEFT JOIN {ENRICHMENT_TABLE} det
            ON det.content_sha256 = a.{sha}
              AND det.ai_provider = '' AND det.ai_model = '' AND det.ai_prompt_version = ''
        LEFT JOIN failed_runs runs
            ON runs.subject_id = a.{sha}
        WHERE {source.stored_predicate}
          AND a.{size} >= %s
          AND COALESCE(runs.error_attempts, 0) < %s
          AND (
              lower(a.{mime}) = ANY(%s)
              OR lower(a.{filename}) LIKE ANY(%s)
              OR {pdf_clause}
          )
          AND NOT EXISTS (
              SELECT 1
              FROM {ENRICHMENT_TABLE} done
              WHERE done.content_sha256 = a.{sha}
                AND done.ai_provider = %s
                AND done.ai_model = %s
                AND done.ai_prompt_version = %s
                AND done.text_extraction_status = ANY(%s)
          )
          AND NOT EXISTS (
              SELECT 1
              FROM {ENRICHMENT_TABLE} unreadable
              WHERE unreadable.content_sha256 = a.{sha}
                AND unreadable.ai_provider = %s
                AND unreadable.ai_model = %s
                AND unreadable.ai_prompt_version = %s
                AND unreadable.text_extraction_status = %s
                {{unreadable_window_sql}}
          )
        {tail}
        """


def _candidate_params(
    source: FileEnrichmentSource,
    *,
    provider: str,
    model: str,
    prompt_version: str,
    max_error_attempts: int,
    error_window_params: list[Any],
    unreadable_window_params: list[Any],
) -> list[Any]:
    params: list[Any] = [
        source.task_type,
        *error_window_params,
        MIN_IMAGE_BYTES,
        int(max_error_attempts),
        list(IMAGE_MIME_TYPES),
        [f"%{extension}" for extension in IMAGE_EXTENSIONS],
    ]
    if source.pdf_requires_prior_extraction:
        params.append(list(PDF_VISION_SOURCE_STATUSES))
    params.extend([provider, model, prompt_version, list(COMPLETED_STATUSES)])
    params.extend([provider, model, prompt_version, STATUS_UNREADABLE, *unreadable_window_params])
    return params


def load_file_enrichment_candidates(
    warehouse,
    *,
    source: FileEnrichmentSource,
    provider: str,
    model: str,
    prompt_version: str,
    limit: int | None,
    max_error_attempts: int = DEFAULT_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS,
    error_window_days: int = DEFAULT_ATTACHMENT_ENRICHMENT_ERROR_WINDOW_DAYS,
) -> list[dict[str, Any]]:
    """Image (or scanned-PDF) attachments stored in the object store that have
    no completed agent enrichment for this provider/model/prompt identity."""
    error_window_sql, error_window_params = _error_window_clause(error_window_days)
    unreadable_window_sql, unreadable_window_params = _error_window_clause(
        error_window_days, column="unreadable.updated_at"
    )
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
        "source_status",
    )
    limit_sql = "LIMIT %s" if limit is not None and limit > 0 else ""
    projection = (
        f"SELECT DISTINCT ON (a.{source.sha_column}) "
        f"a.account, "
        f"a.{source.sha_column} AS content_sha256, "
        f"a.{source.filename_column} AS filename, "
        f"a.{source.mime_column} AS mime_type, "
        f"a.{source.size_column} AS size, "
        f"a.storage_backend, a.storage_key, a.storage_file_id, a.storage_url, "
        f"COALESCE(det.text_extraction_status, '') AS source_status, "
        f"a.{source.order_column} AS order_at"
    )
    inner = _candidate_query(
        source,
        projection=projection,
        tail=f"ORDER BY a.{source.sha_column}, a.{source.order_column} DESC",
    ).format(error_window_sql=error_window_sql, unreadable_window_sql=unreadable_window_sql)
    params = _candidate_params(
        source,
        provider=provider,
        model=model,
        prompt_version=prompt_version,
        max_error_attempts=max_error_attempts,
        error_window_params=error_window_params,
        unreadable_window_params=unreadable_window_params,
    )
    if limit_sql:
        params.append(int(limit))
    rows = warehouse._query(
        f"""
        SELECT account, content_sha256, filename, mime_type, size,
               storage_backend, storage_key, storage_file_id, storage_url, source_status
        FROM ( {inner} ) AS candidates
        ORDER BY order_at DESC
        {limit_sql}
        """,
        tuple(params),
    )
    return [dict(zip(columns, row, strict=True)) for row in rows]


def has_file_enrichment_candidate(
    warehouse,
    *,
    source: FileEnrichmentSource,
    provider: str,
    model: str,
    prompt_version: str,
    max_error_attempts: int = DEFAULT_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS,
    error_window_days: int = DEFAULT_ATTACHMENT_ENRICHMENT_ERROR_WINDOW_DAYS,
) -> bool:
    """Return whether at least one stored attachment needs agent enrichment."""
    error_window_sql, error_window_params = _error_window_clause(error_window_days)
    unreadable_window_sql, unreadable_window_params = _error_window_clause(
        error_window_days, column="unreadable.updated_at"
    )
    query = _candidate_query(
        source,
        projection="SELECT 1",
        tail="LIMIT 1",
    ).format(error_window_sql=error_window_sql, unreadable_window_sql=unreadable_window_sql)
    params = _candidate_params(
        source,
        provider=provider,
        model=model,
        prompt_version=prompt_version,
        max_error_attempts=max_error_attempts,
        error_window_params=error_window_params,
        unreadable_window_params=unreadable_window_params,
    )
    rows = warehouse._query(query, tuple(params))
    return bool(rows)


def attachment_storage_ref(candidate: Mapping[str, Any]) -> dict[str, str]:
    return {
        "storage_backend": str(candidate.get("storage_backend", "")),
        "storage_key": str(candidate.get("storage_key", "")),
        "storage_file_id": str(candidate.get("storage_file_id", "")),
        "storage_url": str(candidate.get("storage_url", "")),
    }


def prepare_attachment_image(*, content: bytes, mime_type: str, filename: str) -> tuple[bytes, str]:
    """Normalize attachment bytes into one image file the agent can view.

    PDFs are rendered to a PNG of the first page; everything else goes through
    PIL for EXIF transposition, downscaling, and JPEG re-encoding so the agent
    never sees oversized or exotic formats.
    """
    extension = Path(filename.lower()).suffix
    if mime_type.lower() == "application/pdf" or extension == ".pdf" or content.startswith(b"%PDF-"):
        pages = render_pdf_pages(content=content, max_pages=PDF_RENDER_MAX_PAGES)
        if not pages:
            raise AttachmentPreparationError("PDF rendered no pages")
        return pages[0], f"{AGENT_ATTACHMENT_INPUT_BASENAME}.png"
    return normalized_model_image(content), f"{AGENT_ATTACHMENT_INPUT_BASENAME}.jpg"


def normalized_model_image(content: bytes) -> bytes:
    try:
        with Image.open(BytesIO(content)) as original:
            rendered = ImageOps.exif_transpose(original)
            width, height = rendered.size
            scale = min(MODEL_IMAGE_MAX_EDGE / max(width, height), 1)
            if scale < 1:
                rendered = rendered.resize(
                    (max(1, int(width * scale)), max(1, int(height * scale))),
                    Image.Resampling.LANCZOS,
                )
            if rendered.mode not in {"RGB", "L"}:
                rendered = rendered.convert("RGB")
            output = BytesIO()
            rendered.save(output, format="JPEG", quality=MODEL_IMAGE_JPEG_QUALITY, optimize=True)
            return output.getvalue()
    except (OSError, UnidentifiedImageError) as exc:
        raise AttachmentPreparationError(f"attachment is not a decodable image: {exc}") from exc


def render_pdf_pages(
    *,
    content: bytes,
    max_pages: int,
    timeout_seconds: int = PDF_RENDER_TIMEOUT_SECONDS,
) -> list[bytes]:
    pdftoppm = shutil.which("pdftoppm")
    if not pdftoppm:
        raise RuntimeError("pdftoppm is not installed; cannot render image-only PDF for agent enrichment")

    with tempfile.TemporaryDirectory() as directory:
        tempdir = Path(directory)
        input_path = tempdir / "attachment.pdf"
        output_prefix = tempdir / "page"
        input_path.write_bytes(content)
        command = [
            pdftoppm,
            "-png",
            "-r",
            "160",
            "-f",
            "1",
            "-l",
            str(max_pages),
            str(input_path),
            str(output_prefix),
        ]
        result = subprocess.run(
            command,
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
        if result.returncode != 0:
            stderr = result.stderr.decode("utf-8", errors="replace").strip()
            # A non-zero pdftoppm exit means the PDF bytes themselves can't be
            # rendered (corrupt/encrypted/not-really-a-PDF). That is permanent for
            # this content-addressed blob, unlike the "pdftoppm is not installed"
            # environment error above which is transient and stays a RuntimeError.
            raise AttachmentPreparationError(f"pdftoppm failed: {stderr or result.returncode}")
        return [path.read_bytes() for path in sorted(tempdir.glob("page-*.png"))]


def attachment_vision_schema() -> dict[str, Any]:
    # Codex enforces OpenAI strict structured outputs: every property must be
    # listed in `required`, so uncertainties is required too (an empty array is fine).
    return {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "is_useful",
            "document_type",
            "summary",
            "visible_text",
            "entities",
            "search_keywords",
            "uncertainties",
        ],
        "properties": {
            "is_useful": {"type": "boolean"},
            "document_type": {"type": "string"},
            "summary": {"type": "string"},
            "visible_text": {"type": "array", "items": {"type": "string"}},
            "entities": {"type": "array", "items": {"type": "string"}},
            "search_keywords": {"type": "array", "items": {"type": "string"}},
            "uncertainties": {"type": "array", "items": {"type": "string"}},
        },
    }


def attachment_vision_instructions(source_label: str) -> str:
    return f"""Extract searchable metadata from this real {source_label} image.

Look at the image first: extract titles, headings, bullets, labels, legends, names, dates, orgs, brands, numbers, and calls to action before writing a caption. For charts, include axis labels, legends, and visible values. For logos and wordmarks, inspect every text region, including small text at the bottom or edges, and include the full visible phrase rather than only the largest words. For photos, briefly describe people, setting, activity, and objects, and extract readable signs/posters/slides.

If any readable text exists anywhere in the image, is_useful must be true. Logos and wordmarks are useful: set document_type to "logo", put all exact brand text in visible_text, and put the normalized brand/org name in entities. Ads, banners, posters, screenshots, charts, and slides are useful when they contain readable text or meaningful visual context.

Never claim the image is a placeholder or app interface unless a literal placeholder or app UI is actually visible. Do not infer visible text from the filename or from these instructions. Preserve acronyms and capitalization exactly as they appear. Only include text that is visibly printed in the image. If no text is visible, visible_text must be [] rather than ["unknown"], ["none"], or prose. If text is partially uncertain, include your best reading and add "(uncertain)" or an uncertainties note. Avoid generic keywords such as image, attachment, and photo unless they are literally visible or specifically useful.

Keep the output concise: prefer at most 12 visible_text chunks, 10 entities, 10 search_keywords, and 5 uncertainties, while still preserving all important names, titles, dates, numbers, and readable sign/poster text.

Use false for is_useful only for blank/decorative/tracking images or literal placeholders with no useful visible text. Do not invent details."""


def attachment_vision_prompt(
    *, image_name: str, candidate: Mapping[str, Any], source_label: str = "attachment"
) -> str:
    # The image path is given relative to the agent's working directory (the run
    # dir), which is exactly where the inputs/ subdir lives. Image-viewing tools
    # such as codex's view_image receive this string verbatim and do NOT perform
    # shell/env expansion, so advertising "$AGENT_INPUT_DIR/..." led the model to
    # guess the expansion and drop the "inputs/" segment, opening a nonexistent
    # "<workdir>/attachment.jpg". A concrete relative path needs no expansion.
    relative_image_path = f"{DEFAULT_AGENT_INPUTS_DIR_NAME}/{image_name}"
    payload = {
        "task": f"Extract searchable metadata from one {source_label} image.",
        "image": {
            "path": relative_image_path,
            "how_to_view": (
                "Open this exact image path with your image-viewing capability "
                "(codex: the view_image tool; claude: the Read tool) before answering. "
                "The path is relative to your current working directory: pass it "
                "verbatim and do not shorten it to the bare filename or drop the "
                "leading directory."
            ),
            "original_filename": str(candidate.get("filename", "")),
            "original_mime_type": str(candidate.get("mime_type", "")),
        },
        "instructions": attachment_vision_instructions(source_label),
        "final_output_contract": {
            "format": "Return one JSON object and no prose.",
            "schema": attachment_vision_schema(),
        },
        "agent_runtime_notes": [
            "You are running as a one-off CLI agent inside an isolated Docker container.",
            "You MUST actually view the image before producing output; never answer from the filename alone.",
            "The final answer must be valid JSON matching final_output_contract.schema.",
        ],
    }
    return json.dumps(payload, sort_keys=True)


def validate_attachment_vision_result(result: Mapping[str, Any]) -> list[str]:
    issues: list[str] = []
    if not isinstance(result.get("is_useful"), bool):
        issues.append("is_useful must be a boolean")
    for key in ("document_type", "summary"):
        if not isinstance(result.get(key), str):
            issues.append(f"{key} must be a string")
    for key in ("visible_text", "entities", "search_keywords", "uncertainties"):
        value = result.get(key)
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
            issues.append(f"{key} must be an array of strings")
    if result.get("is_useful") is True and not str(result.get("summary", "")).strip():
        issues.append("summary must not be empty when is_useful is true")
    return issues


def attachment_enrichment_text(result: Mapping[str, Any]) -> str:
    """Compose the searchable text block stored in file_attachment_enrichments.

    Mirrors the previous enrichment layout so timeline search documents stay
    consistent across old and new rows.
    """
    if result.get("is_useful") is not True:
        return ""
    document_type = _clean_str(result.get("document_type"))
    summary = _clean_str(result.get("summary"))
    visible_text = _indexable_lines(result.get("visible_text"), separator="\n")
    entities = _indexable_lines(result.get("entities"), separator=", ")
    search_keywords = _indexable_lines(result.get("search_keywords"), separator=", ")
    uncertainties = _indexable_lines(result.get("uncertainties"), separator="\n")
    return "\n\n".join(
        part
        for part in (
            "AI attachment extraction",
            f"Document type: {document_type}".strip(),
            f"Summary: {summary}".strip(),
            f"Visible text:\n{visible_text}".strip(),
            f"Entities: {entities}".strip(),
            f"Search keywords: {search_keywords}".strip(),
            f"Uncertainties:\n{uncertainties}".strip(),
        )
        if part and not part.endswith(":")
    )


_NON_INDEXABLE_VALUES = {
    "",
    "n a",
    "no readable text",
    "no text",
    "none",
    "not applicable",
    "unknown",
}


def _clean_str(value: Any) -> str:
    return str(value or "").strip()


def _indexable_lines(value: Any, *, separator: str) -> str:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ""
    cleaned = []
    for item in value:
        text = str(item).strip()
        normalized = " ".join("".join(c if c.isalnum() else " " for c in text.lower()).split())
        if text and normalized not in _NON_INDEXABLE_VALUES:
            cleaned.append(text)
    return separator.join(cleaned)
