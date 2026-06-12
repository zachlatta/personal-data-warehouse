"""Agent-container vision enrichment for Gmail attachments.

Replaces the removed inline Ollama fallback: instead of calling a local vision
model during Gmail sync, a separate pipeline scans for image (and scanned-PDF)
attachments whose deterministic text extraction produced nothing useful, runs
each through the sandboxed agent container (codex/claude CLI), and upserts the
structured result into ``gmail_attachment_enrichments`` under its own
``ai_provider``/``ai_model``/``ai_prompt_version`` identity. Existing search
indexes on that table pick the text up automatically.
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

from PIL import Image, ImageOps, UnidentifiedImageError

from personal_data_warehouse.agent_runner import (
    AgentRunRequest,
    AgentRunResult,
    agent_run_event_rows,
    agent_run_row,
    agent_run_tool_call_rows,
)

AGENT_ATTACHMENT_TASK_TYPE = "gmail_attachment_enrichment"
AGENT_ATTACHMENT_PROMPT_VERSION = "gmail-attachment-agent-v1"
AGENT_ATTACHMENT_INPUT_BASENAME = "attachment"
DEFAULT_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS = 3

STATUS_OK = "agent_ok"
STATUS_NOT_USEFUL = "agent_not_useful"
STATUS_ERROR = "agent_error"
COMPLETED_STATUSES = (STATUS_OK, STATUS_NOT_USEFUL)

IMAGE_MIME_TYPES = (
    "image/png",
    "image/jpeg",
    "image/jpg",
    "image/webp",
    "image/gif",
    "image/bmp",
    "image/tiff",
)
IMAGE_EXTENSIONS = (".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp", ".tiff", ".tif")
PDF_VISION_SOURCE_STATUSES = ("empty", "invalid_pdf")
MIN_IMAGE_BYTES = 256
MODEL_IMAGE_MAX_EDGE = 1280
MODEL_IMAGE_JPEG_QUALITY = 85
PDF_RENDER_MAX_PAGES = 1
PDF_RENDER_TIMEOUT_SECONDS = 60

ATTACHMENT_VISION_INSTRUCTIONS = """Extract searchable metadata from this real Gmail attachment image.

Look at the image first: extract titles, headings, bullets, labels, legends, names, dates, orgs, brands, numbers, and calls to action before writing a caption. For charts, include axis labels, legends, and visible values. For logos and wordmarks, inspect every text region, including small text at the bottom or edges, and include the full visible phrase rather than only the largest words. For photos, briefly describe people, setting, activity, and objects, and extract readable signs/posters/slides.

If any readable text exists anywhere in the image, is_useful must be true. Logos and wordmarks are useful: set document_type to "logo", put all exact brand text in visible_text, and put the normalized brand/org name in entities. Ads, banners, posters, screenshots, charts, and slides are useful when they contain readable text or meaningful visual context.

Never call the image a Gmail placeholder or Gmail interface unless Gmail UI or a literal Gmail attachment placeholder is visible. Do not infer visible text from the filename or from these instructions. Preserve acronyms and capitalization exactly as they appear. Only include text that is visibly printed in the image. If no text is visible, visible_text must be [] rather than ["unknown"], ["none"], or prose. If text is partially uncertain, include your best reading and add "(uncertain)" or an uncertainties note. Avoid generic keywords such as image, attachment, photo, and Gmail unless they are literally visible or specifically useful.

Keep the output concise: prefer at most 12 visible_text chunks, 10 entities, 10 search_keywords, and 5 uncertainties, while still preserving all important names, titles, dates, numbers, and readable sign/poster text.

Use false for is_useful only for blank/decorative/tracking images or literal placeholders with no useful visible text. Do not invent details."""


@dataclass(frozen=True)
class GmailAttachmentEnrichmentSummary:
    attachments_seen: int
    attachments_enriched: int
    attachments_not_useful: int
    attachments_failed: int


class GmailAttachmentEnrichmentRunner:
    def __init__(
        self,
        *,
        warehouse,
        agent,
        object_store_factory: Callable[[str], Any],
        logger,
        provider: str,
        model: str = "",
        prompt_version: str = AGENT_ATTACHMENT_PROMPT_VERSION,
        text_max_chars: int = 20_000,
        max_error_attempts: int = DEFAULT_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._warehouse = warehouse
        self._agent = agent
        self._object_store_factory = object_store_factory
        self._logger = logger
        self._provider = provider
        self._model = model
        self._prompt_version = prompt_version
        self._text_max_chars = text_max_chars
        self._max_error_attempts = max_error_attempts
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._object_stores: dict[str, Any] = {}

    @property
    def enrichment_provider(self) -> str:
        return f"agent_{self._provider}"

    def sync(self, *, limit: int | None) -> GmailAttachmentEnrichmentSummary:
        self._warehouse.ensure_tables()
        self._warehouse.ensure_agent_tables()
        candidates = load_attachment_enrichment_candidates(
            self._warehouse,
            provider=self.enrichment_provider,
            model=self._model,
            prompt_version=self._prompt_version,
            limit=limit,
            max_error_attempts=self._max_error_attempts,
        )
        enriched = 0
        not_useful = 0
        failed = 0
        for index, candidate in enumerate(candidates, start=1):
            content_sha256 = str(candidate.get("content_sha256", ""))
            label = f"{candidate.get('filename') or '<unnamed>'} ({content_sha256[:12]})"
            try:
                self._logger.info("[%s/%s] enriching Gmail attachment %s", index, len(candidates), label)
                status = self._enrich_candidate(candidate)
            except Exception as exc:
                failed += 1
                self._record_failure(candidate, error=str(exc))
                self._logger.warning("[%s/%s] failed %s: %s", index, len(candidates), label, exc)
                continue
            if status == STATUS_OK:
                enriched += 1
            else:
                not_useful += 1
        return GmailAttachmentEnrichmentSummary(
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
        prompt = attachment_vision_prompt(image_name=image_name, candidate=candidate)
        request = AgentRunRequest(
            prompt=prompt,
            schema=attachment_vision_schema(),
            task_type=AGENT_ATTACHMENT_TASK_TYPE,
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

    def _record_failure(self, candidate: Mapping[str, Any], *, error: str) -> None:
        try:
            self._warehouse.insert_attachment_enrichments(
                [
                    self._enrichment_row(
                        candidate,
                        text="",
                        status=STATUS_ERROR,
                        error=error[:2000],
                        prompt="",
                        result=None,
                    )
                ]
            )
        except Exception as exc:
            self._logger.warning(
                "Could not record Gmail attachment enrichment failure for %s: %s",
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


def load_attachment_enrichment_candidates(
    warehouse,
    *,
    provider: str,
    model: str,
    prompt_version: str,
    limit: int | None,
    max_error_attempts: int = DEFAULT_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS,
) -> list[dict[str, Any]]:
    """Image (or scanned-PDF) attachments stored in the object store that have
    no completed agent enrichment for this provider/model/prompt identity."""
    image_mime_types = list(IMAGE_MIME_TYPES)
    image_extension_patterns = [f"%{extension}" for extension in IMAGE_EXTENSIONS]
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
    params: list[Any] = [
        MIN_IMAGE_BYTES,
        image_mime_types,
        image_extension_patterns,
        list(PDF_VISION_SOURCE_STATUSES),
        provider,
        model,
        prompt_version,
        list(COMPLETED_STATUSES),
        AGENT_ATTACHMENT_TASK_TYPE,
        int(max_error_attempts),
    ]
    if limit_sql:
        params.append(int(limit))
    rows = warehouse._query(
        f"""
        SELECT account, content_sha256, filename, mime_type, size,
               storage_backend, storage_key, storage_file_id, storage_url, source_status
        FROM (
            SELECT DISTINCT ON (a.content_sha256)
                a.account,
                a.content_sha256,
                a.filename,
                a.mime_type,
                a.size,
                a.storage_backend,
                a.storage_key,
                a.storage_file_id,
                a.storage_url,
                COALESCE(det.text_extraction_status, '') AS source_status,
                a.internal_date
            FROM gmail_attachments a
            LEFT JOIN gmail_attachment_enrichments det
                ON det.content_sha256 = a.content_sha256
                  AND det.ai_provider = ''
                  AND det.ai_model = ''
                  AND det.ai_prompt_version = ''
            WHERE a.is_deleted = 0
              AND a.content_sha256 <> ''
              AND a.storage_status = 'stored'
              AND a.size >= %s
              AND (
                  lower(a.mime_type) = ANY(%s)
                  OR lower(a.filename) LIKE ANY(%s)
                  OR (
                      (lower(a.mime_type) = 'application/pdf' OR lower(a.filename) LIKE '%%.pdf')
                      AND COALESCE(det.text_extraction_status, '') = ANY(%s)
                  )
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM gmail_attachment_enrichments done
                  WHERE done.content_sha256 = a.content_sha256
                    AND done.ai_provider = %s
                    AND done.ai_model = %s
                    AND done.ai_prompt_version = %s
                    AND done.text_extraction_status = ANY(%s)
              )
              AND (
                  SELECT count(*)
                  FROM agent_runs runs
                  WHERE runs.task_type = %s
                    AND runs.subject_id = a.content_sha256
                    AND runs.status = 'error'
              ) < %s
            ORDER BY a.content_sha256, a.internal_date DESC
        ) AS candidates
        ORDER BY internal_date DESC
        {limit_sql}
        """,
        tuple(params),
    )
    return [dict(zip(columns, row, strict=True)) for row in rows]


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
            raise RuntimeError("PDF rendered no pages")
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
        raise RuntimeError(f"attachment is not a decodable image: {exc}") from exc


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
            raise RuntimeError(f"pdftoppm failed: {stderr or result.returncode}")
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


def attachment_vision_prompt(*, image_name: str, candidate: Mapping[str, Any]) -> str:
    payload = {
        "task": "Extract searchable metadata from one Gmail attachment image.",
        "image": {
            "path": f"$AGENT_INPUT_DIR/{image_name}",
            "how_to_view": (
                "Open the image file with your image-viewing capability "
                "(codex: the view_image tool; claude: the Read tool) before answering."
            ),
            "original_filename": str(candidate.get("filename", "")),
            "original_mime_type": str(candidate.get("mime_type", "")),
        },
        "instructions": ATTACHMENT_VISION_INSTRUCTIONS,
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
    """Compose the searchable text block stored in gmail_attachment_enrichments.

    Mirrors the layout the previous pipeline produced so the BM25/trigram search
    behavior stays consistent across old and new rows.
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
