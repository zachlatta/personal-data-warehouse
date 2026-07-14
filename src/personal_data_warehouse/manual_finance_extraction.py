"""Agent-first structured extraction for manual finance documents.

Bank files are structured in terrible ways, so there are NO format-specific
parsers here and no deterministic path that bypasses the agent. Every document
becomes one agent session equipped to figure it out:

- Input preparation only chooses what the agent sees, never whether it runs:
  a PDF's text layer (pypdf via the shared gmail helpers) when present,
  rendered page images (pdftoppm) when there is no usable text, raw text for
  CSV/OFX/QFX/RTF exports, a normalized JPEG for screenshots/photos.
- The prompt carries the uploader's folder hint (``original_path`` — his
  folder-per-account organization), the currently known ledger accounts, and
  a flexible structured output contract (everything optional-by-emptiness).
- The agent runs with read-only warehouse query access
  (``run_with_warehouse``) so it can look things up when a document is
  ambiguous.

Results land in ``manual_finance.extractions`` (typed columns + the full
payload in ``raw_result_json``), keyed by content sha + model/prompt version:
bumping ``PROMPT_VERSION`` re-extracts the corpus without clobbering history.

Retry cap: like the audio pipeline, failures are counted in the extractions
table itself (never ``agent_runs``) because input-preparation failures happen
before any agent run exists.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from personal_data_warehouse.agent_runner import (
    DEFAULT_AGENT_INPUTS_DIR_NAME,
    AgentRunRequest,
    AgentRunResult,
    agent_run_event_rows,
    agent_run_row,
    agent_run_tool_call_rows,
)
from personal_data_warehouse.file_attachment_enrichment import (
    AttachmentPreparationError,
    _error_window_clause,
    normalized_model_image,
    render_pdf_pages,
)
from personal_data_warehouse.gmail_sync import raw_attachment_text

TASK_TYPE = "manual_finance_extraction"
PROMPT_VERSION = "manual-finance-agent-v1"

STATUS_OK = "ok"
STATUS_NOT_USEFUL = "not_useful"
STATUS_ERROR = "error"
# Permanent input-preparation failure on content-addressed bytes (corrupt
# PDF, undecodable image): retrying can never succeed, so it is excluded from
# candidates inside the error window like an error, but recorded distinctly.
STATUS_UNREADABLE = "unreadable"
COMPLETED_STATUSES = (STATUS_OK, STATUS_NOT_USEFUL)

DEFAULT_MAX_ERROR_ATTEMPTS = 3
DEFAULT_ERROR_WINDOW_DAYS = 14
DEFAULT_RENDER_MAX_PAGES = 10
RENDER_MAX_PAGES_CEILING = 20
# A PDF text layer shorter than this is treated as effectively scanned and
# page images are rendered as well; the agent gets both.
MIN_USEFUL_TEXT_CHARS = 200
TEXT_MAX_CHARS = 200_000

_EPOCH_DATE = date(1970, 1, 1)

_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".heic", ".gif", ".webp", ".tiff", ".bmp"}


@dataclass(frozen=True)
class DocumentInputs:
    """What the agent sees for one document (never whether it runs)."""

    text: str
    input_files: dict[str, bytes]


@dataclass(frozen=True)
class ManualFinanceExtractionSummary:
    documents_seen: int
    documents_extracted: int
    documents_not_useful: int
    documents_failed: int
    documents_unreadable: int


def finance_document_extraction_schema() -> dict[str, Any]:
    # Codex enforces OpenAI strict structured outputs: every property must be
    # listed in `required` (empty strings/arrays express absence). Money is
    # decimal-as-string so nothing round-trips through floats.
    transaction_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["date", "description", "amount", "direction"],
        "properties": {
            "date": {"type": "string", "description": "ISO date YYYY-MM-DD"},
            "description": {"type": "string"},
            "amount": {"type": "string", "description": "unsigned decimal string, e.g. '4.50'"},
            "direction": {"type": "string", "enum": ["in", "out"]},
        },
    }
    balance_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["date", "balance"],
        "properties": {
            "date": {"type": "string", "description": "ISO date YYYY-MM-DD"},
            "balance": {"type": "string", "description": "decimal string as printed on the document"},
        },
    }
    valuation_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["date", "value", "description"],
        "properties": {
            "date": {"type": "string", "description": "ISO date YYYY-MM-DD"},
            "value": {"type": "string", "description": "decimal string"},
            "description": {"type": "string"},
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "is_financial",
            "document_type",
            "institution",
            "account_name_hint",
            "account_mask",
            "period_start",
            "period_end",
            "currency",
            "closing_balance",
            "transactions",
            "balances",
            "valuations",
            "summary",
            "uncertainties",
        ],
        "properties": {
            "is_financial": {"type": "boolean"},
            "document_type": {
                "type": "string",
                "description": (
                    "e.g. bank_statement, credit_card_statement, brokerage_statement, "
                    "mortgage_statement, property_valuation, fund_positions, "
                    "transaction_export, receipt, other"
                ),
            },
            "institution": {"type": "string"},
            "account_name_hint": {"type": "string"},
            "account_mask": {"type": "string", "description": "last digits identifying the account, e.g. '0001'"},
            "period_start": {"type": "string", "description": "ISO date or empty"},
            "period_end": {"type": "string", "description": "ISO date or empty"},
            "currency": {"type": "string", "description": "ISO currency code, e.g. USD"},
            "closing_balance": {"type": "string", "description": "decimal string or empty when absent"},
            "transactions": {"type": "array", "items": transaction_schema},
            "balances": {"type": "array", "items": balance_schema},
            "valuations": {"type": "array", "items": valuation_schema},
            "summary": {"type": "string"},
            "uncertainties": {"type": "array", "items": {"type": "string"}},
        },
    }


def finance_extraction_instructions() -> str:
    return """Extract structured financial facts from this real finance document. It may be a bank/credit-card/brokerage/mortgage statement, a property or vehicle valuation screenshot, a private fund positions report, or a CSV/OFX transaction export — figure out what it is from its content; the format may be messy or unusual.

Extract EVERY transaction the document itself reports (date, description exactly as printed, unsigned decimal amount, direction relative to this account: money arriving is "in", money leaving is "out"). Do not summarize, sample, or skip small transactions. For statements also extract the stated opening/closing balances with their dates into balances (the closing balance also goes in closing_balance). For valuation-style documents (home value screenshots, vehicle values, fund position reports) put each stated value into valuations with its as-of date and a short description.

Identify the account: the institution name, the account's own name/type as printed, and the mask (last digits). The document's original_path folder name usually encodes institution-name-mask — use it as a hint, but prefer what the document itself says. Match against known_accounts when one clearly corresponds. period_start/period_end are the statement period when stated.

Amounts must be copied exactly as printed (no rounding, no sign flips). Use empty strings/arrays for anything the document does not state — never invent or infer missing values. Put doubts, unreadable regions, ambiguous signs, or parsing caveats into uncertainties. Set is_financial=false only when the document contains no financial information at all.

If you need more context (e.g. which known account a mask belongs to, or how a prior statement was read), you may query the warehouse read-only via the provided postgres tools."""


def finance_extraction_prompt(
    *,
    candidate: Mapping[str, Any],
    inputs: DocumentInputs,
    known_accounts: Sequence[Mapping[str, Any]],
) -> str:
    document: dict[str, Any] = {
        "original_path": str(candidate.get("original_path", "")),
        "original_filename": str(candidate.get("filename", "")),
        "original_mime_type": str(candidate.get("mime_type", "")),
    }
    if inputs.text:
        document["text"] = inputs.text
    if inputs.input_files:
        document["pages"] = [
            {"path": f"{DEFAULT_AGENT_INPUTS_DIR_NAME}/{name}"} for name in sorted(inputs.input_files)
        ]
        # The image path is passed verbatim to image-viewing tools (no shell
        # expansion) — the hard-won gmail-enrichment lesson: advertise the
        # concrete relative path and forbid shortening it.
        document["how_to_view_pages"] = (
            "Open each page path with your image-viewing capability (codex: the "
            "view_image tool; claude: the Read tool) before answering. Paths are "
            "relative to your current working directory: pass them verbatim and "
            "do not shorten them to the bare filename or drop the leading directory."
        )
    payload = {
        "task": "Extract structured financial facts from one uploaded finance document.",
        "document": document,
        "known_accounts": [dict(account) for account in known_accounts],
        "instructions": finance_extraction_instructions(),
        "final_output_contract": {
            "format": "Return one JSON object and no prose.",
            "schema": finance_document_extraction_schema(),
        },
        "agent_runtime_notes": [
            "You are running as a one-off CLI agent inside an isolated Docker container.",
            "When page images are provided you MUST actually view every page before producing output.",
            "The warehouse postgres tools are read-only; use them for context, never to write.",
            "The final answer must be valid JSON matching final_output_contract.schema.",
        ],
    }
    return json.dumps(payload, sort_keys=True)


def validate_finance_extraction_result(result: Mapping[str, Any]) -> list[str]:
    issues: list[str] = []
    if not isinstance(result.get("is_financial"), bool):
        issues.append("is_financial must be a boolean")
    for key in (
        "document_type",
        "institution",
        "account_name_hint",
        "account_mask",
        "period_start",
        "period_end",
        "currency",
        "closing_balance",
        "summary",
    ):
        if not isinstance(result.get(key), str):
            issues.append(f"{key} must be a string")
    for key, required_fields in (
        ("transactions", ("date", "description", "amount", "direction")),
        ("balances", ("date", "balance")),
        ("valuations", ("date", "value", "description")),
    ):
        value = result.get(key)
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
            issues.append(f"{key} must be an array")
            continue
        for index, entry in enumerate(value):
            if not isinstance(entry, Mapping):
                issues.append(f"{key}[{index}] must be an object")
                continue
            for field in required_fields:
                if not isinstance(entry.get(field), str):
                    issues.append(f"{key}[{index}].{field} must be a string")
    uncertainties = result.get("uncertainties")
    if not isinstance(uncertainties, Sequence) or isinstance(uncertainties, (str, bytes)):
        issues.append("uncertainties must be an array of strings")
    if isinstance(result.get("closing_balance"), str) and result["closing_balance"].strip():
        if _parse_decimal(result["closing_balance"]) is None:
            issues.append("closing_balance must be a decimal string")
    return issues


def prepare_document_inputs(
    *,
    content: bytes,
    mime_type: str,
    filename: str,
    render_max_pages: int = DEFAULT_RENDER_MAX_PAGES,
) -> DocumentInputs:
    """Choose what the agent sees. Raises AttachmentPreparationError only when
    the bytes themselves are unusable (permanent for content-addressed docs)."""
    extension = Path(filename.lower()).suffix
    lowered_mime = mime_type.lower()
    is_pdf = lowered_mime == "application/pdf" or extension == ".pdf" or content.startswith(b"%PDF-")
    is_image = lowered_mime.startswith("image/") or extension in _IMAGE_EXTENSIONS

    if is_image:
        return DocumentInputs(text="", input_files={"page-01.jpg": normalized_model_image(content)})

    if is_pdf:
        text = raw_attachment_text(
            content=content, mime_type="application/pdf", filename=filename, max_chars=TEXT_MAX_CHARS
        )
        text = (text or "").strip()
        if len(text) >= MIN_USEFUL_TEXT_CHARS:
            return DocumentInputs(text=text[:TEXT_MAX_CHARS], input_files={})
        pages = render_pdf_pages(content=content, max_pages=min(render_max_pages, RENDER_MAX_PAGES_CEILING))
        if not pages:
            raise AttachmentPreparationError("PDF rendered no pages")
        input_files = {f"page-{index:02d}.png": page for index, page in enumerate(pages, start=1)}
        # A thin text layer is still worth passing alongside the page images.
        return DocumentInputs(text=text, input_files=input_files)

    # Everything else (csv/ofx/qfx/rtf/txt/...) is raw text for the agent —
    # no format-specific parsing, ever.
    text = raw_attachment_text(content=content, mime_type=mime_type, filename=filename, max_chars=TEXT_MAX_CHARS)
    if text is None:
        text = content.decode("utf-8", errors="replace")
    text = text.strip()
    if not text:
        raise AttachmentPreparationError("document contains no extractable text")
    return DocumentInputs(text=text[:TEXT_MAX_CHARS], input_files={})


class ManualFinanceExtractionRunner:
    def __init__(
        self,
        *,
        warehouse,
        agent,
        object_store_factory: Callable[[], Any],
        logger,
        provider: str,
        model: str = "",
        max_error_attempts: int = DEFAULT_MAX_ERROR_ATTEMPTS,
        error_window_days: int = DEFAULT_ERROR_WINDOW_DAYS,
        render_max_pages: int = DEFAULT_RENDER_MAX_PAGES,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._warehouse = warehouse
        self._agent = agent
        self._object_store_factory = object_store_factory
        self._logger = logger
        self._provider = provider
        self._model = model
        self._max_error_attempts = max_error_attempts
        self._error_window_days = error_window_days
        self._render_max_pages = render_max_pages
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._object_store: Any = None

    @property
    def enrichment_provider(self) -> str:
        return f"agent_{self._provider}"

    def sync(self, *, limit: int | None) -> ManualFinanceExtractionSummary:
        self._warehouse.ensure_manual_finance_tables()
        candidates = load_extraction_candidates(
            self._warehouse,
            provider=self.enrichment_provider,
            prompt_version=PROMPT_VERSION,
            limit=limit,
            max_error_attempts=self._max_error_attempts,
            error_window_days=self._error_window_days,
        )
        extracted = 0
        not_useful = 0
        failed = 0
        unreadable = 0
        known_accounts = self._known_accounts()
        for index, candidate in enumerate(candidates, start=1):
            label = f"{candidate.get('original_path') or candidate.get('filename') or '<unnamed>'}"
            try:
                self._logger.info("[%s/%s] extracting %s", index, len(candidates), label)
                status = self._extract_candidate(candidate, known_accounts=known_accounts)
            except AttachmentPreparationError as exc:
                unreadable += 1
                self._record_failure(candidate, error=str(exc), status=STATUS_UNREADABLE)
                self._logger.warning("[%s/%s] unreadable %s: %s", index, len(candidates), label, exc)
                continue
            except Exception as exc:
                failed += 1
                self._record_failure(candidate, error=str(exc), status=STATUS_ERROR)
                self._logger.warning("[%s/%s] failed %s: %s", index, len(candidates), label, exc)
                continue
            if status == STATUS_OK:
                extracted += 1
            else:
                not_useful += 1
        self._logger.info(
            "Manual finance extraction: saw %s document(s), extracted %s, not useful %s, failed %s, unreadable %s",
            len(candidates),
            extracted,
            not_useful,
            failed,
            unreadable,
        )
        return ManualFinanceExtractionSummary(
            documents_seen=len(candidates),
            documents_extracted=extracted,
            documents_not_useful=not_useful,
            documents_failed=failed,
            documents_unreadable=unreadable,
        )

    def _extract_candidate(
        self, candidate: Mapping[str, Any], *, known_accounts: Sequence[Mapping[str, Any]]
    ) -> str:
        content = self._store().get_object(document_storage_ref(candidate))
        inputs = prepare_document_inputs(
            content=content,
            mime_type=str(candidate.get("mime_type", "")),
            filename=str(candidate.get("filename", "")),
            render_max_pages=self._render_max_pages,
        )
        prompt = finance_extraction_prompt(
            candidate=candidate, inputs=inputs, known_accounts=known_accounts
        )
        request = AgentRunRequest(
            prompt=prompt,
            schema=finance_document_extraction_schema(),
            task_type=TASK_TYPE,
            subject_id=str(candidate.get("content_sha256", "")),
            prompt_version=PROMPT_VERSION,
            provider=self._provider,
            model=self._model,
            input_files=inputs.input_files,
        )
        # Read-only warehouse access: the agent can look up known accounts or
        # prior extractions while it works.
        result = self._agent.run_with_warehouse(request, warehouse=self._warehouse)
        self._record_agent_result(result)
        if result.status != "completed":
            raise RuntimeError(result.error or f"agent run {result.run_id} failed")
        output = dict(result.final_output_json)
        issues = validate_finance_extraction_result(output)
        if issues:
            raise RuntimeError("agent output failed validation: " + "; ".join(issues[:5]))

        status = STATUS_OK if output.get("is_financial") else STATUS_NOT_USEFUL
        self._warehouse.insert_manual_finance_extractions(
            [self._extraction_row(candidate, output=output, status=status, error="", result=result)]
        )
        return status

    def _record_failure(self, candidate: Mapping[str, Any], *, error: str, status: str) -> None:
        try:
            self._warehouse.insert_manual_finance_extractions(
                [self._extraction_row(candidate, output={}, status=status, error=error[:2000], result=None)]
            )
        except Exception as exc:
            self._logger.warning(
                "Could not record extraction failure for %s: %s",
                candidate.get("content_sha256", ""),
                exc,
            )

    def _extraction_row(
        self,
        candidate: Mapping[str, Any],
        *,
        output: Mapping[str, Any],
        status: str,
        error: str,
        result: AgentRunResult | None,
    ) -> dict[str, Any]:
        created_at = self._now()
        elapsed_ms = 0
        processed_at = created_at
        if result is not None:
            elapsed_ms = int((result.completed_at - result.started_at).total_seconds() * 1000)
            processed_at = result.completed_at
        return {
            "content_sha256": str(candidate.get("content_sha256", "")),
            "ai_provider": self.enrichment_provider,
            "ai_model": self._model,
            "ai_prompt_version": PROMPT_VERSION,
            "status": status,
            "error": error,
            "document_type": str(output.get("document_type", "")),
            "institution": str(output.get("institution", "")),
            "account_name_hint": str(output.get("account_name_hint", "")),
            "account_mask": str(output.get("account_mask", "")),
            "period_start": _parse_date(str(output.get("period_start", ""))),
            "period_end": _parse_date(str(output.get("period_end", ""))),
            "currency": str(output.get("currency", "")),
            "closing_balance": _parse_decimal(str(output.get("closing_balance", ""))) or Decimal(0),
            "transactions_json": list(output.get("transactions", []) or []),
            "balances_json": list(output.get("balances", []) or []),
            "valuations_json": list(output.get("valuations", []) or []),
            "summary": str(output.get("summary", "")),
            "uncertainties_json": list(output.get("uncertainties", []) or []),
            "raw_result_json": dict(output),
            "ai_elapsed_ms": elapsed_ms,
            "ai_processed_at": processed_at,
            "created_at": created_at,
            "sync_version": int(created_at.timestamp() * 1_000_000),
        }

    def _record_agent_result(self, result: AgentRunResult) -> None:
        self._warehouse.insert_agent_runs([agent_run_row(result)])
        event_rows = agent_run_event_rows(result)
        if event_rows:
            self._warehouse.insert_agent_run_events(event_rows)
        tool_call_rows = agent_run_tool_call_rows(result)
        if tool_call_rows:
            self._warehouse.insert_agent_run_tool_calls(tool_call_rows)

    def _known_accounts(self) -> list[dict[str, Any]]:
        try:
            return self._warehouse._query_dicts(
                """
                SELECT name, kind, side, institution, mask
                FROM finance_accounts
                ORDER BY institution, name
                LIMIT 200
                """
            )
        except Exception:
            # The ledger tables may not exist yet; extraction still works, the
            # agent just gets no known-accounts context.
            return []

    def _store(self):
        if self._object_store is None:
            self._object_store = self._object_store_factory()
        return self._object_store


def document_storage_ref(candidate: Mapping[str, Any]) -> dict[str, str]:
    return {
        "storage_backend": str(candidate.get("storage_backend", "")),
        "storage_key": str(candidate.get("storage_key", "")),
        "storage_file_id": str(candidate.get("storage_file_id", "")),
        "storage_url": str(candidate.get("storage_url", "")),
    }


def _extraction_candidate_query(*, error_window_sql: str, unreadable_window_sql: str, projection: str, tail: str) -> str:
    # Retry cap follows the incident-hardened vision pattern: agent-stage
    # failures are counted per RUN in agent_runs (one row per run — the
    # extractions table can't count attempts because its PK collapses repeated
    # failures into one row), while permanent input-prep failures are recorded
    # as UNREADABLE extraction rows and excluded within their window.
    return f"""
        WITH failed_runs AS (
            SELECT subject_id, count(*) AS error_attempts
            FROM agent_runs
            WHERE task_type = %s
              AND status = 'error'
              {error_window_sql}
            GROUP BY subject_id
        )
        {projection}
        FROM manual_finance_documents d
        LEFT JOIN failed_runs runs ON runs.subject_id = d.content_sha256
        WHERE d.is_deleted = 0
          AND d.storage_file_id <> ''
          AND COALESCE(runs.error_attempts, 0) < %s
          AND NOT EXISTS (
              SELECT 1
              FROM manual_finance_extractions done
              WHERE done.content_sha256 = d.content_sha256
                AND done.ai_provider = %s
                AND done.ai_prompt_version = %s
                AND done.status = ANY(%s)
          )
          AND NOT EXISTS (
              SELECT 1
              FROM manual_finance_extractions unreadable
              WHERE unreadable.content_sha256 = d.content_sha256
                AND unreadable.ai_provider = %s
                AND unreadable.ai_prompt_version = %s
                AND unreadable.status = %s
                {unreadable_window_sql}
          )
        {tail}
        """


def _extraction_candidate_params(
    *,
    provider: str,
    prompt_version: str,
    max_error_attempts: int,
    error_window_params: list[Any],
    unreadable_window_params: list[Any],
) -> list[Any]:
    return [
        TASK_TYPE,
        *error_window_params,
        int(max_error_attempts),
        provider,
        prompt_version,
        list(COMPLETED_STATUSES),
        provider,
        prompt_version,
        STATUS_UNREADABLE,
        *unreadable_window_params,
    ]


def load_extraction_candidates(
    warehouse,
    *,
    provider: str,
    prompt_version: str,
    limit: int | None,
    max_error_attempts: int = DEFAULT_MAX_ERROR_ATTEMPTS,
    error_window_days: int = DEFAULT_ERROR_WINDOW_DAYS,
) -> list[dict[str, Any]]:
    error_window_sql, error_window_params = _error_window_clause(error_window_days, column="started_at")
    unreadable_window_sql, unreadable_window_params = _error_window_clause(
        error_window_days, column="unreadable.created_at"
    )
    projection = (
        "SELECT DISTINCT ON (d.content_sha256) "
        "d.account, d.content_sha256, d.filename, d.original_path, d.mime_type, "
        "d.size_bytes, d.storage_backend, d.storage_key, d.storage_file_id, d.storage_url"
    )
    limit_sql = "LIMIT %s" if limit is not None and limit > 0 else ""
    query = _extraction_candidate_query(
        error_window_sql=error_window_sql,
        unreadable_window_sql=unreadable_window_sql,
        projection=projection,
        tail="ORDER BY d.content_sha256, d.ingested_at DESC",
    )
    params = _extraction_candidate_params(
        provider=provider,
        prompt_version=prompt_version,
        max_error_attempts=max_error_attempts,
        error_window_params=error_window_params,
        unreadable_window_params=unreadable_window_params,
    )
    outer = f"SELECT * FROM ({query}) candidates ORDER BY original_path {limit_sql}"
    if limit_sql:
        params.append(int(limit))
    return warehouse._query_dicts(outer, tuple(params))


def has_extraction_candidate(
    warehouse,
    *,
    provider: str,
    prompt_version: str,
    max_error_attempts: int = DEFAULT_MAX_ERROR_ATTEMPTS,
    error_window_days: int = DEFAULT_ERROR_WINDOW_DAYS,
) -> bool:
    error_window_sql, error_window_params = _error_window_clause(error_window_days, column="started_at")
    unreadable_window_sql, unreadable_window_params = _error_window_clause(
        error_window_days, column="unreadable.created_at"
    )
    query = _extraction_candidate_query(
        error_window_sql=error_window_sql,
        unreadable_window_sql=unreadable_window_sql,
        projection="SELECT 1",
        tail="LIMIT 1",
    )
    params = _extraction_candidate_params(
        provider=provider,
        prompt_version=prompt_version,
        max_error_attempts=max_error_attempts,
        error_window_params=error_window_params,
        unreadable_window_params=unreadable_window_params,
    )
    return bool(warehouse._query(query, tuple(params)))


def _parse_date(value: str) -> date:
    value = value.strip()
    if not value:
        return _EPOCH_DATE
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return _EPOCH_DATE


def _parse_decimal(value: str) -> Decimal | None:
    cleaned = value.strip().replace(",", "").replace("$", "")
    if not cleaned:
        return None
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def prompt_sha256(prompt: str) -> str:
    return hashlib.sha256(prompt.encode("utf-8")).hexdigest()
