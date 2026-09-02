from __future__ import annotations

from datetime import UTC, datetime
import hashlib
from io import BytesIO
import json

import pillow_heif
import pytest
from PIL import Image

from personal_data_warehouse.agent_runner import AgentRunRequest, AgentRunResult
from personal_data_warehouse.file_attachment_enrichment import (
    AGENT_ATTACHMENT_PROMPT_VERSION,
    AGENT_ATTACHMENT_TASK_TYPE,
    AMBIGUOUS_MIME_TYPES,
    APPLE_MESSAGES_SOURCE,
    APPLE_NOTES_SOURCE,
    GMAIL_SOURCE,
    IMAGE_EXTENSIONS,
    IMAGE_MIME_TYPES,
    WHATSAPP_SOURCE,
    AttachmentPreparationError,
    FileAttachmentEnrichmentRunner,
    STATUS_ERROR,
    STATUS_NOT_USEFUL,
    STATUS_OK,
    STATUS_UNREADABLE,
    STATUS_UNSUPPORTED,
    TERMINAL_STATUSES,
    AttachmentUnsupportedError,
    attachment_enrichment_text,
    attachment_storage_ref,
    attachment_vision_prompt,
    attachment_vision_schema,
    has_file_enrichment_candidate,
    load_file_enrichment_candidates,
    normalized_model_image,
    prepare_attachment_image,
    sniff_content_type,
    validate_attachment_vision_result,
)


class FakeLogger:
    def __init__(self) -> None:
        self.infos: list[str] = []
        self.warnings: list[str] = []

    def info(self, message, *args) -> None:
        self.infos.append(message % args if args else message)

    def warning(self, message, *args) -> None:
        self.warnings.append(message % args if args else message)


class FakeWarehouse:
    def __init__(self, candidate_rows: list[tuple] | None = None) -> None:
        self.candidate_rows = list(candidate_rows or [])
        self.queries: list[tuple[str, tuple]] = []
        self.enrichment_rows: list[dict] = []
        self.agent_runs: list[dict] = []
        self.agent_run_events: list[dict] = []
        self.agent_run_tool_calls: list[dict] = []

    def ensure_file_attachment_enrichment_tables(self) -> None:
        pass

    def ensure_agent_tables(self) -> None:
        pass

    def _query(self, sql: str, params=None):
        self.queries.append((sql, params))
        return self.candidate_rows

    def insert_attachment_enrichments(self, rows) -> None:
        self.enrichment_rows.extend(rows)

    def insert_agent_runs(self, rows) -> None:
        self.agent_runs.extend(rows)

    def insert_agent_run_events(self, rows) -> None:
        self.agent_run_events.extend(rows)

    def insert_agent_run_tool_calls(self, rows) -> None:
        self.agent_run_tool_calls.extend(rows)


class FakeObjectStore:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.get_calls: list[dict] = []

    def get_object(self, ref) -> bytes:
        self.get_calls.append(dict(ref))
        return self.content


class FakeAgent:
    def __init__(self, output: dict | None = None, *, status: str = "completed", error: str = "") -> None:
        self.output = output or {}
        self.status = status
        self.error = error
        self.requests: list[AgentRunRequest] = []

    def run(self, request: AgentRunRequest) -> AgentRunResult:
        self.requests.append(request)
        now = datetime(2026, 6, 11, 12, 0, tzinfo=UTC)
        return AgentRunResult(
            run_id=request.run_id,
            provider=request.provider or "codex",
            model=request.model or "",
            task_type=request.task_type,
            subject_id=request.subject_id,
            prompt_version=request.prompt_version,
            input_sha256=request.input_sha256,
            status=self.status,
            final_output_json=self.output,
            error=self.error,
            exit_code=0 if self.status == "completed" else 1,
            started_at=now,
            completed_at=now,
            events=[],
        )


def png_bytes(size: tuple[int, int] = (32, 32)) -> bytes:
    output = BytesIO()
    Image.new("RGB", size, color=(120, 30, 30)).save(output, format="PNG")
    return output.getvalue()


def heic_bytes(size: tuple[int, int] = (32, 32)) -> bytes:
    output = BytesIO()
    pillow_heif.from_pillow(Image.new("RGB", size, color=(120, 30, 30))).save(output, format="HEIF")
    return output.getvalue()


def candidate_row(content: bytes, *, filename: str = "logo.png", mime_type: str = "image/png") -> tuple:
    return (
        "zach@example.com",
        hashlib.sha256(content).hexdigest(),
        filename,
        mime_type,
        len(content),
        "google_drive",
        "gmail-attachments/library/x.png",
        "drive-file-id",
        "https://drive.example/x",
        "unsupported",
    )


def useful_output() -> dict:
    return {
        "is_useful": True,
        "document_type": "logo",
        "summary": "The SpaceX wordmark in dark blue.",
        "visible_text": ["SPACEX"],
        "entities": ["SpaceX"],
        "search_keywords": ["spacex", "logo"],
        "uncertainties": [],
    }


def make_runner(warehouse, agent, store, *, source=GMAIL_SOURCE, logger=None) -> FileAttachmentEnrichmentRunner:
    return FileAttachmentEnrichmentRunner(
        source=source,
        warehouse=warehouse,
        agent=agent,
        object_store_factory=lambda account: store,
        logger=logger or FakeLogger(),
        provider="codex",
        model="",
    )


def test_sync_logs_summary_line_with_counts() -> None:
    content = png_bytes()
    warehouse = FakeWarehouse([candidate_row(content)])
    logger = FakeLogger()
    runner = make_runner(warehouse, FakeAgent(useful_output()), FakeObjectStore(content), logger=logger)

    runner.sync(limit=10)

    assert (
        "Gmail attachment enrichment: saw 1 attachments, enriched 1, "
        "not useful 0, failed 0" in logger.infos
    )


def test_sync_logs_summary_line_when_idle() -> None:
    logger = FakeLogger()
    runner = make_runner(FakeWarehouse([]), FakeAgent(useful_output()), FakeObjectStore(b""), logger=logger)

    runner.sync(limit=10)

    assert (
        "Gmail attachment enrichment: saw 0 attachments, enriched 0, "
        "not useful 0, failed 0" in logger.infos
    )


def test_runner_enriches_image_attachment_and_records_agent_run() -> None:
    content = png_bytes()
    warehouse = FakeWarehouse([candidate_row(content)])
    agent = FakeAgent(useful_output())
    store = FakeObjectStore(content)

    summary = make_runner(warehouse, agent, store).sync(limit=10)

    assert summary.attachments_seen == 1
    assert summary.attachments_enriched == 1
    assert summary.attachments_failed == 0

    assert store.get_calls == [
        {
            "storage_backend": "google_drive",
            "storage_key": "gmail-attachments/library/x.png",
            "storage_file_id": "drive-file-id",
            "storage_url": "https://drive.example/x",
        }
    ]

    request = agent.requests[0]
    assert request.task_type == AGENT_ATTACHMENT_TASK_TYPE
    assert request.subject_id == hashlib.sha256(content).hexdigest()
    assert request.prompt_version == AGENT_ATTACHMENT_PROMPT_VERSION
    assert list(request.input_files) == ["attachment.jpg"]
    assert isinstance(request.input_files["attachment.jpg"], bytes)

    assert len(warehouse.agent_runs) == 1
    assert warehouse.agent_runs[0]["task_type"] == AGENT_ATTACHMENT_TASK_TYPE

    row = warehouse.enrichment_rows[0]
    assert row["content_sha256"] == hashlib.sha256(content).hexdigest()
    assert row["ai_provider"] == "agent_codex"
    assert row["ai_prompt_version"] == AGENT_ATTACHMENT_PROMPT_VERSION
    assert row["text_extraction_status"] == STATUS_OK
    assert row["ai_source_status"] == "unsupported"
    assert "SPACEX" in row["text"]
    assert "Summary: The SpaceX wordmark in dark blue." in row["text"]


def test_runner_uses_source_identity_for_whatsapp_media() -> None:
    content = png_bytes()
    warehouse = FakeWarehouse([candidate_row(content)])
    agent = FakeAgent(useful_output())

    summary = make_runner(warehouse, agent, FakeObjectStore(content), source=WHATSAPP_SOURCE).sync(limit=10)

    assert summary.attachments_enriched == 1
    request = agent.requests[0]
    # The WhatsApp source carries its own task_type + prompt_version so its agent
    # runs and enrichment rows are scoped independently from Gmail's.
    assert request.task_type == WHATSAPP_SOURCE.task_type == "whatsapp_media_enrichment"
    assert request.prompt_version == WHATSAPP_SOURCE.prompt_version == "whatsapp-media-agent-v1"
    assert warehouse.agent_runs[0]["task_type"] == "whatsapp_media_enrichment"
    row = warehouse.enrichment_rows[0]
    assert row["ai_prompt_version"] == "whatsapp-media-agent-v1"
    assert "WhatsApp media attachment" in row["ai_prompt"]
    assert "SPACEX" in row["text"]


def test_runner_uses_source_identity_for_apple_messages() -> None:
    content = png_bytes()
    warehouse = FakeWarehouse([candidate_row(content)])
    agent = FakeAgent(useful_output())

    summary = make_runner(
        warehouse, agent, FakeObjectStore(content), source=APPLE_MESSAGES_SOURCE
    ).sync(limit=10)

    assert summary.attachments_enriched == 1
    request = agent.requests[0]
    # The Apple Messages source carries its own task_type + prompt_version so its
    # agent runs and enrichment rows are scoped independently from Gmail/WhatsApp.
    assert request.task_type == APPLE_MESSAGES_SOURCE.task_type == "apple_messages_attachment_enrichment"
    assert request.prompt_version == APPLE_MESSAGES_SOURCE.prompt_version == "apple-messages-attachment-agent-v1"
    assert warehouse.agent_runs[0]["task_type"] == "apple_messages_attachment_enrichment"
    row = warehouse.enrichment_rows[0]
    assert row["ai_prompt_version"] == "apple-messages-attachment-agent-v1"
    assert "iMessage attachment" in row["ai_prompt"]
    assert "SPACEX" in row["text"]


def test_apple_messages_source_has_no_prior_pdf_extraction_gate() -> None:
    # Same as WhatsApp: there is no deterministic PDF text-extraction stage for
    # iMessage attachments, so any PDF is directly eligible for the vision pass.
    assert APPLE_MESSAGES_SOURCE.pdf_requires_prior_extraction is False
    assert APPLE_MESSAGES_SOURCE.table == "marts_files_attachments"
    assert "a.source = 'apple_messages'" in APPLE_MESSAGES_SOURCE.stored_predicate
    assert APPLE_MESSAGES_SOURCE.size_column == "size_bytes"


def test_runner_marks_non_useful_images_without_search_text() -> None:
    content = png_bytes()
    warehouse = FakeWarehouse([candidate_row(content)])
    agent = FakeAgent(
        {
            "is_useful": False,
            "document_type": "unknown",
            "summary": "",
            "visible_text": [],
            "entities": [],
            "search_keywords": [],
            "uncertainties": [],
        }
    )

    summary = make_runner(warehouse, agent, FakeObjectStore(content)).sync(limit=None)

    assert summary.attachments_not_useful == 1
    row = warehouse.enrichment_rows[0]
    assert row["text_extraction_status"] == STATUS_NOT_USEFUL
    assert row["text"] == ""


def test_runner_records_error_row_when_agent_fails() -> None:
    content = png_bytes()
    warehouse = FakeWarehouse([candidate_row(content)])
    agent = FakeAgent(status="error", error="container exploded")

    summary = make_runner(warehouse, agent, FakeObjectStore(content)).sync(limit=None)

    assert summary.attachments_failed == 1
    assert len(warehouse.agent_runs) == 1
    row = warehouse.enrichment_rows[0]
    assert row["text_extraction_status"] == STATUS_ERROR
    assert "container exploded" in row["text_extraction_error"]
    assert row["text"] == ""


def test_runner_records_unreadable_status_for_corrupt_bytes_without_agent_run() -> None:
    # Bytes that PIL cannot decode fail in prepare_attachment_image, before the
    # agent ever runs. Such failures are permanent for a content-addressed blob,
    # so they must be recorded as STATUS_UNREADABLE (not STATUS_ERROR) and must
    # NOT create an agent_runs row — otherwise the old behavior recycled them
    # through the candidate query on every run forever.
    content = b"this is definitely not an image"
    warehouse = FakeWarehouse([candidate_row(content)])
    agent = FakeAgent(useful_output())

    summary = make_runner(warehouse, agent, FakeObjectStore(content)).sync(limit=None)

    assert summary.attachments_failed == 1
    assert agent.requests == []  # the agent never ran
    assert warehouse.agent_runs == []  # so nothing counts toward the agent-error cap
    row = warehouse.enrichment_rows[0]
    assert row["text_extraction_status"] == STATUS_UNREADABLE
    assert "not a decodable image" in row["text_extraction_error"]
    assert row["text"] == ""


def test_prepare_attachment_image_raises_preparation_error_subtype() -> None:
    # The preparation failure is an AttachmentPreparationError (a RuntimeError
    # subclass, so existing `pytest.raises(RuntimeError)` call sites still pass)
    # which is how sync() tells permanent decode failures apart from transient
    # agent failures.
    with pytest.raises(AttachmentPreparationError, match="not a decodable image"):
        prepare_attachment_image(content=b"not an image", mime_type="image/png", filename="x.png")


def test_normalized_model_image_salvages_truncated_image() -> None:
    # A JPEG with its trailing bytes lopped off (a common email/WhatsApp artifact)
    # used to raise "image file is truncated"; with truncated loading enabled the
    # decodable portion is recovered into a valid JPEG instead of failing forever.
    full = BytesIO()
    Image.new("RGB", (256, 256), color=(10, 120, 200)).save(full, format="JPEG", quality=90)
    truncated = full.getvalue()[:-32]

    image = normalized_model_image(truncated)

    with Image.open(BytesIO(image)) as rendered:
        assert rendered.format == "JPEG"
        assert rendered.size == (256, 256)


def test_candidate_query_excludes_terminal_attachments_permanently() -> None:
    warehouse = FakeWarehouse([])
    load_file_enrichment_candidates(
        warehouse,
        source=GMAIL_SOURCE,
        provider="agent_codex",
        prompt_version=GMAIL_SOURCE.prompt_version,
        limit=5,
        error_window_days=14,
    )
    sql, params = warehouse.queries[0]
    # A terminal classification must be PERMANENT. The previous rolling-window
    # exclusion let every agent_unreadable blob re-enter the candidate pool 14
    # days later and re-fail identically forever — a slow loop that doubled the
    # unreadable count between 2026-07-28 and 2026-08-12. Because attachments are
    # content-addressed, the same bytes always fail the same way; the deliberate
    # escape hatch is retry_terminal, not the passage of time.
    assert "terminal.text_extraction_status = ANY(%s)" in sql
    assert "terminal.updated_at > now()" not in sql
    assert list(TERMINAL_STATUSES) in params


def test_candidate_query_admits_metadata_free_blobs() -> None:
    # iMessage stores real images as extension-less "GroupPhotoImage" /
    # "BrandLogoImage" blobs with an application/octet-stream MIME. Neither the
    # exact-MIME list nor the extension patterns match them, so without this
    # clause they are permanently invisible to the vision pass. Their format is
    # settled from their bytes once admitted.
    warehouse = FakeWarehouse([])
    load_file_enrichment_candidates(
        warehouse,
        source=APPLE_MESSAGES_SOURCE,
        provider="agent_codex",
        prompt_version=APPLE_MESSAGES_SOURCE.prompt_version,
        limit=5,
    )
    sql, params = warehouse.queries[0]
    # '%%' is psycopg's escape for a literal percent, matching the existing
    # '%%.pdf' clause; it reaches Postgres as NOT LIKE '%.%'.
    assert "NOT LIKE '%%.%%'" in sql
    assert list(AMBIGUOUS_MIME_TYPES) in params
    # Anchored to the basename, so a dotted DIRECTORY in the stored path cannot
    # masquerade as a file extension.
    assert "regexp_replace" in sql


def test_candidate_query_can_deliberately_retry_terminal_attachments() -> None:
    # After a preparation-pipeline fix (e.g. new format support) the operator
    # re-drives previously terminal blobs explicitly, the same way the photos
    # uploader's --retry-failed clears its backoff.
    warehouse = FakeWarehouse([])
    load_file_enrichment_candidates(
        warehouse,
        source=GMAIL_SOURCE,
        provider="agent_codex",
        prompt_version=GMAIL_SOURCE.prompt_version,
        limit=5,
        retry_terminal=True,
    )
    sql, params = warehouse.queries[0]
    assert "terminal.text_extraction_status" not in sql
    assert list(TERMINAL_STATUSES) not in params


def test_sniff_content_type_identifies_formats_from_bytes() -> None:
    # Declared MIME on iMessage attachments is frequently wrong or absent, so the
    # bytes are the authority.
    assert sniff_content_type(png_bytes()) == "image/png"
    assert sniff_content_type(heic_bytes()) == "image/heic"
    assert sniff_content_type(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n") == "application/pdf"
    assert sniff_content_type(b"GIF89a" + b"\x00" * 16) == "image/gif"
    assert sniff_content_type(b"\x00\x00\x00\x20ftypisom\x00\x00\x02\x00") == "video/mp4"
    assert sniff_content_type(b"\x00\x00\x00\x14ftypqt  \x00\x00\x02\x00") == "video/quicktime"
    assert sniff_content_type(b"bplist00" + b"\x00" * 16) == "application/x-plist"
    # Too short / unrecognized must be "unknown", never a wrong guess.
    assert sniff_content_type(b"tiny") == ""
    assert sniff_content_type(b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c") == ""


def test_prepare_attachment_image_trusts_bytes_over_wrong_declared_mime() -> None:
    # A real PNG delivered with application/octet-stream and a meaningless
    # filename must still be enriched rather than failing as "not a decodable
    # image" and being written off as unreadable.
    image, name = prepare_attachment_image(
        content=png_bytes(),
        mime_type="application/octet-stream",
        filename="6F2A-payload.pluginPayloadAttachment",
    )
    assert name == "attachment.jpg"
    with Image.open(BytesIO(image)) as rendered:
        assert rendered.format == "JPEG"


def test_prepare_attachment_image_renders_pdf_bytes_mislabeled_as_image() -> None:
    pdf = b"%PDF-1.4\n" + b"\x00" * 64
    # Sniffed as PDF despite the .png name, so it takes the PDF render path and
    # any failure there is a PDF failure, not a bogus "not a decodable image".
    with pytest.raises(AttachmentPreparationError, match="pdftoppm|PDF"):
        prepare_attachment_image(content=pdf, mime_type="image/png", filename="scan.png")


def test_prepare_attachment_image_classifies_video_as_unsupported() -> None:
    # Video is deliberately out of scope for the vision pass. It must get a
    # STABLE terminal classification rather than being retried as a decode error,
    # so the coverage report can say "unsupported by design" instead of "missing".
    with pytest.raises(AttachmentUnsupportedError, match="video/mp4"):
        prepare_attachment_image(
            content=b"\x00\x00\x00\x20ftypisom\x00\x00\x02\x00" + b"\x00" * 32,
            mime_type="video/mp4",
            filename="IMG_2231.MOV",
        )


def test_unsupported_error_is_a_preparation_error_subtype() -> None:
    # Both are permanent for a content-addressed blob, so existing call sites that
    # catch AttachmentPreparationError keep working; sync() distinguishes them
    # only to record the more precise status.
    assert issubclass(AttachmentUnsupportedError, AttachmentPreparationError)


def test_runner_records_unsupported_status_without_agent_run() -> None:
    content = b"\x00\x00\x00\x20ftypisom\x00\x00\x02\x00" + b"\x00" * 32
    warehouse = FakeWarehouse(
        [candidate_row(content, filename="IMG_2231.MOV", mime_type="video/quicktime")]
    )
    agent = FakeAgent(useful_output())

    summary = make_runner(warehouse, agent, FakeObjectStore(content)).sync(limit=None)

    assert summary.attachments_failed == 1
    assert agent.requests == []  # never burns an agent run on a known-unsupported format
    assert warehouse.agent_runs == []
    row = warehouse.enrichment_rows[0]
    assert row["text_extraction_status"] == STATUS_UNSUPPORTED
    assert "video/mp4" in row["text_extraction_error"]


def test_candidate_completion_identity_ignores_model() -> None:
    warehouse = FakeWarehouse([])

    load_file_enrichment_candidates(
        warehouse,
        source=GMAIL_SOURCE,
        provider="agent_codex",
        prompt_version=GMAIL_SOURCE.prompt_version,
        limit=5,
    )

    sql, _params = warehouse.queries[0]
    assert "done.ai_model" not in sql
    assert "unreadable.ai_model" not in sql


@pytest.mark.parametrize(
    "source", [GMAIL_SOURCE, WHATSAPP_SOURCE, APPLE_MESSAGES_SOURCE, APPLE_NOTES_SOURCE]
)
@pytest.mark.parametrize("error_window_days", [14, 0])
@pytest.mark.parametrize("retry_terminal", [False, True])
def test_candidate_query_placeholder_count_matches_params(
    source, error_window_days, retry_terminal
) -> None:
    # Guards against parameter/placeholder drift across the several %s groups
    # (cap CTE, eligibility, completed-exclusion, terminal-exclusion, window,
    # limit), which would otherwise raise at execution time in production.
    warehouse = FakeWarehouse([])
    load_file_enrichment_candidates(
        warehouse,
        source=source,
        provider="agent_codex",
        prompt_version=source.prompt_version,
        limit=5,
        error_window_days=error_window_days,
        retry_terminal=retry_terminal,
    )
    sql, params = warehouse.queries[0]
    assert sql.count("%s") == len(params)


@pytest.mark.parametrize(
    "source", [GMAIL_SOURCE, WHATSAPP_SOURCE, APPLE_MESSAGES_SOURCE, APPLE_NOTES_SOURCE]
)
def test_existence_probe_matches_candidate_query_terminal_rules(source) -> None:
    # The sensor's probe and the runner's loader must agree on what counts as a
    # candidate; otherwise the sensor launches runs that find nothing to do.
    probe_warehouse = FakeWarehouse([])
    has_file_enrichment_candidate(
        probe_warehouse,
        source=source,
        provider="agent_codex",
        prompt_version=source.prompt_version,
    )
    probe_sql, probe_params = probe_warehouse.queries[0]
    assert "terminal.text_extraction_status = ANY(%s)" in probe_sql
    assert "terminal.updated_at > now()" not in probe_sql
    assert probe_sql.count("%s") == len(probe_params)


def test_runner_rejects_output_missing_required_fields() -> None:
    content = png_bytes()
    warehouse = FakeWarehouse([candidate_row(content)])
    agent = FakeAgent({"is_useful": True, "summary": 42})

    summary = make_runner(warehouse, agent, FakeObjectStore(content)).sync(limit=None)

    assert summary.attachments_failed == 1
    assert warehouse.enrichment_rows[0]["text_extraction_status"] == STATUS_ERROR


def test_prepare_attachment_image_normalizes_to_jpeg() -> None:
    oversized = BytesIO()
    Image.new("RGB", (3000, 1000), color=(0, 80, 160)).save(oversized, format="PNG")

    image, name = prepare_attachment_image(
        content=oversized.getvalue(),
        mime_type="image/png",
        filename="big.png",
    )

    assert name == "attachment.jpg"
    with Image.open(BytesIO(image)) as rendered:
        assert rendered.format == "JPEG"
        assert max(rendered.size) <= 1280


def test_prepare_attachment_image_rejects_non_image_bytes() -> None:
    with pytest.raises(RuntimeError, match="not a decodable image"):
        prepare_attachment_image(content=b"not an image", mime_type="image/png", filename="x.png")


def test_normalized_model_image_keeps_small_images_decodable() -> None:
    image = normalized_model_image(png_bytes((16, 16)))
    with Image.open(BytesIO(image)) as rendered:
        assert rendered.size == (16, 16)


def test_heic_is_a_recognized_image_kind() -> None:
    # Nearly half of iMessage's stored image blobs are HEIC (the default iPhone
    # camera format); without this the candidate query silently excludes them.
    assert "image/heic" in IMAGE_MIME_TYPES
    assert "image/heif" in IMAGE_MIME_TYPES
    assert ".heic" in IMAGE_EXTENSIONS
    assert ".heif" in IMAGE_EXTENSIONS


def test_normalized_model_image_decodes_heic() -> None:
    image = normalized_model_image(heic_bytes())
    with Image.open(BytesIO(image)) as rendered:
        assert rendered.format == "JPEG"
        assert rendered.size == (32, 32)


def test_prepare_attachment_image_normalizes_heic_to_jpeg() -> None:
    image, name = prepare_attachment_image(
        content=heic_bytes(), mime_type="image/heic", filename="IMG_0001.HEIC"
    )
    assert name == "attachment.jpg"
    with Image.open(BytesIO(image)) as rendered:
        assert rendered.format == "JPEG"


def test_attachment_vision_prompt_points_agent_at_input_file() -> None:
    prompt = attachment_vision_prompt(
        image_name="attachment.jpg",
        candidate={"filename": "logo.png", "mime_type": "image/png"},
    )
    payload = json.loads(prompt)

    # The path must be a concrete working-directory-relative path that needs no
    # shell/env expansion. Image-viewing tools (e.g. codex's view_image) receive
    # this string verbatim; advertising "$AGENT_INPUT_DIR/..." made the model
    # guess the expansion and drop the "inputs/" segment, so the agent opened a
    # nonexistent "<workdir>/attachment.jpg" ("unable to locate image").
    assert payload["image"]["path"] == "inputs/attachment.jpg"
    assert "$AGENT_INPUT_DIR" not in prompt
    assert payload["image"]["original_filename"] == "logo.png"
    assert payload["final_output_contract"]["schema"] == attachment_vision_schema()
    assert "view_image" in payload["image"]["how_to_view"]


def test_validate_attachment_vision_result_flags_bad_shapes() -> None:
    assert validate_attachment_vision_result(useful_output()) == []

    issues = validate_attachment_vision_result({"is_useful": "yes", "visible_text": "SPACEX"})
    assert any("is_useful" in issue for issue in issues)
    assert any("visible_text" in issue for issue in issues)

    issues = validate_attachment_vision_result({**useful_output(), "summary": "  "})
    assert any("summary must not be empty" in issue for issue in issues)


def test_attachment_enrichment_text_filters_non_indexable_values() -> None:
    text = attachment_enrichment_text(
        {
            **useful_output(),
            "visible_text": ["SPACEX", "none", "N/A"],
            "entities": ["SpaceX", "unknown"],
            "uncertainties": ["bottom row hard to read"],
        }
    )

    assert "SPACEX" in text
    assert "none" not in text.split("Visible text:\n", 1)[1].split("\n\n")[0]
    assert "Entities: SpaceX" in text
    assert "Uncertainties:\nbottom row hard to read" in text


def test_attachment_enrichment_text_empty_for_non_useful() -> None:
    assert attachment_enrichment_text({**useful_output(), "is_useful": False}) == ""


def test_attachment_storage_ref_uses_storage_columns() -> None:
    ref = attachment_storage_ref(
        {
            "storage_backend": "google_drive",
            "storage_key": "k",
            "storage_file_id": "f",
            "storage_url": "u",
            "filename": "ignored.png",
        }
    )
    assert ref == {
        "storage_backend": "google_drive",
        "storage_key": "k",
        "storage_file_id": "f",
        "storage_url": "u",
    }
