from __future__ import annotations

from datetime import UTC, datetime
import hashlib
from io import BytesIO
import json

import pytest
from PIL import Image

from personal_data_warehouse.agent_runner import AgentRunRequest, AgentRunResult
from personal_data_warehouse.gmail_attachment_enrichment import (
    AGENT_ATTACHMENT_PROMPT_VERSION,
    AGENT_ATTACHMENT_TASK_TYPE,
    GmailAttachmentEnrichmentRunner,
    STATUS_ERROR,
    STATUS_NOT_USEFUL,
    STATUS_OK,
    attachment_enrichment_text,
    attachment_storage_ref,
    attachment_vision_prompt,
    attachment_vision_schema,
    normalized_model_image,
    prepare_attachment_image,
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

    def ensure_tables(self) -> None:
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


def make_runner(warehouse, agent, store) -> GmailAttachmentEnrichmentRunner:
    return GmailAttachmentEnrichmentRunner(
        warehouse=warehouse,
        agent=agent,
        object_store_factory=lambda account: store,
        logger=FakeLogger(),
        provider="codex",
        model="",
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


def test_attachment_vision_prompt_points_agent_at_input_file() -> None:
    prompt = attachment_vision_prompt(
        image_name="attachment.jpg",
        candidate={"filename": "logo.png", "mime_type": "image/png"},
    )
    payload = json.loads(prompt)

    assert payload["image"]["path"] == "$AGENT_INPUT_DIR/attachment.jpg"
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
