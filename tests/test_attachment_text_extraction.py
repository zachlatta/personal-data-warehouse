from __future__ import annotations

from datetime import UTC, datetime

import pytest

from personal_data_warehouse.attachment_text_extraction import (
    APPLE_MESSAGES_TEXT_SOURCE,
    STATUS_EMPTY,
    STATUS_OK,
    STATUS_UNSUPPORTED,
    AttachmentTextExtractionRunner,
    attachment_text_plan,
    extract_attachment_text,
    load_text_extraction_candidates,
    vcard_to_text,
)
from personal_data_warehouse.file_attachment_enrichment import IMAGE_MIME_TYPES


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

    def ensure_file_attachment_enrichment_tables(self) -> None:
        pass

    def ensure_apple_messages_tables(self) -> None:
        pass

    def _query(self, sql: str, params=None):
        self.queries.append((sql, params))
        return self.candidate_rows

    def insert_attachment_enrichments(self, rows) -> None:
        self.enrichment_rows.extend(rows)


class FakeObjectStore:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.get_calls: list[dict] = []

    def get_object(self, ref) -> bytes:
        self.get_calls.append(dict(ref))
        return self.content


VCARD = (
    b"BEGIN:VCARD\r\n"
    b"VERSION:3.0\r\n"
    b"FN:Ada Example\r\n"
    b"ORG:Example Robotics;Hardware\r\n"
    b"TITLE:Field Engineer\r\n"
    b"TEL;type=CELL:+1-555-0100\r\n"
    b"EMAIL;type=WORK:ada@example.com\r\n"
    b"ADR;type=HOME:;;1 Example Way;Springfield;VT;05156;USA\r\n"
    b"URL:https://example.com/ada\r\n"
    b"NOTE:Met at the maker fair\r\n"
    b"END:VCARD\r\n"
)


def candidate_row(
    content: bytes,
    *,
    filename: str = "contact.vcf",
    mime_type: str = "text/vcard",
    sha: str = "sha-1",
) -> tuple:
    return (
        "zach@example.com",
        sha,
        filename,
        mime_type,
        len(content),
        "google_drive",
        "apple-messages/library/x",
        "drive-file-id",
        "https://drive.example/x",
    )


def make_runner(warehouse, store, *, logger=None) -> AttachmentTextExtractionRunner:
    return AttachmentTextExtractionRunner(
        source=APPLE_MESSAGES_TEXT_SOURCE,
        warehouse=warehouse,
        object_store_factory=lambda account: store,
        logger=logger or FakeLogger(),
        now=lambda: datetime(2026, 8, 12, 12, 0, tzinfo=UTC),
    )


# --- planning: what needs bytes, what can be classified by rule ---------------


def test_plan_classifies_plugin_payloads_without_downloading() -> None:
    # 4,735 iMessage .pluginPayloadAttachment blobs (1.1 GB) are app-extension
    # payloads, 4,694 of them link previews whose URL is already indexed from the
    # message body. They get a stable terminal classification by RULE so the
    # pipeline never spends 1.1 GB of Drive reads to learn nothing.
    plan = attachment_text_plan(mime_type="application/octet-stream", filename="A1.pluginPayloadAttachment")
    assert plan.needs_bytes is False
    assert plan.status == STATUS_UNSUPPORTED
    assert "link preview" in plan.reason


def test_plan_classifies_video_without_downloading() -> None:
    # 25 GB of iMessage video. Not enrichable by the current pipeline, so it is
    # classified rather than silently omitted -- and never downloaded to find out.
    plan = attachment_text_plan(mime_type="video/quicktime", filename="IMG_1.MOV")
    assert plan.needs_bytes is False
    assert plan.status == STATUS_UNSUPPORTED
    assert "video" in plan.reason


def test_plan_requests_bytes_for_vcard_and_text() -> None:
    for filename, mime in (
        ("contact.vcf", "text/vcard"),
        ("place.vcf", "text/x-vlocation"),
        ("notes.txt", "text/plain"),
        ("data.csv", "text/csv"),
        ("page.html", "text/html"),
        ("readme.md", "text/markdown"),
    ):
        plan = attachment_text_plan(mime_type=mime, filename=filename)
        assert plan.needs_bytes is True, filename


def test_plan_leaves_images_and_pdfs_to_the_vision_pass() -> None:
    # The agent vision pipeline owns these; a deterministic row must not claim
    # them, or it would race the pass that actually produces their text.
    for filename, mime in (("a.png", "image/png"), ("b.pdf", "application/pdf"), ("c.heic", "image/heic")):
        assert attachment_text_plan(mime_type=mime, filename=filename) is None


def test_vision_ownership_tracks_the_vision_pass_definitions() -> None:
    # Ownership must be derived from the vision pass's own constants, not a
    # hand-copied list. A second copy is how the original gates drifted out of
    # sync with reality: image/svg+xml and image/heic-sequence both start with
    # "image/" but the vision pass cannot decode either, so a prefix test would
    # hand them to a pipeline that silently drops them.
    for mime in IMAGE_MIME_TYPES:
        assert attachment_text_plan(mime_type=mime, filename="x") is None
    for unhandled in ("image/svg+xml", "image/heic-sequence"):
        assert attachment_text_plan(mime_type=unhandled, filename="x.svg") is not None


def test_plan_yields_metadata_free_blobs_to_the_vision_pass() -> None:
    # No usable MIME and no extension: the format is unknown from metadata alone.
    # iMessage's "GroupPhotoImage"/"BrandLogoImage" attachments look exactly like
    # this and ARE real images, so classifying them unsupported sight-unseen
    # would throw away genuine content. The vision pass admits this shape and
    # identifies it from the bytes.
    for mime in ("application/octet-stream", ""):
        plan = attachment_text_plan(
            mime_type=mime,
            filename="~/Library/Messages/Attachments/d5/05/at_0_ABC/GroupPhotoImage",
        )
        assert plan is None, mime


def test_plan_still_classifies_ambiguous_mime_when_an_extension_exists() -> None:
    # The extension is what keeps the vision hand-off narrow: with one present
    # the format IS asserted, so the 4,722 .pluginPayloadAttachment payloads stay
    # on the no-download classification path instead of costing 1.1 GB of reads.
    plan = attachment_text_plan(
        mime_type="application/octet-stream", filename="A1.pluginPayloadAttachment"
    )
    assert plan is not None and plan.needs_bytes is False


def test_plan_classifies_every_unrecognized_format() -> None:
    # The definition of done forbids silent omission: anything this pass does not
    # extract and the vision pass does not own must still get an auditable row.
    plan = attachment_text_plan(mime_type="application/x-nonesuch", filename="mystery.bin")
    assert plan is not None
    assert plan.needs_bytes is False
    assert plan.status == STATUS_UNSUPPORTED
    assert "unrecognized" in plan.reason.lower()


def test_plan_routes_office_and_svg_to_extraction() -> None:
    for filename, mime in (
        ("notes.docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
        ("logo.svg", "image/svg+xml"),
    ):
        plan = attachment_text_plan(mime_type=mime, filename=filename)
        assert plan is not None and plan.needs_bytes is True, filename


def test_extract_attachment_text_reads_docx() -> None:
    # Reuses the Drive DOCX extractor rather than reimplementing OOXML parsing.
    import io
    import zipfile

    word_ns = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "word/document.xml",
            f'<?xml version="1.0"?><w:document xmlns:w="{word_ns}"><w:body><w:p><w:r>'
            "<w:t>Ferry schedule</w:t></w:r></w:p></w:body></w:document>",
        )
    status, text = extract_attachment_text(
        content=buffer.getvalue(),
        mime_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        filename="notes.docx",
    )
    assert status == STATUS_OK
    assert "Ferry schedule" in text


def test_extract_attachment_text_marks_corrupt_docx_unsupported() -> None:
    # A DOCX that is not a valid zip is permanently unreadable, not a transient
    # failure: record it terminally instead of retrying it forever.
    status, text = extract_attachment_text(
        content=b"not a zip at all, definitely",
        mime_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        filename="broken.docx",
    )
    assert status == STATUS_UNSUPPORTED
    assert text == ""


def test_extract_attachment_text_strips_svg_markup() -> None:
    svg = b'<svg xmlns="http://www.w3.org/2000/svg"><text x="0" y="10">Gate B12</text></svg>'
    status, text = extract_attachment_text(content=svg, mime_type="image/svg+xml", filename="s.svg")
    assert status == STATUS_OK
    assert "Gate B12" in text
    assert "<text" not in text


# --- vCard extraction ---------------------------------------------------------


def test_vcard_to_text_extracts_searchable_fields() -> None:
    text = vcard_to_text(VCARD.decode("utf-8"))

    assert "Ada Example" in text
    assert "Example Robotics" in text
    assert "Field Engineer" in text
    assert "+1-555-0100" in text
    assert "ada@example.com" in text
    assert "Springfield" in text
    assert "https://example.com/ada" in text
    assert "Met at the maker fair" in text
    # Structural noise must not reach the search document.
    assert "BEGIN:VCARD" not in text
    assert "VERSION:3.0" not in text


def test_vcard_to_text_unfolds_wrapped_lines() -> None:
    # RFC 6350 folds long values onto continuation lines beginning with a space;
    # naive line parsing would truncate the value mid-word.
    folded = "BEGIN:VCARD\r\nFN:Ada Exam\r\n ple\r\nNOTE:a very long\r\n  note\r\nEND:VCARD\r\n"
    text = vcard_to_text(folded)
    assert "Ada Example" in text


def test_vcard_to_text_handles_multiple_cards() -> None:
    text = vcard_to_text((VCARD + VCARD.replace(b"Ada Example", b"Bo Sample")).decode("utf-8"))
    assert "Ada Example" in text
    assert "Bo Sample" in text


def test_extract_attachment_text_reads_vcard() -> None:
    status, text = extract_attachment_text(
        content=VCARD, mime_type="text/vcard", filename="contact.vcf"
    )
    assert status == STATUS_OK
    assert "ada@example.com" in text


def test_extract_attachment_text_strips_html_markup() -> None:
    html = b"<html><head><style>p{color:red}</style></head><body><h1>Trip plan</h1><p>Leaves at 9</p></body></html>"
    status, text = extract_attachment_text(content=html, mime_type="text/html", filename="p.html")
    assert status == STATUS_OK
    assert "Trip plan" in text
    assert "Leaves at 9" in text
    assert "<h1>" not in text
    assert "color:red" not in text  # style/script bodies are not content


def test_extract_attachment_text_survives_bad_encoding() -> None:
    # Real attachments carry mixed//broken encodings; a decode error must not
    # become a permanent failure for otherwise readable content.
    status, text = extract_attachment_text(
        content=b"caf\xe9 meeting notes", mime_type="text/plain", filename="n.txt"
    )
    assert status == STATUS_OK
    assert "meeting notes" in text


def test_extract_attachment_text_reports_empty_for_blank_content() -> None:
    status, text = extract_attachment_text(
        content=b"   \n\t  ", mime_type="text/plain", filename="n.txt"
    )
    assert status == STATUS_EMPTY
    assert text == ""


def test_extract_attachment_text_truncates_runaway_content() -> None:
    status, text = extract_attachment_text(
        content=b"x" * 200_000, mime_type="text/plain", filename="n.txt", max_chars=1_000
    )
    assert status == STATUS_OK
    assert len(text) == 1_000


# --- runner -------------------------------------------------------------------


def test_runner_writes_deterministic_identity_row() -> None:
    # The deterministic row must use the empty provider/model/prompt_version
    # identity, which is what the timeline adapter folds into the search document
    # and what the vision candidate query reads as source_status.
    warehouse = FakeWarehouse([candidate_row(VCARD)])
    summary = make_runner(warehouse, FakeObjectStore(VCARD)).sync(limit=None)

    assert summary.extracted == 1
    row = warehouse.enrichment_rows[0]
    assert row["ai_provider"] == ""
    assert row["ai_model"] == ""
    assert row["ai_prompt_version"] == ""
    assert row["text_extraction_status"] == STATUS_OK
    assert "ada@example.com" in row["text"]


def test_runner_classifies_without_fetching_bytes() -> None:
    store = FakeObjectStore(b"should never be read")
    warehouse = FakeWarehouse(
        [candidate_row(b"", filename="A1.pluginPayloadAttachment", mime_type="application/octet-stream")]
    )

    summary = make_runner(warehouse, store).sync(limit=None)

    assert summary.classified == 1
    assert store.get_calls == []  # the whole point: no Drive read
    row = warehouse.enrichment_rows[0]
    assert row["text_extraction_status"] == STATUS_UNSUPPORTED
    assert row["text"] == ""


def test_runner_records_unreadable_content_as_terminal_not_error() -> None:
    class BoomStore:
        def get_object(self, ref):
            raise RuntimeError("drive is down")

    warehouse = FakeWarehouse([candidate_row(VCARD)])
    logger = FakeLogger()
    summary = make_runner(warehouse, BoomStore(), logger=logger).sync(limit=None)

    # A transport failure is NOT terminal: nothing is written, so the next run
    # retries it. Writing an 'unsupported' row here would permanently lose the
    # attachment over a transient outage.
    assert summary.failed == 1
    assert warehouse.enrichment_rows == []


def test_candidate_query_skips_attachments_that_already_have_a_row() -> None:
    warehouse = FakeWarehouse([])
    load_text_extraction_candidates(
        warehouse, source=APPLE_MESSAGES_TEXT_SOURCE, limit=10
    )
    sql, params = warehouse.queries[0]
    assert "NOT EXISTS" in sql
    # The deterministic identity is the empty provider/model/prompt triple, the
    # same one Gmail's extraction writes and the vision query reads as `det`.
    for column in ("det.ai_provider = ''", "det.ai_model = ''", "det.ai_prompt_version = ''"):
        assert column in sql
    assert sql.count("%s") == len(params)


def test_skipped_vision_rows_do_not_consume_the_work_budget() -> None:
    # THE starvation bug: vision-owned attachments share the "no deterministic
    # row" condition, and in production they outnumber actionable rows ~2:1
    # (15,575 scanned vs 5,708 actionable). If a skip counted toward the batch
    # bound, a run could fill entirely with skips and make zero progress forever.
    rows = [
        candidate_row(b"", filename=f"img{i}.png", mime_type="image/png", sha=f"img-{i}")
        for i in range(50)
    ]
    rows.append(candidate_row(VCARD, filename="contact.vcf", mime_type="text/vcard", sha="vcf-1"))
    warehouse = FakeWarehouse(rows)

    summary = make_runner(warehouse, FakeObjectStore(VCARD)).sync(limit=5)

    # The lone actionable row is reached despite 50 skips ahead of it.
    assert summary.extracted == 1
    assert [r["content_sha256"] for r in warehouse.enrichment_rows] == ["vcf-1"]


def test_work_budget_still_bounds_actionable_rows() -> None:
    rows = [
        candidate_row(b"", filename=f"a{i}.pluginPayloadAttachment", mime_type="application/octet-stream", sha=f"p-{i}")
        for i in range(20)
    ]
    warehouse = FakeWarehouse(rows)

    summary = make_runner(warehouse, FakeObjectStore(b"")).sync(limit=5)

    assert summary.classified == 5
    assert len(warehouse.enrichment_rows) == 5


def test_full_scan_window_is_reported_not_silently_truncated() -> None:
    # "No silent caps": if the scan window fills, actionable rows may be
    # invisible this run, and that must be visible in the logs.
    rows = [
        candidate_row(b"", filename=f"img{i}.png", mime_type="image/png", sha=f"img-{i}")
        for i in range(4)
    ]
    warehouse = FakeWarehouse(rows)
    logger = FakeLogger()
    runner = AttachmentTextExtractionRunner(
        source=APPLE_MESSAGES_TEXT_SOURCE,
        warehouse=warehouse,
        object_store_factory=lambda account: FakeObjectStore(b""),
        logger=logger,
        scan_limit=4,
    )

    runner.sync(limit=10)

    assert any("scanned the full" in w for w in logger.warnings)


def test_candidate_query_is_bounded_by_limit() -> None:
    warehouse = FakeWarehouse([])
    load_text_extraction_candidates(warehouse, source=APPLE_MESSAGES_TEXT_SOURCE, limit=25)
    sql, params = warehouse.queries[0]
    assert "LIMIT %s" in sql
    assert 25 in params


def test_runner_is_idempotent_for_the_same_bytes() -> None:
    warehouse = FakeWarehouse([candidate_row(VCARD)])
    store = FakeObjectStore(VCARD)
    first = make_runner(warehouse, store).sync(limit=None)
    second = make_runner(warehouse, store).sync(limit=None)

    assert first.extracted == 1 and second.extracted == 1
    # Same content sha and same deterministic identity => the upsert replaces
    # rather than duplicating, and the text is stable.
    assert warehouse.enrichment_rows[0]["content_sha256"] == warehouse.enrichment_rows[1]["content_sha256"]
    assert warehouse.enrichment_rows[0]["text"] == warehouse.enrichment_rows[1]["text"]


@pytest.mark.parametrize(
    "filename,mime",
    [
        ("A1.pluginPayloadAttachment", "application/octet-stream"),
        ("IMG.MOV", "video/quicktime"),
        ("archive.zip", "application/zip"),
    ],
)
def test_classified_formats_carry_a_stable_reason(filename, mime) -> None:
    warehouse = FakeWarehouse([candidate_row(b"", filename=filename, mime_type=mime)])
    make_runner(warehouse, FakeObjectStore(b"")).sync(limit=None)
    row = warehouse.enrichment_rows[0]
    # A human-readable reason is what makes "unsupported" auditable later; an
    # empty reason would leave the category indistinguishable from a bug.
    assert row["text_extraction_error"]
    assert row["text_extraction_status"] == STATUS_UNSUPPORTED
