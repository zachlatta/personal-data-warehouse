"""Uploader behavior for manual finance documents (scan, envelope, state)."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pytest

from personal_data_warehouse_manual_finance.envelope import (
    build_document_metadata,
    provenance_dedup_sha256,
)
from personal_data_warehouse_manual_finance.state import ManualFinanceUploadState
from personal_data_warehouse_manual_finance.sync import (
    ManualFinanceUploadRunner,
    document_content_type,
    resolve_candidates,
)

_TS = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)


class Logger:
    def info(self, *args) -> None:
        pass

    def warning(self, *args) -> None:
        pass


class FakeIngestClient:
    def __init__(self, *, fail_shas: set[str] | None = None):
        self.files: list[dict] = []
        self.metadata: list[dict] = []
        self._fail_shas = fail_shas or set()

    def upload_manual_finance_document(self, content, **kwargs):
        sha = hashlib.sha256(content).hexdigest()
        if sha in self._fail_shas:
            raise RuntimeError("boom")
        self.files.append({"sha": sha, **kwargs})
        return {"storage_key": f"manual-finance/inbox/x/{sha}"}

    def upload_manual_finance_metadata(self, payload, **kwargs):
        self.metadata.append({"payload": payload, **kwargs})
        return {"storage_key": "meta"}


def _corpus(tmp_path: Path) -> Path:
    root = tmp_path / "accounts"
    (root / "acme-checking-0001").mkdir(parents=True)
    (root / "acme-checking-0001" / "acme-checking-2026-06.pdf").write_bytes(b"%PDF june")
    (root / "acme-checking-0001" / ".DS_Store").write_bytes(b"junk")
    (root / "acme-card-0002").mkdir()
    (root / "acme-card-0002" / "positions.rtf").write_bytes(b"{\\rtf1 positions}")
    (root / "notes.txt").write_bytes(b"unsupported")
    (root / "screenshot.png").write_bytes(b"png-bytes")
    return root


# --- envelope ----------------------------------------------------------------


def test_provenance_sha_ignores_path_but_not_account():
    a = provenance_dedup_sha256(source="manual", account="z@x.test", native_id="sha1", file_content_sha256="sha1")
    b = provenance_dedup_sha256(source="manual", account="z@x.test", native_id="sha1", file_content_sha256="sha1")
    assert a == b
    assert a != provenance_dedup_sha256(
        source="manual", account="other@x.test", native_id="sha1", file_content_sha256="sha1"
    )


def test_build_document_metadata_contract():
    envelope = build_document_metadata(
        account="z@x.test",
        filename="statement.pdf",
        original_path="acme-checking-0001/statement.pdf",
        mime_type="application/pdf",
        size_bytes=10,
        content_sha256="sha1",
        uploaded_at=_TS.isoformat(),
    )
    assert envelope["source"] == "manual"
    assert envelope["file"]["native_id"] == "sha1"
    assert envelope["file"]["original_path"] == "acme-checking-0001/statement.pdf"
    with pytest.raises(ValueError, match="content_sha256"):
        build_document_metadata(
            account="z@x.test",
            filename="statement.pdf",
            original_path="",
            mime_type="application/pdf",
            size_bytes=10,
            content_sha256="",
            uploaded_at=_TS.isoformat(),
        )


# --- scanning ------------------------------------------------------------------


def test_resolve_candidates_preserves_folder_organization(tmp_path):
    root = _corpus(tmp_path)
    candidates, ignored = resolve_candidates([root], root=None)
    by_path = {candidate.original_path: candidate for candidate in candidates}
    assert set(by_path) == {
        "acme-checking-0001/acme-checking-2026-06.pdf",
        "acme-card-0002/positions.rtf",
        "screenshot.png",
    }
    # .DS_Store (hidden) and notes.txt (unsupported extension) are ignored.
    assert ignored == 2
    assert by_path["acme-checking-0001/acme-checking-2026-06.pdf"].account_folder == "acme-checking-0001"
    # Files directly under the root carry no account folder (server maps to "unsorted").
    assert by_path["screenshot.png"].account_folder == ""


def test_resolve_candidates_single_file_uses_parent_as_root(tmp_path):
    root = _corpus(tmp_path)
    single = root / "acme-card-0002" / "positions.rtf"
    candidates, _ = resolve_candidates([single], root=None)
    assert [candidate.original_path for candidate in candidates] == ["positions.rtf"]
    # With an explicit --root the folder hint is preserved.
    candidates, _ = resolve_candidates([single], root=root)
    assert [candidate.original_path for candidate in candidates] == ["acme-card-0002/positions.rtf"]
    assert candidates[0].account_folder == "acme-card-0002"


def test_document_content_type_covers_finance_formats(tmp_path):
    assert document_content_type(Path("a.pdf")) == "application/pdf"
    assert document_content_type(Path("a.ofx")) == "application/x-ofx"
    assert document_content_type(Path("a.rtf")) == "text/rtf"
    assert document_content_type(Path("a.csv")) == "text/csv"


# --- runner --------------------------------------------------------------------


def test_runner_uploads_blob_and_envelope(tmp_path):
    root = _corpus(tmp_path)
    client = FakeIngestClient()
    summary = ManualFinanceUploadRunner(
        account="z@x.test",
        paths=[root],
        ingest_client=client,
        logger=Logger(),
        now=lambda: _TS,
    ).sync()
    assert summary.files_uploaded == 3
    assert summary.metadata_uploaded == 3
    uploaded_folders = {upload["account_folder"] for upload in client.files}
    assert uploaded_folders == {"acme-checking-0001", "acme-card-0002", ""}
    envelope = next(
        upload["payload"]
        for upload in client.metadata
        if upload["payload"]["file"]["filename"] == "positions.rtf"
    )
    assert envelope["file"]["original_path"] == "acme-card-0002/positions.rtf"
    assert envelope["account"] == "z@x.test"
    metadata = client.metadata[0]
    assert metadata["metadata_dedup_sha256"] == provenance_dedup_sha256(
        source="manual",
        account="z@x.test",
        native_id=metadata["file_content_sha256"],
        file_content_sha256=metadata["file_content_sha256"],
    )


def test_runner_skips_state_complete_documents(tmp_path):
    root = _corpus(tmp_path)
    state = ManualFinanceUploadState.open(tmp_path / "state.sqlite", account="z@x.test")
    client = FakeIngestClient()
    runner_kwargs = dict(
        account="z@x.test",
        paths=[root],
        ingest_client=client,
        logger=Logger(),
        now=lambda: _TS,
        upload_state=state,
    )
    ManualFinanceUploadRunner(**runner_kwargs).sync()
    assert len(client.files) == 3
    summary = ManualFinanceUploadRunner(**runner_kwargs).sync()
    assert summary.files_skipped == 3
    assert summary.files_uploaded == 0
    assert len(client.files) == 3
    # A moved file keeps its sha, so it stays skipped (state is content-keyed).
    moved = root / "acme-checking-0001" / "renamed.pdf"
    (root / "acme-checking-0001" / "acme-checking-2026-06.pdf").rename(moved)
    summary = ManualFinanceUploadRunner(**runner_kwargs).sync()
    assert summary.files_uploaded == 0
    state.close()


def test_runner_collects_failures_and_reraises_after_batch(tmp_path):
    root = _corpus(tmp_path)
    failing_sha = hashlib.sha256(b"%PDF june").hexdigest()
    state = ManualFinanceUploadState.open(tmp_path / "state.sqlite", account="z@x.test")
    client = FakeIngestClient(fail_shas={failing_sha})
    with pytest.raises(RuntimeError, match="boom"):
        ManualFinanceUploadRunner(
            account="z@x.test",
            paths=[root],
            ingest_client=client,
            logger=Logger(),
            now=lambda: _TS,
            upload_state=state,
        ).sync()
    # The other documents still uploaded and are recorded complete.
    assert len(client.files) == 2
    assert not state.is_complete(content_sha256=failing_sha)
    state.close()


def test_runner_limit_bounds_a_run(tmp_path):
    root = _corpus(tmp_path)
    client = FakeIngestClient()
    summary = ManualFinanceUploadRunner(
        account="z@x.test",
        paths=[root],
        ingest_client=client,
        logger=Logger(),
        now=lambda: _TS,
        limit=1,
    ).sync()
    assert summary.files_uploaded == 1
