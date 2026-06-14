"""Upload new lines from local agent session transcripts to the Drive inbox.

The transcripts append over time, so each run reads only the bytes past the
last committed offset, wraps every new JSONL line in an envelope, and ships them
as gzipped JSONL batches into ``agent-sessions/inbox/batches/``. A file's offset
only advances after its lines are durably uploaded, so a crash mid-run simply
re-ships the un-acknowledged tail next time (ingest dedupes by primary key).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import gzip
import hashlib
import json
import tempfile

from personal_data_warehouse.objectstore import ObjectStore, StoredObject
from personal_data_warehouse_agent_sessions.scanner import SessionFile, discover_session_files
from personal_data_warehouse_agent_sessions.state import AgentSessionsUploadState

OBJECT_PREFIX = "agent-sessions"
INBOX_PREFIX = f"{OBJECT_PREFIX}/inbox"
BATCH_KIND = "agent_sessions_export_batch"
DEFAULT_BATCH_SIZE = 20000


@dataclass(frozen=True)
class AgentSessionsUploadSummary:
    files_seen: int
    files_with_new_lines: int
    lines_selected: int
    lines_skipped: int
    batches_uploaded: int


class AgentSessionsUploadRunner:
    def __init__(
        self,
        *,
        account: str,
        device: str,
        claude_projects_dir: Path | str | None,
        codex_sessions_dir: Path | str | None,
        object_store: ObjectStore | None = None,
        object_store_factory: Callable[[], ObjectStore] | None = None,
        logger,
        upload_state: AgentSessionsUploadState | None = None,
        now: Callable[[], datetime] | None = None,
        mode: str = "incremental",
        limit: int | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        before_upload_check: Callable[[], str | None] | None = None,
    ) -> None:
        if object_store is None and object_store_factory is None:
            raise ValueError("object_store or object_store_factory must be provided")
        if object_store is not None and object_store_factory is not None:
            raise ValueError("pass only one of object_store or object_store_factory")
        if mode not in {"full", "incremental"}:
            raise ValueError("mode must be 'full' or 'incremental'")
        self._account = account
        self._device = device
        self._claude_projects_dir = claude_projects_dir
        self._codex_sessions_dir = codex_sessions_dir
        self._object_store = object_store
        self._object_store_factory = object_store_factory
        self._logger = logger
        self._upload_state = upload_state
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._mode = mode
        self._limit = limit if (limit is None or limit > 0) else None
        self._batch_size = max(1, batch_size)
        self._before_upload_check = before_upload_check
        self._checked_network = False

    def sync(self) -> AgentSessionsUploadSummary:
        files = discover_session_files(
            claude_projects_dir=self._claude_projects_dir,
            codex_sessions_dir=self._codex_sessions_dir,
        )
        self._logger.info("Discovered %s agent session transcript file(s)", len(files))
        remaining = self._limit
        files_with_new = 0
        lines_selected = 0
        lines_skipped = 0
        batches = 0
        skipped_for_limit = False

        for session_file in files:
            if remaining is not None and remaining <= 0:
                skipped_for_limit = True
                break
            file_result = self._sync_file(session_file, remaining=remaining)
            if file_result is None:
                continue
            if file_result.lines_selected:
                files_with_new += 1
            lines_selected += file_result.lines_selected
            lines_skipped += file_result.lines_skipped
            batches += file_result.batches_uploaded
            if remaining is not None:
                remaining -= file_result.lines_selected
            if file_result.deferred:
                skipped_for_limit = True
                break

        self._logger.info(
            "Agent sessions upload summary: files=%s new=%s lines=%s skipped=%s batches=%s%s",
            len(files),
            files_with_new,
            lines_selected,
            lines_skipped,
            batches,
            " (run limit reached; remaining lines deferred to next run)" if skipped_for_limit else "",
        )
        return AgentSessionsUploadSummary(
            files_seen=len(files),
            files_with_new_lines=files_with_new,
            lines_selected=lines_selected,
            lines_skipped=lines_skipped,
            batches_uploaded=batches,
        )

    def _sync_file(self, session_file: SessionFile, *, remaining: int | None) -> "_FileResult | None":
        path = session_file.path
        try:
            size = path.stat().st_size
        except OSError:
            return None

        start_offset, start_line = self._start_position(path, size)
        if size <= start_offset:
            return _FileResult(lines_selected=0, lines_skipped=0, batches_uploaded=0, deferred=False)

        with path.open("rb") as handle:
            handle.seek(start_offset)
            data = handle.read()

        offset = start_offset
        line_no = start_line
        selected = 0
        skipped = 0
        batches = 0
        deferred = False
        buffer: list[dict[str, object]] = []

        for raw_line in _complete_lines(data):
            if remaining is not None and remaining - selected <= 0:
                deferred = True
                break
            offset += len(raw_line)
            seq = line_no
            line_no += 1
            text = raw_line.decode("utf-8", errors="replace").strip()
            if not text:
                skipped += 1
                continue
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                self._logger.warning("Skipping unparseable line %s in %s", seq, path)
                skipped += 1
                continue
            if not isinstance(parsed, dict):
                skipped += 1
                continue
            buffer.append(self._envelope(session_file, seq=seq, line=parsed))
            selected += 1
            if len(buffer) >= self._batch_size:
                self._upload_and_commit(path, buffer, offset=offset, line_no=line_no)
                batches += 1
                buffer = []

        if buffer:
            self._upload_and_commit(path, buffer, offset=offset, line_no=line_no)
            batches += 1
        elif offset != start_offset:
            # Consumed only skipped/blank lines; still advance so we do not
            # reprocess them next run.
            self._commit(path, offset=offset, line_no=line_no)

        return _FileResult(lines_selected=selected, lines_skipped=skipped, batches_uploaded=batches, deferred=deferred)

    def _start_position(self, path: Path, size: int) -> tuple[int, int]:
        if self._mode != "incremental" or self._upload_state is None:
            return 0, 0
        progress = self._upload_state.progress_for(str(path))
        if progress.uploaded_offset > size:
            # File was truncated/rotated; re-read from the beginning.
            return 0, 0
        return progress.uploaded_offset, progress.uploaded_lines

    def _upload_and_commit(self, path: Path, records: list[dict[str, object]], *, offset: int, line_no: int) -> None:
        self._run_network_check()
        stored = self._upload_batch(records)
        self._commit(path, offset=offset, line_no=line_no)
        self._logger.info("Uploaded agent-sessions batch %s with %s records", stored["storage_key"], len(records))

    def _commit(self, path: Path, *, offset: int, line_no: int) -> None:
        if self._upload_state is not None:
            self._upload_state.record_progress(
                path=str(path), uploaded_offset=offset, uploaded_lines=line_no, now=self._now()
            )

    def _run_network_check(self) -> None:
        if self._before_upload_check is None or self._checked_network:
            return
        self._checked_network = True
        reason = self._before_upload_check()
        if reason:
            raise UploadBlockedError(reason)

    def _upload_batch(self, records: list[dict[str, object]]) -> StoredObject:
        exported_at = self._now()
        encoded = _gzip_jsonl(records)
        batch_sha = hashlib.sha256(encoded).hexdigest()
        object_key = _batch_object_key(exported_at=exported_at, batch_sha256=batch_sha)
        with tempfile.NamedTemporaryFile(suffix=".jsonl.gz") as file:
            file.write(encoded)
            file.flush()
            return self._object_store_instance().put_file(
                path=Path(file.name),
                object_key=object_key,
                content_sha256=batch_sha,
                content_type="application/gzip",
                kind=BATCH_KIND,
                app_properties={"batch_sha256": batch_sha, "exported_at": exported_at.astimezone(UTC).isoformat()},
            )

    def _object_store_instance(self) -> ObjectStore:
        if self._object_store is None:
            if self._object_store_factory is None:
                raise RuntimeError("object_store_factory is not configured")
            self._object_store = self._object_store_factory()
        return self._object_store

    def _envelope(self, session_file: SessionFile, *, seq: int, line: dict[str, object]) -> dict[str, object]:
        return {
            "schema_version": 1,
            "source": "agent_sessions",
            "account": self._account,
            "device": self._device,
            "exported_at": self._now().astimezone(UTC).isoformat(),
            "record_type": f"{session_file.tool}_event",
            "record": {
                "tool": session_file.tool,
                "session_id": session_file.session_id,
                "seq": seq,
                "line": line,
            },
        }


class UploadBlockedError(RuntimeError):
    """Raised when a network/preflight guard blocks the upload."""


@dataclass(frozen=True)
class _FileResult:
    lines_selected: int
    lines_skipped: int
    batches_uploaded: int
    deferred: bool


def _complete_lines(data: bytes) -> list[bytes]:
    """Return only newline-terminated lines (each including its trailing \\n).

    A trailing partial line (no newline yet, e.g. a transcript being written) is
    left unconsumed so the next run picks it up once complete.
    """
    lines: list[bytes] = []
    start = 0
    while True:
        index = data.find(b"\n", start)
        if index == -1:
            break
        lines.append(data[start : index + 1])
        start = index + 1
    return lines


def _gzip_jsonl(records: list[dict[str, object]]) -> bytes:
    lines = b"".join(
        json.dumps(record, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8") + b"\n"
        for record in records
    )
    with tempfile.NamedTemporaryFile() as file:
        with gzip.GzipFile(fileobj=file, mode="wb", mtime=0) as gzip_file:
            gzip_file.write(lines)
        file.flush()
        file.seek(0)
        return file.read()


def _batch_object_key(*, exported_at: datetime, batch_sha256: str) -> str:
    exported = exported_at.astimezone(UTC)
    stamp = exported.strftime("%Y%m%dT%H%M%SZ")
    return f"{INBOX_PREFIX}/batches/{exported.year:04d}/{exported.month:02d}/{stamp}-{batch_sha256}.jsonl.gz"
