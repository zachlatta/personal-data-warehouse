"""Upload new lines from local agent session transcripts through app ingest.

The transcripts append over time, so each run reads only the bytes past the
last committed offset, wraps every new JSONL line in an envelope, and ships them
as gzipped JSONL batches to the app. The app writes
``agent-sessions/inbox/batches/`` objects. A file's offset only advances after
its lines are durably uploaded, so a crash mid-run simply re-ships the
un-acknowledged tail next time (ingest dedupes by primary key).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import gzip
import json
import tempfile

from personal_data_warehouse_agent_sessions.scanner import SessionFile, discover_session_files
from personal_data_warehouse_agent_sessions.state import AgentSessionsUploadState

DEFAULT_BATCH_SIZE = 20000

# Storage reference returned by the ingest client (mirrors the warehouse
# storage_* columns); the app owns the keys and tags behind it.
StoredObject = dict[str, str]


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
        openclaw_sessions_dir: Path | str | None = None,
        batch_uploader: Callable[[bytes, datetime], StoredObject],
        logger,
        upload_state: AgentSessionsUploadState | None = None,
        now: Callable[[], datetime] | None = None,
        mode: str = "incremental",
        limit: int | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        before_upload_check: Callable[[], str | None] | None = None,
    ) -> None:
        # Uploads go through the app's ingest endpoint only; batch_uploader posts
        # a gzipped batch and returns its stored reference.
        if batch_uploader is None:
            raise ValueError("batch_uploader is required")
        if mode not in {"full", "incremental"}:
            raise ValueError("mode must be 'full' or 'incremental'")
        self._account = account
        self._device = device
        self._claude_projects_dir = claude_projects_dir
        self._codex_sessions_dir = codex_sessions_dir
        self._openclaw_sessions_dir = openclaw_sessions_dir
        self._batch_uploader = batch_uploader
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
            openclaw_sessions_dir=self._openclaw_sessions_dir,
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
        # The app owns the object key, kind, and pdw_* tags; we send only the
        # gzipped batch and its export time.
        encoded = _gzip_jsonl(records)
        return self._batch_uploader(encoded, self._now())

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
