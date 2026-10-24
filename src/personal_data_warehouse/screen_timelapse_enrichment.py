"""Read each screen timelapse frame: was the device in use, and what was on it.

The screen timelapse (base_screen_timelapse.screenshots) is a whole-screen
image about once a minute per device and display, plus the accessibility tree
of the same moment when the capture loop could read one. Neither says in words
what was on the screen, and many frames show nothing at all: on 2026-10-24 the
iPhone had written 1,127 byte-identical black frames in five days, one a minute
while it lay asleep.

An agent reads every frame and decides two things: whether the device was in
use (screen_state active, locked or off), and what the screen showed (app,
window title, a summary and the main text). It reads the accessibility tree
first, because the tree carries exact text for free, and opens the screenshot
only when the tree does not say what is on screen. That call is the agent's:
on the 2026-10-24 samples a Mac tree named the query and the error in DataGrip,
while the phone's tree of an open Superhuman email held the buttons and the
sender but not one word of the email.

The tree is read from its inline copy (base_screen_timelapse.accessibility_trees
.content_text, written by the app at ingest); a tree stored before that column
existed is fetched once from the object store and its text written back, and
backfill_accessibility_tree_text does the same for every older tree.

Only the recent window is read (24 hours unless configured), newest first, so
a change to the prompt can be iterated on a day of frames before the history
is spent on it. Code fetches frames, pages candidates and records answers; a
byte-identical frame without a tree reuses the answer already given for those
bytes, because its input is the same. marts_screen_timelapse.active_screens
serves the frames an agent judged active.

All devices land in one raw table, so this reads it directly: there is no
second screen source for an intermediate layer to conform (contract C5's
registry says so in tests/test_repo_contracts.py).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import gzip
from io import BytesIO
import json
from typing import Any

from personal_data_warehouse.agent_runner import (
    DEFAULT_AGENT_INPUTS_DIR_NAME,
    AgentRunRequest,
    AgentRunResult,
    agent_run_event_rows,
    agent_run_row,
    agent_run_tool_call_rows,
)
from personal_data_warehouse.file_attachment_enrichment import normalized_model_image

SCREEN_DESCRIPTION_PROMPT_VERSION = "screen-description-v1"
SCREEN_DESCRIPTION_TASK_TYPE = "screen_timelapse_description"
DEFAULT_SCREEN_DESCRIPTION_LOOKBACK = timedelta(hours=24)
DEFAULT_SCREEN_DESCRIPTION_MAX_ERROR_ATTEMPTS = 3
# Screens are dense with small text. The attachment pass's 1280 px makes a
# Retina Mac frame (3600x2338) unreadable; 2048 keeps body text legible.
SCREEN_IMAGE_MAX_EDGE = 2048
# Measured 2026-10-24: a Mac tree 21 KB, the Pixel's 19 KB, the iPhone's
# 133 KB. The caps only guard against a pathological tree: the inline copy
# stops where the app's does (8 MiB), and the agent reads at most 400k chars.
ACCESSIBILITY_TREE_TEXT_MAX_BYTES = 8 << 20
ACCESSIBILITY_TREE_MAX_CHARS = 400_000
DEFAULT_TREE_TEXT_BACKFILL_LIMIT = 500
SCREENSHOT_INPUT = "screenshot.jpg"
SCREEN_STATES = ("active", "locked", "off")
EVIDENCE_KINDS = ("accessibility_tree", "screenshot", "both")
STATUS_COMPLETED = "completed"
STATUS_ERROR = "error"


@dataclass(frozen=True)
class ScreenTimelapseEnrichmentSummary:
    frames_seen: int
    frames_described: int
    frames_reused: int
    frames_active: int
    frames_failed: int


def load_screen_description_candidates(
    warehouse,
    *,
    prompt_version: str,
    since: datetime,
    limit: int | None,
    max_error_attempts: int = DEFAULT_SCREEN_DESCRIPTION_MAX_ERROR_ATTEMPTS,
) -> list[dict[str, Any]]:
    """Frames captured since ``since`` with no current reading, newest first.

    A reading is current while it read the frame's bytes and the tree its
    moment has now: a tree that lands after its screenshot was read asks
    again. A failed reading is retried until it has failed
    ``max_error_attempts`` times (0 retries forever).
    """
    error_sql = "d.status = 'error'"
    params: list[Any] = [prompt_version, since]
    if max_error_attempts > 0:
        error_sql += " AND d.attempts < %s"
        params.append(int(max_error_attempts))
    limit_sql = ""
    if limit is not None and limit > 0:
        limit_sql = "LIMIT %s"
        params.append(int(limit))
    rows = warehouse._query_dicts(
        f"""
        SELECT
            s.device,
            s.captured_at,
            s.display_index,
            s.content_sha256,
            s.mime_type,
            s.source_filename,
            s.storage_backend,
            s.storage_key,
            s.storage_file_id,
            s.storage_url,
            COALESCE(t.content_sha256, '') AS tree_sha256,
            COALESCE(t.content_text, '') AS tree_content_text,
            COALESCE(t.mime_type, '') AS tree_mime_type,
            COALESCE(t.storage_backend, '') AS tree_storage_backend,
            COALESCE(t.storage_key, '') AS tree_storage_key,
            COALESCE(t.storage_file_id, '') AS tree_storage_file_id,
            COALESCE(t.storage_url, '') AS tree_storage_url,
            (
                SELECT count(*)
                FROM @screen_timelapse_screenshots o
                WHERE o.device = s.device AND o.captured_at = s.captured_at
            ) AS displays_at_moment,
            CASE WHEN d.status = 'error' THEN d.attempts ELSE 0 END AS prior_attempts
        FROM @screen_timelapse_screenshots s
        LEFT JOIN @screen_timelapse_accessibility_trees t
          ON t.device = s.device
         AND t.captured_at = s.captured_at
        LEFT JOIN @screen_timelapse_descriptions d
          ON d.device = s.device
         AND d.captured_at = s.captured_at
         AND d.display_index = s.display_index
         AND d.prompt_version = %s
        WHERE s.captured_at >= %s
          AND s.storage_file_id <> ''
          AND (
              d.device IS NULL
              OR (
                  d.status = 'completed'
                  AND (
                      d.content_sha256 <> s.content_sha256
                      OR d.accessibility_tree_sha256 <> COALESCE(t.content_sha256, '')
                  )
              )
              OR ({error_sql})
          )
        ORDER BY s.captured_at DESC, s.device, s.display_index
        {limit_sql}
        """,
        params,
    )
    return [dict(row) for row in rows]


def load_reusable_description(
    warehouse, *, content_sha256: str, prompt_version: str
) -> dict[str, Any] | None:
    """A completed tree-less reading of the same bytes, if one exists."""
    rows = warehouse._query_dicts(
        """
        SELECT *
        FROM @screen_timelapse_descriptions
        WHERE content_sha256 = %s
          AND prompt_version = %s
          AND accessibility_tree_sha256 = ''
          AND status = 'completed'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (content_sha256, prompt_version),
    )
    return dict(rows[0]) if rows else None


def accessibility_tree_text(content: bytes) -> str:
    """A stored tree's bytes as the text its inline copy holds.

    The Mac writes gzipped JSON, the iPhone plain JSON and the Pixel a gzipped
    uiautomator XML dump. The app decodes the same way at ingest
    (screenTimelapseTreeText): decompressed, at most 8 MiB, and with invalid
    UTF-8 and NUL (which Postgres text cannot hold) replaced.
    """
    if content[:2] == b"\x1f\x8b":
        with gzip.GzipFile(fileobj=BytesIO(content)) as reader:
            content = reader.read(ACCESSIBILITY_TREE_TEXT_MAX_BYTES)
    else:
        content = content[:ACCESSIBILITY_TREE_TEXT_MAX_BYTES]
    return content.decode("utf-8", errors="replace").replace("\x00", "\ufffd")


def backfill_accessibility_tree_text(warehouse, object_store, *, limit: int, logger) -> int:
    """Fill content_text for trees stored before the app wrote it inline.

    Newest first. The update is guarded by the tree's sha, so a tree the app
    replaced meanwhile keeps the text the app wrote. Returns trees filled.
    """
    rows = warehouse._query_dicts(
        """
        SELECT device, captured_at, content_sha256, storage_backend, storage_key, storage_file_id, storage_url
        FROM @screen_timelapse_accessibility_trees
        WHERE content_text = ''
          AND size_bytes > 0
          AND storage_file_id <> ''
        ORDER BY captured_at DESC
        LIMIT %s
        """,
        (int(limit),),
    )
    filled = 0
    for row in rows:
        try:
            text = accessibility_tree_text(object_store.get_object(_storage_ref(row, prefix="")))
        except Exception as exc:  # noqa: BLE001 - one unreadable tree must not stop the rest
            logger.warning("could not read accessibility tree %s/%s: %s", row["device"], row["captured_at"], exc)
            continue
        _write_tree_text(warehouse, row, text)
        filled += 1
    if rows:
        logger.info("accessibility tree text: filled %s of %s trees", filled, len(rows))
    return filled


def _write_tree_text(warehouse, row: Mapping[str, Any], text: str) -> None:
    warehouse._command(
        """
        UPDATE @screen_timelapse_accessibility_trees
        SET content_text = %s
        WHERE device = %s AND captured_at = %s AND content_sha256 = %s
        """,
        (text, row["device"], row["captured_at"], row["content_sha256"]),
    )


def accessibility_tree_input(text: str) -> tuple[str, str]:
    """The tree as one text file the agent can read, named for its format."""
    extension = "xml" if text.lstrip().startswith("<") else "json"
    if len(text) > ACCESSIBILITY_TREE_MAX_CHARS:
        remaining = len(text) - ACCESSIBILITY_TREE_MAX_CHARS
        text = text[:ACCESSIBILITY_TREE_MAX_CHARS] + f"\n[truncated: {remaining} more characters]"
    return f"accessibility_tree.{extension}", text


def screen_description_schema() -> dict[str, Any]:
    # Codex enforces strict structured outputs: every property is required.
    return {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "screen_state",
            "evidence",
            "app",
            "window_title",
            "summary",
            "screen_text",
            "uncertainties",
        ],
        "properties": {
            "screen_state": {"type": "string", "enum": list(SCREEN_STATES)},
            "evidence": {"type": "string", "enum": list(EVIDENCE_KINDS)},
            "app": {"type": "string"},
            "window_title": {"type": "string"},
            "summary": {"type": "string"},
            "screen_text": {"type": "string"},
            "uncertainties": {"type": "array", "items": {"type": "string"}},
        },
    }


def screen_description_instructions(*, has_tree: bool) -> list[str]:
    if has_tree:
        source = [
            "Read the accessibility tree first. It lists the apps, windows and on-screen elements with "
            "their exact text. If it tells you which app is in front and what its main content is (the "
            "email, message, page, document, query or code being shown), answer from the tree alone and "
            "do not open the screenshot: evidence accessibility_tree.",
            "Open the screenshot when the tree does not tell you that: the main content is a web view, "
            "image, video, map, canvas or terminal the tree shows as an empty element; the tree holds only "
            "toolbar and navigation chrome; you cannot tell from it whether the screen was locked or off; "
            "or the tree covers several displays and you cannot tell which windows were on this one. Then "
            "evidence both.",
        ]
    else:
        source = [
            "There is no accessibility tree for this moment. Open the screenshot and answer from it: "
            "evidence screenshot.",
        ]
    return [
        "These are periodic whole-screen captures of Zach Latta's own devices, taken about once a minute "
        "while a device is on. Describe what the screen showed so he can later find what he was looking "
        "at or doing.",
        *source,
        "screen_state: active when the device was in use (an app, the home screen or the desktop is "
        "showing); locked for a lock screen, login window or screensaver, even with notifications on it; "
        "off for a black, blank or asleep display. A frame that is not active needs only a short summary; "
        "leave app, window_title and screen_text empty.",
        "app: the app in front on this display, by the name it shows (Mail, Slack, Google Chrome, "
        "Superhuman); '' if none. window_title: the front window's or page's title; '' if none.",
        "summary: one to three sentences on what is on the screen and what Zach appears to be doing. Name "
        "the specific thing (which conversation, email, page, document, file, query or video), not just "
        "the app.",
        "screen_text: the main readable content, verbatim, most important first: headings, message and "
        "email text, page text, URLs, file names, code. Skip menus and toolbar chrome. At most about 2,000 "
        "characters.",
        "uncertainties: short notes on anything you could not read or decide; [] if none.",
        "Describe only what the tree or the screenshot shows; do not infer from file names. Transcribe "
        "what is there plainly, whatever it is.",
    ]


def screen_description_prompt(candidate: Mapping[str, Any], *, tree_name: str | None) -> str:
    # Paths are relative to the agent's working directory, where inputs/ sits;
    # codex's view_image takes the string verbatim (see attachment_vision_prompt).
    captured_at = candidate.get("captured_at")
    payload: dict[str, Any] = {
        "task": "Say whether this device's screen was in use at this moment, and what was on it.",
        "frame": {
            "device": str(candidate.get("device", "")),
            "captured_at_utc": captured_at.astimezone(UTC).isoformat()
            if isinstance(captured_at, datetime)
            else str(captured_at or ""),
            "display_index": int(candidate.get("display_index") or 1),
            "displays_at_this_moment": int(candidate.get("displays_at_moment") or 1),
        },
        "screenshot": {
            "path": f"{DEFAULT_AGENT_INPUTS_DIR_NAME}/{SCREENSHOT_INPUT}",
            "how_to_view": (
                "Open this exact path with your image-viewing capability (codex: the view_image tool; "
                "claude: the Read tool). It is relative to your current working directory: pass it "
                "verbatim."
            ),
        },
        "instructions": screen_description_instructions(has_tree=tree_name is not None),
        "final_output_contract": {
            "format": "Return one JSON object and no prose.",
            "schema": screen_description_schema(),
        },
        "agent_runtime_notes": [
            "You are running as a one-off CLI agent inside an isolated Docker container.",
            "The final answer must be valid JSON matching final_output_contract.schema.",
        ],
    }
    if tree_name is not None:
        payload["accessibility_tree"] = {
            "path": f"{DEFAULT_AGENT_INPUTS_DIR_NAME}/{tree_name}",
            "how_to_read": "A text file; read it with the shell (cat, grep, jq or python).",
        }
    return json.dumps(payload, sort_keys=True)


def validate_screen_description(result: Mapping[str, Any]) -> list[str]:
    issues: list[str] = []
    if result.get("screen_state") not in SCREEN_STATES:
        issues.append(f"screen_state must be one of {', '.join(SCREEN_STATES)}")
    if result.get("evidence") not in EVIDENCE_KINDS:
        issues.append(f"evidence must be one of {', '.join(EVIDENCE_KINDS)}")
    for key in ("app", "window_title", "summary", "screen_text"):
        if not isinstance(result.get(key), str):
            issues.append(f"{key} must be a string")
    uncertainties = result.get("uncertainties")
    if not isinstance(uncertainties, Sequence) or isinstance(uncertainties, (str, bytes)):
        issues.append("uncertainties must be an array of strings")
    if not str(result.get("summary", "")).strip():
        issues.append("summary must not be empty")
    return issues


def _subject_id(candidate: Mapping[str, Any]) -> str:
    captured_at = candidate.get("captured_at")
    stamp = captured_at.astimezone(UTC).isoformat() if isinstance(captured_at, datetime) else str(captured_at)
    return f"{candidate.get('device', '')}/{stamp}/{candidate.get('display_index', '')}"


class ScreenTimelapseEnrichmentRunner:
    def __init__(
        self,
        *,
        warehouse,
        agent,
        object_store,
        logger,
        provider: str,
        model: str = "",
        prompt_version: str = SCREEN_DESCRIPTION_PROMPT_VERSION,
        lookback: timedelta = DEFAULT_SCREEN_DESCRIPTION_LOOKBACK,
        max_error_attempts: int = DEFAULT_SCREEN_DESCRIPTION_MAX_ERROR_ATTEMPTS,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._warehouse = warehouse
        self._agent = agent
        self._object_store = object_store
        self._logger = logger
        self._provider = provider
        self._model = model
        self._prompt_version = prompt_version
        self._lookback = lookback
        self._max_error_attempts = max_error_attempts
        self._now = now or (lambda: datetime.now(tz=UTC))

    @property
    def enrichment_provider(self) -> str:
        return f"agent_{self._provider}"

    def sync(self, *, limit: int | None) -> ScreenTimelapseEnrichmentSummary:
        """Read up to ``limit`` frames with the agent; reused frames are free."""
        self._warehouse.ensure_screen_timelapse_tables()
        self._warehouse.ensure_agent_tables()
        # Reused frames cost no agent run, so page further than the run budget
        # (an asleep phone's backlog is one judgment and many copies).
        page = None if limit is None or limit <= 0 else limit * 20
        candidates = load_screen_description_candidates(
            self._warehouse,
            prompt_version=self._prompt_version,
            since=self._now() - self._lookback,
            limit=page,
            max_error_attempts=self._max_error_attempts,
        )
        answered: dict[str, dict[str, Any]] = {}
        agent_runs = described = reused = active = failed = 0
        seen = 0
        for candidate in candidates:
            no_tree = not candidate["tree_sha256"]
            prior = None
            if no_tree:
                prior = answered.get(candidate["content_sha256"]) or load_reusable_description(
                    self._warehouse,
                    content_sha256=candidate["content_sha256"],
                    prompt_version=self._prompt_version,
                )
            if prior is None and limit is not None and limit > 0 and agent_runs >= limit:
                continue
            seen += 1
            if prior is not None:
                row = self._reused_row(candidate, prior)
                self._warehouse.insert_screen_timelapse_descriptions([row])
                reused += 1
            else:
                agent_runs += 1
                label = _subject_id(candidate)
                try:
                    self._logger.info("[%s] reading %s", agent_runs, label)
                    row = self._describe(candidate)
                except Exception as exc:  # noqa: BLE001 - recorded and retried
                    failed += 1
                    self._record_failure(candidate, error=str(exc))
                    self._logger.warning("screen description failed for %s: %s", label, exc)
                    continue
                if no_tree:
                    answered[candidate["content_sha256"]] = row
            described += 1
            if row["screen_state"] == "active":
                active += 1
        self._logger.info(
            "screen descriptions: saw %s frames, %s agent runs, described %s (%s reused, %s active), failed %s",
            seen,
            agent_runs,
            described,
            reused,
            active,
            failed,
        )
        return ScreenTimelapseEnrichmentSummary(
            frames_seen=seen,
            frames_described=described,
            frames_reused=reused,
            frames_active=active,
            frames_failed=failed,
        )

    def _describe(self, candidate: Mapping[str, Any]) -> dict[str, Any]:
        image = normalized_model_image(
            self._object_store.get_object(_storage_ref(candidate, prefix="")),
            max_edge=SCREEN_IMAGE_MAX_EDGE,
        )
        input_files: dict[str, str | bytes] = {SCREENSHOT_INPUT: image}
        tree_name = None
        if candidate["tree_sha256"]:
            text = candidate.get("tree_content_text") or ""
            if not text:
                # Stored before the app wrote trees inline: read it once and
                # keep the text, as the backfill would.
                text = accessibility_tree_text(
                    self._object_store.get_object(_storage_ref(candidate, prefix="tree_"))
                )
                _write_tree_text(
                    self._warehouse,
                    {
                        "device": candidate["device"],
                        "captured_at": candidate["captured_at"],
                        "content_sha256": candidate["tree_sha256"],
                    },
                    text,
                )
            tree_name, tree_text = accessibility_tree_input(text)
            input_files[tree_name] = tree_text
        prompt = screen_description_prompt(candidate, tree_name=tree_name)
        request = AgentRunRequest(
            prompt=prompt,
            schema=screen_description_schema(),
            task_type=SCREEN_DESCRIPTION_TASK_TYPE,
            subject_id=_subject_id(candidate),
            prompt_version=self._prompt_version,
            provider=self._provider,
            model=self._model,
            input_files=input_files,
        )
        result = self._agent.run(request)
        self._record_agent_result(result)
        if result.status != "completed":
            raise RuntimeError(result.error or f"agent run {result.run_id} failed")
        output = dict(result.final_output_json)
        issues = validate_screen_description(output)
        if issues:
            raise RuntimeError("agent output failed validation: " + "; ".join(issues[:5]))
        if tree_name is None:
            # There was nothing else to read.
            output["evidence"] = "screenshot"
        row = self._row(
            candidate,
            status=STATUS_COMPLETED,
            output=output,
            agent_run_id=result.run_id,
            model=result.model or self._model,
        )
        self._warehouse.insert_screen_timelapse_descriptions([row])
        return row

    def _reused_row(self, candidate: Mapping[str, Any], prior: Mapping[str, Any]) -> dict[str, Any]:
        output = {
            "screen_state": prior["screen_state"],
            "evidence": prior["evidence"],
            "app": prior["app"],
            "window_title": prior["window_title"],
            "summary": prior["summary"],
            "screen_text": prior["screen_text"],
            "uncertainties": json.loads(prior.get("uncertainties_json") or "[]"),
        }
        row = self._row(
            candidate,
            status=STATUS_COMPLETED,
            output=output,
            agent_run_id=str(prior.get("agent_run_id", "")),
            model=str(prior.get("model", "")),
        )
        row["is_reused"] = 1
        row["attempts"] = 0
        return row

    def _record_failure(self, candidate: Mapping[str, Any], *, error: str) -> None:
        try:
            row = self._row(candidate, status=STATUS_ERROR, output={}, agent_run_id="", model=self._model)
            row["error"] = error[:2000]
            self._warehouse.insert_screen_timelapse_descriptions([row])
        except Exception as exc:  # noqa: BLE001 - the failure is already logged
            self._logger.warning("could not record screen description failure: %s", exc)

    def _row(
        self,
        candidate: Mapping[str, Any],
        *,
        status: str,
        output: Mapping[str, Any],
        agent_run_id: str,
        model: str,
    ) -> dict[str, Any]:
        created_at = self._now()
        return {
            "device": candidate["device"],
            "captured_at": candidate["captured_at"],
            "display_index": candidate["display_index"],
            "prompt_version": self._prompt_version,
            "content_sha256": candidate["content_sha256"],
            "accessibility_tree_sha256": candidate["tree_sha256"],
            "provider": self.enrichment_provider,
            "model": model,
            "status": status,
            "error": "",
            "attempts": int(candidate.get("prior_attempts") or 0) + 1,
            "screen_state": str(output.get("screen_state", "")),
            "evidence": str(output.get("evidence", "")),
            "app": str(output.get("app", "")),
            "window_title": str(output.get("window_title", "")),
            "summary": str(output.get("summary", "")),
            "screen_text": str(output.get("screen_text", "")),
            "uncertainties_json": json.dumps(list(output.get("uncertainties") or []), ensure_ascii=False),
            "is_reused": 0,
            "agent_run_id": agent_run_id,
            "raw_result_json": json.dumps(dict(output), ensure_ascii=False, sort_keys=True) if output else "",
            "created_at": created_at,
            "sync_version": int(created_at.timestamp() * 1000),
        }

    def _record_agent_result(self, result: AgentRunResult) -> None:
        self._warehouse.insert_agent_runs([agent_run_row(result)])
        event_rows = agent_run_event_rows(result)
        if event_rows:
            self._warehouse.insert_agent_run_events(event_rows)
        tool_call_rows = agent_run_tool_call_rows(result)
        if tool_call_rows:
            self._warehouse.insert_agent_run_tool_calls(tool_call_rows)


def _storage_ref(candidate: Mapping[str, Any], *, prefix: str) -> dict[str, str]:
    return {
        "storage_backend": str(candidate.get(f"{prefix}storage_backend", "")),
        "storage_key": str(candidate.get(f"{prefix}storage_key", "")),
        "storage_file_id": str(candidate.get(f"{prefix}storage_file_id", "")),
        "storage_url": str(candidate.get(f"{prefix}storage_url", "")),
    }
