"""Apply approved Apple Notes mutations through Notes.app.

Apple Notes has no server API. iCloud exposes no write endpoint, the desktop app keeps
its content as gzipped protobuf inside a Core Data store, and the only supported way to
change a note is to ask Notes.app itself. So this executor is AppleScript, and it must
run on a Mac that is signed in to the account -- which is why the cloud mutation worker
deliberately does not claim this provider.

The AppleScript is built here rather than kept as a template file so that every value
crossing into it goes through :func:`applescript_string`. AppleScript has no parameter
binding: a note body is spliced into the script text, so an unescaped quote is not a
failed write but an arbitrary-script-execution bug.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
import html as html_module
import os
import re
import subprocess
from typing import Any


APPLE_NOTES_PROVIDER = "apple_notes"
APPLE_NOTES_CREATE_NOTE_OPERATION = "apple_notes.create_note"
APPLE_NOTES_UPDATE_NOTE_OPERATION = "apple_notes.update_note"

APPLE_NOTES_OPERATIONS = (
    APPLE_NOTES_CREATE_NOTE_OPERATION,
    APPLE_NOTES_UPDATE_NOTE_OPERATION,
)

# osascript inherits Notes' own AppleEvent timeout, and Notes can be slow while iCloud
# is pulling. Bound the subprocess well above the in-script `with timeout` so a hung
# Notes surfaces as our timeout with our message, not as an opaque killed process.
DEFAULT_SCRIPT_TIMEOUT_SECONDS = 180
_IN_SCRIPT_TIMEOUT_SECONDS = 120

# Notes returns the id and the resulting title on two lines; keep them apart from any
# body text the note may contain.
_RESULT_SEPARATOR = "\n"

_MARKUP_RE = re.compile(r"<[a-zA-Z/!]")
_CORE_DATA_PREFIX = "x-coredata://"
_UUID_RE = re.compile(r"^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$")


class AppleNotesNoteNotFound(RuntimeError):
    """Raised when a proposed note reference matches no note in the local store."""


@dataclass(frozen=True)
class AppleNotesMutationResult:
    status: str
    result_json: dict[str, Any] = field(default_factory=dict)
    error: str = ""


def applescript_string(value: str) -> str:
    """Render a Python string as an AppleScript string expression.

    Quotes and backslashes are escaped. Newlines cannot appear inside an AppleScript
    string literal at all, so they are spliced in as `linefeed` terms -- which is why
    this returns an *expression* rather than a literal.
    """

    text = "" if value is None else str(value)
    parts = []
    for segment in text.split("\n"):
        escaped = segment.replace("\\", "\\\\").replace('"', '\\"')
        parts.append(f'"{escaped}"')
    return " & linefeed & ".join(parts)


def body_to_html(body: str) -> str:
    """Convert a proposal body into the HTML Notes stores.

    Notes' `body` property is HTML. A proposal that already carries markup is passed
    through untouched (that is how an agent asks for a heading or a list); anything else
    is treated as plain text, escaped, and wrapped one <div> per line. Escaping matters:
    a plain-text body containing `<` would otherwise silently lose everything after it.
    """

    text = "" if body is None else str(body)
    if _MARKUP_RE.search(text):
        return text
    lines = text.split("\n")
    return "".join(f"<div>{html_module.escape(line)}</div>" for line in lines)


def resolve_note_reference(note_id: str, *, lookup: Callable[[str], tuple[str, int] | None] | None = None) -> str:
    """Turn whatever an agent supplied into the id Notes' AppleScript accepts.

    Two identifiers name the same note and they do not look alike. Notes' scripting `id`
    is ``x-coredata://<store-uuid>/ICNote/p<Z_PK>``; the warehouse column an agent would
    naturally read, ``base_apple_notes.notes.note_id``, is the store's ZIDENTIFIER UUID.
    Requiring the Core Data form would mean the one id a proposal can discover is the one
    the executor rejects, so a UUID is resolved through the local store instead.
    """

    reference = str(note_id or "").strip()
    if reference.startswith(_CORE_DATA_PREFIX):
        return reference
    if not _UUID_RE.match(reference):
        raise AppleNotesNoteNotFound(
            f"note_id {reference!r} is neither an x-coredata:// id nor a base_apple_notes.notes.note_id UUID"
        )
    resolve = lookup or note_primary_key_from_store
    found = resolve(reference)
    if not found:
        raise AppleNotesNoteNotFound(
            f"no local Apple note has note_id {reference}; it may live on another Mac or have been deleted"
        )
    store_uuid, primary_key = found
    return f"{_CORE_DATA_PREFIX}{store_uuid}/ICNote/p{int(primary_key)}"


def note_primary_key_from_store(uuid: str, *, store_path: str | None = None) -> tuple[str, int] | None:
    """Read (store uuid, Z_PK) for a note UUID out of a snapshot of NoteStore.sqlite.

    A snapshot, not the live file: the uploader holds the same rule, because Notes writes
    through a WAL and reading it live races the app.
    """

    import sqlite3
    import tempfile
    from pathlib import Path

    from personal_data_warehouse_apple_notes.scanner import snapshot_apple_notes_store

    source = store_path or os.path.expanduser(
        "~/Library/Group Containers/group.com.apple.notes/NoteStore.sqlite"
    )
    with tempfile.TemporaryDirectory() as temp_dir:
        snapshot = snapshot_apple_notes_store(Path(source), Path(temp_dir))
        connection = sqlite3.connect(str(snapshot))
        try:
            store_uuid_row = connection.execute("SELECT Z_UUID FROM Z_METADATA").fetchone()
            row = connection.execute(
                "SELECT Z_PK FROM ZICCLOUDSYNCINGOBJECT WHERE ZIDENTIFIER = ?",
                (uuid,),
            ).fetchone()
        finally:
            connection.close()
    if not row or not store_uuid_row:
        return None
    return (str(store_uuid_row[0]), int(row[0]))


def run_osascript(script: str, *, timeout: int = DEFAULT_SCRIPT_TIMEOUT_SECONDS) -> str:
    completed = subprocess.run(
        ["/usr/bin/osascript", "-"],
        input=script,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if completed.returncode != 0:
        raise RuntimeError((completed.stderr or completed.stdout or "osascript failed").strip())
    return completed.stdout.rstrip("\n")


class AppleNotesMutationExecutor:
    """Executes apple_notes.* mutations claimed from ops.upstream_mutation_operations."""

    def __init__(self, *, runner: Callable[[str], str] | None = None) -> None:
        self._runner = runner or run_osascript

    def execute(self, mutation: Mapping[str, Any]) -> AppleNotesMutationResult:
        provider = str(mutation.get("provider") or "")
        operation = str(mutation.get("operation") or "")
        if provider != APPLE_NOTES_PROVIDER or operation not in APPLE_NOTES_OPERATIONS:
            # Never burn an unrecognized row to failed_terminal: a newer worker may
            # understand it. This mirrors the cloud worker's unknown-provider handling.
            return AppleNotesMutationResult(
                status="failed_retryable",
                error=f"unsupported mutation operation {provider}.{operation}; deferring",
            )

        payload = _mapping(mutation.get("payload_json"))
        try:
            if operation == APPLE_NOTES_CREATE_NOTE_OPERATION:
                return self._create_note(payload)
            return self._update_note(payload)
        except subprocess.TimeoutExpired:
            return AppleNotesMutationResult(
                status="failed_retryable",
                error=f"Notes.app did not answer within {DEFAULT_SCRIPT_TIMEOUT_SECONDS}s",
            )
        except AppleNotesNoteNotFound as error:
            return AppleNotesMutationResult(status="failed_terminal", error=str(error))
        except RuntimeError as error:
            return _classify(str(error))

    def _create_note(self, payload: Mapping[str, Any]) -> AppleNotesMutationResult:
        folder = str(payload.get("folder") or "").strip() or "PDW Agent"
        name = str(payload.get("name") or "").strip()
        body_html = body_to_html(payload.get("body") or "")
        # Notes takes the note's title from the first line of the body, so a proposal
        # that sets `name` gets that name promoted into a leading heading. Setting the
        # `name` property alone does not survive -- Notes recomputes it from the body.
        if name:
            body_html = f"<div><h1>{html_module.escape(name)}</h1></div>{body_html}"

        script = f"""
        with timeout of {_IN_SCRIPT_TIMEOUT_SECONDS} seconds
          tell application "Notes"
            set folderName to {applescript_string(folder)}
            set targetAccount to account 1
            tell targetAccount
              if not (exists folder folderName) then
                make new folder with properties {{name:folderName}}
              end if
              set targetFolder to folder folderName
              set newNote to make new note at targetFolder with properties {{body:{applescript_string(body_html)}}}
              return (id of newNote) & {applescript_string(_RESULT_SEPARATOR)} & (name of newNote)
            end tell
          end tell
        end timeout
        """
        note_id, note_name = _split_result(self._runner(_dedent(script)))
        return AppleNotesMutationResult(
            status="succeeded",
            result_json={
                "note_id": note_id,
                "name": note_name,
                "folder": folder,
                "action": "create",
            },
        )

    def _update_note(self, payload: Mapping[str, Any]) -> AppleNotesMutationResult:
        note_id = resolve_note_reference(str(payload.get("note_id") or "").strip())
        name = str(payload.get("name") or "").strip()
        body = str(payload.get("body") or "")
        append_body = str(payload.get("append_body") or "")

        # Read the current body first, for two reasons: an append needs it, and a
        # replacement should leave the reviewer a way back. The read is a separate
        # script so a note that has vanished fails before anything is written.
        read_script = f"""
        with timeout of {_IN_SCRIPT_TIMEOUT_SECONDS} seconds
          tell application "Notes"
            return body of note id {applescript_string(note_id)}
          end tell
        end timeout
        """
        previous_body = self._runner(_dedent(read_script))

        if append_body.strip():
            new_body = previous_body + body_to_html(append_body)
            change = "append_body"
        elif body.strip():
            new_body = body_to_html(body)
            change = "body"
        else:
            new_body = previous_body
            change = "name"

        if name:
            new_body = _replace_leading_heading(new_body, name)

        write_script = f"""
        with timeout of {_IN_SCRIPT_TIMEOUT_SECONDS} seconds
          tell application "Notes"
            set targetNote to note id {applescript_string(note_id)}
            set body of targetNote to {applescript_string(new_body)}
            return (id of targetNote) & {applescript_string(_RESULT_SEPARATOR)} & (name of targetNote)
          end tell
        end timeout
        """
        resolved_id, resolved_name = _split_result(self._runner(_dedent(write_script)))
        return AppleNotesMutationResult(
            status="succeeded",
            result_json={
                "note_id": resolved_id or note_id,
                "name": resolved_name,
                "action": "update",
                "changed": change,
                "previous_body": previous_body,
            },
        )


def _replace_leading_heading(body_html: str, name: str) -> str:
    """Retitle a note by rewriting its first line, which is what Notes titles from."""

    escaped = html_module.escape(name)
    heading = f"<div><h1>{escaped}</h1></div>"
    match = re.match(r"\s*<div>.*?</div>", body_html, flags=re.DOTALL)
    if match:
        return heading + body_html[match.end() :]
    return heading + body_html


def _split_result(raw: str) -> tuple[str, str]:
    parts = str(raw or "").split(_RESULT_SEPARATOR, 1)
    note_id = parts[0].strip()
    note_name = parts[1].strip() if len(parts) > 1 else ""
    return note_id, note_name


def _classify(message: str) -> AppleNotesMutationResult:
    """Map an osascript failure onto the worker's status vocabulary.

    The distinction that matters: a missing note will never appear, so retrying it
    forever is noise; a busy or unlaunched Notes.app is transient; a refused Automation
    grant needs a human at that Mac and is not a code failure at all.
    """

    if "-1743" in message or "Not authorized to send Apple events" in message:
        return AppleNotesMutationResult(
            status="blocked_missing_credentials",
            error=(
                "Automation permission for Notes.app is not granted to this worker. "
                "Grant it in System Settings > Privacy & Security > Automation. "
                f"osascript said: {message}"
            ),
        )
    if "-1712" in message or "timed out" in message.lower():
        return AppleNotesMutationResult(status="failed_retryable", error=message)
    if "-1728" in message or "-2753" in message or "Invalid key form" in message:
        return AppleNotesMutationResult(status="failed_terminal", error=message)
    if "-600" in message or "not running" in message.lower():
        return AppleNotesMutationResult(status="failed_retryable", error=message)
    return AppleNotesMutationResult(status="failed_terminal", error=message)


def _dedent(script: str) -> str:
    return "\n".join(line.strip() for line in script.strip().splitlines())


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}
