"""Write enriched titles back into the Voice Memos app.

After the warehouse transcribes and enriches a memo, this module renames the
recording inside the Voice Memos app so the library shows the AI title instead
of "New Recording 12" or a street address. Only auto-named memos are ever
touched: a title the user typed is never overwritten.

The rename itself is a proper Core Data save against the app's
CloudRecordings.db store (see store_writer.py), which records persistent
history that the voicememod daemon exports to CloudKit — so renames sync to
every device exactly like a rename made in the app.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import re
import sqlite3
from typing import Callable, Mapping, Sequence

import requests

# Voice Memos keeps this bit set in ZFLAGS while a recording still carries the
# name the app assigned ("New Recording N", or a geocoded location name) and
# clears it when the user types a title. Verified empirically: every
# location-auto-named row carries it, every hand-renamed row lacks it.
AUTO_NAMED_FLAG = 0x1000

# Memos recorded before the flag existed (flags 0/4 era) never carry the bit,
# but the app's non-location default names are still unmistakable.
DEFAULT_TITLE_PATTERN = re.compile(r"New Recording \d+")

MAX_TITLE_LENGTH = 200

# Persistent-history transaction author recorded with every write-back save,
# so warehouse renames are attributable in the store's own audit trail.
TRANSACTION_AUTHOR = "com.zachlatta.pdw.voice-memo-writeback"


@dataclass(frozen=True)
class LocalRecordingTitle:
    unique_id: str
    recording_id: str
    title: str
    flags: int
    filename: str = ""


@dataclass(frozen=True)
class EnrichedTitles:
    by_recording_id: dict[str, str]
    by_content_sha256: dict[str, str]


@dataclass(frozen=True)
class RenamePlanItem:
    unique_id: str
    recording_id: str
    old_title: str
    new_title: str


@dataclass(frozen=True)
class WritebackSummary:
    local_recordings: int
    auto_named: int
    enriched_titles: int
    planned: int
    renamed: int
    skipped: int
    dry_run: bool


def is_auto_named(title: str, flags: int) -> bool:
    if flags & AUTO_NAMED_FLAG:
        return True
    return DEFAULT_TITLE_PATTERN.fullmatch(title or "") is not None


def sanitize_title(title: str | None) -> str | None:
    if title is None:
        return None
    cleaned = "".join(char for char in title if char >= " " or char in "\t\n")
    collapsed = " ".join(cleaned.split())
    if not collapsed:
        return None
    return collapsed[:MAX_TITLE_LENGTH]


def load_local_recording_titles(recordings_path: Path | str) -> list[LocalRecordingTitle]:
    database_path = Path(recordings_path).expanduser() / "CloudRecordings.db"
    if not database_path.exists():
        return []
    connection = sqlite3.connect(f"file:{database_path}?mode=ro", uri=True)
    try:
        rows = connection.execute(
            """
            SELECT ZUNIQUEID, ZPATH, ZENCRYPTEDTITLE, ZFLAGS
            FROM ZCLOUDRECORDING
            WHERE ZUNIQUEID IS NOT NULL AND ZPATH IS NOT NULL
            """
        ).fetchall()
    finally:
        connection.close()
    titles: list[LocalRecordingTitle] = []
    for unique_id, path, title, flags in rows:
        filename = str(path or "")
        if not filename:
            continue
        titles.append(
            LocalRecordingTitle(
                unique_id=str(unique_id),
                recording_id=filename.rsplit(".", 1)[0],
                title=str(title or ""),
                flags=int(flags or 0),
                filename=filename,
            )
        )
    return titles


def resolve_effective_titles(
    local: Sequence[LocalRecordingTitle],
    titles: EnrichedTitles,
    sha_by_filename: Mapping[str, str] | None,
) -> dict[str, str]:
    """Map each local recording's stem to its enriched title.

    Stems normally match `enrichments.recording_id` directly, but Voice Memos
    rebases filename timestamps when the timezone changes, leaving the
    warehouse knowing a memo only under an older stem. The audio content sha
    (cached per filename in the uploader's state) still identifies it, so it
    serves as the fallback join key.
    """
    effective: dict[str, str] = {}
    for recording in local:
        title = titles.by_recording_id.get(recording.recording_id)
        if title is None and sha_by_filename:
            sha = sha_by_filename.get(recording.filename)
            if sha:
                title = titles.by_content_sha256.get(sha)
        if title is not None:
            effective[recording.recording_id] = title
    return effective


def build_rename_plan(
    local: Sequence[LocalRecordingTitle],
    enriched_titles: Mapping[str, str],
    *,
    limit: int | None = None,
) -> list[RenamePlanItem]:
    plan: list[RenamePlanItem] = []
    # Recording ids start with the recording timestamp, so sorting by id
    # descending renames the newest memos first when a limit applies.
    for recording in sorted(local, key=lambda item: item.recording_id, reverse=True):
        if not is_auto_named(recording.title, recording.flags):
            continue
        new_title = sanitize_title(enriched_titles.get(recording.recording_id))
        if new_title is None or new_title == recording.title:
            continue
        plan.append(
            RenamePlanItem(
                unique_id=recording.unique_id,
                recording_id=recording.recording_id,
                old_title=recording.title,
                new_title=new_title,
            )
        )
        if limit is not None and len(plan) >= limit:
            break
    return plan


def fetch_enriched_titles(
    *,
    base_url: str,
    secret_token: str,
    account: str,
    client_name: str = "pdw",
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> EnrichedTitles:
    """Fetch the newest completed enrichment title per recording from the app.

    Uses the same static-bearer HTTP tool API the pdw CLI's `sql` command
    uses, so no new server surface is involved. Titles come back keyed by
    recording_id and by audio content sha (the drift-proof identity).
    """
    escaped_account = account.replace("'", "''")
    sql = (
        "SELECT DISTINCT ON (recording_id) recording_id, content_sha256, title "
        "FROM apple_voice_memos.enrichments "
        "WHERE status = 'completed' AND title IS NOT NULL "
        f"AND account = '{escaped_account}' "
        "ORDER BY recording_id, created_at DESC"
    )
    http = session or requests.Session()
    response = http.post(
        f"{base_url.rstrip('/')}/api/tools/sql",
        json={
            "question": "Voice memo enriched titles for app write-back",
            "sql": sql,
            # The server's name for newline-delimited JSON (unknown formats
            # silently fall back to csv, so this string matters).
            "format": "ndjson",
        },
        headers={"Authorization": f"Bearer {client_name}:{secret_token}"},
        timeout=timeout,
    )
    response.raise_for_status()
    data = response.json().get("data") or {}
    error = data.get("error")
    if error:
        raise RuntimeError(f"enriched title query failed: {error}")
    by_recording_id: dict[str, str] = {}
    by_content_sha256: dict[str, str] = {}
    for line in (data.get("rows") or "").splitlines():
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)
        recording_id = row.get("recording_id")
        title = row.get("title")
        if not recording_id or not title:
            continue
        by_recording_id[str(recording_id)] = str(title)
        sha = row.get("content_sha256")
        if sha:
            by_content_sha256.setdefault(str(sha), str(title))
    return EnrichedTitles(by_recording_id=by_recording_id, by_content_sha256=by_content_sha256)


def _default_writer(store_path, items, *, author, dry_run=False):
    from personal_data_warehouse_voice_memos.store_writer import apply_renames

    return apply_renames(store_path, items, author=author, dry_run=dry_run)


class VoiceMemosWritebackRunner:
    def __init__(
        self,
        *,
        recordings_path: Path | str,
        account: str,
        base_url: str,
        secret_token: str,
        logger,
        client_name: str = "pdw",
        writer: Callable | None = None,
        session: requests.Session | None = None,
        limit: int | None = None,
        dry_run: bool = False,
        sha_by_filename: Mapping[str, str] | None = None,
    ) -> None:
        self._recordings_path = Path(recordings_path).expanduser()
        self._account = account
        self._base_url = base_url
        self._secret_token = secret_token
        self._client_name = client_name
        self._logger = logger
        self._writer = writer or _default_writer
        self._session = session
        self._limit = limit
        self._dry_run = dry_run
        self._sha_by_filename = sha_by_filename

    def run(self) -> WritebackSummary:
        local = load_local_recording_titles(self._recordings_path)
        auto_named = [item for item in local if is_auto_named(item.title, item.flags)]
        self._logger.info(
            "Voice Memos write-back: %s local recordings, %s still auto-named",
            len(local),
            len(auto_named),
        )
        if not auto_named:
            return self._summary(local=local, auto_named=auto_named, enriched_titles={}, plan=[], results=[])

        titles = fetch_enriched_titles(
            base_url=self._base_url,
            secret_token=self._secret_token,
            account=self._account,
            client_name=self._client_name,
            session=self._session,
        )
        enriched_titles = resolve_effective_titles(local, titles, self._sha_by_filename)
        plan = build_rename_plan(local, enriched_titles, limit=self._limit)
        self._logger.info(
            "Voice Memos write-back: %s enriched titles available (%s matched locally), %s renames planned",
            len(titles.by_recording_id),
            len(enriched_titles),
            len(plan),
        )
        for item in plan:
            self._logger.info(
                "%s rename %s: %r -> %r",
                "[dry-run] would" if self._dry_run else "will",
                item.recording_id,
                item.old_title,
                item.new_title,
            )
        if self._dry_run or not plan:
            return self._summary(local=local, auto_named=auto_named, enriched_titles=enriched_titles, plan=plan, results=[])

        results = self._writer(
            self._recordings_path / "CloudRecordings.db",
            plan,
            author=TRANSACTION_AUTHOR,
        )
        for result in results:
            if result["status"] != "renamed":
                self._logger.warning(
                    "Voice Memos write-back skipped %s: %s",
                    result["unique_id"],
                    result["status"],
                )
        return self._summary(local=local, auto_named=auto_named, enriched_titles=enriched_titles, plan=plan, results=results)

    def _summary(self, *, local, auto_named, enriched_titles, plan, results) -> WritebackSummary:
        renamed = sum(1 for result in results if result["status"] == "renamed")
        return WritebackSummary(
            local_recordings=len(local),
            auto_named=len(auto_named),
            enriched_titles=len(enriched_titles),
            planned=len(plan),
            renamed=renamed,
            skipped=len(results) - renamed,
            dry_run=self._dry_run,
        )
