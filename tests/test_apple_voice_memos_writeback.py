from __future__ import annotations

import json
from pathlib import Path
import sqlite3

import pytest

from personal_data_warehouse_voice_memos.writeback import (
    AUTO_NAMED_FLAG,
    EnrichedTitles,
    LocalRecordingTitle,
    RenamePlanItem,
    VoiceMemosWritebackRunner,
    build_rename_plan,
    fetch_enriched_titles,
    is_auto_named,
    load_local_recording_titles,
    resolve_effective_titles,
    sanitize_title,
)


class FakeLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def info(self, *args, **kwargs) -> None:
        self.messages.append(args[0] % args[1:] if len(args) > 1 else str(args[0]))

    def warning(self, *args, **kwargs) -> None:
        self.messages.append(args[0] % args[1:] if len(args) > 1 else str(args[0]))


def test_is_auto_named_trusts_the_flag_bit() -> None:
    assert is_auto_named("Anything At All", AUTO_NAMED_FLAG | 4)
    assert is_auto_named("Some Location Name 3", 0x1604)


def test_is_auto_named_falls_back_to_default_title_pattern() -> None:
    # Pre-flag-era memos (flags 0/4) never carry the auto-named bit, but a
    # "New Recording N" title is still unambiguously a default name.
    assert is_auto_named("New Recording 12", 4)
    assert is_auto_named("New Recording 1", 0)


def test_is_auto_named_never_matches_user_titles() -> None:
    assert not is_auto_named("Board prep walkthrough", 4)
    assert not is_auto_named("New Recording 12 with my notes", 0)
    assert not is_auto_named("Old Location Name 3", 0x604)  # renamed: bit cleared


def test_sanitize_title_collapses_whitespace_and_control_chars() -> None:
    assert sanitize_title("  Weekly \n sync\tnotes  ") == "Weekly sync notes"
    assert sanitize_title("plan\x00ning") == "planning"


def test_sanitize_title_caps_length_and_rejects_empty() -> None:
    long_title = "x" * 500
    sanitized = sanitize_title(long_title)
    assert sanitized is not None and len(sanitized) == 200
    assert sanitize_title("   \n\t  ") is None
    assert sanitize_title("") is None


def _create_cloud_recordings_db(path: Path, rows: list[tuple]) -> None:
    connection = sqlite3.connect(path)
    connection.execute(
        """
        CREATE TABLE ZCLOUDRECORDING (
            Z_PK INTEGER PRIMARY KEY,
            ZUNIQUEID VARCHAR,
            ZPATH VARCHAR,
            ZENCRYPTEDTITLE VARCHAR,
            ZFLAGS INTEGER
        )
        """
    )
    connection.executemany(
        "INSERT INTO ZCLOUDRECORDING (ZUNIQUEID, ZPATH, ZENCRYPTEDTITLE, ZFLAGS) VALUES (?, ?, ?, ?)",
        rows,
    )
    connection.commit()
    connection.close()


def test_load_local_recording_titles_reads_store_rows(tmp_path) -> None:
    _create_cloud_recordings_db(
        tmp_path / "CloudRecordings.db",
        [
            ("AAAA-1111", "20260101 090000-AAAA1111.qta", "New Recording 3", 4100),
            ("BBBB-2222", "20260102 100000-BBBB2222.m4a", "My handwritten title", 4),
            (None, "20260103 110000-CCCC3333.qta", "New Recording 4", 4100),
            ("DDDD-4444", None, "New Recording 5", 4100),
        ],
    )

    titles = load_local_recording_titles(tmp_path)

    assert titles == [
        LocalRecordingTitle(
            unique_id="AAAA-1111",
            recording_id="20260101 090000-AAAA1111",
            title="New Recording 3",
            flags=4100,
            filename="20260101 090000-AAAA1111.qta",
        ),
        LocalRecordingTitle(
            unique_id="BBBB-2222",
            recording_id="20260102 100000-BBBB2222",
            title="My handwritten title",
            flags=4,
            filename="20260102 100000-BBBB2222.m4a",
        ),
    ]


def test_load_local_recording_titles_missing_db_returns_empty(tmp_path) -> None:
    assert load_local_recording_titles(tmp_path) == []


def _local(unique_id: str, recording_id: str, title: str, flags: int) -> LocalRecordingTitle:
    return LocalRecordingTitle(
        unique_id=unique_id,
        recording_id=recording_id,
        title=title,
        flags=flags,
        filename=f"{recording_id}.qta",
    )


def test_build_rename_plan_renames_only_auto_named_matches() -> None:
    local = [
        _local("A", "20260101 090000-AAAA1111", "New Recording 3", 4100),
        _local("B", "20260102 100000-BBBB2222", "My handwritten title", 4),
        _local("C", "20260103 110000-CCCC3333", "Some Street 2", 0x1604),
        _local("D", "20260104 120000-DDDD4444", "New Recording 9", 4100),
    ]
    enriched = {
        "20260101 090000-AAAA1111": "Quarterly planning discussion",
        "20260102 100000-BBBB2222": "Should never be applied",
        "20260103 110000-CCCC3333": "Vendor onboarding call",
    }

    plan = build_rename_plan(local, enriched)

    # Newest recording first; the manual title (B) and the memo with no
    # enrichment yet (D) are never part of the plan.
    assert plan == [
        RenamePlanItem(
            unique_id="C",
            recording_id="20260103 110000-CCCC3333",
            old_title="Some Street 2",
            new_title="Vendor onboarding call",
        ),
        RenamePlanItem(
            unique_id="A",
            recording_id="20260101 090000-AAAA1111",
            old_title="New Recording 3",
            new_title="Quarterly planning discussion",
        ),
    ]


def test_build_rename_plan_skips_noop_and_unsanitizable_titles() -> None:
    local = [
        _local("A", "20260101 090000-AAAA1111", "Quarterly planning discussion", 4100),
        _local("B", "20260102 100000-BBBB2222", "New Recording 4", 4100),
    ]
    enriched = {
        "20260101 090000-AAAA1111": "Quarterly planning discussion",
        "20260102 100000-BBBB2222": "   ",
    }

    assert build_rename_plan(local, enriched) == []


def test_build_rename_plan_orders_newest_first_and_respects_limit() -> None:
    local = [
        _local("A", "20260101 090000-AAAA1111", "New Recording 1", 4100),
        _local("B", "20260301 090000-BBBB2222", "New Recording 2", 4100),
        _local("C", "20260201 090000-CCCC3333", "New Recording 3", 4100),
    ]
    enriched = {
        "20260101 090000-AAAA1111": "January review",
        "20260301 090000-BBBB2222": "March review",
        "20260201 090000-CCCC3333": "February review",
    }

    plan = build_rename_plan(local, enriched, limit=2)

    assert [item.new_title for item in plan] == ["March review", "February review"]


def test_resolve_effective_titles_prefers_stem_match_then_sha() -> None:
    local = [
        # Stem matches the warehouse directly.
        _local("A", "20260101 090000-AAAA1111", "New Recording 3", 4100),
        # Filename drifted (timezone rebase): warehouse knows this memo under
        # an older stem, reachable only through the audio content sha.
        _local("B", "20260430 140736-BBBB2222", "Some Park", 4100),
        # No stem or sha match at all.
        _local("C", "20260501 090000-CCCC3333", "New Recording 9", 4100),
    ]
    titles = EnrichedTitles(
        by_recording_id={
            "20260101 090000-AAAA1111": "Quarterly planning discussion",
            "20260430 110736-BBBB2222": "Park walk debrief",
        },
        by_content_sha256={
            "aaa111": "Should lose to the stem match",
            "bbb222": "Park walk debrief",
        },
    )
    sha_by_filename = {
        "20260101 090000-AAAA1111.qta": "aaa111",
        "20260430 140736-BBBB2222.qta": "bbb222",
    }

    effective = resolve_effective_titles(local, titles, sha_by_filename)

    assert effective == {
        "20260101 090000-AAAA1111": "Quarterly planning discussion",
        "20260430 140736-BBBB2222": "Park walk debrief",
    }


def test_resolve_effective_titles_without_sha_map_uses_stems_only() -> None:
    local = [_local("A", "20260101 090000-AAAA1111", "New Recording 3", 4100)]
    titles = EnrichedTitles(
        by_recording_id={"20260101 090000-AAAA1111": "Quarterly planning discussion"},
        by_content_sha256={"aaa111": "ignored"},
    )

    effective = resolve_effective_titles(local, titles, None)

    assert effective == {"20260101 090000-AAAA1111": "Quarterly planning discussion"}


def test_writeback_enabled_from_env_defaults_on() -> None:
    from personal_data_warehouse_voice_memos.cli import writeback_enabled_from_env

    assert writeback_enabled_from_env(lambda name: None)
    assert writeback_enabled_from_env(lambda name: "1")
    assert not writeback_enabled_from_env(lambda name: "0")
    assert not writeback_enabled_from_env(lambda name: "false")


class FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, payload: dict) -> None:
        self._payload = payload
        self.requests: list[dict] = []

    def post(self, url, *, json=None, headers=None, timeout=None):
        self.requests.append({"url": url, "json": json, "headers": headers, "timeout": timeout})
        return FakeResponse(self._payload)


def _sql_envelope(rows: list[dict]) -> dict:
    return {
        "data": {
            "column_names": ["recording_id", "content_sha256", "title"],
            "total_rows": len(rows),
            "rows": "\n".join(json.dumps(row) for row in rows),
            "error": "",
        }
    }


def test_fetch_enriched_titles_queries_the_app_sql_tool() -> None:
    session = FakeSession(
        _sql_envelope(
            [
                {
                    "recording_id": "20260101 090000-AAAA1111",
                    "content_sha256": "aaa111",
                    "title": "Quarterly planning discussion",
                },
                {
                    "recording_id": "20260102 100000-BBBB2222",
                    "content_sha256": "bbb222",
                    "title": "Vendor onboarding call",
                },
            ]
        )
    )

    titles = fetch_enriched_titles(
        base_url="https://warehouse.example.com/",
        secret_token="secret",
        account="zach@example.com",
        session=session,
    )

    assert titles.by_recording_id == {
        "20260101 090000-AAAA1111": "Quarterly planning discussion",
        "20260102 100000-BBBB2222": "Vendor onboarding call",
    }
    assert titles.by_content_sha256 == {
        "aaa111": "Quarterly planning discussion",
        "bbb222": "Vendor onboarding call",
    }
    request = session.requests[0]
    assert request["url"] == "https://warehouse.example.com/api/tools/sql"
    assert request["headers"]["Authorization"] == "Bearer pdw:secret"
    assert "apple_voice_memos.enrichments" in request["json"]["sql"]
    assert "content_sha256" in request["json"]["sql"]
    assert "'zach@example.com'" in request["json"]["sql"]
    assert request["json"]["format"] == "ndjson"


def test_fetch_enriched_titles_escapes_account_quotes() -> None:
    session = FakeSession(_sql_envelope([]))

    fetch_enriched_titles(
        base_url="https://warehouse.example.com",
        secret_token="secret",
        account="quote'test@example.com",
        session=session,
    )

    assert "'quote''test@example.com'" in session.requests[0]["json"]["sql"]


def test_fetch_enriched_titles_raises_on_sql_error() -> None:
    session = FakeSession({"data": {"rows": "", "error": "relation does not exist"}})

    with pytest.raises(RuntimeError, match="relation does not exist"):
        fetch_enriched_titles(
            base_url="https://warehouse.example.com",
            secret_token="secret",
            account="zach@example.com",
            session=session,
        )


class FakeWriter:
    def __init__(self, results: dict[str, str] | None = None) -> None:
        self.calls: list[dict] = []
        self._results = results or {}

    def __call__(self, store_path, items, *, author, dry_run=False):
        self.calls.append({"store_path": store_path, "items": list(items), "author": author, "dry_run": dry_run})
        return [
            {"unique_id": item.unique_id, "status": self._results.get(item.unique_id, "renamed")}
            for item in items
        ]


def _runner_fixture(tmp_path, *, writer, dry_run=False, limit=None):
    _create_cloud_recordings_db(
        tmp_path / "CloudRecordings.db",
        [
            ("A", "20260101 090000-AAAA1111.qta", "New Recording 3", 4100),
            ("B", "20260102 100000-BBBB2222.qta", "My handwritten title", 4),
        ],
    )
    session = FakeSession(
        _sql_envelope(
            [
                {"recording_id": "20260101 090000-AAAA1111", "title": "Quarterly planning discussion"},
                {"recording_id": "20260102 100000-BBBB2222", "title": "Should never be applied"},
            ]
        )
    )
    return VoiceMemosWritebackRunner(
        recordings_path=tmp_path,
        account="zach@example.com",
        base_url="https://warehouse.example.com",
        secret_token="secret",
        logger=FakeLogger(),
        writer=writer,
        session=session,
        limit=limit,
        dry_run=dry_run,
    )


def test_writeback_runner_applies_planned_renames(tmp_path) -> None:
    writer = FakeWriter()
    runner = _runner_fixture(tmp_path, writer=writer)

    summary = runner.run()

    assert summary.renamed == 1
    assert summary.planned == 1
    assert summary.local_recordings == 2
    assert summary.dry_run is False
    assert len(writer.calls) == 1
    call = writer.calls[0]
    assert call["store_path"] == tmp_path / "CloudRecordings.db"
    assert [item.unique_id for item in call["items"]] == ["A"]
    assert call["author"] == "com.zachlatta.pdw.voice-memo-writeback"


def test_writeback_runner_dry_run_never_writes(tmp_path) -> None:
    writer = FakeWriter()
    runner = _runner_fixture(tmp_path, writer=writer, dry_run=True)

    summary = runner.run()

    assert summary.dry_run is True
    assert summary.planned == 1
    assert summary.renamed == 0
    assert writer.calls == []


def test_writeback_runner_matches_drifted_filenames_by_sha(tmp_path) -> None:
    # Voice Memos rebases filename timestamps across timezone changes, so the
    # warehouse can know a memo only under an older stem. The audio sha (from
    # the uploader's state) still identifies it.
    _create_cloud_recordings_db(
        tmp_path / "CloudRecordings.db",
        [("A", "20260430 140736-AAAA1111.qta", "Some Park", 4100)],
    )
    session = FakeSession(
        _sql_envelope(
            [
                {
                    "recording_id": "20260430 110736-AAAA1111",
                    "content_sha256": "aaa111",
                    "title": "Park walk debrief",
                }
            ]
        )
    )
    writer = FakeWriter()
    runner = VoiceMemosWritebackRunner(
        recordings_path=tmp_path,
        account="zach@example.com",
        base_url="https://warehouse.example.com",
        secret_token="secret",
        logger=FakeLogger(),
        writer=writer,
        session=session,
        sha_by_filename={"20260430 140736-AAAA1111.qta": "aaa111"},
    )

    summary = runner.run()

    assert summary.planned == 1
    assert summary.renamed == 1
    item = writer.calls[0]["items"][0]
    assert item.unique_id == "A"
    assert item.new_title == "Park walk debrief"


def test_writeback_runner_counts_writer_side_skips(tmp_path) -> None:
    writer = FakeWriter(results={"A": "skipped_not_auto_named"})
    runner = _runner_fixture(tmp_path, writer=writer)

    summary = runner.run()

    assert summary.renamed == 0
    assert summary.skipped == 1


def test_writeback_runner_empty_plan_skips_writer(tmp_path) -> None:
    _create_cloud_recordings_db(
        tmp_path / "CloudRecordings.db",
        [("B", "20260102 100000-BBBB2222.qta", "My handwritten title", 4)],
    )
    writer = FakeWriter()
    runner = VoiceMemosWritebackRunner(
        recordings_path=tmp_path,
        account="zach@example.com",
        base_url="https://warehouse.example.com",
        secret_token="secret",
        logger=FakeLogger(),
        writer=writer,
        session=FakeSession(_sql_envelope([])),
    )

    summary = runner.run()

    assert summary.planned == 0
    assert summary.renamed == 0
    assert writer.calls == []
