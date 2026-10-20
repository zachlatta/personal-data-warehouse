"""Back-to-back recordings of one event become one session.

Zach stops a memo and starts another mid-event. The continuity pass asks an
agent, pair by pair, whether the later recording carries on the earlier one,
and marts_voice_memos.sessions joins the runs it says yes to. These tests pin
that time only proposes a pair (the verdict decides), that a session gets one
enrichment over all its members, and that the work is not redone.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.schema import (
    VOICE_MEMO_ENRICHMENT_COLUMNS,
    VOICE_MEMO_FILE_COLUMNS,
    VOICE_MEMO_TRANSCRIPTION_RUN_COLUMNS,
)
from personal_data_warehouse.voice_memo_continuity import (
    CONTINUITY_JUDGMENT_TASK_TYPE,
    JUDGMENT_TRANSCRIPT_EDGE_CHARS,
    SESSION_MERGE_TASK_TYPE,
    VoiceMemoContinuityRunner,
    continuity_task_input,
    load_continuity_pair_candidates,
)
from tests.conftest import cleanup_test_warehouse, make_test_schema
from tests.test_marts_layering import _postgres_url, _row

START = datetime(2026, 10, 20, 20, 0, tzinfo=UTC)
NOW = START + timedelta(hours=3)


@pytest.fixture()
def warehouse():
    wh = PostgresWarehouse(_postgres_url(), schema=make_test_schema("continuity"))
    try:
        yield wh
    finally:
        cleanup_test_warehouse(wh)


class Logger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


class FakeClient:
    """Answers like the agent: a verdict per pair, a merged record per session."""

    model = "fake-model"

    def __init__(self, same_event_pairs: set[tuple[str, str]]) -> None:
        self.same_event_pairs = same_event_pairs
        self.calls: list[dict] = []

    def create_agentic_structured(self, **kwargs):
        self.calls.append(kwargs)
        payload = json.loads(next(iter(kwargs["input_files"].values())))
        if kwargs["task_type"] == CONTINUITY_JUDGMENT_TASK_TYPE:
            pair = (payload["earlier_recording"]["recording_id"], payload["later_recording"]["recording_id"])
            same = pair in self.same_event_pairs
            return {"verdict": "same_event" if same else "separate", "confidence": 0.9, "reason": "test"}
        assert kwargs["task_type"] == SESSION_MERGE_TASK_TYPE
        ids = [item["recording_id"] for item in payload["recordings"]]
        return {
            "title": "Whole lecture",
            "summary": "One summary over " + " + ".join(ids),
            "participants": ["Speaker One", "Zach Latta"],
            "action_items": ["Follow up"],
            "evidence": ["the second recording picks up mid-question"],
        }


def _seed(wh: PostgresWarehouse, recordings: list[tuple[str, datetime, float, str]]) -> None:
    """(recording_id, recorded_at, duration_seconds, transcript), each enriched."""
    wh.ensure_apple_voice_memos_tables(backfill_content_hashes=False)
    wh.ensure_alice_voice_recordings_tables()
    for recording_id, recorded_at, duration, transcript in recordings:
        wh.insert_apple_voice_memos_files(
            [
                _row(
                    VOICE_MEMO_FILE_COLUMNS,
                    account="zach",
                    recording_id=recording_id,
                    title=recording_id,
                    filename=f"{recording_id}.m4a",
                    content_type="audio/mp4",
                    content_sha256=f"sha-{recording_id}",
                    recorded_at=recorded_at,
                    raw_metadata_json=json.dumps({"recording": {"duration_seconds": duration}}),
                    ingested_at=recorded_at,
                    sync_version=1,
                )
            ]
        )
        _transcribe(wh, recording_id, f"tid-{recording_id}", recorded_at + timedelta(minutes=1))
        wh.insert_apple_voice_memos_enrichments(
            [
                _row(
                    VOICE_MEMO_ENRICHMENT_COLUMNS,
                    source="apple_voice_memos",
                    account="zach",
                    recording_id=recording_id,
                    content_sha256=f"sha-{recording_id}",
                    provider="agent",
                    model="m",
                    prompt_version="v1",
                    status="completed",
                    title=f"Title {recording_id}",
                    transcript=transcript,
                    summary=f"Summary {recording_id}",
                    participants_json='["Zach Latta"]',
                    action_items_json="[]",
                    created_at=recorded_at + timedelta(minutes=2),
                    sync_version=1,
                )
            ]
        )


def _transcribe(wh: PostgresWarehouse, recording_id: str, transcript_id: str, completed_at: datetime) -> None:
    wh.insert_apple_voice_memos_transcription_runs(
        [
            _row(
                VOICE_MEMO_TRANSCRIPTION_RUN_COLUMNS,
                source="apple_voice_memos",
                account="zach",
                recording_id=recording_id,
                content_sha256=f"sha-{recording_id}",
                provider="assemblyai",
                provider_transcript_id=transcript_id,
                status="completed",
                transcript_text="run transcript",
                requested_at=completed_at,
                completed_at=completed_at,
                sync_version=int(completed_at.timestamp()),
            )
        ]
    )


def _runner(wh: PostgresWarehouse, client: FakeClient) -> VoiceMemoContinuityRunner:
    return VoiceMemoContinuityRunner(
        warehouse=wh, client=client, logger=Logger(), provider="agent_test", now=lambda: NOW
    )


def _sessions(wh: PostgresWarehouse) -> list[dict]:
    return wh._query_dicts(
        "SELECT session_id, recording_ids, recording_count, title, summary, transcript, "
        "participants_json, action_items_json, merge_status FROM @marts_voice_memos_sessions ORDER BY started_at"
    )


# A lecture stopped and restarted after 3 s, then an unrelated task memo a
# minute later, then a memo an hour later that no window would pair.
LECTURE = [
    ("lecture-1", START, 1500.0, "first half of the talk"),
    ("lecture-2", START + timedelta(seconds=1503), 600.0, "second half and the Q&A"),
    ("task", START + timedelta(seconds=2163), 20.0, "remind me to buy cheese"),
    ("later", START + timedelta(hours=2), 30.0, "an unrelated note"),
]


def test_the_agent_verdict_joins_a_restarted_event_and_keeps_a_task_memo_apart(
    warehouse: PostgresWarehouse,
) -> None:
    _seed(warehouse, LECTURE)
    client = FakeClient({("lecture-1", "lecture-2")})

    summary = _runner(warehouse, client).sync(limit=None)

    # Only the two close pairs were asked about; the hour-later memo was not.
    judged = [
        (c["input_files"], c["task_type"]) for c in client.calls if c["task_type"] == CONTINUITY_JUDGMENT_TASK_TYPE
    ]
    assert len(judged) == 2
    assert (summary.pairs_judged, summary.pairs_same_event, summary.sessions_merged) == (2, 1, 1)

    sessions = _sessions(warehouse)
    assert [s["recording_ids"] for s in sessions] == [["lecture-1", "lecture-2"], ["task"], ["later"]]
    merged, task, _later = sessions
    assert merged["merge_status"] == "merged"
    assert merged["title"] == "Whole lecture"
    assert merged["summary"] == "One summary over lecture-1 + lecture-2"
    assert merged["transcript"] == "first half of the talk\n\n[next recording]\n\nsecond half and the Q&A"
    assert json.loads(merged["participants_json"]) == ["Speaker One", "Zach Latta"]
    assert task["merge_status"] == "single"
    assert task["title"] == "Title task"
    assert task["summary"] == "Summary task"


def test_a_second_pass_redoes_nothing_until_a_transcript_changes(warehouse: PostgresWarehouse) -> None:
    _seed(warehouse, LECTURE)
    _runner(warehouse, FakeClient({("lecture-1", "lecture-2")})).sync(limit=None)

    again = FakeClient({("lecture-1", "lecture-2")})
    summary = _runner(warehouse, again).sync(limit=None)
    assert again.calls == []
    assert (summary.pairs_seen, summary.sessions_seen) == (0, 0)

    # A new transcript for the middle recording asks about both of its seams.
    _transcribe(warehouse, "lecture-2", "tid-lecture-2-new", START + timedelta(hours=1))
    pairs = load_continuity_pair_candidates(warehouse, limit=None)
    assert {(p["previous_recording_id"], p["recording_id"]) for p in pairs} == {
        ("lecture-1", "lecture-2"),
        ("lecture-2", "task"),
    }


def test_a_session_that_could_still_grow_waits_to_be_merged(warehouse: PostgresWarehouse) -> None:
    recent = [
        ("a", NOW - timedelta(minutes=5), 60.0, "start of a thought"),
        ("b", NOW - timedelta(minutes=3), 60.0, "the rest of it"),
    ]
    _seed(warehouse, recent)
    client = FakeClient({("a", "b")})

    summary = _runner(warehouse, client).sync(limit=None)

    assert summary.pairs_same_event == 1
    assert summary.sessions_seen == 0
    (session,) = _sessions(warehouse)
    assert session["recording_ids"] == ["a", "b"]
    # Joined already, with the first member's record standing in.
    assert session["merge_status"] == "pending"
    assert session["title"] == "Title a"


def test_a_failed_judgment_is_retried_then_given_up(warehouse: PostgresWarehouse) -> None:
    _seed(warehouse, LECTURE[:2])

    class Failing(FakeClient):
        def create_agentic_structured(self, **kwargs):
            self.calls.append(kwargs)
            raise RuntimeError("agent exploded")

    for attempt in range(1, 4):
        summary = _runner(warehouse, Failing(set())).sync(limit=None)
        assert summary.pairs_failed == 1
        (row,) = warehouse._query_dicts("SELECT status, attempts, error FROM @voice_memo_continuity_judgments")
        assert (row["status"], row["attempts"], row["error"]) == ("error", attempt, "agent exploded")

    assert load_continuity_pair_candidates(warehouse, limit=None) == []


def test_the_judge_reads_the_seam_end_of_one_and_start_of_the_next() -> None:
    earlier = "e" * (JUDGMENT_TRANSCRIPT_EDGE_CHARS - 1) + "END_OF_FIRST"
    later = "START_OF_SECOND" + "l" * JUDGMENT_TRANSCRIPT_EDGE_CHARS
    payload = json.loads(
        continuity_task_input(
            {
                "previous_recording_id": "a",
                "recording_id": "b",
                "previous_transcript": earlier,
                "transcript": later,
                "gap_seconds": 3.04,
            }
        )
    )
    assert payload["seconds_between_recordings"] == 3.0
    assert payload["earlier_recording"]["transcript_end"].endswith("END_OF_FIRST")
    assert payload["later_recording"]["transcript_start"].startswith("START_OF_SECOND")
    assert len(payload["later_recording"]["transcript_start"]) == JUDGMENT_TRANSCRIPT_EDGE_CHARS + 3
