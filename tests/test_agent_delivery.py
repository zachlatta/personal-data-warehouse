"""Fictional cross-session deliveries; no real prompts or agent identifiers."""

import json
from datetime import timedelta
import shlex

import pytest

from personal_data_warehouse.timeline import adapter_by_name
from tests.test_timeline import _NOW, _engine, _ensure_all_source_tables, warehouse

PROMPT = (
    "Coordination: please inspect the fictional service and report its current state. "
    "The first build should use the isolated database and preserve the existing index. "
    "Return the exact test results to the coordinating agent before changing anything."
)


@pytest.mark.parametrize(
    "kind,offset,same_session,expected",
    [
        ("shell", -2, False, "background"),
        ("mcp", -2, False, "background"),
        ("interpolated", -2, False, "background"),
        ("shell", 2, False, "self"),
        ("shell", -601, False, "self"),
        ("shell", -2, True, "self"),
    ],
)
def test_delivery_proof_requires_an_earlier_call_from_another_session(
    warehouse, kind, offset, same_session, expected
):
    _ensure_all_source_tables(warehouse)
    for seq, text in ((0, "Help me understand this fictional service."), (10, PROMPT)):
        warehouse._command(
            "INSERT INTO @claude_code_events (source,session_id,event_uuid,seq,occurred_at,role,text,ingested_at) "
            "VALUES ('claude_code','human',%s,%s,%s,'user',%s,%s)",
            (str(seq), seq, _NOW, text, _NOW),
        )
    command = "paseo send receiver --no-wait " + shlex.quote(PROMPT)
    tool, args = "Bash", {"command": command}
    if kind == "mcp":
        tool, args = "mcp__paseo__send_agent_prompt", {
            "agent_id": "receiver",
            "prompt": PROMPT,
        }
    elif kind == "interpolated":
        # One runtime-expanded prefix; three independent body segments remain.
        args = {
            "command": 'MSG="'
            + "$PREFIX "
            + PROMPT[32:]
            + '"\npaseo send receiver "$MSG"'
        }
    warehouse._command(
        "INSERT INTO @claude_code_events (source,session_id,event_uuid,seq,occurred_at,role,tool_name,tool_input_json,ingested_at) "
        "VALUES ('claude_code',%s,'delivery',5,%s,'assistant',%s,%s,%s)",
        (
            "human" if same_session else "sender",
            _NOW + timedelta(seconds=offset),
            tool,
            json.dumps(args),
            _NOW,
        ),
    )
    engine = _engine(
        warehouse,
        adapters=[
            adapter_by_name("agent_session"),
            adapter_by_name("agent_session_turn"),
        ],
    )
    try:
        engine.run()
    finally:
        engine.close()
    actual = dict(
        warehouse._query("SELECT event_id,priority::text FROM @timeline_events")
    )
    assert actual["claude_code|human"] == "self"
    assert actual["claude_code|human|0"] == "self"
    assert actual["claude_code|human|10"] == expected


def test_similar_human_text_without_matching_delivery_stays_self(warehouse):
    _ensure_all_source_tables(warehouse)
    # Same introductory topic and ending, different independent middle segments.
    typed = (
        PROMPT[:32]
        + " A human asks about a different feature and an unrelated database. " * 3
        + PROMPT[-32:]
    )
    for seq, text in ((0, "Help me understand the fictional service."), (10, typed)):
        warehouse._command(
            "INSERT INTO @claude_code_events (source,session_id,event_uuid,seq,occurred_at,role,text,ingested_at) "
            "VALUES ('claude_code','human',%s,%s,%s,'user',%s,%s)",
            (str(seq), seq, _NOW, text, _NOW),
        )
    warehouse._command(
        "INSERT INTO @claude_code_events (source,session_id,event_uuid,seq,occurred_at,role,tool_name,tool_input_json,ingested_at) "
        "VALUES ('claude_code','sender','tool',0,%s,'assistant','Bash',%s,%s)",
        (
            _NOW - timedelta(seconds=2),
            json.dumps({"command": "paseo send receiver " + shlex.quote(PROMPT)}),
            _NOW,
        ),
    )
    engine = _engine(
        warehouse,
        adapters=[
            adapter_by_name("agent_session"),
            adapter_by_name("agent_session_turn"),
        ],
    )
    try:
        engine.run()
    finally:
        engine.close()
    assert warehouse._query(
        "SELECT priority::text FROM @timeline_events WHERE event_id='claude_code|human|10'"
    ) == [("self",)]


def test_explicit_agent_relay_marker_is_background_without_a_tool_record(warehouse):
    _ensure_all_source_tables(warehouse)
    for seq, text in (
        (0, "Help me inspect the fictional service."),
        (10, "Please stop this task (relayed by another agent at the user's request)."),
    ):
        warehouse._command(
            "INSERT INTO @claude_code_events (source,session_id,event_uuid,seq,occurred_at,role,text,ingested_at) "
            "VALUES ('claude_code','human',%s,%s,%s,'user',%s,%s)",
            (str(seq), seq, _NOW, text, _NOW),
        )
    engine = _engine(
        warehouse,
        adapters=[
            adapter_by_name("agent_session"),
            adapter_by_name("agent_session_turn"),
        ],
    )
    try:
        engine.run()
    finally:
        engine.close()
    assert warehouse._query(
        "SELECT priority::text FROM @timeline_events WHERE event_id='claude_code|human|10'"
    ) == [("background",)]
