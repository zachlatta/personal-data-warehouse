from datetime import UTC, datetime
from datetime import timedelta

import pytest

from personal_data_warehouse.timeline import adapter_by_name
from tests.test_timeline import _NOW, _engine, _ensure_all_source_tables, warehouse


@pytest.mark.parametrize(
    "prompt",
    [
        "Stock review run. You are one run of the Paseo schedule for stock reviews.",
        "Queued check. You are the single run of a one-shot Paseo schedule.",
        "Queued check. You are the one run of this Paseo schedule.",
        "Queued check. You are the only run of a Paseo schedule.",
        "Queued check. You're the only run of this one-shot Paseo schedule.",
        "You were started automatically because an email arrived in the agents' inbox.",
        "You were started automatically because a voice memo was routed as a task.",
        "You pick the repo for a new card on the agent factory board. Read-only routing.",
        "You choose the repository for a new card on the agent factory board. Read-only routing.",
    ],
)
def test_dispatch_and_schedule_envelopes_keep_the_whole_session_background(
    warehouse, prompt
):
    _ensure_all_source_tables(warehouse)
    for seq, text in enumerate(
        (prompt, "Please also check the latest fictional item.")
    ):
        warehouse._command(
            "INSERT INTO @claude_code_events (source,session_id,event_uuid,seq,occurred_at,"
            "role,text,cwd,entrypoint,ingested_at) "
            "VALUES ('claude_code','dispatched',%s,%s,%s,'user',%s,'/fictional/project','sdk-cli',%s)",
            (f"turn-{seq}", seq, _NOW, text, _NOW),
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
        "SELECT count(*) FROM @timeline_events WHERE priority<>'background'"
    ) == [(0,)]
    assert warehouse._query("SELECT count(*) FROM @timeline_events") == [(3,)]


@pytest.mark.parametrize("cwd", ["/tmp", "/private/tmp"])
def test_temporary_root_helper_needs_no_entrypoint_or_branch(warehouse, cwd):
    _ensure_all_source_tables(warehouse)
    warehouse._command(
        "INSERT INTO @argon_events (source,session_id,event_uuid,seq,occurred_at,role,text,cwd,ingested_at) "
        "VALUES ('argon','helper','turn',0,%s,'user','Inspect the fictional shell helper output.',%s,%s)",
        (_NOW, cwd, _NOW),
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
        "SELECT count(*) FROM @timeline_events WHERE priority<>'background'"
    ) == [(0,)]


def test_review_harness_turns_do_not_demote_the_human_session(warehouse):
    _ensure_all_source_tables(warehouse)
    messages = [
        "Help me understand this fictional algorithm.",
        "A reviewer (a different agent) requested changes to your plan (round 2 of 3). Read the notes.",
        "A reviewer (a different agent) requested changes to your code (round 1 of 3). Read the notes.",
        "Plan approved by the plan reviewer. Build it now.",
        "Code review passed (risky tier). Land it now.",
        "Example sent another email in the thread you are handling. Process the reply.",
    ]
    for seq, text in enumerate(messages):
        warehouse._command(
            "INSERT INTO @claude_code_events (source,session_id,event_uuid,seq,occurred_at,role,text,ingested_at) "
            "VALUES ('claude_code','human-session',%s,%s,%s,'user',%s,%s)",
            (f"turn-{seq}", seq, _NOW, text, _NOW),
        )
    # A model-generated headline is not an incoming reviewer control message.
    warehouse._command(
        "UPDATE @claude_code_events SET session_title='Code review passed (risky tier).' "
        "WHERE session_id='human-session'"
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
    assert actual["claude_code|human-session"] == "self"
    assert actual["claude_code|human-session|0"] == "self"
    for seq in range(1, len(messages)):
        assert actual[f"claude_code|human-session|{seq}"] == "background"


def test_daily_schedule_normalizes_worktree_slugs_and_daylight_saving(warehouse):
    _ensure_all_source_tables(warehouse)
    for i, occurred in enumerate(
        (
            datetime(2026, 10, 31, 8, tzinfo=UTC),
            datetime(2026, 11, 1, 9, tzinfo=UTC),
            datetime(2026, 11, 2, 9, tzinfo=UTC),
        )
    ):
        warehouse._command(
            "INSERT INTO @claude_code_events (source,session_id,event_uuid,seq,occurred_at,role,text,cwd,ingested_at) "
            "VALUES ('claude_code',%s,'opener',0,%s,'user','Start the pass',%s,%s)",
            (
                f"daily-{i}",
                occurred,
                f"/fictional/.paseo/worktrees/project/run-{i}/checks",
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
        "SELECT count(*) FROM @timeline_events WHERE priority<>'background'"
    ) == [(0,)]


@pytest.mark.parametrize(
    "source,cwd,prompt,expected",
    [
        (
            "claude_code",
            "/fictional/project",
            "Read README first. You are a read-only audit subagent for the parent goal.",
            "background",
        ),
        (
            "claude_code",
            "/fictional/project",
            "Read README first. You are the integration subagent in this shared worktree.",
            "background",
        ),
        (
            "claude_code",
            "/fictional/project",
            "Read README first. You are an independent code reviewer.",
            "background",
        ),
        (
            "claude_code",
            "/fictional/project",
            "User explicitly asks for a background worker. You are that runner.",
            "background",
        ),
        (
            "claude_code",
            "/fictional/project",
            "Read README first. Read-only reviewer of the parent-owned patch.",
            "background",
        ),
        (
            "claude_code",
            "/fictional/project",
            "Work in this tree. Parent will run the full suite. No staging or commits.",
            "background",
        ),
        (
            "claude_code",
            "/fictional/project",
            "Read README first. Another worker owns the frontend. No commits.",
            "background",
        ),
        (
            "claude_code",
            "/fictional/project",
            "Work in this tree. I own this worktree and will commit/land. No commits.",
            "background",
        ),
        (
            "claude_code",
            "/fictional/project",
            "Implement thumbnails. User explicitly delegates this to a background subagent.",
            "background",
        ),
        (
            "claude_code",
            "/fictional/project",
            "Investigate the schema, as explicitly requested by the user. Research/design only.",
            "background",
        ),
        (
            "claude_code",
            "/fictional/project",
            "Example explicitly requests you (Opus) to validate this design. Read-only review.",
            "background",
        ),
        (
            "claude_code",
            "/fictional/project",
            "Perform only a read-only computer-use compatibility check. Close your tab afterward.",
            "background",
        ),
        (
            "claude_code",
            "/fictional/project",
            "One-time read-only check, then stop. Inspect the fictional service.",
            "background",
        ),
        (
            "claude_code",
            "/fictional/project",
            "Report the current date and stop. Do not edit any files.",
            "background",
        ),
        (
            "claude_code",
            "/fictional/project",
            "Call the fictional MCP once with uuid 11111111-1111-4111-8111-111111111111. Run nothing else.",
            "background",
        ),
        (
            "muse",
            "/fictional/project",
            "Carry out this assignment end to end, then report the result.",
            "background",
        ),
        (
            "claude_code",
            "/fictional/.paseo/worktrees/project/email-fictional",
            "[Request interrupted by user]",
            "background",
        ),
        (
            "claude_code",
            "/fictional/project",
            "start a subagent to investigate this service",
            "self",
        ),
        ("claude_code", "/fictional/project", "have a reviewer look at this", "self"),
        (
            "claude_code",
            "/fictional/project",
            "start a subagent to perform a read-only audit of this service",
            "self",
        ),
        (
            "claude_code",
            "/fictional/project",
            "have a reviewer look at this; if another agent owns the tests, coordinate first",
            "self",
        ),
        (
            "claude_code",
            "/fictional/project",
            "How do I run a read-only check on this service?",
            "self",
        ),
        (
            "claude_code",
            "/fictional/project",
            "Check 3 contracts and record the results as an agent run. The parent requirement says you are a reviewer.",
            "self",
        ),
        ("claude_code", "/fictional/project", "[Request interrupted by user]", "self"),
    ],
)
def test_delegated_roles_and_probes_preserve_human_requests(
    warehouse, source, cwd, prompt, expected
):
    _ensure_all_source_tables(warehouse)
    warehouse._command(
        f"INSERT INTO @{source}_events (source,session_id,event_uuid,seq,occurred_at,role,text,cwd,ingested_at) "
        "VALUES (%s,'role-test','opener',0,%s,'user',%s,%s,%s)",
        (source, _NOW, prompt, cwd, _NOW),
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
    assert actual[f"{source}|role-test"] == expected
    # Interruption markers are control turns even in an otherwise human session.
    turn_expected = (
        "background" if prompt.startswith("[Request interrupted") else expected
    )
    assert actual[f"{source}|role-test|0"] == turn_expected
    assert warehouse._query("SELECT count(*) FROM @timeline_events") == [(2,)]


def test_agent_sender_envelope_only_demotes_the_injected_turn(warehouse):
    _ensure_all_source_tables(warehouse)
    for seq, text in enumerate(
        (
            "Help me understand this algorithm.",
            "From the testing agent (worktree fictional, branch checks): tests pass.",
        )
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
    actual = dict(
        warehouse._query("SELECT event_id,priority::text FROM @timeline_events")
    )
    assert actual == {
        "claude_code|human": "self",
        "claude_code|human|0": "self",
        "claude_code|human|1": "background",
    }


def test_numbered_email_cohort_is_bulk_but_individual_replies_are_self(warehouse):
    _ensure_all_source_tables(warehouse)
    for prefix, subject in (
        ("sequence", "Example course - week 2 of 6"),
        ("quotes", "Request for a quote"),
    ):
        for i in range(10):
            warehouse._command(
                "INSERT INTO @gmail_messages (account,message_id,thread_id,internal_date,from_address,subject,to_addresses,synced_at) "
                "VALUES ('me@example.test',%s,%s,%s,'me@example.test',%s,%s,%s)",
                (
                    f"{prefix}-{i}",
                    f"{prefix}-thread-{i}",
                    _NOW,
                    subject,
                    [f"person-{i}@example.test"],
                    _NOW,
                ),
            )
    warehouse._command(
        "INSERT INTO @gmail_messages (account,message_id,thread_id,internal_date,from_address,subject,to_addresses,synced_at) "
        "VALUES ('me@example.test','earlier','sequence-thread-0',%s,'person-0@example.test','Course question',ARRAY['me@example.test'],%s)",
        (_NOW - timedelta(hours=1), _NOW),
    )
    engine = _engine(warehouse, adapters=[adapter_by_name("gmail_email")])
    try:
        engine.run()
    finally:
        engine.close()
    actual = dict(
        warehouse._query("SELECT event_id,priority::text FROM @timeline_events")
    )
    assert actual["me@example.test|sequence-0"] == "self"
    for i in range(1, 10):
        assert actual[f"me@example.test|sequence-{i}"] == "noise"
    for i in range(10):
        assert actual[f"me@example.test|quotes-{i}"] == "self"
