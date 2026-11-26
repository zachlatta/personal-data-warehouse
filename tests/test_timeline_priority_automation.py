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
        # A schedule's own envelope: a job title, then "this is an unattended run".
        'Weekly audit (Paseo schedule "Weekly audit" on fictional-host). '
        "This is an unattended run: nobody answers questions, so never stop to wait.",
        # The workflow app dispatching a reviewer's question to a fresh agent.
        "Answer the reviewer's question 3 about workflow proposal "
        "00000000-1111-2222-3333-444444444444. This is a discussion task, not a workflow run.",
        # A one-shot follow-up an agent scheduled, behind a boilerplate first line.
        "Start by reading the start-here skill. You are a scheduled follow-up agent "
        "for a fictional refund request. Read the work item first.",
        # Zach never writes about himself in the third person; an agent does.
        "Zach requested the agent follow-up in the fictional checklist: finish only phase 2.",
        # A connectivity probe with a fixed reply format.
        "Call the fictional MCP tool get_item with uuid 0a1b2c3d-1111-2222-3333-444455556666. "
        "Reply with exactly: TEXT=<text>, or ERROR=<error>.",
        # The schedule declaration hard-wrapped across a line break.
        "One-off follow-up check for a fictional fix. You are the single run of a bounded\n"
        "Paseo schedule, in a fresh worktree. Read the note first.",
        # Run prompts a schedule from the recurring-agent-tasks skill starts: a
        # titled header plus the desk work item that is the run's memory.
        "One dated follow-up: 2026-01-02 fictional rollout audit and retry.\n"
        "Read the desk work item work/2026-01-02-fictional-followup/README.md and state.json.\n"
        "Authority: retry the fictional host only.",
        "Recurring desk watch: fictional continuity on real traffic. This is one bounded check. "
        "Read work/2026-01-03-fictional-watch/README.md and state.json; they hold the stop rules.",
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


def _run_agent_adapters(warehouse):
    engine = _engine(
        warehouse,
        adapters=[
            adapter_by_name("agent_session"),
            adapter_by_name("agent_session_turn"),
        ],
    )
    try:
        return engine.run()
    finally:
        engine.close()


def _agent_priorities(warehouse) -> dict[str, str]:
    return dict(
        warehouse._query("SELECT event_id,priority::text FROM @timeline_events")
    )


@pytest.mark.parametrize(
    "preamble",
    [
        "# AGENTS.md instructions for /fictional/project\n\n<INSTRUCTIONS>\nBe terse.\n</INSTRUCTIONS>",
        # Codex writes the global instructions block without a project path.
        "# AGENTS.md instructions <INSTRUCTIONS>\nBe terse.\n</INSTRUCTIONS>",
    ],
)
def test_codex_instructions_preamble_is_never_the_sessions_prompt(warehouse, preamble):
    """The harness block is skipped; the next user turn decides the session.

    On 2026-11-26 codex began writing `# AGENTS.md instructions <INSTRUCTIONS>`
    (no `for <path>`), so the preamble rule missed it and the block itself
    became the session's first prompt: 5 sessions and their preamble turns
    read `self` in one morning while their real opener was an orchestrator's.
    """
    _ensure_all_source_tables(warehouse)
    for session, opener in (
        ("human", "pull latest from main and keep going"),
        ("dispatched", '<paseo-system kind="agent-message">Do the step.</paseo-system>'),
    ):
        for seq, text in ((7, preamble), (11, opener)):
            warehouse._command(
                "INSERT INTO @codex_events (source,session_id,event_uuid,seq,occurred_at,role,text,ingested_at) "
                "VALUES ('codex',%s,%s,%s,%s,'user',%s,%s)",
                (session, f"t{seq}", seq, _NOW + timedelta(seconds=seq), text, _NOW),
            )
    _run_agent_adapters(warehouse)
    assert _agent_priorities(warehouse) == {
        "codex|human": "self",
        "codex|human|7": "background",
        "codex|human|11": "self",
        "codex|dispatched": "background",
        "codex|dispatched|7": "background",
        "codex|dispatched|11": "background",
    }


def test_a_session_started_from_a_scratch_directory_is_machinery(warehouse):
    """A probe an agent launches from its worktree's scratch folder is not Zach's."""
    _ensure_all_source_tables(warehouse)
    warehouse._command(
        "INSERT INTO @claude_code_events (source,session_id,event_uuid,seq,occurred_at,role,text,cwd,entrypoint,ingested_at) "
        "VALUES ('claude_code','probe','t0',0,%s,'user',"
        "'Which hosts does this repo manage? Answer in one line.',"
        "'/fictional/worktrees/abc/task/.factory-scratch','sdk-cli',%s)",
        (_NOW, _NOW),
    )
    # Claude Code records the CURRENT cwd per line: a human session whose agent
    # later cd'd into scratch is still his, and so are his later words.
    for seq, (text, cwd) in enumerate(
        (
            ("help me fix the fictional outliner page", "/fictional/project"),
            ("no, queue it up as a mutation instead", "/fictional/project/.factory-scratch/wt"),
        )
    ):
        warehouse._command(
            "INSERT INTO @claude_code_events (source,session_id,event_uuid,seq,occurred_at,role,text,cwd,entrypoint,ingested_at) "
            "VALUES ('claude_code','human',%s,%s,%s,'user',%s,%s,'sdk-cli',%s)",
            (f"t{seq}", seq, _NOW + timedelta(minutes=seq), text, cwd, _NOW),
        )
    _run_agent_adapters(warehouse)
    assert _agent_priorities(warehouse) == {
        "claude_code|probe": "background",
        "claude_code|probe|0": "background",
        "claude_code|human": "self",
        "claude_code|human|0": "self",
        "claude_code|human|1": "self",
    }


def test_recurring_prompt_evidence_that_arrives_later_demotes_the_first_runs(warehouse):
    """The first runs of a new hourly job converge to background.

    Recurrence is evidence from LATER runs, so the first three runs of a new
    hourly job were filed `self` when they landed and nothing revisited them:
    measured 2026-11-23, 4 of one job's 72 sessions (and their opening turns)
    stayed `self` while the rest were `background`. The refresh window
    re-walks recent `self` sessions and turns, so they converge.
    """
    _ensure_all_source_tables(warehouse)
    now = datetime.now(tz=UTC).replace(microsecond=0)
    prompt = "Read the fictional state file first, then do one pass of the hourly mute job."

    def run_at(hours_ago: int) -> None:
        session = f"hourly-{hours_ago}"
        warehouse._command(
            "INSERT INTO @claude_code_events (source,session_id,event_uuid,seq,occurred_at,role,text,cwd,entrypoint,ingested_at) "
            "VALUES ('claude_code',%s,'t3',3,%s,'user',%s,'/fictional/worktrees/abc/slug','sdk-cli',%s)",
            (session, now - timedelta(hours=hours_ago), prompt, now - timedelta(hours=hours_ago)),
        )

    run_at(6)
    _run_agent_adapters(warehouse)
    # One run is indistinguishable from a request Zach typed.
    assert _agent_priorities(warehouse)["claude_code|hourly-6"] == "self"

    for hours_ago in (5, 4, 3):
        run_at(hours_ago)
    stats = _run_agent_adapters(warehouse)
    assert {stat.adapter: stat.refreshed_rows > 0 for stat in stats} == {
        "agent_session": True,
        "agent_session_turn": True,
    }
    actual = _agent_priorities(warehouse)
    for hours_ago in (6, 5, 4, 3):
        assert actual[f"claude_code|hourly-{hours_ago}"] == "background"
        assert actual[f"claude_code|hourly-{hours_ago}|3"] == "background"


@pytest.mark.parametrize(
    "changed",
    [
        ("agent_session", "agent_session_turn"),
        # A session-only rule change must re-walk the turns that copy it.
        ("agent_session",),
    ],
)
def test_a_rewalk_classifies_turns_after_their_sessions(warehouse, changed):
    """A turn copies its session's tier, so history re-walks sessions first.

    Backfill walks adapters round-robin, newest first. A session's row sits at
    its START and its turns at their own times, so the turn walk can reach a
    turn before the session walk has re-classified its session; the turn then
    keeps the stale tier forever once it is outside the refresh window.
    """
    _ensure_all_source_tables(warehouse)
    now = datetime.now(tz=UTC).replace(microsecond=0)
    old = now - timedelta(days=40)
    prompt = "Read the fictional state file first, then do one pass of the hourly mute job."

    def insert(session, occurred, text, ingested, *, role="user", seq=3):
        warehouse._command(
            "INSERT INTO @claude_code_events (source,session_id,event_uuid,seq,occurred_at,role,text,cwd,entrypoint,ingested_at) "
            "VALUES ('claude_code',%s,%s,%s,%s,%s,%s,'/fictional/worktrees/abc/slug','sdk-cli',%s)",
            (session, f"t{seq}", seq, occurred, role, text, ingested),
        )

    insert("first-run", old, prompt, old)
    # Sessions with no text turns sit between the job and now, so the
    # re-walk's session pages lag its turn pages.
    for day in (1, 2, 3, 4, 5):
        insert(f"silent-{day}", now - timedelta(days=day), "", now - timedelta(hours=1),
               role="assistant", seq=1)
    _run_agent_adapters(warehouse)
    assert _agent_priorities(warehouse)["claude_code|first-run|3"] == "self"

    # Later runs prove the prompt recurs, long outside the refresh window.
    for hours in (1, 2, 3):
        insert(f"run-{hours}", old + timedelta(hours=hours), prompt, now)
    warehouse._command(
        "UPDATE @timeline_sync_state SET adapter_signature = 'stale' "
        "WHERE adapter = ANY(%s)",
        (list(changed),),
    )
    engine = _engine(
        warehouse,
        adapters=[adapter_by_name("agent_session"), adapter_by_name("agent_session_turn")],
        batch_size=1,
    )
    try:
        for _ in range(3):
            engine.run()
    finally:
        engine.close()
    actual = _agent_priorities(warehouse)
    assert actual["claude_code|first-run"] == "background"
    assert actual["claude_code|first-run|3"] == "background"


def test_backfill_parents_are_registered_earlier():
    from personal_data_warehouse.timeline import TIMELINE_ADAPTERS

    names = [adapter.name for adapter in TIMELINE_ADAPTERS]
    dependents = [a for a in TIMELINE_ADAPTERS if a.backfill_after]
    assert [a.name for a in dependents] == ["agent_session_turn"]
    for adapter in dependents:
        assert names.index(adapter.backfill_after) < names.index(adapter.name)


def test_agent_refresh_rewalks_only_recent_self_rows():
    """Both agent adapters re-walk a bounded window of their own `self` rows.

    Only a `self` row can be demoted by later recurrence evidence, so the
    refresh query reads that set from the timeline rather than regrouping the
    whole transcript table every pass.
    """
    for name in ("agent_session", "agent_session_turn"):
        adapter = adapter_by_name(name)
        assert adapter.refresh_hours >= 24 * 15, name
        assert ">= %(cutoff_ts)s" in adapter.refresh_sql, name
        assert "priority = 'self'" in adapter.refresh_sql, name
        assert f"adapter = '{name}'" in adapter.refresh_sql, name


def test_review_harness_turns_do_not_demote_the_human_session(warehouse):
    _ensure_all_source_tables(warehouse)
    messages = [
        "Help me understand this fictional algorithm.",
        "A reviewer (a different agent) requested changes to your plan (round 2 of 3). Read the notes.",
        "A reviewer (a different agent) requested changes to your code (round 1 of 3). Read the notes.",
        "Plan approved by the plan reviewer. Build it now.",
        "Code review passed (risky tier). Land it now.",
        "Example sent another email in the thread you are handling. Process the reply.",
        # An orchestrator relaying a worker's result into the lead's user channel.
        "[Fictional update] (from the system, not the human) - Worker finished its turn.",
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
            "This is an authorized shell smoke check only. Run exactly echo ok and report the output.",
            "background",
        ),
        (
            "claude_code",
            "/fictional/project",
            "is the fictional deploy a smoke test only, or does it ship?",
            "self",
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
        (
            "claude_code",
            "/fictional/project",
            "call the fictional MCP tool and tell me what it returns",
            "self",
        ),
        ("claude_code", "/fictional/project", "zach here: check the fictional build", "self"),
        (
            "claude_code",
            "/fictional/project",
            "how do scheduled agents work? are you a scheduled agent?",
            "self",
        ),
        # Asking for a follow-up or a recurring task is Zach's request, and so
        # is resuming a work item by hand: only header AND work-item memory
        # together are a schedule's run prompt.
        (
            "claude_code",
            "/fictional/project",
            "one dated follow-up: remind me to call the fictional plumber tomorrow",
            "self",
        ),
        (
            "claude_code",
            "/fictional/project",
            "set up a recurring watch: check my fictional flight every two hours and keep me posted",
            "self",
        ),
        (
            "claude_code",
            "/fictional/project",
            "read work/2026-01-02-fictional-followup/README.md and state.json and tell me where we are",
            "self",
        ),
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
