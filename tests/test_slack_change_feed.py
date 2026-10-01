"""The change feed: which conversations actually need a history call."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from personal_data_warehouse.slack_change_feed import (
    SlackChangeFeed,
    changed_conversations,
)


def _counts(**buckets):
    return {"ok": True, **buckets}


def test_only_conversations_newer_than_our_cursor_are_returned():
    """This is the whole point: 690 conversations in, a handful out.

    The freshness pass polls up to 950 conversations every five minutes because
    the public API cannot say which ones changed. client.counts can, so the pass
    becomes "ask once, then fetch only what moved".
    """
    payload = _counts(
        ims=[
            {"id": "D1", "latest": "1787600000.000000"},
            {"id": "D2", "latest": "1787500000.000000"},
        ],
        mpims=[{"id": "C_MP", "latest": "1787600500.000000"}],
        channels=[{"id": "C1", "latest": "1787400000.000000"}],
    )
    cursors = {
        "D1": 1787500000.0,   # moved
        "D2": 1787500000.0,   # unchanged
        "C_MP": 1787600500.0,  # exactly at our cursor -> unchanged
        "C1": 1787400000.0,   # unchanged
    }
    assert changed_conversations(payload, cursors) == ["D1"]


def test_a_conversation_we_have_never_synced_counts_as_changed():
    payload = _counts(ims=[{"id": "D_NEW", "latest": "1787600000.000000"}])
    assert changed_conversations(payload, {}) == ["D_NEW"]


def test_entries_without_a_latest_marker_are_ignored_not_polled():
    # A bucket entry with no latest tells us nothing; treating it as changed
    # would quietly reintroduce the full-poll behaviour we are removing.
    payload = _counts(ims=[{"id": "D1"}, {"id": "D2", "latest": ""}])
    assert changed_conversations(payload, {}) == []


def test_results_are_ordered_newest_first():
    payload = _counts(
        ims=[
            {"id": "D_OLD", "latest": "1787600000.000000"},
            {"id": "D_NEW", "latest": "1787699999.000000"},
        ]
    )
    assert changed_conversations(payload, {}) == ["D_NEW", "D_OLD"]


def test_a_failed_counts_call_raises_rather_than_reporting_nothing_changed():
    """An empty answer and a broken answer must not look alike.

    Returning [] on failure would read as 'Slack is quiet' and silently stop all
    ingestion -- exactly the class of failure this whole change exists to fix.
    """
    with pytest.raises(SlackChangeFeed.Error, match="not_allowed_token_type"):
        changed_conversations({"ok": False, "error": "not_allowed_token_type"}, {})


def test_feed_reports_coverage_so_the_gap_is_visible():
    """client.counts covers conversations Zach is in -- not all ~13k channels.

    Measured on the real workspace: 316 channels (exactly the 317 he belongs to),
    237 open DMs, 137 open group DMs. The public channels he is not a member of
    are invisible here and still need the slow sweep, so the number is reported
    rather than assumed.
    """
    feed = SlackChangeFeed.from_counts(
        _counts(
            channels=[{"id": "C1", "latest": "1.0"}],
            ims=[{"id": "D1", "latest": "1.0"}],
            mpims=[],
        )
    )
    assert feed.covered_conversation_ids == {"C1", "D1"}
    assert feed.coverage == {"channels": 1, "ims": 1, "mpims": 0}


# --- the freshness pass's use of the feed -------------------------------------


def _settings(monkeypatch):
    from personal_data_warehouse.config import load_settings

    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    return load_settings(require_postgres=False, require_gmail=False, require_slack=True)


class _Warehouse:
    def __init__(self, session=None, cursors=None, known=None):
        self._session = session or {}
        self._cursors = cursors or {}
        # Conversations already in base_slack.conversations. Default to "we hold
        # whatever the feed names" so the tests written before the
        # another-workspace guard keep describing the healthy case.
        self._known = known

    def load_slack_session(self, **_):
        return self._session

    def load_slack_conversation_cursors(self, **_):
        return self._cursors

    def load_slack_known_conversation_ids(self, *, account, team_id, conversation_ids):
        wanted = {str(c) for c in conversation_ids}
        if self._known is None:
            return wanted
        return wanted & self._known


def test_freshness_limits_shrink_to_the_changed_set(monkeypatch):
    """With a session, the pass fetches what moved instead of everything.

    Production numbers: 950 conversations polled per five-minute cycle against a
    ~39 call/minute ceiling, for ~51 conversations that actually had activity.
    """
    from personal_data_warehouse.defs import slack_sync as slack_defs

    monkeypatch.setattr(
        slack_defs,
        "fetch_client_counts",
        lambda **_: {"ok": True, "ims": [{"id": "D1", "latest": "99.0"}], "channels": [], "mpims": []},
    )
    plan = slack_defs.slack_change_plan(
        settings=_settings(monkeypatch),
        warehouse=_Warehouse(
            session={"session_token": "xoxc-t", "session_cookie": "xoxd-c", "team_id": "T1"},
            cursors={"D1": 1.0},
        ),
        account="zrl",
        logger=NullLog(),
    )
    assert plan.usable is True
    assert plan.changed_conversation_ids == ("D1",)


def test_the_change_feed_sends_the_browser_user_agent_the_session_was_pasted_with(monkeypatch):
    """The session now comes from a browser, so its requests name that browser.

    The helper used to hard-code the Slack desktop app's User-Agent. For a
    session minted by Chrome that is a User-Agent the session has never
    presented, which Slack's anomaly detection reports as
    `unexpected_user_agent` (seen 2026-09-28 19:48 beside a sign-out).
    """
    from personal_data_warehouse.defs import slack_sync as slack_defs

    seen = []

    def fake_counts(**kwargs):
        seen.append(kwargs)
        return {"ok": True, "ims": [{"id": "D1", "latest": "99.0"}], "channels": [], "mpims": []}

    monkeypatch.setattr(slack_defs, "fetch_client_counts", fake_counts)
    slack_defs.slack_change_plan(
        settings=_settings(monkeypatch),
        warehouse=_Warehouse(
            session={
                "session_token": "xoxc-t",
                "session_cookie": "xoxd-c",
                "team_id": "T1",
                "user_agent": "Mozilla/5.0 Chrome/140",
            },
            cursors={"D1": 1.0},
        ),
        account="zrl",
        logger=NullLog(),
    )
    assert seen and all(call["user_agent"] == "Mozilla/5.0 Chrome/140" for call in seen)


def test_the_session_helper_puts_the_given_user_agent_on_the_wire(monkeypatch):
    import urllib.request

    from personal_data_warehouse import slack_session

    sent = {}

    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self):
            return b'{"ok": true}'

    def fake_urlopen(request, timeout):
        sent["ua"] = request.get_header("User-agent")
        sent["cookie"] = request.get_header("Cookie")
        return _Response()

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    assert slack_session._slack_post(
        "auth.test", token="xoxc-t", cookie_header="d=xoxd-c", user_agent="Mozilla/5.0 Chrome/140"
    ) == {"ok": True}
    assert sent == {"ua": "Mozilla/5.0 Chrome/140", "cookie": "d=xoxd-c"}


def test_no_session_falls_back_to_polling_rather_than_syncing_nothing(monkeypatch):
    """Absent credential must degrade to the old behaviour, not to silence."""
    from personal_data_warehouse.defs import slack_sync as slack_defs

    plan = slack_defs.slack_change_plan(
        settings=_settings(monkeypatch),
        warehouse=_Warehouse(session={}),
        account="zrl",
        logger=NullLog(),
    )
    assert plan.usable is False
    assert "no published Slack session" in plan.reason


def test_a_broken_counts_call_falls_back_to_polling(monkeypatch):
    # A revoked session must not stop ingestion; it must cost throughput only.
    from personal_data_warehouse.defs import slack_sync as slack_defs

    monkeypatch.setattr(
        slack_defs, "fetch_client_counts", lambda **_: {"ok": False, "error": "invalid_auth"}
    )
    plan = slack_defs.slack_change_plan(
        settings=_settings(monkeypatch),
        warehouse=_Warehouse(session={"session_token": "t", "session_cookie": "c", "team_id": "T1"}),
        account="zrl",
        logger=NullLog(),
    )
    assert plan.usable is False
    assert "invalid_auth" in plan.reason


def test_half_a_credential_is_not_used(monkeypatch):
    from personal_data_warehouse.defs import slack_sync as slack_defs

    plan = slack_defs.slack_change_plan(
        settings=_settings(monkeypatch),
        warehouse=_Warehouse(session={"session_token": "xoxc-t", "session_cookie": "", "team_id": "T1"}),
        account="zrl",
        logger=NullLog(),
    )
    assert plan.usable is False


class NullLog:
    def info(self, *a, **k):
        pass

    def warning(self, *a, **k):
        pass

    def error(self, *a, **k):
        pass


def test_the_freshness_pass_may_look_up_conversations_the_feed_names_but_we_lack(monkeypatch):
    """The change-feed path must be allowed to discover on demand.

    The feed reports ids; the freshness pass loads its candidates from the cached
    conversations table. Without a lookup budget those two disagree exactly when it
    matters -- on a conversation created since the paged discovery walk last ran --
    and the message waits for that walk. Measured 2026-08-28: a group DM created at
    16:02 reached the timeline at 05:36 the next day.
    """
    from personal_data_warehouse.defs import slack_sync as slack_defs

    captured: list[dict] = []

    class _Runner:
        def __init__(self, **kwargs):
            captured.append(kwargs)

        def sync_all(self):
            return []

    monkeypatch.setattr(
        slack_defs,
        "slack_change_plan",
        lambda **_: slack_defs.SlackChangePlan(usable=True, changed_conversation_ids=("D_NEW",)),
    )
    monkeypatch.setattr(slack_defs, "SlackSyncRunner", _Runner)
    monkeypatch.setenv("SLACK_ASSET_READ_STATE_WITH_FRESHNESS", "0")

    slack_defs.run_slack_freshness_sync(
        settings=_settings(monkeypatch), warehouse=_Warehouse(), logger=NullLog()
    )

    assert captured, "the freshness pass must build its per-type runners"
    assert all(kwargs["new_conversation_limit"] > 0 for kwargs in captured)
    assert all(kwargs["conversation_ids"] == ("D_NEW",) for kwargs in captured)


def test_a_feed_describing_another_workspace_is_not_usable(monkeypatch):
    """An `ok: true` payload about someone else's conversations is not a change feed.

    Hack Club is an Enterprise Grid org, and a session's `client.counts` can come
    back scoped to a sibling workspace. Production did exactly that twice:
    2026-08-27 18:15-19:15 and again from 2026-08-28 03:25, when the feed went
    from 694 conversations to 17 whose ids `conversations.info` answered
    `channel_not_found`. `plan.usable` stayed True, so the freshness pass polled
    those 13 "changed" ids -- which can never advance, because we cannot fetch
    them -- and synced **zero** messages for eleven hours while every other Slack
    health number read `ok`. That is the failure this module's docstring says must
    never happen ("nothing changed" vs "we could not ask"), wearing an ok:true.

    A feed naming conversations we do not hold degrades to polling, which costs
    throughput and never coverage.
    """
    from personal_data_warehouse.defs import slack_sync as slack_defs

    monkeypatch.setattr(
        slack_defs,
        "fetch_client_counts",
        lambda **_: {
            "ok": True,
            "channels": [{"id": "C_OTHER_ORG", "latest": "99.0"}],
            "ims": [{"id": "D_OTHER_ORG", "latest": "99.0"}],
            "mpims": [],
        },
    )
    plan = slack_defs.slack_change_plan(
        settings=_settings(monkeypatch),
        warehouse=_Warehouse(
            session={"session_token": "xoxc-t", "session_cookie": "xoxd-c", "team_id": "T1"},
            cursors={},
            known={"D_OURS", "C_OURS"},
        ),
        account="zrl",
        logger=NullLog(),
    )

    assert plan.usable is False
    assert "we hold 0" in plan.reason


def test_a_feed_describing_another_workspace_is_retried_with_an_explicit_workspace(monkeypatch):
    """The sibling-workspace answer is retried with the workspace named explicitly.

    The third and fourth episodes: 2026-09-02 15:00-17:00 and 2026-09-08 12:00
    through 2026-09-09 03:00 (fifteen hours), with the SAME token the hourly
    republish had verified against 685 conversations. The session did not
    change; Slack's answer to an org-scoped session did. The blanket poll the
    guard degrades to burns its rate budget in two minutes and synced zero DMs
    per pass, so DM landing p95 read 42 minutes. Before giving up, the plan asks
    again with the workspace named (form team_id, then the web client's
    slack_route), uses the first answer that names conversations we hold, and
    logs which variant did -- so the next episode says what fixes it.
    """
    from personal_data_warehouse.defs import slack_sync as slack_defs

    calls: list[dict] = []

    def fake_counts(**kwargs):
        calls.append(kwargs)
        if kwargs.get("variant") == "team_id" and kwargs.get("team_id") == "T1":
            return {
                "ok": True,
                "channels": [{"id": "C_OURS", "latest": "99.0"}],
                "ims": [{"id": "D_OURS", "latest": "99.0"}],
                "mpims": [],
            }
        return {
            "ok": True,
            "channels": [{"id": "C_OTHER_ORG", "latest": "99.0"}],
            "ims": [{"id": "D_OTHER_ORG", "latest": "99.0"}],
            "mpims": [],
        }

    monkeypatch.setattr(slack_defs, "fetch_client_counts", fake_counts)
    plan = slack_defs.slack_change_plan(
        settings=_settings(monkeypatch),
        warehouse=_Warehouse(
            session={
                "session_token": "xoxc-t",
                "session_cookie": "xoxd-c",
                "team_id": "T1",
                "enterprise_id": "E1",
            },
            cursors={},
            known={"D_OURS", "C_OURS"},
        ),
        account="zrl",
        logger=NullLog(),
    )

    assert plan.usable is True
    assert set(plan.changed_conversation_ids) == {"D_OURS", "C_OURS"}
    # The plain call first (it is what works every other day), then the scoped ones.
    assert [call["variant"] for call in calls] == ["plain", "team_id"]
    assert calls[1]["team_id"] == "T1" and calls[1]["enterprise_id"] == "E1"


def test_every_workspace_variant_describing_another_workspace_still_degrades_to_polling(monkeypatch):
    from personal_data_warehouse.defs import slack_sync as slack_defs

    calls: list[dict] = []

    def fake_counts(**kwargs):
        calls.append(kwargs)
        return {
            "ok": True,
            "channels": [{"id": "C_OTHER_ORG", "latest": "99.0"}],
            "ims": [{"id": "D_OTHER_ORG", "latest": "99.0"}],
            "mpims": [],
        }

    monkeypatch.setattr(slack_defs, "fetch_client_counts", fake_counts)
    plan = slack_defs.slack_change_plan(
        settings=_settings(monkeypatch),
        warehouse=_Warehouse(
            session={
                "session_token": "xoxc-t",
                "session_cookie": "xoxd-c",
                "team_id": "T1",
                "enterprise_id": "E1",
            },
            cursors={},
            known={"D_OURS", "C_OURS"},
        ),
        account="zrl",
        logger=NullLog(),
    )

    assert plan.usable is False
    assert "we hold 0" in plan.reason
    assert len(calls) == len(slack_defs.CLIENT_COUNTS_WORKSPACE_VARIANTS)


def test_a_feed_naming_our_conversations_stays_usable_when_one_is_brand_new(monkeypatch):
    """The guard must not fire on the normal case it sits next to.

    A conversation created since the discovery walk last ran is legitimately
    unknown, and fetching it is the point of the on-demand lookup. One new id
    among the workspace's ~690 is nothing like a feed that names none of them.
    """
    from personal_data_warehouse.defs import slack_sync as slack_defs

    monkeypatch.setattr(
        slack_defs,
        "fetch_client_counts",
        lambda **_: {
            "ok": True,
            "channels": [],
            "ims": [{"id": "D_OURS", "latest": "99.0"}, {"id": "D_BRAND_NEW", "latest": "99.0"}],
            "mpims": [{"id": "C_OURS", "latest": "99.0"}],
        },
    )
    plan = slack_defs.slack_change_plan(
        settings=_settings(monkeypatch),
        warehouse=_Warehouse(
            session={"session_token": "xoxc-t", "session_cookie": "xoxd-c", "team_id": "T1"},
            cursors={},
            known={"D_OURS", "C_OURS"},
        ),
        account="zrl",
        logger=NullLog(),
    )

    assert plan.usable is True
    assert set(plan.changed_conversation_ids) == {"D_OURS", "D_BRAND_NEW", "C_OURS"}


def test_a_warehouse_with_no_conversations_yet_polls_rather_than_trusting_the_feed(monkeypatch):
    """First run of a fresh warehouse: nothing is known, so nothing can vouch for
    the feed. Falling back to the blanket poll is what fills the conversations
    table in the first place."""
    from personal_data_warehouse.defs import slack_sync as slack_defs

    monkeypatch.setattr(
        slack_defs,
        "fetch_client_counts",
        lambda **_: {"ok": True, "ims": [{"id": "D1", "latest": "99.0"}], "channels": [], "mpims": []},
    )
    plan = slack_defs.slack_change_plan(
        settings=_settings(monkeypatch),
        warehouse=_Warehouse(
            session={"session_token": "xoxc-t", "session_cookie": "xoxd-c", "team_id": "T1"},
            cursors={},
            known=set(),
        ),
        account="zrl",
        logger=NullLog(),
    )

    assert plan.usable is False


class _VerdictWarehouse(_Warehouse):
    """Holds ops.slack_sync_state rows the way the verdict recorder uses them."""

    def __init__(self, session=None):
        super().__init__(session=session)
        self.rows: dict[tuple[str, str, str, str], dict] = {}

    def load_slack_sync_state_by_type(self, object_type):
        return {key: dict(row) for key, row in self.rows.items() if key[2] == object_type}

    def insert_slack_sync_state(self, **row):
        self.rows[(row["account"], row["team_id"], row["object_type"], row["object_id"])] = row

    def verdict(self):
        return self.rows[("zrl", "", "change_feed", "client.counts")]


def test_an_unusable_change_feed_becomes_a_credential_verdict_after_an_hour():
    """A week of fallback polling must not be invisible again.

    From 2026-09-21 every freshness pass logged "change feed unusable" -- the
    published session belonged to another workspace -- and fell back to the
    blanket poll, which never reached a group DM. The session row still read
    `ok`, the Slack pipeline read `ok`, and 164 of 219 group-DM messages in a
    week never landed. The verdict now lives in ops.slack_sync_state, whose
    `action_required` rows the Slack pipeline row reads as `attention` with the
    reason. One bad pass is not an incident (the 2026-09-02 sibling-workspace
    blips cleared on their own), so it takes an hour of continuous failure.
    """
    from datetime import timedelta

    from personal_data_warehouse.defs import slack_sync as slack_defs

    warehouse = _VerdictWarehouse(
        session={
            "team_url": "https://example-other.slack.com/",
            "published_at": datetime(2026, 9, 23, 19, 31, tzinfo=UTC),
        }
    )
    start = datetime(2026, 9, 21, 12, 0, tzinfo=UTC)
    bad = slack_defs.SlackChangePlan(
        usable=False,
        reason="client.counts named 17 conversations and we hold 0 of them; the feed is not describing this workspace",
    )

    slack_defs.record_slack_change_feed_verdict(warehouse=warehouse, account="zrl", plan=bad, now=start)
    first = warehouse.verdict()
    assert first["status"] == "degraded"  # neither error nor attention yet
    assert float(first["cursor_ts"]) == pytest.approx(start.timestamp())

    later = start + timedelta(minutes=65)
    slack_defs.record_slack_change_feed_verdict(warehouse=warehouse, account="zrl", plan=bad, now=later)
    row = warehouse.verdict()
    assert row["status"] == "action_required"
    assert float(row["cursor_ts"]) == pytest.approx(start.timestamp())  # since stays put
    assert row["updated_at"] == later
    for needle in (
        "2026-09-21T12:00", "we hold 0", "example-other.slack.com", "pdw slack publish-session",
        "app.slack.com",
    ):
        assert needle in row["error"], needle
    # The repair is a paste from a browser. The desktop-app capture it used to
    # name signed Zach out of every device on 2026-09-29.
    for stale in ("desktop app", "hourly publisher"):
        assert stale not in row["error"], stale

    slack_defs.record_slack_change_feed_verdict(
        warehouse=warehouse,
        account="zrl",
        plan=slack_defs.SlackChangePlan(usable=True, changed_conversation_ids=()),
        now=later + timedelta(minutes=5),
    )
    healed = warehouse.verdict()
    assert (healed["status"], healed["error"], healed["cursor_ts"]) == ("ok", "", "")


def test_a_disabled_change_feed_records_no_verdict():
    from personal_data_warehouse.defs import slack_sync as slack_defs

    warehouse = _VerdictWarehouse()
    slack_defs.record_slack_change_feed_verdict(
        warehouse=warehouse,
        account="zrl",
        plan=slack_defs.SlackChangePlan(usable=False, reason="change feed disabled"),
        now=datetime(2026, 9, 28, tzinfo=UTC),
    )
    assert warehouse.rows == {}


def test_the_freshness_pass_records_the_feed_verdict(monkeypatch):
    from personal_data_warehouse.defs import slack_sync as slack_defs

    class _Runner:
        def __init__(self, **kwargs):
            pass

        def sync_all(self):
            return []

    monkeypatch.setattr(
        slack_defs,
        "slack_change_plan",
        lambda **_: slack_defs.SlackChangePlan(usable=False, reason="invalid_auth"),
    )
    monkeypatch.setattr(slack_defs, "SlackSyncRunner", _Runner)
    monkeypatch.setenv("SLACK_ASSET_READ_STATE_WITH_FRESHNESS", "0")
    warehouse = _VerdictWarehouse()
    slack_defs.run_slack_freshness_sync(settings=_settings(monkeypatch), warehouse=warehouse, logger=NullLog())
    assert warehouse.verdict()["status"] == "degraded"
    assert "invalid_auth" in warehouse.verdict()["error"]


def test_a_poll_only_freshness_pass_lists_direct_conversations(monkeypatch):
    """With the change feed unusable, the pass finds new DMs and group DMs by listing
    them itself, not by waiting hours for the rotating discovery walk."""
    from personal_data_warehouse.defs import slack_sync as slack_defs

    captured: list[dict] = []

    class _Runner:
        def __init__(self, **kwargs):
            captured.append(kwargs)

        def sync_all(self):
            return []

    monkeypatch.setattr(
        slack_defs,
        "slack_change_plan",
        lambda **_: slack_defs.SlackChangePlan(usable=False, reason="client.counts failed: team_is_restricted"),
    )
    monkeypatch.setattr(slack_defs, "record_slack_change_feed_verdict", lambda **_: None)
    monkeypatch.setattr(slack_defs, "SlackSyncRunner", _Runner)
    monkeypatch.setenv("SLACK_ASSET_READ_STATE_WITH_FRESHNESS", "0")

    slack_defs.run_slack_freshness_sync(
        settings=_settings(monkeypatch), warehouse=_Warehouse(), logger=NullLog()
    )

    assert captured and all(kwargs["conversation_ids"] is None for kwargs in captured)
    assert all(kwargs["discover_new_direct_conversations"] is True for kwargs in captured)
