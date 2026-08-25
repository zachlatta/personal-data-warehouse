from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from personal_data_warehouse.slack_session import (
    CapturedSlackSession,
    SlackSessionCaptureError,
    capture_slack_session,
    scan_local_storage_for_tokens,
)


def _write_leveldb(root: Path, files: dict[str, bytes]) -> Path:
    store = root / "Local Storage" / "leveldb"
    store.mkdir(parents=True)
    for name, payload in files.items():
        (store / name).write_bytes(payload)
    return root


def test_scan_finds_a_token_stored_as_plain_ascii(tmp_path):
    _write_leveldb(tmp_path, {"000005.ldb": b'\x01{"teams":{"T1":{"token":"xoxc-aaaa1111bbbb"}}}'})
    assert scan_local_storage_for_tokens(tmp_path) == ["xoxc-aaaa1111bbbb"]


def test_scan_finds_a_token_stored_as_utf16(tmp_path):
    # Chromium's localStorage keeps some values UTF-16LE, so a byte-level search
    # for "xoxc-" misses them entirely unless the UTF-16 form is searched too.
    payload = b"\x00" + '{"token":"xoxc-utf16token99"}'.encode("utf-16-le")
    _write_leveldb(tmp_path, {"026136.ldb": payload})
    assert scan_local_storage_for_tokens(tmp_path) == ["xoxc-utf16token99"]


def test_scan_returns_newest_file_first(tmp_path):
    # Several .ldb files carry a token because Slack rewrites localConfig on
    # login; the live one is in the most recently written file. Ordering matters
    # because the caller validates candidates in order and stops at the first
    # that Slack accepts.
    root = _write_leveldb(
        tmp_path,
        {
            "000005.ldb": b'{"token":"xoxc-oldoldold11"}',
            "026136.ldb": b'{"token":"xoxc-newnewnew22"}',
        },
    )
    store = root / "Local Storage" / "leveldb"
    import os

    os.utime(store / "000005.ldb", (1_000_000, 1_000_000))
    os.utime(store / "026136.ldb", (2_000_000, 2_000_000))
    assert scan_local_storage_for_tokens(root) == ["xoxc-newnewnew22", "xoxc-oldoldold11"]


def test_scan_deduplicates_and_ignores_non_tokens(tmp_path):
    _write_leveldb(
        tmp_path,
        {
            "a.ldb": b'xoxc-samesame1234 xoxc-samesame1234 xoxb-notauser99 xoxc-sh',
        },
    )
    # xoxb- is a bot token (wrong type) and "xoxc-sh" is too short to be real.
    assert scan_local_storage_for_tokens(tmp_path) == ["xoxc-samesame1234"]


def test_capture_picks_the_token_slack_actually_accepts(tmp_path, monkeypatch):
    # A stale token from a previous login sits in the same store as the live one,
    # and nothing on disk reliably says which is which. Correctness comes from
    # asking Slack, not from parsing LevelDB.
    root = _write_leveldb(
        tmp_path,
        {"old.ldb": b'xoxc-staletoken111', "new.ldb": b'xoxc-livetoken2222'},
    )
    import os

    store = root / "Local Storage" / "leveldb"
    # The stale token's file is OLDER, so it is offered second -- yet the live
    # token here is the newer file, so ordering alone would have picked right.
    # Reverse the mtimes so the stale one is tried FIRST and only Slack's answer
    # can save us.
    os.utime(store / "old.ldb", (2_000_000, 2_000_000))
    os.utime(store / "new.ldb", (1_000_000, 1_000_000))

    seen = []

    def fake_auth_test(*, token, cookie_header):
        seen.append(token)
        if token == "xoxc-livetoken2222":
            return {"ok": True, "team_id": "T0266FRGM", "user_id": "U09UE480JHH", "url": "https://hackclub.slack.com/"}
        return {"ok": False, "error": "invalid_auth"}

    session = capture_slack_session(
        store_root=root,
        cookies={"d": "xoxd-cookievalue"},
        cookie_expires_at=datetime(2027, 9, 28, tzinfo=UTC),
        source="slack-app",
        auth_test=fake_auth_test,
    )
    assert session.token == "xoxc-livetoken2222"
    assert session.team_id == "T0266FRGM"
    assert session.user_id == "U09UE480JHH"
    # Both were tried, stale first.
    assert seen == ["xoxc-staletoken111", "xoxc-livetoken2222"]


def test_capture_fails_loudly_when_no_token_is_accepted(tmp_path):
    root = _write_leveldb(tmp_path, {"a.ldb": b'xoxc-deadtoken1111'})
    with pytest.raises(SlackSessionCaptureError, match="no working"):
        capture_slack_session(
            store_root=root,
            cookies={"d": "xoxd-cookievalue"},
            cookie_expires_at=datetime(2027, 9, 28, tzinfo=UTC),
            source="slack-app",
            auth_test=lambda **_: {"ok": False, "error": "invalid_auth"},
        )


def test_capture_requires_the_session_cookie(tmp_path):
    # The xoxc token alone is useless: Slack rejects it without the `d` cookie,
    # so a missing cookie has to fail here rather than produce a credential that
    # looks fine and 401s on the server.
    root = _write_leveldb(tmp_path, {"a.ldb": b'xoxc-sometoken1111'})
    with pytest.raises(SlackSessionCaptureError, match="`d` session cookie"):
        capture_slack_session(
            store_root=root,
            cookies={"b": "not-the-session"},
            cookie_expires_at=None,
            source="slack-app",
            auth_test=lambda **_: {"ok": True},
        )


def test_redacted_view_carries_no_secret():
    session = CapturedSlackSession(
        source="slack-app",
        token="xoxc-secrettoken1",
        cookie_d="xoxd-secretcookie",
        team_id="T1",
        enterprise_id="",
        user_id="U1",
        team_url="https://hackclub.slack.com/",
        cookie_expires_at=datetime(2027, 9, 28, tzinfo=UTC),
    )
    blob = repr(session.redacted())
    assert "xoxc-secrettoken1" not in blob
    assert "xoxd-secretcookie" not in blob
    assert session.fingerprint() in blob
    assert session.redacted()["team_id"] == "T1"


def test_probe_output_never_prints_the_credential(capsys, monkeypatch):
    # The whole point of the probe is that Zach can run it in a terminal and
    # paste the result; it must be safe to paste.
    import personal_data_warehouse.slack_session as mod

    session = CapturedSlackSession(
        source="slack-app",
        token="xoxc-supersecrettoken",
        cookie_d="xoxd-supersecretcookie",
        team_id="T0266FRGM",
        enterprise_id="E09V59WQY1E",
        user_id="U09UE480JHH",
        team_url="https://hackclub.slack.com/",
        cookie_expires_at=datetime(2027, 9, 28, tzinfo=UTC),
    )
    monkeypatch.setattr(mod, "discover_slack_session", lambda **_: session)
    monkeypatch.setattr(
        mod,
        "probe_client_counts",
        lambda _s: {"ok": True, "channels": 812, "ims": 3627, "mpims": 2788,
                    "total_conversations": 7227, "with_latest_marker": 7227},
    )

    assert mod.main([]) == 0
    out = capsys.readouterr().out
    assert "xoxc-supersecrettoken" not in out
    assert "xoxd-supersecretcookie" not in out
    assert "T0266FRGM" in out
    assert "7227" in out


def test_probe_reports_a_failed_client_counts_as_a_nonzero_exit(capsys, monkeypatch):
    # A capture that works but a client.counts that does not means the whole
    # premise is wrong, so it must not look like success.
    import personal_data_warehouse.slack_session as mod

    session = CapturedSlackSession(
        source="slack-app", token="xoxc-t", cookie_d="xoxd-c", team_id="T1",
        enterprise_id="", user_id="U1", team_url="", cookie_expires_at=None,
    )
    monkeypatch.setattr(mod, "discover_slack_session", lambda **_: session)
    monkeypatch.setattr(mod, "probe_client_counts", lambda _s: {"ok": False, "error": "not_allowed_token_type"})

    assert mod.main([]) != 0
    assert "not_allowed_token_type" in capsys.readouterr().out


def test_capture_does_not_pass_off_an_enterprise_id_as_a_team_id(tmp_path):
    """Hack Club is on Enterprise Grid, and the client session reports the ORG.

    auth.test with the xoxc session returns team_id `E09V59WQY1E` and
    hackclub.enterprise.slack.com, while the app token returns the workspace
    `T0266FRGM` -- which is what all 23,342 conversations and 45M messages in the
    warehouse are keyed by. Storing the E-id as team_id would fork every row into
    a second, parallel dataset. Slack's E-prefix is the tell, so the capture
    refuses to call it a team_id.
    """
    root = _write_leveldb(tmp_path, {"a.ldb": b"xoxc-enterprisetok1"})
    session = capture_slack_session(
        store_root=root,
        cookies={"d": "xoxd-cookievalue"},
        cookie_expires_at=None,
        source="slack-app",
        auth_test=lambda **_: {
            "ok": True,
            "team_id": "E09V59WQY1E",
            "user_id": "U09UE480JHH",
            "url": "https://hackclub.enterprise.slack.com/",
        },
    )
    assert session.enterprise_id == "E09V59WQY1E"
    assert session.team_id == "", "an enterprise id must never be stored as a team id"
    assert session.redacted()["enterprise_id"] == "E09V59WQY1E"


def test_capture_keeps_a_real_workspace_team_id(tmp_path):
    root = _write_leveldb(tmp_path, {"a.ldb": b"xoxc-workspacetok1"})
    session = capture_slack_session(
        store_root=root,
        cookies={"d": "xoxd-cookievalue"},
        cookie_expires_at=None,
        source="slack-app",
        auth_test=lambda **_: {"ok": True, "team_id": "T0266FRGM", "user_id": "U1", "url": ""},
    )
    assert (session.team_id, session.enterprise_id) == ("T0266FRGM", "")


def test_workspace_id_is_resolved_from_the_enterprise_id(monkeypatch):
    """A client session knows only the org; the warehouse is keyed by workspace.

    base_slack.teams already carries the enterprise_id -> team_id mapping, so the
    publisher resolves it rather than asking the human to know it. Without this
    the credential would arrive with an empty team_id and the sync would have
    nothing to key its writes by.
    """
    from personal_data_warehouse import slack_setup

    monkeypatch.setattr(
        slack_setup, "_workspace_ids_for_enterprise", lambda _e: ["T0266FRGM"]
    )
    assert slack_setup.resolve_team_id(team_id="", enterprise_id="E09V59WQY1E") == "T0266FRGM"


def test_a_session_that_already_names_a_workspace_is_left_alone(monkeypatch):
    from personal_data_warehouse import slack_setup

    monkeypatch.setattr(slack_setup, "_workspace_ids_for_enterprise", lambda _e: ["T_OTHER"])
    assert slack_setup.resolve_team_id(team_id="T0266FRGM", enterprise_id="") == "T0266FRGM"


def test_an_ambiguous_enterprise_refuses_to_guess(monkeypatch):
    """Two workspaces under one org is a coin flip we must not take silently."""
    from personal_data_warehouse import slack_setup

    monkeypatch.setattr(
        slack_setup, "_workspace_ids_for_enterprise", lambda _e: ["T1", "T2"]
    )
    with pytest.raises(SlackSessionCaptureError, match="more than one workspace"):
        slack_setup.resolve_team_id(team_id="", enterprise_id="E1")


def test_an_unknown_enterprise_says_what_to_do(monkeypatch):
    from personal_data_warehouse import slack_setup

    monkeypatch.setattr(slack_setup, "_workspace_ids_for_enterprise", lambda _e: [])
    with pytest.raises(SlackSessionCaptureError, match="no workspace"):
        slack_setup.resolve_team_id(team_id="", enterprise_id="E_UNKNOWN")
