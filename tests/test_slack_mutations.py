from __future__ import annotations

from personal_data_warehouse.slack_mutations import (
    SLACK_MARK_CONVERSATION_READ_OPERATION,
    SlackMutationExecutor,
)


class _Warehouse:
    def __init__(self, *, session=None, target=None) -> None:
        self.session = dict(session or {})
        self.target = dict(target or {})
        self.target_calls = []

    def load_slack_session(self, *, account: str):
        assert account == "zrl"
        return self.session

    def load_slack_mark_read_target(self, **kwargs):
        self.target_calls.append(kwargs)
        return self.target


def _session(**overrides):
    session = {
        "session_token": "xoxc-secret",
        "session_cookie": "secret-cookie",
        "team_id": "T1",
        "enterprise_id": "E1",
        "user_id": "U1",
    }
    session.update(overrides)
    return session


def _target(**overrides):
    target = {
        "account": "zrl",
        "team_id": "T1",
        "conversation_id": "D1",
        "message_ts": "1593473566.000200",
        "is_member": 1,
        "is_im": 1,
        "is_mpim": 0,
        "is_archived": 0,
    }
    target.update(overrides)
    return target


def _mutation(**payload_overrides):
    payload = {"conversation_id": "D1", "message_ts": "1593473566.000200"}
    payload.update(payload_overrides)
    return {
        "provider": "slack",
        "operation": SLACK_MARK_CONVERSATION_READ_OPERATION,
        "account": "zrl",
        "payload_json": payload,
    }


def test_marks_exact_synced_message_read_with_xoxc_session() -> None:
    calls = []

    def call(method, *, token, cookie_header, form=None):
        calls.append((method, token, cookie_header, dict(form or {})))
        return {
            "auth.test": {"ok": True, "user_id": "U1", "team_id": "E1"},
            "conversations.info": {"ok": True, "channel": {"id": "D1", "last_read": "1593473500.000100"}},
            "conversations.mark": {"ok": True},
        }[method]

    warehouse = _Warehouse(session=_session(), target=_target())
    result = SlackMutationExecutor(warehouse=warehouse, slack_post=call).execute(_mutation())

    assert result.status == "succeeded"
    assert result.result_json == {
        "conversation_id": "D1",
        "message_ts": "1593473566.000200",
        "team_id": "T1",
        "already_read": False,
    }
    assert [item[0] for item in calls] == ["auth.test", "conversations.info", "conversations.mark"]
    assert all(item[2] == "d=secret-cookie" for item in calls)
    assert calls[-1][3] == {"channel": "D1", "ts": "1593473566.000200"}
    assert warehouse.target_calls == [
        {"account": "zrl", "team_id": "T1", "conversation_id": "D1", "message_ts": "1593473566.000200"}
    ]
    assert "xoxc-secret" not in repr(result)
    assert "secret-cookie" not in repr(result)


def test_already_read_is_idempotent_and_does_not_move_cursor_backwards() -> None:
    calls = []

    def call(method, **kwargs):
        calls.append(method)
        if method == "auth.test":
            return {"ok": True, "user_id": "U1", "team_id": "T1"}
        return {"ok": True, "channel": {"id": "D1", "last_read": "1593474000.000001"}}

    result = SlackMutationExecutor(
        warehouse=_Warehouse(session=_session(), target=_target()), slack_post=call
    ).execute(_mutation())

    assert result.status == "succeeded"
    assert result.result_json["already_read"] is True
    assert calls == ["auth.test", "conversations.info"]


def test_identity_mismatch_fails_before_target_or_write() -> None:
    warehouse = _Warehouse(session=_session(), target=_target())
    calls = []

    def call(method, **kwargs):
        calls.append(method)
        return {"ok": True, "user_id": "U-other", "team_id": "T1"}

    result = SlackMutationExecutor(warehouse=warehouse, slack_post=call).execute(_mutation())

    assert result.status == "blocked_missing_credentials"
    assert "identity" in result.error
    assert warehouse.target_calls == []
    assert calls == ["auth.test"]


def test_missing_xoxc_or_cookie_is_blocked_without_api_call() -> None:
    for missing in ("session_token", "session_cookie", "team_id", "user_id"):
        session = _session()
        session[missing] = ""
        executor = SlackMutationExecutor(
            warehouse=_Warehouse(session=session, target=_target()),
            slack_post=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not call Slack")),
        )
        result = executor.execute(_mutation())
        assert result.status == "blocked_missing_credentials"


def test_target_must_be_exact_synced_member_conversation() -> None:
    calls = []

    def call(method, **kwargs):
        calls.append(method)
        return {"ok": True, "user_id": "U1", "team_id": "T1"}

    result = SlackMutationExecutor(
        warehouse=_Warehouse(session=_session(), target={}), slack_post=call
    ).execute(_mutation())
    assert result.status == "failed_terminal"
    assert "synced" in result.error
    assert calls == ["auth.test"]


def test_slack_errors_are_safely_classified() -> None:
    cases = {
        "invalid_auth": "blocked_missing_credentials",
        "ratelimited": "failed_retryable",
        "http_503": "failed_retryable",
        "channel_not_found": "failed_terminal",
    }
    for error, expected_status in cases.items():
        def call(method, **kwargs):
            if method == "auth.test":
                return {"ok": True, "user_id": "U1", "team_id": "T1"}
            if method == "conversations.info":
                return {"ok": True, "channel": {"id": "D1", "last_read": "0"}}
            return {"ok": False, "error": error}

        result = SlackMutationExecutor(
            warehouse=_Warehouse(session=_session(), target=_target()), slack_post=call
        ).execute(_mutation())
        assert result.status == expected_status
        assert error in result.error
