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


# --- slack.send_message -------------------------------------------------------

from datetime import UTC, datetime, timedelta  # noqa: E402

from personal_data_warehouse.slack_mutations import (  # noqa: E402
    SLACK_SEND_MESSAGE_OPERATION,
    slack_send_message_client_msg_id,
)


class _SendWarehouse(_Warehouse):
    def __init__(self, *, session=None, conversation=None, user=None, dm=None, thread_parent=None, stored=None) -> None:
        super().__init__(session=session, target=thread_parent)
        self.conversation = dict(conversation or {})
        self.user = dict(user or {})
        self.dm = dict(dm or {})
        self.stored = dict(stored or {})
        self.lookups = []

    def load_slack_conversation_target(self, **kwargs):
        self.lookups.append(("conversation", kwargs))
        return self.conversation

    def load_slack_user_target(self, **kwargs):
        self.lookups.append(("user", kwargs))
        return self.user

    def load_slack_dm_conversation(self, **kwargs):
        self.lookups.append(("dm", kwargs))
        return self.dm

    def load_slack_message_target(self, **kwargs):
        self.lookups.append(("message", kwargs))
        return self.target

    def find_slack_message_by_client_msg_id(self, **kwargs):
        self.lookups.append(("client_msg_id", kwargs))
        return self.stored


APPROVED_AT = datetime(2026, 9, 24, 15, 0, tzinfo=UTC)


def _channel(**overrides):
    row = {
        "account": "zrl",
        "team_id": "T1",
        "conversation_id": "C1",
        "conversation_type": "public_channel",
        "name": "ops",
        "is_member": 1,
        "is_im": 0,
        "is_mpim": 0,
        "is_archived": 0,
    }
    row.update(overrides)
    return row


def _send(mutation_id="mut-1", **payload_overrides):
    payload = {"conversation_id": "C1", "user_id": "", "text": "On it.", "thread_ts": "", "reply_broadcast": False}
    payload.update(payload_overrides)
    return {
        "id": mutation_id,
        "provider": "slack",
        "operation": SLACK_SEND_MESSAGE_OPERATION,
        "account": "zrl",
        "approved_at": APPROVED_AT,
        "payload_json": payload,
    }


class _SlackAPI:
    """A scripted Slack: one response per method, every call recorded with its form."""

    def __init__(self, responses, *, raise_on=()):
        self.responses = dict(responses)
        self.raise_on = set(raise_on)
        self.calls = []

    def __call__(self, method, *, token, cookie_header, form=None):
        self.calls.append((method, token, cookie_header, dict(form or {})))
        if method in self.raise_on:
            raise TimeoutError("boom")
        response = self.responses[method]
        return response(dict(form or {})) if callable(response) else response

    @property
    def methods(self):
        return [call[0] for call in self.calls]

    def form(self, method):
        return next(call[3] for call in self.calls if call[0] == method)


def _ok_auth():
    return {"ok": True, "user_id": "U1", "team_id": "T1"}


def test_client_msg_id_is_derived_from_the_mutation_id_alone() -> None:
    assert slack_send_message_client_msg_id("mut-1") == slack_send_message_client_msg_id("mut-1")
    assert slack_send_message_client_msg_id("mut-1") != slack_send_message_client_msg_id("mut-2")
    assert len(slack_send_message_client_msg_id("mut-1")) == 36


def test_sends_to_a_synced_channel_after_checking_for_its_own_message() -> None:
    api = _SlackAPI(
        {
            "auth.test": _ok_auth(),
            "conversations.info": {"ok": True, "channel": {"id": "C1", "is_archived": False}},
            "conversations.history": {"ok": True, "messages": [{"ts": "1.1", "user": "U2", "text": "hello"}]},
            "chat.postMessage": {"ok": True, "ts": "1700000000.000100", "channel": "C1"},
        }
    )
    warehouse = _SendWarehouse(session=_session(), conversation=_channel())
    result = SlackMutationExecutor(warehouse=warehouse, slack_post=api).execute(_send())

    assert result.status == "succeeded"
    client_msg_id = slack_send_message_client_msg_id("mut-1")
    assert result.result_json == {
        "conversation_id": "C1",
        "user_id": "",
        "thread_ts": "",
        "client_msg_id": client_msg_id,
        "team_id": "T1",
        "message_ts": "1700000000.000100",
        "already_sent": False,
    }
    assert api.methods == ["auth.test", "conversations.info", "conversations.history", "chat.postMessage"]
    assert api.form("chat.postMessage") == {"channel": "C1", "text": "On it.", "client_msg_id": client_msg_id}
    history = api.form("conversations.history")
    assert history["channel"] == "C1" and history["limit"] == "200"
    # The pre-check reads from five minutes before the approval, not from the epoch.
    assert history["oldest"] == f"{(APPROVED_AT - timedelta(minutes=5)).timestamp():.6f}"
    assert [name for name, _ in warehouse.lookups] == ["conversation", "client_msg_id"]
    assert warehouse.lookups[0][1] == {"account": "zrl", "team_id": "T1", "conversation_id": "C1"}
    assert "xoxc-secret" not in repr(result) and "secret-cookie" not in repr(result)


def test_dm_by_user_id_reuses_the_synced_dm_and_proves_it_is_theirs() -> None:
    api = _SlackAPI(
        {
            "auth.test": _ok_auth(),
            "conversations.info": {"ok": True, "channel": {"id": "D1", "is_im": True, "user": "UMARCUS"}},
            "conversations.history": {"ok": True, "messages": []},
            "chat.postMessage": {"ok": True, "ts": "2.2"},
        }
    )
    warehouse = _SendWarehouse(
        session=_session(),
        user={"user_id": "UMARCUS", "is_bot": 0, "is_deleted": 0},
        dm={"conversation_id": "D1", "is_archived": 0},
    )
    result = SlackMutationExecutor(warehouse=warehouse, slack_post=api).execute(
        _send(conversation_id="", user_id="UMARCUS", text="Hi Marcus")
    )
    assert result.status == "succeeded"
    assert result.result_json["conversation_id"] == "D1"
    assert result.result_json["user_id"] == "UMARCUS"
    assert result.result_json["dm_source"] == "warehouse"
    assert "conversations.open" not in api.methods
    assert api.form("chat.postMessage")["channel"] == "D1"


def test_dm_by_user_id_opens_the_dm_when_the_warehouse_holds_none() -> None:
    api = _SlackAPI(
        {
            "auth.test": _ok_auth(),
            "conversations.open": {"ok": True, "channel": {"id": "D9"}},
            "conversations.info": {"ok": True, "channel": {"id": "D9", "is_im": True, "user": "UNEW"}},
            "conversations.history": {"ok": True, "messages": []},
            "chat.postMessage": {"ok": True, "ts": "3.3"},
        }
    )
    warehouse = _SendWarehouse(session=_session(), user={"user_id": "UNEW", "is_bot": 0, "is_deleted": 0}, dm={})
    result = SlackMutationExecutor(warehouse=warehouse, slack_post=api).execute(
        _send(conversation_id="", user_id="UNEW", text="Welcome!")
    )
    assert result.status == "succeeded"
    assert api.methods == ["auth.test", "conversations.open", "conversations.info", "conversations.history", "chat.postMessage"]
    assert api.form("conversations.open") == {"users": "UNEW", "return_im": "true"}
    assert result.result_json["conversation_id"] == "D9"
    assert result.result_json["dm_source"] == "conversations.open"


def test_dm_that_slack_says_belongs_to_someone_else_is_refused() -> None:
    api = _SlackAPI(
        {
            "auth.test": _ok_auth(),
            "conversations.info": {"ok": True, "channel": {"id": "D1", "is_im": True, "user": "UOTHER"}},
        }
    )
    warehouse = _SendWarehouse(session=_session(), user={"user_id": "UMARCUS"}, dm={"conversation_id": "D1"})
    result = SlackMutationExecutor(warehouse=warehouse, slack_post=api).execute(
        _send(conversation_id="", user_id="UMARCUS")
    )
    assert result.status == "failed_terminal"
    assert "recipient" in result.error
    assert "chat.postMessage" not in api.methods


def test_thread_reply_lands_under_the_parent_and_can_broadcast() -> None:
    api = _SlackAPI(
        {
            "auth.test": _ok_auth(),
            "conversations.info": {"ok": True, "channel": {"id": "C1"}},
            "conversations.replies": {"ok": True, "messages": [{"ts": "1593473600.000300", "user": "U2", "text": "parent"}]},
            "chat.postMessage": {"ok": True, "ts": "4.4"},
        }
    )
    # The proposal named a reply inside the thread; Slack threads hang off the parent.
    warehouse = _SendWarehouse(
        session=_session(),
        conversation=_channel(),
        thread_parent={"message_ts": "1593473660.000400", "thread_ts": "1593473600.000300"},
    )
    result = SlackMutationExecutor(warehouse=warehouse, slack_post=api).execute(
        _send(thread_ts="1593473660.000400", reply_broadcast=True, text="Fixed.")
    )
    assert result.status == "succeeded"
    assert result.result_json["thread_ts"] == "1593473600.000300"
    assert result.result_json["thread_ts_requested"] == "1593473660.000400"
    assert api.form("chat.postMessage") == {
        "channel": "C1",
        "text": "Fixed.",
        "client_msg_id": slack_send_message_client_msg_id("mut-1"),
        "thread_ts": "1593473600.000300",
        "reply_broadcast": "true",
    }
    assert api.form("conversations.replies")["ts"] == "1593473600.000300"
    assert warehouse.lookups[1] == (
        "message",
        {"account": "zrl", "team_id": "T1", "conversation_id": "C1", "message_ts": "1593473660.000400"},
    )


def test_thread_reply_needs_a_synced_parent() -> None:
    api = _SlackAPI({"auth.test": _ok_auth(), "conversations.info": {"ok": True, "channel": {"id": "C1"}}})
    warehouse = _SendWarehouse(session=_session(), conversation=_channel(), thread_parent={})
    result = SlackMutationExecutor(warehouse=warehouse, slack_post=api).execute(_send(thread_ts="9.9"))
    assert result.status == "failed_terminal"
    assert "thread parent" in result.error
    assert "chat.postMessage" not in api.methods


def test_a_message_already_synced_back_is_not_sent_again() -> None:
    api = _SlackAPI({"auth.test": _ok_auth(), "conversations.info": {"ok": True, "channel": {"id": "C1"}}})
    warehouse = _SendWarehouse(session=_session(), conversation=_channel(), stored={"message_ts": "5.5"})
    result = SlackMutationExecutor(warehouse=warehouse, slack_post=api).execute(_send())
    assert result.status == "succeeded"
    assert result.result_json["already_sent"] is True
    assert result.result_json["message_ts"] == "5.5"
    assert result.result_json["matched_by"] == "warehouse_client_msg_id"
    assert api.methods == ["auth.test", "conversations.info"]


def test_a_message_slack_already_holds_is_not_sent_again() -> None:
    client_msg_id = slack_send_message_client_msg_id("mut-1")
    api = _SlackAPI(
        {
            "auth.test": _ok_auth(),
            "conversations.info": {"ok": True, "channel": {"id": "C1"}},
            "conversations.history": {
                "ok": True,
                "messages": [
                    {"ts": "6.5", "user": "U2", "text": "On it."},
                    {"ts": "6.6", "user": "U1", "client_msg_id": client_msg_id, "text": "On it."},
                ],
            },
        }
    )
    result = SlackMutationExecutor(
        warehouse=_SendWarehouse(session=_session(), conversation=_channel()), slack_post=api
    ).execute(_send())
    assert result.status == "succeeded"
    assert result.result_json["already_sent"] is True
    assert result.result_json["message_ts"] == "6.6"
    assert result.result_json["matched_by"] == "client_msg_id"
    assert "chat.postMessage" not in api.methods


def test_the_text_match_tolerates_slacks_own_rewriting_of_a_message() -> None:
    api = _SlackAPI(
        {
            "auth.test": _ok_auth(),
            "conversations.info": {"ok": True, "channel": {"id": "C1"}},
            "conversations.history": {
                "ok": True,
                "messages": [
                    {"ts": "7.7", "user": "U1", "text": "Docs &amp; notes:  <https://example.test/x|example.test/x>"},
                    {"ts": "7.8", "user": "U2", "text": "Docs & notes: https://example.test/x"},
                ],
            },
        }
    )
    result = SlackMutationExecutor(
        warehouse=_SendWarehouse(session=_session(), conversation=_channel()), slack_post=api
    ).execute(_send(text="Docs & notes: https://example.test/x"))
    assert result.status == "succeeded"
    assert result.result_json["already_sent"] is True
    # Only the session's own message counts; someone else's identical words do not.
    assert result.result_json["message_ts"] == "7.7"
    assert result.result_json["matched_by"] == "text"


def test_an_unrunnable_precheck_refuses_to_send() -> None:
    api = _SlackAPI(
        {
            "auth.test": _ok_auth(),
            "conversations.info": {"ok": True, "channel": {"id": "C1"}},
            "chat.postMessage": {"ok": True, "ts": "8.8"},
        },
        raise_on=("conversations.history",),
    )
    result = SlackMutationExecutor(
        warehouse=_SendWarehouse(session=_session(), conversation=_channel()), slack_post=api
    ).execute(_send())
    assert result.status == "failed_retryable"
    assert "refusing to send unchecked" in result.error
    assert "chat.postMessage" not in api.methods

    rejected = _SlackAPI(
        {
            "auth.test": _ok_auth(),
            "conversations.info": {"ok": True, "channel": {"id": "C1"}},
            "conversations.history": {"ok": False, "error": "invalid_auth"},
        }
    )
    result = SlackMutationExecutor(
        warehouse=_SendWarehouse(session=_session(), conversation=_channel()), slack_post=rejected
    ).execute(_send())
    assert result.status == "blocked_missing_credentials"
    assert result.result_json["client_msg_id"] == slack_send_message_client_msg_id("mut-1")


def test_a_lost_post_response_is_retryable_and_says_why() -> None:
    api = _SlackAPI(
        {
            "auth.test": _ok_auth(),
            "conversations.info": {"ok": True, "channel": {"id": "C1"}},
            "conversations.history": {"ok": True, "messages": []},
        },
        raise_on=("chat.postMessage",),
    )
    result = SlackMutationExecutor(
        warehouse=_SendWarehouse(session=_session(), conversation=_channel()), slack_post=api
    ).execute(_send())
    assert result.status == "failed_retryable"
    assert "looks for the message before sending again" in result.error


def test_recipient_checks_happen_before_any_post() -> None:
    cases = [
        ({"conversation": {}}, "not synced"),
        ({"conversation": _channel(is_archived=1)}, "archived"),
        ({"conversation": _channel(is_member=0)}, "post in"),
        ({"conversation": _channel(team_id="T2")}, "different workspace"),
    ]
    for warehouse_kwargs, expected in cases:
        api = _SlackAPI({"auth.test": _ok_auth(), "chat.postMessage": {"ok": True, "ts": "9.9"}})
        result = SlackMutationExecutor(
            warehouse=_SendWarehouse(session=_session(), **warehouse_kwargs), slack_post=api
        ).execute(_send())
        assert result.status == "failed_terminal", expected
        assert expected in result.error
        assert api.methods == ["auth.test"]

    person_cases = [
        ({"user": {}}, "not a user synced"),
        ({"user": {"user_id": "UX", "is_deleted": 1}}, "deactivated"),
        ({"user": {"user_id": "UX", "is_bot": 1}}, "bot"),
    ]
    for warehouse_kwargs, expected in person_cases:
        api = _SlackAPI({"auth.test": _ok_auth()})
        result = SlackMutationExecutor(
            warehouse=_SendWarehouse(session=_session(), **warehouse_kwargs), slack_post=api
        ).execute(_send(conversation_id="", user_id="UX"))
        assert result.status == "failed_terminal", expected
        assert expected in result.error

    live_archived = _SlackAPI(
        {"auth.test": _ok_auth(), "conversations.info": {"ok": True, "channel": {"id": "C1", "is_archived": True}}}
    )
    result = SlackMutationExecutor(
        warehouse=_SendWarehouse(session=_session(), conversation=_channel()), slack_post=live_archived
    ).execute(_send())
    assert result.status == "failed_terminal"
    assert "archived" in result.error


def test_send_payload_validation_is_terminal_without_a_slack_call() -> None:
    api = _SlackAPI({})
    executor = SlackMutationExecutor(warehouse=_SendWarehouse(session=_session()), slack_post=api)
    cases = [
        (_send(conversation_id="", user_id=""), "names a conversation_id or a user_id"),
        (_send(user_id="U1"), "not both"),
        (_send(conversation_id="nope"), "conversation_id"),
        (_send(conversation_id="", user_id="C1"), "user_id"),
        (_send(text="   "), "text must not be blank"),
        (_send(text="x" * 4001), "longer than"),
        (_send(thread_ts="yesterday"), "thread_ts"),
        (_send(conversation_id="", user_id="U1", thread_ts="1.1"), "needs conversation_id"),
        (_send(mutation_id=""), "mutation id"),
    ]
    for mutation, expected in cases:
        result = executor.execute(mutation)
        assert result.status == "failed_terminal", expected
        assert expected in result.error
    assert api.methods == []


def test_send_errors_from_slack_are_classified_like_the_read_cursor() -> None:
    for error, expected_status in {
        "not_in_channel": "failed_terminal",
        "msg_too_long": "failed_terminal",
        "ratelimited": "failed_retryable",
        "http_502": "failed_retryable",
        "token_revoked": "blocked_missing_credentials",
    }.items():
        api = _SlackAPI(
            {
                "auth.test": _ok_auth(),
                "conversations.info": {"ok": True, "channel": {"id": "C1"}},
                "conversations.history": {"ok": True, "messages": []},
                "chat.postMessage": {"ok": False, "error": error},
            }
        )
        result = SlackMutationExecutor(
            warehouse=_SendWarehouse(session=_session(), conversation=_channel()), slack_post=api
        ).execute(_send())
        assert result.status == expected_status, error
        assert error in result.error


def test_send_identity_mismatch_is_blocked_before_any_lookup() -> None:
    api = _SlackAPI({"auth.test": {"ok": True, "user_id": "U-other", "team_id": "T1"}})
    warehouse = _SendWarehouse(session=_session(), conversation=_channel())
    result = SlackMutationExecutor(warehouse=warehouse, slack_post=api).execute(_send())
    assert result.status == "blocked_missing_credentials"
    assert warehouse.lookups == []
    assert api.methods == ["auth.test"]


def test_send_with_no_approval_time_checks_a_week_back() -> None:
    api = _SlackAPI(
        {
            "auth.test": _ok_auth(),
            "conversations.info": {"ok": True, "channel": {"id": "C1"}},
            "conversations.history": {"ok": True, "messages": []},
            "chat.postMessage": {"ok": True, "ts": "1.0"},
        }
    )
    now = datetime(2026, 9, 24, 16, 0, tzinfo=UTC)
    mutation = _send()
    mutation["approved_at"] = datetime(1970, 1, 1, tzinfo=UTC)
    SlackMutationExecutor(
        warehouse=_SendWarehouse(session=_session(), conversation=_channel()), slack_post=api, now=lambda: now
    ).execute(mutation)
    assert api.form("conversations.history")["oldest"] == f"{(now - timedelta(days=7, minutes=5)).timestamp():.6f}"
