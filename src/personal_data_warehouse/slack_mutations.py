from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import re
from typing import Any

from personal_data_warehouse.slack_session import _slack_post


SLACK_PROVIDER = "slack"
SLACK_MARK_CONVERSATION_READ_OPERATION = "slack.mark_conversation_read"

_CONVERSATION_ID = re.compile(r"^[CDG][A-Z0-9]+$")
_MESSAGE_TS = re.compile(r"^[0-9]+\.[0-9]+$")
_AUTH_ERRORS = {
    "account_inactive",
    "invalid_auth",
    "not_authed",
    "token_expired",
    "token_revoked",
}
_RETRYABLE_ERRORS = {
    "fatal_error",
    "internal_error",
    "ratelimited",
    "request_timeout",
    "service_unavailable",
}


@dataclass(frozen=True)
class SlackMutationResult:
    status: str
    result_json: dict[str, Any]
    error: str = ""


class SlackMutationExecutor:
    """Execute reviewed Slack read-cursor changes with a stored client session.

    A Slack client token is intentionally never accepted in the mutation payload.
    The executor loads the private xoxc + d-cookie pair, proves its user/workspace
    identity, and only then resolves an exact message already synced into the
    warehouse. This fences a stale Enterprise Grid session from writing to a
    sibling workspace.
    """

    def __init__(
        self,
        *,
        warehouse,
        slack_post: Callable[..., Mapping[str, Any]] = _slack_post,
    ) -> None:
        self._warehouse = warehouse
        self._slack_post = slack_post

    def execute(self, mutation: Mapping[str, Any]) -> SlackMutationResult:
        if (
            mutation.get("provider") != SLACK_PROVIDER
            or mutation.get("operation") != SLACK_MARK_CONVERSATION_READ_OPERATION
        ):
            return SlackMutationResult(
                status="failed_terminal",
                result_json={},
                error=f"unsupported mutation operation: {mutation.get('provider')}.{mutation.get('operation')}",
            )

        account = str(mutation.get("account") or "").strip().lower()
        payload = _mapping(mutation.get("payload_json"))
        conversation_id = str(payload.get("conversation_id") or "").strip()
        message_ts = str(payload.get("message_ts") or "").strip()
        safe_result = {"conversation_id": conversation_id, "message_ts": message_ts}
        if not account:
            return _terminal(safe_result, "account must not be blank")
        if not _CONVERSATION_ID.fullmatch(conversation_id):
            return _terminal(safe_result, "conversation_id is not a Slack C, D, or G conversation ID")
        if not _MESSAGE_TS.fullmatch(message_ts):
            return _terminal(safe_result, "message_ts must be an exact Slack timestamp")

        session = self._warehouse.load_slack_session(account=account)
        token = str(session.get("session_token") or "")
        cookie = str(session.get("session_cookie") or "")
        team_id = str(session.get("team_id") or "").strip()
        enterprise_id = str(session.get("enterprise_id") or "").strip()
        user_id = str(session.get("user_id") or "").strip()
        safe_result["team_id"] = team_id
        if not token or not cookie or not team_id or not user_id:
            return SlackMutationResult(
                status="blocked_missing_credentials",
                result_json=safe_result,
                error=(
                    f"Slack client session for account {account!r} is incomplete; "
                    "run `pdw slack publish-session` to publish xoxc + d-cookie credentials"
                ),
            )

        cookie_header = cookie if cookie.startswith("d=") else f"d={cookie}"

        try:
            auth = dict(self._slack_post("auth.test", token=token, cookie_header=cookie_header))
        except Exception as exc:
            return _retryable(safe_result, f"Slack auth.test request failed: {type(exc).__name__}")
        if not auth.get("ok"):
            return _api_failure(safe_result, "auth.test", auth)
        reported_user_id = str(auth.get("user_id") or auth.get("user") or "").strip()
        reported_team_id = str(
            auth.get("team_id") or auth.get("team") or auth.get("enterprise_id") or ""
        ).strip()
        allowed_team_ids = {value for value in (team_id, enterprise_id) if value}
        if reported_user_id != user_id or reported_team_id not in allowed_team_ids:
            return SlackMutationResult(
                status="blocked_missing_credentials",
                result_json=safe_result,
                error=(
                    "stored Slack client session identity does not match its published user/workspace; "
                    "publish a fresh session before retrying"
                ),
            )

        target = self._warehouse.load_slack_mark_read_target(
            account=account,
            team_id=team_id,
            conversation_id=conversation_id,
            message_ts=message_ts,
        )
        if not target:
            return _terminal(
                safe_result,
                "Slack mark-read target is not an exact, live message synced in this account/workspace",
            )
        if str(target.get("team_id") or "") != team_id:
            return _terminal(safe_result, "Slack mark-read target belongs to a different workspace")
        if bool(target.get("is_archived")):
            return _terminal(safe_result, "Slack mark-read target conversation is archived")
        if not any(bool(target.get(field)) for field in ("is_member", "is_im", "is_mpim")):
            return _terminal(safe_result, "Slack mark-read target is not a conversation this user can read")

        try:
            info = dict(
                self._slack_post(
                    "conversations.info",
                    token=token,
                    cookie_header=cookie_header,
                    form={"channel": conversation_id, "include_num_members": "false"},
                )
            )
        except Exception as exc:
            return _retryable(safe_result, f"Slack conversations.info request failed: {type(exc).__name__}")
        if not info.get("ok"):
            return _api_failure(safe_result, "conversations.info", info)
        channel = _mapping(info.get("channel"))
        if str(channel.get("id") or "") != conversation_id:
            return _terminal(safe_result, "Slack conversations.info returned a different conversation")
        try:
            if _slack_ts(channel.get("last_read") or "0") >= _slack_ts(message_ts):
                return SlackMutationResult(
                    status="succeeded",
                    result_json={**safe_result, "already_read": True},
                )
        except ValueError as exc:
            return _retryable(safe_result, str(exc))

        try:
            marked = dict(
                self._slack_post(
                    "conversations.mark",
                    token=token,
                    cookie_header=cookie_header,
                    form={"channel": conversation_id, "ts": message_ts},
                )
            )
        except Exception as exc:
            return _retryable(safe_result, f"Slack conversations.mark request failed: {type(exc).__name__}")
        if not marked.get("ok"):
            return _api_failure(safe_result, "conversations.mark", marked)
        return SlackMutationResult(
            status="succeeded",
            result_json={**safe_result, "already_read": False},
        )


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _slack_ts(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("Slack returned a malformed read cursor") from exc


def _terminal(result: Mapping[str, Any], error: str) -> SlackMutationResult:
    return SlackMutationResult(status="failed_terminal", result_json=dict(result), error=error)


def _retryable(result: Mapping[str, Any], error: str) -> SlackMutationResult:
    return SlackMutationResult(status="failed_retryable", result_json=dict(result), error=error)


def _api_failure(
    result: Mapping[str, Any], method: str, response: Mapping[str, Any]
) -> SlackMutationResult:
    error_code = str(response.get("error") or "unknown_error")
    error = f"Slack {method} failed: {error_code}"
    if error_code in _AUTH_ERRORS:
        status = "blocked_missing_credentials"
    elif (
        error_code in _RETRYABLE_ERRORS
        or error_code == "http_429"
        or error_code.startswith("http_5")
        or any(token in error_code.lower() for token in ("timed out", "timeout", "temporarily unavailable"))
    ):
        status = "failed_retryable"
    else:
        status = "failed_terminal"
    return SlackMutationResult(status=status, result_json=dict(result), error=error)
