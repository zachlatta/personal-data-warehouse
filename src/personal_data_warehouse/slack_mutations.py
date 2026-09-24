from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
import html
import re
from typing import Any
import uuid

from personal_data_warehouse.slack_session import _slack_post


SLACK_PROVIDER = "slack"
SLACK_MARK_CONVERSATION_READ_OPERATION = "slack.mark_conversation_read"
SLACK_SEND_MESSAGE_OPERATION = "slack.send_message"

# One approval sends one message. Every attempt at a send derives the same
# client_msg_id from the mutation id and looks for it -- in the warehouse and
# in Slack -- before posting, so a retry after a lost response cannot post the
# same words twice. The namespace is arbitrary and must never change: changing
# it would make a message sent before the change invisible to the retry that
# follows it.
SLACK_SEND_MESSAGE_CLIENT_MSG_NAMESPACE = uuid.UUID("7c1d2a6e-4b5f-4e8a-9c3d-2f1e0b9a8d77")
# How far before the approval the pre-check reads: the clock on the worker and
# on Slack disagree by seconds, not minutes.
SLACK_SEND_MESSAGE_PRECHECK_SLACK = timedelta(minutes=5)
# A mutation row with no usable approval time (a hand-seeded test row) is
# checked over a week; the cost is one bounded history read.
SLACK_SEND_MESSAGE_PRECHECK_FALLBACK_WINDOW = timedelta(days=7)
SLACK_SEND_MESSAGE_PRECHECK_LIMIT = "200"
# Slack's own advice for chat.postMessage; the Go proposer refuses longer text
# and this is the executor's twin of that check.
SLACK_SEND_MESSAGE_TEXT_MAX_CHARS = 4000

_CONVERSATION_ID = re.compile(r"^[CDG][A-Z0-9]+$")
_USER_ID = re.compile(r"^[UW][A-Z0-9]+$")
_MESSAGE_TS = re.compile(r"^[0-9]+\.[0-9]+$")
_SLACK_LINK = re.compile(r"<(https?://[^|>]+)(?:\|[^>]*)?>")
_WHITESPACE = re.compile(r"\s+")
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
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


@dataclass(frozen=True)
class _SlackSession:
    token: str
    cookie_header: str
    team_id: str
    user_id: str


class _PrecheckUnavailable(Exception):
    """The already-sent check could not run; a send without it is a gamble."""

    def __init__(self, result: SlackMutationResult) -> None:
        super().__init__(result.error)
        self.result = result


def slack_send_message_client_msg_id(mutation_id: str) -> str:
    """The idempotency key a Slack send carries, derived from the mutation id alone."""
    return str(uuid.uuid5(SLACK_SEND_MESSAGE_CLIENT_MSG_NAMESPACE, str(mutation_id or "").strip()))


class SlackMutationExecutor:
    """Execute reviewed Slack writes with a stored client session.

    A Slack client token is intentionally never accepted in the mutation payload.
    The executor loads the private xoxc + d-cookie pair, proves its user/workspace
    identity, and only then resolves the exact conversation, person or message
    already synced into the warehouse. This fences a stale Enterprise Grid session
    from writing to a sibling workspace, and it means a send goes exactly where
    the reviewer saw it going.

    Two operations: `slack.mark_conversation_read` moves a read cursor;
    `slack.send_message` posts as the session's user. A send is never reclaimed
    from a stale `executing` claim (a replay could post twice while the first
    worker is still in flight), but a `failed_retryable` send IS retried -- which
    is safe only because every attempt checks for its own client_msg_id first.
    """

    def __init__(
        self,
        *,
        warehouse,
        slack_post: Callable[..., Mapping[str, Any]] = _slack_post,
        now: Callable[[], datetime] = lambda: datetime.now(tz=UTC),
    ) -> None:
        self._warehouse = warehouse
        self._slack_post = slack_post
        self._now = now

    def execute(self, mutation: Mapping[str, Any]) -> SlackMutationResult:
        provider = mutation.get("provider")
        operation = mutation.get("operation")
        if provider == SLACK_PROVIDER and operation == SLACK_MARK_CONVERSATION_READ_OPERATION:
            return self._execute_mark_conversation_read(mutation)
        if provider == SLACK_PROVIDER and operation == SLACK_SEND_MESSAGE_OPERATION:
            return self._execute_send_message(mutation)
        return SlackMutationResult(
            status="failed_terminal",
            result_json={},
            error=f"unsupported mutation operation: {provider}.{operation}",
        )

    # --- shared: the session and its identity ---------------------------------

    def _authenticate(self, account: str, safe_result: dict[str, Any]) -> _SlackSession | SlackMutationResult:
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
        return _SlackSession(token=token, cookie_header=cookie_header, team_id=team_id, user_id=user_id)

    def _call(self, session: _SlackSession, method: str, form: Mapping[str, str] | None = None) -> dict[str, Any]:
        if form is None:
            return dict(self._slack_post(method, token=session.token, cookie_header=session.cookie_header))
        return dict(
            self._slack_post(method, token=session.token, cookie_header=session.cookie_header, form=dict(form))
        )

    # --- mark read -------------------------------------------------------------

    def _execute_mark_conversation_read(self, mutation: Mapping[str, Any]) -> SlackMutationResult:
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

        session = self._authenticate(account, safe_result)
        if isinstance(session, SlackMutationResult):
            return session
        team_id = session.team_id

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
            info = self._call(session, "conversations.info", {"channel": conversation_id, "include_num_members": "false"})
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
            marked = self._call(session, "conversations.mark", {"channel": conversation_id, "ts": message_ts})
        except Exception as exc:
            return _retryable(safe_result, f"Slack conversations.mark request failed: {type(exc).__name__}")
        if not marked.get("ok"):
            return _api_failure(safe_result, "conversations.mark", marked)
        return SlackMutationResult(
            status="succeeded",
            result_json={**safe_result, "already_read": False},
        )

    # --- send message ------------------------------------------------------------

    def _execute_send_message(self, mutation: Mapping[str, Any]) -> SlackMutationResult:
        account = str(mutation.get("account") or "").strip().lower()
        payload = _mapping(mutation.get("payload_json"))
        conversation_id = str(payload.get("conversation_id") or "").strip()
        user_id = str(payload.get("user_id") or "").strip()
        text = str(payload.get("text") or "").strip()
        thread_ts = str(payload.get("thread_ts") or "").strip()
        reply_broadcast = _truthy(payload.get("reply_broadcast"))
        client_msg_id = slack_send_message_client_msg_id(str(mutation.get("id") or ""))
        safe_result: dict[str, Any] = {
            "conversation_id": conversation_id,
            "user_id": user_id,
            "thread_ts": thread_ts,
            "client_msg_id": client_msg_id,
        }
        if not account:
            return _terminal(safe_result, "account must not be blank")
        if not str(mutation.get("id") or "").strip():
            return _terminal(safe_result, "mutation id must not be blank; it is what makes a retry safe")
        if not conversation_id and not user_id:
            return _terminal(safe_result, "a Slack send names a conversation_id or a user_id")
        if conversation_id and user_id:
            return _terminal(safe_result, "a Slack send names conversation_id or user_id, not both")
        if conversation_id and not _CONVERSATION_ID.fullmatch(conversation_id):
            return _terminal(safe_result, "conversation_id is not a Slack C, D, or G conversation ID")
        if user_id and not _USER_ID.fullmatch(user_id):
            return _terminal(safe_result, "user_id is not a Slack U or W user ID")
        if not text:
            return _terminal(safe_result, "text must not be blank")
        if len(text) > SLACK_SEND_MESSAGE_TEXT_MAX_CHARS:
            return _terminal(safe_result, f"text is longer than {SLACK_SEND_MESSAGE_TEXT_MAX_CHARS} characters")
        if thread_ts and not _MESSAGE_TS.fullmatch(thread_ts):
            return _terminal(safe_result, "thread_ts must be an exact Slack timestamp")
        if thread_ts and not conversation_id:
            return _terminal(safe_result, "a thread reply needs conversation_id")

        session = self._authenticate(account, safe_result)
        if isinstance(session, SlackMutationResult):
            return session
        team_id = session.team_id

        expected_dm_user = ""
        if user_id:
            person = self._warehouse.load_slack_user_target(account=account, team_id=team_id, user_id=user_id)
            if not person:
                return _terminal(safe_result, "Slack recipient is not a user synced in this account/workspace")
            if _truthy(person.get("is_deleted")):
                return _terminal(safe_result, "Slack recipient is deactivated")
            if _truthy(person.get("is_bot")):
                return _terminal(safe_result, "Slack recipient is a bot user")
            expected_dm_user = user_id
            dm = self._warehouse.load_slack_dm_conversation(account=account, team_id=team_id, user_id=user_id)
            if dm and str(dm.get("conversation_id") or "").strip():
                conversation_id = str(dm.get("conversation_id") or "").strip()
                safe_result["dm_source"] = "warehouse"
            else:
                # Opening a DM is idempotent (Slack returns the existing one) and
                # posts nothing; it is the one write before the message itself.
                try:
                    opened = self._call(session, "conversations.open", {"users": user_id, "return_im": "true"})
                except Exception as exc:
                    return _retryable(safe_result, f"Slack conversations.open request failed: {type(exc).__name__}")
                if not opened.get("ok"):
                    return _api_failure(safe_result, "conversations.open", opened)
                conversation_id = str(_mapping(opened.get("channel")).get("id") or "").strip()
                if not conversation_id.startswith("D"):
                    return _terminal(safe_result, "Slack conversations.open did not return a direct message")
                safe_result["dm_source"] = "conversations.open"
            safe_result["conversation_id"] = conversation_id
        else:
            target = self._warehouse.load_slack_conversation_target(
                account=account, team_id=team_id, conversation_id=conversation_id
            )
            if not target:
                return _terminal(safe_result, "Slack conversation is not synced in this account/workspace")
            if str(target.get("team_id") or "") != team_id:
                return _terminal(safe_result, "Slack conversation belongs to a different workspace")
            if _truthy(target.get("is_archived")):
                return _terminal(safe_result, "Slack conversation is archived")
            if not any(_truthy(target.get(field)) for field in ("is_member", "is_im", "is_mpim")):
                return _terminal(safe_result, "Slack conversation is not one this user can post in")

        try:
            info = self._call(session, "conversations.info", {"channel": conversation_id, "include_num_members": "false"})
        except Exception as exc:
            return _retryable(safe_result, f"Slack conversations.info request failed: {type(exc).__name__}")
        if not info.get("ok"):
            return _api_failure(safe_result, "conversations.info", info)
        channel = _mapping(info.get("channel"))
        if str(channel.get("id") or "") != conversation_id:
            return _terminal(safe_result, "Slack conversations.info returned a different conversation")
        if _truthy(channel.get("is_archived")):
            return _terminal(safe_result, "Slack conversation is archived")
        if expected_dm_user:
            if not _truthy(channel.get("is_im")) or str(channel.get("user") or "") != expected_dm_user:
                return _terminal(safe_result, "Slack direct message does not belong to the proposed recipient")

        if thread_ts:
            parent = self._warehouse.load_slack_message_target(
                account=account, team_id=team_id, conversation_id=conversation_id, message_ts=thread_ts
            )
            if not parent:
                return _terminal(safe_result, "Slack thread parent is not a synced message in this conversation")
            parent_thread_ts = str(parent.get("thread_ts") or "").strip()
            if parent_thread_ts and _MESSAGE_TS.fullmatch(parent_thread_ts) and parent_thread_ts != thread_ts:
                # The proposal named a reply; Slack threads hang off the parent.
                safe_result["thread_ts_requested"] = thread_ts
                thread_ts = parent_thread_ts
                safe_result["thread_ts"] = thread_ts

        try:
            already = self._find_already_sent(
                session,
                account=account,
                conversation_id=conversation_id,
                thread_ts=thread_ts,
                client_msg_id=client_msg_id,
                text=text,
                oldest=self._precheck_oldest(mutation),
            )
        except _PrecheckUnavailable as unavailable:
            return SlackMutationResult(
                status=unavailable.result.status,
                result_json={**safe_result, **unavailable.result.result_json},
                error=unavailable.result.error,
            )
        if already:
            return SlackMutationResult(
                status="succeeded",
                result_json={**safe_result, **already, "already_sent": True},
            )

        form: dict[str, str] = {"channel": conversation_id, "text": text, "client_msg_id": client_msg_id}
        if thread_ts:
            form["thread_ts"] = thread_ts
            if reply_broadcast:
                form["reply_broadcast"] = "true"
        try:
            posted = self._call(session, "chat.postMessage", form)
        except Exception as exc:
            return _retryable(
                safe_result,
                f"Slack chat.postMessage request failed: {type(exc).__name__}; "
                "the next attempt looks for the message before sending again",
            )
        if not posted.get("ok"):
            return _api_failure(safe_result, "chat.postMessage", posted)
        message_ts = str(posted.get("ts") or _mapping(posted.get("message")).get("ts") or "").strip()
        return SlackMutationResult(
            status="succeeded",
            result_json={**safe_result, "message_ts": message_ts, "already_sent": False},
        )

    def _precheck_oldest(self, mutation: Mapping[str, Any]) -> str:
        approved_at = mutation.get("approved_at")
        if not isinstance(approved_at, datetime) or approved_at <= _EPOCH:
            approved_at = mutation.get("created_at")
        if not isinstance(approved_at, datetime) or approved_at <= _EPOCH:
            approved_at = self._now() - SLACK_SEND_MESSAGE_PRECHECK_FALLBACK_WINDOW
        if approved_at.tzinfo is None:
            approved_at = approved_at.replace(tzinfo=UTC)
        oldest = approved_at - SLACK_SEND_MESSAGE_PRECHECK_SLACK
        return f"{max(oldest.timestamp(), 0.0):.6f}"

    def _find_already_sent(
        self,
        session: _SlackSession,
        *,
        account: str,
        conversation_id: str,
        thread_ts: str,
        client_msg_id: str,
        text: str,
        oldest: str,
    ) -> dict[str, Any] | None:
        """The message this mutation already posted, if any attempt before us did.

        The warehouse is asked first (free, and the sync lands a DM within
        minutes), then Slack itself, because the attempt that timed out may have
        landed seconds ago. A pre-check that cannot run raises: posting without
        it is exactly the duplicate this exists to prevent.
        """
        stored = self._warehouse.find_slack_message_by_client_msg_id(
            account=account, team_id=session.team_id, conversation_id=conversation_id, client_msg_id=client_msg_id
        )
        if stored and str(stored.get("message_ts") or "").strip():
            return {"message_ts": str(stored.get("message_ts")).strip(), "matched_by": "warehouse_client_msg_id"}

        if thread_ts:
            method = "conversations.replies"
            form = {"channel": conversation_id, "ts": thread_ts, "oldest": oldest, "limit": SLACK_SEND_MESSAGE_PRECHECK_LIMIT, "inclusive": "true"}
        else:
            method = "conversations.history"
            form = {"channel": conversation_id, "oldest": oldest, "limit": SLACK_SEND_MESSAGE_PRECHECK_LIMIT, "inclusive": "true"}
        try:
            response = self._call(session, method, form)
        except Exception as exc:
            raise _PrecheckUnavailable(
                _retryable({}, f"Slack {method} request failed: {type(exc).__name__}; refusing to send unchecked")
            ) from exc
        if not response.get("ok"):
            raise _PrecheckUnavailable(_api_failure({}, method, response))
        messages = [_mapping(item) for item in (response.get("messages") or []) if isinstance(item, Mapping)]
        for message in messages:
            if str(message.get("client_msg_id") or "") == client_msg_id and str(message.get("ts") or ""):
                return {"message_ts": str(message.get("ts")), "matched_by": "client_msg_id"}
        wanted = _comparable_text(text)
        for message in messages:
            if (
                str(message.get("user") or "") == session.user_id
                and str(message.get("ts") or "")
                and _comparable_text(str(message.get("text") or "")) == wanted
            ):
                return {"message_ts": str(message.get("ts")), "matched_by": "text"}
        return None


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)


def _comparable_text(value: str) -> str:
    """Slack rewrites a posted message (entity escapes, <url|label> links); undo enough to compare."""
    unwrapped = _SLACK_LINK.sub(r"\1", value)
    return _WHITESPACE.sub(" ", html.unescape(unwrapped)).strip()


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
