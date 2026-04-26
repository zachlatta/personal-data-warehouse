from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import argparse
import json
import time
from typing import Any

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError, SlackRequestError

from personal_data_warehouse.config import Settings, SlackAccount, load_settings

SLACK_CONVERSATION_TYPES = "public_channel,private_channel,mpim,im"


class SlackRateLimitedError(Exception):
    def __init__(self, *, retry_after: int) -> None:
        super().__init__(f"Slack API rate limited; retry after {retry_after}s")
        self.retry_after = retry_after


class SlackRateLimitBudgetExceeded(Exception):
    pass


class SlackApiCallError(Exception):
    pass


class SlackTransientError(Exception):
    pass


@dataclass(frozen=True)
class SlackSyncSummary:
    account: str
    team_id: str
    sync_type: str
    conversations_seen: int
    messages_written: int
    users_written: int
    files_written: int


class SlackWebApiClient:
    def __init__(self, token: str) -> None:
        self._client = WebClient(token=token)

    def call(self, method: str, **params) -> dict[str, Any]:
        try:
            response = self._client.api_call(method, params=params)
        except SlackApiError as exc:
            if exc.response.status_code == 429:
                retry_after = int(exc.response.headers.get("Retry-After", "60"))
                raise SlackRateLimitedError(retry_after=retry_after) from exc
            raise SlackApiCallError(f"{method} failed: {exc.response.get('error', exc)}") from exc
        except (SlackRequestError, TimeoutError, OSError) as exc:
            raise SlackTransientError(f"{method} transient request failure: {exc}") from exc

        data = dict(response.data)
        if not data.get("ok", False):
            raise SlackApiCallError(f"{method} failed: {data.get('error', 'unknown_error')}")
        return data


class SlackSyncRunner:
    def __init__(
        self,
        *,
        settings: Settings,
        warehouse,
        logger,
        client_factory: Callable[[SlackAccount], Any] | None = None,
        now: Callable[[], datetime] | None = None,
        sleep: Callable[[int], None] | None = None,
        history_window: timedelta | None = None,
        sync_users: bool = True,
        user_page_limit: int | None = None,
        sync_members: bool = True,
        freshness_priority: bool = False,
        use_existing_conversations: bool = False,
        include_archived_conversations: bool = False,
        archived_only: bool = False,
        conversation_types: tuple[str, ...] = (),
        not_full_only: bool = False,
        zero_messages_only: bool = False,
        skip_known_errors: bool = False,
        conversation_limit: int | None = None,
        conversation_page_limit: int | None = None,
        sync_thread_replies: bool = True,
        sync_thread_replies_only: bool = False,
        sync_conversations_only: bool = False,
        skip_completed_full: bool = False,
        skip_completed_threads: bool = False,
        thread_order: str = "recent",
        thread_limit: int | None = None,
        thread_since_days: int | None = None,
        max_rate_limit_sleep_seconds: int | None = None,
    ) -> None:
        self._settings = settings
        self._warehouse = warehouse
        self._logger = logger
        self._client_factory = client_factory or (lambda account: SlackWebApiClient(account.token))
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._sleep = sleep or time.sleep
        self._history_window = history_window
        self._sync_users_enabled = sync_users
        self._user_page_limit = user_page_limit
        self._sync_members_enabled = sync_members
        self._freshness_priority = freshness_priority
        self._use_existing_conversations = use_existing_conversations
        self._include_archived_conversations = include_archived_conversations
        self._archived_only = archived_only
        self._conversation_types = conversation_types
        self._not_full_only = not_full_only
        self._zero_messages_only = zero_messages_only
        self._skip_known_errors = skip_known_errors
        self._conversation_limit = conversation_limit
        self._conversation_page_limit = conversation_page_limit
        self._sync_thread_replies = sync_thread_replies
        self._sync_thread_replies_only = sync_thread_replies_only
        self._sync_conversations_only = sync_conversations_only
        self._skip_completed_full = skip_completed_full
        self._skip_completed_threads = skip_completed_threads
        self._thread_order = thread_order
        self._thread_limit = thread_limit
        self._thread_since_days = thread_since_days
        self._max_rate_limit_sleep_seconds = max_rate_limit_sleep_seconds
        self._rate_limit_sleep_seconds = 0

    def sync_all(self) -> list[SlackSyncSummary]:
        self._warehouse.ensure_slack_tables()
        state_by_key = self._warehouse.load_slack_sync_state()
        summaries: list[SlackSyncSummary] = []
        failures: list[str] = []

        for account in self._settings.slack_accounts:
            client = self._client_factory(account)
            try:
                summaries.append(self._sync_account(account=account, client=client, state_by_key=state_by_key))
            except Exception as exc:
                failures.append(f"{account.account}: {exc}")
                continue

        if failures:
            raise RuntimeError("Slack sync failed for: " + "; ".join(failures))
        return summaries

    def validate_all(self) -> list[dict[str, object]]:
        results = []
        for account in self._settings.slack_accounts:
            client = self._client_factory(account)
            auth = self._call(client, "auth.test")
            team = self._call(client, "team.info")
            results.append(
                {
                    "account": account.account,
                    "team_id": auth.get("team_id"),
                    "team": auth.get("team"),
                    "domain": (team.get("team") or {}).get("domain") if isinstance(team.get("team"), Mapping) else None,
                }
            )
        return results

    def _sync_account(self, *, account: SlackAccount, client, state_by_key: Mapping[tuple[str, str, str, str], Any]) -> SlackSyncSummary:
        synced_at = self._now()
        sync_version = sync_version_from_datetime(synced_at)
        auth = self._call(client, "auth.test")
        team_id = str(account.team_id or auth.get("team_id", ""))
        if not team_id:
            raise RuntimeError("Slack auth.test did not return team_id")
        team_info = self._call(client, "team.info")

        self._warehouse.insert_slack_teams(
            [team_to_row(account=account.account, auth_payload=auth, team_payload=team_info.get("team", {}), synced_at=synced_at)]
        )

        if self._sync_conversations_only:
            conversations = self._refresh_active_conversations(
                account=account.account,
                team_id=team_id,
                client=client,
                synced_at=synced_at,
            )
            return SlackSyncSummary(
                account=account.account,
                team_id=team_id,
                sync_type="conversation_refresh",
                conversations_seen=len(conversations),
                messages_written=0,
                users_written=0,
                files_written=0,
            )

        if self._sync_thread_replies_only:
            return self._sync_account_thread_replies(
                account=account,
                team_id=team_id,
                client=client,
                synced_at=synced_at,
                sync_version=sync_version,
                state_by_key=state_by_key,
            )

        if self._freshness_priority:
            return self._sync_account_freshness_priority(
                account=account,
                team_id=team_id,
                client=client,
                synced_at=synced_at,
                sync_version=sync_version,
            )

        users_written = 0
        if self._sync_users_enabled:
            for page_index, users in enumerate(
                iter_cursor_pages(client, "users.list", "members", limit=self._settings.slack_page_size, call=self._call),
                start=1,
            ):
                rows = [
                    user_to_row(account=account.account, team_id=team_id, user=user, synced_at=synced_at)
                    for user in users
                    if isinstance(user, Mapping)
                ]
                self._warehouse.insert_slack_users(rows)
                users_written += len(rows)
                self._logger.info("Synced %s Slack users for %s so far", users_written, account.account)
                if self._user_page_limit is not None and page_index >= self._user_page_limit:
                    break

        conversation_pages: Iterator[list[object]]
        if self._use_existing_conversations:
            existing_conversations = self._warehouse.load_slack_conversation_payloads(
                account=account.account,
                team_id=team_id,
                include_archived=self._include_archived_conversations,
                archived_only=self._archived_only,
                conversation_types=self._conversation_types,
                not_full_only=self._not_full_only,
                zero_messages_only=self._zero_messages_only,
                skip_known_errors=self._skip_known_errors,
                limit=self._conversation_limit,
            )
            self._logger.info(
                "Loaded %s cached Slack conversations for %s",
                len(existing_conversations),
                account.account,
            )
            conversation_pages = chunked_objects(existing_conversations, self._settings.slack_page_size)
        else:
            conversation_pages = iter_cursor_pages(
                client,
                "conversations.list",
                "channels",
                limit=self._settings.slack_page_size,
                call=self._call,
                types=SLACK_CONVERSATION_TYPES,
                exclude_archived="false",
            )

        conversations_seen = 0
        messages_written = 0
        files_written = 0
        sync_type = "full"
        for conversations in conversation_pages:
            conversation_rows = [
                conversation_to_row(
                    account=account.account,
                    team_id=team_id,
                    conversation=conversation,
                    synced_at=synced_at,
                )
                for conversation in conversations
                if isinstance(conversation, Mapping)
            ]
            if not self._use_existing_conversations:
                self._warehouse.insert_slack_conversations(conversation_rows)
            conversations_seen += len(conversation_rows)
            self._logger.info("Discovered %s Slack conversations for %s so far", conversations_seen, account.account)

            for conversation in conversations:
                if not isinstance(conversation, Mapping) or not conversation.get("id"):
                    continue
                conversation_id = str(conversation["id"])
                state = state_by_key.get((account.account, team_id, "conversation", conversation_id))
                if self._skip_completed_full and self._state_is_completed_full(state):
                    continue
                if self._sync_members_enabled and self._history_window is None:
                    self._sync_members(
                        account=account.account,
                        team_id=team_id,
                        conversation_id=conversation_id,
                        client=client,
                        synced_at=synced_at,
                    )
                oldest_ts = self._oldest_ts_for_conversation(state)
                if oldest_ts is not None and not conversation_may_have_activity_since(conversation, oldest_ts):
                    continue
                if oldest_ts is not None:
                    sync_type = "partial"
                try:
                    result = self._sync_conversation_messages(
                        account=account.account,
                        team_id=team_id,
                        conversation_id=conversation_id,
                        client=client,
                        synced_at=synced_at,
                        sync_version=sync_version,
                        oldest_ts=oldest_ts,
                    )
                except SlackApiCallError as exc:
                    self._record_conversation_error(
                        account=account.account,
                        team_id=team_id,
                        conversation_id=conversation_id,
                        sync_type="partial" if oldest_ts is not None else "full",
                        error=str(exc),
                        synced_at=synced_at,
                        sync_version=sync_version,
                    )
                    self._logger.warning("Could not sync Slack conversation %s: %s", conversation_id, exc)
                    continue
                messages_written += result["messages_written"]
                files_written += result["files_written"]
            self._logger.info(
                "Synced %s Slack messages across %s conversations for %s so far",
                messages_written,
                conversations_seen,
                account.account,
            )

        return SlackSyncSummary(
            account=account.account,
            team_id=team_id,
            sync_type=sync_type,
            conversations_seen=conversations_seen,
            messages_written=messages_written,
            users_written=users_written,
            files_written=files_written,
        )

    def _record_conversation_error(
        self,
        *,
        account: str,
        team_id: str,
        conversation_id: str,
        sync_type: str,
        error: str,
        synced_at: datetime,
        sync_version: int,
    ) -> None:
        self._warehouse.insert_slack_sync_state(
            account=account,
            team_id=team_id,
            object_type="conversation",
            object_id=conversation_id,
            cursor_ts="",
            last_sync_type=sync_type,
            status="error",
            error=error,
            updated_at=synced_at,
            sync_version=sync_version,
        )

    def _sync_account_freshness_priority(
        self,
        *,
        account: SlackAccount,
        team_id: str,
        client,
        synced_at: datetime,
        sync_version: int,
    ) -> SlackSyncSummary:
        oldest_ts = self._freshness_oldest_ts()
        if self._use_existing_conversations:
            conversations = self._warehouse.load_slack_conversation_payloads(
                account=account.account,
                team_id=team_id,
                include_archived=False,
                archived_only=False,
                conversation_types=self._conversation_types,
            )
            self._logger.info("Freshness loaded %s cached active Slack conversations for %s", len(conversations), account.account)
        else:
            conversations = self._refresh_active_conversations(
                account=account.account,
                team_id=team_id,
                client=client,
                synced_at=synced_at,
            )
        priority_groups = (
            ("im",),
            ("mpim",),
            ("private_channel",),
            ("public_channel",),
        )
        messages_written = 0
        files_written = 0
        conversations_seen = 0
        for group in priority_groups:
            group_conversations = [
                conversation
                for conversation in conversations
                if isinstance(conversation, Mapping) and conversation_type(conversation) in group
            ]
            group_conversations.sort(key=conversation_activity_ts, reverse=True)
            if self._conversation_limit is not None:
                group_conversations = group_conversations[: self._conversation_limit]
            group_written = 0
            for conversation in group_conversations:
                if not conversation.get("id"):
                    continue
                if not conversation_may_have_activity_since(conversation, oldest_ts):
                    continue
                result = self._sync_conversation_messages(
                    account=account.account,
                    team_id=team_id,
                    conversation_id=str(conversation["id"]),
                    client=client,
                    synced_at=synced_at,
                    sync_version=sync_version,
                    oldest_ts=oldest_ts,
                )
                conversations_seen += 1
                messages_written += result["messages_written"]
                files_written += result["files_written"]
                group_written += result["messages_written"]
            self._logger.info(
                "Freshness synced %s Slack messages from %s %s conversations for %s",
                group_written,
                len(group_conversations),
                ",".join(group),
                account.account,
            )

        users_written = 0
        if self._sync_users_enabled:
            for page_index, users in enumerate(
                iter_cursor_pages(client, "users.list", "members", limit=self._settings.slack_page_size, call=self._call),
                start=1,
            ):
                rows = [
                    user_to_row(account=account.account, team_id=team_id, user=user, synced_at=synced_at)
                    for user in users
                    if isinstance(user, Mapping)
                ]
                self._warehouse.insert_slack_users(rows)
                users_written += len(rows)
                if self._user_page_limit is not None and page_index >= self._user_page_limit:
                    break

        return SlackSyncSummary(
            account=account.account,
            team_id=team_id,
            sync_type="freshness_priority",
            conversations_seen=conversations_seen,
            messages_written=messages_written,
            users_written=users_written,
            files_written=files_written,
        )

    def _refresh_active_conversations(
        self,
        *,
        account: str,
        team_id: str,
        client,
        synced_at: datetime,
    ) -> list[object]:
        conversations: list[object] = []
        types = ",".join(self._conversation_types) if self._conversation_types else SLACK_CONVERSATION_TYPES
        for page_index, page in enumerate(
            iter_cursor_pages(
                client,
                "conversations.list",
                "channels",
                limit=self._settings.slack_page_size,
                call=self._call,
                types=types,
                exclude_archived="true",
            ),
            start=1,
        ):
            conversations.extend(page)
            rows = [
                conversation_to_row(
                    account=account,
                    team_id=team_id,
                    conversation=conversation,
                    synced_at=synced_at,
                )
                for conversation in page
                if isinstance(conversation, Mapping)
            ]
            self._warehouse.insert_slack_conversations(rows)
            if self._conversation_page_limit is not None and page_index >= self._conversation_page_limit:
                break
        self._logger.info("Freshness discovered %s active Slack conversations for %s", len(conversations), account)
        return conversations

    def _freshness_oldest_ts(self) -> float:
        window = self._history_window or timedelta(minutes=30)
        return self._now().timestamp() - window.total_seconds()

    def _sync_account_thread_replies(
        self,
        *,
        account: SlackAccount,
        team_id: str,
        client,
        synced_at: datetime,
        sync_version: int,
        state_by_key: Mapping[tuple[str, str, str, str], Any],
    ) -> SlackSyncSummary:
        since_ts = None
        if self._thread_since_days is not None:
            since_ts = self._now().timestamp() - self._thread_since_days * 24 * 60 * 60
        thread_refs = self._warehouse.load_slack_thread_parent_refs(
            account=account.account,
            team_id=team_id,
            since_ts=since_ts,
            limit=self._thread_limit,
            skip_completed=self._skip_completed_threads,
            order=self._thread_order,
        )
        messages_written = 0
        files_written = 0
        for index, thread_ref in enumerate(thread_refs, start=1):
            conversation_id = str(thread_ref["conversation_id"])
            thread_ts = str(thread_ref["thread_ts"])
            object_id = thread_state_object_id(conversation_id=conversation_id, thread_ts=thread_ts)
            state = state_by_key.get((account.account, team_id, "thread", object_id))
            if self._skip_completed_threads and self._state_is_ok(state):
                continue
            try:
                result = self._sync_one_thread_replies(
                    account=account.account,
                    team_id=team_id,
                    conversation_id=conversation_id,
                    thread_ts=thread_ts,
                    client=client,
                    synced_at=synced_at,
                    sync_version=sync_version,
                )
            except SlackApiCallError as exc:
                self._logger.warning("Could not sync Slack thread %s in %s: %s", thread_ts, conversation_id, exc)
                self._warehouse.insert_slack_sync_state(
                    account=account.account,
                    team_id=team_id,
                    object_type="thread",
                    object_id=object_id,
                    cursor_ts=thread_ts,
                    last_sync_type="thread_replies",
                    status="error",
                    error=str(exc),
                    updated_at=synced_at,
                    sync_version=sync_version,
                )
                continue
            messages_written += result["messages_written"]
            files_written += result["files_written"]
            if index % 100 == 0:
                self._logger.info("Synced Slack replies for %s threads on %s so far", index, account.account)

        return SlackSyncSummary(
            account=account.account,
            team_id=team_id,
            sync_type="thread_replies",
            conversations_seen=len(thread_refs),
            messages_written=messages_written,
            users_written=0,
            files_written=files_written,
        )

    def _sync_one_thread_replies(
        self,
        *,
        account: str,
        team_id: str,
        conversation_id: str,
        thread_ts: str,
        client,
        synced_at: datetime,
        sync_version: int,
    ) -> dict[str, int]:
        messages_written = 0
        files_written = 0
        latest_cursor = thread_ts
        for messages in iter_cursor_pages(
            client,
            "conversations.replies",
            "messages",
            limit=self._settings.slack_page_size,
            call=self._call,
            channel=conversation_id,
            ts=thread_ts,
        ):
            rows, reaction_rows, file_rows = self._message_related_rows(
                account=account,
                team_id=team_id,
                conversation_id=conversation_id,
                messages=messages,
                synced_at=synced_at,
            )
            self._warehouse.insert_slack_messages(rows)
            self._warehouse.insert_slack_message_reactions(reaction_rows)
            self._warehouse.insert_slack_files(file_rows)
            messages_written += len(rows)
            files_written += len(file_rows)
            page_latest = max(
                (str(message.get("ts")) for message in messages if isinstance(message, Mapping) and message.get("ts")),
                default="",
            )
            if page_latest and float(page_latest) > float(latest_cursor):
                latest_cursor = page_latest

        self._warehouse.insert_slack_sync_state(
            account=account,
            team_id=team_id,
            object_type="thread",
            object_id=thread_state_object_id(conversation_id=conversation_id, thread_ts=thread_ts),
            cursor_ts=latest_cursor,
            last_sync_type="thread_replies",
            status="ok",
            error="",
            updated_at=synced_at,
            sync_version=sync_version,
        )
        return {"messages_written": messages_written, "files_written": files_written}

    def _sync_members(self, *, account: str, team_id: str, conversation_id: str, client, synced_at: datetime) -> None:
        try:
            members = list(
                iter_cursor_items(
                    client,
                    "conversations.members",
                    "members",
                    limit=self._settings.slack_page_size,
                    call=self._call,
                    channel=conversation_id,
                )
            )
        except SlackApiCallError as exc:
            self._logger.warning("Could not sync Slack members for %s: %s", conversation_id, exc)
            return

        rows = [
            conversation_member_to_row(
                account=account,
                team_id=team_id,
                conversation_id=conversation_id,
                user_id=str(user_id),
                synced_at=synced_at,
            )
            for user_id in members
        ]
        self._warehouse.insert_slack_conversation_members(rows)

    def _sync_conversation_messages(
        self,
        *,
        account: str,
        team_id: str,
        conversation_id: str,
        client,
        synced_at: datetime,
        sync_version: int,
        oldest_ts: float | None,
    ) -> dict[str, int]:
        if oldest_ts is None:
            return self._sync_full_conversation_messages_streaming(
                account=account,
                team_id=team_id,
                conversation_id=conversation_id,
                client=client,
                synced_at=synced_at,
                sync_version=sync_version,
            )

        messages = list(
            iter_cursor_items(
                client,
                "conversations.history",
                "messages",
                limit=self._settings.slack_page_size,
                call=self._call,
                channel=conversation_id,
                oldest=oldest_ts,
                inclusive="true",
            )
        )
        rows, reaction_rows, file_rows = self._message_related_rows(
            account=account,
            team_id=team_id,
            conversation_id=conversation_id,
            messages=messages,
            synced_at=synced_at,
        )

        if self._sync_thread_replies:
            thread_parent_ts = [
                str(message["ts"])
                for message in messages
                if isinstance(message, Mapping) and message.get("ts") and int(message.get("reply_count") or 0) > 0
            ]
            for parent_ts in thread_parent_ts:
                replies = list(
                    iter_cursor_items(
                        client,
                        "conversations.replies",
                        "messages",
                        limit=self._settings.slack_page_size,
                        call=self._call,
                        channel=conversation_id,
                        ts=parent_ts,
                    )
                )
                reply_rows, reply_reactions, reply_files = self._message_related_rows(
                    account=account,
                    team_id=team_id,
                    conversation_id=conversation_id,
                    messages=replies,
                    synced_at=synced_at,
                )
                rows.extend(reply_rows)
                reaction_rows.extend(reply_reactions)
                file_rows.extend(reply_files)

        if oldest_ts is not None and rows:
            latest_ts = max(float(row["message_ts"]) for row in rows)
            existing = self._warehouse.existing_slack_message_ids(
                account=account,
                team_id=team_id,
                conversation_id=conversation_id,
                oldest_ts=str(oldest_ts),
                latest_ts=f"{latest_ts:.6f}",
            )
            seen = {row["message_ts"] for row in rows}
            for missing_ts in sorted(existing - seen):
                rows.append(
                    deleted_message_row(
                        account=account,
                        team_id=team_id,
                        conversation_id=conversation_id,
                        message_ts=missing_ts,
                        synced_at=synced_at,
                    )
                )

        self._warehouse.insert_slack_messages(rows)
        self._warehouse.insert_slack_message_reactions(reaction_rows)
        self._warehouse.insert_slack_files(file_rows)
        latest_cursor = max((str(message.get("ts")) for message in messages if isinstance(message, Mapping) and message.get("ts")), default="")
        self._warehouse.insert_slack_sync_state(
            account=account,
            team_id=team_id,
            object_type="conversation",
            object_id=conversation_id,
            cursor_ts=latest_cursor,
            last_sync_type="partial" if oldest_ts is not None else "full",
            status="ok",
            error="",
            updated_at=synced_at,
            sync_version=sync_version,
        )

        return {"messages_written": len(rows), "files_written": len(file_rows)}

    def _sync_full_conversation_messages_streaming(
        self,
        *,
        account: str,
        team_id: str,
        conversation_id: str,
        client,
        synced_at: datetime,
        sync_version: int,
    ) -> dict[str, int]:
        messages_written = 0
        files_written = 0
        latest_cursor = ""
        for messages in iter_cursor_pages(
            client,
            "conversations.history",
            "messages",
            limit=self._settings.slack_page_size,
            call=self._call,
            channel=conversation_id,
            inclusive="true",
        ):
            rows, reaction_rows, file_rows = self._message_related_rows(
                account=account,
                team_id=team_id,
                conversation_id=conversation_id,
                messages=messages,
                synced_at=synced_at,
            )

            if self._sync_thread_replies:
                thread_parent_ts = [
                    str(message["ts"])
                    for message in messages
                    if isinstance(message, Mapping) and message.get("ts") and int(message.get("reply_count") or 0) > 0
                ]
                for parent_ts in thread_parent_ts:
                    replies = list(
                        iter_cursor_items(
                            client,
                            "conversations.replies",
                            "messages",
                            limit=self._settings.slack_page_size,
                            call=self._call,
                            channel=conversation_id,
                            ts=parent_ts,
                        )
                    )
                    reply_rows, reply_reactions, reply_files = self._message_related_rows(
                        account=account,
                        team_id=team_id,
                        conversation_id=conversation_id,
                        messages=replies,
                        synced_at=synced_at,
                    )
                    rows.extend(reply_rows)
                    reaction_rows.extend(reply_reactions)
                    file_rows.extend(reply_files)

            self._warehouse.insert_slack_messages(rows)
            self._warehouse.insert_slack_message_reactions(reaction_rows)
            self._warehouse.insert_slack_files(file_rows)
            messages_written += len(rows)
            files_written += len(file_rows)
            page_latest = max(
                (str(message.get("ts")) for message in messages if isinstance(message, Mapping) and message.get("ts")),
                default="",
            )
            if page_latest and (not latest_cursor or float(page_latest) > float(latest_cursor)):
                latest_cursor = page_latest

        self._warehouse.insert_slack_sync_state(
            account=account,
            team_id=team_id,
            object_type="conversation",
            object_id=conversation_id,
            cursor_ts=latest_cursor,
            last_sync_type="full",
            status="ok",
            error="",
            updated_at=synced_at,
            sync_version=sync_version,
        )

        return {"messages_written": messages_written, "files_written": files_written}

    def _message_related_rows(
        self,
        *,
        account: str,
        team_id: str,
        conversation_id: str,
        messages: list[object],
        synced_at: datetime,
    ) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
        rows = []
        reaction_rows = []
        file_rows = []
        for message in messages:
            if not isinstance(message, Mapping) or not message.get("ts"):
                continue
            row = message_to_row(
                account=account,
                team_id=team_id,
                conversation_id=conversation_id,
                message=message,
                synced_at=synced_at,
            )
            rows.append(row)
            reaction_rows.extend(
                reaction_rows_from_message(
                    account=account,
                    team_id=team_id,
                    conversation_id=conversation_id,
                    message=row,
                    source_message=message,
                    synced_at=synced_at,
                )
            )
            file_rows.extend(
                file_rows_from_message(
                    account=account,
                    team_id=team_id,
                    conversation_id=conversation_id,
                    message_ts=str(message["ts"]),
                    source_message=message,
                    synced_at=synced_at,
                )
            )
        return rows, reaction_rows, file_rows

    def _oldest_ts_for_conversation(self, state: Any) -> float | None:
        if self._history_window is not None:
            return self._now().timestamp() - self._history_window.total_seconds()
        if self._settings.slack_force_full_sync or not state:
            return None
        cursor_ts = state.get("cursor_ts") if isinstance(state, Mapping) else getattr(state, "cursor_ts", "")
        if not cursor_ts:
            return None
        return max(0.0, float(cursor_ts) - self._settings.slack_lookback_days * 24 * 60 * 60)

    def _state_is_completed_full(self, state: Any) -> bool:
        if not state:
            return False
        last_sync_type = state.get("last_sync_type") if isinstance(state, Mapping) else getattr(state, "last_sync_type", "")
        status = state.get("status") if isinstance(state, Mapping) else getattr(state, "status", "")
        return last_sync_type == "full" and status == "ok"

    def _state_is_ok(self, state: Any) -> bool:
        if not state:
            return False
        status = state.get("status") if isinstance(state, Mapping) else getattr(state, "status", "")
        return status == "ok"

    def _call(self, client, method: str, **params) -> dict[str, Any]:
        transient_attempts = 0
        while True:
            try:
                return client.call(method, **params)
            except SlackRateLimitedError as exc:
                if (
                    self._max_rate_limit_sleep_seconds is not None
                    and self._rate_limit_sleep_seconds + exc.retry_after > self._max_rate_limit_sleep_seconds
                ):
                    raise SlackRateLimitBudgetExceeded(
                        "Slack API rate limit budget exceeded "
                        f"after {self._rate_limit_sleep_seconds}s of sleeps; "
                        f"next {method} retry requested {exc.retry_after}s"
                    ) from exc
                self._rate_limit_sleep_seconds += exc.retry_after
                self._logger.warning("Slack rate limited %s for %ss", method, exc.retry_after)
                self._sleep(exc.retry_after)
            except SlackTransientError as exc:
                transient_attempts += 1
                sleep_seconds = min(60, 5 * transient_attempts)
                self._logger.warning("Slack transient failure %s; retrying in %ss: %s", method, sleep_seconds, exc)
                self._sleep(sleep_seconds)


def iter_cursor_items(
    client,
    method: str,
    item_key: str,
    *,
    limit: int,
    call: Callable[..., dict[str, Any]] | None = None,
    **params,
) -> Iterator[object]:
    cursor = ""
    call_fn = call or (lambda current_client, current_method, **current_params: current_client.call(current_method, **current_params))
    while True:
        response = call_fn(client, method, limit=limit, cursor=cursor, **params)
        yield from response.get(item_key, []) or []
        metadata = response.get("response_metadata") or {}
        cursor = str(metadata.get("next_cursor") or "")
        if not cursor:
            return


def iter_cursor_pages(
    client,
    method: str,
    item_key: str,
    *,
    limit: int,
    call: Callable[..., dict[str, Any]] | None = None,
    **params,
) -> Iterator[list[object]]:
    cursor = ""
    call_fn = call or (lambda current_client, current_method, **current_params: current_client.call(current_method, **current_params))
    while True:
        response = call_fn(client, method, limit=limit, cursor=cursor, **params)
        yield list(response.get(item_key, []) or [])
        metadata = response.get("response_metadata") or {}
        cursor = str(metadata.get("next_cursor") or "")
        if not cursor:
            return


def chunked_objects(values: list[object], size: int) -> Iterator[list[object]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


def team_to_row(*, account: str, auth_payload: Mapping[str, object], team_payload: Mapping[str, object], synced_at: datetime) -> dict[str, object]:
    team_id = str(team_payload.get("id") or auth_payload.get("team_id") or "")
    return {
        "account": account,
        "team_id": team_id,
        "team_name": str(team_payload.get("name") or auth_payload.get("team") or ""),
        "domain": str(team_payload.get("domain", "")),
        "enterprise_id": str(auth_payload.get("enterprise_id") or team_payload.get("enterprise_id") or ""),
        "raw_json": json_dumps({"auth": auth_payload, "team": team_payload}),
        "synced_at": synced_at,
        "sync_version": sync_version_from_datetime(synced_at),
    }


def user_to_row(*, account: str, team_id: str, user: Mapping[str, object], synced_at: datetime) -> dict[str, object]:
    profile = user.get("profile", {})
    if not isinstance(profile, Mapping):
        profile = {}
    return {
        "account": account,
        "team_id": team_id,
        "user_id": str(user.get("id", "")),
        "team_user_id": str(user.get("team_id") or team_id),
        "name": str(user.get("name", "")),
        "real_name": str(user.get("real_name") or profile.get("real_name") or ""),
        "display_name": str(profile.get("display_name", "")),
        "email": str(profile.get("email", "")),
        "is_bot": bool_int(user.get("is_bot")),
        "is_app_user": bool_int(user.get("is_app_user")),
        "is_deleted": bool_int(user.get("deleted")),
        "tz": str(user.get("tz", "")),
        "raw_json": json_dumps(user),
        "synced_at": synced_at,
        "sync_version": sync_version_from_datetime(synced_at),
    }


def conversation_to_row(*, account: str, team_id: str, conversation: Mapping[str, object], synced_at: datetime) -> dict[str, object]:
    return {
        "account": account,
        "team_id": team_id,
        "conversation_id": str(conversation.get("id", "")),
        "conversation_type": conversation_type(conversation),
        "name": str(conversation.get("name") or conversation.get("user") or ""),
        "is_channel": bool_int(conversation.get("is_channel")),
        "is_group": bool_int(conversation.get("is_group")),
        "is_im": bool_int(conversation.get("is_im")),
        "is_mpim": bool_int(conversation.get("is_mpim")),
        "is_private": bool_int(conversation.get("is_private")),
        "is_archived": bool_int(conversation.get("is_archived")),
        "is_member": bool_int(conversation.get("is_member")),
        "creator": str(conversation.get("creator", "")),
        "created_at": unix_to_datetime(conversation.get("created")),
        "topic": json_dumps(conversation.get("topic", {})),
        "purpose": json_dumps(conversation.get("purpose", {})),
        "num_members": int(conversation.get("num_members") or 0),
        "raw_json": json_dumps(conversation),
        "synced_at": synced_at,
        "sync_version": sync_version_from_datetime(synced_at),
    }


def conversation_member_to_row(*, account: str, team_id: str, conversation_id: str, user_id: str, synced_at: datetime) -> dict[str, object]:
    return {
        "account": account,
        "team_id": team_id,
        "conversation_id": conversation_id,
        "user_id": user_id,
        "is_deleted": 0,
        "synced_at": synced_at,
        "sync_version": sync_version_from_datetime(synced_at),
    }


def message_to_row(*, account: str, team_id: str, conversation_id: str, message: Mapping[str, object], synced_at: datetime) -> dict[str, object]:
    message_ts = str(message.get("ts", ""))
    thread_ts = str(message.get("thread_ts") or message_ts)
    return {
        "account": account,
        "team_id": team_id,
        "conversation_id": conversation_id,
        "message_ts": message_ts,
        "message_datetime": ts_to_datetime(message_ts),
        "thread_ts": thread_ts,
        "parent_message_ts": "" if thread_ts == message_ts else thread_ts,
        "user_id": str(message.get("user") or message.get("bot_id") or ""),
        "bot_id": str(message.get("bot_id", "")),
        "username": str(message.get("username", "")),
        "type": str(message.get("type", "")),
        "subtype": str(message.get("subtype", "")),
        "text": str(message.get("text", "")),
        "blocks_json": json_dumps(message.get("blocks", [])),
        "attachments_json": json_dumps(message.get("attachments", [])),
        "is_thread_parent": 1 if int(message.get("reply_count") or 0) > 0 else 0,
        "is_thread_reply": 1 if thread_ts != message_ts else 0,
        "reply_count": int(message.get("reply_count") or 0),
        "reply_users_count": int(message.get("reply_users_count") or 0),
        "latest_reply_ts": str(message.get("latest_reply", "")),
        "edited_ts": str((message.get("edited") or {}).get("ts", "")) if isinstance(message.get("edited"), Mapping) else "",
        "client_msg_id": str(message.get("client_msg_id", "")),
        "is_deleted": 0,
        "raw_json": json_dumps(message),
        "synced_at": synced_at,
        "sync_version": sync_version_from_datetime(synced_at),
    }


def deleted_message_row(*, account: str, team_id: str, conversation_id: str, message_ts: str, synced_at: datetime) -> dict[str, object]:
    return {
        "account": account,
        "team_id": team_id,
        "conversation_id": conversation_id,
        "message_ts": message_ts,
        "message_datetime": ts_to_datetime(message_ts),
        "thread_ts": message_ts,
        "parent_message_ts": "",
        "user_id": "",
        "bot_id": "",
        "username": "",
        "type": "",
        "subtype": "",
        "text": "",
        "blocks_json": "[]",
        "attachments_json": "[]",
        "is_thread_parent": 0,
        "is_thread_reply": 0,
        "reply_count": 0,
        "reply_users_count": 0,
        "latest_reply_ts": "",
        "edited_ts": "",
        "client_msg_id": "",
        "is_deleted": 1,
        "raw_json": "{}",
        "synced_at": synced_at,
        "sync_version": sync_version_from_datetime(synced_at),
    }


def reaction_rows_from_message(
    *,
    account: str,
    team_id: str,
    conversation_id: str,
    message: Mapping[str, object],
    source_message: Mapping[str, object],
    synced_at: datetime,
) -> list[dict[str, object]]:
    rows = []
    for reaction in source_message.get("reactions", []) or []:
        if not isinstance(reaction, Mapping):
            continue
        for user_id in reaction.get("users", []) or []:
            rows.append(
                {
                    "account": account,
                    "team_id": team_id,
                    "conversation_id": conversation_id,
                    "message_ts": str(message["message_ts"]),
                    "reaction_name": str(reaction.get("name", "")),
                    "user_id": str(user_id),
                    "reaction_count": int(reaction.get("count") or 0),
                    "is_deleted": 0,
                    "raw_json": json_dumps(reaction),
                    "synced_at": synced_at,
                    "sync_version": sync_version_from_datetime(synced_at),
                }
            )
    return rows


def file_rows_from_message(
    *,
    account: str,
    team_id: str,
    conversation_id: str,
    message_ts: str,
    source_message: Mapping[str, object],
    synced_at: datetime,
) -> list[dict[str, object]]:
    rows = []
    for file in source_message.get("files", []) or []:
        if not isinstance(file, Mapping):
            continue
        rows.append(
            {
                "account": account,
                "team_id": team_id,
                "file_id": str(file.get("id", "")),
                "conversation_id": conversation_id,
                "message_ts": message_ts,
                "user_id": str(file.get("user", "")),
                "created_at": unix_to_datetime(file.get("created")),
                "name": str(file.get("name", "")),
                "title": str(file.get("title", "")),
                "mimetype": str(file.get("mimetype", "")),
                "filetype": str(file.get("filetype", "")),
                "url_private": str(file.get("url_private", "")),
                "size": int(file.get("size") or 0),
                "is_deleted": bool_int(file.get("is_deleted")),
                "raw_json": json_dumps(file),
                "synced_at": synced_at,
                "sync_version": sync_version_from_datetime(synced_at),
            }
        )
    return rows


def conversation_type(conversation: Mapping[str, object]) -> str:
    if conversation.get("is_im"):
        return "im"
    if conversation.get("is_mpim"):
        return "mpim"
    if conversation.get("is_private"):
        return "private_channel"
    if conversation.get("is_channel"):
        return "public_channel"
    return "unknown"


def conversation_may_have_activity_since(conversation: Mapping[str, object], oldest_ts: float) -> bool:
    latest = conversation.get("latest")
    if isinstance(latest, Mapping) and latest.get("ts"):
        return float(str(latest["ts"])) >= oldest_ts
    updated = conversation.get("updated")
    if updated not in (None, ""):
        updated_value = float(str(updated))
        if updated_value > 10_000_000_000:
            updated_value = updated_value / 1000
        return updated_value >= oldest_ts
    return True


def conversation_activity_ts(conversation: Mapping[str, object]) -> float:
    latest = conversation.get("latest")
    if isinstance(latest, Mapping) and latest.get("ts"):
        return float(str(latest["ts"]))
    updated = conversation.get("updated")
    if updated not in (None, ""):
        updated_value = float(str(updated))
        if updated_value > 10_000_000_000:
            updated_value = updated_value / 1000
        return updated_value
    created = conversation.get("created")
    if created not in (None, ""):
        return float(str(created))
    return 0.0


def thread_state_object_id(*, conversation_id: str, thread_ts: str) -> str:
    return f"{conversation_id}:{thread_ts}"


def ts_to_datetime(value: str) -> datetime:
    if not value:
        return datetime.fromtimestamp(0, tz=UTC)
    return datetime.fromtimestamp(float(value), tz=UTC)


def unix_to_datetime(value: object) -> datetime:
    if value in (None, ""):
        return datetime.fromtimestamp(0, tz=UTC)
    return datetime.fromtimestamp(float(value), tz=UTC)


def sync_version_from_datetime(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def bool_int(value: object) -> int:
    return 1 if bool(value) else 0


def json_dumps(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def parse_duration(value: str) -> timedelta:
    value = value.strip().lower()
    units = {"m": 60, "h": 3600, "d": 86400}
    if value[-1:] in units:
        return timedelta(seconds=int(value[:-1]) * units[value[-1]])
    return timedelta(seconds=int(value))


def run_loop(*, runner_factory: Callable[[], SlackSyncRunner], duration: timedelta, interval_seconds: int) -> None:
    deadline = time.monotonic() + duration.total_seconds()
    while True:
        runner_factory().sync_all()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(interval_seconds, remaining))


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync Slack workspace data into ClickHouse.")
    parser.add_argument("--validate-only", action="store_true", help="Validate Slack credentials without syncing data")
    parser.add_argument("--loop", action="store_true", help="Run repeatedly until --duration elapses")
    parser.add_argument("--duration", default="30m", help="Loop duration like 30m, 1h, 3h, 1d")
    parser.add_argument("--interval-seconds", type=int, default=900)
    parser.add_argument("--history-window-minutes", type=int, help="Only fetch recent history windows, for smoke checks")
    parser.add_argument("--skip-users", action="store_true", help="Skip users.list during this run")
    parser.add_argument("--user-page-limit", type=int, help="Maximum users.list pages to fetch in this run")
    parser.add_argument("--skip-members", action="store_true", help="Skip conversations.members during this run")
    parser.add_argument(
        "--freshness-priority",
        action="store_true",
        help="Sync recent Slack messages in UI priority order: DMs, group DMs, private channels, public channels, then metadata",
    )
    parser.add_argument(
        "--use-existing-conversations",
        action="store_true",
        help="Use cached slack_conversations rows instead of calling conversations.list",
    )
    parser.add_argument(
        "--include-archived-conversations",
        action="store_true",
        help="Include archived cached conversations when using --use-existing-conversations",
    )
    parser.add_argument(
        "--archived-only",
        action="store_true",
        help="Only sync archived cached conversations when using --use-existing-conversations",
    )
    parser.add_argument(
        "--conversation-types",
        help="Comma-separated cached conversation types to sync, like im,mpim,private_channel,public_channel",
    )
    parser.add_argument(
        "--not-full-only",
        action="store_true",
        help="When using cached conversations, only load conversations not marked full/ok",
    )
    parser.add_argument(
        "--zero-messages-only",
        action="store_true",
        help="When using cached conversations, only load conversations with zero stored messages",
    )
    parser.add_argument(
        "--skip-known-errors",
        action="store_true",
        help="When using cached conversations, skip conversations already marked error in slack_sync_state",
    )
    parser.add_argument("--conversation-limit", type=int, help="Maximum cached conversations to process in this run")
    parser.add_argument("--conversation-page-limit", type=int, help="Maximum conversations.list pages to fetch in this run")
    parser.add_argument("--skip-thread-replies", action="store_true", help="Skip conversations.replies during this run")
    parser.add_argument(
        "--sync-thread-replies-only",
        action="store_true",
        help="Only backfill conversations.replies for known thread parent messages",
    )
    parser.add_argument(
        "--sync-conversations-only",
        action="store_true",
        help="Only refresh active Slack conversations without fetching messages",
    )
    parser.add_argument(
        "--skip-completed-threads",
        action="store_true",
        help="Skip thread parents already marked ok in slack_sync_state",
    )
    parser.add_argument("--thread-order", choices=["recent", "reply_count"], default="recent")
    parser.add_argument("--thread-limit", type=int, help="Maximum known thread parents to process")
    parser.add_argument("--thread-since-days", type=int, help="Only process known thread parents newer than this many days")
    parser.add_argument(
        "--max-rate-limit-sleep-seconds",
        type=int,
        help="Fail the run after this many cumulative Slack 429 sleep seconds",
    )
    parser.add_argument(
        "--skip-completed-full",
        action="store_true",
        help="When force-full syncing, skip conversations already marked full/ok in slack_sync_state",
    )
    args = parser.parse_args()

    from personal_data_warehouse.clickhouse import ClickHouseWarehouse

    settings = load_settings(require_gmail=False, require_slack=True)
    warehouse = ClickHouseWarehouse(settings.clickhouse_url or "")
    history_window = timedelta(minutes=args.history_window_minutes) if args.history_window_minutes else None

    class PrintLogger:
        def info(self, message, *values):
            print(message % values if values else message, flush=True)

        def warning(self, message, *values):
            print("WARNING: " + (message % values if values else message), flush=True)

    conversation_types = tuple(item.strip() for item in (args.conversation_types or "").split(",") if item.strip())

    def make_runner() -> SlackSyncRunner:
        return SlackSyncRunner(
            settings=settings,
            warehouse=warehouse,
            logger=PrintLogger(),
            history_window=history_window,
            sync_users=not args.skip_users,
            user_page_limit=args.user_page_limit,
            sync_members=not args.skip_members,
            freshness_priority=args.freshness_priority,
            use_existing_conversations=args.use_existing_conversations,
            include_archived_conversations=args.include_archived_conversations,
            archived_only=args.archived_only,
            conversation_types=conversation_types,
            not_full_only=args.not_full_only,
            zero_messages_only=args.zero_messages_only,
            skip_known_errors=args.skip_known_errors,
            conversation_limit=args.conversation_limit,
            conversation_page_limit=args.conversation_page_limit,
            sync_thread_replies=not args.skip_thread_replies,
            sync_thread_replies_only=args.sync_thread_replies_only,
            sync_conversations_only=args.sync_conversations_only,
            skip_completed_full=args.skip_completed_full,
            skip_completed_threads=args.skip_completed_threads,
            thread_order=args.thread_order,
            thread_limit=args.thread_limit,
            thread_since_days=args.thread_since_days,
            max_rate_limit_sleep_seconds=args.max_rate_limit_sleep_seconds,
        )

    if args.validate_only:
        results = make_runner().validate_all()
        for result in results:
            print(f"ok account={result['account']} team={result['team']} team_id={result['team_id']} domain={result['domain']}")
        return

    if args.loop:
        run_loop(runner_factory=make_runner, duration=parse_duration(args.duration), interval_seconds=args.interval_seconds)
        return

    summaries = make_runner().sync_all()
    for summary in summaries:
        print(
            "ok "
            f"account={summary.account} team_id={summary.team_id} sync_type={summary.sync_type} "
            f"conversations={summary.conversations_seen} messages={summary.messages_written} "
            f"users={summary.users_written} files={summary.files_written}"
        )


if __name__ == "__main__":
    main()
