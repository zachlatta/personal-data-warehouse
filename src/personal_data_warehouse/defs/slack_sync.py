from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
import os

from dagster import (
    DefaultScheduleStatus,
    Definitions,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
    define_asset_job,
    definitions,
    schedule,
)

from personal_data_warehouse.build_info import build_metadata
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.slack_change_feed import SlackChangeFeed
from personal_data_warehouse.slack_sync import (
    SLACK_CONVERSATION_LIST_COMPLETE,
    SLACK_CONVERSATION_LIST_STATE_TYPE,
    SLACK_COVERAGE_STAGE_STATE_TYPE,
    SlackSyncRunner,
    SlackSyncSummary,
)
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

SLACK_SYNC_POSTGRES_LOCK_ID = 7_403_111_837
# The freshness stage gets its OWN lock, and that is the point.
#
# Every Slack stage used to serialize on one lock with a non-blocking try, so
# the stage that decides how fast a DM reaches the warehouse competed with the
# slow sweeps (coverage, threads backfill, metadata) and simply forfeited its
# turn whenever one was running. Measured 2026-08-26: freshness executed 84 of
# 225 ticks in 24h -- 63% were `skipped_due_to_lock` no-ops, every one of them
# reporting SUCCESS -- with gaps of p50 15 min, p90 30 min and max 160 min
# between real executions. DM ingest latency was p50 13.4 min but p95 10.3
# DAYS, and 13.6% of DMs arrived more than a day late.
#
# Serializing it made sense when freshness meant calling conversations.history
# on ~950 conversations against a ~39/min ceiling. It does not now: the
# client.counts change feed tells it which conversations moved, and production
# logs it fetching 11-43 of ~690 per tick. That is a small, bounded share of
# the API budget, so it no longer needs to wait behind sweeps that take
# minutes -- and the sweeps still serialize against each other on the original
# lock.
# Deliberately clear of the 7_403_111_83x-85x block: several ids in it are
# written as `<OTHER>_LOCK_ID + 1`, so the next free-looking literal is not
# free. `tests/test_sync_locks.py::test_advisory_lock_ids_are_unique` resolves
# those expressions and caught this twice while picking this number - first
# against calendar sync, then against an alice-voice derived id.
SLACK_FRESHNESS_POSTGRES_LOCK_ID = 7_403_111_920


def _rate_limit_budget_seconds() -> int:
    return _int_env("SLACK_ASSET_RATE_LIMIT_BUDGET_SECONDS", 120)


def _user_sync_lock_wait_seconds() -> int:
    # The full-workspace users.list refresh holds the shared Slack lock for a
    # long time (a large directory can take hours), so it must wait for the lock
    # to free rather than racing the high-frequency message stages and failing.
    # The wait comfortably exceeds the longest message-stage hold, so a timeout
    # here means a stage is genuinely stuck and the run should surface a failure.
    return _int_env("SLACK_USER_SYNC_LOCK_WAIT_SECONDS", 1800)


@dataclass(frozen=True)
class SlackChangePlan:
    """What Slack says has moved, or why we could not ask."""

    usable: bool
    changed_conversation_ids: tuple[str, ...] = ()
    coverage: Mapping[str, int] = field(default_factory=dict)
    reason: str = ""


def fetch_client_counts(*, token: str, cookie: str) -> Mapping[str, object]:
    """One request that reports every conversation's newest message."""
    from personal_data_warehouse.slack_session import _slack_post

    return _slack_post(
        "client.counts",
        token=token,
        cookie_header=f"d={cookie}",
        form={"thread_counts_by_channel": "true", "org_wide_aware": "true"},
    )


def slack_change_plan(*, settings, warehouse, account: str, logger) -> SlackChangePlan:
    """Ask Slack what changed, in one request, using the published session.

    Every failure here degrades to `usable=False`, which leaves the caller
    polling exactly as before. That direction is deliberate: a revoked or
    missing session must cost throughput, never coverage -- returning "nothing
    changed" would silently stop ingestion, which is a far worse outcome than
    the rate-limited polling this replaces.
    """
    try:
        session = warehouse.load_slack_session(account=account)
    except Exception as exc:  # pragma: no cover - a missing table must not break sync
        return SlackChangePlan(usable=False, reason=f"could not read the Slack session: {exc}")
    token = str(session.get("session_token") or "")
    cookie = str(session.get("session_cookie") or "")
    if not token or not cookie:
        # Both halves or nothing: an xoxc token without the `d` cookie
        # authenticates as nobody.
        return SlackChangePlan(usable=False, reason="no published Slack session (run `pdw slack publish-session`)")

    payload = fetch_client_counts(token=token, cookie=cookie)
    try:
        feed = SlackChangeFeed.from_counts(payload)
    except SlackChangeFeed.Error as exc:
        logger.warning("Slack change feed unavailable, falling back to polling: %s", exc)
        return SlackChangePlan(usable=False, reason=str(exc))

    team_id = str(session.get("team_id") or "")
    cursors = warehouse.load_slack_conversation_cursors(account=account, team_id=team_id)
    changed = feed.changed_since(cursors)
    logger.info(
        "Slack change feed: %s conversations covered, %s changed since our high-water",
        sum(feed.coverage.values()),
        len(changed),
    )
    return SlackChangePlan(
        usable=True,
        changed_conversation_ids=tuple(changed),
        coverage=feed.coverage,
    )


def run_slack_freshness_sync(*, settings, warehouse, logger) -> list[SlackSyncSummary]:
    summaries: list[SlackSyncSummary] = []

    # Ask Slack once what moved. When that works, the per-type blanket polls
    # below collapse to just those conversations; when it does not, they run
    # exactly as before, so a missing or revoked session costs throughput and
    # never coverage.
    plan = SlackChangePlan(usable=False, reason="change feed disabled")
    if _bool_env("SLACK_ASSET_USE_CHANGE_FEED", True):
        for account in settings.slack_accounts:
            plan = slack_change_plan(
                settings=settings, warehouse=warehouse, account=account.account, logger=logger
            )
            break
    changed_ids: Sequence[str] | None = None
    if plan.usable:
        changed_ids = plan.changed_conversation_ids
        if not changed_ids:
            logger.info("Slack change feed reports nothing new; skipping the freshness fetch")
            if _bool_env("SLACK_ASSET_READ_STATE_WITH_FRESHNESS", True):
                summaries.extend(run_slack_read_state_sync(settings=settings, warehouse=warehouse, logger=logger))
            return summaries
    else:
        logger.info("Slack change feed unusable (%s); polling as before", plan.reason)

    for conversation_types, window_minutes, conversation_limit in [
        (("im",), _int_env("SLACK_ASSET_DM_WINDOW_MINUTES", 240), _int_env("SLACK_ASSET_DM_FRESHNESS_LIMIT", 500)),
        (("mpim",), _int_env("SLACK_ASSET_MPIM_WINDOW_MINUTES", 240), _int_env("SLACK_ASSET_MPIM_FRESHNESS_LIMIT", 250)),
        (
            ("private_channel",),
            _int_env("SLACK_ASSET_PRIVATE_WINDOW_MINUTES", 180),
            _int_env("SLACK_ASSET_PRIVATE_FRESHNESS_LIMIT", 100),
        ),
        (
            ("public_channel",),
            _int_env("SLACK_ASSET_PUBLIC_WINDOW_MINUTES", 120),
            _int_env("SLACK_ASSET_PUBLIC_FRESHNESS_LIMIT", 100),
        ),
    ]:
        summaries.extend(
            SlackSyncRunner(
                settings=settings,
                warehouse=warehouse,
                logger=logger,
                history_window=timedelta(minutes=window_minutes),
                sync_users=False,
                sync_members=False,
                freshness_priority=True,
                use_existing_conversations=True,
                conversation_types=conversation_types,
                # A change-feed pass is bounded by what actually moved (~51 on a
                # normal day), so the per-type caps that exist to ration a
                # blanket poll would only get in the way.
                conversation_limit=(
                    _int_env("SLACK_ASSET_CHANGED_LIMIT", 500) if changed_ids is not None else conversation_limit
                ),
                conversation_ids=changed_ids,
                # Stop gracefully when the rate-limit budget is exhausted instead of
                # failing the run. The history cursor is persisted per conversation as
                # the pass proceeds, so the next freshness run resumes from there. (This
                # mirrors coverage and thread syncs, which already pass this flag; the
                # freshness stage was the only Slack job that hard-failed on a budget hit.)
                skip_known_errors=True,
                # Fetch replies inline for thread parents that land in the recent
                # window so brand-new threads are captured complete on first pass.
                # Bounded to parents within the freshness window and still capped by
                # the rate-limit budget below. Coverage (which walks multi-year
                # history) stays decoupled and leaves old replies to the backfill job.
                sync_thread_replies=True,
                max_rate_limit_sleep_seconds=_rate_limit_budget_seconds(),
            ).sync_all()
        )

    if _bool_env("SLACK_ASSET_READ_STATE_WITH_FRESHNESS", True):
        summaries.extend(run_slack_read_state_sync(settings=settings, warehouse=warehouse, logger=logger))

    return summaries


def run_slack_coverage_sync(*, settings, warehouse, logger, now: datetime | None = None) -> list[SlackSyncSummary]:
    current_time = now or datetime.now(tz=UTC)
    summaries: list[SlackSyncSummary] = []
    coverage = _coverage_stage(warehouse=warehouse, now=current_time)
    if coverage is not None:
        # Don't force a full sync: channels with a partial cursor resume from cursor
        # via _oldest_ts_for_conversation; channels with no state still get a full
        # streaming sync. Forcing full on every coverage pass restreamed large
        # multi-year channels from scratch and exhausted the rate-limit budget
        # before any progress was recorded.
        summaries.extend(
            SlackSyncRunner(
                settings=settings,
                warehouse=warehouse,
                logger=logger,
                sync_users=False,
                sync_members=False,
                use_existing_conversations=True,
                archived_only=coverage["archived_only"],
                conversation_types=coverage["conversation_types"],
                not_full_only=True,
                zero_messages_only=coverage["zero_messages_only"],
                skip_known_errors=True,
                conversation_limit=_coverage_stage_limit(coverage),
                sync_thread_replies=False,
                max_rate_limit_sleep_seconds=_rate_limit_budget_seconds(),
            ).sync_all()
        )
        # Record the run only after it happened, and only per account that
        # reported a summary: a stage whose run raised must keep its old
        # timestamp so the rotation comes straight back to it.
        _record_coverage_stage_run(
            warehouse=warehouse, stage=coverage, summaries=summaries, now=current_time
        )

    return summaries


def _record_coverage_stage_run(*, warehouse, stage, summaries, now: datetime) -> None:
    writer = getattr(warehouse, "insert_slack_sync_state", None)
    if writer is None:
        return
    scopes = {(summary.account, summary.team_id) for summary in summaries}
    for account, team_id in sorted(scopes):
        writer(
            account=account,
            team_id=team_id,
            object_type=SLACK_COVERAGE_STAGE_STATE_TYPE,
            object_id=str(stage["key"]),
            cursor_ts="",
            last_sync_type="coverage",
            status="ok",
            error="",
            updated_at=now,
            sync_version=int(now.timestamp() * 1_000_000),
        )


def run_slack_metadata_sync(
    *,
    settings,
    warehouse,
    logger,
    now: datetime | None = None,
    respect_interval: bool = False,
) -> list[SlackSyncSummary]:
    current_time = now or datetime.now(tz=UTC)
    summaries: list[SlackSyncSummary] = []
    if respect_interval and current_time.minute % _int_env("SLACK_ASSET_METADATA_EVERY_MINUTES", 15) != 0:
        return summaries

    metadata_conversation_types = _metadata_conversation_types(warehouse=warehouse, now=current_time)
    summaries.extend(
        SlackSyncRunner(
            settings=settings,
            warehouse=warehouse,
            logger=logger,
            sync_users=False,
            sync_members=False,
            conversation_types=metadata_conversation_types,
            conversation_page_limit=_int_env("SLACK_ASSET_METADATA_CONVERSATION_PAGE_LIMIT", 5),
            sync_conversations_only=True,
            sync_thread_replies=False,
            max_rate_limit_sleep_seconds=_rate_limit_budget_seconds(),
        ).sync_all()
    )

    return summaries


def run_slack_user_sync(*, settings, warehouse, logger) -> list[SlackSyncSummary]:
    return SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=logger,
        sync_users=True,
        sync_members=False,
        use_existing_conversations=True,
        conversation_limit=0,
        sync_thread_replies=False,
        max_rate_limit_sleep_seconds=_rate_limit_budget_seconds(),
    ).sync_all()


def run_slack_thread_sync(*, settings, warehouse, logger) -> list[SlackSyncSummary]:
    return SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=logger,
        sync_users=False,
        sync_members=False,
        sync_thread_replies_only=True,
        skip_completed_threads=True,
        skip_known_errors=True,
        thread_order=os.getenv("SLACK_ASSET_THREAD_ORDER", "recent"),
        # Audit replies for recent thread parents. At 1/run this could not keep up
        # with thread creation, so new replies on existing threads fell years behind;
        # 25/run stays ahead of activity while remaining inside the rate-limit budget.
        thread_limit=_int_env("SLACK_ASSET_THREAD_LIMIT", 25),
        thread_since_days=_int_env("SLACK_ASSET_THREAD_SINCE_DAYS", settings.slack_thread_audit_days),
        max_rate_limit_sleep_seconds=_rate_limit_budget_seconds(),
    ).sync_all()


def run_slack_thread_backfill_sync(*, settings, warehouse, logger) -> list[SlackSyncSummary]:
    # Walks parents that have reply_count > 0 but no reply rows in the warehouse,
    # ignoring the rolling thread_since_days window so old backlogs can actually
    # be drained.
    return SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=logger,
        sync_users=False,
        sync_members=False,
        sync_thread_replies_only=True,
        skip_completed_threads=True,
        skip_known_errors=True,
        # Newest-first. missing_replies_only means every processed thread drops
        # out of the candidate set, so ordering only decides which gap closes
        # first — and oldest-first left 35k+ threads from the last 90 days
        # (450k+ replies) unfetched in production while the walker ground
        # through years-old history. Recent threads are what timeline queries
        # actually hit; the historical tail still drains after them.
        thread_order=os.getenv("SLACK_ASSET_THREAD_BACKFILL_ORDER", "recent"),
        # Drain the backlog of parents whose replies were never fetched.
        # At 5/run this would take years; 100/run lets each pass use its full
        # rate-limit budget (it aborts gracefully when the budget is hit, so this is
        # an upper bound, not a guarantee of 100 replies per run).
        thread_limit=_int_env("SLACK_ASSET_THREAD_BACKFILL_LIMIT", 100),
        thread_missing_replies_only=True,
        max_rate_limit_sleep_seconds=_rate_limit_budget_seconds(),
    ).sync_all()


def run_slack_read_state_sync(*, settings, warehouse, logger) -> list[SlackSyncSummary]:
    return SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=logger,
        sync_users=False,
        sync_members=False,
        sync_conversation_info_only=True,
        conversation_limit=_int_env("SLACK_ASSET_READ_STATE_LIMIT", 25),
        max_rate_limit_sleep_seconds=_rate_limit_budget_seconds(),
    ).sync_all()


def run_slack_member_sync(*, settings, warehouse, logger) -> list[SlackSyncSummary]:
    return SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=logger,
        sync_users=False,
        sync_members=False,
        sync_members_only=True,
        use_existing_conversations=True,
        conversation_types=("private_channel",),
        conversation_limit=_int_env("SLACK_ASSET_MEMBER_SYNC_LIMIT", 50),
        sync_thread_replies=False,
        max_rate_limit_sleep_seconds=_rate_limit_budget_seconds(),
    ).sync_all()


METADATA_CONVERSATION_TYPE_ORDER = (
    ("im",),
    ("mpim",),
    ("private_channel",),
    ("public_channel",),
)


def _metadata_conversation_types_for_time(now: datetime) -> tuple[str, ...]:
    stage = ((now.hour * 60) + now.minute) // _int_env("SLACK_ASSET_METADATA_EVERY_MINUTES", 15)
    return METADATA_CONVERSATION_TYPE_ORDER[stage % 4]


def _metadata_conversation_types(*, warehouse, now: datetime) -> tuple[str, ...]:
    """Pick the conversation type whose discovery walk is furthest behind.

    A wall-clock rotation hands each type one 15-minute slot per hour and
    forfeits it whenever that particular run loses the shared Slack lock. In
    production most metadata runs lost the lock, so mpim discovery went 11.5
    hours between refreshes. Choosing from persisted state instead means a lost
    slot is simply picked up by the next metadata run, whenever it wins the lock.
    """
    states = _sync_states_of_type(warehouse, SLACK_CONVERSATION_LIST_STATE_TYPE)
    if states is None:
        return _metadata_conversation_types_for_time(now)

    def rank(conversation_types: tuple[str, ...]) -> tuple[int, float]:
        state = states.get(",".join(conversation_types))
        if state is None:
            # Never walked: nothing of this type has ever been discovered.
            return (0, 0.0)
        # A walk that has not reached the end of the list is part-way through it,
        # and the newest conversations live on its last pages — finish it before
        # rotating away. Completion is carried by `status`, not by a blank cursor.
        mid_walk = str(state.get("status") or "") != SLACK_CONVERSATION_LIST_COMPLETE
        return (0 if mid_walk else 1, _last_run_at(state))

    return min(METADATA_CONVERSATION_TYPE_ORDER, key=rank)


def _sync_states_of_type(warehouse, object_type: str) -> dict[str, Mapping[str, object]] | None:
    """Sync-state rows of one object_type, keyed by object_id.

    Prefers the scoped loader; the full-table fallback materialises 1.1M rows and
    exists only so a warehouse double in a test still works.
    """
    scoped = getattr(warehouse, "load_slack_sync_state_by_type", None)
    try:
        if scoped is not None:
            rows = scoped(object_type)
        else:
            loader = getattr(warehouse, "load_slack_sync_state", None)
            if loader is None:
                return None
            rows = loader()
    except Exception:  # pragma: no cover - a monitoring read must never break sync
        return None
    return {
        str(object_id): state
        for (_account, _team_id, row_type, object_id), state in rows.items()
        if row_type == object_type and isinstance(state, Mapping)
    }


def _last_run_at(state: Mapping[str, object] | None) -> float:
    """Seconds since epoch of a stage's last recorded run; 0.0 if never run."""
    if state is None:
        return 0.0
    updated_at = state.get("updated_at")
    return updated_at.timestamp() if isinstance(updated_at, datetime) else 0.0


def run_intelligent_slack_sync(*, settings, warehouse, logger, now: datetime | None = None) -> list[SlackSyncSummary]:
    current_time = now or datetime.now(tz=UTC)
    summaries = [
        *run_slack_freshness_sync(settings=settings, warehouse=warehouse, logger=logger),
        *run_slack_coverage_sync(settings=settings, warehouse=warehouse, logger=logger, now=current_time),
        *run_slack_metadata_sync(settings=settings, warehouse=warehouse, logger=logger, now=current_time, respect_interval=True),
    ]
    if current_time.minute == 0:
        summaries.extend(run_slack_user_sync(settings=settings, warehouse=warehouse, logger=logger))
    return summaries


# One entry per coverage stage, in the wall-clock slot order the rotation used
# before it became state-driven. `key` is what ops.slack_sync_state records, so
# renaming one resets that stage's clock — treat these as stable identifiers.
COVERAGE_STAGES: tuple[dict[str, object], ...] = (
    {
        "key": "public_channel",
        "conversation_types": ("public_channel",),
        "archived_only": False,
        "zero_messages_only": False,
        "limit_env": "SLACK_ASSET_PUBLIC_COVERAGE_LIMIT",
        "limit_default": 25,
    },
    {
        "key": "mpim",
        "conversation_types": ("mpim",),
        "archived_only": False,
        "zero_messages_only": False,
        "limit_env": "SLACK_ASSET_MPIM_COVERAGE_LIMIT",
        "limit_default": 50,
    },
    {
        "key": "private_channel",
        "conversation_types": ("private_channel",),
        "archived_only": False,
        "zero_messages_only": False,
        "limit_env": "SLACK_ASSET_PRIVATE_COVERAGE_LIMIT",
        "limit_default": 25,
    },
    {
        "key": "private_channel_archived",
        "conversation_types": ("private_channel",),
        "archived_only": True,
        "zero_messages_only": False,
        "limit_env": "SLACK_ASSET_ARCHIVED_PRIVATE_COVERAGE_LIMIT",
        "limit_default": 10,
    },
    {
        "key": "public_channel_archived_zero",
        "conversation_types": ("public_channel",),
        "archived_only": True,
        "zero_messages_only": True,
        "limit_env": "SLACK_ASSET_ARCHIVED_PUBLIC_ZERO_COVERAGE_LIMIT",
        "limit_default": 25,
    },
    {
        "key": "public_channel_archived",
        "conversation_types": ("public_channel",),
        "archived_only": True,
        "zero_messages_only": False,
        "limit_env": "SLACK_ASSET_ARCHIVED_PUBLIC_COVERAGE_LIMIT",
        "limit_default": 25,
    },
    {
        # Direct messages have no other backfill path: the freshness pass only
        # fetches IMs active within its short recent window, so a DM whose last
        # activity predates that window is otherwise never pulled.
        "key": "im",
        "conversation_types": ("im",),
        "archived_only": False,
        "zero_messages_only": False,
        "limit_env": "SLACK_ASSET_IM_COVERAGE_LIMIT",
        "limit_default": 50,
    },
)

_COVERAGE_STAGES_BY_KEY = {str(stage["key"]): stage for stage in COVERAGE_STAGES}


def _coverage_stage(*, warehouse, now: datetime) -> dict[str, object]:
    """Pick the coverage stage that has gone longest without running.

    The wall-clock rotation gave each of the seven stages one slot per 49
    minutes and forfeited it whenever that run lost the shared Slack lock. In
    production 38 of 54 coverage runs over six hours (70%) were lock-skipped
    no-ops, so unarchived public channels — two slots an hour — effectively
    drained a 1,929-channel backlog at about one channel per hour. Reading the
    stage from persisted state means a lost slot is picked up by the next run.
    """
    states = _sync_states_of_type(warehouse, SLACK_COVERAGE_STAGE_STATE_TYPE)
    if states is None:
        stage = _coverage_stage_for_time(now)
        return stage if stage is not None else dict(COVERAGE_STAGES[0])
    return min(COVERAGE_STAGES, key=lambda stage: _last_run_at(states.get(str(stage["key"]))))


def _coverage_stage_limit(stage: Mapping[str, object]) -> int:
    return _int_env(str(stage["limit_env"]), int(stage["limit_default"]))


def _coverage_stage_for_time(now: datetime) -> dict[str, object]:
    """Clock fallback, used only when the warehouse cannot report stage state.

    The coverage job fires every 7 minutes (*/7), so this rotates over the
    fire-slot index within the hour (minute // 7), not minute % N. A plain
    minute % 7 would collapse to a single stage forever because every */7
    fire-minute is congruent to 0 (mod 7); minute // 7 advances by one on each
    fire. The slot order is COVERAGE_STAGES' order, so the two rotations agree.
    """
    return COVERAGE_STAGES[((now.minute // 7) % 7)]


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value else default


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@asset(
    group_name="slack",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def slack_workspace_sync(context) -> MaterializeResult:
    return _run_locked_slack_stage(
        context,
        stage_name="freshness",
        run_fn=run_slack_freshness_sync,
        lock_name="slack-freshness",
        postgres_lock_id=SLACK_FRESHNESS_POSTGRES_LOCK_ID,
    )


@asset(
    group_name="slack",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def slack_workspace_coverage_sync(context) -> MaterializeResult:
    return _run_locked_slack_stage(
        context,
        stage_name="coverage",
        run_fn=run_slack_coverage_sync,
    )


@asset(
    group_name="slack",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def slack_workspace_metadata_sync(context) -> MaterializeResult:
    return _run_locked_slack_stage(
        context,
        stage_name="metadata",
        run_fn=run_slack_metadata_sync,
    )


@asset(
    group_name="slack",
    retry_policy=RetryPolicy(max_retries=2, delay=60),
)
def slack_workspace_user_sync(context) -> MaterializeResult:
    return _run_locked_slack_stage(
        context,
        stage_name="users",
        run_fn=run_slack_user_sync,
        fail_on_lock_contention=True,
        lock_wait_seconds=_user_sync_lock_wait_seconds(),
    )


@asset(
    group_name="slack",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def slack_workspace_thread_sync(context) -> MaterializeResult:
    return _run_locked_slack_stage(
        context,
        stage_name="threads",
        run_fn=run_slack_thread_sync,
    )


@asset(
    group_name="slack",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def slack_workspace_thread_backfill_sync(context) -> MaterializeResult:
    return _run_locked_slack_stage(
        context,
        stage_name="threads_backfill",
        run_fn=run_slack_thread_backfill_sync,
    )


@asset(
    group_name="slack",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def slack_workspace_read_state_sync(context) -> MaterializeResult:
    return _run_locked_slack_stage(
        context,
        stage_name="read_state",
        run_fn=run_slack_read_state_sync,
    )


@asset(
    group_name="slack",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def slack_workspace_member_sync(context) -> MaterializeResult:
    return _run_locked_slack_stage(
        context,
        stage_name="members",
        run_fn=run_slack_member_sync,
    )


def _run_locked_slack_stage(
    context,
    *,
    stage_name: str,
    run_fn,
    fail_on_lock_contention: bool = False,
    lock_wait_seconds: float | None = None,
    lock_name: str = "slack",
    postgres_lock_id: int = SLACK_SYNC_POSTGRES_LOCK_ID,
) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_slack=True)
    warehouse = warehouse_from_settings(settings)
    with exclusive_sync_lock(
        name=lock_name,
        postgres_lock_id=postgres_lock_id,
        wait_seconds=lock_wait_seconds,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping Slack %s sync because another Slack sync is already running", stage_name)
            if fail_on_lock_contention:
                raise RuntimeError(f"Slack {stage_name} sync could not acquire the shared Slack sync lock")
            summaries = []
        else:
            summaries = run_fn(settings=settings, warehouse=warehouse, logger=context.log)

    deployment = build_metadata()
    return MaterializeResult(
        metadata={
            "sync_stage": stage_name,
            "lock_acquired": acquired,
            "skipped_due_to_lock": not acquired,
            "git_sha": deployment["git_sha"],
            "workspaces": MetadataValue.json(
                [
                    {
                        "account": summary.account,
                        "team_id": summary.team_id,
                        "sync_type": summary.sync_type,
                        "conversations_seen": summary.conversations_seen,
                        "messages_written": summary.messages_written,
                        "users_written": summary.users_written,
                        "files_written": summary.files_written,
                    }
                    for summary in summaries
                ]
            ),
            "workspace_count": len(summaries),
            "messages_written": sum(summary.messages_written for summary in summaries),
            "files_written": sum(summary.files_written for summary in summaries),
        }
    )


slack_workspace_sync_job = define_asset_job(
    "slack_workspace_sync_job",
    selection=[slack_workspace_sync],
)

slack_workspace_coverage_sync_job = define_asset_job(
    "slack_workspace_coverage_sync_job",
    selection=[slack_workspace_coverage_sync],
)

slack_workspace_metadata_sync_job = define_asset_job(
    "slack_workspace_metadata_sync_job",
    selection=[slack_workspace_metadata_sync],
)

slack_workspace_user_sync_job = define_asset_job(
    "slack_workspace_user_sync_job",
    selection=[slack_workspace_user_sync],
)

slack_workspace_thread_sync_job = define_asset_job(
    "slack_workspace_thread_sync_job",
    selection=[slack_workspace_thread_sync],
)

slack_workspace_thread_backfill_sync_job = define_asset_job(
    "slack_workspace_thread_backfill_sync_job",
    selection=[slack_workspace_thread_backfill_sync],
)

slack_workspace_read_state_sync_job = define_asset_job(
    "slack_workspace_read_state_sync_job",
    selection=[slack_workspace_read_state_sync],
)

slack_workspace_member_sync_job = define_asset_job(
    "slack_workspace_member_sync_job",
    selection=[slack_workspace_member_sync],
)


@schedule(
    cron_schedule="*/5 * * * *",
    job=slack_workspace_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def slack_workspace_sync_every_five_minutes(context):
    return skip_if_job_active(context, job_name="slack_workspace_sync_job")


@schedule(
    cron_schedule="*/7 * * * *",
    job=slack_workspace_coverage_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def slack_workspace_coverage_sync_every_seven_minutes(context):
    return skip_if_job_active(context, job_name="slack_workspace_coverage_sync_job")


@schedule(
    cron_schedule="*/15 * * * *",
    job=slack_workspace_metadata_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def slack_workspace_metadata_sync_every_fifteen_minutes(context):
    return skip_if_job_active(context, job_name="slack_workspace_metadata_sync_job")


# Daily, not hourly: a full users.list refresh of a large workspace can take a
# couple of hours and holds the shared Slack lock the whole time, so it cannot fit
# an hourly cadence without starving message ingestion. Runs in the early-morning
# low-traffic window (08:11 UTC) to minimise the message-sync gap while it holds
# the lock.
@schedule(
    cron_schedule="11 8 * * *",
    job=slack_workspace_user_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def slack_workspace_user_sync_daily(context):
    return skip_if_job_active(context, job_name="slack_workspace_user_sync_job")


@schedule(
    cron_schedule="*/5 * * * *",
    job=slack_workspace_thread_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def slack_workspace_thread_sync_every_five_minutes(context):
    return skip_if_job_active(context, job_name="slack_workspace_thread_sync_job")


@schedule(
    cron_schedule="3-59/5 * * * *",
    job=slack_workspace_thread_backfill_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def slack_workspace_thread_backfill_sync_every_five_minutes(context):
    return skip_if_job_active(context, job_name="slack_workspace_thread_backfill_sync_job")


@schedule(
    cron_schedule="2-59/5 * * * *",
    job=slack_workspace_read_state_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def slack_workspace_read_state_sync_every_five_minutes(context):
    return skip_if_job_active(context, job_name="slack_workspace_read_state_sync_job")


@schedule(
    cron_schedule="17 * * * *",
    job=slack_workspace_member_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def slack_workspace_member_sync_hourly(context):
    return skip_if_job_active(context, job_name="slack_workspace_member_sync_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[
            slack_workspace_sync,
            slack_workspace_coverage_sync,
            slack_workspace_metadata_sync,
            slack_workspace_user_sync,
            slack_workspace_thread_sync,
            slack_workspace_thread_backfill_sync,
            slack_workspace_read_state_sync,
            slack_workspace_member_sync,
        ],
        jobs=[
            slack_workspace_sync_job,
            slack_workspace_coverage_sync_job,
            slack_workspace_metadata_sync_job,
            slack_workspace_user_sync_job,
            slack_workspace_thread_sync_job,
            slack_workspace_thread_backfill_sync_job,
            slack_workspace_read_state_sync_job,
            slack_workspace_member_sync_job,
        ],
        schedules=[
            slack_workspace_sync_every_five_minutes,
            slack_workspace_coverage_sync_every_seven_minutes,
            slack_workspace_metadata_sync_every_fifteen_minutes,
            slack_workspace_user_sync_daily,
            slack_workspace_thread_sync_every_five_minutes,
            slack_workspace_thread_backfill_sync_every_five_minutes,
            slack_workspace_read_state_sync_every_five_minutes,
            slack_workspace_member_sync_hourly,
        ],
    )
