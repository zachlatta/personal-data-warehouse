"""Poll the consumer ChatGPT backend API from inside the Dagster deployment.

Unlike the CLI agent-session sources (Claude Code / Codex / OpenClaw), which a
local uploader tails, ChatGPT's desktop app stores its conversations encrypted
under an entitlement-locked keychain key we cannot read. So the warehouse polls
ChatGPT's backend API server-side using a web session credential captured by the
local ``pdw chatgpt publish-session`` helper and stored in ``chatgpt_sessions``.

Fail-loud by design: if the stored session is rejected (expired / logged out),
the asset raises ``ChatGPTAuthError`` and the run goes red with instructions to
re-publish, rather than silently ingesting nothing. Before a session is ever
published the sensor simply skips with the same instruction (nothing to heal
yet), so a misconfiguration never becomes a flood of failing runs.
"""

from __future__ import annotations

import time

from dagster import (
    DefaultSensorStatus,
    Definitions,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    RunRequest,
    SkipReason,
    asset,
    define_asset_job,
    definitions,
    sensor,
)

from personal_data_warehouse.chatgpt_backend import (
    ChatGPTAuthError,
    ChatGPTBackendClient,
)
from personal_data_warehouse.chatgpt_backend_ingest import (
    ChatGPTBackendIngestRunner,
    chatgpt_poll_stall_reason,
)
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

CHATGPT_BACKEND_POSTGRES_LOCK_ID = 8_407_112_461
CHATGPT_SENSOR_TICK_SECONDS = 60

_REPUBLISH_HINT = (
    "Run `pdw chatgpt publish-session` on a machine logged into chatgpt.com to refresh it."
)


@asset(
    group_name="chatgpt",
    retry_policy=RetryPolicy(max_retries=2, delay=60),
)
def chatgpt_backend_ingest(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_chatgpt=True)
    if settings.chatgpt is None:
        raise RuntimeError("ChatGPT sync is not configured")
    config = settings.chatgpt

    with exclusive_sync_lock(
        name="chatgpt_backend_ingest",
        postgres_lock_id=CHATGPT_BACKEND_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping ChatGPT ingest because another run is already active")
            return MaterializeResult(metadata={"skipped": MetadataValue.text("another run active")})

        warehouse = warehouse_from_settings(settings)
        try:
            session_row = warehouse.get_chatgpt_session(
                account=config.account, session_key=config.session_key
            )
            credential = str((session_row or {}).get("session_token") or "")
            if not credential:
                raise ChatGPTAuthError(
                    f"No ChatGPT session published for {config.account!r}. {_REPUBLISH_HINT}"
                )

            import requests

            client = ChatGPTBackendClient(
                session_credential=credential,
                session=requests.Session(),
                base_url=config.base_url,
                timeout=config.request_timeout_seconds,
            )
            try:
                summary = ChatGPTBackendIngestRunner(
                    warehouse=warehouse,
                    client=client,
                    account=config.account,
                    page_size=config.page_size,
                    max_conversations_per_run=config.max_conversations_per_run,
                    logger=context.log,
                ).sync()
            except ChatGPTAuthError as exc:
                # Surface a loud, actionable failure instead of a silent skip.
                raise ChatGPTAuthError(f"{exc} {_REPUBLISH_HINT}") from exc
        finally:
            warehouse.close()

    # A poll that is rate limited, writes nothing, and never confirms it is caught up
    # looks identical to a healthy idle poll in the run status, so a persistent throttle
    # silently ingests nothing while reporting success (the multi-day 06-20 freeze). Fail
    # loudly on that exact shape so it surfaces in run monitoring; a poll that made
    # progress or confirmed it was caught up stays green and self-heals on the next tick.
    stall_reason = chatgpt_poll_stall_reason(
        summary, max_conversations_per_run=config.max_conversations_per_run
    )
    if stall_reason is not None:
        raise RuntimeError(stall_reason)

    return MaterializeResult(
        metadata={
            "conversations_seen": MetadataValue.int(summary.conversations_seen),
            "conversations_fetched": MetadataValue.int(summary.conversations_fetched),
            "events_written": MetadataValue.int(summary.events_written),
            "reached_run_limit": MetadataValue.bool(summary.reached_run_limit),
            "rate_limited": MetadataValue.bool(summary.rate_limited),
            "stopped_at_high_water": MetadataValue.bool(summary.stopped_at_high_water),
        }
    )


chatgpt_backend_ingest_job = define_asset_job(
    "chatgpt_backend_ingest_job",
    selection=[chatgpt_backend_ingest],
)


@sensor(
    job=chatgpt_backend_ingest_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=CHATGPT_SENSOR_TICK_SECONDS,
)
def chatgpt_backend_ingest_sensor(context):
    active = skip_if_job_in_progress(context, job_name="chatgpt_backend_ingest_job")
    if isinstance(active, SkipReason):
        return active

    try:
        settings = load_settings(require_gmail=False, require_chatgpt=True)
    except ValueError as exc:
        return SkipReason(f"ChatGPT is not configured: {exc}")
    if settings.chatgpt is None or not settings.chatgpt.client_enabled:
        return SkipReason(
            "ChatGPT sync is disabled by CHATGPT_CLIENT_ENABLED=0; unset it or set "
            "CHATGPT_CLIENT_ENABLED=1 to start it."
        )

    config = settings.chatgpt
    # Skip (don't fail) until a session has been published; there is nothing to
    # heal before first-time setup.
    warehouse = warehouse_from_settings(settings)
    try:
        session_row = warehouse.get_chatgpt_session(
            account=config.account, session_key=config.session_key
        )
    finally:
        warehouse.close()
    if not session_row or not str(session_row.get("session_token") or ""):
        return SkipReason(f"No ChatGPT session published yet. {_REPUBLISH_HINT}")

    # Honor the configured poll interval on top of the 60s sensor tick.
    last_run = float(context.cursor) if context.cursor else 0.0
    now = time.time()
    if now - last_run < config.poll_interval_seconds:
        return SkipReason(
            f"Waiting for poll interval ({config.poll_interval_seconds}s) since last ChatGPT poll."
        )
    context.update_cursor(str(now))
    return RunRequest(tags={"chatgpt_trigger": "poll"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[chatgpt_backend_ingest],
        jobs=[chatgpt_backend_ingest_job],
        sensors=[chatgpt_backend_ingest_sensor],
    )
