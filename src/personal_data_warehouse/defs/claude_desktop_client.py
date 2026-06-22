"""Serverside Claude Desktop (claude.ai) conversation poller.

The Claude Desktop app keeps no conversation transcripts on disk, so unlike the
agent-sessions sources there is nothing for a device to tail. Instead a
clientside ``pdw ingest claude-desktop`` pusher ships the claude.ai session
credential to the app (stored in Postgres); this asset reads that credential and
polls the claude.ai API - the ``sessionKey`` alone authenticates, so it works
from prod's IP. It fetches conversations changed since the per-conversation
Postgres cursor and ships them through the SAME ingest path as the agent-sessions
sources (``/ingest/agent-sessions/batch`` -> ``agent_sessions_drive_ingest`` ->
``agent_session_events`` under ``source='claude_desktop'``).

Polling is plain request/response (no persistent connection), so this runs as a
short job on a keepalive cadence - mirroring the uploader cadence - rather than
the bounded-window loop the WhatsApp linked-device client needs.
"""

from __future__ import annotations

from dagster import (
    DefaultSensorStatus,
    Definitions,
    MaterializeResult,
    MetadataValue,
    RunRequest,
    SkipReason,
    asset,
    define_asset_job,
    definitions,
    sensor,
)

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.ingest_client import (
    ingest_client_from_env,
    ingest_upload_config_problem,
)
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

CLAUDE_DESKTOP_POSTGRES_LOCK_ID = 8_407_112_444
CLAUDE_DESKTOP_SENSOR_INTERVAL_SECONDS = 300


@asset(group_name="claude_desktop")
def claude_desktop_client(context) -> MaterializeResult:
    from datetime import UTC

    from personal_data_warehouse_claude_desktop.api import ClaudeAiClient
    from personal_data_warehouse_claude_desktop.state import WarehouseSyncState
    from personal_data_warehouse_claude_desktop.sync import ClaudeDesktopUploadRunner

    settings = load_settings(require_gmail=False, require_claude_desktop=True)
    if settings.claude_desktop is None:
        raise RuntimeError("Claude Desktop sync is not configured")
    config = settings.claude_desktop

    summary = None
    with exclusive_sync_lock(
        name="claude_desktop_client",
        postgres_lock_id=CLAUDE_DESKTOP_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping Claude Desktop poll because another run is already active")
        else:
            warehouse = warehouse_from_settings(settings)
            try:
                warehouse.ensure_claude_desktop_tables()
                credential = warehouse.read_claude_desktop_credential(account=config.account)
                if not credential or not str(credential.get("session_key", "")).strip():
                    context.log.warning(
                        "No Claude Desktop credential stored for %s yet; run "
                        "`pdw ingest claude-desktop` on the Mac with the desktop app to push one.",
                        config.account,
                    )
                else:
                    from datetime import datetime

                    expires_at = credential.get("expires_at")
                    if expires_at is not None and expires_at < datetime.now(tz=UTC):
                        context.log.warning(
                            "Stored Claude Desktop credential for %s expired at %s; attempting anyway. "
                            "Re-run `pdw ingest claude-desktop` on the Mac to refresh it.",
                            config.account,
                            expires_at,
                        )
                    org_id = config.org_id or str(credential.get("org_id", ""))
                    client = ClaudeAiClient(
                        cookie_header=f"sessionKey={str(credential['session_key']).strip()}",
                        org_id=org_id,
                        base_url=config.base_url,
                    )
                    runner = ClaudeDesktopUploadRunner(
                        account=config.account,
                        device=config.device,
                        client=client,
                        batch_uploader=lambda encoded, exported_at: ingest_client_from_env().upload_agent_sessions_batch(
                            encoded, exported_at=exported_at.astimezone(UTC).isoformat()
                        ),
                        logger=context.log,
                        upload_state=WarehouseSyncState(warehouse=warehouse, account=config.account),
                    )
                    summary = runner.sync()
            finally:
                warehouse.close()

    return MaterializeResult(
        metadata={
            key: MetadataValue.int(getattr(summary, key, 0) if summary else 0)
            for key in (
                "conversations_seen",
                "conversations_changed",
                "messages_uploaded",
                "batches_uploaded",
            )
        }
    )


claude_desktop_client_job = define_asset_job(
    "claude_desktop_client_job",
    selection=[claude_desktop_client],
)


@sensor(
    job=claude_desktop_client_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=CLAUDE_DESKTOP_SENSOR_INTERVAL_SECONDS,
)
def claude_desktop_client_keepalive_sensor(context):
    active = skip_if_job_in_progress(context, job_name="claude_desktop_client_job")
    if isinstance(active, SkipReason):
        return active

    try:
        settings = load_settings(require_gmail=False, require_claude_desktop=True)
    except ValueError as exc:
        return SkipReason(f"Claude Desktop is not configured: {exc}")
    if settings.claude_desktop is None or not settings.claude_desktop.enabled:
        return SkipReason(
            "Claude Desktop client is disabled by CLAUDE_DESKTOP_ENABLED=0; unset it or set "
            "CLAUDE_DESKTOP_ENABLED=1 to start it."
        )

    upload_problem = ingest_upload_config_problem()
    if upload_problem:
        return SkipReason(
            f"Claude Desktop poller cannot upload - http_app ingest is not configured: {upload_problem}"
        )

    return RunRequest(tags={"claude_desktop_trigger": "keepalive"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[claude_desktop_client],
        jobs=[claude_desktop_client_job],
        sensors=[claude_desktop_client_keepalive_sensor],
    )
