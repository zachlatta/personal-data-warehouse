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

    from personal_data_warehouse_claude_desktop.api import (
        ClaudeAiApiError,
        ClaudeAiClient,
        resolve_sync_account,
    )
    from personal_data_warehouse_claude_desktop.state import WarehouseSyncState
    from personal_data_warehouse_claude_desktop.sync import ClaudeDesktopUploadRunner

    settings = load_settings(require_gmail=False, require_claude_desktop=True)
    if settings.claude_desktop is None:
        raise RuntimeError("Claude Desktop sync is not configured")
    config = settings.claude_desktop

    summary = None
    sync_account = ""
    account_source = "none"
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
                # Read whichever credential the clientside pusher last wrote; the
                # account is resolved from the live session below, not from env.
                credential = warehouse.read_latest_claude_desktop_credential()
                if not credential or not str(credential.get("session_key", "")).strip():
                    context.log.warning(
                        "No Claude Desktop credential stored yet; run "
                        "`pdw ingest claude-desktop` on the Mac with the desktop app to push one."
                    )
                else:
                    from datetime import datetime

                    credential_label = str(credential.get("account", "")).strip()
                    expires_at = credential.get("expires_at")
                    if expires_at is not None and expires_at < datetime.now(tz=UTC):
                        context.log.warning(
                            "Stored Claude Desktop credential (%s) expired at %s; attempting anyway. "
                            "Re-run `pdw ingest claude-desktop` on the Mac to refresh it.",
                            credential_label or "unknown account",
                            expires_at,
                        )
                    org_id = config.org_id or str(credential.get("org_id", ""))
                    client = ClaudeAiClient(
                        cookie_header=f"sessionKey={str(credential['session_key']).strip()}",
                        org_id=org_id,
                        base_url=config.base_url,
                    )
                    # The account is whatever the session actually belongs to -
                    # ask the live API (authoritative). Best-effort: a verify
                    # failure (e.g. transient Cloudflare block) must never stop the
                    # sync, so we fall back to the stored label, then env.
                    try:
                        session_account = client.account_email()
                    except ClaudeAiApiError as exc:
                        session_account = ""
                        context.log.debug("Could not verify Claude Desktop account: %s", exc)
                    sync_account, account_source = resolve_sync_account(
                        session_email=session_account,
                        credential_label=credential_label,
                        configured=config.account,
                    )
                    if account_source == "session":
                        context.log.info("Claude Desktop syncing as %s (verified)", sync_account)
                    else:
                        # Couldn't confirm the account against the live session, so
                        # we're tagging under a fallback label that may be wrong -
                        # the exact failure mode that silently synced a near-empty
                        # account. Loud, but only when verification actually fails.
                        context.log.warning(
                            "Could not verify the Claude Desktop account from the live session; "
                            "tagging conversations under the unverified %s label %r. If synced "
                            "conversations look wrong, confirm the desktop app is signed into the "
                            "intended account and re-push with `pdw ingest claude-desktop`.",
                            account_source,
                            sync_account or "unknown",
                        )
                    runner = ClaudeDesktopUploadRunner(
                        account=sync_account,
                        device=config.device,
                        client=client,
                        batch_uploader=lambda encoded, exported_at: ingest_client_from_env().upload_agent_sessions_batch(
                            encoded, exported_at=exported_at.astimezone(UTC).isoformat()
                        ),
                        logger=context.log,
                        upload_state=WarehouseSyncState(warehouse=warehouse, account=sync_account),
                    )
                    summary = runner.sync()
            finally:
                warehouse.close()

    metadata = {
        key: MetadataValue.int(getattr(summary, key, 0) if summary else 0)
        for key in (
            "conversations_seen",
            "conversations_changed",
            "messages_uploaded",
            "batches_uploaded",
        )
    }
    metadata["rate_limited"] = MetadataValue.bool(
        bool(getattr(summary, "rate_limited", False)) if summary else False
    )
    metadata["account"] = MetadataValue.text(sync_account or "unknown")
    metadata["account_source"] = MetadataValue.text(account_source)
    return MaterializeResult(metadata=metadata)


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
