from __future__ import annotations

from dagster import Definitions, MaterializeResult, MetadataValue, RetryPolicy, asset, definitions

from personal_data_warehouse.clickhouse import ClickHouseWarehouse
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.slack_sync import SlackSyncRunner


@asset(
    group_name="slack",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def slack_workspace_sync(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_slack=True)
    warehouse = ClickHouseWarehouse(settings.clickhouse_url or "")
    summaries = SlackSyncRunner(settings=settings, warehouse=warehouse, logger=context.log).sync_all()

    return MaterializeResult(
        metadata={
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


@definitions
def defs() -> Definitions:
    return Definitions(assets=[slack_workspace_sync])
