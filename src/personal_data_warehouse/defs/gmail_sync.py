from __future__ import annotations

from dagster import Definitions, MaterializeResult, MetadataValue, RetryPolicy, asset, definitions

from personal_data_warehouse.clickhouse import ClickHouseWarehouse
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.gmail_sync import GmailSyncRunner


@asset(
    group_name="gmail",
    retry_policy=RetryPolicy(max_retries=3, delay=30),
)
def gmail_mailbox_sync(context) -> MaterializeResult:
    settings = load_settings(require_gmail_client_secrets=False)
    warehouse = ClickHouseWarehouse(settings.clickhouse_url or "")
    summaries = GmailSyncRunner(settings=settings, warehouse=warehouse, logger=context.log).sync_all()

    return MaterializeResult(
        metadata={
            "mailboxes": MetadataValue.json(
                [
                    {
                        "account": summary.account,
                        "sync_type": summary.sync_type,
                        "next_history_id": summary.next_history_id,
                        "messages_written": summary.messages_written,
                        "deleted_messages": summary.deleted_messages,
                        "query": summary.query,
                    }
                    for summary in summaries
                ]
            ),
            "mailbox_count": len(summaries),
            "messages_written": sum(summary.messages_written for summary in summaries),
            "deleted_messages": sum(summary.deleted_messages for summary in summaries),
        }
    )


@definitions
def defs() -> Definitions:
    return Definitions(assets=[gmail_mailbox_sync])
