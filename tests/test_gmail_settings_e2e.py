"""Gmail filters and labels end to end: sync -> real Go proposal/review -> Python worker -> re-sync.

Only Gmail is substituted (an in-memory fake). The isolated database is removed
even on failure. No real mailbox, credentials or production data is used.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
import os
from pathlib import Path
import subprocess

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import make_dsn

from personal_data_warehouse.defs import upstream_mutations as upstream_mutation_defs
from personal_data_warehouse.gmail_mutations import GmailMutationExecutor
from personal_data_warehouse.gmail_settings_sync import GmailSettingsSyncRunner
from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.schema import MESSAGE_COLUMNS
from tests.conftest import make_test_schema
from tests.test_gmail_settings_mutations import SYSTEM, FakeGmail

ACCOUNT = "gmail-settings@example.test"


@contextmanager
def _isolated_warehouse() -> Iterator[tuple[PostgresWarehouse, str]]:
    admin = psycopg2.connect(os.environ["POSTGRES_DATABASE_URL"])
    admin.autocommit = True
    database = make_test_schema("gsettings")
    warehouse = None
    with admin.cursor() as cursor:
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    try:
        url = make_dsn(os.environ["POSTGRES_DATABASE_URL"], dbname=database)
        warehouse = PostgresWarehouse(url)
        warehouse.ensure_upstream_mutation_tables()
        warehouse._ensure_table_group(
            ["gmail_messages", "gmail_attachments", "gmail_labels", "gmail_filters", "gmail_settings_sync_state"]
        )
        yield warehouse, url
    finally:
        if warehouse is not None:
            warehouse.close()
        with admin.cursor() as cursor:
            cursor.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))
        admin.close()


def _message(message_id: str, thread_id: str, *, sender: str, name: str, subject: str, at: datetime, labels: list[str]) -> dict:
    row = {column: "" for column in MESSAGE_COLUMNS}
    row.update(
        account=ACCOUNT, message_id=message_id, thread_id=thread_id, history_id=1, internal_date=at,
        label_ids=labels, is_deleted=0, subject=subject, from_address=sender, to_addresses=[ACCOUNT],
        cc_addresses=[], bcc_addresses=[], size_estimate=1000, synced_at=at, sync_version=1,
        payload_json='{"payload":{"headers":[{"name":"From","value":"%s <%s>"}]}}' % (name, sender),
    )
    return row


class _Logger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass


def _run_go_test(name: str, env: dict[str, str]) -> None:
    result = subprocess.run(
        ["go", "test", "./internal/mutations", "-run", f"^{name}$", "-count=1", "-v"],
        cwd=Path(__file__).resolve().parents[1] / "app",
        env={**os.environ, **env},
        capture_output=True, text=True, timeout=180,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert f"--- PASS: {name}" in result.stdout, result.stdout + result.stderr


def test_gmail_filter_and_label_mutations_from_proposal_to_resync():
    gmail = FakeGmail(
        labels=[
            *SYSTEM,
            {"id": "Label_7", "name": "Newsletters", "type": "user"},
            {"id": "Label_9", "name": "Old Label", "type": "user"},
        ],
        filters=[{
            "id": "ANe1Bold",
            "criteria": {"from": "news@example.com"},
            "action": {"addLabelIds": ["Label_7"], "removeLabelIds": ["INBOX"]},
        }],
    )
    now = datetime.now(tz=UTC)
    with _isolated_warehouse() as (warehouse, url):
        sync = GmailSettingsSyncRunner(accounts=[ACCOUNT], warehouse=warehouse, logger=_Logger(), service_factory=lambda _: gmail)
        sync.run()
        warehouse.insert_messages([
            _message("m1", "t1", sender="shop@example.com", name="Shop", subject="Order 1", at=now - timedelta(days=3), labels=["INBOX"]),
            _message("m2", "t1", sender="shop@example.com", name="Shop", subject="Order 1", at=now - timedelta(days=2), labels=["INBOX"]),
            _message("m3", "t2", sender="shop@example.com", name="Shop", subject="Order 2", at=now - timedelta(days=1), labels=["INBOX"]),
            _message("m4", "t3", sender="shop@example.com", name="Shop", subject="Old order", at=now - timedelta(days=200), labels=[]),
            _message("m5", "t4", sender="x@example.com", name="X", subject="Tagged", at=now - timedelta(days=5), labels=["Label_9"]),
        ])

        _run_go_test("TestGmailSettingsProposalReviewApproval", {"PDW_GMAIL_SETTINGS_TEST_URL": url})

        resynced: list[set[str]] = []
        summary = upstream_mutation_defs.process_upstream_mutation_batch(
            warehouse=warehouse,
            gmail_executor=GmailMutationExecutor(settings=None, service_factory=lambda _: gmail),  # type: ignore[arg-type]
            contact_executor=None,  # type: ignore[arg-type]
            calendar_executor=None,  # type: ignore[arg-type]
            slack_executor=None,  # type: ignore[arg-type]
            limit=50,
            claimed_by="e2e",
            on_gmail_settings_changed=lambda accounts: (resynced.append(accounts), sync.run()),
        )
        rows = warehouse._query_dicts("SELECT request_index, status, error, result_json FROM @upstream_mutations ORDER BY request_index")
        assert summary.succeeded == 5, rows
        assert resynced == [{ACCOUNT}]

        labels = {row["name"]: row for row in warehouse.gmail_labels(account=ACCOUNT)}
        assert "Receipts" in labels and "Old Label" not in labels
        filters = {row["filter_id"]: row for row in warehouse.gmail_filters(account=ACCOUNT)}
        assert "ANe1Bold" not in filters
        by_sender = {row["criteria_json"].get("from"): row for row in filters.values()}
        assert by_sender["shop@example.com"]["add_label_ids"] == [labels["Receipts"]["label_id"]]
        assert by_sender["news@example.com"]["remove_label_ids"] == ["INBOX", "UNREAD"]

        assert warehouse.observe_succeeded_gmail_settings_mutations() == 5
