"""Gmail labels and filters land in base_gmail.labels / base_gmail.filters."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import pytest
from dotenv import load_dotenv
from google.auth.exceptions import RefreshError

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.gmail_settings_sync import (
    GmailSettingsSyncRunner,
    filter_row,
    label_row,
)
from personal_data_warehouse.postgres import PostgresWarehouse


def _postgres_url() -> str:
    load_dotenv()
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if not url:
        pytest.skip("POSTGRES_DATABASE_URL is not set")
    return url


@pytest.fixture()
def warehouse():
    wh = PostgresWarehouse(_postgres_url(), schema=make_test_schema())
    try:
        wh.ensure_tables()
        yield wh
    finally:
        cleanup_test_warehouse(wh)


class _Request:
    def __init__(self, response=None, error=None):
        self._response, self._error = response, error

    def execute(self):
        if self._error is not None:
            raise self._error
        return self._response


class FakeSettingsGmail:
    def __init__(self, labels, filters):
        self.labels_list = labels
        self.filters_list = filters

    def users(self):
        return self

    def labels(self):
        outer = self

        class Labels:
            def list(self, userId):
                return _Request({"labels": outer.labels_list})

        return Labels()

    def settings(self):
        outer = self

        class Settings:
            def filters(self):
                class Filters:
                    def list(self, userId):
                        # Gmail omits the key entirely when there are no filters.
                        return _Request({"filter": outer.filters_list} if outer.filters_list else {})

                return Filters()

        return Settings()


class FakeLogger:
    def __init__(self):
        self.warnings = []

    def info(self, *args, **kwargs):
        pass

    def warning(self, message, *args, **kwargs):
        self.warnings.append(str(message) % args if args else str(message))


NEWSLETTERS = {
    "id": "Label_7",
    "name": "Newsletters",
    "type": "user",
    "labelListVisibility": "labelShow",
    "messageListVisibility": "show",
    "color": {"backgroundColor": "#16a766", "textColor": "#ffffff"},
}
INBOX = {"id": "INBOX", "name": "INBOX", "type": "system"}
FILTER = {
    "id": "ANe1Bmabc",
    "criteria": {"from": "news@example.com"},
    "action": {"addLabelIds": ["Label_7"], "removeLabelIds": ["INBOX"]},
}
NOW = datetime(2026, 10, 11, 12, 0, tzinfo=UTC)


def test_label_and_filter_rows_flatten_what_a_reader_joins_on() -> None:
    label = label_row(NEWSLETTERS)
    assert label["label_id"] == "Label_7" and label["label_type"] == "user"
    assert label["background_color"] == "#16a766" and label["text_color"] == "#ffffff"
    assert label["raw_json"] == NEWSLETTERS
    row = filter_row({**FILTER, "action": {**FILTER["action"], "forward": "x@example.com"}})
    assert row["filter_id"] == "ANe1Bmabc"
    assert row["criteria_json"] == {"from": "news@example.com"}
    assert row["add_label_ids"] == ["Label_7"] and row["remove_label_ids"] == ["INBOX"]
    assert row["forward_to"] == "x@example.com"


def test_sync_snapshots_labels_and_filters_and_tombstones_what_disappears(warehouse) -> None:
    service = FakeSettingsGmail([INBOX, NEWSLETTERS], [FILTER])
    runner = GmailSettingsSyncRunner(
        accounts=["zach@example.test"], warehouse=warehouse, logger=FakeLogger(),
        service_factory=lambda account: service, now=lambda: NOW,
    )
    summary = runner.run()
    assert summary.accounts_synced == 1 and summary.labels == 2 and summary.filters == 1
    labels = {row["label_id"]: row for row in warehouse.gmail_labels(account="zach@example.test")}
    assert set(labels) == {"INBOX", "Label_7"}
    assert labels["Label_7"]["name"] == "Newsletters" and labels["INBOX"]["label_type"] == "system"
    filters = warehouse.gmail_filters(account="zach@example.test")
    assert [row["filter_id"] for row in filters] == ["ANe1Bmabc"]
    assert filters[0]["add_label_ids"] == ["Label_7"]
    state = warehouse._query_dicts("SELECT * FROM @gmail_settings_sync_state")
    assert state[0]["status"] == "ok" and state[0]["label_count"] == 2 and state[0]["filter_count"] == 1

    # The filter and the label are deleted in Gmail: both stay as tombstones.
    service.labels_list = [INBOX]
    service.filters_list = []
    runner._now = lambda: NOW + timedelta(minutes=30)
    runner.run()
    assert [row["label_id"] for row in warehouse.gmail_labels(account="zach@example.test")] == ["INBOX"]
    assert warehouse.gmail_filters(account="zach@example.test") == []
    gone = warehouse.gmail_filters(account="zach@example.test", include_deleted=True)
    assert gone[0]["is_deleted"] == 1

    # Recreated (same id): live again.
    service.labels_list = [INBOX, NEWSLETTERS]
    runner._now = lambda: NOW + timedelta(minutes=60)
    runner.run()
    assert {row["label_id"] for row in warehouse.gmail_labels(account="zach@example.test")} == {"INBOX", "Label_7"}


def test_one_failing_account_is_recorded_and_does_not_stop_the_other(warehouse) -> None:
    good = FakeSettingsGmail([INBOX], [])

    def factory(account):
        if account == "broken@example.test":
            raise RefreshError("invalid_grant: Token has been expired or revoked.")
        return good

    runner = GmailSettingsSyncRunner(
        accounts=["broken@example.test", "zach@example.test"], warehouse=warehouse,
        logger=FakeLogger(), service_factory=factory, now=lambda: NOW,
    )
    summary = runner.run()
    assert summary.accounts_synced == 1 and summary.failed_accounts == ["broken@example.test"]
    state = {row["account"]: row for row in warehouse._query_dicts("SELECT * FROM @gmail_settings_sync_state")}
    assert state["broken@example.test"]["status"] == "failed"
    assert "invalid_grant" in state["broken@example.test"]["error"]
    assert state["zach@example.test"]["status"] == "ok"
    assert [row["label_id"] for row in warehouse.gmail_labels(account="zach@example.test")] == ["INBOX"]
