"""Snapshot every Gmail account's labels and filters into base_gmail.

Reviewed ``gmail.*_label`` / ``gmail.*_filter`` mutations are proposed and
reviewed against these tables -- the review server holds no Google
credentials, so the warehouse is the only place it can read "what this filter
does today" from -- and they turn ``base_gmail.messages.label_ids`` into names.

Both listings are small (hundreds of labels, at most 1,000 filters) and Gmail
offers no change feed for either, so each run takes a full snapshot: two API
calls per account under the read-only scope every sync already holds.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from googleapiclient.discovery import build

from personal_data_warehouse.config import Settings
from personal_data_warehouse.gmail_sync import execute_gmail_request
from personal_data_warehouse.google_auth import load_google_credentials


GMAIL_SETTINGS_STATUS_OK = "ok"
GMAIL_SETTINGS_STATUS_FAILED = "failed"


@dataclass
class GmailSettingsSyncSummary:
    accounts_synced: int = 0
    labels: int = 0
    filters: int = 0
    labels_deleted: int = 0
    filters_deleted: int = 0
    failed_accounts: list[str] = field(default_factory=list)


def label_row(label: Mapping[str, Any]) -> dict[str, Any]:
    color = label.get("color") if isinstance(label.get("color"), Mapping) else {}
    return {
        "label_id": str(label.get("id") or ""),
        "name": str(label.get("name") or ""),
        "label_type": str(label.get("type") or ""),
        "label_list_visibility": str(label.get("labelListVisibility") or ""),
        "message_list_visibility": str(label.get("messageListVisibility") or ""),
        "background_color": str(color.get("backgroundColor") or ""),
        "text_color": str(color.get("textColor") or ""),
        "raw_json": dict(label),
    }


def filter_row(gmail_filter: Mapping[str, Any]) -> dict[str, Any]:
    criteria = gmail_filter.get("criteria") if isinstance(gmail_filter.get("criteria"), Mapping) else {}
    action = gmail_filter.get("action") if isinstance(gmail_filter.get("action"), Mapping) else {}
    return {
        "filter_id": str(gmail_filter.get("id") or ""),
        "criteria_json": dict(criteria),
        "action_json": dict(action),
        "add_label_ids": [str(value) for value in action.get("addLabelIds") or []],
        "remove_label_ids": [str(value) for value in action.get("removeLabelIds") or []],
        "forward_to": str(action.get("forward") or ""),
        "raw_json": dict(gmail_filter),
    }


def list_gmail_labels(service) -> list[dict[str, Any]]:
    response = execute_gmail_request(lambda: service.users().labels().list(userId="me").execute())
    return [dict(label) for label in response.get("labels") or [] if isinstance(label, Mapping)]


def list_gmail_filters(service) -> list[dict[str, Any]]:
    response = execute_gmail_request(lambda: service.users().settings().filters().list(userId="me").execute())
    return [dict(item) for item in response.get("filter") or [] if isinstance(item, Mapping)]


def build_gmail_settings_read_service(*, account: str, settings: Settings):
    credentials = load_google_credentials(
        email_address=settings.account_for_email(account).email_address,
        settings=settings,
        scopes=settings.gmail_scopes,
        service_name="Gmail settings sync",
    )
    return build("gmail", "v1", credentials=credentials, cache_discovery=False)


class GmailSettingsSyncRunner:
    def __init__(
        self,
        *,
        accounts: Iterable[str],
        warehouse,
        logger,
        service_factory: Callable[[str], Any],
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._accounts = [str(account).strip().lower() for account in accounts if str(account).strip()]
        self._warehouse = warehouse
        self._logger = logger
        self._service_factory = service_factory
        self._now = now or (lambda: datetime.now(tz=UTC))

    @classmethod
    def from_settings(cls, *, settings: Settings, warehouse, logger, accounts: Iterable[str] | None = None):
        configured = [account.email_address for account in settings.gmail_accounts]
        return cls(
            accounts=configured if accounts is None else [a for a in accounts if a.lower() in {c.lower() for c in configured}],
            warehouse=warehouse,
            logger=logger,
            service_factory=lambda account: build_gmail_settings_read_service(account=account, settings=settings),
        )

    def run(self) -> GmailSettingsSyncSummary:
        summary = GmailSettingsSyncSummary()
        for account in self._accounts:
            synced_at = self._now()
            try:
                service = self._service_factory(account)
                labels = [label_row(label) for label in list_gmail_labels(service)]
                filters = [filter_row(item) for item in list_gmail_filters(service)]
                deleted = self._warehouse.replace_gmail_settings_snapshot(
                    account=account,
                    labels=[row for row in labels if row["label_id"]],
                    filters=[row for row in filters if row["filter_id"]],
                    synced_at=synced_at,
                )
            except Exception as exc:  # noqa: BLE001 - recorded per account, the next account still runs
                self._logger.warning("Gmail settings sync failed for %s: %s", account, exc)
                summary.failed_accounts.append(account)
                self._warehouse.upsert_gmail_settings_sync_state(
                    {
                        "account": account,
                        "status": GMAIL_SETTINGS_STATUS_FAILED,
                        "error": f"{type(exc).__name__}: {exc}"[:2000],
                        "label_count": 0,
                        "filter_count": 0,
                        "last_synced_at": datetime(1970, 1, 1, tzinfo=UTC),
                        "updated_at": synced_at,
                    }
                )
                continue
            summary.accounts_synced += 1
            summary.labels += len(labels)
            summary.filters += len(filters)
            summary.labels_deleted += deleted.get("gmail_labels", 0)
            summary.filters_deleted += deleted.get("gmail_filters", 0)
            self._warehouse.upsert_gmail_settings_sync_state(
                {
                    "account": account,
                    "status": GMAIL_SETTINGS_STATUS_OK,
                    "error": "",
                    "label_count": len(labels),
                    "filter_count": len(filters),
                    "last_synced_at": synced_at,
                    "updated_at": synced_at,
                }
            )
            self._logger.info("Gmail settings for %s: %d labels, %d filters", account, len(labels), len(filters))
        return summary
