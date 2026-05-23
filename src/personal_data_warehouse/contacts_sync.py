from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
import argparse
import json
import logging
import time
from typing import Any

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from personal_data_warehouse.config import ContactGoogleAccount, Settings, load_settings
from personal_data_warehouse.google_auth import load_google_credentials
from personal_data_warehouse.warehouse import warehouse_from_settings

SOURCE = "google_people"
SOURCE_KIND = "google_contacts"
ADDRESS_BOOK_ID = "people/me"
CONTACT_PERSON_FIELDS = ",".join(
    [
        "addresses",
        "biographies",
        "birthdays",
        "emailAddresses",
        "events",
        "memberships",
        "metadata",
        "names",
        "nicknames",
        "organizations",
        "phoneNumbers",
        "photos",
        "urls",
        "userDefined",
    ]
)
EPOCH_UTC = datetime.fromtimestamp(0, tz=UTC)


@dataclass(frozen=True)
class ContactsSyncSummary:
    account: str
    source: str
    source_kind: str
    address_book_id: str
    sync_type: str
    next_sync_token: str
    cards_written: int
    deleted_cards: int
    tombstones_written: int = 0


def public_contacts_sync_summary(summary: ContactsSyncSummary) -> dict[str, Any]:
    return {
        "account": summary.account,
        "source": summary.source,
        "source_kind": summary.source_kind,
        "address_book_id": summary.address_book_id,
        "sync_type": summary.sync_type,
        "has_next_sync_token": bool(summary.next_sync_token),
        "cards_written": summary.cards_written,
        "deleted_cards": summary.deleted_cards,
        "tombstones_written": summary.tombstones_written,
    }


class ContactsSyncRunner:
    def __init__(
        self,
        *,
        settings: Settings,
        warehouse,
        logger,
        service_factory: Callable[[ContactGoogleAccount], Any] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._settings = settings
        self._warehouse = warehouse
        self._logger = logger
        self._service_factory = service_factory or (
            lambda account: build_google_people_service(account=account, settings=settings)
        )
        self._now = now or (lambda: datetime.now(tz=UTC))

    def sync_all(self) -> list[ContactsSyncSummary]:
        self._warehouse.ensure_contacts_tables()
        state_by_key = self._warehouse.load_contact_sync_state()
        summaries: list[ContactsSyncSummary] = []
        failures: list[str] = []

        for account in self._settings.contact_google_accounts:
            state_key = (SOURCE, account.email_address, SOURCE_KIND, ADDRESS_BOOK_ID)
            state = state_by_key.get(state_key)
            try:
                summary = self._sync_account(
                    account=account,
                    service=self._service_factory(account),
                    state=state,
                )
            except Exception as exc:
                self._warehouse.insert_contact_sync_state(
                    source=SOURCE,
                    account=account.email_address,
                    source_kind=SOURCE_KIND,
                    address_book_id=ADDRESS_BOOK_ID,
                    sync_token=str((state or {}).get("sync_token", "")),
                    last_sync_type=str((state or {}).get("last_sync_type", "unknown")),
                    status="failed",
                    error=truncate_error(str(exc)),
                    full_synced_at=state_datetime(state, "full_synced_at"),
                    updated_at=self._now(),
                )
                failures.append(f"{account.email_address}: {exc}")
                continue

            self._warehouse.insert_contact_sync_state(
                source=SOURCE,
                account=summary.account,
                source_kind=summary.source_kind,
                address_book_id=summary.address_book_id,
                sync_token=summary.next_sync_token,
                last_sync_type=summary.sync_type,
                status="ok",
                error="",
                full_synced_at=self._now() if summary.sync_type == "full" else state_datetime(state, "full_synced_at"),
                updated_at=self._now(),
            )
            summaries.append(summary)

        if failures:
            raise RuntimeError("Contacts sync failed for: " + "; ".join(failures))
        return summaries

    def _sync_account(
        self,
        *,
        account: ContactGoogleAccount,
        service,
        state: Mapping[str, Any] | None,
    ) -> ContactsSyncSummary:
        sync_token = str((state or {}).get("sync_token", ""))
        if self._settings.contact_force_full_sync or not sync_token:
            return self._sync_google_contacts(
                account=account,
                service=service,
                sync_type="full",
                sync_token=None,
            )

        try:
            return self._sync_google_contacts(
                account=account,
                service=service,
                sync_type="partial",
                sync_token=sync_token,
            )
        except HttpError as exc:
            if not is_expired_sync_token_error(exc):
                raise
            self._logger.warning(
                "Google Contacts sync token for %s is stale, falling back to full sync",
                account.email_address,
            )
            return self._sync_google_contacts(
                account=account,
                service=service,
                sync_type="full",
                sync_token=None,
            )

    def _sync_google_contacts(
        self,
        *,
        account: ContactGoogleAccount,
        service,
        sync_type: str,
        sync_token: str | None,
    ) -> ContactsSyncSummary:
        synced_at = self._now()
        next_sync_token = sync_token or ""
        cards_written = 0
        deleted_cards = 0
        active_card_ids: set[str] = set()
        pending_rows: list[dict[str, Any]] = []

        self._logger.info("Starting %s Google Contacts sync for %s", sync_type, account.email_address)
        for people, page_sync_token in iter_google_contact_pages(
            service=service,
            page_size=self._settings.contact_page_size,
            sync_token=sync_token,
        ):
            rows = [
                person_to_contact_card_row(
                    account=account.email_address,
                    person=person,
                    synced_at=synced_at,
                )
                for person in people
            ]
            pending_rows.extend(rows)
            cards_written += len(rows)
            deleted_cards += sum(1 for row in rows if int(row["is_deleted"]) != 0)
            active_card_ids.update(str(row["card_id"]) for row in rows if int(row["is_deleted"]) == 0)
            if page_sync_token:
                next_sync_token = page_sync_token
            self._logger.info(
                "Synced %s Google Contacts cards for %s so far",
                cards_written,
                account.email_address,
            )

        self._warehouse.insert_contact_cards(pending_rows)

        tombstones_written = 0
        if sync_type == "full":
            tombstones_written = self._warehouse.mark_missing_contact_cards_deleted(
                source=SOURCE,
                account=account.email_address,
                source_kind=SOURCE_KIND,
                address_book_id=ADDRESS_BOOK_ID,
                active_card_ids=active_card_ids,
                synced_at=synced_at,
            )
            deleted_cards += tombstones_written

        return ContactsSyncSummary(
            account=account.email_address,
            source=SOURCE,
            source_kind=SOURCE_KIND,
            address_book_id=ADDRESS_BOOK_ID,
            sync_type=sync_type,
            next_sync_token=next_sync_token,
            cards_written=cards_written,
            deleted_cards=deleted_cards,
            tombstones_written=tombstones_written,
        )


def build_google_people_service(*, account: ContactGoogleAccount, settings: Settings):
    credentials = load_google_credentials(
        email_address=account.email_address,
        settings=settings,
        scopes=settings.contact_scopes,
        service_name="Google Contacts",
    )
    return build("people", "v1", credentials=credentials, cache_discovery=False)


def iter_google_contact_pages(
    *,
    service,
    page_size: int,
    sync_token: str | None = None,
) -> Iterator[tuple[list[Mapping[str, Any]], str | None]]:
    page_token: str | None = None
    while True:
        list_kwargs: dict[str, Any] = {
            "resourceName": ADDRESS_BOOK_ID,
            "pageSize": page_size,
            "personFields": CONTACT_PERSON_FIELDS,
            "requestSyncToken": True,
            "sources": ["READ_SOURCE_TYPE_CONTACT"],
        }
        if page_token:
            list_kwargs["pageToken"] = page_token
        if sync_token:
            list_kwargs["syncToken"] = sync_token

        response = execute_contacts_request(lambda: service.people().connections().list(**list_kwargs).execute())
        people = [item for item in response.get("connections", []) if isinstance(item, Mapping)]
        next_sync_token = response.get("nextSyncToken")
        yield people, str(next_sync_token) if next_sync_token else None

        page_token = response.get("nextPageToken")
        if not page_token:
            return


def execute_contacts_request(operation: Callable[[], Any], *, max_attempts: int = 5) -> Any:
    for attempt in range(1, max_attempts + 1):
        try:
            return operation()
        except HttpError as exc:
            status = http_status(exc)
            if status not in {429, 500, 502, 503, 504} or attempt == max_attempts:
                raise
            time.sleep(min(60, attempt))
    raise RuntimeError("unreachable Google Contacts retry state")


def person_to_contact_card_row(
    *,
    account: str,
    person: Mapping[str, Any],
    synced_at: datetime,
) -> dict[str, Any]:
    name = primary_mapping(person.get("names"))
    email = primary_mapping(person.get("emailAddresses"))
    phone = primary_mapping(person.get("phoneNumbers"))
    organization = primary_mapping(person.get("organizations"))
    biography = primary_mapping(person.get("biographies"))
    source = primary_source_metadata(person)
    is_deleted = 1 if person_deleted(person) else 0
    return {
        "source": SOURCE,
        "account": account,
        "source_kind": SOURCE_KIND,
        "address_book_id": ADDRESS_BOOK_ID,
        "card_id": str(person.get("resourceName", "")),
        "etag": str(person.get("etag", "")),
        "source_uid": str(source.get("id", "")),
        "display_name": str(name.get("displayName", "")),
        "given_name": str(name.get("givenName", "")),
        "family_name": str(name.get("familyName", "")),
        "organization": str(organization.get("name", "")),
        "job_title": str(organization.get("title", "")),
        "primary_email": str(email.get("value", "")),
        "primary_phone": str(phone.get("canonicalForm") or phone.get("value", "")),
        "emails": mapping_list(person.get("emailAddresses")),
        "phones": mapping_list(person.get("phoneNumbers")),
        "addresses": mapping_list(person.get("addresses")),
        "organizations": mapping_list(person.get("organizations")),
        "urls": mapping_list(person.get("urls")),
        "groups": mapping_list(person.get("memberships")),
        "dates": {
            "birthdays": mapping_list(person.get("birthdays")),
            "events": mapping_list(person.get("events")),
        },
        "photos": mapping_list(person.get("photos")),
        "notes": str(biography.get("value", "")),
        "is_deleted": is_deleted,
        "source_updated_at": parse_rfc3339(str(source.get("updateTime", ""))) or EPOCH_UTC,
        "synced_at": synced_at,
        "sync_version": sync_version_from_datetime(synced_at),
        "raw_json": dict(person),
    }


def person_deleted(person: Mapping[str, Any]) -> bool:
    metadata = person.get("metadata")
    return isinstance(metadata, Mapping) and bool(metadata.get("deleted"))


def primary_mapping(value: Any) -> Mapping[str, Any]:
    items = mapping_list(value)
    for item in items:
        metadata = item.get("metadata")
        if isinstance(metadata, Mapping) and metadata.get("primary"):
            return item
    return items[0] if items else {}


def primary_source_metadata(person: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = person.get("metadata")
    if not isinstance(metadata, Mapping):
        return {}
    sources = mapping_list(metadata.get("sources"))
    for source in sources:
        if source.get("type") == "CONTACT":
            return source
    return sources[0] if sources else {}


def mapping_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def parse_rfc3339(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def sync_version_from_datetime(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return int(value.astimezone(UTC).timestamp() * 1_000_000)


def state_datetime(state: Mapping[str, Any] | None, key: str) -> datetime:
    value = (state or {}).get(key)
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str) and value:
        return parse_rfc3339(value) or EPOCH_UTC
    return EPOCH_UTC


def is_expired_sync_token_error(exc: HttpError) -> bool:
    status = http_status(exc)
    content = getattr(exc, "content", b"")
    if isinstance(content, bytes):
        text = content.decode("utf-8", errors="ignore")
    else:
        text = str(content)
    lower = text.lower()
    return status == 410 or (status == 400 and ("sync token" in lower or "expired" in lower))


def http_status(exc: HttpError) -> int | None:
    try:
        return int(exc.resp.status)
    except (TypeError, ValueError, AttributeError):
        return None


def truncate_error(value: str, *, max_chars: int = 4000) -> str:
    if len(value) <= max_chars:
        return value
    return value[: max_chars - 3] + "..."


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync Google Contacts into the personal data warehouse.")
    parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = load_settings(require_gmail=False, require_contacts=True)
    warehouse = warehouse_from_settings(settings)
    summaries = ContactsSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=logging.getLogger("personal_data_warehouse.contacts_sync"),
    ).sync_all()
    print(json.dumps([public_contacts_sync_summary(summary) for summary in summaries], sort_keys=True, default=str))


if __name__ == "__main__":
    main()
