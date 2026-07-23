from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
import sqlite3
from typing import Any

APPLE_EPOCH = datetime(2001, 1, 1, tzinfo=UTC)
DEFAULT_CONTACTS_STORE_PATH = "~/Library/Application Support/AddressBook"
STORE_FILENAME = "AddressBook-v22.abcddb"


class AppleContactsSchemaError(RuntimeError):
    pass


@dataclass(frozen=True)
class AppleContact:
    source_id: str
    contact_id: str
    source_uid: str
    display_name: str
    given_name: str
    middle_name: str
    family_name: str
    nickname: str
    organization: str
    department: str
    job_title: str
    primary_email: str
    primary_phone: str
    emails: tuple[dict[str, object], ...]
    phones: tuple[dict[str, object], ...]
    addresses: tuple[dict[str, object], ...]
    organizations: tuple[dict[str, object], ...]
    urls: tuple[dict[str, object], ...]
    nicknames: tuple[dict[str, object], ...]
    groups: tuple[dict[str, object], ...]
    dates: dict[str, object]
    photos: tuple[dict[str, object], ...]
    note: str
    created_at: datetime
    modified_at: datetime
    raw: dict[str, object]


def discover_apple_contacts_stores(store_path: Path | str) -> list[tuple[str, Path]]:
    path = Path(store_path).expanduser()
    if path.is_file():
        return [(path.parent.name or "default", path)]
    candidates = sorted(candidate for candidate in path.rglob(STORE_FILENAME) if candidate.is_file())
    stores = [
        ("local" if candidate.parent == path else candidate.parent.name, candidate)
        for candidate in candidates
    ]
    if not stores:
        raise FileNotFoundError(f"No Apple Contacts stores found under {path}")
    return stores


def snapshot_apple_contacts_store(
    store_path: Path | str,
    destination_path: Path | str,
) -> Path:
    source_path = Path(store_path).expanduser()
    destination = Path(destination_path)
    source_uri = source_path.resolve().as_uri() + "?mode=ro"
    try:
        source = sqlite3.connect(source_uri, uri=True)
    except sqlite3.Error as exc:
        raise PermissionError(
            f"Could not open Apple Contacts store at {source_path}. "
            "Grant Full Disk Access to the launching executable chain and retry."
        ) from exc
    try:
        target = sqlite3.connect(destination)
        try:
            source.backup(target)
        finally:
            target.close()
    finally:
        source.close()
    return destination


def scan_apple_contacts_store(
    store_path: Path | str,
    *,
    source_id: str | None = None,
) -> list[AppleContact]:
    path = Path(store_path)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    try:
        tables = _table_names(connection)
        resolved_source_id = source_id or path.parent.name or "default"
        if "contacts" in tables:
            return _scan_synthetic_store(connection, source_id=resolved_source_id)
        if "ZABCDRECORD" in tables and "Z_PRIMARYKEY" in tables:
            return _scan_core_data_store(connection, source_id=resolved_source_id)
        raise AppleContactsSchemaError(
            "Unsupported Apple Contacts schema: expected a synthetic contacts table "
            "or Apple's ZABCDRECORD Core Data tables"
        )
    finally:
        connection.close()


def _scan_synthetic_store(
    connection: sqlite3.Connection,
    *,
    source_id: str,
) -> list[AppleContact]:
    phones = _synthetic_contact_points(connection, "phones")
    emails = _synthetic_contact_points(connection, "emails")
    contacts: list[AppleContact] = []
    for row in connection.execute("SELECT * FROM contacts ORDER BY contact_id").fetchall():
        contact_id = _string(row, "contact_id")
        row_source_id = _string(row, "source_id") or source_id
        contact_phones = tuple(phones.get(contact_id, ()))
        contact_emails = tuple(emails.get(contact_id, ()))
        organization = _string(row, "organization")
        department = _string(row, "department")
        job_title = _string(row, "job_title")
        nickname = _string(row, "nickname")
        contacts.append(
            AppleContact(
                source_id=row_source_id,
                contact_id=contact_id,
                source_uid=contact_id,
                display_name=_string(row, "display_name"),
                given_name=_string(row, "given_name"),
                middle_name=_string(row, "middle_name"),
                family_name=_string(row, "family_name"),
                nickname=nickname,
                organization=organization,
                department=department,
                job_title=job_title,
                primary_email=_primary_value(contact_emails),
                primary_phone=_primary_value(contact_phones),
                emails=contact_emails,
                phones=contact_phones,
                addresses=(),
                organizations=_organization_values(
                    organization=organization,
                    department=department,
                    job_title=job_title,
                ),
                urls=(),
                nicknames=({"value": nickname},) if nickname else (),
                groups=(),
                dates={},
                photos=(),
                note=_string(row, "note"),
                created_at=_parse_datetime(_value(row, "created_at")),
                modified_at=_parse_datetime(_value(row, "modified_at")),
                raw=_public_row(row),
            )
        )
    return contacts


def _synthetic_contact_points(
    connection: sqlite3.Connection,
    table: str,
) -> dict[str, list[dict[str, object]]]:
    if table not in _table_names(connection):
        return {}
    values: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in connection.execute(
        f"SELECT * FROM {table} ORDER BY contact_id, is_primary DESC, rowid"
    ).fetchall():
        values[_string(row, "contact_id")].append(
            {
                "value": _string(row, "value"),
                "label": _string(row, "label"),
                "metadata": {"primary": bool(_int(row, "is_primary"))},
            }
        )
    return values


def _scan_core_data_store(
    connection: sqlite3.Connection,
    *,
    source_id: str,
) -> list[AppleContact]:
    entity_ids = {
        int(row["Z_ENT"])
        for row in connection.execute(
            "SELECT Z_ENT FROM Z_PRIMARYKEY WHERE Z_NAME IN ('ABCDContact', 'ABCDSubscribedContact')"
        ).fetchall()
    }
    if not entity_ids:
        raise AppleContactsSchemaError("Unsupported Apple Contacts schema: ABCDContact entity is missing")

    phones = _core_data_values(
        connection,
        table="ZABCDPHONENUMBER",
        value_columns=("ZFULLNUMBER",),
    )
    emails = _core_data_values(
        connection,
        table="ZABCDEMAILADDRESS",
        value_columns=("ZADDRESS",),
    )
    addresses = _core_data_addresses(connection)
    urls = _core_data_values(
        connection,
        table="ZABCDURLADDRESS",
        value_columns=("ZURL",),
    )
    related_names = _core_data_values(
        connection,
        table="ZABCDRELATEDNAME",
        value_columns=("ZNAME",),
    )
    social_profiles = _core_data_social_profiles(connection)
    contact_dates = _core_data_dates(connection)
    notes = _core_data_notes(connection)

    placeholders = ",".join("?" for _ in entity_ids)
    rows = connection.execute(
        f"SELECT * FROM ZABCDRECORD WHERE Z_ENT IN ({placeholders}) ORDER BY Z_PK",
        tuple(sorted(entity_ids)),
    ).fetchall()
    contacts: list[AppleContact] = []
    for row in rows:
        record_pk = _int(row, "Z_PK")
        contact_id = _string(row, "ZUNIQUEID") or str(record_pk)
        given_name = _string(row, "ZFIRSTNAME")
        middle_name = _string(row, "ZMIDDLENAME")
        family_name = _string(row, "ZLASTNAME")
        organization = _string(row, "ZORGANIZATION")
        department = _string(row, "ZDEPARTMENT")
        job_title = _string(row, "ZJOBTITLE")
        nickname = _string(row, "ZNICKNAME")
        display_name = _string(row, "ZNAME") or _display_name(
            given_name=given_name,
            middle_name=middle_name,
            family_name=family_name,
            organization=organization,
        )
        contact_phones = tuple(phones.get(record_pk, ()))
        contact_emails = tuple(emails.get(record_pk, ()))
        birthday = _apple_datetime(_value(row, "ZBIRTHDAY"))
        dates: dict[str, object] = {
            "birthdays": ([{"date": birthday.isoformat()}] if birthday else []),
            "events": contact_dates.get(record_pk, []),
            "related_names": related_names.get(record_pk, []),
            "social_profiles": social_profiles.get(record_pk, []),
        }
        photo_values: tuple[dict[str, object], ...] = ()
        if any(_value(row, column) for column in ("ZIMAGEHASH", "ZIMAGEDATA", "ZTHUMBNAILIMAGEDATA")):
            photo_values = ({"has_image": True},)
        contacts.append(
            AppleContact(
                source_id=source_id,
                contact_id=contact_id,
                source_uid=_string(row, "ZEXTERNALUUID") or contact_id,
                display_name=display_name,
                given_name=given_name,
                middle_name=middle_name,
                family_name=family_name,
                nickname=nickname,
                organization=organization,
                department=department,
                job_title=job_title,
                primary_email=_primary_value(contact_emails),
                primary_phone=_primary_value(contact_phones),
                emails=contact_emails,
                phones=contact_phones,
                addresses=tuple(addresses.get(record_pk, ())),
                organizations=_organization_values(
                    organization=organization,
                    department=department,
                    job_title=job_title,
                ),
                urls=tuple(urls.get(record_pk, ())),
                nicknames=({"value": nickname},) if nickname else (),
                groups=(),
                dates=dates,
                photos=photo_values,
                note=notes.get(record_pk, ""),
                created_at=_apple_datetime(_value(row, "ZCREATIONDATE")) or APPLE_EPOCH,
                modified_at=_apple_datetime(_value(row, "ZMODIFICATIONDATE")) or APPLE_EPOCH,
                raw=_public_row(row),
            )
        )
    return contacts


def _core_data_values(
    connection: sqlite3.Connection,
    *,
    table: str,
    value_columns: tuple[str, ...],
) -> dict[int, list[dict[str, object]]]:
    if table not in _table_names(connection):
        return {}
    values: dict[int, list[dict[str, object]]] = defaultdict(list)
    for row in connection.execute(f"SELECT * FROM {table} ORDER BY ZORDERINGINDEX, Z_PK").fetchall():
        owner = _owner_id(row)
        value = next((_string(row, column) for column in value_columns if _string(row, column)), "")
        if not owner or not value:
            continue
        item: dict[str, object] = {
            "value": value,
            "label": _clean_label(_string(row, "ZLABEL")),
            "metadata": {"primary": bool(_int(row, "ZISPRIMARY"))},
        }
        if table == "ZABCDPHONENUMBER":
            item["canonicalForm"] = value
            item["countryCode"] = _string(row, "ZCOUNTRYCODE")
        values[owner].append(item)
    return values


def _core_data_addresses(connection: sqlite3.Connection) -> dict[int, list[dict[str, object]]]:
    table = "ZABCDPOSTALADDRESS"
    if table not in _table_names(connection):
        return {}
    values: dict[int, list[dict[str, object]]] = defaultdict(list)
    for row in connection.execute(f"SELECT * FROM {table} ORDER BY ZORDERINGINDEX, Z_PK").fetchall():
        owner = _owner_id(row)
        if not owner:
            continue
        values[owner].append(
            {
                "streetAddress": _string(row, "ZSTREET"),
                "city": _string(row, "ZCITY"),
                "region": _string(row, "ZSTATE") or _string(row, "ZREGION"),
                "postalCode": _string(row, "ZZIPCODE"),
                "country": _string(row, "ZCOUNTRYNAME"),
                "countryCode": _string(row, "ZCOUNTRYCODE"),
                "label": _clean_label(_string(row, "ZLABEL")),
                "metadata": {"primary": bool(_int(row, "ZISPRIMARY"))},
            }
        )
    return values


def _core_data_social_profiles(connection: sqlite3.Connection) -> dict[int, list[dict[str, object]]]:
    table = "ZABCDSOCIALPROFILE"
    if table not in _table_names(connection):
        return {}
    values: dict[int, list[dict[str, object]]] = defaultdict(list)
    for row in connection.execute(f"SELECT * FROM {table} ORDER BY ZORDERINGINDEX, Z_PK").fetchall():
        owner = _owner_id(row)
        if not owner:
            continue
        values[owner].append(
            {
                "service": _string(row, "ZSERVICENAME"),
                "username": _string(row, "ZUSERNAME"),
                "userIdentifier": _string(row, "ZUSERIDENTIFIER"),
                "url": _string(row, "ZURLSTRING"),
                "label": _clean_label(_string(row, "ZLABEL")),
            }
        )
    return values


def _core_data_dates(connection: sqlite3.Connection) -> dict[int, list[dict[str, object]]]:
    table = "ZABCDCONTACTDATE"
    if table not in _table_names(connection):
        return {}
    values: dict[int, list[dict[str, object]]] = defaultdict(list)
    for row in connection.execute(f"SELECT * FROM {table} ORDER BY ZORDERINGINDEX, Z_PK").fetchall():
        owner = _owner_id(row)
        value = _apple_datetime(_value(row, "ZDATE"))
        if owner and value:
            values[owner].append(
                {
                    "date": value.isoformat(),
                    "label": _clean_label(_string(row, "ZLABEL")),
                }
            )
    return values


def _core_data_notes(connection: sqlite3.Connection) -> dict[int, str]:
    table = "ZABCDNOTE"
    if table not in _table_names(connection):
        return {}
    notes: dict[int, str] = {}
    for row in connection.execute(f"SELECT * FROM {table}").fetchall():
        owner = _int(row, "ZCONTACT") or _int(row, "Z22_CONTACT")
        if owner:
            notes[owner] = _string(row, "ZTEXT")
    return notes


def _organization_values(
    *,
    organization: str,
    department: str,
    job_title: str,
) -> tuple[dict[str, object], ...]:
    if not any((organization, department, job_title)):
        return ()
    return (
        {
            "name": organization,
            "department": department,
            "title": job_title,
            "metadata": {"primary": True},
        },
    )


def _primary_value(values: Iterable[dict[str, object]]) -> str:
    items = list(values)
    for item in items:
        metadata = item.get("metadata")
        if isinstance(metadata, dict) and metadata.get("primary"):
            return str(item.get("value", ""))
    return str(items[0].get("value", "")) if items else ""


def _display_name(*, given_name: str, middle_name: str, family_name: str, organization: str) -> str:
    name = " ".join(part for part in (given_name, middle_name, family_name) if part).strip()
    return name or organization


def _owner_id(row: sqlite3.Row) -> int:
    return _int(row, "ZOWNER") or _int(row, "Z22_OWNER")


def _clean_label(value: str) -> str:
    prefix = "_$!<"
    suffix = ">!$_"
    if value.startswith(prefix) and value.endswith(suffix):
        return value[len(prefix) : -len(suffix)].lower()
    return value


def _apple_datetime(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return APPLE_EPOCH + timedelta(seconds=float(value))
    except (TypeError, ValueError, OverflowError):
        return None


def _parse_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif value:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            parsed = APPLE_EPOCH
    else:
        parsed = APPLE_EPOCH
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _table_names(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }


def _value(row: sqlite3.Row, column: str) -> object:
    return row[column] if column in row.keys() else None


def _string(row: sqlite3.Row, column: str) -> str:
    value = _value(row, column)
    return "" if value is None else str(value)


def _int(row: sqlite3.Row, column: str) -> int:
    value = _value(row, column)
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _public_row(row: sqlite3.Row) -> dict[str, object]:
    public: dict[str, object] = {}
    for key in row.keys():
        value = row[key]
        if isinstance(value, bytes):
            continue
        public[str(key)] = value
    return public
