"""Keep the two address books one set: Google is canonical, iCloud mirrors it.

Zach's contacts live twice -- Google Contacts (``zach@zachlatta.com``, the book Gmail
autocompletes from) and iCloud (the book on his devices) -- and the two drift: a card
edited on the phone never reaches Google, a card added in Google never reaches the
phone. This module computes the delta between ``base_google_contacts.cards`` and
``base_apple_contacts.cards`` and proposes it as ordinary reviewed mutations, so the
books converge without anything writing silently.

The rule is asymmetric on purpose:

* **Google is canonical for what a card SAYS** -- the name, the organization, the
  title. When the two disagree, iCloud is changed to match Google.
* **Both books are additive for how to REACH a person.** An email or phone that only
  one book holds is added to the other; nothing is ever removed from either side.
* **A person is one card in each book.** Two iCloud cards that share an identifier with
  one Google card are merged (fullest kept); duplicate Google cards are folded into
  their fullest sibling. Identity is a shared email or phone, never a name alone.

Cards with no email and no phone cannot be matched and are left alone, as are the
GitHub notification and shared-login "contacts" that neither book should carry.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
import json
import os
import re
from typing import Any


GOOGLE_ACCOUNT_DEFAULT = "zach@zachlatta.com"
MIRROR_TITLE_PREFIX = "Contacts mirror"
JUNK_IDENTIFIER = re.compile(r"(reply\+.*@reply\.github\.com|@noreply\.github\.com|^logins\+|^zrl-bot@)")
_EMAIL_TYPES = {"home", "work", "other"}
_PHONE_TYPES = {"home", "work", "mobile", "main", "other"}


@dataclass
class MirrorPlan:
    apple_account: str
    google_account: str
    apple_mutations: list[dict[str, Any]] = field(default_factory=list)
    google_operations: list[dict[str, Any]] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)

    @property
    def is_empty(self) -> bool:
        return not self.apple_mutations and not self.google_operations


def google_account() -> str:
    return (os.getenv("CONTACTS_MIRROR_GOOGLE_ACCOUNT") or GOOGLE_ACCOUNT_DEFAULT).strip()


# -- card normalization ---------------------------------------------------------------


def _json(value: Any) -> list[Any]:
    if isinstance(value, str):
        return json.loads(value) if value else []
    return list(value or [])


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def normalize_phone(value: str) -> str:
    digits = re.sub(r"\D", "", value or "")
    return "1" + digits if len(digits) == 10 else digits


def _label(entry: Mapping[str, Any]) -> str:
    return _text(entry.get("label") or entry.get("type") or entry.get("formattedType")).lower()


def card_points(card: Mapping[str, Any]) -> tuple[dict[str, str], dict[str, tuple[str, str]]]:
    """(emails: lower -> label, phones: normalized -> (display value, label))."""

    emails: dict[str, str] = {}
    for entry in _json(card.get("emails")):
        value = _text(entry.get("value"))
        if "@" in value:
            emails.setdefault(value.lower(), _label(entry))
    phones: dict[str, tuple[str, str]] = {}
    for entry in _json(card.get("phones")):
        value = _text(entry.get("canonicalForm") or entry.get("value"))
        key = normalize_phone(value)
        if len(key) >= 10:
            phones.setdefault(key, (value, _label(entry)))
    return emails, phones


def card_identifiers(card: Mapping[str, Any]) -> set[str]:
    emails, phones = card_points(card)
    return set(emails) | set(phones)


def is_junk(card: Mapping[str, Any]) -> bool:
    return any(JUNK_IDENTIFIER.search(value) for value in card_identifiers(card))


def _name(card: Mapping[str, Any]) -> tuple[str, str]:
    given, family = _text(card.get("given_name")), _text(card.get("family_name"))
    # Google stores some cards with the whole name in givenName.
    if given and not family and " " in given:
        given, family = given.rsplit(" ", 1)
    return given, family


def _name_usable(card: Mapping[str, Any]) -> bool:
    given, family = _name(card)
    text = (given + " " + family).strip()
    return bool(text) and "@" not in text and re.match(r"^[\d+(]", text) is None


def _fullness(card: Mapping[str, Any]) -> tuple[int, int, int, int, str]:
    emails, phones = card_points(card)
    return (
        1 if card.get("image_type") else 0,
        len(emails) + len(phones),
        len(_text(card.get("notes"))),
        1 if _text(card.get("organization")) else 0,
        _text(card.get("source_updated_at")),
    )


def _same_person_names(cards: Iterable[Mapping[str, Any]]) -> bool:
    """Cards sharing an identifier are one person only when their names agree.

    A household landline links three different first names; that is a family,
    not a duplicate. Names are compatible when every usable given name shares
    its first three letters (Dave/David, Zach/Zachary) or one side has no name.
    """

    firsts = {_name(card)[0].lower()[:3] for card in cards if _name_usable(card)}
    return len(firsts) <= 1


# -- identifier graph ------------------------------------------------------------------


class _Union:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        self.parent.setdefault(x, x)
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: str, b: str) -> None:
        self.parent[self.find(a)] = self.find(b)


def _group_by_identifier(cards: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    union = _Union()
    owner: dict[str, str] = {}
    for card in cards:
        union.find(card["card_id"])
        for value in card_identifiers(card):
            if value in owner:
                union.union(card["card_id"], owner[value])
            else:
                owner[value] = card["card_id"]
    groups: dict[str, list[dict[str, Any]]] = {}
    for card in cards:
        groups.setdefault(union.find(card["card_id"]), []).append(card)
    return list(groups.values())


# -- payload builders ------------------------------------------------------------------


def _apple_entries(kind: str, items: Mapping[str, Any]) -> list[dict[str, str]]:
    out = []
    if kind == "emails":
        for value, label in items.items():
            out.append({"label": label if label in _EMAIL_TYPES else "other", "value": value})
    else:
        for value, label in items.values():
            out.append({"label": label if label in _PHONE_TYPES else "other", "value": value})
    return out


def _google_person(
    name: tuple[str, str] | None,
    org: str,
    title: str,
    emails: Mapping[str, str] | None,
    phones: Mapping[str, tuple[str, str]] | None,
) -> dict[str, Any]:
    person: dict[str, Any] = {}
    if name is not None:
        person["names"] = [{"givenName": name[0], "familyName": name[1]}]
    if org or title:
        entry: dict[str, str] = {}
        if org:
            entry["name"] = org
        if title:
            entry["title"] = title
        person["organizations"] = [entry]
    if emails is not None:
        person["emailAddresses"] = [
            {"value": value, **({"type": label} if label in _EMAIL_TYPES else {})} for value, label in emails.items()
        ]
    if phones is not None:
        person["phoneNumbers"] = [
            {"value": value, **({"type": label} if label in _PHONE_TYPES else {})} for value, label in phones.values()
        ]
    return person


# -- the plan --------------------------------------------------------------------------


def plan_mirror(
    google_cards: list[dict[str, Any]],
    apple_cards: list[dict[str, Any]],
    *,
    apple_account: str = GOOGLE_ACCOUNT_DEFAULT,
    google_account_email: str = GOOGLE_ACCOUNT_DEFAULT,
) -> MirrorPlan:
    plan = MirrorPlan(apple_account=apple_account, google_account=google_account_email)
    counts: dict[str, int] = {}

    def bump(key: str) -> None:
        counts[key] = counts.get(key, 0) + 1

    google = [c for c in google_cards if card_identifiers(c) and not is_junk(c)]
    apple = [c for c in apple_cards if card_identifiers(c) and not is_junk(c)]

    # 1. Google duplicates fold into their fullest sibling (Google is the book of record,
    #    so its own duplicates are resolved first and everything below sees one card).
    google_alive: list[dict[str, Any]] = []
    for group in _group_by_identifier(google):
        if len(group) > 1 and _same_person_names(group):
            keep = max(group, key=_fullness)
            emails: dict[str, str] = {}
            phones: dict[str, tuple[str, str]] = {}
            for card in group:
                e, p = card_points(card)
                for k, v in e.items():
                    emails.setdefault(k, v)
                for k, v in p.items():
                    phones.setdefault(k, v)
            plan.google_operations.append(
                {
                    "op": "update_contact",
                    "resource_name": keep["card_id"],
                    "etag": keep["etag"],
                    "person": _google_person(None, "", "", emails, phones),
                }
            )
            for other in group:
                if other is not keep:
                    plan.google_operations.append(
                        {"op": "delete_contact", "resource_name": other["card_id"], "etag": other["etag"]}
                    )
                    bump("google_duplicates_deleted")
            merged = dict(keep)
            merged["emails"] = json.dumps([{"value": k, "label": v} for k, v in emails.items()])
            merged["phones"] = json.dumps([{"value": v[0], "label": v[1], "canonicalForm": v[0]} for v in phones.values()])
            google_alive.append(merged)
        else:
            google_alive.extend(group)

    apple_by_value: dict[str, dict[str, Any]] = {}
    for card in apple:
        for value in card_identifiers(card):
            apple_by_value.setdefault(value, card)
    google_by_value: dict[str, dict[str, Any]] = {}
    for card in google_alive:
        for value in card_identifiers(card):
            google_by_value.setdefault(value, card)

    matched_apple: set[str] = set()
    for gcard in google_alive:
        twins: dict[str, dict[str, Any]] = {}
        for value in card_identifiers(gcard):
            twin = apple_by_value.get(value)
            if twin is not None:
                twins[twin["card_id"]] = twin
        if not twins:
            # 2. A Google-only person joins iCloud.
            if not _name_usable(gcard) and not _text(gcard.get("organization")):
                continue
            given, family = _name(gcard)
            emails, phones = card_points(gcard)
            contact: dict[str, Any] = {}
            if given:
                contact["given_name"] = given
            if family:
                contact["family_name"] = family
            if _text(gcard.get("organization")):
                contact["organization"] = _text(gcard.get("organization"))
            if _text(gcard.get("job_title")):
                contact["job_title"] = _text(gcard.get("job_title"))
            if emails:
                contact["emails"] = _apple_entries("emails", emails)
            if phones:
                contact["phones"] = _apple_entries("phones", phones)
            plan.apple_mutations.append(
                {"type": "apple_contacts.create_contact", "account": apple_account, "contact": contact}
            )
            bump("apple_created")
            continue

        twin_list = list(twins.values())
        if len(twin_list) > 1 and _same_person_names(twin_list):
            # 3. Several iCloud cards for one Google person: merge into the fullest.
            keep = max(twin_list, key=_fullness)
            plan.apple_mutations.append(
                {
                    "type": "apple_contacts.merge_contacts",
                    "account": apple_account,
                    "keep_card_id": keep["card_id"],
                    "merge_card_ids": [c["card_id"] for c in twin_list if c is not keep],
                }
            )
            bump("apple_merged")
            matched_apple.update(c["card_id"] for c in twin_list)
            continue
        acard = max(twin_list, key=_fullness)
        matched_apple.add(acard["card_id"])

        # 4. Matched pair: Google's name/org win; points are unioned both ways.
        g_emails, g_phones = card_points(gcard)
        a_emails, a_phones = card_points(acard)
        apple_update: dict[str, Any] = {}
        g_name, a_name = _name(gcard), _name(acard)
        if _name_usable(gcard) and g_name != a_name:
            apple_update["given_name"], apple_update["family_name"] = g_name
            apple_update = {k: v for k, v in apple_update.items() if v}
        g_org, a_org = _text(gcard.get("organization")), _text(acard.get("organization"))
        g_title, a_title = _text(gcard.get("job_title")), _text(acard.get("job_title"))
        if g_org and g_org != a_org:
            apple_update["organization"] = g_org
        if g_title and g_title != a_title:
            apple_update["job_title"] = g_title
        missing_emails = {k: v for k, v in g_emails.items() if k not in a_emails}
        missing_phones = {k: v for k, v in g_phones.items() if k not in a_phones}
        if missing_emails:
            apple_update["emails"] = _apple_entries("emails", missing_emails)
        if missing_phones:
            apple_update["phones"] = _apple_entries("phones", missing_phones)
        if apple_update:
            plan.apple_mutations.append(
                {"type": "apple_contacts.update_contact", "account": apple_account, "card_id": acard["card_id"], "contact": apple_update}
            )
            bump("apple_updated")

        google_person: dict[str, Any] = {}
        if not _name_usable(gcard) and _name_usable(acard):
            google_person["names"] = [{"givenName": a_name[0], "familyName": a_name[1]}]
        if not g_org and not g_title and (a_org or a_title):
            google_person.update(_google_person(None, a_org, a_title, None, None))
        union_emails = dict(g_emails)
        for k, v in a_emails.items():
            union_emails.setdefault(k, v)
        union_phones = dict(g_phones)
        for k, v in a_phones.items():
            union_phones.setdefault(k, v)
        if set(union_emails) != set(g_emails) or set(union_phones) != set(g_phones):
            google_person.update(_google_person(None, "", "", union_emails, union_phones))
        if google_person:
            plan.google_operations.append(
                {"op": "update_contact", "resource_name": gcard["card_id"], "etag": gcard["etag"], "person": google_person}
            )
            bump("google_updated")

    # 5. An iCloud-only person joins Google.
    for acard in apple:
        if acard["card_id"] in matched_apple:
            continue
        if any(value in google_by_value for value in card_identifiers(acard)):
            continue
        if not _name_usable(acard):
            continue
        emails, phones = card_points(acard)
        plan.google_operations.append(
            {
                "op": "create_contact",
                "person": _google_person(_name(acard), _text(acard.get("organization")), _text(acard.get("job_title")), emails or None, phones or None),
            }
        )
        bump("google_created")

    plan.counts = counts
    return plan


# -- proposals -------------------------------------------------------------------------


def proposal_payloads(plan: MirrorPlan, *, run_date: str) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    context = {
        "pass": "contacts mirror (Google canonical; iCloud follows; both additive for emails/phones)",
        "date": run_date,
        "counts": plan.counts,
    }
    if plan.apple_mutations:
        payloads.append(
            {
                "title": f"{MIRROR_TITLE_PREFIX} {run_date}: bring iCloud in line with Google ({len(plan.apple_mutations)} changes)",
                "reason": "Google Contacts is the canonical book. These iCloud cards take Google's name/organization, gain emails and phones only Google had, or are created/merged so each person is one card.",
                "context": {**context, "operations": len(plan.apple_mutations)},
                "mutations": plan.apple_mutations,
            }
        )
    if plan.google_operations:
        payloads.append(
            {
                "title": f"{MIRROR_TITLE_PREFIX} {run_date}: give Google what only iCloud had ({len(plan.google_operations)} operations)",
                "reason": "Additive only: emails/phones and people that exist on the devices but not in Google, plus Google's own duplicates folded into one card. Nothing is removed from a surviving card.",
                "context": {**context, "operations": len(plan.google_operations)},
                "mutations": [{"type": "google_people.contacts", "account": plan.google_account, "operations": plan.google_operations}],
            }
        )
    return payloads


# -- warehouse + app plumbing ----------------------------------------------------------


def load_cards(warehouse, *, google_account_email: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    google = warehouse._query_dicts(
        """
        SELECT account, card_id, etag, display_name, given_name, family_name, organization, job_title,
               emails::text AS emails, phones::text AS phones, notes, source_updated_at
        FROM @contact_cards
        WHERE is_deleted = 0 AND source = 'google_people' AND account = %s
        """,
        (google_account_email,),
    )
    apple = warehouse._query_dicts(
        """
        SELECT card_id, display_name, given_name, family_name, organization, job_title,
               emails::text AS emails, phones::text AS phones, notes, source_updated_at,
               raw_json->>'ZIMAGETYPE' AS image_type
        FROM @apple_contact_cards
        WHERE is_deleted = 0
        """
    )
    return google, apple


def pending_mirror_requests(warehouse) -> int:
    rows = warehouse._query_dicts(
        "SELECT count(*) AS n FROM @upstream_mutation_requests WHERE status = 'pending_review' AND title LIKE %s",
        (MIRROR_TITLE_PREFIX + "%",),
    )
    return int(rows[0]["n"]) if rows else 0


class AppProposer:
    """Files a proposal through the app's own propose_mutation tool."""

    def __init__(self, *, base_url: str, secret_token: str, client_name: str = "contacts-mirror", session=None, timeout: float = 120.0) -> None:
        if session is None:
            import requests

            session = requests.Session()
        self._session = session
        self._base_url = base_url.rstrip("/")
        self._secret_token = secret_token
        self._client_name = client_name
        self._timeout = timeout

    def propose(self, payload: dict[str, Any]) -> str:
        response = self._session.post(
            f"{self._base_url}/api/tools/propose_mutation",
            json=payload,
            headers={
                "Authorization": f"Bearer {self._client_name}:{self._secret_token}",
                # Cloudflare 403s default urllib-ish agents in front of the app.
                "User-Agent": "personal-data-warehouse-contacts-mirror/1",
            },
            timeout=self._timeout,
        )
        response.raise_for_status()
        data = (response.json() or {}).get("data") or {}
        if data.get("error"):
            raise RuntimeError(f"propose_mutation refused the contacts mirror request: {data['error']}")
        return str(data.get("request_id") or "")
