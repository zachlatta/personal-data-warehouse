"""Apply approved Apple Contacts mutations through Contacts.app.

iCloud Contacts has no public write API, so -- exactly like Apple Notes -- the only
supported way to change a card is to ask Contacts.app itself on a Mac that is signed in.
The proposal and review halves live with every other mutation type (``app/internal/
mutations/apple_contacts.go``); this executor is what the local apple-contacts worker
runs after a human has approved the row.

Two properties make Contacts easier than Notes: the AppleScript ``id`` of a person is
the same ``<UUID>:ABPerson`` string the uploader stores in
``base_apple_contacts.cards.card_id``, so an agent addresses the card it found; and
every field is a real property rather than HTML.

Three operations:

* ``create_contact`` -- a new card from ``contact``.
* ``update_contact`` -- scalars in ``contact`` are SET, list fields (emails, phones,
  urls) are ADDED when the card lacks the value, ``remove`` lists values to delete,
  ``append_note`` adds to the note while ``note`` replaces it. The pre-edit card is
  recorded in ``result_json.previous_card``.
* ``merge_contacts`` -- union every list field and fill every empty scalar of the kept
  card from the merged cards, apply ``contact`` overrides on top, delete the merged
  cards. Every pre-merge card is recorded in ``result_json.previous_cards``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
import re
import subprocess
from typing import Any

from personal_data_warehouse.apple_automation import (
    DEFAULT_SCRIPT_TIMEOUT_SECONDS,
    IN_SCRIPT_TIMEOUT_SECONDS,
    applescript_string,
    classify_osascript_error,
    dedent_script,
    run_osascript,
)


APPLE_CONTACTS_PROVIDER = "apple_contacts"
APPLE_CONTACTS_CREATE_CONTACT_OPERATION = "apple_contacts.create_contact"
APPLE_CONTACTS_UPDATE_CONTACT_OPERATION = "apple_contacts.update_contact"
APPLE_CONTACTS_MERGE_CONTACTS_OPERATION = "apple_contacts.merge_contacts"

APPLE_CONTACTS_OPERATIONS = (
    APPLE_CONTACTS_CREATE_CONTACT_OPERATION,
    APPLE_CONTACTS_UPDATE_CONTACT_OPERATION,
    APPLE_CONTACTS_MERGE_CONTACTS_OPERATION,
)

# Scalar contact fields, in the order the read dump emits them, mapped to the Contacts
# AppleScript property that holds each one.
SCALAR_FIELDS: tuple[tuple[str, str], ...] = (
    ("given_name", "first name"),
    ("family_name", "last name"),
    ("middle_name", "middle name"),
    ("nickname", "nickname"),
    ("organization", "organization"),
    ("job_title", "job title"),
    ("department", "department"),
)
# List fields, mapped to the AppleScript element class.
LIST_FIELDS: tuple[tuple[str, str], ...] = (("emails", "email"), ("phones", "phone"), ("urls", "url"))

# The read script separates fields with the ASCII unit separator and records with the
# record separator, because a note can contain any printable character including
# newlines and pipes.
FIELD_SEPARATOR = "\x1f"
RECORD_SEPARATOR = "\x1e"
_LABEL_WRAPPER = re.compile(r"^_\$!<(.*)>!\$_$")
_MISSING = "missing value"


class AppleContactsCardNotFound(RuntimeError):
    pass


# Contacts.app's "Can't get person id" -- the record is gone, and it will stay gone.
CARD_NOT_FOUND_ERROR_CODE = "-1728"
# Bound on following merge chains (A merged into B, B merged into C, ...).
MAX_MERGE_CHAIN = 8


def is_card_not_found(error: BaseException) -> bool:
    return CARD_NOT_FOUND_ERROR_CODE in str(error)


class AppleContactsInvalidMutation(RuntimeError):
    pass


@dataclass(frozen=True)
class AppleContactsMutationResult:
    status: str
    error: str = ""
    result_json: dict[str, Any] = field(default_factory=dict)


def normalize_label(raw: str) -> str:
    """``_$!<Work>!$_`` is how Contacts spells its built-in labels; ``work`` is ours."""

    text = "" if raw is None else str(raw).strip()
    if text == _MISSING:
        return ""
    match = _LABEL_WRAPPER.match(text)
    if match:
        return match.group(1).strip().lower()
    return text


def _scalar(value: str) -> str:
    text = "" if value is None else str(value)
    return "" if text == _MISSING else text


def parse_card_dump(raw: str) -> dict[str, Any]:
    records = [r for r in str(raw or "").split(RECORD_SEPARATOR) if r != ""]
    if not records or not records[0].startswith("card" + FIELD_SEPARATOR):
        raise AppleContactsCardNotFound(f"Contacts.app returned no card: {raw!r}")
    head = records[0].split(FIELD_SEPARATOR)
    # card, id, then the scalar fields in SCALAR_FIELDS order, then the note.
    card: dict[str, Any] = {"card_id": head[1] if len(head) > 1 else ""}
    for index, (key, _property) in enumerate(SCALAR_FIELDS):
        card[key] = _scalar(head[2 + index]) if len(head) > 2 + index else ""
    card["note"] = _scalar(head[2 + len(SCALAR_FIELDS)]) if len(head) > 2 + len(SCALAR_FIELDS) else ""
    for key, _element in LIST_FIELDS:
        card[key] = []
    kinds = {element: key for key, element in LIST_FIELDS}
    for record in records[1:]:
        parts = record.split(FIELD_SEPARATOR)
        if len(parts) < 3 or parts[0] not in kinds:
            continue
        card[kinds[parts[0]]].append({"label": normalize_label(parts[1]), "value": _scalar(parts[2])})
    return card


def _value_key(kind: str, value: str) -> str:
    text = str(value or "").strip()
    if kind == "emails":
        return text.lower()
    if kind == "phones":
        digits = re.sub(r"\D", "", text)
        return digits[-10:] if len(digits) >= 10 else digits
    return text.rstrip("/").lower()


def _entries(value: Any) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    if not isinstance(value, list):
        return out
    for item in value:
        if isinstance(item, Mapping):
            text = str(item.get("value") or "").strip()
            if text:
                out.append({"label": str(item.get("label") or "").strip(), "value": text})
        elif isinstance(item, str) and item.strip():
            out.append({"label": "", "value": item.strip()})
    return out


def _read_script(card_id: str) -> str:
    scalar_reads = " & us & ".join(f"my txt({prop} of p)" for _key, prop in SCALAR_FIELDS)
    list_reads = "\n".join(
        f"""repeat with e in {key} of p
        set out to out & rs & "{element}" & us & my txt(label of e) & us & my txt(value of e)
        end repeat"""
        for key, element in LIST_FIELDS
    )
    return f"""
    on txt(v)
      if v is missing value then return ""
      return v as text
    end txt
    with timeout of {IN_SCRIPT_TIMEOUT_SECONDS} seconds
      tell application "Contacts"
        set us to (ASCII character 31)
        set rs to (ASCII character 30)
        set p to person id {applescript_string(card_id)}
        set out to "card" & us & (id of p) & us & {scalar_reads} & us & my txt(note of p)
        {list_reads}
        return out
      end tell
    end timeout
    """


class AppleContactsMutationExecutor:
    """Executes apple_contacts.* mutations claimed from ops.upstream_mutation_operations."""

    def __init__(
        self,
        *,
        runner: Callable[[str], str] | None = None,
        merged_into: Callable[[str], str | None] | None = None,
    ) -> None:
        """``merged_into(card_id)`` names the card a deleted card was merged into.

        Two requests proposed from one warehouse snapshot can name the same card: one
        merges it away, the other updates it. The merge runs first and Contacts.app then
        answers -1728 for the update, which used to die ``failed_terminal`` although the
        surviving card is exactly where the values belong. The resolver is backed by the
        mutation ledger's own succeeded ``merge_contacts`` rows; without one, a missing
        card is simply missing.
        """
        self._runner = runner or run_osascript
        self._merged_into = merged_into or (lambda _card_id: None)

    def _surviving_card(self, card_id: str) -> str | None:
        """Follow succeeded merges from ``card_id`` to the card that still exists."""
        seen = {card_id}
        current = card_id
        target: str | None = None
        for _ in range(MAX_MERGE_CHAIN):
            next_id = self._merged_into(current)
            if not next_id or next_id in seen:
                break
            seen.add(next_id)
            target = current = next_id
        return target

    def execute(self, mutation: Mapping[str, Any]) -> AppleContactsMutationResult:
        provider = str(mutation.get("provider") or "")
        operation = str(mutation.get("operation") or "")
        if provider != APPLE_CONTACTS_PROVIDER or operation not in APPLE_CONTACTS_OPERATIONS:
            # Never burn an unrecognized row to failed_terminal: a newer worker may
            # understand it. This mirrors the cloud worker's unknown-provider handling.
            return AppleContactsMutationResult(
                status="failed_retryable",
                error=f"unsupported mutation operation {provider}.{operation}; deferring",
            )
        payload = _mapping(mutation.get("payload_json"))
        try:
            if operation == APPLE_CONTACTS_CREATE_CONTACT_OPERATION:
                return self._create(payload)
            if operation == APPLE_CONTACTS_UPDATE_CONTACT_OPERATION:
                return self._update(payload)
            return self._merge(payload)
        except subprocess.TimeoutExpired:
            return AppleContactsMutationResult(
                status="failed_retryable",
                error=f"Contacts.app did not answer within {DEFAULT_SCRIPT_TIMEOUT_SECONDS}s",
            )
        except (AppleContactsCardNotFound, AppleContactsInvalidMutation) as error:
            return AppleContactsMutationResult(status="failed_terminal", error=str(error))
        except RuntimeError as error:
            status, message = classify_osascript_error(str(error), app_name="Contacts.app")
            return AppleContactsMutationResult(status=status, error=message)

    # -- create ---------------------------------------------------------------------

    def _create(self, payload: Mapping[str, Any]) -> AppleContactsMutationResult:
        contact = _mapping(payload.get("contact"))
        properties = []
        for key, prop in SCALAR_FIELDS:
            value = str(contact.get(key) or "").strip()
            if value:
                properties.append(f"{prop}:{applescript_string(value)}")
        if not properties:
            raise AppleContactsInvalidMutation("create_contact needs at least a name or an organization")
        lines = [f"set newPerson to make new person with properties {{{', '.join(properties)}}}"]
        # Setting organization inside the make-properties record is not reliable on
        # every macOS release; set it explicitly as well.
        for key, prop in SCALAR_FIELDS:
            value = str(contact.get(key) or "").strip()
            if value and key in ("organization", "job_title", "department"):
                lines.append(f"set {prop} of newPerson to {applescript_string(value)}")
        note = str(contact.get("note") or "")
        if note.strip():
            lines.append(f"set note of newPerson to {applescript_string(note)}")
        for key, element in LIST_FIELDS:
            seen: set[str] = set()
            for entry in _entries(contact.get(key)):
                dedupe = _value_key(key, entry["value"])
                if dedupe in seen:
                    continue
                seen.add(dedupe)
                lines.append(_make_line("newPerson", key, element, entry))
        script = f"""
        with timeout of {IN_SCRIPT_TIMEOUT_SECONDS} seconds
        tell application "Contacts"
        {chr(10).join(lines)}
        save
        return (id of newPerson) & (ASCII character 31) & (name of newPerson)
        end tell
        end timeout
        """
        card_id, name = _split_result(self._runner(dedent_script(script)))
        return AppleContactsMutationResult(
            status="succeeded",
            result_json={"card_id": card_id, "name": name, "action": "create"},
        )

    # -- update ---------------------------------------------------------------------

    def _update(self, payload: Mapping[str, Any]) -> AppleContactsMutationResult:
        card_id = str(payload.get("card_id") or "").strip()
        if not card_id:
            raise AppleContactsInvalidMutation("update_contact needs card_id")
        contact = _mapping(payload.get("contact"))
        remove = _mapping(payload.get("remove"))
        redirected_from = ""
        try:
            current = self._read(card_id)
        except RuntimeError as error:
            if not is_card_not_found(error):
                raise
            survivor = self._surviving_card(card_id)
            if survivor is None:
                raise
            redirected_from, card_id = card_id, survivor
            current = self._read(card_id)

        provenance = {"redirected_from": redirected_from} if redirected_from else {}
        lines, added, removed, note_changed = _apply_changes("p", current, contact, remove)
        changed = bool(lines)
        if not changed:
            return AppleContactsMutationResult(
                status="succeeded",
                result_json={
                    "card_id": card_id,
                    "action": "update",
                    "changed": False,
                    "previous_card": current,
                    **provenance,
                },
            )
        card_id_out, name = self._write(card_id, lines)
        return AppleContactsMutationResult(
            status="succeeded",
            result_json={
                "card_id": card_id_out or card_id,
                "name": name,
                "action": "update",
                "changed": True,
                "added": added,
                "removed": removed,
                "note_changed": note_changed,
                "previous_card": current,
                **provenance,
            },
        )

    # -- merge ----------------------------------------------------------------------

    def _merge(self, payload: Mapping[str, Any]) -> AppleContactsMutationResult:
        keep_id = str(payload.get("keep_card_id") or "").strip()
        merge_ids = [str(x).strip() for x in (payload.get("merge_card_ids") or []) if str(x).strip()]
        if not keep_id or not merge_ids:
            raise AppleContactsInvalidMutation("merge_contacts needs keep_card_id and at least one merge_card_ids entry")
        if keep_id in merge_ids:
            raise AppleContactsInvalidMutation("merge_card_ids must not contain keep_card_id")
        overrides = _mapping(payload.get("contact"))

        keep = self._read(keep_id)
        # A card an earlier merge already folded into keep_id is done, not an error; one
        # folded into some OTHER card is a conflict a human has to look at.
        others: list[dict[str, Any]] = []
        to_delete: list[str] = []
        already_merged: list[str] = []
        for other in merge_ids:
            try:
                others.append(self._read(other))
            except RuntimeError as error:
                if not is_card_not_found(error):
                    raise
                survivor = self._surviving_card(other)
                if survivor == keep_id:
                    already_merged.append(other)
                    continue
                if survivor:
                    raise AppleContactsInvalidMutation(
                        f"card {other} was already merged into {survivor}, not into {keep_id}; "
                        f"Contacts.app said: {error}"
                    )
                raise
            to_delete.append(other)

        # The union: every list value the kept card lacks, every scalar it has empty.
        union: dict[str, Any] = {key: [] for key, _e in LIST_FIELDS}
        for other in others:
            for key, _prop in SCALAR_FIELDS:
                if not str(keep.get(key) or "").strip() and str(other.get(key) or "").strip() and not str(overrides.get(key) or "").strip():
                    union[key] = other[key]
                    keep[key + "__filled"] = True
            for key, _e in LIST_FIELDS:
                union[key].extend(other.get(key) or [])
            other_note = str(other.get("note") or "").strip()
            if other_note and other_note not in str(keep.get("note") or ""):
                union.setdefault("append_note", "")
                union["append_note"] = (union["append_note"] + "\n" + other_note).strip("\n")
        for key, value in overrides.items():
            if key in ("emails", "phones", "urls"):
                union[key].extend(_entries(value))
            elif value not in (None, ""):
                union[key] = value
        keep_clean = {k: v for k, v in keep.items() if not k.endswith("__filled")}

        lines, added, removed, note_changed = _apply_changes("p", keep_clean, union, {})
        for other in to_delete:
            lines.append(f"delete person id {applescript_string(other)}")
        card_id_out, name = self._write(keep_id, lines)
        return AppleContactsMutationResult(
            status="succeeded",
            result_json={
                "card_id": card_id_out or keep_id,
                "name": name,
                "action": "merge",
                "added": added,
                "note_changed": note_changed,
                "deleted_card_ids": to_delete,
                "already_merged_card_ids": already_merged,
                "previous_cards": [keep_clean, *others],
            },
        )

    # -- plumbing -------------------------------------------------------------------

    def _read(self, card_id: str) -> dict[str, Any]:
        return parse_card_dump(self._runner(dedent_script(_read_script(card_id))))

    def _write(self, card_id: str, lines: list[str]) -> tuple[str, str]:
        script = f"""
        with timeout of {IN_SCRIPT_TIMEOUT_SECONDS} seconds
        tell application "Contacts"
        set p to person id {applescript_string(card_id)}
        {chr(10).join(lines)}
        save
        return (id of p) & (ASCII character 31) & (name of p)
        end tell
        end timeout
        """
        return _split_result(self._runner(dedent_script(script)))


def _make_line(target: str, key: str, element: str, entry: Mapping[str, str]) -> str:
    label = str(entry.get("label") or "").strip()
    props = []
    if label:
        props.append(f"label:{applescript_string(label)}")
    props.append(f"value:{applescript_string(entry['value'])}")
    return f"make new {element} at end of {key} of {target} with properties {{{', '.join(props)}}}"


def _apply_changes(
    target: str,
    current: Mapping[str, Any],
    contact: Mapping[str, Any],
    remove: Mapping[str, Any],
) -> tuple[list[str], dict[str, list[str]], dict[str, list[str]], bool]:
    lines: list[str] = []
    for key, prop in SCALAR_FIELDS:
        value = str(contact.get(key) or "").strip()
        if value and value != str(current.get(key) or "").strip():
            lines.append(f"set {prop} of {target} to {applescript_string(value)}")

    note_changed = False
    note = contact.get("note")
    append_note = str(contact.get("append_note") or "")
    if note not in (None, "") and str(note) != str(current.get("note") or ""):
        lines.append(f"set note of {target} to {applescript_string(str(note))}")
        note_changed = True
    elif append_note.strip():
        existing = str(current.get("note") or "")
        combined = append_note if not existing else existing + "\n" + append_note
        lines.append(f"set note of {target} to {applescript_string(combined)}")
        note_changed = True

    removed: dict[str, list[str]] = {}
    for key, element in LIST_FIELDS:
        removed[key] = []
        wanted = {_value_key(key, v): str(v) for v in (remove.get(key) or []) if str(v).strip()}
        for entry in current.get(key) or []:
            dedupe = _value_key(key, entry.get("value", ""))
            if dedupe in wanted:
                lines.append(f"delete (every {element} of {target} whose value is {applescript_string(entry['value'])})")
                removed[key].append(entry["value"])

    added: dict[str, list[str]] = {}
    for key, element in LIST_FIELDS:
        added[key] = []
        have = {_value_key(key, e.get("value", "")) for e in (current.get(key) or [])}
        have -= {_value_key(key, v) for v in removed[key]}
        for entry in _entries(contact.get(key)):
            dedupe = _value_key(key, entry["value"])
            if dedupe in have:
                continue
            have.add(dedupe)
            lines.append(_make_line(target, key, element, entry))
            added[key].append(entry["value"])
    return lines, added, removed, note_changed


def _split_result(raw: str) -> tuple[str, str]:
    parts = str(raw or "").split(FIELD_SEPARATOR, 1)
    return parts[0].strip(), (parts[1].strip() if len(parts) > 1 else "")


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}
