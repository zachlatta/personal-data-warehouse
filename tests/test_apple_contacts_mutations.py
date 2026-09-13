from __future__ import annotations

import pytest

from personal_data_warehouse.apple_contacts_mutations import (
    APPLE_CONTACTS_CREATE_CONTACT_OPERATION,
    APPLE_CONTACTS_MERGE_CONTACTS_OPERATION,
    APPLE_CONTACTS_PROVIDER,
    APPLE_CONTACTS_UPDATE_CONTACT_OPERATION,
    FIELD_SEPARATOR,
    RECORD_SEPARATOR,
    AppleContactsMutationExecutor,
    normalize_label,
    parse_card_dump,
)


def _dump(card_id, first, last, org="", title="", note="", emails=(), phones=(), urls=()):
    head = FIELD_SEPARATOR.join(["card", card_id, first, last, "", "", org, title, "", note])
    records = [head]
    for label, value in emails:
        records.append(FIELD_SEPARATOR.join(["email", label, value]))
    for label, value in phones:
        records.append(FIELD_SEPARATOR.join(["phone", label, value]))
    for label, value in urls:
        records.append(FIELD_SEPARATOR.join(["url", label, value]))
    return RECORD_SEPARATOR.join(records)


class _FakeRunner:
    def __init__(self, results):
        self.results = list(results)
        self.scripts = []

    def __call__(self, script: str) -> str:
        self.scripts.append(script)
        return self.results.pop(0)


def _mutation(operation, payload):
    return {
        "id": "mut-1",
        "provider": APPLE_CONTACTS_PROVIDER,
        "operation": operation,
        "account": "you@example.com",
        "payload_json": payload,
    }


KEEP = "8537DF38-BF0D-4468-9061-D2D41468E05A:ABPerson"
OTHER = "44C1F82A-0000-4468-9061-D2D41468E05A:ABPerson"


def test_normalize_label_strips_the_contacts_wrapper():
    assert normalize_label("_$!<Work>!$_") == "work"
    assert normalize_label("_$!<Mobile>!$_") == "mobile"
    assert normalize_label("Outlook") == "Outlook"
    assert normalize_label("missing value") == ""


def test_parse_card_dump_reads_scalars_and_lists():
    card = parse_card_dump(
        _dump(KEEP, "Melanie", "Smith", org="Hack Club", note="hi",
              emails=[("_$!<Home>!$_", "melanie@hackclub.com")], phones=[("_$!<Other>!$_", "(413) 552-8582")])
    )
    assert card["card_id"] == KEEP
    assert card["given_name"] == "Melanie"
    assert card["organization"] == "Hack Club"
    assert card["emails"] == [{"label": "home", "value": "melanie@hackclub.com"}]
    assert card["phones"] == [{"label": "other", "value": "(413) 552-8582"}]
    assert card["urls"] == []


def test_create_contact_builds_the_card_and_returns_its_id():
    runner = _FakeRunner([f"NEW-ID:ABPerson{FIELD_SEPARATOR}Rebeka Lawrence-Gomez"])
    executor = AppleContactsMutationExecutor(runner=runner)

    result = executor.execute(_mutation(APPLE_CONTACTS_CREATE_CONTACT_OPERATION, {
        "contact": {
            "given_name": "Rebeka", "family_name": "Lawrence-Gomez", "organization": "Hack Club",
            "job_title": "Deputy to the Founder",
            "emails": [{"label": "work", "value": "rebeka@hackclub.com"}],
            "phones": [{"label": "mobile", "value": "+18027528709"}],
        }
    }))

    assert result.status == "succeeded"
    assert result.result_json["card_id"] == "NEW-ID:ABPerson"
    assert result.result_json["action"] == "create"
    script = runner.scripts[0]
    assert 'make new person with properties {first name:"Rebeka", last name:"Lawrence-Gomez"' in script
    assert 'make new email at end of emails of newPerson with properties {label:"work", value:"rebeka@hackclub.com"}' in script
    assert 'set organization of newPerson to "Hack Club"' in script
    assert script.count("\nsave\n") == 1


def test_create_contact_escapes_quotes_in_every_value():
    runner = _FakeRunner([f"X:ABPerson{FIELD_SEPARATOR}n"])
    AppleContactsMutationExecutor(runner=runner).execute(_mutation(APPLE_CONTACTS_CREATE_CONTACT_OPERATION, {
        "contact": {"given_name": 'Ann "Quotes"', "organization": "O", "note": 'say "hi"'}
    }))
    assert 'first name:"Ann \\"Quotes\\""' in runner.scripts[0]
    assert 'set note of newPerson to "say \\"hi\\""' in runner.scripts[0]


def test_update_contact_adds_only_values_the_card_lacks_and_records_the_previous_card():
    runner = _FakeRunner([
        _dump(KEEP, "Melanie", "Smith", emails=[("_$!<Home>!$_", "melanie@hackclub.com")]),
        f"{KEEP}{FIELD_SEPARATOR}Melanie Smith",
    ])
    executor = AppleContactsMutationExecutor(runner=runner)

    result = executor.execute(_mutation(APPLE_CONTACTS_UPDATE_CONTACT_OPERATION, {
        "card_id": KEEP,
        "contact": {
            "organization": "Hack Club", "job_title": "Director of Operations, HCB",
            "emails": [{"label": "work", "value": "Melanie@HackClub.com"}, {"label": "home", "value": "mel@example.com"}],
        },
    }))

    assert result.status == "succeeded"
    assert result.result_json["previous_card"]["given_name"] == "Melanie"
    assert result.result_json["added"] == {"emails": ["mel@example.com"], "phones": [], "urls": []}
    write = runner.scripts[1]
    assert f'person id "{KEEP}"' in write
    assert 'set organization of p to "Hack Club"' in write
    assert 'value:"mel@example.com"' in write
    assert "Melanie@HackClub.com" not in write  # already on the card, case-insensitively


def test_update_contact_can_remove_a_value_and_append_to_the_note():
    runner = _FakeRunner([
        _dump(KEEP, "Max", "Wofford", note="old note", emails=[("_$!<Work>!$_", "max@hackedu.us")]),
        f"{KEEP}{FIELD_SEPARATOR}Max Wofford",
    ])
    executor = AppleContactsMutationExecutor(runner=runner)

    result = executor.execute(_mutation(APPLE_CONTACTS_UPDATE_CONTACT_OPERATION, {
        "card_id": KEEP,
        "contact": {"append_note": "Left HQ 2026"},
        "remove": {"emails": ["max@hackedu.us"]},
    }))

    assert result.status == "succeeded"
    write = runner.scripts[1]
    assert 'delete (every email of p whose value is "max@hackedu.us")' in write
    assert 'set note of p to "old note" & linefeed & "Left HQ 2026"' in write
    assert result.result_json["removed"] == {"emails": ["max@hackedu.us"], "phones": [], "urls": []}


def test_update_contact_with_nothing_new_still_succeeds_without_writing():
    runner = _FakeRunner([_dump(KEEP, "Max", "Wofford", emails=[("_$!<Work>!$_", "max@hackclub.com")])])
    result = AppleContactsMutationExecutor(runner=runner).execute(_mutation(APPLE_CONTACTS_UPDATE_CONTACT_OPERATION, {
        "card_id": KEEP, "contact": {"emails": [{"label": "work", "value": "max@hackclub.com"}]},
    }))
    assert result.status == "succeeded"
    assert result.result_json["changed"] is False
    assert len(runner.scripts) == 1


def test_merge_contacts_unions_the_lists_onto_the_kept_card_and_deletes_the_rest():
    runner = _FakeRunner([
        _dump(KEEP, "Katie", "Latta", emails=[("_$!<Home>!$_", "katie@example.com")]),
        _dump(OTHER, "Katie", "Latta", org="Acme", phones=[("_$!<Mobile>!$_", "(310) 414-7928")]),
        f"{KEEP}{FIELD_SEPARATOR}Katie Latta",
    ])
    executor = AppleContactsMutationExecutor(runner=runner)

    result = executor.execute(_mutation(APPLE_CONTACTS_MERGE_CONTACTS_OPERATION, {
        "keep_card_id": KEEP, "merge_card_ids": [OTHER],
    }))

    assert result.status == "succeeded"
    write = runner.scripts[2]
    assert 'make new phone at end of phones of p with properties {label:"mobile", value:"(310) 414-7928"}' in write
    assert 'set organization of p to "Acme"' in write  # a scalar the kept card lacked
    assert f'delete person id "{OTHER}"' in write
    assert write.index("delete person id") < write.index("\nsave")
    assert [c["card_id"] for c in result.result_json["previous_cards"]] == [KEEP, OTHER]
    assert result.result_json["deleted_card_ids"] == [OTHER]


def test_merge_contacts_applies_field_overrides_on_top_of_the_union():
    runner = _FakeRunner([
        _dump(KEEP, "Rebeka", "Hack Club"),
        _dump(OTHER, "Rebeka", "Lawrence-Gomez", phones=[("_$!<Mobile>!$_", "+18027528709")]),
        f"{KEEP}{FIELD_SEPARATOR}Rebeka Lawrence-Gomez",
    ])
    result = AppleContactsMutationExecutor(runner=runner).execute(_mutation(APPLE_CONTACTS_MERGE_CONTACTS_OPERATION, {
        "keep_card_id": KEEP, "merge_card_ids": [OTHER], "contact": {"family_name": "Lawrence-Gomez"},
    }))
    assert result.status == "succeeded"
    assert 'set last name of p to "Lawrence-Gomez"' in runner.scripts[2]


def test_a_missing_card_is_terminal_not_retryable():
    runner = _FakeRunner([RuntimeError("Contacts got an error: Can’t get person id \"nope\". (-1728)")])

    class Raising(_FakeRunner):
        def __call__(self, script):
            raise self.results.pop(0)

    result = AppleContactsMutationExecutor(runner=Raising([RuntimeError("Can’t get person id \"nope\". (-1728)")])).execute(
        _mutation(APPLE_CONTACTS_UPDATE_CONTACT_OPERATION, {"card_id": "nope:ABPerson", "contact": {"organization": "x"}})
    )
    assert result.status == "failed_terminal"


def test_a_denied_automation_grant_is_blocked_not_failed():
    class Raising:
        def __call__(self, script):
            raise RuntimeError("Not authorized to send Apple events to Contacts. (-1743)")

    result = AppleContactsMutationExecutor(runner=Raising()).execute(
        _mutation(APPLE_CONTACTS_UPDATE_CONTACT_OPERATION, {"card_id": KEEP, "contact": {"organization": "x"}})
    )
    assert result.status == "blocked_missing_credentials"
    assert "Contacts.app" in result.error


def test_an_apple_event_timeout_is_retryable():
    class Raising:
        def __call__(self, script):
            raise RuntimeError("Contacts got an error: AppleEvent timed out. (-1712)")

    result = AppleContactsMutationExecutor(runner=Raising()).execute(
        _mutation(APPLE_CONTACTS_UPDATE_CONTACT_OPERATION, {"card_id": KEEP, "contact": {"organization": "x"}})
    )
    assert result.status == "failed_retryable"


def test_a_foreign_provider_is_left_for_another_worker():
    result = AppleContactsMutationExecutor(runner=_FakeRunner([])).execute(
        {"id": "m", "provider": "gmail", "operation": "gmail.send_email", "payload_json": {}}
    )
    assert result.status == "failed_retryable"


def test_merge_refuses_to_delete_the_kept_card():
    result = AppleContactsMutationExecutor(runner=_FakeRunner([])).execute(_mutation(
        APPLE_CONTACTS_MERGE_CONTACTS_OPERATION, {"keep_card_id": KEEP, "merge_card_ids": [KEEP]}
    ))
    assert result.status == "failed_terminal"
