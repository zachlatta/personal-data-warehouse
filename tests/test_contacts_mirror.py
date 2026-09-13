from __future__ import annotations

import json

from personal_data_warehouse.contacts_mirror import (
    GOOGLE_ACCOUNT_DEFAULT,
    MirrorPlan,
    plan_mirror,
    proposal_payloads,
)


def gcard(cid, given="", family="", emails=(), phones=(), org="", title="", etag="e1"):
    return {
        "account": GOOGLE_ACCOUNT_DEFAULT, "card_id": cid, "etag": etag, "given_name": given, "family_name": family,
        "display_name": (given + " " + family).strip(), "organization": org, "job_title": title,
        "emails": json.dumps([{"value": v, "type": t} for t, v in emails]),
        "phones": json.dumps([{"value": v, "type": t, "canonicalForm": v} for t, v in phones]),
        "source_updated_at": "2026-09-01T00:00:00Z", "notes": "",
    }


def acard(cid, given="", family="", emails=(), phones=(), org="", title="", image=None):
    return {
        "card_id": cid, "given_name": given, "family_name": family, "display_name": (given + " " + family).strip(),
        "organization": org, "job_title": title,
        "emails": json.dumps([{"value": v, "label": t} for t, v in emails]),
        "phones": json.dumps([{"value": v, "label": t, "canonicalForm": v} for t, v in phones]),
        "notes": "", "image_type": image, "source_updated_at": "2026-09-01T00:00:00Z",
    }


def test_a_google_card_with_no_icloud_twin_is_created_in_icloud():
    plan = plan_mirror([gcard("people/1", "Ada", "Lovelace", emails=[("work", "ada@example.test")], org="Hack Club", title="Cofounder")], [])
    assert [m["type"] for m in plan.apple_mutations] == ["apple_contacts.create_contact"]
    contact = plan.apple_mutations[0]["contact"]
    assert contact["given_name"] == "Ada" and contact["family_name"] == "Lovelace"
    assert contact["organization"] == "Hack Club" and contact["job_title"] == "Cofounder"
    assert contact["emails"] == [{"label": "work", "value": "ada@example.test"}]
    assert plan.google_operations == []


def test_google_wins_names_and_org_and_both_sides_gain_the_other_sides_points():
    google = [gcard("people/1", "Maxwell", "Hurley", emails=[("home", "max@example.test")], org="Hack Club")]
    apple = [acard("A:ABPerson", "Max", "Hurley", emails=[("home", "max@example.test")], phones=[("mobile", "+18025550100")])]
    plan = plan_mirror(google, apple)
    [upd] = plan.apple_mutations
    assert upd["type"] == "apple_contacts.update_contact" and upd["card_id"] == "A:ABPerson"
    assert upd["contact"] == {"given_name": "Maxwell", "family_name": "Hurley", "organization": "Hack Club"}
    [gop] = plan.google_operations
    assert gop["op"] == "update_contact" and gop["resource_name"] == "people/1" and gop["etag"] == "e1"
    # the Google card keeps its own name and gains the phone iCloud had; emails are the union
    assert "names" not in gop["person"]
    assert gop["person"]["phoneNumbers"] == [{"value": "+18025550100", "type": "mobile"}]
    assert gop["person"]["emailAddresses"] == [{"value": "max@example.test", "type": "home"}]


def test_a_google_card_with_no_usable_name_takes_icloud_name_instead_of_erasing_it():
    google = [gcard("people/1", emails=[("", "ada@example.test")])]
    apple = [acard("A:ABPerson", "Ada", "Lovelace", emails=[("home", "ada@example.test")])]
    plan = plan_mirror(google, apple)
    assert plan.apple_mutations == []
    [gop] = plan.google_operations
    assert gop["person"]["names"] == [{"givenName": "Ada", "familyName": "Lovelace"}]


def test_identical_pairs_produce_nothing():
    google = [gcard("people/1", "Ada", "Lovelace", emails=[("work", "ada@example.test")], phones=[("mobile", "+1 (802) 555-0100")])]
    apple = [acard("A:ABPerson", "Ada", "Lovelace", emails=[("work", "ADA@example.test")], phones=[("mobile", "+18025550100")])]
    plan = plan_mirror(google, apple)
    assert plan.apple_mutations == [] and plan.google_operations == []
    assert plan.is_empty


def test_an_icloud_card_with_no_google_twin_is_created_in_google():
    plan = plan_mirror([], [acard("A:ABPerson", "Ada", "Lovelace", emails=[("work", "ada@example.test")], org="Hack Club")])
    [gop] = plan.google_operations
    assert gop["op"] == "create_contact"
    assert gop["person"]["names"] == [{"givenName": "Ada", "familyName": "Lovelace"}]
    assert gop["person"]["organizations"] == [{"name": "Hack Club"}]
    assert plan.apple_mutations == []


def test_two_icloud_cards_for_one_google_card_are_merged_into_the_fullest():
    google = [gcard("people/1", "Ada", "Lovelace", emails=[("work", "ada@example.test")], phones=[("mobile", "+18025550100")])]
    apple = [
        acard("A:ABPerson", "Ada", "Lovelace", emails=[("work", "ada@example.test")]),
        acard("B:ABPerson", "Ada", "Lovelace", phones=[("mobile", "+18025550100")], image="PHOTO"),
    ]
    plan = plan_mirror(google, apple)
    merges = [m for m in plan.apple_mutations if m["type"] == "apple_contacts.merge_contacts"]
    assert merges == [{"type": "apple_contacts.merge_contacts", "account": plan.apple_account, "keep_card_id": "B:ABPerson", "merge_card_ids": ["A:ABPerson"]}]
    assert not [m for m in plan.apple_mutations if m["type"] != "apple_contacts.merge_contacts"]


def test_google_duplicates_are_folded_into_the_fullest_google_card():
    google = [
        gcard("people/1", "Ada", "Lovelace", emails=[("work", "ada@example.test")], etag="e1"),
        gcard("people/2", "Ada", "Lovelace", emails=[("work", "ada@example.test")], phones=[("mobile", "+18025550100")], etag="e2"),
    ]
    plan = plan_mirror(google, [acard("A:ABPerson", "Ada", "Lovelace", emails=[("work", "ada@example.test")], phones=[("mobile", "+18025550100")])])
    ops = plan.google_operations
    assert [o["op"] for o in ops] == ["update_contact", "delete_contact"]
    assert ops[0]["resource_name"] == "people/2" and ops[1]["resource_name"] == "people/1" and ops[1]["etag"] == "e1"


def test_junk_and_nameless_cards_are_left_alone():
    google = [gcard("people/1", emails=[("", "reply+abc@reply.github.com")]), gcard("people/2", emails=[("", "logins+x@hackclub.com")]), gcard("people/3", phones=[("", "+18025550199")])]
    apple = [acard("A:ABPerson", emails=[("", "noreply@noreply.github.com")])]
    plan = plan_mirror(google, apple)
    assert plan.is_empty


def test_family_members_sharing_a_number_are_never_merged():
    google = [gcard("people/1", "Justin", "Duong", phones=[("home", "+14155551234")]), gcard("people/2", "Chloe", "Duong", phones=[("home", "+14155551234")])]
    apple = [acard("A:ABPerson", "Justin", "Duong", phones=[("home", "+14155551234")]), acard("B:ABPerson", "Chloe", "Duong", phones=[("home", "+14155551234")])]
    plan = plan_mirror(google, apple)
    assert not [o for o in plan.google_operations if o["op"] == "delete_contact"]
    assert not [m for m in plan.apple_mutations if m["type"] == "apple_contacts.merge_contacts"]


def test_proposal_payloads_split_apple_and_google_and_are_empty_when_nothing_changed():
    plan = MirrorPlan(apple_account="a@example.test", google_account="g@example.test", apple_mutations=[{"type": "apple_contacts.create_contact", "account": "a@example.test", "contact": {"given_name": "A"}}], google_operations=[{"op": "create_contact", "person": {}}])
    payloads = proposal_payloads(plan, run_date="2026-09-13")
    assert [p["title"][:len("Contacts mirror")] for p in payloads] == ["Contacts mirror", "Contacts mirror"]
    assert payloads[0]["mutations"] == plan.apple_mutations
    assert payloads[1]["mutations"] == [{"type": "google_people.contacts", "account": "g@example.test", "operations": plan.google_operations}]
    assert proposal_payloads(MirrorPlan("a", "g", [], []), run_date="2026-09-13") == []
