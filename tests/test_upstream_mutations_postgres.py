from __future__ import annotations

from datetime import UTC, datetime
import os
import uuid

import pytest
from dotenv import load_dotenv

from personal_data_warehouse.clickhouse import CONTACT_CARD_COLUMNS, MESSAGE_COLUMNS
from personal_data_warehouse.postgres import (
    ARRAY_COLUMNS,
    FLOAT_COLUMNS,
    INTEGER_COLUMNS,
    TIMESTAMP_COLUMNS,
    PostgresWarehouse,
)


def _postgres_url() -> str:
    load_dotenv()
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if not url:
        pytest.skip("POSTGRES_DATABASE_URL is not set")
    return url


@pytest.fixture()
def warehouse():
    schema = "pdw_test_" + uuid.uuid4().hex
    wh = PostgresWarehouse(_postgres_url(), schema=schema)
    try:
        yield wh
    finally:
        wh._command(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        wh.close()


def test_upstream_mutation_request_validation_idempotency_and_review(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()
    warehouse.insert_messages(
        [
            _message_row(message_id="m1", thread_id="thread-1", subject="One", labels=["INBOX"], sync_version=1),
            _message_row(message_id="m2", thread_id="thread-2", subject="Two", labels=["INBOX"], sync_version=1),
        ]
    )

    with pytest.raises(ValueError, match="thread_ids"):
        warehouse.propose_gmail_archive_threads(account="zach@example.test", thread_ids=[], reason="clear mail")
    with pytest.raises(ValueError, match="unknown or non-inbox"):
        warehouse.propose_gmail_archive_threads(account="zach@example.test", thread_ids=["missing"], reason="clear mail")

    mutation = warehouse.propose_gmail_archive_threads(
        account="zach@example.test",
        thread_ids=["thread-1", "thread-2", "thread-1"],
        reason="clear mail",
        title="Archive old threads",
        context={"source": "unit-test"},
    )
    duplicate = warehouse.propose_gmail_archive_threads(
        account="zach@example.test",
        thread_ids=["thread-2", "thread-1"],
        reason="duplicate proposal",
    )

    assert duplicate["id"] == mutation["id"]
    assert mutation["id"].startswith("req_")
    assert mutation["status"] == "pending_review"
    assert len(mutation["mutations"]) == 2
    assert [child["payload_json"]["thread_ids"] for child in mutation["mutations"]] == [["thread-1"], ["thread-2"]]
    assert [child["preview_json"]["thread_count"] for child in mutation["mutations"]] == [1, 1]

    edited = warehouse.remove_upstream_mutation_from_request(
        request_id=mutation["id"],
        mutation_id=mutation["mutations"][0]["id"],
        actor_id="zach",
    )
    assert [child["status"] for child in edited["mutations"]] == ["rejected", "pending_review"]
    assert edited["revision"] == 2

    approved = warehouse.approve_upstream_mutation_request(mutation["id"], actor_id="zach")
    assert approved["status"] == "approved"
    assert [child["status"] for child in approved["mutations"]] == ["rejected", "approved"]

    with pytest.raises(ValueError, match="cannot edit"):
        warehouse.remove_upstream_mutation_from_request(
            request_id=mutation["id"],
            mutation_id=mutation["mutations"][1]["id"],
            actor_id="zach",
        )

    events = warehouse.list_upstream_mutation_request_events(mutation["id"])
    assert [event["event_index"] for event in events] == [0, 1, 2]
    assert [event["event_type"] for event in events] == ["created", "mutation_removed", "approved"]


def test_request_rejection_logs_child_mutation_events(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()
    warehouse.insert_messages(
        [
            _message_row(message_id="m1", thread_id="thread-1", subject="One", labels=["INBOX"], sync_version=1),
            _message_row(message_id="m2", thread_id="thread-2", subject="Two", labels=["INBOX"], sync_version=1),
        ]
    )
    request = warehouse.propose_gmail_archive_threads(
        account="zach@example.test",
        thread_ids=["thread-1", "thread-2"],
        reason="clear mail",
    )

    warehouse.reject_upstream_mutation_request(request["id"], actor_id="zach", reason="not today")

    for mutation in request["mutations"]:
        events = warehouse.list_upstream_mutation_events(mutation["id"])
        assert [event["event_type"] for event in events] == ["created", "rejected"]
        assert events[-1]["event_json"] == {"request_id": request["id"], "reason": "not today"}


def test_upstream_mutation_request_can_span_gmail_and_contacts(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()
    warehouse.ensure_contacts_tables()
    warehouse.insert_messages(
        [
            _message_row(message_id="m1", thread_id="thread-1", subject="One", labels=["INBOX"], sync_version=1),
        ]
    )
    warehouse.insert_contact_cards(
        [
            _contact_card_row(card_id="people/update", etag="etag-update", display_name="Update Me"),
            _contact_card_row(card_id="people/delete", etag="etag-delete", display_name="Delete Me"),
        ]
    )

    request = warehouse.propose_upstream_mutation_request(
        title="Morning cleanup",
        reason="clear inbox and clean contacts",
        requested_by="agent-test",
        mutations=[
            {
                "type": "gmail.archive_threads",
                "account": "zach@example.test",
                "thread_ids": ["thread-1"],
            },
            {
                "type": "google_people.contacts",
                "account": "zach@example.test",
                "operations": [
                    {
                        "op": "update_contact",
                        "client_op_id": "update-1",
                        "resource_name": "people/update",
                        "etag": "etag-update",
                        "update_person_fields": ["names"],
                        "person": {"names": [{"displayName": "Updated Person", "givenName": "Updated"}]},
                    },
                    {
                        "op": "delete_contact",
                        "client_op_id": "delete-1",
                        "resource_name": "people/delete",
                        "etag": "etag-delete",
                        "reason": "duplicate",
                    },
                ],
            },
        ],
    )

    assert request["title"] == "Morning cleanup"
    assert request["status"] == "pending_review"
    assert [child["provider"] for child in request["mutations"]] == ["gmail", "google_people", "google_people"]
    assert [child["operation"] for child in request["mutations"]] == [
        "gmail.archive_threads",
        "contacts.batch_mutation",
        "contacts.batch_mutation",
    ]
    assert request["mutations"][1]["payload_json"]["operations"][0]["op"] == "update_contact"
    assert request["mutations"][2]["payload_json"]["operations"][0]["op"] == "delete_contact"

    listed = warehouse.list_upstream_mutation_requests(limit=10)
    assert [row["id"] for row in listed] == [request["id"]]
    assert [row["id"] for row in warehouse.list_upstream_mutations_for_request(request["id"])] == [
        child["id"] for child in request["mutations"]
    ]


def test_upstream_mutation_claim_fail_and_observe_transitions(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()
    warehouse.insert_messages(
        [
            _message_row(message_id="m1", thread_id="thread-1", subject="One", labels=["INBOX"], sync_version=1),
            _message_row(message_id="m2", thread_id="thread-2", subject="Two", labels=["INBOX"], sync_version=1),
        ]
    )

    succeeded_mutation = warehouse.propose_gmail_archive_threads(
        account="zach@example.test",
        thread_ids=["thread-1"],
        reason="clear mail",
    )
    retryable_mutation = warehouse.propose_gmail_archive_threads(
        account="zach@example.test",
        thread_ids=["thread-2"],
        reason="clear mail too",
    )
    warehouse.approve_upstream_mutation_request(succeeded_mutation["id"], actor_id="zach")
    warehouse.approve_upstream_mutation_request(retryable_mutation["id"], actor_id="zach")

    claimed = warehouse.claim_approved_upstream_mutations(limit=10, claimed_by="worker-1")
    assert [row["status"] for row in claimed] == ["executing", "executing"]
    assert {row["request_id"] for row in claimed} == {succeeded_mutation["id"], retryable_mutation["id"]}
    assert warehouse.approved_upstream_mutation_count() == 0

    warehouse.complete_upstream_mutation(
        succeeded_mutation["mutations"][0]["id"],
        result_json={"archived_thread_ids": ["thread-1"]},
        actor_id="worker-1",
    )
    warehouse.fail_upstream_mutation(
        retryable_mutation["mutations"][0]["id"],
        status="failed_retryable",
        error="rate limited",
        result_json={},
        actor_id="worker-1",
    )
    assert warehouse.approved_upstream_mutation_count() == 1

    observed_before_sync = warehouse.observe_succeeded_gmail_archive_mutations()
    assert observed_before_sync == 0

    warehouse.insert_messages(
        [
            _message_row(message_id="m1", thread_id="thread-1", subject="One", labels=[], sync_version=2),
        ]
    )
    observed_after_sync = warehouse.observe_succeeded_gmail_archive_mutations()

    assert observed_after_sync == 1
    assert warehouse.get_upstream_mutation_request(succeeded_mutation["id"])["status"] == "observed"
    assert warehouse.get_upstream_mutation_request(retryable_mutation["id"])["status"] == "failed_retryable"


def test_gmail_unarchive_mutation_validation_and_observe(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()
    warehouse.insert_messages(
        [
            _message_row(message_id="m1", thread_id="archived-thread", subject="Archived", labels=[], sync_version=1),
            _message_row(message_id="m2", thread_id="inbox-thread", subject="Inbox", labels=["INBOX"], sync_version=1),
        ]
    )

    with pytest.raises(ValueError, match="unknown or non-archived"):
        warehouse.propose_upstream_mutation_request(
            title="Bad unarchive",
            reason="not archived",
            mutations=[
                {
                    "type": "gmail.unarchive_threads",
                    "account": "zach@example.test",
                    "thread_ids": ["inbox-thread"],
                }
            ],
        )

    request = warehouse.propose_upstream_mutation_request(
        title="Unarchive one",
        reason="bring it back",
        mutations=[
            {
                "type": "gmail.unarchive_threads",
                "account": "zach@example.test",
                "thread_ids": ["archived-thread"],
            }
        ],
    )

    mutation = request["mutations"][0]
    assert mutation["operation"] == "gmail.unarchive_threads"
    assert mutation["payload_json"] == {"thread_ids": ["archived-thread"], "add_label_ids": ["INBOX"]}
    assert mutation["preview_json"]["thread_count"] == 1

    warehouse.approve_upstream_mutation_request(request["id"], actor_id="zach")
    claimed = warehouse.claim_approved_upstream_mutations(limit=1, claimed_by="worker")
    assert claimed[0]["operation"] == "gmail.unarchive_threads"
    warehouse.complete_upstream_mutation(
        mutation["id"],
        result_json={"unarchived_thread_ids": ["archived-thread"]},
        actor_id="worker",
    )
    assert warehouse.observe_succeeded_gmail_unarchive_mutations() == 0

    warehouse.insert_messages(
        [
            _message_row(
                message_id="m1",
                thread_id="archived-thread",
                subject="Archived",
                labels=["INBOX"],
                sync_version=2,
            ),
        ]
    )
    assert warehouse.observe_succeeded_gmail_unarchive_mutations() == 1
    assert warehouse.get_upstream_mutation_request(request["id"])["status"] == "observed"


def test_gmail_send_email_mutation_proposal_edit_reply_and_observe(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()
    warehouse.insert_messages(
        [
            _message_row(message_id="m1", thread_id="thread-1", subject="Existing thread", labels=["INBOX"], sync_version=1),
        ]
    )

    request = warehouse.propose_upstream_mutation_request(
        title="Send follow-ups",
        reason="agent drafted useful replies",
        mutations=[
            {
                "type": "gmail.send_email",
                "account": "zach@example.test",
                "delivery_mode": "send",
                "message": {
                    "to": ["one@example.test"],
                    "cc": ["two@example.test"],
                    "bcc": ["secret@example.test"],
                    "subject": "New thread",
                    "body_text": "Hello from a new thread",
                },
            },
            {
                "type": "gmail.send_email",
                "account": "zach@example.test",
                "delivery_mode": "draft",
                "message": {
                    "to": ["sender@example.test"],
                    "body_text": "Reply body",
                    "reply_to_thread_id": "thread-1",
                },
            },
        ],
    )

    assert [child["operation"] for child in request["mutations"]] == ["gmail.send_email", "gmail.send_email"]
    new_message = request["mutations"][0]["payload_json"]["message"]
    reply_message = request["mutations"][1]["payload_json"]["message"]
    assert new_message["bcc"] == ["secret@example.test"]
    assert reply_message["subject"] == "Re: Existing thread"
    assert reply_message["reply_to_thread_id"] == "thread-1"
    assert reply_message["in_reply_to"] == "<m1@example.test>"
    assert request["mutations"][1]["preview_json"]["email"]["mode"] == "reply"

    edited = warehouse.update_gmail_email_mutation(
        mutation_id=request["mutations"][0]["id"],
        delivery_mode="draft",
        message={
            **new_message,
            "body_text": "Edited body",
            "cc": ["edited-cc@example.test"],
        },
        actor_id="zach",
    )
    assert edited["payload_json"]["delivery_mode"] == "draft"
    assert edited["payload_json"]["message"]["body_text"] == "Edited body"
    assert edited["preview_json"]["email"]["cc"] == ["edited-cc@example.test"]
    assert edited["revision"] == 2

    warehouse.approve_upstream_mutation(request["mutations"][0]["id"], actor_id="zach")
    claimed = warehouse.claim_approved_upstream_mutations(limit=1, claimed_by="worker")
    assert claimed[0]["operation"] == "gmail.send_email"
    warehouse.complete_upstream_mutation(
        request["mutations"][0]["id"],
        result_json={"delivery_mode": "draft", "draft_id": "draft-1", "draft_message_id": "draft-message-1"},
        actor_id="worker",
    )
    assert warehouse.observe_succeeded_gmail_email_mutations() == 0

    warehouse.insert_messages(
        [
            _message_row(
                message_id="draft-message-1",
                thread_id="draft-thread",
                subject="New thread",
                labels=["DRAFT"],
                sync_version=2,
            ),
        ]
    )
    assert warehouse.observe_succeeded_gmail_email_mutations() == 1
    assert warehouse.get_upstream_mutation(request["mutations"][0]["id"])["status"] == "observed"


def test_contact_mutation_proposal_edit_and_observe(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_contacts_tables()
    warehouse.insert_contact_cards(
        [
            _contact_card_row(card_id="people/update", etag="etag-update", display_name="Update Me"),
            _contact_card_row(card_id="people/delete", etag="etag-delete", display_name="Delete Me"),
        ]
    )

    mutation = warehouse.propose_contact_mutations(
        account="zach@example.test",
        title="Clean contacts",
        reason="test contact batch",
        operations=[
            {
                "op": "update_contact",
                "client_op_id": "update-1",
                "resource_name": "people/update",
                "etag": "etag-update",
                "update_person_fields": ["names"],
                "person": {
                    "names": [{"displayName": "Updated Person", "givenName": "Updated"}],
                },
            },
            {
                "op": "delete_contact",
                "client_op_id": "delete-1",
                "resource_name": "people/delete",
                "etag": "etag-delete",
                "reason": "duplicate",
            },
        ],
    )

    assert mutation["status"] == "pending_review"
    assert [child["provider"] for child in mutation["mutations"]] == ["google_people", "google_people"]
    assert mutation["mutations"][0]["preview_json"]["operation_count"] == 1
    assert mutation["mutations"][0]["payload_json"]["operations"][0]["person"]["resourceName"] == "people/update"
    assert mutation["mutations"][0]["payload_json"]["operations"][0]["person"]["etag"] == "etag-update"

    edited = warehouse.remove_upstream_mutation_from_request(
        request_id=mutation["id"],
        mutation_id=mutation["mutations"][1]["id"],
        actor_id="zach",
    )
    assert [child["status"] for child in edited["mutations"]] == ["pending_review", "rejected"]
    assert edited["revision"] == 2
    assert [event["event_type"] for event in warehouse.list_upstream_mutation_request_events(mutation["id"])] == [
        "created",
        "mutation_removed",
    ]

    warehouse.approve_upstream_mutation_request(mutation["id"], actor_id="zach")
    claimed = warehouse.claim_approved_upstream_mutations(limit=1, claimed_by="worker")
    assert claimed[0]["id"] == mutation["mutations"][0]["id"]
    warehouse.complete_upstream_mutation(
        mutation["mutations"][0]["id"],
        result_json={
            "operation_results": [
                {"op": "update_contact", "resource_name": "people/update", "etag": "etag-updated"}
            ]
        },
        actor_id="worker",
    )
    assert warehouse.observe_succeeded_contact_mutations() == 0

    warehouse.insert_contact_cards(
        [
            _contact_card_row(card_id="people/update", etag="etag-updated", display_name="Updated Person", sync_version=2),
        ]
    )
    assert warehouse.observe_succeeded_contact_mutations() == 1
    assert warehouse.get_upstream_mutation_request(mutation["id"])["status"] == "observed"


def _message_row(
    *,
    message_id: str,
    thread_id: str,
    subject: str,
    labels: list[str],
    sync_version: int,
):
    now = datetime(2026, 5, 22, 12, tzinfo=UTC)
    row = _default_row(
        MESSAGE_COLUMNS,
        account="zach@example.test",
        message_id=message_id,
        thread_id=thread_id,
        history_id=sync_version,
        internal_date=now,
        label_ids=labels,
        snippet="snippet",
        subject=subject,
        from_address="sender@example.test",
        to_addresses=["zach@example.test"],
        delivered_to="zach@example.test",
        rfc822_message_id=f"<{message_id}@example.test>",
        date_header="Fri, 22 May 2026 12:00:00 +0000",
        size_estimate=123,
        body_text="body",
        body_markdown="body",
        body_markdown_full="body",
        body_markdown_clean="body",
        payload_json='{"id":"%s"}' % message_id,
        synced_at=now,
        sync_version=sync_version,
    )
    return row


def _contact_card_row(
    *,
    card_id: str,
    etag: str,
    display_name: str,
    sync_version: int = 1,
    is_deleted: int = 0,
):
    now = datetime(2026, 5, 22, 12, tzinfo=UTC)
    row = _default_row(
        CONTACT_CARD_COLUMNS,
        source="google_people",
        account="zach@example.test",
        source_kind="google_contacts",
        address_book_id="people/me",
        card_id=card_id,
        etag=etag,
        source_uid=f"source-{card_id}",
        display_name=display_name,
        given_name=display_name.split(" ")[0],
        primary_email=f"{card_id.replace('/', '-')}@example.test",
        emails=[{"value": f"{card_id.replace('/', '-')}@example.test"}],
        phones=[],
        addresses=[],
        organizations=[],
        urls=[],
        groups=[],
        dates={"birthdays": [], "events": []},
        photos=[],
        is_deleted=is_deleted,
        source_updated_at=now,
        synced_at=now,
        sync_version=sync_version,
        raw_json={
            "resourceName": card_id,
            "etag": etag,
            "names": [{"displayName": display_name, "givenName": display_name.split(" ")[0]}],
            "emailAddresses": [{"value": f"{card_id.replace('/', '-')}@example.test"}],
        },
    )
    return row


def _default_row(columns: tuple[str, ...], **overrides):
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    row = {}
    for column in columns:
        if column in ARRAY_COLUMNS:
            row[column] = []
        elif column in TIMESTAMP_COLUMNS:
            row[column] = epoch
        elif column in INTEGER_COLUMNS:
            row[column] = 0
        elif column in FLOAT_COLUMNS:
            row[column] = 0.0
        else:
            row[column] = ""
    row.update(overrides)
    return row
