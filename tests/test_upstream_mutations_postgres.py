from __future__ import annotations

from collections.abc import Mapping, Sequence
import copy
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
from typing import Any

import pytest
from dotenv import load_dotenv

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.schema import CALENDAR_EVENT_COLUMNS, CONTACT_CARD_COLUMNS, MESSAGE_COLUMNS
from personal_data_warehouse.warehouse_catalog import CATALOG
from personal_data_warehouse.postgres import (
    ARRAY_COLUMNS,
    CALENDAR_CREATE_EVENT_OPERATION,
    CALENDAR_PROVIDER,
    FLOAT_COLUMNS,
    GMAIL_ARCHIVE_OPERATION,
    GMAIL_SEND_EMAIL_OPERATION,
    GMAIL_UNARCHIVE_OPERATION,
    GOOGLE_CONTACTS_BATCH_MUTATION_OPERATION,
    INTEGER_COLUMNS,
    TIMESTAMP_COLUMNS,
    PostgresWarehouse,
    _jsonb_param,
)


ACCOUNT = "zach@example.test"
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
CONTACT_MUTATION_CONTRACT_PATH = Path(__file__).parent / "contracts" / "google_contacts_mutation_operations.json"


def _postgres_url() -> str:
    load_dotenv()
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if not url:
        pytest.skip("POSTGRES_DATABASE_URL is not set")
    return url


@pytest.fixture()
def warehouse():
    schema = make_test_schema()
    wh = PostgresWarehouse(_postgres_url(), schema=schema)
    try:
        yield wh
    finally:
        cleanup_test_warehouse(wh)


def _seed_mutation_request(
    warehouse: PostgresWarehouse,
    *,
    request_id: str,
    title: str,
    mutations: Sequence[Mapping[str, Any]],
    reason: str = "seeded by test",
    status: str = "approved",
    account: str = ACCOUNT,
    requested_by: str = "test",
) -> dict[str, Any]:
    """Insert an upstream mutation request plus its mutation rows.

    Proposal and review are served by the Go app (``app/internal/mutations``); these tests
    cover the Dagster worker's claim/execute/observe path, so they seed the rows the Go
    proposer would have written rather than going through a second Python proposer.
    """
    warehouse.ensure_upstream_mutation_tables()
    now = datetime.now(tz=UTC)
    warehouse._command(
        """
        INSERT INTO @upstream_mutation_requests (
            id, status, title, reason, context_json, idempotency_key, revision,
            requested_by, approved_by, created_at, updated_at, approved_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, 1, %s, %s, %s, %s, %s)
        """,
        (
            request_id,
            status,
            title,
            reason,
            _jsonb_param({"source": "unit-test"}),
            request_id,
            requested_by,
            "zach" if status == "approved" else "",
            now,
            now,
            now if status == "approved" else EPOCH,
        ),
    )

    children: list[dict[str, Any]] = []
    for index, spec in enumerate(mutations):
        child_status = str(spec.get("status") or status)
        child = {
            "id": f"{request_id}_m{index}",
            "request_id": request_id,
            "request_index": index,
            "provider": str(spec["provider"]),
            "operation": str(spec["operation"]),
            "account": str(spec.get("account") or account),
            "status": child_status,
            "title": str(spec.get("title") or title),
            "reason": str(spec.get("reason") or reason),
            "payload_json": dict(spec.get("payload") or {}),
            "preview_json": dict(spec.get("preview") or {}),
        }
        warehouse._command(
            """
            INSERT INTO @upstream_mutations (
                id, request_id, request_index, provider, operation, account, status,
                title, reason, payload_json, preview_json, idempotency_key, revision,
                requested_by, approved_by, created_at, updated_at, approved_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, %s, %s, %s, %s, %s)
            """,
            (
                child["id"],
                child["request_id"],
                child["request_index"],
                child["provider"],
                child["operation"],
                child["account"],
                child["status"],
                child["title"],
                child["reason"],
                _jsonb_param(child["payload_json"]),
                _jsonb_param(child["preview_json"]),
                child["id"],
                requested_by,
                "zach" if child_status == "approved" else "",
                now,
                now,
                now if child_status == "approved" else EPOCH,
            ),
        )
        warehouse._append_upstream_mutation_event(
            child["id"],
            event_type="created",
            actor_type="human",
            actor_id=requested_by,
            event_json={"request_id": request_id},
        )
        children.append(child)

    return {"id": request_id, "status": status, "title": title, "mutations": children}


def _request_status(warehouse: PostgresWarehouse, request_id: str) -> str:
    rows = warehouse._query_dicts(
        "SELECT status FROM @upstream_mutation_requests WHERE id = %s",
        (request_id,),
    )
    assert rows, f"unknown request {request_id}"
    return str(rows[0]["status"])


def _mutation_events(warehouse: PostgresWarehouse, mutation_id: str) -> list[dict[str, Any]]:
    return warehouse._query_dicts(
        """
        SELECT *
        FROM @upstream_mutation_events
        WHERE mutation_id = %s
        ORDER BY event_index ASC
        """,
        (mutation_id,),
    )


def _contract_operation(case_name: str, **overrides: Any) -> dict[str, Any]:
    """Return the Go proposer's normalized contacts operation for one contract case."""
    contract = json.loads(CONTACT_MUTATION_CONTRACT_PATH.read_text(encoding="utf-8"))
    for case in contract["cases"]:
        if case["name"] == case_name:
            operation = copy.deepcopy(case["normalized"])
            operation.update(copy.deepcopy(overrides))
            return operation
    raise AssertionError(f"unknown contacts mutation contract case: {case_name}")


def _gmail_thread_label_mutation(*, thread_id: str, archive: bool) -> dict[str, Any]:
    return {
        "provider": "gmail",
        "operation": GMAIL_ARCHIVE_OPERATION if archive else GMAIL_UNARCHIVE_OPERATION,
        "payload": (
            {"thread_ids": [thread_id], "remove_label_ids": ["INBOX"]}
            if archive
            else {"thread_ids": [thread_id], "add_label_ids": ["INBOX"]}
        ),
        "preview": {"thread_count": 1, "threads": [{"thread_id": thread_id}]},
    }


def test_gmail_message_ids_for_thread_label_mutation_selects_matching_threads(
    warehouse: PostgresWarehouse,
) -> None:
    warehouse.ensure_tables()
    warehouse.insert_messages(
        [
            _message_row(message_id="m1", thread_id="thread-1", subject="One", labels=["INBOX"], sync_version=1),
            _message_row(message_id="m2", thread_id="thread-2", subject="Two", labels=["INBOX"], sync_version=1),
            _message_row(message_id="m3", thread_id="thread-3", subject="Three", labels=[], sync_version=1),
        ]
    )

    assert warehouse.gmail_message_ids_for_thread_label_mutation(
        account=ACCOUNT,
        thread_ids=["thread-1", "thread-2"],
        archive=True,
    ) == {"thread-1": ["m1"], "thread-2": ["m2"]}
    assert warehouse.gmail_message_ids_for_thread_label_mutation(
        account=ACCOUNT,
        thread_ids=["thread-3"],
        archive=True,
    ) == {"thread-3": []}


def test_upstream_mutation_claim_fail_and_observe_transitions(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()
    warehouse.insert_messages(
        [
            _message_row(message_id="m1", thread_id="thread-1", subject="One", labels=["INBOX"], sync_version=1),
            _message_row(message_id="m2", thread_id="thread-2", subject="Two", labels=["INBOX"], sync_version=1),
        ]
    )

    succeeded_mutation = _seed_mutation_request(
        warehouse,
        request_id="req_succeeded",
        title="Archive 1 Gmail thread",
        mutations=[_gmail_thread_label_mutation(thread_id="thread-1", archive=True)],
    )
    retryable_mutation = _seed_mutation_request(
        warehouse,
        request_id="req_retryable",
        title="Archive 1 Gmail thread",
        mutations=[_gmail_thread_label_mutation(thread_id="thread-2", archive=True)],
    )

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
    assert _request_status(warehouse, succeeded_mutation["id"]) == "observed"
    assert _request_status(warehouse, retryable_mutation["id"]) == "failed_retryable"


def test_complete_upstream_mutations_bulk_updates_rows_and_events(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()
    warehouse.insert_messages(
        [
            _message_row(message_id="m1", thread_id="thread-1", subject="One", labels=["INBOX"], sync_version=1),
            _message_row(message_id="m2", thread_id="thread-2", subject="Two", labels=["INBOX"], sync_version=1),
        ]
    )
    request = _seed_mutation_request(
        warehouse,
        request_id="req_bulk",
        title="Archive 2 Gmail threads",
        mutations=[
            _gmail_thread_label_mutation(thread_id="thread-1", archive=True),
            _gmail_thread_label_mutation(thread_id="thread-2", archive=True),
        ],
    )
    claimed = warehouse.claim_approved_upstream_mutations(limit=10, claimed_by="worker-1")

    completed = warehouse.complete_upstream_mutations(
        completions=[
            (claimed[0]["id"], {"archived_thread_ids": ["thread-1"]}),
            (claimed[1]["id"], {"archived_thread_ids": ["thread-2"]}),
        ],
        actor_id="worker-1",
    )

    assert completed == 2
    assert _request_status(warehouse, request["id"]) == "succeeded"
    for mutation in warehouse.list_upstream_mutations_for_request(request["id"]):
        assert mutation["status"] == "succeeded"
        events = _mutation_events(warehouse, mutation["id"])
        assert events[-1]["event_type"] == "executed"
        assert events[-1]["actor_id"] == "worker-1"
        assert events[-1]["event_json"]["archived_thread_ids"]


def test_observe_upstream_mutations_bulk_updates_rows_and_events(warehouse: PostgresWarehouse) -> None:
    request = _seed_mutation_request(
        warehouse,
        request_id="req-bulk-observe",
        title="Observe two archive mutations",
        status="approved",
        mutations=[
            _gmail_thread_label_mutation(thread_id="thread-1", archive=True),
            _gmail_thread_label_mutation(thread_id="thread-2", archive=True),
        ],
    )
    claimed = warehouse.claim_approved_upstream_mutations(limit=10, claimed_by="worker-1")
    warehouse.complete_upstream_mutations(
        completions=[(row["id"], {"ok": True}) for row in claimed],
        actor_id="worker-1",
    )

    observed = warehouse.observe_upstream_mutations(
        observations=[
            (str(row["id"]), {"thread_ids": [f"thread-{index + 1}"]})
            for index, row in enumerate(claimed)
        ],
        actor_id="observer-1",
    )

    assert observed == 2
    for mutation in warehouse.list_upstream_mutations_for_request(request["id"]):
        assert mutation["status"] == "observed"
        events = _mutation_events(warehouse, mutation["id"])
        assert events[-1]["event_type"] == "observed"
        assert events[-1]["actor_id"] == "observer-1"
        assert events[-1]["event_json"]["thread_ids"]


def test_succeeded_upstream_mutation_observation_state_tracks_backlog_watermark(
    warehouse: PostgresWarehouse,
) -> None:
    request = _seed_mutation_request(
        warehouse,
        request_id="req-observation-state",
        title="Track observation backlog",
        status="approved",
        mutations=[_gmail_thread_label_mutation(thread_id="thread-1", archive=True)],
    )
    claimed = warehouse.claim_approved_upstream_mutations(limit=1, claimed_by="worker-1")
    warehouse.complete_upstream_mutation(
        claimed[0]["id"],
        result_json={"ok": True},
        actor_id="worker-1",
    )

    count, newest_updated_at = warehouse.succeeded_upstream_mutation_observation_state()

    assert count == 1
    assert newest_updated_at is not None
    assert newest_updated_at.tzinfo is not None
    warehouse.observe_upstream_mutations(
        observations=[(request["mutations"][0]["id"], {"ok": True})],
        actor_id="observer-1",
    )
    assert warehouse.succeeded_upstream_mutation_observation_state() == (0, None)


def test_reclaim_stale_executing_mutations_resets_orphaned_gmail_rows(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()
    warehouse.insert_messages(
        [
            _message_row(message_id="m1", thread_id="thread-1", subject="One", labels=["INBOX"], sync_version=1),
            _message_row(message_id="m2", thread_id="thread-2", subject="Two", labels=["INBOX"], sync_version=1),
            _message_row(message_id="m3", thread_id="thread-3", subject="Three", labels=["INBOX"], sync_version=1),
        ]
    )

    stale_request = _seed_mutation_request(
        warehouse,
        request_id="req_stale",
        title="Stale archive",
        reason="worker crashed",
        mutations=[_gmail_thread_label_mutation(thread_id="thread-1", archive=True)],
    )
    fresh_request = _seed_mutation_request(
        warehouse,
        request_id="req_fresh",
        title="Fresh archive",
        reason="just claimed",
        mutations=[_gmail_thread_label_mutation(thread_id="thread-2", archive=True)],
    )
    untouched_request = _seed_mutation_request(
        warehouse,
        request_id="req_untouched",
        title="Already approved",
        reason="not claimed",
        mutations=[_gmail_thread_label_mutation(thread_id="thread-3", archive=True)],
    )

    stale_mutation_id = stale_request["mutations"][0]["id"]
    fresh_mutation_id = fresh_request["mutations"][0]["id"]
    untouched_mutation_id = untouched_request["mutations"][0]["id"]

    # Simulate two prior worker claims: one stale (60 min old), one fresh.
    long_ago = datetime.now(tz=UTC) - timedelta(minutes=60)
    just_now = datetime.now(tz=UTC) - timedelta(minutes=1)
    warehouse._command(
        """
        UPDATE @upstream_mutations
           SET status = 'executing',
               claimed_by = 'dead-worker',
               claimed_at = %s,
               updated_at = %s,
               attempt_count = 1
         WHERE id = %s
        """,
        (long_ago, long_ago, stale_mutation_id),
    )
    warehouse._command(
        """
        UPDATE @upstream_mutations
           SET status = 'executing',
               claimed_by = 'live-worker',
               claimed_at = %s,
               updated_at = %s,
               attempt_count = 1
         WHERE id = %s
        """,
        (just_now, just_now, fresh_mutation_id),
    )

    assert warehouse.stale_reclaimable_upstream_mutation_count(
        stale_after=timedelta(minutes=15),
        idempotent_operations=(("gmail", "gmail.archive_threads"), ("gmail", "gmail.unarchive_threads")),
    ) == 1

    reclaimed = warehouse.reclaim_stale_executing_mutations(
        stale_after=timedelta(minutes=15),
        idempotent_operations=(("gmail", "gmail.archive_threads"), ("gmail", "gmail.unarchive_threads")),
        actor_id="reaper",
    )
    assert reclaimed == 1

    stale_after_reclaim = warehouse.get_upstream_mutation(stale_mutation_id)
    assert stale_after_reclaim["status"] == "approved"
    assert stale_after_reclaim["claimed_by"] == ""
    reclaim_events = [
        event
        for event in _mutation_events(warehouse, stale_mutation_id)
        if event["event_type"] == "reclaimed"
    ]
    assert len(reclaim_events) == 1
    assert reclaim_events[0]["actor_id"] == "reaper"
    assert reclaim_events[0]["event_json"]["previous_claimed_by"] == "dead-worker"
    assert reclaim_events[0]["event_json"]["attempt_count"] == 1

    fresh_after_reclaim = warehouse.get_upstream_mutation(fresh_mutation_id)
    assert fresh_after_reclaim["status"] == "executing"
    assert fresh_after_reclaim["claimed_by"] == "live-worker"

    untouched_after_reclaim = warehouse.get_upstream_mutation(untouched_mutation_id)
    assert untouched_after_reclaim["status"] == "approved"
    assert warehouse.stale_reclaimable_upstream_mutation_count(
        stale_after=timedelta(minutes=15),
        idempotent_operations=(("gmail", "gmail.archive_threads"), ("gmail", "gmail.unarchive_threads")),
    ) == 0

    # Reclaimed row is again claimable by the next worker tick.
    claimed = warehouse.claim_approved_upstream_mutations(limit=10, claimed_by="next-worker")
    assert stale_mutation_id in {row["id"] for row in claimed}


def test_reclaim_stale_executing_mutations_leaves_non_idempotent_ops(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()
    warehouse.insert_messages(
        [
            _message_row(message_id="m1", thread_id="thread-1", subject="One", labels=["INBOX"], sync_version=1),
        ]
    )

    request = _seed_mutation_request(
        warehouse,
        request_id="req_send",
        title="Send email",
        reason="stuck non-idempotent",
        mutations=[
            {
                "provider": "gmail",
                "operation": GMAIL_SEND_EMAIL_OPERATION,
                "payload": {
                    "delivery_mode": "send",
                    "message": {
                        "to": ["one@example.test"],
                        "cc": [],
                        "bcc": [],
                        "subject": "Hi",
                        "body_text": "Body",
                        "body_html": "",
                    },
                },
            }
        ],
    )
    mutation_id = request["mutations"][0]["id"]
    long_ago = datetime.now(tz=UTC) - timedelta(hours=2)
    warehouse._command(
        """
        UPDATE @upstream_mutations
           SET status = 'executing',
               claimed_by = 'dead-worker',
               claimed_at = %s,
               updated_at = %s,
               attempt_count = 1
         WHERE id = %s
        """,
        (long_ago, long_ago, mutation_id),
    )

    reclaimed = warehouse.reclaim_stale_executing_mutations(
        stale_after=timedelta(minutes=15),
        idempotent_operations=(("gmail", "gmail.archive_threads"), ("gmail", "gmail.unarchive_threads")),
        actor_id="reaper",
    )
    assert reclaimed == 0
    assert warehouse.stale_reclaimable_upstream_mutation_count(
        stale_after=timedelta(minutes=15),
        idempotent_operations=(("gmail", "gmail.archive_threads"), ("gmail", "gmail.unarchive_threads")),
    ) == 0
    assert warehouse.get_upstream_mutation(mutation_id)["status"] == "executing"


def test_gmail_unarchive_mutation_claim_and_observe(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()
    warehouse.insert_messages(
        [
            _message_row(message_id="m1", thread_id="archived-thread", subject="Archived", labels=[], sync_version=1),
            _message_row(message_id="m2", thread_id="inbox-thread", subject="Inbox", labels=["INBOX"], sync_version=1),
        ]
    )
    assert warehouse.gmail_message_ids_for_thread_label_mutation(
        account=ACCOUNT,
        thread_ids=["archived-thread"],
        archive=False,
    ) == {"archived-thread": ["m1"]}

    request = _seed_mutation_request(
        warehouse,
        request_id="req_unarchive",
        title="Unarchive one",
        reason="bring it back",
        mutations=[_gmail_thread_label_mutation(thread_id="archived-thread", archive=False)],
    )
    mutation = request["mutations"][0]

    claimed = warehouse.claim_approved_upstream_mutations(limit=1, claimed_by="worker")
    assert claimed[0]["operation"] == GMAIL_UNARCHIVE_OPERATION
    assert claimed[0]["payload_json"] == {"thread_ids": ["archived-thread"], "add_label_ids": ["INBOX"]}
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
    assert _request_status(warehouse, request["id"]) == "observed"


def test_gmail_send_email_mutation_claim_and_observe(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()
    warehouse.insert_messages(
        [
            _message_row(message_id="m1", thread_id="thread-1", subject="Existing thread", labels=["INBOX"], sync_version=1),
        ]
    )

    request = _seed_mutation_request(
        warehouse,
        request_id="req_email",
        title="Send follow-ups",
        reason="agent drafted useful replies",
        mutations=[
            {
                "provider": "gmail",
                "operation": GMAIL_SEND_EMAIL_OPERATION,
                "payload": {
                    "delivery_mode": "draft",
                    "message": {
                        "to": ["one@example.test"],
                        "cc": ["edited-cc@example.test"],
                        "bcc": ["secret@example.test"],
                        "subject": "New thread",
                        "body_text": "Edited body",
                        "body_html": "",
                    },
                },
            }
        ],
    )
    mutation_id = request["mutations"][0]["id"]

    claimed = warehouse.claim_approved_upstream_mutations(limit=1, claimed_by="worker")
    assert claimed[0]["operation"] == GMAIL_SEND_EMAIL_OPERATION
    warehouse.complete_upstream_mutation(
        mutation_id,
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
    assert warehouse.get_upstream_mutation(mutation_id)["status"] == "observed"


def test_contact_mutation_claim_and_observe(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_contacts_tables()
    warehouse.insert_contact_cards(
        [
            _contact_card_row(card_id="people/c1", etag="etag-1", display_name="Update Me"),
            _contact_card_row(card_id="people/c3", etag="etag-3", display_name="Delete Me"),
        ]
    )

    update_operation = _contract_operation("update derives the field mask from the person body")
    delete_operation = _contract_operation("delete carries the reviewed etag")
    request = _seed_mutation_request(
        warehouse,
        request_id="req_contacts",
        title="Clean contacts",
        reason="test contact batch",
        mutations=[
            {
                "provider": "google_people",
                "operation": GOOGLE_CONTACTS_BATCH_MUTATION_OPERATION,
                "payload": {"operations": [update_operation]},
                "preview": {"operation_count": 1},
            },
            {
                # The reviewer removed this one before approving the request.
                "provider": "google_people",
                "operation": GOOGLE_CONTACTS_BATCH_MUTATION_OPERATION,
                "status": "rejected",
                "payload": {"operations": [delete_operation]},
                "preview": {"operation_count": 1},
            },
        ],
    )

    claimed = warehouse.claim_approved_upstream_mutations(limit=1, claimed_by="worker")
    assert [row["id"] for row in claimed] == [request["mutations"][0]["id"]]
    assert claimed[0]["payload_json"]["operations"][0]["person"]["resourceName"] == "people/c1"
    assert claimed[0]["payload_json"]["operations"][0]["person"]["etag"] == "etag-1"
    warehouse.complete_upstream_mutation(
        request["mutations"][0]["id"],
        result_json={
            "operation_results": [
                {"op": "update_contact", "resource_name": "people/c1", "etag": "etag-updated"}
            ]
        },
        actor_id="worker",
    )
    assert warehouse.observe_succeeded_contact_mutations() == 0

    warehouse.insert_contact_cards(
        [
            _contact_card_row(card_id="people/c1", etag="etag-updated", display_name="Ada Lovelace", sync_version=2),
        ]
    )
    assert warehouse.observe_succeeded_contact_mutations() == 1
    assert _request_status(warehouse, request["id"]) == "observed"


def test_contact_nickname_update_observes_synced_value_when_response_etag_differs(
    warehouse: PostgresWarehouse,
) -> None:
    warehouse.ensure_contacts_tables()
    warehouse.insert_contact_cards(
        [
            _contact_card_row(card_id="people/c1", etag="etag-before", display_name="Update Me"),
        ]
    )

    nickname_operation = _contract_operation(
        "update derives the field mask from the person body",
        expected_etag="etag-before",
        update_person_fields=["nicknames"],
        person={
            "nicknames": [{"value": "Ace", "type": "DEFAULT"}],
            "resourceName": "people/c1",
            "etag": "etag-before",
        },
    )
    request = _seed_mutation_request(
        warehouse,
        request_id="req_nickname",
        title="Add nickname",
        reason="test nickname update",
        mutations=[
            {
                "provider": "google_people",
                "operation": GOOGLE_CONTACTS_BATCH_MUTATION_OPERATION,
                "payload": {"operations": [nickname_operation]},
            }
        ],
    )
    child = request["mutations"][0]

    claimed = warehouse.claim_approved_upstream_mutations(limit=1, claimed_by="worker")
    assert claimed[0]["id"] == child["id"]
    warehouse.complete_upstream_mutation(
        child["id"],
        result_json={
            "operation_results": [
                {"op": "update_contact", "resource_name": "people/c1", "etag": "etag-response"}
            ]
        },
        actor_id="worker",
    )

    warehouse.insert_contact_cards(
        [
            _contact_card_row(
                card_id="people/c1",
                etag="etag-from-sync",
                display_name="Update Me",
                nicknames=[{"value": "Ace", "metadata": {"primary": True}}],
                raw_json={
                    "resourceName": "people/c1",
                    "etag": "etag-from-sync",
                    "nicknames": [{"value": "Ace", "metadata": {"primary": True}}],
                },
                sync_version=2,
            ),
        ]
    )

    assert warehouse.observe_succeeded_contact_mutations() == 1
    assert _request_status(warehouse, request["id"]) == "observed"


def test_calendar_event_mutation_claim_and_observe(warehouse: PostgresWarehouse) -> None:
    request = _seed_mutation_request(
        warehouse,
        request_id="req_calendar",
        title="Schedule planning",
        reason="test calendar create",
        mutations=[
            {
                "provider": CALENDAR_PROVIDER,
                "operation": CALENDAR_CREATE_EVENT_OPERATION,
                "payload": {
                    "calendar_id": "primary",
                    "send_updates": "none",
                    "event": {
                        "summary": "Planning",
                        "start": {"dateTime": "2030-01-01T10:00:00", "timeZone": "UTC"},
                        "end": {"dateTime": "2030-01-01T10:30:00", "timeZone": "UTC"},
                    },
                },
            }
        ],
    )
    child = request["mutations"][0]

    claimed = warehouse.claim_approved_upstream_mutations(limit=1, claimed_by="worker")
    assert claimed[0]["id"] == child["id"]
    assert claimed[0]["provider"] == CALENDAR_PROVIDER
    assert claimed[0]["operation"] == CALENDAR_CREATE_EVENT_OPERATION
    assert claimed[0]["payload_json"]["calendar_id"] == "primary"
    assert claimed[0]["payload_json"]["send_updates"] == "none"
    warehouse.complete_upstream_mutation(
        child["id"],
        result_json={"calendar_id": "primary", "event_id": "event-1", "etag": '"created-etag"'},
        actor_id="worker",
    )
    assert warehouse.observe_succeeded_calendar_event_mutations() == 0

    warehouse.insert_calendar_events(
        [
            _calendar_event_row(
                event_id="event-1",
                summary="Planning",
                etag='"created-etag"',
                sync_version=2,
            )
        ]
    )
    assert warehouse.observe_succeeded_calendar_event_mutations() == 1
    assert _request_status(warehouse, request["id"]) == "observed"


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
        account=ACCOUNT,
        message_id=message_id,
        thread_id=thread_id,
        history_id=sync_version,
        internal_date=now,
        label_ids=labels,
        snippet="snippet",
        subject=subject,
        from_address="sender@example.test",
        to_addresses=[ACCOUNT],
        delivered_to=ACCOUNT,
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
    **overrides,
):
    now = datetime(2026, 5, 22, 12, tzinfo=UTC)
    row = _default_row(
        CONTACT_CARD_COLUMNS,
        source="google_people",
        account=ACCOUNT,
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
        nicknames=[],
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
    row.update(overrides)
    return row


def _calendar_event_row(
    *,
    event_id: str,
    summary: str,
    etag: str,
    sync_version: int = 1,
    is_deleted: int = 0,
):
    now = datetime(2026, 5, 22, 12, tzinfo=UTC)
    raw_event = {
        "id": event_id,
        "etag": etag,
        "status": "cancelled" if is_deleted else "confirmed",
        "summary": summary,
    }
    return _default_row(
        CALENDAR_EVENT_COLUMNS,
        account=ACCOUNT,
        calendar_id="primary",
        event_id=event_id,
        status=raw_event["status"],
        is_deleted=is_deleted,
        summary=summary,
        start_at=now,
        end_at=now,
        raw_json=json.dumps(raw_event, sort_keys=True, separators=(",", ":")),
        updated_at=now,
        synced_at=now,
        sync_version=sync_version,
    )


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


# The Go app owns this table's write path but Python's ensure path can bootstrap
# it first. Two definitions of one table that disagree is the same class of
# drift that broke Google Contacts updates, so pin them to agree here.
def test_ensure_upstream_mutation_tables_declares_the_supersede_column(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_upstream_mutation_tables()
    target = CATALOG.object("upstream_mutation_requests")
    # Resolve through the warehouse's own namespace mapping, not the catalog's
    # production schema: under a test namespace ensure_* creates the table in
    # that namespace, so asking information_schema about `ops` only ever passed
    # against a database that already carried production's schema.
    rows = warehouse._query_dicts(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        (warehouse._object_schema("upstream_mutation_requests"), target.name),
    )
    assert "superseded_by_request_id" in {str(row["column_name"]) for row in rows}
