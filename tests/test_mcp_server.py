from __future__ import annotations

import pytest

from personal_data_warehouse import mcp_server


class FakeAccount:
    def __init__(self, email_address: str) -> None:
        self.email_address = email_address


class FakeSettings:
    def __init__(self, configured_account: str = "zach@example.test") -> None:
        self.configured_account = configured_account

    def account_for_email(self, email_address: str):
        if email_address.strip().lower() != self.configured_account:
            raise ValueError(f"{email_address} is not configured")
        return FakeAccount(self.configured_account)

    def contact_account_for_email(self, email_address: str):
        if email_address.strip().lower() != self.configured_account:
            raise ValueError(f"{email_address} is not configured")
        return FakeAccount(self.configured_account)

    def calendar_account_for_email(self, email_address: str):
        if email_address.strip().lower() != self.configured_account:
            raise ValueError(f"{email_address} is not configured")
        return FakeAccount(self.configured_account)


class FakeWarehouse:
    def __init__(self) -> None:
        self.proposals = []
        self.contact_proposals = []
        self.calendar_proposals = []
        self.request_proposals = []
        self.closed = False

    def propose_gmail_archive_threads(self, **kwargs):
        self.proposals.append(kwargs)
        return {"id": "req-123", "status": "pending_review", "mutations": [{"id": "mut-123"}]}

    def propose_gmail_unarchive_threads(self, **kwargs):
        self.proposals.append(kwargs)
        return {"id": "req-unarchive", "status": "pending_review", "mutations": [{"id": "mut-unarchive"}]}

    def propose_gmail_send_email(self, **kwargs):
        self.proposals.append(kwargs)
        return {"id": "req-email", "status": "pending_review", "mutations": [{"id": "mut-email"}]}

    def propose_contact_mutations(self, **kwargs):
        self.contact_proposals.append(kwargs)
        return {"id": "req-contact", "status": "pending_review", "mutations": [{"id": "mut-contact"}]}

    def propose_calendar_create_event(self, **kwargs):
        self.calendar_proposals.append(("create", kwargs))
        return {"id": "req-calendar-create", "status": "pending_review", "mutations": [{"id": "mut-calendar-create"}]}

    def propose_calendar_update_event(self, **kwargs):
        self.calendar_proposals.append(("update", kwargs))
        return {"id": "req-calendar-update", "status": "pending_review", "mutations": [{"id": "mut-calendar-update"}]}

    def propose_calendar_delete_event(self, **kwargs):
        self.calendar_proposals.append(("delete", kwargs))
        return {"id": "req-calendar-delete", "status": "pending_review", "mutations": [{"id": "mut-calendar-delete"}]}

    def propose_upstream_mutation_request(self, **kwargs):
        self.request_proposals.append(kwargs)
        return {"id": "req-combined", "status": "pending_review", "mutations": [{"id": "mut-a"}, {"id": "mut-b"}]}

    def close(self) -> None:
        self.closed = True


class FakeApprovalUi:
    def request_url(self, request_id: str) -> str:
        return f"http://127.0.0.1:9999/requests/{request_id}"


def test_mcp_propose_gmail_archive_threads_returns_approval_url(monkeypatch) -> None:
    warehouse = FakeWarehouse()
    monkeypatch.setattr(mcp_server, "load_settings", lambda **kwargs: FakeSettings())
    monkeypatch.setattr(mcp_server, "warehouse_from_settings", lambda settings: warehouse)

    result = mcp_server.propose_gmail_archive_threads_mutation(
        account="ZACH@example.test",
        thread_ids=[" thread-1 ", "thread-2"],
        reason="clear stale mail",
        title="Archive stale mail",
        context={"source": "test"},
        approval_ui=FakeApprovalUi(),
    )

    assert result == {
        "request_id": "req-123",
        "mutation_ids": ["mut-123"],
        "approval_url": "http://127.0.0.1:9999/requests/req-123",
        "status": "pending_review",
    }
    assert warehouse.closed is True
    assert warehouse.proposals == [
        {
            "account": "zach@example.test",
            "thread_ids": ["thread-1", "thread-2"],
            "reason": "clear stale mail",
            "title": "Archive stale mail",
            "context": {"source": "test"},
            "requested_by": "mcp",
        }
    ]


def test_mcp_propose_gmail_unarchive_threads_returns_approval_url(monkeypatch) -> None:
    warehouse = FakeWarehouse()
    monkeypatch.setattr(mcp_server, "load_settings", lambda **kwargs: FakeSettings())
    monkeypatch.setattr(mcp_server, "warehouse_from_settings", lambda settings: warehouse)

    result = mcp_server.propose_gmail_unarchive_threads_mutation(
        account="ZACH@example.test",
        thread_ids=[" thread-1 "],
        reason="bring back",
        title="Unarchive stale mail",
        context={"source": "test"},
        approval_ui=FakeApprovalUi(),
    )

    assert result == {
        "request_id": "req-unarchive",
        "mutation_ids": ["mut-unarchive"],
        "approval_url": "http://127.0.0.1:9999/requests/req-unarchive",
        "status": "pending_review",
    }
    assert warehouse.proposals == [
        {
            "account": "zach@example.test",
            "thread_ids": ["thread-1"],
            "reason": "bring back",
            "title": "Unarchive stale mail",
            "context": {"source": "test"},
            "requested_by": "mcp",
        }
    ]


def test_mcp_propose_rejects_empty_thread_list_before_opening_warehouse(monkeypatch) -> None:
    monkeypatch.setattr(mcp_server, "load_settings", lambda **kwargs: pytest.fail("settings should not be loaded"))

    with pytest.raises(ValueError, match="thread_ids"):
        mcp_server.propose_gmail_archive_threads_mutation(
            account="zach@example.test",
            thread_ids=[" "],
            reason="clear stale mail",
            approval_ui=FakeApprovalUi(),
        )


def test_mcp_propose_rejects_unknown_account(monkeypatch) -> None:
    monkeypatch.setattr(mcp_server, "load_settings", lambda **kwargs: FakeSettings())
    monkeypatch.setattr(mcp_server, "warehouse_from_settings", lambda settings: pytest.fail("warehouse should not be opened"))

    with pytest.raises(ValueError, match="not configured"):
        mcp_server.propose_gmail_archive_threads_mutation(
            account="other@example.test",
            thread_ids=["thread-1"],
            reason="clear stale mail",
            approval_ui=FakeApprovalUi(),
        )


def test_mcp_propose_contact_mutations_returns_approval_url(monkeypatch) -> None:
    warehouse = FakeWarehouse()
    monkeypatch.setattr(mcp_server, "load_settings", lambda **kwargs: FakeSettings())
    monkeypatch.setattr(mcp_server, "warehouse_from_settings", lambda settings: warehouse)
    operations = [{"op": "create_contact", "person": {"names": [{"givenName": "New"}]}}]

    result = mcp_server.propose_contact_mutations_batch(
        account="ZACH@example.test",
        operations=operations,
        reason="add missing contact",
        title="Add contact",
        context={"source": "test"},
        approval_ui=FakeApprovalUi(),
    )

    assert result == {
        "request_id": "req-contact",
        "mutation_ids": ["mut-contact"],
        "approval_url": "http://127.0.0.1:9999/requests/req-contact",
        "status": "pending_review",
    }
    assert warehouse.contact_proposals == [
        {
            "account": "zach@example.test",
            "operations": operations,
            "reason": "add missing contact",
            "title": "Add contact",
            "context": {"source": "test"},
            "requested_by": "mcp",
        }
    ]


def test_mcp_propose_gmail_send_email_returns_approval_url(monkeypatch) -> None:
    warehouse = FakeWarehouse()
    monkeypatch.setattr(mcp_server, "load_settings", lambda **kwargs: FakeSettings())
    monkeypatch.setattr(mcp_server, "warehouse_from_settings", lambda settings: warehouse)

    result = mcp_server.propose_gmail_send_email_mutation(
        account="ZACH@example.test",
        to=["one@example.test"],
        subject="Hello",
        body_text="Body",
        reason="follow up",
        cc=["two@example.test"],
        bcc=["secret@example.test"],
        reply_to_thread_id="thread-1",
        delivery_mode="draft",
        title="Reply",
        context={"source": "test"},
        approval_ui=FakeApprovalUi(),
    )

    assert result == {
        "request_id": "req-email",
        "mutation_ids": ["mut-email"],
        "approval_url": "http://127.0.0.1:9999/requests/req-email",
        "status": "pending_review",
    }
    assert warehouse.proposals == [
        {
            "account": "zach@example.test",
            "message": {
                "to": ["one@example.test"],
                "cc": ["two@example.test"],
                "bcc": ["secret@example.test"],
                "subject": "Hello",
                "body_text": "Body",
                "body_html": "",
                "reply_to_thread_id": "thread-1",
            },
            "delivery_mode": "draft",
            "reason": "follow up",
            "title": "Reply",
            "context": {"source": "test"},
            "requested_by": "mcp",
        }
    ]


def test_mcp_propose_calendar_create_event_returns_approval_url(monkeypatch) -> None:
    warehouse = FakeWarehouse()
    monkeypatch.setattr(mcp_server, "load_settings", lambda **kwargs: FakeSettings())
    monkeypatch.setattr(mcp_server, "warehouse_from_settings", lambda settings: warehouse)
    event = {
        "summary": "Planning",
        "start": {"dateTime": "2030-01-01T10:00:00", "timeZone": "UTC"},
        "end": {"dateTime": "2030-01-01T10:30:00", "timeZone": "UTC"},
    }

    result = mcp_server.propose_calendar_create_event_mutation(
        account="ZACH@example.test",
        event=event,
        calendar_id="primary",
        send_updates="none",
        reason="schedule planning",
        title="Schedule planning",
        context={"source": "test"},
        approval_ui=FakeApprovalUi(),
    )

    assert result == {
        "request_id": "req-calendar-create",
        "mutation_ids": ["mut-calendar-create"],
        "approval_url": "http://127.0.0.1:9999/requests/req-calendar-create",
        "status": "pending_review",
    }
    assert warehouse.calendar_proposals == [
        (
            "create",
            {
                "account": "zach@example.test",
                "event": event,
                "reason": "schedule planning",
                "calendar_id": "primary",
                "send_updates": "none",
                "title": "Schedule planning",
                "context": {"source": "test"},
                "requested_by": "mcp",
            },
        )
    ]


def test_mcp_propose_mutation_request_returns_request_approval_url(monkeypatch) -> None:
    warehouse = FakeWarehouse()
    monkeypatch.setattr(mcp_server, "load_settings", lambda **kwargs: FakeSettings())
    monkeypatch.setattr(mcp_server, "warehouse_from_settings", lambda settings: warehouse)

    mutations = [
        {"type": "gmail.unarchive_threads", "account": "zach@example.test", "thread_ids": ["thread-1"]},
        {
            "type": "gmail.send_email",
            "account": "zach@example.test",
            "delivery_mode": "draft",
            "message": {"to": ["one@example.test"], "subject": "Hello", "body_text": "Body"},
        },
        {"type": "google_people.contacts", "account": "zach@example.test", "operations": [{"op": "delete_contact", "resource_name": "people/1"}]},
        {
            "type": "calendar.create_event",
            "account": "zach@example.test",
            "event": {
                "summary": "Planning",
                "start": {"dateTime": "2030-01-01T10:00:00", "timeZone": "UTC"},
                "end": {"dateTime": "2030-01-01T10:30:00", "timeZone": "UTC"},
            },
        },
    ]
    result = mcp_server.propose_mutation_request(
        title="Do the task",
        reason="agent found work",
        mutations=mutations,
        context={"source": "test"},
        approval_ui=FakeApprovalUi(),
    )

    assert result == {
        "request_id": "req-combined",
        "mutation_ids": ["mut-a", "mut-b"],
        "approval_url": "http://127.0.0.1:9999/requests/req-combined",
        "status": "pending_review",
    }
    assert warehouse.request_proposals == [
        {
            "title": "Do the task",
            "reason": "agent found work",
            "mutations": mutations,
            "context": {"source": "test"},
            "requested_by": "mcp",
        }
    ]
