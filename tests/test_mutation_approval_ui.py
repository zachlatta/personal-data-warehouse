from __future__ import annotations

from http.cookiejar import CookieJar
import re
from datetime import UTC, datetime
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import HTTPCookieProcessor, Request, build_opener, urlopen

import pytest

from personal_data_warehouse import mutation_approval_ui
from personal_data_warehouse.mutation_approval_ui import (
    MutationApprovalUiConfig,
    mutation_approval_ui_config_from_env,
    start_mutation_approval_ui,
)


class FakeWarehouse:
    def __init__(self) -> None:
        now = datetime(2026, 5, 22, 12, tzinfo=UTC)
        self.requests = {
            "req-1": {
                "id": "req-1",
                "title": "Archive threads",
                "status": "pending_review",
                "reason": "clear newsletters",
                "revision": 1,
                "created_at": now,
                "context_json": {"source": "test"},
            },
            "req-2": {
                "id": "req-2",
                "title": "Reject me",
                "status": "pending_review",
                "reason": "not enough signal",
                "revision": 1,
                "created_at": now,
                "context_json": {},
            },
            "req-contact": {
                "id": "req-contact",
                "title": "Edit contacts",
                "status": "pending_review",
                "reason": "clean contacts",
                "revision": 1,
                "created_at": now,
                "context_json": {},
            },
            "req-email": {
                "id": "req-email",
                "title": "Send email",
                "status": "pending_review",
                "reason": "follow up",
                "revision": 1,
                "created_at": now,
                "context_json": {},
            },
        }
        self.mutations = {
            "mut-1": {
                "id": "mut-1",
                "request_id": "req-1",
                "title": "Archive thread",
                "status": "pending_review",
                "provider": "gmail",
                "operation": "gmail.archive_threads",
                "account": "zach@example.test",
                "reason": "clear newsletter",
                "revision": 1,
                "created_at": now,
                "payload_json": {"thread_ids": ["thread-1", "thread-2"]},
                "preview_json": {
                    "threads": [
                        {"thread_id": "thread-1", "subject": "One", "latest_from_address": "a@example.test", "inbox_message_count": 1},
                        {"thread_id": "thread-2", "subject": "Two", "latest_from_address": "b@example.test", "inbox_message_count": 1},
                    ]
                },
            },
            "mut-2": {
                "id": "mut-2",
                "request_id": "req-1",
                "title": "Archive another thread",
                "status": "pending_review",
                "provider": "gmail",
                "operation": "gmail.archive_threads",
                "account": "zach@example.test",
                "reason": "not enough signal",
                "revision": 1,
                "created_at": now,
                "payload_json": {"thread_ids": ["thread-3"]},
                "preview_json": {
                    "threads": [
                        {"thread_id": "thread-3", "subject": "Three", "latest_from_address": "c@example.test", "inbox_message_count": 1}
                    ]
                },
            },
            "mut-contact": {
                "id": "mut-contact",
                "request_id": "req-contact",
                "title": "Edit contacts",
                "status": "pending_review",
                "provider": "google_people",
                "operation": "contacts.batch_mutation",
                "account": "zach@example.test",
                "reason": "clean contacts",
                "revision": 1,
                "created_at": now,
                "payload_json": {
                    "operations": [
                        {"op": "update_contact", "resource_name": "people/1"},
                        {"op": "delete_contact", "resource_name": "people/2"},
                    ]
                },
                "preview_json": {
                    "operations": [
                        {
                            "op_index": 0,
                            "op": "update_contact",
                            "resource_name": "people/1",
                            "summary": {"display_name": "One", "primary_email": "one@example.test"},
                        },
                        {
                            "op_index": 1,
                            "op": "delete_contact",
                            "resource_name": "people/2",
                            "summary": {"display_name": "Two", "primary_email": "two@example.test"},
                        },
                    ]
                },
            },
            "mut-email": {
                "id": "mut-email",
                "request_id": "req-email",
                "title": "Send email",
                "status": "pending_review",
                "provider": "gmail",
                "operation": "gmail.send_email",
                "account": "zach@example.test",
                "reason": "follow up",
                "revision": 1,
                "created_at": now,
                "payload_json": {
                    "delivery_mode": "send",
                    "message": {
                        "to": ["one@example.test"],
                        "cc": [],
                        "bcc": [],
                        "subject": "Hello",
                        "body_text": "Original body",
                    },
                },
                "preview_json": {
                    "email": {
                        "mode": "new_thread",
                        "delivery_mode": "send",
                        "to": ["one@example.test"],
                        "cc": [],
                        "bcc": [],
                        "subject": "Hello",
                        "body_text": "Original body",
                    }
                },
            },
        }
        self.events = {"mut-1": [], "mut-2": [], "mut-contact": [], "mut-email": []}
        self.request_events = {"req-1": [], "req-2": [], "req-contact": [], "req-email": []}

    def ensure_upstream_mutation_tables(self) -> None:
        pass

    def close(self) -> None:
        pass

    def list_upstream_mutation_requests(self, *, limit: int):
        return list(self.requests.values())[:limit]

    def get_upstream_mutation_request(self, request_id: str):
        request = self.requests.get(request_id)
        if request is None:
            return None
        return {**request, "mutations": self.list_upstream_mutations_for_request(request_id)}

    def list_upstream_mutations_for_request(self, request_id: str):
        return [mutation for mutation in self.mutations.values() if mutation["request_id"] == request_id]

    def list_upstream_mutation_request_events(self, request_id: str):
        return self.request_events.get(request_id, [])

    def get_upstream_mutation(self, mutation_id: str):
        return self.mutations.get(mutation_id)

    def list_upstream_mutation_events(self, mutation_id: str):
        return self.events.get(mutation_id, [])

    def remove_threads_from_gmail_archive_mutation(self, *, mutation_id: str, thread_ids: list[str], actor_id: str):
        mutation = self.mutations[mutation_id]
        if mutation["status"] != "pending_review":
            raise ValueError(f"cannot edit mutation with status {mutation['status']}")
        remove = set(thread_ids)
        mutation["payload_json"]["thread_ids"] = [
            thread_id for thread_id in mutation["payload_json"]["thread_ids"] if thread_id not in remove
        ]
        mutation["preview_json"]["threads"] = [
            thread for thread in mutation["preview_json"]["threads"] if thread["thread_id"] not in remove
        ]
        mutation["revision"] += 1
        self.events[mutation_id].append({"event_type": "edited", "actor_id": actor_id})
        return mutation

    def remove_operations_from_contact_mutation(self, *, mutation_id: str, operation_indexes: list[int], actor_id: str):
        mutation = self.mutations[mutation_id]
        if mutation["status"] != "pending_review":
            raise ValueError(f"cannot edit mutation with status {mutation['status']}")
        remove = set(operation_indexes)
        mutation["payload_json"]["operations"] = [
            operation for index, operation in enumerate(mutation["payload_json"]["operations"]) if index not in remove
        ]
        mutation["preview_json"]["operations"] = [
            operation for operation in mutation["preview_json"]["operations"] if operation["op_index"] not in remove
        ]
        mutation["revision"] += 1
        self.events[mutation_id].append({"event_type": "edited", "actor_id": actor_id})
        return mutation

    def update_gmail_email_mutation(self, *, mutation_id: str, message: dict, delivery_mode: str, actor_id: str):
        mutation = self.mutations[mutation_id]
        if mutation["status"] != "pending_review":
            raise ValueError(f"cannot edit mutation with status {mutation['status']}")
        mutation["payload_json"]["message"] = message
        mutation["payload_json"]["delivery_mode"] = delivery_mode
        mutation["preview_json"]["email"] = {**message, "delivery_mode": delivery_mode, "mode": "new_thread"}
        mutation["revision"] += 1
        self.events[mutation_id].append({"event_type": "edited", "actor_id": actor_id})
        return mutation

    def remove_upstream_mutation_from_request(self, *, request_id: str, mutation_id: str, actor_id: str):
        request = self.requests[request_id]
        mutation = self.mutations[mutation_id]
        if request["status"] != "pending_review":
            raise ValueError(f"cannot edit request with status {request['status']}")
        mutation["status"] = "rejected"
        request["revision"] += 1
        self.request_events[request_id].append({"event_type": "mutation_removed", "actor_id": actor_id})
        return self.get_upstream_mutation_request(request_id)

    def approve_upstream_mutation_request(self, request_id: str, *, actor_id: str):
        request = self.requests[request_id]
        if request["status"] != "pending_review":
            raise ValueError(f"cannot approve request with status {request['status']}")
        request["status"] = "approved"
        for mutation in self.list_upstream_mutations_for_request(request_id):
            if mutation["status"] == "pending_review":
                mutation["status"] = "approved"
        self.request_events[request_id].append({"event_type": "approved", "actor_id": actor_id})
        return self.get_upstream_mutation_request(request_id)

    def reject_upstream_mutation_request(self, request_id: str, *, actor_id: str, reason: str):
        request = self.requests[request_id]
        if request["status"] != "pending_review":
            raise ValueError(f"cannot reject request with status {request['status']}")
        request["status"] = "rejected"
        request["error"] = reason
        for mutation in self.list_upstream_mutations_for_request(request_id):
            if mutation["status"] == "pending_review":
                mutation["status"] = "rejected"
        self.request_events[request_id].append({"event_type": "rejected", "actor_id": actor_id})
        return self.get_upstream_mutation_request(request_id)

    def approve_upstream_mutation(self, mutation_id: str, *, actor_id: str):
        mutation = self.mutations[mutation_id]
        if mutation["status"] != "pending_review":
            raise ValueError(f"cannot approve mutation with status {mutation['status']}")
        mutation["status"] = "approved"
        self.events[mutation_id].append({"event_type": "approved", "actor_id": actor_id})
        return mutation


@pytest.fixture()
def approval_ui(monkeypatch):
    warehouse = FakeWarehouse()
    monkeypatch.setattr(mutation_approval_ui, "load_settings", lambda **_kwargs: object())
    monkeypatch.setattr(mutation_approval_ui, "warehouse_from_settings", lambda _settings: warehouse)
    server = start_mutation_approval_ui(
        MutationApprovalUiConfig(
            bind_host="127.0.0.1",
            port=0,
            public_base_url=None,
            password="correct horse battery staple",
            session_secret="test-session-secret",
            session_ttl_seconds=3600,
        )
    )
    try:
        yield server, warehouse
    finally:
        server.close()


def test_approval_ui_requires_login_and_rejects_bad_password(approval_ui) -> None:
    server, warehouse = approval_ui

    login_page = _get(f"{server.public_base_url}/mutations")
    assert "Sign in" in login_page
    assert "Archive threads" not in login_page

    with pytest.raises(HTTPError) as bad_password:
        _post(server.public_base_url, "/login", {"password": "wrong", "next": "/requests"})
    assert bad_password.value.code == 403
    assert warehouse.requests["req-1"]["status"] == "pending_review"

    opener = _login(server.public_base_url)
    list_page = _get(f"{server.public_base_url}/mutations", opener=opener)
    assert "Archive threads" in list_page


def test_approval_ui_views_removes_thread_and_approves_with_session(approval_ui) -> None:
    server, warehouse = approval_ui
    opener = _login(server.public_base_url)

    detail = _get(f"{server.public_base_url}/requests/req-1", opener=opener)
    assert "Archive Gmail Threads" in detail
    assert "2 pending" in detail
    assert "thread-1" in detail
    assert "thread-2" in detail
    assert "PIN" not in detail
    csrf_token = _csrf_token(detail)

    _post(
        server.public_base_url,
        "/requests/req-1/remove-mutation",
        {"csrf_token": csrf_token, "mutation_id": "mut-1"},
        opener=opener,
    )
    assert warehouse.mutations["mut-1"]["status"] == "rejected"

    with pytest.raises(HTTPError) as bad_csrf:
        _post(server.public_base_url, "/requests/req-1/approve", {"csrf_token": "wrong"}, opener=opener)
    assert bad_csrf.value.code == 403
    assert warehouse.requests["req-1"]["status"] == "pending_review"

    _post(server.public_base_url, "/requests/req-1/approve", {"csrf_token": csrf_token}, opener=opener)
    assert warehouse.requests["req-1"]["status"] == "approved"
    assert warehouse.mutations["mut-2"]["status"] == "approved"

    with pytest.raises(HTTPError) as after_approval:
        _post(
            server.public_base_url,
            "/requests/req-1/remove-mutation",
            {"csrf_token": csrf_token, "mutation_id": "mut-2"},
            opener=opener,
        )
    assert after_approval.value.code == 400


def test_approval_ui_rejects_with_session(approval_ui) -> None:
    server, warehouse = approval_ui
    opener = _login(server.public_base_url)
    csrf_token = _csrf_token(_get(f"{server.public_base_url}/requests/req-2", opener=opener))

    _post(server.public_base_url, "/requests/req-2/reject", {"csrf_token": csrf_token, "reason": "too risky"}, opener=opener)

    assert warehouse.requests["req-2"]["status"] == "rejected"
    assert warehouse.requests["req-2"]["error"] == "too risky"


def test_approval_ui_removes_contact_operation_with_session(approval_ui) -> None:
    server, warehouse = approval_ui
    opener = _login(server.public_base_url)

    detail = _get(f"{server.public_base_url}/requests/req-contact", opener=opener)
    assert "Contact Changes" in detail
    assert "1 update" in detail
    assert "1 delete" in detail
    assert "people/1" in detail
    assert "people/2" in detail
    csrf_token = _csrf_token(detail)

    _post(
        server.public_base_url,
        "/mutations/mut-contact/remove-operation",
        {"csrf_token": csrf_token, "op_index": "1"},
        opener=opener,
    )

    assert warehouse.mutations["mut-contact"]["payload_json"]["operations"] == [
        {"op": "update_contact", "resource_name": "people/1"}
    ]


def test_approval_ui_edits_email_and_approves_as_draft(approval_ui) -> None:
    server, warehouse = approval_ui
    opener = _login(server.public_base_url)

    detail = _get(f"{server.public_base_url}/requests/req-email", opener=opener)
    assert "Emails" in detail
    assert "1 send" in detail
    assert "Original body" in detail
    assert "Approve as draft" in detail
    csrf_token = _csrf_token(detail)

    _post(
        server.public_base_url,
        "/mutations/mut-email/approve-email",
        {
            "csrf_token": csrf_token,
            "delivery_mode": "draft",
            "to": "two@example.test",
            "cc": "cc@example.test",
            "bcc": "bcc@example.test",
            "subject": "Edited subject",
            "body_text": "Edited body",
            "body_html": "",
        },
        opener=opener,
    )

    assert warehouse.mutations["mut-email"]["status"] == "approved"
    assert warehouse.mutations["mut-email"]["payload_json"]["delivery_mode"] == "draft"
    assert warehouse.mutations["mut-email"]["payload_json"]["message"]["to"] == ["two@example.test"]
    assert warehouse.mutations["mut-email"]["payload_json"]["message"]["body_text"] == "Edited body"


def test_approval_ui_config_requires_password(monkeypatch) -> None:
    monkeypatch.delenv("PDW_MUTATION_UI_PASSWORD", raising=False)

    with pytest.raises(ValueError, match="PDW_MUTATION_UI_PASSWORD"):
        mutation_approval_ui_config_from_env()


def test_approval_ui_config_reads_password_session_and_server_settings(monkeypatch) -> None:
    monkeypatch.setenv("PDW_MUTATION_UI_PASSWORD", "secret password")
    monkeypatch.setenv("PDW_MUTATION_UI_SESSION_SECRET", "session-secret")
    monkeypatch.setenv("PDW_MUTATION_UI_SESSION_TTL_SECONDS", "600")
    monkeypatch.setenv("PDW_MUTATION_UI_PORT", "1234")
    monkeypatch.setenv("PDW_MUTATION_UI_PUBLIC_BASE_URL", "https://mutations.example.test")
    monkeypatch.setenv("PDW_MUTATION_UI_BIND_HOST", "0.0.0.0")

    config = mutation_approval_ui_config_from_env()

    assert config.bind_host == "0.0.0.0"
    assert config.port == 1234
    assert config.public_base_url == "https://mutations.example.test"
    assert config.password == "secret password"
    assert config.session_secret == "session-secret"
    assert config.session_ttl_seconds == 600


def test_approval_ui_config_generates_ephemeral_session_secret(monkeypatch) -> None:
    monkeypatch.setenv("PDW_MUTATION_UI_PASSWORD", "secret password")
    monkeypatch.delenv("PDW_MUTATION_UI_SESSION_SECRET", raising=False)
    monkeypatch.delenv("PDW_MUTATION_UI_PORT", raising=False)
    monkeypatch.delenv("PDW_MUTATION_UI_PUBLIC_BASE_URL", raising=False)
    monkeypatch.delenv("PDW_MUTATION_UI_BIND_HOST", raising=False)
    monkeypatch.delenv("PDW_MUTATION_UI_SESSION_TTL_SECONDS", raising=False)

    config = mutation_approval_ui_config_from_env()

    assert config.bind_host == "127.0.0.1"
    assert config.port == 0
    assert config.password == "secret password"
    assert len(config.session_secret) >= 32
    assert config.session_ttl_seconds == 12 * 60 * 60


def _login(base_url: str, password: str = "correct horse battery staple"):
    opener = build_opener(HTTPCookieProcessor(CookieJar()))
    _post(base_url, "/login", {"password": password, "next": "/requests"}, opener=opener)
    return opener


def _csrf_token(html: str) -> str:
    match = re.search(r'name="csrf_token" value="([^"]+)"', html)
    assert match is not None
    return match.group(1)


def _get(url: str, *, opener=None) -> str:
    open_fn = opener.open if opener is not None else urlopen
    with open_fn(url, timeout=5) as response:
        return response.read().decode("utf-8")


def _post(base_url: str, path: str, data: dict[str, str], *, opener=None) -> str:
    request = Request(
        f"{base_url}{path}",
        data=urlencode(data).encode("utf-8"),
        headers={"content-type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    open_fn = opener.open if opener is not None else urlopen
    with open_fn(request, timeout=5) as response:
        return response.read().decode("utf-8")
