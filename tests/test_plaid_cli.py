from __future__ import annotations

import io
import threading

import pytest
import requests

from personal_data_warehouse.plaid_sync import PlaidAPIError, PlaidLinkedItem
from personal_data_warehouse_plaid.cli import (
    LocalPlaidLinkServer,
    _link_page,
    resolve_plaid_item,
    unlink_plaid_item,
)


def test_local_plaid_link_server_receives_public_token_without_query_leak() -> None:
    with LocalPlaidLinkServer(link_token="link-token", client_name="PDW") as server:
        thread = threading.Thread(target=server.wait_for_result, daemon=True)
        thread.start()
        response = requests.post(
            f"{server.url}exchange?state={server.state_token}",
            json={
                "public_token": "public-token",
                "metadata": {"institution": {"institution_id": "ins_1", "name": "Example Bank"}},
            },
            timeout=5,
        )
        thread.join(timeout=5)

    assert response.json() == {"ok": True}
    assert not thread.is_alive()
    assert server.result is not None
    assert server.result.public_token == "public-token"
    assert server.result.institution_id == "ins_1"
    assert server.result.institution_name == "Example Bank"


def test_local_plaid_link_server_error_terminates_with_actionable_message() -> None:
    outcome: dict[str, object] = {}
    with LocalPlaidLinkServer(link_token="link-token", client_name="PDW") as server:
        def wait() -> None:
            try:
                server.wait_for_result()
            except Exception as exc:  # noqa: BLE001 - asserting the surfaced CLI error
                outcome["error"] = exc

        thread = threading.Thread(target=wait, daemon=True)
        thread.start()
        response = requests.post(
            f"{server.url}exchange?state={server.state_token}",
            json={"error": "institution login was canceled"},
            timeout=5,
        )
        thread.join(timeout=5)

    assert response.json() == {"ok": False, "error": "institution login was canceled"}
    assert not thread.is_alive()
    assert isinstance(outcome.get("error"), RuntimeError)
    assert "institution login was canceled" in str(outcome["error"])


def test_plaid_link_page_resumes_oauth_redirect_and_reports_clean_exit() -> None:
    page = _link_page("link-token", "PDW", "state-token")

    assert "oauth_state_id" in page
    assert "receivedRedirectUri" in page
    assert "window.location.href" in page
    assert "Plaid Link exited before an account was linked" in page
    assert "fetch('/exchange?state='" in page


# --- unlink: retiring an Item a re-link left behind --------------------------------


class FakeUnlinkWarehouse:
    def __init__(self, *, accounts=None, counts=None) -> None:
        self._accounts = accounts if accounts is not None else [
            {
                "account_id": "acc-1",
                "name": "Rewards Card",
                "mask": "4242",
                "type": "credit",
                "subtype": "credit card",
                "current_balance": 100.0,
                "is_removed": 0,
            }
        ]
        self._counts = counts or {"plaid_accounts": 2, "plaid_transactions": 12, "plaid_items": 1}
        self.deleted: list[tuple[str, str]] = []

    def load_plaid_item_accounts(self, *, account, item_id):
        return list(self._accounts)

    def count_plaid_item_rows(self, *, account, item_id):
        return dict(self._counts)

    def delete_plaid_item(self, *, account, item_id):
        self.deleted.append((account, item_id))
        return dict(self._counts)


class FakeItemRemoveClient:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.removed: list[str] = []

    def item_remove(self, access_token: str):
        self.removed.append(access_token)
        if self.error is not None:
            raise self.error
        return {"request_id": "req-1"}


def _linked_item(item_id: str = "item-old", institution: str = "Example Bank") -> PlaidLinkedItem:
    return PlaidLinkedItem(
        account="zach@example.com",
        item_id=item_id,
        access_token="access-token-secret",
        institution_id="ins_1",
        institution_name=institution,
    )


def test_resolve_plaid_item_accepts_an_unambiguous_id_prefix() -> None:
    items = [_linked_item("item-oldest"), _linked_item("item-newer")]

    assert resolve_plaid_item(items, "item-oldest").item_id == "item-oldest"
    assert resolve_plaid_item(items, "item-old").item_id == "item-oldest"


def test_resolve_plaid_item_refuses_unknown_and_ambiguous_ids() -> None:
    items = [_linked_item("item-aa"), _linked_item("item-ab")]

    with pytest.raises(ValueError, match="no linked Plaid item"):
        resolve_plaid_item(items, "nope")
    with pytest.raises(ValueError, match="matches 2 linked Plaid items"):
        resolve_plaid_item(items, "item-a")


def test_unlink_revokes_at_plaid_then_deletes_the_items_rows() -> None:
    warehouse = FakeUnlinkWarehouse()
    client = FakeItemRemoveClient()
    out = io.StringIO()

    code = unlink_plaid_item(
        warehouse=warehouse,
        client=client,
        item=_linked_item(),
        confirm=lambda _prompt: True,
        out=out,
    )

    assert code == 0
    assert client.removed == ["access-token-secret"]
    assert warehouse.deleted == [("zach@example.com", "item-old")]
    printed = out.getvalue()
    assert "Example Bank" in printed
    assert "4242" in printed
    assert "plaid_transactions=12" in printed
    # The access token is a credential, never an output.
    assert "access-token-secret" not in printed


def test_unlink_dry_run_reports_the_plan_and_changes_nothing() -> None:
    warehouse = FakeUnlinkWarehouse()
    client = FakeItemRemoveClient()
    out = io.StringIO()

    code = unlink_plaid_item(
        warehouse=warehouse,
        client=client,
        item=_linked_item(),
        confirm=lambda _prompt: True,
        out=out,
        dry_run=True,
    )

    assert code == 0
    assert client.removed == []
    assert warehouse.deleted == []
    assert "dry run" in out.getvalue().lower()


def test_unlink_declined_at_the_prompt_touches_nothing() -> None:
    warehouse = FakeUnlinkWarehouse()
    client = FakeItemRemoveClient()

    code = unlink_plaid_item(
        warehouse=warehouse,
        client=client,
        item=_linked_item(),
        confirm=lambda _prompt: False,
        out=io.StringIO(),
    )

    assert code == 1
    assert client.removed == []
    assert warehouse.deleted == []


def test_unlink_proceeds_when_plaid_has_already_forgotten_the_item() -> None:
    warehouse = FakeUnlinkWarehouse()
    client = FakeItemRemoveClient(PlaidAPIError("ITEM_NOT_FOUND: The Item you requested cannot be found"))
    out = io.StringIO()

    code = unlink_plaid_item(
        warehouse=warehouse,
        client=client,
        item=_linked_item(),
        confirm=lambda _prompt: True,
        out=out,
    )

    assert code == 0
    assert warehouse.deleted == [("zach@example.com", "item-old")]
    assert "ITEM_NOT_FOUND" in out.getvalue()


def test_unlink_keeps_the_rows_when_plaid_fails_for_any_other_reason() -> None:
    warehouse = FakeUnlinkWarehouse()
    client = FakeItemRemoveClient(PlaidAPIError("RATE_LIMIT_EXCEEDED: too many requests"))

    code = unlink_plaid_item(
        warehouse=warehouse,
        client=client,
        item=_linked_item(),
        confirm=lambda _prompt: True,
        out=io.StringIO(),
    )

    assert code == 1
    assert warehouse.deleted == []


def test_unlink_can_skip_the_plaid_call_for_an_already_revoked_item() -> None:
    warehouse = FakeUnlinkWarehouse()
    client = FakeItemRemoveClient()

    code = unlink_plaid_item(
        warehouse=warehouse,
        client=client,
        item=_linked_item(),
        confirm=lambda _prompt: True,
        out=io.StringIO(),
        skip_remote=True,
    )

    assert code == 0
    assert client.removed == []
    assert warehouse.deleted == [("zach@example.com", "item-old")]
