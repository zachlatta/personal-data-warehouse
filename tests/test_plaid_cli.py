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


@pytest.mark.parametrize("payload,success", [
    ({"success": True, "public_token": ""}, True),
    ({"success": True, "public_token": None}, True),
    ({"success": False}, False),
    ({"error": "canceled"}, False),
    ({"success": True, "error": "failed"}, False),
    ({}, False),
])
def test_update_callback_requires_explicit_success_and_valid_state(payload, success) -> None:
    import secrets

    outcome = {}
    with LocalPlaidLinkServer(
        link_token=secrets.token_urlsafe(24), client_name="PDW", mode="update"
    ) as server:
        def wait():
            try:
                outcome["result"] = server.wait_for_result()
            except RuntimeError as exc:
                outcome["error"] = exc
        thread = threading.Thread(target=wait, daemon=True)
        thread.start()
        bad = requests.post(f"{server.url}exchange?state=wrong", json=payload, timeout=5)
        assert bad.status_code == 403
        assert server.result is None
        response = requests.post(
            f"{server.url}exchange?state={server.state_token}", json=payload, timeout=5
        )
        thread.join(timeout=5)
    assert not thread.is_alive()
    assert response.json()["ok"] is success
    assert ("result" in outcome) is success
    assert ("error" in outcome) is not success


def _update_dependencies(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    import secrets
    from personal_data_warehouse_plaid import cli

    item = PlaidLinkedItem(
        account="owner@example.com", item_id="item-existing",
        access_token=secrets.token_urlsafe(24), institution_id="ins_1", institution_name="Example Bank",
    )
    config = SimpleNamespace(account=item.account, client_name="PDW", secret=secrets.token_urlsafe(24))
    warehouse = MagicMock()
    warehouse.load_plaid_item_tokens.return_value = [item]
    client = MagicMock()
    link_token = secrets.token_urlsafe(24)
    client.create_link_token.return_value = {"link_token": link_token}
    client.accounts_get.return_value = {"accounts": [{"account_id": "account-existing"}]}
    server_factory = MagicMock()
    server = server_factory.return_value.__enter__.return_value
    server.url = "http://127.0.0.1:8765/"
    server.wait_for_result.return_value = cli.LinkResult(public_token="")
    monkeypatch.setattr(cli, "load_settings", lambda **kw: SimpleNamespace(plaid=config))
    monkeypatch.setattr(cli, "warehouse_from_settings", lambda settings: warehouse)
    monkeypatch.setattr(cli, "PlaidClient", lambda config: client)
    monkeypatch.setattr(cli, "LocalPlaidLinkServer", server_factory)
    browser = MagicMock()
    monkeypatch.setattr(cli.webbrowser, "open", browser)
    sync = MagicMock()
    monkeypatch.setattr(cli, "PlaidSyncRunner", sync)
    return cli, item, warehouse, client, server_factory, server, browser, sync, link_token


@pytest.mark.parametrize("no_browser", [True, False])
@pytest.mark.parametrize("accounts", [[], [{"account_id": "account-existing"}]])
def test_update_preserves_identity_and_credential_without_sync(monkeypatch, capsys, no_browser, accounts):
    cli, item, warehouse, client, factory, server, browser, sync, link_token = _update_dependencies(monkeypatch)
    client.accounts_get.return_value = {"accounts": accounts}
    argv = ["update", "item-ex", "--host", "127.0.0.1", "--port", "8765"]
    if no_browser:
        argv.append("--no-browser")
    assert cli.main(argv) == (0 if accounts else 1)
    client.create_link_token.assert_called_once_with(account=item.account, access_token=item.access_token)
    assert factory.call_args.kwargs == {
        "link_token": link_token, "client_name": "PDW", "host": "127.0.0.1", "port": 8765, "mode": "update",
    }
    client.accounts_get.assert_called_once_with(item.access_token)
    client.exchange_public_token.assert_not_called()
    client.item_remove.assert_not_called()
    warehouse.upsert_plaid_item_token.assert_not_called()
    warehouse.delete_plaid_item.assert_not_called()
    warehouse.close.assert_called_once()
    sync.assert_not_called()
    assert browser.called is not no_browser
    output = capsys.readouterr()
    text = output.out + output.err
    assert "Existing Plaid Item item-existing updated" in text
    assert f"accounts available: {len(accounts)}" in text
    assert item.access_token not in text
    assert link_token not in text


@pytest.mark.parametrize("needle", ["missing", "item-", ""])
def test_update_refuses_unknown_or_ambiguous_item_before_link(monkeypatch, capsys, needle):
    cli, item, warehouse, client, factory, *_ = _update_dependencies(monkeypatch)
    from dataclasses import replace
    warehouse.load_plaid_item_tokens.return_value.append(replace(item, item_id="item-other"))
    assert cli.main(["update", needle]) == 2
    client.create_link_token.assert_not_called()
    factory.assert_not_called()
    warehouse.close.assert_called_once()
    assert "plaid items" in capsys.readouterr().err


@pytest.mark.parametrize("stage", ["create_link_token", "callback", "accounts_get"])
def test_update_failure_never_replaces_or_deletes_item_or_leaks_tokens(monkeypatch, capsys, stage):
    cli, item, warehouse, client, factory, server, browser, sync, link_token = _update_dependencies(monkeypatch)
    error = RuntimeError(f"provider echoed {item.access_token} {link_token}")
    if stage == "callback":
        server.wait_for_result.side_effect = error
    else:
        getattr(client, stage).side_effect = error
    assert cli.main(["update", item.item_id, "--no-browser"]) == 1
    client.exchange_public_token.assert_not_called()
    client.item_remove.assert_not_called()
    warehouse.upsert_plaid_item_token.assert_not_called()
    warehouse.delete_plaid_item.assert_not_called()
    warehouse.close.assert_called_once()
    sync.assert_not_called()
    output = capsys.readouterr()
    assert item.access_token not in output.out + output.err
    assert link_token not in output.out + output.err


def test_shared_link_page_explicitly_marks_success_and_keeps_oauth():
    page = _link_page("placeholder", "PDW", "state")
    assert "success: true" in page
    assert "receivedRedirectUri = window.location.href" in page


def test_update_requires_item_id():
    from personal_data_warehouse_plaid.cli import build_parser
    with pytest.raises(SystemExit):
        build_parser().parse_args(["update"])


def test_new_link_still_exchanges_and_persists_only_new_item(monkeypatch):
    cli, item, warehouse, client, factory, server, browser, sync, link_token = _update_dependencies(monkeypatch)
    import secrets
    public_token = secrets.token_urlsafe(24)
    server.wait_for_result.return_value = cli.LinkResult(public_token=public_token)
    client.exchange_public_token.return_value = {"item_id": "item-new", "access_token": item.access_token}
    assert cli.main(["link", "--no-browser"]) == 0
    client.create_link_token.assert_called_once_with(account=item.account)
    client.exchange_public_token.assert_called_once_with(public_token)
    assert warehouse.upsert_plaid_item_token.call_args.kwargs["item_id"] == "item-new"
    assert factory.call_args.kwargs["mode"] == "link"
    client.accounts_get.assert_not_called()
    sync.assert_not_called()


def test_new_link_callback_rejects_empty_public_token():
    outcome = {}
    with LocalPlaidLinkServer(link_token="placeholder", client_name="PDW") as server:
        def wait():
            try:
                server.wait_for_result()
            except RuntimeError:
                outcome["failed"] = True
        thread = threading.Thread(target=wait, daemon=True)
        thread.start()
        response = requests.post(
            f"{server.url}exchange?state={server.state_token}",
            json={"success": True, "public_token": ""}, timeout=5,
        )
        thread.join(timeout=5)
    assert response.json()["ok"] is False
    assert outcome == {"failed": True}
    assert server.result is None
