from __future__ import annotations

import threading

import requests

from personal_data_warehouse_plaid.cli import LocalPlaidLinkServer, _link_page


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
