from __future__ import annotations

import requests

from personal_data_warehouse.config import PlaidConfig
from personal_data_warehouse.plaid_sync import PlaidAPIError, PlaidClient


class FakeResponse:
    def __init__(self, payload, status_code=200) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError("boom", response=self)

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, responses) -> None:
        self.responses = list(responses)
        self.posts = []

    def post(self, url, *, json, timeout):
        self.posts.append({"url": url, "json": json, "timeout": timeout})
        return self.responses.pop(0)


def _config() -> PlaidConfig:
    return PlaidConfig(
        account="zach@example.com",
        client_id="client-id",
        secret="secret",
        environment="sandbox",
        products=("transactions", "investments"),
        country_codes=("US",),
        client_name="Personal Data Warehouse",
        request_timeout_seconds=12,
    )


def test_plaid_client_creates_link_token_with_read_only_products() -> None:
    session = FakeSession([FakeResponse({"link_token": "link-token"})])
    client = PlaidClient(_config(), session=session)

    response = client.create_link_token()

    assert response["link_token"] == "link-token"
    request = session.posts[0]
    assert request["url"] == "https://sandbox.plaid.com/link/token/create"
    assert request["timeout"] == 12
    assert request["json"]["client_id"] == "client-id"
    assert request["json"]["secret"] == "secret"
    assert request["json"]["products"] == ["transactions"]
    assert request["json"]["additional_consented_products"] == ["investments"]
    assert request["json"]["transactions"] == {"days_requested": 730}
    assert request["json"]["country_codes"] == ["US"]
    client_user_id = request["json"]["user"]["client_user_id"]
    assert client_user_id.startswith("pdw-")
    assert "@" not in client_user_id
    assert "zach" not in client_user_id
    assert client_user_id == PlaidClient(_config(), session=FakeSession([])).client_user_id


def test_plaid_client_exchanges_public_token() -> None:
    session = FakeSession([FakeResponse({"access_token": "access-token", "item_id": "item-1"})])

    response = PlaidClient(_config(), session=session).exchange_public_token("public-token")

    assert response == {"access_token": "access-token", "item_id": "item-1"}
    assert session.posts[0]["url"] == "https://sandbox.plaid.com/item/public_token/exchange"
    assert session.posts[0]["json"]["public_token"] == "public-token"


def test_plaid_client_surfaces_plaid_error_without_token_values() -> None:
    session = FakeSession(
        [
            FakeResponse(
                {"error_code": "INVALID_PUBLIC_TOKEN", "error_message": "public token expired"},
                status_code=400,
            )
        ]
    )

    try:
        PlaidClient(_config(), session=session).exchange_public_token("public-token-secret")
    except PlaidAPIError as exc:
        message = str(exc)
        assert "INVALID_PUBLIC_TOKEN" in message
        assert "public token expired" in message
        assert "public-token-secret" not in message
        assert "secret" not in message
    else:
        raise AssertionError("Plaid API errors should raise PlaidAPIError")
