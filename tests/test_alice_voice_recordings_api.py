from __future__ import annotations

from typing import Any

from personal_data_warehouse_alice_voice_recordings.api import AliceApiClient


class FakeResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self._payload


class FakeSession:
    def __init__(self) -> None:
        self.get_calls: list[dict[str, Any]] = []

    def post(self, *_args, **_kwargs) -> FakeResponse:
        return FakeResponse({"auth_token": "auth-token", "refresh_token": "refresh-token"})

    def get(self, _url: str, *, params: dict[str, Any], **_kwargs) -> FakeResponse:
        self.get_calls.append(dict(params))
        page = int(params["page"])
        if page == 1:
            count = 50
        elif page == 2:
            count = 25
        else:
            count = 0
        return FakeResponse(
            {
                "data": [{"guid": f"recording-{page}-{index}"} for index in range(count)],
                "meta_data": {
                    "current_page": page,
                    "current_page_record_count": count,
                    "total_record_count": 75,
                },
            }
        )


def test_alice_pagination_continues_when_server_caps_page_size_below_requested_size() -> None:
    session = FakeSession()
    client = AliceApiClient(key_id="key-id", secret_key="secret-key", session=session)

    recordings = list(client.iter_recordings(per_page=100))

    assert len(recordings) == 75
    assert session.get_calls == [
        {"page": 1, "per_page": 100},
        {"page": 2, "per_page": 100},
    ]
