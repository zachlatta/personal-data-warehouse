from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import requests


DEFAULT_ALICE_BASE_URL = "https://aliceapp.ai"
DEFAULT_ALICE_PER_PAGE = 50


@dataclass(frozen=True)
class AliceTokens:
    auth_token: str
    refresh_token: str


class AliceApiClient:
    def __init__(
        self,
        *,
        key_id: str,
        secret_key: str,
        base_url: str = DEFAULT_ALICE_BASE_URL,
        timeout_seconds: int = 60,
        session: requests.Session | None = None,
    ) -> None:
        self._key_id = key_id
        self._secret_key = secret_key
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._session = session or requests.Session()
        self._tokens: AliceTokens | None = None

    def authenticate(self) -> AliceTokens:
        response = self._session.post(
            f"{self._base_url}/api/v2/authenticate_api_client",
            json={"key_id": self._key_id, "secret_key": self._secret_key},
            timeout=self._timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        tokens = AliceTokens(
            auth_token=str(payload.get("auth_token", "")),
            refresh_token=str(payload.get("refresh_token", "")),
        )
        if not tokens.auth_token or not tokens.refresh_token:
            raise ValueError("Alice authentication response did not include auth_token and refresh_token")
        self._tokens = tokens
        return tokens

    def iter_upload_requests(self, *, per_page: int = DEFAULT_ALICE_PER_PAGE) -> Iterable[Mapping[str, Any]]:
        yield from self._iter_paginated(path="/api/v2/recordings/uploads", per_page=per_page)

    def iter_recordings(self, *, per_page: int = DEFAULT_ALICE_PER_PAGE) -> Iterable[Mapping[str, Any]]:
        for item in self._iter_paginated(path="/api/v2/recordings", per_page=per_page):
            yield self._normalize_recording(item)

    def _iter_paginated(self, *, path: str, per_page: int) -> Iterable[Mapping[str, Any]]:
        if self._tokens is None:
            self.authenticate()
        page = 1
        seen = 0
        while True:
            response = self._session.get(
                f"{self._base_url}{path}",
                params={"page": page, "per_page": per_page},
                headers=self._headers(),
                timeout=self._timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
            data = payload.get("data", [])
            if not isinstance(data, list) or not data:
                return
            for item in data:
                if isinstance(item, Mapping):
                    seen += 1
                    yield item
            meta = payload.get("meta_data", {})
            total = int(meta.get("total_record_count", 0) or 0) if isinstance(meta, Mapping) else 0
            if total and seen >= total:
                return
            page += 1

    def _normalize_recording(self, item: Mapping[str, Any]) -> dict[str, Any]:
        normalized = dict(item)
        guid = str(item.get("guid", "") or "")
        if item.get("mp3_download_url"):
            normalized["_pdw_media_url"] = str(item["mp3_download_url"])
        if guid:
            normalized["_pdw_recording_page_url"] = f"{self._base_url}/recordings/{guid}"
        return normalized

    def _headers(self) -> dict[str, str]:
        if self._tokens is None:
            raise RuntimeError("Alice client is not authenticated")
        return {"Authorization": self._tokens.auth_token}
