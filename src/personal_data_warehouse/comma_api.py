"""Client for comma.ai's API (api.commadotai.com), the backend of comma connect.

The credential is the JWT comma connect holds after a sign-in: it lasts 90
days and comma offers no refresh, so ``token_expires_at`` reads its ``exp``
(without verifying the signature, which only comma can) and the sync asks for a
new one before it lapses. Everything that knows the wire format lives here.

``/v1/me/`` and ``/v1/me/devices/`` need their trailing slash: the API answers
the slashless form with an HTML redirect, not JSON.
"""

from __future__ import annotations

import base64
from datetime import UTC, datetime
import json
from typing import Any
from urllib.parse import quote, urlencode

import requests

API_BASE_URL = "https://api.commadotai.com"
USER_AGENT = "personal-data-warehouse"
DEFAULT_TIMEOUT_SECONDS = 30
#: Routes asked for per page. The API returns every route in the window up to it.
ROUTE_PAGE_SIZE = 100
#: A guard against a server that keeps answering full pages.
MAX_ROUTE_PAGES = 100


class CommaError(RuntimeError):
    """A comma API call failed."""


class CommaAuthError(CommaError):
    """The token was refused or is unusable; only a new one fixes it."""


class CommaRequestError(CommaError):
    """Any other failure: the network, the server, or a response we cannot read."""


def token_claims(token: str) -> dict[str, Any]:
    """The JWT's claims, read without verification: ``exp`` and ``identity`` (the user id)."""

    parts = token.split(".")
    if len(parts) != 3:
        raise CommaAuthError("COMMA_API_TOKEN is not a JWT")
    payload = parts[1] + "=" * (-len(parts[1]) % 4)
    try:
        claims = json.loads(base64.urlsafe_b64decode(payload))
        int(claims["exp"])
        str(claims["identity"])
    except (ValueError, KeyError, TypeError) as exc:
        raise CommaAuthError("COMMA_API_TOKEN is not a JWT with exp and identity claims") from exc
    return claims


def token_expires_at(token: str) -> datetime:
    return datetime.fromtimestamp(int(token_claims(token)["exp"]), tz=UTC)


def token_identity(token: str) -> str:
    """The comma user id the token belongs to, the warehouse's ``account``."""

    return str(token_claims(token)["identity"])


class CommaClient:
    def __init__(
        self,
        *,
        token: str,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        base_url: str = API_BASE_URL,
        session: Any = None,
        route_page_size: int = ROUTE_PAGE_SIZE,
    ) -> None:
        self._token = token
        self._timeout = timeout
        self._base_url = base_url.rstrip("/")
        self._session = session or requests.Session()
        self._route_page_size = route_page_size

    def me(self) -> dict[str, Any]:
        return self._get("/v1/me/")

    def devices(self) -> list[dict[str, Any]]:
        return list(self._get("/v1/me/devices/"))

    def device_stats(self, dongle_id: str) -> dict[str, Any]:
        return self._get(f"/v1.1/devices/{quote(dongle_id)}/stats")

    def device_location(self, dongle_id: str) -> dict[str, Any] | None:
        """The device's last GPS fix, or None when comma has never had one."""

        return self._get(f"/v1/devices/{quote(dongle_id)}/location", missing_ok=True)

    def routes(self, dongle_id: str, *, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
        """Every route that started in the window, newest first, paging backwards by start time."""

        routes: list[dict[str, Any]] = []
        seen: set[str] = set()
        end = end_ms
        for _ in range(MAX_ROUTE_PAGES):
            query = urlencode({"start": start_ms, "end": end, "limit": self._route_page_size})
            page = list(self._get(f"/v1/devices/{quote(dongle_id)}/routes_segments?{query}"))
            fresh = [route for route in page if route.get("fullname") not in seen]
            for route in fresh:
                seen.add(route.get("fullname"))
                routes.append(route)
            if len(page) < self._route_page_size or not fresh:
                return routes
            end = min(int(route["start_time_utc_millis"]) for route in page) - 1
        raise CommaRequestError(f"routes for {dongle_id} did not end within {MAX_ROUTE_PAGES} pages")

    def segment_coords(self, route_url: str, segment: int) -> list[dict[str, Any]] | None:
        """One segment's 1 Hz GPS track, or None until comma has processed the segment.

        comma derives ``coords.json`` from the segment's qlog and serves it from
        the route's blob ``url`` (which the API hands out with the route), the
        same file comma connect draws its map from. Each point is ``t`` (seconds
        since the drive started), ``lat``, ``lng``, ``speed`` (m/s) and ``dist``
        (miles so far within this segment). The blob store takes no JWT: it
        rejects an Authorization scheme it does not know.
        """

        url = f"{route_url.rstrip('/')}/{int(segment)}/coords.json"
        try:
            response = self._session.request(
                "GET", url, headers={"User-Agent": USER_AGENT}, timeout=self._timeout
            )
        except requests.RequestException as exc:
            raise CommaRequestError(f"GET segment {segment} coords.json failed: {exc}") from exc
        if response.status_code == 404:
            return None
        if response.status_code >= 300:
            raise CommaRequestError(f"GET segment {segment} coords.json answered HTTP {response.status_code}")
        try:
            points = response.json()
        except ValueError as exc:
            raise CommaRequestError(f"segment {segment} coords.json is not JSON") from exc
        if not isinstance(points, list):
            raise CommaRequestError(f"segment {segment} coords.json is not a list")
        return points

    def _get(self, path: str, *, missing_ok: bool = False) -> Any:
        url = f"{self._base_url}{path}"
        try:
            response = self._session.request(
                "GET",
                url,
                headers={"Authorization": f"JWT {self._token}", "User-Agent": USER_AGENT},
                timeout=self._timeout,
            )
        except requests.RequestException as exc:
            raise CommaRequestError(f"GET {path.split('?')[0]} failed: {exc}") from exc
        if response.status_code in (401, 403):
            raise CommaAuthError(
                f"comma refused the token (HTTP {response.status_code}); sign in at connect.comma.ai "
                "and set a new COMMA_API_TOKEN"
            )
        if missing_ok and response.status_code == 404:
            return None
        if response.status_code >= 300:
            raise CommaRequestError(f"GET {path.split('?')[0]} answered HTTP {response.status_code}")
        try:
            return response.json()
        except ValueError as exc:
            raise CommaRequestError(f"GET {path.split('?')[0]} did not answer JSON") from exc
