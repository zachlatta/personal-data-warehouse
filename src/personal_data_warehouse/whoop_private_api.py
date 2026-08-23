"""Client for WHOOP's private (app) API.

This is the API ``app.whoop.com`` itself calls. It is undocumented, so the
behaviours encoded here were established by probing the live service on
2026-08-23; see ``docs/whoop-private-api.md`` for the full reconnaissance,
including the dead ends that published write-ups will send you down.

Three things in here are load-bearing and look wrong until you know why:

* **Refresh sends the refresh token as the ``Authorization`` bearer with an
  empty body.** The obvious shape -- the refresh token in the request body --
  returns 401. This is the single detail that makes unattended sync possible.
* **``bearer`` is lowercase** and every request carries ``apiVersion=7``, which
  is what the app sends.
* **``metrics-service`` accepts only ``step`` 6, 60 or 600.** Anything else is
  an HTTP 400, so the client rejects it up front rather than returning a
  confusingly empty series.

The client never logs a token and never raises an exception carrying one.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any
import logging

LOGGER = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.prod.whoop.com"
DEFAULT_TIMEOUT_SECONDS = 30.0
API_VERSION = "7"
REFRESH_PATH = "/auth-service/v2/whoop/refresh"

#: metrics-service validates this server-side; 1, 10, 30 and 300 all return 400.
WHOOP_PRIVATE_VALID_STEPS = (6, 60, 600)

#: Refresh this many seconds before the access token actually expires, so a long
#: paging run does not die halfway through.
ACCESS_TOKEN_REFRESH_SKEW_SECONDS = 300

#: Token-endpoint statuses that mean the credential itself was rejected. No
#: retry clears these; only a fresh browser login does.
AUTH_REJECTED_STATUSES = frozenset({400, 401, 403})

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


class WhoopPrivateApiError(RuntimeError):
    """A private-API call failed."""


class WhoopPrivateAuthError(WhoopPrivateApiError):
    """The session was rejected and needs a new browser login.

    This is the ``action_required`` case: the pipeline should record it, stop
    retrying, and surface it on /pipelines rather than generating red runs.
    """


class WhoopPrivateRateLimitedError(WhoopPrivateApiError):
    def __init__(self, *, retry_after: int) -> None:
        super().__init__(f"WHOOP private API rate limited; retry after {retry_after}s")
        self.retry_after = retry_after


@dataclass(frozen=True)
class WhoopPrivateSession:
    """One account's captured browser session.

    ``refresh_expires_at`` is the one that matters operationally: every refresh
    issues a new refresh token, so a sync that runs more often than this never
    needs a human again.
    """

    account: str
    access_token: str
    refresh_token: str
    access_expires_at: datetime
    refresh_expires_at: datetime

    def access_token_expired(self, *, now: datetime, skew_seconds: int = ACCESS_TOKEN_REFRESH_SKEW_SECONDS) -> bool:
        return self.access_expires_at - timedelta(seconds=skew_seconds) <= now

    def refresh_token_expired(self, *, now: datetime) -> bool:
        return self.refresh_expires_at <= now


@dataclass(frozen=True)
class WhoopPrivateIdentity:
    user_id: str
    timezone_offset: str
    raw: dict[str, Any]


def rfc3339_millis(moment: datetime) -> str:
    """The timestamp format the private API expects (millis + literal Z)."""
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")


class WhoopPrivateClient:
    def __init__(
        self,
        *,
        session: WhoopPrivateSession,
        http: Any | None = None,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        now: Callable[[], datetime] | None = None,
        on_session_rotated: Callable[[WhoopPrivateSession], None] | None = None,
    ) -> None:
        self._session = session
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._now = now or (lambda: datetime.now(UTC))
        self._on_session_rotated = on_session_rotated
        self._http = http if http is not None else self._default_http()

    @staticmethod
    def _default_http() -> Any:
        # api.prod.whoop.com does not bot-challenge a plain client, but the
        # project already depends on curl_cffi and impersonation costs nothing.
        from curl_cffi import requests as cffi_requests

        http = cffi_requests.Session(impersonate="chrome131")
        http.headers.update({"user-agent": _USER_AGENT, "accept": "application/json"})
        return http

    @property
    def session(self) -> WhoopPrivateSession:
        return self._session

    # ---- authentication -------------------------------------------------

    def refresh(self) -> WhoopPrivateSession:
        """Rotate the session. See the module docstring for the header/body shape."""
        response = self._http.request(
            "POST",
            f"{self._base_url}{REFRESH_PATH}",
            headers={"authorization": f"bearer {self._session.refresh_token}"},
            json={},
            timeout=self._timeout,
        )
        status = response.status_code
        if status in AUTH_REJECTED_STATUSES:
            raise WhoopPrivateAuthError(
                f"WHOOP private session rejected (HTTP {status}); "
                "re-publish it with `pdw whoop publish-session` after logging in to app.whoop.com"
            )
        if status != 200:
            raise WhoopPrivateApiError(f"WHOOP private token refresh failed with HTTP {status}")
        try:
            payload = response.json()
        except Exception as error:  # noqa: BLE001
            raise WhoopPrivateApiError("WHOOP private token refresh returned a non-JSON body") from error

        now = self._now()
        rotated = replace(
            self._session,
            access_token=str(payload["access_token"]),
            refresh_token=str(payload.get("refresh_token") or self._session.refresh_token),
            access_expires_at=now + timedelta(seconds=int(payload.get("access_token_expires_in", 86_400))),
            refresh_expires_at=now + timedelta(seconds=int(payload.get("refresh_token_expires_in", 2_592_000))),
        )
        self._session = rotated
        if self._on_session_rotated is not None:
            self._on_session_rotated(rotated)
        return rotated

    # ---- transport ------------------------------------------------------

    def get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        if self._session.access_token_expired(now=self._now()):
            self.refresh()
        response = self._request(path, params)
        if response.status_code == 401:
            # Either the token aged out mid-run or the server disagrees with our
            # expiry arithmetic. Refresh once, then believe a second 401.
            self.refresh()
            response = self._request(path, params)
            if response.status_code == 401:
                raise WhoopPrivateAuthError(
                    "WHOOP private API still returned 401 after a successful refresh; "
                    "the session needs a new browser login"
                )
        if response.status_code == 429:
            raise WhoopPrivateRateLimitedError(retry_after=self._retry_after(response))
        if response.status_code != 200:
            raise WhoopPrivateApiError(f"WHOOP private API {path} returned HTTP {response.status_code}")
        try:
            return response.json()
        except Exception as error:  # noqa: BLE001
            raise WhoopPrivateApiError(f"WHOOP private API {path} returned a non-JSON body") from error

    def _request(self, path: str, params: dict[str, Any] | None) -> Any:
        merged = dict(params or {})
        merged.setdefault("apiVersion", API_VERSION)
        return self._http.request(
            "GET",
            f"{self._base_url}{path}",
            params=merged,
            headers={"authorization": f"bearer {self._session.access_token}"},
            timeout=self._timeout,
        )

    @staticmethod
    def _retry_after(response: Any) -> int:
        raw = dict(getattr(response, "headers", {}) or {}).get("retry-after")
        try:
            return max(1, int(str(raw)))
        except (TypeError, ValueError):
            return 60

    # ---- tier 1: data endpoints ----------------------------------------

    def bootstrap(self) -> WhoopPrivateIdentity:
        payload = self.get_json("/users-service/v2/bootstrap/", {"accountType": "users"})
        profile = (payload or {}).get("profile") or {}
        user_id = profile.get("user_id")
        if user_id in (None, ""):
            raise WhoopPrivateApiError("WHOOP bootstrap did not carry profile.user_id")
        return WhoopPrivateIdentity(
            user_id=str(user_id),
            timezone_offset=str(profile.get("timezone_offset") or ""),
            raw=payload,
        )

    def cycles_details(self, *, user_id: str, start: datetime, end: datetime, limit: int = 25) -> Any:
        return self.get_json(
            "/core-details-bff/v0/cycles/details",
            {"id": user_id, "startTime": rfc3339_millis(start), "endTime": rfc3339_millis(end), "limit": limit},
        )

    def sleep_events(self, *, activity_id: str) -> Any:
        return self.get_json("/sleep-service/v1/sleep-events", {"activityId": activity_id})

    def heart_rate(self, *, user_id: str, start: datetime, end: datetime, step: int) -> Any:
        if step not in WHOOP_PRIVATE_VALID_STEPS:
            raise ValueError(
                f"unsupported heart-rate step {step!r}; metrics-service accepts only {WHOOP_PRIVATE_VALID_STEPS}"
            )
        return self.get_json(
            f"/metrics-service/v1/metrics/user/{user_id}",
            {
                "name": "heart_rate",
                "start": rfc3339_millis(start),
                "end": rfc3339_millis(end),
                "step": step,
                "order": "t",
            },
        )

    def sports_catalog(self, *, country_code: str = "US") -> Any:
        return self.get_json("/activities-service/v1/sports/history", {"countryCode": country_code})

    def journal_entries(self, *, day: str) -> Any:
        """The day's journal answers. The v2 ``behaviors`` path is only the catalog."""
        return self.get_json(f"/journal-service/v3/journals/drafts/mobile/{day}")

    def journal_behaviors(self, *, day: str) -> Any:
        return self.get_json(f"/journal-service/v2/journals/behaviors/user/{day}")

    def preferences(self) -> Any:
        return self.get_json("/users-service/v0/users/preference")

    def user_state(self) -> Any:
        return self.get_json("/activities-service/v1/user-state")

    # ---- tier 2: BFF endpoints (store raw; the shape is a UI payload) ----

    def trend(self, *, metric: str, end_date: str) -> Any:
        return self.get_json(f"/progression-service/v3/trends/{metric}", {"endDate": end_date})

    def stress(self, *, day: str) -> Any:
        return self.get_json(f"/health-service/v2/stress-bff/{day}")

    def cardio_details(self, *, activity_id: str) -> Any:
        return self.get_json("/core-details-bff/v1/cardio-details", {"activityId": activity_id})

    def sleep_deep_dive(self, *, day: str) -> Any:
        return self.get_json("/home-service/v1/deep-dive/sleep/last-night", {"date": day})
