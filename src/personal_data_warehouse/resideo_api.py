"""Client for the Resideo consumer API, the backend of the Resideo Connect app.

Resideo publishes no working API for a thermostat on this system: the
developer portal serves the older Lyric generation, and the app's own backend
is the one that holds a T5 set up in Resideo Connect. The endpoints here are
undocumented and may change; they were worked out by the MIT-licensed
sfcodes/ha-resideo project. Everything that knows the wire format lives in
this module, so a change upstream is one file.

The refresh token rotates and Auth0 revokes the whole grant when one is reused,
so a refresh only ever happens inside
``PostgresWarehouse.rotate_resideo_oauth_token``: the sync and the mutation
worker share one row, and the second caller adopts the first one's token.

Never PUT a command with a body that leaves out its value: the cloud accepts it
and applies a default, which upstream saw switch a live thermostat off.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
import base64
import hashlib
import html
import json
import re
import secrets
import time
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

import requests

API_BASE_URL = "https://api.ha.resideo.com"
AUTH_BASE_URL = "https://login.resideo.com"
OAUTH_CLIENT_ID = "SRmiA7CaYi1JgivDZdzzoZu4X5VBogGt"
OAUTH_REDIRECT_URI = "com.resideo.firstalert://login.resideo.com/ios/com.resideo.firstalert/callback"
OAUTH_AUDIENCE = "https://resideo-prod.auth0.com/api/v2/"
OAUTH_SCOPE = "openid profile email offline_access"
OAUTH_TENANT = "resideo-prod"
OAUTH_CONNECTION = "Username-Password-Authentication"
#: What Resideo's own sign-in page sends as its Auth0 client: base64 of its name and version.
AUTH0_LOGIN_PAGE_CLIENT = "eyJuYW1lIjoiYXV0aDAuanMtdWxwIiwidmVyc2lvbiI6IjkuMTMuMiJ9"
COMMAND_CHANNEL_ID = "ds-notification-service"
USER_AGENT = "personal-data-warehouse"
TOKEN_REFRESH_MARGIN_SECONDS = 300
DEFAULT_TIMEOUT_SECONDS = 30

HEAT = "heat"
COOL = "cool"
_SETPOINT_COMMANDS = {HEAT: "heatSetpoint", COOL: "coolSetpoint"}


class ResideoError(RuntimeError):
    """A Resideo call failed."""


class ResideoAuthError(ResideoError):
    """The credential was refused; only a new sign-in fixes it."""


class ResideoTransientError(ResideoError):
    """A connection failure, 429 or 5xx; the same call may work later."""


class ResideoRequestError(ResideoError):
    """Resideo refused this request; repeating it gets the same answer."""


def _error_for_status(status: int, description: str) -> ResideoError:
    if status in (401, 403):
        return ResideoAuthError(description)
    if status == 429 or status >= 500:
        return ResideoTransientError(description)
    return ResideoRequestError(description)


@dataclass(frozen=True)
class AuthorizeRequest:
    url: str
    code_verifier: str
    state: str


def build_authorize_request() -> AuthorizeRequest:
    """A PKCE sign-in URL for a person to open: they can answer a CAPTCHA."""

    code_verifier = _b64url(secrets.token_bytes(32))
    challenge = _b64url(hashlib.sha256(code_verifier.encode("ascii")).digest())
    state = _b64url(secrets.token_bytes(32))
    query = urlencode(
        {
            "client_id": OAUTH_CLIENT_ID,
            "redirect_uri": OAUTH_REDIRECT_URI,
            "response_type": "code",
            "scope": OAUTH_SCOPE,
            "audience": OAUTH_AUDIENCE,
            "code_challenge": challenge,
            "code_challenge_method": "S256",
            "state": state,
            "prompt": "login",
        }
    )
    return AuthorizeRequest(f"{AUTH_BASE_URL}/authorize?{query}", code_verifier, state)


def login_with_password(
    email: str, password: str, *, session: Any = None, timeout: int = DEFAULT_TIMEOUT_SECONDS
) -> dict[str, Any]:
    """Sign in with the account's email and password, as Resideo's sign-in page does.

    Resideo's bot detection can refuse this with `invalid_captcha`; then only the
    browser flow (build_authorize_request) can sign in.
    """

    http = session or requests.Session()
    request = build_authorize_request()
    response = _login_step(http, "GET", request.url, timeout, redirects=False)
    auth_state = parse_qs(urlparse(response.headers.get("Location", "")).query).get("state", [""])[0]
    if not auth_state:
        raise ResideoAuthError("Resideo's sign-in did not start: no state in its redirect")
    _login_step(http, "GET", f"{AUTH_BASE_URL}/login?{urlencode({'state': auth_state})}", timeout)
    csrf = http.cookies.get("_csrf", "")
    if not csrf:
        raise ResideoAuthError("Resideo's sign-in page set no _csrf cookie; its login flow may have changed")
    response = _login_step(
        http,
        "POST",
        f"{AUTH_BASE_URL}/usernamepassword/login",
        timeout,
        check=False,
        json={
            "client_id": OAUTH_CLIENT_ID,
            "redirect_uri": OAUTH_REDIRECT_URI,
            "tenant": OAUTH_TENANT,
            "response_type": "code",
            "scope": OAUTH_SCOPE,
            "audience": OAUTH_AUDIENCE,
            "_csrf": csrf,
            "state": auth_state,
            "_intstate": "deprecated",
            "username": email,
            "password": password,
            "connection": OAUTH_CONNECTION,
        },
        headers={"Auth0-Client": AUTH0_LOGIN_PAGE_CLIENT, "Origin": AUTH_BASE_URL},
    )
    wresult = re.search(r'name="wresult"\s+value="([^"]+)"', response.text)
    if response.status_code != 200 or not wresult:
        raise ResideoAuthError(f"Resideo refused the sign-in: {_login_error(response)}")
    form = {"wa": "wsignin1.0", "wresult": html.unescape(wresult.group(1))}
    wctx = re.search(r'name="wctx"\s+value="([^"]+)"', response.text)
    if wctx:
        form["wctx"] = html.unescape(wctx.group(1))
    response = _login_step(
        http,
        "POST",
        f"{AUTH_BASE_URL}/login/callback",
        timeout,
        redirects=False,
        data=form,
        headers={"Origin": AUTH_BASE_URL},
    )
    resume = response.headers.get("Location", "")
    if not resume:
        raise ResideoAuthError("Resideo's sign-in stopped after the password step: no redirect")
    if not resume.startswith("http"):
        resume = AUTH_BASE_URL + resume
    response = _login_step(http, "GET", resume, timeout, redirects=False)
    code = authorization_code_from_redirect(response.headers.get("Location", ""), expected_state=request.state)
    return exchange_authorization_code(code, code_verifier=request.code_verifier, timeout=timeout)


def _login_step(
    http: Any, method: str, url: str, timeout: int, *, redirects: bool = True, check: bool = True, **kwargs: Any
) -> Any:
    headers = {"User-Agent": USER_AGENT, **kwargs.pop("headers", {})}
    try:
        response = http.request(method, url, headers=headers, timeout=timeout, allow_redirects=redirects, **kwargs)
    except requests.RequestException as exc:
        raise ResideoTransientError(f"Resideo sign-in request failed: {type(exc).__name__}") from exc
    if check and response.status_code >= 400:
        path = urlparse(url).path
        raise _error_for_status(response.status_code, f"Resideo sign-in step {path} returned {response.status_code}")
    return response


def _login_error(response: Any) -> str:
    """Auth0's error code only: the body can repeat what was sent."""
    try:
        payload = response.json()
    except ValueError:
        return f"status {response.status_code}"
    return str(payload.get("code") or payload.get("error") or f"status {response.status_code}")


def authorization_code_from_redirect(pasted: str, *, expected_state: str) -> str:
    """The ``code`` from the redirect a browser could not open, as pasted."""

    query = parse_qs(urlparse(pasted.strip()).query)
    if error := query.get("error", [""])[0]:
        raise ResideoAuthError(f"Resideo refused the sign-in: {error}")
    code = query.get("code", [""])[0]
    if not code:
        raise ResideoAuthError("that URL has no code parameter; copy the whole redirect URL")
    if query.get("state", [""])[0] != expected_state:
        raise ResideoAuthError("that redirect belongs to a different sign-in attempt")
    return code


def exchange_authorization_code(
    code: str, *, code_verifier: str, timeout: int = DEFAULT_TIMEOUT_SECONDS, now: Callable[[], float] = time.time
) -> dict[str, Any]:
    return _token_request(
        {
            "grant_type": "authorization_code",
            "client_id": OAUTH_CLIENT_ID,
            "code": code,
            "code_verifier": code_verifier,
            "redirect_uri": OAUTH_REDIRECT_URI,
        },
        previous={},
        timeout=timeout,
        now=now,
    )


def refresh_token_json(
    token_json: str, *, timeout: int = DEFAULT_TIMEOUT_SECONDS, now: Callable[[], float] = time.time
) -> str:
    previous = json.loads(token_json)
    refresh_token = str(previous.get("refresh_token") or "")
    if not refresh_token:
        raise ResideoAuthError("the stored Resideo token has no refresh_token")
    token = _token_request(
        {"grant_type": "refresh_token", "client_id": OAUTH_CLIENT_ID, "refresh_token": refresh_token},
        previous=previous,
        timeout=timeout,
        now=now,
    )
    return json.dumps(token, sort_keys=True, separators=(",", ":"))


def _token_request(
    body: dict[str, str], *, previous: Mapping[str, Any], timeout: int, now: Callable[[], float]
) -> dict[str, Any]:
    try:
        response = requests.post(
            f"{AUTH_BASE_URL}/oauth/token", json=body, headers={"User-Agent": USER_AGENT}, timeout=timeout
        )
    except requests.RequestException as exc:
        raise ResideoTransientError(f"Resideo token request failed: {type(exc).__name__}") from exc
    if response.status_code != 200:
        # Auth0 answers a dead refresh token 400/401/403 `invalid_grant`. The
        # body is not echoed whole: it can repeat what was sent.
        status = response.status_code
        message = f"Resideo token request returned {status}: {_oauth_error(response)}"
        if 400 <= status < 500 and status != 429:
            raise ResideoAuthError(message)
        raise ResideoTransientError(message)
    payload = response.json()
    if not payload.get("access_token"):
        raise ResideoAuthError("Resideo's token response has no access_token")
    return {
        "access_token": payload["access_token"],
        # Auth0 does not always rotate; keep the old one when it did not.
        "refresh_token": payload.get("refresh_token") or previous.get("refresh_token") or "",
        "expires_at": now() + int(payload.get("expires_in") or 3600),
    }


def _oauth_error(response: requests.Response) -> str:
    try:
        return str(response.json().get("error") or "unknown")
    except ValueError:
        return "unknown"


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


class WarehouseTokenSource:
    """Access tokens from the one private row, refreshed under its row lock."""

    def __init__(
        self,
        *,
        warehouse: Any,
        account: str,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        now: Callable[[], float] = time.time,
    ) -> None:
        self._warehouse = warehouse
        self._account = account
        self._timeout = timeout
        self._now = now

    def access_token(self, *, force_refresh: bool = False) -> str:
        token_json = self._warehouse.load_resideo_oauth_token(account=self._account)
        if not token_json:
            raise ResideoAuthError(
                "no Resideo credential is installed; run `uv run personal-data-warehouse-resideo-auth`"
            )
        token = json.loads(token_json)
        fresh = float(token.get("expires_at") or 0) - self._now() > TOKEN_REFRESH_MARGIN_SECONDS
        if fresh and not force_refresh:
            return str(token["access_token"])
        rotated = self._warehouse.rotate_resideo_oauth_token(
            account=self._account,
            expected_token_json=token_json,
            rotate=lambda current: refresh_token_json(current, timeout=self._timeout, now=self._now),
            updated_at=datetime.fromtimestamp(self._now(), tz=UTC),
        )
        return str(json.loads(rotated)["access_token"])


class ResideoClient:
    def __init__(
        self,
        *,
        tokens: Any,
        subscription_key: str,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        session: Any = None,
    ) -> None:
        self._tokens = tokens
        self._subscription_key = subscription_key
        self._timeout = timeout
        self._session = session or requests.Session()

    def list_thermostats(self) -> list[dict[str, str]]:
        """Every device in the account graph, with its name and location."""

        graph = self._request("GET", "/ris-public-api/api/v1/accounts")
        devices: list[dict[str, str]] = []
        for user in (graph.get("data") or {}).get("consumerUsers") or []:
            for location in (user.get("consumerAccount") or {}).get("locations") or []:
                for consumer_device in location.get("consumerDevices") or []:
                    device = consumer_device.get("device") or {}
                    if device.get("deviceId"):
                        devices.append(
                            {
                                "device_id": str(device["deviceId"]),
                                "name": str(consumer_device.get("name") or ""),
                                "location_name": str(location.get("name") or ""),
                                "device_type": str(device.get("globalDeviceType") or ""),
                            }
                        )
        return devices

    def get_device(self, device_id: str) -> dict[str, Any]:
        return self._request("GET", f"/ris-public-api/api/v2/devices/thermostats/{device_id}")

    def get_configuration(self, device_id: str) -> dict[str, Any]:
        return self._request("GET", f"/ris-public-api/api/v2/devices/thermostats/{device_id}/configuration")

    def set_setpoint(self, device_id: str, *, kind: str, value: float, setpoint_status: str) -> dict[str, Any]:
        """Ask for a setpoint. A 202 means queued: read the device to confirm."""

        return self._request(
            "PUT",
            f"/ris-public-api/api/v1/devices/thermostats/{device_id}/{_SETPOINT_COMMANDS[kind]}",
            json_body={
                "SetpointStatus": setpoint_status,
                "SetpointValue": float(value),
                "ChannelId": COMMAND_CHANNEL_ID,
            },
        )

    def _request(self, method: str, path: str, *, json_body: Mapping[str, Any] | None = None) -> dict[str, Any]:
        response = self._send(method, path, json_body, self._tokens.access_token())
        if response.status_code == 401:
            # A token can die before its expiry; a second refusal is the answer.
            response = self._send(method, path, json_body, self._tokens.access_token(force_refresh=True))
        if not 200 <= response.status_code < 300:
            raise _error_for_status(response.status_code, f"Resideo {method} {path} returned {response.status_code}")
        return response.json() if response.content else {}

    def _send(self, method: str, path: str, json_body: Mapping[str, Any] | None, token: str) -> Any:
        try:
            return self._session.request(
                method,
                API_BASE_URL + path,
                json=json_body,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Ocp-Apim-Subscription-Key": self._subscription_key,
                    "Accept": "application/json",
                    "User-Agent": USER_AGENT,
                },
                timeout=self._timeout,
            )
        except requests.RequestException as exc:
            raise ResideoTransientError(f"Resideo {method} {path} failed: {type(exc).__name__}") from exc
