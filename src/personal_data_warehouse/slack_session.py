"""Slack *client-session* HTTP helper for the Dagster-side sync.

Slack's public Web API cannot tell us *which* conversations changed:
``conversations.list`` returns no last-message marker at all (only ``updated``,
which tracks topic/member edits). Slack's own client answers that question in a
single request (``client.counts``), but only for a real logged-in session. That
session is two pieces which are useless apart: an ``xoxc-`` token from the
desktop app's localStorage and the ``d`` cookie.

Capturing that pair from the Mac signed in to Slack is the Go CLI's job
(``pdw slack publish-session``, app/internal/browsersessions/slack); the app
stores it in ``private.slack_sessions``. This module is the server-side half:
the one HTTP shape every Dagster caller (``slack_change_feed``,
``slack_mutations``, ``defs/slack_sync``) uses to spend that session.

Nothing here logs a secret.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def _slack_post(
    method: str,
    *,
    token: str,
    cookie_header: str,
    form: Mapping[str, str] | None = None,
    query: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """POST to Slack with a *client* session (token + `d` cookie).

    Both parts are required together: the token alone returns `not_authed`, and
    the cookie alone has nothing to authorise. The browser-shaped headers are not
    decoration -- Slack's client endpoints reject requests that do not look like
    the web app.
    """
    import json as _json
    import urllib.error
    import urllib.parse
    import urllib.request

    body = urllib.parse.urlencode({"token": token, **(form or {})}).encode("utf-8")
    url = f"https://slack.com/api/{method}"
    if query:
        # The web client scopes some calls on the URL (slack_route=E:T on an
        # Enterprise Grid session); the form body is the token's.
        url += "?" + urllib.parse.urlencode(dict(query))
    request = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            "Cookie": cookie_header,
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Slack_SSB/4.36.140",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return _json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return {"ok": False, "error": f"http_{exc.code}"}
    except (OSError, ValueError) as exc:  # pragma: no cover - network dependent
        return {"ok": False, "error": str(exc)}


def slack_auth_test(*, token: str, cookie_header: str) -> dict[str, Any]:
    return _slack_post("auth.test", token=token, cookie_header=cookie_header)


__all__ = ["_slack_post", "slack_auth_test"]
