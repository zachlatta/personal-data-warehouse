"""Slack *client-session* HTTP helper for the reviewed Slack writes.

Sending a message as Zach and marking a conversation read need a real logged-in
session: two pieces which are useless apart, an ``xoxc-`` token from the web
client's localStorage and the ``d`` cookie. (The sync used to spend it on
``client.counts`` too, to learn which conversations moved; Slack refused that
with ``team_is_restricted`` on 2026-10-01 and the sync now polls instead.)

Zach pastes that pair from a browser into ``pdw slack publish-session``
(app/internal/browsersessions/slack), together with the browser's own
User-Agent; the app stores all three in ``private.slack_sessions``. It is never
captured on a schedule: an hourly capture from the desktop app, calling Slack
from Go with the desktop's own login, got him signed out of every device on
each run (2026-09-29). This module is the server-side half: the one HTTP shape
``slack_mutations`` uses to spend that session.

Nothing here logs a secret.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


#: Sent only when a session carries no User-Agent of its own -- a bare token
#: pasted without the console snippet's JSON. Every session pasted the normal
#: way carries the minting browser's, and that is the one sent.
FALLBACK_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
)


def _slack_post(
    method: str,
    *,
    token: str,
    cookie_header: str,
    user_agent: str,
    form: Mapping[str, str] | None = None,
    query: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """POST to Slack with a *client* session (token + `d` cookie).

    Both parts are required together: the token alone returns `not_authed`, and
    the cookie alone has nothing to authorise. ``user_agent`` is the browser the
    session was pasted from (``private.slack_sessions.user_agent``): a session
    that suddenly presents a User-Agent it has never used is one of the signals
    Slack's anomaly detection signs a user out for.
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
            "User-Agent": user_agent or FALLBACK_USER_AGENT,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return _json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return {"ok": False, "error": f"http_{exc.code}"}
    except (OSError, ValueError) as exc:  # pragma: no cover - network dependent
        return {"ok": False, "error": str(exc)}


__all__ = ["FALLBACK_USER_AGENT", "_slack_post"]
