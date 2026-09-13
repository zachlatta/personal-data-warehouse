"""Shared plumbing for mutations that drive a macOS app over AppleScript.

Apple Notes and Apple Contacts have no server API, so their mutation executors ask the
app itself, on a Mac that is signed in, through ``osascript``. Everything here is the
part that does not depend on which app: string escaping (AppleScript has no parameter
binding, so an unescaped quote is arbitrary script execution, not a failed write), the
subprocess runner, and the mapping from an osascript failure onto the mutation worker's
status vocabulary.
"""

from __future__ import annotations

import subprocess

# osascript inherits the target app's own AppleEvent timeout, and both Notes and Contacts
# can be slow while iCloud is pulling. Bound the subprocess well above the in-script
# `with timeout` so a hung app surfaces as our timeout with our message.
DEFAULT_SCRIPT_TIMEOUT_SECONDS = 180
IN_SCRIPT_TIMEOUT_SECONDS = 120


def applescript_string(value: str) -> str:
    """Render a Python string as an AppleScript string expression.

    Quotes and backslashes are escaped. Newlines cannot appear inside an AppleScript
    string literal at all, so they are spliced in as `linefeed` terms -- which is why
    this returns an *expression* rather than a literal.
    """

    text = "" if value is None else str(value)
    parts = []
    for segment in text.split("\n"):
        escaped = segment.replace("\\", "\\\\").replace('"', '\\"')
        parts.append(f'"{escaped}"')
    return " & linefeed & ".join(parts)


def run_osascript(script: str, *, timeout: int = DEFAULT_SCRIPT_TIMEOUT_SECONDS) -> str:
    completed = subprocess.run(
        ["/usr/bin/osascript", "-"],
        input=script,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if completed.returncode != 0:
        raise RuntimeError((completed.stderr or completed.stdout or "osascript failed").strip())
    return completed.stdout.rstrip("\n")


def dedent_script(script: str) -> str:
    return "\n".join(line.strip() for line in script.strip().splitlines())


def classify_osascript_error(message: str, *, app_name: str) -> tuple[str, str]:
    """Map an osascript failure onto (status, error).

    The distinction that matters: a missing record will never appear, so retrying it
    forever is noise; a busy or unlaunched app is transient; a refused Automation grant
    needs a human at that Mac and is not a code failure at all.
    """

    if "-1743" in message or "Not authorized to send Apple events" in message:
        return (
            "blocked_missing_credentials",
            f"Automation permission for {app_name} is not granted to this worker. "
            "Grant it in System Settings > Privacy & Security > Automation. "
            f"osascript said: {message}",
        )
    if "-1712" in message or "timed out" in message.lower():
        return ("failed_retryable", message)
    if "-1728" in message or "-2753" in message or "Invalid key form" in message:
        return ("failed_terminal", message)
    if "-600" in message or "not running" in message.lower():
        return ("failed_retryable", message)
    return ("failed_terminal", message)
