"""Browser-bootstrap flow for ``pdw chatgpt publish-session``.

Capturing a chatgpt.com session needs a logged-in Chrome-family browser. This
module makes that a turnkey part of the command: if none is installed it can
install one (Homebrew cask, macOS), then open chatgpt.com and wait for the user
to sign in. Every side effect (install, open URL, prompt) is injected so the
flow is unit-testable and so non-interactive callers can opt out.
"""

from __future__ import annotations

from collections.abc import Callable
import shutil
import subprocess
import sys
import time

from personal_data_warehouse.chatgpt_cookies import (
    DEFAULT_INSTALL_BROWSER,
    BrowserProfile,
    CapturedSession,
    ChatGPTCookieError,
    browser_by_key,
    discover_chatgpt_session,
    installed_browsers,
)

# Injectable seams (overridden in tests).
Runner = Callable[[list[str]], int]
Opener = Callable[[str], None]
Prompter = Callable[[str], str]
Printer = Callable[[str], None]


def _default_runner(argv: list[str]) -> int:
    return subprocess.run(argv).returncode


def _default_opener(target: str) -> None:
    subprocess.run(["open", target], check=False)


def _stderr_print(message: str) -> None:
    print(message, file=sys.stderr)


def ensure_browser(
    *,
    prefer: str | None = None,
    auto_install: bool = True,
    runner: Runner = _default_runner,
    log: Printer = _stderr_print,
) -> BrowserProfile:
    """Return a Chrome-family browser to use, installing one if necessary.

    ``prefer`` forces a specific browser key. When nothing suitable is installed
    and ``auto_install`` is set, installs ``DEFAULT_INSTALL_BROWSER`` via Homebrew
    (macOS). Raises ``ChatGPTCookieError`` if no browser can be made available.
    """
    if prefer:
        profile = browser_by_key(prefer)
        if profile is None:
            raise ChatGPTCookieError(f"unknown browser {prefer!r}")
        if profile.app_bundle and _is_installed(profile):
            return profile
        if not auto_install:
            raise ChatGPTCookieError(f"{profile.display_name} is not installed")
        _install(profile, runner=runner, log=log)
        return profile

    installed = installed_browsers()
    if installed:
        return installed[0]

    if not auto_install:
        raise ChatGPTCookieError(
            "no Chrome-family browser is installed; install Chrome/Brave/Edge/Arc and log into chatgpt.com"
        )
    target = browser_by_key(DEFAULT_INSTALL_BROWSER)
    assert target is not None
    _install(target, runner=runner, log=log)
    return target


def _is_installed(profile: BrowserProfile) -> bool:
    from pathlib import Path

    return bool(profile.app_bundle) and Path(profile.app_bundle).exists()


def _install(profile: BrowserProfile, *, runner: Runner, log: Printer) -> None:
    if sys.platform != "darwin":
        raise ChatGPTCookieError(
            f"cannot auto-install {profile.display_name} on this platform; install it manually"
        )
    if not profile.homebrew_cask or shutil.which("brew") is None:
        raise ChatGPTCookieError(
            f"Homebrew is required to auto-install {profile.display_name}; install it manually "
            "(https://brew.sh) or install the browser yourself"
        )
    log(f"Installing {profile.display_name} via Homebrew (brew install --cask {profile.homebrew_cask})...")
    code = runner(["brew", "install", "--cask", profile.homebrew_cask])
    if code != 0:
        raise ChatGPTCookieError(f"`brew install --cask {profile.homebrew_cask}` failed (exit {code})")
    if not _is_installed(profile):
        raise ChatGPTCookieError(f"{profile.display_name} did not appear after install")
    log(f"Installed {profile.display_name}.")


def ensure_logged_in(
    profile: BrowserProfile,
    *,
    opener: Opener = _default_opener,
    prompt: Prompter = input,
    log: Printer = _stderr_print,
    max_attempts: int = 3,
) -> CapturedSession:
    """Return a captured chatgpt.com session, guiding the user to log in.

    Tries to read the session; if absent, opens chatgpt.com in ``profile`` and
    waits for the user to confirm sign-in, retrying up to ``max_attempts``.
    """
    last_error: ChatGPTCookieError | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return discover_chatgpt_session(browser=profile.key)
        except ChatGPTCookieError as exc:
            last_error = exc
            if attempt == max_attempts:
                break
            log(
                f"No logged-in chatgpt.com session in {profile.display_name} yet. "
                f"Opening chatgpt.com; please sign in (attempt {attempt}/{max_attempts - 1})."
            )
            if profile.app_bundle:
                opener(profile.app_bundle)
                time.sleep(0.5)
            opener("https://chatgpt.com/")
            prompt("Press Enter once you have signed in to chatgpt.com... ")
    raise last_error or ChatGPTCookieError("could not capture a chatgpt.com session")
