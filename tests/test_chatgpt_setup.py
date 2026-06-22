from __future__ import annotations

import pytest

from personal_data_warehouse import chatgpt_setup as setup
from personal_data_warehouse.chatgpt_cookies import (
    CapturedSession,
    ChatGPTCookieError,
)


def test_ensure_browser_returns_installed(monkeypatch):
    profile = setup.browser_by_key("chrome")
    monkeypatch.setattr(setup, "installed_browsers", lambda: [profile])
    runs = []
    got = setup.ensure_browser(runner=lambda argv: runs.append(argv) or 0)
    assert got.key == "chrome"
    assert runs == []  # nothing installed


def test_ensure_browser_auto_installs_brave_when_none(monkeypatch):
    monkeypatch.setattr(setup, "installed_browsers", lambda: [])
    monkeypatch.setattr(setup.sys, "platform", "darwin")
    monkeypatch.setattr(setup.shutil, "which", lambda _name: "/opt/homebrew/bin/brew")
    # Pretend the app appears right after the (stubbed) brew install.
    monkeypatch.setattr(setup, "_is_installed", lambda profile: True)
    runs = []
    got = setup.ensure_browser(runner=lambda argv: runs.append(argv) or 0, log=lambda _m: None)
    assert got.key == "brave"
    assert runs == [["brew", "install", "--cask", "brave-browser"]]


def test_ensure_browser_no_install_raises(monkeypatch):
    monkeypatch.setattr(setup, "installed_browsers", lambda: [])
    with pytest.raises(ChatGPTCookieError):
        setup.ensure_browser(auto_install=False)


def test_ensure_browser_install_failure_raises(monkeypatch):
    monkeypatch.setattr(setup, "installed_browsers", lambda: [])
    monkeypatch.setattr(setup.sys, "platform", "darwin")
    monkeypatch.setattr(setup.shutil, "which", lambda _name: "/opt/homebrew/bin/brew")
    with pytest.raises(ChatGPTCookieError):
        setup.ensure_browser(runner=lambda argv: 1, log=lambda _m: None)


def test_ensure_logged_in_returns_session_when_present(monkeypatch):
    profile = setup.browser_by_key("brave")
    session = CapturedSession(browser="Brave", cookie_header="hdr", cookie_count=2, has_session_token=True)
    monkeypatch.setattr(setup, "discover_chatgpt_session", lambda browser=None: session)
    got = setup.ensure_logged_in(profile, opener=lambda _t: None, prompt=lambda _p: "", log=lambda _m: None)
    assert got.cookie_header == "hdr"


def test_ensure_logged_in_prompts_then_succeeds(monkeypatch):
    profile = setup.browser_by_key("brave")
    calls = {"n": 0}
    session = CapturedSession(browser="Brave", cookie_header="hdr", cookie_count=2, has_session_token=True)

    def _discover(browser=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ChatGPTCookieError("not logged in yet")
        return session

    opened = []
    monkeypatch.setattr(setup, "discover_chatgpt_session", _discover)
    monkeypatch.setattr(setup.time, "sleep", lambda _s: None)
    got = setup.ensure_logged_in(
        profile, opener=lambda t: opened.append(t), prompt=lambda _p: "", log=lambda _m: None
    )
    assert got.cookie_header == "hdr"
    assert "https://chatgpt.com/" in opened


def test_ensure_logged_in_gives_up_after_attempts(monkeypatch):
    profile = setup.browser_by_key("brave")

    def _discover(browser=None):
        raise ChatGPTCookieError("never logs in")

    monkeypatch.setattr(setup, "discover_chatgpt_session", _discover)
    monkeypatch.setattr(setup.time, "sleep", lambda _s: None)
    with pytest.raises(ChatGPTCookieError):
        setup.ensure_logged_in(
            profile, opener=lambda _t: None, prompt=lambda _p: "", log=lambda _m: None, max_attempts=2
        )
