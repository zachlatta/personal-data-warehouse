"""Read the chatgpt.com web session from a local Chrome-family browser (macOS).

Chrome/Brave/Edge/Arc store cookies in a SQLite ``Cookies`` DB with values
AES-128-CBC encrypted under a key derived (PBKDF2-HMAC-SHA1, 1003 rounds,
``b'saltysalt'``) from the browser's "Safe Storage" keychain password. That
password lives in the *legacy* login keychain, so unlike the ChatGPT desktop
app's entitlement-locked key, it is readable with the user's consent (a
one-time "allow" prompt from ``security``).

``discover_chatgpt_session`` returns the full chatgpt.com ``Cookie`` header
(session token, any NextAuth chunk cookies, and Cloudflare cookies), which the
backend client replays. Decryption uses the standard library plus
``cryptography`` for AES; no key material is logged.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tempfile

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

SESSION_TOKEN_PREFIX = "__Secure-next-auth.session-token"
_PBKDF2_SALT = b"saltysalt"
_PBKDF2_ROUNDS = 1003
_PBKDF2_KEYLEN = 16
_AES_IV = b" " * 16


class ChatGPTCookieError(RuntimeError):
    """No readable, logged-in chatgpt.com session was found locally."""


@dataclass(frozen=True)
class BrowserProfile:
    key: str  # cli-facing name, e.g. "chrome"
    display_name: str
    support_subdir: str  # under ~/Library/Application Support
    safe_storage_service: str
    safe_storage_account: str
    app_bundle: str = ""  # /Applications path, for "is it installed?"
    homebrew_cask: str = ""  # cask to `brew install --cask`, when auto-installing


# Ordered by practical local-default likelihood; auto-detect walks this list.
BROWSERS: tuple[BrowserProfile, ...] = (
    BrowserProfile("chrome", "Google Chrome", "Google/Chrome", "Chrome Safe Storage", "Chrome", "/Applications/Google Chrome.app", "google-chrome"),
    BrowserProfile("brave", "Brave", "BraveSoftware/Brave-Browser", "Brave Safe Storage", "Brave", "/Applications/Brave Browser.app", "brave-browser"),
    BrowserProfile("edge", "Microsoft Edge", "Microsoft Edge", "Microsoft Edge Safe Storage", "Microsoft Edge", "/Applications/Microsoft Edge.app", "microsoft-edge"),
    BrowserProfile("arc", "Arc", "Arc/User Data", "Arc Safe Storage", "Arc", "/Applications/Arc.app", "arc"),
    BrowserProfile("chromium", "Chromium", "Chromium", "Chromium Safe Storage", "Chromium", "/Applications/Chromium.app", "chromium"),
    BrowserProfile("vivaldi", "Vivaldi", "Vivaldi", "Vivaldi Safe Storage", "Vivaldi", "/Applications/Vivaldi.app", "vivaldi"),
)

# Default browser to auto-install when none is present.
DEFAULT_INSTALL_BROWSER = "brave"


def installed_browsers() -> list[BrowserProfile]:
    """Chrome-family browsers whose app bundle is present in /Applications."""
    return [b for b in BROWSERS if b.app_bundle and Path(b.app_bundle).exists()]


@dataclass(frozen=True)
class CapturedSession:
    browser: str
    cookie_header: str
    cookie_count: int
    has_session_token: bool


def _application_support() -> Path:
    return Path(os.path.expanduser("~/Library/Application Support"))


def browser_by_key(key: str) -> BrowserProfile | None:
    for profile in BROWSERS:
        if profile.key == key.lower():
            return profile
    return None


def _cookie_dbs(profile: BrowserProfile) -> list[Path]:
    base = _application_support() / profile.support_subdir
    if not base.is_dir():
        return []
    # Cookies live per-profile, historically at "<profile>/Cookies" and on newer
    # Chromium at "<profile>/Network/Cookies". Search both, dedup, stable order.
    found: list[Path] = []
    for pattern in ("Cookies", "*/Cookies", "Network/Cookies", "*/Network/Cookies"):
        for path in sorted(base.glob(pattern)):
            if path.is_file() and path not in found:
                found.append(path)
    return found


def _safe_storage_key(profile: BrowserProfile) -> bytes:
    try:
        completed = subprocess.run(
            [
                "security",
                "find-generic-password",
                "-w",
                "-s",
                profile.safe_storage_service,
                "-a",
                profile.safe_storage_account,
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env dependent
        raise ChatGPTCookieError(f"could not run `security` to read {profile.safe_storage_service}: {exc}") from exc
    if completed.returncode != 0:
        raise ChatGPTCookieError(
            f"could not read {profile.safe_storage_service!r} from the keychain "
            f"(is {profile.display_name} installed and have you allowed access?): "
            f"{completed.stderr.strip()}"
        )
    password = completed.stdout.strip("\n")
    return hashlib.pbkdf2_hmac("sha1", password.encode("utf-8"), _PBKDF2_SALT, _PBKDF2_ROUNDS, dklen=_PBKDF2_KEYLEN)


def _decrypt_value(encrypted: bytes, *, key: bytes, host_key: str, plain: str) -> str:
    if not encrypted:
        return plain or ""
    version = encrypted[:3]
    if version not in (b"v10", b"v11"):
        # Unencrypted or unknown scheme: fall back to the plaintext column.
        try:
            return encrypted.decode("utf-8")
        except UnicodeDecodeError:
            return plain or ""
    cipher = Cipher(algorithms.AES(key), modes.CBC(_AES_IV))
    decryptor = cipher.decryptor()
    decrypted = decryptor.update(encrypted[3:]) + decryptor.finalize()
    decrypted = _strip_pkcs7(decrypted)
    # Newer Chromium prepends the SHA-256 of the cookie's host_key to the value.
    domain_hash = hashlib.sha256(host_key.encode("utf-8")).digest()
    if decrypted[:32] == domain_hash:
        decrypted = decrypted[32:]
    return decrypted.decode("utf-8", errors="replace")


def _strip_pkcs7(data: bytes) -> bytes:
    if not data:
        return data
    pad = data[-1]
    if 1 <= pad <= 16 and len(data) >= pad:
        return data[:-pad]
    return data


def _read_chatgpt_cookies(db_path: Path, key: bytes) -> list[tuple[str, str]]:
    with tempfile.TemporaryDirectory() as tmp:
        # Copy DB (+ WAL/SHM) so a running browser's lock doesn't block us and
        # so any pending WAL writes are visible.
        local = Path(tmp) / "Cookies"
        shutil.copy(db_path, local)
        for suffix in ("-wal", "-shm"):
            sidecar = Path(str(db_path) + suffix)
            if sidecar.exists():
                shutil.copy(sidecar, Path(str(local) + suffix))
        connection = sqlite3.connect(str(local))
        try:
            rows = connection.execute(
                """
                SELECT host_key, name, encrypted_value, value
                FROM cookies
                WHERE host_key LIKE '%chatgpt.com' OR host_key LIKE '%openai.com'
                ORDER BY name
                """
            ).fetchall()
        finally:
            connection.close()

    cookies: list[tuple[str, str]] = []
    for host_key, name, encrypted_value, value in rows:
        decoded = _decrypt_value(
            bytes(encrypted_value) if encrypted_value is not None else b"",
            key=key,
            host_key=str(host_key),
            plain=str(value or ""),
        )
        if decoded:
            cookies.append((str(name), decoded))
    return cookies


def _cookie_header(cookies: Iterable[tuple[str, str]]) -> str:
    # De-dupe by name (later wins), session-token chunks first for readability.
    by_name: dict[str, str] = {}
    for name, value in cookies:
        by_name[name] = value
    ordered = sorted(by_name.items(), key=lambda kv: (not kv[0].startswith(SESSION_TOKEN_PREFIX), kv[0]))
    return "; ".join(f"{name}={value}" for name, value in ordered)


def discover_chatgpt_session(*, browser: str | None = None) -> CapturedSession:
    """Find a logged-in chatgpt.com session and return its full Cookie header.

    When ``browser`` is given, only that browser is tried; otherwise each
    installed Chrome-family browser is tried in turn.
    """
    if browser:
        profile = browser_by_key(browser)
        if profile is None:
            valid = ", ".join(p.key for p in BROWSERS)
            raise ChatGPTCookieError(f"unknown browser {browser!r}; valid: {valid}")
        candidates = [profile]
    else:
        candidates = list(BROWSERS)

    problems: list[str] = []
    for profile in candidates:
        dbs = _cookie_dbs(profile)
        if not dbs:
            continue
        try:
            key = _safe_storage_key(profile)
        except ChatGPTCookieError as exc:
            problems.append(str(exc))
            continue
        for db in dbs:
            try:
                cookies = _read_chatgpt_cookies(db, key)
            except sqlite3.Error as exc:  # pragma: no cover - env dependent
                problems.append(f"{profile.display_name}: could not read {db}: {exc}")
                continue
            has_token = any(name.startswith(SESSION_TOKEN_PREFIX) for name, _ in cookies)
            if has_token:
                return CapturedSession(
                    browser=profile.display_name,
                    cookie_header=_cookie_header(cookies),
                    cookie_count=len({name for name, _ in cookies}),
                    has_session_token=True,
                )

    detail = ("; ".join(problems)) if problems else (
        "no Chrome-family browser with a chatgpt.com login was found"
    )
    raise ChatGPTCookieError(
        "could not find a logged-in chatgpt.com session in a local browser. "
        "Open Chrome/Brave/Edge/Arc, log into chatgpt.com, then retry. " + detail
    )
