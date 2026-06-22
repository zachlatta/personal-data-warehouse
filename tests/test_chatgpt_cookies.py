from __future__ import annotations

import hashlib
from pathlib import Path
import sqlite3

import pytest
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from personal_data_warehouse import chatgpt_cookies as cc


def _key(password="s3cr3t"):
    return hashlib.pbkdf2_hmac("sha1", password.encode(), b"saltysalt", 1003, dklen=16)


def _encrypt(value: str, key: bytes, *, host_key: str = "", domain_prefix: bool = False) -> bytes:
    plaintext = value.encode("utf-8")
    if domain_prefix:
        plaintext = hashlib.sha256(host_key.encode()).digest() + plaintext
    pad = 16 - (len(plaintext) % 16)
    plaintext += bytes([pad]) * pad
    cipher = Cipher(algorithms.AES(key), modes.CBC(b" " * 16))
    enc = cipher.encryptor()
    return b"v10" + enc.update(plaintext) + enc.finalize()


def test_decrypt_value_roundtrip():
    key = _key()
    enc = _encrypt("token-value-123", key, host_key="chatgpt.com")
    assert cc._decrypt_value(enc, key=key, host_key="chatgpt.com", plain="") == "token-value-123"


def test_decrypt_strips_domain_hash_prefix():
    key = _key()
    enc = _encrypt("real-token", key, host_key=".chatgpt.com", domain_prefix=True)
    assert cc._decrypt_value(enc, key=key, host_key=".chatgpt.com", plain="") == "real-token"


def test_unencrypted_falls_back_to_plain():
    key = _key()
    assert cc._decrypt_value(b"", key=key, host_key="chatgpt.com", plain="plainval") == "plainval"


def test_cookie_header_orders_session_token_first():
    header = cc._cookie_header(
        [("cf_clearance", "cf"), ("__Secure-next-auth.session-token", "tok"), ("oai-did", "d")]
    )
    assert header.startswith("__Secure-next-auth.session-token=tok")
    assert "cf_clearance=cf" in header and "oai-did=d" in header


def _make_cookie_db(path: Path, rows):
    conn = sqlite3.connect(str(path))
    conn.execute(
        "CREATE TABLE cookies (host_key TEXT, name TEXT, encrypted_value BLOB, value TEXT)"
    )
    conn.executemany("INSERT INTO cookies VALUES (?,?,?,?)", rows)
    conn.commit()
    conn.close()


def test_discover_reads_session_from_browser(tmp_path, monkeypatch):
    key = _key()
    support = tmp_path / "Application Support"
    profile_dir = support / "Google/Chrome/Default"
    profile_dir.mkdir(parents=True)
    db = profile_dir / "Cookies"
    _make_cookie_db(
        db,
        [
            ("chatgpt.com", "__Secure-next-auth.session-token", _encrypt("sess-tok", key, host_key="chatgpt.com"), ""),
            ("chatgpt.com", "cf_clearance", _encrypt("cf-tok", key, host_key="chatgpt.com"), ""),
            ("example.com", "irrelevant", _encrypt("nope", key, host_key="example.com"), ""),
        ],
    )

    monkeypatch.setattr(cc, "_application_support", lambda: support)
    monkeypatch.setattr(cc, "_safe_storage_key", lambda profile: key)

    captured = cc.discover_chatgpt_session(browser="chrome")
    assert captured.browser == "Google Chrome"
    assert captured.has_session_token is True
    assert "__Secure-next-auth.session-token=sess-tok" in captured.cookie_header
    assert "cf_clearance=cf-tok" in captured.cookie_header
    # The non-chatgpt/openai cookie is excluded by the SQL filter.
    assert "irrelevant" not in captured.cookie_header


def test_discover_raises_when_no_session(tmp_path, monkeypatch):
    support = tmp_path / "Application Support"
    profile_dir = support / "Google/Chrome/Default"
    profile_dir.mkdir(parents=True)
    key = _key()
    _make_cookie_db(
        profile_dir / "Cookies",
        [("chatgpt.com", "cf_clearance", _encrypt("cf", key, host_key="chatgpt.com"), "")],
    )
    monkeypatch.setattr(cc, "_application_support", lambda: support)
    monkeypatch.setattr(cc, "_safe_storage_key", lambda profile: key)

    with pytest.raises(cc.ChatGPTCookieError):
        cc.discover_chatgpt_session(browser="chrome")


def test_unknown_browser_raises(monkeypatch):
    with pytest.raises(cc.ChatGPTCookieError):
        cc.discover_chatgpt_session(browser="netscape")
