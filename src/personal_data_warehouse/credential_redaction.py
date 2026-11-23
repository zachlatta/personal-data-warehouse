"""Credential boundary for conversation storage. Never logs candidate values.

Mirrored byte-for-byte in app/internal/server/credential_redaction.go and
pinned by tests/fixtures/credential_redaction_cases.json. Each secret becomes
``[REDACTED:<kind>:<first 6 hex of sha256(secret)>]`` so one leaked value stays
matchable across rows without being stored. Masking is not revocation: a secret
that reached a transcript must still be rotated.

Rules run in priority order; a lower-priority edit that overlaps an accepted
one is dropped. Valid JSON is walked leaf by leaf (decoded strings, at every
nesting level), and a ``"password": "<value>"``-style field masks its value.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import Counter

from personal_data_warehouse import card_redaction as _cr

_WS = ' \t\n\r\f\v'
_URL = re.compile(r'(?i)(?:postgres(?:ql)?|mysql|mariadb|rediss?|amqps?|mongodb(?:\+srv)?|https?)://'
                  r'([^ \t\n\r\f\v/@:?#\'"<>\[\]\\]*):([^ \t\n\r\f\v/@?#\'"<>\[\]\\]+)@')
_PDW = re.compile(r'(pdw_[a-z0-9]{12,24}_)([A-Za-z0-9_-]{30,})')
_SLACK = re.compile(r'xox[abcdepr](?:\.xox[a-z])?-[A-Za-z0-9%-]{10,}')
_GITHUB = re.compile(r'gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{22,}')
_SK = re.compile(r'sk-[A-Za-z0-9_-]{20,}')
_AWS_ID = re.compile(r'(?:AKIA|ASIA)[A-Z0-9]{16}')
_AWS_RUN = re.compile(r'[A-Za-z0-9/+=]+')
_PEM_HEAD = re.compile(r'-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----')
_PEM_FOOT = re.compile(r'-----END [A-Z0-9 ]*PRIVATE KEY-----')
_PEM_TAIL = re.compile(r'[A-Za-z0-9+/=\r\n\\]*')
_BASE64_CHAR = re.compile(r'[A-Za-z0-9+/]')
_BEARER = re.compile(r'(?i)bearer[ \t]+([A-Za-z0-9._~+/=-]{16,})')
_LABEL = (r'(?:secret[ _-]?access[ _-]?key|client[ _-]?secret|secret[ _-]?key|private[ _-]?key|'
          r'api[ _-]?key|apikey|passphrase|password|passwd|secret|token)')
# The label is matched alone and its identifier found by a bounded walk back:
# a lazy identifier prefix in the pattern is quadratic on long base64 runs.
_LABELLED = re.compile(r'(?i)(' + _LABEL + r')[ \t]*\\*["\']?[ \t]*[:=][ \t]*\\*["\']?[ \t]*'
                       r'([^ \t\n\r\f\v"\'\\`,;&|<>(){}\[\]]+)')
_LABEL_KEY = re.compile(r'(?i)[A-Za-z0-9_. -]*?' + _LABEL)
_NOT_SECRET_LABEL = re.compile(r'(?i)(?:^|[_. -])(?:page|next|prev|previous|continuation|sync|cursor|csrf|xsrf|'
                               r'idempotency|pagination|max|min|num|total|input|output|cache|count|reasoning|'
                               r'completion|prompt|email|visit|client|set)[ _-]?token$')
# camelCase identifiers (nextPageToken) are split before the check above.
_CAMEL = re.compile(r'([a-z0-9])([A-Z])')
# RFC 2606 example hosts: a URL pointing at one is an example, not a login.
_URL_HOST = re.compile(r'[^:/?#@ \t\n\r\f\v\'"<>\[\]\\]*')
_EXAMPLE_HOSTS = ('example', 'example.com', 'example.net', 'example.org')
# Real Slack tokens are 40+ characters; shorter ones are test or doc fakes.
_SLACK_MIN = 40
_ENV_CHARS = frozenset('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_')
_PLACEHOLDER_WORDS = ('...', '***', 'xxxx', 'redacted', 'example', 'placeholder', 'changeme', 'your', 'dummy')
_PLACEHOLDER_PASSWORDS = frozenset(('password', 'pass', 'passwd', 'pwd', 'secret', 'changeme', 'admin', 'root', 'guest'))
_WORD = frozenset('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_')
_IDENTIFIER = _WORD | frozenset('.-')
_IDENTIFIER_WALK = 128
# A backslash ends a run too: escaped JSON quoted inside non-JSON text.
_DELIMITERS = frozenset(' \t\n\r\f\v"\'=:,;()[]{}<>|`\\')
_MARKER_KIND = re.compile(r'\[REDACTED:([a-z_]+):[0-9a-f]{6}\]')


# Maintenance scan prefilters: a case-insensitive superset of every rule's
# anchor, applied after removing all backslashes. JSON can escape any byte: an
# escaped tab leaves 't' and a \uXXXX unit leaves 'u00XX'. Only units for
# characters an anchor can contain are kept (tab and printable ASCII except
# '&', '<', '>' and '?', which Go escapes routinely and no anchor uses). An sk-
# key's left boundary may be an escape letter or unit. Python confirms each row.
_PREFILTER_HEAD = (r"pdw_|u00(09|2[0-57-9a-f]|3[0-9abd]|[4-7][0-9a-f])|xox[abcdepr]|gh[pousr]_|github_pat_|"
                   r"(^|[^a-z0-9_-]|[ntrbf]|u00[0-9a-f]{2})sk-[a-z0-9_-]{20}|private key|bearer|"
                   r"(akia|asia)[a-z0-9]{16}|")
SQL_PREFILTER = (_PREFILTER_HEAD + r"://[^[:space:]/]*@|" + _LABEL + r"""[[:space:]"'t]*[:=]""")
_PREFILTER = re.compile('(?i)' + _PREFILTER_HEAD + r"://[^\s/]*@|" + _LABEL + r"""[\s"'t]*[:=]""")


def might_contain_credential(value: str) -> bool:
    return bool(_PREFILTER.search(value.replace('\\', '')))


def marker(kind: str, value: str) -> str:
    digest = hashlib.sha256(value.encode('utf-8', 'surrogatepass')).hexdigest()[:6]
    return '[REDACTED:' + kind + ':' + digest + ']'


def _has_letter_and_digit(value):
    return (any('a' <= c <= 'z' or 'A' <= c <= 'Z' for c in value) and
            any('0' <= c <= '9' for c in value))


def _placeholder(value):
    lower = value.lower()
    return (value[0] in '$%<{*[.~/=#' or len(set(value)) == 1 or
            any(word in lower for word in _PLACEHOLDER_WORDS))


def _secret_value(value):
    return (len(value) >= 8 and _has_letter_and_digit(value) and not _placeholder(value) and
            not ('_' in value and all(c in _ENV_CHARS for c in value)))


def _not_secret_label(label):
    return bool(_NOT_SECRET_LABEL.search(_CAMEL.sub(r'\1_\2', label)))


def _example_host(host):
    host = host.lower()
    return host in _EXAMPLE_HOSTS or host.endswith(tuple('.' + h for h in _EXAMPLE_HOSTS))


def _boundary(value, start, extra=''):
    return start == 0 or value[start - 1] not in _WORD and value[start - 1] not in extra


def _text_edits(value):
    candidates = []
    for head in _PEM_HEAD.finditer(value):
        foot = _PEM_FOOT.search(value, head.end())
        end = foot.start() if foot else _PEM_TAIL.match(value, head.end()).end()
        body = value[head.end():end]
        if len(_BASE64_CHAR.findall(body)) >= 64:
            candidates.append((head.end(), end, 'private_key'))
    for m in _URL.finditer(value):
        user, password = m[1], m[2]
        if (len(password) >= 6 and password != user and not _placeholder(password) and
                password.lower() not in _PLACEHOLDER_PASSWORDS and not _example_host(_URL_HOST.match(value, m.end())[0])):
            candidates.append((m.start(2), m.end(2), 'url_password'))
    for m in _PDW.finditer(value):
        candidates.append((m.start(2), m.end(2), 'pdw_token'))
    for m in _SLACK.finditer(value):
        if (len(m[0]) >= _SLACK_MIN and _boundary(value, m.start()) and _has_letter_and_digit(m[0][5:]) and
                not _placeholder(m[0])):
            candidates.append((m.start(), m.end(), 'slack_token'))
    for m in _GITHUB.finditer(value):
        if _boundary(value, m.start(), '-') and _has_letter_and_digit(m[0]) and not _placeholder(m[0]):
            candidates.append((m.start(), m.end(), 'github_token'))
    for m in _SK.finditer(value):
        # OpenSSH security-key algorithms (sk-ssh-ed25519@openssh.com) are names.
        if (_boundary(value, m.start(), '-/.') and value[m.end():m.end() + 1] != '@' and
                _has_letter_and_digit(m[0][3:]) and not _placeholder(m[0])):
            candidates.append((m.start(), m.end(), 'anthropic_key' if m[0].startswith('sk-ant-') else 'openai_key'))
    if any(_boundary(value, m.start()) and (m.end() == len(value) or value[m.end()] not in _WORD)
           for m in _AWS_ID.finditer(value)):
        for m in _AWS_RUN.finditer(value):
            run = m[0]
            # The run must stand alone: one cut out of a URL path or encoding
            # (.com/..., -2B..., %2F...) is part of a longer value.
            if (len(run) == 40 and '=' not in run and
                    (m.start() == 0 or value[m.start() - 1] in _DELIMITERS) and
                    (m.end() == len(value) or value[m.end()] in _DELIMITERS) and any(c.isascii() and c.isupper() for c in run) and
                    any(c.isascii() and c.islower() for c in run) and any('0' <= c <= '9' for c in run) and
                    not _placeholder(run)):
                candidates.append((m.start(), m.end(), 'aws_secret_key'))
    for m in _BEARER.finditer(value):
        if _boundary(value, m.start()) and _secret_value(m[1]):
            candidates.append((m.start(1), m.end(1), 'bearer_token'))
    for m in _LABELLED.finditer(value):
        start = m.start(1)
        while start > max(0, m.start(1) - _IDENTIFIER_WALK) and value[start - 1] in _IDENTIFIER:
            start -= 1
        label, secret = value[start:m.end(1)], m[2]
        # A label ending a path component (/etc/passwd:) names a file.
        if (_not_secret_label(label) or not _secret_value(secret) or start > 0 and value[start - 1] == '/' or
                m.end() < len(value) and value[m.end()] == '('):
            continue
        candidates.append((m.start(2), m.end(2), 'password' if 'pass' in label.lower() else 'secret'))
    accepted = []
    for start, end, kind in candidates:
        if not any(start < b and end > a for a, b, _ in accepted):
            accepted.append((start, end, marker(kind, value[start:end])))
    return sorted(accepted)


def credential_edits(value: str):
    """(start, end, replacement) spans; JSON lexemes outside them are untouched."""
    tokens = _cr._json_tokens(value)
    if tokens is None:
        return _text_edits(value)
    edits = []
    for index, token in enumerate(tokens):
        raw = token[0]
        if not raw.startswith('"'):
            continue
        decoded = json.loads(raw)
        inner = credential_edits(decoded)
        if not inner and index >= 2 and tokens[index - 1][0] == ':' and tokens[index - 2][0].startswith('"'):
            key = json.loads(tokens[index - 2][0])
            if (len(key) <= _IDENTIFIER_WALK and _LABEL_KEY.fullmatch(key) and not _not_secret_label(key) and
                    not any(c in _WS for c in decoded) and _secret_value(decoded)):
                inner = [(0, len(decoded), marker('password' if 'pass' in key.lower() else 'secret', decoded))]
        if inner:
            bounds = _cr._json_string_bounds(raw)
            edits.extend((token.start() + bounds[a], token.start() + bounds[b], replacement)
                         for a, b, replacement in inner)
    return edits


def credential_count(value: str) -> int:
    return len(credential_edits(value))


def credential_kinds(value: str) -> Counter:
    return Counter(_MARKER_KIND.match(replacement)[1] for _, _, replacement in credential_edits(value))


def redact_credentials(value: str) -> str:
    return _cr._apply_edits(value, credential_edits(value))
