"""Fixed-seed credential corpus, mirrored in app/internal/server/credential_redaction_property_test.go.

Secrets are joined from parts at test time so the public fixture holds no
token-shaped string. Both languages build the same corpus (pinned by
``corpus_sha256``) and must produce exactly the expected masked text.
"""
import hashlib
import json
import re
from pathlib import Path

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'credential_redaction_cases.json').read_text())
_TEMPLATE = re.compile(r'<<(?:mask:([a-z_]+):)?([a-z0-9]+)>>')
MARKER = re.compile(r'\[REDACTED:[a-z_]+:[0-9a-f]{6}\]')


def secret(name):
    return ''.join(FIXTURE['secrets'][name])


def expand(template):
    def replace(match):
        value = secret(match[2])
        if match[1] is None:
            return value
        return '[REDACTED:' + match[1] + ':' + hashlib.sha256(value.encode()).hexdigest()[:6] + ']'
    return _TEMPLATE.sub(replace, template)


def json_quote(value):
    out = ['"']
    for char in value:
        if char == '\\':
            out.append('\\\\')
        elif char == '"':
            out.append('\\"')
        elif char == '\n':
            out.append('\\n')
        elif char == '\t':
            out.append('\\t')
        elif char == '\r':
            out.append('\\r')
        elif ord(char) < 0x20:
            out.append('\\u%04x' % ord(char))
        else:
            out.append(char)
    out.append('"')
    return ''.join(out)


def escape_letters(value, keep_markers):
    """JSON string body with every ASCII letter written as a \\u00XX escape."""
    parts = []
    cursor = 0
    spans = [m.span() for m in MARKER.finditer(value)] if keep_markers else []
    for start, end in spans + [(len(value), len(value))]:
        parts.extend('\\u%04x' % ord(c) if c.isascii() and c.isalpha() else json_quote(c)[1:-1]
                     for c in value[cursor:start])
        parts.append(value[start:end])
        cursor = end
    return ''.join(parts)


def wrap(kind, text, expected):
    if kind == 'plain':
        return text
    if kind == 'json_text':
        return '{"type":"tool_result","text":' + json_quote(text) + '}'
    if kind == 'nested_json':
        return '{"message":' + json_quote(wrap('json_text', text, expected)) + '}'
    if kind == 'escaped_json':
        return '{"text":"' + escape_letters(text, keep_markers=expected) + '"}'
    raise ValueError(kind)


def corpus():
    """(case name, wrapped input, wrapped expected) in a fixed order."""
    config = FIXTURE['property']
    state = config['seed']

    def rand(n):
        nonlocal state
        state = (1664525 * state + 1013904223) % 2**32
        return state % n

    result = []
    for case in FIXTURE['cases']:
        raw = expand(case['input'])
        want = expand(case.get('expected', case['input']))
        for kind in config['wrappers']:
            if kind == 'escaped_json' and '[REDACTED' in raw:
                continue
            for _ in range(config['iterations']):
                prefix = config['prefixes'][rand(len(config['prefixes']))]
                suffix = config['suffixes'][rand(len(config['suffixes']))]
                result.append((case['name'], wrap(kind, prefix + raw + suffix, False),
                               wrap(kind, prefix + want + suffix, True)))
    return result


def corpus_digest(items):
    digest = hashlib.sha256()
    for _, value, _ in items:
        digest.update(value.encode() + b'\x00')
    return digest.hexdigest()
