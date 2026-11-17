"""Payment data boundary for conversation storage. Never logs candidate values."""
from __future__ import annotations

import base64
import binascii
import json
import re
from pathlib import Path
from decimal import Decimal, InvalidOperation
from typing import Any

CARD_POLICY = json.loads(Path(__file__).with_name('card_policy.json').read_text())
_LITERAL_ATTACHED_LABELS = frozenset(CARD_POLICY['literal_attached_labels'])
_PAYMENT_LABELS = '(?:' + '|'.join(re.escape(label) for label in CARD_POLICY['attached_labels']) + '|' + CARD_POLICY['payment_label_pattern'] + ')'
_FIELD_SEPARATOR = CARD_POLICY['payment_field_separator_pattern']
_PAYMENT_AFTER = re.compile(r'(?i)^(?:[\s/|;#=&+,:\-\x22\x27]{1,24}|-[A-Za-z]{1,16}[ \t]+)'+_PAYMENT_LABELS+_FIELD_SEPARATOR+r'[0-9]')
_PAYMENT_BEFORE = re.compile(r'(?i)(?:^|[^A-Za-z0-9_])'+_PAYMENT_LABELS+_FIELD_SEPARATOR+'$')


def card_brands(digits):
    return {rule['name'] for rule in CARD_POLICY['issuers'] if len(digits) in rule['lengths']
            and any(low <= digits[:len(low)] <= high for low,high in rule['ranges'])}


def _word_char(char):
    return char.isascii() and (char.isalnum() or char == '_')


def _encoded_char(char):
    return _word_char(char) or char in '+/=-'


def encoded_token(value):
    """Require a canonical standard/URL-safe encoding, not just its alphabet.

    A candidate spanning whitespace cannot belong to one encoded token. Strict
    round trips also reject invalid padding and nonzero unused trailing bits.
    """
    if not (any(c.isascii() and c.isalpha() for c in value) and
            any('0' <= c <= '9' for c in value)):
        return False
    urlsafe = '-' in value or '_' in value
    if urlsafe and ('+' in value or '/' in value):
        return False
    try:
        raw = base64.b64decode(value+'='*(-len(value)%4), altchars=b'-_' if urlsafe else None, validate=True)
        canonical = (base64.urlsafe_b64encode(raw) if urlsafe else base64.b64encode(raw)).decode()
        return value == (canonical if '=' in value else canonical.rstrip('='))
    except (ValueError, binascii.Error):
        return False


def card_token_boundary(value,start,end):
    # Payment fields establish context; path segments such as /pan/ do not.
    left=start
    while left and _encoded_char(value[left-1]):left-=1
    right=end
    while right<len(value) and _encoded_char(value[right]):right+=1
    payment_after=bool(_PAYMENT_AFTER.search(value[end:end+128]))
    prefix=value[left:start].lower()
    attached=prefix in CARD_POLICY['attached_labels']
    if prefix in _LITERAL_ATTACHED_LABELS and (right==end or payment_after):return True
    if payment_after and (left==start or attached or prefix.rstrip('/-') in CARD_POLICY['attached_labels']):return True
    if _PAYMENT_BEFORE.search(value[max(0,start-128):start]):return True
    # Ordinary identifiers still need protection even when they are not base64.
    # Only exact recognised labels may be attached to a card value.
    if (attached and right==end or
            left==start and value[end:right].lower() in CARD_POLICY['attached_labels']):
        return not encoded_token(value[left:right])
    if start and _word_char(value[start-1]) or end<len(value) and _word_char(value[end]):return False
    return not encoded_token(value[left:right])

_LABEL = r'(?:cvv2?|cvc2?|cid|csc|cvd|cvn|(?:security|card)[ _-]*code)'
_CODE = re.compile(r'(?i)(\b' + _LABEL + r'\b(?:\s+is)?[\s\\\x22\x27:=,-]{0,24})([0-9]{3,4})(?![0-9])')
_CODE_KEY = re.compile(r'(?i)^' + _LABEL + '$')
_CODE_SCALAR = re.compile(r'[0-9]{3,4}(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?')
_ESCAPE = re.compile(r'\\+u(003[0-9]|0020|002[dD])')
_DIGITS = re.compile(r'[0-9]+')
PDW_TOKEN = re.compile(r'(pdw_[a-z0-9]{12,24}_)[A-Za-z0-9_-]{30,}')
_SEPARATOR = re.compile(r'[ -]{1,3}')
_ADJACENT_CODE = re.compile(r'(?:[ -]{1,3}|[:=] {0,3})([0-9]{3,4})(?![0-9])')
_JSON_TOKEN = re.compile(r'"(?:[^"\\]|\\.)*"|-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?|[{}\[\]:,]|true|false|null')
# Explicit storage policy, mirrored in Go and pinned by shared fixtures.
# A contiguous PAN is one whole run; grouped PANs must match one of these
# layouts. Independent identifiers are never joined by length/Luhn alone.
_CARD_LAYOUTS = frozenset(
    [(n,) for n in range(13, 20)] +
    [(4,4,4,4), (4,6,5), (4,6,4), (4,4,5), (5,5,5), (5,5,6)] +
    [(4,4,4,n) for n in range(1,4)] +
    [(4,4,4,4,n) for n in range(1,4)]
)


def standalone_end(value: str, end: int) -> bool:
    if end == len(value):
        return True
    next_char = value[end]
    return not (next_char.isalnum() or next_char in '_-/:' or
                (next_char == '.' and end + 1 < len(value) and value[end + 1].isdigit()))


def card_spans(value: str, *, merge=True):
    """Enumerate complete runs/layouts at every start, then check Luhn.

    Separators may mix 1–3 spaces/dashes. A complete valid layout wins over
    neighbouring phone/date/time interpretations. No speculative token veto
    may suppress it, and no layout-free concatenation may invent a card.
    """
    atoms = list(_DIGITS.finditer(value))
    readings = set()
    for i,atom in enumerate(atoms):
        digits = ''
        groups = ()
        for j in range(i,len(atoms)):
            if j>i and not _SEPARATOR.fullmatch(value[atoms[j-1].end():atoms[j].start()]):
                break
            digits += atoms[j][0]
            groups += (len(atoms[j][0]),)
            if len(digits)>19:
                break
            if (groups in _CARD_LAYOUTS and is_card(digits) and card_brands(digits)
                    and card_token_boundary(value,atom.start(),atoms[j].end())):
                readings.add((atom.start(),atoms[j].end()))
    valid = sorted(readings)
    codes = []
    for start,end in valid:
        code = _ADJACENT_CODE.match(value,end)
        if not code or not standalone_end(value,code.end()):
            continue
        size = sum(c.isdigit() for c in value[start:end])
        # Do not mistake the end of another, equally complete overlapping PAN
        # for this reading's bare code (e.g. a preceding four-digit token).
        if any(s>start and e==code.end() and sum(c.isdigit() for c in value[s:e])>=size for s,e in valid):
            continue
        codes.append((code.start(1),code.end()))
    valid = [(a,b) for a,b in valid if not any(a<d and b>c for c,d in codes)]
    if not merge:
        yield from valid
        return
    # Mask the union of overlapping valid readings: ambiguity never exposes a
    # complete alternative PAN. Only explicit layouts reached this point.
    merged = []
    for start,end in valid:
        if merged and start<merged[-1][1]:
            merged[-1] = (merged[-1][0],max(end,merged[-1][1]))
        else:
            merged.append((start,end))
    yield from merged

def _apply_edits(value, edits):
    output = []
    cursor = 0
    for start, end, replacement in edits:
        output.extend((value[cursor:start], replacement))
        cursor = end
    output.append(value[cursor:])
    return ''.join(output)


def _text_edits(value):
    token_edits = [(m.start(0) + len(m[1]), m.end(), '[REDACTED]') for m in PDW_TOKEN.finditer(value)]
    # Keep offsets through escaped-digit decoding: unmasked escapes must survive.
    chars = []; bounds = [0]; pos = 0
    while pos < len(value):
        match = _ESCAPE.match(value, pos) if value[pos] == '\\' else None
        chars.append(chr(int(match[1], 16)) if match else value[pos])
        pos = match.end() if match else pos + 1
        bounds.append(pos)
    decoded = ''.join(chars)
    edits = []
    for start, end in card_spans(decoded):
        digits = decoded[start:end].replace(' ', '').replace('-', '')
        edits.append((start, end, '[card ****'+digits[-4:]+']'))
        while (code := _ADJACENT_CODE.match(decoded, end)) and standalone_end(decoded, code.end()):
            edits.append((code.start(1), code.end(), '[redacted]'))
            end = code.end()
    for match in _CODE.finditer(decoded):
        start, end = match.span(2)
        if not card_token_boundary(decoded,start,end):continue
        if not any(start < b and end > a for a, b, _ in edits):
            edits.append((start, end, '[redacted]'))
    # Adjacent-code chains can reach a later card reading. Merge their spans
    # before applying edits so covered bytes cannot reappear behind the cursor.
    merged = []
    for start, end, replacement in sorted(edits):
        if merged and start < merged[-1][1]:
            previous_start, previous_end, previous_replacement = merged[-1]
            if not previous_replacement.startswith('[card ') and replacement.startswith('[card '):
                previous_replacement = replacement
            merged[-1] = (previous_start, max(end, previous_end), previous_replacement)
        else:
            merged.append((start, end, replacement))
    edits = [(bounds[a], bounds[b], replacement) for a, b, replacement in merged]
    # A token owns its entire secret, even when it contains card-like digits.
    edits = [e for e in edits if not any(e[0] < b and e[1] > a for a,b,_ in token_edits)]
    return sorted(edits + token_edits)


def _json_string_bounds(token):
    """Map decoded character boundaries to offsets in an original JSON string."""
    bounds = [1]; pos = 1
    while pos < len(token)-1:
        if token[pos] != '\\':
            pos += 1
        elif token[pos+1] != 'u':
            pos += 2
        else:
            high = int(token[pos+2:pos+6], 16)
            pos += 6
            if (0xD800 <= high <= 0xDBFF and token[pos:pos+2] == '\\u' and
                    0xDC00 <= int(token[pos+2:pos+6], 16) <= 0xDFFF):
                pos += 6
        bounds.append(pos)
    return bounds


def _code_token(token):
    if token.startswith('"'):
        return bool(_CODE_SCALAR.fullmatch(json.loads(token).strip()))
    try:
        number = Decimal(token)
        return number.is_finite() and 0 <= number <= 9999 and number == number.to_integral_value()
    except InvalidOperation:
        return False


def _reject_constant(_):
    raise ValueError('non-JSON constant')


def _json_tokens(value):
    if value.lstrip().startswith(('{', '[', '"')):
        try:
            # Validate only; these objects are never serialized or stored.
            # Callbacks avoid binary floats and Python's integer digit limit.
            json.loads(value, parse_float=str, parse_int=str, parse_constant=_reject_constant)
        except (ValueError, RecursionError):
            pass
        else:
            return list(_JSON_TOKEN.finditer(value))
    return None


def _redaction_edits(value, context):
    if not context and not sensitive_count(value):
        return []
    tokens = _json_tokens(value)
    if tokens is not None:
        codes = set()
        for index, token in enumerate(tokens[:-2]):
            if (token[0].startswith('"') and tokens[index+1][0] == ':' and
                    _CODE_KEY.fullmatch(json.loads(token[0])) and _code_token(tokens[index+2][0])):
                codes.add(index+2)
        edits = []
        for index, token in enumerate(tokens):
            raw = token[0]
            if index in codes:
                edits.append((token.start(), token.end(), '"[redacted]"'))
            elif raw.startswith('"'):
                inner = _redaction_edits(json.loads(raw), True)
                if inner:
                    bounds = _json_string_bounds(raw)
                    edits.extend((token.start()+bounds[a], token.start()+bounds[b],
                                  json.dumps(replacement, ensure_ascii=True)[1:-1])
                                 for a, b, replacement in inner)
            elif raw[0] in '-0123456789':
                inner = _text_edits(raw)
                if inner:
                    edits.append((token.start(), token.end(), json.dumps(_apply_edits(raw, inner))))
        return edits
    return _text_edits(value)

def is_card(value: str) -> bool:
    digits = value.replace(' ', '').replace('-', '')
    if not 13 <= len(digits) <= 19 or not digits.isascii() or not digits.isdigit():
        return False
    total = 0
    for index, digit in enumerate(reversed(digits)):
        n = int(digit) * (2 if index % 2 else 1)
        total += n - 9 if n > 9 else n
    return total % 10 == 0


def card_count(value: str) -> int:
    """Count leaf readings using the same decoded JSON traversal as masking.

    A raw escape regex is not a safe negative filter: Unicode can encode the
    escape backslashes themselves. Count leaves once, not at every wrapper.
    """
    tokens = _json_tokens(value)
    if tokens is not None:
        return sum(card_count(json.loads(raw)) if raw.startswith('"') else card_count(raw)
                   for token in tokens if (raw := token[0])[0] in '"-0123456789')
    decoded = _ESCAPE.sub(lambda m: chr(int(m[1], 16)), value)
    return sum(1 for _ in card_spans(decoded))


def redact(value: str, *, card_context: bool = False) -> str:
    """Replace only sensitive spans, preserving every other JSON/text byte.

    JSON numeric card/code fields become strings, so the result stays valid JSON.
    A code is masked only when this value (or its containing row) holds a card.
    """
    return _apply_edits(value, _redaction_edits(value, card_context))


def sensitive_count(value: str) -> int:
    tokens = _json_tokens(value)
    if tokens is not None:
        return sum(sensitive_count(json.loads(raw)) if raw.startswith('"') else sensitive_count(raw)
                   for token in tokens if (raw := token[0])[0] in '"-0123456789')
    return card_count(value) + len(PDW_TOKEN.findall(value))


def redact_row(row: dict[str, Any]) -> dict[str, Any]:
    context = any(card_count(v if isinstance(v, str) else json.dumps(v)) for v in row.values() if isinstance(v, (str, dict, list)))
    return {key: redact(value, card_context=context) if isinstance(value, str)
            else json.loads(redact(json.dumps(value), card_context=context)) if isinstance(value, (dict, list))
            else value for key, value in row.items()}
