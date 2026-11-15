"""Payment data boundary for conversation storage. Never logs candidate values."""
from __future__ import annotations

import json
import re
from typing import Any

_LABEL = r'(?:cvv2?|cvc2?|cid|csc|cvd|cvn|(?:security|card)[ _-]*code)'
_CODE = re.compile(r'(?i)(\b' + _LABEL + r'\b(?:\s+is)?[\s\\\x22\x27:=,-]{0,24})([0-9]{3,4})(?![0-9])')
_CODE_KEY = re.compile(r'(?i)^' + _LABEL + '$')
_CODE_SCALAR = re.compile(r'[0-9]{3,4}(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?')
_ESCAPE = re.compile(r'\\+u(003[0-9]|0020|002[dD])')
_DIGITS = re.compile(r'[0-9]+')
_SEPARATOR = re.compile(r'[ -]{1,3}')
_ADJACENT_CODE = re.compile(r'(?:[ -]{1,3}|[:=] {0,3})([0-9]{3,4})(?![0-9])')
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


def card_spans(value: str):
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
            if groups in _CARD_LAYOUTS and is_card(digits):
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
    # Mask the union of overlapping valid readings: ambiguity never exposes a
    # complete alternative PAN. Only explicit layouts reached this point.
    merged = []
    for start,end in valid:
        if merged and start<merged[-1][1]:
            merged[-1] = (merged[-1][0],max(end,merged[-1][1]))
        else:
            merged.append((start,end))
    yield from merged

def _mask_cards(value: str) -> str:
    output = []
    cursor = 0
    for start,end in card_spans(value):
        if start < cursor:
            continue
        digits = value[start:end].replace(' ', '').replace('-', '')
        output.extend((value[cursor:start], '[card ****'+digits[-4:]+']'))
        while (code := _ADJACENT_CODE.match(value,end)) and standalone_end(value,code.end()):
            output.extend((value[end:code.start(1)], '[redacted]'))
            end=code.end()
        cursor=end
    output.append(value[cursor:])
    return ''.join(output)

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
    decoded = _ESCAPE.sub(lambda m: chr(int(m[1], 16)), value)
    return sum(1 for _ in card_spans(decoded))


def redact(value: str, *, card_context: bool = False) -> str:
    """Preserve unchanged strings byte-for-byte; decode nested JSON before masking.

    JSON numeric card/code fields become strings, so the result stays valid JSON.
    A code is masked only when this value (or its containing row) holds a card.
    """
    context = card_context or card_count(value) > 0
    if not context:
        return value
    numeric_changed = False
    def parse_number(literal):
        # Preserve the exact numeric lexeme until after redaction; converting
        # a 19-digit PAN to a binary float first can round away its last digits.
        nonlocal numeric_changed
        cleaned = _mask_cards(literal)
        if cleaned != literal:
            numeric_changed = True
            return cleaned
        return float(literal)
    try:
        parsed = json.loads(value, parse_float=parse_number)
    except (ValueError, TypeError):
        parsed = None
    if isinstance(parsed, (dict, list)):
        def walk(item: Any) -> Any:
            if isinstance(item, dict):
                return {redact(k, card_context=True): (
                    '[redacted]' if _CODE_KEY.fullmatch(k) and (
                        _CODE_SCALAR.fullmatch(str(v).strip()) or
                        isinstance(v,(int,float)) and not isinstance(v,bool) and 0<=v<=9999 and int(v)==v)
                    else walk(v)
                ) for k, v in item.items()}
            if isinstance(item, list):
                return [walk(v) for v in item]
            if isinstance(item, str):
                return redact(item, card_context=True)
            if isinstance(item, int) and is_card(str(item)):
                return '[card ****' + str(item)[-4:] + ']'
            return item
        cleaned = walk(parsed)
        return json.dumps(cleaned, ensure_ascii=False) if numeric_changed or cleaned != parsed else value
    decoded = _ESCAPE.sub(lambda m: chr(int(m[1], 16)), value)
    cleaned = _mask_cards(decoded)
    cleaned = _CODE.sub(lambda m: m[1] + '[redacted]', cleaned)
    return cleaned if cleaned != decoded else value


def redact_row(row: dict[str, Any]) -> dict[str, Any]:
    context = any(card_count(v if isinstance(v, str) else json.dumps(v)) for v in row.values() if isinstance(v, (str, dict, list)))
    return {key: redact(value, card_context=context) if isinstance(value, str)
            else json.loads(redact(json.dumps(value), card_context=context)) if isinstance(value, (dict, list))
            else value for key, value in row.items()}
