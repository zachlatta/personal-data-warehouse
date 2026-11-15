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
_EXPIRY_SUFFIX = re.compile(r'/[0-9]{2}/(?:[0-9]{4}|[0-9]{2})(?![0-9/])')
_PROTECTED = [
    ('phone', re.compile(r'(?:\+[0-9]{1,3}[ -]{1,3})?[0-9]{3}[ -]{1,3}[0-9]{3}[ -]{1,3}[0-9]{4}')),
    ('phone', re.compile(r'\([0-9]{3}\) {0,3}[0-9]{3}[ -]{1,3}[0-9]{4}')),
    ('date', re.compile(r'[0-9]{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])')),
    ('date', re.compile(r'[0-9]{4}/(?:0[1-9]|1[0-2])/(?:0[1-9]|[12][0-9]|3[01])')),
    ('date', re.compile(r'[0-9]{1,2}/[0-9]{2,4}')),
    ('time', re.compile(r'[0-9]{1,3}:[0-9]{2}(?::[0-9]{2})?(?:\.[0-9]+)?')),
    ('uuid', re.compile(r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}')),
    ('id', re.compile(r'[0-9]{1,4}\.[0-9]+')),
    ('id', re.compile(r'[0-9]{1,4}/[0-9]{2,4}')),
    ('id', re.compile(r'[0-9]{1,4}[a-zA-Z_][a-zA-Z_0-9]*')),
]


def standalone_end(value: str, end: int) -> bool:
    if end == len(value):
        return True
    next_char = value[end]
    return not (next_char.isalnum() or next_char in '_-/:' or
                (next_char == '.' and end + 1 < len(value) and value[end + 1].isdigit()))


def card_spans(value: str):
    """Enumerate every overlapping digit-run reading, then apply exclusions.

    A reading has 13–19 digits, never splits a contiguous run, and may join
    groups across any 1–3 spaces/dashes. Layout and separator uniformity cannot
    hide a card. Recognized non-card tokens, rather than candidate shape, keep
    readings from consuming phone/date/time/identifier fragments.
    """
    atoms = list(_DIGITS.finditer(value))
    readings = set()
    for i,atom in enumerate(atoms):
        digits = ''
        for j in range(i,len(atoms)):
            if j>i and not _SEPARATOR.fullmatch(value[atoms[j-1].end():atoms[j].start()]):
                break
            digits += atoms[j][0]
            if len(digits)>19:
                break
            if is_card(digits):
                readings.add((atom.start(),atoms[j].end()))
    if not readings:
        return
    protected = []
    token_starts = sorted({m.start() for m in atoms} | {m.start()-1 for m in atoms if m.start() and value[m.start()-1] in '+('})
    for kind,pattern in _PROTECTED:
        positions = [m.start() for m in pattern.finditer(value)] if kind=='uuid' else token_starts
        for position in positions:
            match = pattern.match(value,position)
            if match is None:
                continue
            if match.start() and value[match.start()-1].isdigit():
                continue
            if match.end()<len(value) and value[match.end()].isdigit():
                continue
            first = _DIGITS.search(value,match.start(),match.end())
            protected.append((kind,match.start(),match.end(),first.end()))
    valid = []
    for start,end in sorted(readings):
        blocked = False
        for kind,a,b,first_end in protected:
            if start>=b or end<=a:
                continue
            # A confirmed PAN immediately before a numeric field/expiry has
            # precedence over interpreting its final group as a new token.
            # An earlier complete PAN instead makes that token a separate field.
            groups = [len(m[0]) for m in _DIGITS.finditer(value,start,end)]
            card_layout = (len(groups)==4 and groups[:3]==[4,4,4] and 1<=groups[3]<=7 or
                           len(groups)==5 and groups[:4]==[4,4,4,4] and 1<=groups[4]<=3 or groups==[4,6,5])
            numeric_field = card_layout and kind in ('date','time','id') and start<a and end==first_end
            earlier_pan = any(s<a and start<e<=a for s,e in readings)
            external_pan = any(s<start and start<e<=a for s,e in readings)
            if numeric_field and (not earlier_pan or _EXPIRY_SUFFIX.match(value,end) and not external_pan):
                continue
            blocked = True
            break
        if not blocked:
            valid.append((start,end))
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
    # complete alternative PAN. Recognized safe tokens were excluded above.
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
