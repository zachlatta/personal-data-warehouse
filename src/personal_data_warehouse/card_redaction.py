"""Payment data boundary for conversation storage. Never logs candidate values."""
from __future__ import annotations

import json
import re
from typing import Any

_RUN = re.compile(r'(?<![0-9])[0-9](?:[ -]*[0-9])*(?![0-9])')
_LABEL = r'(?:cvv2?|cvc2?|cid|csc|cvd|cvn|(?:security|card)[ _-]*code)'
_CODE = re.compile(r'(?i)(\b' + _LABEL + r'\b(?:\s+is)?[\s\\\x22\x27:=,-]{0,24})([0-9]{3,4})(?![0-9])')
_CODE_KEY = re.compile(r'(?i)^' + _LABEL + '$')
_ESCAPE = re.compile(r'\\+u(003[0-9]|0020|002[dD])')
_DIGITS = re.compile(r'[0-9]+')
_BARE_CODE = re.compile(r'[ -]+([0-9]{3,4})(?![0-9])')


def standalone_end(value: str, end: int) -> bool:
    if end == len(value):
        return True
    next_char = value[end]
    return not (next_char.isalnum() or next_char in '_-/:' or
                (next_char == '.' and end + 1 < len(value) and value[end + 1].isdigit()))


def card_spans(value: str):
    """Token-aligned Luhn windows; never split a long contiguous identifier.

    Prefer a complete contiguous PAN or standard 4/4/4/4 and 4/6/5
    groupings over a longer window that accidentally includes an adjacent code.
    """
    for run in _RUN.finditer(value):
        tokens = list(_DIGITS.finditer(run[0]))
        i = 0
        while i < len(tokens):
            candidates = []
            digits = ''
            for j in range(i, len(tokens)):
                digits += tokens[j][0]
                if len(digits) > 19:
                    break
                if not is_card(digits):
                    continue
                # The first complete PAN accepts punctuation as a delimiter.
                # Only a LONGER, ambiguous window might have swallowed a date,
                # phone prefix or code appended after that already-valid PAN.
                if candidates and not standalone_end(value, run.start() + tokens[j].end()):
                    continue
                groups = [len(t[0]) for t in tokens[i:j+1]]
                rank = 0 if len(groups) == 1 else 1 if groups in ([4,4,4,4], [4,6,5]) else 2
                candidates.append((rank, len(digits), j))
            if not candidates:
                i += 1
                continue
            _, _, j = min(candidates)
            furthest = max(c[2] for c in candidates)
            # Ambiguous valid windows are all covered, but retain digits only
            # from the preferred PAN, never an adjacent code in a longer window.
            yield run.start() + tokens[i].start(), run.start() + tokens[furthest].end(), run.start() + tokens[j].end()
            i = furthest + 1


def _mask_cards(value: str) -> str:
    output = []
    cursor = 0
    for start, end, retained_end in card_spans(value):
        if start < cursor:
            continue
        digits = value[start:retained_end].replace(' ', '').replace('-', '')
        output.extend((value[cursor:start], '[card ****' + digits[-4:] + ']'))
        if end > retained_end:
            output.append(' [redacted]')
        # Bare adjacent security codes have no label to match later. Consume
        # them separately, so they never become part of the retained last four.
        code = _BARE_CODE.match(value, end)
        if code and standalone_end(value, code.end()):
            output.extend((value[end:code.start(1)], '[redacted]'))
            end = code.end()
        cursor = end
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
    try:
        parsed = json.loads(value)
    except (ValueError, TypeError):
        parsed = None
    if isinstance(parsed, (dict, list)):
        def walk(item: Any) -> Any:
            if isinstance(item, dict):
                return {redact(k, card_context=True): (
                    '[redacted]' if _CODE_KEY.fullmatch(k) and re.fullmatch(r'[0-9]{3,4}', str(v))
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
        return json.dumps(cleaned, ensure_ascii=False) if cleaned != parsed else value
    decoded = _ESCAPE.sub(lambda m: chr(int(m[1], 16)), value)
    cleaned = _mask_cards(decoded)
    cleaned = _CODE.sub(lambda m: m[1] + '[redacted]', cleaned)
    return cleaned if cleaned != decoded else value


def redact_row(row: dict[str, Any]) -> dict[str, Any]:
    context = any(card_count(v if isinstance(v, str) else json.dumps(v)) for v in row.values() if isinstance(v, (str, dict, list)))
    return {key: redact(value, card_context=context) if isinstance(value, str)
            else json.loads(redact(json.dumps(value), card_context=context)) if isinstance(value, (dict, list))
            else value for key, value in row.items()}
