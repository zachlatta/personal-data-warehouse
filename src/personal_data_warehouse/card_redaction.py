"""Payment data boundary for conversation storage. Never logs candidate values."""
from __future__ import annotations

import json
import re
from typing import Any

_LABEL = r'(?:cvv2?|cvc2?|cid|csc|cvd|cvn|(?:security|card)[ _-]*code)'
_CODE = re.compile(r'(?i)(\b' + _LABEL + r'\b(?:\s+is)?[\s\\\x22\x27:=,-]{0,24})([0-9]{3,4})(?![0-9])')
_CODE_KEY = re.compile(r'(?i)^' + _LABEL + '$')
_ESCAPE = re.compile(r'\\+u(003[0-9]|0020|002[dD])')
_BARE_CODE = re.compile(r'[ -]+([0-9]{3,4})(?![0-9])')
_CARD_TOKENS = [re.compile(r'[0-9]{13,19}')] + [
    re.compile(re.escape(sep).join(grouping))
    for sep in (' ', '-') for grouping in (
        (r'[0-9]{4}',r'[0-9]{4}',r'[0-9]{4}',r'[0-9]{1,7}'),
        (r'[0-9]{4}',r'[0-9]{4}',r'[0-9]{4}',r'[0-9]{4}',r'[0-9]{1,3}'),
        (r'[0-9]{4}',r'[0-9]{6}',r'[0-9]{5}'))]
_EXPIRY_SUFFIX = re.compile(r'/[0-9]{2}/(?:[0-9]{4}|[0-9]{2})(?![0-9/])')


def standalone_end(value: str, end: int) -> bool:
    if end == len(value):
        return True
    next_char = value[end]
    return not (next_char.isalnum() or next_char in '_-/:' or
                (next_char == '.' and end + 1 < len(value) and value[end + 1].isdigit()))


def pan_token_end(value: str, end: int, *, contiguous=False) -> bool:
    """A PAN ends at punctuation, not inside a numeric/identifier token.

    The one numeric suffix belonging to card syntax is /MM/YY (or /MM/YYYY).
    It remains outside the PAN. No arbitrary numeric window is considered.
    """
    if end==len(value):
        return True
    char=value[end]
    if char.isdigit():
        return False
    if char.isalnum() or char=='_':
        # A whole 13–19 digit run remains sensitive inside prose/identifiers.
        # A short grouped tail attached to a word (123abc) is not a card group.
        return contiguous
    if char in '-/:.' and end+1<len(value) and value[end+1].isdigit():
        return char=='/' and _EXPIRY_SUFFIX.match(value,end) is not None
    return True


def card_spans(value: str):
    """One card token: contiguous digits or one recognized consistent grouping.

    Never join arbitrary neighboring numbers, mix separators, split a longer
    digit run, or cross a numeric continuation. Luhn runs AFTER
    lexical recognition. Bare codes are handled only after confirming a PAN.
    """
    candidates = {}
    for index,pattern in enumerate(_CARD_TOKENS):
        for match in pattern.finditer(value):
            start,end = match.span()
            if start and (value[start-1].isdigit() or (value[start-1]=='-' and start>1 and value[start-2].isdigit())):
                continue
            if not pan_token_end(value,end,contiguous=index==0):
                continue
            if is_card(match[0]):
                candidates.setdefault(start,set()).add(end)
    cursor = 0
    for start in sorted(candidates):
        if start < cursor:
            continue
        ends = sorted(candidates[start])
        end = max(ends)
        # A complete shorter PAN followed by an isolated 3–4 digit token is
        # ambiguous with a longer grouping. Prefer the confirmed shorter PAN
        # and suppress the entire code separately, never retaining code digits.
        for short in ends:
            code = _BARE_CODE.match(value,short)
            if short<end and code and code.end()==end and standalone_end(value,end):
                end=short
                break
        yield start,end
        cursor=end


def _mask_cards(value: str) -> str:
    output = []
    cursor = 0
    for start,end in card_spans(value):
        if start < cursor:
            continue
        digits = value[start:end].replace(' ', '').replace('-', '')
        output.extend((value[cursor:start], '[card ****'+digits[-4:]+']'))
        while (code := _BARE_CODE.match(value,end)) and standalone_end(value,code.end()):
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
