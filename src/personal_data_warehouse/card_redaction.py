"""Payment data boundary for conversation storage. Never logs candidate values."""
from __future__ import annotations

import json
import re
from typing import Any

_RUN = re.compile(r'(?<![0-9])[0-9](?:[ -]*[0-9])*(?![0-9])')
_CODE = re.compile(r'(?i)((?:cvv|cvc|security[ _-]*code)\b(?:\s+is)?[\s\\\x22\x27:=,-]{0,24})([0-9]{3,4})(?![0-9])')
_CODE_KEY = re.compile(r'(?i)^(?:cvv|cvc|security[ _-]*code)$')
_ESCAPE = re.compile(r'\\+u(003[0-9]|0020|002[dD])')


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
    return sum(is_card(m[0]) for m in _RUN.finditer(decoded))


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
    cleaned = _RUN.sub(lambda m: '[card ****' + m[0].replace(' ', '').replace('-', '')[-4:] + ']' if is_card(m[0]) else m[0], decoded)
    cleaned = _CODE.sub(lambda m: m[1] + '[redacted]', cleaned)
    return cleaned if cleaned != decoded else value


def redact_row(row: dict[str, Any]) -> dict[str, Any]:
    context = any(card_count(v if isinstance(v, str) else json.dumps(v)) for v in row.values() if isinstance(v, (str, dict, list)))
    return {key: redact(value, card_context=context) if isinstance(value, str)
            else json.loads(redact(json.dumps(value), card_context=context)) if isinstance(value, (dict, list))
            else value for key, value in row.items()}
