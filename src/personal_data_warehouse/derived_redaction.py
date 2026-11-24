"""Credential masking for a derived copy cut out of masked conversation text.

Conversation rows are masked at ingest, whole. A timeline title or snippet is a
prefix of a turn and a search chunk is a window of it, and a cut can make what
the full text never held: a labelled value that is a placeholder in full (a
later 'placeholder', a following '(') becomes a secret-shaped fragment, and a
cut through ``[REDACTED:kind:abcdef]`` leaves a broken marker. Ingest-time sync,
the scrub's rebuild and the chunk builder all mask through these functions, so
the counting report, the scrub's safety check and the stored rows agree.

Credentials only: the credential scope deliberately leaves payment data alone
in every copy. Never logs candidate values.
"""
from __future__ import annotations

import re

from personal_data_warehouse.credential_redaction import might_contain_credential, redact_credentials

_MARKER = re.compile(r'\[REDACTED:[a-z_]+:[0-9a-f]{6}\]')
_MARKER_MAX = 64
# A proper prefix of [REDACTED:<kind>:<6 hex>] at the end of a prefix copy.
_TRAILING_PARTIAL = re.compile(
    r'\[(?:R(?:E(?:D(?:A(?:C(?:T(?:E(?:D(?::(?:[a-z_]+(?::[0-9a-f]{0,6})?)?)?)?)?)?)?)?)?)?)?\Z')


def mask_cut(value: str, *, cut_end: bool = False) -> str:
    """Mask a prefix copy (a title or snippet) of text that was masked whole.

    ``cut_end``: the value was cut at its cap, so a marker the cut went
    through is dropped first. A copy with nothing selected and no cut marker is
    returned unchanged. A masked fragment's marker can be longer than the
    fragment, so a masked title may exceed its cap by up to one marker.
    """
    if cut_end and (partial := _TRAILING_PARTIAL.search(value)):
        value = value[:partial.start()]
    return redact_credentials(value) if might_contain_credential(value) else value


def _marker_around(document: str, offset: int):
    for marker in _MARKER.finditer(document, max(0, offset - _MARKER_MAX), offset + _MARKER_MAX):
        if marker.start() < offset < marker.end():
            return marker
    return None


def mask_window(document: str, start: int, end: int) -> str:
    """Mask ``document[start:end]`` (a search chunk), never splitting a marker.

    The document is known here, so a window edge inside a marker moves to the
    marker's edge exactly; then what the cut selects is masked.
    """
    if marker := _marker_around(document, start):
        start = marker.end()
    if marker := _marker_around(document, end):
        end = marker.start()
    return mask_cut(document[start:end]) if start < end else ''
