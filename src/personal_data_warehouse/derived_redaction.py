"""Credential masking for a derived copy cut out of masked conversation text.

Conversation rows are masked at ingest, whole. A timeline title or snippet is a
prefix of a turn and a search chunk is a window of it, and a cut can make what
the full text never held: a labelled value that is a placeholder in full (a
later 'placeholder', a following '(') becomes a secret-shaped fragment, and a
cut through ``[REDACTED:kind:abcdef]`` leaves a broken marker. Ingest-time sync,
the scrub's rebuild and the chunk builder all mask through ``mask_cut``, so the
counting report, the scrub's safety check and the stored rows agree.

Credentials only: the credential scope deliberately leaves payment data alone
in every copy. Never logs candidate values.
"""
from __future__ import annotations

import re

from personal_data_warehouse.credential_redaction import might_contain_credential, redact_credentials

# A proper prefix of [REDACTED:<kind>:<6 hex>] at the end of a cut value ...
_TRAILING_PARTIAL = re.compile(
    r'\[(?:R(?:E(?:D(?:A(?:C(?:T(?:E(?:D(?::(?:[a-z_]+(?::[0-9a-f]{0,6})?)?)?)?)?)?)?)?)?)?)?\Z')
# ... and a proper suffix of one at the start (it never begins with '[').
_LEADING_PARTIAL = re.compile(
    r'(?:(?:(?:REDACTED|EDACTED|DACTED|ACTED|CTED|TED|ED|D)?:)?[a-z_]*:)?[0-9a-f]{0,6}\]')


def mask_cut(value: str, *, cut_start: bool = False, cut_end: bool = False) -> str:
    """Drop a marker the cut went through, then mask what the cut selects.

    A copy with nothing selected and no cut marker is returned unchanged. A
    masked fragment's marker can be longer than the fragment, so a masked title
    may exceed its character cap by up to one marker.
    """
    if cut_end and (partial := _TRAILING_PARTIAL.search(value)):
        value = value[:partial.start()]
    if cut_start and (partial := _LEADING_PARTIAL.match(value)):
        value = value[partial.end():]
    return redact_credentials(value) if might_contain_credential(value) else value
