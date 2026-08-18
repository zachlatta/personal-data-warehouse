"""'Who sent this image?' — match a local picture to a Slack file row.

This closes the 2026-08-16 gap end to end. That agent had a PNG, found the
right ``base_slack.files`` row by luck and context, could not confirm it, and
fabricated an answer. Here the picture itself is the query, and the answer
always carries the uploader — resolved through ``base_slack.users``, which is
the join that agent never made.

Why the probe is hashed **here, in Python**, and not in the Go CLI: the probe
hash has to be bit-identical to the one the backfill stored, and Pillow's
LANCZOS resample is not guaranteed to agree with a Go imaging library. A
fingerprint that drifts does not error — it silently stops matching. So the Go
subcommand is a thin exec of this module, the same shape as ``pdw ingest``.

Ranking is a plain SQL ``bit_count`` XOR over the 256-bit dhash, so it runs
through the ordinary read-only query role with no extension and no new server
surface.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from personal_data_warehouse.photo_fingerprint import HASH_VERSION, Fingerprint, compute_dhash
from personal_data_warehouse.slack_file_fingerprints import DEFAULT_MAX_PIXELS
from personal_data_warehouse.relations import relation

__all__ = [
    "DEFAULT_LIMIT",
    "DEFAULT_MAX_DISTANCE",
    "SlackImageMatch",
    "build_lookup_sql",
    "format_matches",
    "lookup_sql_for_image",
    "parse_matches",
    "probe_fingerprint",
]

DEFAULT_LIMIT = 5
#: 256-bit dhash. A re-encode/rescale of the same picture lands in the single
#: digits; unrelated pictures sit near 100+. 40 is a deliberately generous
#: ceiling so a near miss is still *shown* with its distance rather than
#: silently dropped — the caller can judge.
DEFAULT_MAX_DISTANCE = 40

_HEX_256 = re.compile(r"\A[0-9a-f]{64}\Z")


@dataclass
class SlackImageMatch:
    file_id: str
    distance: int
    name: str = ""
    title: str = ""
    uploader_user_id: str = ""
    uploader_display_name: str = ""
    uploader_real_name: str = ""
    uploader_name: str = ""
    conversation_id: str = ""
    conversation_name: str = ""
    conversation_kind: str = ""
    created_at: str = ""
    size: int = 0
    mimetype: str = ""
    account: str = ""
    team_id: str = ""
    content_sha256: str = ""
    width: int = 0
    height: int = 0
    url_private: str = ""
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def uploader(self) -> str:
        """Best available human name, never an empty string.

        real_name first: it is the one a person would recognise. The handle and
        display name are surfaced separately rather than collapsed into this,
        because Slack keeps all three and they routinely differ.
        """
        for candidate in (self.uploader_real_name, self.uploader_display_name, self.uploader_name):
            if candidate:
                return candidate
        return self.uploader_user_id or "unknown"

    @property
    def handle(self) -> str:
        """The @handle (``base_slack.users.name``), which is what people ask for."""
        return self.uploader_name

    @property
    def identity(self) -> str:
        """Every identity Slack holds for the uploader, deduplicated."""
        parts = [self.uploader]
        if self.handle and self.handle != self.uploader:
            parts.append(f"@{self.handle}")
        if (
            self.uploader_display_name
            and self.uploader_display_name not in (self.uploader, self.handle)
        ):
            parts.append(f'display "{self.uploader_display_name}"')
        parts.append(self.uploader_user_id or "unknown user")
        return f"{parts[0]} (" + ", ".join(parts[1:]) + ")" if len(parts) > 1 else parts[0]

    @property
    def channel(self) -> str:
        # Kind first, name second. Slack stores a DM's `name` as the other
        # user's id and a group DM's as `mpdm-a--b--c-1`, so trusting the name
        # would render a DM as "#U0EXAMPLE123" -- a channel that does not exist.
        if self.conversation_kind == "im":
            return f"DM {self.conversation_id}"
        if self.conversation_kind == "mpim":
            return f"group DM {self.conversation_id}"
        if self.conversation_name:
            return f"#{self.conversation_name}"
        return self.conversation_id or "unknown"

    @property
    def slack_link(self) -> str:
        if self.team_id and self.conversation_id:
            return f"https://app.slack.com/client/{self.team_id}/{self.conversation_id}"
        return ""


def probe_fingerprint(path: str | os.PathLike[str]) -> Fingerprint:
    """Fingerprint a local image with the exact algorithm the backfill uses.

    Including the pixel ceiling: the probe and the stored hash must be produced
    by identical code, or a print-resolution poster would hash on one side and
    fail on the other.
    """
    return compute_dhash(Path(path).read_bytes(), max_pixels=DEFAULT_MAX_PIXELS)


def _sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def build_lookup_sql(
    dhash: str,
    *,
    limit: int = DEFAULT_LIMIT,
    max_distance: int = DEFAULT_MAX_DISTANCE,
    account: str | None = None,
    hash_version: str = HASH_VERSION,
) -> str:
    """Rank stored Slack images by Hamming distance from ``dhash``.

    The hash is interpolated into SQL that crosses the HTTP tool API, so it is
    validated as exactly 256 bits of lowercase hex rather than escaped.
    """
    normalized = str(dhash).strip().lower()
    if not _HEX_256.match(normalized):
        raise ValueError(
            f"dhash must be 64 lowercase hex characters (256 bits), got {dhash!r}"
        )
    limit = max(1, int(limit))
    max_distance = max(0, min(256, int(max_distance)))

    view = relation("slack_image_fingerprints")
    # Named through the catalog: this SQL crosses the HTTP tool API, so the
    # warehouse's own @marker expansion never sees it.
    target = f"{view.schema}.{view.name}"

    filters = [
        f"hash_version = {_sql_literal(hash_version)}",
        "is_deleted = 0",
    ]
    if account:
        filters.append(f"account = {_sql_literal(account)}")

    return f"""
SELECT
    file_id,
    name,
    title,
    bit_count(('x' || dhash)::bit(256) # ('x' || {_sql_literal(normalized)})::bit(256)) AS distance,
    uploader_user_id,
    uploader_display_name,
    uploader_real_name,
    uploader_name,
    conversation_id,
    conversation_name,
    conversation_kind,
    created_at,
    size,
    mimetype,
    width,
    height,
    content_sha256,
    account,
    team_id,
    url_private
FROM {target}
WHERE {' AND '.join(filters)}
  AND bit_count(('x' || dhash)::bit(256) # ('x' || {_sql_literal(normalized)})::bit(256)) <= {max_distance}
ORDER BY distance ASC, created_at DESC
LIMIT {limit}
""".strip()


def parse_matches(ndjson: str) -> list[SlackImageMatch]:
    matches: list[SlackImageMatch] = []
    for line in (ndjson or "").splitlines():
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)
        matches.append(
            SlackImageMatch(
                file_id=str(row.get("file_id") or ""),
                distance=int(row.get("distance") or 0),
                name=str(row.get("name") or ""),
                title=str(row.get("title") or ""),
                uploader_user_id=str(row.get("uploader_user_id") or ""),
                uploader_display_name=str(row.get("uploader_display_name") or ""),
                uploader_real_name=str(row.get("uploader_real_name") or ""),
                uploader_name=str(row.get("uploader_name") or ""),
                conversation_id=str(row.get("conversation_id") or ""),
                conversation_name=str(row.get("conversation_name") or ""),
                conversation_kind=str(row.get("conversation_kind") or ""),
                created_at=str(row.get("created_at") or ""),
                size=int(row.get("size") or 0),
                mimetype=str(row.get("mimetype") or ""),
                account=str(row.get("account") or ""),
                team_id=str(row.get("team_id") or ""),
                content_sha256=str(row.get("content_sha256") or ""),
                width=int(row.get("width") or 0),
                height=int(row.get("height") or 0),
                url_private=str(row.get("url_private") or ""),
                raw=row,
            )
        )
    return matches


def _confidence(distance: int) -> str:
    if distance <= 6:
        return "same image"
    if distance <= 16:
        return "very likely the same image"
    if distance <= 28:
        return "possibly related"
    return "weak — check by eye"


def format_matches(matches: Sequence[SlackImageMatch]) -> str:
    if not matches:
        return (
            "No match. Nothing in the fingerprinted Slack corpus is close to this image.\n"
            "That is not proof the image was never sent: only files already fingerprinted "
            "are searchable. Check coverage with:\n"
            "  pdw sql --output json -q 'slack fingerprint coverage' "
            '"SELECT status, count(*) FROM derived_slack.file_fingerprints GROUP BY 1"'
        )
    lines: list[str] = []
    for index, match in enumerate(matches, start=1):
        lines.append(
            f"{index}. {match.identity} uploaded {match.name or match.file_id} to {match.channel}"
        )
        lines.append(f"   when:     {match.created_at}")
        lines.append(f"   file_id:  {match.file_id}   distance: {match.distance}/256 ({_confidence(match.distance)})")
        details = f"   bytes:    {match.size}"
        if match.width and match.height:
            details += f"   stored dims: {match.width}x{match.height}"
        lines.append(details)
        if match.slack_link:
            lines.append(f"   open:     {match.slack_link}")
        lines.append("")
    return "\n".join(lines).rstrip()


def lookup_sql_for_image(
    path: str | os.PathLike[str],
    *,
    limit: int = DEFAULT_LIMIT,
    max_distance: int = DEFAULT_MAX_DISTANCE,
    account: str | None = None,
) -> str:
    """Hash a local image and return the SQL that identifies it.

    Run the result with `pdw sql` or the `query` tool; the answer carries the
    uploader already resolved.
    """
    return build_lookup_sql(
        probe_fingerprint(path).dhash, limit=limit, max_distance=max_distance, account=account
    )
