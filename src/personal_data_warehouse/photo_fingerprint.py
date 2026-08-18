"""Perceptual fingerprints for photo dedup.

A 256-bit difference hash (dhash): orientation-normalize, grayscale, resize to
17x16, then compare each pixel to its right neighbor. Robust to the exact
transformations photo pipelines apply between sources — downscaling and
re-encoding (verified on real library HEICs: the same shot re-encoded at
1024px JPEG lands at Hamming distance ~1/256, a different shot at ~120/256).
NOT robust to crops or rotation-without-EXIF; those arrive as separate assets
by design (conservative dedup never destroys a real photo).

``hash_version`` is stored alongside every fingerprint so the algorithm can
evolve: bump HASH_VERSION and old rows simply stop matching the version
filter, recomputing lazily.
"""

from __future__ import annotations

import warnings
from contextlib import contextmanager
from dataclasses import dataclass
from io import BytesIO

import pillow_heif
from PIL import Image, ImageOps

pillow_heif.register_heif_opener()

HASH_VERSION = "dhash16-v1"
_GRID = 16


class ImageTooLargeError(Exception):
    """The image has more pixels than the caller is willing to decode."""


@contextmanager
def _pixel_ceiling(max_pixels: int | None):
    """Temporarily raise Pillow's decompression-bomb guard.

    Pillow's default ceiling is tuned for camera photos. Slack carries print
    artwork -- the file that motivated Slack fingerprinting is 420,750,000
    pixels (11x17 inches at 1500 DPI) -- which the default rejects outright.

    This is scoped and restored rather than set at import, because
    ``compute_dhash`` is shared with the photos pipeline and silently widening
    *its* decompression-bomb posture would be a security change smuggled in as
    a feature. Raising the ceiling cannot change a resulting hash: the guard
    runs before decoding, so a file either hashes identically or not at all.
    """
    if max_pixels is None:
        yield
        return
    previous = Image.MAX_IMAGE_PIXELS
    # Pillow warns above the limit and errors above 2x it. Setting half the
    # requested ceiling makes ``max_pixels`` the true hard stop. The warning in
    # between is then expected rather than informative -- the large size is the
    # documented reason this context manager exists -- so it is silenced here
    # and nowhere wider.
    Image.MAX_IMAGE_PIXELS = max(1, int(max_pixels) // 2)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", Image.DecompressionBombWarning)
            yield
    finally:
        Image.MAX_IMAGE_PIXELS = previous


@dataclass(frozen=True)
class Fingerprint:
    dhash: str  # 64 hex chars = 256 bits
    width: int
    height: int


def compute_dhash(content: bytes, *, max_pixels: int | None = None) -> Fingerprint:
    """Fingerprint image bytes. Raises on undecodable input (callers treat
    that as "no fingerprint", never as a fatal row error).

    ``max_pixels`` opts into decoding images above Pillow's default
    decompression-bomb ceiling, and raises :class:`ImageTooLargeError` beyond
    it so a caller can tell "too big" (a bounded resource decision) apart from
    "corrupt" (a permanent property of the bytes). Omitted, behaviour is
    exactly Pillow's default -- the photos pipeline is unaffected.
    """
    with _pixel_ceiling(max_pixels):
        try:
            return _compute_dhash(content)
        except Image.DecompressionBombError as exc:
            raise ImageTooLargeError(
                f"image exceeds the {max_pixels} pixel ceiling: {exc}"
            ) from exc


def _compute_dhash(content: bytes) -> Fingerprint:
    with Image.open(BytesIO(content)) as original:
        rendered = ImageOps.exif_transpose(original)
        width, height = rendered.size
        grid = rendered.convert("L").resize((_GRID + 1, _GRID), Image.Resampling.LANCZOS)
        pixels = list(grid.tobytes())
    bits = 0
    for row in range(_GRID):
        for column in range(_GRID):
            left = pixels[row * (_GRID + 1) + column]
            right = pixels[row * (_GRID + 1) + column + 1]
            bits = (bits << 1) | (1 if left > right else 0)
    return Fingerprint(dhash=f"{bits:064x}", width=width, height=height)


def hamming(a_hex: str, b_hex: str) -> int:
    return (int(a_hex, 16) ^ int(b_hex, 16)).bit_count()
