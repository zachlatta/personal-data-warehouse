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

from dataclasses import dataclass
from io import BytesIO

import pillow_heif
from PIL import Image, ImageOps

pillow_heif.register_heif_opener()

HASH_VERSION = "dhash16-v1"
_GRID = 16


@dataclass(frozen=True)
class Fingerprint:
    dhash: str  # 64 hex chars = 256 bits
    width: int
    height: int


def compute_dhash(content: bytes) -> Fingerprint:
    """Fingerprint image bytes. Raises on undecodable input (callers treat
    that as "no fingerprint", never as a fatal row error)."""
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
