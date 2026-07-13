"""Perceptual fingerprint contract: robust to resize/re-encode, not to content."""

from __future__ import annotations

import random
from io import BytesIO

from PIL import Image, ImageDraw

from personal_data_warehouse.photo_fingerprint import HASH_VERSION, compute_dhash, hamming


def synthetic_photo(seed: int, *, size: tuple[int, int] = (640, 480)) -> Image.Image:
    """Deterministic photo-like image: gradient background + shapes."""
    rng = random.Random(seed)
    # Seed-dependent gradient direction/palette so distinct "photos" differ in
    # their coarse structure, the way real distinct photos do.
    flip_x, flip_y = rng.random() < 0.5, rng.random() < 0.5
    base = rng.randint(0, 255)
    image = Image.new("RGB", size)
    for y in range(size[1]):
        for x in range(0, size[0], 16):
            gx = size[0] - x if flip_x else x
            gy = size[1] - y if flip_y else y
            image.paste(
                (int(255 * gx / size[0]), int(255 * gy / size[1]), base),
                (x, y, min(x + 16, size[0]), y + 1),
            )
    draw = ImageDraw.Draw(image)
    for _ in range(12):
        x0, y0 = rng.randint(0, size[0] - 60), rng.randint(0, size[1] - 60)
        x1, y1 = x0 + rng.randint(20, 60), y0 + rng.randint(20, 60)
        color = (rng.randint(0, 255), rng.randint(0, 255), rng.randint(0, 255))
        if rng.random() < 0.5:
            draw.ellipse((x0, y0, x1, y1), fill=color)
        else:
            draw.rectangle((x0, y0, x1, y1), fill=color)
    return image


def image_bytes(image: Image.Image, *, format: str = "PNG", **kwargs) -> bytes:
    buffer = BytesIO()
    image.save(buffer, format=format, **kwargs)
    return buffer.getvalue()


def test_hash_version_is_pinned():
    # Stored fingerprints are keyed by this; changing the algorithm requires a
    # version bump, not a silent behavior change.
    assert HASH_VERSION == "dhash16-v1"


def test_dhash_is_stable_for_identical_bytes():
    content = image_bytes(synthetic_photo(1))
    first = compute_dhash(content)
    second = compute_dhash(content)
    assert first.dhash == second.dhash
    assert len(first.dhash) == 64
    assert (first.width, first.height) == (640, 480)


def test_dhash_survives_resize_and_recompression():
    # The core cross-source scenario: the same shot at a different resolution
    # and encoding (e.g. full-res HEIC vs a Takeout-style downscaled JPEG).
    original = synthetic_photo(2)
    full = compute_dhash(image_bytes(original))
    rendition = original.resize((256, 192), Image.Resampling.LANCZOS)
    recompressed = compute_dhash(image_bytes(rendition, format="JPEG", quality=70))
    assert hamming(full.dhash, recompressed.dhash) <= 8


def test_dhash_normalizes_exif_orientation():
    upright = synthetic_photo(3)
    # Store the pixels rotated but tagged with EXIF orientation 6 (rotate 90
    # CW to display); exif_transpose must bring both to the same hash space.
    rotated = upright.rotate(90, expand=True)
    exif = Image.Exif()
    exif[0x0112] = 6
    tagged = image_bytes(rotated, format="JPEG", quality=95, exif=exif.tobytes())
    assert hamming(
        compute_dhash(image_bytes(upright)).dhash,
        compute_dhash(tagged).dhash,
    ) <= 8


def test_distinct_images_are_far_apart():
    a = compute_dhash(image_bytes(synthetic_photo(4)))
    b = compute_dhash(image_bytes(synthetic_photo(5)))
    assert hamming(a.dhash, b.dhash) > 48


def test_hamming_counts_bit_differences():
    assert hamming("0" * 64, "0" * 64) == 0
    assert hamming("0" * 64, "f" * 64) == 256
    assert hamming("0" * 63 + "1", "0" * 64) == 1
