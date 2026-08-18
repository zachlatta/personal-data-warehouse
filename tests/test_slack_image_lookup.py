"""'Who sent this image?' — probe a local image against Slack's fingerprints.

The 2026-08-16 failure had two halves. This module is the second half: the
agent could not compare the picture it was handed to anything stored. It also
fixes the first half's tail — the answer must resolve the uploader, because
that agent found the right row and still never joined raw user id to
base_slack.users.
"""

from __future__ import annotations

from io import BytesIO

import pytest
from PIL import Image

from personal_data_warehouse.photo_fingerprint import HASH_VERSION, compute_dhash, hamming
from personal_data_warehouse.slack_image_lookup import (
    SlackImageMatch,
    build_lookup_sql,
    format_matches,
    lookup_sql_for_image,
    parse_matches,
    probe_fingerprint,
)

POSTER_SHA = "a" * 64
PROBE_DHASH = "0123456789abcdef" * 4


def poster_image(size=(680, 1056)) -> Image.Image:
    """Poster-shaped: smooth gradient ground plus a few solid blocks.

    Deliberately *not* a high-frequency checkerboard. dhash compares each cell
    of a 17x16 grid to its right neighbour, so detail finer than the grid
    aliases under rescaling — a synthetic checkerboard drifts ~30/256 on a 75%
    resize while a real poster (large smooth areas, blocks of text) drifts in
    the single digits. Testing against the pathological case would measure the
    fixture, not the algorithm.
    """
    image = Image.new("RGB", size, (250, 245, 230))
    width, height = size
    for y in range(height):
        for x in range(width):
            image.putpixel((x, y), (30 + (200 * x) // width, 40 + (180 * y) // height, 150))
    for top, left, box_h, box_w, colour in (
        (80, 60, 260, 560, (250, 250, 245)),
        (420, 60, 90, 400, (15, 15, 20)),
        (560, 60, 60, 520, (220, 60, 40)),
        (700, 120, 240, 240, (10, 90, 160)),
    ):
        for y in range(top, min(top + box_h, height)):
            for x in range(left, min(left + box_w, width)):
                image.putpixel((x, y), colour)
    return image


def encode(image: Image.Image, fmt="PNG", **kwargs) -> bytes:
    buffer = BytesIO()
    image.save(buffer, format=fmt, **kwargs)
    return buffer.getvalue()


# --- the property the whole feature rests on --------------------------------


def test_reencoded_copy_has_a_different_sha_but_a_near_identical_fingerprint():
    """Slack's copy is 20,055,308 bytes; Zach's paste was 18,779,083.

    Exact-sha matching is therefore useless, which is the whole reason for
    perceptual hashing. A re-encode must stay close; a different image must not.
    """
    original = poster_image()
    stored = encode(original)
    pasted = encode(original.resize((510, 792), Image.Resampling.LANCZOS), fmt="JPEG", quality=82)

    assert stored != pasted  # different bytes, so different sha
    distance = hamming(compute_dhash(stored).dhash, compute_dhash(pasted).dhash)
    assert distance <= 12, f"re-encoded copy drifted {distance}/256"

    unrelated = encode(Image.new("RGB", (680, 1056), (10, 10, 10)))
    assert hamming(compute_dhash(stored).dhash, compute_dhash(unrelated).dhash) > 40


def test_probe_fingerprint_reads_a_local_path(tmp_path):
    path = tmp_path / "poster.png"
    path.write_bytes(encode(poster_image(size=(120, 160))))

    fingerprint = probe_fingerprint(path)

    assert len(fingerprint.dhash) == 64
    assert fingerprint.width == 120 and fingerprint.height == 160


# --- the SQL -----------------------------------------------------------------


def test_lookup_sql_ranks_by_hamming_distance_and_resolves_the_uploader():
    sql = build_lookup_sql(PROBE_DHASH, limit=5, max_distance=40)

    assert sql.lstrip().upper().startswith("SELECT")
    assert PROBE_DHASH in sql
    assert "bit_count" in sql
    assert "marts_slack.image_fingerprints" in sql
    # Uploader identity is not optional: it is the answer to the question.
    for column in ("uploader_user_id", "uploader_display_name", "conversation_id", "distance"):
        assert column in sql
    assert "LIMIT 5" in sql


def test_lookup_sql_filters_to_the_stored_hash_version():
    assert HASH_VERSION in build_lookup_sql(PROBE_DHASH)


@pytest.mark.parametrize(
    "bad",
    ["", "xyz", "g" * 64, "a" * 63, "a" * 65, "'; DROP TABLE base_slack.files; --"],
)
def test_lookup_sql_rejects_anything_that_is_not_a_256_bit_hex_hash(bad):
    """The hash is interpolated into SQL sent over the HTTP tool API."""
    with pytest.raises(ValueError):
        build_lookup_sql(bad)


def test_account_filter_is_escaped():
    sql = build_lookup_sql(PROBE_DHASH, account="o'brien")

    assert "o''brien" in sql


# --- parsing and presentation ------------------------------------------------


NDJSON = (
    '{"file_id":"F_TESTPOSTER","name":"11x17.png","distance":3,'
    '"uploader_user_id":"U_TESTUPLOADER","uploader_display_name":"Poster Designer",'
    '"uploader_name":"designer","conversation_id":"D_TESTDM","conversation_name":"",'
    '"conversation_kind":"im","created_at":"2026-08-11T18:37:54Z","size":20055308,'
    '"mimetype":"image/png","permalink":"","account":"zrl","team_id":"T_TESTTEAM"}\n'
)


def test_parse_matches_reads_the_ndjson_the_sql_api_returns():
    matches = parse_matches(NDJSON)

    assert len(matches) == 1
    match = matches[0]
    assert match.file_id == "F_TESTPOSTER"
    assert match.distance == 3
    assert match.uploader_display_name == "Poster Designer"
    assert match.conversation_id == "D_TESTDM"


def test_format_matches_leads_with_who_sent_it():
    output = format_matches(parse_matches(NDJSON))

    assert "F_TESTPOSTER" in output
    assert "Poster Designer" in output
    assert "U_TESTUPLOADER" in output
    assert "D_TESTDM" in output
    assert "2026-08-11" in output
    assert "3" in output


def test_format_matches_says_so_when_nothing_is_close():
    assert "no match" in format_matches([]).lower()


def test_lookup_sql_for_image_hashes_the_file_and_embeds_that_hash(tmp_path):
    path = tmp_path / "poster.png"
    path.write_bytes(encode(poster_image(size=(120, 160))))
    expected = compute_dhash(path.read_bytes()).dhash

    sql = lookup_sql_for_image(path, limit=5, max_distance=40)

    assert expected in sql
    assert "marts_slack.image_fingerprints" in sql


def test_the_module_exposes_no_command_line_surface():
    """The lookup is plain SQL through existing tools; no new command exists."""
    from personal_data_warehouse import slack_image_lookup as module

    assert not hasattr(module, "main")
    assert "argparse" not in dir(module)


# --- DM rendering (found running against the real 2026-08-16 row) -----------


def test_a_dm_is_rendered_as_a_dm_not_as_a_channel():
    """Slack stores a DM's `name` as the *other user's id*, not a channel name.

    Rendering that as `#U0EXAMPLE123` reads as a channel called U06..., which is
    exactly the kind of confidently-wrong detail that started this. An `im`
    must render as a DM whatever its stored name is.
    """
    match = SlackImageMatch(
        file_id="F_TESTPOSTER",
        distance=0,
        conversation_id="D_TESTDM",
        conversation_name="U_TESTUPLOADER",  # Slack really does store it this way
        conversation_kind="im",
    )

    assert match.channel == "DM D_TESTDM"
    assert "#" not in match.channel


def test_a_real_channel_still_renders_with_a_hash():
    match = SlackImageMatch(
        file_id="F_TESTPOSTER", distance=0,
        conversation_id="C_TESTCHAN", conversation_name="design", conversation_kind="channel",
    )

    assert match.channel == "#design"


def test_a_group_dm_is_not_rendered_as_a_channel():
    match = SlackImageMatch(
        file_id="F_TESTPOSTER", distance=0,
        conversation_id="G_TESTMPIM", conversation_name="mpdm-a--b--c-1", conversation_kind="mpim",
    )

    assert match.channel.startswith("group DM")


# --- identity completeness (the original question asked for a HANDLE) -------


def test_output_shows_real_name_handle_and_user_id_together():
    """Slack keeps three different identities and they are all different.

    A real row has real_name "A Person", display_name "aperson", and name
    (the @handle) "aperson110". The question that started this asked for the
    *handle* specifically, so collapsing these to one field can answer the
    wrong question while looking right.
    """
    match = SlackImageMatch(
        file_id="F_TESTPOSTER",
        distance=0,
        name="poster.png",
        uploader_user_id="U_TESTUPLOADER",
        uploader_real_name="A Person",
        uploader_display_name="aperson",
        uploader_name="aperson110",
        conversation_id="D_TESTDM",
        conversation_kind="im",
    )

    output = format_matches([match])

    assert "A Person" in output
    assert "@aperson110" in output, "the Slack handle must be shown"
    assert "aperson" in output
    assert "U_TESTUPLOADER" in output


def test_handle_falls_back_when_slack_has_no_name():
    match = SlackImageMatch(file_id="F", distance=0, uploader_user_id="U1")

    assert match.handle == ""
    assert "U1" in format_matches([match])
