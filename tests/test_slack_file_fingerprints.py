"""Backfilling perceptual fingerprints over base_slack.files.

Reuses the photos machinery (compute_dhash + derived_enrichment.media_fingerprints);
the only new state is the link from a Slack file to the content sha its bytes
hash to. The corpus is ~905k live images / ~552 GB, so these tests pin the
properties that make walking it survivable: bounded, resumable, backed off, and
never caching the bytes.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from io import BytesIO

import pytest
from PIL import Image

from personal_data_warehouse.photo_fingerprint import HASH_VERSION
from personal_data_warehouse.slack_file_fingerprints import (
    SlackThumbnailFetcher,
    SlackFileFetchError,
    SlackFileMissingError,
    SlackFileRateLimitedError,
    SlackFileRef,
    SlackFileTooLargeError,
    STATUS_FAILED,
    STATUS_MISSING,
    STATUS_OK,
    STATUS_TOO_LARGE,
    STATUS_UNDECODABLE,
    SlackFileFingerprintRunner,
)

NOW = datetime(2026, 8, 18, 12, 0, tzinfo=UTC)


def png_bytes(color=(200, 30, 40), size=(64, 64)) -> bytes:
    buffer = BytesIO()
    image = Image.new("RGB", size, color)
    # A flat fill dhashes to all-zero; add structure so hashes differ per colour.
    for x in range(size[0] // 2):
        for y in range(size[1]):
            image.putpixel((x, y), (color[0] // 3, color[1], color[2]))
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def candidate(**overrides) -> dict:
    row = {
        "account": "zrl",
        "team_id": "T_TESTTEAM",
        "file_id": "F_TESTPOSTER",
        "url_private": "https://files.slack.com/files-pri/T09-F_TESTPOSTER/11x17.png",
        "thumbnail_url": "https://files.slack.com/files-tmb/T09-F_TESTPOSTER-abc/11x17_1024.png",
        "mimetype": "image/png",
        "name": "11x17.png",
        "size": 20055308,
    }
    row.update(overrides)
    return row


class NullLogger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


class FakeWarehouse:
    def __init__(self, candidates):
        self._candidates = list(candidates)
        self.ensure_calls = 0
        self.media_fingerprints = []
        self.links = []
        self.candidate_calls = []

    def ensure_slack_file_fingerprint_tables(self):
        self.ensure_calls += 1

    def slack_file_fingerprint_candidates(self, *, limit, now):
        self.candidate_calls.append({"limit": limit, "now": now})
        return self._candidates[:limit]

    def insert_media_fingerprints(self, rows):
        self.media_fingerprints.extend(rows)

    def upsert_slack_file_fingerprints(self, rows):
        self.links.extend(rows)

    def link_for(self, file_id):
        matches = [row for row in self.links if row["file_id"] == file_id]
        assert matches, f"no link row written for {file_id}"
        return matches[-1]


class FakeFetcher:
    def __init__(self, results):
        self.results = dict(results)
        self.fetched = []

    def fetch(self, ref: SlackFileRef) -> bytes:
        self.fetched.append(ref.file_id)
        result = self.results[ref.file_id]
        if isinstance(result, Exception):
            raise result
        return result


def make_runner(candidates, results, **kwargs):
    warehouse = FakeWarehouse(candidates)
    fetcher = FakeFetcher(results)
    runner = SlackFileFingerprintRunner(
        warehouse=warehouse,
        fetcher=fetcher,
        logger=NullLogger(),
        now=lambda: NOW,
        sleep=lambda _s: None,
        **kwargs,
    )
    return runner, warehouse, fetcher


# --- the core behaviour -----------------------------------------------------


def test_fingerprints_a_file_into_the_shared_media_fingerprints_table():
    content = png_bytes()
    runner, warehouse, _ = make_runner([candidate()], {"F_TESTPOSTER": content})

    summary = runner.run()

    assert warehouse.ensure_calls == 1
    assert summary.fingerprinted == 1
    # Reuses the photos fingerprint table and its versioned hash, not a fork.
    assert len(warehouse.media_fingerprints) == 1
    fingerprint = warehouse.media_fingerprints[0]
    assert fingerprint["hash_version"] == HASH_VERSION
    assert len(fingerprint["dhash"]) == 64
    assert fingerprint["width"] == 64 and fingerprint["height"] == 64

    link = warehouse.link_for("F_TESTPOSTER")
    assert link["status"] == STATUS_OK
    assert link["content_sha256"] == fingerprint["content_sha256"]
    assert link["account"] == "zrl" and link["team_id"] == "T_TESTTEAM"


def test_bytes_are_never_persisted_only_the_fingerprint():
    """552 GB of Slack images must not be copied into the warehouse."""
    content = png_bytes()
    runner, warehouse, _ = make_runner([candidate()], {"F_TESTPOSTER": content})

    runner.run()

    written = warehouse.media_fingerprints + warehouse.links
    for row in written:
        for key, value in row.items():
            assert not isinstance(value, (bytes, bytearray)), f"{key} persisted raw bytes"


def test_identical_bytes_in_two_files_share_one_fingerprint_row():
    content = png_bytes()
    runner, warehouse, _ = make_runner(
        [candidate(), candidate(file_id="F2", url_private="https://files.slack.com/f/F2")],
        {"F_TESTPOSTER": content, "F2": content},
    )

    runner.run()

    shas = {row["content_sha256"] for row in warehouse.media_fingerprints}
    assert len(shas) == 1
    assert len(warehouse.links) == 2


def test_undecodable_bytes_are_classified_not_fatal():
    runner, warehouse, _ = make_runner([candidate()], {"F_TESTPOSTER": b"\x89PNG\r\n\x1a\ngarbage"})

    summary = runner.run()

    assert summary.undecodable == 1
    assert warehouse.media_fingerprints == []
    assert warehouse.link_for("F_TESTPOSTER")["status"] == STATUS_UNDECODABLE


def test_oversized_and_missing_files_are_recorded_not_retried_forever():
    runner, warehouse, _ = make_runner(
        [candidate(), candidate(file_id="F2", url_private="https://x/F2")],
        {
            "F_TESTPOSTER": SlackFileTooLargeError("too big"),
            "F2": SlackFileMissingError("gone"),
        },
    )

    summary = runner.run()

    assert summary.too_large == 1 and summary.missing == 1
    assert warehouse.link_for("F_TESTPOSTER")["status"] == STATUS_TOO_LARGE
    assert warehouse.link_for("F2")["status"] == STATUS_MISSING


def test_a_failure_records_backoff_so_the_next_run_skips_it():
    runner, warehouse, _ = make_runner(
        [candidate()], {"F_TESTPOSTER": SlackFileFetchError("login page")}
    )

    summary = runner.run()

    assert summary.failed == 1
    link = warehouse.link_for("F_TESTPOSTER")
    assert link["status"] == STATUS_FAILED
    assert link["attempts"] == 1
    assert link["next_attempt_at"] > NOW
    assert link["last_error"]


def test_backoff_grows_with_attempts():
    runner, warehouse, _ = make_runner(
        [candidate(attempts=3)], {"F_TESTPOSTER": SlackFileFetchError("login page")}
    )

    runner.run()

    link = warehouse.link_for("F_TESTPOSTER")
    assert link["attempts"] == 4
    assert link["next_attempt_at"] - NOW > timedelta(hours=1)


# --- bounded and resumable --------------------------------------------------


def test_run_is_bounded_by_limit():
    rows = [candidate(file_id=f"F{i}", url_private=f"https://x/F{i}") for i in range(10)]
    runner, warehouse, fetcher = make_runner(
        rows, {f"F{i}": png_bytes(color=(i * 20 % 255, 40, 90)) for i in range(10)}, limit=3
    )

    runner.run()

    assert warehouse.candidate_calls[0]["limit"] == 3
    assert len(fetcher.fetched) == 3


def test_rate_limiting_stops_the_run_cleanly_so_it_resumes_next_time():
    """Slack limits are real here; a 429 must end the slice, not hammer on."""
    rows = [candidate(file_id=f"F{i}", url_private=f"https://x/F{i}") for i in range(3)]
    runner, warehouse, fetcher = make_runner(
        rows,
        {
            "F0": png_bytes(),
            "F1": SlackFileRateLimitedError("slow down", retry_after=90),
            "F2": png_bytes(color=(10, 200, 10)),
        },
    )

    summary = runner.run()

    assert summary.rate_limited is True
    assert fetcher.fetched == ["F0", "F1"]  # stopped, did not continue to F2
    # The rate-limited file keeps no failure attempt against it: it was never
    # given a fair try, so it must not burn down its retry budget.
    assert all(row["file_id"] != "F1" or row["status"] != STATUS_FAILED for row in warehouse.links)


def test_downloads_are_spaced_out_never_a_burst():
    """Slack flags a burst of downloads on one user as `excessive_downloads`.

    The hourly slice used to fetch ~300 images in about five minutes. Spacing
    each download keeps the run's rate close to a person browsing.
    """
    rows = [candidate(file_id=f"F{i}", url_private=f"https://x/F{i}") for i in range(3)]
    slept: list[float] = []
    warehouse = FakeWarehouse(rows)
    fetcher = FakeFetcher({f"F{i}": png_bytes(color=(i * 40 % 255, 60, 10)) for i in range(3)})
    runner = SlackFileFingerprintRunner(
        warehouse=warehouse,
        fetcher=fetcher,
        logger=NullLogger(),
        now=lambda: NOW,
        sleep=slept.append,
        download_spacing_seconds=90,
    )

    runner.run()

    assert fetcher.fetched == ["F0", "F1", "F2"]
    assert slept == [90, 90]  # between downloads, not before the first or after the last


def test_wall_clock_budget_stops_the_run():
    rows = [candidate(file_id=f"F{i}", url_private=f"https://x/F{i}") for i in range(5)]
    ticks = iter([NOW + timedelta(seconds=i * 30) for i in range(20)])
    warehouse = FakeWarehouse(rows)
    fetcher = FakeFetcher({f"F{i}": png_bytes(color=(i * 40 % 255, 60, 10)) for i in range(5)})
    runner = SlackFileFingerprintRunner(
        warehouse=warehouse,
        fetcher=fetcher,
        logger=NullLogger(),
        now=lambda: next(ticks),
        sleep=lambda _s: None,
        max_run_seconds=45,
    )

    runner.run()

    assert len(fetcher.fetched) < 5


# --- print-resolution uploads (found against the real 2026-08-16 file) -------


def test_a_print_resolution_poster_is_fingerprinted_not_rejected(monkeypatch):
    """The motivating file is 420,750,000 pixels: 11x17 inches at 1500 DPI.

    Pillow refuses images over 2x MAX_IMAGE_PIXELS as possible decompression
    bombs, and that default is tuned for phone photos. Left alone, the exact
    file that started all this would be classified 'undecodable' forever and
    never be findable. Slack carries print artwork, so this pipeline raises the
    ceiling deliberately rather than inheriting the photo default.
    """
    from PIL import Image

    content = png_bytes(size=(64, 64))
    # Stand in for a 420 MP poster by shrinking the guard instead of building one.
    monkeypatch.setattr(Image, "MAX_IMAGE_PIXELS", 8)

    runner, warehouse, _ = make_runner([candidate()], {"F_TESTPOSTER": content})
    summary = runner.run()

    assert summary.fingerprinted == 1, "print-resolution upload was not fingerprinted"
    assert warehouse.link_for("F_TESTPOSTER")["status"] == STATUS_OK


def test_raising_the_pixel_ceiling_does_not_leak_into_other_pipelines(monkeypatch):
    """photo_identity must keep its own decompression-bomb posture."""
    from PIL import Image

    monkeypatch.setattr(Image, "MAX_IMAGE_PIXELS", 12345)
    runner, _, _ = make_runner([candidate()], {"F_TESTPOSTER": png_bytes(size=(32, 32))})

    runner.run()

    assert Image.MAX_IMAGE_PIXELS == 12345


def test_an_image_beyond_even_the_raised_ceiling_is_recorded_as_too_large(monkeypatch):
    from PIL import Image

    monkeypatch.setattr(Image, "MAX_IMAGE_PIXELS", 8)
    runner, warehouse, _ = make_runner(
        [candidate()], {"F_TESTPOSTER": png_bytes(size=(64, 64))}, max_pixels=4
    )

    summary = runner.run()

    assert summary.too_large == 1
    assert summary.undecodable == 0
    link = warehouse.link_for("F_TESTPOSTER")
    assert link["status"] == STATUS_TOO_LARGE
    assert "pixel" in link["last_error"].lower()


# --- fetching a thumbnail, not the file ---------------------------------------
#
# Slack's audit log records every full download (`file_downloaded`), and ~6,500
# of them a day drew an `excessive_downloads` anomaly on Zach's user every three
# hours until 2026-10-05. A thumbnail fetch (files-tmb) is not audited, is ~100
# KB instead of ~600 KB, and its dhash is within 0-7 of 256 bits of the full
# image's (measured on 12 production files; lookups match within 40).


class FakeHTTP:
    def __init__(self, responses):
        self._responses = list(responses)
        self.gets = []

    def get(self, url, *, headers=None, stream=False, timeout=None):
        self.gets.append({"url": url, "headers": dict(headers or {}), "stream": stream})
        result = self._responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class FakeResp:
    def __init__(self, *, status_code=200, body=b"", headers=None):
        self.status_code = status_code
        self._body = body
        self.headers = headers or {}

    def iter_content(self, chunk_size=1):
        for i in range(0, len(self._body), chunk_size):
            yield self._body[i : i + chunk_size]

    def close(self):
        pass


def candidate_ref(**overrides):
    row = candidate(size=512)
    row.update(overrides)
    return SlackFileRef.from_row(row)


PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64


def make_thumb_fetcher(responses, **kwargs):
    http = FakeHTTP(responses)
    fetcher = SlackThumbnailFetcher(tokens={"zrl": "xoxp-test"}, session=http, **kwargs)
    return fetcher, http


def test_fetch_downloads_the_thumbnail_with_the_accounts_token():
    fetcher, http = make_thumb_fetcher([FakeResp(body=PNG)])

    assert fetcher.fetch(candidate_ref()) == PNG

    assert http.gets[0]["url"] == "https://files.slack.com/files-tmb/T09-F_TESTPOSTER-abc/11x17_1024.png"
    assert http.gets[0]["headers"]["Authorization"] == "Bearer xoxp-test"
    assert len(http.gets) == 1  # no files.info, no app hop, no full download


def test_a_file_without_a_thumbnail_falls_back_to_the_full_download():
    fetcher, http = make_thumb_fetcher([FakeResp(body=PNG)])

    fetcher.fetch(candidate_ref(thumbnail_url=""))

    assert http.gets[0]["url"] == "https://files.slack.com/files-pri/T09-F_TESTPOSTER/11x17.png"


def test_a_huge_original_is_still_fingerprinted_from_its_thumbnail():
    fetcher, http = make_thumb_fetcher([FakeResp(body=PNG)], max_bytes=100)

    assert fetcher.fetch(candidate_ref(size=500_000_000)) == PNG


def test_an_oversized_full_download_is_rejected_without_a_request():
    fetcher, http = make_thumb_fetcher([], max_bytes=100)

    with pytest.raises(SlackFileTooLargeError):
        fetcher.fetch(candidate_ref(thumbnail_url="", size=101))
    assert http.gets == []


def test_a_404_is_classified_missing():
    fetcher, _ = make_thumb_fetcher([FakeResp(status_code=404)])

    with pytest.raises(SlackFileMissingError):
        fetcher.fetch(candidate_ref())


def test_slack_rate_limiting_stops_the_run():
    fetcher, _ = make_thumb_fetcher([FakeResp(status_code=429, headers={"Retry-After": "30"})])

    with pytest.raises(SlackFileRateLimitedError) as excinfo:
        fetcher.fetch(candidate_ref())
    assert excinfo.value.retry_after == 30


def test_slacks_html_login_page_is_a_fetch_error_not_an_image():
    fetcher, _ = make_thumb_fetcher(
        [FakeResp(body=b"<!DOCTYPE html><html>sign in</html>", headers={"Content-Type": "text/html"})]
    )

    with pytest.raises(SlackFileFetchError):
        fetcher.fetch(candidate_ref())


def test_an_account_without_a_token_is_a_fetch_error():
    fetcher, http = make_thumb_fetcher([])

    with pytest.raises(SlackFileFetchError):
        fetcher.fetch(candidate_ref(account="someone-else"))
    assert http.gets == []


def test_full_downloads_are_capped_per_run_so_they_never_burst():
    """The rare image with no thumbnail costs an audited download; a run takes few."""
    rows = [candidate(file_id=f"F{i}", thumbnail_url="", url_private=f"https://x/F{i}") for i in range(5)]
    rows.append(candidate(file_id="F_THUMB"))
    runner, warehouse, fetcher = make_runner(
        rows,
        {**{f"F{i}": png_bytes(color=(i * 40 % 255, 60, 10)) for i in range(5)}, "F_THUMB": png_bytes()},
        max_full_downloads=2,
    )

    summary = runner.run()

    assert fetcher.fetched == ["F0", "F1", "F_THUMB"]
    assert summary.full_downloads == 2
    # The skipped ones are not recorded, so the next run picks them up.
    assert {row["file_id"] for row in warehouse.links} == {"F0", "F1", "F_THUMB"}
