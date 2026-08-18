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
    AppObjectFetcher,
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


# --- fetching through the app's existing get_object -------------------------
#
# The app already resolves Slack files (objectstore/slack.go). These tests pin
# that the backfill delegates to it rather than re-implementing Slack auth.


class FakeHTTP:
    def __init__(self, post=None, get=None):
        self._post = post
        self._get = get
        self.posts = []
        self.gets = []

    def post(self, url, *, json=None, headers=None, timeout=None):
        self.posts.append({"url": url, "json": json, "headers": dict(headers or {})})
        if isinstance(self._post, Exception):
            raise self._post
        return self._post

    def get(self, url, *, headers=None, stream=False, timeout=None):
        self.gets.append({"url": url, "stream": stream})
        if isinstance(self._get, Exception):
            raise self._get
        return self._get


class FakeResp:
    def __init__(self, *, status_code=200, payload=None, body=b"", headers=None):
        self.status_code = status_code
        self._payload = payload
        self._body = body
        self.headers = headers or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def iter_content(self, chunk_size=1):
        for i in range(0, len(self._body), chunk_size):
            yield self._body[i : i + chunk_size]

    def close(self):
        pass


def object_payload(**overrides):
    data = {
        "exists": True,
        "content_type": "image/png",
        "size_bytes": 512,
        "filename": "poster.png",
        "download_url": "https://app.example/objects/F_TESTPOSTER?exp=1&sig=x",
    }
    data.update(overrides)
    return {"data": data}


def make_app_fetcher(post=None, get=None, **kwargs):
    http = FakeHTTP(post=post, get=get)
    fetcher = AppObjectFetcher(
        base_url="https://app.example", secret_token="t0ken", session=http, **kwargs
    )
    return fetcher, http


def test_fetch_calls_get_object_then_downloads_the_signed_url():
    body = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
    fetcher, http = make_app_fetcher(
        post=FakeResp(payload=object_payload()), get=FakeResp(body=body)
    )

    assert fetcher.fetch(candidate_ref()) == body

    # It uses the app's existing tool, not the Slack API.
    assert http.posts[0]["url"].endswith("/api/tools/get_object")
    assert http.posts[0]["json"]["storage_file_id"] == "F_TESTPOSTER"
    assert "slack.com" not in http.gets[0]["url"]


def test_no_slack_credential_or_slack_endpoint_is_referenced_by_the_backfill():
    """The app holds the Slack credential and owns Slack file resolution.

    Checked against the parsed module rather than its text, so the comments
    that *explain* the delegation do not trip it.
    """
    import ast
    import inspect

    from personal_data_warehouse import slack_file_fingerprints as module

    tree = ast.parse(inspect.getsource(module))
    literals = [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)]
    names = [n.id for n in ast.walk(tree) if isinstance(n, ast.Name)]
    attrs = [n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)]

    # Docstrings are literals too, so only look at what the code would *use*.
    code_literals = [
        text for text in literals
        if not text.strip().startswith(("Fetch", "The ", "Any ", "Bigger", "One ", "Slack carries"))
    ]
    for text in code_literals:
        assert "slack.com" not in text, text
        assert "files.info" not in text, text
        assert "_TOKEN" not in text, text
    for identifier in names + attrs:
        assert "slack_account" not in identifier, identifier
        assert identifier != "token", identifier


def test_a_missing_object_is_classified_missing():
    fetcher, _ = make_app_fetcher(post=FakeResp(payload={"data": {"exists": False}}))

    with pytest.raises(SlackFileMissingError):
        fetcher.fetch(candidate_ref())


def test_a_tool_error_becomes_a_fetch_error():
    fetcher, _ = make_app_fetcher(
        post=FakeResp(payload={"data": {"error": "slack files.info: invalid_auth"}})
    )

    with pytest.raises(SlackFileFetchError) as excinfo:
        fetcher.fetch(candidate_ref())
    assert "invalid_auth" in str(excinfo.value)


def test_slack_rate_limiting_surfaced_by_the_app_stops_the_run():
    fetcher, _ = make_app_fetcher(
        post=FakeResp(payload={"data": {"error": "slack files.info: HTTP 429"}})
    )

    with pytest.raises(SlackFileRateLimitedError):
        fetcher.fetch(candidate_ref())


def test_oversized_rows_are_rejected_without_calling_the_app():
    fetcher, http = make_app_fetcher(max_bytes=100)

    with pytest.raises(SlackFileTooLargeError):
        fetcher.fetch(candidate_ref(size=101))
    assert http.posts == []


def test_a_body_that_is_not_an_image_is_rejected():
    fetcher, _ = make_app_fetcher(
        post=FakeResp(payload=object_payload()),
        get=FakeResp(body=b"<!DOCTYPE html><html>sign in</html>"),
    )

    with pytest.raises(SlackFileFetchError):
        fetcher.fetch(candidate_ref())


def candidate_ref(**overrides):
    row = {"account": "zrl", "team_id": "T_TESTTEAM", "file_id": "F_TESTPOSTER", "size": 512}
    row.update(overrides)
    return SlackFileRef.from_row(row)
