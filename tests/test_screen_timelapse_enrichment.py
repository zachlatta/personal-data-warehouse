"""What was on a screen, frame by frame, and only the frames where it was in use.

The screen timelapse stores a whole-screen image about once a minute and, when
the capture loop could read one, the accessibility tree of the same moment. An
agent reads each frame: the tree first, and the screenshot only when the tree
does not say what is on screen. It decides whether the device was in use at
all (an iPhone face down writes a black frame every minute), and
marts_screen_timelapse.active_screens is the frames where it was, with how to
fetch each one. These tests pin that the agent gets the tree before the
pixels, that a byte-identical frame is judged once, that only the recent
window is read, and that a failure or a new prompt does not lose a frame.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import gzip
from io import BytesIO
import json

from PIL import Image
import pytest

from personal_data_warehouse.agent_runner import AgentRunRequest, AgentRunResult
from personal_data_warehouse.file_attachment_enrichment import sniff_content_type
from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.screen_timelapse_enrichment import (
    SCREEN_DESCRIPTION_PROMPT_VERSION,
    SCREEN_DESCRIPTION_TASK_TYPE,
    ScreenTimelapseEnrichmentRunner,
    accessibility_tree_text,
    backfill_accessibility_tree_text,
    load_screen_description_candidates,
    screen_description_schema,
)
from tests.conftest import cleanup_test_warehouse, make_test_schema
from tests.test_marts_layering import _postgres_url

NOW = datetime(2026, 10, 24, 14, 0, tzinfo=UTC)


@pytest.fixture()
def warehouse():
    wh = PostgresWarehouse(_postgres_url(), schema=make_test_schema("screen_enrich"))
    try:
        wh.ensure_screen_timelapse_tables()
        yield wh
    finally:
        cleanup_test_warehouse(wh)


class Logger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


def _png(color: tuple[int, int, int], size: tuple[int, int] = (60, 130)) -> bytes:
    output = BytesIO()
    Image.new("RGB", size, color=color).save(output, format="PNG")
    return output.getvalue()


BLACK = _png((0, 0, 0))
MAIL = _png((240, 240, 250))
MAC_TREE = gzip.compress(
    json.dumps(
        {
            "frontmost_app": {"name": "Mail"},
            "apps": [{"name": "Mail", "windows": [{"title": "Inbox", "children": [{"value": "Quarterly plan"}]}]}],
        }
    ).encode()
)


class FakeStore:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.fetched: list[str] = []

    def get_object(self, ref) -> bytes:
        self.fetched.append(ref["storage_file_id"])
        return self.objects[ref["storage_file_id"]]


class FakeAgent:
    """Answers like the agent would, keyed by what it was given."""

    def __init__(self, *, fail: bool = False) -> None:
        self.requests: list[AgentRunRequest] = []
        self.fail = fail

    def run(self, request: AgentRunRequest) -> AgentRunResult:
        self.requests.append(request)
        names = set(request.input_files)
        screenshot = request.input_files["screenshot.jpg"]
        with Image.open(BytesIO(screenshot)) as image:
            dark = max(image.convert("L").getextrema()) < 10
        if dark:
            output = {
                "screen_state": "off",
                "evidence": "screenshot",
                "app": "",
                "window_title": "",
                "summary": "The display is black.",
                "screen_text": "",
                "uncertainties": [],
            }
        elif "accessibility_tree.json" in names:
            output = {
                "screen_state": "active",
                "evidence": "accessibility_tree",
                "app": "Mail",
                "window_title": "Inbox",
                "summary": "Reading the inbox in Mail.",
                "screen_text": "Quarterly plan",
                "uncertainties": [],
            }
        else:
            output = {
                "screen_state": "active",
                "evidence": "screenshot",
                "app": "Messages",
                "window_title": "",
                "summary": "A conversation in Messages.",
                "screen_text": "see you at 6",
                "uncertainties": [],
            }
        return AgentRunResult(
            run_id=request.run_id,
            provider=request.provider or "codex",
            model=request.model or "fake-model",
            task_type=request.task_type,
            subject_id=request.subject_id,
            prompt_version=request.prompt_version,
            input_sha256=request.input_sha256,
            status="error" if self.fail else "completed",
            final_output_json={} if self.fail else output,
            error="agent exploded" if self.fail else "",
            exit_code=1 if self.fail else 0,
            started_at=NOW,
            completed_at=NOW + timedelta(seconds=12),
            events=[],
        )


def _screenshot(
    wh,
    device,
    captured_at,
    *,
    display=1,
    sha,
    file_id,
    mime="image/png",
    ingested_at=None,
):
    wh._command(
        """
        INSERT INTO @screen_timelapse_screenshots (device, captured_at, display_index,
            source_filename, content_sha256, mime_type, size_bytes, storage_backend,
            storage_key, storage_file_id, storage_url, ingested_at)
        VALUES (%s, %s, %s, %s, %s, %s, 1000, 'google_drive', %s, %s, '', %s)
        ON CONFLICT (device, captured_at, display_index) DO UPDATE SET
            source_filename = EXCLUDED.source_filename,
            content_sha256 = EXCLUDED.content_sha256,
            mime_type = EXCLUDED.mime_type,
            storage_key = EXCLUDED.storage_key,
            storage_file_id = EXCLUDED.storage_file_id,
            ingested_at = EXCLUDED.ingested_at
        """,
        (
            device,
            captured_at,
            display,
            f"{file_id}.png",
            sha,
            mime,
            f"key/{file_id}",
            file_id,
            ingested_at or captured_at,
        ),
    )


def _tree(wh, device, captured_at, *, sha, file_id, mime="application/gzip", text=""):
    """A tree row; ``text`` empty is a tree stored before the app wrote it inline."""
    wh._command(
        """
        INSERT INTO @screen_timelapse_accessibility_trees (device, captured_at, source_filename,
            content_sha256, mime_type, size_bytes, storage_backend, storage_key, storage_file_id,
            storage_url, ingested_at, content_text)
        VALUES (%s, %s, %s, %s, %s, 100, 'google_drive', %s, %s, '', %s, %s)
        """,
        (device, captured_at, f"{file_id}.ax", sha, mime, f"key/{file_id}", file_id, captured_at, text),
    )


def _tree_text(wh) -> dict[str, str]:
    return {
        r["storage_file_id"]: r["content_text"]
        for r in wh._query_dicts("SELECT storage_file_id, content_text FROM @screen_timelapse_accessibility_trees")
    }


def _runner(wh, store, agent, *, at: datetime = NOW, **kwargs) -> ScreenTimelapseEnrichmentRunner:
    return ScreenTimelapseEnrichmentRunner(
        warehouse=wh,
        agent=agent,
        object_store=store,
        logger=Logger(),
        provider="codex",
        model="fake-model",
        now=lambda: at,
        **kwargs,
    )


def _mart(wh) -> list[dict]:
    return wh._query_dicts(
        "SELECT * FROM @marts_screen_timelapse_active_screens ORDER BY device, captured_at, display_index"
    )


def test_the_agent_reads_the_tree_first_and_the_mart_holds_only_frames_in_use(warehouse) -> None:
    mac_at = NOW - timedelta(hours=1)
    phone_at = NOW - timedelta(hours=2)
    _screenshot(warehouse, "crobat", mac_at, sha="sha-mail", file_id="fid-mail")
    _tree(warehouse, "crobat", mac_at, sha="sha-tree", file_id="fid-tree")
    _screenshot(warehouse, "joltik", phone_at, sha="sha-black", file_id="fid-black")
    store = FakeStore({"fid-mail": MAIL, "fid-tree": MAC_TREE, "fid-black": BLACK})
    agent = FakeAgent()

    summary = _runner(warehouse, store, agent).sync(limit=10)

    assert summary.frames_described == 2
    mac_request = next(r for r in agent.requests if "accessibility_tree.json" in r.input_files)
    # The tree reaches the agent decompressed, beside the screenshot it may open.
    assert json.loads(mac_request.input_files["accessibility_tree.json"])["frontmost_app"]["name"] == "Mail"
    assert mac_request.task_type == SCREEN_DESCRIPTION_TASK_TYPE
    prompt = json.loads(mac_request.prompt)
    assert prompt["accessibility_tree"]["path"] == "inputs/accessibility_tree.json"
    assert prompt["screenshot"]["path"] == "inputs/screenshot.jpg"

    rows = _mart(warehouse)
    assert [(r["device"], r["app"], r["evidence"]) for r in rows] == [("crobat", "Mail", "accessibility_tree")]
    mac = rows[0]
    # How to get the frame itself, and its tree.
    assert mac["storage_file_id"] == "fid-mail"
    assert mac["mime_type"] == "image/png"
    assert mac["accessibility_tree_storage_file_id"] == "fid-tree"
    assert mac["summary"] == "Reading the inbox in Mail."
    assert mac["screen_text"] == "Quarterly plan"
    assert mac["prompt_version"] == SCREEN_DESCRIPTION_PROMPT_VERSION
    # The tree predated its inline copy, so reading it fetched it once and kept its text.
    assert json.loads(_tree_text(warehouse)["fid-tree"])["frontmost_app"]["name"] == "Mail"

    # The black frame was judged (off) and is kept out of the mart, not lost.
    states = warehouse._query_dicts(
        "SELECT device, screen_state, status FROM @screen_timelapse_descriptions ORDER BY device"
    )
    assert states == [
        {"device": "crobat", "screen_state": "active", "status": "completed"},
        {"device": "joltik", "screen_state": "off", "status": "completed"},
    ]
    # Agent runs are recorded like every other enrichment's.
    runs = warehouse._query_dicts("SELECT task_type, status FROM @agent_runs")
    assert runs == [{"task_type": SCREEN_DESCRIPTION_TASK_TYPE, "status": "completed"}] * 2


def test_a_frame_without_a_tree_is_read_from_the_screenshot(warehouse) -> None:
    at = NOW - timedelta(minutes=30)
    _screenshot(warehouse, "pichu", at, sha="sha-chat", file_id="fid-chat", mime="image/avif")
    agent = FakeAgent()

    _runner(warehouse, FakeStore({"fid-chat": _png((10, 120, 200))}), agent).sync(limit=10)

    (request,) = agent.requests
    assert set(request.input_files) == {"screenshot.jpg"}
    assert "accessibility_tree" not in json.loads(request.prompt)
    (row,) = _mart(warehouse)
    assert row["evidence"] == "screenshot"
    assert row["accessibility_tree_storage_file_id"] is None


def test_byte_identical_frames_without_a_tree_are_judged_once(warehouse) -> None:
    for minutes in range(5):
        _screenshot(
            warehouse, "joltik", NOW - timedelta(minutes=minutes + 1), sha="sha-black", file_id=f"fid-{minutes}"
        )
    store = FakeStore({f"fid-{minutes}": BLACK for minutes in range(5)})
    agent = FakeAgent()

    summary = _runner(warehouse, store, agent).sync(limit=10)

    assert len(agent.requests) == 1
    assert summary.frames_described == 5
    assert summary.frames_reused == 4
    rows = warehouse._query_dicts(
        "SELECT screen_state, is_reused, agent_run_id FROM @screen_timelapse_descriptions ORDER BY captured_at"
    )
    assert {r["screen_state"] for r in rows} == {"off"}
    assert sorted(r["is_reused"] for r in rows) == [0, 1, 1, 1, 1]
    assert len({r["agent_run_id"] for r in rows}) == 1
    # A later run finds the judgment already made and runs nothing.
    _screenshot(warehouse, "joltik", NOW, sha="sha-black", file_id="fid-new")
    store.objects["fid-new"] = BLACK
    _runner(warehouse, store, agent).sync(limit=10)
    assert len(agent.requests) == 1
    assert _mart(warehouse) == []


def test_only_the_recent_window_is_read_newest_first(warehouse) -> None:
    _screenshot(warehouse, "crobat", NOW - timedelta(hours=30), sha="sha-old", file_id="fid-old")
    _screenshot(
        warehouse,
        "pichu",
        NOW - timedelta(hours=36),
        sha="sha-late",
        file_id="fid-late",
        ingested_at=NOW - timedelta(minutes=10),
    )
    _screenshot(warehouse, "crobat", NOW - timedelta(hours=3), sha="sha-a", file_id="fid-a")
    _screenshot(warehouse, "crobat", NOW - timedelta(hours=1), sha="sha-b", file_id="fid-b")

    candidates = load_screen_description_candidates(
        warehouse,
        prompt_version=SCREEN_DESCRIPTION_PROMPT_VERSION,
        since=NOW - timedelta(hours=24),
        limit=None,
    )

    assert [c["storage_file_id"] for c in candidates] == ["fid-b", "fid-a", "fid-late"]


def test_a_reencoded_image_for_an_already_described_frame_is_not_redescribed(warehouse) -> None:
    at = NOW - timedelta(hours=30)
    _screenshot(
        warehouse,
        "pichu",
        at,
        sha="sha-avif",
        file_id="fid-avif",
        mime="image/avif",
        ingested_at=NOW - timedelta(minutes=20),
    )
    store = FakeStore({"fid-avif": MAIL, "fid-webp": _png((20, 140, 220))})
    agent = FakeAgent()

    first = _runner(warehouse, store, agent).sync(limit=10)
    assert first.frames_described == 1
    assert len(agent.requests) == 1

    # Switchover uploads a differently encoded image for the same moment; the
    # completed description stands and is not re-run.
    _screenshot(
        warehouse,
        "pichu",
        at,
        sha="sha-webp",
        file_id="fid-webp",
        mime="image/webp",
        ingested_at=NOW - timedelta(minutes=5),
    )
    second = _runner(warehouse, store, agent).sync(limit=10)
    assert second.frames_seen == 0
    assert second.frames_described == 0
    assert len(agent.requests) == 1


def test_a_tree_that_lands_after_its_frame_was_described_is_read_again(warehouse) -> None:
    at = NOW - timedelta(minutes=10)
    _screenshot(warehouse, "crobat", at, sha="sha-mail", file_id="fid-mail")
    store = FakeStore({"fid-mail": MAIL, "fid-tree": MAC_TREE})
    agent = FakeAgent()
    _runner(warehouse, store, agent).sync(limit=10)
    assert _mart(warehouse)[0]["evidence"] == "screenshot"

    _tree(warehouse, "crobat", at, sha="sha-tree", file_id="fid-tree")
    _runner(warehouse, store, agent).sync(limit=10)

    assert len(agent.requests) == 2
    (row,) = _mart(warehouse)
    assert row["evidence"] == "accessibility_tree"
    assert row["accessibility_tree_storage_file_id"] == "fid-tree"


def test_a_failure_is_retried_up_to_the_cap_and_never_reaches_the_mart(warehouse) -> None:
    _screenshot(warehouse, "crobat", NOW - timedelta(minutes=5), sha="sha-mail", file_id="fid-mail")
    store = FakeStore({"fid-mail": MAIL})
    failing = FakeAgent(fail=True)

    for _ in range(3):
        summary = _runner(warehouse, store, failing, max_error_attempts=2).sync(limit=10)
    assert summary.frames_seen == 0
    assert len(failing.requests) == 2
    (row,) = warehouse._query_dicts("SELECT status, attempts, error FROM @screen_timelapse_descriptions")
    assert row == {"status": "error", "attempts": 2, "error": "agent exploded"}
    assert _mart(warehouse) == []


def test_a_new_prompt_version_reads_every_frame_again_and_keeps_the_old_answer_until_then(warehouse) -> None:
    _screenshot(warehouse, "crobat", NOW - timedelta(minutes=5), sha="sha-mail", file_id="fid-mail")
    store = FakeStore({"fid-mail": MAIL})
    _runner(warehouse, store, FakeAgent()).sync(limit=10)

    later = NOW + timedelta(minutes=1)
    _runner(
        warehouse, store, FakeAgent(fail=True), at=later, prompt_version="screen-description-test-v2"
    ).sync(limit=10)
    (row,) = _mart(warehouse)
    assert row["prompt_version"] == SCREEN_DESCRIPTION_PROMPT_VERSION

    _runner(warehouse, store, FakeAgent(), at=later, prompt_version="screen-description-test-v2").sync(limit=10)
    (row,) = _mart(warehouse)
    assert row["prompt_version"] == "screen-description-test-v2"


def test_the_output_schema_is_strict_for_codex() -> None:
    schema = screen_description_schema()
    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == set(schema["properties"])
    assert schema["properties"]["screen_state"]["enum"] == ["active", "locked", "off"]


def test_the_pixel_phones_avif_frames_are_identified_as_images() -> None:
    avif_header = b"\x00\x00\x00\x1cftypavif" + b"\x00" * 16
    assert sniff_content_type(avif_header) == "image/avif"


def test_a_tree_stored_inline_is_read_from_the_warehouse_not_the_object_store(warehouse) -> None:
    at = NOW - timedelta(minutes=3)
    _screenshot(warehouse, "pichu", at, sha="sha-mail", file_id="fid-mail", mime="image/avif")
    xml = '<?xml version="1.0"?><hierarchy><node text="Inbox" package="com.example.mail"/></hierarchy>'
    _tree(warehouse, "pichu", at, sha="sha-tree", file_id="fid-tree", mime="application/gzip", text=xml)
    store = FakeStore({"fid-mail": MAIL})
    agent = FakeAgent()

    _runner(warehouse, store, agent).sync(limit=10)

    assert store.fetched == ["fid-mail"]
    (request,) = agent.requests
    assert request.input_files["accessibility_tree.xml"] == xml
    assert json.loads(request.prompt)["accessibility_tree"]["path"] == "inputs/accessibility_tree.xml"


def test_backfill_writes_every_older_trees_text_inline_once(warehouse) -> None:
    old = NOW - timedelta(days=40)
    _tree(warehouse, "crobat", old, sha="sha-mac", file_id="fid-mac")
    _tree(warehouse, "joltik", old, sha="sha-phone", file_id="fid-phone", mime="application/json")
    _tree(warehouse, "pichu", old, sha="sha-done", file_id="fid-done", text="<hierarchy/>")
    phone_tree = b'[{"caption": "Messages"}, {"caption": "a\x00b \xff"}]'
    store = FakeStore({"fid-mac": MAC_TREE, "fid-phone": phone_tree})

    filled = backfill_accessibility_tree_text(warehouse, store, limit=100, logger=Logger())

    assert filled == 2
    texts = _tree_text(warehouse)
    assert json.loads(texts["fid-mac"])["frontmost_app"]["name"] == "Mail"
    # NUL and invalid UTF-8 are replaced, as the app's ingest does.
    assert texts["fid-phone"] == '[{"caption": "Messages"}, {"caption": "a\ufffdb \ufffd"}]'
    assert texts["fid-done"] == "<hierarchy/>"
    assert backfill_accessibility_tree_text(warehouse, store, limit=100, logger=Logger()) == 0
    assert sorted(store.fetched) == ["fid-mac", "fid-phone"]


def test_the_inline_text_column_reaches_a_tree_table_that_predates_it(warehouse) -> None:
    warehouse._command("ALTER TABLE @screen_timelapse_accessibility_trees DROP COLUMN content_text")

    warehouse.ensure_screen_timelapse_tables()

    columns = {
        r["column_name"]
        for r in warehouse._query_dicts(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
            (warehouse.physical_schema_name("base_screen_timelapse"), "accessibility_trees"),
        )
    }
    assert "content_text" in columns


def test_tree_text_matches_what_the_app_writes() -> None:
    assert accessibility_tree_text(gzip.compress(b'{"a": "x\x00y"}')) == '{"a": "x\ufffdy"}'
    assert accessibility_tree_text(b"<hierarchy/>") == "<hierarchy/>"
