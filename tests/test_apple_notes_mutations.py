from __future__ import annotations

import pytest

from personal_data_warehouse.apple_notes_mutations import (
    APPLE_NOTES_CREATE_NOTE_OPERATION,
    APPLE_NOTES_PROVIDER,
    APPLE_NOTES_UPDATE_NOTE_OPERATION,
    AppleNotesMutationExecutor,
    applescript_string,
    body_to_html,
)


def test_applescript_string_escapes_quotes_and_backslashes():
    assert applescript_string('he said "hi"') == '"he said \\"hi\\""'
    assert applescript_string("back\\slash") == '"back\\\\slash"'


def test_applescript_string_encodes_newlines_as_concatenated_returns():
    # A literal newline inside an AppleScript string literal is a syntax error, so the
    # builder has to splice them in as `linefeed` terms instead of embedding them.
    assert applescript_string("a\nb") == '"a" & linefeed & "b"'


def test_body_to_html_wraps_plain_text_lines_in_divs():
    assert body_to_html("one\ntwo") == "<div>one</div><div>two</div>"


def test_body_to_html_escapes_html_special_characters():
    assert body_to_html("a < b & c") == "<div>a &lt; b &amp; c</div>"


def test_body_to_html_passes_existing_markup_through():
    assert body_to_html("<div><b>bold</b></div>") == "<div><b>bold</b></div>"


class _FakeRunner:
    def __init__(self, results):
        self.results = list(results)
        self.scripts = []

    def __call__(self, script: str) -> str:
        self.scripts.append(script)
        return self.results.pop(0)


def _mutation(operation, payload):
    return {
        "id": "mut-1",
        "provider": APPLE_NOTES_PROVIDER,
        "operation": operation,
        "account": "you@example.com",
        "payload_json": payload,
    }


def test_create_note_returns_the_new_note_id():
    runner = _FakeRunner(["x-coredata://ABC/ICNote/p9\ncreated title"])
    executor = AppleNotesMutationExecutor(runner=runner)

    result = executor.execute(
        _mutation(
            APPLE_NOTES_CREATE_NOTE_OPERATION,
            {"folder": "PDW Agent", "name": "Runway", "body": "12 months"},
        )
    )

    assert result.status == "succeeded"
    assert result.result_json["note_id"] == "x-coredata://ABC/ICNote/p9"
    assert result.result_json["folder"] == "PDW Agent"
    script = runner.scripts[0]
    assert '"PDW Agent"' in script
    assert "make new folder" in script


def test_update_note_records_the_previous_body_so_a_replacement_is_recoverable():
    runner = _FakeRunner(["<div>old body</div>", "x-coredata://ABC/ICNote/p9\nNew title"])
    executor = AppleNotesMutationExecutor(runner=runner)

    result = executor.execute(
        _mutation(
            APPLE_NOTES_UPDATE_NOTE_OPERATION,
            {"note_id": "x-coredata://ABC/ICNote/p9", "body": "replacement"},
        )
    )

    assert result.status == "succeeded"
    assert result.result_json["previous_body"] == "<div>old body</div>"
    assert result.result_json["note_id"] == "x-coredata://ABC/ICNote/p9"


def test_append_body_keeps_the_existing_body_and_adds_to_it():
    runner = _FakeRunner(["<div>old</div>", "x-coredata://ABC/ICNote/p9\nTitle"])
    executor = AppleNotesMutationExecutor(runner=runner)

    result = executor.execute(
        _mutation(
            APPLE_NOTES_UPDATE_NOTE_OPERATION,
            {"note_id": "x-coredata://ABC/ICNote/p9", "append_body": "more"},
        )
    )

    assert result.status == "succeeded"
    write_script = runner.scripts[1]
    assert "<div>old</div>" in write_script
    assert "<div>more</div>" in write_script


def test_a_missing_note_is_terminal_not_retryable():
    def runner(_script):
        raise RuntimeError("Notes got an error: Can’t get note id \"x\". (-1728)")

    executor = AppleNotesMutationExecutor(runner=runner)
    result = executor.execute(
        _mutation(
            APPLE_NOTES_UPDATE_NOTE_OPERATION,
            {"note_id": "x-coredata://ABC/ICNote/gone", "append_body": "more"},
        )
    )

    assert result.status == "failed_terminal"
    assert "-1728" in result.error


def test_an_apple_event_timeout_is_retryable_because_notes_may_just_be_busy():
    def runner(_script):
        raise RuntimeError("Notes got an error: AppleEvent timed out. (-1712)")

    executor = AppleNotesMutationExecutor(runner=runner)
    result = executor.execute(
        _mutation(
            APPLE_NOTES_CREATE_NOTE_OPERATION,
            {"folder": "PDW Agent", "body": "hello"},
        )
    )

    assert result.status == "failed_retryable"


def test_a_denied_automation_grant_is_blocked_not_failed():
    def runner(_script):
        raise RuntimeError("Not authorized to send Apple events to Notes. (-1743)")

    executor = AppleNotesMutationExecutor(runner=runner)
    result = executor.execute(
        _mutation(
            APPLE_NOTES_CREATE_NOTE_OPERATION,
            {"folder": "PDW Agent", "body": "hello"},
        )
    )

    assert result.status == "blocked_missing_credentials"
    assert "Automation" in result.error


def test_a_foreign_provider_is_left_for_another_worker():
    executor = AppleNotesMutationExecutor(runner=lambda _s: "")
    result = executor.execute({"provider": "gmail", "operation": "gmail.archive_threads"})
    assert result.status == "failed_retryable"


def test_a_core_data_note_id_is_passed_through_untouched():
    from personal_data_warehouse.apple_notes_mutations import resolve_note_reference

    reference = "x-coredata://STORE/ICNote/p12"
    assert resolve_note_reference(reference, lookup=_unreachable_lookup) == reference


def test_a_warehouse_uuid_is_resolved_to_the_core_data_id_applescript_needs():
    # base_apple_notes.notes.note_id is ZIDENTIFIER, a UUID. Notes' AppleScript `id` is
    # an x-coredata:// URI built from the store UUID and the row's Z_PK. An agent reading
    # a note_id out of the warehouse must not have to know that.
    from personal_data_warehouse.apple_notes_mutations import resolve_note_reference

    def lookup(uuid):
        assert uuid == "0A1B2C3D-4E5F-6071-8293-A4B5C6D7E8F9"
        return ("1A2B3C4D-5E6F-7081-9203-A4B5C6D7E8F9", 2230)

    assert (
        resolve_note_reference("0A1B2C3D-4E5F-6071-8293-A4B5C6D7E8F9", lookup=lookup)
        == "x-coredata://1A2B3C4D-5E6F-7081-9203-A4B5C6D7E8F9/ICNote/p2230"
    )


def test_an_unresolvable_note_reference_fails_loudly():
    from personal_data_warehouse.apple_notes_mutations import (
        AppleNotesNoteNotFound,
        resolve_note_reference,
    )

    with pytest.raises(AppleNotesNoteNotFound):
        resolve_note_reference("00000000-0000-0000-0000-000000000000", lookup=lambda _u: None)


def _unreachable_lookup(_uuid):  # pragma: no cover - asserts it is never called
    raise AssertionError("a core-data id must not trigger a store lookup")
