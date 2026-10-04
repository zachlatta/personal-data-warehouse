from __future__ import annotations

import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "voice_memo_speaker_benchmark.py"
spec = importlib.util.spec_from_file_location("voice_memo_speaker_benchmark", SCRIPT)
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


def _word(text, start_s, speaker):
    return {"text": text, "start": int(start_s * 1000), "end": int(start_s * 1000) + 900, "speaker": speaker, "confidence": 0.9}


RESULT = {
    "utterances": [
        {"speaker": "A", "start": 0, "end": 9_900, "confidence": 0.9, "text": "host words.", "words": [_word("host.", 0, "A")]},
        {"speaker": "A", "start": 10_000, "end": 19_900, "confidence": 0.9, "text": "guest words.", "words": [_word("guest.", 10, "A")]},
        {"speaker": "B", "start": 20_000, "end": 29_900, "confidence": 0.9, "text": "host again.", "words": [_word("again.", 20, "B")]},
    ]
}
LABELS = {
    "recording_id": "memo",
    "anchors": [{"ms": 0, "person": "host"}, {"ms": 10_000, "person": "guest"}, {"ms": 20_000, "person": "host"}],
    "expected_names": {"host": "alex rivera", "guest": "priya"},
}


def test_diarization_report_counts_a_label_that_holds_two_people() -> None:
    report = benchmark.diarization_report(LABELS, RESULT)

    assert report["labels"] == 2
    assert report["labels_holding_several_people"] == {"A": ["guest", "host"]}
    assert report["precision"] == 0.0 and report["recall"] == 0.0


def test_attribution_report_separates_wrong_names_from_unresolved_ones() -> None:
    transcript = "Alex Rivera: host words.\nAlex Rivera: guest words.\nSpeaker B: host again."

    report = benchmark.attribution_report(LABELS, RESULT, transcript)

    assert (report["correct"], report["wrong"], report["unresolved"]) == (1, 1, 1)
    assert report["wrong_examples"] == [{"person": "guest", "attributed": "Alex Rivera", "minute": 0}]
