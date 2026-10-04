"""Score voice-memo speaker diarization and enrichment attribution against hand labels.

The labels name real people, so they live outside this public repository
(default ~/.config/pdw/voice-benchmark/<name>.json):

    {
      "recording_id": "...",
      "anchors": [{"ms": 65000, "person": "host"}, ...],
      "expected_names": {"host": "first last", "guest": "first", "unnamed": null}
    }

An anchor is a moment whose true speaker is certain. ``expected_names`` maps each
anchor person to the lower-case prefix a correct attributed name starts with, or
null when the person's name is unknown (any name that is not another known
person's then counts as correct).

    diarization <labels.json> <assemblyai_result.json>...
        Pairwise score: two anchors should share a speaker label exactly when they
        are the same person. Also reports label count and how much audio sits in
        utterances over three minutes.

    attribution <labels.json> <assemblyai_result.json> <transcript.txt>
        The transcript is the locally assembled one (one "Name: text" line per
        stored segment). Counts anchors attributed correctly, attributed to the
        WRONG person (the number that matters -- a wrong name is worse than an
        unresolved one), or left unresolved.
"""
from __future__ import annotations

import argparse
import bisect
import itertools
import json
import statistics
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from personal_data_warehouse.apple_voice_memos_transcription import transcription_segment_rows

UNRESOLVED_MARKERS = ("speaker", "unresolved", "mixed", "unknown", "unidentified")


def load_result(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text())
    return data.get("result", data)


def word_label_at(words: list[dict[str, Any]], starts: list[int], ms: int) -> str | None:
    index = bisect.bisect_right(starts, ms) - 1
    best: tuple[int, str | None] | None = None
    for candidate in (index, index + 1):
        if 0 <= candidate < len(words):
            word = words[candidate]
            distance = 0 if word["start"] <= ms <= word["end"] else min(abs(word["start"] - ms), abs(word["end"] - ms))
            if best is None or distance < best[0]:
                best = (distance, word.get("speaker"))
    return best[1] if best and best[0] <= 3000 else None


def pairwise_scores(people_and_labels: list[tuple[str, str | None]]) -> dict[str, float]:
    tp = fp = fn = 0
    for (person_a, label_a), (person_b, label_b) in itertools.combinations(people_and_labels, 2):
        same_label = label_a is not None and label_a == label_b
        if person_a == person_b and same_label:
            tp += 1
        elif person_a == person_b:
            fn += 1
        elif same_label:
            fp += 1
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {"f1": f1, "precision": precision, "recall": recall}


def diarization_report(labels: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    utterances = result.get("utterances") or []
    words = sorted(
        result.get("words") or [word for utterance in utterances for word in utterance.get("words", [])],
        key=lambda word: word["start"],
    )
    starts = [word["start"] for word in words]
    durations = [(u["end"] - u["start"]) / 1000 for u in utterances]
    people_and_labels = [(a["person"], word_label_at(words, starts, a["ms"])) for a in labels["anchors"]]
    merged: dict[str, set[str]] = {}
    for person, label in people_and_labels:
        merged.setdefault(str(label), set()).add(person)
    return {
        "labels": len({u["speaker"] for u in utterances}),
        "utterances": len(utterances),
        "median_utterance_s": statistics.median(durations) if durations else 0,
        "share_in_utterances_over_3min": sum(d for d in durations if d > 180) / (sum(durations) or 1),
        **pairwise_scores(people_and_labels),
        "labels_holding_several_people": {k: sorted(v) for k, v in merged.items() if len(v) > 1},
    }


def attribution_report(labels: dict[str, Any], result: dict[str, Any], transcript: str) -> dict[str, Any]:
    segments = transcription_segment_rows({"recording_id": labels["recording_id"]}, result, created_at=datetime.now(tz=UTC))
    lines = transcript.splitlines()
    if len(lines) != len(segments):
        raise SystemExit(f"transcript has {len(lines)} lines for {len(segments)} segments; score the locally assembled transcript")
    names = [line.split(":", 1)[0].strip() for line in lines]
    expected_names: dict[str, str | None] = labels["expected_names"]
    counts = {"correct": 0, "wrong": 0, "unresolved": 0}
    wrong: list[dict[str, Any]] = []
    for anchor in labels["anchors"]:
        ms = anchor["ms"]
        index = min(
            range(len(segments)),
            key=lambda i: 0
            if segments[i]["start_ms"] <= ms <= segments[i]["end_ms"]
            else min(abs(segments[i]["start_ms"] - ms), abs(segments[i]["end_ms"] - ms)),
        )
        name = names[index].lower()
        expected = expected_names.get(anchor["person"])
        if any(marker in name for marker in UNRESOLVED_MARKERS):
            counts["unresolved"] += 1
            continue
        if expected is None:
            others = [prefix for person, prefix in expected_names.items() if prefix and person != anchor["person"]]
            ok = not any(name.startswith(prefix) for prefix in others)
        else:
            ok = name.startswith(expected)
        if ok:
            counts["correct"] += 1
        else:
            counts["wrong"] += 1
            wrong.append({"person": anchor["person"], "attributed": names[index], "minute": ms // 60000})
    total = len(labels["anchors"]) or 1
    return {
        **counts,
        "correct_share": counts["correct"] / total,
        "wrong_share": counts["wrong"] / total,
        "distinct_names": len(set(names)),
        "wrong_examples": wrong[:20],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    diarization = sub.add_parser("diarization")
    diarization.add_argument("labels", type=Path)
    diarization.add_argument("results", type=Path, nargs="+")
    attribution = sub.add_parser("attribution")
    attribution.add_argument("labels", type=Path)
    attribution.add_argument("result", type=Path)
    attribution.add_argument("transcript", type=Path)
    args = parser.parse_args()
    labels = json.loads(args.labels.read_text())
    if args.command == "diarization":
        for path in args.results:
            print(path.name, json.dumps(diarization_report(labels, load_result(path)), sort_keys=True))
    else:
        print(json.dumps(attribution_report(labels, load_result(args.result), args.transcript.read_text()), indent=2))


if __name__ == "__main__":
    main()
