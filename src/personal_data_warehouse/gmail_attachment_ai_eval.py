from __future__ import annotations

from collections.abc import Iterable, Mapping
import re
from typing import Any


GENERIC_PLACEHOLDER_TERMS = [
    "generic gmail attachment",
    "gmail attachment placeholder",
    "blank attachment",
    "no readable text",
    "no discernible content",
]


PRODUCTION_RUBRIC_CASES: list[dict[str, Any]] = [
    {
        "case_id": 1,
        "filename": "ATT00001.png",
        "description": "Common Sense Media logo.",
        "required_terms": ["common sense media"],
        "forbidden_terms": GENERIC_PLACEHOLDER_TERMS,
    },
    {
        "case_id": 2,
        "filename": "image.png",
        "description": "UC Berkeley Cybersecurity Clinic quick facts slide.",
        "required_terms": [
            "UC Berkeley Cybersecurity Clinic",
            "Quick Facts",
            "300 students",
            "cyberdefense",
            "50+ clients",
            "four continents",
            "5 years",
            "Consortium of Cybersecurity Clinics",
            "10x growth",
            "2200 students",
            "700 clients",
            "2024-25",
        ],
        "forbidden_terms": GENERIC_PLACEHOLDER_TERMS,
    },
    {
        "case_id": 3,
        "filename": "gekOqYRbWn4F_web_p95.png",
        "description": "Weekly response times chart.",
        "required_terms": ["WEEKLY RESPONSE TIMES", "200ms", "100ms", "83ms last week", "255ms this week"],
        "forbidden_terms": GENERIC_PLACEHOLDER_TERMS,
    },
    {
        "case_id": 4,
        "filename": "1.png",
        "description": "Fast Company / Girls Who Code graphic.",
        "required_terms": [
            "FAST COMPANY",
            "Why AI's Flaws Are Hurting Girls Most",
            "Dr. Tarika Barrett",
            "girls who code",
        ],
        "forbidden_terms": GENERIC_PLACEHOLDER_TERMS + ["FlawsAreMorty"],
    },
    {
        "case_id": 5,
        "filename": "ATT00001.png",
        "description": "Common Sense Media logo duplicate.",
        "required_terms": ["common sense media"],
        "forbidden_terms": GENERIC_PLACEHOLDER_TERMS,
    },
    {
        "case_id": 6,
        "filename": "image001.png",
        "description": "NPower / DoD SkillBridge congratulations graphic.",
        "required_terms": ["npower", "DoD SkillBridge", "CONGRATULATIONS", "SPRING 2026 COHORT"],
        "forbidden_terms": GENERIC_PLACEHOLDER_TERMS + ["DOWSKILLBRIDGE"],
    },
    {
        "case_id": 7,
        "filename": "Screenshot 2026-02-27 at 4.36.22 PM.png",
        "description": "Fast Company / Girls Who Code quote graphic.",
        "required_terms": [
            "FAST COMPANY",
            "Why AI's Flaws Are Hurting Girls Most",
            "Dr. Tarika Barrett",
            "women's perspectives",
            "replicate existing inequities",
            "AI is failing girls and women",
            "CEO, GIRLS WHO CODE",
        ],
        "forbidden_terms": GENERIC_PLACEHOLDER_TERMS + ["Al's systems"],
    },
    {
        "case_id": 8,
        "filename": "noname",
        "description": "CLTC Center for Long-Term Cybersecurity UC Berkeley logo.",
        "required_terms": ["CLTC", "Center for Long-Term Cybersecurity", "UC Berkeley"],
        "forbidden_terms": GENERIC_PLACEHOLDER_TERMS,
    },
    {
        "case_id": 9,
        "filename": "image002.png",
        "description": "NPower ad/banner.",
        "required_terms": ["Launching tech careers", "Transforming lives", "npower", "LEARN MORE"],
        "forbidden_terms": GENERIC_PLACEHOLDER_TERMS + ["launching teachers", "learning teachers", "Gmail interface"],
    },
    {
        "case_id": 10,
        "filename": "1.png",
        "description": "Fast Company / Girls Who Code graphic duplicate.",
        "required_terms": [
            "FAST COMPANY",
            "Why AI's Flaws Are Hurting Girls Most",
            "Dr. Tarika Barrett",
            "girls who code",
        ],
        "forbidden_terms": GENERIC_PLACEHOLDER_TERMS + ["FlawsAreMorty"],
    },
]


def score_attachment_ai_text(
    text: str,
    *,
    required_terms: Iterable[str],
    forbidden_terms: Iterable[str] = (),
) -> dict[str, Any]:
    normalized = normalize_for_eval(text)
    required = [term for term in required_terms if term.strip()]
    forbidden = [term for term in forbidden_terms if term.strip()]
    matched = [term for term in required if normalize_for_eval(term) in normalized]
    missing = [term for term in required if term not in matched]
    forbidden_matches = [term for term in forbidden if normalize_for_eval(term) in normalized]

    if not required:
        score = 0
    else:
        score = round(10 * len(matched) / len(required), 1)
    if forbidden_matches:
        score = min(score, 6.0)
        score = max(0.0, score - len(forbidden_matches))

    return {
        "score": score,
        "matched_terms": matched,
        "missing_terms": missing,
        "forbidden_matches": forbidden_matches,
    }


def score_attachment_ai_results(
    results: Iterable[Mapping[str, Any]],
    cases: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    cases_by_id = {int(case["case_id"]): case for case in cases}
    scored_results = []
    for result in results:
        case_id = int(result.get("case_id") or result.get("sample_id") or 0)
        case = cases_by_id.get(case_id)
        if case is None:
            continue
        text = str(result.get("formatted_text") or result.get("text") or result.get("raw_response") or "")
        score = score_attachment_ai_text(
            text,
            required_terms=case.get("required_terms", []),
            forbidden_terms=case.get("forbidden_terms", []),
        )
        scored_results.append({"case_id": case_id, "filename": case.get("filename", ""), **score})

    average = 0.0
    if scored_results:
        average = round(sum(float(result["score"]) for result in scored_results) / len(scored_results), 2)
    return {"average_score": average, "results": scored_results}


def normalize_for_eval(value: str) -> str:
    value = value.lower()
    value = value.replace("’", "'")
    value = re.sub(r"[^a-z0-9+.'-]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()
