"""The private fusion lab must faithfully reproduce the production ranker."""

from __future__ import annotations

from personal_data_warehouse.postgres import (
    SEARCH_HYBRID_EXACT_WEIGHT,
    SEARCH_HYBRID_LEXICAL_HEAD_RANKS,
    SEARCH_HYBRID_LEXICAL_HEAD_WEIGHT,
    SEARCH_HYBRID_SEMANTIC_WEIGHT,
)
from personal_data_warehouse.search_benchmark import BenchmarkCase
from scripts import search_fusion_lab as lab


def test_production_fusion_config_is_derived_from_search_constants() -> None:
    assert lab.production_fusion_config() == {
        "w_lex": 1.0,
        "w_sem": SEARCH_HYBRID_SEMANTIC_WEIGHT,
        "w_exact": SEARCH_HYBRID_EXACT_WEIGHT,
        "lex_top": (SEARCH_HYBRID_LEXICAL_HEAD_RANKS, SEARCH_HYBRID_LEXICAL_HEAD_WEIGHT),
    }


def test_collection_preserves_source_and_time_scope(monkeypatch) -> None:
    sql_calls: list[str] = []
    monkeypatch.setattr(lab, "embed", lambda forms: [[0.0] * 512 for _ in forms])

    def fake_sql(_question: str, sql: str):
        sql_calls.append(sql)
        return []

    monkeypatch.setattr(lab, "pdw_sql", fake_sql)
    case = BenchmarkCase(
        query="offer letter", stratum="identifier", verdict="FOUND",
        truth_refs=("gmail_email:x",), truth_predicate=None, ambiguous=False, note="",
        sources=("gmail",), since="2026-08-01",
    )
    evidence = lab.collect_case(case, "model", "prefix: ")

    assert len(sql_calls) == 3  # BM25 + literal + one instructed ANN leg
    assert evidence["collection_version"] == lab.COLLECTION_VERSION
    assert "ARRAY['gmail']::text[]" in sql_calls[0]
    assert "'2026-08-01'::timestamptz" in sql_calls[0]
    assert "ARRAY['gmail']::text[]" in sql_calls[1]


def test_semantic_pool_limit_filters_each_ann_leg_before_fusion() -> None:
    evidence = {
        "query": "what was the final delivery decision",
        "lexical": [], "exact": [],
        "semantic": [
            [("near", 2, 1.0), ("deep", 400, 2.0)],
            [("near", 3, 1.0), ("deep", 450, 2.0)],
        ],
    }
    assert lab.fuse(evidence, sem_pool=300)[0] == "near"


def test_evidence_cache_key_includes_search_scope() -> None:
    base = dict(
        query="offer letter", stratum="identifier", verdict="FOUND",
        truth_refs=("gmail_email:x",), truth_predicate=None, ambiguous=False, note="",
    )
    unscoped = BenchmarkCase(**base)
    scoped = BenchmarkCase(**base, sources=("gmail",), since="2026-08-01")
    assert lab.case_key(unscoped) != lab.case_key(scoped)
