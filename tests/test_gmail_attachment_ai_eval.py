from personal_data_warehouse.gmail_attachment_ai_eval import (
    PRODUCTION_RUBRIC_CASES,
    score_attachment_ai_results,
    score_attachment_ai_text,
)


def test_score_attachment_ai_text_rewards_required_terms() -> None:
    scored = score_attachment_ai_text(
        "Document type: logo\n\nVisible text:\ncommon sense media\n\nEntities: Common Sense Media",
        required_terms=["common sense media"],
        forbidden_terms=["generic Gmail attachment", "blank attachment"],
    )

    assert scored["score"] == 10
    assert scored["matched_terms"] == ["common sense media"]
    assert scored["missing_terms"] == []
    assert scored["forbidden_matches"] == []


def test_score_attachment_ai_text_penalizes_placeholder_language() -> None:
    scored = score_attachment_ai_text(
        "The image appears to be a generic Gmail attachment placeholder. Visible text: Gmail attachment",
        required_terms=["common sense media"],
        forbidden_terms=["generic Gmail attachment", "Gmail attachment placeholder"],
    )

    assert scored["score"] < 5
    assert "common sense media" in scored["missing_terms"]
    assert scored["forbidden_matches"]


def test_score_attachment_ai_results_uses_production_rubric_cases() -> None:
    scored = score_attachment_ai_results(
        [
            {
                "case_id": 8,
                "formatted_text": (
                    "Document type: logo\n\n"
                    "Visible text:\nCLTC\nCenter for Long-Term Cybersecurity\nUC Berkeley\n\n"
                    "Entities: CLTC, Center for Long-Term Cybersecurity, UC Berkeley"
                ),
            }
        ],
        PRODUCTION_RUBRIC_CASES,
    )

    assert scored["average_score"] == 10
    assert scored["results"][0]["case_id"] == 8
