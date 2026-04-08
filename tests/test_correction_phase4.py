from correction.fix_ranker import rank_fix_candidates
from correction.schemas import FixCandidate, IssueRecord, ReviewDecision
from learning.feedback_store import empty_feedback_store, update_feedback_store
from learning.ranking_updater import build_feedback_weights


def _issue_with_candidates() -> IssueRecord:
    issue = IssueRecord(
        issue_id="issue_1",
        product_id="123",
        rule_name="carbohydrates_over_105g",
        tag="carbohydrates",
        severity="error",
        jurisdiction="global",
        condition="carbohydrates > 105",
        condition_type="field_threshold",
        issue_description="Carbohydrates exceed the allowed threshold.",
        rule_explanation="Carbohydrates should not be above 105g per 100g.",
        current_values={"carbohydrates": 114.8},
        current_values_display="carbohydrates=114.8",
        confidence=0.91,
        migration_state="accepted",
        migration_state_reason="High parity and stable migration.",
        best_engine="soda",
        rule_ir_hash="abc123",
    )
    issue.fix_candidates = [
        FixCandidate(
            fix_id="issue_1_limit",
            suggested_change="Set carbohydrates to 105.",
            target_field="carbohydrates",
            new_value=105.0,
            confidence=0.88,
            explanation="Clamp to the supported maximum.",
            expected_outcome="The row stops violating the threshold rule.",
            fix_strategy="clamp_to_limit",
        ),
        FixCandidate(
            fix_id="issue_1_clear",
            suggested_change="Clear carbohydrates and review the label.",
            target_field="carbohydrates",
            new_value=None,
            confidence=0.58,
            explanation="Use this if the recorded value is obviously wrong.",
            expected_outcome="The suspicious value is removed pending review.",
            fix_strategy="clear_suspicious_value",
        ),
        FixCandidate(
            fix_id="issue_1_manual",
            suggested_change="Keep for manual review.",
            target_field="",
            new_value="",
            confidence=0.35,
            explanation="Use when none of the quick fixes can be trusted.",
            expected_outcome="The issue remains open for manual resolution.",
            fix_strategy="manual_review_required",
        ),
    ]
    return issue


def test_feedback_store_records_selected_strategy_and_competing_rejections() -> None:
    issue = _issue_with_candidates()
    decision = ReviewDecision(
        issue_id=issue.issue_id,
        product_id=issue.product_id,
        rule_name=issue.rule_name,
        user_action="approve",
        selected_fix_id="issue_1_clear",
        manual_changes={},
        review_notes="Prefer clearing suspicious carbohydrate values.",
        applied_changes={"carbohydrates": None},
        source_fix_ids=["issue_1_clear"],
    )

    store = update_feedback_store(empty_feedback_store(), [decision], {issue.issue_id: issue})

    assert store["global"]["clear_suspicious_value"]["accepted"] == 1
    assert store["by_rule"][issue.rule_name]["clear_suspicious_value"]["accepted"] == 1
    assert store["by_rule"][issue.rule_name]["clamp_to_limit"]["rejected"] == 1


def test_feedback_weights_can_reorder_future_fix_ranking() -> None:
    issue = _issue_with_candidates()
    decision = ReviewDecision(
        issue_id=issue.issue_id,
        product_id=issue.product_id,
        rule_name=issue.rule_name,
        user_action="approve",
        selected_fix_id="issue_1_clear",
        manual_changes={},
        review_notes="Prefer clearing suspicious carbohydrate values.",
        applied_changes={"carbohydrates": None},
        source_fix_ids=["issue_1_clear"],
    )
    store = update_feedback_store(empty_feedback_store(), [decision], {issue.issue_id: issue})

    weights = build_feedback_weights(issue, store)
    ranked = rank_fix_candidates(issue, issue.fix_candidates, feedback_weights=weights)

    assert ranked[0].fix_strategy == "clear_suspicious_value"
    assert weights["clear_suspicious_value"] > 0
    assert weights["clamp_to_limit"] < 0
    assert "Reviewer feedback boosted" in ranked[0].ranking_reason
