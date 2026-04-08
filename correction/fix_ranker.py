"""Rank deterministic fix suggestions for spreadsheet review."""
from __future__ import annotations

from typing import Iterable, List, Mapping

from correction.schemas import FixCandidate, IssueRecord

STRATEGY_BONUS = {
    "clamp_to_limit": 0.05,
    "copy_existing_value": 0.06,
    "derive_from_related_field": 0.05,
    "align_related_fields": 0.04,
    "restore_missing_supporting_field": 0.04,
    "rebalance_component": 0.02,
    "clear_incorrect_flag": -0.02,
    "clear_suspicious_value": -0.04,
    "manual_review_required": -0.25,
}


def rank_fix_candidates(
    issue: IssueRecord,
    fix_candidates: Iterable[FixCandidate],
    feedback_weights: Mapping[str, float] | None = None,
) -> List[FixCandidate]:
    feedback_weights = dict(feedback_weights or {})
    scored: List[tuple[float, FixCandidate]] = []

    for candidate in fix_candidates:
        score = float(candidate.confidence)
        score += STRATEGY_BONUS.get(candidate.fix_strategy, 0.0)
        feedback_bonus = float(feedback_weights.get(candidate.fix_strategy, 0.0))
        score += feedback_bonus

        if issue.severity == "error" and candidate.fix_strategy != "manual_review_required":
            score += 0.02
        if issue.condition_type == "missing_field" and candidate.fix_strategy == "copy_existing_value":
            score += 0.03
        scored.append((score, candidate))

    ranked = [candidate for _, candidate in sorted(scored, key=lambda item: item[0], reverse=True)]
    for index, candidate in enumerate(ranked, start=1):
        feedback_bonus = float(feedback_weights.get(candidate.fix_strategy, 0.0))
        feedback_text = ""
        if feedback_bonus:
            direction = "boosted" if feedback_bonus > 0 else "down-ranked"
            feedback_text = f" Reviewer feedback {direction} this strategy by {abs(feedback_bonus):.2f}."
        candidate.rank = index
        candidate.ranking_reason = (
            f"Ranked #{index} from base confidence {candidate.confidence:.2f} "
            f"and strategy `{candidate.fix_strategy}`.{feedback_text}"
        )
    return ranked
