"""Translate stored reviewer feedback into ranking weights."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, Mapping

from correction.schemas import IssueRecord


def build_feedback_weights(issue: IssueRecord, feedback_store: Mapping[str, object] | None) -> Dict[str, float]:
    if not feedback_store:
        return {}

    strategies = {candidate.fix_strategy for candidate in issue.fix_candidates}
    weights: Dict[str, float] = {}
    global_stats = feedback_store.get("global", {})
    by_rule = feedback_store.get("by_rule", {})
    rule_stats = by_rule.get(issue.rule_name, {}) if isinstance(by_rule, Mapping) else {}

    for strategy in strategies:
        global_weight = _stats_to_weight(global_stats.get(strategy), scale=0.10, min_reviews=2)
        rule_weight = _stats_to_weight(rule_stats.get(strategy), scale=0.24, min_reviews=1)
        total = max(min(global_weight + rule_weight, 0.22), -0.22)
        if total:
            weights[strategy] = round(total, 4)
    return weights


def write_ranking_weights_snapshot(
    issues: Iterable[IssueRecord],
    feedback_store: Mapping[str, object] | None,
    path: Path,
) -> None:
    rows = []
    for issue in issues:
        rows.append(
            {
                "issue_id": issue.issue_id,
                "rule_name": issue.rule_name,
                "weights": build_feedback_weights(issue, feedback_store),
            }
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, indent=2), encoding="utf-8")


def _stats_to_weight(
    raw_stats: Mapping[str, object] | None,
    *,
    scale: float,
    min_reviews: int,
) -> float:
    if not isinstance(raw_stats, Mapping):
        return 0.0
    accepted = int(raw_stats.get("accepted", 0))
    rejected = int(raw_stats.get("rejected", 0))
    reviewed = accepted + rejected
    if reviewed < min_reviews:
        return 0.0
    preference_gap = (accepted - rejected) / reviewed
    return preference_gap * scale
