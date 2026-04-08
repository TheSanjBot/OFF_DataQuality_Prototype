"""Persist lightweight reviewer feedback for future fix ranking."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Mapping

from correction.schemas import IssueRecord, ReviewDecision

FeedbackStats = Dict[str, int]
FeedbackStore = Dict[str, object]


def empty_feedback_store() -> FeedbackStore:
    return {
        "global": {},
        "by_rule": {},
        "decision_count": 0,
        "last_updated": "",
    }


def load_feedback_store(path: Path) -> FeedbackStore:
    if not path.exists():
        return empty_feedback_store()
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        return empty_feedback_store()
    store = empty_feedback_store()
    store["global"] = dict(payload.get("global", {}))
    store["by_rule"] = dict(payload.get("by_rule", {}))
    store["decision_count"] = int(payload.get("decision_count", 0))
    store["last_updated"] = str(payload.get("last_updated", ""))
    return store


def save_feedback_store(store: Mapping[str, object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(store, indent=2), encoding="utf-8")


def update_feedback_store(
    store: Mapping[str, object] | None,
    decisions: Iterable[ReviewDecision],
    issues_by_id: Mapping[str, IssueRecord],
) -> FeedbackStore:
    updated = empty_feedback_store()
    if store:
        updated["global"] = json.loads(json.dumps(store.get("global", {})))
        updated["by_rule"] = json.loads(json.dumps(store.get("by_rule", {})))
        updated["decision_count"] = int(store.get("decision_count", 0))
        updated["last_updated"] = str(store.get("last_updated", ""))

    decision_count = int(updated.get("decision_count", 0))
    for decision in decisions:
        issue = issues_by_id.get(decision.issue_id)
        if issue is None:
            continue
        decision_count += 1

        if decision.selected_fix_id:
            selected_fix = next((fix for fix in issue.fix_candidates if fix.fix_id == decision.selected_fix_id), None)
            if selected_fix is not None:
                outcome = "accepted" if decision.user_action == "approve" else "rejected"
                _increment_strategy(updated, issue.rule_name, selected_fix.fix_strategy, outcome)
                if decision.user_action == "approve":
                    for candidate in issue.fix_candidates:
                        if candidate.fix_id == selected_fix.fix_id:
                            continue
                        if candidate.fix_strategy == selected_fix.fix_strategy:
                            continue
                        _increment_strategy(updated, issue.rule_name, candidate.fix_strategy, "rejected")

        if decision.manual_changes:
            _increment_strategy(updated, issue.rule_name, "__manual_edit__", "accepted")

    updated["decision_count"] = decision_count
    updated["last_updated"] = datetime.now(timezone.utc).isoformat()
    return updated


def feedback_summary(store: Mapping[str, object]) -> Dict[str, object]:
    global_stats = store.get("global", {})
    top_strategies = sorted(
        (
            (
                strategy,
                int(stats.get("accepted", 0)),
                int(stats.get("rejected", 0)),
            )
            for strategy, stats in global_stats.items()
            if strategy != "__manual_edit__" and isinstance(stats, Mapping)
        ),
        key=lambda item: (item[1] - item[2], item[1]),
        reverse=True,
    )
    return {
        "decision_count": int(store.get("decision_count", 0)),
        "tracked_rules": len(store.get("by_rule", {})),
        "tracked_strategies": len(global_stats),
        "top_strategies": [
            {
                "fix_strategy": strategy,
                "accepted": accepted,
                "rejected": rejected,
            }
            for strategy, accepted, rejected in top_strategies[:5]
        ],
    }


def _increment_strategy(store: FeedbackStore, rule_name: str, strategy: str, outcome: str) -> None:
    global_stats = _ensure_stats(store.setdefault("global", {}), strategy)
    rule_bucket = store.setdefault("by_rule", {})
    if not isinstance(rule_bucket, dict):
        rule_bucket = {}
        store["by_rule"] = rule_bucket
    per_rule_stats = rule_bucket.setdefault(rule_name, {})
    if not isinstance(per_rule_stats, dict):
        per_rule_stats = {}
        rule_bucket[rule_name] = per_rule_stats
    rule_stats = _ensure_stats(per_rule_stats, strategy)

    global_stats[outcome] += 1
    rule_stats[outcome] += 1


def _ensure_stats(bucket: Dict[str, object], strategy: str) -> FeedbackStats:
    stats = bucket.get(strategy)
    if not isinstance(stats, Mapping):
        stats = {"accepted": 0, "rejected": 0}
        bucket[strategy] = stats
    accepted = int(stats.get("accepted", 0))
    rejected = int(stats.get("rejected", 0))
    normalized = {"accepted": accepted, "rejected": rejected}
    bucket[strategy] = normalized
    return normalized
