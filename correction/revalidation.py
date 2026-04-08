"""Revalidate the corrected dataset and summarize quality improvement."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List, Mapping

from correction.issue_builder import build_issues_from_comparison, load_comparison_report
from validation.engine_comparison import run_engine_comparison


def _issues_by_rule(issues: Iterable[object]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for issue in issues:
        counts[str(issue.rule_name)] = counts.get(str(issue.rule_name), 0) + 1
    return counts


def _quality_score(issue_count: int, products_tested: int, rule_count: int) -> float:
    denominator = max(products_tested * max(rule_count, 1), 1)
    return max(0.0, 1.0 - (issue_count / denominator))


def build_revalidation_summary(
    before_report: Mapping[str, object],
    after_report: Mapping[str, object],
    before_issues: List[object],
    after_issues: List[object],
    corrected_dataset_path: Path,
    patches_path: Path,
    audit_log_path: Path,
) -> Dict[str, object]:
    before_issue_count = len(before_issues)
    after_issue_count = len(after_issues)
    resolved_issue_count = max(before_issue_count - after_issue_count, 0)
    introduced_issue_count = max(after_issue_count - before_issue_count, 0)

    products_tested = int(before_report.get("dataset", {}).get("products_tested", 0))
    rule_count = len(before_report.get("rule_comparison", []))
    quality_before = round(_quality_score(before_issue_count, products_tested, rule_count), 4)
    quality_after = round(_quality_score(after_issue_count, products_tested, rule_count), 4)

    before_by_rule = _issues_by_rule(before_issues)
    after_by_rule = _issues_by_rule(after_issues)
    all_rules = sorted(set(before_by_rule) | set(after_by_rule))
    per_rule_changes = []
    for rule_name in all_rules:
        before_count = before_by_rule.get(rule_name, 0)
        after_count = after_by_rule.get(rule_name, 0)
        delta = before_count - after_count
        per_rule_changes.append(
            {
                "rule_name": rule_name,
                "before_count": before_count,
                "after_count": after_count,
                "resolved_count": max(delta, 0),
                "introduced_count": max(-delta, 0),
                "delta": delta,
            }
        )
    per_rule_changes.sort(key=lambda row: (row["resolved_count"], -row["introduced_count"], row["rule_name"]), reverse=True)

    return {
        "corrected_dataset_path": str(corrected_dataset_path),
        "patches_path": str(patches_path),
        "audit_log_path": str(audit_log_path),
        "before_issue_count": before_issue_count,
        "after_issue_count": after_issue_count,
        "resolved_issue_count": resolved_issue_count,
        "introduced_issue_count": introduced_issue_count,
        "issue_resolution_rate": round((resolved_issue_count / before_issue_count), 4) if before_issue_count else 0.0,
        "quality_score_before": quality_before,
        "quality_score_after": quality_after,
        "quality_score_delta": round(quality_after - quality_before, 4),
        "before_rules_with_issues": len(before_by_rule),
        "after_rules_with_issues": len(after_by_rule),
        "before_passed_rules": int(sum(1 for row in before_report.get("rule_comparison", []) if row.get("migration_state") == "accepted")),
        "after_passed_rules": int(sum(1 for row in after_report.get("rule_comparison", []) if row.get("migration_state") == "accepted")),
        "before_comparison_run_id": str(before_report.get("comparison_fingerprint", {}).get("comparison_run_id", "")),
        "after_comparison_run_id": str(after_report.get("comparison_fingerprint", {}).get("comparison_run_id", "")),
        "per_rule_changes": per_rule_changes,
    }


def write_revalidation_summary(summary: Mapping[str, object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2), encoding="utf-8")


def run_revalidation(
    comparison_path: Path,
    corrected_dataset_path: Path,
    revalidated_comparison_path: Path,
    revalidation_summary_path: Path,
    revalidation_db_path: Path,
    patches_path: Path,
    audit_log_path: Path,
) -> Dict[str, object]:
    before_report = load_comparison_report(comparison_path)
    before_db_path_text = str(before_report.get("dataset", {}).get("duckdb_path", "")).strip()
    before_issues = build_issues_from_comparison(
        comparison_report=before_report,
        db_path=Path(before_db_path_text) if before_db_path_text else None,
        max_issues_per_rule=None,
    )

    run_config = dict(before_report.get("run_config", {}))
    perl_rules_source = str(before_report.get("dataset", {}).get("perl_rules_source", "")).strip()
    perl_rules_dir = None if perl_rules_source in {"", "inline_legacy_rules"} else Path(perl_rules_source)

    after_report = run_engine_comparison(
        dataset_size=int(before_report.get("dataset", {}).get("products_tested", 0) or 0),
        seed=int(run_config.get("seed", 17) or 17),
        source_jsonl=corrected_dataset_path,
        use_default_off_source=True,
        llm_provider=str(run_config.get("llm_provider", "groq")),
        llm_model=run_config.get("llm_model") or None,
        perl_rules_dir=perl_rules_dir,
        db_path=revalidation_db_path,
        results_path=revalidated_comparison_path,
        require_real_llm=bool(run_config.get("require_real_llm", False)),
        profile=str(run_config.get("profile", "global")),
        soda_mode=str(run_config.get("soda_mode", "local")),
    )
    after_issues = build_issues_from_comparison(
        comparison_report=after_report,
        db_path=revalidation_db_path,
        max_issues_per_rule=None,
    )

    summary = build_revalidation_summary(
        before_report=before_report,
        after_report=after_report,
        before_issues=before_issues,
        after_issues=after_issues,
        corrected_dataset_path=corrected_dataset_path,
        patches_path=patches_path,
        audit_log_path=audit_log_path,
    )
    write_revalidation_summary(summary, revalidation_summary_path)
    return summary
