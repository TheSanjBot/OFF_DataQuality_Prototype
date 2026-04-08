"""Run the correction workflow on top of existing comparison output."""
from __future__ import annotations

import argparse
import csv
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from audit.audit_logger import build_audit_entries, write_audit_log
from correction.correction_applier import apply_review_decisions
from correction.correction_export import REVIEW_SHEET_COLUMNS, export_review_sheet, write_issues_json, write_review_instructions, write_review_manifest
from correction.correction_import import import_review_decisions, load_issue_records, validate_review_sheet
from correction.fix_generator import generate_fix_candidates
from correction.fix_ranker import rank_fix_candidates
from correction.issue_builder import build_issues_from_comparison, load_comparison_report
from correction.revalidation import run_revalidation
from correction.review_status import (
    ISSUE_STATUS_APPLIED,
    ISSUE_STATUS_APPROVED,
    ISSUE_STATUS_PENDING_REVIEW,
    ISSUE_STATUS_REVALIDATED_REMAINING,
    ISSUE_STATUS_REVALIDATED_RESOLVED,
    apply_status_updates,
)
from correction.schemas import PatchRecord
from learning.feedback_store import feedback_summary, load_feedback_store, save_feedback_store, update_feedback_store
from learning.ranking_updater import build_feedback_weights, write_ranking_weights_snapshot
from validation.engine_comparison import COMPARISON_PATH

ISSUES_PATH = Path(__file__).resolve().parent.parent / "results" / "issues.json"
REVIEW_SHEET_PATH = Path(__file__).resolve().parent.parent / "results" / "review_sheet.csv"
REVIEWED_SHEET_PATH = Path(__file__).resolve().parent.parent / "results" / "reviewed_sheet.csv"
CORRECTED_DATASET_PATH = Path(__file__).resolve().parent.parent / "results" / "corrected_dataset.jsonl"
PATCHES_PATH = Path(__file__).resolve().parent.parent / "results" / "correction_patches.jsonl"
AUDIT_LOG_PATH = Path(__file__).resolve().parent.parent / "results" / "audit_log.jsonl"
REVALIDATED_COMPARISON_PATH = Path(__file__).resolve().parent.parent / "results" / "revalidated_engine_comparison.json"
REVALIDATION_SUMMARY_PATH = Path(__file__).resolve().parent.parent / "results" / "revalidation_summary.json"
REVALIDATION_DB_PATH = Path(__file__).resolve().parent.parent / "results" / "revalidation_off_quality.db"
SOURCE_SNAPSHOT_PATH = Path(__file__).resolve().parent.parent / "results" / "review_source_dataset.jsonl"
FEEDBACK_STORE_PATH = Path(__file__).resolve().parent.parent / "results" / "feedback_store.json"
RANKING_WEIGHTS_PATH = Path(__file__).resolve().parent.parent / "results" / "ranking_weights.json"
APPLY_SUMMARY_PATH = Path(__file__).resolve().parent.parent / "results" / "apply_summary.json"
REVIEW_INSTRUCTIONS_PATH = Path(__file__).resolve().parent.parent / "results" / "review_instructions.md"
REVIEW_MANIFEST_PATH = Path(__file__).resolve().parent.parent / "results" / "review_manifest.json"
REVIEW_VALIDATION_SUMMARY_PATH = Path(__file__).resolve().parent.parent / "results" / "review_validation_summary.json"


def _load_review_sheet_rows(path: Path) -> tuple[List[str], List[Dict[str, object]]]:
    if not path.exists():
        return list(REVIEW_SHEET_COLUMNS), []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or REVIEW_SHEET_COLUMNS)
        rows = [dict(row) for row in reader]
    return fieldnames, rows


def _write_review_sheet_rows(path: Path, fieldnames: List[str], rows: List[Dict[str, object]]) -> None:
    final_fieldnames = list(REVIEW_SHEET_COLUMNS)
    for fieldname in fieldnames:
        if fieldname not in final_fieldnames:
            final_fieldnames.append(fieldname)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=final_fieldnames)
        writer.writeheader()
        for row in rows:
            normalized = {field: row.get(field, "") for field in final_fieldnames}
            writer.writerow(normalized)


def _update_review_sheet_statuses(path: Path, status_by_issue_id: Dict[str, str]) -> None:
    fieldnames, rows = _load_review_sheet_rows(path)
    updated_rows = apply_status_updates(rows, status_by_issue_id)
    _write_review_sheet_rows(path, fieldnames, updated_rows)


def _resolve_stateful_review_sheet_path(path: Path) -> Path:
    if path == REVIEW_SHEET_PATH and REVIEWED_SHEET_PATH.exists():
        return REVIEWED_SHEET_PATH
    return path


def run_correction_phase_one(
    comparison_path: Path = COMPARISON_PATH,
    issues_path: Path = ISSUES_PATH,
    review_sheet_path: Path = REVIEW_SHEET_PATH,
    source_snapshot_path: Path = SOURCE_SNAPSHOT_PATH,
    feedback_store_path: Path = FEEDBACK_STORE_PATH,
    ranking_weights_path: Path = RANKING_WEIGHTS_PATH,
    review_instructions_path: Path = REVIEW_INSTRUCTIONS_PATH,
    review_manifest_path: Path = REVIEW_MANIFEST_PATH,
    max_issues_per_rule: int | None = None,
) -> Dict[str, object]:
    report = load_comparison_report(comparison_path)
    report_db_path = str(report.get("dataset", {}).get("duckdb_path", "")).strip()
    report_source_jsonl = str(report.get("dataset", {}).get("jsonl_path", "")).strip()
    issues = build_issues_from_comparison(
        comparison_report=report,
        db_path=Path(report_db_path) if report_db_path else None,
        max_issues_per_rule=max_issues_per_rule,
    )
    feedback_store = load_feedback_store(feedback_store_path)

    enriched_issues = []
    for issue in issues:
        generated = generate_fix_candidates(issue)
        issue.fix_candidates = list(generated)
        feedback_weights = build_feedback_weights(issue, feedback_store)
        ranked = rank_fix_candidates(issue, generated, feedback_weights=feedback_weights)
        enriched_issues.append(issue.with_fixes(ranked))

    write_issues_json(enriched_issues, issues_path)
    export_rows = export_review_sheet(enriched_issues, review_sheet_path)
    write_ranking_weights_snapshot(enriched_issues, feedback_store, ranking_weights_path)
    if report_source_jsonl:
        source_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(report_source_jsonl, source_snapshot_path)
    write_review_instructions(review_instructions_path, review_sheet_path.name)
    write_review_manifest(
        review_manifest_path,
        review_sheet_path=review_sheet_path,
        issues_path=issues_path,
        source_snapshot_path=source_snapshot_path,
        comparison_report=report,
        issue_count=len(enriched_issues),
        rules_with_issues=len({issue.rule_name for issue in enriched_issues}),
    )

    summary = {
        "issues_generated": len(enriched_issues),
        "rules_with_issues": len({issue.rule_name for issue in enriched_issues}),
        "issues_path": str(issues_path),
        "review_sheet_path": str(review_sheet_path),
        "source_snapshot_path": str(source_snapshot_path) if report_source_jsonl else "",
        "feedback_store_path": str(feedback_store_path),
        "ranking_weights_path": str(ranking_weights_path),
        "review_instructions_path": str(review_instructions_path),
        "review_manifest_path": str(review_manifest_path),
        "feedback_summary": feedback_summary(feedback_store),
        "top_rules": _top_rules(enriched_issues),
        "review_rows": len(export_rows),
    }
    return summary


def run_correction_phase_two(
    comparison_path: Path = COMPARISON_PATH,
    issues_path: Path = ISSUES_PATH,
    reviewed_sheet_path: Path = REVIEW_SHEET_PATH,
    corrected_dataset_path: Path = CORRECTED_DATASET_PATH,
    patches_path: Path = PATCHES_PATH,
    audit_log_path: Path = AUDIT_LOG_PATH,
    source_snapshot_path: Path = SOURCE_SNAPSHOT_PATH,
    feedback_store_path: Path = FEEDBACK_STORE_PATH,
    apply_summary_path: Path = APPLY_SUMMARY_PATH,
    review_validation_summary_path: Path = REVIEW_VALIDATION_SUMMARY_PATH,
    source_jsonl_path: Path | None = None,
) -> Dict[str, object]:
    report = load_comparison_report(comparison_path)
    report_source_jsonl = str(report.get("dataset", {}).get("jsonl_path", "")).strip()
    snapshot_candidate = source_snapshot_path if source_snapshot_path.exists() else None
    resolved_source_jsonl = source_jsonl_path or snapshot_candidate or (Path(report_source_jsonl) if report_source_jsonl else None)
    if resolved_source_jsonl is None:
        raise ValueError("A source_jsonl_path is required for correction application.")

    validation_summary = validate_review_sheet(reviewed_sheet_path, issues_path)
    review_validation_summary_path.parent.mkdir(parents=True, exist_ok=True)
    review_validation_summary_path.write_text(json.dumps(validation_summary, indent=2), encoding="utf-8")
    if not validation_summary["valid"]:
        preview_errors = "; ".join(validation_summary["errors"][:3])
        raise ValueError(f"Reviewed sheet validation failed: {preview_errors}")

    decisions = import_review_decisions(reviewed_sheet_path, issues_path)
    apply_summary = apply_review_decisions(
        source_jsonl_path=resolved_source_jsonl,
        decisions=decisions,
        corrected_dataset_path=corrected_dataset_path,
        patches_path=patches_path,
    )

    patches = [PatchRecord(**patch) for patch in apply_summary.pop("patches", [])]
    audit_entries = build_audit_entries(patches)
    write_audit_log(audit_entries, audit_log_path)
    issues_by_id = load_issue_records(issues_path)
    store = load_feedback_store(feedback_store_path)
    updated_store = update_feedback_store(store, decisions, issues_by_id)
    save_feedback_store(updated_store, feedback_store_path)
    applied_issue_ids = {
        decision.issue_id
        for decision in decisions
        if decision.issue_status == ISSUE_STATUS_APPROVED
    }
    if applied_issue_ids:
        _update_review_sheet_statuses(
            reviewed_sheet_path,
            {issue_id: ISSUE_STATUS_APPLIED for issue_id in applied_issue_ids},
        )

    summary = {
        **apply_summary,
        "reviewed_sheet_path": str(reviewed_sheet_path),
        "issues_path": str(issues_path),
        "audit_log_path": str(audit_log_path),
        "source_snapshot_path": str(source_snapshot_path) if source_snapshot_path.exists() else "",
        "feedback_store_path": str(feedback_store_path),
        "review_validation_summary_path": str(review_validation_summary_path),
        "feedback_summary": feedback_summary(updated_store),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "apply_summary_path": str(apply_summary_path),
        "audit_entry_count": len(audit_entries),
    }
    apply_summary_path.parent.mkdir(parents=True, exist_ok=True)
    apply_summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def run_correction_phase_three(
    comparison_path: Path = COMPARISON_PATH,
    corrected_dataset_path: Path = CORRECTED_DATASET_PATH,
    reviewed_sheet_path: Path = REVIEWED_SHEET_PATH,
    patches_path: Path = PATCHES_PATH,
    audit_log_path: Path = AUDIT_LOG_PATH,
    revalidated_comparison_path: Path = REVALIDATED_COMPARISON_PATH,
    revalidation_summary_path: Path = REVALIDATION_SUMMARY_PATH,
    revalidation_db_path: Path = REVALIDATION_DB_PATH,
) -> Dict[str, object]:
    summary = run_revalidation(
        comparison_path=comparison_path,
        corrected_dataset_path=corrected_dataset_path,
        revalidated_comparison_path=revalidated_comparison_path,
        revalidation_summary_path=revalidation_summary_path,
        revalidation_db_path=revalidation_db_path,
        patches_path=patches_path,
        audit_log_path=audit_log_path,
    )
    after_report = load_comparison_report(revalidated_comparison_path)
    after_issues = build_issues_from_comparison(
        comparison_report=after_report,
        db_path=revalidation_db_path,
        max_issues_per_rule=None,
    )
    remaining_issue_ids = {issue.issue_id for issue in after_issues}
    _, current_rows = _load_review_sheet_rows(reviewed_sheet_path)
    status_updates: Dict[str, str] = {}
    for row in current_rows:
        issue_id = str(row.get("issue_id", ""))
        issue_status = str(row.get("issue_status", ISSUE_STATUS_PENDING_REVIEW))
        if issue_status not in {ISSUE_STATUS_APPLIED, ISSUE_STATUS_REVALIDATED_RESOLVED, ISSUE_STATUS_REVALIDATED_REMAINING}:
            continue
        status_updates[issue_id] = (
            ISSUE_STATUS_REVALIDATED_REMAINING if issue_id in remaining_issue_ids else ISSUE_STATUS_REVALIDATED_RESOLVED
        )
    if status_updates:
        _update_review_sheet_statuses(reviewed_sheet_path, status_updates)
    return summary


def run_review_validation(
    review_sheet_path: Path = REVIEW_SHEET_PATH,
    issues_path: Path = ISSUES_PATH,
    review_validation_summary_path: Path = REVIEW_VALIDATION_SUMMARY_PATH,
) -> Dict[str, object]:
    summary = validate_review_sheet(review_sheet_path, issues_path)
    review_validation_summary_path.parent.mkdir(parents=True, exist_ok=True)
    review_validation_summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return {
        **summary,
        "review_sheet_path": str(review_sheet_path),
        "issues_path": str(issues_path),
        "review_validation_summary_path": str(review_validation_summary_path),
    }


def _top_rules(issues: List[object], limit: int = 5) -> List[Dict[str, object]]:
    counts: Dict[str, int] = {}
    for issue in issues:
        counts[str(issue.rule_name)] = counts.get(str(issue.rule_name), 0) + 1
    ranked = sorted(counts.items(), key=lambda item: item[1], reverse=True)
    return [{"rule_name": name, "issue_count": count} for name, count in ranked[:limit]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate and apply correction review sheets.")
    parser.add_argument("--mode", choices=["export", "validate-review", "apply", "revalidate"], default="export", help="Correction workflow mode.")
    parser.add_argument("--comparison-path", type=Path, default=COMPARISON_PATH, help="Engine comparison JSON path.")
    parser.add_argument("--issues-path", type=Path, default=ISSUES_PATH, help="Structured issues JSON path.")
    parser.add_argument("--review-sheet-path", type=Path, default=REVIEW_SHEET_PATH, help="CSV review sheet path.")
    parser.add_argument("--max-issues-per-rule", type=int, default=None, help="Optional cap for issue rows per rule.")
    parser.add_argument("--source-snapshot-path", type=Path, default=SOURCE_SNAPSHOT_PATH, help="Frozen source dataset snapshot path for correction review batches.")
    parser.add_argument("--source-jsonl-path", type=Path, default=None, help="Optional source JSONL path for correction application.")
    parser.add_argument("--feedback-store-path", type=Path, default=FEEDBACK_STORE_PATH, help="Reviewer feedback store JSON path.")
    parser.add_argument("--ranking-weights-path", type=Path, default=RANKING_WEIGHTS_PATH, help="Ranking weights snapshot JSON path.")
    parser.add_argument("--review-instructions-path", type=Path, default=REVIEW_INSTRUCTIONS_PATH, help="Reviewer instructions markdown path.")
    parser.add_argument("--review-manifest-path", type=Path, default=REVIEW_MANIFEST_PATH, help="Review bundle manifest JSON path.")
    parser.add_argument("--corrected-dataset-path", type=Path, default=CORRECTED_DATASET_PATH, help="Corrected JSONL output path.")
    parser.add_argument("--patches-path", type=Path, default=PATCHES_PATH, help="Applied patch JSONL output path.")
    parser.add_argument("--audit-log-path", type=Path, default=AUDIT_LOG_PATH, help="Audit log JSONL output path.")
    parser.add_argument("--apply-summary-path", type=Path, default=APPLY_SUMMARY_PATH, help="Latest apply summary JSON path.")
    parser.add_argument("--review-validation-summary-path", type=Path, default=REVIEW_VALIDATION_SUMMARY_PATH, help="Reviewed-sheet validation summary JSON path.")
    parser.add_argument("--revalidated-comparison-path", type=Path, default=REVALIDATED_COMPARISON_PATH, help="Revalidated engine comparison JSON path.")
    parser.add_argument("--revalidation-summary-path", type=Path, default=REVALIDATION_SUMMARY_PATH, help="Before/after quality summary path.")
    parser.add_argument("--revalidation-db-path", type=Path, default=REVALIDATION_DB_PATH, help="Temporary DuckDB path for revalidation.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    effective_review_sheet_path = _resolve_stateful_review_sheet_path(args.review_sheet_path)
    if args.mode == "export":
        summary = run_correction_phase_one(
            comparison_path=args.comparison_path,
            issues_path=args.issues_path,
            review_sheet_path=args.review_sheet_path,
            source_snapshot_path=args.source_snapshot_path,
            feedback_store_path=args.feedback_store_path,
            ranking_weights_path=args.ranking_weights_path,
            review_instructions_path=args.review_instructions_path,
            review_manifest_path=args.review_manifest_path,
            max_issues_per_rule=args.max_issues_per_rule,
        )
        print(f"Issues generated: {summary['issues_generated']}")
        print(f"Rules with issues: {summary['rules_with_issues']}")
        print(f"Issues JSON: {summary['issues_path']}")
        print(f"Review sheet: {summary['review_sheet_path']}")
        print(f"Feedback store: {summary['feedback_store_path']}")
        print(f"Ranking weights: {summary['ranking_weights_path']}")
        print(f"Review instructions: {summary['review_instructions_path']}")
        print(f"Review manifest: {summary['review_manifest_path']}")
        if summary["top_rules"]:
            print("Top rules:")
            for row in summary["top_rules"]:
                print(f"  - {row['rule_name']}: {row['issue_count']}")
        return

    if args.mode == "validate-review":
        summary = run_review_validation(
            review_sheet_path=effective_review_sheet_path,
            issues_path=args.issues_path,
            review_validation_summary_path=args.review_validation_summary_path,
        )
        print(f"Review rows: {summary['review_rows']}")
        print(f"Approved rows: {summary['approved_rows']}")
        print(f"Rejected rows: {summary['rejected_rows']}")
        print(f"Pending rows: {summary['pending_rows']}")
        print(f"Errors: {summary['error_count']}")
        print(f"Warnings: {summary['warning_count']}")
        print(f"Validation summary: {summary['review_validation_summary_path']}")
        if summary["errors"]:
            print("First errors:")
            for message in summary["errors"][:5]:
                print(f"  - {message}")
        if summary["warnings"]:
            print("First warnings:")
            for message in summary["warnings"][:5]:
                print(f"  - {message}")
        return

    if args.mode == "apply":
        summary = run_correction_phase_two(
            comparison_path=args.comparison_path,
            issues_path=args.issues_path,
            reviewed_sheet_path=effective_review_sheet_path,
            corrected_dataset_path=args.corrected_dataset_path,
            patches_path=args.patches_path,
            audit_log_path=args.audit_log_path,
            source_snapshot_path=args.source_snapshot_path,
            feedback_store_path=args.feedback_store_path,
            apply_summary_path=args.apply_summary_path,
            review_validation_summary_path=args.review_validation_summary_path,
            source_jsonl_path=args.source_jsonl_path,
        )
        print(f"Reviewed rows: {summary['review_rows']}")
        print(f"Approved rows: {summary['approved_rows']}")
        print(f"Rejected rows: {summary['rejected_rows']}")
        print(f"Applied patches: {summary['applied_patch_count']}")
        print(f"Changed products: {summary['changed_products']}")
        print(f"Corrected dataset: {summary['corrected_dataset_path']}")
        print(f"Patch file: {summary['patches_path']}")
        print(f"Audit log: {summary['audit_log_path']}")
        print(f"Feedback store: {summary['feedback_store_path']}")
        print(f"Apply summary: {summary['apply_summary_path']}")
        print(f"Validation summary: {summary['review_validation_summary_path']}")
        return

    summary = run_correction_phase_three(
        comparison_path=args.comparison_path,
        corrected_dataset_path=args.corrected_dataset_path,
        reviewed_sheet_path=effective_review_sheet_path,
        patches_path=args.patches_path,
        audit_log_path=args.audit_log_path,
        revalidated_comparison_path=args.revalidated_comparison_path,
        revalidation_summary_path=args.revalidation_summary_path,
        revalidation_db_path=args.revalidation_db_path,
    )
    print(f"Before issues: {summary['before_issue_count']}")
    print(f"After issues: {summary['after_issue_count']}")
    print(f"Resolved issues: {summary['resolved_issue_count']}")
    print(f"Introduced issues: {summary['introduced_issue_count']}")
    print(f"Quality score before: {summary['quality_score_before']:.2%}")
    print(f"Quality score after: {summary['quality_score_after']:.2%}")
    print(f"Revalidation summary: {args.revalidation_summary_path}")
    print(f"Revalidated comparison: {args.revalidated_comparison_path}")


if __name__ == "__main__":
    main()
