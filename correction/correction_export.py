"""Export correction issues to spreadsheet-friendly review files."""
from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Mapping

from correction.review_status import ISSUE_STATUS_PENDING_REVIEW
from correction.schemas import IssueRecord

REVIEW_SHEET_COLUMNS = [
    "issue_id",
    "product_id",
    "rule_name",
    "severity",
    "jurisdiction",
    "issue_description",
    "current_values",
    "rule_explanation",
    "confidence",
    "migration_state",
    "best_engine",
    "recommended_fix",
    "suggested_fix_1",
    "suggested_fix_2",
    "suggested_fix_3",
    "selected_fix_id",
    "manual_edit",
    "user_action",
    "issue_status",
    "review_notes",
    "reviewer_name",
    "reviewed_at_utc",
]


def write_issues_json(issues: Iterable[IssueRecord], path: Path) -> None:
    payload = [issue.to_dict() for issue in issues]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _format_fix_cell(issue: IssueRecord, index: int) -> str:
    if index > len(issue.fix_candidates):
        return ""
    fix = issue.fix_candidates[index - 1]
    return (
        f"{fix.fix_id} | {fix.suggested_change} | "
        f"confidence={fix.confidence:.2f} | {fix.explanation}"
    )


def export_review_sheet(issues: Iterable[IssueRecord], path: Path) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for issue in issues:
        rows.append(
            {
                "issue_id": issue.issue_id,
                "product_id": issue.product_id,
                "rule_name": issue.rule_name,
                "severity": issue.severity,
                "jurisdiction": issue.jurisdiction,
                "issue_description": issue.issue_description,
                "current_values": issue.current_values_display,
                "rule_explanation": issue.rule_explanation,
                "confidence": round(issue.confidence, 4),
                "migration_state": issue.migration_state,
                "best_engine": issue.best_engine,
                "recommended_fix": issue.fix_candidates[0].suggested_change if issue.fix_candidates else "",
                "suggested_fix_1": _format_fix_cell(issue, 1),
                "suggested_fix_2": _format_fix_cell(issue, 2),
                "suggested_fix_3": _format_fix_cell(issue, 3),
                "selected_fix_id": "",
                "manual_edit": "",
                "user_action": "",
                "issue_status": ISSUE_STATUS_PENDING_REVIEW,
                "review_notes": "",
                "reviewer_name": "",
                "reviewed_at_utc": "",
            }
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=REVIEW_SHEET_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
    else:
        path.write_text("", encoding="utf-8")
    return rows


def write_review_instructions(path: Path, review_sheet_name: str) -> None:
    issued_at = datetime.now(timezone.utc).isoformat()
    instructions = f"""# Correction Review Instructions

Generated at: {issued_at}
Review file: `{review_sheet_name}`

## Purpose
Review flagged data-quality issues in a spreadsheet-friendly format, choose a suggested fix or enter a manual edit, and send the reviewed sheet back for safe batch application.

## Recommended workflow
1. Open the CSV in Google Sheets or Excel.
2. Do not delete or rename columns.
3. For each row, either:
   - choose a `selected_fix_id` from one of the suggested fix columns, or
   - enter a `manual_edit` using `field=value` pairs.
4. Set `user_action` to `approve` or `reject`.
5. Leave `issue_status` alone if possible; the system updates it as the workflow progresses.
6. Optionally add `review_notes`, `reviewer_name`, and `reviewed_at_utc`.
7. Export the reviewed file back to CSV before importing it into the correction system.

## Column guide
- `selected_fix_id`: copy just the fix id, for example `abc123_limit`
- `manual_edit`: examples: `carbohydrates=105` or `fat=10;saturated_fat=8`
- `user_action`: `approve` or `reject`
- `issue_status`: system-managed workflow state such as `pending_review`, `approved`, `applied`
- `reviewer_name`: optional reviewer name or initials
- `reviewed_at_utc`: optional ISO timestamp such as `2026-04-03T10:30:00Z`

## Validation rules
- `issue_id` must stay unchanged
- `selected_fix_id` must match the row's suggested fixes
- `manual_edit` must use valid `field=value` pairs if provided
- approved rows with no selected fix and no manual edit will be skipped
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(instructions, encoding="utf-8")


def write_review_manifest(
    path: Path,
    *,
    review_sheet_path: Path,
    issues_path: Path,
    source_snapshot_path: Path,
    comparison_report: Mapping[str, object],
    issue_count: int,
    rules_with_issues: int,
) -> None:
    run_config = dict(comparison_report.get("run_config", {}))
    dataset = dict(comparison_report.get("dataset", {}))
    fingerprint = dict(comparison_report.get("comparison_fingerprint", {}))
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "review_sheet_path": str(review_sheet_path),
        "issues_path": str(issues_path),
        "source_snapshot_path": str(source_snapshot_path),
        "issue_count": issue_count,
        "rules_with_issues": rules_with_issues,
        "required_columns": REVIEW_SHEET_COLUMNS,
        "comparison_run_id": fingerprint.get("comparison_run_id", ""),
        "dataset_source_jsonl": dataset.get("source_jsonl", ""),
        "profile": dataset.get("profile", run_config.get("profile", "")),
        "llm_provider": run_config.get("llm_provider", ""),
        "soda_mode": run_config.get("soda_mode", ""),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
