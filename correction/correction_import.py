"""Import reviewed correction decisions from a spreadsheet-style CSV."""
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Dict, List, Mapping

from correction.correction_export import REVIEW_SHEET_COLUMNS
from correction.review_status import (
    ISSUE_STATUS_APPROVED,
    ISSUE_STATUS_REJECTED,
    ISSUE_STATUS_VALUES,
    derive_issue_status,
    is_valid_issue_status,
    normalize_issue_status,
)
from correction.schemas import IssueRecord, ReviewDecision


def load_issue_records(issues_path: Path) -> Dict[str, IssueRecord]:
    payload = json.loads(issues_path.read_text(encoding="utf-8"))
    return {str(item.get("issue_id", "")): IssueRecord.from_mapping(item) for item in payload}


def _normalize_action(value: str) -> str:
    normalized = _normalize_optional_text(value).strip().lower()
    if normalized in {"approve", "approved", "apply", "yes", "y"}:
        return "approve"
    if normalized in {"reject", "rejected", "skip", "no", "n"}:
        return "reject"
    return normalized or ""


def _normalize_optional_text(value: object) -> str:
    text = "" if value is None else str(value).strip()
    if text.lower() in {"", "nan", "none", "null"}:
        return ""
    return text


def _coerce_value(raw_value: str) -> object:
    value = raw_value.strip()
    if value == "":
        return ""
    lowered = value.lower()
    if lowered in {"null", "none", "blank"}:
        return None
    if lowered in {"true", "yes"}:
        return 1
    if lowered in {"false", "no"}:
        return 0
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def parse_manual_edit(manual_edit: str) -> Dict[str, object]:
    text = _normalize_optional_text(manual_edit)
    if not text:
        return {}
    if text.startswith("{"):
        parsed = json.loads(text)
        if not isinstance(parsed, dict):
            raise ValueError("manual_edit JSON must be an object of field/value pairs.")
        return {str(key): value for key, value in parsed.items()}

    changes: Dict[str, object] = {}
    for chunk in [part.strip() for part in text.split(";") if part.strip()]:
        if "=" in chunk:
            field, raw_value = chunk.split("=", 1)
        elif ":" in chunk:
            field, raw_value = chunk.split(":", 1)
        else:
            raise ValueError(f"Invalid manual_edit chunk `{chunk}`. Use field=value pairs.")
        changes[field.strip()] = _coerce_value(raw_value)
    return changes


def validate_review_sheet(review_sheet_path: Path, issues_path: Path) -> Dict[str, object]:
    issues_by_id = load_issue_records(issues_path)
    errors: List[str] = []
    warnings: List[str] = []
    seen_issue_ids: set[str] = set()
    approved_rows = 0
    rejected_rows = 0
    pending_rows = 0

    with review_sheet_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        missing_columns = [column for column in REVIEW_SHEET_COLUMNS if column not in fieldnames]
        if missing_columns:
            errors.append(f"Missing required columns: {', '.join(missing_columns)}")

        row_count = 0
        for row_index, row in enumerate(reader, start=2):
            row_count += 1
            issue_id = _normalize_optional_text(row.get("issue_id", ""))
            if not issue_id:
                errors.append(f"Row {row_index}: issue_id is required.")
                continue
            if issue_id in seen_issue_ids:
                errors.append(f"Row {row_index}: duplicate issue_id `{issue_id}`.")
                continue
            seen_issue_ids.add(issue_id)

            issue = issues_by_id.get(issue_id)
            if issue is None:
                errors.append(f"Row {row_index}: unknown issue_id `{issue_id}`.")
                continue

            action = _normalize_action(row.get("user_action", ""))
            issue_status = normalize_issue_status(row.get("issue_status", ""))
            effective_status = derive_issue_status(issue_status, action)
            selected_fix_id = _normalize_optional_text(row.get("selected_fix_id", ""))
            if "|" in selected_fix_id:
                selected_fix_id = selected_fix_id.split("|", 1)[0].strip()
            manual_edit_raw = _normalize_optional_text(row.get("manual_edit", ""))

            if effective_status == ISSUE_STATUS_APPROVED:
                approved_rows += 1
            elif effective_status == ISSUE_STATUS_REJECTED:
                rejected_rows += 1
            else:
                pending_rows += 1

            if action not in {"", "approve", "reject"}:
                errors.append(f"Row {row_index}: invalid user_action `{row.get('user_action', '')}`.")
            if issue_status not in ISSUE_STATUS_VALUES:
                errors.append(f"Row {row_index}: invalid issue_status `{row.get('issue_status', '')}`.")

            if selected_fix_id:
                selected_fix = next((fix for fix in issue.fix_candidates if fix.fix_id == selected_fix_id), None)
                if selected_fix is None:
                    errors.append(f"Row {row_index}: unknown selected_fix_id `{selected_fix_id}` for issue `{issue_id}`.")

            if manual_edit_raw:
                try:
                    parse_manual_edit(manual_edit_raw)
                except ValueError as exc:
                    errors.append(f"Row {row_index}: {exc}")

            if effective_status == ISSUE_STATUS_APPROVED and not selected_fix_id and not manual_edit_raw:
                warnings.append(
                    f"Row {row_index}: marked approve but has no selected_fix_id or manual_edit; it will be skipped during apply."
                )
            if effective_status == normalize_issue_status("pending_review") and (selected_fix_id or manual_edit_raw):
                warnings.append(
                    f"Row {row_index}: contains a fix choice or manual edit but no user_action; it will remain pending."
                )

    return {
        "valid": not errors,
        "review_rows": len(seen_issue_ids),
        "approved_rows": approved_rows,
        "rejected_rows": rejected_rows,
        "pending_rows": pending_rows,
        "error_count": len(errors),
        "warning_count": len(warnings),
        "errors": errors,
        "warnings": warnings,
        "required_columns": REVIEW_SHEET_COLUMNS,
    }


def import_review_decisions(review_sheet_path: Path, issues_path: Path) -> List[ReviewDecision]:
    issues_by_id = load_issue_records(issues_path)
    decisions: List[ReviewDecision] = []

    with review_sheet_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            issue_id = str(row.get("issue_id", "")).strip()
            if not issue_id:
                continue
            issue = issues_by_id.get(issue_id)
            if issue is None:
                raise ValueError(f"Reviewed sheet contains unknown issue_id `{issue_id}`.")

            action = _normalize_action(str(row.get("user_action", "")))
            issue_status = derive_issue_status(row.get("issue_status", ""), action)
            selected_fix_id = _normalize_optional_text(row.get("selected_fix_id", ""))
            if "|" in selected_fix_id:
                selected_fix_id = selected_fix_id.split("|", 1)[0].strip()
            manual_changes = parse_manual_edit(str(row.get("manual_edit", "")))
            review_notes = _normalize_optional_text(row.get("review_notes", ""))
            reviewer_name = _normalize_optional_text(row.get("reviewer_name", ""))
            reviewed_at_utc = _normalize_optional_text(row.get("reviewed_at_utc", ""))

            applied_changes: Dict[str, object] = {}
            source_fix_ids: List[str] = []
            if selected_fix_id:
                selected_fix = next((fix for fix in issue.fix_candidates if fix.fix_id == selected_fix_id), None)
                if selected_fix is None:
                    raise ValueError(f"Unknown selected_fix_id `{selected_fix_id}` for issue `{issue_id}`.")
                if selected_fix.target_field:
                    applied_changes[selected_fix.target_field] = selected_fix.new_value
                source_fix_ids.append(selected_fix.fix_id)

            if manual_changes:
                applied_changes.update(manual_changes)
                source_fix_ids.append("manual_edit")

            decisions.append(
                ReviewDecision(
                    issue_id=issue.issue_id,
                    product_id=issue.product_id,
                    rule_name=issue.rule_name,
                    user_action=action,
                    issue_status=issue_status,
                    selected_fix_id=selected_fix_id,
                    manual_changes=manual_changes,
                    review_notes=review_notes,
                    applied_changes=applied_changes,
                    source_fix_ids=source_fix_ids,
                    reviewer_name=reviewer_name,
                    reviewed_at_utc=reviewed_at_utc,
                )
            )
    return decisions
