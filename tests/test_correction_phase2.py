import csv
import json

from correction.correction_import import import_review_decisions, parse_manual_edit, validate_review_sheet
from correction.review_status import ISSUE_STATUS_APPLIED
from correction.issue_builder import build_issues_from_comparison
from correction.schemas import IssueRecord
from orchestrator.correction_runner import run_correction_phase_one, run_correction_phase_two
from validation.engine_comparison import run_engine_comparison


def test_parse_manual_edit_supports_pairs_and_json() -> None:
    assert parse_manual_edit("carbohydrates=103;sugars=100") == {"carbohydrates": 103, "sugars": 100}
    assert parse_manual_edit('{"fat": 10, "saturated_fat": 8}') == {"fat": 10, "saturated_fat": 8}
    assert parse_manual_edit("nan") == {}
    assert parse_manual_edit("") == {}


def test_validate_review_sheet_reports_invalid_roundtrip_rows(tmp_path) -> None:
    issues_path = tmp_path / "issues.json"
    review_path = tmp_path / "reviewed_sheet.csv"
    issues_path.write_text(
        json.dumps(
            [
                {
                    "issue_id": "issue_1",
                    "product_id": "123",
                    "rule_name": "carbohydrates_over_105g",
                    "tag": "carbohydrates",
                    "severity": "error",
                    "jurisdiction": "global",
                    "condition": "carbohydrates > 105",
                    "condition_type": "field_threshold",
                    "issue_description": "Carbohydrates exceed 105.",
                    "rule_explanation": "Carbohydrates should not exceed 105.",
                    "current_values": {"carbohydrates": 114.8},
                    "current_values_display": "carbohydrates=114.8",
                    "confidence": 0.91,
                    "migration_state": "accepted",
                    "migration_state_reason": "test",
                    "best_engine": "dbt",
                    "rule_ir_hash": "abc123",
                    "fix_candidates": [
                        {
                            "fix_id": "issue_1_limit",
                            "suggested_change": "Set carbohydrates to 105.",
                            "target_field": "carbohydrates",
                            "new_value": 105.0,
                            "confidence": 0.88,
                            "explanation": "Clamp to limit.",
                            "expected_outcome": "Rule should pass.",
                            "fix_strategy": "clamp_to_limit",
                            "rank": 1,
                            "ranking_reason": "test",
                        }
                    ],
                }
            ],
            indent=2,
        ),
        encoding="utf-8",
    )
    fieldnames = [
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
    with review_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "issue_id": "issue_1",
                "product_id": "123",
                "rule_name": "carbohydrates_over_105g",
                "severity": "error",
                "jurisdiction": "global",
                "issue_description": "Carbohydrates exceed 105",
                "current_values": "carbohydrates=114.8",
                "rule_explanation": "Rule explanation",
                "confidence": "0.91",
                "migration_state": "accepted",
                "best_engine": "dbt",
                "recommended_fix": "Set carbohydrates to 105",
                "suggested_fix_1": "issue_1_limit | Set carbohydrates to 105",
                "suggested_fix_2": "",
                "suggested_fix_3": "",
                "selected_fix_id": "issue_1_missing",
                "manual_edit": "broken-edit",
                "user_action": "approve",
                "issue_status": "pending_review",
                "review_notes": "Needs review",
                "reviewer_name": "SJ",
                "reviewed_at_utc": "",
            }
        )

    summary = validate_review_sheet(review_path, issues_path)

    assert not summary["valid"]
    assert summary["error_count"] >= 2
    assert any("unknown selected_fix_id" in message for message in summary["errors"])
    assert any("Invalid manual_edit chunk" in message for message in summary["errors"])



def test_correction_phase_two_applies_reviewed_changes(tmp_path) -> None:
    comparison_path = tmp_path / "engine_comparison.json"
    db_path = tmp_path / "off_quality.db"
    issues_path = tmp_path / "issues.json"
    review_path = tmp_path / "review_sheet.csv"
    corrected_dataset_path = tmp_path / "corrected_dataset.jsonl"
    patches_path = tmp_path / "correction_patches.jsonl"
    audit_log_path = tmp_path / "audit_log.jsonl"

    run_engine_comparison(
        dataset_size=40,
        seed=17,
        source_jsonl=None,
        use_default_off_source=False,
        llm_provider="simulated",
        db_path=db_path,
        results_path=comparison_path,
    )

    phase_one = run_correction_phase_one(
        comparison_path=comparison_path,
        issues_path=issues_path,
        review_sheet_path=review_path,
        max_issues_per_rule=5,
    )
    assert phase_one["issues_generated"] > 0

    issues_payload = json.loads(issues_path.read_text(encoding="utf-8"))
    carb_issue = next(item for item in issues_payload if item["rule_name"] == "carbohydrates_over_105g")
    energy_issue = next(item for item in issues_payload if item["rule_name"] == "energy_kcal_vs_kj")

    with review_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    fieldnames = list(rows[0].keys())
    for row in rows:
        if row["issue_id"] == carb_issue["issue_id"]:
            row["selected_fix_id"] = carb_issue["fix_candidates"][0]["fix_id"]
            row["user_action"] = "approve"
            row["review_notes"] = "Apply direct threshold fix"
        elif row["issue_id"] == energy_issue["issue_id"]:
            row["manual_edit"] = "energy_kcal=100"
            row["user_action"] = "approve"
            row["review_notes"] = "Manual correction from label"
        else:
            row["user_action"] = "reject"

    with review_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    decisions = import_review_decisions(review_path, issues_path)
    approved = [decision for decision in decisions if decision.user_action == "approve"]
    assert len(approved) == 2
    assert all(decision.issue_status == "approved" for decision in approved)

    phase_two = run_correction_phase_two(
        comparison_path=comparison_path,
        issues_path=issues_path,
        reviewed_sheet_path=review_path,
        corrected_dataset_path=corrected_dataset_path,
        patches_path=patches_path,
        audit_log_path=audit_log_path,
    )

    assert phase_two["approved_rows"] == 2
    assert phase_two["applied_patch_count"] >= 2
    assert corrected_dataset_path.exists()
    assert patches_path.exists()
    assert audit_log_path.exists()

    corrected_rows = [json.loads(line) for line in corrected_dataset_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    corrected_by_id = {str(row["product_id"]): row for row in corrected_rows}
    assert corrected_by_id[carb_issue["product_id"]]["carbohydrates"] == 105.0
    assert corrected_by_id[energy_issue["product_id"]]["energy_kcal"] == 100

    patch_rows = [json.loads(line) for line in patches_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert any(row["field"] == "carbohydrates" for row in patch_rows)
    assert any(row["field"] == "energy_kcal" for row in patch_rows)

    audit_rows = [json.loads(line) for line in audit_log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(audit_rows) == len(patch_rows)
    assert audit_rows[0]["review_action"] == "approve"

    reviewed_rows = list(csv.DictReader(review_path.open("r", encoding="utf-8", newline="")))
    applied_statuses = {
        row["issue_status"]
        for row in reviewed_rows
        if row["issue_id"] in {carb_issue["issue_id"], energy_issue["issue_id"]}
    }
    assert applied_statuses == {ISSUE_STATUS_APPLIED}
