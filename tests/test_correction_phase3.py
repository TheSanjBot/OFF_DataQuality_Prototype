import csv
import json

from correction.review_status import (
    ISSUE_STATUS_REVALIDATED_REMAINING,
    ISSUE_STATUS_REVALIDATED_RESOLVED,
)
from orchestrator.correction_runner import (
    run_correction_phase_one,
    run_correction_phase_three,
    run_correction_phase_two,
)
from validation.engine_comparison import run_engine_comparison


def test_correction_phase_three_revalidation_shows_improvement(tmp_path) -> None:
    comparison_path = tmp_path / "engine_comparison.json"
    db_path = tmp_path / "off_quality.db"
    issues_path = tmp_path / "issues.json"
    review_path = tmp_path / "review_sheet.csv"
    corrected_dataset_path = tmp_path / "corrected_dataset.jsonl"
    patches_path = tmp_path / "correction_patches.jsonl"
    audit_log_path = tmp_path / "audit_log.jsonl"
    revalidated_comparison_path = tmp_path / "revalidated_engine_comparison.json"
    revalidation_summary_path = tmp_path / "revalidation_summary.json"
    revalidation_db_path = tmp_path / "revalidation_off_quality.db"

    run_engine_comparison(
        dataset_size=40,
        seed=17,
        source_jsonl=None,
        use_default_off_source=False,
        llm_provider="simulated",
        db_path=db_path,
        results_path=comparison_path,
    )

    run_correction_phase_one(
        comparison_path=comparison_path,
        issues_path=issues_path,
        review_sheet_path=review_path,
        max_issues_per_rule=5,
    )

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
            row["selected_fix_id"] = energy_issue["fix_candidates"][0]["fix_id"]
            row["user_action"] = "approve"
            row["review_notes"] = "Apply suggested energy fix"
        else:
            row["user_action"] = "reject"

    with review_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    phase_two = run_correction_phase_two(
        comparison_path=comparison_path,
        issues_path=issues_path,
        reviewed_sheet_path=review_path,
        corrected_dataset_path=corrected_dataset_path,
        patches_path=patches_path,
        audit_log_path=audit_log_path,
    )
    assert phase_two["approved_rows"] == 2

    summary = run_correction_phase_three(
        comparison_path=comparison_path,
        corrected_dataset_path=corrected_dataset_path,
        reviewed_sheet_path=review_path,
        patches_path=patches_path,
        audit_log_path=audit_log_path,
        revalidated_comparison_path=revalidated_comparison_path,
        revalidation_summary_path=revalidation_summary_path,
        revalidation_db_path=revalidation_db_path,
    )

    assert revalidated_comparison_path.exists()
    assert revalidation_summary_path.exists()
    assert summary["before_issue_count"] > summary["after_issue_count"]
    assert summary["resolved_issue_count"] >= 2
    assert summary["quality_score_after"] > summary["quality_score_before"]
    assert any(row["rule_name"] == "carbohydrates_over_105g" and row["resolved_count"] >= 1 for row in summary["per_rule_changes"])

    reviewed_rows = list(csv.DictReader(review_path.open("r", encoding="utf-8", newline="")))
    revalidated_statuses = {
        row["issue_status"]
        for row in reviewed_rows
        if row["issue_id"] in {carb_issue["issue_id"], energy_issue["issue_id"]}
    }
    assert revalidated_statuses
    assert revalidated_statuses.issubset(
        {
            ISSUE_STATUS_REVALIDATED_RESOLVED,
            ISSUE_STATUS_REVALIDATED_REMAINING,
        }
    )
