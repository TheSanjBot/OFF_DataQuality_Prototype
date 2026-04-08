import csv
import json

from correction.fix_generator import generate_fix_candidates
from correction.fix_ranker import rank_fix_candidates
from correction.correction_export import REVIEW_SHEET_COLUMNS
from correction.issue_builder import build_issues_from_comparison
from correction.schemas import IssueRecord
from orchestrator.correction_runner import run_correction_phase_one
from validation.engine_comparison import run_engine_comparison


def test_correction_phase_one_export(tmp_path) -> None:
    comparison_path = tmp_path / "engine_comparison.json"
    db_path = tmp_path / "off_quality.db"
    issues_path = tmp_path / "issues.json"
    review_path = tmp_path / "review_sheet.csv"
    instructions_path = tmp_path / "review_instructions.md"
    manifest_path = tmp_path / "review_manifest.json"

    report = run_engine_comparison(
        dataset_size=60,
        seed=17,
        source_jsonl=None,
        use_default_off_source=False,
        llm_provider="simulated",
        db_path=db_path,
        results_path=comparison_path,
    )

    issues = build_issues_from_comparison(report, db_path=db_path, max_issues_per_rule=5)
    assert issues
    assert issues[0].product_id
    assert issues[0].rule_name
    assert issues[0].current_values

    summary = run_correction_phase_one(
        comparison_path=comparison_path,
        issues_path=issues_path,
        review_sheet_path=review_path,
        review_instructions_path=instructions_path,
        review_manifest_path=manifest_path,
        max_issues_per_rule=5,
    )

    assert summary["issues_generated"] > 0
    assert issues_path.exists()
    assert review_path.exists()
    assert instructions_path.exists()
    assert manifest_path.exists()

    structured = json.loads(issues_path.read_text(encoding="utf-8"))
    assert structured[0]["fix_candidates"]
    assert len(structured[0]["fix_candidates"]) <= 3
    assert structured[0]["fix_candidates"][0]["rank"] == 1

    with review_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
    assert rows
    assert "suggested_fix_1" in rows[0]
    assert "selected_fix_id" in rows[0]
    assert "issue_status" in rows[0]
    assert "reviewer_name" in rows[0]
    assert "reviewed_at_utc" in rows[0]
    assert rows[0]["recommended_fix"]
    assert list(rows[0].keys()) == REVIEW_SHEET_COLUMNS

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["required_columns"] == REVIEW_SHEET_COLUMNS
    assert manifest["issue_count"] == summary["issues_generated"]


def test_threshold_rule_gets_limit_fix() -> None:
    issue = IssueRecord(
        issue_id="issue123",
        product_id="0001",
        rule_name="carbohydrates_over_105g",
        tag="carbohydrates-value-over-105g",
        severity="warning",
        jurisdiction="global",
        condition="carbohydrates > 105",
        condition_type="field_threshold",
        issue_description="Carbohydrates is 140, above the allowed maximum of 105.",
        rule_explanation="Carbohydrates should not exceed 105.",
        current_values={"carbohydrates": 140.0},
        current_values_display="carbohydrates=140",
        confidence=0.82,
        migration_state="accepted",
        migration_state_reason="test",
        best_engine="dbt",
        rule_ir_hash="abc123",
    )

    ranked = rank_fix_candidates(issue, generate_fix_candidates(issue))
    assert ranked[0].suggested_change == "Set carbohydrates to 105."
    assert ranked[0].target_field == "carbohydrates"
