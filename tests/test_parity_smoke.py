from pathlib import Path

import validation.parity_validator as parity_validator


def test_parity_pipeline_smoke_simulated(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(parity_validator, "DEFAULT_OFF_JSONL", tmp_path / "missing_off_source.jsonl")
    results_path = tmp_path / "migration_results.json"
    perl_rules_dir = Path(__file__).resolve().parent.parent / "perl_checks" / "rules"

    payload = parity_validator.run_pipeline(
        dataset_size=100,
        seed=17,
        results_path=results_path,
        source_jsonl=None,
        use_default_off_source=False,
        db_path=tmp_path / "off_quality_test.db",
        llm_provider="simulated",
        perl_rules_dir=perl_rules_dir,
    )

    summary = payload["migration_summary"]
    assert results_path.exists()
    assert summary["total_rules"] == 8
    assert summary["passed_rules"] == 8
    assert summary["rules_needing_review"] == 0
    first_rule = payload["rule_results"][0]
    assert "parity_ci_lower" in first_rule
    assert "evidence_ci_lower" in first_rule
    assert "overall_method" in first_rule
