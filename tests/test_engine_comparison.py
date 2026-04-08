import json

from validation.engine_comparison import ENGINES, run_engine_comparison


def test_engine_comparison_smoke(tmp_path) -> None:
    results_path = tmp_path / "engine_comparison.json"
    db_path = tmp_path / "engine_compare.db"

    report = run_engine_comparison(
        dataset_size=100,
        seed=17,
        source_jsonl=None,
        use_default_off_source=False,
        llm_provider="simulated",
        llm_model=None,
        perl_rules_dir=None,
        db_path=db_path,
        results_path=results_path,
    )

    assert results_path.exists()
    parsed = json.loads(results_path.read_text(encoding="utf-8"))
    assert parsed["engines"] == list(ENGINES)
    assert set(parsed["per_engine_summary"].keys()) == set(ENGINES)
    assert len(parsed["rule_comparison"]) > 0
    assert "comparison_method" in parsed
    assert "run_config" in parsed
    assert "per_complexity_summary" in parsed
    assert "migration_state_summary" in parsed
    assert parsed["run_config"]["soda_mode"] == "local"
    assert "comparison_fingerprint" in parsed
    assert "engine_run_fingerprints" in parsed
    assert parsed["comparison_fingerprint"]["dataset_fingerprint_consistent"] is True
    assert parsed["comparison_fingerprint"]["rulepack_fingerprint_consistent"] is True
    first_rule = parsed["rule_comparison"][0]
    assert set(first_rule["engines"].keys()) == set(ENGINES)
    assert "best_engine" in first_rule
    assert "recommendation" in first_rule
    assert "migration_state" in first_rule
    assert "migration_state_reason" in first_rule
    assert "complexity" in first_rule
    assert "effective_confidence" in first_rule["engines"]["python"]
    assert "provider_factor" in first_rule["engines"]["python"]
    assert report["engines"] == list(ENGINES)
