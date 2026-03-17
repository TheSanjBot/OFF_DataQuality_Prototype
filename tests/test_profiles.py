from pathlib import Path

from rulepacks.registry import DEFAULT_PROFILE, SUPPORTED_PROFILES, get_profile_rule_names, validate_profile
from validation.parity_validator import run_pipeline


def test_profile_registry_basics() -> None:
    assert DEFAULT_PROFILE in SUPPORTED_PROFILES
    assert validate_profile("HYBRID") == "hybrid"
    rule_names = [
        "energy_kcal_vs_kj",
        "main_language_code_missing",
        "main_language_missing",
        "ca_fop_required_but_symbol_missing",
    ]
    assert get_profile_rule_names("global", rule_names) == ["energy_kcal_vs_kj"]
    assert get_profile_rule_names("canada", rule_names) == [
        "main_language_code_missing",
        "main_language_missing",
        "ca_fop_required_but_symbol_missing",
    ]
    assert get_profile_rule_names("hybrid", rule_names) == rule_names


def test_canada_profile_pipeline_subset(tmp_path, monkeypatch) -> None:
    import validation.parity_validator as parity_validator

    monkeypatch.setattr(parity_validator, "DEFAULT_OFF_JSONL", tmp_path / "missing_off_source.jsonl")
    payload = run_pipeline(
        dataset_size=120,
        seed=17,
        results_path=tmp_path / "migration_results_canada.json",
        source_jsonl=None,
        use_default_off_source=False,
        db_path=tmp_path / "off_quality_canada.db",
        llm_provider="simulated",
        perl_rules_dir=Path(__file__).resolve().parent.parent / "perl_checks" / "rules",
        execution_engine="python",
        profile="canada",
    )

    assert payload["dataset"]["profile"] == "canada"
    assert payload["migration_summary"]["total_rules"] == 7
    jurisdictions = {row["jurisdiction"] for row in payload["rule_results"]}
    assert jurisdictions == {"ca"}
    citations = [str(row.get("legal_citation", "")) for row in payload["rule_results"]]
    assert all(citation != "" for citation in citations)
