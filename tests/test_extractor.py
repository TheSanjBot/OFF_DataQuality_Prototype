from pathlib import Path

from extractor.perl_logic_extractor import extract_rules
from perl_checks.legacy_checks import RULE_FILES_DIR, load_rule_snippets_from_directory


def test_extract_rules_from_perl_files() -> None:
    snippets = load_rule_snippets_from_directory(RULE_FILES_DIR)
    rules = extract_rules(snippets)

    assert len(rules) >= 19
    names = {rule["rule_name"] for rule in rules}
    assert "energy_kcal_vs_kj" in names
    assert "main_language_code_missing" in names
    assert "energy_kj_mismatch_low" in names
    assert "energy_kj_computed_mismatch_low" in names
    assert "energy_kj_computed_mismatch_high" in names
    assert "sugars_plus_starch_vs_carbohydrates" in names
    assert "ca_fop_required_but_symbol_missing" in names
    assert "ca_contains_statement_without_allergen_evidence" in names

    missing_rule = next(rule for rule in rules if rule["rule_name"] == "main_language_code_missing")
    assert missing_rule["condition_type"] == "missing_field"
    assert missing_rule["duckdb_condition"] == "lc IS NULL OR TRIM(lc) = ''"
    assert "rule_ir" in missing_rule
    assert len(str(missing_rule.get("rule_ir_hash", ""))) == 12

    affine_rule = next(rule for rule in rules if rule["rule_name"] == "energy_kj_mismatch_low")
    assert affine_rule["condition_type"] == "affine_field_comparison"
    assert affine_rule["complexity"] == "intricate"
    assert affine_rule["declarative_friendly"] is False
    assert affine_rule["scale_factor"] == 3.7
    assert affine_rule["offset"] == -2.0

    sum_rule = next(rule for rule in rules if rule["rule_name"] == "sugars_plus_starch_vs_carbohydrates")
    assert sum_rule["condition_type"] == "sum_fields_comparison"
    assert sum_rule["left_operands"] == ["sugars", "starch"]
    assert sum_rule["right_operand"] == "carbohydrates"

    computed_rule = next(rule for rule in rules if rule["rule_name"] == "energy_kj_computed_mismatch_low")
    assert computed_rule["condition_type"] == "affine_field_comparison"
    assert computed_rule["left_operand"] == "energy_kj_computed"
    assert computed_rule["right_operand"] == "energy_kj"
    assert computed_rule["scale_factor"] == 0.7
    assert computed_rule["offset"] == -5.0

    ca_fop_rule = next(rule for rule in rules if rule["rule_name"] == "ca_fop_required_but_symbol_missing")
    assert ca_fop_rule["condition_type"] == "compound_threshold_and"
    assert ca_fop_rule["complexity"] == "medium"


def test_perl_rule_files_are_present() -> None:
    rule_files = sorted(Path(RULE_FILES_DIR).glob("*.pl"))
    assert len(rule_files) >= 8
