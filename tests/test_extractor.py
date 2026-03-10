from pathlib import Path

from extractor.perl_logic_extractor import extract_rules
from perl_checks.legacy_checks import RULE_FILES_DIR, load_rule_snippets_from_directory


def test_extract_rules_from_perl_files() -> None:
    snippets = load_rule_snippets_from_directory(RULE_FILES_DIR)
    rules = extract_rules(snippets)

    assert len(rules) == 8
    names = {rule["rule_name"] for rule in rules}
    assert "energy_kcal_vs_kj" in names
    assert "missing_language_code" in names

    missing_rule = next(rule for rule in rules if rule["rule_name"] == "missing_language_code")
    assert missing_rule["condition_type"] == "missing_field"
    assert missing_rule["duckdb_condition"] == "language_code IS NULL OR TRIM(language_code) = ''"


def test_perl_rule_files_are_present() -> None:
    rule_files = sorted(Path(RULE_FILES_DIR).glob("*.pl"))
    assert len(rule_files) >= 8
