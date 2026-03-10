from extractor.perl_logic_extractor import extract_rules
from migration.llm_converter import convert_rules
from perl_checks.legacy_checks import LEGACY_RULES, get_perl_rule_snippets
from python_checks.generated_checks import compile_generated_checks


def test_deterministic_conversion_behaves_like_expected_templates() -> None:
    structured_rules = extract_rules(get_perl_rule_snippets(LEGACY_RULES))
    converted_rules = convert_rules(structured_rules, provider="simulated")
    checks, metadata = compile_generated_checks(converted_rules)

    energy_tag = checks["energy_kcal_vs_kj"]({"energy_kcal": 200.0, "energy_kj": 100.0})
    assert energy_tag == "energy-value-in-kcal-greater-than-in-kj"
    assert checks["energy_kcal_vs_kj"]({"energy_kcal": 50.0, "energy_kj": 100.0}) is None

    sugars_tag = checks["sugars_over_105g"]({"sugars": 106.0})
    assert sugars_tag == "sugars-value-over-105g"
    assert checks["sugars_over_105g"]({"sugars": 104.9}) is None

    missing_tag = checks["missing_language_code"]({"language_code": "   "})
    assert missing_tag == "missing-language-code"
    assert checks["missing_language_code"]({"language_code": "en"}) is None

    assert metadata["energy_kcal_vs_kj"]["provider"] == "simulated"
