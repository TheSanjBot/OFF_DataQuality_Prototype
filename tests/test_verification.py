from extractor.perl_logic_extractor import extract_rules
from migration.llm_converter import convert_rules
from perl_checks.legacy_checks import LEGACY_RULES, get_legacy_rule_map, get_perl_rule_snippets
from python_checks.generated_checks import compile_generated_checks
from validation.verification import run_rule_verification


def test_rule_verification_for_deterministic_conversion() -> None:
    structured_rules = extract_rules(get_perl_rule_snippets(LEGACY_RULES))
    converted_rules = convert_rules(structured_rules, provider="simulated")
    checks, metadata = compile_generated_checks(converted_rules)
    legacy_map = get_legacy_rule_map(LEGACY_RULES)

    rule = next(rule for rule in structured_rules if rule["rule_name"] == "energy_kcal_vs_kj")
    verification = run_rule_verification(
        rule=rule,
        perl_evaluator=legacy_map["energy_kcal_vs_kj"].evaluator,
        check_fn=checks["energy_kcal_vs_kj"],
        python_code=str(metadata["energy_kcal_vs_kj"]["python_code"]),
        function_name=str(metadata["energy_kcal_vs_kj"]["function_name"]),
        seed=17,
    )

    assert verification["equivalence_mismatches"] == 0
    assert verification["equivalence_status"] == "PASS"
    assert verification["mutation_total"] >= 1
    assert 0.0 <= float(verification["mutation_score"]) <= 1.0
