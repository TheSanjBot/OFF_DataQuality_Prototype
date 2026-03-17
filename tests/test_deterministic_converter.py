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

    low_energy_tag = checks["energy_kj_mismatch_low"]({"energy_kj": 300.0, "energy_kcal": 100.0})
    assert low_energy_tag == "energy-value-in-kcal-does-not-match-value-in-kj-low"
    assert checks["energy_kj_mismatch_low"]({"energy_kj": 380.0, "energy_kcal": 100.0}) is None

    high_energy_tag = checks["energy_kj_mismatch_high"]({"energy_kj": 500.0, "energy_kcal": 100.0})
    assert high_energy_tag == "energy-value-in-kcal-does-not-match-value-in-kj-high"
    assert checks["energy_kj_mismatch_high"]({"energy_kj": 450.0, "energy_kcal": 100.0}) is None

    computed_low_tag = checks["energy_kj_computed_mismatch_low"]({"energy_kj_computed": 60.0, "energy_kj": 100.0})
    assert computed_low_tag == "energy-value-in-kj-does-not-match-value-computed-from-other-nutrients-low"
    assert checks["energy_kj_computed_mismatch_low"]({"energy_kj_computed": 80.0, "energy_kj": 100.0}) is None

    computed_high_tag = checks["energy_kj_computed_mismatch_high"]({"energy_kj_computed": 150.0, "energy_kj": 100.0})
    assert computed_high_tag == "energy-value-in-kj-does-not-match-value-computed-from-other-nutrients-high"
    assert checks["energy_kj_computed_mismatch_high"]({"energy_kj_computed": 130.0, "energy_kj": 100.0}) is None

    sugar_starch_tag = checks["sugars_plus_starch_vs_carbohydrates"](
        {"sugars": 12.0, "starch": 8.2, "carbohydrates": 20.0}
    )
    assert sugar_starch_tag == "sugars-plus-starch-greater-than-carbohydrates"
    assert (
        checks["sugars_plus_starch_vs_carbohydrates"]({"sugars": 8.0, "starch": 6.0, "carbohydrates": 20.0}) is None
    )

    missing_lc_tag = checks["main_language_code_missing"]({"lc": "   "})
    assert missing_lc_tag == "main-language-code-missing"
    assert checks["main_language_code_missing"]({"lc": "en"}) is None

    missing_lang_tag = checks["main_language_missing"]({"lang": ""})
    assert missing_lang_tag == "main-language-missing"
    assert checks["main_language_missing"]({"lang": "en"}) is None

    ca_allergen_tag = checks["ca_allergen_evidence_missing_ingredients_text"](
        {"allergen_evidence_present": 1, "ingredients_text_present": 0}
    )
    assert ca_allergen_tag == "ca-allergen-evidence-but-missing-ingredients-text"
    assert (
        checks["ca_allergen_evidence_missing_ingredients_text"](
            {"allergen_evidence_present": 1, "ingredients_text_present": 1}
        )
        is None
    )

    ca_fop_missing_tag = checks["ca_fop_required_but_symbol_missing"](
        {
            "fop_threshold_exceeded": 1,
            "fop_symbol_present": 0,
            "fop_exempt_proxy": 0,
            "product_is_prepackaged_proxy": 1,
        }
    )
    assert ca_fop_missing_tag == "ca-fop-required-but-symbol-missing"
    assert (
        checks["ca_fop_required_but_symbol_missing"](
            {
                "fop_threshold_exceeded": 1,
                "fop_symbol_present": 1,
                "fop_exempt_proxy": 0,
                "product_is_prepackaged_proxy": 1,
            }
        )
        is None
    )

    assert metadata["energy_kcal_vs_kj"]["provider"] == "simulated"
