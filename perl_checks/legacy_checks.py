"""Simulated legacy Perl checks for migration parity testing."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Mapping, Sequence, Set

Product = Mapping[str, object]


@dataclass(frozen=True)
class LegacyRule:
    rule_name: str
    tag: str
    severity: str
    condition: str
    duckdb_condition: str
    complexity: str
    declarative_friendly: bool
    perl_logic: str
    evaluator: Callable[[Product], bool]


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _greater_than(left_value: object, right_value: object) -> bool:
    left = _to_float(left_value)
    right = _to_float(right_value)
    return left is not None and right is not None and left > right


def _greater_than_plus_offset(left_value: object, right_value: object, offset: float) -> bool:
    left = _to_float(left_value)
    right = _to_float(right_value)
    return left is not None and right is not None and left > (right + offset)


def _affine_compare(
    left_value: object,
    right_value: object,
    operator: str,
    factor: float,
    offset: float,
) -> bool:
    left = _to_float(left_value)
    right = _to_float(right_value)
    if left is None or right is None:
        return False
    target = (factor * right) + offset
    if operator == ">":
        return left > target
    if operator == "<":
        return left < target
    if operator == ">=":
        return left >= target
    if operator == "<=":
        return left <= target
    if operator == "==":
        return left == target
    if operator == "!=":
        return left != target
    return False


def _sum_compare(
    left_a: object,
    left_b: object,
    operator: str,
    right: object,
    right_offset: float,
) -> bool:
    left_a_num = _to_float(left_a)
    left_b_num = _to_float(left_b)
    right_num = _to_float(right)
    if left_a_num is None or left_b_num is None or right_num is None:
        return False
    left_sum = left_a_num + left_b_num
    right_value = right_num + right_offset
    if operator == ">":
        return left_sum > right_value
    if operator == "<":
        return left_sum < right_value
    if operator == ">=":
        return left_sum >= right_value
    if operator == "<=":
        return left_sum <= right_value
    if operator == "==":
        return left_sum == right_value
    if operator == "!=":
        return left_sum != right_value
    return False


def _is_missing(value: object) -> bool:
    return value is None or str(value).strip() == ""


def _compare_values(left_value: object, operator: str, right_value: object) -> bool:
    left = _to_float(left_value)
    right = _to_float(right_value)
    if left is None or right is None:
        return False
    if operator == ">":
        return left > right
    if operator == "<":
        return left < right
    if operator == ">=":
        return left >= right
    if operator == "<=":
        return left <= right
    if operator == "==":
        return left == right
    if operator == "!=":
        return left != right
    return False


LEGACY_RULES: List[LegacyRule] = [
    LegacyRule(
        rule_name="energy_kcal_vs_kj",
        tag="energy-value-in-kcal-greater-than-in-kj",
        severity="error",
        condition="energy_kcal > energy_kj",
        duckdb_condition="energy_kcal > energy_kj",
        complexity="simple",
        declarative_friendly=True,
        perl_logic="""
# RULE_NAME: energy_kcal_vs_kj
# SEVERITY: error
# COMPLEXITY: simple
# DECLARATIVE_FRIENDLY: yes
if ($energy_kcal > $energy_kj) {
    push @{$product_ref->{$data_quality_tags}}, "energy-value-in-kcal-greater-than-in-kj";
}
""".strip(),
        evaluator=lambda product: _greater_than(product.get("energy_kcal"), product.get("energy_kj")),
    ),
    LegacyRule(
        rule_name="energy_kj_mismatch_low",
        tag="energy-value-in-kcal-does-not-match-value-in-kj-low",
        severity="error",
        condition="energy_kj < (3.7 * energy_kcal - 2)",
        duckdb_condition="energy_kj < (3.7 * energy_kcal - 2)",
        complexity="intricate",
        declarative_friendly=False,
        perl_logic="""
# RULE_NAME: energy_kj_mismatch_low
# SEVERITY: error
# COMPLEXITY: intricate
# DECLARATIVE_FRIENDLY: no
if ($energy_kj < (3.7 * $energy_kcal - 2)) {
    push @{$product_ref->{$data_quality_tags}}, "energy-value-in-kcal-does-not-match-value-in-kj-low";
}
""".strip(),
        evaluator=lambda product: _affine_compare(
            product.get("energy_kj"),
            product.get("energy_kcal"),
            operator="<",
            factor=3.7,
            offset=-2.0,
        ),
    ),
    LegacyRule(
        rule_name="energy_kj_mismatch_high",
        tag="energy-value-in-kcal-does-not-match-value-in-kj-high",
        severity="error",
        condition="energy_kj > (4.7 * energy_kcal + 2)",
        duckdb_condition="energy_kj > (4.7 * energy_kcal + 2)",
        complexity="intricate",
        declarative_friendly=False,
        perl_logic="""
# RULE_NAME: energy_kj_mismatch_high
# SEVERITY: error
# COMPLEXITY: intricate
# DECLARATIVE_FRIENDLY: no
if ($energy_kj > (4.7 * $energy_kcal + 2)) {
    push @{$product_ref->{$data_quality_tags}}, "energy-value-in-kcal-does-not-match-value-in-kj-high";
}
""".strip(),
        evaluator=lambda product: _affine_compare(
            product.get("energy_kj"),
            product.get("energy_kcal"),
            operator=">",
            factor=4.7,
            offset=2.0,
        ),
    ),
    LegacyRule(
        rule_name="energy_kj_over_3911",
        tag="value-over-3911-energy",
        severity="error",
        condition="energy_kj > 3911",
        duckdb_condition="energy_kj > 3911",
        complexity="simple",
        declarative_friendly=True,
        perl_logic="""
# RULE_NAME: energy_kj_over_3911
# SEVERITY: error
# COMPLEXITY: simple
# DECLARATIVE_FRIENDLY: yes
if ($energy_kj > 3911) {
    push @{$product_ref->{$data_quality_tags}}, "value-over-3911-energy";
}
""".strip(),
        evaluator=lambda product: _greater_than(product.get("energy_kj"), 3911),
    ),
    LegacyRule(
        rule_name="energy_kj_computed_mismatch_low",
        tag="energy-value-in-kj-does-not-match-value-computed-from-other-nutrients-low",
        severity="error",
        condition="energy_kj_computed < (0.7 * energy_kj - 5)",
        duckdb_condition="energy_kj_computed < (0.7 * energy_kj - 5)",
        complexity="intricate",
        declarative_friendly=False,
        perl_logic="""
# RULE_NAME: energy_kj_computed_mismatch_low
# SEVERITY: error
# COMPLEXITY: intricate
# DECLARATIVE_FRIENDLY: no
if ($energy_kj_computed < (0.7 * $energy_kj - 5)) {
    push @{$product_ref->{$data_quality_tags}}, "energy-value-in-kj-does-not-match-value-computed-from-other-nutrients-low";
}
""".strip(),
        evaluator=lambda product: _affine_compare(
            product.get("energy_kj_computed"),
            product.get("energy_kj"),
            operator="<",
            factor=0.7,
            offset=-5.0,
        ),
    ),
    LegacyRule(
        rule_name="energy_kj_computed_mismatch_high",
        tag="energy-value-in-kj-does-not-match-value-computed-from-other-nutrients-high",
        severity="error",
        condition="energy_kj_computed > (1.3 * energy_kj + 5)",
        duckdb_condition="energy_kj_computed > (1.3 * energy_kj + 5)",
        complexity="intricate",
        declarative_friendly=False,
        perl_logic="""
# RULE_NAME: energy_kj_computed_mismatch_high
# SEVERITY: error
# COMPLEXITY: intricate
# DECLARATIVE_FRIENDLY: no
if ($energy_kj_computed > (1.3 * $energy_kj + 5)) {
    push @{$product_ref->{$data_quality_tags}}, "energy-value-in-kj-does-not-match-value-computed-from-other-nutrients-high";
}
""".strip(),
        evaluator=lambda product: _affine_compare(
            product.get("energy_kj_computed"),
            product.get("energy_kj"),
            operator=">",
            factor=1.3,
            offset=5.0,
        ),
    ),
    LegacyRule(
        rule_name="saturated_fat_vs_fat",
        tag="saturated-fat-greater-than-fat",
        severity="error",
        condition="saturated_fat > (1 * fat + 0.001)",
        duckdb_condition="saturated_fat > (1 * fat + 0.001)",
        complexity="simple",
        declarative_friendly=True,
        perl_logic="""
# RULE_NAME: saturated_fat_vs_fat
# SEVERITY: error
# COMPLEXITY: simple
# DECLARATIVE_FRIENDLY: yes
if ($saturated_fat > (1 * $fat + 0.001)) {
    push @{$product_ref->{$data_quality_tags}}, "saturated-fat-greater-than-fat";
}
""".strip(),
        evaluator=lambda product: _greater_than_plus_offset(product.get("saturated_fat"), product.get("fat"), 0.001),
    ),
    LegacyRule(
        rule_name="sugars_plus_starch_vs_carbohydrates",
        tag="sugars-plus-starch-greater-than-carbohydrates",
        severity="error",
        condition="(sugars + starch) > (carbohydrates + 0.001)",
        duckdb_condition="(sugars + starch) > (carbohydrates + 0.001)",
        complexity="medium",
        declarative_friendly=True,
        perl_logic="""
# RULE_NAME: sugars_plus_starch_vs_carbohydrates
# SEVERITY: error
# COMPLEXITY: medium
# DECLARATIVE_FRIENDLY: yes
if (($sugars + $starch) > ($carbohydrates + 0.001)) {
    push @{$product_ref->{$data_quality_tags}}, "sugars-plus-starch-greater-than-carbohydrates";
}
""".strip(),
        evaluator=lambda product: _sum_compare(
            product.get("sugars"),
            product.get("starch"),
            operator=">",
            right=product.get("carbohydrates"),
            right_offset=0.001,
        ),
    ),
    LegacyRule(
        rule_name="fat_over_105g",
        tag="fat-value-over-105g",
        severity="warning",
        condition="fat > 105",
        duckdb_condition="fat > 105",
        complexity="simple",
        declarative_friendly=True,
        perl_logic="""
# RULE_NAME: fat_over_105g
# SEVERITY: warning
# COMPLEXITY: simple
# DECLARATIVE_FRIENDLY: yes
if ($fat > 105) {
    push @{$product_ref->{$data_quality_tags}}, "fat-value-over-105g";
}
""".strip(),
        evaluator=lambda product: _greater_than(product.get("fat"), 105),
    ),
    LegacyRule(
        rule_name="saturated_fat_over_105g",
        tag="saturated-fat-value-over-105g",
        severity="warning",
        condition="saturated_fat > 105",
        duckdb_condition="saturated_fat > 105",
        complexity="simple",
        declarative_friendly=True,
        perl_logic="""
# RULE_NAME: saturated_fat_over_105g
# SEVERITY: warning
# COMPLEXITY: simple
# DECLARATIVE_FRIENDLY: yes
if ($saturated_fat > 105) {
    push @{$product_ref->{$data_quality_tags}}, "saturated-fat-value-over-105g";
}
""".strip(),
        evaluator=lambda product: _greater_than(product.get("saturated_fat"), 105),
    ),
    LegacyRule(
        rule_name="carbohydrates_over_105g",
        tag="carbohydrates-value-over-105g",
        severity="warning",
        condition="carbohydrates > 105",
        duckdb_condition="carbohydrates > 105",
        complexity="simple",
        declarative_friendly=True,
        perl_logic="""
# RULE_NAME: carbohydrates_over_105g
# SEVERITY: warning
# COMPLEXITY: simple
# DECLARATIVE_FRIENDLY: yes
if ($carbohydrates > 105) {
    push @{$product_ref->{$data_quality_tags}}, "carbohydrates-value-over-105g";
}
""".strip(),
        evaluator=lambda product: _greater_than(product.get("carbohydrates"), 105),
    ),
    LegacyRule(
        rule_name="sugars_over_105g",
        tag="sugars-value-over-105g",
        severity="warning",
        condition="sugars > 105",
        duckdb_condition="sugars > 105",
        complexity="simple",
        declarative_friendly=True,
        perl_logic="""
# RULE_NAME: sugars_over_105g
# SEVERITY: warning
# COMPLEXITY: simple
# DECLARATIVE_FRIENDLY: yes
if ($sugars > 105) {
    push @{$product_ref->{$data_quality_tags}}, "sugars-value-over-105g";
}
""".strip(),
        evaluator=lambda product: _greater_than(product.get("sugars"), 105),
    ),
    LegacyRule(
        rule_name="main_language_code_missing",
        tag="main-language-code-missing",
        severity="bug",
        condition="missing(lc)",
        duckdb_condition="lc IS NULL OR TRIM(lc) = ''",
        complexity="simple",
        declarative_friendly=True,
        perl_logic="""
# RULE_NAME: main_language_code_missing
# SEVERITY: bug
# COMPLEXITY: simple
# DECLARATIVE_FRIENDLY: yes
if (!defined $lc || $lc eq "") {
    push @{$product_ref->{$data_quality_tags}}, "main-language-code-missing";
}
""".strip(),
        evaluator=lambda product: _is_missing(product.get("lc")),
    ),
    LegacyRule(
        rule_name="main_language_missing",
        tag="main-language-missing",
        severity="bug",
        condition="missing(lang)",
        duckdb_condition="lang IS NULL OR TRIM(lang) = ''",
        complexity="simple",
        declarative_friendly=True,
        perl_logic="""
# RULE_NAME: main_language_missing
# SEVERITY: bug
# COMPLEXITY: simple
# DECLARATIVE_FRIENDLY: yes
if (!defined $lang || $lang eq "") {
    push @{$product_ref->{$data_quality_tags}}, "main-language-missing";
}
        """.strip(),
        evaluator=lambda product: _is_missing(product.get("lang")),
    ),
    LegacyRule(
        rule_name="ca_allergen_evidence_missing_ingredients_text",
        tag="ca-allergen-evidence-but-missing-ingredients-text",
        severity="warning",
        condition="allergen_evidence_present > 0 && ingredients_text_present == 0",
        duckdb_condition="allergen_evidence_present > 0 AND ingredients_text_present == 0",
        complexity="medium",
        declarative_friendly=True,
        perl_logic="""
# RULE_NAME: ca_allergen_evidence_missing_ingredients_text
# SEVERITY: warning
# COMPLEXITY: medium
# DECLARATIVE_FRIENDLY: yes
if (($allergen_evidence_present > 0) && ($ingredients_text_present == 0)) {
    push @{$product_ref->{$data_quality_tags}}, "ca-allergen-evidence-but-missing-ingredients-text";
}
""".strip(),
        evaluator=lambda product: (
            _compare_values(product.get("allergen_evidence_present"), ">", 0)
            and _compare_values(product.get("ingredients_text_present"), "==", 0)
        ),
    ),
    LegacyRule(
        rule_name="ca_contains_statement_without_allergen_evidence",
        tag="ca-contains-statement-without-allergen-evidence",
        severity="warning",
        condition="contains_statement_present > 0 && allergen_evidence_present == 0",
        duckdb_condition="contains_statement_present > 0 AND allergen_evidence_present == 0",
        complexity="medium",
        declarative_friendly=True,
        perl_logic="""
# RULE_NAME: ca_contains_statement_without_allergen_evidence
# SEVERITY: warning
# COMPLEXITY: medium
# DECLARATIVE_FRIENDLY: yes
if (($contains_statement_present > 0) && ($allergen_evidence_present == 0)) {
    push @{$product_ref->{$data_quality_tags}}, "ca-contains-statement-without-allergen-evidence";
}
""".strip(),
        evaluator=lambda product: (
            _compare_values(product.get("contains_statement_present"), ">", 0)
            and _compare_values(product.get("allergen_evidence_present"), "==", 0)
        ),
    ),
    LegacyRule(
        rule_name="ca_fop_required_but_symbol_missing",
        tag="ca-fop-required-but-symbol-missing",
        severity="error",
        condition="fop_threshold_exceeded > 0 && fop_symbol_present == 0 && fop_exempt_proxy == 0 && product_is_prepackaged_proxy > 0",
        duckdb_condition=(
            "fop_threshold_exceeded > 0 AND fop_symbol_present == 0 "
            "AND fop_exempt_proxy == 0 AND product_is_prepackaged_proxy > 0"
        ),
        complexity="medium",
        declarative_friendly=True,
        perl_logic="""
# RULE_NAME: ca_fop_required_but_symbol_missing
# SEVERITY: error
# COMPLEXITY: medium
# DECLARATIVE_FRIENDLY: yes
if (($fop_threshold_exceeded > 0) && ($fop_symbol_present == 0) && ($fop_exempt_proxy == 0) && ($product_is_prepackaged_proxy > 0)) {
    push @{$product_ref->{$data_quality_tags}}, "ca-fop-required-but-symbol-missing";
}
""".strip(),
        evaluator=lambda product: (
            _compare_values(product.get("fop_threshold_exceeded"), ">", 0)
            and _compare_values(product.get("fop_symbol_present"), "==", 0)
            and _compare_values(product.get("fop_exempt_proxy"), "==", 0)
            and _compare_values(product.get("product_is_prepackaged_proxy"), ">", 0)
        ),
    ),
    LegacyRule(
        rule_name="ca_fop_symbol_present_but_not_required",
        tag="ca-fop-symbol-present-but-not-required",
        severity="warning",
        condition="fop_symbol_present > 0 && fop_threshold_exceeded == 0 && fop_exempt_proxy == 0 && product_is_prepackaged_proxy > 0",
        duckdb_condition=(
            "fop_symbol_present > 0 AND fop_threshold_exceeded == 0 "
            "AND fop_exempt_proxy == 0 AND product_is_prepackaged_proxy > 0"
        ),
        complexity="medium",
        declarative_friendly=True,
        perl_logic="""
# RULE_NAME: ca_fop_symbol_present_but_not_required
# SEVERITY: warning
# COMPLEXITY: medium
# DECLARATIVE_FRIENDLY: yes
if (($fop_symbol_present > 0) && ($fop_threshold_exceeded == 0) && ($fop_exempt_proxy == 0) && ($product_is_prepackaged_proxy > 0)) {
    push @{$product_ref->{$data_quality_tags}}, "ca-fop-symbol-present-but-not-required";
}
""".strip(),
        evaluator=lambda product: (
            _compare_values(product.get("fop_symbol_present"), ">", 0)
            and _compare_values(product.get("fop_threshold_exceeded"), "==", 0)
            and _compare_values(product.get("fop_exempt_proxy"), "==", 0)
            and _compare_values(product.get("product_is_prepackaged_proxy"), ">", 0)
        ),
    ),
    LegacyRule(
        rule_name="ca_fop_symbol_present_on_exempt_product",
        tag="ca-fop-symbol-present-on-exempt-product",
        severity="warning",
        condition="fop_symbol_present > 0 && fop_exempt_proxy > 0 && product_is_prepackaged_proxy > 0",
        duckdb_condition="fop_symbol_present > 0 AND fop_exempt_proxy > 0 AND product_is_prepackaged_proxy > 0",
        complexity="medium",
        declarative_friendly=True,
        perl_logic="""
# RULE_NAME: ca_fop_symbol_present_on_exempt_product
# SEVERITY: warning
# COMPLEXITY: medium
# DECLARATIVE_FRIENDLY: yes
if (($fop_symbol_present > 0) && ($fop_exempt_proxy > 0) && ($product_is_prepackaged_proxy > 0)) {
    push @{$product_ref->{$data_quality_tags}}, "ca-fop-symbol-present-on-exempt-product";
}
""".strip(),
        evaluator=lambda product: (
            _compare_values(product.get("fop_symbol_present"), ">", 0)
            and _compare_values(product.get("fop_exempt_proxy"), ">", 0)
            and _compare_values(product.get("product_is_prepackaged_proxy"), ">", 0)
        ),
    ),
]


RULE_FILES_DIR = Path(__file__).resolve().parent / "rules"


def load_rule_snippets_from_directory(rules_dir: Path = RULE_FILES_DIR) -> List[str]:
    snippets: List[str] = []
    for file_path in sorted(rules_dir.glob("*.pl")):
        content = file_path.read_text(encoding="utf-8").lstrip("\ufeff").strip()
        if content:
            snippets.append(content)
    if not snippets:
        raise ValueError(f"No Perl rule files found in {rules_dir}")
    return snippets


def get_perl_rule_snippets(
    rules: Sequence[LegacyRule] = LEGACY_RULES,
    rules_dir: Path | None = None,
) -> List[str]:
    if rules_dir is not None:
        return load_rule_snippets_from_directory(Path(rules_dir))
    return [rule.perl_logic for rule in rules]


def get_legacy_rule_map(rules: Sequence[LegacyRule] = LEGACY_RULES) -> Dict[str, LegacyRule]:
    return {rule.rule_name: rule for rule in rules}


def run_perl_checks(products: Sequence[Product], rules: Sequence[LegacyRule] = LEGACY_RULES) -> Dict[str, Dict[str, object]]:
    """Run simulated Perl checks and return per-product and per-rule outputs."""
    per_product_tags: Dict[str, List[str]] = {}
    per_rule_products: Dict[str, Set[str]] = {rule.rule_name: set() for rule in rules}

    for product in products:
        product_id = str(product.get("product_id"))
        emitted_tags: List[str] = []
        for rule in rules:
            if rule.evaluator(product):
                emitted_tags.append(rule.tag)
                per_rule_products[rule.rule_name].add(product_id)
        per_product_tags[product_id] = emitted_tags

    return {
        "per_product": per_product_tags,
        "per_rule": {name: sorted(ids) for name, ids in per_rule_products.items()},
    }
