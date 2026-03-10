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


def _missing_language_code(product: Product) -> bool:
    value = product.get("language_code")
    if value is None:
        return True
    return str(value).strip() == ""


LEGACY_RULES: List[LegacyRule] = [
    LegacyRule(
        rule_name="energy_kcal_vs_kj",
        tag="energy-value-in-kcal-greater-than-in-kj",
        severity="error",
        condition="energy_kcal > energy_kj",
        duckdb_condition="energy_kcal > energy_kj",
        perl_logic="""
# RULE_NAME: energy_kcal_vs_kj
# SEVERITY: error
if ($energy_kcal > $energy_kj) {
    push @{$product_ref->{$data_quality_tags}}, "energy-value-in-kcal-greater-than-in-kj";
}
""".strip(),
        evaluator=lambda product: _greater_than(product.get("energy_kcal"), product.get("energy_kj")),
    ),
    LegacyRule(
        rule_name="sugars_vs_carbohydrates",
        tag="sugars-greater-than-carbohydrates",
        severity="error",
        condition="sugars > carbohydrates",
        duckdb_condition="sugars > carbohydrates",
        perl_logic="""
# RULE_NAME: sugars_vs_carbohydrates
# SEVERITY: error
if ($sugars > $carbohydrates) {
    push @{$product_ref->{$data_quality_tags}}, "sugars-greater-than-carbohydrates";
}
""".strip(),
        evaluator=lambda product: _greater_than(product.get("sugars"), product.get("carbohydrates")),
    ),
    LegacyRule(
        rule_name="saturated_fat_vs_fat",
        tag="saturated-fat-greater-than-fat",
        severity="error",
        condition="saturated_fat > fat",
        duckdb_condition="saturated_fat > fat",
        perl_logic="""
# RULE_NAME: saturated_fat_vs_fat
# SEVERITY: error
if ($saturated_fat > $fat) {
    push @{$product_ref->{$data_quality_tags}}, "saturated-fat-greater-than-fat";
}
""".strip(),
        evaluator=lambda product: _greater_than(product.get("saturated_fat"), product.get("fat")),
    ),
    LegacyRule(
        rule_name="fat_over_105g",
        tag="fat-value-over-105g",
        severity="warning",
        condition="fat > 105",
        duckdb_condition="fat > 105",
        perl_logic="""
# RULE_NAME: fat_over_105g
# SEVERITY: warning
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
        perl_logic="""
# RULE_NAME: saturated_fat_over_105g
# SEVERITY: warning
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
        perl_logic="""
# RULE_NAME: carbohydrates_over_105g
# SEVERITY: warning
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
        perl_logic="""
# RULE_NAME: sugars_over_105g
# SEVERITY: warning
if ($sugars > 105) {
    push @{$product_ref->{$data_quality_tags}}, "sugars-value-over-105g";
}
""".strip(),
        evaluator=lambda product: _greater_than(product.get("sugars"), 105),
    ),
    LegacyRule(
        rule_name="missing_language_code",
        tag="missing-language-code",
        severity="warning",
        condition="missing(language_code)",
        duckdb_condition="language_code IS NULL OR TRIM(language_code) = ''",
        perl_logic="""
# RULE_NAME: missing_language_code
# SEVERITY: warning
if (!defined $language_code || $language_code eq "") {
    push @{$product_ref->{$data_quality_tags}}, "missing-language-code";
}
""".strip(),
        evaluator=_missing_language_code,
    ),
]


RULE_FILES_DIR = Path(__file__).resolve().parent / "rules"


def load_rule_snippets_from_directory(rules_dir: Path = RULE_FILES_DIR) -> List[str]:
    snippets: List[str] = []
    for file_path in sorted(rules_dir.glob("*.pl")):
        content = file_path.read_text(encoding="utf-8").strip()
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
