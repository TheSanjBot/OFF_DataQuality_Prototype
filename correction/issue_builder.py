"""Build correction issue rows from the existing validation output."""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Sequence

from correction.schemas import IssueRecord
from duckdb_utils.create_tables import TABLE_NAME, connect
from perl_checks.legacy_checks import LEGACY_RULES
from validation.engine_comparison import COMPARISON_PATH

KNOWN_FIELDS = (
    "energy_kj",
    "energy_kj_computed",
    "energy_kcal",
    "fat",
    "saturated_fat",
    "carbohydrates",
    "sugars",
    "starch",
    "sodium",
    "ingredients_text",
    "ingredients_text_present",
    "contains_statement_present",
    "allergen_evidence_present",
    "fop_threshold_exceeded",
    "fop_symbol_present",
    "fop_exempt_proxy",
    "product_is_prepackaged_proxy",
    "lc",
    "lang",
    "language_code",
)

LEGACY_RULE_MAP = {rule.rule_name: rule for rule in LEGACY_RULES}


def load_comparison_report(path: Path = COMPARISON_PATH) -> Dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _fetch_rule_violations(condition_sql: str, db_path: Path, limit: int | None = None) -> List[Dict[str, object]]:
    query = f"SELECT * FROM {TABLE_NAME} WHERE {condition_sql} ORDER BY product_id"
    if limit is not None:
        query += f" LIMIT {int(limit)}"
    with connect(db_path) as con:
        rows = con.execute(query).fetchdf()
    return rows.to_dict("records")


def _fields_from_condition(condition: str, required_fields: Sequence[str]) -> List[str]:
    selected = list(dict.fromkeys(str(field) for field in required_fields if field))
    for field in KNOWN_FIELDS:
        if re.search(rf"\b{re.escape(field)}\b", condition):
            selected.append(field)
    if "lc" in condition or "lang" in condition:
        selected.extend(["lc", "lang", "language_code"])
    unique = list(dict.fromkeys(selected))
    return unique or ["product_id"]


def _format_value(value: object) -> str:
    if isinstance(value, float):
        return f"{value:.3f}".rstrip("0").rstrip(".")
    if value is None:
        return "blank"
    return str(value)


def _format_current_values(current_values: Mapping[str, object]) -> str:
    return ", ".join(f"{field}={_format_value(value)}" for field, value in current_values.items())


def _rule_explanation(rule_row: Mapping[str, object]) -> str:
    rule_name = str(rule_row.get("rule_name", ""))
    condition = str(rule_row.get("condition", ""))
    if rule_name.endswith("_over_105g"):
        field = rule_name.replace("_over_105g", "").replace("_", " ")
        return f"{field.title()} should not be greater than 105."
    if rule_name == "energy_kj_over_3911":
        return "Energy in kJ should stay at or below 3911."
    explanations = {
        "energy_kcal_vs_kj": "Energy in kcal should not exceed the recorded energy in kJ.",
        "energy_kj_mismatch_low": "Energy in kJ looks too low compared with the kcal value.",
        "energy_kj_mismatch_high": "Energy in kJ looks too high compared with the kcal value.",
        "energy_kj_computed_mismatch_low": "Computed energy from other nutrients looks too low compared with the declared kJ value.",
        "energy_kj_computed_mismatch_high": "Computed energy from other nutrients looks too high compared with the declared kJ value.",
        "saturated_fat_vs_fat": "Saturated fat should not be greater than total fat.",
        "sugars_plus_starch_vs_carbohydrates": "Sugars plus starch should not exceed total carbohydrates.",
        "main_language_code_missing": "The main language code is missing.",
        "main_language_missing": "The main language is missing.",
        "ca_allergen_evidence_missing_ingredients_text": "Allergen evidence is present, but ingredients text is missing.",
        "ca_contains_statement_without_allergen_evidence": "A contains statement exists without recorded allergen evidence.",
        "ca_fop_required_but_symbol_missing": "A front-of-pack symbol seems required but is missing.",
        "ca_fop_symbol_present_but_not_required": "A front-of-pack symbol is present even though it does not appear required.",
        "ca_fop_symbol_present_on_exempt_product": "A front-of-pack symbol is present on a product marked exempt.",
    }
    return explanations.get(rule_name, f"Check whether this product violates: {condition}.")


def _issue_description(rule_row: Mapping[str, object], product: Mapping[str, object], current_values: Mapping[str, object]) -> str:
    rule_name = str(rule_row.get("rule_name", ""))
    condition = str(rule_row.get("condition", ""))

    if rule_name == "main_language_code_missing":
        return "Main language code is blank for this product."
    if rule_name == "main_language_missing":
        return "Main language is blank for this product."
    if rule_name.endswith("_over_105g"):
        field = rule_name.replace("_over_105g", "").replace("_", " ")
        value = next(iter(current_values.values()), product.get(field.replace(" ", "_")))
        return f"{field.title()} is {_format_value(value)}, above the allowed maximum of 105."
    if rule_name == "energy_kj_over_3911":
        return f"Energy in kJ is {_format_value(product.get('energy_kj'))}, above the 3911 upper limit."
    if rule_name == "energy_kcal_vs_kj":
        return (
            f"Energy in kcal ({_format_value(product.get('energy_kcal'))}) is greater than energy in kJ "
            f"({_format_value(product.get('energy_kj'))})."
        )
    if rule_name == "saturated_fat_vs_fat":
        return (
            f"Saturated fat ({_format_value(product.get('saturated_fat'))}) is greater than total fat "
            f"({_format_value(product.get('fat'))})."
        )
    if rule_name == "sugars_plus_starch_vs_carbohydrates":
        return (
            f"Sugars + starch ({_format_value(product.get('sugars'))} + {_format_value(product.get('starch'))}) "
            f"exceeds carbohydrates ({_format_value(product.get('carbohydrates'))})."
        )
    if "mismatch" in rule_name:
        return f"The recorded nutrition values do not fit the expected relationship: {condition}."
    return f"This product violates the rule: {condition}."


def build_issues_from_comparison(
    comparison_report: Mapping[str, object] | None = None,
    comparison_path: Path = COMPARISON_PATH,
    db_path: Path | None = None,
    max_issues_per_rule: int | None = None,
) -> List[IssueRecord]:
    report = dict(comparison_report or load_comparison_report(comparison_path))
    report_db_path = str(report.get("dataset", {}).get("duckdb_path", "")).strip()
    resolved_db_path = db_path or (Path(report_db_path) if report_db_path else None)
    if resolved_db_path is None:
        raise ValueError("DuckDB path is required to build correction issues.")

    issues: List[IssueRecord] = []
    for rule_row in report.get("rule_comparison", []):
        rule_name = str(rule_row.get("rule_name", ""))
        legacy_rule = LEGACY_RULE_MAP.get(rule_name)
        if legacy_rule is None:
            continue
        violating_products = _fetch_rule_violations(
            condition_sql=legacy_rule.duckdb_condition,
            db_path=resolved_db_path,
            limit=max_issues_per_rule,
        )
        if not violating_products:
            continue

        fields = _fields_from_condition(
            condition=str(rule_row.get("condition", "")),
            required_fields=rule_row.get("required_fields", []),
        )
        best_engine = str(rule_row.get("best_engine", "python"))
        best_engine_row = dict(rule_row.get("engines", {}).get(best_engine, {}))
        confidence = float(best_engine_row.get("effective_confidence", 0.0))

        for product in violating_products:
            product_id = str(product.get("product_id", ""))
            current_values = {field: product.get(field) for field in fields if field in product}
            issue_key = f"{rule_name}:{product_id}"
            issue_id = hashlib.sha1(issue_key.encode("utf-8")).hexdigest()[:12]
            issues.append(
                IssueRecord(
                    issue_id=issue_id,
                    product_id=product_id,
                    rule_name=rule_name,
                    tag=str(rule_row.get("tag", "")),
                    severity=str(rule_row.get("severity", "warning")),
                    jurisdiction=str(rule_row.get("jurisdiction", "global")),
                    condition=str(rule_row.get("condition", "")),
                    condition_type=str(rule_row.get("condition_type", "unknown")),
                    issue_description=_issue_description(rule_row, product, current_values),
                    rule_explanation=_rule_explanation(rule_row),
                    current_values=current_values,
                    current_values_display=_format_current_values(current_values),
                    confidence=confidence,
                    migration_state=str(rule_row.get("migration_state", "review")),
                    migration_state_reason=str(rule_row.get("migration_state_reason", "")),
                    best_engine=best_engine,
                    rule_ir_hash=str(rule_row.get("rule_ir_hash", "")),
                )
            )
    return issues


def issues_to_jsonable(issues: Iterable[IssueRecord]) -> List[Dict[str, object]]:
    return [issue.to_dict() for issue in issues]
