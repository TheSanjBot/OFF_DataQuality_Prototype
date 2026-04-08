"""Deterministic fix generation for issue rows."""
from __future__ import annotations

from typing import List

from correction.schemas import FixCandidate, IssueRecord

KCAL_TO_KJ = 4.184


def _num(issue: IssueRecord, field: str) -> float | None:
    value = issue.current_values.get(field)
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _text(issue: IssueRecord, field: str) -> str:
    value = issue.current_values.get(field)
    return "" if value is None else str(value)


def _make_fix(
    issue: IssueRecord,
    suffix: str,
    suggested_change: str,
    target_field: str,
    new_value: object,
    confidence: float,
    explanation: str,
    expected_outcome: str,
    fix_strategy: str,
) -> FixCandidate:
    return FixCandidate(
        fix_id=f"{issue.issue_id}_{suffix}",
        suggested_change=suggested_change,
        target_field=target_field,
        new_value=new_value,
        confidence=confidence,
        explanation=explanation,
        expected_outcome=expected_outcome,
        fix_strategy=fix_strategy,
    )


def generate_fix_candidates(issue: IssueRecord) -> List[FixCandidate]:
    rule = issue.rule_name
    fixes: List[FixCandidate] = []

    if rule.endswith("_over_105g"):
        field = rule.replace("_over_105g", "")
        label = field.replace("_", " ")
        fixes.append(
            _make_fix(
                issue,
                "limit",
                f"Set {label} to 105.",
                field,
                105.0,
                0.88,
                "This is the smallest change that brings the value back inside the allowed maximum.",
                "The product should stop triggering the over-105g rule.",
                "clamp_to_limit",
            )
        )
        fixes.append(
            _make_fix(
                issue,
                "clear",
                f"Clear {label} and review the nutrition panel.",
                field,
                None,
                0.58,
                "Use this if the recorded value looks obviously wrong or unverified.",
                "The suspicious value is removed until a verified number is added.",
                "clear_suspicious_value",
            )
        )
    elif rule == "energy_kj_over_3911":
        fixes.append(
            _make_fix(
                issue,
                "limit",
                "Set energy_kj to 3911.",
                "energy_kj",
                3911.0,
                0.87,
                "This directly brings the value back under the allowed maximum.",
                "The product should stop triggering the upper-limit energy rule.",
                "clamp_to_limit",
            )
        )
        fixes.append(
            _make_fix(
                issue,
                "recalc",
                "Recalculate energy_kj from energy_kcal.",
                "energy_kj",
                round((_num(issue, "energy_kcal") or 0.0) * KCAL_TO_KJ, 2),
                0.72,
                "Use this if the kJ value likely drifted away from the kcal declaration.",
                "The kJ value becomes consistent with the kcal field.",
                "derive_from_related_field",
            )
        )
    elif rule == "energy_kcal_vs_kj":
        energy_kj = _num(issue, "energy_kj")
        energy_kcal = _num(issue, "energy_kcal")
        fixes.append(
            _make_fix(
                issue,
                "derive_kcal",
                "Recalculate energy_kcal from energy_kj.",
                "energy_kcal",
                round((energy_kj or 0.0) / KCAL_TO_KJ, 2),
                0.81,
                "This preserves the kJ value and derives the kcal value from it.",
                "Energy in kcal should no longer exceed energy in kJ.",
                "derive_from_related_field",
            )
        )
        fixes.append(
            _make_fix(
                issue,
                "derive_kj",
                "Recalculate energy_kj from energy_kcal.",
                "energy_kj",
                round((energy_kcal or 0.0) * KCAL_TO_KJ, 2),
                0.69,
                "Use this if the kcal number looks more trustworthy than the kJ number.",
                "The two energy fields become unit-consistent.",
                "derive_from_related_field",
            )
        )
    elif rule in {"energy_kj_mismatch_low", "energy_kj_mismatch_high"}:
        energy_kj = _num(issue, "energy_kj")
        energy_kcal = _num(issue, "energy_kcal")
        fixes.append(
            _make_fix(
                issue,
                "align_kj",
                "Align energy_kj with the kcal value.",
                "energy_kj",
                round((energy_kcal or 0.0) * KCAL_TO_KJ, 2),
                0.84,
                "This uses the standard kcal-to-kJ conversion and is usually the most direct repair.",
                "The energy relationship should fall back inside the expected range.",
                "derive_from_related_field",
            )
        )
        fixes.append(
            _make_fix(
                issue,
                "align_kcal",
                "Align energy_kcal with the kJ value.",
                "energy_kcal",
                round((energy_kj or 0.0) / KCAL_TO_KJ, 2),
                0.68,
                "Use this if the kJ declaration is trusted and the kcal field seems off.",
                "The energy relationship should become consistent.",
                "derive_from_related_field",
            )
        )
    elif rule in {"energy_kj_computed_mismatch_low", "energy_kj_computed_mismatch_high"}:
        energy_kj = _num(issue, "energy_kj")
        computed = _num(issue, "energy_kj_computed")
        fixes.append(
            _make_fix(
                issue,
                "sync_computed",
                "Set energy_kj_computed to match energy_kj.",
                "energy_kj_computed",
                energy_kj,
                0.82,
                "This is the most direct way to align computed energy with the declared value.",
                "The computed-energy mismatch should disappear.",
                "align_related_fields",
            )
        )
        fixes.append(
            _make_fix(
                issue,
                "sync_declared",
                "Set energy_kj to match energy_kj_computed.",
                "energy_kj",
                computed,
                0.63,
                "Use this if the computed value is believed to be more reliable than the declaration.",
                "The declared kJ value should align with the computed estimate.",
                "align_related_fields",
            )
        )
    elif rule == "saturated_fat_vs_fat":
        fat = _num(issue, "fat")
        saturated_fat = _num(issue, "saturated_fat")
        fixes.append(
            _make_fix(
                issue,
                "cap_sat_fat",
                "Set saturated_fat to the total fat value.",
                "saturated_fat",
                fat,
                0.86,
                "Saturated fat should not exceed total fat, so this is the safest direct correction.",
                "The product should stop triggering the fat consistency rule.",
                "align_related_fields",
            )
        )
        fixes.append(
            _make_fix(
                issue,
                "raise_fat",
                "Set total fat to the saturated fat value.",
                "fat",
                saturated_fat,
                0.61,
                "Use this if total fat appears under-reported compared with saturated fat.",
                "Total fat becomes at least as large as saturated fat.",
                "align_related_fields",
            )
        )
    elif rule == "sugars_plus_starch_vs_carbohydrates":
        sugars = _num(issue, "sugars") or 0.0
        starch = _num(issue, "starch") or 0.0
        carbs = _num(issue, "carbohydrates") or 0.0
        fixes.append(
            _make_fix(
                issue,
                "raise_carbs",
                "Set carbohydrates to sugars + starch.",
                "carbohydrates",
                round(sugars + starch, 3),
                0.84,
                "This restores the expected nutrition relationship directly.",
                "Sugars plus starch will no longer exceed carbohydrates.",
                "derive_from_related_field",
            )
        )
        fixes.append(
            _make_fix(
                issue,
                "lower_starch",
                "Lower starch so sugars + starch fits within carbohydrates.",
                "starch",
                round(max(carbs - sugars, 0.0), 3),
                0.69,
                "Use this if carbohydrates is trusted and starch looks overstated.",
                "The combined sugars and starch value should fit inside carbohydrates.",
                "rebalance_component",
            )
        )
    elif rule == "main_language_code_missing":
        lang = _text(issue, "lang")
        language_code = _text(issue, "language_code")
        if lang:
            fixes.append(
                _make_fix(
                    issue,
                    "copy_lang",
                    f"Copy lang into lc ({lang}).",
                    "lc",
                    lang,
                    0.86,
                    "The product already has a main language, so copying it into lc is the least disruptive fix.",
                    "The missing main language code should be filled.",
                    "copy_existing_value",
                )
            )
        if language_code:
            fixes.append(
                _make_fix(
                    issue,
                    "copy_language_code",
                    f"Copy language_code into lc ({language_code}).",
                    "lc",
                    language_code,
                    0.74,
                    "Use this if language_code is already the trusted source field.",
                    "The missing main language code should be filled.",
                    "copy_existing_value",
                )
            )
    elif rule == "main_language_missing":
        lc = _text(issue, "lc")
        language_code = _text(issue, "language_code")
        if lc:
            fixes.append(
                _make_fix(
                    issue,
                    "copy_lc",
                    f"Copy lc into lang ({lc}).",
                    "lang",
                    lc,
                    0.86,
                    "The language code is already present, so copying it into the missing field is low risk.",
                    "The missing main language should be filled.",
                    "copy_existing_value",
                )
            )
        if language_code:
            fixes.append(
                _make_fix(
                    issue,
                    "copy_language_code",
                    f"Copy language_code into lang ({language_code}).",
                    "lang",
                    language_code,
                    0.74,
                    "Use this if language_code is already the trusted source field.",
                    "The missing main language should be filled.",
                    "copy_existing_value",
                )
            )
    elif rule == "ca_allergen_evidence_missing_ingredients_text":
        fixes.append(
            _make_fix(
                issue,
                "set_ingredients_present",
                "Mark ingredients_text_present as 1 after confirming ingredients text exists on the label.",
                "ingredients_text_present",
                1,
                0.77,
                "This keeps the allergen evidence and restores the expected supporting ingredients text signal.",
                "The Canada allergen consistency rule should stop triggering.",
                "restore_missing_supporting_field",
            )
        )
        fixes.append(
            _make_fix(
                issue,
                "clear_allergen_evidence",
                "Set allergen_evidence_present to 0 if the allergen evidence was recorded in error.",
                "allergen_evidence_present",
                0,
                0.61,
                "Use this only if the allergen evidence flag is the incorrect field.",
                "The record should no longer be treated as having unsupported allergen evidence.",
                "clear_incorrect_flag",
            )
        )
    elif rule == "ca_contains_statement_without_allergen_evidence":
        fixes.append(
            _make_fix(
                issue,
                "set_allergen_evidence",
                "Set allergen_evidence_present to 1.",
                "allergen_evidence_present",
                1,
                0.79,
                "This is the most direct fix when the contains statement is correct but the evidence flag is missing.",
                "The contains statement and allergen evidence will agree.",
                "restore_missing_supporting_field",
            )
        )
        fixes.append(
            _make_fix(
                issue,
                "clear_contains_statement",
                "Set contains_statement_present to 0 if the contains statement was added by mistake.",
                "contains_statement_present",
                0,
                0.63,
                "Use this if the contains statement should not be present for the product.",
                "The unsupported contains statement will be removed.",
                "clear_incorrect_flag",
            )
        )
    elif rule == "ca_fop_required_but_symbol_missing":
        fixes.append(
            _make_fix(
                issue,
                "set_symbol",
                "Set fop_symbol_present to 1 after confirming the package carries the symbol.",
                "fop_symbol_present",
                1,
                0.81,
                "This is the direct fix when the threshold logic is correct and only the symbol flag is missing.",
                "The missing front-of-pack symbol should be restored.",
                "restore_missing_supporting_field",
            )
        )
        fixes.append(
            _make_fix(
                issue,
                "clear_threshold",
                "Set fop_threshold_exceeded to 0 if the product does not actually require the symbol.",
                "fop_threshold_exceeded",
                0,
                0.57,
                "Use this if the threshold classification is the field that is wrong.",
                "The product will no longer be considered to require the symbol.",
                "clear_incorrect_flag",
            )
        )
    elif rule == "ca_fop_symbol_present_but_not_required":
        fixes.append(
            _make_fix(
                issue,
                "clear_symbol",
                "Set fop_symbol_present to 0.",
                "fop_symbol_present",
                0,
                0.82,
                "This is the safest fix if the symbol should not appear for this product.",
                "The unnecessary front-of-pack symbol will be removed.",
                "clear_incorrect_flag",
            )
        )
        fixes.append(
            _make_fix(
                issue,
                "set_threshold",
                "Set fop_threshold_exceeded to 1 if the product should require the symbol.",
                "fop_threshold_exceeded",
                1,
                0.58,
                "Use this if the symbol is correct and the threshold flag is the missing piece.",
                "The symbol requirement logic will match the symbol presence flag.",
                "restore_missing_supporting_field",
            )
        )
    elif rule == "ca_fop_symbol_present_on_exempt_product":
        fixes.append(
            _make_fix(
                issue,
                "clear_symbol",
                "Set fop_symbol_present to 0.",
                "fop_symbol_present",
                0,
                0.83,
                "If the product is truly exempt, removing the symbol flag is the least disruptive correction.",
                "The product will stop appearing as an exempt item with a symbol.",
                "clear_incorrect_flag",
            )
        )
        fixes.append(
            _make_fix(
                issue,
                "clear_exempt_flag",
                "Set fop_exempt_proxy to 0 if the exemption flag is wrong.",
                "fop_exempt_proxy",
                0,
                0.56,
                "Use this when the symbol is correct and the exemption flag is the mistaken field.",
                "The product will no longer be treated as exempt.",
                "clear_incorrect_flag",
            )
        )

    fixes.append(
        _make_fix(
            issue,
            "manual_review",
            "Keep the row for manual nutrition review.",
            "",
            "",
            0.35,
            "Use this when none of the suggested single-field fixes can be trusted without label review.",
            "The issue stays open for manual resolution.",
            "manual_review_required",
        )
    )
    return fixes[:3]
