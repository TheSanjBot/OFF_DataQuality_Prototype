"""Verification utilities for migrated rule quality.

This module adds:
- rule-level equivalence checks (Perl evaluator vs generated check),
- mutation testing for generated Python checks.
"""
from __future__ import annotations

import json
import random
from typing import Callable, Dict, List, Mapping, Sequence


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _comparison_pairs(operator: str) -> tuple[tuple[float, float], tuple[float, float]]:
    pairs = {
        ">": ((2.0, 1.0), (1.0, 2.0)),
        "<": ((1.0, 2.0), (2.0, 1.0)),
        ">=": ((2.0, 2.0), (1.0, 2.0)),
        "<=": ((2.0, 2.0), (3.0, 2.0)),
        "==": ((2.0, 2.0), (2.0, 3.0)),
        "!=": ((2.0, 3.0), (2.0, 2.0)),
    }
    return pairs.get(operator, ((2.0, 1.0), (1.0, 2.0)))


def _threshold_values(operator: str, threshold: float) -> tuple[float, float]:
    if operator == ">":
        return threshold + 1.0, threshold
    if operator == "<":
        return threshold - 1.0, threshold
    if operator == ">=":
        return threshold, threshold - 1.0
    if operator == "<=":
        return threshold, threshold + 1.0
    if operator == "==":
        return threshold, threshold + 1.0
    if operator == "!=":
        return threshold + 1.0, threshold
    return threshold + 1.0, threshold


def _dedupe_cases(cases: Sequence[Mapping[str, object]]) -> List[Dict[str, object]]:
    deduped: List[Dict[str, object]] = []
    seen: set[str] = set()
    for case in cases:
        payload = dict(case)
        key = json.dumps(payload, sort_keys=True, default=str)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(payload)
    return deduped


def generate_equivalence_cases(
    rule: Mapping[str, object],
    seed: int = 17,
    random_cases: int = 18,
) -> List[Dict[str, object]]:
    rng = random.Random(seed + (sum(ord(ch) for ch in str(rule.get("rule_name", ""))) % 997))
    condition_type = str(rule.get("condition_type", ""))
    cases: List[Dict[str, object]] = []

    if condition_type == "field_comparison":
        left = str(rule.get("left_operand"))
        right = str(rule.get("right_operand"))
        operator = str(rule.get("operator"))
        true_pair, false_pair = _comparison_pairs(operator)
        cases.extend(
            [
                {left: true_pair[0], right: true_pair[1]},
                {left: false_pair[0], right: false_pair[1]},
                {left: true_pair[1], right: true_pair[1]},
                {left: None, right: true_pair[1]},
                {left: true_pair[0], right: None},
            ]
        )
        for _ in range(random_cases):
            cases.append({left: round(rng.uniform(-10, 200), 3), right: round(rng.uniform(-10, 200), 3)})

    elif condition_type == "field_threshold":
        left = str(rule.get("left_operand"))
        operator = str(rule.get("operator"))
        threshold = float(rule.get("right_operand", 0.0))
        true_value, false_value = _threshold_values(operator, threshold)
        cases.extend([{left: true_value}, {left: false_value}, {left: None}, {left: "nan_text"}])
        for _ in range(random_cases):
            cases.append({left: round(rng.uniform(threshold - 100, threshold + 100), 3)})

    elif condition_type == "missing_field":
        field = str(rule.get("left_operand"))
        cases.extend(
            [
                {field: None},
                {field: ""},
                {field: "   "},
                {field: "en"},
                {field: "xx"},
            ]
        )

    elif condition_type == "scaled_field_comparison":
        left = str(rule.get("left_operand"))
        right = str(rule.get("right_operand"))
        operator = str(rule.get("operator"))
        factor = float(rule.get("scale_factor", 1.0))
        right_value = 10.0
        target = right_value * factor
        true_value, false_value = _threshold_values(operator, target)
        cases.extend(
            [
                {left: true_value, right: right_value},
                {left: false_value, right: right_value},
                {left: None, right: right_value},
                {left: true_value, right: None},
            ]
        )
        for _ in range(random_cases):
            random_right = round(rng.uniform(0.1, 150), 3)
            random_target = random_right * factor
            delta = rng.uniform(-10, 10)
            cases.append({left: round(random_target + delta, 3), right: random_right})

    elif condition_type == "affine_field_comparison":
        left = str(rule.get("left_operand"))
        right = str(rule.get("right_operand"))
        operator = str(rule.get("operator"))
        factor = float(rule.get("scale_factor", 1.0))
        offset = float(rule.get("offset", 0.0))
        right_value = 10.0
        target = (factor * right_value) + offset
        true_value, false_value = _threshold_values(operator, target)
        cases.extend(
            [
                {left: true_value, right: right_value},
                {left: false_value, right: right_value},
                {left: None, right: right_value},
                {left: true_value, right: None},
            ]
        )
        for _ in range(random_cases):
            random_right = round(rng.uniform(0.1, 150), 3)
            random_target = (factor * random_right) + offset
            delta = rng.uniform(-10, 10)
            cases.append({left: round(random_target + delta, 3), right: random_right})

    elif condition_type == "sum_fields_comparison":
        left_operands = list(rule.get("left_operands", []))
        if len(left_operands) >= 2:
            left_a = str(left_operands[0])
            left_b = str(left_operands[1])
            right = str(rule.get("right_operand"))
            operator = str(rule.get("operator"))
            right_offset = float(rule.get("right_offset", 0.0))
            right_value = 20.0
            target = right_value + right_offset
            true_sum, false_sum = _threshold_values(operator, target)
            cases.extend(
                [
                    {left_a: true_sum / 2.0, left_b: true_sum / 2.0, right: right_value},
                    {left_a: false_sum / 2.0, left_b: false_sum / 2.0, right: right_value},
                    {left_a: None, left_b: 2.0, right: right_value},
                    {left_a: 2.0, left_b: None, right: right_value},
                ]
            )
            for _ in range(random_cases):
                random_right = round(rng.uniform(0.1, 120), 3)
                random_target = random_right + right_offset
                left_sum = random_target + rng.uniform(-20, 20)
                left_part = round(rng.uniform(0, max(left_sum, 0.1)), 3)
                cases.append(
                    {
                        left_a: left_part,
                        left_b: round(left_sum - left_part, 3),
                        right: random_right,
                    }
                )

    elif condition_type == "compound_threshold_and":
        clauses = list(rule.get("clauses", []))
        passing: Dict[str, object] = {}
        failing: Dict[str, object] = {}
        for idx, clause in enumerate(clauses):
            field = str(clause.get("left_operand"))
            operator = str(clause.get("operator"))
            threshold = float(clause.get("right_operand", 0.0))
            true_value, false_value = _threshold_values(operator, threshold)
            passing[field] = true_value
            failing[field] = true_value
            if idx == 0:
                failing[field] = false_value
        if passing:
            cases.append(passing)
        if failing:
            cases.append(failing)

    return _dedupe_cases(cases)


def evaluate_rule_equivalence(
    rule: Mapping[str, object],
    perl_evaluator: Callable[[Mapping[str, object]], bool],
    check_fn: Callable[[Mapping[str, object]], object],
    seed: int = 17,
    cases: Sequence[Mapping[str, object]] | None = None,
    max_counterexamples: int = 5,
) -> Dict[str, object]:
    sample_cases = list(cases) if cases is not None else generate_equivalence_cases(rule, seed=seed)
    tag = str(rule.get("tag"))
    matches = 0
    mismatches = 0
    counterexamples: List[Dict[str, object]] = []

    for case in sample_cases:
        product = dict(case)
        expected = tag if bool(perl_evaluator(product)) else None
        try:
            actual = check_fn(product)
        except Exception as exc:  # noqa: BLE001
            actual = f"EXCEPTION:{exc.__class__.__name__}"
        if actual == expected:
            matches += 1
        else:
            mismatches += 1
            if len(counterexamples) < max_counterexamples:
                counterexamples.append({"input": product, "expected": expected, "actual": actual})

    total = len(sample_cases)
    rate = (matches / total) if total else 1.0
    return {
        "equivalence_cases": total,
        "equivalence_matches": matches,
        "equivalence_mismatches": mismatches,
        "equivalence_match_rate": round(rate, 4),
        "equivalence_status": "PASS" if mismatches == 0 else "FAIL",
        "counterexamples": counterexamples,
    }


def _mutate_once(code: str, old: str, new: str) -> str | None:
    if old not in code:
        return None
    mutated = code.replace(old, new, 1)
    if mutated == code:
        return None
    return mutated


def build_mutants(rule: Mapping[str, object], python_code: str) -> List[Dict[str, object]]:
    mutants: List[Dict[str, object]] = []
    condition_type = str(rule.get("condition_type", ""))
    operator = str(rule.get("operator", ""))
    operator_swap = {">": ">=", "<": "<=", ">=": ">", "<=": "<", "==": "!=", "!=": "=="}
    swapped = operator_swap.get(operator)
    if swapped:
        mutated = _mutate_once(python_code, f" {operator} ", f" {swapped} ")
        if mutated is not None:
            mutants.append({"name": f"operator_{operator}_to_{swapped}", "code": mutated})

    if condition_type == "missing_field":
        mutated = _mutate_once(python_code, 'or str(value).strip() == ""', 'and str(value).strip() == ""')
        if mutated is not None:
            mutants.append({"name": "missing_logic_or_to_and", "code": mutated})

    if condition_type == "sum_fields_comparison":
        mutated = _mutate_once(python_code, "left_sum = left_a_value + left_b_value", "left_sum = left_a_value - left_b_value")
        if mutated is not None:
            mutants.append({"name": "sum_to_difference", "code": mutated})

    scale_factor = rule.get("scale_factor")
    if isinstance(scale_factor, (int, float)):
        old = str(float(scale_factor))
        new = str(round(float(scale_factor) + 0.3, 6))
        mutated = _mutate_once(python_code, old, new)
        if mutated is not None:
            mutants.append({"name": "scale_factor_perturbed", "code": mutated})

    offset = rule.get("offset")
    if isinstance(offset, (int, float)):
        old = str(float(offset))
        new = str(round(float(offset) + 1.0, 6))
        mutated = _mutate_once(python_code, old, new)
        if mutated is not None:
            mutants.append({"name": "offset_perturbed", "code": mutated})

    threshold = rule.get("right_operand")
    if condition_type == "field_threshold" and isinstance(threshold, (int, float)):
        old = str(float(threshold))
        new = str(round(float(threshold) + 1.0, 6))
        mutated = _mutate_once(python_code, old, new)
        if mutated is not None:
            mutants.append({"name": "threshold_perturbed", "code": mutated})

    deduped: List[Dict[str, object]] = []
    seen: set[str] = set()
    for mutant in mutants:
        code = str(mutant["code"])
        if code in seen:
            continue
        seen.add(code)
        deduped.append(mutant)
    return deduped[:8]


def evaluate_mutation_suite(
    rule: Mapping[str, object],
    perl_evaluator: Callable[[Mapping[str, object]], bool],
    python_code: str,
    function_name: str,
    seed: int = 17,
) -> Dict[str, object]:
    cases = generate_equivalence_cases(rule, seed=seed)
    mutants = build_mutants(rule, python_code)
    total = 0
    killed = 0
    survived: List[str] = []

    for mutant in mutants:
        namespace: Dict[str, object] = {}
        try:
            exec(str(mutant["code"]), {}, namespace)
            fn = namespace.get(function_name)
            if not callable(fn):
                continue
        except Exception:
            continue

        total += 1
        result = evaluate_rule_equivalence(
            rule=rule,
            perl_evaluator=perl_evaluator,
            check_fn=fn,
            seed=seed,
            cases=cases,
            max_counterexamples=1,
        )
        if int(result["equivalence_mismatches"]) > 0:
            killed += 1
        else:
            survived.append(str(mutant["name"]))

    score = (killed / total) if total else 1.0
    return {
        "mutation_total": total,
        "mutation_killed": killed,
        "mutation_survived": total - killed,
        "mutation_score": round(score, 4),
        "mutation_survived_mutants": survived,
    }


def run_rule_verification(
    rule: Mapping[str, object],
    perl_evaluator: Callable[[Mapping[str, object]], bool],
    check_fn: Callable[[Mapping[str, object]], object],
    python_code: str,
    function_name: str,
    seed: int = 17,
) -> Dict[str, object]:
    equivalence = evaluate_rule_equivalence(
        rule=rule,
        perl_evaluator=perl_evaluator,
        check_fn=check_fn,
        seed=seed,
    )
    mutation = evaluate_mutation_suite(
        rule=rule,
        perl_evaluator=perl_evaluator,
        python_code=python_code,
        function_name=function_name,
        seed=seed,
    )
    verification_score = float(equivalence["equivalence_match_rate"]) * float(mutation["mutation_score"])
    return {
        **equivalence,
        **mutation,
        "verification_score": round(verification_score, 4),
    }
