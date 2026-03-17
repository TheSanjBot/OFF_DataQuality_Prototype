"""LLM-style conversion from structured rules to Python check code."""
from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass
from typing import Dict, List, Sequence


@dataclass(frozen=True)
class ConversionResult:
    rule_name: str
    function_name: str
    python_code: str
    llm_confidence: float
    conversion_notes: str
    provider: str


def _safe_identifier(name: str) -> str:
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_")
    if not sanitized:
        sanitized = "generated_rule"
    if sanitized[0].isdigit():
        sanitized = f"rule_{sanitized}"
    return sanitized


def _python_literal(value: object) -> str:
    if isinstance(value, str):
        return repr(value)
    if isinstance(value, (int, float)):
        return str(value)
    if value is None:
        return "None"
    return repr(str(value))


def _confidence_for_rule(rule: Dict[str, object]) -> float:
    condition_type = str(rule.get("condition_type", ""))
    if condition_type == "field_comparison":
        return 0.98
    if condition_type == "field_threshold":
        return 0.96
    if condition_type == "missing_field":
        return 0.93
    if condition_type == "sum_fields_comparison":
        return 0.91
    if condition_type == "compound_threshold_and":
        return 0.90
    if condition_type == "affine_field_comparison":
        return 0.89
    if condition_type == "scaled_field_comparison":
        return 0.88
    return 0.80


def _build_python_code(rule: Dict[str, object], function_name: str) -> str:
    tag_literal = _python_literal(rule["tag"])
    condition_type = str(rule.get("condition_type"))

    if condition_type == "field_comparison":
        left = str(rule["left_operand"])
        right = str(rule["right_operand"])
        operator = str(rule["operator"])
        return f"""def {function_name}(product):
    left_raw = product.get({left!r})
    right_raw = product.get({right!r})
    try:
        left_value = float(left_raw)
        right_value = float(right_raw)
    except (TypeError, ValueError):
        return None
    if left_value {operator} right_value:
        return {tag_literal}
    return None
"""

    if condition_type == "field_threshold":
        left = str(rule["left_operand"])
        operator = str(rule["operator"])
        right_literal = _python_literal(rule["right_operand"])
        return f"""def {function_name}(product):
    left_raw = product.get({left!r})
    try:
        left_value = float(left_raw)
    except (TypeError, ValueError):
        return None
    if left_value {operator} {right_literal}:
        return {tag_literal}
    return None
"""

    if condition_type == "missing_field":
        field = str(rule["left_operand"])
        return f"""def {function_name}(product):
    value = product.get({field!r})
    if value is None or str(value).strip() == "":
        return {tag_literal}
    return None
"""

    if condition_type == "scaled_field_comparison":
        left = str(rule["left_operand"])
        right = str(rule["right_operand"])
        operator = str(rule["operator"])
        factor = float(rule["scale_factor"])
        return f"""def {function_name}(product):
    left_raw = product.get({left!r})
    right_raw = product.get({right!r})
    try:
        left_value = float(left_raw)
        right_value = float(right_raw)
    except (TypeError, ValueError):
        return None
    scaled_value = right_value * {factor}
    if left_value {operator} scaled_value:
        return {tag_literal}
    return None
"""

    if condition_type == "compound_threshold_and":
        clauses = list(rule.get("clauses", []))
        lines = [
            f"def {function_name}(product):",
            "    try:",
        ]
        for idx, clause in enumerate(clauses):
            field = str(clause["left_operand"])
            lines.append(f"        value_{idx} = float(product.get({field!r}))")
        lines.append("    except (TypeError, ValueError):")
        lines.append("        return None")
        checks: List[str] = []
        for idx, clause in enumerate(clauses):
            operator = str(clause["operator"])
            threshold = float(clause["right_operand"])
            checks.append(f"(value_{idx} {operator} {threshold})")
        joined = " and ".join(checks) if checks else "False"
        lines.append(f"    if {joined}:")
        lines.append(f"        return {tag_literal}")
        lines.append("    return None")
        return "\n".join(lines) + "\n"

    if condition_type == "affine_field_comparison":
        left = str(rule["left_operand"])
        right = str(rule["right_operand"])
        operator = str(rule["operator"])
        factor = float(rule["scale_factor"])
        offset = float(rule["offset"])
        return f"""def {function_name}(product):
    left_raw = product.get({left!r})
    right_raw = product.get({right!r})
    try:
        left_value = float(left_raw)
        right_value = float(right_raw)
    except (TypeError, ValueError):
        return None
    target = ({factor} * right_value) + ({offset})
    if left_value {operator} target:
        return {tag_literal}
    return None
"""

    if condition_type == "sum_fields_comparison":
        left_operands = list(rule.get("left_operands", []))
        if len(left_operands) != 2:
            raise ValueError("sum_fields_comparison requires exactly two left_operands.")
        left_a = str(left_operands[0])
        left_b = str(left_operands[1])
        right = str(rule["right_operand"])
        operator = str(rule["operator"])
        right_offset = float(rule["right_offset"])
        return f"""def {function_name}(product):
    left_a_raw = product.get({left_a!r})
    left_b_raw = product.get({left_b!r})
    right_raw = product.get({right!r})
    try:
        left_a_value = float(left_a_raw)
        left_b_value = float(left_b_raw)
        right_value = float(right_raw)
    except (TypeError, ValueError):
        return None
    left_sum = left_a_value + left_b_value
    right_target = right_value + ({right_offset})
    if left_sum {operator} right_target:
        return {tag_literal}
    return None
"""

    raise ValueError(f"Unsupported condition type: {condition_type}")


def _extract_code_block(text: str) -> str:
    match = re.search(r"```(?:python)?\s*(?P<code>.*?)```", text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group("code").strip()
    return text.strip()


def _validate_generated_code(code: str, function_name: str) -> None:
    namespace: Dict[str, object] = {}
    exec(code, {}, namespace)
    fn = namespace.get(function_name)
    if not callable(fn):
        raise ValueError(f"Generated code does not define callable `{function_name}`.")
    # Basic runtime contract checks to avoid unsafe generated code.
    probe_products = [
        {},
        {
            "energy_kj": None,
            "energy_kj_computed": None,
            "energy_kcal": 100.0,
            "fat": None,
            "saturated_fat": None,
            "carbohydrates": None,
            "sugars": None,
            "language_code": None,
        },
        {
            "energy_kj": 100.0,
            "energy_kj_computed": 100.0,
            "energy_kcal": 10.0,
            "fat": 10.0,
            "saturated_fat": 2.0,
            "carbohydrates": 15.0,
            "sugars": 5.0,
            "language_code": "en",
        },
    ]
    for product in probe_products:
        try:
            result = fn(product)
        except Exception as exc:  # noqa: BLE001 - intentional hard guard for generated code
            raise ValueError(f"Generated function raised {exc.__class__.__name__}: {exc}") from exc
        if result is not None and not isinstance(result, str):
            raise ValueError("Generated function must return str or None.")


def _comparison_truth_pairs(operator_token: str) -> tuple[tuple[float, float], tuple[float, float]]:
    pairs = {
        ">": ((2.0, 1.0), (1.0, 2.0)),
        "<": ((1.0, 2.0), (2.0, 1.0)),
        ">=": ((2.0, 2.0), (1.0, 2.0)),
        "<=": ((2.0, 2.0), (3.0, 2.0)),
        "==": ((2.0, 2.0), (2.0, 3.0)),
        "!=": ((2.0, 3.0), (2.0, 2.0)),
    }
    if operator_token not in pairs:
        raise ValueError(f"Unsupported comparison operator: {operator_token}")
    return pairs[operator_token]


def _threshold_truth_values(operator_token: str, threshold: float) -> tuple[float, float]:
    if operator_token == ">":
        return threshold + 1.0, threshold
    if operator_token == "<":
        return threshold - 1.0, threshold
    if operator_token == ">=":
        return threshold, threshold - 1.0
    if operator_token == "<=":
        return threshold, threshold + 1.0
    if operator_token == "==":
        return threshold, threshold + 1.0
    if operator_token == "!=":
        return threshold + 1.0, threshold
    raise ValueError(f"Unsupported threshold operator: {operator_token}")


def _semantic_test_cases(rule: Dict[str, object]) -> List[tuple[Dict[str, object], str | None, str]]:
    condition_type = str(rule.get("condition_type"))
    tag = str(rule["tag"])

    if condition_type == "field_comparison":
        left = str(rule["left_operand"])
        right = str(rule["right_operand"])
        operator_token = str(rule["operator"])
        true_pair, false_pair = _comparison_truth_pairs(operator_token)
        return [
            ({left: true_pair[0], right: true_pair[1]}, tag, "comparison_true"),
            ({left: false_pair[0], right: false_pair[1]}, None, "comparison_false"),
            ({left: None, right: true_pair[1]}, None, "comparison_missing_left"),
            ({left: true_pair[0], right: None}, None, "comparison_missing_right"),
            ({left: "nan_text", right: true_pair[1]}, None, "comparison_non_numeric_left"),
        ]

    if condition_type == "field_threshold":
        left = str(rule["left_operand"])
        operator_token = str(rule["operator"])
        threshold = float(rule["right_operand"])
        true_value, false_value = _threshold_truth_values(operator_token, threshold)
        return [
            ({left: true_value}, tag, "threshold_true"),
            ({left: false_value}, None, "threshold_false"),
            ({left: None}, None, "threshold_missing"),
            ({left: "nan_text"}, None, "threshold_non_numeric"),
        ]

    if condition_type == "missing_field":
        field = str(rule["left_operand"])
        return [
            ({field: None}, tag, "missing_none"),
            ({field: ""}, tag, "missing_empty_string"),
            ({field: "   "}, tag, "missing_whitespace"),
            ({field: "en"}, None, "missing_present"),
        ]

    if condition_type == "scaled_field_comparison":
        left = str(rule["left_operand"])
        right = str(rule["right_operand"])
        operator_token = str(rule["operator"])
        factor = float(rule["scale_factor"])
        true_right = 10.0
        scaled = true_right * factor
        if operator_token == ">":
            true_left, false_left = scaled + 1.0, scaled - 1.0
        elif operator_token == ">=":
            true_left, false_left = scaled, scaled - 1.0
        elif operator_token == "<":
            true_left, false_left = scaled - 1.0, scaled + 1.0
        elif operator_token == "<=":
            true_left, false_left = scaled, scaled + 1.0
        elif operator_token == "==":
            true_left, false_left = scaled, scaled + 1.0
        elif operator_token == "!=":
            true_left, false_left = scaled + 1.0, scaled
        else:
            raise ValueError(f"Unsupported scaled comparison operator: {operator_token}")
        return [
            ({left: true_left, right: true_right}, tag, "scaled_true"),
            ({left: false_left, right: true_right}, None, "scaled_false"),
            ({left: None, right: true_right}, None, "scaled_missing_left"),
            ({left: true_left, right: None}, None, "scaled_missing_right"),
            ({left: "nan_text", right: true_right}, None, "scaled_non_numeric"),
        ]

    if condition_type == "affine_field_comparison":
        left = str(rule["left_operand"])
        right = str(rule["right_operand"])
        operator_token = str(rule["operator"])
        factor = float(rule["scale_factor"])
        offset = float(rule["offset"])
        true_right = 10.0
        target = (factor * true_right) + offset
        if operator_token == ">":
            true_left, false_left = target + 1.0, target - 1.0
        elif operator_token == ">=":
            true_left, false_left = target, target - 1.0
        elif operator_token == "<":
            true_left, false_left = target - 1.0, target + 1.0
        elif operator_token == "<=":
            true_left, false_left = target, target + 1.0
        elif operator_token == "==":
            true_left, false_left = target, target + 1.0
        elif operator_token == "!=":
            true_left, false_left = target + 1.0, target
        else:
            raise ValueError(f"Unsupported affine comparison operator: {operator_token}")
        return [
            ({left: true_left, right: true_right}, tag, "affine_true"),
            ({left: false_left, right: true_right}, None, "affine_false"),
            ({left: None, right: true_right}, None, "affine_missing_left"),
            ({left: true_left, right: None}, None, "affine_missing_right"),
            ({left: "nan_text", right: true_right}, None, "affine_non_numeric"),
        ]

    if condition_type == "sum_fields_comparison":
        left_operands = list(rule.get("left_operands", []))
        if len(left_operands) != 2:
            raise ValueError("sum_fields_comparison rule must define two left operands.")
        left_a = str(left_operands[0])
        left_b = str(left_operands[1])
        right = str(rule["right_operand"])
        operator_token = str(rule["operator"])
        right_offset = float(rule["right_offset"])
        right_value = 20.0
        target = right_value + right_offset
        if operator_token == ">":
            true_sum, false_sum = target + 1.0, target - 1.0
        elif operator_token == ">=":
            true_sum, false_sum = target, target - 1.0
        elif operator_token == "<":
            true_sum, false_sum = target - 1.0, target + 1.0
        elif operator_token == "<=":
            true_sum, false_sum = target, target + 1.0
        elif operator_token == "==":
            true_sum, false_sum = target, target + 1.0
        elif operator_token == "!=":
            true_sum, false_sum = target + 1.0, target
        else:
            raise ValueError(f"Unsupported sum comparison operator: {operator_token}")

        true_a = true_sum / 2.0
        true_b = true_sum - true_a
        false_a = false_sum / 2.0
        false_b = false_sum - false_a
        return [
            ({left_a: true_a, left_b: true_b, right: right_value}, tag, "sum_true"),
            ({left_a: false_a, left_b: false_b, right: right_value}, None, "sum_false"),
            ({left_a: None, left_b: true_b, right: right_value}, None, "sum_missing_left_a"),
            ({left_a: true_a, left_b: None, right: right_value}, None, "sum_missing_left_b"),
            ({left_a: true_a, left_b: true_b, right: None}, None, "sum_missing_right"),
        ]

    if condition_type == "compound_threshold_and":
        clauses = list(rule.get("clauses", []))
        if not clauses:
            raise ValueError("compound_threshold_and rule must define clauses.")
        true_product: Dict[str, object] = {}
        false_product: Dict[str, object] = {}
        missing_product: Dict[str, object] = {}
        for idx, clause in enumerate(clauses):
            field = str(clause["left_operand"])
            operator_token = str(clause["operator"])
            threshold = float(clause["right_operand"])
            true_value, false_value = _threshold_truth_values(operator_token, threshold)
            true_product[field] = true_value
            false_product[field] = true_value
            missing_product[field] = true_value
            if idx == 0:
                false_product[field] = false_value
                missing_product[field] = None
        return [
            (true_product, tag, "compound_true"),
            (false_product, None, "compound_false"),
            (missing_product, None, "compound_missing"),
        ]

    raise ValueError(f"Unsupported condition type for semantic checks: {condition_type}")


def _validate_generated_semantics(code: str, function_name: str, rule: Dict[str, object]) -> None:
    namespace: Dict[str, object] = {}
    exec(code, {}, namespace)
    fn = namespace.get(function_name)
    if not callable(fn):
        raise ValueError(f"Generated code does not define callable `{function_name}`.")

    for product, expected, case_name in _semantic_test_cases(rule):
        try:
            result = fn(product)
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"Semantic test `{case_name}` raised {exc.__class__.__name__}: {exc}") from exc
        if result != expected:
            raise ValueError(
                f"Semantic test `{case_name}` failed: expected {expected!r}, got {result!r}."
            )


def _normalize_function_name(code: str, function_name: str) -> str:
    """Rename the first generated function to the expected runtime name."""
    match = re.search(r"def\s+(?P<name>[a-zA-Z_]\w*)\s*\(", code)
    if not match:
        return code
    found_name = match.group("name")
    if found_name == function_name:
        return code
    start, end = match.span("name")
    return code[:start] + function_name + code[end:]


def _build_llm_prompt(rule: Dict[str, object], function_name: str) -> str:
    rule_payload = {
        "rule_name": rule["rule_name"],
        "tag": rule["tag"],
        "condition_type": rule["condition_type"],
        "condition": rule.get("condition"),
        "duckdb_condition": rule.get("duckdb_condition"),
        "left_operand": rule.get("left_operand"),
        "left_operands": rule.get("left_operands"),
        "operator": rule.get("operator"),
        "right_operand": rule.get("right_operand"),
        "scale_factor": rule.get("scale_factor"),
        "offset": rule.get("offset"),
        "right_offset": rule.get("right_offset"),
        "clauses": rule.get("clauses"),
        "complexity": rule.get("complexity"),
    }
    examples: List[Dict[str, object]] = []
    for product, expected, case_name in _semantic_test_cases(rule):
        examples.append({"case": case_name, "input": product, "expected": expected})
    return (
        "Generate one Python function for a data-quality rule.\n"
        f"Function name must be exactly: {function_name}\n"
        "Input: product (dict)\n"
        "Output: return the rule tag string if the VIOLATION condition is TRUE; else return None.\n"
        "Important: do NOT invert the condition.\n"
        "Important: return only Python code, no markdown, no explanation.\n"
        "Behavior requirements:\n"
        "- field_comparison and field_threshold rules: if value is missing/non-numeric, return None.\n"
        '- missing_field rule: None, empty string "", or whitespace-only string => return tag.\n'
        "- Otherwise return None.\n"
        f"Rule JSON: {json.dumps(rule_payload)}\n"
        f"Validation examples (must pass): {json.dumps(examples)}"
    )


def _build_llm_repair_prompt(
    rule: Dict[str, object],
    function_name: str,
    previous_code: str,
    error_message: str,
) -> str:
    return (
        "Your previous function failed validator checks.\n"
        f"Validation error: {error_message}\n"
        "Fix the function so all examples pass.\n"
        "Return only corrected Python code.\n"
        f"Previous code:\n{previous_code}\n\n"
        f"{_build_llm_prompt(rule, function_name)}"
    )


def _call_groq(
    rule: Dict[str, object],
    function_name: str,
    model: str,
    prompt: str | None = None,
) -> str:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY is not set.")

    try:
        from openai import OpenAI
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("openai package is required for Groq provider.") from exc

    endpoint = os.getenv("GROQ_ENDPOINT", "https://api.groq.com/openai/v1")
    client = OpenAI(api_key=api_key, base_url=endpoint)
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are a strict Python code generator for data quality rules."},
            {"role": "user", "content": prompt or _build_llm_prompt(rule, function_name)},
        ],
        temperature=0.0,
    )
    content = response.choices[0].message.content
    if not isinstance(content, str):
        content = str(content)
    return _extract_code_block(content)


def convert_rule_to_python(
    rule: Dict[str, object],
    provider: str = "groq",
    model: str | None = None,
) -> ConversionResult:
    """Convert one structured rule to Python code and confidence metadata."""
    function_name = f"check_{_safe_identifier(str(rule['rule_name']))}"
    confidence = _confidence_for_rule(rule)
    provider_used = provider

    if provider == "groq":
        strict_llm = os.getenv("LLM_STRICT", "0").strip().lower() in {"1", "true", "yes", "on"}
        chosen_model = model or os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")
        first_attempt_code = ""
        try:
            python_code = _call_groq(rule, function_name=function_name, model=chosen_model)
            first_attempt_code = python_code
            python_code = _normalize_function_name(python_code, function_name=function_name)
            _validate_generated_code(python_code, function_name=function_name)
            _validate_generated_semantics(python_code, function_name=function_name, rule=rule)
            notes = f"Converted via Groq ({chosen_model})."
            confidence = min(0.99, confidence + 0.01)
        except (
            RuntimeError,
            ValueError,
            TypeError,
            json.JSONDecodeError,
            TimeoutError,
        ) as exc:
            repair_exc: Exception | None = None
            try:
                repair_prompt = _build_llm_repair_prompt(
                    rule=rule,
                    function_name=function_name,
                    previous_code=first_attempt_code or "# no usable code returned in first attempt",
                    error_message=f"{exc.__class__.__name__}: {exc}",
                )
                python_code = _call_groq(
                    rule,
                    function_name=function_name,
                    model=chosen_model,
                    prompt=repair_prompt,
                )
                python_code = _normalize_function_name(python_code, function_name=function_name)
                _validate_generated_code(python_code, function_name=function_name)
                _validate_generated_semantics(python_code, function_name=function_name, rule=rule)
                notes = f"Converted via Groq ({chosen_model}) after repair pass."
                confidence = min(0.99, confidence + 0.005)
            except (
                RuntimeError,
                ValueError,
                TypeError,
                json.JSONDecodeError,
                TimeoutError,
            ) as second_exc:
                repair_exc = second_exc

            if repair_exc is not None:
                if strict_llm:
                    raise RuntimeError(
                        f"Groq conversion failed and LLM_STRICT=1 is enabled. "
                        f"First error: {exc.__class__.__name__}: {exc} | "
                        f"Repair error: {repair_exc.__class__.__name__}: {repair_exc}"
                    ) from repair_exc
                python_code = _build_python_code(rule, function_name=function_name)
                _validate_generated_code(python_code, function_name=function_name)
                _validate_generated_semantics(python_code, function_name=function_name, rule=rule)
                notes = (
                    f"Groq conversion failed after retry "
                    f"(first: {exc.__class__.__name__}: {exc}; "
                    f"retry: {repair_exc.__class__.__name__}: {repair_exc}). "
                    "Fell back to deterministic converter."
                )
                provider_used = "simulated_fallback"
                confidence = max(0.70, confidence - 0.05)
    else:
        if provider not in {"simulated", "simulated_fallback"}:
            provider_used = "simulated_fallback"
        python_code = _build_python_code(rule, function_name=function_name)
        _validate_generated_code(python_code, function_name=function_name)
        _validate_generated_semantics(python_code, function_name=function_name, rule=rule)
        notes = f"Converted {rule['condition_type']} rule using deterministic template."

    return ConversionResult(
        rule_name=str(rule["rule_name"]),
        function_name=function_name,
        python_code=python_code,
        llm_confidence=confidence,
        conversion_notes=notes,
        provider=provider_used,
    )


def _build_counterexample_repair_prompt(
    rule: Dict[str, object],
    function_name: str,
    previous_code: str,
    counterexamples: Sequence[Dict[str, object]],
) -> str:
    limited = list(counterexamples)[:5]
    return (
        "Your previous code fails equivalence against legacy Perl behavior.\n"
        "Fix the function using these concrete failing examples.\n"
        "Return only Python code.\n"
        f"Function name must remain: {function_name}\n"
        f"Counterexamples: {json.dumps(limited)}\n"
        f"Previous code:\n{previous_code}\n\n"
        f"{_build_llm_prompt(rule, function_name)}"
    )


def repair_conversion_with_counterexamples(
    rule: Dict[str, object],
    converted_rule: Dict[str, object],
    counterexamples: Sequence[Dict[str, object]],
    provider: str = "groq",
    model: str | None = None,
) -> Dict[str, object]:
    """Attempt counterexample-driven repair for an already converted rule."""
    if provider != "groq":
        return dict(converted_rule)
    if not counterexamples:
        return dict(converted_rule)

    function_name = str(converted_rule["function_name"])
    previous_code = str(converted_rule["python_code"])
    chosen_model = model or os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")
    repair_prompt = _build_counterexample_repair_prompt(
        rule=rule,
        function_name=function_name,
        previous_code=previous_code,
        counterexamples=counterexamples,
    )
    repaired_code = _call_groq(rule, function_name=function_name, model=chosen_model, prompt=repair_prompt)
    repaired_code = _normalize_function_name(repaired_code, function_name=function_name)
    _validate_generated_code(repaired_code, function_name=function_name)
    _validate_generated_semantics(repaired_code, function_name=function_name, rule=rule)

    repaired = dict(converted_rule)
    repaired["python_code"] = repaired_code
    repaired["provider"] = "groq"
    repaired["llm_confidence"] = min(0.99, float(converted_rule.get("llm_confidence", 0.9)) + 0.01)
    repaired["conversion_notes"] = (
        f"{converted_rule.get('conversion_notes', '')} "
        "Counterexample-driven Groq repair applied."
    ).strip()
    return repaired


def convert_rules(
    rules: Sequence[Dict[str, object]],
    provider: str = "groq",
    model: str | None = None,
) -> List[Dict[str, object]]:
    """Batch convert structured rules to generated Python snippets."""
    return [asdict(convert_rule_to_python(rule, provider=provider, model=model)) for rule in rules]
