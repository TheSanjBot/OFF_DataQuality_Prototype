"""LLM-style conversion from structured rules to Python check code."""
from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass
from typing import Dict, List, Sequence
from urllib import error as urlerror
from urllib import request as urlrequest


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
            "energy_kcal": 100.0,
            "fat": None,
            "saturated_fat": None,
            "carbohydrates": None,
            "sugars": None,
            "language_code": None,
        },
        {
            "energy_kj": 100.0,
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
        "left_operand": rule["left_operand"],
        "operator": rule["operator"],
        "right_operand": rule["right_operand"],
    }
    return (
        "Convert this rule into Python.\n"
        f"Function name must be exactly `{function_name}`.\n"
        "Input argument: product (dict).\n"
        "Return rule tag string when condition fails, else return None.\n"
        "Return only valid Python code for the function.\n"
        f"Rule JSON: {json.dumps(rule_payload)}"
    )


def _parse_chat_response(body: str) -> str:
    parsed = json.loads(body)
    choices = parsed.get("choices", [])
    if not choices:
        raise RuntimeError("No choices returned from LLM response.")
    message = choices[0].get("message", {})
    content = message.get("content", "")
    if isinstance(content, list):
        text_parts: List[str] = []
        for chunk in content:
            if isinstance(chunk, dict):
                text_parts.append(str(chunk.get("text", "")))
            else:
                text_parts.append(str(chunk))
        content = "".join(text_parts)
    if not isinstance(content, str):
        content = str(content)
    return _extract_code_block(content)


def _call_openrouter(rule: Dict[str, object], function_name: str, model: str) -> str:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is not set.")

    endpoint = os.getenv("OPENROUTER_ENDPOINT", "https://openrouter.ai/api/v1/chat/completions")
    model_name = model
    app_title = os.getenv("OPENROUTER_APP_TITLE", "off-quality-migration-prototype")
    app_url = os.getenv("OPENROUTER_APP_URL", "http://localhost")

    payload = {
        "model": model_name,
        "messages": [
            {"role": "system", "content": "You are a strict Python code generator for data quality rules."},
            {"role": "user", "content": _build_llm_prompt(rule, function_name)},
        ],
        "temperature": 0.0,
    }

    req = urlrequest.Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "HTTP-Referer": app_url,
            "X-Title": app_title,
        },
        method="POST",
    )

    with urlrequest.urlopen(req, timeout=90) as response:
        body = response.read().decode("utf-8")
    return _parse_chat_response(body)


def _call_groq(rule: Dict[str, object], function_name: str, model: str) -> str:
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
            {"role": "user", "content": _build_llm_prompt(rule, function_name)},
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
        try:
            python_code = _call_groq(rule, function_name=function_name, model=chosen_model)
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
            urlerror.URLError,
            urlerror.HTTPError,
            TimeoutError,
        ) as exc:
            if strict_llm:
                raise RuntimeError(
                    f"Groq conversion failed and LLM_STRICT=1 is enabled. "
                    f"Error: {exc.__class__.__name__}: {exc}"
                ) from exc
            python_code = _build_python_code(rule, function_name=function_name)
            _validate_generated_code(python_code, function_name=function_name)
            _validate_generated_semantics(python_code, function_name=function_name, rule=rule)
            notes = (
                f"Groq conversion failed ({exc.__class__.__name__}: {exc}). "
                "Fell back to deterministic converter."
            )
            provider_used = "simulated_fallback"
            confidence = max(0.70, confidence - 0.05)
    elif provider == "openrouter":
        strict_llm = os.getenv("LLM_STRICT", "0").strip().lower() in {"1", "true", "yes", "on"}
        primary_model = model or os.getenv("OPENROUTER_MODEL", "arcee-ai/trinity-large-preview:free")
        fallback_model = os.getenv("OPENROUTER_FALLBACK_MODEL", "")
        model_candidates = [primary_model]
        if fallback_model and fallback_model != primary_model:
            model_candidates.append(fallback_model)

        errors: List[str] = []
        generated_code: str | None = None
        chosen_model: str | None = None
        used_fallback_model = False

        for idx, candidate_model in enumerate(model_candidates):
            try:
                candidate_code = _call_openrouter(rule, function_name=function_name, model=candidate_model)
                candidate_code = _normalize_function_name(candidate_code, function_name=function_name)
                _validate_generated_code(candidate_code, function_name=function_name)
                _validate_generated_semantics(candidate_code, function_name=function_name, rule=rule)
                generated_code = candidate_code
                chosen_model = candidate_model
                used_fallback_model = idx > 0
                break
            except (
                RuntimeError,
                ValueError,
                TypeError,
                json.JSONDecodeError,
                urlerror.URLError,
                urlerror.HTTPError,
                TimeoutError,
            ) as exc:
                errors.append(f"{candidate_model}: {exc.__class__.__name__}: {exc}")

        if generated_code is not None:
            python_code = generated_code
            if used_fallback_model:
                provider_used = "openrouter_model_fallback"
                notes = (
                    f"Primary model failed; converted via OpenRouter fallback model ({chosen_model}). "
                    f"Primary error: {errors[0]}"
                )
                confidence = min(0.99, confidence)
            else:
                notes = f"Converted via OpenRouter ({chosen_model})."
                confidence = min(0.99, confidence + 0.01)
        else:
            error_summary = " | ".join(errors) if errors else "No OpenRouter attempt was made."
            if strict_llm:
                raise RuntimeError(
                    f"OpenRouter conversion failed and LLM_STRICT=1 is enabled. "
                    f"Errors: {error_summary}"
                )
            python_code = _build_python_code(rule, function_name=function_name)
            _validate_generated_code(python_code, function_name=function_name)
            _validate_generated_semantics(python_code, function_name=function_name, rule=rule)
            notes = (
                f"OpenRouter conversion failed ({error_summary}). "
                "Fell back to deterministic converter."
            )
            provider_used = "simulated_fallback"
            confidence = max(0.70, confidence - 0.05)
    else:
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


def convert_rules(
    rules: Sequence[Dict[str, object]],
    provider: str = "groq",
    model: str | None = None,
) -> List[Dict[str, object]]:
    """Batch convert structured rules to generated Python snippets."""
    return [asdict(convert_rule_to_python(rule, provider=provider, model=model)) for rule in rules]
