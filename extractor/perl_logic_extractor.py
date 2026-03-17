"""Perl logic extractor for converting legacy checks into structured rules."""
from __future__ import annotations

import hashlib
import json
import re
from typing import Dict, Iterable, List, Mapping, Sequence

RULE_NAME_RE = re.compile(r"#\s*RULE_NAME:\s*(?P<value>[a-zA-Z0-9_]+)")
SEVERITY_RE = re.compile(r"#\s*SEVERITY:\s*(?P<value>[a-zA-Z0-9_]+)")
COMPLEXITY_RE = re.compile(r"#\s*COMPLEXITY:\s*(?P<value>[a-zA-Z0-9_]+)")
DECLARATIVE_RE = re.compile(r"#\s*DECLARATIVE_FRIENDLY:\s*(?P<value>[a-zA-Z0-9_]+)")
TAG_RE = re.compile(r'"(?P<tag>[a-z0-9\-]+)"\s*;')

VAR_COMPARE_RE = re.compile(
    r"if\s*\(\s*\$(?P<left>[a-zA-Z_]\w*)\s*(?P<op>>=|<=|>|<|==|!=)\s*\$(?P<right>[a-zA-Z_]\w*)\s*\)",
    re.DOTALL,
)
VALUE_COMPARE_RE = re.compile(
    r"if\s*\(\s*\$(?P<left>[a-zA-Z_]\w*)\s*(?P<op>>=|<=|>|<|==|!=)\s*(?P<right>\d+(?:\.\d+)?)\s*\)",
    re.DOTALL,
)
MISSING_FIELD_RE = re.compile(
    r'if\s*\(\s*!defined\s+\$(?P<field>[a-zA-Z_]\w*)\s*\|\|\s*\$(?P=field)\s+eq\s+""\s*\)',
    re.DOTALL,
)
SCALED_FIELD_COMPARE_RE = re.compile(
    r"if\s*\(\s*\$(?P<left>[a-zA-Z_]\w*)\s*(?P<op>>=|<=|>|<|==|!=)\s*\(\s*\$(?P<right>[a-zA-Z_]\w*)\s*\*\s*(?P<factor>\d+(?:\.\d+)?)\s*\)\s*\)",
    re.DOTALL,
)
AFFINE_FIELD_COMPARE_RE = re.compile(
    r"if\s*\(\s*\$(?P<left>[a-zA-Z_]\w*)\s*(?P<op>>=|<=|>|<|==|!=)\s*\(\s*(?P<factor>\d+(?:\.\d+)?)\s*\*\s*\$(?P<right>[a-zA-Z_]\w*)\s*(?P<sign>[+-])\s*(?P<offset>\d+(?:\.\d+)?)\s*\)\s*\)",
    re.DOTALL,
)
SUM_FIELDS_COMPARE_RE = re.compile(
    r"if\s*\(\s*\(\s*\$(?P<left_a>[a-zA-Z_]\w*)\s*\+\s*\$(?P<left_b>[a-zA-Z_]\w*)\s*\)\s*(?P<op>>=|<=|>|<|==|!=)\s*\(\s*\$(?P<right>[a-zA-Z_]\w*)\s*(?P<sign>[+-])\s*(?P<offset>\d+(?:\.\d+)?)\s*\)\s*\)",
    re.DOTALL,
)
IF_CONDITION_RE = re.compile(r"if\s*\(\s*(?P<condition>.*?)\s*\)\s*\{", re.DOTALL)
THRESHOLD_CLAUSE_RE = re.compile(
    r"^\s*\$(?P<left>[a-zA-Z_]\w*)\s*(?P<op>>=|<=|>|<|==|!=)\s*(?P<right>\d+(?:\.\d+)?)\s*$"
)


def _extract_named_value(pattern: re.Pattern[str], text: str, default: str) -> str:
    match = pattern.search(text)
    if not match:
        return default
    return match.group("value").strip()


def _extract_tag(perl_logic: str) -> str:
    match = TAG_RE.search(perl_logic)
    if not match:
        raise ValueError(f"Unable to extract tag from Perl logic:\n{perl_logic}")
    return match.group("tag")


def _default_rule_name(tag: str) -> str:
    return tag.replace("-", "_")


def _parse_declarative_flag(value: str) -> bool:
    return value.strip().lower() in {"yes", "true", "1", "y"}


def _default_complexity(condition_type: str) -> str:
    if condition_type in {"field_comparison", "field_threshold", "missing_field"}:
        return "simple"
    if condition_type == "compound_threshold_and":
        return "medium"
    if condition_type == "sum_fields_comparison":
        return "medium"
    if condition_type == "affine_field_comparison":
        return "intricate"
    return "intricate"


def _build_rule_ir(rule: Mapping[str, object]) -> Dict[str, object]:
    condition_type = str(rule.get("condition_type", "unknown"))
    ir: Dict[str, object] = {
        "version": "1.0",
        "rule_name": rule.get("rule_name"),
        "condition_type": condition_type,
        "severity": rule.get("severity"),
        "tag": rule.get("tag"),
    }
    if condition_type in {"field_comparison", "field_threshold", "missing_field", "scaled_field_comparison", "affine_field_comparison"}:
        ir["left_operand"] = rule.get("left_operand")
    if condition_type in {"field_comparison", "field_threshold", "scaled_field_comparison", "affine_field_comparison", "sum_fields_comparison"}:
        ir["operator"] = rule.get("operator")
    if condition_type in {"field_comparison", "field_threshold", "scaled_field_comparison", "affine_field_comparison", "sum_fields_comparison"}:
        ir["right_operand"] = rule.get("right_operand")
    if condition_type == "sum_fields_comparison":
        ir["left_operands"] = rule.get("left_operands")
        ir["right_offset"] = rule.get("right_offset")
    if condition_type == "scaled_field_comparison":
        ir["scale_factor"] = rule.get("scale_factor")
    if condition_type == "affine_field_comparison":
        ir["scale_factor"] = rule.get("scale_factor")
        ir["offset"] = rule.get("offset")
    if condition_type == "compound_threshold_and":
        ir["clauses"] = rule.get("clauses")
    return ir


def _attach_rule_ir(rule: Dict[str, object]) -> Dict[str, object]:
    ir = _build_rule_ir(rule)
    ir_json = json.dumps(ir, sort_keys=True, default=str)
    rule["rule_ir"] = ir
    rule["rule_ir_hash"] = hashlib.sha1(ir_json.encode("utf-8")).hexdigest()[:12]
    return rule


def extract_rule(perl_logic: str) -> Dict[str, object]:
    """Extract one structured rule from a Perl snippet."""
    perl_logic = perl_logic.lstrip("\ufeff").strip()
    tag = _extract_tag(perl_logic)
    rule_name = _extract_named_value(RULE_NAME_RE, perl_logic, _default_rule_name(tag))
    severity = _extract_named_value(SEVERITY_RE, perl_logic, "error")
    complexity_meta = _extract_named_value(COMPLEXITY_RE, perl_logic, "")
    declarative_meta = _extract_named_value(DECLARATIVE_RE, perl_logic, "")
    declarative_friendly = _parse_declarative_flag(declarative_meta) if declarative_meta else None

    missing_match = MISSING_FIELD_RE.search(perl_logic)
    if missing_match:
        field = missing_match.group("field")
        return {
            "rule_name": rule_name,
            "condition": f"missing({field})",
            "duckdb_condition": f"{field} IS NULL OR TRIM({field}) = ''",
            "condition_type": "missing_field",
            "left_operand": field,
            "operator": "missing",
            "right_operand": None,
            "complexity": complexity_meta or _default_complexity("missing_field"),
            "declarative_friendly": True if declarative_friendly is None else declarative_friendly,
            "tag": tag,
            "severity": severity,
            "perl_logic": perl_logic.strip(),
        }

    scaled_match = SCALED_FIELD_COMPARE_RE.search(perl_logic)
    if scaled_match:
        left = scaled_match.group("left")
        operator = scaled_match.group("op")
        right = scaled_match.group("right")
        factor = float(scaled_match.group("factor"))
        condition = f"{left} {operator} ({right} * {factor})"
        return {
            "rule_name": rule_name,
            "condition": condition,
            "duckdb_condition": f"{left} {operator} ({right} * {factor})",
            "condition_type": "scaled_field_comparison",
            "left_operand": left,
            "operator": operator,
            "right_operand": right,
            "scale_factor": factor,
            "complexity": complexity_meta or _default_complexity("scaled_field_comparison"),
            "declarative_friendly": False if declarative_friendly is None else declarative_friendly,
            "tag": tag,
            "severity": severity,
            "perl_logic": perl_logic.strip(),
        }

    affine_match = AFFINE_FIELD_COMPARE_RE.search(perl_logic)
    if affine_match:
        left = affine_match.group("left")
        operator = affine_match.group("op")
        right = affine_match.group("right")
        factor = float(affine_match.group("factor"))
        offset = float(affine_match.group("offset"))
        if affine_match.group("sign") == "-":
            offset *= -1.0
        offset_sign = "+" if offset >= 0 else "-"
        offset_abs = abs(offset)
        condition = f"{left} {operator} ({factor} * {right} {offset_sign} {offset_abs})"
        return {
            "rule_name": rule_name,
            "condition": condition,
            "duckdb_condition": condition,
            "condition_type": "affine_field_comparison",
            "left_operand": left,
            "operator": operator,
            "right_operand": right,
            "scale_factor": factor,
            "offset": offset,
            "complexity": complexity_meta or _default_complexity("affine_field_comparison"),
            "declarative_friendly": False if declarative_friendly is None else declarative_friendly,
            "tag": tag,
            "severity": severity,
            "perl_logic": perl_logic.strip(),
        }

    sum_match = SUM_FIELDS_COMPARE_RE.search(perl_logic)
    if sum_match:
        left_a = sum_match.group("left_a")
        left_b = sum_match.group("left_b")
        operator = sum_match.group("op")
        right = sum_match.group("right")
        offset = float(sum_match.group("offset"))
        if sum_match.group("sign") == "-":
            offset *= -1.0
        offset_sign = "+" if offset >= 0 else "-"
        offset_abs = abs(offset)
        condition = f"({left_a} + {left_b}) {operator} ({right} {offset_sign} {offset_abs})"
        return {
            "rule_name": rule_name,
            "condition": condition,
            "duckdb_condition": condition,
            "condition_type": "sum_fields_comparison",
            "left_operands": [left_a, left_b],
            "operator": operator,
            "right_operand": right,
            "right_offset": offset,
            "left_operand": None,
            "complexity": complexity_meta or _default_complexity("sum_fields_comparison"),
            "declarative_friendly": True if declarative_friendly is None else declarative_friendly,
            "tag": tag,
            "severity": severity,
            "perl_logic": perl_logic.strip(),
        }

    if "&&" in perl_logic:
        condition_match = IF_CONDITION_RE.search(perl_logic)
        if condition_match:
            condition_body = condition_match.group("condition").strip()
            clauses: List[Dict[str, object]] = []
            for part in [token.strip() for token in condition_body.split("&&")]:
                clause_match = THRESHOLD_CLAUSE_RE.match(part)
                if not clause_match:
                    clauses = []
                    break
                clauses.append(
                    {
                        "left_operand": clause_match.group("left"),
                        "operator": clause_match.group("op"),
                        "right_operand": float(clause_match.group("right")),
                    }
                )
            if clauses:
                duckdb_condition = " AND ".join(
                    f"{clause['left_operand']} {clause['operator']} {clause['right_operand']}" for clause in clauses
                )
                condition = " && ".join(
                    f"{clause['left_operand']} {clause['operator']} {clause['right_operand']}" for clause in clauses
                )
                return {
                    "rule_name": rule_name,
                    "condition": condition,
                    "duckdb_condition": duckdb_condition,
                    "condition_type": "compound_threshold_and",
                    "clauses": clauses,
                    "left_operand": None,
                    "operator": "&&",
                    "right_operand": None,
                    "complexity": complexity_meta or _default_complexity("compound_threshold_and"),
                    "declarative_friendly": True if declarative_friendly is None else declarative_friendly,
                    "tag": tag,
                    "severity": severity,
                    "perl_logic": perl_logic.strip(),
                }

    var_match = VAR_COMPARE_RE.search(perl_logic)
    if var_match:
        left = var_match.group("left")
        operator = var_match.group("op")
        right = var_match.group("right")
        condition = f"{left} {operator} {right}"
        return {
            "rule_name": rule_name,
            "condition": condition,
            "duckdb_condition": condition,
            "condition_type": "field_comparison",
            "left_operand": left,
            "operator": operator,
            "right_operand": right,
            "complexity": complexity_meta or _default_complexity("field_comparison"),
            "declarative_friendly": True if declarative_friendly is None else declarative_friendly,
            "tag": tag,
            "severity": severity,
            "perl_logic": perl_logic.strip(),
        }

    value_match = VALUE_COMPARE_RE.search(perl_logic)
    if value_match:
        left = value_match.group("left")
        operator = value_match.group("op")
        right = value_match.group("right")
        condition = f"{left} {operator} {right}"
        return {
            "rule_name": rule_name,
            "condition": condition,
            "duckdb_condition": condition,
            "condition_type": "field_threshold",
            "left_operand": left,
            "operator": operator,
            "right_operand": float(right),
            "complexity": complexity_meta or _default_complexity("field_threshold"),
            "declarative_friendly": True if declarative_friendly is None else declarative_friendly,
            "tag": tag,
            "severity": severity,
            "perl_logic": perl_logic.strip(),
        }

    raise ValueError(f"Unsupported Perl condition format:\n{perl_logic}")


def extract_rules(perl_snippets: Sequence[str] | str) -> List[Dict[str, object]]:
    """Extract structured rules from Perl snippets."""
    snippets: Iterable[str]
    if isinstance(perl_snippets, str):
        snippets = [chunk.strip() for chunk in perl_snippets.split("\n\n") if chunk.strip()]
    else:
        snippets = perl_snippets
    return [_attach_rule_ir(extract_rule(snippet)) for snippet in snippets]
