"""Perl logic extractor for converting legacy checks into structured rules."""
from __future__ import annotations

import re
from typing import Dict, Iterable, List, Sequence

RULE_NAME_RE = re.compile(r"#\s*RULE_NAME:\s*(?P<value>[a-zA-Z0-9_]+)")
SEVERITY_RE = re.compile(r"#\s*SEVERITY:\s*(?P<value>[a-zA-Z0-9_]+)")
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


def extract_rule(perl_logic: str) -> Dict[str, object]:
    """Extract one structured rule from a Perl snippet."""
    tag = _extract_tag(perl_logic)
    rule_name = _extract_named_value(RULE_NAME_RE, perl_logic, _default_rule_name(tag))
    severity = _extract_named_value(SEVERITY_RE, perl_logic, "error")

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
    return [extract_rule(snippet) for snippet in snippets]
