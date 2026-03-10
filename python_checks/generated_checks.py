"""Runtime compiler for generated Python checks."""
from __future__ import annotations

from typing import Callable, Dict, List, Tuple


def compile_generated_checks(
    converted_rules: List[Dict[str, object]],
) -> Tuple[Dict[str, Callable[[Dict[str, object]], object]], Dict[str, Dict[str, object]]]:
    """Compile generated Python snippets into callable checks."""
    checks: Dict[str, Callable[[Dict[str, object]], object]] = {}
    metadata: Dict[str, Dict[str, object]] = {}

    for converted in converted_rules:
        function_name = str(converted["function_name"])
        rule_name = str(converted["rule_name"])
        code = str(converted["python_code"])

        namespace: Dict[str, object] = {}
        exec(code, {}, namespace)
        check_fn = namespace[function_name]
        checks[rule_name] = check_fn
        metadata[rule_name] = {
            "function_name": function_name,
            "python_code": code,
            "llm_confidence": float(converted["llm_confidence"]),
            "conversion_notes": converted["conversion_notes"],
            "provider": converted.get("provider", "unknown"),
        }
    return checks, metadata


def render_generated_module(metadata: Dict[str, Dict[str, object]]) -> str:
    """Return a readable module-like rendering of generated checks."""
    code_blocks = [str(info["python_code"]).rstrip() for info in metadata.values()]
    return "\n\n".join(code_blocks) + "\n"
