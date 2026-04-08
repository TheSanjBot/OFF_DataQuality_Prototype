"""Compare python/dbt/soda execution engines against the same Perl baseline."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Dict, Iterable, List, Mapping, Sequence, Tuple
from uuid import uuid4

from data.load_dataset import DB_PATH, DEFAULT_OFF_JSONL
from rulepacks.registry import DEFAULT_PROFILE, SUPPORTED_PROFILES
from validation.parity_validator import RESULT_PATH, run_pipeline

COMPARISON_PATH = Path(__file__).resolve().parent.parent / "results" / "engine_comparison.json"
ENGINES = ("python", "dbt", "soda")


def _line_count(text: object) -> int:
    snippet = str(text or "").strip("\n")
    if not snippet:
        return 0
    return len(snippet.splitlines())


def _comparison_fingerprint_payload(
    engine_payloads: Mapping[str, Mapping[str, object]],
    generated_at_utc: str,
) -> Dict[str, object]:
    engine_run_ids: Dict[str, str] = {}
    dataset_hashes: Dict[str, str] = {}
    rulepack_hashes: Dict[str, str] = {}
    commits: Dict[str, str] = {}

    for engine, payload in engine_payloads.items():
        run_fp = payload.get("run_fingerprint", {})
        engine_run_ids[engine] = str(run_fp.get("run_id", ""))
        commits[engine] = str(run_fp.get("code_commit", "unknown"))
        dataset_fp = run_fp.get("dataset_fingerprint", {})
        rulepack_fp = run_fp.get("rulepack_fingerprint", {})
        dataset_hashes[engine] = str(dataset_fp.get("sha256", ""))
        rulepack_hashes[engine] = str(rulepack_fp.get("rule_ir_sha256", ""))

    dataset_unique = sorted({value for value in dataset_hashes.values() if value})
    rulepack_unique = sorted({value for value in rulepack_hashes.values() if value})
    commit_unique = sorted({value for value in commits.values() if value and value != "unknown"})

    safe_timestamp = generated_at_utc.replace(":", "").replace("-", "").replace(".", "").replace("+", "p")
    comparison_run_id = f"comparison_{safe_timestamp}_{uuid4().hex[:8]}"
    comparison_sha_input = {
        "engine_run_ids": engine_run_ids,
        "dataset_hashes": dataset_hashes,
        "rulepack_hashes": rulepack_hashes,
        "generated_at_utc": generated_at_utc,
    }
    comparison_sha = hashlib.sha256(
        json.dumps(comparison_sha_input, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")
    ).hexdigest()

    return {
        "comparison_run_id": comparison_run_id,
        "comparison_sha256": comparison_sha,
        "engine_run_ids": engine_run_ids,
        "dataset_fingerprint_sha256_by_engine": dataset_hashes,
        "rulepack_fingerprint_sha256_by_engine": rulepack_hashes,
        "dataset_fingerprint_consistent": len(dataset_unique) <= 1,
        "rulepack_fingerprint_consistent": len(rulepack_unique) <= 1,
        "dataset_fingerprint_sha256": dataset_unique[0] if dataset_unique else "",
        "rulepack_fingerprint_sha256": rulepack_unique[0] if rulepack_unique else "",
        "code_commit": commit_unique[0] if len(commit_unique) == 1 else "mixed_or_unknown",
        "code_commits_by_engine": commits,
    }


def _is_fallback_provider(provider: str) -> bool:
    return "fallback" in provider.lower()


def _python_provider_is_real_llm(provider: str) -> bool:
    normalized = provider.strip().lower()
    return normalized in {"groq"}


def _provider_factor(engine: str, provider: str) -> float:
    normalized = provider.strip().lower()
    if engine == "python":
        if normalized == "groq":
            return 1.0
        if normalized == "simulated_fallback":
            return 0.55
        return 0.75
    if engine == "dbt":
        if normalized == "dbt_core":
            return 1.0
        if normalized == "dbt_core_sql_fallback":
            return 0.85
        return 0.9
    if engine == "soda":
        if normalized == "soda_cloud":
            return 1.0
        if normalized == "soda_core":
            return 1.0
        if normalized == "soda_core_sql_fallback":
            return 0.85
        return 0.9
    return 0.8


def _effective_confidence(engine: str, row: Mapping[str, object]) -> float:
    overall = float(row.get("overall_confidence", 0.0))
    provider = str(row.get("conversion_provider", "unknown"))
    return overall * _provider_factor(engine, provider)


def _is_declarative_friendly(condition: str) -> bool:
    text = condition.strip().lower()
    if not text:
        return False
    if text.startswith("missing("):
        return True
    return any(op in text for op in (">", "<", ">=", "<=", "==", "!="))


def _decision_score(engine: str, row: Mapping[str, object], declarative_friendly: bool) -> float:
    score = _effective_confidence(engine, row)
    status = str(row.get("status", "REVIEW"))
    mismatches = int(row.get("mismatches", 0))
    equivalence_rate = float(row.get("equivalence_match_rate", 1.0))
    mutation_score = float(row.get("mutation_score", 1.0))

    if status != "MATCH":
        score -= 0.25
    score -= mismatches * 1.0
    if engine == "python":
        score += 0.05 * equivalence_rate
        score += 0.05 * mutation_score
        if str(row.get("equivalence_status", "PASS")) != "PASS":
            score -= 0.10

    if declarative_friendly and engine in {"dbt", "soda"}:
        score += 0.035
    if (not declarative_friendly) and engine == "python":
        score += 0.035
    return score


def _status_rank(row: Mapping[str, object]) -> int:
    return 1 if str(row.get("status", "REVIEW")) == "MATCH" else 0


def _declarative_tie_break(
    rule_name: str,
    declarative_friendly: bool,
    dbt_row: Mapping[str, object],
    soda_row: Mapping[str, object],
) -> Tuple[str, str, bool]:
    """Pick best declarative engine with an explicit, balanced tie-break."""
    dbt_score = _decision_score("dbt", dbt_row, declarative_friendly)
    soda_score = _decision_score("soda", soda_row, declarative_friendly)
    dbt_effective = _effective_confidence("dbt", dbt_row)
    soda_effective = _effective_confidence("soda", soda_row)
    dbt_overall = float(dbt_row.get("overall_confidence", 0.0))
    soda_overall = float(soda_row.get("overall_confidence", 0.0))
    dbt_mismatches = int(dbt_row.get("mismatches", 0))
    soda_mismatches = int(soda_row.get("mismatches", 0))
    dbt_status = _status_rank(dbt_row)
    soda_status = _status_rank(soda_row)

    # Primary deterministic comparison.
    dbt_tuple = (dbt_status, -dbt_mismatches, dbt_score, dbt_effective, dbt_overall)
    soda_tuple = (soda_status, -soda_mismatches, soda_score, soda_effective, soda_overall)
    if dbt_tuple != soda_tuple:
        if dbt_tuple > soda_tuple:
            return "dbt", "declarative-rank:dbt>soda", False
        return "soda", "declarative-rank:soda>dbt", False

    # Explicit tie case: keep correctness identical, distribute ties fairly.
    # Stable rule-name hash parity avoids always preferring dbt.
    hash_int = int(hashlib.sha1(rule_name.encode("utf-8")).hexdigest(), 16)
    chosen = "dbt" if hash_int % 2 == 0 else "soda"
    reason = "explicit-hash-tie-break-even->dbt" if chosen == "dbt" else "explicit-hash-tie-break-odd->soda"
    return chosen, reason, True


def _engine_summary(engine: str, payload: Mapping[str, object]) -> Dict[str, object]:
    rule_results = list(payload.get("rule_results", []))
    if not rule_results:
        empty = {
            "rules": 0,
            "passed": 0,
            "avg_overall_confidence": 0.0,
            "avg_effective_confidence": 0.0,
            "avg_parity_ci_lower": 0.0,
            "fallback_rules": 0,
            "avg_equivalence_rate": 0.0,
            "avg_mutation_score": 0.0,
        }
        if engine == "python":
            empty["real_llm_rules"] = 0
            empty["real_llm_rate"] = 0.0
            empty["repairs_applied"] = 0
        return empty

    summary = {
        "rules": len(rule_results),
        "passed": sum(1 for row in rule_results if row.get("status") == "MATCH"),
        "avg_overall_confidence": round(mean(float(row.get("overall_confidence", 0.0)) for row in rule_results), 4),
        "avg_effective_confidence": round(mean(_effective_confidence(engine, row) for row in rule_results), 4),
        "avg_parity_ci_lower": round(mean(float(row.get("parity_ci_lower", 0.0)) for row in rule_results), 4),
        "avg_equivalence_rate": round(mean(float(row.get("equivalence_match_rate", 1.0)) for row in rule_results), 4),
        "avg_mutation_score": round(mean(float(row.get("mutation_score", 1.0)) for row in rule_results), 4),
        "fallback_rules": sum(
            1 for row in rule_results if _is_fallback_provider(str(row.get("conversion_provider", "")))
        ),
    }
    if engine == "python":
        real_llm_rules = sum(
            1 for row in rule_results if _python_provider_is_real_llm(str(row.get("conversion_provider", "")))
        )
        summary["real_llm_rules"] = real_llm_rules
        summary["real_llm_rate"] = round(real_llm_rules / len(rule_results), 4)
        summary["repairs_applied"] = sum(1 for row in rule_results if bool(row.get("counterexample_repair_applied")))
    return summary


def _best_engine_for_rule(per_engine_rows: Mapping[str, Mapping[str, object]]) -> Tuple[str, str, bool]:
    candidate_rows: List[Tuple[str, Mapping[str, object]]] = [(engine, row) for engine, row in per_engine_rows.items()]
    reference_row = next(iter(per_engine_rows.values()))
    if reference_row.get("declarative_friendly") is None:
        declarative_friendly = _is_declarative_friendly(str(reference_row.get("condition", "")))
    else:
        declarative_friendly = bool(reference_row.get("declarative_friendly"))
    ranked = sorted(
        candidate_rows,
        key=lambda item: (
            _decision_score(item[0], item[1], declarative_friendly),
            _effective_confidence(item[0], item[1]),
            float(item[1].get("overall_confidence", 0.0)),
        ),
        reverse=True,
    )

    rows = {engine: row for engine, row in candidate_rows}
    python_row = rows.get("python", {})
    python_match = str(python_row.get("status", "")) == "MATCH"
    python_effective = _effective_confidence("python", python_row) if python_row else 0.0

    dbt_row = rows.get("dbt", {})
    soda_row = rows.get("soda", {})
    declarative_best_engine = None
    declarative_best_effective = 0.0
    declarative_reason = "declarative-unavailable"
    declarative_tie_applied = False
    if dbt_row and soda_row:
        declarative_best_engine, declarative_reason, declarative_tie_applied = _declarative_tie_break(
            rule_name=str(reference_row.get("rule_name", "")),
            declarative_friendly=declarative_friendly,
            dbt_row=dbt_row,
            soda_row=soda_row,
        )
        declarative_best_effective = _effective_confidence(declarative_best_engine, rows[declarative_best_engine])
    elif dbt_row:
        declarative_best_engine = "dbt"
        declarative_best_effective = _effective_confidence("dbt", dbt_row)
        declarative_reason = "only-dbt-available"
    elif soda_row:
        declarative_best_engine = "soda"
        declarative_best_effective = _effective_confidence("soda", soda_row)
        declarative_reason = "only-soda-available"

    declarative_candidates = [
        (engine, row)
        for engine, row in candidate_rows
        if engine in {"dbt", "soda"} and str(row.get("status", "")) == "MATCH"
    ]
    if declarative_candidates and declarative_best_engine is None:
        declarative_best_engine, declarative_best_row = max(
            declarative_candidates,
            key=lambda item: _effective_confidence(item[0], item[1]),
        )
        declarative_best_effective = _effective_confidence(declarative_best_engine, declarative_best_row)
        declarative_reason = "fallback-declarative-selection"

    closeness_threshold = 0.20
    if declarative_friendly and python_match and declarative_best_engine is not None:
        if abs(python_effective - declarative_best_effective) <= closeness_threshold:
            return (
                declarative_best_engine,
                f"hybrid-close-declarative:{declarative_reason}",
                declarative_tie_applied,
            )
    if (not declarative_friendly) and python_match and declarative_best_engine is not None:
        if abs(python_effective - declarative_best_effective) <= closeness_threshold:
            return "python", "hybrid-close-procedural:prefer-python", False

    top_engine = ranked[0][0]
    if top_engine in {"dbt", "soda"} and declarative_tie_applied and declarative_best_engine in {"dbt", "soda"}:
        return declarative_best_engine, f"explicit-declarative-tie:{declarative_reason}", True
    return top_engine, "top-decision-score", False


def _rule_recommendation(rule_row: Mapping[str, object], best_engine: str) -> str:
    mismatches = int(rule_row.get("mismatches", 0))
    condition = str(rule_row.get("condition", ""))
    condition_type = str(rule_row.get("condition_type", ""))
    complexity = str(rule_row.get("complexity", "unknown"))
    equivalence_status = str(rule_row.get("equivalence_status", "PASS"))
    if rule_row.get("declarative_friendly") is None:
        declarative_friendly = _is_declarative_friendly(condition)
    else:
        declarative_friendly = bool(rule_row.get("declarative_friendly"))
    provider = str(rule_row.get("conversion_provider", ""))
    if mismatches > 0:
        return "Needs manual review; parity mismatches exist."
    if _is_fallback_provider(provider):
        return (
            f"{best_engine} currently wins, but provider is fallback. "
            "Enable real engine execution to confirm this choice."
        )
    if best_engine == "python" and equivalence_status != "PASS":
        return "Python rule failed equivalence checks; inspect counterexamples before accepting."
    if best_engine in {"dbt", "soda"} and declarative_friendly:
        return "Declarative-friendly rule; prefer dbt/soda for readability and operations."
    if best_engine == "python" and "fallback" in provider.lower():
        return "Use python path with caution; conversion fell back and needs prompt/model tuning."
    if best_engine == "python" and complexity in {"medium", "intricate"}:
        return (
            f"Procedural preference: rule is {complexity} ({condition_type}). "
            "Python migration is preferred under current evidence."
        )
    if best_engine == "python":
        return "Keep procedural Python migration path for this rule."
    return f"Prefer {best_engine} for this rule under current evidence."


def _migration_state(rule_row: Mapping[str, object], best_engine: str) -> Tuple[str, str]:
    status = str(rule_row.get("status", "REVIEW"))
    mismatches = int(rule_row.get("mismatches", 0))
    provider = str(rule_row.get("conversion_provider", "unknown"))
    parity_ci_lower = float(rule_row.get("parity_ci_lower", 0.0))
    effective_confidence = _effective_confidence(best_engine, rule_row)
    legal_review_status = str(rule_row.get("review_status", "")).strip().lower()

    if status != "MATCH" or mismatches > 0:
        return "review", "Parity mismatches remain or the rule still needs manual inspection."

    if best_engine == "python" and str(rule_row.get("equivalence_status", "PASS")) != "PASS":
        return "review", "Python conversion has not yet passed equivalence verification."

    if parity_ci_lower < 0.95:
        return "review", "Parity evidence is still too weak for acceptance."

    if _is_fallback_provider(provider):
        return "candidate", "Behavior matches, but the current engine/provider is still a fallback path."

    if best_engine == "python" and float(rule_row.get("mutation_score", 1.0)) < 0.60:
        return "candidate", "Python rule matches, but mutation resistance should be improved before acceptance."

    if effective_confidence < 0.10:
        return "candidate", "Rule matches the baseline, but confidence remains too low for final acceptance."

    if legal_review_status in {"draft", "pending-mentor-review"}:
        return "candidate", "Technically validated, but regulatory review is still pending."

    return "accepted", "Rule meets parity, provider-quality, and verification requirements."


def _build_complexity_summary(rule_comparison: Sequence[Mapping[str, object]]) -> Dict[str, Dict[str, object]]:
    summary: Dict[str, Dict[str, object]] = {}
    for tier in ("simple", "medium", "intricate", "unknown"):
        tier_rows = [row for row in rule_comparison if str(row.get("complexity", "unknown")) == tier]
        if not tier_rows:
            continue
        wins = {"python": 0, "dbt": 0, "soda": 0}
        for row in tier_rows:
            wins[str(row.get("best_engine", "python"))] += 1
        summary[tier] = {
            "rules": len(tier_rows),
            "python_wins": wins["python"],
            "dbt_wins": wins["dbt"],
            "soda_wins": wins["soda"],
            "avg_best_effective_confidence": round(
                mean(
                    float(row.get("engines", {}).get(str(row.get("best_engine")), {}).get("effective_confidence", 0.0))
                    for row in tier_rows
                ),
                4,
            ),
        }
    return summary


def _build_migration_state_summary(rule_comparison: Sequence[Mapping[str, object]]) -> Dict[str, int]:
    summary = {"accepted": 0, "candidate": 0, "review": 0}
    for row in rule_comparison:
        state = str(row.get("migration_state", "review")).lower()
        if state not in summary:
            state = "review"
        summary[state] += 1
    return summary


def _build_rule_comparison(engine_payloads: Mapping[str, Mapping[str, object]]) -> List[Dict[str, object]]:
    rows_by_engine_and_rule: Dict[str, Dict[str, Mapping[str, object]]] = {}
    for engine, payload in engine_payloads.items():
        row_map: Dict[str, Mapping[str, object]] = {}
        for row in payload.get("rule_results", []):
            row_map[str(row["rule_name"])] = row
        rows_by_engine_and_rule[engine] = row_map

    rule_names = sorted(set().union(*(set(m.keys()) for m in rows_by_engine_and_rule.values())))
    results: List[Dict[str, object]] = []

    for rule_name in rule_names:
        per_engine: Dict[str, Mapping[str, object]] = {
            engine: rows_by_engine_and_rule[engine][rule_name]
            for engine in ENGINES
            if rule_name in rows_by_engine_and_rule[engine]
        }
        if not per_engine:
            continue
        best_engine, selection_reason, tie_break_applied = _best_engine_for_rule(per_engine)
        reference = next(iter(per_engine.values()))

        rule_out: Dict[str, object] = {
            "rule_name": rule_name,
            "tag": reference.get("tag"),
            "severity": reference.get("severity"),
            "condition": reference.get("condition"),
            "jurisdiction": reference.get("jurisdiction", "global"),
            "profile_tags": list(reference.get("profile_tags", [])),
            "regulatory_type": reference.get("regulatory_type", ""),
            "legal_citation": reference.get("legal_citation", ""),
            "source_url": reference.get("source_url", ""),
            "effective_date": reference.get("effective_date", ""),
            "review_status": reference.get("review_status", ""),
            "reviewer": reference.get("reviewer", ""),
            "required_fields": list(reference.get("required_fields", [])),
            "exemption_logic": reference.get("exemption_logic", ""),
            "rule_notes": reference.get("rule_notes", ""),
            "rule_ir_hash": reference.get("rule_ir_hash"),
            "condition_type": reference.get("condition_type", "unknown"),
            "complexity": reference.get("complexity", "unknown"),
            "declarative_friendly": (
                _is_declarative_friendly(str(reference.get("condition", "")))
                if reference.get("declarative_friendly") is None
                else bool(reference.get("declarative_friendly"))
            ),
            "products_tested": reference.get("products_tested"),
            "best_engine": best_engine,
            "selection_reason": selection_reason,
            "declarative_tie_break_applied": tie_break_applied,
            "recommendation": _rule_recommendation(per_engine[best_engine], best_engine),
            "engines": {},
        }
        migration_state, migration_state_reason = _migration_state(per_engine[best_engine], best_engine)
        rule_out["migration_state"] = migration_state
        rule_out["migration_state_reason"] = migration_state_reason
        for engine, row in per_engine.items():
            conversion_text = row.get("python_conversion", "")
            provider = str(row.get("conversion_provider", "unknown"))
            provider_factor = _provider_factor(engine, provider)
            effective = _effective_confidence(engine, row)
            rule_out["engines"][engine] = {
                "status": row.get("status"),
                "mismatches": row.get("mismatches"),
                "parity_ci_lower": row.get("parity_ci_lower"),
                "overall_confidence": row.get("overall_confidence"),
                "equivalence_match_rate": row.get("equivalence_match_rate", 1.0),
                "equivalence_status": row.get("equivalence_status", "PASS"),
                "equivalence_cases": row.get("equivalence_cases", 0),
                "mutation_score": row.get("mutation_score", 1.0),
                "mutation_total": row.get("mutation_total", 0),
                "mutation_killed": row.get("mutation_killed", 0),
                "verification_score": row.get("verification_score", 1.0),
                "counterexample_repair_applied": row.get("counterexample_repair_applied", False),
                "equivalence_counterexamples": row.get("equivalence_counterexamples", []),
                "effective_confidence": round(effective, 4),
                "provider_factor": round(provider_factor, 4),
                "real_llm_used": _python_provider_is_real_llm(provider) if engine == "python" else None,
                "decision_score": round(_decision_score(engine, row, rule_out["declarative_friendly"]), 4),
                "conversion_provider": provider,
                "conversion_notes": row.get("conversion_notes", ""),
                "execution_mode": row.get("conversion_execution_mode", ""),
                "cloud_connected": bool(row.get("conversion_cloud_connected", False)),
                "cloud_scan_id": row.get("conversion_cloud_scan_id", ""),
                "cloud_scan_url": row.get("conversion_cloud_scan_url", ""),
                "conversion_artifact": conversion_text,
                "conversion_lines": _line_count(conversion_text),
                "failed_test_cases": row.get("failed_test_cases", []),
            }
        results.append(rule_out)
    return results


def _run_for_engines(
    dataset_size: int,
    seed: int,
    source_jsonl: Path | None,
    use_default_off_source: bool,
    llm_provider: str,
    llm_model: str | None,
    perl_rules_dir: Path | None,
    db_path: Path,
    profile: str,
    soda_mode: str,
) -> Dict[str, Dict[str, object]]:
    engine_payloads: Dict[str, Dict[str, object]] = {}
    temp_results_dir = RESULT_PATH.parent / "tmp_engine_runs"
    temp_results_dir.mkdir(parents=True, exist_ok=True)

    for engine in ENGINES:
        engine_results_path = temp_results_dir / f"migration_results_{engine}.json"
        payload = run_pipeline(
            dataset_size=dataset_size,
            seed=seed,
            results_path=engine_results_path,
            source_jsonl=source_jsonl,
            use_default_off_source=use_default_off_source,
            db_path=db_path,
            llm_provider=llm_provider,
            llm_model=llm_model,
            perl_rules_dir=perl_rules_dir,
            execution_engine=engine,
            soda_mode=soda_mode,
            profile=profile,
        )
        engine_payloads[engine] = payload
    return engine_payloads


def run_engine_comparison(
    dataset_size: int = 300,
    seed: int = 17,
    source_jsonl: Path | None = None,
    use_default_off_source: bool = True,
    llm_provider: str = "groq",
    llm_model: str | None = None,
    perl_rules_dir: Path | None = None,
    db_path: Path = DB_PATH,
    results_path: Path = COMPARISON_PATH,
    require_real_llm: bool = False,
    profile: str = DEFAULT_PROFILE,
    soda_mode: str = "local",
) -> Dict[str, object]:
    """Run all engines and emit a rule-by-rule comparison report."""
    engine_payloads = _run_for_engines(
        dataset_size=dataset_size,
        seed=seed,
        source_jsonl=source_jsonl,
        use_default_off_source=use_default_off_source,
        llm_provider=llm_provider,
        llm_model=llm_model,
        perl_rules_dir=perl_rules_dir,
        db_path=db_path,
        profile=profile,
        soda_mode=soda_mode,
    )
    if require_real_llm:
        python_rows = list(engine_payloads.get("python", {}).get("rule_results", []))
        non_llm_rules = [
            str(row.get("rule_name"))
            for row in python_rows
            if not _python_provider_is_real_llm(str(row.get("conversion_provider", "")))
        ]
        if non_llm_rules:
            raise RuntimeError(
                "Real LLM mode is enabled, but python engine used fallback/non-LLM providers "
                f"for rules: {', '.join(non_llm_rules)}. "
                "Set GROQ_API_KEY and verify model access."
            )

    dataset_meta = engine_payloads["python"].get("dataset", {})
    per_engine_summary = {engine: _engine_summary(engine, payload) for engine, payload in engine_payloads.items()}
    rule_comparison = _build_rule_comparison(engine_payloads)
    complexity_summary = _build_complexity_summary(rule_comparison)
    migration_state_summary = _build_migration_state_summary(rule_comparison)
    generated_at_utc = datetime.now(timezone.utc).isoformat()
    comparison_fingerprint = _comparison_fingerprint_payload(
        engine_payloads=engine_payloads,
        generated_at_utc=generated_at_utc,
    )

    report = {
        "generated_at_utc": generated_at_utc,
        "comparison_fingerprint": comparison_fingerprint,
        "comparison_method": {
            "best_engine_ranking": (
                "Prefer MATCH status, then fewer mismatches, then higher effective_confidence. "
                "effective_confidence = overall_confidence * provider_factor."
            ),
            "decision_score": (
                "decision_score = effective_confidence "
                "+ architecture_bonus(declarative for simple rules / python for complex rules) "
                "- mismatch_penalty - review_penalty."
            ),
            "hybrid_tie_break": (
                "If python and best declarative engine are close (within 0.20 effective confidence): "
                "prefer declarative for declarative-friendly rules, prefer python for non-declarative rules."
            ),
            "declarative_tie_break": (
                "When dbt and soda are exactly tied on status/mismatches/scores for a rule, "
                "use a stable hash of rule_name to select dbt or soda explicitly."
            ),
            "provider_factor_notes": {
                "python_real_llm": 1.0,
                "python_simulated_fallback": 0.55,
                "dbt_sql_fallback": 0.85,
                "soda_cloud": 1.0,
                "soda_sql_fallback": 0.85,
            },
        },
        "dataset": dataset_meta,
        "engines": list(ENGINES),
        "engine_run_fingerprints": {
            engine: payload.get("run_fingerprint", {})
            for engine, payload in engine_payloads.items()
        },
        "per_engine_summary": per_engine_summary,
        "per_complexity_summary": complexity_summary,
        "migration_state_summary": migration_state_summary,
        "rule_comparison": rule_comparison,
        "run_config": {
            "llm_provider": llm_provider,
            "llm_model": llm_model,
            "require_real_llm": require_real_llm,
            "groq_api_key_set": bool(os.getenv("GROQ_API_KEY")),
            "profile": profile,
            "dataset_size": dataset_size,
            "seed": seed,
            "mode": "off" if use_default_off_source else "synthetic",
            "source_jsonl": str(source_jsonl) if source_jsonl else "",
            "soda_mode": soda_mode,
            "soda_cloud_credentials_set": bool(
                os.getenv("SODA_CLOUD_API_KEY_ID")
                and os.getenv("SODA_CLOUD_API_KEY_SECRET")
                and os.getenv("SODA_CLOUD_HOST")
            ),
        },
    }

    results_path.parent.mkdir(parents=True, exist_ok=True)
    with results_path.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare python/dbt/soda engines for rule migration parity.")
    parser.add_argument("--size", type=int, default=300, help="Number of products to test.")
    parser.add_argument("--seed", type=int, default=17, help="Seed for synthetic mode.")
    parser.add_argument(
        "--mode",
        choices=["off", "synthetic"],
        default="off" if DEFAULT_OFF_JSONL.exists() else "synthetic",
        help="Dataset source mode: off (JSONL) or synthetic.",
    )
    parser.add_argument(
        "--source-jsonl",
        type=Path,
        default=DEFAULT_OFF_JSONL if DEFAULT_OFF_JSONL.exists() else None,
        help="OFF JSONL path (used when --mode off).",
    )
    parser.add_argument(
        "--llm-provider",
        choices=["simulated", "groq"],
        default="groq",
        help="LLM provider for python execution engine.",
    )
    parser.add_argument("--llm-model", default=None, help="Optional model override for selected LLM provider.")
    parser.add_argument("--perl-rules-dir", type=Path, default=None, help="Optional directory of .pl snippets.")
    parser.add_argument(
        "--profile",
        choices=list(SUPPORTED_PROFILES),
        default=DEFAULT_PROFILE,
        help="Rule-pack profile to compare: global, canada, or hybrid.",
    )
    parser.add_argument("--results-path", type=Path, default=COMPARISON_PATH, help="Output comparison JSON path.")
    parser.add_argument(
        "--require-real-llm",
        action="store_true",
        help="Fail if python engine did not use real LLM providers (no simulated fallback allowed).",
    )
    parser.add_argument(
        "--soda-mode",
        choices=["local", "cloud"],
        default="local",
        help="Soda execution mode for soda engine runs.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    use_off_mode = args.mode == "off"
    source_jsonl = args.source_jsonl if use_off_mode else None

    report = run_engine_comparison(
        dataset_size=args.size,
        seed=args.seed,
        source_jsonl=source_jsonl,
        use_default_off_source=use_off_mode,
        llm_provider=args.llm_provider,
        llm_model=args.llm_model,
        perl_rules_dir=args.perl_rules_dir,
        results_path=args.results_path,
        require_real_llm=args.require_real_llm,
        profile=args.profile,
        soda_mode=args.soda_mode,
    )
    print(f"Engines compared: {', '.join(report['engines'])}")
    for engine, summary in report["per_engine_summary"].items():
        print(
            f"{engine}: passed {summary['passed']}/{summary['rules']}, "
            f"avg_overall={summary['avg_overall_confidence']:.2%}, "
            f"avg_effective={summary['avg_effective_confidence']:.2%}, "
            f"fallback_rules={summary['fallback_rules']}"
        )
    if report.get("migration_state_summary"):
        states = report["migration_state_summary"]
        print(
            "migration states: "
            f"accepted={states.get('accepted', 0)}, "
            f"candidate={states.get('candidate', 0)}, "
            f"review={states.get('review', 0)}"
        )
    if report.get("run_config"):
        print(
            "LLM key status: "
            f"GROQ_API_KEY={report['run_config']['groq_api_key_set']}"
        )
    if report.get("dataset"):
        print(f"Profile: {report['dataset'].get('profile', DEFAULT_PROFILE)}")
    print(f"Soda mode: {args.soda_mode}")
    if report.get("comparison_fingerprint"):
        fingerprint = report["comparison_fingerprint"]
        print(f"Comparison run ID: {fingerprint.get('comparison_run_id', 'n/a')}")
        print(
            "Fingerprints: "
            f"dataset={str(fingerprint.get('dataset_fingerprint_sha256', ''))[:16]} | "
            f"rulepack={str(fingerprint.get('rulepack_fingerprint_sha256', ''))[:16]}"
        )
    print(f"Comparison written to: {args.results_path}")


if __name__ == "__main__":
    main()
