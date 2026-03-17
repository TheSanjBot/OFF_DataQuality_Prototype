"""Back-to-back validation engine for Perl-to-Python rule migration."""
from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Dict, List, Mapping, Sequence, Set

from declarative.check_runners import run_declarative_checks
from data.load_dataset import (
    DB_PATH,
    DEFAULT_OFF_JSONL,
    SAMPLE_FILE,
    create_and_load_dataset,
)
from duckdb_utils.create_tables import count_violations, sample_violations
from extractor.perl_logic_extractor import extract_rules
from migration.llm_converter import convert_rules, repair_conversion_with_counterexamples
from perl_checks.legacy_checks import LEGACY_RULES, get_legacy_rule_map, get_perl_rule_snippets, run_perl_checks
from python_checks.generated_checks import compile_generated_checks
from validation.verification import run_rule_verification

RESULT_PATH = Path(__file__).resolve().parent.parent / "results" / "migration_results.json"
TABLE_NAME = "nutrition_table"


def _wilson_interval(successes: int, trials: int, z: float = 1.96) -> tuple[float, float]:
    """Return two-sided Wilson score interval for a binomial proportion."""
    if trials <= 0:
        return 0.0, 1.0
    p = successes / trials
    z2 = z * z
    denom = 1.0 + (z2 / trials)
    center = (p + (z2 / (2.0 * trials))) / denom
    margin = (z * (((p * (1.0 - p)) + (z2 / (4.0 * trials))) / trials) ** 0.5) / denom
    lower = max(0.0, center - margin)
    upper = min(1.0, center + margin)
    return lower, upper


def _beta_continued_fraction(a: float, b: float, x: float, max_iter: int = 400, eps: float = 3e-12) -> float:
    """Continued fraction helper for incomplete beta evaluation."""
    qab = a + b
    qap = a + 1.0
    qam = a - 1.0
    c = 1.0
    d = 1.0 - (qab * x / qap)
    if abs(d) < 1e-30:
        d = 1e-30
    d = 1.0 / d
    h = d

    for m in range(1, max_iter + 1):
        m2 = 2 * m
        aa = (m * (b - m) * x) / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        if abs(d) < 1e-30:
            d = 1e-30
        c = 1.0 + aa / c
        if abs(c) < 1e-30:
            c = 1e-30
        d = 1.0 / d
        h *= d * c

        aa = (-(a + m) * (qab + m) * x) / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        if abs(d) < 1e-30:
            d = 1e-30
        c = 1.0 + aa / c
        if abs(c) < 1e-30:
            c = 1e-30
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < eps:
            break

    return h


def _regularized_incomplete_beta(a: float, b: float, x: float) -> float:
    """Regularized incomplete beta I_x(a,b) in [0,1]."""
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0

    bt = math.exp(
        math.lgamma(a + b)
        - math.lgamma(a)
        - math.lgamma(b)
        + a * math.log(x)
        + b * math.log(1.0 - x)
    )

    if x < (a + 1.0) / (a + b + 2.0):
        return bt * _beta_continued_fraction(a, b, x) / a
    return 1.0 - (bt * _beta_continued_fraction(b, a, 1.0 - x) / b)


def _beta_ppf(probability: float, alpha: float, beta: float, tol: float = 1e-7, max_iter: int = 200) -> float:
    """Inverse CDF for Beta(alpha, beta) using monotonic bisection."""
    p = min(1.0, max(0.0, probability))
    lo = 0.0
    hi = 1.0
    for _ in range(max_iter):
        mid = (lo + hi) / 2.0
        cdf_mid = _regularized_incomplete_beta(alpha, beta, mid)
        if abs(cdf_mid - p) < tol:
            return mid
        if cdf_mid < p:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0


def _run_python_checks(
    products: Sequence[Mapping[str, object]],
    structured_rules: Sequence[Dict[str, object]],
    python_checks: Dict[str, object],
) -> Dict[str, Dict[str, object]]:
    rule_names = [str(rule["rule_name"]) for rule in structured_rules]
    per_rule_products: Dict[str, Set[str]] = {rule_name: set() for rule_name in rule_names}
    per_product_tags: Dict[str, List[str]] = {}

    for product in products:
        product_id = str(product.get("product_id"))
        tags: List[str] = []
        for rule in structured_rules:
            rule_name = str(rule["rule_name"])
            check_fn = python_checks[rule_name]
            tag = check_fn(product)
            if tag:
                tags.append(str(tag))
                per_rule_products[rule_name].add(product_id)
        per_product_tags[product_id] = tags

    return {
        "per_product": per_product_tags,
        "per_rule": {name: sorted(ids) for name, ids in per_rule_products.items()},
    }


def _run_python_verification(
    structured_rules: Sequence[Dict[str, object]],
    python_checks: Mapping[str, object],
    conversion_metadata: Mapping[str, Mapping[str, object]],
    seed: int,
) -> Dict[str, Dict[str, object]]:
    legacy_map = get_legacy_rule_map(LEGACY_RULES)
    verification_by_rule: Dict[str, Dict[str, object]] = {}
    for rule in structured_rules:
        rule_name = str(rule["rule_name"])
        legacy_rule = legacy_map.get(rule_name)
        if legacy_rule is None:
            verification_by_rule[rule_name] = {
                "equivalence_cases": 0,
                "equivalence_matches": 0,
                "equivalence_mismatches": 0,
                "equivalence_match_rate": 1.0,
                "equivalence_status": "PASS",
                "counterexamples": [],
                "mutation_total": 0,
                "mutation_killed": 0,
                "mutation_survived": 0,
                "mutation_score": 1.0,
                "mutation_survived_mutants": [],
                "verification_score": 1.0,
            }
            continue
        verification_by_rule[rule_name] = run_rule_verification(
            rule=rule,
            perl_evaluator=legacy_rule.evaluator,
            check_fn=python_checks[rule_name],
            python_code=str(conversion_metadata[rule_name]["python_code"]),
            function_name=str(conversion_metadata[rule_name]["function_name"]),
            seed=seed,
        )
    return verification_by_rule


def _build_failed_case_rows(
    mismatch_ids: Sequence[str],
    product_map: Dict[str, Mapping[str, object]],
    perl_ids: Set[str],
    python_ids: Set[str],
    limit: int = 10,
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for product_id in mismatch_ids[:limit]:
        product = dict(product_map[product_id])
        rows.append(
            {
                "product_id": product_id,
                "perl_triggered": product_id in perl_ids,
                "python_triggered": product_id in python_ids,
                "energy_kj": product.get("energy_kj"),
                "energy_kj_computed": product.get("energy_kj_computed"),
                "energy_kcal": product.get("energy_kcal"),
                "fat": product.get("fat"),
                "saturated_fat": product.get("saturated_fat"),
                "carbohydrates": product.get("carbohydrates"),
                "sugars": product.get("sugars"),
                "starch": product.get("starch"),
                "lc": product.get("lc"),
                "lang": product.get("lang"),
                "language_code": product.get("language_code"),
            }
        )
    return rows


def _compute_rule_result(
    rule: Dict[str, object],
    perl_rule_products: Sequence[str],
    python_rule_products: Sequence[str],
    product_map: Dict[str, Mapping[str, object]],
    conversion_meta: Dict[str, object],
    verification_meta: Mapping[str, object] | None = None,
    db_path: Path = DB_PATH,
) -> Dict[str, object]:
    product_ids = set(product_map)
    perl_ids = set(perl_rule_products)
    python_ids = set(python_rule_products)
    supporting_ids = perl_ids | python_ids

    matching_products = {
        product_id
        for product_id in product_ids
        if (product_id in perl_ids) == (product_id in python_ids)
    }
    mismatch_ids = sorted(product_ids - matching_products)
    total_tests = len(product_ids)
    supporting_violations = len(supporting_ids)
    positive_matches = len(perl_ids & python_ids)
    positive_coverage = (supporting_violations / total_tests) if total_tests else 0.0

    parity_confidence = (len(matching_products) / total_tests) if total_tests else 1.0
    parity_ci_lower, parity_ci_upper = _wilson_interval(len(matching_products), total_tests)
    coverage_ci_lower, coverage_ci_upper = _wilson_interval(supporting_violations, total_tests)
    positive_agreement = (positive_matches / supporting_violations) if supporting_violations else 0.5
    evidence_alpha = positive_matches + 1.0
    evidence_beta = (supporting_violations - positive_matches) + 1.0
    # Posterior mean for reference.
    evidence_posterior_mean = evidence_alpha / (evidence_alpha + evidence_beta)
    # Conservative 95% lower credible bound.
    evidence_ci_lower = _beta_ppf(0.05, evidence_alpha, evidence_beta)
    evidence_ci_upper = _beta_ppf(0.95, evidence_alpha, evidence_beta)
    llm_confidence = float(conversion_meta["llm_confidence"])
    overall_confidence = llm_confidence * parity_ci_lower * evidence_ci_lower
    status = "MATCH" if not mismatch_ids else "REVIEW"

    duckdb_condition = str(rule["duckdb_condition"])
    duckdb_error_count = count_violations(duckdb_condition, db_path=db_path)
    duckdb_examples = sample_violations(duckdb_condition, limit=5, db_path=db_path)
    verification = dict(verification_meta or {})

    return {
        "rule_name": rule["rule_name"],
        "tag": rule["tag"],
        "severity": rule["severity"],
        "condition": rule["condition"],
        "rule_ir": rule.get("rule_ir"),
        "rule_ir_hash": rule.get("rule_ir_hash"),
        "condition_type": rule.get("condition_type", "unknown"),
        "complexity": rule.get("complexity", "unknown"),
        "declarative_friendly": rule.get("declarative_friendly"),
        "products_tested": total_tests,
        "perl_errors": len(perl_ids),
        "python_errors": len(python_ids),
        "supporting_violations": supporting_violations,
        "positive_matches": positive_matches,
        "positive_agreement": round(positive_agreement, 4),
        "positive_coverage": round(positive_coverage, 4),
        "parity_ci_lower": round(parity_ci_lower, 4),
        "parity_ci_upper": round(parity_ci_upper, 4),
        "coverage_ci_lower": round(coverage_ci_lower, 4),
        "coverage_ci_upper": round(coverage_ci_upper, 4),
        "evidence_alpha": round(evidence_alpha, 4),
        "evidence_beta": round(evidence_beta, 4),
        "evidence_posterior_mean": round(evidence_posterior_mean, 4),
        "evidence_ci_lower": round(evidence_ci_lower, 4),
        "evidence_ci_upper": round(evidence_ci_upper, 4),
        # Back-compat alias for old dashboards/scripts.
        "evidence_factor": round(evidence_ci_lower, 4),
        "matches": len(matching_products),
        "mismatches": len(mismatch_ids),
        "confidence": round(parity_confidence, 4),
        "llm_confidence": round(llm_confidence, 4),
        "overall_confidence": round(overall_confidence, 4),
        "overall_method": "llm_confidence * parity_ci_lower_95 * evidence_ci_lower_95_beta_posterior",
        "status": status,
        "duckdb_query": f"SELECT * FROM {TABLE_NAME} WHERE {duckdb_condition}",
        "duckdb_errors": duckdb_error_count,
        "duckdb_condition": duckdb_condition,
        "duckdb_example_rows": duckdb_examples,
        "equivalence_cases": int(verification.get("equivalence_cases", 0)),
        "equivalence_matches": int(verification.get("equivalence_matches", 0)),
        "equivalence_mismatches": int(verification.get("equivalence_mismatches", 0)),
        "equivalence_match_rate": round(float(verification.get("equivalence_match_rate", 1.0)), 4),
        "equivalence_status": verification.get("equivalence_status", "PASS"),
        "equivalence_counterexamples": list(verification.get("counterexamples", [])),
        "mutation_total": int(verification.get("mutation_total", 0)),
        "mutation_killed": int(verification.get("mutation_killed", 0)),
        "mutation_survived": int(verification.get("mutation_survived", 0)),
        "mutation_score": round(float(verification.get("mutation_score", 1.0)), 4),
        "mutation_survived_mutants": list(verification.get("mutation_survived_mutants", [])),
        "verification_score": round(float(verification.get("verification_score", 1.0)), 4),
        "counterexample_repair_attempted": bool(verification.get("counterexample_repair_attempted", False)),
        "counterexample_repair_applied": bool(verification.get("counterexample_repair_applied", False)),
        "counterexample_repair_error": str(verification.get("counterexample_repair_error", "")),
        "mismatch_product_ids": mismatch_ids,
        "failed_test_cases": _build_failed_case_rows(
            mismatch_ids=mismatch_ids,
            product_map=product_map,
            perl_ids=perl_ids,
            python_ids=python_ids,
            limit=10,
        ),
        "perl_logic": rule["perl_logic"],
        "python_conversion": conversion_meta["python_code"],
        "conversion_notes": conversion_meta["conversion_notes"],
        "conversion_provider": conversion_meta.get("provider", "unknown"),
    }


def run_pipeline(
    dataset_size: int = 300,
    seed: int = 17,
    results_path: Path = RESULT_PATH,
    source_jsonl: Path | None = None,
    use_default_off_source: bool = True,
    db_path: Path = DB_PATH,
    llm_provider: str = "groq",
    llm_model: str | None = None,
    perl_rules_dir: Path | None = None,
    execution_engine: str = "python",
) -> Dict[str, object]:
    """Run the full migration prototype pipeline and persist JSON results."""
    if execution_engine not in {"python", "dbt", "soda"}:
        raise ValueError("execution_engine must be one of: python, dbt, soda")

    source_path = source_jsonl
    if source_path is None and use_default_off_source and DEFAULT_OFF_JSONL.exists():
        source_path = DEFAULT_OFF_JSONL

    products = create_and_load_dataset(size=dataset_size, seed=seed, db_path=db_path, source_jsonl=source_path)
    product_map = {str(product["product_id"]): product for product in products}

    perl_output = run_perl_checks(products, LEGACY_RULES)
    structured_rules = extract_rules(get_perl_rule_snippets(LEGACY_RULES, rules_dir=perl_rules_dir))
    engine_run: Dict[str, object] | None = None
    verification_by_rule: Dict[str, Dict[str, object]] = {}

    if execution_engine == "python":
        converted_rules = convert_rules(structured_rules, provider=llm_provider, model=llm_model)
        converted_by_name = {str(item["rule_name"]): dict(item) for item in converted_rules}
        repair_flags: Dict[str, Dict[str, object]] = {
            str(rule["rule_name"]): {
                "counterexample_repair_attempted": False,
                "counterexample_repair_applied": False,
                "counterexample_repair_error": "",
            }
            for rule in structured_rules
        }

        python_checks, conversion_metadata = compile_generated_checks(
            [converted_by_name[str(rule["rule_name"])] for rule in structured_rules]
        )
        initial_verification = _run_python_verification(
            structured_rules=structured_rules,
            python_checks=python_checks,
            conversion_metadata=conversion_metadata,
            seed=seed,
        )

        if llm_provider == "groq":
            for rule in structured_rules:
                rule_name = str(rule["rule_name"])
                verification = initial_verification.get(rule_name, {})
                provider = str(conversion_metadata[rule_name].get("provider", ""))
                if provider != "groq":
                    continue
                if int(verification.get("equivalence_mismatches", 0)) <= 0:
                    continue

                repair_flags[rule_name]["counterexample_repair_attempted"] = True
                try:
                    repaired = repair_conversion_with_counterexamples(
                        rule=rule,
                        converted_rule=converted_by_name[rule_name],
                        counterexamples=list(verification.get("counterexamples", [])),
                        provider=llm_provider,
                        model=llm_model,
                    )
                    if str(repaired.get("python_code", "")) != str(converted_by_name[rule_name].get("python_code", "")):
                        converted_by_name[rule_name] = repaired
                        repair_flags[rule_name]["counterexample_repair_applied"] = True
                except Exception as exc:  # noqa: BLE001
                    repair_flags[rule_name]["counterexample_repair_error"] = f"{exc.__class__.__name__}: {exc}"

        python_checks, conversion_metadata = compile_generated_checks(
            [converted_by_name[str(rule["rule_name"])] for rule in structured_rules]
        )
        verification_by_rule = _run_python_verification(
            structured_rules=structured_rules,
            python_checks=python_checks,
            conversion_metadata=conversion_metadata,
            seed=seed,
        )
        for rule in structured_rules:
            rule_name = str(rule["rule_name"])
            verification_by_rule.setdefault(rule_name, {}).update(repair_flags.get(rule_name, {}))

        candidate_output = _run_python_checks(products, structured_rules, python_checks)
    else:
        declarative_result = run_declarative_checks(
            rules=structured_rules,
            products=products,
            db_path=db_path,
            engine=execution_engine,
        )
        candidate_output = {
            "per_product": declarative_result["per_product"],
            "per_rule": declarative_result["per_rule"],
        }
        conversion_metadata = declarative_result["conversion_metadata"]
        engine_run = declarative_result["engine_run"]

    rule_results: List[Dict[str, object]] = []
    for rule in structured_rules:
        rule_name = str(rule["rule_name"])
        rule_result = _compute_rule_result(
            rule=rule,
            perl_rule_products=perl_output["per_rule"][rule_name],
            python_rule_products=candidate_output["per_rule"][rule_name],
            product_map=product_map,
            conversion_meta=conversion_metadata[rule_name],
            verification_meta=verification_by_rule.get(rule_name),
            db_path=db_path,
        )
        rule_results.append(rule_result)

    passed_rules = sum(1 for row in rule_results if row["status"] == "MATCH")
    total_rules = len(rule_results)
    avg_confidence = mean([row["overall_confidence"] for row in rule_results]) if rule_results else 0.0

    result_payload: Dict[str, object] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "dataset": {
            "jsonl_path": str(SAMPLE_FILE),
            "duckdb_path": str(db_path),
            "products_tested": len(products),
            "source_jsonl": str(source_path) if source_path else "synthetic",
            "perl_rules_source": str(perl_rules_dir) if perl_rules_dir else "inline_legacy_rules",
            "execution_engine": execution_engine,
        },
        "migration_summary": {
            "total_rules": total_rules,
            "passed_rules": passed_rules,
            "rules_needing_review": total_rules - passed_rules,
            "average_overall_confidence": round(avg_confidence, 4),
        },
        "rule_results": rule_results,
    }
    if engine_run is not None:
        result_payload["declarative_engine_run"] = engine_run

    results_path.parent.mkdir(parents=True, exist_ok=True)
    with results_path.open("w", encoding="utf-8") as handle:
        json.dump(result_payload, handle, indent=2)
    return result_payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Perl/Python parity validation prototype.")
    parser.add_argument("--size", type=int, default=300, help="Number of products to generate.")
    parser.add_argument(
        "--seed",
        type=int,
        default=17,
        help="Random seed for synthetic data generation (ignored when --source-jsonl is set).",
    )
    parser.add_argument(
        "--source-jsonl",
        type=Path,
        default=DEFAULT_OFF_JSONL if DEFAULT_OFF_JSONL.exists() else None,
        help="OFF JSONL source path. Defaults to ./openfoodfacts-products.jsonl when present.",
    )
    parser.add_argument(
        "--llm-provider",
        choices=["simulated", "groq"],
        default="groq",
        help="Rule conversion provider.",
    )
    parser.add_argument(
        "--llm-model",
        default=None,
        help="Optional model override (for selected LLM provider).",
    )
    parser.add_argument(
        "--perl-rules-dir",
        type=Path,
        default=None,
        help="Optional directory containing .pl rule snippets for extractor input.",
    )
    parser.add_argument(
        "--execution-engine",
        choices=["python", "dbt", "soda"],
        default="python",
        help="Check execution engine for parity target: python (LLM converted), dbt, or soda.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    results = run_pipeline(
        dataset_size=args.size,
        seed=args.seed,
        source_jsonl=args.source_jsonl,
        llm_provider=args.llm_provider,
        llm_model=args.llm_model,
        perl_rules_dir=args.perl_rules_dir,
        execution_engine=args.execution_engine,
    )
    summary = results["migration_summary"]
    print(f"Rules analyzed: {summary['total_rules']}")
    print(f"Passed rules: {summary['passed_rules']}")
    print(f"Rules needing review: {summary['rules_needing_review']}")
    print(f"Dataset source: {results['dataset']['source_jsonl']}")
    print(f"Execution engine: {results['dataset'].get('execution_engine', 'python')}")
    print(f"Results written to: {RESULT_PATH}")


if __name__ == "__main__":
    main()
