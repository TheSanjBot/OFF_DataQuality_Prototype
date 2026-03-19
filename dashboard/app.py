"""Streamlit dashboard for cross-engine migration comparison."""
from __future__ import annotations

import html
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Mapping

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from rulepacks.registry import DEFAULT_PROFILE, SUPPORTED_PROFILES
from validation.engine_comparison import COMPARISON_PATH, run_engine_comparison

DEFAULT_SOURCE_JSONL = PROJECT_ROOT / "openfoodfacts-products.jsonl"
ENGINE_COLORS = {
    "python": "#2563eb",
    "dbt": "#f97316",
    "soda": "#10b981",
}


def load_report() -> dict:
    if not COMPARISON_PATH.exists():
        return {}
    with COMPARISON_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _inject_theme() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap');
        @import url('https://fonts.googleapis.com/css2?family=Material+Symbols+Rounded:opsz,wght,FILL,GRAD@20..48,400,0,0');
        html, body, [class*="st-"], [class*="css"] {
            font-family: "Space Grotesk", sans-serif;
        }
        [class^="material-symbols"], [class*=" material-symbols"] {
            font-family: "Material Symbols Rounded" !important;
            font-weight: normal;
            font-style: normal;
            font-size: 1rem;
            line-height: 1;
            letter-spacing: normal;
            text-transform: none;
            display: inline-block;
            white-space: nowrap;
            word-wrap: normal;
            direction: ltr;
            -webkit-font-smoothing: antialiased;
        }
        .block-container {
            max-width: 1260px;
            padding-top: 1.15rem;
            padding-bottom: 2rem;
        }
        [data-testid="stMetric"] {
            background: linear-gradient(120deg, #f8fafc 0%, #ecfeff 100%);
            border: 1px solid #dbeafe;
            border-radius: 14px;
            padding: 0.6rem 0.8rem;
            min-height: 118px;
        }
        [data-testid="stMetricLabel"] > div {
            white-space: normal !important;
            line-height: 1.15;
        }
        [data-testid="stMetricValue"] > div {
            line-height: 1.15;
        }
        .stat-chip {
            border-radius: 16px;
            border: 1px solid #dbeafe;
            background: linear-gradient(135deg, #f8fafc 0%, #ecfeff 100%);
            padding: 0.8rem 0.95rem;
            min-height: 112px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            overflow: hidden;
            box-shadow: 0 4px 12px rgba(15, 23, 42, 0.06);
        }
        .stat-chip .label {
            color: #475569;
            font-size: 0.76rem;
            line-height: 1.1rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            word-break: normal;
            overflow-wrap: break-word;
        }
        .stat-chip .value {
            color: #0f172a;
            font-size: 1.28rem;
            font-weight: 700;
            line-height: 1.4rem;
            margin-top: 0.5rem;
            white-space: normal;
            word-break: break-word;
        }
        .hero {
            border-radius: 16px;
            border: 1px solid #cbd5e1;
            background: radial-gradient(circle at 10% 20%, #f0fdfa 0%, #eff6ff 45%, #fff7ed 100%);
            padding: 1rem 1.2rem;
            margin-bottom: 1.1rem;
        }
        .hero h1 {
            margin: 0;
            color: #0b1324;
            font-size: 1.38rem;
            letter-spacing: -0.01em;
        }
        .hero p {
            margin: 0.3rem 0 0 0;
            color: #334155;
            font-size: 0.95rem;
        }
        .explain-chip {
            border-radius: 14px;
            border: 1px solid #bfdbfe;
            background: linear-gradient(135deg, #eff6ff 0%, #f0f9ff 100%);
            padding: 0.8rem 0.95rem;
            margin: 0.6rem 0 0.9rem 0;
            box-shadow: 0 4px 12px rgba(15, 23, 42, 0.05);
        }
        .explain-chip .title {
            color: #1e3a8a;
            font-size: 0.88rem;
            line-height: 1.2rem;
            font-weight: 700;
            margin-bottom: 0.35rem;
            text-transform: uppercase;
            letter-spacing: 0.03em;
        }
        .explain-chip ul {
            margin: 0.1rem 0 0 1rem;
            padding: 0;
        }
        .explain-chip li {
            color: #1e293b;
            font-size: 0.92rem;
            line-height: 1.35rem;
            margin: 0.2rem 0;
        }
        [data-testid="stExpander"] > details > summary {
            display: flex;
            align-items: center;
            gap: 0.5rem;
            line-height: 1.25;
        }
        [data-testid="stExpander"] > details > summary p {
            margin: 0 !important;
            line-height: 1.25 !important;
        }
        [data-testid="stExpander"] > details > summary svg {
            flex-shrink: 0;
            margin-top: 0 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _to_pct(value: object) -> float:
    try:
        return round(float(value) * 100, 2)
    except (TypeError, ValueError):
        return 0.0


def _render_stat_chip(label: str, value: object) -> None:
    st.markdown(
        (
            "<div class='stat-chip'>"
            f"<div class='label'>{label}</div>"
            f"<div class='value'>{value}</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def _render_explain_chip(title: str, lines: List[str]) -> None:
    safe_items = "".join(f"<li>{html.escape(line)}</li>" for line in lines if line)
    st.markdown(
        (
            "<div class='explain-chip'>"
            f"<div class='title'>{html.escape(title)}</div>"
            f"<ul>{safe_items}</ul>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def _engine_summary_frame(per_engine_summary: Mapping[str, Mapping[str, object]]) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    for engine in ("python", "dbt", "soda"):
        summary = per_engine_summary.get(engine, {})
        rows.append(
            {
                "engine": engine.upper(),
                "rules": int(summary.get("rules", 0)),
                "passed": int(summary.get("passed", 0)),
                "avg_overall_confidence_pct": _to_pct(summary.get("avg_overall_confidence", 0.0)),
                "avg_effective_confidence_pct": _to_pct(summary.get("avg_effective_confidence", 0.0)),
                "avg_parity_ci_low_pct": _to_pct(summary.get("avg_parity_ci_lower", 0.0)),
                "avg_equivalence_rate_pct": _to_pct(summary.get("avg_equivalence_rate", 0.0)),
                "avg_mutation_score_pct": _to_pct(summary.get("avg_mutation_score", 0.0)),
                "fallback_rules": int(summary.get("fallback_rules", 0)),
                "real_llm_rules": int(summary.get("real_llm_rules", 0)) if engine == "python" else None,
                "real_llm_rate_pct": _to_pct(summary.get("real_llm_rate", 0.0)) if engine == "python" else None,
                "repairs_applied": int(summary.get("repairs_applied", 0)) if engine == "python" else None,
            }
        )
    return pd.DataFrame(rows)


def _rule_frame(rule_comparison: List[Mapping[str, object]]) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    for item in rule_comparison:
        engines = item.get("engines", {})
        py = engines.get("python", {})
        dbt = engines.get("dbt", {})
        soda = engines.get("soda", {})
        best_engine = str(item.get("best_engine", "python"))

        effective_confidences = {
            "python": float(py.get("effective_confidence", py.get("overall_confidence", 0.0))),
            "dbt": float(dbt.get("effective_confidence", dbt.get("overall_confidence", 0.0))),
            "soda": float(soda.get("effective_confidence", soda.get("overall_confidence", 0.0))),
        }
        sorted_conf = sorted(effective_confidences.values(), reverse=True)
        margin = sorted_conf[0] - sorted_conf[1] if len(sorted_conf) > 1 else sorted_conf[0]
        decision_scores = {
            "python": float(py.get("decision_score", effective_confidences["python"])),
            "dbt": float(dbt.get("decision_score", effective_confidences["dbt"])),
            "soda": float(soda.get("decision_score", effective_confidences["soda"])),
        }
        sorted_scores = sorted(decision_scores.values(), reverse=True)
        score_margin = sorted_scores[0] - sorted_scores[1] if len(sorted_scores) > 1 else sorted_scores[0]

        rows.append(
            {
                "rule_name": item.get("rule_name", ""),
                "severity": item.get("severity", ""),
                "condition": item.get("condition", ""),
                "condition_type": item.get("condition_type", "unknown"),
                "complexity": item.get("complexity", "unknown"),
                "declarative_friendly": bool(item.get("declarative_friendly", False)),
                "jurisdiction": str(item.get("jurisdiction", "global")),
                "regulatory_type": str(item.get("regulatory_type", "")),
                "legal_citation": str(item.get("legal_citation", "")),
                "review_status": str(item.get("review_status", "")),
                "products_tested": int(item.get("products_tested", 0)),
                "best_engine": best_engine,
                "best_effective_pct": _to_pct(effective_confidences.get(best_engine, 0.0)),
                "effective_margin_pct": round(margin * 100, 2),
                "best_decision_score": round(decision_scores.get(best_engine, 0.0), 4),
                "decision_margin": round(score_margin, 4),
                "python_status": py.get("status", "n/a"),
                "dbt_status": dbt.get("status", "n/a"),
                "soda_status": soda.get("status", "n/a"),
                "python_effective_pct": _to_pct(py.get("effective_confidence", py.get("overall_confidence", 0.0))),
                "dbt_effective_pct": _to_pct(dbt.get("effective_confidence", dbt.get("overall_confidence", 0.0))),
                "soda_effective_pct": _to_pct(soda.get("effective_confidence", soda.get("overall_confidence", 0.0))),
                "python_decision_score": round(decision_scores["python"], 4),
                "dbt_decision_score": round(decision_scores["dbt"], 4),
                "soda_decision_score": round(decision_scores["soda"], 4),
                "python_overall_pct": _to_pct(py.get("overall_confidence", 0.0)),
                "dbt_overall_pct": _to_pct(dbt.get("overall_confidence", 0.0)),
                "soda_overall_pct": _to_pct(soda.get("overall_confidence", 0.0)),
                "python_equivalence_pct": _to_pct(py.get("equivalence_match_rate", 1.0)),
                "python_mutation_pct": _to_pct(py.get("mutation_score", 1.0)),
                "python_verification_pct": _to_pct(py.get("verification_score", 1.0)),
                "python_repair_applied": bool(py.get("counterexample_repair_applied", False)),
                "python_mismatches": int(py.get("mismatches", 0)),
                "dbt_mismatches": int(dbt.get("mismatches", 0)),
                "soda_mismatches": int(soda.get("mismatches", 0)),
                "python_real_llm": bool(py.get("real_llm_used", False)),
                "python_provider": py.get("conversion_provider", ""),
                "dbt_provider": dbt.get("conversion_provider", ""),
                "soda_provider": soda.get("conversion_provider", ""),
                "selection_reason": item.get("selection_reason", ""),
                "declarative_tie_break_applied": bool(item.get("declarative_tie_break_applied", False)),
                "recommendation": item.get("recommendation", ""),
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    return frame.sort_values(by=["best_effective_pct", "rule_name"], ascending=[False, True]).reset_index(drop=True)


def _build_effective_chart(frame: pd.DataFrame) -> go.Figure:
    chart = frame[["rule_name", "python_effective_pct", "dbt_effective_pct", "soda_effective_pct"]].copy()
    chart = chart.melt(id_vars=["rule_name"], var_name="engine", value_name="effective_pct")
    chart["engine"] = chart["engine"].str.replace("_effective_pct", "", regex=False).str.upper()
    fig = px.bar(
        chart,
        x="rule_name",
        y="effective_pct",
        color="engine",
        barmode="group",
        color_discrete_map={"PYTHON": ENGINE_COLORS["python"], "DBT": ENGINE_COLORS["dbt"], "SODA": ENGINE_COLORS["soda"]},
    )
    fig.update_layout(
        height=460,
        margin=dict(l=20, r=20, t=30, b=80),
        xaxis=dict(title=None, tickangle=-30),
        yaxis=dict(title="Effective confidence (%)", range=[0, 100]),
        legend=dict(orientation="h", y=1.08, x=0),
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
    )
    return fig


def _build_best_engine_chart(frame: pd.DataFrame) -> go.Figure:
    dist = frame["best_engine"].value_counts().rename_axis("engine").reset_index(name="rules")
    fig = px.pie(
        dist,
        names="engine",
        values="rules",
        hole=0.58,
        color="engine",
        color_discrete_map=ENGINE_COLORS,
    )
    fig.update_layout(height=420, margin=dict(l=5, r=5, t=10, b=10), legend=dict(orientation="h", y=-0.1, x=0))
    fig.update_traces(textinfo="label+value")
    return fig


def _complexity_summary_frame(per_complexity_summary: Mapping[str, Mapping[str, object]]) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    for tier in ("simple", "medium", "intricate", "unknown"):
        info = per_complexity_summary.get(tier)
        if not info:
            continue
        rows.append(
            {
                "complexity": tier,
                "rules": int(info.get("rules", 0)),
                "python_wins": int(info.get("python_wins", 0)),
                "dbt_wins": int(info.get("dbt_wins", 0)),
                "soda_wins": int(info.get("soda_wins", 0)),
                "avg_best_effective_pct": _to_pct(info.get("avg_best_effective_confidence", 0.0)),
            }
        )
    return pd.DataFrame(rows)


def _engine_detail_frame(selected_rule: Mapping[str, object]) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    engines = selected_rule.get("engines", {})
    for engine in ("python", "dbt", "soda"):
        row = engines.get(engine, {})
        rows.append(
            {
                "engine": engine.upper(),
                "status": row.get("status", "n/a"),
                "overall_confidence_pct": _to_pct(row.get("overall_confidence", 0.0)),
                "effective_confidence_pct": _to_pct(row.get("effective_confidence", row.get("overall_confidence", 0.0))),
                "decision_score": float(row.get("decision_score", row.get("effective_confidence", 0.0))),
                "parity_ci_low_pct": _to_pct(row.get("parity_ci_lower", 0.0)),
                "equivalence_pct": _to_pct(row.get("equivalence_match_rate", 1.0)),
                "mutation_pct": _to_pct(row.get("mutation_score", 1.0)),
                "verification_pct": _to_pct(row.get("verification_score", 1.0)),
                "mismatches": int(row.get("mismatches", 0)),
                "provider_factor_pct": _to_pct(row.get("provider_factor", 0.0)),
                "provider": row.get("conversion_provider", ""),
                "execution_mode": row.get("execution_mode", ""),
                "cloud_connected": bool(row.get("cloud_connected", False)),
                "cloud_scan_id": row.get("cloud_scan_id", ""),
                "cloud_scan_url": row.get("cloud_scan_url", ""),
                "real_llm_used": bool(row.get("real_llm_used")) if engine == "python" else None,
                "repair_applied": bool(row.get("counterexample_repair_applied", False)) if engine == "python" else None,
                "artifact_lines": int(row.get("conversion_lines", 0)),
            }
        )
    return pd.DataFrame(rows)


def _engine_artifact(selected_rule: Mapping[str, object], engine: str) -> str:
    return str(selected_rule.get("engines", {}).get(engine, {}).get("conversion_artifact", "")).strip()


def _engine_failed_cases(selected_rule: Mapping[str, object], engine: str) -> List[Mapping[str, object]]:
    return list(selected_rule.get("engines", {}).get(engine, {}).get("failed_test_cases", []))


def _engine_equivalence_counterexamples(selected_rule: Mapping[str, object], engine: str) -> List[Mapping[str, object]]:
    return list(selected_rule.get("engines", {}).get(engine, {}).get("equivalence_counterexamples", []))


def main() -> None:
    st.set_page_config(page_title="OFF Migration Comparison Dashboard", layout="wide")
    _inject_theme()
    st.markdown(
        """
        <div class="hero">
            <h1>Open Food Facts Migration Comparison Dashboard</h1>
            <p>Compare Python (LLM), dbt, and Soda migrations for every rule, side-by-side.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.subheader("Comparison Run")
        size = st.slider("Dataset size", min_value=100, max_value=500, value=300, step=25)
        default_mode = "OFF JSONL" if DEFAULT_SOURCE_JSONL.exists() else "Synthetic"
        dataset_mode = st.radio("Dataset mode", options=["OFF JSONL", "Synthetic"], index=0 if default_mode == "OFF JSONL" else 1)
        use_off_source = dataset_mode == "OFF JSONL"
        seed = st.number_input("Seed (synthetic mode)", min_value=1, max_value=99999, value=17, disabled=use_off_source)
        source_jsonl_text = st.text_input("Source JSONL path", value=str(DEFAULT_SOURCE_JSONL), disabled=not use_off_source)
        perl_rules_dir_text = st.text_input("Perl rules directory (optional)", value="")
        llm_provider = "groq"
        st.caption("Python engine uses Groq for LLM conversion in this dashboard.")
        llm_model = st.text_input("LLM model override", value="openai/gpt-oss-120b")
        soda_mode = st.selectbox(
            "Soda mode",
            options=["local", "cloud"],
            index=0,
            help="Use `cloud` to attempt Soda Cloud scan publishing; falls back to local if unavailable.",
        )
        profile = st.selectbox("Rule profile", options=list(SUPPORTED_PROFILES), index=list(SUPPORTED_PROFILES).index(DEFAULT_PROFILE))
        has_groq_key = bool(os.getenv("GROQ_API_KEY"))
        if not has_groq_key:
            st.warning("GROQ_API_KEY is not set. Python engine will fall back unless key is provided.")

        if st.button("Run Full Comparison"):
            try:
                if not has_groq_key:
                    raise RuntimeError("GROQ_API_KEY is required. Set it before running the comparison.")
                with st.spinner("Running Python + dbt + Soda comparison..."):
                    run_engine_comparison(
                        dataset_size=int(size),
                        seed=int(seed),
                        source_jsonl=Path(source_jsonl_text) if use_off_source else None,
                        use_default_off_source=use_off_source,
                        llm_provider=llm_provider,
                        llm_model=llm_model.strip() or None,
                        perl_rules_dir=Path(perl_rules_dir_text) if perl_rules_dir_text.strip() else None,
                        results_path=COMPARISON_PATH,
                        require_real_llm=True,
                        profile=profile,
                        soda_mode=soda_mode,
                    )
                st.success("Comparison completed.")
            except Exception as exc:  # noqa: BLE001
                st.error(str(exc))

    payload = load_report()
    if not payload:
        st.info("No comparison report found yet. Click 'Run Full Comparison' in the sidebar.")
        return

    dataset = payload.get("dataset", {})
    rule_comparison = list(payload.get("rule_comparison", []))
    engine_summary = payload.get("per_engine_summary", {})
    complexity_summary = payload.get("per_complexity_summary", {})
    run_config = payload.get("run_config", {})
    comparison_method = payload.get("comparison_method", {})
    comparison_fingerprint = payload.get("comparison_fingerprint", {})
    frame = _rule_frame(rule_comparison)
    if frame.empty:
        st.warning("Comparison report has no rule rows.")
        return

    python_wins = int((frame["best_engine"] == "python").sum())
    dbt_wins = int((frame["best_engine"] == "dbt").sum())
    soda_wins = int((frame["best_engine"] == "soda").sum())
    avg_best = frame["best_effective_pct"].mean()
    canada_rules = int((frame["jurisdiction"] == "ca").sum()) if "jurisdiction" in frame.columns else 0

    k1, k2, k3 = st.columns(3, gap="large")
    with k1:
        _render_stat_chip("Rules compared", len(frame))
    with k2:
        _render_stat_chip("Python wins", python_wins)
    with k3:
        _render_stat_chip("dbt wins", dbt_wins)
    k4, k5, k6 = st.columns(3, gap="large")
    with k4:
        _render_stat_chip("Soda wins", soda_wins)
    with k5:
        _render_stat_chip("Avg best effective %", f"{avg_best:.2f}%")
    with k6:
        _render_stat_chip("Canada rules", canada_rules)

    st.caption(f"Generated at: {payload.get('generated_at_utc', 'n/a')}")
    st.caption(f"Dataset size: {dataset.get('products_tested', 'n/a')}")
    st.caption(f"Dataset source: {dataset.get('source_jsonl', 'n/a')}")
    st.caption(f"Profile: {dataset.get('profile', DEFAULT_PROFILE)}")
    st.caption(
        "Confidence context: we compare engines using effective confidence "
        "(overall confidence x provider factor), with status and mismatches prioritized."
    )
    if run_config:
        st.caption(
            f"LLM provider={run_config.get('llm_provider', 'n/a')} | "
            f"GROQ key set={run_config.get('groq_api_key_set')} | "
            f"Require real LLM={run_config.get('require_real_llm')} | "
            f"Run profile={run_config.get('profile', DEFAULT_PROFILE)} | "
            f"Soda mode={run_config.get('soda_mode', 'local')} | "
            f"Soda Cloud creds set={run_config.get('soda_cloud_credentials_set', False)}"
        )
    if comparison_fingerprint:
        st.caption(
            f"Comparison run ID: {comparison_fingerprint.get('comparison_run_id', 'n/a')} | "
            f"Run SHA: {str(comparison_fingerprint.get('comparison_sha256', ''))[:12]}"
        )
        st.caption(
            f"Dataset fingerprint: {str(comparison_fingerprint.get('dataset_fingerprint_sha256', ''))[:16]} | "
            f"Rulepack fingerprint: {str(comparison_fingerprint.get('rulepack_fingerprint_sha256', ''))[:16]} | "
            f"Commit: {str(comparison_fingerprint.get('code_commit', 'unknown'))[:12]}"
        )
        st.caption(
            f"Cross-engine consistency -> "
            f"dataset={comparison_fingerprint.get('dataset_fingerprint_consistent', False)} | "
            f"rulepack={comparison_fingerprint.get('rulepack_fingerprint_consistent', False)}"
        )
    selection_lines: List[str] = [
        str(
            comparison_method.get(
                "best_engine_ranking",
                "Prefer MATCH status, then fewer mismatches, then higher effective confidence.",
            )
        )
    ]
    if comparison_method.get("hybrid_tie_break"):
        selection_lines.append(str(comparison_method["hybrid_tie_break"]))
    if comparison_method.get("declarative_tie_break"):
        selection_lines.append(str(comparison_method["declarative_tie_break"]))
    _render_explain_chip("How Best Migration Is Chosen", selection_lines)

    st.subheader("Engine Summary")
    summary_frame = _engine_summary_frame(engine_summary)
    st.dataframe(
        summary_frame,
        use_container_width=True,
        hide_index=True,
        column_config={
            "engine": st.column_config.TextColumn("Engine"),
            "rules": st.column_config.NumberColumn("Rules", format="%d"),
            "passed": st.column_config.NumberColumn("Passed", format="%d"),
            "avg_overall_confidence_pct": st.column_config.NumberColumn("Avg overall %", format="%.2f"),
            "avg_effective_confidence_pct": st.column_config.NumberColumn("Avg effective %", format="%.2f"),
            "avg_parity_ci_low_pct": st.column_config.NumberColumn("Avg parity CI low %", format="%.2f"),
            "avg_equivalence_rate_pct": st.column_config.NumberColumn("Avg equivalence %", format="%.2f"),
            "avg_mutation_score_pct": st.column_config.NumberColumn("Avg mutation %", format="%.2f"),
            "fallback_rules": st.column_config.NumberColumn("Fallback rules", format="%d"),
            "real_llm_rules": st.column_config.NumberColumn("Real LLM rules", format="%d"),
            "real_llm_rate_pct": st.column_config.NumberColumn("Real LLM rate %", format="%.2f"),
            "repairs_applied": st.column_config.NumberColumn("Repairs applied", format="%d"),
        },
    )

    complexity_frame = _complexity_summary_frame(complexity_summary)
    if not complexity_frame.empty:
        st.subheader("Mixed-Complexity Benchmark Summary")
        st.dataframe(
            complexity_frame,
            use_container_width=True,
            hide_index=True,
            column_config={
                "complexity": st.column_config.TextColumn("Complexity tier"),
                "rules": st.column_config.NumberColumn("Rules", format="%d"),
                "python_wins": st.column_config.NumberColumn("Python wins", format="%d"),
                "dbt_wins": st.column_config.NumberColumn("dbt wins", format="%d"),
                "soda_wins": st.column_config.NumberColumn("Soda wins", format="%d"),
                "avg_best_effective_pct": st.column_config.NumberColumn("Avg best effective %", format="%.2f"),
            },
        )

    left, right = st.columns([0.72, 0.28], gap="large")
    with left:
        st.subheader("Effective Confidence by Rule and Engine")
        st.plotly_chart(_build_effective_chart(frame), use_container_width=True)
    with right:
        st.subheader("Best Engine Distribution")
        st.plotly_chart(_build_best_engine_chart(frame), use_container_width=True)

    st.subheader("Rule Validation Table (Comparison View)")
    show_advanced = st.checkbox("Show advanced metrics", value=False)
    show_providers = st.checkbox("Show provider columns", value=False)
    jurisdictions = sorted(frame["jurisdiction"].dropna().unique().tolist())
    selected_jurisdictions = st.multiselect("Filter jurisdiction", options=jurisdictions, default=jurisdictions)
    table_frame = frame[frame["jurisdiction"].isin(selected_jurisdictions)] if selected_jurisdictions else frame
    columns = [
        "rule_name",
        "jurisdiction",
        "regulatory_type",
        "review_status",
        "legal_citation",
        "complexity",
        "condition_type",
        "severity",
        "best_engine",
        "best_effective_pct",
        "decision_margin",
        "recommendation",
    ]
    if show_advanced:
        columns.extend(
            [
                "best_decision_score",
                "effective_margin_pct",
                "python_effective_pct",
                "dbt_effective_pct",
                "soda_effective_pct",
                "python_equivalence_pct",
                "python_mutation_pct",
                "python_verification_pct",
                "python_repair_applied",
                "python_mismatches",
                "dbt_mismatches",
                "soda_mismatches",
                "python_real_llm",
                "declarative_tie_break_applied",
                "selection_reason",
            ]
        )
    if show_providers:
        columns.extend(["python_provider", "dbt_provider", "soda_provider"])
    st.dataframe(
        table_frame[columns],
        use_container_width=True,
        hide_index=True,
        column_config={
            "rule_name": st.column_config.TextColumn("Rule"),
            "jurisdiction": st.column_config.TextColumn("Jurisdiction"),
            "regulatory_type": st.column_config.TextColumn("Regulatory type"),
            "review_status": st.column_config.TextColumn("Review status"),
            "legal_citation": st.column_config.TextColumn("Legal citation"),
            "complexity": st.column_config.TextColumn("Complexity"),
            "condition_type": st.column_config.TextColumn("Condition type"),
            "severity": st.column_config.TextColumn("Severity"),
            "best_engine": st.column_config.TextColumn("Best migration"),
            "best_effective_pct": st.column_config.NumberColumn("Best effective %", format="%.2f"),
            "best_decision_score": st.column_config.NumberColumn("Best decision score", format="%.4f"),
            "effective_margin_pct": st.column_config.NumberColumn("Win margin %", format="%.2f"),
            "decision_margin": st.column_config.NumberColumn("Score margin", format="%.4f"),
            "python_effective_pct": st.column_config.NumberColumn("Python effective %", format="%.2f"),
            "dbt_effective_pct": st.column_config.NumberColumn("dbt effective %", format="%.2f"),
            "soda_effective_pct": st.column_config.NumberColumn("Soda effective %", format="%.2f"),
            "python_equivalence_pct": st.column_config.NumberColumn("Python equiv %", format="%.2f"),
            "python_mutation_pct": st.column_config.NumberColumn("Python mutation %", format="%.2f"),
            "python_verification_pct": st.column_config.NumberColumn("Python verify %", format="%.2f"),
            "python_repair_applied": st.column_config.CheckboxColumn("Repair applied"),
            "python_mismatches": st.column_config.NumberColumn("Python mismatches", format="%d"),
            "dbt_mismatches": st.column_config.NumberColumn("dbt mismatches", format="%d"),
            "soda_mismatches": st.column_config.NumberColumn("Soda mismatches", format="%d"),
            "python_real_llm": st.column_config.CheckboxColumn("Python real LLM"),
            "declarative_tie_break_applied": st.column_config.CheckboxColumn("dbt/soda tie-break"),
            "selection_reason": st.column_config.TextColumn("Selection reason"),
            "recommendation": st.column_config.TextColumn("Recommendation"),
            "python_provider": st.column_config.TextColumn("Python provider"),
            "dbt_provider": st.column_config.TextColumn("dbt provider"),
            "soda_provider": st.column_config.TextColumn("Soda provider"),
        },
    )

    st.subheader("Rule Detail")
    rule_names = [row.get("rule_name", "") for row in rule_comparison]
    selected_rule_name = st.selectbox("Select rule", rule_names)
    selected_rule = next(row for row in rule_comparison if row.get("rule_name") == selected_rule_name)

    d1, d2, d3 = st.columns(3, gap="large")
    with d1:
        _render_stat_chip("Best migration", str(selected_rule.get("best_engine", "n/a")).upper())
    with d2:
        _render_stat_chip("Severity", str(selected_rule.get("severity", "n/a")))
    with d3:
        _render_stat_chip("Products tested", int(selected_rule.get("products_tested", 0)))
    best_engine = str(selected_rule.get("best_engine", "python"))
    best_effective = selected_rule.get("engines", {}).get(best_engine, {}).get("effective_confidence", 0.0)
    detail_row = frame.loc[frame["rule_name"] == selected_rule_name].iloc[0]
    d4, d5 = st.columns(2, gap="large")
    with d4:
        _render_stat_chip("Best effective %", f"{_to_pct(best_effective):.2f}")
    with d5:
        _render_stat_chip("Decision margin", f"{float(detail_row['decision_margin']):.4f}")

    st.caption(f"Condition: {selected_rule.get('condition', 'n/a')}")
    st.caption(f"Rule IR hash: {selected_rule.get('rule_ir_hash', 'n/a')}")
    st.caption(f"Condition type: {selected_rule.get('condition_type', 'n/a')}")
    st.caption(f"Complexity tier: {selected_rule.get('complexity', 'n/a')}")
    st.caption(f"Declarative friendly: {selected_rule.get('declarative_friendly', 'n/a')}")
    st.caption(f"Jurisdiction: {selected_rule.get('jurisdiction', 'global')}")
    st.caption(f"Regulatory type: {selected_rule.get('regulatory_type', 'n/a')}")
    st.caption(f"Legal citation: {selected_rule.get('legal_citation', 'n/a')}")
    source_url = str(selected_rule.get("source_url", "")).strip()
    if source_url:
        st.markdown(f"Source URL: [{source_url}]({source_url})")
    else:
        st.caption("Source URL: n/a")
    st.caption(f"Effective date: {selected_rule.get('effective_date', 'n/a')}")
    st.caption(f"Review status: {selected_rule.get('review_status', 'n/a')}")
    st.caption(f"Reviewer: {selected_rule.get('reviewer', 'n/a')}")
    st.caption(f"Exemption logic: {selected_rule.get('exemption_logic', 'n/a')}")
    st.caption(f"Selection reason: {selected_rule.get('selection_reason', 'n/a')}")
    st.caption(f"dbt/soda explicit tie-break applied: {selected_rule.get('declarative_tie_break_applied', False)}")
    st.caption(f"Recommendation: {selected_rule.get('recommendation', 'n/a')}")
    soda_meta = selected_rule.get("engines", {}).get("soda", {})
    st.caption(
        "Soda execution: "
        f"mode={soda_meta.get('execution_mode', 'n/a')} | "
        f"cloud_connected={soda_meta.get('cloud_connected', False)} | "
        f"scan_id={soda_meta.get('cloud_scan_id', '') or 'n/a'}"
    )
    soda_scan_url = str(soda_meta.get("cloud_scan_url", "")).strip()
    if soda_scan_url:
        st.markdown(f"Soda Cloud scan URL: [{soda_scan_url}]({soda_scan_url})")

    st.markdown("**Engine metrics for selected rule**")
    st.dataframe(
        _engine_detail_frame(selected_rule),
        use_container_width=True,
        hide_index=True,
        column_config={
            "engine": st.column_config.TextColumn("Engine"),
            "status": st.column_config.TextColumn("Status"),
            "overall_confidence_pct": st.column_config.NumberColumn("Overall %", format="%.2f"),
            "effective_confidence_pct": st.column_config.NumberColumn("Effective %", format="%.2f"),
            "decision_score": st.column_config.NumberColumn("Decision score", format="%.4f"),
            "parity_ci_low_pct": st.column_config.NumberColumn("Parity CI low %", format="%.2f"),
            "equivalence_pct": st.column_config.NumberColumn("Equivalence %", format="%.2f"),
            "mutation_pct": st.column_config.NumberColumn("Mutation %", format="%.2f"),
            "verification_pct": st.column_config.NumberColumn("Verification %", format="%.2f"),
            "mismatches": st.column_config.NumberColumn("Mismatches", format="%d"),
            "provider_factor_pct": st.column_config.NumberColumn("Provider factor %", format="%.2f"),
            "provider": st.column_config.TextColumn("Provider"),
            "execution_mode": st.column_config.TextColumn("Execution mode"),
            "cloud_connected": st.column_config.CheckboxColumn("Cloud connected"),
            "cloud_scan_id": st.column_config.TextColumn("Cloud scan ID"),
            "cloud_scan_url": st.column_config.TextColumn("Cloud scan URL"),
            "real_llm_used": st.column_config.CheckboxColumn("Real LLM used"),
            "repair_applied": st.column_config.CheckboxColumn("Repair applied"),
            "artifact_lines": st.column_config.NumberColumn("Artifact lines", format="%d"),
        },
    )

    st.markdown("**All three migration artifacts for this rule**")
    c1, c2, c3 = st.columns(3, gap="large")
    with c1:
        st.markdown("`PYTHON`")
        st.code(_engine_artifact(selected_rule, "python"), language="python")
    with c2:
        st.markdown("`DBT`")
        st.code(_engine_artifact(selected_rule, "dbt"), language="sql")
    with c3:
        st.markdown("`SODA`")
        st.code(_engine_artifact(selected_rule, "soda"), language="yaml")

    st.markdown("**Python Equivalence Counterexamples**")
    python_counterexamples = _engine_equivalence_counterexamples(selected_rule, "python")
    if python_counterexamples:
        st.dataframe(pd.DataFrame(python_counterexamples), use_container_width=True, hide_index=True)
    else:
        st.success("No Python equivalence counterexamples.")

    st.markdown("**Parity mismatch samples by engine**")
    for engine, title in [("python", "Python"), ("dbt", "dbt"), ("soda", "Soda")]:
        st.markdown(f"`{title}`")
        failed_cases = _engine_failed_cases(selected_rule, engine)
        if failed_cases:
            st.dataframe(pd.DataFrame(failed_cases), use_container_width=True, hide_index=True)
        else:
            st.success(f"No mismatches for {title}.")


if __name__ == "__main__":
    main()
