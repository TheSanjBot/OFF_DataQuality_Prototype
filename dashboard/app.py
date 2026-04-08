"""Streamlit dashboard for cross-engine migration comparison."""
from __future__ import annotations

import html
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Mapping

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from correction.correction_import import load_issue_records, parse_manual_edit
from correction.review_status import (
    ISSUE_STATUS_APPROVED,
    ISSUE_STATUS_PENDING_REVIEW,
    derive_issue_status,
    normalize_issue_status,
)
from orchestrator.correction_runner import run_correction_phase_two, run_correction_phase_three, run_review_validation
from rulepacks.registry import DEFAULT_PROFILE, SUPPORTED_PROFILES
from validation.engine_comparison import COMPARISON_PATH, run_engine_comparison

DEFAULT_SOURCE_JSONL = PROJECT_ROOT / "openfoodfacts-products.jsonl"
RESULTS_DIR = PROJECT_ROOT / "results"
ISSUES_PATH = RESULTS_DIR / "issues.json"
REVIEW_SHEET_PATH = RESULTS_DIR / "review_sheet.csv"
REVIEWED_SHEET_PATH = RESULTS_DIR / "reviewed_sheet.csv"
REVIEW_INSTRUCTIONS_PATH = RESULTS_DIR / "review_instructions.md"
REVIEW_MANIFEST_PATH = RESULTS_DIR / "review_manifest.json"
REVIEW_VALIDATION_SUMMARY_PATH = RESULTS_DIR / "review_validation_summary.json"
PATCHES_PATH = RESULTS_DIR / "correction_patches.jsonl"
AUDIT_LOG_PATH = RESULTS_DIR / "audit_log.jsonl"
CORRECTED_DATASET_PATH = RESULTS_DIR / "corrected_dataset.jsonl"
REVALIDATION_SUMMARY_PATH = RESULTS_DIR / "revalidation_summary.json"
FEEDBACK_STORE_PATH = RESULTS_DIR / "feedback_store.json"
RANKING_WEIGHTS_PATH = RESULTS_DIR / "ranking_weights.json"
APPLY_SUMMARY_PATH = RESULTS_DIR / "apply_summary.json"
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


def _load_optional_json(path: Path) -> object:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _load_optional_jsonl(path: Path) -> List[Dict[str, object]]:
    if not path.exists():
        return []
    rows: List[Dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def _load_optional_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, keep_default_na=False).fillna("")


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
                "migration_state": str(item.get("migration_state", "review")),
                "migration_state_reason": str(item.get("migration_state_reason", "")),
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


def _normalize_review_actions(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "user_action" not in frame.columns:
        return frame
    normalized = frame.copy()
    action = normalized["user_action"].fillna("").astype(str).str.strip().str.lower()
    action = action.replace(
        {
            "approved": "approve",
            "apply": "approve",
            "yes": "approve",
            "y": "approve",
            "rejected": "reject",
            "skip": "reject",
            "no": "reject",
            "n": "reject",
        }
    )
    normalized["user_action_normalized"] = action
    if "issue_status" not in normalized.columns:
        normalized["issue_status"] = ISSUE_STATUS_PENDING_REVIEW
    normalized["issue_status_normalized"] = [
        derive_issue_status(status, action_value)
        for status, action_value in zip(normalized["issue_status"], normalized["user_action_normalized"], strict=False)
    ]
    return normalized


def _feedback_global_frame(feedback_store: Mapping[str, object]) -> pd.DataFrame:
    global_stats = feedback_store.get("global", {})
    if not isinstance(global_stats, Mapping):
        return pd.DataFrame()
    rows: List[Dict[str, object]] = []
    for strategy, stats in global_stats.items():
        if not isinstance(stats, Mapping):
            continue
        accepted = int(stats.get("accepted", 0))
        rejected = int(stats.get("rejected", 0))
        rows.append(
            {
                "fix_strategy": strategy,
                "accepted": accepted,
                "rejected": rejected,
                "net_preference": accepted - rejected,
            }
        )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(by=["net_preference", "accepted"], ascending=[False, False]).reset_index(drop=True)


def _feedback_rule_frame(feedback_store: Mapping[str, object]) -> pd.DataFrame:
    by_rule = feedback_store.get("by_rule", {})
    if not isinstance(by_rule, Mapping):
        return pd.DataFrame()
    rows: List[Dict[str, object]] = []
    for rule_name, stats_by_strategy in by_rule.items():
        if not isinstance(stats_by_strategy, Mapping):
            continue
        for strategy, stats in stats_by_strategy.items():
            if not isinstance(stats, Mapping):
                continue
            accepted = int(stats.get("accepted", 0))
            rejected = int(stats.get("rejected", 0))
            rows.append(
                {
                    "rule_name": str(rule_name),
                    "fix_strategy": str(strategy),
                    "accepted": accepted,
                    "rejected": rejected,
                    "net_preference": accepted - rejected,
                }
            )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(by=["net_preference", "accepted"], ascending=[False, False]).reset_index(drop=True)


def _per_rule_change_frame(summary: Mapping[str, object]) -> pd.DataFrame:
    rows = list(summary.get("per_rule_changes", []))
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    return frame.sort_values(by=["resolved_count", "rule_name"], ascending=[False, True]).reset_index(drop=True)


def _render_download_button(label: str, path: Path, key: str) -> None:
    if not path.exists():
        st.caption(f"{label}: not available yet")
        return
    st.download_button(label, data=path.read_bytes(), file_name=path.name, key=key)


def _clean_editor_value(value: object) -> str:
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null"}:
        return ""
    return text


def _ensure_reviewed_sheet_exists() -> None:
    if REVIEWED_SHEET_PATH.exists() or not REVIEW_SHEET_PATH.exists():
        return
    REVIEWED_SHEET_PATH.write_bytes(REVIEW_SHEET_PATH.read_bytes())


def _load_issue_lookup() -> Dict[str, object]:
    if not ISSUES_PATH.exists():
        return {}
    return load_issue_records(ISSUES_PATH)


def _save_reviewed_sheet(frame: pd.DataFrame) -> None:
    ordered_columns = [
        "issue_id",
        "product_id",
        "rule_name",
        "severity",
        "jurisdiction",
        "issue_description",
        "current_values",
        "rule_explanation",
        "confidence",
        "migration_state",
        "best_engine",
        "recommended_fix",
        "suggested_fix_1",
        "suggested_fix_2",
        "suggested_fix_3",
        "selected_fix_id",
        "manual_edit",
        "user_action",
        "issue_status",
        "review_notes",
        "reviewer_name",
        "reviewed_at_utc",
    ]
    writable = frame.copy()
    for column in ordered_columns:
        if column not in writable.columns:
            writable[column] = ""
    for column in ["selected_fix_id", "manual_edit", "user_action", "review_notes", "reviewer_name", "reviewed_at_utc"]:
        writable[column] = writable[column].apply(_clean_editor_value)
    writable["issue_status"] = writable["issue_status"].apply(normalize_issue_status)
    writable = writable[ordered_columns]
    writable.to_csv(REVIEWED_SHEET_PATH, index=False)


def _issue_fix_options(issue: object) -> Dict[str, str]:
    options = {"": "No suggested fix selected"}
    for candidate in getattr(issue, "fix_candidates", []):
        options[str(candidate.fix_id)] = f"{candidate.fix_id} | {candidate.suggested_change}"
    return options


def _issue_label(row: Mapping[str, object]) -> str:
    return (
        f"{row.get('issue_id', '')} | "
        f"{row.get('rule_name', '')} | "
        f"product {row.get('product_id', '')}"
    )


def _preview_rows_for_issue(
    issue: object,
    selected_fix_id: str,
    manual_edit: str,
) -> tuple[List[Dict[str, object]], str]:
    previews: List[Dict[str, object]] = []
    selected_fix_id = _clean_editor_value(selected_fix_id)
    manual_edit = _clean_editor_value(manual_edit)

    if selected_fix_id:
        selected_fix = next(
            (candidate for candidate in getattr(issue, "fix_candidates", []) if candidate.fix_id == selected_fix_id),
            None,
        )
        if selected_fix is not None and selected_fix.target_field:
            previews.append(
                {
                    "source": "suggested_fix",
                    "field": selected_fix.target_field,
                    "old_value": getattr(issue, "current_values", {}).get(selected_fix.target_field),
                    "new_value": selected_fix.new_value,
                    "fix_strategy": selected_fix.fix_strategy,
                }
            )

    if manual_edit:
        try:
            manual_changes = parse_manual_edit(manual_edit)
        except ValueError as exc:
            return previews, str(exc)
        for field, new_value in manual_changes.items():
            previews.append(
                {
                    "source": "manual_edit",
                    "field": field,
                    "old_value": getattr(issue, "current_values", {}).get(field),
                    "new_value": new_value,
                    "fix_strategy": "manual_edit",
                }
            )
    return previews, ""


def _pending_approved_preview_frame(reviewed_frame: pd.DataFrame, issue_lookup: Mapping[str, object]) -> pd.DataFrame:
    if reviewed_frame.empty:
        return pd.DataFrame()
    rows: List[Dict[str, object]] = []
    approved = reviewed_frame[reviewed_frame.get("issue_status_normalized", pd.Series(dtype="string")) == ISSUE_STATUS_APPROVED]
    for _, row in approved.iterrows():
        issue_id = str(row.get("issue_id", ""))
        issue = issue_lookup.get(issue_id)
        if issue is None:
            continue
        previews, preview_error = _preview_rows_for_issue(
            issue,
            str(row.get("selected_fix_id", "")),
            str(row.get("manual_edit", "")),
        )
        if preview_error:
            rows.append(
                {
                    "issue_id": issue_id,
                    "product_id": str(row.get("product_id", "")),
                    "rule_name": str(row.get("rule_name", "")),
                    "source": "invalid_manual_edit",
                    "field": "",
                    "old_value": "",
                    "new_value": "",
                    "fix_strategy": preview_error,
                }
            )
            continue
        for preview in previews:
            rows.append(
                {
                    "issue_id": issue_id,
                    "product_id": str(row.get("product_id", "")),
                    "rule_name": str(row.get("rule_name", "")),
                    **preview,
                }
            )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _format_file_timestamp(path: Path) -> str:
    if not path.exists():
        return "n/a"
    return pd.Timestamp(path.stat().st_mtime, unit="s").strftime("%Y-%m-%d %H:%M:%S")


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
    migration_state_summary = payload.get("migration_state_summary", {})
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
    g1, g2, g3 = st.columns(3, gap="large")
    with g1:
        _render_stat_chip("Accepted rules", int(migration_state_summary.get("accepted", 0)))
    with g2:
        _render_stat_chip("Candidate rules", int(migration_state_summary.get("candidate", 0)))
    with g3:
        _render_stat_chip("Review rules", int(migration_state_summary.get("review", 0)))

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
        "migration_state",
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
                "migration_state_reason",
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
            "migration_state": st.column_config.TextColumn("Migration state"),
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
            "migration_state_reason": st.column_config.TextColumn("State reason"),
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
    d4, d5, d6 = st.columns(3, gap="large")
    with d4:
        _render_stat_chip("Best effective %", f"{_to_pct(best_effective):.2f}")
    with d5:
        _render_stat_chip("Decision margin", f"{float(detail_row['decision_margin']):.4f}")
    with d6:
        _render_stat_chip("Migration state", str(selected_rule.get("migration_state", "review")).upper())

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
    st.caption(f"Migration state reason: {selected_rule.get('migration_state_reason', 'n/a')}")
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

    st.divider()
    st.subheader("Correction Workflow")
    _render_explain_chip(
        "How the correction workflow works",
        [
            "Phase 1 exports a spreadsheet-style review sheet with ranked fix suggestions for each flagged issue.",
            "Phase 2 imports reviewer decisions, applies only approved fixes in batch, and records every patch in an audit log.",
            "Phase 3 reruns validation on the corrected dataset and measures whether quality improved.",
            "Phase 4 learns from approved and rejected fixes to improve future suggestion order.",
        ],
    )

    review_frame = _load_optional_csv(REVIEW_SHEET_PATH)
    _ensure_reviewed_sheet_exists()
    reviewed_frame = _normalize_review_actions(_load_optional_csv(REVIEWED_SHEET_PATH))
    issue_lookup = _load_issue_lookup()
    patches = _load_optional_jsonl(PATCHES_PATH)
    audit_rows = _load_optional_jsonl(AUDIT_LOG_PATH)
    revalidation_summary = _load_optional_json(REVALIDATION_SUMMARY_PATH)
    feedback_store = _load_optional_json(FEEDBACK_STORE_PATH)
    ranking_weights = _load_optional_json(RANKING_WEIGHTS_PATH)
    apply_summary = _load_optional_json(APPLY_SUMMARY_PATH)

    review_rows = len(review_frame)
    reviewed_rows = len(reviewed_frame)
    approved_rows = int((reviewed_frame.get("issue_status_normalized", pd.Series(dtype="string")) == "approved").sum()) if not reviewed_frame.empty else 0
    rejected_rows = int((reviewed_frame.get("issue_status_normalized", pd.Series(dtype="string")) == "rejected").sum()) if not reviewed_frame.empty else 0
    applied_rows = int((reviewed_frame.get("issue_status_normalized", pd.Series(dtype="string")) == "applied").sum()) if not reviewed_frame.empty else 0
    resolved_rows = int((reviewed_frame.get("issue_status_normalized", pd.Series(dtype="string")) == "revalidated_resolved").sum()) if not reviewed_frame.empty else 0
    changed_products = len({str(row.get("product_id", "")) for row in patches})
    feedback_decisions = int(feedback_store.get("decision_count", 0)) if isinstance(feedback_store, Mapping) else 0
    pending_preview_frame = _pending_approved_preview_frame(reviewed_frame, issue_lookup)

    c1, c2, c3 = st.columns(3, gap="large")
    with c1:
        _render_stat_chip("Review rows", review_rows)
    with c2:
        _render_stat_chip("Approved rows", approved_rows)
    with c3:
        _render_stat_chip("Applied patches", len(patches))
    c4, c5, c6 = st.columns(3, gap="large")
    with c4:
        _render_stat_chip("Applied rows", applied_rows)
    with c5:
        _render_stat_chip("Resolved rows", resolved_rows)
    with c6:
        _render_stat_chip("Feedback decisions", feedback_decisions)

    s1, s2, s3 = st.columns(3, gap="large")
    with s1:
        latest_apply = (
            str(apply_summary.get("generated_at_utc", "n/a"))[:19].replace("T", " ")
            if isinstance(apply_summary, Mapping) and apply_summary
            else _format_file_timestamp(APPLY_SUMMARY_PATH)
        )
        _render_stat_chip("Latest apply run", latest_apply)
    with s2:
        latest_revalidation = _format_file_timestamp(REVALIDATION_SUMMARY_PATH)
        _render_stat_chip("Latest revalidation", latest_revalidation)
    with s3:
        pending_count = len(pending_preview_frame)
        _render_stat_chip("Pending preview changes", pending_count)
    st.caption(f"Changed products in latest apply run: {changed_products}")

    tabs = st.tabs(["Review", "Apply", "Revalidate", "Learning"])

    with tabs[0]:
        left, right = st.columns([0.72, 0.28], gap="large")
        with left:
            if reviewed_frame.empty:
                st.info("No review sheet has been exported yet.")
            else:
                visible_columns = [
                    "product_id",
                    "rule_name",
                    "issue_description",
                    "current_values",
                    "recommended_fix",
                    "suggested_fix_1",
                    "suggested_fix_2",
                    "suggested_fix_3",
                    "selected_fix_id",
                    "manual_edit",
                    "user_action",
                    "issue_status_normalized",
                ]
                existing_columns = [column for column in visible_columns if column in reviewed_frame.columns]
                st.dataframe(reviewed_frame[existing_columns], use_container_width=True, hide_index=True, height=360)
        with right:
            st.markdown("**Artifacts**")
            _render_download_button("Download original review template", REVIEW_SHEET_PATH, "download_review_sheet")
            _render_download_button("Download reviewed sheet", REVIEWED_SHEET_PATH, "download_reviewed_sheet")
            _render_download_button("Download issues JSON", ISSUES_PATH, "download_issues_json")
            _render_download_button("Download review instructions", REVIEW_INSTRUCTIONS_PATH, "download_review_instructions")
            _render_download_button("Download review manifest", REVIEW_MANIFEST_PATH, "download_review_manifest")
            if not reviewed_frame.empty:
                st.caption(f"Reviewed rows loaded: {reviewed_rows}")
                st.caption(f"Approved: {approved_rows} | Rejected: {rejected_rows}")
            else:
                st.caption("No reviewed sheet loaded yet.")
            if st.button("Validate reviewed sheet", key="validate_reviewed_sheet_button"):
                try:
                    summary = run_review_validation(
                        review_sheet_path=REVIEWED_SHEET_PATH if REVIEWED_SHEET_PATH.exists() else REVIEW_SHEET_PATH,
                        issues_path=ISSUES_PATH,
                        review_validation_summary_path=REVIEW_VALIDATION_SUMMARY_PATH,
                    )
                    if summary["valid"]:
                        st.success(
                            f"Reviewed sheet is valid. Approved={summary['approved_rows']} | Pending={summary['pending_rows']}."
                        )
                    else:
                        st.error(
                            f"Reviewed sheet has {summary['error_count']} errors and {summary['warning_count']} warnings."
                        )
                except Exception as exc:  # noqa: BLE001
                    st.error(str(exc))

        if not reviewed_frame.empty and issue_lookup:
            st.markdown("**Review editor**")
            issue_rows = reviewed_frame.to_dict(orient="records")
            label_to_issue_id = {_issue_label(row): str(row.get("issue_id", "")) for row in issue_rows}
            selected_label = st.selectbox(
                "Choose an issue to review",
                options=list(label_to_issue_id.keys()),
                key="correction_issue_picker",
            )
            selected_issue_id = label_to_issue_id[selected_label]
            selected_row = next(row for row in issue_rows if str(row.get("issue_id", "")) == selected_issue_id)
            issue = issue_lookup.get(selected_issue_id)
            if issue is not None:
                st.caption(f"Issue description: {selected_row.get('issue_description', '')}")
                st.caption(f"Current values: {selected_row.get('current_values', '')}")
                st.caption(f"Rule explanation: {selected_row.get('rule_explanation', '')}")
                st.caption(f"Current issue status: {selected_row.get('issue_status_normalized', selected_row.get('issue_status', 'pending_review'))}")

                fix_options = _issue_fix_options(issue)
                fix_ids = list(fix_options.keys())
                current_fix_id = _clean_editor_value(selected_row.get("selected_fix_id", ""))
                selected_fix = st.selectbox(
                    "Suggested fix",
                    options=fix_ids,
                    format_func=lambda fix_id: fix_options.get(fix_id, fix_id),
                    index=fix_ids.index(current_fix_id) if current_fix_id in fix_ids else 0,
                    key=f"selected_fix_{selected_issue_id}",
                )
                manual_edit_value = st.text_input(
                    "Manual edit",
                    value=_clean_editor_value(selected_row.get("manual_edit", "")),
                    key=f"manual_edit_{selected_issue_id}",
                    help="Optional custom change, e.g. field=value or field1=value1; field2=value2",
                )
                reviewer_name_value = st.text_input(
                    "Reviewer name",
                    value=_clean_editor_value(selected_row.get("reviewer_name", "")),
                    key=f"reviewer_name_{selected_issue_id}",
                )
                action_options = ["", "approve", "reject"]
                current_action = _clean_editor_value(selected_row.get("user_action", "")).strip().lower()
                selected_action = st.selectbox(
                    "Reviewer action",
                    options=action_options,
                    index=action_options.index(current_action) if current_action in action_options else 0,
                    key=f"user_action_{selected_issue_id}",
                )
                review_notes_value = st.text_area(
                    "Review notes",
                    value=_clean_editor_value(selected_row.get("review_notes", "")),
                    key=f"review_notes_{selected_issue_id}",
                    height=100,
                )

                preview_rows, preview_error = _preview_rows_for_issue(issue, selected_fix, manual_edit_value)
                st.markdown("**Patch preview for this issue**")
                if preview_error:
                    st.error(preview_error)
                if preview_rows:
                    st.dataframe(pd.DataFrame(preview_rows), use_container_width=True, hide_index=True)
                else:
                    st.info("Choose a suggested fix or enter a manual edit to preview the change.")

                p1, p2 = st.columns(2, gap="large")
                with p1:
                    if st.button("Save this review decision", key=f"save_issue_{selected_issue_id}"):
                        updated_frame = reviewed_frame.copy()
                        match = updated_frame["issue_id"].astype(str) == selected_issue_id
                        updated_frame.loc[match, "selected_fix_id"] = selected_fix
                        updated_frame.loc[match, "manual_edit"] = manual_edit_value
                        updated_frame.loc[match, "user_action"] = selected_action
                        updated_frame.loc[match, "issue_status"] = derive_issue_status(
                            selected_row.get("issue_status", ISSUE_STATUS_PENDING_REVIEW),
                            selected_action,
                        )
                        updated_frame.loc[match, "review_notes"] = review_notes_value
                        updated_frame.loc[match, "reviewer_name"] = reviewer_name_value
                        updated_frame.loc[match, "reviewed_at_utc"] = datetime.now(timezone.utc).isoformat()
                        _save_reviewed_sheet(updated_frame)
                        st.success(f"Saved review decision for issue {selected_issue_id}.")
                        st.rerun()
                with p2:
                    if st.button("Reset this issue row", key=f"reset_issue_{selected_issue_id}"):
                        updated_frame = reviewed_frame.copy()
                        match = updated_frame["issue_id"].astype(str) == selected_issue_id
                        updated_frame.loc[match, ["selected_fix_id", "manual_edit", "user_action", "review_notes", "reviewer_name", "reviewed_at_utc"]] = ""
                        updated_frame.loc[match, "issue_status"] = ISSUE_STATUS_PENDING_REVIEW
                        _save_reviewed_sheet(updated_frame)
                        st.success(f"Cleared review decision for issue {selected_issue_id}.")
                        st.rerun()

    with tabs[1]:
        left, right = st.columns([0.6, 0.4], gap="large")
        with left:
            if not pending_preview_frame.empty:
                st.markdown("**Pending approved changes preview**")
                st.dataframe(pending_preview_frame, use_container_width=True, hide_index=True, height=220)
            if patches:
                st.markdown("**Applied patches**")
                st.dataframe(pd.DataFrame(patches), use_container_width=True, hide_index=True, height=260)
            else:
                st.info("No applied patches yet.")
            if audit_rows:
                st.markdown("**Audit log**")
                st.dataframe(pd.DataFrame(audit_rows), use_container_width=True, hide_index=True, height=260)
        with right:
            st.markdown("**Outputs**")
            _render_download_button("Download patch file", PATCHES_PATH, "download_patch_file")
            _render_download_button("Download audit log", AUDIT_LOG_PATH, "download_audit_log")
            _render_download_button("Download corrected dataset", CORRECTED_DATASET_PATH, "download_corrected_dataset")
            if isinstance(apply_summary, Mapping) and apply_summary:
                st.markdown("**Latest apply summary**")
                st.caption(f"Generated: {apply_summary.get('generated_at_utc', 'n/a')}")
                st.caption(
                    f"Reviewed={apply_summary.get('review_rows', 0)} | "
                    f"Approved={apply_summary.get('approved_rows', 0)} | "
                    f"Rejected={apply_summary.get('rejected_rows', 0)}"
                )
                st.caption(
                    f"Patches={apply_summary.get('applied_patch_count', 0)} | "
                    f"Changed products={apply_summary.get('changed_products', 0)}"
                )
            if not reviewed_frame.empty and ISSUES_PATH.exists():
                if st.button("Apply approved fixes", key="apply_reviewed_fixes"):
                    try:
                        summary = run_correction_phase_two(
                            comparison_path=COMPARISON_PATH,
                            issues_path=ISSUES_PATH,
                            reviewed_sheet_path=REVIEWED_SHEET_PATH,
                            corrected_dataset_path=CORRECTED_DATASET_PATH,
                            patches_path=PATCHES_PATH,
                            audit_log_path=AUDIT_LOG_PATH,
                            source_snapshot_path=RESULTS_DIR / "review_source_dataset.jsonl",
                            feedback_store_path=FEEDBACK_STORE_PATH,
                            apply_summary_path=APPLY_SUMMARY_PATH,
                            review_validation_summary_path=REVIEW_VALIDATION_SUMMARY_PATH,
                        )
                        st.success(
                            f"Applied {summary['applied_patch_count']} patches across {summary['changed_products']} products."
                        )
                        st.rerun()
                    except Exception as exc:  # noqa: BLE001
                        st.error(str(exc))

    with tabs[2]:
        if isinstance(revalidation_summary, Mapping) and revalidation_summary:
            r1, r2, r3 = st.columns(3, gap="large")
            with r1:
                _render_stat_chip("Before issues", int(revalidation_summary.get("before_issue_count", 0)))
            with r2:
                _render_stat_chip("After issues", int(revalidation_summary.get("after_issue_count", 0)))
            with r3:
                _render_stat_chip("Introduced issues", int(revalidation_summary.get("introduced_issue_count", 0)))
            r4, r5, r6 = st.columns(3, gap="large")
            with r4:
                _render_stat_chip("Quality before", f"{_to_pct(revalidation_summary.get('quality_score_before', 0.0)):.2f}%")
            with r5:
                _render_stat_chip("Quality after", f"{_to_pct(revalidation_summary.get('quality_score_after', 0.0)):.2f}%")
            with r6:
                delta = float(revalidation_summary.get("quality_score_delta", 0.0))
                _render_stat_chip("Quality delta", f"{delta * 100:.2f} pts")

            change_frame = _per_rule_change_frame(revalidation_summary)
            if not change_frame.empty:
                st.markdown("**Per-rule change summary**")
                st.dataframe(change_frame, use_container_width=True, hide_index=True, height=320)
                resolved_only = change_frame[change_frame["resolved_count"] > 0]
                if not resolved_only.empty:
                    fig = px.bar(
                        resolved_only,
                        x="rule_name",
                        y="resolved_count",
                        color_discrete_sequence=["#10b981"],
                    )
                    fig.update_layout(
                        height=320,
                        margin=dict(l=20, r=20, t=20, b=80),
                        xaxis=dict(title=None, tickangle=-30),
                        yaxis=dict(title="Resolved issues"),
                        plot_bgcolor="#ffffff",
                        paper_bgcolor="#ffffff",
                    )
                    st.plotly_chart(fig, use_container_width=True)
            _render_download_button("Download revalidation summary", REVALIDATION_SUMMARY_PATH, "download_revalidation_summary")
            st.markdown("**Latest revalidation run**")
            st.caption(f"Updated: {_format_file_timestamp(REVALIDATION_SUMMARY_PATH)}")
            st.caption(
                f"Before={revalidation_summary.get('before_issue_count', 0)} | "
                f"After={revalidation_summary.get('after_issue_count', 0)} | "
                f"Resolved={revalidation_summary.get('resolved_issue_count', 0)} | "
                f"Introduced={revalidation_summary.get('introduced_issue_count', 0)}"
            )
        else:
            st.info("No revalidation summary available yet.")
        if CORRECTED_DATASET_PATH.exists():
            if st.button("Run revalidation on corrected dataset", key="run_revalidation_button"):
                try:
                    summary = run_correction_phase_three(
                        comparison_path=COMPARISON_PATH,
                        corrected_dataset_path=CORRECTED_DATASET_PATH,
                        reviewed_sheet_path=REVIEWED_SHEET_PATH,
                        patches_path=PATCHES_PATH,
                        audit_log_path=AUDIT_LOG_PATH,
                        revalidated_comparison_path=RESULTS_DIR / "revalidated_engine_comparison.json",
                        revalidation_summary_path=REVALIDATION_SUMMARY_PATH,
                        revalidation_db_path=RESULTS_DIR / "revalidation_off_quality.db",
                    )
                    st.success(
                        f"Revalidation complete: resolved {summary['resolved_issue_count']} issues, introduced {summary['introduced_issue_count']}."
                    )
                    st.rerun()
                except Exception as exc:  # noqa: BLE001
                    st.error(str(exc))

    with tabs[3]:
        if isinstance(feedback_store, Mapping) and feedback_store:
            st.caption(f"Last updated: {feedback_store.get('last_updated', 'n/a')}")
            global_feedback = _feedback_global_frame(feedback_store)
            rule_feedback = _feedback_rule_frame(feedback_store)
            if not global_feedback.empty:
                st.markdown("**Global fix-strategy preferences**")
                st.dataframe(global_feedback, use_container_width=True, hide_index=True, height=220)
            if not rule_feedback.empty:
                st.markdown("**Per-rule feedback**")
                st.dataframe(rule_feedback, use_container_width=True, hide_index=True, height=260)
            if isinstance(ranking_weights, list) and ranking_weights:
                st.markdown("**Current ranking weights snapshot**")
                st.dataframe(pd.DataFrame(ranking_weights[:25]), use_container_width=True, hide_index=True, height=260)
            _render_download_button("Download feedback store", FEEDBACK_STORE_PATH, "download_feedback_store")
            _render_download_button("Download ranking weights", RANKING_WEIGHTS_PATH, "download_ranking_weights")
        else:
            st.info("No feedback learning data available yet.")


if __name__ == "__main__":
    main()
