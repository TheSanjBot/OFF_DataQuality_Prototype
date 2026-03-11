"""Streamlit dashboard for migration prototype outputs."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from validation.parity_validator import run_pipeline

RESULT_PATH = Path(__file__).resolve().parent.parent / "results" / "migration_results.json"
DEFAULT_SOURCE_JSONL = PROJECT_ROOT / "openfoodfacts-products.jsonl"
STATUS_COLORS = {
    "MATCH": "#0f766e",
    "REVIEW": "#be123c",
}


def load_results() -> dict:
    if not RESULT_PATH.exists():
        return {}
    with RESULT_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _rule_dataframe(rule_results: list[dict]) -> pd.DataFrame:
    if not rule_results:
        return pd.DataFrame()
    frame = pd.DataFrame(rule_results)
    default_values = {
        "rule_name": "unknown_rule",
        "severity": "warning",
        "products_tested": 0,
        "perl_errors": 0,
        "python_errors": 0,
        "supporting_violations": 0,
        "positive_matches": 0,
        "positive_agreement": 0.0,
        "positive_coverage": 0.0,
        "parity_ci_lower": 0.0,
        "parity_ci_upper": 0.0,
        "coverage_ci_lower": 0.0,
        "coverage_ci_upper": 0.0,
        "evidence_alpha": 1.0,
        "evidence_beta": 1.0,
        "evidence_posterior_mean": 0.5,
        "evidence_ci_lower": 0.05,
        "evidence_ci_upper": 0.95,
        "evidence_factor": 0.0,
        "matches": 0,
        "mismatches": 0,
        "confidence": 0.0,
        "llm_confidence": 0.0,
        "overall_confidence": 0.0,
        "status": "REVIEW",
        "tag": "",
    }
    for column, default in default_values.items():
        if column not in frame.columns:
            frame[column] = default

    # Backward compatibility for results generated before evidence fields existed.
    if "supporting_violations" in frame.columns and (
        frame["supporting_violations"].isna().any() or (frame["supporting_violations"] == 0).all()
    ):
        if "perl_errors" in frame.columns and "python_errors" in frame.columns:
            frame["supporting_violations"] = frame[["perl_errors", "python_errors"]].max(axis=1)
    if "positive_coverage" in frame.columns and (
        frame["positive_coverage"].isna().any() or (frame["positive_coverage"] == 0).all()
    ):
        denom = frame["products_tested"].replace(0, 1)
        frame["positive_coverage"] = frame["supporting_violations"] / denom
    if "evidence_factor" in frame.columns and (
        frame["evidence_factor"].isna().any() or (frame["evidence_factor"] == 0).all()
    ):
        frame["evidence_factor"] = (frame["positive_coverage"] / 0.05).clip(upper=1.0)
    if "evidence_posterior_mean" in frame.columns and (
        frame["evidence_posterior_mean"].isna().any() or (frame["evidence_posterior_mean"] == 0).all()
    ):
        frame["evidence_posterior_mean"] = frame["evidence_factor"].replace(0, 0.5)
    if "evidence_ci_lower" in frame.columns and (
        frame["evidence_ci_lower"].isna().any() or (frame["evidence_ci_lower"] == 0).all()
    ):
        frame["evidence_ci_lower"] = frame["evidence_factor"].replace(0, 0.05)
    if "evidence_ci_upper" in frame.columns and (
        frame["evidence_ci_upper"].isna().any() or (frame["evidence_ci_upper"] == 0).all()
    ):
        frame["evidence_ci_upper"] = frame["evidence_posterior_mean"].clip(lower=0.05, upper=0.99)
    if "evidence_alpha" in frame.columns and (
        frame["evidence_alpha"].isna().any() or (frame["evidence_alpha"] == 0).all()
    ):
        frame["evidence_alpha"] = frame["positive_matches"] + 1.0
    if "evidence_beta" in frame.columns and (
        frame["evidence_beta"].isna().any() or (frame["evidence_beta"] == 0).all()
    ):
        frame["evidence_beta"] = (frame["supporting_violations"] - frame["positive_matches"]).clip(lower=0) + 1.0
    if "positive_agreement" in frame.columns and (
        frame["positive_agreement"].isna().any() or (frame["positive_agreement"] == 0).all()
    ):
        denom = frame["supporting_violations"].replace(0, 1)
        frame["positive_agreement"] = frame["positive_matches"] / denom
    if "parity_ci_lower" in frame.columns and (
        frame["parity_ci_lower"].isna().any() or (frame["parity_ci_lower"] == 0).all()
    ):
        frame["parity_ci_lower"] = frame["confidence"]
    if "parity_ci_upper" in frame.columns and (
        frame["parity_ci_upper"].isna().any() or (frame["parity_ci_upper"] == 0).all()
    ):
        frame["parity_ci_upper"] = frame["confidence"]
    if "coverage_ci_lower" in frame.columns and (
        frame["coverage_ci_lower"].isna().any() or (frame["coverage_ci_lower"] == 0).all()
    ):
        frame["coverage_ci_lower"] = frame["positive_coverage"]
    if "coverage_ci_upper" in frame.columns and (
        frame["coverage_ci_upper"].isna().any() or (frame["coverage_ci_upper"] == 0).all()
    ):
        frame["coverage_ci_upper"] = frame["positive_coverage"]

    columns = [
        "rule_name",
        "severity",
        "products_tested",
        "perl_errors",
        "python_errors",
        "supporting_violations",
        "positive_matches",
        "positive_agreement",
        "positive_coverage",
        "parity_ci_lower",
        "evidence_ci_lower",
        "evidence_posterior_mean",
        "matches",
        "mismatches",
        "confidence",
        "llm_confidence",
        "overall_confidence",
        "status",
        "tag",
    ]
    return frame[columns]


def _inject_theme() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap');
        html, body, [class*="st-"], [class*="css"] {
            font-family: "Space Grotesk", sans-serif;
        }
        .block-container {
            max-width: 1200px;
            padding-top: 1.2rem;
            padding-bottom: 2rem;
        }
        [data-testid="stHorizontalBlock"] > [data-testid="column"] {
            padding-right: 0.45rem;
            padding-left: 0.45rem;
        }
        [data-testid="stMetric"] {
            background: linear-gradient(120deg, #f8fafc 0%, #ecfeff 100%);
            border: 1px solid #dbeafe;
            border-radius: 14px;
            padding: 0.6rem 0.8rem;
            min-height: 118px;
        }
        [data-testid="stMetricLabel"] {
            font-size: 0.9rem;
            color: #334155;
        }
        [data-testid="stMetricValue"] {
            color: #0f172a;
            font-size: 1.55rem;
            line-height: 1.2;
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
            font-size: 1.35rem;
            letter-spacing: -0.01em;
        }
        .hero p {
            margin: 0.25rem 0 0 0;
            color: #334155;
            font-size: 0.95rem;
        }
        .mono {
            font-family: "IBM Plex Mono", monospace;
        }
        .section-gap {
            margin-top: 0.35rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _build_confidence_chart(frame: pd.DataFrame) -> go.Figure:
    chart_df = frame.copy()
    chart_df["overall_pct"] = chart_df["overall_confidence"] * 100
    chart_df["parity_pct"] = chart_df["confidence"] * 100
    chart_df["llm_pct"] = chart_df["llm_confidence"] * 100
    chart_df = chart_df.sort_values("overall_pct", ascending=True)
    chart_df["rule_label"] = [
        f"{rule} ({overall:.1f}%)"
        for rule, overall in zip(chart_df["rule_name"], chart_df["overall_pct"])
    ]

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=chart_df["parity_pct"],
            y=chart_df["rule_label"],
            orientation="h",
            name="Parity confidence",
            marker_color="rgba(29, 78, 216, 0.28)",
            width=0.34,
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Parity: %{x:.2f}%<br>"
                "<extra></extra>"
            ),
        )
    )
    fig.add_trace(
        go.Bar(
            x=chart_df["overall_pct"],
            y=chart_df["rule_label"],
            orientation="h",
            name="Overall confidence",
            marker_color=[STATUS_COLORS.get(status, "#64748b") for status in chart_df["status"]],
            width=0.62,
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Overall: %{x:.2f}%<br>"
                "<extra></extra>"
            ),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=chart_df["llm_pct"],
            y=chart_df["rule_label"],
            mode="markers",
            name="LLM confidence",
            marker=dict(color="#f59e0b", size=10, symbol="diamond", line=dict(color="#7c2d12", width=0.6)),
            hovertemplate="<b>%{y}</b><br>LLM: %{x:.2f}%<extra></extra>",
        )
    )

    fig.update_layout(
        height=max(420, 56 * len(chart_df)),
        margin=dict(l=40, r=70, t=30, b=32),
        barmode="overlay",
        xaxis=dict(title="Confidence (%)", range=[0, 105], gridcolor="#e2e8f0", ticksuffix="%"),
        yaxis=dict(title=None),
        legend=dict(orientation="h", y=1.08, x=0),
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
    )
    return fig


def _build_status_chart(frame: pd.DataFrame) -> go.Figure:
    status_df = frame["status"].value_counts().rename_axis("status").reset_index(name="count")
    fig = px.pie(
        status_df,
        names="status",
        values="count",
        hole=0.62,
        color="status",
        color_discrete_map=STATUS_COLORS,
    )
    fig.update_layout(
        height=420,
        margin=dict(l=5, r=5, t=20, b=5),
        showlegend=True,
        legend=dict(orientation="h", y=-0.1, x=0),
    )
    fig.update_traces(textinfo="label+value")
    return fig


def _build_table(frame: pd.DataFrame) -> pd.DataFrame:
    table = frame.copy()
    table["confidence"] = (table["confidence"] * 100).round(2)
    table["parity_ci_lower"] = (table["parity_ci_lower"] * 100).round(2)
    table["llm_confidence"] = (table["llm_confidence"] * 100).round(2)
    table["overall_confidence"] = (table["overall_confidence"] * 100).round(2)
    table["positive_agreement"] = (table["positive_agreement"] * 100).round(2)
    table["positive_coverage"] = (table["positive_coverage"] * 100).round(2)
    table["evidence_ci_lower"] = (table["evidence_ci_lower"] * 100).round(2)
    table["evidence_posterior_mean"] = (table["evidence_posterior_mean"] * 100).round(2)
    table = table.sort_values(by=["status", "overall_confidence"], ascending=[True, False])
    return table


def main() -> None:
    st.set_page_config(page_title="OFF Migration Dashboard", layout="wide")
    _inject_theme()
    st.markdown(
        """
        <div class="hero">
            <h1>Open Food Facts Migration Dashboard</h1>
            <p>Rule-by-rule parity between legacy Perl checks and generated Python checks.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.subheader("Pipeline")
        size = st.slider("Dataset size", min_value=100, max_value=500, value=300, step=25)
        default_mode = "OFF JSONL" if DEFAULT_SOURCE_JSONL.exists() else "Synthetic"
        dataset_mode = st.radio(
            "Dataset mode",
            options=["OFF JSONL", "Synthetic"],
            index=0 if default_mode == "OFF JSONL" else 1,
            horizontal=False,
            help="Choose OFF JSONL to read from file, or Synthetic to generate demo data.",
        )
        use_off_source = dataset_mode == "OFF JSONL"
        seed = st.number_input(
            "Seed (synthetic mode)",
            min_value=1,
            max_value=99999,
            value=17,
            step=1,
            disabled=use_off_source,
            help="Used only when OFF source is disabled.",
        )
        if use_off_source:
            st.caption("Seed is ignored while using OFF JSONL source.")
        source_jsonl_text = st.text_input(
            "Source JSONL path",
            value=str(DEFAULT_SOURCE_JSONL),
            disabled=not use_off_source,
        )
        perl_rules_dir_text = st.text_input("Perl rules directory (optional)", value="")
        llm_provider = st.selectbox("LLM provider", options=["groq", "openrouter", "simulated"], index=0)
        llm_model = st.text_input("LLM model override", value="openai/gpt-oss-120b")
        if st.button("Run Pipeline"):
            with st.spinner("Running migration prototype..."):
                run_pipeline(
                    dataset_size=int(size),
                    seed=int(seed),
                    source_jsonl=Path(source_jsonl_text) if use_off_source else None,
                    use_default_off_source=use_off_source,
                    llm_provider=llm_provider,
                    llm_model=llm_model.strip() or None,
                    perl_rules_dir=Path(perl_rules_dir_text) if perl_rules_dir_text.strip() else None,
                )
            st.success("Pipeline completed.")

    payload = load_results()
    if not payload:
        st.info("No results yet. Click 'Run Pipeline' in the sidebar.")
        return

    summary = payload.get("migration_summary", {})
    rule_results = payload.get("rule_results", [])

    col1, col2, col3, col4 = st.columns(4, gap="large")
    col1.metric("Total rules", int(summary.get("total_rules", 0)))
    col2.metric("Passed rules", int(summary.get("passed_rules", 0)))
    col3.metric("Needs review", int(summary.get("rules_needing_review", 0)))
    col4.metric("Avg confidence", f"{float(summary.get('average_overall_confidence', 0)):.2%}")

    st.caption(f"Generated at: {payload.get('generated_at_utc', 'n/a')}")
    st.caption(f"Dataset size: {payload.get('dataset', {}).get('products_tested', 'n/a')}")
    st.caption(f"Dataset source: {payload.get('dataset', {}).get('source_jsonl', 'n/a')}")
    st.caption(f"Perl rules source: {payload.get('dataset', {}).get('perl_rules_source', 'n/a')}")

    frame = _rule_dataframe(rule_results)
    if frame.empty:
        st.warning("No rule-level results found.")
        return

    st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)
    chart_left, chart_right = st.columns([0.7, 0.3], gap="large")
    with chart_left:
        st.subheader("Confidence by Rule")
        st.caption("Thick bar = overall confidence, blue band = parity confidence, amber diamond = LLM confidence.")
        st.plotly_chart(_build_confidence_chart(frame), use_container_width=True)
    with chart_right:
        st.subheader("Rule Status")
        st.plotly_chart(_build_status_chart(frame), use_container_width=True)

    st.subheader("Rule Validation Table")
    table = _build_table(frame)
    show_advanced_table = st.checkbox("Show advanced metrics columns", value=False)
    if show_advanced_table:
        visible_columns = [
            "rule_name",
            "severity",
            "products_tested",
            "perl_errors",
            "python_errors",
            "supporting_violations",
            "positive_matches",
            "positive_agreement",
            "positive_coverage",
            "parity_ci_lower",
            "evidence_ci_lower",
            "evidence_posterior_mean",
            "confidence",
            "llm_confidence",
            "overall_confidence",
            "mismatches",
            "status",
            "tag",
        ]
    else:
        visible_columns = [
            "rule_name",
            "severity",
            "products_tested",
            "perl_errors",
            "python_errors",
            "mismatches",
            "parity_ci_lower",
            "llm_confidence",
            "overall_confidence",
            "status",
        ]

    st.dataframe(
        table[visible_columns],
        use_container_width=True,
        hide_index=True,
        column_config={
            "rule_name": st.column_config.TextColumn("Rule"),
            "severity": st.column_config.TextColumn("Severity"),
            "products_tested": st.column_config.NumberColumn("Products", format="%d"),
            "perl_errors": st.column_config.NumberColumn("Perl violations", format="%d"),
            "python_errors": st.column_config.NumberColumn("Python violations", format="%d"),
            "mismatches": st.column_config.NumberColumn("Mismatches", format="%d"),
            "supporting_violations": st.column_config.NumberColumn("Evidence count", format="%d"),
            "positive_matches": st.column_config.NumberColumn("Positive matches", format="%d"),
            "positive_agreement": st.column_config.NumberColumn("Positive agreement %", format="%.2f"),
            "positive_coverage": st.column_config.NumberColumn("Coverage %", format="%.2f"),
            "parity_ci_lower": st.column_config.NumberColumn("Parity CI low %", format="%.2f"),
            "evidence_ci_lower": st.column_config.NumberColumn("Evidence CI low %", format="%.2f"),
            "evidence_posterior_mean": st.column_config.NumberColumn("Evidence mean %", format="%.2f"),
            "confidence": st.column_config.NumberColumn("Parity %", format="%.2f"),
            "llm_confidence": st.column_config.NumberColumn("LLM %", format="%.2f"),
            "overall_confidence": st.column_config.NumberColumn("Overall %", format="%.2f"),
            "status": st.column_config.TextColumn("Status"),
            "tag": st.column_config.TextColumn("Tag"),
        },
    )

    st.subheader("Rule Detail")
    selected_rule_name = st.selectbox("Select rule", frame["rule_name"].tolist())
    selected = next(item for item in rule_results if item["rule_name"] == selected_rule_name)

    summary_cols = st.columns(5, gap="large")
    summary_cols[0].metric("Status", selected.get("status", "n/a"))
    summary_cols[1].metric("Parity %", f"{float(selected.get('confidence', 0)) * 100:.2f}")
    summary_cols[2].metric("Parity CI low %", f"{float(selected.get('parity_ci_lower', 0)) * 100:.2f}")
    summary_cols[3].metric("Overall %", f"{float(selected.get('overall_confidence', 0)) * 100:.2f}")
    summary_cols[4].metric("Mismatches", int(selected.get("mismatches", 0)))
    st.caption(f"Conversion provider: {selected.get('conversion_provider', 'n/a')}")
    if selected.get("overall_method"):
        st.caption(f"Scoring method: {selected.get('overall_method')}")
    st.caption(
        "Overall formula: LLM% x parity CI lower (95%) x evidence CI lower (95%, Beta posterior)."
    )

    tab_logic, tab_mismatch, tab_duckdb = st.tabs(["Logic", "Parity Mismatches", "DuckDB View"])

    with tab_logic:
        left, right = st.columns(2)
        with left:
            st.markdown("**Perl logic**")
            st.code(selected["perl_logic"], language="perl")
        with right:
            st.markdown("**Generated Python conversion**")
            st.code(selected["python_conversion"], language="python")

    with tab_mismatch:
        st.markdown("**Failed test cases (Perl vs Python mismatches)**")
        failed_cases = selected.get("failed_test_cases", [])
        if failed_cases:
            st.dataframe(pd.DataFrame(failed_cases), use_container_width=True, hide_index=True)
        else:
            st.success("No mismatches for this rule.")

    with tab_duckdb:
        st.markdown("**DuckDB query**")
        st.code(selected["duckdb_query"], language="sql")
        st.markdown(f"DuckDB violations: `{selected['duckdb_errors']}`")
        st.markdown("**Sample violating rows**")
        duckdb_examples = selected.get("duckdb_example_rows", [])
        if duckdb_examples:
            st.dataframe(pd.DataFrame(duckdb_examples), use_container_width=True, hide_index=True)
        else:
            st.info("No violating rows in DuckDB for this rule.")


if __name__ == "__main__":
    main()
