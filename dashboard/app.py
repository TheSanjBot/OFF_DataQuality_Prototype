"""Streamlit dashboard for migration prototype outputs."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from validation.parity_validator import run_pipeline

RESULT_PATH = Path(__file__).resolve().parent.parent / "results" / "migration_results.json"
DEFAULT_SOURCE_JSONL = PROJECT_ROOT / "openfoodfacts-products.jsonl"


def load_results() -> dict:
    if not RESULT_PATH.exists():
        return {}
    with RESULT_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _rule_dataframe(rule_results: list[dict]) -> pd.DataFrame:
    if not rule_results:
        return pd.DataFrame()
    frame = pd.DataFrame(rule_results)
    columns = [
        "rule_name",
        "severity",
        "products_tested",
        "perl_errors",
        "python_errors",
        "matches",
        "mismatches",
        "confidence",
        "llm_confidence",
        "overall_confidence",
        "status",
        "tag",
    ]
    return frame[columns]


def main() -> None:
    st.set_page_config(page_title="OFF Migration Dashboard", layout="wide")
    st.title("Open Food Facts Quality Check Migration Dashboard")

    with st.sidebar:
        st.subheader("Pipeline")
        size = st.slider("Dataset size", min_value=100, max_value=500, value=300, step=25)
        seed = st.number_input("Seed", min_value=1, max_value=99999, value=17, step=1)
        use_off_source = st.checkbox("Use OFF JSONL source", value=DEFAULT_SOURCE_JSONL.exists())
        source_jsonl_text = st.text_input("Source JSONL path", value=str(DEFAULT_SOURCE_JSONL))
        perl_rules_dir_text = st.text_input("Perl rules directory (optional)", value="")
        llm_provider = st.selectbox("LLM provider", options=["groq", "openrouter", "simulated"], index=0)
        llm_model = st.text_input("LLM model override", value="openai/gpt-oss-120b")
        if st.button("Run Pipeline"):
            with st.spinner("Running migration prototype..."):
                run_pipeline(
                    dataset_size=int(size),
                    seed=int(seed),
                    source_jsonl=Path(source_jsonl_text) if use_off_source else None,
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

    col1, col2, col3, col4 = st.columns(4)
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

    st.subheader("Confidence by Rule")
    st.bar_chart(frame.set_index("rule_name")[["confidence", "llm_confidence", "overall_confidence"]])

    st.subheader("Rule Validation Table")
    st.dataframe(frame, use_container_width=True, hide_index=True)

    st.subheader("Rule Detail")
    selected_rule_name = st.selectbox("Select rule", frame["rule_name"].tolist())
    selected = next(item for item in rule_results if item["rule_name"] == selected_rule_name)

    left, right = st.columns(2)
    with left:
        st.markdown("**Perl logic**")
        st.code(selected["perl_logic"], language="perl")
    with right:
        st.markdown("**Generated Python conversion**")
        st.code(selected["python_conversion"], language="python")
        st.caption(f"Conversion provider: {selected.get('conversion_provider', 'n/a')}")

    st.markdown("**DuckDB Query**")
    st.code(selected["duckdb_query"], language="sql")
    st.markdown(f"DuckDB violations: `{selected['duckdb_errors']}`")

    st.markdown("**Failed test cases (Perl vs Python mismatches)**")
    failed_cases = selected.get("failed_test_cases", [])
    if failed_cases:
        st.dataframe(pd.DataFrame(failed_cases), use_container_width=True, hide_index=True)
    else:
        st.success("No mismatches for this rule.")

    st.markdown("**DuckDB sample violations**")
    duckdb_examples = selected.get("duckdb_example_rows", [])
    if duckdb_examples:
        st.dataframe(pd.DataFrame(duckdb_examples), use_container_width=True, hide_index=True)
    else:
        st.info("No violating rows in DuckDB for this rule.")


if __name__ == "__main__":
    main()
