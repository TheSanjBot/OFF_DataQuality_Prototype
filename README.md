# OFF Quality Migration Prototype

This project is a production-style prototype for migrating legacy Open Food
Facts data quality checks from Perl into modern execution targets and then
operating a human-in-the-loop correction workflow on top of the validation
results.

It currently supports:

- Perl-rule extraction into structured rule metadata
- multi-engine execution across Python, dbt, and Soda
- parity validation and conservative confidence scoring
- migration governance (`accepted`, `candidate`, `review`)
- correction issue export with ranked fix suggestions
- batch correction with audit logs and corrected dataset output
- revalidation after corrections
- lightweight feedback learning from reviewer decisions
- Streamlit dashboard workflows plus Google Sheets-compatible CSV round-trip

## Prototype Pipeline

Dataset (JSONL)
-> DuckDB table
-> Simulated legacy Perl checks
-> Perl logic extractor
-> Rule-pack profile layer (global / canada / hybrid)
-> LLM-style Python conversion
-> Semantic guardrail checks on generated Python
-> Generated Python checks
   OR Declarative checks via dbt-core / soda-core
-> Back-to-back validator
-> Confidence scoring
-> Migration governance
-> Issue packaging for correction review
-> Ranked fix suggestions
-> Human review (dashboard or Google Sheets / CSV)
-> Batch correction apply
-> Audit log
-> Revalidation
-> Feedback learning
-> Streamlit dashboard

## Project Structure

```text
off_quality_migration_prototype/
  data/
    load_dataset.py
    sample_products.jsonl
  duckdb_utils/
    create_tables.py
  perl_checks/
    legacy_checks.py
  rulepacks/
    registry.py
  extractor/
    perl_logic_extractor.py
  migration/
    llm_converter.py
  declarative/
    check_runners.py
  python_checks/
    generated_checks.py
  correction/
    issue_builder.py
    fix_generator.py
    fix_ranker.py
    correction_export.py
    correction_import.py
    correction_applier.py
    revalidation.py
    review_status.py
    schemas.py
  audit/
    audit_logger.py
  learning/
    feedback_store.py
    ranking_updater.py
  orchestrator/
    correction_runner.py
  validation/
    parity_validator.py
    engine_comparison.py
  dashboard/
    app.py
  results/
    engine_comparison.json
    review_sheet.csv
    reviewed_sheet.csv
    corrected_dataset.jsonl
    correction_patches.jsonl
    audit_log.jsonl
    revalidation_summary.json
  requirements.txt
  README.md
```

Note: the database helper package is named `duckdb_utils` (instead of
`duckdb/`) to avoid conflicting with the `duckdb` Python package import.

## Implemented Example Checks

- `energy_kcal > energy_kj`
- `energy_kj < (3.7 * energy_kcal - 2)` (intricate)
- `energy_kj > (4.7 * energy_kcal + 2)` (intricate)
- `energy_kj > 3911`
- `energy_kj_computed < (0.7 * energy_kj - 5)` (intricate)
- `energy_kj_computed > (1.3 * energy_kj + 5)` (intricate)
- `saturated_fat > (fat + 0.001)`
- `(sugars + starch) > (carbohydrates + 0.001)` (medium)
- `fat > 105`
- `saturated_fat > 105`
- `carbohydrates > 105`
- `sugars > 105`
- missing main language code (`lc is null/empty`) [Canada profile]
- missing main language value (`lang is null/empty`) [Canada profile]
- `allergen_evidence_present > 0 && ingredients_text_present == 0` [Canada profile]
- `contains_statement_present > 0 && allergen_evidence_present == 0` [Canada profile]
- `fop_threshold_exceeded > 0 && fop_symbol_present == 0 && fop_exempt_proxy == 0 && product_is_prepackaged_proxy > 0` [Canada profile]
- `fop_symbol_present > 0 && fop_threshold_exceeded == 0 && fop_exempt_proxy == 0 && product_is_prepackaged_proxy > 0` [Canada profile]
- `fop_symbol_present > 0 && fop_exempt_proxy > 0 && product_is_prepackaged_proxy > 0` [Canada profile]

Each check emits an OFF-style quality tag.

## Install Dependencies

```bash
pip install -r requirements.txt
```

Current external dependencies remain:

- `duckdb`
- `streamlit`
- `pandas`
- `openai`
- `pytest`
- `plotly`
- `dbt-duckdb`
- `soda-duckdb`

## Compare Python vs dbt vs Soda (Recommended)

Run a single experiment that executes all three engines on the same dataset
and writes a unified report:

```bash
python -m validation.engine_comparison --size 300 --mode off --llm-provider groq
```

Synthetic mode:

```bash
python -m validation.engine_comparison --size 300 --mode synthetic --seed 17 --llm-provider simulated
```

Soda Cloud mode:

```bash
python -m validation.engine_comparison --size 300 --mode off --llm-provider groq --soda-mode cloud
```

Run a specific rule-pack profile:

```bash
python -m validation.engine_comparison --size 300 --mode off --llm-provider groq --profile hybrid
python -m validation.engine_comparison --size 300 --mode off --llm-provider groq --profile global
python -m validation.engine_comparison --size 300 --mode off --llm-provider groq --profile canada
```

Require real LLM usage for the Python engine:

```bash
python -m validation.engine_comparison --size 300 --mode off --llm-provider groq --require-real-llm
```

Output:

- `results/engine_comparison.json`

The report includes:

- per-engine summary (pass count, average overall confidence, average effective confidence, fallback rules)
- per-complexity summary (simple/medium/intricate win distribution)
- rule-by-rule metrics across all engines
- best engine recommendation per rule
- migration governance state per rule
- jurisdiction and legal-traceability metadata when available

Best-engine ranking uses:

- status priority (`MATCH` over `REVIEW`)
- fewer mismatches
- higher `effective_confidence`

Where:

```text
effective_confidence = overall_confidence * provider_factor
```

`provider_factor` penalizes fallback providers so non-LLM/non-native fallback paths
do not unfairly win by tiny score differences.

## Optional Parity Validator Runs

The lower-level parity pipeline is still available when you want to run a
single execution engine directly:

```bash
python -m validation.parity_validator --size 300 --seed 17
python -m validation.parity_validator --size 300 --execution-engine dbt
python -m validation.parity_validator --size 300 --execution-engine soda
python -m validation.parity_validator --size 300 --execution-engine soda --soda-mode cloud
```

Optional: use file-based Perl snippets for extractor input:

```bash
python -m validation.parity_validator --size 300 --perl-rules-dir perl_checks/rules
```

## Correction Workflow

The correction subsystem starts **after** issues have already been detected by
the validation/migration pipeline.

### Workflow phases

1. **Issue packaging and export**
   - flagged violations are converted into spreadsheet-friendly issue rows
   - each row gets 2-3 ranked suggested fixes
   - output files:
     - `results/issues.json`
     - `results/review_sheet.csv`
     - `results/review_instructions.md`
     - `results/review_manifest.json`

2. **Human review**
   - reviewers can work in the Streamlit dashboard or by editing the CSV in
     Google Sheets / Excel
   - for each row, the reviewer can:
     - choose a suggested fix via `selected_fix_id`
     - enter a custom `manual_edit`
     - set `user_action` to `approve` or `reject`
     - add `review_notes`, `reviewer_name`, and `reviewed_at_utc`

3. **Batch apply**
   - only approved rows are applied
   - the original OFF JSONL is left unchanged
   - approved fixes are applied to a corrected output copy
   - output files:
     - `results/corrected_dataset.jsonl`
     - `results/correction_patches.jsonl`
     - `results/audit_log.jsonl`
     - `results/apply_summary.json`

4. **Revalidation**
   - reruns validation on the corrected dataset
   - measures before/after improvement
   - output files:
     - `results/revalidated_engine_comparison.json`
     - `results/revalidation_summary.json`

5. **Feedback learning**
   - approved and rejected fix strategies are stored
   - future fix ranking is adjusted using reviewer feedback
   - output files:
     - `results/feedback_store.json`
     - `results/ranking_weights.json`

### Issue workflow state

The review sheet uses an explicit workflow state column instead of relying on
`user_action` alone:

- `pending_review`
- `approved`
- `rejected`
- `applied`
- `revalidated_resolved`
- `revalidated_remaining`

This makes it possible to distinguish:

- rows ready to apply
- rows already applied
- rows that were rechecked and resolved
- rows that were rechecked and still remain problematic

## Run Validation -> Correction End-to-End

### 1. Run the cross-engine comparison

Synthetic/local example:

```bash
python -m validation.engine_comparison --size 100 --mode synthetic --llm-provider simulated --profile global --soda-mode local --results-path results/engine_comparison.json
```

Real OFF + Groq + Soda Cloud example:

```bash
python -m validation.engine_comparison --size 300 --mode off --llm-provider groq --profile hybrid --soda-mode cloud --require-real-llm --results-path results/engine_comparison.json
```

### 2. Export the correction review bundle

```bash
python -m orchestrator.correction_runner --mode export --comparison-path results/engine_comparison.json --issues-path results/issues.json --review-sheet-path results/review_sheet.csv --source-snapshot-path results/review_source_dataset.jsonl --feedback-store-path results/feedback_store.json --ranking-weights-path results/ranking_weights.json --review-instructions-path results/review_instructions.md --review-manifest-path results/review_manifest.json --max-issues-per-rule 10
```

### 3. Validate the reviewed sheet

If using dashboard review, the dashboard writes to `results/reviewed_sheet.csv`.
If using external review, download `review_sheet.csv`, edit it, and save the
reviewed copy back as `results/reviewed_sheet.csv`.

Then validate:

```bash
python -m orchestrator.correction_runner --mode validate-review --review-sheet-path results/reviewed_sheet.csv --issues-path results/issues.json --review-validation-summary-path results/review_validation_summary.json
```

### 4. Apply approved fixes

```bash
python -m orchestrator.correction_runner --mode apply --comparison-path results/engine_comparison.json --issues-path results/issues.json --review-sheet-path results/reviewed_sheet.csv --source-snapshot-path results/review_source_dataset.jsonl --corrected-dataset-path results/corrected_dataset.jsonl --patches-path results/correction_patches.jsonl --audit-log-path results/audit_log.jsonl --feedback-store-path results/feedback_store.json
```

### 5. Revalidate corrected data

```bash
python -m orchestrator.correction_runner --mode revalidate --comparison-path results/engine_comparison.json --review-sheet-path results/reviewed_sheet.csv --corrected-dataset-path results/corrected_dataset.jsonl --patches-path results/correction_patches.jsonl --audit-log-path results/audit_log.jsonl --revalidated-comparison-path results/revalidated_engine_comparison.json --revalidation-summary-path results/revalidation_summary.json --revalidation-db-path results/revalidation_off_quality.db
```

## Google Sheets-Compatible Review Loop

The current external review workflow is CSV-based and Google Sheets-compatible:

1. export `results/review_sheet.csv`
2. upload/import it into Google Sheets
3. edit only the reviewer columns:
   - `selected_fix_id`
   - `manual_edit`
   - `user_action`
   - `review_notes`
   - `reviewer_name`
   - `reviewed_at_utc`
4. download the reviewed sheet back as CSV
5. save it to `results/reviewed_sheet.csv`
6. run `validate-review`
7. run `apply`
8. run `revalidate`

Important:

- keep `issue_id`, `product_id`, and `selected_fix_id` as **plain text** in
  Google Sheets to avoid leading-zero conversion
- do not rename or delete columns
- do not edit structural columns like `issue_id`, `rule_name`, or the
  `suggested_fix_*` text columns

## Dashboard Correction Workflow

The Streamlit dashboard includes a full correction section with:

- review table and issue-by-issue editor
- suggested-fix dropdown
- manual edit field
- approve/reject action
- patch preview before apply
- apply summary and audit log view
- revalidation summary
- feedback-learning tables
- artifact downloads for both:
  - original review template
  - reviewed sheet

Run:

```bash
streamlit run dashboard/app.py
```

Then open:

- `Correction Workflow -> Review`
- `Correction Workflow -> Apply`
- `Correction Workflow -> Revalidate`
- `Correction Workflow -> Learning`

## Profile Layer (Global / Canada / Hybrid)

The prototype supports one core engine with profile-driven rule packs:

- `global`: OFF-derived generic rules
- `canada`: Canada-focused rules with citation metadata
- `hybrid`: union of both packs (default)

Canada rules currently included in profile mode:

- `main_language_code_missing`
- `main_language_missing`
- `ca_allergen_evidence_missing_ingredients_text`
- `ca_contains_statement_without_allergen_evidence`
- `ca_fop_required_but_symbol_missing`
- `ca_fop_symbol_present_but_not_required`
- `ca_fop_symbol_present_on_exempt_product`

Canada profile metadata fields emitted per rule:

- `jurisdiction`
- `regulatory_type`
- `legal_citation`
- `source_url`
- `effective_date`
- `review_status`
- `reviewer`
- `required_fields`
- `exemption_logic`

Important note:

- Current Canada rules are **phase-1 regulatory proxies** using fields available in this
  prototype dataset. They provide migration/validation architecture for CA logic with legal
  traceability metadata, but are not a full legal compliance engine.

## Mixed-Complexity Benchmark

The benchmark includes:

- simple rules (basic comparisons/thresholds/missing fields)
- medium rules (compound `AND` thresholds)
- intricate rules (scaled field comparisons)

This enables a realistic hybrid recommendation:

- declarative engines (`dbt`/`soda`) for declarative-friendly constraints
- procedural Python migration for intricate/procedural checks

## Using Real OFF JSONL + Groq

If `openfoodfacts-products.jsonl` exists in the project root, the pipeline
uses it by default and streams only the first `--size` products after field
extraction.

Set your Groq API key as an environment variable:

```powershell
$env:GROQ_API_KEY="YOUR_KEY_HERE"
```

Then run:

```bash
python -m validation.parity_validator --size 300 --llm-provider groq --llm-model openai/gpt-oss-120b
```

Run with a selected profile:

```bash
python -m validation.parity_validator --size 300 --llm-provider groq --profile hybrid
python -m validation.parity_validator --size 300 --llm-provider groq --profile canada
```

If API access is unavailable, conversion automatically falls back to the
deterministic converter so the pipeline still completes.

Recommended default model:

- `openai/gpt-oss-120b`

## LLM-Only Mode (No simulated fallback)

If you want the pipeline to fail instead of using deterministic fallback:

```powershell
$env:LLM_STRICT="1"
```

## Semantic Guardrails

Generated LLM code is validated in two stages before being accepted:

- runtime contract checks (callability, no crashes on probe inputs, return type)
- deterministic semantic checks for each rule type (comparison, threshold, missing field)

If semantic checks fail, the conversion is rejected and falls back to deterministic
templates unless `LLM_STRICT=1`.

## Declarative Pilot (dbt-core / soda-core)

This prototype supports an execution-engine switch:

- `python`: LLM-assisted Perl -> Python migration target
- `dbt`: declarative dbt-core target (DuckDB-backed)
- `soda`: declarative SodaCL target (DuckDB-backed)

All three are compared against the same simulated Perl baseline for parity.

Generated declarative artifacts are written under:

- `results/declarative_runtime/dbt/`
- `results/declarative_runtime/soda/`

## Confidence Scoring (Statistical)

Rule-level overall confidence uses a conservative statistical formula:

```text
overall_confidence = llm_confidence * parity_ci_lower_95 * evidence_ci_lower_95
```

Where:

- `parity_ci_lower_95` is the 95% Wilson lower bound on parity agreement
- `evidence_ci_lower_95` is the 95% lower credible bound from a Beta posterior
  on positive-case agreement (`Beta(positive_matches + 1, supporting_violations - positive_matches + 1)`).

This avoids optimistic confidence inflation when positive evidence is sparse.

## Automated Tests

Run:

```bash
pytest -q
```

Test coverage included:

- extractor tests (`tests/test_extractor.py`)
- deterministic converter tests (`tests/test_deterministic_converter.py`)
- parity pipeline smoke test (`tests/test_parity_smoke.py`)
- correction workflow export/import tests (`tests/test_correction_phase1.py`, `tests/test_correction_phase2.py`)
- correction revalidation tests (`tests/test_correction_phase3.py`)
- feedback learning tests (`tests/test_correction_phase4.py`)
