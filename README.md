# OFF Quality Migration Prototype

This project is a proof-of-concept for migrating legacy Open Food Facts
data quality checks from Perl to Python with automated parity validation.

## Prototype Pipeline

Dataset (JSONL)
-> DuckDB table
-> Simulated legacy Perl checks
-> Perl logic extractor
-> LLM-style Python conversion
-> Semantic guardrail checks on generated Python
-> Generated Python checks
   OR Declarative checks via dbt-core / soda-core
-> Back-to-back validator
-> Confidence scoring
-> Streamlit dashboard

## Project Structure

```
off_quality_migration_prototype/
  data/
    load_dataset.py
    sample_products.jsonl
  duckdb_utils/
    create_tables.py
  perl_checks/
    legacy_checks.py
  extractor/
    perl_logic_extractor.py
  migration/
    llm_converter.py
  declarative/
    check_runners.py
  python_checks/
    generated_checks.py
  validation/
    parity_validator.py
    engine_comparison.py
  dashboard/
    app.py
  results/
    migration_results.json
  requirements.txt
  README.md
```

Note: the database helper package is named `duckdb_utils` (instead of
`duckdb/`) to avoid conflicting with the `duckdb` Python package import.

## Implemented Example Checks

- `energy_kcal > energy_kj`
- `sugars > carbohydrates`
- `saturated_fat > fat`
- `fat > 105`
- `saturated_fat > 105`
- `carbohydrates > 105`
- `sugars > 105`
- missing language code (`language_code is null/empty`)
- `saturated_fat > (fat * 0.8)` (intricate)
- `sugars > (carbohydrates * 0.95)` (intricate)
- `energy_kcal > 350 && fat < 5 && carbohydrates < 10` (medium)
- `energy_kcal > 420 && fat > 25 && sugars > 30` (medium)

Each check emits an OFF-style quality tag.

## Run End-to-End

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Run the parity pipeline:

```bash
python -m validation.parity_validator --size 300 --seed 17
```

`python` is the default execution engine (Perl baseline vs generated Python checks).

Optional: use file-based Perl snippets for extractor input:

```bash
python -m validation.parity_validator --size 300 --perl-rules-dir perl_checks/rules
```

Optional: run declarative parity target via `dbt` or `soda`:

```bash
python -m validation.parity_validator --size 300 --execution-engine dbt
python -m validation.parity_validator --size 300 --execution-engine soda
```

Notes for declarative engines:

- If `dbt` CLI is available, dbt tests are generated/executed against DuckDB.
- If `soda` CLI is available, SodaCL checks are generated/executed against DuckDB.
- If CLI is unavailable, the prototype still runs parity using SQL-equivalent declarative conditions and marks provider as `*_sql_fallback`.

3. Open dashboard:

```bash
streamlit run dashboard/app.py
```

The pipeline writes results to `results/migration_results.json`.
The comparison dashboard writes/reads `results/engine_comparison.json` and
shows Python/dbt/Soda side-by-side per rule (best migration highlighted).
In dashboard mode, Python engine is Groq-based and real LLM usage is enforced
for comparison runs.

## Compare Python vs dbt vs Soda (Recommended)

Run a single experiment that executes all three engines on the same dataset
and writes a unified report:

```bash
python -m validation.engine_comparison --size 300 --mode off --llm-provider groq
```

Require real LLM usage for Python engine (fail if simulated fallback is used):

```bash
python -m validation.engine_comparison --size 300 --mode off --llm-provider groq --require-real-llm
```

Synthetic mode:

```bash
python -m validation.engine_comparison --size 300 --mode synthetic --seed 17 --llm-provider simulated
```

Output:

- `results/engine_comparison.json`

The report includes:

- per-engine summary (pass count, average overall confidence, average effective confidence, fallback rules)
- per-complexity summary (simple/medium/intricate win distribution)
- rule-by-rule metrics across all engines
- best engine recommendation per rule

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

## Mixed-Complexity Benchmark (What It Adds)

The benchmark now includes:

- simple rules (basic comparisons/thresholds/missing fields)
- medium rules (compound `AND` thresholds)
- intricate rules (scaled field comparisons)

This enables a realistic hybrid recommendation:

- declarative engines (`dbt`/`soda`) for declarative-friendly constraints
- procedural Python migration for intricate/procedural checks

## Using Real OFF JSONL + Groq (Default)

If `openfoodfacts-products.jsonl` exists in the project root, the pipeline
uses it by default and streams only the first `--size` products after field
extraction.

Set your Groq API key as an environment variable (do not hardcode):

```powershell
$env:GROQ_API_KEY="YOUR_KEY_HERE"
```

Then run:

```bash
python -m validation.parity_validator --size 300 --llm-provider groq --llm-model openai/gpt-oss-120b
```

If API access is unavailable, conversion automatically falls back to the
deterministic converter so the pipeline still completes.

Recommended default model:

- `openai/gpt-oss-120b`

## LLM-Only Mode (No simulated_fallback)

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

This prototype now supports an execution-engine switch:

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

- `parity_ci_lower_95` is the 95% Wilson lower bound on parity agreement.
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
