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
  python_checks/
    generated_checks.py
  validation/
    parity_validator.py
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

Optional: use file-based Perl snippets for extractor input:

```bash
python -m validation.parity_validator --size 300 --perl-rules-dir perl_checks/rules
```

3. Open dashboard:

```bash
streamlit run dashboard/app.py
```

The pipeline writes results to `results/migration_results.json`.

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
