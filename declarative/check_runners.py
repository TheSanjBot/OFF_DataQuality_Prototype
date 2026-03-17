"""Declarative check runners for dbt-core and soda-core pilots.

These runners are intentionally lightweight:
- They always compute violating product IDs from DuckDB SQL conditions
  (deterministic parity baseline).
- They optionally execute dbt/soda commands to prove declarative integration.
"""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Dict, List, Mapping, Sequence

import duckdb

from duckdb_utils.create_tables import TABLE_NAME

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RUNTIME_DIR = PROJECT_ROOT / "results" / "declarative_runtime"


def _sql_for_rule(rule: Mapping[str, object]) -> str:
    condition = str(rule["duckdb_condition"])
    return f"SELECT product_id FROM {TABLE_NAME} WHERE {condition}"


def _query_rule_product_ids(rule: Mapping[str, object], db_path: Path) -> List[str]:
    query = _sql_for_rule(rule)
    with duckdb.connect(db_path.as_posix()) as con:
        rows = con.execute(query).fetchall()
    return sorted(str(row[0]) for row in rows)


def _build_per_product_tags(
    rules: Sequence[Mapping[str, object]],
    per_rule_ids: Dict[str, List[str]],
    products: Sequence[Mapping[str, object]],
) -> Dict[str, List[str]]:
    per_product_tags: Dict[str, List[str]] = {str(product.get("product_id")): [] for product in products}
    for rule in rules:
        rule_name = str(rule["rule_name"])
        tag = str(rule["tag"])
        for product_id in per_rule_ids[rule_name]:
            if product_id in per_product_tags:
                per_product_tags[product_id].append(tag)
    return per_product_tags


def _run_command(command: List[str], cwd: Path, env: Mapping[str, str] | None = None) -> Dict[str, object]:
    try:
        full_env = os.environ.copy()
        if env:
            full_env.update(env)
        completed = subprocess.run(
            command,
            cwd=cwd,
            env=full_env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
            timeout=180,
        )
        stdout = completed.stdout or ""
        stderr = completed.stderr or ""
        return {
            "command": " ".join(command),
            "executed": True,
            "success": completed.returncode == 0,
            "return_code": completed.returncode,
            "stdout_tail": stdout[-1200:],
            "stderr_tail": stderr[-1200:],
        }
    except FileNotFoundError:
        return {
            "command": " ".join(command),
            "executed": False,
            "success": False,
            "return_code": None,
            "stdout_tail": "",
            "stderr_tail": f"Command not found: {command[0]}",
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "command": " ".join(command),
            "executed": True,
            "success": False,
            "return_code": None,
            "stdout_tail": (exc.stdout or "")[-1200:] if exc.stdout else "",
            "stderr_tail": (exc.stderr or "")[-1200:] if exc.stderr else "Command timed out.",
        }


def _prepare_dbt_project(rules: Sequence[Mapping[str, object]], db_path: Path, root_dir: Path) -> Dict[str, object]:
    project_dir = root_dir / "dbt_project"
    tests_dir = project_dir / "tests"
    models_dir = project_dir / "models"
    macros_dir = project_dir / "macros"
    runtime_db_path = root_dir / "dbt_runtime.db"
    tests_dir.mkdir(parents=True, exist_ok=True)
    models_dir.mkdir(parents=True, exist_ok=True)
    macros_dir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(db_path, runtime_db_path)

    (project_dir / "dbt_project.yml").write_text(
        "\n".join(
            [
                "name: off_quality_declarative",
                "version: '1.0'",
                "config-version: 2",
                "profile: off_quality_duckdb",
                "model-paths: ['models']",
                "test-paths: ['tests']",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    (project_dir / "profiles.yml").write_text(
        "\n".join(
            [
                "off_quality_duckdb:",
                "  target: dev",
                "  outputs:",
                "    dev:",
                "      type: duckdb",
                f"      path: '{runtime_db_path.as_posix()}'",
                "      schema: main",
                "      threads: 1",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    (models_dir / "sources.yml").write_text(
        "\n".join(
            [
                "version: 2",
                "sources:",
                "  - name: off_source",
                "    schema: main",
                "    tables:",
                "      - name: nutrition_table",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    test_sql_by_rule: Dict[str, str] = {}
    for rule in rules:
        rule_name = str(rule["rule_name"])
        condition = str(rule["duckdb_condition"])
        sql = (
            "SELECT product_id\n"
            "FROM {{ source('off_source', 'nutrition_table') }}\n"
            f"WHERE {condition}"
        )
        test_sql_by_rule[rule_name] = sql
        (tests_dir / f"{rule_name}.sql").write_text(sql + "\n", encoding="utf-8")

    # dbt test command uses multiprocessing in this environment and fails with WinError 5.
    # Use run-operation to execute each rule query in real dbt runtime instead.
    (macros_dir / "count_violations.sql").write_text(
        "\n".join(
            [
                "{% macro count_violations(condition_sql) %}",
                "  {% set q %}",
                "    select count(*) as violation_count",
                "    from {{ source('off_source', 'nutrition_table') }}",
                "    where {{ condition_sql }}",
                "  {% endset %}",
                "  {% set t = run_query(q) %}",
                "  {% if execute %}",
                "    {% set c = t.columns[0].values()[0] %}",
                "    {% do log('VIOLATION_COUNT=' ~ c, info=True) %}",
                "  {% endif %}",
                "{% endmacro %}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    dbt_cmd = shutil.which("dbt")
    if not dbt_cmd:
        return {
            "run": {
                "command": "dbt test --project-dir ... --profiles-dir ...",
                "executed": False,
                "success": False,
                "return_code": None,
                "stdout_tail": "",
                "stderr_tail": "dbt CLI not found. Install dbt-duckdb.",
            },
            "test_sql_by_rule": test_sql_by_rule,
        }

    debug_info = _run_command(
        [dbt_cmd, "debug", "--project-dir", str(project_dir), "--profiles-dir", str(project_dir)],
        cwd=project_dir,
    )
    per_rule_runs: Dict[str, Dict[str, object]] = {}
    for rule in rules:
        rule_name = str(rule["rule_name"])
        condition = str(rule["duckdb_condition"])
        args_yaml = f"{{condition_sql: {condition!r}}}"
        per_rule_runs[rule_name] = _run_command(
            [
                dbt_cmd,
                "run-operation",
                "count_violations",
                "--project-dir",
                str(project_dir),
                "--profiles-dir",
                str(project_dir),
                "--args",
                args_yaml,
            ],
            cwd=project_dir,
        )

    all_executed = bool(debug_info.get("executed")) and all(bool(run.get("executed")) for run in per_rule_runs.values())
    all_success_codes = (
        debug_info.get("return_code") == 0 and all(run.get("return_code") == 0 for run in per_rule_runs.values())
    )
    failing_rules = [rule_name for rule_name, run in per_rule_runs.items() if run.get("return_code") != 0]
    summary_stdout = "\n".join(
        f"{rule_name}: rc={run.get('return_code')}, success={run.get('success')}"
        for rule_name, run in per_rule_runs.items()
    )[-1200:]
    summary_stderr = "\n".join(
        f"{rule_name}: {str(run.get('stderr_tail', '')).strip()}"
        for rule_name, run in per_rule_runs.items()
        if str(run.get("stderr_tail", "")).strip()
    )[-1200:]

    run_info = {
        "command": (
            f"dbt debug + dbt run-operation count_violations "
            f"(per-rule x{len(per_rule_runs)}) --project-dir {project_dir} --profiles-dir {project_dir}"
        ),
        "executed": all_executed,
        "success": all_success_codes,
        "return_code": 0 if all_success_codes else 2,
        "stdout_tail": summary_stdout,
        "stderr_tail": summary_stderr,
        "real_execution": all_success_codes,
        "failed_rules": failing_rules,
    }
    return {"run": run_info, "test_sql_by_rule": test_sql_by_rule}


def _prepare_soda_contract(rules: Sequence[Mapping[str, object]], db_path: Path, root_dir: Path) -> Dict[str, object]:
    soda_dir = root_dir / "soda"
    soda_dir.mkdir(parents=True, exist_ok=True)
    data_source_name = "off_quality"
    runtime_db_path = root_dir / "soda_runtime.db"
    config_path = soda_dir / "data_source.yml"
    contracts_dir = soda_dir / "contracts"
    contracts_dir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(db_path, runtime_db_path)

    config_path.write_text(
        "\n".join(
            [
                "type: duckdb",
                f"name: {data_source_name}",
                "connection:",
                f"  database: {runtime_db_path.as_posix()}",
                "  schema: main",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    with duckdb.connect(db_path.as_posix()) as con:
        columns = [str(row[1]) for row in con.execute(f"PRAGMA table_info('{TABLE_NAME}')").fetchall()]

    check_yaml_by_rule: Dict[str, str] = {}
    soda_cmd = shutil.which("soda")
    if not soda_cmd:
        return {
            "run": {
                "command": f"soda contract verify -c <contract.yml> -ds {config_path}",
                "executed": False,
                "success": False,
                "return_code": None,
                "stdout_tail": "",
                "stderr_tail": "soda CLI not found. Install soda-duckdb.",
            },
            "check_yaml_by_rule": check_yaml_by_rule,
        }

    soda_env = {
        # Avoid blocked telemetry calls and Windows cp1252 console issues with emoji output.
        "OTEL_SDK_DISABLED": "true",
        "PYTHONUTF8": "1",
        "PYTHONIOENCODING": "utf-8",
    }
    per_rule_runs: Dict[str, Dict[str, object]] = {}
    for rule in rules:
        rule_name = str(rule["rule_name"])
        condition = str(rule["duckdb_condition"])
        check_block = (
            f"  - failed_rows:\n"
            f"      name: {rule_name}\n"
            f"      expression: {condition}\n"
        )
        check_yaml_by_rule[rule_name] = check_block.rstrip("\n")

        contract_lines: List[str] = [
            f"dataset: {data_source_name}/main/{TABLE_NAME}",
            "columns:",
        ]
        for column in columns:
            contract_lines.append(f"  - name: {column}")
        contract_lines.extend(["checks:", check_block.rstrip("\n")])

        contract_path = contracts_dir / f"{rule_name}.yml"
        contract_path.write_text("\n".join(contract_lines) + "\n", encoding="utf-8")

        per_rule_runs[rule_name] = _run_command(
            [soda_cmd, "contract", "verify", "-c", str(contract_path), "-ds", str(config_path)],
            cwd=soda_dir,
            env=soda_env,
        )

    all_executed = all(bool(run.get("executed")) for run in per_rule_runs.values())
    all_success_codes = all(run.get("return_code") in {0, 1} for run in per_rule_runs.values())
    all_real = all(_soda_run_is_real(run) for run in per_rule_runs.values())
    failing_rules = [rule_name for rule_name, run in per_rule_runs.items() if not _soda_run_is_real(run)]

    summary_stdout = "\n".join(
        f"{rule_name}: rc={run.get('return_code')}, success={run.get('success')}"
        for rule_name, run in per_rule_runs.items()
    )[-1200:]
    summary_stderr = "\n".join(
        f"{rule_name}: {str(run.get('stderr_tail', '')).strip()}"
        for rule_name, run in per_rule_runs.items()
        if str(run.get("stderr_tail", "")).strip()
    )[-1200:]

    run_info = {
        "command": f"soda contract verify (per-rule x{len(per_rule_runs)}) -ds {config_path}",
        "executed": all_executed,
        "success": all_success_codes,
        "return_code": 0 if all_success_codes else 3,
        "stdout_tail": summary_stdout,
        "stderr_tail": summary_stderr,
        "real_execution": all_real,
        "failed_rules": failing_rules,
    }
    return {"run": run_info, "check_yaml_by_rule": check_yaml_by_rule}


def _dbt_run_is_real(run_info: Mapping[str, object]) -> bool:
    if "real_execution" in run_info:
        return bool(run_info.get("real_execution"))
    if not bool(run_info.get("executed")):
        return False
    return run_info.get("return_code") in {0, 1}


def _soda_run_is_real(run_info: Mapping[str, object]) -> bool:
    if not bool(run_info.get("executed")):
        return False
    output = f"{run_info.get('stdout_tail', '')}\n{run_info.get('stderr_tail', '')}".lower()
    if "soda v3 commands are not supported" in output:
        return False
    if "contract results for" in output:
        return True
    return bool(run_info.get("success"))


def run_declarative_checks(
    rules: Sequence[Mapping[str, object]],
    products: Sequence[Mapping[str, object]],
    db_path: Path,
    engine: str,
) -> Dict[str, object]:
    """Run declarative checks and return parity-compatible output payload."""
    if engine not in {"dbt", "soda"}:
        raise ValueError(f"Unsupported declarative engine: {engine}")

    runtime_root = RUNTIME_DIR / engine
    runtime_root.mkdir(parents=True, exist_ok=True)

    per_rule_ids: Dict[str, List[str]] = {}
    for rule in rules:
        per_rule_ids[str(rule["rule_name"])] = _query_rule_product_ids(rule, db_path=db_path)

    per_product_tags = _build_per_product_tags(rules=rules, per_rule_ids=per_rule_ids, products=products)

    if engine == "dbt":
        artifact = _prepare_dbt_project(rules=rules, db_path=db_path, root_dir=runtime_root)
        run_info = artifact["run"]
        snippets = artifact["test_sql_by_rule"]
        is_real = _dbt_run_is_real(run_info)
        provider = "dbt_core" if is_real else "dbt_core_sql_fallback"
        confidence = 0.99 if is_real else 0.92
        note_prefix = (
            "dbt tests executed through dbt-core."
            if is_real
            else "dbt execution unavailable/failed; used SQL-equivalent declarative parity."
        )
    else:
        artifact = _prepare_soda_contract(rules=rules, db_path=db_path, root_dir=runtime_root)
        run_info = artifact["run"]
        snippets = artifact["check_yaml_by_rule"]
        is_real = bool(run_info.get("real_execution")) or _soda_run_is_real(run_info)
        provider = "soda_core" if is_real else "soda_core_sql_fallback"
        confidence = 0.99 if is_real else 0.92
        note_prefix = (
            "Soda contract verification executed through soda-core."
            if is_real
            else "Soda execution unavailable/failed; used SQL-equivalent declarative parity."
        )

    conversion_metadata: Dict[str, Dict[str, object]] = {}
    for rule in rules:
        rule_name = str(rule["rule_name"])
        conversion_metadata[rule_name] = {
            "function_name": f"{engine}_{rule_name}",
            "python_code": snippets.get(rule_name, _sql_for_rule(rule)),
            "llm_confidence": confidence,
            "conversion_notes": (
                f"{note_prefix} "
                f"Command: {run_info['command']} | "
                f"Success: {run_info['success']} | "
                f"Return code: {run_info['return_code']}"
            ),
            "provider": provider,
        }

    return {
        "per_product": per_product_tags,
        "per_rule": per_rule_ids,
        "conversion_metadata": conversion_metadata,
        "engine_run": run_info,
    }
