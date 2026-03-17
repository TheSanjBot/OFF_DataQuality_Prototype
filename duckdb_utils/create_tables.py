"""DuckDB helpers for the migration prototype."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import duckdb

DB_PATH = Path(__file__).resolve().parent.parent / "off_quality.db"
TABLE_NAME = "nutrition_table"

SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    product_id TEXT,
    energy_kj DOUBLE,
    energy_kj_computed DOUBLE,
    energy_kcal DOUBLE,
    fat DOUBLE,
    saturated_fat DOUBLE,
    carbohydrates DOUBLE,
    sugars DOUBLE,
    starch DOUBLE,
    lc TEXT,
    lang TEXT,
    language_code TEXT
)
"""


def connect(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db_path.as_posix())


def recreate_nutrition_table(db_path: Path = DB_PATH) -> None:
    """Drop and recreate the main table for deterministic runs."""
    with connect(db_path) as con:
        con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        con.execute(SCHEMA_SQL)


def load_jsonl_to_duckdb(jsonl_path: Path, db_path: Path = DB_PATH) -> None:
    """Load JSONL records into DuckDB."""
    with connect(db_path) as con:
        con.execute(
            f"""
            INSERT INTO {TABLE_NAME}
            SELECT
                product_id,
                energy_kj,
                energy_kj_computed,
                energy_kcal,
                fat,
                saturated_fat,
                carbohydrates,
                sugars,
                starch,
                lc,
                lang,
                language_code
            FROM read_json_auto(?)
            """,
            [jsonl_path.as_posix()],
        )


def fetch_products(db_path: Path = DB_PATH) -> List[Dict[str, object]]:
    """Read all products from DuckDB as dictionaries."""
    with connect(db_path) as con:
        rows = con.execute(f"SELECT * FROM {TABLE_NAME}").fetchdf()
    return rows.to_dict("records")


def count_rows(db_path: Path = DB_PATH) -> int:
    with connect(db_path) as con:
        value = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()
    return int(value[0] if value else 0)


def count_violations(condition_sql: str, db_path: Path = DB_PATH) -> int:
    """Count records matching a rule condition."""
    query = f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE {condition_sql}"
    with connect(db_path) as con:
        value = con.execute(query).fetchone()
    return int(value[0] if value else 0)


def sample_violations(condition_sql: str, limit: int = 5, db_path: Path = DB_PATH) -> List[Dict[str, object]]:
    """Return sample violating records for dashboard drilldown."""
    query = f"""
        SELECT *
        FROM {TABLE_NAME}
        WHERE {condition_sql}
        ORDER BY product_id
        LIMIT {int(limit)}
    """
    with connect(db_path) as con:
        rows = con.execute(query).fetchdf()
    return rows.to_dict("records")
