"""Dataset utilities for the migration prototype.

This module creates a reduced Open Food Facts-like JSONL dataset and loads
it into DuckDB for downstream parity validation.
"""
from __future__ import annotations

import argparse
import json
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "off_quality.db"
SAMPLE_FILE = Path(__file__).resolve().parent / "sample_products.jsonl"
DEFAULT_OFF_JSONL = PROJECT_ROOT / "openfoodfacts-products.jsonl"

CORE_FIELDS = [
    "product_id",
    "energy_kj",
    "energy_kcal",
    "fat",
    "saturated_fat",
    "carbohydrates",
    "sugars",
]

# Additional field used for the simplified "missing language code" check.
OPTIONAL_FIELDS = ["language_code"]


@dataclass(frozen=True)
class DatasetConfig:
    """Configuration for synthetic dataset generation."""

    size: int = 300
    seed: int = 17
    output_path: Path = SAMPLE_FILE


def _maybe(probability: float, rng: random.Random) -> bool:
    return rng.random() < probability


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _first_number(*values: object) -> float | None:
    for value in values:
        parsed = _to_float(value)
        if parsed is not None:
            return parsed
    return None


def generate_product(index: int, rng: random.Random) -> Dict[str, object]:
    """Generate a single product with occasional quality rule violations."""
    energy_kj = rng.randint(50, 4800)
    energy_kcal = int(round(energy_kj / 4.184))

    fat = round(rng.uniform(0.0, 100.0), 1)
    saturated_fat = round(rng.uniform(0.0, fat), 1)
    carbohydrates = round(rng.uniform(0.0, 100.0), 1)
    sugars = round(rng.uniform(0.0, carbohydrates), 1)

    if _maybe(0.10, rng):
        energy_kcal = energy_kj + rng.randint(1, 100)
    if _maybe(0.09, rng):
        sugars = round(carbohydrates + rng.uniform(0.1, 25.0), 1)
    if _maybe(0.08, rng):
        saturated_fat = round(fat + rng.uniform(0.1, 20.0), 1)

    product: Dict[str, object] = {
        "product_id": f"{index:013d}",
        "energy_kj": energy_kj,
        "energy_kcal": energy_kcal,
        "fat": fat,
        "saturated_fat": saturated_fat,
        "carbohydrates": carbohydrates,
        "sugars": sugars,
    }

    for nutrient in ("fat", "saturated_fat", "carbohydrates", "sugars"):
        if _maybe(0.08, rng):
            product[nutrient] = round(rng.uniform(106.0, 140.0), 1)

    language_code = rng.choices(
        ["en", "fr", "es", "de", "it", "", None],
        weights=[0.55, 0.1, 0.08, 0.06, 0.06, 0.08, 0.07],
        k=1,
    )[0]
    product["language_code"] = language_code
    return product


def generate_products(config: DatasetConfig) -> List[Dict[str, object]]:
    """Generate ``config.size`` synthetic products."""
    rng = random.Random(config.seed)
    return [generate_product(i, rng) for i in range(1, config.size + 1)]


def extract_product_from_off_record(record: Mapping[str, object]) -> Dict[str, object] | None:
    """Extract prototype fields from one Open Food Facts product object."""
    product_id = str(record.get("code") or record.get("_id") or record.get("id") or "").strip()
    if not product_id:
        return None

    nutriments = record.get("nutriments")
    if not isinstance(nutriments, Mapping):
        nutriments = {}

    energy_kj = _first_number(
        nutriments.get("energy-kj_100g"),
        nutriments.get("energy-kj"),
        nutriments.get("energy_100g"),
        nutriments.get("energy"),
    )
    energy_kcal = _first_number(
        nutriments.get("energy-kcal_100g"),
        nutriments.get("energy-kcal"),
        nutriments.get("energy-kcal_value"),
        nutriments.get("energy-kcal_value_computed"),
    )
    fat = _first_number(nutriments.get("fat_100g"), nutriments.get("fat"))
    saturated_fat = _first_number(nutriments.get("saturated-fat_100g"), nutriments.get("saturated-fat"))
    carbohydrates = _first_number(nutriments.get("carbohydrates_100g"), nutriments.get("carbohydrates"))
    sugars = _first_number(nutriments.get("sugars_100g"), nutriments.get("sugars"))
    language_code = record.get("lc") or record.get("lang")

    return {
        "product_id": product_id,
        "energy_kj": energy_kj,
        "energy_kcal": energy_kcal,
        "fat": fat,
        "saturated_fat": saturated_fat,
        "carbohydrates": carbohydrates,
        "sugars": sugars,
        "language_code": language_code,
    }


def extract_products_from_off_jsonl(source_path: Path, max_products: int = 300) -> List[Dict[str, object]]:
    """Stream OFF JSONL and extract up to ``max_products`` normalized records."""
    products: List[Dict[str, object]] = []
    with source_path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            if len(products) >= max_products:
                break
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(record, Mapping):
                continue
            extracted = extract_product_from_off_record(record)
            if extracted is not None:
                products.append(extracted)

    if not products:
        raise ValueError(f"No usable products extracted from {source_path}")
    return products


def write_products_jsonl(products: Iterable[Dict[str, object]], output_path: Path) -> None:
    """Persist product records to JSONL."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for record in products:
            handle.write(json.dumps(record) + "\n")


def read_products_jsonl(path: Path = SAMPLE_FILE) -> List[Dict[str, object]]:
    """Read products from JSONL into a list."""
    records: List[Dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                records.append(json.loads(line))
    return records


def create_and_load_dataset(
    size: int = 300,
    seed: int = 17,
    output_path: Path = SAMPLE_FILE,
    db_path: Path = DB_PATH,
    source_jsonl: Path | None = None,
) -> List[Dict[str, object]]:
    """Generate a synthetic dataset and load it into DuckDB."""
    if source_jsonl is not None:
        products = extract_products_from_off_jsonl(Path(source_jsonl), max_products=size)
    else:
        config = DatasetConfig(size=size, seed=seed, output_path=output_path)
        products = generate_products(config)
    write_products_jsonl(products, output_path)

    # Local import avoids module cycles between data and duckdb layers.
    from duckdb_utils.create_tables import load_jsonl_to_duckdb, recreate_nutrition_table

    recreate_nutrition_table(db_path=db_path)
    load_jsonl_to_duckdb(jsonl_path=output_path, db_path=db_path)
    return products


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate and load prototype dataset.")
    parser.add_argument("--size", type=int, default=300, help="Number of products (100-500 recommended).")
    parser.add_argument("--seed", type=int, default=17, help="Random seed for reproducibility.")
    parser.add_argument(
        "--source-jsonl",
        type=Path,
        default=None,
        help="Path to OFF JSONL source file (if omitted, synthetic data is generated).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    products = create_and_load_dataset(size=args.size, seed=args.seed, source_jsonl=args.source_jsonl)
    print(f"Generated {len(products)} records at {SAMPLE_FILE}")
    if args.source_jsonl:
        print(f"Source dataset: {Path(args.source_jsonl).resolve()}")
    else:
        print("Source dataset: synthetic generator")
    print(f"Loaded dataset into {DB_PATH}")


if __name__ == "__main__":
    main()
