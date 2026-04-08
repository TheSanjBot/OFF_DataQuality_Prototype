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
    "energy_kj_computed",
    "energy_kcal",
    "fat",
    "saturated_fat",
    "carbohydrates",
    "sugars",
    "starch",
    "sodium",
    "ingredients_text",
    "ingredients_text_present",
    "contains_statement_present",
    "allergen_evidence_present",
    "fop_threshold_exceeded",
    "fop_symbol_present",
    "fop_exempt_proxy",
    "product_is_prepackaged_proxy",
]

# Additional fields used for OFF language-related checks.
OPTIONAL_FIELDS = ["lc", "lang", "language_code"]


def _looks_like_normalized_product(record: Mapping[str, object]) -> bool:
    """Return True when a JSONL row already matches the prototype schema."""
    return "product_id" in record and any(field in record for field in CORE_FIELDS[1:] + OPTIONAL_FIELDS)


def _normalize_existing_product_record(record: Mapping[str, object]) -> Dict[str, object]:
    """Project an existing normalized record onto the expected prototype fields."""
    normalized: Dict[str, object] = {"product_id": str(record.get("product_id", ""))}
    for field in CORE_FIELDS[1:] + OPTIONAL_FIELDS:
        normalized[field] = record.get(field)
    return normalized


def _to_int_flag(value: bool) -> int:
    return 1 if value else 0


def _compute_fop_threshold_exceeded(sugars: object, saturated_fat: object, sodium: object) -> int:
    """Proxy Front-of-Pack trigger for prototype experiments.

    This is intentionally a simplified threshold model to exercise migration
    architecture and should not be interpreted as full legal implementation.
    """
    sugars_val = _to_float(sugars) or 0.0
    sat_fat_val = _to_float(saturated_fat) or 0.0
    sodium_val = _to_float(sodium) or 0.0
    return _to_int_flag((sugars_val >= 15.0) or (sat_fat_val >= 6.0) or (sodium_val >= 0.6))


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


def _apply_deterministic_synthetic_scenarios(index: int, product: Dict[str, object], rng: random.Random) -> None:
    """Inject deterministic rule-violation scenarios for synthetic datasets.

    This keeps synthetic runs visually informative in the dashboard by ensuring
    each rule receives recurring, known-positive examples.
    """
    bucket = index % 21

    if bucket == 1:
        # energy_kcal > energy_kj
        energy_kj = float(product["energy_kj"])
        product["energy_kcal"] = round(energy_kj + rng.uniform(1.0, 50.0), 1)
    elif bucket == 2:
        # energy_kj < (3.7 * energy_kcal - 2)
        energy_kcal = round(rng.uniform(80.0, 320.0), 1)
        product["energy_kcal"] = energy_kcal
        product["energy_kj"] = round((3.7 * energy_kcal) - rng.uniform(3.0, 30.0), 1)
    elif bucket == 3:
        # energy_kj > (4.7 * energy_kcal + 2)
        energy_kcal = round(rng.uniform(80.0, 320.0), 1)
        product["energy_kcal"] = energy_kcal
        product["energy_kj"] = round((4.7 * energy_kcal) + rng.uniform(3.0, 30.0), 1)
    elif bucket == 4:
        # energy_kj > 3911
        product["energy_kj"] = round(rng.uniform(3912.0, 5200.0), 1)
    elif bucket == 5:
        # saturated_fat > (fat + 0.001)
        fat = round(rng.uniform(10.0, 80.0), 3)
        product["fat"] = fat
        product["saturated_fat"] = round(fat + rng.uniform(0.01, 9.0), 3)
    elif bucket == 6:
        # sugars + starch > carbohydrates + 0.001
        carbs = round(rng.uniform(10.0, 90.0), 3)
        sugars = round(rng.uniform(3.0, 60.0), 3)
        starch = round(max((carbs - sugars) + rng.uniform(0.01, 6.0), 0.0), 3)
        product["carbohydrates"] = carbs
        product["sugars"] = sugars
        product["starch"] = starch
    elif bucket == 7:
        # fat > 105
        product["fat"] = round(rng.uniform(106.0, 135.0), 1)
    elif bucket == 8:
        # saturated_fat > 105
        product["saturated_fat"] = round(rng.uniform(106.0, 135.0), 1)
    elif bucket == 9:
        # carbohydrates > 105
        product["carbohydrates"] = round(rng.uniform(106.0, 140.0), 1)
    elif bucket == 10:
        # sugars > 105
        product["sugars"] = round(rng.uniform(106.0, 140.0), 1)
    elif bucket == 11:
        # missing lc
        product["lc"] = ""
        product["language_code"] = ""
    elif bucket == 12:
        # missing lang
        product["lang"] = ""
        if product.get("lc"):
            product["language_code"] = str(product["lc"])
        else:
            product["language_code"] = ""
    elif bucket == 13:
        # energy_kj_computed < (0.7 * energy_kj - 5)
        energy_kj = float(product["energy_kj"])
        product["energy_kj_computed"] = round((0.7 * energy_kj) - rng.uniform(6.0, 25.0), 1)
    elif bucket == 14:
        # energy_kj_computed > (1.3 * energy_kj + 5)
        energy_kj = float(product["energy_kj"])
        product["energy_kj_computed"] = round((1.3 * energy_kj) + rng.uniform(6.0, 25.0), 1)
    elif bucket == 15:
        # Allergen evidence present but ingredients text missing.
        product["allergen_evidence_present"] = 1
        product["contains_statement_present"] = 1
        product["ingredients_text"] = ""
        product["ingredients_text_present"] = 0
    elif bucket == 16:
        # Contains statement present without allergen evidence.
        product["contains_statement_present"] = 1
        product["allergen_evidence_present"] = 0
        product["ingredients_text"] = "Contains: milk, soy."
        product["ingredients_text_present"] = 1
    elif bucket == 17:
        # FOP required but symbol missing.
        product["fop_threshold_exceeded"] = 1
        product["fop_symbol_present"] = 0
        product["fop_exempt_proxy"] = 0
        product["product_is_prepackaged_proxy"] = 1
    elif bucket == 18:
        # FOP symbol present but threshold not exceeded (and not exempt).
        product["fop_threshold_exceeded"] = 0
        product["fop_symbol_present"] = 1
        product["fop_exempt_proxy"] = 0
        product["product_is_prepackaged_proxy"] = 1
    elif bucket == 19:
        # FOP symbol present on exempt product (proxy inconsistency).
        product["fop_threshold_exceeded"] = 1
        product["fop_symbol_present"] = 1
        product["fop_exempt_proxy"] = 1
        product["product_is_prepackaged_proxy"] = 1
    elif bucket == 20:
        # Not prepackaged proxy case (used to suppress FOP obligations).
        product["product_is_prepackaged_proxy"] = 0
        product["fop_symbol_present"] = 0


def generate_product(index: int, rng: random.Random) -> Dict[str, object]:
    """Generate a single product with occasional quality rule violations."""
    energy_kj = rng.randint(50, 4800)
    energy_kcal = int(round(energy_kj / 4.184))

    fat = round(rng.uniform(0.0, 100.0), 1)
    saturated_fat = round(rng.uniform(0.0, fat), 1)
    carbohydrates = round(rng.uniform(0.0, 100.0), 1)
    sugars = round(rng.uniform(0.0, carbohydrates), 1)
    starch = round(rng.uniform(0.0, max(carbohydrates - sugars, 0.0)), 1)
    sodium = round(rng.uniform(0.0, 1.5), 3)

    if _maybe(0.10, rng):
        energy_kcal = energy_kj + rng.randint(1, 100)
    if _maybe(0.07, rng):
        energy_kj = round((3.7 * energy_kcal) - rng.uniform(3.0, 30.0), 1)
    if _maybe(0.07, rng):
        energy_kj = round((4.7 * energy_kcal) + rng.uniform(3.0, 30.0), 1)
    if _maybe(0.08, rng):
        saturated_fat = round(fat + rng.uniform(0.1, 20.0), 1)
    if _maybe(0.07, rng):
        starch = round(max((carbohydrates - sugars) + rng.uniform(0.01, 6.0), 0.0), 1)

    energy_kj_computed = round(float(energy_kj) * rng.uniform(0.92, 1.08), 1)
    if _maybe(0.06, rng):
        energy_kj_computed = round((0.65 * float(energy_kj)) - rng.uniform(1.0, 8.0), 1)
    if _maybe(0.06, rng):
        energy_kj_computed = round((1.35 * float(energy_kj)) + rng.uniform(1.0, 8.0), 1)

    product: Dict[str, object] = {
        "product_id": f"{index:013d}",
        "energy_kj": energy_kj,
        "energy_kj_computed": energy_kj_computed,
        "energy_kcal": energy_kcal,
        "fat": fat,
        "saturated_fat": saturated_fat,
        "carbohydrates": carbohydrates,
        "sugars": sugars,
        "starch": starch,
        "sodium": sodium,
    }

    for nutrient in ("fat", "saturated_fat", "carbohydrates", "sugars"):
        if _maybe(0.08, rng):
            product[nutrient] = round(rng.uniform(106.0, 140.0), 1)

    language_code = rng.choices(
        ["en", "fr", "es", "de", "it", "", None],
        weights=[0.55, 0.1, 0.08, 0.06, 0.06, 0.08, 0.07],
        k=1,
    )[0]
    lang_value = rng.choices(
        ["en", "fr", "es", "de", "it", "xx", "", None],
        weights=[0.5, 0.1, 0.08, 0.06, 0.06, 0.03, 0.09, 0.08],
        k=1,
    )[0]
    product["lc"] = language_code
    product["lang"] = lang_value
    product["language_code"] = language_code or lang_value
    ingredients_text = rng.choices(
        [
            "Sugar, milk powder, cocoa butter.",
            "Water, apple juice concentrate.",
            "Ingredients: wheat flour, salt, yeast.",
            "",
            None,
        ],
        weights=[0.30, 0.22, 0.22, 0.16, 0.10],
        k=1,
    )[0]
    product["ingredients_text"] = ingredients_text if ingredients_text is not None else ""
    product["ingredients_text_present"] = _to_int_flag(str(product["ingredients_text"]).strip() != "")

    # Prototype proxies for Canadian allergen/FOP checks.
    contains_statement_present = _maybe(0.22, rng)
    allergen_evidence_present = contains_statement_present or _maybe(0.15, rng)
    fop_threshold_exceeded = _compute_fop_threshold_exceeded(product.get("sugars"), product.get("saturated_fat"), product.get("sodium"))
    fop_exempt_proxy = _to_int_flag(_maybe(0.10, rng))
    product_is_prepackaged_proxy = _to_int_flag(not _maybe(0.05, rng))
    fop_symbol_present = _to_int_flag(
        (fop_threshold_exceeded == 1 and _maybe(0.78, rng))
        or (fop_threshold_exceeded == 0 and _maybe(0.10, rng))
    )

    product["contains_statement_present"] = _to_int_flag(contains_statement_present)
    product["allergen_evidence_present"] = _to_int_flag(allergen_evidence_present)
    product["fop_threshold_exceeded"] = int(fop_threshold_exceeded)
    product["fop_symbol_present"] = int(fop_symbol_present)
    product["fop_exempt_proxy"] = int(fop_exempt_proxy)
    product["product_is_prepackaged_proxy"] = int(product_is_prepackaged_proxy)
    _apply_deterministic_synthetic_scenarios(index=index, product=product, rng=rng)
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
    energy_kj_computed = _first_number(
        nutriments.get("energy-kj_value_computed"),
        nutriments.get("energy-kj_value-computed"),
        nutriments.get("energy-kj_computed"),
    )
    fat = _first_number(nutriments.get("fat_100g"), nutriments.get("fat"))
    saturated_fat = _first_number(nutriments.get("saturated-fat_100g"), nutriments.get("saturated-fat"))
    carbohydrates = _first_number(nutriments.get("carbohydrates_100g"), nutriments.get("carbohydrates"))
    sugars = _first_number(nutriments.get("sugars_100g"), nutriments.get("sugars"))
    starch = _first_number(nutriments.get("starch_100g"), nutriments.get("starch"))
    sodium = _first_number(nutriments.get("sodium_100g"), nutriments.get("sodium"))
    if sodium is None:
        salt_value = _first_number(nutriments.get("salt_100g"), nutriments.get("salt"))
        if salt_value is not None:
            sodium = round(float(salt_value) * 0.393, 4)
    lc = record.get("lc")
    lang = record.get("lang")
    language_code = lc or lang
    ingredients_text = str(record.get("ingredients_text") or "").strip()

    allergens_tags = record.get("allergens_tags")
    allergens_list = allergens_tags if isinstance(allergens_tags, list) else []
    contains_statement_present = bool(record.get("allergens")) or bool(record.get("traces")) or bool(allergens_list)
    allergen_evidence_present = contains_statement_present or bool(record.get("allergens_from_ingredients"))

    labels_tags = record.get("labels_tags")
    labels_list = labels_tags if isinstance(labels_tags, list) else []
    labels_text = " ".join(str(item).lower() for item in labels_list)
    fop_symbol_present = (
        ("high-in-sugars" in labels_text)
        or ("high-in-sodium" in labels_text)
        or ("high-in-saturated-fat" in labels_text)
    )

    categories_tags = record.get("categories_tags")
    categories_list = categories_tags if isinstance(categories_tags, list) else []
    categories_text = " ".join(str(item).lower() for item in categories_list)
    fop_exempt_proxy = ("en:waters" in categories_text) or ("en:unflavoured-waters" in categories_text)
    product_is_prepackaged_proxy = True
    fop_threshold_exceeded = _compute_fop_threshold_exceeded(sugars, saturated_fat, sodium)

    return {
        "product_id": product_id,
        "energy_kj": energy_kj,
        "energy_kj_computed": energy_kj_computed,
        "energy_kcal": energy_kcal,
        "fat": fat,
        "saturated_fat": saturated_fat,
        "carbohydrates": carbohydrates,
        "sugars": sugars,
        "starch": starch,
        "sodium": sodium,
        "ingredients_text": ingredients_text,
        "ingredients_text_present": _to_int_flag(ingredients_text != ""),
        "contains_statement_present": _to_int_flag(contains_statement_present),
        "allergen_evidence_present": _to_int_flag(allergen_evidence_present),
        "fop_threshold_exceeded": int(fop_threshold_exceeded),
        "fop_symbol_present": _to_int_flag(fop_symbol_present),
        "fop_exempt_proxy": _to_int_flag(fop_exempt_proxy),
        "product_is_prepackaged_proxy": _to_int_flag(product_is_prepackaged_proxy),
        "lc": lc,
        "lang": lang,
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
            if _looks_like_normalized_product(record):
                extracted = _normalize_existing_product_record(record)
            else:
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
    """Build dataset records and load them into DuckDB.

    Notes:
    - ``seed`` is used only for synthetic generation.
    - When ``source_jsonl`` is provided, records are streamed from that file and
      ``seed`` has no effect.
    """
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
    parser.add_argument(
        "--seed",
        type=int,
        default=17,
        help="Random seed for synthetic data reproducibility (ignored when --source-jsonl is set).",
    )
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
