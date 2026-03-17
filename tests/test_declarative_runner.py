from pathlib import Path

from data.load_dataset import create_and_load_dataset
from declarative import check_runners
from extractor.perl_logic_extractor import extract_rules
from perl_checks.legacy_checks import LEGACY_RULES, get_perl_rule_snippets


def _run_engine(engine: str, tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(check_runners.shutil, "which", lambda _: None)
    db_path = tmp_path / f"{engine}_checks.db"
    products = create_and_load_dataset(size=120, seed=17, db_path=db_path, source_jsonl=None)
    rules = extract_rules(get_perl_rule_snippets(LEGACY_RULES))

    result = check_runners.run_declarative_checks(
        rules=rules,
        products=products,
        db_path=db_path,
        engine=engine,
    )

    assert set(result["per_rule"].keys()) == {rule["rule_name"] for rule in rules}
    assert set(result["conversion_metadata"].keys()) == {rule["rule_name"] for rule in rules}
    first_rule = rules[0]["rule_name"]
    provider = result["conversion_metadata"][first_rule]["provider"]
    assert provider.endswith("_sql_fallback")


def test_declarative_dbt_runner_smoke(tmp_path, monkeypatch) -> None:
    _run_engine("dbt", tmp_path=tmp_path, monkeypatch=monkeypatch)


def test_declarative_soda_runner_smoke(tmp_path, monkeypatch) -> None:
    _run_engine("soda", tmp_path=tmp_path, monkeypatch=monkeypatch)

