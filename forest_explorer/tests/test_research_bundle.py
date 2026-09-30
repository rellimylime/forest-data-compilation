"""Contract tests for the research bundle definition.

These need no data, so they run in a code-only checkout:

    python -m pytest forest_explorer/tests/test_research_bundle.py
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
BUNDLE = REPO_ROOT / "forest_explorer" / "registry" / "research_bundle.yaml"
SNAPSHOT = REPO_ROOT / "forest_explorer" / "catalog" / "snapshot" / "catalog.json"
BUNDLE_FORMATS = {"parquet_file", "parquet_dataset", "csv", "csv_glob"}


@pytest.fixture(scope="module")
def bundle() -> dict:
    return yaml.safe_load(BUNDLE.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def products() -> dict:
    catalog = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
    return {product["id"]: product for product in catalog["products"]}


def table_ids(bundle: dict) -> list[str]:
    return [table["product_id"] for table in bundle["tables"]]


def test_tables_are_unique_known_and_readable(bundle, products):
    ids = table_ids(bundle)
    assert len(ids) == len(set(ids))
    for product_id in ids:
        assert product_id in products, product_id
        assert products[product_id]["format"] in BUNDLE_FORMATS, product_id


def test_every_question_uses_bundle_tables(bundle):
    ids = set(table_ids(bundle))
    question_ids = [question["id"] for question in bundle["questions"]]
    assert len(question_ids) == len(set(question_ids))
    for question in bundle["questions"]:
        assert question["request"] and question["how"]
        assert question["tables"] and set(question["tables"]) <= ids, question["id"]


def test_views_read_only_bundle_tables(bundle):
    ids = set(table_ids(bundle))
    names = [view["name"] for view in bundle["views"]]
    assert len(names) == len(set(names))
    assert not set(names) & ids
    for view in bundle["views"]:
        referenced = set(re.findall(r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*)", view["sql"], re.I))
        assert referenced and referenced <= ids, (view["name"], referenced - ids)
