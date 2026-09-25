"""Contract tests for curated joins, query recipes, and SQL generation."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from forest_explorer.catalog.query_planner import (
    as_list,
    build_copy_sql,
    build_duckdb_sql,
    join_map,
    product_columns,
    product_map,
    validate_plan,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SNAPSHOT_DIR = REPO_ROOT / "forest_explorer" / "catalog" / "snapshot"
JOIN_SOURCE = REPO_ROOT / "forest_explorer" / "registry" / "joins.yaml"
PRESET_SOURCE = REPO_ROOT / "forest_explorer" / "registry" / "query_presets.yaml"

VALID_RELATIONSHIPS = {"one_to_one", "many_to_one", "one_to_many"}
VALID_JOIN_STATUS = {"certified", "constrained", "review_required"}


@pytest.fixture(scope="module")
def catalog() -> dict:
    return json.loads((SNAPSHOT_DIR / "catalog.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def joins() -> dict:
    return yaml.safe_load(JOIN_SOURCE.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def presets() -> dict:
    return yaml.safe_load(PRESET_SOURCE.read_text(encoding="utf-8"))


def test_join_ids_are_unique(joins):
    ids = [join["id"] for join in joins["joins"]]
    assert len(ids) == len(set(ids))


def test_join_references_and_keys_resolve(catalog, joins):
    products = product_map(catalog)
    for join in joins["joins"]:
        assert join["left_product_id"] in products
        assert join["right_product_id"] in products
        assert join["relationship"] in VALID_RELATIONSHIPS
        assert join["status"] in VALID_JOIN_STATUS
        assert join.get("warning")
        left_on, right_on = as_list(join["left_on"]), as_list(join["right_on"])
        assert left_on and len(left_on) == len(right_on)
        assert set(left_on) <= set(product_columns(products[join["left_product_id"]]))
        assert set(right_on) <= set(product_columns(products[join["right_product_id"]]))


def test_preset_ids_are_unique(presets):
    ids = [preset["id"] for preset in presets["presets"]]
    assert len(ids) == len(set(ids))


def test_every_preset_is_a_valid_safe_plan(catalog, joins, presets):
    for preset in presets["presets"]:
        chosen, _ = validate_plan(
            catalog,
            joins,
            preset["anchor_product_id"],
            as_list(preset.get("join_ids")),
            preset.get("columns", {}),
        )
        assert sum(join["relationship"] == "one_to_many" for join in chosen) <= 1
        assert preset.get("description")
        assert preset.get("request_tags")
        assert "cautions" in preset
        assert "missing_capabilities" in preset


def test_committed_navigation_snapshots_match_sources(joins, presets):
    committed_joins = json.loads((SNAPSHOT_DIR / "joins.json").read_text(encoding="utf-8"))
    committed_presets = json.loads((SNAPSHOT_DIR / "query_presets.json").read_text(encoding="utf-8"))
    assert committed_joins["join_registry_version"] == joins["join_registry_version"]
    assert [item["id"] for item in committed_joins["joins"]] == [item["id"] for item in joins["joins"]]
    assert committed_presets["preset_registry_version"] == presets["preset_registry_version"]
    assert [item["id"] for item in committed_presets["presets"]] == [item["id"] for item in presets["presets"]]


def test_all_presets_generate_deterministic_sql(catalog, joins, presets):
    for preset in presets["presets"]:
        sql = build_duckdb_sql(
            catalog,
            joins,
            preset["anchor_product_id"],
            as_list(preset.get("join_ids")),
            preset.get("columns", {}),
            limit=1000,
        )
        assert sql.startswith("SELECT\n")
        assert sql.endswith("LIMIT 1000;")
        assert "read_parquet(" in sql or "read_csv_auto(" in sql
        assert sql.count("LEFT JOIN") == len(as_list(preset.get("join_ids")))
        assert sql == build_duckdb_sql(
            catalog,
            joins,
            preset["anchor_product_id"],
            as_list(preset.get("join_ids")),
            preset.get("columns", {}),
            limit=1000,
        )


def test_sql_quotes_filters_and_export_paths(catalog, joins):
    sql = build_duckdb_sql(
        catalog,
        joins,
        "analysis_condition_visit_cwm",
        ["condition_cwm__disturbance_flags"],
        {
            "analysis_condition_visit_cwm": ["PLT_CN", "state", "layer"],
            "fia_condition_disturbance_flags": ["ELEV"],
        },
        filters=[
            {"column": "state", "operator": "IN", "value": ["CA", "O'R"]},
            {"column": "INVYR", "operator": ">=", "value": 2010},
        ],
    )
    assert "base.\"state\" IN ('CA', 'O''R')" in sql
    assert "base.\"INVYR\" >= 2010" in sql
    assert 'AS "fia_condition_disturbance_flags__ELEV"' in sql
    exported = build_copy_sql(sql, "scratch_output/boss's table.parquet")
    assert "boss''s table.parquet" in exported
    assert exported.startswith("COPY (\nSELECT")


def test_multiple_expanding_joins_are_rejected(catalog, joins):
    with pytest.raises(ValueError, match="at most one row-expanding"):
        build_duckdb_sql(
            catalog,
            joins,
            "plot_condition_metadata",
            ["condition_metadata__understory_structure", "condition_metadata__damage_agents"],
            {"plot_condition_metadata": ["PLT_CN"]},
        )
