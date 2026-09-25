"""Pure helpers for the snapshot-backed dataset query planner.

The dashboard imports this module, but it has no Streamlit dependency. It never
opens repository data: it validates a plan against committed metadata and emits
DuckDB SQL that a researcher may run deliberately from the repository root.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


SUPPORTED_FORMATS = {"parquet_file", "parquet_dataset", "csv", "csv_glob"}
RELATIONSHIPS = {"one_to_one", "many_to_one", "one_to_many"}
FILTER_OPERATORS = {"=", ">=", "<=", "IN"}


def as_list(value: Any) -> list:
    """Normalize JSON/YAML scalar-or-list fields."""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def product_map(catalog: Mapping[str, Any]) -> dict[str, dict]:
    return {product["id"]: product for product in catalog.get("products", [])}


def join_map(join_registry: Mapping[str, Any]) -> dict[str, dict]:
    return {join["id"]: join for join in join_registry.get("joins", [])}


def product_columns(product: Mapping[str, Any]) -> list[str]:
    """Return observed columns, tolerating both list and named-map JSON shapes."""
    observed = product.get("observed") or {}
    raw = observed.get("columns", []) if isinstance(observed, Mapping) else []
    if isinstance(raw, Mapping):
        names = list(raw)
    else:
        names = []
        for column in raw:
            if isinstance(column, (list, tuple)) and column:
                names.append(str(column[0]))
            elif isinstance(column, str):
                names.append(column)
    declared = as_list(product.get("keys")) + as_list(product.get("facets"))
    return list(dict.fromkeys(names + [str(value) for value in declared]))


def joins_for_anchor(join_registry: Mapping[str, Any], anchor_id: str) -> list[dict]:
    return [
        join for join in join_registry.get("joins", [])
        if join.get("left_product_id") == anchor_id
    ]


def quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def quote_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def source_sql(product: Mapping[str, Any]) -> str:
    """Build a DuckDB scan expression for one catalog product."""
    path = str(product["path"])
    escaped = path.replace("'", "''")
    fmt = product["format"]
    if fmt == "parquet_file":
        return f"read_parquet('{escaped}')"
    if fmt == "parquet_dataset":
        return f"read_parquet('{escaped}/**/*.parquet', union_by_name = true)"
    if fmt == "csv":
        return f"read_csv_auto('{escaped}', header = true)"
    if fmt == "csv_glob":
        suffix = "" if any(token in path for token in "*?[") else "/**/*.csv"
        return f"read_csv_auto('{escaped}{suffix}', header = true, union_by_name = true)"
    raise ValueError(f"{product['id']} has unsupported query format {fmt!r}")


def validate_plan(
    catalog: Mapping[str, Any],
    join_registry: Mapping[str, Any],
    anchor_id: str,
    selected_join_ids: Iterable[str],
    selected_columns: Mapping[str, Iterable[str]],
) -> tuple[list[dict], dict[str, dict]]:
    """Validate references, schemas, and row-expansion safety."""
    products = product_map(catalog)
    joins = join_map(join_registry)
    if anchor_id not in products:
        raise ValueError(f"Unknown anchor product: {anchor_id}")
    if products[anchor_id]["format"] not in SUPPORTED_FORMATS:
        raise ValueError(f"{anchor_id} cannot be queried by this planner")

    chosen = []
    for join_id in selected_join_ids:
        if join_id not in joins:
            raise ValueError(f"Unknown join: {join_id}")
        join = joins[join_id]
        if join["left_product_id"] != anchor_id:
            raise ValueError(f"{join_id} does not start from {anchor_id}")
        if join["right_product_id"] not in products:
            raise ValueError(f"{join_id} references an unknown right product")
        if join["relationship"] not in RELATIONSHIPS:
            raise ValueError(f"{join_id} has an unknown relationship")
        if products[join["right_product_id"]]["format"] not in SUPPORTED_FORMATS:
            raise ValueError(f"{join['right_product_id']} cannot be queried by this planner")
        left_on, right_on = as_list(join["left_on"]), as_list(join["right_on"])
        if not left_on or len(left_on) != len(right_on):
            raise ValueError(f"{join_id} has invalid join keys")
        if not set(left_on).issubset(product_columns(products[anchor_id])):
            raise ValueError(f"{join_id} uses missing anchor columns")
        if not set(right_on).issubset(product_columns(products[join["right_product_id"]])):
            raise ValueError(f"{join_id} uses missing right-side columns")
        chosen.append(join)

    expanding = [join["id"] for join in chosen if join["relationship"] == "one_to_many"]
    if len(expanding) > 1:
        raise ValueError(
            "Choose at most one row-expanding (1:many) join per query; "
            f"selected: {', '.join(expanding)}"
        )

    reachable = {anchor_id} | {join["right_product_id"] for join in chosen}
    for product_id, columns in selected_columns.items():
        if product_id not in reachable:
            raise ValueError(f"Columns selected from unreachable product: {product_id}")
        unknown = set(columns) - set(product_columns(products[product_id]))
        if unknown:
            raise ValueError(
                f"Unknown columns for {product_id}: {', '.join(sorted(unknown))}"
            )
    return chosen, products


def _filter_sql(filters: Iterable[Mapping[str, Any]], alias: str, columns: set[str]) -> list[str]:
    clauses = []
    for item in filters:
        column = str(item["column"])
        operator = str(item["operator"]).upper()
        if column not in columns:
            raise ValueError(f"Filter column is not in the anchor product: {column}")
        if operator not in FILTER_OPERATORS:
            raise ValueError(f"Unsupported filter operator: {operator}")
        value = item.get("value")
        qualified = f"{alias}.{quote_identifier(column)}"
        if operator == "IN":
            values = as_list(value)
            if not values:
                continue
            clauses.append(f"{qualified} IN ({', '.join(quote_literal(v) for v in values)})")
        else:
            clauses.append(f"{qualified} {operator} {quote_literal(value)}")
    return clauses


def build_duckdb_sql(
    catalog: Mapping[str, Any],
    join_registry: Mapping[str, Any],
    anchor_id: str,
    selected_join_ids: Iterable[str],
    selected_columns: Mapping[str, Iterable[str]],
    filters: Iterable[Mapping[str, Any]] = (),
    limit: int | None = None,
) -> str:
    """Return a deterministic, paste-ready DuckDB SELECT statement."""
    selected_join_ids = list(selected_join_ids)
    selected_columns = {key: list(value) for key, value in selected_columns.items()}
    chosen, products = validate_plan(
        catalog, join_registry, anchor_id, selected_join_ids, selected_columns
    )
    aliases = {anchor_id: "base"}
    for index, join in enumerate(chosen, start=1):
        aliases[join["right_product_id"]] = f"j{index}"

    select_lines = []
    for product_id, columns in selected_columns.items():
        alias = aliases[product_id]
        for column in columns:
            output_name = column if product_id == anchor_id else f"{product_id}__{column}"
            select_lines.append(
                f"  {alias}.{quote_identifier(column)} AS {quote_identifier(output_name)}"
            )
    if not select_lines:
        raise ValueError("Select at least one output column")

    sql = [
        "SELECT",
        ",\n".join(select_lines),
        f"FROM {source_sql(products[anchor_id])} AS base",
    ]
    for join in chosen:
        right_id = join["right_product_id"]
        alias = aliases[right_id]
        predicates = [
            f"base.{quote_identifier(left)} = {alias}.{quote_identifier(right)}"
            for left, right in zip(as_list(join["left_on"]), as_list(join["right_on"]))
        ]
        sql.extend([
            f"LEFT JOIN {source_sql(products[right_id])} AS {alias}",
            "  ON " + "\n AND ".join(predicates),
        ])

    clauses = _filter_sql(filters, "base", set(product_columns(products[anchor_id])))
    if clauses:
        sql.append("WHERE " + "\n  AND ".join(clauses))
    if limit is not None:
        if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
            raise ValueError("limit must be a positive integer")
        sql.append(f"LIMIT {limit}")
    return "\n".join(sql) + ";"


def build_copy_sql(select_sql: str, output_path: str) -> str:
    """Wrap a generated SELECT in a DuckDB Parquet export statement."""
    query = select_sql.strip()
    if query.endswith(";"):
        query = query[:-1]
    path = str(output_path).replace("'", "''")
    return f"COPY (\n{query}\n) TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD);"
