# ==============================================================================
# pages/8_Query_Builder.py
# Snapshot-backed product search, join roadmap, and DuckDB SQL generator.
# ==============================================================================

import json
import shlex
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import (  # noqa: E402
    REPO_ROOT,
    apply_dark_css,
    load_product_catalog,
    load_query_navigation,
    render_top_nav,
)

sys.path.insert(0, str(REPO_ROOT))
from forest_explorer.catalog.query_planner import (  # noqa: E402
    as_list,
    build_copy_sql,
    build_duckdb_sql,
    joins_for_anchor,
    product_columns,
)

st.set_page_config(
    page_title="Build a Dataset",
    page_icon="🧩",
    layout="wide",
    initial_sidebar_state="collapsed",
)
apply_dark_css()
render_top_nav()

catalog, catalog_source, catalog_error = load_product_catalog()
joins, presets = load_query_navigation()
if not catalog or not joins or not presets:
    st.error(
        catalog_error
        or "The committed catalog, join map, or recipe snapshot is unavailable. "
        "Run `Rscript forest_explorer/catalog/build_snapshot.R`."
    )
    st.stop()

products = {product["id"]: product for product in catalog["products"]}
join_by_id = {join["id"]: join for join in joins["joins"]}
preset_by_id = {preset["id"]: preset for preset in presets["presets"]}
RELATIONSHIP_LABELS = {
    "one_to_one": "1:1 · preserves rows",
    "many_to_one": "many:1 · preserves anchor rows",
    "one_to_many": "1:many · expands rows",
}
STATUS_LABELS = {
    "certified": "Certified",
    "constrained": "Constrained",
    "review_required": "Scientific review required",
}

st.title("🧩 Build a Dataset")
st.markdown(
    "Search the committed product snapshot, choose an existing table as the "
    "anchor, and add only documented joins. The builder returns a roadmap and "
    "paste-ready DuckDB SQL; it does not scan, alter, or regenerate repository data."
)
st.caption(
    f"Catalog source: {catalog_source} · catalog v{catalog['registry_version']} · "
    f"joins v{joins['join_registry_version']} · recipes v{presets['preset_registry_version']}"
)

st.subheader("Find a field, product, or recipe")
search = st.text_input(
    "Search the snapshot",
    placeholder="Try slope, mortality, Shannon, understory, agent, CWD, or SPCD",
    label_visibility="collapsed",
)
if search:
    query = search.casefold()
    results = []
    for preset in presets["presets"]:
        blob = " ".join([
            preset["title"], preset["description"],
            " ".join(as_list(preset.get("request_tags"))),
            " ".join(as_list(preset.get("missing_capabilities"))),
        ]).casefold()
        if query in blob:
            missing = [item for item in as_list(preset.get("missing_capabilities"))
                       if query in item.casefold()]
            results.append({
                "Type": "Recipe",
                "Name": preset["title"],
                "Where / status": ("Not yet available: " + "; ".join(missing)
                                   if missing else preset["anchor_product_id"]),
            })
    for product in catalog["products"]:
        base_blob = " ".join([
            product["title"], product["id"], product["path"],
            product["one_row_is"], " ".join(product_columns(product)),
        ]).casefold()
        if query in base_blob:
            matching = [column for column in product_columns(product)
                        if query in column.casefold()]
            results.append({
                "Type": "Product" if not matching else "Product + field",
                "Name": product["title"],
                "Where / status": ", ".join(matching[:8]) if matching else product["path"],
            })
    for join in joins["joins"]:
        blob = " ".join([
            join["title"], join["warning"], join["left_product_id"],
            join["right_product_id"], " ".join(as_list(join["left_on"])),
        ]).casefold()
        if query in blob:
            results.append({"Type": "Join", "Name": join["title"],
                            "Where / status": RELATIONSHIP_LABELS[join["relationship"]]})
    if results:
        st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)
    else:
        st.info("No snapshot result matches that search. The field may not be extracted yet.")

st.subheader("1. Choose a starting point")
recipe_options = ["custom"] + list(preset_by_id)
recipe_id = st.selectbox(
    "Research recipe",
    recipe_options,
    format_func=lambda value: "Custom query" if value == "custom" else preset_by_id[value]["title"],
)
recipe = preset_by_id.get(recipe_id)
if recipe:
    st.markdown(recipe["description"])
    if recipe.get("cautions"):
        with st.expander("Recipe cautions", expanded=recipe_id == "boss_baseline_export"):
            for caution in as_list(recipe["cautions"]):
                st.markdown(f"- {caution}")
    missing = as_list(recipe.get("missing_capabilities"))
    if missing:
        st.warning("Still missing from this recipe: " + "; ".join(missing))

anchor_ids = sorted(
    {join["left_product_id"] for join in joins["joins"]},
    key=lambda product_id: products[product_id]["title"],
)
default_anchor = recipe["anchor_product_id"] if recipe else anchor_ids[0]
anchor_id = st.selectbox(
    "Anchor product — its row scale controls the result",
    anchor_ids,
    index=anchor_ids.index(default_anchor),
    format_func=lambda product_id: (
        f"{products[product_id]['title']} · {products[product_id]['one_row_is']}"
    ),
    key=f"anchor_{recipe_id}",
)
anchor = products[anchor_id]
st.caption(f"Path: `{anchor['path']}` · key: `{', '.join(as_list(anchor.get('keys')))}`")

st.subheader("2. Add compatible products")
options = joins_for_anchor(joins, anchor_id)
option_ids = [join["id"] for join in options]
default_joins = [join_id for join_id in as_list(recipe.get("join_ids") if recipe else [])
                 if join_id in option_ids]
selected_join_ids = st.multiselect(
    "Documented one-hop joins",
    option_ids,
    default=default_joins,
    format_func=lambda join_id: (
        f"{join_by_id[join_id]['title']} · "
        f"{RELATIONSHIP_LABELS[join_by_id[join_id]['relationship']]}"
    ),
    key=f"joins_{recipe_id}_{anchor_id}",
)
selected_joins = [join_by_id[join_id] for join_id in selected_join_ids]
expanding = [join for join in selected_joins if join["relationship"] == "one_to_many"]
if len(expanding) > 1:
    st.error(
        "Choose only one 1:many join. Combining detail tables would create an "
        "ambiguous cross-product rather than a trustworthy dataset."
    )

roadmap_rows = [{
    "Step": "Anchor", "Product": anchor["title"],
    "Grain effect": anchor["one_row_is"],
    "Keys": ", ".join(as_list(anchor.get("keys"))),
    "Review": anchor["review_status"],
}]
for join in selected_joins:
    right = products[join["right_product_id"]]
    roadmap_rows.append({
        "Step": "LEFT JOIN", "Product": right["title"],
        "Grain effect": RELATIONSHIP_LABELS[join["relationship"]],
        "Keys": " + ".join(as_list(join["left_on"])),
        "Review": STATUS_LABELS[join["status"]],
    })
st.dataframe(pd.DataFrame(roadmap_rows), use_container_width=True, hide_index=True)
for join in selected_joins:
    st.warning(f"{join['title']}: {join['warning']}")

st.subheader("3. Choose fields and optional filters")
reachable_ids = [anchor_id] + [join["right_product_id"] for join in selected_joins]
recipe_columns = recipe.get("columns", {}) if recipe and recipe["anchor_product_id"] == anchor_id else {}
selected_columns = {}
for product_id in reachable_ids:
    product = products[product_id]
    columns = product_columns(product)
    defaults = [column for column in as_list(recipe_columns.get(product_id)) if column in columns]
    if not defaults:
        defaults = [column for column in as_list(product.get("keys")) if column in columns]
    selected_columns[product_id] = st.multiselect(
        product["title"], columns, default=defaults,
        key=f"columns_{recipe_id}_{anchor_id}_{product_id}_{'_'.join(selected_join_ids)}",
    )

filters = []
anchor_columns = set(product_columns(anchor))
filter_columns = st.columns(3)
with filter_columns[0]:
    state_values = st.text_input(
        "State filter (comma-separated)", placeholder="CA, OR, WA",
        disabled="state" not in anchor_columns,
    )
    if state_values and "state" in anchor_columns:
        values = [value.strip() for value in state_values.split(",") if value.strip()]
        filters.append({"column": "state", "operator": "IN", "value": values})
with filter_columns[1]:
    year_column = next((name for name in (
        "INVYR", "T2_INVYR", "T1_INVYR", "first_inventory_year"
    ) if name in anchor_columns), None)
    use_years = st.checkbox("Filter years", disabled=year_column is None)
    if use_years and year_column:
        start_year = st.number_input("First year", 1900, 2100, 2000)
        end_year = st.number_input("Last year", 1900, 2100, 2025)
        filters.extend([
            {"column": year_column, "operator": ">=", "value": int(start_year)},
            {"column": year_column, "operator": "<=", "value": int(end_year)},
        ])
with filter_columns[2]:
    layer_values = st.text_input(
        "Layer filter (comma-separated)", placeholder="saplings, trees",
        disabled="layer" not in anchor_columns,
    )
    if layer_values and "layer" in anchor_columns:
        values = [value.strip() for value in layer_values.split(",") if value.strip()]
        filters.append({"column": "layer", "operator": "IN", "value": values})

preview_only = st.checkbox("Add a 1,000-row preview limit", value=True)
output_path = st.text_input(
    "Export path used in the generated command",
    value="scratch_output/forest_query.parquet",
)

st.subheader("4. Review and run deliberately")
try:
    select_sql = build_duckdb_sql(
        catalog, joins, anchor_id, selected_join_ids, selected_columns,
        filters=filters, limit=1000 if preview_only else None,
    )
    export_sql = build_copy_sql(select_sql, output_path)
except ValueError as error:
    st.error(str(error))
    st.stop()

st.success(
    "The plan is structurally valid against the committed snapshot. It has not "
    "been executed and scientific-review warnings above still apply."
)
query_tab, export_tab, run_tab = st.tabs(["Preview query", "Parquet export", "How to run"])
with query_tab:
    st.code(select_sql, language="sql")
with export_tab:
    st.code(export_sql, language="sql")
    st.download_button(
        "Download forest_query.sql", data=export_sql + "\n",
        file_name="forest_query.sql", mime="text/plain",
    )
with run_tab:
    # DuckDB's COPY does not create missing directories.
    output_dir = Path(output_path).parent.as_posix()
    st.markdown("From the repository root, after installing DuckDB:")
    st.code(f"mkdir -p {shlex.quote(output_dir)}\nduckdb < forest_query.sql", language="bash")
    st.markdown("Or run the downloaded SQL through R:")
    st.code(
        f"""library(DBI)
library(duckdb)
dir.create({json.dumps(output_dir)}, recursive = TRUE, showWarnings = FALSE)
con <- dbConnect(duckdb())
sql <- paste(readLines("forest_query.sql"), collapse = "\\n")
dbExecute(con, sql)
dbDisconnect(con, shutdown = TRUE)""",
        language="r",
    )

st.caption(
    "GitHub-readable join map and recipes: `docs/QUERY_GUIDE.md`. "
    "Single-product paths and schemas remain in `docs/DATA_CATALOG.md`."
)
