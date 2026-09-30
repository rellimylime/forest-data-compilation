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
    repo_link,
    page_intro,
    REPO_ROOT,
    apply_dark_css,
    load_product_catalog,
    load_query_navigation,
)

sys.path.insert(0, str(REPO_ROOT))
from forest_explorer.catalog.query_planner import (  # noqa: E402
    SUPPORTED_FORMATS,
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

DOMAIN_ORDER = [
    "fia", "analysis", "species", "ids", "disturbance", "climate", "other",
]
DOMAIN_LABELS = {
    "fia": "FIA processed inventory",
    "analysis": "Condition-level analysis",
    "species": "Species and climate niches",
    "ids": "IDS aerial survey",
    "disturbance": "Linked disturbance evidence",
    "climate": "Climate linked to IDS areas",
    "other": "Other repository products",
}
FAMILY_DOMAINS = {
    "fia_partitions": "fia",
    "fia_summaries": "fia",
    "fia_lookups": "fia",
    "analysis": "analysis",
    "thermophilization": "analysis",
    "species_niches": "species",
    "ids": "ids",
    "disturbance_linkage": "disturbance",
    "climate_grids": "climate",
}
TOPIC_ORDER = [
    "Trees", "Seedlings and regeneration", "Conditions and site",
    "Disturbance and treatments", "Mortality", "Damage agents", "Understory",
    "Climate and community", "Time and remeasurement", "Reference labels", "Other",
]
FIELD_GROUP_ORDER = [
    "Identifiers and time", "Location and topography", "Tree structure and diversity",
    "Seedlings and regeneration", "Disturbance, treatment, and damage", "Mortality",
    "Climate and community", "Quality and eligibility", "Other fields",
]


def product_domain(product: dict) -> str:
    """Map registry families to researcher-facing builder domains."""
    if product["id"] == "analysis_fia_remeasurement_components":
        return "fia"
    return FAMILY_DOMAINS.get(product.get("family"), "other")


def product_topic(product: dict) -> str:
    """Group compatible additions by the information they contribute."""
    text = f"{product['id']} {product['title']}".casefold()
    checks = [
        ("Understory", ("understory", "p2veg")),
        ("Damage agents", ("damage_agent", "damage agent")),
        ("Mortality", ("mortality",)),
        ("Disturbance and treatments", ("disturbance", "treatment", "harvest", "fire")),
        ("Seedlings and regeneration", ("seedling", "sapling", "regeneration")),
        ("Trees", ("tree_metric", "tree species", "tree structure")),
        ("Climate and community", ("climate", "cwm", "niche", "cwd")),
        ("Time and remeasurement", ("remeasurement", "interval", "history")),
        ("Conditions and site", ("condition", "topography", "slope", "aspect")),
        ("Reference labels", ("lookup", "reference", "labels")),
    ]
    return next(
        (label for label, terms in checks if any(term in text for term in terms)),
        "Other",
    )


def field_group(column: str) -> str:
    """Place physical column names into a small semantic browsing hierarchy."""
    name = column.casefold()
    identifiers = {
        "state", "statecd", "plt_cn", "prev_plt_cn", "condid", "subp", "spcd",
        "invyr", "t1_invyr", "t2_invyr", "year", "month", "calendar_year",
        "calendar_month", "water_year", "layer", "community_layer",
    }
    if (name in identifiers or name.endswith("_id") or name.endswith("_key")
            or "date" in name or "year" in name or "month" in name):
        return "Identifiers and time"
    if any(term in name for term in (
        "latitude", "longitude", "elev", "slope", "aspect", "physio", "coordinate",
    )):
        return "Location and topography"
    if any(term in name for term in ("seedling", "sapling", "regen")):
        return "Seedlings and regeneration"
    if any(term in name for term in ("mort", "cause_of_death", "tpamort")):
        return "Mortality"
    if any(term in name for term in (
        "disturb", "dstrb", "treat", "trt", "fire", "insect", "disease",
        "damage", "agent", "harvest",
    )):
        return "Disturbance, treatment, and damage"
    if any(term in name for term in (
        "temperature", "precip", "climate", "cwd", "cwm", "tmm", "vpd", "pet", "aet",
    )):
        return "Climate and community"
    if any(term in name for term in (
        "flag", "exclude", "eligible", "complete", "valid", "quality", "status",
    )):
        return "Quality and eligibility"
    if any(term in name for term in (
        "basal", "ba_", "_ba", "tree", "tpa", "diameter", "_dia", "shannon",
        "richness", "species", "softwood", "hardwood",
    )):
        return "Tree structure and diversity"
    return "Other fields"


def grouped_columns(columns: list[str]) -> dict[str, list[str]]:
    grouped = {label: [] for label in FIELD_GROUP_ORDER}
    for column in columns:
        grouped[field_group(column)].append(column)
    return {label: grouped[label] for label in FIELD_GROUP_ORDER if grouped[label]}


st.title("🧩 Build a Dataset")
page_intro(
    "Decide what one output row should represent, browse only the information that can be "
    "attached safely, and choose the exact variables you need. The builder generates DuckDB "
    "SQL; it never runs the query or changes data.",
    "tool",
    [("pages/5_Data_Catalog.py", "Find data"), ("pages/6_Analysis.py", "Analysis")],
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
        st.dataframe(pd.DataFrame(results), width="stretch", hide_index=True)
    else:
        st.info("No snapshot result matches that search. The field may not be extracted yet.")

st.subheader("1. Choose what one output row represents")
recipe_options = ["custom"] + list(preset_by_id)
recipe_id = st.selectbox(
    "Optional starting recipe",
    recipe_options,
    format_func=lambda value: (
        "Build my own dataset" if value == "custom" else preset_by_id[value]["title"]
    ),
)
recipe = preset_by_id.get(recipe_id)
if recipe:
    st.markdown(recipe["description"])
    if recipe.get("cautions"):
        with st.expander("Recipe cautions"):
            for caution in as_list(recipe["cautions"]):
                st.markdown(f"- {caution}")
    missing = as_list(recipe.get("missing_capabilities"))
    if missing:
        st.warning("Still missing from this recipe: " + "; ".join(missing))

queryable_anchor_ids = sorted(
    [
        product_id for product_id, product in products.items()
        if product.get("format") in SUPPORTED_FORMATS
        and product.get("grain_id")
        and product_columns(product)
        and product.get("availability") in ("available", "partial")
    ],
    key=lambda product_id: products[product_id]["title"],
)
if not queryable_anchor_ids:
    st.error("No available queryable products were found in this catalog snapshot.")
    st.stop()

default_anchor = (
    recipe["anchor_product_id"]
    if recipe and recipe["anchor_product_id"] in queryable_anchor_ids
    else (
        "plot_condition_metadata"
        if "plot_condition_metadata" in queryable_anchor_ids
        else queryable_anchor_ids[0]
    )
)
domain_options = [
    domain for domain in DOMAIN_ORDER
    if any(product_domain(products[product_id]) == domain for product_id in queryable_anchor_ids)
]
default_domain = product_domain(products[default_anchor])
domain_id = st.selectbox(
    "Data area",
    domain_options,
    index=domain_options.index(default_domain),
    format_func=DOMAIN_LABELS.get,
    key=f"domain_{recipe_id}",
)
domain_anchor_ids = [
    product_id for product_id in queryable_anchor_ids
    if product_domain(products[product_id]) == domain_id
]
grain_ids = list(dict.fromkeys(
    products[product_id]["grain_id"] for product_id in domain_anchor_ids
))
grain_ids.sort(key=lambda value: catalog.get("grains", {}).get(value, {}).get(
    "one_row_is", value
))
default_grain = products[default_anchor]["grain_id"]
if default_grain not in grain_ids:
    default_grain = grain_ids[0]
grain_id = st.selectbox(
    "One output row should represent",
    grain_ids,
    index=grain_ids.index(default_grain),
    format_func=lambda value: catalog.get("grains", {}).get(value, {}).get(
        "one_row_is", value
    ),
    key=f"grain_{recipe_id}_{domain_id}",
)
anchor_ids = [
    product_id for product_id in domain_anchor_ids
    if products[product_id]["grain_id"] == grain_id
]
source_default = default_anchor if default_anchor in anchor_ids else anchor_ids[0]
anchor_id = st.selectbox(
    "Start with this repository product",
    anchor_ids,
    index=anchor_ids.index(source_default),
    format_func=lambda product_id: products[product_id]["title"],
    key=f"anchor_{recipe_id}_{domain_id}_{grain_id}",
)
anchor = products[anchor_id]
st.success(f"**Starting grain:** {anchor['one_row_is']}.")
st.caption(
    f"Repository product: `{anchor_id}` · path: `{anchor['path']}` · "
    f"identifier: `{', '.join(as_list(anchor.get('keys')))}`"
)

st.subheader("2. Add compatible information")
options = [
    join for join in joins_for_anchor(joins, anchor_id)
    if join["right_product_id"] in products
    and products[join["right_product_id"]].get("availability") in ("available", "partial")
    and products[join["right_product_id"]].get("format") in SUPPORTED_FORMATS
]
option_ids = [join["id"] for join in options]
default_joins = [
    join_id
    for join_id in as_list(recipe.get("join_ids") if recipe else [])
    if join_id in option_ids
]
available_topics = [
    topic for topic in TOPIC_ORDER
    if any(product_topic(products[join["right_product_id"]]) == topic for join in options)
]
default_topics = list(dict.fromkeys(
    product_topic(products[join_by_id[join_id]["right_product_id"]])
    for join_id in default_joins
))

selected_join_ids = []
if available_topics:
    selected_topics = st.multiselect(
        "Information categories",
        available_topics,
        default=default_topics,
        placeholder="Choose a category to see compatible products",
        key=f"topics_{recipe_id}_{anchor_id}",
    )
    for topic in selected_topics:
        topic_join_ids = [
            join["id"] for join in options
            if product_topic(products[join["right_product_id"]]) == topic
        ]
        topic_defaults = [
            join_id for join_id in default_joins if join_id in topic_join_ids
        ]
        selected_join_ids.extend(st.multiselect(
            topic,
            topic_join_ids,
            default=topic_defaults,
            format_func=lambda join_id: (
                f"{products[join_by_id[join_id]['right_product_id']]['title']} · "
                f"{RELATIONSHIP_LABELS[join_by_id[join_id]['relationship']]}"
            ),
            key=f"joins_{recipe_id}_{anchor_id}_{topic}",
        ))
    st.caption(
        "Only products with a documented, one-step key relationship to the starting "
        "product are offered."
    )
else:
    st.info(
        "No additional products have a documented one-step join to this starting "
        "product. You can still select and export fields from it."
    )

if domain_id == "fia":
    with st.expander("Why aren't general TerraClimate variables available here?"):
        st.markdown(
            "FIA records and monthly climate grids do not share a direct table key. A valid "
            "link must first define both the spatial match and the time window—for example, "
            "measurement-year climate, a 30-year normal, or climate between plot visits. "
            "The repository does not yet register a general FIA–TerraClimate bridge, so this "
            "builder will not invent one. Analysis-specific climate summaries will appear here "
            "once their meaning, grain, and join keys are documented."
        )

selected_joins = [join_by_id[join_id] for join_id in selected_join_ids]
expanding = [join for join in selected_joins if join["relationship"] == "one_to_many"]
plan_blocked = len(expanding) > 1
if plan_blocked:
    st.error(
        "Choose only one row-expanding addition. Combining multiple detail tables would "
        "create an ambiguous cross-product rather than a trustworthy dataset."
    )

if selected_joins:
    with st.expander("Compatibility details", expanded=bool(expanding)):
        compatibility_rows = []
        for join in selected_joins:
            right = products[join["right_product_id"]]
            compatibility_rows.append({
                "Information": right["title"],
                "Row effect": RELATIONSHIP_LABELS[join["relationship"]],
                "Join key": " + ".join(as_list(join["left_on"])),
                "Review": STATUS_LABELS[join["status"]],
            })
        st.dataframe(
            pd.DataFrame(compatibility_rows), width="stretch", hide_index=True
        )
        for join in selected_joins:
            if join.get("warning"):
                st.markdown(f"- **{join['title']}:** {join['warning']}")

st.subheader("3. Select the exact variables")
reachable_ids = [anchor_id] + [join["right_product_id"] for join in selected_joins]
recipe_columns = (
    recipe.get("columns", {})
    if recipe and recipe["anchor_product_id"] == anchor_id
    else {}
)
selected_columns = {}
selection_key = "_".join(selected_join_ids) or "single"
for product_id in reachable_ids:
    product = products[product_id]
    columns = product_columns(product)
    defaults = [
        column for column in as_list(recipe_columns.get(product_id))
        if column in columns
    ]
    if not defaults:
        defaults = [
            column for column in as_list(product.get("keys")) if column in columns
        ]
    selected_columns[product_id] = []
    with st.expander(product["title"], expanded=True):
        st.caption(f"{product['one_row_is']} · {product['path']}")
        for group, group_fields in grouped_columns(columns).items():
            group_defaults = [
                column for column in defaults if column in group_fields
            ]
            group_key = "".join(
                character if character.isalnum() else "_"
                for character in group
            ).strip("_").lower()
            selected_columns[product_id].extend(st.multiselect(
                group,
                group_fields,
                default=group_defaults,
                key=(
                    f"fields_{recipe_id}_{anchor_id}_{product_id}_"
                    f"{selection_key}_{group_key}"
                ),
            ))

selected_field_count = sum(len(columns) for columns in selected_columns.values())
st.caption(f"{selected_field_count} variable(s) selected.")

filters = []
anchor_columns = set(product_columns(anchor))
with st.expander("Optional row filters"):
    filter_columns = st.columns(3)
    with filter_columns[0]:
        state_values = st.text_input(
            "State filter (comma-separated)",
            placeholder="CA, OR, WA",
            disabled="state" not in anchor_columns,
        )
        if state_values and "state" in anchor_columns:
            values = [
                value.strip() for value in state_values.split(",") if value.strip()
            ]
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
            "Layer filter (comma-separated)",
            placeholder="saplings, trees",
            disabled="layer" not in anchor_columns,
        )
        if layer_values and "layer" in anchor_columns:
            values = [
                value.strip() for value in layer_values.split(",") if value.strip()
            ]
            filters.append({"column": "layer", "operator": "IN", "value": values})

with st.expander("Preview and export settings"):
    preview_only = st.checkbox("Add a 1,000-row preview limit", value=True)
    output_path = st.text_input(
        "Export path used in the generated command",
        value="scratch_output/forest_query.parquet",
    )

st.subheader("4. Review the result")
result_grain_id = (
    expanding[0]["result_grain_id"] if len(expanding) == 1 else anchor["grain_id"]
)
result_grain = catalog.get("grains", {}).get(result_grain_id, {})
result_description = result_grain.get("one_row_is", anchor["one_row_is"])
if expanding:
    st.warning(f"**One output row will represent:** {result_description}.")
else:
    st.success(f"**One output row will represent:** {result_description}.")

roadmap_rows = [{
    "Step": "Start",
    "Product": anchor["title"],
    "Row effect": "Defines the starting rows",
    "Join key": ", ".join(as_list(anchor.get("keys"))),
}]
for join in selected_joins:
    right = products[join["right_product_id"]]
    roadmap_rows.append({
        "Step": "Add",
        "Product": right["title"],
        "Row effect": RELATIONSHIP_LABELS[join["relationship"]],
        "Join key": " + ".join(as_list(join["left_on"])),
    })
st.dataframe(pd.DataFrame(roadmap_rows), width="stretch", hide_index=True)

if plan_blocked:
    st.stop()
if not selected_field_count:
    st.error("Select at least one variable before generating a query.")
    st.stop()

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
    "GitHub-readable join map and recipes: " + repo_link("docs/QUERY_GUIDE.md") + ". "
    "Single-product paths and schemas remain in " + repo_link("docs/DATA_CATALOG.md") + "."
)
