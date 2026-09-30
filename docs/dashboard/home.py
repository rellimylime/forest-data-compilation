# ==============================================================================
# docs/dashboard/home.py
# Forest Data Explorer — Home page
#
# Orients a researcher in one screen: the research question, where to go next,
# the core pipeline's state on this machine, the current results, and search.
# Navigation is declared in app.py.
# ==============================================================================

import re
import json
import html

import streamlit as st

from utils import (
    REPO_ROOT, apply_dark_css, latest_model_run, load_csv, load_product_catalog,
    metric_card, page_header, repo_path,
)

apply_dark_css()

# ------------------------------------------------------------------------------
# Pipeline inventory — all expected outputs with metadata
# ------------------------------------------------------------------------------

# Availability and schemas come from the committed repository snapshot. A local
# measured inventory is only a fallback when the snapshot is unavailable.
PIPELINE_STATE_LABEL = {
    "available": "ready",
    "partial": "present, grain unconfirmed",
    "missing": "not built here",
    "error": "unreadable",
    "unmeasured": "availability not measured",
}

# Which workflow page covers each registry family.
FAMILY_ROUTES = {
    "fia_partitions": "pages/3_FIA_Forest.py",
    "fia_summaries": "pages/3_FIA_Forest.py",
    "fia_lookups": "pages/3_FIA_Forest.py",
    "climate_grids": "pages/2_Climate.py",
    "species_niches": "pages/6_Analysis.py",
    "thermophilization": "pages/6_Analysis.py",
    "ids": "pages/1_IDS_Survey.py",
    "disturbance_linkage": "pages/6_Analysis.py",
    "analysis": "pages/6_Analysis.py",
}


CATALOG, CATALOG_SOURCE, CATALOG_ERROR = load_product_catalog()
if CATALOG:
    fams = CATALOG["families"]
    PIPELINE = []
    for prod in CATALOG["products"]:
        obs = prod.get("observed") or {}
        bits = [prod["one_row_is"]]
        if obs.get("n_rows"):
            bits.append(f"{obs['n_rows']:,} rows")
        PIPELINE.append({
            "id": prod["id"],
            "section": fams[prod["family"]]["title"],
            "family": prod["family"],
            "label": prod["title"],
            "path": prod["path"],
            "description": " · ".join(bits),
            "search_terms": " ".join([
                " ".join(prod.get("keys") or []),
                " ".join(prod.get("facets") or []),
                " ".join(str(column[0]) for column in obs.get("columns", [])),
                prod.get("producer") or "",
                prod.get("grain_id") or "",
            ]),
            "availability": prod["availability"],
            "n_rows": obs.get("n_rows"),
            "bytes": obs.get("bytes"),
        })
else:
    PIPELINE = []



# Routing for registry products lives in FAMILY_ROUTES above.

PAGE_SEARCH_INDEX = [
    {
        "title": "Repository map",
        "page": "pages/4_Architecture.py",
        "body": "Workflow map, pixel decomposition, IDS polygon extraction, FIA point extraction, and shared climate summary pattern.",
    },
    {
        "title": "IDS survey",
        "page": "pages/1_IDS_Survey.py",
        "body": "IDS damage areas, surveyed areas, host codes, DCA codes, maps, and lookup tables.",
    },
    {
        "title": "Climate datasets",
        "page": "pages/2_Climate.py",
        "body": "TerraClimate, PRISM, WorldClim variables, pixel maps, grids, schemas, and climate summaries.",
    },
    {
        "title": "Processed FIA data",
        "page": "pages/3_FIA_Forest.py",
        "body": "Repository-derived FIA products: tree metrics, filters, disturbance, damage agents, mortality, seedlings, and treatments.",
    },
    {
        "title": "Plot-visit community climate (related work)",
        "page": "pages/6_Analysis.py",
        "body": "07_thermophilization: an alternative plot-visit method with basal-area weighting and consecutive or first-to-last change. Not used by the current models.",
    },
    {
        "title": "Analysis",
        "page": "pages/6_Analysis.py",
        "body": "The core pipeline from raw FIA to model results: condition histories, cumulative mortality, site climatic water deficit, CWM change, model inputs, committed model results, and robustness checks.",
    },
    {
        "title": "Find data",
        "page": "pages/5_Data_Catalog.py",
        "body": "All repository outputs, file paths, row counts, schemas, and load examples.",
    },
    {
        "title": "Build a dataset",
        "page": "pages/8_Query_Builder.py",
        "body": "Search fields and recipes, inspect join keys and row expansion, and generate DuckDB SQL for a reproducible dataset export.",
    },
]

SCRIPT_SEARCH_INDEX = [
    {
        "title": "Build climate summaries",
        "path": "scripts/build_climate_summaries.R",
        "body": "Build monthly area-weighted climate summaries for IDS observations from TerraClimate, PRISM, or WorldClim.",
        "page": "pages/2_Climate.py",
    },
    {
        "title": "Download FIA",
        "path": "05_fia/scripts/core/01_download_fia.R",
        "body": "Download USDA FIADB source files.",
        "page": "pages/3_FIA_Forest.py",
    },
    {
        "title": "Inspect FIA",
        "path": "05_fia/scripts/core/02_inspect_fia.R",
        "body": "Inspect FIADB schema and generate lookup tables.",
        "page": "pages/3_FIA_Forest.py",
    },
    {
        "title": "Extract FIA trees and conditions",
        "path": "05_fia/scripts/core/03_extract_trees.R",
        "body": "Extract TREE, COND, PLOT-related records and build basal-area inputs.",
        "page": "pages/3_FIA_Forest.py",
    },
    {
        "title": "Extract FIA seedlings and mortality",
        "path": "05_fia/scripts/core/04_extract_seedlings_mortality.R",
        "body": "Extract SEEDLING and TREE_GRM_COMPONENT mortality source records.",
        "page": "pages/3_FIA_Forest.py",
    },
    {
        "title": "Build FIA summaries",
        "path": "05_fia/scripts/core/05_build_fia_summaries.R",
        "body": "Build analysis-ready FIA summary parquets from extracted FIA source records.",
        "page": "pages/3_FIA_Forest.py",
    },
    {
        "title": "Run condition-level analysis",
        "path": "09_analysis/scripts/run_analysis_pipeline.R",
        "body": "Build condition histories, mortality, site CWD, model inputs, preliminary models, robustness checks, and QA.",
        "page": "pages/6_Analysis.py",
    },
    {
        "title": "Build condition community climate (07, related work)",
        "path": "07_thermophilization/scripts/01_build_condition_community_climate.R",
        "body": "Compute community-weighted climate affinity per FIA condition, for one life stage.",
        "page": "pages/6_Analysis.py",
    },
    {
        "title": "Build forest plot-visit CWM (07, related work)",
        "path": "07_thermophilization/scripts/02_build_forest_plot_visit_cwm.R",
        "body": "Collapse forested conditions into one climate-affinity score per plot visit, weighted by forested-area share.",
        "page": "pages/6_Analysis.py",
    },
    {
        "title": "Build plot disturbance extent (07, related work)",
        "path": "07_thermophilization/scripts/03_build_plot_disturbance_severity.R",
        "body": "Aggregate FIA condition disturbance codes to the share of each plot visit affected by each type.",
        "page": "pages/6_Analysis.py",
    },
    {
        "title": "Build consecutive-survey change (07, related work)",
        "path": "07_thermophilization/scripts/04_build_visit_interval_change.R",
        "body": "Compare each survey with the one before it, using FIA's official remeasurement link.",
        "page": "pages/6_Analysis.py",
    },
    {
        "title": "Build first-to-last change (07, related work)",
        "path": "07_thermophilization/scripts/05_build_first_last_change.R",
        "body": "Compare a plot's earliest survey with its latest, with all three life stages on the same interval.",
        "page": "pages/6_Analysis.py",
    },
]

FIA_GUIDE_INDEX_JSON = REPO_ROOT / "05_fia" / "docs" / "dashboard" / "fiadb_user_guide_index_v94.json"
FIA_NAVIGATOR_PAGE = "pages/7_FIA_Navigator.py"


def _matches(query: str, *values) -> bool:
    q = (query or "").strip().upper()
    return bool(q) and any(q in str(value or "").upper() for value in values)


@st.cache_data(show_spinner=False)
def load_fia_guide_index() -> dict:
    if not FIA_GUIDE_INDEX_JSON.exists():
        return {}
    try:
        return json.loads(FIA_GUIDE_INDEX_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {}


def fia_extraction_hint(table: str, column: str = "") -> str:
    table = (table or "").upper()
    column = (column or "").upper()
    if table in {"TREE", "COND", "PLOT", "REF_SPECIES", "REF_FOREST_TYPE"} or column in {"SPCD", "DIA", "PLT_CN", "CONDID"}:
        return "Start with `Rscript 05_fia/scripts/core/03_extract_trees.R`, then run `Rscript 05_fia/scripts/core/05_build_fia_summaries.R`."
    if table in {"SEEDLING", "TREE_GRM_COMPONENT"} or "MORT" in column:
        return "Start with `Rscript 05_fia/scripts/core/04_extract_seedlings_mortality.R`, then run `Rscript 05_fia/scripts/core/05_build_fia_summaries.R`."
    if table.startswith("REF_"):
        return "Use the FIA navigator for the source reference table, then add the field to the relevant FIA extraction/summarizer if needed."
    return "Use the FIA navigator to inspect the source table/variable, then add it to the FIA extraction and summary scripts if it should become a workflow output."


def search_workflow(query: str) -> tuple[list[dict], list[dict]]:
    workflow_results = []
    fia_source_results = []

    for item in PAGE_SEARCH_INDEX:
        if _matches(query, item["title"], item["body"]):
            workflow_results.append(
                {
                    "kind": "Workflow page",
                    "title": item["title"],
                    "body": item["body"],
                    "meta": "Already represented in the dashboard",
                    "page": item["page"],
                }
            )

    for prod in PIPELINE:
        if _matches(query, prod["section"], prod["label"], prod["path"],
                    prod["description"], prod.get("search_terms")):
            state = PIPELINE_STATE_LABEL.get(prod["availability"], prod["availability"])
            workflow_results.append(
                {
                    "kind": "Workflow output",
                    "title": prod["label"],
                    "body": prod["description"],
                    "meta": f"{prod['section']} · {state} · {prod['path']}",
                    "page": FAMILY_ROUTES.get(prod["family"],
                                              "pages/5_Data_Catalog.py"),
                }
            )

    for item in SCRIPT_SEARCH_INDEX:
        if _matches(query, item["title"], item["path"], item["body"]):
            workflow_results.append(
                {
                    "kind": "Workflow script",
                    "title": item["title"],
                    "body": item["body"],
                    "meta": item["path"],
                    "page": item["page"],
                }
            )

    guide = load_fia_guide_index()
    for row in (guide.get("tables_index", []) or []):
        table = row.get("oracle_table", "")
        desc = row.get("description", "")
        if _matches(query, table, row.get("table_name"), desc):
            fia_source_results.append(
                {
                    "kind": "FIA source table",
                    "title": table,
                    "body": (desc or "FIADB source table.").split("\n")[0][:360],
                    "meta": fia_extraction_hint(table),
                    "navigator": True,
                }
            )

    for row in (guide.get("columns_index", []) or []):
        column = row.get("column_name", "")
        table = row.get("oracle_table", "")
        desc = row.get("descriptive_name", "")
        if _matches(query, column, table, desc):
            fia_source_results.append(
                {
                    "kind": "FIA source variable",
                    "title": f"{table}.{column}" if table else column,
                    "body": desc or "FIADB source variable.",
                    "meta": fia_extraction_hint(table, column),
                    "navigator": True,
                }
            )

    return workflow_results[:12], fia_source_results[:12]


def render_search_result(result: dict, key_prefix: str) -> None:
    st.markdown(
        f"""
        <div class="fd-card">
          <div class="fd-card-title">{html.escape(result.get("title", ""))}</div>
          <div class="fd-card-body">
            <strong>{html.escape(result.get("kind", ""))}</strong><br>
            {html.escape(result.get("body", ""))}
            <br><code>{html.escape(result.get("meta", ""))}</code>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if result.get("page"):
        if st.button("Open dashboard page", key=f"{key_prefix}_{result['title']}_{result['kind']}"):
            st.switch_page(result["page"])
    elif result.get("navigator"):
        if st.button("Open FIA navigator", key=f"{key_prefix}_{result['title']}_{result['kind']}_navigator"):
            st.switch_page(FIA_NAVIGATOR_PAGE)

# ------------------------------------------------------------------------------
# Page
# ------------------------------------------------------------------------------

START_CARDS = [
    ("Analysis and results", "pages/6_Analysis.py",
     "The three-step pipeline, the tables it builds, and the current model results, figures, and reports."),
    ("Find data", "pages/5_Data_Catalog.py",
     "Search every table and variable: what one row is, where it lives, and which script makes it."),
    ("Build a dataset", "pages/8_Query_Builder.py",
     "Start from a recipe or pick fields, see how tables join, and get a ready-to-run export query."),
    ("FIA field guide", "pages/7_FIA_Navigator.py",
     "Look up raw FIA tables and fields, including ones the pipeline has not extracted yet."),
]

CORE_STEPS = [
    ("1", "FIA inventory", "05_fia", ("fia_partitions", "fia_summaries", "fia_lookups"),
     "pages/3_FIA_Forest.py", "Processed FIA data"),
    ("2", "Species climate niches", "06_species_niches", ("species_niches",),
     "pages/5_Data_Catalog.py", "Find data"),
    ("3", "Condition-level analysis", "09_analysis", ("analysis",),
     "pages/6_Analysis.py", "Analysis"),
]


def present_count(families: tuple[str, ...]) -> tuple[int, int]:
    """How many of a step's registered tables exist on this machine."""
    products = [p for p in (CATALOG or {}).get("products", []) if p["family"] in families]
    present = sum(repo_path(p["path"]).exists() for p in products)
    return present, len(products)


def rebuild_date(run_dir) -> str:
    readme = run_dir / "README.md"
    text = readme.read_text(encoding="utf-8") if readme.is_file() else ""
    found = re.search(r"rebuild completed (\d{4}-\d{2}-\d{2})", text)
    return found.group(1) if found else run_dir.name[:8]


st.markdown(
    page_header(
        "Forest Data Explorer",
        "Forest community change after disturbance",
        "Are forest communities shifting toward species from warmer or drier parts of their "
        "ranges, and does fire, insect, or disease mortality help explain it? This dashboard "
        "shows the pipeline behind that question, the data it uses, and the current results.",
    ),
    unsafe_allow_html=True,
)

st.markdown('<div class="fd-section-label">Start here</div>', unsafe_allow_html=True)
for row_start in range(0, len(START_CARDS), 2):
    columns = st.columns(2)
    for column, (title, page, body) in zip(columns, START_CARDS[row_start:row_start + 2]):
        with column.container(border=True):
            st.markdown(f"**{title}**")
            st.caption(body)
            st.page_link(page, label="Open", icon=":material/arrow_forward:")

st.markdown('<div class="fd-section-label">Core pipeline on this machine</div>', unsafe_allow_html=True)
st.caption("Three steps produce the model data and results, in this order.")
for column, (step, title, module, families, page, label) in zip(st.columns(3), CORE_STEPS):
    present, total = present_count(families)
    with column.container(border=True):
        st.markdown(f"**{step} · {title}**")
        st.caption(f"`{module}` · {present} of {total} registered tables present")
        st.page_link(page, label=label, icon=":material/arrow_forward:")

st.markdown('<div class="fd-section-label">Current results</div>', unsafe_allow_html=True)
run_dir = latest_model_run()
model_fit, _ = load_csv(str(run_dir / "model_fit.csv")) if run_dir else (None, None)
if model_fit is None or model_fit.empty:
    st.info("No committed model run was found under `09_analysis/results/model_runs/`.")
else:
    m1, m2, m3, m4 = st.columns(4)
    m1.markdown(metric_card("Preliminary models", str(len(model_fit)),
                            "3 responses × saplings, adults, combined"), unsafe_allow_html=True)
    m2.markdown(metric_card("Conditions per model",
                            f"{model_fit['n'].min():,}–{model_fit['n'].max():,}",
                            "complete stable-condition histories"), unsafe_allow_html=True)
    m3.markdown(metric_card("Stable plots", f"{model_fit['stable_plots'].max():,}",
                            "largest model"), unsafe_allow_html=True)
    m4.markdown(metric_card("Last verified rebuild", rebuild_date(run_dir),
                            "authoritative model run"), unsafe_allow_html=True)
    st.caption(
        "Responses are first-to-last change in community climate affinity (temperature, "
        "precipitation, CWD). Predictors are cumulative fire, insect, and disease mortality "
        "and cumulative site CWD. These are preliminary models."
    )
    st.page_link("pages/6_Analysis.py", label="See coefficients, figures, and reports",
                 icon=":material/arrow_forward:")

st.markdown('<div class="fd-section-label">Search</div>', unsafe_allow_html=True)
workflow_query = st.text_input(
    "Search tables, variables, pages, scripts, and raw FIA fields",
    placeholder="Try CWM, mortality, SPCD, CONDID, damage agents, site CWD",
    key="workflow_search_query",
)
if workflow_query.strip():
    workflow_results, fia_source_results = search_workflow(workflow_query)
    if not workflow_results and not fia_source_results:
        st.info("No matches. Try a table name, a variable such as `SPCD`, or a script name.")
    else:
        result_tabs = st.tabs([
            f"In this repository ({len(workflow_results)})",
            f"Raw FIA fields, not yet extracted ({len(fia_source_results)})",
        ])
        with result_tabs[0]:
            if workflow_results:
                for i, result in enumerate(workflow_results):
                    render_search_result(result, f"workflow_result_{i}")
            else:
                st.info("No repository table, page, or script matches. Check the raw FIA tab.")
        with result_tabs[1]:
            if fia_source_results:
                for i, result in enumerate(fia_source_results):
                    render_search_result(result, f"fia_source_result_{i}")
            else:
                st.info("No raw FIA table or field matches.")

st.markdown("---")
st.caption(
    "The IDS survey, climate datasets, and repository map are under **Other workstreams** in the "
    "menu; the current analysis does not use them. Run the dashboard from the repository root "
    "with `streamlit run docs/dashboard/app.py`."
)
