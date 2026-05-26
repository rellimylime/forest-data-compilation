# ==============================================================================
# docs/dashboard/app.py
# Forest Data Compilation — Unified Repository Dashboard
#
# Multi-page Streamlit app covering the full pipeline:
#   IDS aerial survey data (01_ids)
#   Climate datasets: TerraClimate, PRISM, WorldClim (02-04)
#   FIA forest inventory data (05_fia)
#
# Usage:
#   streamlit run docs/dashboard/app.py
#
# Pages are in docs/dashboard/pages/ and appear automatically in the sidebar.
# ==============================================================================

import os
import json
import html
from pathlib import Path

import pandas as pd
import streamlit as st

from utils import (
    REPO_ROOT, apply_dark_css, metric_card, parquet_meta,
    file_status, repo_path, color_status, PLOTLY_AVAILABLE,
    plot_source_link, render_top_nav, route_grid,
)

st.set_page_config(
    page_title="Forest Data Explorer",
    page_icon="🌲",
    layout="wide",
    initial_sidebar_state="collapsed",
)
apply_dark_css()
render_top_nav()

# ------------------------------------------------------------------------------
# Pipeline inventory — all expected outputs with metadata
# ------------------------------------------------------------------------------

PIPELINE = [
    # (section, label, rel_path, description)
    ("IDS", "IDS cleaned geopackage",
     "01_ids/data/processed/ids_layers_cleaned.gpkg",
     "4.4M damage areas, 1.2M damage points, 74.5K surveyed areas (3 layers)"),

    ("TerraClimate", "IDS pixel map",
     "02_terraclimate/data/processed/pixel_maps/damage_areas_pixel_map.parquet",
     "Links each IDS damage area to TerraClimate 4km pixels (coverage-weighted)"),
    ("TerraClimate", "Summaries — aet",
     "processed/climate/terraclimate/damage_areas_summaries/aet.parquet",
     "Monthly actual evapotranspiration per damage area (area-weighted, 1997-2024)"),
    ("TerraClimate", "Summaries — def",
     "processed/climate/terraclimate/damage_areas_summaries/def.parquet",
     "Monthly climate water deficit per damage area (area-weighted, 1997-2024)"),
    ("TerraClimate", "Summaries — pdsi",
     "processed/climate/terraclimate/damage_areas_summaries/pdsi.parquet",
     "Monthly Palmer Drought Severity Index per damage area (area-weighted, 1997-2024)"),
    ("TerraClimate", "Summaries — pet",
     "processed/climate/terraclimate/damage_areas_summaries/pet.parquet",
     "Monthly reference evapotranspiration per damage area (area-weighted, 1997-2024)"),
    ("TerraClimate", "Summaries — pr",
     "processed/climate/terraclimate/damage_areas_summaries/pr.parquet",
     "Monthly precipitation per damage area (area-weighted, 1997-2024)"),
    ("TerraClimate", "Summaries — ro",
     "processed/climate/terraclimate/damage_areas_summaries/ro.parquet",
     "Monthly runoff per damage area (area-weighted, 1997-2024)"),
    ("TerraClimate", "Summaries — soil",
     "processed/climate/terraclimate/damage_areas_summaries/soil.parquet",
     "Monthly soil moisture per damage area (area-weighted, 1997-2024)"),
    ("TerraClimate", "Summaries — srad",
     "processed/climate/terraclimate/damage_areas_summaries/srad.parquet",
     "Monthly downward shortwave radiation per damage area (area-weighted, 1997-2024)"),
    ("TerraClimate", "Summaries — swe",
     "processed/climate/terraclimate/damage_areas_summaries/swe.parquet",
     "Monthly snow water equivalent per damage area (area-weighted, 1997-2024)"),
    ("TerraClimate", "Summaries — tmmn",
     "processed/climate/terraclimate/damage_areas_summaries/tmmn.parquet",
     "Monthly min temperature per damage area (area-weighted, 1997-2024)"),
    ("TerraClimate", "Summaries — tmmx",
     "processed/climate/terraclimate/damage_areas_summaries/tmmx.parquet",
     "Monthly max temperature per damage area (area-weighted, 1997-2024)"),
    ("TerraClimate", "Summaries — vap",
     "processed/climate/terraclimate/damage_areas_summaries/vap.parquet",
     "Monthly vapor pressure per damage area (area-weighted, 1997-2024)"),
    ("TerraClimate", "Summaries — vpd",
     "processed/climate/terraclimate/damage_areas_summaries/vpd.parquet",
     "Monthly vapor pressure deficit per damage area (area-weighted, 1997-2024)"),
    ("TerraClimate", "Summaries — vs",
     "processed/climate/terraclimate/damage_areas_summaries/vs.parquet",
     "Monthly wind speed per damage area (area-weighted, 1997-2024)"),

    ("PRISM", "IDS pixel map",
     "03_prism/data/processed/pixel_maps/damage_areas_pixel_map.parquet",
     "Links each IDS damage area to PRISM 800m pixels (coverage-weighted, CONUS only)"),
    ("PRISM", "Summaries — ppt",
     "processed/climate/prism/damage_areas_summaries/ppt.parquet",
     "Monthly precipitation per damage area (area-weighted, 1997-2024)"),
    ("PRISM", "Summaries — tdmean",
     "processed/climate/prism/damage_areas_summaries/tdmean.parquet",
     "Monthly mean dew point temperature per damage area (area-weighted, 1997-2024)"),
    ("PRISM", "Summaries — tmax",
     "processed/climate/prism/damage_areas_summaries/tmax.parquet",
     "Monthly max temperature per damage area (area-weighted, 1997-2024)"),
    ("PRISM", "Summaries — tmean",
     "processed/climate/prism/damage_areas_summaries/tmean.parquet",
     "Monthly mean temperature per damage area (area-weighted, 1997-2024)"),
    ("PRISM", "Summaries — tmin",
     "processed/climate/prism/damage_areas_summaries/tmin.parquet",
     "Monthly min temperature per damage area (area-weighted, 1997-2024)"),
    ("PRISM", "Summaries — vpdmax",
     "processed/climate/prism/damage_areas_summaries/vpdmax.parquet",
     "Monthly max vapor pressure deficit per damage area (area-weighted, 1997-2024)"),
    ("PRISM", "Summaries — vpdmin",
     "processed/climate/prism/damage_areas_summaries/vpdmin.parquet",
     "Monthly min vapor pressure deficit per damage area (area-weighted, 1997-2024)"),

    ("WorldClim", "IDS pixel map",
     "04_worldclim/data/processed/pixel_maps/damage_areas_pixel_map.parquet",
     "Links each IDS damage area to WorldClim 4.5km pixels (coverage-weighted)"),
    ("WorldClim", "Summaries — prec",
     "processed/climate/worldclim/damage_areas_summaries/prec.parquet",
     "Monthly precipitation per damage area (area-weighted, 1997-2024)"),
    ("WorldClim", "Summaries — tmax",
     "processed/climate/worldclim/damage_areas_summaries/tmax.parquet",
     "Monthly max temperature per damage area (area-weighted, 1997-2024)"),
    ("WorldClim", "Summaries — tmin",
     "processed/climate/worldclim/damage_areas_summaries/tmin.parquet",
     "Monthly min temperature per damage area (area-weighted, 1997-2024)"),

    ("FIA", "Tree metrics",
     "05_fia/data/processed/summaries/plot_tree_metrics.parquet",
     "BA, diversity, size class, canopy layer per plot × year"),
    ("FIA", "Plot exclusion flags",
     "05_fia/data/processed/summaries/plot_exclusion_flags.parquet",
     "Pre-built nonforest / harvest / human-disturbance filter flags"),
    ("FIA", "Disturbance history",
     "05_fia/data/processed/summaries/plot_disturbance_history.parquet",
     "Long-format fire, insect, disease, and other disturbance events"),
    ("FIA", "Damage agents",
     "05_fia/data/processed/summaries/plot_damage_agents.parquet",
     "Tree-level insect and disease agent codes"),
    ("FIA", "Mortality metrics",
     "05_fia/data/processed/summaries/plot_mortality_metrics.parquet",
     "Between-measurement mortality by agent (natural + harvest)"),
    ("FIA", "Seedling metrics",
     "05_fia/data/processed/summaries/plot_seedling_metrics.parquet",
     "Seedling regeneration by functional group and diversity"),
    ("FIA", "Treatment history",
     "05_fia/data/processed/summaries/plot_treatment_history.parquet",
     "Silvicultural treatments: cutting, site prep, regen (all 5 TRTCD codes)"),
    ("FIA", "Condition / forest type",
     "05_fia/data/processed/summaries/plot_cond_fortypcd.parquet",
     "Condition-level forest type and disturbance codes pass-through"),
    ("FIA", "Condition metadata",
     "05_fia/data/processed/summaries/plot_condition_metadata.parquet",
     "Condition-level stable plot IDs, coordinates, forest type groups, and forested area fields"),
    ("FIA", "Seedling species",
     "05_fia/data/processed/summaries/plot_seedling_species.parquet",
     "Species-level seedling counts per plot condition, preserving SPCD for recruitment analyses"),
    ("FIA", "Disturbance classification",
     "05_fia/data/processed/summaries/plot_disturbance_classification.parquet",
     "Control/disturbed eligibility, natural disturbance classes, timing, and matching strata"),
    ("FIA", "Site pixel map",
     "05_fia/data/processed/site_climate/site_pixel_map.parquet",
     "TerraClimate 4km pixel assignments for 6,956 FIA plot locations"),
    ("FIA", "Site climate",
     "05_fia/data/processed/site_climate/site_climate.parquet",
     "Monthly TerraClimate at FIA sites: 6 variables, 1958–2024 (23.5M rows)"),

    ("Species niches", "Species climate niches",
     "06_species_niches/data/processed/species_climate_niches.parquet",
     "External occurrence-based climate envelopes for FIA tree species"),
    ("Thermophilization", "Recruitment CWM",
     "07_thermophilization/data/processed/plot_recruitment_cwm.parquet",
     "Seedling community-weighted climate affinity per FIA condition visit"),
    ("Thermophilization", "Matched disturbed-control pairs",
     "07_thermophilization/data/processed/plot_matches.parquet",
     "Five climate-matched controls per disturbed FIA condition with pairwise CWM deltas"),
    ("Thermophilization", "Class x region summary",
     "07_thermophilization/data/processed/thermophilization_by_class_region.parquet",
     "Bootstrap mean deltas by disturbance class and East/West region"),
    ("Thermophilization", "High-severity fire summary",
     "07_thermophilization/data/processed/thermophilization_high_severity.parquet",
     "Crown-fire high-severity proxy summary by East/West region"),
    ("Thermophilization", "Time x region summary",
     "07_thermophilization/data/processed/thermophilization_by_time_region.parquet",
     "Time-since-disturbance summary pooled across natural disturbance classes"),
    ("Thermophilization", "Class x time x region summary",
     "07_thermophilization/data/processed/thermophilization_by_class_time_region.parquet",
     "Time-since-disturbance summary stratified by disturbance class and region"),
    ("Thermophilization", "Disturbance year coverage",
     "07_thermophilization/data/processed/disturbance_year_coverage.parquet",
     "Diagnostic for how often FIA has usable disturbance years"),
]


PAGE_ROUTES = {
    "IDS": "pages/1_IDS_Survey.py",
    "TerraClimate": "pages/2_Climate.py",
    "PRISM": "pages/2_Climate.py",
    "WorldClim": "pages/2_Climate.py",
    "FIA": "pages/3_FIA_Forest.py",
    "Thermophilization": "pages/6_Thermophilization.py",
    "Architecture": "pages/4_Architecture.py",
    "Data Catalog": "pages/5_Data_Catalog.py",
}

PAGE_SEARCH_INDEX = [
    {
        "title": "Architecture",
        "page": "pages/4_Architecture.py",
        "body": "Workflow map, pixel decomposition, IDS polygon extraction, FIA point extraction, and shared climate summary pattern.",
    },
    {
        "title": "IDS Survey",
        "page": "pages/1_IDS_Survey.py",
        "body": "IDS damage areas, surveyed areas, host codes, DCA codes, maps, and lookup tables.",
    },
    {
        "title": "Climate",
        "page": "pages/2_Climate.py",
        "body": "TerraClimate, PRISM, WorldClim variables, pixel maps, grids, schemas, and climate summaries.",
    },
    {
        "title": "FIA Forest",
        "page": "pages/3_FIA_Forest.py",
        "body": "FIA derived products: tree metrics, filters, disturbance, damage agents, mortality, seedlings, treatments, site climate.",
    },
    {
        "title": "Thermophilization",
        "page": "pages/6_Thermophilization.py",
        "body": "FIA recruitment thermophilization workflow: species climate affinity, seedling CWM, disturbed-control matching, deltas, and species-shift checks.",
    },
    {
        "title": "Data Catalog",
        "page": "pages/5_Data_Catalog.py",
        "body": "All repository outputs, file paths, row counts, schemas, and load examples.",
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
        "path": "05_fia/scripts/01_download_fia.R",
        "body": "Download USDA FIADB source files.",
        "page": "pages/3_FIA_Forest.py",
    },
    {
        "title": "Inspect FIA",
        "path": "05_fia/scripts/02_inspect_fia.R",
        "body": "Inspect FIADB schema and generate lookup tables.",
        "page": "pages/3_FIA_Forest.py",
    },
    {
        "title": "Extract FIA trees and conditions",
        "path": "05_fia/scripts/03_extract_trees.R",
        "body": "Extract TREE, COND, PLOT-related records and build basal-area inputs.",
        "page": "pages/3_FIA_Forest.py",
    },
    {
        "title": "Extract FIA seedlings and mortality",
        "path": "05_fia/scripts/04_extract_seedlings_mortality.R",
        "body": "Extract SEEDLING and TREE_GRM_COMPONENT mortality source records.",
        "page": "pages/3_FIA_Forest.py",
    },
    {
        "title": "Build FIA summaries",
        "path": "05_fia/scripts/05_build_fia_summaries.R",
        "body": "Build analysis-ready FIA summary parquets from extracted FIA source records.",
        "page": "pages/3_FIA_Forest.py",
    },
    {
        "title": "Extract FIA site climate",
        "path": "05_fia/scripts/06_extract_site_climate.R",
        "body": "Extract TerraClimate monthly values for FIA and ITRDB sites.",
        "page": "pages/3_FIA_Forest.py",
    },
    {
        "title": "Build recruitment CWM",
        "path": "07_thermophilization/scripts/01_build_plot_recruitment_cwm.R",
        "body": "Compute seedling community-weighted climate affinity per FIA condition visit.",
        "page": "pages/6_Thermophilization.py",
    },
    {
        "title": "Match disturbed controls",
        "path": "07_thermophilization/scripts/02_match_disturbed_to_controls.R",
        "body": "Match disturbed FIA conditions to clean controls by forest type, region, inventory year, and baseline climate.",
        "page": "pages/6_Thermophilization.py",
    },
    {
        "title": "Stratified thermophilization",
        "path": "07_thermophilization/scripts/03_stratified_thermophilization.R",
        "body": "Summarize recruitment thermophilization deltas by disturbance class, region, time, and high-severity proxy.",
        "page": "pages/6_Thermophilization.py",
    },
    {
        "title": "Thermophilization by class and time",
        "path": "07_thermophilization/scripts/04_thermophilization_by_class_time.R",
        "body": "Summarize deltas by disturbance class, region, and time since disturbance; writes disturbance-year coverage diagnostics.",
        "page": "pages/6_Thermophilization.py",
    },
]

FIA_GUIDE_INDEX_JSON = REPO_ROOT / "05_fia" / "docs" / "dashboard" / "fiadb_user_guide_index_v94.json"
FIA_NAVIGATOR_URL = "https://rellimylime.github.io/forest-data-compilation/fia-explorer.html"


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
        return "Start with `Rscript 05_fia/scripts/03_extract_trees.R`, then run `Rscript 05_fia/scripts/05_build_fia_summaries.R`."
    if table in {"SEEDLING", "TREE_GRM_COMPONENT"} or "MORT" in column:
        return "Start with `Rscript 05_fia/scripts/04_extract_seedlings_mortality.R`, then run `Rscript 05_fia/scripts/05_build_fia_summaries.R`."
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

    for section, label, rel_path, description in PIPELINE:
        if _matches(query, section, label, rel_path, description):
            exists = os.path.isfile(REPO_ROOT / rel_path)
            workflow_results.append(
                {
                    "kind": "Workflow output",
                    "title": label,
                    "body": description,
                    "meta": f"{section} · {'ready' if exists else 'not found'} · {rel_path}",
                    "page": PAGE_ROUTES.get(section, "pages/5_Data_Catalog.py"),
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
        st.markdown(
            f"[Open the FIA Forest Explorer]({FIA_NAVIGATOR_URL})"
        )

# ------------------------------------------------------------------------------
# Page
# ------------------------------------------------------------------------------

st.markdown(
    """
    <div class="fd-page-title">Forest Data Explorer</div>
    <div class="fd-page-lead">
      Interactive explorer for the forest-data-compilation repository. The pipeline brings
      together USDA Forest Service IDS aerial survey data, gridded climate products, and
      FIA forest inventory plots into a single navigable data workspace.
    </div>
    <div class="fd-callout">
      Start with <code>Architecture</code> for the workflow map, then open <code>Climate</code>
      for gridding and matching, <code>FIA Forest</code> for inventory outputs, or
      <code>Thermophilization</code> for the recruitment analysis layer. Use
      <code>Data Catalog</code> when you need exact paths, schemas, and load examples.
    </div>
    """,
    unsafe_allow_html=True,
)

# ── Workflow search ───────────────────────────────────────────────────────────
st.markdown(
    route_grid(
        [
            {
                "title": "Architecture",
                "body": "The calm map of how IDS, climate, FIA, and downstream analyses fit together.",
                "pills": ["start here", "workflow map"],
            },
            {
                "title": "Climate gridding",
                "body": "How raster pixels are matched to IDS polygons and FIA points, with reusable output schemas.",
                "pills": ["pixel_map", "IDS", "FIA"],
            },
            {
                "title": "FIA outputs",
                "body": "Tree, seedling, disturbance, treatment, mortality, damage-agent, and site-climate products.",
                "pills": ["PLT_CN", "INVYR", "CONDID"],
            },
            {
                "title": "Thermophilization",
                "body": "Species climate affinity, seedling CWM, matched controls, deltas, and species-level checks.",
                "pills": ["SPCD", "CWM", "delta"],
            },
            {
                "title": "Data Catalog",
                "body": "Exact output paths, current file status, row counts, schemas, and load snippets.",
                "pills": ["paths", "schemas"],
            },
        ]
    ),
    unsafe_allow_html=True,
)

st.markdown('<div class="fd-section-label">Workflow search</div>', unsafe_allow_html=True)
search_cols = st.columns([2.3, 1])
workflow_query = search_cols[0].text_input(
    "Search outputs, pages, scripts, FIA tables, and FIA variables",
    placeholder="Try plot_tree_metrics, SPCD, CWM, thermophilization, TerraClimate, COND, damage agents",
    key="workflow_search_query",
)
search_cols[1].markdown(
    """
    <div class="fd-card">
      <div class="fd-card-title">How results route</div>
      <div class="fd-card-body">
        Dashboard outputs open the matching page. FIA source fields that are not dashboard outputs point to extraction code and the FIA navigator.
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

if workflow_query.strip():
    workflow_results, fia_source_results = search_workflow(workflow_query)
    total_results = len(workflow_results) + len(fia_source_results)
    r1, r2, r3 = st.columns(3)
    r1.metric("Matches", total_results)
    r2.metric("In workflow", len(workflow_results))
    r3.metric("FIA source / unextracted", len(fia_source_results))

    if total_results:
        result_tabs = st.tabs(["Workflow", "FIA source / unextracted"])
        with result_tabs[0]:
            if workflow_results:
                for i, result in enumerate(workflow_results):
                    render_search_result(result, f"workflow_result_{i}")
            else:
                st.info("No dashboard output/page/script matches. Check the FIA source tab for raw FIADB matches.")
        with result_tabs[1]:
            if fia_source_results:
                st.info(
                    "These look like source FIADB tables or variables. They may not be extracted into a current workflow output yet."
                )
                for i, result in enumerate(fia_source_results):
                    render_search_result(result, f"fia_source_result_{i}")
            else:
                st.info("No raw FIADB table or variable matches.")
    else:
        st.info("No matches yet. Try an output name, script name, FIA table, or FIA variable such as `SPCD`.")

st.markdown("---")

# ── Data inventory ────────────────────────────────────────────────────────────
st.markdown('<div class="fd-section-label">Pipeline status</div>', unsafe_allow_html=True)
st.caption(
    "File existence check across all expected outputs. "
    "For parquets, row counts are read from file metadata (instant, no data loaded)."
)

rows = []
for section, label, rel_path, description in PIPELINE:
    full_path = str(REPO_ROOT / rel_path)
    exists = os.path.isfile(full_path)
    status = "✅" if exists else "❌"

    if exists and rel_path.endswith(".parquet"):
        meta = parquet_meta(full_path)
        rows_val = f"{meta['rows']:,}" if meta.get("rows") else "—"
        size_val = f"{meta['size_mb']:.0f} MB" if meta.get("size_mb") else "—"
    elif exists:
        size_val = f"{os.path.getsize(full_path) / 1e6:.0f} MB"
        rows_val = "—"
    else:
        rows_val = "—"
        size_val = "—"

    rows.append({
        "Section":     section,
        "Output":      label,
        "Status":      status,
        "Size":        size_val,
        "Rows":        rows_val,
        "Description": description,
    })

inv_df = pd.DataFrame(rows)

# Summary counts by section
st.markdown("#### By section")
section_order = ["IDS", "TerraClimate", "PRISM", "WorldClim", "FIA", "Thermophilization"]
for sec in section_order:
    sec_df = inv_df[inv_df["Section"] == sec]
    n_ok = (sec_df["Status"] == "✅").sum()
    n_total = len(sec_df)
    pct = 100 * n_ok / n_total if n_total else 0
    st.markdown(
        f"""
        <div class="fd-pipeline-row">
          <div class="fd-pipeline-head">
            <span class="fd-pipeline-name">{sec}</span>
            <span class="fd-pipeline-count">{n_ok}/{n_total} ready</span>
          </div>
          <div class="fd-progress">
            <div class="fd-progress-fill" style="width:{pct:.1f}%"></div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown("#### All outputs")
st.dataframe(
    inv_df.style.map(color_status, subset=["Status"]),
    use_container_width=True,
    hide_index=True,
)

st.markdown("---")

# ── QA / QC ───────────────────────────────────────────────────────────────────
st.markdown('<div class="fd-section-label">QA / QC</div>', unsafe_allow_html=True)
st.markdown(
    "The repository includes a `testthat`-based suite that validates output schemas, "
    "value ranges, key uniqueness, water year arithmetic, and pixel map correctness "
    "across all five pipeline modules. See [`docs/TESTING.md`](docs/TESTING.md) for "
    "what each test suite checks."
)
st.code(
    "# Non-strict: skip missing outputs (good during development)\n"
    "Rscript scripts/run_tests.R\n\n"
    "# Strict: fail on missing outputs (use for release QA)\n"
    "Rscript scripts/run_tests.R --strict\n\n"
    "# Single module\n"
    "Rscript scripts/run_tests.R 05_fia",
    language="bash",
)

st.markdown("---")

# ── Quick corpus stats ────────────────────────────────────────────────────────
st.markdown('<div class="fd-section-label">Quick stats</div>', unsafe_allow_html=True)
c1, c2, c3, c4, c5 = st.columns(5)

ids_path = str(repo_path("01_ids", "data", "processed", "ids_layers_cleaned.gpkg"))
c1.markdown(
    metric_card("IDS Damage Areas", "4.4M", "1997–2024, 10 FS regions"),
    unsafe_allow_html=True,
)
c2.markdown(
    metric_card("Climate Datasets", "3", "TerraClimate · PRISM · WorldClim"),
    unsafe_allow_html=True,
)
c3.markdown(
    metric_card("Climate Variables", "24", "14 TC · 7 PRISM · 3 WC"),
    unsafe_allow_html=True,
)

fia_tree_path = str(repo_path("05_fia", "data", "processed", "summaries", "plot_tree_metrics.parquet"))
if os.path.isfile(fia_tree_path):
    m = parquet_meta(fia_tree_path)
    c4.markdown(
        metric_card("FIA Plot Visits", f"{m['rows']:,}" if m.get("rows") else "—",
                    "PLT_CN × INVYR rows"),
        unsafe_allow_html=True,
    )
else:
    c4.markdown(metric_card("FIA Plot Visits", "—", "run pipeline first"), unsafe_allow_html=True)

fia_clim_path = str(repo_path("05_fia", "data", "processed", "site_climate", "site_climate.parquet"))
if os.path.isfile(fia_clim_path):
    m2 = parquet_meta(fia_clim_path)
    c5.markdown(
        metric_card("FIA Site Climate Rows", f"{m2['rows']:,}" if m2.get("rows") else "—",
                    "6,956 sites · 1958–2024"),
        unsafe_allow_html=True,
    )
else:
    c5.markdown(metric_card("FIA Site Climate", "—", "run 06_extract_site_climate.R"), unsafe_allow_html=True)

st.markdown("---")

# ── Demo outputs ──────────────────────────────────────────────────────────────
st.markdown('<div class="fd-section-label">Demo outputs</div>', unsafe_allow_html=True)
st.caption(
    "Figures generated by the demo scripts. Run the scripts first — see README for instructions."
)

d_tab1, d_tab2, d_tab3, d_tab4 = st.tabs([
    "IDS + Climate (Demo 01)",
    "Cross-Dataset Comparison",
    "FIA Forest (Demo 02)",
    "Site Climate (Demo 03)",
])

# Demo 01 — support both new (demo_01_ids_climate.R) and old deprecated output paths
_demo01_new = REPO_ROOT / "output" / "demo_01_ids_climate_terraclimate"
_demo01_old = REPO_ROOT / "output" / "demo_mpb_terraclimate"
_demo01_dir = _demo01_new if _demo01_new.is_dir() else (_demo01_old if _demo01_old.is_dir() else None)
_demo01_source_lines = {
    "01_outbreak_timeline.png": 163,
    "02_climate_timeseries.png": 187,
    "03_outbreak_vs_climate.png": 214,
}

with d_tab1:
    st.markdown(
        "[`scripts/demos/demo_01_ids_climate.R`](scripts/demos/demo_01_ids_climate.R) — "
        "MPB outbreak severity vs. water-year climate. Accepts `terraclimate` (default), `prism`, or `worldclim`."
    )
    if _demo01_dir is not None:
        for fname, cap in [
            ("01_outbreak_timeline.png",   "MPB damage extent over time (1997–2024)"),
            ("02_climate_timeseries.png",  "Water-year temperature and precipitation at MPB damage sites"),
            ("03_outbreak_vs_climate.png", "Outbreak severity vs. each climate variable (linear fit)"),
        ]:
            p = _demo01_dir / fname
            if p.exists():
                st.image(str(p), caption=cap, use_container_width=True)
                plot_source_link("scripts/demos/demo_01_ids_climate.R", line=_demo01_source_lines.get(fname))
    else:
        st.info("Run `Rscript scripts/demos/demo_01_ids_climate.R` to generate figures.")

with d_tab2:
    st.markdown(
        "[`scripts/demos/demo_04_compare_climate_datasets.R`](scripts/demos/demo_04_compare_climate_datasets.R) — "
        "TerraClimate, PRISM, and WorldClim plotted on the same axes for direct comparison. "
        "Run all three `demo_01` variants first."
    )
    _compare_dir = REPO_ROOT / "output" / "demo_mpb_comparison"
    _compare_source_lines = {
        "01_climate_comparison.png": 57,
        "02_outbreak_vs_climate_comparison.png": 88,
    }
    if _compare_dir.is_dir():
        for fname, cap in [
            ("01_climate_comparison.png",             "Water-year climate at MPB sites — all three datasets"),
            ("02_outbreak_vs_climate_comparison.png", "Outbreak severity vs. climate — all three datasets"),
        ]:
            p = _compare_dir / fname
            if p.exists():
                st.image(str(p), caption=cap, use_container_width=True)
                plot_source_link(
                    "scripts/demos/demo_04_compare_climate_datasets.R",
                    line=_compare_source_lines.get(fname),
                )
    else:
        st.info(
            "Run all three demo_01 variants, then: "
            "`Rscript scripts/demos/demo_04_compare_climate_datasets.R`"
        )

with d_tab3:
    st.markdown(
        "[`scripts/demos/demo_02_fia_forest.R`](scripts/demos/demo_02_fia_forest.R) — "
        "FIA exclusion flags, tree metrics, disturbance history, damage agents, treatments, seedlings, mortality."
    )
    _demo02_dir = REPO_ROOT / "output" / "demo_02_fia_forest"
    _demo02_source_lines = {
        "01_ba_annual.png": 138,
        "02_diversity_dist.png": 154,
        "03_disturbance_annual.png": 223,
    }
    if _demo02_dir.is_dir():
        for p in sorted(_demo02_dir.glob("*.png")):
            st.image(str(p), caption=p.stem.replace("_", " "), use_container_width=True)
            plot_source_link("scripts/demos/demo_02_fia_forest.R", line=_demo02_source_lines.get(p.name))
    else:
        st.info("Run `Rscript scripts/demos/demo_02_fia_forest.R` to generate figures.")

with d_tab4:
    st.markdown(
        "[`scripts/demos/demo_03_site_climate.R`](scripts/demos/demo_03_site_climate.R) — "
        "Monthly TerraClimate at 6,956 FIA sites — CWD trends, summer temperatures, long-term climatology."
    )
    _demo03_dir = REPO_ROOT / "output" / "demo_03_site_climate"
    _demo03_source_lines = {
        "01_cwd_timeseries.png": 159,
        "02_summer_tmax.png": 185,
        "03_climate_space.png": 202,
    }
    if _demo03_dir.is_dir():
        for p in sorted(_demo03_dir.glob("*.png")):
            st.image(str(p), caption=p.stem.replace("_", " "), use_container_width=True)
            plot_source_link("scripts/demos/demo_03_site_climate.R", line=_demo03_source_lines.get(p.name))
    else:
        st.info("Run `Rscript scripts/demos/demo_03_site_climate.R` to generate figures.")

st.markdown("---")
st.caption(
    f"Repo: `{REPO_ROOT}` &nbsp;|&nbsp; "
    "Run: `streamlit run docs/dashboard/app.py` from the repo root"
)
