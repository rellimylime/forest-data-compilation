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
from pathlib import Path

import pandas as pd
import streamlit as st

from utils import (
    REPO_ROOT, apply_dark_css, metric_card, parquet_meta,
    file_status, repo_path, color_status, PLOTLY_AVAILABLE
)

st.set_page_config(
    page_title="Forest Data Explorer",
    page_icon="🌲",
    layout="wide",
    initial_sidebar_state="expanded",
)
apply_dark_css()

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
    ("FIA", "FIA site pixel map",
     "05_fia/data/processed/site_climate/fia_site_pixel_map.parquet",
     "TerraClimate 4km pixel assignments for 6,956 FIA plot locations"),
    ("FIA", "FIA site climate",
     "05_fia/data/processed/site_climate/fia_site_climate.parquet",
     "Monthly TerraClimate at FIA sites: 6 variables, 1958–2024 (23.5M rows)"),
]

# ------------------------------------------------------------------------------
# Page
# ------------------------------------------------------------------------------

st.title("🌲 Forest Data Compilation — Dashboard")
st.markdown(
    "Interactive explorer for the **forest-data-compilation** repository. "
    "This pipeline integrates USDA Forest Service [IDS aerial survey data](01_ids/) "
    "with three gridded climate datasets and FIA forest inventory plots. "
    "Use the **sidebar** to navigate to each section."
)

st.markdown("---")

# ── Data inventory ────────────────────────────────────────────────────────────
st.subheader("Pipeline Status")
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
section_order = ["IDS", "TerraClimate", "PRISM", "WorldClim", "FIA"]
for sec in section_order:
    sec_df = inv_df[inv_df["Section"] == sec]
    n_ok = (sec_df["Status"] == "✅").sum()
    n_total = len(sec_df)
    pct = 100 * n_ok / n_total if n_total else 0
    bar = "█" * n_ok + "░" * (n_total - n_ok)
    st.markdown(
        f"**{sec}** &nbsp; `{bar}` &nbsp; {n_ok}/{n_total} outputs ready",
        unsafe_allow_html=True,
    )

st.markdown("#### All outputs")
st.dataframe(
    inv_df.style.map(color_status, subset=["Status"]),
    use_container_width=True,
    hide_index=True,
)

st.markdown("---")

# ── Quick corpus stats ────────────────────────────────────────────────────────
st.subheader("Quick Stats")
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

fia_clim_path = str(repo_path("05_fia", "data", "processed", "site_climate", "fia_site_climate.parquet"))
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

# ── Navigation guide ──────────────────────────────────────────────────────────
st.subheader("Navigation")
st.markdown(
    "| Page | What it covers |\n"
    "|------|----------------|\n"
    "| 🗺️ **IDS Survey** | Damage areas, damage points, surveyed areas; DCA/host code lookups; coverage map |\n"
    "| 🌡️ **Climate** | TerraClimate, PRISM, WorldClim — variable catalogs, pixel grid visualization, schema explorer |\n"
    "| 🌲 **FIA Forest** | All FIA summary parquets — tree metrics, plot filters, disturbance, damage agents, mortality, seedlings, site climate |\n"
    "| 🔗 **Architecture** | Pixel decomposition workflow — how IDS polygon extraction and FIA point extraction share the same pattern |\n"
    "| 📋 **Data Catalog** | Every output file — path, size, schema, R + Python load code snippets |\n"
)

st.markdown("---")
st.caption(
    f"Repo: `{REPO_ROOT}` &nbsp;|&nbsp; "
    "Run: `streamlit run docs/dashboard/app.py` from the repo root"
)
