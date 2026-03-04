# ==============================================================================
# pages/2_Climate.py
# Climate Datasets explorer — TerraClimate, PRISM, WorldClim
# ==============================================================================

import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import (
    apply_dark_css, metric_card, dark_fig, parquet_meta,
    load_parquet, repo_path,
)

st.set_page_config(page_title="Climate Data", page_icon="🌡️", layout="wide")
apply_dark_css()

try:
    import plotly.express as px
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

st.title("🌡️ Climate Datasets")
st.markdown(
    "Three gridded climate datasets are extracted for every IDS damage area using the "
    "**pixel decomposition** pattern — see the Architecture page for how it works."
)

# ------------------------------------------------------------------------------
# Variable catalogs (from config.yaml)
# ------------------------------------------------------------------------------

TC_VARS = [
    ("tmmx",  "Maximum temperature",                           "°C",      0.1),
    ("tmmn",  "Minimum temperature",                           "°C",      0.1),
    ("pr",    "Precipitation accumulation",                    "mm",      1.0),
    ("srad",  "Downward surface shortwave radiation",          "W/m²",    0.1),
    ("vs",    "Wind speed at 10m",                             "m/s",     0.01),
    ("vap",   "Vapor pressure",                                "kPa",     0.001),
    ("vpd",   "Vapor pressure deficit",                        "kPa",     0.01),
    ("pet",   "Reference evapotranspiration (Penman-Monteith)","mm",      0.1),
    ("aet",   "Actual evapotranspiration",                     "mm",      0.1),
    ("def",   "Climate water deficit",                         "mm",      0.1),
    ("soil",  "Soil moisture",                                 "mm",      0.1),
    ("swe",   "Snow water equivalent",                         "mm",      1.0),
    ("ro",    "Runoff",                                        "mm",      1.0),
    ("pdsi",  "Palmer Drought Severity Index",                 "unitless",0.01),
]

PRISM_VARS = [
    ("ppt",    "Total precipitation",           "mm",  1.0),
    ("tmean",  "Mean temperature",              "°C",  1.0),
    ("tmin",   "Minimum temperature",           "°C",  1.0),
    ("tmax",   "Maximum temperature",           "°C",  1.0),
    ("tdmean", "Mean dew point temperature",    "°C",  1.0),
    ("vpdmin", "Minimum vapor pressure deficit","hPa", 1.0),
    ("vpdmax", "Maximum vapor pressure deficit","hPa", 1.0),
]

WC_VARS = [
    ("tmin", "Minimum temperature", "°C", 1.0),
    ("tmax", "Maximum temperature", "°C", 1.0),
    ("prec", "Precipitation",       "mm", 1.0),
]

# ------------------------------------------------------------------------------
# File inventory helpers
# ------------------------------------------------------------------------------

def _tc_summary_files():
    base = repo_path("processed", "climate", "terraclimate", "damage_areas_summaries")
    return {v: base / f"{v}.parquet" for v, *_ in TC_VARS}

def _prism_summary_files():
    base = repo_path("processed", "climate", "prism", "damage_areas_summaries")
    return {v: base / f"{v}.parquet" for v, *_ in PRISM_VARS}

def _wc_summary_files():
    base = repo_path("processed", "climate", "worldclim", "damage_areas_summaries")
    return {v: base / f"{v}.parquet" for v, *_ in WC_VARS}

def file_inventory_table(var_list, file_dict) -> pd.DataFrame:
    rows = []
    for var, desc, unit, scale in var_list:
        p = file_dict.get(var)
        exists = p is not None and p.is_file()
        if exists:
            m = parquet_meta(str(p))
            size = f"{m['size_mb']:.0f} MB" if m.get("size_mb") else "—"
            nrow = f"{m['rows']:,}" if m.get("rows") else "—"
        else:
            size, nrow = "—", "—"
        rows.append({
            "Variable":    var,
            "Description": desc,
            "Units":       unit,
            "Scale":       scale,
            "Status":      "✅" if exists else "❌",
            "Size":        size,
            "Rows":        nrow,
        })
    return pd.DataFrame(rows)

# ------------------------------------------------------------------------------
# Shared summary schema
# ------------------------------------------------------------------------------

SUMMARY_SCHEMA = [
    ("OBSERVATION_ID",        "str",    "Links back to source IDS observation"),
    ("DAMAGE_AREA_ID",        "large_string", "Links to damage_areas layer in GeoPackage"),
    ("calendar_year",         "int",    "Calendar year of the monthly record"),
    ("calendar_month",        "int",    "Calendar month (1–12)"),
    ("water_year",            "int",    "Water year (Oct–Sep; month ≥ 10 → yr+1)"),
    ("water_year_month",      "int",    "Month position within the water year (1=Oct … 12=Sep)"),
    ("variable",              "str",    "Climate variable name (e.g. tmmx, pr)"),
    ("weighted_mean",         "float",  "Area-weighted mean across overlapping pixels"),
    ("value_min",             "float",  "Minimum pixel value within the damage area"),
    ("value_max",             "float",  "Maximum pixel value within the damage area"),
    ("n_pixels",              "int",    "Total pixels overlapping the damage area"),
    ("n_pixels_with_data",    "int",    "Pixels with non-null data"),
    ("sum_coverage_fraction", "float",  "Sum of pixel coverage fractions (≈ 1.0 for full coverage)"),
]

# ==============================================================================
# Sub-tabs: TerraClimate | PRISM | WorldClim
# ==============================================================================

tc_tab, prism_tab, wc_tab, grid_tab = st.tabs([
    "🌐 TerraClimate",
    "🇺🇸 PRISM",
    "🌍 WorldClim",
    "🔲 Pixel Grid",
])

# ==============================================================================
# TERRACLIMATE
# ==============================================================================
with tc_tab:
    st.subheader("TerraClimate")
    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(metric_card("Resolution", "~4 km", "1/24th degree"), unsafe_allow_html=True)
    c2.markdown(metric_card("Coverage", "Global", "land areas"), unsafe_allow_html=True)
    c3.markdown(metric_card("Period", "1958–2024", "monthly"), unsafe_allow_html=True)
    c4.markdown(metric_card("Variables", "14", "temperature · water · radiation"), unsafe_allow_html=True)

    st.markdown(
        "**Source:** [Climatology Lab / IDAHO_EPSCOR/TERRACLIMATE](https://www.climatologylab.org/terraclimate.html)  \n"
        "**Citation:** Abatzoglou et al. 2018, *Scientific Data*  \n"
        "**Access:** Google Earth Engine (`IDAHO_EPSCOR/TERRACLIMATE`)"
    )

    st.markdown("---")
    st.subheader("Variable Catalog")
    vc_df = pd.DataFrame(TC_VARS, columns=["Variable", "Description", "Units", "GEE Scale"])
    st.dataframe(vc_df, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.subheader("Output File Inventory")
    st.caption(
        "One ~10–13 GB parquet per variable in "
        "`processed/climate/terraclimate/damage_areas_summaries/`. "
        "Sizes below are read from file metadata — no data loaded."
    )
    inv = file_inventory_table(TC_VARS, _tc_summary_files())
    from utils import color_status
    st.dataframe(
        inv.style.applymap(color_status, subset=["Status"]),
        use_container_width=True, hide_index=True,
    )

    # Pixel map stats
    pm_path = str(repo_path("02_terraclimate", "data", "processed",
                             "pixel_maps", "damage_areas_pixel_map.parquet"))
    st.markdown("---")
    st.subheader("Pixel Map")
    if os.path.isfile(pm_path):
        m = parquet_meta(pm_path)
        st.markdown(
            f"✅ `02_terraclimate/data/processed/pixel_maps/damage_areas_pixel_map.parquet`  \n"
            f"{m.get('rows', 0):,} rows · {m.get('size_mb', 0):.1f} MB  \n"
            f"Columns: {', '.join(f'`{c}`' for c in m.get('columns', []))}"
        )
    else:
        st.warning("Pixel map not found. Run `02_terraclimate/scripts/02_build_pixel_maps.R`.")

    st.markdown("---")
    st.subheader("Load in R")
    st.code(
        'library(arrow); library(dplyr)\n'
        '\n'
        '# Open the full 10-13 GB parquet lazily (no data loaded yet)\n'
        'tmmx <- open_dataset("processed/climate/terraclimate/damage_areas_summaries/tmmx.parquet")\n'
        '\n'
        '# Filter to MPB (DCA 11006) damage areas, summer months, 2010-2020\n'
        'mpb_tmmx <- tmmx |>\n'
        '  filter(calendar_month %in% 6:8, calendar_year %in% 2010:2020) |>\n'
        '  collect()',
        language="r",
    )

# ==============================================================================
# PRISM
# ==============================================================================
with prism_tab:
    st.subheader("PRISM")
    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(metric_card("Resolution", "800 m", "~30 arc-seconds"), unsafe_allow_html=True)
    c2.markdown(metric_card("Coverage", "CONUS", "excludes AK, HI"), unsafe_allow_html=True)
    c3.markdown(metric_card("Period", "1997–2024", "monthly"), unsafe_allow_html=True)
    c4.markdown(metric_card("Variables", "7", "temperature · precipitation · VPD"), unsafe_allow_html=True)

    st.markdown(
        "**Source:** [PRISM Climate Group, Oregon State University](https://prism.oregonstate.edu/)  \n"
        "**Product:** AN81m (monthly 800m normals)  \n"
        "**Access:** Direct web service (`services.nacse.org`)  \n"
        "**Note:** CONUS-only — AK and HI damage areas have no PRISM values."
    )

    st.markdown("---")
    st.subheader("Variable Catalog")
    vc_df = pd.DataFrame(PRISM_VARS, columns=["Variable", "Description", "Units", "Scale"])
    st.dataframe(vc_df, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.subheader("Output File Inventory")
    st.caption("One ~19–23 GB parquet per variable in `processed/climate/prism/damage_areas_summaries/`.")
    inv = file_inventory_table(PRISM_VARS, _prism_summary_files())
    st.dataframe(
        inv.style.applymap(color_status, subset=["Status"]),
        use_container_width=True, hide_index=True,
    )

    pm_path = str(repo_path("03_prism", "data", "processed",
                             "pixel_maps", "damage_areas_pixel_map.parquet"))
    st.markdown("---")
    st.subheader("Pixel Map")
    if os.path.isfile(pm_path):
        m = parquet_meta(pm_path)
        st.markdown(
            f"✅ `03_prism/data/processed/pixel_maps/damage_areas_pixel_map.parquet`  \n"
            f"{m.get('rows', 0):,} rows · {m.get('size_mb', 0):.1f} MB  \n"
            f"Columns: {', '.join(f'`{c}`' for c in m.get('columns', []))}"
        )
    else:
        st.warning("Pixel map not found. Run `03_prism/scripts/01_build_pixel_maps.R`.")

# ==============================================================================
# WORLDCLIM
# ==============================================================================
with wc_tab:
    st.subheader("WorldClim")
    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(metric_card("Resolution", "~4.5 km", "2.5 arc-minutes"), unsafe_allow_html=True)
    c2.markdown(metric_card("Coverage", "Global", "land areas"), unsafe_allow_html=True)
    c3.markdown(metric_card("Period", "1950–2024", "monthly"), unsafe_allow_html=True)
    c4.markdown(metric_card("Variables", "3", "tmin · tmax · prec"), unsafe_allow_html=True)

    st.markdown(
        "**Source:** [WorldClim v2.1 historical monthly weather](https://www.worldclim.org/data/monthlywth.html)  \n"
        "**Citation:** Fick & Hijmans 2017, *International Journal of Climatology*  \n"
        "**Access:** Bulk GeoTIFF download by decade (~600 MB per variable per decade)"
    )

    st.markdown("---")
    st.subheader("Variable Catalog")
    vc_df = pd.DataFrame(WC_VARS, columns=["Variable", "Description", "Units", "Scale"])
    st.dataframe(vc_df, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.subheader("Output File Inventory")
    st.caption("One ~9–13 GB parquet per variable in `processed/climate/worldclim/damage_areas_summaries/`.")
    inv = file_inventory_table(WC_VARS, _wc_summary_files())
    st.dataframe(
        inv.style.applymap(color_status, subset=["Status"]),
        use_container_width=True, hide_index=True,
    )

    pm_path = str(repo_path("04_worldclim", "data", "processed",
                             "pixel_maps", "damage_areas_pixel_map.parquet"))
    st.markdown("---")
    st.subheader("Pixel Map")
    if os.path.isfile(pm_path):
        m = parquet_meta(pm_path)
        st.markdown(
            f"✅ `04_worldclim/data/processed/pixel_maps/damage_areas_pixel_map.parquet`  \n"
            f"{m.get('rows', 0):,} rows · {m.get('size_mb', 0):.1f} MB  \n"
            f"Columns: {', '.join(f'`{c}`' for c in m.get('columns', []))}"
        )
    else:
        st.warning("Pixel map not found. Run `04_worldclim/scripts/02_build_pixel_maps.R`.")

# ==============================================================================
# PIXEL GRID VISUALIZATION
# ==============================================================================
with grid_tab:
    st.subheader("Pixel Grid Visualization")
    st.markdown(
        "Each dataset decomposes IDS damage areas (polygons) or FIA sites (points) into the "
        "underlying raster pixels they overlap. This tab visualises pixel centroids to show "
        "what the grid structure looks like on a map."
    )

    # Dataset selector
    grid_dataset = st.radio(
        "Show pixel map for",
        ["FIA sites (TerraClimate 4km, 6,956 points — loads instantly)",
         "IDS damage areas — TerraClimate (sampled 30k)",
         "IDS damage areas — PRISM (sampled 30k)",
         "IDS damage areas — WorldClim (sampled 30k)"],
        key="grid_dataset_sel",
    )

    if grid_dataset.startswith("FIA"):
        pm_path = str(repo_path("05_fia", "data", "processed", "site_climate",
                                 "fia_site_pixel_map.parquet"))
        color_seq = ["#4e79a7"]
        title_suffix = "FIA site pixel centroids (TerraClimate 4km grid)"
        sample_n = None
    elif "TerraClimate" in grid_dataset:
        pm_path = str(repo_path("02_terraclimate", "data", "processed",
                                 "pixel_maps", "damage_areas_pixel_map.parquet"))
        color_seq = ["#e15759"]
        title_suffix = "IDS × TerraClimate pixel centroids (4km grid)"
        sample_n = 30_000
    elif "PRISM" in grid_dataset:
        pm_path = str(repo_path("03_prism", "data", "processed",
                                 "pixel_maps", "damage_areas_pixel_map.parquet"))
        color_seq = ["#59a14f"]
        title_suffix = "IDS × PRISM pixel centroids (800m grid)"
        sample_n = 30_000
    else:
        pm_path = str(repo_path("04_worldclim", "data", "processed",
                                 "pixel_maps", "damage_areas_pixel_map.parquet"))
        color_seq = ["#f28e2b"]
        title_suffix = "IDS × WorldClim pixel centroids (4.5km grid)"
        sample_n = 30_000

    if not os.path.isfile(pm_path):
        st.info(f"File not found: `{pm_path}`")
    elif not PLOTLY_AVAILABLE:
        st.warning("Install `plotly` to view the pixel grid map.")
    else:
        pm_df, pm_err = load_parquet(pm_path)
        if pm_err:
            st.error(pm_err)
        elif pm_df is not None:
            # Get unique pixels
            x_col = "x" if "x" in pm_df.columns else None
            y_col = "y" if "y" in pm_df.columns else None
            pid_col = "pixel_id" if "pixel_id" in pm_df.columns else None

            if not (x_col and y_col):
                st.warning(f"Expected `x`, `y` columns. Found: {pm_df.columns.tolist()}")
            else:
                if pid_col:
                    unique_px = pm_df[[pid_col, x_col, y_col]].drop_duplicates(pid_col)
                else:
                    unique_px = pm_df[[x_col, y_col]].drop_duplicates()

                n_total = len(unique_px)
                if sample_n and n_total > sample_n:
                    unique_px = unique_px.sample(sample_n, random_state=42)

                st.markdown(
                    f"**{n_total:,} unique pixels** · "
                    f"showing {len(unique_px):,} on map"
                )

                # scatter_map is the Plotly 5.17+ / 6.x API (scatter_mapbox removed in 6.0)
                try:
                    fig = px.scatter_map(
                        unique_px,
                        lat=y_col, lon=x_col,
                        color_discrete_sequence=color_seq,
                        map_style="open-street-map",
                        zoom=3, center={"lat": 44, "lon": -105},
                        opacity=0.6,
                        title=title_suffix,
                    )
                except AttributeError:
                    fig = px.scatter_mapbox(
                        unique_px,
                        lat=y_col, lon=x_col,
                        color_discrete_sequence=color_seq,
                        mapbox_style="open-street-map",
                        zoom=3, center={"lat": 44, "lon": -105},
                        opacity=0.6,
                        title=title_suffix,
                    )
                fig.update_traces(marker_size=4)
                fig.update_layout(
                    paper_bgcolor="#0e1117", font_color="#ddd",
                    margin=dict(l=0, r=0, t=30, b=0),
                )
                st.plotly_chart(fig, use_container_width=True)

                st.caption(
                    "Each dot is one unique raster pixel. "
                    "The grid pattern reflects the underlying dataset resolution: "
                    "~4km (TerraClimate / WorldClim) or ~800m (PRISM). "
                    "FIA sites use the same TerraClimate grid but for point locations "
                    "rather than area decomposition — see the Architecture page."
                )

    st.markdown("---")
    st.subheader("Summary Schema")
    st.markdown(
        "All three datasets produce summary parquets with the same schema — "
        "one row per **damage area × month** with area-weighted climate values."
    )
    schema_df = pd.DataFrame(SUMMARY_SCHEMA, columns=["Column", "Type", "Description"])
    st.dataframe(schema_df, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.subheader("Dataset Comparison")
    comp_df = pd.DataFrame([
        ["TerraClimate", "GEE (IDAHO_EPSCOR/TERRACLIMATE)", "~4 km", "1958–2024", "14", "Global", "~140 GB"],
        ["PRISM",        "Web service (nacse.org)",          "800 m",  "1997–2024", "7",  "CONUS",  "~135 GB"],
        ["WorldClim",    "Direct download (GeoTIFF)",        "~4.5 km","1950–2024", "3",  "Global", "~31 GB"],
    ], columns=["Dataset", "Access", "Resolution", "Period", "Variables", "Coverage", "Summary Size"])
    st.dataframe(comp_df, use_container_width=True, hide_index=True)
