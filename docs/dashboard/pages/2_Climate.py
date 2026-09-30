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
    page_intro,
    apply_dark_css, metric_card, dark_fig, parquet_meta,
    load_parquet, repo_path, plot_source_link,
    route_grid, workflow_grid,
)

st.set_page_config(page_title="Climate datasets", page_icon="🌡️", layout="wide")
apply_dark_css()

try:
    import plotly.express as px
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

st.title("🌡️ Climate Datasets")
st.markdown(
    "Three gridded climate datasets are extracted for every IDS damage area using the "
    "**pixel decomposition** pattern — see the Repository map page for how it works."
)
page_intro(
    "TerraClimate, PRISM, and WorldClim values extracted at IDS survey locations. The current "
    "analysis does not use these outputs; its separate cumulative-CWD cache is documented "
    "on the Analysis page.",
    "other",
    [("pages/6_Analysis.py", "Analysis"), ("pages/1_IDS_Survey.py", "IDS survey"),
     ("pages/4_Architecture.py", "Repository map")],
)

st.markdown(
    route_grid(
        [
            {
                "title": "IDS polygons",
                "body": "Damage areas are decomposed into every overlapping raster pixel and summarized with coverage fractions.",
                "pills": ["DAMAGE_AREA_ID", "coverage_fraction"],
            },
            {
                "title": "Shared summary pattern",
                "body": "Climate values are extracted once per unique pixel and joined back to observation keys only when summaries are built.",
                "pills": ["pixel values", "summaries"],
            },
        ]
    ),
    unsafe_allow_html=True,
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

workflow_tab, match_tab, tc_tab, prism_tab, wc_tab, grid_tab = st.tabs([
    "Workflow",
    "Matching Examples",
    "🌐 TerraClimate",
    "🇺🇸 PRISM",
    "🌍 WorldClim",
    "🔲 Pixel Grid",
])

# ==============================================================================
# WORKFLOW GUIDE
# ==============================================================================
with workflow_tab:
    st.subheader("Grid first, summarize second")
    st.markdown(
        "The climate layer is deliberately split into small reusable tables. A pixel map "
        "records the relationship between an observation and the raster grid; pixel-value "
        "files hold climate histories for unique pixels; summary tables join the two only "
        "when an analysis needs observation-level climate."
    )
    st.markdown(
        workflow_grid(
            [
                {
                    "label": "1",
                    "title": "Choose observation geometry",
                    "body": "IDS uses damage polygons and damage-point observations.",
                },
                {
                    "label": "2",
                    "title": "Build a pixel map",
                    "body": "Polygons keep coverage fractions; damage points receive the containing climate pixel.",
                },
                {
                    "label": "3",
                    "title": "Extract unique pixels",
                    "body": "Climate values are pulled once per pixel-month instead of once per observation-month.",
                },
                {
                    "label": "4",
                    "title": "Join and summarize",
                    "body": "IDS polygons get area-weighted summaries; damage points use direct pixel values where enabled.",
                },
            ]
        ),
        unsafe_allow_html=True,
    )

    st.markdown("#### Two matching modes")
    st.markdown(
        "| Target | Geometry | Pixel relationship | Final output |\n"
        "|---|---|---|---|\n"
        "| IDS damage areas | Polygon | Many pixels with `coverage_fraction` weights | `processed/climate/<dataset>/damage_areas_summaries/<variable>.parquet` |\n"
        "| IDS damage points | Point | One containing pixel | point pixel maps / values where enabled |\n"
    )

    st.info(
        "The same idea can be reused for any new dataset: create stable observation IDs, "
        "build an observation-to-pixel map for the chosen climate grid, extract unique "
        "pixels, then join by `pixel_id`."
    )


# ==============================================================================
# MATCHING EXAMPLES
# ==============================================================================
with match_tab:
    st.subheader("How to connect climate to other repo outputs")
    ids_col = st.container()
    with ids_col:
        st.markdown("#### IDS example: climate at damage polygons")
        st.markdown(
            "Use `DAMAGE_AREA_ID` to move from the cleaned IDS layer to the per-variable "
            "climate summary. The summary already contains the area-weighted climate value "
            "for each polygon-month."
        )
        st.code(
            'library(sf)\n'
            'library(arrow)\n'
            'library(dplyr)\n\n'
            'damage <- st_read(\n'
            '  "01_ids/data/processed/ids_layers_cleaned.gpkg",\n'
            '  layer = "damage_areas",\n'
            '  query = "SELECT DAMAGE_AREA_ID, DCA_CODE, HOST_CODE, YEAR FROM damage_areas WHERE DCA_CODE = 11006"\n'
            ')\n\n'
            'tc_def <- open_dataset("processed/climate/terraclimate/damage_areas_summaries/def.parquet")\n'
            'mpb_cwd <- tc_def |>\n'
            '  filter(calendar_month %in% 6:8) |>\n'
            '  collect() |>\n'
            '  inner_join(st_drop_geometry(damage), by = "DAMAGE_AREA_ID")',
            language="r",
        )


    st.markdown("#### Output checklist after workflows run")
    st.markdown(
        "| Workflow | What you get | Use it for |\n"
        "|---|---|---|\n"
        "| IDS foundation | cleaned GeoPackage layers and lookups | damage/host filtering, survey geometry, map joins |\n"
        "| IDS + climate | pixel maps, yearly pixel values, per-variable damage-area summaries | outbreak climate histories and lag analyses |\n"
        "| FIA summaries | tree, seedling, mortality, disturbance, treatment, condition, and damage-agent parquets | plot-level forest structure and disturbance questions |\n"
    )

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
    st.dataframe(vc_df, width="stretch", hide_index=True)

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
        inv.style.map(color_status, subset=["Status"]),
        width="stretch", hide_index=True,
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
    st.dataframe(vc_df, width="stretch", hide_index=True)

    st.markdown("---")
    st.subheader("Output File Inventory")
    st.caption("One ~19–23 GB parquet per variable in `processed/climate/prism/damage_areas_summaries/`.")
    inv = file_inventory_table(PRISM_VARS, _prism_summary_files())
    st.dataframe(
        inv.style.map(color_status, subset=["Status"]),
        width="stretch", hide_index=True,
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
    st.dataframe(vc_df, width="stretch", hide_index=True)

    st.markdown("---")
    st.subheader("Output File Inventory")
    st.caption("One ~9–13 GB parquet per variable in `processed/climate/worldclim/damage_areas_summaries/`.")
    inv = file_inventory_table(WC_VARS, _wc_summary_files())
    st.dataframe(
        inv.style.map(color_status, subset=["Status"]),
        width="stretch", hide_index=True,
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
        "Each climate dataset decomposes IDS damage areas into the raster pixels they "
        "overlap. This tab visualizes pixel centroids to show the grid structure on a map."
    )

    # Dataset selector
    grid_dataset = st.radio(
        "Show pixel map for",
        ["IDS damage areas — TerraClimate (sampled 30k)",
         "IDS damage areas — PRISM (sampled 30k)",
         "IDS damage areas — WorldClim (sampled 30k)"],
        key="grid_dataset_sel",
    )

    if "TerraClimate" in grid_dataset:
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
                    paper_bgcolor="#fffefa", font_color="#495149",
                    margin=dict(l=0, r=0, t=30, b=0),
                )
                st.plotly_chart(fig, width="stretch")
                plot_source_link("docs/dashboard/pages/2_Climate.py", line=377)

                st.caption(
                    "Each dot is one unique raster pixel. "
                    "The grid pattern reflects the underlying dataset resolution: "
                    "~4km (TerraClimate / WorldClim) or ~800m (PRISM). "
                )

    st.markdown("---")
    st.subheader("Summary Schema")
    st.markdown(
        "All three datasets produce summary parquets with the same schema — "
        "one row per **damage area × month** with area-weighted climate values."
    )
    schema_df = pd.DataFrame(SUMMARY_SCHEMA, columns=["Column", "Type", "Description"])
    st.dataframe(schema_df, width="stretch", hide_index=True)

    st.markdown("---")
    st.subheader("Dataset Comparison")
    comp_df = pd.DataFrame([
        ["TerraClimate", "GEE (IDAHO_EPSCOR/TERRACLIMATE)", "~4 km", "1958–2024", "14", "Global", "~140 GB"],
        ["PRISM",        "Web service (nacse.org)",          "800 m",  "1997–2024", "7",  "CONUS",  "~135 GB"],
        ["WorldClim",    "Direct download (GeoTIFF)",        "~4.5 km","1950–2024", "3",  "Global", "~31 GB"],
    ], columns=["Dataset", "Access", "Resolution", "Period", "Variables", "Coverage", "Summary Size"])
    st.dataframe(comp_df, width="stretch", hide_index=True)
