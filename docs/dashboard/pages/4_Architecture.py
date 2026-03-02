# ==============================================================================
# pages/4_Architecture.py
# Pixel Decomposition Architecture — workflow diagrams and pipeline explanation
# ==============================================================================

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import apply_dark_css, metric_card

st.set_page_config(page_title="Architecture", page_icon="🔗", layout="wide")
apply_dark_css()

st.title("🔗 Pipeline Architecture")
st.markdown(
    "How climate data is extracted for both **IDS damage areas** and **FIA forest plots** "
    "using the shared **pixel decomposition** pattern."
)

tab_overview, tab_ids_pipeline, tab_fia_pipeline, tab_comparison, tab_code = st.tabs([
    "🗂️ Overview",
    "🗺️ IDS Pipeline",
    "🌲 FIA Pipeline",
    "⚖️ Comparison",
    "💻 Code Reference",
])

# ==============================================================================
# OVERVIEW — dual-pipeline graphviz diagram
# ==============================================================================
with tab_overview:
    st.subheader("The Pixel Decomposition Pattern")
    st.markdown(
        "Rather than clipping rasters per observation (slow, imprecise for large polygons), "
        "this pipeline maps each observation to the underlying raster **pixels** it overlaps, "
        "then extracts climate once **per unique pixel** and joins back. "
        "This approach is shared by both IDS polygon extraction and FIA point extraction."
    )

    st.graphviz_chart("""
digraph pipeline {
    rankdir=LR;
    graph [bgcolor="#0e1117" fontcolor="#dddddd" fontname="sans-serif"];
    node  [shape=box style="filled,rounded" fillcolor="#161b22" fontcolor="#dddddd"
           fontname="sans-serif" color="#30363d" margin="0.2,0.1"];
    edge  [color="#58a6ff" fontcolor="#aaaaaa" fontname="sans-serif" fontsize=10];

    // Shared climate source
    tc [label="TerraClimate\\n(GEE: IDAHO_EPSCOR/TERRACLIMATE)\\n4km global · monthly · 1958–2024"
        shape=cylinder fillcolor="#1a365d"];

    subgraph cluster_ids {
        label="IDS Pathway — Area-Weighted Extraction";
        style=filled;
        color="#222c3c";
        fontcolor="#58a6ff";

        ids   [label="IDS Damage Areas\\n(4.4M polygons)\\nDCA_CODE · HOST_CODE · YEAR"];
        pm1   [label="Pixel Map\\n(damage_areas_pixel_map.parquet)\\nDAMAGE_AREA_ID · pixel_id · coverage_fraction"];
        pv1   [label="Pixel Values\\n(unique pixels × month)\\ntc_{year}.parquet"];
        summ1 [label="Climate Summaries\\n(per damage area × month)\\nweighted_mean · value_min · value_max"];

        ids -> pm1  [label="exact_extract()\\ncoverage-weighted"];
        pm1 -> pv1  [label="extract per\\nunique pixel"];
        pv1 -> summ1[label="area-weighted\\njoin + aggregate"];
    }

    subgraph cluster_fia {
        label="FIA Pathway — Point Extraction";
        style=filled;
        color="#1f2d1f";
        fontcolor="#58a6ff";

        fia  [label="FIA Plot Locations\\n(6,956 points)\\nall_site_locations.csv"];
        pm2  [label="Site Pixel Map\\n(fia_site_pixel_map.parquet)\\nsite_id · pixel_id · x · y"];
        pv2  [label="Pixel Values\\n(unique pixels × month)\\n_gee_annual/yr_{year}.parquet"];
        sc   [label="Site Climate\\n(fia_site_climate.parquet)\\nsite_id · year · month · variable · value"];

        fia -> pm2  [label="cellFromXY()\\nnearest centroid"];
        pm2 -> pv2  [label="extract per\\nunique pixel"];
        pv2 -> sc   [label="direct join\\n(no weighting)"];
    }

    tc -> pv1;
    tc -> pv2;
}
""")

    st.markdown("---")
    st.markdown(
        "**Key insight:** FIA sites and IDS damage areas both ultimately query the same "
        "TerraClimate pixels via the same GEE extraction utilities (`gee_utils.R`, "
        "`climate_extract.R`). The only difference is how observations map to pixels:\n\n"
        "- **IDS polygons** → multiple overlapping pixels with `coverage_fraction` weights → "
        "area-weighted mean\n"
        "- **FIA points** → single nearest-centroid pixel → direct value (no weighting)\n"
    )

# ==============================================================================
# IDS PIPELINE — step by step
# ==============================================================================
with tab_ids_pipeline:
    st.subheader("IDS Climate Extraction — Step by Step")

    steps = [
        ("Step 0", "Download + merge IDS geodatabases",
         "10 regional .gdb files → `ids_layers_cleaned.gpkg`",
         "`01_ids/scripts/01_download_ids.R` + `02_merge_clean_ids.R`"),
        ("Step 1", "Build pixel maps",
         "For each layer (damage_areas, damage_points, surveyed_areas), "
         "use `exact_extract()` to find all TerraClimate pixels that overlap each polygon. "
         "Output: `{layer}_pixel_map.parquet` with `coverage_fraction` per pixel.",
         "`02_terraclimate/scripts/02_build_pixel_maps.R`"),
        ("Step 2", "Extract climate values per unique pixel",
         "Deduplicate pixels across all observations. "
         "Pull monthly climate from GEE for only the unique pixels (far fewer than n_observations × n_pixels). "
         "Output: `terraclimate_{year}.parquet` (wide format, one row per pixel-month).",
         "`02_terraclimate/scripts/03_extract_terraclimate.R`"),
        ("Step 3", "Build area-weighted summaries",
         "Join pixel values back through pixel maps to observations. "
         "Compute `weighted_mean = sum(value × coverage_fraction) / sum(coverage_fraction)` "
         "per observation × month. Output: `{variable}.parquet` (~10 GB each).",
         "`scripts/build_summaries.R` (shared across all climate datasets)"),
    ]

    for step, title, description, script in steps:
        with st.expander(f"**{step}: {title}**", expanded=True):
            st.markdown(description)
            st.caption(f"Script: `{script}`")

    st.markdown("---")
    st.subheader("Why Pixel Decomposition?")
    st.markdown(
        "| Concern | Naive approach (clip rasters) | Pixel decomposition |\n"
        "|---------|-------------------------------|---------------------|\n"
        "| **Speed** | Clip once per observation × month | Extract once per unique pixel; "
        "4.4M damage areas share ~N unique pixels |\n"
        "| **Accuracy** | Partial pixel coverage not handled | `coverage_fraction` captures "
        "how much of each pixel falls within the polygon |\n"
        "| **Storage** | One raster per observation | Long-format parquets, columnar "
        "compression; load one variable at a time with Arrow |\n"
        "| **'Pancake features'** | Adjacent polygons share raster cells → redundant extraction | "
        "Deduplication step handles this naturally |\n"
        "| **Flexibility** | Fixed spatial summary | Can recompute summaries with "
        "different weights or spatial filters without re-extracting |\n"
    )

# ==============================================================================
# FIA PIPELINE — step by step
# ==============================================================================
with tab_fia_pipeline:
    st.subheader("FIA Site Climate Extraction — Step by Step")

    fia_steps = [
        ("Step 1", "Compile FIA site locations",
         "`all_site_locations.csv` — 6,956 FIA plot locations (lat/lon, site_id) "
         "compiled from the FIA PLOT table across all 50 states.",
         "`05_fia/scripts/03_extract_trees.R` (LAT/LON extracted from PLOT table)"),
        ("Step 2", "Build site pixel map",
         "For point locations, pixel assignment is simpler than polygons: "
         "find the TerraClimate 4km pixel centroid nearest to each site. "
         "Uses `build_pixel_map()` from `climate_extract.R` with a reference raster. "
         "Output: `fia_site_pixel_map.parquet` (site_id · pixel_id · x · y).",
         "`05_fia/scripts/06_extract_site_climate.R`"),
        ("Step 3", "Authenticate GEE",
         "One-time browser authentication: `ee$Authenticate(auth_mode = 'notebook')`. "
         "Credentials saved to `~/.config/earthengine/credentials` — "
         "subsequent runs authenticate automatically via `ee$Initialize()`.",
         "Manual step; run once per machine"),
        ("Step 4", "Extract TerraClimate 1958–2024 via GEE",
         "Loop over years 1958–2024. For each year: build a stacked GEE Image "
         "(`IDAHO_EPSCOR/TERRACLIMATE`, 6 variables × 12 months), "
         "extract values at unique site pixels via `.sampleRegions()`. "
         "Save annual parquet to `_gee_annual/yr_{year}.parquet`. "
         "Years with no GEE data (2025+) are skipped gracefully with a warning.",
         "`05_fia/scripts/06_extract_site_climate.R` (calls `climate_extract.R`)"),
        ("Step 5", "Consolidate annual parquets",
         "Join pixel values back to site pixel map. "
         "Since all FIA sites are points (`coverage_fraction = 1.0`), "
         "no area-weighting is needed — direct join on `pixel_id`. "
         "Add `water_year` and `water_year_month` columns. "
         "Output: `fia_site_climate.parquet` (23.5M rows, 61.9 MB).",
         "`05_fia/scripts/06_extract_site_climate.R`"),
    ]

    for step, title, description, script in fia_steps:
        with st.expander(f"**{step}: {title}**", expanded=True):
            st.markdown(description)
            st.caption(f"Script: `{script}`")

    st.markdown("---")
    st.subheader("Why FIA site climate is in 05_fia/")
    st.markdown(
        "The FIA site climate data lives in `05_fia/data/processed/site_climate/` "
        "because the *observations* are FIA plots. Conceptually, however, the "
        "extraction pattern is identical to `02_terraclimate` — it just operates "
        "on points instead of polygons. The shared utilities (`gee_utils.R`, "
        "`climate_extract.R`) make this connection explicit in the code."
    )

# ==============================================================================
# COMPARISON TABLE
# ==============================================================================
with tab_comparison:
    st.subheader("IDS vs. FIA Extraction Comparison")

    comp_data = [
        ["Observation type",        "Polygon (damage area)",          "Point (plot location)"],
        ["Pixel assignment method", "`exact_extract()` + coverage_fraction", "`cellFromXY()` nearest centroid"],
        ["Coverage fraction",       "Weighted (0–1 per pixel)",        "Always 1.0 (centroid)"],
        ["Area-weighted mean",      "Yes — required for accuracy",     "No — single pixel value"],
        ["n_observations",          "~4.4M damage areas",              "6,956 FIA sites"],
        ["n_unique_pixels",         "Much smaller than n_obs × n_px",  "≤ 6,956 (one per site)"],
        ["Climate source",          "TerraClimate, PRISM, WorldClim",  "TerraClimate only"],
        ["Temporal range",          "1997–2024 (survey era)",          "1958–2024 (full record)"],
        ["Output format",           "One parquet per variable (~10 GB)","Single consolidated parquet (62 MB)"],
        ["Output location",         "`processed/climate/{dataset}/damage_areas_summaries/`",
                                    "`05_fia/data/processed/site_climate/`"],
        ["Utility functions",       "`build_pixel_map()`, `extract_climate_from_gee()`",
                                    "Same functions from `climate_extract.R`"],
    ]
    comp_df = pd.DataFrame(comp_data, columns=["Aspect", "IDS Pathway", "FIA Pathway"])
    st.dataframe(comp_df, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.subheader("Shared Utility Functions")
    st.markdown(
        "| Function | File | Used by |\n"
        "|----------|------|---------|\n"
        "| `build_pixel_map()` | `scripts/utils/climate_extract.R` | Both IDS and FIA pipelines |\n"
        "| `extract_climate_from_gee()` | `scripts/utils/climate_extract.R` | Both |\n"
        "| `init_gee()` | `scripts/utils/gee_utils.R` | Both (GEE authentication + initialization) |\n"
        "| `calendar_to_water_year()` | `scripts/utils/time_utils.R` | Both (adds water_year column) |\n"
        "| `load_config()` | `scripts/utils/load_config.R` | Both (reads config.yaml) |\n"
    )

    st.markdown("---")
    st.subheader("Water Year Convention")
    st.markdown(
        "All time series use **both** calendar year/month and water year/month:\n\n"
        "- Water year runs **October → September**\n"
        "- Month ≥ 10 → `water_year = calendar_year + 1`\n"
        "- `water_year_month`: 1 = October, 2 = November, …, 12 = September\n\n"
        "This allows analyses to use whichever time base is appropriate: "
        "calendar year (temperature), water year (precipitation/drought), "
        "or survey timing (FIA INVYR)."
    )

# ==============================================================================
# CODE REFERENCE
# ==============================================================================
with tab_code:
    st.subheader("Code Reference")

    st.markdown("### Utility file overview")
    utils_data = [
        ["`scripts/utils/load_config.R`",      "Reads `config.yaml`; exposes nested access to paths and parameters"],
        ["`scripts/utils/climate_extract.R`",  "Core extraction logic: `build_pixel_map()`, `extract_climate_from_gee()`, area-weighted summaries"],
        ["`scripts/utils/gee_utils.R`",        "GEE authentication, initialization, and TerraClimate asset helpers"],
        ["`scripts/utils/time_utils.R`",       "Water year conversion: `calendar_to_water_year()`"],
        ["`scripts/utils/metadata_utils.R`",   "FIA REF table lookups, IDS data dictionary helpers"],
    ]
    st.dataframe(pd.DataFrame(utils_data, columns=["File", "Purpose"]),
                 use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("### Pixel map schema")
    st.code(
        '# damage_areas_pixel_map.parquet\n'
        'DAMAGE_AREA_ID      large_string   # links to IDS damage area\n'
        'OBSERVATION_ID      str            # original observation ID\n'
        'pixel_id            int64          # unique raster pixel identifier\n'
        'x                   float64        # pixel centroid longitude (WGS84)\n'
        'y                   float64        # pixel centroid latitude (WGS84)\n'
        'coverage_fraction   float64        # fraction of pixel covered by polygon (0–1)\n',
        language="text",
    )
    st.code(
        '# fia_site_pixel_map.parquet\n'
        'site_id             str            # FIA site identifier\n'
        'pixel_id            int64          # unique raster pixel (same grid as above)\n'
        'x                   float64        # pixel centroid longitude\n'
        'y                   float64        # pixel centroid latitude\n'
        '# Note: coverage_fraction = 1.0 for all point observations (implicit)',
        language="text",
    )

    st.markdown("---")
    st.markdown("### Reading large climate summaries efficiently (R)")
    st.code(
        'library(arrow); library(dplyr)\n\n'
        '# DON\'T: read_parquet() loads all 10 GB into memory\n'
        '# DO:    open_dataset() creates a lazy reference\n'
        'ds <- open_dataset("processed/climate/terraclimate/damage_areas_summaries/tmmx.parquet")\n\n'
        '# Filter before collect() — only touched rows are loaded\n'
        'mpb_tmmx_summer <- ds |>\n'
        '  filter(calendar_month %in% 6:8) |>\n'
        '  collect()\n\n'
        '# Join damage areas to climate via DAMAGE_AREA_ID\n'
        'library(sf)\n'
        'mpb <- st_read("01_ids/data/processed/ids_layers_cleaned.gpkg",\n'
        '               layer = "damage_areas",\n'
        '               query = "SELECT DAMAGE_AREA_ID FROM damage_areas WHERE DCA_CODE = 11006")\n'
        'st_geometry(mpb) <- NULL\n\n'
        'mpb_climate <- ds |>\n'
        '  semi_join(mpb, by = "DAMAGE_AREA_ID", copy = TRUE) |>\n'
        '  collect()',
        language="r",
    )

    st.markdown("---")
    st.markdown("### GEE authentication (one-time setup)")
    st.code(
        'library(reticulate)\n'
        'ee <- import("ee")\n\n'
        '# First time only — opens browser URL for auth\n'
        'ee$Authenticate(auth_mode = "notebook")\n\n'
        '# All subsequent runs — reads ~/.config/earthengine/credentials\n'
        'ee$Initialize(project = "your-gee-project-id")',
        language="r",
    )
